"""Tests for consuming the markup engine in billing (B3 + B4 + B5).

- B4: the daily cost rollup carries ``served_model`` + ``byo`` so per-model
  markup resolves at billing time.
- compute_billed_line: the shared per-line primitive — fallback precedence,
  specific-rule override, and the billing-mode matrix (byo / pass-through).
- B3 + B5: ``generate_billing`` applies markup per line and honours billing_mode
  (parity under global-default-only, differentiation under a specific rule).

The pure ``compute_billed_line`` tests build a resolver from in-memory rules; the
rollup + generate_billing tests run against the ORM schema (conftest).
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select

import observability.metrics as metrics
from models.db import (
    NULL_SUBJECT_ID,
    CostDaily,
    CostEvent,
    MarkupRule,
    TenantConfig,
)
from services.aggregator import rollup_daily
from services.billing import generate_billing
from services.markup import (
    BILLING_MODE_BYO,
    BILLING_MODE_BYO_MARKUP_ONLY,
    BILLING_MODE_INTERNAL,
    BILLING_MODE_PASS_THROUGH,
    BILLING_MODE_STANDARD,
    MarkupResolver,
    compute_billed_line,
)
from tests.conftest import T

PAST = datetime(2026, 1, 1, tzinfo=UTC)


def _rule(**kw) -> MarkupRule:
    kw.setdefault("effective_from", PAST)
    return MarkupRule(**kw)


# --------------------------------------------------------------------------
# compute_billed_line — pure, no DB
# --------------------------------------------------------------------------


def test_global_default_falls_back_to_tenant_pct():
    """Only the all-wildcard default matches → the tenant's fallback_pct wins,
    not the default rule's own pct (preserves legacy tenant overrides)."""
    resolver = MarkupResolver([_rule(markup_pct=Decimal("30"))])
    billed, pct, rule = compute_billed_line(
        resolver,
        BILLING_MODE_STANDARD,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=40.0,
        provider="openai",
        model="gpt-5",
    )
    assert billed == Decimal("14.00")
    assert pct == 40.0
    assert rule is not None  # the global default matched


def test_specific_rule_overrides_fallback():
    resolver = MarkupResolver(
        [
            _rule(markup_pct=Decimal("30")),
            _rule(tenant_id=T("t1"), provider="openai", model="gpt-5", markup_pct=Decimal("100")),
        ]
    )
    billed, pct, rule = compute_billed_line(
        resolver,
        BILLING_MODE_STANDARD,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=40.0,
        provider="openai",
        model="gpt-5",
    )
    assert billed == Decimal("20.00")
    assert pct == 100.0
    assert rule is not None
    assert rule.markup_pct == Decimal("100")


def test_byo_mode_forces_zero_pct_keeps_surcharge():
    resolver = MarkupResolver(
        [
            _rule(
                tenant_id=T("t1"),
                provider="openai",
                markup_pct=Decimal("50"),
                surcharge_input_per_1m_usd=Decimal("2"),
            )
        ]
    )
    billed, pct, _ = compute_billed_line(
        resolver,
        BILLING_MODE_BYO,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="openai",
        byo=True,
        input_tokens=1_000_000,
    )
    # pct forced to 0; only the absolute platform fee: 10 + 1e6 * 2 / 1e6 = 12
    assert billed == Decimal("12.00")
    assert pct == 0.0


def test_byo_mode_leaves_non_byo_line_standard():
    resolver = MarkupResolver(
        [_rule(tenant_id=T("t1"), provider="openai", markup_pct=Decimal("50"))]
    )
    billed, pct, _ = compute_billed_line(
        resolver,
        BILLING_MODE_BYO,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="openai",
        byo=False,
    )
    assert billed == Decimal("15.00")
    assert pct == 50.0


def test_byo_line_global_default_bills_incurred():
    """BYO line, byo mode, only the global default → pct forced 0 and no
    surcharge defined → billed equals incurred."""
    resolver = MarkupResolver([_rule(markup_pct=Decimal("30"))])
    billed, pct, _ = compute_billed_line(
        resolver,
        BILLING_MODE_BYO,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="openai",
        byo=True,
    )
    assert billed == Decimal("10.00")
    assert pct == 0.0


def test_pass_through_bills_incurred_ignoring_rules():
    resolver = MarkupResolver(
        [
            _rule(markup_pct=Decimal("30")),
            _rule(tenant_id=T("t1"), provider="openai", markup_pct=Decimal("99")),
        ]
    )
    billed, pct, rule = compute_billed_line(
        resolver,
        BILLING_MODE_PASS_THROUGH,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="openai",
    )
    assert billed == Decimal("10.00")
    assert pct == 0.0
    assert rule is None


# --------------------------------------------------------------------------
# compute_billed_line — zero-margin loud-miss counter
# --------------------------------------------------------------------------


def _zero_margin_count(billing_mode: str, service_key: str = "none") -> float:
    """Current value of the zero-margin counter for one label combo."""
    return metrics.markup_zero_margin_total.labels(
        billing_mode=billing_mode, service_key=service_key
    )._value.get()


def test_zero_fallback_billable_increments_and_bills_at_cost():
    """A real billable line with no matching rule and fallback_pct=0 is charged
    AT COST (0% margin) — a loud, alertable miss that increments the counter
    without changing the billed amount. Deleting the .inc() fails this test."""
    before = _zero_margin_count(BILLING_MODE_STANDARD)
    resolver = MarkupResolver([])  # no rules at all → genuine miss
    billed, pct, rule = compute_billed_line(
        resolver,
        BILLING_MODE_STANDARD,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=0.0,
        provider="openai",
        model="gpt-5",
    )
    assert billed == Decimal("10.00")  # at cost, zero margin
    assert pct == 0.0
    assert rule is None
    assert _zero_margin_count(BILLING_MODE_STANDARD) - before == 1


def test_zero_incurred_line_does_not_increment():
    """A $0-incurred billable line has no cost to have earned margin on, so it is
    NOT a zero-margin event — the counter must not fire (avoids alert noise on
    free/zero-cost lines). Reverting the ``incurred_usd > 0`` guard fails this."""
    before = _zero_margin_count(BILLING_MODE_STANDARD)
    resolver = MarkupResolver([])  # no rules → fallback path, pct 0
    billed, pct, rule = compute_billed_line(
        resolver,
        BILLING_MODE_STANDARD,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("0"),
        fallback_pct=0.0,
        provider="openai",
        model="gpt-5",
    )
    assert billed == Decimal("0.00")
    assert pct == 0.0
    assert _zero_margin_count(BILLING_MODE_STANDARD) - before == 0


def test_specific_rule_does_not_increment():
    """A specificity>0 rule owns the line → no zero-margin fallback, no count."""
    before = _zero_margin_count(BILLING_MODE_STANDARD)
    resolver = MarkupResolver(
        [_rule(tenant_id=T("t1"), provider="openai", model="gpt-5", markup_pct=Decimal("50"))]
    )
    _, _, rule = compute_billed_line(
        resolver,
        BILLING_MODE_STANDARD,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=0.0,
        provider="openai",
        model="gpt-5",
    )
    assert rule is not None
    assert _zero_margin_count(BILLING_MODE_STANDARD) - before == 0


def test_internal_and_passthrough_do_not_increment():
    """Internal + pass_through return before the fallback block — never billable
    revenue lines, so a 0% fallback must not fire the zero-margin miss."""
    before_internal = _zero_margin_count(BILLING_MODE_INTERNAL)
    before_pt = _zero_margin_count(BILLING_MODE_PASS_THROUGH)
    resolver = MarkupResolver([])
    compute_billed_line(
        resolver,
        BILLING_MODE_INTERNAL,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=0.0,
        provider="openai",
    )
    compute_billed_line(
        resolver,
        BILLING_MODE_PASS_THROUGH,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=0.0,
        provider="openai",
    )
    assert _zero_margin_count(BILLING_MODE_INTERNAL) - before_internal == 0
    assert _zero_margin_count(BILLING_MODE_PASS_THROUGH) - before_pt == 0


def test_byo_forced_zero_does_not_increment():
    """BYO + byo=True forces pct to 0 by design (platform-fee mode) — the
    force_zero path is intentional, not a miss, so it must not increment."""
    before = _zero_margin_count(BILLING_MODE_BYO)
    resolver = MarkupResolver([])
    billed, pct, _ = compute_billed_line(
        resolver,
        BILLING_MODE_BYO,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=0.0,
        provider="openai",
        byo=True,
    )
    assert billed == Decimal("10.00")
    assert pct == 0.0
    assert _zero_margin_count(BILLING_MODE_BYO) - before == 0


# --------------------------------------------------------------------------
# B4 — rollup carries served_model + byo
# --------------------------------------------------------------------------


async def test_rollup_populates_served_model_and_byo(db_session_factory):
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
    lic, sub = uuid.uuid4(), uuid.uuid4()
    async with db_session_factory() as s:
        s.add_all(
            [
                CostEvent(
                    tenant_id=T("t-b4"),
                    license_id=lic,
                    subject_id=sub,
                    service_key="kag",
                    provider="openai",
                    served_model="gpt-5",
                    input_tokens=10,
                    output_tokens=2,
                    cost_usd=Decimal("0.01"),
                    cost_source="inference-router",
                    created_at=ts,
                ),
                CostEvent(
                    tenant_id=T("t-b4"),
                    license_id=lic,
                    subject_id=sub,
                    service_key="kag",
                    provider="openai",
                    served_model="gpt-5-mini",
                    input_tokens=5,
                    output_tokens=1,
                    cost_usd=Decimal("0.002"),
                    cost_source="inference-router",
                    created_at=ts,
                ),
                CostEvent(
                    tenant_id=T("t-b4"),
                    license_id=lic,
                    subject_id=sub,
                    service_key="wm",
                    provider="anthropic",
                    served_model="claude",
                    input_tokens=7,
                    output_tokens=1,
                    cost_usd=Decimal("0"),
                    cost_source="worker_management",
                    usage_extra={"byo": True},
                    created_at=ts,
                ),
            ]
        )
        await s.commit()

    await rollup_daily(db_session_factory)

    async with db_session_factory() as s:
        rows = (
            (await s.execute(select(CostDaily).where(CostDaily.tenant_id == T("t-b4"))))
            .scalars()
            .all()
        )
    by_key = {(r.served_model, r.byo): r for r in rows}
    assert ("gpt-5", False) in by_key
    assert ("gpt-5-mini", False) in by_key
    assert ("claude", True) in by_key
    assert by_key[("gpt-5", False)].total_input_tokens == 10


async def test_rollup_byo_defaults_false_when_absent(db_session_factory):
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
    async with db_session_factory() as s:
        s.add(
            CostEvent(
                tenant_id=T("t-b4-nb"),
                license_id=uuid.uuid4(),
                subject_id=uuid.uuid4(),
                service_key="kag",
                provider="openai",
                served_model=None,  # NULL → '__unknown__' sentinel
                input_tokens=1,
                output_tokens=1,
                cost_usd=Decimal("0.001"),
                cost_source="inference-router",
                created_at=ts,
            )
        )
        await s.commit()
    await rollup_daily(db_session_factory)
    async with db_session_factory() as s:
        row = (
            await s.execute(select(CostDaily).where(CostDaily.tenant_id == T("t-b4-nb")))
        ).scalar_one()
    assert row.served_model == "__unknown__"
    assert row.byo is False


async def test_rollup_splits_owner_provided_from_platform(db_session_factory):
    """owner_provided (managed BYO) is a distinct cost_daily dimension from a
    plain platform line — even though both carry byo=false — so managed-BYO spend
    is attributable separately."""
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
    lic, sub = uuid.uuid4(), uuid.uuid4()
    async with db_session_factory() as s:
        s.add_all(
            [
                CostEvent(
                    tenant_id=T("t-op"),
                    license_id=lic,
                    subject_id=sub,
                    service_key="w2v-tts",
                    provider="openai",
                    served_model="tts-1",
                    input_tokens=100,
                    output_tokens=0,
                    cost_usd=Decimal("0.0015"),
                    cost_source="worker-management",
                    usage_extra={"owner_provided": True},  # managed BYO
                    created_at=ts,
                ),
                CostEvent(
                    tenant_id=T("t-op"),
                    license_id=lic,
                    subject_id=sub,
                    service_key="w2v-tts",
                    provider="openai",
                    served_model="tts-1",
                    input_tokens=50,
                    output_tokens=0,
                    cost_usd=Decimal("0.00075"),
                    cost_source="worker-management",  # no provenance = plain platform
                    created_at=ts,
                ),
            ]
        )
        await s.commit()

    await rollup_daily(db_session_factory)

    async with db_session_factory() as s:
        rows = (
            (await s.execute(select(CostDaily).where(CostDaily.tenant_id == T("t-op"))))
            .scalars()
            .all()
        )
    by_owner = {r.owner_provided: r for r in rows}
    # Two distinct rows — same provider/model/byo, split only by owner_provided.
    assert set(by_owner) == {True, False}
    assert by_owner[True].total_input_tokens == 100
    assert by_owner[False].total_input_tokens == 50
    assert by_owner[True].byo is False and by_owner[False].byo is False


# --------------------------------------------------------------------------
# B3 + B5 — generate_billing
# --------------------------------------------------------------------------


def _this_month() -> tuple[str, date]:
    today = datetime.now(UTC).date()
    return today.strftime("%Y-%m"), today


async def _seed_cost_daily(
    session, *, tenant, tenant_name, license_id, provider, model, cost, byo=False
):
    _, day = _this_month()
    session.add(
        CostDaily(
            tenant_id=tenant,
            tenant_name=tenant_name,
            license_id=license_id,
            subject_id=NULL_SUBJECT_ID,
            service_key="kag",
            provider=provider,
            served_model=model,
            byo=byo,
            cost_source="inference-router",
            bucket_date=day,
            total_input_tokens=1000,
            total_output_tokens=200,
            total_cost_usd=Decimal(cost),
            request_count=1,
        )
    )


async def test_generate_billing_parity_under_global_default(db_session, test_settings):
    month, _ = _this_month()
    name = "t-bill-parity"
    tenant, lic = T(name), uuid.uuid4()
    db_session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name=name,
            markup_pct=Decimal("30"),
            billing_mode="standard_markup",
        )
    )
    db_session.add(_rule(markup_pct=Decimal("30")))  # global default
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        tenant_name=name,
        license_id=lic,
        provider="openai",
        model="gpt-5",
        cost="10",
    )
    await db_session.commit()

    result = await generate_billing(tenant, month, db_session, test_settings)
    totals = result["totals"]
    assert Decimal(totals["internal_cost_usd"]) == Decimal("10")
    # Only the global default → fallback 30% → 13.00 (identical to legacy total).
    assert Decimal(totals["customer_cost_usd"]) == Decimal("13.00")
    assert result["billing_mode"] == "standard_markup"


async def test_generate_billing_differentiates_per_model(db_session, test_settings):
    month, _ = _this_month()
    name = "t-bill-diff"
    tenant, lic = T(name), uuid.uuid4()
    db_session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name=name,
            markup_pct=Decimal("30"),
            billing_mode="standard_markup",
        )
    )
    db_session.add(_rule(markup_pct=Decimal("30")))  # global default
    db_session.add(
        _rule(tenant_id=tenant, provider="openai", model="gpt-5", markup_pct=Decimal("100"))
    )
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        tenant_name=name,
        license_id=lic,
        provider="openai",
        model="gpt-5",
        cost="10",
    )
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        tenant_name=name,
        license_id=lic,
        provider="anthropic",
        model="claude",
        cost="10",
    )
    await db_session.commit()

    result = await generate_billing(tenant, month, db_session, test_settings)
    billed_by_provider = result["totals"]["billed_by_provider"]
    assert Decimal(billed_by_provider["openai"]) == Decimal("20.00")  # 100%
    assert Decimal(billed_by_provider["anthropic"]) == Decimal("13.00")  # fallback 30%
    assert Decimal(result["totals"]["customer_cost_usd"]) == Decimal("33.00")


async def test_generate_billing_byo_platform_fee(db_session, test_settings):
    month, _ = _this_month()
    name = "t-bill-byo"
    tenant, lic = T(name), uuid.uuid4()
    db_session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name=name,
            markup_pct=Decimal("30"),
            billing_mode="byo_platform_fee",
        )
    )
    db_session.add(_rule(markup_pct=Decimal("30")))  # global default
    # BYO line escapes markup; non-BYO line keeps fallback 30%.
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        tenant_name=name,
        license_id=lic,
        provider="openai",
        model="gpt-5",
        cost="10",
        byo=True,
    )
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        tenant_name=name,
        license_id=lic,
        provider="anthropic",
        model="claude",
        cost="10",
        byo=False,
    )
    await db_session.commit()

    result = await generate_billing(tenant, month, db_session, test_settings)
    # 10 (byo, no markup) + 13 (non-byo, 30%) = 23
    assert Decimal(result["totals"]["customer_cost_usd"]) == Decimal("23.00")


async def test_generate_billing_pass_through(db_session, test_settings):
    month, _ = _this_month()
    name = "t-bill-pt"
    tenant, lic = T(name), uuid.uuid4()
    db_session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name=name,
            markup_pct=Decimal("30"),
            billing_mode="pass_through",
        )
    )
    db_session.add(_rule(markup_pct=Decimal("30")))
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        tenant_name=name,
        license_id=lic,
        provider="openai",
        model="gpt-5",
        cost="10",
    )
    await db_session.commit()

    result = await generate_billing(tenant, month, db_session, test_settings)
    assert Decimal(result["totals"]["customer_cost_usd"]) == Decimal("10.00")


# --------------------------------------------------------------------------
# byo_markup_only — the customer paid the provider; bill ONLY the percentage
# --------------------------------------------------------------------------


def test_byo_markup_only_bills_the_percentage_not_the_base():
    """⛔ THE ASSERTION THE MODE EXISTS FOR: incurred x pct, NOT incurred x (1 + pct).
    standard_markup would charge the provider cost the customer already paid a second time."""
    resolver = MarkupResolver(
        [_rule(tenant_id=T("t1"), provider="openai", markup_pct=Decimal("20"))]
    )
    billed, pct, _ = compute_billed_line(
        resolver,
        BILLING_MODE_BYO_MARKUP_ONLY,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="openai",
        byo=True,
    )
    assert billed == Decimal("2.00")
    assert billed != Decimal("12.00")
    assert pct == 20.0


def test_byo_markup_only_keeps_absolute_surcharges():
    resolver = MarkupResolver(
        [
            _rule(
                tenant_id=T("t1"),
                provider="openai",
                markup_pct=Decimal("20"),
                surcharge_input_per_1m_usd=Decimal("2"),
            )
        ]
    )
    billed, _, _ = compute_billed_line(
        resolver,
        BILLING_MODE_BYO_MARKUP_ONLY,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="openai",
        byo=True,
        input_tokens=1_000_000,
    )
    # 10 x 20% + 1e6 x 2 / 1e6 = 2 + 2
    assert billed == Decimal("4.00")


def test_byo_markup_only_fallback_branch_is_also_fee_only():
    """No specific rule → the tenant fallback pct, still WITHOUT the base."""
    billed, pct, _ = compute_billed_line(
        MarkupResolver([_rule(markup_pct=Decimal("30"))]),
        BILLING_MODE_BYO_MARKUP_ONLY,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=25.0,
        provider="openai",
        byo=True,
    )
    assert billed == Decimal("2.50")
    assert pct == 25.0


def test_byo_markup_only_leaves_non_byo_line_standard():
    resolver = MarkupResolver(
        [_rule(tenant_id=T("t1"), provider="openai", markup_pct=Decimal("50"))]
    )
    billed, _, _ = compute_billed_line(
        resolver,
        BILLING_MODE_BYO_MARKUP_ONLY,
        project_id=None,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="openai",
        byo=False,
    )
    assert billed == Decimal("15.00")


async def test_generate_billing_byo_markup_only_excludes_byo_base_from_internal_cost(
    db_session, test_settings
):
    """⛔ HIGHEST-RISK EDIT: a BYO line costs the platform NOTHING. If its reference cost stayed
    in internal_cost the invoice would report a negative margin and claim the platform paid money
    it did not."""
    month, _ = _this_month()
    name = "t-bill-byo-markup-only"
    tenant, lic = T(name), uuid.uuid4()
    db_session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name=name,
            markup_pct=Decimal("20"),
            billing_mode="byo_markup_only",
        )
    )
    db_session.add(_rule(markup_pct=Decimal("30")))  # global default
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        tenant_name=name,
        license_id=lic,
        provider="openai",
        model="gpt-5",
        cost="10",
        byo=True,
    )
    await _seed_cost_daily(
        db_session,
        tenant=tenant,
        tenant_name=name,
        license_id=lic,
        provider="anthropic",
        model="claude",
        cost="10",
        byo=False,
    )
    await db_session.commit()

    result = await generate_billing(tenant, month, db_session, test_settings)
    totals = result["totals"]
    # BYO: 10 x 20% = 2.00 ; platform line: 10 x 1.20 = 12.00
    assert Decimal(totals["customer_cost_usd"]) == Decimal("14.00")
    # Only the platform-paid line is internal cost.
    assert Decimal(totals["internal_cost_usd"]) == Decimal("10.00")
    assert Decimal(totals["margin_usd"]) == Decimal("4.00")
    assert Decimal(totals["margin_usd"]) >= 0
    # The base the 20% was taken from stays visible, outside internal cost.
    assert Decimal(totals["byo_reference_cost_usd"]) == Decimal("10.00")
