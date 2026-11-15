"""Tests for the north-star markup engine (services/markup.py).

Covers the consumption-dominant most-specific-wins ladder, the deliberate
"consumption beats flat license" collision (George's decision), wildcard /
no-license semantics, effective-dating, upsert close-prior, and the billed-USD
math (relative pct + absolute surcharge + force_pct_zero for BYO).

These run against the ORM schema (conftest's Base.metadata.create_all), so the
migration's global-default seed is NOT present — each test inserts the rows it
needs explicitly.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from models.db import MarkupRule
from models.domain import MarkupRuleCreate, MarkupRuleResponse
from services import markup as markup_svc
from tests.conftest import T

pytestmark = pytest.mark.asyncio

TS = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


def _rule(**kw) -> MarkupRule:
    kw.setdefault("effective_from", TS - timedelta(days=1))
    return MarkupRule(**kw)


async def _add(session, *rules: MarkupRule) -> None:
    for r in rules:
        session.add(r)
    await session.flush()


#: The specificity ladder, highest rung first, with its weight constant.
_SPEC_RUNGS = [
    ("byo", markup_svc._W_BYO),
    ("model", markup_svc._W_MODEL),
    ("provider", markup_svc._W_PROVIDER),
    ("service_key", markup_svc._W_SERVICE_KEY),
    ("license", markup_svc._W_LICENSE),
    ("project", markup_svc._W_PROJECT),
    ("tenant", markup_svc._W_TENANT),
]

#: `MarkupRule` kwargs that pin EXACTLY one ladder dimension. These let the
#: ladder be read off `_specificity` itself instead of off the constants — the
#: two can disagree, and only the scorer decides a price.
_SPEC_PINS: dict[str, dict] = {
    "byo": {"byo": True},
    "model": {"model": "gpt5"},
    "provider": {"provider": "openai"},
    "service_key": {"service_key": "fr"},
    "license": {"license_id": uuid4()},
    "project": {"project_id": uuid4()},
    "tenant": {"tenant_id": T("t1")},
}


async def test_global_default_matches_everything(db_session):
    await _add(db_session, _rule(markup_pct=Decimal("30")))
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), provider="openai", model="gpt5", ts=TS, project_id=None
    )
    assert row is not None
    assert row.markup_pct == Decimal("30")
    assert markup_svc._specificity(row) == 0


async def test_miss_when_no_rule_seeded(db_session):
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), ts=TS, project_id=None
    )
    assert row is None


async def test_consumption_rule_beats_flat_license(db_session):
    """George's decision: a fully-specified consumption rule outranks a flat
    per-license rule for an event matching both."""
    lic = uuid4()
    await _add(
        db_session,
        _rule(markup_pct=Decimal("30")),  # global default
        _rule(tenant_id=T("t1"), license_id=lic, markup_pct=Decimal("25")),  # flat license
        _rule(  # full consumption rule
            tenant_id=T("t1"),
            service_key="fr",
            provider="openai",
            model="gpt5",
            markup_pct=Decimal("40"),
        ),
    )
    row = await markup_svc.resolve_markup_rule(
        db_session,
        tenant_id=T("t1"),
        license_id=lic,
        service_key="fr",
        provider="openai",
        model="gpt5",
        ts=TS,
        project_id=None,
    )
    assert row is not None
    assert row.markup_pct == Decimal("40")
    # model32 + provider16 + service_key8 + tenant1 = 57.
    # ⚠️ Was 29 under the pre-0036 scale (model16+provider8+service_key4+tenant1).
    # 0036 DOUBLED every weight to free the slot between LICENSE and TENANT for the
    # new `project` dimension — there is no integer between 2 and 1. Doubling is a
    # monotone transform, so ORDERING is unchanged and only the absolute number
    # moved. The real control in this test is the assertion above (the consumption
    # rule still outranks the flat-licence rule); it passed untouched.
    assert markup_svc._specificity(row) == 57


async def test_flat_license_applies_without_consumption_rule(db_session):
    lic = uuid4()
    await _add(
        db_session,
        _rule(markup_pct=Decimal("30")),
        _rule(tenant_id=T("t1"), license_id=lic, markup_pct=Decimal("25")),
    )
    row = await markup_svc.resolve_markup_rule(
        db_session,
        tenant_id=T("t1"),
        license_id=lic,
        provider="openai",
        model="gpt5",
        ts=TS,
        project_id=None,
    )
    assert row is not None
    assert row.markup_pct == Decimal("25")


async def test_provider_rule_beats_bare_tenant(db_session):
    await _add(
        db_session,
        _rule(tenant_id=T("t1"), markup_pct=Decimal("20")),
        _rule(tenant_id=T("t1"), provider="anthropic", markup_pct=Decimal("35")),
    )
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), provider="anthropic", model="opus", ts=TS, project_id=None
    )
    assert row is not None
    assert row.markup_pct == Decimal("35")


async def test_pinned_license_does_not_match_event_without_license(db_session):
    lic = uuid4()
    await _add(
        db_session,
        _rule(markup_pct=Decimal("30")),
        _rule(tenant_id=T("t1"), license_id=lic, markup_pct=Decimal("99")),
    )
    # Event carries no license — the license-pinned rule must NOT match.
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), license_id=None, ts=TS, project_id=None
    )
    assert row is not None
    assert row.markup_pct == Decimal("30")


async def test_other_tenant_rule_is_ignored(db_session):
    await _add(
        db_session,
        _rule(markup_pct=Decimal("30")),
        _rule(tenant_id=T("t2"), provider="openai", markup_pct=Decimal("80")),
    )
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), provider="openai", ts=TS, project_id=None
    )
    assert row is not None
    assert row.markup_pct == Decimal("30")  # t2's rule does not leak to t1


async def test_effective_dating_excludes_closed_row(db_session):
    await _add(
        db_session,
        _rule(
            tenant_id=T("t1"),
            provider="openai",
            markup_pct=Decimal("10"),
            effective_from=TS - timedelta(days=10),
            effective_until=TS - timedelta(days=5),
        ),
        _rule(
            tenant_id=T("t1"),
            provider="openai",
            markup_pct=Decimal("50"),
            effective_from=TS - timedelta(days=5),
        ),
    )
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), provider="openai", ts=TS, project_id=None
    )
    assert row is not None
    assert row.markup_pct == Decimal("50")


async def test_upsert_closes_prior_same_tuple_only(db_session):
    await _add(db_session, _rule(markup_pct=Decimal("30")))  # global default
    first, prior = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), provider="openai", markup_pct=Decimal("20")),
    )
    assert prior is None
    second, prior2 = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), provider="openai", markup_pct=Decimal("33")),
    )
    assert prior2 is not None and prior2.id == first.id
    assert first.effective_until is not None  # prior closed
    # Only one open row for the tuple, and the global default is untouched.
    current = await markup_svc.list_current_markup_rules(db_session)
    open_openai = [r for r in current if r.tenant_id == T("t1") and r.provider == "openai"]
    assert len(open_openai) == 1 and open_openai[0].markup_pct == Decimal("33")
    assert any(r.tenant_id is None and r.markup_pct == Decimal("30") for r in current)


async def test_compute_billed_usd_pct_and_surcharge(db_session):
    rule = _rule(
        markup_pct=Decimal("20"),
        surcharge_output_per_1m_usd=Decimal("2"),
        surcharge_per_request_usd=Decimal("0.01"),
    )
    # incurred 10 * 1.20 = 12 ; + 1M output tokens * $2/1M = 2 ; + 5 req * 0.01 = 0.05
    billed = markup_svc.compute_billed_usd(
        rule,
        incurred_usd=Decimal("10"),
        output_tokens=1_000_000,
        request_count=5,
    )
    assert billed == Decimal("14.05")


async def test_compute_billed_usd_force_pct_zero_byo(db_session):
    """BYO platform-fee: a $0 incurred line still bills the absolute fee."""
    rule = _rule(markup_pct=Decimal("40"), surcharge_output_per_1m_usd=Decimal("3"))
    billed = markup_svc.compute_billed_usd(
        rule,
        incurred_usd=Decimal("0"),
        output_tokens=2_000_000,
        force_pct_zero=True,
    )
    assert billed == Decimal("6.00")  # 0*(1+0) + 2M*$3/1M


async def test_compute_billed_usd_none_rule_passthrough(db_session):
    assert markup_svc.compute_billed_usd(None, incurred_usd=Decimal("7.5")) == Decimal("7.50")


async def test_markup_miss_increments_provider_only_metric(db_session):
    """A miss (nothing seeded) increments markup_miss_total under a *provider*
    label only. The per-tenant UUID label was dropped to avoid an unbounded
    Prometheus series on the systemic-misconfig miss path."""
    import observability.metrics as om

    counter = om.markup_miss_total.labels(provider="anthropic")
    before = counter._value.get()

    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), provider="anthropic", model="opus", ts=TS, project_id=None
    )
    assert row is None  # no rule seeded -> miss path fires the metric
    assert counter._value.get() == before + 1

    # Mutation guard: the tenant_id label must no longer exist on this metric.
    with pytest.raises(ValueError):
        om.markup_miss_total.labels(tenant_id=T("t1"), provider="anthropic")


# --------------------------------------------------------------------------- #
# The `byo` dimension (0027) — a tenant's BYO rate is expressible at all
#
# Before 0027 `resolve_billed_line` computed
#     force_zero = billing_mode == BILLING_MODE_BYO and byo
# unconditionally, so a byo_platform_fee tenant's BYO spend was ALWAYS marked up
# 0% and no row could say otherwise. George asked (2026-09-10) for 20% on BYO
# lines. These tests pin both halves: the new capability, and the fact that
# nothing changes for an estate that has written no byo-scoped rule.
# --------------------------------------------------------------------------- #


def _resolver(*rules: MarkupRule) -> markup_svc.MarkupResolver:
    """A resolver over in-memory rules — no DB round-trip, no flush."""
    return markup_svc.MarkupResolver(list(rules))


async def test_byo_scoped_rule_lifts_the_forced_zero(db_session):
    """THE CAPABILITY: byo=True + 20% bills incurred*1.20 under byo_platform_fee.

    ⚠️ MUTATION TARGET: drop `and not byo_rule_applies` from `force_zero` in
    services.markup.resolve_billed_line and this reads 10.00, not 12.00.
    """
    rule = _rule(tenant_id=T("t1"), byo=True, markup_pct=Decimal("20"))
    line = markup_svc.compute_billed_line_detailed(
        _resolver(rule),
        markup_svc.BILLING_MODE_BYO,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=0.0,
        byo=True,
        project_id=None,
    )
    assert line.billed_usd == Decimal("12.00")
    assert line.applied_pct == 20.0
    # Not a PLATFORM FEE: the fee component exists only while the pct is forced
    # to zero. A marked-up BYO line is ordinary markup revenue.
    assert line.platform_fee_usd == Decimal("0.00")


async def test_byo_line_with_no_byo_rule_keeps_the_forced_zero(db_session):
    """BACKWARD COMPATIBILITY: the historical behaviour, unchanged.

    A byo-agnostic (byo=None) tenant rule still gets 0% on a BYO line, and its
    absolute surcharge is still reported as the platform fee.
    """
    rule = _rule(
        tenant_id=T("t1"),
        markup_pct=Decimal("40"),
        surcharge_output_per_1m_usd=Decimal("3"),
    )
    line = markup_svc.compute_billed_line_detailed(
        _resolver(rule),
        markup_svc.BILLING_MODE_BYO,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=0.0,
        byo=True,
        output_tokens=1_000_000,
        project_id=None,
    )
    assert line.applied_pct == 0.0
    assert line.billed_usd == Decimal("13.00")  # 10*(1+0) + 1M*$3/1M
    assert line.platform_fee_usd == Decimal("3.00")


async def test_byo_false_rule_does_not_lift_the_zero(db_session):
    """A `byo=False` rule is about PLATFORM-PROVIDED lines and must not match a
    BYO line at all — so the forced zero stands and the rule's 40% is not used.

    This is why the check is `rule.byo is True` rather than truthiness: `byo` has
    three meanings, and only one of them is a statement about BYO spend.
    """
    rule = _rule(tenant_id=T("t1"), byo=False, markup_pct=Decimal("40"))
    line = markup_svc.compute_billed_line_detailed(
        _resolver(rule),
        markup_svc.BILLING_MODE_BYO,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=0.0,
        byo=True,
        project_id=None,
    )
    assert line.rule is None  # did not match
    assert line.applied_pct == 0.0
    assert line.billed_usd == Decimal("10.00")


async def test_model_pinned_rule_does_not_outrank_byo_rule(db_session):
    """⭐ THE WEIGHT ORDERING, and it is a commercial invariant.

    `_W_BYO` (64) > `_W_MODEL` (32) so that adding a rate-card row for a new
    model can never silently re-price a tenant's BYO traffic.

    ⚠️ MUTATION TARGET: set `_W_BYO = 8` and the model rule wins, billing 5%.
    (Post-0036 the weights are 64/32/16/8/4/2/1; any `_W_BYO` <= 32 flips it.)
    """
    byo_rule = _rule(tenant_id=T("t1"), byo=True, markup_pct=Decimal("20"))
    model_rule = _rule(tenant_id=T("t1"), model="gpt5", markup_pct=Decimal("5"))
    line = markup_svc.compute_billed_line_detailed(
        _resolver(byo_rule, model_rule),
        markup_svc.BILLING_MODE_BYO,
        tenant_id=T("t1"),
        incurred_usd=Decimal("100"),
        fallback_pct=0.0,
        model="gpt5",
        byo=True,
        project_id=None,
    )
    assert line.rule is byo_rule
    assert line.billed_usd == Decimal("120.00")


async def test_byo_rule_does_not_touch_platform_provided_lines(db_session):
    """The converse containment: a byo=True rule must not match byo=False spend,
    which would apply the BYO contract rate to platform-provided usage."""
    byo_rule = _rule(tenant_id=T("t1"), byo=True, markup_pct=Decimal("20"))
    plain = _rule(tenant_id=T("t1"), markup_pct=Decimal("50"))
    line = markup_svc.compute_billed_line_detailed(
        _resolver(byo_rule, plain),
        markup_svc.BILLING_MODE_STANDARD,
        tenant_id=T("t1"),
        incurred_usd=Decimal("100"),
        fallback_pct=0.0,
        byo=False,
        project_id=None,
    )
    assert line.rule is plain
    assert line.billed_usd == Decimal("150.00")


async def test_byo_pinned_rule_ignored_when_caller_states_no_byo_value(db_session):
    """A caller that does not know whether the line is BYO (the app-request path
    passes no value) must not be served a byo-pinned rule on a guess."""
    rule = _rule(tenant_id=T("t1"), byo=True, markup_pct=Decimal("20"))
    resolved = markup_svc.MarkupResolver([rule]).resolve(
        tenant_id=T("t1"), byo=None, project_id=None
    )
    assert resolved is None


async def test_omitting_byo_entirely_is_not_an_assertion_of_not_byo(db_session):
    """⭐ THE DEFAULT, not the explicit value — this is the half the sibling above
    cannot see.

    `test_byo_pinned_rule_ignored_when_caller_states_no_byo_value` passes
    `byo=None` EXPLICITLY, so it passed both before and after the defect was
    fixed: Python does not enforce annotations, so `None` always worked at
    runtime. What was wrong was the DEFAULT — `compute_billed_line_detailed` was
    annotated `byo: bool = False`, and the app-request caller
    (`services.billing.generate_billing`) passes no `byo` at all. So an
    app-request line silently ASSERTED "not BYO".

    Why that is not a harmless default: `_W_BYO = 64` is the highest specificity
    weight, above `_W_MODEL`, `_W_PROVIDER` and service_key. A tenant-wide
    `byo=False` rule therefore outranks a service_key-scoped rule, and an
    app-request line — which has no provider and no BYO dimension at all — would
    take a rate written for platform-provided GPU spend.

    Measured on prod 2026-09-11: ten `markup_rules` rows, every one `byo` NULL or
    `true`, so this was LATENT — one row away from a live overcharge, not already
    one.

    ⚠️ MUTATION TARGET: restore `byo: bool = False` on
    `services.markup.compute_billed_line_detailed` and this fails while its
    explicit-`None` sibling still passes.
    """
    byo_false_rule = _rule(tenant_id=T("t1"), byo=False, markup_pct=Decimal("500"))
    service_rule = _rule(  # pragma: allowlist secret - "api" is a service_key, not a credential
        tenant_id=T("t1"), service_key="api", markup_pct=Decimal("20")
    )
    resolver = markup_svc.MarkupResolver([byo_false_rule, service_rule])

    # Exactly the app-request call shape: a service_key, no provider, no model,
    # and NO `byo` argument at all.
    line = markup_svc.compute_billed_line_detailed(
        resolver,
        "standard_markup",
        tenant_id=T("t1"),
        incurred_usd=Decimal("100"),
        fallback_pct=0.0,
        service_key="api",  # pragma: allowlist secret
        request_count=10,
        project_id=None,
    )
    # The service_key rule must win. With `byo` defaulting to False the
    # byo=False rule matches at specificity 33 and bills 500% instead.
    assert line.billed_usd == Decimal("120.00"), (
        "an app-request line took a byo-pinned rate; byo must default to None"
    )
    assert line.applied_pct == 20.0


async def test_upsert_byo_rule_does_not_close_the_byo_agnostic_rule(db_session):
    """⭐ THE SILENT ONE. The open-row lookup in `upsert_markup_rule` must scope
    by `byo` too. Without it, writing "BYO = 20%" finds the tenant's general
    open rule, CLOSES it, and returns 201 — retiring the markup on every
    non-BYO line, recorded in audit as an unrelated rule's creation.

    ⚠️ MUTATION TARGET: remove `(MarkupRule.byo, payload.byo)` from the loop in
    services.markup.upsert_markup_rule and `general.effective_until` is set.
    """
    general, prior = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), markup_pct=Decimal("20")),
        ts=TS,
    )
    assert prior is None
    byo_row, prior2 = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), byo=True, markup_pct=Decimal("20")),
        ts=TS + timedelta(minutes=1),
    )
    # The byo-scoped write is a DIFFERENT tuple: it closed nothing...
    assert prior2 is None
    # ...and the general rule is still OPEN.
    assert general.effective_until is None
    assert byo_row.byo is True
    assert byo_row.id != general.id


async def test_upsert_byo_rule_closes_only_the_prior_byo_rule(db_session):
    """Re-pricing BYO closes the previous BYO row and nothing else."""
    general, _ = await markup_svc.upsert_markup_rule(
        db_session, MarkupRuleCreate(tenant_id=T("t1"), markup_pct=Decimal("20")), ts=TS
    )
    first_byo, _ = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), byo=True, markup_pct=Decimal("20")),
        ts=TS,
    )
    second_byo, prior = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), byo=True, markup_pct=Decimal("35")),
        ts=TS + timedelta(days=1),
    )
    assert prior is first_byo
    assert first_byo.effective_until == TS + timedelta(days=1)
    assert general.effective_until is None
    assert second_byo.markup_pct == Decimal("35")


async def test_byo_dimension_raises_specificity_and_is_reported(db_session):
    """`specificity` is disclosed on the API response, so the new weight has to
    show up there — an operator comparing two rules reads that number."""
    plain = _rule(tenant_id=T("t1"), markup_pct=Decimal("20"))
    byo_row = _rule(tenant_id=T("t1"), byo=True, markup_pct=Decimal("20"))
    assert markup_svc.markup_to_response_dict(plain)["specificity"] == 1
    # 64 (byo) + 1 (tenant) = 65. ⚠️ Was 33 pre-0036 — see the doubling note in
    # test_consumption_rule_beats_flat_license. The control is that byo RAISES
    # specificity above a plain tenant rule (asserted as 1 on the line above),
    # not the literal value. ⚠️ `specificity` IS disclosed on the API response,
    # so this number is externally visible; it stays comparative, not absolute.
    assert markup_svc.markup_to_response_dict(byo_row)["specificity"] == 65
    assert markup_svc.markup_to_response_dict(byo_row)["byo"] is True


async def test_specificity_ladder_ordering_is_the_contract_not_the_literals():
    """⭐ The ladder's CONTRACT is an ordering, not a set of magic numbers.

    Two tests in this file assert absolute specificity values (57, 65). Those are
    brittle by construction: 0036 doubled every weight to open the slot between
    LICENSE and TENANT for `project`, and both had to be rewritten. This test pins
    what actually must never change, so the next rescale is caught by MEANING
    rather than by an arithmetic diff.

    The operator's decision (2026-09-15): a project rule overrides the tenant-wide
    default, but a per-licence/app rule still overrides the project rule.
    """
    m = markup_svc
    # 1. Strict descending ladder — each rung outranks every rung below it.
    rungs = _SPEC_RUNGS
    weights = [w for _, w in rungs]
    assert weights == sorted(weights, reverse=True), rungs
    assert len(set(weights)) == len(weights), f"weights must be distinct: {rungs}"

    # 2. ⛔ THE OPERATOR'S RULE: a licence rule still beats project+tenant combined.
    #    Additive scoring means a rung can be outvoted by the SUM of lower rungs —
    #    that is the failure mode a pure "ladder" reading misses.
    assert m._W_LICENSE > m._W_PROJECT + m._W_TENANT

    # 3. A project rule beats a bare tenant-wide default.
    assert m._W_PROJECT > m._W_TENANT

    # 4. Each rung must outrank the SUM of everything strictly below it, which is
    #    what makes the ladder a true precedence order under additive scoring.
    for i, (name, w) in enumerate(rungs[:-1]):
        below = sum(x for _, x in rungs[i + 1 :])
        assert w > below, f"{name}={w} must exceed the sum of all lower rungs ({below})"

    # 5. ⛔ THE SAME LADDER, READ OFF `_specificity` ITSELF — NOT OFF THE CONSTANTS.
    #
    # Sections 1-4 pin the NUMBERS. They do not pin the SCORER, and the gap is
    # not theoretical: a mutant that drops the `project_id` term from
    # `_specificity` leaves every assertion above GREEN, because none of them
    # ever calls it. The constants would still promise a ladder the scorer no
    # longer implements — and the scorer is what actually picks the rate.
    #
    # So re-derive the identical contract from real `MarkupRule` rows scored by
    # the real function. Anything the scorer ignores now shows up here.
    for i, (name, _w) in enumerate(_SPEC_RUNGS[:-1]):
        alone = markup_svc._specificity(_rule(**_SPEC_PINS[name]))
        below_pins = {}
        for lower, _ in _SPEC_RUNGS[i + 1 :]:
            below_pins.update(_SPEC_PINS[lower])
        all_below = markup_svc._specificity(_rule(**below_pins))
        assert alone > all_below, (
            f"_specificity: a rule pinning only {name} scored {alone}, which does "
            f"not outrank a rule pinning every lower rung ({all_below}). The "
            f"constants promise it does — the scorer disagrees."
        )

    # 6. The operator's decision, scored: licence > project > bare tenant.
    tenant_only = markup_svc._specificity(_rule(**_SPEC_PINS["tenant"]))
    tenant_project = markup_svc._specificity(_rule(**_SPEC_PINS["tenant"], **_SPEC_PINS["project"]))
    tenant_licence = markup_svc._specificity(_rule(**_SPEC_PINS["tenant"], **_SPEC_PINS["license"]))
    assert tenant_licence > tenant_project > tenant_only


async def test_specificity_counts_every_rung_it_promises():
    """⭐ Every ladder rung must actually CONTRIBUTE to `_specificity`.

    The sharpest form of the gap above: score a row pinning exactly ONE
    dimension and require it to equal that dimension's weight. A term silently
    dropped from the sum — the `project_id` term is the one that was measured to
    survive the old ladder test — makes its rung score 0 and fails here by name.

    ⚠️ The all-wildcard baseline MUST be 0 and is asserted first: it is
    load-bearing elsewhere, because `compute_billed_line_detailed` decides
    "a real rule matched" with `_specificity(rule) > 0`. If the global-default
    row ever scored above 0 it would stop falling back to the tenant's
    configured markup_pct, silently re-pricing every legacy tenant override.
    """
    assert markup_svc._specificity(_rule()) == 0, (
        "the all-wildcard global default must score 0 — "
        "compute_billed_line_detailed's `> 0` fallback test depends on it"
    )
    for name, weight in _SPEC_RUNGS:
        scored = markup_svc._specificity(_rule(**_SPEC_PINS[name]))
        assert scored == weight, (
            f"_specificity ignores the {name!r} dimension: a rule pinning only "
            f"{name} scored {scored}, expected its weight {weight}. The scorer "
            f"is blind to a rung the constants still advertise."
        )


# --- The kanban-project axis (0036) ------------------------------------------


async def test_project_rule_beats_tenant_wide_rule(db_session):
    """A project-scoped rule overrides the tenant-wide default for its project.

    `_W_PROJECT`(2) > `_W_TENANT`(1), so `tenant+project` (3) outranks a bare
    `tenant` rule (1).
    """
    proj = uuid4()
    await _add(
        db_session,
        _rule(markup_pct=Decimal("30")),  # global default
        _rule(tenant_id=T("t1"), markup_pct=Decimal("20")),  # tenant-wide
        _rule(tenant_id=T("t1"), project_id=proj, markup_pct=Decimal("45")),
    )
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), project_id=proj, provider="openai", ts=TS
    )
    assert row is not None
    assert row.markup_pct == Decimal("45")
    assert markup_svc._specificity(row) == 3  # project 2 + tenant 1


async def test_license_rule_still_beats_project_rule(db_session):
    """⭐ THE OPERATOR'S DECISION (2026-09-15): a per-licence rule outranks a
    project rule even though both also pin the tenant.

    This is NOT implied by the ladder being descending — additive scoring lets a
    rung be outvoted by the SUM of the rungs below it. It holds only because
    `_W_LICENSE`(4) > `_W_PROJECT`(2) + `_W_TENANT`(1) = 3, i.e. by ONE point.

    ⚠️ MUTATION TARGET: set `_W_PROJECT = 4` (or `_W_LICENSE = 2`) in
    services.markup and the project rule wins this resolution at 5 vs 5 / 3 vs 3
    and the assertion below fails.
    """
    lic = uuid4()
    proj = uuid4()
    await _add(
        db_session,
        _rule(tenant_id=T("t1"), markup_pct=Decimal("20")),
        _rule(tenant_id=T("t1"), project_id=proj, markup_pct=Decimal("45")),
        _rule(tenant_id=T("t1"), license_id=lic, markup_pct=Decimal("25")),
    )
    # An event matching BOTH the licence rule and the project rule.
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), license_id=lic, project_id=proj, ts=TS
    )
    assert row is not None
    assert row.markup_pct == Decimal("25"), (
        "a per-licence rule must still outrank a project rule: "
        "_W_LICENSE(4) > _W_PROJECT(2) + _W_TENANT(1)"
    )
    assert markup_svc._specificity(row) == 5  # license 4 + tenant 1
    # The margin is exactly one point — pin it, because that is the whole reason
    # the weights were doubled rather than a project weight of 3 invented.
    assert markup_svc._W_LICENSE == markup_svc._W_PROJECT + markup_svc._W_TENANT + 1


async def test_project_rule_does_not_apply_to_a_different_project(db_session):
    """A rule pinned to project A must not price project B's line."""
    proj_a = uuid4()
    proj_b = uuid4()
    await _add(
        db_session,
        _rule(tenant_id=T("t1"), markup_pct=Decimal("20")),
        _rule(tenant_id=T("t1"), project_id=proj_a, markup_pct=Decimal("45")),
    )
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), project_id=proj_b, ts=TS
    )
    assert row is not None
    assert row.markup_pct == Decimal("20")
    assert row.project_id is None


async def test_project_rule_does_not_apply_when_no_project_is_asserted(db_session):
    """A project-pinned rule cannot match a line that carries NO project.

    Same semantics as a licence-pinned rule against a licence-less event: a
    pinned dimension requires the event to carry exactly that value, and `None`
    is not that value. Guessing otherwise would apply one project's negotiated
    rate to every unattributed line the tenant runs.
    """
    proj = uuid4()
    await _add(
        db_session,
        _rule(tenant_id=T("t1"), markup_pct=Decimal("20")),
        _rule(tenant_id=T("t1"), project_id=proj, markup_pct=Decimal("45")),
    )
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), ts=TS, project_id=None
    )
    assert row is not None
    assert row.markup_pct == Decimal("20")


async def test_tenantless_project_rule_never_matches(db_session):
    """⛔ CROSS-TENANT GUARD. Project ids are minted by kanban, so a rule with a
    set `project_id` and a NULL `tenant_id` would otherwise reprice ANY tenant
    carrying that project id.

    ⚠️ MUTATION TARGET: delete the
    `if row.project_id is not None and row.tenant_id is None: return False`
    guard at the top of services.markup._matches and this resolves to 45.
    """
    proj = uuid4()
    await _add(
        db_session,
        _rule(markup_pct=Decimal("30")),  # global default
        _rule(project_id=proj, markup_pct=Decimal("45")),  # tenantless — inert
    )
    row = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), project_id=proj, ts=TS
    )
    assert row is not None
    assert row.markup_pct == Decimal("30")


async def test_upsert_project_rule_does_not_close_the_tenant_wide_rule(db_session):
    """⭐ THE SILENT-CLOSE REGRESSION — THE TEST THAT WOULD HAVE CAUGHT 0027.

    0027 added `byo` to `ix_markuprules_open` and very nearly shipped without
    adding it to the open-row lookup in `upsert_markup_rule`. That omission is
    SILENT, not an error: the write FINDS the broader open row, CLOSES it, and
    returns 201 with an audit trail recording an unrelated rule's creation.

    `project_id` (0036) has the same failure mode and a WIDER blast radius:
    without `(MarkupRule.project_id, payload.project_id)` in that loop, pricing
    ONE kanban project retires the tenant's baseline markup rule and therefore
    re-prices EVERY OTHER PROJECT for that tenant.

    ⚠️ MUTATION TARGET: remove `(MarkupRule.project_id, payload.project_id)`
    from the loop in services.markup.upsert_markup_rule and
    `tenant_wide.effective_until` is set instead of staying None.
    """
    proj = uuid4()
    # ⚠️ `effective_from` is passed EXPLICITLY. `MarkupRuleCreate` defaults it to
    # `datetime.now(UTC)`, not to the `ts=` close-timestamp, so a row built
    # without it is not yet effective at TS and the resolve below reads a miss
    # (`markup_miss`) rather than the tenant-wide rate — a false green dressed
    # as a false red.
    tenant_wide, prior = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), markup_pct=Decimal("20"), effective_from=TS),
        ts=TS,
    )
    assert prior is None

    project_row, prior2 = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(
            tenant_id=T("t1"),
            project_id=proj,
            markup_pct=Decimal("45"),
            effective_from=TS + timedelta(minutes=1),
        ),
        ts=TS + timedelta(minutes=1),
    )
    # The project-scoped write is a DIFFERENT tuple: it closed nothing...
    assert prior2 is None
    # ...and the tenant-wide rule is STILL OPEN. This is the assertion.
    assert tenant_wide.effective_until is None, (
        "writing a project-scoped rule closed the tenant-wide rule — every "
        "other project for this tenant has just been silently re-priced"
    )
    assert project_row.project_id == proj
    assert project_row.id != tenant_wide.id

    # And prove the consequence directly, not just the column: a line on a
    # DIFFERENT project still resolves to the tenant-wide 20%.
    other = await markup_svc.resolve_markup_rule(
        db_session, tenant_id=T("t1"), project_id=uuid4(), ts=TS + timedelta(minutes=2)
    )
    assert other is not None
    assert other.markup_pct == Decimal("20")


async def test_upsert_project_rule_closes_only_the_prior_rule_for_that_project(db_session):
    """Re-pricing project A closes A's previous row and nothing else — not the
    tenant-wide rule and not project B's rule."""
    proj_a = uuid4()
    proj_b = uuid4()
    tenant_wide, _ = await markup_svc.upsert_markup_rule(
        db_session, MarkupRuleCreate(tenant_id=T("t1"), markup_pct=Decimal("20")), ts=TS
    )
    first_a, _ = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), project_id=proj_a, markup_pct=Decimal("45")),
        ts=TS,
    )
    row_b, _ = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), project_id=proj_b, markup_pct=Decimal("50")),
        ts=TS,
    )
    second_a, prior = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), project_id=proj_a, markup_pct=Decimal("55")),
        ts=TS + timedelta(days=1),
    )
    assert prior is first_a
    assert first_a.effective_until == TS + timedelta(days=1)
    assert tenant_wide.effective_until is None
    assert row_b.effective_until is None
    assert second_a.markup_pct == Decimal("55")


async def test_project_id_round_trips_onto_the_api_response(db_session):
    """`project_id` and the `specificity` it produces must reach the wire.

    `specificity` is already published on `MarkupRuleResponse`, so an operator
    comparing two rules reads that number; a project rule that reported the same
    score as the tenant-wide rule it overrides would be unreadable.
    """
    proj = uuid4()
    row, _ = await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), project_id=proj, markup_pct=Decimal("45")),
        ts=TS,
    )
    # The CREATE schema carried it through to the stored row...
    assert row.project_id == proj
    # ...and the response dict publishes it with the right score.
    dumped = markup_svc.markup_to_response_dict(row)
    assert dumped["project_id"] == proj
    assert dumped["specificity"] == 3  # project 2 + tenant 1
    # Comparative control: the tenant-wide rule it overrides scores lower.
    plain = _rule(tenant_id=T("t1"), markup_pct=Decimal("20"))
    assert markup_svc.markup_to_response_dict(plain)["specificity"] == 1
    # And it really is the MarkupRuleResponse shape, not a loose dict.
    assert MarkupRuleResponse(**dumped).project_id == proj


async def test_project_rule_without_a_tenant_is_refused_at_the_wire():
    """A tenantless project rule is INERT, so it is rejected rather than stored.

    `services.markup._matches` refuses the shape, so such a row would be
    accepted with 201, listed, shown with a non-zero specificity, and never
    match a line — a configured commercial term that silently does nothing.
    """
    with pytest.raises(ValidationError):
        MarkupRuleCreate(project_id=uuid4(), markup_pct=Decimal("45"))
    # Control: the same body WITH a tenant validates, so the refusal is about
    # the missing tenant and not about `project_id` being rejected outright.
    ok = MarkupRuleCreate(tenant_id=T("t1"), project_id=uuid4(), markup_pct=Decimal("45"))
    assert ok.project_id is not None


async def test_markup_history_does_not_mix_project_rules_into_the_tenant_lineage(db_session):
    """The tenant-wide lineage must not absorb project-scoped rows.

    `list_markup_history` matches each dimension in its NULL form; a dimension
    absent from that loop is matched as a WILDCARD instead, which would make a
    project's negotiated rate appear in the history of the tenant's baseline.
    """
    proj = uuid4()
    await markup_svc.upsert_markup_rule(
        db_session, MarkupRuleCreate(tenant_id=T("t1"), markup_pct=Decimal("20")), ts=TS
    )
    await markup_svc.upsert_markup_rule(
        db_session,
        MarkupRuleCreate(tenant_id=T("t1"), project_id=proj, markup_pct=Decimal("45")),
        ts=TS,
    )
    tenant_lineage = await markup_svc.list_markup_history(db_session, tenant_id=T("t1"))
    assert [r.markup_pct for r in tenant_lineage] == [Decimal("20")]
    assert all(r.project_id is None for r in tenant_lineage)

    project_lineage = await markup_svc.list_markup_history(
        db_session, tenant_id=T("t1"), project_id=proj
    )
    assert [r.markup_pct for r in project_lineage] == [Decimal("45")]


# --------------------------------------------------------------------------- #
# ⭐ MULTI-LINE ROUNDING — the contract the 54 tests above could not see
# --------------------------------------------------------------------------- #
#
# Every markup test written before 2026-09-15 priced a SINGLE cost line, and on a
# single line "quantise each line" and "quantise the total" are the same
# arithmetic. So the per-line rounding this module used to do had NO test at this
# layer at all: it could be deleted and all 54 stayed green. These tests are the
# ones that would have caught it, and they are all multi-line by construction.
#
# ⛔ Rounding does not distribute over addition. That is the whole defect: it made
# the bill a function of how the same spend was SPLIT into rollup rows rather than
# of what the spend was — and a rollup key gaining a column (as `project_id` did
# in #206) re-splits every tenant's lines without anyone touching a rate.


def _sum_billed(rows, resolver, **kw) -> Decimal:
    """Σ of the billed figures for `rows` — deliberately UNquantised."""
    total = Decimal("0")
    for incurred in rows:
        total += markup_svc.compute_billed_line_detailed(
            resolver,
            markup_svc.BILLING_MODE_STANDARD,
            tenant_id=T("t1"),
            incurred_usd=incurred,
            fallback_pct=0.0,
            project_id=None,
            **kw,
        ).billed_usd
    return total


async def test_twenty_sub_cent_lines_bill_the_money_they_are_worth(db_session):
    """20 x $0.001 at 50% is $0.03, and must not round away to $0.00.

    ⭐ THE REGRESSION, EXACTLY. Each line bills 0.0015; a per-line quantum turns
    every one of them into 0.00 and the invoice charges NOTHING for three cents of
    real, marked-up spend. Quantising the SUM once charges 0.03. Neither figure is
    a rounding preference — one of them is the money.
    """
    await _add(db_session, _rule(tenant_id=T("t1"), markup_pct=Decimal("50")))
    resolver = await markup_svc.build_markup_resolver(db_session, tenant_id=T("t1"), ts=TS)

    lines = [Decimal("0.001")] * 20
    billed = _sum_billed(lines, resolver)

    assert billed == Decimal("0.030"), billed
    assert markup_svc.quantize_money(billed) == Decimal("0.03")
    # The control that names the defect: had each line been quantised first, the
    # twenty of them would have summed to nothing at all.
    per_line = sum((markup_svc.quantize_money(b) for b in [Decimal("0.0015")] * 20), Decimal("0"))
    assert per_line == Decimal("0.00")


async def test_splitting_one_line_into_many_does_not_change_the_bill(db_session):
    """The same spend, one line or twelve, bills the same money.

    ⛔ THIS IS THE INVARIANT, and the per-line quantum broke it in BOTH
    directions — a split could round the bill down (sub-cent lines vanishing) or
    up (twelve half-cents each rounding away from zero). A rollup regrouping is
    not a repricing event and must never read as one.
    """
    await _add(db_session, _rule(tenant_id=T("t1"), markup_pct=Decimal("35")))
    resolver = await markup_svc.build_markup_resolver(db_session, tenant_id=T("t1"), ts=TS)

    whole = _sum_billed([Decimal("7.777777")], resolver)
    split = _sum_billed([Decimal("7.777777") / 12] * 12, resolver)

    assert markup_svc.quantize_money(whole) == markup_svc.quantize_money(split)
    assert markup_svc.quantize_money(whole) == Decimal("10.50")


async def test_many_half_cent_lines_do_not_each_round_away_from_zero(db_session):
    """Nine lines that each land on a true half-cent bill 0.045, not 0.09.

    ROUND_HALF_UP is the right mode and is NOT what is on trial here: applied
    nine times it is applied to nine numbers that were never money. 9 x 0.005
    is four and a half cents; per-line rounding bills nine — a 100% overcharge
    built entirely out of correct roundings of the wrong quantities.
    """
    await _add(db_session, _rule(tenant_id=T("t1"), markup_pct=Decimal("0")))
    resolver = await markup_svc.build_markup_resolver(db_session, tenant_id=T("t1"), ts=TS)

    billed = _sum_billed([Decimal("0.005")] * 9, resolver)

    assert billed == Decimal("0.045")
    assert markup_svc.quantize_money(billed) == Decimal("0.05")
    per_line = sum((markup_svc.quantize_money(Decimal("0.005")) for _ in range(9)), Decimal("0"))
    assert per_line == Decimal("0.09"), "control: the per-line path doubles the charge"


async def test_a_billed_line_is_returned_unquantised(db_session):
    """The primitive itself must hand back full precision — the direct contract.

    The three tests above would also pass if the line were quantised to some
    finer-than-cent scale, so this pins the actual rule: `compute_billed_usd`
    and `compute_billed_line_detailed` round NOTHING. Whoever emits a figure
    rounds it, once.
    """
    rule = _rule(tenant_id=T("t1"), markup_pct=Decimal("50"))
    await _add(db_session, rule)
    resolver = await markup_svc.build_markup_resolver(db_session, tenant_id=T("t1"), ts=TS)

    assert markup_svc.compute_billed_usd(rule, incurred_usd=Decimal("0.001")) == Decimal("0.0015")
    line = markup_svc.compute_billed_line_detailed(
        resolver,
        markup_svc.BILLING_MODE_STANDARD,
        tenant_id=T("t1"),
        incurred_usd=Decimal("0.001"),
        fallback_pct=0.0,
        project_id=None,
    )
    assert line.billed_usd == Decimal("0.0015")

    # The fallback branch (no rule more specific than the global default) is a
    # SECOND implementation of the same arithmetic and rounded on its own line
    # too, so it needs its own assertion or half the money path is unpinned.
    empty = await markup_svc.build_markup_resolver(db_session, tenant_id=T("nobody"), ts=TS)
    fallback = markup_svc.compute_billed_line_detailed(
        empty,
        markup_svc.BILLING_MODE_STANDARD,
        tenant_id=T("nobody"),
        incurred_usd=Decimal("0.001"),
        fallback_pct=50.0,
        project_id=None,
    )
    assert fallback.billed_usd == Decimal("0.0015")

    # And pass-through, the third branch that used to quantise.
    passthru = markup_svc.compute_billed_line_detailed(
        empty,
        markup_svc.BILLING_MODE_PASS_THROUGH,
        tenant_id=T("nobody"),
        incurred_usd=Decimal("0.001234"),
        fallback_pct=50.0,
        project_id=None,
    )
    assert passthru.billed_usd == Decimal("0.001234")
