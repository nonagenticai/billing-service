"""``services.invoices`` — the invoice as a persisted OBJECT, exercised for real.

Everything here gates a claim currently asserted only in a commit message or a
docstring: that :func:`reproduce` never re-derives a number, that the
disclosure columns are disclosure and not a charge, that the invoice-number
series cannot gap even under a rollback, that :func:`assert_no_bucket_gap`
detects exactly the absence it documents (and nothing more), that
:func:`assert_all_service_keys_priced` refuses an unpriced ``service_key``
without refusing a deliberately free one, that
:func:`assert_byo_mode_available` refuses a close whose billing mode licences
cannot state, and that closing or issuing twice is a true no-op.

See ``src/services/invoices.py`` for the reasoning this file holds in place.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import func, select

from models.db import (
    ATTRIBUTION_UNLICENSED,
    NULL_LICENSE_ID,
    BillingInvoice,
    BillingInvoiceCounter,
    CostDaily,
    MarkupRule,
    TenantAccessCharge,
    TenantConfig,
)
from routers.billing import _invoice_response
from services.billing import ACCESS_BASIS_NO_MATCH, generate_billing
from services.invoices import (
    _CHARGED_MONEY_COLUMNS,
    _MONEY_COLUMNS,
    _TOTAL_COMPONENT_COLUMNS,
    _TOTAL_IDENTITY_TOLERANCE_USD,
    BucketGapError,
    InvoiceMoneyImbalanceError,
    PeriodNotBillableError,
    TenantNotInvoiceableError,
    _bill_ts,
    _counter_scope,
    assert_no_bucket_gap,
    close_period,
    issue,
    reproduce,
)
from services.markup import MARKUP_SOURCE_SETTINGS, quantize_money
from services.tenant_names import TenantRegistryUnavailableError
from tests.conftest import T

# A month with the fewest days available (2026 is not a leap year), to keep the
# per-day bucket-gap padding this file has to seed as small as it can be.
_MONTH_START = date(2026, 2, 1)
_MONTH_END = date(2026, 3, 1)
_MONTH = "2026-02"


async def _seed_full_month(
    session,  # noqa: ANN001
    *,
    tenant_id: str,
    license_id: uuid.UUID,
    daily_cost_usd: Decimal = Decimal("2.500000"),
    service_key: str = "workers",
    provider: str = "test",
    month_start: date = _MONTH_START,
    month_end: date = _MONTH_END,
) -> None:
    """One CostDaily row per calendar day of the month, all on ONE billed line.

    ``assert_no_bucket_gap`` refuses to close a period with even one entirely
    absent day (fleet-wide), so every test that actually calls
    :func:`close_period` has to cover the whole month it closes -- there is no
    smaller fixture that satisfies the real gate.
    """
    d = month_start
    while d < month_end:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                license_id=license_id,
                service_key=service_key,
                provider=provider,
                bucket_date=d,
                total_input_tokens=1_000,
                total_output_tokens=200,
                total_cost_usd=daily_cost_usd,
                request_count=10,
            )
        )
        d += timedelta(days=1)
    await session.flush()


def _license_payload(license_id: uuid.UUID) -> list[dict]:
    return [
        {
            "id": str(license_id),
            "app": {"key": "workers"},
            "plan": {"code": "pro", "quota_monthly": None},
        }
    ]


def _tenant_scoped_markup_rule(tenant_id: str, pct: str) -> MarkupRule:
    """A rule pinned to ``tenant_id`` -- specificity > 0, so it actually wins.

    ⚠️ An all-wildcard (every dimension NULL) rule would NOT do this: per
    ``services.markup.compute_billed_line``, a rule with specificity 0 is
    treated the same as a miss and the line falls back to
    ``tenant_config.markup_pct`` instead. Pinning ``tenant_id`` is the smallest
    change that makes the seeded rule the one actually applied.
    """
    return MarkupRule(
        tenant_id=tenant_id,
        markup_pct=Decimal(pct),
        effective_from=datetime(2025, 1, 1, tzinfo=UTC),
        effective_until=None,
    )


def _draft_invoice(tenant_id: str, period_start: date, period_end: date) -> BillingInvoice:
    """The minimum valid row for exercising :func:`issue` directly, bypassing
    ``close_period`` (and its licences call and month-long bucket-gap
    requirement) for tests that are about the counter, not the generator."""
    return BillingInvoice(
        tenant_id=tenant_id,
        tenant_name="acme",
        period_start=period_start,
        period_end=period_end,
        status="draft",
        subtotal_usd=Decimal("0"),
        total_usd=Decimal("0"),
        internal_cost_usd=Decimal("0"),
        margin_usd=Decimal("0"),
        billing_mode="standard_markup",
        markup_pct=Decimal("30.00"),
        effective_markup_pct=Decimal("30.0000"),
        snapshot={},
        inputs={},
        snapshot_sha256="0" * 64,
        generator_version="invoice.v1",
    )


@pytest.fixture(autouse=True)
def _licences_answers_byo_mode(monkeypatch):
    """Answer the licences ``byo-mode`` endpoint, and isolate its TTL cache.

    ⚠️ THIS IS A DEPENDENCY THIS FILE ALREADY HAD AND WAS SATISFYING BY ACCIDENT.
    ``close_period`` resolves the tenant's ``byo_mode`` twice (``_capture_inputs``
    and ``generate_billing``). ``test_settings.licenses_url`` points at
    ``http://localhost:9999``, where nothing listens, so every one of those calls
    was a real ``ConnectError`` silently absorbed by ``get_byo_mode``'s
    revenue-safe default — i.e. every close in this file has been running
    against a *guessed* billing mode. Now that ``assert_byo_mode_available``
    refuses that guess, the dependency has to be stated, which is an improvement
    on its own: a close here now exercises the real 200 path.

    ⚠️ URL-SCOPED AND DELEGATING, not a blanket stub. Every other GET — notably
    ``fetch_licenses`` (patched per-test, see ``_patch_fetch_licenses``) and
    ``get_tenant_names`` (which this file deliberately lets degrade) — is handed
    to the real method unchanged, so this fixture cannot silently repair a
    different test's expectations. Mirrors the URL-aware ``seats_say`` stub in
    ``tests/test_commercial_terms.py`` and exists for the same reason.

    The cache is cleared both sides: it is module state with a 5-min TTL, so one
    test's answer must not decide another's — and in particular must not let a
    cached real answer satisfy ``test_close_period_refuses_when_licences_cannot_
    state_the_billing_mode``.
    """
    from services import byo_mode as byo

    byo.clear_byo_cache()
    real_get = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if "byo-mode" in str(url):
            return httpx.Response(200, json={"byo_mode": "byo_required"})
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    yield
    byo.clear_byo_cache()


def _patch_fetch_licenses(payload: list[dict]):  # noqa: ANN201
    """Patch BOTH bindings of ``fetch_licenses``.

    ``services.invoices`` holds one (close_period's own pre-fetch) and
    ``services.billing`` holds the other (generate_billing's). Both are patched
    because leaving either unpatched sends that call to the real network.

    ⚠️ close_period now passes its pre-fetched payload INTO generate_billing, so
    a close should drive the ``services.billing`` binding ZERO times — see
    ``test_close_period_fetches_licences_exactly_once``. The second patch stays
    as a net: if the single-fetch property regresses, that test fails on the
    call count rather than the suite failing on a live HTTP call.
    """
    close_patch = patch("services.invoices.fetch_licenses", new_callable=AsyncMock)
    gen_patch = patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    return close_patch, gen_patch


# ---------------------------------------------------------------------------
# A. Reproducibility — the load-bearing property
# ---------------------------------------------------------------------------


async def test_reproduce_survives_a_markup_rule_mutation(db_session, test_settings) -> None:
    tenant = T("invoice-repro")
    license_id = uuid.uuid4()

    rule = _tenant_scoped_markup_rule(tenant, "30.00")
    db_session.add(rule)
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
        )
        assert inv.status == "issued"
        invoice_id = inv.id

        # Baseline captured from a ROUND-TRIPPED read (commit + expire + reload),
        # not from the in-memory pre-insert dict: jsonb does not guarantee to
        # preserve a JSON number's original textual form (e.g. `30.0` vs `30`),
        # so comparing an in-memory dict against a later jsonb-round-tripped one
        # could report a spurious "change" that is really just storage
        # normalisation. Reading the baseline back through the same round trip
        # the later comparison also goes through makes this a fair test of
        # whether the STORED value moved, not of jsonb's formatting.
        await db_session.commit()
        db_session.expire_all()
        baseline = await db_session.get(BillingInvoice, invoice_id)
        stored_snapshot_json = json.dumps(baseline.snapshot, sort_keys=True)
        stored_sha = baseline.snapshot_sha256

        fresh_before = await generate_billing(tenant, _MONTH, db_session, test_settings)

        # Mutate the rate basis the invoice was already computed from: close
        # the 30% rule before bill_ts and open a 90% one in its place.
        rule.effective_until = _bill_ts(_MONTH_END)
        db_session.add(_tenant_scoped_markup_rule(tenant, "90.00"))
        await db_session.commit()
        db_session.expire_all()

        reloaded = await db_session.get(BillingInvoice, invoice_id)
        assert json.dumps(reloaded.snapshot, sort_keys=True) == stored_snapshot_json, (
            "the stored snapshot moved when the rate basis it was computed "
            "from changed -- it is not a record if it can still be edited "
            "by editing something else"
        )
        assert reloaded.snapshot_sha256 == stored_sha

        reproduced = await reproduce(db_session, invoice_id=invoice_id)
        assert reproduced["snapshot"] == reloaded.snapshot
        assert reproduced["snapshot_sha256"] == stored_sha
        assert reproduced["digest_matches"] is True

        fresh_after = await generate_billing(tenant, _MONTH, db_session, test_settings)

    # ⛔ Without this the test is vacuous — it passes on a mutation that never
    # took effect. The stored snapshot being unchanged proves nothing about
    # whether the mutation was real unless a FRESH computation over the same
    # month demonstrably disagrees with the one taken before the mutation.
    assert (
        fresh_after["totals"]["customer_cost_usd"] != fresh_before["totals"]["customer_cost_usd"]
    ), "the markup mutation had no effect on a fresh render — the fixture never moved anything"


# ---------------------------------------------------------------------------
# B. Disclosure is disclosed, not charged
# ---------------------------------------------------------------------------


async def test_disclosure_does_not_change_the_bill(db_session, test_settings) -> None:
    tenant_without = T("disclosure-without")
    tenant_with = T("disclosure-with")
    license_a = uuid.uuid4()
    license_b = uuid.uuid4()

    for tenant, license_id in ((tenant_without, license_a), (tenant_with, license_b)):
        db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
        await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)

    # The disclosed-but-unbillable row: no license reached billing, and the
    # rollup says so explicitly via attribution_status, not merely via a NULL.
    db_session.add(
        CostDaily(
            tenant_id=tenant_with,
            license_id=NULL_LICENSE_ID,
            service_key="unattributed",
            provider="test",
            bucket_date=_MONTH_START + timedelta(days=5),
            total_input_tokens=0,
            total_output_tokens=0,
            total_cost_usd=Decimal("1.0008"),
            request_count=0,
            attribution_status=ATTRIBUTION_UNLICENSED,
        )
    )
    await db_session.commit()

    close_patch_a, gen_patch_a = _patch_fetch_licenses(_license_payload(license_a))
    with close_patch_a as mock_a1, gen_patch_a as mock_a2:
        mock_a1.return_value = _license_payload(license_a)
        mock_a2.return_value = _license_payload(license_a)
        inv_without = await close_period(
            db_session,
            tenant_id=tenant_without,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
        )

    close_patch_b, gen_patch_b = _patch_fetch_licenses(_license_payload(license_b))
    with close_patch_b as mock_b1, gen_patch_b as mock_b2:
        mock_b1.return_value = _license_payload(license_b)
        mock_b2.return_value = _license_payload(license_b)
        inv_with = await close_period(
            db_session,
            tenant_id=tenant_with,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
        )

    assert inv_with.excluded_unbillable_usd == Decimal("1.0008")
    assert inv_with.excluded_unbillable_rows == 1
    # ⚠️ THE BRIEF FOR THIS TEST SAID "the row count lands in excluded_by_status".
    # That is WRONG against the real code and is not implemented here:
    # close_period writes `excluded_by_status={k: str(v["usd"]) for k, v in
    # excluded["by_status"].items()}` -- USD only, per status. The per-status
    # ROW COUNT is computed by measure_excluded_unbillable but never persisted
    # anywhere; only the fleet-wide total (excluded_unbillable_rows, asserted
    # above) survives onto the row.
    assert Decimal(inv_with.excluded_by_status["unlicensed"]) == Decimal("1.0008")

    # The load-bearing assertion: disclosure must not move the bill.
    assert inv_with.subtotal_usd == inv_without.subtotal_usd
    assert inv_with.total_usd == inv_without.total_usd
    # Mutant to catch: measure_excluded_unbillable pointed at
    # CostDaily.license_attributed_clause() instead of its complement would
    # measure the BILLED population, not the excluded one -- it would either
    # report zero excluded rows here (a real miss) or, worse, fold this row's
    # cost into subtotal_usd/total_usd, turning "disclosed" into "charged".
    # Either way this test goes red.


# ---------------------------------------------------------------------------
# C. Gapless numbering
# ---------------------------------------------------------------------------


async def test_issue_numbering_survives_a_rolled_back_allocation(db_session) -> None:
    tenant = T("gapless-numbering")
    year = 2026

    inv_a = _draft_invoice(tenant, date(year, 1, 1), date(year, 2, 1))
    inv_b = _draft_invoice(tenant, date(year, 2, 1), date(year, 3, 1))
    db_session.add_all([inv_a, inv_b])
    await db_session.flush()

    issued_a = await issue(db_session, invoice_id=inv_a.id)
    issued_b = await issue(db_session, invoice_id=inv_b.id)
    assert issued_a.invoice_number == f"INV-{year}-000001"
    assert issued_b.invoice_number == f"INV-{year}-000002"

    inv_c = _draft_invoice(tenant, date(year, 3, 1), date(year, 4, 1))
    db_session.add(inv_c)
    await db_session.flush()
    # ⚠️ Capture the id HERE — before the savepoint, not after it. Rolling back a
    # begin_nested() expires every object that was modified inside it, and issue()
    # modifies inv_c, so by the time the block exits `inv_c.id` is already expired.
    # Touching it then is a lazy refresh, i.e. synchronous IO in an async test, and
    # raises MissingGreenlet — failing this test for a reason unrelated to numbering.
    # Capturing after the rollback is too late; that was the first attempt at this.
    inv_c_id = inv_c.id

    class _AbortError(Exception):
        pass

    with pytest.raises(_AbortError):
        async with db_session.begin_nested():
            await issue(db_session, invoice_id=inv_c.id)
            raise _AbortError

    db_session.expire_all()
    reloaded_c = await db_session.get(BillingInvoice, inv_c_id)
    assert reloaded_c.status == "draft", "the rolled-back issue must not have persisted"
    assert reloaded_c.invoice_number is None

    inv_d = _draft_invoice(tenant, date(year, 4, 1), date(year, 5, 1))
    db_session.add(inv_d)
    await db_session.flush()
    issued_d = await issue(db_session, invoice_id=inv_d.id)

    # A SEQUENCE would have burned 000003 on the rolled-back attempt and moved
    # straight on to 000004 -- that is the mutant this pins. The counter is an
    # ordinary row incremented under FOR UPDATE precisely so a rollback takes
    # the increment with it.
    assert issued_d.invoice_number == f"INV-{year}-000003"


async def test_two_legal_entities_do_not_collide_on_the_global_number_index(db_session) -> None:
    """Two series, same year, both drawing seq 1 — and still distinct numbers.

    ⚠️ THIS IS A FUTURE-PROOFING TEST FOR A DEFECT THAT CANNOT BITE TODAY, and it
    is here precisely because of that. ``legal_entity`` is part of the
    present-and-null legal set, so every invoice currently shares one series and
    nothing collides. But the counter is keyed per ``(series, year)`` while
    ``ux_billing_invoices_number`` is GLOBALLY unique: a number that omitted the
    series would render identically for the first invoice of every entity, and
    the second ``issue()`` would die on that index.

    That would turn filling in ``legal_entity`` — the whole point of the
    present-and-null design — from a fill-in into a migration against rows that
    are immutable by design. The mutant: render the number without ``series``.
    """
    year = 2031
    a = _draft_invoice(T("entity-a"), date(year, 1, 1), date(year, 2, 1))
    a.legal_entity = "ACME"
    b = _draft_invoice(T("entity-b"), date(year, 1, 1), date(year, 2, 1))
    b.legal_entity = "GLOBEX"
    db_session.add_all([a, b])
    await db_session.flush()

    issued_a = await issue(db_session, invoice_id=a.id)
    issued_b = await issue(db_session, invoice_id=b.id)

    # Both are seq 1 of their OWN series — the counters are independent...
    assert issued_a.invoice_number == f"ACME-{year}-000001"
    assert issued_b.invoice_number == f"GLOBEX-{year}-000001"
    # ...and the globally unique index is satisfied because the series is IN the
    # rendered string. Flushing is what would raise if it were not.
    assert issued_a.invoice_number != issued_b.invoice_number
    await db_session.flush()

    # And an unset legal_entity still renders the default series, so today's
    # numbers are unchanged by the fix that made the above possible.
    c = _draft_invoice(T("entity-none"), date(year, 1, 1), date(year, 2, 1))
    db_session.add(c)
    await db_session.flush()
    issued_c = await issue(db_session, invoice_id=c.id)
    assert issued_c.invoice_number == f"INV-{year}-000001"


# ---------------------------------------------------------------------------
# D. assert_no_bucket_gap
# ---------------------------------------------------------------------------


async def test_assert_no_bucket_gap_names_the_missing_date(db_session) -> None:
    """A SYNTHETIC absent bucket, deliberately not coupled to any real date.

    ⚠️ 2026-09-05 is the real, measured case this check CANNOT catch (a bucket
    written short rather than entirely absent -- billing_cost_events was
    TRUNCATE'd mid-day while the rollup still wrote a normal-looking row count).
    Mentioned here only as the boundary of what this test's technique reaches;
    the fixture below never touches it, and another lane is actively
    diagnosing the neighbouring 2026-09-02/03/04 dates, whose data may still
    move.
    """
    tenant = T("bucket-gap")
    period_start = date(2026, 5, 10)
    period_end = date(2026, 5, 15)  # exclusive: 10, 11, 12, 13, 14
    missing = date(2026, 5, 12)

    for d in (date(2026, 5, 10), date(2026, 5, 11), date(2026, 5, 13), date(2026, 5, 14)):
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=uuid.uuid4(),
                service_key="probe",
                provider="test",
                bucket_date=d,
                total_input_tokens=0,
                total_output_tokens=0,
                total_cost_usd=Decimal("0"),
                request_count=0,
            )
        )
    await db_session.commit()

    with pytest.raises(BucketGapError) as exc:
        await assert_no_bucket_gap(
            db_session, tenant_id=tenant, period_start=period_start, period_end=period_end
        )
    assert exc.value.missing_dates == [missing]
    assert missing.isoformat() in str(exc.value)

    # Fill the gap: the same period is now contiguous and must not raise.
    db_session.add(
        CostDaily(
            tenant_id=tenant,
            license_id=uuid.uuid4(),
            service_key="probe",
            provider="test",
            bucket_date=missing,
            total_input_tokens=0,
            total_output_tokens=0,
            total_cost_usd=Decimal("0"),
            request_count=0,
        )
    )
    await db_session.commit()

    await assert_no_bucket_gap(
        db_session, tenant_id=tenant, period_start=period_start, period_end=period_end
    )


async def test_close_period_bucket_gap_is_a_labelled_refusal_not_an_outage(
    db_session, test_settings
) -> None:
    """A gapped period's refusal must be COUNTABLE and DISTINCT from an outage.

    ⭐ WHY THIS MATTERS TODAY: INVOICE_CLOSE_ENABLED=true is being armed while
    the billing basis is truncated (2026-09-17), so close will correctly
    REFUSE every current period via ``BucketGapError``. Before this change
    that refusal folded only into the unlabelled aggregate
    ``billing_invoice_close_failures_total{reason="tenant_refused"}`` —
    identical to what a licences outage produces. An operator watching only
    that aggregate cannot tell "correctly declined" from "broken".

    Three properties, all in one close attempt so they cannot drift apart:

    1. ``BucketGapError`` is raised (the existing, unchanged behaviour).
    2. ``billing_invoice_close_refused_total{reason="bucket_gap"}`` increments
       by exactly 1, while the sibling ``{reason="licenses_unavailable"}``
       (a gate this close never reaches — bucket_gap fires first) does not
       move at all, proving the label is SPECIFIC rather than a second copy
       of the aggregate.
    3. ``billing_invoice_counters`` gains no row for this period's year: the
       refusal fires at :func:`assert_no_bucket_gap`, strictly before
       ``issue``'s ``FOR UPDATE`` draw, so a gapped close must never burn (or
       even seed) a number from the gapless series.

    MUTATION CONTROL: comment out the
    ``observability_metrics.invoice_close_refused_total.labels(
    reason="bucket_gap").inc()`` line inside ``assert_no_bucket_gap``
    (``src/services/invoices.py``) and property 2's first assertion goes red
    (``0.0 == before + 1.0``) while the raise in property 1 stays green —
    proving this test checks the METRIC, not merely the exception.
    """
    import observability.metrics as metrics

    tenant = T("bucket-gap-refusal-metric")
    # A period nothing else in this suite seeds, so the fleet-wide presence
    # query in `assert_no_bucket_gap` sees zero rows for every day in it —
    # i.e. the whole period is "missing", which is sufficient to raise; the
    # test's subject is the refusal's SIDE EFFECTS, not the gap-detection
    # logic already proven above.
    period_start = date(2031, 1, 1)
    period_end = date(2031, 2, 1)

    def _refused(reason: str) -> float:
        return metrics.invoice_close_refused_total.labels(reason=reason)._value.get()

    bucket_gap_before = _refused("bucket_gap")
    licenses_unavailable_before = _refused("licenses_unavailable")
    counters_before = (
        await db_session.execute(
            select(func.count())
            .select_from(BillingInvoiceCounter)
            .where(BillingInvoiceCounter.period_year == period_start.year)
        )
    ).scalar_one()

    with pytest.raises(BucketGapError):
        await close_period(
            db_session,
            tenant_id=tenant,
            period_start=period_start,
            period_end=period_end,
            settings=test_settings,
        )

    assert _refused("bucket_gap") == bucket_gap_before + 1, (
        "BucketGapError fired without incrementing its own labelled reason"
    )
    assert _refused("licenses_unavailable") == licenses_unavailable_before, (
        "a gate the close never reached moved a metric it does not own"
    )

    counters_after = (
        await db_session.execute(
            select(func.count())
            .select_from(BillingInvoiceCounter)
            .where(BillingInvoiceCounter.period_year == period_start.year)
        )
    ).scalar_one()
    assert counters_after == counters_before, (
        "a refused close seeded or advanced the gapless invoice-number counter"
    )

    rows = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == tenant)
            )
        )
        .scalars()
        .all()
    )
    assert rows == [], "a refused close still wrote an invoice row"


# ---------------------------------------------------------------------------
# E. Idempotency
# ---------------------------------------------------------------------------


async def test_close_period_twice_is_a_true_no_op(db_session, test_settings) -> None:
    tenant = T("idempotent-close")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        first = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
        )
        calls_after_first = mock_close_fetch.call_count
        gen_calls_after_first = mock_gen_fetch.call_count
        assert calls_after_first == 1

        second = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
        )

        # No recomputation: find_open short-circuits before either fetch_licenses
        # call is ever reached a second time.
        assert mock_close_fetch.call_count == calls_after_first, (
            "close_period recomputed on the second call instead of returning the existing invoice"
        )
        # ⚠️ Compared against what it was after the FIRST close, not against a
        # literal. This read `== 1`, which pinned an incidental implementation
        # detail — that generate_billing fetched licences for itself — rather
        # than the property under test. Threading the pre-fetched payload through
        # made the true count 0 and reddened an idempotency test that the change
        # did not affect. The property is "the second close adds no calls"; the
        # absolute number is not this test's business.
        assert mock_gen_fetch.call_count == gen_calls_after_first, (
            "the second close drove generate_billing's licences fetch again"
        )

    assert second.id == first.id
    assert second.invoice_number == first.invoice_number

    rows = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == tenant)
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 1, "a second close_period wrote a second row"


async def test_issue_twice_is_a_true_no_op(db_session) -> None:
    tenant = T("idempotent-issue")
    inv = _draft_invoice(tenant, date(2026, 6, 1), date(2026, 7, 1))
    db_session.add(inv)
    await db_session.flush()

    first = await issue(db_session, invoice_id=inv.id)
    scope, _series, _year = _counter_scope(first)
    next_seq_after_first = (
        await db_session.execute(
            select(BillingInvoiceCounter.next_seq).where(BillingInvoiceCounter.scope == scope)
        )
    ).scalar_one()

    second = await issue(db_session, invoice_id=inv.id)
    next_seq_after_second = (
        await db_session.execute(
            select(BillingInvoiceCounter.next_seq).where(BillingInvoiceCounter.scope == scope)
        )
    ).scalar_one()

    assert second.invoice_number == first.invoice_number
    assert next_seq_after_second == next_seq_after_first, (
        "issue() drew a second number on a no-op re-issue"
    )


async def test_close_period_fetches_licences_exactly_once(db_session, test_settings) -> None:
    """One close, one licences call — and the pair it freezes cannot disagree.

    close_period used to fetch licences TWICE: once to freeze into
    ``inputs.licenses_response``, and again inside ``generate_billing``. A blip
    between the two froze a record whose ``inputs`` said licences were readable
    while the snapshot's labelling had degraded to ``[]``, with nothing recording
    that it had happened.

    ⚠️ No MONEY figure ever differed — ``license_map`` only enriches labels, and
    the figures come from CostDaily/MeteringDaily — which is exactly why this
    could sit there unnoticed. A statement that exists to be reproducible must
    not be internally inconsistent about what it read, even in metadata.

    THE MUTANT: drop ``licenses_data=licenses_response`` from close_period's
    generate_billing call. The generator then fetches for itself, the billing
    binding is driven once, and this goes red.
    """
    tenant = T("invoice-single-fetch")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    assert mock_close_fetch.call_count == 1, "close_period must pre-fetch exactly once"
    assert mock_gen_fetch.call_count == 0, (
        "generate_billing fetched licences for itself during a close; the pre-fetched "
        "payload was not threaded through, so inputs and snapshot can disagree"
    )

    # Non-vacuity: the licences actually reached the frozen record, so a zero
    # call count means "threaded through", not "never asked for".
    assert inv.inputs["licenses_response"], "the frozen inputs carry no licences at all"


# ---------------------------------------------------------------------------
# F. An absent line is not a zero line
# ---------------------------------------------------------------------------


async def test_zero_platform_fee_and_markup_provenance_reach_the_response(
    db_session, test_settings
) -> None:
    """⭐ AN ABSENT LINE IS NOT A ZERO LINE — asserted on what the CUSTOMER SEES.

    Seeds the production shape EXACTLY: zero ``tenant_access_charge`` rows, zero
    OPEN ``markup_rules``, zero ``tenant_config`` rows. So the platform fee is
    genuinely $0 and the 30% markup genuinely comes from
    ``settings.default_markup_pct`` — a code constant, not a commercial rule.

    ⚠️ THE ASSERTION TARGET IS THE ``InvoiceResponse``, NOT THE DB ROW, AND THAT
    IS THE ENTIRE POINT. ``billing_invoices.access_charge_usd`` and
    ``platform_fee_usd`` already exist with ``server_default '0'``, so a test
    that reads the row passes on unmodified ``main`` and proves nothing: the
    columns were populated and simply never projected onto the wire model, so
    close/issue/void returned a statement with no platform-fee field at all. A
    bill you cannot read the fee off cannot be audited or reconciled, whatever
    the table says.

    What this pins:
      1. all four money/markup fields are PRESENT and TYPED on the response;
      2. the fee is an explicit ``0.00``, not an omission;
      3. the markup names its SOURCE, and that source is honestly the settings
         constant rather than a rule nobody has opened;
      4. the itemised ``access_charges`` array agrees with the aggregate — it
         carries one explicit zero line instead of being empty.
    """
    tenant = T("invoice-always-present-lines")
    license_id = uuid.uuid4()

    # ⛔ SEED NO PRICING ROWS. No TenantAccessCharge, no MarkupRule, no
    # TenantConfig — this is the measured production state (2026-09-08), and
    # seeding any of them would make the test pass for a reason prod does not
    # enjoy. Only the usage the month actually incurred is seeded.
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    # Control: prove the emptiness the test's premise rests on, rather than
    # assuming it. An empty result from a table someone else seeded would let
    # every assertion below pass for the wrong reason.
    async def _count(model) -> int:  # noqa: ANN001
        return (await db_session.execute(select(func.count()).select_from(model))).scalar_one()

    assert await _count(TenantAccessCharge) == 0, "premise broken: an access charge exists"
    assert await _count(TenantConfig) == 0, "premise broken: a tenant_config row exists"
    open_rules = (
        await db_session.execute(
            select(func.count()).select_from(MarkupRule).where(MarkupRule.effective_until.is_(None))
        )
    ).scalar_one()
    assert open_rules == 0, "premise broken: an OPEN markup rule exists"

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    # ⭐ THE WIRE MODEL — the exact projection close/issue/void return.
    resp = _invoice_response(inv)

    # 1. present and typed
    for field, typ in (
        ("access_charge_usd", Decimal),
        ("platform_fee_usd", Decimal),
        ("markup_pct", Decimal),
        ("effective_markup_pct", Decimal),
        ("markup_source", str),
    ):
        assert hasattr(resp, field), (
            f"InvoiceResponse has no {field!r}: the statement the customer is "
            f"handed cannot show it, whatever billing_invoices stores"
        )
        assert isinstance(getattr(resp, field), typ), (
            f"{field} is {type(getattr(resp, field)).__name__}, expected {typ.__name__}"
        )

    # 2. an EXPLICIT zero, not an omission
    assert resp.platform_fee_usd == Decimal("0.00"), (
        f"platform fee is {resp.platform_fee_usd}; this tenant is charged none"
    )
    assert resp.access_charge_usd == Decimal("0.00")

    # 3. provenance — and it must name the CODE CONSTANT, because that is the
    #    truth here. Asserting merely "not empty" would pass on a field that
    #    always says the same wrong thing.
    assert resp.markup_source == MARKUP_SOURCE_SETTINGS, (
        f"markup_source is {resp.markup_source!r}; with zero open markup_rules "
        f"and zero tenant_config rows the 30% can only be settings.default_markup_pct"
    )
    assert resp.markup_pct == Decimal(str(test_settings.default_markup_pct))

    # The response must SERIALISE these too — a field FastAPI drops on the way
    # out is not on the bill, and `hasattr` alone would not catch that.
    dumped = resp.model_dump(mode="json")
    for field in (
        "access_charge_usd",
        "platform_fee_usd",
        "markup_pct",
        "effective_markup_pct",
        "markup_source",
    ):
        assert field in dumped, f"{field} is on the model but not in its serialised form"

    # 4. the itemisation agrees with the aggregate it summarises
    access_lines = inv.snapshot["totals"]["access_charges"]
    assert len(access_lines) == 1, (
        f"expected exactly one explicit zero access line, got {access_lines!r}; "
        f"an empty array leaves totals.access_charge_usd='0.00' unitemised"
    )
    assert access_lines[0]["amount_usd"] == "0.00"
    assert access_lines[0]["basis"] == ACCESS_BASIS_NO_MATCH, (
        "the zero line must say the axis was EVALUATED and matched nothing — "
        "which is a different fact from the axis not applying"
    )

    # The frozen row and the snapshot must not disagree about the fee.
    assert Decimal(inv.snapshot["totals"]["platform_fee_usd"]) == resp.platform_fee_usd
    assert inv.snapshot["markup_source"] == resp.markup_source


# ---------------------------------------------------------------------------
# G. close_due_periods — the PRODUCER, against a real database
# ---------------------------------------------------------------------------
#
# ⚠️ WHY THIS SECTION EXISTS. Every other test that mentions close_due_periods
# monkeypatches it away and grades what the CALLER does with a hand-built
# CloseDueOutcome. That proves the consumer's wiring and nothing whatever about
# the producer's partitioning: with the whole body of close_due_periods stubbed,
# deleting its find_open pre-check — or flipping `unchanged.append(existing)` to
# `created.append(existing)` — leaves the suite fully green while production
# returns to re-counting every already-closed tenant as a NEW close on every
# hourly tick. These two tests import and call the REAL function.
#
# Imports are function-local ON PURPOSE: this whole section is additive so that
# a concurrent edit to this file's module-level import block merges without a
# conflict. (Ruff's E402 forbids a second module-level import block down here.)

_SWEEP_NOW = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
"""A FIXED clock, never the real one. ``_previous_month`` maps it to
[2026-02-01, 2026-03-01) — the same month ``_seed_full_month`` defaults to, so
the fixtures above are reused unchanged."""


async def test_close_due_periods_second_sweep_creates_nothing(
    db_session, db_session_factory, test_settings
) -> None:
    """Two identical sweeps: the first CREATES, the second reports UNCHANGED.

    ⛔ THIS IS THE TEST THAT DIES IF THE ``find_open`` PRE-CHECK IS DELETED FROM
    ``close_due_periods``. Without it, sweep 2 falls through to ``close_period``,
    whose own no-op path hands back the EXISTING row — indistinguishable in
    shape from a fresh one — and the loop appends it to ``created``. The row
    count in the database stays at 1 (nothing is written twice), so ONLY the
    partitioning of the return value can detect it. That is the whole defect
    this branch exists to fix: an hourly sweep re-reporting every already-closed
    tenant as a new close, roughly 650 phantom closes per tenant per month.

    The load-bearing assertion is ``created == []`` on sweep 2. It also kills the
    other mutant in the same line: ``unchanged.append(existing)`` rewritten to
    ``created.append(existing)``.
    """
    from services.invoices import close_due_periods

    tenant = T("close-due-partition")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        first = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

        # Non-vacuity for the whole test: the tenant WAS a candidate and WAS
        # closed. If this half were empty, sweep 2's `created == []` would pass
        # for the trivial reason that nothing was ever findable.
        assert len(first.created) == 1, (
            "the seeded tenant was not closed at all; the sweep found no candidate, "
            "so this test cannot say anything about the second sweep"
        )
        assert first.unchanged == []
        assert first.refused == 0
        assert first.failed == 0
        assert first.created[0].tenant_id == tenant
        assert first.created[0].period_start == _MONTH_START
        assert first.created[0].period_end == _MONTH_END
        first_id = first.created[0].id

        # Same arguments, same clock, nothing else changed in between.
        second = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

    # ⛔ THE ASSERTION. Delete the find_open pre-check in close_due_periods and
    # this line goes red with created == [the same invoice].
    assert second.created == [], (
        "the second sweep re-reported an already-closed period as a NEW close — "
        "the find_open pre-check is gone, and every hourly tick for the rest of "
        "the month will do the same"
    )
    assert len(second.unchanged) == 1
    assert second.unchanged[0].id == first_id
    assert second.refused == 0
    assert second.failed == 0

    # And the database agrees that no second row was written — which is exactly
    # why the return value's partitioning is the only place the defect shows.
    rows = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == tenant)
            )
        )
        .scalars()
        .all()
    )
    assert rows == [first_id]


async def test_close_due_periods_one_tenants_refusal_does_not_block_another(
    db_session, db_session_factory, test_settings, monkeypatch
) -> None:
    """One tenant refuses, the other still closes; ``refused``/``failed`` split.

    ⚠️ THE REFUSAL IS PATCHED, AND THAT LIMITS WHAT THIS TEST PROVES: it grades
    the loop's ACCOUNTING and its per-tenant isolation, not the refusal itself.
    That is a deliberate choice, not a shortcut — there is no genuine
    tenant-scoped ``InvoiceError`` reachable from a seeded test database:

      * ``assert_no_bucket_gap`` is FLEET-WIDE by design (its ``present`` query
        carries no tenant filter, and its own docstring says why: one tenant's
        quiet day is not evidence of anything). A seeded bucket gap therefore
        refuses EVERY tenant in the sweep, which is the opposite of the split
        under test. ``test_assert_no_bucket_gap_names_the_missing_date`` above
        exercises the real refusal directly.
      * ``InvoicePeriodError`` can only come from a malformed period, and
        ``_previous_month`` always yields a whole calendar month.
      * ``InvoiceStateError`` from ``issue`` needs a non-draft row that
        ``find_open`` would have returned first.

    So the ONE tenant that refuses is patched, and the other goes through the
    REAL ``close_period`` — the wrapper delegates — so the "not blocked" half is
    genuine work against the database, not a stub agreeing with itself.

    ⛔ Assertion that dies if the ``find_open`` pre-check is deleted: the
    ``second.created == []`` line at the end. The refusal accounting alone would
    survive that mutation.
    """
    from services import invoices as invoices_mod
    from services.invoices import InvoiceError, close_due_periods

    clean = T("close-due-clean")
    refuser = T("close-due-refuser")
    clean_license = uuid.uuid4()
    refuser_license = uuid.uuid4()

    for tenant, license_id in ((clean, clean_license), (refuser, refuser_license)):
        db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
        await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    real_close_period = invoices_mod.close_period

    async def _close_period_refusing_one(session, **kwargs):  # noqa: ANN001, ANN202
        if kwargs.get("tenant_id") == refuser:
            raise InvoiceError(f"synthetic refusal for {refuser}")
        return await real_close_period(session, **kwargs)

    monkeypatch.setattr(invoices_mod, "close_period", _close_period_refusing_one)

    payload = _license_payload(clean_license)
    close_patch, gen_patch = _patch_fetch_licenses(payload)
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = payload
        mock_gen_fetch.return_value = payload

        first = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

        # The refusal is counted as a REFUSAL, not as a failure. `failed` is the
        # blind `except Exception` arm; if InvoiceError ever stopped being
        # caught by the arm above it, this pair flips and says so.
        assert first.refused == 1, "the refusing tenant was not counted in `refused`"
        assert first.failed == 0, (
            "an InvoiceError landed in `failed`; the two arms have been reordered "
            "or InvoiceError is no longer caught first"
        )

        # Per-tenant isolation: one tenant's refusal must not take the other's
        # month with it. This is what ONE SESSION PER TENANT buys.
        assert len(first.created) == 1
        assert first.created[0].tenant_id == clean
        assert first.unchanged == []
        clean_id = first.created[0].id

        second = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

    # ⛔ Same mutation, second witness: without the find_open pre-check the clean
    # tenant is re-reported as a new close here too.
    assert second.created == [], "an already-closed tenant was re-reported as created"
    assert [inv.id for inv in second.unchanged] == [clean_id]
    assert second.refused == 1, "the refusing tenant refused again, as it must"
    assert second.failed == 0

    # The refuser has NO invoice: refusals are skipped, never degraded into a
    # row nobody can stand behind.
    refuser_rows = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == refuser)
            )
        )
        .scalars()
        .all()
    )
    assert refuser_rows == []


async def test_close_due_periods_counts_an_unexpected_error_as_failed(
    db_session, db_session_factory, test_settings, monkeypatch
) -> None:
    """A non-``InvoiceError`` for one tenant lands on ``failed``, not ``refused``.

    The blind ``except Exception`` arm is the one that keeps the sweep alive
    through a transport error or an unexpected shape for a SINGLE tenant. Its
    count is exported as
    ``billing_invoice_close_failures_total{reason="tenant_error"}``, and a
    signal nothing ever executes is a claim rather than a behaviour — the
    refusal test above covers only the ``InvoiceError`` arm.

    ⛔ AND THIS IS THE CONTROL FOR THE NAMED REFUSAL ARM BELOW. The three
    upstream-unavailable classes are now classified as ``refused``; widen that arm
    to ``except Exception`` — or catch ``RuntimeError``, which is what all three
    of them subclass — and the ``tenant_error`` signal goes silent for real
    defects while every ``refused``/``failed`` assertion in the three tests below
    still passes. This test is the only thing that goes red, which is why the
    exception raised here stays a BARE ``RuntimeError``: a plain ``Exception``
    would not detect the ``RuntimeError`` widening.

    ⚠️ The two arms mean different things and must not collapse into each
    other: a refusal is this module declining ON PURPOSE, a failure is it
    breaking. Asserting BOTH counters on each test is what stops a future
    reorder of the arms from going unnoticed.

    MEASURED, 2026-09-08, not reasoned: changing this arm's ``failed += 1`` to
    ``refused += 1`` killed THIS test and left the other two
    ``close_due_periods`` tests green. The asymmetry is the evidence — it pins
    the red to the arm rather than to the fixture.
    """
    from services import invoices as invoices_mod
    from services.invoices import close_due_periods

    clean = T("close-due-ok")
    broken = T("close-due-broken")
    clean_license = uuid.uuid4()
    broken_license = uuid.uuid4()

    for tenant, license_id in ((clean, clean_license), (broken, broken_license)):
        db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
        await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    real_close_period = invoices_mod.close_period

    async def _close_period_exploding_one(session, **kwargs):  # noqa: ANN001, ANN202
        if kwargs.get("tenant_id") == broken:
            # Deliberately NOT an InvoiceError: this is the arm that catches
            # what the module did not anticipate.
            raise RuntimeError(f"synthetic transport failure for {broken}")
        return await real_close_period(session, **kwargs)

    monkeypatch.setattr(invoices_mod, "close_period", _close_period_exploding_one)

    payload = _license_payload(clean_license)
    close_patch, gen_patch = _patch_fetch_licenses(payload)
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = payload
        mock_gen_fetch.return_value = payload

        outcome = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

    assert outcome.failed == 1, "a RuntimeError was not counted in `failed`"
    assert outcome.refused == 0, (
        "a non-InvoiceError landed in `refused`; the blind arm is catching what "
        "the InvoiceError arm should, or the two have been merged"
    )

    # The other tenant's month still closed: one tenant's failure must not take
    # the fleet with it.
    assert len(outcome.created) == 1
    assert outcome.created[0].tenant_id == clean

    broken_rows = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == broken)
            )
        )
        .scalars()
        .all()
    )
    assert broken_rows == [], "a tenant that raised mid-close still left an invoice row"


async def _sweep_with_one_tenant_raising(  # noqa: ANN201
    db_session,  # noqa: ANN001
    db_session_factory,  # noqa: ANN001
    test_settings,  # noqa: ANN001
    monkeypatch,  # noqa: ANN001
    *,
    clean_label: str,
    raising_label: str,
    exc: BaseException,
):
    """Run ONE real sweep over two seeded tenants, one of which raises ``exc``.

    Factored out because the three upstream-unavailable refusals below differ
    only in the exception class, and the thing under test is the classification
    arm that names them — not three different fixtures. Returns
    ``(outcome, clean_tenant, raising_tenant)``.

    ``close_period`` is patched for the raising tenant ONLY; the clean tenant goes
    through the real one, which is what makes each caller's ``created == 1``
    assertion a genuine non-vacuity check rather than a stub agreeing with
    itself. Same shape as the two tests above, deliberately.
    """
    from services import invoices as invoices_mod
    from services.invoices import close_due_periods

    clean = T(clean_label)
    raising = T(raising_label)
    clean_license = uuid.uuid4()
    raising_license = uuid.uuid4()

    for tenant, license_id in ((clean, clean_license), (raising, raising_license)):
        db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
        await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    real_close_period = invoices_mod.close_period

    async def _close_period_raising_one(session, **kwargs):  # noqa: ANN001, ANN202
        if kwargs.get("tenant_id") == raising:
            raise exc
        return await real_close_period(session, **kwargs)

    monkeypatch.setattr(invoices_mod, "close_period", _close_period_raising_one)

    payload = _license_payload(clean_license)
    close_patch, gen_patch = _patch_fetch_licenses(payload)
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = payload
        mock_gen_fetch.return_value = payload

        outcome = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

    return outcome, clean, raising


async def _assert_counted_as_refusal(
    outcome,  # noqa: ANN001
    clean: str,
    raising: str,
    db_session,  # noqa: ANN001
) -> None:
    """The shared verdict for an upstream non-answer: REFUSED, not FAILED.

    Both counters are asserted on every case, for the reason stated on
    ``test_close_due_periods_counts_an_unexpected_error_as_failed``: a single-sided
    assertion survives a reorder of the arms. The row check is here too because a
    refusal that still wrote an invoice would be the worse half of the same bug —
    a frozen number drawn from a figure licences never stated.
    """
    assert outcome.refused == 1, (
        "an upstream that could not state a figure the invoice needs was not counted "
        "in `refused`; it fell through to the blind `except Exception` arm and now "
        'pages BillingInvoiceCloseErrored on {reason="tenant_error"}'
    )
    assert outcome.failed == 0, (
        "a designed refusal landed in `failed`, which is the code-defect signal — "
        "the named refusal arm is gone, or sits BELOW the blind `except Exception`"
    )

    # Non-vacuity: the sweep really ran and really closed the other tenant, so
    # `failed == 0` cannot pass because nothing was ever attempted.
    assert len(outcome.created) == 1, (
        "the clean tenant was not closed, so this sweep proves nothing about how "
        "the other tenant's exception was classified"
    )
    assert outcome.created[0].tenant_id == clean

    rows = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == raising)
            )
        )
        .scalars()
        .all()
    )
    assert rows == [], "a refused close still wrote an invoice row"


async def test_close_due_periods_counts_seat_count_unavailable_as_refused(
    db_session, db_session_factory, test_settings, monkeypatch
) -> None:
    """``SeatCountUnavailableError`` is a REFUSAL, not a code defect.

    ⛔ WHY THIS IS NOT TIDINESS. ``refused`` exports as
    ``billing_invoice_close_failures_total{reason="tenant_refused"}`` and
    ``failed`` as ``{reason="tenant_error"}``
    (``services/invoice_closer.py``). The ``BillingInvoiceCloseErrored``
    Prometheus alert pages on ``tenant_error`` to mean THE MODULE BROKE. This
    exception is a ``RuntimeError``, so before the named arm was added every
    licences blip reached the blind ``except Exception`` and paged as a defect —
    which both cried wolf and diluted the one counter that is supposed to mean a
    human must go and read a traceback.

    The class's own docstring already said a caller freezing money "must
    propagate this and refuse". The HIERARCHY is unchanged — it stays a plain
    ``RuntimeError``, so nothing that catches a concrete ``InvoiceError``
    subclass (``routers/billing.py``'s 409/422 mappings) is touched; only the
    sweep's classification is.
    """
    from services.seats import SeatCountUnavailableError

    outcome, clean, raising = await _sweep_with_one_tenant_raising(
        db_session,
        db_session_factory,
        test_settings,
        monkeypatch,
        clean_label="close-due-seat-ok",
        raising_label="close-due-seat-unavailable",
        exc=SeatCountUnavailableError("synthetic: licences cannot state a seat count"),
    )
    await _assert_counted_as_refusal(outcome, clean, raising, db_session)


async def test_close_due_periods_counts_licenses_unavailable_as_refused(
    db_session, db_session_factory, test_settings, monkeypatch
) -> None:
    """``LicensesUnavailableError`` is a REFUSAL, not a code defect.

    The blind arm's comment used to name "a licences outage" as one of the things
    it caught, which is precisely the misclassification: ``close_period`` calls
    ``fetch_licenses`` and lets this propagate ON PURPOSE so that no draft is
    written with the licence list degraded to ``[]``. The outage is an
    environment fact with no traceback to read and no code to change, and it
    self-heals on the next sweep — a refusal in everything but the counter it
    landed in.
    """
    from services.billing import LicensesUnavailableError

    tenant = T("close-due-licences-unavailable")
    outcome, clean, raising = await _sweep_with_one_tenant_raising(
        db_session,
        db_session_factory,
        test_settings,
        monkeypatch,
        clean_label="close-due-licences-ok",
        raising_label="close-due-licences-unavailable",
        exc=LicensesUnavailableError(tenant, "synthetic HTTP 503"),
    )
    await _assert_counted_as_refusal(outcome, clean, raising, db_session)


async def test_close_due_periods_counts_byo_mode_unavailable_as_refused(
    db_session, db_session_factory, test_settings, monkeypatch
) -> None:
    """``ByoModeUnavailableError`` is a REFUSAL, not a code defect.

    The most expensive of the three to mistake, and the reason is on
    ``assert_byo_mode_available``: ``byo_mode`` decides whether the tenant is
    billed AT ALL, so an unknown answer risks a permanent COMMERCIAL statement
    for an internal cost centre. The gate therefore refuses for EVERY tenant,
    unconditionally — a deliberate decline, which is what ``refused`` counts.

    ⛔ ``test_close_period_refuses_when_byo_mode_unavailable`` proves the gate
    RAISES. Nothing proved what the sweep then DID with it, and the sweep is
    where the operator-facing signal is produced.
    """
    from services.byo_mode import ByoModeUnavailableError

    outcome, clean, raising = await _sweep_with_one_tenant_raising(
        db_session,
        db_session_factory,
        test_settings,
        monkeypatch,
        clean_label="close-due-byo-ok",
        raising_label="close-due-byo-unavailable",
        exc=ByoModeUnavailableError("synthetic: licences cannot state byo_mode"),
    )
    await _assert_counted_as_refusal(outcome, clean, raising, db_session)


# ---------------------------------------------------------------------------
# H. assert_all_service_keys_priced — the gate, and the three false refusals
#    it must NOT make
# ---------------------------------------------------------------------------
#
# ⚠️ WHY THIS SECTION EXISTS. `services/billing.py:562` resolves an app charge as
# `app_rates.get(row.service_key, Decimal("0"))`, and `billing_app_cost_rates`
# held ZERO rows in prod when that was measured — so without this gate every app
# line of every tenant closes at a fabricated $0.00, on an invoice number that is
# gapless and irreversible, with `services/markup.py:498`'s zero-margin alarm
# silenced by its own `incurred_usd > 0` condition.
#
# The three negative controls are the point as much as the refusal is: a gate
# that tested `rate == 0` instead of `key in app_rates`, or that skipped the
# `requests > 0` / system-sentinel filters, would refuse closes that are not
# defective. Each of those three is a DIFFERENT wrong implementation, and each
# has its own test below.
#
# Imports are function-local, per the convention stated in section G: this whole
# section is additive so a concurrent edit to the module-level import block
# merges without a conflict.

#: A date inside _MONTH, for the metering rows these tests hang the gate on.
_METERING_DATE = _MONTH_START + timedelta(days=14)


def _metering_row(
    tenant_id: str,
    *,
    license_id: uuid.UUID,
    service_key: str,
    request_count: int,
):  # noqa: ANN202
    """One usage row — the table the app-charge loop reads per ``service_key``.

    ``subject_id``/``app_id``/``plan_id`` are left at their model defaults:
    nothing in the app-charge path groups on them, and the gate groups on
    ``service_key`` alone.
    """
    from models.db import MeteringDaily

    return MeteringDaily(
        tenant_id=tenant_id,
        license_id=license_id,
        service_key=service_key,
        bucket_date=_METERING_DATE,
        request_count=request_count,
        denied_count=0,
    )


def _app_rate_missing_close_gate_count(service_key: str) -> float:
    """Current value of ``app_rate_missing_total{service_key,surface="close_gate"}``.

    ``close_gate`` is a THIRD surface value alongside ``invoice`` (the generator)
    and ``overview`` (the admin read). Reading the gate's own surface is what
    makes this assertion non-vacuous: a gate that incremented ``invoice``
    instead would double-count the same gap and this would stay at 0.
    """
    import observability.metrics as metrics

    return metrics.app_rate_missing_total.labels(
        service_key=service_key, surface="close_gate"
    )._value.get()


async def test_close_period_refuses_a_service_key_with_no_app_cost_rate(
    db_session, test_settings
) -> None:
    """THE GATE: billable requests on an unpriced key REFUSE the close.

    THE MUTANT: delete the ``assert_all_service_keys_priced`` call from
    ``close_period``. The close then succeeds and writes an invoice whose app
    line for ``unpriced-probe`` is $0.00 — which is why the invoice-row-count
    assertion below is part of the test and not decoration.
    """
    from services.invoices import UnpricedServiceKeyError

    tenant = T("invoice-unpriced")
    license_id = uuid.uuid4()
    before = _app_rate_missing_close_gate_count("unpriced-probe")

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(
        _metering_row(tenant, license_id=license_id, service_key="unpriced-probe", request_count=7)
    )
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        with pytest.raises(UnpricedServiceKeyError) as exc:
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_MONTH_START,
                period_end=_MONTH_END,
                settings=test_settings,
            )

    assert exc.value.service_keys == ["unpriced-probe"]
    assert "unpriced-probe" in str(exc.value)
    assert "billing_app_cost_rates" in str(exc.value), (
        "the refusal does not name the table an operator has to fix"
    )
    # Refused BEFORE the one HTTP call and before any row exists to clean up —
    # the ordering claim in close_period's docstring, asserted.
    assert mock_close_fetch.call_count == 0, "the gate ran after the licences fetch"

    rows = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == tenant)
            )
        )
        .scalars()
        .all()
    )
    assert rows == [], "a refused close still wrote an invoice row"

    assert _app_rate_missing_close_gate_count("unpriced-probe") == before + 1, (
        "the gate refused without counting the miss under surface='close_gate'"
    )


async def test_close_period_allows_a_free_tier_whose_rate_row_states_zero(
    db_session, test_settings
) -> None:
    """NEGATIVE CONTROL — a stated 0 is a PRICE; an absent row is not.

    MEMBERSHIP, not value, is the discriminator. ``cost_per_request_usd`` is NOT
    NULL, so a deliberately free service is a PRESENT key holding 0. A gate that
    tested ``rate == 0`` instead of ``key in app_rates`` would refuse to close
    every tenant on a free tier — the defect, not the fix — and this test is the
    only thing standing between the two implementations.
    """
    from models.db import AppCostRate

    tenant = T("invoice-free-tier")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    db_session.add(AppCostRate(app_key="free-tier-probe", cost_per_request_usd=Decimal("0")))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(
        _metering_row(tenant, license_id=license_id, service_key="free-tier-probe", request_count=7)
    )
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    assert inv.id is not None
    # Gate and DISCLOSURE recorder agree on membership semantics here: the
    # priced-at-zero key is not reported unpriced either.
    assert inv.inputs["unpriced_service_keys"]["metering"] == []


async def test_close_period_allows_an_unpriced_key_with_zero_requests(
    db_session, test_settings
) -> None:
    """NEGATIVE CONTROL — ``request_count > 0`` is load-bearing, not a tidy-up.

    ``rate * 0 == 0`` whatever the rate is, so an absent rate row cannot
    fabricate a charge for a key nobody called, and refusing here would block a
    tenant's close over a service it did not use. The disclosure recorder DOES
    still name the key (it deliberately over-reports), which is asserted so this
    test also pins the recorder/gate split rather than just the gate.
    """
    tenant = T("invoice-unpriced-idle")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(
        _metering_row(tenant, license_id=license_id, service_key="idle-probe", request_count=0)
    )
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    assert inv.id is not None
    assert "idle-probe" in inv.inputs["unpriced_service_keys"]["metering"], (
        "the disclosure recorder stopped reporting a key the gate deliberately allows"
    )


async def test_close_period_allows_an_unpriced_key_seen_only_on_the_system_licence(
    db_session, test_settings
) -> None:
    """NEGATIVE CONTROL — the gate mirrors the generator's own system-licence skip.

    ``generate_billing``'s usage loop does ``if is_system_license(row.license_id):
    continue`` (``services/billing.py:516``) BEFORE it reaches ``app_rates``, so a
    key seen only under ``NULL_LICENSE_ID`` is never charged for and an absent
    rate cannot fabricate anything. The sentinel carries the majority of measured
    spend, so a gate that refused on it would block closes that are not defective.
    """
    tenant = T("invoice-unpriced-system")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(
        _metering_row(
            tenant,
            license_id=NULL_LICENSE_ID,
            service_key="system-only-probe",
            request_count=7,
        )
    )
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    assert inv.id is not None
    # Non-vacuity: the key really was present and really was unpriced — the
    # recorder (which does NOT filter the sentinel) still names it.
    assert "system-only-probe" in inv.inputs["unpriced_service_keys"]["metering"], (
        "the fixture did not actually produce an unpriced key, so the allow proves nothing"
    )


# ---------------------------------------------------------------------------
# I. assert_byo_mode_available — the gate that decides whether a tenant is
#    billed AT ALL, and the cache prime that makes it stick
# ---------------------------------------------------------------------------
#
# ⚠️ WHY THIS IS THE LARGEST OF THE FOUR CLOSE GATES. The other three refuse a
# WRONG NUMBER on an otherwise-correct statement. This one refuses a statement
# that should not exist: ``byo_mode`` feeds ``effective_billing_mode``, which
# sends ``local_allowed``/``platform_only`` to ``internal`` (bill $0) and
# ``byo_required`` to the configured mode (20% markup PLUS a $5,000 flat access
# charge). ``get_byo_mode`` defaults to ``byo_required`` on a lookup failure —
# correct for the live gate, where the output is a transient decision and the
# alternative gives platform GPU away free, and wrong here, where the output is
# an immutable row from a gapless series. Measured 2026-09-11: 49 of 59
# unarchived tenants carry ``local_allowed``/``platform_only``, i.e. one licences
# blip mid-close could commercially invoice up to 49 internal cost centres, and
# there is no pass anywhere that recomputes a closed invoice's billing mode.


def _byo_mode_stub(monkeypatch, answers):  # noqa: ANN001, ANN201
    """Replace the autouse ``byo-mode`` answer with ``answers`` (a callable taking
    the 1-based call index and returning a response, or raising).

    Everything that is not the ``byo-mode`` endpoint still goes to the real
    method, exactly as in the autouse fixture.
    """
    real_get = httpx.AsyncClient.get
    calls = {"n": 0}

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if "byo-mode" in str(url):
            calls["n"] += 1
            return answers(calls["n"])
        # The autouse fixture already wrapped the real method, and the chain is
        # deliberate: this one only overrides the byo-mode leg.
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return calls


async def test_close_period_refuses_when_licences_cannot_state_the_billing_mode(
    db_session, test_settings, monkeypatch
) -> None:
    """⛔ THE ONE THAT PROTECTS MONEY — and the biggest of the four refusals.

    A 503 from licences is a statement about the SERVICE. Without this gate the
    close proceeds on ``get_byo_mode``'s revenue-safe guess and freezes a
    COMMERCIAL statement for a tenant that bills $0, on an irreversible invoice
    number.

    ⚠️ MUTATION CONTROL: delete the ``assert_byo_mode_available`` call from
    ``close_period`` (or make it swallow ``ByoModeUnavailableError``) and this
    test fails twice — no exception is raised, AND an invoice row appears.
    """
    from services.byo_mode import ByoModeUnavailableError

    tenant = T("invoice-byo-unavailable")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    _byo_mode_stub(monkeypatch, lambda _n: httpx.Response(503, json={"error": "down"}))

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        with pytest.raises(ByoModeUnavailableError) as exc:
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_MONTH_START,
                period_end=_MONTH_END,
                settings=test_settings,
            )

    assert "503" in str(exc.value), (
        "the refusal does not name the status, so an operator cannot tell an outage "
        "from a misconfiguration"
    )
    # Refused BEFORE the one licences-list HTTP call and before any row exists to
    # clean up — the ordering claim in close_period's docstring, asserted.
    assert mock_close_fetch.call_count == 0, "the gate ran after the licences fetch"

    rows = (
        (await db_session.execute(select(BillingInvoice).where(BillingInvoice.tenant_id == tenant)))
        .scalars()
        .all()
    )
    assert list(rows) == [], "a commercial invoice was frozen on a guessed billing mode"
    # And the counter was never drawn from: a refusal must not consume a number.
    numbers = (await db_session.execute(select(func.count()).select_from(BillingInvoice))).scalar()
    assert numbers == 0


async def test_close_period_succeeds_when_licences_answers(
    db_session, test_settings, monkeypatch
) -> None:
    """NEGATIVE CONTROL — the gate is not a blanket refusal.

    Without this, a mutant that makes ``assert_byo_mode_available`` raise
    unconditionally would leave the refusal test above green while blocking every
    close in the estate.
    """
    tenant = T("invoice-byo-available")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    calls = _byo_mode_stub(
        monkeypatch, lambda _n: httpx.Response(200, json={"byo_mode": "byo_required"})
    )

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
        )

    assert inv.id is not None
    assert inv.status == "issued"
    assert inv.billing_mode == "standard_markup"
    rows = (
        (await db_session.execute(select(BillingInvoice).where(BillingInvoice.tenant_id == tenant)))
        .scalars()
        .all()
    )
    assert len(list(rows)) == 1
    # ⭐ ONE resolution for the whole close, not three: the gate primed the cache
    # that `_capture_inputs` and `generate_billing` then read.
    assert calls["n"] == 1, (
        f"byo_mode was resolved {calls['n']}x — the gate did not prime the TTL cache, "
        "so a blip mid-close can still reach the frozen row"
    )


async def test_close_period_freezes_the_real_mode_when_licences_dies_after_the_gate(
    db_session, test_settings, monkeypatch
) -> None:
    """⭐ THE TOCTOU WINDOW THE ORDERING CLOSES, driven end to end.

    licences answers the gate truthfully (``local_allowed`` -> internal -> $0),
    then dies. ``_capture_inputs`` and ``generate_billing`` re-resolve the same
    tenant through the DEFAULTING ``get_byo_mode``, which on a dead licences
    answers ``byo_required`` = BILL COMMERCIALLY. They must instead read the real
    answer the gate left in the 5-minute cache.

    ⚠️ MUTATION CONTROL: delete the ``_byo_cache[tenant_id] = mode`` write in
    ``services/byo_mode.py::_resolve_byo_mode`` and this fails — the second
    lookup becomes a ConnectError, defaults to ``byo_required``, and the frozen
    row reads ``standard_markup`` instead of ``internal``. (It also fails on the
    ``local_allowed`` assertion in ``inputs``, which is the same defect seen from
    the other frozen document.)
    """
    tenant = T("invoice-byo-dies-midclose")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    def _answer(n: int):  # noqa: ANN202
        if n == 1:
            return httpx.Response(200, json={"byo_mode": "local_allowed"})
        raise httpx.ConnectError("licences died mid-close")

    calls = _byo_mode_stub(monkeypatch, _answer)

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        # ⚠️ ASSERTED THROUGH THE REFUSAL SINCE 2026-09-13, and the property under
        # test is UNCHANGED. An internal tenant bills $0.00, and
        # `PeriodNotBillableError` now declines to spend an invoice number on a
        # statement that charges nothing — so this scenario, which is internal BY
        # CONSTRUCTION, can no longer produce a row to read `billing_mode` off.
        # The mode therefore rides on the exception: see that class's `.billing_mode`.
        #
        # ⭐ THE MUTATION CONTROL STILL DISCRIMINATES, and more sharply than before.
        # Delete the `_byo_cache[tenant_id] = mode` write in
        # `services/byo_mode.py::_resolve_byo_mode` and the second lookup raises,
        # defaults to `byo_required`, the period becomes COMMERCIAL and therefore
        # BILLABLE — so the mutant does not merely change a field, it produces an
        # invoice where this asserts none can exist, failing on `pytest.raises`
        # itself as well as on the mode below.
        with pytest.raises(PeriodNotBillableError) as exc:
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_MONTH_START,
                period_end=_MONTH_END,
                settings=test_settings,
            )

    # ⭐ THE DISCRIMINATOR. Exactly one resolution means the later lookups were
    # served from the primed cache and the simulated outage was never reached.
    # Against the mutant the count is 2+, the second call raises, and the mode
    # defaults to byo_required — which the assertion below then catches.
    assert calls["n"] == 1, (
        f"byo_mode was re-resolved {calls['n']}x after the gate; the cache prime is gone"
    )
    assert exc.value.billing_mode == "internal", (
        "an internal/demo tenant resolved as commercial because a defaulting "
        "lookup ran after licences died"
    )
    assert (
        await db_session.execute(select(BillingInvoice).where(BillingInvoice.tenant_id == tenant))
    ).scalars().all() == [], "an internal $0.00 period must not leave a numbered statement behind"


# ---------------------------------------------------------------------------
# J. The digest covers the MONEY, and `reproduce` can explain its own total
# ---------------------------------------------------------------------------
#
# ⚠️ WHY THIS SECTION EXISTS. `snapshot_sha256` used to be
# `sha256({"snapshot": ..., "inputs": ...})` and `reproduce` verified it by
# re-hashing THOSE SAME TWO FIELDS — so the check confirmed itself. The money
# columns (`total_usd`, `subtotal_usd`, `margin_usd`, `seat_fee_usd`, ...) are
# DERIVED from the snapshot once at close time and then stored as copies, and a
# repo-wide grep found nothing anywhere comparing a copy against its source.
# A wrong invoice total was therefore detectable by NO code path in this service:
# not by the trigger (which freezes a row but cannot know the figure was wrong
# when written), not by the digest, and not by the one re-render that exists.
#
# `billing_invoices` held 0 rows when this was written, which is the only reason
# the digested payload could be widened at all — every stored digest would
# otherwise have to be recomputed against live money.
#
# Imports are function-local, per the convention stated in section G: this whole
# section is additive so a concurrent edit to the module-level import block
# merges without a conflict.


async def _close_a_draft(db_session, test_settings, tenant, *, actor=None):  # noqa: ANN001, ANN202
    """Close one month for ``tenant`` as a DRAFT and return its id.

    ⭐ DRAFT, NOT ISSUED, AND THAT IS LOAD-BEARING FOR THE MUTATION TESTS.
    `billing_forbid_invoice_mutation` refuses every UPDATE to an ISSUED row
    except a void, so a test that tampers with `total_usd` can only do so on a
    draft — `src/models/db/invoice_immutability.py` returns NEW unconditionally
    for `OLD.status = 'draft'` ("a draft is working state"). Tampering is what
    these tests have to simulate, so the fixture has to stay on the one status
    the guard permits it on. That is a narrower scope than the defect (an issued
    row written with an already-wrong total is the real exposure), and
    `money_matches_snapshot` is the half of the fix that covers THAT case,
    because it compares the column against the document rather than against the
    close-time digest.
    """
    inv = await close_period(
        db_session,
        tenant_id=tenant,
        period_start=_MONTH_START,
        period_end=_MONTH_END,
        settings=test_settings,
        actor=actor,
        issue_immediately=False,
    )
    assert inv.status == "draft"
    return inv.id


def _unlicensed_row(tenant_id: str, usd: str):  # noqa: ANN202
    """A disclosed-but-unbillable cost row, so `excluded_unbillable_usd` is NOT 0.

    Copied in shape from section B. It makes the one money column that
    `_money_from_snapshot` CANNOT re-derive carry a real figure, which is what
    makes the digest-only coverage of it testable rather than trivially true at 0.
    """
    return CostDaily(
        tenant_id=tenant_id,
        license_id=NULL_LICENSE_ID,
        service_key="unattributed",
        provider="test",
        bucket_date=_MONTH_START + timedelta(days=5),
        total_input_tokens=0,
        total_output_tokens=0,
        total_cost_usd=Decimal(usd),
        request_count=0,
        attribution_status=ATTRIBUTION_UNLICENSED,
    )


async def _force_money_column(db_session, invoice_id, column: str, value: str) -> None:  # noqa: ANN001
    """Overwrite ONE money column behind the service's back, then forget the row.

    Raw SQL on purpose: the point is to simulate an edit that did NOT come
    through `close_period`, which is the only interesting kind. `expire_all`
    afterwards so nothing can be answered out of the identity map — `reproduce`
    must read what Postgres actually holds, including the column's own
    `Numeric(20, 8)` scale.

    ⚠️ The column name is interpolated, not bound: an identifier cannot be a
    bind parameter. Every caller passes a literal from `_MONEY_COLUMNS`.
    """
    from sqlalchemy import text

    from services.invoices import _MONEY_COLUMNS

    assert column in _MONEY_COLUMNS, f"{column} is not a digested money column"
    await db_session.execute(
        text(f"UPDATE billing_invoices SET {column} = :v WHERE id = :id"),  # noqa: S608
        {"v": Decimal(value), "id": invoice_id},
    )
    await db_session.commit()
    db_session.expire_all()


async def test_reproduce_detects_an_edited_total_usd(db_session, test_settings) -> None:
    """FIX 1, THE KILLING TEST: a tampered ``total_usd`` must read as tampered.

    THE MUTANT THIS KILLS IS THE CODE AS IT SHIPPED. With `_digest` hashing only
    `{"snapshot", "inputs"}`, the UPDATE below moves a column that is in neither
    document, `reproduce` re-hashes the same two untouched fields, and
    `digest_matches` stays **True** — the row reads as pristine while its total
    is off by $1,000. Both assertions after the mutation fail on unmodified code.

    ⛔ THE PRE-MUTATION ASSERTIONS ARE NOT DECORATION. Widening the digested
    payload to include money fails in a direction that LOOKS like success: the
    close hashes pre-insert Python values while `reproduce` hashes what Postgres
    returns, and `Numeric(20, 8)` reads `Decimal("0")` back as
    `Decimal("0.00000000")`. Get the canonicalisation wrong and `digest_matches`
    is False for EVERY invoice — at which case this test's post-mutation half
    passes for entirely the wrong reason. Asserting True first is what separates
    "the detector works" from "the detector is stuck on".
    """
    tenant = T("digest-covers-total")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)
        invoice_id = await _close_a_draft(db_session, test_settings, tenant)

    await db_session.commit()
    db_session.expire_all()

    clean = await reproduce(db_session, invoice_id=invoice_id)
    assert clean["digest_matches"] is True, (
        "an untouched invoice reads as tampered -- the money columns are hashed "
        "pre-insert and verified post-read, so this is the Numeric(20, 8) scale "
        "trap in _money_canon, not a real mismatch"
    )
    assert clean["money_matches_snapshot"] is True, clean["money_mismatches"]
    # Non-vacuity: the figure being protected is a real amount, not 0. 28 days
    # x $2.50 incurred, marked up 30%.
    original = Decimal(clean["total_usd"])
    assert original > 0, "the fixture billed nothing, so there is no total to tamper with"

    tampered = original + Decimal("1000")
    await _force_money_column(db_session, invoice_id, "total_usd", str(tampered))

    after = await reproduce(db_session, invoice_id=invoice_id)
    assert Decimal(after["total_usd"]) == tampered, "the UPDATE did not land"
    assert after["digest_matches"] is False, (
        "the stored digest still matches after total_usd was rewritten -- the "
        "money columns are outside the digested payload"
    )
    # The second, independent half of the fix: the column no longer agrees with
    # the document it was derived from, and the report NAMES which column.
    assert after["money_matches_snapshot"] is False
    assert set(after["money_mismatches"]) == {"total_usd"}, (
        "the mismatch report is not specific to the edited column"
    )
    assert Decimal(after["money_mismatches"]["total_usd"]["stored"]) == tampered
    assert Decimal(after["money_mismatches"]["total_usd"]["from_snapshot"]) == original


async def test_reproduce_detects_an_edited_excluded_unbillable_usd(
    db_session, test_settings
) -> None:
    """FIX 1, the column only the DIGEST can protect — the documented asymmetry.

    `excluded_unbillable_usd` comes from `measure_excluded_unbillable`, a query
    over `cost_daily`, so `_money_from_snapshot` cannot re-derive it without
    reading a mutable table — which `reproduce` may never do. This test pins
    both halves of that: the digest DOES catch the edit, and
    `money_matches_snapshot` correctly stays True because it is not in the
    derived set. A future change that "tidied" this column into
    `_money_from_snapshot` by reading cost_daily would flip the second assertion
    and be caught here.
    """
    tenant = T("digest-covers-excluded")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(_unlicensed_row(tenant, "1.0008"))
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)
        invoice_id = await _close_a_draft(db_session, test_settings, tenant)

    await db_session.commit()
    db_session.expire_all()

    clean = await reproduce(db_session, invoice_id=invoice_id)
    # Non-vacuity, and a genuine exercise of _money_canon: 1.0008 is scale 4 in
    # the fixture and scale 8 in the column, so a digest over raw str() would
    # already be broken here.
    assert Decimal(clean["excluded_unbillable_usd"]) == Decimal("1.0008")
    assert clean["digest_matches"] is True

    await _force_money_column(db_session, invoice_id, "excluded_unbillable_usd", "999.0")

    after = await reproduce(db_session, invoice_id=invoice_id)
    assert after["digest_matches"] is False, (
        "a rewritten excluded_unbillable_usd went unnoticed -- it is a stored "
        "copy like the rest and must be inside the digest"
    )
    assert after["money_matches_snapshot"] is True, (
        "excluded_unbillable_usd is NOT snapshot-derivable; if it is being "
        "compared against the snapshot then reproduce has started reading "
        "cost_daily, which it must never do"
    )


async def test_reproduce_returns_the_components_that_explain_the_total(
    db_session, test_settings
) -> None:
    """FIX 2, THE KILLING TEST: the seven omitted fields reach the re-render.

    THE MUTANT THIS KILLS IS THE CODE AS IT SHIPPED. `reproduce`'s select list
    and return dict omitted all seven, so each subscript below raises `KeyError`
    and the only re-render path there is returned a total with no components to
    explain it, by an issuer it could not name.

    ISSUED, not draft, because `issued_by` is written by `issue()` and is
    structurally NULL on a draft — a draft fixture would assert None == None and
    prove nothing about that field.
    """
    tenant = T("reproduce-components")
    license_id = uuid.uuid4()
    actor = "close-bot@test"

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(_unlicensed_row(tenant, "1.0008"))
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)
        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            actor=actor,
        )
    assert inv.status == "issued"
    invoice_id = inv.id

    await db_session.commit()
    db_session.expire_all()
    stored = await db_session.get(BillingInvoice, invoice_id)
    doc = await reproduce(db_session, invoice_id=invoice_id)

    # THE SEVEN, each against the value actually on the row. Presence alone
    # would pass on a field hardcoded to None, so every one is an equality.
    assert Decimal(doc["seat_fee_usd"]) == stored.seat_fee_usd
    assert doc["seat_count"] == stored.seat_count
    assert Decimal(doc["llm_usage_usd"]) == stored.llm_usage_usd
    assert Decimal(doc["llm_markup_usd"]) == stored.llm_markup_usd
    assert Decimal(doc["excluded_unbillable_usd"]) == stored.excluded_unbillable_usd
    assert doc["billing_mode"] == stored.billing_mode
    assert doc["issued_by"] == stored.issued_by

    # NON-VACUITY. `seat_fee_usd`/`seat_count` are structurally 0/None in this
    # fixture (no `cadence='per_seat'` access charge), so their equalities above
    # are the only thing holding them — which is enough to fail on an omitted
    # key, but not enough to prove a real figure survives the round trip. These
    # five carry real values and would each fail on a hardcoded default.
    assert Decimal(doc["llm_usage_usd"]) > 0, "no LLM cost was billed; the fixture is inert"
    assert Decimal(doc["llm_markup_usd"]) > 0, "a 30% markup produced no markup figure"
    assert Decimal(doc["excluded_unbillable_usd"]) == Decimal("1.0008")
    assert doc["billing_mode"] != "", (
        "billing_mode came back empty, so the re-render cannot say whether this "
        "tenant was billed commercially or at $0"
    )
    assert doc["issued_by"] == actor, "the re-render cannot name who issued the statement"

    # ⚠️ NO ARITHMETIC IDENTITY IS ASSERTED BETWEEN THESE COMPONENTS AND
    # `total_usd`/`margin_usd`, and that is deliberate. `generate_billing` reports
    # margin and the customer total through `totals`, and nothing in this repo
    # states that the seat fee, the access charge and the LLM markup sum to
    # either of them — markup is applied per billed LINE, allowances are netted
    # before markup, and the platform fee is folded INSIDE a billed line rather
    # than added to it. Writing a plausible-looking sum here would encode a
    # relationship nobody has measured as the spec. What this test asserts is the
    # claim actually being fixed: the components are RETURNED, with the values
    # the row holds.


# ---------------------------------------------------------------------------
# K. ONE read of `billing_app_cost_rates` per close — the gate, the frozen
#    provenance and the money lines cannot be computed from different rate sets
# ---------------------------------------------------------------------------
#
# ⚠️ WHY THIS SECTION EXISTS. `close_period` used to read
# `billing_app_cost_rates` THREE times through one session — the gate
# (`assert_all_service_keys_priced`), the provenance (`_capture_inputs`) and the
# money (`get_app_cost_rates` inside `generate_billing`) — with no cache, no
# `FOR UPDATE` and no isolation override. Under Postgres default READ COMMITTED
# every statement takes a FRESH snapshot, so an operator inserting or repricing a
# rate row mid-close (exactly when they would: the gate's refusal is the message
# telling them to) made the three disagree. The close would then be ALLOWED on
# one rate set, CLAIM a second in `inputs`, and CHARGE at a third, on an
# immutable row drawn from a gapless series with no later pass able to reconcile
# it.
#
# ⚠️ THIS IS NOT THE `byo_mode` TOCTOU OF SECTION I. That one is narrowed by a
# TTL cache because neither of its consumers takes an injected value; this one is
# closed outright by threading an explicit argument, so a future edit cannot drop
# the dependency without changing a call site.
#
# Imports are function-local, per the convention stated in section H.

#: Requests on the priced key, so the app-request line carries real money: the
#: rate threading is only observable in the figures if some figure depends on it.
_RATE_REQUESTS = 100

#: The rate the table really holds for the whole close.
_RATE_REAL = Decimal("0.01000000")

#: What a SECOND read of the table is made to return — the concurrent reprice.
#: Three orders of magnitude away from `_RATE_REAL` so any consumer that re-reads
#: is visible in the money, not merely in a call log.
_RATE_RACED = Decimal("9.99000000")

#: Sentinel for "this consumer was not passed `app_rates` at all", kept distinct
#: from `None` (which is the self-fetch default and a legitimate value to pass).
_NO_RATES = object()


def _raced_app_rate_reads(monkeypatch, service_key: str):  # noqa: ANN001, ANN202
    """Patch the ONE rate-table read so the 2nd+ call returns a DIFFERENT rate.

    Returns the call log. With the snapshot threaded there is exactly one read,
    so ``_RATE_RACED`` is never produced and no consumer can see it; any consumer
    that re-reads gets the reprice and says so in the figures it produces.

    ⚠️ BOTH BINDINGS are patched. ``services.invoices`` holds one (close_period's
    single read) and ``services.billing`` holds the other — and
    ``services.billing.get_app_cost_rates``, which is what an un-threaded
    consumer calls, goes through the billing one. Patching only one would let a
    re-read slip past uncounted and the test would pass on the defect.

    The replacement row is a TRANSIENT ``AppCostRate`` — constructed, never added
    to a session — so the simulated reprice cannot be flushed into the database
    and leak into the assertions about what was really stored.
    """
    from models.db import AppCostRate
    from services import billing as billing_mod
    from services import invoices as invoices_mod

    real = billing_mod.get_app_cost_rate_rows
    reads: list[int] = []

    async def _counting(session):  # noqa: ANN001, ANN202
        reads.append(len(reads) + 1)
        if len(reads) == 1:
            return await real(session)
        return [AppCostRate(app_key=service_key, cost_per_request_usd=_RATE_RACED)]

    monkeypatch.setattr(billing_mod, "get_app_cost_rate_rows", _counting)
    monkeypatch.setattr(invoices_mod, "get_app_cost_rate_rows", _counting)
    return reads


def _record_app_rates(monkeypatch, names: tuple[str, ...]):  # noqa: ANN001, ANN202
    """Record, PER CALL, the ``app_rates`` each named consumer received.

    ⛔ A LIST OF CALLS, NOT A LAST-WRITE DICT. A dict keyed by consumer name
    records only the final value per key, and in this repo a last-write
    assertion has already let a DELETED argument survive a full green suite:
    whichever consumer ran last still reported a correct-looking value. Every
    call is appended and every entry is asserted on below.

    The wrappers DELEGATE to the real functions — nothing is stubbed out, so the
    gate still refuses, the inputs are still frozen and the money is still
    computed for real.
    """
    from services import invoices as invoices_mod

    calls: list[tuple[str, object]] = []

    def _wrap(name: str) -> None:
        real = getattr(invoices_mod, name)

        async def _wrapped(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            calls.append((name, kwargs.get("app_rates", _NO_RATES)))
            return await real(*args, **kwargs)

        monkeypatch.setattr(invoices_mod, name, _wrapped)

    for name in names:
        _wrap(name)
    return calls


async def _seed_priced_month(db_session, *, tenant: str, license_id: uuid.UUID):  # noqa: ANN001, ANN202
    """A closeable month on ONE priced service_key, with billable requests."""
    from models.db import AppCostRate

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(
        _metering_row(
            tenant, license_id=license_id, service_key="workers", request_count=_RATE_REQUESTS
        )
    )
    db_session.add(AppCostRate(app_key="workers", cost_per_request_usd=_RATE_REAL))
    await db_session.commit()


async def test_close_period_threads_one_app_rate_snapshot_through_all_three_consumers(
    db_session, test_settings, monkeypatch
) -> None:
    """⭐ ONE rate read per close, and the SAME object reaches all three consumers.

    Asserted three independent ways, because each catches a different way the
    threading can rot:

      1. the rate table is read EXACTLY ONCE for the whole close;
      2. every RECORDED CALL — a list, one entry per consumer — received the
         SAME mapping object, with a sentinel distinguishing "passed ``None``"
         from "not passed at all";
      3. the MONEY and the frozen provenance both show the first read's rate,
         while a second read would have returned a rate 999x larger.

    MUTATION CONTROL: delete ``app_rates=app_rates`` from close_period's
    ``generate_billing`` call (the form this defect had on ``main``). The
    generator then re-reads the table, the read count becomes 2, the recorded
    entry for ``generate_billing`` is the ``_NO_RATES`` sentinel, and the app
    line is billed at ``_RATE_RACED`` — so (1), (2) and (3) all go red. Deleting
    it from the gate or from ``_capture_inputs`` instead reds (1) and (2).
    """
    tenant = T("invoice-one-rate-read")
    license_id = uuid.uuid4()
    await _seed_priced_month(db_session, tenant=tenant, license_id=license_id)

    reads = _raced_app_rate_reads(monkeypatch, "workers")
    calls = _record_app_rates(
        monkeypatch,
        ("assert_all_service_keys_priced", "_capture_inputs", "generate_billing"),
    )

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    # (1) One statement against billing_app_cost_rates for the whole close.
    assert len(reads) == 1, (
        f"billing_app_cost_rates was read {len(reads)}x during one close; under READ "
        "COMMITTED those reads can disagree, so the gate, the frozen inputs and the "
        "billed lines can each be computed from a different rate set"
    )

    # (2) Every consumer, asserted per call — never a last-write collapse.
    assert [name for name, _ in calls] == [
        "assert_all_service_keys_priced",
        "_capture_inputs",
        "generate_billing",
    ], f"the three rate consumers did not all run exactly once: {[n for n, _ in calls]}"
    shared = calls[0][1]
    assert shared is not _NO_RATES, "the gate was not passed the shared rate snapshot at all"
    assert shared == {"workers": _RATE_REAL}, (
        f"the shared snapshot is not the table's real contents: {shared!r}"
    )
    assert [rates is shared for _, rates in calls] == [True, True, True], (
        "a consumer received a different rates object (or none at all) than the gate "
        f"ran on: {[(n, r if r is _NO_RATES else id(r)) for n, r in calls]}"
    )

    # (3) The figures and the provenance both name the FIRST read's rate. This is
    # what makes (1) and (2) non-vacuous: the threaded object is demonstrably the
    # one the money was computed from, not merely an argument that was passed.
    app_cost = sum(
        Decimal(entry["costs"]["app_request_cost_usd"]) for entry in inv.snapshot["licenses"]
    )
    assert app_cost == _RATE_REAL * _RATE_REQUESTS, (
        f"the app-request line billed {app_cost}, not {_RATE_REAL * _RATE_REQUESTS}: the "
        "generator computed money from a rate set the gate never saw"
    )
    # ⚠️ `Decimal(...)`: `_jsonable` freezes every money figure as a STRING, so a
    # bare comparison against a Decimal would fail on a correct invoice.
    assert [Decimal(r["cost_per_request_usd"]) for r in inv.inputs["app_cost_rates"]] == [
        _RATE_REAL
    ], "the frozen rate card is not the one the close was allowed on"
    assert inv.inputs["unpriced_service_keys"]["priced_keys_in_force"] == ["workers"], (
        "the frozen unpriced-key set was derived from a different rate read"
    )


async def test_generate_billing_still_self_fetches_rates_when_app_rates_is_omitted(
    db_session, test_settings, monkeypatch
) -> None:
    """⛔ THE READ SURFACE MUST NOT HAVE CHANGED. ``app_rates`` is OPTIONAL.

    ``routers/billing.py`` (``GET /api/billing/tenant/{id}``) and
    ``routers/dashboard.py`` call ``generate_billing`` with exactly the argument
    list used here and know nothing about rate snapshots. Making the parameter
    required — or making the self-fetch conditional on truthiness rather than on
    ``is None``, which would silently re-read for an empty-but-real rate set —
    turns the whole billing read surface into 500s.

    Asserts the report path still reads the table ITSELF (exactly once) and still
    produces the rate-driven figure.
    """
    tenant = T("invoice-rates-omitted")
    license_id = uuid.uuid4()
    await _seed_priced_month(db_session, tenant=tenant, license_id=license_id)

    reads = _raced_app_rate_reads(monkeypatch, "workers")

    _, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with gen_patch as mock_gen_fetch:
        mock_gen_fetch.return_value = _license_payload(license_id)
        # The routers' exact call shape: no app_rates, no licenses_data.
        report = await generate_billing(
            tenant, _MONTH, db_session, test_settings, token_provider=None
        )

    assert len(reads) == 1, (
        f"the report path read the rate table {len(reads)}x; it must read it once for itself"
    )
    app_cost = sum(Decimal(entry["costs"]["app_request_cost_usd"]) for entry in report["licenses"])
    assert app_cost == _RATE_REAL * _RATE_REQUESTS, (
        "the report path lost the app-request charge when app_rates was omitted"
    )

    # The parameter is keyword-usable and defaults to None — the property the
    # routers depend on, stated where a signature change would break it.
    import inspect

    param = inspect.signature(generate_billing).parameters["app_rates"]
    assert param.default is None, "app_rates is not optional; every report caller breaks"
    assert param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD, (
        "app_rates changed kind; close_period passes it by keyword"
    )


# ---------------------------------------------------------------------------
# H. The PROVIDER-price gate — "unknown cost" must not freeze as $0.00
#
# The other half of the pricing question from section F's app-RATE gate, and a
# different table: `provider_pricing` (what the tokens cost us) rather than
# `billing_app_cost_rates` (what we charge per request). `services.pricing`
# refuses to guess and returns None, `cost_flusher` stores that refusal as
# `cost_usd IS NULL`, and `rollup_engine`'s `coalesce(sum(cost_usd), 0)` then
# stores a hard $0.000000 beside a real token count — at which point "free" and
# "unknown" are the same bytes. `CostDaily.unpriced_event_count` (alembic 0031)
# is the only column that still knows the difference.
# ---------------------------------------------------------------------------

#: A month whose every day sits ABOVE `_UNPRICED_EVENT_COUNT_MEASURED_FROM`, so a
#: `unpriced_event_count` of 0 on its rows is a MEASUREMENT. `_MONTH_START`
#: (February 2026) is below the floor and therefore cannot be used to prove a
#: clean close — that is the distinction the floor exists to draw, and using the
#: existing fixture month here would make the success test vacuous.
_MEASURED_MONTH_START = date(2026, 10, 1)
_MEASURED_MONTH_END = date(2026, 11, 1)
_MEASURED_MONTH_DAYS = 31


def _unpriced_cost_row(
    tenant_id: str,
    *,
    license_id: uuid.UUID,
    bucket_date: date,
    provider: str,
    unpriced_event_count: int,
    service_key: str = "workers",
):  # noqa: ANN202
    """A rollup row whose cost is UNKNOWN, in the exact shape the rollup writes it.

    ``total_cost_usd`` is a hard ``0.000000`` beside a real token and request
    count — not an invention of this fixture but what
    ``coalesce(sum(cost_usd), 0)`` stores when every source row of the group was
    a pricing miss. The only thing separating it from a provider an operator
    priced free on purpose is ``unpriced_event_count``.

    ``provider`` differs from ``_seed_full_month``'s ``"test"`` so this is a new
    ``uq_cost_daily`` group rather than a conflicting upsert on an existing one.
    """
    return CostDaily(
        tenant_id=tenant_id,
        license_id=license_id,
        service_key=service_key,
        provider=provider,
        bucket_date=bucket_date,
        total_input_tokens=6_000,
        total_output_tokens=87,
        total_cost_usd=Decimal("0.000000"),
        request_count=4,
        unpriced_event_count=unpriced_event_count,
    )


async def test_close_period_refuses_a_provider_cost_it_could_not_price(
    db_session, test_settings
) -> None:
    """THE GATE: an unpriced provider/day REFUSES the close.

    THE MUTANT: delete the ``assert_no_unpriced_provider_cost`` call from
    ``close_period``. The close then succeeds and freezes a statement whose
    internal cost for ``openai_compatible`` is $0.00 against 6,087 tokens —
    under-stating cost and over-stating margin on an immutable row drawn from a
    gapless series. The invoice-row-count assertion below is what makes that
    mutant fail rather than merely changing an exception type.

    ⚠️ THE OFFENDING ROW IS BUCKETED IN FEBRUARY, i.e. BELOW the floor, ON
    PURPOSE. A count above zero can only have been written by code that knows the
    column exists, so it is a real measurement at any ``bucket_date``; the floor
    gates the CLEAN verdict, never the refusal. A gate that applied the floor to
    both would pass this and is the implementation this test exists to reject.
    """
    from services.invoices import UnpricedProviderCostError

    tenant = T("invoice-unpriced-provider")
    license_id = uuid.uuid4()
    unpriced_date = _MONTH_START + timedelta(days=9)

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(
        _unpriced_cost_row(
            tenant,
            license_id=license_id,
            bucket_date=unpriced_date,
            provider="openai_compatible",
            unpriced_event_count=3,
        )
    )
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        with pytest.raises(UnpricedProviderCostError) as exc:
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_MONTH_START,
                period_end=_MONTH_END,
                settings=test_settings,
            )

    assert exc.value.dates == [unpriced_date]
    assert exc.value.providers == ["openai_compatible"]
    # The message has to be actionable on its own: WHICH day, WHICH provider,
    # WHICH table. An operator reading only the log line must not have to guess.
    assert unpriced_date.isoformat() in str(exc.value), "the refusal does not name the date"
    assert "openai_compatible" in str(exc.value), "the refusal does not name the provider"
    assert "provider_pricing" in str(exc.value), (
        "the refusal does not name the table an operator has to fix"
    )

    # ⛔ A DISTINCT ERROR, NOT THE APP-RATE ONE. Collapsing the two would send an
    # operator to billing_app_cost_rates for a provider_pricing gap.
    from services.invoices import UnpricedServiceKeyError

    assert not isinstance(exc.value, UnpricedServiceKeyError)
    assert "billing_app_cost_rates" not in str(exc.value)

    # Refused BEFORE the one HTTP call and before any row exists to clean up.
    assert mock_close_fetch.call_count == 0, "the gate ran after the licences fetch"

    rows = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == tenant)
            )
        )
        .scalars()
        .all()
    )
    assert rows == [], "a refused close still wrote an invoice row"


async def test_close_period_closes_a_month_measured_clean_above_the_floor(
    db_session, test_settings
) -> None:
    """⛔ THE REGRESSION GUARD. A month the gate CAN read, and reads as clean, closes.

    A refusal gate that cannot be satisfied is an outage, so this is the half that
    keeps the new error honest. The coverage assertion ABOVE the close is what
    stops the test being vacuous: it proves the 31 rows were actually MEASURED at
    zero rather than merely unreadable — a period the gate cannot read at all also
    closes, and would pass a test that only asserted the success.
    """
    from services.invoices import assert_no_unpriced_provider_cost

    tenant = T("invoice-priced-clean")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(
        db_session,
        tenant_id=tenant,
        license_id=license_id,
        month_start=_MEASURED_MONTH_START,
        month_end=_MEASURED_MONTH_END,
    )
    await db_session.commit()

    coverage = await assert_no_unpriced_provider_cost(
        db_session,
        tenant_id=tenant,
        period_start=_MEASURED_MONTH_START,
        period_end=_MEASURED_MONTH_END,
    )
    assert coverage.measured_rows == _MEASURED_MONTH_DAYS, (
        "the fixture's rows were not counted as measured, so the close below would "
        "succeed for the wrong reason"
    )
    assert coverage.unmeasurable_rows == 0
    assert coverage.unmeasurable_dates == []

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MEASURED_MONTH_START,
            period_end=_MEASURED_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    assert inv.id is not None
    assert inv.period_start == _MEASURED_MONTH_START


async def test_a_zero_below_the_floor_is_not_counted_as_clean(db_session) -> None:
    """⛔ A 0 ON A PRE-0031 ROW IS AN ABSENCE OF MEASUREMENT, NOT A MEASUREMENT OF ABSENCE.

    alembic 0031 added ``unpriced_event_count NOT NULL DEFAULT 0`` and ships no
    backfill on purpose, so every row bucketed before the floor reads exactly like
    a day on which nothing was unpriced. ``billing_cost_daily`` has no
    ``created_at``/``updated_at``, so the two cases are indistinguishable after the
    fact and the gate must not claim either.

    THE MUTANT: drop the floor and count every 0 as measured — i.e. in
    ``assert_no_unpriced_provider_cost`` replace the ``measured_rows`` comprehension's
    ``if g.bucket_date >= _UNPRICED_EVENT_COUNT_MEASURED_FROM`` with nothing. This
    test then fails on ``measured_rows == 0``, having been told 28 February rows
    were verified clean by a column that did not exist when they were written.
    """
    from services.invoices import (
        _UNPRICED_EVENT_COUNT_MEASURED_FROM,
        assert_no_unpriced_provider_cost,
    )

    tenant = T("invoice-unpriced-prefloor")
    license_id = uuid.uuid4()

    # The fixture month really is below the floor — stated, not assumed, so this
    # test cannot quietly become a second copy of the one above.
    assert _MONTH_END <= _UNPRICED_EVENT_COUNT_MEASURED_FROM

    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()

    coverage = await assert_no_unpriced_provider_cost(
        db_session,
        tenant_id=tenant,
        period_start=_MONTH_START,
        period_end=_MONTH_END,
    )
    assert coverage.measured_rows == 0, (
        "a pre-0031 row's 0 was counted as a measurement; the gate is reporting "
        "coverage it does not have"
    )
    assert coverage.unmeasurable_rows == 28
    assert len(coverage.unmeasurable_dates) == 28
    assert coverage.unmeasurable_dates[0] == _MONTH_START


async def test_the_measured_floor_is_a_boundary_on_the_day_after_0031(db_session) -> None:
    """The floor is exact: the day BEFORE it is unmeasurable, the day ON it is measured.

    0031's own ``Create Date`` is 2026-09-11 and the floor is the day AFTER, because
    a row bucketed on the migration's own date may have been written that morning
    by a pod predating the deploy — and ``billing_cost_daily`` carries no write
    timestamp to tell. An off-by-one either way is invisible in production (both
    sides read 0) and this is the only thing that pins it.
    """
    from services.invoices import (
        _UNPRICED_EVENT_COUNT_MEASURED_FROM,
        assert_no_unpriced_provider_cost,
    )

    tenant = T("invoice-unpriced-boundary")
    license_id = uuid.uuid4()
    floor = _UNPRICED_EVENT_COUNT_MEASURED_FROM
    below = floor - timedelta(days=1)

    for d in (below, floor):
        db_session.add(
            _unpriced_cost_row(
                tenant,
                license_id=license_id,
                bucket_date=d,
                provider="test",
                unpriced_event_count=0,
            )
        )
    await db_session.commit()

    coverage = await assert_no_unpriced_provider_cost(
        db_session,
        tenant_id=tenant,
        period_start=below,
        period_end=floor + timedelta(days=1),
    )
    assert coverage.measured_rows == 1
    assert coverage.unmeasurable_rows == 1
    assert coverage.unmeasurable_dates == [below]


async def test_unpriced_system_cost_does_not_refuse_a_close(db_session, test_settings) -> None:
    """NEGATIVE CONTROL for the REUSED predicate — system/S2S spend is not a customer line.

    ``CostDaily.license_attributed_clause()`` is the exact predicate
    ``generate_billing`` bills on, and the gate reuses it rather than re-spelling
    the NULL-licence exclusion. Unpriced cost under ``NULL_LICENSE_ID`` never
    becomes a customer figure, so refusing on it would block closes that are not
    defective — and the sentinel carries the majority of measured spend, so this is
    the common case rather than an edge.

    THE MUTANT: delete ``CostDaily.license_attributed_clause()`` from the gate's
    ``where()``. This test then fails with ``UnpricedProviderCostError``.
    """
    tenant = T("invoice-unpriced-system")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(
        _unpriced_cost_row(
            tenant,
            license_id=NULL_LICENSE_ID,
            bucket_date=_MONTH_START + timedelta(days=3),
            provider="system-only-provider",
            unpriced_event_count=9,
        )
    )
    await db_session.commit()

    # Non-vacuity: the unpriced row really is there and really is unpriced — the
    # allow proves nothing if the fixture did not produce the shape.
    total_unpriced = (
        await db_session.execute(
            select(func.coalesce(func.sum(CostDaily.unpriced_event_count), 0)).where(
                CostDaily.tenant_id == tenant
            )
        )
    ).scalar_one()
    assert total_unpriced == 9

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)

        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    assert inv.id is not None


# --------------------------------------------------------------------------- #
# assert_tenant_invoiceable + the no-billable-basis skip (2026-09-13)
#
# ⭐ WHY THESE TESTS ASSERT THE INVOICE-NUMBER COUNTER AND NOT ONLY THE
# EXCEPTION. The gates exist to stop a number being spent, and a number is spent
# by `issue()` from a GAPLESS counter row — so "it raised" is the cheap half of
# the claim. A gate that raised AFTER drawing would satisfy a `pytest.raises`
# and still have burned the thing the gate is for. Every test below therefore
# reads `billing_invoice_counters` before and after.
# --------------------------------------------------------------------------- #


def _registry_stub(monkeypatch, response):  # noqa: ANN001, ANN201
    """Replace the autouse ``/internal/tenants`` answer. Chains like
    :func:`_byo_mode_stub` — every other URL goes on to the real method."""
    real_get = httpx.AsyncClient.get
    calls = {"n": 0}

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if str(url).endswith("/internal/tenants"):
            calls["n"] += 1
            return response(calls["n"])
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return calls


async def _counter_state(session) -> list[tuple[str, int]]:  # noqa: ANN001
    rows = (await session.execute(select(BillingInvoiceCounter))).scalars().all()
    return sorted((r.key, r.last_number) for r in rows)


async def test_close_refuses_a_tenant_absent_from_the_registry(
    db_session, test_settings, monkeypatch
) -> None:
    """⭐ THE MEASURED PRODUCTION CASE. On 2026-09-13 four tenant_ids carried
    $23.17 of cost-ledger spend while appearing in NO ``licenses.tenants`` row.
    ``services.byo_mode`` maps their licences 404 to ``byo_required``, which
    ``effective_billing_mode`` then keeps as the configured commercial mode — so
    before this gate a close would have minted a MARKED-UP statement for a tenant
    that does not exist.

    A raw ``uuid.uuid4()`` is the suite's established spelling for "not
    registered" (see ``conftest.T``'s docstring), so this test says it the same
    way the flusher's exclusion tests do.
    """
    tenant = str(uuid.uuid4())  # NOT T(): deliberately unregistered
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()
    before = await _counter_state(db_session)

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as m1, gen_patch as m2:
        m1.return_value = _license_payload(license_id)
        m2.return_value = _license_payload(license_id)
        with pytest.raises(TenantNotInvoiceableError) as exc:
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_MONTH_START,
                period_end=_MONTH_END,
                settings=test_settings,
            )

    assert exc.value.reason == "tenant_unregistered"
    rows = (
        (await db_session.execute(select(BillingInvoice).where(BillingInvoice.tenant_id == tenant)))
        .scalars()
        .all()
    )
    assert list(rows) == [], "a refused close must leave NO invoice row behind"
    assert await _counter_state(db_session) == before, (
        "a refused close drew an invoice number — the gapless series cannot be repaired"
    )


async def test_close_refuses_an_archived_tenant(db_session, test_settings, monkeypatch) -> None:
    """``rmt-e2e`` (archived 2026-09-03, ``kind='test'``, ``byo_mode='byo_required'``)
    had August cost rows, so an armed sweep would have invoiced it commercially.

    ⭐ AND LICENCES ITSELF IS STRICTER THAN THAT: its mint path refuses an archived
    tenant outright. A close that bills a tenant licences would not even issue a
    licence for is laxer than the service it bills for.
    """
    tenant = T("invoice-archived")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()
    before = await _counter_state(db_session)

    _registry_stub(
        monkeypatch,
        lambda _n: httpx.Response(
            200,
            json=[{"id": tenant, "name": "rmt-e2e-like", "kind": "test", "archived": True}],
        ),
    )

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as m1, gen_patch as m2:
        m1.return_value = _license_payload(license_id)
        m2.return_value = _license_payload(license_id)
        with pytest.raises(TenantNotInvoiceableError) as exc:
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_MONTH_START,
                period_end=_MONTH_END,
                settings=test_settings,
            )

    assert exc.value.reason == "tenant_archived"
    assert await _counter_state(db_session) == before


async def test_close_refuses_when_the_registry_cannot_answer(
    db_session, test_settings, monkeypatch
) -> None:
    """⛔ A NON-ANSWER IS NOT AN ABSENCE, and conflating them is what made the
    fail-open reader unusable for a gate: ``get_tenant_info`` returns ``{}`` for a
    500 exactly as it does for an empty registry, so a gate built on it would call
    every tenant in the estate a phantom the moment licences blipped — and bill
    none of them, while reading as enforcement.

    The strict reader raises instead, which is the same contract
    ``SeatCountUnavailableError`` and ``ByoModeUnavailableError`` already carry.
    """
    tenant = T("invoice-registry-down")
    license_id = uuid.uuid4()

    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()
    before = await _counter_state(db_session)

    _registry_stub(monkeypatch, lambda _n: httpx.Response(503, json={"error": "down"}))

    with pytest.raises(TenantRegistryUnavailableError):
        await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
        )
    assert await _counter_state(db_session) == before


async def test_close_skips_a_period_that_charges_nothing_and_draws_no_number(
    db_session, test_settings, monkeypatch
) -> None:
    """⭐ THE EXACT SHAPE OF ``INV-2026-000001``: an INTERNAL tenant with real
    platform cost and nothing charged, which was nevertheless issued — $0.00,
    August, ``n8n-dev-portal-realm``, ``cost_centre=ci`` — consuming number 1 of
    a gapless, irreversible series. ~10 further internal cost centres had activity
    in the same period, so an armed loop would have spent a number a month on each.

    ``byo_mode='local_allowed'`` is what makes it internal:
    ``effective_billing_mode`` collapses it to ``BILLING_MODE_INTERNAL`` and the
    charged columns all come out zero while ``internal_cost_usd`` does NOT.

    ⭐ THAT ASYMMETRY IS THE TEST. It is the reason the skip reads
    ``_CHARGED_MONEY_COLUMNS`` rather than every money column: had
    ``internal_cost_usd`` been in the set, this period would look billable and the
    skip would never fire — indistinguishable, from outside, from not having been
    written.
    """
    tenant = T("invoice-internal-zero")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    await db_session.commit()
    before = await _counter_state(db_session)

    _byo_mode_stub(monkeypatch, lambda _n: httpx.Response(200, json={"byo_mode": "local_allowed"}))

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as m1, gen_patch as m2:
        m1.return_value = _license_payload(license_id)
        m2.return_value = _license_payload(license_id)
        with pytest.raises(PeriodNotBillableError):
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_MONTH_START,
                period_end=_MONTH_END,
                settings=test_settings,
            )

    rows = (
        (await db_session.execute(select(BillingInvoice).where(BillingInvoice.tenant_id == tenant)))
        .scalars()
        .all()
    )
    assert list(rows) == [], "a skipped period must leave NO invoice row"
    assert await _counter_state(db_session) == before, (
        "the skip ran AFTER the number was drawn — it must run before"
    )


def test_charged_money_columns_exclude_the_three_that_are_not_charges() -> None:
    """⛔ PINS THE SUBSET ITSELF, because the whole behaviour turns on which
    columns are in it and the failure is SILENT in both directions: add
    ``internal_cost_usd`` and the skip never fires (every internal cost centre
    looks billable); drop ``access_charge_usd`` and a customer on a flat fee with
    no usage gets silently skipped instead of invoiced.
    """
    # ⛔ THE COMPONENT LINES ARE THE ONES THAT MATTER HERE, and this assertion is
    # the one that would have caught the first version of the rule. Measured: an
    # internal-mode close carries llm_usage_usd=+70.00 and llm_markup_usd=-70.00
    # netting to total_usd=0.00, so reading a component concludes "billable" for
    # every internal cost centre and the skip never fires in production.
    for not_a_charge in (
        "internal_cost_usd",
        "margin_usd",
        "excluded_unbillable_usd",
        "seat_count",
        "llm_usage_usd",
        "llm_markup_usd",
        "subtotal_usd",
        "platform_fee_usd",
    ):
        assert not_a_charge not in _CHARGED_MONEY_COLUMNS, (
            f"{not_a_charge} is not proof the customer owes anything; "
            "including it stops the skip firing"
        )
    # ⚠️ And these three must STAY, each for its own reason: the total is what is
    # owed, and the other two are the footprint of a commercial term someone
    # configured — dropping either silently never invoices a customer who has one.
    for is_a_charge in ("total_usd", "access_charge_usd", "seat_fee_usd"):
        assert is_a_charge in _CHARGED_MONEY_COLUMNS, (
            f"{is_a_charge} IS a charge; excluding it silently skips a real invoice"
        )
    assert set(_CHARGED_MONEY_COLUMNS) <= set(_MONEY_COLUMNS), (
        "every charged column must be a real money column the digest covers"
    )


# ---------------------------------------------------------------------------
# L. The candidate set — who the close even ASKS about
# ---------------------------------------------------------------------------
#
# ⛔ WHY THIS SECTION EXISTS, AND IT IS THE MOST EXPENSIVE HOLE THE FILE COVERS.
# `close_due_periods` built its candidate set from `billing_cost_daily` +
# `billing_metering_daily` alone. `tenant_access_charge` was never consulted —
# while `_CHARGED_MONEY_COLUMNS` carries `access_charge_usd` and `seat_fee_usd`
# precisely so a configured commercial term produces a statement even when the
# total nets to zero. So a customer on a flat $5,000/month access charge who made
# no API calls in a month was not invoiced, not refused and not skipped: nothing
# reached a gate, so there was no row, no log line and nothing to alert on. The
# platform overview meanwhile counted that $5,000 as revenue (`routers/billing.py`
# via `active_access_charges_all_tenants`), and `_measure_invoice_close_lag` used
# the SAME blind definition, so the backlog gauge could not see the hole either.
#
# Section G proves the sweep's PARTITIONING. Nothing in it — or anywhere else —
# proved its ENROLMENT, which is a different property: every test in G seeds usage
# first, so each of its tenants is a candidate under either definition and all of
# them stay green with the access axis deleted.
#
# ⚠️ THE FLEET-WIDE BUCKET GAP IS WHY EVERY TEST HERE SEEDS A SECOND TENANT.
# `assert_no_bucket_gap`'s presence query carries no tenant filter (see its
# docstring: one tenant's quiet day is not evidence of anything, nobody's is), so
# a period in which the ONLY tenant has zero cost rows refuses EVERY candidate
# with `BucketGapError`. `_padding_tenant` supplies the month's coverage and is a
# genuine candidate in its own right — which also makes it each test's
# non-vacuity control: if it stops closing, the fixture broke, not the axis.
#
# Imports are function-local, per the convention stated in section G.


def _access_charge(
    tenant_id: str,
    *,
    amount_usd: str,
    cadence: str = "monthly",
    effective_from: datetime = datetime(2025, 1, 1, tzinfo=UTC),
    effective_until: datetime | None = None,
) -> TenantAccessCharge:
    """One tenant-wide (NULL ``service_key``) access charge.

    ``cadence="monthly"`` on purpose in every test here: it is the cadence that
    bills in EVERY effective month, so a test asserting candidacy for a chosen
    month is not also asserting `_applies_this_month`'s one_time bucketing. It
    also keeps `assert_seat_count_available` inert — that gate is scoped to
    tenants carrying a `per_seat` term, and an unwanted seats lookup here would
    refuse the close for a reason that has nothing to do with the candidate set.
    """
    return TenantAccessCharge(
        tenant_id=tenant_id,
        service_key=None,
        amount_usd=Decimal(amount_usd),
        cadence=cadence,
        effective_from=effective_from,
        effective_until=effective_until,
    )


async def _seed_padding_tenant(session, *, tenant_id: str, license_id: uuid.UUID) -> None:
    """Cover every day of ``_MONTH`` so the FLEET-WIDE bucket-gap gate passes.

    Not a detail of the fixture: without it the access-charge tenant's close
    refuses with ``BucketGapError`` before the candidate set is ever the thing
    under test, and the assertion would pass or fail on the wrong gate.
    """
    session.add(_tenant_scoped_markup_rule(tenant_id, "30.00"))
    await _seed_full_month(session, tenant_id=tenant_id, license_id=license_id)


async def test_close_due_periods_invoices_an_access_charge_with_zero_usage(
    db_session, db_session_factory, test_settings
) -> None:
    """⭐ THE TEST THIS SECTION EXISTS FOR, AND IT GOES RED ON THE OLD DEFINITION.

    A tenant with an active ``monthly`` access charge for the closed month and
    ZERO rows in BOTH ``billing_cost_daily`` and ``billing_metering_daily`` must
    be a candidate and must end the sweep holding a statement.

    ⛔ WHY IT CANNOT PASS WITHOUT THE FIX. Under
    ``tenant_ids = sorted(cost_tenants | metering_tenants)`` this tenant appears
    in neither set — it has no row in either table, by construction — so the loop
    never reaches it, ``close_period`` is never called for it, and `created`
    contains ONLY the padding tenant. There is no other path that could invoice
    it: ``close_due_periods`` is the sole automated minting path, and both the
    `created` assertion and the direct ``billing_invoices`` read below name THIS
    tenant. Deleting the access axis from ``find_close_candidates`` therefore
    fails both of them, and the padding tenant's assertions stay green — which is
    what attributes the red to the axis and not to the fixture.

    ⚠️ ASSERTS THE MONEY ONLY AS A FLOOR (``access_charge_usd == 5000.00``, the
    seeded figure). How money is computed is not this test's subject; that the
    charge SURVIVED to the frozen row is, because a candidate that closes at
    $0.00 would be skipped by the ``_CHARGED_MONEY_COLUMNS`` gate and prove the
    opposite of what the section claims.
    """
    from models.db import MeteringDaily
    from services.invoices import close_due_periods

    charged = T("candidate-access-charge-only")
    padding = T("candidate-padding-usage")
    padding_license = uuid.uuid4()

    # The subject: a $5,000 flat monthly term and NOT ONE usage row anywhere.
    db_session.add(_access_charge(charged, amount_usd="5000.00"))
    await _seed_padding_tenant(db_session, tenant_id=padding, license_id=padding_license)
    await db_session.commit()

    # Non-vacuity for the premise itself: the subject really has no usage. If a
    # fixture ever seeds it some, this test silently stops testing the axis.
    for table, column in (
        (CostDaily, CostDaily.tenant_id),
        (MeteringDaily, MeteringDaily.tenant_id),
    ):
        rows = (
            await db_session.execute(
                select(func.count()).select_from(table).where(column == charged)
            )
        ).scalar_one()
        assert rows == 0, (
            f"{table.__name__} holds {rows} row(s) for the subject tenant; this test "
            "can only speak about the access axis while BOTH usage tables are empty for it"
        )

    payload = _license_payload(padding_license)
    close_patch, gen_patch = _patch_fetch_licenses(payload)
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = payload
        mock_gen_fetch.return_value = payload

        outcome = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

    closed_by_tenant = {inv.tenant_id: inv for inv in outcome.created}

    # ⛔ THE ASSERTION. Delete the access axis from find_close_candidates and this
    # is the line that goes red.
    assert charged in closed_by_tenant, (
        "a tenant with a $5,000 active access charge and zero usage was never "
        "invoiced — it was not even a CANDIDATE, so no gate refused or skipped it "
        f"and nothing anywhere records the omission. Closed this sweep: "
        f"{sorted(closed_by_tenant)}; refused={outcome.refused} "
        f"skipped={outcome.skipped} failed={outcome.failed}"
    )

    # The control: the usage tenant still closes, so a red above is the axis and
    # not a broken fixture or a gate refusing the whole sweep.
    assert padding in closed_by_tenant, (
        "the padding tenant did not close either — the sweep failed for a reason "
        "unrelated to the candidate set, so the assertion above is uninterpretable"
    )
    assert outcome.failed == 0, "a candidate died of a defect, not a refusal"

    invoice = closed_by_tenant[charged]
    assert invoice.period_start == _MONTH_START
    assert invoice.period_end == _MONTH_END
    # The term survived to the frozen row — otherwise the close would have been
    # skipped as having no billable basis, and candidacy alone would be hollow.
    assert invoice.access_charge_usd == Decimal("5000.00000000"), (
        f"the statement froze access_charge_usd={invoice.access_charge_usd}; a $0.00 "
        "here means the tenant was enrolled but its term was not billed"
    )

    # And the row is really in the table, not just in the return value.
    persisted = (
        (
            await db_session.execute(
                select(BillingInvoice.id).where(BillingInvoice.tenant_id == charged)
            )
        )
        .scalars()
        .all()
    )
    assert len(persisted) == 1, (
        f"expected exactly one persisted invoice for the access-charge tenant, got {persisted}"
    )


async def test_close_due_periods_ignores_a_tenant_with_neither_usage_nor_a_charge(
    db_session, db_session_factory, test_settings
) -> None:
    """THE NEGATIVE CONTROL. No usage, no access charge → not a candidate.

    Without this, the test above is satisfied by any mutation that widens the
    candidate set to "every registered tenant" — which would close a numbered
    $0.00 statement for all ~49 internal cost centres. ``T()`` registers what it
    mints, so this tenant is a REGISTERED tenant that is simply owed nothing;
    the distinction between "registered" and "billable" is exactly what a
    too-wide union would erase.

    ⚠️ Asserted as ABSENCE FROM ALL FOUR OUTCOMES, not merely "no invoice". A
    non-candidate must not appear as `created`, `unchanged`, or in the `refused`
    / `skipped` counts, because reaching a gate at all means it WAS enrolled —
    and a tenant enrolled and then skipped is a different (and noisier) state
    from one never asked about. The counts are pinned to the padding tenant's
    own outcome, so an extra refusal or skip from anywhere shows up here.
    """
    from services.invoices import close_due_periods, find_close_candidates

    idle = T("candidate-idle-no-terms")
    padding = T("candidate-idle-padding")
    padding_license = uuid.uuid4()

    await _seed_padding_tenant(db_session, tenant_id=padding, license_id=padding_license)
    await db_session.commit()

    async with db_session_factory() as session:
        candidates = await find_close_candidates(
            session, period_start=_MONTH_START, period_end=_MONTH_END
        )
    assert idle not in candidates.tenant_ids, (
        f"a tenant with no usage and no access charge was enrolled as a candidate: "
        f"cost={sorted(candidates.cost)} metering={sorted(candidates.metering)} "
        f"access_charge={sorted(candidates.access_charge)}"
    )
    # Non-vacuity: the query DOES find the tenant that qualifies, so the absence
    # above is a verdict rather than an empty result from a broken query.
    assert padding in candidates.tenant_ids, (
        "the padding tenant was not a candidate either — find_close_candidates "
        "returned nothing useful and the absence above proves nothing"
    )

    payload = _license_payload(padding_license)
    close_patch, gen_patch = _patch_fetch_licenses(payload)
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = payload
        mock_gen_fetch.return_value = payload

        outcome = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

    assert [inv.tenant_id for inv in outcome.created] == [padding], (
        "the sweep closed something other than the one tenant with usage"
    )
    assert outcome.unchanged == []
    assert (outcome.refused, outcome.skipped, outcome.failed) == (0, 0, 0), (
        "the idle tenant reached a GATE — it was enrolled and then declined, "
        "which is a different state from never being asked about"
    )


async def test_close_due_periods_ignores_an_access_charge_that_ended_in_the_period(
    db_session, db_session_factory, test_settings
) -> None:
    """THE BOUNDARY CONTROL, and it is where ``effective_until``'s STRICT ``>`` bites.

    ``services.access_charge`` qualifies a row with ``effective_until IS NULL OR
    effective_until > bill_ts``, where ``bill_ts`` is the LAST INSTANT of the
    period (``_bill_ts``). The comparison is strict, so a charge closed ANYWHERE
    inside the month — here on the 15th, after two weeks of being effective —
    does not qualify for that month at all.

    ⭐ AND THAT ASYMMETRY IS THE BEHAVIOUR UNDER TEST, not an accident being
    documented. ``generate_billing`` bills the access axis through the same
    effective-dated match at the same instant, so such a charge computes to
    $0.00. A candidate rule generous enough to enrol it would nominate a tenant
    whose statement then charges nothing, and the ``_CHARGED_MONEY_COLUMNS`` skip
    would decline it on this and every future sweep. Candidate membership and
    billability have to be decided by ONE predicate; this test pins them to the
    same one.

    ⚠️ The ended charge is seeded on its OWN tenant with no usage, so its
    exclusion is observable. Hung on a tenant with usage it would be invisible —
    that tenant is a candidate either way.
    """
    from services.invoices import close_due_periods, find_close_candidates

    expired = T("candidate-charge-ended-midmonth")
    padding = T("candidate-boundary-padding")
    padding_license = uuid.uuid4()

    db_session.add(
        _access_charge(
            expired,
            amount_usd="5000.00",
            # Effective for the first half of the closed month and then closed.
            # `bill_ts` is 2026-02-28T23:59:59.999999Z, so `effective_until >
            # bill_ts` is FALSE and the row does not apply to 2026-02.
            effective_until=datetime(2026, 2, 15, tzinfo=UTC),
        )
    )
    await _seed_padding_tenant(db_session, tenant_id=padding, license_id=padding_license)
    await db_session.commit()

    async with db_session_factory() as session:
        candidates = await find_close_candidates(
            session, period_start=_MONTH_START, period_end=_MONTH_END
        )
    assert expired not in candidates.access_charge, (
        "a charge closed on 2026-02-15 was treated as effective for 2026-02; the "
        "candidate query is resolving the access axis at a different instant than "
        "generate_billing bills it at, so the two will disagree about the money"
    )
    assert expired not in candidates.tenant_ids, (
        f"the expired-charge tenant was enrolled anyway: cost={sorted(candidates.cost)} "
        f"metering={sorted(candidates.metering)}"
    )

    # ⛔ THE CONTROL THAT MAKES THE ABSENCE MEAN SOMETHING: the SAME charge, on the
    # same period, still open. If this half failed, the assertion above would pass
    # for a tenant the axis cannot see at all and would say nothing about
    # `effective_until`.
    still_open = T("candidate-charge-still-open")
    db_session.add(_access_charge(still_open, amount_usd="5000.00", effective_until=None))
    await db_session.commit()

    async with db_session_factory() as session:
        candidates = await find_close_candidates(
            session, period_start=_MONTH_START, period_end=_MONTH_END
        )
    assert still_open in candidates.access_charge, (
        "an OPEN access charge was not seen by the axis either — the exclusion "
        "above cannot be attributed to effective_until"
    )

    payload = _license_payload(padding_license)
    close_patch, gen_patch = _patch_fetch_licenses(payload)
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = payload
        mock_gen_fetch.return_value = payload

        outcome = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

    closed = {inv.tenant_id for inv in outcome.created}
    assert expired not in closed, "a period whose only term had ended was invoiced anyway"
    assert {padding, still_open} <= closed, (
        f"the two tenants that DO owe for 2026-02 did not both close: {sorted(closed)}"
    )


# ---------------------------------------------------------------------------
# P. The APP-REQUEST stream, and the identity that makes it enforceable
#
# `services/billing.py` bills TWO streams into one `totals.customer_cost_usd`:
#     customer_cost = Σ(app_request_billed + llm_billed) + access_total + seat_fee_total
# Until alembic 0033 only the llm stream had component columns, so the stored
# components understated the stored total by EXACTLY the app-request billed
# amount — and `reproduce`'s `money_matches_snapshot` could not see it, because
# it re-derives with the same `_money_from_snapshot` that dropped the term and
# then compares that derivation against the columns it produced. Both sides
# shared the defect, so the check agreed with itself.
#
# ⚠️ MEASURED 2026-09-13: `billing_app_cost_rates` held 2 rows (`workers`,
# `fr-backend`), BOTH priced 0.00000000 — so the live imbalance was zero. That is
# a CONFIGURATION STATE, not a safeguard, which is why the tests below split into
# a priced fixture (where the defect is visible) and a zero-rate control (the
# estate's actual configuration, which must keep closing unchanged).
# ---------------------------------------------------------------------------


def _identity_residual(inv) -> Decimal:  # noqa: ANN001 - ORM row
    """``total_usd − Σcomponents`` off the STORED row, via the gate's own set.

    Reads :data:`_TOTAL_COMPONENT_COLUMNS` rather than re-listing the six names,
    so a component added to the gate without being added here cannot leave these
    tests asserting an identity narrower than the one the close enforces.
    """
    return Decimal(inv.total_usd) - sum(
        (Decimal(getattr(inv, name)) for name in _TOTAL_COMPONENT_COLUMNS), Decimal("0")
    )


async def test_the_app_request_stream_reaches_its_own_invoice_columns(
    db_session, test_settings
) -> None:
    """⭐ THE KILLING TEST FOR THE DERIVATION: the app-request pair is real money.

    THE MUTANT THIS KILLS is `_money_from_snapshot` as it stood before alembic
    0033 — drop the `app_request_usage_usd`/`app_request_markup_usd` derivation
    (or hardcode either to 0) and this test goes red twice over: the equalities
    against the snapshot's own per-licence sums fail, and the close itself now
    refuses with `InvoiceMoneyImbalanceError` because the components no longer
    reach the total.

    ⚠️ A PRICED FIXTURE IS THE WHOLE POINT. `_seed_priced_month` seeds
    `AppCostRate(workers, _RATE_REAL)` — a NON-ZERO rate — against
    `_RATE_REQUESTS` metered requests. Run against the estate's live all-zero
    rate table this test would pass over the defect, because the missing term
    would be 0: exactly the reason the imbalance stayed latent in production.
    """
    tenant = T("invoice-app-request-columns")
    license_id = uuid.uuid4()
    await _seed_priced_month(db_session, tenant=tenant, license_id=license_id)

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)
        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    # The snapshot's OWN per-licence figures, summed the way the derivation must
    # sum them. Asserting against these rather than only against literals is what
    # makes this a test of the derivation and not of my arithmetic.
    incurred = sum(
        (Decimal(e["costs"]["app_request_cost_usd"]) for e in inv.snapshot["licenses"]),
        Decimal("0"),
    )
    billed = sum(
        (Decimal(e["costs"]["app_request_billed_usd"]) for e in inv.snapshot["licenses"]),
        Decimal("0"),
    )
    assert inv.app_request_usage_usd == incurred, (
        f"app_request_usage_usd={inv.app_request_usage_usd} is not the snapshot's "
        f"summed app_request_cost_usd ({incurred})"
    )
    assert inv.app_request_markup_usd == billed - incurred, (
        f"app_request_markup_usd={inv.app_request_markup_usd} is not "
        f"billed − incurred ({billed} − {incurred})"
    )

    # NON-VACUITY. Both equalities above hold trivially at 0/0 under the estate's
    # live rate table, so each figure is also pinned to the value the seeded rate
    # and markup rule must produce: 100 requests × $0.01 = $1.00 incurred, +30%.
    assert inv.app_request_usage_usd == _RATE_REAL * _RATE_REQUESTS, (
        "the app-request line did not bill the seeded rate; the fixture is inert "
        "and the equalities above proved nothing"
    )
    assert inv.app_request_markup_usd > 0, "a 30% markup produced no app-request markup figure"

    # And the identity the gate enforces actually holds on a real closed row —
    # the claim the two columns exist to make true.
    assert _identity_residual(inv) == 0, (
        f"total_usd={inv.total_usd} is not the sum of its components "
        f"(residual {_identity_residual(inv)})"
    )


async def test_close_refuses_a_total_its_components_cannot_account_for(
    db_session, test_settings, monkeypatch
) -> None:
    """⭐ THE KILLING TEST FOR THE GATE: delete the check and this goes green.

    ⛔ THE INJECTED SHAPE IS THE HISTORICAL DEFECT, NOT AN ARBITRARY ONE. The
    wrapper below zeroes exactly the two keys `_money_from_snapshot` failed to
    derive before alembic 0033 and changes nothing else — so what
    `close_period` is handed is byte-for-byte what it was handed on `main`, and
    what the gate must refuse is precisely what it used to freeze.

    ⚠️ IT MUST BE INJECTED RATHER THAN SEEDED, because no input can produce this
    state any more: the derivation is now correct, so the only way to present the
    close with an unbalanced money set is to reach past it. That is also why this
    test cannot be replaced by one that seeds odd rates — a seeded imbalance would
    be testing the generator, and this is testing the gate.

    MUTATION CONTROL: remove the
    `assert_total_is_the_sum_of_its_components(...)` call from `close_period` and
    the close succeeds, `pytest.raises` fails, and the row assertions below never
    run. Remove the tolerance instead (`<=` → `<`) and this still passes, which is
    correct: a $1.30 residual is 130× the quantum and must not depend on it.
    """
    from services import invoices as invoices_mod

    tenant = T("invoice-identity-gate")
    license_id = uuid.uuid4()
    await _seed_priced_month(db_session, tenant=tenant, license_id=license_id)

    real = invoices_mod._money_from_snapshot
    #: What the dropped stream was actually worth, read off the SAME snapshot the
    #: close is computing from. Captured rather than hardcoded: a literal here
    #: would silently assume the resolved billing mode and markup, and would fail
    #: for the wrong reason the day either moves.
    dropped: dict[str, Decimal] = {}

    def _drop_the_app_request_pair(snapshot: dict) -> dict:
        money = real(snapshot)
        dropped["app_billed"] = sum(
            (
                Decimal(str((e.get("costs") or {}).get("app_request_billed_usd", "0")))
                for e in snapshot.get("licenses") or []
            ),
            Decimal("0"),
        )
        money["app_request_usage_usd"] = Decimal("0")
        money["app_request_markup_usd"] = Decimal("0")
        return money

    monkeypatch.setattr(invoices_mod, "_money_from_snapshot", _drop_the_app_request_pair)

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)
        with pytest.raises(InvoiceMoneyImbalanceError) as exc:
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_MONTH_START,
                period_end=_MONTH_END,
                settings=test_settings,
                issue_immediately=False,
            )

    # ⭐ THE RESIDUAL IS THE DEFECT'S OWN MAGNITUDE, not just "non-zero": the
    # dropped stream is the whole app-request BILLED amount ($1.00 incurred +30%),
    # so this equality pins that the gate measured the right gap rather than
    # tripping over an unrelated rounding.
    assert exc.value.residual == dropped["app_billed"], (
        f"the gate reported residual={exc.value.residual}, not the "
        f"{dropped['app_billed']} of app-request billing that was dropped — it is "
        "measuring something other than the missing stream"
    )
    # NON-VACUITY: with the estate's live all-zero rate table the dropped stream
    # is worth $0 and the equality above would hold on a gate that never fires.
    assert dropped["app_billed"] > 0, (
        "the fixture billed no app requests, so nothing was actually dropped and "
        "this test would pass with the gate deleted"
    )
    assert exc.value.tenant_id == tenant
    # The refusal names the figures, so an operator does not re-run the close to
    # see which line is wrong.
    assert set(exc.value.components) == set(_TOTAL_COMPONENT_COLUMNS)

    # ⛔ AND NOTHING WAS WRITTEN. The gate's entire value is that it fires BEFORE
    # the row exists: `billing_invoices` rows are immutable and `issue` draws from
    # a GAPLESS counter, so a refusal that still left a draft behind would have
    # frozen the imbalance anyway and merely reported it.
    rows = (
        (await db_session.execute(select(BillingInvoice).where(BillingInvoice.tenant_id == tenant)))
        .scalars()
        .all()
    )
    assert list(rows) == [], (
        "the close refused but still left an invoice row: the imbalance was "
        "frozen onto an immutable statement regardless"
    )


async def test_a_zero_rated_app_month_closes_and_balances_unchanged(
    db_session, test_settings
) -> None:
    """NEGATIVE CONTROL — green under BOTH mutations, and that is the point.

    This is the estate's REAL configuration, measured 2026-09-13: every
    `billing_app_cost_rates` row priced 0.00000000. With a zero rate the
    app-request stream contributes nothing, so:

      * dropping the derivation (mutation 1) changes nothing — the term is 0
        either way, which is exactly WHY the defect stayed latent in production;
      * removing the identity gate (mutation 2) changes nothing — the identity
        already held.

    A test that went red here would be claiming the fix altered the behaviour of
    every statement the estate closes today, which it must not. ⚠️ Its value is
    entirely as a control: it is NOT evidence the gate or the derivation works,
    and must never be cited as such.

    ⚠️ NON-VACUOUS ALL THE SAME — the llm stream carries real money (28 days ×
    $2.50 incurred, +30%), so the identity below is asserted over a statement with
    a genuine total rather than over an all-zero row that would balance trivially.
    """
    from models.db import AppCostRate

    tenant = T("invoice-zero-app-rate")
    license_id = uuid.uuid4()

    db_session.add(_tenant_scoped_markup_rule(tenant, "30.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    db_session.add(
        _metering_row(
            tenant, license_id=license_id, service_key="workers", request_count=_RATE_REQUESTS
        )
    )
    # The live shape: a PRESENT row stating a price of zero (never an absent row,
    # which `assert_all_service_keys_priced` refuses — see section H).
    db_session.add(AppCostRate(app_key="workers", cost_per_request_usd=Decimal("0")))
    await db_session.commit()

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)
        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    assert inv.id is not None, "a zero-rated app month must still close"
    assert inv.app_request_usage_usd == 0
    assert inv.app_request_markup_usd == 0
    assert _identity_residual(inv) == 0, (
        f"a statement the estate closes today no longer balances (residual "
        f"{_identity_residual(inv)}) — the change altered live behaviour"
    )
    assert inv.llm_usage_usd > 0, (
        "no LLM cost was billed; this control balanced trivially at zero and "
        "proves nothing about a real statement"
    )


def test_total_component_columns_are_the_six_that_sum_to_the_total() -> None:
    """⛔ PINS THE SUMMED SET ITSELF — the gate is only as right as this tuple.

    Both directions fail SILENTLY in production and loudly nowhere else. ADD
    ``platform_fee_usd`` and every statement carrying a BYO surcharge is refused
    on a false imbalance: the fee is folded INSIDE the billed lines (it is
    accumulated from the same `compute_billed_line_detailed` results whose
    `billed_usd` already sits in the two streams), so summing it double-counts
    it. Add ``subtotal_usd`` and the gate asserts ``total == 2 × total``,
    refusing every non-zero invoice in the estate. DROP either half of a stream's
    pair and the gate stops enforcing the very identity it exists for.
    """
    assert set(_TOTAL_COMPONENT_COLUMNS) == {
        "access_charge_usd",
        "seat_fee_usd",
        "llm_usage_usd",
        "llm_markup_usd",
        "app_request_usage_usd",
        "app_request_markup_usd",
    }
    for not_a_component in (
        # A COMPONENT of the billed lines, never an addition to them — see
        # models/db/invoices.py and `_money_from_snapshot`.
        "platform_fee_usd",
        # EQUALS the total while the legal set is present-and-null.
        "subtotal_usd",
        # The COST axis: what the platform paid and what it kept.
        "internal_cost_usd",
        "margin_usd",
        # Spend the statement deliberately did NOT bill.
        "excluded_unbillable_usd",
        # A QUANTITY, not money.
        "seat_count",
    ):
        assert not_a_component not in _TOTAL_COMPONENT_COLUMNS, (
            f"{not_a_component} is not a component of total_usd; summing it "
            "refuses honest statements"
        )
    assert set(_TOTAL_COMPONENT_COLUMNS) <= set(_MONEY_COLUMNS), (
        "every summed component must be a real money column the digest covers"
    )


# ---------------------------------------------------------------------------
# ⭐ THE #202 IDENTITY UNDER SUB-CENT ACCESS AND SEAT FEES
#
# ⛔ THE GATE'S TOLERANCE IS DERIVED, AND REMOVING PER-LINE ROUNDING CHANGED THE
# DERIVATION. `_TOTAL_IDENTITY_TOLERANCE_USD` is one cent because that was
# PROVABLY the largest honest residual — and the proof rested on every billed LINE
# being a whole cent, which made the residual a cent MULTIPLE bounded by 0.015 and
# therefore one of {-0.01, 0, +0.01}. Quantise once per invoice instead and the
# residual becomes CONTINUOUS on the same interval: -0.014 is honest, is not a
# defect, and refuses the close.
#
# The repair is in `generate_billing`, not in the tolerance: the access and seat
# fees now enter the total as the CENTS THEY ARE CHARGED AS, so the whole residual
# is `q(B) - B` for the billed stream `B` — one rounding, bounded by HALF a cent,
# independent of the fees' scale. Loosening the tolerance was the alternative and
# it would have blinded the whole-dropped-stream detector by exactly the amount it
# was loosened.
#
# The fixture below is built so BOTH halves are visible: the close succeeds, and
# the residual the OLD fold would have produced is computed from the same figures
# and shown to exceed the tolerance.
# ---------------------------------------------------------------------------

#: A tenant-wide access fee and a per-seat rate that are each BELOW one cent and
#: each round UP — so `q(A) + q(S)` (0.02) is more than double `A + S` (0.01).
#: This is the configuration the old fold could not balance.
_SUBCENT_ACCESS_USD = Decimal("0.00500000")
_SUBCENT_SEAT_RATE_USD = Decimal("0.00500000")
_SUBCENT_SEATS = 1

#: A sub-cent tail on the LLM stream, on its own provider so it is its own billed
#: line.
#:
#: ⛔ THE VALUE AND ITS SCALE ARE BOTH LOAD-BEARING. `q(B) - B` must come out
#: NEGATIVE for this fixture to reproduce the refusal: with `A = S = 0.005` the
#: old fold's residual is exactly `(q(B) - B) - 0.01`, so a positive half-cent
#: gives -0.005 (inside the tolerance, fixture proves nothing) while 0.004999
#: gives **-0.014999** — the shape the audit measured as -0.0140.
#: ⚠️ `billing_cost_daily.total_cost_usd` is `Numeric(20, 6)`, so a value with
#: more than six decimals is ROUNDED BY POSTGRES on the way in: `0.00499999`
#: stores as `0.005000` and silently flips the sign of the residual. Six decimals,
#: deliberately.
_SUBCENT_TAIL_USD = Decimal("0.004999")


class _SeatTokenProvider:
    """A service-account token provider that always has a token."""

    async def get_token(self) -> str:
        return "test-service-token"  # pragma: allowlist secret

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        return {  # pragma: allowlist secret
            "X-Tenant-Id": tenant_id,
            "Authorization": "Bearer test-service-token",  # pragma: allowlist secret
        }


def _seats_answer(monkeypatch, seats: int) -> None:
    """Answer the licences seat-count endpoint, delegating every other GET.

    ⚠️ Wraps whatever `httpx.AsyncClient.get` is CURRENTLY bound, which is the
    autouse byo-mode stub — a blanket replacement would starve `byo_mode` and the
    close would refuse for an unrelated reason.

    ⛔ THE TOKEN PROVIDER IS PART OF THE STUB, not an optional extra. `/internal/`
    routes on licences are gated, and `services.seats` fails CLOSED before the
    HTTP call when no service-account token is available — so a GET stub alone
    leaves the close refusing with `SeatCountUnavailableError` for a reason that
    has nothing to do with what is being asserted. The seat CACHE is module state
    with a 5-minute TTL and is cleared here for the same isolation reason
    `tests/test_commercial_terms.py` clears it.
    """
    from services import seats as seats_svc

    seats_svc.clear_seat_cache()
    monkeypatch.setattr(seats_svc, "_provider_for", lambda _s: _SeatTokenProvider())
    current = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if "seat-count" in str(url):
            return httpx.Response(
                200, json={"tenant_id": "t", "seats": seats, "basis": "active_licences"}
            )
        return await current(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)


async def _seed_subcent_fee_month(db_session, *, tenant: str, license_id: uuid.UUID):  # noqa: ANN001, ANN202
    """A closeable month with both billed streams AND two sub-cent fees.

    Markup is pinned at 0% so the billed stream equals the incurred one and the
    arithmetic below is readable end to end; the identity being tested is about
    ROUNDING, and a markup percentage would only obscure which digits came from
    where.
    """
    from models.db import AppCostRate, TenantAccessCharge

    db_session.add(_tenant_scoped_markup_rule(tenant, "0.00"))
    await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
    # The sub-cent tail, on its own provider so it groups as a separate line.
    db_session.add(
        CostDaily(
            tenant_id=tenant,
            license_id=license_id,
            service_key="workers",  # pragma: allowlist secret
            provider="subcent",
            bucket_date=_MONTH_START,
            total_input_tokens=1,
            total_output_tokens=1,
            total_cost_usd=_SUBCENT_TAIL_USD,
            request_count=1,
        )
    )
    # The APP-REQUEST stream, so the identity is exercised on both halves.
    db_session.add(
        _metering_row(
            tenant,
            license_id=license_id,
            service_key="workers",  # pragma: allowlist secret
            request_count=_RATE_REQUESTS,
        )
    )
    db_session.add(AppCostRate(app_key="workers", cost_per_request_usd=_RATE_REAL))
    db_session.add(
        TenantAccessCharge(
            tenant_id=tenant,
            service_key=None,
            amount_usd=_SUBCENT_ACCESS_USD,
            cadence="monthly",
            effective_from=datetime(2025, 1, 1, tzinfo=UTC),
        )
    )
    db_session.add(
        TenantAccessCharge(
            tenant_id=tenant,
            service_key="workers",  # pragma: allowlist secret
            amount_usd=_SUBCENT_SEAT_RATE_USD,
            cadence="per_seat",
            effective_from=datetime(2025, 1, 1, tzinfo=UTC),
        )
    )
    await db_session.commit()


async def test_the_component_identity_holds_with_sub_cent_access_and_seat_fees(
    db_session, test_settings, monkeypatch
) -> None:
    """⭐ THE CLOSE SUCCEEDS, AND THE OLD FOLD WOULD HAVE REFUSED IT.

    MUTATION CONTROL: in `services/billing.py`, fold the fees in unrounded again
    (`customer_cost = total_customer + access_total + seat_fee_total`). The close
    then raises `InvoiceMoneyImbalanceError` with a residual of -0.014999 and
    this test goes red at the `close_period` call — before any assertion runs.

    ⛔ AND THE TOLERANCE IS NOT THE PLACE TO FIX THAT: widen
    `_TOTAL_IDENTITY_TOLERANCE_USD` to 0.02 and the close passes again, but so
    does a statement that has silently dropped up to two cents of a billed
    stream. The gate's whole job is to be unable to do that.
    """
    tenant = T("invoice-subcent-fee-identity")
    license_id = uuid.uuid4()
    await _seed_subcent_fee_month(db_session, tenant=tenant, license_id=license_id)
    _seats_answer(monkeypatch, _SUBCENT_SEATS)

    close_patch, gen_patch = _patch_fetch_licenses(_license_payload(license_id))
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = _license_payload(license_id)
        mock_gen_fetch.return_value = _license_payload(license_id)
        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    # NON-VACUITY FIRST. Every assertion below is trivially true on an all-zero
    # invoice, and this fixture is elaborate enough that a silent mis-seed is the
    # likeliest way for it to rot.
    assert inv.seat_count == _SUBCENT_SEATS, "the per-seat term did not resolve"
    assert inv.access_charge_usd == Decimal("0.01"), inv.access_charge_usd
    assert inv.seat_fee_usd == Decimal("0.01"), inv.seat_fee_usd
    assert inv.llm_usage_usd > 0 and inv.app_request_usage_usd > 0, (
        "one of the two billed streams is empty, so the identity is only being "
        "checked on half the money it is supposed to cover"
    )
    # ⭐ THE FEES ARE GENUINELY SUB-CENT, AND BOTH ROUND UP. Without this the
    # fixture could degrade to whole-cent fees and the test would pass on a
    # configuration that never exercised the defect at all.
    assert Decimal("0.01") > _SUBCENT_ACCESS_USD
    assert Decimal("0.01") > _SUBCENT_SEAT_RATE_USD * _SUBCENT_SEATS

    # The billed stream, read off the stored components — `B` in the derivation.
    billed_stream = (
        Decimal(inv.llm_usage_usd)
        + Decimal(inv.llm_markup_usd)
        + Decimal(inv.app_request_usage_usd)
        + Decimal(inv.app_request_markup_usd)
    )
    assert billed_stream != quantize_money(billed_stream), (
        "the billed stream landed on a whole cent, so `q(B) - B` is zero and this "
        "test cannot distinguish the fixed fold from the broken one"
    )

    # (1) THE IDENTITY THE GATE ENFORCES, on the row that was actually written.
    residual = _identity_residual(inv)
    assert abs(residual) <= _TOTAL_IDENTITY_TOLERANCE_USD, (
        f"the closed invoice's components miss its total by {residual}, which is "
        "outside the tolerance the close is allowed"
    )

    # (2) AND THE STRONGER BOUND THE NEW DERIVATION CLAIMS: the entire residual is
    # `q(B) - B`, one rounding, never more than half a quantum. Asserting only (1)
    # would let the derivation silently decay back toward the old bound.
    assert residual == quantize_money(billed_stream) - billed_stream, (
        f"residual {residual} is not q(B) - B ({quantize_money(billed_stream)} - "
        f"{billed_stream}); the fees are leaking rounding into the identity again"
    )
    # Half a quantum INCLUSIVE: ROUND_HALF_UP makes exactly +0.005 attainable
    # (a billed stream landing on a true half-cent), so `<` would be a claim the
    # derivation does not make.
    assert abs(residual) <= _TOTAL_IDENTITY_TOLERANCE_USD / 2

    # (3) THE CONTROL — what the OLD fold would have produced from these same
    # figures. Computed from the seeded fees rather than from the stored columns,
    # because the whole point is that the stored columns are the ROUNDED ones.
    raw_fees = _SUBCENT_ACCESS_USD + _SUBCENT_SEAT_RATE_USD * _SUBCENT_SEATS
    old_total = quantize_money(billed_stream + raw_fees)
    old_residual = old_total - (
        Decimal(inv.access_charge_usd) + Decimal(inv.seat_fee_usd) + billed_stream
    )
    assert abs(old_residual) > _TOTAL_IDENTITY_TOLERANCE_USD, (
        f"the old unrounded fold would have produced a residual of {old_residual}, "
        "which is INSIDE the tolerance — so this fixture does not actually "
        "reproduce the refusal and proves nothing about the fix"
    )


#: A per-licence sub-cent tail sitting on a TRUE half cent, so `q(b) - b` is
#: exactly +0.005 for each licence and the per-licence roundings ACCUMULATE.
#: Six decimals for the reason `_SUBCENT_TAIL_USD` states above: Postgres rounds
#: `billing_cost_daily.total_cost_usd` (`Numeric(20, 6)`) on the way in.
_HALF_CENT_TAIL_USD = Decimal("0.005000")

#: ⭐ THREE, AND THREE IS THE MINIMUM THAT CAN FAIL — see the test below, which
#: checks that arithmetic rather than trusting this comment. At one licence the
#: accumulated error is 0.005 and at two it is exactly 0.010, both of which the
#: gate's `<=` one-cent tolerance ADMITS. Only at three does it reach 0.015 and
#: refuse. Every existing identity test in this file closes ONE licence.
_HALF_CENT_LICENCES = 3


async def _seed_half_cent_month_for_each(
    db_session,  # noqa: ANN001
    *,
    tenant: str,
    license_ids: tuple[uuid.UUID, ...],
) -> None:
    """A closeable month where EVERY licence's billed line ends on a half cent.

    Markup is pinned at 0% so the billed stream equals the incurred one and each
    licence's contribution is readable as the two numbers seeded here; the
    property under test is about ROUNDING accumulating across licences, and a
    markup percentage would only obscure which digits came from where.
    """
    db_session.add(_tenant_scoped_markup_rule(tenant, "0.00"))
    for license_id in license_ids:
        await _seed_full_month(db_session, tenant_id=tenant, license_id=license_id)
        # The half-cent tail, on its own provider so it does not collide with the
        # whole-month rows above on the same day.
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=license_id,
                service_key="workers",  # pragma: allowlist secret
                provider="halfcent",
                bucket_date=_MONTH_START,
                total_input_tokens=1,
                total_output_tokens=1,
                total_cost_usd=_HALF_CENT_TAIL_USD,
                request_count=1,
            )
        )
    await db_session.commit()


async def test_the_component_identity_survives_three_licences_at_a_half_cent_boundary(
    db_session, test_settings
) -> None:
    """⭐ THE CONTROL OVER `total_customer += billed_cost` STAYING RAW.

    ⚠️ THIS TEST IS GREEN BEFORE AND AFTER the itemisation fix it ships with —
    that is its job. Making `licenses[].costs.billed_cost_usd` render raw is a
    DISPLAY change, and this proves it grew no residual. What it exists to catch
    is the next reader who sees a raw figure on a bill, decides it is untidy, and
    quantises the ACCUMULATOR at `services/billing.py`'s
    `total_customer += billed_cost` instead.

    THE MUTANT: `total_customer += quantize_money(billed_cost)`. The stored total
    becomes `Σ q(b_i)` while `_money_from_snapshot` still rebuilds the components
    from the RAW `llm_billed_usd` / `app_request_billed_usd` keys, so the residual
    grows by up to half a cent PER LICENCE. `close_period` then raises
    `InvoiceMoneyImbalanceError` and this test reds AT THE CLOSE CALL, before any
    assertion runs.

    ⛔ THE FIXTURE PROPERTY, AND WHY THE EXISTING SUB-CENT TEST CANNOT DO THIS:
    `test_the_component_identity_holds_with_sub_cent_access_and_seat_fees` closes
    ONE licence, so that mutant costs it 0.005 — comfortably inside the gate's
    one-cent tolerance, and it stays green. The error only escapes the tolerance
    once it has accumulated over THREE licences. Varying the licence COUNT is the
    whole fixture; asserting harder on a single-licence month could not have
    found this.
    """
    tenant = T("invoice-three-licence-half-cent")
    license_ids = tuple(uuid.uuid4() for _ in range(_HALF_CENT_LICENCES))
    await _seed_half_cent_month_for_each(db_session, tenant=tenant, license_ids=license_ids)

    payload = [entry for lic in license_ids for entry in _license_payload(lic)]
    close_patch, gen_patch = _patch_fetch_licenses(payload)
    with close_patch as mock_close_fetch, gen_patch as mock_gen_fetch:
        mock_close_fetch.return_value = payload
        mock_gen_fetch.return_value = payload
        inv = await close_period(
            db_session,
            tenant_id=tenant,
            period_start=_MONTH_START,
            period_end=_MONTH_END,
            settings=test_settings,
            issue_immediately=False,
        )

    # NON-VACUITY FIRST — the multi-licence property is the fixture, so a month
    # that silently closed one licence would make every number below honest and
    # the test meaningless.
    entries = inv.snapshot["licenses"]
    assert len(entries) == _HALF_CENT_LICENCES, (
        f"closed {len(entries)} licence(s), not {_HALF_CENT_LICENCES}; the "
        "accumulation this test measures cannot happen on fewer"
    )

    # Each licence's billed line really does sit on a half cent, and — FIX 2's
    # property, checked on the frozen document — equals its own two components.
    per_licence: list[Decimal] = []
    for entry in entries:
        costs = entry["costs"]
        billed = Decimal(costs["billed_cost_usd"])
        assert billed == Decimal(costs["app_request_billed_usd"]) + Decimal(
            costs["llm_billed_usd"]
        ), (
            f"licence {entry['license_id']} shows a billed total of {billed} that "
            f"its own displayed components do not reach"
        )
        assert quantize_money(billed) - billed == Decimal("0.005"), (
            f"licence {entry['license_id']} billed {billed}, whose rounding error "
            f"is {quantize_money(billed) - billed} and not the half cent this "
            "fixture is built to produce"
        )
        per_licence.append(billed)

    # (1) THE IDENTITY THE GATE ENFORCES, on the row that was actually written.
    residual = _identity_residual(inv)
    assert abs(residual) <= _TOTAL_IDENTITY_TOLERANCE_USD, (
        f"the closed invoice's components miss its total by {residual}"
    )

    # (2) AND THE STRONGER BOUND: the whole residual is `q(B) - B`, ONE rounding
    # over the summed stream, no matter how many licences it was split across.
    # This is the half that a per-licence quantisation destroys.
    billed_stream = sum(per_licence, Decimal("0"))
    assert residual == quantize_money(billed_stream) - billed_stream, (
        f"residual {residual} is not q(B) - B; rounding is leaking in per LINE"
    )
    assert abs(residual) <= _TOTAL_IDENTITY_TOLERANCE_USD / 2

    # (3) THE CONTROL — what the per-licence-quantised accumulator would have
    # produced from these same figures, and the proof that THREE licences is the
    # smallest fixture that can catch it. Computed here rather than asserted in
    # prose, because a comment claiming a bound cannot go stale loudly.
    mutant_error = sum((quantize_money(b) - b for b in per_licence), Decimal("0"))
    assert abs(mutant_error) > _TOTAL_IDENTITY_TOLERANCE_USD, (
        f"quantising per licence would shift the total by {mutant_error}, which is "
        f"INSIDE the {_TOTAL_IDENTITY_TOLERANCE_USD} tolerance — this fixture does "
        "not actually reproduce the refusal and proves nothing"
    )
    two_licence_error = sum((quantize_money(b) - b for b in per_licence[:2]), Decimal("0"))
    assert abs(two_licence_error) <= _TOTAL_IDENTITY_TOLERANCE_USD, (
        f"two licences already exceed the tolerance ({two_licence_error}), so the "
        f"{_HALF_CENT_LICENCES}-licence fixture is larger than it needs to be and "
        "its docstring's claim about the minimum is wrong"
    )
