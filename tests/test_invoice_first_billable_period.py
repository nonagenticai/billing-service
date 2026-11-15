"""``Settings.invoice_first_billable_period`` — the floor under the scheduled close.

Periods STRICTLY BEFORE the floor are not billable: the sweep
(``services.invoices.close_due_periods``) attempts no close for them and the
backlog census (``services.invoice_closer._measure_invoice_close_lag``) does not
count them as lag. A period AT or AFTER the floor behaves exactly as it did
before the setting existed — including being refused on a bucket gap.

Why the floor exists at all: the source cost events for 2026-08/09 were purged
on 2026-09-30 (pre-production, owner-approved), so September 2026 can never pass
``assert_no_bucket_gap`` and ``BillingInvoiceCloseBehindSchedule`` fired forever.

EVERY TEST HERE COMES WITH A BOUNDARY TWIN, and the pair is the instrument. "No
close was attempted" and "lag is 0" are both satisfied by a fixture that seeded
nothing; the twin runs the SAME seeding one month over (or with the floor moved
onto the period) and must observe the close attempt / the lag. Mutants applied
to the source and observed red are listed in the commit that added this file.

Real Postgres throughout, via the same ``db_session_factory`` / ``test_settings``
fixtures the neighbouring invoice tests use.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from models.db import BillingInvoice, CostDaily
from observability import metrics as observability_metrics
from services import invoices as invoices_module
from services.invoice_closer import _measure_invoice_close_lag
from services.invoices import BucketGapError, CloseDueOutcome, close_due_periods, close_period
from settings import Settings
from tests.conftest import T

# The real shape of the incident: at this instant the due period is September
# 2026, whose rollup buckets are (in production, and here) almost all absent.
_SWEEP_NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
_SEPTEMBER = (date(2026, 9, 1), date(2026, 10, 1))
_OCTOBER = (date(2026, 10, 1), date(2026, 11, 1))

# Census clock: the last completed month is October 2026, and with
# ``invoice_close_lag_days = 3`` both it and September are past their settle date.
_CENSUS_NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
_LAG_IF_SEPTEMBER_COUNTS = (_CENSUS_NOW.date() - _SEPTEMBER[1]).days  # 50
_LAG_IF_ONLY_OCTOBER_COUNTS = (_CENSUS_NOW.date() - _OCTOBER[1]).days  # 19


def _bucket_gap_refusals() -> float:
    return observability_metrics.invoice_close_refused_total.labels(
        reason="bucket_gap"
    )._value.get()


async def _seed_one_cost_day(session_factory, *, tenant_id: str, bucket_date: date) -> None:
    """ONE ``billing_cost_daily`` row: enough to make the tenant a close
    candidate for that month, and — every other day being absent fleet-wide —
    enough to guarantee ``assert_no_bucket_gap`` refuses the month."""
    async with session_factory() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                license_id=uuid.uuid4(),
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                bucket_date=bucket_date,
                total_input_tokens=1_000,
                total_output_tokens=200,
                total_cost_usd=Decimal("2.500000"),
                request_count=10,
            )
        )
        await session.commit()


@pytest.fixture
def close_period_spy(monkeypatch):
    """Count ``close_period`` calls made BY THE SWEEP, delegating to the real one.

    Delegating, not stubbing: the at-the-floor twin needs the real gates to run
    so that what it observes is a real ``BucketGapError`` refusal.
    """
    calls: list[tuple[date, date]] = []
    real = invoices_module.close_period

    async def _spy(session, **kwargs):  # noqa: ANN001, ANN003, ANN202
        calls.append((kwargs["period_start"], kwargs["period_end"]))
        return await real(session, **kwargs)

    monkeypatch.setattr(invoices_module, "close_period", _spy)
    return calls


# --------------------------------------------------------------------------- #
# The setting itself
# --------------------------------------------------------------------------- #


class TestSettingValidation:
    def test_unset_means_no_floor(self, monkeypatch) -> None:
        monkeypatch.delenv("INVOICE_FIRST_BILLABLE_PERIOD", raising=False)
        settings = Settings()
        assert settings.invoice_first_billable_period is None
        assert settings.invoice_first_billable_period_start is None

    def test_well_formed_value_is_read_from_the_env(self, monkeypatch) -> None:
        monkeypatch.setenv("INVOICE_FIRST_BILLABLE_PERIOD", "2026-10")
        settings = Settings()
        assert settings.invoice_first_billable_period == "2026-10"
        assert settings.invoice_first_billable_period_start == date(2026, 10, 1)

    @pytest.mark.parametrize(
        "bad",
        [
            "2026-13",  # no such month
            "2026-00",
            "2026-9",  # not zero-padded
            "2026-09-01",  # a date, not a month
            "202609",
            "2026/09",
            " 2026-09",  # no stripping: a stray space is a typo, not a value
            "2026-09 ",
            "sept-2026",
            "0000-01",  # matches the digits, is not a date
            "",  # empty is NOT "unset" — leave the variable out instead
        ],
    )
    def test_malformed_value_refuses_to_construct(self, monkeypatch, bad: str) -> None:
        """A malformed floor must crash startup, never read as "no floor"."""
        monkeypatch.delenv("INVOICE_FIRST_BILLABLE_PERIOD", raising=False)
        with pytest.raises(ValidationError, match="invoice_first_billable_period"):
            Settings(invoice_first_billable_period=bad)

    def test_malformed_env_value_refuses_to_construct(self, monkeypatch) -> None:
        """The same refusal through the door production actually uses."""
        monkeypatch.setenv("INVOICE_FIRST_BILLABLE_PERIOD", "2026-9")
        with pytest.raises(ValidationError, match="invoice_first_billable_period"):
            Settings()

    def test_malformed_value_assigned_after_construction_still_raises(self) -> None:
        """``Settings`` does not validate on assignment (the test suite relies on
        that), so the read path must refuse on its own rather than trust that the
        validator ran."""
        settings = Settings()
        settings.invoice_first_billable_period = "2026-9"
        with pytest.raises(ValueError, match="invoice_first_billable_period"):
            _ = settings.invoice_first_billable_period_start


# --------------------------------------------------------------------------- #
# close_due_periods
# --------------------------------------------------------------------------- #


class TestSweepFloor:
    async def test_due_period_before_the_floor_attempts_no_close(
        self, db_session_factory, test_settings, close_period_spy
    ) -> None:
        """Due = September, floor = October → nothing due; no gate is even asked.

        Mutant: delete the floor from ``close_due_periods``. The tenant is then a
        candidate, ``close_period`` is called once and refuses on the gap: the spy
        reads 1, ``refused`` reads 1 and the bucket-gap counter moves.
        """
        test_settings.invoice_first_billable_period = "2026-10"
        await _seed_one_cost_day(
            db_session_factory, tenant_id=T("floor-sweep-before"), bucket_date=date(2026, 9, 30)
        )
        refusals_before = _bucket_gap_refusals()

        outcome = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

        assert close_period_spy == [], "a pre-floor period was handed to close_period"
        assert outcome == CloseDueOutcome(created=[], unchanged=[], refused=0, failed=0, skipped=0)
        assert _bucket_gap_refusals() == refusals_before, (
            "a pre-floor period reached assert_no_bucket_gap — the sweep attempted the close"
        )

    async def test_due_period_at_the_floor_is_still_refused_on_a_bucket_gap(
        self, db_session_factory, test_settings, close_period_spy
    ) -> None:
        """THE TWIN, and the anti-bypass test: identical seeding, floor ON the due
        month → the close IS attempted and IS refused by ``assert_no_bucket_gap``.

        Mutant: ``period_start <= first_billable`` in ``close_due_periods``. The
        floor month itself is then skipped — spy 0, ``refused`` 0 — which is the
        setting quietly swallowing a month that owes money.
        """
        test_settings.invoice_first_billable_period = "2026-09"
        tenant = T("floor-sweep-at")
        await _seed_one_cost_day(
            db_session_factory, tenant_id=tenant, bucket_date=date(2026, 9, 30)
        )
        refusals_before = _bucket_gap_refusals()

        outcome = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

        assert close_period_spy == [_SEPTEMBER], "the period AT the floor was not attempted"
        assert outcome.refused == 1
        assert outcome.created == []
        assert _bucket_gap_refusals() == refusals_before + 1, (
            "the refusal at the floor was not a BucketGapError"
        )
        async with db_session_factory() as session:
            from sqlalchemy import func, select

            drawn = (
                await session.execute(
                    select(func.count())
                    .select_from(BillingInvoice)
                    .where(BillingInvoice.tenant_id == tenant)
                )
            ).scalar_one()
        assert drawn == 0, "a gapped period at the floor drew an invoice"

    async def test_unset_floor_attempts_the_close_as_before(
        self, db_session_factory, test_settings, close_period_spy
    ) -> None:
        """No floor → today's behaviour: attempted, refused on the gap."""
        assert test_settings.invoice_first_billable_period is None
        await _seed_one_cost_day(
            db_session_factory, tenant_id=T("floor-sweep-unset"), bucket_date=date(2026, 9, 30)
        )

        outcome = await close_due_periods(db_session_factory, test_settings, now=_SWEEP_NOW)

        assert close_period_spy == [_SEPTEMBER]
        assert outcome.refused == 1


class TestManualCloseIgnoresTheFloor:
    """``close_period`` — the manual path — does not consult the setting."""

    async def test_close_period_at_the_floor_raises_bucket_gap(
        self, db_session, db_session_factory, test_settings
    ) -> None:
        test_settings.invoice_first_billable_period = "2026-09"
        tenant = T("floor-manual-at")
        await _seed_one_cost_day(
            db_session_factory, tenant_id=tenant, bucket_date=date(2026, 9, 30)
        )

        with pytest.raises(BucketGapError) as exc:
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_SEPTEMBER[0],
                period_end=_SEPTEMBER[1],
                settings=test_settings,
            )
        assert date(2026, 9, 1) in exc.value.missing_dates
        assert date(2026, 9, 30) not in exc.value.missing_dates

    async def test_close_period_before_the_floor_meets_the_same_gates(
        self, db_session, db_session_factory, test_settings
    ) -> None:
        """A manual close of a PRE-floor period gets the existing refusal, not a
        new one and not a pass: the floor is neither a bypass nor a new gate."""
        test_settings.invoice_first_billable_period = "2026-10"
        tenant = T("floor-manual-before")
        await _seed_one_cost_day(
            db_session_factory, tenant_id=tenant, bucket_date=date(2026, 9, 30)
        )

        with pytest.raises(BucketGapError):
            await close_period(
                db_session,
                tenant_id=tenant,
                period_start=_SEPTEMBER[0],
                period_end=_SEPTEMBER[1],
                settings=test_settings,
            )


# --------------------------------------------------------------------------- #
# _measure_invoice_close_lag
# --------------------------------------------------------------------------- #


class TestCensusFloor:
    async def test_uncovered_period_before_the_floor_is_not_lag(
        self, db_session_factory, test_settings
    ) -> None:
        """September has a candidate and no invoice; floor = October → 0.

        Mutant: delete the floor from ``_measure_invoice_close_lag`` → 50.
        """
        test_settings.invoice_close_lag_days = 3
        test_settings.invoice_first_billable_period = "2026-10"
        await _seed_one_cost_day(
            db_session_factory, tenant_id=T("floor-census-before"), bucket_date=date(2026, 9, 14)
        )

        lag = await _measure_invoice_close_lag(
            db_session_factory, settings=test_settings, now=_CENSUS_NOW
        )

        assert lag == 0, f"a pre-floor period was counted as lag ({lag} days)"

    async def test_uncovered_period_after_the_floor_is_still_lag(
        self, db_session_factory, test_settings
    ) -> None:
        """September AND October uncovered, floor = October → October's age (19),
        not September's (50) and not 0. One number rules out both failure modes:
        the floor counting too much and the floor silencing everything."""
        test_settings.invoice_close_lag_days = 3
        test_settings.invoice_first_billable_period = "2026-10"
        await _seed_one_cost_day(
            db_session_factory, tenant_id=T("floor-census-sep"), bucket_date=date(2026, 9, 14)
        )
        await _seed_one_cost_day(
            db_session_factory, tenant_id=T("floor-census-oct"), bucket_date=date(2026, 10, 14)
        )

        lag = await _measure_invoice_close_lag(
            db_session_factory, settings=test_settings, now=_CENSUS_NOW
        )

        assert lag == _LAG_IF_ONLY_OCTOBER_COUNTS, (
            f"expected the post-floor period's age {_LAG_IF_ONLY_OCTOBER_COUNTS}, got {lag} "
            f"({_LAG_IF_SEPTEMBER_COUNTS} = the pre-floor period leaked in; 0 = the floor "
            "silenced a billable period)"
        )

    async def test_uncovered_period_at_the_floor_is_still_lag(
        self, db_session_factory, test_settings
    ) -> None:
        """THE TWIN: same September seeding, floor ON September → 50.

        Mutant: ``period_start <= first_billable`` in the census → 0.
        """
        test_settings.invoice_close_lag_days = 3
        test_settings.invoice_first_billable_period = "2026-09"
        await _seed_one_cost_day(
            db_session_factory, tenant_id=T("floor-census-at"), bucket_date=date(2026, 9, 14)
        )

        lag = await _measure_invoice_close_lag(
            db_session_factory, settings=test_settings, now=_CENSUS_NOW
        )

        assert lag == _LAG_IF_SEPTEMBER_COUNTS

    async def test_unset_floor_counts_the_period_as_before(
        self, db_session_factory, test_settings
    ) -> None:
        test_settings.invoice_close_lag_days = 3
        assert test_settings.invoice_first_billable_period is None
        await _seed_one_cost_day(
            db_session_factory, tenant_id=T("floor-census-unset"), bucket_date=date(2026, 9, 14)
        )

        lag = await _measure_invoice_close_lag(
            db_session_factory, settings=test_settings, now=_CENSUS_NOW
        )

        assert lag == _LAG_IF_SEPTEMBER_COUNTS
