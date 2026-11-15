"""``billing_cost_actor_daily`` shipped with NO retention; these pin the window.

THE DEFECT UNDER TEST
---------------------
The per-actor rollup (alembic 0040) was added with no purge of any kind, so it
grew forever -- an unbounded table on the same worker4 volume as the production
primary. Raw ``billing_cost_events`` retention is partition-native
(``drop_old_cost_partitions``, ``settings.retention_days`` = 90) and does not
reach this table at all.

⛔ AND IT MUST NOT BE FIXED BY REUSING ``retention_days``. 90 days would age out
per-member chargeback evidence roughly three months after the invoice it
supports was issued, and the raw events needed to rebuild it are gone by then.
The window is 25 months -- a full PRIOR calendar year (12) + the current one (12)
+ the month in hand (1) -- so a dispute raised on 31 December about the previous
January can still be evidenced.

⚠️ WHAT WOULD PASS WITHOUT THE BOUNDARY TESTS. A purge that deleted everything,
and a purge that deleted nothing, both leave "the table does not grow forever"
sounding true. Sections 1 and 3 therefore assert the EXACT boundary row in both
directions, and section 3 asserts a surviving row by id, not by count.
"""

from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import insert, select

from models.db import CostActorDaily
from services.aggregator import actor_rollup_cutoff, purge_old_actor_rollup
from settings import MIN_ACTOR_ROLLUP_RETENTION_MONTHS, Settings, get_settings
from tests.conftest import T

TENANT = T("actor-retention")
DEFAULT_MONTHS = 25

# A fixed "today" so the boundary is arithmetic, not calendar-dependent. ⛔ Do
# not replace with date.today(): a test whose expected values move with the wall
# clock cannot distinguish "the cutoff is wrong" from "the month turned over".
TODAY = date(2026, 9, 17)
# 25 months inclusive of September 2026 => the oldest KEPT month is 2024-09.
EXPECTED_CUTOFF = date(2024, 9, 1)


# ---------------------------------------------------------------------------
# 1. The cutoff -- pure, no database
# ---------------------------------------------------------------------------


class TestActorRollupCutoff:
    @pytest.mark.unit
    def test_the_default_window_keeps_exactly_twenty_five_monthly_buckets(self):
        """⚠️ MUTATION TARGET: the ``- 1`` in ``(retention_months - 1)``.

        Dropping it keeps TWENTY-SIX buckets and makes the setting's name a lie
        by one month -- invisible until somebody reconciles a retention claim
        against a row count.
        """
        cutoff = actor_rollup_cutoff(DEFAULT_MONTHS, today=TODAY)
        assert cutoff == EXPECTED_CUTOFF
        kept = (TODAY.year * 12 + TODAY.month) - (cutoff.year * 12 + cutoff.month) + 1
        assert kept == DEFAULT_MONTHS

    @pytest.mark.unit
    def test_a_full_prior_calendar_year_survives_the_last_tick_of_december(self):
        """The requirement, asserted as the requirement rather than as arithmetic.

        On 31 December 2026 a customer disputes a January 2025 invoice line. The
        evidence must still be there.
        """
        cutoff = actor_rollup_cutoff(DEFAULT_MONTHS, today=date(2026, 12, 31))
        assert cutoff <= date(2025, 1, 1)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("today", "expected"),
        [
            # ⚠️ YEAR-BOUNDARY ARITHMETIC. A naive ``month - n`` produces month 0
            # or a negative month and raises ValueError, or -- worse -- wraps.
            (date(2026, 1, 15), date(2024, 1, 1)),
            (date(2026, 2, 1), date(2024, 2, 1)),
            (date(2026, 12, 31), date(2024, 12, 1)),
        ],
    )
    def test_it_crosses_year_boundaries(self, today, expected):
        assert actor_rollup_cutoff(DEFAULT_MONTHS, today=today) == expected

    @pytest.mark.unit
    def test_the_cutoff_is_always_the_first_of_a_month(self):
        """⚠️ MUTATION TARGET: month-anchoring replaced by ``timedelta(days=30*n)``.

        A day-arithmetic cutoff drifts ~5 days a year and lands MID-MONTH, so a
        purge deletes part of a month's buckets and keeps the rest. An invoice
        period that is half-evidenced is worse than one cleanly absent, because a
        partial SUM looks like a complete one.
        """
        for month in range(1, 13):
            assert actor_rollup_cutoff(DEFAULT_MONTHS, today=date(2026, month, 28)).day == 1


# ---------------------------------------------------------------------------
# 2. The setting
# ---------------------------------------------------------------------------


class TestTheSetting:
    @pytest.mark.unit
    def test_the_default_is_twenty_five_months(self):
        """⛔ Do NOT "fix" a failure here by editing the right-hand side.

        25 is the dispute window, not a tuning knob: 12 (prior year) + 12
        (current year) + 1 (the month in hand).
        """
        get_settings.cache_clear()
        try:
            assert get_settings().actor_rollup_retention_months == 25
        finally:
            get_settings.cache_clear()

    @pytest.mark.unit
    def test_raw_retention_is_not_changed(self):
        """The control. This change must not touch ``retention_days``."""
        get_settings.cache_clear()
        try:
            assert get_settings().retention_days == 90
        finally:
            get_settings.cache_clear()

    @pytest.mark.unit
    @pytest.mark.parametrize("bad", [0, 1, 12, MIN_ACTOR_ROLLUP_RETENTION_MONTHS - 1])
    def test_a_window_too_short_to_answer_a_dispute_is_refused_at_construction(self, bad):
        """⚠️ MUTATION TARGET: the ``_actor_rollup_retention_covers_a_prior_year`` validator.

        ⛔ 0 IS NOT "DISABLED" HERE. The window is in MONTHS, so 0 puts the
        cutoff at the start of the CURRENT month and deletes every completed
        month in one tick -- a value that reads like an off-switch and behaves
        like a wipe. The pod must crash at startup rather than discover this on
        the first daily rollup.
        """
        with pytest.raises(ValueError, match="actor_rollup_retention_months"):
            Settings(environment="test", actor_rollup_retention_months=bad)

    @pytest.mark.unit
    def test_the_floor_itself_is_accepted(self):
        """The control for the test above: the validator must refuse BELOW the
        floor, not AT it, or the boundary assertion proves nothing.

        ⚠️ ``environment="test"`` on both constructions, not decoration:
        ``Settings.environment`` defaults to ``"production"`` and
        ``_enforce_production_required_fields`` then demands redis_url,
        licenses_url, direct_postgres_host, keycloak_url and
        otlp_traces_endpoint. Without it this test would raise for a reason that
        has nothing to do with retention, and the refusal test above would pass
        for the wrong reason.
        """
        assert (
            Settings(
                environment="test",
                actor_rollup_retention_months=MIN_ACTOR_ROLLUP_RETENTION_MONTHS,
            ).actor_rollup_retention_months
            == MIN_ACTOR_ROLLUP_RETENTION_MONTHS
        )


# ---------------------------------------------------------------------------
# 3. The purge deletes ONLY what is past the window
# ---------------------------------------------------------------------------

# One row per bucket_date, each with a DISTINCT cost, so a purge that deletes
# the wrong side cannot produce the right total by coincidence.
ROW_SPECS: list[tuple[str, date, Decimal]] = [
    ("ancient", date(2023, 1, 1), Decimal("71.71")),  # far past  -> deleted
    ("day_before_cutoff", date(2024, 8, 31), Decimal("72.72")),  # -1 day -> deleted
    ("exactly_cutoff", EXPECTED_CUTOFF, Decimal("73.73")),  # == cutoff -> KEPT
    ("day_after_cutoff", date(2024, 9, 2), Decimal("74.74")),  # +1 day -> KEPT
    ("recent", date(2026, 9, 1), Decimal("75.75")),  # in window -> KEPT
]

DELETED = {"ancient", "day_before_cutoff"}
KEPT = {"exactly_cutoff", "day_after_cutoff", "recent"}

_IDS: dict[str, uuid.UUID] = {
    name: uuid.uuid5(uuid.NAMESPACE_URL, f"actor-retention/{name}") for name, _, _ in ROW_SPECS
}


async def _seed(session_factory) -> None:
    async with session_factory() as session:
        await session.execute(
            insert(CostActorDaily),
            [
                {
                    "id": _IDS[name],
                    "bucket_date": bucket,
                    "tenant_id": TENANT,
                    "license_id": uuid.uuid4(),
                    "subject_id": uuid.uuid4(),
                    # Distinct actor per row so nothing can collide on
                    # uq_cost_actor_daily and silently seed fewer rows than the
                    # test believes it seeded.
                    "actor_id": _IDS[name],
                    "service_key": "commonpractice",  # pragma: allowlist secret
                    "provider": "claude",
                    "served_model": "claude-3-5-sonnet-20241022",
                    "byo": False,
                    "owner_provided": False,
                    "total_input_tokens": 100,
                    "total_output_tokens": 50,
                    "total_cost_usd": cost,
                    "total_vendor_cost_usd": None,
                    "request_count": 1,
                }
                for name, bucket, cost in ROW_SPECS
            ],
        )
        await session.commit()


async def _surviving(session_factory) -> set[str]:
    async with session_factory() as session:
        ids = set((await session.execute(select(CostActorDaily.id))).scalars().all())
    return {name for name, got in _IDS.items() if got in ids}


class TestPurgeOldActorRollup:
    @pytest.mark.integration
    async def test_it_deletes_only_rows_past_the_window(self, db_session_factory):
        """⚠️ MUTATION TARGET: ``bucket_date < cutoff`` -> ``<=`` (or ``>``).

        ``<=`` deletes ``exactly_cutoff`` -- the oldest month the window promises
        to keep -- and ``>`` inverts the purge entirely. Both are invisible to a
        test that only counts rows, which is why this asserts the surviving SET
        by id.
        """
        await _seed(db_session_factory)
        deleted = await purge_old_actor_rollup(db_session_factory, DEFAULT_MONTHS, today=TODAY)

        assert deleted == len(DELETED)
        assert await _surviving(db_session_factory) == KEPT

    @pytest.mark.integration
    async def test_it_is_idempotent(self, db_session_factory):
        """It runs on every daily tick; a second pass must delete nothing more."""
        await _seed(db_session_factory)
        first = await purge_old_actor_rollup(db_session_factory, DEFAULT_MONTHS, today=TODAY)
        second = await purge_old_actor_rollup(db_session_factory, DEFAULT_MONTHS, today=TODAY)

        assert first == len(DELETED)
        assert second == 0
        assert await _surviving(db_session_factory) == KEPT

    @pytest.mark.integration
    async def test_a_window_wide_enough_deletes_nothing(self, db_session_factory):
        """The control. Without it, a purge that deletes on a condition unrelated
        to ``bucket_date`` still satisfies the test above."""
        await _seed(db_session_factory)
        deleted = await purge_old_actor_rollup(db_session_factory, 600, today=TODAY)

        assert deleted == 0
        assert await _surviving(db_session_factory) == set(_IDS)

    @pytest.mark.integration
    async def test_it_does_not_touch_the_raw_cost_events(self, db_session_factory):
        """⛔ RAW RETENTION IS NOT CHANGED, and this asserts it rather than
        asserting that nobody meant to change it.

        ``billing_cost_events`` retention is a partition DROP driven by
        ``settings.retention_days``; a purge that reached it would destroy 90
        days of money records under a 25-month setting.
        """
        from models.db import CostEvent

        await _seed(db_session_factory)
        async with db_session_factory() as session:
            await session.execute(
                insert(CostEvent),
                [
                    {
                        "tenant_id": TENANT,
                        "tenant_name": "",
                        "license_id": None,
                        "service_key": "commonpractice",  # pragma: allowlist secret
                        "provider": "claude",
                        "input_tokens": 1,
                        "output_tokens": 1,
                        "cost_usd": Decimal("1.01"),
                    }
                ],
            )
            await session.commit()

        await purge_old_actor_rollup(db_session_factory, DEFAULT_MONTHS, today=TODAY)

        async with db_session_factory() as session:
            remaining = (await session.execute(select(CostEvent.id))).scalars().all()
        assert len(remaining) == 1
