"""Tests for the cost_events partition lifecycle (ensure + drop-style retention).

The decision logic lives in pure helpers (``_months_to_ensure``,
``_aged_cost_partitions``) which are unit-tested exhaustively without a DB. The
SQL wrappers are covered by a stub-function test (``ensure_*``) and a real
partitioned-table integration test for the destructive drop path.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.rollup_engine import (
    MAX_COST_PARTITIONS_DROPPED_PER_RUN,
    _aged_cost_partitions,
    _months_to_ensure,
    drop_old_cost_partitions,
    ensure_cost_event_partitions,
)

# ---------------------------------------------------------------------------
# Pure helpers (no DB)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMonthsToEnsure:
    def test_includes_current_month_first(self):
        now = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)
        assert _months_to_ensure(now, 0) == [(2026, 5)]

    def test_rolling_window(self):
        now = datetime(2026, 5, 1, tzinfo=UTC)
        assert _months_to_ensure(now, 3) == [(2026, 5), (2026, 6), (2026, 7), (2026, 8)]

    def test_year_rollover(self):
        now = datetime(2026, 11, 15, tzinfo=UTC)
        # Nov, Dec, Jan(+1), Feb(+1)
        assert _months_to_ensure(now, 3) == [(2026, 11), (2026, 12), (2027, 1), (2027, 2)]

    def test_december_start(self):
        now = datetime(2026, 12, 31, tzinfo=UTC)
        assert _months_to_ensure(now, 1) == [(2026, 12), (2027, 1)]


@pytest.mark.unit
class TestAgedCostPartitions:
    def test_fully_aged_dropped_straddling_kept(self):
        # cutoff mid-May 2026 (= now - 90d ~ feb..). Use explicit cutoff.
        cutoff = datetime(2026, 5, 17, tzinfo=UTC)
        names = [
            "billing_cost_events_2026_01",  # upper 2026-02-01 <= cutoff -> aged
            "billing_cost_events_2026_02",  # upper 2026-03-01 <= cutoff -> aged
            "billing_cost_events_2026_04",  # upper 2026-05-01 <= cutoff -> aged
            "billing_cost_events_2026_05",  # upper 2026-06-01 >  cutoff -> KEEP (straddles)
            "billing_cost_events_2026_06",  # upper 2026-07-01 >  cutoff -> KEEP (future)
        ]
        assert _aged_cost_partitions(names, cutoff) == [
            "billing_cost_events_2026_01",
            "billing_cost_events_2026_02",
            "billing_cost_events_2026_04",
        ]

    def test_boundary_upper_equals_cutoff_is_dropped(self):
        # upper bound exactly == cutoff -> the whole month is at/older -> drop
        cutoff = datetime(2026, 5, 1, tzinfo=UTC)
        assert _aged_cost_partitions(["billing_cost_events_2026_04"], cutoff) == [
            "billing_cost_events_2026_04"
        ]
        # one second before that boundary -> partition still straddles -> keep
        cutoff_minus = cutoff - timedelta(seconds=1)
        assert _aged_cost_partitions(["billing_cost_events_2026_04"], cutoff_minus) == []

    def test_december_partition_upper_rolls_to_next_year(self):
        # cost_events_2026_12 upper = 2027-01-01
        assert _aged_cost_partitions(
            ["billing_cost_events_2026_12"], datetime(2027, 1, 1, tzinfo=UTC)
        ) == ["billing_cost_events_2026_12"]
        assert (
            _aged_cost_partitions(
                ["billing_cost_events_2026_12"], datetime(2026, 12, 31, tzinfo=UTC)
            )
            == []
        )

    def test_non_matching_names_ignored(self):
        cutoff = datetime(2030, 1, 1, tzinfo=UTC)
        names = [
            "billing_cost_events_old",  # the standalone backup — never a partition match
            "billing_cost_events",  # the parent
            "billing_cost_events_2026_13",  # invalid month — rejected by the 01-12 regex
            "billing_metering_events",
            "billing_cost_events_2026_01",  # the only real match
        ]
        result = _aged_cost_partitions(names, cutoff)
        assert result == ["billing_cost_events_2026_01"]

    def test_returns_sorted(self):
        cutoff = datetime(2030, 1, 1, tzinfo=UTC)
        names = [
            "billing_cost_events_2026_03",
            "billing_cost_events_2026_01",
            "billing_cost_events_2026_02",
        ]
        assert _aged_cost_partitions(names, cutoff) == [
            "billing_cost_events_2026_01",
            "billing_cost_events_2026_02",
            "billing_cost_events_2026_03",
        ]


# ---------------------------------------------------------------------------
# SQL wrappers
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_ensure_cost_event_partitions_calls_helper_for_window(db_session, db_session_factory):
    """ensure_* invokes the idempotent DB helper for current..+N months.

    Unit schema's cost_events is not partitioned (Base.metadata.create_all), so
    install a stub create_cost_events_partition_for that returns the partition
    name without the real CREATE TABLE (same convention as
    test_cost_events_partitioning.py).
    """
    await db_session.execute(
        text("""
            CREATE OR REPLACE FUNCTION create_cost_events_partition_for(p_year INT, p_month INT)
            RETURNS TEXT LANGUAGE plpgsql AS $$
            BEGIN
                RETURN format('billing_cost_events_%s_%s',
                    lpad(p_year::text, 4, '0'), lpad(p_month::text, 2, '0'));
            END $$
        """)
    )
    await db_session.commit()

    expected = [
        f"billing_cost_events_{y:04d}_{m:02d}" for y, m in _months_to_ensure(datetime.now(UTC), 3)
    ]
    touched = await ensure_cost_event_partitions(db_session_factory, months_ahead=3)
    assert touched == expected
    assert len(touched) == 4


@pytest.mark.unit
async def test_drop_old_cost_partitions_noop_when_not_partitioned(db_session_factory):
    """Against a plain (non-partitioned) cost_events, pg_inherits is empty so the
    function is a safe no-op and never issues a DROP."""
    dropped = await drop_old_cost_partitions(db_session_factory, retention_days=90)
    assert dropped == []


@pytest.mark.integration
async def test_drop_old_cost_partitions_drops_only_fully_aged(db_engine):
    """End-to-end against a real partitioned cost_events: only the fully-aged
    partition is dropped; the current (straddling) month is kept."""
    from db import Base

    factory = async_sessionmaker(bind=db_engine, expire_on_commit=False)
    now = datetime.now(UTC)
    cur_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    nxt_start = (cur_start + timedelta(days=32)).replace(day=1)
    cur_name = f"billing_cost_events_{cur_start.year:04d}_{cur_start.month:02d}"

    try:
        async with factory() as s:
            # Rebuild cost_events as a real partitioned table (minimal columns
            # sufficient for partition management — full schema not needed here).
            await s.execute(text("DROP TABLE IF EXISTS billing_cost_events CASCADE"))
            await s.execute(
                text(
                    "CREATE TABLE billing_cost_events ("
                    "  id uuid DEFAULT gen_random_uuid(),"
                    "  created_at timestamptz NOT NULL,"
                    "  PRIMARY KEY (id, created_at)"
                    ") PARTITION BY RANGE (created_at)"
                )
            )
            # Fully-aged month (2020) and the current straddling month.
            await s.execute(
                text(
                    "CREATE TABLE billing_cost_events_2020_01 PARTITION OF billing_cost_events "
                    "FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-02-01 00:00:00+00')"
                )
            )
            await s.execute(
                text(
                    f"CREATE TABLE {cur_name} PARTITION OF billing_cost_events "
                    f"FOR VALUES FROM ('{cur_start.isoformat()}') TO ('{nxt_start.isoformat()}')"
                )
            )
            await s.commit()

        dropped = await drop_old_cost_partitions(factory, retention_days=90)
        assert dropped == ["billing_cost_events_2020_01"]

        async with factory() as s:
            remaining = (
                (
                    await s.execute(
                        text(
                            "SELECT c.relname FROM pg_inherits i "
                            "JOIN pg_class c ON c.oid = i.inhrelid "
                            "WHERE i.inhparent = 'billing_cost_events'::regclass"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert remaining == [cur_name]
    finally:
        # Restore the ORM cost_events so sibling tests in this session's schema
        # keep working (the session-scoped schema is shared within a worker).
        async with db_engine.begin() as conn:
            await conn.exec_driver_sql("DROP TABLE IF EXISTS billing_cost_events CASCADE")
            await conn.run_sync(Base.metadata.tables["billing_cost_events"].create)


# ---------------------------------------------------------------------------
# The DROP path bypasses BOTH post-incident controls, so it needs its own
# refusal.
#
# truncate_guard.py is a BEFORE TRUNCATE trigger; delete_audit.py fires AFTER
# DELETE / AFTER TRUNCATE. A DROP TABLE fires neither — delete_audit's own module
# docstring says so ("retention is invisible to this control by construction").
# So an unsafe retention_days destroys months of cost attribution in ONE
# transaction while billing_delete_audit stays correctly EMPTY: the destroyed day
# is indistinguishable from a healthy one. These tests fail on the pre-guard code
# by DROPping the partitions they assert are still present.
# ---------------------------------------------------------------------------


async def _build_partitioned_cost_events(factory, aged_months: list[tuple[int, int]]) -> str:
    """Rebuild cost_events partitioned, with ``aged_months`` + the current month.

    Returns the current (straddling, never-droppable) partition's name.
    """
    now = datetime.now(UTC)
    cur_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    nxt_start = (cur_start + timedelta(days=32)).replace(day=1)
    cur_name = f"billing_cost_events_{cur_start.year:04d}_{cur_start.month:02d}"
    async with factory() as s:
        await s.execute(text("DROP TABLE IF EXISTS billing_cost_events CASCADE"))
        await s.execute(
            text(
                "CREATE TABLE billing_cost_events ("
                "  id uuid DEFAULT gen_random_uuid(),"
                "  created_at timestamptz NOT NULL,"
                "  PRIMARY KEY (id, created_at)"
                ") PARTITION BY RANGE (created_at)"
            )
        )
        for year, month in aged_months:
            up_y, up_m = (year + 1, 1) if month == 12 else (year, month + 1)
            name = f"billing_cost_events_{year:04d}_{month:02d}"
            await s.execute(
                text(
                    f"CREATE TABLE {name} PARTITION OF billing_cost_events "
                    f"FOR VALUES FROM ('{year:04d}-{month:02d}-01 00:00:00+00') "
                    f"TO ('{up_y:04d}-{up_m:02d}-01 00:00:00+00')"
                )
            )
            # One real row per partition, so a DROP is a measurable loss of
            # financial records and not just of an empty shell.
            await s.execute(
                text(
                    f"INSERT INTO {name} (created_at) "
                    f"VALUES ('{year:04d}-{month:02d}-15 00:00:00+00')"
                )
            )
        await s.execute(
            text(
                f"CREATE TABLE {cur_name} PARTITION OF billing_cost_events "
                f"FOR VALUES FROM ('{cur_start.isoformat()}') TO ('{nxt_start.isoformat()}')"
            )
        )
        await s.commit()
    return cur_name


async def _live_partitions(factory) -> list[str]:
    async with factory() as s:
        return sorted(
            (
                await s.execute(
                    text(
                        "SELECT c.relname FROM pg_inherits i "
                        "JOIN pg_class c ON c.oid = i.inhrelid "
                        "WHERE i.inhparent = 'billing_cost_events'::regclass"
                    )
                )
            )
            .scalars()
            .all()
        )


async def _restore_orm_cost_events(db_engine) -> None:
    from db import Base

    async with db_engine.begin() as conn:
        await conn.exec_driver_sql("DROP TABLE IF EXISTS billing_cost_events CASCADE")
        await conn.run_sync(Base.metadata.tables["billing_cost_events"].create)


@pytest.mark.integration
async def test_drop_refuses_retention_days_zero_and_destroys_nothing(db_engine):
    """retention_days=0 puts the cutoff at ``now``: EVERY completed month ages out.

    Pre-guard this dropped 11 of the 12 partitions in one transaction and logged
    one INFO line. It must now drop nothing at all.
    """
    factory = async_sessionmaker(bind=db_engine, expire_on_commit=False)
    aged = [(2025, m) for m in range(1, 12)]  # 11 fully-aged monthly partitions
    try:
        cur_name = await _build_partitioned_cost_events(factory, aged)
        before = await _live_partitions(factory)
        assert len(before) == 12

        dropped = await drop_old_cost_partitions(factory, retention_days=0)

        assert dropped == []
        assert await _live_partitions(factory) == before
        assert cur_name in before
    finally:
        await _restore_orm_cost_events(db_engine)


@pytest.mark.integration
async def test_drop_refuses_when_the_drop_set_exceeds_the_per_run_bound(db_engine):
    """Even with a VALID retention_days, a drop set larger than the bound refuses.

    This is the argument-door half: settings.MIN_RETENTION_DAYS cannot help a
    caller that passes retention_days directly, and a legitimate-looking 90 still
    selects every one of these long-dead months at once.
    """
    factory = async_sessionmaker(bind=db_engine, expire_on_commit=False)
    aged = [(2020, m) for m in range(1, 6)]  # 5 > MAX_COST_PARTITIONS_DROPPED_PER_RUN
    try:
        await _build_partitioned_cost_events(factory, aged)
        before = await _live_partitions(factory)

        dropped = await drop_old_cost_partitions(factory, retention_days=90)

        assert dropped == []
        assert await _live_partitions(factory) == before
    finally:
        await _restore_orm_cost_events(db_engine)


@pytest.mark.integration
async def test_drop_at_the_bound_still_proceeds(db_engine):
    """The bound is inclusive: a one-missed-month catch-up is NOT a refusal.

    Guards that also block the legitimate case get switched off, so prove the
    steady state (and one month of slack) still works.
    """
    factory = async_sessionmaker(bind=db_engine, expire_on_commit=False)
    aged = [(2020, 1), (2020, 2)]  # == MAX_COST_PARTITIONS_DROPPED_PER_RUN
    assert len(aged) == MAX_COST_PARTITIONS_DROPPED_PER_RUN
    try:
        cur_name = await _build_partitioned_cost_events(factory, aged)

        dropped = await drop_old_cost_partitions(factory, retention_days=90)

        assert dropped == ["billing_cost_events_2020_01", "billing_cost_events_2020_02"]
        assert await _live_partitions(factory) == [cur_name]
    finally:
        await _restore_orm_cost_events(db_engine)


@pytest.mark.unit
async def test_drop_refuses_non_positive_retention_without_touching_the_db():
    """The retention_days<=0 refusal precedes any DB work at all.

    A session_factory that raises if used proves the refusal is not merely
    "found nothing to drop" against an empty catalogue.
    """

    def _exploding_factory():
        raise AssertionError("drop_old_cost_partitions opened a session for retention_days<=0")

    factory = cast("async_sessionmaker[AsyncSession]", _exploding_factory)
    assert await drop_old_cost_partitions(factory, retention_days=0) == []
    assert await drop_old_cost_partitions(factory, retention_days=-1) == []
