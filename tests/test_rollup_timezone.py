"""Regression proof: daily rollups bucket by UTC date regardless of session TZ.

Prod's billing pod runs against a pgbouncer session whose timezone is
Europe/Amsterdam, while every window/`target_date` in the rollup engine is
computed in UTC (`datetime.now(UTC)`). A bare `cast(created_at AS date)` resolves
in the session's LOCAL time, so an event at 23:30 UTC (01:30 Amsterdam next day)
was bucketed to the wrong calendar day — ~8.8% of live events at day boundaries.

These tests reproduce the prod condition by forcing the session timezone to
Europe/Amsterdam on EVERY pooled connection (the rollup opens its own session, so
a single `SET TIME ZONE` on the test session is not enough), then assert the
rollup still buckets by UTC. Test A fails on the pre-fix code (bare cast); Test B
guards the delete-then-rebuild that keeps restatement free of stale rows.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from models.db import CostDaily, CostEvent
from services.rollup_engine import rollup_daily
from tests.conftest import TEST_DATABASE_URL, T, pgoptions_with


@pytest.fixture
async def amsterdam_session_factory(db_engine):
    """A session factory whose EVERY connection is pinned to Europe/Amsterdam.

    Depends on `db_engine` so the schema exists and tables are truncated first.
    Sets the timezone via libpq `options` (``-c timezone=...``) in connect_args
    so postgres applies it at connection STARTUP for every pooled connection —
    reliable for async psycopg, unlike a "connect" event listener whose
    ``cursor().execute()`` returns an un-awaited coroutine and silently no-ops on
    some connections. This mirrors prod's non-UTC pgbouncer session, which is
    exactly the condition the bare ``cast(... AS date)`` mis-buckets under.

    The options string MUST go through ``pgoptions_with`` — an explicit libpq
    ``options`` parameter replaces ``PGOPTIONS`` instead of merging with it, so a
    bare literal drops the ``search_path`` conftest installs for a non-``public``
    ``TEST_SCHEMA`` and this fixture's engine lands in an empty ``public``.
    """
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
        connect_args={"options": pgoptions_with("timezone=Europe/Amsterdam")},
    )
    factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.mark.unit
async def test_daily_rollup_buckets_boundary_event_by_utc_date(amsterdam_session_factory):
    """An event at 23:30 UTC belongs to that UTC day, not the next Amsterdam day.

    FAILS on the pre-fix `cast(created_at AS date)` (Amsterdam session buckets it
    to the next day, so rollup_daily(utc_day) finds nothing and writes no row).
    """
    factory = amsterdam_session_factory
    # Sanity: the session really is Amsterdam, so this test exercises the bug.
    async with factory() as session:
        from sqlalchemy import text

        tz = (await session.execute(text("SHOW timezone"))).scalar_one()
    assert tz == "Europe/Amsterdam", f"fixture did not pin the session TZ (got {tz})"

    utc_day = date(2026, 6, 15)
    # 23:30 UTC on utc_day == 01:30 Amsterdam on utc_day+1.
    ts = datetime(2026, 6, 15, 23, 30, tzinfo=UTC)
    async with factory() as session:
        session.add(
            CostEvent(
                tenant_id=T("tz-boundary"),
                license_id=uuid.uuid4(),
                service_key="inference-router",  # pragma: allowlist secret
                provider="deepseek",
                input_tokens=1000,
                output_tokens=200,
                cost_usd=Decimal("0.03"),
                created_at=ts,
            )
        )
        await session.commit()

    await rollup_daily(factory, utc_day)

    async with factory() as session:
        rows = (
            (
                await session.execute(
                    select(CostDaily).where(CostDaily.tenant_id == T("tz-boundary"))
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1, "the 23:30-UTC event must roll up under its UTC day"
    assert rows[0].bucket_date == utc_day
    assert rows[0].total_cost_usd == Decimal("0.03")


@pytest.mark.unit
async def test_restatement_deletes_stale_rows_when_event_moves_day(amsterdam_session_factory):
    """Delete-then-rebuild: re-rolling a day whose events have moved away must
    REMOVE the stale bucket, not leave an over-counted orphan.

    Simulates the pre-fix state: a CostDaily row sits on the (wrong) Amsterdam
    day for a 23:30-UTC event. After the UTC fix, that event belongs to the prior
    UTC day, so re-rolling the Amsterdam day must find no source rows and clear
    the stale one. FAILS if rollup_daily UPSERTs without deleting first.
    """
    factory = amsterdam_session_factory
    utc_day = date(2026, 6, 15)
    amsterdam_day = date(2026, 6, 16)  # where the old code wrongly placed it
    ts = datetime(2026, 6, 15, 23, 30, tzinfo=UTC)

    async with factory() as session:
        session.add(
            CostEvent(
                tenant_id=T("tz-stale"),
                license_id=uuid.uuid4(),
                service_key="inference-router",  # pragma: allowlist secret
                provider="deepseek",
                input_tokens=1000,
                output_tokens=200,
                cost_usd=Decimal("0.03"),
                created_at=ts,
            )
        )
        # Pre-seed the stale bucket the OLD Amsterdam rollup would have written.
        session.add(
            CostDaily(
                tenant_id=T("tz-stale"),
                license_id=uuid.uuid4(),
                subject_id=uuid.uuid4(),
                service_key="inference-router",  # pragma: allowlist secret
                provider="deepseek",
                served_model="__unknown__",
                byo=False,
                owner_provided=False,
                cost_source="unknown",
                bucket_date=amsterdam_day,
                total_input_tokens=1000,
                total_output_tokens=200,
                total_cost_usd=Decimal("0.03"),
                request_count=1,
            )
        )
        await session.commit()

    # Re-roll the day the stale row sits on. Under the UTC fix the event is not in
    # this day, so the source query is empty and delete-then-rebuild clears it.
    await rollup_daily(factory, amsterdam_day)
    # And the event's real UTC day now carries it.
    await rollup_daily(factory, utc_day)

    async with factory() as session:
        rows = (
            (await session.execute(select(CostDaily).where(CostDaily.tenant_id == T("tz-stale"))))
            .scalars()
            .all()
        )
    by_day = {r.bucket_date: r.total_cost_usd for r in rows}
    assert amsterdam_day not in by_day, (
        "stale Amsterdam-day bucket must be deleted, not left orphaned"
    )
    assert by_day.get(utc_day) == Decimal("0.03"), "event must land on its UTC day"
    assert len(rows) == 1
