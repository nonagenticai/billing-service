"""Tests for services.rollup_engine (rollup_hourly + rollup_daily + cleanup)."""

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import fakeredis.aioredis
import pytest
from sqlalchemy import delete, func, insert, select

from models.db import (
    CostDaily,
    CostEvent,
    MeteringDaily,
    MeteringEvent,
    MeteringHourly,
    ProcessedCostMessage,
)
from services.aggregator import (
    cleanup_old_events,
    flush_costs,
    rollup_daily,
    rollup_hourly,
    trim_stream,
)
from services.consumer import _stage_message, ensure_consumer_groups
from services.rollup_engine import (
    COST_STREAM_MAX_AGE_HOURS,
    EVENTS_STREAM_MAX_AGE_HOURS,
    PROCESSED_MESSAGE_RETENTION_DAYS,
)
from settings import Settings
from tests._aggregator_helpers import _make_cost, _make_event
from tests.conftest import T


class TestCleanupOldEvents:
    @pytest.mark.unit
    async def test_cleanup_deletes_old_metering_events(self, db_session_factory):
        """Events older than retention_days should be deleted."""
        old_time = datetime.now(UTC) - timedelta(days=100)

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("t1"),
                        "license_id": uuid.uuid4(),
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/workers/api/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": old_time,
                    },
                ],
            )
            await session.commit()

        deleted = await cleanup_old_events(db_session_factory, retention_days=90)
        assert deleted >= 1

        async with db_session_factory() as session:
            count = (
                await session.execute(select(func.count()).select_from(MeteringEvent))
            ).scalar()
            assert count == 0

    @pytest.mark.unit
    async def test_cleanup_leaves_cost_events_untouched(self, db_session_factory):
        """cost_events retention is partition-native (drop_old_cost_partitions),
        NOT row-DELETE — cleanup_old_events must leave cost_events alone even for
        rows older than the retention window."""
        old_time = datetime.now(UTC) - timedelta(days=100)

        async with db_session_factory() as session:
            await session.execute(
                insert(CostEvent),
                [
                    {
                        "tenant_id": T("t1"),
                        "license_id": uuid.uuid4(),
                        "service_key": "inference-router",
                        "provider": "claude",
                        "input_tokens": 100,
                        "output_tokens": 50,
                        "cost_usd": Decimal("0.01"),
                        "created_at": old_time,
                    },
                ],
            )
            await session.commit()

        # cleanup_old_events only prunes metering_events now.
        deleted = await cleanup_old_events(db_session_factory, retention_days=90)
        assert deleted == 0

        async with db_session_factory() as session:
            count = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert count == 1  # cost_events untouched by cleanup_old_events

    @pytest.mark.unit
    async def test_cleanup_preserves_recent_events(self, db_session_factory):
        """Events within retention window should NOT be deleted."""
        recent_time = datetime.now(UTC) - timedelta(days=10)

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("t1"),
                        "license_id": uuid.uuid4(),
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/workers/api/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": recent_time,
                    },
                ],
            )
            await session.execute(
                insert(CostEvent),
                [
                    {
                        "tenant_id": T("t1"),
                        "license_id": uuid.uuid4(),
                        "service_key": "inference-router",
                        "provider": "claude",
                        "input_tokens": 100,
                        "output_tokens": 50,
                        "cost_usd": Decimal("0.01"),
                        "created_at": recent_time,
                    },
                ],
            )
            await session.commit()

        deleted = await cleanup_old_events(db_session_factory, retention_days=90)
        assert deleted == 0

        async with db_session_factory() as session:
            me_count = (
                await session.execute(select(func.count()).select_from(MeteringEvent))
            ).scalar()
            ce_count = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert me_count == 1
            assert ce_count == 1

    @pytest.mark.unit
    async def test_cleanup_mixed_old_and_recent(self, db_session_factory):
        """Only old events should be deleted; recent ones should survive."""
        old_time = datetime.now(UTC) - timedelta(days=100)
        recent_time = datetime.now(UTC) - timedelta(days=10)

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("old"),
                        "license_id": uuid.uuid4(),
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/workers/api/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": old_time,
                    },
                    {
                        "tenant_id": T("recent"),
                        "license_id": uuid.uuid4(),
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/workers/api/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": recent_time,
                    },
                ],
            )
            await session.commit()

        deleted = await cleanup_old_events(db_session_factory, retention_days=90)
        assert deleted == 1

        async with db_session_factory() as session:
            rows = (await session.execute(select(MeteringEvent))).scalars().all()
            assert len(rows) == 1
            assert rows[0].tenant_id == T("recent")

    @pytest.mark.unit
    async def test_cleanup_empty_tables(self, db_session_factory):
        """Cleanup on empty tables should return 0 and not fail."""
        deleted = await cleanup_old_events(db_session_factory, retention_days=90)
        assert deleted == 0

    @pytest.mark.unit
    async def test_cleanup_boundary_event_at_exact_cutoff(self, db_session_factory):
        """An event at EXACTLY the cutoff survives — the predicate is `<`, not `<=`.

        This needs an injected clock, which is why ``cleanup_old_events`` now
        takes ``now``. Without it the test can only seed at its own
        ``now - retention`` and hope: cleanup recomputes its cutoff a few
        milliseconds LATER, so the row is always strictly older and always
        deleted, and the boundary this test is named for is never exercised.

        The previous version said as much in a comment ("in practice this will
        be 0 or 1 depending on timing") and then asserted ``count + deleted == 1``
        — a tautology. The row either survives (1 + 0) or is deleted (0 + 1), so
        the sum is 1 whatever the code does. It could not fail.
        """
        retention = 30
        now = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
        cutoff = now - timedelta(days=retention)

        def _ev(label: str, created_at: datetime) -> dict:
            return {
                "tenant_id": T(label),
                "license_id": uuid.uuid4(),
                "app_id": uuid.uuid4(),
                "plan_id": uuid.uuid4(),
                "method": "GET",
                "path": "/x",
                "service_key": "x",
                "status": "allowed",
                "created_at": created_at,
            }

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    _ev("at-cutoff", cutoff),
                    _ev("just-before", cutoff - timedelta(microseconds=1)),
                ],
            )
            await session.commit()

        deleted = await cleanup_old_events(db_session_factory, retention_days=retention, now=now)

        # Exactly one side of the boundary goes. Asserting BOTH the count and
        # WHICH row survived is what makes this non-vacuous: a `<=` predicate
        # would delete both and fail on the first assert.
        assert deleted == 1
        async with db_session_factory() as session:
            rows = (await session.execute(select(MeteringEvent))).scalars().all()
            assert [r.tenant_id for r in rows] == [T("at-cutoff")]

    @pytest.mark.unit
    async def test_cleanup_large_retention(self, db_session_factory):
        """Cleanup with very large retention should delete nothing."""
        old_time = datetime.now(UTC) - timedelta(days=100)
        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("t"),
                        "license_id": uuid.uuid4(),
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/x",
                        "service_key": "x",
                        "status": "allowed",
                        "created_at": old_time,
                    },
                ],
            )
            await session.commit()

        deleted = await cleanup_old_events(db_session_factory, retention_days=365)
        assert deleted == 0

    @pytest.mark.unit
    async def test_cleanup_prunes_metering_only_not_cost(self, db_session_factory):
        """cleanup_old_events prunes metering_events (plain table) but NOT
        cost_events (partition-native retention via drop_old_cost_partitions),
        even when both have rows older than retention."""
        old_time = datetime.now(UTC) - timedelta(days=200)

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("t"),
                        "license_id": uuid.uuid4(),
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/x",
                        "service_key": "x",
                        "status": "allowed",
                        "created_at": old_time,
                    },
                ],
            )
            await session.execute(
                insert(CostEvent),
                [
                    {
                        "tenant_id": T("t"),
                        "license_id": uuid.uuid4(),
                        "service_key": "inference-router",
                        "provider": "claude",
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "cost_usd": Decimal("0.001"),
                        "created_at": old_time,
                    },
                ],
            )
            await session.commit()

        deleted = await cleanup_old_events(db_session_factory, retention_days=90)
        assert deleted == 1  # metering_events only

        async with db_session_factory() as session:
            cost_count = (
                await session.execute(select(func.count()).select_from(CostEvent))
            ).scalar()
            assert cost_count == 1  # cost_events left for partition-native retention


# ===========================================================================
# rollup_hourly / rollup_daily
# ===========================================================================


class TestRollups:
    @pytest.mark.unit
    async def test_rollup_hourly_empty(self, db_session_factory):
        """No events in the previous hour -> 0 rows rolled up."""
        count = await rollup_hourly(db_session_factory)
        assert count == 0

    @pytest.mark.unit
    async def test_rollup_hourly_with_events(self, db_session_factory):
        """Events in the previous hour should be aggregated into MeteringHourly."""
        now = datetime.now(UTC)
        bucket = now.replace(minute=0, second=0, microsecond=0)
        prev_hour = bucket - timedelta(hours=1)
        event_time = prev_hour + timedelta(minutes=30)  # middle of previous hour

        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        # Insert raw events in the previous hour's window
        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("rollup-t"),
                        "license_id": lic_id,
                        "app_id": app_id,
                        "plan_id": plan_id,
                        "method": "GET",
                        "path": "/workers/api/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": event_time,
                    },
                    {
                        "tenant_id": T("rollup-t"),
                        "license_id": lic_id,
                        "app_id": app_id,
                        "plan_id": plan_id,
                        "method": "POST",
                        "path": "/workers/api/x",
                        "service_key": "workers",
                        "status": "denied",
                        "created_at": event_time,
                    },
                ],
            )
            await session.commit()

        count = await rollup_hourly(db_session_factory)
        assert count == 1  # one group (same tenant/license/app/plan/service)

        # Verify MeteringHourly row
        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringHourly))).scalar_one()
            assert row.tenant_id == T("rollup-t")
            # 2 events, 1 of them DENIED. request_count is the BILLED axis
            # (generate_billing multiplies it by the per-app AppCostRate and
            # feeds the per-request surcharge), so only the allowed one counts;
            # the denial is recorded on denied_count. See
            # tests/test_rollup_denied.py::TestDeniedNotBilled.
            assert row.request_count == 1
            assert row.denied_count == 1
            # bucket_hour comes back from postgres in the session's tz (e.g.
            # Europe/Amsterdam on a developer laptop); normalize to UTC before
            # comparing the wall-clock value with prev_hour (which is UTC).
            assert row.bucket_hour.astimezone(UTC).replace(tzinfo=None) == prev_hour.replace(
                tzinfo=None
            )

    @pytest.mark.unit
    async def test_rollup_hourly_multiple_groups(self, db_session_factory):
        """Different service keys in same hour should produce separate rollup rows."""
        now = datetime.now(UTC)
        bucket = now.replace(minute=0, second=0, microsecond=0)
        prev_hour = bucket - timedelta(hours=1)
        event_time = prev_hour + timedelta(minutes=15)

        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("t"),
                        "license_id": lic_id,
                        "app_id": app_id,
                        "plan_id": plan_id,
                        "method": "GET",
                        "path": "/workers/api/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": event_time,
                    },
                    {
                        "tenant_id": T("t"),
                        "license_id": lic_id,
                        "app_id": app_id,
                        "plan_id": plan_id,
                        "method": "GET",
                        "path": "/kanban/api/boards",
                        "service_key": "kanban",
                        "status": "allowed",
                        "created_at": event_time,
                    },
                ],
            )
            await session.commit()

        count = await rollup_hourly(db_session_factory)
        assert count == 2  # Two separate groups

        async with db_session_factory() as session:
            rows = (await session.execute(select(MeteringHourly))).scalars().all()
            assert len(rows) == 2
            service_keys = {r.service_key for r in rows}
            assert service_keys == {"workers", "kanban"}

    @pytest.mark.unit
    async def test_rollup_hourly_ignores_current_hour(self, db_session_factory):
        """Events in the current hour should NOT be rolled up."""
        now = datetime.now(UTC)
        # Event in the current hour (after the bucket boundary)
        event_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=5)
        if event_time > now:
            # If we're within the first 5 minutes, put it at now
            event_time = now

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("current-hour"),
                        "license_id": uuid.uuid4(),
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": event_time,
                    },
                ],
            )
            await session.commit()

        count = await rollup_hourly(db_session_factory)
        assert count == 0

    @pytest.mark.unit
    async def test_rollup_hourly_ignores_old_events(self, db_session_factory):
        """Events from 2 hours ago should NOT be included in hourly rollup (only prev hour)."""
        now = datetime.now(UTC)
        bucket = now.replace(minute=0, second=0, microsecond=0)
        two_hours_ago = bucket - timedelta(hours=2)
        event_time = two_hours_ago + timedelta(minutes=30)

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("old-hour"),
                        "license_id": uuid.uuid4(),
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": event_time,
                    },
                ],
            )
            await session.commit()

        count = await rollup_hourly(db_session_factory)
        assert count == 0

    @pytest.mark.unit
    async def test_rollup_daily_empty(self, db_session_factory):
        """No hourly data for yesterday -> 0 rows rolled up."""
        count = await rollup_daily(db_session_factory)
        assert count == 0

    @pytest.mark.unit
    async def test_rollup_daily_no_data_for_yesterday(self, db_session_factory):
        """rollup_daily with no matching data returns 0 and doesn't fail."""
        count = await rollup_daily(db_session_factory)
        assert count == 0

    @pytest.mark.unit
    async def test_rollup_daily_with_metering_data(self, db_session_factory):
        """rollup_daily aggregates real MeteringHourly + CostEvent rows into the
        daily tables. Real-DB (not mocked call-counts) so it survives changes to
        the internal statement sequence and actually proves the output."""
        yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
        bucket = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
        lic = uuid.uuid4()
        async with db_session_factory() as session:
            session.add(
                MeteringHourly(
                    tenant_id=T("daily-t"),
                    license_id=lic,
                    subject_id=uuid.uuid4(),
                    app_id=uuid.uuid4(),
                    plan_id=uuid.uuid4(),
                    service_key="workers",
                    bucket_hour=bucket,
                    request_count=25,
                    denied_count=3,
                )
            )
            session.add(
                CostEvent(
                    tenant_id=T("daily-t"),
                    license_id=lic,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="claude",
                    input_tokens=5000,
                    output_tokens=1000,
                    cost_usd=Decimal("0.05"),
                    created_at=bucket,
                )
            )
            await session.commit()

        count = await rollup_daily(db_session_factory, yesterday)
        assert count == 1  # one metering group

        async with db_session_factory() as session:
            md = (
                await session.execute(
                    select(MeteringDaily).where(MeteringDaily.tenant_id == T("daily-t"))
                )
            ).scalar_one()
            assert md.request_count == 25
            assert md.denied_count == 3
            cd = (
                await session.execute(select(CostDaily).where(CostDaily.tenant_id == T("daily-t")))
            ).scalar_one()
            assert cd.total_input_tokens == 5000
            assert cd.total_cost_usd == Decimal("0.05")

    @pytest.mark.unit
    async def test_rollup_daily_with_cost_only(self, db_session_factory):
        """A day with cost rows but no metering rows returns 0 (the metering
        count) yet still writes the CostDaily row."""
        yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
        ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
        async with db_session_factory() as session:
            session.add(
                CostEvent(
                    tenant_id=T("cost-only-t"),
                    license_id=None,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="openai",
                    input_tokens=2000,
                    output_tokens=500,
                    cost_usd=Decimal("0.02"),
                    created_at=ts,
                )
            )
            await session.commit()

        count = await rollup_daily(db_session_factory, yesterday)
        assert count == 0  # no metering groups

        async with db_session_factory() as session:
            cd = (
                await session.execute(
                    select(CostDaily).where(CostDaily.tenant_id == T("cost-only-t"))
                )
            ).scalar_one()
            assert cd.total_cost_usd == Decimal("0.02")

    @pytest.mark.unit
    async def test_rollup_daily_multiple_metering_groups(self, db_session_factory):
        """Two distinct service_key groups produce two MeteringDaily rows."""
        yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
        bucket = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
        lic = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        async with db_session_factory() as session:
            session.add_all(
                [
                    MeteringHourly(
                        tenant_id=T("multi-t"),
                        license_id=lic,
                        subject_id=uuid.uuid4(),
                        app_id=app_id,
                        plan_id=plan_id,
                        service_key="workers",
                        bucket_hour=bucket,
                        request_count=10,
                        denied_count=1,
                    ),
                    MeteringHourly(
                        tenant_id=T("multi-t"),
                        license_id=lic,
                        subject_id=uuid.uuid4(),
                        app_id=app_id,
                        plan_id=plan_id,
                        service_key="kanban",
                        bucket_hour=bucket,
                        request_count=5,
                        denied_count=0,
                    ),
                ]
            )
            await session.commit()

        count = await rollup_daily(db_session_factory, yesterday)
        assert count == 2  # two metering groups

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringDaily).where(MeteringDaily.tenant_id == T("multi-t"))
                    )
                )
                .scalars()
                .all()
            )
            assert {r.service_key: r.request_count for r in rows} == {"workers": 10, "kanban": 5}


class TestRollupTenantRename:
    """A tenant renamed WITHIN a rollup window must not abort the rollup.

    tenant_name is NOT part of uq_daily / uq_cost_daily. If the aggregation still
    GROUPed BY tenant_name, a mid-window rename would emit two rows sharing one
    conflict-key; ``on_conflict_do_update`` then sends both in one statement and
    Postgres raises "ON CONFLICT DO UPDATE command cannot affect row a second
    time", aborting the whole daily rollup. The fix aggregates tenant_name with
    func.max(...) so there is exactly one row per constraint key.

    MUTATION-PROOF: reverting func.max(tenant_name) back to GROUP BY tenant_name
    makes both tests raise (rollup_daily blows up) instead of asserting.
    """

    @pytest.mark.unit
    async def test_cost_daily_rename_within_day_one_row(self, db_session_factory):
        yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
        ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC) + timedelta(hours=6)
        lic = uuid.uuid4()
        sub = uuid.uuid4()
        # Two cost events sharing the ENTIRE uq_cost_daily key (tenant_id,
        # license_id, subject_id, service_key, provider, served_model, byo,
        # owner_provided, cost_source, bucket_date) but a different tenant_name.
        async with db_session_factory() as session:
            session.add_all(
                [
                    CostEvent(
                        tenant_id=T("t-rename-c"),
                        tenant_name="aaa-old-name",
                        license_id=lic,
                        subject_id=sub,
                        service_key="inference-router",  # pragma: allowlist secret
                        provider="claude",
                        input_tokens=100,
                        output_tokens=50,
                        cost_usd=Decimal("0.01"),
                        created_at=ts,
                    ),
                    CostEvent(
                        tenant_id=T("t-rename-c"),
                        tenant_name="zzz-new-name",
                        license_id=lic,
                        subject_id=sub,
                        service_key="inference-router",  # pragma: allowlist secret
                        provider="claude",
                        input_tokens=200,
                        output_tokens=80,
                        cost_usd=Decimal("0.02"),
                        created_at=ts,
                    ),
                ]
            )
            await session.commit()

        # Must NOT raise (pre-fix: two rows, same uq key, in one UPSERT -> abort).
        await rollup_daily(db_session_factory, yesterday)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(CostDaily).where(CostDaily.tenant_id == T("t-rename-c"))
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1, "one conflict-key must collapse to exactly one cost_daily row"
        assert rows[0].tenant_name == "zzz-new-name"  # func.max(tenant_name)
        assert rows[0].total_input_tokens == 300  # both events summed into the one row
        assert rows[0].total_cost_usd == Decimal("0.03")

    @pytest.mark.unit
    async def test_metering_daily_rename_within_day_one_row(self, db_session_factory):
        yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
        bucket = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)
        lic = uuid.uuid4()
        sub = uuid.uuid4()
        app = uuid.uuid4()
        plan = uuid.uuid4()
        # Two hourly rows sharing the whole uq_daily grouping key but a different
        # tenant_name, on distinct hours of the same UTC day (a mid-day rename).
        async with db_session_factory() as session:
            session.add_all(
                [
                    MeteringHourly(
                        tenant_id=T("t-rename-m"),
                        tenant_name="aaa-old-name",
                        license_id=lic,
                        subject_id=sub,
                        app_id=app,
                        plan_id=plan,
                        service_key="workers",
                        bucket_hour=bucket + timedelta(hours=1),
                        request_count=3,
                        denied_count=0,
                    ),
                    MeteringHourly(
                        tenant_id=T("t-rename-m"),
                        tenant_name="zzz-new-name",
                        license_id=lic,
                        subject_id=sub,
                        app_id=app,
                        plan_id=plan,
                        service_key="workers",
                        bucket_hour=bucket + timedelta(hours=5),
                        request_count=5,
                        denied_count=0,
                    ),
                ]
            )
            await session.commit()

        await rollup_daily(db_session_factory, yesterday)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringDaily).where(MeteringDaily.tenant_id == T("t-rename-m"))
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1, "one conflict-key must collapse to exactly one metering_daily row"
        assert rows[0].tenant_name == "zzz-new-name"  # func.max(tenant_name)
        assert rows[0].request_count == 8  # both hourly rows summed


# ===========================================================================
# flush_loop
# ===========================================================================


class TestSubjectIdRollups:
    """Hourly + daily + cost_daily rollups all keep subject grain."""

    @pytest.mark.unit
    async def test_hourly_rollup_groups_by_subject(self, db_session_factory):
        from datetime import timedelta as _td

        from services.aggregator import rollup_hourly

        # Seed two events for two different subjects in the prior hour window.
        prior = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - _td(minutes=30)
        lic = uuid.uuid4()
        sub_a = uuid.uuid4()
        sub_b = uuid.uuid4()

        async with db_session_factory() as session:
            session.add_all(
                [
                    MeteringEvent(
                        tenant_id=T("t-rollup"),
                        license_id=lic,
                        subject_id=sub_a,
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        method="GET",
                        path="/workers/x",
                        service_key="workers",
                        status="allowed",
                        created_at=prior,
                    ),
                    MeteringEvent(
                        tenant_id=T("t-rollup"),
                        license_id=lic,
                        subject_id=sub_b,
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        method="GET",
                        path="/workers/x",
                        service_key="workers",
                        status="allowed",
                        created_at=prior,
                    ),
                ]
            )
            await session.commit()

        await rollup_hourly(db_session_factory)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringHourly).where(MeteringHourly.tenant_id == T("t-rollup"))
                    )
                )
                .scalars()
                .all()
            )
            subjects = {r.subject_id for r in rows}
            # Two distinct subject ids => two distinct hourly rows.
            assert sub_a in subjects
            assert sub_b in subjects
            assert len(rows) == 2

    @pytest.mark.unit
    async def test_daily_rollup_groups_by_subject(self, db_session_factory):
        from datetime import timedelta as _td

        from models.db import MeteringDaily
        from services.aggregator import rollup_daily

        yesterday = (datetime.now(UTC) - _td(days=1)).date()
        lic = uuid.uuid4()
        sub_a = uuid.uuid4()
        sub_b = uuid.uuid4()
        bucket = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)

        async with db_session_factory() as session:
            session.add_all(
                [
                    MeteringHourly(
                        tenant_id=T("t-roll-d"),
                        license_id=lic,
                        subject_id=sub_a,
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_hour=bucket,
                        request_count=3,
                        denied_count=0,
                    ),
                    MeteringHourly(
                        tenant_id=T("t-roll-d"),
                        license_id=lic,
                        subject_id=sub_b,
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_hour=bucket,
                        request_count=5,
                        denied_count=0,
                    ),
                ]
            )
            await session.commit()

        await rollup_daily(db_session_factory, yesterday)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringDaily).where(MeteringDaily.tenant_id == T("t-roll-d"))
                    )
                )
                .scalars()
                .all()
            )
            by_sub = {r.subject_id: r.request_count for r in rows}
            assert by_sub[sub_a] == 3
            assert by_sub[sub_b] == 5

    @pytest.mark.unit
    async def test_cost_daily_rollup_groups_by_subject(self, db_session_factory):
        from datetime import timedelta as _td

        from models.db import CostDaily
        from services.aggregator import rollup_daily

        yesterday = (datetime.now(UTC) - _td(days=1)).date()
        lic = uuid.uuid4()
        sub_a = uuid.uuid4()
        sub_b = uuid.uuid4()
        ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)

        async with db_session_factory() as session:
            session.add_all(
                [
                    CostEvent(
                        tenant_id=T("t-cost"),
                        license_id=lic,
                        subject_id=sub_a,
                        service_key="inference-router",
                        provider="claude",
                        input_tokens=100,
                        output_tokens=50,
                        cost_usd=Decimal("0.01"),
                        created_at=ts,
                    ),
                    CostEvent(
                        tenant_id=T("t-cost"),
                        license_id=lic,
                        subject_id=sub_b,
                        service_key="inference-router",
                        provider="claude",
                        input_tokens=200,
                        output_tokens=80,
                        cost_usd=Decimal("0.02"),
                        created_at=ts,
                    ),
                ]
            )
            await session.commit()

        await rollup_daily(db_session_factory, yesterday)

        async with db_session_factory() as session:
            rows = (
                (await session.execute(select(CostDaily).where(CostDaily.tenant_id == T("t-cost"))))
                .scalars()
                .all()
            )
            by_sub = {r.subject_id: r for r in rows}
            assert by_sub[sub_a].total_input_tokens == 100
            assert by_sub[sub_b].total_input_tokens == 200

    @pytest.mark.unit
    async def test_cost_daily_rollup_nullable_cost_usd_coalesces_to_zero(self, db_session_factory):
        """CostEvent.cost_usd is nullable; an unpriced provider produces NULL SUM().
        The rollup must COALESCE to 0 instead of violating CostDaily.total_cost_usd
        NOT NULL. Regression for the 2026-05-07 incident where claude was briefly
        enabled in inference-router without a provider_pricing row.

        ⚠️ THE COALESCE IS NOT A CLAIM THAT THE USAGE WAS FREE, and until 0031
        this test could not tell the difference — it asserted the stored 0 and
        stopped, which made the stored 0 the whole specification. A row that
        reads $0.000000 because nothing could be priced was byte-identical to
        one that reads $0.000000 because the usage genuinely cost nothing, and
        `services/reprice.py:207-222` names exactly that collapse as a defect.
        So the 0 assertion stays (the NOT NULL violation is still the thing the
        rollup must not do) and `unpriced_event_count` now carries the part the
        money column cannot: HOW MANY of the aggregated rows had no price.

        Uses raw SQL to insert NULL ``cost_usd`` directly — the ORM model has
        ``default=0`` which would otherwise mask the NULL on the Python side.
        """
        from datetime import timedelta as _td

        from sqlalchemy import text

        from models.db import CostDaily
        from services.aggregator import rollup_daily

        yesterday = (datetime.now(UTC) - _td(days=1)).date()
        lic = uuid.uuid4()
        sub = uuid.uuid4()
        ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)

        async with db_session_factory() as session:
            await session.execute(
                text(
                    "INSERT INTO billing_cost_events "
                    "(id, tenant_id, license_id, subject_id, service_key, provider, "
                    " input_tokens, output_tokens, cost_usd, created_at) "
                    "VALUES "
                    "(:id1, :t, :l, :s, :sk, :p, 100, 50, NULL, :ts), "
                    "(:id2, :t, :l, :s, :sk, :p, 200, 80, NULL, :ts)"
                ),
                {
                    "id1": uuid.uuid4(),
                    "id2": uuid.uuid4(),
                    "t": T("t-null-cost"),
                    "l": lic,
                    "s": sub,
                    "sk": "inference-router",
                    "p": "claude",
                    "ts": ts,
                },
            )
            await session.commit()

        await rollup_daily(db_session_factory, yesterday)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(CostDaily).where(CostDaily.tenant_id == T("t-null-cost"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            row = rows[0]
            assert row.total_cost_usd == Decimal("0")
            # ⭐ The assertion that makes the 0 above interpretable: both events
            # were unpriced, so this is "I do not know what 2 events cost", not
            # "2 events were free". Drop this line and the test goes back to
            # pinning the defect as the spec.
            assert row.unpriced_event_count == 2
            assert row.total_input_tokens == 300
            assert row.total_output_tokens == 130
            assert row.request_count == 2


# ===========================================================================
# rollup_daily(target_date) + latest_rolled_date watermark (catch-up support)
# ===========================================================================


class TestRollupDailyTargetDate:
    """rollup_daily honours an explicit target_date (used by the catch-up loop)."""

    @pytest.mark.unit
    async def test_rollup_daily_targets_explicit_date(self, db_session_factory):
        """rollup_daily(target_date) rolls up exactly that day — not yesterday —
        and ignores hourly/cost rows on other days."""
        from models.db import CostDaily, MeteringDaily

        target = (datetime.now(UTC) - timedelta(days=5)).date()
        other = (datetime.now(UTC) - timedelta(days=4)).date()
        lic = uuid.uuid4()
        sub = uuid.uuid4()
        bucket = datetime.combine(target, datetime.min.time(), tzinfo=UTC)
        other_bucket = datetime.combine(other, datetime.min.time(), tzinfo=UTC)

        async with db_session_factory() as session:
            session.add_all(
                [
                    MeteringHourly(
                        tenant_id=T("t-explicit"),
                        license_id=lic,
                        subject_id=sub,
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_hour=bucket,
                        request_count=7,
                        denied_count=0,
                    ),
                    # A different day's row must NOT be picked up by target_date.
                    MeteringHourly(
                        tenant_id=T("t-explicit"),
                        license_id=lic,
                        subject_id=sub,
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_hour=other_bucket,
                        request_count=99,
                        denied_count=0,
                    ),
                ]
            )
            session.add(
                CostEvent(
                    tenant_id=T("t-explicit"),
                    license_id=lic,
                    subject_id=sub,
                    service_key="inference-router",
                    provider="claude",
                    input_tokens=100,
                    output_tokens=50,
                    cost_usd=Decimal("0.01"),
                    created_at=bucket,
                )
            )
            await session.commit()

        await rollup_daily(db_session_factory, target)

        async with db_session_factory() as session:
            md = (
                (
                    await session.execute(
                        select(MeteringDaily).where(MeteringDaily.tenant_id == T("t-explicit"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(md) == 1
            assert md[0].bucket_date == target
            assert md[0].request_count == 7  # the target day only, not 99

            cd = (
                (
                    await session.execute(
                        select(CostDaily).where(CostDaily.tenant_id == T("t-explicit"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(cd) == 1
            assert cd[0].bucket_date == target
            assert cd[0].total_input_tokens == 100

    @pytest.mark.unit
    async def test_rollup_daily_idempotent_on_rerun(self, db_session_factory):
        """Re-running rollup_daily for the same date UPSERTs (replaces), never doubles."""
        from models.db import CostDaily, MeteringDaily

        target = (datetime.now(UTC) - timedelta(days=3)).date()
        lic = uuid.uuid4()
        sub = uuid.uuid4()
        bucket = datetime.combine(target, datetime.min.time(), tzinfo=UTC)

        async with db_session_factory() as session:
            session.add(
                MeteringHourly(
                    tenant_id=T("t-idem"),
                    license_id=lic,
                    subject_id=sub,
                    app_id=uuid.uuid4(),
                    plan_id=uuid.uuid4(),
                    service_key="workers",
                    bucket_hour=bucket,
                    request_count=4,
                    denied_count=1,
                )
            )
            session.add(
                CostEvent(
                    tenant_id=T("t-idem"),
                    license_id=lic,
                    subject_id=sub,
                    service_key="inference-router",
                    provider="claude",
                    input_tokens=10,
                    output_tokens=5,
                    cost_usd=Decimal("0.02"),
                    created_at=bucket,
                )
            )
            await session.commit()

        await rollup_daily(db_session_factory, target)
        await rollup_daily(db_session_factory, target)  # second pass must not duplicate

        async with db_session_factory() as session:
            md = (
                (
                    await session.execute(
                        select(MeteringDaily).where(MeteringDaily.tenant_id == T("t-idem"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(md) == 1
            assert md[0].request_count == 4  # replaced, not summed to 8

            cd = (
                (await session.execute(select(CostDaily).where(CostDaily.tenant_id == T("t-idem"))))
                .scalars()
                .all()
            )
            assert len(cd) == 1
            assert cd[0].total_input_tokens == 10  # not 20


class TestMissingDailyDates:
    """missing_daily_dates is gap-aware: it finds interior daily holes a
    max(bucket_date) watermark would skip."""

    @pytest.mark.unit
    async def test_finds_interior_hole_below_daily_max(self, db_session_factory):
        """metering_hourly has day D1 but metering_daily does not, while a LATER day
        D2 is present in metering_daily — so max(bucket_date)=D2 sits above the hole.
        missing_daily_dates must still return D1 (the regression that stranded the
        05-09..05-21 request-counts after the rolling deploy)."""
        from models.db import MeteringDaily
        from services.aggregator import missing_daily_dates

        now_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        d1 = (now_hour - timedelta(days=3)).date()  # hole: hourly yes, daily no
        d2 = (now_hour - timedelta(days=1)).date()  # present in daily (the max)
        floor = (now_hour - timedelta(days=20)).date()
        lic = uuid.uuid4()

        async with db_session_factory() as session:
            # Hourly rows for BOTH days.
            session.add_all(
                [
                    MeteringHourly(
                        tenant_id=T("t-hole"),
                        license_id=lic,
                        subject_id=uuid.uuid4(),
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_hour=datetime.combine(d1, datetime.min.time(), tzinfo=UTC)
                        + timedelta(hours=6),
                        request_count=3,
                        denied_count=0,
                    ),
                    MeteringHourly(
                        tenant_id=T("t-hole"),
                        license_id=lic,
                        subject_id=uuid.uuid4(),
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_hour=datetime.combine(d2, datetime.min.time(), tzinfo=UTC)
                        + timedelta(hours=6),
                        request_count=5,
                        denied_count=0,
                    ),
                ]
            )
            # Daily row for D2 only → D2 is the max, D1 is an interior hole.
            session.add(
                MeteringDaily(
                    tenant_id=T("t-hole"),
                    license_id=lic,
                    subject_id=uuid.uuid4(),
                    app_id=uuid.uuid4(),
                    plan_id=uuid.uuid4(),
                    service_key="workers",
                    bucket_date=d2,
                    request_count=5,
                    denied_count=0,
                )
            )
            await session.commit()

        missing = await missing_daily_dates(db_session_factory, floor)
        assert d1 in missing  # interior hole found despite a later daily max
        assert d2 not in missing  # already rolled

    @pytest.mark.unit
    async def test_excludes_today_and_respects_floor(self, db_session_factory):
        """The current (incomplete) day and days older than floor are never returned."""
        from services.aggregator import missing_daily_dates

        now_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        today = now_hour.date()
        too_old = (now_hour - timedelta(days=40)).date()
        floor = (now_hour - timedelta(days=10)).date()
        lic = uuid.uuid4()

        async with db_session_factory() as session:
            session.add_all(
                [
                    MeteringHourly(
                        tenant_id=T("t-bounds"),
                        license_id=lic,
                        subject_id=uuid.uuid4(),
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_hour=now_hour,  # today (incomplete)
                        request_count=1,
                        denied_count=0,
                    ),
                    MeteringHourly(
                        tenant_id=T("t-bounds"),
                        license_id=lic,
                        subject_id=uuid.uuid4(),
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_hour=datetime.combine(too_old, datetime.min.time(), tzinfo=UTC)
                        + timedelta(hours=6),  # older than floor
                        request_count=1,
                        denied_count=0,
                    ),
                ]
            )
            await session.commit()

        missing = await missing_daily_dates(db_session_factory, floor)
        assert today not in missing
        assert too_old not in missing

    @pytest.mark.unit
    async def test_detects_cost_only_gap(self, db_session_factory):
        """A day with cost_events but no cost_daily row is reported (cost side of the
        union), independent of the metering side."""
        from services.aggregator import missing_daily_dates

        now_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        d = (now_hour - timedelta(days=2)).date()
        floor = (now_hour - timedelta(days=10)).date()

        async with db_session_factory() as session:
            session.add(
                CostEvent(
                    tenant_id=T("t-costgap"),
                    license_id=uuid.uuid4(),
                    subject_id=uuid.uuid4(),
                    service_key="inference-router",
                    provider="claude",
                    input_tokens=10,
                    output_tokens=5,
                    cost_usd=Decimal("0.01"),
                    created_at=datetime.combine(d, datetime.min.time(), tzinfo=UTC)
                    + timedelta(hours=6),
                )
            )
            await session.commit()

        missing = await missing_daily_dates(db_session_factory, floor)
        assert d in missing


class TestRollupHourlyWindowAndGaps:
    """rollup_hourly honours an explicit window; missing_event_hours finds holes."""

    @pytest.mark.unit
    async def test_rollup_hourly_targets_explicit_window(self, db_session_factory):
        """rollup_hourly(window_start) rolls exactly that hour — the catch-up path —
        and ignores events in adjacent hours."""
        target_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
            days=3
        )
        other_hour = target_hour - timedelta(hours=1)
        lic = uuid.uuid4()
        app = uuid.uuid4()
        plan = uuid.uuid4()  # shared dims so the two target events aggregate to one row

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("t-hr"),
                        "license_id": lic,
                        "app_id": app,
                        "plan_id": plan,
                        "method": "GET",
                        "path": "/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": target_hour + timedelta(minutes=20),
                    },
                    {
                        "tenant_id": T("t-hr"),
                        "license_id": lic,
                        "app_id": app,
                        "plan_id": plan,
                        "method": "POST",
                        "path": "/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": target_hour + timedelta(minutes=40),
                    },
                    {  # adjacent hour — must be excluded by the window
                        "tenant_id": T("t-hr"),
                        "license_id": lic,
                        "app_id": app,
                        "plan_id": plan,
                        "method": "GET",
                        "path": "/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": other_hour + timedelta(minutes=20),
                    },
                ],
            )
            await session.commit()

        count = await rollup_hourly(db_session_factory, target_hour)
        assert count == 1

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringHourly).where(MeteringHourly.tenant_id == T("t-hr"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            assert rows[0].request_count == 2  # only the target hour's two events
            assert rows[0].bucket_hour.astimezone(UTC).replace(tzinfo=None) == target_hour.replace(
                tzinfo=None
            )

    @pytest.mark.unit
    async def test_missing_event_hours_excludes_present_and_current(self, db_session_factory):
        """Returns hours with raw events but no hourly row; skips already-rolled
        hours and the current (incomplete) hour."""
        from services.aggregator import missing_event_hours

        now_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        h_present = now_hour - timedelta(days=2)  # has events AND an hourly row
        h_missing = now_hour - timedelta(days=2, hours=1)  # has events, no hourly row
        floor = now_hour - timedelta(days=10)
        lic = uuid.uuid4()

        async with db_session_factory() as session:
            await session.execute(
                insert(MeteringEvent),
                [
                    {
                        "tenant_id": T("t-miss"),
                        "license_id": lic,
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": h_present + timedelta(minutes=10),
                    },
                    {
                        "tenant_id": T("t-miss"),
                        "license_id": lic,
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": h_missing + timedelta(minutes=10),
                    },
                    {  # current incomplete hour — must be excluded
                        "tenant_id": T("t-miss"),
                        "license_id": lic,
                        "app_id": uuid.uuid4(),
                        "plan_id": uuid.uuid4(),
                        "method": "GET",
                        "path": "/x",
                        "service_key": "workers",
                        "status": "allowed",
                        "created_at": now_hour + timedelta(minutes=5),
                    },
                ],
            )
            # An existing hourly row for h_present → excluded via EXCEPT.
            session.add(
                MeteringHourly(
                    tenant_id=T("t-miss"),
                    license_id=lic,
                    subject_id=uuid.uuid4(),
                    app_id=uuid.uuid4(),
                    plan_id=uuid.uuid4(),
                    service_key="workers",
                    bucket_hour=h_present,
                    request_count=1,
                    denied_count=0,
                )
            )
            await session.commit()

        missing = await missing_event_hours(db_session_factory, floor)
        norm = {m.astimezone(UTC).replace(tzinfo=None) for m in missing}
        assert h_missing.replace(tzinfo=None) in norm
        assert h_present.replace(tzinfo=None) not in norm  # already rolled
        assert now_hour.replace(tzinfo=None) not in norm  # current hour excluded


def _stream_settings(**overrides) -> Settings:
    defaults: dict[str, object] = {
        "database_url": "postgresql+psycopg://postgres:postgres@localhost:5436/metering_test",  # pragma: allowlist secret
        "redis_url": "redis://localhost:6379/15",
        "log_format": "console",
        "environment": "test",
        "licenses_url": "http://localhost:9999",
        "events_stream": "billing:events",
        "costs_stream": "billing:costs",
        "consumer_group": "billing-service",
        "consumer_name": "worker-0",
    }
    defaults.update(overrides)
    return Settings.model_validate(defaults)


async def _consume_once(fake_redis, settings, cost_buffer):
    """Read everything currently pending via ">" and stage it like the consumer."""
    event_buffer: list[dict] = []
    results = await fake_redis.xreadgroup(
        groupname=settings.consumer_group,
        consumername=settings.consumer_name,
        streams={settings.events_stream: ">", settings.costs_stream: ">"},
        count=100,
        block=0,
    )
    for stream_name, entries in results or []:
        for msg_id, fields in entries:
            _stage_message(fields, msg_id, stream_name, settings, event_buffer, cost_buffer)


async def _fake_redis_one_entry_per_stream_node() -> fakeredis.aioredis.FakeRedis:
    """FakeRedis whose stream nodes hold ONE entry each.

    ``trim_stream`` uses approximate ``XTRIM MINID ~``, which (in real Redis,
    and in fakeredis since 2.40) only drops WHOLE macro nodes. At the default
    ``stream-node-max-entries 100`` the few entries these tests add share one
    node, so ``~`` legitimately removes nothing. One entry per node makes
    ``~`` remove exactly the entries below the cutoff, so the exact counts
    asserted below stay meaningful against a faithful fake.
    """
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await fake.config_set("stream-node-max-entries", 1)
    return fake


class TestCostStreamAgeBound:
    """FIX A1 — age-bounding the cost stream prevents NOGROUP-replay double-billing."""

    @pytest.mark.unit
    def test_invariant_stream_age_below_inbox_retention(self):
        """Both stream age horizons MUST stay strictly below the inbox prune horizon."""
        assert COST_STREAM_MAX_AGE_HOURS < PROCESSED_MESSAGE_RETENTION_DAYS * 24
        assert EVENTS_STREAM_MAX_AGE_HOURS < PROCESSED_MESSAGE_RETENTION_DAYS * 24

    @pytest.mark.unit
    async def test_trim_removes_only_entries_older_than_horizon(self):
        fake = await _fake_redis_one_entry_per_stream_node()
        settings = _stream_settings()
        now = datetime.now(UTC)
        old_ms = int((now - timedelta(hours=40)).timestamp() * 1000)
        recent_ms = int((now - timedelta(hours=1)).timestamp() * 1000)
        await fake.xadd(
            settings.costs_stream, _make_cost(tenant_id=T("test-tenant")), id=f"{old_ms}-0"
        )
        await fake.xadd(
            settings.costs_stream, _make_cost(tenant_id=T("test-tenant")), id=f"{recent_ms}-0"
        )

        removed = await trim_stream(fake, settings.costs_stream, max_age_hours=36)

        assert removed == 1
        remaining = await fake.xrange(settings.costs_stream)
        ids = [entry_id for entry_id, _ in (remaining or [])]
        assert ids == [f"{recent_ms}-0"]

    @pytest.mark.unit
    async def test_trim_events_stream_removes_only_entries_older_than_horizon(self):
        """The EVENTS stream is age-trimmed too — a low-volume event can otherwise
        outlive its inbox row (MAXLEN-only bound) and double-count on NOGROUP replay."""
        fake = await _fake_redis_one_entry_per_stream_node()
        settings = _stream_settings()
        now = datetime.now(UTC)
        old_ms = int((now - timedelta(hours=40)).timestamp() * 1000)
        recent_ms = int((now - timedelta(hours=1)).timestamp() * 1000)
        await fake.xadd(
            settings.events_stream, _make_event(tenant_id=T("test-tenant")), id=f"{old_ms}-0"
        )
        await fake.xadd(
            settings.events_stream, _make_event(tenant_id=T("test-tenant")), id=f"{recent_ms}-0"
        )

        removed = await trim_stream(
            fake, settings.events_stream, max_age_hours=EVENTS_STREAM_MAX_AGE_HOURS
        )

        assert removed == 1
        remaining = await fake.xrange(settings.events_stream)
        ids = [entry_id for entry_id, _ in (remaining or [])]
        assert ids == [f"{recent_ms}-0"]

    @pytest.mark.unit
    async def test_trim_noop_without_redis(self):
        assert await trim_stream(None, "billing:costs") == 0

    @pytest.mark.unit
    async def test_trim_increments_expired_counter_by_the_returned_removed_count(self):
        """billing_cost_dead_letter_expired_total{stream=...} must move by
        EXACTLY what XTRIM itself reports removing -- never by an
        independently computed estimate, since MINID ``~`` makes the trim
        approximate. This is the counter that closes the gap
        billing_cost_dead_letter_depth cannot see: a sub-500 backlog
        expiring at 30d, one entry at a time, with the depth alert never
        tripping.

        Delta-based, not an absolute read at the end: the default
        prometheus registry is shared module state for the whole pytest
        session (see test_metrics_endpoint.py's ``_COLLECT_AT_BOOT``
        comment), and this exact stream label is incremented by sibling
        tests in this class (they trim ``billing:costs``, not
        ``billing:costs:dead``, but the label is per-stream so a sibling
        trimming a DIFFERENT stream cannot move this one -- proving the
        label actually discriminates is part of what this test is for).
        """
        import observability.metrics as om

        fake = await _fake_redis_one_entry_per_stream_node()
        settings = _stream_settings()
        stream = settings.cost_dead_letter_stream  # "billing:costs:dead"
        now = datetime.now(UTC)
        old_ms = int((now - timedelta(hours=800)).timestamp() * 1000)
        recent_ms = int((now - timedelta(hours=1)).timestamp() * 1000)
        # Two old entries past the 720h/30d default cutoff, one recent one
        # that must survive -- so a removed count of exactly 2 is the only
        # correct outcome, not "some" or "all".
        await fake.xadd(stream, {"_dlq_reason": "nil_tenant_id"}, id=f"{old_ms}-0")
        await fake.xadd(stream, {"_dlq_reason": "nil_tenant_id"}, id=f"{old_ms}-1")
        await fake.xadd(stream, {"_dlq_reason": "nil_tenant_id"}, id=f"{recent_ms}-0")

        before = om.cost_dead_letter_expired_total.labels(stream=stream)._value.get()
        removed = await trim_stream(
            fake, stream, max_age_hours=settings.dead_letter_stream_max_age_hours
        )
        after = om.cost_dead_letter_expired_total.labels(stream=stream)._value.get()

        assert removed == 2, "test fixture premise: exactly 2 entries are past cutoff"
        assert after - before == removed, (
            f"counter moved by {after - before}, XTRIM reported removing {removed}"
        )
        remaining = await fake.xrange(stream)
        assert [entry_id for entry_id, _ in (remaining or [])] == [f"{recent_ms}-0"]

    @pytest.mark.unit
    async def test_age_bound_prevents_double_insert_on_group_recreate(self, db_session_factory):
        """A forced group-recreate-at-0 replay must NOT re-insert a previously-pruned
        entry once the stream has been age-trimmed below the inbox retention horizon."""
        fake = await _fake_redis_one_entry_per_stream_node()
        settings = _stream_settings()
        cg = settings.consumer_group
        now = datetime.now(UTC)
        old_ms = int((now - timedelta(hours=40)).timestamp() * 1000)
        recent_ms = int((now - timedelta(hours=1)).timestamp() * 1000)

        await fake.xadd(
            settings.costs_stream, _make_cost(tenant_id=T("test-tenant")), id=f"{old_ms}-0"
        )
        await fake.xadd(
            settings.costs_stream, _make_cost(tenant_id=T("test-tenant")), id=f"{recent_ms}-0"
        )
        await ensure_consumer_groups(fake, settings)

        # First consume + flush — both rows land, both inbox rows claimed, both acked.
        buf: list[dict] = []
        await _consume_once(fake, settings, buf)
        first = await flush_costs(buf, db_session_factory, fake, cg)
        assert first == 2

        # Simulate the inbox prune of the OLD entry (>48h => row deleted), while the
        # recent entry's dedup row survives.
        async with db_session_factory() as session:
            await session.execute(
                delete(ProcessedCostMessage).where(
                    ProcessedCostMessage.stream_msg_id == f"{old_ms}-0"
                )
            )
            await session.commit()

        # FIX A1: age-trim the stream so the now-unprotected old entry is gone.
        removed = await trim_stream(fake, settings.costs_stream, max_age_hours=36)
        assert removed == 1

        # NOGROUP replay: destroy + recreate the group at id=0 and re-read everything.
        await fake.xgroup_destroy(settings.events_stream, cg)
        await fake.xgroup_destroy(settings.costs_stream, cg)
        await ensure_consumer_groups(fake, settings)
        buf2: list[dict] = []
        await _consume_once(fake, settings, buf2)
        second = await flush_costs(buf2, db_session_factory, fake, cg)
        # Only the recent entry replays (old was trimmed) and it dedups → 0 new.
        assert second == 0

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
        assert n == 2  # old entry NOT double-inserted

    @pytest.mark.unit
    async def test_without_trim_pruned_entry_double_inserts(self, db_session_factory):
        """Counterfactual proving the trim is load-bearing: WITHOUT age-trimming, a
        group-recreate replay re-inserts the entry whose inbox row was pruned."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        settings = _stream_settings()
        cg = settings.consumer_group
        now = datetime.now(UTC)
        old_ms = int((now - timedelta(hours=40)).timestamp() * 1000)

        await fake.xadd(
            settings.costs_stream, _make_cost(tenant_id=T("test-tenant")), id=f"{old_ms}-0"
        )
        await ensure_consumer_groups(fake, settings)
        buf: list[dict] = []
        await _consume_once(fake, settings, buf)
        assert await flush_costs(buf, db_session_factory, fake, cg) == 1

        # Prune the inbox row but DO NOT trim the stream (the pre-A1 bug).
        async with db_session_factory() as session:
            await session.execute(
                delete(ProcessedCostMessage).where(
                    ProcessedCostMessage.stream_msg_id == f"{old_ms}-0"
                )
            )
            await session.commit()

        await fake.xgroup_destroy(settings.events_stream, cg)
        await fake.xgroup_destroy(settings.costs_stream, cg)
        await ensure_consumer_groups(fake, settings)
        buf2: list[dict] = []
        await _consume_once(fake, settings, buf2)
        # The still-present stream entry replays and, with its inbox row gone,
        # is re-inserted → the double-count the A1 fix exists to prevent.
        assert await flush_costs(buf2, db_session_factory, fake, cg) == 1

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
        assert n == 2


class TestReconcileIsNonDestructive:
    """The trailing reconcile window must never erase a bucket it cannot rebuild.

    ``services.aggregator.run_rollup_loop`` re-rolls the last
    ``settings.daily_reconcile_days`` COMPLETE days on every pass. Until this
    class existed, ``rollup_daily`` opened with an unconditional
    ``DELETE ... WHERE bucket_date = target_date`` on both daily tables and only
    re-inserted ``if rows:`` / ``if cost_rows:``. Those two facts compose into a
    data-destruction bug the moment the raw sources for a day are gone —
    retention aged them out, a partition was dropped, or an operator truncated
    the table: the delete lands, the insert is skipped, and a good historical
    bucket is destroyed by the pass whose entire job is to CORRECT it.

    This is not hypothetical. ``billing_cost_events`` was truncated in prod at
    2026-09-05T07:01:56Z; the next reconcile pass destroyed the 2026-09-02,
    09-03 and 09-04 ``billing_cost_daily`` buckets — precisely the trailing
    3-day window — leaving ``max(bucket_date)`` stranded at 2026-09-01 with the
    older days intact. The daily tables were the last copy of that spend, so it
    is gone.

    Each test drives the real reconcile entry point over a day whose source rows
    have been removed. Against the pre-fix code every one of them fails on a
    ``NoResultFound`` from the ``scalar_one()``, because the bucket it is
    asserting has just been deleted.
    """

    @staticmethod
    def _past_day(days_ago: int = 2) -> date:
        """A COMPLETE past day — i.e. one the reconcile window actually re-rolls."""
        return (datetime.now(UTC) - timedelta(days=days_ago)).date()

    @pytest.mark.unit
    async def test_reconcile_preserves_cost_bucket_when_source_rows_are_gone(
        self, db_session_factory
    ):
        """A reconcile pass over a day with zero cost_events keeps its CostDaily row."""
        day = self._past_day()
        tenant = T("reconcile-cost")

        # Reproduces the prod shape exactly: a good historical bucket from an
        # earlier successful rollup, no source rows for `day`, and source rows
        # that resume AFTER it — i.e. the table was truncated and has since been
        # refilled by live traffic. It is that "newer rows exist" detail that
        # makes the naive `if not rows: skip` guard insufficient and forces the
        # coverage discriminator: the source is non-empty, it just no longer
        # reaches back to this day.
        async with db_session_factory() as session:
            session.add(
                CostEvent(
                    tenant_id=tenant,
                    license_id=None,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="openai",
                    input_tokens=1,
                    output_tokens=1,
                    cost_usd=Decimal("0.000001"),
                    created_at=datetime.now(UTC),
                )
            )
            session.add(
                CostDaily(
                    tenant_id=tenant,
                    tenant_name=tenant,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="openai",
                    bucket_date=day,
                    total_input_tokens=2000,
                    total_output_tokens=500,
                    total_cost_usd=Decimal("1.234567"),
                    request_count=7,
                )
            )
            await session.commit()

        await rollup_daily(db_session_factory, day)

        async with db_session_factory() as session:
            cd = (
                await session.execute(select(CostDaily).where(CostDaily.tenant_id == tenant))
            ).scalar_one()
            # Untouched, not merely present: a reconcile with nothing to say
            # about a day must leave every number on it alone.
            assert cd.total_cost_usd == Decimal("1.234567")
            assert cd.total_input_tokens == 2000
            assert cd.total_output_tokens == 500
            assert cd.request_count == 7

    @pytest.mark.unit
    async def test_reconcile_preserves_metering_bucket_when_source_rows_are_gone(
        self, db_session_factory
    ):
        """Same contract on the metering side — it has the identical shape.

        ``metering_daily`` reads ``metering_hourly`` while ``cost_daily`` reads
        ``cost_events``: two different sources on two different retention
        clocks. Guarding only one of them would leave the other destroying
        history, so both are asserted.
        """
        day = self._past_day()
        tenant = T("reconcile-metering")

        async with db_session_factory() as session:
            # Source history resumes after `day` — same post-truncate shape as
            # the cost test above.
            session.add(
                MeteringHourly(
                    tenant_id=tenant,
                    service_key="workers",
                    bucket_hour=datetime.now(UTC).replace(minute=0, second=0, microsecond=0),
                    request_count=1,
                )
            )
            session.add(
                MeteringDaily(
                    tenant_id=tenant,
                    tenant_name=tenant,
                    service_key="workers",
                    bucket_date=day,
                    request_count=42,
                    denied_count=3,
                    budget_denied_count=1,
                )
            )
            await session.commit()

        await rollup_daily(db_session_factory, day)

        async with db_session_factory() as session:
            md = (
                await session.execute(
                    select(MeteringDaily).where(MeteringDaily.tenant_id == tenant)
                )
            ).scalar_one()
            assert md.request_count == 42
            assert md.denied_count == 3
            assert md.budget_denied_count == 1

    @pytest.mark.unit
    async def test_one_empty_source_cannot_erase_the_other_tables_bucket(self, db_session_factory):
        """cost_events present + metering_hourly empty rebuilds cost, keeps metering.

        The two guards are independent. This pins that independence: a day that
        still has cost source data must rebuild its CostDaily row (the delete
        DOES run there, so the rebuild is a real replacement, not an append)
        while the metering bucket — whose source is gone — survives untouched.
        """
        day = self._past_day()
        tenant = T("reconcile-split")
        ts = datetime.combine(day, datetime.min.time(), tzinfo=UTC)

        async with db_session_factory() as session:
            # A STALE cost bucket that the rebuild must overwrite, proving the
            # delete still fires on the has-source path.
            session.add(
                CostDaily(
                    tenant_id=tenant,
                    tenant_name=tenant,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="openai",
                    bucket_date=day,
                    total_cost_usd=Decimal("99.000000"),
                    request_count=999,
                )
            )
            session.add(
                MeteringDaily(
                    tenant_id=tenant,
                    tenant_name=tenant,
                    service_key="workers",
                    bucket_date=day,
                    request_count=42,
                )
            )
            session.add(
                CostEvent(
                    tenant_id=tenant,
                    license_id=None,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="openai",
                    input_tokens=10,
                    output_tokens=5,
                    cost_usd=Decimal("0.010000"),
                    created_at=ts,
                )
            )
            await session.commit()

        await rollup_daily(db_session_factory, day)

        async with db_session_factory() as session:
            cost = (
                (await session.execute(select(CostDaily).where(CostDaily.tenant_id == tenant)))
                .scalars()
                .all()
            )
            # Exactly one row, carrying the RECOMPUTED total — the stale 99.00
            # bucket was replaced, not added to.
            assert len(cost) == 1
            assert cost[0].total_cost_usd == Decimal("0.010000")
            assert cost[0].request_count == 1

            md = (
                await session.execute(
                    select(MeteringDaily).where(MeteringDaily.tenant_id == tenant)
                )
            ).scalar_one()
            assert md.request_count == 42

    @pytest.mark.unit
    async def test_a_day_still_covered_by_source_history_is_still_cleared(self, db_session_factory):
        """The positive arm of the discriminator — the guard must not over-preserve.

        Paired with the preservation tests above. Here the source DOES still
        reach back through the day (an event sits earlier than the day's end),
        so an empty result for that day is a real measurement — the events moved
        or were removed — and the stale bucket must be cleared as before. This
        is the same guarantee
        ``test_rollup_timezone.py::test_restatement_deletes_stale_rows_when_event_moves_day``
        enforces; it is restated here so the two arms of one decision sit
        together and neither can be "simplified" away without reddening its pair.
        """
        day = self._past_day(2)
        tenant = T("reconcile-covered")
        # The event lives on the PREVIOUS day, so `day` itself aggregates to
        # nothing while source history plainly still covers it.
        earlier = datetime.combine(self._past_day(3), datetime.min.time(), tzinfo=UTC) + timedelta(
            hours=12
        )

        async with db_session_factory() as session:
            session.add(
                CostEvent(
                    tenant_id=tenant,
                    license_id=None,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="openai",
                    input_tokens=10,
                    output_tokens=5,
                    cost_usd=Decimal("0.010000"),
                    created_at=earlier,
                )
            )
            session.add(
                CostDaily(
                    tenant_id=tenant,
                    tenant_name=tenant,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="openai",
                    bucket_date=day,
                    total_cost_usd=Decimal("5.000000"),
                    request_count=5,
                )
            )
            await session.commit()

        await rollup_daily(db_session_factory, day)

        async with db_session_factory() as session:
            remaining = (
                (
                    await session.execute(
                        select(CostDaily).where(
                            CostDaily.tenant_id == tenant, CostDaily.bucket_date == day
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert remaining == [], "a day the source still covers must be cleared, not frozen"


class TestDailyRollupDateGaps:
    """The rollup must be able to notice a day that is simply ABSENT.

    THE DEFECT. ``missing_daily_dates`` builds its candidate set FROM the source
    tables (``metering_hourly`` / ``cost_events``), so a day with no source rows
    at all was never a candidate and could never be reported. The evidence of the
    gap and the evidence needed to detect it were the same rows.

    Measured 2026-09-05: ``billing_cost_daily`` holds zero rows for 2026-09-02,
    09-03 and 09-04 and would never have reported them, because the 07:01:56Z
    TRUNCATE removed the events those dates would have been derived from.
    ``sum(total_cost_usd)`` for 2026-04-26..09-01 is $5,613.14 of rollups the
    event table can no longer substantiate.

    ``daily_rollup_date_gaps`` derives its candidates from a CALENDAR range
    instead and is therefore blind to the source tables entirely. It REPORTS —
    it never rolls, inserts or deletes, and its result is never fed back to
    ``rollup_daily``, because re-rolling a day whose source is gone deletes the
    surviving bucket rather than rebuilding it.
    """

    @staticmethod
    def _day(days_ago: int) -> date:
        return (datetime.now(UTC) - timedelta(days=days_ago)).date()

    async def _seed_prod_shape(self, db_session_factory, tenant: str) -> tuple[date, date, list]:
        """CostDaily rows on an OLD and a RECENT day, nothing between, no events.

        The exact prod shape: good historical buckets either side of a hole, and
        an EMPTY cost_events (the table was truncated). Returns
        ``(anchor_old, anchor_recent, hole_dates)``.
        """
        anchor_old = self._day(8)
        anchor_recent = self._day(2)
        holes = [self._day(n) for n in (7, 6, 5, 4, 3)]

        async with db_session_factory() as session:
            for day in (anchor_old, anchor_recent):
                session.add(
                    CostDaily(
                        tenant_id=tenant,
                        tenant_name="acme-corp",
                        service_key="inference-router",  # pragma: allowlist secret
                        provider="openai",
                        bucket_date=day,
                        total_input_tokens=2000,
                        total_output_tokens=500,
                        total_cost_usd=Decimal("1.234567"),
                        request_count=7,
                    )
                )
            await session.commit()
        return anchor_old, anchor_recent, holes

    @pytest.mark.unit
    async def test_a_day_with_zero_events_is_reported_as_a_gap(self, db_session_factory):
        """The fix itself, on the exact shape that was invisible in production."""
        from services.rollup_engine import daily_rollup_date_gaps

        tenant = T("gap-cost")
        anchor_old, anchor_recent, holes = await self._seed_prod_shape(db_session_factory, tenant)
        floor = self._day(30)

        gaps = await daily_rollup_date_gaps(db_session_factory, floor)

        for hole in holes:
            assert hole in gaps["cost_daily"], f"{hole} is absent from cost_daily and unreported"
        # The days that DO have a bucket are not gaps.
        assert anchor_old not in gaps["cost_daily"]
        assert anchor_recent not in gaps["cost_daily"]

    @pytest.mark.unit
    async def test_the_old_detector_is_blind_to_the_same_days(self, db_session_factory):
        """THE CONTROL. Without this, the new function could be asserting nothing.

        ``missing_daily_dates`` must still return none of these dates — that
        blindness IS the defect, and it is why gap detection had to be built as a
        separate, source-independent function rather than by widening that one.
        """
        from services.aggregator import missing_daily_dates

        tenant = T("gap-control")
        _old, _recent, holes = await self._seed_prod_shape(db_session_factory, tenant)
        floor = self._day(30)

        missing = await missing_daily_dates(db_session_factory, floor)
        assert not (set(holes) & set(missing)), (
            "missing_daily_dates suddenly reports empty days — it now feeds them "
            "to rollup_daily, whose guarded delete is the only thing protecting "
            "orphaned rollups"
        )

    @pytest.mark.unit
    async def test_reporting_a_gap_deletes_nothing(self, db_session_factory):
        """⛔ THE HIGHEST-RISK PROPERTY. The reporter has no write path.

        Every surviving bucket must still be there afterwards, byte for byte.
        This is what makes it safe to run on every daily pass beside a
        ``rollup_daily`` whose delete guard is the only thing standing between
        $5,613.14 of orphaned rollups and a DELETE.
        """
        from services.rollup_engine import daily_rollup_date_gaps

        tenant = T("gap-nodelete")
        await self._seed_prod_shape(db_session_factory, tenant)

        async with db_session_factory() as session:
            before = (
                await session.execute(
                    select(CostDaily.bucket_date, CostDaily.total_cost_usd).order_by(
                        CostDaily.bucket_date
                    )
                )
            ).all()
        assert len(before) == 2

        await daily_rollup_date_gaps(db_session_factory, self._day(30))

        async with db_session_factory() as session:
            after = (
                await session.execute(
                    select(CostDaily.bucket_date, CostDaily.total_cost_usd).order_by(
                        CostDaily.bucket_date
                    )
                )
            ).all()
            events = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
        assert after == before
        assert events == 0  # and it did not manufacture source rows either

    @pytest.mark.unit
    async def test_the_delete_guard_still_holds_for_a_day_with_rollups_and_no_events(
        self, db_session_factory
    ):
        """A day with a bucket but zero events survives the daily pass, unchanged.

        Runs the REAL ``rollup_daily`` over the anchor days of the same shape.
        ``cost_covered`` is False (cost_events is empty, so no row is older than
        the day's end), so the guard refuses the delete and the only surviving
        record of that day's spend is preserved. Pinned here as well as in
        ``TestReconcileDoesNotEraseUnbackedRollups`` because this change adds a
        caller to the daily pass, and a regression that made ``cost_covered``
        False-negative would destroy exactly these rows.
        """
        from services.rollup_engine import daily_rollup_date_gaps

        tenant = T("gap-guard")
        anchor_old, anchor_recent, holes = await self._seed_prod_shape(db_session_factory, tenant)

        gaps = await daily_rollup_date_gaps(db_session_factory, self._day(30))
        assert set(holes) <= set(gaps["cost_daily"])

        # The daily pass rolls the reconcile window + the repair list; neither
        # contains the reported gaps, but both reach the anchors.
        for day in (anchor_old, anchor_recent, *holes):
            await rollup_daily(db_session_factory, day)

        async with db_session_factory() as session:
            rows = (
                await session.execute(
                    select(CostDaily.bucket_date, CostDaily.total_cost_usd).order_by(
                        CostDaily.bucket_date
                    )
                )
            ).all()
        assert [r[0] for r in rows] == [anchor_old, anchor_recent]
        assert all(r[1] == Decimal("1.234567") for r in rows)

    @pytest.mark.unit
    async def test_an_empty_table_reports_nothing(self, db_session_factory):
        """No rollups anywhere is a different condition, and not this one.

        Reporting the whole retention window as "missing" for a service with no
        history would be noise, not a gap — there is nothing to be missing from.
        """
        from services.rollup_engine import daily_rollup_date_gaps

        gaps = await daily_rollup_date_gaps(db_session_factory, self._day(30))
        assert gaps == {"cost_daily": [], "metering_daily": []}

    @pytest.mark.unit
    async def test_days_before_the_oldest_bucket_are_not_reported(self, db_session_factory):
        """The lower bound is the table's own oldest surviving bucket, not `floor`.

        Days before the service had any history are not gaps.
        """
        from services.rollup_engine import daily_rollup_date_gaps

        tenant = T("gap-anchor")
        async with db_session_factory() as session:
            session.add(
                CostDaily(
                    tenant_id=tenant,
                    tenant_name="acme-corp",
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="openai",
                    bucket_date=self._day(3),
                    total_cost_usd=Decimal("1.000000"),
                    request_count=1,
                )
            )
            await session.commit()

        gaps = await daily_rollup_date_gaps(db_session_factory, self._day(30))
        assert self._day(20) not in gaps["cost_daily"]
        assert self._day(4) not in gaps["cost_daily"]
        # The days AFTER the anchor and before today are real gaps.
        assert self._day(2) in gaps["cost_daily"]
        assert self._day(1) in gaps["cost_daily"]
        # Today is incomplete and is never reported.
        assert self._day(0) not in gaps["cost_daily"]

    @pytest.mark.unit
    async def test_the_metering_side_is_measured_independently(self, db_session_factory):
        """Two tables, two retention clocks, two anchors — one must not mask the other."""
        from services.rollup_engine import daily_rollup_date_gaps

        tenant = T("gap-metering")
        lic = uuid.uuid4()
        async with db_session_factory() as session:
            for day in (self._day(5), self._day(2)):
                session.add(
                    MeteringDaily(
                        tenant_id=tenant,
                        license_id=lic,
                        subject_id=uuid.uuid4(),
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="workers",
                        bucket_date=day,
                        request_count=1,
                        denied_count=0,
                    )
                )
            await session.commit()

        gaps = await daily_rollup_date_gaps(db_session_factory, self._day(30))
        assert self._day(4) in gaps["metering_daily"]
        assert self._day(3) in gaps["metering_daily"]
        assert gaps["cost_daily"] == []  # cost_daily is empty → silent, not noisy

    @pytest.mark.unit
    async def test_the_gauge_carries_the_count(self, db_session_factory):
        """A gap nothing counts is the failure mode this closes; the log alone is
        not enough, because nothing alerts on a log line here."""
        import observability.metrics as om
        from services.rollup_engine import daily_rollup_date_gaps

        tenant = T("gap-gauge")
        _old, _recent, holes = await self._seed_prod_shape(db_session_factory, tenant)

        gaps = await daily_rollup_date_gaps(db_session_factory, self._day(30))
        # The gauge is the count the function actually reported, not a
        # re-derivation of it — a second derivation here could agree with a
        # broken one. NB the reported set is a SUPERSET of the seeded holes: the
        # complete days after the most recent anchor are genuine gaps too.
        assert set(holes) < set(gaps["cost_daily"])
        assert om.rollup_date_gaps.labels(table="cost_daily")._value.get() == len(
            gaps["cost_daily"]
        )


class TestUnrolledSourceDays:
    """The precise "a SOURCE day has no daily row" signal.

    Its entire reason to exist is the case the two older gauges get wrong: a day
    with ZERO source rows must report 0, not look like a missed rollup. Measured
    in production 2026-11-26: metering_daily read lag=2 because
    billing_metering_events held zero rows for 2026-11-25, while the rollup was
    healthy.
    """

    @staticmethod
    def _day(days_ago: int) -> date:
        return (datetime.now(UTC) - timedelta(days=days_ago)).date()

    async def _seed_source_day(self, db_session_factory, tenant: str, day: date) -> None:
        async with db_session_factory() as session:
            session.add(
                CostEvent(
                    tenant_id=tenant,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="claude",
                    input_tokens=10,
                    output_tokens=5,
                    cost_usd=Decimal("0.01"),
                    created_at=datetime(day.year, day.month, day.day, 12, tzinfo=UTC),
                )
            )
            await session.commit()

    @pytest.mark.unit
    async def test_a_source_day_with_no_daily_row_counts_as_unrolled(self, db_session_factory):
        from services.rollup_engine import unrolled_source_days

        await self._seed_source_day(db_session_factory, T("unrolled-cost"), self._day(2))

        counts = await unrolled_source_days(db_session_factory, self._day(30))

        assert counts["cost_daily"] == 1
        assert counts["metering_daily"] == 0

    @pytest.mark.unit
    async def test_a_rolled_source_day_counts_as_zero(self, db_session_factory):
        from services.rollup_engine import rollup_daily, unrolled_source_days

        day = self._day(2)
        await self._seed_source_day(db_session_factory, T("unrolled-rolled"), day)
        await rollup_daily(db_session_factory, day)

        counts = await unrolled_source_days(db_session_factory, self._day(30))

        assert counts == {"cost_daily": 0, "metering_daily": 0}

    @pytest.mark.unit
    async def test_an_empty_source_day_is_not_unrolled(self, db_session_factory):
        """THE CASE THAT MOTIVATED THE GAUGE. An idle day has no source rows, so
        there is nothing to roll; the lag/gap gauges cannot say that, this one
        can."""
        from services.rollup_engine import unrolled_source_days

        counts = await unrolled_source_days(db_session_factory, self._day(30))

        assert counts == {"cost_daily": 0, "metering_daily": 0}

    @pytest.mark.unit
    async def test_the_gauge_carries_the_count(self, db_session_factory):
        """The alert gate reads the GAUGE, not the return value — a return-only
        test would pass while the rule stayed blind."""
        import observability.metrics as om
        from services.rollup_engine import unrolled_source_days

        await self._seed_source_day(db_session_factory, T("unrolled-gauge"), self._day(2))

        counts = await unrolled_source_days(db_session_factory, self._day(30))

        assert (
            om.rollup_unrolled_source_days.labels(table="cost_daily")._value.get()
            == counts["cost_daily"]
            == 1
        )
        assert om.rollup_unrolled_source_days.labels(table="metering_daily")._value.get() == 0
