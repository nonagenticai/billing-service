"""Tests for the aggregator orchestration loops + cross-cutting infra paths.

Module-specific tests live in:
- tests/test_event_flusher.py — flush_events, parse helpers, monthly usage
- tests/test_cost_flusher.py — flush_costs, pricing/budget paths, edges
- tests/test_rollup_engine.py — hourly+daily rollups, retention cleanup,
  subject-id rollup grain

Kept here:
- run_flush_loop / run_rollup_loop (the actual orchestrators)
- TestRedisFailurePaths — exercises both event + cost paths under Redis errors
- TestSubjectIdIngest — exercises both flushers under the subject-id grain
"""

import asyncio
import contextlib
import decimal
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

import services.aggregator as aggregator
from models.db import (
    CostEvent,
    MeteringEvent,
)
from observability import metrics as observability_metrics
from services.aggregator import (
    flush_costs,
    flush_events,
    run_flush_loop,
    run_rollup_loop,
)
from tests._aggregator_helpers import _make_cost, _make_event
from tests.conftest import T


class _AlwaysLeader:
    """Stand-in for services.aggregator.AsyncLeaderElection that skips postgres.

    Unit tests for run_rollup_loop only care about the loop's branching logic;
    they should not pay the variable cost of a real psycopg connect + advisory
    lock against the CI postgres. Patching this in keeps the 5s polling
    deadlines deterministic.
    """

    def __init__(self, *_args, **_kwargs):
        pass

    async def is_leader(self) -> bool:
        return True

    async def release(self) -> None:
        return None


class TestFlushLoop:
    @pytest.mark.unit
    async def test_flush_loop_runs_and_stops(self, db_session_factory, test_settings):
        """Flush loop should process buffers and stop on cancel."""
        stop_event = asyncio.Event()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []

        # Start the loop with short interval
        test_settings.flush_interval_seconds = 0.1
        task = asyncio.create_task(
            run_flush_loop(
                test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
            )
        )

        # Let it run a couple iterations
        await asyncio.sleep(0.3)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.unit
    async def test_flush_loop_flushes_on_cancel(self, db_session_factory, test_settings):
        """On cancellation, remaining events in buffer should be flushed to DB."""
        stop_event = asyncio.Event()
        lic_id = str(uuid.uuid4())
        event_buffer: list[dict] = [
            _make_event(tenant_id=T("test-tenant"), license_id=lic_id),
        ]
        cost_buffer: list[dict] = [
            _make_cost(
                tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()), provider="deepseek"
            ),
        ]

        test_settings.flush_interval_seconds = 10.0  # long interval so it won't auto-flush
        task = asyncio.create_task(
            run_flush_loop(
                test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
            )
        )

        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        # Buffers should be drained
        assert len(event_buffer) == 0
        assert len(cost_buffer) == 0

        # Verify data actually reached the database
        async with db_session_factory() as session:
            me_count = (
                await session.execute(select(func.count()).select_from(MeteringEvent))
            ).scalar()
            assert me_count == 1, "MeteringEvent should have been flushed to DB on cancel"

            ce_count = (await session.execute(select(func.count()).select_from(CostEvent))).scalar()
            assert ce_count == 1, "CostEvent should have been flushed to DB on cancel"


class TestFlushLoopExtended:
    @pytest.mark.unit
    async def test_flush_loop_batch_size_trigger(self, db_session_factory, test_settings):
        """Filling buffer to batch_size should trigger immediate flush."""
        stop_event = asyncio.Event()
        lic_id = str(uuid.uuid4())
        app_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())

        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []

        test_settings.flush_interval_seconds = 60.0  # very long
        test_settings.batch_size = 3  # low threshold

        task = asyncio.create_task(
            run_flush_loop(
                test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
            )
        )

        # Give the loop time to start
        await asyncio.sleep(0.15)

        # Fill event buffer to batch_size
        for _ in range(3):
            event_buffer.append(
                _make_event(
                    tenant_id=T("test-tenant"), license_id=lic_id, app_id=app_id, plan_id=plan_id
                )
            )

        # Wait for the flush to happen (poll; fixed sleep is flaky under load)
        for _ in range(50):
            await asyncio.sleep(0.1)
            if len(event_buffer) == 0:
                break

        # Buffer should have been drained
        assert len(event_buffer) == 0

        # Verify DB has the events (poll to avoid read-before-commit race)
        me_count = 0
        for _ in range(50):
            async with db_session_factory() as session:
                me_count = (
                    await session.execute(select(func.count()).select_from(MeteringEvent))
                ).scalar()
            if me_count == 3:
                break
            await asyncio.sleep(0.1)
        assert me_count == 3

        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.unit
    async def test_flush_loop_cost_batch_size_trigger(self, db_session_factory, test_settings):
        """Filling cost_buffer to batch_size should trigger immediate flush."""
        stop_event = asyncio.Event()

        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []

        test_settings.flush_interval_seconds = 60.0
        test_settings.batch_size = 2

        task = asyncio.create_task(
            run_flush_loop(
                test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
            )
        )

        await asyncio.sleep(0.15)

        # Fill cost buffer
        for _ in range(2):
            cost_buffer.append(_make_cost(tenant_id=T("test-tenant")))

        # Poll for flush completion (loop wakes up ~every 0.1s on buffer
        # fill). Under CI/parallel-test load a fixed 0.5s sleep is flaky.
        for _ in range(50):
            await asyncio.sleep(0.1)
            if len(cost_buffer) == 0:
                break

        assert len(cost_buffer) == 0

        # The flush task commits in its own session; give the commit a beat
        # to land and then poll the row count to avoid a read-before-commit
        # race under load.
        ce_count = 0
        for _ in range(50):
            async with db_session_factory() as session:
                ce_count = (
                    await session.execute(select(func.count()).select_from(CostEvent))
                ).scalar()
            if ce_count == 2:
                break
            await asyncio.sleep(0.1)
        assert ce_count == 2

        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.unit
    async def test_flush_loop_interval_flush(self, db_session_factory, test_settings):
        """Events added below batch_size should still flush after interval."""
        stop_event = asyncio.Event()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []

        test_settings.flush_interval_seconds = 0.2
        test_settings.batch_size = 1000  # won't hit batch trigger

        task = asyncio.create_task(
            run_flush_loop(
                test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
            )
        )

        # Add events below batch size
        event_buffer.append(_make_event(tenant_id=T("test-tenant")))
        cost_buffer.append(_make_cost(tenant_id=T("test-tenant")))

        # Poll for drain rather than sleeping a fixed amount — CI runners
        # can take noticeably longer than local to schedule the flush task,
        # and a too-short fixed sleep both flakes the assertion AND leaves
        # an in-flight INSERT that deadlocks teardown's DROP SCHEMA.
        deadline = asyncio.get_event_loop().time() + 5.0
        # noqa justification: ASYNC110 wants asyncio.Event, but the flusher's
        # internal task drains the buffers from a separate task we don't expose
        # an Event on. Bounded poll-with-deadline is the correct test idiom here.
        while (  # noqa: ASYNC110
            event_buffer or cost_buffer
        ) and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)

        assert len(event_buffer) == 0
        assert len(cost_buffer) == 0

        # Stop the loop and AWAIT task completion before returning so any
        # final flush settles before pytest tears the schema down.
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.unit
    async def test_flush_loop_error_handling(self, db_session_factory, test_settings):
        """Flush loop should survive flush_events raising an exception."""
        stop_event = asyncio.Event()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []

        test_settings.flush_interval_seconds = 0.1

        call_count = 0

        async def _failing_flush(events, sf, redis, consumer_group=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("DB unavailable")
            # Second call succeeds normally
            events.clear()
            return 0

        with patch("services.aggregator.flush_events", side_effect=_failing_flush):
            # Add event so flush_events gets called
            event_buffer.append(_make_event(tenant_id=T("test-tenant")))

            task = asyncio.create_task(
                run_flush_loop(
                    test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
                )
            )

            # Poll until the mocked flush has been invoked at least once. Leader
            # acquisition + first psycopg connection can blow a fixed sleep budget
            # under CI load (same pattern as commit 75aea0b for the daily branch).
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if call_count >= 1:
                    break
                await asyncio.sleep(0.05)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # Should have survived the error
        assert call_count >= 1

    @pytest.mark.unit
    async def test_flush_loop_infra_error_branch(self, db_session_factory, test_settings):
        """Flush loop should log warning (not error) for SQLAlchemyError/RedisError."""
        from redis.exceptions import RedisError
        from sqlalchemy.exc import SQLAlchemyError

        stop_event = asyncio.Event()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []

        test_settings.flush_interval_seconds = 0.1

        call_count = 0

        async def _infra_failing_flush(events, sf, redis, consumer_group=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise SQLAlchemyError("connection lost")
            if call_count == 2:
                raise RedisError("timeout")
            # 3rd+ call: succeed normally
            events.clear()
            return 0

        with patch("services.aggregator.flush_events", side_effect=_infra_failing_flush):
            event_buffer.append(_make_event(tenant_id=T("test-tenant")))

            task = asyncio.create_task(
                run_flush_loop(
                    test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
                )
            )

            # Poll until the mocked flush has been invoked at least twice (first
            # call raises SQLAlchemyError, second call raises RedisError, third
            # call succeeds). Leader acquisition + first psycopg connection can
            # blow a fixed sleep budget under CI load.
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if call_count >= 2:
                    break
                await asyncio.sleep(0.05)
            stop_event.set()
            # Wait for the loop to exit cleanly via the while condition
            await asyncio.wait_for(task, timeout=2.0)

        # Loop survived both infra errors and continued
        assert call_count >= 2

    @pytest.mark.unit
    async def test_flush_loop_stop_event_exit(self, db_session_factory, test_settings):
        """Setting stop_event should cause the loop to exit via the while condition."""
        stop_event = asyncio.Event()
        event_buffer: list[dict] = []
        cost_buffer: list[dict] = []

        test_settings.flush_interval_seconds = 0.1

        task = asyncio.create_task(
            run_flush_loop(
                test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
            )
        )

        # Let it run one iteration
        await asyncio.sleep(0.15)
        stop_event.set()

        # Should exit cleanly without needing cancel
        await asyncio.wait_for(task, timeout=2.0)


# ===========================================================================
# rollup_loop
# ===========================================================================


class TestRollupLoop:
    @pytest.mark.unit
    async def test_rollup_loop_runs_and_stops(self, db_session_factory, test_settings):
        """Rollup loop should run and stop cleanly."""
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1

        task = asyncio.create_task(run_rollup_loop(test_settings, db_session_factory, stop_event))

        await asyncio.sleep(0.3)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


class TestRollupLoopExtended:
    """⚠️ ROLLUP SEAMS ARE PATCHED ON ``services.rollup_engine``, NOT on
    ``services.aggregator`` — both ``rollup_daily`` AND ``missing_daily_dates``. ``run_rollup_loop`` no longer calls
    rollup_daily itself — it calls ``rollup_engine.catchup_daily``, which calls
    ``rollup_engine.rollup_daily`` through the engine module's own namespace. The
    ``catchup_daily`` likewise resolves ``missing_daily_dates`` in the engine's
    namespace. The aggregator names survive only as re-exports in ``__all__``, so
    patching them binds a symbol nothing reads and the mock never fires. Three tests in this
    class failed loudly on that (they assert WHICH dates were rolled); eight more
    patched it purely to keep the real rollup from running and went silently
    VACUOUS — they kept passing while the unmocked rollup ran underneath. A
    no-op patch is the failure mode that does not announce itself, so if this
    ever reads ``services.aggregator.…`` again, that is the bug.
    """

    @pytest.mark.unit
    async def test_rollup_loop_daily_branch(self, db_session_factory, test_settings):
        """The daily pass rolls every date missing_daily_dates reports, then cleans up."""
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1

        today = datetime.now(UTC).date()
        yesterday = today - timedelta(days=1)

        rollup_hourly_calls = 0
        rolled_dates: list = []
        cleanup_calls = 0

        async def _mock_rollup_hourly(sf, window_start=None):
            nonlocal rollup_hourly_calls
            rollup_hourly_calls += 1
            return 0

        async def _mock_rollup_daily(sf, target_date=None):
            rolled_dates.append(target_date)
            return 0

        async def _mock_cleanup(sf, days):
            nonlocal cleanup_calls
            cleanup_calls += 1
            return 0

        async def _no_missing_hours(sf, floor):
            return []

        async def _missing_days(sf, floor):
            return [yesterday]

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.aggregator.rollup_hourly", side_effect=_mock_rollup_hourly),
            patch("services.rollup_engine.rollup_daily", side_effect=_mock_rollup_daily),
            patch("services.aggregator.cleanup_old_events", side_effect=_mock_cleanup),
            patch("services.aggregator.missing_event_hours", side_effect=_no_missing_hours),
            patch("services.rollup_engine.missing_daily_dates", side_effect=_missing_days),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )

            # Poll until every step this test ASSERTS has fired (CI timing
            # varies — leader acquisition + new psycopg connection can blow a
            # fixed sleep budget). Cap at 5s so a real bug still fails fast.
            #
            # ⚠️ `cleanup_calls` belongs in this condition. It used to break on
            # `rollup_hourly_calls and rolled_dates` alone and then immediately
            # `task.cancel()` — but the loop body calls `cleanup_old_events`
            # AFTER `rollup_daily`, so on a loaded box the cancel lands in
            # between and the final assertion fails with `assert 0 >= 1`.
            # Measured 2026-09-06: reproducible twice on a load-40 machine,
            # green in isolation. A wait condition that does not cover its own
            # assertions is a flake generator, not a timing quirk.
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if rollup_hourly_calls >= 1 and rolled_dates and cleanup_calls >= 1:
                    break
                await asyncio.sleep(0.1)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert rollup_hourly_calls >= 1
        assert yesterday in rolled_dates
        assert cleanup_calls >= 1

    @pytest.mark.unit
    async def test_rollup_loop_catches_up_multi_day_gap(self, db_session_factory, test_settings):
        """A multi-day gap self-heals: every date missing_daily_dates reports is rolled.

        Regression for the structural freeze where the loop only ever rolled
        "yesterday" and reset its watermark to today on each restart — and for the
        interior-hole case a max(bucket_date) watermark would skip. The gap-aware
        daily pass rolls exactly the reported dates and does not re-roll on later
        ticks (already done for today).
        """
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1

        today = datetime.now(UTC).date()
        expected = [today - timedelta(days=n) for n in (3, 2, 1)]

        rolled: list = []

        async def _noop_hourly(sf, window_start=None):
            return 0

        async def _record_daily(sf, target_date=None):
            rolled.append(target_date)
            return 0

        async def _noop_cleanup(sf, days):
            return 0

        async def _no_missing_hours(sf, floor):
            return []

        async def _missing_days(sf, floor):
            return list(expected)

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.aggregator.rollup_hourly", side_effect=_noop_hourly),
            patch("services.rollup_engine.rollup_daily", side_effect=_record_daily),
            patch("services.aggregator.cleanup_old_events", side_effect=_noop_cleanup),
            patch("services.aggregator.missing_event_hours", side_effect=_no_missing_hours),
            patch("services.rollup_engine.missing_daily_dates", side_effect=_missing_days),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if len(rolled) >= len(expected):
                    break
                await asyncio.sleep(0.1)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # Exactly the reported dates were rolled, in order.
        assert rolled[: len(expected)] == expected
        # The daily pass runs once per day → no re-rolling on later ticks.
        assert rolled.count(today - timedelta(days=1)) == 1

    @pytest.mark.unit
    async def test_a_failing_gap_scan_does_not_abort_the_daily_cycle(
        self, db_session_factory, test_settings
    ):
        """⛔ A MEASUREMENT MUST NOT ABORT THE ROLLUPS IT RIDES ALONG WITH.

        ``daily_rollup_date_gaps`` is report-only, but it sits in the leader's
        critical path and issues two SELECTs of its own (its gauge and its log are
        wrapped; the queries are not). Unsuppressed, one SQLAlchemyError from the
        gap scan escapes to the loop's outer handler and takes the WHOLE cycle
        with it: ``last_daily_pass`` never advances, so ``cleanup_old_events``,
        the processed-message prune, ``drop_old_cost_partitions`` and the audit
        purge are all skipped — and repeated failures hand the advisory lock back
        via ``_cycle_failed``, so retention stops estate-wide.

        Same contract, and the same test shape, as ``run_attribution_audit``'s
        suppression sixty lines above the call site.
        """
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1

        today = datetime.now(UTC).date()
        yesterday = today - timedelta(days=1)
        cleanup_calls: list = []
        scan_calls: list = []

        async def _noop_hourly(sf, window_start=None):
            return 0

        async def _noop_daily(sf, target_date=None):
            return 0

        async def _record_cleanup(sf, days):
            cleanup_calls.append(days)
            return 0

        async def _no_missing_hours(sf, floor):
            return []

        async def _missing_days(sf, floor):
            return [yesterday]

        async def _exploding_gap_scan(sf, floor):
            scan_calls.append(floor)
            raise SQLAlchemyError("gap scan blew up")

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.aggregator.rollup_hourly", side_effect=_noop_hourly),
            patch("services.rollup_engine.rollup_daily", side_effect=_noop_daily),
            patch("services.aggregator.cleanup_old_events", side_effect=_record_cleanup),
            patch("services.aggregator.missing_event_hours", side_effect=_no_missing_hours),
            patch("services.rollup_engine.missing_daily_dates", side_effect=_missing_days),
            patch("services.aggregator.daily_rollup_date_gaps", side_effect=_exploding_gap_scan),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if cleanup_calls:
                    break
                await asyncio.sleep(0.1)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # The control: the scan really did run and really did raise. Without this
        # the assertion below would pass just as well on a loop that never
        # reached the call at all.
        assert scan_calls, "the gap scan never ran — this test proves nothing"
        # And the retention work AFTER it still happened.
        assert cleanup_calls, (
            "a failing gap report aborted the daily cycle: retention, the "
            "processed-message prune, partition drops and the audit purge were "
            "all skipped"
        )

    @pytest.mark.unit
    async def test_rollup_loop_backfills_missing_hourly_then_daily(
        self, db_session_factory, test_settings
    ):
        """End-to-end: a rollup-loop outage hole in metering_hourly is refilled
        from raw events on the first leader tick, then the daily catch-up rolls it
        into metering_daily. Regression for the interior-hole case that
        rollup_hourly (previous-hour-only) cannot self-heal."""
        import uuid

        from sqlalchemy import func, select

        from models.db import MeteringDaily, MeteringEvent, MeteringHourly

        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1
        test_settings.retention_days = 5  # bound the daily catch-up + retention floor

        # Pin to 12:00 UTC (not the current hour): the rollup engine buckets the
        # daily row by the LOCAL (Europe/Amsterdam) date, while gap_day below is the
        # UTC date. At the current hour those diverge by a day whenever the run lands
        # in the 22:00–24:00 UTC window (Amsterdam already past midnight), flaking the
        # bucket_date assertion. Midday UTC is the same calendar date in both zones.
        gap_hour = datetime.now(UTC).replace(
            hour=12, minute=0, second=0, microsecond=0
        ) - timedelta(days=2)
        gap_day = gap_hour.date()
        lic = uuid.uuid4()
        app = uuid.uuid4()
        plan = uuid.uuid4()  # shared dims so the two events aggregate to one row

        async with db_session_factory() as session:
            session.add_all(
                [
                    MeteringEvent(
                        tenant_id=T("t-gap"),
                        license_id=lic,
                        app_id=app,
                        plan_id=plan,
                        method="GET",
                        path="/x",
                        service_key="workers",  # pragma: allowlist secret
                        status="allowed",
                        created_at=gap_hour + timedelta(minutes=10),
                    ),
                    MeteringEvent(
                        tenant_id=T("t-gap"),
                        license_id=lic,
                        app_id=app,
                        plan_id=plan,
                        method="GET",
                        path="/x",
                        service_key="workers",  # pragma: allowlist secret
                        status="allowed",
                        created_at=gap_hour + timedelta(minutes=20),
                    ),
                ]
            )
            await session.commit()

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            deadline = asyncio.get_event_loop().time() + 8.0
            ok = False
            while asyncio.get_event_loop().time() < deadline:
                async with db_session_factory() as session:
                    hc = (
                        await session.execute(
                            select(func.count())
                            .select_from(MeteringHourly)
                            .where(MeteringHourly.tenant_id == T("t-gap"))
                        )
                    ).scalar()
                    dc = (
                        await session.execute(
                            select(func.count())
                            .select_from(MeteringDaily)
                            .where(MeteringDaily.tenant_id == T("t-gap"))
                        )
                    ).scalar()
                if hc and dc:
                    ok = True
                    break
                await asyncio.sleep(0.2)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert ok, "hourly hole was not backfilled and rolled into daily within deadline"
        async with db_session_factory() as session:
            hrow = (
                (
                    await session.execute(
                        select(MeteringHourly).where(MeteringHourly.tenant_id == T("t-gap"))
                    )
                )
                .scalars()
                .all()
            )
            drow = (
                (
                    await session.execute(
                        select(MeteringDaily).where(MeteringDaily.tenant_id == T("t-gap"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(hrow) == 1 and hrow[0].request_count == 2
            assert len(drow) == 1
            assert drow[0].bucket_date == gap_day
            assert drow[0].request_count == 2

    @pytest.mark.unit
    async def test_rollup_loop_reconciles_present_but_stale_daily(
        self, db_session_factory, test_settings
    ):
        """A daily row first written from incomplete hourly (early "yesterday" roll)
        is reconciled by the trailing window once hourly is corrected — even though
        missing_daily_dates does NOT report it (the row is present, just stale).

        Regression for the silent daily undercount: missing_daily_dates only fills
        ABSENT dates, so without the trailing reconciliation window a present-but-stale
        daily row stays frozen at its pre-backfill total forever.
        """
        from models.db import MeteringDaily, MeteringHourly

        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1

        # yesterday is inside the default daily_reconcile_days=3 trailing window.
        day = datetime.now(UTC).date() - timedelta(days=1)
        bucket = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
        lic, sub, app, plan = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        dims = {
            "tenant_id": T("t-stale"),
            "license_id": lic,
            "subject_id": sub,
            "app_id": app,
            "plan_id": plan,
            "service_key": "workers",  # pragma: allowlist secret
        }

        async with db_session_factory() as session:
            # hourly already CORRECTED to 7 (a late hourly backfill), but daily is
            # FROZEN at the stale 2 it was first rolled from before the backfill.
            session.add(MeteringHourly(**dims, bucket_hour=bucket, request_count=7, denied_count=0))
            session.add(MeteringDaily(**dims, bucket_date=day, request_count=2, denied_count=0))
            await session.commit()

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            deadline = asyncio.get_event_loop().time() + 8.0
            reconciled = False
            while asyncio.get_event_loop().time() < deadline:
                async with db_session_factory() as session:
                    rc = (
                        await session.execute(
                            select(MeteringDaily.request_count).where(
                                MeteringDaily.tenant_id == T("t-stale"),
                                MeteringDaily.bucket_date == day,
                            )
                        )
                    ).scalar()
                if rc == 7:
                    reconciled = True
                    break
                await asyncio.sleep(0.2)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert reconciled, "stale daily row was not reconciled from corrected hourly"
        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringDaily).where(MeteringDaily.tenant_id == T("t-stale"))
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1  # UPSERT replaced the stale row, did not duplicate it
        assert rows[0].request_count == 7

    @pytest.mark.unit
    async def test_rollup_loop_trailing_window_rerolls_last_n_days(
        self, db_session_factory, test_settings
    ):
        """The daily pass rolls the UNION of the trailing reconciliation window
        (last daily_reconcile_days complete days) and missing_daily_dates, excludes
        the incomplete current day, and is idempotent across ticks (once per day)."""
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1
        test_settings.daily_reconcile_days = 3

        today = datetime.now(UTC).date()
        yesterday = today - timedelta(days=1)
        # A missing interior date OUTSIDE the trailing window proves the union.
        missing_date = today - timedelta(days=10)

        rolled: list = []

        async def _noop_hourly(sf, window_start=None):
            return 0

        async def _record_daily(sf, target_date=None):
            rolled.append(target_date)
            return 0

        async def _noop_cleanup(sf, days):
            return 0

        async def _no_missing_hours(sf, floor):
            return []

        async def _missing_days(sf, floor):
            return [missing_date]

        expected = {
            yesterday,
            today - timedelta(days=2),
            today - timedelta(days=3),
            missing_date,
        }

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.aggregator.rollup_hourly", side_effect=_noop_hourly),
            patch("services.rollup_engine.rollup_daily", side_effect=_record_daily),
            patch("services.aggregator.cleanup_old_events", side_effect=_noop_cleanup),
            patch("services.aggregator.missing_event_hours", side_effect=_no_missing_hours),
            patch("services.rollup_engine.missing_daily_dates", side_effect=_missing_days),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if set(rolled) >= expected:
                    break
                await asyncio.sleep(0.1)
            # Extra ticks elapse to prove the daily pass does not re-roll the same day.
            await asyncio.sleep(0.4)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert set(rolled) == expected
        assert today not in rolled  # incomplete current day excluded
        # Daily pass runs once per day → each date rolled exactly once despite many ticks.
        assert all(rolled.count(d) == 1 for d in expected)

    @pytest.mark.unit
    async def test_rollup_loop_error_handling(self, db_session_factory, test_settings):
        """Rollup loop should survive rollup_hourly raising an exception."""
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1

        call_count = 0

        async def _failing_rollup(sf, window_start=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("DB timeout")
            return 0

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.aggregator.rollup_hourly", side_effect=_failing_rollup),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )

            # Poll until the mocked rollup has been invoked at least once. Leader
            # acquisition + first psycopg connection can blow a fixed sleep budget
            # under CI load (same pattern as commit 75aea0b for the daily branch).
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if call_count >= 1:
                    break
                await asyncio.sleep(0.05)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert call_count >= 1

    @pytest.mark.unit
    async def test_rollup_loop_infra_error_branch(self, db_session_factory, test_settings):
        """Rollup loop should log warning (not error) for SQLAlchemyError/RedisError."""
        from sqlalchemy.exc import SQLAlchemyError

        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1

        call_count = 0

        async def _infra_failing_rollup(sf):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise SQLAlchemyError("connection reset")
            return 0

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.aggregator.rollup_hourly", side_effect=_infra_failing_rollup),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )

            # Poll until the mocked rollup has been invoked at least twice (first
            # call raises, second call returns 0). Leader acquisition + first
            # psycopg connection can blow a fixed sleep budget under CI load.
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if call_count >= 2:
                    break
                await asyncio.sleep(0.05)
            stop_event.set()
            # Wait for the loop to exit cleanly via the while condition
            await asyncio.wait_for(task, timeout=2.0)

        assert call_count >= 2

    @pytest.mark.unit
    async def test_rollup_loop_stop_event_exit(self, db_session_factory, test_settings):
        """Setting stop_event should make the loop exit cleanly."""
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.1

        task = asyncio.create_task(run_rollup_loop(test_settings, db_session_factory, stop_event))

        await asyncio.sleep(0.15)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ===========================================================================
# _parse_uuid / _parse_int / _parse_decimal
# ===========================================================================


class TestRedisFailurePaths:
    @pytest.mark.unit
    async def test_flush_events_redis_failure(self, db_session_factory, fake_redis):
        """Redis incrby failure should be swallowed -- events still saved to DB."""
        lic_id = str(uuid.uuid4())
        app_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())

        buffer = [
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                status="allowed",
            ),
        ]

        # Make Redis incrby fail
        original_incrby = fake_redis.incrby

        async def _broken_incrby(*args, **kwargs):
            raise RedisConnectionError("Redis down")

        fake_redis.incrby = _broken_incrby

        count = await flush_events(buffer, db_session_factory, fake_redis)
        assert count == 1  # DB write succeeded despite Redis failure

        # Restore for cleanup
        fake_redis.incrby = original_incrby

    @pytest.mark.unit
    async def test_flush_costs_redis_failure(self, db_session_factory, fake_redis):
        """Redis incrby failure in flush_costs should be swallowed."""
        lic_id = str(uuid.uuid4())

        buffer = [
            _make_cost(
                license_id=lic_id,
                tenant_id=T("test-tenant"),
                input_tokens="500",
                output_tokens="100",
            ),
        ]

        async def _broken_incrby(*args, **kwargs):
            raise RedisConnectionError("Redis down")

        fake_redis.incrby = _broken_incrby

        count = await flush_costs(buffer, db_session_factory, fake_redis)
        assert count == 1  # DB write succeeded

    @pytest.mark.unit
    async def test_flush_events_none_redis_no_error(self, db_session_factory):
        """Passing None as redis_conn should work without errors."""
        buffer = [_make_event(tenant_id=T("test-tenant"))]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

    @pytest.mark.unit
    async def test_flush_costs_none_redis_no_error(self, db_session_factory):
        """Passing None as redis_conn should work without errors."""
        buffer = [_make_cost(tenant_id=T("test-tenant"))]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1


# ===========================================================================
# flush_costs edge cases
# ===========================================================================


class TestSubjectIdIngest:
    """Raw event ingest carries subject_id from the Redis payload."""

    @pytest.mark.unit
    async def test_event_with_subject_id_persisted(self, db_session_factory):
        """A metering event with a subject_id must land on MeteringEvent.subject_id."""
        sub_id = str(uuid.uuid4())
        buffer = [
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=str(uuid.uuid4()),
                app_id=str(uuid.uuid4()),
                plan_id=str(uuid.uuid4()),
            )
            | {"subject_id": sub_id},
        ]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.subject_id == uuid.UUID(sub_id)

    @pytest.mark.unit
    async def test_event_without_subject_id_persists_null(self, db_session_factory):
        """No subject_id on the wire => MeteringEvent.subject_id is NULL."""
        buffer = [_make_event(tenant_id=T("test-tenant"))]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.subject_id is None

    @pytest.mark.unit
    async def test_event_empty_subject_id_persists_null(self, db_session_factory):
        """Empty-string subject_id (license-authz default for "no subject") => NULL."""
        buffer = [_make_event(tenant_id=T("test-tenant")) | {"subject_id": ""}]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.subject_id is None

    @pytest.mark.unit
    async def test_cost_event_with_subject_id_persisted(self, db_session_factory):
        """A cost event with subject_id lands on CostEvent.subject_id."""
        sub_id = str(uuid.uuid4())
        buffer = [
            _make_cost(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()))
            | {"subject_id": sub_id}
        ]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1
        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalar_one()
            assert row.subject_id == uuid.UUID(sub_id)


# ===========================================================================
# ACK-after-flush durability — guards the consumer→flusher handoff.
# ===========================================================================


class TestAckAfterFlush:
    """Verify the consumer→flusher handoff doesn't lose events on a crash.

    The invariant: messages stay in the Redis pending-entries-list (PEL)
    until the DB commit succeeds; only then does the flusher XACK them.
    Acking before commit would silently lose events when a replica restarts
    between in-memory buffer and the next flush.
    """

    @pytest.mark.unit
    async def test_flush_events_acks_after_db_commit(
        self, db_session_factory, fake_redis, test_settings
    ):
        """Events tagged with _msg_id + _stream_name are XACK'd after commit."""
        from services.consumer import ensure_consumer_groups

        await ensure_consumer_groups(fake_redis, test_settings)
        msg_id = await fake_redis.xadd(
            test_settings.events_stream,
            {"x": "1"},  # body unused — we drive flush directly
        )
        # Move the message into the PEL by reading via the group.
        await fake_redis.xreadgroup(
            groupname=test_settings.consumer_group,
            consumername=test_settings.consumer_name,
            streams={test_settings.events_stream: ">"},
            count=10,
            block=10,
        )
        pending_before = await fake_redis.xpending(
            test_settings.events_stream, test_settings.consumer_group
        )
        assert pending_before["pending"] == 1

        buffer = [
            _make_event(tenant_id=T("test-tenant"))
            | {"_msg_id": msg_id, "_stream_name": test_settings.events_stream},
        ]
        count = await flush_events(
            buffer, db_session_factory, fake_redis, test_settings.consumer_group
        )
        assert count == 1

        # Post-commit: PEL drained.
        pending_after = await fake_redis.xpending(
            test_settings.events_stream, test_settings.consumer_group
        )
        assert pending_after["pending"] == 0

    @pytest.mark.unit
    async def test_flush_events_does_not_ack_on_commit_failure(
        self, db_session_factory, fake_redis, test_settings
    ):
        """If the DB commit raises, the message must remain in the PEL.

        Otherwise a replica restart between ack and commit loses the event.
        """
        from services.consumer import ensure_consumer_groups

        await ensure_consumer_groups(fake_redis, test_settings)
        msg_id = await fake_redis.xadd(test_settings.events_stream, {"x": "1"})
        await fake_redis.xreadgroup(
            groupname=test_settings.consumer_group,
            consumername=test_settings.consumer_name,
            streams={test_settings.events_stream: ">"},
            count=10,
            block=10,
        )

        buffer = [
            _make_event(tenant_id=T("test-tenant"))
            | {"_msg_id": msg_id, "_stream_name": test_settings.events_stream},
        ]
        # Force the DB session's commit to blow up by patching the factory's
        # AsyncSession.commit. Any session-level failure inside flush_events
        # exits before the XACK runs.
        from sqlalchemy.ext.asyncio import AsyncSession

        async def _broken_commit(self):
            raise RuntimeError("simulated commit failure")

        with (
            patch.object(AsyncSession, "commit", _broken_commit),
            pytest.raises(RuntimeError, match="simulated commit failure"),
        ):
            await flush_events(
                buffer,
                db_session_factory,
                fake_redis,
                test_settings.consumer_group,
            )

        # Message stays in the PEL — next restart can XCLAIM/redeliver it.
        pending = await fake_redis.xpending(
            test_settings.events_stream, test_settings.consumer_group
        )
        assert pending["pending"] == 1

    @pytest.mark.unit
    async def test_flush_events_skips_ack_when_consumer_group_none(
        self, db_session_factory, fake_redis, test_settings
    ):
        """Direct callers (tests) pass consumer_group=None and get no XACK.

        Backward-compat for callers that don't have the Redis ack coordinates.
        """
        from services.consumer import ensure_consumer_groups

        await ensure_consumer_groups(fake_redis, test_settings)
        await fake_redis.xadd(test_settings.events_stream, {"x": "1"})
        await fake_redis.xreadgroup(
            groupname=test_settings.consumer_group,
            consumername=test_settings.consumer_name,
            streams={test_settings.events_stream: ">"},
            count=10,
            block=10,
        )

        # No _msg_id tags + consumer_group=None → flusher must not touch xack.
        buffer = [_make_event(tenant_id=T("test-tenant"))]
        count = await flush_events(buffer, db_session_factory, fake_redis, None)
        assert count == 1
        pending = await fake_redis.xpending(
            test_settings.events_stream, test_settings.consumer_group
        )
        assert pending["pending"] == 1

    @pytest.mark.unit
    async def test_flush_costs_acks_after_db_commit(
        self, db_session_factory, fake_redis, test_settings
    ):
        """Cost path parallels the event path — XACK only after the cost_events commit."""
        from services.consumer import ensure_consumer_groups

        await ensure_consumer_groups(fake_redis, test_settings)
        msg_id = await fake_redis.xadd(test_settings.costs_stream, {"x": "1"})
        await fake_redis.xreadgroup(
            groupname=test_settings.consumer_group,
            consumername=test_settings.consumer_name,
            streams={test_settings.costs_stream: ">"},
            count=10,
            block=10,
        )
        pending_before = await fake_redis.xpending(
            test_settings.costs_stream, test_settings.consumer_group
        )
        assert pending_before["pending"] == 1

        buffer = [
            _make_cost(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()))
            | {"_msg_id": msg_id, "_stream_name": test_settings.costs_stream},
        ]
        count = await flush_costs(
            buffer, db_session_factory, fake_redis, test_settings.consumer_group
        )
        assert count == 1

        pending_after = await fake_redis.xpending(
            test_settings.costs_stream, test_settings.consumer_group
        )
        assert pending_after["pending"] == 0


class TestRollupLoopTrimsCostStream:
    """FIX A1 — the leader rollup tick age-trims the cost stream when given redis."""

    @pytest.mark.unit
    async def test_rollup_loop_trims_stream_each_tick(self, db_session_factory, test_settings):
        import fakeredis.aioredis

        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        test_settings.rollup_interval_seconds = 0.05
        stop_event = asyncio.Event()
        trim_mock = AsyncMock(return_value=0)

        async def _mock_rollup_hourly(sf, window_start=None):
            return 0

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.trim_stream", trim_mock),
            patch("services.aggregator.rollup_hourly", side_effect=_mock_rollup_hourly),
            patch("services.aggregator.missing_event_hours", new=AsyncMock(return_value=[])),
            patch("services.rollup_engine.missing_daily_dates", new=AsyncMock(return_value=[])),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.rollup_engine.rollup_daily", new_callable=AsyncMock),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event, fake)
            )
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if trim_mock.await_count >= 2:
                    break
                await asyncio.sleep(0.05)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # Both ingest streams are age-trimmed each tick (cost + events).
        assert trim_mock.await_count >= 2
        trimmed_streams = {call.args[1] for call in trim_mock.await_args_list}
        assert test_settings.costs_stream in trimmed_streams
        assert test_settings.events_stream in trimmed_streams
        for call in trim_mock.await_args_list:
            assert call.args[0] is fake

    @pytest.mark.unit
    async def test_rollup_loop_no_trim_without_redis(self, db_session_factory, test_settings):
        """When redis_conn is None (e.g. redis down) the loop must not attempt a trim."""
        test_settings.rollup_interval_seconds = 0.05
        stop_event = asyncio.Event()
        trim_mock = AsyncMock(return_value=0)
        hourly_calls = 0

        async def _mock_rollup_hourly(sf, window_start=None):
            nonlocal hourly_calls
            hourly_calls += 1
            return 0

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.trim_stream", trim_mock),
            patch("services.aggregator.rollup_hourly", side_effect=_mock_rollup_hourly),
            patch("services.aggregator.missing_event_hours", new=AsyncMock(return_value=[])),
            patch("services.rollup_engine.missing_daily_dates", new=AsyncMock(return_value=[])),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.rollup_engine.rollup_daily", new_callable=AsyncMock),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            deadline = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < deadline:
                if hourly_calls >= 1:
                    break
                await asyncio.sleep(0.05)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert hourly_calls >= 1
        trim_mock.assert_not_awaited()


# ===========================================================================
# Roll-window gap — a FOLLOWER must retry leadership on the SHORT interval
# ===========================================================================


class _NeverLeader:
    """Stand-in election that always answers "you are a follower"."""

    def __init__(self, *_args, **_kwargs):
        pass

    async def is_leader(self) -> bool:
        return False

    async def release(self) -> None:
        return None


def _record_wait_timeouts(monkeypatch) -> list[float]:
    """Capture every ``timeout=`` the loop hands to ``asyncio.wait_for``.

    The wait duration itself is not a usable observable (the test arms the stop
    event to cut it short), so record the timeout ARGUMENT instead. The real
    ``wait_for`` is captured before the patch and still drives the wait, so the
    stop-event semantics under test remain the production ones.
    """
    timeouts: list[float] = []
    real_wait_for = asyncio.wait_for

    async def recording_wait_for(aw, timeout=None):  # noqa: ASYNC109 — wraps wait_for
        assert timeout is not None, "the loop must always bound its wait"
        timeouts.append(timeout)
        return await real_wait_for(aw, timeout)

    monkeypatch.setattr(asyncio, "wait_for", recording_wait_for)
    return timeouts


async def _wait_for_n_waits(timeouts: list[float], n: int) -> None:
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline:
        if len(timeouts) >= n:
            return
        await asyncio.sleep(0.01)


class TestLeaderRetryWindow:
    """Regression for the observed 5-minute leaderless window.

    2026-07-13, billing namespace: the pgbouncer-session pod was restarted, both
    billing pods logged ``leader_election_error``, the ROLLUP advisory lock erred
    at 18:47:41 and no replica reacquired until 18:52:42 — 301s (= exactly one
    ``rollup_interval_seconds``) with no rollup leader, because a follower slept
    the FULL rollup interval before re-racing for the lock.

    ``rollup_interval_seconds`` is left at the production 300.0 in these tests on
    purpose: if the loop ever waits it on a non-leader path, the test hangs out its
    5s budget and fails instead of silently passing.
    """

    @pytest.mark.unit
    async def test_follower_never_waits_the_full_rollup_interval(
        self, db_session_factory, test_settings, monkeypatch
    ):
        timeouts = _record_wait_timeouts(monkeypatch)
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 300.0
        test_settings.leader_retry_interval_seconds = 0.05

        with patch("services.aggregator.AsyncLeaderElection", _NeverLeader):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await _wait_for_n_waits(timeouts, 3)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # >=3 waits inside 5s is only possible on the short retry: at 300s the loop
        # would still be parked in its first wait.
        assert len(timeouts) >= 3, f"follower did not re-poll for leadership: {timeouts}"
        assert all(t == 0.05 for t in timeouts), timeouts
        assert 300.0 not in timeouts

    @pytest.mark.unit
    async def test_leader_waits_the_full_rollup_interval(
        self, db_session_factory, test_settings, monkeypatch
    ):
        timeouts = _record_wait_timeouts(monkeypatch)
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 300.0
        test_settings.leader_retry_interval_seconds = 0.05

        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.rollup_hourly", new_callable=AsyncMock),
            patch("services.aggregator.missing_event_hours", new=AsyncMock(return_value=[])),
            patch("services.rollup_engine.missing_daily_dates", new=AsyncMock(return_value=[])),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.rollup_engine.rollup_daily", new_callable=AsyncMock),
            patch("services.aggregator.cleanup_old_events", new_callable=AsyncMock),
            patch("services.aggregator.cleanup_old_processed_messages", new_callable=AsyncMock),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await _wait_for_n_waits(timeouts, 2)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # First wait = the pre-leadership probe delay (short); the wait AFTER the
        # leader verdict is the full rollup cadence, not the retry interval.
        assert len(timeouts) >= 2, f"leader loop never re-waited: {timeouts}"
        assert timeouts[0] == 0.05
        assert timeouts[1] == 300.0

    @pytest.mark.unit
    async def test_stop_event_cuts_the_wait_immediately(self, db_session_factory, test_settings):
        """No shutdown regression: the wait is interruptible, not a fixed sleep."""
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 300.0
        test_settings.leader_retry_interval_seconds = 15.0

        with patch("services.aggregator.AsyncLeaderElection", _NeverLeader):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await asyncio.sleep(0.05)  # let the loop park in its 15s wait
            loop = asyncio.get_event_loop()
            began = loop.time()
            stop_event.set()
            await asyncio.wait_for(task, timeout=5.0)  # no cancel() — must exit on its own
            assert loop.time() - began < 1.0


class _RecordingLeader:
    """AlwaysLeader that records whether the loop gave up leadership."""

    released = 0

    def __init__(self, *_args, **_kwargs):
        pass

    async def is_leader(self) -> bool:
        return True

    async def release(self) -> None:
        type(self).released += 1


@pytest.mark.timeout(300)
class TestLoopResilience:
    """An unenumerated exception must cost one cycle, not the loop.

    **Why the timeout marker.** ``pytest-timeout`` runs with ``func_only:
    False``, so the 60s global budget in ``pyproject.toml`` covers SETUP as
    well as the call — and ``db_engine`` TRUNCATEs every table before each
    test, which measured 12–42s on a loaded ARC runner. These are the only
    tests in the suite with a multi-second CALL phase on top of that, so they
    were the first (and only) ones to trip it: 3 failed / 1145 passed in a
    single full-suite process, all three green in isolation and at file scope.
    Fix B's only validation must not be the flakiest thing in the suite. The
    budget is raised here rather than flipping ``timeout_func_only`` globally,
    which would stop the whole suite from ever timing out on a wedged fixture.

    Both loops enumerated their catches — ``(SQLAlchemyError, RedisError)`` then
    ``(TimeoutError, OSError, KeyError, ValueError, TypeError)``. Anything
    outside those tuples (AttributeError, RuntimeError, and any ArithmeticError
    such as ``decimal.InvalidOperation``, which is NOT a ValueError) escaped the
    ``while`` and killed the task for the process lifetime.

    Nothing notices or recovers: ``main._log_task_death`` only logs — there is
    no restart — and ``main.readiness`` checks the engine and Redis, never the
    background tasks, so the pod stays Ready and Live with buffers growing
    unbounded and all billing ingestion lost. ``run_rollup_loop`` is worse: its
    ``finally: election.release()`` handed the advisory lock to the next
    replica, which hit the same poison and died too — a cascade to zero rollup
    leaders.

    This is a latent class, not a replay of a measured outage: the raises below
    are injected. ``_parsers._parse_decimal`` already guards
    ``(InvalidOperation, TypeError, ValueError)`` and the one raw division in
    ``cost_flusher`` is gated on ``authoritative > 0``, so no live trigger was
    demonstrated. The catch is correct and cheap regardless.
    """

    @staticmethod
    async def _spin_until(pred, seconds=5.0):
        deadline = asyncio.get_event_loop().time() + seconds
        while asyncio.get_event_loop().time() < deadline:
            if pred():
                return
            await asyncio.sleep(0.005)

    @staticmethod
    @contextlib.asynccontextmanager
    async def _running(coro):
        """Run a loop task and ALWAYS cancel it, including on assertion failure.

        Without the ``finally`` an assert inside the body leaves the loop task
        alive past the ``patch`` context, so it tight-loops against the real
        rollup/flush functions on the session-scoped event loop and wedges
        every later test in the file. That — not the assertion — is what turned
        one honest red into a suite-wide hang.
        """
        task = asyncio.create_task(coro)
        try:
            yield task
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=5.0)

    @pytest.mark.parametrize(
        "exc",
        [AttributeError("'NoneType' object has no attribute 'get'"), RuntimeError("boom")],
        ids=["attribute_error", "runtime_error"],
    )
    @pytest.mark.unit
    async def test_flush_loop_survives_unenumerated_exception(
        self, db_session_factory, test_settings, monkeypatch, exc
    ):
        """flush_events raising outside the enumerated tuple must not kill the loop."""
        calls = {"n": 0}

        async def _boom(*_args, **_kwargs):
            calls["n"] += 1
            raise exc

        monkeypatch.setattr("services.aggregator.flush_events", _boom)
        test_settings.flush_interval_seconds = 0.01

        stop_event = asyncio.Event()
        event_buffer: list[dict] = [
            _make_event(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()))
        ]
        task = asyncio.create_task(
            run_flush_loop(test_settings, db_session_factory, None, event_buffer, [], stop_event)
        )
        await self._spin_until(lambda: calls["n"] >= 3)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert calls["n"] >= 3, f"loop died after {calls['n']} flush(es)"

    @pytest.mark.unit
    async def test_flush_loop_survives_arithmetic_error(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """decimal.InvalidOperation is an ArithmeticError — never a ValueError.

        That is precisely why enumerating ValueError/TypeError was not enough.
        """
        calls = {"n": 0}

        async def _boom(*_args, **_kwargs):
            calls["n"] += 1
            raise decimal.InvalidOperation("[<class 'decimal.ConversionSyntax'>]")

        assert not isinstance(decimal.InvalidOperation(), ValueError)
        assert isinstance(decimal.InvalidOperation(), ArithmeticError)

        monkeypatch.setattr("services.aggregator.flush_costs", _boom)
        test_settings.flush_interval_seconds = 0.01

        stop_event = asyncio.Event()
        cost_buffer: list[dict] = [
            _make_cost(
                tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()), provider="deepseek"
            )
        ]
        task = asyncio.create_task(
            run_flush_loop(test_settings, db_session_factory, None, [], cost_buffer, stop_event)
        )
        await self._spin_until(lambda: calls["n"] >= 3)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert calls["n"] >= 3, f"loop died after {calls['n']} flush(es)"

    @pytest.mark.unit
    async def test_flush_loop_error_metrics_still_increment(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """The SLO alert reads events_flushed_total{status="error"} — keep feeding it.

        The mock must DRAIN the buffer, because the real ``flush_events`` /
        ``flush_costs`` do so as their second statement, before anything that
        can raise (``batch = list(events); events.clear()``). A mock that
        leaves the buffer populated validates a property production does not
        have: the previous version of this test passed against an except arm
        doing ``inc(len(event_buffer))`` on an ALREADY-EMPTY buffer, i.e.
        ``inc(0)``. Measured delta against the real function: 0.0 — an
        unenumerated exception swallowed with a zero counter and one log line,
        quieter than the pre-fix crash, which at least tripped
        ``_log_task_death``.

        The cost half is asserted to stay at ZERO in the same cycle: when
        ``flush_events`` raises, the cost batch was never handed to
        ``flush_costs``, is still buffered and will be retried, so counting it
        as a failed cost flush would inflate the error rate every cycle.
        """
        import observability.metrics as om

        def _err_events() -> float:
            return om.events_flushed_total.labels(status="error")._value.get()

        def _err_costs() -> float:
            return om.cost_events_flushed_total.labels(status="error")._value.get()

        async def _boom_events(events, *_args, **_kwargs):
            events.clear()  # exactly what flush_events does before it can raise
            raise AttributeError("nope")

        monkeypatch.setattr("services.aggregator.flush_events", _boom_events)
        test_settings.flush_interval_seconds = 0.01

        before_events, before_costs = _err_events(), _err_costs()
        stop_event = asyncio.Event()
        event_buffer: list[dict] = [
            _make_event(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4())) for _ in range(3)
        ]
        cost_buffer: list[dict] = [
            _make_cost(
                tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()), provider="deepseek"
            )
        ]
        task = asyncio.create_task(
            run_flush_loop(
                test_settings, db_session_factory, None, event_buffer, cost_buffer, stop_event
            )
        )
        await self._spin_until(lambda: _err_events() > before_events)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        # The in-flight batch size must be captured BEFORE the flush call, so
        # the first failing cycle contributes exactly its own batch size.
        assert _err_events() - before_events >= 3.0, "error counter increments by zero"
        assert _err_costs() == before_costs, "counted a cost batch that was never attempted"

    @pytest.mark.unit
    async def test_flush_loop_cost_error_metric_counts_the_in_flight_batch(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """Same zero-increment defect on the cost half (``cost_flusher:58``)."""
        import observability.metrics as om

        def _err_costs() -> float:
            return om.cost_events_flushed_total.labels(status="error")._value.get()

        async def _boom_costs(costs, *_args, **_kwargs):
            costs.clear()  # exactly what flush_costs does before it can raise
            raise RuntimeError("nope")

        monkeypatch.setattr("services.aggregator.flush_costs", _boom_costs)
        test_settings.flush_interval_seconds = 0.01

        before = _err_costs()
        stop_event = asyncio.Event()
        cost_buffer: list[dict] = [
            _make_cost(
                tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()), provider="deepseek"
            )
            for _ in range(4)
        ]
        task = asyncio.create_task(
            run_flush_loop(test_settings, db_session_factory, None, [], cost_buffer, stop_event)
        )
        await self._spin_until(lambda: _err_costs() > before)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert _err_costs() - before >= 4.0, "cost error counter increments by zero"

    @pytest.mark.unit
    async def test_rollup_loop_holds_leadership_through_transient_failures(
        self, db_session_factory, test_settings
    ):
        """A blip that clears must NOT cost leadership.

        Releasing on every exception is what handed poison input to the next
        replica pre-fix (and killed this loop on the way out). The
        consecutive-failure counter must therefore reset on the first cycle
        that completes.
        """
        cap = aggregator.MAX_CONSECUTIVE_ROLLUP_FAILURES
        assert cap > 1, "a cap of 1 thrashes leadership on any transient blip"

        calls = {"n": 0}

        async def _boom_then_recover(*_args, **_kwargs):
            calls["n"] += 1
            # Repeating bursts of exactly cap-1 failures, each cleared by one
            # healthy cycle: never `cap` in a row, however long this runs.
            if calls["n"] % cap != 0:
                raise AttributeError("'NoneType' object has no attribute 'strftime'")

        _RecordingLeader.released = 0
        test_settings.rollup_interval_seconds = 0.01
        test_settings.leader_retry_interval_seconds = 0.01

        stop_event = asyncio.Event()
        with (
            patch("services.aggregator.AsyncLeaderElection", _RecordingLeader),
            patch("services.aggregator.rollup_hourly", side_effect=_boom_then_recover),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.rollup_engine.rollup_daily", new_callable=AsyncMock),
            patch("services.aggregator.cleanup_old_events", new_callable=AsyncMock),
            patch(
                "services.aggregator.missing_event_hours", new_callable=AsyncMock, return_value=[]
            ),
            patch(
                "services.rollup_engine.missing_daily_dates",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            async with self._running(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            ):
                # Two full bursts: more total failures than the cap, never
                # `cap` of them in a row.
                target = cap * 2
                await self._spin_until(lambda: calls["n"] >= target, seconds=20.0)
                assert calls["n"] >= target, f"rollup loop died after {calls['n']} cycle(s)"
                assert _RecordingLeader.released == 0, (
                    "handed over leadership over a transient blip that had already cleared"
                )
                stop_event.set()

    @pytest.mark.unit
    async def test_rollup_loop_hands_over_leadership_after_consecutive_failures(
        self, db_session_factory, test_settings
    ):
        """A WEDGED leader must give the lock back — while staying alive.

        The opposite defect from the one above, and the one the previous commit
        introduced: with a permanently raising ``rollup_hourly`` the loop
        survived but held the advisory lock indefinitely
        (``task_alive=True, release()_called=0``). No rollups, no retention
        cleanup, no cost-partition creation anywhere in the fleet, every pod
        still Ready, and no replica able to take over — pre-fix,
        release-and-handover was the escape hatch for a replica-LOCAL fault.
        """
        calls = {"n": 0}

        async def _boom(*_args, **_kwargs):
            calls["n"] += 1
            raise AttributeError("'NoneType' object has no attribute 'strftime'")

        _RecordingLeader.released = 0
        test_settings.rollup_interval_seconds = 0.01
        test_settings.leader_retry_interval_seconds = 0.01

        stop_event = asyncio.Event()
        with (
            patch("services.aggregator.AsyncLeaderElection", _RecordingLeader),
            patch("services.aggregator.rollup_hourly", side_effect=_boom),
        ):
            async with self._running(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            ):
                await self._spin_until(lambda: _RecordingLeader.released >= 1)
                assert _RecordingLeader.released >= 1, (
                    f"leader held the advisory lock through {calls['n']} consecutive "
                    "failures — no replica can take over and no rollup ever runs"
                )

                # ...and it is still looping after handing over: the release
                # must not be the old `finally:` on the way out of a dead task.
                calls_at_release = calls["n"]
                await self._spin_until(lambda: calls["n"] > calls_at_release)
                assert calls["n"] > calls_at_release, "loop died when it released leadership"
                stop_event.set()

    @pytest.mark.unit
    async def test_rollup_loop_failures_are_visible_as_a_metric(
        self, db_session_factory, test_settings
    ):
        """A surviving-but-broken rollup leader must not be silent.

        ``grep observability_metrics`` inside ``run_rollup_loop`` used to
        return nothing: the broad ``except`` keeps the loop alive, so a
        permanently failing rollup produced no crash, no un-Ready pod and no
        metric — the only signal was a log line nobody alerts on.
        """
        import observability.metrics as om

        def _errs() -> float:
            return om.rollup_cycles_total.labels(status="error")._value.get()

        async def _boom(*_args, **_kwargs):
            raise decimal.DivisionByZero("division by zero")

        test_settings.rollup_interval_seconds = 0.01
        test_settings.leader_retry_interval_seconds = 0.01

        before = _errs()
        stop_event = asyncio.Event()
        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.rollup_hourly", side_effect=_boom),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await self._spin_until(lambda: _errs() > before)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert _errs() - before >= 1.0, "a failing rollup cycle emitted no metric"

    @pytest.mark.unit
    async def test_rollup_loop_counts_successful_cycles(self, db_session_factory, test_settings):
        """The error counter is only alertable against an ok denominator."""
        import observability.metrics as om

        def _oks() -> float:
            return om.rollup_cycles_total.labels(status="ok")._value.get()

        test_settings.rollup_interval_seconds = 0.01
        test_settings.leader_retry_interval_seconds = 0.01

        before = _oks()
        stop_event = asyncio.Event()
        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.rollup_hourly", new_callable=AsyncMock),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.rollup_engine.rollup_daily", new_callable=AsyncMock),
            patch("services.aggregator.cleanup_old_events", new_callable=AsyncMock),
            patch(
                "services.aggregator.missing_event_hours", new_callable=AsyncMock, return_value=[]
            ),
            patch(
                "services.rollup_engine.missing_daily_dates",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await self._spin_until(lambda: _oks() > before)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert _oks() - before >= 1.0

    @pytest.mark.unit
    async def test_rollup_loop_survives_arithmetic_error(self, db_session_factory, test_settings):
        """Same for an ArithmeticError, which no enumerated tuple covered."""
        calls = {"n": 0}

        async def _boom(*_args, **_kwargs):
            calls["n"] += 1
            raise decimal.DivisionByZero("division by zero")

        test_settings.rollup_interval_seconds = 0.01
        test_settings.leader_retry_interval_seconds = 0.01

        stop_event = asyncio.Event()
        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.rollup_hourly", side_effect=_boom),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await self._spin_until(lambda: calls["n"] >= 3)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert calls["n"] >= 3, f"rollup loop died after {calls['n']} cycle(s)"

    @pytest.mark.unit
    async def test_the_hourly_lag_is_still_measured_when_rollup_hourly_raises(
        self, db_session_factory, test_settings
    ):
        """⭐ THE INSTRUMENT MUST SURVIVE THE THING IT MEASURES.

        ``billing_metering_hourly_lag_hours`` exists to make a stalled hourly
        rollup visible. The first version of this loop called
        ``rollup_hourly(...)`` UNGUARDED immediately before the measurement, so
        the exception that makes the hourly stage fall behind was also the
        exception that skipped the measurement — and a prometheus_client gauge
        is process-lifetime state, so the series went on exporting the last
        healthy ``1`` indefinitely. Rollup starts throwing at 09:00Z; at 15:00Z
        the true lag is 6 and the series still reads 1.

        ⛔ This test reds if the ``finally`` is unwrapped back into two
        statements. It asserts the measurement runs on the RAISING path
        specifically — a green here with a bare ``await rollup_hourly(...)``
        would mean the harness, not the loop, is doing the work.
        """
        measured = {"n": 0}
        rolled = {"n": 0}

        async def _boom(*_args, **_kwargs):
            rolled["n"] += 1
            raise decimal.DivisionByZero("division by zero")

        async def _measure(*_args, **_kwargs):
            measured["n"] += 1
            return 6

        test_settings.rollup_interval_seconds = 0.01
        test_settings.leader_retry_interval_seconds = 0.01

        stop_event = asyncio.Event()
        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.rollup_hourly", side_effect=_boom),
            patch("services.aggregator.metering_hourly_lag_hours", side_effect=_measure),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await self._spin_until(lambda: measured["n"] >= 2)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert rolled["n"] >= 2, "the rollup never ran, so the test proved nothing"
        assert measured["n"] >= 2, (
            f"rollup_hourly raised {rolled['n']} time(s) and the hourly-lag measurement ran "
            f"{measured['n']} time(s). The gauge is skipped by exactly the failure it exists "
            "to expose and will hold its last healthy value indefinitely."
        )

    @pytest.mark.unit
    async def test_a_raising_rollup_hourly_still_reaches_the_loops_error_arm(
        self, db_session_factory, test_settings
    ):
        """⛔ The `finally` must not SWALLOW the rollup failure it measures around.

        A `try/finally` whose body raises still propagates — but a `try/except`
        typo, or an exception escaping the inner measurement, would convert a
        broken rollup into a silently 'ok' cycle. That would be a worse defect
        than the one the `finally` fixes, so it is asserted separately:
        ``billing_rollup_cycles_total{status="error"}`` must still advance.
        """
        import observability.metrics as om

        def _errs() -> float:
            return om.rollup_cycles_total.labels(status="error")._value.get()

        async def _boom(*_args, **_kwargs):
            raise decimal.DivisionByZero("division by zero")

        # The measurement raises too: the failure of the instrument must not be
        # able to mask, replace or duplicate the failure of the subject.
        async def _measure_boom(*_args, **_kwargs):
            raise RuntimeError("lag scan failed")

        test_settings.rollup_interval_seconds = 0.01
        test_settings.leader_retry_interval_seconds = 0.01

        before = _errs()
        stop_event = asyncio.Event()
        with (
            patch("services.aggregator.AsyncLeaderElection", _AlwaysLeader),
            patch("services.aggregator.rollup_hourly", side_effect=_boom),
            patch("services.aggregator.metering_hourly_lag_hours", side_effect=_measure_boom),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await self._spin_until(lambda: _errs() > before)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert _errs() - before >= 1.0, (
            "a raising rollup_hourly no longer reaches the loop's error arm — the "
            "measurement wrapper is eating the failure"
        )

    @pytest.mark.unit
    async def test_flush_loop_shutdown_flush_failure_is_logged_not_swallowed(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """The CancelledError arm's final flush is best-effort — not silent.

        It runs OUTSIDE the loop's error handling, and the only ``gather`` on
        these tasks uses ``return_exceptions=True``, so a raise there vanished
        completely: last batch lost, nothing logged, exit code clean.
        """
        import observability.metrics as om

        def _err_total() -> float:
            return om.events_flushed_total.labels(status="error")._value.get()

        async def _boom(events, *_args, **_kwargs):
            events.clear()  # exactly what flush_events does before it can raise
            raise AttributeError("shutdown boom")

        monkeypatch.setattr("services.aggregator.flush_events", _boom)
        test_settings.flush_interval_seconds = 10.0  # park in the inner wait

        before = _err_total()
        stop_event = asyncio.Event()
        event_buffer: list[dict] = [
            _make_event(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4())) for _ in range(2)
        ]
        task = asyncio.create_task(
            run_flush_loop(test_settings, db_session_factory, None, event_buffer, [], stop_event)
        )
        await asyncio.sleep(0.05)
        task.cancel()
        # Must surface as a clean cancel, NOT as the AttributeError leaking out.
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5.0)

        assert _err_total() - before >= 2.0, (
            "shutdown flush failure was not counted (or counted by zero — the "
            "real flush drains the buffer before it can raise)"
        )

    @pytest.mark.unit
    async def test_rollup_loop_still_releases_leadership_on_shutdown(
        self, db_session_factory, test_settings
    ):
        """Keeping the lock while looping must not leak it past shutdown."""
        _RecordingLeader.released = 0
        test_settings.rollup_interval_seconds = 0.01
        test_settings.leader_retry_interval_seconds = 0.01

        stop_event = asyncio.Event()
        with (
            patch("services.aggregator.AsyncLeaderElection", _RecordingLeader),
            patch("services.aggregator.rollup_hourly", new_callable=AsyncMock),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.rollup_engine.rollup_daily", new_callable=AsyncMock),
            patch("services.aggregator.cleanup_old_events", new_callable=AsyncMock),
            patch(
                "services.aggregator.missing_event_hours", new_callable=AsyncMock, return_value=[]
            ),
            patch(
                "services.rollup_engine.missing_daily_dates",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            task = asyncio.create_task(
                run_rollup_loop(test_settings, db_session_factory, stop_event)
            )
            await asyncio.sleep(0.1)
            stop_event.set()
            await asyncio.wait_for(task, timeout=5.0)

        assert _RecordingLeader.released >= 1


# ===========================================================================
# billing_rollup_is_leader — refreshed by EVERY replica on EVERY tick
#
# Prometheus scrapes all replicas. Before this gauge existed, nothing on the
# wire said which replica held the lease, and nothing could distinguish
# "leadership moved" from "nobody is rolling up". Measured on prod 2026-08-25:
# six handovers in five minutes (psycopg `server closed the connection
# unexpectedly`) with no series recording any of it.
#
# It is a DIAGNOSTIC. It must never be used to pick whose coherence gauges to
# believe -- a loop wedged inside the rollup's `except Exception` keeps
# answering 1 while its gauges freeze. That job belongs to the freshness
# timestamp (see tests/test_attribution_audit.py).
# ===========================================================================


#: Every DB-touching callee in the leader branch is patched below, so the loop
#: never dereferences the session factory. Passing a sentinel instead of the
#: postgres fixture keeps these three tests running anywhere -- they are about
#: branching, not about the database.
_NO_DB = object()


class _RaisingLeader:
    """Election whose is_leader() blows up rather than answering."""

    def __init__(self, *_args, **_kwargs):
        pass

    async def is_leader(self) -> bool:
        raise RuntimeError("advisory lock connection died")

    async def release(self) -> None:
        return None


@pytest.mark.timeout(120)
class TestRollupIsLeaderGauge:
    async def _run_one_tick(self, election_cls, test_settings, session_factory):
        stop_event = asyncio.Event()
        test_settings.rollup_interval_seconds = 0.05
        test_settings.leader_retry_interval_seconds = 0.05
        observability_metrics.rollup_is_leader.set(-1)  # neither 0 nor 1

        with (
            patch("services.aggregator.AsyncLeaderElection", election_cls),
            patch("services.aggregator.ensure_cost_event_partitions", new_callable=AsyncMock),
            patch("services.aggregator.drop_old_cost_partitions", new_callable=AsyncMock),
            patch("services.aggregator.rollup_hourly", new_callable=AsyncMock),
            patch("services.rollup_engine.rollup_daily", new_callable=AsyncMock),
            patch("services.aggregator.cleanup_old_events", new_callable=AsyncMock),
            patch("services.aggregator.run_attribution_audit", new_callable=AsyncMock),
            patch(
                "services.aggregator.missing_event_hours", new_callable=AsyncMock, return_value=[]
            ),
            patch(
                "services.rollup_engine.missing_daily_dates",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            task = asyncio.create_task(run_rollup_loop(test_settings, session_factory, stop_event))
            deadline = asyncio.get_event_loop().time() + 10.0
            while asyncio.get_event_loop().time() < deadline:
                if observability_metrics.rollup_is_leader._value.get() in (0.0, 1.0):
                    break
                await asyncio.sleep(0.02)
            stop_event.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        return observability_metrics.rollup_is_leader._value.get()

    @pytest.mark.unit
    async def test_the_leader_publishes_one(self, test_settings):
        value = await self._run_one_tick(_AlwaysLeader, test_settings, _NO_DB)
        assert value == 1.0

    @pytest.mark.unit
    async def test_a_follower_publishes_zero_rather_than_staying_silent(self, test_settings):
        """A follower that never writes is indistinguishable from a dead pod.

        This is the whole point of setting it on BOTH branches: `sum()` over the
        fleet can only mean "nobody is rolling up" if every replica contributes.
        """
        value = await self._run_one_tick(_NeverLeader, test_settings, _NO_DB)
        assert value == 0.0

    @pytest.mark.unit
    async def test_a_replica_that_cannot_tell_publishes_zero_not_one(self, test_settings):
        """is_leader() opens a transaction; a raise must not leave a stale 1.

        Measured on prod: six handovers in five minutes from psycopg connection
        drops. A replica that pre-set 1 and then raised would keep claiming
        leadership it does not hold, and `sum() > 1` split-brain detection would
        read a phantom.
        """
        value = await self._run_one_tick(_RaisingLeader, test_settings, _NO_DB)
        assert value == 0.0
