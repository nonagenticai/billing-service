"""The election keepalive, and the demotion hook that stops the gauges lying.

WHY THIS FILE EXISTS (measured, round 8)
----------------------------------------
``AsyncLeaderElection`` used to ping its held session ONLY from ``is_leader()`` —
once per loop tick. The reaper is Postgres ``idle_session_timeout`` = **600000 ms**
on the worker4 primary (``pg_settings.source = 'configuration file'``), NOT
pgbouncer-session's ``server_idle_timeout = 240`` (a session-mode backend held by
a live client is never idle *in the pool*; a rollup election session was measured
surviving 3h17m on 300s pings). So every loop whose tick exceeds 600s lost its
session EVERY cycle:

    invoice close                 3600s
    licence↔budget reconcile       900s   <- caught live: acquired 10:53:28Z,
                                             leader_election_error 11:08:28Z
                                             (exactly 900s), peer acquired
                                             11:03:31Z

Mutual exclusion was never broken (4 advisory rows in ``billing``, one pid each).
What broke was OBSERVABILITY: the ``*_is_leader`` gauges are written once per tick,
so a demoted replica kept publishing 1 for up to a whole interval while its
successor also published 1 — ``sum()`` read 2 and split-brain detection saw a
phantom.

⚠️ These tests are deliberately DB-FREE. The point is that the keepalive fires on
its OWN schedule, so the honest test drives a fake connection at a 10ms interval
and counts pings — NOT a 600s sleep, and not a real Postgres whose
``idle_session_timeout`` is 600s anyway and could never be exercised in CI. The
real-Postgres properties (advisory-lock mutual exclusion, invalidate-releases-the-
lock) are covered in ``tests/test_leader_election.py`` and are unchanged.
"""

import asyncio
import contextlib
import uuid
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from observability import metrics as observability_metrics
from services.leader_election import (
    _KEEPALIVE_INTERVAL_S,
    _LOOP_LABELS,
    WATCHER_LOCK_KEY,
    AsyncLeaderElection,
)

# Never a production key: these tests never reach a database, but a stray key
# collision would still be confusing in a log.
_TEST_DSN = "postgresql://u:p@127.0.0.1:1/billing"  # pragma: allowlist secret
_LOOP = "test_loop"
_NO_DB = cast(async_sessionmaker[AsyncSession], object())


def _fresh_lock_key() -> int:
    return int.from_bytes(uuid.uuid4().bytes[:8], byteorder="big", signed=True)


class _Result:
    """Minimal stand-in for a SQLAlchemy ``Result``."""

    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value

    def one(self):
        return self._value


class _FakePingConn:
    """A connection whose backend pid we control, counting every execute().

    ``pid`` is mutable so a test can swap the backend under a live keepalive —
    the txn-mode-pooler misconfiguration — without a database.
    """

    def __init__(self, pid: int = 4242):
        self.pid = pid
        self.pings = 0
        self.invalidated = False
        self.closed = False
        self.raise_on_ping: BaseException | None = None

    async def execute(self, *_args, **_kwargs):
        self.pings += 1
        if self.raise_on_ping is not None:
            raise self.raise_on_ping
        return _Result(self.pid)

    async def invalidate(self):
        self.invalidated = True

    async def close(self):
        self.closed = True


def _held_election(
    *,
    conn: _FakePingConn | None = None,
    interval: float = 0.01,
    on_demote=None,
) -> tuple[AsyncLeaderElection, _FakePingConn]:
    """An election already holding leadership on a fake connection.

    Reaches past the constructor exactly as ``tests/test_leader_election.py``
    already does (``a._conn``, ``a._pid``): the alternative is a real Postgres,
    which cannot exercise a 600s reaper inside a CI job.
    """
    c = conn or _FakePingConn()
    e = AsyncLeaderElection(
        _TEST_DSN,
        _fresh_lock_key(),
        loop=_LOOP,
        on_demote=on_demote,
        keepalive_interval_s=interval,
    )
    e._engine = cast(AsyncEngine, object())  # non-None ⇒ nothing tries to build a real engine
    e._conn = c
    e._held = True
    e._pid = c.pid
    return e, c


async def _until(predicate, timeout_s: float = 2.0) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.005)
    return predicate()


def _failures(loop_label: str = _LOOP) -> float:
    return observability_metrics.leader_keepalive_failures_total.labels(
        loop=loop_label
    )._value.get()


def _live_keepalive_tasks() -> list[asyncio.Task]:
    return [
        t
        for t in asyncio.all_tasks()
        if t.get_name().startswith("leader-keepalive-") and not t.done()
    ]


# ---------------------------------------------------------------------------
# The interval itself — the only assertion that encodes the MEASURED bound
# ---------------------------------------------------------------------------


class TestKeepaliveInterval:
    @pytest.mark.unit
    def test_interval_sits_under_both_measured_reapers(self):
        """⛔ The bound is measured, not chosen. Do not raise this constant.

        ``idle_session_timeout`` = 600s is what actually reaped the long-interval
        loops' sessions. ``idle_in_transaction_session_timeout`` = 300s cannot bite
        while the engine is AUTOCOMMIT, but staying under it too means an
        accidental loss of AUTOCOMMIT degrades to a slow flap instead of a session
        reaped every cycle.
        """
        assert _KEEPALIVE_INTERVAL_S < 600.0, "idle_session_timeout (600s) would reap the session"
        assert _KEEPALIVE_INTERVAL_S < 300.0, (
            "idle_in_transaction_session_timeout (300s) — keep a margin even though "
            "AUTOCOMMIT should prevent it biting"
        )

    @pytest.mark.unit
    def test_one_dropped_ping_does_not_cost_the_session(self):
        """A single lost ping must not be fatal: ≥2 pings inside the 600s window."""
        assert 600.0 / _KEEPALIVE_INTERVAL_S >= 2.0

    @pytest.mark.unit
    def test_the_longest_loop_interval_is_covered_many_times_over(self):
        """invoice close ticks every 3600s — the whole reason this exists."""
        assert 3600.0 / _KEEPALIVE_INTERVAL_S >= 2.0

    @pytest.mark.unit
    def test_every_production_lock_key_has_its_own_metric_label(self):
        """A metric that labelled all six loops the same would be unreadable.

        The per-loop breakdown is what identifies WHICH interval outran the reaper,
        so an unlabelled (or uniformly-labelled) counter would have left the
        original defect just as invisible as no counter at all.
        """
        assert len(set(_LOOP_LABELS.values())) == len(_LOOP_LABELS) == 6
        assert _LOOP_LABELS[WATCHER_LOCK_KEY] == "budget_watcher"


# ---------------------------------------------------------------------------
# The keepalive fires on its OWN schedule
# ---------------------------------------------------------------------------


class TestKeepaliveRunsIndependentlyOfTheTick:
    @pytest.mark.unit
    async def test_session_is_pinged_with_no_is_leader_call_at_all(self):
        """THE regression test. Not one ``is_leader()`` call in this test.

        On the pre-fix code the held session is pinged only from ``is_leader()``,
        so a loop that ticks once per 3600s pings once per 3600s and the session is
        reaped at 600s. Here the tick never happens and the session must STILL be
        pinged repeatedly — that is the whole property.
        """
        e, conn = _held_election(interval=0.01)
        try:
            e._ensure_keepalive()
            assert await _until(lambda: conn.pings >= 3), (
                f"keepalive did not ping on its own schedule (pings={conn.pings}); "
                "leadership is only as durable as the loop's tick interval"
            )
            pings_before_release = conn.pings
            assert e._held is True, "leadership must survive N keepalive rounds"
            assert e._pid == conn.pid
        finally:
            await e.release()
        # Sanity: the unlock in release() is the only extra execute().
        assert conn.pings == pings_before_release + 1

    @pytest.mark.unit
    async def test_the_ping_is_pg_backend_pid_and_the_pid_is_compared(self):
        """⛔ Never weaken the ping to ``SELECT 1``.

        ``SELECT 1`` proves *a* session answered, not that we still hold the lock.
        A keepalive that kept the session warm while a txn-mode pooler had already
        moved the lock would be strictly WORSE than the bug being fixed: a loud
        self-correcting flap would become a permanent silent split-brain.
        """
        seen: list[str] = []

        class _Recording(_FakePingConn):
            async def execute(self, statement=None, *_args, **_kwargs):
                seen.append(str(statement))
                return await super().execute(statement, *_args, **_kwargs)

        e, conn = _held_election(conn=_Recording(), interval=0.01)
        try:
            e._ensure_keepalive()
            assert await _until(lambda: conn.pings >= 1)
        finally:
            await e.release()
        assert any("pg_backend_pid()" in s for s in seen), seen
        assert not any(s.strip() in ("SELECT 1", "select 1") for s in seen), seen


# ---------------------------------------------------------------------------
# A keepalive that loses the session must FAIL CLOSED
# ---------------------------------------------------------------------------


class TestKeepaliveFailsClosed:
    @pytest.mark.unit
    async def test_a_pid_swap_during_keepalive_drops_leadership(self):
        """The backend moved under us ⇒ the session-level lock is not ours.

        MUTATION-PROOF: with the pid comparison removed from the keepalive the
        swap is invisible, ``_held`` stays True and every assertion below fails.
        """
        demotions: list[int] = []
        e, conn = _held_election(interval=0.01, on_demote=lambda: demotions.append(1))
        try:
            e._ensure_keepalive()
            assert await _until(lambda: conn.pings >= 1)
            assert e._held is True and demotions == [], "non-vacuity: healthy first"

            before = _failures()
            conn.pid = conn.pid + 100_000  # txn-mode pooler rebind

            assert await _until(lambda: e._held is False), (
                "a swapped backend during keepalive must drop leadership"
            )
            assert demotions == [1], "the demotion hook must fire exactly once"
            assert _failures() == before + 1
            # invalidate(), not close(): close() returns the session — and the
            # advisory lock — to the QueuePool ALIVE, and then no replica can
            # acquire it.
            assert conn.invalidated is True, "_reset_conn must hard-close the session"
            assert e._pid is None
            # And the next tick reports the demotion rather than papering over it.
            assert await e.is_leader() is False
        finally:
            await e.release()

    @pytest.mark.unit
    async def test_a_connection_error_during_keepalive_drops_leadership(self):
        demotions: list[int] = []
        conn = _FakePingConn()
        e, _ = _held_election(conn=conn, interval=0.01, on_demote=lambda: demotions.append(1))
        try:
            e._ensure_keepalive()
            assert await _until(lambda: conn.pings >= 1)
            assert e._held is True, "non-vacuity: healthy first"

            before = _failures()
            conn.raise_on_ping = OSError("transient connection blip")

            assert await _until(lambda: e._held is False)
            assert demotions == [1]
            assert _failures() == before + 1
            assert conn.invalidated is True
            assert await e.is_leader() is False
        finally:
            await e.release()

    @pytest.mark.unit
    async def test_the_demotion_is_reported_for_exactly_one_tick(self):
        """Fails closed for ONE tick, then re-races — not forever.

        A permanent False would park the loop; the loops' not-leader path is the
        SHORT ``leader_retry_interval_seconds`` (~15s), so one reported demotion
        costs 15s and buys a gauge that tells the truth.
        """
        e, conn = _held_election(interval=0.01)
        try:
            e._ensure_keepalive()
            assert await _until(lambda: conn.pings >= 1)
            conn.pid += 100_000
            assert await _until(lambda: e._held is False)

            assert e._keepalive_failed is True
            assert await e.is_leader() is False  # consumes the flag
            assert e._keepalive_failed is False, "the flag must not latch False forever"
        finally:
            await e.release()

    @pytest.mark.unit
    async def test_a_raising_demotion_hook_does_not_break_the_election(self):
        """A broken gauge write must never cost leadership correctness."""

        def _angry_hook():
            raise RuntimeError("prometheus exploded")

        e, conn = _held_election(interval=0.01, on_demote=_angry_hook)
        try:
            e._ensure_keepalive()
            assert await _until(lambda: conn.pings >= 1)
            conn.pid += 100_000
            assert await _until(lambda: e._held is False), (
                "a raising hook must not stop the demotion itself"
            )
        finally:
            await e.release()  # must not raise either

    @pytest.mark.unit
    async def test_an_unexpected_exception_in_the_task_is_counted_not_swallowed(self):
        """NOTHING awaits the keepalive task — a silent death would restore the bug.

        A task that dies quietly means the service reverts to pinging once per tick
        with no signal at all. Fail closed AND count it.
        """
        e, conn = _held_election(interval=0.01)
        try:
            # A non-conn-layer exception: not in the (SQLAlchemyError, psycopg.Error,
            # OSError) tuple, so it escapes _ping_locked's own arms.
            conn.raise_on_ping = ValueError("something nobody predicted")
            before = _failures()
            e._ensure_keepalive()
            assert await _until(lambda: e._held is False), (
                "an unexpected exception must still demote, not leave a zombie leader"
            )
            assert _failures() == before + 1
            # ⛔ Demoting WITHOUT invalidating is the zombie-lock bug: we would
            # stop believing we are leader while our own session still held the
            # advisory lock, so no replica could ever acquire it.
            assert await _until(lambda: conn.invalidated is True), (
                "the unexpected-exception arm must also hard-close the session"
            )
        finally:
            await e.release()


# ---------------------------------------------------------------------------
# Shutdown: cancelled cleanly, and never one task per is_leader() call
# ---------------------------------------------------------------------------


class TestKeepaliveLifecycle:
    @pytest.mark.unit
    async def test_release_cancels_the_keepalive_with_no_leaked_task(self):
        e, _ = _held_election(interval=0.01)
        e._ensure_keepalive()
        task = e._keepalive_task
        assert task is not None
        await asyncio.sleep(0.03)

        await e.release()

        assert e._keepalive_task is None
        assert task.done(), "release() must not leave the keepalive task running"
        assert _live_keepalive_tasks() == [], "leaked keepalive task after shutdown"

    @pytest.mark.unit
    async def test_repeated_is_leader_calls_do_not_leak_a_task_each(self):
        """One live task per election, however often the loop ticks."""
        # A long interval so the task only ever sleeps: we are counting tasks, not
        # pings.
        e, conn = _held_election(interval=30.0)
        try:
            for _ in range(5):
                assert await e.is_leader() is True
            named = [
                t for t in _live_keepalive_tasks() if t.get_name() == f"leader-keepalive-{_LOOP}"
            ]
            assert len(named) == 1, f"is_leader() leaks a task per call: {len(named)} live"
            assert e._keepalive_task is named[0]
            # The 5 ticks did their own ownership re-verification as well.
            assert conn.pings == 5
        finally:
            await e.release()

    @pytest.mark.unit
    async def test_a_dead_keepalive_task_is_replaced_on_the_next_tick(self):
        """Self-healing: a task that ended must not leave the session unpinged."""
        e, _ = _held_election(interval=30.0)
        try:
            assert await e.is_leader() is True
            first = e._keepalive_task
            assert first is not None
            first.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await first

            assert await e.is_leader() is True
            assert e._keepalive_task is not first, "a dead keepalive must be replaced"
            assert e._keepalive_task is not None
            assert not e._keepalive_task.done()
        finally:
            await e.release()

    @pytest.mark.unit
    async def test_release_is_idempotent_with_a_keepalive_running(self):
        e, _ = _held_election(interval=0.01)
        e._ensure_keepalive()
        await asyncio.sleep(0.03)
        await e.release()
        await e.release()  # must not raise
        assert e._keepalive_task is None

    @pytest.mark.unit
    async def test_release_does_not_report_a_phantom_demotion(self):
        """A deliberate release is not a keepalive failure."""
        e, _ = _held_election(interval=0.01)
        e._ensure_keepalive()
        await asyncio.sleep(0.03)
        await e.release()
        assert e._keepalive_failed is False


# ---------------------------------------------------------------------------
# The lying gauge — budget_watcher
# ---------------------------------------------------------------------------


class _NeverLeader:
    def __init__(self, *_args, **_kwargs):
        pass

    async def is_leader(self) -> bool:
        return False

    async def release(self) -> None:
        return None


class _AlwaysLeader(_NeverLeader):
    async def is_leader(self) -> bool:
        return True


class _RaisingLeader(_NeverLeader):
    async def is_leader(self) -> bool:
        raise RuntimeError("advisory lock connection died")


@pytest.mark.timeout(120)
class TestBudgetWatcherIsLeaderGauge:
    """``billing_budget_watcher_is_leader`` did not exist in code AT ALL.

    The watcher was the one leader-elected loop whose leadership was entirely
    unobservable: no gauge, so neither ``sum()==0`` (nobody watching budgets) nor
    ``sum()>1`` (split brain) could ever fire for it.
    """

    async def _one_tick(self, election_cls, test_settings, monkeypatch) -> float:
        from services import budget_watcher as bw

        test_settings.budget_watcher_interval_seconds = 0.05
        test_settings.leader_retry_interval_seconds = 0.05
        # Neither 0 nor 1: a loop that never writes the gauge must FAIL here rather
        # than pass on a value some other test left behind.
        observability_metrics.budget_watcher_is_leader.set(-1)

        monkeypatch.setattr("services.leader_election.AsyncLeaderElection", election_cls)
        monkeypatch.setattr(bw, "check_and_fire_overages", AsyncMock(return_value=0))

        stop_event = asyncio.Event()
        task = asyncio.create_task(bw.run_watcher_loop(test_settings, _NO_DB, stop_event))
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 10.0
        while loop.time() < deadline:
            if observability_metrics.budget_watcher_is_leader._value.get() in (0.0, 1.0):
                break
            await asyncio.sleep(0.02)
        # ⚠️ Read BEFORE stopping: the loop's finally sets the gauge to 0, which
        # would make the leader case indistinguishable from the follower case.
        value = observability_metrics.budget_watcher_is_leader._value.get()
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return value

    @pytest.mark.unit
    async def test_the_leader_publishes_one(self, test_settings, monkeypatch):
        assert await self._one_tick(_AlwaysLeader, test_settings, monkeypatch) == 1.0

    @pytest.mark.unit
    async def test_a_follower_publishes_zero_rather_than_staying_silent(
        self, test_settings, monkeypatch
    ):
        """A follower that never writes is indistinguishable from a dead pod."""
        assert await self._one_tick(_NeverLeader, test_settings, monkeypatch) == 0.0

    @pytest.mark.unit
    async def test_a_replica_that_cannot_tell_publishes_zero_not_one(
        self, test_settings, monkeypatch
    ):
        """is_leader() raising must not leave the previous tick's 1 standing."""
        assert await self._one_tick(_RaisingLeader, test_settings, monkeypatch) == 0.0

    @pytest.mark.unit
    async def test_shutdown_leaves_a_zero_not_a_frozen_one(self, test_settings, monkeypatch):
        """prometheus_client keeps exporting the last value written forever."""
        from services import budget_watcher as bw

        test_settings.budget_watcher_interval_seconds = 0.05
        test_settings.leader_retry_interval_seconds = 0.05
        observability_metrics.budget_watcher_is_leader.set(-1)
        monkeypatch.setattr("services.leader_election.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr(bw, "check_and_fire_overages", AsyncMock(return_value=0))

        stop_event = asyncio.Event()
        task = asyncio.create_task(bw.run_watcher_loop(test_settings, _NO_DB, stop_event))
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 10.0
        while loop.time() < deadline:
            if observability_metrics.budget_watcher_is_leader._value.get() == 1.0:
                break
            await asyncio.sleep(0.02)
        assert observability_metrics.budget_watcher_is_leader._value.get() == 1.0
        stop_event.set()
        await asyncio.wait_for(task, timeout=5.0)  # exits on its own
        assert observability_metrics.budget_watcher_is_leader._value.get() == 0.0


class TestBudgetWatcherDemotionHook:
    @pytest.mark.unit
    async def test_the_loop_wires_on_demote_to_its_own_gauge(self, test_settings, monkeypatch):
        """THE wiring test. ⛔ Replicate this exact shape in the other three loops.

        ``leader_election`` cannot publish a demotion itself — it does not own the
        gauges. Without the hook the gauge is rewritten only once per tick, so a
        demoted replica publishes a stale 1 for up to a full interval (3600s for
        invoice close) while its successor publishes 1 too.
        """
        from services import budget_watcher as bw

        captured: dict = {}

        class _Capturing(_NeverLeader):
            def __init__(self, *args, **kwargs):
                captured["args"] = args
                captured["kwargs"] = kwargs

        test_settings.budget_watcher_interval_seconds = 0.05
        test_settings.leader_retry_interval_seconds = 0.05
        monkeypatch.setattr("services.leader_election.AsyncLeaderElection", _Capturing)
        monkeypatch.setattr(bw, "check_and_fire_overages", AsyncMock(return_value=0))

        stop_event = asyncio.Event()
        task = asyncio.create_task(bw.run_watcher_loop(test_settings, _NO_DB, stop_event))
        assert await _until(lambda: "kwargs" in captured, timeout_s=5.0)
        stop_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        hook = captured["kwargs"].get("on_demote")
        assert hook is not None, (
            "run_watcher_loop must pass on_demote= so a demotion drops the gauge "
            "immediately instead of a full interval later"
        )
        assert captured["kwargs"].get("loop") == "budget_watcher", (
            "pass loop= so billing_leader_keepalive_failures_total is labelled"
        )

        observability_metrics.budget_watcher_is_leader.set(1)
        hook()
        assert observability_metrics.budget_watcher_is_leader._value.get() == 0.0

    @pytest.mark.unit
    async def test_a_real_demotion_zeroes_the_real_gauge_end_to_end(self):
        """Wiring + mechanism together: no monkeypatched election anywhere here."""
        from services.budget_watcher import _clear_leader_gauge

        e, conn = _held_election(interval=0.01, on_demote=_clear_leader_gauge)
        try:
            observability_metrics.budget_watcher_is_leader.set(1)
            e._ensure_keepalive()
            assert await _until(lambda: conn.pings >= 1)
            assert observability_metrics.budget_watcher_is_leader._value.get() == 1.0, (
                "non-vacuity: the gauge must really be 1 before the demotion"
            )

            conn.pid += 100_000
            assert await _until(lambda: e._held is False)
            assert observability_metrics.budget_watcher_is_leader._value.get() == 0.0, (
                "a demotion must drop the gauge at the moment it happens, not one tick later"
            )
        finally:
            await e.release()
            observability_metrics.budget_watcher_is_leader.set(0)

    @pytest.mark.unit
    async def test_release_also_drops_the_gauge(self):
        """A handover release (invoice_closer._cycle_failed) must not leave a 1."""
        from services.budget_watcher import _clear_leader_gauge

        e, _ = _held_election(interval=30.0, on_demote=_clear_leader_gauge)
        observability_metrics.budget_watcher_is_leader.set(1)
        await e.release()
        assert observability_metrics.budget_watcher_is_leader._value.get() == 0.0
