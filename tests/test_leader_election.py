"""Tests for services.leader_election against a real Postgres instance.

These tests MUST talk to Postgres over a connection that PINS ONE BACKEND.
Session-level advisory locks (``pg_try_advisory_lock``) cannot survive
pgbouncer transaction-mode pooling: each statement may land on a different
server backend, so the lock is released the moment its acquiring statement
commits. Production sidesteps this by building the leader DSN with
:func:`direct_postgres_dsn`, which repoints the host at
``DIRECT_POSTGRES_HOST`` — in prod the *session-mode* pgbouncer, which pins a
backend per client connection. The tests use a direct Postgres, which pins a
backend just the same, so they exercise the real production semantics rather
than passing by accident (an uncommitted implicit transaction used to pin a
pgbouncer backend, masking the issue until the connection ran in AUTOCOMMIT).

Two-instance tests use a per-test lock_key derived from a unique offset to
ensure parallel test runs don't trample each other's locks.
"""

import asyncio
import contextlib
import os
import uuid
from typing import Any
from urllib.parse import urlsplit

import psycopg
import pytest
import pytest_asyncio

from services.leader_election import (
    AsyncLeaderElection,
    direct_postgres_dsn,
    release_etl_leader,
    try_acquire_etl_leader,
)

_RAW_TEST_URL = os.getenv(
    "TEST_DATABASE_URL",
    # Same 2-stage fallback as tests/conftest.py: CI's python-service archetype
    # exports DATABASE_URL via $GITHUB_ENV; honor it before the localhost
    # default so this test module works in CI alongside the conftest fixtures.
    # NEVER put 5436 back here: that port is ``ir-r21-pg-5436``, ANOTHER
    # session's Postgres container. This repo's own test container is
    # 127.0.0.1:5502/metering_test. Keep in step with tests/conftest.py.
    os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@127.0.0.1:5502/metering_test",
    ),
)


def _resolve_direct_dsn(raw_url: str) -> str:
    """Return a libpq DSN guaranteed to be a backend-pinning (non-txn-pooled) session.

    CI now runs a per-job ephemeral ``postgres:18`` container and points
    DATABASE_URL straight at it, so the DSN is already a direct Postgres and is
    returned untouched — as it is locally (e.g. :5436).

    The rewrite branch below is a leftover guard for a txn-mode pgbouncer DSN
    (ports 6432 / the old :31259 NodePort). That shared-CI-Postgres topology
    (NodePort -> pgbouncer -> pool ci_test -> master:5433) is DECOMMISSIONED, so
    the branch is inert; it is kept only so that pointing these tests at a
    transaction pooler by accident still degrades to a direct connection rather
    than silently testing meaningless advisory locks.
    """
    libpq = raw_url.replace("postgresql+psycopg://", "postgresql://", 1).replace(
        "postgresql+asyncpg://", "postgresql://", 1
    )
    if urlsplit(libpq).port in (31259, 6432):
        direct_port = os.getenv("CI_DIRECT_POSTGRES_PORT", "5433")
        return direct_postgres_dsn(libpq, f"{urlsplit(libpq).hostname}:{direct_port}")
    return libpq


_LIBPQ_TEST_DSN = _resolve_direct_dsn(_RAW_TEST_URL)


def _fresh_lock_key() -> int:
    """Per-test 64-bit advisory lock key.

    UUID4 → first 8 bytes → signed 64-bit int (Postgres advisory keys are
    bigint). Avoids collisions across parallel test runs and against the
    production keys (ROLLUP_LOCK_KEY / ETL_LOCK_KEY).
    """
    raw = uuid.uuid4().bytes[:8]
    return int.from_bytes(raw, byteorder="big", signed=True)


async def _acquires_within(election: AsyncLeaderElection, timeout_s: float = 5.0) -> bool:
    """Poll ``is_leader()`` until it wins the lock, or give up after ``timeout_s``.

    ``invalidate()`` closes the client socket; Postgres then reaps the backend and
    releases its session-scoped advisory lock *asynchronously*. Under load that
    teardown is not instantaneous, so asserting an immediate re-acquire is flaky.

    This bounded wait keeps ALL of the test's discriminating power: with the
    ``close()``-only bug the zombie session is parked in a live QueuePool and stays
    alive for the whole test, so the lock is never released no matter how long we
    wait — this helper still returns False there.
    """
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        if await election.is_leader():
            return True
        if asyncio.get_running_loop().time() >= deadline:
            return False
        await asyncio.sleep(0.05)


class _FlakyConn:
    """Wraps a real ``AsyncConnection`` and makes ``execute()`` raise ``OSError``.

    Models a *transient* client-side blip: the Postgres backend stays alive and
    keeps holding its session-scoped advisory lock. ``invalidate()`` and
    ``close()`` delegate to the real connection so the production cleanup path
    is exercised against the real session (we are testing whether the lock is
    genuinely released, so the cleanup must not be faked).

    Needed because ``AsyncConnection.execute`` is read-only and cannot be
    monkeypatched on the instance.
    """

    def __init__(self, real: Any):
        self._real = real

    async def execute(self, *_args, **_kwargs):
        raise OSError("transient connection blip")

    async def invalidate(self, *args, **kwargs):
        return await self._real.invalidate(*args, **kwargs)

    async def close(self, *args, **kwargs):
        return await self._real.close(*args, **kwargs)


# ---------------------------------------------------------------------------
# direct_postgres_dsn — pure-string unit tests
# ---------------------------------------------------------------------------


class TestDirectPostgresDsn:
    @pytest.mark.unit
    def test_strips_psycopg_driver_and_rewrites_host(self):
        result = direct_postgres_dsn(
            "postgresql+psycopg://user:pass@pgbouncer.svc:6432/metering",
            "postgres.example.com:5432",
        )
        assert result == "postgresql://user:pass@postgres.example.com:5432/metering"

    @pytest.mark.unit
    def test_strips_asyncpg_driver(self):
        result = direct_postgres_dsn(
            "postgresql+asyncpg://user:pass@pgbouncer.svc:6432/metering",
            "postgres.example.com:5432",
        )
        assert result == "postgresql://user:pass@postgres.example.com:5432/metering"

    @pytest.mark.unit
    def test_strips_query_string(self):
        result = direct_postgres_dsn(
            "postgresql+psycopg://u:p@host:6432/metering?sslmode=require&application_name=foo",
            "postgres.example.com:5432",
        )
        assert result == "postgresql://u:p@postgres.example.com:5432/metering"

    @pytest.mark.unit
    def test_handles_libpq_input(self):
        result = direct_postgres_dsn(
            "postgresql://u:p@host:6432/metering", "postgres.example.com:5432"
        )
        assert result == "postgresql://u:p@postgres.example.com:5432/metering"

    @pytest.mark.unit
    def test_raises_when_db_path_missing(self):
        """Fail closed on a path-less DSN — never guess the database.

        This used to default to ``"metering"``. An advisory lock is scoped to a
        *database*, and these lock keys are inherited from metering-service (so
        they are NOT unique across services). Guessing a database name would
        take billing's leadership lock in someone else's database, where it
        neither mutually excludes the right processes nor is observable.
        """
        with pytest.raises(ValueError, match="missing the /<database> path"):
            direct_postgres_dsn("postgresql+psycopg://u:p@host:6432", "postgres.example.com:5432")

    @pytest.mark.unit
    def test_raises_on_empty_db_path(self):
        # Trailing slash with no database name is equally unusable.
        with pytest.raises(ValueError, match="empty /<database> path"):
            direct_postgres_dsn("postgresql+psycopg://u:p@host:6432/", "postgres.example.com:5432")

    @pytest.mark.unit
    def test_raises_on_missing_credentials(self):
        with pytest.raises(ValueError, match="missing credentials"):
            direct_postgres_dsn(
                "postgresql+psycopg://pgbouncer.svc:6432/metering", "postgres.example.com:5432"
            )

    @pytest.mark.unit
    def test_raises_on_empty_direct_host(self):
        with pytest.raises(ValueError, match="direct_host"):
            direct_postgres_dsn("postgresql+psycopg://u:p@pgbouncer:6432/metering", "")

    @pytest.mark.unit
    def test_uses_supplied_direct_host(self):
        # Different host:port should be honored — not hardcoded to mf1.
        result = direct_postgres_dsn(
            "postgresql+psycopg://u:p@pgbouncer:6432/metering",
            "postgres.master.example.com:5433",
        )
        assert result == "postgresql://u:p@postgres.master.example.com:5433/metering"


# ---------------------------------------------------------------------------
# AsyncLeaderElection — real DB
# ---------------------------------------------------------------------------


class TestAsyncLeaderElection:
    @pytest_asyncio.fixture
    async def lock_key(self) -> int:
        return _fresh_lock_key()

    @pytest.mark.unit
    async def test_single_instance_acquires(self, lock_key: int):
        e = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        try:
            assert await e.is_leader() is True
            # idempotent — second call is keepalive, still leader
            assert await e.is_leader() is True
        finally:
            await e.release()

    @pytest.mark.unit
    async def test_only_one_of_two_acquires(self, lock_key: int):
        a = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        b = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        try:
            a_first = await a.is_leader()
            b_first = await b.is_leader()
            assert (a_first, b_first) == (True, False), (
                f"Expected exactly one leader; got a={a_first} b={b_first}"
            )
            # Stable across retries — b stays follower while a holds the lock.
            assert await b.is_leader() is False
            assert await a.is_leader() is True  # a's call is a keepalive
        finally:
            await a.release()
            await b.release()

    @pytest.mark.unit
    async def test_release_lets_other_acquire(self, lock_key: int):
        a = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        b = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        try:
            assert await a.is_leader() is True
            assert await b.is_leader() is False
            await a.release()
            assert await b.is_leader() is True
        finally:
            await a.release()
            await b.release()

    @pytest.mark.unit
    async def test_release_is_idempotent(self, lock_key: int):
        e = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        await e.is_leader()
        await e.release()
        # second release is a no-op, no exception
        await e.release()

    @pytest.mark.unit
    async def test_release_before_acquire_is_safe(self, lock_key: int):
        e = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        # release without ever acquiring
        await e.release()

    @pytest.mark.unit
    async def test_keepalive_leaves_session_idle_not_in_transaction(self, lock_key: int):
        """Regression: the held-branch keepalive ping must leave the dedicated
        session ``idle``, never ``idle in transaction``.

        The rollup loop only pings every ~300s. If the connection sits in an
        implicit transaction between pings, Postgres
        ``idle_in_transaction_session_timeout`` reaps it before the next ping,
        the advisory lock auto-releases, and leadership flaps every cycle
        (degraded ~600s rollup cadence). AUTOCOMMIT keeps each ping out of a
        transaction so the session stays plain-idle and the lock is held.
        """
        e = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        try:
            assert await e.is_leader() is True  # acquire
            assert await e.is_leader() is True  # steady-state keepalive (held branch)

            # Observe the leader backend's state from an INDEPENDENT connection
            # so we don't perturb e._conn's transaction state. Locate it via the
            # advisory lock it holds: pg_try_advisory_lock(bigint) stores the
            # high 32 bits in classid, the low 32 bits in objid, objsubid=1.
            with (
                psycopg.connect(_LIBPQ_TEST_DSN, autocommit=True) as obs,
                obs.cursor() as cur,
            ):
                cur.execute(
                    "SELECT a.state FROM pg_stat_activity a "
                    "JOIN pg_locks l USING (pid) "
                    "WHERE l.locktype='advisory' AND l.objsubid=1 "
                    "AND a.datname=current_database() "
                    "AND l.classid=%s AND l.objid=%s",
                    ((lock_key >> 32) & 0xFFFFFFFF, lock_key & 0xFFFFFFFF),
                )
                row = cur.fetchone()
            assert row is not None, "leader's advisory lock not visible in pg_locks"
            assert row[0] == "idle", (
                f"leader session is {row[0]!r}; AUTOCOMMIT must keep it 'idle', "
                "else idle_in_transaction_session_timeout reaps the lock between ticks"
            )
        finally:
            await e.release()

    @pytest.mark.unit
    async def test_kill_session_externally_lets_other_acquire(self, lock_key: int):
        """If the leader's session is killed (e.g. pg restart, pgbouncer cycle),
        the lock is auto-released and the other instance acquires on next try.
        """
        a = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        b = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        try:
            assert await a.is_leader() is True
            assert await b.is_leader() is False

            # Find a's backend pid and pg_terminate_backend it externally.
            with (
                psycopg.connect(_LIBPQ_TEST_DSN, autocommit=True) as kill_conn,
                kill_conn.cursor() as cur,
            ):
                cur.execute(
                    "SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid=%s",
                    (lock_key & 0xFFFFFFFF,),  # advisory locks split key
                )
                rows = cur.fetchall()
                # advisory lock pid lookup — best-effort. Fallback: kill all
                # idle connections on the test DB except us.
                if rows:
                    for (pid,) in rows:
                        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                else:
                    # Shouldn't happen, but if pg_locks doesn't surface the
                    # advisory key (older PG), fall back to terminating all
                    # other backends except us.
                    cur.execute(
                        "SELECT pid FROM pg_stat_activity "
                        "WHERE datname=current_database() "
                        "AND pid <> pg_backend_pid()"
                    )
                    for (pid,) in cur.fetchall():
                        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))

            # a's next is_leader detects the dead session and drops leadership.
            # b can then acquire.
            await asyncio.sleep(0.1)  # let postgres clean up
            assert await a.is_leader() is False
            assert await b.is_leader() is True
        finally:
            await a.release()
            await b.release()

    @pytest.mark.unit
    async def test_transient_error_frees_lock_for_other_instance(self, lock_key: int):
        """REGRESSION: a transient error must actually RELEASE the advisory lock.

        This is the zombie-lock bug. ``is_leader()`` catches any
        ``SQLAlchemyError``/``psycopg.Error``/``OSError``, clears ``_held`` and
        calls ``_reset_conn()``. ``_reset_conn`` used to only ``close()`` the
        connection — but the engine is a QueuePool (``pool_size=1``), so
        ``close()`` merely returns the DBAPI connection to the pool with the
        *server session still alive*, and a session-scoped
        ``pg_try_advisory_lock`` dies only with its session.

        So on a transient blip that does NOT kill the backend, the replica gave
        up leadership while still holding the lock. Neither replica could then
        acquire it, and billing's hourly rollup + budget-overage webhooks
        silently stopped fleet-wide until a pod restarted. (billing runs at
        ``replicas: 2``.)

        The fix is ``invalidate()`` before ``close()``, which hard-closes the
        backend and lets Postgres auto-release the lock.

        We assert BEHAVIOUR — that a second election can acquire — not that any
        particular method was called. On the old ``close()``-only code the final
        assertion fails: ``b`` cannot acquire, because the zombie pooled session
        still holds the lock.
        """
        a = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        b = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        try:
            assert await a.is_leader() is True, "a should acquire the free lock"
            # Non-vacuity guard: prove the lock is genuinely held right now, so
            # a later successful acquire by b means something.
            assert await b.is_leader() is False, "b must not acquire while a holds it"

            # Force a's next is_leader() down its except path with a TRANSIENT
            # error. OSError is in the caught tuple, and — crucially — raising it
            # client-side leaves the Postgres backend perfectly alive and still
            # holding the lock. This is the blip the old code mishandled.
            #
            # AsyncConnection.execute is read-only, so we wrap rather than patch.
            # invalidate()/close() delegate to the REAL connection, so the real
            # cleanup path runs against the real Postgres session — only execute()
            # is poisoned.
            a._conn = _FlakyConn(a._conn)

            assert await a.is_leader() is False, "a should drop leadership on the error"

            # THE ASSERTION. a no longer believes it is leader; the lock must
            # therefore be genuinely free. On close()-only code it is NOT free —
            # a's pooled session is a zombie still holding it — and b stays a
            # follower forever, so rollups/webhooks stop fleet-wide.
            assert await _acquires_within(b), (
                "lock still held by a's zombie pooled session after a transient "
                "error — _reset_conn() must invalidate(), not just close()"
            )
        finally:
            await a.release()
            await b.release()

    @pytest.mark.unit
    async def test_backend_pid_swap_drops_leadership(self, lock_key: int):
        """A swapped backend pid means the session-level lock is not ours.

        Simulates the txn-mode-pooler misconfiguration (``DIRECT_POSTGRES_HOST``
        pointed at ``pgbouncer:6432`` instead of the session-mode pooler): the
        pooler rebinds a different server backend, so the lock we took is gone
        even though a ``SELECT 1`` keepalive would happily succeed. We pin the
        acquiring pid and compare, turning a SILENT split-brain into a loud,
        self-correcting flap.
        """
        a = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        b = AsyncLeaderElection(_LIBPQ_TEST_DSN, lock_key)
        try:
            assert await a.is_leader() is True
            assert a._pid is not None, "acquire must pin the owning backend pid"

            # Pretend the lock was acquired on a different backend than the one
            # we are now talking to.
            a._pid = a._pid + 100_000

            assert await a.is_leader() is False, "pid mismatch must drop leadership"
            assert a._held is False

            # And the connection was invalidated, so the lock is free again
            # (bounded wait — the backend teardown is asynchronous).
            assert await _acquires_within(b), "pid-mismatch reset must free the lock"
        finally:
            await a.release()
            await b.release()


# ---------------------------------------------------------------------------
# _reset_conn — invalidate-before-close ordering (no DB needed)
# ---------------------------------------------------------------------------


class TestResetConn:
    @pytest.mark.unit
    async def test_invalidate_is_awaited_before_close(self):
        """``invalidate()`` must come first — it is what kills the backend.

        ``close()`` alone returns the connection to the QueuePool with the
        session (and its advisory lock) still alive. Order matters: closing
        first would hand the live session back to the pool before we killed it.
        """
        calls: list[str] = []

        class _FakeConn:
            async def invalidate(self):
                calls.append("invalidate")

            async def close(self):
                calls.append("close")

        e = AsyncLeaderElection(_LIBPQ_TEST_DSN, _fresh_lock_key())
        e._conn = _FakeConn()

        await e._reset_conn()

        assert calls == ["invalidate", "close"]
        assert e._conn is None

    @pytest.mark.unit
    async def test_reset_conn_survives_a_raising_connection(self):
        """A wedged connection must not turn cleanup into an exception."""

        class _AngryConn:
            async def invalidate(self):
                raise OSError("backend is wedged")

            async def close(self):
                raise OSError("backend is wedged")

        e = AsyncLeaderElection(_LIBPQ_TEST_DSN, _fresh_lock_key())
        e._conn = _AngryConn()

        await e._reset_conn()  # must not raise

        assert e._conn is None


# ---------------------------------------------------------------------------
# Sync ETL leader (psycopg sync)
# ---------------------------------------------------------------------------


class TestEtlLeaderSync:
    @pytest.mark.unit
    def test_acquires_then_release_flow(self):
        # Use the real ETL key here — but reset state at end so other tests
        # aren't affected.
        try:
            assert try_acquire_etl_leader(_LIBPQ_TEST_DSN) is True
            # Idempotent — second call is keepalive, still leader.
            assert try_acquire_etl_leader(_LIBPQ_TEST_DSN) is True
        finally:
            release_etl_leader()

    @pytest.mark.unit
    def test_release_is_idempotent(self):
        try_acquire_etl_leader(_LIBPQ_TEST_DSN)
        release_etl_leader()
        release_etl_leader()  # no exception

    @pytest.mark.unit
    def test_after_release_can_reacquire(self):
        try:
            assert try_acquire_etl_leader(_LIBPQ_TEST_DSN) is True
            release_etl_leader()
            assert try_acquire_etl_leader(_LIBPQ_TEST_DSN) is True
        finally:
            release_etl_leader()

    @pytest.mark.unit
    def test_backend_pid_swap_drops_leadership_sync(self):
        """A swapped backend pid means the session-level lock is NOT ours.

        Sync mirror of ``TestAsyncLeaderElection.test_backend_pid_swap_drops_leadership``.
        Simulates the txn-mode-pooler misconfiguration (``DIRECT_POSTGRES_HOST``
        pointed at ``pgbouncer:6432`` instead of the session-mode pooler): the
        pooler rebinds a different server backend, so the advisory lock we took
        is gone even though a ``SELECT 1`` keepalive would happily succeed and
        keep reporting us leader forever → two ETL leaders → double exec_quality
        ETL. Pinning + re-comparing ``pg_backend_pid()`` turns that SILENT
        split-brain into a loud, self-correcting flap.

        MUTATION-PROOF: against the old ``SELECT 1`` keepalive (which never pins
        or compares a pid) the post-swap call returns True — leadership is
        falsely retained — so both the ``is not None`` pin assertion and the
        post-swap ``is False`` assertion fail.
        """
        from services import leader_election as le

        try:
            assert try_acquire_etl_leader(_LIBPQ_TEST_DSN) is True
            assert le._etl_state.get("pid") is not None, "acquire must pin the owning backend pid"

            # Pretend the lock was acquired on a different backend than the one
            # we are now talking to (txn-pooler rebind under us).
            le._etl_state["pid"] = le._etl_state["pid"] + 100_000

            assert try_acquire_etl_leader(_LIBPQ_TEST_DSN) is False, (
                "pid mismatch must drop leadership, not falsely report it"
            )
            assert le._etl_state["held"] is False

            # The mismatch closed the connection → the old session ended and
            # Postgres auto-released the lock, so the next tick re-acquires
            # cleanly rather than staying wedged as a non-leader. Backend
            # teardown after close() is asynchronous, so poll with a bound
            # (same rationale as the async _acquires_within helper).
            import time as _time

            deadline = _time.monotonic() + 5.0
            reacquired = False
            while _time.monotonic() < deadline:
                if try_acquire_etl_leader(_LIBPQ_TEST_DSN) is True:
                    reacquired = True
                    break
                _time.sleep(0.05)
            assert reacquired, "pid-mismatch reset must free the lock so the next tick re-acquires"
        finally:
            release_etl_leader()


# ---------------------------------------------------------------------------
# A bad leader-election DSN must fail LOUDLY, not silently
# ---------------------------------------------------------------------------


class TestBadDsnFailsLoudly:
    """The fail-closed ValueError is only worth having if somebody HEARS it.

    ``run_rollup_loop`` / ``run_watcher_loop`` are ``asyncio.create_task``'d in
    ``main.lifespan``. A ValueError raised *inside* one of them dies silently:
    the ``tasks`` list holds a reference (so asyncio never emits "exception was
    never retrieved") and the only ``gather`` is at shutdown with
    ``return_exceptions=True``. A misconfigured DSN would therefore mean the
    hourly rollup and the budget-overage watcher never run, with ZERO log
    output — trading a wrong-database lock for an equally silent dead loop.

    So the DSN is resolved at BOOT (loud crash), and any task that dies anyway
    is reported by ``main._log_task_death``.
    """

    def _settings(self, **overrides):
        from settings import Settings

        # Annotated: an untyped dict literal narrows to `dict[str, str | None]`,
        # and `Settings(**base)` then reports one type error per bool field on
        # the model — a count that grows every time a bool setting is added.
        base: dict[str, Any] = {
            # No /<database> path, and no explicit leader_election_dsn → the
            # resolver must refuse to guess a database.
            "database_url": "postgresql+psycopg://u:p@pgbouncer:6432",
            # Pin to None explicitly: Settings is a BaseSettings, so a stray
            # LEADER_ELECTION_DSN in the environment would otherwise satisfy the
            # resolver and silently make these assertions vacuous.
            "leader_election_dsn": None,
            "direct_postgres_host": "pgbouncer-session:6432",
            "redis_url": "redis://localhost:6379/15",
            "licenses_url": "http://localhost:9999",
            "environment": "test",
        }
        base.update(overrides)
        return Settings(**base)

    @pytest.mark.unit
    def test_resolver_raises_on_pathless_dsn(self):
        with pytest.raises(ValueError, match="missing the /<database> path"):
            self._settings().resolve_leader_election_dsn()

    @pytest.mark.unit
    def test_resolver_passes_through_explicit_dsn(self):
        s = self._settings(leader_election_dsn="postgresql://u:p@localhost:5432/billing")
        assert s.resolve_leader_election_dsn() == "postgresql://u:p@localhost:5432/billing"

    @pytest.mark.unit
    def test_resolver_derives_from_database_url(self):
        s = self._settings(database_url="postgresql+psycopg://u:p@pgbouncer:6432/billing")
        assert s.resolve_leader_election_dsn() == "postgresql://u:p@pgbouncer-session:6432/billing"

    @pytest.mark.unit
    def test_boot_check_is_reached_before_the_loops_are_spawned(self):
        """main.lifespan must resolve the DSN itself, not leave it to the tasks.

        Guards the regression directly: if resolution moves back inside the
        create_task'd loops, a bad DSN goes silent again.
        """
        import inspect

        import main

        src = inspect.getsource(main.lifespan)
        assert "_validate_leader_election(settings)" in src, (
            "main.lifespan must resolve the leader-election DSN at boot so a bad "
            "config fails loudly instead of silently killing the create_task'd "
            "rollup/watcher loops"
        )
        assert "_log_task_death" in src, (
            "background tasks must carry the _log_task_death done-callback"
        )

    def _prod_settings(self, **overrides):
        """A production Settings whose ONLY defect is the leader-election DSN.

        Settings._enforce_production_required_fields already rejects an empty
        ``direct_postgres_host`` in production, so the only bad-DSN case that can
        actually reach the boot check in prod is a ``database_url`` with no
        ``/<database>`` path. Everything else here exists purely to satisfy that
        validator — otherwise Settings would raise a pydantic ValidationError
        (itself a ValueError!) during construction and we would be asserting on
        the wrong exception.
        """
        from settings import Settings

        base: dict[str, Any] = {
            "environment": "production",
            # No /<db> path — the one bad-DSN case that can actually reach the
            # boot check in production (direct_postgres_host is already required
            # there by Settings._enforce_production_required_fields). Same u:p@
            # placeholder the rest of this module uses, so detect-secrets does
            # not see a new credential.
            "database_url": "postgresql+psycopg://u:p@pgbouncer:6432",
            "direct_postgres_host": "pgbouncer-session:6432",
            "leader_election_dsn": None,
            "internal_secret": "s3cret",  # pragma: allowlist secret
            "redis_url": "redis://redis:6379/0",
            "licenses_url": "http://licenses:9000",
            "keycloak_url": "http://keycloak:8080",
            "otlp_traces_endpoint": "http://tempo:4318/v1/traces",
            # conftest disables auth; production refuses that at load
            # (Settings._refuse_auth_disabled_in_production).
            "dashboard_auth_enabled": "true",
        }
        base.update(overrides)
        return Settings(**base)

    @pytest.mark.unit
    def test_production_boot_is_fatal_on_a_bad_dsn(self):
        """In production a pod that cannot elect a leader must CRASH, not run Ready.

        Sitting Ready with a dead rollup loop and a dead overage watcher is
        exactly the silent no-op this PR exists to eliminate.
        """
        from structlog.testing import capture_logs

        from main import _validate_leader_election

        settings = self._prod_settings()  # constructed OUTSIDE pytest.raises

        with capture_logs() as logs, pytest.raises(ValueError, match="/<database> path"):
            _validate_leader_election(settings)

        crit = [e for e in logs if e.get("event") == "leader_election_dsn_invalid"]
        assert crit, "a fatal misconfiguration must be logged, not just raised"
        assert crit[0]["log_level"] == "critical"
        assert crit[0]["fatal"] is True

    @pytest.mark.unit
    def test_non_production_boot_logs_critical_but_keeps_booting(self):
        """dev/test/CI run with no session pooler; they must still boot — LOUDLY.

        The loops die there anyway, but _log_task_death makes that visible rather
        than silent, which is the property we actually care about.
        """
        from structlog.testing import capture_logs

        from main import _validate_leader_election

        with capture_logs() as logs:
            _validate_leader_election(self._settings(environment="test"))  # must NOT raise

        crit = [e for e in logs if e.get("event") == "leader_election_dsn_invalid"]
        assert crit and crit[0]["log_level"] == "critical"
        assert crit[0]["fatal"] is False

    @pytest.mark.unit
    def test_valid_dsn_boots_quietly(self):
        """Non-vacuity guard: a good config must not cry wolf."""
        from structlog.testing import capture_logs

        from main import _validate_leader_election

        good = self._settings(database_url="postgresql+psycopg://u:p@pgbouncer:6432/billing")
        with capture_logs() as logs:
            _validate_leader_election(good)

        assert not [e for e in logs if e.get("event") == "leader_election_dsn_invalid"]

    @pytest.mark.unit
    def test_redis_none_at_boot_is_fatal_in_production(self):
        """Redis down at boot in prod must CRASH, not serve Ready with dead loops.

        The consumer/flush/rollup loops are created only inside `if redis_conn:`,
        once, at boot — so a redis-None boot never starts them, yet readiness()
        re-pings live and flips Ready on recovery, silently dropping billing events.
        """
        from structlog.testing import capture_logs

        from main import _validate_redis_at_boot

        settings = self._prod_settings()  # constructed OUTSIDE pytest.raises
        with capture_logs() as logs, pytest.raises(RuntimeError, match="Redis unavailable"):
            _validate_redis_at_boot(settings, None)

        crit = [e for e in logs if e.get("event") == "redis_unavailable_at_boot"]
        assert crit, "a fatal misconfiguration must be logged, not just raised"
        assert crit[0]["log_level"] == "critical"
        assert crit[0]["fatal"] is True

    @pytest.mark.unit
    def test_redis_none_at_boot_non_production_keeps_booting(self):
        """dev/test/CI legitimately boot without Redis — loudly, but not fatally."""
        from structlog.testing import capture_logs

        from main import _validate_redis_at_boot

        with capture_logs() as logs:
            _validate_redis_at_boot(self._settings(environment="test"), None)  # must NOT raise

        crit = [e for e in logs if e.get("event") == "redis_unavailable_at_boot"]
        assert crit and crit[0]["log_level"] == "critical"
        assert crit[0]["fatal"] is False

    @pytest.mark.unit
    def test_redis_present_boots_quietly(self):
        """Non-vacuity guard: a live Redis connection must not cry wolf."""
        from structlog.testing import capture_logs

        from main import _validate_redis_at_boot

        with capture_logs() as logs:
            _validate_redis_at_boot(self._prod_settings(), object())  # a live conn

        assert not [e for e in logs if e.get("event") == "redis_unavailable_at_boot"]

    @pytest.mark.unit
    async def test_log_task_death_reports_a_dying_task(self):
        """A background task that raises must produce a LOUD log line.

        Uses ``structlog.testing.capture_logs`` rather than ``caplog``: structlog
        is not necessarily wired to the stdlib logging handlers in a bare unit
        test, so caplog can miss the event even though production emits it.
        """
        from structlog.testing import capture_logs

        from main import _log_task_death

        async def _boom():
            raise RuntimeError("loop exploded")

        task = asyncio.create_task(_boom(), name="rollup")
        with contextlib.suppress(RuntimeError):
            await task

        with capture_logs() as logs:
            _log_task_death(task)

        died = [e for e in logs if e.get("event") == "background_task_died"]
        assert died, f"a dying background task must be logged loudly; got {logs}"
        assert died[0]["log_level"] == "error"
        assert died[0]["task"] == "rollup"

    @pytest.mark.unit
    async def test_log_task_death_stays_silent_for_a_healthy_task(self):
        """Non-vacuity guard: the callback must not cry wolf on a clean exit."""
        from structlog.testing import capture_logs

        from main import _log_task_death

        async def _fine():
            return None

        task = asyncio.create_task(_fine(), name="rollup")
        await task

        with capture_logs() as logs:
            _log_task_death(task)

        assert not [e for e in logs if e.get("event") == "background_task_died"]

    @pytest.mark.unit
    async def test_log_task_death_is_quiet_on_cancellation(self):
        """Shutdown cancels every task — that is expected, not an error."""
        from main import _log_task_death

        async def _forever():
            await asyncio.sleep(3600)

        task = asyncio.create_task(_forever(), name="rollup")
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        _log_task_death(task)  # must not raise, must not log an error
