"""Postgres advisory-lock-based leader election for background loops.

billing-service runs at ``replicas: 2``, so every in-process background loop
would otherwise run once *per replica* — duplicate hourly rollups and duplicate
budget-overage webhooks. This module gates such loops so exactly one replica
does the work:

  * :class:`AsyncLeaderElection` — the async ``run_rollup_loop``
    (``ROLLUP_LOCK_KEY``) and ``run_watcher_loop`` (``WATCHER_LOCK_KEY``) paths.
  * :func:`try_acquire_etl_leader` / :func:`release_etl_leader` — the sync
    ``exec_quality_etl.start_scheduler`` APScheduler tick (``ETL_LOCK_KEY``).

WHICH CONNECTION HOLDS THE LOCK — read this before "fixing" it
--------------------------------------------------------------
The lock is taken on the **billing** database over a **session-mode pooler**.
In production ``DIRECT_POSTGRES_HOST`` points at the session-mode pgbouncer
service (see the billing Deployment manifest), and
:func:`direct_postgres_dsn` rewrites ``DATABASE_URL``'s host to it while
preserving the database path. Two properties are load-bearing:

* **Session mode, not transaction mode.** An advisory lock is *session*-scoped.
  The session-mode pooler pins one server backend per client connection for the
  life of that connection, so the lock survives between the ~300s ticks.
  ``DATABASE_URL`` itself points at the **TRANSACTION**-mode pooler, which
  rebinds a different server backend per transaction: a session-level lock taken there is silently
  released on the next checkout, so every replica would observe
  ``pg_try_advisory_lock -> true`` and believe it is leader. Split-brain, with
  double rollups and double overage webhooks. ``DATABASE_URL`` MUST NEVER be
  used for the lock — that is the whole reason ``DIRECT_POSTGRES_HOST`` exists.
  (An earlier version of this docstring claimed we "bypass pgbouncer" for a
  *direct* postgres connection. That is no longer true: pods cannot reach
  the Postgres primary directly — the ambient ztunnel refuses the same-node
  pod→host-IP hop — so we ride the session-mode pooler, which pins a backend
  and is equally correct.)
* **A dedicated pgbouncer-session pool entry.** billing has its own ``billing``
  entry in the pgbouncer-session configmap; each replica parks its lock
  connection there for the lifetime of leadership, so it must draw on billing's
  own pool budget rather than a shared one.

The lock dies with the session — so a crashed/OOMed leader cleanly transfers
leadership to whichever surviving replica acquires next, without needing a
separate liveness/heartbeat mechanism. Note this is exactly why
:meth:`AsyncLeaderElection._reset_conn` must *invalidate* rather than merely
close the connection; see its docstring.

WHAT REAPS THE HELD SESSION — measured, and a previous round got it wrong
------------------------------------------------------------------------
⛔ The reaper is Postgres ``idle_session_timeout``, which is **600000 ms** on the
worker4 primary (``pg_settings.source = 'configuration file'``). It is NOT
pgbouncer-session's ``server_idle_timeout = 240``: in session mode a server
backend held by a live client is never *idle in the pool*, and a rollup election
session was measured surviving **3h17m** on 300s pings. Do not "fix" the pooler.

Before :attr:`AsyncLeaderElection._keepalive_interval_s` existed, the held
session was pinged ONLY from :meth:`AsyncLeaderElection.is_leader`, i.e. once per
loop tick. Every loop whose tick interval exceeds 600s therefore had its election
session reaped **every single cycle**:

  * invoice close — ``invoice_close_interval_seconds`` = 3600
  * licence↔budget reconcile — ``licence_budget_reconcile_interval_seconds`` = 900

Measured live: ``leader_acquired`` on lock ``0x4d455f4c425243`` at 10:53:28Z,
``leader_election_error`` at 11:08:28Z — exactly 900s later — while the peer
acquired the same lock at 11:03:31Z.

⚠️ Scope it honestly: mutual exclusion was NOT broken (4 advisory rows in
``billing``, each held by exactly one pid; the standby holds 0). The damage was
(a) the ``*_is_leader`` gauges LYING — a demoted pod kept publishing 1 until its
next tick, so ``sum()`` read 2 and split-brain detection saw a phantom;
(b) pointless churn and doubled downstream load; and (c) ``close_period``'s
docstring calling the second-writer race "theoretical" stopped being true.

The fix is a keepalive on its OWN schedule (:meth:`AsyncLeaderElection._keepalive_loop`),
decoupled from the tick, plus a demotion callback so a loop's gauge drops to 0 at
the moment leadership is lost rather than a whole interval later.

Lock keys are stable 64-bit integers, hex-ascii mnemonic so a future
collision check is grep-able. They are inherited from metering-service (billing
was carved out of it) and are therefore NOT unique across services — they are
only unique *within a database*, which is all Postgres advisory locks scope to.
That is another reason the database in the lock DSN must never be defaulted;
see :func:`direct_postgres_dsn`.

  ``ME_ROLLP`` = 0x4D45_5F52_4F4C_4C50  (rollup loop)
  ``ME_EQALT`` = 0x4D45_5F45_5141_4C54  (exec_quality ETL)
  ``ME_WATCH`` = 0x4D45_5F57_4154_4348  (budget overage watcher)
"""

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any

import psycopg
import structlog
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from observability import metrics as observability_metrics

logger = structlog.stdlib.get_logger(__name__)

ROLLUP_LOCK_KEY: int = 0x4D45_5F52_4F4C_4C50
ETL_LOCK_KEY: int = 0x4D45_5F45_5141_4C54
WATCHER_LOCK_KEY: int = 0x4D45_5F57_4154_4348
# "ME_INVOC" — the monthly invoice close loop (services/invoice_closer.py). Its own key,
# because closing a period takes the number counter's FOR UPDATE and must never contend
# with the rollup or watcher loops for a lock. ⚠️ Like the three above, this MUST be taken
# on the SESSION-mode pooler (Settings.resolve_leader_election_dsn), never DATABASE_URL:
# on the transaction-mode pooler every replica sees pg_try_advisory_lock -> true, and here
# that means two replicas closing the same period and minting two invoices for it.
INVOICE_LOCK_KEY: int = 0x4D45_5F49_4E56_4F43
# "ME_LBRC" — the licence↔budget reconcile loop
# (services/licence_budget_reconcile.py). Its own key so it can hold leadership
# on a different replica from the loops above; it must not contend with the
# rollup or invoice locks, whose cycles are far longer than its own. ⚠️ Same
# SESSION-mode pooler requirement as every key above: on the transaction-mode
# pooler every replica sees pg_try_advisory_lock -> true, and here that means N
# replicas each fetching the whole licence registry every tick.
LICENCE_BUDGET_RECONCILE_LOCK_KEY: int = 0x4D45_5F4C_4252_43
# "ME_QCVR" — the request_quota_config value repair loop
# (services/quota_config_sync.py). Its own key for the same reason as the one
# above, and with the same SESSION-mode pooler requirement.
QUOTA_CONFIG_SYNC_LOCK_KEY: int = 0x4D45_5F51_4356_52

# Upper bound on the shutdown unlock. A wedged connection must not block the
# pod's lifespan shutdown: on timeout we close the session instead, which
# auto-releases the advisory lock anyway.
_RELEASE_TIMEOUT_S: float = 5.0

# How often the HELD election session is pinged, independently of the loop tick.
#
# ⛔ This number is bounded ABOVE by TWO measured server-side reapers and must
# stay comfortably under BOTH. Do not raise it to "save a round-trip":
#
#   * ``idle_session_timeout``              = 600000 ms (600s) — the reaper that
#     actually killed the long-interval loops' sessions. See the module
#     docstring; measured from ``pg_settings`` on the worker4 primary.
#   * ``idle_in_transaction_session_timeout`` = 300000 ms (300s) — cannot bite
#     while the connection is AUTOCOMMIT (see the engine comment in
#     :meth:`AsyncLeaderElection.is_leader`), but 240s stays under it anyway so
#     that an accidental loss of AUTOCOMMIT degrades to a slow flap rather than
#     to a session reaped every cycle.
#
# 240s gives 2.5 pings inside the 600s window, so a single dropped ping does not
# cost the session. Lowering it is safe; raising it past ~290s is not.
_KEEPALIVE_INTERVAL_S: float = 240.0

# ``loop`` label for billing_leader_keepalive_failures_total, and the ``loop=``
# log field. Keyed by lock key so EVERY caller gets a correct label with no
# wiring — a metric that silently labelled all five loops "async" would make the
# per-loop breakdown (the thing that identifies WHICH interval is too long)
# unreadable. Callers may still override with ``loop=``.
_LOOP_LABELS: dict[int, str] = {
    ROLLUP_LOCK_KEY: "rollup",
    ETL_LOCK_KEY: "etl",
    WATCHER_LOCK_KEY: "budget_watcher",
    INVOICE_LOCK_KEY: "invoice_close",
    LICENCE_BUDGET_RECONCILE_LOCK_KEY: "licence_budget_reconcile",
    QUOTA_CONFIG_SYNC_LOCK_KEY: "quota_config_sync",
}


def direct_postgres_dsn(pgbouncer_dsn: str, direct_host: str) -> str:
    """Rewrite a pgbouncer DSN to point at direct postgres ``direct_host``.

    Returns a libpq-style URL (no ``+psycopg`` driver suffix). Mirrors the
    shell logic in ``cronjob-pg-dump.yaml``.

    The async leader adds the ``+psycopg`` prefix back when building the
    SQLAlchemy engine; the sync ETL helper passes the libpq form straight
    to ``psycopg.connect()``.

    ``direct_host`` is ``host:port`` (e.g.
    ``pgbouncer-session.example.com:6432``) and comes from
    ``settings.direct_postgres_host`` (set per-env via the Deployment manifest).
    Required because the transaction-mode pgbouncer multiplexes server
    connections per transaction; advisory locks need a stable session.

    The database path is copied verbatim from ``pgbouncer_dsn`` and is NEVER
    defaulted. A path-less DSN is a misconfiguration and MUST fail closed: the
    advisory-lock keys in this module are inherited from metering-service, so
    they are not unique across services, and an advisory lock is only scoped to
    a *database*. Silently guessing a database name would take billing's
    leadership lock in some other service's database — where it can neither be
    observed nor be trusted to mutually exclude the right set of processes.

    The ``ValueError`` only helps if somebody HEARS it. Both leader-elected loops
    are ``asyncio.create_task``'d, so raising this inside one of them would kill
    that task silently and the loop would simply never run — trading a wrong-DB
    lock for an equally-silent dead loop. That is why the DSN is resolved at boot
    via ``Settings.resolve_leader_election_dsn`` on the ``main.lifespan`` path,
    which logs CRITICAL and (in production) aborts startup; ``_log_task_death``
    covers the general case of a background task dying. Do not move resolution
    back inside the loops.

    Raises :class:`ValueError` when the input lacks ``user:pass@`` credentials,
    when it carries no ``/<database>`` path, or when ``direct_host`` is empty.
    """
    if not direct_host:
        raise ValueError(
            "direct_postgres_dsn requires a direct_host (set "
            "DIRECT_POSTGRES_HOST in the deployment)"
        )
    raw = pgbouncer_dsn
    for prefix in ("postgresql+psycopg://", "postgresql+asyncpg://", "postgresql://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix) :]
            break
    if "@" not in raw:
        raise ValueError(f"DSN missing credentials: {pgbouncer_dsn!r}")
    creds, hostpath = raw.split("@", 1)
    if "/" not in hostpath:
        raise ValueError(
            "DSN missing the /<database> path, refusing to guess one: "
            f"{pgbouncer_dsn!r}. The advisory lock is database-scoped and these "
            "lock keys are shared with metering-service, so a guessed database "
            "would hold billing's leadership lock in the wrong database."
        )
    _hostport, dbpath = hostpath.split("/", 1)
    if "?" in dbpath:
        dbpath = dbpath.split("?", 1)[0]
    if not dbpath:
        raise ValueError(
            f"DSN has an empty /<database> path, refusing to guess one: {pgbouncer_dsn!r}"
        )
    return f"postgresql://{creds}@{direct_host}/{dbpath}"


class AsyncLeaderElection:
    """Async leader election via ``pg_try_advisory_lock``.

    Holds a single dedicated connection (NOT pooled) for the lifetime of
    leadership. Safe to call :meth:`is_leader` repeatedly — first call opens
    the connection and tries to acquire.

    ⚠️ The session is kept alive by a BACKGROUND TASK on its own 240s schedule
    (:meth:`_keepalive_loop`), NOT by :meth:`is_leader`. It used to be the other
    way round, and every loop whose tick exceeded ``idle_session_timeout`` (600s)
    lost its session every cycle — see the module docstring. ``is_leader`` is now
    a *verdict*, the keepalive is the *liveness*; keep them separate.

    ``on_demote`` is invoked (synchronously, exceptions suppressed and logged)
    every time leadership is given up — the error arm, the pid-mismatch arm, a
    failed keepalive round, and :meth:`release`. This module does NOT own the
    ``billing_*_is_leader`` gauges; the loops do, and a loop that only writes its
    gauge once per tick publishes a stale 1 for up to a whole interval after being
    demoted. Each loop therefore passes a hook that drops its own gauge::

        election = AsyncLeaderElection(
            direct_dsn, WATCHER_LOCK_KEY, on_demote=_clear_leader_gauge
        )

    Usage::

        election = AsyncLeaderElection(direct_dsn, ROLLUP_LOCK_KEY)
        try:
            while not stop_event.is_set():
                if not await election.is_leader():
                    await asyncio.sleep(POLL_INTERVAL)
                    continue
                ...  # do leader-only work
        finally:
            await election.release()   # also cancels the keepalive task
    """

    def __init__(
        self,
        dsn: str,
        lock_key: int,
        *,
        loop: str | None = None,
        on_demote: Callable[[], None] | None = None,
        keepalive_interval_s: float = _KEEPALIVE_INTERVAL_S,
    ):
        # Accept either libpq (``postgresql://``) or sqlalchemy
        # (``postgresql+psycopg://``) form — normalize to the latter.
        if dsn.startswith("postgresql://"):
            dsn = "postgresql+psycopg://" + dsn[len("postgresql://") :]
        self._dsn = dsn
        self._lock_key = lock_key
        self._engine: AsyncEngine | None = None
        self._conn: Any = None  # AsyncConnection
        self._held = False
        # Backend pid of the session that acquired the lock. The keepalive
        # compares against it to prove we are still on THAT backend — see
        # is_leader().
        self._pid: int | None = None
        self._loop_label = loop or _LOOP_LABELS.get(lock_key, hex(lock_key))
        self._on_demote = on_demote
        self._keepalive_interval_s = keepalive_interval_s
        self._keepalive_task: asyncio.Task[None] | None = None
        # Serialises the ONE dedicated connection between is_leader(), the
        # keepalive task and release(). Two coroutines executing on the same
        # AsyncConnection concurrently is undefined behaviour, and the whole
        # point of the keepalive is that it runs when nobody called is_leader().
        # ⛔ _reset_conn() must NEVER take this lock: it is called from inside
        # every holder of it (asyncio.Lock is not reentrant → instant deadlock).
        self._conn_lock = asyncio.Lock()
        # Set by a keepalive round that LOST the session. Consumed by the next
        # is_leader(), which reports the demotion instead of silently
        # re-acquiring on the same tick — see is_leader().
        self._keepalive_failed = False

    async def is_leader(self) -> bool:
        """Return True if this instance currently holds the lock.

        Side effects:

          * First call: opens the dedicated single-connection engine.
          * When not held: tries to acquire (non-blocking), pinning the backend
            pid that now owns the lock.
          * When held: verifies we are still on that same backend. If the pid
            changed, or the ping fails, leadership is dropped and the next call
            re-attempts acquisition (advisory locks are auto-released by
            Postgres when a session ends).

        The keepalive deliberately checks ``pg_backend_pid()`` rather than
        ``SELECT 1``. ``SELECT 1`` only proves *a* session answered — not that we
        still hold the lock. If ``DIRECT_POSTGRES_HOST`` is ever misconfigured to
        the TRANSACTION-mode pooler (an easy copy-paste of ``DATABASE_URL``),
        pgbouncer rebinds a different server backend per transaction, the
        session-level lock is silently gone, and every replica would keep
        reporting itself leader forever: a *silent* split-brain running double
        rollups and double overage webhooks. Comparing the pid turns that footgun
        into a loud, self-correcting flap (``leader_backend_swapped`` WARN +
        re-acquire) for the cost of zero extra round-trips.

        ⚠️ The held-branch ping below is now a *second* verifier, not the
        liveness mechanism: :meth:`_keepalive_loop` pings every 240s regardless of
        how rarely this is called. It is kept because it is free (the caller is
        already awaiting) and because it verifies ownership at exactly the moment
        the caller is about to act on the answer.
        """
        if self._keepalive_failed:
            # A keepalive round lost the session since the last tick. It already
            # demoted, invalidated the connection and fired ``on_demote``. Report
            # the demotion for ONE tick rather than re-acquiring here: the loops'
            # not-leader path is the SHORT ``leader_retry_interval_seconds``
            # (~15s), so failing closed costs 15s, and a silent same-tick
            # re-acquire would hide a flapping session behind an unbroken run of
            # ``is_leader() -> True``.
            self._keepalive_failed = False
            logger.info(
                "leader_keepalive_demotion_reported",
                loop=self._loop_label,
                lock_key=hex(self._lock_key),
            )
            return False
        try:
            async with self._conn_lock:
                return await self._check_locked()
        except SQLAlchemyError, psycopg.Error, OSError:
            # Any conn-layer issue = treat as lost leadership. _reset_conn()
            # INVALIDATES the connection (hard-closes the session), which is what
            # actually drops the advisory lock — a plain close() would return the
            # conn to the pool with the session, and the lock, still alive. See
            # _reset_conn().
            logger.warning(
                "leader_election_error",
                loop=self._loop_label,
                lock_key=hex(self._lock_key),
                exc_info=True,
            )
            self._demote()
            # Re-taken because the ``async with`` above already unwound when the
            # exception propagated. Safe (we are outside it) and required:
            # _reset_conn() mutates self._conn, which the keepalive task also
            # reads under this lock.
            async with self._conn_lock:
                await self._reset_conn()
            return False

    async def _check_locked(self) -> bool:
        """The body of :meth:`is_leader`, run with ``_conn_lock`` HELD.

        Split out only so the lock is taken once, around everything that touches
        ``self._conn`` — including the ``_reset_conn()`` in the error arm, which
        must therefore not take the lock itself.
        """
        if self._engine is None:
            self._engine = create_async_engine(
                self._dsn,
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=False,
                # AUTOCOMMIT so the dedicated connection never sits in an
                # implicit transaction between pings. Without autocommit
                # SQLAlchemy opens a txn on the first execute and the session is
                # reaped by Postgres ``idle_in_transaction_session_timeout``
                # (300s) before the next ping, flapping leadership every cycle.
                # The lock is session-level (``pg_try_advisory_lock``), so it
                # survives autocommit — same rationale as the sync ETL helper
                # below. ⚠️ This is a DIFFERENT reaper from the one the keepalive
                # exists for (``idle_session_timeout``, 600s); AUTOCOMMIT alone
                # does not save a session that is simply never pinged.
                isolation_level="AUTOCOMMIT",
            )
        if self._conn is None:
            self._conn = await self._engine.connect()
        if not self._held:
            # Acquire + pin the owning backend in ONE round-trip.
            result = await self._conn.execute(
                text("SELECT pg_try_advisory_lock(:k), pg_backend_pid()"),
                {"k": self._lock_key},
            )
            acquired, pid = result.one()
            self._held = bool(acquired)
            self._pid = int(pid) if self._held else None
            if self._held:
                logger.info(
                    "leader_acquired",
                    loop=self._loop_label,
                    lock_key=hex(self._lock_key),
                    backend_pid=self._pid,
                )
        else:
            # Ownership re-verification: same backend ⇒ the session-level lock we
            # took is still ours. (Liveness is the keepalive task's job, not
            # this call's — see the method docstring.)
            result = await self._conn.execute(text("SELECT pg_backend_pid()"))
            pid = int(result.scalar_one())
            if pid != self._pid:
                # The backend was swapped under us (txn-mode pooler rebind, or
                # a transparent reconnect). Whatever lock we held belonged to
                # the OLD session, not this backend — we are NOT leader,
                # whatever this connection says. _reset_conn() invalidates
                # (hard-closes) the session so no lock we may still own on it
                # lingers and blocks every replica from acquiring.
                logger.warning(
                    "leader_backend_swapped",
                    loop=self._loop_label,
                    source="tick",
                    lock_key=hex(self._lock_key),
                    expected_pid=self._pid,
                    actual_pid=pid,
                    hint=(
                        "advisory lock lost — is DIRECT_POSTGRES_HOST pointed "
                        "at the TRANSACTION pooler? it must be the session-mode "
                        "pooler"
                    ),
                )
                self._demote()
                await self._reset_conn()
                return False
        if self._held:
            # Self-healing and leak-free: creates a task only when there is not
            # already a live one, so N is_leader() calls never mean N tasks, and
            # a keepalive task that died for any reason is replaced on the next
            # tick rather than leaving the session unpinged forever.
            self._ensure_keepalive()
        return self._held

    # ------------------------------------------------------------------
    # Demotion + keepalive
    # ------------------------------------------------------------------

    def _demote(self, *, keepalive_failed: bool = False) -> None:
        """Give up leadership and tell the owning loop, in ONE place.

        Every path that clears ``_held`` goes through here so that no arm can be
        added later which drops leadership without dropping the loop's gauge —
        that asymmetry IS the lying-gauge defect, and it was originally introduced
        by exactly such an arm.

        Deliberately synchronous: it is called from :meth:`release` and from the
        keepalive task, and a hook that could block would let a wedged gauge write
        hold up pod shutdown. ``on_demote`` is therefore contractually non-blocking
        (a ``Gauge.set``), and a raising hook is logged rather than allowed to
        break the election — a broken observability write must never cost
        leadership correctness.
        """
        self._held = False
        self._pid = None
        if keepalive_failed:
            self._keepalive_failed = True
        if self._on_demote is None:
            return
        try:
            self._on_demote()
        except Exception:
            logger.warning(
                "leader_demote_hook_failed",
                loop=self._loop_label,
                lock_key=hex(self._lock_key),
                exc_info=True,
            )

    def _ensure_keepalive(self) -> None:
        """Start the keepalive task unless one is already running. Idempotent."""
        if self._keepalive_task is not None and not self._keepalive_task.done():
            return
        self._keepalive_task = asyncio.create_task(
            self._keepalive_loop(),
            name=f"leader-keepalive-{self._loop_label}",
        )

    async def _keepalive_loop(self) -> None:
        """Ping the held session every ``_keepalive_interval_s``, independent of the tick.

        ⛔ This task is the ONLY thing standing between a >600s loop interval and
        a session reaped every cycle (module docstring). Its ping is the SAME
        ``SELECT pg_backend_pid()`` + pid comparison as the tick's, deliberately:
        a ``SELECT 1`` would keep the session warm while a txn-mode-pooler
        misconfiguration silently moved the lock, which is strictly worse than the
        bug being fixed — it would turn a loud flap into a permanent silent
        split-brain. Do not "optimise" the ping.

        It exits (rather than looping) after any failure: ``_demote`` has already
        cleared ``_held`` and set ``_keepalive_failed``, and ``_reset_conn`` has
        invalidated the session, so there is nothing left to keep alive. The next
        ``is_leader()`` reports the demotion and the one after that re-acquires,
        restarting this task via :meth:`_ensure_keepalive`.
        """
        try:
            while self._held:
                await asyncio.sleep(self._keepalive_interval_s)
                if not self._held:
                    return
                async with self._conn_lock:
                    # Re-checked under the lock: a concurrent tick or release()
                    # may have demoted and torn the connection down while we
                    # were queued on it.
                    if not self._held or self._conn is None:
                        return
                    if not await self._ping_locked():
                        return
        except asyncio.CancelledError:
            raise
        except Exception:
            # NOTHING awaits this task, so an unexpected exception here would be
            # swallowed and the service would silently revert to pinging once per
            # tick — the exact defect this task exists to fix, but now invisible.
            # Fail closed and LOUDLY: count it, log it, drop leadership.
            logger.error(
                "leader_keepalive_task_died",
                loop=self._loop_label,
                lock_key=hex(self._lock_key),
                exc_info=True,
            )
            observability_metrics.leader_keepalive_failures_total.labels(
                loop=self._loop_label
            ).inc()
            self._demote(keepalive_failed=True)
            # ⛔ The reset is NOT optional here. Demoting without invalidating is
            # the zombie-lock bug (_reset_conn's docstring): we would stop
            # believing we are leader while our own live session still holds the
            # advisory lock, and then NO replica could acquire it — budget
            # alerting and the hourly rollup would stop fleet-wide until a pod
            # restarts. The lock is re-taken because the ``async with`` above
            # already unwound as the exception propagated.
            async with self._conn_lock:
                await self._reset_conn()

    async def _ping_locked(self) -> bool:
        """One keepalive round, with ``_conn_lock`` HELD. False ⇒ leadership lost.

        Both failure arms match :meth:`_check_locked`'s exactly — ``_demote()``
        plus ``_reset_conn()``, which INVALIDATES (hard-closes) the session. A
        plain ``close()`` would return the session, and the advisory lock, to the
        QueuePool alive, and then NO replica could acquire it. Fails closed.
        """
        try:
            result = await self._conn.execute(text("SELECT pg_backend_pid()"))
            pid = int(result.scalar_one())
        except SQLAlchemyError, psycopg.Error, OSError:
            logger.warning(
                "leader_keepalive_failed",
                loop=self._loop_label,
                lock_key=hex(self._lock_key),
                interval_s=self._keepalive_interval_s,
                exc_info=True,
            )
            observability_metrics.leader_keepalive_failures_total.labels(
                loop=self._loop_label
            ).inc()
            self._demote(keepalive_failed=True)
            await self._reset_conn()
            return False
        if pid != self._pid:
            logger.warning(
                "leader_backend_swapped",
                loop=self._loop_label,
                source="keepalive",
                lock_key=hex(self._lock_key),
                expected_pid=self._pid,
                actual_pid=pid,
                hint=(
                    "advisory lock lost — is DIRECT_POSTGRES_HOST pointed "
                    "at the TRANSACTION pooler? it must be the session-mode "
                    "pooler"
                ),
            )
            observability_metrics.leader_keepalive_failures_total.labels(
                loop=self._loop_label
            ).inc()
            self._demote(keepalive_failed=True)
            await self._reset_conn()
            return False
        return True

    async def _stop_keepalive(self) -> None:
        """Cancel the keepalive task and wait for it to unwind. Idempotent.

        ⚠️ Cancelled BEFORE ``release()`` takes ``_conn_lock``, never after: a task
        parked on that lock can be cancelled, but a task HOLDING it while
        ``release()`` waits for it would deadlock.
        """
        task = self._keepalive_task
        self._keepalive_task = None
        if task is None or task.done():
            return
        task.cancel()
        # release() is routinely called from a loop's ``finally`` while that loop's
        # own task is being cancelled, in which case awaiting anything can re-raise
        # CancelledError that is not ours. Suppress so shutdown always completes
        # the unlock below — the alternative is a lock stranded until Postgres
        # reaps the backend.
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    async def _reset_conn(self) -> None:
        """Hard-kill the dedicated session so Postgres auto-releases the lock.

        ⛔ Callers MUST already hold ``_conn_lock``; this method must never take it
        itself. It is invoked from inside :meth:`_check_locked` and
        :meth:`_ping_locked`, both of which run under that lock, and
        ``asyncio.Lock`` is not reentrant — taking it here deadlocks the election
        permanently and silently (the loop just stops being leader forever).

        ``invalidate()`` is LOAD-BEARING and must come first. ``close()`` alone
        does NOT close the backend — the engine is a QueuePool
        (``pool_size=1``), so close() returns the DBAPI connection to that pool
        and the *server session stays alive and keeps holding the advisory
        lock*. Two things break if we only close():

        * We clear ``_held`` while still owning the lock, so NO replica can
          acquire it — the hourly rollup and the budget-overage watcher silently
          stop fleet-wide until a pod restarts. On a transient error that does
          not kill the backend (a blip that raises ``OSError`` /
          ``SQLAlchemyError`` but leaves the session alive) this is reachable in
          normal operation. This is the bug this function exists to fix.
        * The next acquire lands on the SAME pooled backend, and
          ``pg_try_advisory_lock`` is re-entrant, so the hold count goes to 2
          while a single ``pg_advisory_unlock`` only decrements it to 1. In
          practice this does not strand the lock — :meth:`release` calls
          ``_reset_conn()``, and ``invalidate()`` kills the session, which drops
          every hold count regardless of depth — but the count is still a lie
          about the state of the world, and it is only harmless *because* we
          invalidate. Do not go back to close().

        In production the DSN points at the SESSION-mode pgbouncer, not at
        Postgres, so ``invalidate()`` hard-closes the client socket to pgbouncer
        rather than the Postgres backend itself. The lock is still released,
        because on client disconnect pgbouncer runs its ``server_reset_query``
        before returning the backend to the pool, and that query — pgbouncer's
        DEFAULT, ``DISCARD ALL`` — includes ``pg_advisory_unlock_all()``.

        Note that default is load-bearing and we do NOT set it explicitly
        (the pgbouncer-session ``configmap.yaml`` sets only
        ``pool_mode = session``). A session-mode pooler configured with
        an empty ``server_reset_query`` would strand the lock on the pooled
        backend. If you ever touch that configmap, keep the reset query.

        ``invalidate()`` hard-closes the DBAPI connection, killing the session —
        which is the only thing that actually drops the lock. It also avoids
        ``close()``'s ``pool_reset_on_return='rollback'`` round-trip, which on a
        wedged backend could block (and neither ``_reset_conn`` nor ``dispose``
        is covered by the release() ``wait_for`` budget).
        """
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.invalidate()
            with contextlib.suppress(Exception):
                await self._conn.close()
            self._conn = None

    async def release(self) -> None:
        """Release the lock and close the dedicated connection. Idempotent.

        The ``wait_for`` below bounds the unlock (a wedged backend, a half-open
        TCP session with no server-side timeout): a blocked unlock must never
        hold up pod shutdown, because SIGKILL would then leave the lock held
        until Postgres reaps the backend, and the surviving replica could not
        take over the rollup/watcher loops in the meantime.

        ⚠️ The keepalive task is cancelled FIRST — before ``_conn_lock`` is taken
        and before the unlock — for two reasons: a task mid-ping would be racing
        this unlock on the one shared connection, and a task left running past
        shutdown is a leak that keeps a reference to the engine we are about to
        dispose. Ordering matters; see :meth:`_stop_keepalive`.
        """
        await self._stop_keepalive()
        # A deliberate release is not a keepalive failure — do not make the next
        # is_leader() report a phantom demotion.
        self._keepalive_failed = False
        async with self._conn_lock:
            try:
                if self._conn is not None and self._held:
                    await asyncio.wait_for(
                        self._conn.execute(
                            text("SELECT pg_advisory_unlock(:k)"), {"k": self._lock_key}
                        ),
                        timeout=_RELEASE_TIMEOUT_S,
                    )
                    logger.info(
                        "leader_released",
                        loop=self._loop_label,
                        lock_key=hex(self._lock_key),
                    )
            except TimeoutError:
                # Unlock did not come back in time. Fall through and close the
                # connection anyway — killing the session auto-releases the advisory
                # lock, which is the outcome we wanted.
                logger.debug(
                    "leader_release_timeout",
                    loop=self._loop_label,
                    lock_key=hex(self._lock_key),
                    timeout_s=_RELEASE_TIMEOUT_S,
                )
            except SQLAlchemyError, psycopg.Error, OSError:
                # Best-effort unlock during shutdown; conn-layer errors are non-fatal
                # (the advisory lock auto-releases when the session closes). Log at
                # debug so a recurring shutdown-unlock failure is still observable.
                logger.debug(
                    "leader_release_failed",
                    loop=self._loop_label,
                    lock_key=hex(self._lock_key),
                    exc_info=True,
                )
            # Through _demote() rather than clearing the flags inline, so a
            # release ALSO drops the loop's gauge. A handover release (see
            # invoice_closer._cycle_failed) that left the gauge at 1 would
            # publish leadership this replica has just given away.
            self._demote()
            await self._reset_conn()
            if self._engine is not None:
                with contextlib.suppress(Exception):
                    await self._engine.dispose()
                self._engine = None


# --- Sync variant for the exec_quality_etl APScheduler tick ---
# Module-level state because the APScheduler is a process-singleton; we
# need a persistent psycopg connection alive for the duration of leadership
# (opening one per tick would defeat the lock — it'd auto-release between
# ticks when the connection closes).

_etl_state: dict[str, Any] = {"conn": None, "held": False, "pid": None}


def try_acquire_etl_leader(libpq_dsn: str) -> bool:
    """Try to acquire the ETL leader lock; idempotent across calls.

    For use inside a sync APScheduler tick. Holds a process-singleton
    psycopg connection so the lock survives across ticks.

    ``libpq_dsn`` must be the libpq form (``postgresql://...``) — pass the
    output of :func:`direct_postgres_dsn`.

    Returns True if this process is leader for the ETL.
    """
    state = _etl_state
    try:
        if state["conn"] is None:
            # autocommit so SELECT pg_try_advisory_lock doesn't sit in an
            # implicit transaction (lock is session-level either way, but
            # autocommit avoids the extra commit churn on keepalives).
            state["conn"] = psycopg.connect(libpq_dsn, autocommit=True)
        if not state["held"]:
            # Acquire + pin the owning backend in ONE round-trip, mirroring the
            # async path (AsyncLeaderElection.is_leader). The pinned pid is what
            # the keepalive re-verifies below.
            with state["conn"].cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s), pg_backend_pid()", (ETL_LOCK_KEY,))
                row = cur.fetchone()
                state["held"] = bool(row[0]) if row else False
                state["pid"] = int(row[1]) if (row and state["held"]) else None
                if state["held"]:
                    logger.info(
                        "leader_acquired",
                        loop="etl",
                        lock_key=hex(ETL_LOCK_KEY),
                        backend_pid=state["pid"],
                    )
        else:
            # Keepalive that actually VERIFIES ownership: same backend ⇒ the
            # session-level lock we took is still ours. A plain ``SELECT 1`` only
            # proves *a* session answered — not that we still hold the lock. If
            # DIRECT_POSTGRES_HOST is ever misconfigured to the TRANSACTION-mode
            # pooler (an easy copy-paste of DATABASE_URL), pgbouncer rebinds a
            # different server backend per transaction, the session-level lock is
            # silently gone, and every replica would keep reporting itself leader
            # forever: a *silent* split-brain running double exec_quality ETL.
            # Comparing the pid turns that footgun into a loud, self-correcting
            # flap (leader_backend_swapped WARN + re-acquire) — same as the async
            # path. Closing the psycopg connection on mismatch ends the session,
            # so Postgres auto-releases any lock the old backend still held.
            with state["conn"].cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                row = cur.fetchone()
                pid = int(row[0]) if row else None
            if pid != state["pid"]:
                logger.warning(
                    "leader_backend_swapped",
                    loop="etl",
                    lock_key=hex(ETL_LOCK_KEY),
                    expected_pid=state["pid"],
                    actual_pid=pid,
                    hint=(
                        "advisory lock lost — is DIRECT_POSTGRES_HOST pointed "
                        "at the TRANSACTION pooler? it must be the session-mode "
                        "pooler"
                    ),
                )
                state["held"] = False
                state["pid"] = None
                with contextlib.suppress(Exception):
                    state["conn"].close()
                state["conn"] = None
                return False
        return state["held"]
    except psycopg.Error, OSError:
        # psycopg connect/execute failures or socket errors before psycopg
        # wraps them — treat as lost leadership and reset state.
        logger.warning(
            "leader_election_error",
            loop="etl",
            lock_key=hex(ETL_LOCK_KEY),
            exc_info=True,
        )
        state["held"] = False
        state["pid"] = None
        if state["conn"] is not None:
            with contextlib.suppress(Exception):
                state["conn"].close()
            state["conn"] = None
        return False


def release_etl_leader() -> None:
    """Release the ETL leader lock and close the connection. Idempotent."""
    state = _etl_state
    try:
        if state["conn"] is not None and state["held"]:
            with state["conn"].cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (ETL_LOCK_KEY,))
            logger.info("leader_released", loop="etl", lock_key=hex(ETL_LOCK_KEY))
    except psycopg.Error, OSError:
        # Best-effort unlock during shutdown; conn-layer errors are non-fatal.
        pass
    state["held"] = False
    state["pid"] = None
    if state["conn"] is not None:
        with contextlib.suppress(Exception):
            state["conn"].close()
        state["conn"] = None
