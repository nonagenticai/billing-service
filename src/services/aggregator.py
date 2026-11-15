"""Background loops that drive event/cost flushing + rollups.

This module is the orchestration layer; the actual work lives in:
- :mod:`services.event_flusher` — flush_events
- :mod:`services.cost_flusher` — flush_costs
- :mod:`services.rollup_engine` — rollup_hourly, rollup_daily, cleanup_old_events

Re-exports those symbols so external callers (``from services.aggregator
import flush_events``) keep working without forcing the import paths through
the new module names.
"""

import asyncio
import contextlib
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

import redis.asyncio as aioredis
import structlog
from redis.exceptions import RedisError
from sqlalchemy import delete
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import observability.metrics as observability_metrics
from models.db import CostActorDaily, CostUsageDaily
from services import audit as audit_svc
from services._parsers import (
    _parse_decimal,
    _parse_int,
    _parse_usage_extra,
    _parse_uuid,
)
from services.archived_tenant_spend import publish_archived_tenant_spend
from services.attribution_audit import run_attribution_audit
from services.cost_flusher import flush_costs
from services.emitter_freshness import publish_emitter_freshness
from services.event_flusher import flush_events
from services.leader_election import (
    ROLLUP_LOCK_KEY,
    AsyncLeaderElection,
)
from services.ledger_guard_audit import run_ledger_guard_audit
from services.rollup_engine import (
    EVENTS_STREAM_MAX_AGE_HOURS,
    catchup_daily,
    cleanup_old_events,
    cleanup_old_processed_messages,
    cost_events_oldest_age_seconds,
    daily_actor_axis_cardinality,
    daily_rollup_date_gaps,
    daily_rollup_lag_days,
    drop_old_cost_partitions,
    ensure_cost_event_partitions,
    late_event_hours,
    metering_hourly_lag_hours,
    missing_daily_dates,
    missing_event_hours,
    rollup_daily,
    rollup_hourly,
    trim_stream,
)
from services.tenant_id_divergence import run_tenant_id_divergence_audit
from services.unregistered_tenant_spend import publish_unregistered_tenant_spend
from settings import Settings

logger = structlog.stdlib.get_logger(__name__)

# Upper bound on how many days a single catch-up pass will roll up, so a cold
# (empty) daily table or a misconfigured huge retention can't make one tick
# loop hundreds of times. The retention floor below already caps the gap; this
# is a belt-and-braces ceiling.
MAX_CATCHUP_DAYS = 92

# How many CONSECUTIVE failed rollup cycles the leader tolerates before it
# releases the advisory lock and lets another replica race for it.
#
# Pre-fix, ANY exception escaped the ``while``, ran ``finally:
# election.release()`` and handed the lock over — which was a cascade when the
# input was poison (every replica died in turn), but was also the only escape
# hatch from a replica-LOCAL fault. Keeping the lock forever traded one failure
# mode for the other: a poisoned leader that survives every cycle holds the lock
# indefinitely, so no rollups, no retention cleanup and no cost-partition
# creation happen anywhere in the fleet, while every pod stays Ready
# (``_log_task_death`` only logs; ``/health/ready`` never looks at the
# background tasks). Measured on the pre-this-fix commit: with a permanently
# raising ``rollup_hourly``, ``task_alive=True, release()_called=0``.
#
# Handing over after N consecutive failures restores that escape hatch without
# restoring the cascade: the loop no longer dies on the way out, so the worst
# case for a genuinely global poison input is leadership ping-ponging between
# replicas at the retry cadence — cheap — while a replica-local fault (wedged
# lock connection, bad in-process state) now actually moves. N > 1 so a single
# transient hiccup does not thrash leadership.
MAX_CONSECUTIVE_ROLLUP_FAILURES = 3


def _count_failed_flush(in_flight_events: int, in_flight_costs: int) -> None:
    """Attribute a failed flush cycle to the batch sizes that were in flight."""
    if in_flight_events:
        observability_metrics.events_flushed_total.labels(status="error").inc(in_flight_events)
    if in_flight_costs:
        observability_metrics.cost_events_flushed_total.labels(status="error").inc(in_flight_costs)


def _log_loop_failure(event: str, level: str = "error") -> None:
    """Emit a loop-failure log without letting the LOGGER take the loop down.

    Always called AFTER the error counter is incremented, deliberately.
    structlog renders inside this call, so a broken handler or a bad value in
    the bound context raises HERE — inside the loop's own except arm, where
    nothing is left to catch it, so it escapes the ``while`` and kills the
    task: precisely the failure mode the broad arm exists to prevent. Counting
    first means the SLO alert still fires even when the log line never lands.
    """
    with contextlib.suppress(Exception):
        getattr(logger, level)(event, exc_info=True)


# Re-exports — keep `from services.aggregator import X` working.
__all__ = [
    "_parse_decimal",
    "_parse_int",
    "_parse_usage_extra",
    "_parse_uuid",
    "cleanup_old_events",
    "cleanup_old_processed_messages",
    "cost_events_oldest_age_seconds",
    "catchup_daily",
    "daily_actor_axis_cardinality",
    "daily_rollup_date_gaps",
    "daily_rollup_lag_days",
    "flush_costs",
    "flush_events",
    "late_event_hours",
    "missing_daily_dates",
    "missing_event_hours",
    "metering_hourly_lag_hours",
    "actor_rollup_cutoff",
    "purge_old_actor_rollup",
    "purge_old_usage_rollup",
    "rollup_daily",
    "rollup_hourly",
    "run_flush_loop",
    "run_rollup_loop",
    "trim_stream",
]


def actor_rollup_cutoff(retention_months: int, *, today: date | None = None) -> date:
    """The oldest ``bucket_date`` :func:`purge_old_actor_rollup` KEEPS.

    Pure and injectable so the boundary can be pinned without a database and
    without waiting for a calendar month to turn over -- the same reason
    ``_resolve_project_id`` in ``services.cost_flusher`` takes its collaborators
    as arguments.

    ⭐ THE WINDOW IS INCLUSIVE OF THE MONTH IN HAND, so the cutoff is
    ``retention_months - 1`` months before the CURRENT month's first day, not
    ``retention_months``. With the default 25 and a tick on 2026-09-17 the cutoff
    is 2024-09-01 and the table holds 2024-09 .. 2026-09 -- twenty-five monthly
    buckets, counted. Using ``retention_months`` directly would keep twenty-six
    and make the setting's name a lie by one month, which is exactly the kind of
    off-by-one that is invisible until someone reconciles a retention claim
    against a row count.

    ⛔ MONTH-ANCHORED, NEVER ``timedelta(days=30 * n)``. A day-arithmetic cutoff
    drifts by ~5 days a year against the calendar and lands MID-MONTH, so a
    purge would delete part of a month's buckets and keep the rest -- an invoice
    period that is half-evidenced is worse than one that is cleanly absent,
    because a partial SUM looks like a complete one.

    ⚠️ ``today`` defaults to the UTC date, not the local one. Every bucket_date
    in ``billing_cost_actor_daily`` is written from a UTC-normalised rollup (see
    ``services.rollup_engine``), and the prod pgbouncer session runs
    Europe/Amsterdam -- so a local ``date.today()`` would shift the boundary by a
    day for ~2 hours of every day, which is a silent one-day retention change
    nobody asked for.
    """
    anchor = today if today is not None else datetime.now(UTC).date()
    # Months since year 0, so the subtraction cannot produce month 0 or 13.
    total = anchor.year * 12 + (anchor.month - 1) - (retention_months - 1)
    return date(total // 12, total % 12 + 1, 1)


async def purge_old_actor_rollup(
    session_factory: async_sessionmaker[AsyncSession],
    retention_months: int,
    *,
    today: date | None = None,
) -> int:
    """DELETE ``billing_cost_actor_daily`` rows older than the retention window.

    ⛔ A ROW DELETE, NOT A PARTITION DROP, AND THE DIFFERENCE IS THE POINT.
    ``billing_cost_events`` is monthly RANGE-partitioned, so its retention is
    ``drop_old_cost_partitions`` -- instant reclaim, and invisible to BOTH
    post-incident controls (``truncate_guard`` is a BEFORE TRUNCATE trigger;
    ``delete_audit`` is DELETE/TRUNCATE-only, and a ``DROP TABLE`` fires
    neither). ``billing_cost_actor_daily`` is a plain table (``CREATE TABLE`` in
    ``ADDITIVE_MIGRATIONS``, no ``PARTITION BY``), so this DELETE IS seen by the
    delete audit. That is a feature, not an inefficiency: this table is ~3 orders
    of magnitude smaller than the raw events it summarises, and paying dead
    tuples for an auditable deletion of invoice evidence is the right trade.

    ⚠️ RAW RETENTION IS NOT TOUCHED AND MUST NOT BE. ``settings.retention_days``
    (90) stays exactly as it was; this window is ~760 days. The two are
    deliberately independent -- see the setting's own description -- and the
    consequence is that for most of this window the rollup is the ONLY surviving
    record of who spent what, because the events it was derived from have been
    dropped. It therefore cannot be rebuilt after the fact, which is why the
    floor is enforced at Settings construction rather than here.

    Returns the number of rows deleted, so a caller (and a test) can assert on
    the EFFECT rather than on the absence of an exception -- a purge that silently
    matched nothing and a purge that ran correctly are otherwise the same event.
    """
    cutoff = actor_rollup_cutoff(retention_months, today=today)
    async with session_factory() as session:
        # `CursorResult.rowcount` is concrete on a DELETE; the abstract `Result` return type loses
        # the attribute, so cast to the concrete type — the same idiom `services/audit.py`'s purge
        # uses, and for the same reason.
        result = cast(
            "CursorResult[Any]",
            await session.execute(
                delete(CostActorDaily).where(CostActorDaily.bucket_date < cutoff)
            ),
        )
        await session.commit()
    deleted = int(result.rowcount or 0)
    # Logged UNCONDITIONALLY, including the zero. A retention job that only
    # speaks when it deletes something is indistinguishable from one that has
    # stopped running, and this one runs at most once a day -- so "no line
    # yesterday" would have to be read as evidence of health.
    logger.info(
        "actor_rollup_retention_purged",
        deleted=deleted,
        cutoff=cutoff.isoformat(),
        retention_months=retention_months,
    )
    return deleted


async def purge_old_usage_rollup(
    session_factory: async_sessionmaker[AsyncSession],
    retention_months: int,
    *,
    today: date | None = None,
) -> int:
    """DELETE ``billing_cost_usage_daily`` rows older than the PER-ACTOR retention window.

    The cross rollup (alembic 0053) carries the same per-member axis as
    ``billing_cost_actor_daily``, so it is held for exactly the same window and
    purged on the same tick -- see :func:`purge_old_actor_rollup` for why that
    window is decoupled from raw retention. Returns rows deleted; logged even at 0.
    """
    cutoff = actor_rollup_cutoff(retention_months, today=today)
    async with session_factory() as session:
        result = cast(
            "CursorResult[Any]",
            await session.execute(
                delete(CostUsageDaily).where(CostUsageDaily.bucket_date < cutoff)
            ),
        )
        await session.commit()
    deleted = int(result.rowcount or 0)
    logger.info(
        "usage_rollup_retention_purged",
        deleted=deleted,
        cutoff=cutoff.isoformat(),
        retention_months=retention_months,
    )
    return deleted


async def run_flush_loop(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    redis_conn: aioredis.Redis | None,
    event_buffer: list[dict[str, Any]],
    cost_buffer: list[dict[str, Any]],
    stop_event: asyncio.Event,
) -> None:
    """Periodically flush buffered events to the database.

    Flushes every ``flush_interval_seconds`` **or** when either buffer reaches
    ``batch_size``, whichever comes first.

    Not leader-elected (unlike :func:`run_rollup_loop`). The Redis consumer
    group already partitions stream messages across replicas — each message
    is delivered to exactly one consumer, so N replicas flushing in parallel
    handle disjoint events. The flushers carry the ``consumer_group`` through
    so they can XACK after the DB commit succeeds (see ``flush_events``).
    """
    cg = settings.consumer_group
    poll_interval = 0.1  # check buffer sizes frequently
    while not stop_event.is_set():
        # In-flight batch sizes, captured immediately BEFORE each flush call
        # and zeroed once it returns. ``flush_events``/``flush_costs`` drain
        # their buffer as their SECOND statement (``batch = list(events);
        # events.clear()``), before anything that can raise, so by the time an
        # except arm runs the buffer is already empty: the previous
        # ``inc(len(event_buffer))`` incremented the error counter by ZERO on
        # every failure. That is worse than no fix — the loop now survives, so
        # an unenumerated exception was swallowed with a silent counter and one
        # log line, where before it at least tripped ``_log_task_death``.
        # Zeroing after a successful call matters too: if the event flush
        # succeeds and the cost flush then raises, only the cost batch was
        # actually lost.
        in_flight_events = 0
        in_flight_costs = 0
        try:
            elapsed = 0.0
            while elapsed < settings.flush_interval_seconds:
                if (
                    len(event_buffer) >= settings.batch_size
                    or len(cost_buffer) >= settings.batch_size
                ):
                    break
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

            if event_buffer:
                in_flight_events = len(event_buffer)
                n = await flush_events(event_buffer, session_factory, redis_conn, cg)
                in_flight_events = 0
                observability_metrics.events_flushed_total.labels(status="ok").inc(n)
            if cost_buffer:
                in_flight_costs = len(cost_buffer)
                n = await flush_costs(
                    cost_buffer,
                    session_factory,
                    redis_conn,
                    cg,
                    dead_letter_stream=settings.cost_dead_letter_stream,
                )
                in_flight_costs = 0
                observability_metrics.cost_events_flushed_total.labels(status="ok").inc(n)

        except asyncio.CancelledError:
            # Final flush — best-effort; metric label "ok" since cancellation
            # isn't a failure in this path (we're shutting down cleanly).
            # Guarded for the same reason as the broad arm below: an unhandled
            # raise here escapes into the shutdown ``gather(...,
            # return_exceptions=True)``, which swallows it, so the last batch
            # would be lost with ZERO log output. Best-effort means it may
            # fail — it must never fail silently. The in-flight counters are
            # NOT reset first: if the cancellation landed mid-flush, that batch
            # was already drained from the buffer and is genuinely lost, so it
            # still belongs in the error count.
            try:
                if event_buffer:
                    in_flight_events = len(event_buffer)
                    n = await flush_events(event_buffer, session_factory, redis_conn, cg)
                    in_flight_events = 0
                    observability_metrics.events_flushed_total.labels(status="ok").inc(n)
                if cost_buffer:
                    in_flight_costs = len(cost_buffer)
                    n = await flush_costs(
                        cost_buffer,
                        session_factory,
                        redis_conn,
                        cg,
                        dead_letter_stream=settings.cost_dead_letter_stream,
                    )
                    in_flight_costs = 0
                    observability_metrics.cost_events_flushed_total.labels(status="ok").inc(n)
            except Exception:
                _count_failed_flush(in_flight_events, in_flight_costs)
                _log_loop_failure("flush_error_on_shutdown")
            break
        except (SQLAlchemyError, RedisError):  # fmt: skip
            # Whatever was in flight at the moment of failure counts as an
            # error-status flush attempt — count *attempt sizes* so the rate
            # alert (status="error" / total) is accurate.
            _count_failed_flush(in_flight_events, in_flight_costs)
            _log_loop_failure("flush_error_infra", level="warning")
        except Exception:
            # Deliberately broad, and it must stay that way. Previously this
            # arm enumerated (TimeoutError, OSError, KeyError, ValueError,
            # TypeError), so anything outside that tuple — an AttributeError, a
            # RuntimeError, an ArithmeticError (decimal.InvalidOperation is one,
            # and is NOT a ValueError) — escaped the ``while`` and killed the
            # task for the process lifetime. Nothing restarts it:
            # ``main._log_task_death`` only LOGS, and ``/health/ready`` checks
            # engine + redis, not the background tasks, so the pod stays Ready
            # and Live while both buffers grow unbounded and every metering and
            # cost event is lost. One bad batch must cost one cycle, not the
            # flusher. The error-status increments stay so the SLO alert still
            # fires on the rate.
            _count_failed_flush(in_flight_events, in_flight_costs)
            _log_loop_failure("flush_error_unexpected")


async def run_rollup_loop(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    stop_event: asyncio.Event,
    redis_conn: aioredis.Redis | None = None,
) -> None:
    """Periodically run hourly and daily rollups + retention cleanup.

    Leader-elected via Postgres advisory lock — only one replica runs the
    rollups at a time. If the leader's connection dies, leadership transfers to
    whichever surviving replica calls ``is_leader`` first.

    **The wait is leadership-dependent.** The leader waits the full
    ``rollup_interval_seconds`` (that is the rollup cadence); a follower waits only
    ``leader_retry_interval_seconds`` (default 15s) before re-racing for the lock.
    Pre-fix a follower idled the FULL rollup interval, so the leaderless window
    after any event that drops the leader's session was up to one whole interval:
    on 2026-07-13 the pgbouncer-session pod was restarted, both billing pods logged
    ``leader_election_error``, the ROLLUP lock erred at 18:47:41 and no replica
    reacquired until 18:52:42 — 301s with no rollup leader. ``is_leader()`` fails
    CLOSED (a conn-layer error is logged and returns False), so the error path is
    the follower path and gets the same short retry.

    ``redis_conn`` (when provided) lets the leader age-trim the cost stream each
    tick (FIX A1) so its oldest entry stays younger than the idempotency-inbox
    retention — without that bound a NOGROUP replay of a stream entry whose inbox
    row was already pruned would double-count.

    Unlike :func:`run_flush_loop`, rollups read the *whole* raw events table
    for a time window rather than a partitioned stream — N replicas without
    a lock would each run identical UPSERTs (ON CONFLICT keeps results
    correct but wastes IO and contends on the same rows).
    """
    # Settings.leader_election_dsn lets tests + non-prod envs bypass the
    # pgbouncer→session-pooler rewrite. Production leaves it None and we derive
    # from database_url + settings.direct_postgres_host. Already validated at
    # boot in main.lifespan, so a raise here would be a programming error, not a
    # config one.
    direct_dsn = settings.resolve_leader_election_dsn()
    election = AsyncLeaderElection(direct_dsn, ROLLUP_LOCK_KEY)
    # The UTC day we last ran the daily pass for. ``None`` = not yet run in this
    # process (or leadership just (re)acquired) → forces a full gap-aware
    # catch-up. Tracking the day means we scan for gaps once per day, not per
    # tick. The previous wall-clock `last_daily = datetime.now(UTC).date()` reset
    # on every pod boot, so a restart before UTC-midnight permanently skipped a
    # day's rollup and froze the daily tables — gap detection removes that whole
    # failure mode (and fills interior holes a max-watermark can't).
    last_daily_pass: date | None = None

    # How long the NEXT wait lasts, re-derived every cycle from the leadership
    # verdict: leader → full interval, follower (or a cycle that raised before
    # proving leadership) → short retry. It STARTS at the short retry because a
    # freshly-started replica has not proven leadership either — the pre-fix loop
    # opened with a bare ``sleep(rollup_interval_seconds)``, so a pod that came up
    # to replace the dead leader sat out a full 300s before it even LOOKED at the
    # lock. That is the same leaderless window from the other end.
    # Clamped to the rollup interval: a follower must never wait LONGER than the
    # leader's own cadence (a deployment with a 10s rollup interval and a 15s retry
    # would otherwise re-race for the lock more slowly than the leader ticks).
    retry_seconds = min(settings.leader_retry_interval_seconds, settings.rollup_interval_seconds)
    wait_seconds = retry_seconds

    # Consecutive failed LEADER cycles. Reset by any cycle that completes; at
    # MAX_CONSECUTIVE_ROLLUP_FAILURES the lock is handed back (see that
    # constant for why holding it forever is not the safe default).
    consecutive_failures = 0

    def _cycle_ok() -> None:
        nonlocal consecutive_failures
        consecutive_failures = 0
        observability_metrics.rollup_cycles_total.labels(status="ok").inc()

    async def _cycle_failed() -> None:
        nonlocal consecutive_failures, wait_seconds
        consecutive_failures += 1
        observability_metrics.rollup_cycles_total.labels(status="error").inc()
        if consecutive_failures < MAX_CONSECUTIVE_ROLLUP_FAILURES:
            return
        # Wedged: give the lock back so a replica that is NOT wedged can run
        # the rollups. Suppressed because this runs inside the loop's own
        # except arm — a raise here would escape the ``while`` and kill the
        # task, which is the exact failure the broad arm exists to prevent.
        # is_leader() re-acquires on the next cycle if nobody else took it.
        with contextlib.suppress(Exception):
            await election.release()
        observability_metrics.rollup_leadership_handovers_total.inc()
        _log_loop_failure("rollup_leadership_handover", level="warning")
        consecutive_failures = 0
        wait_seconds = retry_seconds

    try:
        while not stop_event.is_set():
            try:
                # Interruptible wait (was a bare asyncio.sleep): shutdown must not
                # have to wait out a full interval either.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                if stop_event.is_set():
                    break

                # Not yet proven leader for THIS cycle → if anything below raises,
                # retry soon rather than parking for a full interval.
                wait_seconds = retry_seconds

                # Refreshed by EVERY replica every tick, both branches, so
                # sum() over the fleet is meaningful: 0 means nobody is rolling
                # up, >1 means split brain. The `finally` covers the raising
                # path too -- is_leader() opens a transaction and can fail, and
                # a replica that cannot tell whether it is leader is not one.
                # DIAGNOSTIC ONLY: never select a replica's coherence gauges on
                # this. A wedged loop reports 1 forever while its gauges freeze;
                # the freshness timestamp is what may be trusted.
                leader = False
                try:
                    leader = await election.is_leader()
                finally:
                    observability_metrics.rollup_is_leader.set(1 if leader else 0)

                if not leader:
                    # Another replica owns the rollup work; force a fresh gap
                    # scan when this replica next (re)acquires leadership.
                    last_daily_pass = None
                    continue

                wait_seconds = settings.rollup_interval_seconds

                # 🔴 MEASURE THE HOURLY STAGE, on EVERY leader tick — not only
                # on the daily pass, because the hourly stage is what the daily
                # pass reads and a stall here is invisible in
                # billing_rollup_lag_days{table="metering_daily"} (rollup_daily
                # builds a day from whatever hourly holds, without requiring all
                # 24 hours). Until this gauge existed nothing measured metering's
                # FIRST aggregation stage, so a metering rollup could sit behind
                # exactly the way cost_daily sat ~3 days behind before
                # 2026-09-05, with every other signal green.
                #
                # ⛔ THE `finally` IS THE WHOLE POINT — DO NOT UNWRAP IT.
                # ⚠️ CORRECTED 2026-09-08: the first version of this block put
                # the measurement AFTER a bare `await rollup_hourly(...)`. That
                # made the gauge un-writable by exactly the event it exists to
                # expose. rollup_hourly raising IS the hourly stage falling
                # behind; on that path control left for the loop's outer except,
                # the gauge was never set, and — being process-lifetime state —
                # it went on exporting the last healthy 1 forever. Rollup starts
                # throwing at 09:00Z, at 15:00Z the true lag is 6, the series
                # still reads 1. An instrument that goes stale-but-plausible
                # precisely when its subject breaks is worse than no instrument:
                # no instrument at least does not vouch for anything.
                #
                # Measuring in `finally` covers the raising path without
                # changing the happy one (still measured after the roll, so a
                # successful roll is reflected immediately). What `finally`
                # CANNOT cover — the measurement query itself failing, an empty
                # table, leadership moving, a wedged pod — is covered by
                # billing_metering_hourly_lag_timestamp_seconds, which advances
                # only when a sample is actually published. Stale timestamp =
                # UNKNOWN. Deliberately NOT a sentinel value on the lag gauge:
                # 0 would read healthier than healthy and a large number would
                # be a false alarm, while a frozen value plus a frozen timestamp
                # is honestly unreadable, which is the true state.
                #
                # The inner suppression is the same rule as the scans below: a
                # measurement must never abort the rollups it rides with, and
                # must never mask the rollup exception it is riding on — hence
                # `finally`, not a second `try` after the call: a plain
                # sequential `await` + `try` runs the measurement only when
                # `rollup_hourly` did NOT raise, which is precisely the case
                # this gauge exists to cover.
                try:
                    await rollup_hourly(session_factory)
                finally:
                    try:
                        await metering_hourly_lag_hours(session_factory)
                    except Exception:
                        logger.warning("metering_hourly_lag_scan_failed", exc_info=True)

                # Licence/tenant coherence + standing budget utilisation. Read
                # only, leader-only, out of band from the cost flush so it adds
                # nothing to the hot ingest path. Suppressed on purpose: this is
                # a measurement, and a measurement must never be able to abort
                # the rollups it rides along with.
                try:
                    await run_attribution_audit(session_factory)
                except Exception:
                    logger.warning("attribution_audit_failed", exc_info=True)

                # R15 estate repair — divergent (unregistered) tenant_id spend,
                # by tenancy.classify_tenant_id's verdict. Same shape as the
                # audit above: read-only, leader-only, suppressed on purpose
                # because a measurement must never abort the rollups it rides
                # along with.
                try:
                    await run_tenant_id_divergence_audit(session_factory)
                except Exception:
                    logger.warning("tenant_id_divergence_audit_failed", exc_info=True)

                # Is the ledger's anti-truncation guard still attached, and has a
                # truncate ever been allowed? The control went in after
                # 2026-09-05 and is armed, but NOTHING watched it -- a sweep of
                # every Prometheus rule file found zero references to the guard,
                # the trigger or billing_delete_audit, so the incident rows were
                # found five days later by going to look. Separately suppressed
                # from the attribution audit above: these are different
                # questions, and a failure of one must not cost the other its
                # pass.
                try:
                    await run_ledger_guard_audit(session_factory)
                except Exception:
                    logger.warning("ledger_guard_audit_failed", exc_info=True)

                # 🔴 HOW DEEP IS THE LEDGER? The guard audit above answers "is the
                # control still attached"; this answers "is the data still there",
                # and on 2026-09-05 the second question went unasked for six days
                # after billing_cost_events was TRUNCATEd. Every FLOW signal was
                # green the whole time because ingest resumed immediately.
                #
                # On EVERY leader tick, not on the daily pass: the loss this
                # detects is instantaneous and was already invisible for six days
                # once. A daily cadence would re-buy up to 24h of that blindness
                # for nothing — the measurement is one `min(created_at)` aggregate.
                #
                # Separately suppressed from the audits above, for the reason each
                # of them is: these are different questions, and a failure of one
                # must not cost another its pass. A measurement may never abort the
                # rollups it rides with.
                try:
                    await cost_events_oldest_age_seconds(session_factory)
                except Exception:
                    logger.warning("cost_events_oldest_age_scan_failed", exc_info=True)

                # Per-emitter ledger freshness. Publishes a LAST-SEEN TIMESTAMP per
                # (service_key, provider) read from billing_cost_events itself, so an
                # emitter that dies is visible even though no pod-local counter for it
                # ever moves again. The existing per-pod
                # billing_cost_events_by_service_total cannot answer this: it is born
                # at pod start and had series for only 6 of the ledger's 12 service
                # keys when measured, so "no series" and "dead emitter" are the same
                # reading. Leader-gated like its neighbours; a follower publishes
                # NOTHING rather than a zero, so consumers must aggregate with max().
                try:
                    async with session_factory() as freshness_session:
                        await publish_emitter_freshness(freshness_session)
                except Exception:
                    logger.warning("emitter_freshness_scan_failed", exc_info=True)

                # Spend sitting on tenant ids the licences registry does not vouch
                # for — money invoiceable to nobody. Read from the LEDGER, not from
                # a counter, for the same reason as its neighbour above:
                # `billing_tenant_unregistered_total` is per-pod-lifetime and read
                # 0 on both replicas on 2026-09-12 while the ledger carried $22.89
                # over 903 rows on three such ids, because they last emitted three
                # days before the pods started. A restart erased the only
                # instrument that could report a standing condition. Leader-gated
                # like its neighbours; a follower publishes NOTHING rather than a
                # zero, so consumers must aggregate with max(). Reporting only —
                # it never rejects, because resolution deliberately fails open.
                try:
                    async with session_factory() as unregistered_session:
                        await publish_unregistered_tenant_spend(unregistered_session)
                except Exception:
                    logger.warning("unregistered_tenant_spend_scan_failed", exc_info=True)

                # Spend billed against a tenant `cost_flusher` flagged
                # `billed_while_archived` — real money for compute already
                # spent on an account the licences registry says is archived.
                # Read from the LEDGER, not from
                # `billing_cost_events_billed_while_archived_total`, for the
                # same reason as its neighbour above: that counter is
                # per-pod-lifetime and reads 0 after every restart regardless
                # of what the ledger holds. Leader-gated like its neighbours;
                # a follower publishes NOTHING rather than a zero, so
                # consumers must aggregate with max(). Reporting only — it
                # never rejects, because the flusher already made that call.
                try:
                    async with session_factory() as archived_session:
                        await publish_archived_tenant_spend(archived_session)
                except Exception:
                    logger.warning("archived_tenant_spend_scan_failed", exc_info=True)

                # FIX A1: age-bound BOTH ingest streams every leader tick (cheap +
                # idempotent), independent of whether a daily roll happened, so
                # each stream's oldest entry is always younger than the inbox
                # retention horizon. Guarded so a Redis hiccup can't abort the
                # DB rollups below — the loop's outer RedisError handler would
                # otherwise skip the daily pass.
                if redis_conn is not None:
                    try:
                        await trim_stream(redis_conn, settings.costs_stream)
                        await trim_stream(
                            redis_conn,
                            settings.events_stream,
                            max_age_hours=EVENTS_STREAM_MAX_AGE_HOURS,
                        )
                    except RedisError:
                        logger.warning("stream_trim_failed", exc_info=True)

                today = datetime.now(UTC).date()
                if last_daily_pass == today:
                    # Daily pass already done for today; keep hourly fresh only.
                    # Still a completed leader cycle — count it and clear the
                    # consecutive-failure streak.
                    _cycle_ok()
                    continue

                cap_days = min(settings.retention_days, MAX_CATCHUP_DAYS)
                floor = today - timedelta(days=cap_days)

                if last_daily_pass is None:
                    # First pass after (re)acquiring leadership: refill any hourly
                    # buckets a rollup-loop outage left empty (gap-aware), BEFORE
                    # the daily pass reads them — rollup_hourly only ever does the
                    # previous hour, so an interior hole never self-heals otherwise.
                    floor_dt = datetime.now(UTC) - timedelta(days=cap_days)
                    for hour in await missing_event_hours(session_factory, floor_dt):
                        await rollup_hourly(session_factory, hour)

                # ⭐ Re-roll hours that received a metering event AFTER they had
                # closed. On EVERY daily pass, not only the first, and BEFORE
                # catchup_daily below reads metering_hourly.
                #
                # 🔴 THIS IS THE OTHER HALF OF EVENT-TIME BUCKETING. rollup_hourly
                # now counts an event into the hour its PRODUCER stamped it in, so
                # a stalled consumer can deliver a row for an already-rolled hour —
                # and the hourly UPSERT REPLACES a bucket rather than adding to it,
                # so an hour nothing revisits stays short FOREVER. That would be a
                # silent under-count, strictly worse than the wrong-bucket defect
                # being fixed. missing_event_hours cannot see it (the bucket
                # exists, it is merely incomplete); late_event_hours can.
                #
                # ⚠️ THE FLOOR IS THE DAILY RECONCILE WINDOW, NOT RETENTION, and
                # that is deliberate. catchup_daily below re-rolls only the
                # trailing `daily_reconcile_days` complete days; restating an
                # hourly bucket whose day it will not revisit would leave
                # metering_hourly and metering_daily — the table invoices are built
                # from — disagreeing about the same hour. The `+ 1` day of slack
                # covers hours at the oldest edge of that window. Anything older is
                # the residual this change does not close; it is the metering twin
                # of billing_cost_event_ts_backdated_past_rollup_total.
                reconcile_floor = datetime.now(UTC) - timedelta(
                    days=max(settings.daily_reconcile_days, 0) + 1
                )
                for hour in await late_event_hours(session_factory, reconcile_floor):
                    await rollup_hourly(session_factory, hour)

                # Keep a rolling runway of cost_events partitions. cost_events is
                # monthly RANGE-partitioned; an INSERT with no covering partition
                # hard-fails. 0012 documented a gitops cron for this that was never
                # built, so the daily pass drives it (idempotent CREATE IF NOT EXISTS).
                await ensure_cost_event_partitions(
                    session_factory, settings.cost_partition_months_ahead
                )

                # Daily pass rolls the UNION of two date sets, both bounded by floor
                # and idempotent (rollup_daily UPSERTs the recomputed sum):
                #  (a) missing_daily_dates — complete days in [floor, yesterday] with
                #      source data but no daily row: a newly-complete yesterday AND any
                #      interior hole a max(bucket_date) watermark would skip.
                #  (b) a trailing reconciliation window of the last N complete days.
                #      missing_daily_dates only fills ABSENT dates, so a row first
                #      written from incomplete hourly (early "yesterday" roll) and later
                #      corrected by a hourly backfill is present-but-stale and would
                #      never reconcile. Re-rolling it from the now-complete hourly fixes
                #      the undercount with zero double-count risk.
                # ⛔ DO NOT collapse this back into a bare
                # ``for day in sorted(missing | window)``. Three things live in
                # catchup_daily that did not live here, and each is load-bearing:
                #   * the boundary (last CLOSED UTC day) is derived ONCE and the
                #     reconcile window is anchored on it, so the window and
                #     missing_daily_dates cannot drift apart across UTC midnight;
                #   * the batch is capped and rolled OLDEST-FIRST, which keeps the
                #     alembic-0017 TRUNCATE guard armed while a backlog drains
                #     (newest-first jumps max(bucket_date) to the head of the
                #     backlog and disarms it mid-drain);
                #   * it distinguishes "succeeded" from "finished".
                catchup = await catchup_daily(
                    session_factory,
                    floor=floor,
                    reconcile_days=settings.daily_reconcile_days,
                    max_days=settings.rollup_catchup_max_days,
                )
                rolled_any = bool(catchup.rolled)

                # 🔴 EMIT THE WATERMARK, NOT JUST SUCCESS. Runs on every daily
                # pass, unconditionally — including a pass that rolled nothing,
                # which is precisely the state that must still be measurable.
                # A cycle counter, a completion log line and a Ready pod were all
                # green throughout an incident in which this number was 3.
                # Suppressed like the gap scan below and for the same reason: a
                # measurement must never abort the rollups it rides with.
                try:
                    await daily_rollup_lag_days(session_factory)
                except Exception:
                    logger.warning("daily_rollup_lag_scan_failed", exc_info=True)

                # Actor-axis cardinality (billing_cost_actor_daily_distinct_actors).
                # REPORTING ONLY, same suppression rule as its siblings above/below: a
                # measurement must never abort the rollups it rides with.
                try:
                    await daily_actor_axis_cardinality(session_factory)
                except Exception:
                    logger.warning("daily_actor_axis_cardinality_scan_failed", exc_info=True)

                # (c) Gap REPORT, run AFTER the roll so it measures the settled
                #     state. Neither set above can see a day with no source rows
                #     — both are derived from the source tables — so a date that
                #     is simply absent from cost_daily/metering_daily was
                #     undetectable until this call. Its return value is
                #     deliberately NOT unioned into the loop above and must never
                #     be: rolling a day whose events are gone DELETES the
                #     surviving bucket instead of rebuilding it, which is how
                #     2026-09-02..09-04 were lost. Report, do not repair.
                #
                # Suppressed for exactly the reason `run_attribution_audit` above
                # is: a measurement must never be able to abort the rollups it
                # rides along with. Its gauge and its log are already wrapped, but
                # its two SELECTs were not — so one SQLAlchemyError here would
                # escape to the loop's outer handler, leave `last_daily_pass`
                # unset, and skip cleanup_old_events, the processed-message prune,
                # drop_old_cost_partitions and the audit purge for the whole
                # cycle; repeated failures would then hand back the advisory lock
                # via `_cycle_failed`. A gap report is worth none of that.
                try:
                    await daily_rollup_date_gaps(session_factory, floor)
                except Exception:
                    logger.warning("daily_rollup_gap_scan_failed", exc_info=True)

                if rolled_any:
                    await cleanup_old_events(session_factory, settings.retention_days)
                    # Prune the cost-stream idempotency inbox (own short horizon,
                    # independent of the raw-event retention window).
                    await cleanup_old_processed_messages(session_factory)
                    # cost_events retention is partition-native: DROP whole aged
                    # partitions (instant reclaim, no dead-tuple bloat) rather than
                    # row-DELETE a 100MB+ table. metering_events (plain table) stays
                    # on DELETE inside cleanup_old_events above.
                    await drop_old_cost_partitions(session_factory, settings.retention_days)
                    # The per-actor rollup has its OWN, far longer window and is
                    # purged on the SAME tick — deliberately adjacent to the
                    # line above rather than in a loop of its own. Two retention
                    # jobs on two schedules is two things to notice have stopped;
                    # one tick means a leader that is alive is running both.
                    #
                    # ⛔ IT IS NOT DERIVED FROM `retention_days` AND MUST NOT BE.
                    # That window is 90 days and DROPs raw partitions; this one
                    # is 25 months and holds the per-member chargeback evidence
                    # for invoices that have already been ISSUED. Collapsing them
                    # would age out the evidence three months after the invoice
                    # it supports, and the raw events needed to rebuild it are
                    # gone by then.
                    #
                    # Placed INSIDE `if rolled_any:` with its siblings: retention
                    # is a consequence of a pass that actually rolled, so a
                    # wedged rollup must not keep deleting history it is no
                    # longer replacing.
                    await purge_old_actor_rollup(
                        session_factory, settings.actor_rollup_retention_months
                    )
                    await purge_old_usage_rollup(
                        session_factory, settings.actor_rollup_retention_months
                    )
                    # Audit-log retention runs once a day on the same tick as the
                    # raw-event cleanup. Independent retention window (default
                    # 180d vs 90d) because audit rows are smaller and compliance
                    # wants longer history.
                    async with session_factory() as audit_session:
                        await audit_svc.purge_old_audit_log(
                            audit_session, settings.audit_retention_days
                        )

                # ⚠️ THE CAP IS NOT A CEILING ON CATCH-UP — this line is what
                # makes that true. ``last_daily_pass`` parks the daily pass for a
                # whole calendar day, so marking the day done after a capped pass
                # would leave the remainder un-rolled until tomorrow and rebuild
                # the original defect (a pass that advances a bounded number of
                # buckets, then waits a day) with a larger constant. Leaving it
                # unset makes the next cycle — one ``rollup_interval_seconds``
                # away, not one day — resume draining. The pass is still counted
                # as a healthy cycle: it did not fail, it is not finished, and
                # ``billing_rollup_catchup_remaining_days`` is where that
                # difference is published.
                if catchup.complete:
                    last_daily_pass = today
                _cycle_ok()

            except asyncio.CancelledError:
                break
            except (SQLAlchemyError, RedisError):  # fmt: skip
                # Counted BEFORE it is logged: structlog renders in the log
                # call, so a broken handler there would escape this arm and
                # kill the task. See _log_loop_failure.
                await _cycle_failed()
                _log_loop_failure("rollup_error_infra", level="warning")
            except Exception:
                # Deliberately broad, and it must stay that way — same reasoning
                # as run_flush_loop, one degree worse. This arm used to enumerate
                # (TimeoutError, OSError, KeyError, ValueError, TypeError); an
                # AttributeError / RuntimeError / ArithmeticError escaped the
                # ``while``, ran the ``finally: election.release()`` below and
                # handed the advisory lock to the next replica — which picks up
                # the same poison input and dies the same way. That is a
                # fleet-wide cascade to zero rollup leaders, with every pod still
                # Ready (``main._log_task_death`` only logs; ``/health/ready``
                # never looks at the background tasks). Retention cleanup and
                # cost-partition creation stop with it. One bad cycle must cost
                # one cycle. Leadership is kept while we keep looping, but NOT
                # forever: a leader that fails MAX_CONSECUTIVE_ROLLUP_FAILURES
                # cycles in a row hands the lock back (``_cycle_failed``), so a
                # replica-local wedge cannot freeze every rollup in the fleet
                # behind a pod that is still Ready. That failure was invisible
                # too — this loop had no metric at all until
                # ``rollup_cycles_total``.
                await _cycle_failed()
                _log_loop_failure("rollup_error_unexpected")
    finally:
        await election.release()
