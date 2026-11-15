"""Hourly + daily rollup engine + retention cleanup + cost_events partitions."""

import re
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

import structlog
from sqlalchemy import Date, case, delete, func, select, text, union
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import observability.metrics as observability_metrics
from models.db import (
    NULL_ACTOR_ID,
    NULL_APP_ID,
    NULL_LICENSE_ID,
    NULL_PLAN_ID,
    NULL_PROJECT_ID,
    NULL_SUBJECT_ID,
    CostActorDaily,
    CostDaily,
    CostEvent,
    CostUsageDaily,
    MeteringDaily,
    MeteringEvent,
    MeteringHourly,
    ProcessedCostMessage,
    ProcessedEventMessage,
    RequestQuotaConfig,
    classify_cost_attribution,
    cost_source_marks_refusal,
)
from services.funding import FUNDING_UNKNOWN
from services.license_resolver import parse_service_key_app_map
from services.metering_status import denied_sql_expr
from settings import get_settings

logger = structlog.stdlib.get_logger(__name__)

#: ``classid`` half of the two-int ``pg_advisory_xact_lock`` key that serialises
#: concurrent rollers of one ``bucket_date`` (see rollup_daily). The ``objid``
#: half is ``target_date.toordinal()`` — 739,000-ish for the 2020s, comfortably
#: inside int4 until the year 5874.
#:
#: ⚠️ Advisory-lock keys are scoped to the DATABASE, not to the service, and
#: leader_election.py records that this repo's keys are inherited verbatim from
#: metering-service and are therefore NOT unique across services sharing a
#: database. This value is deliberately not in that inherited range. Collisions
#: cost throughput, never correctness — a false conflict serialises two
#: transactions that did not need serialising, and each holder releases at
#: commit.
ROLLUP_DAILY_LOCK_CLASSID = 0x62_6C_64_31  # 'bld1' — billing daily rollup, v1


def _resolved_event_time(model: Any) -> Any:
    """``coalesce(event_ts, created_at)`` — the ONE spelling of "when did this happen".

    A factory rather than two literal COALESCEs, because the two money axes of a
    single request are bucketed by the two expressions it produces
    (:data:`cost_event_time` and :data:`metering_event_time`) and they must never
    be able to drift apart. That drift IS the defect this exists to prevent: the
    cost bus was moved onto producer time by alembic 0028 and the metering bus was
    left on ingest time, so one request emitted at 23:59:50 on the last day of a
    month could have its LLM cost billed in month M and its per-request surcharge
    in month M+1 — two immutable, gaplessly-numbered invoices, neither amendable.

    ``event_ts`` is the PRODUCER's event time; ``created_at`` is INGEST time.
    Prefer the producer's, fall back to ingest when it is NULL — which is every
    row written before the respective migration (never backfilled; that history is
    gone) and any row whose wire ``timestamp`` was unusable.
    """
    return func.coalesce(model.event_ts, model.created_at)


#: THE ONE resolved event-time expression for ``billing_cost_events``. Every
#: question this module asks about "when did this cost happen" must be asked
#: through THIS object, never by re-spelling the COALESCE at the call site.
#:
#: WHY A SINGLE SHARED OBJECT. ``rollup_daily`` derives a bucket_date from it AND
#: gates a DELETE of that bucket on it. If the gate and the thing it gates resolve
#: the timestamp differently, the guard answers a question about a different set of
#: rows than the one being rewritten — a defect class this repo has already paid
#: for (see the guarded-delete essay in :func:`rollup_daily`, written after the
#: 2026-09-02..09-04 cost buckets were destroyed). One object means a future edit
#: physically cannot move one and leave the other.
#:
#: ``event_ts`` is the PRODUCER's event time (alembic 0028) and ``created_at`` is
#: INGEST time. Prefer the producer's, fall back to ingest when it is NULL —
#: which is every row written before 0028 (never backfilled; that history is
#: gone) and any row whose wire ``timestamp`` was unusable. ⚠️ The fallback is
#: load-bearing, not cosmetic: two of the twelve emitting ``service_key``s
#: (``w2v-video-analyzer`` and ``w2v-video-compiler``, the #2 spender) were never
#: confirmed to use the shared emitter library, so "every row has event_ts" is an
#: assumption until the fallback counter reads zero.
cost_event_time = _resolved_event_time(CostEvent)

#: THE ONE resolved event-time expression for ``billing_metering_events`` — the
#: metering bus's twin of :data:`cost_event_time`, built by the SAME factory so
#: the two clocks cannot be edited apart.
#:
#: 🔴 WHY THIS HAD TO EXIST. Metering is the OTHER money axis of the same request:
#: ``request_count`` is multiplied by the per-app ``AppCostRate`` and feeds the
#: per-request surcharge in ``services.billing.compute_billed_line``. Bucketing it
#: on ``created_at`` while the cost half bucketed on producer time is what let ONE
#: request's money land on TWO invoices.
#:
#: ⚠️ The fallback is load-bearing here in a way it is not on the cost side.
#: ``billing:events`` has TWO producers with different guarantees — fr-backend
#: (~99.5% of rows, via a globally-installed ``withUsageSpans``) and license-authz
#: — and until ``billing_metering_event_ts_fallback_total`` exists and reads zero
#: for both, "every metering row carries a wire ``timestamp``" is an assumption.
#: A NULL row must keep billing exactly where it billed before, never drop out.
metering_event_time = _resolved_event_time(MeteringEvent)


def _naive_utc_hour(expr: Any) -> Any:
    """``date_trunc('hour', …)`` of a timestamptz, resolved in UTC. Returns NAIVE.

    date_trunc on a timestamptz truncates in the SESSION timezone, so on a
    non-UTC session (prod pgbouncer = Europe/Amsterdam) a fractional-hour zone
    would land on the wrong instant. The inner ``timezone('UTC', …)`` converts to
    a naive UTC wall-clock first, making the truncation TZ-independent.

    Kept separate from :func:`_event_hour_expr` because two naive hours of the
    same shape can be COMPARED directly — which is how :func:`late_event_hours`
    asks "did this row arrive after its own hour closed" without doing interval
    arithmetic on an untyped SQL function expression.
    """
    return func.date_trunc("hour", func.timezone("UTC", expr))


def _event_hour_expr() -> Any:
    """The UTC hour bucket a raw metering event belongs to. ONE definition.

    Shared by :func:`missing_event_hours` and :func:`late_event_hours` so the two
    gap-finders and the roller cannot answer about different buckets.

    Whole-hour session zones happen to coincide with the UTC hour rollup_hourly
    stores, but this is TZ-independent regardless: :func:`_naive_utc_hour` gives
    the naive UTC hour and the outer ``timezone('UTC', …)`` turns it back into the
    aware UTC instant, so ``EXCEPT`` still matches ``bucket_hour`` by instant and
    the value handed to ``rollup_hourly`` is a real ``window_start``.
    """
    return func.timezone("UTC", _naive_utc_hour(metering_event_time))


# Idempotency-inbox rows only matter while a Redis stream message can still be
# redelivered (PEL retry / NOGROUP replay). BOTH the cost stream and the events
# stream are bounded by COUNT (XADD MAXLEN ~ 100000) by the producer, NOT by age,
# so under low volume either can retain entries far older than this prune horizon.
# A NOGROUP recovery recreates the group at id=0 and mass-replays every retained
# entry; any replayed entry whose inbox row had already been pruned would be
# re-processed: for the cost stream that re-inserts cost_events AND re-debits
# budget; for the events stream it re-inserts metering_events AND additively
# double-counts request_count/denied_count + double-increments the quota counter.
# To make that impossible the rollup loop / consumer also AGE-trim EACH stream to
# its ``*_STREAM_MAX_AGE_HOURS`` (< this retention), so an entry old enough to
# still live in a stream ALWAYS still has its inbox row. INVARIANT (must hold):
# both COST_STREAM_MAX_AGE_HOURS and EVENTS_STREAM_MAX_AGE_HOURS < the retention
# horizon below.
PROCESSED_MESSAGE_RETENTION_DAYS = 2

# Age ceilings the rollup loop / consumer enforce on each stream via XTRIM MINID.
# Both must stay strictly below the inbox retention (PROCESSED_MESSAGE_RETENTION_DAYS,
# 48h) so any entry still replayable from a stream is guaranteed to still have its
# dedup row in processed_cost_messages / processed_event_messages — see the
# invariant above and FIX A1.
COST_STREAM_MAX_AGE_HOURS = 36
EVENTS_STREAM_MAX_AGE_HOURS = 36


async def rollup_hourly(
    session_factory: async_sessionmaker[AsyncSession],
    window_start: datetime | None = None,
) -> int:
    """Aggregate raw metering_events into metering_hourly for one hour window.

    ``window_start`` defaults to the previous complete hour (UTC), preserving
    the once-per-tick forward behaviour. The gap-aware hourly catch-up in
    :func:`services.aggregator.run_rollup_loop` passes explicit past hours to
    backfill a rollup-loop outage (rollup_hourly otherwise only ever does the
    immediately-previous hour); the UPSERT on ``uq_hourly`` makes re-runs
    idempotent. ``window_start`` must be aligned to a UTC hour boundary.

    ⭐ THE HOUR IS THE EVENT'S, NOT THE INGEST'S (:data:`metering_event_time`).
    An event is counted into the hour its PRODUCER stamped it in, falling back to
    ``created_at`` only when no usable ``event_ts`` was asserted. Bucketing on
    ingest time put a 23:59:50 request whose metering flushed at 00:00:05 into the
    next hour — and, on the last day of a month, onto the next INVOICE, while the
    same request's LLM cost (alembic 0028) stayed on the correct one.

    ⚠️ THE COMPANION GUARANTEE, WITHOUT WHICH THIS FIX LOSES MONEY. Moving the
    bucket backwards in time means a row can arrive for an hour that has already
    been rolled, and the hourly UPSERT REPLACES a bucket's counts rather than
    adding to them — so an already-rolled hour that is never revisited would
    simply never see that row. The forward roller covers lag up to ~1h for free
    (``run_rollup_loop`` re-rolls the previous hour on every
    ``rollup_interval_seconds`` tick, ~12 times per hour). Beyond that,
    :func:`late_event_hours` names the hours that received a row AFTER they closed
    and the daily pass re-rolls exactly those, inside the same trailing window
    ``catchup_daily`` re-rolls days in — so hourly and daily are restated
    together. ⛔ Do not remove either half: with event-time bucketing and no
    re-roll, a stalled consumer under-counts SILENTLY, which is strictly worse
    than the mis-bucketing this replaces.
    """
    if window_start is None:
        bucket = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        window_start = bucket - timedelta(hours=1)
    else:
        bucket = window_start + timedelta(hours=1)

    async with session_factory() as session:
        # Group raw events by dimensions, coalescing NULLs to sentinels so
        # denied events (which have NULL license/app/plan) survive the NOT NULL
        # constraint on metering_hourly.  Reuse the same expression objects in
        # SELECT and GROUP BY so SQLAlchemy emits identical bind parameters.
        _lic = func.coalesce(MeteringEvent.license_id, NULL_LICENSE_ID)
        _sub = func.coalesce(MeteringEvent.subject_id, NULL_SUBJECT_ID)
        _app = func.coalesce(MeteringEvent.app_id, NULL_APP_ID)
        _plan = func.coalesce(MeteringEvent.plan_id, NULL_PLAN_ID)
        # THE denied predicate — one expression object, reused by both the
        # billed request_count and the denied_count so the two can never
        # disagree about what "denied" means. The vocabulary is
        # event_flusher's: license-authz writes `allowed` / `denied…`
        # decisions. NOTE the OTHER writer: fr writes HTTP codes (`200`,
        # `201`, `404`, `422`) into the same column, and those are ordinary
        # BILLABLE requests. So the billable count is "everything that is NOT
        # denied", never "status == 'allowed'" — the latter would silently
        # stop billing every fr request, and billing has no refund path in
        # either direction.
        #
        # ⛔ SHARED WITH event_flusher's monthly aggregation via
        # services.metering_status — see that module's docstring for the
        # money-adjacent bug this closes: event_flusher used to spell "denied"
        # differently and, for a third case (an HTTP code), incremented
        # NOTHING. `denied_sql_expr` is now the ONLY place the "denied%"
        # literal is written.
        _denied = denied_sql_expr(MeteringEvent.status)
        stmt = (
            select(
                MeteringEvent.tenant_id,
                # tenant_name is NOT part of uq_hourly. A tenant renamed within
                # the window would otherwise split one conflict-key into two rows
                # with different tenant_name — the ON CONFLICT UPSERT then affects
                # the same row twice in one statement and Postgres aborts the whole
                # rollup ("command cannot affect row a second time"). Aggregate it
                # (max) so there is exactly one row per constraint key.
                func.max(MeteringEvent.tenant_name).label("tenant_name"),
                _lic.label("license_id"),
                _sub.label("subject_id"),
                _app.label("app_id"),
                _plan.label("plan_id"),
                MeteringEvent.service_key,
                # BILLED request count — denied requests EXCLUDED. request_count
                # is money: services.billing.generate_billing multiplies it by
                # the per-app AppCostRate AND feeds it to the per-request
                # surcharge in compute_billed_line, so a refused request was
                # being charged twice over. Expressed as (all - denied) rather
                # than a filtered count so it stays exact even if `status` ever
                # arrives NULL (a NULL would make `NOT LIKE` unknown and drop
                # the row from a filtered count — an UNDER-charge, equally
                # unrecoverable). Invariant: request_count + denied_count ==
                # rows in the window.
                (func.count() - func.count().filter(_denied)).label("total"),
                func.count().filter(_denied).label("denied"),
                func.count()
                .filter(MeteringEvent.status.in_(["denied_budget", "denied_quota"]))
                .label("budget_denied"),
            )
            .where(
                # ⭐ THE FIX: an event belongs to the hour it HAPPENED in, not the
                # hour billing happened to receive it. Resolved through
                # `metering_event_time`, the SAME shared expression
                # `missing_event_hours` and `late_event_hours` read, so the roller
                # and its two gap-finders can never disagree about which bucket a
                # row belongs to.
                #
                # Sargable LOWER bound on the bare `created_at` first, purely as an
                # `ix_events_created` hint. It is SOUND for the same reason the
                # cost side's is, and by the same write-side invariant:
                # `_parsers.parse_event_ts` refuses any event time later than the
                # instant billing received the message, so
                # `event_ts <= received_at <= created_at` for every stored row and
                # `metering_event_time >= window_start` implies
                # `created_at >= window_start`. ⛔ If that refusal is ever relaxed,
                # this bound silently starts dropping rows.
                MeteringEvent.created_at >= window_start,
                # ⛔ THERE IS DELIBERATELY NO `created_at < bucket` BOUND, AND IT
                # MUST NOT BE RE-ADDED — it is exactly what made this query blind
                # to the bug being fixed. An event emitted at 23:59:50 and ingested
                # at 00:00:05 belongs to the EARLIER hour (and, at a month
                # boundary, the earlier INVOICE) by event time, but its
                # `created_at` is past `bucket`, so the bound excluded the very row
                # the resolved predicate exists to capture. Any finite replacement
                # ("bucket + a lag tolerance") silently drops whatever exceeds the
                # tolerance, and the observed consumer stall was 62 HOURS.
                metering_event_time >= window_start,
                metering_event_time < bucket,
            )
            .group_by(
                MeteringEvent.tenant_id,
                _lic,
                _sub,
                _app,
                _plan,
                MeteringEvent.service_key,
            )
        )
        result = await session.execute(stmt)
        rows = result.all()

        count = len(rows)
        if rows:
            hourly_rows = [
                {
                    "tenant_id": r.tenant_id,
                    "tenant_name": r.tenant_name,
                    "license_id": r.license_id,
                    "subject_id": r.subject_id,
                    "app_id": r.app_id,
                    "plan_id": r.plan_id,
                    "service_key": r.service_key,
                    "bucket_hour": window_start,
                    "request_count": r.total,
                    "denied_count": r.denied,
                    "budget_denied_count": r.budget_denied,
                }
                for r in rows
            ]
            upsert = pg_insert(MeteringHourly).values(hourly_rows)
            upsert = upsert.on_conflict_do_update(
                constraint="uq_hourly",
                set_={
                    "request_count": upsert.excluded.request_count,
                    "denied_count": upsert.excluded.denied_count,
                    "budget_denied_count": upsert.excluded.budget_denied_count,
                },
            )
            await session.execute(upsert)

        await session.commit()

    if count:
        logger.info("hourly_rollup_completed", bucket=window_start.isoformat(), rows=count)
    return count


async def rollup_daily(
    session_factory: async_sessionmaker[AsyncSession],
    target_date: date | None = None,
) -> int:
    """Aggregate metering_hourly + cost_events into the daily tables.

    ``target_date`` defaults to yesterday (UTC), preserving the original
    once-a-day forward-rollup behaviour. The watermark-driven catch-up loop in
    :func:`services.aggregator.run_rollup_loop` passes explicit past dates to
    backfill days missed during an outage; the UPSERT on ``uq_daily`` /
    ``uq_cost_daily`` makes any re-run (or overlap) idempotent.
    """
    if target_date is None:
        target_date = (datetime.now(UTC) - timedelta(days=1)).date()

    # Sargable UTC bounds for the day. Added ALONGSIDE the cast(... AS Date)
    # predicates below purely as a partition-prune / index hint: created_at is the
    # RANGE-partition key on cost_events, and wrapping it in timezone('UTC', …)+cast
    # defeats pruning (Postgres scans every monthly partition). These raw
    # inequalities on the bare timestamptz let the planner prune to the one or two
    # partitions the UTC day touches. Because timestamptz comparisons are by
    # absolute instant (session-TZ independent), [day_start, day_end) selects
    # exactly the instants whose UTC date == target_date — the SAME set the cast
    # filters — so the bounds never change the result, only the plan. The cast is
    # kept as the explicit exact-date correctness guarantee (belt-and-suspenders).
    day_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    async with session_factory() as session:
        # ── Serialise concurrent rollers of THIS ONE bucket ──────────────────
        # Two callers reach rollup_daily and they are NOT mutually excluded:
        # services.aggregator.run_rollup_loop is leader-elected (one replica),
        # but services.reprice.reprice_cost_events calls it from an HTTP handler
        # (reprice.py, once per batch-date and again per window-date) under no
        # lock at all — so an operator-triggered reprice can run concurrently
        # with the leader's nightly pass, and two reprices can run concurrently
        # with each other.
        #
        # Without this lock the interleaving is not corrupting but it is not
        # free either: under READ COMMITTED each DELETE takes its snapshot at
        # statement start, so B's DELETE (started before A's INSERT committed)
        # does not see A's new rows, and B's UPSERT then re-adds its own — both
        # recomputed from the same source, so the values converge. What does NOT
        # converge is a grouping-key combo A wrote and B did not: it survives B's
        # blind delete as a stale over-counted row, which is the exact failure
        # the delete-then-rebuild exists to prevent. Serialising makes the second
        # run a clean full rebuild rather than a partial overlay.
        #
        # xact-scoped (pg_advisory_xact_lock), NOT session-scoped: the DSN here
        # can be pgbouncer in TRANSACTION pooling mode, where a session-level
        # lock is taken on a server connection that is handed to another client
        # at commit — the reason leader_election.py forces a DIRECT connection
        # for its session lock (see its module docstring, "Session mode, not
        # transaction mode"). A transaction lock is held and released inside one
        # transaction on one server connection, so it is correct under either
        # pooling mode. It is released by the commit below (or by a rollback),
        # so a crash cannot strand it.
        #
        # DEADLOCK-FREE BY CONSTRUCTION: one call locks exactly ONE key and holds
        # no other lock when it asks, so no two transactions can hold-and-wait in
        # a cycle. This is why the loop in catchup_daily rolls each date in its
        # own transaction rather than locking a range.
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:cls, :obj)"),
            {"cls": ROLLUP_DAILY_LOCK_CLASSID, "obj": target_date.toordinal()},
        )

        # Delete-then-rebuild this day's rollup rows — but the delete is GUARDED.
        # The ordering below (SELECT first, DELETE second, inside the guard) is
        # the whole point and must not be "tidied" back into an unconditional
        # pre-delete. Read all three paragraphs before touching it.
        #
        # WHY the delete exists: the per-table UPSERTs only touch grouping-key
        # combos that STILL produce a row for target_date. A combo whose events
        # all moved to a different day (a UTC-bucketing restatement, a late event
        # shifting a boundary) would otherwise leave a STALE over-counted row the
        # UPSERT never revisits. Clearing the day first is what makes forward
        # rollups and reprice restatements convergent — see
        # tests/test_rollup_timezone.py::test_restatement_deletes_stale_rows_
        # when_event_moves_day, which is the control on this half.
        #
        # WHY it may not be unconditional: `daily_reconcile_days` re-rolls a
        # trailing window of ALREADY-COMPLETE days on every pass, and the raw
        # sources it rebuilds from are transient — cleanup_old_events and
        # drop_old_cost_partitions age them out, and an operator can truncate
        # them outright. Once a day's source is gone, an unconditional delete
        # lands and the guarded re-insert does not, so the reconcile ERASES a
        # good historical bucket it has no way to rebuild. Not hypothetical:
        # billing_cost_events was truncated 2026-09-05T07:01:56Z and the next
        # pass destroyed the 2026-09-02..09-04 billing_cost_daily buckets —
        # exactly the trailing 3-day window — stranding max(bucket_date) at
        # 2026-09-01. The daily tables WERE the last copy; those days are gone.
        #
        # THE DISCRIMINATOR. Both cases look identical from here ("zero source
        # rows for target_date"), so the guard cannot be "did I get rows". It
        # asks a different question: is this day still INSIDE the source's
        # retained history? If the source holds any row older than the end of
        # this day, then history reaches back through it and an empty result is
        # a real measurement — the day genuinely has nothing, clear it. If the
        # source's oldest row is NEWER than this day, the day has been truncated
        # or aged out from under us, an empty result measures nothing, and the
        # existing bucket is the only surviving record — leave it alone.
        #
        # The residual risk is deliberately one-directional: a restatement that
        # moves every event of the OLDEST retained day forward keeps a stale
        # bucket instead of dropping it. That is a wrong number an operator can
        # delete by hand. The alternative destroys a number nothing can rebuild.

        # Evaluated per table: metering_daily reads metering_hourly, cost_daily
        # reads cost_events — two different sources on two different retention
        # clocks, so one being truncated must not license clearing the other's
        # day. Coverage deliberately ignores excluded_from_billing: a parked row
        # is not billable but it still PROVES the history was not truncated.
        metering_covered = (
            await session.execute(
                select(MeteringHourly.id).where(MeteringHourly.bucket_hour < day_end).limit(1)
            )
        ).first() is not None
        # ⚠️ THIS GATE MUST RESOLVE THE TIMESTAMP THE SAME WAY THE BUCKET DOES.
        # It asks "does retained history reach back through this day?" about the
        # very rows the DELETE below will rewrite, and those rows are selected by
        # `cost_event_time`, not by `created_at`. Asking on `created_at` while
        # bucketing on `cost_event_time` would make the guard answer about a
        # different population than the one it gates — so it reads the shared
        # expression, like every other event-time question in this module.
        #
        # ⚠️ KNOWN NARROW RESIDUAL, recorded rather than silently shipped. Because
        # `event_ts <= created_at` always (the flusher refuses a future event
        # time), resolving through the COALESCE makes this gate fire on slightly
        # MORE rows than the old `created_at` form: a midnight-crossing row stored
        # in month M+1's partition now proves coverage of a day in month M. If
        # month M's partition has been DROPPED by retention, that crosser would
        # license a delete the re-insert cannot fully rebuild — exactly the
        # 2026-09-05 failure shape. It is unreachable today because every caller
        # is floored at retention (`catchup_daily(floor=…)` and
        # `missing_daily_dates(floor=…)`), so no date whose partition is gone is
        # ever rolled. ⛔ If a caller is ever given a date older than the
        # retention floor, close this first: the conservative form is
        # `cost_event_time < day_start` (history reaches back PAST this day), which
        # is both grain-consistent and strictly safer, and is a deliberate,
        # separately-tested behaviour change rather than a drive-by here.
        cost_covered = (
            await session.execute(select(CostEvent.id).where(cost_event_time < day_end).limit(1))
        ).first() is not None

        stmt = (
            select(
                MeteringHourly.tenant_id,
                # Aggregate (not group by): tenant_name is not part of uq_daily, so
                # grouping by it would emit two rows for one conflict-key after a
                # tenant rename and abort the UPSERT — see the hourly note above.
                func.max(MeteringHourly.tenant_name).label("tenant_name"),
                MeteringHourly.license_id,
                MeteringHourly.subject_id,
                MeteringHourly.app_id,
                MeteringHourly.plan_id,
                MeteringHourly.service_key,
                func.sum(MeteringHourly.request_count).label("total"),
                func.sum(MeteringHourly.denied_count).label("denied"),
                func.sum(MeteringHourly.budget_denied_count).label("budget_denied"),
            )
            .where(
                # Raw sargable bounds (prune/index hint) — see day_start/day_end.
                MeteringHourly.bucket_hour >= day_start,
                MeteringHourly.bucket_hour < day_end,
                # UTC-explicit: bucket_hour is timestamptz and the DB session TZ
                # is not guaranteed UTC (prod pgbouncer session is Europe/
                # Amsterdam), so a bare cast(... AS Date) would resolve in local
                # time and mis-bucket events near the UTC day boundary. target_date
                # is computed in UTC (datetime.now(UTC)); force the SQL to match.
                func.cast(func.timezone("UTC", MeteringHourly.bucket_hour), Date) == target_date,
            )
            .group_by(
                MeteringHourly.tenant_id,
                MeteringHourly.license_id,
                MeteringHourly.subject_id,
                MeteringHourly.app_id,
                MeteringHourly.plan_id,
                MeteringHourly.service_key,
            )
        )
        result = await session.execute(stmt)
        rows = result.all()

        count = len(rows)
        # Guarded delete — see the block comment above. Two ways to earn it:
        # we hold the replacement rows (a rebuild), or the source still covers
        # this day so the empty result is a real "this day is now empty".
        # Neither → the source was truncated/aged out; preserve the bucket.
        if rows or metering_covered:
            await session.execute(
                delete(MeteringDaily).where(MeteringDaily.bucket_date == target_date)
            )
        if rows:
            daily_rows = [
                {
                    "tenant_id": r.tenant_id,
                    "tenant_name": r.tenant_name,
                    "license_id": r.license_id,
                    "subject_id": r.subject_id,
                    "app_id": r.app_id,
                    "plan_id": r.plan_id,
                    "service_key": r.service_key,
                    "bucket_date": target_date,
                    "request_count": r.total,
                    "denied_count": r.denied,
                    "budget_denied_count": r.budget_denied,
                }
                for r in rows
            ]
            upsert = pg_insert(MeteringDaily).values(daily_rows)
            upsert = upsert.on_conflict_do_update(
                constraint="uq_daily",
                set_={
                    "request_count": upsert.excluded.request_count,
                    "denied_count": upsert.excluded.denied_count,
                    "budget_denied_count": upsert.excluded.budget_denied_count,
                },
            )
            await session.execute(upsert)

        # Also roll up costs.  Coalesce subject_id NULL → NULL_SUBJECT_ID so
        # the UNIQUE constraint on cost_daily (which has subject_id NOT NULL)
        # can absorb S2S/system events into a separate "no user" row.
        _csub = func.coalesce(CostEvent.subject_id, NULL_SUBJECT_ID)
        # The project axis (alembic 0036). EXACTLY the `_csub` idiom above, for
        # exactly the same reason: `CostEvent.project_id` is NULLABLE, and
        # `CostDaily.project_id` is NOT NULL and a member of `uq_cost_daily` --
        # this upsert's ON CONFLICT target. Postgres treats NULL != NULL in a
        # plain UNIQUE (`NULLS DISTINCT`), so a NULL reaching the key would never
        # match itself and every rollup cycle would INSERT a duplicate row for the
        # same project-less group instead of restating it.
        #
        # ⚠️ STATED HONESTLY: SQL `GROUP BY` folds NULLs into ONE group (unlike
        # UNIQUE, which does not), so grouping on the raw column would produce the
        # right GRAIN and merely hand back `None` for the value. The SQL coalesce
        # is therefore not load-bearing for the grain -- it is here so the group
        # key and the stored value are the SAME expression, evaluated once. That
        # is what stops a later edit from changing one and not the other, and it
        # is precisely the `_csub` idiom five lines up, not a new invention.
        #
        # ⭐ THE `nullif` IS A RECLASSIFICATION, NOT A COVER-UP. A `project_id`
        # byte-identical to its own row's `tenant_id` names no project: it is the
        # tenant's id wearing the project column, and no such project exists in
        # any registry. `NULL_PROJECT_ID` means exactly "spend no project was
        # asserted for", so it is the CORRECT value for that row, arrived at the
        # same way the NULL case is -- which is why one `nullif` inside the
        # existing coalesce is the whole change: forged and absent are the same
        # state, so they take the same branch and land on the same sentinel.
        #
        # ⛔ RE-KEYED, NEVER DROPPED. Filtering these events out of the aggregate
        # would be the worse defect: `billing_cost_daily` would stop summing to
        # `billing_cost_events`, and a rollup that does not reconcile with its own
        # ledger is unusable for every reader, not just the project axis. The
        # dollars are unchanged by this expression -- only which bucket they land
        # in. A tenant's forged rows simply merge into its project-less group (the
        # GROUP BY sees one key for both), which is the same fold a genuinely
        # project-less event already gets.
        #
        # ⭐ DETECTION SURVIVES, MEASURED WHERE IT IS ACTUALLY READ. This touches
        # the ROLLUP only. `services.attribution_audit._PROJECT_SELF_TENANT_SQL`
        # reads the RAW `billing_cost_events` ledger, which no line of this
        # function writes, so every forged event stays visible and countable
        # there -- the forgery is reported at the ledger while the contamination
        # stops before `/api/costs/*` per-project breakdowns, which read the
        # rollup and apply no licence clause of their own
        # (`routers/_aggregation.py::aggregate_by_project`, four call sites).
        #
        # ⚠️ AND IT IS SELF-CLEANING, WHICH IS WHY IT BELONGS HERE AND NOT IN A
        # ONE-OFF. `rollup_daily` guarded-DELETEs a `bucket_date` and rebuilds it
        # from `cost_events`, so hand-deleting contaminated daily rows would not
        # hold -- the next reconcile re-mints them from the unchanged source
        # events. Putting the rule in the aggregate instead makes every
        # subsequent re-roll of an affected bucket rewrite those rows onto the
        # sentinel with no operator action.
        #
        # ⛔ STATED WITH ITS BOUND, because "self-cleaning" is otherwise read as
        # unconditional: a bucket is only re-rolled automatically while it is
        # inside the trailing `daily_reconcile_days` window (DEFAULT 3 --
        # settings.py), or if it goes ABSENT (`missing_daily_dates` fills only
        # absent dates), or if `services.reprice` re-invokes this function for
        # it. The production contamination measured for this change is
        # `bucket_date = 2026-09-15` (3 rows, $5.994795), which is inside that
        # window today -- so it cleans itself on the next pass. A bucket that
        # ages out of the window before this ships would NOT, and would need a
        # deliberate re-roll. That is a bounded, nameable follow-up, not a
        # property to assume.
        #
        # ⚠️ `type_` IS PINNED DEFENSIVELY, AND THE ORIGINAL RATIONALE FOR IT WAS
        # FALSE -- corrected here rather than deleted, because a wrong mechanism
        # in a comment becomes cited precedent. The first version of this note
        # claimed that without the pin "the sentinel bind param loses its uuid
        # typing in the GROUP BY". Measured against this repo's own venv
        # (SQLAlchemy 2.0.51): bare `nullif` does carry `NullType()`, but
        # `coalesce(NullType, <uuid bind>)` resolves to `Uuid()` regardless, and
        # the COMPILED SQL IS BYTE-IDENTICAL either way (`...::UUID`), with the
        # bind typed `Uuid()` in both. The pin's only real effect is to upgrade
        # the expression type `Uuid()` -> the dialect's `DBUUID()`.
        #
        # ⇒ It is kept because stating the type explicitly is cheap and survives
        # a future edit that changes what `coalesce` is given to infer from --
        # NOT because anything is broken without it. Do not cite this as evidence
        # that a generic function's `type_` must be pinned to keep a bind typed;
        # that claim was tested and refuted.
        _cproj = func.coalesce(
            func.nullif(CostEvent.project_id, CostEvent.tenant_id, type_=CostEvent.project_id.type),
            NULL_PROJECT_ID,
        )
        # served_model + byo enter the cost line so per-model markup resolves at
        # billing time. NULL served_model → '__unknown__' sentinel; byo is the
        # JSON boolean usage_extra.byo (absent/non-true → false).
        _served = func.coalesce(CostEvent.served_model, "__unknown__")
        _byo = case((CostEvent.usage_extra["byo"].astext == "true", True), else_=False)
        # owner_provided (managed BYO) is a distinct provenance from byo — the
        # emitter sets exactly one of usage_extra.byo / usage_extra.owner_provided.
        _owner = case((CostEvent.usage_extra["owner_provided"].astext == "true", True), else_=False)
        # The licence mirror, read ONCE per day-rollup rather than per row: it is
        # the only local evidence that a customer relationship exists to attribute
        # licence-less spend to. Sentinel licences are excluded from the set so a
        # tenant whose only mirror row is itself a placeholder does not read as a
        # licence owner.
        #
        # ⚠️ MEASURED 2026-09-07 and CORRECTED: an earlier pass at this predicate
        # was reported as "tenant owns ANY licence, for ANY product" answering a
        # coarser question than the one it feeds (classify_cost_attribution wants
        # "does this tenant own THE licence that would cover THIS call"). A naive
        # service_key -> app-name string match (e.g. `hirag` ~ "HiRAG
        # Decomposition") measured the gap at $3,354.35 of $3,386.93 (99%)
        # "unlicensed" spend belonging to tenants with no licence for the
        # generating app. That number does not replicate against this repo's own
        # ALREADY-AUTHORITATIVE map: `settings.cost_attribution_app_by_service_key`
        # (used identically by services.license_resolver.resolve_license, with
        # per-entry evidence for every grouping, e.g. `hirag`/`karma`/`ingestion`/
        # `kag-graph` all -> "KAG Module"). Re-measured against THAT map, live on
        # prod: only 6 rows / $0.52 of the $3,386.93 are a tenant holding a
        # licence for an unrelated app; 544 rows / $3,383.63 hold an ACTIVE
        # licence (checked directly against the `licenses` service DB, not just
        # this stale-by-up-to-11-days mirror) for the exact app the call belongs
        # to, and 13 rows / $2.77 have a `service_key` this map does not cover at
        # all. The fix below reuses that map rather than inventing a second,
        # divergent one -- see `app_owning_pairs` and its per-row lookup further
        # down.
        licence_owning_tenants: set[str] = {
            str(t)
            for (t,) in (
                await session.execute(
                    select(RequestQuotaConfig.tenant_id)
                    .where(RequestQuotaConfig.license_id != NULL_LICENSE_ID)
                    .distinct()
                )
            ).all()
            if t
        }
        # App-scoped ownership: (tenant_id, app_name.casefold()) pairs the mirror
        # actually carries a non-sentinel licence for. `app_name` (not `app_id`)
        # is the join key because it is what operators author
        # `cost_attribution_app_by_service_key` with -- see models.db.quota's
        # comment on why both columns are kept. This existence check is
        # deliberately NOT `services.license_resolver.get_resolution_map`: that
        # map drops an ambiguous `(tenant, app)` pair (two candidate licences) to
        # avoid *picking* one for an invoice line, but ownership -- "does this
        # tenant hold a relevant licence at all" -- is true either way, so
        # reusing the resolver's map here would silently under-count ownership
        # for every tenant with two licences for the same app.
        app_owning_pairs: set[tuple[str, str]] = {
            (str(t), a.casefold())
            for (t, a) in (
                await session.execute(
                    select(RequestQuotaConfig.tenant_id, RequestQuotaConfig.app_name)
                    .where(
                        RequestQuotaConfig.license_id != NULL_LICENSE_ID,
                        RequestQuotaConfig.app_name.is_not(None),
                    )
                    .distinct()
                )
            ).all()
            if t and a
        }
        # The same explicit, evidence-based map license_resolver uses to attribute
        # a licence to a licence-less event -- see settings.py's field docstring
        # ("A service_key absent from this map is never resolved"). Reused here,
        # not re-derived, so the two places that answer "which app does this
        # service_key belong to" cannot silently disagree.
        app_by_service_key = parse_service_key_app_map(
            get_settings().cost_attribution_app_by_service_key
        )
        # ⚠️ NOT status/expiry aware, and this cannot be fixed locally. Measured
        # 2026-09-07: `request_quota_config` has no status/starts_at/expires_at/
        # revoked_at column (confirmed on the live schema), and
        # services.license_resolver documents the identical limitation for the
        # same reason -- the sync-from-licenses reconcile only ever mirrors
        # `app_id`/`app_name`, and a revoked/expired licence's row is pruned only
        # on its NEXT successful sync (delete_absent_quota_configs), which was
        # last 2026-09-05 05:10 UTC at write time, up to 11 days stale for most
        # rows. So "owns a licence for this app" here means "as of the last
        # successful sync", not "was active on this cost row's bucket_date" --
        # closing that gap needs either a live call to the licenses service or
        # the reconcile carrying status onto this table; this predicate does
        # neither rather than fabricate one. It is the SAME approximation
        # `services.license_resolver.resolve_license` already ships with.

        cost_stmt = (
            select(
                CostEvent.tenant_id,
                # Aggregate (not group by): tenant_name is not part of uq_cost_daily,
                # so grouping by it would emit two rows for one conflict-key after a
                # tenant rename and abort the UPSERT — see the hourly note above.
                func.max(CostEvent.tenant_name).label("tenant_name"),
                CostEvent.license_id,
                _csub.label("subject_id"),
                _cproj.label("project_id"),
                CostEvent.service_key,
                CostEvent.provider,
                _served.label("served_model"),
                _byo.label("byo"),
                _owner.label("owner_provided"),
                CostEvent.cost_source,
                func.coalesce(func.sum(CostEvent.input_tokens), 0).label("inp"),
                func.coalesce(func.sum(CostEvent.output_tokens), 0).label("outp"),
                # ⚠️ THIS SUM IS NOT WRONG, IT IS SILENT — and the silence is what
                # the counter two entries below exists to break. `CostEvent.cost_usd`
                # is NULLABLE and NULL is load-bearing: `services.pricing` refuses
                # rather than guessing a rate and the flusher stores that refusal as
                # NULL (see events.CostEvent.cost_usd). SQL `sum()` SKIPS those rows,
                # so `cost` is exactly the spend that is KNOWN — correct, and
                # deliberately not changed here. The `coalesce(..., 0)` fires only
                # when EVERY row of the group is unpriced, and then stores $0.000000
                # beside a real token count, which reads as "this spend was free"
                # instead of "this spend is unknown".
                func.coalesce(func.sum(CostEvent.cost_usd), 0).label("cost"),
                # NULL when no event in the group reported a vendor cost — never coalesced to 0.
                func.sum(CostEvent.vendor_cost_usd).label("vendor_cost"),
                func.count().label("cnt"),
                # How many of this group's rows could not be PRICED at all
                # (`cost_usd IS NULL`). Counted in the SAME aggregate that does the
                # summing, for exactly the reason given on `ts_fallbacks` below: the
                # number can never describe a different row set than the one just
                # billed. A second query over the same day would be free to answer
                # about a different population (a concurrent flush, a reprice landing
                # between the two statements) and then agree with nothing.
                #
                # ⚠️ LATENT TODAY: measured 2026-09-11, no row in
                # `billing_cost_events` has a NULL `cost_usd`, so this is 0 on every
                # current bucket. It is the signal an invoice close can REFUSE on
                # instead of billing an unknown as zero — see
                # `services.invoices.assert_all_service_keys_priced`, which today
                # gates only on a missing app-request RATE and cannot see a missing
                # PROVIDER price at all.
                func.coalesce(func.sum(case((CostEvent.cost_usd.is_(None), 1), else_=0)), 0).label(
                    "unpriced"
                ),
                # How many of this group's rows were bucketed on INGEST time
                # because the producer asserted no usable event time. Counted in
                # the SAME aggregate that does the bucketing — not by a second
                # query — so the number can never describe a different row set
                # than the one just billed.
                func.coalesce(func.sum(case((CostEvent.event_ts.is_(None), 1), else_=0)), 0).label(
                    "ts_fallbacks"
                ),
            )
            .where(
                # Sargable LOWER bound only, on the bare partition key, purely as
                # a partition-prune / index hint. It is SOUND, and the proof is an
                # invariant the write side enforces, not an assumption: the flusher
                # refuses any `event_ts` later than the instant it received the
                # message (`_parsers.parse_event_ts`), so
                # `event_ts <= received_at <= created_at` for every stored row and
                # therefore `cost_event_time >= day_start` implies
                # `created_at >= day_start`. Every partition older than
                # target_date is still pruned away. ⛔ If that refusal is ever
                # relaxed, this bound silently starts dropping rows.
                CostEvent.created_at >= day_start,
                # ⛔ THERE IS DELIBERATELY NO `created_at < day_end` BOUND, AND IT
                # MUST NOT BE RE-ADDED. That bound is what made this query blind to
                # the bug being fixed: an event emitted at 23:59:50 and ingested at
                # 00:00:05 belongs to the EARLIER day by event time but has a
                # `created_at` past `day_end`, so the bound excluded the very row
                # the resolved cast exists to capture. Any finite replacement
                # ("day_end + a lag tolerance") silently drops whatever exceeds the
                # tolerance, and the observed consumer stall was 62 HOURS. The cost
                # is scanning partitions NEWER than target_date; rollups run on
                # yesterday, so in practice that is one current partition.
                # UTC-explicit — see the bucket_hour note above. Both clocks are
                # timestamptz; without the AT TIME ZONE 'UTC' wrap this groups cost
                # into local-date buckets that disagree with the UTC target_date,
                # mis-attributing ~8.8% of events at day boundaries.
                #
                # ⭐ THE ACTUAL FIX: the bucket date now comes from the PRODUCER's
                # event time where it exists, not from ingest time. Billing a flush
                # that crossed midnight under `created_at` put the usage on the
                # wrong day and, at a month boundary, on the wrong INVOICE. Read
                # through `cost_event_time` so this and `cost_covered` above cannot
                # answer different questions.
                func.cast(func.timezone("UTC", cost_event_time), Date) == target_date,
                # Parked rows are a REVERSIBLE billing exclusion, not deleted
                # data — a duplicate that billing keeps for audit. Every
                # aggregate that turns events into money must drop them, and
                # this is the one that feeds the invoice.
                CostEvent.excluded_from_billing.is_(False),
            )
            .group_by(
                CostEvent.tenant_id,
                CostEvent.license_id,
                _csub,
                # ⭐ THE GRAIN SPLIT. Without this term the aggregate folds every
                # project of a tenant into one row and the stored project_id is
                # whichever one the sentinel default supplies -- i.e. the axis
                # exists in the schema and is dead in the data.
                _cproj,
                CostEvent.service_key,
                CostEvent.provider,
                _served,
                _byo,
                _owner,
                CostEvent.cost_source,
            )
        )
        cost_result = await session.execute(cost_stmt)
        cost_rows = cost_result.all()

        # ── Count the event-time FALLBACK, per emitter ───────────────────────
        # Rows bucketed on `created_at` because the producer asserted no usable
        # `event_ts`. This is the arming criterion for ever trusting event time
        # unconditionally: ⚠️ only ten of the twelve live `service_key`s are
        # CONFIRMED to send the wire `timestamp` — `w2v-video-analyzer` and
        # `w2v-video-compiler` (the #2 spender) could not be verified to use the
        # shared emitter library — so "all emitters send it" stays an assumption
        # until this reads zero for every service_key. Labelled by service_key
        # precisely so it names WHICH emitter is still missing, instead of one
        # aggregate number nobody can act on.
        #
        # It is the read-side twin of `billing_placeholder_value_total{field=
        # "timestamp"}` (born from COST_METADATA_WHITELIST at the write side) and
        # it is NOT redundant with it: that one sees absence and sentinel words on
        # the wire, this one additionally catches a value that ARRIVED but was
        # refused — unparseable, naive, clock-skewed, or back-dated past retention.
        #
        # Follows this module's existing convention: function-local import inside
        # try/except, because observability must never break a billing aggregate.
        # ⚠️ That makes a MISSING counter a log line rather than a crash, which is
        # only safe because tests/test_rollup_event_time.py asserts the series
        # exists and moves — CI goes red, production does not.
        try:
            from observability.metrics import cost_event_ts_fallback_total

            ts_fallback_by_service: dict[str, int] = {}
            for c in cost_rows:
                if c.ts_fallbacks:
                    key = c.service_key or "unknown"
                    ts_fallback_by_service[key] = ts_fallback_by_service.get(key, 0) + int(
                        c.ts_fallbacks
                    )
            for _service_key, _n in ts_fallback_by_service.items():
                cost_event_ts_fallback_total.labels(service_key=_service_key).inc(_n)
        except Exception:  # pragma: no cover - observability must not break the loop
            logger.warning(
                "cost event_ts fallback counter not published — "
                "observability.metrics.cost_event_ts_fallback_total{service_key} is missing",
                exc_info=True,
            )

        # Guarded delete — same contract as the metering side above, evaluated
        # independently against cost_events' own retention.
        if cost_rows or cost_covered:
            await session.execute(delete(CostDaily).where(CostDaily.bucket_date == target_date))

        def _tenant_owns_relevant_licence(tenant_id: Any, service_key: str | None) -> bool:
            """Scoped where the map covers this service_key, coarse otherwise.

            An unmapped `service_key` means license_resolver itself cannot say
            which app the call belongs to either (rule 1, "never fabricate" --
            see license_resolver.py) -- so this predicate makes the same choice:
            it does not guess an app, it falls back to the pre-existing
            tenant-wide check rather than defaulting an unresolvable row to
            SYSTEM, which would be inventing precision this repo does not have.
            """
            mapped_app = app_by_service_key.get(service_key) if service_key else None
            if mapped_app is None:
                return str(tenant_id) in licence_owning_tenants
            return (str(tenant_id), mapped_app) in app_owning_pairs

        if cost_rows:
            cost_daily_rows = [
                {
                    "tenant_id": c.tenant_id,
                    "tenant_name": c.tenant_name,
                    "license_id": c.license_id or NULL_LICENSE_ID,
                    "subject_id": c.subject_id or NULL_SUBJECT_ID,
                    # Belt-and-braces, matching the line above: `_cproj` already
                    # coalesced this in SQL, so this can only fire if that
                    # expression is ever reverted to the raw column.
                    "project_id": c.project_id or NULL_PROJECT_ID,
                    "service_key": c.service_key,
                    "provider": c.provider,
                    "served_model": c.served_model,
                    "byo": c.byo,
                    "owner_provided": c.owner_provided,
                    "cost_source": c.cost_source,
                    # DERIVED, never asserted -- see models.db._common.
                    # classify_cost_attribution. This is what stops the
                    # NULL_LICENSE_ID sentinel meaning two different things:
                    # deliberate system/S2S cost, and a licensed customer whose
                    # x-license-id was dropped east-west. license_id itself is
                    # untouched, so every budget predicate and upsert key that
                    # keys on the sentinel keeps behaving identically.
                    "attribution_status": classify_cost_attribution(
                        c.license_id,
                        tenant_id=c.tenant_id,
                        tenant_owns_licence=_tenant_owns_relevant_licence(
                            c.tenant_id, c.service_key
                        ),
                        # ⭐ THE CALLER NAMED A LICENCE AND THE FLUSHER REFUSED
                        # IT. ``services.cost_flusher.adjudicate_asserted_license``
                        # nulls such a row's ``license_id`` — that removal is what
                        # keeps it outside ``license_attributed_clause()`` and
                        # therefore off every invoice line — and leaves this
                        # marker on ``cost_source`` so the REASON survives the
                        # removal. Without this argument the row would read
                        # 'unlicensed' or 'system' here and a confidently-wrong
                        # assertion would be indistinguishable from an honest
                        # absence, which is the distinction the whole value
                        # exists to preserve.
                        #
                        # ⚠️ ``cost_source`` is a ``uq_cost_daily`` dimension, so
                        # it is grouped ON and arrives intact — a refused row can
                        # never be summed together with an accepted one under a
                        # single status.
                        license_refused=cost_source_marks_refusal(c.cost_source),
                    ),
                    "bucket_date": target_date,
                    "total_input_tokens": c.inp,
                    "total_output_tokens": c.outp,
                    "total_cost_usd": c.cost,
                    "total_vendor_cost_usd": c.vendor_cost,
                    "request_count": c.cnt,
                    # Stored beside the money, from the same aggregate. `cost` above
                    # is the KNOWN spend of this group; this says how many of its
                    # `request_count` rows contributed nothing to that figure because
                    # nothing could price them. `> 0` with a non-zero
                    # `total_input_tokens` is the shape that used to read as free.
                    "unpriced_event_count": c.unpriced,
                }
                for c in cost_rows
            ]
            cost_upsert = pg_insert(CostDaily).values(cost_daily_rows)
            cost_upsert = cost_upsert.on_conflict_do_update(
                constraint="uq_cost_daily",
                set_={
                    "total_input_tokens": cost_upsert.excluded.total_input_tokens,
                    "total_output_tokens": cost_upsert.excluded.total_output_tokens,
                    "total_cost_usd": cost_upsert.excluded.total_cost_usd,
                    "total_vendor_cost_usd": cost_upsert.excluded.total_vendor_cost_usd,
                    "request_count": cost_upsert.excluded.request_count,
                    # Listed so this count can never survive a restatement of the
                    # total it qualifies (a row claiming "unknown spend" beside a
                    # total that has since been repriced). ⚠️ Like
                    # `attribution_status` below, this entry covers ONLY the residual
                    # concurrent-writer path — the guarded DELETE above means a
                    # re-rollup, including the one `services.reprice` triggers, never
                    # reaches ON CONFLICT. No test exercises it; do not call it the
                    # mechanism.
                    "unpriced_event_count": cost_upsert.excluded.unpriced_event_count,
                    # ⚠️ MEASURED SURVIVOR, kept for symmetry and stated honestly.
                    # Deleting this line kills NOTHING: the guarded DELETE ~20
                    # lines up removes the whole `bucket_date` before this INSERT
                    # runs, so a re-rollup of the same day never reaches ON
                    # CONFLICT at all. Reclassification when the licence mirror
                    # MOVES (a tenant acquires its first licence, turning
                    # yesterday's 'system' rows into 'unlicensed') is therefore
                    # delivered by delete+insert, which is what
                    # test_reclassifies_when_the_tenant_acquires_a_licence
                    # actually proves. This entry only covers the residual path
                    # where a concurrent writer wins the row between the delete
                    # and the insert; it is NOT the mechanism, and no test
                    # exercises it. Do not describe it as one.
                    "attribution_status": cost_upsert.excluded.attribution_status,
                },
            )
            await session.execute(cost_upsert)

        # ⭐ PER-ACTOR AXIS (alembic 0040). Same filters and bucket date as the CostDaily aggregate
        # above, so the two tables reconcile exactly; grouped by actor instead of project/source.
        # Rebuilt for the whole bucket_date like CostDaily, so a re-rollup is idempotent.
        if cost_rows or cost_covered:
            await session.execute(
                delete(CostActorDaily).where(CostActorDaily.bucket_date == target_date)
            )
        _cactor = func.coalesce(CostEvent.actor_id, NULL_ACTOR_ID)
        _clic = func.coalesce(CostEvent.license_id, NULL_LICENSE_ID)
        actor_rows = (
            await session.execute(
                select(
                    CostEvent.tenant_id,
                    _clic.label("license_id"),
                    _csub.label("subject_id"),
                    _cactor.label("actor_id"),
                    CostEvent.service_key,
                    CostEvent.provider,
                    _served.label("served_model"),
                    _byo.label("byo"),
                    _owner.label("owner_provided"),
                    func.coalesce(func.sum(CostEvent.input_tokens), 0).label("inp"),
                    func.coalesce(func.sum(CostEvent.output_tokens), 0).label("outp"),
                    func.coalesce(func.sum(CostEvent.cost_usd), 0).label("cost"),
                    func.sum(CostEvent.vendor_cost_usd).label("vendor_cost"),
                    func.count().label("cnt"),
                )
                .where(
                    CostEvent.created_at >= day_start,
                    func.cast(func.timezone("UTC", cost_event_time), Date) == target_date,
                    CostEvent.excluded_from_billing.is_(False),
                )
                .group_by(
                    CostEvent.tenant_id,
                    _clic,
                    _csub,
                    _cactor,
                    CostEvent.service_key,
                    CostEvent.provider,
                    _served,
                    _byo,
                    _owner,
                )
            )
        ).all()
        if actor_rows:
            await session.execute(
                pg_insert(CostActorDaily).values(
                    [
                        {
                            "id": uuid.uuid4(),
                            "bucket_date": target_date,
                            "tenant_id": a.tenant_id,
                            "license_id": a.license_id,
                            "subject_id": a.subject_id,
                            "actor_id": a.actor_id,
                            "service_key": a.service_key,
                            "provider": a.provider,
                            "served_model": a.served_model,
                            "byo": a.byo,
                            "owner_provided": a.owner_provided,
                            "total_input_tokens": a.inp,
                            "total_output_tokens": a.outp,
                            "total_cost_usd": a.cost,
                            "total_vendor_cost_usd": a.vendor_cost,
                            "request_count": a.cnt,
                        }
                        for a in actor_rows
                    ]
                )
            )

        # ⭐ CROSS AXIS (alembic 0053): project × actor × app × funding × provider × model. Same
        # filters, bucket date and delete guard as the two blocks above, so it reconciles to
        # CostDaily exactly; it is what /api/usage/breakdown reads.
        if cost_rows or cost_covered:
            await session.execute(
                delete(CostUsageDaily).where(CostUsageDaily.bucket_date == target_date)
            )
        _capp = func.coalesce(CostEvent.app_id, NULL_APP_ID)
        _cfund = func.coalesce(CostEvent.funding, FUNDING_UNKNOWN)
        usage_rows = (
            await session.execute(
                select(
                    CostEvent.tenant_id,
                    _cproj.label("project_id"),
                    _cactor.label("actor_id"),
                    _capp.label("app_id"),
                    _cfund.label("funding"),
                    CostEvent.provider,
                    _served.label("served_model"),
                    func.count().label("cnt"),
                    func.coalesce(func.sum(CostEvent.input_tokens), 0).label("inp"),
                    func.coalesce(func.sum(CostEvent.output_tokens), 0).label("outp"),
                    func.coalesce(func.sum(CostEvent.cost_usd), 0).label("cost"),
                )
                .where(
                    CostEvent.created_at >= day_start,
                    func.cast(func.timezone("UTC", cost_event_time), Date) == target_date,
                    CostEvent.excluded_from_billing.is_(False),
                )
                .group_by(
                    CostEvent.tenant_id,
                    _cproj,
                    _cactor,
                    _capp,
                    _cfund,
                    CostEvent.provider,
                    _served,
                )
            )
        ).all()
        if usage_rows:
            await session.execute(
                pg_insert(CostUsageDaily).values(
                    [
                        {
                            "id": uuid.uuid4(),
                            "bucket_date": target_date,
                            "tenant_id": u.tenant_id,
                            "project_id": u.project_id,
                            "actor_id": u.actor_id,
                            "app_id": u.app_id,
                            "funding": u.funding,
                            "provider": u.provider,
                            "served_model": u.served_model,
                            "event_count": u.cnt,
                            "total_input_tokens": u.inp,
                            "total_output_tokens": u.outp,
                            "total_cost_usd": u.cost,
                        }
                        for u in usage_rows
                    ]
                )
            )

        await session.commit()

    if count or cost_rows:
        logger.info(
            "daily_rollup_completed",
            date=str(target_date),
            metering_rows=count,
            cost_rows=len(cost_rows),
        )
    return count


async def missing_daily_dates(
    session_factory: async_sessionmaker[AsyncSession],
    floor: date,
) -> list[date]:
    """Past complete days in ``[floor, yesterday]`` with source data but no daily row.

    A day needs (re)rolling if metering_hourly has it but metering_daily doesn't,
    or cost_events has it but cost_daily doesn't. Gap-aware like
    :func:`missing_event_hours`: this fills both a forward gap (a newly-complete
    yesterday) **and** an interior hole that a plain ``max(bucket_date)`` watermark
    would skip — e.g. metering_daily missing the days cost_daily already advanced
    past after a rolling deploy. Bounded by ``floor`` (retention) and excludes the
    current incomplete day. rollup_daily is idempotent, so re-rolling an
    already-present sibling table for one of these dates is harmless.

    ⛔ THIS IS A REPAIR LIST, NOT A GAP DETECTOR, and it structurally cannot be
    both. Every candidate date here is derived FROM a source table
    (``metering_hourly`` / ``cost_events``), so a day with no source rows at all
    is not a candidate and can never be reported — that is the blind spot
    :func:`daily_rollup_date_gaps` exists to cover, and it is deliberately a
    SEPARATE function rather than a widening of this one. Widening this one would
    feed empty days into ``rollup_daily``, which is the one thing that must not
    happen: a day with no events legitimately has no rollup, and rolling it can
    only either write nothing or, if that day's source is still covered, DELETE
    the bucket. Report, do not repair.
    """
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    # UTC-explicit buckets so the gap-detector's day set matches what rollup_daily
    # writes (and the UTC `yesterday`/`floor` bounds) regardless of session TZ.
    m_day = func.cast(func.timezone("UTC", MeteringHourly.bucket_hour), Date)
    # Through the SAME resolved expression rollup_daily buckets by. If this asked
    # on `created_at` while rollup_daily wrote buckets by event time, the repair
    # list would name dates the roller does not write and MISS dates it does: a
    # midnight-crossing event would be reported as a permanent gap on its ingest
    # day (re-rolled forever, never satisfied) while the event day it actually
    # belongs to went unreported.
    c_day = func.cast(func.timezone("UTC", cost_event_time), Date)
    m_src = (select(m_day).where(m_day >= floor, m_day <= yesterday).group_by(m_day)).except_(
        select(MeteringDaily.bucket_date)
    )
    # A day whose events are ALL parked has no billable cost and must not be
    # reported as a gap — re-rolling it would write nothing and it would be
    # rediscovered as missing on every pass.
    c_src = (
        select(c_day)
        .where(c_day >= floor, c_day <= yesterday, CostEvent.excluded_from_billing.is_(False))
        .group_by(c_day)
    ).except_(select(CostDaily.bucket_date))
    async with session_factory() as session:
        result = await session.execute(union(m_src, c_src))
    return sorted(row[0] for row in result.all())


@dataclass(frozen=True)
class CatchupResult:
    """What one catch-up pass actually achieved — and what it did NOT."""

    #: Buckets rolled by this pass, oldest first.
    rolled: list[date]
    #: Buckets that were due and were NOT rolled because the batch cap was hit.
    #: ``> 0`` means STILL BEHIND: the pass finished without error and is not
    #: caught up. Those are different facts and the caller must not conflate them.
    remaining: int
    #: Newest bucket this pass was permitted to roll (the last CLOSED UTC day).
    boundary: date
    #: Number of due buckets before the cap was applied.
    due: int

    @property
    def complete(self) -> bool:
        """True only when nothing due was left behind."""
        return self.remaining == 0


async def catchup_daily(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    floor: date,
    reconcile_days: int,
    max_days: int,
    now: datetime | None = None,
) -> CatchupResult:
    """Roll EVERY un-rolled closed day up to the boundary, oldest first, capped.

    Replaces the one-shot ``for day in sorted(missing | window)`` in
    ``services.aggregator``. Three properties it adds, each load-bearing:

    ── 1. WHERE IT TERMINATES ──────────────────────────────────────────────────
    ``boundary = yesterday (UTC)`` — the last day that can no longer receive
    rows. Never today. This is not a new rule, it is the one
    :func:`missing_daily_dates` already enforces (``yesterday =
    datetime.now(UTC).date() - timedelta(days=1)``); it is restated here so the
    trailing reconcile window is bounded by the SAME date rather than by an
    independently-computed one, and so a future edit cannot widen one without
    the other.

    Rolling an OPEN bucket is a distinct data-loss bug from the lag this fixes:
    ``rollup_daily`` DELETEs the bucket and rewrites it from whatever has landed
    so far, so a bucket rolled at 09:00 for a day still accruing rows is written
    SHORT. The UTC day boundary alone does not make a day closed — cost rows
    reach ``cost_events`` through a buffered flush loop
    (``run_flush_loop``/``flush_interval_seconds``), so an event stamped
    23:59:59Z can be INSERTed after 00:00:00Z the next day.

    The grace for that is deliberately NOT a second knob: ``daily_reconcile_days``
    (default 3) already re-rolls the trailing window on every subsequent pass, so
    a yesterday rolled seconds-too-early is rewritten complete on the next pass
    and again for two more days. Adding a grace period here would duplicate an
    existing mechanism with a second number to keep in sync. The invariant that
    makes that sound is asserted below: the reconcile window is a subset of
    ``[floor, boundary]``, so every day this pass may write is re-rolled at least
    ``reconcile_days`` more times before it is left alone.

    ── 2. OLDEST FIRST, AND WHY THE ORDER IS NOT COSMETIC ──────────────────────
    The batch is ``candidates[:max_days]`` after an ASCENDING sort, so a capped
    pass advances ``max(bucket_date)`` by the smallest amount that still makes
    progress. Newest-first would be actively dangerous: the ``BEFORE TRUNCATE``
    guard added in alembic 0017 refuses to truncate ``billing_cost_events`` while
    raw events exist NEWER than ``max(bucket_date)``. Rolling the newest hole
    first jumps that watermark to the head of the backlog and DISARMS the guard
    while the middle of the backlog is still un-rolled — re-opening exactly the
    2026-09-05T07:01:56Z exposure. Oldest-first keeps the guard armed until the
    backlog is genuinely drained.

    ── 3. A CAPPED PASS MUST NOT LOOK FINISHED ────────────────────────────────
    The candidate set is bounded only by retention (up to 92 dates), and each
    date costs a delete plus a partition-scanning re-aggregate. ``max_days`` caps
    the work; ``CatchupResult.complete`` is False when the cap bit, and
    ``services.aggregator.run_rollup_loop`` uses that to decline marking the
    calendar day done — so the next cycle resumes in ``rollup_interval_seconds``
    instead of a full day. A cap that silently parked the remainder for 24h would
    reproduce the original defect (one bucket per run) with a nicer name.
    """
    today = (now or datetime.now(UTC)).date()
    boundary = today - timedelta(days=1)

    # Trailing reconciliation window — see settings.daily_reconcile_days. Anchored
    # on `boundary`, not on an independent "yesterday", so (1) above holds.
    window = {
        day for n in range(max(reconcile_days, 0)) if floor <= (day := boundary - timedelta(days=n))
    }
    missing = set(await missing_daily_dates(session_factory, floor))

    # Belt-and-braces on the boundary: missing_daily_dates computes its own
    # `yesterday` from its own clock read, so a pass that straddles UTC midnight
    # could otherwise be handed today's date by it. Filter, do not trust.
    candidates = sorted(d for d in (missing | window) if floor <= d <= boundary)

    batch = candidates[:max_days] if max_days > 0 else []
    for day in batch:
        # One transaction (and one advisory lock) per bucket — see rollup_daily.
        # A crash between two iterations therefore leaves every earlier bucket
        # fully committed and the current one fully rolled back: no bucket is
        # ever left half-rewritten, and the un-rolled remainder is simply
        # rediscovered by missing_daily_dates on the next pass.
        await rollup_daily(session_factory, day)

    result = CatchupResult(
        rolled=batch,
        remaining=len(candidates) - len(batch),
        boundary=boundary,
        due=len(candidates),
    )

    try:
        from observability.metrics import rollup_catchup_remaining_days

        rollup_catchup_remaining_days.set(result.remaining)
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning("rollup catch-up backlog gauge not published", exc_info=True)

    if not result.complete:
        logger.warning(
            "daily_rollup_catchup_capped",
            rolled=len(batch),
            remaining=result.remaining,
            due=result.due,
            cap=max_days,
            oldest_rolled=str(batch[0]) if batch else None,
            newest_rolled=str(batch[-1]) if batch else None,
            boundary=str(boundary),
            note=(
                "catch-up stopped at its batch cap and is STILL BEHIND. This pass "
                "succeeded; it did not finish. The leader will not mark the day "
                "done, so the next cycle resumes draining."
            ),
        )
    elif batch:
        logger.info(
            "daily_rollup_catchup_completed",
            rolled=len(batch),
            oldest_rolled=str(batch[0]),
            newest_rolled=str(batch[-1]),
            boundary=str(boundary),
        )
    return result


async def daily_rollup_lag_days(
    session_factory: async_sessionmaker[AsyncSession],
    now: datetime | None = None,
) -> dict[str, int | None]:
    """Days between today (UTC) and each daily table's newest bucket. REPORTING ONLY.

    🔴 THIS IS THE INSTRUMENT THAT WAS MISSING, and its absence is why the
    incident ran for months in the open. Every existing signal answered "did the
    job run?": ``billing_rollup_cycles_total`` counted successful leader cycles,
    the loop logged ``daily_rollup_completed`` on every pass, and the pods were
    Ready throughout. All of them were GREEN while ``rollup_daily`` advanced one
    bucket per run and sat permanently ~3 days behind — because "it ran" and "it
    is caught up" are different propositions and nothing measured the second one.

    Healthy is **1**: the last closed UTC day is rolled. ``>= 2`` means buckets
    that are closed and rollable have not been rolled. Alert on the VALUE, never
    on the freshness of a ``last_successful_run`` timestamp — that timestamp was
    correct every single day of the incident.

    Why this is not :func:`daily_rollup_date_gaps`: that function counts days
    ABSENT INSIDE the table's own covered span (``max(floor, min(bucket_date))``
    → yesterday). It answers "is my history holed?", and its own docstring says a
    non-zero reading is not necessarily a defect — a day with no billable
    activity legitimately has no row, so it cannot be alerted on tightly. Lag
    answers the orthogonal question "has the head stopped moving?", is 1 in every
    healthy state, and therefore CAN be. Two numbers, two alerts; neither
    substitutes for the other.

    A table with no rows at all reports ``None`` and publishes no sample, for the
    same reason :func:`daily_rollup_date_gaps` stays silent on one: there is no
    head to be behind. Alert on ``absent()`` for that case, not on a sentinel —
    a sentinel would have to be a number, and every number here is also a real
    lag someone could read as one.
    """
    today = (now or datetime.now(UTC)).date()
    lag: dict[str, int | None] = {}

    async with session_factory() as session:
        for table_name, column in (
            ("cost_daily", CostDaily.bucket_date),
            ("metering_daily", MeteringDaily.bucket_date),
        ):
            newest = (await session.execute(select(func.max(column)))).scalar()
            lag[table_name] = None if newest is None else (today - newest).days

    try:
        from observability.metrics import rollup_lag_days

        for table_name, days in lag.items():
            if days is not None:
                rollup_lag_days.labels(table=table_name).set(days)
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning("rollup lag gauge not published", exc_info=True)

    for table_name, days in lag.items():
        if days is not None and days > 1:
            logger.warning(
                "daily_rollup_behind",
                table=table_name,
                lag_days=days,
                note=(
                    "newest daily bucket is more than one day old — closed days "
                    "exist that have not been rolled. A successful rollup cycle "
                    "does NOT contradict this."
                ),
            )

    return lag


async def metering_hourly_lag_hours(
    session_factory: async_sessionmaker[AsyncSession],
    now: datetime | None = None,
) -> int | None:
    """Hours between the last CLOSED UTC hour and metering_hourly's newest bucket.

    REPORTING ONLY. Returns ``None`` (and publishes no sample) when the table is
    empty, for the same reason :func:`daily_rollup_lag_days` does: there is no
    head to be behind. Never a sentinel — every sentinel here is also a real lag
    someone could read as one.

    ⚠️ CORRECTED 2026-09-08 — this paragraph used to say "Alert on ``absent()``
    for that case". ``absent()`` CANNOT FIRE on this series. Measured with
    prometheus_client: an unlabelled ``Gauge`` is exported the moment it is
    constructed, so ``billing_metering_hourly_lag_hours`` reads ``0.0`` on a
    replica that has never measured — and 0 is *lower* than the healthy 1, so
    "never measured" renders as better-than-healthy.
    :func:`daily_rollup_lag_days` may use ``absent()`` because its gauge is
    labelled; this one may not. The discriminator here is
    ``billing_metering_hourly_lag_timestamp_seconds``, written by this function
    immediately after the value and only when a value was published. **Believe
    the lag only while that timestamp is recent** — the same construction
    ``billing_attribution_audit_last_success_timestamp_seconds`` already
    provides for the coherence gauges, and for the same reason: a value that
    stops being maintained must stop looking maintained.

    🔴 WHY THIS EXISTS SEPARATELY FROM ``daily_rollup_lag_days``. That function
    measures the two DAILY tables. Metering aggregates in TWO stages
    (``billing_metering_events -> billing_metering_hourly ->
    billing_metering_daily``) and NOTHING measured the first one. It is the
    stage that decides whether a raw metering event has reached any aggregate at
    all, so a stall there is the metering equivalent of the ~3-day cost-daily
    backlog that the 2026-09-05 TRUNCATE turned into permanent, unbackfillable
    loss — and it was equally invisible: cycle counters, completion logs and pod
    readiness were all green throughout that incident.

    ⛔ ``billing_rollup_lag_days{table="metering_daily"}`` does NOT cover this,
    and reading it as if it did is the trap. :func:`rollup_daily` builds a day's
    metering_daily rows from whatever ``billing_metering_hourly`` holds for that
    date; it does not require all 24 hours to be present. So the daily lag can
    read a perfectly healthy ``1`` over a day whose hourly coverage is holed —
    the daily head moved, and the underlying hours never arrived. Two stages,
    two numbers; neither implies the other.

    Healthy is **1**: ``rollup_hourly`` rolls the previous complete hour, so at
    any instant the newest bucket should be the hour before the current one.
    ``>= 2`` means closed, rollable hours have not been rolled — which is also
    when ``billing_unrolled_metering_truncate_guard_trg`` (alembic 0023) starts
    refusing TRUNCATEs of ``billing_metering_events``. That gate refuses
    silently as far as monitoring is concerned; this gauge is what turns "the
    gate is holding something back" into an observable.
    """
    reference = now or datetime.now(UTC)
    # Floor to the hour: the current, still-OPEN hour is not rollable, so the
    # newest bucket a caught-up rollup can hold is the one before it. Comparing
    # against a raw `now` would report a fractional-hour lag as 0 for 59 of
    # every 60 minutes and 1 for the rest.
    current_hour = reference.astimezone(UTC).replace(minute=0, second=0, microsecond=0)

    async with session_factory() as session:
        newest = (await session.execute(select(func.max(MeteringHourly.bucket_hour)))).scalar()

    if newest is None:
        return None

    # bucket_hour is timestamptz; normalise to UTC before subtracting so the
    # session timezone cannot shift the arithmetic. Same UTC-explicitness the
    # rollup itself uses when it assigns the bucket.
    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=UTC)
    hours = int((current_hour - newest.astimezone(UTC)).total_seconds() // 3600)

    try:
        from observability.metrics import (
            metering_hourly_lag_hours as _gauge,
        )
        from observability.metrics import (
            metering_hourly_lag_timestamp_seconds as _fresh,
        )

        _gauge.set(hours)
        # AFTER the value, and only on the path that published one. A
        # prometheus_client gauge holds its last value for the life of the
        # process, so the lag alone cannot distinguish "measured, and it is 1"
        # from "nobody has measured since 09:00 and 1 is what it said then".
        # This timestamp can, and it is the ONLY thing that can: the lag series
        # is unlabelled, so it is exported as 0.0 from construction and
        # absent() never fires on it. Select on the pair.
        _fresh.set(time.time())
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning("metering hourly lag gauge not published", exc_info=True)

    if hours > 1:
        logger.warning(
            "metering_hourly_rollup_behind",
            lag_hours=hours,
            newest_bucket_hour=newest.isoformat(),
            note=(
                "newest hourly bucket is more than one closed hour old — closed "
                "hours exist that have not been rolled. A successful rollup cycle "
                "does NOT contradict this, and neither does a healthy "
                "billing_rollup_lag_days{table='metering_daily'}: rollup_daily "
                "builds a day from whatever hourly holds without requiring all 24 "
                "hours. While this is non-zero, alembic 0023's gate refuses a "
                "TRUNCATE of billing_metering_events."
            ),
        )

    return hours


async def cost_events_oldest_age_seconds(
    session_factory: async_sessionmaker[AsyncSession],
    now: datetime | None = None,
) -> float | None:
    """Publish how far back ``billing_cost_events`` actually reaches, in seconds.

    REPORTING ONLY — reads one aggregate and writes one gauge. Returns the age it
    published, or ``None`` when it published nothing because the query could not
    be read.

    🔴 WHY THIS EXISTS. On 2026-09-05T07:01:56Z this table was TRUNCATEd and
    NOTHING NOTICED FOR SIX DAYS. Every instrument the service had measured FLOW —
    ``billing_events_in_total``, ``billing_cost_events_flushed_total``,
    ``billing_rollup_cycles_total``, the lag of the NEWEST bucket — and flow was
    healthy throughout, because ingest resumed within the minute. Not one series
    described the ledger's DEPTH, so a ledger holding 90 days and a ledger holding
    nine minutes were the same green dashboard.
    ``billing_truncate_guard_relations`` and ``billing_delete_audit_rows`` watch the
    GUARD and its audit trail; this watches the CONSEQUENCE, and it is the only one
    of the three that also catches a loss no trigger mediated — a dropped
    partition, a wrong-DSN DELETE, a volume restored from an empty snapshot.

    ⛔ THE EMPTY TABLE PUBLISHES 0, AND THAT IS DELIBERATE — it is the inverse of
    :func:`metering_hourly_lag_hours` and of
    ``billing_cost_dead_letter_oldest_age_seconds``, both of which publish NOTHING
    (or NaN) when their subject is empty. For those, empty is a healthy state with
    no head to measure. Here an empty cost ledger IS the catastrophe this gauge
    exists to report, so it must produce a number an alert can fire on. A
    "never measured" reading is the case that must stay unfireable instead, and the
    gauge carries NaN from import for exactly that — see the metric's own HELP.

    ⛔ AND IT IS ``created_at``, NOT ``coalesce(event_ts, created_at)``. The
    question is how much HISTORY THIS TABLE RETAINS, which is a property of when
    rows were written and of what retention has dropped. ``event_ts`` is
    producer-asserted and may legitimately be back-dated up to 90 days
    (``_parsers.MAX_EVENT_TS_BACKDATE_DAYS``), so one backdating emitter could pin
    this gauge at a comfortable 90 days across a ledger that was emptied an hour
    ago — the instrument would be loudest exactly when it was least true.
    """
    reference = now or datetime.now(UTC)

    async with session_factory() as session:
        oldest = (await session.execute(select(func.min(CostEvent.created_at)))).scalar()

    if oldest is None:
        # EMPTY, which here is the alarming reading and not an abstention. 0 is
        # the honest answer to "how far back does the ledger reach": nowhere.
        age = 0.0
    else:
        if oldest.tzinfo is None:
            oldest = oldest.replace(tzinfo=UTC)
        # clamp at 0: a clock skew between the DB and this process must not
        # publish a negative age, which would read as a future-dated ledger.
        age = max((reference - oldest.astimezone(UTC)).total_seconds(), 0.0)

    try:
        from observability.metrics import cost_events_oldest_age_seconds as _gauge

        _gauge.set(age)
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning(
            "cost events oldest-age gauge not published — "
            "observability.metrics.cost_events_oldest_age_seconds is missing",
            exc_info=True,
        )
        return None

    if oldest is None:
        logger.error(
            "cost_events_ledger_empty",
            note=(
                "billing_cost_events holds NO rows. On a running estate this is a "
                "TRUNCATE, a dropped partition or a wrong-DSN DELETE, not a quiet "
                "hour; on a fresh deployment it is expected. Check "
                "billing_delete_audit_rows and billing_truncate_guard_relations."
            ),
        )

    return age


#: How many gap dates one WARNING log line names before it truncates. The count
#: is always exact (and is also on the gauge); the list is bounded because a
#: retention-length outage would otherwise emit a 90-element log field.
_GAP_LOG_SAMPLE = 14


async def daily_rollup_date_gaps(
    session_factory: async_sessionmaker[AsyncSession],
    floor: date,
) -> dict[str, list[date]]:
    """Complete days in the rollup's own calendar range that have NO daily row.

    REPORTING ONLY. Returns ``{"cost_daily": [...], "metering_daily": [...]}``
    and writes a gauge + a WARNING; it never rolls, inserts or deletes anything,
    and its result must never be fed to :func:`rollup_daily`. See the ⚠️ below.

    THE DEFECT THIS CLOSES. :func:`missing_daily_dates` builds its candidate set
    FROM ``metering_hourly`` / ``cost_events``, so a day with no source rows is
    never a candidate and the rollup can never notice that the day is absent.
    Measured 2026-09-05: ``billing_cost_daily`` holds zero rows for 2026-09-02,
    09-03 and 09-04, and would never have reported them, because the
    07:01:56Z TRUNCATE removed the very events those dates would have been
    derived from — the evidence of the gap and the evidence needed to detect it
    were the same rows. ``sum(total_cost_usd)`` for 2026-04-26..09-01 is
    $5,613.14 of rollups the event table can no longer substantiate.

    HOW THE CANDIDATE SET IS DERIVED, and why it is not simply ``[floor,
    yesterday]``: the lower bound is ``max(floor, min(bucket_date))`` of the
    table itself. A calendar range alone would report every day before the
    service had any history as "missing", which is noise, not a gap — there is
    nothing to be missing from. Anchoring on the table's own oldest surviving
    bucket asks the only answerable question: *within the span this table
    actually covers, is a day absent?* A table with no rows at all yields no
    candidates and is silent, which is correct: "no rollups anywhere" is a
    different condition, and one the existing repair path already handles.

    ⚠️ A REPORTED GAP IS NOT NECESSARILY A DEFECT. A day on which nothing was
    billed legitimately has no rollup row, and this will report it. That is
    accepted deliberately: a false "you have a gap" costs an operator one query,
    while the alternative — inferring emptiness from a source table that may have
    been truncated — is exactly the reasoning that made the real gap invisible.
    An operator confirms with the raw sources; the engine does not guess, and
    above all it does not invent rows to close a gap it cannot explain.

    ⚠️ IT MUST NOT REPAIR. Handing these dates to :func:`rollup_daily` would put
    a day with zero source rows through the guarded delete at the top of that
    function. For a genuinely-empty day inside retention the guard's
    ``cost_covered`` is True (older events exist), so the delete FIRES and the
    re-insert does not — which is precisely how the 2026-09-02..09-04 buckets
    were destroyed. This function therefore has no write path at all, and
    ``services.aggregator`` calls it for its return value only.
    """
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    gaps: dict[str, list[date]] = {"cost_daily": [], "metering_daily": []}

    async with session_factory() as session:
        for table_name, column in (
            ("cost_daily", CostDaily.bucket_date),
            ("metering_daily", MeteringDaily.bucket_date),
        ):
            # One round trip per table: the oldest surviving bucket (the anchor)
            # and the full set of dates it holds inside the window. Bounded by
            # `floor` on BOTH sides of the comparison, so the result set is at
            # most retention-many dates regardless of how old the table is.
            present = {
                row[0]
                for row in (
                    await session.execute(
                        select(column).where(column >= floor, column <= yesterday).group_by(column)
                    )
                ).all()
            }
            oldest = (await session.execute(select(func.min(column)))).scalar()
            if oldest is None:
                # No rollups have ever been written for this table. Nothing to be
                # missing from — silent on purpose (see the docstring).
                continue
            start = max(floor, oldest)
            if start > yesterday:
                continue
            span = (yesterday - start).days + 1
            gaps[table_name] = [
                day for n in range(span) if (day := start + timedelta(days=n)) not in present
            ]

    try:
        from observability.metrics import rollup_date_gaps

        for table_name, days in gaps.items():
            rollup_date_gaps.labels(table=table_name).set(len(days))
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning("rollup date-gap gauge not published", exc_info=True)

    for table_name, days in gaps.items():
        if not days:
            continue
        logger.warning(
            "daily_rollup_date_gap",
            table=table_name,
            gap_days=len(days),
            # Exact count above; the list is a bounded SAMPLE — see _GAP_LOG_SAMPLE.
            dates=[str(d) for d in days[:_GAP_LOG_SAMPLE]],
            truncated=len(days) > _GAP_LOG_SAMPLE,
            note=(
                "no daily rollup row for these complete days inside the table's "
                "own covered span. NOT auto-repaired: a day with no billable "
                "activity legitimately has no row, and re-rolling a day whose "
                "source events are gone would DELETE the surviving bucket rather "
                "than rebuild it. Confirm against the raw sources before acting."
            ),
        )

    return gaps


async def daily_actor_axis_cardinality(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    """Distinct REAL ``actor_id`` values in ``billing_cost_actor_daily``. REPORTING ONLY.

    ⭐ THE OBSERVATION THIS COVERS. ``CostActorDaily.actor_id`` is coalesced from
    ``cost_events.actor_id`` (:func:`rollup_daily`'s ``_cactor``), which alembic 0021 made
    nullable and NEVER defaulted -- "no emitter sends this yet; this is the wire contract
    future emitters land on" (``services.cost_flusher``). Until an emitter adopts it, every
    row's actor collapses to :data:`NULL_ACTOR_ID`, the
    ``00000000-0000-0000-0000-000000000005`` sentinel, and a dashboard grouped by actor
    renders exactly one bar -- indistinguishable, on the wire, from a working per-user
    breakdown. This is NOT a defect in the rollup: ``subject_id`` (WHO PAYS) is fully
    populated and is the axis every invoice, overview and reprice reader is actually
    written against; ``actor_id`` (WHO ACTED) is a second, additive axis that is simply
    unfed today. See ``CostActorDaily``'s own docstring.

    This function measures the ONE fact that was previously only implied by a chart nobody
    questioned: how many distinct real actors currently have rows. A reading of 0 is the
    CORRECT and EXPECTED state for a fleet with no actor-sending emitters onboarded --
    treat this as context, not an alarm, and read it alongside the rollout of an
    actor-aware emitter rather than in isolation.
    """
    async with session_factory() as session:
        distinct_actors = (
            await session.execute(
                select(func.count(func.distinct(CostActorDaily.actor_id))).where(
                    CostActorDaily.actor_id != NULL_ACTOR_ID
                )
            )
        ).scalar_one()

    try:
        from observability.metrics import cost_actor_daily_distinct_actors

        cost_actor_daily_distinct_actors.set(distinct_actors)
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning("actor axis cardinality gauge not published", exc_info=True)

    return distinct_actors


async def missing_event_hours(
    session_factory: async_sessionmaker[AsyncSession],
    floor: datetime,
) -> list[datetime]:
    """Hour buckets that have raw metering_events but no metering_hourly row.

    rollup_hourly only ever rolls the immediately-previous hour, so a
    rollup-loop outage leaves an *interior hole* in metering_hourly (the raw
    events survive in metering_events, but the hourly aggregate for those hours
    was never written). This returns the distinct UTC hour boundaries in
    ``[floor, current_hour)`` that still need a (re-)roll, so the catch-up can
    refill them gap-aware — a plain ``max(bucket_hour)`` watermark can't fill an
    interior hole. Bounded by ``floor`` (the raw-event retention window).

    ⚠️ Resolved through :data:`metering_event_time`, the same expression
    :func:`rollup_hourly` buckets by. If this asked on ``created_at`` while the
    roller wrote buckets by event time, the repair list would name hours the
    roller does not write and MISS hours it does: a midnight-crossing event would
    be reported as a permanent gap on its INGEST hour (re-rolled forever, never
    satisfied) while the event hour it actually belongs to went unreported. Same
    reasoning, and the same shared-expression rule, as
    :func:`missing_daily_dates`.
    """
    cur_bucket = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    event_hour = _event_hour_expr()
    present = select(MeteringHourly.bucket_hour).where(MeteringHourly.bucket_hour >= floor)
    stmt = (
        select(event_hour)
        .where(
            # Sargable lower bound on the bare partition-free `created_at` as an
            # `ix_events_created` hint. Sound because `event_ts <= created_at` for
            # every stored row — see rollup_hourly's predicate for the invariant.
            MeteringEvent.created_at >= floor,
            metering_event_time >= floor,
            metering_event_time < cur_bucket,
        )
        .group_by(event_hour)
        .except_(present)
    )
    async with session_factory() as session:
        result = await session.execute(stmt)
    return sorted(row[0] for row in result.all())


async def late_event_hours(
    session_factory: async_sessionmaker[AsyncSession],
    floor: datetime,
) -> list[datetime]:
    """Hour buckets in ``[floor, current_hour)`` that received a row AFTER they closed.

    🔴 THIS IS THE HALF THAT KEEPS EVENT-TIME BUCKETING FROM LOSING MONEY, and it
    is not optional. :func:`missing_event_hours` only reports hours with NO
    ``metering_hourly`` row at all, so it structurally cannot see this case: the
    bucket EXISTS, it is simply short. And the hourly UPSERT REPLACES a bucket's
    counts (``request_count = excluded.request_count``) rather than adding to
    them, so a late row is only ever counted if the whole hour is recomputed.

    Under the old ``created_at`` bucketing this could not happen — an event was
    counted into the hour it arrived in, which by definition was still open. Once
    the bucket comes from the PRODUCER's clock, a stalled or replayed consumer can
    deliver a row for an hour that was rolled hours ago. ``run_rollup_loop``
    re-rolls the previous hour on every tick, so lag up to ~1h self-heals; beyond
    that, nothing did, and the failure would have been a SILENT under-count —
    strictly worse than the mis-bucketing this change replaces.

    THE PREDICATE, and why it is deliberately a superset. An hour needs re-rolling
    iff a row joined it after its LAST roll, and nothing records when that was. So
    this asks the conservative, recorded question instead: did any row whose
    resolved event time falls in hour ``H`` have ``created_at >= H + 1 hour``,
    i.e. arrive after ``H`` closed? That over-reports (it also names hours whose
    late row arrived during ``H+1``, while the forward roller was still revisiting
    them) and the cost of a false positive is one idempotent re-roll of an hour
    that recomputes to the same numbers. Under-reporting would cost revenue.

    ⚠️ ``floor`` MUST STAY INSIDE THE DAILY RECONCILE WINDOW. Re-rolling an hour
    whose DAY ``catchup_daily`` no longer revisits updates ``metering_hourly`` and
    leaves ``metering_daily`` — the table invoices are built from — stale, which
    makes the two stages disagree about the same hour. The caller passes a floor
    derived from ``settings.daily_reconcile_days`` for exactly that reason. Rows
    older than that window are the residual this change does not close, and they
    are the metering twin of the cost bus's already-measured
    ``billing_cost_event_ts_backdated_past_rollup_total``.
    """
    cur_bucket = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    event_hour = _event_hour_expr()
    stmt = (
        select(event_hour)
        .where(
            # Sargable lower bound, same invariant as everywhere else here.
            MeteringEvent.created_at >= floor,
            metering_event_time >= floor,
            metering_event_time < cur_bucket,
            # Arrived after its own hour had closed: the INGEST hour is strictly
            # later than the EVENT hour. Deliberately a comparison of two
            # identically-shaped naive UTC hours rather than
            # `created_at >= event_hour + interval '1 hour'` — the truncated
            # expression is an untyped SQL function, so interval arithmetic on it
            # is a compile-time hazard, while `>` on two of them is not.
            _naive_utc_hour(MeteringEvent.created_at) > _naive_utc_hour(metering_event_time),
        )
        .group_by(event_hour)
    )
    async with session_factory() as session:
        result = await session.execute(stmt)
    hours = sorted(row[0] for row in result.all())
    if hours:
        logger.info(
            "metering_hours_received_late_events",
            hours=len(hours),
            oldest=hours[0].isoformat(),
            newest=hours[-1].isoformat(),
            note=(
                "raw metering events arrived for hours that had already closed; "
                "those hourly buckets are being recomputed so the event-time "
                "bucket is not left short. NOT an error."
            ),
        )
    return hours


async def cleanup_old_events(
    session_factory: async_sessionmaker[AsyncSession],
    retention_days: int,
    *,
    now: datetime | None = None,
) -> int:
    """Delete raw ``metering_events`` older than the retention period.

    ``cost_events`` is deliberately NOT handled here: it is monthly
    RANGE-partitioned, so its retention is partition-native via
    :func:`drop_old_cost_partitions` (DROP a whole aged partition — instant
    reclaim, no dead-tuple bloat from row-DELETE-ing a 100MB+ table).
    ``metering_events`` is a plain table and stays on DELETE.
    """
    # ``now`` is injectable so the retention BOUNDARY is testable. Without it a
    # test can only seed at its own ``now - retention_days`` and hope: this
    # function recomputes the cutoff milliseconds LATER, so the seeded row is
    # always strictly older and always deleted, and the `<` vs `<=` distinction
    # below can never be observed. Production passes nothing and reads the clock.
    cutoff = (now or datetime.now(UTC)) - timedelta(days=retention_days)

    async with session_factory() as session:
        result = cast(
            "CursorResult[Any]",
            await session.execute(delete(MeteringEvent).where(MeteringEvent.created_at < cutoff)),
        )
        await session.commit()
        total = result.rowcount or 0

    if total:
        logger.info("retention_cleanup", deleted=total, cutoff=cutoff.isoformat())
    return total


async def cleanup_old_processed_messages(
    session_factory: async_sessionmaker[AsyncSession],
    retention_days: int = PROCESSED_MESSAGE_RETENTION_DAYS,
) -> int:
    """Prune both stream idempotency inboxes (``processed_cost_messages`` +
    ``processed_event_messages``).

    A row only needs to outlive its Redis stream message's redelivery window.
    The rollup loop / consumer age-trim EACH stream to its ``*_STREAM_MAX_AGE_HOURS``
    (36h for both cost and events), strictly below this 48h prune horizon, so once
    an entry is old enough to be dropped here it can no longer exist in its stream
    (and thus can never be redelivered via a NOGROUP replay), making the inbox row
    safe to drop. Keeps the tables tiny without weakening exactly-once.
    """
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)

    total = 0
    async with session_factory() as session:
        for model in (ProcessedCostMessage, ProcessedEventMessage):
            result = cast(
                "CursorResult[Any]",
                await session.execute(delete(model).where(model.processed_at < cutoff)),
            )
            total += result.rowcount or 0
        await session.commit()

    if total:
        logger.info("processed_messages_cleanup", deleted=total, cutoff=cutoff.isoformat())
    return total


async def trim_stream(
    redis_conn: Any,
    stream: str,
    max_age_hours: int = COST_STREAM_MAX_AGE_HOURS,
) -> int:
    """Age-bound a Redis stream via ``XTRIM <stream> MINID ~ <cutoff_ms>``.

    The producer bounds each stream (cost + events) by COUNT (``XADD MAXLEN ~
    100000``), not age, so under low volume entries can outlive the idempotency
    inbox's prune horizon (:data:`PROCESSED_MESSAGE_RETENTION_DAYS`). A NOGROUP
    recovery recreates the group at id=0 and mass-replays every retained entry;
    an entry whose inbox row was already pruned would then be re-processed
    (cost: re-insert + re-debit; events: re-insert + additively double-count
    request_count/denied_count + double-increment the quota counter) = the harm
    the inbox exists to prevent.

    Trimming the stream so its oldest entry is younger than ``max_age_hours``
    (kept strictly below the inbox retention) guarantees any replayable entry
    still has its dedup row, so the claim in ``flush_costs`` / ``flush_events``
    drops it. Redis stream ids are ``<ms>-<seq>``; the cutoff is the service-clock
    millisecond boundary. ``~`` (approximate) lets Redis trim at macro-node
    granularity for efficiency — it never trims a TOO-young entry, only possibly
    keeps a few slightly-older ones, which is safe (they still have inbox rows).

    Stream-agnostic: the ``stream=`` log label disambiguates which stream was
    trimmed. No-op (returns 0) when ``redis_conn`` is None. Returns entries removed.

    ⭐ Every call also increments ``billing_cost_dead_letter_expired_total``
    {stream=...} by exactly this return value. XTRIM's ``~`` makes the trim
    APPROXIMATE — Redis may keep a few entries slightly older than
    ``cutoff_ms`` for macro-node efficiency — so the count of entries this
    call actually destroyed is never derivable from ``max_age_hours`` or from
    re-reading the stream after the fact; it is only ever the integer XTRIM
    itself reports. That is why the metric is incremented with ``removed``
    (the SDK's own return value), never with an independently computed
    estimate: the two can never disagree, because they are the same number.
    See observability.metrics.cost_dead_letter_expired_total for why this
    exists (the age-trim loss the depth-based alert cannot see).
    """
    if redis_conn is None:
        return 0
    cutoff_ms = int((datetime.now(UTC) - timedelta(hours=max_age_hours)).timestamp() * 1000)
    removed = await redis_conn.xtrim(stream, minid=cutoff_ms, approximate=True)
    removed = removed or 0
    observability_metrics.cost_dead_letter_expired_total.labels(stream=stream).inc(removed)
    if removed:
        logger.info(
            "stream_trimmed",
            stream=stream,
            removed=removed,
            max_age_hours=max_age_hours,
        )
    return removed


# ---------------------------------------------------------------------------
# cost_events partition lifecycle
#
# cost_events is monthly RANGE-partitioned on created_at (alembic 0012). Two
# things must happen for the table not to "get out of hand":
#   * future partitions must exist before rows arrive (else INSERT hard-fails),
#   * fully-aged partitions must be dropped (retention + space reclaim).
# 0012 installed the idempotent DB helper create_cost_events_partition_for and
# documented a gitops cron to call it — but the cron was never built. The rollup
# loop now drives both on its daily pass. The decision logic is factored into
# pure helpers so the month/year arithmetic and the "is this partition fully
# aged" rule are unit-testable without a real partitioned table.
# ---------------------------------------------------------------------------

# Partitions are named ``cost_events_YYYY_MM``. The standalone pre-partition
# backup ``cost_events_old`` and the parent table never match, so discovery
# filtered on this pattern can never touch them.
_COST_PARTITION_RE = re.compile(r"^billing_cost_events_(\d{4})_(0[1-9]|1[0-2])$")

#: Most ``cost_events`` partitions ONE retention run may DROP before it refuses.
#:
#: Steady state is 1 and only on one day a month: partitions are monthly, the
#: retention loop runs daily, so on ~30 days of 31 the aged set is empty and on
#: the 31st exactly one month has fully aged out. 2 allows a single missed month
#: (pod down over a boundary) and nothing more.
#:
#: This bound exists because ``DROP TABLE`` is invisible to BOTH controls built
#: after the 2026-09-05 loss: ``models/db/truncate_guard.py`` is a BEFORE TRUNCATE
#: trigger and ``models/db/delete_audit.py`` is DELETE/TRUNCATE-only (its module
#: docstring: "DROP TABLE fires no row or statement DELETE trigger, so retention
#: is invisible to this control by construction"). A bad ``retention_days`` can
#: therefore destroy every completed month in one transaction while
#: ``billing_delete_audit`` stays correctly EMPTY — the destroyed day is
#: indistinguishable from a healthy one. ``settings.MIN_RETENTION_DAYS`` closes
#: the config door; this closes the argument door, which a caller invoking the
#: function directly (tests, scripts, a future loop) uses instead.
MAX_COST_PARTITIONS_DROPPED_PER_RUN = 2


def _months_to_ensure(now: datetime, months_ahead: int) -> list[tuple[int, int]]:
    """(year, month) pairs from the current month through ``months_ahead`` ahead.

    Pure helper so month/year rollover is unit-testable without a DB.
    """
    base = now.year * 12 + (now.month - 1)
    out: list[tuple[int, int]] = []
    for offset in range(months_ahead + 1):
        year, month_idx = divmod(base + offset, 12)
        out.append((year, month_idx + 1))
    return out


def _aged_cost_partitions(names: Iterable[str], cutoff: datetime) -> list[str]:
    """Names of ``cost_events`` partitions whose entire month is older than cutoff.

    A monthly partition ``cost_events_YYYY_MM`` covers ``[month, next-month)``.
    It is safe to drop only when its exclusive upper bound (next-month start) is
    ``<= cutoff`` — a partition straddling the cutoff still holds in-retention
    rows and is kept. Partition boundaries are stored at midnight UTC, so the
    UTC upper bound derived from the name matches the stored bound exactly.
    Names not matching the monthly pattern (e.g. ``cost_events_old``) are
    ignored. Pure helper.
    """
    aged: list[str] = []
    for name in names:
        match = _COST_PARTITION_RE.match(name)
        if match is None:
            continue
        year, month = int(match.group(1)), int(match.group(2))
        upper_year, upper_month = (year + 1, 1) if month == 12 else (year, month + 1)
        upper_bound = datetime(upper_year, upper_month, 1, tzinfo=UTC)
        if upper_bound <= cutoff:
            aged.append(name)
    return sorted(aged)


async def ensure_cost_event_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    months_ahead: int,
) -> list[str]:
    """Idempotently ensure ``cost_events`` has partitions for the coming months.

    Calls the idempotent DB helper ``create_cost_events_partition_for`` for the
    current month through ``months_ahead`` ahead, keeping a rolling runway so an
    INSERT never hits a missing partition. Returns the partition names touched
    (created or already-present).
    """
    touched: list[str] = []
    async with session_factory() as session:
        for year, month in _months_to_ensure(datetime.now(UTC), months_ahead):
            name = (
                await session.execute(
                    text("SELECT create_cost_events_partition_for(:y, :m)"),
                    {"y": year, "m": month},
                )
            ).scalar_one()
            touched.append(name)
        await session.commit()
    logger.info("cost_partitions_ensured", partitions=touched)
    return touched


async def drop_old_cost_partitions(
    session_factory: async_sessionmaker[AsyncSession],
    retention_days: int,
) -> list[str]:
    """Drop whole ``cost_events`` partitions older than the retention window.

    Partition-native retention: dropping a fully-aged monthly partition reclaims
    space instantly with no dead-tuple bloat. Only partitions whose entire month
    is older than ``retention_days`` are dropped (see :func:`_aged_cost_partitions`),
    so retention is partition-granular — up to ~1 extra month is kept until the
    boundary month fully ages out. Returns the dropped partition names. A no-op
    (returns ``[]``) when ``cost_events`` is not partitioned.

    REFUSES, dropping nothing and returning ``[]``, when either
    * ``retention_days <= 0`` — the same convention ``audit.purge_old_audit_log``
      uses for the sibling purge on this very tick, except that for a DROP a
      cutoff of ``now`` is not "retention disabled", it is "destroy every
      completed month"; or
    * the computed drop set exceeds :data:`MAX_COST_PARTITIONS_DROPPED_PER_RUN`.

    Both refusals log at ERROR, as does the drop itself — with the partition
    names and their exact row counts, captured BEFORE the DROP, because after it
    nothing can reconstruct them. ``LOG_LEVEL=INFO`` is production, so the old
    single INFO line was the entire forensic record of an irreversible
    destruction that no trigger sees (see :data:`MAX_COST_PARTITIONS_DROPPED_PER_RUN`).

    It refuses rather than raises deliberately. The one caller is the leader-only
    rollup loop, whose broad ``except Exception`` arm counts a raise as a failed
    cycle and, after ``MAX_CONSECUTIVE_ROLLUP_FAILURES``, hands the advisory lock
    to the next replica — which meets the same input and dies the same way. A
    guard against data loss must not be able to cascade every rollup leader in
    the fleet to zero. Refusing stalls retention (disk grows, recoverable) and
    leaves the rest of the daily maintenance running; proceeding destroys
    financial records (unrecoverable). The ERROR log is the alarm.
    """
    if retention_days <= 0:
        logger.error(
            "cost_partition_drop_refused",
            reason="non_positive_retention_days",
            retention_days=retention_days,
            max_allowed=MAX_COST_PARTITIONS_DROPPED_PER_RUN,
        )
        return []
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    async with session_factory() as session:
        rows = await session.execute(
            text(
                "SELECT c.relname FROM pg_inherits i "
                "JOIN pg_class c ON c.oid = i.inhrelid "
                "WHERE i.inhparent = 'billing_cost_events'::regclass"
            )
        )
        names = [row[0] for row in rows.all()]
        dropped = _aged_cost_partitions(names, cutoff)
        if not dropped:
            return []
        if len(dropped) > MAX_COST_PARTITIONS_DROPPED_PER_RUN:
            logger.error(
                "cost_partition_drop_refused",
                reason="drop_set_exceeds_bound",
                partitions=dropped,
                partition_count=len(dropped),
                max_allowed=MAX_COST_PARTITIONS_DROPPED_PER_RUN,
                retention_days=retention_days,
                cutoff=cutoff.isoformat(),
            )
            return []
        # Count BEFORE the DROP: this is the only record that will exist of what
        # was destroyed. Names come from _COST_PARTITION_RE → safe to interpolate.
        row_counts = {
            name: (await session.execute(text(f'SELECT count(*) FROM "{name}"'))).scalar_one()
            for name in dropped
        }
        logger.error(
            "cost_partitions_dropping",
            partitions=dropped,
            row_counts=row_counts,
            rows_total=sum(row_counts.values()),
            retention_days=retention_days,
            cutoff=cutoff.isoformat(),
        )
        for name in dropped:
            # name is validated by _COST_PARTITION_RE → safe to interpolate;
            # DROP TABLE cannot be parameterized.
            await session.execute(text(f'DROP TABLE IF EXISTS "{name}"'))
        await session.commit()
    logger.info("cost_partitions_dropped", partitions=dropped, cutoff=cutoff.isoformat())
    return dropped
