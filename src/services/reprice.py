"""Retroactively price cost events that were flushed during a pricing gap.

``billing_cost_events.cost_usd`` is computed **exactly once**, at flush time
(:func:`services.cost_flusher.flush_costs` -> :func:`services.pricing.compute_cost_usd`).
If no ``provider_pricing`` row matched at that instant the column is written
NULL and *nothing in the system ever recomputes it* — adding the correct
pricing row afterwards fixes future events only. The already-landed events stay
NULL forever, so their spend is invisible to ``billing_cost_daily``, to
/api/usage, and to every invoice fold.

This module is the repair path: it re-resolves pricing for NULL-cost events in
a bounded window and, on a real run, restates the affected
``billing_cost_daily`` buckets so the raw table and the rollup agree again.

Design notes that matter for correctness
----------------------------------------
**Which timestamp?** ``compute_cost_usd`` resolves an *effective-dated* pricing
row, so repricing with the wrong instant silently picks the wrong rate.
``flush_costs`` captures ``now = datetime.now(UTC)`` once per batch
(cost_flusher.py:83), carries it through ``pre_rows`` (:153) and passes it as
``ts=`` (:224). The same flush then INSERTs the row without an explicit
``created_at``, so the column takes its ORM default ``_utcnow()`` microseconds
later. ``created_at`` is therefore the persisted equivalent of the ``ts`` the
original pricing lookup used — it is what we replay with here.

**Which model?** ``flush_costs`` prices on ``served_model or model``
(cost_flusher.py:107 -> :211), so we mirror that fallback rather than reading
``served_model`` alone (rows predating the ``served_model`` column have it NULL
with the legacy ``model`` populated).

**Which bucket_date?** ``rollup_daily`` groups cost events by
``func.cast(func.timezone("UTC", rollup_engine.cost_event_time), Date)``
  (one shared object — see rollup_engine.py:70)
and stores that as ``bucket_date``. We compute the affected dates with the *same*
UTC-explicit SQL expression rather than a Python ``created_at.date()``: a bare
``cast(timestamptz AS date)`` is evaluated in the session's TimeZone (prod's
pgbouncer session is Europe/Amsterdam, not UTC), so the ``AT TIME ZONE 'UTC'``
wrap is what keeps the affected-date set aligned with the UTC buckets and stops a
restatement from drifting a day.

**Pricing lookup is cached.** ``flush_costs`` calls ``compute_cost_usd`` once
per event, and each call re-SELECTs the provider's whole pricing history — an
N+1 that is tolerable for a 100-event flush and not for a 17k-event backfill.
Here the resolved row is memoized per
``(provider, served_model, service_key, pricing_segment)``. The segment is the
index of the interval between two consecutive ``effective_from``/
``effective_until`` boundaries; the set of effective pricing rows is constant
inside one segment, so the memo can never return a row from the wrong
effective-dated era. The composition
``_resolve_pricing_row`` + ``_cost_from_row`` used below is exactly what
``compute_cost_usd`` does internally (see ``compute_cost_usd_with_cost_model``)
— same code, same 4-rung precedence, same rounding, and since the unit check
landed it also passes each event's ``quantity_unit`` so the repair path refuses
exactly what the flush path refuses.
"""

from __future__ import annotations

import bisect
import uuid
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import structlog
from sqlalchemy import Date, Table, and_, bindparam, func, or_, select, union, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import observability.metrics as observability_metrics
from models.db import CostEvent, ProviderPricing
from models.domain import (
    MAX_REPRICE_WINDOW_DAYS,
    RepriceProviderStat,
    RepriceResult,
)
from services import rollup_engine
from services.pricing import _cost_from_row, _resolve_pricing_row

logger = structlog.stdlib.get_logger(__name__)

_COST_EVENTS: Table = cast("Table", CostEvent.__table__)

# Single-row UPDATE reused with executemany. Two guards are deliberate:
#   * ``created_at =`` gives the partition pruner the RANGE key, so the UPDATE
#     touches one monthly partition instead of all of them;
#   * ``cost_usd IS NULL`` re-asserts the core invariant *in SQL*, so even a
#     logic bug upstream cannot overwrite an already-priced event.
# Bind params are ``b_``-prefixed so they cannot collide with column names in
# the executemany parameter dicts.
_UPDATE_COST_USD = (
    update(_COST_EVENTS)
    .where(
        _COST_EVENTS.c.id == bindparam("b_id"),
        _COST_EVENTS.c.created_at == bindparam("b_created_at"),
        _COST_EVENTS.c.cost_usd.is_(None),
    )
    .values(
        cost_usd=bindparam("b_cost_usd"),
        pricing_id=bindparam("b_pricing_id"),
    )
)


def _as_utc(value: datetime) -> datetime:
    """Treat an offset-naive datetime as UTC; pass aware ones through."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


async def _pricing_boundaries(session: AsyncSession, *, provider: str | None) -> list[datetime]:
    """Sorted instants at which the effective set of ``rate_class='cost'`` rows changes.

    Every ``effective_from`` and every non-NULL ``effective_until``. Between two
    consecutive boundaries the set of rows satisfying ``_resolve_pricing_row``'s
    ``effective_from <= ts < effective_until`` predicate is invariant, which is
    what makes the per-segment resolution memo sound. Extra boundaries (e.g. from
    providers outside a ``provider`` filter) only cost cache hits — never
    correctness — so the list is intentionally allowed to be over-broad.
    """
    froms = select(ProviderPricing.effective_from.label("b")).where(
        ProviderPricing.rate_class == "cost"
    )
    untils = select(ProviderPricing.effective_until.label("b")).where(
        ProviderPricing.rate_class == "cost",
        ProviderPricing.effective_until.is_not(None),
    )
    if provider is not None:
        froms = froms.where(ProviderPricing.provider == provider)
        untils = untils.where(ProviderPricing.provider == provider)
    rows = (await session.execute(union(froms, untils))).all()
    return sorted(_as_utc(r[0]) for r in rows if r[0] is not None)


async def _dates_with_cost_events(
    session: AsyncSession, *, since: datetime, until: datetime
) -> list[date]:
    """Every ``bucket_date`` in ``[since, until)`` that has at least one cost event.

    Derived from the DB and from the *request window* only — never from what a
    particular run happened to touch. That is what makes the restatement step
    convergent: a run that crashes after some batches committed leaves
    ``billing_cost_events`` repriced but ``billing_cost_daily`` stale, and the
    retry cannot rediscover those dates from ``cost_usd IS NULL`` (they are no
    longer NULL). Re-deriving the full window means the retry restates them
    anyway.

    Deliberately NOT filtered by ``provider``: ``rollup_daily`` restates a whole
    calendar date across every provider, so narrowing here could only hide a
    date that still needs restating.
    """
    # UTC-explicit, matching rollup_daily's grouping (see its bucket note) — a
    # bare cast would derive the affected-dates set in the session's local time
    # and could restate the wrong calendar day on a non-UTC session.
    #
    # ⛔ MUST be rollup_engine's OWN expression, not a local copy of it. Since
    # alembic 0028 rollup_daily groups by coalesce(event_ts, created_at); a bucket
    # derived here from created_at alone would hand rollup_daily a date SET that
    # does not contain the date the crossing event actually rolls into, so a
    # repriced midnight-crosser would be restated into a bucket nobody asked for
    # and its true bucket would never be re-rolled at all.
    # ⚠️ The WINDOW below stays on created_at deliberately: `since`/`until` select
    # which events to RECONSIDER (an ingest-time question), while the bucket says
    # where each one BELONGS (an event-time question). An event ingested in-window
    # whose event_ts precedes it is exactly the case that must restate an earlier
    # date.
    bucket = func.cast(func.timezone("UTC", rollup_engine.cost_event_time), Date)
    stmt = (
        select(bucket.label("bucket_date"))
        .where(CostEvent.created_at >= since, CostEvent.created_at < until)
        .group_by(bucket)
        .order_by(bucket)
    )
    return [r.bucket_date for r in (await session.execute(stmt)).all()]


def _new_stat() -> dict[str, Any]:
    return {"scanned": 0, "repriced": 0, "still_unpriced": 0, "cost_usd_added": Decimal("0")}


async def reprice_null_costs(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    since: datetime,
    until: datetime,
    provider: str | None = None,
    limit: int = 1000,
    dry_run: bool = False,
) -> RepriceResult:
    """Fill in ``cost_usd``/``pricing_id`` for NULL-cost events in ``[since, until)``.

    Only rows matching ``cost_usd IS NULL AND created_at >= since AND created_at
    < until`` are considered, and the UPDATE itself re-checks ``cost_usd IS
    NULL``.

    **Invariant: a row whose ``cost_usd`` is already non-NULL is never read,
    never counted and never written.** Consequences, both relied on by callers:

    * Re-running over the same window reprices nothing — everything priced by
      the first run is excluded from the second (``repriced == 0``). It does
      still restate the rollup (see below); that is deliberate.
    * An event that still has no matching pricing row — or whose matched row
      cannot honestly price it — is counted in ``still_unpriced`` and **left
      NULL**. ``billing_cost_events.cost_usd`` is never written as ``0`` here: a
      genuine zero and "we don't know" must stay distinguishable, and a stored
      0 would be permanently invisible to the next repair run.

      ⚠️ THAT PROMISE IS SCOPED TO ``billing_cost_events``, AND IT IS NOT WHAT A
      READER SEES DOWNSTREAM. The restatement this function performs two
      paragraphs below folds those NULLs with
      ``coalesce(sum(CostEvent.cost_usd), 0)`` (``rollup_engine``) into
      ``billing_cost_daily.total_cost_usd``, a ``nullable=False`` column
      (``models/db/rollups.py``) — so an entirely unpriced provider/day lands in
      the rollup as a hard ``0.000000`` next to its real token count, which
      reads as "this spend was free" rather than "this spend is unknown".
      Measured: ``provider='openai_compatible'`` / ``service_key=
      'commonpractice'`` has NO ``provider_pricing`` row at all, and its
      2026-08-08 bucket carries ``total_cost_usd = 0.000000`` against 6,087
      tokens. Expressing that absence needs a nullable rollup column (or a
      coverage flag beside it) — ``/api/usage`` already has the idiom in its
      ``cost_unavailable`` / ``cost_unavailable_days`` fields. Neither
      ``rollup_engine`` nor the column is changed by this module.

    The window is mandatory (never widened to "everything") because
    ``billing_cost_events`` is monthly RANGE-partitioned on ``created_at`` — the
    bound is what lets Postgres prune partitions instead of scanning years.

    On a real run each batch commits as it goes (so a long backfill makes
    durable progress and never holds one giant transaction) and
    :func:`services.rollup_engine.rollup_daily` runs in **two** places:

    * immediately after each batch commit, for that batch's dates — bounding the
      raw/rollup disagreement to a single batch while a long backfill runs;
    * once at the end, for **every** ``bucket_date`` in ``[since, until)`` that
      has any cost event at all (``_dates_with_cost_events``), not just the ones
      this run touched.

    The second pass is the correctness guarantee, and it is why the date set is
    derived from the *request* rather than from in-run state. If the process
    dies after a batch commits, those events are durably repriced and therefore
    invisible to the next run's ``cost_usd IS NULL`` filter — an in-memory
    ``affected`` set could never rediscover their dates, and ``cost_daily``
    would stay silently wrong forever (the aggregator's trailing
    ``daily_reconcile_days`` window and ``missing_daily_dates`` do not cover a
    present-but-stale date weeks in the past). Re-deriving the window means
    simply re-running the same request converges. ``rollup_daily`` is an
    idempotent UPSERT on ``uq_cost_daily``, so restating an unchanged date is a
    harmless no-op, and the window is hard-capped at ``MAX_REPRICE_WINDOW_DAYS``
    so the call count is bounded by the operator's own choice of window.

    With ``dry_run=True`` everything is computed and **nothing is written** — no
    UPDATE, no rollup, no metric — but the returned result (including
    ``total_cost_usd_added``) is identical to what a real run would produce, so
    an operator can review the dollar delta before opting in.

    Raises ``ValueError`` if the window is empty/inverted or longer than
    ``MAX_REPRICE_WINDOW_DAYS``.
    """
    since = _as_utc(since)
    until = _as_utc(until)
    if since >= until:
        raise ValueError("since must be strictly before until")
    if (until - since) > timedelta(days=MAX_REPRICE_WINDOW_DAYS):
        raise ValueError(f"window must not exceed {MAX_REPRICE_WINDOW_DAYS} days")

    scanned = 0
    repriced = 0
    still_unpriced = 0
    total_added = Decimal("0")
    by_provider: dict[str, dict[str, Any]] = defaultdict(_new_stat)
    affected: set[date] = set()
    restated: set[date] = set()

    # Keyset cursor on (created_at, id). A plain OFFSET would be wrong on a dry
    # run and a plain "re-query the NULLs" loop would spin forever on the
    # still_unpriced rows (they stay NULL by design), so we always page forward.
    cursor: tuple[datetime, uuid.UUID] | None = None
    # Cache is per-run: a pricing row added mid-run is picked up by the next run.
    cache: dict[tuple[str, str | None, str | None, int], ProviderPricing | None] = {}

    async with session_factory() as session:
        boundaries = await _pricing_boundaries(session, provider=provider)

        while True:
            stmt = select(
                CostEvent.id,
                CostEvent.created_at,
                CostEvent.provider,
                CostEvent.model,
                CostEvent.served_model,
                CostEvent.service_key,
                CostEvent.input_tokens,
                CostEvent.output_tokens,
                CostEvent.cache_creation_input_tokens,
                CostEvent.cache_read_input_tokens,
                # The emitter's asserted unit for this event. Passed to
                # `_cost_from_row` so a quantity is refused rather than silently
                # multiplied by a rate from a DIFFERENT unit family — the three
                # token-family cost_models share identical arithmetic, so without
                # this the mispricing has no signature at all.
                CostEvent.quantity_unit,
                # Same UTC-explicit expression rollup_daily groups on — see module
                # docstring and rollup_engine's bucket note. Shares
                # rollup_engine.cost_event_time for the reason given at
                # _affected_bucket_dates above: a local created_at copy would label
                # a crossing event with a bucket rollup_daily will not group it
                # into.
                func.cast(func.timezone("UTC", rollup_engine.cost_event_time), Date).label(
                    "bucket_date"
                ),
            ).where(
                CostEvent.cost_usd.is_(None),
                CostEvent.created_at >= since,
                CostEvent.created_at < until,
                # A parked duplicate contributes to no aggregate, so pricing it
                # is work whose only effect is to make a duplicate look priced.
                CostEvent.excluded_from_billing.is_(False),
            )
            if provider is not None:
                stmt = stmt.where(CostEvent.provider == provider)
            if cursor is not None:
                last_ts, last_id = cursor
                stmt = stmt.where(
                    or_(
                        CostEvent.created_at > last_ts,
                        and_(CostEvent.created_at == last_ts, CostEvent.id > last_id),
                    )
                )
            stmt = stmt.order_by(CostEvent.created_at, CostEvent.id).limit(limit)

            rows = (await session.execute(stmt)).all()
            if not rows:
                break
            cursor = (rows[-1].created_at, rows[-1].id)

            params: list[dict[str, Any]] = []
            batch_by_provider: dict[str, int] = defaultdict(int)
            batch_dates: set[date] = set()

            for row in rows:
                scanned += 1
                stat = by_provider[row.provider]
                stat["scanned"] += 1

                ts = _as_utc(row.created_at)
                # Mirror cost_flusher.py:107 — `served_model` falls back to the
                # legacy `model` alias for rows predating the served_model column.
                served_model = row.served_model or row.model
                key = (
                    row.provider,
                    served_model,
                    row.service_key,
                    bisect.bisect_right(boundaries, ts),
                )
                if key in cache:
                    pricing_row = cache[key]
                else:
                    pricing_row = await _resolve_pricing_row(
                        session,
                        provider=row.provider,
                        served_model=served_model,
                        service_key=row.service_key,
                        ts=ts,
                    )
                    cache[key] = pricing_row

                if pricing_row is None:
                    still_unpriced += 1
                    stat["still_unpriced"] += 1
                    continue

                cost = _cost_from_row(
                    pricing_row,
                    input_tokens=row.input_tokens,
                    output_tokens=row.output_tokens,
                    cache_creation_input_tokens=row.cache_creation_input_tokens,
                    cache_read_input_tokens=row.cache_read_input_tokens,
                    quantity_unit=row.quantity_unit,
                )
                # A row was matched but it cannot price this usage (its rate for
                # its own cost_model was never stated, or the event's
                # quantity_unit contradicts the row's cost_model family — see
                # pricing._cost_from_row). That is STILL UNPRICED, not repriced:
                # counting it as repriced would report a successful backfill while
                # writing the same NULL back, and `total_added += None` would
                # raise. The event stays NULL so a later run repairs it once the
                # rate is filled in.
                if cost is None:
                    still_unpriced += 1
                    stat["still_unpriced"] += 1
                    continue

                repriced += 1
                stat["repriced"] += 1
                total_added += cost
                stat["cost_usd_added"] += cost
                affected.add(row.bucket_date)
                batch_dates.add(row.bucket_date)
                batch_by_provider[row.provider] += 1
                params.append(
                    {
                        "b_id": row.id,
                        "b_created_at": row.created_at,
                        "b_cost_usd": cost,
                        "b_pricing_id": pricing_row.id,
                    }
                )

            if params and not dry_run:
                await session.execute(_UPDATE_COST_USD, params)
                await session.commit()
                # Metrics only after the write is durable, and only on a real
                # run — a dry run must leave no trace anywhere.
                for prov, count in batch_by_provider.items():
                    observability_metrics.cost_events_repriced_total.labels(
                        provider=prov or "unknown"
                    ).inc(count)

                # Restate this batch's dates immediately, so a crash mid-backfill
                # leaves at most ONE batch of raw/rollup disagreement instead of
                # everything committed so far. The final full-window pass below
                # is what guarantees convergence; this is what bounds the window
                # of inconsistency during a long run.
                # NOTE (perf, follow-up PR): rollup_daily's cost query filters on
                # `cast(created_at AS date) = target_date` with no created_at
                # range bound (rollup_engine.py:255), which is NOT partition-
                # prunable. Measured on a 200k-row, 8-monthly-partition Postgres
                # 18 fixture: the cast form parallel-seq-scans ALL 8 partitions
                # (1670 shared buffers); the equivalent
                # `created_at >= d AND created_at < d+1` form prunes to the one
                # partition and uses the created_at index (237 buffers, ~7x less
                # I/O, and it scales with the partition count). This module now
                # calls rollup_daily once per date per batch plus once per date
                # in the window, so that cost is multiplied. Deliberately NOT
                # fixed here — the fix belongs in rollup_engine and would touch
                # every caller.
                for bucket_date in sorted(batch_dates):
                    await rollup_engine.rollup_daily(session_factory, bucket_date)
                    restated.add(bucket_date)

            if len(rows) < limit:
                break

    bucket_dates = sorted(affected)
    if not dry_run:
        # Convergence step. Derived from the *request window*, not from `affected`
        # — see _dates_with_cost_events. Runs unconditionally (even when this run
        # repriced nothing), because "repriced nothing" is exactly the state a
        # retry-after-crash is in. rollup_daily is an idempotent UPSERT on
        # uq_cost_daily, so restating an unchanged date is a no-op; the window is
        # hard-capped at MAX_REPRICE_WINDOW_DAYS so the call count is bounded and
        # operator-chosen.
        async with session_factory() as session:
            window_dates = await _dates_with_cost_events(session, since=since, until=until)
        for bucket_date in window_dates:
            await rollup_engine.rollup_daily(session_factory, bucket_date)
            restated.add(bucket_date)

    result = RepriceResult(
        scanned=scanned,
        repriced=repriced,
        still_unpriced=still_unpriced,
        total_cost_usd_added=total_added,
        affected_bucket_dates=bucket_dates,
        restated_bucket_dates=sorted(restated),
        by_provider=[
            RepriceProviderStat(
                provider=prov,
                scanned=s["scanned"],
                repriced=s["repriced"],
                still_unpriced=s["still_unpriced"],
                cost_usd_added=s["cost_usd_added"],
            )
            for prov, s in sorted(by_provider.items())
        ],
        dry_run=dry_run,
    )
    logger.info(
        "cost_events_repriced",
        since=since.isoformat(),
        until=until.isoformat(),
        provider=provider,
        dry_run=dry_run,
        scanned=scanned,
        repriced=repriced,
        still_unpriced=still_unpriced,
        total_cost_usd_added=str(total_added),
        restated_bucket_dates=[str(d) for d in sorted(restated)],
    )
    return result
