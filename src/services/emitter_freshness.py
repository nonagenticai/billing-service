"""Per-emitter last-seen freshness, derived from the LEDGER rather than from flow.

🔴 THE GAP THIS CLOSES: A DEAD EMITTER IS CURRENTLY INVISIBLE, AND THAT IS NOT A
HYPOTHESIS. Measured on prod over 7 days, ``billing_cost_events`` carries **13
providers across 12 distinct ``service_key``s**. The instrument that looks like it
would notice one of them stopping — ``billing_cost_events_by_service_total`` — had
live series for only **6**, because it is a per-pod Counter born at process start
whose child series exists only once that key emits *again*. So "no series" and
"this emitter has been dead for three months" are the same empty vector, and an
empty vector reads as a measured zero to anyone writing an alert. That is the
exact instrument-shaped trap that produced a wrong conclusion in an earlier round.

⛔ AND FOUR OF THEM CANNOT BE WATCHED FROM THEIR OWN SIDE AT ALL. ``asr``, ``tts``,
``image-gen`` and ``lean4-prover`` have **no Prometheus scrape target** — they are
launchd services on mini1/mini2, not pods — so "ask the emitter whether it is
alive" is not available for them at any price. The ledger is the only place their
liveness is observable, which is why this measurement lives here and not in a
blackbox probe. Known-real cases this would have named: ``mflux`` (image-gen) last
emitted **2026-09-10 18:39**, and ``asr`` has been dormant since **2026-05-27**.

⭐ IT PUBLISHES A TIMESTAMP, NOT AN AGE, and that is the whole design decision.
An age computed in this process is correct only at the instant it is computed and
then rots between scrapes — and worse, it rots *silently*, because a Gauge keeps
exporting its last value for the life of the process. A unix timestamp does not
rot: the consumer writes ``time() - billing_cost_events_last_seen_timestamp_seconds``
and gets a true age at evaluation time, on every evaluation, forever. The same
reasoning is written out at length on ``billing_metering_hourly_lag_timestamp_seconds``.

⚠️ A FROZEN SERIES AGES FORWARD, WHICH IS CORRECT AND ALSO A FOOTGUN. This never
deletes a label it has published, so an emitter that stops emitting keeps its
last-seen timestamp and its computed age climbs — that is precisely the signal.
The consequence is that a **deliberately retired** emitter also climbs forever,
until retention drops its rows (which does NOT clear the series — it is
process-lifetime state) or the pod restarts. Retire an emitter by silencing or
by excluding its label in the rule, never by "fixing" this function to forget it.
"""

from __future__ import annotations

from datetime import UTC

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import CostEvent

logger = structlog.stdlib.get_logger(__name__)

#: Hard ceiling on how many ``(service_key, provider)`` series one pass publishes.
#:
#: ⛔ WHY A CAP EXISTS AT ALL: both labels are ``varchar(64)`` columns fed from
#: PRODUCER INPUT (``services._parsers``), so the label domain is OPEN — there is
#: no finite roster to enumerate, the way ``LICENCE_BUDGET_RECONCILE_FAILURE_REASONS``
#: enumerates its own. An emitter with a buggy or per-request ``service_key`` (a
#: request id, a hostname, a model name) would mint one permanent Prometheus time
#: series per distinct value, and a labelled Gauge never forgets a child. That is
#: a cardinality incident in the scrape target, not in this process, so nothing
#: here would look unhealthy while it happened.
#:
#: WHY 50: the measured population is 13 providers over 12 service_keys — at most
#: ~20 real pairs — so 50 is ~2.5x headroom for genuine growth while still being a
#: number an operator can eyeball in a dashboard. Raising it is cheap; raising it
#: because the drop counter is non-zero is usually WRONG — a non-zero drop counter
#: on a 13-provider estate means someone is minting label values, and the fix is
#: upstream in the emitter.
MAX_FRESHNESS_SERIES = 50

#: ONE aggregate: the newest ingest time per emitting pair.
#:
#: ⛔ IT IS ``created_at``, NOT ``event_ts`` AND NOT ``coalesce(event_ts, created_at)``.
#: ``event_ts`` was only added by alembic 0028 and populated from 2026-09-11 13:57Z
#: — **95.6% of rows have it NULL**, never backfilled, that history is gone — so a
#: ``max(event_ts)`` would read as "last seen 2026-09-11" for every emitter that
#: has not written since, i.e. it would report the MIGRATION as the liveness of the
#: fleet. The COALESCE is wrong for a different and worse reason: ``event_ts`` is
#: producer-asserted and may legitimately be back-dated up to 90 days
#: (``_parsers.MAX_EVENT_TS_BACKDATE_DAYS``) or, being off the wire, forward-dated
#: by a broken clock — one such row would pin an emitter's last-seen into the
#: future and make a dead emitter unfireable. The question here is "when did a row
#: from this emitter LAST LAND", which is an ingest-time question and has exactly
#: one honest answer. ``rollup_engine.cost_events_oldest_age_seconds`` declines
#: ``event_ts`` for the mirror image of this reason; read its docstring.
#:
#: ⚠️ NO SQL ``LIMIT``, DELIBERATELY, even though only MAX_FRESHNESS_SERIES rows
#: are published. ``LIMIT 50`` would make the overflow UNMEASURABLE: the result set
#: would saturate at exactly the cap and "50 pairs exist" would be
#: indistinguishable from "5,000 pairs exist", so the drop counter — the only thing
#: that can report a cardinality incident upstream — would read 0 forever. The row
#: count here is the distinct-pair cardinality itself (~20 today), not a scan of
#: the ledger, so the exact overflow is cheap to know and is worth knowing.
_LAST_SEEN_SQL = (
    select(
        CostEvent.service_key,
        CostEvent.provider,
        func.max(CostEvent.created_at).label("last_seen"),
    )
    .group_by(CostEvent.service_key, CostEvent.provider)
    .order_by(func.max(CostEvent.created_at).desc())
)


async def publish_emitter_freshness(session: AsyncSession) -> int:
    """Publish last-seen age per (service_key, provider) from the ledger itself.

    Sets ``billing_cost_events_last_seen_timestamp_seconds{service_key, provider}``
    to the unix time of that pair's newest ``created_at``, and returns the NUMBER
    OF SERIES PUBLISHED so the caller can log it. REPORTING ONLY — one aggregate
    read, no writes, no repair.

    ⛔ THE CALLER MUST BE LEADER-GATED, and this function cannot check that for
    itself. The leader guard for this loop lives in the CALLER
    (``services.aggregator.run_rollup_loop`` proves leadership with
    ``election.is_leader()`` and ``continue``s when false, exactly as it does for
    :func:`services.rollup_engine.cost_events_oldest_age_seconds` and
    :func:`services.ledger_guard_audit.run_ledger_guard_audit`), so calling this
    from an ungated path puts every replica's reading into the same series.

    ⚠️ CONSEQUENTLY: AGGREGATE WITH ``max()``, NEVER ``sum()`` OR ``avg()``. A
    follower publishes NOTHING here — it does not publish 0, and it must never be
    changed to, because an earlier round lost time to a follower publishing
    ``0``/``NaN`` that was read as "last seen at the epoch", i.e. as an ancient
    success rather than as an abstention. ``max()`` over the fleet picks the
    leader's reading and ignores every replica that has not measured; a ``sum()``
    would add stale ex-leader timestamps together into a number that is not a
    time at all. An ex-leader keeps exporting its last reading for the life of the
    process, so pair the rule with a recent
    ``billing_metering_hourly_lag_timestamp_seconds`` — written by the same loop on
    the same leader tick — as the freshness selector, rather than minting another.

    ⛔ IT NEVER RAISES. A measurement may not abort the rollups it rides with, and
    this one rides the tick that does the actual billing work. Both the read and
    the publish are suppressed separately and the function returns 0, which the
    caller should read as "published nothing", NOT as "no emitters exist" — those
    two are distinguished by the log line, never by the return value.
    """
    try:
        rows = (await session.execute(_LAST_SEEN_SQL)).all()
    except Exception:  # pragma: no cover - a measurement must not break the tick
        logger.warning(
            "emitter_freshness_query_failed",
            note=(
                "billing_cost_events last-seen aggregate could not be read; every "
                "billing_cost_events_last_seen_timestamp_seconds series keeps its "
                "previous value and therefore keeps AGEING. Read it against the "
                "tick's freshness timestamp before concluding an emitter is dead."
            ),
            exc_info=True,
        )
        return 0

    published = 0
    dropped = max(len(rows) - MAX_FRESHNESS_SERIES, 0)

    try:
        from observability.metrics import (
            cost_events_last_seen_series_dropped_total as _dropped,
        )
        from observability.metrics import (
            cost_events_last_seen_timestamp_seconds as _gauge,
        )

        # Rows arrive newest-first, so the cap keeps the pairs an operator is most
        # likely to be asked about. ⚠️ That ordering is also the cap's weakness and
        # is why the drop counter is not optional: a flood of freshly-minted label
        # values sorts to the TOP and evicts the genuinely-interesting dormant
        # emitters. Non-zero drops means "do not trust this series set", not
        # "raise the cap".
        for service_key, provider, last_seen in rows[:MAX_FRESHNESS_SERIES]:
            if last_seen is None:
                # Cannot happen for a grouped max over a NOT NULL column, and is
                # skipped rather than published as 0: a 0 here would read as
                # "last seen 1970" and fire as the oldest emitter in the estate.
                continue
            # ``created_at`` is timestamptz; normalise to UTC before taking the
            # POSIX timestamp so a session timezone cannot shift it. A tz-naive
            # value is assumed UTC, the same assumption rollup_engine makes.
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=UTC)
            _gauge.labels(service_key=service_key, provider=provider).set(last_seen.timestamp())
            published += 1

        if dropped:
            _dropped.inc(dropped)
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning(
            "emitter_freshness_gauge_not_published — "
            "observability.metrics.cost_events_last_seen_timestamp_seconds is missing",
            exc_info=True,
        )
        return 0

    if dropped:
        logger.warning(
            "emitter_freshness_series_capped",
            distinct_pairs=len(rows),
            published=published,
            dropped=dropped,
            cap=MAX_FRESHNESS_SERIES,
            note=(
                "more distinct (service_key, provider) pairs exist than this gauge "
                "will publish. The measured estate has ~20; a larger number means "
                "an emitter is minting label values from request-scoped data. Fix "
                "the emitter — do NOT raise MAX_FRESHNESS_SERIES to make this quiet."
            ),
        )

    return published
