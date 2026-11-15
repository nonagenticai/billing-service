"""Ledger-derived gauge: spend sitting on tenant ids no registry vouches for.

THE GAP THIS CLOSES, and it is NOT "nothing detects this". Something does:
:func:`tenancy._note_unregistered_tenant` increments
``billing_tenant_unregistered_total`` every time a cost event resolves to a uuid
the licences-registry snapshot does not vouch for, and
:func:`tenancy.is_registered_tenant_id` is consulted by the cost flusher to park
such a row on ``excluded_from_billing``. Both work.

⛔ THE PROBLEM IS THAT THE EXISTING COUNTER IS BORN AFTER THE PHENOMENON.
``billing_tenant_unregistered_total`` is per-POD-lifetime. Measured 2026-09-12:
both replicas read **0**, while the ledger carried **$22.89 over 903 rows on three
tenant ids** (``ad976f5b…``, ``6c6a0da6…``, ``351fdc59…``) that resolve in none of
``licenses.tenants``, ``licenses.subject`` or ``keycloak.user_entity``. The zero is
CORRECT and still useless: those ids last emitted 2026-09-09 17:4xZ and the pods
started 2026-09-12 07:42Z, so "0" means "not since the last deploy", which is
indistinguishable from "never happens". A pod restart resets the only instrument
that could report a standing condition. That is the same trap
:mod:`services.emitter_freshness` exists to escape, and this module is its sibling:
ask the LEDGER, which is durable, rather than a counter, which is not.

⚠️ IT IS ALSO UNWATCHED. There is no alert anywhere in gitops on
``billing_tenant_unregistered_total`` — verified 2026-09-12 by grepping all three
prometheus rules ConfigMaps. A counter nobody reads and a counter that cannot
answer are two different failures; this had both.

⛔ AND UNTIL 2026-09-13 IT COULD NOT NAME ANYONE. The two gauges above are
aggregates with no tenant label, and the log line below runs every id through
``tenancy.redact_tenant_id``, which truncates anything over 24 characters — a
uuid is 36, so production logged ``ad976f5b…(redacted,len=36)`` and the full id
was recoverable from NOWHERE but a hand-run query against the prod ledger. An
operator paged on ``billing_unregistered_tenant_count > 0`` learned that four
tenants were unbillable and could not learn which. That is the defect already
filed as ``mlx_ingestion#54``, so this module now also publishes a NAMED,
cardinality-capped breakdown — see :func:`_publish_named_series`.

⛔ REPORTING ONLY. One aggregate read, no writes, no repair, and it must never
reject: resolution deliberately FAILS OPEN because rejecting before the
denominator is measured silently drops live spend (see
``tenancy._note_unregistered_tenant``). This module measures the denominator. It
does not act on it.

⚠️ AGGREGATE WITH ``max()``, NEVER ``sum()``. Like its neighbours this runs only
on the LEADER, and the caller owns that gate — a follower publishes NOTHING here,
not a zero, so a naive ``sum()`` across replicas is a half-reading and an ``avg()``
halves the money.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import func, select

from models.db import CostEvent
from observability import metrics as observability_metrics
from tenancy import is_registered_tenant_id, redact_tenant_id

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.stdlib.get_logger(__name__)

#: How far back to look. The ledger itself only reaches 2026-09-05 (the TRUNCATE),
#: so today this is "everything there is" — it is written as a bound rather than an
#: unbounded scan so the query's cost stays predictable as history accumulates.
#:
#: ⚠️ NOT a decaying window by accident: 30 days is long enough that a tenant which
#: stopped emitting days ago STILL shows up, which is the whole point. The three
#: ids measured on 2026-09-12 had been silent for ~3 days and a 24h window would
#: have reported a comfortable $0.00.
LOOKBACK_DAYS = 30

#: Cap on how many distinct offending ids are named in the log line.
MAX_LOGGED_IDS = 20

#: Hard ceiling on how many tenant ids get their OWN Prometheus series.
#:
#: ⛔ WHY A CAP EXISTS AT ALL: ``tenant_id`` is caller-supplied. Nothing on the
#: write path requires it to be a tenant that exists — that is the entire defect
#: this module reports — so a broken or hostile emitter can persist a row under a
#: freshly minted uuid on every request, and each distinct value would otherwise
#: become a permanent time series in the scrape target. That is a cardinality
#: incident in Prometheus, not in this process, so nothing here would look
#: unhealthy while it happened. Same reasoning, same shape, as
#: :data:`services.emitter_freshness.MAX_FRESHNESS_SERIES`.
#:
#: ⭐ ORDERED BY SPEND DESCENDING, WHICH IS WHAT MAKES THE CAP SAFE. The cap in
#: ``emitter_freshness`` orders by recency and therefore has a documented
#: weakness: freshly minted label values sort to the TOP and evict the genuinely
#: interesting rows. Ordering by money inverts that — a flood of newly minted
#: uuids is by construction low-spend and sorts to the BOTTOM, so the expensive
#: ghosts an operator actually needs named keep their series while the flood
#: lands in ``__overflow__``. The cap degrades in the right direction.
#:
#: WHY 20: it is :data:`MAX_LOGGED_IDS`, deliberately reused rather than picked
#: independently, so the metric and the log name the SAME set and cannot drift
#: into disagreeing about who the offenders are. The measured population is 4
#: (2026-09-13), so this is 5x headroom.
MAX_NAMED_TENANT_SERIES = MAX_LOGGED_IDS

#: Label value carrying the money and events of every id past the cap.
#:
#: ⛔ NOT a uuid, and it must never collide with one: a real ``tenant_id`` here has
#: come out of a Postgres ``uuid`` column, so no genuine value can contain ``_``.
#: Without this bucket the labelled view would silently under-report the total the
#: moment the cap engaged, and the gap between it and the unlabelled total would
#: look like a bug in one of the two. With it, the labelled series always sum to
#: the unlabelled total.
OVERFLOW_TENANT_LABEL = "__overflow__"


#: ``excluded_from_billing`` is NOT filtered out, deliberately, and this is the one
#: judgement call in the module. The flusher parks unregistered rows on that flag
#: precisely BECAUSE they are unregistered, so filtering ``= false`` would remove
#: most of the population this gauge exists to size and the reading would trend to
#: zero as the parking mechanism worked. The money is reported as what it IS:
#: spend recorded against an id nobody can bill, whether or not it was already
#: excluded from a billable total.
def _spend_by_tenant_sql(since: datetime):
    """One aggregate per tenant id since ``since``.

    ⛔ Bounded on ``created_at``, NOT ``event_ts`` and NOT
    ``coalesce(event_ts, created_at)``. ``event_ts`` is producer-asserted, NULL on
    94.8% of rows, and may legitimately be back-dated up to 90 days or
    forward-dated by a broken clock — any of which would move a row in or out of
    this window for reasons that have nothing to do with when the spend landed.
    "Did unbillable spend arrive in the last 30 days" is an ingest-time question.
    The same reasoning is spelled out at length in
    :mod:`services.emitter_freshness`.
    """
    return (
        select(
            CostEvent.tenant_id,
            func.count().label("events"),
            func.coalesce(func.sum(CostEvent.cost_usd), 0).label("spend"),
            func.max(CostEvent.created_at).label("last_seen"),
        )
        .where(CostEvent.created_at >= since)
        .group_by(CostEvent.tenant_id)
    )


def _publish_named_series(offenders: list[tuple[str, int, Decimal]]) -> int:
    """Publish the per-tenant breakdown. Returns the number of ids NAMED.

    ``offenders`` must already be sorted by spend descending — the cap keeps a
    prefix of it, and that ordering is what makes the cap degrade safely (see
    :data:`MAX_NAMED_TENANT_SERIES`).

    ⛔ IT CLEARS BOTH GAUGES FIRST, AND THAT IS NOT OPTIONAL — it is the one way
    this differs from :mod:`services.emitter_freshness`, which deliberately does
    NOT clear. There, a series is a last-seen timestamp and SHOULD persist and
    age after the emitter goes quiet. Here a series asserts "this tenant is
    unregistered RIGHT NOW". A labelled Gauge never forgets a child, so without
    the clear, a tenant that gets registered — the exact outcome this metric is
    supposed to drive — would keep exporting its last non-zero spend forever, and
    the alert it raised could never resolve. The fix would look like the defect.
    Clearing makes the series set self-healing: it is re-derived in full on every
    leader tick, so a resolved tenant simply stops existing.

    ⛔ IT NEVER RAISES. A measurement may not abort the rollups it rides with —
    same contract as :func:`services.emitter_freshness.publish_emitter_freshness`.
    """
    try:
        spend_gauge = observability_metrics.unregistered_spend_by_tenant_usd
        events_gauge = observability_metrics.unregistered_events_by_tenant

        # Re-derived from scratch every pass; see the docstring above.
        spend_gauge.clear()
        events_gauge.clear()

        named = offenders[:MAX_NAMED_TENANT_SERIES]
        for tenant_id, event_count, spend in named:
            spend_gauge.labels(tenant_id=tenant_id).set(float(spend))
            events_gauge.labels(tenant_id=tenant_id).set(event_count)

        # The residual, so the labelled series still sum to the unlabelled total.
        # Published ONLY when it is non-empty: an always-present zero-valued
        # __overflow__ series would be indistinguishable from a real overflow that
        # happened to cost nothing, and would put a permanent non-uuid label value
        # in front of every operator who reads this metric.
        dropped = offenders[MAX_NAMED_TENANT_SERIES:]
        if dropped:
            spend_gauge.labels(tenant_id=OVERFLOW_TENANT_LABEL).set(
                float(sum((s for _, _, s in dropped), Decimal("0")))
            )
            events_gauge.labels(tenant_id=OVERFLOW_TENANT_LABEL).set(sum(n for _, n, _ in dropped))
            observability_metrics.unregistered_tenant_series_dropped_total.inc(len(dropped))
            logger.warning(
                "unregistered_tenant_series_capped",
                distinct_tenants=len(offenders),
                named=len(named),
                dropped=len(dropped),
                cap=MAX_NAMED_TENANT_SERIES,
                note=(
                    "more distinct unregistered tenant ids exist than this metric "
                    "will name. The measured population is 4; a larger number "
                    "means an emitter is minting tenant uuids. Fix the emitter — "
                    "do NOT raise MAX_NAMED_TENANT_SERIES to make this quiet."
                ),
            )
        return len(named)
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning(
            "unregistered_tenant_named_series_not_published",
            exc_info=True,
        )
        return 0


async def publish_unregistered_tenant_spend(session: AsyncSession) -> int:
    """Publish the size of the unregistered-tenant population from the ledger.

    Sets ``billing_unregistered_tenant_spend_usd`` and
    ``billing_unregistered_tenant_count`` (the totals) plus
    ``billing_unregistered_spend_by_tenant_usd{tenant_id}`` and
    ``billing_unregistered_events_by_tenant{tenant_id}`` (the same population,
    NAMED and capped — see :func:`_publish_named_series`), and returns the number
    of offending tenant ids found, so the caller can log it.

    ⚠️ The return value is the number FOUND, not the number named. Those differ
    once the cap engages, and the totals stay authoritative in both cases.

    ⛔ THE CALLER MUST BE LEADER-GATED; this function cannot check that itself.
    The guard lives in :func:`services.aggregator.run_rollup_loop`, exactly as it
    does for :func:`services.emitter_freshness.publish_emitter_freshness`.

    ⭐ PUBLISHES AN EXPLICIT ZERO when nothing is found, and that matters: the
    whole defect being fixed is a metric that cannot distinguish "clean" from
    "not observed". A leader that scanned and found nothing must say so.
    """
    since = datetime.now(UTC) - timedelta(days=LOOKBACK_DAYS)
    rows = (await session.execute(_spend_by_tenant_sql(since))).all()

    offenders: list[tuple[str, int, Decimal]] = []
    total = Decimal("0")
    events = 0
    for row in rows:
        canon = str(row.tenant_id)
        if is_registered_tenant_id(canon):
            continue
        spend = Decimal(str(row.spend or 0))
        offenders.append((canon, int(row.events), spend))
        total += spend
        events += int(row.events)

    observability_metrics.unregistered_tenant_spend_usd.set(float(total))
    observability_metrics.unregistered_tenant_count.set(len(offenders))

    offenders.sort(key=lambda t: t[2], reverse=True)
    _publish_named_series(offenders)

    if offenders:
        logger.warning(
            "unregistered_tenant_spend",
            tenant_count=len(offenders),
            event_count=events,
            spend_usd=str(total),
            lookback_days=LOOKBACK_DAYS,
            tenants=[
                {"tenant_id": redact_tenant_id(t), "events": n, "spend_usd": str(s)}
                for t, n, s in offenders[:MAX_LOGGED_IDS]
            ],
            note=(
                "spend recorded against tenant ids the licences registry does not "
                "vouch for; these cannot be invoiced to anyone. Fix the EMITTER or "
                "register the tenant — do NOT start rejecting at the resolution "
                "boundary, which would drop the money record as well as the bill"
            ),
        )
    else:
        logger.debug("unregistered_tenant_spend_clean", lookback_days=LOOKBACK_DAYS)
    return len(offenders)
