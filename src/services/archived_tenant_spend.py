"""Ledger-derived gauge: spend billed against a tenant flagged archived at flush time.

THE GAP THIS CLOSES. ``archived_at`` is enforced pre-spend in FOUR places in
this estate — licences' own licence-mint path (``licenses/main.py:3704``), its
token-mint path (``licenses/main.py:4775``), Envoy ext_authz's
``/license/introspect`` (``licenses/models.py:3603-3606``, GATEWAY-scoped only
and blind to east-west traffic), and this service's own pre-spend reservation
gate (``routers/budget.py``'s ``reserve_tenant_budget``). NONE of them sees the
cost events that ``inference-router`` and ``mlx-ingestion-backend`` push
directly onto ``billing:costs`` east-west, bypassing both the gateway and the
reservation gate. ``services.cost_flusher.flush_costs`` is the one place that
DOES see that traffic, and — by design — it never refuses it: the compute is
already spent by the time a cost event reaches the flusher, so rejecting the
row would destroy the only record of money already gone, which the estate has
already decided (``routers/budget.py``'s own comment) is worse than billing it.
Instead the flusher stamps ``billing_cost_events.billed_while_archived`` and
increments ``billing_cost_events_billed_while_archived_total`` (alembic 0037).

⛔ THAT COUNTER ALONE HAS THE SAME DEFECT ITS NEIGHBOUR HAD. Exactly like
:mod:`services.unregistered_tenant_spend` documents for
``billing_tenant_unregistered_total``: a per-pod Counter is born at 0 on every
restart, and 0-since-last-restart is indistinguishable from "never happened".
This module is that module's sibling for the archived-tenant population: ask
the LEDGER, which is durable, rather than a counter, which is not.

⛔ REPORTING ONLY. One aggregate read, no writes, no repair, and it must never
raise or reject — the flusher already made the billing decision; this only
measures what it decided. See :mod:`services.unregistered_tenant_spend` for
the identical judgement, restated here rather than re-derived.

⚠️ AGGREGATE WITH ``max()``, NEVER ``sum()``. Like its neighbours this runs
only on the LEADER, and the caller owns that gate — a follower publishes
NOTHING here, not a zero, so a naive ``sum()`` across replicas is a half-
reading and an ``avg()`` halves the money.

⚠️ UNLIKE ITS NEIGHBOUR, THIS DOES NOT RE-DERIVE ARCHIVED STATUS. The row
already carries the answer ``is_tenant_archived`` gave AT FLUSH TIME, stamped
durably on ``billed_while_archived``. Re-checking the live registry here would
answer a DIFFERENT question — "is this tenant archived RIGHT NOW" — which can
disagree with the historical fact (a tenant can be un-archived after the spend
that flagged it, or archived after a row that predates this column was
written). The gauge reports what the LEDGER recorded, not what the registry
says today.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import func, select

from models.db import CostEvent
from observability import metrics as observability_metrics
from tenancy import redact_tenant_id

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.stdlib.get_logger(__name__)

#: Mirrors services.unregistered_tenant_spend.LOOKBACK_DAYS — same reasoning:
#: long enough that a tenant archived and quiet for days still shows up, and a
#: bound rather than an unbounded scan so the query's cost stays predictable.
LOOKBACK_DAYS = 30

#: Cap on how many distinct offending ids are named in the log line.
MAX_LOGGED_IDS = 20

#: Hard ceiling on how many tenant ids get their OWN Prometheus series.
#: Mirrors services.unregistered_tenant_spend.MAX_NAMED_TENANT_SERIES.
#: ⚠️ This population is licences-bounded (a real, registered, archived
#: tenant) rather than caller-supplied, so it is materially smaller in
#: practice — but the cap stays anyway: nothing here should assume that bound
#: holds forever just because it holds today.
MAX_NAMED_TENANT_SERIES = MAX_LOGGED_IDS

#: Label value carrying the money and events of every id past the cap.
#: Mirrors services.unregistered_tenant_spend.OVERFLOW_TENANT_LABEL.
OVERFLOW_TENANT_LABEL = "__overflow__"


def _spend_by_archived_tenant_sql(since: datetime):
    """One aggregate per tenant id, over rows flagged ``billed_while_archived``.

    ⛔ Bounded on ``created_at``, NOT ``event_ts`` — same reasoning as
    :func:`services.unregistered_tenant_spend._spend_by_tenant_sql`: "did
    archived-tenant spend arrive in the last 30 days" is an ingest-time
    question, and ``event_ts`` is producer-asserted and frequently NULL.

    ``excluded_from_billing`` is NOT filtered out, for the same reason its
    neighbour does not filter it: this reports the money AS IT IS, whether or
    not some other exclusion (unregistered tenant, health probe) also parked
    the same row.
    """
    return (
        select(
            CostEvent.tenant_id,
            func.count().label("events"),
            func.coalesce(func.sum(CostEvent.cost_usd), 0).label("spend"),
        )
        .where(
            CostEvent.created_at >= since,
            CostEvent.billed_while_archived.is_(True),
        )
        .group_by(CostEvent.tenant_id)
    )


def _publish_named_series(offenders: list[tuple[str, int, Decimal]]) -> int:
    """Publish the per-tenant breakdown. Returns the number of ids NAMED.

    Mirrors :func:`services.unregistered_tenant_spend._publish_named_series`
    byte-for-byte in structure — same clear-then-rebuild-every-tick contract
    (a tenant un-archived, or simply quiet past the lookback window, must stop
    exporting its last non-zero reading rather than exporting it forever), and
    the same never-raise contract.
    """
    try:
        spend_gauge = observability_metrics.archived_spend_by_tenant_usd
        events_gauge = observability_metrics.archived_events_by_tenant

        spend_gauge.clear()
        events_gauge.clear()

        named = offenders[:MAX_NAMED_TENANT_SERIES]
        for tenant_id, event_count, spend in named:
            spend_gauge.labels(tenant_id=tenant_id).set(float(spend))
            events_gauge.labels(tenant_id=tenant_id).set(event_count)

        dropped = offenders[MAX_NAMED_TENANT_SERIES:]
        if dropped:
            spend_gauge.labels(tenant_id=OVERFLOW_TENANT_LABEL).set(
                float(sum((s for _, _, s in dropped), Decimal("0")))
            )
            events_gauge.labels(tenant_id=OVERFLOW_TENANT_LABEL).set(sum(n for _, n, _ in dropped))
            observability_metrics.archived_tenant_series_dropped_total.inc(len(dropped))
            logger.warning(
                "archived_tenant_series_capped",
                distinct_tenants=len(offenders),
                named=len(named),
                dropped=len(dropped),
                cap=MAX_NAMED_TENANT_SERIES,
            )
        return len(named)
    except Exception:  # pragma: no cover - observability must not break the loop
        logger.warning(
            "archived_tenant_named_series_not_published",
            exc_info=True,
        )
        return 0


async def publish_archived_tenant_spend(session: AsyncSession) -> int:
    """Publish the size of the archived-tenant-but-billed population from the ledger.

    Sets ``billing_archived_tenant_spend_usd`` and ``billing_archived_tenant_count``
    (the totals) plus ``billing_archived_spend_by_tenant_usd{tenant_id}`` and
    ``billing_archived_events_by_tenant{tenant_id}`` (the same population,
    NAMED and capped), and returns the number of tenant ids found.

    ⛔ THE CALLER MUST BE LEADER-GATED; this function cannot check that itself.
    Mirrors :func:`services.unregistered_tenant_spend.publish_unregistered_tenant_spend`'s
    wiring in :func:`services.aggregator.run_rollup_loop`.

    ⭐ PUBLISHES AN EXPLICIT ZERO when nothing is found — same reasoning as its
    neighbour: a metric that cannot distinguish "clean" from "not observed" is
    the exact defect this whole ledger-derived pattern exists to fix.
    """
    since = datetime.now(UTC) - timedelta(days=LOOKBACK_DAYS)
    rows = (await session.execute(_spend_by_archived_tenant_sql(since))).all()

    offenders: list[tuple[str, int, Decimal]] = []
    total = Decimal("0")
    events = 0
    for row in rows:
        canon = str(row.tenant_id)
        spend = Decimal(str(row.spend or 0))
        offenders.append((canon, int(row.events), spend))
        total += spend
        events += int(row.events)

    observability_metrics.archived_tenant_spend_usd.set(float(total))
    observability_metrics.archived_tenant_count.set(len(offenders))

    offenders.sort(key=lambda t: t[2], reverse=True)
    _publish_named_series(offenders)

    if offenders:
        logger.warning(
            "archived_tenant_spend",
            tenant_count=len(offenders),
            event_count=events,
            spend_usd=str(total),
            lookback_days=LOOKBACK_DAYS,
            tenants=[
                {"tenant_id": redact_tenant_id(t), "events": n, "spend_usd": str(s)}
                for t, n, s in offenders[:MAX_LOGGED_IDS]
            ],
            note=(
                "spend billed against a tenant flagged archived at flush time — "
                "real money for compute already spent, not a data-quality gap. "
                "See the four PRE-SPEND archival gates (licences licence- and "
                "token-mint, routers/budget.py's reservation gate) to close this "
                "upstream of the compute rather than here."
            ),
        )
    else:
        logger.debug("archived_tenant_spend_clean", lookback_days=LOOKBACK_DAYS)
    return len(offenders)
