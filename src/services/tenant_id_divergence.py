"""R15 estate repair — measure and record spend booked to an unregistered tenant_id.

⛔ THE BACKFILL WAS REFUSED. The original ask was "backfill the 1,536 divergent
``tenant_id`` rows to canonical". Measured 2026-09-16, across 37 distinct
divergent ids and every plausible derivation: 1 of 37 exists anywhere in the
licences registry (with its OWN ``tenant_id`` NULL there too), 0 match any
``subject.keycloak_sub``, and 0 of 2,190 (uuid namespace × candidate label)
combinations reproduce any of the 19 v5-shaped ids as ``uuid5()`` of a
registered tenant's name. There is no canonical value to backfill TO — writing
one would MANUFACTURE attribution, assigning real dollars to a tenant chosen
by us rather than by evidence. This module measures and records instead; it
never writes ``tenant_id``.

Two capabilities, both read-only against the tables they measure:

1. :func:`run_tenant_id_divergence_audit` — the LIVE gauge. Runs on the
   rollup leader's tick (see ``services.aggregator``, beside
   ``run_attribution_audit``), scoped to ``billing_cost_events`` over a
   rolling window for the same reason ``attribution_audit._UNREGISTERED_TENANT_
   SQL`` is scoped there: it is the one table this classification's DOLLAR
   figure is meaningful for and the one already read on every tick, so this
   adds no new table scan shape to the hot rollup path. Publishes
   ``billing_tenant_id_divergence_rows`` / ``..._spend_usd`` UNCONDITIONALLY,
   by classification — a measured zero is written, never withheld, exactly as
   ``attribution_audit``'s project-axis gauges are (see that module's own
   comment for why: a series absent on a healthy estate and absent on a broken
   one is the failure this discipline exists to end).

2. :func:`record_tenant_id_divergence_ledger` — the STANDING RECORD. Writes
   append-only rows into ``tenant_id_divergence_audit`` (alembic 0039) naming
   exactly which row, in which table, carried which unregistered tenant_id and
   what it classified as. Deliberately NOT wired into the leader tick: it
   scans whichever tables it is given in full (no time-window), and the tables
   in :data:`DEFAULT_LEDGER_TABLES` include a config-shaped one
   (``token_budgets``) that has no natural recency column to bound a scan by
   -- running that shape on every rollup tick is a cost this estate does not
   need to pay for a population that is, by its own nature, estate DEBT rather
   than a fast-moving stream. It is exposed as a plain importable function
   (and ``scripts/record_tenant_id_divergence.py`` as a thin CLI) for an
   operator to run as a deliberate, observable, idempotent step -- idempotent
   via the ledger's own ``(table_name, row_pk)`` unique constraint, so running
   it twice records nothing twice.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if TYPE_CHECKING:
    from sqlalchemy.engine import CursorResult

from observability import metrics as observability_metrics
from tenancy import TENANT_ID_DIVERGENCE_CLASSES, classify_tenant_id

logger = structlog.get_logger(__name__)

#: Same grain as attribution_audit.DEFAULT_WINDOW_HOURS, deliberately: this
#: rides the same rollup tick and there is no reason for the two windows to
#: disagree about what "recent" means on this estate.
DEFAULT_WINDOW_HOURS = 24

_DIVERGENCE_WINDOW_SQL = text(
    """
    SELECT tenant_id::text AS tenant_id,
           count(*)        AS rows,
           coalesce(sum(cost_usd), 0)::float8 AS spend
    FROM billing_cost_events
    WHERE created_at >= :floor
      AND tenant_id IS NOT NULL
    GROUP BY 1
    """
)


@dataclass
class DivergenceSnapshot:
    """Pure aggregation result of :func:`_aggregate_divergence`. No I/O."""

    rows_by_class: dict[str, int] = field(
        default_factory=lambda: dict.fromkeys(TENANT_ID_DIVERGENCE_CLASSES, 0)
    )
    spend_by_class: dict[str, float] = field(
        default_factory=lambda: dict.fromkeys(TENANT_ID_DIVERGENCE_CLASSES, 0.0)
    )
    #: Distinct tenant ids seen at all (registered + divergent), for a caller
    #: that wants a denominator. Not published on any gauge today.
    tenants_seen: int = 0


def _aggregate_divergence(
    per_tenant: list[tuple[str, int, float]],
) -> DivergenceSnapshot:
    """Classify each ``(tenant_id, rows, spend)`` tuple and sum by class.

    Pure — no DB, no metrics, no settings. Split out from the async runner so
    the aggregation itself is testable without a database, the same split
    ``attribution_audit.classify_attribution`` makes from ``run_attribution_
    audit``. Registered tenants are counted toward ``tenants_seen`` and then
    dropped: this is a DIVERGENCE snapshot, and a "registered" bucket in a
    gauge named divergence would be a contradiction, not a measurement.
    """
    snapshot = DivergenceSnapshot()
    for tenant_id, rows, spend in per_tenant:
        snapshot.tenants_seen += 1
        classification = classify_tenant_id(tenant_id)
        if classification == "registered":
            continue
        snapshot.rows_by_class[classification] = snapshot.rows_by_class.get(
            classification, 0
        ) + int(rows)
        snapshot.spend_by_class[classification] = snapshot.spend_by_class.get(
            classification, 0.0
        ) + float(spend or 0.0)
    return snapshot


async def run_tenant_id_divergence_audit(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> DivergenceSnapshot:
    """Measure current divergent-tenant spend and publish the gauges.

    Read-only. Intended to run on the rollup leader's tick, out of band from
    the cost flush — see the call site in ``services.aggregator`` beside
    ``run_attribution_audit``. Exceptions are the CALLER's to swallow (same
    contract as ``run_attribution_audit``): a measurement must never be able
    to abort the rollup it rides along with, so this function does not catch
    its own DB errors — the caller's ``try/except`` around this call is where
    that happens, exactly as it already is around ``run_attribution_audit``.
    """
    now = datetime.now(UTC)
    floor = now - timedelta(hours=window_hours)
    async with session_factory() as session:
        per_tenant = [
            (str(r.tenant_id), int(r.rows), float(r.spend or 0.0))
            for r in (await session.execute(_DIVERGENCE_WINDOW_SQL, {"floor": floor})).all()
        ]
    snapshot = _aggregate_divergence(per_tenant)

    # Set UNCONDITIONALLY, including 0 for a classification with nothing this
    # window — a measured zero, not a withheld one. See the module docstring
    # and the two gauges' own help text in observability.metrics.
    for classification in TENANT_ID_DIVERGENCE_CLASSES:
        observability_metrics.tenant_id_divergence_rows.labels(classification=classification).set(
            snapshot.rows_by_class.get(classification, 0)
        )
        observability_metrics.tenant_id_divergence_spend_usd.labels(
            classification=classification
        ).set(snapshot.spend_by_class.get(classification, 0.0))
    # LAST, and only here — a partial pass (an exception before this line)
    # leaves the timestamp stale rather than certifying half-written gauges.
    observability_metrics.tenant_id_divergence_audit_last_success_timestamp_seconds.set(time.time())
    return snapshot


#: (table_name, primary-key column name) for every table the 2026-09-16 R15
#: measurement found divergent rows in, MINUS the partition child
#: (``billing_cost_events_2026_09`` etc.) — scanning the partitioned PARENT
#: already reaches every child row, and scanning both would double-record
#: through the ledger's own (table_name, row_pk) uniqueness under two
#: different table_name values for the same physical row.
DEFAULT_LEDGER_TABLES: tuple[tuple[str, str], ...] = (
    ("audit_log", "id"),
    ("billing_cost_daily", "id"),
    ("billing_cost_events", "id"),
    ("token_budgets", "id"),
    ("billing_metering_events", "id"),
    ("billing_metering_hourly", "id"),
    ("token_usage", "id"),
    ("billing_metering_daily", "id"),
)


async def record_tenant_id_divergence_ledger(
    session: AsyncSession,
    *,
    tables: tuple[tuple[str, str], ...] = DEFAULT_LEDGER_TABLES,
    observed_at: datetime | None = None,
) -> dict[str, int]:
    """Scan ``tables`` and INSERT one ledger row per divergent row found.

    ⛔ READ-ONLY against every table named in ``tables``, and INSERT-ONLY
    against the ledger — no ``UPDATE``, no touching ``tenant_id`` anywhere,
    ever. ``ON CONFLICT (table_name, row_pk) DO NOTHING`` makes re-running
    this idempotent: a second pass over an unchanged table records nothing
    new, and a table that has grown records only the rows that are new to it.

    Table/PK identifiers come from :data:`DEFAULT_LEDGER_TABLES`, a fixed
    tuple of names this module owns — never from caller input — so this
    can never become a SQL-injection-shaped way to read an arbitrary table.

    Returns the number of NEW ledger rows written, by table name, so a caller
    (an ops script, a test) can assert on it without a second query.
    """
    when = observed_at or datetime.now(UTC)
    inserted: dict[str, int] = {}
    for table_name, pk_column in tables:
        rows = (
            await session.execute(
                text(
                    f"""
                    SELECT {pk_column}::text AS row_pk, tenant_id::text AS tenant_id
                    FROM {table_name}
                    WHERE tenant_id IS NOT NULL
                    """  # noqa: S608 - table_name/pk_column are from the fixed
                    # DEFAULT_LEDGER_TABLES tuple this module owns, never from
                    # caller input.
                )
            )
        ).all()
        table_inserted = 0
        insert_stmt = text(
            """
            INSERT INTO tenant_id_divergence_audit
                (table_name, row_pk, tenant_id, class_detected, observed_at)
            VALUES
                (:table_name, :row_pk, :tenant_id, :class_detected, :observed_at)
            ON CONFLICT (table_name, row_pk) DO NOTHING
            """
        )
        for r in rows:
            classification = classify_tenant_id(r.tenant_id)
            if classification == "registered":
                continue
            # One execute per row rather than an executemany batch: rowcount
            # semantics for a multi-row executemany + ON CONFLICT DO NOTHING
            # vary by driver, and this loop is at most a few thousand rows —
            # a one-off ops pass, not a hot path.
            # CursorResult.rowcount is concrete on INSERT; the abstract Result
            # return type loses this attribute, so cast to the concrete type
            # — same idiom as routers/quota.py and services/audit.py.
            result = cast(
                "CursorResult[Any]",
                await session.execute(
                    insert_stmt,
                    {
                        "table_name": table_name,
                        "row_pk": r.row_pk,
                        "tenant_id": r.tenant_id,
                        "class_detected": classification,
                        "observed_at": when,
                    },
                ),
            )
            if result.rowcount:
                table_inserted += 1
        inserted[table_name] = table_inserted
    await session.commit()
    logger.info("tenant_id_divergence_ledger_recorded", inserted=inserted)
    return inserted
