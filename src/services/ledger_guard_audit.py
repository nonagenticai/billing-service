"""Is the ledger's anti-truncation guard still attached, and has it ever yielded?

THE INCIDENT THIS WATCHES FOR ALREADY HAPPENED. 2026-09-05 07:01:56.876899Z, prod
billing primary: ``billing_cost_events`` and all 12 partitions,
``billing_metering_events`` and every rollup table were TRUNCATEd in ONE
statement, ``RESTART IDENTITY``, by ``billing_user`` from the shared node
egress address. The shape is ``Base.metadata.sorted_tables`` — this repo's own
``tests/conftest.py``
per-test isolation step, pointed at the production DSN. ``billing_delete_audit``
was itself eleventh in that table list, so the audit table was emptied by the
same statement its trigger was writing into; rows 1-14 survive only because an
AFTER TRUNCATE trigger runs after the truncation completes.

⚠️ THE CONTROL EXISTS AND IS ARMED. THE GAP IS THAT NOTHING WATCHES IT.
Migrations 0017/0018/0019/0023 installed ``public.billing_forbid_truncate()``
plus BEFORE TRUNCATE triggers, and ``tests/conftest.py:81`` refuses to arm the
GUC hatch against a production database. Both halves are live (verified on prod
2026-09-10: ``billing_forbid_truncate`` present, ``billing_truncate_guard_trg``
on every ledger relation and partition, alembic head ``0026_quota_config_status``).
But a sweep of every Prometheus rule file found **zero** references to the guard,
the trigger or ``billing_delete_audit``. So:

* if someone drops the trigger, nothing says so, and the next wrong DSN empties
  the ledger exactly as before;
* if a truncate is ALLOWED (the GUC armed deliberately, or by a future caller
  that arms it for its own reasons), the audit row is written and read by nobody.
  The 2026-09-05 rows were found **five days later**, by going to look.

An armed control with no detector degrades to a hope the moment anyone edits the
schema. These two gauges are the detector.

⛔ WHY NOT ``REVOKE TRUNCATE`` — it has been tried and it is INERT, not merely
weak. ``billing_user`` OWNS all 14 relations (``pg_tables.tableowner``), and
TRUNCATE rights flow from ownership, so a revoke from the owner executes and
changes nothing. Migration 0017 records this; do not reach for it again.

⭐ A COUNT OF TRIGGERS IS NOT A TEST OF THE FUNCTION, and this deliberately does
not try to be. Proving the guard REFUSES means attempting a TRUNCATE, which on
prod is the incident. So this measures attachment (cheap, safe, and the thing
that actually rots when a migration is rewritten); the refusal itself is proven
in ``tests/test_truncate_guard.py`` against a test database, where a failure
costs nothing.
"""

from __future__ import annotations

from datetime import datetime

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from observability import metrics as observability_metrics

logger = structlog.get_logger(__name__)

#: Relations whose BEFORE TRUNCATE guard is counted. Partitions are included by
#: the LIKE, not enumerated: ``create_cost_events_partition_for`` attaches the
#: guard to every new partition it creates, so a hard-coded list would silently
#: stop covering the newest month — the exact window where all live rows are.
_GUARD_COUNT_SQL = text(
    """
    SELECT count(*)
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    WHERE t.tgname = 'billing_truncate_guard_trg'
      AND NOT t.tgisinternal
    """
)

#: ⚠️ Counts rows BY OPERATION rather than counting TRUNCATEs alone. A
#: deployment where the audit trigger itself was dropped reads zero TRUNCATE
#: rows — indistinguishable from a healthy ledger — so the DELETE rows are
#: carried as the liveness evidence that the audit table is still being written
#: to at all.
_AUDIT_ROWS_SQL = text(
    """
    SELECT operation, count(*)
    FROM billing_delete_audit
    GROUP BY operation
    """
)


#: TRUNCATE rows newer than the acknowledged high-water mark. ``:ack`` NULL means
#: nothing is acknowledged, so every TRUNCATE row counts.
_NEW_TRUNCATES_SQL = text(
    """
    SELECT count(*), max(ts)
    FROM billing_delete_audit
    WHERE operation = 'TRUNCATE'
      AND (CAST(:ack AS timestamptz) IS NULL OR ts > CAST(:ack AS timestamptz))
    """
)

_UNSET: object = object()


async def run_ledger_guard_audit(
    session_factory: async_sessionmaker[AsyncSession],
    acknowledged_through: datetime | None | object = _UNSET,
) -> tuple[int, dict[str, int]]:
    """Publish guard attachment and delete-audit depth. Returns both for tests.

    Read-only, leader-only, out of band from the cost flush. Exceptions are the
    caller's to swallow: a measurement must never abort the rollup it rides with.
    """
    if acknowledged_through is _UNSET:
        from settings import get_settings

        acknowledged_through = get_settings().ledger_truncate_acknowledged_through
    async with session_factory() as session:
        attached = int((await session.execute(_GUARD_COUNT_SQL)).scalar_one() or 0)
        rows = {str(op): int(n) for op, n in (await session.execute(_AUDIT_ROWS_SQL)).all() if op}
        new_truncates, newest_truncate = (
            await session.execute(_NEW_TRUNCATES_SQL, {"ack": acknowledged_through})
        ).one()

    observability_metrics.truncate_guard_relations.set(attached)
    # ⚠️ Every operation seen in THIS pass is published, and nothing else is
    # touched. A labelled Gauge keeps the last value it was given for a label it
    # no longer receives, which is correct here: ``billing_delete_audit`` is
    # append-only in normal operation, so a label that stops appearing means the
    # table was emptied — and pinning the previous count is exactly the reading
    # that makes that visible, where zeroing it would erase the evidence.
    for operation, count in rows.items():
        observability_metrics.delete_audit_rows.labels(operation=operation).set(count)

    # Warn on TRUNCATEs NEWER than the acknowledged high-water mark only. The
    # all-time count above stays published (an alert can still key on it), but a
    # WARNING that fires every pass forever for two investigated incidents is
    # noise that buries the next one. A new allowed TRUNCATE has ts > the mark
    # and warns on the very next pass.
    if new_truncates:
        # WARNING, not INFO: LOG_LEVEL is INFO in this estate.
        logger.warning(
            "ledger_truncate_audit_rows_present",
            truncate_rows=rows.get("TRUNCATE", 0),
            unacknowledged_truncate_rows=int(new_truncates),
            newest_truncate_at=newest_truncate.isoformat() if newest_truncate else None,
            acknowledged_through=(
                acknowledged_through.isoformat()
                if isinstance(acknowledged_through, datetime)
                else None
            ),
            guard_relations=attached,
            reason=(
                "billing_delete_audit records an ALLOWED TRUNCATE of the cost ledger "
                "newer than ledger_truncate_acknowledged_through; the guard refuses "
                "unless a session sets billing.allow_truncate, so someone did"
            ),
        )
    return attached, rows
