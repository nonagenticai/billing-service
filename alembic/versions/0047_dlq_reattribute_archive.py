"""Dead-letter exits: re-attribution receipts (#243) + archive-before-trim.

WHY
---
A cost event quarantined in ``billing:costs:dead`` had exactly two exits —
replay (re-fails validation for ``nil_tenant_id``) and discard (refuses to
delete money) — and Redis was its only copy, so the 720h age trim destroyed it.
Measured: 11 entries were lost that way between 2026-09-30 and 2026-10-02 while
``BillingCostDeadLetterNearExpiry`` was firing.

WHAT THIS CREATES
-----------------
* ``billing_dlq_reattributions`` — receipts for ``POST /api/admin/dlq/reattribute``
  (``services.dlq_ops.reattribute_dlq``), committed BEFORE the ``XDEL``.
  ``uq_dlq_reattributions_marker`` is UNIQUE on the per-EVENT replay marker.
* ``billing_dlq_archive`` — one row per dead-letter entry the age trim is about
  to remove. ``services.dlq_ops.archive_then_trim_dead_letter`` commits these
  rows BEFORE it issues the ``XTRIM``; if the commit fails it does not trim. So
  the AGE trim no longer loses entries. ⚠️ The quarantine XADD's own
  ``MAXLEN ~ 100000`` cap (``cost_flusher``) still drops the oldest entries
  unarchived if the stream ever reaches 100k; that path is not covered here.
  ``uq_dlq_archive_entry`` is UNIQUE on the stream id, so re-archiving on a
  later tick is a no-op.
* Both are guarded by 0043's ATTACH-IF-ABSENT truncate-guard sweep, so neither
  is born unguarded.

⚠️ COMPANION EDIT — ``src/db.py``. The CREATE statements are mirrored
BYTE-IDENTICALLY into ``db.ADDITIVE_MIGRATIONS`` and replayed on pod start; all
are idempotent. The guard sweep is NOT mirrored (it runs once, here, under a
bounded lock_timeout).

Revision ID: 0047_dlq_reattribute_archive
Revises: 0046_token_budget_audit
Create Date: 2026-10-04
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0047_dlq_reattribute_archive"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0046_token_budget_audit"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Mirrored byte-identically in src/db.py ADDITIVE_MIGRATIONS.
_CREATE_TABLE = (
    "CREATE TABLE IF NOT EXISTS billing_dlq_reattributions ("
    "id BIGSERIAL PRIMARY KEY, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "dead_entry_id VARCHAR(64) NOT NULL, "
    "original_id VARCHAR(64), "
    "replay_marker VARCHAR(128) NOT NULL, "
    "reattributed_from TEXT, "
    "tenant_id UUID NOT NULL, "
    "reattributed_by VARCHAR(255) NOT NULL, "
    "reason TEXT NOT NULL, "
    "original_fields JSONB NOT NULL, "
    "reinjected_stream_id VARCHAR(64) NOT NULL, "
    "request_id VARCHAR(64), "
    "CONSTRAINT uq_dlq_reattributions_marker UNIQUE (replay_marker))"
)
_CREATE_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_dlq_reattributions_tenant_created "
    "ON billing_dlq_reattributions (tenant_id, created_at)"
)
_CREATE_ARCHIVE_TABLE = (
    "CREATE TABLE IF NOT EXISTS billing_dlq_archive ("
    "id BIGSERIAL PRIMARY KEY, "
    "archived_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "stream VARCHAR(128) NOT NULL, "
    "dead_entry_id VARCHAR(64) NOT NULL, "
    "dlq_reason VARCHAR(128), "
    "dlq_at VARCHAR(64), "
    "fields JSONB NOT NULL, "
    "CONSTRAINT uq_dlq_archive_entry UNIQUE (stream, dead_entry_id))"
)
_CREATE_ARCHIVE_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_dlq_archive_archived_at ON billing_dlq_archive (archived_at)"
)

REQUIRE_GUARD_FUNCTION_DDL = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NULL THEN
        RAISE EXCEPTION
            'alembic 0047 requires public.billing_forbid_truncate() from 0017'
            USING HINT = 'Run alembic 0017 first.';
    END IF;
END
$do$;
"""

#: 0043's catalog sweep (ATTACH-IF-ABSENT), the same predicate 0044/0045 reuse:
#: arms the tables born here and touches nothing already guarded.
ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL = r"""
DO $do$
DECLARE
    rel   text;
    armed integer := 0;
BEGIN
    FOR rel IN
        SELECT c.relname
          FROM pg_class c
          JOIN pg_namespace ns ON ns.oid = c.relnamespace
         WHERE ns.nspname = 'public'
           AND c.relkind IN ('r', 'p')
           AND c.relpersistence = 'p'
           AND c.relname <> 'alembic_version'
           AND NOT EXISTS (
                   SELECT 1
                     FROM pg_depend d
                    WHERE d.classid = to_regclass('pg_class')
                      AND d.objid = c.oid
                      AND d.deptype = 'e')
           AND NOT EXISTS (
                   SELECT 1
                     FROM pg_trigger t
                    WHERE t.tgrelid = c.oid
                      AND t.tgname = 'billing_truncate_guard_trg')
         ORDER BY c.relname
    LOOP
        EXECUTE format(
            'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate()',
            rel);
        armed := armed + 1;
        RAISE NOTICE 'billing_truncate_guard_trg -> public.%', rel;
    END LOOP;

    RAISE NOTICE 'alembic 0047: armed % newly-guarded relation(s)', armed;
END
$do$;
"""

LOCK_TIMEOUT = "3s"
STATEMENT_TIMEOUT = "60s"


def _bound_the_wait() -> None:
    """Same discipline as 0043-0046: SET LOCAL, then read back (never vacuous)."""
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0047 refuses to run without a bounded lock_timeout "
            f"(SET LOCAL reported {effective!r}: not inside a transaction block)."
        )


def upgrade() -> None:
    _bound_the_wait()
    op.execute(_CREATE_TABLE)
    op.execute(_CREATE_INDEX)
    op.execute(_CREATE_ARCHIVE_TABLE)
    op.execute(_CREATE_ARCHIVE_INDEX)
    op.execute(REQUIRE_GUARD_FUNCTION_DDL)
    op.execute(ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL)


def downgrade() -> None:
    # ⚠️ Reversible as schema, LOSSY as data: each receipt / archive row holds the
    # only copy of a dead-letter entry that was XDELed or XTRIMmed after it was
    # written. Re-attributed spend survives in billing_cost_events; the
    # provenance and the archived quarantine records do not.
    _bound_the_wait()
    op.execute("DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.billing_dlq_archive")
    op.execute(
        "DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.billing_dlq_reattributions"
    )
    op.execute("DROP INDEX IF EXISTS ix_dlq_archive_archived_at")
    op.execute("DROP TABLE IF EXISTS billing_dlq_archive")
    op.execute("DROP INDEX IF EXISTS ix_dlq_reattributions_tenant_created")
    op.execute("DROP TABLE IF EXISTS billing_dlq_reattributions")
