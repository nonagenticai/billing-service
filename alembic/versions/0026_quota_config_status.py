"""Carry licence lifecycle status onto ``request_quota_config``.

THE DEFECT
----------
``request_quota_config`` is billing's local MIRROR of the licence registry and
is the ONLY table ``services.license_resolver`` reads to answer "which licence
does this cost event belong to?". Until this migration it carried
``license_id, tenant_id, quota_monthly, app_id, app_name, created_at,
updated_at`` and **no lifecycle column at all** — no status, no active flag, no
expiry.

``services.license_resolver`` states the consequence verbatim in its module
docstring (the "LIMITATION — no lifecycle filter is possible here" block): a
REVOKED or EXPIRED licence stays resolvable and its tenant keeps attracting
attributed invoice lines, because the only predicate the two map-builders can
apply is ``app_id IS NOT NULL``. The docstring also names the remedy this
migration implements: *"Closing the gap properly requires that reconcile to
carry a status ONTO this table."*

Measured on the licences registry 2026-09-08: **43 unrevoked, unexpired tokens
hang off ``status='revoked'`` licences** (control: 177 hang off active ones) —
35 of them on ``n8n-dev``/Licenses Module, plus ahtapol FR/KAG and four
singletons. Registry-wide the split is 164 active / 92 revoked / 2 expired of
258 rows.

WHY A DELETE WAS NOT ALREADY ENOUGH
-----------------------------------
``services.budget_crud.delete_absent_quota_configs`` does remove mirror rows
whose licence is no longer in the active set, so a revoked licence's row does
eventually vanish. Two reasons that is not the same as a status column:

1. ``POST /api/budget/sync-from-licenses`` is **operator-triggered only** (see
   its docstring — no scheduler, no CronJob, no startup hook). Between two
   manual invocations a revocation is simply not reflected, and the resolver
   keeps attributing.
2. Deleting is a LOSSY way to express "revoked". The row carries the
   enforcement limit, and ``quota_check`` falls back to a CALLER-SUPPLIED
   ``quota_monthly`` when no row exists — so the hard delete trades a stale
   attribution for a fail-open quota. A status column lets the mirror say
   "known, and not active" without discarding the limit.

VALUES
------
Constrained to the same four the registry itself allows
(``licenses/models.py::ck_license_status``): ``active``, ``revoked``,
``expired``, ``pending``. Mirroring the upstream vocabulary exactly is
deliberate — a translated/narrowed enum here would silently coerce a status
this service does not yet understand into one it does, and the resolver's
predicate is ``== 'active'`` precisely so that an UNRECOGNISED value fails
CLOSED (not resolvable) rather than open.

⚠️ The upstream ``status`` is the STORED column, not
``License.effective_status()`` — ``internal_list_licenses`` returns
``lic.status`` verbatim and never calls the deriving helper. So a licence whose
``expires_at`` has passed while its stored status is still ``active`` mirrors
here as ``active``. That residual gap is expiry-shaped, is NOT closed by this
migration, and closing it needs ``expires_at`` mirrored too — recorded here so
the next reader does not mistake this column for a complete lifecycle filter.

BACKFILL
--------
Every pre-existing row becomes ``'active'``. That is the status they were
written under: until now the sync fetched ``?status=active`` exclusively, so a
row could only have been created from a licence that was active at the time it
was mirrored. It is also the ONLY backfill that preserves current behaviour —
any other value would make the resolver's new predicate drop live licences and
silently stop attributing real spend on deploy. Rows whose licence has since
been revoked are corrected by the first ``sync-from-licenses`` run after this
ships, which now fetches every status rather than only active ones.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0026_quota_config_status"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0025_invoice_markup_source"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# DEFAULT 'active' is what makes this safe to run against a live table: the
# column is NOT NULL from the first statement, and every existing row takes the
# default in the same operation. No separate UPDATE pass, no window in which a
# concurrent writer can insert a NULL.
_ADD_COLUMN = (
    "ALTER TABLE request_quota_config "
    "ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'active'"
)

# Belt-and-braces for a table that somehow already had a nullable `status`
# (a hand-patched environment): make the backfill explicit rather than relying
# solely on the column default above.
_BACKFILL = "UPDATE request_quota_config SET status = 'active' WHERE status IS NULL"

# Mirrors licences' own ck_license_status. NOT VALID is deliberate: it enforces
# every future write immediately while skipping the full-table verification
# scan, and the backfill above already guarantees existing rows conform.
_CONSTRAINT = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ck_request_quota_status'
    ) THEN
        ALTER TABLE request_quota_config
            ADD CONSTRAINT ck_request_quota_status
            CHECK (status IN ('active', 'revoked', 'expired', 'pending'));
    END IF;
END
$$;
"""

# The resolver's two map-builders both filter `status = 'active' AND app_id IS
# NOT NULL`. A partial index on exactly that predicate keeps the map build a
# single index scan as the mirror accumulates revoked rows it no longer deletes.
_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_request_quota_config_active "
    "ON request_quota_config (tenant_id, app_name) "
    "WHERE status = 'active' AND app_id IS NOT NULL"
)


def upgrade() -> None:
    op.execute(_ADD_COLUMN)
    op.execute(_BACKFILL)
    op.execute(_CONSTRAINT)
    op.execute(_INDEX)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_request_quota_config_active")
    op.execute("ALTER TABLE request_quota_config DROP CONSTRAINT IF EXISTS ck_request_quota_status")
    op.execute("ALTER TABLE request_quota_config DROP COLUMN IF EXISTS status")
