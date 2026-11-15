"""app_registry — mirror of the licences APP registry (app_key -> app_id).

``X-App-Id`` carries the caller's app KEY. Migration 0049 resolved it through
``request_quota_config.app_key``, which only holds apps that own a LICENCE, so
registration-only platform apps (``core-app``, ``chat-bridge``,
``chat-bridge-rmt``, ``chat-bridge-merqi``) never resolved. This table mirrors
every registered app from ``GET {licenses_url}/internal/apps`` and is what
``services.license_resolver.get_app_key_map`` reads.

New empty table; filled by the next ``POST /api/budget/sync-from-licenses``.
Until then the key map falls back to the 0049 column (counted on
``billing_app_key_map_fallback_total``).

DRIFT-TOLERANT: ``IF NOT EXISTS`` because ``src/db.py`` replays the same
statement on every pod start (``ADDITIVE_MIGRATIONS``) and ``init_db`` runs
``create_all``, so the table may already exist when this revision runs.

TRUNCATE GUARD: born guarded. 0018/0043 sweep the catalog once; a table created
after them is born unguarded unless its own migration attaches
``billing_truncate_guard_trg`` (what 0045 does for its table). Attached
IF-ABSENT, so a re-run is a no-op.

Revision ID: 0050_app_registry
Revises: 0049_quota_config_app_key
Create Date: 2026-10-07
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0050_app_registry"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0049_quota_config_app_key"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


ATTACH_TRUNCATE_GUARD_IF_ABSENT = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NULL THEN
        RAISE EXCEPTION 'alembic 0050 requires public.billing_forbid_truncate() from 0017';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid = 'public.app_registry'::regclass
           AND tgname = 'billing_truncate_guard_trg'
    ) THEN
        CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.app_registry
            FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate();
    END IF;
END
$do$;
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '3s'")
    op.execute(
        "CREATE TABLE IF NOT EXISTS app_registry ("
        "app_id uuid PRIMARY KEY, "
        "app_key text NOT NULL, "
        "revoked boolean NOT NULL DEFAULT false, "
        "synced_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
        "CONSTRAINT app_registry_app_key_key UNIQUE (app_key))"
    )
    op.execute(ATTACH_TRUNCATE_GUARD_IF_ABSENT)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app_registry")
