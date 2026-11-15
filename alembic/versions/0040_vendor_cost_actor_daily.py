"""Per-actor daily cost rollup + the provider-reported vendor cost on the daily rollup.

WHY
---
``billing_cost_events`` carries ``actor_id`` (WHO ACTED — the Keycloak sub) and ``vendor_cost_usd``
(the provider's own charge), but the daily rollup kept neither. Raw events are retained for days, so
per-user cost and the real vendor figure were lost at the next partition drop, and
``generate_billing`` could only ever bill the price-card estimate.

1. ``billing_cost_daily.total_vendor_cost_usd`` — NULLABLE, NO DEFAULT: a catalogue-only change,
   no rewrite, no backfill. NULL means "no event in the group reported one", which is the only
   true statement available for historical rows.
2. ``billing_cost_actor_daily`` — a SIBLING table, deliberately not a new member of
   ``uq_cost_daily``: changing that grain would move every invoice, overview and reprice reader.

⚠️ DEPLOY ORDER / COMPANION EDIT — ``src/db.py``. The rollup NAMES both objects, so a pod on this
code against a pre-0040 database would fail every daily rollup. Both statements are mirrored
BYTE-IDENTICALLY into ``db.ADDITIVE_MIGRATIONS`` and replayed on every pod start. Both are
idempotent (``IF NOT EXISTS``) and neither drops anything.

Revision ID: 0040_vendor_cost_actor_daily
Revises: 0039_tenant_id_divergence
Create Date: 2026-09-17
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0040_vendor_cost_actor_daily"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0039_tenant_id_divergence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Mirrored byte-identically in src/db.py ADDITIVE_MIGRATIONS.
_ADD_VENDOR_COLUMN = (
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS total_vendor_cost_usd NUMERIC(20, 8)"
)
_CREATE_ACTOR_DAILY = (
    "CREATE TABLE IF NOT EXISTS billing_cost_actor_daily ("
    "id UUID PRIMARY KEY, "
    "bucket_date DATE NOT NULL, "
    "tenant_id UUID NOT NULL, "
    "license_id UUID NOT NULL, "
    "subject_id UUID NOT NULL, "
    "actor_id UUID NOT NULL, "
    "service_key VARCHAR(64) NOT NULL, "
    "provider VARCHAR(64) NOT NULL, "
    "served_model VARCHAR(128) NOT NULL, "
    "byo BOOLEAN NOT NULL, "
    "owner_provided BOOLEAN NOT NULL, "
    "total_input_tokens BIGINT NOT NULL, "
    "total_output_tokens BIGINT NOT NULL, "
    "total_cost_usd NUMERIC(20, 6) NOT NULL, "
    "total_vendor_cost_usd NUMERIC(20, 8), "
    "request_count INTEGER NOT NULL, "
    "CONSTRAINT uq_cost_actor_daily UNIQUE (bucket_date, tenant_id, license_id, subject_id, "
    "actor_id, service_key, provider, served_model, byo, owner_provided))"
)
_CREATE_ACTOR_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_cost_actor_daily_tenant_actor_date "
    "ON billing_cost_actor_daily (tenant_id, actor_id, bucket_date)"
)


def upgrade() -> None:
    op.execute(_ADD_VENDOR_COLUMN)
    op.execute(_CREATE_ACTOR_DAILY)
    op.execute(_CREATE_ACTOR_INDEX)


def downgrade() -> None:
    # Lossy as data: the actor axis exists nowhere else once raw events age out.
    op.execute("DROP TABLE IF EXISTS billing_cost_actor_daily")
    op.execute("ALTER TABLE billing_cost_daily DROP COLUMN IF EXISTS total_vendor_cost_usd")
