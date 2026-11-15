"""billing_metering_events.retarget_tenant_id — persist license-authz's retarget.

license-authz#65 (merged, live) adds an OPTIONAL ``retarget_tenant_id`` to each
``billing:events`` message: a uuid string, present only when the request was
retargeted, never ``""``. billing-service dropped it on the floor
(billing-service#235). This adds the column it lands in; the writer is
``services.event_flusher._parse_retarget_tenant_id``.

Additive only: one nullable ``uuid`` column, typed like ``tenant_id`` (0011).
Catalogue-only in Postgres (no default, no rewrite, no row touched).

⛔ NO BACKFILL: the value was never stored for earlier rows and exists nowhere.
⛔ NO INDEX: raw events are read by time, never grouped by this column — the
same judgement as ``project_id`` (0036).

⚠️ COMPANION EDIT — ``src/db.py``: the ADD COLUMN is mirrored byte-identically
into ``db.ADDITIVE_MIGRATIONS`` and replayed on pod start, because the flusher
NAMES this column in every INSERT; a pod on this code against a pre-0042
database would otherwise fail every metering flush.

Revision ID: 0042_metering_retarget_tenant
Revises: 0041_attribution_rules
Create Date: 2026-09-30
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0042_metering_retarget_tenant"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0041_attribution_rules"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ADD_COLUMN = "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS retarget_tenant_id UUID"
_DROP_COLUMN = "ALTER TABLE billing_metering_events DROP COLUMN IF EXISTS retarget_tenant_id"


def upgrade() -> None:
    op.execute(_ADD_COLUMN)


def downgrade() -> None:
    # ⚠️ Reversible as schema, lossy as data: every retarget recorded since the
    # upgrade is destroyed and has no other copy (the Redis stream is trimmed).
    op.execute(_DROP_COLUMN)
