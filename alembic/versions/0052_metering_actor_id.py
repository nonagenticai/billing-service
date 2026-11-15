"""billing_metering_events.actor_id — persist who acted on a metered request.

license-authz#73 (merged, live) adds an OPTIONAL ``actor_id`` to each
``billing:events`` message: the UUID Keycloak ``sub`` of who acted (the verified
JWT ``sub``, or the licence subject's ``subject_keycloak_sub``), present only
when it is a UUID, never ``""``. billing-service dropped it on the floor. This
adds the column it lands in; the writer is
``services.event_flusher._parse_actor_id``.

Additive only: one nullable ``uuid`` column, the same type as
``billing_cost_events.actor_id`` (0021). Catalogue-only in Postgres (no default,
no rewrite, no row touched).

⛔ NO BACKFILL: the value was never stored for earlier rows and exists nowhere.
⛔ NO INDEX: raw events are read by time, never grouped by this column — the
same judgement as ``retarget_tenant_id`` (0042).

⚠️ COMPANION EDIT — ``src/db.py``: the ADD COLUMN is mirrored byte-identically
into ``db.ADDITIVE_MIGRATIONS`` and replayed on pod start, because the flusher
NAMES this column in every INSERT.

Revision ID: 0052_metering_actor_id
Revises: 0051_rekey_portal_render_e2e
Create Date: 2026-10-07
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0052_metering_actor_id"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0051_rekey_portal_render_e2e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ADD_COLUMN = "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS actor_id UUID"
_DROP_COLUMN = "ALTER TABLE billing_metering_events DROP COLUMN IF EXISTS actor_id"


def upgrade() -> None:
    op.execute(_ADD_COLUMN)


def downgrade() -> None:
    op.execute(_DROP_COLUMN)
