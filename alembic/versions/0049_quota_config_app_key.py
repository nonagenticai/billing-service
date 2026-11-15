"""request_quota_config.app_key — the app's registry KEY (X-App-Id slug).

Every inference-router caller is a registered app in licences and sends its app
KEY (``core-app``, ``chat-bridge-rmt``, ``workers``) in ``X-App-Id``. The cost
flusher resolves that key to the registry app uuid through this mirror column
(``services.license_resolver.get_app_key_map``). Filled by
``POST /api/budget/sync-from-licenses`` from ``InternalLicenseResponse.app.key``.

Nullable, no default, no backfill: catalogue-only change, no row rewritten.
Rows stay NULL until the next sync-from-licenses run.

DRIFT-TOLERANT: ``IF NOT EXISTS`` because ``src/db.py`` replays the same
statement on every pod start (``ADDITIVE_MIGRATIONS``), so this revision and a
booted pod must be able to run in either order.

Revision ID: 0049_quota_config_app_key
Revises: 0048_rekey_book_fulfillment
Create Date: 2026-10-07
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0049_quota_config_app_key"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0048_rekey_book_fulfillment"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE request_quota_config ADD COLUMN IF NOT EXISTS app_key text")


def downgrade() -> None:
    op.execute("ALTER TABLE request_quota_config DROP COLUMN IF EXISTS app_key")
