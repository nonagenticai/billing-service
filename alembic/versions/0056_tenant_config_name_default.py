"""tenant_config.tenant_name DEFAULT '' — the column the ORM believed had one.

WHY
---
Prod, 2026-10-13: CommonPractice called
``PUT /api/config/tenants/{id}/commercial-terms`` for a tenant it had just
created and got a 503 —
``psycopg.errors.NotNullViolation: null value in column "tenant_name" of
relation "tenant_config"``.

``models.db.config.TenantConfig.tenant_name`` declares ``server_default=""``,
so SQLAlchemy OMITS the column from an INSERT that does not set it and expects
the database to fill it in. But 0001's baseline created the column
``NOT NULL`` with NO default, and no later revision added one. Every handler
that creates a ``tenant_config`` row without naming ``tenant_name`` therefore
inserted NULL: the commercial-terms PUT, ``PUT /api/config/tenants/{id}`` and
``PUT /api/config/markup``. The test suite never saw it because it builds its
schema with ``Base.metadata.create_all`` — i.e. from the MODEL, which has the
default — never from this chain.

The handlers now set ``tenant_name`` explicitly as well; this revision makes the
database agree with the model, so an omitted column is ``''`` rather than a 503.
``tests/test_migration_0056_tenant_config_name_default.py`` pins it against the
REAL chain and ratchets the remaining model-vs-chain default drifts.

Catalogue-only: ``SET DEFAULT`` rewrites no row and does not scan the table.

Revision ID: 0056_tenant_config_name_default
Revises: 0055_money_budget_topup
Create Date: 2026-10-13
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0056_tenant_config_name_default"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0055_money_budget_topup"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SET_DEFAULT = "ALTER TABLE tenant_config ALTER COLUMN tenant_name SET DEFAULT ''"
DROP_DEFAULT = "ALTER TABLE tenant_config ALTER COLUMN tenant_name DROP DEFAULT"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '3s'")
    op.execute(SET_DEFAULT)


def downgrade() -> None:
    op.execute(DROP_DEFAULT)
