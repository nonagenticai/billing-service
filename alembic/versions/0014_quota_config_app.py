"""Record WHICH APP each licence is for, so unattributed spend can be resolved.

WHAT IS BROKEN
--------------
79% of billable ``billing_cost_events`` rows carry ``license_id IS NULL``
(measured 2026-08-27, 24h, ``NOT excluded_from_billing``: 3,751 of 4,993). Such
spend is billed and capped -- it drains the tenant-level pool -- but it can
never reach a customer invoice line, because ``CostDaily.license_attributed_clause``
drops it from every licence-keyed aggregate.

The root cause is upstream of billing and is a TRANSPORT gap, not a billing bug:
Istio ext_authz runs only at the ingress gateway (verified live 2026-08-27 --
exactly three namespaces carry a CUSTOM AuthorizationPolicy: ``control-service``,
``marketing``, ``sales``, and NO emitter namespace does), so east-west calls
never receive license-authz's ``x-license-id`` header, and the callers that could
forward one by hand mostly do not.

WHY THIS COLUMN
---------------
billing already holds the licence -> owning-tenant map locally in
``request_quota_config``, refreshed from ``GET /internal/licenses`` by
``POST /api/budget/sync-from-licenses``. That map is enough to answer "who owns
licence X" but NOT "which licence of tenant T covers this spend", because every
tenant that matters owns several:

    platform-core   20 licences
    platform-cp-1    5
    platform-cp-3    4

so an "exactly one candidate per tenant" rule never fires, and anything looser
would be GUESSING AN INVOICE LINE. The upstream response already carries the
answer (``InternalLicenseResponse.app``); billing simply discarded it. Persisting
``app_id``/``app_name`` makes the lookup ``(tenant_id, app)`` -> licence
deterministic, which is the difference between a resolved attribution and a
fabricated one.

NULLABLE, AND WHY IT STAYS THAT WAY
-----------------------------------
Backfill is not possible from inside this database -- the app is only known to
the licences service -- so existing rows stay NULL until the next reconcile pass
refreshes them. A NULL app means "cannot resolve", which the resolver treats
exactly like "no match": leave ``license_id`` NULL and keep counting it. Making
the column NOT NULL would fail the reconcile for every licence whose app the
upstream omits, i.e. it would trade a measured invoicing gap for an outage.

The partial index is the resolver's read path; it excludes the un-resolvable
rows so the index stays small and a NULL app can never be selected.

Revision ID: 0014_quota_config_app
Revises: 0013_park_router_duplicates
"""

from alembic import op

revision = "0014_quota_config_app"
down_revision = "0013_park_router_duplicates"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # IF NOT EXISTS: src/db.py replays additive DDL on every pod start, so this
    # migration and a booted pod must be able to run in either order without one
    # of them aborting (the failure mode 0012 exists to close).
    op.execute("ALTER TABLE request_quota_config ADD COLUMN IF NOT EXISTS app_id uuid")
    op.execute("ALTER TABLE request_quota_config ADD COLUMN IF NOT EXISTS app_name text")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_quota_config_tenant_app
            ON request_quota_config (tenant_id, app_id)
            WHERE app_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_quota_config_tenant_app")
    op.execute("ALTER TABLE request_quota_config DROP COLUMN IF EXISTS app_name")
    op.execute("ALTER TABLE request_quota_config DROP COLUMN IF EXISTS app_id")
