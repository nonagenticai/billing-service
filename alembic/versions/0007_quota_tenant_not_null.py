"""Make ``request_quota_config.tenant_id`` NOT NULL (close the cross-tenant read gap).

``GET /api/quota/check`` scopes a customer caller (``X-Tenant-Id`` present) to its
own licenses by comparing the header against ``request_quota_config.tenant_id``. A
row whose ``tenant_id`` is NULL cannot be proven foreign, so it was served to ANY
caller — a cross-tenant read leak (the runtime half was closed in #63). The
structural loaded gun was any writer that could persist a NULL-tenant row
(``scripts/backfill_quota_config_from_redis.py`` upserted ``tenant_id=None``, since
a Redis ``quota:limit:*`` key carries no tenant). This migration removes the shape
permanently.

Live prod at authoring time had 99 rows, 0 with NULL tenant_id, so the NOT NULL
alter needs no real backfill. We still (a) best-effort recover any residual NULL
from an in-DB mapping and (b) fail LOUD — never blind-fill — if a NULL remains,
rather than silently inventing a tenant-of-record for a security-relevant column.

Revision ID: 0007_quota_tenant_not_null
Revises: 0006_request_quota_config
Create Date: 2026-07-29

(Revision id kept <=32 chars — ``alembic_version.version_num`` is varchar(32).)
"""

from __future__ import annotations

from alembic import op

revision = "0007_quota_tenant_not_null"
down_revision = "0006_request_quota_config"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # (a) Best-effort backfill of any residual NULL tenant_id from an in-DB
    # source of truth: the hourly rollup carries license_id + a NOT NULL
    # tenant_id. Only trust a license that maps to exactly ONE tenant (a
    # license is tenant-scoped, so >1 would signal corrupt data we must not
    # guess through). Excludes the system/S2S sentinel license.
    op.execute(
        """
        UPDATE request_quota_config q
        SET tenant_id = m.tenant_id
        FROM (
            SELECT license_id, min(tenant_id) AS tenant_id
            FROM billing_metering_hourly
            WHERE license_id <> '00000000-0000-0000-0000-000000000000'
            GROUP BY license_id
            HAVING count(DISTINCT tenant_id) = 1
        ) m
        WHERE q.license_id = m.license_id
          AND q.tenant_id IS NULL
        """
    )

    # (b) Fail LOUD if any NULL remains — do NOT invent a tenant. An operator
    # must resolve the residual rows (e.g. re-run POST /api/quota/config with
    # the correct tenant) before this migration can complete.
    op.execute(
        """
        DO $$
        DECLARE
            n bigint;
        BEGIN
            SELECT count(*) INTO n
            FROM request_quota_config
            WHERE tenant_id IS NULL;
            IF n > 0 THEN
                RAISE EXCEPTION
                    'request_quota_config has % row(s) with NULL tenant_id that '
                    'could not be recovered from billing_metering_hourly; resolve '
                    'them (POST /api/quota/config with the correct tenant) before '
                    'applying 0007 — refusing to blind-fill a security column', n;
            END IF;
        END $$
        """
    )

    op.execute("ALTER TABLE request_quota_config ALTER COLUMN tenant_id SET NOT NULL")


def downgrade() -> None:
    op.execute("ALTER TABLE request_quota_config ALTER COLUMN tenant_id DROP NOT NULL")
