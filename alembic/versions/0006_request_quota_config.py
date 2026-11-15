"""Durable request-quota config table.

Gives the per-license monthly request-quota limit a persistent home. Before this,
a quota configured via ``POST /api/quota/config`` lived only as the Redis key
``quota:limit:{license_id}`` (35-day TTL, nothing re-seeding it): once it expired,
``license-authz`` enforcement failed open and ``GET /api/quota/status`` could not
see it. The row is now the source of truth; ``sync_all_quota_limits_to_redis``
republishes the Redis key from these rows on startup and on a periodic loop.

Idempotent by design: the app also runs ``Base.metadata.create_all`` at startup
(``src/db.py``), so the table may already exist when ``alembic upgrade head`` runs
in the migrate initContainer — ``CREATE TABLE IF NOT EXISTS`` + a named constraint
tolerate that and make re-apply / partial-failure re-run safe.

No in-migration backfill: the existing live limits exist only in Redis
(``quota:limit:*``), which a SQL migration cannot read. Recover them with the
operator one-shot ``scripts/backfill_quota_config_from_redis.py`` (run before the
35-day TTL lapses), or they repopulate on the next ``POST /api/quota/config``.

Revision ID: 0006_request_quota_config
Revises: 0005_budget_alert_target_dedupe
Create Date: 2026-07-21
"""

from __future__ import annotations

from alembic import op

revision = "0006_request_quota_config"
down_revision = "0005_budget_alert_target_dedupe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # DDL is kept byte-compatible with what ``Base.metadata.create_all`` produces
    # from the model (tests build the schema that way): unnamed PRIMARY KEY (so
    # Postgres auto-names it ``request_quota_config_pkey``), the named CHECK
    # ``ck_request_quota_positive`` (declared in the model ``__table_args__``), and
    # NO server DEFAULT on the timestamps (the ORM ``default=_utcnow`` populates
    # them, mirroring the ``token_budgets`` convention). This keeps the fresh-alembic
    # (prod) and create_all (test) schemas identical.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.request_quota_config (
            license_id    uuid        NOT NULL,
            tenant_id     varchar(255),
            quota_monthly integer     NOT NULL,
            created_at    timestamptz NOT NULL,
            updated_at    timestamptz NOT NULL,
            PRIMARY KEY (license_id),
            CONSTRAINT ck_request_quota_positive CHECK (quota_monthly > 0)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS public.request_quota_config")
