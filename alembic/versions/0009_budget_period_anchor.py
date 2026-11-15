"""Add ``token_budgets.consumed_period_start`` — the period ``consumed_tokens`` belongs to.

``period`` ('monthly'/'weekly') was decoration: it selected a Redis key suffix and
an alert bucket, but nothing ever reset ``consumed_tokens``. The column only grew
(``current + take``) and was zeroed exactly once, at row creation, so a monthly
budget was in fact a LIFETIME cap — a tenant that burned it in January was denied
for the life of the row.

This column anchors the consumption to a period; ``services/budget_period.py``
reports 0 once the anchor is older than the current period, and the debit path
rebases the column inside its ``SELECT ... FOR UPDATE`` transaction. No reset cron
(a missed run would silently over-enforce, the same class of bug).

Backfill writes the period the row was CREATED in (``starts_at``), which is the
same rule the code applies lazily to a NULL, so eager and lazy resolution agree.
Rows left NULL (unknown/other period) keep lifetime semantics — the conservative
direction.

Idempotent: ``ADD COLUMN IF NOT EXISTS`` + a NULL-only UPDATE. The app also runs
``Base.metadata.create_all`` at startup, so the column may already exist when the
migrate initContainer runs.

Revision ID: 0009_budget_period_anchor
Revises: 0008_quota_zero_blocks
Create Date: 2026-07-29
"""

from __future__ import annotations

from alembic import op

revision = "0009_budget_period_anchor"
down_revision = "0008_quota_zero_blocks"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE token_budgets ADD COLUMN IF NOT EXISTS consumed_period_start DATE")
    op.execute(
        "UPDATE token_budgets SET consumed_period_start = CASE "
        "WHEN period = 'monthly' THEN date_trunc('month', starts_at AT TIME ZONE 'UTC')::date "
        "WHEN period = 'weekly' THEN date_trunc('week', starts_at AT TIME ZONE 'UTC')::date "
        "END WHERE consumed_period_start IS NULL AND period IN ('monthly', 'weekly')"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE token_budgets DROP COLUMN IF EXISTS consumed_period_start")
