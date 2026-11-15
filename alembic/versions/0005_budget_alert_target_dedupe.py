"""Per-destination dedupe for budget overage alerts.

Adds ``budget_overage_alerts.target_key`` and moves the dedupe unique constraint
from ``(budget_id, threshold_pct, period_start)`` to
``(budget_id, threshold_pct, period_start, target_key)``.

Why: a crossing fans out to N destinations (every active ``tenant_webhooks`` row
PLUS the env-configured platform sink). One row covered all of them, and the
watcher wrote it whenever ANY destination returned 2xx — so a tenant webhook
succeeding while the platform sink timed out marked the crossing delivered and
the sink (the fr notification path) never received it, permanently. The inverse
naive fix — record only when ALL destinations succeed — would re-POST the
healthy ones every 60s, reintroducing the spam removed in #39. Per-destination
rows satisfy both properties.

``target_key`` is a SHA-256 hex of the destination URL rather than the URL
itself: stable across a webhook row being deleted/recreated, bounded length, and
it avoids persisting URLs (which can carry tokens in query params) into a
long-lived audit table.

Existing rows get ``''`` — a LEGACY SENTINEL read as "this whole crossing was
already handled". That preserves the deliberate no-replay-of-history behaviour:
enabling per-target dedupe must NOT resurrect pre-existing crossings (e.g. the
``no_webhooks`` rows already recorded for the current period).

Revision ID: 0005_budget_alert_target_dedupe
Revises: 0004_processed_event_messages
Create Date: 2026-07-20
"""

from __future__ import annotations

from alembic import op

revision = "0005_budget_alert_target_dedupe"
down_revision = "0004_processed_event_messages"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE public.budget_overage_alerts "
        "ADD COLUMN IF NOT EXISTS target_key varchar(64) NOT NULL DEFAULT ''"
    )
    # Swap the constraint. Existing rows all carry target_key='' and were already
    # unique on the 3-tuple, so widening to the 4-tuple cannot violate.
    op.execute(
        "ALTER TABLE public.budget_overage_alerts "
        "DROP CONSTRAINT IF EXISTS uq_budget_overage_dedupe"
    )
    op.execute(
        "ALTER TABLE public.budget_overage_alerts "
        "ADD CONSTRAINT uq_budget_overage_dedupe "
        "UNIQUE (budget_id, threshold_pct, period_start, target_key)"
    )


def downgrade() -> None:
    # Collapsing back to the 3-tuple can only succeed if at most one row exists
    # per crossing; keep the oldest per crossing so the constraint can be created.
    op.execute(
        """
        DELETE FROM public.budget_overage_alerts a
        USING public.budget_overage_alerts b
        WHERE a.budget_id = b.budget_id
          AND a.threshold_pct = b.threshold_pct
          AND a.period_start = b.period_start
          AND a.dispatched_at > b.dispatched_at
        """
    )
    op.execute(
        "ALTER TABLE public.budget_overage_alerts "
        "DROP CONSTRAINT IF EXISTS uq_budget_overage_dedupe"
    )
    op.execute(
        "ALTER TABLE public.budget_overage_alerts "
        "ADD CONSTRAINT uq_budget_overage_dedupe "
        "UNIQUE (budget_id, threshold_pct, period_start)"
    )
    op.execute("ALTER TABLE public.budget_overage_alerts DROP COLUMN IF EXISTS target_key")
