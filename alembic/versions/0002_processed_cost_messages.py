"""Idempotency inbox for the cost-event stream consumer.

Adds ``processed_cost_messages`` — a non-partitioned table keyed on the Redis
Stream message id. The cost flusher claims each id with ``INSERT … ON CONFLICT
DO NOTHING`` in the same transaction as the ``cost_events`` insert, so an
at-least-once redelivery (crash-after-commit-before-XACK, or a NOGROUP recovery
recreating the group at id=0 against a now-durable stream) can never double-bill.

It is deliberately NOT part of ``cost_events`` (which is RANGE-partitioned on
``created_at``; a unique constraint there would have to include ``created_at``,
which defaults to ingest ``now()`` and is not stable across redelivery).

Revision ID: 0002_processed_cost_messages
Revises: 0001_billing_baseline
Create Date: 2026-06-29
"""

from __future__ import annotations

from alembic import op

revision = "0002_processed_cost_messages"
down_revision = "0001_billing_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.processed_cost_messages (
            stream_msg_id varchar(64) PRIMARY KEY,
            processed_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_processed_cost_messages_processed_at "
        "ON public.processed_cost_messages (processed_at)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS public.processed_cost_messages")
