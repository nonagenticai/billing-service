"""Idempotency inbox for the usage/metering-event stream consumer.

Adds ``processed_event_messages`` — a non-partitioned table keyed on the Redis
Stream message id, mirroring ``processed_cost_messages`` (0002) for the EVENTS
path. The event flusher claims each id with ``INSERT … ON CONFLICT DO NOTHING``
in the same transaction as the ``billing_metering_events`` insert + the additive
monthly rollup, so an at-least-once redelivery (crash-after-commit-before-XACK,
or a NOGROUP recovery recreating the group at id=0 against a now-durable stream)
can never double-increment ``request_count``/``denied_count`` or the Redis
``quota:{lic}:{month}`` counter (which would falsely push a tenant over quota).

It is deliberately NOT part of ``billing_metering_events`` (a standalone PK on
the immutable Redis id is the clean dedupe key).

Revision ID: 0004_processed_event_messages
Revises: 0003_rename_to_billing
Create Date: 2026-07-19
"""

from __future__ import annotations

from alembic import op

revision = "0004_processed_event_messages"
down_revision = "0003_rename_to_billing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.processed_event_messages (
            stream_msg_id varchar(64) PRIMARY KEY,
            processed_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_processed_event_messages_processed_at "
        "ON public.processed_event_messages (processed_at)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS public.processed_event_messages")
