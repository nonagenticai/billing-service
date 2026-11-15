"""Append-only audit log."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, String, Text, UniqueConstraint, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import _utcnow


class AuditLog(Base):
    """Append-only record of who-changed-what for budget/markup/provider rate writes.

    No FKs by design — the audit log must survive even if a referenced row is
    hard-deleted. (Deletes are moving toward soft-delete anyway.)
    """

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        server_default=text("now()"),
    )
    actor_sub: Mapped[str | None] = mapped_column(String(255), nullable=True)
    actor_role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # tenant_id is a real Postgres `uuid` column (alembic 0011). as_uuid=False is
    # deliberate: the DB type is uuid while Python keeps receiving/binding `str`, so
    # every existing comparison, log line and JSON serialisation keeps working.
    tenant_id: Mapped[str | None] = mapped_column(Uuid(as_uuid=False), nullable=True)
    tenant_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    before: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    after: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    request_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        Index("ix_audit_log_ts", ts.desc()),
        Index("ix_audit_log_tenant_ts", "tenant_id", ts.desc()),
        Index("ix_audit_log_action_ts", "action", ts.desc()),
        Index("ix_audit_log_tenant_name", "tenant_name"),
    )


class DlqReattribution(Base):
    """Receipt for one dead-letter cost event re-attributed by an operator (#243).

    Written by :func:`services.dlq_ops.reattribute_dlq` and committed BEFORE the
    entry is ``XDEL``ed from ``billing:costs:dead``: Redis was the event's only
    copy, so this row (``original_fields`` holds the full quarantine record,
    ``_dlq_*`` included) is what survives of the entry. ``reinjected_stream_id``
    is the new ``billing:costs`` id, which ``processed_cost_messages`` keys on.

    ``replay_marker`` is UNIQUE: it is the same per-EVENT key the Redis
    idempotency marker uses, so a second receipt for one event is refused by the
    database as well. ``reattributed_from`` is NULL when the stored tenant was
    absent/blank, else the raw stored value (the nil uuid). No FKs, as for
    ``audit_log``. Alembic 0047.
    """

    __tablename__ = "billing_dlq_reattributions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        server_default=text("now()"),
    )
    dead_entry_id: Mapped[str] = mapped_column(String(64), nullable=False)
    original_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    replay_marker: Mapped[str] = mapped_column(String(128), nullable=False)
    reattributed_from: Mapped[str | None] = mapped_column(Text, nullable=True)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    reattributed_by: Mapped[str] = mapped_column(String(255), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    original_fields: Mapped[dict] = mapped_column(JSONB, nullable=False)
    reinjected_stream_id: Mapped[str] = mapped_column(String(64), nullable=False)
    request_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        UniqueConstraint("replay_marker", name="uq_dlq_reattributions_marker"),
        Index("ix_dlq_reattributions_tenant_created", "tenant_id", "created_at"),
    )


class DlqArchive(Base):
    """One dead-letter entry the age trim removed — archived BEFORE the ``XTRIM``.

    Written by :func:`services.dlq_ops.archive_then_trim_dead_letter`, which
    commits these rows and only then trims ``billing:costs:dead``; if the commit
    fails it does not trim. ``fields`` is the full quarantine record, ``_dlq_*``
    included, so an AGE-expired entry is recoverable from here instead of lost.
    (Not covered: the quarantine XADD's ``MAXLEN ~ 100000`` cap, which drops the
    oldest entries unarchived if the stream ever reaches 100k.)
    UNIQUE on (stream, dead_entry_id): re-archiving on a later tick is a no-op.
    Alembic 0047.
    """

    __tablename__ = "billing_dlq_archive"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    archived_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        server_default=text("now()"),
    )
    stream: Mapped[str] = mapped_column(String(128), nullable=False)
    dead_entry_id: Mapped[str] = mapped_column(String(64), nullable=False)
    dlq_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dlq_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fields: Mapped[dict] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint("stream", "dead_entry_id", name="uq_dlq_archive_entry"),
        Index("ix_dlq_archive_archived_at", "archived_at"),
    )
