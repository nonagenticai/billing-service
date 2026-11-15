"""tenant_id_divergence_audit — the R15 estate-repair ledger (alembic 0039).

Append-only record of divergent (unregistered) ``tenant_id`` findings. See
``services.tenant_id_divergence`` for the write path
(``record_tenant_id_divergence_ledger``) and ``alembic/versions/0039_tenant_id_
divergence.py`` for the full refusal-of-backfill rationale this table exists
to serve instead. This module's DDL must stay byte-identical in SHAPE (column
names/types/constraints) to that migration — ``tests/test_infra.py`` and the
schema created by ``Base.metadata.create_all`` in the test suite are what
would catch a drift, since the test DB is built from THIS model, not from
running the migration.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, Text, UniqueConstraint, Uuid, text
from sqlalchemy.orm import Mapped, mapped_column

from db import Base


class TenantIdDivergenceAudit(Base):
    """One row per (table, row) found carrying a ``tenant_id`` no registry vouches for.

    No FKs to the table/row it describes, by the same reasoning
    ``models/db/audit.py`` gives for ``AuditLog``: this ledger must survive
    even if the source row is later deleted by that table's own retention
    policy. ``tenant_id`` is never written back anywhere — this table only
    ever grows via INSERT from ``services.tenant_id_divergence.
    record_tenant_id_divergence_ledger``.
    """

    __tablename__ = "tenant_id_divergence_audit"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    #: e.g. "billing_cost_events" — see services.tenant_id_divergence.DEFAULT_LEDGER_TABLES.
    table_name: Mapped[str] = mapped_column(Text, nullable=False)
    #: The source row's own PK, as text — the 8 source tables mix a bigint PK
    #: (audit_log) with a uuid PK (the other 7); text holds either exactly.
    row_pk: Mapped[str] = mapped_column(Text, nullable=False)
    #: The divergent value itself, kept verbatim. NEVER written back to the
    #: source row.
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    #: tenancy.classify_tenant_id's verdict: "synthetic" | "v5_derived" | "v7"
    #: | "v4" | "other". Never "registered" — the recorder skips those.
    class_detected: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )

    __table_args__ = (
        UniqueConstraint(
            "table_name", "row_pk", name="tenant_id_divergence_audit_table_row_pk_key"
        ),
        Index("ix_tenant_id_divergence_audit_class_detected", "class_detected"),
        Index("ix_tenant_id_divergence_audit_tenant_id", "tenant_id"),
    )
