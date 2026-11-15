"""tenant_id_divergence_audit — a ledger table, not a backfill.

⛔ THE ORIGINAL ASK WAS "backfill the 1,536 divergent tenant_id rows to
canonical". THAT IS REFUSED, and this migration does not do it. Measured
2026-09-16, across 37 distinct divergent tenant ids spanning 8 base tables
(``audit_log`` 371, ``billing_cost_daily`` 55, ``billing_cost_events`` 938,
``token_budgets`` 99, ``billing_metering_events`` 39, ``billing_metering_hourly``
17, ``token_usage`` 10, ``billing_metering_daily`` 7 — 1,536 rows total):

* Of the 37, exactly 1 exists anywhere in ``licenses.subject`` — and that one
  row's OWN ``tenant_id`` is NULL there too.
* 0 of the 37 exist in ``licenses.license`` or ``licenses.tenants``.
* 0 match any ``subject.keycloak_sub`` (positive control: 275 subjects carry
  one, so the join itself works).
* 0 of 2,190 combinations (5 uuid namespaces x 438 candidate labels) show ANY
  of the 19 v5-shaped ids to be a ``uuid5()`` of a registered tenant's current
  name, realm, or id (positive control passed on a known-good pair first).

There is no canonical value to backfill TO. Writing one would MANUFACTURE
attribution rather than measure it — assigning real dollars to a tenant chosen
by this migration instead of by evidence, on data that is append-only with no
refund path.

WHAT THIS MIGRATION ACTUALLY DOES
----------------------------------
Creates ``tenant_id_divergence_audit``: an append-only ledger for RECORDING
which row, in which table, carried which unregistered ``tenant_id`` and what
``tenancy.classify_tenant_id`` said it was (``synthetic`` / ``v5_derived`` /
``v7`` / ``v4`` / ``other`` — never ``registered``: a registered row is not
divergent and has no reason to appear here). Schema only — this migration
inserts NOTHING. Population is
``services.tenant_id_divergence.record_tenant_id_divergence_ledger`` (also
reachable as ``scripts/record_tenant_id_divergence.py``), run as a deliberate,
observable, idempotent operator step, NOT baked into `alembic upgrade`:
classification there consults ``tenancy.is_registered_tenant_id``, which can
reach the LIVE licences registry over HTTP — a schema migration must stay
deterministic and network-independent, so that lookup does not belong here.

``(table_name, row_pk)`` is UNIQUE so re-running the recorder is a no-op on
rows already known, and neither ``tenant_id`` nor ``table_name``/``row_pk`` is
a foreign key into the tables they describe: this ledger must survive even if
the row it describes is later deleted by that table's own retention policy
(``settings.Settings.retention_days``), the same "no FKs, so forensic evidence
outlives its subject" choice ``models/db/audit.py`` already makes for
``audit_log``.

⛔ NO UPDATE OF ``tenant_id`` IN THIS MIGRATION OR ANYWHERE THIS PR TOUCHES.

Revision ID: 0039_tenant_id_divergence
Revises: 0038_metering_status_widen
Create Date: 2026-09-16
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0039_tenant_id_divergence"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0038_metering_status_widen"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "tenant_id_divergence_audit"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        # The table this finding came from, e.g. "billing_cost_events". A
        # fixed vocabulary in practice (services.tenant_id_divergence.
        # DEFAULT_LEDGER_TABLES) but stored as plain text, not an enum: this
        # ledger must keep accepting a table added to that tuple later
        # without a second migration just to widen a CHECK constraint.
        sa.Column("table_name", sa.Text(), nullable=False),
        # The source row's own primary key, stored as text because the 8
        # source tables mix a bigint PK (audit_log) with a uuid PK (the
        # other 7) — a single typed column cannot hold both, and text is
        # exact and lossless for either.
        sa.Column("row_pk", sa.Text(), nullable=False),
        # The divergent value itself, kept verbatim for the record. NEVER
        # written back to the source row — see the module docstring's ⛔.
        sa.Column("tenant_id", sa.Uuid(as_uuid=False), nullable=False),
        # tenancy.classify_tenant_id's verdict at the time this row was
        # recorded: "synthetic" | "v5_derived" | "v7" | "v4" | "other".
        # Never "registered" by construction — the recorder skips those.
        sa.Column("class_detected", sa.Text(), nullable=False),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", name=f"{_TABLE}_pkey"),
        sa.UniqueConstraint("table_name", "row_pk", name=f"{_TABLE}_table_row_pk_key"),
    )
    # The read pattern this ledger exists to serve: "how much divergence, by
    # class, has been recorded so far" and "everything recorded for tenant X".
    op.create_index(f"ix_{_TABLE}_class_detected", _TABLE, ["class_detected"])
    op.create_index(f"ix_{_TABLE}_tenant_id", _TABLE, ["tenant_id"])


def downgrade() -> None:
    op.drop_index(f"ix_{_TABLE}_tenant_id", table_name=_TABLE)
    op.drop_index(f"ix_{_TABLE}_class_detected", table_name=_TABLE)
    op.drop_table(_TABLE)
