"""billing_attribution_rules — re-attribute autonomous spend to a bill tenant + member.

WHY
---
Operator policy (2026-09-25): every improvement stream (workflow / procedure /
prompt) is billed as a MEMBER of the fr-test tenant, and execution projects are
billed to the fr-test tenant's EXECUTION member. Asset-authoring spend follows
the project it happens in. Today every autonomous stream calls through ONE shared
licence subject (svc-requirements) under its own source tenant and with
``actor_id`` NULL, so neither the bill tenant nor the member can be derived from
the wire. Improvement streams carry NO fr project (``fr.streams.project_id`` is
NULL); any real project id on that spend is an execution project. So the match
is (source tenant, optional source subject, project present / absent / any).

WHAT THIS CREATES
-----------------
``billing_attribution_rules`` — config, read by ``services.attribution_rules``
on the cost ingest path (``services.cost_flusher``). Schema only: NO rows are
seeded, because the member ids do not exist yet; rules are inserted through
``PUT /api/config/attribution-rules``. Forward-only: no existing
``billing_cost_events`` row is rewritten.

``uq_attribution_rules_match`` is a unique INDEX (not a constraint) because it
is over an expression: ``source_subject_id`` NULL means "any subject", and a
plain unique would let two tenant-wide rules for the same (tenant, match_project)
co-exist (NULLs are distinct), making precedence ambiguous.

⚠️ COMPANION EDIT — ``src/db.py``. Every statement is mirrored BYTE-IDENTICALLY
into ``db.ADDITIVE_MIGRATIONS`` and replayed on pod start; all are idempotent.
The flusher reads this table inside a SAVEPOINT and fails soft (no attribution)
if it is missing, so a pod on this code against a pre-0041 database keeps
ingesting unchanged.

Revision ID: 0041_attribution_rules
Revises: 0040_vendor_cost_actor_daily
Create Date: 2026-09-25
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0041_attribution_rules"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0040_vendor_cost_actor_daily"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Mirrored byte-identically in src/db.py ADDITIVE_MIGRATIONS.
_CREATE_RULES = (
    "CREATE TABLE IF NOT EXISTS billing_attribution_rules ("
    "id UUID PRIMARY KEY, "
    "source_tenant_id UUID NOT NULL, "
    "source_subject_id UUID, "
    "match_project TEXT NOT NULL, "
    "bill_tenant_id UUID NOT NULL, "
    "actor_id UUID, "
    "reason TEXT NOT NULL, "
    "enabled BOOLEAN NOT NULL DEFAULT true, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT ck_attribution_rules_match_project CHECK "
    "(match_project IN ('any', 'with_project', 'without_project')))"
)
_CREATE_RULES_UNIQUE = (
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_attribution_rules_match "
    "ON billing_attribution_rules (source_tenant_id, "
    "coalesce(source_subject_id, '00000000-0000-0000-0000-000000000000'::uuid), "
    "match_project)"
)


def upgrade() -> None:
    op.execute(_CREATE_RULES)
    op.execute(_CREATE_RULES_UNIQUE)


def downgrade() -> None:
    # Config only: the attribution already applied lives on each cost event
    # (tenant_id + metadata.attributed_from_tenant_id), so dropping the rules
    # loses no ledger data.
    op.execute("DROP TABLE IF EXISTS billing_attribution_rules")
