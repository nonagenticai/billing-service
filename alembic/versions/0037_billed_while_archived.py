"""billing_cost_events.billed_while_archived — flag archived-tenant spend, never refuse it.

THE GAP THIS CLOSES
-------------------
``archived_at`` is enforced pre-spend in FOUR places in this estate — licences'
own licence-mint path (``licenses/main.py:3704``), its token-mint path
(``licenses/main.py:4775``), Envoy ext_authz's ``/license/introspect``
(``licenses/models.py:3603-3606``, GATEWAY-scoped only and blind to east-west
traffic), and this service's own pre-spend reservation gate
(``routers/budget.py``'s ``reserve_tenant_budget``). NONE of them is the cost-
event ingestion path: ``services.cost_flusher.flush_costs`` persists every
``CostEvent`` off the ``billing:costs`` Redis stream, which ``inference-router``
and ``mlx-ingestion-backend`` push to DIRECTLY, east-west, bypassing both the
gateway check and the budget gate. Measured: tenant ``rmt-e2e`` billed $1.3151
over 7 days while archived; alert ``TenantSpendOnArchivedRegistryRow`` firing;
licences issues #207 and #197 open.

WHY THIS IS A FLAG, NOT A REFUSAL
----------------------------------
By the time a cost event reaches the flusher, the compute is already spent.
``routers/budget.py``'s own comment on its (pre-spend) archival refusal states
the general rule verbatim: dropping a row "converts an attribution fault into an
UNMETERED one" — which is worse than mis-attributing money already spent, not
better. So ``flush_costs`` persists the row exactly as it would any other row and
merely marks it: ``billed_while_archived = true`` when
``services.tenant_names.is_tenant_archived`` answered True for that row's tenant
at flush time. It never touches ``excluded_from_billing`` — the two are
independent, and combining them would silently stop billing money that was
genuinely spent.

NULLABLE, NEVER BACKFILLED, NO DEFAULT
---------------------------------------
⛔ THERE IS NO BACKFILL AND NONE IS POSSIBLE. Whether a given historical row's
tenant was archived AT THE TIME that row was flushed is not reconstructable —
the licences registry only exposes CURRENT archived state, not a history of it.
Every row written before this migration ships is NULL forever, and NULL means
"the check never ran on this row" — never "checked, and clean". That is a
different meaning from ``False``, which every row flushed after this ships will
carry explicitly whenever ``is_tenant_archived`` said "no". Same rule 0032 states
for ``reservation_id`` and 0036 states for ``project_id``: any future edit that
adds an ``UPDATE`` here is a correctness regression, not a completeness
improvement, because there is nothing true to backfill FROM.

NO INDEX, DELIBERATELY
-----------------------
The ledger-derived gauge that reads this column
(``services.archived_tenant_spend.publish_archived_tenant_spend``) runs on the
rollup leader's own timer, at a low cadence, over a 30-day window already
bounded by ``created_at`` — which the table's existing
``ix_costs_tenant_created`` composite already serves. A bare btree on this
column alone would be paid for on every cost INSERT (the hot ingest path,
alongside every other row on this table) and would not be selective enough to
help a query that is already filtering on ``created_at`` first. Same judgement
0036 makes for ``project_id`` on this identical table; do not add one here by
analogy either.

SAFE AGAINST A LIVE, PARTITIONED TABLE
---------------------------------------
``billing_cost_events`` is RANGE-partitioned on ``created_at``. Quoting the rule
0021/0028/0032/0036 already established for this table: ``ADD COLUMN`` of a
NULLABLE column with NO default is a CATALOGUE-ONLY change on Postgres — no
table rewrite, no row touched, no backfill — and on a RANGE-partitioned parent
it cascades to every existing partition and is inherited by every future one
(``create_cost_events_partition_for`` mints partitions with ``CREATE TABLE …
PARTITION OF``). So the statement below targets the PARENT ONLY; no
per-partition DDL is issued and the partition function is unchanged. No trigger
on the parent or any partition is dropped, disabled or recreated.

⚠️ DEPLOY ORDER / COMPANION EDIT — ``src/db.py``
-------------------------------------------------
The flusher NAMES ``billed_while_archived`` in its cost INSERT, so a pod
carrying the new code against a database still on pre-0037 would fail EVERY
cost INSERT with ``UndefinedColumn`` — a full billing-ingest outage, not a
degradation, exactly the trap 0028/0032/0036 each document for their own
column. The ``_ADD_COLUMN`` string below is mirrored BYTE-IDENTICALLY into
``db.ADDITIVE_MIGRATIONS``, replayed on every pod start, immediately after the
0036 ``project_id`` entry.

Revision ID: 0037_billed_while_archived
Revises: 0036_project_axis
Create Date: 2026-09-16
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0037_billed_while_archived"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0036_project_axis"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Mirrors models/db/events.py CostEvent.billed_while_archived, and mirrored
# byte-identically in src/db.py ADDITIVE_MIGRATIONS. PARENT ONLY — see the
# module docstring for why no per-partition DDL is issued.
_ADD_COLUMN = (
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS billed_while_archived BOOLEAN"
)
_DROP_COLUMN = "ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS billed_while_archived"


def upgrade() -> None:
    op.execute(_ADD_COLUMN)
    # ⛔ NO BACKFILL STATEMENT, DELIBERATELY, AND NO INDEX. See the module
    # docstring: NULL is the correct permanent value for every pre-0037 row —
    # there is nothing true to backfill FROM, because whether a row's tenant
    # was archived at the time it was flushed cannot be reconstructed from the
    # registry's current-only view of archived state.


def downgrade() -> None:
    # Reversible as schema, lossy as data — dropping the column destroys every
    # archived-tenant flag written since the upgrade, and this database holds
    # no other copy of it (the fact lived only in the licences registry's
    # state AT THE MOMENT of each flush, which is gone the instant the flush
    # moves on). Re-upgrading restores the column, empty. Same trade-off 0032
    # accepts for reservation_id.
    op.execute(_DROP_COLUMN)
