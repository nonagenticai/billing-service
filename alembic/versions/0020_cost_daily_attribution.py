"""Split the two meanings of NULL_LICENSE_ID on billing_cost_daily.

THE DEFECT
----------
``NULL_LICENSE_ID`` (``000…0000``) is written by exactly one rule --
``license_id or NULL_LICENSE_ID`` -- so it means "no licence reached billing",
nothing more. But ``CostDaily.license_attributed_clause()`` reads it as
"system/S2S cost", and drops every such row from the per-tenant invoice and the
all-tenant overview. Deliberate system cost and a licensed customer whose
``x-license-id`` header was dropped east-west therefore land on the same value
and are written off together.

⛔ The exclusion itself is CORRECT and is not touched here. System cost must
never reach a customer invoice line. What this migration adds is the ability to
tell the two populations apart.

MEASURED ON PROD (worker4, ``billing_cost_daily``, all time, SELECT-only,
2026-09-06), using this migration's own backfill predicate::

    attributed   472 rows  30 tenants  $2,217.53
    unlicensed   562 rows  22 tenants  $3,386.52   <- real customers, written off
    system       233 rows  32 tenants  $  266.28   <- stays excluded
                                       ---------
                                       $5,870.33   (= sum(total_cost_usd), exact)

WHY A COLUMN AND NOT A SECOND SENTINEL
--------------------------------------
``NULL_LICENSE_ID`` is mechanism, not a label: ``budget_debit`` narrows active
budgets with ``license_id.in_({license_id, NULL_LICENSE_ID})`` (licence-less
spend drains the tenant pool -- a new sentinel matches neither branch and stops
being capped), and it is a column of ``uq_cost_daily``/``uq_token_usage`` (a new
value splits every conflict key). Full reasoning in
``models/db/_common.py::classify_cost_attribution``.

NOT ADDED TO uq_cost_daily
--------------------------
``attribution_status`` is functionally determined by ``tenant_id`` +
``license_id``, both already in that key, so including it would split no group
while rewriting the ON CONFLICT shape of every existing row. It is also
deliberately not on ``billing_cost_events``: that table is RANGE-partitioned on
``created_at``, so any constraint touching it must carry the partition key, and
the event grain already holds the truth (``license_id`` is nullable there). The
collapse happens in the rollup, so the fix belongs in the rollup.

The column is NOT NULL with server_default 'unknown' so the ADD COLUMN is a
catalog-only rewrite on PG 11+, then a single UPDATE backfills. ``unknown``
survives only on rows no rollup ever re-stamps.

⚠️ Re-stamping is delivered by DELETE+INSERT, not by ON CONFLICT: ``rollup_daily``
issues a guarded ``DELETE FROM billing_cost_daily WHERE bucket_date = …`` before
the INSERT, so a re-rollup of the same day never reaches the ON CONFLICT path.
That entry is kept for the concurrent-writer residue only (``reprice.py`` holds
no advisory lock and is HTTP-reachable, so under READ COMMITTED it can fire).
Mutation M4 measured this: deleting the ON CONFLICT entry kills no test.
"""

import sqlalchemy as sa
from alembic import op

revision = "0020_cost_daily_attribution"  # <= 32 chars: alembic_version.version_num is varchar(32)
# ⚠️ Re-parented from 0018 onto 0019 when #140 merged. Both branches had
# authored a `0019` off 0018, so the merge produced TWO alembic heads and
# `alembic upgrade head` failed outright ("Multiple head revisions are
# present"). Nothing textual conflicted -- git merged both files cleanly -- so
# the break was only visible by running alembic. The chain is linear again;
# `test_0014_follows_0013_and_keeps_the_chain_linear` asserts `len(heads) == 1`
# and is what fails if a third branch does this again.
down_revision = "0019_unrolled_truncate_gate"
branch_labels = None
depends_on = None

_NIL = "00000000-0000-0000-0000-000000000000"

# The SQL twin of models.db._common.classify_cost_attribution. Same four
# branches, same order.
#
# ⚠️ THE TWO SIDES ARE NOT WRITTEN IN THE SAME LANGUAGE. Every status here is a
# bare SQL string literal ('attributed', 'unlicensed', 'system'); everywhere
# else in the codebase the same values arrive by importing the ATTRIBUTION_*
# constants. So renaming a constant moves the Python side and STRANDS these
# literals, and any test that imports the constant renames its own expectation
# along with it and stays green.
#
# `test_0020_backfill_reproduces_the_python_rule` is the check that closes that:
# it drops the column, stamps 0019, runs THIS migration, and compares every
# backfilled row against `classify_cost_attribution` computed in Python. It is
# the only test that executes this SQL -- the conftest fixture builds the schema
# with `Base.metadata.create_all`, so no other test in the suite runs a
# migration at all.
#
# ⛔ An earlier version of this comment claimed the backfill was "checked against
# it by running the migration in the test database". That was false when written
# and is recorded here because a false coverage claim is worse than no coverage:
# it stops the next reader looking.
_BACKFILL = f"""
UPDATE billing_cost_daily d
SET attribution_status = CASE
    WHEN d.license_id <> '{_NIL}'::uuid THEN 'attributed'
    WHEN d.tenant_id = '{_NIL}'::uuid THEN 'system'
    WHEN EXISTS (
        SELECT 1 FROM request_quota_config q
        WHERE q.tenant_id::text = d.tenant_id::text
          AND q.license_id <> '{_NIL}'::uuid
    ) THEN 'unlicensed'
    ELSE 'system'
END
"""


def upgrade() -> None:
    op.add_column(
        "billing_cost_daily",
        sa.Column(
            "attribution_status",
            sa.String(length=16),
            nullable=False,
            server_default="unknown",
        ),
    )
    op.execute(_BACKFILL)
    # Partial index: every reader of this column filters to the sentinel first
    # (both new clauses are `license_id = NULL_LICENSE_ID AND status = …`), and
    # the sentinel rows are the minority the invoice clause drops.
    op.create_index(
        "ix_cost_daily_attribution_status",
        "billing_cost_daily",
        ["attribution_status", "bucket_date"],
        postgresql_where=sa.text(f"license_id = '{_NIL}'::uuid"),
    )


def downgrade() -> None:
    op.drop_index("ix_cost_daily_attribution_status", table_name="billing_cost_daily")
    op.drop_column("billing_cost_daily", "attribution_status")
