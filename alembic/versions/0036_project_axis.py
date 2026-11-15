"""project_id — a first-class axis so spend can be priced per kanban project.

WHAT THIS IS
------------
Today spend is scoped by tenant / licence / subject / service / provider / model.
A "project" (the kanban unit work is actually planned and budgeted in) is not one
of those dimensions, so there is no way to price, aggregate or rate-scope by it.
This migration adds the COLUMN everywhere the axis has to live and rebuilds the
two uniqueness objects that would otherwise be wrong once it exists.

⛔ THIS MIGRATION IS INERT ON PURPOSE. NOTHING SENDS A PROJECT ID YET.
Every producer is unchanged, so after this runs:

* ``billing_cost_events.project_id`` and ``billing_metering_events.project_id``
  are NULL on every row, old and new, until a producer starts carrying one;
* ``billing_cost_daily.project_id`` is the sentinel on every row (see below), so
  every existing aggregate keeps its current grain and every total is unchanged;
* ``markup_rules.project_id`` is NULL on every existing row — the wildcard —
  which is exactly the behaviour those rules have today.

There is NO backfill and none is possible: nothing upstream ever asserted a
project, so there is nothing to backfill FROM. Inventing one would fabricate an
attribution that never existed. Any future edit that adds an ``UPDATE`` here is a
correctness regression, not a completeness improvement (same rule 0032 states for
``reservation_id``).

⚠️ DEDUP SEMANTICS OF ``uq_cost_daily`` CHANGE, AND THAT IS THE POINT
---------------------------------------------------------------------
``project_id`` joins ``uq_cost_daily``, taking it from 10 columns to 11. Once a
producer DOES start sending a project id, rows that previously aggregated into
ONE tenant-level daily row split into one row PER PROJECT. That is intended —
per-project pricing is the whole purpose — but it is a real change of grain, so:

* any reader that assumed "one row per (tenant, licence, subject, service,
  provider, model, byo, owner_provided, cost_source, day)" must now SUM across
  ``project_id``, exactly as it already sums across the other dimensions;
* no total changes while the axis is unpopulated, because every row carries the
  same sentinel and therefore still collapses onto one row.

⭐ WHY A SENTINEL ON ``billing_cost_daily`` AND NOT NULL
--------------------------------------------------------
``billing_cost_daily.project_id`` is ``NOT NULL DEFAULT
'00000000-0000-0000-0000-000000000004'``. It must NOT be nullable — but read the
reason carefully, because the obvious one is NOT the live mechanism.

⚠️ WHAT IS *NOT* TRUE: "a NULL would make every rollup cycle INSERT a fresh row
instead of UPDATEing". A re-rollup of the same day never reaches ``ON CONFLICT``
at all. ``services.rollup_engine.rollup_daily`` issues a guarded
``DELETE FROM billing_cost_daily WHERE bucket_date = :target_date`` BEFORE the
insert, so the whole day is removed and re-inserted each cycle.

⚠️ CITE THAT DELETE BY SYMBOL AND GUARD, NEVER BY LINE. It is the
``delete(CostDaily).where(CostDaily.bucket_date == target_date)`` sitting under
the guard ``if cost_rows or cost_covered:`` immediately above it, inside
``rollup_daily``; the conflict target is the
``on_conflict_do_update(constraint="uq_cost_daily")`` further down the same
function. This paragraph used to say "~line 818" — both had since moved (the
DELETE to ~841, the constraint to ~905) with nothing about the mechanism
changed. A line number is a fingerprint of one revision; a named symbol and its
guard condition survive the edit.

``rollup_engine``'s own comments on the ``on_conflict_do_update`` block say the
same thing, and no test exercises the conflict path.
Cross-cycle fan-out is prevented by delete-then-insert, not by the sentinel.

⭐ WHAT IS TRUE, AND WHY THE COLUMN STAYS ``NOT NULL`` ANYWAY — DEFENCE IN DEPTH.
``uq_cost_daily`` genuinely IS the conflict target: the rollup upserts with
``on_conflict_do_update(constraint="uq_cost_daily")``, and ``project_id`` is now
one of its 11 columns. A plain UNIQUE is ``NULLS DISTINCT``, so a NULL never
matches itself. Two consequences that do not depend on the delete:

* the constraint would stop enforcing the daily grain for project-less rows —
  it is the ONLY database-side object that does — so ANY writer (the residual
  concurrent-writer path the ``ON CONFLICT`` clause covers, a backfill, a repair
  script) could silently duplicate them;
* the moment the guarded DELETE is removed, narrowed to a subset of the day, or
  its ``if cost_rows or cost_covered`` guard declines to fire, the fan-out above
  becomes real and silent — with no schema-level refusal anywhere.

So the sentinel is the net under a behaviour enforced today by one ``DELETE`` in
application code. Do not make this column nullable, and do not "simplify" it away
on the grounds that the upsert never conflicts in practice.

This is not a new convention: ``NULL_LICENSE_ID`` / ``NULL_APP_ID`` /
``NULL_PLAN_ID`` / ``NULL_SUBJECT_ID`` are the same device for the same reason
(``models/db/_common.py``), and 0035 documents it at length for
``billing_metering_monthly_usage.plan_id``. The new sentinel is
``NULL_PROJECT_ID`` = ``...0004``, added beside the other four. It means "no
project was asserted for this spend" — never a real project, and never a default
project.

``ADD COLUMN … NOT NULL DEFAULT <constant>`` is catalogue-only on Postgres 11+
(the default is stored as a missing-value, not written into every row), so this
takes no table rewrite even though the table is live.

⛔ REBUILDING ``ix_markuprules_open`` IS THE LOAD-BEARING HALF
--------------------------------------------------------------
``ix_markuprules_open`` is the partial unique enforcing "at most one OPEN row per
match-tuple", and ``services.markup.upsert_markup_rule`` depends on it: it closes
the currently-open row for a tuple and inserts the new one.

If ``project_id`` were added to the column but NOT to that index, a tenant-wide
rule and a project-specific rule with otherwise identical dimensions would occupy
the SAME open slot. Two consequences, both bad and both silent: writing the
project rule would CLOSE the tenant-wide rule and change the rate on every other
project; and, until it did, both rows would be "open" at once with the database
offering no refusal at all — leaving resolution to be *scored* at read time.

⛔ Ambiguity must be refused on WRITE, never scored on READ. The column without
the index is worse than neither. This is the identical judgement 0027 made when
it added ``byo`` as the 6th term; ``project_id`` is the 7th.

⚠️ The index is rebuilt from its CURRENT, POST-0027 SIX-TERM form — not from the
five-term form a stale ``pg_indexes`` transcript or the 0001 baseline shows.
0027 already extended it. Reproducing the baseline shape here would silently DROP
the ``byo`` term and re-open the exact collision 0027 closed.

NO INDEX ON THE RAW EVENT TABLES, DELIBERATELY
----------------------------------------------
``billing_cost_events`` and ``billing_metering_events`` get the column and NO
index. Raw events are read by partition + time (or by the existing
tenant/licence/subject/provider composites), never grouped by project — the
per-project question is answered off ``billing_cost_daily``, which is what the
new ``ix_cost_daily_tenant_project_date`` serves. A bare btree on the raw tables
would be paid for on every INSERT on the hot ingest path and serve nothing. ⚠️
This is a DIFFERENT judgement from 0028's ``event_ts`` (which DID get a partial
index because it serves direct range scans); do not add one here by analogy.

SAFE AGAINST A LIVE, PARTITIONED TABLE
--------------------------------------
``billing_cost_events`` is RANGE-partitioned on ``created_at`` (14 monthly
children attached in the 0001 baseline; the count is not load-bearing, the
cascade is). Quoting the rule 0021/0028/0032 established for this table so the
next reader does not re-derive it: ``ADD COLUMN`` of a NULLABLE column with NO
default is a CATALOGUE-ONLY change — no table rewrite, no row touched, no
backfill — and on a RANGE-partitioned parent it cascades to every existing
partition and is inherited by every future one
(``create_cost_events_partition_for`` mints partitions with ``CREATE TABLE …
PARTITION OF``). So the statement here targets the **PARENT ONLY**; no
per-partition DDL is issued and the partition function is unchanged. No trigger
on the parent or any partition is dropped, disabled or recreated.

``billing_metering_events``, ``billing_cost_daily`` and ``markup_rules`` are NOT
partitioned; each takes a single ordinary ``ALTER TABLE``.

⛔ QUOTA COUNTERS DO NOT GET THIS AXIS — DO NOT "COMPLETE" THIS LATER
---------------------------------------------------------------------
``billing_metering_monthly_usage`` / ``uq_monthly_usage``, and the
``billing_metering_hourly`` / ``billing_metering_daily`` grains, get **NO**
``project_id`` column. This is a deliberate asymmetry with ``billing_cost_daily``
above, and it is a SECURITY property, not an oversight:

``billing_metering_monthly_usage`` answers "how many requests has this licence
made this month" for **QUOTA ENFORCEMENT**. Adding ``project_id`` to its key
would split the quota counter per project, so a tenant capped at N requests could
spread work across K projects and serve K×N — evading the limit by a
reorganisation of its own kanban board, with every counter reading under its cap.

The rule that separates the two tables: **money is attributed per project;
entitlement is enforced per licence.** A column that joins a COST aggregation key
splits a bill (correct, that is pricing). The same column on a QUOTA key splits a
CAP (a fail-open). Anyone extending the axis later must state which of the two a
table is before touching its unique constraint.

⚠️ DEPLOY ORDER / COMPANION EDIT — ``src/db.py``
-----------------------------------------------
Every ``ADD COLUMN`` string below is mirrored BYTE-IDENTICALLY into
``db.ADDITIVE_MIGRATIONS``, replayed on every pod start. That mirror is not
cosmetic: once a producer names ``project_id`` in its INSERT, a pod carrying the
new code against a database still on pre-0036 fails EVERY such INSERT with
``UndefinedColumn`` — a full ingest outage, not a degradation. 0032 documents
this exact trap ("⛔ THE OMISSION HERE IS AN INGEST OUTAGE, NOT A COSMETIC GAP").

⚠️ Only the ADD COLUMNs are mirrored. The two index/constraint REBUILDS stay here
and here only: ``ADDITIVE_MIGRATIONS`` fails SOFT per statement, so a boot-time
drop-then-recreate of ``uq_cost_daily`` or ``ix_markuprules_open`` would open a
window with no uniqueness on every pod start and, if the recreate failed, leave
the object simply GONE with the pod healthy. That is what
``tests/test_additive_migrations_are_additive.py`` forbids and what
``0015_pricing_cost_daily_shapes`` exists to own.

Revision ID: 0036_project_axis
Revises: 0035_monthly_usage_plan_default
Create Date: 2026-09-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0036_project_axis"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0035_monthly_usage_plan_default"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


#: ⛔ MIRRORED BYTE-IDENTICALLY IN ``src/db.py``'s ``ADDITIVE_MIGRATIONS``.
#: A boot-replayed schema that differs from the migrated one is how the two
#: copies drift apart silently.
#:
#: PARENT ONLY — ``billing_cost_events`` is RANGE-partitioned; see the module
#: docstring for why no per-partition DDL is issued.
_ADD_COST_EVENT_COLUMN = "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS project_id UUID"
#: ``billing_metering_events`` is NOT partitioned. Nullable, no index.
_ADD_METERING_EVENT_COLUMN = (
    "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS project_id UUID"
)
#: NOT NULL + sentinel default, because this column joins ``uq_cost_daily`` — the
#: only database-side guard on the daily grain — and a NULL in a ``NULLS
#: DISTINCT`` unique key enforces nothing. ⚠️ NOT because the rollup's
#: ``ON CONFLICT`` fires each cycle: the guarded DELETE in
#: ``services.rollup_engine`` removes the day first. See the module docstring.
#: Literal must stay in step with ``models/db/_common.py`` NULL_PROJECT_ID.
_ADD_COST_DAILY_COLUMN = (
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS project_id UUID "
    "NOT NULL DEFAULT '00000000-0000-0000-0000-000000000004'::uuid"
)
#: Nullable on ``markup_rules``: NULL means the rule is TENANT-WIDE (the
#: wildcard, matching every other dimension's convention). A real uuid scopes the
#: rule to one project.
_ADD_MARKUP_RULE_COLUMN = "ALTER TABLE markup_rules ADD COLUMN IF NOT EXISTS project_id UUID"

_DROP_COST_EVENT_COLUMN = "ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS project_id"
_DROP_METERING_EVENT_COLUMN = "ALTER TABLE billing_metering_events DROP COLUMN IF EXISTS project_id"
_DROP_COST_DAILY_COLUMN = "ALTER TABLE billing_cost_daily DROP COLUMN IF EXISTS project_id"
_DROP_MARKUP_RULE_COLUMN = "ALTER TABLE markup_rules DROP COLUMN IF EXISTS project_id"


# --- uq_cost_daily: 10 columns -> 11 --------------------------------------
# The pre-0036 column list, verbatim from `0015_pricing_cost_daily_shapes`
# (which owns this constraint's shape).
_COST_DAILY_COLUMNS_NO_PROJECT = (
    "tenant_id, license_id, subject_id, service_key, provider, "
    "served_model, byo, owner_provided, cost_source, bucket_date"
)
# WITH project_id. Appended last, after `bucket_date`, so the existing 10-column
# prefix is untouched.
_COST_DAILY_COLUMNS_WITH_PROJECT = f"{_COST_DAILY_COLUMNS_NO_PROJECT}, project_id"

_DROP_COST_DAILY_UNIQUE = "ALTER TABLE billing_cost_daily DROP CONSTRAINT IF EXISTS uq_cost_daily"
_ADD_COST_DAILY_UNIQUE_WITH_PROJECT = (
    f"ALTER TABLE billing_cost_daily ADD CONSTRAINT uq_cost_daily "
    f"UNIQUE ({_COST_DAILY_COLUMNS_WITH_PROJECT})"
)
_ADD_COST_DAILY_UNIQUE_NO_PROJECT = (
    f"ALTER TABLE billing_cost_daily ADD CONSTRAINT uq_cost_daily "
    f"UNIQUE ({_COST_DAILY_COLUMNS_NO_PROJECT})"
)

# The per-project read path. This is the ONE index the axis earns: the
# per-project question is answered off the daily rollup, never off raw events.
_CREATE_COST_DAILY_PROJECT_IDX = (
    "CREATE INDEX IF NOT EXISTS ix_cost_daily_tenant_project_date "
    "ON billing_cost_daily (tenant_id, project_id, bucket_date)"
)
_DROP_COST_DAILY_PROJECT_IDX = "DROP INDEX IF EXISTS ix_cost_daily_tenant_project_date"


# --- ix_markuprules_open: 6 terms -> 7 ------------------------------------
_DROP_OPEN_IDX = "DROP INDEX IF EXISTS ix_markuprules_open"

# WITH project_id — mirrors models/db/config.py MarkupRule.__table_args__ after
# 0036. The first six terms are 0027's CURRENT definition, reproduced verbatim;
# `byo` must not be lost here.
_CREATE_OPEN_IDX_WITH_PROJECT = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_markuprules_open ON markup_rules ("
    "coalesce(tenant_id::text, ''), "
    "coalesce(license_id::text, ''), "
    "coalesce(service_key, ''), "
    "coalesce(provider, ''), "
    "coalesce(model, ''), "
    "coalesce(byo::text, ''), "
    "coalesce(project_id::text, '')"
    ") WHERE effective_until IS NULL"
)

# WITHOUT project_id — the pre-0036 (post-0027) definition, for downgrade.
# ⚠️ Six terms, not five: 0027 added `byo`.
_CREATE_OPEN_IDX_NO_PROJECT = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_markuprules_open ON markup_rules ("
    "coalesce(tenant_id::text, ''), "
    "coalesce(license_id::text, ''), "
    "coalesce(service_key, ''), "
    "coalesce(provider, ''), "
    "coalesce(model, ''), "
    "coalesce(byo::text, '')"
    ") WHERE effective_until IS NULL"
)


def upgrade() -> None:
    # 1. Raw event tables. Nullable, no default, no backfill, NO INDEX.
    #    billing_cost_events is PARENT ONLY — the ADD COLUMN cascades to all 14
    #    partitions and is inherited by future ones.
    op.execute(_ADD_COST_EVENT_COLUMN)
    op.execute(_ADD_METERING_EVENT_COLUMN)

    # 2. The cost rollup. Sentinel, NOT NULL — see the module docstring: a
    #    nullable column in uq_cost_daily would disarm the only database-side
    #    guard on the daily grain (NULLS DISTINCT). ⚠️ It is the guarded DELETE
    #    in services.rollup_engine, not this sentinel, that keeps a re-rollup
    #    from duplicating the day; the sentinel is the net if that DELETE ever
    #    goes away.
    op.execute(_ADD_COST_DAILY_COLUMN)

    # 3. Widen the upsert key. Order matters: the column must exist before a
    #    constraint can reference it. ⚠️ This CHANGES DEDUP SEMANTICS once the
    #    axis is populated (per-project rows instead of one tenant-level row);
    #    it is a no-op today because every row carries the same sentinel.
    #    ADD CONSTRAINT has no IF NOT EXISTS in Postgres; the DROP IF EXISTS
    #    immediately above is what makes the pair idempotent.
    op.execute(_DROP_COST_DAILY_UNIQUE)
    op.execute(_ADD_COST_DAILY_UNIQUE_WITH_PROJECT)
    op.execute(_CREATE_COST_DAILY_PROJECT_IDX)

    # 4. The pricing rule dimension, and the unique that makes it unambiguous.
    #    ⛔ The index rebuild is the load-bearing half — without it a tenant-wide
    #    and a project-specific rule share one open slot.
    op.execute(_ADD_MARKUP_RULE_COLUMN)
    op.execute(_DROP_OPEN_IDX)
    op.execute(_CREATE_OPEN_IDX_WITH_PROJECT)


def downgrade() -> None:
    # Reverses in mirror order, each uniqueness object returning to its narrower
    # form BEFORE its column goes.
    #
    # ⚠️ A downgrade can FAIL here BY DESIGN, in two places, and both failures
    # are the correct outcome rather than a bug to work around:
    #
    #  * ``ix_markuprules_open`` — if two open rules differ only in
    #    ``project_id``, the narrower unique cannot be built over them. Silently
    #    dropping one of two deliberately distinct commercial rules would change
    #    what a tenant is billed. Close one of them first (0027 says the same
    #    about ``byo``).
    #  * ``uq_cost_daily`` — if the axis has been populated, two daily rows may
    #    differ only in ``project_id``. Rebuilding the 10-column constraint over
    #    them would require MERGING money across projects, which is a
    #    restatement, not a schema change. Re-aggregate deliberately first.
    #
    # ⚠️ Reversible as schema, LOSSY as data: dropping the columns destroys every
    # project attribution written since the upgrade, and nothing in this database
    # holds a second copy. Re-upgrading restores the columns empty/sentinel.
    op.execute(_DROP_OPEN_IDX)
    op.execute(_CREATE_OPEN_IDX_NO_PROJECT)
    op.execute(_DROP_MARKUP_RULE_COLUMN)

    op.execute(_DROP_COST_DAILY_PROJECT_IDX)
    op.execute(_DROP_COST_DAILY_UNIQUE)
    op.execute(_ADD_COST_DAILY_UNIQUE_NO_PROJECT)
    # Dropping the column after the constraint is rebuilt keeps the window in
    # which billing_cost_daily has NO upsert key as short as possible; doing it
    # the other way round would let Postgres cascade-drop the constraint.
    op.execute(_DROP_COST_DAILY_COLUMN)

    op.execute(_DROP_METERING_EVENT_COLUMN)
    op.execute(_DROP_COST_EVENT_COLUMN)
