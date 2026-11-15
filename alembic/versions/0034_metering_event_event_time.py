"""billing_metering_events.event_ts — stop splitting ONE request across TWO invoices.

THE DEFECT, AND WHY IT IS WORSE THAN THE ONE 0028 FIXED
-------------------------------------------------------
A single metered request produces money on TWO buses:

* the COST bus (``billing_cost_events`` → ``billing_cost_daily``) — the LLM spend;
* the METERING bus (``billing_metering_events`` → ``billing_metering_hourly`` →
  ``billing_metering_daily``) — ``request_count``, which
  ``services.billing.generate_billing`` multiplies by the per-app ``AppCostRate``
  and feeds to the per-request surcharge in ``compute_billed_line``.

Alembic 0028 moved the COST bus onto the producer's clock and left the METERING
bus on ingest time. From that moment the two halves of one request were bucketed
on two different clocks. A request emitted at 23:59:50 on the last day of month
M, whose cost flushes promptly but whose metering event is ingested after
midnight, bills its LLM cost in month M and its per-request surcharge in month
M+1. **Both invoices are wrong and NEITHER is amendable**: ``billing_invoices``
rows are immutable (``invoice_immutability`` trigger, alembic 0024) and gaplessly
numbered, so there is no correcting entry to make.

0028's own defect was a charge on the wrong invoice. This one is a charge SPLIT
across two — the same input, billed twice into two different periods, with the
two components no longer summing to anything a reconciliation can recognise.

THE DATA WAS ALREADY ON THE WIRE — MEASURED, NOT ASSUMED
---------------------------------------------------------
Nothing new has to be emitted. ``services/consumer.py`` records a live
measurement of both streams taken 2026-09-05: "``billing:costs`` entries carry
``service_key`` … but ``billing:events`` entries do not — their fields are
tenant/subject/license/app/plan, method, path, status, ``source_ip``,
**timestamp**, latency." The producer's event time reached billing intact and was
then simply never read into a column, exactly as on the cost side before 0028.
This migration gives it somewhere to land.

⚠️ TWO PRODUCERS, NOT ONE, AND THAT IS WHY THE FALLBACK MATTERS
----------------------------------------------------------------
``docs/EMITTER_GUIDE.md`` records (measured 2026-09-03) that ``billing:events``
has **two** producers with different identity guarantees — fr-backend at ~99.5%
of rows via a globally-installed ``withUsageSpans``, and license-authz — and the
canonical field table in that guide documents the ``billing:costs`` shape, not
this one. So "every metering row carries a usable wire ``timestamp``" is an
ASSUMPTION here, in the same way it was an assumption for two of the twelve cost
emitters. ``coalesce(event_ts, created_at)`` keeps a row with no usable event
time billing exactly where it bills today; it must never drop out of the rollup.

NULLABLE, AND NULL IS LOAD-BEARING
----------------------------------
NULL means "no usable producer event time", and readers fall back to
``created_at``. ``services.rollup_engine`` resolves both clocks through ONE
factory (``_resolved_event_time``) that produces both ``cost_event_time`` and
``metering_event_time``, so the two buses cannot be edited apart again — which is
the whole content of this defect.

⛔ FORWARD-ONLY. NO BACKFILL. NOT NOW, NOT LATER.
-------------------------------------------------
Every row written before this migration had its wire ``timestamp`` dropped on the
floor at ingest; it exists nowhere in this database. There is nothing to backfill
FROM. ``event_ts = created_at`` for history would not be a backfill — it would
restate ingest time as event time under a column whose entire purpose is to be
the other clock.

⭐ AND THAT IS ALSO WHAT MAKES THIS SAFE FOR CLOSED PERIODS. Because every
pre-0034 row has ``event_ts IS NULL``, ``coalesce(event_ts, created_at)``
reproduces the OLD bucket bit-for-bit for 100% of existing rows. No historical
hour, day, month or invoice can move as a result of this change. Only rows
written after the flusher starts populating the column are bucketed on producer
time, and such a row is bucketed correctly on its FIRST roll — it is never a
restatement of a number that was already published.

⚠️ THIS TABLE IS NOT PARTITIONED — AND THAT IS THE DIFFERENCE FROM 0028
------------------------------------------------------------------------
``billing_cost_events`` is monthly RANGE-partitioned on ``created_at``, so 0028
had to reason about parent-vs-partition DDL cascade. ``billing_metering_events``
is **``relkind = 'r'``, an ordinary table** — measured and recorded in alembic
0023's docstring ("``billing_metering_events`` is ``relkind = 'r'`` — an ordinary
table, not partitioned — so this control attaches to exactly one relation"), and
corroborated by ``rollup_engine.cleanup_old_events`` ("``metering_events`` is a
plain table and stays on DELETE") and by ``drop_old_cost_partitions``, which
discovers partitions of ``billing_cost_events`` only. So the two statements below
target one relation, there is no cascade to reason about, and no per-partition
DDL is issued or needed. ⛔ Do not copy 0028's partition wording onto this file;
it would assert a property this relation does not have.

SAFE AGAINST A LIVE TABLE
-------------------------
``ADD COLUMN`` of a NULLABLE column with NO default is a catalogue-only change:
no table rewrite, no row touched, no backfill. The index is PARTIAL (``WHERE
event_ts IS NOT NULL``) for the same reason ``ix_costs_event_ts`` is — the column
is NULL for 100% of existing rows and is never backfilled — so it is built over
an empty predicate set at apply time and takes a trivial lock. No trigger on this
relation is dropped, disabled or recreated: ``billing_delete_audit_trg``,
``billing_truncate_audit_trg``, ``billing_truncate_guard_trg`` (0018) and
``billing_unrolled_metering_truncate_guard_trg`` (0023) are all untouched.

⚠️ WHAT THIS DOES TO 0023's STATE GATE, STATED RATHER THAN DISCOVERED LATER.
That gate refuses a TRUNCATE while raw rows exist at or after
``least(hourly_end, daily_end)``, comparing on ``created_at``. Once buckets come
from event time, a row can be rolled while its ``created_at`` still sits past
that cutoff, so the gate will occasionally refuse over a row that IS already
aggregated. That is an OVER-refusal — the direction 0023 explicitly chose ("a
deliberate over-refusal … the correct direction of error for a control whose
failure mode is permanent revenue loss") — so the gate is left exactly as it is.

⛔ THE PRODUCER HALF IS NOT IN THIS CHANGE, AND UNTIL IT LANDS THIS COLUMN IS
   NULL ON EVERY NEW ROW
------------------------------------------------------------------------------
Say this plainly rather than let the rest of this file imply otherwise: as of
this migration NOTHING WRITES ``event_ts``. ``services/event_flusher.py`` builds
the raw-event row dict and is owned by a different change in flight; it does not
yet read the wire ``timestamp``. So on a database with this migration applied and
the current flusher, every metering row still has ``event_ts IS NULL`` and
``coalesce(event_ts, created_at)`` bills it EXACTLY where it bills today. The
schema and rollup half are correct and inert; the defect is not closed until the
flusher writes the column. The change needed there is one line in the ``row``
dict built in ``flush_events``::

    "event_ts": parse_event_ts(e.get("timestamp"), received_at=now),

reusing ``services._parsers.parse_event_ts`` — the SAME helper ``cost_flusher``
already calls — with ``now`` captured once before the batch loop, exactly as
``cost_flusher`` does it. Do not re-implement the rule; the two flushers
disagreeing about what a usable event time is would be a new defect.

⚠️ DEPLOY ORDER, AND WHY THIS ONE ALSO GOES IN ADDITIVE_MIGRATIONS
--------------------------------------------------------------------
Once that flusher line lands it NAMES this column on the raw-event INSERT, so a
pod running the new flusher against a database where this migration has not run
would fail EVERY insert with ``UndefinedColumn`` — a full metering-ingest outage,
not a degradation. 0028 handled the same hazard by requiring
migration-before-image. Here the schema half and the flusher half are authored
separately and can be deployed in either order, so the (nullable, index-only)
mirror is ALSO added to ``src/db.py``'s ``ADDITIVE_MIGRATIONS`` — the escape
hatch 0028's own docstring offered — so neither ordering can take ingest down.

Revision ID: 0034_metering_event_event_time
Revises: 0033_invoice_app_request_money
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0034_metering_event_event_time"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0033_invoice_app_request_money"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Mirrors models/db/events.py MeteringEvent.event_ts — same type and nullability
# as CostEvent.event_ts (0028). Kept as raw SQL, matching 0028's shape, so the
# statement replayed by src/db.py ADDITIVE_MIGRATIONS is textually the same one.
_ADD_COLUMN = "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS event_ts TIMESTAMPTZ"
_DROP_COLUMN = "ALTER TABLE billing_metering_events DROP COLUMN IF EXISTS event_ts"

# Mirrors models/db/events.py MeteringEvent.__table_args__ ix_events_event_ts.
#
# ⚠️ Be honest about what this index does and does not serve. It serves a direct
# range scan on event time (an audit, a restatement by event time). It does NOT
# accelerate rollup_hourly's resolved predicate: a btree on one column cannot
# answer a COALESCE over two. What keeps that query cheap is the sargable LOWER
# bound on `created_at` and ix_events_created.
_CREATE_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_events_event_ts "
    "ON billing_metering_events (event_ts) WHERE event_ts IS NOT NULL"
)
_DROP_INDEX = "DROP INDEX IF EXISTS ix_events_event_ts"


def upgrade() -> None:
    op.execute(_ADD_COLUMN)
    # Order matters: the column must exist before an index can reference it.
    op.execute(_CREATE_INDEX)
    # ⛔ NO BACKFILL STATEMENT, DELIBERATELY. See the module docstring: the true
    # event time of every existing row was discarded at ingest and exists nowhere,
    # so there is nothing to backfill from, and `event_ts = created_at` would
    # restate ingest time as event time. It would ALSO silently move historical
    # hourly buckets — the one thing this migration guarantees it cannot do. A
    # future edit that adds an UPDATE here is a correctness regression, not a
    # completeness improvement.


def downgrade() -> None:
    # ⚠️ Dropping this column destroys every producer-asserted metering event time
    # written since the upgrade — that data has no other copy in this database
    # (the Redis stream is COUNT-trimmed), so re-upgrading restores the column
    # EMPTY and every affected row silently reverts to being counted on its ingest
    # hour. Reversible as schema, lossy as data. Same trade 0028 made, for the
    # same reason: the alternative is parking a wide timestamptz column nothing
    # reads.
    op.execute(_DROP_INDEX)
    op.execute(_DROP_COLUMN)
