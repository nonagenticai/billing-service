"""billing_cost_events.event_ts — bill usage on EVENT time, not ingest time.

THE DEFECT, MEASURED
--------------------
``billing_cost_daily.bucket_date`` was derived from ``billing_cost_events.created_at``,
which is stamped at INGEST (``default=_utcnow`` at INSERT, server ``now()`` on the
partition's own default). So the calendar day a cost is billed on was the day
billing happened to *receive* the message, not the day the work happened. When a
flush crosses midnight the usage lands on the wrong day; when it crosses a MONTH
boundary it lands on the wrong INVOICE. Normally the lag is seconds. An observed
consumer stall made it 62 HOURS.

``created_at`` could never have been the right clock for this, and the codebase
already said so in two places before this migration: ``models/db/events.py`` on
``event_key`` ("``created_at`` is ingest-time and unstable across redelivery") and
``ProcessedCostMessage``'s docstring ("ingest ``now()`` and not stable across
redelivery"). A number that changes when a message is redelivered cannot be the
basis of a financial period.

THE DATA WAS ALREADY ON THE WIRE
--------------------------------
Nothing new has to be emitted for this to work. Every measured emitter already
sends a ``timestamp`` field taken at emit time —
``inference-router/src/services/metering.py``, ``w2v_common/metering/cost_bus.py``,
``kag_common/metering/__init__.py``, all ``datetime.now(UTC).isoformat()`` — and
Redis ``XADD`` on stream ``billing:costs`` is the ONLY ingest path (there is no
HTTP cost-event endpoint). ``services.consumer._stage_message`` appends the whole
raw field dict, so the value reached the flusher intact and was then simply never
read into a column. This migration gives it somewhere to land.

WHAT event_ts MEANS, AND WHAT IT DOES NOT
-----------------------------------------
It is the producer's ``now()`` at the moment it FINISHED metering the call — i.e.
END of request. ⛔ It is NOT request-start time and must never be documented,
graphed or billed as such: for a 40-minute video compile it sits 40 minutes after
the work began. For a BUCKET DATE that is good enough and it is strictly better
than ``created_at`` for the one property that matters here — it is STABLE ACROSS
REDELIVERY, which ``created_at`` explicitly is not.

NULLABLE, AND NULL IS LOAD-BEARING
----------------------------------
NULL means "no usable producer event time", and readers fall back to
``created_at`` for such a row. ``services.rollup_engine`` resolves both clocks
through ONE shared expression (``cost_event_time`` = ``coalesce(event_ts,
created_at)``) used by the bucket-date cast, by the guarded-delete coverage gate
and by the gap detector, so the gate can never answer a question about a
different row set than the one it gates. The fallback is COUNTED per
``service_key``: ⚠️ only ten of the twelve live emitting ``service_key``s were
confirmed to use the shared emitter library — ``w2v-video-analyzer`` and
``w2v-video-compiler`` (the estate's #2 spender) could not be — so "every row has
an event_ts" is an assumption until that counter reads zero.

⛔ FORWARD-ONLY. NO BACKFILL. NOT NOW, NOT LATER.
-------------------------------------------------
Every row written before this migration has had its true event time DISCARDED at
ingest: the wire ``timestamp`` was dropped on the floor and exists nowhere in this
database. There is therefore nothing to backfill FROM. Setting ``event_ts =
created_at`` for history would not be a backfill — it would restate ingest time as
event time under a column whose whole purpose is to be the other clock, and it
would make the fallback counter read zero while measuring nothing. Every
pre-0028 row stays NULL forever and is billed, exactly as it is billed today, on
its ingest day. Re-deriving history would need an external source (the producers'
own logs), which this migration does not have and does not pretend to.

⚠️ NOT A NEW PARTITION KEY
--------------------------
``billing_cost_events`` stays RANGE-partitioned on ``created_at``, and retention
(``drop_old_cost_partitions``) still runs on it. Partitioning on a nullable column
the producer controls is not on the table. The invariant that keeps the rollup's
partition pruning correct lives on the WRITE side instead:
``services._parsers.parse_event_ts`` refuses any event time later than the instant
billing received the message, so ``event_ts <= created_at`` for every stored row —
which is what lets ``rollup_daily`` keep a sargable LOWER bound on ``created_at``
while filtering exactly on the resolved event date.

⚠️ DEPLOY ORDER MATTERS HERE
---------------------------
``services.cost_flusher`` writes this column explicitly, and ``src/db.py``'s
boot-time ``ADDITIVE_MIGRATIONS`` replay does NOT mirror it, so a pod running the
new flusher against a database where this migration has not run would fail every
INSERT with ``UndefinedColumn`` — i.e. a full billing-ingest outage, not a
degradation. Run this migration BEFORE rolling the new image, or add the
(nullable, index-only) mirror to that tuple first.

SAFE AGAINST A LIVE TABLE
-------------------------
``ADD COLUMN`` of a NULLABLE column with NO default is a catalogue-only change:
no table rewrite, no row touched, no backfill. On a RANGE-partitioned parent it
cascades to every existing partition and is inherited by every future one
(``create_cost_events_partition_for`` mints partitions with ``CREATE TABLE …
PARTITION OF``), so every statement here targets the PARENT only and no
per-partition DDL is issued — the same shape alembic 0021 established for this
table. The index is PARTIAL (``WHERE event_ts IS NOT NULL``) for the same reason
``ix_costs_actor_created`` is: the column is NULL for 100% of existing rows and is
never backfilled, so a full index would index absence. It is built over an empty
predicate set at apply time and therefore takes a trivial lock. No trigger on the
parent or any partition is dropped, disabled or recreated.

Revision ID: 0028_cost_event_event_time
Revises: 0027_markup_rule_byo
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0028_cost_event_event_time"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0027_markup_rule_byo"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Mirrors models/db/events.py CostEvent.event_ts.
_ADD_COLUMN = "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS event_ts TIMESTAMPTZ"
_DROP_COLUMN = "ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS event_ts"

# Mirrors models/db/events.py CostEvent.__table_args__ ix_costs_event_ts.
#
# ⚠️ Be honest about what this index does and does not serve. It serves a direct
# range scan on event time (a reprice window, an audit, an invoice restated by
# event time). It does NOT accelerate rollup_daily's resolved predicate:
# `cast(timezone('UTC', coalesce(event_ts, created_at)) AS date)` is not indexable
# at all, because `timezone(text, timestamptz)` is STABLE rather than IMMUTABLE —
# and a plain btree on one column could not answer a COALESCE over two anyway.
# What prunes that query is still the lower bound on the partition key.
_CREATE_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_costs_event_ts "
    "ON billing_cost_events (event_ts) WHERE event_ts IS NOT NULL"
)
_DROP_INDEX = "DROP INDEX IF EXISTS ix_costs_event_ts"


def upgrade() -> None:
    op.execute(_ADD_COLUMN)
    # Order matters: the column must exist before an index can reference it.
    op.execute(_CREATE_INDEX)
    # ⛔ NO BACKFILL STATEMENT, DELIBERATELY. See the module docstring: the true
    # event time of every existing row was discarded at ingest and exists nowhere,
    # so there is nothing to backfill from, and `event_ts = created_at` would
    # restate ingest time as event time. A future edit that adds an UPDATE here is
    # a correctness regression, not a completeness improvement.


def downgrade() -> None:
    # ⚠️ Dropping this column destroys every producer-asserted event time written
    # since the upgrade — that data has no other copy in this database (the Redis
    # stream is COUNT-trimmed), so re-upgrading restores the column EMPTY and
    # every affected row silently reverts to being billed on its ingest day.
    # Reversible as schema, lossy as data. Intentional: the alternative is
    # parking a wide timestamptz column nothing reads.
    op.execute(_DROP_INDEX)
    op.execute(_DROP_COLUMN)
