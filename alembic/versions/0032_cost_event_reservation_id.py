"""billing_cost_events.reservation_id — a settlement may only release its OWN reservation.

THE DEFECT, MEASURED
--------------------
Budget reservations are currently anonymous: a pre-flight reserves headroom, and
settlement releases *a* reservation rather than *the* reservation it reserved.
With no handle tying a release back to the reservation that authorised the spend,
the budget cap fails OPEN — measured at **151.6 over-releases/hour**
(billing-service#178). An over-release returns headroom that was never held, so
the cap lets through spend it was installed to refuse. A cap that can be talked
into returning headroom it never issued is not a cap.

WHAT THE COLUMN IS
------------------
A per-reservation handle, minted by BILLING when it grants the reservation,
carried by ``inference-router`` on the cost event it later emits, and consumed at
SETTLEMENT so the release can be matched against exactly one reservation. The
round trip is mint → wire → event row → settle, which is why the handle has to
survive on the event row: settlement reads the event, not the pre-flight call.

NULLABLE, AND NULL IS THE CORRECT PERMANENT VALUE
-------------------------------------------------
⛔ THERE IS NO BACKFILL, AND NONE IS POSSIBLE OR WANTED. NULL is not a gap to be
filled later — it is the right, permanent answer for any event that was never
pre-flighted through a reservation, which is **roughly 90% of debits, by design**:
most spend is metered after the fact and never passes through a reservation at
all. Two consequences follow and both are deliberate:

* a NULL ``reservation_id`` must never be read as "reservation lost" or "data
  missing"; it means "this debit had no reservation", and settlement has nothing
  to release for it;
* every row written before this migration is NULL forever. Pre-0032 events were
  emitted by producers that did not yet carry the handle, so there is nothing to
  backfill FROM — inventing one would fabricate an authorisation that never
  existed, which is strictly worse than the fail-open this migration closes.

Any future edit that adds an ``UPDATE`` here is a correctness regression, not a
completeness improvement.

NO INDEX, DELIBERATELY
----------------------
The column is written on INSERT and read only in-process, during the same
settlement batch that wrote it — it is never the sole predicate of a query. An
index would be paid for on every cost INSERT (the hot ingest path) and serve
nothing. ⚠️ Note this is a DIFFERENT judgement from 0028's ``event_ts``, which
DID get a partial index because it serves direct range scans (a reprice window,
an audit). Do not add one here by analogy.

SAFE AGAINST A LIVE, PARTITIONED TABLE
--------------------------------------
``billing_cost_events`` is RANGE-partitioned on ``created_at`` with 12 live
monthly partitions. Quoting 0028's rule and its reason so the next reader does
not have to re-derive it: ``ADD COLUMN`` of a NULLABLE column with NO default is
a CATALOGUE-ONLY change — no table rewrite, no row touched, no backfill — and on
a RANGE-partitioned parent it cascades to every existing partition and is
inherited by every future one (``create_cost_events_partition_for`` mints
partitions with ``CREATE TABLE … PARTITION OF``). So every statement here
targets the PARENT ONLY; no per-partition DDL is issued, and the partition
function is unchanged. Same shape alembic 0021 and 0028 established for this
table. No trigger on the parent or any partition is dropped, disabled or
recreated.

⚠️ DEPLOY ORDER / COMPANION EDIT — ``src/db.py``
-----------------------------------------------
The flusher NAMES ``reservation_id`` in its cost INSERT, so a pod carrying the
new code against a database still on pre-0032 would fail EVERY cost INSERT with
``UndefinedColumn`` — a full billing-ingest outage, not a degradation. That is
what ``db.ADDITIVE_MIGRATIONS`` is the safety net for (see its ``event_ts``
block: "⛔ THE OMISSION HERE IS AN INGEST OUTAGE, NOT A COSMETIC GAP"). This
migration's ``_ADD_COLUMN`` string is mirrored BYTE-IDENTICALLY there, beside the
``event_ts`` entry; a boot-replayed schema that differs from the migrated one is
how the two drift apart silently.

Revision ID: 0032_cost_event_reservation
Revises: 0031_cost_daily_unpriced_visible
Create Date: 2026-09-12
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0032_cost_event_reservation"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0031_cost_daily_unpriced_visible"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Mirrors models/db/events.py CostEvent.reservation_id, and mirrored
# byte-identically in src/db.py ADDITIVE_MIGRATIONS. PARENT ONLY — see the
# module docstring for why no per-partition DDL is issued.
_ADD_COLUMN = "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS reservation_id UUID"
_DROP_COLUMN = "ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS reservation_id"


def upgrade() -> None:
    op.execute(_ADD_COLUMN)
    # ⛔ NO BACKFILL STATEMENT, DELIBERATELY, AND NO INDEX. See the module
    # docstring: NULL is the correct permanent value for the ~90% of debits that
    # were never pre-flighted through a reservation, and for every pre-0032 row
    # there is nothing to backfill from.


def downgrade() -> None:
    # ⚠️ Dropping this column destroys every reservation handle written since the
    # upgrade, and those handles have no other copy in this database (the Redis
    # cost stream is COUNT-trimmed). Re-upgrading restores the column EMPTY, so
    # every in-flight reservation becomes unmatchable at settlement and the cap
    # reverts to failing open. Reversible as schema, lossy as data — intentional,
    # the alternative being a column nothing reads.
    op.execute(_DROP_COLUMN)
