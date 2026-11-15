"""Give ``billing_metering_monthly_usage.plan_id`` the NULL_PLAN_ID sentinel default.

WHAT IS BROKEN
--------------
Request-quota enforcement reads a per-licence monthly usage counter that is
almost never written. Measured live:

* ``billing_metering_monthly_usage`` — **6 rows, 5 distinct licences**
* ``request_quota_config``           — **160 rows**

so **155 of 160 (96.9%) configured licences have no usage row at all**. With no
row, ``event_flusher._read_quota_baselines`` has nothing to seed the Redis
counter from and ``services.quota._get_db_usage`` finds nothing, so the check
falls through fail-open. A licence capped at 100k requests/month can serve 400k
and still report ``used=0, over_quota=False`` for the whole month.

The single writer is gated in ``services/event_flusher.py`` by what used to be
``if license_id and plan_id:``. The second conjunct could essentially never be
true, because **billing cannot produce a plan_id**:

* ``plan_id`` arrives ONLY off the wire (``_parse_uuid(e.get("plan_id", ""))``).
* ``request_quota_config`` — the only local licence mirror — has no ``plan_id``
  and no ``plan_code`` column. ``resolve_app`` exists because ``app_id`` IS
  mirrored; there is no ``resolve_plan`` because nothing is mirrored to resolve
  from.
* There is no Plan/LicensePlan model anywhere under ``models/``. The licence ->
  plan edge lives in the licences service and is reachable only by an outbound
  HTTP ``fetch_licenses()`` on the REPORT path, never on the flush path.

So the counter was conditioned on a field this service has no way of obtaining.

WHY A SENTINEL AND NOT A NULLABLE COLUMN
----------------------------------------
``plan_id`` is part of ``uq_monthly_usage (license_id, subject_id, plan_id,
month_start)``, which is what the flusher's ``ON CONFLICT ON CONSTRAINT``
upsert uses to INCREMENT an existing month's row.

⛔ Making the column NULLABLE would BREAK that de-duplication. A plain UNIQUE
constraint in Postgres is ``NULLS DISTINCT`` by default, so two NULL plan_ids do
NOT collide: every flush batch would INSERT a fresh row instead of incrementing,
the counter would fan out into one row per flush, and the readers — which SUM
across the plan grain — would add up duplicates. A sentinel uuid collides with
itself, so the upsert keeps working with no change at all.

The sentinel is ``NULL_PLAN_ID`` = ``00000000-0000-0000-0000-000000000002``
(``models/db/_common.py:86``). This is NOT a new convention invented here: it is
already the default on ``billing_metering_hourly.plan_id`` and
``billing_metering_daily.plan_id`` (``models/db/rollups.py:64,104``), already
what ``rollup_engine`` coalesces to when writing those grains
(``func.coalesce(MeteringEvent.plan_id, NULL_PLAN_ID)``), and structurally the
same device this very table already uses for ``subject_id``. It means "no plan
was asserted for this usage" — never a real plan, and never the free plan.

⚠️ ACCOUNTING IS NOT FLATTENED. The plan stays IN the unique key. A row that
carries a real plan_id still gets its own row, so a licence that genuinely
changes plan mid-month still splits into two rows exactly as before. Only rows
that assert NO plan are collapsed onto one another, which is correct — they are
the same unknown. Neither reader keys on plan_id anyway
(``services/quota.py::_get_db_usage`` and
``event_flusher::_read_quota_baselines`` both SUM across the plan grain,
deliberately, because ``quota:{license}:{YYYY-MM}`` carries no plan in its key),
so no aggregate's definition changes.

⛔ NO ENFORCEMENT CHANGE. No threshold moves and no fail-open direction flips
here or in the companion code change. This makes the COUNTER EXIST. Arming it is
a separate, later decision.

⛔ AND IT IS NOT SUFFICIENT BY ITSELF — READ THIS BEFORE DECLARING QUOTA FIXED.
``monthly_counts`` is assigned only in the flusher's denied branch and its
``status == "allowed"`` branch. fr writes HTTP CODES into the same ``status``
column (``200``, ``201``, ``404``, ``422``; measured 159,085 rows over 30d), and
those land in a third branch that assigns nothing — so they still produce NO
ROW, now refused by the status vocabulary rather than by the plan. Note that
``rollup_engine`` bills the daily grain as "everything that is NOT denied" and
warns in as many words against the ``status == "allowed"`` spelling the monthly
writer uses, so the two writers disagree about the same events. Closing that
changes MONEY (``services.billing.generate_billing`` takes
``max(daily_requests, monthly_usage)`` per licence), so it belongs in its own
change with its own before/after on real invoices.

SAFE ON A LIVE DATABASE
-----------------------
``ALTER COLUMN … SET DEFAULT`` is a catalogue-only change: no table rewrite, no
scan, no row-level locks held for a scan, and existing rows are untouched (a
DEFAULT applies to future INSERTs that OMIT the column, nothing else). All six
existing rows carry a real plan_id and keep it. Re-running is a no-op, which is
what makes it safe to mirror into ``db.ADDITIVE_MIGRATIONS`` — replayed on every
pod start.

The column stays NOT NULL. Nothing is dropped, so this is additive and cannot
open a window in which an object does not exist.

Revision ID: 0035_monthly_usage_plan_default
Revises: 0034_metering_event_event_time
Create Date: 2026-09-14
"""

from __future__ import annotations

from alembic import op

# <= 32 chars: alembic_version.version_num is varchar(32).
revision: str = "0035_monthly_usage_plan_default"
# ⚠️ CHAINED ONTO 0034, AND THIS FILE WAS ORIGINALLY WRITTEN AS A SECOND 0034.
# A sibling lane on this same branch (``0034_metering_event_event_time``, the
# MeteringEvent.event_ts work) landed its revision off 0033 too. When this
# migration was drafted, 0033 WAS the single head — verified, and the check was
# correct at the instant it ran and stale minutes later, because the sibling's
# file was still untracked and did not exist yet. A head check is a reading of a
# mutable tree, not a standing fact, so it has to be re-taken at the moment of
# writing the `down_revision` rather than trusted from earlier in the session.
#
# ⛔ TWO REVISIONS OFF ONE PARENT FORKS THE TREE, and `alembic upgrade head`
# refuses a multi-head chain — every pod would fail to START. It is also
# invisible to a per-PR check that only looks at its own diff: both branches are
# individually well-formed and both PRs go green. Renumbering this one (rather
# than the sibling, whose work is not this lane's to move) restores a single
# head. The two are independent — different tables, no shared object — so the
# order between them carries no meaning beyond making the chain linear.
down_revision: str | None = "0034_metering_event_event_time"
branch_labels = None
depends_on = None

#: ⛔ MIRRORED VERBATIM IN ``src/db.py``'s ``ADDITIVE_MIGRATIONS``.
#:
#: This service builds part of its schema TWICE — the alembic chain, and the
#: ``ADDITIVE_MIGRATIONS`` tuple replayed on every pod start — and migration
#: 0012 exists because objects living in only one of the two made a fresh
#: database a genuinely different schema from production. Kept as a module-level
#: tuple, rather than inline ``op.execute`` calls, so a test can assert the two
#: copies are byte-identical; substring matching would not do, since a differing
#: DEFAULT is still a superstring of the column name and that difference is the
#: whole point of this migration.
STATEMENTS: tuple[str, ...] = (
    "ALTER TABLE billing_metering_monthly_usage "
    "ALTER COLUMN plan_id SET DEFAULT '00000000-0000-0000-0000-000000000002'::uuid",
)


def upgrade() -> None:
    for stmt in STATEMENTS:
        op.execute(stmt)


def downgrade() -> None:
    # Symmetric and safe: dropping the DEFAULT restores a column with no default
    # and leaves every stored value — sentinel rows included — exactly as it
    # found them. Rows already written against the sentinel keep it; this is a
    # catalogue change, not a data change.
    op.execute("ALTER TABLE billing_metering_monthly_usage ALTER COLUMN plan_id DROP DEFAULT")
