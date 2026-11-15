"""Widen the BEFORE TRUNCATE guard from 14 relations to every billing base table.

THE GAP 0017 LEFT
-----------------
0017 attached ``billing_truncate_guard_trg`` to 14 relations:
``billing_cost_events``, ``billing_metering_events`` and the 12
``billing_cost_events_YYYY_MM`` partitions. The 2026-09-05 07:01:56.876899Z
statement named 24. Because a TRUNCATE is one statement and ANY single
BEFORE TRUNCATE trigger that raises aborts the whole of it, that incident's
exact statement would now be refused -- which is what made 0017 look complete.

It is not. A statement naming ONLY the currently-unguarded relations still
succeeds today. ⚠️ CORRECTED after audit: an earlier draft of this docstring said
"the ten" and listed ten. Running this migration's own predicate against the LIVE
prod database selects **twenty-one**::

    audit_log                      billing_app_cost_rates
    billing_cost_daily             billing_delete_audit
    billing_metering_daily         billing_metering_hourly
    billing_metering_monthly_usage billing_provider_costs
    budget_overage_alerts          markup_rules
    processed_cost_event_keys      processed_cost_messages
    processed_event_messages       provider_pricing
    request_quota_config           tenant_access_charge
    tenant_config                  tenant_id_crosswalk
    tenant_webhooks                token_budgets
    token_usage

Eleven of those are config/reference tables rather than ledgers, which is why the
under-count was easy to make and worth correcting: the enumerated list is the
part an operator reads, and it understated the blast radius by eleven tables.

That is every rollup (the aggregates the invoices are actually built from),
``token_usage``, all three ``processed_*`` dedupe-key tables -- emptying those
does not lose money directly, it makes the redelivery path re-ingest and
DOUBLE-BILL -- and ``billing_delete_audit`` itself, which is precisely how the
audit table erased its own history on 2026-09-05 (it was eleventh in that
statement's list, so the AFTER TRUNCATE trigger's own record went with it).

⛔ ATTRIBUTION: the client that issued the 2026-09-05 statement is UNIDENTIFIED.
0017's docstring blamed ``tests/conftest.py``; that is refuted on two
independent axes (no ``CASCADE``; a table set ``Base.metadata.sorted_tables``
cannot produce). The working is in ``tests/_production_tripwire.py``. The gap
this migration closes does not depend on who the client was -- it is a hole in
the control regardless.

WHY SET-BASED AND NOT A LIST OF TEN NAMES
-----------------------------------------
A hardcoded list is correct exactly once. The next table added to this schema
would be born unguarded, and nothing would say so -- the same failure mode that
put ten tables outside 0017's list in the first place. This migration asks the
catalog which base tables in ``public`` do NOT already carry the trigger and
attaches it to those. Re-running it after adding a table re-closes the hole.

That is also why the loop is ATTACH-IF-ABSENT rather than 0017's
``DROP TRIGGER IF EXISTS`` + ``CREATE``: the 14 relations 0017 armed are skipped
by the ``NOT EXISTS`` predicate and are never dropped and rebuilt, so this
migration cannot open a window -- however brief, and inside a transaction it
would be invisible -- in which an already-guarded ledger is unguarded.

WHAT IS DELIBERATELY EXCLUDED
-----------------------------
* ``alembic_version`` -- alembic's own bookkeeping, not billing data, and
  excluded from every other schema sweep in this repo (see
  ``scripts/validate_migrations.py``). Guarding it would put a billing control
  in the path of migration tooling for no billing benefit.
* Extension-owned tables (``pg_depend.deptype = 'e'``) -- not ours to trigger.
* Anything not ``relpersistence = 'p'`` -- a TEMP or UNLOGGED table is
  per-session scratch, never a ledger.

Partitions need no special case here, which is the point of asking the catalog:
a partition is an ordinary ``relkind = 'r'`` row in ``pg_class`` and is picked
up like any other table. The 12 existing ones already carry the trigger from
0017 and are skipped; a partition minted LATER gets its guard from
``create_cost_events_partition_for``, which 0017 already rewrote -- this
migration does not touch that function, so 0016's audit half is untouched too.

THE ESCAPE HATCH IS UNCHANGED
-----------------------------
The guard function ``public.billing_forbid_truncate()`` is NOT redefined here.
Its only predicate remains ``current_setting('billing.allow_truncate')``, and
legitimate truncation still works the same way::

    SET LOCAL billing.allow_truncate = 'on';
    TRUNCATE billing_metering_hourly;

VERIFIED against the test suite before widening: ``tests/conftest.py``'s
``db_engine`` fixture arms the hatch (``SET LOCAL``) in the same transaction as
its per-test ``TRUNCATE``, and it truncates the whole of
``Base.metadata.sorted_tables`` -- so it already covers the relations this
migration arms, and covering them cannot break it. Had it NOT armed the hatch,
widening the guard would have broken every DB test and this migration would be
the wrong fix.

⚠️ ONE EXCEPTION, found by audit: ``tenant_id_crosswalk`` is migration-only
bookkeeping and is NOT in ``Base.metadata``, so conftest's truncate does not name
it and the "already covered by the suite" argument does NOT extend to it. Nothing
truncates it today -- so this is a documentation limit, not a defect -- but if a
future caller needs to truncate it, the hatch must be armed explicitly rather
than inherited from the suite's blanket coverage.

WHY THIS IS SAFE ON A LIVE DATABASE
-----------------------------------
Purely additive: it creates triggers and nothing else. No function is replaced,
no column altered, no row touched. Re-running is a no-op because the
``NOT EXISTS`` predicate finds nothing left to attach. A BEFORE TRUNCATE trigger
fires on TRUNCATE and on nothing else, and billing does not truncate in normal
operation, so the ingest hot path cannot notice this migration exists.

LOCK DISCIPLINE
---------------
Identical to 0016 and 0017, and it matters MORE here because the relation count
is larger and includes the rollup tables the flushers write to. ``CREATE
TRIGGER`` takes ``ShareRowExclusiveLock``, which conflicts with the
``RowExclusiveLock`` every ``INSERT`` holds, and the lock queue is FIFO: a
blocked DDL puts every subsequent INSERT behind itself. All relations are armed
in ONE transaction under a bounded ``lock_timeout``, so this aborts and rolls
back cleanly rather than stalling billing ingest. Deliberately not split: a
partially applied widening is a control with holes whose location nobody knows.

⛔ NOT APPLIED TO PROD BY THIS PR.

Revision ID: 0018_truncate_guard_widen
Revises: 0017_truncate_guard
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy.exc import DBAPIError

revision: str = "0018_truncate_guard_widen"
down_revision: str | None = "0017_truncate_guard"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ⛔ The trigger name and the schema are written as LITERALS inside the DDL
# below, never as Python constants interpolated into it. 0017 does the same, and
# it matters here for a reason beyond style: the ``NOT EXISTS`` predicate that
# makes this migration idempotent and non-destructive only recognises the 14
# relations 0017 armed if the name matches 0017's EXACTLY. A constant that could
# be edited on one side is a way for those two spellings to drift apart, and the
# failure would be silent -- a second trigger on every already-guarded ledger.


#: Fail loudly if 0017's function is missing rather than emitting ten
#: ``CREATE TRIGGER``s that each fail with a bare "function does not exist".
#: This can only happen if 0017 was downgraded without downgrading this one.
REQUIRE_GUARD_FUNCTION_DDL = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NULL THEN
        RAISE EXCEPTION
            'alembic 0018 requires public.billing_forbid_truncate() from 0017'
            USING HINT =
                'The guard function is missing, so there is nothing to attach. '
                'Run alembic 0017 first, or downgrade past 0018 as well.';
    END IF;
END
$do$;
"""


#: The widening itself. Catalog-driven ON PURPOSE -- see the docstring.
ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL = r"""
DO $do$
DECLARE
    rel   text;
    armed integer := 0;
BEGIN
    FOR rel IN
        SELECT c.relname
          FROM pg_class c
          JOIN pg_namespace ns ON ns.oid = c.relnamespace
         WHERE ns.nspname = 'public'
           -- 'r' ordinary table (this is what a partition is, too) and
           -- 'p' partitioned parent. Views, matviews, sequences, indexes and
           -- foreign tables cannot be TRUNCATEd and are not candidates.
           AND c.relkind IN ('r', 'p')
           -- Permanent only: TEMP/UNLOGGED relations are per-session scratch.
           AND c.relpersistence = 'p'
           -- Alembic's own bookkeeping is not billing data.
           AND c.relname <> 'alembic_version'
           -- Extension-owned tables are not ours to put triggers on.
           AND NOT EXISTS (
                   SELECT 1
                     FROM pg_depend d
                    WHERE d.classid = to_regclass('pg_class')
                      AND d.objid = c.oid
                      AND d.deptype = 'e')
           -- ⛔ The idempotency predicate AND the promise not to disturb the 14
           -- relations 0017 armed. Attach only where the trigger is ABSENT;
           -- never drop and recreate one that already exists.
           AND NOT EXISTS (
                   SELECT 1
                     FROM pg_trigger t
                    WHERE t.tgrelid = c.oid
                      AND t.tgname = 'billing_truncate_guard_trg')
         ORDER BY c.relname
    LOOP
        EXECUTE format(
            'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate()',
            rel);
        armed := armed + 1;
        RAISE NOTICE 'billing_truncate_guard_trg -> public.%', rel;
    END LOOP;

    RAISE NOTICE 'alembic 0018: armed % newly-guarded relation(s)', armed;
END
$do$;
"""


#: Back out to EXACTLY 0017's coverage: the cost-events parent, the metering
#: events table, and every partition of the cost-events parent. Everything else
#: loses the trigger. Expressed as a set difference for the same reason the
#: upgrade is set-based -- a hardcoded list of ten would strand any relation
#: armed by a re-run after a new table was added.
DETACH_GUARD_FROM_WIDENED_TABLES_DDL = r"""
DO $do$
DECLARE
    rel text;
BEGIN
    FOR rel IN
        SELECT c.relname
          FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          JOIN pg_namespace ns ON ns.oid = c.relnamespace
         WHERE t.tgname = 'billing_truncate_guard_trg'
           AND ns.nspname = 'public'
           AND c.relname NOT IN ('billing_cost_events', 'billing_metering_events')
           AND NOT EXISTS (
                   SELECT 1
                     FROM pg_inherits i
                    WHERE i.inhrelid = c.oid
                      AND i.inhparent = to_regclass('public.billing_cost_events'))
         GROUP BY c.relname
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.%I', rel);
    END LOOP;
END
$do$;
"""


# --- lock discipline (see "LOCK DISCIPLINE" in the module docstring) --------

#: 3 seconds, the same bound 0016 and 0017 chose against the same hot path.
#: Seconds, never minutes: while this DDL waits, every new INSERT queues behind
#: it.
LOCK_TIMEOUT = "3s"

#: Catalog-only work once the locks are held; 60s cannot be hit by healthy work.
STATEMENT_TIMEOUT = "60s"

#: 55P03 lock_not_available / 57014 query_canceled.
_TIMEOUT_SQLSTATES = frozenset({"55P03", "57014"})

_RETRY_IN_A_QUIETER_WINDOW = f"""
alembic 0018 could not acquire its DDL locks within {LOCK_TIMEOUT}. The whole
migration has ROLLED BACK - nothing was applied, and it is safe to re-run.

Something held a conflicting lock on one of the billing rollup, dedupe or audit
tables. Find it with:

    SELECT pid, state, xact_start, wait_event_type, left(query, 120)
      FROM pg_stat_activity
     WHERE datname = current_database() AND xact_start IS NOT NULL
     ORDER BY xact_start;

RETRY IN A QUIETER WINDOW (or after that transaction ends).

Do NOT raise LOCK_TIMEOUT to get past this. CREATE TRIGGER takes
ShareRowExclusiveLock, which conflicts with the RowExclusiveLock every INSERT
holds, and the lock queue is FIFO: while this migration waits, every NEW insert
into the billing rollups queues behind it. A longer timeout is a longer billing
ingest stall, not a better chance of success.
""".strip()


def _bound_the_wait() -> None:
    """Bound this migration's lock wait, and prove the bound actually took.

    ``SET LOCAL`` outside a transaction block is a WARNING and a no-op, which
    would leave this guard vacuous. Read back rather than trusted -- same
    reasoning as 0016 and 0017.
    """
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0018 refuses to run without a bounded lock_timeout: SET LOCAL "
            f"reported lock_timeout={effective!r}, which means this is not running "
            "inside a transaction block and the DDL below could queue the billing "
            "INSERT path behind itself indefinitely."
        )


def _fail_fast(fn):  # noqa: ANN001, ANN202
    """Turn a lock/statement timeout into an instruction the operator can act on."""
    try:
        fn()
    except DBAPIError as exc:
        sqlstate = getattr(exc.orig, "sqlstate", None)
        if sqlstate in _TIMEOUT_SQLSTATES:
            raise RuntimeError(_RETRY_IN_A_QUIETER_WINDOW) from exc
        raise


def upgrade() -> None:
    _bound_the_wait()
    _fail_fast(_upgrade_ddl)


def _upgrade_ddl() -> None:
    op.execute(REQUIRE_GUARD_FUNCTION_DDL)
    op.execute(ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL)


def downgrade() -> None:
    # DROP TRIGGER takes the same ShareRowExclusiveLock on the same relations,
    # so the downgrade can stall ingest exactly the way the upgrade can.
    _bound_the_wait()
    _fail_fast(_downgrade_ddl)


def _downgrade_ddl() -> None:
    # The guard FUNCTION is left in place: it belongs to 0017, and 0017's own
    # downgrade is what removes it. Dropping it here would break the 14
    # relations this downgrade deliberately leaves armed.
    op.execute(DETACH_GUARD_FROM_WIDENED_TABLES_DDL)
