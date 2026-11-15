"""Gate a TRUNCATE of the cost ledger on the rollup watermark (2026-09-02..09-04 loss).

WHAT THIS CLOSES
----------------
``billing_cost_daily`` holds zero rows for 2026-09-02, 09-03 and 09-04. The
rollup ran on all three days and never reached those buckets: UUIDv7 ids decode
write times and show every run writing exactly ONE bucket_date, one day per run,
permanently ~3 days behind (09-01 21:15 -> bucket 08-29 ... 09-04 23:14 -> bucket
09-01). At 2026-09-05T07:01:56Z a TRUNCATE emptied ``billing_cost_events`` and
all 12 partitions while the last rolled bucket was 09-01 -- it deleted exactly
the un-rolled backlog. ``min(created_at)`` in that table is now 09-05T07:01:58Z,
two seconds after. NOTHING IS RECOVERABLE, and this migration does not attempt
recovery: no surviving table witnesses that window, so a backfill would invent
numbers.

0017/0018 gate on INTENT (``billing.allow_truncate``). This gates on STATE: an
ARMED truncate of ``billing_cost_events`` is still refused while raw events exist
newer than ``max(bucket_date)`` in ``billing_cost_daily``. Full reasoning --
including why the gate is a trigger rather than application code, and why the
override is a second distinct GUC -- is in ``src/models/db/unrolled_guard.py``.

ESCAPE HATCH (explicit and audited, never the default path)::

    SET LOCAL billing.allow_truncate = 'on';
    SET LOCAL billing.allow_unrolled_truncate = 'on';
    SET LOCAL billing.delete_reason = '<why>';
    TRUNCATE billing_cost_events;

Taking it writes a ``TRUNCATE_UNROLLED_OVERRIDE`` row into
``billing_delete_audit`` carrying the watermark, the un-aggregated row count and
the created_at window about to be destroyed.

NOT FIXED HERE: the 3-day rollup LAG itself, which lives in
``src/services/rollup_engine.py`` / ``src/models/db/rollups.py`` and is a
separately-tracked follow-up. This makes the lag non-destructive; it does not
make it stop.

WHY THIS IS SAFE ON A LIVE DATABASE
-----------------------------------
Additive: one new function, one trigger per cost-events relation, and a
CREATE OR REPLACE of ``create_cost_events_partition_for`` whose body is DERIVED
from 0017's (so 0016's audit triggers and 0017's guard attachment are carried
forward byte-for-byte, not re-typed). No row is read or written, no column
altered. A BEFORE TRUNCATE trigger fires on TRUNCATE and nothing else, and
billing does not truncate in normal operation, so the ingest hot path cannot
notice this migration exists.

LOCK DISCIPLINE
---------------
Identical to 0016/0017/0018. CREATE TRIGGER takes ShareRowExclusiveLock, which
conflicts with the RowExclusiveLock every INSERT holds, and the lock queue is
FIFO -- a blocked DDL puts every subsequent INSERT behind itself. Bounded
lock_timeout, one transaction, read back to prove the bound took.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy.exc import DBAPIError

revision: str = "0019_unrolled_truncate_gate"
down_revision: str | None = "0018_truncate_guard_widen"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# FROZEN COPIES of src/models/db/unrolled_guard.py, duplicated rather than
# imported so this migration stays fixed in time. tests/test_unrolled_truncate_
# gate.py asserts byte identity, which is the only thing that keeps the two
# copies from drifting -- 0012 and 0015 exist solely because this repo once let
# two schema sources drift.
# ---------------------------------------------------------------------------

UNROLLED_GUARD_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_unrolled_truncate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
DECLARE
    v_watermark date;
    v_cutoff    timestamptz;
    v_unrolled  boolean;
    v_rows      bigint;
    v_min       timestamptz;
    v_max       timestamptz;
BEGIN
    -- to_regclass, not a cast: this function is also installed by the metadata
    -- after_create hook, where the rollup table may not exist yet. A missing
    -- watermark table is treated as "nothing has been rolled up", which is the
    -- fail-CLOSED reading -- an unreadable watermark is not evidence of safety.
    IF to_regclass('public.billing_cost_daily') IS NULL THEN
        v_watermark := NULL;
    ELSE
        SELECT max(bucket_date) INTO v_watermark FROM public.billing_cost_daily;
    END IF;

    IF v_watermark IS NULL THEN
        -- No bucket has ever been written: every raw row is un-aggregated.
        v_cutoff := '-infinity'::timestamptz;
    ELSE
        -- Start of the day AFTER the last rolled bucket. Equivalent to
        -- cast(timezone('UTC', created_at) as date) > v_watermark -- which is
        -- exactly how rollup_engine.rollup_daily assigns bucket_date -- but as a
        -- sargable range predicate, so the healthy path is an index probe.
        v_cutoff := (v_watermark + 1)::timestamp AT TIME ZONE 'UTC';
    END IF;

    -- Probe the relation the trigger actually fired on: on the parent this
    -- scans every partition, on a child only that child. A TRUNCATE aimed at a
    -- single partition is judged on that partition's own rows -- which is the
    -- shape the 2026-09-05 statement had.
    EXECUTE format(
        'SELECT EXISTS (SELECT 1 FROM public.%I WHERE created_at >= $1)',
        TG_TABLE_NAME)
      INTO v_unrolled
      USING v_cutoff;

    IF NOT v_unrolled THEN
        -- The rollup has caught up with this relation. Whatever intent-level
        -- control ran before us has already had its say; this gate has no
        -- further objection. THIS ARM MATTERS: a gate that refuses everything
        -- is as broken as one that refuses nothing.
        RETURN NULL;
    END IF;

    -- From here on a scan is affordable: either we abort the statement, or the
    -- caller is about to destroy the table anyway.
    EXECUTE format(
        'SELECT count(*), min(created_at), max(created_at) '
        'FROM public.%I WHERE created_at >= $1',
        TG_TABLE_NAME)
      INTO v_rows, v_min, v_max
      USING v_cutoff;

    IF coalesce(current_setting('billing.allow_unrolled_truncate', true), '')
       <> 'on' THEN
        RAISE EXCEPTION
            'TRUNCATE of %.% would delete % un-aggregated cost event(s)',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, v_rows
            USING
                ERRCODE = 'P0001',
                DETAIL = format(
                    'billing_cost_daily watermark is %s; %s row(s) with created_at in '
                    '[%s, %s] have not been rolled up into any daily bucket. Deleting '
                    'them destroys revenue that no other table witnesses.',
                    coalesce(v_watermark::text, '(no bucket has ever been written)'),
                    v_rows, coalesce(v_min::text, '-'), coalesce(v_max::text, '-')),
                HINT =
                    'On 2026-09-05 a truncate removed exactly this backlog and NOTHING '
                    'was recoverable. Run the rollup until max(bucket_date) covers these '
                    'rows, then retry. If you MEAN to delete un-aggregated revenue, say '
                    'so explicitly in the same transaction: '
                    'SET LOCAL billing.allow_unrolled_truncate = ''on''; '
                    'SET LOCAL billing.delete_reason = ''<why>''; -- then TRUNCATE. '
                    'That path is audited into billing_delete_audit as '
                    'TRUNCATE_UNROLLED_OVERRIDE.';
    END IF;

    -- Override taken. Record it BEFORE the rows go, with the numbers that will
    -- otherwise be unreconstructable -- the 2026-09-05 loss took UUIDv7 decoding
    -- to characterise because no row like this existed.
    INSERT INTO public.billing_delete_audit (
        table_name, rows_deleted, operation, reason, db_user,
        session_user_name, application_name, client_addr, backend_pid,
        txid, statement, created_at_min, created_at_max
    ) VALUES (
        TG_TABLE_NAME,
        -- The UN-AGGREGATED count, not the total the statement removes. 0016's
        -- AFTER TRUNCATE row carries the statement; this row carries the loss.
        v_rows,
        'TRUNCATE_UNROLLED_OVERRIDE',
        format(
            'unrolled-truncate override: %s row(s) after watermark %s. operator reason: %s',
            v_rows,
            coalesce(v_watermark::text, '(none)'),
            coalesce(nullif(current_setting('billing.delete_reason', true), ''),
                     'unspecified')),
        current_user, session_user,
        nullif(current_setting('application_name', true), ''),
        inet_client_addr(), pg_backend_pid(),
        pg_current_xact_id_if_assigned()::text::bigint,
        left(current_query(), 8000),
        v_min, v_max
    );

    RETURN NULL;
END
$fn$;
"""

GATE_ATTACH_DDL = r"""
DROP TRIGGER IF EXISTS billing_unrolled_truncate_guard_trg ON public.billing_cost_events;
CREATE TRIGGER billing_unrolled_truncate_guard_trg
    BEFORE TRUNCATE ON public.billing_cost_events
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_unrolled_truncate();
"""

ATTACH_GATE_TO_EXISTING_PARTITIONS_DDL = r"""
DO $do$
DECLARE
    v_parent oid;
    part     text;
BEGIN
    v_parent := to_regclass('public.billing_cost_events');
    IF v_parent IS NULL THEN
        RETURN;
    END IF;
    FOR part IN
        SELECT c.relname
          FROM pg_inherits i
          JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent = v_parent
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS billing_unrolled_truncate_guard_trg ON public.%I',
            part);
        EXECUTE format(
            'CREATE TRIGGER billing_unrolled_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_unrolled_truncate()',
            part);
    END LOOP;
END
$do$;
"""

PARTITION_FN_WITH_GATE_DDL = r"""
CREATE OR REPLACE FUNCTION public.create_cost_events_partition_for(p_year integer, p_month integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
        DECLARE
            partition_name TEXT;
            from_date TEXT;
            to_date TEXT;
        BEGIN
            partition_name := format('billing_cost_events_%s_%s',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            from_date := format('%s-%s-01',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            -- Compute the next month's start by adding 1 month then truncating.
            to_date := to_char(
                (from_date::timestamp + interval '1 month')::date,
                'YYYY-MM-01'
            );

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF billing_cost_events
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, from_date, to_date
            );

            -- Issue #92: a delete aimed at a partition does not fire the
            -- parent's statement trigger, so every minted partition carries
            -- its own. DROP IF EXISTS first keeps this idempotent under the
            -- CREATE TABLE IF NOT EXISTS above.
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_delete_audit_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_delete_audit_trg AFTER DELETE ON public.%I '
                'REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT '
                'EXECUTE FUNCTION public.billing_audit_deleted_rows()',
                partition_name);
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_truncate_audit_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_truncate_audit_trg AFTER TRUNCATE ON public.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_deleted_rows()',
                partition_name);

            -- A TRUNCATE aimed at a partition does not fire the parent's
            -- trigger, so every minted partition carries its own guard --
            -- exactly as it carries its own audit trigger above. A partition
            -- created without this is a hole in the control, in precisely the
            -- place the 2026-09-05 truncation landed.
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate()',
                partition_name);

            -- A TRUNCATE aimed at a partition does not fire the parent's
            -- trigger, so every minted partition carries the watermark gate too
            -- -- exactly as it carries the audit and intent triggers above. A
            -- partition minted without this is a hole in the control in
            -- precisely the place the 2026-09-05 truncation landed.
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_unrolled_truncate_guard_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_unrolled_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_unrolled_truncate()',
                partition_name);

            RETURN partition_name;
        END
        $function$;
"""


#: The whole control, in application order. Byte-identical to
#: ``unrolled_guard.UNROLLED_GUARD_DDL``.
UNROLLED_GUARD_DDL: tuple[str, ...] = (
    UNROLLED_GUARD_FUNCTION_DDL,
    GATE_ATTACH_DDL,
    ATTACH_GATE_TO_EXISTING_PARTITIONS_DDL,
    PARTITION_FN_WITH_GATE_DDL,
)


#: 0017's partition-minting body, for the downgrade. Restoring an INVENTED body
#: would silently drop 0016's audit triggers or 0017's guard from every FUTURE
#: partition -- a hole in a different control, opened by reverting this one, and
#: visible only months later on the next mint. Byte-identical to
#: ``truncate_guard.PARTITION_FN_WITH_GUARD_DDL``.
_FN_WITH_GUARD_ONLY = r"""
CREATE OR REPLACE FUNCTION public.create_cost_events_partition_for(p_year integer, p_month integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
        DECLARE
            partition_name TEXT;
            from_date TEXT;
            to_date TEXT;
        BEGIN
            partition_name := format('billing_cost_events_%s_%s',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            from_date := format('%s-%s-01',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            -- Compute the next month's start by adding 1 month then truncating.
            to_date := to_char(
                (from_date::timestamp + interval '1 month')::date,
                'YYYY-MM-01'
            );

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF billing_cost_events
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, from_date, to_date
            );

            -- Issue #92: a delete aimed at a partition does not fire the
            -- parent's statement trigger, so every minted partition carries
            -- its own. DROP IF EXISTS first keeps this idempotent under the
            -- CREATE TABLE IF NOT EXISTS above.
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_delete_audit_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_delete_audit_trg AFTER DELETE ON public.%I '
                'REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT '
                'EXECUTE FUNCTION public.billing_audit_deleted_rows()',
                partition_name);
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_truncate_audit_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_truncate_audit_trg AFTER TRUNCATE ON public.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_deleted_rows()',
                partition_name);

            -- A TRUNCATE aimed at a partition does not fire the parent's
            -- trigger, so every minted partition carries its own guard --
            -- exactly as it carries its own audit trigger above. A partition
            -- created without this is a hole in the control, in precisely the
            -- place the 2026-09-05 truncation landed.
            EXECUTE format(
                'DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.%I',
                partition_name);
            EXECUTE format(
                'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate()',
                partition_name);

            RETURN partition_name;
        END
        $function$;
"""


#: Fail loudly if 0016's audit table is missing: the override path INSERTs into
#: it, and a gate whose audit sink does not exist would refuse the override with
#: an unrelated "relation does not exist" that reads like a bug in the gate.
REQUIRE_AUDIT_TABLE_DDL = r"""
DO $do$
BEGIN
    IF to_regclass('public.billing_delete_audit') IS NULL THEN
        RAISE EXCEPTION
            'alembic 0019 requires public.billing_delete_audit from 0016'
            USING HINT =
                'The watermark gate audits its own override into that table. '
                'Run alembic 0016 first, or downgrade past 0019 as well.';
    END IF;
END
$do$;
"""


# --- lock discipline (see "LOCK DISCIPLINE" in the module docstring) --------

#: 3 seconds -- the same bound 0016, 0017 and 0018 chose against the same hot
#: path. Seconds, never minutes: while this DDL waits, every new INSERT into
#: billing_cost_events queues behind it.
LOCK_TIMEOUT = "3s"

#: Catalog-only work once the locks are held; 60s cannot be hit by healthy work.
STATEMENT_TIMEOUT = "60s"

#: 55P03 lock_not_available / 57014 query_canceled.
_TIMEOUT_SQLSTATES = frozenset({"55P03", "57014"})

_RETRY_IN_A_QUIETER_WINDOW = f"""
alembic 0019 could not acquire its DDL locks within {LOCK_TIMEOUT}. The whole
migration has ROLLED BACK - nothing was applied, and it is safe to re-run.

Something held a conflicting lock on billing_cost_events or one of its
partitions. Find it with:

    SELECT pid, state, xact_start, wait_event_type, left(query, 120)
      FROM pg_stat_activity
     WHERE datname = current_database() AND xact_start IS NOT NULL
     ORDER BY xact_start;

RETRY IN A QUIETER WINDOW (or after that transaction ends).

Do NOT raise LOCK_TIMEOUT to get past this. CREATE TRIGGER takes
ShareRowExclusiveLock, which conflicts with the RowExclusiveLock every INSERT
holds, and the lock queue is FIFO: while this migration waits, every NEW cost
event queues behind it. A longer timeout is a longer billing ingest stall, not a
better chance of success.
""".strip()


def _bound_the_wait() -> None:
    """Bound this migration's lock wait, and prove the bound actually took.

    ``SET LOCAL`` outside a transaction block is a WARNING and a no-op, which
    would leave this guard vacuous. Read back rather than trusted -- same
    reasoning as 0016, 0017 and 0018.
    """
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0019 refuses to run without a bounded lock_timeout: SET LOCAL "
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
    op.execute(REQUIRE_AUDIT_TABLE_DDL)
    for stmt in UNROLLED_GUARD_DDL:
        op.execute(stmt)


def downgrade() -> None:
    # DROP TRIGGER takes the same ShareRowExclusiveLock on the same relations,
    # so the downgrade can stall ingest exactly the way the upgrade can.
    _bound_the_wait()
    _fail_fast(_downgrade_ddl)


def _downgrade_ddl() -> None:
    # Restore the partition-minting function to its 0017 body FIRST, so a
    # partition minted between the trigger drop and the function restore cannot
    # reference a function that is about to disappear. It comes back WITH 0016's
    # audit triggers and 0017's intent guard: this downgrade removes the
    # watermark gate, and only the watermark gate.
    op.execute(_FN_WITH_GUARD_ONLY)
    op.execute(
        """
        DO $do$
        DECLARE
            rel text;
        BEGIN
            FOR rel IN
                SELECT c.oid::regclass::text
                  FROM pg_trigger t
                  JOIN pg_class c ON c.oid = t.tgrelid
                 WHERE t.tgname = 'billing_unrolled_truncate_guard_trg'
                 GROUP BY c.oid
            LOOP
                EXECUTE format(
                    'DROP TRIGGER IF EXISTS billing_unrolled_truncate_guard_trg ON %s',
                    rel);
            END LOOP;
        END
        $do$;
        """
    )
    # Dropped only after every trigger that references it is gone. The function
    # holds no state; the audit ROWS it wrote stay, because they are records of
    # something that happened and must outlive the control.
    op.execute("DROP FUNCTION IF EXISTS public.billing_forbid_unrolled_truncate()")
