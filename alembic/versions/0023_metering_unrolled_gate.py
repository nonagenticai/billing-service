"""Give ``billing_metering_events`` the STATE gate the cost ledger got in 0019.

WHAT HAPPENED, AND WHAT 0019 LEFT OPEN
--------------------------------------
2026-09-05T07:01:56.876899Z, PROD billing primary (worker4). ONE statement
TRUNCATEd ``billing_cost_events`` and all 12 partitions, ``billing_metering_events``,
and every rollup table. ``billing_delete_audit`` ids 1-14 are the only surviving
record. Three days of un-aggregated revenue went with it; there is no source to
backfill from.

0017/0018 installed an INTENT gate (``billing.allow_truncate``) on both ledgers:
"did you MEAN to truncate?". 0019 (#140) added a STATE gate to the cost ledger:
"has the rollup caught up?" — refused even for an ARMED session, because the
armed path is the everyday one and it destroys un-aggregated revenue just as
completely. ⛔ ``billing_metering_events`` never got the second half. Measured on
the production primary immediately before this migration was written::

    SELECT c.relname, t.tgname FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid WHERE NOT t.tgisinternal;

    billing_cost_events      billing_delete_audit_trg
                             billing_truncate_audit_trg
                             billing_truncate_guard_trg           <- intent
                             billing_unrolled_truncate_guard_trg  <- state
    billing_metering_events  billing_delete_audit_trg
                             billing_truncate_audit_trg
                             billing_truncate_guard_trg           <- intent
                             -- nothing --

A metering rollup that stalls is therefore in exactly the position the cost
rollup was in on 2026-09-04: silently behind, and one armed TRUNCATE from
permanent loss.

WHY A SECOND FUNCTION AND NOT 0019's
------------------------------------
``billing_forbid_unrolled_truncate()`` hardcodes ``public.billing_cost_daily``
and ignores ``TG_TABLE_NAME``. Attached to the metering table it would judge
metering rows against the COST watermark — a gate that passes or refuses for a
reason unrelated to the table it is protecting. Worse than absent, because it
reports a property it never measured. This installs
``public.billing_forbid_unrolled_metering_truncate()``, which reads the METERING
chain and RAISEs if it is ever attached to a relation that is not
``billing_metering_events`` or a partition of it.

THE PREDICATE IS COMPOUND
-------------------------
Metering aggregates in two stages (``events -> billing_metering_hourly ->
billing_metering_daily``) which lag INDEPENDENTLY. ``rollup_daily`` builds a
day from whatever hourly holds for it without requiring all 24 hours, so
``max(bucket_date)`` can advance over holed hourly coverage — a daily-only gate
passes there. Hourly alone trusts a witness the 2026-09-05 statement was
destroying in the same breath. So the cutoff is the EARLIER of the two
coverage ends, each ``coalesce``d to ``-infinity`` BEFORE ``least()`` (which
ignores NULLs and would otherwise be fail-OPEN). Full rationale, and the reason
each stage alone is insufficient, lives in ``src/models/db/metering_unrolled_guard.py``.

⚠️ 0019's docstring said this gap was "separately tracked". Measured 2026-09-08:
zero PRs, zero issues, against a positive control of 6 issues. It was not.
That line is corrected in place in ``unrolled_guard.py``.

PARTITIONING
------------
``billing_metering_events`` is ``relkind = 'r'`` — NOT partitioned; only
``billing_cost_events`` is ``'p'`` (12 children). So there is no
partition-minting function to extend and this migration does not touch
``create_cost_events_partition_for`` at all — 0019's version of that function is
left exactly as it is. The partition-attachment loop below is still executed and
still finds nothing: Postgres does NOT clone statement-level TRUNCATE triggers
to partitions, so it is what makes this control correct the day the table is
partitioned rather than the day someone remembers.

THE ESCAPE HATCH — A THIRD GUC
------------------------------
::

    SET LOCAL billing.allow_truncate = \'on\';                     -- 0017: intent
    SET LOCAL billing.allow_unrolled_metering_truncate = \'on\';   -- state
    SET LOCAL billing.delete_reason = \'why, in words\';
    TRUNCATE billing_metering_events;

Deliberately NOT 0019's ``billing.allow_unrolled_truncate``: the incident\'s
shape is one statement naming many tables, so arming the cost ledger\'s override
must not carry permission over the metering ledger. Taking it writes a
``billing_delete_audit`` row BEFORE the rows go, ``operation =
\'TRUNCATE_UNROLLED_METERING_OVERRIDE\'``.

WHY THIS IS SAFE ON A LIVE DATABASE
-----------------------------------
Additive only: one function and one trigger. No column altered, no row deleted,
no existing function replaced. Every statement is idempotent
(``CREATE OR REPLACE`` / ``DROP TRIGGER IF EXISTS``), so a re-run is a no-op. A
BEFORE TRUNCATE trigger fires on TRUNCATE and nothing else, and billing never
truncates in normal operation, so the ingest hot path cannot notice this exists.

LOCK DISCIPLINE
---------------
Identical to 0016-0019 and for the identical reason: ``CREATE TRIGGER`` takes
``ShareRowExclusiveLock``, which conflicts with the ``RowExclusiveLock`` every
``INSERT`` holds, and the lock queue is FIFO — a blocked DDL puts every
subsequent INSERT behind itself. Bounded ``lock_timeout``, so this aborts and
rolls back cleanly rather than stalling metering ingest.

⛔ NOT APPLIED TO PROD BY THIS PR.

Revision ID: 0023_metering_unrolled_gate
Revises: 0022_provider_pricing_cost_basis
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy.exc import DBAPIError

revision: str = "0023_metering_unrolled_gate"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0022_provider_pricing_cost_basis"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# FROZEN COPIES of src/models/db/metering_unrolled_guard.py, duplicated rather
# than imported so this migration stays fixed in time.
# tests/test_metering_unrolled_truncate_gate.py asserts byte identity, which is
# the only thing that keeps the two copies from drifting -- 0012 and 0015 exist
# solely because this repo once let two schema sources drift.
# ---------------------------------------------------------------------------

METERING_GATE_TRIGGER = "billing_unrolled_metering_truncate_guard_trg"

METERING_UNROLLED_GUARD_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_unrolled_metering_truncate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
DECLARE
    v_hourly_ts  timestamptz;
    v_daily_max  date;
    v_hourly_end timestamptz;
    v_daily_end  timestamptz;
    v_cutoff     timestamptz;
    v_unrolled   boolean;
    v_rows       bigint;
    v_min        timestamptz;
    v_max        timestamptz;
BEGIN
    -- ⛔ ANSWER ONLY FOR THE TABLE THIS WATERMARK DESCRIBES. 0019's function
    -- hardcodes billing_cost_daily and ignores TG_TABLE_NAME, which is why it
    -- could not be reused here: attached to the wrong relation it returns a
    -- confident verdict about a table it never measured. This one refuses.
    IF TG_TABLE_NAME <> 'billing_metering_events'
       AND to_regclass('public.' || quote_ident(TG_TABLE_NAME)) NOT IN (
            SELECT i.inhrelid::regclass
              FROM pg_inherits i
             WHERE i.inhparent = to_regclass('public.billing_metering_events'))
    THEN
        RAISE EXCEPTION
            'billing_forbid_unrolled_metering_truncate fired on %.%, which is not '
            'billing_metering_events or a partition of it', TG_TABLE_SCHEMA, TG_TABLE_NAME
            USING
                ERRCODE = 'P0001',
                HINT =
                    'This function reads the METERING rollup watermarks '
                    '(billing_metering_hourly, billing_metering_daily). Attaching it to '
                    'another relation would judge that relation against the wrong '
                    'aggregate and pass or refuse for the wrong reason. Write a gate for '
                    'that table''s own rollup chain instead.';
    END IF;

    -- STAGE 1 of 2: hourly. to_regclass, not a cast, for the same reason 0019
    -- uses it: this function is also installed by the metadata after_create
    -- hook, where a rollup table may not exist yet. A missing stage is treated
    -- as "nothing has been rolled up" -- the fail-CLOSED reading.
    IF to_regclass('public.billing_metering_hourly') IS NULL THEN
        v_hourly_ts := NULL;
    ELSE
        SELECT max(bucket_hour) INTO v_hourly_ts FROM public.billing_metering_hourly;
    END IF;

    -- STAGE 2 of 2: daily.
    IF to_regclass('public.billing_metering_daily') IS NULL THEN
        v_daily_max := NULL;
    ELSE
        SELECT max(bucket_date) INTO v_daily_max FROM public.billing_metering_daily;
    END IF;

    -- rollup_hourly covers [bucket_hour, bucket_hour + 1 hour), so the first
    -- instant hourly does NOT cover is one hour after the newest bucket.
    v_hourly_end := coalesce(v_hourly_ts + interval '1 hour', '-infinity'::timestamptz);

    -- rollup_daily covers a whole UTC day, so the first instant daily does NOT
    -- cover is the start of the day after the newest bucket. UTC-explicit: this
    -- is exactly how rollup_daily assigns bucket_date, and the session timezone
    -- must not be able to shift the boundary.
    v_daily_end := coalesce(
        (v_daily_max + 1)::timestamp AT TIME ZONE 'UTC', '-infinity'::timestamptz);

    -- ⭐ THE COMPOUND PREDICATE: the EARLIER of the two stages' coverage ends.
    -- The stages lag independently -- a day rolled from an incomplete hourly
    -- advances max(bucket_date) over hours that reached no aggregate at all --
    -- so neither alone is a gate. coalesce BEFORE least(), never after: least()
    -- ignores NULLs, so an empty stage would otherwise silently hand the whole
    -- decision to the other one, which is fail-OPEN.
    v_cutoff := least(v_hourly_end, v_daily_end);

    EXECUTE format(
        'SELECT EXISTS (SELECT 1 FROM public.%I WHERE created_at >= $1)',
        TG_TABLE_NAME)
      INTO v_unrolled
      USING v_cutoff;

    IF NOT v_unrolled THEN
        -- Both stages have caught up with this relation. Whatever intent-level
        -- control ran before us has already had its say; this gate has no
        -- further objection. THIS ARM MATTERS: a gate that refuses everything is
        -- as broken as one that refuses nothing.
        RETURN NULL;
    END IF;

    -- From here a scan is affordable: either we abort the statement, or the
    -- caller is about to destroy the table anyway.
    EXECUTE format(
        'SELECT count(*), min(created_at), max(created_at) '
        'FROM public.%I WHERE created_at >= $1',
        TG_TABLE_NAME)
      INTO v_rows, v_min, v_max
      USING v_cutoff;

    IF coalesce(current_setting('billing.allow_unrolled_metering_truncate', true), '')
       <> 'on' THEN
        RAISE EXCEPTION
            'TRUNCATE of %.% would delete % un-aggregated metering event(s)',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, v_rows
            USING
                ERRCODE = 'P0001',
                DETAIL = format(
                    'metering rollup covers up to %s (hourly frontier %s, daily frontier '
                    '%s -- the EARLIER binds); %s row(s) with created_at in [%s, %s] have '
                    'not been aggregated by both stages. Deleting them destroys usage '
                    'that no other table witnesses.',
                    v_cutoff,
                    coalesce(v_hourly_ts::text, '(no hourly bucket has ever been written)'),
                    coalesce(v_daily_max::text, '(no daily bucket has ever been written)'),
                    v_rows, coalesce(v_min::text, '-'), coalesce(v_max::text, '-')),
                HINT =
                    'On 2026-09-05 one statement truncated this table alongside the cost '
                    'ledger and NOTHING was recoverable. Run the rollup until BOTH '
                    'billing_metering_hourly and billing_metering_daily cover these rows, '
                    'then retry. If you MEAN to delete un-aggregated metering, say so '
                    'explicitly in the same transaction: '
                    'SET LOCAL billing.allow_unrolled_metering_truncate = ''on''; '
                    'SET LOCAL billing.delete_reason = ''<why>''; -- then TRUNCATE. '
                    'That is a DIFFERENT GUC from the cost ledger''s '
                    'billing.allow_unrolled_truncate, deliberately: arming one ledger '
                    'must not silently authorise draining the other in the same '
                    'statement. That path is audited into billing_delete_audit as '
                    'TRUNCATE_UNROLLED_METERING_OVERRIDE.';
    END IF;

    -- Override taken. Record it BEFORE the rows go, with the numbers that are
    -- otherwise unreconstructable -- the 2026-09-05 loss had to be characterised
    -- by decoding UUIDv7 timestamps because no row like this existed.
    INSERT INTO public.billing_delete_audit (
        table_name, rows_deleted, operation, reason, db_user,
        session_user_name, application_name, client_addr, backend_pid,
        txid, statement, created_at_min, created_at_max
    ) VALUES (
        TG_TABLE_NAME,
        -- The UN-AGGREGATED count, not the total the statement removes. 0016's
        -- AFTER TRUNCATE row carries the statement; this row carries the loss.
        v_rows,
        'TRUNCATE_UNROLLED_METERING_OVERRIDE',
        format(
            'unrolled-metering-truncate override: %s row(s) after cutoff %s '
            '(hourly %s, daily %s). operator reason: %s',
            v_rows,
            v_cutoff,
            coalesce(v_hourly_ts::text, '(none)'),
            coalesce(v_daily_max::text, '(none)'),
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


def _metering_gate_attach_ddl(relation: str) -> str:
    """Idempotent BEFORE TRUNCATE gate attachment for one relation."""
    return f"""
DROP TRIGGER IF EXISTS {METERING_GATE_TRIGGER} ON public.{relation};
CREATE TRIGGER {METERING_GATE_TRIGGER}
    BEFORE TRUNCATE ON public.{relation}
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.billing_forbid_unrolled_metering_truncate();
"""


ATTACH_METERING_GATE_TO_EXISTING_PARTITIONS_DDL = r"""
DO $do$
DECLARE
    v_parent oid;
    part     text;
BEGIN
    v_parent := to_regclass('public.billing_metering_events');
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
            'DROP TRIGGER IF EXISTS billing_unrolled_metering_truncate_guard_trg '
            'ON public.%I', part);
        EXECUTE format(
            'CREATE TRIGGER billing_unrolled_metering_truncate_guard_trg '
            'BEFORE TRUNCATE ON public.%I FOR EACH STATEMENT '
            'EXECUTE FUNCTION public.billing_forbid_unrolled_metering_truncate()', part);
    END LOOP;
END
$do$;
"""


#: The whole control, in application order. Identical tuple to the module's.
METERING_UNROLLED_GUARD_DDL: tuple[str, ...] = (
    METERING_UNROLLED_GUARD_FUNCTION_DDL,
    _metering_gate_attach_ddl("billing_metering_events"),
    ATTACH_METERING_GATE_TO_EXISTING_PARTITIONS_DDL,
)


#: The gate audits its own override into billing_delete_audit (alembic 0016). If
#: that table is absent the override path would fail with an unrelated "relation
#: does not exist" that reads like a bug in the gate rather than a missing
#: dependency. Same precondition, same wording, as 0019.
REQUIRE_AUDIT_TABLE_DDL = r"""
DO $do$
BEGIN
    IF to_regclass('public.billing_delete_audit') IS NULL THEN
        RAISE EXCEPTION
            'alembic 0023 requires public.billing_delete_audit from 0016'
            USING HINT =
                'The metering watermark gate audits its own override into that table. '
                'Run alembic 0016 first, or downgrade past 0023 as well.';
    END IF;
END
$do$;
"""


# --- lock discipline (see "LOCK DISCIPLINE" in the module docstring) --------

#: 3 seconds -- the same bound 0016-0019 chose against the same hot path.
#: Seconds, never minutes: while this DDL waits, every new INSERT into
#: billing_metering_events queues behind it.
LOCK_TIMEOUT = "3s"

#: Catalog-only work once the lock is held; 60s cannot be hit by healthy work.
STATEMENT_TIMEOUT = "60s"

#: 55P03 lock_not_available / 57014 query_canceled.
_TIMEOUT_SQLSTATES = {"55P03", "57014"}

_RETRY_IN_A_QUIETER_WINDOW = (
    "alembic 0023 timed out waiting for a lock on billing_metering_events. Nothing "
    "was applied and nothing is half-applied: the whole migration is one "
    "transaction. CREATE TRIGGER needs ShareRowExclusiveLock, which conflicts with "
    "the RowExclusiveLock every metering INSERT holds, and the lock queue is FIFO -- "
    "so waiting longer would have queued live ingest behind this DDL rather than "
    "made it succeed. Re-run during a quiet window, or pause the event flusher."
)


def _bound_the_wait() -> None:
    """Bound this migration's lock wait, and prove the bound actually took.

    ``SET LOCAL`` outside a transaction block is a WARNING and a no-op, which
    would leave this guard vacuous. Read back rather than trusted -- same
    reasoning as 0016-0019.
    """
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0023 refuses to run without a bounded lock_timeout: SET LOCAL "
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
    for stmt in METERING_UNROLLED_GUARD_DDL:
        op.execute(stmt)


def downgrade() -> None:
    # DROP TRIGGER takes the same ShareRowExclusiveLock on the same relation, so
    # the downgrade can stall ingest exactly the way the upgrade can.
    _bound_the_wait()
    _fail_fast(_downgrade_ddl)


def _downgrade_ddl() -> None:
    # ⛔ Deliberately does NOT touch create_cost_events_partition_for. Unlike
    # 0019, this migration never replaced it, so restoring an earlier body here
    # would revert a DIFFERENT control (0019's watermark gate on newly minted
    # cost partitions) while claiming to revert this one.
    #
    # Drop by catalog lookup rather than by a hardcoded relation list, so a
    # partition attached by a future upgrade is also released. The function is
    # dropped only after every trigger referencing it is gone.
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
                 WHERE t.tgname = 'billing_unrolled_metering_truncate_guard_trg'
                 GROUP BY c.oid
            LOOP
                EXECUTE format(
                    'DROP TRIGGER IF EXISTS billing_unrolled_metering_truncate_guard_trg '
                    'ON %s', rel);
            END LOOP;
        END
        $do$;
        """
    )
    # The audit ROWS the gate wrote stay: they are records of something that
    # happened and must outlive the control that recorded them.
    op.execute("DROP FUNCTION IF EXISTS public.billing_forbid_unrolled_metering_truncate()")
