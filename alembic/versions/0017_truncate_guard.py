"""Refuse a TRUNCATE of the cost ledger unless the session deliberately arms it.

WHAT HAPPENED
-------------
2026-09-05 07:01:56.876899Z, PROD billing primary (worker4). An automated client
TRUNCATEd ``billing_cost_events`` and all 12 partitions, plus
``billing_metering_events`` and every rollup table, in ONE statement.
``billing_delete_audit`` ids 1-14 are the only surviving record: db_user
``billing_user``, empty ``application_name``, every row carrying the same
single cluster-node egress address, reason ``unspecified``. Note ``billing_delete_audit`` was itself eleventh in
that table list -- the audit table was emptied by the same statement its trigger
was writing into. The 14 rows exist only because an AFTER TRUNCATE trigger runs
after the truncation.

The statement's shape is ``Base.metadata.sorted_tables`` in order with
``RESTART IDENTITY`` -- what ``tests/conftest.py`` builds for per-test isolation.
This repo's own suite, pointed at prod.

WHY NOT ``REVOKE TRUNCATE``
---------------------------
``billing_user`` OWNS all 14 relations (measured: ``pg_tables.tableowner``).
TRUNCATE rights flow from ownership; revoking from an owner executes and changes
nothing. Inert here, not merely weak.

WHAT THIS INSTALLS
------------------
``public.billing_forbid_truncate()`` plus a statement-level BEFORE TRUNCATE
trigger ``billing_truncate_guard_trg`` on ``billing_cost_events``,
``billing_metering_events``, every existing cost-events partition, and inside
``create_cost_events_partition_for`` so future partitions inherit it. The
trigger RAISEs unless::

    SET LOCAL billing.allow_truncate = 'on';

is in effect for the session. A deliberate truncate is one extra line; an
automated client pointed at the wrong DSN writes no such line. That asymmetry is
the entire control, and it is the shape the operator approved.

The rationale -- why a trigger, why a GUC and not a table flag, what the guard
does NOT stop, and how it interacts with 0016's AFTER TRUNCATE audit trigger --
lives in ``src/models/db/truncate_guard.py``, which is also where the DDL text
lives. This migration imports nothing: the DDL is duplicated here verbatim so the
migration stays frozen in time, and ``tests/test_truncate_guard.py`` asserts the
two copies are byte-identical so they cannot drift. Same discipline as 0016.

⛔ THE TRIGGER ALONE IS VACUOUS AGAINST THE INCIDENT ABOVE
---------------------------------------------------------
``tests/conftest.py`` MUST arm the hatch or every DB test fails on its own
isolation step -- so the suite, run against prod, would arm it and truncate prod
again. The other half of this control is in ``tests/conftest.py``, which refuses
to arm the hatch when the target database is the production billing database and
fails loudly instead. BOTH halves are required; neither is sufficient. Do not
remove one and assume the other still covers it.

INTERACTION WITH 0016 (``billing_truncate_audit_trg``)
------------------------------------------------------
No conflict, and not redundant. Postgres runs every BEFORE trigger before
performing the truncation and the AFTER triggers only after it completes, so a
refusal aborts the statement and the audit trigger never fires. A BLOCKED
truncate therefore leaves NO audit row -- nothing was truncated, so there is
nothing to audit; the refusal is an ERROR in the server log. An ALLOWED truncate
is unchanged: the guard returns and 0016's audit row is written exactly as
today.

``create_cost_events_partition_for`` is replaced with 0016's body PLUS the guard
attachment. The audit half is carried through unchanged -- in the module it is
DERIVED from 0016's constant rather than retyped, and the byte-equality test is
what keeps this frozen copy honest.

WHY THIS IS SAFE ON A LIVE DATABASE
-----------------------------------
Additive only: one function, triggers, and a CREATE OR REPLACE of an existing
function whose prior body is preserved inside the new one. No column is altered
and no row is deleted. Every statement is idempotent
(``CREATE OR REPLACE`` / ``DROP TRIGGER IF EXISTS``), so a re-run is a no-op.

It changes no INSERT, UPDATE, DELETE or SELECT path: a BEFORE TRUNCATE trigger
fires on TRUNCATE and nothing else, and billing never truncates in normal
operation. The ingest hot path cannot notice this migration exists.

LOCK DISCIPLINE
---------------
Identical to 0016 and for the identical reason: ``CREATE TRIGGER`` takes
``ShareRowExclusiveLock``, which conflicts with the ``RowExclusiveLock`` every
``INSERT`` holds, and the lock queue is FIFO -- a blocked DDL puts every
subsequent INSERT behind itself. All 14 relations are done in ONE transaction
with a bounded ``lock_timeout``, so the migration aborts and rolls back cleanly
rather than stalling billing ingest. DELIBERATELY NOT SPLIT: a partially applied
guard across 14 relations is a control with holes in exactly the places
(partitions) the incident landed in.

⛔ NOT APPLIED TO PROD BY THIS PR.

Revision ID: 0017_truncate_guard
Revises: 0016_delete_audit_trigger
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy.exc import DBAPIError

revision: str = "0017_truncate_guard"
down_revision: str | None = "0016_delete_audit_trigger"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# --- DDL, kept byte-identical to src/models/db/truncate_guard.py -----------
# A test asserts this equality. Edit both or neither.

ALLOW_TRUNCATE_GUC = "billing.allow_truncate"
ALLOW_TRUNCATE_VALUE = "on"

TRUNCATE_GUARD_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_truncate()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
    -- current_setting(..., true) -> NULL for an unset GUC instead of raising,
    -- so an un-armed session takes the refusal path rather than an
    -- "unrecognized configuration parameter" error that reads like a bug in
    -- the guard.
    IF coalesce(current_setting('billing.allow_truncate', true), '') = 'on' THEN
        RETURN NULL;
    END IF;

    RAISE EXCEPTION
        'TRUNCATE of financial ledger %.% is blocked', TG_TABLE_SCHEMA, TG_TABLE_NAME
        USING
            ERRCODE = 'P0001',
            HINT =
                'This table holds billing records; billing is append-only and has no '
                'refund path, so an accidental TRUNCATE is unrecoverable. On 2026-09-05 '
                'an automated client emptied this ledger and every partition of it. If '
                'you MEAN to do this, arm the escape hatch in the same transaction: '
                'SET LOCAL billing.allow_truncate = ''on''; -- then TRUNCATE. '
                'Set billing.delete_reason too, so the audit row says why.';
END
$fn$;
"""

AUDITED_TABLES = ("billing_cost_events", "billing_metering_events")


def _guard_attach_ddl(relation: str) -> str:
    """Idempotent BEFORE TRUNCATE guard attachment for one relation."""
    return f"""
DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.{relation};
CREATE TRIGGER billing_truncate_guard_trg
    BEFORE TRUNCATE ON public.{relation}
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate();
"""


ATTACH_GUARD_TO_EXISTING_PARTITIONS_DDL = r"""
DO $do$
DECLARE
    v_parent oid;
    part     text;
BEGIN
    -- to_regclass, not a ::regclass cast, for the same reason 0016 uses it: this
    -- also runs from a per-table create hook where the parent may not exist yet.
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
            'DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.%I', part);
        EXECUTE format(
            'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate()', part);
    END LOOP;
END
$do$;
"""

PARTITION_FN_WITH_GUARD_DDL = r"""
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


#: The whole control, in application order. Byte-identical to the module's.
TRUNCATE_GUARD_DDL: tuple[str, ...] = (
    TRUNCATE_GUARD_FUNCTION_DDL,
    *(_guard_attach_ddl(t) for t in AUDITED_TABLES),
    ATTACH_GUARD_TO_EXISTING_PARTITIONS_DDL,
    PARTITION_FN_WITH_GUARD_DDL,
)

# --- end shared DDL --------------------------------------------------------


# 0016's partition-minting function, restored verbatim by downgrade(). This is
# the AUDIT-ONLY body: dropping the guard must leave #92's control fully intact,
# so the downgrade puts back exactly what 0016 installed rather than a body it
# invents. A test asserts this equals 0016's own constant.
_FN_WITH_AUDIT_ONLY = r"""
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

            RETURN partition_name;
        END
        $function$;
"""


# --- lock discipline (see "LOCK DISCIPLINE" in the module docstring) --------

#: 3 seconds, chosen against the hot path exactly as 0016 chose it. Seconds,
#: never minutes: while this DDL waits, every new INSERT queues behind it.
LOCK_TIMEOUT = "3s"

#: Catalog-only work once the locks are held; 60s cannot be hit by healthy work.
STATEMENT_TIMEOUT = "60s"

#: 55P03 lock_not_available / 57014 query_canceled.
_TIMEOUT_SQLSTATES = frozenset({"55P03", "57014"})

_RETRY_IN_A_QUIETER_WINDOW = f"""
alembic 0017 could not acquire its DDL locks within {LOCK_TIMEOUT}. The whole
migration has ROLLED BACK - nothing was applied, and it is safe to re-run.

Something held a conflicting lock on billing_cost_events, one of its
partitions, or billing_metering_events. Find it with:

    SELECT pid, state, xact_start, wait_event_type, left(query, 120)
      FROM pg_stat_activity
     WHERE datname = current_database() AND xact_start IS NOT NULL
     ORDER BY xact_start;

RETRY IN A QUIETER WINDOW (or after that transaction ends).

Do NOT raise LOCK_TIMEOUT to get past this. CREATE TRIGGER takes
ShareRowExclusiveLock, which conflicts with the RowExclusiveLock every INSERT
holds, and the lock queue is FIFO: while this migration waits, every NEW insert
into the billing ledgers queues behind it. A longer timeout is a longer billing
ingest stall, not a better chance of success.
""".strip()


def _bound_the_wait() -> None:
    """Bound this migration's lock wait, and prove the bound actually took.

    ``SET LOCAL`` outside a transaction block is a WARNING and a no-op, which
    would leave this guard vacuous. Read back rather than trusted -- same
    reasoning as 0016.
    """
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0017 refuses to run without a bounded lock_timeout: SET LOCAL "
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
    for stmt in TRUNCATE_GUARD_DDL:
        op.execute(stmt)


def downgrade() -> None:
    # DROP TRIGGER takes the same ShareRowExclusiveLock on the same 14 relations,
    # so the downgrade can stall ingest exactly the way the upgrade can.
    _bound_the_wait()
    _fail_fast(_downgrade_ddl)


def _downgrade_ddl() -> None:
    # Restore the partition-minting function to its audit-only (0016) body
    # FIRST, so a partition minted between the trigger drop and the function
    # restore cannot reference a function that is about to disappear. It comes
    # back WITH 0016's audit triggers: this downgrade removes the guard, not the
    # audit control.
    op.execute(_FN_WITH_AUDIT_ONLY)
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
                 WHERE t.tgname = 'billing_truncate_guard_trg'
                 GROUP BY c.oid
            LOOP
                EXECUTE format(
                    'DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON %s', rel);
            END LOOP;
        END
        $do$;
        """
    )
    # Dropped only after every trigger that references it is gone. Unlike 0016's
    # audit TABLE (which holds records that must outlive the control), this
    # function holds no state -- there is nothing to preserve.
    op.execute("DROP FUNCTION IF EXISTS public.billing_forbid_truncate()")
