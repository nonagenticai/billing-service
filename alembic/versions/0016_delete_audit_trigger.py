"""Audit every DELETE against the two financial event ledgers (issue #92).

WHAT IS BROKEN
--------------
22,438 nil-tenant financial rows were deleted from the billing primary with no
audit trail. The only witness was ``pg_stat_user_tables.n_tup_del`` per
partition (May 19 vs 19, June 5,981 vs 5,981) — a counter, not a record. It
cannot say which rows, whose tenant, by whom, or under what statement.
``log_statement=ddl`` logs no DML, so the statement text never existed. No
migration deletes those rows. 0013 states the principle this broke: "Nothing is
deleted — these are financial records."

WHAT THIS INSTALLS
------------------
``billing_delete_audit`` plus a statement-level AFTER DELETE / AFTER TRUNCATE
trigger on ``billing_cost_events`` and ``billing_metering_events``, on every
existing cost-events partition, and inside
``create_cost_events_partition_for`` so future partitions inherit it.

The rationale — why a trigger and not pgaudit, why STATEMENT level and not ROW,
why routine retention is RECORDED rather than EXCLUDED, and the PostgreSQL 18.6
measurements about partitioned tables that shape the attachment — is in
``src/models/db/delete_audit.py``, which is also where the DDL text lives. This
migration imports nothing: the DDL is duplicated here verbatim so the migration
stays frozen in time, and ``tests/test_delete_audit_trigger.py`` asserts the two
copies are byte-identical so they cannot drift (0012 and 0015 exist only because
this repo let two schema sources drift; that is not repeated here).

WHY THIS IS SAFE ON A LIVE DATABASE
-----------------------------------
Additive only. It creates one new table, one function, and triggers; it alters
no existing column and deletes no row. ``create_cost_events_partition_for`` is
replaced with its current body (0003 ``_FN_BILLING``) plus the trigger
attachment — the CREATE TABLE half is unchanged. Every statement is idempotent
(``IF NOT EXISTS`` / ``DROP TRIGGER IF EXISTS`` / ``CREATE OR REPLACE``), so a
re-run is a no-op. It requires no restart and no ``shared_preload_libraries``
change, which is the whole reason it is a trigger and not pgaudit: the prod
primary on worker4 serves 73 databases.

"Additive" is not the same as "cheap to acquire", which is why the next section
exists.

LOCK DISCIPLINE — WHY THIS MIGRATION MUST FAIL FAST
---------------------------------------------------
``CREATE TRIGGER`` takes ``ShareRowExclusiveLock``, which CONFLICTS with the
``RowExclusiveLock`` every ``INSERT`` holds. Alembic runs the parent, all 14
existing cost-events partitions and ``billing_metering_events`` in ONE
transaction, and PostgreSQL's server default is ``lock_timeout = 0`` — wait
forever.

Measured on postgres:18.6 with no ``lock_timeout`` set: one ordinary open
transaction on the hot table left ``CREATE TRIGGER`` in
``pg_stat_activity.wait_event_type = 'Lock'``, and a FRESH ``INSERT`` arriving
afterwards ALSO showed ``wait_event_type = 'Lock'`` — the lock queue is FIFO, so
the new insert queued BEHIND the blocked DDL. On the prod billing primary that
is billing ingest stalled for as long as the blocking transaction lives, caused
by a migration that is "additive".

So the migration bounds its own wait: ``SET LOCAL lock_timeout`` aborts the
whole transaction and rolls back cleanly instead of holding the ingest path
hostage. Same treatment in ``downgrade()``, which takes the same lock via
``DROP TRIGGER``.

DELIBERATELY NOT SPLIT into per-relation transactions. A partially-applied
trigger set across 15 relations is an audit control with silent holes in
exactly the places (partitions) that #92 happened in — worse than a clean
failure that applied nothing. All-or-nothing, fail fast, retry in a quiet
window.

Revision ID: 0016_delete_audit_trigger
Revises: 0015_pricing_cost_daily_shapes
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.exc import DBAPIError

revision: str = "0016_delete_audit_trigger"
down_revision: str | None = "0015_pricing_cost_daily_shapes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# --- DDL, kept byte-identical to src/models/db/delete_audit.py --------------
# A test asserts this equality. Edit both or neither.

AUDIT_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_audit_deleted_rows()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
DECLARE
    v_rows        bigint;
    v_tenants     uuid[];
    v_tenant_n    integer;
    v_ids         uuid[];
    v_created_min timestamptz;
    v_created_max timestamptz;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        INSERT INTO public.billing_delete_audit (
            table_name, rows_deleted, operation, reason, db_user,
            session_user_name, application_name, client_addr, backend_pid,
            txid, statement
        ) VALUES (
            TG_TABLE_NAME, NULL, TG_OP,
            coalesce(nullif(current_setting('billing.delete_reason', true), ''), 'unspecified'),
            current_user, session_user,
            nullif(current_setting('application_name', true), ''),
            inet_client_addr(), pg_backend_pid(),
            pg_current_xact_id_if_assigned()::text::bigint,
            left(current_query(), 8000)
        );
        RETURN NULL;
    END IF;

    SELECT count(*),
           (array_agg(DISTINCT d.tenant_id))[1:20],
           count(DISTINCT d.tenant_id),
           (array_agg(d.id))[1:20],
           min(d.created_at),
           max(d.created_at)
      INTO v_rows, v_tenants, v_tenant_n, v_ids, v_created_min, v_created_max
      FROM deleted d;

    -- A DELETE whose predicate matched nothing is not an event worth a row.
    IF v_rows = 0 THEN
        RETURN NULL;
    END IF;

    INSERT INTO public.billing_delete_audit (
        table_name, rows_deleted, operation, reason, db_user,
        session_user_name, application_name, client_addr, backend_pid,
        txid, statement, distinct_tenant_count, tenant_ids, sample_ids,
        created_at_min, created_at_max
    ) VALUES (
        TG_TABLE_NAME, v_rows, TG_OP,
        coalesce(nullif(current_setting('billing.delete_reason', true), ''), 'unspecified'),
        current_user, session_user,
        nullif(current_setting('application_name', true), ''),
        inet_client_addr(), pg_backend_pid(),
        pg_current_xact_id_if_assigned()::text::bigint,
        left(current_query(), 8000),
        v_tenant_n, v_tenants, v_ids, v_created_min, v_created_max
    );
    RETURN NULL;
END
$fn$;
"""

ATTACH_TO_EXISTING_PARTITIONS_DDL = r"""
DO $do$
DECLARE
    v_parent oid;
    part     text;
BEGIN
    -- to_regclass, not a ::regclass cast: this runs from a per-table create
    -- hook too, where billing_cost_events may not exist yet. A cast would
    -- raise; this returns NULL and skips.
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
            'DROP TRIGGER IF EXISTS billing_delete_audit_trg ON public.%I', part);
        EXECUTE format(
            'CREATE TRIGGER billing_delete_audit_trg AFTER DELETE ON public.%I '
            'REFERENCING OLD TABLE AS deleted FOR EACH STATEMENT '
            'EXECUTE FUNCTION public.billing_audit_deleted_rows()', part);
        EXECUTE format(
            'DROP TRIGGER IF EXISTS billing_truncate_audit_trg ON public.%I', part);
        EXECUTE format(
            'CREATE TRIGGER billing_truncate_audit_trg AFTER TRUNCATE ON public.%I '
            'FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_deleted_rows()', part);
    END LOOP;
END
$do$;
"""

PARTITION_FN_WITH_AUDIT_DDL = r"""
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

AUDITED_TABLES = ("billing_cost_events", "billing_metering_events")


def _attach_ddl(relation: str) -> str:
    """Idempotent DELETE + TRUNCATE trigger attachment for one relation."""
    return f"""
DROP TRIGGER IF EXISTS billing_delete_audit_trg ON public.{relation};
CREATE TRIGGER billing_delete_audit_trg
    AFTER DELETE ON public.{relation}
    REFERENCING OLD TABLE AS deleted
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_deleted_rows();

DROP TRIGGER IF EXISTS billing_truncate_audit_trg ON public.{relation};
CREATE TRIGGER billing_truncate_audit_trg
    AFTER TRUNCATE ON public.{relation}
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_deleted_rows();
"""


DELETE_AUDIT_DDL: tuple[str, ...] = (
    AUDIT_FUNCTION_DDL,
    *(_attach_ddl(t) for t in AUDITED_TABLES),
    ATTACH_TO_EXISTING_PARTITIONS_DDL,
    PARTITION_FN_WITH_AUDIT_DDL,
)

# --- end shared DDL ---------------------------------------------------------


# 0003's _FN_BILLING, restored verbatim by downgrade().
_FN_BILLING_WITHOUT_AUDIT = r"""
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
            RETURN partition_name;
        END
        $function$;
"""


# --- lock discipline (see "LOCK DISCIPLINE" in the module docstring) --------

#: 3 seconds. Chosen against the hot path, not against the migration's comfort:
#: the cost flusher writes ``billing_cost_events`` in short batches, so any
#: conflicting lock still held after 3s is a long-running transaction, not
#: normal traffic — and waiting on one queues every subsequent INSERT behind
#: this DDL. 3s is long enough to ride out ordinary contention and short enough
#: that the worst case is a blip rather than an outage. Seconds, never minutes.
LOCK_TIMEOUT = "3s"

#: Once the locks are held, every statement here is catalog-only work that
#: completes in milliseconds. 60s cannot be hit by healthy work; it exists so a
#: pathological case (a catalog lock taken between statements) still terminates.
STATEMENT_TIMEOUT = "60s"

#: SQLSTATE 55P03 lock_not_available (lock_timeout) / 57014 query_canceled
#: (statement_timeout). Verified against postgres:18.6 — psycopg raises
#: ``LockNotAvailable`` with ``sqlstate == "55P03"``.
_TIMEOUT_SQLSTATES = frozenset({"55P03", "57014"})

_RETRY_IN_A_QUIETER_WINDOW = f"""
alembic 0016 could not acquire its DDL locks within {LOCK_TIMEOUT}. The whole
migration has ROLLED BACK — nothing was applied, and it is safe to re-run.

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
    would leave this guard vacuous — passing while protecting nothing. Alembic
    wraps each migration in a transaction (``env.py`` uses
    ``context.begin_transaction()``), so this reads the value back rather than
    trusting it.
    """
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0016 refuses to run without a bounded lock_timeout: SET LOCAL "
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
    op.create_table(
        "billing_delete_audit",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "ts", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("table_name", sa.Text(), nullable=False),
        sa.Column("rows_deleted", sa.BigInteger(), nullable=True),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), server_default=sa.text("'unspecified'"), nullable=False),
        sa.Column("db_user", sa.Text(), nullable=False),
        sa.Column("session_user_name", sa.Text(), nullable=False),
        sa.Column("application_name", sa.Text(), nullable=True),
        sa.Column("client_addr", sa.dialects.postgresql.INET(), nullable=True),
        sa.Column("backend_pid", sa.Integer(), nullable=False),
        sa.Column("txid", sa.BigInteger(), nullable=True),
        sa.Column("statement", sa.Text(), nullable=True),
        sa.Column("distinct_tenant_count", sa.Integer(), nullable=True),
        sa.Column(
            "tenant_ids",
            sa.dialects.postgresql.ARRAY(sa.dialects.postgresql.UUID(as_uuid=False)),
            nullable=True,
        ),
        sa.Column(
            "sample_ids",
            sa.dialects.postgresql.ARRAY(sa.dialects.postgresql.UUID(as_uuid=False)),
            nullable=True,
        ),
        sa.Column("created_at_min", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at_max", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_delete_audit_ts",
        "billing_delete_audit",
        [sa.text("ts DESC")],
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_delete_audit_table_ts",
        "billing_delete_audit",
        ["table_name", sa.text("ts DESC")],
        if_not_exists=True,
    )

    for stmt in DELETE_AUDIT_DDL:
        op.execute(stmt)


def downgrade() -> None:
    # DROP TRIGGER takes the same ShareRowExclusiveLock as CREATE TRIGGER, on
    # the same 15 relations, so the downgrade can stall billing ingest exactly
    # the way the upgrade can. Same bound, same instruction.
    _bound_the_wait()
    _fail_fast(_downgrade_ddl)


def _downgrade_ddl() -> None:
    # Restore the partition-minting function to its pre-audit body FIRST, so a
    # partition minted between the trigger drop and the function restore cannot
    # reference a function that is about to disappear.
    op.execute(_FN_BILLING_WITHOUT_AUDIT)
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
                 WHERE t.tgname IN ('billing_delete_audit_trg', 'billing_truncate_audit_trg')
                 GROUP BY c.oid
            LOOP
                EXECUTE format(
                    'DROP TRIGGER IF EXISTS billing_delete_audit_trg ON %s', rel);
                EXECUTE format(
                    'DROP TRIGGER IF EXISTS billing_truncate_audit_trg ON %s', rel);
            END LOOP;
        END
        $do$;
        """
    )
    op.execute("DROP FUNCTION IF EXISTS public.billing_audit_deleted_rows()")
    # The audit table itself is deliberately NOT dropped: it holds the only
    # record of any delete that happened while the trigger was live, and this
    # migration's entire premise is that such a record must outlive the rows it
    # describes. Drop it by hand if you truly mean to destroy it.
