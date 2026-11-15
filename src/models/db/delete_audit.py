"""Forensic audit of every row DELETE-d from the two financial event ledgers.

WHY THIS EXISTS (issue #92)
---------------------------
22,438 nil-tenant financial rows were deleted from the billing primary and
NOTHING recorded it. The only witness was ``pg_stat_user_tables.n_tup_del`` per
partition (May 19 vs 19, June 5,981 vs 5,981) — a counter, not a record: it
says a number of rows left, never which, whose, by whom, or under what
statement. ``log_statement=ddl`` logs no DML, so the statement text was gone.
No migration deletes those rows. Alembic 0013 states the principle this broke:
*"Nothing is deleted — these are financial records."*

WHY A TRIGGER AND NOT pgaudit
-----------------------------
pgaudit needs ``shared_preload_libraries`` and therefore a full restart of the
prod primary, which serves 73 databases. A per-table AFTER DELETE trigger into
a billing-owned table is billing-only, needs no restart, and ships as an
ordinary alembic migration.

The "an audit would flood the disk" objection is REFUTED, not deferred: billing
DML is ~22 MB/day, the whole 73-database instance ~149 MB/day, against 1.2 TB
free. And this design writes per STATEMENT, not per ROW (below), so its volume
is bounded by statement count — a few rows a day.

STATEMENT-LEVEL, AND NOTHING IS EXCLUDED
----------------------------------------
The live objection to a blanket DELETE trigger is that it amplifies routine
cleanup. Measured against the actual delete paths in this service:

* ``billing_cost_events`` — retention is partition-native
  (``rollup_engine.drop_old_cost_partitions`` DROPs a whole aged monthly
  partition). ``DROP TABLE`` fires no row or statement DELETE trigger, so
  retention is invisible to this control by construction. Every audited DELETE
  on cost_events is therefore already anomalous.
* ``billing_metering_events`` — retention IS a row DELETE
  (``rollup_engine.cleanup_old_events``: ``DELETE ... WHERE created_at <
  cutoff``). This is the real amplifier, and it is a SINGLE statement that may
  remove many thousands of rows.

So the trigger is ``FOR EACH STATEMENT ... REFERENCING OLD TABLE`` and writes
exactly ONE row per DELETE statement, carrying the row count and a summary of
the transition table. The nightly retention sweep costs one audit row.

That is the "record it compactly with a distinguishing reason" option, and it
was chosen over "exclude retention via a session GUC / role check" deliberately:

* an exclusion is an override, and an override is the thing an unlogged delete
  would reach for — a control that can be switched off by the caller it is
  auditing is not a control;
* an exclusion also blinds the audit to retention going *wrong* (a bad cutoff
  deleting a year of events looks exactly like a normal sweep);
* the reason is recorded, never enforced. ``billing.delete_reason`` is an
  optional session GUC a caller MAY set to label its own statement. Setting it
  changes one text column; it never suppresses a row. Mislabelling is possible
  and harmless — the statement text, user, client address and row count are
  captured regardless and are what actually identify the caller.

TRUNCATE is covered by a second trigger. It is a DELETE-shaped way to empty a
financial table that row triggers do not see at all, and leaving it uncovered
would leave the obvious evasion open. It cannot report a row count (there is no
transition table for TRUNCATE), so ``rows_deleted`` is NULL there.

PARTITIONS — MEASURED, NOT ASSUMED
----------------------------------
Verified on PostgreSQL 18.6 before writing this:

* a statement trigger with a transition table on a RANGE-partitioned PARENT is
  accepted and fires once for a parent-level DELETE;
* a ``DELETE FROM billing_cost_events_2026_09`` aimed DIRECTLY at a child
  partition does **not** fire the parent's trigger — and per-partition
  ``n_tup_del`` was the incident's only witness, so a partition-targeted delete
  is precisely the shape that occurred and precisely the shape a parent-only
  trigger would miss;
* attaching the same trigger to parent AND children does not double count: a
  parent-level DELETE fires only the parent's, a child-level DELETE only the
  child's.

IS AFTER DELETE EVEN THE RIGHT INSTRUMENT? (measured read-only on worker4)
-------------------------------------------------------------------------
A partition DROP or DETACH fires no DELETE trigger, so if the 22,438 rows had
left via partition maintenance this control could not see its own motivating
incident. It did not. From ``pg_stat_user_tables`` on the prod billing DB:

    billing_cost_events         (parent)   n_tup_del = 0
    billing_cost_events_2026_06            n_tup_del = 5,981
    billing_cost_events_2026_07            n_tup_del = 11,592
    billing_cost_events_2026_08            n_tup_del = 4,848

n_tup_del being NON-ZERO ON A LIVE CHILD is itself the proof: a dropped
partition takes its ``pg_stat`` row with it, so a DROP leaves no n_tup_del
anywhere. Those 22,421 rows were removed by real row DELETEs, which is exactly
what this trigger records.

It also settles the attachment point. The parent's counter is 0 and every
non-zero count is on a CHILD, so the incident was partition-local — and a
parent-only statement trigger does not fire for a child-targeted DELETE
(measured above). A parent-only attachment would have been blind to the very
event it exists for.

RESIDUAL GAPS, NAMED
--------------------
* A partition DROP/DETACH is still invisible here. Covering it needs a DDL
  event trigger, which PostgreSQL only lets a SUPERUSER create — so it cannot
  ship as an ordinary migration run by the application role, and it is not in
  this change. Note that ``drop_old_cost_partitions`` legitimately drops aged
  partitions (prod's oldest is 2026_06; 2026_05 and earlier are gone), so a
  DDL-level control would have to distinguish retention from anything else.
* A partition created by some path other than
  ``create_cost_events_partition_for`` would not be armed. That function is the
  only creation path in this service today (``ensure_cost_event_partitions``).
* ``billing_delete_audit`` is NOT itself in ``AUDITED_TABLES``, so the audit can
  be emptied without leaving an audit. Deliberate, not an oversight: the shared
  trigger function is written against the LEDGER shape — it aggregates ``d.id``,
  ``d.tenant_id`` and ``d.created_at`` out of the transition table, and this
  table has none of those columns, so arming it needs a SECOND function rather
  than one more entry in the tuple. It would also buy little: anyone able to
  delete audit rows can ``DROP TRIGGER`` too, so a self-trace deters only the
  careless. The real containment is grants and retention on this table, which is
  not in this change.

A NOTE ON COUNTING: "one audit row per statement" is true of DELETE, and of
TRUNCATE against a plain table. It is NOT true of ``TRUNCATE
billing_cost_events``: TRUNCATE recurses into every partition and each truncated
relation fires its OWN statement trigger, so the audit gets N+1 rows for N
partitions (measured 2 with one partition; 15 against the prod primary's 14).
That is correct and kept — those rows are what say WHICH partitions were
emptied, and collapsing them would reintroduce exactly the blindness
``n_tup_del`` already had.

COST
----
This is the FIRST non-internal trigger in the billing database, and it sits on
tables that also carry the hot INSERT path. The cost is confined to DELETE and
TRUNCATE: an AFTER STATEMENT trigger adds nothing to INSERT or UPDATE, and its
per-statement work is one aggregate over the transition table plus one INSERT,
so a bulk delete pays one scan of the rows it was already touching rather than
per-row overhead.

Attachment, therefore: the parent, every existing partition, and teach
``create_cost_events_partition_for`` to attach it to every partition it mints —
otherwise the control silently lapses the first month a new partition appears.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Index,
    Integer,
    Text,
    event,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, INET
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import _utcnow
from .events import CostEvent, MeteringEvent

#: Value written when the caller set no ``billing.delete_reason``.
DELETE_REASON_UNSPECIFIED = "unspecified"

#: The two RANGE/plain financial event ledgers this control covers. Both carry
#: ``id uuid``, ``tenant_id uuid`` and ``created_at timestamptz``, which is what
#: lets a single trigger function serve both.
AUDITED_TABLES: tuple[str, ...] = (
    "billing_cost_events",
    "billing_metering_events",
)


class BillingDeleteAudit(Base):
    """One row per DELETE (or TRUNCATE) STATEMENT against an audited ledger.

    No FKs and no ON DELETE behaviour by design: the whole point is to outlive
    the rows it describes. Nothing in this service ever writes it from Python —
    the only writer is the database trigger.
    """

    __tablename__ = "billing_delete_audit"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        server_default=text("now()"),
    )
    #: The relation the statement actually named — the PARTITION name for a
    #: partition-targeted delete, which is the distinction the n_tup_del
    #: witness could see and nothing else could.
    table_name: Mapped[str] = mapped_column(Text, nullable=False)
    #: NULL for TRUNCATE, which has no transition table to count.
    rows_deleted: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    operation: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text(f"'{DELETE_REASON_UNSPECIFIED}'")
    )
    db_user: Mapped[str] = mapped_column(Text, nullable=False)
    session_user_name: Mapped[str] = mapped_column(Text, nullable=False)
    application_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    client_addr: Mapped[str | None] = mapped_column(INET, nullable=True)
    backend_pid: Mapped[int] = mapped_column(Integer, nullable=False)
    txid: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    #: The exact SQL. This is the field whose absence made #92 unanswerable:
    #: log_statement=ddl captured no DML, so there was no statement to read.
    statement: Mapped[str | None] = mapped_column(Text, nullable=True)
    distinct_tenant_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    #: Capped at 20 — enough to identify the target of a delete, bounded so a
    #: million-row sweep cannot write a million-element array.
    tenant_ids: Mapped[list[str] | None] = mapped_column(
        ARRAY(PGUUID(as_uuid=False)), nullable=True
    )
    sample_ids: Mapped[list[str] | None] = mapped_column(
        ARRAY(PGUUID(as_uuid=False)), nullable=True
    )
    created_at_min: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at_max: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_billing_delete_audit_ts", ts.desc()),
        Index("ix_billing_delete_audit_table_ts", "table_name", ts.desc()),
    )


# ---------------------------------------------------------------------------
# The DDL. ONE definition, consumed by alembic 0016 AND by the metadata
# after_create hook below, so the migration and the schema the tests exercise
# cannot drift. This repo already paid for that drift twice (alembic 0012 and
# 0015 exist only to reconcile two schema sources), so the trigger does not get
# to open a third front.
# ---------------------------------------------------------------------------

#: SECURITY DEFINER so a role that may DELETE from a ledger but has no INSERT
#: on the audit table still lands the record. A caller must not be able to
#: delete without being audited because of a missing grant.
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


#: Attach to every EXISTING partition of billing_cost_events. A no-op where the
#: table is not partitioned (the test schema, built by ``create_all``), which is
#: why it is a dynamic DO block rather than a static list.
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

#: ``create_cost_events_partition_for`` verbatim from alembic 0003 (_FN_BILLING)
#: with ONE addition: the minted partition gets the audit triggers. Without this
#: the control lapses the first month a new partition is created — the parent's
#: trigger does not fire for a delete aimed at a child (measured, see module
#: docstring).
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

#: The whole control, in application order. Alembic 0016 executes exactly this.
DELETE_AUDIT_DDL: tuple[str, ...] = (
    AUDIT_FUNCTION_DDL,
    *(_attach_ddl(t) for t in AUDITED_TABLES),
    ATTACH_TO_EXISTING_PARTITIONS_DDL,
    PARTITION_FN_WITH_AUDIT_DDL,
)


def _install_for(relation: str) -> tuple[str, ...]:
    """The DDL that arms ONE relation, in dependency order."""
    ddl = [AUDIT_FUNCTION_DDL, _attach_ddl(relation)]
    if relation == "billing_cost_events":
        ddl += [ATTACH_TO_EXISTING_PARTITIONS_DDL, PARTITION_FN_WITH_AUDIT_DDL]
    return tuple(ddl)


def _arm(relation: str):  # noqa: ANN202
    def _listener(target, connection, **kw) -> None:  # noqa: ANN001, ARG001
        for stmt in _install_for(relation):
            # psycopg3 reads "%" in a driver-level statement as a placeholder
            # ("only '%s', '%b', '%t' are allowed as placeholders, got '%I'"),
            # and this DDL is full of plpgsql format() specifiers. Escaped here,
            # at the driver boundary, NOT in the DDL constants: alembic's
            # op.execute() does not interpolate (0003 ships %s in this same
            # function body), so doubling them in the constant would put literal
            # "%%" into production.
            connection.exec_driver_sql(stmt.replace("%", "%%"))

    return _listener


# Armed PER TABLE, not on ``Base.metadata``, and this is load-bearing.
#
# The test suite builds its schema with ``create_all`` rather than alembic, so
# without a hook the trigger would exist only in the migration and every test of
# it would be vacuous — passing against a schema that has no trigger in it.
#
# A metadata-level ``after_create`` looked sufficient and was not.
# ``tests/test_cost_partition_maintenance.py`` DROPs ``billing_cost_events``,
# rebuilds it as a real partitioned table for its own assertions, and restores
# it in teardown with ``Table.create`` — which fires the TABLE event and never
# the METADATA one. The table came back unarmed and stayed unarmed for the rest
# of the session, so this module passed alone and failed in the full suite (923
# passed, 1 failed) with "DELETE left no audit row: []". A table-level listener
# arms the relation whichever path creates it.
for _model in (CostEvent, MeteringEvent):
    event.listen(_model.__table__, "after_create", _arm(_model.__tablename__))
