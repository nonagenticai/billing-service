"""Tamper-evidence for ``audit_log`` — the provenance record had no provenance.

THE DEFECT, MEASURED ON THE PROD PRIMARY
----------------------------------------
``audit_log`` holds 4,103 live rows and carried exactly ONE trigger,
``billing_truncate_guard_trg`` (alembic 0018's catalog sweep). The forensic
control from #92, ``billing_delete_audit_trg``, is attached to
``billing_metering_events`` and to every ``billing_cost_events_*`` partition and
was **never attached to audit_log**. Meanwhile ``services.audit.purge_old_audit_log``
runs ``DELETE FROM audit_log WHERE ts < cutoff`` on a retention schedule and
self-commits.

So the table that exists to record who changed money could itself be purged or
edited with no trace. Every other financial relation in this database is watched
by the table that was not watching itself.

TWO CONTROLS, AND THEY ARE DELIBERATELY ASYMMETRIC
--------------------------------------------------
``DELETE`` / ``TRUNCATE`` are **RECORDED**, never refused. Retention is a
legitimate DELETE and it is the only one in ``src/``; refusing it would break a
cleanup job, and excluding it would be an override (see below).

``UPDATE`` is **REFUSED**. Unlike the event ledgers, nothing legitimately
rewrites an audit row: the statements against ``AuditLog`` in ``src/`` are
INSERTs from ``services.audit.record``, the single retention DELETE, and SELECTs
(``routers/export.py``, ``routers/audit.py``). An audit row is the record OF a
change; editing one destroys the only evidence of what the value was before, and
``audit_log`` carries no column that could say a row had been edited — so the
edit is indistinguishable from the truth. That is the one thing an audit log must
make impossible, so here a refusal is available where the ledgers could only
afford a recording.

⛔ THE UPDATE REFUSAL IS NOT UNCONDITIONAL, AND THE EXCEPTION IS NOT AN OVERRIDE
-------------------------------------------------------------------------------
⚠️ MEASURED, and it refutes "nothing in this service updates audit_log":
``src/db.py`` ships ``UPDATE audit_log SET tenant_name = tenant_id::text WHERE
tenant_name IS NULL`` in ``ADDITIVE_MIGRATIONS``, executed on **every pod
start**. Its own comment records that it is a VALUE no-op today — prod has 200
rows with ``tenant_name IS NULL`` and those are EXACTLY the 200 with ``tenant_id
IS NULL``, so it writes NULL over NULL — and that NULL is meaningful ("this event
had no tenant"), which is why that comment forbids turning it into ``''``.

A blanket row-level refusal would therefore fire 200 times on every boot. It
would not crash startup (``apply_additive_migrations`` runs each statement on an
AUTOCOMMIT connection and fails soft, so the refusal cannot abort the statements
after it) but it would log ``additive_migration_failed`` at WARNING forever —
poisoning the one signal that is supposed to mean "a schema change did not
apply" — and it would be invisible in the suite, where ``audit_log`` is empty
when that statement runs, so nothing would have gone red.

So the predicate is on the DATA, not on the caller::

    to_jsonb(NEW) IS NOT DISTINCT FROM to_jsonb(OLD)  ->  permitted

A statement that leaves the row byte-identical has not mutated anything; there
is nothing to be evidence of. Anything that moves a single byte raises. This is
NOT the kind of exclusion ``delete_audit.py``'s docstring rejected: it is not a
session GUC, a role check or a statement-text test, so the caller being audited
cannot switch it off — it cannot even tell that the guard is there. And it is
strictly tighter than today: the moment that startup statement STOPS being a
no-op (an audit row with ``tenant_id`` set and ``tenant_name`` NULL), it is
refused — which is precisely the laundering ``src/db.py``'s comment says must
never happen, enforced by the database instead of by a comment. The answer then
is to delete that statement, as that comment already instructs; it is not to
widen this guard.

⚠️ UPDATED 2026-09-15 — THAT STATEMENT IS GONE, AND THIS GUARD IS WHY. Its
precondition broke. Measured on the prod billing DB: ``audit_log`` holds 4,186
rows, 286 with ``tenant_name IS NULL``, and **55 of those have ``tenant_id``
SET** — so it stopped being a value no-op and started being REFUSED here, on
every pod start, softly, at ``additive_migration_failed`` WARNING. It was
therefore DELETED from ``db.ADDITIVE_MIGRATIONS``; the full record sits in
``src/db.py`` just above ``ix_audit_log_tenant_name``. ⛔ **Nothing in ``src/``
updates ``audit_log`` any more**, and
``tests/test_audit_log_tamper_evidence.py::test_no_startup_statement_updates_audit_log``
is what keeps that true. Read the two paragraphs above as the history that
produced this design, not as a live claim that a startup statement still runs.

⛔ This is NOT licence to simplify the predicate into a blanket refusal now that
its one known exception is gone. The predicate is on the DATA so that the caller
being audited cannot switch it off or even detect it; the DDL below is pinned
byte-identical to alembic 0029 by
``test_the_migration_ddl_has_not_drifted_from_the_module``; and the refusal that
this deletion answers is the proof the design works, not a cost it imposed.

``IS NOT DISTINCT FROM`` rather than ``=`` so a NULL on either side compares as a
value. The comparison is whole-row ``to_jsonb`` rather than a column list for the
reason ``invoice_immutability.py`` spells out: a column list is a denylist that
silently stops covering the next ``ADD COLUMN``.

STATEMENT-LEVEL, AND NOTHING IS EXCLUDED
----------------------------------------
The forensic trigger is ``FOR EACH STATEMENT ... REFERENCING OLD TABLE``, so the
180-day retention purge costs exactly ONE audit row however many it removes. That
is ``delete_audit.py``'s adjudicated choice ("record it compactly" over "exclude
retention via a session GUC / role check") and the three reasons it gives apply
here unchanged, the first most of all: a control that can be switched off by the
caller it is auditing is not a control. An exclusion would also blind the audit
to retention going WRONG — a bad ``AUDIT_RETENTION_DAYS`` deleting a year of
audit history looks exactly like a normal sweep.

⛔ WHY A DEDICATED FUNCTION AND NOT ONE MORE ENTRY IN ``AUDITED_TABLES``
-----------------------------------------------------------------------
This is the trap, and it would have shipped green. ``delete_audit.py``'s
``public.billing_audit_deleted_rows()`` aggregates the LEDGER shape out of its
transition table::

    (array_agg(d.id))[1:20]  INTO v_ids uuid[]
    min(d.created_at), max(d.created_at)

``audit_log`` has ``id bigint`` (which cannot be assigned into ``uuid[]``) and no
``created_at`` column at all — its timestamp is ``ts``. A plpgsql body is parsed
at EXECUTION, not at ``CREATE FUNCTION``, so attaching that trigger would make
the migration PASS and then break the retention purge at its next run: a silent
breakage of a cleanup job, found months later. ``audit_log`` therefore gets its
own function, and
``tests/test_audit_log_tamper_evidence.py::test_the_ledger_audit_function_cannot_read_audit_logs_shape``
proves the hazard is real rather than asserting it in prose, while
``::test_the_delete_trigger_on_audit_log_points_at_the_audit_log_function``
catches a future catalog-driven widening (0018's shape) that re-attaches the
LEDGER function here and reopens it.

WHERE THE RECORD GOES, AND THE ONE FIELD IT CANNOT CARRY
--------------------------------------------------------
Into the existing ``billing_delete_audit``. A second audit table would be a
control with no detector: ``services.ledger_guard_audit`` already reads that
table, groups by ``operation`` and publishes ``billing_delete_audit_rows``, so a
purge of ``audit_log`` becomes a number on a dashboard the day this ships, and
``table_name`` keeps it distinguishable from the ledgers' rows. The column types
fit the shape with one exception:

* ``tenant_id uuid`` -> ``tenant_ids uuid[]`` / ``distinct_tenant_count``: fits.
* ``min(ts)/max(ts)`` -> ``created_at_min``/``created_at_max``: fits. The column
  names say ``created_at`` because the ledgers' timestamp is called that;
  ``unrolled_guard.py`` already writes a differently-named timestamp into them.
* ⚠️ ``sample_ids`` is ``uuid[]`` and ``audit_log.id`` is ``bigint``: it does NOT
  fit and is left **NULL**. The id window is carried in ``reason`` instead, as
  ``id range [min, max]`` — the same column ``unrolled_guard.py`` uses to carry a
  formatted summary ahead of the operator's own reason, because widening
  ``billing_delete_audit`` is ``delete_audit.py``'s change to make, not this
  module's. What is lost against a ledger audit row is the list of up to 20
  individual ids; ``rows_deleted`` + the id range + the ``ts`` window + the
  tenant sample + the statement text remain, and for the tampering shape that
  matters most — a single audit row deleted to hide one change — that set
  identifies the row exactly.

COST
----
Nothing on the hot path. ``audit_log``'s only high-frequency statement is the
INSERT from ``services.audit.record`` (one row per mutating request on the write
surface), and neither trigger fires on INSERT. The forensic trigger's work is one
aggregate over a transition table the DELETE was already materialising plus one
INSERT, so the retention sweep pays one extra scan of rows it is removing anyway,
not per-row overhead. The refusal computes ``to_jsonb`` twice per updated row,
which is twice for each of the ~200 rows ``src/db.py``'s backfill can still touch
at a pod start and zero the rest of the time.

RESIDUAL GAPS, NAMED
--------------------
* A DROP TABLE, an ``ALTER TABLE ... DROP TRIGGER``, or a superuser detaching
  these triggers is invisible here, as it is for every other guard in this
  package. The attachment count is what ``services.ledger_guard_audit`` watches,
  and it currently counts only ``billing_truncate_guard_trg``; extending it to
  these trigger names is a change to ``src/services/`` and is not in this module.
* ⚠️ The two schema sources disagree about TRUNCATE on ``audit_log``, and this
  module does not change that. In PROD, 0018's catalog sweep put
  ``billing_truncate_guard_trg`` on ``audit_log``, so a TRUNCATE is REFUSED
  unless ``billing.allow_truncate`` is armed. Under ``create_all`` (the suite's
  schema) ``truncate_guard.py`` arms only the two ledgers and the two invoice
  tables, so ``TRUNCATE audit_log`` is permitted there. The AFTER TRUNCATE
  forensic trigger below is attached by BOTH sources and records whatever gets
  through either.
* An UPDATE matching zero rows raises nothing, because a ``FOR EACH ROW`` trigger
  never fires. Correct: nothing was changed.
"""

from __future__ import annotations

from sqlalchemy import event

from .audit import AuditLog

#: The relation this module guards. A tuple, mirroring the other guard modules,
#: so the migration and the create_all hook iterate the same list — but there is
#: exactly one member and there is no partition loop, because ``audit_log`` is a
#: plain table. Nothing in this module touches
#: ``create_cost_events_partition_for``.
GUARDED_AUDIT_TABLES: tuple[str, ...] = ("audit_log",)

#: Prefix on the ``reason`` column of a recorded DELETE. Exported so a test can
#: assert the forensics without re-typing the format string; kept in step with
#: the DDL by the import-time check at the bottom of this module.
AUDIT_LOG_DELETE_REASON_PREFIX = "audit_log delete:"

#: Prefix on the ``reason`` column of a recorded TRUNCATE. A TRUNCATE has no
#: transition table, so the row count, id range and ts window are genuinely
#: unknowable there and the reason says so rather than leaving a reader to guess
#: whether a NULL ``rows_deleted`` means "none" or "not measured".
AUDIT_LOG_TRUNCATE_REASON_PREFIX = "audit_log truncate:"


# ---------------------------------------------------------------------------
# 1. The forensic function. ONE definition, consumed by the alembic migration
#    AND by the after_create hook at the bottom, so the migration and the schema
#    the tests exercise cannot drift.
# ---------------------------------------------------------------------------

#: SECURITY DEFINER for the reason ``delete_audit.py`` gives: a role that may
#: DELETE from ``audit_log`` but holds no INSERT on ``billing_delete_audit`` must
#: still land the record. A caller must never be able to delete WITHOUT being
#: audited because of a missing grant — "the INSERT was refused" and "the DELETE
#: was never audited" are the same silence. ``SET search_path = public, pg_temp``
#: because a SECURITY DEFINER function without a pinned search_path can be
#: redirected at a shadowing relation by the very caller it is auditing.
#:
#: The companion refusal function below is deliberately NOT SECURITY DEFINER: it
#: only RAISEs, writes nothing, and needs no privilege it does not already have —
#: exactly as ``billing_forbid_truncate()`` and
#: ``billing_forbid_invoice_mutation()`` are plain SECURITY INVOKER.
AUDIT_LOG_FORENSICS_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_audit_log_deleted_rows()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
DECLARE
    v_rows     bigint;
    v_tenants  uuid[];
    v_tenant_n integer;
    v_id_min   bigint;
    v_id_max   bigint;
    v_ts_min   timestamptz;
    v_ts_max   timestamptz;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        INSERT INTO public.billing_delete_audit (
            table_name, rows_deleted, operation, reason, db_user,
            session_user_name, application_name, client_addr, backend_pid,
            txid, statement
        ) VALUES (
            TG_TABLE_NAME, NULL, TG_OP,
            format(
                'audit_log truncate: row count, id range and ts window are '
                'unavailable (TRUNCATE has no transition table). operator reason: %s',
                coalesce(nullif(current_setting('billing.delete_reason', true), ''),
                         'unspecified')),
            current_user, session_user,
            nullif(current_setting('application_name', true), ''),
            inet_client_addr(), pg_backend_pid(),
            pg_current_xact_id_if_assigned()::text::bigint,
            left(current_query(), 8000)
        );
        RETURN NULL;
    END IF;

    -- ⛔ audit_log's SHAPE, not the ledgers'. id is bigint (so it cannot be
    -- aggregated into billing_delete_audit.sample_ids, which is uuid[]) and the
    -- timestamp column is ts, not created_at. Aggregating d.id and d.created_at
    -- here -- which is what public.billing_audit_deleted_rows() does -- parses
    -- fine at CREATE FUNCTION time and fails at DELETE time, i.e. inside the
    -- retention purge, months after the migration went green.
    SELECT count(*),
           (array_agg(DISTINCT d.tenant_id))[1:20],
           count(DISTINCT d.tenant_id),
           min(d.id),
           max(d.id),
           min(d.ts),
           max(d.ts)
      INTO v_rows, v_tenants, v_tenant_n, v_id_min, v_id_max, v_ts_min, v_ts_max
      FROM deleted d;

    -- A DELETE whose predicate matched nothing is not an event worth a row.
    -- Same rule as the ledger audit, and it is what keeps the idempotent second
    -- retention run (which deletes zero) from writing a second forensic row.
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
        -- The id window goes in reason because billing_delete_audit has no
        -- bigint id column and widening that table belongs to the module that
        -- owns it. unrolled_guard.py already carries a formatted summary here
        -- ahead of the operator's own reason; same shape, same column.
        format(
            'audit_log delete: %s row(s), id range [%s, %s]. operator reason: %s',
            v_rows, v_id_min, v_id_max,
            coalesce(nullif(current_setting('billing.delete_reason', true), ''),
                     'unspecified')),
        current_user, session_user,
        nullif(current_setting('application_name', true), ''),
        inet_client_addr(), pg_backend_pid(),
        pg_current_xact_id_if_assigned()::text::bigint,
        left(current_query(), 8000),
        v_tenant_n, v_tenants,
        -- ⚠️ NULL, explicitly: sample_ids is uuid[] and audit_log.id is bigint.
        -- Written out rather than omitted from the column list so a reader of
        -- this row knows the NULL is a type limit, not a trigger that failed to
        -- collect. The ids are in reason, above.
        NULL,
        v_ts_min, v_ts_max
    );
    RETURN NULL;
END
$fn$;
"""


# ---------------------------------------------------------------------------
# 2. The UPDATE refusal.
# ---------------------------------------------------------------------------

AUDIT_LOG_IMMUTABILITY_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_audit_log_update()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_old jsonb;
    v_new jsonb;
BEGIN
    -- ⛔ THIS FUNCTION BODY CARRIES NO PERCENT SIGN, DELIBERATELY, for the
    -- reason billing_forbid_invoice_mutation() states: this DDL ships both
    -- through alembic's op.execute() (which passes the text to the server
    -- untouched) and through exec_driver_sql() from the create_all hook below,
    -- where psycopg3 reads a percent sign as a parameter placeholder. With none
    -- in the body both paths carry byte-identical SQL and there is no way to
    -- land a "too many parameters specified for RAISE" that would surface at
    -- refusal time rather than install time. Messages are built with || and
    -- handed to RAISE as USING MESSAGE/DETAIL/HINT, never a format string, and
    -- every interpolated value is coalesce()d because '||' with a NULL operand
    -- yields NULL and USING MESSAGE = NULL raises 22004 instead of the refusal
    -- the caller has to be able to read.
    v_old := to_jsonb(OLD);
    v_new := to_jsonb(NEW);

    -- A statement that leaves the row byte-identical has not mutated anything,
    -- so there is nothing for an audit to be evidence of. This is what keeps
    -- src/db.py's startup backfill
    --   UPDATE audit_log SET tenant_name = tenant_id::text WHERE tenant_name IS NULL
    -- working: on every row it can match today, tenant_id is NULL too, so it
    -- writes NULL over NULL. The day it would write a real uuid into the
    -- display column it is REFUSED -- which is exactly the laundering the
    -- comment above that statement says must never happen.
    --
    -- IS NOT DISTINCT FROM, not '=', so a NULL on either side compares as a
    -- value. The predicate is on the DATA and never on the caller: no GUC, no
    -- role test, no statement-text test, so the writer being audited cannot
    -- switch it off.
    IF v_new IS NOT DISTINCT FROM v_old THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION
        USING
            ERRCODE = 'P0001',
            MESSAGE =
                'UPDATE of audit_log row ' || coalesce(OLD.id::text, '<NULL>')
                || ' (action ' || coalesce(OLD.action, '<NULL>') || ', recorded '
                || coalesce(OLD.ts::text, '<NULL>') || ') is blocked',
            -- Both sides are to_jsonb() of the SAME table, so the key sets are
            -- identical and comparing OLD's keys against NEW's values finds
            -- every difference, including a column that became NULL.
            DETAIL =
                'changed: '
                || (SELECT coalesce(string_agg(o.key, ', ' ORDER BY o.key), '(none)')
                      FROM jsonb_each(v_old) AS o
                     WHERE v_new -> o.key IS DISTINCT FROM o.value),
            HINT =
                'An audit row is the record OF a change, not a row that holds current '
                'state: it is the only evidence of who moved this tenant''s money and '
                'what the value was before. audit_log has no column that could say a '
                'row had been edited, so an edited row is indistinguishable from the '
                'truth -- which is the one thing an audit log exists to make '
                'impossible. APPEND a new audit row describing the correction instead '
                '(services.audit.record is the only writer); never edit one. A DELETE '
                'is NOT refused here -- 180-day retention legitimately purges -- but '
                'it IS recorded into billing_delete_audit by '
                'billing_audit_log_deleted_rows(). If you are seeing this from the '
                'startup backfill in src/db.py (UPDATE audit_log SET tenant_name = '
                'tenant_id::text WHERE tenant_name IS NULL), that statement has '
                'stopped being a value no-op and is about to stamp a uuid into a '
                'display column: DELETE the statement, as the comment above it '
                'instructs. Do not widen this guard.';
END
$fn$;
"""


# ---------------------------------------------------------------------------
# 3. Attachment. Idempotent (DROP IF EXISTS then CREATE), the shape
#    delete_audit._attach_ddl and invoice_immutability._immutability_attach_ddl
#    both use.
# ---------------------------------------------------------------------------


def _forensics_attach_ddl(relation: str) -> str:
    """Idempotent DELETE + TRUNCATE forensic attachment for one relation.

    FOR EACH STATEMENT, so the retention purge costs ONE row however many it
    removes. REFERENCING OLD TABLE on the DELETE half only: TRUNCATE has no
    transition table, which is why ``rows_deleted`` is NULL there.
    """
    return f"""
DROP TRIGGER IF EXISTS billing_audit_log_delete_audit_trg ON public.{relation};
CREATE TRIGGER billing_audit_log_delete_audit_trg
    AFTER DELETE ON public.{relation}
    REFERENCING OLD TABLE AS deleted
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_log_deleted_rows();

DROP TRIGGER IF EXISTS billing_audit_log_truncate_audit_trg ON public.{relation};
CREATE TRIGGER billing_audit_log_truncate_audit_trg
    AFTER TRUNCATE ON public.{relation}
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_log_deleted_rows();
"""


def _immutability_attach_ddl(relation: str) -> str:
    """Idempotent BEFORE UPDATE refusal for one relation.

    FOR EACH ROW, because the refusal compares OLD against NEW. A statement-level
    trigger cannot see an individual row's before-image, so it could not
    distinguish the value no-op above from a real edit.
    """
    return f"""
DROP TRIGGER IF EXISTS billing_audit_log_immutability_trg ON public.{relation};
CREATE TRIGGER billing_audit_log_immutability_trg
    BEFORE UPDATE ON public.{relation}
    FOR EACH ROW EXECUTE FUNCTION public.billing_forbid_audit_log_update();
"""


#: ⛔ THE WHOLE CONTROL, IN APPLICATION ORDER. The alembic migration executes
#: exactly this tuple, in exactly this order: both functions first, then the
#: triggers that reference them. Nothing here replaces a function another module
#: owns, so this tuple is order-independent against the rest of the package — the
#: only internal requirement is functions-before-triggers.
AUDIT_LOG_GUARD_DDL: tuple[str, ...] = (
    AUDIT_LOG_FORENSICS_FUNCTION_DDL,
    AUDIT_LOG_IMMUTABILITY_FUNCTION_DDL,
    *(_forensics_attach_ddl(t) for t in GUARDED_AUDIT_TABLES),
    *(_immutability_attach_ddl(t) for t in GUARDED_AUDIT_TABLES),
)


#: The inverse, in reverse dependency order: triggers first, then the functions
#: they reference. The migration's downgrade executes exactly this.
AUDIT_LOG_GUARD_DROP_DDL: tuple[str, ...] = (
    *(
        f"DROP TRIGGER IF EXISTS billing_audit_log_immutability_trg ON public.{t};"
        for t in GUARDED_AUDIT_TABLES
    ),
    *(
        f"DROP TRIGGER IF EXISTS billing_audit_log_truncate_audit_trg ON public.{t};"
        for t in GUARDED_AUDIT_TABLES
    ),
    *(
        f"DROP TRIGGER IF EXISTS billing_audit_log_delete_audit_trg ON public.{t};"
        for t in GUARDED_AUDIT_TABLES
    ),
    "DROP FUNCTION IF EXISTS public.billing_forbid_audit_log_update();",
    "DROP FUNCTION IF EXISTS public.billing_audit_log_deleted_rows();",
)


# ---------------------------------------------------------------------------
# The two reason prefixes are exported for tests AND written as literals inside
# the DDL (the DDL must stay byte-identical to the migration's frozen copy, so it
# cannot interpolate Python). Fail at IMPORT if they drift — the same idiom
# truncate_guard._with_guard uses, and the alternative is a test asserting a
# string the trigger no longer writes.
# ---------------------------------------------------------------------------
for _prefix in (AUDIT_LOG_DELETE_REASON_PREFIX, AUDIT_LOG_TRUNCATE_REASON_PREFIX):
    if _prefix not in AUDIT_LOG_FORENSICS_FUNCTION_DDL:
        raise RuntimeError(
            f"audit_log_guard: {_prefix!r} is not in "
            "AUDIT_LOG_FORENSICS_FUNCTION_DDL. The exported prefix and the reason "
            "the trigger actually writes have drifted; fix the constant or the DDL "
            "rather than the test that reads it."
        )


def _install_for(relation: str) -> tuple[str, ...]:
    """The DDL that arms ONE relation under ``create_all``, in dependency order."""
    if relation not in GUARDED_AUDIT_TABLES:
        return ()
    return (
        AUDIT_LOG_FORENSICS_FUNCTION_DDL,
        AUDIT_LOG_IMMUTABILITY_FUNCTION_DDL,
        _forensics_attach_ddl(relation),
        _immutability_attach_ddl(relation),
    )


def _arm(relation: str):  # noqa: ANN202
    def _listener(target, connection, **kw) -> None:  # noqa: ANN001, ARG001
        for stmt in _install_for(relation):
            # Same driver-boundary escaping as delete_audit._arm: psycopg3 reads
            # "%" in a driver-level statement as a placeholder, and the forensic
            # function is full of format() specifiers. Escaped HERE, never in the
            # constants, because alembic's op.execute() does not interpolate --
            # doubling them in the constant would put literal "%%" into
            # production. A no-op for the refusal function, which carries no
            # percent sign at all.
            connection.exec_driver_sql(stmt.replace("%", "%%"))

    return _listener


# Armed PER TABLE, for the reason delete_audit.py spells out: the suite builds
# its schema with ``create_all`` rather than alembic, so without this hook the
# triggers would exist only in the migration and every test of them would be
# vacuous — passing against a schema that has no trigger in it.
#
# ⛔ THIS MODULE REGISTERS NOTHING THAT CAN DISTURB THE PARTITION-FUNCTION CHAIN,
# and that is the one ordering constraint in this package. ``.delete_audit``,
# ``.truncate_guard`` and ``.unrolled_guard`` each CREATE OR REPLACE
# ``create_cost_events_partition_for``, each body a superset of the last, so they
# must be imported in that order or a minted partition loses its triggers. The
# listener here hangs off ``AuditLog.__table__`` alone, owns both of its
# functions outright, replaces nothing anyone else defines, and never mentions
# ``create_cost_events_partition_for`` — ``audit_log`` is not partitioned and has
# no minting function. So the import position of this module in
# ``models/db/__init__.py`` is NOT load-bearing, and it is placed where ruff's
# isort puts it. If ``audit_log`` is ever partitioned and gains a minting
# function, re-read the ordering comments in ``__init__.py`` before touching this.
event.listen(AuditLog.__table__, "after_create", _arm(AuditLog.__tablename__))
