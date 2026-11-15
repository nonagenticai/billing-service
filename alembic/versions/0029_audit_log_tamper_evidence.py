"""audit_log tamper evidence: record every DELETE/TRUNCATE, refuse a real UPDATE

Revision ID: 0029_audit_log_tamper_evidence
Revises: 0028_cost_event_event_time
Create Date: 2026-09-11

``audit_log`` is the table that records who changed money, and measured on the
prod primary 2026-09-11 it held **4,103 rows behind exactly ONE trigger**
(``billing_truncate_guard_trg``). ``billing_delete_audit_trg`` — the #92 forensic
control — is attached to ``billing_metering_events`` and every
``billing_cost_events_*`` partition and was **never** attached here, while
``services/audit.py`` DELETEs on a 180-day retention and self-commits. So the
provenance record had no provenance: a purge or an edit left nothing behind.

⛔ THE OBVIOUS IMPLEMENTATIONS ARE BOTH WRONG, AND BOTH WERE TRIED.

1. ``audit_log`` CANNOT join ``AUDITED_TABLES`` and reuse
   ``billing_audit_deleted_rows()``. That function aggregates ``(array_agg(d.id))``
   into a ``uuid[]`` and reads ``min(d.created_at)``, but ``audit_log.id`` is
   **bigint** and the table has **no created_at — only ts**. plpgsql bodies are
   parsed at EXECUTION, so attaching it would let this migration PASS and then
   break the retention purge at its next run: a cleanup job silently dead, found
   months later. Hence a dedicated function for this table's shape.
   ``tests/test_audit_log_tamper_evidence.py`` reproduces that failure on purpose.

2. The DELETE is NOT excluded for the legitimate purge. ``delete_audit.py``'s own
   docstring already adjudicated that trade: *"an exclusion is an override, and an
   override is the thing an unlogged delete would reach for — a control that can be
   switched off by the caller it is auditing is not a control"*, and an exclusion
   also blinds the audit to retention going WRONG. So the trigger is
   ``FOR EACH STATEMENT ... REFERENCING OLD TABLE`` and the 180-day purge costs
   exactly ONE forensic row however many it removes.

⚠️ THE UPDATE REFUSAL IS KEYED ON DATA, NOT ON CALLER, and that is load-bearing.
A blanket refusal looked safe because nothing in the ORM updates ``audit_log`` —
but ``src/db.py`` replays ``UPDATE audit_log SET tenant_name = tenant_id::text
WHERE tenant_name IS NULL`` on **every pod start**. It is inert today (prod's 200
``tenant_name IS NULL`` rows are exactly its 200 ``tenant_id IS NULL`` rows), and
``apply_additive_migrations`` fails soft, so a blanket refusal would not have
crashed a boot — it would have logged ``additive_migration_failed`` at WARNING on
prod forever while staying invisible in CI, where that table is empty at replay
time. So: ``to_jsonb(NEW) IS NOT DISTINCT FROM to_jsonb(OLD)`` is permitted. A
no-op UPDATE passes; the day that statement would actually stamp a uuid into a
display column it is REFUSED — which is exactly what db.py's own comment says must
never happen, now enforced instead of hoped for.

⚠️ DDL IS INLINED VERBATIM, per this tree's convention that a migration imports
nothing (no file under alembic/versions imports from ``models``). It was GENERATED
from ``models/db/audit_log_guard.py``, not retyped, and
``tests/test_audit_log_tamper_evidence.py::test_the_migration_ddl_has_not_drifted_from_the_module``
asserts byte equality so the copy cannot rot.
"""

from __future__ import annotations

from alembic import op

revision = "0029_audit_log_tamper_evidence"
down_revision = "0028_cost_event_event_time"
branch_labels = None
depends_on = None


#: Applied in this order: both functions, then the forensic triggers, then the
#: immutability trigger. Byte-identical to
#: models.db.audit_log_guard.AUDIT_LOG_GUARD_DDL.
UPGRADE_DDL: tuple[str, ...] = (
    r"""
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
""",
    r"""
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
""",
    r"""
DROP TRIGGER IF EXISTS billing_audit_log_delete_audit_trg ON public.audit_log;
CREATE TRIGGER billing_audit_log_delete_audit_trg
    AFTER DELETE ON public.audit_log
    REFERENCING OLD TABLE AS deleted
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_log_deleted_rows();

DROP TRIGGER IF EXISTS billing_audit_log_truncate_audit_trg ON public.audit_log;
CREATE TRIGGER billing_audit_log_truncate_audit_trg
    AFTER TRUNCATE ON public.audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_log_deleted_rows();
""",
    r"""
DROP TRIGGER IF EXISTS billing_audit_log_immutability_trg ON public.audit_log;
CREATE TRIGGER billing_audit_log_immutability_trg
    BEFORE UPDATE ON public.audit_log
    FOR EACH ROW EXECUTE FUNCTION public.billing_forbid_audit_log_update();
""",
)


#: Triggers first, then the functions they reference. Byte-identical to
#: models.db.audit_log_guard.AUDIT_LOG_GUARD_DROP_DDL.
DOWNGRADE_DDL: tuple[str, ...] = (
    r"""DROP TRIGGER IF EXISTS billing_audit_log_immutability_trg ON public.audit_log;""",
    r"""DROP TRIGGER IF EXISTS billing_audit_log_truncate_audit_trg ON public.audit_log;""",
    r"""DROP TRIGGER IF EXISTS billing_audit_log_delete_audit_trg ON public.audit_log;""",
    r"""DROP FUNCTION IF EXISTS public.billing_forbid_audit_log_update();""",
    r"""DROP FUNCTION IF EXISTS public.billing_audit_log_deleted_rows();""",
)


def upgrade() -> None:
    for stmt in UPGRADE_DDL:
        op.execute(stmt)


def downgrade() -> None:
    for stmt in DOWNGRADE_DDL:
        op.execute(stmt)
