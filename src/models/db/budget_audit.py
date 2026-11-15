"""Forensic audit of token-budget and request-quota CONFIG removal (issue #244).

THE DEFECT
----------
On 2026-09-30 ``scripts/tenant_purge.py --all-tenants --include-config`` hard-
DELETEd netcare's ACTIVE EUR 250 monthly budget from ``token_budgets``. Nothing
recorded it: the #92 control (``delete_audit.py``) armed only the two event
ledgers, and the purge script's own receipt labelled ``token_budgets`` and
``request_quota_config`` "unaudited — counts are the only record". The API path
never hard-deletes (``services/budget_crud.py`` soft-deletes by ``active =
false`` and writes ``audit_log``), so the only removals that could happen were
exactly the ones nobody saw.

WHAT THIS INSTALLS
------------------
* ``token_budgets`` gets the #92 control VERBATIM: ``billing_delete_audit_trg`` /
  ``billing_truncate_audit_trg`` executing ``billing_audit_deleted_rows()``. The
  table carries ``id uuid``, ``tenant_id uuid`` and ``created_at timestamptz``,
  which is the ledger shape that function aggregates, so it fits unchanged.
* ``token_budgets`` also gets ``billing_budget_deactivate_audit_trg``: AFTER
  UPDATE, FOR EACH STATEMENT, with OLD/NEW transition tables joined on ``id``.
  It writes ONE ``billing_delete_audit`` row (``operation = 'DEACTIVATE'``) per
  statement that flipped ``active`` true -> false, and nothing otherwise. A
  deactivated budget refuses spend exactly like a deleted one
  (``budget_redis`` returns ``no_active_budget``), so it is the same event for
  the tenant and is recorded in the same place, whoever issued it. The API
  paths already write ``audit_log`` for their own deactivations; this row is
  the one a raw ``UPDATE`` cannot skip.
* ``request_quota_config`` gets its OWN function,
  ``billing_audit_deleted_quota_config()``. ⛔ It cannot reuse the ledger
  function: its primary key is ``license_id`` and it has NO ``id`` column, and
  a plpgsql body is resolved at EXECUTION, so attaching the ledger function
  would pass the migration and then raise inside the next DELETE (the same trap
  ``audit_log_guard.py`` documents). ``sample_ids`` carries the deleted
  ``license_id`` values instead — they are uuids, and they are the key.

All three functions are SECURITY DEFINER with a pinned ``search_path``, for the
reason ``delete_audit.py`` gives: a caller must not escape the audit for want of
an INSERT grant on ``billing_delete_audit``.

COST
----
The DEACTIVATE trigger fires on EVERY ``UPDATE token_budgets``, including the
debit path's ``consumed_tokens`` writes. A column list (``AFTER UPDATE OF
active``) would avoid that but PostgreSQL refuses transition tables on a trigger
with a column list, and a row-level trigger would cost per row. The per-statement
cost is one join over transition tables holding only the rows that statement
touched, plus nothing when no row flipped.
"""

from __future__ import annotations

from sqlalchemy import event

from .budgets import TokenBudget
from .delete_audit import AUDIT_FUNCTION_DDL as LEDGER_AUDIT_FUNCTION_DDL
from .quota import RequestQuotaConfig

#: Tables armed with the #92 ledger function unchanged.
LEDGER_SHAPED_CONFIG_TABLES: tuple[str, ...] = ("token_budgets",)

#: Operation written for an ``active`` true -> false flip.
DEACTIVATE_OPERATION = "DEACTIVATE"

DEACTIVATE_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_audit_deactivated_budgets()
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
    SELECT count(*),
           (array_agg(DISTINCT n.tenant_id))[1:20],
           count(DISTINCT n.tenant_id),
           (array_agg(n.id))[1:20],
           min(n.created_at),
           max(n.created_at)
      INTO v_rows, v_tenants, v_tenant_n, v_ids, v_created_min, v_created_max
      FROM old_rows o
      JOIN new_rows n ON n.id = o.id
     WHERE o.active AND NOT n.active;

    -- Only a true -> false flip is an event. Debit writes, re-activations and
    -- no-op updates leave no row.
    IF v_rows = 0 THEN
        RETURN NULL;
    END IF;

    INSERT INTO public.billing_delete_audit (
        table_name, rows_deleted, operation, reason, db_user,
        session_user_name, application_name, client_addr, backend_pid,
        txid, statement, distinct_tenant_count, tenant_ids, sample_ids,
        created_at_min, created_at_max
    ) VALUES (
        TG_TABLE_NAME, v_rows, 'DEACTIVATE',
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

QUOTA_CONFIG_AUDIT_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_audit_deleted_quota_config()
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

    -- request_quota_config's SHAPE: the key is license_id (uuid); there is no
    -- id column, which is why the ledger function cannot be attached here.
    SELECT count(*),
           (array_agg(DISTINCT d.tenant_id))[1:20],
           count(DISTINCT d.tenant_id),
           (array_agg(d.license_id))[1:20],
           min(d.created_at),
           max(d.created_at)
      INTO v_rows, v_tenants, v_tenant_n, v_ids, v_created_min, v_created_max
      FROM deleted d;

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


def _ledger_attach_ddl(relation: str) -> str:
    """Idempotent DELETE + TRUNCATE attachment — identical to ``delete_audit._attach_ddl``."""
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


DEACTIVATE_ATTACH_DDL = r"""
DROP TRIGGER IF EXISTS billing_budget_deactivate_audit_trg ON public.token_budgets;
CREATE TRIGGER billing_budget_deactivate_audit_trg
    AFTER UPDATE ON public.token_budgets
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_deactivated_budgets();
"""

QUOTA_CONFIG_ATTACH_DDL = r"""
DROP TRIGGER IF EXISTS billing_quota_config_delete_audit_trg ON public.request_quota_config;
CREATE TRIGGER billing_quota_config_delete_audit_trg
    AFTER DELETE ON public.request_quota_config
    REFERENCING OLD TABLE AS deleted
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_deleted_quota_config();

DROP TRIGGER IF EXISTS billing_quota_config_truncate_audit_trg ON public.request_quota_config;
CREATE TRIGGER billing_quota_config_truncate_audit_trg
    AFTER TRUNCATE ON public.request_quota_config
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_audit_deleted_quota_config();
"""

#: The whole control as alembic 0046 applies it: functions first, then triggers.
#: ``billing_audit_deleted_rows()`` is NOT here — 0016 owns it and it already
#: exists at 0046. The create_all hook below prepends it (see ``_install_for``).
BUDGET_AUDIT_DDL: tuple[str, ...] = (
    DEACTIVATE_FUNCTION_DDL,
    QUOTA_CONFIG_AUDIT_FUNCTION_DDL,
    *(_ledger_attach_ddl(t) for t in LEDGER_SHAPED_CONFIG_TABLES),
    DEACTIVATE_ATTACH_DDL,
    QUOTA_CONFIG_ATTACH_DDL,
)

#: The inverse, triggers first. alembic 0046's downgrade executes exactly this.
BUDGET_AUDIT_DROP_DDL: tuple[str, ...] = (
    "DROP TRIGGER IF EXISTS billing_quota_config_truncate_audit_trg "
    "ON public.request_quota_config;",
    "DROP TRIGGER IF EXISTS billing_quota_config_delete_audit_trg ON public.request_quota_config;",
    "DROP TRIGGER IF EXISTS billing_budget_deactivate_audit_trg ON public.token_budgets;",
    *(
        f"DROP TRIGGER IF EXISTS billing_truncate_audit_trg ON public.{t};"
        for t in LEDGER_SHAPED_CONFIG_TABLES
    ),
    *(
        f"DROP TRIGGER IF EXISTS billing_delete_audit_trg ON public.{t};"
        for t in LEDGER_SHAPED_CONFIG_TABLES
    ),
    "DROP FUNCTION IF EXISTS public.billing_audit_deleted_quota_config();",
    "DROP FUNCTION IF EXISTS public.billing_audit_deactivated_budgets();",
)


def _install_for(relation: str) -> tuple[str, ...]:
    """The DDL that arms ONE relation under ``create_all``, in dependency order."""
    if relation == "token_budgets":
        return (
            DEACTIVATE_FUNCTION_DDL,
            _ledger_attach_ddl(relation),
            DEACTIVATE_ATTACH_DDL,
        )
    if relation == "request_quota_config":
        return (QUOTA_CONFIG_AUDIT_FUNCTION_DDL, QUOTA_CONFIG_ATTACH_DDL)
    return ()


def _arm(relation: str):  # noqa: ANN202
    def _listener(target, connection, **kw) -> None:  # noqa: ANN001, ARG001
        if relation in LEDGER_SHAPED_CONFIG_TABLES:
            # token_budgets may be created before either ledger, whose hook is
            # what installs 0016's function. Install it only if ABSENT, so this
            # hook can never overwrite a body another module installed later.
            exists = connection.exec_driver_sql(
                "SELECT to_regprocedure('public.billing_audit_deleted_rows()')"
            ).scalar()
            if exists is None:
                connection.exec_driver_sql(LEDGER_AUDIT_FUNCTION_DDL.replace("%", "%%"))
        for stmt in _install_for(relation):
            # Same driver-boundary escaping as delete_audit._arm (psycopg3 reads
            # "%" as a placeholder); never in the constants, which alembic
            # passes to the server untouched.
            connection.exec_driver_sql(stmt.replace("%", "%%"))

    return _listener


# Armed PER TABLE for the reason delete_audit.py gives: the suite builds its
# schema with create_all, so without these hooks every test of the triggers
# would run against a schema that has none. This module replaces no function
# another module owns except re-issuing 0016's ledger function byte-identically,
# so its import position is not load-bearing.
for _model in (TokenBudget, RequestQuotaConfig):
    event.listen(_model.__table__, "after_create", _arm(_model.__tablename__))
