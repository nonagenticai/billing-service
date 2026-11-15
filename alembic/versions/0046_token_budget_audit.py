"""Audit token_budgets / request_quota_config removal (issue #244).

WHAT IS BROKEN
--------------
2026-09-30: ``scripts/tenant_purge.py --all-tenants --include-config``
hard-DELETEd netcare's ACTIVE EUR 250 monthly budget from ``token_budgets``
with no audit trail. 0016 armed only the two event ledgers; the purge receipt
itself labelled both config tables "unaudited — counts are the only record".

WHAT THIS INSTALLS
------------------
* ``token_budgets``: 0016's ``billing_audit_deleted_rows()`` attached exactly as
  0016 attaches it (``billing_delete_audit_trg`` / ``billing_truncate_audit_trg``,
  FOR EACH STATEMENT, REFERENCING OLD TABLE AS deleted). The table has the
  ledger shape (``id uuid``, ``tenant_id uuid``, ``created_at``).
* ``token_budgets``: ``billing_audit_deactivated_budgets()`` +
  ``billing_budget_deactivate_audit_trg`` (AFTER UPDATE, FOR EACH STATEMENT,
  OLD/NEW transition tables joined on id): ONE ``billing_delete_audit`` row with
  ``operation = 'DEACTIVATE'`` per statement that flips ``active`` true -> false.
* ``request_quota_config``: its OWN function
  ``billing_audit_deleted_quota_config()`` + ``billing_quota_config_delete_audit_trg``
  / ``billing_quota_config_truncate_audit_trg``. The ledger function CANNOT be
  attached: the table has no ``id`` column (PK is ``license_id``) and a plpgsql
  body is resolved at execution, so it would migrate green and raise inside the
  next DELETE. ``sample_ids`` carries ``license_id`` instead.

Rationale and the create_all twin live in ``src/models/db/budget_audit.py``. The
DDL is duplicated here verbatim so the migration stays frozen;
``tests/test_budget_audit_trigger.py`` asserts the two copies are identical.

LOCK DISCIPLINE
---------------
``CREATE TRIGGER`` takes ``ShareRowExclusiveLock``, which conflicts with the
``RowExclusiveLock`` held by the debit path's ``UPDATE token_budgets``. Same
bounded-wait treatment as 0016: ``SET LOCAL lock_timeout`` and a clean, whole
rollback rather than queueing the debit path behind DDL.

Revision ID: 0046_token_budget_audit
Revises: 0045_money_multi_currency
Create Date: 2026-10-02
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy.exc import DBAPIError

revision: str = "0046_token_budget_audit"
down_revision: str | None = "0045_money_multi_currency"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# --- DDL, kept identical to src/models/db/budget_audit.py -------------------
# A test asserts this equality. Edit both or neither.

LEDGER_SHAPED_CONFIG_TABLES: tuple[str, ...] = ("token_budgets",)

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
    """Idempotent DELETE + TRUNCATE attachment — identical to 0016's ``_attach_ddl``."""
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

BUDGET_AUDIT_DDL: tuple[str, ...] = (
    DEACTIVATE_FUNCTION_DDL,
    QUOTA_CONFIG_AUDIT_FUNCTION_DDL,
    *(_ledger_attach_ddl(t) for t in LEDGER_SHAPED_CONFIG_TABLES),
    DEACTIVATE_ATTACH_DDL,
    QUOTA_CONFIG_ATTACH_DDL,
)

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

# --- end shared DDL ---------------------------------------------------------


# --- lock discipline (0016's, same values and reasons) ----------------------

LOCK_TIMEOUT = "3s"
STATEMENT_TIMEOUT = "60s"
_TIMEOUT_SQLSTATES = frozenset({"55P03", "57014"})

_RETRY_IN_A_QUIETER_WINDOW = f"""
alembic 0046 could not acquire its DDL locks within {LOCK_TIMEOUT}. The whole
migration has ROLLED BACK — nothing was applied, and it is safe to re-run.

Something held a conflicting lock on token_budgets or request_quota_config
(the budget debit path UPDATEs token_budgets). Find it with:

    SELECT pid, state, xact_start, wait_event_type, left(query, 120)
      FROM pg_stat_activity
     WHERE datname = current_database() AND xact_start IS NOT NULL
     ORDER BY xact_start;

RETRY IN A QUIETER WINDOW. Do NOT raise LOCK_TIMEOUT: the lock queue is FIFO,
so while this waits every new budget debit queues behind it.
""".strip()


def _bound_the_wait() -> None:
    """Bound the lock wait and prove the bound took (SET LOCAL outside a txn is a no-op)."""
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0046 refuses to run without a bounded lock_timeout: SET LOCAL "
            f"reported lock_timeout={effective!r}, so this is not inside a transaction "
            "block and the DDL below could queue the budget debit path indefinitely."
        )


def _fail_fast(fn):  # noqa: ANN001, ANN202
    try:
        fn()
    except DBAPIError as exc:
        if getattr(exc.orig, "sqlstate", None) in _TIMEOUT_SQLSTATES:
            raise RuntimeError(_RETRY_IN_A_QUIETER_WINDOW) from exc
        raise


def _upgrade_ddl() -> None:
    for stmt in BUDGET_AUDIT_DDL:
        op.execute(stmt)


def _downgrade_ddl() -> None:
    # Drops exactly what upgrade created. 0016's billing_audit_deleted_rows()
    # and billing_delete_audit are 0016's and are left alone; so are the audit
    # rows this control wrote.
    for stmt in BUDGET_AUDIT_DROP_DDL:
        op.execute(stmt)


def upgrade() -> None:
    _bound_the_wait()
    _fail_fast(_upgrade_ddl)


def downgrade() -> None:
    _bound_the_wait()
    _fail_fast(_downgrade_ddl)
