"""EUR money budgets, FX rates, budget_unit, and cost-event consumption columns.

Tenant inference provisioning (PLAN rev E, §2.1 / §3.1 / §3.2, ledger T-24).

WHAT THIS ADDS (all additive, forward-only)
-------------------------------------------
- ``tenant_money_budget`` — one ACTIVE whole-EUR budget per tenant (partial
  unique on ``tenant_id WHERE active``). ``consumed`` is EUR at 8 dp and is
  period-anchored by ``consumed_period_start`` exactly like
  ``token_budgets.consumed_tokens`` (services.budget_period).
- ``fx_rate`` — owner-set, effective-dated USD->EUR rates. A missing row is a
  REFUSAL (``fx_rate_missing``), never a guessed rate.
- ``tenant_config.budget_unit`` ('tokens' | 'money', default 'tokens') — the
  switch that routes a tenant's settlement to the money ledger.
- ``tenant_config.markup_pct`` DROP NOT NULL — so ``PUT /api/budget/money``
  can create the tenant's config row (to flip ``budget_unit``) WITHOUT writing a
  markup nobody chose. NULL means "not configured": ``services.billing``
  ``get_markup_pct_with_source`` already falls through to the DB default /
  settings on NULL, exactly as for a tenant with no row at all.
- ``billing_cost_events.consumption_amount / consumption_currency / llm_setup``
  — the per-event EUR consumption and the setup it was computed under. NULL on
  every legacy (token-path) row, permanently; there is no backfill.

The truncate guard is attached to both new tables with 0043's catalog sweep
(ATTACH-IF-ABSENT), so neither is born unguarded.

Mirrored in ``src/db.py`` ADDITIVE_MIGRATIONS (the flusher NAMES the three new
cost-event columns in every INSERT; a pod ahead of this migration must still
ingest).

Revision ID: 0044_money_budgets
Revises: 0043_truncate_guard_reattach
Create Date: 2026-10-02
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0044_money_budgets"
down_revision: str | None = "0043_truncate_guard_reattach"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Byte-identical to the entries in src/db.py ADDITIVE_MIGRATIONS.
CREATE_MONEY_BUDGET = (
    "CREATE TABLE IF NOT EXISTS tenant_money_budget ("
    "id UUID PRIMARY KEY, "
    "tenant_id UUID NOT NULL, "
    "currency CHAR(3) NOT NULL, "
    "amount BIGINT NOT NULL, "
    "period VARCHAR(10) NOT NULL DEFAULT 'monthly', "
    "consumed NUMERIC(20, 8) NOT NULL DEFAULT 0, "
    "consumed_period_start DATE, "
    "active BOOLEAN NOT NULL DEFAULT true, "
    "source VARCHAR(32) NOT NULL DEFAULT 'manual', "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT ck_money_budget_currency CHECK (currency = 'EUR'), "
    "CONSTRAINT ck_money_budget_amount CHECK (amount >= 0), "
    "CONSTRAINT ck_money_budget_source CHECK "
    "(source IN ('provisioning', 'conversion', 'manual')))"
)
CREATE_MONEY_BUDGET_ACTIVE_UNIQUE = (
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_money_budget_active_tenant "
    "ON tenant_money_budget (tenant_id) WHERE active"
)
CREATE_FX_RATE = (
    "CREATE TABLE IF NOT EXISTS fx_rate ("
    "id BIGSERIAL PRIMARY KEY, "
    "base CHAR(3) NOT NULL DEFAULT 'USD', "
    "quote CHAR(3) NOT NULL DEFAULT 'EUR', "
    "rate NUMERIC(20, 10) NOT NULL, "
    "effective_from TIMESTAMP WITH TIME ZONE NOT NULL, "
    "effective_until TIMESTAMP WITH TIME ZONE, "
    "set_by TEXT, "
    "CONSTRAINT ck_fx_rate_positive CHECK (rate > 0))"
)
CREATE_FX_RATE_LOOKUP = (
    "CREATE INDEX IF NOT EXISTS ix_fx_rate_lookup ON fx_rate (base, quote, effective_from)"
)
ADD_BUDGET_UNIT = (
    "ALTER TABLE tenant_config ADD COLUMN IF NOT EXISTS budget_unit VARCHAR(8) "
    "NOT NULL DEFAULT 'tokens' "
    "CONSTRAINT ck_tenant_config_budget_unit CHECK (budget_unit IN ('tokens', 'money'))"
)
RELAX_TENANT_MARKUP_PCT = "ALTER TABLE tenant_config ALTER COLUMN markup_pct DROP NOT NULL"
ADD_CONSUMPTION_AMOUNT = (
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS consumption_amount NUMERIC(20, 8)"
)
ADD_CONSUMPTION_CURRENCY = (
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS consumption_currency CHAR(3)"
)
ADD_LLM_SETUP = (
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS llm_setup SMALLINT "
    "CONSTRAINT ck_cost_events_llm_setup CHECK (llm_setup IN (1, 2, 3))"
)

UPGRADE_DDL: tuple[str, ...] = (
    CREATE_MONEY_BUDGET,
    CREATE_MONEY_BUDGET_ACTIVE_UNIQUE,
    CREATE_FX_RATE,
    CREATE_FX_RATE_LOOKUP,
    ADD_BUDGET_UNIT,
    RELAX_TENANT_MARKUP_PCT,
    ADD_CONSUMPTION_AMOUNT,
    ADD_CONSUMPTION_CURRENCY,
    ADD_LLM_SETUP,
)


#: 0017's guard function must exist; identical to 0043's precondition.
REQUIRE_GUARD_FUNCTION_DDL = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NULL THEN
        RAISE EXCEPTION
            'alembic 0044 requires public.billing_forbid_truncate() from 0017'
            USING HINT =
                'The guard function is missing, so there is nothing to attach. '
                'Run alembic 0017 first, or downgrade past 0018/0044 as well.';
    END IF;
END
$do$;
"""


#: 0043's catalog sweep, VERBATIM (0043:82) -- ATTACH-IF-ABSENT, so it arms the
#: two tables born here and touches nothing already guarded.
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
           -- ⛔ The idempotency predicate AND the promise not to disturb any
           -- relation already armed (by 0018 or a prior run of 0043/0044).
           -- Attach only where the trigger is ABSENT; never drop and recreate
           -- one that already exists.
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

    RAISE NOTICE 'alembic 0044: armed % newly-guarded relation(s)', armed;
END
$do$;
"""


REFUSE_NULL_MARKUP_ON_DOWNGRADE_DDL = r"""
DO $do$
DECLARE
    n integer;
BEGIN
    SELECT count(*) INTO n FROM tenant_config WHERE markup_pct IS NULL;
    IF n > 0 THEN
        RAISE EXCEPTION
            'alembic 0044 downgrade: % tenant_config row(s) have markup_pct NULL '
            '(created by PUT /api/budget/money, markup never configured)', n
            USING HINT =
                'Set an explicit markup_pct on those rows (or delete them) first; '
                'the downgrade will not invent a rate nobody chose.';
    END IF;
END
$do$;
"""


LOCK_TIMEOUT = "3s"
STATEMENT_TIMEOUT = "60s"


def _bound_the_wait() -> None:
    """Bound the lock wait (ALTER TABLE billing_cost_events is on the ingest path).

    Same discipline as 0016-0018/0043: SET LOCAL, then read back -- outside a
    transaction block SET LOCAL is a silent no-op and the guard would be vacuous.
    """
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0044 refuses to run without a bounded lock_timeout "
            f"(SET LOCAL reported {effective!r}: not inside a transaction block)."
        )


def upgrade() -> None:
    _bound_the_wait()
    for stmt in UPGRADE_DDL:
        op.execute(stmt)
    op.execute(REQUIRE_GUARD_FUNCTION_DDL)
    op.execute(ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL)


def downgrade() -> None:
    _bound_the_wait()
    # The pre-0044 schema cannot say "markup not configured". Refuse (before
    # dropping anything) rather than invent a rate nobody chose.
    op.execute(REFUSE_NULL_MARKUP_ON_DOWNGRADE_DDL)
    op.execute("DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.fx_rate")
    op.execute("DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.tenant_money_budget")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS llm_setup")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS consumption_currency")
    op.execute("ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS consumption_amount")
    op.execute("ALTER TABLE tenant_config DROP COLUMN IF EXISTS budget_unit")
    op.execute("ALTER TABLE tenant_config ALTER COLUMN markup_pct SET NOT NULL")
    op.execute("DROP TABLE IF EXISTS fx_rate")
    op.execute("DROP TABLE IF EXISTS tenant_money_budget")
