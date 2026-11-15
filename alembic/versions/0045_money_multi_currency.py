"""Multi-currency money budgets (EUR | USD | CAD) and the currency-change ledger.

Owner ruling 2026-10-02 (late), tenant-inference-provisioning PLAN REV F:
every money amount carries its ISO code; a tenant's budget currency is one of
EUR, USD, CAD; ``fx_rate`` holds USD -> X (USD itself is implicitly 1); and the
currency is updatable mid-month by the owner, converting the remaining budget
and recording the conversion.

WHAT THIS DOES (additive / relaxing only, forward-only)
-------------------------------------------------------
- ``tenant_money_budget``: 0044's ``ck_money_budget_currency`` (``= 'EUR'``) is
  RELAXED to ``ck_money_budget_currency_iso`` (``IN ('EUR','USD','CAD')``).
  Every existing row is EUR, so the new check validates trivially.
- ``fx_rate``: ``ck_fx_rate_pair`` -- ``base = 'USD' AND quote IN ('EUR','CAD')``.
  Every existing row is USD->EUR (the 0044 API accepted nothing else).
- ``tenant_money_budget_adjustment``: new table, one row per currency
  conversion, holding the before-figures in the ORIGINAL currency.
- The truncate guard is attached to the new table with 0043's ATTACH-IF-ABSENT
  catalog sweep, so it is never born unguarded.

Each constraint step SKIPS when the target shape already exists, so a rerun (or
a database where ``src/db.py`` ADDITIVE_MIGRATIONS already created the table)
takes no lock it does not need.

Revision ID: 0045_money_multi_currency
Revises: 0044_money_budgets
Create Date: 2026-10-02
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0045_money_multi_currency"
down_revision: str | None = "0044_money_budgets"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Byte-identical to the entries in src/db.py ADDITIVE_MIGRATIONS.
CREATE_ADJUSTMENT = (
    "CREATE TABLE IF NOT EXISTS tenant_money_budget_adjustment ("
    "id UUID PRIMARY KEY, "
    "tenant_id UUID NOT NULL, "
    "budget_id UUID NOT NULL, "
    "kind VARCHAR(32) NOT NULL, "
    "period_start DATE, "
    "from_currency CHAR(3) NOT NULL, "
    "to_currency CHAR(3) NOT NULL, "
    "fx_rate NUMERIC(30, 18) NOT NULL, "
    "usd_rate_from NUMERIC(20, 10) NOT NULL, "
    "usd_rate_to NUMERIC(20, 10) NOT NULL, "
    "amount_before BIGINT NOT NULL, "
    "amount_after BIGINT NOT NULL, "
    "consumed_before NUMERIC(20, 8) NOT NULL, "
    "consumed_after NUMERIC(20, 8) NOT NULL, "
    "remaining_before NUMERIC(20, 8) NOT NULL, "
    "remaining_after NUMERIC(20, 8) NOT NULL, "
    "actor_sub TEXT, "
    "actor_role TEXT, "
    "via_client TEXT, "
    "reason TEXT, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT ck_money_adj_kind CHECK (kind IN ('currency_conversion')), "
    "CONSTRAINT ck_money_adj_currency CHECK "
    "(from_currency IN ('EUR', 'USD', 'CAD') AND to_currency IN ('EUR', 'USD', 'CAD')), "
    "CONSTRAINT ck_money_adj_rate CHECK (fx_rate > 0))"
)
CREATE_ADJUSTMENT_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_money_adj_tenant_created "
    "ON tenant_money_budget_adjustment (tenant_id, created_at)"
)
#: One-way relaxation of 0044's EUR-only check (also in ADDITIVE_MIGRATIONS).
DROP_EUR_ONLY_CHECK = (
    "ALTER TABLE tenant_money_budget DROP CONSTRAINT IF EXISTS ck_money_budget_currency"
)

ADD_ISO_CHECK_IF_ABSENT = r"""
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conname = 'ck_money_budget_currency_iso'
                      AND conrelid = 'public.tenant_money_budget'::regclass) THEN
        ALTER TABLE tenant_money_budget ADD CONSTRAINT ck_money_budget_currency_iso
            CHECK (currency IN ('EUR', 'USD', 'CAD'));
    END IF;
END
$do$;
"""

ADD_FX_PAIR_CHECK_IF_ABSENT = r"""
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conname = 'ck_fx_rate_pair'
                      AND conrelid = 'public.fx_rate'::regclass) THEN
        ALTER TABLE fx_rate ADD CONSTRAINT ck_fx_rate_pair
            CHECK (base = 'USD' AND quote IN ('EUR', 'CAD'));
    END IF;
END
$do$;
"""

REQUIRE_GUARD_FUNCTION_DDL = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NULL THEN
        RAISE EXCEPTION
            'alembic 0045 requires public.billing_forbid_truncate() from 0017'
            USING HINT = 'Run alembic 0017 first.';
    END IF;
END
$do$;
"""

#: 0043's catalog sweep (ATTACH-IF-ABSENT), the same predicate 0044 reuses: arms
#: the table born here and touches nothing already guarded.
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
           AND c.relkind IN ('r', 'p')
           AND c.relpersistence = 'p'
           AND c.relname <> 'alembic_version'
           AND NOT EXISTS (
                   SELECT 1
                     FROM pg_depend d
                    WHERE d.classid = to_regclass('pg_class')
                      AND d.objid = c.oid
                      AND d.deptype = 'e')
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

    RAISE NOTICE 'alembic 0045: armed % newly-guarded relation(s)', armed;
END
$do$;
"""

REFUSE_NON_EUR_ON_DOWNGRADE_DDL = r"""
DO $do$
DECLARE
    n integer;
BEGIN
    SELECT count(*) INTO n FROM tenant_money_budget WHERE currency <> 'EUR';
    IF n > 0 THEN
        RAISE EXCEPTION
            'alembic 0045 downgrade: % money budget(s) are not EUR', n
            USING HINT = 'The 0044 schema can only hold EUR; convert them first.';
    END IF;
    SELECT count(*) INTO n FROM fx_rate WHERE quote <> 'EUR';
    IF n > 0 THEN
        RAISE EXCEPTION 'alembic 0045 downgrade: % non-EUR fx_rate row(s)', n;
    END IF;
END
$do$;
"""

LOCK_TIMEOUT = "3s"
STATEMENT_TIMEOUT = "60s"


def _bound_the_wait() -> None:
    """Same discipline as 0043/0044: SET LOCAL, then read back (never vacuous)."""
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0045 refuses to run without a bounded lock_timeout "
            f"(SET LOCAL reported {effective!r}: not inside a transaction block)."
        )


def upgrade() -> None:
    _bound_the_wait()
    op.execute(CREATE_ADJUSTMENT)
    op.execute(CREATE_ADJUSTMENT_INDEX)
    op.execute(DROP_EUR_ONLY_CHECK)
    op.execute(ADD_ISO_CHECK_IF_ABSENT)
    op.execute(ADD_FX_PAIR_CHECK_IF_ABSENT)
    op.execute(REQUIRE_GUARD_FUNCTION_DDL)
    op.execute(ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL)


def downgrade() -> None:
    _bound_the_wait()
    op.execute(REFUSE_NON_EUR_ON_DOWNGRADE_DDL)
    op.execute(
        "DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.tenant_money_budget_adjustment"
    )
    op.execute("DROP TABLE IF EXISTS tenant_money_budget_adjustment")
    op.execute("ALTER TABLE fx_rate DROP CONSTRAINT IF EXISTS ck_fx_rate_pair")
    op.execute(
        "ALTER TABLE tenant_money_budget DROP CONSTRAINT IF EXISTS ck_money_budget_currency_iso"
    )
    op.execute(
        "ALTER TABLE tenant_money_budget ADD CONSTRAINT ck_money_budget_currency "
        "CHECK (currency = 'EUR')"
    )
