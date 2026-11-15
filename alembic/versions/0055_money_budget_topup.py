"""Prepaid credit: tenant_credit_ledger + billing_invoices.prepaid_credit_applied_usd.

WHY
---
CommonPractice Part B, owner ruling 2026-10-13: a tenant_admin's top-up is
PREPAID CREDIT. It is invoiced once, as a ``one_time`` access charge, when it is
bought. The tenant then spends it down: metered LLM usage that the credit covers
is netted out of each statement and is never invoiced again. Unused credit
carries over between periods.

The credit is an append-only LEDGER, never a mutated cap. Top-ups are credits;
each closed invoice's drawdown is a debit; voiding that invoice appends a
``drawdown_reversal`` credit. All are idempotent on
``UNIQUE (tenant_id, entry_key)``, and the balance is the sum.

WHAT THIS DOES
--------------
1. ``CREATE TABLE tenant_credit_ledger`` + the (tenant_id, created_at) index.
2. Arms ``billing_truncate_guard_trg`` on it (0043's lesson).
3. ``ALTER TABLE billing_invoices ADD COLUMN prepaid_credit_applied_usd
   NUMERIC(20, 8) NOT NULL DEFAULT 0``. A constant default is catalogue-only
   (no table rewrite, no row UPDATE), so the immutability trigger never sees a
   changed row. Existing statements read 0, which is what they applied.

⚠️ COMPANION EDIT -- ``src/db.py``: statements 1 and 3 are mirrored
BYTE-IDENTICALLY into ``db.ADDITIVE_MIGRATIONS``, because ``close_period``
writes the new column. A pod booting ahead of this migration must still close.

Revision ID: 0055_money_budget_topup
Revises: 0054_rekey_cdmsfirst_demo
Create Date: 2026-10-13
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0055_money_budget_topup"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0054_rekey_cdmsfirst_demo"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Byte-identical to the entries in src/db.py ADDITIVE_MIGRATIONS.
CREATE_LEDGER = (
    "CREATE TABLE IF NOT EXISTS tenant_credit_ledger ("
    "id UUID PRIMARY KEY, "
    "tenant_id UUID NOT NULL, "
    "kind VARCHAR(24) NOT NULL, "
    "entry_key VARCHAR(160) NOT NULL, "
    "amount_usd NUMERIC(20, 8) NOT NULL, "
    "period_start DATE, "
    "invoice_id UUID, "
    "source_amount BIGINT, "
    "source_currency CHAR(3), "
    "fx_rate NUMERIC(20, 10), "
    "access_charge_id BIGINT, "
    "actor_sub TEXT, "
    "actor_role TEXT, "
    "via_client TEXT, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT ck_credit_ledger_kind CHECK "
    "(kind IN ('topup', 'usage_drawdown', 'drawdown_reversal')), "
    "CONSTRAINT ck_credit_ledger_amount CHECK (amount_usd >= 0), "
    "CONSTRAINT ck_credit_ledger_shape CHECK "
    "((kind = 'topup' AND amount_usd > 0 AND source_amount > 0 "
    "AND source_currency IN ('EUR', 'USD', 'CAD') AND fx_rate > 0) "
    "OR (kind IN ('usage_drawdown', 'drawdown_reversal') "
    "AND period_start IS NOT NULL AND invoice_id IS NOT NULL)), "
    "CONSTRAINT uq_credit_ledger_entry UNIQUE (tenant_id, entry_key))"
)
CREATE_LEDGER_INDEX = (
    "CREATE INDEX IF NOT EXISTS ix_credit_ledger_tenant_created "
    "ON tenant_credit_ledger (tenant_id, created_at)"
)
ADD_INVOICE_CREDIT_COLUMN = (
    "ALTER TABLE billing_invoices ADD COLUMN IF NOT EXISTS prepaid_credit_applied_usd "
    "NUMERIC(20, 8) NOT NULL DEFAULT 0"
)

_ARM_TRUNCATE_GUARD = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM pg_trigger
            WHERE tgrelid = to_regclass('public.tenant_credit_ledger')
              AND tgname = 'billing_truncate_guard_trg') THEN
        EXECUTE 'CREATE TRIGGER billing_truncate_guard_trg BEFORE TRUNCATE ON '
                'public.tenant_credit_ledger FOR EACH STATEMENT '
                'EXECUTE FUNCTION public.billing_forbid_truncate()';
    END IF;
END
$do$;
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '3s'")
    op.execute(CREATE_LEDGER)
    op.execute(CREATE_LEDGER_INDEX)
    op.execute(_ARM_TRUNCATE_GUARD)
    op.execute(ADD_INVOICE_CREDIT_COLUMN)


def downgrade() -> None:
    # Forward-only estate; this exists for the chain round-trip only, and refuses
    # to destroy a credit ledger or a statement that applied credit.
    op.execute(
        "DO $do$ BEGIN "
        "IF EXISTS (SELECT 1 FROM tenant_credit_ledger) THEN "
        "RAISE EXCEPTION 'alembic 0055 downgrade: tenant_credit_ledger holds rows'; END IF; "
        "IF EXISTS (SELECT 1 FROM billing_invoices WHERE prepaid_credit_applied_usd <> 0) THEN "
        "RAISE EXCEPTION 'alembic 0055 downgrade: an invoice applied prepaid credit'; END IF; "
        "END $do$;"
    )
    op.execute("ALTER TABLE billing_invoices DROP COLUMN IF EXISTS prepaid_credit_applied_usd")
    op.execute("DROP TABLE IF EXISTS tenant_credit_ledger")
