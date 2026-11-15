"""Token budgets, legacy token-usage rollup, and T1.6 overage webhook tables."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import DBUUID, NULL_LICENSE_ID, _new_uuid, _utcnow


class TokenBudget(Base):
    """Token budget per tenant or license.

    Supports stacking — multiple concurrent active budgets per tenant draining
    in cascade order. The cascade logic itself lives in ``services.budget``;
    this model carries the columns needed to express the order.
    """

    __tablename__ = "token_budgets"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    # tenant_id is a real Postgres `uuid` column (alembic 0011). as_uuid=False is
    # deliberate: the DB type is uuid while Python keeps receiving/binding `str`, so
    # every existing comparison, log line and JSON serialisation keeps working.
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    tenant_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    # NOT NULL: application code always substitutes NULL_LICENSE_ID for None
    license_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_LICENSE_ID)
    budget_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False)
    period: Mapped[str] = mapped_column(String(10), nullable=False, default="monthly")
    includes_input: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    includes_output: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Stacking columns — lower priority drains first.
    priority: Mapped[int] = mapped_column(
        Integer, nullable=False, default=100, server_default="100"
    )
    starts_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utcnow,
        server_default=text("now()"),
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consumed_tokens: Mapped[Decimal] = mapped_column(
        Numeric(20, 6), nullable=False, default=0, server_default="0"
    )
    # Which PERIOD `consumed_tokens` belongs to (first day of that period, UTC).
    # Without this, `period` was decoration and a 'monthly' budget behaved as a
    # LIFETIME cap: consumed_tokens only ever grew and was zeroed once, at row
    # creation. Readers go through services.budget_period.effective_consumed,
    # which reports 0 once this anchor is older than the current period; the
    # debit path rebases the column inside its FOR UPDATE transaction.
    # NULL = pre-column row; resolved lazily from `starts_at` (same rule the
    # backfill migration uses), so both paths agree.
    consumed_period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    source: Mapped[str] = mapped_column(
        String(64), nullable=False, default="manual", server_default="manual"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    __table_args__ = (
        Index(
            "ix_tokenbudget_active_drain",
            "tenant_id",
            "active",
            "priority",
            "expires_at",
        ),
        Index("ix_token_budgets_tenant_name", "tenant_name"),
    )


class MoneyBudget(Base):
    """Whole-unit money budget per tenant (alembic 0044; multi-currency 0045, REV F).

    ``currency`` is the tenant's budget currency, one of EUR | USD | CAD, and
    every money figure on this row (``amount``, ``consumed``) is in it. It is
    changed only by ``POST /api/budget/money/{t}/currency`` (owner only), which
    converts the remaining budget and records a :class:`MoneyBudgetAdjustment`.

    The money ledger for a tenant whose ``tenant_config.budget_unit = 'money'``.
    ONE active row per tenant (partial unique ``uq_money_budget_active_tenant``);
    budgets pool per tenant, so there is no license axis and no stacking.

    ``amount`` is WHOLE EUR (an integer by contract — 10.5 is a 422 at the API).
    ``consumed`` is EUR at 8 dp, debited by ``services.cost_flusher`` inside the
    flush transaction (FOR UPDATE), and period-anchored by
    ``consumed_period_start`` under the same rule as
    ``TokenBudget.consumed_period_start``: a reader treats consumption from an
    earlier period as 0 (``services.money_budget.effective_money_consumed``) and
    the debit path rebases it to 0 inside its lock.
    """

    __tablename__ = "tenant_money_budget"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    period: Mapped[str] = mapped_column(
        String(10), nullable=False, default="monthly", server_default="monthly"
    )
    consumed: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, default=0, server_default="0"
    )
    consumed_period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    source: Mapped[str] = mapped_column(
        String(32), nullable=False, default="manual", server_default="manual"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("now()")
    )

    __table_args__ = (
        # alembic 0045 (REV F multi-currency) replaced 0044's EUR-only check.
        CheckConstraint("currency IN ('EUR', 'USD', 'CAD')", name="ck_money_budget_currency_iso"),
        CheckConstraint("amount >= 0", name="ck_money_budget_amount"),
        CheckConstraint(
            "source IN ('provisioning', 'conversion', 'manual')", name="ck_money_budget_source"
        ),
        Index(
            "uq_money_budget_active_tenant",
            "tenant_id",
            unique=True,
            postgresql_where=text("active"),
        ),
    )


class TokenUsage(Base):
    """Whole-period token rollup per (tenant, period, period_start, license).

    Written on EVERY flush for EVERY tenant by ``cost_flusher.flush_costs`` —
    a live rollup, not a legacy relic. Read by ``services.billing`` for monthly
    invoice token totals.

    NOT a per-budget usage source, and no longer a fallback for one. The grain
    has no budget_id, so attributing a row to a budget reports the tenant's
    whole period against that single budget — and against every stacked sibling
    as well. ``services.budget_status`` used to do exactly that whenever
    ``consumed_tokens`` was 0 (the normal state of any undebited budget); that
    path is deleted. Per-budget usage is ``TokenBudget.consumed_tokens``,
    drained by the cascade in ``services.budget_debit``.
    """

    __tablename__ = "token_usage"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    tenant_name: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    # NOT NULL: application code always substitutes NULL_LICENSE_ID for None
    license_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False, default=NULL_LICENSE_ID)
    period_type: Mapped[str] = mapped_column(String(10), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    input_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    output_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "license_id",
            "period_type",
            "period_start",
            name="uq_token_usage",
        ),
        Index("ix_token_usage_tenant_name", "tenant_name"),
    )


class TenantWebhook(Base):
    """Per-tenant outbound webhook registration.

    ``secret`` is the HMAC signing key the receiver uses to verify each
    payload (header ``X-Metering-Signature: sha256=<hex>``). Multiple
    rows per tenant are allowed; the watcher fans out to every active URL.
    """

    __tablename__ = "tenant_webhooks"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    secret: Mapped[str] = mapped_column(String(128), nullable=False)
    active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("now()")
    )

    __table_args__ = (Index("ix_tenant_webhooks_tenant_active", "tenant_id", "active"),)


class BudgetOverageAlert(Base):
    """Idempotency dedupe row per (budget, threshold, period, TARGET).

    The watcher writes one row per successfully-delivered *destination*; the
    unique constraint ``uq_budget_overage_dedupe`` guarantees at-most-once
    per period per destination, even across replicas / restarts / clock drift.
    ``outcome`` records whether delivery succeeded ('ok' / 'http_error' /
    'timeout' / 'no_webhooks').

    ``target_key`` exists because a crossing fans out to N destinations (each
    active tenant_webhooks row PLUS the platform sink). Keying only on
    (budget, threshold, period) meant ONE row covered them all, and the watcher
    wrote it whenever ANY destination returned 2xx — so a tenant webhook
    succeeding while the platform sink timed out marked the crossing delivered
    and the sink never received it. Note the trap in the other direction:
    recording only when ALL destinations succeed would re-POST the healthy ones
    every tick, which is exactly the spam #39 removed. Per-destination rows are
    what satisfy both.

    ``target_key`` is a SHA-256 of the destination URL, not the URL itself:
    stable across a webhook row being deleted and recreated, bounded in length,
    and it avoids persisting URLs (which can embed tokens in query params) into
    a long-lived audit table. Rows written before this column existed carry
    ``''`` — a LEGACY SENTINEL meaning "this whole crossing was already
    handled", which preserves the deliberate no-replay-of-history rule.
    """

    __tablename__ = "budget_overage_alerts"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    budget_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False)
    threshold_pct: Mapped[int] = mapped_column(Integer, nullable=False)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    dispatched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("now()")
    )
    outcome: Mapped[str] = mapped_column(
        String(32), nullable=False, default="ok", server_default="ok"
    )
    # SHA-256 hex of the destination URL. '' = legacy pre-column row covering
    # the whole crossing (see class docstring).
    target_key: Mapped[str] = mapped_column(
        String(64), nullable=False, default="", server_default=""
    )

    __table_args__ = (
        UniqueConstraint(
            "budget_id",
            "threshold_pct",
            "period_start",
            "target_key",
            name="uq_budget_overage_dedupe",
        ),
        Index("ix_budget_overage_tenant_dispatched", "tenant_id", "dispatched_at"),
    )


class MoneyBudgetAdjustment(Base):
    """One audited change to a money budget's denomination (alembic 0045, REV F).

    Written by ``POST /api/budget/money/{tenant_id}/currency``. The budget row is
    converted IN PLACE (same id, so the watcher's per-budget dedupe does not
    re-fire thresholds already crossed); this row is the permanent record of
    what it was before, in the ORIGINAL currency:

    * ``consumed_before`` / ``remaining_before`` / ``amount_before`` -- in
      ``from_currency``, for the period ``period_start``.
    * ``remaining_after`` = ``remaining_before`` x ``fx_rate``, ROUND_HALF_UP to
      minor units (2 dp).
    * ``amount_after`` = ``amount_before`` x ``fx_rate``, ROUND_HALF_UP to whole
      units -- the cap for later periods.
    * ``consumed_after`` = ``amount_after`` - ``remaining_after`` -- the opening
      consumed-equivalent, in ``to_currency``; NEGATIVE when the cap rounded
      down below the converted remaining (an opening credit, so the remaining
      is preserved exactly). Status subtracts it to report the consumption
      since the conversion.
    * ``fx_rate`` is the cross rate from -> to = rate(USD->to) / rate(USD->from);
      the two USD legs are kept too (``usd_rate_from`` / ``usd_rate_to``; 1 for USD).
    """

    __tablename__ = "tenant_money_budget_adjustment"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    budget_id: Mapped[uuid.UUID] = mapped_column(DBUUID(), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    from_currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    to_currency: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    fx_rate: Mapped[Decimal] = mapped_column(Numeric(30, 18), nullable=False)
    usd_rate_from: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    usd_rate_to: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    amount_before: Mapped[int] = mapped_column(BigInteger, nullable=False)
    amount_after: Mapped[int] = mapped_column(BigInteger, nullable=False)
    consumed_before: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    consumed_after: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    remaining_before: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    remaining_after: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    actor_sub: Mapped[str | None] = mapped_column(Text, nullable=True)
    actor_role: Mapped[str | None] = mapped_column(Text, nullable=True)
    # The relaying service account's ``azp`` (the OWNER is actor_sub/actor_role).
    via_client: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint("kind IN ('currency_conversion')", name="ck_money_adj_kind"),
        CheckConstraint(
            "from_currency IN ('EUR', 'USD', 'CAD') AND to_currency IN ('EUR', 'USD', 'CAD')",
            name="ck_money_adj_currency",
        ),
        CheckConstraint("fx_rate > 0", name="ck_money_adj_rate"),
        Index("ix_money_adj_tenant_created", "tenant_id", "created_at"),
    )


class CreditLedgerEntry(Base):
    """One entry of a tenant's PREPAID CREDIT ledger, in USD (alembic 0055).

    Owner ruling 2026-10-13: a top-up is prepaid credit the tenant spends down,
    invoiced ONCE when bought; metered LLM usage the credit covers is never
    invoiced again. The balance is never stored -- it is
    ``Σ topup - Σ usage_drawdown`` over this table, so there is no cap to mutate
    and no running total that can drift from its history.

    ``kind``:

    * ``'topup'`` -- a CREDIT, written by ``POST /api/budget/money/topup``.
      ``entry_key = 'topup:' || idempotency_key``. Carries what was bought
      (``source_amount`` whole units of ``source_currency``, ``fx_rate`` USD ->
      that currency) and the ``one_time`` access charge that invoiced it
      (``access_charge_id``).
    * ``'usage_drawdown'`` -- a DEBIT, written by ``services.invoices.close_period``
      in the invoice's own transaction: the credit that statement applied
      against its LLM usage. ``entry_key = 'usage:' || invoice_id``,
      ``period_start`` = the billed month. A zero drawdown is written too.
    * ``'drawdown_reversal'`` -- a CREDIT, written by ``services.invoices.void``:
      gives back exactly that invoice's drawdown (``'reversal:' || invoice_id``),
      so the re-close freezes a fresh one. Append-only: nothing is UPDATEd or
      DELETEd.

    ⭐ ``UNIQUE (tenant_id, entry_key)`` IS THE IDEMPOTENCY OF BOTH KINDS: a
    replayed top-up and a re-closed period (after a void, or a lost race) each
    find their row and write nothing. ``amount_usd`` is always >= 0; the sign is
    the ``kind``.
    """

    __tablename__ = "tenant_credit_ledger"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    entry_key: Mapped[str] = mapped_column(String(160), nullable=False)
    amount_usd: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    invoice_id: Mapped[uuid.UUID | None] = mapped_column(DBUUID(), nullable=True)
    source_amount: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_currency: Mapped[str | None] = mapped_column(CHAR(3), nullable=True)
    fx_rate: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    access_charge_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    actor_sub: Mapped[str | None] = mapped_column(Text, nullable=True)
    actor_role: Mapped[str | None] = mapped_column(Text, nullable=True)
    via_client: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint(
            "kind IN ('topup', 'usage_drawdown', 'drawdown_reversal')",
            name="ck_credit_ledger_kind",
        ),
        CheckConstraint("amount_usd >= 0", name="ck_credit_ledger_amount"),
        CheckConstraint(
            "(kind = 'topup' AND amount_usd > 0 AND source_amount > 0 "
            "AND source_currency IN ('EUR', 'USD', 'CAD') AND fx_rate > 0) "
            "OR (kind IN ('usage_drawdown', 'drawdown_reversal') "
            "AND period_start IS NOT NULL AND invoice_id IS NOT NULL)",
            name="ck_credit_ledger_shape",
        ),
        UniqueConstraint("tenant_id", "entry_key", name="uq_credit_ledger_entry"),
        Index("ix_credit_ledger_tenant_created", "tenant_id", "created_at"),
    )
