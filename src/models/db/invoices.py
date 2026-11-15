"""The invoice as a persisted OBJECT, not a re-derived report.

WHY A TABLE AND NOT A QUERY
---------------------------
``services.billing.generate_billing`` computes a tenant's invoice on demand from
``billing_cost_daily`` + ``markup_rules`` + ``tenant_access_charge``. That is a
REPORT: re-render the same month tomorrow and a markup rule edited in between
silently changes the answer. ``routers.billing`` already carries the scar --
"resolving at now() meant re-rendering an old month picked up every rule change
made since, so the overview and the invoices drifted".

A statement a customer has been shown is a RECORD. It has to keep saying the
same thing after the inputs move, which means the numbers, the inputs they were
computed from, and the code version that computed them are all stored on the row
rather than looked up again. That is what ``snapshot`` / ``inputs`` /
``snapshot_sha256`` / ``generator_version`` are for: a later reader can re-run
the generator against the recorded inputs and compare, instead of trusting that
the world has not moved.

WHAT THIS PR DOES *NOT* DECIDE
------------------------------
The legal/tax/multi-currency columns below (``fx_rate``, ``tax_scheme``,
``tax_rate``, ``tax_amount``, ``legal_entity``, ``customer_legal_name``,
``billing_address``, ``fx_base_currency``) are PRESENT AND NULL. Nothing in this
PR writes them and no reader may assume them.

They exist now rather than later on purpose, and the reason is not tidiness: the
immutability trigger in :mod:`.invoice_immutability` compares ``to_jsonb(OLD)``
against ``to_jsonb(NEW)``, so the guarded shape is the shape of the ROW. Adding
a column to an issued-invoice table later changes what "unchanged" means for
every historical row, and the migration that adds it has to reason about rows it
must not be able to touch. Declaring the legal set once, unset, keeps that
question closed. A NULL here means "this PR did not decide it" -- never "no tax
applies" or "the rate was 0".

MONEY PRECISION
---------------
Every money column is ``Numeric(20, 8)``, matching
``billing_cost_events.cost_usd`` (the scale-8 source these totals are summed
from). ``tests/test_money_column_precision.py`` records why the narrow
``Numeric(12, x)`` shapes were abandoned: a real magnitude overflows them and
Postgres aborts the whole write. An invoice total is an aggregate of aggregates
and is the LAST place that may overflow.

⚠️ ``billing_cost_daily.total_cost_usd`` is ``Numeric(20, 6)``, one scale digit
short of this. That is deliberate on the rollup side (it is itself a sum) and is
not copied here: an invoice may carry a per-line figure that came straight from
a scale-8 cost event, so widening to 8 loses nothing and truncating to 6 would.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    Text,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db import Base

from ._common import DBUUID, _new_uuid

#: Every status this table recognises. The CHECK below is written from this
#: tuple's members as a LITERAL rather than interpolated, for the same reason
#: alembic 0018 spells the trigger name as a literal: the migration carries its
#: own frozen copy, and a constant that can be edited on one side is a way for
#: the two spellings to drift silently.
INVOICE_STATUSES = ("draft", "issued", "void")


class BillingInvoice(Base):
    """One rendered statement for one tenant over one period.

    ``period_end`` is EXCLUSIVE (``[period_start, period_end)``). A half-open
    interval is the only spelling under which "the month of June" and "the month
    of July" are adjacent with no overlap and no gap, and under which a partial
    period is expressible without a special case.
    """

    __tablename__ = "billing_invoices"

    id: Mapped[uuid.UUID] = mapped_column(DBUUID(), primary_key=True, default=_new_uuid)
    # Same type as CostDaily.tenant_id: a real Postgres `uuid` column (alembic
    # 0011) bound as `str` on the Python side, so every existing comparison,
    # log line and JSON serialisation keeps working. See rollups.py.
    tenant_id: Mapped[str] = mapped_column(Uuid(as_uuid=False), nullable=False)
    # Denormalised at render time and then FROZEN. A tenant that renames must
    # not retroactively rename itself on a statement already sent.
    tenant_name: Mapped[str] = mapped_column(Text, nullable=False, server_default="")

    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    #: EXCLUSIVE end of the billed interval.
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="draft")

    # NULL until issued. The number comes from `billing_invoice_counters`, never
    # from a sequence -- see BillingInvoiceCounter for why that distinction is
    # not a micro-optimisation.
    invoice_number: Mapped[str | None] = mapped_column(Text, nullable=True)

    # --- present-and-null legal set (see the module docstring) --------------
    # ⛔ NOTHING IN THIS PR WRITES ANY OF THESE. A reader that treats NULL here
    # as a decided value ("no tax", "rate 1.0", "our only legal entity") is
    # asserting something this PR refused to assert.
    #
    # `currency` is the one exception in shape: NOT NULL DEFAULT 'USD'. Every
    # figure on this table is a USD figure today (the cost ledger is USD-only),
    # so a NULL currency would be the wrong kind of unknown -- it would let a
    # reader render an amount with no unit at all. The sibling columns say what
    # is undecided; this one says what is true.
    currency: Mapped[str] = mapped_column(Text, nullable=False, server_default="USD")
    fx_rate: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    fx_base_currency: Mapped[str | None] = mapped_column(Text, nullable=True)
    tax_scheme: Mapped[str | None] = mapped_column(Text, nullable=True)
    tax_rate: Mapped[Decimal | None] = mapped_column(Numeric(9, 6), nullable=True)
    tax_amount: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    legal_entity: Mapped[str | None] = mapped_column(Text, nullable=True)
    customer_legal_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    billing_address: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # --- the money ----------------------------------------------------------
    # No server_default on the four totals: a row with no total is not a
    # zero-value invoice, it is a generator that failed to fill one in, and
    # `0` would make that indistinguishable from a genuinely empty period.
    # The component columns DO default to 0 -- a period with no LLM usage
    # legitimately has llm_usage_usd = 0.
    subtotal_usd: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    total_usd: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    #: What the platform PAID (pre-markup), carried so margin is auditable on
    #: the row rather than recomputed from a rollup that has since moved.
    internal_cost_usd: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    margin_usd: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)

    access_charge_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    platform_fee_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    llm_usage_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    llm_markup_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    #: The APP-REQUEST billed stream, split incurred/markup exactly as the llm
    #: pair above is. ``generate_billing`` bills TWO streams into
    #: ``totals.customer_cost_usd`` — ``app_request_billed`` and ``llm_billed`` —
    #: and until alembic 0033 only the second had columns, so the components
    #: stored here understated ``total_usd`` by precisely the app-request amount.
    #:
    #: ⭐ THE GAP WAS LATENT, NOT ABSENT. Measured 2026-09-13,
    #: ``billing_app_cost_rates`` held 2 rows, both priced 0.00000000, so the
    #: missing term was zero for every statement ever closed. One non-zero rate
    #: arms it, silently, on rows that are immutable by design. See
    #: ``services.invoices.assert_total_is_the_sum_of_its_components`` for the
    #: gate that now refuses a close where the sum does not hold.
    #:
    #: ⚠️ ``app_request_markup_usd`` IS SIGNED. Under ``BILLING_MODE_INTERNAL``
    #: every line bills $0 against a real incurred cost, so the pair reads
    #: ``+X``/``−X`` and nets to zero — the same shape the llm pair takes, and the
    #: reason neither carries a ``>= 0`` CHECK.
    app_request_usage_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    app_request_markup_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    seat_fee_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    #: PREPAID CREDIT applied against this statement's LLM usage (alembic 0055),
    #: in cents. SUBTRACTED from the components: ``total_usd = Σ components -
    #: prepaid_credit_applied_usd``. The same figure is the period's
    #: ``usage_drawdown`` entry in ``tenant_credit_ledger``, frozen by the close.
    prepaid_credit_applied_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    #: NULL = seats were not part of this statement's basis at all, which is a
    #: different fact from 0 seats. Not defaulted for that reason.
    seat_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # --- disclosure of what was NOT billed ----------------------------------
    # An invoice that silently drops spend it could not attribute is the exact
    # failure `CostDaily.unlicensed_attributable_clause` was written to make
    # visible ($3,386.52 across 22 licence-owning tenants, measured 2026-09-06).
    # These three columns carry that number ONTO the statement's own row, so the
    # gap is countable per invoice instead of only discoverable by re-querying
    # the rollups months later.
    excluded_unbillable_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 8), nullable=False, server_default="0"
    )
    excluded_unbillable_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    #: ``{attribution_status: usd}``, so "we dropped $X" can be read as "we
    #: dropped $A of genuine system cost and $B of a real customer's unlicensed
    #: spend" -- two facts with opposite implications.
    excluded_by_status: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )

    # --- provenance: how this number was arrived at -------------------------
    billing_mode: Mapped[str] = mapped_column(Text, nullable=False)
    markup_pct: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    #: The markup actually REALISED (margin / internal cost), which diverges
    #: from the nominal ``markup_pct`` whenever per-model rules, BYO lines or
    #: absolute surcharges are in play. Wider scale because it is a computed
    #: ratio, not an operator-entered percentage.
    effective_markup_pct: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    #: ⭐ WHERE ``markup_pct`` CAME FROM: ``markup_rule:<id>`` for an
    #: effective-dated rule, ``tenant_config`` / ``tenant_config_default`` for a
    #: persisted config row, or ``settings_default`` for the code constant
    #: ``settings.default_markup_pct``. See ``services.markup`` for the full set.
    #:
    #: A statement recording the RATE but not its SOURCE cannot be reconciled
    #: against a rate card — "30%" from a commercial rule an operator signed off
    #: and "30%" from a default nobody chose are the same number and different
    #: facts. This matters precisely BECAUSE every ``markup_rules`` row is
    #: currently closed, so today every tenant silently rides the constant.
    #:
    #: ``server_default`` is the honest ``'unknown'`` rather than a plausible
    #: source: a row written before this column existed has no provenance, and
    #: defaulting it to ``settings_default`` would invent one.
    markup_source: Mapped[str] = mapped_column(Text, nullable=False, server_default="unknown")
    #: The resolved rate cards / rules / charges as they stood at render time.
    snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)
    #: The generator's INPUTS (the aggregates it consumed), so the computation
    #: can be replayed without re-querying tables that have since changed.
    inputs: Mapped[dict] = mapped_column(JSONB, nullable=False)
    #: Digest over ``snapshot``. Cheap equality for "did the basis move?"
    #: without diffing two JSONB documents.
    snapshot_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    lines: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    #: The generator that produced this. A statement rendered by a version with
    #: a known bug must be FINDABLE; without this the only way to scope a
    #: recall is by date, which is a guess.
    generator_version: Mapped[str] = mapped_column(Text, nullable=False)

    # --- lifecycle ----------------------------------------------------------
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    issued_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    voided_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    voided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    void_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'issued', 'void')",
            name="ck_billing_invoices_status",
        ),
        # An invoice NUMBER is unique across the whole series or it is not a
        # number. Partial so the many draft rows carrying NULL do not collide
        # with each other (Postgres treats NULL != NULL for uniqueness, so the
        # predicate is belt-and-braces -- it also keeps the index small).
        Index(
            "ux_billing_invoices_number",
            "invoice_number",
            unique=True,
            postgresql_where=text("invoice_number IS NOT NULL"),
        ),
        # At most one LIVE statement per (tenant, period). A voided invoice
        # steps out of the set so the period can be re-issued.
        #
        # ⛔ THE PREDICATE IS POSITIVE ON PURPOSE: `status IN ('draft','issued')`,
        # never `status <> 'void'`. Under `<>`, a status added later -- a
        # plausible 'credited' or 'superseded' -- JOINS the uniqueness set the
        # moment it exists, with no edit to this file and no test going red, and
        # the table starts refusing legitimate re-issues. The positive spelling
        # fails the other way: a new status is EXCLUDED until someone names it
        # here, which is a decision made deliberately instead of inherited.
        Index(
            "ux_billing_invoices_open_period",
            "tenant_id",
            "period_start",
            "period_end",
            unique=True,
            postgresql_where=text("status IN ('draft', 'issued')"),
        ),
        # The read path: a tenant's statements, newest period first.
        Index("ix_billing_invoices_tenant_period", "tenant_id", text("period_start DESC")),
    )


class BillingInvoiceCounter(Base):
    """Gapless per-(legal entity, year) invoice number allocator.

    ⛔ WHY THIS IS NOT A POSTGRES SEQUENCE. A ``SEQUENCE`` is explicitly NOT
    gapless: ``nextval`` is non-transactional by design, so a transaction that
    rolls back BURNS its number forever, and so does a crash between allocation
    and commit. That property is what makes sequences fast and concurrent, and
    it is exactly the property a legal invoice series cannot have -- most
    jurisdictions require a consecutive, unbroken series, and an auditor reading
    ``INV-2026-0041`` followed by ``INV-2026-0043`` is entitled to ask which
    statement was destroyed.

    And a gap cannot be repaired after the fact. Retro-fitting the missing
    number means either renumbering statements already sent to customers, or
    minting a document for a number that never had one. Both are worse than the
    cost avoided. So the counter is an ordinary ROW, incremented inside the same
    transaction that writes the invoice, under ``SELECT ... FOR UPDATE``: a
    rollback takes the increment with it, and the series stays consecutive at
    the price of serialising issuance within one (entity, year).

    ``scope`` is the primary key and is spelled ``'<legal_entity_or_NA>:<year>'``
    -- e.g. ``'NA:2026'`` while ``billing_invoices.legal_entity`` is still
    unset (see :class:`BillingInvoice`'s present-and-null legal set). The year is
    ALSO carried as its own column rather than only parsed back out of the key,
    so a series can be queried and audited per year without string surgery.
    """

    __tablename__ = "billing_invoice_counters"

    scope: Mapped[str] = mapped_column(Text, primary_key=True)
    period_year: Mapped[int] = mapped_column(Integer, nullable=False)
    #: The NEXT number to hand out, not the last one issued. BigInteger because
    #: this is a legal series that must never wrap.
    next_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="1")
