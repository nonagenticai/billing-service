"""Prepaid credit: an invoiced top-up the tenant spends down (alembic 0055).

Owner ruling 2026-10-13 (CommonPractice Part B):

* A TOP-UP is prepaid credit. It is invoiced ONCE, when bought, as a
  ``cadence='one_time'`` access charge whose service key is ``topup:<entry id>``.
  The invoice generator itemises that charge in the grant month.
* Metered LLM usage that the credit COVERS is never invoiced again.
  :func:`services.billing.generate_billing` nets each statement's billed LLM
  usage against the credit available, and bills only usage beyond it. Unused
  credit carries over.
* The credit is an APPEND-ONLY LEDGER (:class:`models.db.CreditLedgerEntry`) in
  USD, the invoice currency. It has three kinds of entry:
  ``topup`` (+), ``usage_drawdown`` (-, one per closed INVOICE) and
  ``drawdown_reversal`` (+, written when that invoice is voided). Every entry is
  idempotent on ``UNIQUE (tenant_id, entry_key)``. The balance is the sum and
  is never stored.

⭐ WHY THE MONEY-BUDGET CAP STAYS SEPARATE (and is not touched by a top-up).
``tenant_money_budget.amount`` is a SPEND-RATE guard. It is a whole-unit cap in
the budget currency that RESETS every period and is enforced per inference hop.
Credit is a durable USD balance that CARRIES OVER and is settled per invoice.
Deriving one from the other gives either a cap that never resets or credit that
expires monthly, with two currencies on one figure.

AVAILABLE CREDIT -- ORDER-INDEPENDENT (audit of #267, 2026-10-13)::

    available = max(0, Σ topup - Σ live drawdown)      over ALL months

A drawdown is "live" while its invoice is not voided (no reversal entry). The
first version subtracted only drawdowns of EARLIER months, on the assumption
that months close in order. They do not: ``close_due_periods`` skips a refused
month, and an operator later closes it by hand. That assumption spent credit
twice (top-up 100; close May at 90, then April at 30 -> drawn 120, balance -20).
The sum over every live drawdown cannot double-spend, whatever the close order.

⭐ SERIALISED PER TENANT. :func:`lock_tenant_credit` takes a transaction-scoped
advisory lock. ``close_period`` and ``void`` take it BEFORE they read credit, so
two closes for one tenant cannot both see the same balance. :func:`freeze_drawdown`
then re-checks ``balance >= 0`` under that lock and raises
:class:`CreditOverdrawnError` rather than commit an overdraft.

FROZEN, BUT REVERSIBLE BY VOID. A closed month's LIVE INVOICE is
authoritative: a re-render applies exactly the ``prepaid_credit_applied_usd``
that invoice froze -- including 0 for a month closed before any credit existed,
so a later top-up never shows up as credit on an already-issued month's report. Voiding the invoice appends a ``drawdown_reversal`` for exactly that
amount. The month is then open again, and its re-close freezes a NEW drawdown
on the new invoice. Invoice and ledger agree at every point:
``Σ prepaid_credit_applied_usd over live invoices == Σ live drawdowns``.

⚠️ Stated limits:

* Credit is applied per STATEMENT at close time, not per day. Credit bought
  before a month closes can pay for that month's usage.
* A PREVIEW of an open month counts only drawdowns already frozen, so it can
  read high while another month's usage is unclosed. Frozen statements are
  exact, because the lock plus the re-check make overdraft impossible.
* ORDER-DEPENDENT AFTER A VOID: voiding a LATER month gives its credit back, but
  an EARLIER month already closed short of credit keeps its frozen statement
  (it is not re-opened). That under-credit is accepted; re-closing the earlier
  month (void + close) picks the freed credit up.
* ROUNDING: credit is applied ROUNDED DOWN to the cent, so the statement keeps
  the sub-cent residual of usage (10.005 of usage under 100 of credit applies
  10.00 and bills 0.01). Rounding up let a statement go negative.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import and_, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import aliased

from models.db import BillingInvoice, CreditLedgerEntry, TenantAccessCharge
from models.domain import AccessChargeCreate
from services import access_charge as access_charge_svc
from services.money_budget import MoneyRefusal, fx_rate_at

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.stdlib.get_logger(__name__)

KIND_TOPUP = "topup"
KIND_DRAWDOWN = "usage_drawdown"
KIND_REVERSAL = "drawdown_reversal"
CREDIT_CURRENCY = "USD"

#: Prefix of the reserved ``service_key`` a top-up's billable line occupies:
#: ``topup:<ledger entry id>``. One key PER top-up, forced by
#: ``services.access_charge.upsert_access_charge``: a ``one_time`` charge bills
#: once per ``(tenant, service_key)`` lineage EVER, so a shared key would amend
#: the first top-up instead of billing the second.
TOPUP_SERVICE_KEY_PREFIX = "topup:"  # pragma: allowlist secret - a service_key, not a credential

_Q2 = Decimal("0.01")

#: Advisory-lock namespace of :func:`lock_tenant_credit`. ``scripts/tenant_purge.py``
#: spells the same string (it imports nothing from ``src``); a test pins them equal.
CREDIT_LOCK_PREFIX = "billing:prepaid-credit:"


class CreditOverdrawnError(RuntimeError):
    """A freeze would take the tenant's credit balance below zero. Nothing commits."""

    def __init__(self, tenant_id: str, remaining: Decimal) -> None:
        super().__init__(f"prepaid credit for {tenant_id} would be {remaining} after this drawdown")
        self.tenant_id = tenant_id
        self.remaining = remaining


def _cents(value: Decimal) -> Decimal:
    return value.quantize(_Q2, rounding=ROUND_HALF_UP)


def topup_entry_key(idempotency_key: str) -> str:
    return f"topup:{idempotency_key}"


def drawdown_entry_key(invoice_id: uuid.UUID) -> str:
    """One drawdown per INVOICE, so a void + re-close freezes a fresh one."""
    return f"usage:{invoice_id}"


def reversal_entry_key(invoice_id: uuid.UUID) -> str:
    return f"reversal:{invoice_id}"


def topup_amount_usd(amount: int, fx_rate: Decimal) -> Decimal:
    """Whole units of ``currency`` -> USD cents (``fx_rate`` = USD -> currency)."""
    return _cents(Decimal(amount) / fx_rate)


async def lock_tenant_credit(session: AsyncSession, tenant_id: str) -> None:
    """Serialise every credit READ-then-WRITE for one tenant until this transaction ends.

    ``pg_advisory_xact_lock`` is released by COMMIT or ROLLBACK and needs no row
    to exist (a tenant with no ledger entries still serialises). The key is
    namespaced so that it cannot collide with another advisory-lock user.
    """
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
        {"k": f"{CREDIT_LOCK_PREFIX}{tenant_id}"},
    )


# --- reads --------------------------------------------------------------------------


@dataclass(frozen=True)
class CreditForMonth:
    """The credit a statement for one month may apply."""

    available_usd: Decimal
    #: What the month's LIVE (un-voided) invoice applied, when it has one; else None.
    frozen_drawdown_usd: Decimal | None

    def applied(self, billed_llm_usd: Decimal) -> Decimal:
        """Credit to net against ``billed_llm_usd``: whole cents, ROUNDED DOWN.

        ⛔ ROUND_FLOOR over the RAW usage, never usage rounded to cents first. The
        caller subtracts this from the UNROUNDED usage, so a half-up 10.005 ->
        10.01 credit made the statement 10.005 - 10.01 = -0.005 -> -0.01, inside
        the one-cent identity tolerance (#267 re-audit). Floored, the credit can
        never exceed the usage it covers, so the remainder is >= 0.
        """
        usage = max(billed_llm_usd, Decimal("0"))
        cap = (
            self.frozen_drawdown_usd
            if self.frozen_drawdown_usd is not None
            else max(self.available_usd, Decimal("0"))
        )
        return min(cap, usage).quantize(_Q2, rounding=ROUND_FLOOR)

    # ⛔ ``available_usd`` MEANS DIFFERENT THINGS FOR AN OPEN AND A CLOSED MONTH, so
    # nothing outside this class may do arithmetic on it (#267 third audit:
    # ``available - applied`` on a closed month subtracted its own drawdown TWICE,
    # top-up 100 / usage 90 read ``remaining -80`` while the ledger said 10).
    #   * open month   -- the ledger has no drawdown for it yet: available is the
    #     credit ENTERING the month, and this month's use comes off it.
    #   * closed month -- its drawdown is already ON the ledger: available is what
    #     is LEFT, and adding the frozen use back gives what entered.

    def entering(self, applied: Decimal) -> Decimal:
        """Credit available to this statement before it applied any (cents, >= 0)."""
        base = max(self.available_usd, Decimal("0"))
        return _cents(base + applied if self.frozen_drawdown_usd is not None else base)

    def remaining(self, applied: Decimal) -> Decimal:
        """Credit left after this statement (cents, never negative)."""
        base = max(self.available_usd, Decimal("0"))
        left = base if self.frozen_drawdown_usd is not None else base - applied
        return _cents(max(left, Decimal("0")))


async def _sum(session: AsyncSession, *where: Any) -> Decimal:
    total = (
        await session.execute(
            select(func.coalesce(func.sum(CreditLedgerEntry.amount_usd), 0)).where(*where)
        )
    ).scalar_one()
    return Decimal(total)


def _live_drawdowns(tenant_id: str):  # noqa: ANN202 - a SQLAlchemy select
    """Drawdowns of ``tenant_id`` with no reversal for the same invoice."""
    rev = aliased(CreditLedgerEntry)
    return (
        select(CreditLedgerEntry)
        .outerjoin(
            rev,
            and_(
                rev.tenant_id == CreditLedgerEntry.tenant_id,
                rev.kind == KIND_REVERSAL,
                rev.invoice_id == CreditLedgerEntry.invoice_id,
            ),
        )
        .where(
            CreditLedgerEntry.tenant_id == tenant_id,
            CreditLedgerEntry.kind == KIND_DRAWDOWN,
            rev.id.is_(None),
        )
    )


async def _net_drawn(session: AsyncSession, tenant_id: str) -> Decimal:
    """Σ drawdown - Σ reversal: the credit currently spent by live invoices."""
    drawn = await _sum(
        session, CreditLedgerEntry.tenant_id == tenant_id, CreditLedgerEntry.kind == KIND_DRAWDOWN
    )
    reversed_ = await _sum(
        session, CreditLedgerEntry.tenant_id == tenant_id, CreditLedgerEntry.kind == KIND_REVERSAL
    )
    return drawn - reversed_


async def credit_for_month(
    session: AsyncSession, *, tenant_id: str, month_start: date
) -> CreditForMonth:
    """Available credit for the statement of ``month_start`` (see the module docstring).

    ⭐ A month with a LIVE invoice is frozen at what THAT INVOICE applied, read from
    ``billing_invoices`` rather than the ledger. A month closed without credit has
    no ledger drawdown (only credit > 0 writes one), so reading the ledger made
    a later top-up appear as credit on that month's report (#267 re-audit).
    """
    issued = (
        await session.execute(
            select(BillingInvoice.prepaid_credit_applied_usd)
            .where(
                BillingInvoice.tenant_id == tenant_id,
                BillingInvoice.period_start == month_start,
                BillingInvoice.status != "void",
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    frozen = Decimal(issued) if issued is not None else None
    credits = await _sum(
        session, CreditLedgerEntry.tenant_id == tenant_id, CreditLedgerEntry.kind == KIND_TOPUP
    )
    if credits == 0:
        return CreditForMonth(available_usd=Decimal("0"), frozen_drawdown_usd=frozen)
    available = max(credits - await _net_drawn(session, tenant_id), Decimal("0"))
    return CreditForMonth(available_usd=available, frozen_drawdown_usd=frozen)


@dataclass(frozen=True)
class CreditBalance:
    granted_usd: Decimal
    drawn_usd: Decimal
    remaining_usd: Decimal
    #: Last month with a live (un-voided) drawdown (``YYYY-MM``), or None.
    settled_through: str | None


async def credit_balance(session: AsyncSession, tenant_id: str) -> CreditBalance:
    """Ledger balance: every top-up minus every LIVE drawdown.

    Usage in a month not yet closed is not in it yet. That month's invoice
    preview carries ``totals.prepaid_credit.remaining_usd`` for the provisional
    figure.
    """
    granted = await _sum(
        session, CreditLedgerEntry.tenant_id == tenant_id, CreditLedgerEntry.kind == KIND_TOPUP
    )
    drawn = await _net_drawn(session, tenant_id)
    live_months = (await session.execute(_live_drawdowns(tenant_id))).scalars().all()
    last = max((r.period_start for r in live_months if r.period_start), default=None)
    return CreditBalance(
        granted_usd=_cents(granted),
        drawn_usd=_cents(drawn),
        remaining_usd=_cents(granted - drawn),
        settled_through=f"{last:%Y-%m}" if last is not None else None,
    )


# --- writes ---------------------------------------------------------------------------


async def freeze_drawdown(
    session: AsyncSession,
    *,
    tenant_id: str,
    month_start: date,
    amount_usd: Decimal,
    invoice_id: uuid.UUID,
) -> Decimal:
    """Write ``invoice_id``'s drawdown ONCE and prove the balance is still >= 0.

    The caller holds :func:`lock_tenant_credit`. The insert is
    ``ON CONFLICT DO NOTHING`` on ``(tenant_id, 'usage:<invoice_id>')``, so a
    retried freeze for the same invoice writes nothing. ``close_period`` calls it
    only when credit > 0 was applied; a month closed with none is frozen by its
    invoice (``prepaid_credit_applied_usd = 0``), which ``credit_for_month`` reads.

    :raises CreditOverdrawnError: the balance would go negative. The caller's
        transaction (the invoice INSERT with it) must roll back.
    """
    await session.execute(
        pg_insert(CreditLedgerEntry)
        .values(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            kind=KIND_DRAWDOWN,
            entry_key=drawdown_entry_key(invoice_id),
            amount_usd=_cents(amount_usd),
            period_start=month_start,
            invoice_id=invoice_id,
            created_at=datetime.now(UTC),
        )
        .on_conflict_do_nothing(constraint="uq_credit_ledger_entry")
    )
    balance = await credit_balance(session, tenant_id)
    if balance.remaining_usd < 0:
        logger.error(
            "prepaid_credit_overdraft_refused",
            tenant_id=tenant_id,
            month=f"{month_start:%Y-%m}",
            remaining_usd=str(balance.remaining_usd),
        )
        raise CreditOverdrawnError(tenant_id, balance.remaining_usd)
    stored = (
        await session.execute(
            select(CreditLedgerEntry.amount_usd).where(
                CreditLedgerEntry.tenant_id == tenant_id,
                CreditLedgerEntry.entry_key == drawdown_entry_key(invoice_id),
            )
        )
    ).scalar_one()
    return Decimal(stored)


async def reverse_drawdown(
    session: AsyncSession, *, tenant_id: str, invoice_id: uuid.UUID
) -> Decimal | None:
    """Give back the credit a voided invoice drew. Idempotent; None when it drew none.

    Appends a ``drawdown_reversal`` for exactly that drawdown's amount, in the
    void's own transaction. The month is then open again on the ledger, and its
    re-close freezes a fresh drawdown on the new invoice.
    """
    drawdown = (
        await session.execute(
            select(CreditLedgerEntry).where(
                CreditLedgerEntry.tenant_id == tenant_id,
                CreditLedgerEntry.entry_key == drawdown_entry_key(invoice_id),
            )
        )
    ).scalar_one_or_none()
    if drawdown is None:
        return None
    await session.execute(
        pg_insert(CreditLedgerEntry)
        .values(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            kind=KIND_REVERSAL,
            entry_key=reversal_entry_key(invoice_id),
            amount_usd=drawdown.amount_usd,
            period_start=drawdown.period_start,
            invoice_id=invoice_id,
            created_at=datetime.now(UTC),
        )
        .on_conflict_do_nothing(constraint="uq_credit_ledger_entry")
    )
    return Decimal(drawdown.amount_usd)


class TopupConflict(Exception):  # noqa: N818 - a verdict
    """409: this idempotency key was already used with a different body."""

    def __init__(self, reason: str, **detail: Any) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class TopupResult:
    entry: CreditLedgerEntry
    charge: TenantAccessCharge
    replayed: bool


async def _entry(session: AsyncSession, tenant_id: str, entry_key: str) -> CreditLedgerEntry | None:
    return (
        await session.execute(
            select(CreditLedgerEntry).where(
                CreditLedgerEntry.tenant_id == tenant_id,
                CreditLedgerEntry.entry_key == entry_key,
            )
        )
    ).scalar_one_or_none()


async def _replay(
    session: AsyncSession, existing: CreditLedgerEntry, *, amount: int, currency: str
) -> TopupResult:
    if (existing.source_amount, existing.source_currency) != (amount, currency):
        raise TopupConflict(
            "idempotency_key_reused",
            topup_id=str(existing.id),
            amount=existing.source_amount,
            currency=existing.source_currency,
        )
    charge = (
        await session.execute(
            select(TenantAccessCharge).where(TenantAccessCharge.id == existing.access_charge_id)
        )
    ).scalar_one()
    return TopupResult(entry=existing, charge=charge, replayed=True)


async def topup_credit(
    session: AsyncSession,
    tenant_id: str,
    *,
    amount: int,
    currency: str,
    idempotency_key: str,
    actor_sub: str | None,
    actor_role: str | None,
    via_client: str | None,
    now: datetime | None = None,
) -> TopupResult:
    """Grant ``amount`` of ``currency`` as USD credit and invoice it, once per key.

    In the CALLER's transaction (flush only; the route commits once):

    1. a known ``(tenant, 'topup:'||key)`` is a REPLAY. The same body returns
       the original entry and writes nothing; a different body raises
       :class:`TopupConflict`;
    2. price it in USD at the FX rate effective now. A missing rate raises
       :class:`MoneyRefusal` ``fx_rate_missing``, and nothing is written;
    3. write the credit entry and its ``one_time`` access charge (no allowance:
       the credit is applied by the ledger, not by ``included_cost_usd``).

    Concurrency: two first uses of one key race on ``uq_credit_ledger_entry``.
    The loser's INSERT ... ON CONFLICT DO NOTHING returns no row, and it replays.
    """
    now = now or datetime.now(UTC)
    key = topup_entry_key(idempotency_key)
    existing = await _entry(session, tenant_id, key)
    if existing is not None:
        return await _replay(session, existing, amount=amount, currency=currency)

    rate = await fx_rate_at(session, now, quote=currency)
    if rate is None:
        raise MoneyRefusal("fx_rate_missing")
    amount_usd = topup_amount_usd(amount, rate)

    entry_id = uuid.uuid4()
    inserted = (
        await session.execute(
            pg_insert(CreditLedgerEntry)
            .values(
                id=entry_id,
                tenant_id=tenant_id,
                kind=KIND_TOPUP,
                entry_key=key,
                amount_usd=amount_usd,
                source_amount=amount,
                source_currency=currency,
                fx_rate=rate,
                actor_sub=actor_sub,
                actor_role=actor_role,
                via_client=via_client,
                created_at=now,
            )
            .on_conflict_do_nothing(constraint="uq_credit_ledger_entry")
            .returning(CreditLedgerEntry.id)
        )
    ).scalar_one_or_none()
    if inserted is None:
        raced = await _entry(session, tenant_id, key)
        assert raced is not None  # the conflicting row is committed and visible
        return await _replay(session, raced, amount=amount, currency=currency)

    charge, _prior = await access_charge_svc.upsert_access_charge(
        session,
        AccessChargeCreate(
            tenant_id=tenant_id,
            service_key=f"{TOPUP_SERVICE_KEY_PREFIX}{entry_id}",
            amount_usd=amount_usd,
            cadence="one_time",
            included_cost_usd=None,
            effective_from=now,
            notes=(
                f"Prepaid credit top-up {amount} {currency} = {amount_usd} USD "
                f"(idempotency_key={idempotency_key}, fx USD->{currency}={rate.normalize():f})"
            ),
        ),
        ts=now,
    )
    entry = await _entry(session, tenant_id, key)
    assert entry is not None
    entry.access_charge_id = charge.id
    await session.flush()
    return TopupResult(entry=entry, charge=charge, replayed=False)
