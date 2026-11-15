"""Prepaid credit is invoiced ONCE and then spent down (alembic 0055).

Owner ruling 2026-10-13, the four scenarios it named, end to end through the real
``generate_billing`` and ``close_period``:

1. top-up 100, usage 30  -> the statement bills 100 (the top-up) and 0 for usage;
   70 credit left.
2. next period, usage 90 -> bills 20; 0 credit left.
3. a replayed top-up is idempotent.
4. a period close freezes the drawdown exactly once; a VOID reverses it, so the
   re-close freezes the corrected figure and invoice and ledger always agree.

And the two money bugs the #267 audit reproduced (each test names its probe):
months closed OUT OF ORDER must not spend credit twice, and a void + re-close
must not keep the stale debit. Plus: an overdrawing freeze is refused, and two
closes for one tenant serialise on the credit lock.

Plus the regression the skip rule would otherwise cause: a month whose usage the
credit covers in full nets to $0 with no fee and must STILL close, or its drawdown
is never frozen and the same credit pays for the next month too.

USD throughout so the figures are the owner's figures (no FX).
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from models.db import (
    BillingInvoice,
    CostDaily,
    CreditLedgerEntry,
    MarkupRule,
    TenantAccessCharge,
)
from services import prepaid_credit as credit_svc
from services.billing import generate_billing
from services.invoices import close_period, reproduce, void
from tests.conftest import T
from tests.test_invoices import _patch_fetch_licenses

pytestmark = pytest.mark.asyncio

#: The billed product. A named constant, not an inline kwarg literal, so the
#: detect-secrets keyword plugin does not read `service_key="..."` as a credential.
PRODUCT = "workers"

APR, MAY, JUN = date(2026, 4, 1), date(2026, 5, 1), date(2026, 6, 1)


@pytest.fixture(autouse=True)
def _byo_mode_answers(monkeypatch):
    """Same licences byo-mode stub as tests/test_invoices.py (close_period gates on it)."""
    import httpx

    from services import byo_mode as byo

    byo.clear_byo_cache()
    real_get = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if "byo-mode" in str(url):
            return httpx.Response(200, json={"byo_mode": "byo_required"})
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    yield
    byo.clear_byo_cache()


def _licences(lic: uuid.UUID) -> list[dict]:
    return [{"id": str(lic), "app": {"key": "workers"}, "plan": {"code": "pro"}}]


async def _seed_month(session, tenant: str, lic: uuid.UUID, start: date, end: date, total: str):  # noqa: ANN001
    """Every day of the month (the bucket-gap gate), summing to exactly ``total``.

    The whole amount sits on the first day and every other day is $0, so any
    month length works without a fractional daily figure.
    """
    d = start
    while d < end:
        session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic,
                service_key=PRODUCT,
                provider="test",
                bucket_date=d,
                total_input_tokens=1_000,
                total_output_tokens=200,
                total_cost_usd=Decimal(total) if d == start else Decimal("0"),
                request_count=10,
            )
        )
        d += timedelta(days=1)
    await session.flush()


def _zero_markup(tenant: str) -> MarkupRule:
    """Billed == incurred, so 30 of usage bills exactly 30 (tenant-pinned, so it wins)."""
    return MarkupRule(
        tenant_id=tenant,
        markup_pct=Decimal("0.00"),
        effective_from=datetime(2025, 1, 1, tzinfo=UTC),
    )


async def _topup(session, tenant: str, amount: int, when: datetime, key: str = "cp-credit-0001"):  # noqa: ANN001
    result = await credit_svc.topup_credit(
        session,
        tenant,
        amount=amount,
        currency="USD",
        idempotency_key=key,
        actor_sub=None,
        actor_role=None,
        via_client=None,
        now=when,
    )
    await session.commit()
    return result


def _next(month_start: date) -> date:
    return (month_start + timedelta(days=32)).replace(day=1)


async def _close(session, tenant: str, lic: uuid.UUID, start: date, test_settings):  # noqa: ANN001
    close_patch, gen_patch = _patch_fetch_licenses(_licences(lic))
    with close_patch as a, gen_patch as b:
        a.return_value = _licences(lic)
        b.return_value = _licences(lic)
        inv = await close_period(
            session,
            tenant_id=tenant,
            period_start=start,
            period_end=_next(start),
            settings=test_settings,
        )
    await session.commit()
    return inv


async def _drawdowns(session, tenant: str) -> list[CreditLedgerEntry]:  # noqa: ANN001
    rows = await session.execute(
        select(CreditLedgerEntry).where(
            CreditLedgerEntry.tenant_id == tenant,
            CreditLedgerEntry.kind == credit_svc.KIND_DRAWDOWN,
        )
    )
    return list(rows.scalars().all())


async def _setup(db_session, label: str, *, april: str, may: str | None = None):  # noqa: ANN001, ANN202
    tenant, lic = T(label), uuid.uuid4()
    db_session.add(_zero_markup(tenant))
    await _seed_month(db_session, tenant, lic, APR, MAY, april)
    if may is not None:
        await _seed_month(db_session, tenant, lic, MAY, JUN, may)
    await db_session.commit()
    return tenant, lic


async def test_topup_100_usage_30_bills_100_and_leaves_70(db_session, test_settings) -> None:
    tenant, lic = await _setup(db_session, "credit-100-30", april="30")
    await _topup(db_session, tenant, 100, datetime(2026, 4, 5, tzinfo=UTC))

    preview = (await generate_billing(tenant, "2026-04", db_session, test_settings))["totals"]
    assert Decimal(preview["access_charge_usd"]) == Decimal("100.00")  # the top-up line
    assert Decimal(preview["prepaid_credit_applied_usd"]) == Decimal("30.00")
    assert Decimal(preview["customer_cost_usd"]) == Decimal("100.00")  # 0 for usage
    assert Decimal(preview["prepaid_credit"]["remaining_usd"]) == Decimal("70.00")

    inv = await _close(db_session, tenant, lic, APR, test_settings)
    assert inv.total_usd == Decimal("100.00")
    assert inv.prepaid_credit_applied_usd == Decimal("30.00")
    assert inv.llm_usage_usd == Decimal("30")
    bal = await credit_svc.credit_balance(db_session, tenant)
    assert (bal.granted_usd, bal.drawn_usd, bal.remaining_usd) == (
        Decimal("100.00"),
        Decimal("30.00"),
        Decimal("70.00"),
    )
    assert bal.settled_through == "2026-04"
    check = await reproduce(db_session, invoice_id=inv.id)
    assert check["digest_matches"] and check["money_matches_snapshot"], check


async def test_next_period_usage_90_bills_20_and_leaves_0(db_session, test_settings) -> None:
    tenant, lic = await _setup(db_session, "credit-next-90", april="30", may="90")
    await _topup(db_session, tenant, 100, datetime(2026, 4, 5, tzinfo=UTC))
    await _close(db_session, tenant, lic, APR, test_settings)

    may = await _close(db_session, tenant, lic, MAY, test_settings)
    assert may.total_usd == Decimal("20.00")
    assert may.prepaid_credit_applied_usd == Decimal("70.00")
    assert may.access_charge_usd == Decimal("0.00")  # the top-up bills once, in April
    assert (await credit_svc.credit_balance(db_session, tenant)).remaining_usd == Decimal("0.00")


async def test_a_replayed_topup_is_idempotent(db_session, test_settings) -> None:
    tenant, _lic = await _setup(db_session, "credit-replay", april="30")
    when = datetime(2026, 4, 5, tzinfo=UTC)
    first = await _topup(db_session, tenant, 100, when)
    again = await _topup(db_session, tenant, 100, when)
    assert again.replayed and not first.replayed
    assert again.entry.id == first.entry.id and again.charge.id == first.charge.id
    with pytest.raises(credit_svc.TopupConflict):
        await _topup(db_session, tenant, 101, when)
    await db_session.rollback()

    n_credit = (
        await db_session.execute(
            select(func.count())
            .select_from(CreditLedgerEntry)
            .where(CreditLedgerEntry.tenant_id == tenant)
        )
    ).scalar()
    n_lines = (
        await db_session.execute(
            select(func.count())
            .select_from(TenantAccessCharge)
            .where(
                TenantAccessCharge.tenant_id == tenant,
                TenantAccessCharge.service_key.like("topup:%"),
            )
        )
    ).scalar()
    assert (n_credit, n_lines) == (1, 1)
    totals = (await generate_billing(tenant, "2026-04", db_session, test_settings))["totals"]
    assert Decimal(totals["access_charge_usd"]) == Decimal("100.00")


async def _invoice_ledger_agree(session, tenant: str) -> tuple[Decimal, Decimal]:  # noqa: ANN001
    """(Σ credit applied by LIVE invoices, Σ live ledger drawdowns) -- must be equal."""
    applied = (
        await session.execute(
            select(func.coalesce(func.sum(BillingInvoice.prepaid_credit_applied_usd), 0)).where(
                BillingInvoice.tenant_id == tenant, BillingInvoice.status != "void"
            )
        )
    ).scalar_one()
    bal = await credit_svc.credit_balance(session, tenant)
    return Decimal(applied), bal.drawn_usd


async def test_period_close_freezes_the_drawdown_exactly_once(db_session, test_settings) -> None:
    tenant, lic = await _setup(db_session, "credit-freeze", april="30")
    await _topup(db_session, tenant, 100, datetime(2026, 4, 5, tzinfo=UTC))
    first = await _close(db_session, tenant, lic, APR, test_settings)
    again = await _close(db_session, tenant, lic, APR, test_settings)  # no-op close
    assert again.id == first.id
    assert [d.amount_usd for d in await _drawdowns(db_session, tenant)] == [Decimal("30.00000000")]
    # A re-render of the closed month applies exactly the frozen figure.
    totals = (await generate_billing(tenant, "2026-04", db_session, test_settings))["totals"]
    assert totals["prepaid_credit"]["frozen"] is True
    assert Decimal(totals["prepaid_credit_applied_usd"]) == Decimal("30.00")
    applied, drawn = await _invoice_ledger_agree(db_session, tenant)
    assert applied == drawn == Decimal("30.00")


async def test_void_reverses_the_drawdown_and_the_reclose_freezes_the_new_figure(
    db_session, test_settings
) -> None:
    """#267 audit probe 2: close April applying 30, void, correct usage to 20, re-close.

    Before the fix the invoice applied 20 while the ledger still debited 30 (the
    stale drawdown survived ON CONFLICT DO NOTHING) -- 10 of paid credit lost.
    """
    tenant, lic = await _setup(db_session, "credit-void", april="30")
    await _topup(db_session, tenant, 100, datetime(2026, 4, 5, tzinfo=UTC))
    first = await _close(db_session, tenant, lic, APR, test_settings)
    assert first.prepaid_credit_applied_usd == Decimal("30.00")

    await void(db_session, invoice_id=first.id, reason="test: usage corrected", actor="test")
    await db_session.commit()
    assert (await credit_svc.credit_balance(db_session, tenant)).remaining_usd == Decimal("100.00")
    await db_session.execute(
        update(CostDaily)
        .where(CostDaily.tenant_id == tenant, CostDaily.bucket_date == APR)
        .values(total_cost_usd=Decimal("20"))
    )
    await db_session.commit()

    reclosed = await _close(db_session, tenant, lic, APR, test_settings)
    assert reclosed.id != first.id
    assert reclosed.prepaid_credit_applied_usd == Decimal("20.00")
    assert reclosed.total_usd == Decimal("100.00")  # the top-up line; usage fully covered
    bal = await credit_svc.credit_balance(db_session, tenant)
    assert (bal.drawn_usd, bal.remaining_usd) == (Decimal("20.00"), Decimal("80.00"))
    applied, drawn = await _invoice_ledger_agree(db_session, tenant)
    assert applied == drawn == Decimal("20.00")
    # Append-only: the first drawdown, its reversal, and the new drawdown.
    kinds = sorted(
        (
            await db_session.execute(
                select(CreditLedgerEntry.kind).where(
                    CreditLedgerEntry.tenant_id == tenant,
                    CreditLedgerEntry.kind != credit_svc.KIND_TOPUP,
                )
            )
        )
        .scalars()
        .all()
    )
    assert kinds == ["drawdown_reversal", "usage_drawdown", "usage_drawdown"]


async def test_out_of_order_closes_never_spend_credit_twice(db_session, test_settings) -> None:
    """#267 audit probe 1: top-up 100 in March; April 30, May 90; close MAY first.

    Before the fix April saw only drawdowns of EARLIER months (none), applied 30
    on top of May's 90 -> drawn 120, remaining -20.
    """
    tenant, lic = await _setup(db_session, "credit-order", april="30", may="90")
    await _topup(db_session, tenant, 100, datetime(2026, 3, 10, tzinfo=UTC))

    may = await _close(db_session, tenant, lic, MAY, test_settings)
    april = await _close(db_session, tenant, lic, APR, test_settings)
    assert may.prepaid_credit_applied_usd == Decimal("90.00")
    assert april.prepaid_credit_applied_usd == Decimal("10.00")  # only what was left
    assert april.total_usd == Decimal("20.00")
    bal = await credit_svc.credit_balance(db_session, tenant)
    assert (bal.drawn_usd, bal.remaining_usd) == (Decimal("100.00"), Decimal("0.00"))
    applied, drawn = await _invoice_ledger_agree(db_session, tenant)
    assert applied == drawn == Decimal("100.00")


async def test_a_freeze_that_would_overdraw_is_refused(db_session) -> None:
    tenant = T("credit-overdraw")
    await _topup(db_session, tenant, 100, datetime(2026, 4, 5, tzinfo=UTC))
    with pytest.raises(credit_svc.CreditOverdrawnError):
        await credit_svc.freeze_drawdown(
            db_session,
            tenant_id=tenant,
            month_start=APR,
            amount_usd=Decimal("150"),
            invoice_id=uuid.uuid4(),
        )
    await db_session.rollback()
    assert await _drawdowns(db_session, tenant) == []


async def test_concurrent_closes_for_one_tenant_serialise(
    db_session, db_session_factory, test_settings
) -> None:
    """close_period takes the per-tenant credit lock before it reads any credit."""
    tenant, lic = await _setup(db_session, "credit-lock", april="30")
    async with db_session_factory() as holder, db_session_factory() as closer:
        await credit_svc.lock_tenant_credit(holder, tenant)  # held, uncommitted
        await closer.execute(text("SET LOCAL lock_timeout = '300ms'"))
        with pytest.raises(DBAPIError, match="lock timeout"):
            await _close(closer, tenant, lic, APR, test_settings)
        await closer.rollback()
        await holder.rollback()


async def test_a_fully_covered_month_still_closes_and_freezes(db_session, test_settings) -> None:
    """Credit bought in March covers all of April: $0 total, no fee -- still a statement."""
    tenant, lic = await _setup(db_session, "credit-covered", april="30")
    # March: no usage, so the top-up line is March's (and March is not closed here).
    await _topup(db_session, tenant, 100, datetime(2026, 3, 10, tzinfo=UTC))

    inv = await _close(db_session, tenant, lic, APR, test_settings)
    assert inv.total_usd == Decimal("0.00")
    assert inv.prepaid_credit_applied_usd == Decimal("30.00")
    assert [d.amount_usd for d in await _drawdowns(db_session, tenant)] == [Decimal("30.00000000")]
    assert (await credit_svc.credit_balance(db_session, tenant)).remaining_usd == Decimal("70.00")


# --- #267 re-audit -----------------------------------------------------------------------


@pytest.mark.parametrize(("usage", "applied"), [("10.005", "10.00"), ("99.995", "99.99")])
async def test_credit_rounds_down_so_a_statement_is_never_negative(
    db_session, test_settings, usage, applied
) -> None:
    """Re-audit item 1: half-up credit on 10.005 of usage billed -0.01.

    Credit bought in March, so April carries no top-up line: the statement is
    usage minus credit alone, i.e. exactly the sub-cent residual.
    """
    tenant, lic = await _setup(db_session, f"credit-round-{usage}", april=usage)
    await _topup(db_session, tenant, 100, datetime(2026, 3, 10, tzinfo=UTC))
    inv = await _close(db_session, tenant, lic, APR, test_settings)
    assert inv.prepaid_credit_applied_usd == Decimal(applied)
    assert inv.total_usd == Decimal("0.01")  # q(0.005), never -0.01
    applied_sum, drawn = await _invoice_ledger_agree(db_session, tenant)
    assert applied_sum == drawn == Decimal(applied)


async def test_a_negative_total_is_refused_at_write_time(
    db_session, test_settings, monkeypatch
) -> None:
    """Any path to total < 0 (identity intact) is a refused close, not an issued one."""
    import services.invoices as invoices_mod

    real = invoices_mod._money_from_snapshot

    def _negative(snapshot):  # noqa: ANN001, ANN202
        money = real(snapshot)
        shift = Decimal(str(money["total_usd"])) + Decimal("0.01")
        money["total_usd"] = money["subtotal_usd"] = Decimal("-0.01")
        money["prepaid_credit_applied_usd"] = (
            Decimal(str(money["prepaid_credit_applied_usd"])) + shift
        )
        return money

    tenant, lic = await _setup(db_session, "credit-negative", april="30")
    await _topup(db_session, tenant, 100, datetime(2026, 3, 10, tzinfo=UTC))
    monkeypatch.setattr(invoices_mod, "_money_from_snapshot", _negative)
    with pytest.raises(invoices_mod.InvoiceNegativeTotalError):
        await _close(db_session, tenant, lic, APR, test_settings)
    await db_session.rollback()
    assert (
        await db_session.execute(
            select(func.count())
            .select_from(BillingInvoice)
            .where(BillingInvoice.tenant_id == tenant)
        )
    ).scalar() == 0


async def test_a_month_closed_without_credit_stays_uncredited_after_a_later_topup(
    db_session, test_settings
) -> None:
    """Re-audit item 4: the report must read the frozen INVOICE, not re-apply new credit."""
    tenant, lic = await _setup(db_session, "credit-later-topup", april="30")
    closed = await _close(db_session, tenant, lic, APR, test_settings)
    assert closed.prepaid_credit_applied_usd == Decimal("0.00")
    await _topup(db_session, tenant, 100, datetime(2026, 5, 10, tzinfo=UTC))

    totals = (await generate_billing(tenant, "2026-04", db_session, test_settings))["totals"]
    assert totals["prepaid_credit"]["frozen"] is True
    assert Decimal(totals["prepaid_credit_applied_usd"]) == Decimal("0.00")
    assert Decimal(totals["customer_cost_usd"]) == closed.total_usd == Decimal("30.00")
    assert (await credit_svc.credit_balance(db_session, tenant)).remaining_usd == Decimal("100.00")


async def test_void_takes_the_tenant_credit_lock(
    db_session, db_session_factory, test_settings
) -> None:
    """void() reverses credit, so it must serialise with closes on the same lock."""
    tenant, lic = await _setup(db_session, "credit-void-lock", april="30")
    await _topup(db_session, tenant, 100, datetime(2026, 4, 5, tzinfo=UTC))
    inv = await _close(db_session, tenant, lic, APR, test_settings)
    async with db_session_factory() as holder, db_session_factory() as voider:
        await credit_svc.lock_tenant_credit(holder, tenant)  # held, uncommitted
        await voider.execute(text("SET LOCAL lock_timeout = '300ms'"))
        with pytest.raises(DBAPIError, match="lock timeout"):
            await void(voider, invoice_id=inv.id, reason="test: lock", actor="test")
        await voider.rollback()
        await holder.rollback()


#: ``_digest`` of the fixed statement below, computed by ``origin/main`` (ed6e1ee,
#: before alembic 0055 existed) -- i.e. what every pre-0055 invoice's
#: ``snapshot_sha256`` was computed WITH. Recomputed on this branch it must not move.
_PRE_0055_DIGEST = "88b10e2ac83ba048a337f7e6e7dc8b7925f43ffe102060f37f1f6fa63de95920"  # pragma: allowlist secret - a sha256 digest, not a credential


async def test_a_pre_0055_invoice_keeps_its_exact_digest() -> None:
    """Re-audit item 3: a zero-credit statement hashes exactly as before 0055."""
    from services.invoices import _MONEY_COLUMNS, _digest

    snapshot = {
        "totals": {"customer_cost_usd": "12.34", "access_charge_usd": "10.00"},
        "licenses": [],
    }
    inputs = {"period": "2026-08", "allowance": {"opening_total": "0"}}
    money: dict[str, Decimal | None] = {
        c: Decimal("0") for c in _MONEY_COLUMNS
    }  # includes the 0055 column, at 0
    money.update(
        subtotal_usd=Decimal("12.34"),
        total_usd=Decimal("12.34"),
        access_charge_usd=Decimal("10.00"),
        llm_usage_usd=Decimal("2.34"),
        internal_cost_usd=Decimal("2.34"),
        margin_usd=Decimal("10.00"),
        seat_count=None,
    )
    assert "prepaid_credit_applied_usd" in money
    assert _digest(snapshot, inputs, money) == _PRE_0055_DIGEST
    # ...while a statement that DID apply credit is covered by the digest.
    money["prepaid_credit_applied_usd"] = Decimal("0.01")
    assert _digest(snapshot, inputs, money) != _PRE_0055_DIGEST


async def test_remaining_credit_is_right_for_an_open_and_a_closed_month(
    db_engine, db_session, test_settings, fake_redis, monkeypatch
) -> None:
    """#267 third audit: a CLOSED month's remaining subtracted its own drawdown twice.

    Top-up 100, April usage 90. Before the fix the closed month read
    ``available 10 / remaining -80`` (the ledger said 10), and the preview route
    handed the -80 straight to CP.
    """
    from tests._money_factory import money_client

    tenant, lic = await _setup(db_session, "credit-remaining", april="90")
    await _topup(db_session, tenant, 100, datetime(2026, 4, 5, tzinfo=UTC))

    async def _views() -> tuple[dict, dict]:
        report = (await generate_billing(tenant, "2026-04", db_session, test_settings))["totals"]
        async with money_client(db_engine, monkeypatch, fake_redis) as c:
            r = await c.get(
                "/api/billing/invoices/preview", params={"tenant_id": tenant, "period": "2026-04"}
            )
        assert r.status_code == 200, r.text
        return report["prepaid_credit"], r.json()["data"]

    for state in ("open", "closed"):
        if state == "closed":
            await _close(db_session, tenant, lic, APR, test_settings)
        credit, preview = await _views()
        assert credit["frozen"] is (state == "closed"), state
        assert (credit["available_usd"], credit["applied_usd"], credit["remaining_usd"]) == (
            "100.00",
            "90.00",
            "10.00",
        ), (state, credit)
        assert Decimal(preview["prepaid_credit_remaining_usd"]) == Decimal("10.00"), state
        assert Decimal(preview["prepaid_credit_applied_usd"]) == Decimal("90.00"), state
    assert (await credit_svc.credit_balance(db_session, tenant)).remaining_usd == Decimal("10.00")
