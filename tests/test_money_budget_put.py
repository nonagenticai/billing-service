"""PUT /api/budget/money/{tenant_id}, GET quote/status, PUT/GET /api/config/fx-rates.

Contract (PLAN rev E §3.1):

    PUT  /api/budget/money/{tenant_id}  {"amount":100,"currency":"EUR","period":"monthly",
                                         "source":"provisioning"}
         -> 200 {budget_id, amount, currency, budget_unit:"money"}
    GET  /api/budget/money/quote?tenant_id&setup&provider&model&input_tokens&output_tokens
         -> 200 {"currency":"EUR","amount":"…","markup_pct":"25.00","fx_rate":"0.92…"}
          | 422 {"reason":"price_missing|fx_rate_missing|markup_missing"}
    GET  /api/budget/money/status/{tenant_id}
         -> {amount, consumed, remaining, currency, thresholds_crossed:[50,80]}
    PUT  /api/config/fx-rates  {"base":"USD","quote":"EUR","rate":"0.9200","effective_from":"…"}
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from models.db import FxRate, MoneyBudget, TenantConfig, TokenBudget
from tests._money_factory import (
    DS_IN,
    DS_OUT,
    FX,
    fx_row,
    markup_row,
    money_budget,
    money_client,
    pricing_rows,
    q8,
    seed,
    usd,
)
from tests.conftest import T


def _put_body(**over) -> dict:
    body = {"amount": 10, "currency": "EUR", "period": "monthly", "source": "provisioning"}
    body.update(over)
    return body


@pytest.mark.asyncio
async def test_put_creates_budget_and_flips_budget_unit(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    t = T("money-put")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put(f"/api/budget/money/{t}", json=_put_body(amount=10))

    assert r.status_code == 200, r.text
    d = r.json()
    assert d["amount"] == 10
    assert d["currency"] == "EUR"
    assert d["budget_unit"] == "money"
    assert d["budget_id"]

    async with db_session_factory() as s:
        mb = (await s.execute(select(MoneyBudget).where(MoneyBudget.tenant_id == t))).scalar_one()
        assert mb.amount == 10 and mb.active and mb.source == "provisioning"
        tc = (await s.execute(select(TenantConfig).where(TenantConfig.tenant_id == t))).scalar_one()
        assert tc.budget_unit == "money"
        # Exit proof 1a: 0 token_budgets rows for an EUR tenant.
        n = (
            await s.execute(
                select(func.count()).select_from(TokenBudget).where(TokenBudget.tenant_id == t)
            )
        ).scalar()
        assert n == 0


@pytest.mark.asyncio
async def test_put_is_idempotent_same_budget_id(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    t = T("money-put-idem")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        a = (await c.put(f"/api/budget/money/{t}", json=_put_body(amount=10))).json()
        b = (await c.put(f"/api/budget/money/{t}", json=_put_body(amount=10))).json()
        c2 = (await c.put(f"/api/budget/money/{t}", json=_put_body(amount=25))).json()

    assert a["budget_id"] == b["budget_id"] == c2["budget_id"]
    assert c2["amount"] == 25
    async with db_session_factory() as s:
        n = (
            await s.execute(
                select(func.count()).select_from(MoneyBudget).where(MoneyBudget.tenant_id == t)
            )
        ).scalar()
        assert n == 1


@pytest.mark.asyncio
async def test_put_does_not_write_a_markup_nobody_chose(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    """F1: the PUT creates tenant_config only to flip budget_unit; markup stays unset.

    ``get_markup_pct_with_source`` must keep answering from the fallback, not
    report the code default as a tenant-configured markup.
    """
    from services.billing import get_markup_pct_map, get_markup_pct_with_source
    from services.markup import MARKUP_SOURCE_SETTINGS
    from settings import get_settings

    t = T("money-put-markup")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put(f"/api/budget/money/{t}", json=_put_body(amount=10))
        cfg = await c.get(f"/api/config/tenants/{t}")
    assert r.status_code == 200, r.text

    async with db_session_factory() as s:
        tc = (await s.execute(select(TenantConfig).where(TenantConfig.tenant_id == t))).scalar_one()
        assert tc.budget_unit == "money"
        assert tc.markup_pct is None
        settings = get_settings()
        pct, source = await get_markup_pct_with_source(t, s, settings)
        assert source == MARKUP_SOURCE_SETTINGS
        assert pct == settings.default_markup_pct
        assert (await get_markup_pct_map([t], s, settings)) == {t: settings.default_markup_pct}
    assert cfg.status_code == 200, cfg.text
    assert cfg.json()["data"]["markup_pct"] == get_settings().default_markup_pct


@pytest.mark.asyncio
async def test_put_keeps_an_existing_tenant_markup(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    """An operator-set markup survives the PUT; only budget_unit changes."""
    t = T("money-put-keep")
    await seed(db_session, TenantConfig(tenant_id=t, markup_pct=Decimal("12.50")))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put(f"/api/budget/money/{t}", json=_put_body(amount=10))
    assert r.status_code == 200, r.text
    async with db_session_factory() as s:
        tc = (await s.execute(select(TenantConfig).where(TenantConfig.tenant_id == t))).scalar_one()
    assert tc.markup_pct == Decimal("12.50")
    assert tc.budget_unit == "money"


@pytest.mark.asyncio
async def test_concurrent_first_puts_do_not_500(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    """Two simultaneous first PUTs for one tenant: both 200, one budget, one config row.

    Separate sessions/connections (money_client opens one per request), so the
    two INSERTs genuinely race on the partial unique index and the
    tenant_config unique key.
    """
    import asyncio

    for i in range(5):
        t = T(f"money-put-race-{i}")
        async with money_client(db_engine, monkeypatch, fake_redis) as c:
            a, b = await asyncio.gather(
                c.put(f"/api/budget/money/{t}", json=_put_body(amount=10)),
                c.put(f"/api/budget/money/{t}", json=_put_body(amount=10)),
            )
        assert (a.status_code, b.status_code) == (200, 200), (a.text, b.text)
        assert a.json()["budget_id"] == b.json()["budget_id"]
        async with db_session_factory() as s:
            n_b = (
                await s.execute(
                    select(func.count()).select_from(MoneyBudget).where(MoneyBudget.tenant_id == t)
                )
            ).scalar()
            n_c = (
                await s.execute(
                    select(func.count())
                    .select_from(TenantConfig)
                    .where(TenantConfig.tenant_id == t)
                )
            ).scalar()
        assert (n_b, n_c) == (1, 1)


@pytest.mark.parametrize(
    "bad",
    [
        {"amount": 10.5},
        {"amount": "10"},
        {"amount": -1},
        # REV F: EUR/USD/CAD are valid; anything else is not.
        {"currency": "GBP"},
        {"source": "elsewhere"},
    ],
)
@pytest.mark.asyncio
async def test_put_rejects_non_integer_eur(bad, db_engine, fake_redis, monkeypatch):
    t = T("money-put-422")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put(f"/api/budget/money/{t}", json=_put_body(**bad))
    assert r.status_code == 422, r.text


@pytest.mark.asyncio
async def test_quote_setup2(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-quote")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        markup_row(t, provider="deepseek", model="deepseek-v4-pro", byo=False, pct="25.00"),
    )
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.get(
            "/api/budget/money/quote",
            params={
                "tenant_id": t,
                "setup": 2,
                "provider": "deepseek",
                "model": "deepseek-v4-pro",
                "input_tokens": 1000,
                "output_tokens": 1000,
            },
        )
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["currency"] == "EUR"
    assert d["markup_pct"] == "25.00"
    assert Decimal(d["fx_rate"]) == FX
    assert Decimal(d["amount"]) == q8(usd(1000, 1000, DS_IN, DS_OUT) * Decimal("1.25") * FX)


@pytest.mark.parametrize(
    ("seeded", "reason"),
    [("no_price", "price_missing"), ("no_fx", "fx_rate_missing"), ("no_markup", "markup_missing")],
)
@pytest.mark.asyncio
async def test_quote_422_reasons(seeded, reason, db_engine, db_session, fake_redis, monkeypatch):
    t = T(f"money-quote-{seeded}")
    objs: list = []
    if seeded != "no_price":
        objs += pricing_rows()
    if seeded != "no_fx":
        objs.append(fx_row())
    if seeded != "no_markup":
        objs.append(
            markup_row(t, provider="deepseek", model="deepseek-v4-pro", byo=True, pct="10.00")
        )
    await seed(db_session, *objs)
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.get(
            "/api/budget/money/quote",
            params={
                "tenant_id": t,
                "setup": 3,
                "provider": "deepseek",
                "model": "deepseek-v4-pro",
                "input_tokens": 1000,
                "output_tokens": 1000,
            },
        )
    assert r.status_code == 422, r.text
    assert r.json() == {"reason": reason}


@pytest.mark.asyncio
async def test_status_reports_thresholds(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-status")
    await seed(db_session, money_budget(t, amount=100, consumed="85"))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.get(f"/api/budget/money/status/{t}", headers={"X-Tenant-Id": t})
        other = await c.get(f"/api/budget/money/status/{t}", headers={"X-Tenant-Id": T("nosy")})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["currency"] == "EUR"
    assert d["amount"] == 100
    assert Decimal(d["consumed"]) == Decimal("85")
    assert Decimal(d["remaining"]) == Decimal("15")
    assert d["thresholds_crossed"] == [50, 80]
    assert other.status_code == 403


@pytest.mark.asyncio
async def test_fx_rate_put_closes_the_open_row(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    eff1 = datetime.now(UTC) - timedelta(days=2)
    eff2 = datetime.now(UTC) - timedelta(days=1)
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r1 = await c.put(
            "/api/config/fx-rates",
            json={
                "base": "USD",
                "quote": "EUR",
                "rate": "0.9100",
                "effective_from": eff1.isoformat(),
            },
        )
        r2 = await c.put(
            "/api/config/fx-rates",
            json={
                "base": "USD",
                "quote": "EUR",
                "rate": "0.9200",
                "effective_from": eff2.isoformat(),
            },
        )
        bad = await c.put(
            "/api/config/fx-rates",
            json={"base": "USD", "quote": "EUR", "rate": "0", "effective_from": eff2.isoformat()},
        )
        listing = await c.get("/api/config/fx-rates")
    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    assert bad.status_code == 422
    assert listing.status_code == 200
    async with db_session_factory() as s:
        rows = list((await s.execute(select(FxRate).order_by(FxRate.effective_from))).scalars())
    assert [r.rate for r in rows] == [Decimal("0.9100"), Decimal("0.9200")]
    assert rows[0].effective_until == rows[1].effective_from
    assert rows[1].effective_until is None
