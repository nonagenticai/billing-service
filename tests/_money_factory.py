"""Shared seeding + client helpers for the EUR money-budget suites (alembic 0044).

Contract under test: tenant-inference-provisioning PLAN rev E, PART 3 §3.1/§3.2.
Kept in one place so the four money suites seed pricing, FX and markup the same
way and assert against the same hand-computed figures.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import FxRate, MarkupRule, MoneyBudget, ProviderPricing

#: Far enough in the past that every effective-dated row below is live "now".
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)

#: pgx-local COST rate (setup 1): $0.10 / 1M in, $0.40 / 1M out.
PGX_IN = Decimal("0.10")
PGX_OUT = Decimal("0.40")
#: deepseek LIST rate (setups 2/3): $0.50 / 1M in, $2.00 / 1M out.
DS_IN = Decimal("0.50")
DS_OUT = Decimal("2.00")
#: deepseek COST rate — deliberately different from list, so a test that
#: priced setup 2 off the cost row would get a visibly different number.
DS_COST_IN = Decimal("0.05")
DS_COST_OUT = Decimal("0.20")
#: USD -> EUR.
FX = Decimal("0.9200")
#: USD -> CAD.
FX_CAD = Decimal("1.3700")


def usd(inp: int, out: int, rate_in: Decimal, rate_out: Decimal) -> Decimal:
    return (Decimal(inp) * rate_in + Decimal(out) * rate_out) / Decimal(1_000_000)


def q8(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"))


def pricing_rows() -> list[ProviderPricing]:
    def _row(provider, model, rate_class, i, o):
        return ProviderPricing(
            provider=provider,
            model=model,
            rate_class=rate_class,
            cost_model="per_token",
            input_per_1m_usd=i,
            output_per_1m_usd=o,
            effective_from=EPOCH,
        )

    return [
        _row("pgx-local", "local-llm", "cost", PGX_IN, PGX_OUT),
        _row("deepseek", "deepseek-v4-pro", "list", DS_IN, DS_OUT),
        _row("deepseek", "deepseek-v4-pro", "cost", DS_COST_IN, DS_COST_OUT),
    ]


def fx_row(
    rate: Decimal = FX, *, effective_from: datetime = EPOCH, until=None, quote: str = "EUR"
) -> FxRate:
    return FxRate(
        base="USD",
        quote=quote,
        rate=rate,
        effective_from=effective_from,
        effective_until=until,
        set_by="test",
    )


def markup_row(tenant_id: str, *, provider: str, model: str, byo: bool, pct: str) -> MarkupRule:
    return MarkupRule(
        tenant_id=tenant_id,
        provider=provider,
        model=model,
        byo=byo,
        markup_pct=Decimal(pct),
        effective_from=EPOCH,
    )


def money_budget(
    tenant_id: str,
    *,
    amount: int,
    consumed: str = "0",
    period_start: date | None = None,
    currency: str = "EUR",
) -> MoneyBudget:
    now = datetime.now(UTC)
    return MoneyBudget(
        tenant_id=tenant_id,
        currency=currency,
        amount=amount,
        period="monthly",
        consumed=Decimal(consumed),
        consumed_period_start=period_start or now.date().replace(day=1),
        active=True,
        source="provisioning",
    )


def last_month_start() -> date:
    first = datetime.now(UTC).date().replace(day=1)
    return (first - timedelta(days=1)).replace(day=1)


async def seed(session: AsyncSession, *objs) -> None:
    session.add_all(list(objs))
    await session.commit()


@asynccontextmanager
async def money_client(db_engine, monkeypatch, redis_conn) -> AsyncGenerator[AsyncClient]:
    """ASGI client on the real app, in the TEST's event loop, with the flag ON.

    Same loop as the test, so the fakeredis instance the test inspects is the
    very one the route wrote to.
    """
    import routers.money_budget as money_router
    from db import get_session
    from main import app
    from settings import get_settings

    monkeypatch.setenv("BILLING_MONEY_BUDGETS_ENABLED", "true")
    get_settings.cache_clear()
    monkeypatch.setattr(money_router, "get_redis", lambda: redis_conn)
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)

    async def _session():
        async with factory() as s:
            yield s

    app.dependency_overrides[get_session] = _session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            yield c
    finally:
        app.dependency_overrides.clear()
        get_settings.cache_clear()
