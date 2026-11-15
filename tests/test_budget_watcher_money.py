"""The overage watcher evaluates EUR money budgets too (PLAN rev E §2.1, 50/80/100%).

Payload carries ``{tenant_id, currency, amount, consumed, pct}`` and is deduped on
the same ``budget_overage_alerts`` per-destination key as the token path.
"""

from __future__ import annotations

import json
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import select

from models.db import BudgetOverageAlert, MoneyBudget
from services.budget_watcher import _PlatformWebhook, check_and_fire_overages
from tests._money_factory import last_month_start, money_budget
from tests.conftest import T


def _recording_client(sink: list[dict]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        sink.append(json.loads(request.content))
        return httpx.Response(200)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def _money_alerts(factory, budget_id) -> list[int]:
    async with factory() as s:
        rows = (
            await s.execute(
                select(BudgetOverageAlert.threshold_pct).where(
                    BudgetOverageAlert.budget_id == budget_id
                )
            )
        ).scalars()
        return sorted(rows)


@pytest.mark.asyncio
async def test_money_budget_crossings_fire_with_money_payload(db_session_factory):
    t = T("watch-money")
    async with db_session_factory() as s:
        b = money_budget(t, amount=100, consumed="85.5")
        s.add(b)
        await s.commit()
        bid = b.id

    sent: list[dict] = []
    async with _recording_client(sent) as client:
        fired = await check_and_fire_overages(
            db_session_factory,
            http_client=client,
            platform_webhook=_PlatformWebhook(url="https://sink.example/h", secret="s3cr3t"),
        )
        again = await check_and_fire_overages(
            db_session_factory,
            http_client=client,
            platform_webhook=_PlatformWebhook(url="https://sink.example/h", secret="s3cr3t"),
        )

    assert fired == 2
    assert again == 0, "dedupe: one crossing fires once per period per destination"
    assert await _money_alerts(db_session_factory, bid) == [50, 80]
    money = [p for p in sent if p.get("currency") == "EUR"]
    assert {p["threshold_pct"] for p in money} == {50, 80}
    p = money[0]
    assert p["tenant_id"] == t
    assert p["amount"] == 100
    assert Decimal(p["consumed"]) == Decimal("85.5")
    assert p["pct"] == pytest.approx(85.5)


@pytest.mark.asyncio
async def test_money_budget_below_50_and_rolled_over_do_not_fire(db_session_factory):
    low = T("watch-money-low")
    rolled = T("watch-money-rolled")
    async with db_session_factory() as s:
        s.add(money_budget(low, amount=100, consumed="10"))
        s.add(money_budget(rolled, amount=100, consumed="99", period_start=last_month_start()))
        await s.commit()

    fired = await check_and_fire_overages(db_session_factory)
    assert fired == 0


@pytest.mark.asyncio
async def test_zero_amount_money_budget_is_skipped_not_divided(db_session_factory):
    t = T("watch-money-zero")
    async with db_session_factory() as s:
        s.add(money_budget(t, amount=0, consumed="0"))
        await s.commit()
    assert await check_and_fire_overages(db_session_factory) == 0
    async with db_session_factory() as s:
        assert (await s.execute(select(MoneyBudget))).scalars().first() is not None
