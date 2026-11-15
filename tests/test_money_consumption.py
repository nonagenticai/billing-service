"""Settlement of an IR cost event against an EUR money budget (PLAN rev E §3.1/§3.2).

    EUR = fx(USD->EUR @ event_time) x base
      setup 1: base = cost price (provider_pricing rate_class='cost')
      setup 2: base = list x (1 + pct/100)
      setup 3: base = list x pct/100
    pct = resolved MarkupRule for tenant x provider x model x byo

Settlement = the cost event carrying ``reservation_id``: billing writes the
consumption columns, debits the pooled ``tenant_money_budget`` and releases the
handle. An event WITHOUT ``usage_extra.setup`` takes the legacy token path,
unchanged.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from models.db import CostEvent, MoneyBudget, TenantConfig, TokenBudget
from services import money_budget as mb_svc
from services.cost_flusher import flush_costs
from tests._aggregator_helpers import _make_cost
from tests._budget_factory import make_budget
from tests._money_factory import (
    DS_IN,
    DS_OUT,
    FX,
    PGX_IN,
    PGX_OUT,
    fx_row,
    markup_row,
    money_budget,
    pricing_rows,
    q8,
    seed,
    usd,
)
from tests.conftest import T

IN, OUT = 1000, 500


def _extra(setup: int, *, byo: int = 0, identity: str = "pgx-local", hop: int = 0) -> str:
    return json.dumps(
        {
            "setup": setup,
            "key_owner": {1: "platform", 2: "owner", 3: "tenant"}[setup],
            "byo": byo,
            "policy_version": 1,
            "hop_index": hop,
            "hop_reason": "first",
            "chain_id": "0190a5f0-0000-7000-8000-000000000000",
            "provider_identity": identity,
        }
    )


def _event(tenant: str, *, provider: str, model: str, **extra) -> dict:
    return _make_cost(
        tenant_id=tenant,
        license_id="",
        provider=provider,
        model=model,
        served_model=model,
        input_tokens=str(IN),
        output_tokens=str(OUT),
        cost_usd="0.001",
        **extra,
    )


def _money_tenant(t: str, *, amount: int = 100, consumed: str = "0"):
    return [
        TenantConfig(tenant_id=t, markup_pct=Decimal("30"), budget_unit="money"),
        money_budget(t, amount=amount, consumed=consumed),
    ]


async def _rows(factory, tenant: str) -> list[CostEvent]:
    async with factory() as s:
        return list(
            (await s.execute(select(CostEvent).where(CostEvent.tenant_id == tenant))).scalars()
        )


async def _budget(factory, tenant: str) -> MoneyBudget:
    async with factory() as s:
        return (
            await s.execute(select(MoneyBudget).where(MoneyBudget.tenant_id == tenant))
        ).scalar_one()


def test_consumption_usd_math():
    cost = Decimal("1.00")
    assert mb_svc.consumption_usd(1, cost, None) == Decimal("1.00")
    assert mb_svc.consumption_usd(2, cost, Decimal("25")) == Decimal("1.25")
    assert mb_svc.consumption_usd(3, cost, Decimal("10")) == Decimal("0.10")
    assert mb_svc.consumption_usd(3, cost, Decimal("0")) == Decimal("0")


@pytest.mark.asyncio
async def test_setup1_cost_price_times_fx(db_session, db_session_factory, fake_redis):
    t = T("cons-s1")
    await seed(db_session, *pricing_rows(), fx_row(), *_money_tenant(t, consumed="1.5"))

    await flush_costs(
        [_event(t, provider="pgx-local", model="local-llm", usage_extra=_extra(1))],
        db_session_factory,
        fake_redis,
    )

    expected = q8(usd(IN, OUT, PGX_IN, PGX_OUT) * FX)
    [row] = await _rows(db_session_factory, t)
    assert row.llm_setup == 1
    assert row.consumption_currency == "EUR"
    assert row.consumption_amount == expected
    assert (await _budget(db_session_factory, t)).consumed == Decimal("1.5") + expected


@pytest.mark.asyncio
async def test_setup2_and_setup3_use_list_and_the_byo_markup_axis(
    db_session, db_session_factory, fake_redis
):
    t = T("cons-s23")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        *_money_tenant(t),
        # Same tenant x provider x model, split ONLY on byo: setup 2 must take
        # the byo=False rule (25%), setup 3 the byo=True rule (10%).
        markup_row(t, provider="deepseek", model="deepseek-v4-pro", byo=False, pct="25.00"),
        markup_row(t, provider="deepseek", model="deepseek-v4-pro", byo=True, pct="10.00"),
    )

    await flush_costs(
        [
            _event(
                t,
                provider="deepseek",
                model="deepseek-v4-pro",
                usage_extra=_extra(2, identity="owner:deepseek:abcd1234"),
            ),
            _event(
                t,
                provider="deepseek",
                model="deepseek-v4-pro",
                usage_extra=_extra(3, byo=1, identity="byo:cred"),
            ),
        ],
        db_session_factory,
        fake_redis,
    )

    lst = usd(IN, OUT, DS_IN, DS_OUT)
    s2 = q8(lst * Decimal("1.25") * FX)
    s3 = q8(lst * Decimal("0.10") * FX)
    rows = {r.llm_setup: r for r in await _rows(db_session_factory, t)}
    assert rows[2].consumption_amount == s2
    assert rows[3].consumption_amount == s3
    assert (await _budget(db_session_factory, t)).consumed == s2 + s3


@pytest.mark.asyncio
async def test_setup3_zero_markup_consumes_zero(db_session, db_session_factory, fake_redis):
    t = T("cons-s3-zero")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        *_money_tenant(t),
        markup_row(t, provider="deepseek", model="deepseek-v4-pro", byo=True, pct="0"),
    )
    await flush_costs(
        [_event(t, provider="deepseek", model="deepseek-v4-pro", usage_extra=_extra(3, byo=1))],
        db_session_factory,
        fake_redis,
    )
    [row] = await _rows(db_session_factory, t)
    assert row.consumption_amount == Decimal("0")
    assert (await _budget(db_session_factory, t)).consumed == Decimal("0")


@pytest.mark.asyncio
async def test_fx_is_taken_at_event_time(db_session, db_session_factory, fake_redis):
    t = T("cons-fx-time")
    now = datetime.now(UTC)
    switch = now - timedelta(days=1)
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(Decimal("0.8000"), until=switch),
        fx_row(Decimal("0.9200"), effective_from=switch),
        *_money_tenant(t),
    )
    event_time = now - timedelta(days=2)

    await flush_costs(
        [
            _event(
                t,
                provider="pgx-local",
                model="local-llm",
                usage_extra=_extra(1),
                timestamp=event_time.isoformat(),
            )
        ],
        db_session_factory,
        fake_redis,
    )
    [row] = await _rows(db_session_factory, t)
    assert row.consumption_amount == q8(usd(IN, OUT, PGX_IN, PGX_OUT) * Decimal("0.8000"))


def _unpriced(reason: str) -> float:
    from prometheus_client import REGISTRY

    return (
        REGISTRY.get_sample_value("billing_money_unpriced_events_total", {"reason": reason}) or 0.0
    )


@pytest.mark.asyncio
async def test_unpriceable_settlement_is_counted(db_session, db_session_factory, fake_redis):
    """No price row at settlement: row stored with NULL consumption AND counted."""
    t = T("cons-unpriced")
    await seed(db_session, fx_row(), *_money_tenant(t))
    before = _unpriced("price_missing")

    await flush_costs(
        [_event(t, provider="pgx-local", model="local-llm", usage_extra=_extra(1))],
        db_session_factory,
        fake_redis,
    )

    [row] = await _rows(db_session_factory, t)
    assert row.consumption_amount is None
    assert _unpriced("price_missing") == before + 1
    assert (await _budget(db_session_factory, t)).consumed == Decimal("0")


@pytest.mark.asyncio
async def test_event_older_than_the_first_fx_row_is_counted_by_name(
    db_session, db_session_factory, fake_redis
):
    """The first FX row was not back-dated: the event predates it -> named reason."""
    t = T("cons-before-fx")
    now = datetime.now(UTC)
    await seed(db_session, *pricing_rows(), fx_row(effective_from=now - timedelta(days=1)))
    await seed(db_session, *_money_tenant(t))
    before_first = _unpriced("before_first_fx_rate")
    before_gap = _unpriced("fx_rate_missing")

    await flush_costs(
        [
            _event(
                t,
                provider="pgx-local",
                model="local-llm",
                usage_extra=_extra(1),
                timestamp=(now - timedelta(days=3)).isoformat(),
            )
        ],
        db_session_factory,
        fake_redis,
    )

    [row] = await _rows(db_session_factory, t)
    assert row.consumption_amount is None
    assert _unpriced("before_first_fx_rate") == before_first + 1
    assert _unpriced("fx_rate_missing") == before_gap


@pytest.mark.asyncio
async def test_settlement_releases_the_reservation_by_handle(
    db_session, db_session_factory, fake_redis
):
    t = T("cons-release")
    await seed(db_session, *pricing_rows(), fx_row(), *_money_tenant(t))

    async with db_session_factory() as s:
        held = await mb_svc.reserve_money(
            s,
            fake_redis,
            tenant_id=t,
            setup=1,
            provider="pgx-local",
            model="local-llm",
            byo=False,
            input_tokens=IN,
            output_tokens=OUT,
            ttl_seconds=60,
        )
        other = await mb_svc.reserve_money(
            s,
            fake_redis,
            tenant_id=t,
            setup=1,
            provider="pgx-local",
            model="local-llm",
            byo=False,
            input_tokens=IN,
            output_tokens=OUT,
            ttl_seconds=60,
        )
    assert held["allowed"] and other["allowed"]
    two = await mb_svc.in_flight_amount(fake_redis, t)

    await flush_costs(
        [
            _event(
                t,
                provider="pgx-local",
                model="local-llm",
                usage_extra=_extra(1),
                reservation_id=held["reservation_id"],
            )
        ],
        db_session_factory,
        fake_redis,
    )

    # Only THIS handle's hold is discharged; the other caller's stays in flight.
    assert await mb_svc.in_flight_amount(fake_redis, t) == two / 2
    [row] = await _rows(db_session_factory, t)
    assert str(row.reservation_id) == held["reservation_id"]


@pytest.mark.asyncio
async def test_money_tenant_event_does_not_debit_token_budgets(
    db_session, db_session_factory, fake_redis
):
    t = T("cons-no-token-debit")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        *_money_tenant(t),
        make_budget(tenant=t, cap=1_000_000, consumed=0),
    )
    await flush_costs(
        [_event(t, provider="pgx-local", model="local-llm", usage_extra=_extra(1))],
        db_session_factory,
        fake_redis,
    )
    async with db_session_factory() as s:
        tb = (await s.execute(select(TokenBudget).where(TokenBudget.tenant_id == t))).scalar_one()
    assert tb.consumed_tokens == 0


@pytest.mark.asyncio
async def test_event_without_setup_takes_the_legacy_token_path(
    db_session, db_session_factory, fake_redis
):
    t = T("cons-legacy")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        *_money_tenant(t),
        make_budget(tenant=t, cap=1_000_000, consumed=0),
    )
    await flush_costs(
        [_event(t, provider="pgx-local", model="local-llm")],
        db_session_factory,
        fake_redis,
    )
    [row] = await _rows(db_session_factory, t)
    assert row.llm_setup is None
    assert row.consumption_amount is None
    assert row.consumption_currency is None
    assert (await _budget(db_session_factory, t)).consumed == Decimal("0")
    async with db_session_factory() as s:
        tb = (await s.execute(select(TokenBudget).where(TokenBudget.tenant_id == t))).scalar_one()
    assert tb.consumed_tokens == IN + OUT


@pytest.mark.asyncio
async def test_token_tenant_with_setup_keeps_token_path(db_session, db_session_factory, fake_redis):
    """budget_unit='tokens' (the default): a setup-tagged event still debits tokens."""
    t = T("cons-token-tenant")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        make_budget(tenant=t, cap=1_000_000, consumed=0),
    )
    await flush_costs(
        [_event(t, provider="pgx-local", model="local-llm", usage_extra=_extra(1))],
        db_session_factory,
        fake_redis,
    )
    [row] = await _rows(db_session_factory, t)
    assert row.llm_setup == 1
    assert row.consumption_amount is None
    async with db_session_factory() as s:
        tb = (await s.execute(select(TokenBudget).where(TokenBudget.tenant_id == t))).scalar_one()
    assert tb.consumed_tokens == IN + OUT
