"""POST /api/budget/money/reserve — the EUR pre-flight gate (PLAN rev E §3.1).

Wire contract (IR -> billing), pinned here field by field:

    200 {"data":{"allowed":true,"reservation_id":"<uuid>","currency":"EUR",
         "reserved_amount":"…","cap_amount":"100","consumed_amount":"…",
         "in_flight_amount":"…","remaining_amount":"…"}}
    429 (not enveloped) {"detail":{"allowed":false,"reservation_id":null,
         "currency":"EUR","reason":"over_budget|no_active_budget|tenant_archived|
         fx_rate_missing|price_missing", ...same amounts}}

⛔ ``no_active_budget`` is a HARD STOP here, unlike the token path (which ALLOWS a
tenant with no budget row). A money tenant without a money budget must not spend.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from services import money_budget as money_svc
from services import tenant_names
from services.tenant_names import TenantInfo
from tests._money_factory import (
    DS_IN,
    DS_OUT,
    FX,
    PGX_IN,
    PGX_OUT,
    fx_row,
    last_month_start,
    markup_row,
    money_budget,
    money_client,
    pricing_rows,
    q8,
    seed,
    usd,
)
from tests.conftest import T

PATH = "/api/budget/money/reserve"
AMOUNT_KEYS = (
    "reserved_amount",
    "cap_amount",
    "consumed_amount",
    "in_flight_amount",
    "remaining_amount",
)


def _body(**over) -> dict:
    body = {
        "setup": 1,
        "provider": "pgx-local",
        "model": "local-llm",
        "byo": False,
        "estimated_input_tokens": 1200,
        "max_output_tokens": 512,
        "hop_index": 0,
        "inference_id": str(uuid.uuid4()),
        "ttl_seconds": 60,
    }
    body.update(over)
    return body


@pytest.fixture(autouse=True)
def _clean_registry_cache():
    tenant_names.reset_cache()
    yield
    tenant_names.reset_cache()


async def _post(client, tenant: str, body: dict):
    return await client.post(PATH, headers={"X-Tenant-Id": tenant}, json=body)


@pytest.mark.asyncio
async def test_allowed_within_cap_returns_the_contract_shape(
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("money-ok")
    await seed(db_session, *pricing_rows(), fx_row(), money_budget(t, amount=100, consumed="5.5"))

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(c, t, _body())

    assert r.status_code == 200, r.text
    d = r.json()["data"]
    expected = q8(usd(1200, 512, PGX_IN, PGX_OUT) * FX)
    assert d["allowed"] is True
    assert d["currency"] == "EUR"
    assert uuid.UUID(d["reservation_id"])
    assert d["cap_amount"] == "100"
    assert Decimal(d["reserved_amount"]) == expected
    assert d["reserved_amount"] == f"{expected:.8f}"
    assert d["consumed_amount"] == "5.50000000"
    assert Decimal(d["in_flight_amount"]) == expected
    assert Decimal(d["remaining_amount"]) == Decimal(100) - Decimal("5.5") - expected
    # Every amount is a string with 8 decimals, except the integer cap.
    for k in AMOUNT_KEYS:
        assert isinstance(d[k], str), k


@pytest.mark.asyncio
async def test_second_reserve_sees_the_first_in_flight(
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("money-inflight")
    await seed(db_session, *pricing_rows(), fx_row(), money_budget(t, amount=100))

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        a = (await _post(c, t, _body())).json()["data"]
        b = (await _post(c, t, _body())).json()["data"]

    assert a["reservation_id"] != b["reservation_id"]
    assert Decimal(b["in_flight_amount"]) == 2 * Decimal(a["reserved_amount"])


@pytest.mark.asyncio
async def test_in_flight_holds_count_against_the_cap(
    db_engine, db_session, fake_redis, monkeypatch
):
    """F2: consumed + IN-FLIGHT + estimate > cap refuses, even though consumed + estimate fits.

    Kills the mutation "the Lua admission ignores in-flight holds": the cap
    leaves room for ONE estimate but not two, and nothing is consumed between
    the calls, so only the first hold (still in flight) can refuse the second.
    """
    t = T("money-inflight-cap")
    est = q8(usd(1200, 512, PGX_IN, PGX_OUT) * FX)
    # 1 EUR cap; consumed leaves headroom for 1.5 estimates.
    consumed = Decimal(1) - est * Decimal("1.5")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        money_budget(t, amount=1, consumed=f"{q8(consumed)}"),
    )

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        first = await _post(c, t, _body())
        second = await _post(c, t, _body())

    assert first.status_code == 200, first.text
    assert Decimal(first.json()["data"]["reserved_amount"]) == est
    # consumed + est <= cap on its own, so ONLY the in-flight hold can refuse this.
    assert q8(consumed) + est <= Decimal(1)
    assert second.status_code == 429, second.text
    d = second.json()["detail"]
    assert d["reason"] == "over_budget"
    assert Decimal(d["in_flight_amount"]) == est


@pytest.mark.asyncio
async def test_retry_with_same_inference_id_returns_the_existing_hold(
    db_engine, db_session, fake_redis, monkeypatch
):
    """A retry (same inference_id + hop) while the hold is live holds nothing more."""
    t = T("money-idem")
    await seed(db_session, *pricing_rows(), fx_row(), money_budget(t, amount=100))
    body = _body(inference_id=str(uuid.uuid4()), hop_index=0)

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        a = (await _post(c, t, body)).json()["data"]
        b = (await _post(c, t, body)).json()["data"]
        # Same inference, NEXT hop: a genuinely new hold, not hop 0's.
        h1 = (await _post(c, t, {**body, "hop_index": 1})).json()["data"]

    assert b["reservation_id"] == a["reservation_id"]
    assert b["reserved_amount"] == a["reserved_amount"]
    assert Decimal(b["in_flight_amount"]) == Decimal(a["reserved_amount"])
    assert await fake_redis.hlen(money_svc.money_amounts_key(t)) == 2
    assert h1["reservation_id"] not in (a["reservation_id"],)
    assert Decimal(h1["in_flight_amount"]) == 2 * Decimal(a["reserved_amount"])


@pytest.mark.asyncio
async def test_retry_after_the_hold_is_released_reserves_afresh(
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("money-idem-rel")
    await seed(db_session, *pricing_rows(), fx_row(), money_budget(t, amount=100))
    body = _body(inference_id=str(uuid.uuid4()))

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        a = (await _post(c, t, body)).json()["data"]
        await money_svc.release_money_reservations(fake_redis, t, [a["reservation_id"]])
        b = (await _post(c, t, body)).json()["data"]

    assert b["reservation_id"] != a["reservation_id"]
    assert Decimal(b["in_flight_amount"]) == Decimal(a["reserved_amount"])


@pytest.mark.asyncio
async def test_over_budget_is_429_not_enveloped(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-over")
    # 10 EUR cap, 9.99999999 already consumed: any non-zero estimate overflows.
    await seed(
        db_session, *pricing_rows(), fx_row(), money_budget(t, amount=10, consumed="9.99999999")
    )

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(c, t, _body())

    assert r.status_code == 429
    body = r.json()
    assert set(body) == {"detail"}, "429 must be the bare {'detail': ...} shape"
    d = body["detail"]
    assert d["allowed"] is False
    assert d["reservation_id"] is None
    assert d["currency"] == "EUR"
    assert d["reason"] == "over_budget"
    assert d["cap_amount"] == "10"
    assert d["consumed_amount"] == "9.99999999"
    assert d["reserved_amount"] == "0.00000000"
    for k in AMOUNT_KEYS:
        assert k in d


@pytest.mark.asyncio
async def test_no_active_budget_is_a_hard_stop(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-none")
    await seed(db_session, *pricing_rows(), fx_row())

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(c, t, _body())

    assert r.status_code == 429
    d = r.json()["detail"]
    assert d["reason"] == "no_active_budget"
    assert d["allowed"] is False
    assert d["cap_amount"] == "0"


@pytest.mark.asyncio
async def test_fx_rate_missing(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-nofx")
    await seed(db_session, *pricing_rows(), money_budget(t, amount=100))

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(c, t, _body())

    assert r.status_code == 429
    assert r.json()["detail"]["reason"] == "fx_rate_missing"


@pytest.mark.asyncio
async def test_price_missing(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-noprice")
    await seed(db_session, fx_row(), money_budget(t, amount=100))

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(c, t, _body())

    assert r.status_code == 429
    assert r.json()["detail"]["reason"] == "price_missing"


@pytest.mark.asyncio
async def test_setup2_prices_off_the_LIST_row_with_markup(  # noqa: N802
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("money-s2")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        money_budget(t, amount=100),
        markup_row(t, provider="deepseek", model="deepseek-v4-pro", byo=False, pct="25.00"),
    )

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(
            c, t, _body(setup=2, provider="deepseek", model="deepseek-v4-pro", byo=False)
        )

    assert r.status_code == 200, r.text
    expected = q8(usd(1200, 512, DS_IN, DS_OUT) * Decimal("1.25") * FX)
    assert Decimal(r.json()["data"]["reserved_amount"]) == expected


@pytest.mark.asyncio
async def test_tenant_archived(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-arch")
    await seed(db_session, *pricing_rows(), fx_row(), money_budget(t, amount=100))
    tenant_names._name_cache[tenant_names._CACHE_KEY] = {
        t.lower(): TenantInfo(name="gone", archived=True)
    }

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(c, t, _body())

    assert r.status_code == 429
    d = r.json()["detail"]
    assert d["reason"] == "tenant_archived"
    # Real numbers, not zeros (same rule as the token path).
    assert d["cap_amount"] == "100"


@pytest.mark.asyncio
async def test_last_months_consumption_does_not_count(
    db_engine, db_session, fake_redis, monkeypatch
):
    """A monthly money budget rolls over: last month's spend is not this month's."""
    t = T("money-rollover")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        money_budget(t, amount=10, consumed="10", period_start=last_month_start()),
    )

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(c, t, _body())

    assert r.status_code == 200, r.text
    assert r.json()["data"]["consumed_amount"] == "0.00000000"


@pytest.mark.asyncio
async def test_flag_off_is_404(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-flag")
    await seed(db_session, *pricing_rows(), fx_row(), money_budget(t, amount=100))

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        from settings import get_settings

        monkeypatch.setenv("BILLING_MONEY_BUDGETS_ENABLED", "false")
        get_settings.cache_clear()
        r = await _post(c, t, _body())

    assert r.status_code == 404


@pytest.mark.asyncio
async def test_bad_setup_is_422(db_engine, db_session, fake_redis, monkeypatch):
    t = T("money-422")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _post(c, t, _body(setup=4))
    assert r.status_code == 422
