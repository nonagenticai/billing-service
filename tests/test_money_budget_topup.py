"""POST /api/budget/money/topup + GET /api/budget/money/credit (alembic 0055) + rate card.

Contract (CommonPractice Part B; owner ruling 2026-10-13: a top-up is PREPAID CREDIT):

    POST /api/budget/money/topup
         {"tenant_id","amount":50,"currency":"EUR","idempotency_key":"..."}
      -> 200 {topup_id, replayed, amount, currency, credit_usd,
              invoice_line:{access_charge_id, service_key:"topup:<id>", amount_usd, ...},
              credit:{currency:"USD", granted_usd, drawn_usd, remaining_usd, settled_through}}
       | 409 idempotency_key_reused | 422 fx_rate_missing | 401 / 403 not a writer
    GET  /api/budget/money/credit/{tenant_id} -> the `credit` body
    GET  /api/budget/money/status/{tenant_id} -> the cap + `credit`
    GET  /api/config/rate-card -> {"data":{platform_fee_usd, seat_fee_usd, markup_pct,
                                           byo_markup_pct, currency:"USD", ...}}

The invoicing half (netting, carry-over, freeze-once) is
tests/test_prepaid_credit_invoicing.py. Every test here fails against the code
before this change: none of these routes, nor the ledger, existed.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

import pytest
from sqlalchemy import select

import auth
import auth_deps
from models.db import CreditLedgerEntry, MoneyBudget, TenantAccessCharge
from services.billing import generate_billing
from tests._money_factory import FX, fx_row, money_budget, money_client, seed
from tests.conftest import T

pytestmark = pytest.mark.asyncio


def _body(tenant: str, *, amount: int = 50, currency: str = "EUR", key: str = "cp-topup-0001"):
    return {"tenant_id": tenant, "amount": amount, "currency": currency, "idempotency_key": key}


def _usd(amount: int, rate: Decimal = FX) -> Decimal:
    return (Decimal(amount) / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _as_caller(monkeypatch, payload: dict | None) -> None:
    """Turn real auth ON and make the bearer decode to ``payload`` (None -> 401)."""
    from fastapi import HTTPException

    async def _decode(_request):
        if payload is None:
            raise HTTPException(status_code=401, detail="Missing bearer token")
        return payload

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _decode)


CP_SA = {
    "sub": "11111111-2222-3333-4444-555555555555",
    "azp": "commonpractice-licenses-admin",
    "preferred_username": "service-account-commonpractice-licenses-admin",
    "realm_access": {"roles": ["default-roles-secure-apps"]},
}


async def _ledger(db_session_factory, tenant: str) -> list[CreditLedgerEntry]:
    async with db_session_factory() as s:
        rows = await s.execute(
            select(CreditLedgerEntry).where(CreditLedgerEntry.tenant_id == tenant)
        )
        return list(rows.scalars().all())


async def _topup_charges(db_session_factory, tenant: str) -> list[TenantAccessCharge]:
    async with db_session_factory() as s:
        rows = await s.execute(
            select(TenantAccessCharge).where(
                TenantAccessCharge.tenant_id == tenant,
                TenantAccessCharge.service_key.like("topup:%"),
            )
        )
        return list(rows.scalars().all())


async def _cap(db_session_factory, tenant: str) -> int:
    async with db_session_factory() as s:
        return int(
            (
                await s.execute(select(MoneyBudget.amount).where(MoneyBudget.tenant_id == tenant))
            ).scalar_one()
        )


# --- credit, not cap ------------------------------------------------------------------


async def test_topup_grants_credit_and_leaves_the_cap_alone(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    t = T("topup-credit")
    await seed(db_session, money_budget(t, amount=100, consumed="30"), fx_row())
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        a = await c.post("/api/budget/money/topup", json=_body(t, amount=50, key="cp-key-aaaa"))
        b = await c.post("/api/budget/money/topup", json=_body(t, amount=25, key="cp-key-bbbb"))
        status = await c.get(f"/api/budget/money/status/{t}")
        credit = await c.get(f"/api/budget/money/credit/{t}")

    assert a.status_code == 200, a.text
    assert b.status_code == 200, b.text
    assert a.json()["credit_usd"] == str(_usd(50))
    granted = _usd(50) + _usd(25)
    assert b.json()["credit"]["remaining_usd"] == str(granted)
    assert credit.json() == b.json()["credit"] == status.json()["credit"]
    assert credit.json()["drawn_usd"] == "0.00" and credit.json()["settled_through"] is None
    # ⛔ The cap is a separate concept: a top-up never mutates it.
    assert status.json()["amount"] == 100
    assert await _cap(db_session_factory, t) == 100
    kinds = sorted(e.kind for e in await _ledger(db_session_factory, t))
    assert kinds == ["topup", "topup"]


async def test_topup_needs_no_money_budget(db_engine, db_session_factory, fake_redis, monkeypatch):
    t = T("topup-no-budget")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.post("/api/budget/money/topup", json=_body(t, currency="USD", amount=40))
        credit = await c.get(f"/api/budget/money/credit/{t}")
    assert r.status_code == 200, r.text
    assert r.json()["credit_usd"] == "40.00"
    assert credit.json()["remaining_usd"] == "40.00"


# --- idempotent ---------------------------------------------------------------------------


async def test_topup_replay_with_same_key_writes_nothing(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    t = T("topup-idem")
    await seed(db_session, fx_row())
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        first = await c.post("/api/budget/money/topup", json=_body(t, amount=40))
        again = await c.post("/api/budget/money/topup", json=_body(t, amount=40))
        reused = await c.post("/api/budget/money/topup", json=_body(t, amount=41))

    assert first.status_code == 200 and again.status_code == 200, (first.text, again.text)
    f, g = first.json(), again.json()
    assert g["replayed"] is True and f["replayed"] is False
    assert g["topup_id"] == f["topup_id"]
    assert g["invoice_line"]["access_charge_id"] == f["invoice_line"]["access_charge_id"]
    assert g["credit"]["remaining_usd"] == str(_usd(40))

    assert reused.status_code == 409, reused.text
    assert reused.json()["detail"]["reason"] == "idempotency_key_reused"
    assert len(await _ledger(db_session_factory, t)) == 1
    assert len(await _topup_charges(db_session_factory, t)) == 1


# --- invoiced: exactly one line -----------------------------------------------------------


async def test_topup_creates_exactly_one_invoice_line(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch, test_settings
):
    t = T("topup-invoice")
    await seed(db_session, fx_row())
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.post("/api/budget/money/topup", json=_body(t, amount=50))
        await c.post("/api/budget/money/topup", json=_body(t, amount=50))  # replay
    assert r.status_code == 200, r.text
    line = r.json()["invoice_line"]
    assert line["amount_usd"] == str(_usd(50))
    assert line["service_key"] == f"topup:{r.json()['topup_id']}"

    month = datetime.now(UTC).strftime("%Y-%m")
    async with db_session_factory() as s:
        result = await generate_billing(t, month, s, test_settings)
    topup_lines = [
        c
        for c in result["totals"]["access_charges"]
        if (c.get("service_key") or "").startswith("topup:")
    ]
    assert len(topup_lines) == 1, result["totals"]["access_charges"]
    assert topup_lines[0]["cadence"] == "one_time"
    assert Decimal(topup_lines[0]["amount_usd"]) == _usd(50)
    assert Decimal(result["totals"]["access_charge_usd"]) == _usd(50)


async def test_the_same_key_on_another_tenant_is_a_separate_topup(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    """Idempotency is PER TENANT: tenant B reusing tenant A's key buys B its own credit.

    Pins the ``tenant_id ==`` in ``services.prepaid_credit._entry``: without it B's
    request finds A's entry and is answered as A's replay (B never gets credit).
    """
    a, b = T("topup-scope-a"), T("topup-scope-b")
    shared = "cp-shared-key-0001"
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        ra = await c.post("/api/budget/money/topup", json=_body(a, currency="USD", key=shared))
        rb = await c.post("/api/budget/money/topup", json=_body(b, currency="USD", key=shared))
    assert ra.status_code == 200 and rb.status_code == 200, (ra.text, rb.text)
    assert rb.json()["replayed"] is False
    assert rb.json()["topup_id"] != ra.json()["topup_id"]
    assert rb.json()["tenant_id"] == b
    assert rb.json()["credit"]["remaining_usd"] == "50.00"
    assert [e.tenant_id for e in await _ledger(db_session_factory, b)] == [b]
    assert len(await _ledger(db_session_factory, a)) == 1


async def test_an_unknown_tenant_is_refused(db_engine, db_session_factory, fake_redis, monkeypatch):
    stranger = str(uuid.uuid4())  # not registered via T(): absent from the registry
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.post("/api/budget/money/topup", json=_body(stranger, currency="USD"))
    assert r.status_code == 404, r.text
    assert r.json()["detail"]["reason"] == "tenant_unregistered"
    assert await _ledger(db_session_factory, stranger) == []


async def test_a_registry_outage_is_503_not_unknown(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    """An unreachable registry must not read as "tenant unknown" (or as a 500)."""
    import routers.money_budget as money_router
    from services.tenant_names import TenantRegistryUnavailableError

    t = T("topup-registry-down")

    async def _down(*_a, **_kw):
        raise TenantRegistryUnavailableError("licences registry unreachable: test")

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        monkeypatch.setattr(money_router, "get_tenant_info_strict", _down)
        r = await c.post("/api/budget/money/topup", json=_body(t, currency="USD"))
    assert r.status_code == 503, r.text
    assert r.json()["detail"]["reason"] == "tenant_registry_unavailable"
    assert await _ledger(db_session_factory, t) == []


async def test_an_archived_tenant_is_refused(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    import routers.money_budget as money_router
    from services.tenant_names import TenantInfo

    t = T("topup-archived")

    async def _registry(*_a, **_kw):
        return {t: TenantInfo(name="gone", kind="customer", archived=True)}

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        monkeypatch.setattr(money_router, "get_tenant_info_strict", _registry)
        r = await c.post("/api/budget/money/topup", json=_body(t, currency="USD"))
    assert r.status_code == 409, r.text
    assert r.json()["detail"]["reason"] == "tenant_archived"
    assert await _ledger(db_session_factory, t) == []


# --- refusals that write nothing --------------------------------------------------------------


async def test_topup_refusals_write_nothing(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    cad = T("topup-cad-nofx")
    await seed(db_session, fx_row())  # EUR only: no USD->CAD rate
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        no_fx = await c.post("/api/budget/money/topup", json=_body(cad, currency="CAD"))
        frac = await c.post("/api/budget/money/topup", json={**_body(cad), "amount": 10.5})
        zero = await c.post("/api/budget/money/topup", json={**_body(cad), "amount": 0})
    assert no_fx.status_code == 422 and no_fx.json() == {"reason": "fx_rate_missing"}
    assert frac.status_code == 422
    assert zero.status_code == 422
    assert await _ledger(db_session_factory, cad) == []
    assert await _topup_charges(db_session_factory, cad) == []


async def test_topup_and_credit_404_while_the_money_flag_is_off(
    db_engine, db_session_factory, fake_redis, monkeypatch
):
    t = T("topup-flag-off")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        from settings import get_settings

        monkeypatch.setenv("BILLING_MONEY_BUDGETS_ENABLED", "false")
        get_settings.cache_clear()
        r = await c.post("/api/budget/money/topup", json=_body(t, currency="USD"))
        g = await c.get(f"/api/budget/money/credit/{t}")
    assert (r.status_code, g.status_code) == (404, 404)
    assert await _ledger(db_session_factory, t) == []


async def test_credit_read_is_tenant_isolated(db_engine, fake_redis, monkeypatch):
    mine, theirs = T("credit-mine"), T("credit-theirs")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.get(f"/api/budget/money/credit/{theirs}", headers={"X-Tenant-Id": mine})
    assert r.status_code == 403


# --- authorisation ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (None, 401),  # no bearer (e.g. a licence-token caller with only X-Tenant-Id)
        ({"sub": "x", "realm_access": {"roles": ["billing-internal"]}}, 403),
        ({"sub": "x", "realm_access": {"roles": ["onboarding-internal"]}}, 403),
        ({"sub": "x", "roles": ["license-admin"], "realm_access": {"roles": []}}, 403),
        ({**CP_SA, "preferred_username": "someone"}, 403),  # azp alone is not enough
        ({**CP_SA, "azp": "fr-backend", "preferred_username": "service-account-fr-backend"}, 403),
    ],
)
async def test_unauthorised_callers_are_refused(
    db_engine, db_session_factory, fake_redis, monkeypatch, payload, expected
):
    t = T("topup-authz")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        _as_caller(monkeypatch, payload)
        r = await c.post(
            "/api/budget/money/topup", json=_body(t, currency="USD"), headers={"X-Tenant-Id": t}
        )
    assert r.status_code == expected, r.text
    assert await _ledger(db_session_factory, t) == []


@pytest.mark.parametrize(
    "payload",
    [CP_SA, {"sub": "op", "realm_access": {"roles": ["license-admin"]}}],
)
async def test_cp_service_account_and_license_admin_may_top_up(
    db_engine, db_session_factory, fake_redis, monkeypatch, payload
):
    t = T("topup-authz-ok")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        _as_caller(monkeypatch, payload)
        r = await c.post(
            "/api/budget/money/topup",
            json=_body(t, amount=5, currency="USD", key=f"cp-authz-{payload['sub']}"),
        )
    assert r.status_code == 200, r.text
    topup_id = uuid.UUID(r.json()["topup_id"])
    rows = [e for e in await _ledger(db_session_factory, t) if e.id == topup_id]
    expected_role = (
        "topup-writer:commonpractice-licenses-admin" if payload is CP_SA else "license-admin"
    )
    assert rows[0].actor_role == expected_role


# --- rate card -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (None, 401),
        ({"sub": "x", "realm_access": {"roles": ["onboarding-internal"]}}, 403),
        ({"sub": "x", "realm_access": {"roles": ["pricing-writer"]}}, 403),
        ({"sub": "x", "realm_access": {"roles": ["billing-internal"]}}, 200),
        ({"sub": "x", "realm_access": {"roles": ["license-admin"]}}, 200),
    ],
)
async def test_rate_card_is_guarded_like_the_terms_reads(
    db_engine, fake_redis, monkeypatch, payload, expected
):
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        _as_caller(monkeypatch, payload)
        r = await c.get("/api/config/rate-card")
    assert r.status_code == expected, r.text


async def test_rate_card_is_the_standard_terms_default(db_engine, fake_redis, monkeypatch):
    from settings import get_settings

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.get("/api/config/rate-card")
        s = get_settings()
    assert r.status_code == 200, r.text
    card = r.json()["data"]
    assert card["currency"] == "USD"
    assert card["source"] == "settings_default"
    assert Decimal(card["platform_fee_usd"]) == Decimal(str(s.default_platform_fee_usd))
    assert Decimal(card["seat_fee_usd"]) == Decimal(str(s.default_seat_fee_usd))
    assert Decimal(card["markup_pct"]) == Decimal(str(s.default_markup_pct))
    assert Decimal(card["byo_markup_pct"]) == Decimal(str(s.default_byo_markup_pct))
