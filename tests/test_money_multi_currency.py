"""Multi-currency money budgets: EUR, USD, CAD (PLAN REV F, owner ruling 2026-10-02 late).

Contract under test:

* A tenant's budget currency is one of ``EUR | USD | CAD``. Every money amount
  carries its ISO code (budget row, cost-event consumption, adjustment rows,
  every response body).
* ``fx_rate`` holds rates USD -> X. For a USD budget the rate is exactly 1 and
  NO row is required. A missing row for a non-USD currency FAILS CLOSED
  (``fx_rate_missing``), exactly as for EUR before.
* Consumption is computed in the tenant's budget currency:
  ``amount = fx(USD->budget currency @ event time) x base``.
* ``POST /api/budget/money/{tenant_id}/currency`` (license-admin = owner /
  super-admin only, audited) converts the REMAINING budget at the current
  cross rate, records an adjustment row, keeps the month's consumed-to-date in
  its original currency (status reports both), and converts in-flight Redis
  holds so they stay consistent and still settle by handle.
* ``PUT /api/budget/money/{t}`` never converts: a currency different from the
  active budget's is a 409 (the provisioning SA may not change currency).
* Tenants without a money budget see zero behaviour change.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

import jwt
import jwt.algorithms
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from sqlalchemy import func, select

import auth
import auth_deps
from models.db import (
    AuditLog,
    CostEvent,
    FxRate,
    MoneyBudget,
    MoneyBudgetAdjustment,
    TenantConfig,
    TokenBudget,
)
from services import money_budget as mb_svc
from services import tenant_names
from services.cost_flusher import flush_costs
from settings import Settings, get_settings
from tests._aggregator_helpers import _make_cost
from tests._budget_factory import make_budget
from tests._money_factory import (
    FX,
    FX_CAD,
    PGX_IN,
    PGX_OUT,
    fx_row,
    money_budget,
    money_client,
    pricing_rows,
    q8,
    seed,
    usd,
)
from tests.conftest import T

IN, OUT = 1000, 500
RESERVE = "/api/budget/money/reserve"


@pytest.fixture(autouse=True)
def _clean_registry_cache():
    tenant_names.reset_cache()
    yield
    tenant_names.reset_cache()


def _extra(setup: int = 1) -> str:
    return json.dumps({"setup": setup, "byo": 0, "hop_index": 0, "policy_version": 1})


def _event(tenant: str, **extra) -> dict:
    return _make_cost(
        tenant_id=tenant,
        license_id="",
        provider="pgx-local",
        model="local-llm",
        served_model="local-llm",
        input_tokens=str(IN),
        output_tokens=str(OUT),
        cost_usd="0.001",
        **extra,
    )


def _reserve_body(**over) -> dict:
    body = {
        "setup": 1,
        "provider": "pgx-local",
        "model": "local-llm",
        "byo": False,
        "estimated_input_tokens": IN,
        "max_output_tokens": OUT,
        "hop_index": 0,
        "inference_id": str(uuid.uuid4()),
        "ttl_seconds": 60,
    }
    body.update(over)
    return body


def _money_tenant(t: str, *, currency: str, amount: int = 100, consumed: str = "0"):
    return [
        TenantConfig(tenant_id=t, markup_pct=Decimal("30"), budget_unit="money"),
        money_budget(t, amount=amount, consumed=consumed, currency=currency),
    ]


async def _budget(factory, tenant: str) -> MoneyBudget:
    async with factory() as s:
        return (
            await s.execute(
                select(MoneyBudget).where(
                    MoneyBudget.tenant_id == tenant, MoneyBudget.active.is_(True)
                )
            )
        ).scalar_one()


# --- PUT ---------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("currency", ["USD", "CAD", "EUR"])
async def test_put_creates_a_budget_in_each_supported_currency(
    currency, db_engine, db_session_factory, fake_redis, monkeypatch
):
    t = T(f"mc-put-{currency}")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put(
            f"/api/budget/money/{t}",
            json={"amount": 50, "currency": currency, "source": "provisioning"},
        )
    assert r.status_code == 200, r.text
    assert r.json()["currency"] == currency
    assert (await _budget(db_session_factory, t)).currency == currency


@pytest.mark.asyncio
async def test_put_refuses_an_unsupported_currency(db_engine, fake_redis, monkeypatch):
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put(f"/api/budget/money/{T('mc-gbp')}", json={"amount": 5, "currency": "GBP"})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_put_with_a_different_currency_is_409_and_converts_nothing(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    t = T("mc-put-409")
    await seed(db_session, *_money_tenant(t, currency="EUR", amount=100, consumed="10"))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put(f"/api/budget/money/{t}", json={"amount": 100, "currency": "USD"})
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["reason"] == "currency_change_requires_conversion"
    assert detail["current_currency"] == "EUR"
    b = await _budget(db_session_factory, t)
    assert (b.currency, b.amount, b.consumed) == ("EUR", 100, Decimal("10"))


# --- FX ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fx_put_accepts_usd_to_cad_and_refuses_usd_to_usd(db_engine, fake_redis, monkeypatch):
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        ok = await c.put(
            "/api/config/fx-rates",
            json={
                "base": "USD",
                "quote": "CAD",
                "rate": "1.37",
                "effective_from": "2026-01-01T00:00:00Z",
            },
        )
        same = await c.put(
            "/api/config/fx-rates", json={"base": "USD", "quote": "USD", "rate": "1"}
        )
    assert ok.status_code == 200, ok.text
    assert ok.json()["data"]["quote"] == "CAD"
    assert same.status_code == 422


@pytest.mark.asyncio
async def test_usd_rate_is_one_without_any_row(db_session):
    assert await mb_svc.fx_rate_at(db_session, datetime.now(UTC), quote="USD") == Decimal(1)


# --- reserve -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_usd_tenant_reserves_with_no_fx_row_at_all(
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("mc-res-usd")
    await seed(db_session, *pricing_rows(), money_budget(t, amount=100, currency="USD"))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.post(RESERVE, headers={"X-Tenant-Id": t}, json=_reserve_body())
    assert r.status_code == 200, r.text
    d = r.json()["data"]
    assert d["currency"] == "USD"
    assert Decimal(d["reserved_amount"]) == q8(usd(IN, OUT, PGX_IN, PGX_OUT))


@pytest.mark.asyncio
async def test_cad_tenant_is_priced_with_the_cad_rate(
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("mc-res-cad")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),  # EUR row present -- must NOT be used for a CAD budget
        fx_row(FX_CAD, quote="CAD"),
        money_budget(t, amount=100, currency="CAD"),
    )
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.post(RESERVE, headers={"X-Tenant-Id": t}, json=_reserve_body())
    assert r.status_code == 200, r.text
    d = r.json()["data"]
    assert d["currency"] == "CAD"
    assert Decimal(d["reserved_amount"]) == q8(usd(IN, OUT, PGX_IN, PGX_OUT) * FX_CAD)


@pytest.mark.asyncio
async def test_cad_tenant_without_a_cad_rate_fails_closed(
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("mc-res-cad-missing")
    await seed(db_session, *pricing_rows(), fx_row(), money_budget(t, amount=100, currency="CAD"))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.post(RESERVE, headers={"X-Tenant-Id": t}, json=_reserve_body())
    assert r.status_code == 429, r.text
    d = r.json()["detail"]
    assert d["reason"] == "fx_rate_missing"
    assert d["currency"] == "CAD"
    assert d["reservation_id"] is None


# --- quote / status ------------------------------------------------------------


@pytest.mark.asyncio
async def test_quote_defaults_to_the_tenant_currency_and_takes_an_explicit_one(
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("mc-quote")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        fx_row(FX_CAD, quote="CAD"),
        money_budget(t, amount=100, currency="CAD"),
    )
    q = f"tenant_id={t}&setup=1&provider=pgx-local&model=local-llm&input_tokens={IN}&output_tokens={OUT}"
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        default = await c.get(f"/api/budget/money/quote?{q}")
        in_usd = await c.get(f"/api/budget/money/quote?{q}&currency=USD")
        bad = await c.get(f"/api/budget/money/quote?{q}&currency=GBP")
    base = usd(IN, OUT, PGX_IN, PGX_OUT)
    assert default.status_code == 200, default.text
    assert default.json()["currency"] == "CAD"
    assert Decimal(default.json()["amount"]) == q8(base * FX_CAD)
    assert in_usd.json()["currency"] == "USD"
    assert Decimal(in_usd.json()["amount"]) == q8(base)
    assert in_usd.json()["fx_rate"] == "1"
    assert bad.status_code == 422


@pytest.mark.asyncio
async def test_status_reports_the_budget_currency(db_engine, db_session, fake_redis, monkeypatch):
    t = T("mc-status-cad")
    await seed(db_session, money_budget(t, amount=200, consumed="20", currency="CAD"))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.get(f"/api/budget/money/status/{t}", headers={"X-Tenant-Id": t})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["currency"] == "CAD"
    assert d["consumed_by_currency"] == [{"currency": "CAD", "consumed": "20.00000000"}]
    assert d["conversions"] == []


# --- settlement ----------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(("currency", "rate"), [("USD", Decimal(1)), ("CAD", FX_CAD)])
async def test_settlement_is_computed_and_stamped_in_the_budget_currency(
    currency, rate, db_session, db_session_factory, fake_redis
):
    t = T(f"mc-settle-{currency}")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        fx_row(FX_CAD, quote="CAD"),
        *_money_tenant(t, currency=currency, consumed="1"),
    )
    await flush_costs([_event(t, usage_extra=_extra(1))], db_session_factory, fake_redis)

    expected = q8(usd(IN, OUT, PGX_IN, PGX_OUT) * rate)
    async with db_session_factory() as s:
        [row] = (await s.execute(select(CostEvent).where(CostEvent.tenant_id == t))).scalars()
    assert row.consumption_currency == currency
    assert row.consumption_amount == expected
    assert (await _budget(db_session_factory, t)).consumed == Decimal(1) + expected


# --- watcher -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_watcher_payload_carries_the_budget_currency(db_session_factory):
    import httpx

    from services.budget_watcher import _PlatformWebhook, check_and_fire_overages

    t = T("mc-watch-cad")
    async with db_session_factory() as s:
        s.add(money_budget(t, amount=100, consumed="60", currency="CAD"))
        await s.commit()
    sent: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await check_and_fire_overages(
            db_session_factory,
            http_client=client,
            platform_webhook=_PlatformWebhook(url="https://sink.example/h", secret="s"),
        )
    mine = [p for p in sent if p.get("tenant_id") == t]
    assert [p["threshold_pct"] for p in mine] == [50]
    assert mine[0]["currency"] == "CAD"


# --- currency change -------------------------------------------------------------
#
# Caller contract (provisioning PR #45, owner ruling + coordinator 2026-10-02):
# an SA holding onboarding-internal (or license-admin) RELAYS the owner's portal
# JWT in X-Forwarded-User-Authorization; that JWT is verified against the portal
# realm (signature, pinned issuer, expiry) and must carry super_admin or
# platform-admin. Tokens below are genuinely RS256-signed; only the JWKS network
# fetch is substituted.

PORTAL_ISS = "https://kc.test/realms/portal"
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_KID = "portal-test-kid"
OWNER_SUB = "11111111-2222-3333-4444-555555555555"


def _jwks() -> dict:
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(_KEY.public_key()))
    jwk.update(kid=_KID, use="sig", alg="RS256")
    return {"keys": [jwk]}


def _owner_jwt(
    *,
    roles: tuple[str, ...] = ("super_admin",),
    key=_KEY,
    iss: str = PORTAL_ISS,
    exp_in: int = 300,
    sub: str = OWNER_SUB,
    azp: str | None = "fr-frontend",
) -> str:
    now = int(time.time())
    claims: dict = {
        "sub": sub,
        "iss": iss,
        "iat": now,
        "exp": now + exp_in,
        "realm_access": {"roles": list(roles)},
    }
    if azp is not None:
        claims["azp"] = azp
    return jwt.encode(
        claims,
        key,
        algorithm="RS256",
        headers={"kid": _KID},
    )


def _owner(token: str | None = None) -> dict[str, str]:
    return {"X-Forwarded-User-Authorization": f"Bearer {token or _owner_jwt()}"}


@pytest.fixture(autouse=True)
def _portal_jwks(monkeypatch):
    async def _fetch():
        return _jwks()

    monkeypatch.setenv("PORTAL_JWT_ISSUER", PORTAL_ISS)
    monkeypatch.setattr(auth, "_fetch_portal_jwks", _fetch)
    auth.reset_portal_jwks_cache()
    yield
    auth.reset_portal_jwks_cache()


def _sa(monkeypatch, roles: list[str], azp: str = "provisioning-billing") -> dict[str, str]:
    """A service-account bearer with exactly ``roles`` (auth enabled for the SA leg)."""

    async def _decode(_request):
        return {"sub": f"service-account-{azp}", "azp": azp, "realm_access": {"roles": roles}}

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _decode)
    return {"Authorization": "Bearer sa-token"}


def _r2(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _r0(x: Decimal) -> int:
    return int(x.to_integral_value(rounding=ROUND_HALF_UP))


async def _convert(c, t: str, currency: str, headers: dict | None = None, **body):
    return await c.post(
        f"/api/budget/money/{t}/currency",
        json={"currency": currency, **body},
        headers=headers if headers is not None else _owner(),
    )


@pytest.mark.asyncio
async def test_sa_plus_super_admin_token_converts_and_keeps_history(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    t = T("mc-convert")
    await seed(
        db_session,
        fx_row(),
        fx_row(FX_CAD, quote="CAD"),
        *_money_tenant(t, currency="EUR", amount=100, consumed="40"),
    )
    sa = _sa(monkeypatch, ["onboarding-internal"])
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _convert(c, t, "USD", {**sa, **_owner()}, reason="owner test")
        status = await c.get(f"/api/budget/money/status/{t}", headers={"X-Tenant-Id": t, **sa})

    assert r.status_code == 200, r.text
    d = r.json()
    cross = Decimal(1) / FX  # EUR -> USD = rate(USD->USD) / rate(USD->EUR)
    remaining_after = _r2(Decimal(60) * cross)  # 65.2173.. -> 65.22 (HALF_UP, minor units)
    amount_after = _r0(Decimal(100) * cross)  # 108.69.. -> 109
    assert d["changed"] is True
    assert (d["from_currency"], d["to_currency"]) == ("EUR", "USD")
    assert Decimal(d["remaining_before"]) == Decimal(60)
    assert Decimal(d["remaining_after"]) == remaining_after == Decimal("65.22")
    assert (d["amount_before"], d["amount"]) == (100, amount_after)
    assert Decimal(d["consumed_before"]) == Decimal(40)
    assert Decimal(d["fx_rate"]) == cross.normalize() or abs(
        Decimal(d["fx_rate"]) - cross
    ) < Decimal("1e-15")

    b = await _budget(db_session_factory, t)
    assert (b.currency, b.amount) == ("USD", amount_after)
    assert Decimal(b.amount) - b.consumed == remaining_after
    assert str(b.id) == d["budget_id"]

    async with db_session_factory() as s:
        [adj] = (
            await s.execute(
                select(MoneyBudgetAdjustment).where(MoneyBudgetAdjustment.tenant_id == t)
            )
        ).scalars()
        assert adj.kind == "currency_conversion"
        assert (adj.from_currency, adj.to_currency) == ("EUR", "USD")
        assert adj.consumed_before == Decimal(40)
        assert adj.remaining_after == remaining_after
        assert (adj.usd_rate_from, adj.usd_rate_to) == (FX, Decimal(1))
        assert (adj.actor_sub, adj.actor_role) == (OWNER_SUB, "super_admin")
        assert adj.via_client == "provisioning-billing"
        assert adj.reason == "owner test"
        [audit] = (
            (
                await s.execute(
                    select(AuditLog).where(
                        AuditLog.action == "budget.money.currency_change",
                        AuditLog.resource_id == str(b.id),
                    )
                )
            )
            .scalars()
            .all()
        )
    assert (audit.actor_sub, audit.actor_role) == (OWNER_SUB, "super_admin")
    assert audit.after["via"] == "provisioning-billing"
    assert (audit.before["currency"], audit.after["currency"]) == ("EUR", "USD")
    assert (audit.before["amount"], audit.after["amount"]) == (100, amount_after)
    assert "Bearer" not in json.dumps([audit.before, audit.after])

    assert status.status_code == 200, status.text
    st = status.json()
    assert st["currency"] == "USD"
    assert Decimal(st["remaining"]) == remaining_after
    assert st["consumed_by_currency"] == [
        {"currency": "EUR", "consumed": "40.00000000"},
        {"currency": "USD", "consumed": "0.00000000"},
    ]
    [conv] = st["conversions"]
    assert (conv["from_currency"], conv["to_currency"]) == ("EUR", "USD")


@pytest.mark.asyncio
async def test_platform_admin_and_license_admin_sa_are_admitted(
    db_engine, db_session, fake_redis, monkeypatch
):
    t = T("mc-convert-pa")
    await seed(db_session, fx_row(), *_money_tenant(t, currency="EUR"))
    sa = _sa(monkeypatch, ["license-admin"], azp="fr-backend")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _convert(c, t, "USD", {**sa, **_owner(_owner_jwt(roles=("platform-admin",)))})
    assert r.status_code == 200, r.text
    assert r.json()["changed"] is True


@pytest.mark.asyncio
async def test_two_conversions_eur_usd_cad_chain_history(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    t = T("mc-convert-chain")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        fx_row(FX_CAD, quote="CAD"),
        *_money_tenant(t, currency="EUR", amount=100, consumed="10"),
    )
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r1 = await _convert(c, t, "USD")
        assert r1.status_code == 200, r1.text
        # 1 USD of spend in the USD segment.
        async with db_session_factory() as s:
            await mb_svc.debit_money_budget(s, t, Decimal(1), now=datetime.now(UTC))
            await s.commit()
        r2 = await _convert(c, t, "CAD")
        assert r2.status_code == 200, r2.text
        st = (await c.get(f"/api/budget/money/status/{t}", headers={"X-Tenant-Id": t})).json()

    assert Decimal(r2.json()["fx_rate"]) == FX_CAD  # USD -> CAD cross = rate(CAD) / 1
    assert st["currency"] == "CAD"
    assert st["consumed_by_currency"] == [
        {"currency": "EUR", "consumed": "10.00000000"},
        {"currency": "USD", "consumed": "1.00000000"},
        {"currency": "CAD", "consumed": "0.00000000"},
    ]
    assert [c["to_currency"] for c in st["conversions"]] == ["USD", "CAD"]


@pytest.mark.asyncio
async def test_cap_rounding_down_leaves_an_opening_credit_not_a_lost_remaining(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    """USD 10 -> EUR @0.924: remaining 9.24, cap rounds HALF_UP to 9 -> consumed -0.24."""
    t = T("mc-convert-credit")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(Decimal("0.924")),
        *_money_tenant(t, currency="USD", amount=10),
    )
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _convert(c, t, "EUR")
        st = (await c.get(f"/api/budget/money/status/{t}", headers={"X-Tenant-Id": t})).json()
    assert r.status_code == 200, r.text
    assert (r.json()["amount"], Decimal(r.json()["remaining_after"])) == (9, Decimal("9.24"))
    b = await _budget(db_session_factory, t)
    assert b.consumed == Decimal("-0.24")
    assert Decimal(st["remaining"]) == Decimal("9.24")
    assert st["thresholds_crossed"] == []


@pytest.mark.asyncio
async def test_change_to_the_same_currency_is_an_idempotent_200(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    """Provisioning calls billing FIRST and retries it: a repeat must not convert again."""
    t = T("mc-convert-same")
    await seed(db_session, fx_row(), *_money_tenant(t, currency="EUR", consumed="5"))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        same = await _convert(c, t, "EUR")
        first = await _convert(c, t, "USD")
        retry = await _convert(c, t, "USD")
    assert same.status_code == 200 and same.json()["changed"] is False
    assert first.json()["changed"] is True
    assert retry.status_code == 200, retry.text
    assert retry.json()["changed"] is False
    assert retry.json()["amount"] == first.json()["amount"]
    async with db_session_factory() as s:
        n = (
            await s.execute(
                select(func.count())
                .select_from(MoneyBudgetAdjustment)
                .where(MoneyBudgetAdjustment.tenant_id == t)
            )
        ).scalar()
    assert n == 1


@pytest.mark.asyncio
async def test_change_currency_without_the_target_rate_is_fx_rate_missing(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    t = T("mc-convert-norate")
    await seed(db_session, fx_row(), *_money_tenant(t, currency="EUR", consumed="5"))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _convert(c, t, "CAD")
        missing = await _convert(c, T("mc-nobudget"), "USD")
    assert r.status_code == 422, r.text
    assert r.json()["reason"] == "fx_rate_missing"
    b = await _budget(db_session_factory, t)
    assert (b.currency, b.consumed) == ("EUR", Decimal(5))
    assert missing.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("label", "sa_roles", "owner", "expected"),
    [
        ("no-forwarded-token", ["onboarding-internal"], None, 401),
        ("no-forwarded-token-even-license-admin", ["license-admin"], None, 401),
        ("non-owner-token", ["onboarding-internal"], ("member",), 403),
        ("tenant-admin-token", ["onboarding-internal"], ("tenant_admin",), 403),
        ("forged-token", ["onboarding-internal"], "forged", 401),
        ("expired-token", ["onboarding-internal"], "expired", 401),
        ("wrong-realm-token", ["onboarding-internal"], "wrong-iss", 401),
        ("not-bearer", ["onboarding-internal"], "basic", 401),
        # A genuine portal super_admin token minted for ANOTHER portal client.
        ("foreign-client-token", ["onboarding-internal"], "foreign-azp", 401),
        ("no-azp-token", ["onboarding-internal"], "no-azp", 401),
        ("billing-internal-sa-only", ["billing-internal"], ("super_admin",), 403),
        ("no-sa-role", ["default-roles-secure-apps"], ("super_admin",), 403),
    ],
)
async def test_currency_change_refusals_write_nothing(
    label,
    sa_roles,
    owner,
    expected,
    db_engine,
    db_session,
    db_session_factory,
    fake_redis,
    monkeypatch,
):
    t = T(f"mc-refuse-{label}")
    await seed(db_session, fx_row(), *_money_tenant(t, currency="EUR"))
    headers = _sa(monkeypatch, sa_roles)
    if owner == "forged":
        headers.update(_owner(_owner_jwt(key=_OTHER_KEY)))
    elif owner == "expired":
        headers.update(_owner(_owner_jwt(exp_in=-120)))
    elif owner == "wrong-iss":
        headers.update(_owner(_owner_jwt(iss="https://kc.test/realms/secure-apps")))
    elif owner == "basic":
        headers["X-Forwarded-User-Authorization"] = "Basic Zm9vOmJhcg=="
    elif owner == "foreign-azp":
        headers.update(_owner(_owner_jwt(azp="commonpractice-frontend")))
    elif owner == "no-azp":
        headers.update(_owner(_owner_jwt(azp=None)))
    elif owner is not None:
        headers.update(_owner(_owner_jwt(roles=owner)))
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _convert(c, t, "USD", headers)
    assert r.status_code == expected, (label, r.text)
    assert "eyJ" not in r.text  # the token is never echoed
    b = await _budget(db_session_factory, t)
    assert b.currency == "EUR"
    async with db_session_factory() as s:
        n = (
            await s.execute(
                select(func.count())
                .select_from(MoneyBudgetAdjustment)
                .where(MoneyBudgetAdjustment.tenant_id == t)
            )
        ).scalar()
    assert n == 0


@pytest.mark.asyncio
async def test_change_currency_is_refused_while_holds_are_in_flight(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    """409 holds_in_flight while a reservation is open; once it settles, it converts."""
    t = T("mc-convert-holds")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        fx_row(FX_CAD, quote="CAD"),
        *_money_tenant(t, currency="EUR", amount=100, consumed="0"),
    )
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
    assert held["allowed"] and held["currency"] == "EUR"

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        refused = await _convert(c, t, "CAD")
        assert refused.status_code == 409, refused.text
        assert refused.json()["detail"] == {"reason": "holds_in_flight", "holds": 1}
        assert (await _budget(db_session_factory, t)).currency == "EUR"

        await flush_costs(
            [_event(t, usage_extra=_extra(1), reservation_id=held["reservation_id"])],
            db_session_factory,
            fake_redis,
        )
        assert await mb_svc.in_flight_amount(fake_redis, t) == Decimal(0)
        ok = await _convert(c, t, "CAD")
    assert ok.status_code == 200, ok.text
    assert await fake_redis.get(mb_svc.money_currency_key(t)) == "CAD"
    async with db_session_factory() as s:
        [row] = (await s.execute(select(CostEvent).where(CostEvent.tenant_id == t))).scalars()
    # Settled BEFORE the change: stays in its original currency.
    assert row.consumption_currency == "EUR"
    assert row.consumption_amount == q8(usd(IN, OUT, PGX_IN, PGX_OUT) * FX)


@pytest.mark.asyncio
async def test_a_hold_racing_the_change_is_redenominated_and_settles_by_handle(
    db_session, db_session_factory, fake_redis
):
    """A reserve that read EUR and landed its hold after the hold count: the
    post-commit marker move re-denominates it (rounded UP), it keeps its handle,
    and its cost event settles in the NEW currency and releases it."""
    t = T("mc-race-hold")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        fx_row(FX_CAD, quote="CAD"),
        *_money_tenant(t, currency="EUR", amount=100),
    )
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
    eur_units = int(await fake_redis.hget(mb_svc.money_amounts_key(t), held["reservation_id"]))
    async with db_session_factory() as s:
        b = await mb_svc.get_active_money_budget(s, t, for_update=True)
        assert b is not None
        b.currency = "CAD"
        await s.commit()
    n = await mb_svc.convert_money_holds(
        fake_redis, t, from_currency="EUR", to_currency="CAD", rate=FX_CAD / FX
    )
    assert n == 1
    raw = int(await fake_redis.hget(mb_svc.money_amounts_key(t), held["reservation_id"]))
    assert abs(raw - math.ceil(Decimal(eur_units) * FX_CAD / FX)) <= 1
    assert raw >= Decimal(eur_units) * FX_CAD / FX
    await flush_costs(
        [_event(t, usage_extra=_extra(1), reservation_id=held["reservation_id"])],
        db_session_factory,
        fake_redis,
    )
    assert await mb_svc.in_flight_amount(fake_redis, t) == Decimal(0)
    async with db_session_factory() as s:
        [row] = (await s.execute(select(CostEvent).where(CostEvent.tenant_id == t))).scalars()
    assert row.consumption_currency == "CAD"
    assert row.consumption_amount == q8(usd(IN, OUT, PGX_IN, PGX_OUT) * FX_CAD)


@pytest.mark.asyncio
async def test_reserve_self_heals_when_redis_holds_are_in_a_stale_currency(
    db_session, db_session_factory, fake_redis
):
    """The conversion committed but its Redis step never ran (Redis blip).

    The next reserve must notice the currency marker disagrees with the
    (authoritative) DB budget, convert the stale holds, and admit against
    consistently-denominated numbers -- never add CAD units to a EUR pool.
    """
    t = T("mc-selfheal")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        fx_row(FX_CAD, quote="CAD"),
        *_money_tenant(t, currency="EUR", amount=100),
    )
    kw: dict = {
        "setup": 1,
        "provider": "pgx-local",
        "model": "local-llm",
        "byo": False,
        "input_tokens": IN,
        "output_tokens": OUT,
        "ttl_seconds": 60,
    }
    async with db_session_factory() as s:
        first = await mb_svc.reserve_money(s, fake_redis, tenant_id=t, **kw)
    eur_units = int(await fake_redis.hget(mb_svc.money_amounts_key(t), first["reservation_id"]))

    # Simulate the DB half of a conversion only.
    async with db_session_factory() as s:
        b = await mb_svc.get_active_money_budget(s, t, for_update=True)
        assert b is not None
        b.currency = "CAD"
        await s.commit()

    async with db_session_factory() as s:
        second = await mb_svc.reserve_money(s, fake_redis, tenant_id=t, **kw)
    assert second["allowed"] and second["currency"] == "CAD"
    converted = int(await fake_redis.hget(mb_svc.money_amounts_key(t), first["reservation_id"]))
    assert abs(converted - math.ceil(Decimal(eur_units) * FX_CAD / FX)) <= 1
    assert await fake_redis.get(mb_svc.money_currency_key(t)) == "CAD"


# --- existing tenants: zero behaviour change -------------------------------------


@pytest.mark.asyncio
async def test_token_tenant_without_money_budget_sees_no_change(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch
):
    """A tenant with no money budget: token debit as before, no money columns,
    no money Redis keys, money status 404, and the currency route 404."""
    t = T("mc-legacy")
    await seed(
        db_session,
        *pricing_rows(),
        fx_row(),
        fx_row(FX_CAD, quote="CAD"),
        make_budget(tenant=t, cap=1_000_000, consumed=0),
    )
    await flush_costs([_event(t), _event(t, usage_extra=_extra(1))], db_session_factory, fake_redis)
    async with db_session_factory() as s:
        rows = list((await s.execute(select(CostEvent).where(CostEvent.tenant_id == t))).scalars())
        tb = (await s.execute(select(TokenBudget).where(TokenBudget.tenant_id == t))).scalar_one()
        n_money = (
            await s.execute(
                select(func.count()).select_from(MoneyBudget).where(MoneyBudget.tenant_id == t)
            )
        ).scalar()
    assert len(rows) == 2
    assert all(r.consumption_amount is None and r.consumption_currency is None for r in rows)
    assert tb.consumed_tokens == Decimal(2 * (IN + OUT))
    assert n_money == 0
    assert [k async for k in fake_redis.scan_iter(f"budget:money:{{{t}}}:*")] == []

    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        st = await c.get(f"/api/budget/money/status/{t}", headers={"X-Tenant-Id": t})
        cv = await _convert(c, t, "USD")
    assert st.status_code == 404
    assert cv.status_code == 404


# --- FX rate attribution (2026-10-14) ----------------------------------------------
#
# fr's super-admin pricing page PUTs the rate with its secure-apps SA (license-admin)
# and RELAYS the signed-in super_admin's portal JWT in X-Forwarded-User-Authorization
# -- the currency-change contract above. set_by and the audit row must name that
# HUMAN, not the SA; with no header the caller's own sub is recorded.

_FX_BODY = {"base": "USD", "quote": "CAD", "rate": "1.37", "effective_from": "2026-01-01T00:00:00Z"}
SA_SUB = "service-account-fr-secure-apps"


async def _fx_rows_and_audit(db_session) -> tuple[list[FxRate], list[AuditLog]]:
    rows = (await db_session.execute(select(FxRate).where(FxRate.quote == "CAD"))).scalars().all()
    audits = (
        (await db_session.execute(select(AuditLog).where(AuditLog.action == "config.fx_rate.put")))
        .scalars()
        .all()
    )
    return list(rows), list(audits)


@pytest.mark.asyncio
async def test_fx_put_relayed_by_a_pricing_writer_records_the_owner(
    db_engine, db_session, fake_redis, monkeypatch
):
    sa = _sa(monkeypatch, ["license-admin"], azp="fr-secure-apps")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put("/api/config/fx-rates", json=_FX_BODY, headers={**sa, **_owner()})
    assert r.status_code == 200, r.text
    assert r.json()["data"]["set_by"] == OWNER_SUB
    rows, audits = await _fx_rows_and_audit(db_session)
    assert [x.set_by for x in rows] == [OWNER_SUB]
    assert [(a.actor_sub, a.actor_role) for a in audits] == [(OWNER_SUB, "super_admin")]


@pytest.mark.asyncio
async def test_fx_put_without_a_forwarded_owner_records_the_caller(
    db_engine, db_session, fake_redis, monkeypatch
):
    sa = _sa(monkeypatch, ["pricing-writer"], azp="fr-secure-apps")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put("/api/config/fx-rates", json=_FX_BODY, headers=sa)
    assert r.status_code == 200, r.text
    assert r.json()["data"]["set_by"] == SA_SUB
    _rows, audits = await _fx_rows_and_audit(db_session)
    assert [(a.actor_sub, a.actor_role) for a in audits] == [(SA_SUB, "pricing-writer")]


@pytest.mark.asyncio
# Token kwargs, not tokens: a JWT minted at collection time expires (300 s) long
# before a full-suite run reaches this test, turning the 403 case into a 401.
@pytest.mark.parametrize(
    ("token_kwargs", "status"),
    [
        ({"key": _OTHER_KEY}, 401),  # forged signature
        ({"exp_in": -60}, 401),  # expired
        ({"iss": "https://kc.test/realms/secure-apps"}, 401),  # wrong realm
        ({"roles": ("tenant_admin",)}, 403),  # verified, but not an owner
        ({"azp": "kag-frontend"}, 401),  # super_admin, but another portal client
        ({"azp": None}, 401),  # no azp at all
    ],
    ids=["forged", "expired", "wrong_realm", "not_owner", "foreign_client", "no_azp"],
)
async def test_fx_put_with_an_unverifiable_or_non_owner_actor_writes_nothing(
    db_engine, db_session, fake_redis, monkeypatch, token_kwargs, status
):
    token = _owner_jwt(**token_kwargs)
    sa = _sa(monkeypatch, ["license-admin"], azp="fr-secure-apps")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put("/api/config/fx-rates", json=_FX_BODY, headers={**sa, **_owner(token)})
    assert r.status_code == status, r.text
    assert await _fx_rows_and_audit(db_session) == ([], [])


@pytest.mark.asyncio
async def test_fx_put_a_forwarded_owner_does_not_elevate_a_non_pricing_writer(
    db_engine, db_session, fake_redis, monkeypatch
):
    # billing-internal may seed provider cost but never billed money (#55); a
    # perfectly valid owner token relayed by it must not change that.
    sa = _sa(monkeypatch, ["billing-internal"], azp="billing-service")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put("/api/config/fx-rates", json=_FX_BODY, headers={**sa, **_owner()})
    assert r.status_code == 403, r.text
    assert await _fx_rows_and_audit(db_session) == ([], [])


# --- FORWARDED_OWNER_ALLOWED_AZP is a setting -------------------------------------


@pytest.fixture
def _allowed_azp(monkeypatch):
    def _set(value: str) -> None:
        monkeypatch.setenv("FORWARDED_OWNER_ALLOWED_AZP", value)
        get_settings.cache_clear()

    yield _set
    monkeypatch.delenv("FORWARDED_OWNER_ALLOWED_AZP", raising=False)
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_fx_put_owner_from_an_allow_listed_extra_client_is_accepted(
    db_engine, db_session, fake_redis, monkeypatch, _allowed_azp
):
    _allowed_azp("fr-frontend, other-spa")
    sa = _sa(monkeypatch, ["license-admin"], azp="fr-secure-apps")
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await c.put(
            "/api/config/fx-rates",
            json=_FX_BODY,
            headers={**sa, **_owner(_owner_jwt(azp="other-spa"))},
        )
    assert r.status_code == 200, r.text
    assert r.json()["data"]["set_by"] == OWNER_SUB


@pytest.mark.asyncio
async def test_currency_change_refuses_fr_frontend_once_it_is_not_allow_listed(
    db_engine, db_session, db_session_factory, fake_redis, monkeypatch, _allowed_azp
):
    _allowed_azp("other-spa")
    t = T("mc-azp-narrowed")
    await seed(db_session, fx_row(), *_money_tenant(t, currency="EUR"))
    headers = {**_sa(monkeypatch, ["onboarding-internal"]), **_owner()}
    async with money_client(db_engine, monkeypatch, fake_redis) as c:
        r = await _convert(c, t, "USD", headers)
    assert r.status_code == 401, r.text
    assert "forwarded_user_token_invalid" in r.text
    assert (await _budget(db_session_factory, t)).currency == "EUR"


def test_an_empty_forwarded_owner_allowlist_is_refused_at_construction(monkeypatch):
    monkeypatch.setenv("FORWARDED_OWNER_ALLOWED_AZP", " , ")
    with pytest.raises(ValueError, match="forwarded_owner_allowed_azp"):
        Settings()
