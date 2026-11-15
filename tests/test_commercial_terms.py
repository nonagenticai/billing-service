"""Per-tenant commercial terms — the rate card as one idempotent operation.

George, 2026-09-10: platform fee $5,000/month flat (usage on top), seat fee $25,
one seat = one active licence, markup 20% globally AND on BYO lines, "configure
at provisioning a new tenant and after they are live".

These tests pin the four things that can silently produce a wrong bill:
  1. idempotency — a replay must not re-date a live tenant's terms;
  2. the seat RATE is multiplied, not billed flat;
  3. a licences outage never becomes a fabricated zero on a frozen invoice;
  4. provenance — a default-valued term says so, and is not mistaken for a
     priced one.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import AuditLog, MarkupRule, TenantAccessCharge
from models.domain import AccessChargeCreate
from service_auth import ServiceTokenProvider
from services import access_charge as access_charge_svc
from services import markup as markup_svc
from services import seats as seats_svc
from tests.conftest import T

pytestmark = pytest.mark.asyncio

TID = T("ahtapol")


@pytest.fixture
def admin_actor(monkeypatch):
    """Force require_pricing_admin to a known admin payload."""
    fake_payload = {"sub": "test-admin-user", "realm_access": {"roles": ["license-admin"]}}

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


@pytest.fixture(autouse=True)
def _seat_cache_isolated():
    """The seat cache is module state with a 5-min TTL — one test's answer must
    not decide another's. Cleared both sides so ordering cannot matter."""
    seats_svc.clear_seat_cache()
    yield
    seats_svc.clear_seat_cache()


@pytest.fixture
def seats_say(monkeypatch):
    """Stub the licences SEAT-COUNT endpoint with a chosen answer.

    ⚠️ URL-AWARE ON PURPOSE. `httpx.AsyncClient.get` is also how
    `services.billing.fetch_licenses` retrieves the licence LIST, so a blanket
    stub feeds a seat-count object to a caller expecting a list of licences —
    which surfaces as `AttributeError: 'str' object has no attribute 'get'` deep
    inside `generate_billing` and looks like a billing bug. Every other GET gets
    a shape its caller can actually parse.
    """

    def _install(seats: int | None = 3, status_code: int = 200):
        seats_request.clear()
        seats_requests.clear()

        async def _fake_get(self, url, *a, **kw):  # noqa: ANN001
            if "seat-count" in str(url):
                seats_request["url"] = str(url)
                seats_request["headers"] = dict(kw.get("headers") or {})
                seats_request["params"] = dict(kw.get("params") or {})
                # ⭐ EVERY request, not only the last. `close_period` makes TWO seat
                # calls — `assert_seat_count_available` (the gate) and then
                # `generate_billing` (the number that is FROZEN) — so a last-write
                # dict silently grades only the second. Asserting on the list is
                # what makes a missing `as_of` on the GATE visible; with the dict
                # alone that mutation survived a full green suite.
                seats_requests.append(dict(seats_request))
                payload = {"tenant_id": TID, "seats": seats, "basis": "active_licences"}
                return httpx.Response(status_code, json=payload)
            # Each other endpoint gets a shape ITS caller can parse. A single
            # generic answer does not exist here: `fetch_licenses` wants a list,
            # `get_byo_mode` wants an object with `byo_mode`, and feeding either
            # the other's shape raises deep inside generate_billing.
            if "byo-mode" in str(url):
                return httpx.Response(200, json={"byo_mode": "byo_required"})
            # ⛔ AND `/internal/tenants` WANTS A REGISTRY, NOT AN EMPTY LIST. Added
            # 2026-09-13 with `services.invoices.assert_tenant_invoiceable`, which
            # refuses to close a period for a tenant the registry does not vouch
            # for: the generic `[]` below made every close in this file refuse as a
            # phantom tenant. Same reasoning as the arms above — each endpoint gets a
            # shape its caller can act on, and for this one an empty answer is not a
            # degraded label, it is a refusal.
            if str(url).endswith("/internal/tenants"):
                return httpx.Response(
                    200,
                    json=[
                        {
                            "id": TID,
                            "name": "terms-tenant",
                            "kind": "customer",
                            "archived": False,
                        }
                    ],
                )
            return httpx.Response(200, json=[])

        monkeypatch.setattr(httpx.AsyncClient, "get", _fake_get)

    return _install


#: What the last stubbed seat-count request actually carried. A module-level dict
#: rather than a second fixture so `seats_say` can write into it without the test
#: having to request both and keep them in step.
seats_request: dict[str, Any] = {}

#: Every stubbed seat-count request, in order. See the note in `seats_say`.
seats_requests: list[dict] = []


class _StubTokenProvider:
    """A service-account token provider that always has a token."""

    async def get_token(self) -> str:
        # pragma: allowlist secret - a fixed test string, not a credential
        return "test-service-token"  # pragma: allowlist secret

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        return {  # pragma: allowlist secret
            "X-Tenant-Id": tenant_id,
            "Authorization": "Bearer test-service-token",  # pragma: allowlist secret
        }


@pytest.fixture(autouse=True)
def _seat_service_token(monkeypatch):
    """Give every test in this module a service-account token for the seat call.

    ⭐ WHY THIS IS AUTOUSE AND NOT PART OF `seats_say`. `/internal/` routes on
    licenses are gated by `require_internal_service`, and `services.seats` fails
    CLOSED when no token is available — it raises BEFORE the HTTP call. Several
    tests here stub `httpx.AsyncClient.get` directly rather than through
    `seats_say`, so a stub attached only to that fixture would leave them raising
    `SeatCountUnavailableError` for a reason unrelated to what they assert.

    ⚠️ The token mint is a POST; this module's stubs patch GET. Without this, a
    test would make a real POST to the configured Keycloak URL — slow,
    non-deterministic, and in CI simply absent.

    ⛔ This cannot hide a regression that drops the header: `seats_request`
    captures what reached the wire and
    `TestSeatCountAuthentication::test_authorization_header_reaches_licences`
    asserts on it. A test that needs the NO-token path overrides this explicitly.
    """
    monkeypatch.setattr(seats_svc, "_provider_for", lambda _s: _StubTokenProvider())


async def _rows(db_engine, model):
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        return list((await s.execute(select(model))).scalars().all())


async def _audit(db_engine, prefix: str):
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        rows = list((await s.execute(select(AuditLog))).scalars().all())
    return [r for r in rows if r.action.startswith(prefix)]


# --------------------------------------------------------------------------- #
# The materialising endpoint
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestMaterialiseTerms:
    async def test_creates_the_four_terms_at_georges_numbers(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        seats_say(seats=3)
        resp = app_client.post(
            f"/api/config/tenants/{TID}/terms", headers={"Authorization": "Bearer fake"}, json={}
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()["data"]

        assert Decimal(data["platform_fee"]["value"]) == Decimal("5000")
        assert Decimal(data["seat_fee"]["value"]) == Decimal("25")
        assert Decimal(data["markup"]["value"]) == Decimal("20")
        assert Decimal(data["byo_markup"]["value"]) == Decimal("20")
        assert data["fully_configured"] is True
        # The seat count is resolved live and disclosed beside the rate.
        assert data["seat_count"] == 3
        assert data["seat_basis"] == "active_licences"

        charges = await _rows(db_engine, TenantAccessCharge)
        by_key = {c.service_key: c for c in charges}
        # ⚠️ The platform fee owns service_key NULL; the seat charge MUST take a
        # distinct key or ix_accesscharge_open would force one to close the other.
        assert by_key[None].cadence == "monthly"
        assert by_key[None].included_cost_usd is None  # flat: usage billed on top
        assert by_key["__seat__"].cadence == "per_seat"

        rules = await _rows(db_engine, MarkupRule)
        assert {r.byo for r in rules} == {None, True}

        # Four audit rows, one per term — the change is attributable.
        assert len(await _audit(db_engine, "config.terms.")) == 4

    async def test_overrides_win_over_defaults(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        seats_say(seats=1)
        resp = app_client.post(
            f"/api/config/tenants/{TID}/terms",
            headers={"Authorization": "Bearer fake"},
            json={"platform_fee_usd": "12000", "byo_markup_pct": "35"},
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()["data"]
        assert Decimal(data["platform_fee"]["value"]) == Decimal("12000")
        assert Decimal(data["byo_markup"]["value"]) == Decimal("35")
        # Unstated terms still take the standard rate card.
        assert Decimal(data["seat_fee"]["value"]) == Decimal("25")
        assert Decimal(data["markup"]["value"]) == Decimal("20")

    async def test_replay_is_idempotent_and_does_not_redate(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        """⭐ THE ONE THAT MATTERS FOR PROVISIONING RETRIES.

        `upsert_access_charge` and `upsert_markup_rule` CLOSE the prior open row.
        So an "always upsert" version of this endpoint would, on every retry, end
        the current period and open a new one — shredding the effective-dated
        history and making a tenant look re-priced on a schedule nobody chose.

        ⚠️ MUTATION TARGET: delete the `await _open_access_charge(...) is None`
        guards in routers/config.py and the row count doubles.
        """
        seats_say(seats=2)
        h = {"Authorization": "Bearer fake"}
        first = app_client.post(f"/api/config/tenants/{TID}/terms", headers=h, json={})
        assert first.status_code == 201
        charges_1 = await _rows(db_engine, TenantAccessCharge)
        rules_1 = await _rows(db_engine, MarkupRule)

        second = app_client.post(f"/api/config/tenants/{TID}/terms", headers=h, json={})
        assert second.status_code == 201
        charges_2 = await _rows(db_engine, TenantAccessCharge)
        rules_2 = await _rows(db_engine, MarkupRule)

        assert len(charges_2) == len(charges_1) == 2
        assert len(rules_2) == len(rules_1) == 2
        # Nothing was closed: every row is still the open one it was.
        assert all(c.effective_until is None for c in charges_2)
        assert all(r.effective_until is None for r in rules_2)
        assert {c.id for c in charges_2} == {c.id for c in charges_1}
        # And the second call wrote no audit rows, because it changed nothing.
        assert len(await _audit(db_engine, "config.terms.")) == 4

    async def test_force_reprices_by_closing_the_prior_row(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        """`?force=true` is the deliberate re-price: prior rows CLOSE, they are
        never edited, so the history survives the change."""
        seats_say(seats=2)
        h = {"Authorization": "Bearer fake"}
        app_client.post(f"/api/config/tenants/{TID}/terms", headers=h, json={})
        resp = app_client.post(
            f"/api/config/tenants/{TID}/terms?force=true",
            headers=h,
            json={"platform_fee_usd": "7500"},
        )
        assert resp.status_code == 201, resp.text
        assert Decimal(resp.json()["data"]["platform_fee"]["value"]) == Decimal("7500")

        charges = await _rows(db_engine, TenantAccessCharge)
        closed = [c for c in charges if c.effective_until is not None]
        open_rows = [c for c in charges if c.effective_until is None]
        assert len(closed) == 2  # the prior platform fee and the prior seat fee
        assert len(open_rows) == 2
        assert Decimal(next(c.amount_usd for c in open_rows if c.service_key is None)) == Decimal(
            "7500"
        )


# --------------------------------------------------------------------------- #
# Read-back and provenance
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestReadBackProvenance:
    async def test_unpriced_tenant_reports_default_and_says_it_is_a_default(
        self, app_client: TestClient, admin_actor
    ) -> None:
        """⭐ The distinction the whole endpoint exists for.

        A tenant with no rows still BILLS at 20% (settings fallback), so showing
        only numbers would make it indistinguishable from a priced tenant. The
        markup term carries the value with `source=settings_default`, and the
        platform fee — which genuinely does not exist — reports value None, NOT
        0: "no fee configured" and "a $0.00 fee" are different commercial facts.
        """
        resp = app_client.get(
            f"/api/config/tenants/{TID}/terms", headers={"Authorization": "Bearer fake"}
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["fully_configured"] is False
        assert data["platform_fee"]["value"] is None
        assert data["platform_fee"]["source"] == "no_matching_access_charge"
        assert Decimal(data["markup"]["value"]) == Decimal("20.0")
        assert data["markup"]["source"] == "settings_default"
        # No per-seat term => no seat count, and None is not 0.
        assert data["seat_count"] is None
        assert data["seat_basis"] is None

    async def test_priced_tenant_reports_the_rule_id_as_provenance(
        self, app_client: TestClient, admin_actor, seats_say
    ) -> None:
        seats_say(seats=3)
        h = {"Authorization": "Bearer fake"}
        app_client.post(f"/api/config/tenants/{TID}/terms", headers=h, json={})
        data = app_client.get(f"/api/config/tenants/{TID}/terms", headers=h).json()["data"]
        assert data["markup"]["source"].startswith("markup_rule:")
        assert data["byo_markup"]["source"].startswith("markup_rule:")
        assert data["platform_fee"]["source"] == "tenant_access_charge"
        assert data["platform_fee"]["effective_from"] is not None

    async def test_licences_outage_does_not_make_terms_unreadable(
        self, app_client: TestClient, admin_actor, seats_say
    ) -> None:
        """A 503 from licences must degrade the seat COUNT, not the whole page —
        and it must say `unavailable` rather than reporting 0 seats."""
        h = {"Authorization": "Bearer fake"}
        seats_say(seats=3)
        app_client.post(f"/api/config/tenants/{TID}/terms", headers=h, json={})
        seats_svc.clear_seat_cache()
        seats_say(status_code=503)
        data = app_client.get(f"/api/config/tenants/{TID}/terms", headers=h).json()["data"]
        assert Decimal(data["seat_fee"]["value"]) == Decimal("25")  # the RATE survives
        assert data["seat_count"] is None
        assert data["seat_basis"] == "unavailable"


# --------------------------------------------------------------------------- #
# services.seats — fail closed, and cache only real answers
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestSeatCount:
    async def test_200_returns_the_count(self, seats_say) -> None:
        from settings import get_settings

        seats_say(seats=3)
        assert await seats_svc.get_seat_count(TID, get_settings()) == 3

    async def test_404_is_a_real_answer_and_means_zero(self, seats_say) -> None:
        """licences not knowing a tenant is durable information ABOUT the tenant —
        it has no seats to bill — so 0 is correct here and is cached."""
        from settings import get_settings

        seats_say(seats=None, status_code=404)
        assert await seats_svc.get_seat_count(TID, get_settings()) == 0

    async def test_503_raises_and_is_not_cached(self, monkeypatch) -> None:
        """⭐ A 5xx says something about the SERVICE, not the tenant.

        Two properties in one test because they are one decision: it raises
        (never guesses), and it does not poison the cache — so the next call,
        seconds later against a recovered licences, gets the real answer instead
        of a 5-minute-old failure.

        ⚠️ MUTATION TARGET: replace the `raise` in services/seats.py with
        `return 0` and this fails.
        """
        from settings import get_settings

        calls = {"n": 0}

        async def _flaky(self, url, *a, **kw):  # noqa: ANN001
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(503, json={"error": "down"})
            return httpx.Response(200, json={"tenant_id": TID, "seats": 7})

        monkeypatch.setattr(httpx.AsyncClient, "get", _flaky)
        with pytest.raises(seats_svc.SeatCountUnavailableError):
            await seats_svc.get_seat_count(TID, get_settings())
        assert await seats_svc.get_seat_count(TID, get_settings()) == 7
        assert calls["n"] == 2  # the failure was not cached

    async def test_network_error_raises(self, monkeypatch) -> None:
        from settings import get_settings

        async def _boom(self, url, *a, **kw):  # noqa: ANN001
            raise httpx.ConnectError("no route")

        monkeypatch.setattr(httpx.AsyncClient, "get", _boom)
        with pytest.raises(seats_svc.SeatCountUnavailableError):
            await seats_svc.get_seat_count(TID, get_settings())

    async def test_unparseable_and_negative_both_raise(self, monkeypatch) -> None:
        """A 200 carrying nonsense is not an answer either."""
        from settings import get_settings

        async def _no_seats(self, url, *a, **kw):  # noqa: ANN001
            return httpx.Response(200, json={"tenant_id": TID})

        monkeypatch.setattr(httpx.AsyncClient, "get", _no_seats)
        with pytest.raises(seats_svc.SeatCountUnavailableError):
            await seats_svc.get_seat_count(TID, get_settings())

        async def _negative(self, url, *a, **kw):  # noqa: ANN001
            return httpx.Response(200, json={"tenant_id": TID, "seats": -5})

        seats_svc.clear_seat_cache()
        monkeypatch.setattr(httpx.AsyncClient, "get", _negative)
        with pytest.raises(seats_svc.SeatCountUnavailableError):
            await seats_svc.get_seat_count(TID, get_settings())

    async def test_envelope_wrapped_response_is_accepted(self, monkeypatch) -> None:
        """licenses wraps some internal routes in an Envelope and not others;
        which one this route uses is not billing's business to assume."""
        from settings import get_settings

        async def _wrapped(self, url, *a, **kw):  # noqa: ANN001
            return httpx.Response(200, json={"data": {"tenant_id": TID, "seats": 4}})

        monkeypatch.setattr(httpx.AsyncClient, "get", _wrapped)
        assert await seats_svc.get_seat_count(TID, get_settings()) == 4


# --------------------------------------------------------------------------- #
# services.seats — the call is AUTHENTICATED, and counted at the period's instant
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestSeatCountAuthentication:
    """The seat-count call sends a service-account token and an `as_of`.

    ⛔ WHAT THIS REGRESSION-GUARDS, measured on prod 2026-09-11 from inside the
    billing pod:
        GET {licenses}/internal/tenants/<uuid>/seat-count
        -> 401 {"error":{"message":"Missing authentication token"}}
    `get_seat_count` built a bare `httpx.AsyncClient` and sent no `Authorization`
    header, while licenses gates every `/internal/` route with
    `require_internal_service`. billing's own SA token carries `service-internal`,
    which that gate accepts — so the credential was always correct and simply was
    never attached. Consequences: the per-seat access charge could never bill at
    all, and once any tenant had a `per_seat` term `assert_seat_count_available`
    refused every close.
    """

    async def test_authorization_header_reaches_licences(self, seats_say) -> None:
        """The header must arrive at the endpoint, not merely be computed.

        Mutation: drop `headers=headers` from the request in
        `services.seats.get_seat_count` and this fails — which is the whole point,
        because the autouse token fixture would otherwise make every other test in
        this file pass with an unauthenticated request.
        """
        from settings import get_settings

        seats_say(seats=3)
        assert await seats_svc.get_seat_count(TID, get_settings()) == 3
        assert seats_request["headers"].get("Authorization") == "Bearer test-service-token"
        assert seats_request["headers"].get("X-Tenant-Id") == TID

    async def test_no_token_fails_closed_before_any_request(self, seats_say, monkeypatch) -> None:
        """No token => refuse, and do NOT send an unauthenticated request.

        ⭐ The second half matters as much as the first. `build_headers` omits the
        header when no token is cached rather than raising, so a version that
        proceeded would surface a bare 401 — sending the operator to debug the
        licences service instead of billing's own credential.
        """
        from settings import get_settings

        class _NoToken:
            async def get_token(self) -> None:
                return None

            def build_headers(self, tenant_id: str) -> dict[str, str]:
                return {"X-Tenant-Id": tenant_id}

        seats_say(seats=3)
        seats_request.clear()
        monkeypatch.setattr(seats_svc, "_provider_for", lambda _s: _NoToken())
        with pytest.raises(seats_svc.SeatCountUnavailableError, match="service-account token"):
            await seats_svc.get_seat_count(TID, get_settings())
        assert seats_request == {}, "it must not have reached the wire"

    async def test_as_of_is_sent_and_is_part_of_the_cache_key(self, seats_say) -> None:
        """A closing period is counted at ITS instant, and does not share a cache
        entry with the live count.

        The endpoint's own parameter description: "A CLOSED billing period must be
        counted at its own instant, never at whenever the query is re-run."

        Mutation: drop `params=params` and the first assertion fails; drop `as_of`
        from the cache key and the third call is served the first call's answer,
        so `seats_request["params"]` still shows the old instant.
        """
        from datetime import UTC, datetime

        from settings import get_settings

        as_of = datetime(2026, 8, 31, 23, 59, 59, tzinfo=UTC)

        seats_say(seats=7)
        assert await seats_svc.get_seat_count(TID, get_settings(), as_of=as_of) == 7
        assert seats_request["params"] == {"as_of": as_of.isoformat()}

        # A live read (no as_of) must NOT be served the as_of answer.
        seats_say(seats=2)
        assert await seats_svc.get_seat_count(TID, get_settings()) == 2
        assert seats_request["params"] == {}

        # And the as_of read is still cached under its own key — a different
        # instant is a different question and re-fetches.
        other = datetime(2026, 7, 31, 23, 59, 59, tzinfo=UTC)
        seats_say(seats=5)
        assert await seats_svc.get_seat_count(TID, get_settings(), as_of=other) == 5
        assert seats_request["params"] == {"as_of": other.isoformat()}

    async def test_close_counts_seats_at_the_periods_own_instant(
        self, db_session: AsyncSession, test_settings, seats_say
    ) -> None:
        """A close must count seats at the LAST INSTANT OF THE PERIOD, not at now.

        ⛔ WHY THIS TEST HAD TO EXIST, proven rather than assumed. With
        `as_of=bill_ts` deleted from `services.invoices.assert_seat_count_available`
        the whole of this file still passed — 28 tests, all green. Every other
        `as_of` assertion here calls `get_seat_count` DIRECTLY, so none of them can
        see whether the CLOSE path supplies one. An unprotected line on a money
        path is how August gets billed at September's seat count.

        The instant asserted is `period_end` minus one microsecond, which is
        `services.invoices._bill_ts` — the same resolution point the effective-dated
        markup, pricing and access-charge lookups already use, so a seat count and
        the rate it multiplies cannot be read as of two different moments.
        """
        from services.byo_mode import clear_byo_cache
        from services.invoices import close_period

        seats_say(seats=3)
        clear_byo_cache()

        await _seed_fully_configured_tenant(db_session)
        await db_session.commit()

        _, today = _this_month()
        period_start = today.replace(day=1)
        period_end = _next_month_start(today)
        await close_period(
            db_session,
            tenant_id=TID,
            period_start=period_start,
            period_end=period_end,
            settings=test_settings,
        )

        expected = (
            datetime.combine(period_end, datetime.min.time(), tzinfo=UTC)
            - timedelta(microseconds=1)
        ).isoformat()
        # ⛔ EVERY seat call the close makes must carry the SAME instant, not just
        # the one whose number is frozen. `close_period` calls
        # `assert_seat_count_available` first and `generate_billing` second; a gate
        # that asks a DIFFERENT question from the freeze is not a precondition for
        # it — licences could answer "now" and refuse the period's instant, and the
        # close would then fail after the gate had already passed.
        assert seats_requests, "the close made no seat-count call at all"
        assert [r["params"] for r in seats_requests] == [
            {"as_of": expected} for _ in seats_requests
        ], (
            "every seat call in a close must use the period's own last instant; got "
            f"{[r['params'] for r in seats_requests]}"
        )
        # And it is authenticated on this path too — the close is the one caller
        # whose failure is permanent, so it must not be the one that forgets.
        assert seats_request["headers"].get("Authorization") == "Bearer test-service-token"

    async def test_an_injected_provider_wins_over_the_default(self, seats_say) -> None:
        """`token_provider=` is honoured — the two routers pass `app.state`'s one."""
        from settings import get_settings

        class _Injected:
            async def get_token(self) -> str:
                return "injected"

            def build_headers(self, tenant_id: str) -> dict[str, str]:
                return {"X-Tenant-Id": tenant_id, "Authorization": "Bearer injected"}

        seats_say(seats=1)
        assert (
            await seats_svc.get_seat_count(
                TID, get_settings(), token_provider=cast(ServiceTokenProvider, _Injected())
            )
            == 1
        )
        assert seats_request["headers"]["Authorization"] == "Bearer injected"


# --------------------------------------------------------------------------- #
# per_seat cadence — the RATE is multiplied, never billed flat
# --------------------------------------------------------------------------- #


@pytest.mark.unit
class TestPerSeatCadence:
    async def test_per_seat_row_applies_every_effective_month(self, db_session) -> None:
        await access_charge_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=TID,
                service_key="__seat__",  # pragma: allowlist secret
                amount_usd=Decimal("25"),
                cadence="per_seat",
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        )
        rows = await access_charge_svc.active_access_charges_for_month(
            db_session,
            tenant_id=TID,
            month_start=date(2026, 9, 1),
            month_end=date(2026, 10, 1),
            bill_ts=datetime(2026, 9, 30, 23, 59, tzinfo=UTC),
        )
        # Like `monthly`, unlike `one_time` — it bills in a month long after its
        # effective_from.
        assert [r.cadence for r in rows] == ["per_seat"]

    async def test_per_seat_grants_no_allowance_bucket(self, db_session) -> None:
        """A per-seat row must never appear in the allowance buckets: the wire
        refuses `included_cost_usd` on it, and the allowance builder only folds in
        `cadence == "monthly"`. Both halves, so neither can silently change."""
        await access_charge_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=TID,
                service_key="__seat__",  # pragma: allowlist secret
                amount_usd=Decimal("25"),
                cadence="per_seat",
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        )
        buckets = await access_charge_svc.active_allowances_all_tenants(
            db_session,
            month_start=date(2026, 9, 1),
            month_end=date(2026, 10, 1),
            bill_ts=datetime(2026, 9, 30, 23, 59, tzinfo=UTC),
        )
        assert buckets.get(TID, {}) == {}

    async def test_per_seat_is_excluded_from_the_flat_access_total(self, db_session) -> None:
        """⭐ `active_access_charges_all_tenants` reports flat fees for the
        platform overview. A per-seat RATE reported in there would show $25 as
        revenue regardless of seat count — the overview and the invoice would
        disagree.

        ⚠️ It returns the AMOUNTS, one entry per applying term, not a summed
        total: the caller quantizes each term before summing, because that is
        the fold the invoice charges on. So the flat term appears here as a
        one-element list and the per-seat term not at all.
        """
        for key, cadence, amt in (
            (None, "monthly", Decimal("5000")),
            ("__seat__", "per_seat", Decimal("25")),
        ):
            await access_charge_svc.upsert_access_charge(
                db_session,
                AccessChargeCreate(
                    tenant_id=TID,
                    service_key=key,
                    amount_usd=amt,
                    cadence=cadence,
                    effective_from=datetime(2026, 1, 1, tzinfo=UTC),
                ),
            )
        totals = await access_charge_svc.active_access_charges_all_tenants(
            db_session,
            month_start=date(2026, 9, 1),
            month_end=date(2026, 10, 1),
            bill_ts=datetime(2026, 9, 30, 23, 59, tzinfo=UTC),
        )
        assert totals[TID] == [Decimal("5000")], (
            "the flat term must be reported on its own; a per_seat RATE folded "
            "in here would be billed as revenue at any seat count"
        )


# --------------------------------------------------------------------------- #
# End-to-end: the seat fee on a generated bill, and the close-time refusal
# --------------------------------------------------------------------------- #


def _this_month() -> tuple[str, date]:
    today = datetime.now(UTC).date()
    return today.strftime("%Y-%m"), today


def _next_month_start(d: date) -> date:
    """The period's EXCLUSIVE end, matching BillingInvoice's half-open interval."""
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


async def _seed_cost_daily(session, *, tenant, license_id, cost, byo=False):
    from models.db import NULL_SUBJECT_ID, CostDaily

    _, day = _this_month()
    session.add(
        CostDaily(
            tenant_id=tenant,
            tenant_name="seat-test",
            license_id=license_id,
            subject_id=NULL_SUBJECT_ID,
            service_key="fr",  # pragma: allowlist secret
            provider="openai",
            served_model="gpt-5",
            byo=byo,
            cost_source="inference-router",
            bucket_date=day,
            total_input_tokens=1000,
            total_output_tokens=200,
            total_cost_usd=Decimal(cost),
            request_count=1,
        )
    )


@pytest.mark.unit
class TestSeatFeeOnTheBill:
    async def test_seat_rate_is_multiplied_by_the_count(
        self, db_session: AsyncSession, test_settings, seats_say
    ) -> None:
        """⭐ THE ARITHMETIC. 3 seats x $25 = $75, and it is NOT $25.

        ⚠️ MUTATION TARGET: drop the `* seat_count` in services/billing.py and
        this reads 25.00 — which is exactly what a `per_seat` cadence would have
        billed before the multiplier existed, because `_applies`' `return True`
        passes any unknown cadence through as a flat charge.
        """
        import uuid as _uuid

        from services.billing import generate_billing

        seats_say(seats=3)
        month, _ = _this_month()
        lic = _uuid.uuid4()
        await _seed_cost_daily(db_session, tenant=TID, license_id=lic, cost="10")
        await access_charge_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=TID,
                service_key="__seat__",  # pragma: allowlist secret
                amount_usd=Decimal("25"),
                cadence="per_seat",
            ),
        )
        await db_session.commit()

        totals = (await generate_billing(TID, month, db_session, test_settings))["totals"]
        assert Decimal(totals["seat_fee_usd"]) == Decimal("75.00")
        assert totals["seat_count"] == 3
        assert totals["seat_basis"] == "active_licences"
        # DISJOINT from the flat access axis — the two invoice columns must not
        # double-count. No flat charge was configured, so that axis is 0.00.
        assert Decimal(totals["access_charge_usd"]) == Decimal("0.00")
        # Seat revenue raises customer cost and margin by its full amount (usage
        # 10 * 1.20 = 12.00 at the new 20% default, + 75 seats = 87.00).
        assert Decimal(totals["customer_cost_usd"]) == Decimal("87.00")
        assert Decimal(totals["margin_usd"]) == Decimal("77.00")
        # The line itemises the rate AND the quantity — "$75" alone is not
        # reconcilable against a rate card.
        seat_line = next(c for c in totals["access_charges"] if c["cadence"] == "per_seat")
        assert Decimal(seat_line["seat_rate_usd"]) == Decimal("25")
        assert seat_line["seat_count"] == 3

    async def test_platform_fee_and_seat_fee_coexist_without_overlap(
        self, db_session: AsyncSession, test_settings, seats_say
    ) -> None:
        """$5,000 flat + 2 x $25 = $5,050, reported in two disjoint fields."""
        import uuid as _uuid

        from services.billing import generate_billing

        seats_say(seats=2)
        month, _ = _this_month()
        await _seed_cost_daily(db_session, tenant=TID, license_id=_uuid.uuid4(), cost="10")
        for key, cadence, amt in (
            (None, "monthly", Decimal("5000")),
            ("__seat__", "per_seat", Decimal("25")),
        ):
            await access_charge_svc.upsert_access_charge(
                db_session,
                AccessChargeCreate(tenant_id=TID, service_key=key, amount_usd=amt, cadence=cadence),
            )
        await db_session.commit()

        totals = (await generate_billing(TID, month, db_session, test_settings))["totals"]
        assert Decimal(totals["access_charge_usd"]) == Decimal("5000.00")
        assert Decimal(totals["seat_fee_usd"]) == Decimal("50.00")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("5062.00")  # 12 + 5000 + 50

    async def test_report_path_degrades_to_unavailable_never_to_zero_seats(
        self, db_session: AsyncSession, test_settings, seats_say
    ) -> None:
        """A licences outage on the REPORT path yields seat_basis='unavailable'
        and a null count — never a $0 seat fee presented as a fact."""
        import uuid as _uuid

        from services.billing import generate_billing

        seats_say(status_code=503)
        month, _ = _this_month()
        await _seed_cost_daily(db_session, tenant=TID, license_id=_uuid.uuid4(), cost="10")
        await access_charge_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=TID,
                service_key="__seat__",  # pragma: allowlist secret
                amount_usd=Decimal("25"),
                cadence="per_seat",
            ),
        )
        await db_session.commit()

        totals = (await generate_billing(TID, month, db_session, test_settings))["totals"]
        assert totals["seat_basis"] == "unavailable"
        assert totals["seat_count"] is None
        assert Decimal(totals["seat_fee_usd"]) == Decimal("0.00")
        # ⭐ And the $0 is NOT presentable as a fact: seat_basis says why. The
        # close path refuses outright — see TestCloseRefusesWithoutSeats.

    async def test_a_tenant_with_no_seat_term_reports_null_not_zero(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        """No per-seat row => seat_count is None, and licences is never called.

        `billing_invoices.seat_count` is nullable precisely so "no per-seat term"
        and "zero seats" stay distinguishable on a frozen statement.
        """
        import uuid as _uuid

        from services.billing import generate_billing

        month, _ = _this_month()
        await _seed_cost_daily(db_session, tenant=TID, license_id=_uuid.uuid4(), cost="10")
        await db_session.commit()
        totals = (await generate_billing(TID, month, db_session, test_settings))["totals"]
        assert totals["seat_count"] is None
        assert totals["seat_basis"] is None
        assert Decimal(totals["seat_fee_usd"]) == Decimal("0.00")


# --------------------------------------------------------------------------- #
# The configured tenant's CLOSED INVOICE ROW — the money actually written down
# --------------------------------------------------------------------------- #


async def _pad_month_for_bucket_gap(session, *, tenant, license_id, skip: date) -> None:
    """A $0.00 ``CostDaily`` row for every day of the CURRENT month except ``skip``.

    ``close_period`` runs ``assert_no_bucket_gap``, which refuses a period with
    even ONE fleet-wide absent ``bucket_date``. So a close of the current month
    has to cover all of it — including days still in the future — and there is no
    smaller fixture that satisfies the real gate. ``tests/test_invoices.py``
    solves the same problem with ``_seed_full_month``, but pins February 2026 to
    keep the padding small; this file cannot, because the access charges it seeds
    take ``effective_from=now`` and therefore only apply from the current month
    onward.

    ⭐ THE PADDING IS $0.00 AND ZERO REQUESTS ON PURPOSE: it makes the gate pass
    without moving a single figure the assertions below state, so the arithmetic
    stays the arithmetic of the ONE seeded usage row.
    """
    from models.db import NULL_SUBJECT_ID, CostDaily

    _, today = _this_month()
    d = today.replace(day=1)
    end = _next_month_start(today)
    while d < end:
        if d != skip:
            session.add(
                CostDaily(
                    tenant_id=tenant,
                    tenant_name="seat-test",
                    license_id=license_id,
                    subject_id=NULL_SUBJECT_ID,
                    service_key="fr",  # pragma: allowlist secret
                    provider="openai",
                    served_model="gpt-5",
                    byo=False,
                    cost_source="inference-router",
                    bucket_date=d,
                    total_input_tokens=0,
                    total_output_tokens=0,
                    total_cost_usd=Decimal("0"),
                    request_count=0,
                )
            )
        d += timedelta(days=1)


async def _seed_fully_configured_tenant(session) -> MarkupRule:
    """Every term George named, on ONE tenant, for the month ``close_period`` closes.

    $10.00 of usage + the $5,000/month flat platform fee + the $25 per-seat rate
    + a tenant-scoped 20% markup rule + the ``tenant_config`` row that makes the
    percentage an operator decision rather than a code constant.

    :returns: the ``MarkupRule``, so a caller can name it with
        ``services.markup.markup_source_for_rule`` instead of inventing a literal.
    """
    import uuid as _uuid

    from models.db import TenantConfig

    _, today = _this_month()
    lic = _uuid.uuid4()

    await _seed_cost_daily(session, tenant=TID, license_id=lic, cost="10")
    await _pad_month_for_bucket_gap(session, tenant=TID, license_id=lic, skip=today)

    # ⚠️ The flat fee owns ``service_key`` NULL and the seat rate a DISTINCT key,
    # or ix_accesscharge_open forces one to close the other — same constraint
    # TestMaterialiseTerms pins.
    for key, cadence, amt in (
        (None, "monthly", Decimal("5000")),
        ("__seat__", "per_seat", Decimal("25")),  # pragma: allowlist secret
    ):
        await access_charge_svc.upsert_access_charge(
            session,
            AccessChargeCreate(tenant_id=TID, service_key=key, amount_usd=amt, cadence=cadence),
        )

    # ⚠️ TENANT-PINNED, not all-wildcard. `services.markup.compute_billed_line`
    # only honours a resolved rule when `_specificity(rule) > 0`, so a rule with
    # every dimension NULL matches, scores 0, and loses to the fallback it was
    # meant to replace — it would never appear in `markup_sources_applied`.
    rule = MarkupRule(
        tenant_id=TID,
        markup_pct=Decimal("20"),
        effective_from=datetime(2025, 1, 1, tzinfo=UTC),
        effective_until=None,
    )
    session.add(rule)
    # ⚠️ AND a tenant_config row at the same 20. `BillingInvoice.markup_pct` /
    # `.markup_source` are wired to `get_markup_pct_with_source`
    # (services/billing.py:828-835), whose only three answers are
    # `tenant_config`, `tenant_config_default` and `settings_default` — the
    # COLUMN can never read `markup_rule:<id>`. Without this row the frozen
    # provenance would be the code constant, which is exactly what the test
    # below has to be able to fail on.
    session.add(
        TenantConfig(
            tenant_id=TID,
            tenant_name="seat-test",
            markup_pct=Decimal("20"),
            billing_mode="standard_markup",
        )
    )
    await session.flush()
    return rule


@pytest.mark.unit
class TestConfiguredTenantInvoiceRow:
    """⭐ THE POSITIVE CASE ON THE PERSISTED ROW — every figure NON-ZERO.

    WHAT THIS EXISTS TO REFUTE, BY NAME.
    ``tests/test_invoices.py::test_zero_platform_fee_and_markup_provenance_reach_the_response``
    (the "always present lines" test, ~line 674) closes a period for a tenant
    with ZERO terms and asserts ``access_charge_usd == Decimal("0.00")`` and
    ``platform_fee_usd == Decimal("0.00")``. Its own docstring says asserting the
    RESPONSE rather than the row is the point — and it is right about that — but
    an ALL-ZERO assertion cannot distinguish "correctly projected" from
    "hardcoded zero". It did not: ``seat_fee_usd`` was a literal ``Decimal("0")``
    with ``seat_count=None`` at the invoice write site until PR #176, and that
    test passed the whole time. Every money column but ``subtotal_usd`` /
    ``total_usd`` also carries ``server_default "0"``, so a DROPPED key in the
    projection is green there too.

    So every assertion in this class is NON-ZERO and non-default by
    construction. A hardcoded ``0``, a ``None`` ``seat_count``, or a
    settings-default provenance each make it red.

    ⚠️ ONE HONEST LIMIT, STATED. ``settings.default_markup_pct`` is ALSO 20.0, so
    ``markup_pct == Decimal("20.00")`` alone cannot refute a settings fallback —
    it would hold either way. ``markup_source == MARKUP_SOURCE_TENANT_CONFIG`` is
    the assertion that does, and the ``markup_rule:<id>`` membership check is
    what proves the commercial RULE, not the fallback, billed the usage.
    """

    async def test_closed_invoice_row_carries_the_configured_money(
        self, db_session: AsyncSession, db_engine, test_settings, seats_say
    ) -> None:
        """⛔ THE WRITE-SIDE PROOF. ``billing_invoices`` has 0 rows in production,
        so the projection from ``generate_billing``'s ``totals`` dict into the
        row's columns (``services/invoices.py:1201-1239``) has never once been
        exercised with a configured tenant's money in it.

        THE ARITHMETIC, from the seeded numbers:
          usage      $10.00 x 1.20 (the 20% rule)      = $ 12.00
          platform   $5,000 flat, usage billed on top  = $5000.00
          seats      3 seats x $25                     = $  75.00
                                                         ---------
          total_usd                                      $5087.00
          margin     5087.00 - 10.00 incurred          = $5077.00
        """
        from models.db import BillingInvoice
        from services.byo_mode import clear_byo_cache
        from services.invoices import close_period
        from services.markup import MARKUP_SOURCE_TENANT_CONFIG, markup_source_for_rule

        seats_say(seats=3)
        # ⚠️ `byo_mode` is module state with a 5-min TTL, exactly like the seat
        # cache — and `_seat_cache_isolated` above covers only seats.
        # `close_period`'s `assert_byo_mode_available` must reach the stub
        # installed one line up, not a neighbour test's cached answer. A cached
        # `local_allowed`/`platform_only` would collapse this tenant to
        # BILLING_MODE_INTERNAL and bill $0 — every assertion below would read 0
        # and name the wrong cause.
        clear_byo_cache()

        rule = await _seed_fully_configured_tenant(db_session)
        await db_session.commit()

        _, today = _this_month()
        inv = await close_period(
            db_session,
            tenant_id=TID,
            period_start=today.replace(day=1),
            period_end=_next_month_start(today),
            settings=test_settings,
        )
        assert inv.status == "issued", "the close did not reach issue()"
        await db_session.commit()

        # ⭐ ASSERTED ON THE RE-READ ROW, not on the returned object, and via a
        # SEPARATE SESSION (`_rows`) rather than a re-SELECT on `db_session`: the
        # identity map would hand the same Python instance straight back, so a
        # "re-read" on this session would grade the object close_period built in
        # memory and could not see a value the DB refused, defaulted, or
        # truncated. A fresh session has an empty identity map, so every figure
        # below came out of Postgres.
        rows = await _rows(db_engine, BillingInvoice)
        assert len(rows) == 1, f"expected exactly one invoice, got {len(rows)}"
        row = rows[0]

        # 1. The $5,000/month flat fee reached its OWN column.
        assert row.access_charge_usd == Decimal("5000.00")
        # 2. The seat RATE was multiplied AND the quantity was written down.
        #    ⚠️ seat_count is nullable on purpose, so `is not None` is part of
        #    the assertion: `None` is the pre-#176 value, not a small number.
        assert row.seat_count == 3
        assert row.seat_fee_usd == Decimal("75.00")  # 3 x $25
        # 3. The percentage and ITS PROVENANCE. markup_pct is Numeric(6, 2).
        assert row.markup_pct == Decimal("20.00")
        assert row.markup_source == MARKUP_SOURCE_TENANT_CONFIG, (
            f"frozen provenance is {row.markup_source!r}; a configured tenant's "
            "statement must not record a code constant as the source of its rate"
        )
        # effective_markup_pct is Numeric(9, 4) — the blended rate on usage,
        # 12.00 billed over 10.00 incurred.
        assert row.effective_markup_pct == Decimal("20.0000")
        # 4. ⭐ And the commercial RULE is what actually billed the usage line.
        #    The column cannot carry `markup_rule:<id>` (see
        #    _seed_fully_configured_tenant), so the rule is named from the frozen
        #    snapshot's per-line provenance set, via the repo's own helper rather
        #    than a literal.
        applied = row.snapshot["totals"]["markup_sources_applied"]
        assert markup_source_for_rule(rule) in applied, (
            f"{applied} does not name the rule: the tenant-scoped markup rule "
            "billed nothing; the usage line fell "
            "back to tenant_config/settings and the 20% in markup_pct is a "
            "coincidence, not the rule being applied"
        )
        # 5. The total, and the two figures it is built from.
        assert row.internal_cost_usd == Decimal("10.00")
        assert row.total_usd == Decimal("5087.00")  # 12.00 + 5000.00 + 75.00
        assert row.subtotal_usd == Decimal("5087.00")  # no tax: subtotal == total
        assert row.margin_usd == Decimal("5077.00")  # 5087.00 - 10.00

    async def test_row_projection_agrees_with_the_report_for_the_same_period(
        self, db_session: AsyncSession, db_engine, test_settings, seats_say
    ) -> None:
        """The frozen ROW's money equals ``generate_billing``'s totals.

        ⚠️ NOT AN INDEPENDENT CROSS-CHECK, AND IT MUST NOT BE READ AS ONE.
        ``close_period`` CALLS ``generate_billing`` and freezes its output, so
        agreement on the totals is STRUCTURAL — the two cannot disagree about a
        number. What this test actually pins is the step in between: the
        hand-written ``totals`` dict -> column projection at
        ``services/invoices.py:1201-1239``, which is the one place a field can be
        dropped or cross-wired silently, because every money column but
        ``subtotal_usd``/``total_usd`` carries ``server_default "0"``.

        The `> 0` floors are load-bearing for the same reason the class exists:
        two zeros agree perfectly.
        """
        from models.db import BillingInvoice
        from services.billing import generate_billing
        from services.byo_mode import clear_byo_cache
        from services.invoices import close_period

        seats_say(seats=3)
        clear_byo_cache()

        await _seed_fully_configured_tenant(db_session)
        await db_session.commit()

        month, today = _this_month()
        await close_period(
            db_session,
            tenant_id=TID,
            period_start=today.replace(day=1),
            period_end=_next_month_start(today),
            settings=test_settings,
        )
        await db_session.commit()

        row = (await _rows(db_engine, BillingInvoice))[0]
        totals = (await generate_billing(TID, month, db_session, test_settings))["totals"]

        # The report is not all zeros — otherwise the agreement below is vacuous.
        assert Decimal(totals["access_charge_usd"]) > 0
        assert Decimal(totals["seat_fee_usd"]) > 0
        assert Decimal(totals["customer_cost_usd"]) > 0
        assert totals["seat_count"] == 3

        assert row.access_charge_usd == Decimal(totals["access_charge_usd"])
        assert row.seat_fee_usd == Decimal(totals["seat_fee_usd"])
        assert row.seat_count == totals["seat_count"]
        # ⚠️ `customer_cost_usd` is NOT a column — close_period copies it into
        # BOTH subtotal_usd and total_usd (invoices.py:1201-1202). Asserting both
        # is what catches a cross-wire that leaves one of them at a stale value.
        assert row.total_usd == Decimal(totals["customer_cost_usd"])
        assert row.subtotal_usd == Decimal(totals["customer_cost_usd"])


@pytest.mark.unit
class TestCloseRefusesWithoutSeats:
    async def test_close_refuses_when_licences_cannot_state_the_count(
        self, db_session: AsyncSession, test_settings, seats_say
    ) -> None:
        """⛔ THE ONE THAT PROTECTS MONEY.

        A frozen invoice with a fabricated 0 seats is an under-bill nobody can
        detect later — there is no "recount seats on closed invoices" pass. So
        close_period refuses, like assert_no_bucket_gap does for a rollup gap.

        ⚠️ MUTATION TARGET: make `assert_seat_count_available` swallow
        SeatCountUnavailableError and this test stops raising AND an invoice row
        appears — both assertions below fail.
        """
        from models.db import BillingInvoice
        from services.invoices import assert_seat_count_available

        _, today = _this_month()
        await access_charge_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=TID,
                service_key="__seat__",  # pragma: allowlist secret
                amount_usd=Decimal("25"),
                cadence="per_seat",
            ),
        )
        await db_session.commit()
        seats_say(status_code=503)

        with pytest.raises(seats_svc.SeatCountUnavailableError):
            await assert_seat_count_available(
                db_session,
                tenant_id=TID,
                settings=test_settings,
                period_start=today.replace(day=1),
                period_end=_next_month_start(today),
            )
        # No invoice was written.
        rows = (await db_session.execute(select(BillingInvoice))).scalars().all()
        assert list(rows) == []

    async def test_close_gate_is_inert_for_a_tenant_with_no_seat_term(
        self, db_session: AsyncSession, test_settings, monkeypatch
    ) -> None:
        """⚠️ THE SCOPE OF THE REFUSAL, and it is the half that keeps a narrow
        dependency from becoming an estate-wide one.

        A licences outage must NOT block closing the invoices of tenants billed
        on usage alone. Proven by a licences stub that would RAISE if called:
        the gate must not reach it.
        """
        from services.invoices import assert_seat_count_available

        _, today = _this_month()

        async def _must_not_be_called(self, url, *a, **kw):  # noqa: ANN001
            if "seat-count" in str(url):
                raise AssertionError("seat-count must not be consulted for a seatless tenant")
            if "byo-mode" in str(url):
                return httpx.Response(200, json={"byo_mode": "byo_required"})
            return httpx.Response(200, json=[])

        monkeypatch.setattr(httpx.AsyncClient, "get", _must_not_be_called)
        # No per-seat row for this tenant => returns cleanly, no lookup.
        await assert_seat_count_available(
            db_session,
            tenant_id=TID,
            settings=test_settings,
            period_start=today.replace(day=1),
            period_end=_next_month_start(today),
        )


# --------------------------------------------------------------------------- #
# PUT /api/config/tenants/{id}/commercial-terms — the whole rate card, atomically
#
# ⛔ THE DEFECT: configuring a billed tenant took FOUR requests in FOUR
# transactions (`PUT /tenants/{id}` for billing_mode, `POST /access-charges`
# twice, `POST /markup-rules` once). `licenses`' `provision_tenant_offering` saga
# drives them and can die between any two; its compensation rolls back only its
# OWN local posture, and billing exposes no DELETE for a markup rule or an access
# charge. So a half-applied saga leaves a tenant priced wrong PERMANENTLY, and in
# the direction nobody notices: a tenant with fees and no markup bills at
# `settings.default_markup_pct` with `markup_source=settings_default`, which is
# the exact signature of a tenant nobody has ever priced.
#
# These tests pin the four properties that make the endpoint worth having:
#   1. ATOMICITY — an induced failure mid-write leaves NO charge and NO rule;
#   2. VALUE-IDEMPOTENCE — a replay writes nothing and re-dates nothing;
#   3. the read-back reflects what was written, with provenance; and
#   4. the seat term takes the per-app `seat:<app>` key and RETIRES the legacy
#      `__seat__` lineage, because two open per_seat rows are two seat fees.
# --------------------------------------------------------------------------- #

CP_TERMS = {
    "billing_mode": "byo_markup_only",
    "platform_fee_usd": "5000",
    "seat_app_key": "commonpractice",
    "seat_fee_usd": "25",
    "markup_service_key": "commonpractice",  # pragma: allowlist secret
    "byo_markup_pct": "20",
}

CP_SEAT_KEY = "seat:commonpractice"


async def _tenant_config(db_engine):
    from models.db import TenantConfig

    rows = await _rows(db_engine, TenantConfig)
    return next((r for r in rows if r.tenant_id == TID), None)


@pytest.mark.unit
class TestCommercialTermsPut:
    async def test_one_call_sets_all_four_terms(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        """The whole point: one request, one transaction, four terms.

        ⚠️ MUTATION TARGET: delete any one of the four writer blocks in
        `put_commercial_terms` — the corresponding assertion below fails and
        `fully_configured` goes False.
        """
        seats_say(seats=4)
        resp = app_client.put(
            f"/api/config/tenants/{TID}/commercial-terms",
            headers={"Authorization": "Bearer fake"},
            json=CP_TERMS,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]

        assert data["billing_mode"] == "byo_markup_only"
        assert data["billing_mode_source"] == "tenant_config"
        assert Decimal(data["platform_fee"]["value"]) == Decimal("5000")
        assert Decimal(data["seat_fee"]["value"]) == Decimal("25")
        assert Decimal(data["byo_markup"]["value"]) == Decimal("20")
        assert data["seat_service_key"] == CP_SEAT_KEY
        assert data["markup_service_key"] == "commonpractice"  # pragma: allowlist secret
        assert data["fully_configured"] is True
        assert set(data["changed"]) == {
            "billing_mode",
            "platform_fee",
            "seat_fee",
            "byo_markup",
        }

        # ...and the rows really are the shapes billing reads.
        charges = {c.service_key: c for c in await _rows(db_engine, TenantAccessCharge)}
        assert charges[None].cadence == "monthly"
        # ⛔ NULL allowance: a non-null `included_cost_usd` here would make the
        # first N dollars of usage free, which is a different product.
        assert charges[None].included_cost_usd is None
        assert charges[CP_SEAT_KEY].cadence == "per_seat"

        rules = await _rows(db_engine, MarkupRule)
        assert len(rules) == 1
        assert (rules[0].byo, rules[0].service_key) == (True, "commonpractice")
        assert (await _tenant_config(db_engine)).billing_mode == "byo_markup_only"

    async def test_an_induced_failure_mid_write_leaves_no_charge_and_no_rule(
        self, app_client: TestClient, db_engine, admin_actor, seats_say, monkeypatch
    ) -> None:
        """⭐ THE PROPERTY THE ENDPOINT EXISTS FOR.

        The markup rule is written LAST, so blowing it up is the half-apply the
        saga produced: fees written, markup missing, no DELETE to undo it. With
        one transaction nothing survives — not the access charges, not the
        tenant_config row, not even the audit rows, because `services.audit.record`
        flushes and never commits.

        ⚠️ MUTATION TARGET: move `await session.commit()` in
        `put_commercial_terms` to just after the two `_ensure_charge` calls. The
        platform fee and the seat rate then survive the failure and this test
        finds 2 charges instead of 0 — which is precisely the production state
        being closed.
        """
        seats_say(seats=2)

        async def _boom(*_a, **_kw):
            raise RuntimeError("induced failure between the charges and the markup rule")

        monkeypatch.setattr(markup_svc, "upsert_markup_rule", _boom)

        with pytest.raises(RuntimeError, match="induced failure"):
            app_client.put(
                f"/api/config/tenants/{TID}/commercial-terms",
                headers={"Authorization": "Bearer fake"},
                json=CP_TERMS,
            )

        assert await _rows(db_engine, TenantAccessCharge) == []
        assert await _rows(db_engine, MarkupRule) == []
        assert await _tenant_config(db_engine) is None
        assert await _audit(db_engine, "config.commercial_terms.") == []

    async def test_a_replay_with_the_same_body_redates_nothing(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        """⭐ VALUE-IDEMPOTENCE, AND WHY IT IS NOT OPTIONAL FOR A PUT.

        `upsert_access_charge` and `upsert_markup_rule` CLOSE the prior open row
        and open a new one — that is what makes the history auditable, and it is
        also what makes "a PUT always writes" catastrophic: every provisioning
        retry ends the current period and starts a fresh one, so a live tenant
        appears re-priced on whatever schedule the saga happens to retry on and
        the effective-dated history is shredded into noise.

        ⚠️ MUTATION TARGET: delete the early `return` in `_ensure_charge` (or the
        `prior_rule.markup_pct != body.byo_markup_pct` guard). Row counts double,
        every row but the newest carries an `effective_until`, and `changed` stops
        being empty.
        """
        seats_say(seats=2)
        h = {"Authorization": "Bearer fake"}
        url = f"/api/config/tenants/{TID}/commercial-terms"

        first = app_client.put(url, headers=h, json=CP_TERMS)
        assert first.status_code == 200, first.text
        charges_1 = await _rows(db_engine, TenantAccessCharge)
        rules_1 = await _rows(db_engine, MarkupRule)

        second = app_client.put(url, headers=h, json=CP_TERMS)
        assert second.status_code == 200, second.text
        # `changed: []` is the OBSERVABLE difference between "the terms already
        # said this" and "I re-dated four periods". Without it a caller cannot
        # tell a harmless retry from a destructive one.
        assert second.json()["data"]["changed"] == []

        charges_2 = await _rows(db_engine, TenantAccessCharge)
        rules_2 = await _rows(db_engine, MarkupRule)
        assert len(charges_2) == len(charges_1) == 2
        assert len(rules_2) == len(rules_1) == 1
        assert {c.id for c in charges_2} == {c.id for c in charges_1}
        assert {r.id for r in rules_2} == {r.id for r in rules_1}
        assert all(c.effective_until is None for c in charges_2)
        assert all(r.effective_until is None for r in rules_2)
        # The replay changed nothing, so it recorded nothing.
        assert len(await _audit(db_engine, "config.commercial_terms.")) == 4

    async def test_a_changed_value_redates_only_that_term(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        """A re-price is deliberate and auditable: the prior row CLOSES rather
        than being edited, and the terms that did not change are left alone.

        ⚠️ MUTATION TARGET: drop the `service_key` scoping from
        `_ensure_charge`'s `prior_open` lookup — the seat term then matches the
        platform fee's row and both get re-dated on every call.
        """
        seats_say(seats=2)
        h = {"Authorization": "Bearer fake"}
        url = f"/api/config/tenants/{TID}/commercial-terms"
        app_client.put(url, headers=h, json=CP_TERMS)
        seat_id_before = next(
            c.id for c in await _rows(db_engine, TenantAccessCharge) if c.service_key
        )

        resp = app_client.put(url, headers=h, json={**CP_TERMS, "platform_fee_usd": "7500"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["data"]["changed"] == ["platform_fee"]
        assert Decimal(resp.json()["data"]["platform_fee"]["value"]) == Decimal("7500")

        charges = await _rows(db_engine, TenantAccessCharge)
        flat = [c for c in charges if c.service_key is None]
        assert len(flat) == 2  # the closed prior row plus the new open one
        assert sorted(c.effective_until is None for c in flat) == [False, True]
        # The seat term was untouched — same row, still open.
        seat_rows = [c for c in charges if c.service_key == CP_SEAT_KEY]
        assert len(seat_rows) == 1
        assert seat_rows[0].id == seat_id_before
        assert seat_rows[0].effective_until is None

    async def test_the_seat_term_takes_the_per_app_key(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        """⭐ `seat:<app>`, NOT `__seat__`, and it is a billing behaviour rather
        than a naming preference: `services.seats.seat_scope_for_charge`
        recognises ONLY this prefix and turns it into a PERIOD-SCOPED, PER-APP
        seat count. An unprefixed key falls through to the all-apps
        point-in-time count, so a tenant licensed for three products is billed
        CommonPractice's seat rate over all three products' seats.

        ⚠️ MUTATION TARGET: make `_seat_service_key` return `SEAT_SERVICE_KEY`.
        """
        from services.seats import seat_scope_for_charge

        seats_say(seats=2)
        app_client.put(
            f"/api/config/tenants/{TID}/commercial-terms",
            headers={"Authorization": "Bearer fake"},
            json=CP_TERMS,
        )
        seat_row = next(c for c in await _rows(db_engine, TenantAccessCharge) if c.service_key)
        assert seat_row.service_key == CP_SEAT_KEY
        # The key is one licences can actually scope a count with.
        scope = seat_scope_for_charge(seat_row.service_key, date(2026, 9, 1), date(2026, 10, 1))
        assert scope["app_key"] == "commonpractice"

    async def test_a_legacy_seat_term_is_retired_so_seats_are_not_billed_twice(
        self, app_client: TestClient, db_engine, db_session, admin_actor, seats_say
    ) -> None:
        """⛔ THE ONE THAT COSTS THE CUSTOMER MONEY IF IT REGRESSES.

        `generate_billing` discriminates a seat term by `cadence == "per_seat"`
        ALONE and ADDS every one it finds into `seat_fee_total`, while
        `ix_accesscharge_open` is unique per `(tenant, service_key)` — so a legacy
        `__seat__` row and a `seat:<app>` row are two DIFFERENT open slots and the
        tenant is billed TWO seat fees on one invoice, on lines that each look
        correct. A tenant previously put on the standard card by
        `POST /tenants/{id}/terms` is exactly the tenant this endpoint is called
        on next, so this is the common path.

        ⚠️ MUTATION TARGET: delete the `retire any OTHER open per-seat lineage`
        loop — two open per_seat rows survive and `extra_seat_service_keys` is
        non-empty.
        """
        await access_charge_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=TID,
                service_key="__seat__",  # pragma: allowlist secret - a service_key
                amount_usd=Decimal("25"),
                cadence="per_seat",
            ),
        )
        await db_session.commit()

        seats_say(seats=2)
        resp = app_client.put(
            f"/api/config/tenants/{TID}/commercial-terms",
            headers={"Authorization": "Bearer fake"},
            json=CP_TERMS,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["seat_service_key"] == CP_SEAT_KEY
        # Exactly ONE open per-seat lineage remains.
        assert data["extra_seat_service_keys"] == []
        assert "retired_seat_term:__seat__" in data["changed"]

        open_seats = [
            c
            for c in await _rows(db_engine, TenantAccessCharge)
            if c.cadence == "per_seat" and c.effective_until is None
        ]
        assert [c.service_key for c in open_seats] == [CP_SEAT_KEY]

    async def test_a_prefixed_seat_app_key_is_refused_at_the_wire(
        self, app_client: TestClient, admin_actor
    ) -> None:
        """`seat_app_key="seat:cp"` would store `seat:seat:cp`, and
        `seat_scope_for_charge` strips exactly ONE prefix — so licences is asked
        for an app called `seat:cp`, answers 422, and every close for that tenant
        REFUSES. The row looks well-formed in every listing until then.

        ⚠️ MUTATION TARGET: delete
        `CommercialTermsPut._seat_app_key_is_not_already_prefixed`.
        """
        resp = app_client.put(
            f"/api/config/tenants/{TID}/commercial-terms",
            headers={"Authorization": "Bearer fake"},
            json={**CP_TERMS, "seat_app_key": "seat:commonpractice"},
        )
        assert resp.status_code == 422, resp.text

    async def test_the_scoped_markup_rule_does_not_close_the_tenant_wide_rule(
        self, app_client: TestClient, db_engine, admin_actor, seats_say
    ) -> None:
        """Pricing ONE product must not retire the tenant's baseline markup.

        `service_key` and `byo` are both terms of `ix_markuprules_open`, so the
        two rules are different tuples and `upsert_markup_rule` leaves the other
        open. Stated as a test because the failure mode is SILENT: the upsert
        would return the new rule with a 200 and an audit trail recording it as
        an unrelated rule's creation, while every non-CommonPractice line
        silently re-priced.

        ⚠️ MUTATION TARGET: remove `service_key` from `upsert_markup_rule`'s
        open-row loop in `services/markup.py`.
        """
        seats_say(seats=2)
        h = {"Authorization": "Bearer fake"}
        # A tenant-wide rate card first (this is the standard-card endpoint).
        app_client.post(f"/api/config/tenants/{TID}/terms", headers=h, json={})
        app_client.put(f"/api/config/tenants/{TID}/commercial-terms", headers=h, json=CP_TERMS)

        open_rules = [r for r in await _rows(db_engine, MarkupRule) if r.effective_until is None]
        by_key = {(r.service_key, r.byo) for r in open_rules}
        assert (None, None) in by_key  # the tenant-wide baseline survives
        assert (None, True) in by_key  # ...and its BYO sibling
        assert ("commonpractice", True) in by_key  # ...beside the scoped one


@pytest.mark.unit
class TestCommercialTermsRead:
    async def test_get_reflects_what_the_put_wrote(
        self, app_client: TestClient, admin_actor, seats_say
    ) -> None:
        """A GET on the same path answers with the same shape, and `changed` is
        always `[]` there — it reports what a PUT did, and a GET does nothing.

        ⚠️ MUTATION TARGET: have `get_commercial_terms` pass `changed=` through
        from anywhere; or point `_read_commercial_terms` at the `__seat__` key.
        """
        seats_say(seats=7)
        h = {"Authorization": "Bearer fake"}
        url = f"/api/config/tenants/{TID}/commercial-terms"
        written = app_client.put(url, headers=h, json=CP_TERMS).json()["data"]
        read = app_client.get(url, headers=h).json()["data"]

        assert read["changed"] == []
        for field in (
            "billing_mode",
            "billing_mode_source",
            "seat_service_key",
            "markup_service_key",
            "fully_configured",
        ):
            assert read[field] == written[field], field
        assert Decimal(read["platform_fee"]["value"]) == Decimal("5000")
        assert Decimal(read["byo_markup"]["value"]) == Decimal("20")
        assert read["byo_markup"]["source"].startswith("markup_rule:")
        assert read["platform_fee"]["source"] == "tenant_access_charge"

    async def test_an_unpriced_tenant_says_its_values_are_defaults(
        self, app_client: TestClient, admin_actor
    ) -> None:
        """⭐ The distinction every term here carries. A tenant with no rows still
        BILLS — `standard_markup` from the column's server default, 20% from
        `settings.default_byo_markup_pct` — so showing only numbers makes it
        indistinguishable from a priced tenant. The platform fee, which genuinely
        does not exist, reports `None` and NOT 0: "no fee configured" and "a $0.00
        fee" are different commercial facts.

        ⚠️ MUTATION TARGET: return `Decimal("0")` instead of `None` from
        `_charge_term`'s no-row arm, or drop `billing_mode_source`.
        """
        data = app_client.get(
            f"/api/config/tenants/{TID}/commercial-terms",
            headers={"Authorization": "Bearer fake"},
        ).json()["data"]
        assert data["fully_configured"] is False
        assert data["billing_mode"] == "standard_markup"
        assert data["billing_mode_source"] == "settings_default"
        assert data["platform_fee"]["value"] is None
        assert data["platform_fee"]["source"] == "no_matching_access_charge"
        assert data["seat_fee"]["value"] is None
        assert Decimal(data["byo_markup"]["value"]) == Decimal("20.0")
        assert data["byo_markup"]["source"] == "settings_default"

    async def test_a_double_billed_seat_state_is_reported_not_hidden(
        self, app_client: TestClient, db_session, admin_actor
    ) -> None:
        """⛔ Two open `per_seat` lineages means the tenant IS billed twice, and a
        read-back that quietly picked the "best" one would make that state
        invisible to the only page an operator looks at.

        ⚠️ MUTATION TARGET: return only `seats[0]` from `_pick_seat_row` and drop
        `extra_seat_service_keys`.
        """
        for key in ("__seat__", CP_SEAT_KEY):  # pragma: allowlist secret - service_keys
            await access_charge_svc.upsert_access_charge(
                db_session,
                AccessChargeCreate(
                    tenant_id=TID, service_key=key, amount_usd=Decimal("25"), cadence="per_seat"
                ),
            )
        await db_session.commit()

        data = app_client.get(
            f"/api/config/tenants/{TID}/commercial-terms",
            headers={"Authorization": "Bearer fake"},
        ).json()["data"]
        # The `seat:`-prefixed lineage is preferred, and the other is DISCLOSED.
        assert data["seat_service_key"] == CP_SEAT_KEY
        assert data["extra_seat_service_keys"] == ["__seat__"]


# --------------------------------------------------------------------------- #
# tenant_name on the FIRST tenant_config row (prod 503, 2026-10-13)
#
# ⛔ THE DEFECT: the first `tenant_config` row for a brand-new tenant omitted
# `tenant_name`; the chain-built column had no default, so the INSERT was a
# NotNullViolation -> 503. This suite's schema comes from `create_all` (the
# model HAS the default), so it cannot see the NULL — that half is pinned
# against the real chain in tests/test_migration_0056_tenant_config_name_default.py.
# What it CAN pin is that the writers name the column, with the registry's name.
# --------------------------------------------------------------------------- #


@pytest.fixture
def registry_names():
    import tenancy

    tenancy.reset_live_tenant_registry()
    yield tenancy.set_live_tenant_registry
    tenancy.reset_live_tenant_registry()


@pytest.mark.unit
class TestFirstTenantConfigRowIsNamed:
    async def test_commercial_terms_stores_the_registry_name(
        self, app_client: TestClient, db_engine, admin_actor, seats_say, registry_names
    ) -> None:
        registry_names({"Ahtapol Holding": TID})
        seats_say(seats=1)
        resp = app_client.put(
            f"/api/config/tenants/{TID}/commercial-terms",
            headers={"Authorization": "Bearer fake"},
            json=CP_TERMS,
        )
        assert resp.status_code == 200, resp.text
        assert (await _tenant_config(db_engine)).tenant_name == "Ahtapol Holding"

    async def test_commercial_terms_for_an_unnamed_tenant_stores_empty_not_uuid(
        self, app_client: TestClient, db_engine, admin_actor, seats_say, registry_names
    ) -> None:
        seats_say(seats=1)
        resp = app_client.put(
            f"/api/config/tenants/{TID}/commercial-terms",
            headers={"Authorization": "Bearer fake"},
            json=CP_TERMS,
        )
        assert resp.status_code == 200, resp.text
        assert (await _tenant_config(db_engine)).tenant_name == ""

    async def test_tenant_config_put_stores_the_registry_name(
        self, app_client: TestClient, db_engine, admin_actor, registry_names
    ) -> None:
        registry_names({"Ahtapol Holding": TID})
        resp = app_client.put(
            f"/api/config/tenants/{TID}",
            headers={"Authorization": "Bearer fake"},
            json={"markup_pct": 25},
        )
        assert resp.status_code == 200, resp.text
        assert (await _tenant_config(db_engine)).tenant_name == "Ahtapol Holding"
