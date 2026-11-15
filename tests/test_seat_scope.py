"""Per-seat terms pinned to a product: that app's seats over the billed PERIOD.

⭐ CommonPractice bills "distinct employees who held an active licence at any point in the period".
A per-seat term on a ``seat:commonpractice`` key (explicit opt-in) asks licences for that app's seats over
``[month_start, month_end)``; the reserved tenant-wide ``__seat__`` term (every existing one) keeps
the all-apps, point-in-time count unchanged.

⛔ FAIL CLOSED ON AN UNSCOPED ANSWER. A licences build without the scope parameters ignores them
and answers the old count with a 200. The response's echoed ``app_key`` / ``basis`` must confirm the
scope, or the count is refused — a close must never freeze the answer to a different question.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast

import httpx
import pytest

import services.seats as seats_svc
from routers.config import SEAT_SERVICE_KEY
from service_auth import ServiceTokenProvider
from services.seats import SeatCountUnavailableError, seat_scope_for_charge

TID = "0195f48b-d000-7000-8000-0000000c5ea7"


class _Token:
    async def get_token(self) -> str:
        return "t"  # pragma: allowlist secret

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        return {"X-Tenant-Id": tenant_id, "Authorization": "Bearer t"}  # pragma: allowlist secret


def _tok() -> ServiceTokenProvider:
    return cast(ServiceTokenProvider, _Token())


@pytest.fixture(autouse=True)
def _isolated_cache():
    seats_svc.clear_seat_cache()
    yield
    seats_svc.clear_seat_cache()


def _licences(monkeypatch, *, answer: dict, status: int = 200) -> list[dict]:
    seen: list[dict] = []

    async def _get(self, url, *a, **kw):  # noqa: ANN001
        seen.append({"url": str(url), "params": dict(kw.get("params") or {})})
        return httpx.Response(status, json=answer)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return seen


@pytest.mark.parametrize(
    "key", [None, "", SEAT_SERVICE_KEY, "__seat_two__", "commonpractice", "seat:"]
)
def test_unpinned_terms_keep_the_legacy_count(key) -> None:
    assert seat_scope_for_charge(key, date(2026, 8, 1), date(2026, 9, 1)) == {}


def test_a_product_pinned_term_scopes_to_that_app_and_the_period() -> None:
    assert seat_scope_for_charge("seat:commonpractice", date(2026, 8, 1), date(2026, 9, 1)) == {
        "app_key": "commonpractice",
        "period_start": datetime(2026, 8, 1, tzinfo=UTC),
        "period_end": datetime(2026, 9, 1, tzinfo=UTC),
    }


async def test_a_scoped_request_sends_app_and_period_and_no_as_of(monkeypatch) -> None:
    from settings import get_settings

    seen = _licences(
        monkeypatch,
        answer={
            "seats": 4,
            "human_subjects": 4,
            "app_key": "commonpractice",
            "basis": "active_during_period",
        },
    )
    scope = seat_scope_for_charge("seat:commonpractice", date(2026, 8, 1), date(2026, 9, 1))
    n = await seats_svc.get_seat_count(
        TID,
        get_settings(),
        token_provider=_tok(),
        as_of=datetime(2026, 8, 31, tzinfo=UTC),
        **scope,
    )
    assert n == 4
    params = seen[0]["params"]
    assert params["app"] == "commonpractice"
    assert params["period_start"].startswith("2026-08-01")
    assert params["period_end"].startswith("2026-09-01")
    # ⚠️ No as_of: a period count must not be silently narrowed to an instant.
    assert "as_of" not in params


@pytest.mark.parametrize(
    "answer",
    [
        # A licences build that IGNORED the scope: legacy basis, no app echo.
        {"seats": 9, "basis": "active_licences"},
        # Scoped to the wrong app.
        {"seats": 9, "app_key": "other", "basis": "active_during_period"},
        # Right app, but counted at an instant.
        {"seats": 9, "app_key": "commonpractice", "basis": "active_licences"},
    ],
)
async def test_an_answer_that_does_not_confirm_the_scope_is_refused(monkeypatch, answer) -> None:
    from settings import get_settings

    _licences(monkeypatch, answer=answer)
    scope = seat_scope_for_charge("seat:commonpractice", date(2026, 8, 1), date(2026, 9, 1))
    with pytest.raises(SeatCountUnavailableError):
        await seats_svc.get_seat_count(TID, get_settings(), token_provider=_tok(), **scope)


async def test_a_period_term_bills_people_not_licences(monkeypatch) -> None:
    """One employee revoked and re-licensed in the month holds two licence rows plus one
    service-realm licence: 3 ``seats``, 1 person. The bill is 1.

    ⚠️ MUTATION TARGET: read ``seats`` instead of ``human_subjects`` in services/seats.py -> 3."""
    from settings import get_settings

    _licences(
        monkeypatch,
        answer={
            "seats": 3,
            "subjects": 2,
            "human_subjects": 1,
            "service_subjects": 1,
            "app_key": "commonpractice",
            "basis": "active_during_period",
        },
    )
    scope = seat_scope_for_charge("seat:commonpractice", date(2026, 8, 1), date(2026, 9, 1))
    assert await seats_svc.get_seat_count(TID, get_settings(), token_provider=_tok(), **scope) == 1


async def test_a_period_answer_without_a_person_count_is_refused(monkeypatch) -> None:
    from settings import get_settings

    _licences(
        monkeypatch,
        answer={"seats": 3, "app_key": "commonpractice", "basis": "active_during_period"},
    )
    scope = seat_scope_for_charge("seat:commonpractice", date(2026, 8, 1), date(2026, 9, 1))
    with pytest.raises(SeatCountUnavailableError):
        await seats_svc.get_seat_count(TID, get_settings(), token_provider=_tok(), **scope)


async def test_an_unknown_app_422_is_unavailable_never_zero(monkeypatch) -> None:
    from settings import get_settings

    _licences(monkeypatch, answer={"detail": "app_not_found"}, status=422)
    scope = seat_scope_for_charge("seat:no-such-app", date(2026, 8, 1), date(2026, 9, 1))
    with pytest.raises(SeatCountUnavailableError):
        await seats_svc.get_seat_count(TID, get_settings(), token_provider=_tok(), **scope)


async def test_scoped_and_unscoped_counts_do_not_share_a_cache_entry(monkeypatch) -> None:
    from settings import get_settings

    _licences(
        monkeypatch,
        answer={
            "seats": 2,
            "human_subjects": 2,
            "app_key": "commonpractice",
            "basis": "active_during_period",
        },
    )
    scope = seat_scope_for_charge("seat:commonpractice", date(2026, 8, 1), date(2026, 9, 1))
    assert await seats_svc.get_seat_count(TID, get_settings(), token_provider=_tok(), **scope) == 2
    _licences(monkeypatch, answer={"seats": 7, "basis": "active_licences"})
    assert await seats_svc.get_seat_count(TID, get_settings(), token_provider=_tok()) == 7


async def test_the_bill_counts_a_seat_prefixed_term_over_the_period(
    monkeypatch, db_session, test_settings
) -> None:
    """End to end on the report path (the figure close_period freezes): a ``seat:commonpractice``
    term is billed at the PERIOD count for that app, and says so on its line.

    ⚠️ MUTATION TARGET: drop ``**scope`` from the per-seat loop in services/billing.py and the
    request carries no ``app`` — the stub then answers unscoped and this reads unavailable."""
    from decimal import Decimal

    from models.domain import AccessChargeCreate
    from services import access_charge as access_charge_svc
    from services.billing import generate_billing

    monkeypatch.setattr(seats_svc, "_provider_for", lambda _s: _Token())
    requests: list[dict] = []

    async def _get(self, url, *a, **kw):  # noqa: ANN001
        params = dict(kw.get("params") or {})
        if "seat-count" in str(url):
            requests.append(params)
            if params.get("app") == "commonpractice" and "period_start" in params:
                return httpx.Response(
                    200,
                    json={
                        "seats": 3,
                        "human_subjects": 3,
                        "app_key": "commonpractice",
                        "basis": "active_during_period",
                    },
                )
            return httpx.Response(200, json={"seats": 99, "basis": "active_licences"})
        if "byo-mode" in str(url):
            return httpx.Response(200, json={"byo_mode": "byo_required"})
        if str(url).endswith("/internal/tenants"):
            return httpx.Response(
                200, json=[{"id": TID, "name": "cp", "kind": "customer", "archived": False}]
            )
        return httpx.Response(200, json=[])

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    await access_charge_svc.upsert_access_charge(
        db_session,
        AccessChargeCreate(
            tenant_id=TID,
            service_key="seat:commonpractice",  # pragma: allowlist secret
            amount_usd=Decimal("25"),
            cadence="per_seat",
        ),
    )
    await db_session.commit()

    month = datetime.now(UTC).strftime("%Y-%m")
    totals = (await generate_billing(TID, month, db_session, test_settings))["totals"]
    assert totals["seat_count"] == 3
    assert totals["seat_basis"] == "active_during_period"
    assert Decimal(totals["seat_fee_usd"]) == Decimal("75.00")
    assert requests and requests[0]["app"] == "commonpractice"
