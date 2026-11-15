"""#265 — every per-seat term bills PEOPLE, never non-human licence subjects.

licences' ``/internal/tenants/{id}/seat-count`` answers ``seats`` (licence ROWS) and
``human_subjects`` (distinct people). ``seats`` includes licences held by subjects in
``NON_HUMAN_SUBJECT_REALMS``: ``service`` robots and, since licences#259, CommonPractice's
tenant-licence subjects ``cp-tenant:<tenantId>:a|b``. Before #265 only a ``seat:<app>`` term read
``human_subjects``; ``__seat__``, a NULL key and operator keys billed ``seats``, so every CP
workspace paid one seat too many.

The fixture is the shape licences returns today (``InternalSeatCountResponse``) for a tenant with
TWO people and ONE cp-tenant licence: ``seats=3``, ``human_subjects=2``, ``service_subjects=1``.
Every seat term type must bill 2.

⚠️ MUTATION TARGET: read ``seats`` instead of ``human_subjects`` for unpinned terms in
``services/seats.py`` and every non-``seat:`` case below reads 3.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from routers.config import SEAT_SERVICE_KEY
from services import access_charge as access_charge_svc
from services import seats as seats_svc
from services.byo_mode import clear_byo_cache
from services.seats import seat_scope_for_charge
from tests.conftest import T

pytestmark = pytest.mark.asyncio

TID = T("cp-workspace")
APP = "commonpractice"

#: Every per-seat term shape the code distinguishes: the reserved tenant-wide key, a NULL key,
#: an operator-chosen tenant-wide key, and the product-pinned period key.
SEAT_TERM_KEYS = [SEAT_SERVICE_KEY, None, "__seat_two__", f"seat:{APP}"]


def _two_humans_one_cp_tenant(params: dict[str, str]) -> dict[str, Any]:
    """licences' answer for 2 people + 1 ``cp-tenant`` licence, echoing the requested scope."""
    period = "period_start" in params
    return {
        "tenant_id": TID,
        "seats": 3,
        "subjects": 3,
        "human_subjects": 2,
        "service_subjects": 1,
        "unresolved_subjects": 0,
        "basis": "active_during_period" if period else "active_licences",
        "tenant_kind": "customer",
        "tenant_archived": False,
        "as_of": "2026-10-31T23:59:59.999999+00:00",
        "app_key": params.get("app"),
        "period_start": params.get("period_start"),
        "period_end": params.get("period_end"),
    }


class _Token:
    async def get_token(self) -> str:
        return "t"  # pragma: allowlist secret

    def build_headers(self, tenant_id: str) -> dict[str, str]:
        return {"X-Tenant-Id": tenant_id, "Authorization": "Bearer t"}  # pragma: allowlist secret


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    seats_svc.clear_seat_cache()
    clear_byo_cache()
    monkeypatch.setattr(seats_svc, "_provider_for", lambda _s: _Token())
    yield
    seats_svc.clear_seat_cache()
    clear_byo_cache()


def _licences(
    monkeypatch, seat_answer=_two_humans_one_cp_tenant, *, tenant_id: str = TID
) -> list[dict]:
    """Stub every licences GET the billing paths make; seat-count answers ``seat_answer``."""
    seen: list[dict] = []

    async def _get(self, url, *a, **kw):  # noqa: ANN001
        params = dict(kw.get("params") or {})
        if "seat-count" in str(url):
            seen.append(params)
            return httpx.Response(200, json=seat_answer(params))
        if "byo-mode" in str(url):
            return httpx.Response(200, json={"byo_mode": "byo_required"})
        if str(url).endswith("/internal/tenants"):
            return httpx.Response(
                200,
                json=[
                    {"id": tenant_id, "name": "cp-workspace", "kind": "customer", "archived": False}
                ],
            )
        return httpx.Response(200, json=[])

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    return seen


async def _per_seat_term(session: AsyncSession, key: str | None) -> None:
    from models.domain import AccessChargeCreate

    await access_charge_svc.upsert_access_charge(
        session,
        AccessChargeCreate(
            tenant_id=TID, service_key=key, amount_usd=Decimal("25"), cadence="per_seat"
        ),
    )
    await session.commit()


@pytest.mark.unit
@pytest.mark.parametrize("key", SEAT_TERM_KEYS)
async def test_the_seat_count_excludes_a_cp_tenant_licence_for_every_term(monkeypatch, key) -> None:
    from settings import get_settings

    _licences(monkeypatch)
    scope = seat_scope_for_charge(key, date(2026, 10, 1), date(2026, 11, 1))
    n = await seats_svc.get_seat_count(
        TID, get_settings(), as_of=datetime(2026, 10, 31, 23, 59, tzinfo=UTC), **scope
    )
    assert n == 2, f"term {key!r} billed {n} seats; the cp-tenant licence is not a person"


@pytest.mark.unit
@pytest.mark.parametrize("key", SEAT_TERM_KEYS)
async def test_the_bill_charges_two_seats_on_every_term_type(
    monkeypatch, db_session, test_settings, key
) -> None:
    """The report path — the figure ``close_period`` freezes — bills 2 x $25 on each term type."""
    from services.billing import generate_billing

    _licences(monkeypatch)
    await _per_seat_term(db_session, key)

    month = datetime.now(UTC).strftime("%Y-%m")
    totals = (await generate_billing(TID, month, db_session, test_settings))["totals"]
    assert totals["seat_count"] == 2
    assert Decimal(totals["seat_fee_usd"]) == Decimal("50.00")


@pytest.mark.unit
async def test_the_closed_invoice_freezes_two_seats(
    monkeypatch, db_session, db_engine, test_settings
) -> None:
    """The money actually written down: the frozen row carries 2 seats and $50, not 3 and $75."""
    from models.db import BillingInvoice
    from services.invoices import close_period
    from tests.test_commercial_terms import TID as TERMS_TID
    from tests.test_commercial_terms import (
        _next_month_start,
        _rows,
        _seed_fully_configured_tenant,
        _this_month,
    )

    def _answer(params: dict[str, str]) -> dict[str, Any]:
        return {**_two_humans_one_cp_tenant(params), "tenant_id": TERMS_TID}

    # `_seed_fully_configured_tenant` seeds TERMS_TID, so the registry stub must vouch for it.
    seen = _licences(monkeypatch, _answer, tenant_id=TERMS_TID)

    await _seed_fully_configured_tenant(db_session)
    await db_session.commit()
    _, today = _this_month()
    inv = await close_period(
        db_session,
        tenant_id=TERMS_TID,
        period_start=today.replace(day=1),
        period_end=_next_month_start(today),
        settings=test_settings,
    )
    assert inv.status == "issued"
    await db_session.commit()

    assert seen, "the close never asked licences for a seat count"
    rows = await _rows(db_engine, BillingInvoice)
    assert len(rows) == 1
    assert rows[0].seat_count == 2
    assert rows[0].seat_fee_usd == Decimal("50.00")


@pytest.mark.unit
async def test_an_older_licences_without_human_subjects_falls_back_and_logs(
    monkeypatch,
) -> None:
    """A licences build that predates ``human_subjects``: bill the licence count billing used
    before #265, and say so in the log — never crash, never refuse an unpinned term."""
    from settings import get_settings

    _licences(monkeypatch, lambda _p: {"tenant_id": TID, "seats": 3, "basis": "active_licences"})
    with capture_logs() as logs:
        n = await seats_svc.get_seat_count(TID, get_settings())
    assert n == 3
    assert any(
        e["event"] == "seat_count_human_subjects_missing_fallback_to_licences"
        and e["log_level"] == "warning"
        and e["seats"] == 3
        for e in logs
    ), logs


@pytest.mark.unit
async def test_an_envelope_wrapped_answer_still_bills_people(monkeypatch) -> None:
    from settings import get_settings

    _licences(monkeypatch, lambda p: {"data": _two_humans_one_cp_tenant(p)})
    assert await seats_svc.get_seat_count(TID, get_settings()) == 2


@pytest.mark.unit
async def test_zero_humans_is_zero_seats_not_the_licence_count(monkeypatch) -> None:
    """A workspace whose only licence is its cp-tenant one has no people: 0, not 1.
    ``0`` is falsy — a truthiness check on ``human_subjects`` would fall back to ``seats``."""
    from settings import get_settings

    _licences(
        monkeypatch,
        lambda p: {**_two_humans_one_cp_tenant(p), "seats": 1, "subjects": 1, "human_subjects": 0},
    )
    assert await seats_svc.get_seat_count(TID, get_settings()) == 0
