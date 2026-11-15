"""Contract tests for the three /api/billing/invoices READ routes.

``tests/routers/test_invoices_routes.py`` pins the WRITE surface — who may close,
issue and void, and whether the route commits. This file pins the READ surface
the CommonPractice tenant-admin panel consumes: list, get-one, preview.

⛔ THE THREE PROPERTIES THIS FILE EXISTS FOR, STATED FIRST, BECAUSE EACH IS A
DEFECT THAT WOULD SHIP GREEN WITHOUT IT:

1. **Scope.** ``require_service_caller`` proves the caller is an internal
   service; it says nothing about WHICH tenant they may read, because an
   internal SA token legitimately spans the estate. A route that forgot its
   ``tenant_id`` predicate would return 200 with every tenant's platform fee,
   seat count and realised margin, and every other assertion here would still
   pass.
2. **The seat basis.** A preview of a period must count that PERIOD's seats.
   The distinction is invisible in the number — "3" from the right question and
   "3" from the wrong one are the same integer — so it is asserted through a
   stub that answers the two questions DIFFERENTLY.
3. **Absence vs zero.** ``$0.00`` is the one answer that means four different
   things: nothing was owed, nothing was measured, everything measured was
   deliberately excluded, or the basis can no longer be read. A test suite that
   only asserted "the number is right" cannot tell any of them apart.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from fastapi.routing import iter_route_contexts
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import (
    BillingInvoice,
    BillingInvoiceCounter,
    CostDaily,
    CostEvent,
    TenantAccessCharge,
)
from tests.conftest import T

# --- Principals -------------------------------------------------------------

TENANT = T("invoice-read-mine")
OTHER_TENANT = T("invoice-read-theirs")

BEARER = {"Authorization": "Bearer fake"}

_APR_START = date(2026, 4, 1)
_APR_END = date(2026, 5, 1)
_MAY_START = date(2026, 5, 1)
_MAY_END = date(2026, 6, 1)
_APR = "2026-04"

#: A non-sentinel licence id — see the ⛔ at the CostDaily fixtures below.
_LICENCE = uuid.uuid5(uuid.NAMESPACE_URL, "invoice-read/licence")

LIST_PATH = "/api/billing/invoices"
PREVIEW_PATH = "/api/billing/invoices/preview"


def _money(value: object) -> Decimal:
    """Parse a money field off the wire.

    ⚠️ ``Decimal(str(x))``, NEVER ``Decimal(x)``, AND IT IS NOT DEFENSIVE NOISE.
    Pydantic serialises a ``Decimal`` as a JSON string, but FastAPI's
    ``jsonable_encoder`` path emits a float — and ``Decimal(0.60)`` is
    ``0.59999999999999997779553950749686919152736663818359375``, which is NOT
    equal to ``Decimal("0.60")``. Going through ``str`` yields the float's repr
    ("0.6") and compares correctly under BOTH serialisations, so a test in this
    file can only fail for the endpoint and never for the encoder. Same idiom,
    same reason, as ``tests/test_costs_actor_auth.py``.
    """
    return Decimal(str(value))


def _token(monkeypatch, roles: list[str], sub: str = "test-sa") -> dict:
    """Force bearer decoding to a known payload, patching BOTH modules.

    ``auth_deps`` imports ``_decode_bearer_token`` by name at module load, so
    patching only ``auth`` leaves the copy the dependency actually calls
    untouched. Same helper, same reason, as ``tests/test_costs_actor_auth.py``.
    """
    payload = {"sub": sub, "realm_access": {"roles": roles}}

    async def _fake_decode(_request):  # noqa: ANN001, ANN202
        return payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return payload


def _no_token(monkeypatch) -> None:
    """Bearer decoding fails, as it does for a licence token from license-authz.

    A licence token is not a Keycloak service-account JWT: it does not verify
    against the secure-apps JWKS, so the real dependency raises 401. That 401 is
    the INPUT to the behaviour under test, not the behaviour.
    """
    from fastapi import HTTPException

    async def _fail(_request):  # noqa: ANN001, ANN202
        raise HTTPException(status_code=401, detail="Invalid token")

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fail)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fail)


@pytest.fixture
def service_caller(monkeypatch) -> dict:
    """An internal SA token — the identity CP's backend presents when proxying."""
    return _token(monkeypatch, roles=["billing-internal"], sub="service-account-commonpractice")


@pytest.fixture
def licences_answer_byo_mode(monkeypatch):
    """Answer the licences ``byo-mode`` endpoint; delegate every other GET.

    ``generate_billing`` resolves the tenant-wide BYO posture on every call, and
    an unanswered lookup collapses the tenant to ``byo_required`` by default —
    which happens to be what this file wants, but silently and for the wrong
    reason. URL-scoped and delegating rather than a blanket stub, for the reason
    given at the same fixture in ``tests/routers/test_invoices_routes.py``: a
    stub that answered everything could repair an expectation another test
    depends on being broken.
    """
    from services import byo_mode as byo

    byo.clear_byo_cache()
    real_get = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if "byo-mode" in str(url):
            return httpx.Response(200, json={"byo_mode": "byo_required"})
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    yield
    byo.clear_byo_cache()


@pytest.fixture
def seat_counter(monkeypatch) -> Iterator[list[dict]]:
    """A seat oracle that answers the PERIOD question and the LIVE question
    DIFFERENTLY, and records every ask.

    ⭐ THE TWO ANSWERS ARE THE INSTRUMENT. A stub returning one number for both
    shapes would make ``test_preview_counts_the_periods_seats`` pass under the
    live-count implementation too — the assertion would be measuring the stub,
    not the route. 2 vs 7 makes the seat FEE itself say which question was
    asked, so the property is visible in the response body a consumer reads and
    not only in a recorded call.

    ⚠️ It patches ``services.billing.get_seat_count``, not ``services.seats``'s:
    ``services/billing.py`` does ``from services.seats import get_seat_count``,
    so it holds its own module-level name and patching the origin would leave
    the copy the code calls untouched. Same shape as the ``_decode_bearer_token``
    double-patch above, and the same class of mistake.
    """
    from services import billing as billing_service
    from services import seats as seats_service

    seats_service.clear_seat_cache()
    calls: list[dict] = []

    async def _fake(tenant_id, settings, **kw):  # noqa: ANN001, ANN202
        calls.append(kw)
        return 2 if kw.get("period_start") is not None else 7

    monkeypatch.setattr(billing_service, "get_seat_count", _fake)
    yield calls
    seats_service.clear_seat_cache()


# --- Fixture rows -----------------------------------------------------------


def _factory(db_engine):  # noqa: ANN001, ANN201
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db_engine, *rows) -> None:  # noqa: ANN001
    """Commit fixture rows on a session of their own, so nothing the route does
    (or fails to do) can be confused with the fixture's own transaction."""
    async with _factory(db_engine)() as session:
        for row in rows:
            session.add(row)
        await session.commit()


def _invoice_row(
    tenant_id: str,
    *,
    status: str = "issued",
    invoice_number: str | None = None,
    period_start: date = _APR_START,
    period_end: date = _APR_END,
    total_usd: str = "5075.00",
) -> BillingInvoice:
    """A ``billing_invoices`` row whose COMPONENTS ADD UP TO ITS TOTAL.

    ⭐ THE ARITHMETIC IS THE FIXTURE'S JOB, NOT DECORATION. ``InvoiceSummary``
    promises that the six ``_TOTAL_COMPONENT_COLUMNS`` sum to ``total_usd``, and
    a fixture of all-zeros with a nonzero total could not detect a projector that
    read the wrong column into the wrong field. Here:
    5000 access + 50 seat + 10 llm usage + 3 llm markup + 10 app usage + 2 app
    markup = 5075.00.
    """
    return BillingInvoice(
        tenant_id=tenant_id,
        tenant_name="acme",
        period_start=period_start,
        period_end=period_end,
        status=status,
        invoice_number=invoice_number,
        issued_at=datetime(2026, 5, 1, tzinfo=UTC) if status == "issued" else None,
        subtotal_usd=Decimal(total_usd),
        total_usd=Decimal(total_usd),
        internal_cost_usd=Decimal("20.00"),
        margin_usd=Decimal("5055.00"),
        access_charge_usd=Decimal("5000.00"),
        seat_fee_usd=Decimal("50.00"),
        seat_count=2,
        llm_usage_usd=Decimal("10.00"),
        llm_markup_usd=Decimal("3.00"),
        app_request_usage_usd=Decimal("10.00"),
        app_request_markup_usd=Decimal("2.00"),
        platform_fee_usd=Decimal("1.00"),
        billing_mode="byo_markup_only",
        markup_pct=Decimal("30.00"),
        effective_markup_pct=Decimal("30.0000"),
        snapshot={},
        inputs={},
        snapshot_sha256="0" * 64,
        generator_version="invoice.v1",
    )


def _seat_term(tenant_id: str, *, service_key: str, rate: str = "25") -> TenantAccessCharge:
    """An open per-seat term. ``seat:<app>`` is the scoped form
    :func:`services.seats.seat_scope_for_charge` recognises."""
    return TenantAccessCharge(
        tenant_id=tenant_id,
        service_key=service_key,
        amount_usd=Decimal(rate),
        cadence="per_seat",
        effective_from=datetime(2026, 1, 1, tzinfo=UTC),
    )


# --------------------------------------------------------------------------- #
# 1. Scope
# --------------------------------------------------------------------------- #


@pytest.mark.integration
class TestTheListIsScopedToOneTenant:
    async def test_list_returns_only_the_named_tenants_invoices(
        self, db_engine, app_client, service_caller
    ):
        """⚠️ MUTATION TARGET: delete ``BillingInvoice.tenant_id == tenant_id``
        from the ``select`` in ``routers/billing.py::list_invoices`` and this
        fails on the length — the response carries the other tenant's $5,075.00
        statement, its platform fee and its margin, under a 200.

        ⛔ IT ASSERTS THE ABSENCE BY ID, not merely a count. A count alone would
        also pass against a route that returned two rows of the WRONG tenant.
        """
        mine_a = _invoice_row(TENANT, invoice_number="INV-1")
        mine_b = _invoice_row(
            TENANT, invoice_number="INV-2", period_start=_MAY_START, period_end=_MAY_END
        )
        theirs = _invoice_row(OTHER_TENANT, invoice_number="INV-3")
        await _seed(db_engine, mine_a, mine_b, theirs)

        resp = app_client.get(LIST_PATH, params={"tenant_id": TENANT}, headers=BEARER)
        assert resp.status_code == 200, resp.text

        rows = resp.json()["data"]
        assert len(rows) == 2
        assert {r["tenant_id"] for r in rows} == {TENANT}
        assert str(theirs.id) not in {r["id"] for r in rows}

    async def test_the_newest_period_comes_first(self, db_engine, app_client, service_caller):
        """Newest first, as the panel renders them.

        ⚠️ MUTATION TARGET: drop ``.desc()`` from the ``period_start`` ordering
        and this fails — April leads instead of May.
        """
        await _seed(
            db_engine,
            _invoice_row(TENANT, invoice_number="APR"),
            _invoice_row(
                TENANT, invoice_number="MAY", period_start=_MAY_START, period_end=_MAY_END
            ),
        )
        resp = app_client.get(LIST_PATH, params={"tenant_id": TENANT}, headers=BEARER)
        assert [r["invoice_number"] for r in resp.json()["data"]] == ["MAY", "APR"]

    async def test_a_voided_statement_is_still_listed(self, db_engine, app_client, service_caller):
        """⭐ A void KEEPS its number and stays readable — that is what lets a
        reader see a statement was superseded rather than destroyed. Filtering it
        out here would make a re-billed month look as if it had only ever been
        billed once.

        ⚠️ MUTATION TARGET: add ``.where(BillingInvoice.status != "void")`` and
        this fails.
        """
        await _seed(
            db_engine,
            _invoice_row(TENANT, status="void", invoice_number="VOIDED"),
        )
        rows = app_client.get(LIST_PATH, params={"tenant_id": TENANT}, headers=BEARER).json()[
            "data"
        ]
        assert [r["status"] for r in rows] == ["void"]
        assert rows[0]["invoice_number"] == "VOIDED"

    async def test_the_components_add_up_to_the_total_they_are_shown_beside(
        self, db_engine, app_client, service_caller
    ):
        """⭐ THE PANEL'S RECONCILIATION, ASSERTED AT THE WIRE.

        ``InvoiceSummary`` carries exactly ``services.invoices.
        _TOTAL_COMPONENT_COLUMNS`` so a rendered statement adds up to its own
        displayed lines. A reader that could not do this arithmetic would have to
        take ``total_usd`` on faith.

        ⛔ AND IT PINS THE TWO NON-COMPONENTS OUT OF THE SUM. ``platform_fee_usd``
        is a surcharge already folded INSIDE the markup columns and
        ``excluded_unbillable_usd`` is spend deliberately NOT billed; adding
        either would double-count or invert. Both are asserted present and
        excluded from the identity.

        ⚠️ MUTATION TARGET: project ``inv.total_usd`` into ``llm_markup_usd`` in
        ``_invoice_summary`` (a plausible copy-paste) and the sum overshoots.
        """
        await _seed(db_engine, _invoice_row(TENANT, invoice_number="INV-1"))
        row = app_client.get(LIST_PATH, params={"tenant_id": TENANT}, headers=BEARER).json()[
            "data"
        ][0]

        components = (
            "access_charge_usd",
            "seat_fee_usd",
            "llm_usage_usd",
            "llm_markup_usd",
            "app_request_usage_usd",
            "app_request_markup_usd",
        )
        assert sum(_money(row[c]) for c in components) == _money(row["total_usd"])
        # Present, and deliberately outside the identity above.
        assert _money(row["platform_fee_usd"]) == Decimal("1.00")
        assert _money(row["excluded_unbillable_usd"]) == Decimal("0")
        # ⚠️ A QUANTITY, and 2 rather than None — the other arm of the nullable
        # ``seat_count`` contract is covered by the preview tests below.
        assert row["seat_count"] == 2
        assert row["billing_mode"] == "byo_markup_only"


@pytest.mark.integration
class TestAForeignInvoiceIsNotFound:
    async def test_another_tenants_invoice_id_is_404(self, db_engine, app_client, service_caller):
        """⚠️ MUTATION TARGET: delete ``BillingInvoice.tenant_id == tenant_id``
        from the ``select`` in ``routers/billing.py::get_invoice`` and this
        returns **200 with another tenant's statement**.

        ⛔ AND 404 IS PART OF THE ASSERTION, NOT 403. "Forbidden" is an oracle:
        it confirms the id exists and belongs to someone else. A test that
        accepted "anything but 200" would pass against that disclosure.
        """
        theirs = _invoice_row(OTHER_TENANT, invoice_number="INV-3")
        await _seed(db_engine, theirs)

        resp = app_client.get(
            f"/api/billing/invoices/{theirs.id}",
            params={"tenant_id": TENANT},
            headers=BEARER,
        )
        assert resp.status_code == 404, resp.text

    async def test_the_owning_tenant_gets_it(self, db_engine, app_client, service_caller):
        """⭐ THE CONTROL THAT KEEPS THE 404 ABOVE HONEST. Without it, a route
        that 404'd for EVERYONE — the panel completely broken — would satisfy
        every refusal in this class and read as a security win.
        """
        mine = _invoice_row(TENANT, invoice_number="INV-1")
        await _seed(db_engine, mine)

        resp = app_client.get(
            f"/api/billing/invoices/{mine.id}", params={"tenant_id": TENANT}, headers=BEARER
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["data"]["invoice_number"] == "INV-1"

    async def test_an_unknown_id_is_404_not_500(self, db_engine, app_client, service_caller):
        resp = app_client.get(
            f"/api/billing/invoices/{uuid.uuid4()}",
            params={"tenant_id": TENANT},
            headers=BEARER,
        )
        assert resp.status_code == 404, resp.text

    async def test_preview_is_not_swallowed_by_the_id_route(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode
    ):
        """⛔ THE REGISTRATION-ORDER GUARD, AND IT IS NOT HYPOTHETICAL.

        FastAPI matches in declaration order. Move ``GET /invoices/preview``
        below ``GET /invoices/{invoice_id}`` in ``routers/billing.py`` and the
        literal string "preview" is captured by the parameterised route, fails to
        parse as a UUID, and this endpoint answers **422 for every call while
        still appearing in ``docs/openapi.json``**.

        ⚠️ MUTATION TARGET: swap the two route declarations -> 422.
        """
        resp = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        )
        assert resp.status_code == 200, resp.text


# --------------------------------------------------------------------------- #
# 2. Who may read
# --------------------------------------------------------------------------- #


@pytest.mark.integration
class TestOnlyAServiceCallerMayRead:
    """A statement names the platform fee, the seat count, the negotiated markup
    and the realised margin. That is tenant-ADMIN data, and ``require_tenant``
    accepts the header every member of a tenant holds.
    """

    @pytest.mark.parametrize(
        "path,params",
        [
            (LIST_PATH, {"tenant_id": TENANT}),
            (PREVIEW_PATH, {"tenant_id": TENANT, "period": _APR}),
            (f"/api/billing/invoices/{uuid.uuid4()}", {"tenant_id": TENANT}),
        ],
    )
    async def test_a_licence_token_caller_is_refused(
        self, app_client, monkeypatch, path: str, params: dict
    ):
        """⚠️ MUTATION TARGET: change any of the three dependencies back to
        ``require_tenant`` and that row returns 200 with the tenant's commercial
        terms.

        ⛔ THE STATUS IS PART OF THE ASSERTION. A test accepting 401 would pass
        against a gate that merely asked this caller to re-authenticate — which
        cannot help, because no licence token will ever satisfy these routes.
        ⚠️ And the id route is asserted at **403, not 404**: the auth gate runs
        as a dependency, BEFORE the handler, so an unauthorised caller must never
        learn whether the id exists.
        """
        _no_token(monkeypatch)
        resp = app_client.get(path, params=params, headers={**BEARER, "X-Tenant-Id": TENANT})
        assert resp.status_code == 403, resp.text

    async def test_no_credential_at_all_is_401_not_403(self, app_client, monkeypatch):
        """⭐ PINS THAT THE HEADER IS NOT THE CREDENTIAL. ``require_service_caller``
        reads ``X-Tenant-Id`` only to choose between two REFUSALS; if its absence
        could turn a refusal into a 403-rather-than-401 it would mean the header
        was being consulted as identity.
        """
        _no_token(monkeypatch)
        resp = app_client.get(LIST_PATH, params={"tenant_id": TENANT}, headers=BEARER)
        assert resp.status_code == 401, resp.text

    async def test_a_valid_sa_token_without_an_internal_role_is_refused(
        self, app_client, monkeypatch
    ):
        """⚠️ MUTATION TARGET: widen ``auth_deps.INTERNAL_SERVICE_ROLES``."""
        _token(monkeypatch, roles=["portal-user"])
        resp = app_client.get(
            LIST_PATH, params={"tenant_id": TENANT}, headers={**BEARER, "X-Tenant-Id": TENANT}
        )
        assert resp.status_code == 403, resp.text

    async def test_a_proxying_caller_may_not_read_past_its_own_header(
        self, db_engine, app_client, service_caller
    ):
        """⭐ THE SECOND, INDEPENDENT GATE. CP's backend legitimately presents an
        internal SA token AND ``X-Tenant-Id`` naming the tenant it proxies for.
        ``enforce_tenant_isolation`` stops it then reading a DIFFERENT tenant's
        statements under that assertion.

        ⚠️ MUTATION TARGET: delete the ``enforce_tenant_isolation`` call in
        ``list_invoices`` -> 200.
        """
        await _seed(db_engine, _invoice_row(OTHER_TENANT, invoice_number="INV-3"))
        resp = app_client.get(
            LIST_PATH,
            params={"tenant_id": OTHER_TENANT},
            headers={**BEARER, "X-Tenant-Id": TENANT},
        )
        assert resp.status_code == 403, resp.text


@pytest.mark.unit
class TestBillingRouteGateInventory:
    """Pin the auth dependency on every ``/api/billing`` route.

    ⛔ THE SIBLING INVENTORY DID NOT COVER THIS ROUTER, AND THAT GAP IS WHY THIS
    CLASS EXISTS. ``tests/test_pricing_write_gate.py::TestConfigRouteGateInventory``
    filters ``path.startswith("/api/config")``, so every ``/api/billing`` route
    has always been ungated by any inventory — a re-gate here, in either
    direction, moved no test. The per-route assertions above cover the three new
    reads; this covers the ROUTER, so a future route added without a deliberate
    gate decision reds instead of inheriting one by accident.

    ⚠️ It asserts EQUALITY, not containment. A subset check would let a new
    unlisted route ship ungated, which is the failure mode being closed.
    """

    def test_billing_route_gates_are_pinned(self) -> None:
        from main import app

        expected = {
            ("GET", "/api/billing/tenant/{tenant_id}"): "require_tenant",
            ("GET", "/api/billing/overview"): "require_admin",
            # WRITES. ⛔ `require_pricing_writer`, NOT `require_admin` and NOT
            # `require_pricing_admin`: minting a customer's statement is a
            # money-config write, and billing #55 deliberately removed the broad
            # `billing-internal` SA role from that capability.
            ("POST", "/api/billing/invoices/close"): "require_pricing_writer",
            ("POST", "/api/billing/invoices/{invoice_id}/issue"): "require_pricing_writer",
            ("POST", "/api/billing/invoices/{invoice_id}/void"): "require_pricing_writer",
            # ⚠️ `/reproduce` STAYS ON `require_tenant`, DELIBERATELY, AND IT IS
            # NOT AN OVERSIGHT THAT IT DIFFERS FROM THE READS BELOW. It re-renders
            # ONE statement the caller already holds the id of, and pairs the
            # dependency with `enforce_tenant_isolation`; a tenant member asking
            # for their own workspace's statement by id is the intended caller.
            # The three reads below are a different question — they ENUMERATE and
            # they PROJECT commercial terms — so they take the stricter gate.
            ("GET", "/api/billing/invoices/{invoice_id}/reproduce"): "require_tenant",
            # READS. ⛔ `require_service_caller`: a statement names the platform
            # fee, the seat count, the negotiated markup and the realised margin.
            # Under `require_tenant` any seat holding a licence token could read
            # their workspace's commercial terms.
            ("GET", "/api/billing/invoices"): "require_service_caller",
            ("GET", "/api/billing/invoices/preview"): "require_service_caller",
            ("GET", "/api/billing/invoices/{invoice_id}"): "require_service_caller",
        }

        actual: dict[tuple[str, str], str] = {}
        # FastAPI >=0.137 nests included routers behind `_IncludedRouter`
        # wrappers; iter_route_contexts flattens them to the effective routes
        # (prefix + router-level dependencies applied), as app.routes used to.
        for route in iter_route_contexts(app.routes):
            path = route.path or ""
            dependant = getattr(route, "dependant", None)
            if not path.startswith("/api/billing") or dependant is None:
                continue
            gates = sorted(
                {
                    d.call.__name__
                    for d in dependant.dependencies
                    if getattr(d, "call", None) is not None
                    and d.call.__name__.startswith("require_")
                }
            )
            methods = route.methods
            assert methods is not None, path
            for method in sorted(methods - {"HEAD", "OPTIONS"}):
                actual[(method, path)] = ",".join(gates)

        assert actual == expected


# --------------------------------------------------------------------------- #
# 3. The preview
# --------------------------------------------------------------------------- #


@pytest.mark.integration
class TestThePreviewCountsThePeriodsSeats:
    async def test_a_scoped_term_is_counted_over_the_period_not_at_a_point(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode, seat_counter
    ):
        """⛔ THE DEFECT THIS CLOSES IS INVISIBLE IN THE NUMBER ALONE.

        A ``seat:<app>`` term bills that product's seats held at ANY instant in
        ``[period_start, period_end)``, counted as DISTINCT PEOPLE
        (``human_subjects``). A point-in-time count of the same tenant is a
        different, equally plausible integer, and an invoice billed off the wrong
        one looks entirely ordinary. The stub therefore answers 2 to the period
        question and 7 to the point-in-time one, so the SEAT FEE says which was
        asked.

        ⚠️ MUTATION TARGET: make ``services.seats.seat_scope_for_charge`` return
        ``{}`` (or strip the ``**scope`` from ``services.billing.
        _resolve_seat_count``'s call) and this fails three ways at once —
        ``seat_count`` reads 7, ``seat_fee_usd`` reads "175.00", and
        ``seat_basis`` reads "active_licences".

        ⚠️ That ``human_subjects`` rather than ``seats`` is read out of the
        licences response is pinned one layer down, by
        ``tests/test_seat_scope.py::test_a_period_term_bills_people_not_licences``
        — this test pins that the period QUESTION is asked at all.
        """
        await _seed(
            db_engine,
            _seat_term(TENANT, service_key="seat:commonpractice"),  # pragma: allowlist secret
        )  # pragma: allowlist secret

        resp = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]

        assert data["seat_count"] == 2
        assert data["seat_basis"] == "active_during_period"
        assert _money(data["seat_fee_usd"]) == Decimal("50.00")

        # And the ask itself carried the period, not a wall-clock instant.
        assert len(seat_counter) == 1
        ask = seat_counter[0]
        assert ask["app_key"] == "commonpractice"
        assert ask["period_start"] == datetime(2026, 4, 1, tzinfo=UTC)
        assert ask["period_end"] == datetime(2026, 5, 1, tzinfo=UTC)

    async def test_an_unpinned_term_is_counted_at_the_periods_last_instant(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode, seat_counter
    ):
        """The legacy ``__seat__`` shape, and the honest statement of what it does.

        An unpinned term has no product to scope to, so it takes the
        point-in-time basis — but ``as_of`` is the LAST INSTANT OF THE BILLED
        PERIOD, never ``now()``. That is what stops a re-preview of a CLOSED month
        drifting as licences change.

        ⚠️ MUTATION TARGET: drop ``as_of=bill_ts`` from
        ``services.billing._resolve_seat_count``'s call and ``ask["as_of"]``
        becomes ``None`` — the count silently becomes today's.
        """
        await _seed(
            db_engine,
            _seat_term(TENANT, service_key="__seat__"),  # pragma: allowlist secret
        )  # pragma: allowlist secret

        resp = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        )
        data = resp.json()["data"]

        assert data["seat_count"] == 7
        assert data["seat_basis"] == "active_licences"
        ask = seat_counter[0]
        assert ask["period_start"] is None
        # The last microsecond of the half-open period — not its exclusive
        # midnight boundary, which sits before a charge created during it.
        assert ask["as_of"] == datetime(2026, 5, 1, tzinfo=UTC) - timedelta(microseconds=1)


@pytest.mark.integration
class TestThePreviewMutatesNothing:
    async def test_it_writes_no_statement_and_draws_no_invoice_number(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode, seat_counter
    ):
        """⛔ A PREVIEW THAT CONSUMED A NUMBER WOULD PUNCH A HOLE IN A SERIES
        WHOSE WHOLE PURPOSE IS TO HAVE NONE.

        ``billing_invoice_counters.next_seq`` is drawn from under
        ``SELECT … FOR UPDATE`` and a drawn number is never returned, so a
        preview that reached ``issue`` would burn one per page load.

        ⚠️ THE READ-BACK IS ON A SEPARATE SESSION, OPENED AFTER THE RESPONSE —
        the same method the write tests use. The request's own session is the one
        that rolls back, so checking through it would report "nothing written"
        whether or not anything was.

        ⚠️ MUTATION TARGET: add a ``close_period`` call (or any ``session.add``)
        to ``preview_invoice`` and ``invoices != []`` reds. Calling it twice is
        the control for an idempotent-looking write that still moves a counter.
        """
        await _seed(
            db_engine,
            _seat_term(TENANT, service_key="seat:commonpractice"),  # pragma: allowlist secret
        )  # pragma: allowlist secret

        for _ in range(2):
            resp = app_client.get(
                PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
            )
            assert resp.status_code == 200, resp.text

        async with _factory(db_engine)() as session:
            invoices = list((await session.execute(select(BillingInvoice))).scalars().all())
            counters = list((await session.execute(select(BillingInvoiceCounter))).scalars().all())
        assert invoices == []
        # ⭐ NO COUNTER ROW AT ALL is the strongest available assertion: `issue`
        # CREATES the row on first draw, so its absence rules out a draw that was
        # then rolled back as well as one that stuck.
        assert counters == []

    async def test_the_preview_carries_no_id_and_no_number(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode, seat_counter
    ):
        """⭐ A PREVIEW IS NOT CITABLE, AND THE SHAPE SAYS SO. If it carried an
        ``invoice_number`` a caller would reasonably quote it, and there would be
        no statement behind it — the same "200 with a phantom number" failure the
        missing router commits produced.
        """
        await _seed(
            db_engine,
            _seat_term(TENANT, service_key="seat:commonpractice"),  # pragma: allowlist secret
        )  # pragma: allowlist secret
        data = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        ).json()["data"]
        assert "id" not in data
        assert "invoice_number" not in data


# --------------------------------------------------------------------------- #
# 4. Absence is not zero
# --------------------------------------------------------------------------- #


@pytest.mark.integration
class TestAnEmptyPeriodIsDistinguishableFromAZeroBill:
    """⛔ THE TWO TESTS BELOW ARE ONE ASSERTION IN TWO HALVES AND NEITHER WORKS
    ALONE. "no data" and "a genuine $0.00" must produce DIFFERENT bodies; a test
    of either on its own is satisfied by a route that returns that body always.
    """

    async def test_a_period_with_no_basis_reports_no_basis_and_null_money(
        self, db_engine, app_client, service_caller
    ):
        """⚠️ MUTATION TARGET: delete the ``if not cost_rows and not
        metering_rows and not access_terms`` branch in ``preview_invoice`` and
        this fails on ``total_usd is None`` — the route answers a confident
        ``"0.00"`` for a period nothing was ever measured in, which a tenant
        admin reads as "we owe nothing".

        ⚠️ AND IT IS `is None`, NOT falsiness: ``Decimal("0") == 0`` is True and
        ``not Decimal("0")`` is True, so an ``assert not data["total_usd"]``
        would pass against the exact defect.
        """
        resp = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]

        assert data["basis"] == "no_basis"
        assert data["total_usd"] is None
        assert data["access_charge_usd"] is None
        assert data["seat_fee_usd"] is None
        assert data["effective_markup_pct"] is None
        # The three counts the verdict was taken from, disclosed so a reader can
        # see WHICH was empty rather than only that the verdict was reached.
        assert data["cost_rollup_rows"] == 0
        assert data["metering_rollup_rows"] == 0
        assert data["access_charge_terms"] == 0

    async def test_a_measured_zero_bill_reports_zero_not_absence(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode
    ):
        """The other half: rollup rows EXIST and the bill really is $0.00.

        ⚠️ MUTATION TARGET: make the ``no_basis`` branch trigger on
        ``total_usd == 0`` instead of on the three counts, and this fails —
        a real, measured, correct $0.00 month becomes indistinguishable from a
        purged one.
        """
        await _seed(
            db_engine,
            CostDaily(
                tenant_id=TENANT,
                # ⛔ A REAL LICENCE ID, NOT THE DEFAULT. `CostDaily.license_id`
                # defaults to `NULL_LICENSE_ID`, the system/S2S sentinel, and
                # `generate_billing` SKIPS every sentinel row (`is_system_license`)
                # because system cost is never billed to a customer. A fixture
                # that omits it therefore produces a $0.00 bill for a reason
                # having nothing to do with the property under test — which is
                # exactly how a "measured zero" assertion passes vacuously.
                license_id=_LICENCE,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                bucket_date=_APR_START,
                total_input_tokens=0,
                total_output_tokens=0,
                total_cost_usd=Decimal("0"),
                request_count=0,
            ),
        )
        data = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        ).json()["data"]

        assert data["basis"] == "rollup"
        assert data["cost_rollup_rows"] == 1
        assert _money(data["total_usd"]) == Decimal("0.00")
        # ⭐ NO PER-SEAT TERM APPLIES, which is a THIRD fact — not 0 seats, and
        # not "licences could not be asked". Both nulls, read together.
        assert data["seat_count"] is None
        assert data["seat_basis"] is None

    async def test_an_access_term_alone_is_a_basis(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode
    ):
        """⭐ THE THIRD COUNT, AND THE EASIEST TO FORGET. A tenant with an open
        flat platform fee and no usage at all genuinely owes that fee — measured
        on prod, the one tenant with open access rows carries $5,000.00/month. A
        basis test that consulted only the two rollups would report ``no_basis``
        and ``None`` money for a real five-thousand-dollar bill.

        ⚠️ MUTATION TARGET: drop ``access_terms`` from the ``if`` in
        ``preview_invoice`` -> ``basis == "no_basis"`` and ``total_usd is None``.
        """
        await _seed(
            db_engine,
            TenantAccessCharge(
                tenant_id=TENANT,
                service_key=None,
                amount_usd=Decimal("5000.00"),
                cadence="monthly",
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        )
        data = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        ).json()["data"]

        assert data["basis"] == "rollup"
        assert data["cost_rollup_rows"] == 0
        assert data["access_charge_terms"] == 1
        assert _money(data["total_usd"]) == Decimal("5000.00")

    async def test_an_unreadable_raw_window_reports_unknown_not_zero(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode
    ):
        """⛔ THE THIRD STATE: spend that was metered and deliberately NOT billed.

        ``excluded_from_billing`` / ``exclusion_reason`` live ONLY on
        ``billing_cost_events``, and ``rollup_daily`` filters those rows out on
        the way into ``billing_cost_daily`` — so the rollup this preview is
        computed from is already net of them and has no column that could say by
        how much. Measured fleet-wide 2026-09-17: $118.708744 raw against
        $118.184744 rolled up, the $0.524 difference being 18
        ``unregistered_tenant`` rows and 2,750 ``health_probe`` rows.

        ⛔ AND THE RAW TABLE IS RETENTION-BOUNDED (``settings.retention_days``,
        default 90, dropped a whole monthly partition at a time), so for an older
        window the honest answer is UNKNOWN. This test pins that it is reported
        as ``None`` and NOT as 0: "nothing was excluded" and "we can no longer
        tell what was excluded" are different facts and only one is reassuring.

        ⚠️ MUTATION TARGET: replace the ``if not present`` early return in
        ``_measure_exclusions`` with a fall-through, and ``excluded_usd`` becomes
        ``"0"`` — a fabricated zero, indistinguishable from a measured one.

        ⚠️ AND IT IS `is None`, not falsiness — see the sibling test above.
        """
        await _seed(
            db_engine,
            CostDaily(
                tenant_id=TENANT,
                # ⛔ A REAL LICENCE ID, NOT THE DEFAULT. `CostDaily.license_id`
                # defaults to `NULL_LICENSE_ID`, the system/S2S sentinel, and
                # `generate_billing` SKIPS every sentinel row (`is_system_license`)
                # because system cost is never billed to a customer. A fixture
                # that omits it therefore produces a $0.00 bill for a reason
                # having nothing to do with the property under test — which is
                # exactly how a "measured zero" assertion passes vacuously.
                license_id=_LICENCE,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                bucket_date=_APR_START,
                total_input_tokens=10,
                total_output_tokens=2,
                total_cost_usd=Decimal("1.500000"),
                request_count=1,
            ),
        )
        data = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        ).json()["data"]

        # The rollup is readable, so the BILL is a real number...
        assert data["basis"] == "rollup"
        assert _money(data["total_usd"]) > 0
        # ...while the parked residual behind it is not, and says so.
        assert data["exclusions"]["basis"] == "raw_events_absent"
        assert data["exclusions"]["excluded_usd"] is None
        assert data["exclusions"]["excluded_rows"] is None
        # ⛔ `None`, NOT `{}`. An empty map reads as "measured, nothing
        # excluded", which is the claim this branch cannot make.
        assert data["exclusions"]["by_reason"] is None

    async def test_the_exclusion_verdict_is_taken_even_when_there_is_no_basis(
        self, db_engine, app_client, service_caller
    ):
        """⛔ A WINDOW WHOSE EVENTS WERE ALL PARKED HAS ZERO ROLLUP ROWS BY
        CONSTRUCTION, so ``no_basis`` and "everything was excluded" produce the
        identical empty rollup. Short-circuiting the exclusion query on
        ``no_basis`` would hide exactly the spend this disclosure exists to
        surface.

        ⚠️ MUTATION TARGET: move the ``_measure_exclusions`` call below the
        ``no_basis`` early return in ``preview_invoice``, or give the early
        return a ``not_applicable`` exclusions block, and this fails — the
        ``exclusions`` object stops being measured for precisely the state in
        which an all-parked window lands.
        """
        data = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        ).json()["data"]

        assert data["basis"] == "no_basis"
        # Measured, not skipped: the verdict came from the raw table.
        assert data["exclusions"]["basis"] == "raw_events_absent"

    async def test_parked_spend_is_disclosed_beside_the_billable_total(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode
    ):
        """⛔ THE SECOND STATE, MEASURED: rows existed, and some were deliberately
        not billed.

        A tenant admin shown "$1.50" for a period in which $2.25 was metered
        deserves to see where the other $0.75 went. The rollup cannot tell them —
        ``rollup_daily`` filters parked rows out on the way in, so
        ``billing_cost_daily`` is already net of them and has no column that
        could say by how much — which is why this is a second query against
        ``billing_cost_events``.

        ⭐ THE TWO REASONS ARE KEPT APART, NOT SUMMED. ``unregistered_tenant``
        says a uuid nothing vouches for is metering, which is a security and
        attribution finding a human must act on; ``health_probe`` is a known,
        allowlisted, deliberately-excluded population. One combined figure would
        retire the first alarm by averaging it into the second.

        ⚠️ MUTATION TARGET: flip ``excluded_from_billing.is_(True)`` to
        ``is_(False)`` in ``_measure_exclusions`` and this fails — the disclosure
        reports the BILLABLE spend as if it had been withheld, which is the same
        number the panel already shows and therefore invisible without this
        assertion.
        """
        await _seed(
            db_engine,
            CostDaily(
                tenant_id=TENANT,
                # ⛔ A REAL LICENCE ID, NOT THE DEFAULT. `CostDaily.license_id`
                # defaults to `NULL_LICENSE_ID`, the system/S2S sentinel, and
                # `generate_billing` SKIPS every sentinel row (`is_system_license`)
                # because system cost is never billed to a customer. A fixture
                # that omits it therefore produces a $0.00 bill for a reason
                # having nothing to do with the property under test — which is
                # exactly how a "measured zero" assertion passes vacuously.
                license_id=_LICENCE,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                bucket_date=_APR_START,
                total_input_tokens=10,
                total_output_tokens=2,
                total_cost_usd=Decimal("1.500000"),
                request_count=1,
            ),
            # The billable twin of that rollup row — present so the query has to
            # DISCRIMINATE rather than merely find rows.
            CostEvent(
                tenant_id=TENANT,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                input_tokens=10,
                output_tokens=2,
                cost_usd=Decimal("1.50"),
                created_at=datetime(2026, 4, 15, 12, 0, tzinfo=UTC),
            ),
            CostEvent(
                tenant_id=TENANT,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                cost_usd=Decimal("0.60"),
                created_at=datetime(2026, 4, 16, 12, 0, tzinfo=UTC),
                excluded_from_billing=True,
                exclusion_reason="unregistered_tenant",
            ),
            CostEvent(
                tenant_id=TENANT,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                cost_usd=Decimal("0.15"),
                created_at=datetime(2026, 4, 17, 12, 0, tzinfo=UTC),
                excluded_from_billing=True,
                exclusion_reason="health_probe",
            ),
            # ⛔ ANOTHER TENANT'S PARKED SPEND, seeded on purpose: without it a
            # missing ``tenant_id`` predicate in ``_measure_exclusions`` would
            # leak another customer's withheld spend onto this panel and every
            # other assertion here would still pass.
            CostEvent(
                tenant_id=OTHER_TENANT,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                cost_usd=Decimal("99.00"),
                created_at=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
                excluded_from_billing=True,
                exclusion_reason="health_probe",
            ),
        )
        data = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        ).json()["data"]

        excl = data["exclusions"]
        assert excl["basis"] == "raw_cost_events"
        assert excl["excluded_rows"] == 2
        assert _money(excl["excluded_usd"]) == Decimal("0.75")
        assert {k: _money(v) for k, v in excl["by_reason"].items()} == {
            "unregistered_tenant": Decimal("0.60"),
            "health_probe": Decimal("0.15"),
        }
        # And the BILL is still only the billable half — the disclosure sits
        # beside the total, never inside it.
        assert _money(data["total_usd"]) > 0
        assert _money(data["total_usd"]) < Decimal("99.00")

    async def test_a_window_with_rows_and_no_parking_reports_a_measured_zero(
        self, db_engine, app_client, service_caller, licences_answer_byo_mode
    ):
        """⭐ THE CONTROL FOR THE `None` ASSERTIONS ABOVE, AND IT IS THE PAIR THAT
        GIVES THEM MEANING. ``0.00`` here and ``None`` there must come from the
        same field for two different reasons; a suite asserting only one of them
        is satisfied by a route that always answers that one.

        ⚠️ MUTATION TARGET: return ``EXCLUSION_BASIS_UNMEASURABLE`` whenever the
        grouped query is empty (i.e. collapse the presence probe into the sum)
        and this fails — a window that was genuinely read and genuinely had
        nothing parked would report "we cannot tell", which is a false alarm in
        the other direction.
        """
        await _seed(
            db_engine,
            CostDaily(
                tenant_id=TENANT,
                # ⛔ A REAL LICENCE ID, NOT THE DEFAULT. `CostDaily.license_id`
                # defaults to `NULL_LICENSE_ID`, the system/S2S sentinel, and
                # `generate_billing` SKIPS every sentinel row (`is_system_license`)
                # because system cost is never billed to a customer. A fixture
                # that omits it therefore produces a $0.00 bill for a reason
                # having nothing to do with the property under test — which is
                # exactly how a "measured zero" assertion passes vacuously.
                license_id=_LICENCE,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                bucket_date=_APR_START,
                total_cost_usd=Decimal("1.500000"),
                request_count=1,
            ),
            CostEvent(
                tenant_id=TENANT,
                service_key="workers",  # pragma: allowlist secret
                provider="test",
                cost_usd=Decimal("1.50"),
                created_at=datetime(2026, 4, 15, 12, 0, tzinfo=UTC),
            ),
        )
        excl = app_client.get(
            PREVIEW_PATH, params={"tenant_id": TENANT, "period": _APR}, headers=BEARER
        ).json()["data"]["exclusions"]

        assert excl["basis"] == "raw_cost_events"
        assert excl["excluded_rows"] == 0
        assert _money(excl["excluded_usd"]) == Decimal("0")
        # ⛔ `{}`, NOT `None`. "Measured, nothing parked" — the opposite claim
        # from the `raw_events_absent` branch, and the reason both exist.
        assert excl["by_reason"] == {}
