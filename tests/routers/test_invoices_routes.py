"""Contract tests for the /api/billing/invoices routes.

These pin the ROUTE LAYER: who is allowed in, which malformed requests are
refused before any money code runs, and how a service refusal is mapped onto a
status code. The service's own behaviour — freezing, gapless numbering, the
disclosure census — is covered by ``tests/test_invoices.py`` and
``tests/test_invoice_immutability.py``.

⛔ THERE IS NOW A HAPPY PATH HERE, AND ITS ABSENCE WAS THE DEFECT.
This docstring used to argue that a happy-path close belonged in
``tests/test_invoices.py`` because ``close_period`` reaches the licences service
and "faking that well enough to be meaningful would test the fake". The argument
was wrong in a specific and expensive way: ``tests/test_invoices.py`` drives the
SERVICE on a session it commits itself, so it can never see whether the ROUTE
commits — and the route did not. ``db.get_session`` yields a session and never
commits, ``AsyncSession``'s exit rolls back, and ``services.audit.record`` only
flushes, so every invoice write returned 200 with an ``invoice_number`` in the
body and then discarded the row, the number and the audit entry. Between the two
files, every refusal was covered and no success was, which is exactly the shape
that lets a write path ship without a commit.

So the happy paths below are not convenience coverage; they are the control. Each
re-reads the row from a SEPARATE session opened AFTER the response — never from
the response body and never from the request's own session, because asserting the
body is precisely the check that passed for two months while nothing persisted.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import AuditLog, BillingInvoice, CostDaily, MarkupRule
from tests.conftest import T


@pytest.fixture
def pricing_writer(monkeypatch):
    """A bearer that carries ``pricing-writer`` — the role the writes require."""
    payload = {"sub": "test-pricing-writer", "realm_access": {"roles": ["pricing-writer"]}}

    async def _fake_decode(_request):
        return payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return payload


@pytest.fixture
def billing_internal_only(monkeypatch):
    """A bearer carrying ONLY ``billing-internal``.

    ⭐ This is the control that gives the authz test its meaning. auth_deps is
    explicit that a service account allowed to seed provider cost must NOT
    thereby be able to change what a customer is charged. If the invoice writes
    had been put behind ``require_admin`` or ``require_pricing_admin`` instead,
    this identity would get through — so a test that only proved "a valid token
    works" would have passed against the wrong gate.
    """
    payload = {"sub": "test-internal", "realm_access": {"roles": ["billing-internal"]}}

    async def _fake_decode(_request):
        return payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return payload


_CLOSE = "/api/billing/invoices/close"


def _close_body(**over):
    body = {
        "tenant_id": str(uuid.uuid4()),
        "period_start": "2026-06-01",
        "period_end": "2026-07-01",
    }
    body.update(over)
    return body


class TestInvoiceWritesRequirePricingWriter:
    def test_billing_internal_alone_cannot_close(self, app_client, billing_internal_only):
        """The whole point of the require_pricing_writer split."""
        r = app_client.post(_CLOSE, json=_close_body(), headers={"Authorization": "Bearer x"})
        assert r.status_code == 403, r.text

    def test_billing_internal_alone_cannot_issue(self, app_client, billing_internal_only):
        r = app_client.post(
            f"/api/billing/invoices/{uuid.uuid4()}/issue",
            headers={"Authorization": "Bearer x"},
        )
        assert r.status_code == 403, r.text

    def test_billing_internal_alone_cannot_void(self, app_client, billing_internal_only):
        r = app_client.post(
            f"/api/billing/invoices/{uuid.uuid4()}/void",
            json={"reason": "a real reason"},
            headers={"Authorization": "Bearer x"},
        )
        assert r.status_code == 403, r.text


class TestRefusedBeforeAnyMoneyCodeRuns:
    """Malformed requests must die at the boundary, not inside the generator."""

    def test_a_backwards_period_is_422(self, app_client, pricing_writer):
        r = app_client.post(
            _CLOSE,
            json=_close_body(period_start="2026-07-01", period_end="2026-06-01"),
            headers={"Authorization": "Bearer x"},
        )
        assert r.status_code == 422, r.text

    def test_a_zero_length_period_is_422(self, app_client, pricing_writer):
        """period_end is EXCLUSIVE, so equal bounds cover nothing at all."""
        r = app_client.post(
            _CLOSE,
            json=_close_body(period_start="2026-06-01", period_end="2026-06-01"),
            headers={"Authorization": "Bearer x"},
        )
        assert r.status_code == 422, r.text

    @pytest.mark.parametrize("reason", ["", "   ", "\t", "\n"])
    def test_a_blank_void_reason_is_refused(self, app_client, pricing_writer, reason):
        """The immutability trigger refuses a void with no reason; this is the
        same rule one layer earlier, with a better error.

        ⚠️ The whitespace cases are not padding. ``btrim/1`` in the trigger trims
        SPACE only, which is why a tab-only reason once passed the database gate
        — see the parametrised case in test_invoice_immutability.py. The route
        must not be the layer that lets one through either.
        """
        r = app_client.post(
            f"/api/billing/invoices/{uuid.uuid4()}/void",
            json={"reason": reason},
            headers={"Authorization": "Bearer x"},
        )
        assert r.status_code == 422, r.text


class TestServiceRefusalsMapToStatusCodes:
    def test_reproducing_an_unknown_invoice_is_404(self, app_client):
        """InvoiceStateError from a missing row must not surface as a 500."""
        r = app_client.get(
            f"/api/billing/invoices/{uuid.uuid4()}/reproduce",
            headers={"X-Tenant-Id": str(uuid.uuid4())},
        )
        assert r.status_code == 404, r.text


# --------------------------------------------------------------------------- #
# The happy paths — does a 200 mean the row is THERE?
#
# ⛔ EVERY READ-BACK BELOW GOES THROUGH ITS OWN SESSION, OPENED AFTER THE
# RESPONSE. That is the whole method. The request's session is the one that was
# rolling the work back, so reading through it (or through the response body)
# reports the work as done either way and the test cannot fail for the bug it is
# supposed to catch. A fresh session is a fresh transaction with a fresh
# snapshot: it sees committed rows and nothing else, which is the only
# instrument that can tell "written" from "returned".
# --------------------------------------------------------------------------- #

_FEB_START = date(2026, 2, 1)
_FEB_END = date(2026, 3, 1)


def _factory(db_engine):  # noqa: ANN001, ANN201
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


async def _seed(db_engine, *rows) -> None:  # noqa: ANN001
    """Commit fixture rows on a session of their own, so nothing the route does
    (or fails to do) can be confused with the fixture's own transaction."""
    async with _factory(db_engine)() as session:
        for row in rows:
            session.add(row)
        await session.commit()


async def _read_invoice(db_engine, invoice_id) -> BillingInvoice | None:  # noqa: ANN001
    async with _factory(db_engine)() as session:
        return (
            await session.execute(select(BillingInvoice).where(BillingInvoice.id == invoice_id))
        ).scalar_one_or_none()


async def _read_invoices_for(db_engine, tenant_id: str) -> list[BillingInvoice]:
    async with _factory(db_engine)() as session:
        return list(
            (
                await session.execute(
                    select(BillingInvoice).where(BillingInvoice.tenant_id == tenant_id)
                )
            )
            .scalars()
            .all()
        )


async def _read_audit(db_engine, action: str) -> list[AuditLog]:
    async with _factory(db_engine)() as session:
        return list(
            (await session.execute(select(AuditLog).where(AuditLog.action == action)))
            .scalars()
            .all()
        )


def _full_month_cost_rows(tenant_id: str, license_id: uuid.UUID) -> list[CostDaily]:
    """One CostDaily row per calendar day of Feb 2026.

    ``assert_no_bucket_gap`` refuses a period with even one absent day, so there
    is no smaller fixture that reaches the real close. Mirrors
    ``tests/test_invoices.py::_seed_full_month`` — February because 2026 is not a
    leap year, i.e. the fewest days this gate can be satisfied with.
    """
    rows = []
    d = _FEB_START
    while d < _FEB_END:
        rows.append(
            CostDaily(
                tenant_id=tenant_id,
                license_id=license_id,
                service_key="workers",
                provider="test",
                bucket_date=d,
                total_input_tokens=1_000,
                total_output_tokens=200,
                total_cost_usd=Decimal("2.500000"),
                request_count=10,
            )
        )
        d += timedelta(days=1)
    return rows


def _invoice_row(
    tenant_id: str, *, status: str, invoice_number: str | None = None
) -> BillingInvoice:
    """The minimum valid ``billing_invoices`` row, seeded directly.

    Deliberately bypasses ``close_period`` for the /issue and /void tests: those
    are about whether the ROUTE commits the state transition, and routing them
    through a real close would make them depend on the generator, the licences
    stub and the month-long bucket-gap fixture as well. The immutability trigger
    is ``BEFORE UPDATE OR DELETE`` (``models/db/invoice_immutability.py``), so a
    direct INSERT of an already-issued row is permitted — it is the UPDATE the
    routes perform that the trigger polices, and that is left real.
    """
    return BillingInvoice(
        tenant_id=tenant_id,
        tenant_name="acme",
        period_start=_FEB_START,
        period_end=_FEB_END,
        status=status,
        invoice_number=invoice_number,
        issued_at=datetime(2026, 3, 1, tzinfo=UTC) if status == "issued" else None,
        subtotal_usd=Decimal("0"),
        total_usd=Decimal("0"),
        internal_cost_usd=Decimal("0"),
        margin_usd=Decimal("0"),
        billing_mode="standard_markup",
        markup_pct=Decimal("30.00"),
        effective_markup_pct=Decimal("30.0000"),
        snapshot={},
        inputs={},
        snapshot_sha256="0" * 64,
        generator_version="invoice.v1",
    )


@pytest.fixture
def licences_answer_byo_mode(monkeypatch):
    """Answer the licences ``byo-mode`` endpoint; delegate every other GET.

    ``assert_byo_mode_available`` refuses a close whose billing mode licences
    cannot state, so a real close cannot be reached without this. URL-scoped and
    delegating rather than a blanket stub, for the reason given at the same
    fixture in ``tests/test_invoices.py``: a stub that answered everything could
    silently repair an expectation some other test in this file depends on being
    broken. The TTL cache is cleared both sides — it is module state, so one
    test's answer must not decide another's.
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


class TestAMutationSurvivesTheResponse:
    """⛔ THE REGRESSION GATE FOR THE MISSING ROUTER COMMITS.

    Each test states the mutation that reds it. Those mutations are one-line
    deletions in ``src/routers/billing.py`` and they are the point: before this
    suite existed, all three lines were absent and the whole file was green.
    """

    async def test_close_persists_the_invoice_whose_number_it_returned(
        self, db_engine, app_client, pricing_writer, licences_answer_byo_mode
    ):
        """⛔ MUTATION CONTROL: delete ``await session.commit()`` in
        ``routers/billing.py::close_invoice`` (line ~553) and this test fails on
        ``persisted != []`` — the response is still a 200 carrying an
        ``invoice_number``, which is exactly why asserting the body proves
        nothing.
        """
        tenant = T("invoice-route-close-persists")
        license_id = uuid.uuid4()
        await _seed(
            db_engine,
            MarkupRule(
                tenant_id=tenant,
                markup_pct=Decimal("30.00"),
                effective_from=datetime(2025, 1, 1, tzinfo=UTC),
                effective_until=None,
            ),
            *_full_month_cost_rows(tenant, license_id),
        )

        # Same shape as tests/test_invoices.py::_license_payload — the payload
        # the real close is already known to accept.
        licences = [
            {
                "id": str(license_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": None},
            }
        ]
        # Both bindings: services.invoices holds close_period's pre-fetch,
        # services.billing holds the generator's. close_period passes its
        # pre-fetched payload INTO generate_billing, so the second should be
        # driven zero times — it is patched anyway so that a regression in the
        # single-fetch property fails on a call count rather than on a live HTTP
        # call from the test suite.
        close_fetch = patch(
            "services.invoices.fetch_licenses", new_callable=AsyncMock, return_value=licences
        )
        gen_fetch = patch(
            "services.billing.fetch_licenses", new_callable=AsyncMock, return_value=licences
        )
        with close_fetch, gen_fetch:
            r = app_client.post(
                _CLOSE,
                json={
                    "tenant_id": tenant,
                    "period_start": _FEB_START.isoformat(),
                    "period_end": _FEB_END.isoformat(),
                },
                headers={"Authorization": "Bearer x"},
            )

        assert r.status_code == 200, r.text
        reported = r.json()["data"]
        assert reported["invoice_number"], "the route reported no number at all"

        # ⛔ THE ASSERTION THAT MATTERS. Fresh session, fresh transaction.
        persisted = await _read_invoices_for(db_engine, tenant)
        assert persisted != [], (
            "the close returned 200 with invoice_number "
            f"{reported['invoice_number']!r} and billing_invoices is EMPTY — the "
            "route never committed, so the caller holds a number for a statement "
            "that does not exist"
        )
        assert len(persisted) == 1
        row = persisted[0]
        assert str(row.id) == reported["id"]
        assert row.invoice_number == reported["invoice_number"]
        # issue_immediately defaults True, so the counter draw must be durable too.
        assert row.status == "issued"
        assert row.issued_at is not None

        # The audit row rides the SAME transaction (services/audit.py only
        # flushes), so it was being lost with the invoice.
        audit_rows = await _read_audit(db_engine, "billing.invoice.close")
        assert len(audit_rows) == 1, "the audit row for the close did not survive either"
        assert audit_rows[0].resource_id == reported["id"]
        assert audit_rows[0].actor_sub == "test-pricing-writer"

    async def test_issue_persists_the_number_it_allocated(
        self, db_engine, app_client, pricing_writer
    ):
        """⛔ MUTATION CONTROL: delete ``await session.commit()`` in
        ``routers/billing.py::issue_invoice`` (line ~613) and this test fails on
        ``row.status == "issued"`` — the row is re-read as ``draft`` with a NULL
        ``invoice_number`` while the response body carries the number.
        """
        tenant = T("invoice-route-issue-persists")
        draft = _invoice_row(tenant, status="draft")
        await _seed(db_engine, draft)
        invoice_id = draft.id

        r = app_client.post(
            f"/api/billing/invoices/{invoice_id}/issue",
            headers={"Authorization": "Bearer x"},
        )
        assert r.status_code == 200, r.text
        reported_number = r.json()["data"]["invoice_number"]
        assert reported_number, "the route reported no number at all"

        row = await _read_invoice(db_engine, invoice_id)
        assert row is not None
        assert row.status == "issued", (
            f"the route returned 200 with invoice_number {reported_number!r} but the "
            f"row is still {row.status!r} — the number exists only in the response"
        )
        assert row.invoice_number == reported_number
        assert row.issued_at is not None

        audit_rows = await _read_audit(db_engine, "billing.invoice.issue")
        assert len(audit_rows) == 1, "the audit row for the issue did not survive either"
        assert audit_rows[0].resource_id == str(invoice_id)

    async def test_void_persists_the_terminal_status_it_reported(
        self, db_engine, app_client, pricing_writer
    ):
        """⛔ MUTATION CONTROL: delete ``await session.commit()`` in
        ``routers/billing.py::void_billing_invoice`` (line ~674) and this test
        fails on ``row.status == "void"`` — the statement is re-read as ``issued``
        and still live, with no recorded reason, while the caller was told it was
        withdrawn.
        """
        tenant = T("invoice-route-void-persists")
        issued = _invoice_row(tenant, status="issued", invoice_number="NA-2026-009001")
        await _seed(db_engine, issued)
        invoice_id = issued.id

        r = app_client.post(
            f"/api/billing/invoices/{invoice_id}/void",
            json={"reason": "superseded by a corrected statement"},
            headers={"Authorization": "Bearer x"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["data"]["status"] == "void"

        row = await _read_invoice(db_engine, invoice_id)
        assert row is not None
        assert row.status == "void", (
            f"the route returned status 'void' but the row is still {row.status!r} — "
            "the statement is live, still holds the period against "
            "ux_billing_invoices_open_period, and cannot be re-closed"
        )
        assert row.void_reason == "superseded by a corrected statement"
        assert row.voided_at is not None
        # The number is NOT returned to the series: gapless means gapless in
        # allocation, and a void row keeps its number so a reader can see the
        # statement was superseded rather than destroyed.
        assert row.invoice_number == "NA-2026-009001"

        audit_rows = await _read_audit(db_engine, "billing.invoice.void")
        assert len(audit_rows) == 1, "the audit row for the void did not survive either"
        assert audit_rows[0].resource_id == str(invoice_id)


class TestAnUnpriceableProviderCostIsARefusalNotACrash:
    """``UnpricedProviderCostError`` -> 409.

    Lives down here rather than in ``TestServiceRefusalsMapToStatusCodes`` because
    it needs the full-month cost fixture and the licences stub that the happy-path
    block defines — reaching this gate means reaching a REAL close, since it is the
    fourth gate in ``close_period`` and the three before it run first.

    ⚠️ 409 vs 500 IS THE WHOLE POINT. The service is refusing on purpose: a
    provider whose events no ``provider_pricing`` row could price is stored by the
    rollup as a hard $0.000000, and freezing that would understate internal cost
    and overstate margin on an immutable, gaplessly-numbered row. A 500 would tell
    the operator the billing service is broken and give them nothing to fix; a 409
    with the date and provider in the detail tells them which ``provider_pricing``
    row to add.
    """

    async def test_an_unpriced_provider_cost_is_409_and_writes_no_invoice(
        self, db_engine, app_client, pricing_writer, licences_answer_byo_mode
    ):
        """⛔ MUTATION CONTROL: remove ``UnpricedProviderCostError`` from the
        ``except (BucketGapError, UnpricedServiceKeyError, UnpricedProviderCostError)``
        tuple in ``routers/billing.py::close_invoice``. The refusal then escapes as
        an unhandled exception and this test fails on the status code — 500 (or a
        raised error out of the test client) instead of 409.
        """
        tenant = T("invoice-route-unpriced-provider")
        license_id = uuid.uuid4()
        unpriced_date = _FEB_START + timedelta(days=9)
        await _seed(
            db_engine,
            MarkupRule(
                tenant_id=tenant,
                markup_pct=Decimal("30.00"),
                effective_from=datetime(2025, 1, 1, tzinfo=UTC),
                effective_until=None,
            ),
            *_full_month_cost_rows(tenant, license_id),
            # The shape the rollup writes for spend it could not price: a hard
            # $0.000000 beside a real token count, with unpriced_event_count as the
            # only column that still knows the difference from a free provider. A
            # distinct `provider` keeps it a separate uq_cost_daily group.
            CostDaily(
                tenant_id=tenant,
                license_id=license_id,
                service_key="workers",
                provider="openai_compatible",
                bucket_date=unpriced_date,
                total_input_tokens=6_000,
                total_output_tokens=87,
                total_cost_usd=Decimal("0.000000"),
                request_count=4,
                unpriced_event_count=3,
            ),
        )

        licences = [
            {
                "id": str(license_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": None},
            }
        ]
        # The gate refuses BEFORE close_period's licences fetch, so these should be
        # driven zero times; patched anyway so that a reordering fails on this
        # assertion rather than on a live HTTP call from the suite.
        close_fetch = patch(
            "services.invoices.fetch_licenses", new_callable=AsyncMock, return_value=licences
        )
        gen_fetch = patch(
            "services.billing.fetch_licenses", new_callable=AsyncMock, return_value=licences
        )
        with close_fetch as mock_close_fetch, gen_fetch:
            r = app_client.post(
                _CLOSE,
                json={
                    "tenant_id": tenant,
                    "period_start": _FEB_START.isoformat(),
                    "period_end": _FEB_END.isoformat(),
                },
                headers={"Authorization": "Bearer x"},
            )

        assert r.status_code == 409, r.text
        # ``main._http_exception_handler`` puts a STRING ``HTTPException.detail`` at
        # ``error.message`` (a dict detail would go to ``error.details`` instead), so
        # this is where the refusal's own text lands — and it has to arrive intact,
        # because the operator-actionable part of this refusal IS the text.
        message = r.json()["error"]["message"]
        assert "openai_compatible" in message, "the 409 body does not name the provider"
        assert unpriced_date.isoformat() in message, "the 409 body does not name the date"
        assert "provider_pricing" in message, "the 409 body does not name the table to fix"
        assert mock_close_fetch.call_count == 0

        # Fresh session: a refusal must leave NOTHING behind — no draft, and no
        # number drawn from the gapless series.
        assert await _read_invoices_for(db_engine, tenant) == []
