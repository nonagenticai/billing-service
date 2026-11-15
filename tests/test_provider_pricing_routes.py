"""CRUD endpoints for /api/config/provider-pricing — Phase 4."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import AuditLog, ProviderPricing
from models.domain import ProviderPricingCreate


@pytest.fixture
def admin_actor(monkeypatch):
    """Force require_admin to a known JWT payload (Keycloak-shaped)."""
    fake_payload = {
        "sub": "test-admin-user",
        "realm_access": {"roles": ["license-admin"]},
    }

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


async def _seed_pricing(db_engine, **kwargs) -> ProviderPricing:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        row = ProviderPricing(**kwargs)
        s.add(row)
        await s.commit()
        await s.refresh(row)
        return row


async def _fetch_audit_rows(db_engine, *, action: str | None = None) -> list[AuditLog]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        stmt = select(AuditLog)
        if action is not None:
            stmt = stmt.where(AuditLog.action == action)
        stmt = stmt.order_by(AuditLog.ts.desc())
        return list((await s.execute(stmt)).scalars().all())


async def _fetch_pricing_rows(db_engine, *, provider: str) -> list[ProviderPricing]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        stmt = (
            select(ProviderPricing)
            .where(ProviderPricing.provider == provider)
            .order_by(ProviderPricing.effective_from)
        )
        return list((await s.execute(stmt)).scalars().all())


@pytest.mark.unit
class TestUpsertProviderPricing:
    async def test_creates_first_row(self, app_client: TestClient, db_engine, admin_actor) -> None:
        body = {
            "provider": "anthropic",
            "model": "claude-3-5-sonnet-20241022",
            "cost_model": "per_token",
            "input_per_1m_usd": "3",
            "output_per_1m_usd": "15",
            "cache_creation_per_1m_usd": "3.75",
            "cache_read_per_1m_usd": "0.30",
            "effective_from": "2026-04-01T00:00:00+00:00",
        }
        resp = app_client.post(
            "/api/config/provider-pricing",
            headers={"Authorization": "Bearer fake"},
            json=body,
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()["data"]
        assert data["provider"] == "anthropic"
        assert data["model"] == "claude-3-5-sonnet-20241022"
        assert Decimal(data["input_per_1m_usd"]) == Decimal("3")
        assert Decimal(data["cache_creation_per_1m_usd"]) == Decimal("3.75")
        assert data["effective_until"] is None

        # Audit trail
        rows = await _fetch_audit_rows(db_engine, action="config.pricing.upsert")
        assert len(rows) == 1
        audit = rows[0]
        assert audit.before is None
        assert audit.after is not None
        assert audit.after["provider"] == "anthropic"

    async def test_closes_prior_open_row(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        await _seed_pricing(
            db_engine,
            provider="anthropic",
            model="claude-3-5-sonnet-20241022",
            cost_model="per_token",
            input_per_1m_usd=Decimal("3"),
            output_per_1m_usd=Decimal("15"),
            effective_from=datetime(2026, 1, 1, tzinfo=UTC),
        )

        new_from = "2026-04-01T00:00:00+00:00"
        resp = app_client.post(
            "/api/config/provider-pricing",
            headers={"Authorization": "Bearer fake"},
            json={
                "provider": "anthropic",
                "model": "claude-3-5-sonnet-20241022",
                "cost_model": "per_token",
                "input_per_1m_usd": "4",
                "output_per_1m_usd": "18",
                "effective_from": new_from,
            },
        )
        assert resp.status_code == 201, resp.text

        rows = await _fetch_pricing_rows(db_engine, provider="anthropic")
        assert len(rows) == 2
        old, new = rows
        # Old row is now closed.
        assert old.effective_until is not None
        # New row is open.
        assert new.effective_until is None
        assert new.input_per_1m_usd == Decimal("4")

        # Audit captured before+after.
        audit_rows = await _fetch_audit_rows(db_engine, action="config.pricing.upsert")
        assert len(audit_rows) == 1
        assert audit_rows[0].before is not None
        assert Decimal(audit_rows[0].before["input_per_1m_usd"]) == Decimal("3")
        assert audit_rows[0].after is not None
        assert Decimal(audit_rows[0].after["input_per_1m_usd"]) == Decimal("4")

    async def test_non_admin_rejected(self, app_client: TestClient, db_engine, monkeypatch) -> None:
        import auth

        async def _fake_decode(_request):
            return {"sub": "tenant-user", "realm_access": {"roles": ["customer"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)

        resp = app_client.post(
            "/api/config/provider-pricing",
            headers={"Authorization": "Bearer fake"},
            json={
                "provider": "anthropic",
                "cost_model": "per_token",
                "input_per_1m_usd": "3",
                "output_per_1m_usd": "15",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 403


@pytest.mark.unit
class TestProviderPricingRowValidation:
    """A malformed row must be rejected at the door — a per_token row with no
    rates prices real usage at $0.00 and looks like a successful charge."""

    @pytest.mark.parametrize(
        "body",
        [
            {
                "provider": "anthropic",
                "model": "claude-3-5-sonnet-20241022",
                "cost_model": "per_token",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
            {
                "provider": "anthropic",
                "cost_model": "per_request",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
            {
                "provider": "selfhosted",
                "cost_model": "self_hosted_amortized",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
        ],
        ids=["per_token_no_rates", "per_request_no_price", "self_hosted_no_monthly"],
    )
    async def test_malformed_row_rejected(
        self, app_client: TestClient, db_engine, admin_actor, body
    ) -> None:
        resp = app_client.post(
            "/api/config/provider-pricing",
            headers={"Authorization": "Bearer fake"},
            json=body,
        )
        assert resp.status_code == 422, resp.text
        # The field-level reason now reaches the caller over HTTP. Until
        # 2026-08-11 it did not: _validation_exception_handler computed
        # `str(exc.errors())` and passed neither `leak_details` nor the
        # LEAK_EXCEPTION_DETAILS flag to `_error_envelope`, so the wire body
        # carried only "Request validation failed" and this assertion could
        # only be made against the model (test_malformed_row_message_names_field
        # below, which is kept as the model-level half).
        details = resp.json()["error"]["details"]
        assert isinstance(details, list) and details
        assert all(set(e) == {"field", "message", "type"} for e in details), details
        # Nothing was written.
        assert await _fetch_pricing_rows(db_engine, provider=body["provider"]) == []

    @pytest.mark.parametrize(
        ("kwargs", "expect_field"),
        [
            (
                {"provider": "anthropic", "cost_model": "per_token"},
                "input_per_1m_usd",
            ),
            (
                {"provider": "anthropic", "cost_model": "per_request"},
                "per_request_usd",
            ),
            (
                {"provider": "selfhosted", "cost_model": "self_hosted_amortized"},
                "monthly_fixed_cost_usd",
            ),
        ],
        ids=["per_token_no_rates", "per_request_no_price", "self_hosted_no_monthly"],
    )
    def test_malformed_row_message_names_field(self, kwargs, expect_field) -> None:
        """The raised message must name the offending field AND the cost_model.

        The seeder (scripts/seed_initial_pricing.py) surfaces the pydantic
        error text, so this is what makes a failing seed run diagnosable
        without attaching a debugger.
        """
        with pytest.raises(ValidationError) as exc:
            ProviderPricingCreate(effective_from=datetime(2026, 4, 1, tzinfo=UTC), **kwargs)
        rendered = str(exc.value)
        assert expect_field in rendered
        assert kwargs["cost_model"] in rendered

    async def test_unknown_cost_model_rejected(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        """A typo used to fall through to the per_token branch and price $0."""
        resp = app_client.post(
            "/api/config/provider-pricing",
            headers={"Authorization": "Bearer fake"},
            json={
                "provider": "anthropic",
                "cost_model": "per_tokens",
                "input_per_1m_usd": "3",
                "output_per_1m_usd": "15",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 422, resp.text
        assert await _fetch_pricing_rows(db_engine, provider="anthropic") == []

    @pytest.mark.parametrize(
        "body",
        [
            {
                "provider": "goodtoken",
                "cost_model": "per_token",
                "input_per_1m_usd": "3",
                "output_per_1m_usd": "15",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
            # Output-only is legitimate (some providers bill output only).
            {
                "provider": "goodtoken",
                "cost_model": "per_token",
                "output_per_1m_usd": "15",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
            {
                "provider": "goodreq",
                "cost_model": "per_request",
                "per_request_usd": "0.002",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
            {
                "provider": "goodself",
                "cost_model": "self_hosted_amortized",
                "monthly_fixed_cost_usd": "1200",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
        ],
        ids=["per_token_both", "per_token_output_only", "per_request", "self_hosted_amortized"],
    )
    async def test_valid_row_of_each_cost_model_still_accepted(
        self, app_client: TestClient, db_engine, admin_actor, body
    ) -> None:
        """Guard against over-rejecting — every legitimate shape must survive."""
        resp = app_client.post(
            "/api/config/provider-pricing",
            headers={"Authorization": "Bearer fake"},
            json=body,
        )
        assert resp.status_code == 201, resp.text
        assert resp.json()["data"]["cost_model"] == body["cost_model"]

    async def test_zero_rate_row_is_accepted(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        """An explicit 0 is a deliberate free tier, not a malformed row — the
        validator rejects NULL/absent rates, never a stated zero."""
        resp = app_client.post(
            "/api/config/provider-pricing",
            headers={"Authorization": "Bearer fake"},
            json={
                "provider": "freeprov",
                "cost_model": "per_token",
                "input_per_1m_usd": "0",
                "output_per_1m_usd": "0",
                "effective_from": "2026-04-01T00:00:00+00:00",
            },
        )
        assert resp.status_code == 201, resp.text


@pytest.mark.unit
class TestListCurrentProviderPricing:
    async def test_lists_only_open_rows(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        # closed
        await _seed_pricing(
            db_engine,
            provider="anthropic",
            model=None,
            cost_model="per_token",
            input_per_1m_usd=Decimal("2"),
            output_per_1m_usd=Decimal("10"),
            effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=datetime(2026, 4, 1, tzinfo=UTC),
        )
        # open default
        await _seed_pricing(
            db_engine,
            provider="anthropic",
            model=None,
            cost_model="per_token",
            input_per_1m_usd=Decimal("3"),
            output_per_1m_usd=Decimal("15"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )
        # open per-model
        await _seed_pricing(
            db_engine,
            provider="anthropic",
            model="claude-3-5-sonnet-20241022",
            cost_model="per_token",
            input_per_1m_usd=Decimal("3"),
            output_per_1m_usd=Decimal("15"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )

        resp = app_client.get(
            "/api/config/provider-pricing",
            headers={"Authorization": "Bearer fake"},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert len(data) == 2
        # Provider-default sorts first.
        assert data[0]["model"] is None
        assert data[1]["model"] == "claude-3-5-sonnet-20241022"


@pytest.mark.unit
class TestProviderPricingHistory:
    async def test_lists_all_rows_descending(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        await _seed_pricing(
            db_engine,
            provider="anthropic",
            model=None,
            cost_model="per_token",
            input_per_1m_usd=Decimal("2"),
            output_per_1m_usd=Decimal("10"),
            effective_from=datetime(2026, 1, 1, tzinfo=UTC),
            effective_until=datetime(2026, 4, 1, tzinfo=UTC),
        )
        await _seed_pricing(
            db_engine,
            provider="anthropic",
            model=None,
            cost_model="per_token",
            input_per_1m_usd=Decimal("3"),
            output_per_1m_usd=Decimal("15"),
            effective_from=datetime(2026, 4, 1, tzinfo=UTC),
        )

        resp = app_client.get(
            "/api/config/provider-pricing/history",
            params={"provider": "anthropic"},
            headers={"Authorization": "Bearer fake"},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert len(data) == 2
        # Newest first.
        assert Decimal(data[0]["input_per_1m_usd"]) == Decimal("3")
        assert Decimal(data[1]["input_per_1m_usd"]) == Decimal("2")
