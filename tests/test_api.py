"""Tests for API endpoints using TestClient with real DB integration."""

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import quote

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import (
    NULL_LICENSE_ID,
    AppCostRate,
    CostDaily,
    CostEvent,
    MeteringDaily,
    MeteringHourly,
    MeteringMonthlyUsage,
    ProviderCost,
    TenantConfig,
    TokenUsage,
)
from tests.conftest import T

# --- Helpers ---


async def _seed_daily_usage(
    db_engine,
    tenant_id=T("t1"),
    service_key="workers",
    count=100,
    denied=5,
    bucket_date=None,
):
    """Insert metering_daily rows for testing usage/billing endpoints."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    lic_id = uuid.uuid4()
    app_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    day = bucket_date or datetime.now(UTC).date()
    async with factory() as session:
        session.add(
            MeteringDaily(
                tenant_id=tenant_id,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key=service_key,
                bucket_date=day,
                request_count=count,
                denied_count=denied,
            )
        )
        await session.commit()
    return lic_id, app_id, plan_id


async def _seed_cost_daily(
    db_engine,
    tenant_id=T("t1"),
    provider="claude",
    license_id=None,
    input_tokens=50000,
    output_tokens=10000,
    cost_usd=1.50,
    request_count=25,
    bucket_date=None,
):
    """Insert cost_daily rows for testing cost/billing endpoints."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    lic_id = license_id or uuid.uuid4()
    day = bucket_date or datetime.now(UTC).date()
    async with factory() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                license_id=lic_id,
                service_key="inference-router",
                provider=provider,
                bucket_date=day,
                total_input_tokens=input_tokens,
                total_output_tokens=output_tokens,
                total_cost_usd=cost_usd,
                request_count=request_count,
            )
        )
        await session.commit()
    return lic_id


async def _seed_cost_event(
    db_engine,
    *,
    tenant_id=T("t1"),
    correlation_id=None,
    provider="claude",
    served_model="claude-sonnet-4-20250514",
    input_tokens=1000,
    output_tokens=200,
    cost_usd="0.0165",
    stage=None,
):
    """Insert one raw cost_events row for by-correlation read tests."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    cid = correlation_id or uuid.uuid4()
    async with factory() as session:
        session.add(
            CostEvent(
                tenant_id=tenant_id,
                service_key="inference-router",
                provider=provider,
                served_model=served_model,
                model=served_model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost_usd=Decimal(cost_usd),
                correlation_id=cid,
                extra_metadata=({"stage": stage} if stage else None),
            )
        )
        await session.commit()
    return cid


async def _seed_hourly_usage(db_engine, tenant_id=T("t1")):
    """Insert metering_hourly rows."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    lic_id = uuid.uuid4()
    app_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    now = datetime.now(UTC)
    bucket = now.replace(minute=0, second=0, microsecond=0)
    async with factory() as session:
        session.add(
            MeteringHourly(
                tenant_id=tenant_id,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_hour=bucket,
                request_count=50,
                denied_count=2,
            )
        )
        await session.commit()
    return lic_id, app_id, plan_id


async def _seed_monthly_usage(db_engine, license_id, plan_id, count=200, subject_id=None):
    """Insert metering_monthly_usage row for quota tests.

    ``subject_id`` defaults to the model's ``NULL_SUBJECT_ID``. The uniqueness
    grain is (license_id, subject_id, plan_id, month_start), so passing distinct
    subjects seeds the multi-row-per-license shape the event flusher really
    writes.
    """
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime.now(UTC)
    month_start = date(now.year, now.month, 1)
    kwargs = {} if subject_id is None else {"subject_id": subject_id}
    async with factory() as session:
        session.add(
            MeteringMonthlyUsage(
                license_id=license_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=count,
                **kwargs,
            )
        )
        await session.commit()


async def _seed_token_usage(db_engine, tenant_id, license_id=None, inp=5000, outp=3000):
    """Insert token_usage row for budget tests."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime.now(UTC)
    month_start = date(now.year, now.month, 1)
    effective_lic = license_id or NULL_LICENSE_ID
    async with factory() as session:
        session.add(
            TokenUsage(
                tenant_id=tenant_id,
                license_id=effective_lic,
                period_type="monthly",
                period_start=month_start,
                input_tokens=inp,
                output_tokens=outp,
                total_tokens=inp + outp,
            )
        )
        await session.commit()


async def _seed_provider_cost(db_engine, provider="claude", inp_rate=3.0, outp_rate=15.0):
    """Insert provider_costs row."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            ProviderCost(
                provider=provider,
                input_per_1m_usd=inp_rate,
                output_per_1m_usd=outp_rate,
            )
        )
        await session.commit()


async def _seed_app_cost_rate(db_engine, app_key="workers", cost_per_request=0.001):
    """Insert app_cost_rates row."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            AppCostRate(
                app_key=app_key,
                cost_per_request_usd=cost_per_request,
            )
        )
        await session.commit()


async def _seed_tenant_config(db_engine, tenant_id, markup_pct=20.0, notes=None):
    """Insert tenant_config row."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            TenantConfig(
                tenant_id=tenant_id,
                markup_pct=Decimal(str(markup_pct)),
                notes=notes,
            )
        )
        await session.commit()


# --- Health ---


class TestHealthEndpoints:
    @pytest.mark.unit
    def test_readiness_check(self, app_client):
        """Readiness probe reports database and redis status.

        The status code must AGREE with the verdict: 200 for "ready", 503 for
        "not_ready". This previously asserted a flat 200 regardless, which is
        what let /health/ready answer 200 while both dependencies were down —
        invisible to the httpGet readinessProbe, which reads only the code.
        """
        response = app_client.get("/health/ready")
        data = response.json()
        assert data["status"] in ("ready", "not_ready")
        expected = 200 if data["status"] == "ready" else 503
        assert response.status_code == expected, (
            f'status "{data["status"]}" must be served as HTTP {expected}, '
            f"got {response.status_code}"
        )
        assert "database" in data
        assert "redis" in data
        assert data["database"] in ("connected", "disconnected")
        assert data["redis"] in ("connected", "disconnected")

    @pytest.mark.unit
    def test_nonexistent_endpoint_404(self, app_client):
        """Request to undefined endpoint should return 404."""
        response = app_client.get("/api/nonexistent")
        assert response.status_code == 404


# --- Config ---


class TestConfigEndpoints:
    @pytest.mark.unit
    def test_get_default_markup_returns_env_default(self, app_client):
        """Default markup should return the settings default when no DB override exists."""
        response = app_client.get("/api/config/markup")
        assert response.status_code == 200
        data = response.json()["data"]
        assert "default_markup_pct" in data
        assert isinstance(data["default_markup_pct"], float)
        # Read the setting, not a literal: the default moved 30 -> 20 on
        # 2026-09-10 and this test is about the ENDPOINT echoing settings, not
        # about the rate. tests/test_infra.py pins the literal value.
        from settings import get_settings

        assert data["default_markup_pct"] == get_settings().default_markup_pct

    @pytest.mark.unit
    def test_update_default_markup(self, app_client):
        """PUT markup should persist the new value."""
        response = app_client.put("/api/config/markup", json={"default_markup_pct": 25.0})
        assert response.status_code == 200
        assert response.json()["data"]["default_markup_pct"] == 25.0

        # Verify persistence via GET
        response = app_client.get("/api/config/markup")
        assert response.json()["data"]["default_markup_pct"] == 25.0

    @pytest.mark.unit
    def test_update_default_markup_idempotent(self, app_client):
        """PUT markup twice should update in-place, not create duplicates."""
        app_client.put("/api/config/markup", json={"default_markup_pct": 10.0})
        app_client.put("/api/config/markup", json={"default_markup_pct": 15.0})
        response = app_client.get("/api/config/markup")
        assert response.json()["data"]["default_markup_pct"] == 15.0

    @pytest.mark.unit
    def test_update_default_markup_zero(self, app_client):
        """Zero markup should be valid (ge=0 constraint)."""
        response = app_client.put("/api/config/markup", json={"default_markup_pct": 0.0})
        assert response.status_code == 200
        assert response.json()["data"]["default_markup_pct"] == 0.0

    @pytest.mark.unit
    def test_update_default_markup_negative_rejected(self, app_client):
        """Negative markup should fail validation (ge=0)."""
        response = app_client.put("/api/config/markup", json={"default_markup_pct": -1.0})
        assert response.status_code == 422

    @pytest.mark.unit
    def test_update_default_markup_missing_field(self, app_client):
        """PUT markup without required field should return 422."""
        response = app_client.put("/api/config/markup", json={})
        assert response.status_code == 422

    @pytest.mark.unit
    def test_list_providers_empty(self, app_client):
        """Provider list should be empty initially."""
        response = app_client.get("/api/config/providers")
        assert response.status_code == 200
        assert response.json()["data"] == []

    @pytest.mark.unit
    def test_create_provider_cost(self, app_client):
        """PUT should create a new provider cost rate."""
        response = app_client.put(
            "/api/config/providers/claude",
            json={"input_per_1m_usd": 3.0, "output_per_1m_usd": 15.0},
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["provider"] == "claude"
        assert float(data["input_per_1m_usd"]) == 3.0
        assert float(data["output_per_1m_usd"]) == 15.0

    @pytest.mark.unit
    def test_update_provider_cost_persists_in_list(self, app_client):
        """Created provider should appear in the list endpoint."""
        app_client.put(
            "/api/config/providers/openai-test",
            json={"input_per_1m_usd": 5.0, "output_per_1m_usd": 15.0},
        )
        response = app_client.get("/api/config/providers")
        assert response.status_code == 200
        providers = response.json()["data"]
        names = [p["provider"] for p in providers]
        assert "openai-test" in names

    @pytest.mark.unit
    def test_update_provider_cost_idempotent(self, app_client):
        """PUT same provider twice should update the existing row."""
        app_client.put(
            "/api/config/providers/openai",
            json={"input_per_1m_usd": 5.0, "output_per_1m_usd": 15.0},
        )
        resp = app_client.put(
            "/api/config/providers/openai",
            json={"input_per_1m_usd": 8.0, "output_per_1m_usd": 20.0},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert float(data["input_per_1m_usd"]) == 8.0
        assert float(data["output_per_1m_usd"]) == 20.0

    @pytest.mark.unit
    def test_update_provider_cost_negative_rejected(self, app_client):
        """Negative cost rates should fail validation (ge=0)."""
        response = app_client.put(
            "/api/config/providers/bad",
            json={"input_per_1m_usd": -1.0, "output_per_1m_usd": 5.0},
        )
        assert response.status_code == 422

    @pytest.mark.unit
    def test_update_provider_cost_zero_valid(self, app_client):
        """Zero cost rates should be accepted."""
        response = app_client.put(
            "/api/config/providers/free-tier",
            json={"input_per_1m_usd": 0.0, "output_per_1m_usd": 0.0},
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert float(data["input_per_1m_usd"]) == 0.0
        assert float(data["output_per_1m_usd"]) == 0.0

    @pytest.mark.unit
    def test_list_apps_empty(self, app_client):
        """App cost rates list should be empty initially."""
        response = app_client.get("/api/config/apps")
        assert response.status_code == 200
        assert response.json()["data"] == []

    @pytest.mark.unit
    def test_create_app_cost_rate(self, app_client):
        """PUT should create a new app cost rate."""
        response = app_client.put(
            "/api/config/apps/workers",
            json={"cost_per_request_usd": 0.001},
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["app_key"] == "workers"
        assert float(data["cost_per_request_usd"]) == 0.001

    @pytest.mark.unit
    def test_update_app_cost_rate_persists_in_list(self, app_client):
        """Created app rate should appear in the list endpoint."""
        app_client.put(
            "/api/config/apps/kanban-test",
            json={"cost_per_request_usd": 0.002},
        )
        response = app_client.get("/api/config/apps")
        apps = response.json()["data"]
        keys = [a["app_key"] for a in apps]
        assert "kanban-test" in keys

    @pytest.mark.unit
    def test_update_app_rate_idempotent(self, app_client):
        """PUT same app_key twice should update the existing row."""
        app_client.put(
            "/api/config/apps/kanban",
            json={"cost_per_request_usd": 0.002},
        )
        resp = app_client.put(
            "/api/config/apps/kanban",
            json={"cost_per_request_usd": 0.005},
        )
        assert resp.status_code == 200
        assert float(resp.json()["data"]["cost_per_request_usd"]) == 0.005

    @pytest.mark.unit
    def test_update_app_rate_negative_rejected(self, app_client):
        """Negative cost per request should fail validation (ge=0)."""
        response = app_client.put(
            "/api/config/apps/bad",
            json={"cost_per_request_usd": -0.001},
        )
        assert response.status_code == 422

    @pytest.mark.unit
    def test_get_tenant_config_returns_default_for_unknown(self, app_client):
        """Unknown tenant should return default markup from settings."""
        response = app_client.get(f"/api/config/tenants/{T('new-tenant')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["tenant_id"] == T("new-tenant")
        assert data["markup_pct"] >= 0
        assert data["notes"] is None

    @pytest.mark.unit
    def test_update_tenant_config(self, app_client):
        """PUT tenant config should persist markup and notes."""
        response = app_client.put(
            f"/api/config/tenants/{T('test-t')}",
            json={"markup_pct": 20.0, "notes": "VIP customer"},
        )
        assert response.status_code == 200
        assert response.json()["data"]["markup_pct"] == 20.0
        assert response.json()["data"]["notes"] == "VIP customer"

        # Verify GET returns updated values
        response = app_client.get(f"/api/config/tenants/{T('test-t')}")
        assert response.json()["data"]["markup_pct"] == 20.0
        assert response.json()["data"]["notes"] == "VIP customer"

    @pytest.mark.unit
    def test_update_tenant_config_idempotent(self, app_client):
        """PUT same tenant twice should update, not create duplicate."""
        app_client.put(
            f"/api/config/tenants/{T('idem-t')}",
            json={"markup_pct": 10.0},
        )
        resp = app_client.put(
            f"/api/config/tenants/{T('idem-t')}",
            json={"markup_pct": 50.0, "notes": "updated"},
        )
        assert resp.status_code == 200
        assert resp.json()["data"]["markup_pct"] == 50.0
        assert resp.json()["data"]["notes"] == "updated"

    @pytest.mark.unit
    def test_update_tenant_config_invalid_markup(self, app_client):
        """Negative markup should fail validation."""
        response = app_client.put(
            f"/api/config/tenants/{T('bad-t')}",
            json={"markup_pct": -5.0},
        )
        assert response.status_code == 422

    @pytest.mark.unit
    def test_update_tenant_config_notes_null(self, app_client):
        """Setting notes to null should clear them."""
        app_client.put(
            f"/api/config/tenants/{T('null-notes-t')}",
            json={"markup_pct": 10.0, "notes": "something"},
        )
        resp = app_client.put(
            f"/api/config/tenants/{T('null-notes-t')}",
            json={"markup_pct": 10.0, "notes": None},
        )
        assert resp.status_code == 200
        assert resp.json()["data"]["notes"] is None

    @pytest.mark.unit
    def test_update_tenant_config_high_markup(self, app_client):
        """Very high markup values should still be accepted."""
        response = app_client.put(
            f"/api/config/tenants/{T('high-markup-t')}",
            json={"markup_pct": 999.99},
        )
        assert response.status_code == 200
        assert response.json()["data"]["markup_pct"] == 999.99

    @pytest.mark.unit
    def test_tenant_allowance_usage_month_13_rejected(self, app_client):
        """month=2026-13 was a 500 (ValueError in date()); now a 422 at validation."""
        r = app_client.get(f"/api/config/tenant-allowance-usage?tenant_id={T('t1')}&month=2026-13")
        assert r.status_code == 422

    @pytest.mark.unit
    def test_tenant_allowance_usage_month_00_rejected(self, app_client):
        """month=2026-00 was a 500 (ValueError in date()); now a 422 at validation."""
        r = app_client.get(f"/api/config/tenant-allowance-usage?tenant_id={T('t1')}&month=2026-00")
        assert r.status_code == 422

    @pytest.mark.unit
    def test_tenant_allowance_usage_valid_month_ok(self, app_client):
        """A valid YYYY-MM still passes validation and returns 200."""
        r = app_client.get(f"/api/config/tenant-allowance-usage?tenant_id={T('t1')}&month=2026-07")
        assert r.status_code == 200


# --- Usage ---


class TestUsageEndpoints:
    @pytest.mark.unit
    async def test_usage_by_tenant_empty(self, app_client):
        """Querying a tenant with no data should return zero totals."""
        response = app_client.get(f"/api/usage/tenant/{T('no-data')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_requests"] == 0
        assert data["total_denied"] == 0
        assert data["by_service"] == []
        assert data["by_license"] == []
        assert data["tenant_id"] == T("no-data")
        assert data["granularity"] == "daily"

    @pytest.mark.unit
    async def test_usage_by_tenant_with_data(self, db_engine, app_client):
        """Usage endpoint returns correct counts from seeded metering_daily."""
        await _seed_daily_usage(db_engine, tenant_id=T("usage-t1"))
        response = app_client.get(f"/api/usage/tenant/{T('usage-t1')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_requests"] == 100
        assert data["total_denied"] == 5
        assert len(data["by_service"]) == 1
        assert data["by_service"][0]["service_key"] == "workers"
        assert data["by_service"][0]["request_count"] == 100
        assert data["by_service"][0]["denied_count"] == 5

    @pytest.mark.unit
    async def test_usage_by_tenant_with_service_filter(self, db_engine, app_client):
        """service_key filter limits results to matching service only."""
        await _seed_daily_usage(db_engine, tenant_id=T("usage-t2"), service_key="kanban")
        response = app_client.get(f"/api/usage/tenant/{T('usage-t2')}?service_key=kanban")
        assert response.status_code == 200
        assert response.json()["data"]["total_requests"] == 100

        response = app_client.get(f"/api/usage/tenant/{T('usage-t2')}?service_key=nonexistent")
        assert response.json()["data"]["total_requests"] == 0
        assert response.json()["data"]["by_service"] == []

    @pytest.mark.unit
    async def test_usage_by_tenant_hourly(self, db_engine, app_client):
        """Hourly granularity queries metering_hourly table."""
        await _seed_hourly_usage(db_engine, tenant_id=T("usage-t3"))
        response = app_client.get(f"/api/usage/tenant/{T('usage-t3')}?granularity=hourly")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_requests"] == 50
        assert data["total_denied"] == 2
        assert data["granularity"] == "hourly"

    @pytest.mark.unit
    async def test_usage_by_tenant_invalid_granularity(self, app_client):
        """Invalid granularity value should fail validation."""
        response = app_client.get(f"/api/usage/tenant/{T('t1')}?granularity=minutely")
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_usage_by_license(self, db_engine, app_client):
        """Usage by license returns data for specific license_id."""
        lic_id, _, _ = await _seed_daily_usage(db_engine, tenant_id=T("usage-t4"))
        response = app_client.get(f"/api/usage/license/{lic_id}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_requests"] == 100
        assert data["license_id"] == str(lic_id)

    @pytest.mark.unit
    async def test_usage_by_license_empty(self, app_client):
        """Querying an unknown license_id returns zero totals."""
        fake_id = uuid.uuid4()
        response = app_client.get(f"/api/usage/license/{fake_id}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_requests"] == 0
        assert data["by_service"] == []

    @pytest.mark.unit
    async def test_usage_by_license_customer_scoped_to_own_tenant(self, db_engine, app_client):
        """A customer caller (X-Tenant-Id) must NOT read another tenant's license usage.

        The license_id is a UUID, not a secret; before the isolation fix a
        customer authenticated for tenant A could pass tenant B's license_id and
        read its per-service usage counts. With the fix the query is scoped to
        the caller's own tenant, so a foreign license yields zero.
        """
        lic_id, _, _ = await _seed_daily_usage(db_engine, tenant_id=T("owner-tenant"))

        # Customer authenticated for a DIFFERENT tenant sees nothing.
        foreign = app_client.get(
            f"/api/usage/license/{lic_id}", headers={"X-Tenant-Id": T("attacker-tenant")}
        )
        assert foreign.status_code == 200
        assert foreign.json()["data"]["total_requests"] == 0

        # The owning tenant still sees its own data.
        owner = app_client.get(
            f"/api/usage/license/{lic_id}", headers={"X-Tenant-Id": T("owner-tenant")}
        )
        assert owner.status_code == 200
        assert owner.json()["data"]["total_requests"] == 100

        # An admin JWT call (no X-Tenant-Id header) remains cross-tenant.
        admin = app_client.get(f"/api/usage/license/{lic_id}")
        assert admin.status_code == 200
        assert admin.json()["data"]["total_requests"] == 100

    @pytest.mark.unit
    async def test_usage_by_app(self, db_engine, app_client):
        """Usage by app returns data for specific app_id."""
        _, app_id, _ = await _seed_daily_usage(db_engine, tenant_id=T("usage-t5"))
        response = app_client.get(f"/api/usage/app/{app_id}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_requests"] == 100
        assert data["app_id"] == str(app_id)

    @pytest.mark.unit
    async def test_usage_by_app_with_tenant_filter(self, db_engine, app_client):
        """usage/app accepts optional tenant_id filter."""
        _, app_id, _ = await _seed_daily_usage(db_engine, tenant_id=T("usage-t5f"))
        response = app_client.get(f"/api/usage/app/{app_id}?tenant_id={T('usage-t5f')}")
        assert response.status_code == 200
        assert response.json()["data"]["total_requests"] == 100
        assert response.json()["data"]["tenant_id"] == T("usage-t5f")

        # Wrong tenant should return 0
        response = app_client.get(f"/api/usage/app/{app_id}?tenant_id={T('other')}")
        assert response.json()["data"]["total_requests"] == 0

    @pytest.mark.unit
    async def test_usage_by_app_empty(self, app_client):
        """Querying an unknown app_id returns zero totals."""
        fake_id = uuid.uuid4()
        response = app_client.get(f"/api/usage/app/{fake_id}")
        assert response.status_code == 200
        assert response.json()["data"]["total_requests"] == 0

    @pytest.mark.unit
    async def test_usage_summary(self, db_engine, app_client):
        """Summary endpoint aggregates by service for a tenant."""
        await _seed_daily_usage(db_engine, tenant_id=T("usage-t6"))
        response = app_client.get(f"/api/usage/summary?tenant_id={T('usage-t6')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_requests"] == 100
        assert data["tenant_id"] == T("usage-t6")
        assert len(data["by_service"]) == 1

    @pytest.mark.unit
    async def test_usage_summary_includes_denied(self, db_engine, app_client):
        """Summary includes denied counts from seeded data."""
        await _seed_daily_usage(db_engine, tenant_id=T("usage-t6d"))
        response = app_client.get(f"/api/usage/summary?tenant_id={T('usage-t6d')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_denied"] == 5

    @pytest.mark.unit
    async def test_usage_summary_missing_tenant_id(self, app_client):
        """Summary without required tenant_id should return 422."""
        response = app_client.get("/api/usage/summary")
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_usage_by_tenant_date_range_excludes(self, db_engine, app_client):
        """Data outside the requested date range should not be returned."""
        # Pin bucket_date to a fixed reference so the seed and the query both
        # use the same clock — a free-running datetime.now() can straddle the
        # 00:00 UTC boundary and put the seeded row outside the query window.
        now = datetime.now(UTC)
        await _seed_daily_usage(db_engine, tenant_id=T("usage-t7"), bucket_date=now.date())
        # Query a range far in the past
        response = app_client.get(
            f"/api/usage/tenant/{T('usage-t7')}",
            params={"start": "2020-01-01T00:00:00Z", "end": "2020-02-01T00:00:00Z"},
        )
        assert response.status_code == 200
        assert response.json()["data"]["total_requests"] == 0

    @pytest.mark.unit
    async def test_usage_by_tenant_date_range_includes(self, db_engine, app_client):
        """Data within the date range should be included."""
        # Pin bucket_date to the same `now` used to build the query window —
        # otherwise a midnight-UTC rollover between the seed call and `now`
        # below puts the seeded row a day outside the [start, end] window.
        now = datetime.now(UTC)
        await _seed_daily_usage(db_engine, tenant_id=T("usage-range-ok"), bucket_date=now.date())
        start = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        response = app_client.get(
            f"/api/usage/tenant/{T('usage-range-ok')}?start={start}&end={end}"
        )
        assert response.status_code == 200
        assert response.json()["data"]["total_requests"] == 100

    @pytest.mark.unit
    async def test_usage_response_has_by_license(self, db_engine, app_client):
        """The by_license breakdown should be present in usage/tenant responses."""
        lic_id, _, _ = await _seed_daily_usage(db_engine, tenant_id=T("usage-t8"))
        response = app_client.get(f"/api/usage/tenant/{T('usage-t8')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert len(data["by_license"]) == 1
        assert data["by_license"][0]["license_id"] == str(lic_id)
        assert data["by_license"][0]["request_count"] == 100

    @pytest.mark.unit
    async def test_usage_response_has_start_end(self, app_client):
        """Usage response always includes start and end timestamps."""
        response = app_client.get(f"/api/usage/tenant/{T('any')}")
        data = response.json()["data"]
        assert "start" in data
        assert "end" in data

    @pytest.mark.unit
    async def test_usage_multiple_services(self, db_engine, app_client):
        """Multiple services for the same tenant should each appear in by_service."""
        await _seed_daily_usage(
            db_engine, tenant_id=T("usage-multi"), service_key="workers", count=50
        )
        await _seed_daily_usage(
            db_engine, tenant_id=T("usage-multi"), service_key="kanban", count=30
        )
        response = app_client.get(f"/api/usage/tenant/{T('usage-multi')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["total_requests"] == 80
        assert len(data["by_service"]) == 2
        svc_keys = {s["service_key"] for s in data["by_service"]}
        assert svc_keys == {"workers", "kanban"}

    @pytest.mark.unit
    async def test_usage_by_tenant_inverted_window_rejected(self, app_client):
        """start > end is a 422, not a misleading empty 200."""
        now = datetime.now(UTC)
        start = quote(now.isoformat())
        end = quote((now - timedelta(days=1)).isoformat())
        r = app_client.get(f"/api/usage/tenant/{T('t1')}?start={start}&end={end}")
        assert r.status_code == 422

    @pytest.mark.unit
    async def test_usage_by_tenant_window_too_large_rejected(self, app_client):
        """A span wider than MAX_WINDOW_DAYS is a 422."""
        from routers.export import MAX_WINDOW_DAYS

        end = datetime.now(UTC)
        start = end - timedelta(days=MAX_WINDOW_DAYS + 5)
        r = app_client.get(
            f"/api/usage/tenant/{T('t1')}?start={quote(start.isoformat())}&end={quote(end.isoformat())}"
        )
        assert r.status_code == 422

    @pytest.mark.unit
    async def test_usage_by_tenant_valid_window_ok(self, app_client):
        """A sane sub-cap window still succeeds."""
        end = datetime.now(UTC)
        start = end - timedelta(days=7)
        r = app_client.get(
            f"/api/usage/tenant/{T('t1')}?start={quote(start.isoformat())}&end={quote(end.isoformat())}"
        )
        assert r.status_code == 200

    @pytest.mark.unit
    async def test_usage_me_inverted_window_rejected(self, app_client):
        """The /me usage window is validated via _usage_for_subject too."""
        now = datetime.now(UTC)
        start = quote(now.isoformat())
        end = quote((now - timedelta(days=2)).isoformat())
        # X-Tenant-Id is supplied so the 422 below is genuinely the WINDOW
        # verdict. /me now refuses a tenant-less read with 400 before it ever
        # looks at the window, and a test that omitted the header would still
        # have gone red here — but for a test asserting 400 it would have PASSED
        # for the wrong reason. See TestMyUsageTenantScope.
        r = app_client.get(
            f"/api/usage/me?start={start}&end={end}",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": T("me-window")},
        )
        assert r.status_code == 422

    @pytest.mark.unit
    async def test_usage_by_license_inverted_window_rejected(self, app_client):
        """The customer-facing /license endpoint (require_tenant) must also bound
        its window — an inverted range must 422, not silently scan/return empty."""
        now = datetime.now(UTC)
        start = quote(now.isoformat())
        end = quote((now - timedelta(days=2)).isoformat())
        r = app_client.get(f"/api/usage/license/{uuid.uuid4()}?start={start}&end={end}")
        assert r.status_code == 422


# --- Costs ---


class TestCostEndpoints:
    @pytest.mark.unit
    async def test_costs_by_tenant_empty(self, app_client):
        """Tenant with no cost data returns zero totals."""
        response = app_client.get(f"/api/costs/tenant/{T('no-data')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert float(data["total_cost_usd"]) == 0
        assert data["total_input_tokens"] == 0
        assert data["total_output_tokens"] == 0
        assert data["by_provider"] == []
        assert data["tenant_id"] == T("no-data")

    @pytest.mark.unit
    async def test_costs_by_tenant_with_data(self, db_engine, app_client):
        """Cost endpoint returns correct totals from seeded cost_daily."""
        await _seed_cost_daily(db_engine, tenant_id=T("cost-t1"))
        response = app_client.get(f"/api/costs/tenant/{T('cost-t1')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert float(data["total_cost_usd"]) == 1.5
        assert data["total_input_tokens"] == 50000
        assert data["total_output_tokens"] == 10000
        assert len(data["by_provider"]) == 1
        assert data["by_provider"][0]["provider"] == "claude"

    @pytest.mark.unit
    async def test_costs_by_tenant_with_provider_filter(self, db_engine, app_client):
        """Provider filter limits results to matching provider."""
        await _seed_cost_daily(db_engine, tenant_id=T("cost-tf"), provider="openai")
        response = app_client.get(f"/api/costs/tenant/{T('cost-tf')}?provider=openai")
        assert response.status_code == 200
        assert float(response.json()["data"]["total_cost_usd"]) > 0

        response = app_client.get(f"/api/costs/tenant/{T('cost-tf')}?provider=nonexistent")
        assert float(response.json()["data"]["total_cost_usd"]) == 0
        assert response.json()["data"]["by_provider"] == []

    @pytest.mark.unit
    async def test_costs_by_tenant_token_counts(self, db_engine, app_client):
        """Verify input/output token totals match seeded data."""
        await _seed_cost_daily(
            db_engine,
            tenant_id=T("cost-tok"),
            input_tokens=75000,
            output_tokens=15000,
        )
        response = app_client.get(f"/api/costs/tenant/{T('cost-tok')}")
        data = response.json()["data"]
        assert data["total_input_tokens"] == 75000
        assert data["total_output_tokens"] == 15000

    @pytest.mark.unit
    async def test_costs_by_provider(self, db_engine, app_client):
        """Provider endpoint aggregates costs across all tenants by provider."""
        await _seed_cost_daily(db_engine, tenant_id=T("cost-t2"), provider="deepseek")
        response = app_client.get("/api/costs/provider")
        assert response.status_code == 200
        data = response.json()["data"]
        assert "by_provider" in data
        providers = [p["provider"] for p in data["by_provider"]]
        assert "deepseek" in providers

    @pytest.mark.unit
    async def test_costs_by_provider_aggregates_all_tenants(self, db_engine, app_client):
        """Provider endpoint sums costs from multiple tenants for same provider."""
        await _seed_cost_daily(db_engine, tenant_id=T("cost-pa"), provider="claude-agg")
        await _seed_cost_daily(db_engine, tenant_id=T("cost-pb"), provider="claude-agg")
        response = app_client.get("/api/costs/provider")
        data = response.json()["data"]
        agg = [p for p in data["by_provider"] if p["provider"] == "claude-agg"]
        assert len(agg) == 1
        assert float(agg[0]["cost_usd"]) == 3.0  # 1.50 * 2
        assert agg[0]["input_tokens"] == 100000  # 50000 * 2
        assert agg[0]["output_tokens"] == 20000  # 10000 * 2
        assert agg[0]["request_count"] == 50  # 25 * 2

    @pytest.mark.unit
    async def test_costs_summary(self, db_engine, app_client):
        """Summary returns total platform cost metrics."""
        await _seed_cost_daily(db_engine, tenant_id=T("cost-t3"))
        response = app_client.get("/api/costs/summary")
        assert response.status_code == 200
        data = response.json()["data"]
        assert "total_internal_cost_usd" in data
        assert float(data["total_internal_cost_usd"]) > 0

    @pytest.mark.unit
    async def test_costs_summary_structure(self, db_engine, app_client):
        """Summary response includes all expected fields."""
        await _seed_cost_daily(db_engine, tenant_id=T("cost-sum"))
        response = app_client.get("/api/costs/summary")
        data = response.json()["data"]
        assert "start" in data
        assert "end" in data
        assert "by_provider" in data
        assert "total_input_tokens" in data
        assert "total_output_tokens" in data

    @pytest.mark.unit
    async def test_costs_by_tenant_date_range(self, db_engine, app_client):
        """Date range filter excludes data outside the requested range."""
        await _seed_cost_daily(db_engine, tenant_id=T("cost-dr"))
        response = app_client.get(
            f"/api/costs/tenant/{T('cost-dr')}",
            params={"start": "2020-01-01T00:00:00Z", "end": "2020-02-01T00:00:00Z"},
        )
        assert response.status_code == 200
        assert float(response.json()["data"]["total_cost_usd"]) == 0

    @pytest.mark.unit
    async def test_costs_by_tenant_multiple_providers(self, db_engine, app_client):
        """Multiple providers should each appear in by_provider breakdown."""
        await _seed_cost_daily(
            db_engine, tenant_id=T("cost-multi"), provider="claude", cost_usd=1.0
        )
        await _seed_cost_daily(
            db_engine, tenant_id=T("cost-multi"), provider="openai", cost_usd=2.0
        )
        response = app_client.get(f"/api/costs/tenant/{T('cost-multi')}")
        data = response.json()["data"]
        assert len(data["by_provider"]) == 2
        assert float(data["total_cost_usd"]) == 3.0
        providers = {p["provider"] for p in data["by_provider"]}
        assert providers == {"claude", "openai"}

    @pytest.mark.unit
    async def test_costs_summary_empty(self, app_client):
        """Cost summary with no data returns zero totals."""
        response = app_client.get(
            "/api/costs/summary",
            params={"start": "2020-01-01T00:00:00Z", "end": "2020-02-01T00:00:00Z"},
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert float(data["total_internal_cost_usd"]) == 0
        assert data["total_input_tokens"] == 0

    @pytest.mark.unit
    async def test_costs_by_correlation_totals(self, db_engine, app_client):
        """by-correlation sums priced cost + tokens for the given run ids."""
        cid = uuid.uuid4()
        await _seed_cost_event(db_engine, correlation_id=cid, cost_usd="0.10", stage="narration")
        await _seed_cost_event(db_engine, correlation_id=cid, cost_usd="0.05", stage="render")
        response = app_client.get(f"/api/costs/by-correlation?ids={cid}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert float(data["total_cost_usd"]) == 0.15
        assert data["event_count"] == 2
        assert data["correlation_ids"] == [str(cid)]

    @pytest.mark.unit
    async def test_costs_by_correlation_group_by_stage(self, db_engine, app_client):
        """group_by=stage returns one group per stage with its own cost."""
        cid = uuid.uuid4()
        await _seed_cost_event(db_engine, correlation_id=cid, cost_usd="0.10", stage="narration")
        await _seed_cost_event(db_engine, correlation_id=cid, cost_usd="0.05", stage="render")
        response = app_client.get(f"/api/costs/by-correlation?ids={cid}&group_by=stage")
        data = response.json()["data"]
        by_stage = {g["key"]["stage"]: float(g["cost_usd"]) for g in data["groups"]}
        assert by_stage == {"narration": 0.10, "render": 0.05}

    @pytest.mark.unit
    async def test_costs_by_correlation_tenant_isolation(self, db_engine, app_client):
        """A customer caller (X-Tenant-Id) only sees its own tenant's events."""
        cid = uuid.uuid4()
        await _seed_cost_event(
            db_engine, tenant_id=T("owner-t"), correlation_id=cid, cost_usd="0.10"
        )
        # Same correlation id reused under a different tenant must not leak.
        await _seed_cost_event(
            db_engine, tenant_id=T("other-t"), correlation_id=cid, cost_usd="9.99"
        )
        response = app_client.get(
            f"/api/costs/by-correlation?ids={cid}",
            headers={"X-Tenant-Id": T("owner-t")},
        )
        data = response.json()["data"]
        assert float(data["total_cost_usd"]) == 0.10
        assert data["event_count"] == 1

    @pytest.mark.unit
    async def test_costs_by_correlation_invalid_id(self, app_client):
        """A malformed correlation id is a 422."""
        response = app_client.get("/api/costs/by-correlation?ids=not-a-uuid")
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_costs_by_correlation_invalid_group_by(self, app_client):
        """An unknown group_by dimension is a 422."""
        cid = uuid.uuid4()
        response = app_client.get(f"/api/costs/by-correlation?ids={cid}&group_by=bogus")
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_costs_by_correlation_truncation_refused(
        self, db_engine, app_client, monkeypatch
    ):
        """A set exceeding the row cap must 400 — never a silently-undercounted 200.

        Mutation-proof: lower the cap to 2, seed 3 priced events under one id.
        The pre-fix code summed the truncated 50k page and returned 200 with a
        money total LOWER than reality; the fix refuses so the caller narrows.
        """
        import routers.costs as costs_router

        monkeypatch.setattr(costs_router, "_MAX_CORRELATION_ROWS", 2)
        cid = uuid.uuid4()
        for _ in range(3):
            await _seed_cost_event(db_engine, correlation_id=cid, cost_usd="0.10")
        response = app_client.get(f"/api/costs/by-correlation?ids={cid}")
        assert response.status_code == 400
        assert "narrow" in response.text.lower()

    @pytest.mark.unit
    async def test_costs_by_correlation_at_cap_is_complete(
        self, db_engine, app_client, monkeypatch
    ):
        """Exactly cap rows is a COMPLETE result — 200 with the real total, no false 400.

        Proves the cap+1 sentinel: a result landing exactly on the cap is not
        truncation, so it must still return the honest sum.
        """
        import routers.costs as costs_router

        monkeypatch.setattr(costs_router, "_MAX_CORRELATION_ROWS", 2)
        cid = uuid.uuid4()
        for _ in range(2):
            await _seed_cost_event(db_engine, correlation_id=cid, cost_usd="0.10")
        response = app_client.get(f"/api/costs/by-correlation?ids={cid}")
        assert response.status_code == 200
        assert float(response.json()["data"]["total_cost_usd"]) == 0.20

    @pytest.mark.unit
    async def test_costs_by_tenant_inverted_window_rejected(self, app_client):
        """start > end is a 422, not a misleading empty 200."""
        now = datetime.now(UTC)
        start = quote(now.isoformat())
        end = quote((now - timedelta(days=1)).isoformat())
        r = app_client.get(f"/api/costs/tenant/{T('t1')}?start={start}&end={end}")
        assert r.status_code == 422

    @pytest.mark.unit
    async def test_costs_by_tenant_window_too_large_rejected(self, app_client):
        """A span wider than MAX_WINDOW_DAYS is a 422."""
        from routers.export import MAX_WINDOW_DAYS

        end = datetime.now(UTC)
        start = end - timedelta(days=MAX_WINDOW_DAYS + 5)
        r = app_client.get(
            f"/api/costs/tenant/{T('t1')}?start={quote(start.isoformat())}&end={quote(end.isoformat())}"
        )
        assert r.status_code == 422

    @pytest.mark.unit
    async def test_costs_by_tenant_valid_window_ok(self, app_client):
        """A sane sub-cap window still succeeds."""
        end = datetime.now(UTC)
        start = end - timedelta(days=7)
        r = app_client.get(
            f"/api/costs/tenant/{T('t1')}?start={quote(start.isoformat())}&end={quote(end.isoformat())}"
        )
        assert r.status_code == 200

    @pytest.mark.unit
    async def test_costs_me_inverted_window_rejected(self, app_client):
        """The /me cost window is validated via _costs_for_subject too."""
        now = datetime.now(UTC)
        start = quote(now.isoformat())
        end = quote((now - timedelta(days=2)).isoformat())
        r = app_client.get(
            f"/api/costs/me?start={start}&end={end}",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": T("mc-window")},
        )
        assert r.status_code == 422


# --- Quota ---


class TestQuotaEndpoints:
    @pytest.mark.unit
    async def test_quota_check_no_usage(self, db_engine, app_client):
        """Quota check with no usage returns full remaining quota."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        response = app_client.get(
            f"/api/quota/check?license_id={lic_id}&plan_id={plan_id}&quota_monthly=1000"
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["license_id"] == str(lic_id)
        assert data["plan_id"] == str(plan_id)
        assert data["used_this_month"] == 0
        assert data["remaining"] == 1000
        assert data["over_quota"] is False
        assert data["quota_monthly"] == 1000

    @pytest.mark.unit
    async def test_quota_check_with_usage(self, db_engine, app_client):
        """Quota check returns actual usage when monthly data is seeded."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        await _seed_monthly_usage(db_engine, license_id=lic_id, plan_id=plan_id, count=200)
        response = app_client.get(
            f"/api/quota/check?license_id={lic_id}&plan_id={plan_id}&quota_monthly=1000"
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["used_this_month"] == 200
        assert data["remaining"] == 800
        assert data["over_quota"] is False

    @pytest.mark.unit
    async def test_quota_check_exactly_at_limit(self, db_engine, app_client):
        """Quota check at exact limit should show over_quota=True (remaining=0)."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        await _seed_monthly_usage(db_engine, license_id=lic_id, plan_id=plan_id, count=1000)
        response = app_client.get(
            f"/api/quota/check?license_id={lic_id}&plan_id={plan_id}&quota_monthly=1000"
        )
        data = response.json()["data"]
        assert data["used_this_month"] == 1000
        assert data["remaining"] == 0
        assert data["over_quota"] is True

    @pytest.mark.unit
    async def test_quota_check_over_quota(self, db_engine, app_client):
        """When usage exceeds quota, over_quota should be True and remaining negative."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        await _seed_monthly_usage(db_engine, license_id=lic_id, plan_id=plan_id, count=1500)
        response = app_client.get(
            f"/api/quota/check?license_id={lic_id}&plan_id={plan_id}&quota_monthly=1000"
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["used_this_month"] == 1500
        assert data["remaining"] == -500
        assert data["over_quota"] is True

    @pytest.mark.unit
    async def test_quota_check_no_quota_limit(self, db_engine, app_client):
        """When quota_monthly is not provided, remaining should be null."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        response = app_client.get(f"/api/quota/check?license_id={lic_id}&plan_id={plan_id}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["quota_monthly"] is None
        assert data["remaining"] is None
        assert data["over_quota"] is False

    @pytest.mark.unit
    async def test_quota_check_invalid_uuid(self, app_client):
        """Invalid UUID format should return 400."""
        response = app_client.get("/api/quota/check?license_id=not-uuid&plan_id=also-not")
        assert response.status_code == 400
        assert "Invalid UUID" in response.json()["error"]["message"]

    @pytest.mark.unit
    async def test_quota_check_missing_params(self, app_client):
        """Missing required license_id/plan_id should return 422."""
        response = app_client.get("/api/quota/check")
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_quota_status(self, app_client):
        """Quota status for a tenant returns tenant info.

        REWRITTEN: this used to reach the (unreachable) test licenses_url and
        rely on ``fetch_licenses`` degrading to ``[]`` — i.e. it asserted a 200
        that was only produced by the outage-looks-like-no-licenses defect. The
        licenses answer is now stubbed explicitly so the test proves the shape
        of a SUCCESSFUL enumeration, which is what it was named for.
        """
        with patch("routers.quota.fetch_licenses", AsyncMock(return_value=[])):
            response = app_client.get(f"/api/quota/status/{T('some-tenant')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["tenant_id"] == T("some-tenant")
        assert isinstance(data["licenses"], list)

    @pytest.mark.unit
    async def test_quota_status_licenses_outage_is_503_not_an_empty_200(self, app_client):
        """REWRITTEN from ``test_quota_status_returns_empty_licenses``.

        That test pinned the defect: with the licenses service unreachable it
        asserted ``200`` + ``licenses == []``, which is exactly the answer a
        tenant with genuinely zero licenses gets. A caller could not tell the
        two apart, so a Keycloak outage read as "nothing to meter". The outage
        must now surface as 503; the genuine-empty case is covered by
        ``test_quota_status`` above.
        """
        from services.billing import LicensesUnavailableError

        with patch(
            "routers.quota.fetch_licenses",
            AsyncMock(side_effect=LicensesUnavailableError(T("any-tenant"), "HTTP 401")),
        ):
            response = app_client.get(f"/api/quota/status/{T('any-tenant')}")
        assert response.status_code == 503

    @pytest.mark.unit
    async def test_quota_config_persists_even_when_redis_down(self, db_engine, app_client):
        """Config must persist the durable row even with Redis unavailable.

        app_client sets redis to None. The durable row is the source of truth and
        the periodic re-seed rebuilds the Redis key, so a Redis outage at config
        time must NOT 503 and lose the config — it returns 201 and the row lands.
        """
        from sqlalchemy import select
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        from models.db import RequestQuotaConfig

        lic_id = str(uuid.uuid4())
        response = app_client.post(
            "/api/quota/config",
            json={"license_id": lic_id, "tenant_id": T("t-redis-down"), "quota_monthly": 5000},
        )
        assert response.status_code == 201, response.text
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            row = (
                await session.execute(
                    select(RequestQuotaConfig).where(
                        RequestQuotaConfig.license_id == uuid.UUID(lic_id)
                    )
                )
            ).scalar_one_or_none()
        assert row is not None and row.quota_monthly == 5000

    @pytest.mark.unit
    async def test_quota_config_invalid_license_id(self, app_client):
        """POST /api/quota/config with invalid UUID should return 400."""
        response = app_client.post(
            "/api/quota/config",
            json={"license_id": "not-a-uuid", "tenant_id": T("t-bad-uuid"), "quota_monthly": 5000},
        )
        assert response.status_code == 400

    @pytest.mark.unit
    async def test_quota_config_missing_fields(self, app_client):
        """POST /api/quota/config with missing fields should return 422."""
        response = app_client.post("/api/quota/config", json={})
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_quota_config_zero_is_accepted_block_all(self, app_client):
        """POST /api/quota/config with quota_monthly=0 now stores a BLOCK-ALL row
        (was 422 under gt=0). Unlimited is the absence of a config, not a 0."""
        lic_id = str(uuid.uuid4())
        response = app_client.post(
            "/api/quota/config",
            json={"license_id": lic_id, "tenant_id": T("t-zero"), "quota_monthly": 0},
        )
        assert response.status_code == 201, response.text

    @pytest.mark.unit
    async def test_quota_config_negative_quota_value(self, app_client):
        """POST /api/quota/config with a negative quota_monthly still fails (ge=0)."""
        lic_id = str(uuid.uuid4())
        response = app_client.post(
            "/api/quota/config",
            json={"license_id": lic_id, "tenant_id": T("t-neg"), "quota_monthly": -5},
        )
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_quota_config_requires_tenant_id(self, app_client):
        """A tenant-less config is a 422 client error, not a persisted NULL-tenant
        row: tenant_id is NOT NULL (migration 0007) and a NULL/absent
        tenant-of-record would be cross-tenant-readable. The API mirrors the DB
        invariant so the request is rejected at the boundary, not as a 503."""
        lic_id = str(uuid.uuid4())
        response = app_client.post(
            "/api/quota/config",
            json={"license_id": lic_id, "quota_monthly": 5000},
        )
        assert response.status_code == 422, response.text


class TestQuotaStatusEndpoint:
    """GET /api/quota/status/{tenant_id} — per-tenant quota rollup (S2S-fed)."""

    @pytest.mark.unit
    async def test_status_reports_per_license_usage(self, db_engine, app_client):
        lic_id, plan_id = uuid.uuid4(), uuid.uuid4()
        await _seed_monthly_usage(db_engine, license_id=lic_id, plan_id=plan_id, count=200)
        licenses = [
            {
                "id": str(lic_id),
                "tenant_id": T("t-quota"),
                "plan": {"id": str(plan_id), "quota_monthly": 1000},
            },
        ]
        with patch("routers.quota.fetch_licenses", AsyncMock(return_value=licenses)):
            r = app_client.get(
                f"/api/quota/status/{T('t-quota')}", headers={"X-Tenant-Id": T("t-quota")}
            )
        assert r.status_code == 200, r.text
        data = r.json()["data"]
        assert data["tenant_id"] == T("t-quota")
        assert len(data["licenses"]) == 1
        entry = data["licenses"][0]
        assert entry["license_id"] == str(lic_id)
        assert entry["plan_id"] == str(plan_id)
        assert entry["used_this_month"] == 200
        assert entry["remaining"] == 800
        assert entry["over_quota"] is False

    @pytest.mark.unit
    async def test_status_survives_a_license_with_two_subjects(self, db_engine, app_client):
        """A license used by two subjects must report their SUM, not 503.

        ``_get_db_usage`` used to ``scalar_one_or_none()`` a per-license filter
        against a table whose grain includes ``subject_id``, so a second subject
        raised ``MultipleResultsFound``. That is a ``SQLAlchemyError``, which
        ``main._sqlalchemy_exception_handler`` turns into
        **503 DATABASE_UNAVAILABLE / "Database temporarily unavailable"** — a
        healthy Postgres blamed for a query bug. And because the router loops
        over licenses with no per-license guard, the ONE bad license took the
        whole tenant's response with it, including the healthy sibling asserted
        on below.
        """
        lic_multi, lic_ok = uuid.uuid4(), uuid.uuid4()
        plan_id = uuid.uuid4()
        await _seed_monthly_usage(
            db_engine, license_id=lic_multi, plan_id=plan_id, count=300, subject_id=uuid.uuid4()
        )
        await _seed_monthly_usage(
            db_engine, license_id=lic_multi, plan_id=plan_id, count=450, subject_id=uuid.uuid4()
        )
        await _seed_monthly_usage(db_engine, license_id=lic_ok, plan_id=plan_id, count=10)
        licenses = [
            {
                "id": str(lic_multi),
                "tenant_id": T("t-multisub"),
                "plan": {"id": str(plan_id), "quota_monthly": 1000},
            },
            {
                "id": str(lic_ok),
                "tenant_id": T("t-multisub"),
                "plan": {"id": str(plan_id), "quota_monthly": 1000},
            },
        ]
        with patch("routers.quota.fetch_licenses", AsyncMock(return_value=licenses)):
            r = app_client.get(
                f"/api/quota/status/{T('t-multisub')}", headers={"X-Tenant-Id": T("t-multisub")}
            )
        assert r.status_code == 200, r.text
        by_lic = {e["license_id"]: e for e in r.json()["data"]["licenses"]}
        assert by_lic[str(lic_multi)]["used_this_month"] == 750
        assert by_lic[str(lic_ok)]["used_this_month"] == 10, "sibling lost to a bad license"

    @pytest.mark.unit
    async def test_status_omits_licenses_without_a_plan(self, db_engine, app_client):
        lic_with, plan_id = uuid.uuid4(), uuid.uuid4()
        lic_without = uuid.uuid4()
        licenses = [
            {
                "id": str(lic_with),
                "tenant_id": T("t-noplan"),
                "plan": {"id": str(plan_id), "quota_monthly": 500},
            },
            {"id": str(lic_without), "tenant_id": T("t-noplan"), "plan": None},
        ]
        with patch("routers.quota.fetch_licenses", AsyncMock(return_value=licenses)):
            r = app_client.get(
                f"/api/quota/status/{T('t-noplan')}", headers={"X-Tenant-Id": T("t-noplan")}
            )
        assert r.status_code == 200
        licenses_out = r.json()["data"]["licenses"]
        assert [entry["license_id"] for entry in licenses_out] == [str(lic_with)]

    @pytest.mark.unit
    async def test_status_empty_when_tenant_genuinely_has_no_licenses(self, db_engine, app_client):
        # RENAMED + RESCOPED: the stub returns [] because the licenses service
        # SAID so (a 200 with an empty list), which is a real answer and must
        # still be a 200. It used to be captioned "fails closed to [] when the
        # licenses service is down" — conflating the two states. The outage
        # branch is asserted separately, as a 503.
        with patch("routers.quota.fetch_licenses", AsyncMock(return_value=[])):
            r = app_client.get(
                f"/api/quota/status/{T('t-empty')}", headers={"X-Tenant-Id": T("t-empty")}
            )
        assert r.status_code == 200
        assert r.json()["data"]["licenses"] == []

    @pytest.mark.unit
    async def test_status_is_503_when_licenses_cannot_be_enumerated(self, db_engine, app_client):
        from services.billing import LicensesUnavailableError

        with patch(
            "routers.quota.fetch_licenses",
            AsyncMock(side_effect=LicensesUnavailableError(T("t-empty"), "transport error")),
        ):
            r = app_client.get(
                f"/api/quota/status/{T('t-empty')}", headers={"X-Tenant-Id": T("t-empty")}
            )
        assert r.status_code == 503

    @pytest.mark.unit
    async def test_status_enforces_tenant_isolation(self, db_engine, app_client):
        # A customer (X-Tenant-Id) cannot read another tenant's quota.
        r = app_client.get(
            f"/api/quota/status/{T('t-other')}", headers={"X-Tenant-Id": T("t-self")}
        )
        assert r.status_code == 403


# --- Budget ---


class TestBudgetEndpoints:
    @pytest.mark.unit
    def test_create_budget(self, app_client):
        """POST budget creates a new token budget and returns 201."""
        response = app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("bud-t1"),
                "budget_tokens": 1000000,
                "period": "monthly",
            },
        )
        assert response.status_code == 201
        data = response.json()["data"]
        assert data["tenant_id"] == T("bud-t1")
        assert data["budget_tokens"] == 1000000
        assert data["period"] == "monthly"
        assert data["active"] is True
        assert data["includes_input"] is True
        assert data["includes_output"] is True
        assert data["license_id"] is None  # tenant-level budget
        assert "id" in data

    @pytest.mark.unit
    def test_create_budget_with_license(self, app_client):
        """Budget with license_id should store the association."""
        lic_id = str(uuid.uuid4())
        response = app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("bud-t2"),
                "license_id": lic_id,
                "budget_tokens": 500000,
                "period": "weekly",
            },
        )
        assert response.status_code == 201
        data = response.json()["data"]
        assert data["license_id"] == lic_id
        assert data["period"] == "weekly"

    @pytest.mark.unit
    def test_create_budget_stacks(self, app_client):
        """Stacked-budget semantics: each POST inserts a fresh row.

        Replaces the old upsert test — no more upsert; both budgets coexist.
        """
        first = app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-up"), "budget_tokens": 100000, "period": "monthly"},
        )
        second = app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-up"), "budget_tokens": 200000, "period": "monthly"},
        )
        assert first.status_code == 201
        assert second.status_code == 201
        assert first.json()["data"]["id"] != second.json()["data"]["id"]
        assert second.json()["data"]["budget_tokens"] == 200000

    @pytest.mark.unit
    def test_create_budget_zero_is_accepted_block_all(self, app_client):
        """POST /api/budget with budget_tokens=0 now creates a BLOCK-ALL budget
        (was 422 under gt=0). Unlimited is the absence of an active budget —
        the ``no_active_budget`` short-circuit — never a stored 0. Mirrors the
        same inversion PR #68 made for quota_monthly. Full semantics are pinned
        in tests/test_budget_zero_block_all.py."""
        response = app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-zero"), "budget_tokens": 0, "period": "monthly"},
        )
        assert response.status_code == 201, response.text
        assert response.json()["data"]["budget_tokens"] == 0

    @pytest.mark.unit
    def test_create_budget_invalid_tokens_negative(self, app_client):
        """Negative budget_tokens should fail validation."""
        response = app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-bad"), "budget_tokens": -100, "period": "monthly"},
        )
        assert response.status_code == 422

    @pytest.mark.unit
    def test_create_budget_invalid_period(self, app_client):
        """Invalid period should fail validation (only monthly/weekly)."""
        response = app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-bad"), "budget_tokens": 1000, "period": "yearly"},
        )
        assert response.status_code == 422

    @pytest.mark.unit
    def test_create_budget_missing_tenant_id(self, app_client):
        """Missing tenant_id should return 422."""
        response = app_client.post(
            "/api/budget",
            json={"budget_tokens": 1000, "period": "monthly"},
        )
        assert response.status_code == 422

    @pytest.mark.unit
    def test_create_budget_missing_budget_tokens(self, app_client):
        """Missing budget_tokens should return 422."""
        response = app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-bad")},
        )
        assert response.status_code == 422

    @pytest.mark.unit
    def test_create_budget_custom_includes(self, app_client):
        """Budget can be configured to track only input or output tokens."""
        response = app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("bud-custom-inc"),
                "budget_tokens": 50000,
                "period": "monthly",
                "includes_input": True,
                "includes_output": False,
            },
        )
        assert response.status_code == 201
        data = response.json()["data"]
        assert data["includes_input"] is True
        assert data["includes_output"] is False

    @pytest.mark.unit
    def test_create_budget_license_sync_new_inserts(self, app_client):
        """First POST with source='license_sync' for a new (tenant, license, period)
        inserts a new row."""
        lic_id = str(uuid.uuid4())
        response = app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("ls-new"),
                "license_id": lic_id,
                "budget_tokens": 100_000,
                "period": "monthly",
                "source": "license_sync",
            },
        )
        assert response.status_code == 201, response.text
        data = response.json()["data"]
        assert data["tenant_id"] == T("ls-new")
        assert data["license_id"] == lic_id
        assert data["budget_tokens"] == 100_000
        assert data["source"] == "license_sync"
        assert data["active"] is True

    @pytest.mark.unit
    def test_create_budget_license_sync_dedups(self, app_client):
        """Second POST with source='license_sync' for the SAME (tenant, license,
        period) reconciles in place — same id, no duplicate row.

        This is the regression guard for the live E2E bug: license issuance and
        sync-from-licenses must NOT produce stacked license_sync budgets.
        """
        lic_id = str(uuid.uuid4())
        body = {
            "tenant_id": T("ls-dedup"),
            "license_id": lic_id,
            "budget_tokens": 100_000,
            "period": "monthly",
            "source": "license_sync",
        }
        first = app_client.post("/api/budget", json=body)
        assert first.status_code == 201
        first_id = first.json()["data"]["id"]

        # Second call with a different budget_tokens — should UPDATE same row.
        second = app_client.post(
            "/api/budget",
            json={**body, "budget_tokens": 250_000},
        )
        assert second.status_code == 201
        second_data = second.json()["data"]
        assert second_data["id"] == first_id
        assert second_data["budget_tokens"] == 250_000
        assert second_data["source"] == "license_sync"

    @pytest.mark.unit
    def test_create_budget_license_sync_requires_license_id(self, app_client):
        """source='license_sync' without license_id is a 400 — the dedup key
        depends on license_id, so omitting it is a contract violation."""
        response = app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("ls-no-lic"),
                "budget_tokens": 100_000,
                "period": "monthly",
                "source": "license_sync",
            },
        )
        assert response.status_code == 400
        assert "license_id" in response.json()["error"]["message"].lower()

    @pytest.mark.unit
    def test_create_budget_manual_source_still_stacks(self, app_client):
        """Source='manual' (default) keeps stacking semantics — two POSTs with
        the same (tenant, license, period) produce two distinct rows."""
        lic_id = str(uuid.uuid4())
        body = {
            "tenant_id": T("manual-stack"),
            "license_id": lic_id,
            "budget_tokens": 1000,
            "period": "monthly",
            # no source — defaults to 'manual'
        }
        first = app_client.post("/api/budget", json=body)
        second = app_client.post("/api/budget", json=body)
        assert first.status_code == 201
        assert second.status_code == 201
        assert first.json()["data"]["id"] != second.json()["data"]["id"]
        assert first.json()["data"]["source"] == "manual"
        assert second.json()["data"]["source"] == "manual"

    @pytest.mark.unit
    def test_budget_check_requires_tenant(self, app_client):
        """Budget check without tenant_id should return 400."""
        response = app_client.get("/api/budget/check")
        assert response.status_code == 400
        assert "tenant_id" in response.json()["error"]["message"].lower()

    @pytest.mark.unit
    def test_budget_check_with_created_budget(self, app_client):
        """Budget check returns aggregate over the tenant's stacked budgets."""
        app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("bud-t3"),
                "budget_tokens": 1000000,
                "period": "monthly",
            },
        )
        response = app_client.get(f"/api/budget/check?tenant_id={T('bud-t3')}")
        assert response.status_code == 200
        data = response.json()["data"]
        # New aggregate shape — see BudgetAggregateStatus.
        assert data["tenant_id"] == T("bud-t3")
        assert data["budget_count"] == 1
        assert data["total_remaining"] == 1000000
        assert len(data["budgets"]) == 1
        assert data["budgets"][0]["budget_tokens"] == 1000000
        assert data["budgets"][0]["used_tokens"] == 0
        assert data["budgets"][0]["remaining_tokens"] == 1000000
        assert data["budgets"][0]["over_budget"] is False
        assert data["budgets"][0]["period"] == "monthly"

    @pytest.mark.unit
    async def test_budget_check_with_usage(self, db_engine, app_client):
        """Budget check reflects consumed_tokens after a cascade debit."""
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        from services.budget import debit_budget

        app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-tu"), "budget_tokens": 100000, "period": "monthly"},
        )
        # Drain the budget directly to set consumed_tokens — the aggregate
        # shape reads consumed_tokens, not the legacy TokenUsage rollup.
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as s:
            await debit_budget(s, None, T("bud-tu"), 8000)

        response = app_client.get(f"/api/budget/check?tenant_id={T('bud-tu')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["budget_count"] == 1
        assert data["total_remaining"] == 92000
        assert data["budgets"][0]["used_tokens"] == 8000
        assert data["budgets"][0]["remaining_tokens"] == 92000
        assert data["budgets"][0]["over_budget"] is False

    @pytest.mark.unit
    async def test_budget_check_over_budget(self, db_engine, app_client):
        """Cascade-debit oversold flag surfaces in the aggregate response."""
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        from services.budget import debit_budget

        app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-over"), "budget_tokens": 5000, "period": "monthly"},
        )
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as s:
            await debit_budget(s, None, T("bud-over"), 7000)  # oversold by 2000

        response = app_client.get(f"/api/budget/check?tenant_id={T('bud-over')}")
        data = response.json()["data"]
        # Single budget; consumed_tokens=7000, budget_tokens=5000.
        assert data["budgets"][0]["used_tokens"] == 7000
        assert data["budgets"][0]["remaining_tokens"] == 0  # clamped
        assert data["budgets"][0]["over_budget"] is True
        assert data["total_remaining"] == 0

    @pytest.mark.unit
    def test_budget_check_no_budgets(self, app_client):
        """Tenant with no budgets returns an empty aggregate."""
        response = app_client.get(f"/api/budget/check?tenant_id={T('no-budgets-here')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["tenant_id"] == T("no-budgets-here")
        assert data["budget_count"] == 0
        assert data["total_remaining"] == 0
        assert data["budgets"] == []

    @pytest.mark.unit
    def test_budget_check_aggregates_stacked(self, app_client):
        """Multiple budgets are aggregated; license_id filter no longer applies."""
        lic_id = str(uuid.uuid4())
        app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("bud-lic-filter"),
                "license_id": lic_id,
                "budget_tokens": 100000,
                "period": "monthly",
            },
        )
        app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("bud-lic-filter"),
                "budget_tokens": 500000,
                "period": "monthly",
            },
        )
        response = app_client.get(f"/api/budget/check?tenant_id={T('bud-lic-filter')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["budget_count"] == 2
        assert data["total_remaining"] == 600000
        # Both budgets present; no client-side license filter on aggregate.
        license_ids = {b["license_id"] for b in data["budgets"]}
        assert lic_id in license_ids
        assert None in license_ids

    @pytest.mark.unit
    def test_budget_status(self, app_client):
        """Budget status (path-keyed) returns the same aggregate shape as /check."""
        app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("bud-t4"),
                "budget_tokens": 2000000,
                "period": "monthly",
            },
        )
        response = app_client.get(f"/api/budget/status/{T('bud-t4')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["tenant_id"] == T("bud-t4")
        assert data["budget_count"] == 1
        assert data["budgets"][0]["budget_tokens"] == 2000000

    @pytest.mark.unit
    def test_budget_status_empty(self, app_client):
        """Status for tenant with no budgets returns empty aggregate."""
        response = app_client.get(f"/api/budget/status/{T('no-budgets')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["budget_count"] == 0
        assert data["budgets"] == []

    @pytest.mark.unit
    def test_delete_budget(self, app_client):
        """DELETE deactivates a budget and returns 204."""
        resp = app_client.post(
            "/api/budget",
            json={
                "tenant_id": T("bud-t5"),
                "budget_tokens": 100000,
                "period": "monthly",
            },
        )
        budget_id = resp.json()["data"]["id"]
        response = app_client.delete(f"/api/budget/{budget_id}")
        assert response.status_code == 204

    @pytest.mark.unit
    def test_delete_budget_removes_from_check(self, app_client):
        """After deletion, budget should not appear in the aggregate."""
        resp = app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-del"), "budget_tokens": 100000, "period": "monthly"},
        )
        budget_id = resp.json()["data"]["id"]
        app_client.delete(f"/api/budget/{budget_id}")

        response = app_client.get(f"/api/budget/check?tenant_id={T('bud-del')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["budget_count"] == 0
        assert data["budgets"] == []

    @pytest.mark.unit
    def test_delete_budget_removes_from_status(self, app_client):
        """After deletion, budget should not appear in status either."""
        resp = app_client.post(
            "/api/budget",
            json={"tenant_id": T("bud-del-st"), "budget_tokens": 100000, "period": "monthly"},
        )
        budget_id = resp.json()["data"]["id"]
        app_client.delete(f"/api/budget/{budget_id}")

        response = app_client.get(f"/api/budget/status/{T('bud-del-st')}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["budget_count"] == 0
        assert data["budgets"] == []

    @pytest.mark.unit
    def test_delete_budget_not_found(self, app_client):
        """Deleting a non-existent budget returns 404."""
        response = app_client.delete(f"/api/budget/{uuid.uuid4()}")
        assert response.status_code == 404

    @pytest.mark.unit
    def test_delete_budget_invalid_uuid(self, app_client):
        """Deleting with invalid UUID format returns 400."""
        response = app_client.delete("/api/budget/not-a-uuid")
        assert response.status_code == 400
        assert "Invalid UUID" in response.json()["error"]["message"]

    @pytest.mark.unit
    def test_sync_from_licenses_success(self, app_client, fake_redis, monkeypatch):
        """Successful sync creates budgets from licenses and syncs to Redis."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)

        lic_id = str(uuid.uuid4())
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {
                "id": lic_id,
                "tenant_id": T("sync-tenant"),
                "plan": {"token_budget_monthly": 50000, "quota_monthly": 1000},
            },
        ]

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            response = app_client.post("/api/budget/sync-from-licenses")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["licenses_synced"] == 1
        assert data["quotas_synced"] == 1

    @pytest.mark.unit
    def test_sync_from_licenses_non_200(self, app_client, fake_redis, monkeypatch):
        """Non-200 from licenses service fails loud with 502 (was 200/partial).

        A total-fetch failure must NOT masquerade as HTTP success — a CronJob
        that checks only the status code would otherwise record a green run
        while zero budgets synced.
        """
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            response = app_client.post("/api/budget/sync-from-licenses")
        assert response.status_code == 502
        details = response.json()["error"]["details"]
        assert "500" in details["error"]
        assert details["licenses_synced"] == 0

    @pytest.mark.unit
    def test_sync_from_licenses_connection_error(self, app_client, fake_redis, monkeypatch):
        """Connection error to licenses service fails loud with 502 (was 200/partial)."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)

        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.ConnectError("connection refused")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            response = app_client.post("/api/budget/sync-from-licenses")
        assert response.status_code == 502
        details = response.json()["error"]["details"]
        assert "unreachable" in details["error"]
        assert details["licenses_synced"] == 0

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("failure_kind", "expected_fragment"),
        [("non_200", "500"), ("unreachable", "unreachable")],
    )
    def test_sync_from_licenses_upstream_error_is_5xx(
        self, app_client, fake_redis, monkeypatch, failure_kind, expected_fragment
    ):
        """Total upstream-fetch failure surfaces as 5xx (502), never a 200 body.

        Mutation-proof: if the handler reverts to returning 200 with a
        ``status: "partial"`` body, the ``== 502`` assertion fails. Covers both
        total-failure branches — non-200 upstream and an unreachable service.
        """
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)

        mock_client = AsyncMock()
        if failure_kind == "non_200":
            mock_resp = MagicMock()
            mock_resp.status_code = 500
            mock_client.get.return_value = mock_resp
        else:
            mock_client.get.side_effect = httpx.ConnectError("connection refused")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            response = app_client.post("/api/budget/sync-from-licenses")

        assert response.status_code == 502
        assert 500 <= response.status_code < 600
        details = response.json()["error"]["details"]
        assert expected_fragment in details["error"]
        assert details["licenses_synced"] == 0

    @pytest.mark.unit
    def test_sync_from_licenses_skips_incomplete_licenses(
        self, app_client, fake_redis, monkeypatch
    ):
        """Licenses missing id or tenant_id are skipped."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"id": str(uuid.uuid4()), "plan": {"token_budget_monthly": 1000}},  # no tenant_id
            {"tenant_id": T("t1"), "plan": {"token_budget_monthly": 1000}},  # no id
            {"id": str(uuid.uuid4()), "tenant_id": T("t1"), "plan": {}},  # no budget/quota
        ]
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            response = app_client.post("/api/budget/sync-from-licenses")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["licenses_synced"] == 0
        assert data["quotas_synced"] == 0


# --- Billing ---


class TestBillingEndpoints:
    @pytest.mark.unit
    async def test_billing_tenant_empty(self, app_client):
        """Billing for tenant with no data returns zero totals."""
        month = datetime.now(UTC).strftime("%Y-%m")
        response = app_client.get(f"/api/billing/tenant/{T('no-data')}?month={month}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["tenant_id"] == T("no-data")
        assert data["month"] == month
        assert data["totals"]["total_requests"] == 0
        assert float(data["totals"]["internal_cost_usd"]) == 0
        assert float(data["totals"]["customer_cost_usd"]) == 0
        assert float(data["totals"]["margin_usd"]) == 0
        assert data["licenses"] == []

    @pytest.mark.unit
    async def test_billing_tenant_with_data(self, db_engine, app_client):
        """Billing endpoint with metering + cost data and mocked license fetch."""
        lic_id, app_id, plan_id = await _seed_daily_usage(
            db_engine, tenant_id=T("bill-td"), service_key="workers", count=200
        )
        await _seed_cost_daily(db_engine, tenant_id=T("bill-td"), provider="claude")
        month = datetime.now(UTC).strftime("%Y-%m")

        mock_licenses = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 5000},
            }
        ]
        with patch("services.billing.fetch_licenses", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_licenses
            response = app_client.get(f"/api/billing/tenant/{T('bill-td')}?month={month}")

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["tenant_id"] == T("bill-td")
        assert data["totals"]["total_requests"] >= 200
        assert len(data["licenses"]) >= 1
        lic_entry = data["licenses"][0]
        assert lic_entry["license_id"] == str(lic_id)
        assert lic_entry["app_key"] == "workers"
        assert lic_entry["plan_code"] == "pro"

    @pytest.mark.unit
    async def test_billing_tenant_markup_applied(self, db_engine, app_client):
        """Customer cost should be internal cost * (1 + markup_pct/100)."""
        await _seed_cost_daily(db_engine, tenant_id=T("bill-mk"), provider="claude")
        month = datetime.now(UTC).strftime("%Y-%m")

        with patch("services.billing.fetch_licenses", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = []
            response = app_client.get(f"/api/billing/tenant/{T('bill-mk')}?month={month}")

        assert response.status_code == 200
        data = response.json()["data"]
        internal = Decimal(data["totals"]["internal_cost_usd"])
        customer = Decimal(data["totals"]["customer_cost_usd"])
        markup = data["markup_pct"]
        if internal > 0:
            expected_customer = internal * Decimal(str(1 + markup / 100))
            assert abs(customer - expected_customer.quantize(Decimal("0.01"))) <= Decimal("0.01")

    @pytest.mark.unit
    async def test_billing_tenant_default_markup_from_settings(self, db_engine, app_client):
        """A tenant with no config row bills at the settings default markup.

        ⚠️ Renamed from `..._30pct`: a test whose NAME carries the rate becomes a
        lie the moment the rate changes, and the rate changed to 20% on
        2026-09-10. The property is "falls back to settings", not "is 30".
        """
        from settings import get_settings

        month = datetime.now(UTC).strftime("%Y-%m")
        response = app_client.get(f"/api/billing/tenant/{T('bill-default-markup')}?month={month}")
        data = response.json()["data"]
        assert data["markup_pct"] == get_settings().default_markup_pct

    @pytest.mark.unit
    async def test_billing_tenant_custom_markup(self, db_engine, app_client):
        """Tenant with custom markup_pct should override default."""
        await _seed_tenant_config(db_engine, T("bill-custom-mk"), markup_pct=15.0)
        await _seed_cost_daily(db_engine, tenant_id=T("bill-custom-mk"), provider="claude")
        month = datetime.now(UTC).strftime("%Y-%m")

        with patch("services.billing.fetch_licenses", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = []
            response = app_client.get(f"/api/billing/tenant/{T('bill-custom-mk')}?month={month}")

        data = response.json()["data"]
        assert data["markup_pct"] == 15.0

    @pytest.mark.unit
    async def test_billing_overview_empty(self, app_client):
        """Overview with no data returns current month and empty tenants."""
        month = datetime.now(UTC).strftime("%Y-%m")
        response = app_client.get(f"/api/billing/overview?month={month}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["month"] == month
        assert isinstance(data["tenants"], list)
        assert float(data["platform_total_internal_usd"]) == 0
        assert float(data["platform_total_customer_usd"]) == 0

    @pytest.mark.unit
    async def test_billing_overview_with_data(self, db_engine, app_client):
        """Overview with data shows per-tenant breakdown."""
        await _seed_daily_usage(db_engine, tenant_id=T("bill-t1"))
        await _seed_cost_daily(db_engine, tenant_id=T("bill-t1"))
        month = datetime.now(UTC).strftime("%Y-%m")
        response = app_client.get(f"/api/billing/overview?month={month}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert len(data["tenants"]) >= 1
        tenant = next(t for t in data["tenants"] if t["tenant_id"] == T("bill-t1"))
        assert tenant["total_requests"] == 100
        assert float(tenant["internal_cost_usd"]) > 0
        assert float(tenant["customer_cost_usd"]) > float(tenant["internal_cost_usd"])

    @pytest.mark.unit
    async def test_billing_overview_platform_totals(self, db_engine, app_client):
        """Overview includes platform-level totals across all tenants."""
        await _seed_cost_daily(db_engine, tenant_id=T("bill-pt1"), provider="claude")
        await _seed_cost_daily(db_engine, tenant_id=T("bill-pt2"), provider="claude")
        month = datetime.now(UTC).strftime("%Y-%m")
        response = app_client.get(f"/api/billing/overview?month={month}")
        assert response.status_code == 200
        data = response.json()["data"]
        assert float(data["platform_total_internal_usd"]) == 3.0
        assert float(data["platform_total_customer_usd"]) > 3.0  # includes markup

    @pytest.mark.unit
    async def test_billing_invalid_month_format(self, app_client):
        """Invalid month format should be rejected by regex validation."""
        response = app_client.get(f"/api/billing/tenant/{T('t1')}?month=2025-1")
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_billing_overview_invalid_month(self, app_client):
        """Invalid month format in overview should return 422."""
        response = app_client.get("/api/billing/overview?month=bad")
        assert response.status_code == 422

    @pytest.mark.unit
    async def test_billing_tenant_with_app_cost_rate(self, db_engine, app_client):
        """Billing includes per-request app costs when app rates are configured."""
        lic_id, app_id, plan_id = await _seed_daily_usage(
            db_engine, tenant_id=T("bill-app-cost"), service_key="workers", count=1000
        )
        await _seed_app_cost_rate(db_engine, app_key="workers", cost_per_request=0.01)
        month = datetime.now(UTC).strftime("%Y-%m")

        mock_licenses = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "basic"},
            }
        ]
        with patch("services.billing.fetch_licenses", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_licenses
            response = app_client.get(f"/api/billing/tenant/{T('bill-app-cost')}?month={month}")

        data = response.json()["data"]
        assert data["totals"]["total_requests"] >= 1000
        assert float(data["totals"]["internal_cost_usd"]) >= 10.0  # 1000 * 0.01

    @pytest.mark.unit
    async def test_billing_tenant_token_info(self, db_engine, app_client):
        """Billing includes token usage and budget info."""
        lic_id, _, _ = await _seed_daily_usage(
            db_engine, tenant_id=T("bill-tok"), service_key="inference-router", count=50
        )
        await _seed_cost_daily(
            db_engine,
            tenant_id=T("bill-tok"),
            provider="claude",
            license_id=lic_id,
        )
        await _seed_token_usage(
            db_engine, tenant_id=T("bill-tok"), license_id=lic_id, inp=10000, outp=5000
        )
        month = datetime.now(UTC).strftime("%Y-%m")

        with patch("services.billing.fetch_licenses", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = [
                {"id": str(lic_id), "app": {"key": "inference-router"}, "plan": {"code": "pro"}}
            ]
            response = app_client.get(f"/api/billing/tenant/{T('bill-tok')}?month={month}")

        data = response.json()["data"]
        assert data["totals"]["total_input_tokens"] == 10000
        assert data["totals"]["total_output_tokens"] == 5000

    @pytest.mark.unit
    async def test_billing_overview_multiple_tenants(self, db_engine, app_client):
        """Overview shows multiple tenants sorted by tenant_id."""
        await _seed_daily_usage(db_engine, tenant_id=T("bill-multi-a"), count=50)
        await _seed_daily_usage(db_engine, tenant_id=T("bill-multi-b"), count=75)
        month = datetime.now(UTC).strftime("%Y-%m")
        response = app_client.get(f"/api/billing/overview?month={month}")
        data = response.json()["data"]
        tenant_ids = [t["tenant_id"] for t in data["tenants"]]
        assert T("bill-multi-a") in tenant_ids
        assert T("bill-multi-b") in tenant_ids
        # Should be sorted
        assert tenant_ids == sorted(tenant_ids)


# ===========================================================================
# Per-user metering: subject_id query API (Phase 3)
# ===========================================================================


async def _seed_daily_usage_with_subject(
    db_engine,
    tenant_id: str,
    subject_id: uuid.UUID,
    license_id: uuid.UUID | None = None,
    service_key: str = "workers",
    count: int = 100,
    denied: int = 0,
    bucket_date=None,
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """Insert one MeteringDaily row attributed to a specific subject."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    lic_id = license_id or uuid.uuid4()
    app_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    day = bucket_date or datetime.now(UTC).date()
    async with factory() as session:
        session.add(
            MeteringDaily(
                tenant_id=tenant_id,
                license_id=lic_id,
                subject_id=subject_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key=service_key,
                bucket_date=day,
                request_count=count,
                denied_count=denied,
            )
        )
        await session.commit()
    return lic_id, app_id, plan_id


async def _seed_cost_daily_with_subject(
    db_engine,
    tenant_id: str,
    subject_id: uuid.UUID,
    license_id: uuid.UUID | None = None,
    provider: str = "claude",
    input_tokens: int = 1000,
    output_tokens: int = 500,
    cost_usd=Decimal("0.05"),
    request_count: int = 5,
    bucket_date=None,
    service_key: str = "inference-router",
) -> uuid.UUID:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    lic_id = license_id or uuid.uuid4()
    day = bucket_date or datetime.now(UTC).date()
    async with factory() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                license_id=lic_id,
                subject_id=subject_id,
                service_key=service_key,
                provider=provider,
                bucket_date=day,
                total_input_tokens=input_tokens,
                total_output_tokens=output_tokens,
                total_cost_usd=cost_usd,
                request_count=request_count,
            )
        )
        await session.commit()
    return lic_id


class TestUsageBySubjectFilter:
    """The optional ``subject_id=`` query param narrows the existing endpoints."""

    @pytest.mark.unit
    async def test_tenant_filter_by_subject(self, db_engine, app_client):
        sub_a = uuid.uuid4()
        sub_b = uuid.uuid4()
        await _seed_daily_usage_with_subject(db_engine, T("sub-t1"), sub_a, count=10)
        await _seed_daily_usage_with_subject(db_engine, T("sub-t1"), sub_b, count=20)

        # No filter => sum
        resp = app_client.get(f"/api/usage/tenant/{T('sub-t1')}")
        assert resp.status_code == 200
        assert resp.json()["data"]["total_requests"] == 30

        # Filtered by subject => only that subject's row
        resp = app_client.get(f"/api/usage/tenant/{T('sub-t1')}?subject_id={sub_a}")
        assert resp.status_code == 200
        assert resp.json()["data"]["total_requests"] == 10

    @pytest.mark.unit
    async def test_license_filter_by_subject(self, db_engine, app_client):
        sub_a = uuid.uuid4()
        sub_b = uuid.uuid4()
        lic = uuid.uuid4()
        await _seed_daily_usage_with_subject(db_engine, T("sub-t2"), sub_a, license_id=lic, count=7)
        await _seed_daily_usage_with_subject(
            db_engine, T("sub-t2"), sub_b, license_id=lic, count=13
        )

        resp = app_client.get(f"/api/usage/license/{lic}")
        assert resp.status_code == 200
        assert resp.json()["data"]["total_requests"] == 20

        resp = app_client.get(f"/api/usage/license/{lic}?subject_id={sub_a}")
        assert resp.status_code == 200
        assert resp.json()["data"]["total_requests"] == 7


class TestUsageByUserAdmin:
    """Admin ``/api/usage/user/{subject_id}`` aggregates across tenants."""

    @pytest.mark.unit
    async def test_aggregates_across_tenants(self, db_engine, app_client):
        sub = uuid.uuid4()
        await _seed_daily_usage_with_subject(db_engine, T("user-tA"), sub, count=4)
        await _seed_daily_usage_with_subject(db_engine, T("user-tB"), sub, count=6)
        # Different subject in same tenant — must NOT be counted.
        await _seed_daily_usage_with_subject(db_engine, T("user-tA"), uuid.uuid4(), count=99)

        resp = app_client.get(f"/api/usage/user/{sub}")
        assert resp.status_code == 200
        assert resp.json()["data"]["total_requests"] == 10

    @pytest.mark.unit
    async def test_narrowed_by_tenant_id_query(self, db_engine, app_client):
        sub = uuid.uuid4()
        await _seed_daily_usage_with_subject(db_engine, T("user-tA2"), sub, count=4)
        await _seed_daily_usage_with_subject(db_engine, T("user-tB2"), sub, count=6)
        resp = app_client.get(f"/api/usage/user/{sub}?tenant_id={T('user-tA2')}")
        assert resp.status_code == 200
        assert resp.json()["data"]["total_requests"] == 4


class TestMyUsage:
    """``/api/usage/me`` reads X-Subject-Id + X-Tenant-Id, returns only caller's data."""

    @pytest.mark.unit
    async def test_returns_caller_usage_only(self, db_engine, app_client):
        me = uuid.uuid4()
        other = uuid.uuid4()
        await _seed_daily_usage_with_subject(db_engine, T("me-t1"), me, count=11)
        await _seed_daily_usage_with_subject(db_engine, T("me-t1"), other, count=99)

        resp = app_client.get(
            "/api/usage/me",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("me-t1")},
        )
        assert resp.status_code == 200
        assert resp.json()["data"]["total_requests"] == 11

    @pytest.mark.unit
    async def test_missing_header_returns_401(self, app_client):
        resp = app_client.get("/api/usage/me")
        assert resp.status_code == 401

    @pytest.mark.unit
    async def test_invalid_header_returns_401(self, app_client):
        resp = app_client.get("/api/usage/me", headers={"X-Subject-Id": "not-a-uuid"})
        assert resp.status_code == 401


class TestMyUsageSeries:
    """``GET /api/usage/me/series`` per-bucket time-series for the caller."""

    @pytest.mark.unit
    async def test_missing_header_returns_401(self, app_client):
        resp = app_client.get("/api/usage/me/series?window=30d&granularity=daily")
        assert resp.status_code == 401

    @pytest.mark.unit
    async def test_invalid_window_returns_400(self, app_client):
        # ⚠️ X-Tenant-Id is NOT optional decoration in these two window tests.
        # A tenant-less /me read is itself a 400, so without the header both
        # assertions below would pass while `window` was never parsed at all —
        # green for a reason the test does not name. The header makes the 400
        # attributable to the window.
        resp = app_client.get(
            "/api/usage/me/series?window=24h",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": T("ms-window")},
        )
        assert resp.status_code == 400

    @pytest.mark.unit
    async def test_zero_window_returns_400(self, app_client):
        resp = app_client.get(
            "/api/usage/me/series?window=0d",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": T("ms-window")},
        )
        assert resp.status_code == 400

    @pytest.mark.unit
    async def test_empty_window_daily_returns_no_buckets(self, app_client):
        me = uuid.uuid4()
        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("ms-empty")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["buckets"] == []
        # No cost_daily rows for this subject → cost_unavailable.
        assert body["cost_unavailable"] is True
        assert body["granularity"] == "daily"
        assert body["window"] == "30d"

    @pytest.mark.unit
    async def test_daily_buckets_combine_requests_and_cost(self, db_engine, app_client):
        me = uuid.uuid4()
        today = datetime.now(UTC).date()
        await _seed_daily_usage_with_subject(db_engine, T("ms-t1"), me, count=12, bucket_date=today)
        await _seed_cost_daily_with_subject(
            db_engine,
            T("ms-t1"),
            me,
            cost_usd=Decimal("0.42"),
            input_tokens=200,
            output_tokens=100,
            bucket_date=today,
        )

        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("ms-t1")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert len(body["buckets"]) == 1
        bucket = body["buckets"][0]
        assert bucket["requests"] == 12
        assert bucket["tokens"] == 300
        assert Decimal(bucket["cost_usd"]) == Decimal("0.42")
        assert body["cost_unavailable"] is False

    @pytest.mark.unit
    async def test_tenant_isolation_other_subject_excluded(self, db_engine, app_client):
        me = uuid.uuid4()
        other = uuid.uuid4()
        today = datetime.now(UTC).date()
        await _seed_daily_usage_with_subject(db_engine, T("ms-t2"), me, count=5, bucket_date=today)
        await _seed_daily_usage_with_subject(
            db_engine, T("ms-t2"), other, count=99, bucket_date=today
        )

        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("ms-t2")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert sum(b["requests"] for b in body["buckets"]) == 5

    @pytest.mark.unit
    async def test_hourly_granularity_marks_cost_unavailable(self, db_engine, app_client):
        me = uuid.uuid4()
        # Seed an hourly row directly.
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        bucket = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        async with factory() as session:
            session.add(
                MeteringHourly(
                    tenant_id=T("ms-t3"),
                    license_id=uuid.uuid4(),
                    subject_id=me,
                    app_id=uuid.uuid4(),
                    plan_id=uuid.uuid4(),
                    service_key="inference-router",
                    bucket_hour=bucket,
                    request_count=4,
                    denied_count=0,
                )
            )
            await session.commit()

        resp = app_client.get(
            "/api/usage/me/series?window=7d&granularity=hourly",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("ms-t3")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["granularity"] == "hourly"
        assert body["cost_unavailable"] is True
        assert len(body["buckets"]) == 1
        assert body["buckets"][0]["requests"] == 4
        assert Decimal(body["buckets"][0]["cost_usd"]) == Decimal("0")


class TestMyUsageBreakdown:
    """``GET /api/usage/me/breakdown`` per-service usage + cost for the caller."""

    @pytest.mark.unit
    async def test_missing_header_returns_401(self, app_client):
        resp = app_client.get("/api/usage/me/breakdown?window=30d")
        assert resp.status_code == 401

    @pytest.mark.unit
    async def test_invalid_window_returns_400(self, app_client):
        # X-Tenant-Id present so this 400 is the WINDOW's, not the missing
        # tenant's — see the note in TestMyUsageSeries.
        resp = app_client.get(
            "/api/usage/me/breakdown?window=garbage",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": T("mb-window")},
        )
        assert resp.status_code == 400

    @pytest.mark.unit
    async def test_empty_window_returns_no_services(self, app_client):
        me = uuid.uuid4()
        resp = app_client.get(
            "/api/usage/me/breakdown?window=30d",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("mb-empty")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["services"] == []
        assert body["cost_unavailable"] is True

    @pytest.mark.unit
    async def test_per_service_breakdown_combines_requests_and_cost(self, db_engine, app_client):
        me = uuid.uuid4()
        today = datetime.now(UTC).date()
        # Two services with requests; one of them also has a cost row.
        await _seed_daily_usage_with_subject(
            db_engine, T("mb-t1"), me, service_key="inference-router", count=60, bucket_date=today
        )
        await _seed_daily_usage_with_subject(
            db_engine, T("mb-t1"), me, service_key="kag", count=20, bucket_date=today
        )
        await _seed_cost_daily_with_subject(
            db_engine,
            T("mb-t1"),
            me,
            cost_usd=Decimal("1.25"),
            input_tokens=400,
            output_tokens=200,
            bucket_date=today,
        )

        resp = app_client.get(
            "/api/usage/me/breakdown?window=30d",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("mb-t1")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        # Sorted by requests desc.
        assert body["services"][0]["service"] == "inference-router"
        assert body["services"][0]["requests"] == 60
        assert body["services"][0]["tokens"] == 600
        assert Decimal(body["services"][0]["cost_usd"]) == Decimal("1.25")
        # Second service has request count but no cost row.
        assert body["services"][1]["service"] == "kag"
        assert body["services"][1]["requests"] == 20
        assert body["services"][1]["tokens"] == 0
        assert Decimal(body["services"][1]["cost_usd"]) == Decimal("0")
        # ...and that "0" is an ABSENCE, not a measured zero. This assertion
        # used to demand `cost_unavailable is False` on exactly this shape,
        # i.e. it pinned the all-or-nothing defect as correct: `kag` had 20
        # metered requests and no cost row, and the response said cost was
        # fully available. The gap must now be expressible AND named.
        assert body["cost_unavailable"] is True
        assert body["cost_unavailable_services"] == ["kag"]

    @pytest.mark.unit
    async def test_tenant_isolation_other_subject_excluded(self, db_engine, app_client):
        me = uuid.uuid4()
        other = uuid.uuid4()
        today = datetime.now(UTC).date()
        await _seed_daily_usage_with_subject(
            db_engine, T("mb-t2"), me, service_key="srv", count=3, bucket_date=today
        )
        await _seed_daily_usage_with_subject(
            db_engine, T("mb-t2"), other, service_key="srv", count=999, bucket_date=today
        )
        resp = app_client.get(
            "/api/usage/me/breakdown?window=30d",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("mb-t2")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert len(body["services"]) == 1
        assert body["services"][0]["requests"] == 3


def _assert_refusal_carries_no_usage(resp) -> None:
    """Assert a ``/me`` refusal returned NO usage payload, structurally.

    ⚠️ NOT a substring search for the seeded figure, which is what this helper
    replaced. The error envelope carries a ``request_id`` uuid and an ISO
    timestamp, so ``assert "700" not in resp.text`` fails whenever those happen
    to contain the digits — measured: request_id ``…9094-ebe5-7003-…`` reddened
    this very check on its first run, and a seconds field of ``:11.004…`` would
    do the same to a money canary. A canary that fires on the clock cannot
    distinguish a leak from bad luck, so assert on the SHAPE: a refusal must
    carry no ``data`` member and none of the field names any usage/cost payload
    would serialise.
    """
    body = resp.json()
    assert "data" not in body, f"refusal returned a data payload: {body!r}"
    text = resp.text
    for field in (
        "total_requests",
        "total_denied",
        "total_cost_usd",
        "by_service",
        "by_license",
        "by_provider",
        "buckets",
        "services",
    ):
        assert field not in text, f"refusal leaked the {field!r} field: {text!r}"


class TestMeRoutesAreTenantScoped:
    """The four ``/me`` reads must be scoped to the ACTIVE TENANT, not just the subject.

    ⛔ WHAT THIS CLASS EXISTS TO PREVENT. ``X-Subject-Id`` names one human, and
    ``get_subject_id`` validates only the SHAPE of that header — it is a uuid
    parse, not a token check and not a tenant binding. One human legitimately
    holds licences in several tenants: that is precisely what the admin route
    ``/api/usage/user/{subject_id}`` sums on purpose, and
    ``TestUsageByUserAdmin::test_aggregates_across_tenants`` asserts it does.
    The four ``/me`` routes applied the SAME subject-only predicate while being
    rendered into one workspace's dashboard, so tenant A's customer read tenant
    B's request counts, token counts and — on ``/api/costs/me`` — B's money.

    Every ``test_*_excludes_other_tenants_*`` method below is a NEGATIVE CONTROL
    and MUST FAIL on the pre-fix code. The seeded "other tenant" figures are
    deliberately an order of magnitude larger than the subject's own, so a
    regression does not merely shift a number — it fails the equality outright.
    Each one also asserts the echoed ``tenant_id`` where the payload carries it:
    a response whose scope is correct but which refuses to name the tenant it
    scoped to is not auditable afterwards.

    The missing-header methods assert **400 with no rows**. Not 401 (the caller
    authenticated upstream; re-authenticating cannot supply a tenant) and
    emphatically not an empty 200 — an un-scoped caller answered with zero rows
    is indistinguishable from a caller who genuinely has no usage, which is how
    a silent scope failure gets read as "this customer is idle".
    """

    # Figures chosen so MINE + THEIRS is never equal to MINE, and so the leaked
    # total is visible in an assertion message rather than plausible.
    MINE_REQUESTS = 7
    THEIRS_REQUESTS = 700
    MINE_COST = Decimal("0.11")
    THEIRS_COST = Decimal("11.00")

    # --- GET /api/usage/me ---------------------------------------------------

    @pytest.mark.unit
    async def test_usage_me_excludes_other_tenants_rows(self, db_engine, app_client):
        """CROSS-TENANT NEGATIVE CONTROL — fails pre-fix with 707 != 7."""
        me = uuid.uuid4()
        await _seed_daily_usage_with_subject(
            db_engine, T("xt-u-a"), me, count=self.MINE_REQUESTS, denied=1
        )
        await _seed_daily_usage_with_subject(
            db_engine, T("xt-u-b"), me, count=self.THEIRS_REQUESTS, denied=13
        )

        resp = app_client.get(
            "/api/usage/me",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-u-a")},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["total_requests"] == self.MINE_REQUESTS
        assert data["total_denied"] == 1
        # The response must NAME the scope it applied; pre-fix this was null.
        assert data["tenant_id"] == T("xt-u-a")

    @pytest.mark.unit
    async def test_usage_me_without_tenant_header_is_400_and_returns_no_rows(
        self, db_engine, app_client
    ):
        """MISSING-HEADER CONTROL — 400, and the seeded rows appear nowhere."""
        me = uuid.uuid4()
        await _seed_daily_usage_with_subject(
            db_engine, T("xt-u-nh"), me, count=self.THEIRS_REQUESTS
        )

        resp = app_client.get("/api/usage/me", headers={"X-Subject-Id": str(me)})
        assert resp.status_code == 400
        _assert_refusal_carries_no_usage(resp)

    @pytest.mark.unit
    async def test_usage_me_single_tenant_is_unchanged(self, db_engine, app_client):
        """REGRESSION CONTROL — the ordinary one-tenant read still answers in full."""
        me = uuid.uuid4()
        other = uuid.uuid4()
        await _seed_daily_usage_with_subject(db_engine, T("xt-u-reg"), me, count=11, denied=2)
        # A different subject in the SAME tenant is still excluded: this change
        # ADDS a tenant predicate, it does not replace the subject one.
        await _seed_daily_usage_with_subject(db_engine, T("xt-u-reg"), other, count=99)

        resp = app_client.get(
            "/api/usage/me",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-u-reg")},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["total_requests"] == 11
        assert data["total_denied"] == 2

    # --- GET /api/usage/me/series -------------------------------------------

    @pytest.mark.unit
    async def test_usage_me_series_excludes_other_tenants_rows(self, db_engine, app_client):
        """CROSS-TENANT NEGATIVE CONTROL — fails pre-fix on both requests and cost."""
        me = uuid.uuid4()
        today = datetime.now(UTC).date()
        for tenant, requests, cost in (
            (T("xt-s-a"), self.MINE_REQUESTS, self.MINE_COST),
            (T("xt-s-b"), self.THEIRS_REQUESTS, self.THEIRS_COST),
        ):
            await _seed_daily_usage_with_subject(
                db_engine, tenant, me, count=requests, bucket_date=today
            )
            await _seed_cost_daily_with_subject(
                db_engine,
                tenant,
                me,
                cost_usd=cost,
                input_tokens=100,
                output_tokens=50,
                bucket_date=today,
            )

        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-s-a")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert sum(b["requests"] for b in body["buckets"]) == self.MINE_REQUESTS
        # Money: the incurred figure must be tenant A's alone. Asserted on
        # cost_usd rather than billed_cost_usd so the check does not depend on
        # the default markup this tenant happens to resolve.
        assert sum(Decimal(b["cost_usd"]) for b in body["buckets"]) == self.MINE_COST
        # Tokens leak on the same predicate and are the input to billing.
        assert sum(b["tokens"] for b in body["buckets"]) == 150

    @pytest.mark.unit
    async def test_usage_me_series_hourly_excludes_other_tenants_rows(self, db_engine, app_client):
        """CROSS-TENANT NEGATIVE CONTROL for the HOURLY branch.

        ``_usage_series_payload`` takes a different query path for hourly
        granularity (``metering_hourly``, requests only). The scope predicate is
        applied in both branches, so both branches need their own control — a
        test only of the daily branch would leave half the route unmeasured.
        """
        me = uuid.uuid4()
        bucket = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            for tenant, requests in ((T("xt-h-a"), 4), (T("xt-h-b"), 400)):
                session.add(
                    MeteringHourly(
                        tenant_id=tenant,
                        license_id=uuid.uuid4(),
                        subject_id=me,
                        app_id=uuid.uuid4(),
                        plan_id=uuid.uuid4(),
                        service_key="inference-router",
                        bucket_hour=bucket,
                        request_count=requests,
                        denied_count=0,
                    )
                )
            await session.commit()

        resp = app_client.get(
            "/api/usage/me/series?window=7d&granularity=hourly",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-h-a")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert sum(b["requests"] for b in body["buckets"]) == 4

    @pytest.mark.unit
    async def test_usage_me_series_without_tenant_header_is_400_and_returns_no_rows(
        self, db_engine, app_client
    ):
        """MISSING-HEADER CONTROL."""
        me = uuid.uuid4()
        await _seed_daily_usage_with_subject(
            db_engine, T("xt-s-nh"), me, count=self.THEIRS_REQUESTS
        )

        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me)},
        )
        assert resp.status_code == 400
        _assert_refusal_carries_no_usage(resp)

    @pytest.mark.unit
    async def test_usage_me_series_single_tenant_is_unchanged(self, db_engine, app_client):
        """REGRESSION CONTROL — requests and cost still combine on one bucket."""
        me = uuid.uuid4()
        today = datetime.now(UTC).date()
        await _seed_daily_usage_with_subject(
            db_engine, T("xt-s-reg"), me, count=12, bucket_date=today
        )
        await _seed_cost_daily_with_subject(
            db_engine,
            T("xt-s-reg"),
            me,
            cost_usd=Decimal("0.42"),
            input_tokens=200,
            output_tokens=100,
            bucket_date=today,
        )

        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-s-reg")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert len(body["buckets"]) == 1
        assert body["buckets"][0]["requests"] == 12
        assert body["buckets"][0]["tokens"] == 300
        assert Decimal(body["buckets"][0]["cost_usd"]) == Decimal("0.42")
        assert body["cost_unavailable"] is False

    # --- GET /api/usage/me/breakdown ----------------------------------------

    @pytest.mark.unit
    async def test_usage_me_breakdown_excludes_other_tenants_services(self, db_engine, app_client):
        """CROSS-TENANT NEGATIVE CONTROL — a service used only in B must not appear.

        Pre-fix this returned TWO service rows, and ``srv-b``'s cost with them,
        so a customer could enumerate which services a colleague's other
        workspace uses and what that workspace spent.
        """
        me = uuid.uuid4()
        today = datetime.now(UTC).date()
        await _seed_daily_usage_with_subject(
            db_engine,
            T("xt-b-a"),
            me,
            service_key="srv-a",  # pragma: allowlist secret
            count=self.MINE_REQUESTS,
            bucket_date=today,
        )
        await _seed_cost_daily_with_subject(
            db_engine,
            T("xt-b-a"),
            me,
            cost_usd=self.MINE_COST,
            bucket_date=today,
            service_key="srv-a",  # pragma: allowlist secret
        )
        await _seed_daily_usage_with_subject(
            db_engine,
            T("xt-b-b"),
            me,
            service_key="srv-b",  # pragma: allowlist secret
            count=self.THEIRS_REQUESTS,
            bucket_date=today,
        )
        await _seed_cost_daily_with_subject(
            db_engine,
            T("xt-b-b"),
            me,
            cost_usd=self.THEIRS_COST,
            bucket_date=today,
            service_key="srv-b",  # pragma: allowlist secret
        )

        resp = app_client.get(
            "/api/usage/me/breakdown?window=30d",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-b-a")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert [s["service"] for s in body["services"]] == ["srv-a"]
        assert body["services"][0]["requests"] == self.MINE_REQUESTS
        assert Decimal(body["services"][0]["cost_usd"]) == self.MINE_COST
        # The other tenant's service key must not leak through the gap list either.
        assert "srv-b" not in body["cost_unavailable_services"]
        assert "srv-b" not in resp.text

    @pytest.mark.unit
    async def test_usage_me_breakdown_without_tenant_header_is_400_and_returns_no_rows(
        self, db_engine, app_client
    ):
        """MISSING-HEADER CONTROL."""
        me = uuid.uuid4()
        await _seed_daily_usage_with_subject(
            db_engine,
            T("xt-b-nh"),
            me,
            service_key="srv-leak",  # pragma: allowlist secret
            count=self.THEIRS_REQUESTS,
        )

        resp = app_client.get(
            "/api/usage/me/breakdown?window=30d",
            headers={"X-Subject-Id": str(me)},
        )
        assert resp.status_code == 400
        _assert_refusal_carries_no_usage(resp)
        # "srv-leak" has non-hex letters, so it cannot collide with a request-id.
        assert "srv-leak" not in resp.text

    @pytest.mark.unit
    async def test_usage_me_breakdown_single_tenant_is_unchanged(self, db_engine, app_client):
        """REGRESSION CONTROL — including the absent-vs-zero cost gap flag."""
        me = uuid.uuid4()
        today = datetime.now(UTC).date()
        await _seed_daily_usage_with_subject(
            db_engine,
            T("xt-b-reg"),
            me,
            service_key="inference-router",
            count=60,
            bucket_date=today,
        )
        await _seed_daily_usage_with_subject(
            db_engine, T("xt-b-reg"), me, service_key="kag", count=20, bucket_date=today
        )
        await _seed_cost_daily_with_subject(
            db_engine,
            T("xt-b-reg"),
            me,
            cost_usd=Decimal("1.25"),
            input_tokens=400,
            output_tokens=200,
            bucket_date=today,
        )

        resp = app_client.get(
            "/api/usage/me/breakdown?window=30d",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-b-reg")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert [s["service"] for s in body["services"]] == ["inference-router", "kag"]
        assert Decimal(body["services"][0]["cost_usd"]) == Decimal("1.25")
        assert body["services"][1]["tokens"] == 0
        # `kag`'s zero is still an ABSENCE, not a measured zero — the tenant
        # predicate must not have collapsed that distinction.
        assert body["cost_unavailable"] is True
        assert body["cost_unavailable_services"] == ["kag"]

    # --- GET /api/costs/me --------------------------------------------------

    @pytest.mark.unit
    async def test_costs_me_excludes_other_tenants_spend(self, db_engine, app_client):
        """CROSS-TENANT NEGATIVE CONTROL — fails pre-fix with 11.11 != 0.11.

        This is the money route: the figure returned here is what a customer
        reads as their own spend. Pre-fix it was the subject's spend in every
        tenant they belong to, added together.
        """
        me = uuid.uuid4()
        await _seed_cost_daily_with_subject(
            db_engine,
            T("xt-c-a"),
            me,
            cost_usd=self.MINE_COST,
            input_tokens=100,
            output_tokens=50,
        )
        await _seed_cost_daily_with_subject(
            db_engine,
            T("xt-c-b"),
            me,
            cost_usd=self.THEIRS_COST,
            input_tokens=9000,
            output_tokens=4000,
        )

        resp = app_client.get(
            "/api/costs/me",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-c-a")},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert Decimal(str(data["total_cost_usd"])) == self.MINE_COST
        assert data["total_input_tokens"] == 100
        assert data["total_output_tokens"] == 50
        assert data["tenant_id"] == T("xt-c-a")

    @pytest.mark.unit
    async def test_costs_me_without_tenant_header_is_400_and_returns_no_rows(
        self, db_engine, app_client
    ):
        """MISSING-HEADER CONTROL."""
        me = uuid.uuid4()
        await _seed_cost_daily_with_subject(db_engine, T("xt-c-nh"), me, cost_usd=self.THEIRS_COST)

        resp = app_client.get("/api/costs/me", headers={"X-Subject-Id": str(me)})
        assert resp.status_code == 400
        _assert_refusal_carries_no_usage(resp)

    @pytest.mark.unit
    async def test_costs_me_single_tenant_is_unchanged(self, db_engine, app_client):
        """REGRESSION CONTROL — including the ``provider`` narrowing filter."""
        me = uuid.uuid4()
        other = uuid.uuid4()
        await _seed_cost_daily_with_subject(
            db_engine, T("xt-c-reg"), me, cost_usd=Decimal("0.25"), provider="claude"
        )
        await _seed_cost_daily_with_subject(
            db_engine, T("xt-c-reg"), me, cost_usd=Decimal("0.30"), provider="openai"
        )
        await _seed_cost_daily_with_subject(
            db_engine, T("xt-c-reg"), other, cost_usd=Decimal("9.99")
        )

        headers = {"X-Subject-Id": str(me), "X-Tenant-Id": T("xt-c-reg")}
        resp = app_client.get("/api/costs/me", headers=headers)
        assert resp.status_code == 200
        assert Decimal(str(resp.json()["data"]["total_cost_usd"])) == Decimal("0.55")

        narrowed = app_client.get("/api/costs/me?provider=claude", headers=headers)
        assert narrowed.status_code == 200
        assert Decimal(str(narrowed.json()["data"]["total_cost_usd"])) == Decimal("0.25")

    # --- shared refusal shape ------------------------------------------------

    @pytest.mark.parametrize(
        "path",
        [
            "/api/usage/me",
            "/api/usage/me/series?window=30d&granularity=daily",
            "/api/usage/me/breakdown?window=30d",
            "/api/costs/me",
        ],
    )
    @pytest.mark.parametrize("bad_tenant", ["", "   ", "not-a-tenant", "Bearer abc.def.ghi"])
    @pytest.mark.unit
    async def test_unresolvable_tenant_header_is_400_on_every_me_route(
        self, app_client, path, bad_tenant
    ):
        """All four routes refuse an unresolvable tenant identically.

        Parametrised across the routes on purpose: four hand-written copies of
        this assertion is four chances for one route to drift open while the
        other three stay green. ``""``/``"   "`` is the shape ``license-authz``
        sends when introspection returns no tenant, and the ``Bearer …`` case is
        a value this header has actually been observed carrying in production —
        it must be refused, and must not be echoed back.
        """
        resp = app_client.get(
            path,
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": bad_tenant},
        )
        assert resp.status_code == 400
        assert "abc.def.ghi" not in resp.text

    @pytest.mark.parametrize(
        "path",
        [
            "/api/usage/me",
            "/api/usage/me/series?window=30d&granularity=daily",
            "/api/usage/me/breakdown?window=30d",
            "/api/costs/me",
        ],
    )
    @pytest.mark.unit
    async def test_missing_subject_still_401_not_400_on_every_me_route(self, app_client, path):
        """The subject check must keep its own 401 and keep running FIRST.

        Adding the tenant requirement must not reclassify "who are you" as "which
        tenant" — a caller with no ``X-Subject-Id`` is still told 401, even when
        the tenant header is also absent, so the two failures stay tellable apart
        in a log.
        """
        resp = app_client.get(path)
        assert resp.status_code == 401


class TestTenantUsageSeriesBreakdown:
    """``/api/usage/tenant/{id}/series`` + ``/breakdown`` — tenant-scoped.

    These are the siblings of the subject ``/me/*`` endpoints used by fr's
    Usage page. The crux: real cost/usage rolls up under the NULL-subject
    sentinel, so a tenant view must surface rows that a per-subject ``/me``
    view never matches.
    """

    @pytest.mark.unit
    async def test_series_surfaces_sentinel_subject_rows_that_me_misses(
        self, db_engine, app_client
    ):
        # Seed under the tenant with the DEFAULT (sentinel) subject — the
        # real-world rollup shape. _seed_* helpers leave subject_id at the
        # model default (00000000-…003).
        today = datetime.now(UTC).date()
        await _seed_daily_usage(db_engine, tenant_id=T("tn-series"), count=42, bucket_date=today)
        await _seed_cost_daily(
            db_engine,
            tenant_id=T("tn-series"),
            cost_usd=Decimal("3.50"),
            input_tokens=1000,
            output_tokens=500,
            bucket_date=today,
        )

        # Tenant view: surfaces the data.
        resp = app_client.get(
            f"/api/usage/tenant/{T('tn-series')}/series?window=30d&granularity=daily"
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert len(body["buckets"]) == 1
        assert body["buckets"][0]["requests"] == 42
        assert body["buckets"][0]["tokens"] == 1500
        assert Decimal(body["buckets"][0]["cost_usd"]) == Decimal("3.50")
        assert body["cost_unavailable"] is False

        # Same data via /me with a human subject id → empty (the bug we fix).
        # The tenant header names the SAME tenant, so the emptiness is entirely
        # the subject mismatch: the rollup sits under the sentinel subject.
        me_resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": T("tn-series")},
        )
        assert me_resp.status_code == 200
        assert me_resp.json()["data"]["buckets"] == []

    @pytest.mark.unit
    async def test_series_tenant_isolation(self, db_engine, app_client):
        today = datetime.now(UTC).date()
        await _seed_daily_usage(db_engine, tenant_id=T("tn-iso-a"), count=7, bucket_date=today)
        await _seed_daily_usage(db_engine, tenant_id=T("tn-iso-b"), count=99, bucket_date=today)

        resp = app_client.get(
            f"/api/usage/tenant/{T('tn-iso-a')}/series?window=30d&granularity=daily"
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        # Only tenant A's rows — zero of tenant B's 99.
        assert sum(b["requests"] for b in body["buckets"]) == 7

    @pytest.mark.unit
    async def test_breakdown_tenant_scoped_per_service(self, db_engine, app_client):
        today = datetime.now(UTC).date()
        await _seed_daily_usage(
            db_engine,
            tenant_id=T("tn-bd"),
            service_key="inference-router",
            count=30,
            bucket_date=today,
        )
        await _seed_daily_usage(
            db_engine, tenant_id=T("tn-bd"), service_key="kag", count=10, bucket_date=today
        )
        await _seed_cost_daily(
            db_engine, tenant_id=T("tn-bd"), cost_usd=Decimal("2.00"), bucket_date=today
        )

        resp = app_client.get(f"/api/usage/tenant/{T('tn-bd')}/breakdown?window=30d")
        assert resp.status_code == 200
        body = resp.json()["data"]
        services = {s["service"]: s for s in body["services"]}
        assert services["inference-router"]["requests"] == 30
        assert services["kag"]["requests"] == 10
        # `kag` has requests and no cost_daily row — a partial gap, so the flag
        # is set and the gap is named. (This assertion previously read
        # `is False`, pinning the all-or-nothing defect.)
        assert body["cost_unavailable"] is True
        assert body["cost_unavailable_services"] == ["kag"]
        assert Decimal(services["inference-router"]["cost_usd"]) == Decimal("2.00")

    @pytest.mark.unit
    async def test_breakdown_tenant_isolation(self, db_engine, app_client):
        today = datetime.now(UTC).date()
        await _seed_daily_usage(
            db_engine, tenant_id=T("tn-bd-a"), service_key="srv", count=4, bucket_date=today
        )
        await _seed_daily_usage(
            db_engine, tenant_id=T("tn-bd-b"), service_key="srv", count=400, bucket_date=today
        )
        resp = app_client.get(f"/api/usage/tenant/{T('tn-bd-a')}/breakdown?window=30d")
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert sum(s["requests"] for s in body["services"]) == 4


class TestCostsBySubjectFilter:
    """Costs endpoints accept the same subject_id filter."""

    @pytest.mark.unit
    async def test_tenant_filter_by_subject(self, db_engine, app_client):
        sub_a = uuid.uuid4()
        sub_b = uuid.uuid4()
        await _seed_cost_daily_with_subject(db_engine, T("ct1"), sub_a, cost_usd=Decimal("1.00"))
        await _seed_cost_daily_with_subject(db_engine, T("ct1"), sub_b, cost_usd=Decimal("2.00"))

        resp = app_client.get(f"/api/costs/tenant/{T('ct1')}")
        assert resp.status_code == 200
        assert Decimal(str(resp.json()["data"]["total_cost_usd"])) == Decimal("3.00")

        resp = app_client.get(f"/api/costs/tenant/{T('ct1')}?subject_id={sub_a}")
        assert resp.status_code == 200
        assert Decimal(str(resp.json()["data"]["total_cost_usd"])) == Decimal("1.00")

    @pytest.mark.unit
    async def test_costs_by_user_admin(self, db_engine, app_client):
        sub = uuid.uuid4()
        await _seed_cost_daily_with_subject(db_engine, T("ctA"), sub, cost_usd=Decimal("0.40"))
        await _seed_cost_daily_with_subject(db_engine, T("ctB"), sub, cost_usd=Decimal("0.60"))
        resp = app_client.get(f"/api/costs/user/{sub}")
        assert resp.status_code == 200
        assert Decimal(str(resp.json()["data"]["total_cost_usd"])) == Decimal("1.00")

    @pytest.mark.unit
    async def test_my_costs_returns_caller(self, db_engine, app_client):
        me = uuid.uuid4()
        other = uuid.uuid4()
        await _seed_cost_daily_with_subject(db_engine, T("mc1"), me, cost_usd=Decimal("0.25"))
        await _seed_cost_daily_with_subject(db_engine, T("mc1"), other, cost_usd=Decimal("9.99"))
        resp = app_client.get(
            "/api/costs/me",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("mc1")},
        )
        assert resp.status_code == 200
        assert Decimal(str(resp.json()["data"]["total_cost_usd"])) == Decimal("0.25")

    @pytest.mark.unit
    async def test_my_costs_missing_header_401(self, app_client):
        resp = app_client.get("/api/costs/me")
        assert resp.status_code == 401
