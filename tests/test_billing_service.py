"""Tests for billing service."""

import uuid
from datetime import date
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import observability.metrics as metrics
from models.db import (
    NULL_LICENSE_ID,
    AppCostRate,
    CostDaily,
    MeteringDaily,
    MeteringMonthlyUsage,
    ProviderCost,
    TenantConfig,
    TokenBudget,
    TokenUsage,
)
from service_auth import ServiceTokenProvider
from services.billing import (
    LicensesUnavailableError,
    fetch_licenses,
    generate_billing,
    get_app_cost_rates,
    get_markup_pct,
    get_provider_costs,
)
from services.markup import quantize_money
from tests.conftest import T


class TestGetMarkupPct:
    async def test_default_markup(self, db_session, test_settings):
        """No tenant config => returns the settings default (20.0 since 2026-09-10)."""
        result = await get_markup_pct(T("unknown-tenant"), db_session, test_settings)
        assert result == test_settings.default_markup_pct

    async def test_tenant_override(self, db_session, test_settings):
        """Tenant with custom markup should return that value."""
        db_session.add(TenantConfig(tenant_id=T("vip"), markup_pct=Decimal("15.00")))
        await db_session.commit()
        result = await get_markup_pct(T("vip"), db_session, test_settings)
        assert result == 15.0

    async def test_global_db_default(self, db_session, test_settings):
        """DB-level __default__ tenant config overrides settings default."""
        db_session.add(TenantConfig(tenant_id=T("__default__"), markup_pct=Decimal("22.00")))
        await db_session.commit()
        result = await get_markup_pct(T("other-tenant"), db_session, test_settings)
        assert result == 22.0

    async def test_tenant_specific_beats_db_default(self, db_session, test_settings):
        """Tenant-specific markup takes priority over __default__."""
        db_session.add(TenantConfig(tenant_id=T("__default__"), markup_pct=Decimal("20.00")))
        db_session.add(TenantConfig(tenant_id=T("priority-tenant"), markup_pct=Decimal("45.50")))
        await db_session.commit()

        result = await get_markup_pct(T("priority-tenant"), db_session, test_settings)
        assert result == 45.50

    async def test_zero_markup(self, db_session, test_settings):
        """Tenant with 0% markup should return 0.0."""
        db_session.add(TenantConfig(tenant_id=T("free-tier"), markup_pct=Decimal("0.00")))
        await db_session.commit()
        result = await get_markup_pct(T("free-tier"), db_session, test_settings)
        assert result == 0.0

    async def test_high_markup(self, db_session, test_settings):
        """Tenant with very high markup should return exact value."""
        db_session.add(TenantConfig(tenant_id=T("premium"), markup_pct=Decimal("99.99")))
        await db_session.commit()
        result = await get_markup_pct(T("premium"), db_session, test_settings)
        assert result == 99.99

    async def test_fractional_markup(self, db_session, test_settings):
        """Fractional markup (e.g. 12.75%) should work."""
        db_session.add(TenantConfig(tenant_id=T("frac"), markup_pct=Decimal("12.75")))
        await db_session.commit()
        result = await get_markup_pct(T("frac"), db_session, test_settings)
        assert result == 12.75


class TestGetProviderCosts:
    async def test_empty(self, db_session):
        """No providers => returns empty dict."""
        result = await get_provider_costs(db_session)
        assert result == {}

    async def test_with_data(self, db_session):
        """Insert ProviderCost rows, verify they are returned correctly."""
        db_session.add(
            ProviderCost(
                provider="claude",
                input_per_1m_usd=Decimal("3.0000"),
                output_per_1m_usd=Decimal("15.0000"),
            )
        )
        db_session.add(
            ProviderCost(
                provider="deepseek",
                input_per_1m_usd=Decimal("0.1400"),
                output_per_1m_usd=Decimal("0.2800"),
            )
        )
        await db_session.commit()

        result = await get_provider_costs(db_session)
        assert "claude" in result
        assert "deepseek" in result
        assert result["claude"]["input_per_1m_usd"] == Decimal("3.0000")
        assert result["claude"]["output_per_1m_usd"] == Decimal("15.0000")
        assert result["deepseek"]["input_per_1m_usd"] == Decimal("0.1400")
        assert result["deepseek"]["output_per_1m_usd"] == Decimal("0.2800")

    async def test_single_provider(self, db_session):
        """A single provider should return a dict with one entry."""
        db_session.add(
            ProviderCost(
                provider="openai",
                input_per_1m_usd=Decimal("2.5000"),
                output_per_1m_usd=Decimal("10.0000"),
            )
        )
        await db_session.commit()

        result = await get_provider_costs(db_session)
        assert len(result) == 1
        assert "openai" in result


class TestGetAppCostRates:
    async def test_empty(self, db_session):
        """No rates => empty dict."""
        result = await get_app_cost_rates(db_session)
        assert result == {}

    async def test_with_data(self, db_session):
        """Insert AppCostRate rows, verify returned as dict."""
        db_session.add(AppCostRate(app_key="workers", cost_per_request_usd=Decimal("0.00100000")))
        db_session.add(AppCostRate(app_key="chatbot", cost_per_request_usd=Decimal("0.00050000")))
        await db_session.commit()

        result = await get_app_cost_rates(db_session)
        assert result["workers"] == Decimal("0.00100000")
        assert result["chatbot"] == Decimal("0.00050000")

    async def test_zero_cost_rate(self, db_session):
        """App with zero cost rate should be returned as Decimal('0')."""
        db_session.add(AppCostRate(app_key="free-app", cost_per_request_usd=Decimal("0.00000000")))
        await db_session.commit()

        result = await get_app_cost_rates(db_session)
        assert result["free-app"] == Decimal("0.00000000")


class TestFetchLicenses:
    """NOTE — four tests in this class used to ASSERT the defect.

    ``test_failure_returns_empty`` / ``test_non_200_returns_empty`` /
    ``test_404_returns_empty`` / ``test_timeout_returns_empty`` each pinned
    "any failure returns []", which is exactly what made a Keycloak outage
    indistinguishable from a tenant holding no licenses. They are rewritten (not
    removed) to assert the failure SURFACES; the success and genuine-empty cases
    are unchanged. See tests/test_license_scoped_debit.py for the
    route-level 502 proof.
    """

    async def test_unreachable_service_raises(self, test_settings):
        """No server running at test URL => must RAISE, not return []."""
        with pytest.raises(LicensesUnavailableError):
            await fetch_licenses(T("test-tenant"), test_settings)

    async def test_success(self, test_settings):
        """Successful HTTP response should return parsed JSON."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "lic1", "app": {"key": "workers"}, "plan": {"code": "free"}}
        ]

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("services.billing.httpx.AsyncClient", return_value=mock_client):
            result = await fetch_licenses(T("test-tenant"), test_settings)
            assert len(result) == 1
            assert result[0]["id"] == "lic1"

    async def test_non_200_raises(self, test_settings):
        """Non-200 HTTP status must RAISE, not answer with an empty list."""
        mock_response = httpx.Response(
            500, request=httpx.Request("GET", "http://licenses/internal/licenses")
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("services.billing.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(LicensesUnavailableError),
        ):
            await fetch_licenses(T("test-tenant"), test_settings)

    async def test_404_raises(self, test_settings):
        """404 (wrong URL / unknown tenant) must RAISE — it is not "no licenses"."""
        mock_response = httpx.Response(
            404, request=httpx.Request("GET", "http://licenses/internal/licenses")
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("services.billing.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(LicensesUnavailableError),
        ):
            await fetch_licenses(T("test-tenant"), test_settings)

    async def test_timeout_raises(self, test_settings):
        """An httpx timeout is logged and RE-RAISED (it used to be swallowed)."""
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.ReadTimeout("timed out"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("services.billing.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(LicensesUnavailableError),
        ):
            await fetch_licenses(T("test-tenant"), test_settings)

    async def test_multiple_licenses(self, test_settings):
        """Multiple licenses should all be returned."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "lic1", "app": {"key": "workers"}, "plan": {"code": "free"}},
            {"id": "lic2", "app": {"key": "chatbot"}, "plan": {"code": "pro"}},
        ]

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("services.billing.httpx.AsyncClient", return_value=mock_client):
            result = await fetch_licenses(T("test-tenant"), test_settings)
            assert len(result) == 2


class TestGenerateBilling:
    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_empty_billing(self, mock_fetch, db_session, test_settings):
        """No data for tenant => returns zeroed report."""
        mock_fetch.return_value = []

        result = await generate_billing(
            T("empty-billing-tenant"), "2026-03", db_session, test_settings
        )

        # NOTE: tautological — generate_billing echoes its tenant_id argument
        # back verbatim (services/billing.py:609), so both sides are T(...) of
        # the same label with no production transform in between.
        assert result["tenant_id"] == T("empty-billing-tenant")
        assert result["month"] == "2026-03"
        # ⚠️ ASSERT THE SETTING, NOT THE NUMBER. This read 30.0 until George set
        # the default to 20% on 2026-09-10, and a hardcoded literal here just
        # moves the rot to the next rate change.
        assert result["markup_pct"] == test_settings.default_markup_pct
        assert result["licenses"] == []
        assert result["totals"]["total_requests"] == 0
        # ⚠️ "0.00", not "0": `internal_cost_usd` is quantized at the point of
        # storage like the two columns it has to balance against. It used to be
        # written RAW (`str(total_internal)`), which is why
        # `customer − internal == margin` could fail at the cent.
        assert result["totals"]["internal_cost_usd"] == "0.00"
        assert result["totals"]["total_input_tokens"] == 0
        assert result["totals"]["total_output_tokens"] == 0

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_billing_with_usage(self, mock_fetch, db_session, test_settings):
        """Full billing report with seeded data should calculate costs correctly."""
        tenant = T("billing-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 10000},
            },
        ]

        # Seed AppCostRate
        db_session.add(AppCostRate(app_key="workers", cost_per_request_usd=Decimal("0.00100000")))

        # Seed MeteringDaily
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 5),
                request_count=100,
                denied_count=2,
            )
        )

        # Seed CostDaily
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="workers",
                provider="openai",
                bucket_date=date(2026, 3, 5),
                total_input_tokens=50000,
                total_output_tokens=10000,
                total_cost_usd=Decimal("0.500000"),
                request_count=100,
            )
        )

        # Seed TokenUsage
        db_session.add(
            TokenUsage(
                tenant_id=tenant,
                license_id=lic_id,
                period_type="monthly",
                period_start=date(2026, 3, 1),
                input_tokens=50000,
                output_tokens=10000,
                total_tokens=60000,
            )
        )

        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        # NOTE: tautological — generate_billing echoes tenant_id verbatim.
        assert result["tenant_id"] == tenant
        assert result["month"] == "2026-03"
        assert result["markup_pct"] == test_settings.default_markup_pct
        assert len(result["licenses"]) == 1

        lic_entry = result["licenses"][0]
        assert lic_entry["license_id"] == str(lic_id)
        assert lic_entry["app_key"] == "workers"
        assert lic_entry["plan_code"] == "pro"
        assert lic_entry["usage"]["request_count"] == 100
        assert lic_entry["usage"]["quota_monthly"] == 10000

        # Cost calculations:
        # app_request_cost = 0.001 * 100 = 0.1
        # llm_cost = 0.5
        assert lic_entry["costs"]["app_request_cost_usd"] == "0.10000000"
        assert lic_entry["costs"]["llm_cost_usd"] == "0.500000"

        # Token info
        assert lic_entry["tokens"]["input_tokens"] == 50000
        assert lic_entry["tokens"]["output_tokens"] == 10000

        # Totals
        totals = result["totals"]
        assert totals["total_requests"] == 100
        assert totals["total_input_tokens"] == 50000
        assert totals["total_output_tokens"] == 10000

        # Markup: internal_cost * (1 + default/100), derived from the setting so
        # the expectation follows a rate change instead of contradicting it.
        internal = Decimal("0.10000000") + Decimal("0.500000")
        customer = internal * (1 + Decimal(str(test_settings.default_markup_pct)) / 100)
        # ⚠️ `quantize_money`, not a bare `.quantize(...)`. A bare quantize in a
        # test silently takes ROUND_HALF_EVEN and would re-assert the very
        # omission the production code was fixed for — the expectation must be
        # spelled in the mode the money path actually uses.
        assert totals["customer_cost_usd"] == str(quantize_money(customer))

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_generate_billing_december(self, mock_fetch, db_session, test_settings):
        """December billing should correctly handle year boundary for month_end."""
        mock_fetch.return_value = []

        result = await generate_billing(T("dec-tenant"), "2026-12", db_session, test_settings)

        assert result["month"] == "2026-12"
        assert result["licenses"] == []

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_generate_billing_with_budget(self, mock_fetch, db_session, test_settings):
        """Billing should include token budget info when budget exists."""
        tenant = T("budget-billing-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "chatbot"},
                "plan": {"code": "basic", "quota_monthly": 5000},
            },
        ]

        # Seed budget
        db_session.add(
            TokenBudget(
                tenant_id=tenant,
                license_id=lic_id,
                budget_tokens=100_000,
                period="monthly",
                active=True,
            )
        )

        # Seed MeteringDaily
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="chatbot",
                bucket_date=date(2026, 3, 10),
                request_count=50,
                denied_count=0,
            )
        )

        # Seed TokenUsage
        db_session.add(
            TokenUsage(
                tenant_id=tenant,
                license_id=lic_id,
                period_type="monthly",
                period_start=date(2026, 3, 1),
                input_tokens=20000,
                output_tokens=10000,
                total_tokens=30000,
            )
        )

        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        assert lic_entry["tokens"]["budget_tokens"] == 100_000
        assert lic_entry["tokens"]["budget_remaining"] == 70000  # 100k - (20k+10k)
        assert lic_entry["tokens"]["budget_pct"] == 30.0  # (30k/100k)*100

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_generate_billing_quota_pct(self, mock_fetch, db_session, test_settings):
        """Quota percentage should be calculated correctly from request_count / quota_monthly."""
        tenant = T("quota-pct-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "api"},
                "plan": {"code": "starter", "quota_monthly": 1000},
            },
        ]

        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="api",
                bucket_date=date(2026, 3, 15),
                request_count=250,
                denied_count=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        assert lic_entry["usage"]["quota_pct"] == 25.0  # 250/1000 * 100

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_generate_billing_no_quota(self, mock_fetch, db_session, test_settings):
        """No quota_monthly => quota_pct should be None."""
        tenant = T("no-quota-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "unlimited", "quota_monthly": None},
            },
        ]

        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 1),
                request_count=5000,
                denied_count=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        assert lic_entry["usage"]["quota_pct"] is None
        assert lic_entry["usage"]["quota_monthly"] is None

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_generate_billing_multiple_providers(self, mock_fetch, db_session, test_settings):
        """Cost breakdown should track multiple providers separately."""
        tenant = T("multi-prov-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "chat"},
                "plan": {"code": "pro", "quota_monthly": 50000},
            },
        ]

        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="chat",
                bucket_date=date(2026, 3, 5),
                request_count=300,
                denied_count=0,
            )
        )

        # Two different providers with costs
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="chat",
                provider="openai",
                bucket_date=date(2026, 3, 5),
                total_input_tokens=100000,
                total_output_tokens=50000,
                total_cost_usd=Decimal("1.200000"),
                request_count=200,
            )
        )
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="chat",
                provider="anthropic",
                bucket_date=date(2026, 3, 5),
                total_input_tokens=50000,
                total_output_tokens=25000,
                total_cost_usd=Decimal("0.800000"),
                request_count=100,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        assert "openai" in lic_entry["costs"]["by_provider"]
        assert "anthropic" in lic_entry["costs"]["by_provider"]
        assert lic_entry["costs"]["by_provider"]["openai"] == "1.200000"
        assert lic_entry["costs"]["by_provider"]["anthropic"] == "0.800000"

        # Total LLM cost should be sum
        assert lic_entry["costs"]["llm_cost_usd"] == "2.000000"

        # Totals should aggregate providers
        assert "openai" in result["totals"]["cost_by_provider"]
        assert "anthropic" in result["totals"]["cost_by_provider"]

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_cost_only_license_no_metering(self, mock_fetch, db_session, test_settings):
        """License with cost_daily but no metering_daily should still appear (lines 203-214)."""
        tenant = T("cost-only-tenant")
        lic_id = uuid.uuid4()

        mock_fetch.return_value = []

        # Only CostDaily, no MeteringDaily for this license
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="inference",
                provider="deepseek",
                bucket_date=date(2026, 3, 8),
                total_input_tokens=10000,
                total_output_tokens=5000,
                total_cost_usd=Decimal("0.020000"),
                request_count=5,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        # Should have one license entry created from cost data
        assert len(result["licenses"]) == 1
        lic_entry = result["licenses"][0]
        assert lic_entry["license_id"] == str(lic_id)
        assert lic_entry["costs"]["llm_cost_usd"] == "0.020000"
        assert lic_entry["usage"]["request_count"] == 0
        assert lic_entry["costs"]["app_request_cost_usd"] == "0"

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_null_license_id_cost_skipped(self, mock_fetch, db_session, test_settings):
        """CostDaily rows with NULL_LICENSE_ID should be skipped (else: continue path)."""
        tenant = T("null-lic-cost-tenant")

        mock_fetch.return_value = []

        # CostDaily with NULL_LICENSE_ID - should be skipped
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=NULL_LICENSE_ID,
                service_key="inference",
                provider="openai",
                bucket_date=date(2026, 3, 10),
                total_input_tokens=1000,
                total_output_tokens=500,
                total_cost_usd=Decimal("0.010000"),
                request_count=1,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        # NULL_LICENSE_ID cost rows are skipped, so no license entries
        assert result["licenses"] == []
        assert result["totals"]["total_requests"] == 0

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_monthly_usage_overrides_daily_when_larger(
        self, mock_fetch, db_session, test_settings
    ):
        """MeteringMonthlyUsage.request_count > daily sum should be used (line 238)."""
        tenant = T("monthly-override-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 10000},
            },
        ]

        # Daily says 50 requests
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 5),
                request_count=50,
                denied_count=0,
            )
        )

        # Monthly says 150 (higher because current day not rolled up yet)
        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=date(2026, 3, 1),
                request_count=150,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        # Should use monthly value since it's larger
        assert lic_entry["usage"]["request_count"] == 150
        assert result["totals"]["total_requests"] == 150

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_monthly_usage_not_used_when_smaller(self, mock_fetch, db_session, test_settings):
        """MeteringMonthlyUsage < daily sum should NOT override."""
        tenant = T("monthly-smaller-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 10000},
            },
        ]

        # Daily says 200 requests
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 5),
                request_count=200,
                denied_count=0,
            )
        )

        # Monthly says 100 (less than daily, maybe stale)
        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=date(2026, 3, 1),
                request_count=100,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        # Should use daily value since it's larger
        assert lic_entry["usage"]["request_count"] == 200

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_monthly_usage_sums_multiple_rows_per_license(
        self, mock_fetch, db_session, test_settings
    ):
        """A license with multiple monthly-usage rows in one month (distinct
        subject_id / plan_id, e.g. per-user + S2S) must SUM to the total, not
        keep one arbitrary row. Reverting to dict-overwrite yields 100 or 5."""
        tenant = T("monthly-sum-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 10000},
            },
        ]

        # Small daily row so the license is present; monthly SUM (105) overrides.
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 5),
                request_count=10,
                denied_count=0,
            )
        )

        # Two monthly rows for the SAME license & month, distinct subject_id.
        # A per-user row (100) and an S2S row (5) — must sum to 105.
        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                subject_id=uuid.uuid4(),
                plan_id=plan_id,
                month_start=date(2026, 3, 1),
                request_count=100,
            )
        )
        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                subject_id=uuid.uuid4(),
                plan_id=plan_id,
                month_start=date(2026, 3, 1),
                request_count=5,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        # SUM of both monthly rows (100 + 5), not a single overwritten row.
        assert lic_entry["usage"]["request_count"] == 105
        assert result["totals"]["total_requests"] == 105

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_multiple_licenses_billing(self, mock_fetch, db_session, test_settings):
        """Billing with multiple licenses aggregates totals correctly."""
        tenant = T("multi-lic-tenant")
        lic1 = uuid.uuid4()
        lic2 = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic1),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 5000},
            },
            {
                "id": str(lic2),
                "app": {"key": "chatbot"},
                "plan": {"code": "basic", "quota_monthly": 1000},
            },
        ]

        db_session.add(AppCostRate(app_key="workers", cost_per_request_usd=Decimal("0.00200000")))
        db_session.add(AppCostRate(app_key="chatbot", cost_per_request_usd=Decimal("0.00050000")))

        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic1,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 10),
                request_count=200,
                denied_count=5,
            )
        )
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic2,
                app_id=app_id,
                plan_id=plan_id,
                service_key="chatbot",
                bucket_date=date(2026, 3, 10),
                request_count=300,
                denied_count=10,
            )
        )

        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic1,
                service_key="workers",
                provider="openai",
                bucket_date=date(2026, 3, 10),
                total_input_tokens=40000,
                total_output_tokens=20000,
                total_cost_usd=Decimal("0.300000"),
                request_count=200,
            )
        )
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic2,
                service_key="chatbot",
                provider="anthropic",
                bucket_date=date(2026, 3, 10),
                total_input_tokens=30000,
                total_output_tokens=15000,
                total_cost_usd=Decimal("0.500000"),
                request_count=300,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        assert len(result["licenses"]) == 2
        assert result["totals"]["total_requests"] == 500  # 200 + 300

        # Internal cost: (0.002*200 + 0.3) + (0.0005*300 + 0.5) = 0.7 + 0.65 = 1.35
        # But let's just verify both providers in totals
        assert "openai" in result["totals"]["cost_by_provider"]
        assert "anthropic" in result["totals"]["cost_by_provider"]

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_multi_day_aggregation(self, mock_fetch, db_session, test_settings):
        """Usage across multiple days in the same month should be summed."""
        tenant = T("multi-day-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "api"},
                "plan": {"code": "pro", "quota_monthly": 10000},
            },
        ]

        # 3 days of usage
        for day_num, count in [(1, 100), (10, 200), (20, 300)]:
            db_session.add(
                MeteringDaily(
                    tenant_id=tenant,
                    license_id=lic_id,
                    app_id=app_id,
                    plan_id=plan_id,
                    service_key="api",
                    bucket_date=date(2026, 3, day_num),
                    request_count=count,
                    denied_count=0,
                )
            )

        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        assert lic_entry["usage"]["request_count"] == 600  # 100+200+300

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_zero_markup_billing(self, mock_fetch, db_session, test_settings):
        """With 0% markup, customer_cost == internal_cost."""
        tenant = T("zero-markup-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        db_session.add(TenantConfig(tenant_id=tenant, markup_pct=Decimal("0.00")))

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "free", "quota_monthly": None},
            },
        ]

        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 1),
                request_count=10,
                denied_count=0,
            )
        )
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="workers",
                provider="openai",
                bucket_date=date(2026, 3, 1),
                total_input_tokens=1000,
                total_output_tokens=500,
                total_cost_usd=Decimal("0.100000"),
                request_count=10,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        assert result["markup_pct"] == 0.0
        # With 0% markup, customer == internal
        internal = Decimal(result["totals"]["internal_cost_usd"])
        customer = Decimal(result["totals"]["customer_cost_usd"])
        assert customer == internal.quantize(Decimal("0.01"))
        assert result["totals"]["margin_usd"] == "0.00"

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_custom_tenant_markup_billing(self, mock_fetch, db_session, test_settings):
        """Tenant-specific markup should be used in cost calculations."""
        tenant = T("custom-markup-tenant")
        lic_id = uuid.uuid4()

        db_session.add(TenantConfig(tenant_id=tenant, markup_pct=Decimal("50.00")))

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": None},
            },
        ]

        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="workers",
                provider="openai",
                bucket_date=date(2026, 3, 1),
                total_input_tokens=10000,
                total_output_tokens=5000,
                total_cost_usd=Decimal("1.000000"),
                request_count=10,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        assert result["markup_pct"] == 50.0
        # Internal cost = 1.0 (only LLM cost, no metering_daily = no app cost)
        # Customer cost = 1.0 * 1.50 = 1.50
        assert result["totals"]["customer_cost_usd"] == "1.50"
        assert result["totals"]["margin_usd"] == "0.50"

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_budget_fully_consumed(self, mock_fetch, db_session, test_settings):
        """Budget fully consumed: remaining should be 0, pct should be 100."""
        tenant = T("exhausted-budget-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "chatbot"},
                "plan": {"code": "basic", "quota_monthly": None},
            },
        ]

        db_session.add(
            TokenBudget(
                tenant_id=tenant,
                license_id=lic_id,
                budget_tokens=50000,
                period="monthly",
                active=True,
            )
        )
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="chatbot",
                bucket_date=date(2026, 3, 1),
                request_count=100,
                denied_count=0,
            )
        )
        db_session.add(
            TokenUsage(
                tenant_id=tenant,
                license_id=lic_id,
                period_type="monthly",
                period_start=date(2026, 3, 1),
                input_tokens=30000,
                output_tokens=20000,
                total_tokens=50000,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        assert lic_entry["tokens"]["budget_tokens"] == 50000
        assert lic_entry["tokens"]["budget_remaining"] == 0
        assert lic_entry["tokens"]["budget_pct"] == 100.0

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_budget_overconsumed(self, mock_fetch, db_session, test_settings):
        """Usage exceeds budget: remaining clamped to 0 via max(0, ...)."""
        tenant = T("over-budget-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "chatbot"},
                "plan": {"code": "basic", "quota_monthly": None},
            },
        ]

        db_session.add(
            TokenBudget(
                tenant_id=tenant,
                license_id=lic_id,
                budget_tokens=10000,
                period="monthly",
                active=True,
            )
        )
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="chatbot",
                bucket_date=date(2026, 3, 1),
                request_count=50,
                denied_count=0,
            )
        )
        db_session.add(
            TokenUsage(
                tenant_id=tenant,
                license_id=lic_id,
                period_type="monthly",
                period_start=date(2026, 3, 1),
                input_tokens=8000,
                output_tokens=5000,
                total_tokens=13000,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        # budget_remaining = max(0, 10000 - 13000) = 0
        assert lic_entry["tokens"]["budget_remaining"] == 0
        # budget_pct = 13000/10000 * 100 = 130.0
        assert lic_entry["tokens"]["budget_pct"] == 130.0

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_inactive_budget_excluded(self, mock_fetch, db_session, test_settings):
        """Inactive token budget should NOT appear in billing."""
        tenant = T("inactive-budget-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "chatbot"},
                "plan": {"code": "basic", "quota_monthly": None},
            },
        ]

        # Inactive budget
        db_session.add(
            TokenBudget(
                tenant_id=tenant,
                license_id=lic_id,
                budget_tokens=100_000,
                period="monthly",
                active=False,
            )
        )
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="chatbot",
                bucket_date=date(2026, 3, 1),
                request_count=10,
                denied_count=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        assert lic_entry["tokens"]["budget_tokens"] is None
        assert lic_entry["tokens"]["budget_remaining"] is None
        assert lic_entry["tokens"]["budget_pct"] is None

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_no_token_usage_with_budget(self, mock_fetch, db_session, test_settings):
        """Budget exists but no token usage => zero tokens, full budget remaining."""
        tenant = T("no-tokens-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "chatbot"},
                "plan": {"code": "basic", "quota_monthly": None},
            },
        ]

        db_session.add(
            TokenBudget(
                tenant_id=tenant,
                license_id=lic_id,
                budget_tokens=50000,
                period="monthly",
                active=True,
            )
        )
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="chatbot",
                bucket_date=date(2026, 3, 1),
                request_count=10,
                denied_count=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        lic_entry = result["licenses"][0]
        assert lic_entry["tokens"]["input_tokens"] == 0
        assert lic_entry["tokens"]["output_tokens"] == 0
        assert lic_entry["tokens"]["budget_tokens"] == 50000
        assert lic_entry["tokens"]["budget_remaining"] == 50000
        assert lic_entry["tokens"]["budget_pct"] == 0.0

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_data_outside_month_excluded(self, mock_fetch, db_session, test_settings):
        """Data from other months should not be included in the report."""
        tenant = T("date-filter-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 10000},
            },
        ]

        # Data in February (should be excluded from March report)
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 2, 28),
                request_count=999,
                denied_count=0,
            )
        )
        # Data in March (should be included)
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 15),
                request_count=42,
                denied_count=1,
            )
        )
        # Data in April (should be excluded)
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 4, 1),
                request_count=888,
                denied_count=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        assert len(result["licenses"]) == 1
        assert result["licenses"][0]["usage"]["request_count"] == 42
        assert result["totals"]["total_requests"] == 42

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_denied_count_tracked(self, mock_fetch, db_session, test_settings):
        """Denied requests should be tracked in billing license entry."""
        tenant = T("denied-count-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "api"},
                "plan": {"code": "free", "quota_monthly": 100},
            },
        ]

        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="api",
                bucket_date=date(2026, 3, 1),
                request_count=80,
                denied_count=20,
            )
        )
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="api",
                bucket_date=date(2026, 3, 2),
                request_count=50,
                denied_count=30,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        # Check quota_pct: 130 / 100 * 100 = 130.0%
        assert result["licenses"][0]["usage"]["quota_pct"] == 130.0

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_license_not_in_licenses_api_uses_service_key(
        self, mock_fetch, db_session, test_settings
    ):
        """License not returned by licenses API should fallback to service_key for app_key."""
        tenant = T("unknown-lic-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        # licenses API returns nothing for this license
        mock_fetch.return_value = []

        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="custom-service",
                bucket_date=date(2026, 3, 5),
                request_count=10,
                denied_count=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        assert len(result["licenses"]) == 1
        lic_entry = result["licenses"][0]
        # Should fallback to service_key since license not in API response
        assert lic_entry["app_key"] == "custom-service"
        assert lic_entry["plan_code"] is None
        assert lic_entry["usage"]["quota_monthly"] is None
        assert lic_entry["usage"]["quota_pct"] is None

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_january_billing(self, mock_fetch, db_session, test_settings):
        """January billing should work correctly (month=1, no off-by-one)."""
        mock_fetch.return_value = []

        result = await generate_billing(T("jan-tenant"), "2026-01", db_session, test_settings)

        assert result["month"] == "2026-01"
        assert result["licenses"] == []

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_multiple_services_same_license(self, mock_fetch, db_session, test_settings):
        """A license using multiple services should aggregate all service costs."""
        tenant = T("multi-svc-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "platform"},
                "plan": {"code": "enterprise", "quota_monthly": 100000},
            },
        ]

        db_session.add(AppCostRate(app_key="workers", cost_per_request_usd=Decimal("0.00100000")))
        db_session.add(AppCostRate(app_key="chatbot", cost_per_request_usd=Decimal("0.00200000")))

        # Same license, different service_keys
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 5),
                request_count=100,
                denied_count=0,
            )
        )
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="chatbot",
                bucket_date=date(2026, 3, 5),
                request_count=200,
                denied_count=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        assert len(result["licenses"]) == 1
        lic_entry = result["licenses"][0]
        # Total requests: 100 + 200 = 300
        assert lic_entry["usage"]["request_count"] == 300
        # App cost: (0.001*100) + (0.002*200) = 0.1 + 0.4 = 0.5
        assert lic_entry["costs"]["app_request_cost_usd"] == "0.50000000"

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_system_license_app_request_excluded(self, mock_fetch, db_session, test_settings):
        """S2S/system usage (NULL_LICENSE_ID) on a PAID service_key must not be billed.

        Regression: the LLM-cost loop skipped system rows via is_system_license,
        but the app-request/usage loop did not — so system traffic metered under
        the sentinel license, on a service_key carrying a nonzero AppCostRate, got
        a spurious per-request charge folded into internal/customer cost AND a
        bogus license entry keyed by the all-zeros sentinel. Only the real
        license's requests may be billed. Removing the new guard makes this fail.
        """
        tenant = T("s2s-app-request-tenant")
        lic_id = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 10000},
            },
        ]

        # Paid per-request app rate on the shared service_key.
        db_session.add(AppCostRate(app_key="workers", cost_per_request_usd=Decimal("0.00100000")))

        # Real-license traffic — billable (100 requests → $0.10 app cost).
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 5),
                request_count=100,
                denied_count=0,
            )
        )
        # System/S2S traffic on the SAME paid service_key — must be excluded.
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=NULL_LICENSE_ID,
                app_id=app_id,
                plan_id=plan_id,
                service_key="workers",
                bucket_date=date(2026, 3, 5),
                request_count=500,
                denied_count=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        # Only the real license appears — no sentinel-keyed entry.
        license_ids = {e["license_id"] for e in result["licenses"]}
        assert license_ids == {str(lic_id)}
        assert str(NULL_LICENSE_ID) not in license_ids

        entry = result["licenses"][0]
        # 100 real requests billed, not 600; $0.10 app cost, not $0.60.
        assert entry["usage"]["request_count"] == 100
        assert result["totals"]["total_requests"] == 100
        assert entry["costs"]["app_request_cost_usd"] == "0.10000000"
        assert Decimal(result["totals"]["internal_cost_usd"]) == Decimal("0.10000000")
        # 0.10 * (1 + default markup). Derived, not hardcoded — see above.
        expected = (
            Decimal("0.10000000") * (1 + Decimal(str(test_settings.default_markup_pct)) / 100)
        ).quantize(Decimal("0.01"))
        assert Decimal(result["totals"]["customer_cost_usd"]) == expected


# ---------------------------------------------------------------------------
# An unpriced service_key and a deliberately-free one are different facts
# ---------------------------------------------------------------------------


def _app_rate_missing_count(service_key: str, surface: str = "invoice") -> float:
    """Current value of the unpriced-app-rate counter for one service_key.

    ``surface`` defaults to ``invoice`` because that is the path
    ``generate_billing`` takes; the admin overview increments the same counter
    under ``surface="overview"`` and must not be confused with it.
    """
    return metrics.app_rate_missing_total.labels(
        service_key=service_key, surface=surface
    )._value.get()


async def test_missing_app_rate_is_reported_and_a_deliberate_zero_rate_is_not(
    db_session, test_settings
) -> None:
    """⭐ THE TWO CASES MUST BE DISTINGUISHABLE, and this asserts BOTH.

    ``services/billing.py`` resolves the per-request charge with
    ``app_rates.get(service_key, Decimal("0"))``. That collapses two facts with
    opposite meanings:

      * ``free-svc``     — an operator priced it at EXACTLY 0. Deliberate.
      * ``unpriced-svc`` — nobody ever gave it a row. An accident that invoices
        at $0, and whose fabricated $0 then makes ``app_incurred`` zero, which
        ``services/markup.py`` uses to gate its zero-margin alarm
        (``incurred_usd > 0``) — so the accident SUPPRESSES the alarm that would
        have named it.

    ⚠️ BOTH HALVES ARE LOad-BEARING. A test that only seeded the missing-row
    case would pass identically against an implementation that incremented
    UNCONDITIONALLY — which would bury the real signal under every deliberately
    free service in the estate. The ``free-svc`` assertion is what makes the
    counter mean "unpriced" rather than "cheap", and it is the half that fails
    if someone reaches for ``rate == 0`` instead of ``key not in app_rates``.

    Both service keys are billed in the SAME ``generate_billing`` run, so the
    two outcomes cannot be an artefact of differing setup.
    """
    tenant = T("billing-unpriced-app-rate")
    lic_id = uuid.uuid4()

    # Exactly one AppCostRate row, and its value is a genuine, deliberate zero.
    # ⛔ No row is added for "unpriced-svc" — that absence IS the subject.
    db_session.add(AppCostRate(app_key="free-svc", cost_per_request_usd=Decimal("0.00000000")))
    for service_key in ("free-svc", "unpriced-svc"):
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                service_key=service_key,
                bucket_date=date(2026, 3, 5),
                request_count=100,
                denied_count=0,
            )
        )
    await db_session.commit()

    before_unpriced = _app_rate_missing_count("unpriced-svc")
    before_free = _app_rate_missing_count("free-svc")

    with patch("services.billing.fetch_licenses", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "free-svc"},
                "plan": {"code": "pro", "quota_monthly": None},
            },
        ]
        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

    # The service_key with NO rate row is reported.
    assert _app_rate_missing_count("unpriced-svc") == before_unpriced + 1, (
        "a service_key with no billing_app_cost_rates row billed at $0 and said "
        "nothing — the accident is indistinguishable from a deliberate free tier"
    )
    # The service_key priced at 0 ON PURPOSE is NOT reported.
    assert _app_rate_missing_count("free-svc") == before_free, (
        "a deliberate 0 rate tripped the unpriced counter; the discriminator "
        "must be MEMBERSHIP in app_rates, not the rate's VALUE, or the signal "
        "drowns in every genuinely free service"
    )

    # ⛔ REPORT ONLY — no price moved. Neither key has a nonzero per-request
    # rate, so the app-request charge is still $0 for both.
    for entry in result["licenses"]:
        assert Decimal(entry["costs"]["app_request_cost_usd"]) == Decimal("0")


# ---------------------------------------------------------------------------
# A DISCLOSED QUANTITY IS SCOPED TO THE TENANT BEING BILLED
# ---------------------------------------------------------------------------
# `billing_metering_monthly_usage` has NO tenant_id column (model
# models/db/rollups.py:303, table alembic 0001 baseline), so the read in
# `generate_billing` CANNOT carry a tenant predicate and never could. Tenant
# isolation on that read rests entirely on the LICENCE: rows are SUMmed per
# license_id, the licence belongs to exactly one tenant, and only licence ids
# drawn from the tenant-filtered MeteringDaily/CostDaily queries are ever looked
# up. That is a real invariant and it is also an invisible one — nothing said it
# out loud and nothing would have noticed it breaking.
class TestMonthlyUsageIsScopedToTheBilledTenant:
    """The request count on a tenant's line counts that tenant's requests only."""

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_other_tenants_monthly_usage_is_not_disclosed(
        self, mock_fetch, db_session, test_settings
    ):
        """Two tenants, same month, both with monthly-usage rows.

        Tenant A's invoice line must disclose A's 150 requests — never A's 150
        plus B's 9,999, and never B's count alone. The frozen invoice line is a
        DISCLOSURE: a request count that silently includes another customer's
        traffic is a leak, not a rounding error, and it is unrecoverable once
        `close_period` has frozen it.
        """
        tenant_a = T("monthly-scope-tenant-a")
        tenant_b = T("monthly-scope-tenant-b")
        lic_a = uuid.uuid4()
        lic_b = uuid.uuid4()
        app_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        for tenant, lic, daily, monthly in (
            (tenant_a, lic_a, 50, 150),
            (tenant_b, lic_b, 1, 9999),
        ):
            # A daily row so the licence appears on its own tenant's invoice at
            # all (license_usage is keyed from the tenant-filtered reads).
            db_session.add(
                MeteringDaily(
                    tenant_id=tenant,
                    license_id=lic,
                    app_id=app_id,
                    plan_id=plan_id,
                    service_key="workers",
                    bucket_date=date(2026, 3, 5),
                    request_count=daily,
                    denied_count=0,
                )
            )
            # Monthly rows for BOTH tenants in the SAME month — the only thing
            # separating them in this table is the licence.
            db_session.add(
                MeteringMonthlyUsage(
                    license_id=lic,
                    plan_id=plan_id,
                    month_start=date(2026, 3, 1),
                    request_count=monthly,
                )
            )
        await db_session.commit()

        mock_fetch.return_value = [
            {
                "id": str(lic_a),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 100000},
            },
        ]
        result_a = await generate_billing(tenant_a, "2026-03", db_session, test_settings)

        # Exactly one licence, exactly A's own count.
        assert [e["license_id"] for e in result_a["licenses"]] == [str(lic_a)]
        assert result_a["licenses"][0]["usage"]["request_count"] == 150, (
            "tenant A's invoice line disclosed a request count that is not A's — "
            "the monthly-usage read is keyed by licence and must never attribute "
            "another tenant's licence row to this tenant"
        )
        assert result_a["totals"]["total_requests"] == 150
        # ⛔ The other tenant's figure must not appear anywhere in A's totals,
        # neither alone nor summed in (9999, or 150 + 9999 = 10149).
        assert result_a["totals"]["total_requests"] not in (9999, 10149)

        mock_fetch.return_value = [
            {
                "id": str(lic_b),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": 100000},
            },
        ]
        result_b = await generate_billing(tenant_b, "2026-03", db_session, test_settings)

        # POSITIVE CONTROL: B's own count really is in the table and really is
        # reachable — without this, an isolation assertion passes just as well
        # when the monthly read returns nothing at all.
        assert result_b["licenses"][0]["usage"]["request_count"] == 9999
        assert result_b["totals"]["total_requests"] == 9999


# ---------------------------------------------------------------------------
# THE MONEY TRIPLE BALANCES — total − internal == margin, AT THE CENT
# ---------------------------------------------------------------------------
class TestTotalsRoundingCoherence:
    """One rounding mode, quantized once, and the three columns balance.

    All three figures used to be produced differently: `internal_cost_usd` raw,
    `customer_cost_usd` and `margin_usd` each independently rounded with a BARE
    `.quantize(Decimal("0.01"))` (i.e. the context default ROUND_HALF_EVEN),
    while every per-line figure feeding them was rounded ROUND_HALF_UP in
    services/markup.py. Two modes in one statement, and an equation that did not
    hold.
    """

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_triple_balances_on_half_cent_inputs_both_directions(
        self, mock_fetch, db_session, test_settings
    ):
        """`customer − internal == margin` exactly, on exact-half-cent inputs.

        The cases are chosen so the unrounded value lands on a TRUE half cent in
        each direction under banker's rounding:
          * 0.005 → HALF_UP 0.01, HALF_EVEN 0.00 (rounds DOWN to even 0)
          * 0.025 → HALF_UP 0.03, HALF_EVEN 0.02 (rounds DOWN to even 2)
          * 0.0375 (0.025 at 50% markup) → HALF_UP 0.04, HALF_EVEN 0.04
        The third case exists so the invariant is also exercised with a NONZERO
        margin — a triple of 0.00/0.00/0.00 balances trivially and proves little.

        ⛔ Decimal, never float. `Decimal(0.025)` is not a half at all; the whole
        path is Decimal and these literals are strings for that reason.
        """
        cases = [
            # (label, incurred, markup_pct, internal, customer, margin)
            ("half-up-from-5", Decimal("0.005000"), Decimal("0"), "0.01", "0.01", "0.00"),
            ("half-up-from-25", Decimal("0.025000"), Decimal("0"), "0.03", "0.03", "0.00"),
            ("nonzero-margin", Decimal("0.025000"), Decimal("50"), "0.03", "0.04", "0.01"),
        ]

        for label, incurred, pct, exp_internal, exp_customer, exp_margin in cases:
            tenant = T(f"cent-coherence-{label}")
            lic_id = uuid.uuid4()
            db_session.add(TenantConfig(tenant_id=tenant, markup_pct=pct))
            db_session.add(
                CostDaily(
                    tenant_id=tenant,
                    license_id=lic_id,
                    service_key="workers",
                    provider="openai",
                    bucket_date=date(2026, 3, 1),
                    total_input_tokens=10,
                    total_output_tokens=5,
                    total_cost_usd=incurred,
                    request_count=1,
                )
            )
            await db_session.commit()

            mock_fetch.return_value = [
                {
                    "id": str(lic_id),
                    "app": {"key": "workers"},
                    "plan": {"code": "pro", "quota_monthly": None},
                },
            ]
            report = await generate_billing(tenant, "2026-03", db_session, test_settings)
            totals = report["totals"]

            internal = Decimal(totals["internal_cost_usd"])
            customer = Decimal(totals["customer_cost_usd"])
            margin = Decimal(totals["margin_usd"])

            # THE INVARIANT. Stated as an equation, not as a tolerance: an
            # invoice whose three money columns do not balance cannot be
            # reconciled by the customer or by us.
            assert customer - internal == margin, (
                f"[{label}] totals do not balance: {customer} - {internal} = "
                f"{customer - internal}, but margin_usd says {margin}"
            )
            # Every column is AT the cent — including internal_cost_usd, which
            # used to be written raw and is what broke the equation.
            for name, value in (
                ("internal_cost_usd", totals["internal_cost_usd"]),
                ("customer_cost_usd", totals["customer_cost_usd"]),
                ("margin_usd", totals["margin_usd"]),
            ):
                assert Decimal(value).as_tuple().exponent == -2, (
                    f"[{label}] {name} is not quantized to cents: {value!r}"
                )
            # THE MODE. These literals are what ROUND_HALF_UP gives; the bare
            # quantize this replaced would report 0.00 / 0.02 / 0.02 instead.
            assert totals["internal_cost_usd"] == exp_internal, label
            assert totals["customer_cost_usd"] == exp_customer, label
            assert totals["margin_usd"] == exp_margin, label

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_sub_cent_line_rounds_to_zero_but_is_still_visible(
        self, mock_fetch, db_session, test_settings
    ):
        """A sub-cent line bills $0.00 — ON PURPOSE, and not silently.

        $0.004 of incurred cost cannot be charged in a cent-denominated invoice,
        so the cent columns say 0.00 and the triple still balances. What must NOT
        happen is the money disappearing from the statement entirely: the raw
        per-licence `llm_cost_usd` keeps the sub-cent figure, which is the only
        place a reader can see that the month was not genuinely free. If a future
        change quantizes the per-licence cost columns too, this test fails and
        that is the point — rounding money to nothing is a decision that has to
        be made deliberately, not inherited.
        """
        tenant = T("sub-cent-tenant")
        lic_id = uuid.uuid4()
        db_session.add(TenantConfig(tenant_id=tenant, markup_pct=Decimal("0")))
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="workers",
                provider="openai",
                bucket_date=date(2026, 3, 1),
                total_input_tokens=1,
                total_output_tokens=1,
                total_cost_usd=Decimal("0.004000"),
                request_count=1,
            )
        )
        await db_session.commit()

        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "workers"},
                "plan": {"code": "pro", "quota_monthly": None},
            },
        ]
        result = await generate_billing(tenant, "2026-03", db_session, test_settings)
        totals = result["totals"]

        # Charged nothing, and the triple still balances at zero.
        assert totals["internal_cost_usd"] == "0.00"
        assert totals["customer_cost_usd"] == "0.00"
        assert totals["margin_usd"] == "0.00"
        internal = Decimal(totals["internal_cost_usd"])
        customer = Decimal(totals["customer_cost_usd"])
        assert customer - internal == Decimal(totals["margin_usd"])

        # ...but the cost is still ON the statement, unrounded, where it can be
        # seen. An invoice that reports 0.00 everywhere and nowhere records the
        # $0.004 is indistinguishable from a month with no usage at all.
        entry = result["licenses"][0]
        assert Decimal(entry["costs"]["llm_cost_usd"]) == Decimal("0.004"), (
            "the sub-cent incurred cost vanished from the per-licence breakdown "
            "as well as from the totals — there is then nothing on the statement "
            "that says this month had usage"
        )


class TestSweepPathSendsAnAuthorizationHeader:
    """``fetch_licenses`` with NO injected provider must still authenticate.

    ⭐ THE PRODUCTION MEASUREMENT THAT FORCED THIS, and the reason a unit test has
    to pin it: `/internal/licenses` is under the licences service's
    PROTECTED_PREFIXES, and ``services.invoices.close_due_periods`` calls
    ``close_period`` with no ``token_provider`` at all. On 2026-09-13T07:50Z, the
    first tick the monthly invoice-close loop has EVER run, **28 of 35 August
    candidate tenants refused with `LicensesUnavailableError: HTTP 401`** — every
    one solely for want of this header.

    ⛔ AND THE OLD BEHAVIOUR WOULD HAVE PASSED EVERY EXISTING TEST IN THIS FILE,
    because they all either inject a provider or assert the FAILURE path. "Sends no
    header when none was injected" was not wrong against any assertion that
    existed; it was only wrong against production.
    """

    @staticmethod
    def _capture(settings):  # noqa: ANN001, ANN205
        """A stubbed client that records the headers of the one GET it serves."""
        seen: dict[str, object] = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        async def _get(url, **kwargs):  # noqa: ANN001, ANN003, ANN202
            seen["headers"] = dict(kwargs.get("headers") or {})
            return mock_response

        mock_client = AsyncMock()
        mock_client.get = _get
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        return seen, mock_client

    async def test_no_injected_provider_still_authenticates(self, test_settings, monkeypatch):
        """The sweep path's exact call shape: token_provider omitted entirely."""
        import services.billing as billing_mod

        billing_mod.clear_default_token_providers()

        class _Provider:
            async def get_token(self):  # noqa: ANN202
                return "sweep-token"

        monkeypatch.setattr(billing_mod, "_default_token_provider", lambda _s: _Provider())

        seen, mock_client = self._capture(test_settings)
        with patch("services.billing.httpx.AsyncClient", return_value=mock_client):
            await fetch_licenses(T("sweep-tenant"), test_settings)

        headers = seen["headers"]
        assert headers.get("Authorization") == "Bearer sweep-token", (
            "the sweep path sent no Authorization header — this is the HTTP 401 that "
            f"refused 28 of 35 tenants on the first real tick. headers={headers}"
        )
        # The tenant header is unchanged by the fallback.
        assert headers.get("X-Tenant-Id") == T("sweep-tenant")

    async def test_an_injected_provider_still_wins(self, test_settings, monkeypatch):
        """⛔ NEGATIVE CONTROL. Without this, a fallback that IGNORED the injected
        provider would pass the test above while silently re-authenticating every
        HTTP route as the default account."""
        import services.billing as billing_mod

        billing_mod.clear_default_token_providers()
        monkeypatch.setattr(
            billing_mod,
            "_default_token_provider",
            lambda _s: (_ for _ in ()).throw(AssertionError("default must not be consulted")),
        )

        class _Injected:
            async def get_token(self):  # noqa: ANN202
                return "injected-token"

        seen, mock_client = self._capture(test_settings)
        with patch("services.billing.httpx.AsyncClient", return_value=mock_client):
            await fetch_licenses(
                T("sweep-tenant"),
                test_settings,
                token_provider=cast(ServiceTokenProvider, _Injected()),
            )

        assert seen["headers"].get("Authorization") == "Bearer injected-token"

    async def test_an_unbuildable_provider_does_not_raise(self, test_settings, monkeypatch):
        """⚠️ The fallback must only ever ADD a header. A settings object with no
        Keycloak credentials (the suite's narrow fakes, and any misconfiguration)
        must leave the call working exactly as it did before — unauthenticated and
        reported by the status check, never as a surprise exception from here."""
        import services.billing as billing_mod

        billing_mod.clear_default_token_providers()
        monkeypatch.setattr(billing_mod, "_default_token_provider", lambda _s: None)

        seen, mock_client = self._capture(test_settings)
        with patch("services.billing.httpx.AsyncClient", return_value=mock_client):
            await fetch_licenses(T("sweep-tenant"), test_settings)

        assert "Authorization" not in seen["headers"]
        assert seen["headers"].get("X-Tenant-Id") == T("sweep-tenant")
