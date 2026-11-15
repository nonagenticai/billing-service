"""Tests for admin dashboard endpoints — server-rendered HTML pages."""

import re
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import (
    NULL_LICENSE_ID,
    AppCostRate,
    CostDaily,
    MeteringDaily,
    ProviderCost,
    TokenBudget,
)
from services.markup import quantize_money
from services.tenant_names import reset_cache as reset_tenant_name_cache
from tests.conftest import T


@pytest.fixture(autouse=True)
def _reset_tenant_name_cache():
    """Drop the process-wide tenant-name TTL cache around every dashboard test.

    ``services.tenant_names`` caches its answer — INCLUDING the empty map it
    falls back to — for 5 minutes, which outlives any test. Without this, the
    first costs-page test in the file poisons every later one: a test that mocks
    a SUCCESSFUL licences response would be served the earlier test's cached
    ``{}`` and assert against uuid labels while believing it proved resolution.
    """
    reset_tenant_name_cache()
    yield
    reset_tenant_name_cache()


# --- Helpers ---


async def _seed_daily_usage(db_engine, tenant_id, service_key="workers", count=100):
    """Insert metering_daily rows for testing dashboard endpoints."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    lic_id = uuid.uuid4()
    app_id = uuid.uuid4()
    plan_id = uuid.uuid4()
    today = datetime.now(UTC).date()
    async with factory() as session:
        session.add(
            MeteringDaily(
                tenant_id=tenant_id,
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                service_key=service_key,
                bucket_date=today,
                request_count=count,
                denied_count=5,
            )
        )
        await session.commit()
    return lic_id, app_id, plan_id


async def _seed_cost_daily(db_engine, tenant_id, provider="claude", cost_usd=1.50):
    """Insert cost_daily rows for testing dashboard endpoints."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    lic_id = uuid.uuid4()
    today = datetime.now(UTC).date()
    async with factory() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                license_id=lic_id,
                service_key="inference-router",
                provider=provider,
                bucket_date=today,
                total_input_tokens=50000,
                total_output_tokens=10000,
                total_cost_usd=cost_usd,
                request_count=25,
            )
        )
        await session.commit()
    return lic_id


async def _seed_cost_row(
    db_engine,
    tenant_id,
    *,
    subject_id=None,
    tenant_name="",
    provider="claude",
    input_tokens=0,
    output_tokens=0,
    cost_usd="1.00",
    request_count=1,
):
    """Insert ONE billing_cost_daily row with explicit token/subject values.

    ``_seed_cost_daily`` above fixes the token counts and leaves ``subject_id``
    on its sentinel, so it cannot express the per-tenant/per-subject ledger the
    costs page now renders.
    """
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    today = datetime.now(UTC).date()
    async with factory() as session:
        row = CostDaily(
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            license_id=uuid.uuid4(),
            service_key="inference-router",
            provider=provider,
            bucket_date=today,
            total_input_tokens=input_tokens,
            total_output_tokens=output_tokens,
            total_cost_usd=Decimal(cost_usd),
            request_count=request_count,
        )
        if subject_id is not None:
            row.subject_id = subject_id
        session.add(row)
        await session.commit()


def _mock_licenses_client(*, tenants=None, exc=None, status_code=200):
    """Build an httpx.AsyncClient stand-in for GET /internal/tenants."""
    mock_client = AsyncMock()
    if exc is not None:
        mock_client.get = AsyncMock(side_effect=exc)
    else:
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = tenants or []
        mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    return mock_client


async def _seed_provider_cost(db_engine, provider="claude"):
    """Insert provider_costs row for costs page."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            ProviderCost(
                provider=provider,
                input_per_1m_usd=Decimal("3.0"),
                output_per_1m_usd=Decimal("15.0"),
            )
        )
        await session.commit()


async def _seed_app_cost_rate(db_engine, app_key="workers"):
    """Insert app_cost_rates row for costs page."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            AppCostRate(
                app_key=app_key,
                cost_per_request_usd=Decimal("0.001"),
            )
        )
        await session.commit()


async def _seed_budget(db_engine, tenant_id, budget_tokens=1000000):
    """Insert active token_budget row for budgets page."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        budget = TokenBudget(
            tenant_id=tenant_id,
            license_id=NULL_LICENSE_ID,
            budget_tokens=budget_tokens,
            period="monthly",
            includes_input=True,
            includes_output=True,
            active=True,
        )
        session.add(budget)
        await session.commit()
        return str(budget.id)


class TestDashboardOverview:
    @pytest.mark.unit
    def test_overview_returns_html(self, app_client):
        """GET /dashboard/ should return 200 with HTML content type."""
        resp = app_client.get("/dashboard/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    @pytest.mark.unit
    def test_overview_contains_page_title(self, app_client):
        """Overview page should contain the Platform Overview heading."""
        resp = app_client.get("/dashboard/")
        assert resp.status_code == 200
        assert "Platform Overview" in resp.text

    @pytest.mark.unit
    def test_overview_with_month_param(self, app_client):
        """GET /dashboard/?month=2026-01 should filter by that month."""
        resp = app_client.get("/dashboard/?month=2026-01")
        assert resp.status_code == 200
        assert "2026-01" in resp.text

    @pytest.mark.unit
    def test_overview_invalid_month_rejected(self, app_client):
        """Invalid month format should be rejected by regex."""
        resp = app_client.get("/dashboard/?month=bad-format")
        assert resp.status_code == 422

    @pytest.mark.unit
    def test_overview_empty_data_shows_message(self, app_client):
        """Overview should show 'No billing data' when there is no data."""
        resp = app_client.get("/dashboard/")
        assert resp.status_code == 200
        assert "No billing data" in resp.text

    @pytest.mark.unit
    async def test_overview_with_usage_data(self, db_engine, app_client):
        """Overview with seeded data should show the tenant in the table."""
        await _seed_daily_usage(db_engine, tenant_id=T("dash-overview-t1"), count=500)
        month = datetime.now(UTC).strftime("%Y-%m")
        resp = app_client.get(f"/dashboard/?month={month}")
        assert resp.status_code == 200
        assert T("dash-overview-t1") in resp.text
        assert "500" in resp.text

    @pytest.mark.unit
    async def test_overview_with_cost_data_shows_financial_info(self, db_engine, app_client):
        """Overview with cost data should display cost and margin info."""
        await _seed_cost_daily(db_engine, tenant_id=T("dash-cost-t1"), cost_usd=10.0)
        month = datetime.now(UTC).strftime("%Y-%m")
        resp = app_client.get(f"/dashboard/?month={month}")
        assert resp.status_code == 200
        assert T("dash-cost-t1") in resp.text
        # Internal cost should appear
        assert "$" in resp.text
        # The margin section should be present
        assert "Margin" in resp.text

    @pytest.mark.unit
    async def test_overview_multiple_tenants(self, db_engine, app_client):
        """Overview should list multiple tenants when data exists for several."""
        await _seed_daily_usage(db_engine, tenant_id=T("dash-mt-a"), count=100)
        await _seed_daily_usage(db_engine, tenant_id=T("dash-mt-b"), count=200)
        month = datetime.now(UTC).strftime("%Y-%m")
        resp = app_client.get(f"/dashboard/?month={month}")
        assert resp.status_code == 200
        assert T("dash-mt-a") in resp.text
        assert T("dash-mt-b") in resp.text

    @pytest.mark.unit
    def test_overview_contains_navigation(self, app_client):
        """Overview should contain links for key summary cards."""
        resp = app_client.get("/dashboard/")
        assert resp.status_code == 200
        # Should have the summary cards
        assert "Total Requests" in resp.text
        assert "Internal Cost" in resp.text
        assert "Customer Revenue" in resp.text


class TestDashboardTenantDetail:
    @pytest.mark.unit
    def test_tenant_detail_returns_html(self, app_client):
        """GET /dashboard/tenant/{id} should return 200 even with no data."""
        resp = app_client.get(f"/dashboard/tenant/{T('test-tenant')}")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    @pytest.mark.unit
    def test_tenant_detail_contains_tenant_id(self, app_client):
        """Tenant detail page should display the tenant_id."""
        resp = app_client.get(f"/dashboard/tenant/{T('test-tenant')}")
        assert resp.status_code == 200
        # tenant.html renders the raw tenant_id ({{ tenant_id }} in <h1>/<title>),
        # never a display name — so the uuid, not the label, is what reaches the body.
        assert T("test-tenant") in resp.text

    @pytest.mark.unit
    def test_tenant_detail_shows_billing_cards(self, app_client):
        """Tenant detail page should show billing summary cards."""
        resp = app_client.get(f"/dashboard/tenant/{T('detail-t1')}")
        assert resp.status_code == 200
        assert "Requests" in resp.text
        assert "Internal Cost" in resp.text
        assert "Customer Cost" in resp.text

    @pytest.mark.unit
    async def test_tenant_detail_with_data(self, db_engine, app_client):
        """Tenant detail with seeded data should show request counts."""
        lic_id, _, _ = await _seed_daily_usage(db_engine, tenant_id=T("dash-detail-t1"), count=750)
        await _seed_cost_daily(db_engine, tenant_id=T("dash-detail-t1"))
        month = datetime.now(UTC).strftime("%Y-%m")
        resp = app_client.get(f"/dashboard/tenant/{T('dash-detail-t1')}?month={month}")
        assert resp.status_code == 200
        assert T("dash-detail-t1") in resp.text
        assert "750" in resp.text

    @pytest.mark.unit
    def test_tenant_detail_with_month_param(self, app_client):
        """Tenant detail accepts month query parameter."""
        resp = app_client.get(f"/dashboard/tenant/{T('t1')}?month=2025-06")
        assert resp.status_code == 200
        assert "2025-06" in resp.text

    @pytest.mark.unit
    def test_tenant_detail_invalid_month(self, app_client):
        """Invalid month format should be rejected."""
        resp = app_client.get(f"/dashboard/tenant/{T('t1')}?month=invalid")
        assert resp.status_code == 422

    @pytest.mark.unit
    def test_tenant_detail_has_back_link(self, app_client):
        """Tenant detail page should have a back link to overview."""
        resp = app_client.get(f"/dashboard/tenant/{T('t1')}")
        assert resp.status_code == 200
        assert "Back" in resp.text


class TestDashboardCostsPage:
    @pytest.mark.unit
    def test_costs_page_returns_html(self, app_client):
        """GET /dashboard/costs should return 200 with HTML content."""
        resp = app_client.get("/dashboard/costs")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    @pytest.mark.unit
    def test_costs_page_contains_title(self, app_client):
        """Costs page should contain the Cost Configuration heading."""
        resp = app_client.get("/dashboard/costs")
        assert "Cost Configuration" in resp.text

    @pytest.mark.unit
    def test_costs_page_shows_provider_section(self, app_client):
        """Costs page should have LLM Provider Rates section."""
        resp = app_client.get("/dashboard/costs")
        assert "LLM Provider Rates" in resp.text

    @pytest.mark.unit
    def test_costs_page_shows_app_rates_section(self, app_client):
        """Costs page should have App Cost Rates section."""
        resp = app_client.get("/dashboard/costs")
        assert "App Cost Rates" in resp.text

    @pytest.mark.unit
    def test_costs_page_empty_shows_no_providers(self, app_client):
        """With no provider data, costs page should show placeholder text."""
        resp = app_client.get("/dashboard/costs")
        assert resp.status_code == 200
        assert "No provider rates configured" in resp.text

    @pytest.mark.unit
    async def test_costs_page_with_provider_data(self, db_engine, app_client):
        """Costs page with seeded provider should display the provider name."""
        await _seed_provider_cost(db_engine, provider="claude-test")
        resp = app_client.get("/dashboard/costs")
        assert resp.status_code == 200
        assert "claude-test" in resp.text

    @pytest.mark.unit
    async def test_costs_page_with_app_rate_data(self, db_engine, app_client):
        """Costs page with seeded app rate should display the app key."""
        await _seed_app_cost_rate(db_engine, app_key="kanban-dash")
        resp = app_client.get("/dashboard/costs")
        assert resp.status_code == 200
        assert "kanban-dash" in resp.text

    @pytest.mark.unit
    async def test_costs_page_with_cost_summary(self, db_engine, app_client):
        """Costs page shows recent cost summary when cost data exists."""
        await _seed_cost_daily(db_engine, tenant_id=T("dash-cost-sum"), cost_usd=5.0)
        resp = app_client.get("/dashboard/costs")
        assert resp.status_code == 200
        # Page should render without error even with cost data


class TestDashboardCostsTenantLedger:
    """The per-tenant / per-subject token ledger on /dashboard/costs."""

    @pytest.mark.unit
    async def test_per_tenant_rows_show_summed_tokens(self, db_engine, app_client):
        """Two rows for one tenant must render as ONE row carrying their sum.

        The two tenants carry deliberately different totals so the assertion
        cannot be satisfied by the page's GLOBAL summary card: the global input
        total is 107,000, which is neither 100,000 nor 7,000.
        """
        await _seed_cost_row(
            db_engine, T("ledger-a"), provider="claude", input_tokens=60000, output_tokens=12000
        )
        await _seed_cost_row(
            db_engine, T("ledger-a"), provider="openai", input_tokens=40000, output_tokens=8000
        )
        await _seed_cost_row(
            db_engine, T("ledger-b"), provider="claude", input_tokens=7000, output_tokens=3000
        )

        resp = app_client.get("/dashboard/costs")
        assert resp.status_code == 200
        assert "Tokens by Tenant" in resp.text
        assert T("ledger-a") in resp.text
        assert T("ledger-b") in resp.text
        # ledger-a's two rows summed, not either row alone.
        assert "100,000" in resp.text
        assert "20,000" in resp.text
        assert "7,000" in resp.text

    @pytest.mark.unit
    async def test_per_subject_rows_appear(self, db_engine, app_client):
        """Each subject under a tenant gets its own row in the subject table."""
        subject_a = uuid.uuid4()
        subject_b = uuid.uuid4()
        await _seed_cost_row(
            db_engine,
            T("ledger-subj"),
            subject_id=subject_a,
            provider="claude",
            input_tokens=11000,
            output_tokens=1000,
        )
        await _seed_cost_row(
            db_engine,
            T("ledger-subj"),
            subject_id=subject_b,
            provider="claude",
            input_tokens=22000,
            output_tokens=2000,
        )

        resp = app_client.get("/dashboard/costs")
        assert resp.status_code == 200
        assert "Tokens by Subject" in resp.text
        assert str(subject_a) in resp.text
        assert str(subject_b) in resp.text
        # The tenant table collapses both subjects into one row of 33,000 input
        # and reports a subject count of 2.
        assert "33,000" in resp.text

    @pytest.mark.unit
    async def test_page_renders_when_licenses_lookup_fails(self, db_engine, app_client):
        """NEGATIVE CONTROL: /internal/tenants is not deployed yet.

        Every transport failure, timeout and non-200 must degrade to the uuid
        label. The page must still be a 200 — a name lookup for a display column
        may never take the ledger down with it.
        """
        await _seed_cost_row(db_engine, T("ledger-nolic"), input_tokens=5000, output_tokens=500)

        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(exc=httpx.ConnectError("no route")),
        ):
            resp = app_client.get("/dashboard/costs")

        assert resp.status_code == 200
        assert "Tokens by Tenant" in resp.text
        # Fallback (c): the uuid itself.
        assert T("ledger-nolic") in resp.text

    @pytest.mark.unit
    async def test_page_renders_when_licenses_returns_non_200(self, db_engine, app_client):
        """A 404 (the state until the endpoint ships) is not an exception."""
        await _seed_cost_row(db_engine, T("ledger-404"), input_tokens=4000, output_tokens=400)

        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(status_code=404),
        ):
            resp = app_client.get("/dashboard/costs")

        assert resp.status_code == 200
        assert T("ledger-404") in resp.text

    @pytest.mark.unit
    async def test_uuid_stored_name_is_replaced_by_resolved_name(self, db_engine, app_client):
        """Stored tenant_name == the uuid is NOT a name; licences wins.

        Emitters that only know the uuid write it into ``tenant_name``, so
        fallback (b) must reject it and fall through to the licences answer.
        """
        tenant_id = T("ledger-named")
        await _seed_cost_row(
            db_engine,
            tenant_id,
            tenant_name=tenant_id,
            input_tokens=9000,
            output_tokens=900,
        )

        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(
                tenants=[
                    {
                        "id": tenant_id,
                        "name": "ahtapol-ledger",
                        "keycloak_realm": "portal-realm",
                        "archived": False,
                    }
                ]
            ),
        ):
            resp = app_client.get("/dashboard/costs")

        assert resp.status_code == 200
        assert "ahtapol-ledger" in resp.text
        # The uuid stays visible as the secondary line under the name.
        assert tenant_id in resp.text

    @pytest.mark.unit
    async def test_stored_name_used_when_licenses_has_no_entry(self, db_engine, app_client):
        """Fallback (b): a real stored tenant_name beats the bare uuid."""
        await _seed_cost_row(
            db_engine,
            T("ledger-stored"),
            tenant_name="stored-display-name",
            input_tokens=3000,
            output_tokens=300,
        )

        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(tenants=[]),
        ):
            resp = app_client.get("/dashboard/costs")

        assert resp.status_code == 200
        assert "stored-display-name" in resp.text


class TestTenantCostCentre:
    """`kind` / `cost_centre` from licences PR #177 reach the cost report.

    The operator wants cost split by cost centre WITHOUT a hand-maintained
    crosswalk in `tenancy.py`. Two facts constrain what can be asserted here:

    * `cost_centre` is NULL fleet-wide (licenses migration 027 leaves it unset on
      purpose), so `kind` is the field that actually carries information today;
    * `/internal/tenants` answers billing with **401** in prod right now, so the
      unresolved path is the live path, not a hypothetical.

    Both are pinned below.
    """

    @pytest.mark.unit
    async def test_fetch_parses_kind_and_cost_centre(self):
        """The parse loop carries all three fields, not just `name`."""
        from services.tenant_names import get_tenant_info
        from settings import Settings

        tid = T("cc-parse")
        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(
                tenants=[
                    {
                        "id": tid,
                        "name": "n8n-dev",
                        "keycloak_realm": "portal-realm",
                        "archived": False,
                        "kind": "internal",
                        "cost_centre": "improvement",
                    }
                ]
            ),
        ):
            info = await get_tenant_info(
                Settings(licenses_url="https://licenses.example.com"), None
            )

        assert info[tid.lower()].name == "n8n-dev"
        assert info[tid.lower()].kind == "internal"
        assert info[tid.lower()].cost_centre == "improvement"

    @pytest.mark.unit
    async def test_get_tenant_names_still_returns_plain_names(self):
        """The name-only accessor is unchanged for callers that want a label."""
        from services.tenant_names import get_tenant_names
        from settings import Settings

        tid = T("cc-nameonly")
        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(
                tenants=[{"id": tid, "name": "ahtapol", "kind": "customer", "cost_centre": None}]
            ),
        ):
            names = await get_tenant_names(
                Settings(licenses_url="https://licenses.example.com"), None
            )

        assert names == {tid.lower(): "ahtapol"}

    @pytest.mark.unit
    def test_resolve_cost_centre_precedence(self):
        """cost_centre wins; kind is the fallback; unknown fails closed."""
        from services.tenant_names import TenantInfo, resolve_cost_centre

        tenants = {
            "a": TenantInfo(name="a", kind="internal", cost_centre="improvement"),
            "b": TenantInfo(name="b", kind="customer", cost_centre=None),
            "c": TenantInfo(name="c"),
        }
        assert resolve_cost_centre("a", tenants) == "improvement"
        assert resolve_cost_centre("b", tenants) == "customer"
        assert resolve_cost_centre("c", tenants) == "unclassified"
        # A tenant licences never reported is NOT promoted to any centre.
        assert resolve_cost_centre("missing", tenants) == "unclassified"

    @pytest.mark.unit
    async def test_costs_page_groups_by_cost_centre(self, db_engine, app_client):
        """Two tenants of different kinds land in two named buckets.

        This is the mutation-verified assertion: revert the consume change and
        the page has no cost-centre table at all.
        """
        customer = T("cc-customer")
        internal = T("cc-internal")
        await _seed_cost_row(db_engine, customer, input_tokens=1000, cost_usd="7.25")
        await _seed_cost_row(db_engine, internal, input_tokens=2000, cost_usd="91.50")

        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(
                tenants=[
                    {"id": customer, "name": "ahtapol", "kind": "customer", "cost_centre": None},
                    {"id": internal, "name": "n8n-dev", "kind": "internal", "cost_centre": None},
                ]
            ),
        ):
            resp = app_client.get("/dashboard/costs")

        assert resp.status_code == 200
        assert "Cost by Cost Centre" in resp.text
        assert "customer" in resp.text
        assert "internal" in resp.text
        # Grouped totals, not the per-tenant figures repeated.
        assert "91.50" in resp.text
        assert "7.25" in resp.text

    @pytest.mark.unit
    async def test_cost_centre_column_prefers_cost_centre_over_kind(self, db_engine, app_client):
        """When licences DOES set a cost_centre, it beats `kind` on the page."""
        tid = T("cc-explicit")
        await _seed_cost_row(db_engine, tid, input_tokens=500, cost_usd="3.00")

        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(
                tenants=[{"id": tid, "name": "ci-loop", "kind": "test", "cost_centre": "ci"}]
            ),
        ):
            resp = app_client.get("/dashboard/costs")

        assert resp.status_code == 200
        assert "ci-loop" in resp.text
        # Asserted as a whole CELL, not a substring: bare `"ci" in resp.text`
        # matches incidental text anywhere on the page and passes even with the
        # change reverted, which makes it a test that cannot fail.
        assert "<td>ci</td>" in resp.text
        # `kind` LOST to the explicit cost_centre — "test" is not the bucket.
        assert "<td>test</td>" not in resp.text

    @pytest.mark.unit
    async def test_401_degrades_to_unclassified_and_still_200(self, db_engine, app_client):
        """NEGATIVE CONTROL — this is the LIVE prod shape today.

        `/internal/tenants` 401s to billing. Every tenant must fall to the
        fail-closed bucket and the page must still render. A missing `kind` may
        never be an incident.
        """
        await _seed_cost_row(db_engine, T("cc-401"), input_tokens=800, cost_usd="2.00")

        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(status_code=401),
        ):
            resp = app_client.get("/dashboard/costs")

        assert resp.status_code == 200
        assert "unclassified" in resp.text
        # And the uuid label fallback is untouched by the richer payload.
        assert T("cc-401") in resp.text

    @pytest.mark.unit
    async def test_row_missing_kind_keeps_its_name(self, db_engine, app_client):
        """A payload with no `kind` must not cost the row its LABEL.

        Dropping the row on a missing classification field would be a regression
        against the name-only behaviour that shipped first.
        """
        tid = T("cc-nokind")
        await _seed_cost_row(db_engine, tid, input_tokens=600, cost_usd="1.00")

        with patch(
            "services.tenant_names.httpx.AsyncClient",
            return_value=_mock_licenses_client(tenants=[{"id": tid, "name": "legacy-shape"}]),
        ):
            resp = app_client.get("/dashboard/costs")

        assert resp.status_code == 200
        assert "legacy-shape" in resp.text
        assert "unclassified" in resp.text


class TestDashboardBudgetsPage:
    @pytest.mark.unit
    def test_budgets_page_returns_html(self, app_client):
        """GET /dashboard/budgets should return 200 with HTML content."""
        resp = app_client.get("/dashboard/budgets")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    @pytest.mark.unit
    def test_budgets_page_contains_title(self, app_client):
        """Budgets page should contain the Token Budgets heading."""
        resp = app_client.get("/dashboard/budgets")
        assert "Token Budgets" in resp.text

    @pytest.mark.unit
    def test_budgets_page_has_create_form(self, app_client):
        """Budgets page should contain a form to create new budgets."""
        resp = app_client.get("/dashboard/budgets")
        assert "Create Budget" in resp.text
        assert "Tenant ID" in resp.text
        assert "Budget Tokens" in resp.text

    @pytest.mark.unit
    def test_budgets_page_has_sync_button(self, app_client):
        """Budgets page should have sync from licenses button."""
        resp = app_client.get("/dashboard/budgets")
        assert "Sync from Licenses" in resp.text

    @pytest.mark.unit
    async def test_budgets_page_with_budget_data(self, db_engine, app_client):
        """Budgets page with seeded budget should display the tenant's budget info."""
        await _seed_budget(db_engine, tenant_id=T("dash-bud-t1"), budget_tokens=500000)
        resp = app_client.get("/dashboard/budgets")
        assert resp.status_code == 200
        assert T("dash-bud-t1") in resp.text

    @pytest.mark.unit
    def test_budgets_page_empty(self, app_client):
        """Budgets page should render correctly with no budget data."""
        resp = app_client.get("/dashboard/budgets")
        assert resp.status_code == 200
        # Page renders without error


class TestDashboardStaticFiles:
    @pytest.mark.unit
    def test_static_css_served(self, app_client):
        """Static CSS file should be accessible and served correctly."""
        resp = app_client.get("/static/style.css")
        assert resp.status_code == 200
        assert "text/css" in resp.headers.get("content-type", "")
        assert len(resp.text) > 0

    @pytest.mark.unit
    def test_static_nonexistent_404(self, app_client):
        """Request to nonexistent static file should return 404."""
        resp = app_client.get("/static/nonexistent.css")
        assert resp.status_code == 404


class TestDashboardAuthDisabled:
    """Tests that dashboard is accessible when DASHBOARD_AUTH_ENABLED=false."""

    @pytest.mark.unit
    def test_overview_no_auth_needed(self, app_client):
        """Dashboard should be accessible without auth header when auth is disabled."""
        resp = app_client.get("/dashboard/")
        assert resp.status_code == 200

    @pytest.mark.unit
    def test_costs_no_auth_needed(self, app_client):
        """Costs page should be accessible without auth when disabled."""
        resp = app_client.get("/dashboard/costs")
        assert resp.status_code == 200

    @pytest.mark.unit
    def test_budgets_no_auth_needed(self, app_client):
        """Budgets page should be accessible without auth when disabled."""
        resp = app_client.get("/dashboard/budgets")
        assert resp.status_code == 200

    @pytest.mark.unit
    def test_tenant_detail_no_auth_needed(self, app_client):
        """Tenant detail should be accessible without auth when disabled."""
        resp = app_client.get(f"/dashboard/tenant/{T('any-tenant')}")
        assert resp.status_code == 200


class TestOverviewMarginWithNoInternalCost:
    """An unbounded margin must not render as 0%.

    ``internal_cost_usd == 0`` with ``customer_cost_usd > 0`` is reachable in
    production: a BYO tenant pays us but incurs no provider cost on our
    account, and so does a tenant billed only a per-request surcharge. The
    margin percentage there has no denominator -- it is not zero, it is
    unbounded -- and the overview page printed "0%", i.e. "this tenant earns us
    nothing" about the most profitable row on the page.

    ``dashboard_overview`` now returns ``None`` for that cell, the same idiom
    and for the same reason as ``AttributionAudit.mismatch_ratio``. The
    assertions below deliberately cover BOTH layers: the value AND its
    rendering. A ``None`` that a template coerces back to ``0`` is a fix that
    was undone one layer up.

    Multi-valued: three different customer costs (including a genuinely zero
    one, whose margin is equally undefined) plus a control row that DOES have a
    denominator, so no hardcoded output can satisfy the set.
    """

    def _overview(self, rows):
        from models.domain import BillingOverview, BillingOverviewEntry

        entries = [
            BillingOverviewEntry(
                tenant_id=tid,
                total_requests=req,
                internal_cost_usd=Decimal(internal),
                # Whole-cent fixtures only in this class, so the pre-image and
                # the displayed cent are the same number — but the producer
                # ALWAYS populates the field, and leaving it at its
                # `Decimal("0")` default here would model an entry
                # `compute_billing_overview` never emits.
                internal_cost_exact_usd=Decimal(internal),
                customer_cost_usd=Decimal(customer),
            )
            for tid, req, internal, customer in rows
        ]
        return BillingOverview(
            month="2026-09",
            tenants=entries,
            platform_total_internal_usd=sum((e.internal_cost_usd for e in entries), Decimal("0")),
            platform_total_customer_usd=sum((e.customer_cost_usd for e in entries), Decimal("0")),
        )

    @pytest.mark.unit
    @pytest.mark.parametrize("customer", ["5.00", "12.50", "0.00"])
    async def test_zero_internal_cost_renders_not_applicable_not_zero(self, app_client, customer):
        tid = T("dash-margin-inf")
        overview = self._overview([(tid, 42, "0.00", customer)])
        with patch(
            "routers.billing.compute_billing_overview",
            AsyncMock(return_value=overview),
        ):
            resp = app_client.get("/dashboard/?month=2026-09")

        assert resp.status_code == 200
        # THE GUARD: the undefined margin is rendered distinctly, and the page
        # never claims a measured 0%.
        assert "n/a" in resp.text
        assert "0%" not in resp.text
        # And `None` reached the template rather than being stringified there.
        assert "None%" not in resp.text

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("internal", "customer", "expected"),
        [("10.00", "13.00", "30.0%"), ("4.00", "5.00", "25.0%")],
    )
    async def test_nonzero_internal_cost_still_reports_a_percentage(
        self, app_client, internal, customer, expected
    ):
        """Control: a real denominator still yields a real percentage."""
        tid = T("dash-margin-real")
        overview = self._overview([(tid, 7, internal, customer)])
        with patch(
            "routers.billing.compute_billing_overview",
            AsyncMock(return_value=overview),
        ):
            resp = app_client.get("/dashboard/?month=2026-09")

        assert resp.status_code == 200
        assert expected in resp.text
        assert "n/a" not in resp.text


# --- Overview arithmetic: quantize the leaves, then sum ---------------------- #


def _overview_payload(rows, *, upstream_folded=True):
    """Same shape as ``TestOverviewMarginWithNoInternalCost._overview``.

    Deliberately module-level rather than inherited: subclassing that class to
    borrow the helper would make pytest re-collect all of its parametrised
    cases under a second name, so the duplication is the cheaper of the two.

    ⭐ THE DEFAULT MODELS THE REAL PRODUCER, AND THE TWO INTERNAL COLUMNS
    THEREFORE DIFFER. ``compute_billing_overview`` returns the QUANTIZED cent in
    ``internal_cost_usd``/``customer_cost_usd`` (``internal_charged`` /
    ``customer_charged``, routers/billing.py) and the RAW pre-image in
    ``internal_cost_exact_usd``. This helper used to set ``internal_cost_usd``
    and ``internal_cost_exact_usd`` to the SAME raw string, which no producer
    ever emits — and with the two columns equal, every consumer of the exact
    column was graded by a fixture that could not tell them apart:
    ``internal_exact = Decimal(t.internal_cost_exact_usd)`` (routers/dashboard.py)
    could be rewritten to read ``internal_cost_usd`` and the whole file stayed
    green, i.e. the sub-cent marker's INPUT was untested while its output was
    asserted five ways.
    ⛔ The cent is derived here with ``quantize_money`` — the producer's own
    helper — never hardcoded beside the raw value: a pair of unrelated literals
    would decouple from the service the moment ``MONEY_ROUNDING`` changed, and
    the fixture would then assert a rounding the code does not perform.

    ⚠️ ``upstream_folded=False`` hands the page an UNFOLDED payload: both display
    columns raw. That is NOT the producer's output and is never the default —
    it exists because the renderer quantizes both columns AGAIN
    (``quantize_money(t.internal_cost_usd)`` / ``(t.customer_cost_usd)``), and
    those two calls are idempotent on a folded payload, so a folded fixture can
    never fail for their removal. Defence in depth disarms its own mutation
    control unless one fixture removes the outer guard. The class that uses it
    says so at each call site.

    ⚠️ The platform totals are the RAW sums of the fixture's INPUTS in both
    modes — deliberately NOT ``Σ`` of the entries above. The real producer
    accumulates the charged cents, so this is the one figure here that models a
    PRE-fix ``compute_billing_overview``: the page must not source its footer
    from these fields at all, and a fixture whose totals already equal the
    column's sum could not fail if it did.
    """
    from models.domain import BillingOverview, BillingOverviewEntry

    raw = [
        (tid, req, Decimal(internal), Decimal(customer)) for tid, req, internal, customer in rows
    ]
    entries = [
        BillingOverviewEntry(
            tenant_id=tid,
            total_requests=req,
            # The cent the row RENDERS, folded exactly as the producer folds it.
            internal_cost_usd=quantize_money(internal) if upstream_folded else internal,
            customer_cost_usd=quantize_money(customer) if upstream_folded else customer,
            # The RAW pre-image, which is what these fixtures' `internal` string
            # already is (`"20.005000"`) — distinct from the column above
            # whenever the input carries sub-cent scale, which is the property
            # that makes the sub-cent marker's wiring expressible at all.
            internal_cost_exact_usd=internal,
        )
        for tid, req, internal, customer in raw
    ]
    return BillingOverview(
        month="2026-09",
        tenants=entries,
        platform_total_internal_usd=sum((r[2] for r in raw), Decimal("0")),
        platform_total_customer_usd=sum((r[3] for r in raw), Decimal("0")),
    )


def _render(app_client, rows, *, upstream_folded=True):
    """Render /dashboard/ over a mocked overview payload."""
    with patch(
        "routers.billing.compute_billing_overview",
        AsyncMock(return_value=_overview_payload(rows, upstream_folded=upstream_folded)),
    ):
        resp = app_client.get("/dashboard/?month=2026-09")
    assert resp.status_code == 200
    return resp.text


#: Every money cell in a tenant row: internal, customer, margin, in that order.
#: The requests cell carries no ``$`` and so is excluded by construction.
_MONEY_CELL = re.compile(r'<td class="mono">\$([0-9.]+)</td>')
_MARGIN_CARD = re.compile(r'<div class="label">Margin</div>\s*<div class="value[^"]*">\$([0-9.]+)')


def _margin_column(text):
    """The margin cell of every rendered tenant row, as Decimals."""
    rows = re.findall(r"<tr>(.*?)</tr>", text, re.DOTALL)
    out = []
    for row in rows:
        cells = _MONEY_CELL.findall(row)
        if len(cells) == 3:  # internal, customer, margin
            out.append(Decimal(cells[2]))
    return out


def _margin_card(text):
    m = _MARGIN_CARD.search(text)
    assert m is not None, "Margin summary card not found in the rendered page"
    return Decimal(m.group(1))


class TestOverviewArithmeticIsQuantizedThenSummed:
    """The overview's three money columns must be ONE consistent arithmetic.

    ⭐ THE FIXTURES ARE THE POINT. Every case below feeds BOTH money columns at
    SUB-CENT scale (``20.005000`` / ``30.024000``) — the Numeric(20,6)/(20,8)
    scale ``billing_cost_daily.total_cost_usd`` actually carries. While every
    internal figure happened to be a whole cent, a raw column and a quantized
    column were the SAME NUMBER and none of these assertions could fail: the
    pre-existing tests in this file all use whole cents, which is exactly why
    they were blind to a page rendering one column in cents and the other raw.
    Varying the assertion alone would not have helped; the fixture is what
    makes the defect expressible.

    ⚠️ AND THE SUB-CENT SCALE HAD TO BE ON **BOTH** COLUMNS. Only the internal
    fixture carried it, so the renderer's ``quantize_money(t.customer_cost_usd)``
    could be deleted with all 307 tests still passing — the same defect as the
    internal column, surviving on the customer side because the fixture property
    was applied to one of the two.

    ⚠️ EVERY CASE HERE RENDERS AN **UNFOLDED** PAYLOAD (``upstream_folded=False``).
    The producer now quantizes both columns itself, which makes the renderer's own
    two quantize calls idempotent — so on a producer-faithful payload neither call
    can be made to fail, and the guard grades only the guard upstream of it. This
    class exists to hold the RENDERER's fold; the producer's own fold is held
    against a real database in
    ``tests/test_project_pricing_readers_agree.py`` (§ the internal-quantize and
    exact-pre-image sections). The sub-cent MARKER class below renders the
    faithful payload instead, because its subject is a field the producer emits
    differently from the column beside it.

    The rule, applied identically on the invoice (services/billing.py's
    ``internal_cost_q`` / ``margin_q``): quantize the leaves, then sum. A
    displayed component is the cent it is charged as, a derived column is
    derived from the two stored cents, and a displayed total is the sum of the
    displayed components.
    """

    @pytest.mark.unit
    async def test_margin_usd_is_the_difference_of_the_two_displayed_cents(self, app_client):
        """Sub-cent cost on BOTH columns: each margin cell equals what its row shows.

        FIXTURE: internal ``20.005000`` AND customer ``60.014000`` — both at
        sub-cent scale, which is the property under test. The page shows $20.01
        and $60.01, so the only margin an operator can reconcile is $40.00.
        Computing ``q(customer − raw)`` instead yields ``q(40.005) = $40.01`` —
        a cent that appears nowhere in the row it sits beside.

        ⚠️ ASSERTED ON THE MARGIN CELL, not on ``"$40.00" in text``. The footer
        card is ``q(Σ margin)``, so a renderer that dropped the CUSTOMER
        quantize still prints $40.00 in the card (``q(40.004) = 40.00``) while
        the cell reads $40.004000 — a substring assertion is satisfied by the
        card and the mutation survives. Measured: that is exactly how
        ``customer_charged = quantize_money(...) → t.customer_cost_usd`` stayed
        alive through 307 passing tests.
        """
        tid = T("dash-q-margin-usd")
        # upstream_folded=False: the renderer's own two quantize calls are the
        # thing under test here, and they are inert on a folded payload.
        text = _render(app_client, [(tid, 3, "20.005000", "60.014000")], upstream_folded=False)

        assert _margin_column(text) == [Decimal("40.00")], _margin_column(text)
        assert "$40.01" not in text
        # And BOTH money columns are themselves cents, not the raw Numeric scale.
        assert "$20.01" in text
        assert "$60.01" in text
        assert "20.005000" not in text
        assert "60.014" not in text

    @pytest.mark.unit
    async def test_margin_pct_is_the_ratio_of_the_two_displayed_cents(self, app_client):
        """Sub-cent internal cost: the percentage must match the printed row.

        FIXTURE: internal ``0.005000`` AND customer ``0.034000`` — both sub-cent
        scale. The row displays $0.01 internal against a $0.02 margin, i.e.
        200.0%. Taking the ratio over a RAW numerator AND a RAW denominator gives
        ``0.029 / 0.005 = 580.0%`` — a figure derived from two numbers the page
        never prints; dropping only the CUSTOMER fold gives ``0.024 / 0.01 =
        240.0%``, equally unreconcilable.
        """
        tid = T("dash-q-margin-pct")
        # upstream_folded=False — see the sibling test; the renderer's folds are
        # what is under test and a folded payload makes them inert.
        text = _render(app_client, [(tid, 5, "0.005000", "0.034000")], upstream_folded=False)

        assert "200.0%" in text
        # Both folds dropped / only the internal one / only the customer one —
        # each leaves a different wrong percentage, and none of the three is the
        # ratio of the two printed cells.
        assert "580.0%" not in text
        assert "500.0%" not in text
        assert "240.0%" not in text
        assert "$0.01" in text
        assert "0.005000" not in text
        assert "0.034" not in text
        # A denominator of one displayed cent is a real denominator.
        assert "n/a" not in text

    @pytest.mark.unit
    async def test_margin_card_equals_the_sum_of_the_margin_column(self, app_client):
        """Two sub-cent rows: the footer card must add up to its own column.

        FIXTURE: TWO tenants at internal ``20.005000`` / customer ``30.024000``
        — both sub-cent, the property under test. Each row's margin is $10.01,
        so the card must read $20.02. Deriving the card from the platform totals
        makes it ``q(Σcustomer − Σinternal) = q(20.038) = $20.04`` — `qΣ` under a
        column that is `Σq`, and the gap grows with tenant count.
        """
        rows = [
            (T("dash-q-card-a"), 7, "20.005000", "30.024000"),
            (T("dash-q-card-b"), 9, "20.005000", "30.024000"),
        ]
        # upstream_folded=False — see the first test in this class.
        text = _render(app_client, rows, upstream_folded=False)

        column = _margin_column(text)
        assert column == [Decimal("10.01"), Decimal("10.01")], column
        # THE IDENTITY, derived from the rendered page rather than asserted as a
        # constant: the footer IS the column.
        assert _margin_card(text) == sum(column)
        assert _margin_card(text) == Decimal("20.02")
        assert "$20.04" not in text

    @pytest.mark.unit
    async def test_the_footer_cards_are_money_on_a_month_with_no_tenants(self, app_client):
        """⭐ THE ONE CASE THE FOOTER QUANTIZES ARE NOT IDEMPOTENT ON.

        The three cards sum per-row figures that are ALREADY whole cents, so
        `quantize_money(...)` around each moves no money and every other fixture
        here leaves a mutation that drops those calls alive. That is not a reason
        to delete them and not a reason to assert an identity: with no rows the
        accumulators are a bare `Decimal("0")` at exponent 0, and the call is the
        only thing that makes the page print `$0.00` rather than `$0`.

        ⚠️ MUTATION TARGET: `"margin_total": margin_total` (drop the
        `quantize_money`) in `routers.dashboard.dashboard_overview`. THE FIXTURE
        PROPERTY is the empty month — asserted on the literal rendered string,
        because `Decimal("0") == Decimal("0.00")` and a numeric compare cannot
        see the difference.
        """
        text = _render(app_client, [])

        assert "No billing data" in text, (
            "this fixture must render the empty table — with a row present the "
            "accumulators already carry two places and the quantize is inert"
        )
        for label in ("Internal Cost", "Customer Revenue", "Margin"):
            card = re.search(
                rf'<div class="label">{label}</div>\s*<div class="value[^"]*">(\$[0-9.]+)',
                text,
            )
            assert card is not None, f"the {label} summary card is not on the page"
            assert card.group(1) == "$0.00", (
                f"the {label} card renders {card.group(1)} on an empty month: a "
                "bare zero rather than a money figure at two places"
            )

    @pytest.mark.unit
    @pytest.mark.parametrize("upstream_folded", [True, False])
    async def test_whole_cent_rows_are_untouched(self, app_client, upstream_folded):
        """NON-VACUITY CONTROL: nothing that was already right moves.

        Whole-cent fixtures only — the regime every other test in this file
        uses. Raw and quantized coincide there, so every figure below is
        identical before and after the fix. If this case changed, the change
        would not be a rounding fix, it would be a re-pricing.

        Run over BOTH payload shapes: on whole cents a folded and an unfolded
        producer are the same payload by construction, so the page must render
        byte-identically either way. That is also the control for the
        ``upstream_folded`` switch itself — if the two parametrisations ever
        disagreed here, the helper would be changing figures rather than only
        changing where the fold happened.
        """
        rows = [
            (T("dash-q-control-a"), 11, "10.00", "13.00"),
            (T("dash-q-control-b"), 13, "4.00", "5.00"),
        ]
        text = _render(app_client, rows, upstream_folded=upstream_folded)

        assert _margin_column(text) == [Decimal("3.00"), Decimal("1.00")]
        assert _margin_card(text) == Decimal("4.00")
        assert "30.0%" in text
        assert "25.0%" in text
        # The other two cards are unmoved as well.
        assert "$14.00" in text
        assert "$18.00" in text


class TestOverviewMarginMarkerDistinguishesZeroFromSubCent:
    """A missing margin percentage is TWO different facts; the cell must say which.

    ⭐ THE REGRESSION THIS PINS. Quantizing the internal column is what makes
    `customer − internal = margin` hold at the cent, and the percentage is
    deliberately taken against the DISPLAYED denominator — a ratio of two
    figures the page does not show is not reconcilable by a reader. But that
    maps every tenant-month under half a cent onto the same `$0.00` as a
    tenant-month with no spend at all, and both then rendered one "n/a" whose
    tooltip read "No internal cost this month". For a tenant with real sub-cent
    spend that sentence is FALSE, and the row lost the only trace of its cost on
    the page. Measured on prod 2026-09-15: of 60 tenant-months carrying any
    licence-attributed cost, 12 are positive and sub-cent, one of them in the
    current month — so this is a live population, not a hypothetical.

    THE FIX UNDER TEST is a third state, not a rescued percentage: no ratio is
    invented, and the marker carries the raw figure instead of erasing it.

    ⭐ EVERY CASE HERE RENDERS THE PRODUCER-FAITHFUL PAYLOAD (``_overview_payload``
    default), i.e. ``internal_cost_usd`` folded to the cent and
    ``internal_cost_exact_usd`` the raw pre-image — DIFFERENT NUMBERS. That
    distinction is the subject: while the helper set both columns to the same raw
    string, the renderer's
    ``internal_exact = Decimal(t.internal_cost_exact_usd)`` could be rewritten to
    read ``t.internal_cost_usd`` and all 307 tests still passed, because the two
    attributes were interchangeable in the fixture and only there. Mutated
    against this payload a `0.004000` tenant gets
    ``sub_cent_internal = (0 == 0 and 0 > 0) = False`` and the page renders the
    plain "No internal cost this month" — the exact falsehood this third state
    exists to remove — so the mutation now fails here.
    """

    @pytest.mark.unit
    async def test_sub_cent_internal_cost_gets_its_own_marker_and_keeps_its_figure(
        self, app_client
    ):
        """FIXTURE PROPERTY: internal cost positive but BELOW half a cent.

        `0.004000` quantizes to `$0.00`, so the denominator the page shows is
        zero and no honest percentage exists — while the cost itself is real.
        Seed the `0.005000` every other sub-cent fixture in this file uses and
        it rounds UP to `$0.01`, a real denominator, a real percentage, and this
        test cannot fail.
        """
        tid = T("dash-margin-subcent")
        text = _render(app_client, [(tid, 5, "0.004000", "5.00")])

        # The denominator really does display as zero — the precondition.
        assert "$0.00" in text
        # No fabricated ratio, in either direction.
        assert "%" not in _margin_cells_text(text)
        # ...and the cell says WHICH fact it is, with the figure still on the page.
        assert "n/a (cost &lt; $0.01)" in text
        # ⚠️ `$0.004`, not `$0.004000`. The pre-image arrives at Numeric(20, 6)
        # column scale and the tooltip used to print the storage padding; it is
        # TRIMMED of trailing zeros (`routers.dashboard._exact_money`), never
        # rounded — rounding it to the cent would print `$0.00`, the very
        # statement this marker exists to contradict.
        assert "$0.004" in text, (
            "the raw sub-cent cost is nowhere on the page: the row's only trace "
            "of its real spend has been erased, not merely un-ratioed"
        )
        assert "0.004000" not in text, (
            "the tooltip is still printing the Numeric(20, 6) storage scale"
        )
        # ⛔ AND IT IS NOT ROUNDED AWAY. `$0.00` appears on this page as the
        # internal-cost CELL, so the discriminating assertion is that the
        # tooltip's own figure is not the rounded one.
        assert "Internal cost this month is $0.00 " not in text, (
            "the tooltip rounded the pre-image to the cent: it now reads $0.00 "
            "while asserting the cost is real, which is self-contradictory"
        )
        assert "No internal cost this month" not in text, (
            "the tooltip asserts something untrue about a tenant that DID incur cost"
        )

    @pytest.mark.unit
    async def test_a_genuinely_zero_internal_cost_still_reads_plain_n_a(self, app_client):
        """THE CONTROL: the new marker must not swallow the case it was split from.

        FIXTURE PROPERTY: internal cost EXACTLY zero. Without this, rendering the
        sub-cent marker unconditionally — for every row with no percentage —
        would satisfy the test above while making the page lie in the other
        direction, about a tenant with genuinely no spend.
        """
        tid = T("dash-margin-zero")
        text = _render(app_client, [(tid, 5, "0", "5.00")])

        assert "No internal cost this month" in text
        assert "cost &lt; $0.01" not in text
        assert "%" not in _margin_cells_text(text)

    @pytest.mark.unit
    async def test_a_tooltip_figure_at_tiny_exponent_is_not_scientific_notation(self, app_client):
        """⭐ THE HAZARD ``_exact_money`` CHOOSES ``format(value, "f")`` FOR.

        FIXTURE PROPERTY, MEASURED not guessed: ``Decimal("0.00000007")`` has
        adjusted exponent −8, so ``str()`` and ``.normalize()`` BOTH render it
        ``7E-8`` while fixed-point renders ``0.00000007``. Every other sub-cent
        fixture in this file sits at exponent −6 or larger, where ``str()`` and
        ``format(…, "f")`` agree character for character — which is why
        ``format(value, "f") → str(value)`` was a surviving mutation across the
        whole suite even though the docstring names that exact risk.

        The scale is reachable: ``app_cost_rates.cost_per_request_usd`` and
        ``tenant_markup_rules.per_request_usd`` are ``Numeric(20, 8)``, and a
        Decimal product carries the SUM of its operands' scales, so a
        few-request tenant-month lands well below 1e-7.

        ⚠️ Asserted on the RENDERED page, not on the helper: ``$7E-8`` in a
        tooltip is the defect, and only the template shows whether the helper is
        the thing producing that string.
        """
        raw = Decimal("0.00000007")
        # The precondition — if this ever stops holding, the test below is
        # vacuous and says so here rather than passing quietly.
        assert str(raw) == "7E-8", str(raw)
        assert str(raw.normalize()) == "7E-8"
        assert format(raw, "f") == "0.00000007"
        assert quantize_money(raw) == 0  # the marker's branch is reachable

        tid = T("dash-margin-tiny-exponent")
        text = _render(app_client, [(tid, 2, "0.00000007", "5.00")])

        # The third display state is reached at all...
        assert "n/a (cost &lt; $0.01)" in text
        # ...and its figure is fixed-point, at full precision, with no exponent.
        assert "$0.00000007" in text
        assert "7E-8" not in text
        assert "E-8" not in text and "E+" not in text


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected", "hazard"),
    [
        # (value, fixed-point trimmed form, what str()/normalize() would emit)
        ("0.004000", "0.004", None),  # the ordinary case: storage padding only
        ("0.00000007", "0.00000007", "7E-8"),  # str() goes scientific
        ("10.000000", "10", "1E+1"),  # .normalize() goes scientific
        ("1E+1", "10", "1E+1"),  # already scientific on the way in
    ],
)
def test_exact_money_is_fixed_point_at_every_exponent(raw, expected, hazard):
    """``_exact_money`` trims zeros and NEVER emits an exponent.

    ⚠️ MUTATION TARGETS, both named in the helper's own docstring and neither
    previously tested: ``format(value, "f")`` → ``str(value)``, and →
    ``value.normalize()``. The ``hazard`` column is the string the mutant would
    print, asserted absent — and it is MEASURED per row (the assert below fails
    if a row's hazard is not real), so a row that stops discriminating is a
    failure rather than a silent pass.
    """
    from routers.dashboard import _exact_money

    value = Decimal(raw)
    if hazard is not None:
        assert hazard in (str(value), str(value.normalize())), (
            f"{raw} no longer renders as {hazard} under str()/normalize(): this "
            "row no longer discriminates the mutation it exists for"
        )
        assert hazard != expected

    assert _exact_money(value) == expected
    # The property, independent of the table: never an exponent, and
    # arithmetically unchanged.
    out = _exact_money(value)
    assert "E" not in out and "e" not in out
    assert Decimal(out) == value


def _margin_cells_text(text):
    """The rendered margin-% cell of every tenant row, concatenated."""
    rows = re.findall(r"<tr>(.*?)</tr>", text, re.DOTALL)
    return "".join(row.rsplit("<td", 1)[-1] for row in rows if _MONEY_CELL.findall(row))
