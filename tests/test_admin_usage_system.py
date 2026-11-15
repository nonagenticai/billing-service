"""Tests for /api/admin/usage/system + billed_cost_usd on /api/usage/me*.

Covers the super-admin system-wide rollup (incurred + billed grand totals,
per-tenant rows with per-tenant markup, per-provider incurred-only rows)
and the billed_cost_usd field surfaced on the user-facing /me endpoints.

Auth: tests run with DASHBOARD_AUTH_ENABLED=false (set in conftest), so
require_admin returns a stub admin payload. The 403 path is enforced by
auth.verify_admin_token when AUTH is enabled in production.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import NULL_LICENSE_ID, CostDaily, MeteringDaily, TenantAccessCharge, TenantConfig
from services.admin_aggregate import resolve_window, system_usage
from services.billing import apply_markup, generate_billing, get_markup_pct_map
from tests.conftest import T


def _today() -> datetime:
    return datetime.now(UTC)


async def _seed_tenant_markup(db_engine, tenant_id: str, markup_pct: float) -> None:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(TenantConfig(tenant_id=tenant_id, markup_pct=Decimal(str(markup_pct))))
        await session.commit()


async def _seed_monthly_allowance(
    db_engine, *, tenant_id: str, included_usd: Decimal, service_key: str | None = None
) -> None:
    """Grant a tenant-wide monthly included-usage allowance.

    ``service_key=None`` lands in ``consume_allowance``'s tenant-wide ("")
    bucket, which every cost line draws from regardless of its product, so the
    fixture does not have to mirror whatever service_key ``_seed_cost`` picks.
    """
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            TenantAccessCharge(
                tenant_id=tenant_id,
                service_key=service_key,
                amount_usd=Decimal("0"),
                cadence="monthly",
                included_cost_usd=included_usd,
                effective_from=datetime(2020, 1, 1, tzinfo=UTC),
            )
        )
        await session.commit()


async def _seed_cost(
    db_engine,
    *,
    tenant_id: str,
    provider: str,
    cost_usd: Decimal,
    input_tokens: int = 1000,
    output_tokens: int = 500,
    request_count: int = 3,
    tenant_name: str | None = None,
) -> None:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                tenant_name=tenant_name or "",
                license_id=uuid.uuid4(),
                subject_id=uuid.uuid4(),
                service_key="inference-router",  # pragma: allowlist secret
                provider=provider,
                bucket_date=_today().date(),
                total_input_tokens=input_tokens,
                total_output_tokens=output_tokens,
                total_cost_usd=cost_usd,
                request_count=request_count,
            )
        )
        await session.commit()


async def _seed_requests(
    db_engine,
    *,
    tenant_id: str,
    count: int,
    tenant_name: str | None = None,
) -> None:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            MeteringDaily(
                tenant_id=tenant_id,
                tenant_name=tenant_name or "",
                license_id=uuid.uuid4(),
                subject_id=uuid.uuid4(),
                app_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                service_key="inference-router",  # pragma: allowlist secret
                bucket_date=_today().date(),
                request_count=count,
                denied_count=0,
            )
        )
        await session.commit()


class TestApplyMarkup:
    """Unit tests for the apply_markup helper (services/billing.py)."""

    def test_thirty_percent_markup(self):
        assert apply_markup(Decimal("1.00"), 30.0) == Decimal("1.30")

    def test_zero_markup(self):
        assert apply_markup(Decimal("4.20"), 0.0) == Decimal("4.20")

    def test_fractional_markup(self):
        # 1.00 × 1.125 = 1.125 → quantized to 1.12 under ROUND_HALF_EVEN
        # (banker's rounding rounds the half to the nearest even digit).
        assert apply_markup(Decimal("1.00"), 12.5) == Decimal("1.12")
        # 1.00 × 1.135 = 1.135 → 1.14 (round half to even, 4 is even).
        assert apply_markup(Decimal("1.00"), 13.5) == Decimal("1.14")

    def test_quantizes_to_cents(self):
        # Avoids floating-point drift surfacing in the response.
        # 0.10 × 1.33333 = 0.133333 → 0.13.
        result = apply_markup(Decimal("0.10"), 33.333)
        assert result == Decimal("0.13")
        assert result.as_tuple().exponent == -2

    def test_zero_input(self):
        assert apply_markup(Decimal("0"), 30.0) == Decimal("0.00")


class TestGetMarkupPctMap:
    """Batch markup lookup — used by the admin aggregate path."""

    async def test_returns_per_tenant_markup(self, db_session, test_settings):
        db_session.add(TenantConfig(tenant_id=T("t-a"), markup_pct=Decimal("10.00")))
        db_session.add(TenantConfig(tenant_id=T("t-b"), markup_pct=Decimal("50.00")))
        await db_session.commit()
        result = await get_markup_pct_map([T("t-a"), T("t-b")], db_session, test_settings)
        assert result == {T("t-a"): 10.0, T("t-b"): 50.0}

    async def test_unconfigured_tenant_falls_back_to_default(self, db_session, test_settings):
        db_session.add(TenantConfig(tenant_id=T("t-a"), markup_pct=Decimal("10.00")))
        await db_session.commit()
        result = await get_markup_pct_map([T("t-a"), T("t-unset")], db_session, test_settings)
        assert result[T("t-a")] == 10.0
        assert result[T("t-unset")] == test_settings.default_markup_pct

    async def test_db_default_overrides_env_default(self, db_session, test_settings):
        # The reserved global-default row. Since alembic 0011 the column is a real
        # uuid and production reads tenancy.DEFAULT_CONFIG_TENANT_ID, which IS
        # uuid5(NS, "__default__") — so T("__default__") is byte-identical to what
        # the lookup uses. Derived here rather than imported on purpose: importing
        # the constant would make this agree with any future wrong edit to it.
        db_session.add(TenantConfig(tenant_id=T("__default__"), markup_pct=Decimal("22.00")))
        await db_session.commit()
        result = await get_markup_pct_map([T("t-x")], db_session, test_settings)
        assert result[T("t-x")] == 22.0

    async def test_empty_input_returns_empty_map(self, db_session, test_settings):
        assert await get_markup_pct_map([], db_session, test_settings) == {}


class TestResolveWindow:
    def test_today(self):
        start, end = resolve_window("today")
        assert start == end == datetime.now(UTC).date()

    def test_month(self):
        start, end = resolve_window("month")
        assert start.day == 1
        assert end == datetime.now(UTC).date()

    def test_nd(self):
        start, end = resolve_window("7d")
        assert end - start == timedelta(days=7)

    def test_invalid_window(self):
        with pytest.raises(ValueError):
            resolve_window("garbage")

    def test_zero_days(self):
        with pytest.raises(ValueError):
            resolve_window("0d")

    def test_too_many_days(self):
        with pytest.raises(ValueError):
            resolve_window("999d")


class TestAdminUsageSystem:
    """End-to-end tests for GET /api/admin/usage/system."""

    @pytest.mark.unit
    async def test_empty_window_returns_zeros(self, app_client):
        resp = app_client.get("/api/admin/usage/system?window=today")
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["grand_total"]["requests"] == 0
        assert Decimal(body["grand_total"]["incurred_cost_usd"]) == Decimal("0")
        assert Decimal(body["grand_total"]["billed_cost_usd"]) == Decimal("0.00")
        assert body["by_tenant"] == []
        assert body["by_provider"] == []

    @pytest.mark.unit
    async def test_per_tenant_markup_applied_correctly(self, db_engine, app_client):
        await _seed_tenant_markup(db_engine, T("tenant-a"), 30.0)
        await _seed_tenant_markup(db_engine, T("tenant-b"), 50.0)
        await _seed_cost(
            db_engine,
            tenant_id=T("tenant-a"),
            provider="claude",
            cost_usd=Decimal("1.00"),
            tenant_name="Tenant A",
        )
        await _seed_cost(
            db_engine,
            tenant_id=T("tenant-b"),
            provider="claude",
            cost_usd=Decimal("2.00"),
            tenant_name="Tenant B",
        )
        await _seed_requests(db_engine, tenant_id=T("tenant-a"), count=10)
        await _seed_requests(db_engine, tenant_id=T("tenant-b"), count=20)

        resp = app_client.get("/api/admin/usage/system?window=today")
        assert resp.status_code == 200
        body = resp.json()["data"]

        by_tenant = {t["tenant_id"]: t for t in body["by_tenant"]}
        assert Decimal(by_tenant[T("tenant-a")]["incurred_cost_usd"]) == Decimal("1.00")
        assert Decimal(by_tenant[T("tenant-a")]["billed_cost_usd"]) == Decimal("1.30")
        assert by_tenant[T("tenant-a")]["markup_pct"] == 30.0
        assert by_tenant[T("tenant-a")]["tenant_name"] == "Tenant A"

        assert Decimal(by_tenant[T("tenant-b")]["incurred_cost_usd"]) == Decimal("2.00")
        assert Decimal(by_tenant[T("tenant-b")]["billed_cost_usd"]) == Decimal("3.00")
        assert by_tenant[T("tenant-b")]["markup_pct"] == 50.0

        # Grand totals = sum across tenants with per-tenant markup applied.
        assert Decimal(body["grand_total"]["incurred_cost_usd"]) == Decimal("3.00")
        assert Decimal(body["grand_total"]["billed_cost_usd"]) == Decimal("4.30")
        assert body["grand_total"]["requests"] == 30

    @pytest.mark.unit
    async def test_by_provider_is_incurred_only(self, db_engine, app_client):
        await _seed_cost(
            db_engine,
            tenant_id=T("tx"),
            provider="claude",
            cost_usd=Decimal("3.00"),
        )
        await _seed_cost(
            db_engine,
            tenant_id=T("tx"),
            provider="deepseek",
            cost_usd=Decimal("0.10"),
        )

        resp = app_client.get("/api/admin/usage/system?window=today")
        assert resp.status_code == 200
        body = resp.json()["data"]

        by_provider = {p["provider"]: p for p in body["by_provider"]}
        assert Decimal(by_provider["claude"]["incurred_cost_usd"]) == Decimal("3.00")
        assert Decimal(by_provider["deepseek"]["incurred_cost_usd"]) == Decimal("0.10")
        # No billed column on per-provider rows — we don't bill providers.
        assert "billed_cost_usd" not in by_provider["claude"]

    @pytest.mark.unit
    async def test_local_provider_zero_cost_still_surfaced(self, db_engine, app_client):
        await _seed_cost(
            db_engine,
            tenant_id=T("ty"),
            provider="gemma",  # local vLLM, $0
            cost_usd=Decimal("0"),
            input_tokens=12345,
            output_tokens=6789,
        )
        await _seed_requests(db_engine, tenant_id=T("ty"), count=5)

        resp = app_client.get("/api/admin/usage/system?window=today")
        body = resp.json()["data"]
        by_tenant = {t["tenant_id"]: t for t in body["by_tenant"]}
        assert Decimal(by_tenant[T("ty")]["incurred_cost_usd"]) == Decimal("0")
        assert Decimal(by_tenant[T("ty")]["billed_cost_usd"]) == Decimal("0.00")
        # Token counts and requests still come through.
        assert by_tenant[T("ty")]["input_tokens"] == 12345
        assert by_tenant[T("ty")]["output_tokens"] == 6789
        assert by_tenant[T("ty")]["requests"] == 5

    @pytest.mark.unit
    async def test_invalid_window_returns_400(self, app_client):
        resp = app_client.get("/api/admin/usage/system?window=not-a-window")
        assert resp.status_code == 400


class TestSystemUsageExcludesSystemLicense:
    """The admin system aggregate must exclude the system/S2S sentinel license,
    matching the real per-tenant invoice so the two billing paths cannot drift."""

    @pytest.mark.unit
    async def test_billed_matches_invoice_and_excludes_system(self, db_session, test_settings):
        """system_usage billed for a tenant must equal generate_billing's customer
        cost (both drop NULL_LICENSE_ID cost before markup).

        Regression: admin_aggregate.cost_stmt had no license dimension and no
        license_attributed_clause, so the $5 system/S2S cost was grouped in and
        marked up — inflating the admin report above the actual invoice. Removing
        the clause groups the system row back in (incurred 7.00, billed 9.10),
        breaking every assertion below.
        """
        tenant = T("no-drift-tenant")
        lic_id = uuid.uuid4()
        month = _today().strftime("%Y-%m")
        day = _today().date()

        # Real-license LLM cost — billable.
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                bucket_date=day,
                total_input_tokens=1000,
                total_output_tokens=500,
                total_cost_usd=Decimal("2.000000"),
                request_count=2,
            )
        )
        # System/S2S LLM cost on the SAME (service_key, provider, model) group —
        # would merge into the real line's aggregate row without the guard.
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=NULL_LICENSE_ID,
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                bucket_date=day,
                total_input_tokens=4000,
                total_output_tokens=2000,
                total_cost_usd=Decimal("5.000000"),
                request_count=8,
            )
        )
        await db_session.commit()

        # No TenantConfig row → the settings default markup, standard_markup mode.
        # ⚠️ The rate is read from test_settings below rather than written as a
        # literal: it was 30% until George set it to 20% on 2026-09-10, and the
        # point of this test is the EXCLUSION of the system licence, not the rate.
        # fetch_licenses hits the unreachable test licenses_url and returns [],
        # so generate_billing drives purely off the seeded cost rows.
        invoice = await generate_billing(tenant, month, db_session, test_settings)
        system = await system_usage(db_session, test_settings, "today")

        su_tenant = {t.tenant_id: t for t in system.by_tenant}[tenant]

        # System's $5 is excluded on BOTH paths: incurred = $2, billed = $2 marked up.
        billed_expected = (
            Decimal("2.00") * (1 + Decimal(str(test_settings.default_markup_pct)) / 100)
        ).quantize(Decimal("0.01"))
        assert Decimal(invoice["totals"]["internal_cost_usd"]) == Decimal("2.00")
        assert Decimal(su_tenant.incurred_cost_usd) == Decimal("2.00")
        assert Decimal(invoice["totals"]["customer_cost_usd"]) == billed_expected
        assert Decimal(su_tenant.billed_cost_usd) == billed_expected

        # No drift: the admin aggregate's billed figure equals the invoice's.
        assert Decimal(su_tenant.billed_cost_usd) == Decimal(invoice["totals"]["customer_cost_usd"])


class TestBilledCostOnUsageMe:
    """billed_cost_usd field on /api/usage/me/series + /me/breakdown."""

    @pytest.mark.unit
    async def test_series_applies_active_tenant_markup(self, db_engine, app_client):
        me = uuid.uuid4()
        await _seed_tenant_markup(db_engine, T("mark-t"), 40.0)
        # Seed a cost row attributed to this subject under mark-t.
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            session.add(
                CostDaily(
                    tenant_id=T("mark-t"),
                    license_id=uuid.uuid4(),
                    subject_id=me,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="claude",
                    bucket_date=_today().date(),
                    total_input_tokens=100,
                    total_output_tokens=50,
                    total_cost_usd=Decimal("1.00"),
                    request_count=1,
                )
            )
            await session.commit()

        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("mark-t")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["markup_pct"] == 40.0
        assert len(body["buckets"]) == 1
        bucket = body["buckets"][0]
        assert Decimal(bucket["cost_usd"]) == Decimal("1.00")
        assert Decimal(bucket["billed_cost_usd"]) == Decimal("1.40")

    @pytest.mark.unit
    async def test_breakdown_applies_active_tenant_markup(self, db_engine, app_client):
        me = uuid.uuid4()
        await _seed_tenant_markup(db_engine, T("brk-t"), 25.0)
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            session.add(
                CostDaily(
                    tenant_id=T("brk-t"),
                    license_id=uuid.uuid4(),
                    subject_id=me,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="claude",
                    bucket_date=_today().date(),
                    total_input_tokens=200,
                    total_output_tokens=100,
                    total_cost_usd=Decimal("2.00"),
                    request_count=2,
                )
            )
            await session.commit()

        resp = app_client.get(
            "/api/usage/me/breakdown?window=30d",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("brk-t")},
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["markup_pct"] == 25.0
        services = {s["service"]: s for s in body["services"]}
        assert Decimal(services["inference-router"]["cost_usd"]) == Decimal("2.00")
        assert Decimal(services["inference-router"]["billed_cost_usd"]) == Decimal("2.50")

    @pytest.mark.unit
    async def test_series_without_tenant_header_is_refused(self, db_engine, app_client):
        """No X-Tenant-Id → 400, and NOT an un-scoped 200.

        ⚠️ THIS TEST USED TO ASSERT THE DEFECT AS THE SPEC. It was
        ``test_series_without_tenant_header_falls_back`` and it demanded
        ``markup_pct is None`` with ``billed_cost_usd == cost_usd`` for a
        tenant-less ``/me/series`` read — i.e. it certified that the route
        answered 200 with rows while no tenant predicate was applied at all. The
        "fallback" it named was not a pricing nicety: the same missing header was
        the reason the row filter was missing, so this assertion stood guard over
        a cross-tenant read and would have outvoted the fix.

        There is no tenant-less ``/me`` read any more, so the un-marked-up
        rendering path is unreachable here by construction. What must be asserted
        instead is that the refusal carries NO DATA: an un-scoped caller must not
        be answered with rows, and must not be answered with an empty 200 either
        — that would be indistinguishable from a caller who genuinely has none.
        """
        me = uuid.uuid4()
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            session.add(
                CostDaily(
                    tenant_id=T("no-header"),
                    license_id=uuid.uuid4(),
                    subject_id=me,
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="claude",
                    bucket_date=_today().date(),
                    total_input_tokens=100,
                    total_output_tokens=50,
                    total_cost_usd=Decimal("0.50"),
                    request_count=1,
                )
            )
            await session.commit()

        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me)},
        )
        assert resp.status_code == 400
        # The refusal must not smuggle the rows out in the error body. Asserted
        # on the SHAPE, not on the seeded figure: the error envelope carries a
        # request-id uuid and an ISO timestamp, so a substring search for
        # "0.50" would red whenever the seconds field happened to read
        # ``:30.50…`` — a canary that fires on the clock proves nothing.
        assert "data" not in resp.json()
        assert "buckets" not in resp.text
        assert "cost_usd" not in resp.text

        # Positive control: the SAME request with the tenant header succeeds and
        # returns exactly that row — so the 400 above is the missing header, not
        # a seeding failure that would make any assertion here vacuous.
        ok = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(me), "X-Tenant-Id": T("no-header")},
        )
        assert ok.status_code == 200
        assert Decimal(ok.json()["data"]["buckets"][0]["cost_usd"]) == Decimal("0.50")

    @pytest.mark.unit
    async def test_series_empty_tenant_header_is_refused(self, app_client):
        """Present-but-empty X-Tenant-Id is a caller error, not "no header".

        ``license-authz`` sends ``tenant_id = result.tenant_id or ""`` when
        introspection returns no tenant, so this is the shape that actually
        arrives in production when a token carries no tenant claim.
        """
        resp = app_client.get(
            "/api/usage/me/series?window=30d&granularity=daily",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": "  "},
        )
        assert resp.status_code == 400


class TestBudgetStatusMe:
    """GET /api/budget/status/me — uses active X-Tenant-Id header."""

    @pytest.mark.unit
    async def test_returns_aggregate_for_active_tenant(self, db_engine, app_client):
        from models.db import TokenBudget

        tenant_id = T("bm-t1")
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            session.add(
                TokenBudget(
                    tenant_id=tenant_id,
                    license_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                    budget_tokens=100_000,
                    period="monthly",
                    includes_input=True,
                    includes_output=True,
                    active=True,
                    priority=0,
                    consumed_tokens=12_345,
                    source="manual",
                )
            )
            await session.commit()

        resp = app_client.get(
            "/api/budget/status/me",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": tenant_id},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()["data"]
        assert body["tenant_id"] == tenant_id
        assert body["budget_count"] >= 1

    @pytest.mark.unit
    async def test_missing_tenant_header_returns_400(self, app_client):
        resp = app_client.get(
            "/api/budget/status/me",
            headers={"X-Subject-Id": str(uuid.uuid4())},
        )
        assert resp.status_code == 400

    @pytest.mark.unit
    async def test_missing_subject_header_returns_401(self, app_client):
        resp = app_client.get(
            "/api/budget/status/me",
            headers={"X-Tenant-Id": T("any-tenant")},
        )
        assert resp.status_code == 401


class TestMarkupPctBaseAgreesWithBilled:
    """`markup_pct` must divide `billed` by the base markup was APPLIED to.

    ``system_usage`` nets a tenant's included-usage allowance out of incurred
    cost before calling ``compute_billed_line`` (month-anchored windows only),
    so ``billed`` is a function of the NETTED cost. Dividing it by the RAW
    incurred cost mixed two bases and published a markup that was never
    charged -- negative, on every tenant holding an allowance.

    Multi-valued on purpose: three (incurred, allowance, markup) triples with
    three different true rates and three different netted bases, so no single
    hardcoded constant can satisfy the assertions. The third row carries NO
    allowance and is the control -- raw and netted bases coincide there, so it
    stays green under the pre-fix code and proves the other two failures come
    from the allowance, not from the harness.
    """

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("slug", "incurred", "allowance", "markup", "exp_eff", "exp_billed", "exp_pct"),
        [
            ("mk-a", "10.00", "4.00", 20.0, "6.00", "7.20", 20.0),
            ("mk-b", "20.00", "15.00", 50.0, "5.00", "7.50", 50.0),
            ("mk-c", "8.00", None, 30.0, "8.00", "10.40", 30.0),
        ],
    )
    async def test_markup_pct_is_the_rate_on_the_netted_base(
        self,
        db_engine,
        app_client,
        slug,
        incurred,
        allowance,
        markup,
        exp_eff,
        exp_billed,
        exp_pct,
    ):
        tid = T(slug)
        await _seed_tenant_markup(db_engine, tid, markup)
        await _seed_cost(db_engine, tenant_id=tid, provider="claude", cost_usd=Decimal(incurred))
        await _seed_requests(db_engine, tenant_id=tid, count=5)
        if allowance is not None:
            await _seed_monthly_allowance(db_engine, tenant_id=tid, included_usd=Decimal(allowance))

        # 'month' — allowance netting only applies to a month-anchored window.
        resp = app_client.get("/api/admin/usage/system?window=month")
        assert resp.status_code == 200
        row = {t["tenant_id"]: t for t in resp.json()["data"]["by_tenant"]}[tid]

        # Raw cost is untouched: the allowance is a credit, not a cost revision.
        assert Decimal(row["incurred_cost_usd"]) == Decimal(incurred)
        # The netted base is published so the ratio can be audited on the row.
        assert Decimal(row["effective_incurred_cost_usd"]) == Decimal(exp_eff)
        assert Decimal(row["billed_cost_usd"]) == Decimal(exp_billed)
        # THE GUARD: the displayed rate is the rate that was charged, and it is
        # never negative merely because the tenant was granted an allowance.
        assert row["markup_pct"] == exp_pct
        assert row["markup_pct"] >= 0

        # And the row reconciles end to end.
        eff = Decimal(row["effective_incurred_cost_usd"])
        billed = Decimal(row["billed_cost_usd"])
        assert eff * (Decimal(1) + Decimal(str(row["markup_pct"])) / 100) == billed
