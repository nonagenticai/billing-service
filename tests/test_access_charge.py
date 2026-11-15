"""Access charges — the ACCESS-billing axis (entity, CRUD routes, billing fold-in).

- Service layer: effective-dated open-row upsert + cadence-aware month resolution.
- CRUD routes: /api/config/access-charges (upsert/list/history + auth gate).
- generate_billing: a monthly charge folds into customer_cost + margin (not
  internal_cost), a one_time charge bills only in its month, a paid-but-unused
  product still bills, and an internal/demo tenant bills $0.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import auth_deps
from models.db import (
    NULL_LICENSE_ID,
    NULL_SUBJECT_ID,
    AuditLog,
    CostDaily,
    MeteringDaily,
    TenantAccessCharge,
    TenantConfig,
)
from models.domain import AccessChargeCreate
from routers.billing import compute_billing_overview
from services import access_charge as ac_svc
from services.billing import generate_billing
from tests.conftest import TEST_DATABASE_URL, T, pgoptions_with

# A product/service_key literal, referenced by name so detect-secrets' Secret
# Keyword plugin doesn't flag `service_key="..."` kwargs (identifier ends in _key).
PRODUCT = "fr"
# A SECOND product, for the lineage-scoping control. Named for the same reason
# as PRODUCT above: passing the literal inline would trip detect-secrets.
OTHER_PRODUCT = "kag"


def _this_month() -> tuple[str, date, date, date, datetime]:
    today = datetime.now(UTC).date()
    month_start = date(today.year, today.month, 1)
    month_end = (
        date(today.year + 1, 1, 1) if today.month == 12 else date(today.year, today.month + 1, 1)
    )
    bill_ts = datetime(month_end.year, month_end.month, month_end.day, tzinfo=UTC) - timedelta(
        microseconds=1
    )
    return today.strftime("%Y-%m"), today, month_start, month_end, bill_ts


async def _seed_cost_daily(session, *, tenant, license_id, service_key, cost):
    _, day, *_ = _this_month()
    session.add(
        CostDaily(
            tenant_id=T(tenant),
            tenant_name=tenant,
            license_id=license_id,
            subject_id=NULL_SUBJECT_ID,
            service_key=service_key,
            provider="openai",
            served_model="gpt-5",
            byo=False,
            cost_source="inference-router",
            bucket_date=day,
            total_input_tokens=1000,
            total_output_tokens=200,
            total_cost_usd=Decimal(cost),
            request_count=1,
        )
    )


async def _seed_metering_daily(session, *, tenant, license_id, service_key):
    _, day, *_ = _this_month()
    session.add(
        MeteringDaily(
            tenant_id=T(tenant),
            tenant_name=tenant,
            license_id=license_id,
            service_key=service_key,
            bucket_date=day,
            request_count=1,
        )
    )


def _month_offset(n: int) -> tuple[str, date, datetime]:
    """(YYYY-MM, a mid-month day, a mid-month datetime) for `n` months before now.

    Used by the one_time cross-month netting tests to seed cost + effective_from
    in specific calendar months (the shared ``_this_month`` helper only knows now).
    """
    today = datetime.now(UTC).date()
    total = today.year * 12 + (today.month - 1) - n
    yy, mm = divmod(total, 12)
    mm += 1
    return f"{yy:04d}-{mm:02d}", date(yy, mm, 15), datetime(yy, mm, 15, tzinfo=UTC)


async def _seed_cost_on(session, *, tenant, license_id, service_key, cost, day):
    """Seed one cost_daily row on an explicit ``bucket_date`` (any month)."""
    session.add(
        CostDaily(
            tenant_id=T(tenant),
            tenant_name=tenant,
            license_id=license_id,
            subject_id=NULL_SUBJECT_ID,
            service_key=service_key,
            provider="openai",
            served_model="gpt-5",
            byo=False,
            cost_source="inference-router",
            bucket_date=day,
            total_input_tokens=1000,
            total_output_tokens=200,
            total_cost_usd=Decimal(cost),
            request_count=1,
        )
    )


# --------------------------------------------------------------------------
# Service layer
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestAccessChargeService:
    async def test_upsert_closes_prior_open_row(self, db_session: AsyncSession) -> None:
        for amt in ("10", "15"):
            await ac_svc.upsert_access_charge(
                db_session,
                AccessChargeCreate(tenant_id=T("t1"), service_key=PRODUCT, amount_usd=Decimal(amt)),
            )
        await db_session.commit()

        rows = list(
            (
                await db_session.execute(
                    select(TenantAccessCharge).where(TenantAccessCharge.tenant_id == T("t1"))
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 2
        open_rows = [r for r in rows if r.effective_until is None]
        assert len(open_rows) == 1 and open_rows[0].amount_usd == Decimal("15")

    async def test_tenant_wide_and_product_charge_coexist(self, db_session: AsyncSession) -> None:
        # NULL service_key (tenant-wide) and a product charge are distinct tuples.
        await ac_svc.upsert_access_charge(
            db_session, AccessChargeCreate(tenant_id=T("t1"), amount_usd=Decimal("5"))
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(tenant_id=T("t1"), service_key=PRODUCT, amount_usd=Decimal("9")),
        )
        await db_session.commit()
        open_rows = await ac_svc.list_current_access_charges(db_session)
        assert len(open_rows) == 2

    async def test_active_monthly_applies(self, db_session: AsyncSession) -> None:
        _, _, month_start, month_end, bill_ts = _this_month()
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("20"),
                cadence="monthly",
                effective_from=datetime(2020, 1, 1, tzinfo=UTC),
            ),
        )
        await db_session.commit()
        rows = await ac_svc.active_access_charges_for_month(
            db_session,
            tenant_id=T("t1"),
            month_start=month_start,
            month_end=month_end,
            bill_ts=bill_ts,
        )
        assert len(rows) == 1 and rows[0].amount_usd == Decimal("20")

    async def test_active_one_time_only_in_its_month(self, db_session: AsyncSession) -> None:
        _, _, month_start, month_end, bill_ts = _this_month()
        # one_time charge effective LAST month → not billable this month.
        last_month_day = month_start - timedelta(days=1)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("99"),
                cadence="one_time",
                effective_from=datetime(
                    last_month_day.year, last_month_day.month, last_month_day.day, tzinfo=UTC
                ),
            ),
        )
        await db_session.commit()
        rows = await ac_svc.active_access_charges_for_month(
            db_session,
            tenant_id=T("t1"),
            month_start=month_start,
            month_end=month_end,
            bill_ts=bill_ts,
        )
        assert rows == []


@pytest.mark.unit
class TestTenantAllowanceUsage:
    """The compute-on-read fold behind the fr "Your plan" tile.

    used / allowance / remaining per service_key, netting via the same ladder as
    ``generate_billing`` so the tile reconciles with the invoice.
    """

    async def test_used_allowance_remaining_per_product(self, db_session: AsyncSession) -> None:
        _, _, month_start, _, _ = _this_month()
        lic = uuid.uuid4()
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                cadence="monthly",
                included_cost_usd=Decimal("20"),
                effective_from=datetime(2020, 1, 1, tzinfo=UTC),
            ),
        )
        await _seed_cost_daily(
            db_session, tenant="t1", license_id=lic, service_key=PRODUCT, cost="5"
        )
        await db_session.commit()

        folded = await ac_svc.tenant_allowance_usage(
            db_session, tenant_id=T("t1"), month_start=month_start
        )
        assert folded[PRODUCT] == {
            "used_usd": Decimal("5"),
            "allowance_usd": Decimal("20"),
            "remaining_usd": Decimal("15"),
        }

    async def test_overage_draws_tenant_wide_bucket(self, db_session: AsyncSession) -> None:
        _, _, month_start, _, _ = _this_month()
        lic = uuid.uuid4()
        # Product allowance 3, tenant-wide allowance 10; product usage 5.
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                cadence="monthly",
                included_cost_usd=Decimal("3"),
                effective_from=datetime(2020, 1, 1, tzinfo=UTC),
            ),
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                amount_usd=Decimal("0"),
                cadence="monthly",
                included_cost_usd=Decimal("10"),
                effective_from=datetime(2020, 1, 1, tzinfo=UTC),
            ),
        )
        await _seed_cost_daily(
            db_session, tenant="t1", license_id=lic, service_key=PRODUCT, cost="5"
        )
        await db_session.commit()

        folded = await ac_svc.tenant_allowance_usage(
            db_session, tenant_id=T("t1"), month_start=month_start
        )
        # Product bucket fully drained (3); overflow (2) drawn from tenant-wide (10 → 8).
        assert folded[PRODUCT]["used_usd"] == Decimal("5")
        assert folded[PRODUCT]["remaining_usd"] == Decimal("0")
        assert folded[""]["allowance_usd"] == Decimal("10")
        assert folded[""]["remaining_usd"] == Decimal("8")

    async def test_no_usage_no_allowance_is_empty(self, db_session: AsyncSession) -> None:
        _, _, month_start, _, _ = _this_month()
        folded = await ac_svc.tenant_allowance_usage(
            db_session, tenant_id=T("nobody"), month_start=month_start
        )
        assert folded == {}


# --------------------------------------------------------------------------
# CRUD routes
# --------------------------------------------------------------------------


@pytest.fixture
def admin_actor(monkeypatch):
    fake_payload = {"sub": "test-admin-user", "realm_access": {"roles": ["license-admin"]}}

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


async def _fetch_audit(db_engine, action: str) -> list[AuditLog]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        return list(
            (await s.execute(select(AuditLog).where(AuditLog.action == action))).scalars().all()
        )


@pytest.mark.unit
class TestAccessChargeRoutes:
    async def test_upsert_creates_and_audits(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        resp = app_client.post(
            "/api/config/access-charges",
            headers={"Authorization": "Bearer fake"},
            json={
                "tenant_id": T("t1"),
                "service_key": PRODUCT,
                "amount_usd": "49",
                "cadence": "monthly",
            },
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()["data"]
        assert data["service_key"] == PRODUCT
        assert Decimal(data["amount_usd"]) == Decimal("49")
        assert data["cadence"] == "monthly"
        assert data["effective_until"] is None

        audit = await _fetch_audit(db_engine, "config.access_charge.upsert")
        assert len(audit) == 1
        assert audit[0].after is not None and audit[0].after["service_key"] == PRODUCT

    async def test_list_and_history(self, app_client: TestClient, db_engine, admin_actor) -> None:
        for amt in ("10", "12"):
            r = app_client.post(
                "/api/config/access-charges",
                headers={"Authorization": "Bearer fake"},
                json={"tenant_id": T("t1"), "service_key": PRODUCT, "amount_usd": amt},
            )
            assert r.status_code == 201, r.text

        listing = app_client.get(
            "/api/config/access-charges", headers={"Authorization": "Bearer fake"}
        )
        assert listing.status_code == 200
        assert len(listing.json()["data"]) == 1  # only the open row

        hist = app_client.get(
            "/api/config/access-charges/history",
            headers={"Authorization": "Bearer fake"},
            params={"tenant_id": T("t1"), "service_key": PRODUCT},
        )
        assert hist.status_code == 200
        assert len(hist.json()["data"]) == 2  # closed + open

    async def test_tenant_allowance_usage_route(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        # A monthly allowance on PRODUCT + a seeded cost row → used/allowance/remaining.
        r = app_client.post(
            "/api/config/access-charges",
            headers={"Authorization": "Bearer fake"},
            json={
                "tenant_id": T("t1"),
                "service_key": PRODUCT,
                "amount_usd": "0",
                "cadence": "monthly",
                "included_cost_usd": "20",
            },
        )
        assert r.status_code == 201, r.text
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as s:
            await _seed_cost_daily(
                s, tenant="t1", license_id=uuid.uuid4(), service_key=PRODUCT, cost="5"
            )
            await s.commit()

        resp = app_client.get(
            "/api/config/tenant-allowance-usage",
            headers={"Authorization": "Bearer fake"},
            params={"tenant_id": T("t1")},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()["data"]
        assert body["tenant_id"] == T("t1")
        entry = next(e for e in body["entries"] if e["service_key"] == PRODUCT)
        assert Decimal(entry["used_usd"]) == Decimal("5")
        assert Decimal(entry["allowance_usd"]) == Decimal("20")
        assert Decimal(entry["remaining_usd"]) == Decimal("15")

    async def test_non_admin_rejected(self, app_client: TestClient, db_engine, monkeypatch) -> None:
        import auth

        async def _fake_decode(_request):
            return {"sub": "tenant-user", "realm_access": {"roles": ["customer"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)

        resp = app_client.post(
            "/api/config/access-charges",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "amount_usd": "10"},
        )
        assert resp.status_code == 403


# --------------------------------------------------------------------------
# generate_billing fold-in
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestAccessChargeBilling:
    async def test_monthly_charge_folds_into_customer_and_margin(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        month, *_ = _this_month()
        tenant, lic = "t-ac-monthly", uuid.uuid4()
        db_session.add(
            TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30"))
        )
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        # A request row so the license line's app_key resolves to "fr" (the
        # licenses service is unreachable in tests → app_key falls back to the
        # usage service_key), exercising per-license attribution.
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(tenant_id=T(tenant), service_key=PRODUCT, amount_usd=Decimal("50")),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        totals = result["totals"]
        # usage: 10 * 1.30 = 13.00; + access 50 = 63.00 customer.
        assert Decimal(totals["access_charge_usd"]) == Decimal("50.00")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("63.00")
        assert Decimal(totals["internal_cost_usd"]) == Decimal("10")  # access is NOT internal
        assert Decimal(totals["margin_usd"]) == Decimal("53.00")
        assert len(totals["access_charges"]) == 1
        # per-license attribution by app_key (falls back to service_key when the
        # licenses service is unreachable in tests).
        lic_entry = result["licenses"][0]
        assert Decimal(lic_entry["costs"]["access_charge_usd"]) == Decimal("50")

    async def test_zero_usage_paid_access_still_bills(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        month, *_ = _this_month()
        tenant = "t-ac-unused"
        db_session.add(
            TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30"))
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(tenant_id=T(tenant), service_key=PRODUCT, amount_usd=Decimal("25")),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        totals = result["totals"]
        assert Decimal(totals["internal_cost_usd"]) == Decimal("0")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("25.00")
        assert Decimal(totals["margin_usd"]) == Decimal("25.00")

    async def test_one_time_charge_not_billed_in_later_month(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        month, _, month_start, *_ = _this_month()
        tenant = "t-ac-onetime"
        db_session.add(
            TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30"))
        )
        last_month_day = month_start - timedelta(days=1)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("77"),
                cadence="one_time",
                effective_from=datetime(
                    last_month_day.year, last_month_day.month, last_month_day.day, tzinfo=UTC
                ),
            ),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        assert Decimal(result["totals"]["access_charge_usd"]) == Decimal("0.00")

    async def test_overview_counts_zero_usage_paid_tenant(
        self, db_session: AsyncSession, monkeypatch
    ) -> None:
        # The platform overview must include a tenant whose only bill line is a
        # paid access charge (no usage rows at all).
        month, *_ = _this_month()
        tenant = "t-ac-overview"
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(tenant_id=T(tenant), service_key=PRODUCT, amount_usd=Decimal("40")),
        )
        await db_session.commit()

        overview = await compute_billing_overview(month, db_session)
        entry = next((e for e in overview.tenants if e.tenant_id == T(tenant)), None)
        assert entry is not None
        assert entry.customer_cost_usd == Decimal("40.00")
        assert overview.platform_total_customer_usd >= Decimal("40.00")

    async def test_internal_tenant_skips_access(
        self, db_session: AsyncSession, test_settings, monkeypatch
    ) -> None:
        month, *_ = _this_month()
        tenant = "t-ac-internal"
        db_session.add(
            TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal("30"))
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(tenant_id=T(tenant), service_key=PRODUCT, amount_usd=Decimal("50")),
        )
        await db_session.commit()

        # Force the byo_mode gate to collapse this tenant to the internal ($0) mode.
        async def _fake_byo_mode(_tid, _settings):
            return "local_allowed"

        monkeypatch.setattr("services.billing.get_byo_mode", _fake_byo_mode)

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        assert result["billing_mode"] == "internal"
        assert Decimal(result["totals"]["access_charge_usd"]) == Decimal("0.00")
        assert Decimal(result["totals"]["customer_cost_usd"]) == Decimal("0.00")


# --------------------------------------------------------------------------
# Included-usage allowance netting (the ALLOWANCE axis)
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestAllowanceNetting:
    """`tenant_access_charge.included_cost_usd` offsets incurred LLM cost BEFORE
    markup: the first $N of a product's provider cost bills at $0 (funded by the
    access fee), internal_cost stays raw, and the applied credit is reported."""

    async def _tenant(self, session, tenant, markup="30"):
        session.add(
            TenantConfig(tenant_id=T(tenant), tenant_name=tenant, markup_pct=Decimal(markup))
        )

    async def test_allowance_reduces_billed_before_markup(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        month, *_ = _this_month()
        tenant, lic = "t-al-basic", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        # amount 0 isolates the allowance effect; included 6 nets against incurred.
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                included_cost_usd=Decimal("6"),
            ),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        totals = result["totals"]
        # incurred 10 − allowance 6 = 4 marked up @30% → 5.20 billed. internal
        # stays raw at 10 → margin = 5.20 − 10 = −4.80 (platform absorbs the
        # included usage's provider cost; funded by the access fee elsewhere).
        assert Decimal(totals["included_applied_usd"]) == Decimal("6.00")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("5.20")
        assert Decimal(totals["internal_cost_usd"]) == Decimal("10")
        assert Decimal(totals["margin_usd"]) == Decimal("-4.80")
        assert Decimal(result["licenses"][0]["costs"]["included_applied_usd"]) == Decimal("6")

    async def test_allowance_capped_at_incurred(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        month, *_ = _this_month()
        tenant, lic = "t-al-cap", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="4"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                included_cost_usd=Decimal("10"),  # exceeds incurred 4
            ),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        totals = result["totals"]
        # only 4 of the 10 credit is consumed; billed floors at 0, never negative.
        assert Decimal(totals["included_applied_usd"]) == Decimal("4.00")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("0.00")

    async def test_allowance_scoped_to_its_product(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        month, *_ = _this_month()
        tenant, lic = "t-al-scope", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key="kag", cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,  # allowance only for 'fr'
                amount_usd=Decimal("0"),
                included_cost_usd=Decimal("10"),
            ),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        totals = result["totals"]
        # fr: (10−10)·1.3 = 0; kag: 10·1.3 = 13 (no allowance). customer = 13.
        assert Decimal(totals["included_applied_usd"]) == Decimal("10.00")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("13.00")

    async def test_tenant_wide_allowance_spans_products(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        month, *_ = _this_month()
        tenant, lic = "t-al-wide", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key="kag", cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        # service_key omitted → tenant-wide credit of 15, consumed across products.
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant), amount_usd=Decimal("0"), included_cost_usd=Decimal("15")
            ),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        totals = result["totals"]
        # 15 credit consumed across two 10-cost products (10 then 5), leaving
        # 5 of incurred marked up @30% → 6.50 billed.
        assert Decimal(totals["included_applied_usd"]) == Decimal("15.00")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("6.50")

    async def test_one_time_credit_nets_in_grant_month(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        # A one_time included credit is a durable prepaid balance: it now nets
        # against usage (was a no-op before WS-G). In its grant month the full
        # grant is available.
        month, *_ = _this_month()
        tenant, lic = "t-al-onetime", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("20"),
                cadence="one_time",
                included_cost_usd=Decimal("6"),
            ),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        totals = result["totals"]
        # incurred 10 − credit 6 = 4 @30% → 5.20 usage, + one_time access fee 20 = 25.20.
        assert Decimal(totals["included_applied_usd"]) == Decimal("6.00")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("25.20")

    async def test_one_time_credit_exhausts_across_months(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        # A 12-credit granted two months ago drains across months and never
        # resets: month -2 partial, month -1 drains the remainder, this month $0.
        m0, _, _ = _month_offset(0)
        m1, day1, _ = _month_offset(1)
        m2, day2, ef2 = _month_offset(2)
        tenant, lic = "t-al-onetime-multi", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_on(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="5", day=day2
        )
        await _seed_cost_on(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10", day=day1
        )
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                cadence="one_time",
                included_cost_usd=Decimal("12"),
                effective_from=ef2,
            ),
        )
        await db_session.commit()

        r2 = await generate_billing(T(tenant), m2, db_session, test_settings)
        r1 = await generate_billing(T(tenant), m1, db_session, test_settings)
        r0 = await generate_billing(T(tenant), m0, db_session, test_settings)
        # -2: draws 5 of 12 (billed 0); -1: draws remaining 7 of 10 → (10−7)·1.3 = 3.90;
        # this month: credit exhausted → 0 applied, full 10·1.3 = 13.00 billed.
        assert Decimal(r2["totals"]["included_applied_usd"]) == Decimal("5.00")
        assert Decimal(r2["totals"]["customer_cost_usd"]) == Decimal("0.00")
        assert Decimal(r1["totals"]["included_applied_usd"]) == Decimal("7.00")
        assert Decimal(r1["totals"]["customer_cost_usd"]) == Decimal("3.90")
        assert Decimal(r0["totals"]["included_applied_usd"]) == Decimal("0.00")
        assert Decimal(r0["totals"]["customer_cost_usd"]) == Decimal("13.00")

    async def test_one_time_netting_recomputes_stably(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        # Pure compute-on-read: billing the same month twice is identical (no
        # cross-call state / no ledger write draining the credit on every read).
        m0, _, _ = _month_offset(0)
        _, day1, _ = _month_offset(1)
        _, _, ef1 = _month_offset(1)
        tenant, lic = "t-al-onetime-stable", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_on(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="4", day=day1
        )
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                cadence="one_time",
                included_cost_usd=Decimal("10"),
                effective_from=ef1,
            ),
        )
        await db_session.commit()

        first = await generate_billing(T(tenant), m0, db_session, test_settings)
        second = await generate_billing(T(tenant), m0, db_session, test_settings)
        # 10 granted last month, 4 drawn then → 6 remaining this month; both reads agree.
        assert first["totals"]["included_applied_usd"] == second["totals"]["included_applied_usd"]
        assert Decimal(second["totals"]["included_applied_usd"]) == Decimal("6.00")

    async def test_one_time_and_monthly_coexist_specificity_order(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        # A monthly product-scoped credit and a one_time tenant-wide credit
        # coexist across different buckets. consume_allowance drains the
        # product bucket (monthly) first, then the tenant-wide bucket (one_time),
        # and the one_time balance carries across months.
        m0, _, _ = _month_offset(0)
        _, day1, ef1 = _month_offset(1)
        _, _, ef2 = _month_offset(2)
        tenant, lic = "t-al-onetime-coexist", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_on(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10", day=day1
        )
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        # Monthly product credit effective since two months ago (so it also
        # participates in the -1 fold), one_time tenant-wide credit granted -1.
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                cadence="monthly",
                included_cost_usd=Decimal("4"),
                effective_from=ef2,
            ),
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                amount_usd=Decimal("0"),
                cadence="one_time",
                included_cost_usd=Decimal("10"),
                effective_from=ef1,
            ),
        )
        await db_session.commit()

        result = await generate_billing(T(tenant), m0, db_session, test_settings)
        totals = result["totals"]
        # -1: monthly 4 + one_time 6 consumed of the 10 cost → one_time left = 4.
        # this month: monthly 4 (reset) + one_time 4 = 8 applied of 10 → (10−8)·1.3 = 2.60.
        assert Decimal(totals["included_applied_usd"]) == Decimal("8.00")
        assert Decimal(totals["customer_cost_usd"]) == Decimal("2.60")

    async def test_overview_nets_one_time_credit(self, db_session: AsyncSession) -> None:
        # The platform overview mirrors the per-tenant invoice for one_time too.
        month, *_ = _this_month()
        tenant, lic = "t-al-onetime-overview", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                amount_usd=Decimal("0"),
                cadence="one_time",
                included_cost_usd=Decimal("10"),  # tenant-wide, fully offsets the 10 incurred
            ),
        )
        await db_session.commit()

        overview = await compute_billing_overview(month, db_session)
        entry = next((e for e in overview.tenants if e.tenant_id == T(tenant)), None)
        assert entry is not None
        assert entry.customer_cost_usd == Decimal("0.00")

    async def test_internal_tenant_no_allowance(
        self, db_session: AsyncSession, test_settings, monkeypatch
    ) -> None:
        month, *_ = _this_month()
        tenant, lic = "t-al-internal", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                included_cost_usd=Decimal("6"),
            ),
        )
        await db_session.commit()

        async def _fake_byo_mode(_tid, _settings):
            return "local_allowed"

        monkeypatch.setattr("services.billing.get_byo_mode", _fake_byo_mode)

        result = await generate_billing(T(tenant), month, db_session, test_settings)
        assert result["billing_mode"] == "internal"
        # internal bills $0 across the board; the allowance is a no-op there.
        assert Decimal(result["totals"]["included_applied_usd"]) == Decimal("0.00")
        assert Decimal(result["totals"]["customer_cost_usd"]) == Decimal("0.00")

    async def test_allowance_is_recomputed_per_call(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        # The allowance is rebuilt from scratch each generate_billing call (no
        # cross-call state), so a monthly credit resets every billed month.
        month, *_ = _this_month()
        tenant, lic = "t-al-reset", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                included_cost_usd=Decimal("6"),
            ),
        )
        await db_session.commit()

        first = await generate_billing(T(tenant), month, db_session, test_settings)
        second = await generate_billing(T(tenant), month, db_session, test_settings)
        assert first["totals"]["included_applied_usd"] == second["totals"]["included_applied_usd"]
        assert Decimal(second["totals"]["included_applied_usd"]) == Decimal("6.00")

    async def test_overview_nets_allowance(self, db_session: AsyncSession) -> None:
        # The platform overview mirrors the per-tenant invoice's allowance offset.
        month, *_ = _this_month()
        tenant, lic = "t-al-overview", uuid.uuid4()
        await self._tenant(db_session, tenant)
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                included_cost_usd=Decimal("10"),  # fully offsets the 10 incurred
            ),
        )
        await db_session.commit()

        overview = await compute_billing_overview(month, db_session)
        entry = next((e for e in overview.tenants if e.tenant_id == T(tenant)), None)
        assert entry is not None
        assert entry.customer_cost_usd == Decimal("0.00")

    async def test_overview_excludes_system_cost_matching_invoice(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        """System/S2S cost (sentinel license) must not be billed to a customer nor
        consume the customer's allowance. The overview must reconcile to the
        per-tenant invoice on every money-path figure. Before the fix the overview
        folded the $5 system row into internal + customer and let it draw down the
        allowance, so its totals diverged from ``generate_billing``."""
        month, *_ = _this_month()
        tenant, lic = "t-al-syscost", uuid.uuid4()
        await self._tenant(db_session, tenant)
        # License-attributed cost: the only thing a customer is billed for.
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=lic, service_key=PRODUCT, cost="10"
        )
        await _seed_metering_daily(db_session, tenant=tenant, license_id=lic, service_key=PRODUCT)
        # System/S2S cost on the sentinel license — never billed, never nets allowance.
        await _seed_cost_daily(
            db_session, tenant=tenant, license_id=NULL_LICENSE_ID, service_key=PRODUCT, cost="5"
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T(tenant),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                included_cost_usd=Decimal("6"),
            ),
        )
        await db_session.commit()

        overview = await compute_billing_overview(month, db_session)
        entry = next((e for e in overview.tenants if e.tenant_id == T(tenant)), None)
        assert entry is not None
        result = await generate_billing(T(tenant), month, db_session, test_settings)
        totals = result["totals"]

        # Overview reconciles to the invoice on customer + internal.
        assert entry.customer_cost_usd == Decimal(totals["customer_cost_usd"])
        assert entry.internal_cost_usd == Decimal(totals["internal_cost_usd"])
        # Both exclude the $5 system row: internal is the raw $10 license cost, the
        # $6 allowance draws only against license cost, billed = (10 − 6) × 1.3.
        assert entry.internal_cost_usd == Decimal("10")
        assert entry.customer_cost_usd == Decimal("5.20")
        assert Decimal(totals["included_applied_usd"]) == Decimal("6.00")


# --------------------------------------------------------------------------
# one_time cadence: month bucketing is by the UTC calendar, not the session TZ
# --------------------------------------------------------------------------


@pytest.fixture
async def amsterdam_session_factory(db_engine):
    """A session factory pinned to Europe/Amsterdam (prod pgbouncer's TZ).

    Depends on ``db_engine`` so the schema exists and tables are truncated first.
    Pinning the TZ via libpq ``options`` makes psycopg read a stored ``timestamptz``
    back as a *session-TZ-aware* datetime — exactly the condition under which a bare
    ``effective_from.date()`` yields the local calendar date and mis-buckets a charge
    whose instant sits within ~2h of a UTC month boundary. Mirrors
    ``test_rollup_timezone.amsterdam_session_factory``.

    The options string MUST go through ``pgoptions_with``: an explicit libpq
    ``options`` parameter replaces ``PGOPTIONS`` rather than merging with it, so
    a bare literal would drop conftest's ``-c search_path=<TEST_SCHEMA>`` and
    strand this engine in an empty ``public`` schema.
    """
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
        connect_args={"options": pgoptions_with("timezone=Europe/Amsterdam")},
    )
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.mark.unit
async def test_one_time_charge_buckets_boundary_by_utc_month(amsterdam_session_factory):
    """A one_time charge effective 2026-06-30T23:30Z (= 01:30 Amsterdam on 07-01)
    is billable in JUNE (its UTC month), never July.

    FAILS on the pre-fix ``effective_from.date()`` — on an Amsterdam session the
    timestamptz reads back as 2026-07-01, so June returns nothing and July
    wrongly bills it. The fix (``effective_from.astimezone(UTC).date()``) buckets
    by the UTC calendar regardless of session TZ.
    """
    factory = amsterdam_session_factory

    # Sanity: the session really is Amsterdam, so this test exercises the bug.
    async with factory() as session:
        tz = (await session.execute(text("SHOW timezone"))).scalar_one()
    assert tz == "Europe/Amsterdam", f"fixture did not pin the session TZ (got {tz})"

    ef = datetime(2026, 6, 30, 23, 30, tzinfo=UTC)
    async with factory() as session:
        await ac_svc.upsert_access_charge(
            session,
            AccessChargeCreate(
                tenant_id=T("t-tz-boundary"),
                service_key=PRODUCT,
                amount_usd=Decimal("99"),
                cadence="one_time",
                effective_from=ef,
            ),
        )
        await session.commit()

    june_start, june_end = date(2026, 6, 1), date(2026, 7, 1)
    june_bill_ts = datetime(2026, 7, 1, tzinfo=UTC) - timedelta(microseconds=1)
    july_start, july_end = date(2026, 7, 1), date(2026, 8, 1)
    july_bill_ts = datetime(2026, 8, 1, tzinfo=UTC) - timedelta(microseconds=1)

    # A fresh session reloads effective_from from the DB (Amsterdam-aware).
    async with factory() as session:
        june = await ac_svc.active_access_charges_for_month(
            session,
            tenant_id=T("t-tz-boundary"),
            month_start=june_start,
            month_end=june_end,
            bill_ts=june_bill_ts,
        )
        july = await ac_svc.active_access_charges_for_month(
            session,
            tenant_id=T("t-tz-boundary"),
            month_start=july_start,
            month_end=july_end,
            bill_ts=july_bill_ts,
        )

    assert len(june) == 1 and june[0].amount_usd == Decimal("99"), (
        "the 23:30-UTC charge must bill in its UTC month (June)"
    )
    assert july == [], "the charge must NOT bill in the following month (July)"


class TestOneTimeUpsertIsIdempotent:
    """A ``one_time`` fee is charged once per its natural key — the
    ``(tenant_id, coalesce(service_key,''))`` lineage — EVER.

    Before the fix, ``AccessChargeCreate.effective_from`` defaulted to *now*, so
    any later re-upsert (an amount bump, a notes typo, a provisioning script
    re-run) stamped a fresh grant instant and the whole fee fell due AGAIN in
    the month of the edit. Billing is append-only — negative amounts are
    rejected at ingest and nothing but a migration ever excludes a cost event —
    so that second charge was permanent.
    """

    @staticmethod
    def _bounds(months_ago: int) -> tuple[datetime, date, date, datetime]:
        """(an instant in that month, month_start, month_end, bill_ts)."""
        _label, day, dt = _month_offset(months_ago)
        m_start, m_end, bill_ts = ac_svc._month_bounds(date(day.year, day.month, 1))
        return dt, m_start, m_end, bill_ts

    @staticmethod
    async def _charges(session: AsyncSession, bounds: tuple[date, date, datetime], tenant="t1"):
        m_start, m_end, bill_ts = bounds
        return await ac_svc.active_access_charges_for_month(
            session,
            tenant_id=T(tenant),
            month_start=m_start,
            month_end=m_end,
            bill_ts=bill_ts,
        )

    async def test_reupsert_does_not_rebill_a_one_time_fee(self, db_session: AsyncSession) -> None:
        grant_dt, g_start, g_end, g_bill_ts = self._bounds(2)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("99"),
                cadence="one_time",
                effective_from=grant_dt,
            ),
        )
        await db_session.commit()

        # THE EDIT: the same charge re-upserted with no explicit effective_from —
        # exactly what the CRUD route sends, since the field defaults to now().
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("99"),
                cadence="one_time",
                notes="corrected the note",
            ),
        )
        await db_session.commit()

        # THE FIX: the edit month owes nothing. The fee was already billed.
        _, _, this_start, this_end, this_bill_ts = _this_month()
        assert await self._charges(db_session, (this_start, this_end, this_bill_ts)) == [], (
            "a re-upserted one_time fee must not fall due a second time"
        )

        # …and the ORIGINAL grant month still bills it exactly ONCE. Pinning the
        # grant instant without retiring the ancestor row would leave two
        # overlapping one_time rows here and double-bill the very month the
        # fee belongs to.
        grant_rows = await self._charges(db_session, (g_start, g_end, g_bill_ts))
        assert len(grant_rows) == 1, "the grant month must select exactly one one_time row"
        assert grant_rows[0].amount_usd == Decimal("99")
        assert grant_rows[0].notes == "corrected the note", "the amendment must be the live row"

        # Same answer through the all-tenants aggregate the platform overview uses.
        totals = await ac_svc.active_access_charges_all_tenants(
            db_session, month_start=this_start, month_end=this_end, bill_ts=this_bill_ts
        )
        # ⚠️ EMPTY, not "sums to zero": the aggregate reports one entry per
        # applying term, so a one_time grant already billed contributes NO entry
        # at all in a later month.
        assert totals.get(T("t1"), []) == []

    async def test_repeated_reupserts_never_accumulate(self, db_session: AsyncSession) -> None:
        """Idempotent means idempotent — five edits, still one charge."""
        grant_dt, g_start, g_end, g_bill_ts = self._bounds(3)
        first_call: dict[str, Any] = {"effective_from": grant_dt}
        for i in range(5):
            await ac_svc.upsert_access_charge(
                db_session,
                AccessChargeCreate(
                    tenant_id=T("t1"),
                    service_key=PRODUCT,
                    amount_usd=Decimal("40"),
                    cadence="one_time",
                    # Only the FIRST call carries the grant instant; the rest
                    # default to now(), as the route does.
                    **(first_call if i == 0 else {}),
                ),
            )
            await db_session.commit()

        _, _, this_start, this_end, this_bill_ts = _this_month()
        assert await self._charges(db_session, (this_start, this_end, this_bill_ts)) == []
        assert len(await self._charges(db_session, (g_start, g_end, g_bill_ts))) == 1

        # Exactly one OPEN row survives (ix_accesscharge_open's invariant), and
        # every retired ancestor is zero-width so it can never be selected.
        history = await ac_svc.list_access_charge_history(
            db_session, tenant_id=T("t1"), service_key=PRODUCT
        )
        assert len([r for r in history if r.effective_until is None]) == 1
        assert all(
            r.effective_until == r.effective_from for r in history if r.effective_until is not None
        )

    async def test_a_new_one_time_charge_still_bills_in_its_month(
        self, db_session: AsyncSession
    ) -> None:
        """CONTROL — the legitimate case. A first-ever one_time grant is charged."""
        _, _, this_start, this_end, this_bill_ts = _this_month()
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("250"),
                cadence="one_time",
            ),
        )
        await db_session.commit()

        rows = await self._charges(db_session, (this_start, this_end, this_bill_ts))
        assert len(rows) == 1 and rows[0].amount_usd == Decimal("250")

    async def test_a_one_time_on_a_different_product_is_its_own_grant(
        self, db_session: AsyncSession
    ) -> None:
        """CONTROL — the lineage is per (tenant, service_key), not per tenant.

        A one_time already granted on PRODUCT must not suppress a first-ever
        one_time on another product, nor on the tenant-wide (NULL) lineage.
        """
        grant_dt, *_ = self._bounds(2)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("99"),
                cadence="one_time",
                effective_from=grant_dt,
            ),
        )
        await db_session.commit()

        _, _, this_start, this_end, this_bill_ts = _this_month()
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=OTHER_PRODUCT,
                amount_usd=Decimal("11"),
                cadence="one_time",
            ),
        )
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                amount_usd=Decimal("7"),
                cadence="one_time",
            ),
        )
        await db_session.commit()

        rows = await self._charges(db_session, (this_start, this_end, this_bill_ts))
        assert sorted(r.amount_usd for r in rows) == [Decimal("7"), Decimal("11")]

    async def test_a_monthly_charge_still_bills_every_month_after_a_reupsert(
        self, db_session: AsyncSession
    ) -> None:
        """CONTROL — the pin is one_time-only. A monthly fee is recurring by design."""
        past_dt, *_ = self._bounds(2)
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("20"),
                cadence="monthly",
                effective_from=past_dt,
            ),
        )
        await db_session.commit()
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("25"),
                cadence="monthly",
            ),
        )
        await db_session.commit()

        _, _, this_start, this_end, this_bill_ts = _this_month()
        rows = await self._charges(db_session, (this_start, this_end, this_bill_ts))
        assert len(rows) == 1 and rows[0].amount_usd == Decimal("25")

    async def test_a_one_time_credit_is_not_re_granted_by_a_reupsert(
        self, db_session: AsyncSession
    ) -> None:
        """The allowance axis re-grants too — a durable prepaid credit is granted once.

        ``included_cost_usd`` on a one_time charge is drawn down across months
        from its grant month. A re-upsert used to move that grant month forward,
        handing the tenant the whole credit again.
        """
        grant_dt, g_start, _g_end, _ = self._bounds(2)
        lic = uuid.uuid4()
        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                cadence="one_time",
                included_cost_usd=Decimal("10"),
                effective_from=grant_dt,
            ),
        )
        # Spend the whole credit in its grant month.
        await _seed_cost_on(
            db_session,
            tenant="t1",
            license_id=lic,
            service_key=PRODUCT,
            cost="10",
            day=g_start.replace(day=15),
        )
        await db_session.commit()

        await ac_svc.upsert_access_charge(
            db_session,
            AccessChargeCreate(
                tenant_id=T("t1"),
                service_key=PRODUCT,
                amount_usd=Decimal("0"),
                cadence="one_time",
                included_cost_usd=Decimal("10"),
                notes="same credit, edited",
            ),
        )
        await db_session.commit()

        _, _, this_start, _this_end, this_bill_ts = _this_month()
        remaining = await ac_svc.one_time_remaining_before(
            db_session, tenant_id=T("t1"), month_start=this_start, bill_ts=this_bill_ts
        )
        assert remaining == {}, "an exhausted one_time credit must not be re-granted by an edit"
