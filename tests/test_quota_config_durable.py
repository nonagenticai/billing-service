"""Durable request-quota config — model, config write, status/check, delete.

Every test runs against real Postgres (the repo standard; ``conftest`` errors
without a DB) and asserts the durable row AND the HTTP response together, so a
green run proves the whole write→persist→read path — not just a 201 body.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import MeteringMonthlyUsage, RequestQuotaConfig
from tests.conftest import T


async def _get_config(db_engine, license_id: uuid.UUID) -> RequestQuotaConfig | None:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        return (
            await session.execute(
                select(RequestQuotaConfig).where(RequestQuotaConfig.license_id == license_id)
            )
        ).scalar_one_or_none()


async def _seed_config(db_engine, license_id, quota_monthly, tenant_id=T("t-seed")):
    # tenant_id is NOT NULL (migration 0007); the default is a non-null sentinel
    # for tests that do not exercise tenant scoping. Tests that send an
    # X-Tenant-Id header pass a matching tenant_id explicitly.
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add(
            RequestQuotaConfig(
                license_id=license_id, tenant_id=tenant_id, quota_monthly=quota_monthly
            )
        )
        await session.commit()


async def _seed_monthly_usage(db_engine, license_id, plan_id, count, subject_id=None):
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


# --- Model / migration ---


class TestRequestQuotaConfigModel:
    @pytest.mark.unit
    async def test_row_roundtrips(self, db_engine):
        lic = uuid.uuid4()
        await _seed_config(db_engine, lic, 5000, tenant_id=T("t-1"))
        row = await _get_config(db_engine, lic)
        assert row is not None
        assert row.quota_monthly == 5000
        assert row.tenant_id == T("t-1")
        assert row.created_at is not None and row.updated_at is not None

    @pytest.mark.unit
    async def test_zero_is_a_valid_block_all_row(self, db_engine):
        """quota_monthly=0 is a first-class BLOCK-ALL value and must persist.

        Was rejected by the old ``> 0`` CHECK, which made a genuine
        zero-allowance plan unstorable. Under ``ck_request_quota_non_negative``
        (migration 0008) a 0 round-trips; unlimited is the ABSENCE of a row.
        """
        lic = uuid.uuid4()
        await _seed_config(db_engine, lic, 0, tenant_id=T("t-0"))
        row = await _get_config(db_engine, lic)
        assert row is not None
        assert row.quota_monthly == 0

    @pytest.mark.unit
    async def test_check_constraint_rejects_negative(self, db_engine):
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            # tenant_id supplied so this isolates the quota_monthly>=0 CHECK, not
            # the (also-present) NOT NULL tenant_id constraint.
            session.add(
                RequestQuotaConfig(license_id=uuid.uuid4(), tenant_id=T("t-neg"), quota_monthly=-1)
            )
            with pytest.raises(IntegrityError):
                await session.commit()

    @pytest.mark.unit
    def test_tenant_id_is_not_null(self):
        """tenant_id is NOT NULL (migration 0007) — the model and the create_all
        schema must agree so the NULL-tenant cross-tenant-read shape is gone."""
        assert RequestQuotaConfig.__table__.columns["tenant_id"].nullable is False

    @pytest.mark.unit
    async def test_tenant_id_not_null_enforced_in_db(self, db_engine):
        """The live DB schema (build via create_all / equivalent to alembic head)
        rejects a NULL tenant_id at insert time."""
        from sqlalchemy import inspect

        async with db_engine.connect() as conn:
            cols = await conn.run_sync(lambda c: inspect(c).get_columns("request_quota_config"))
        tenant_col = next(c for c in cols if c["name"] == "tenant_id")
        assert tenant_col["nullable"] is False


# --- Config write (POST /api/quota/config) ---


class TestQuotaConfigWrite:
    @pytest.mark.unit
    async def test_config_upserts_durable_row(self, db_engine, app_client):
        lic = uuid.uuid4()
        r = app_client.post(
            "/api/quota/config",
            json={"license_id": str(lic), "tenant_id": T("t-x"), "quota_monthly": 5000},
        )
        assert r.status_code == 201, r.text
        # durable row
        row = await _get_config(db_engine, lic)
        assert row is not None and row.quota_monthly == 5000 and row.tenant_id == T("t-x")

    @pytest.mark.unit
    async def test_config_upsert_is_idempotent(self, db_engine, app_client):
        lic = uuid.uuid4()
        app_client.post(
            "/api/quota/config",
            json={"license_id": str(lic), "tenant_id": T("t-x"), "quota_monthly": 100},
        )
        first = await _get_config(db_engine, lic)
        r2 = app_client.post(
            "/api/quota/config",
            json={"license_id": str(lic), "tenant_id": T("t-y"), "quota_monthly": 999},
        )
        assert r2.status_code == 201, r2.text
        # exactly one row, latest value, tenant updated, created_at preserved
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            rows = (
                (
                    await session.execute(
                        select(RequestQuotaConfig).where(RequestQuotaConfig.license_id == lic)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1
        assert rows[0].quota_monthly == 999
        assert rows[0].tenant_id == T("t-y")
        assert first is not None
        assert rows[0].created_at == first.created_at


# --- Status / check parity ---


class TestQuotaStatusParity:
    @pytest.mark.unit
    async def test_status_reports_configured_but_planless_license(self, db_engine, app_client):
        """A license with a durable quota but NO plan is no longer silently dropped.

        This is the exact regression: config → 201 but status showed the license
        empty because status derived the limit from ``plan.quota_monthly`` and
        omitted planless licenses.
        """
        lic = uuid.uuid4()
        await _seed_config(db_engine, lic, 500, tenant_id=T("t-noplan"))
        await _seed_monthly_usage(db_engine, license_id=lic, plan_id=uuid.uuid4(), count=120)
        licenses = [{"id": str(lic), "tenant_id": T("t-noplan"), "plan": None}]
        with patch("routers.quota.fetch_licenses", AsyncMock(return_value=licenses)):
            r = app_client.get(
                f"/api/quota/status/{T('t-noplan')}",
                headers={"X-Tenant-Id": T("t-noplan")},
            )
        assert r.status_code == 200, r.text
        out = r.json()["data"]["licenses"]
        assert len(out) == 1
        assert out[0]["license_id"] == str(lic)
        assert out[0]["quota_monthly"] == 500
        assert out[0]["used_this_month"] == 120
        assert out[0]["remaining"] == 380

    @pytest.mark.unit
    async def test_status_config_overrides_plan_quota(self, db_engine, app_client):
        lic, plan_id = uuid.uuid4(), uuid.uuid4()
        await _seed_config(db_engine, lic, 999, tenant_id=T("t-ovr"))
        licenses = [
            {
                "id": str(lic),
                "tenant_id": T("t-ovr"),
                "plan": {"id": str(plan_id), "quota_monthly": 10},
            }
        ]
        with patch("routers.quota.fetch_licenses", AsyncMock(return_value=licenses)):
            r = app_client.get(
                f"/api/quota/status/{T('t-ovr')}", headers={"X-Tenant-Id": T("t-ovr")}
            )
        assert r.status_code == 200, r.text
        entry = r.json()["data"]["licenses"][0]
        assert entry["quota_monthly"] == 999  # durable config wins, not the plan's 10

    @pytest.mark.unit
    async def test_status_still_omits_planless_unconfigured_license(self, db_engine, app_client):
        """No plan AND no durable config → still nothing to report (unchanged)."""
        lic = uuid.uuid4()
        licenses = [{"id": str(lic), "tenant_id": T("t-bare"), "plan": None}]
        with patch("routers.quota.fetch_licenses", AsyncMock(return_value=licenses)):
            r = app_client.get(
                f"/api/quota/status/{T('t-bare')}", headers={"X-Tenant-Id": T("t-bare")}
            )
        assert r.status_code == 200
        assert r.json()["data"]["licenses"] == []

    @pytest.mark.unit
    async def test_check_endpoint_uses_durable_config(self, db_engine, app_client):
        """GET /api/quota/check with no quota_monthly query resolves the durable row."""
        lic, plan_id = uuid.uuid4(), uuid.uuid4()
        await _seed_config(db_engine, lic, 300, tenant_id=T("t-chk"))
        await _seed_monthly_usage(db_engine, license_id=lic, plan_id=plan_id, count=310)
        r = app_client.get(
            "/api/quota/check",
            params={"license_id": str(lic), "plan_id": str(plan_id)},
            headers={"X-Tenant-Id": T("t-chk")},
        )
        assert r.status_code == 200, r.text
        data = r.json()["data"]
        assert data["quota_monthly"] == 300
        assert data["used_this_month"] == 310
        assert data["over_quota"] is True  # 310 >= 300, enforced from the durable config

    @pytest.mark.unit
    async def test_check_without_plan_id_reads_usage_from_monthly_usage(
        self, db_engine, app_client
    ):
        """No plan_id: 200, plan_id echoed null, usage summed from metering_monthly_usage.

        Two subjects on two plans: usage is the licence-month total across both.
        """
        lic = uuid.uuid4()
        await _seed_config(db_engine, lic, 1000, tenant_id=T("t-noplanid"))
        await _seed_monthly_usage(db_engine, license_id=lic, plan_id=uuid.uuid4(), count=400)
        await _seed_monthly_usage(
            db_engine, license_id=lic, plan_id=uuid.uuid4(), count=250, subject_id=uuid.uuid4()
        )
        r = app_client.get(
            "/api/quota/check",
            params={"license_id": str(lic)},
            headers={"X-Tenant-Id": T("t-noplanid")},
        )
        assert r.status_code == 200, r.text
        data = r.json()["data"]
        assert data["plan_id"] is None
        assert data["quota_monthly"] == 1000
        assert data["used_this_month"] == 650
        assert data["remaining"] == 350
        assert data["over_quota"] is False

    @pytest.mark.unit
    async def test_config_zero_persists_and_blocks_end_to_end(self, db_engine, app_client):
        """quota_monthly=0 write→persist→enforce: 201 stored (was 422 under gt=0),
        and a subsequent /check reports over_quota=True at 0 usage (block-all)."""
        lic, plan_id = uuid.uuid4(), uuid.uuid4()
        r = app_client.post(
            "/api/quota/config",
            json={"license_id": str(lic), "tenant_id": T("t-block"), "quota_monthly": 0},
        )
        assert r.status_code == 201, r.text
        row = await _get_config(db_engine, lic)
        assert row is not None and row.quota_monthly == 0
        # /check: 0 usage, 0 limit -> blocked
        chk = app_client.get(
            "/api/quota/check",
            params={"license_id": str(lic), "plan_id": str(plan_id)},
            headers={"X-Tenant-Id": T("t-block")},
        )
        assert chk.status_code == 200, chk.text
        data = chk.json()["data"]
        assert data["quota_monthly"] == 0
        assert data["remaining"] == 0
        assert data["over_quota"] is True


# --- Delete (DELETE /api/quota/config/{license_id}) ---


class TestQuotaConfigDelete:
    @pytest.mark.unit
    async def test_delete_removes_row(self, db_engine, app_client):
        lic = uuid.uuid4()
        await _seed_config(db_engine, lic, 100)

        r = app_client.delete(f"/api/quota/config/{lic}")
        assert r.status_code == 200, r.text
        assert r.json()["deleted"] == "1"
        assert await _get_config(db_engine, lic) is None

    @pytest.mark.unit
    async def test_delete_absent_config_is_idempotent(self, app_client):
        r = app_client.delete(f"/api/quota/config/{uuid.uuid4()}")
        assert r.status_code == 200, r.text
        assert r.json()["deleted"] == "0"

    @pytest.mark.unit
    async def test_delete_invalid_uuid_400(self, app_client):
        r = app_client.delete("/api/quota/config/not-a-uuid")
        assert r.status_code == 400


class TestQuotaCheckTenantIsolation:
    """A customer caller (X-Tenant-Id present) may only read its OWN license.

    A durable row owned by a DIFFERENT tenant → 404 (indistinguishable from a
    non-existent license). The tenant-of-record is `request_quota_config.tenant_id`,
    which provisioning's `register_quota` DOES persist (see the config-write tests
    asserting `row.tenant_id`). The gap: a row with NULL tenant_id cannot be proven
    foreign, so it is still served cross-tenant — documented below.
    """

    @pytest.mark.unit
    async def test_foreign_tenant_gets_404(self, db_engine, app_client):
        lic, plan_id = uuid.uuid4(), uuid.uuid4()
        await _seed_config(db_engine, lic, 300, tenant_id=T("tenant-B"))
        r = app_client.get(
            "/api/quota/check",
            params={"license_id": str(lic), "plan_id": str(plan_id)},
            headers={"X-Tenant-Id": T("tenant-A")},  # A trying to read B's license
        )
        assert r.status_code == 404, r.text

    @pytest.mark.unit
    async def test_own_tenant_reads_own_license(self, db_engine, app_client):
        lic, plan_id = uuid.uuid4(), uuid.uuid4()
        await _seed_config(db_engine, lic, 300, tenant_id=T("tenant-B"))
        r = app_client.get(
            "/api/quota/check",
            params={"license_id": str(lic), "plan_id": str(plan_id)},
            headers={"X-Tenant-Id": T("tenant-B")},
        )
        assert r.status_code == 200, r.text
        assert r.json()["data"]["quota_monthly"] == 300

    @pytest.mark.unit
    async def test_admin_no_header_unaffected(self, db_engine, app_client):
        """Admin caller (no X-Tenant-Id) may read any tenant's license — no 404."""
        lic, plan_id = uuid.uuid4(), uuid.uuid4()
        await _seed_config(db_engine, lic, 300, tenant_id=T("tenant-B"))
        r = app_client.get(
            "/api/quota/check",
            params={"license_id": str(lic), "plan_id": str(plan_id)},
        )
        assert r.status_code == 200, r.text
        assert r.json()["data"]["quota_monthly"] == 300

    @pytest.mark.unit
    async def test_null_tenant_row_cannot_be_constructed(self, db_engine):
        """FIX CONTRACT (was ``..._still_served_documents_gap``): a NULL-tenant
        row was the ONLY cross-tenant-readable shape — it could not be proven
        foreign, so the check served it to any caller. tenant_id is now NOT NULL
        (migration 0007), so that row can no longer be constructed at all:
        inserting one raises an IntegrityError.
        """
        lic = uuid.uuid4()
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        with pytest.raises(IntegrityError):
            async with factory() as session:
                session.add(RequestQuotaConfig(license_id=lic, tenant_id=None, quota_monthly=300))
                await session.commit()

    @pytest.mark.unit
    async def test_no_row_is_served(self, app_client):
        """No durable row cannot be proven foreign → served (query-param fallback)."""
        lic, plan_id = uuid.uuid4(), uuid.uuid4()
        r = app_client.get(
            "/api/quota/check",
            params={"license_id": str(lic), "plan_id": str(plan_id), "quota_monthly": 500},
            headers={"X-Tenant-Id": T("tenant-A")},
        )
        assert r.status_code == 200, r.text
        assert r.json()["data"]["quota_monthly"] == 500


class TestQuotaConfigCarriesAppId:
    """``app_id`` on POST /api/quota/config — the field the licence resolver needs.

    WHY THIS SUITE EXISTS. ``license_resolver._build_map`` selects candidates with
    ``where(RequestQuotaConfig.app_id.is_not(None))``, so a row with a NULL ``app_id``
    can never resolve and every cost event for that licence's tenant stays unattributed
    forever. Before this change the ONLY writer that ever set ``app_id`` was
    ``POST /api/budget/sync-from-licenses``, which nothing schedules; the armed daily
    reconcile CronJob pushes to ``/api/quota/config`` instead, whose request model had
    no app field at all. Measured in prod 2026-08-28: 26 of 189 rows had a NULL app_id.

    Each test asserts the DURABLE ROW, never the 201 body — a model that merely accepts
    the field while the router drops it would pass a response-shape assertion.
    """

    @pytest.mark.unit
    async def test_app_id_reaches_the_durable_row(self, db_engine, app_client):
        """The whole point: POST it, read it back off the row."""
        lic = uuid.uuid4()
        app = uuid.uuid4()
        r = app_client.post(
            "/api/quota/config",
            json={
                "license_id": str(lic),
                "tenant_id": T("t-app"),
                "quota_monthly": 42,
                "app_id": str(app),
                "app_name": "Workers Module",
            },
        )
        assert r.status_code == 201, r.text
        row = await _get_config(db_engine, lic)
        assert row is not None
        assert row.app_id == app, "app_id did not reach the row — the resolver stays blind"
        assert row.app_name == "Workers Module"

    @pytest.mark.unit
    async def test_omitting_app_id_still_writes_the_row(self, db_engine, app_client):
        """Both fields MUST stay optional.

        Every existing caller — including the reconcile CronJob until its own change
        lands — omits them. A required field here is a 422 on the estate's only
        scheduled quota writer.
        """
        lic = uuid.uuid4()
        r = app_client.post(
            "/api/quota/config",
            json={"license_id": str(lic), "tenant_id": T("t-noapp"), "quota_monthly": 7},
        )
        assert r.status_code == 201, r.text
        row = await _get_config(db_engine, lic)
        assert row is not None and row.quota_monthly == 7
        assert row.app_id is None

    @pytest.mark.unit
    async def test_omitted_app_id_does_not_clobber_a_stored_one(self, db_engine, app_client):
        """⛔ THE REGRESSION THIS GUARDS.

        The reconcile CronJob re-pushes quota for licences it finds, carrying no app.
        If that push overwrote ``app_id`` with NULL, arming this field would make
        attribution WORSE every night at 03:41 UTC — a row that resolved yesterday
        would stop resolving. ``upsert_request_quota_config`` COALESCEs for exactly
        this reason; this test is what keeps that true.
        """
        lic = uuid.uuid4()
        app = uuid.uuid4()
        first = app_client.post(
            "/api/quota/config",
            json={
                "license_id": str(lic),
                "tenant_id": T("t-keep"),
                "quota_monthly": 10,
                "app_id": str(app),
                "app_name": "KAG Module",
            },
        )
        assert first.status_code == 201, first.text

        second = app_client.post(
            "/api/quota/config",
            json={"license_id": str(lic), "tenant_id": T("t-keep"), "quota_monthly": 99},
        )
        assert second.status_code == 201, second.text

        row = await _get_config(db_engine, lic)
        assert row is not None
        assert row.quota_monthly == 99, "the quota SHOULD update"
        assert row.app_id == app, "app_id was clobbered by an app-less push — resolver goes blind"
        assert row.app_name == "KAG Module"
