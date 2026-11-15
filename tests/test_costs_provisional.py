"""GET /api/costs/provisional — tenant (or one subject's) not-yet-rolled-up cost.

Real-DB behaviour: tenant scoping, subject filter, the watermark split (a rolled
day's raw events are not provisional), the no-rollup-yet case and
excluded_from_billing rows. Shares its query with /projects/{id}'s provisional
block (routers.costs._provisional_cost).
"""

import uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import CostDaily, CostEvent
from tests.conftest import T

TENANT_A = T("cprov-tenant-a")
TENANT_B = T("cprov-tenant-b")
ALICE = uuid.UUID("5b1ec700-0000-4000-8000-00000000000a")
BOB = uuid.UUID("5b1ec700-0000-4000-8000-00000000000b")


def _today() -> date:
    return datetime.now(UTC).date()


def _at(day: date, hh: int, mm: int = 0, ss: int = 0) -> datetime:
    return datetime.combine(day, time(hh, mm, ss), tzinfo=UTC)


def _factory(db_engine):
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


async def _daily(db_engine, *, tenant_id, day, cost="1.0", calls=1, subject_id=None):
    async with _factory(db_engine)() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                license_id=uuid.uuid4(),
                subject_id=subject_id,
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                bucket_date=day,
                total_input_tokens=100,
                total_output_tokens=10,
                total_cost_usd=Decimal(cost),
                request_count=calls,
            )
        )
        await session.commit()


async def _event(
    db_engine,
    *,
    tenant_id,
    cost,
    created_at,
    subject_id=None,
    project_id=None,
    event_ts=None,
    excluded=False,
    tin=1000,
    tout=200,
):
    async with _factory(db_engine)() as session:
        session.add(
            CostEvent(
                tenant_id=tenant_id,
                subject_id=subject_id,
                project_id=project_id,
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                served_model="claude-sonnet-4-20250514",
                model="claude-sonnet-4-20250514",
                input_tokens=tin,
                output_tokens=tout,
                cost_usd=None if cost is None else Decimal(cost),
                excluded_from_billing=excluded,
                created_at=created_at,
                event_ts=event_ts,
            )
        )
        await session.commit()


def _get(app_client, tenant, subject_id=None):
    params = {"subject_id": str(subject_id)} if subject_id is not None else None
    return app_client.get("/api/costs/provisional", params=params, headers={"X-Tenant-Id": tenant})


@pytest.mark.unit
class TestCostsProvisional:
    async def test_watermark_split_rolled_day_excluded(self, db_engine, app_client):
        today = _today()
        yesterday = today - timedelta(days=1)
        await _daily(db_engine, tenant_id=TENANT_A, day=yesterday, cost="5.0", calls=10)
        # Rolled day, by producer time and by ingestion time: not provisional.
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            cost="4.00",
            event_ts=_at(yesterday, 12),
            created_at=_at(yesterday, 12),
        )
        # Producer time on the rolled day, ingested after midnight: still yesterday's.
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            cost="1.00",
            event_ts=_at(yesterday, 23, 59, 50),
            created_at=_at(today, 0, 0, 5),
        )
        now = datetime.now(UTC)
        await _event(db_engine, tenant_id=TENANT_A, cost="2.00", created_at=now)
        # Pricing miss: a call at $0, tokens still counted.
        await _event(db_engine, tenant_id=TENANT_A, cost=None, created_at=now)

        r = _get(app_client, TENANT_A)
        assert r.status_code == 200, r.text
        d = r.json()["data"]
        assert d["tenant_id"] == TENANT_A
        assert d["subject_id"] is None
        assert d["currency"] == "USD"
        assert d["settled_through"] == yesterday.isoformat()
        assert d["since"] == today.isoformat()
        assert d["cost"] == "2.000000"
        assert d["calls"] == 2
        assert d["tokens_in"] == 2000
        assert d["tokens_out"] == 400
        assert d["basis"]
        assert d["as_of"]

    async def test_no_rollup_yet_everything_is_provisional(self, db_engine, app_client):
        old = datetime.now(UTC) - timedelta(days=3)
        await _event(db_engine, tenant_id=TENANT_A, cost="1.5", event_ts=old, created_at=old)
        await _event(db_engine, tenant_id=TENANT_A, cost="0.5", created_at=datetime.now(UTC))
        d = _get(app_client, TENANT_A).json()["data"]
        assert d["settled_through"] is None
        assert d["since"] is None
        assert d["cost"] == "2.000000"
        assert d["calls"] == 2

    async def test_tenant_scoping(self, db_engine, app_client):
        now = datetime.now(UTC)
        await _event(db_engine, tenant_id=TENANT_A, subject_id=ALICE, cost="2.00", created_at=now)
        await _event(db_engine, tenant_id=TENANT_B, subject_id=ALICE, cost="0.10", created_at=now)

        b = _get(app_client, TENANT_B).json()["data"]
        assert b["tenant_id"] == TENANT_B
        assert b["cost"] == "0.100000"
        assert b["calls"] == 1
        # Same subject uuid, asked from B: only B's spend.
        assert _get(app_client, TENANT_B, ALICE).json()["data"]["cost"] == "0.100000"
        # A tenant with nothing sees zeros, never A's or B's figures.
        c = _get(app_client, T("cprov-tenant-c"), ALICE).json()["data"]
        assert c["cost"] == "0.000000"
        assert c["calls"] == 0
        assert _get(app_client, TENANT_A).json()["data"]["cost"] == "2.000000"

    async def test_subject_filter(self, db_engine, app_client):
        now = datetime.now(UTC)
        await _event(db_engine, tenant_id=TENANT_A, subject_id=ALICE, cost="1.00", created_at=now)
        await _event(db_engine, tenant_id=TENANT_A, subject_id=ALICE, cost="0.25", created_at=now)
        await _event(db_engine, tenant_id=TENANT_A, subject_id=BOB, cost="3.00", created_at=now)
        await _event(db_engine, tenant_id=TENANT_A, subject_id=None, cost="0.50", created_at=now)

        a = _get(app_client, TENANT_A, ALICE).json()["data"]
        assert a["subject_id"] == str(ALICE)
        assert a["cost"] == "1.250000"
        assert a["calls"] == 2
        assert _get(app_client, TENANT_A, BOB).json()["data"]["cost"] == "3.000000"
        assert _get(app_client, TENANT_A, uuid.uuid4()).json()["data"]["calls"] == 0
        whole = _get(app_client, TENANT_A).json()["data"]
        assert whole["cost"] == "4.750000"
        assert whole["calls"] == 4

    async def test_excluded_from_billing_dropped(self, db_engine, app_client):
        now = datetime.now(UTC)
        await _event(db_engine, tenant_id=TENANT_A, subject_id=ALICE, cost="1.00", created_at=now)
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            subject_id=ALICE,
            cost="9.00",
            created_at=now,
            excluded=True,
        )
        for subject in (None, ALICE):
            d = _get(app_client, TENANT_A, subject).json()["data"]
            assert d["cost"] == "1.000000"
            assert d["calls"] == 1

    async def test_matches_project_endpoint_split(self, db_engine, app_client):
        """One implementation: when the tenant's only spend is one project, the
        project's ``provisional`` block and this endpoint report the same figures."""
        today = _today()
        yesterday = today - timedelta(days=1)
        project = uuid.uuid4()
        # Watermark from another tenant: the split point is GLOBAL.
        await _daily(db_engine, tenant_id=TENANT_B, day=yesterday)
        # Settled side (rolled day): in neither provisional figure.
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=project,
            cost="4.00",
            event_ts=_at(yesterday, 12),
            created_at=_at(yesterday, 12),
        )
        now = datetime.now(UTC)
        await _event(db_engine, tenant_id=TENANT_A, project_id=project, cost="1.25", created_at=now)
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=project,
            cost="0.50",
            created_at=now,
            tin=300,
            tout=30,
        )
        await _event(db_engine, tenant_id=TENANT_A, project_id=project, cost=None, created_at=now)
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=project,
            cost="9.00",
            created_at=now,
            excluded=True,
        )

        p = app_client.get(
            f"/api/costs/projects/{project}", headers={"X-Tenant-Id": TENANT_A}
        ).json()["data"]["provisional"]
        d = _get(app_client, TENANT_A).json()["data"]
        assert d["settled_through"] == yesterday.isoformat()
        assert p["since"] == d["since"] == today.isoformat()
        assert p["cost"] == d["cost"] == "1.750000"
        assert p["calls"] == d["calls"] == 3
        assert p["tokens_in"] == d["tokens_in"] == 2300
        assert p["tokens_out"] == d["tokens_out"] == 430

    async def test_invalid_subject_id(self, app_client):
        r = app_client.get(
            "/api/costs/provisional",
            params={"subject_id": "not-a-uuid"},
            headers={"X-Tenant-Id": TENANT_A},
        )
        assert r.status_code == 422

    async def test_requires_tenant_context(self, app_client):
        r = app_client.get("/api/costs/provisional", headers={"X-Tenant-Id": ""})
        assert r.status_code == 400
