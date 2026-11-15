"""GET /api/costs/projects/{project_id} — settled (rollup) + provisional (raw) per project.

Covers: the split at the rollup watermark (incl. an event whose producer time is
on the settled day but which was ingested after midnight), by_card with the
null bucket, excluded_from_billing rows, tenant isolation and the empty project.
"""

import uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import NULL_PROJECT_ID, CostDaily, CostEvent
from tests.conftest import T

TENANT_A = T("cbp-tenant-a")
TENANT_B = T("cbp-tenant-b")
PROJECT = uuid.UUID("0ad49b9c-0000-4000-8000-000000000001")
OTHER_PROJECT = uuid.UUID("0ad49b9c-0000-4000-8000-000000000002")


def _today() -> date:
    return datetime.now(UTC).date()


def _at(day: date, hh: int, mm: int = 0, ss: int = 0) -> datetime:
    return datetime.combine(day, time(hh, mm, ss), tzinfo=UTC)


def _factory(db_engine):
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


async def _daily(db_engine, *, tenant_id, project_id, day, cost, calls, tin=100, tout=10):
    async with _factory(db_engine)() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                license_id=uuid.uuid4(),
                project_id=project_id,
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                bucket_date=day,
                total_input_tokens=tin,
                total_output_tokens=tout,
                total_cost_usd=Decimal(cost),
                request_count=calls,
            )
        )
        await session.commit()


async def _event(
    db_engine,
    *,
    tenant_id,
    project_id,
    cost,
    created_at,
    event_ts=None,
    card_id=None,
    excluded=False,
    tin=1000,
    tout=200,
):
    async with _factory(db_engine)() as session:
        session.add(
            CostEvent(
                tenant_id=tenant_id,
                project_id=project_id,
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                served_model="claude-sonnet-4-20250514",
                model="claude-sonnet-4-20250514",
                input_tokens=tin,
                output_tokens=tout,
                cost_usd=Decimal(cost),
                extra_metadata=({"card_id": card_id} if card_id is not None else None),
                excluded_from_billing=excluded,
                created_at=created_at,
                event_ts=event_ts,
            )
        )
        await session.commit()


def _get(app_client, project_id, tenant):
    return app_client.get(f"/api/costs/projects/{project_id}", headers={"X-Tenant-Id": tenant})


@pytest.mark.unit
class TestCostsByProject:
    async def test_settled_provisional_split_at_watermark(self, db_engine, app_client):
        today = _today()
        yesterday = today - timedelta(days=1)
        # Rolled-up yesterday -> watermark = yesterday.
        await _daily(
            db_engine,
            tenant_id=TENANT_A,
            project_id=PROJECT,
            day=yesterday,
            cost="5.000000",
            calls=10,
        )
        # Producer time on the SETTLED day, ingested after midnight: belongs to
        # yesterday (as rollup_daily buckets it), so it must NOT be provisional.
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=PROJECT,
            cost="1.00",
            card_id="c1",
            event_ts=_at(yesterday, 23, 59, 50),
            created_at=_at(today, 0, 0, 5),
        )
        now = datetime.now(UTC)
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=PROJECT,
            cost="2.00",
            card_id="c1",
            event_ts=now,
            created_at=now,
        )
        await _event(db_engine, tenant_id=TENANT_A, project_id=PROJECT, cost="0.50", created_at=now)
        # Parked duplicate: never money.
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=PROJECT,
            cost="9.00",
            card_id="c1",
            created_at=now,
            excluded=True,
        )

        r = _get(app_client, PROJECT, TENANT_A)
        assert r.status_code == 200, r.text
        d = r.json()["data"]
        assert d["project_id"] == str(PROJECT)
        assert d["currency"] == "USD"
        assert d["basis"]
        assert d["as_of"]
        assert d["settled"] == [
            {
                "day": yesterday.isoformat(),
                "cost": "5.000000",
                "calls": 10,
                "tokens_in": 100,
                "tokens_out": 10,
            }
        ]
        prov = d["provisional"]
        assert prov["since"] == today.isoformat()
        # Every cost field carries the rollup's 6-dp scale, raw-derived ones included.
        assert prov["cost"] == "2.500000"
        assert prov["calls"] == 2
        assert prov["tokens_in"] == 2000
        assert d["totals"]["cost"] == "7.500000"
        assert d["totals"]["calls"] == 12
        assert d["totals"]["settled_cost"] == "5.000000"
        assert d["totals"]["provisional_cost"] == "2.500000"
        # by_card spans settled + provisional RAW events; null bucket first.
        cards = {c["card_id"]: (c["cost"], c["calls"]) for c in d["by_card"]}
        assert cards == {None: ("0.500000", 1), "c1": ("3.000000", 2)}
        assert d["by_card"][0]["card_id"] is None
        assert d["by_card_since"] == yesterday.isoformat()

    async def test_watermark_is_global_not_per_project(self, db_engine, app_client):
        """Another project's rollup row sets the watermark; this project's raw
        events on or before it are not provisional (they await the re-roll)."""
        today = _today()
        yesterday = today - timedelta(days=1)
        await _daily(
            db_engine,
            tenant_id=TENANT_B,
            project_id=OTHER_PROJECT,
            day=yesterday,
            cost="1.0",
            calls=1,
        )
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=PROJECT,
            cost="4.00",
            event_ts=_at(yesterday, 12),
            created_at=_at(yesterday, 12),
        )
        now = datetime.now(UTC)
        await _event(db_engine, tenant_id=TENANT_A, project_id=PROJECT, cost="0.25", created_at=now)
        d = _get(app_client, PROJECT, TENANT_A).json()["data"]
        assert d["settled"] == []
        assert d["provisional"]["since"] == today.isoformat()
        assert Decimal(d["provisional"]["cost"]) == Decimal("0.25")
        assert d["provisional"]["calls"] == 1

    async def test_no_rollup_yet_everything_is_provisional(self, db_engine, app_client):
        now = datetime.now(UTC)
        await _event(db_engine, tenant_id=TENANT_A, project_id=PROJECT, cost="1.5", created_at=now)
        d = _get(app_client, PROJECT, TENANT_A).json()["data"]
        assert d["provisional"]["since"] is None
        assert Decimal(d["provisional"]["cost"]) == Decimal("1.5")

    async def test_tenant_isolation(self, db_engine, app_client):
        """Same project uuid on two tenants: each caller sees only its own spend."""
        today = _today()
        now = datetime.now(UTC)
        await _daily(
            db_engine,
            tenant_id=TENANT_A,
            project_id=PROJECT,
            day=today - timedelta(days=1),
            cost="5.0",
            calls=3,
        )
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=PROJECT,
            cost="2.00",
            card_id="ca",
            created_at=now,
        )
        await _event(
            db_engine,
            tenant_id=TENANT_B,
            project_id=PROJECT,
            cost="0.10",
            card_id="cb",
            created_at=now,
        )

        b = _get(app_client, PROJECT, TENANT_B)
        assert b.status_code == 200
        db = b.json()["data"]
        assert db["tenant_id"] == TENANT_B
        assert db["settled"] == []
        assert Decimal(db["totals"]["cost"]) == Decimal("0.10")
        assert [c["card_id"] for c in db["by_card"]] == ["cb"]

        # A third tenant with nothing sees zeros — never A's or B's figures.
        c = _get(app_client, PROJECT, T("cbp-tenant-c")).json()["data"]
        assert Decimal(c["totals"]["cost"]) == 0
        assert c["by_card"] == []
        assert c["settled"] == []

        a = _get(app_client, PROJECT, TENANT_A).json()["data"]
        assert Decimal(a["totals"]["cost"]) == Decimal("7.00")
        assert [c["card_id"] for c in a["by_card"]] == ["ca"]

    async def test_costs_sum_full_precision_then_round_half_up(self, db_engine, app_client):
        """Two raw events of $0.0000005 each: summed first (= $0.000001), never
        rounded per event (would be $0.000002); a lone half-quantum rounds UP,
        as Postgres does when the rollup casts into Numeric(20, 6)."""
        now = datetime.now(UTC)
        for _ in range(2):
            await _event(
                db_engine,
                tenant_id=TENANT_A,
                project_id=PROJECT,
                cost="0.00000050",
                card_id="c1",
                created_at=now,
            )
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=PROJECT,
            cost="0.00000050",
            card_id="c2",
            created_at=now,
        )
        d = _get(app_client, PROJECT, TENANT_A).json()["data"]
        assert d["provisional"]["cost"] == "0.000002"  # 0.0000015 -> half-up
        assert d["totals"]["cost"] == "0.000002"
        assert d["totals"]["provisional_cost"] == "0.000002"
        assert d["totals"]["settled_cost"] == "0.000000"
        cards = {c["card_id"]: c["cost"] for c in d["by_card"]}
        assert cards == {"c1": "0.000001", "c2": "0.000001"}

    async def test_tenant_own_uuid_as_project_refused(self, db_engine, app_client):
        """rollup_daily stores project_id == tenant_id as NULL_PROJECT_ID, so this
        id would show today's raw spend and lose it from totals once rolled up."""
        today = _today()
        tenant_as_project = uuid.UUID(TENANT_A)
        await _daily(
            db_engine,
            tenant_id=TENANT_A,
            project_id=NULL_PROJECT_ID,  # what the rollup wrote for it
            day=today - timedelta(days=1),
            cost="3.000000",
            calls=1,
        )
        await _event(
            db_engine,
            tenant_id=TENANT_A,
            project_id=tenant_as_project,
            cost="1.00",
            created_at=datetime.now(UTC),
        )
        r = _get(app_client, tenant_as_project, TENANT_A)
        assert r.status_code == 422, r.text
        # Upper-case spelling of the same uuid is the same refusal.
        r = _get(app_client, str(tenant_as_project).upper(), TENANT_A)
        assert r.status_code == 422, r.text
        # Another tenant's uuid is not THIS caller's tenant: ordinary 200-with-zeros.
        r = _get(app_client, tenant_as_project, TENANT_B)
        assert r.status_code == 200, r.text
        assert r.json()["data"]["totals"]["cost"] == "0.000000"

    async def test_empty_project(self, app_client):
        r = _get(app_client, uuid.uuid4(), TENANT_A)
        assert r.status_code == 200
        d = r.json()["data"]
        assert d["settled"] == []
        assert d["by_card"] == []
        assert d["by_card_since"] is None
        assert d["provisional"]["calls"] == 0
        assert Decimal(d["provisional"]["cost"]) == 0
        assert d["totals"]["calls"] == 0
        assert d["totals"]["cost"] == "0.000000"

    async def test_sentinel_project_refused(self, app_client):
        assert _get(app_client, NULL_PROJECT_ID, TENANT_A).status_code == 422

    async def test_invalid_project_id(self, app_client):
        assert _get(app_client, "not-a-uuid", TENANT_A).status_code == 422

    async def test_requires_tenant_context(self, app_client):
        r = app_client.get(f"/api/costs/projects/{PROJECT}", headers={"X-Tenant-Id": ""})
        assert r.status_code == 400
