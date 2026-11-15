"""Tests for T1.5 cost_source attribution.

Covers:
- ``flush_costs`` persists ``cost_source`` from the event payload onto
  ``cost_events.cost_source``.
- Missing ``cost_source`` falls back to ``'unknown'``.
- ``rollup_daily`` groups by ``cost_source``: two distinct sources on the
  same (tenant, license, subject, provider, service_key) day produce two
  ``cost_daily`` rows.
"""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from models.db import (
    CostDaily,
    CostEvent,
)
from services.aggregator import flush_costs, rollup_daily
from tests._aggregator_helpers import _make_cost
from tests.conftest import T


class TestCostSourcePersistence:
    @pytest.mark.unit
    async def test_flush_persists_cost_source_from_event(self, db_session_factory):
        buffer = [
            _make_cost(
                tenant_id=T("t-source"),
                license_id=str(uuid.uuid4()),
                cost_source="inference-router",
            ),
        ]
        count = await flush_costs(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (
                await session.execute(select(CostEvent).where(CostEvent.tenant_id == T("t-source")))
            ).scalar_one()
            assert row.cost_source == "inference-router"

    @pytest.mark.unit
    async def test_flush_defaults_cost_source_to_unknown(self, db_session_factory):
        buffer = [
            _make_cost(
                tenant_id=T("t-source-default"),
                license_id=str(uuid.uuid4()),
            ),
        ]
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("t-source-default"))
                )
            ).scalar_one()
            assert row.cost_source == "unknown"

    @pytest.mark.unit
    async def test_flush_empty_string_cost_source_falls_back_to_unknown(self, db_session_factory):
        buffer = [
            _make_cost(
                tenant_id=T("t-source-empty"),
                license_id=str(uuid.uuid4()),
                cost_source="",
            ),
        ]
        await flush_costs(buffer, db_session_factory, None)

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent).where(CostEvent.tenant_id == T("t-source-empty"))
                )
            ).scalar_one()
            assert row.cost_source == "unknown"


class TestCostDailyGroupsByCostSource:
    @pytest.mark.unit
    async def test_rollup_daily_emits_one_row_per_cost_source(self, db_session_factory):
        yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
        lic = uuid.uuid4()
        sub = uuid.uuid4()
        ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)

        async with db_session_factory() as session:
            session.add_all(
                [
                    CostEvent(
                        tenant_id=T("t-cs"),
                        license_id=lic,
                        subject_id=sub,
                        service_key="inference-router",
                        provider="claude",
                        input_tokens=100,
                        output_tokens=20,
                        cost_usd=Decimal("0.01"),
                        cost_source="inference-router",
                        created_at=ts,
                    ),
                    CostEvent(
                        tenant_id=T("t-cs"),
                        license_id=lic,
                        subject_id=sub,
                        service_key="inference-router",
                        provider="claude",
                        input_tokens=300,
                        output_tokens=40,
                        cost_usd=Decimal("0.04"),
                        cost_source="worker_management",
                        created_at=ts,
                    ),
                ]
            )
            await session.commit()

        await rollup_daily(db_session_factory)

        async with db_session_factory() as session:
            rows = (
                (await session.execute(select(CostDaily).where(CostDaily.tenant_id == T("t-cs"))))
                .scalars()
                .all()
            )
            by_source = {r.cost_source: r for r in rows}
            assert set(by_source) == {"inference-router", "worker_management"}
            assert by_source["inference-router"].total_input_tokens == 100
            assert by_source["inference-router"].total_cost_usd == Decimal("0.010000")
            assert by_source["worker_management"].total_input_tokens == 300
            assert by_source["worker_management"].total_cost_usd == Decimal("0.040000")

    @pytest.mark.unit
    async def test_rollup_daily_aggregates_within_same_cost_source(self, db_session_factory):
        yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
        lic = uuid.uuid4()
        sub = uuid.uuid4()
        ts = datetime.combine(yesterday, datetime.min.time(), tzinfo=UTC)

        async with db_session_factory() as session:
            session.add_all(
                [
                    CostEvent(
                        tenant_id=T("t-cs-agg"),
                        license_id=lic,
                        subject_id=sub,
                        service_key="inference-router",
                        provider="claude",
                        input_tokens=10,
                        output_tokens=2,
                        cost_usd=Decimal("0.001"),
                        cost_source="inference-router",
                        created_at=ts,
                    ),
                    CostEvent(
                        tenant_id=T("t-cs-agg"),
                        license_id=lic,
                        subject_id=sub,
                        service_key="inference-router",
                        provider="claude",
                        input_tokens=20,
                        output_tokens=4,
                        cost_usd=Decimal("0.002"),
                        cost_source="inference-router",
                        created_at=ts,
                    ),
                ]
            )
            await session.commit()

        await rollup_daily(db_session_factory)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(CostDaily).where(CostDaily.tenant_id == T("t-cs-agg"))
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            row = rows[0]
            assert row.cost_source == "inference-router"
            assert row.total_input_tokens == 30
            assert row.total_output_tokens == 6
            assert row.request_count == 2
            assert row.total_cost_usd == Decimal("0.003000")


@pytest.mark.unit
def test_group_key_day_dimension_uses_utc_not_session_local_date():
    """costs.py `_group_key(dim='day')` must bucket by the UTC calendar day.

    psycopg3 returns a timestamptz as a datetime whose tzinfo is the DB session
    TimeZone (prod's pgbouncer session is Europe/Amsterdam), so a bare
    `created_at.date()` would group a 23:30-UTC event under the next Amsterdam
    day. Construct exactly that datetime (01:30 Amsterdam == 23:30 UTC the prior
    day) and assert the grouped day is the UTC day.
    """
    from datetime import timedelta as _td
    from datetime import timezone as _tz

    from models.db import CostEvent
    from routers.costs import _group_key

    amsterdam = _tz(_td(hours=2))  # +02:00, i.e. Europe/Amsterdam in summer
    # 2026-06-16 01:30 +02:00 == 2026-06-15 23:30 UTC.
    created = datetime(2026, 6, 16, 1, 30, tzinfo=amsterdam)
    event = CostEvent(
        tenant_id=T("t"),
        provider="deepseek",
        served_model="deepseek-v4-flash",
        input_tokens=1,
        output_tokens=1,
        cost_usd=Decimal("0"),
        created_at=created,
    )
    key = dict(_group_key(event, ["day"]))
    assert key["day"] == "2026-06-15", (
        "day dimension must be the UTC date, not the +02:00 local date"
    )
