"""A PARTIAL cost gap must be expressible, not rendered as a measured zero.

``/api/usage/*/series`` and ``/api/usage/*/breakdown`` join metered request
counts against ``cost_daily``. Where a cost row is missing they emit
``cost_usd:"0"`` — and the ``cost_unavailable`` flag that exists to mark that
zero as UNKNOWN was set from ``len(cost_rows) == 0``, i.e. only when the window
had NO cost data whatsoever. A window with one priced day and twenty-nine
unpriced ones therefore reported ``cost_unavailable: false`` while rendering
twenty-nine real-looking ``$0.00`` days: an absent measurement served as a
value, which is this fleet's dominant defect class.

The fix makes the gap expressible, never fatal: the request still returns 200
with every bucket it has, the flag is set whenever ANY gap exists, and
``cost_unavailable_days`` / ``cost_unavailable_services`` name exactly which
buckets are unmeasured so a UI can grey those cells instead of the whole column.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tests.conftest import T
from tests.test_api import _seed_cost_daily, _seed_daily_usage


@pytest.mark.unit
class TestSeriesPartialDayGap:
    async def test_a_gap_day_inside_a_priced_window_sets_the_flag_and_is_named(
        self, db_engine, app_client
    ):
        """Two metered days, only one priced. The unpriced day is a GAP."""
        today = datetime.now(UTC).date()
        yesterday = today - timedelta(days=1)
        await _seed_daily_usage(db_engine, tenant_id=T("gap-s1"), count=10, bucket_date=yesterday)
        await _seed_daily_usage(db_engine, tenant_id=T("gap-s1"), count=20, bucket_date=today)
        await _seed_cost_daily(
            db_engine, tenant_id=T("gap-s1"), cost_usd=Decimal("4.00"), bucket_date=today
        )

        resp = app_client.get(
            f"/api/usage/tenant/{T('gap-s1')}/series?window=30d&granularity=daily"
        )
        # The gap is EXPRESSIBLE, not fatal — the request still succeeds and
        # still carries both buckets.
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert len(body["buckets"]) == 2

        assert body["cost_unavailable"] is True, (
            "a window with a priced day and an unpriced day reported cost as fully "
            "available; the unpriced day's cost_usd:'0' reads as a measured zero"
        )
        assert body["cost_unavailable_days"] == [yesterday.isoformat()]

        by_day = {b["ts"][:10]: b for b in body["buckets"]}
        assert Decimal(by_day[today.isoformat()]["cost_usd"]) == Decimal("4.00")
        # The gap day still renders "0" for backwards compatibility — the flag
        # plus the day list are what make it readable as UNKNOWN.
        assert Decimal(by_day[yesterday.isoformat()]["cost_usd"]) == Decimal("0")

    async def test_a_fully_priced_window_reports_cost_available(self, db_engine, app_client):
        """CONTROL: no gaps -> flag stays False and the day list is empty."""
        today = datetime.now(UTC).date()
        yesterday = today - timedelta(days=1)
        for day in (yesterday, today):
            await _seed_daily_usage(db_engine, tenant_id=T("gap-s2"), count=5, bucket_date=day)
            await _seed_cost_daily(
                db_engine, tenant_id=T("gap-s2"), cost_usd=Decimal("1.00"), bucket_date=day
            )

        resp = app_client.get(
            f"/api/usage/tenant/{T('gap-s2')}/series?window=30d&granularity=daily"
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert len(body["buckets"]) == 2
        assert body["cost_unavailable"] is False
        assert body["cost_unavailable_days"] == []

    async def test_a_window_with_no_cost_at_all_still_reports_unavailable(
        self, db_engine, app_client
    ):
        """CONTROL: the pre-existing all-or-nothing case is unchanged."""
        today = datetime.now(UTC).date()
        await _seed_daily_usage(db_engine, tenant_id=T("gap-s3"), count=9, bucket_date=today)

        resp = app_client.get(
            f"/api/usage/tenant/{T('gap-s3')}/series?window=30d&granularity=daily"
        )
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["cost_unavailable"] is True
        assert body["cost_unavailable_days"] == [today.isoformat()]


@pytest.mark.unit
class TestBreakdownPartialServiceGap:
    async def test_a_service_with_requests_and_no_cost_row_is_named_as_a_gap(
        self, db_engine, app_client
    ):
        today = datetime.now(UTC).date()
        # inference-router is what _seed_cost_daily prices; the other two are not.
        await _seed_daily_usage(
            db_engine,
            tenant_id=T("gap-b1"),
            service_key="inference-router",  # pragma: allowlist secret
            count=30,
            bucket_date=today,
        )
        await _seed_daily_usage(
            db_engine, tenant_id=T("gap-b1"), service_key="kag", count=10, bucket_date=today
        )
        await _seed_daily_usage(
            db_engine, tenant_id=T("gap-b1"), service_key="artifact", count=4, bucket_date=today
        )
        await _seed_cost_daily(
            db_engine, tenant_id=T("gap-b1"), cost_usd=Decimal("2.00"), bucket_date=today
        )

        resp = app_client.get(f"/api/usage/tenant/{T('gap-b1')}/breakdown?window=30d")
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert len(body["services"]) == 3
        assert body["cost_unavailable"] is True
        # Sorted, and ONLY the unpriced ones.
        assert body["cost_unavailable_services"] == ["artifact", "kag"]

    async def test_every_service_priced_reports_cost_available(self, db_engine, app_client):
        """CONTROL: no gap service -> flag False, list empty."""
        today = datetime.now(UTC).date()
        await _seed_daily_usage(
            db_engine,
            tenant_id=T("gap-b2"),
            service_key="inference-router",  # pragma: allowlist secret
            count=30,
            bucket_date=today,
        )
        await _seed_cost_daily(
            db_engine, tenant_id=T("gap-b2"), cost_usd=Decimal("2.00"), bucket_date=today
        )

        resp = app_client.get(f"/api/usage/tenant/{T('gap-b2')}/breakdown?window=30d")
        assert resp.status_code == 200
        body = resp.json()["data"]
        assert body["cost_unavailable"] is False
        assert body["cost_unavailable_services"] == []
