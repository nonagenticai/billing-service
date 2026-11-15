"""Tests for denied event rollup persistence with sentinel UUIDs."""

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import (
    NULL_APP_ID,
    NULL_LICENSE_ID,
    NULL_PLAN_ID,
    MeteringDaily,
    MeteringEvent,
    MeteringHourly,
)
from services.aggregator import flush_events, rollup_daily, rollup_hourly
from tests.conftest import T


def _previous_hour_ts() -> datetime:
    """Return a timestamp inside the previous-hour rollup window."""
    now = datetime.now(UTC)
    bucket = now.replace(minute=0, second=0, microsecond=0)
    return bucket - timedelta(minutes=30)  # middle of previous hour


async def _backdate_events(
    session_factory: async_sessionmaker[AsyncSession],
    tenant_id: str,
) -> None:
    """Move all events for *tenant_id* into the previous-hour window so rollup_hourly picks them up."""
    ts = _previous_hour_ts()
    async with session_factory() as session:
        stmt = (
            update(MeteringEvent).where(MeteringEvent.tenant_id == tenant_id).values(created_at=ts)
        )
        await session.execute(stmt)
        await session.commit()


class TestDeniedEventRollup:
    async def test_denied_events_survive_hourly_rollup(
        self, db_session_factory: async_sessionmaker[AsyncSession]
    ):
        """Events with NULL IDs must appear in metering_hourly with sentinel UUIDs."""
        events: list[dict] = [
            {
                "tenant_id": T("tenant-denied-test"),
                "license_id": "",
                "app_id": "",
                "plan_id": "",
                "method": "GET",
                "path": "/workers/api/tasks",
                "status": "denied_missing_token",
                "source_ip": "1.2.3.4",
            }
        ]
        flushed = await flush_events(events, db_session_factory, None)
        assert flushed == 1

        await _backdate_events(db_session_factory, T("tenant-denied-test"))

        count = await rollup_hourly(db_session_factory)
        assert count >= 1

        async with db_session_factory() as session:
            stmt = select(MeteringHourly).where(MeteringHourly.tenant_id == T("tenant-denied-test"))
            result = await session.execute(stmt)
            rows = result.scalars().all()
            assert len(rows) == 1
            row = rows[0]
            assert row.license_id == NULL_LICENSE_ID
            assert row.app_id == NULL_APP_ID
            assert row.plan_id == NULL_PLAN_ID
            assert row.denied_count == 1
            # OVER-BILLING GUARD: a REFUSED request must not enter the billed
            # request_count. generate_billing multiplies request_count by the
            # per-app AppCostRate and feeds it to the per-request surcharge, so
            # counting a denial here charged the tenant for service it was
            # refused. Billing is append-only — there is no refund path.
            assert row.request_count == 0

    async def test_allowed_events_keep_real_ids(
        self, db_session_factory: async_sessionmaker[AsyncSession]
    ):
        """Events with real IDs must keep them in rollup (not replaced by sentinels)."""
        real_lic = uuid.uuid4()
        real_app = uuid.uuid4()
        real_plan = uuid.uuid4()
        events: list[dict] = [
            {
                "tenant_id": T("tenant-real-test"),
                "license_id": str(real_lic),
                "app_id": str(real_app),
                "plan_id": str(real_plan),
                "method": "POST",
                "path": "/workers/api/tasks",
                "status": "allowed",
                "source_ip": "1.2.3.5",
            }
        ]
        await flush_events(events, db_session_factory, None)
        await _backdate_events(db_session_factory, T("tenant-real-test"))
        await rollup_hourly(db_session_factory)

        async with db_session_factory() as session:
            stmt = select(MeteringHourly).where(MeteringHourly.tenant_id == T("tenant-real-test"))
            result = await session.execute(stmt)
            rows = result.scalars().all()
            assert len(rows) == 1
            row = rows[0]
            assert row.license_id == real_lic
            assert row.app_id == real_app
            assert row.plan_id == real_plan
            assert row.request_count == 1
            assert row.denied_count == 0

    async def test_mixed_events_produce_separate_rollup_rows(
        self, db_session_factory: async_sessionmaker[AsyncSession]
    ):
        """Both allowed+denied events roll up into separate groups."""
        lic = uuid.uuid4()
        app = uuid.uuid4()
        plan = uuid.uuid4()
        events: list[dict] = [
            {
                "tenant_id": T("tenant-mix-test"),
                "license_id": str(lic),
                "app_id": str(app),
                "plan_id": str(plan),
                "method": "GET",
                "path": "/workers/api/tasks",
                "status": "allowed",
                "source_ip": "1.2.3.5",
            },
            {
                "tenant_id": T("tenant-mix-test"),
                "license_id": "",
                "app_id": "",
                "plan_id": "",
                "method": "GET",
                "path": "/workers/api/tasks",
                "status": "denied_missing_token",
                "source_ip": "1.2.3.6",
            },
        ]
        await flush_events(events, db_session_factory, None)
        await _backdate_events(db_session_factory, T("tenant-mix-test"))
        count = await rollup_hourly(db_session_factory)
        assert count == 2


class TestDeniedNotBilled:
    """`request_count` is MONEY — it is multiplied by the per-app AppCostRate and
    fed to the per-request surcharge in ``services.billing.generate_billing``.

    Denied requests must not enter it. The trap on the other side: `status` has
    TWO writers — license-authz writes the `allowed`/`denied…` decision
    vocabulary, and fr writes raw HTTP codes (`200`, `201`, `404`, `422`). An
    HTTP-code row is an ordinary BILLABLE request, so the billable count is
    "NOT denied", never "== 'allowed'". Billing is append-only: an over-charge
    and an under-charge are both unrecoverable.
    """

    @staticmethod
    def _event(tenant: str, status: str, ip: str, *, ids: tuple[str, str, str]) -> dict:
        return {
            "tenant_id": tenant,
            "license_id": ids[0],
            "app_id": ids[1],
            "plan_id": ids[2],
            "method": "GET",
            "path": "/workers/api/tasks",
            "status": status,
            "source_ip": ip,
        }

    async def test_denied_flavours_are_excluded_but_http_codes_still_bill(
        self, db_session_factory: async_sessionmaker[AsyncSession]
    ):
        """One rollup group: 3 denied flavours excluded, `allowed` + HTTP codes billed."""
        tenant = T("tenant-denied-not-billed")
        ids = (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()))
        statuses = [
            "denied_missing_token",  # excluded
            "denied_budget",  # excluded
            "denied",  # excluded (bare form, no underscore)
            "allowed",  # billed
            "200",  # billed — fr's HTTP-code writer
            "422",  # billed — a 4xx is still a served request
        ]
        events = [self._event(tenant, s, f"9.9.9.{i}", ids=ids) for i, s in enumerate(statuses)]
        assert await flush_events(events, db_session_factory, None) == len(statuses)

        await _backdate_events(db_session_factory, tenant)
        assert await rollup_hourly(db_session_factory) >= 1

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringHourly).where(MeteringHourly.tenant_id == tenant)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1
        row = rows[0]
        # THE FIX: 3 denials do not reach the billed count.
        assert row.request_count == 3, "denied requests must not be billed"
        assert row.denied_count == 3
        assert row.budget_denied_count == 1
        # THE CONTROL, in the other direction: the legitimately-billable
        # classes are all still there. request_count + denied_count accounts
        # for every row in the window — nothing was silently dropped.
        assert row.request_count + row.denied_count == len(statuses)

    async def test_a_purely_denied_group_bills_zero_requests(
        self, db_session_factory: async_sessionmaker[AsyncSession]
    ):
        """A tenant that was refused every time owes nothing on the request axis."""
        tenant = T("tenant-all-denied")
        ids = (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()))
        events = [
            self._event(tenant, "denied_app_mismatch", f"8.8.8.{i}", ids=ids) for i in range(4)
        ]
        assert await flush_events(events, db_session_factory, None) == 4

        await _backdate_events(db_session_factory, tenant)
        await rollup_hourly(db_session_factory)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringHourly).where(MeteringHourly.tenant_id == tenant)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1
        assert rows[0].request_count == 0
        assert rows[0].denied_count == 4

    async def test_daily_rollup_inherits_the_billed_count(
        self, db_session_factory: async_sessionmaker[AsyncSession]
    ):
        """metering_daily is what generate_billing reads — prove the fix reaches it."""
        tenant = T("tenant-denied-daily")
        ids = (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()))
        events = [
            self._event(tenant, "allowed", "7.7.7.1", ids=ids),
            self._event(tenant, "denied_budget", "7.7.7.2", ids=ids),
            self._event(tenant, "denied_quota", "7.7.7.3", ids=ids),
        ]
        await flush_events(events, db_session_factory, None)
        await _backdate_events(db_session_factory, tenant)
        await rollup_hourly(db_session_factory)

        # The previous hour may belong to yesterday (00:xx UTC); roll up the
        # day that hour actually landed in.
        target = _previous_hour_ts().date()
        await rollup_daily(db_session_factory, target)

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringDaily).where(MeteringDaily.tenant_id == tenant)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1
        assert rows[0].request_count == 1, "denied must not reach the invoice's request axis"
        assert rows[0].denied_count == 2
