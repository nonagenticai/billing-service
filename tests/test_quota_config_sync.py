"""The periodic request_quota_config value repair (services/quota_config_sync.py).

The defect: a ``quota_override`` set upstream after provisioning never reached
the mirror unless an operator POSTed ``/api/budget/sync-from-licenses``, so
licence 01a0fdb9… enforced 1,000,000 against an override of 10,000.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import cast

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db.quota import RequestQuotaConfig
from services import quota_config_sync as qcs
from services.licence_budget_reconcile import LicenceFetchError
from services.quota import upsert_request_quota_config
from settings import Settings

from .conftest import T

pytestmark = pytest.mark.asyncio


def _lic(
    lic_id: uuid.UUID, *, plan_quota: int | None, override: int | None = None, status="active"
):
    return {
        "id": str(lic_id),
        "tenant_id": T("t1"),
        "status": status,
        "quota_override": override,
        "plan": {"quota_monthly": plan_quota},
    }


def _client(payload, status_code: int = 200) -> httpx.AsyncClient:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=payload)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class _Settings:
    licenses_url = "http://licences.invalid"
    quota_config_sync_interval_seconds = 3600.0
    leader_retry_interval_seconds = 0.001

    def resolve_leader_election_dsn(self) -> str:
        return "postgresql://unused.invalid/none"


def _settings() -> Settings:
    return cast(Settings, _Settings())


async def _seed(session: AsyncSession, lic_id: uuid.UUID, quota: int) -> None:
    await upsert_request_quota_config(session, lic_id, T("t1"), quota)
    await session.commit()


async def _mirrored(session_factory, lic_id: uuid.UUID) -> int | None:
    async with session_factory() as s:
        return (
            await s.execute(
                select(RequestQuotaConfig.quota_monthly).where(
                    RequestQuotaConfig.license_id == lic_id
                )
            )
        ).scalar_one_or_none()


class TestRepairOnce:
    async def test_an_override_set_after_provisioning_reaches_the_mirror(
        self, db_session, db_session_factory
    ):
        stale, agreeing, unmirrored = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, stale, 1_000_000)
        await _seed(db_session, agreeing, 500)
        payload = [
            _lic(stale, plan_quota=1_000_000, override=10_000),
            _lic(agreeing, plan_quota=500),
            _lic(unmirrored, plan_quota=42),
        ]

        repaired = await qcs.repair_quota_config_values_once(
            _settings(), db_session_factory, None, client=_client(payload)
        )

        assert repaired == [stale]
        assert await _mirrored(db_session_factory, stale) == 10_000
        assert await _mirrored(db_session_factory, agreeing) == 500
        # Never creates a row: provisioning owns creation.
        assert await _mirrored(db_session_factory, unmirrored) is None

    async def test_a_second_tick_is_a_no_op(self, db_session, db_session_factory):
        lic = uuid.uuid4()
        await _seed(db_session, lic, 1_000_000)
        payload = [_lic(lic, plan_quota=1_000_000, override=10_000)]
        first = await qcs.repair_quota_config_values_once(
            _settings(), db_session_factory, None, client=_client(payload)
        )
        second = await qcs.repair_quota_config_values_once(
            _settings(), db_session_factory, None, client=_client(payload)
        )
        assert (first, second) == ([lic], [])

    async def test_a_non_active_licence_is_left_alone(self, db_session, db_session_factory):
        revoked, active = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, revoked, 1_000_000)
        await _seed(db_session, active, 7)
        payload = [
            _lic(revoked, plan_quota=1_000_000, override=10, status="revoked"),
            _lic(active, plan_quota=7),
        ]
        assert (
            await qcs.repair_quota_config_values_once(
                _settings(), db_session_factory, None, client=_client(payload)
            )
            == []
        )
        assert await _mirrored(db_session_factory, revoked) == 1_000_000

    async def test_an_incomplete_registry_read_writes_nothing(self, db_session, db_session_factory):
        lic = uuid.uuid4()
        await _seed(db_session, lic, 1_000_000)
        with pytest.raises(LicenceFetchError):
            await qcs.repair_quota_config_values_once(
                _settings(), db_session_factory, None, client=_client({"detail": "x"}, 503)
            )
        assert await _mirrored(db_session_factory, lic) == 1_000_000


class _FakeElection:
    def __init__(self, *_a, leader: bool = True, **_k) -> None:
        self.leader = leader
        self.releases = 0

    async def is_leader(self) -> bool:
        return self.leader

    async def release(self) -> None:
        self.releases += 1


class TestLoop:
    async def _run(self, monkeypatch, *, leader: bool, on_tick) -> list[int]:
        monkeypatch.setattr(
            qcs, "AsyncLeaderElection", lambda *a, **k: _FakeElection(leader=leader)
        )
        stop = asyncio.Event()
        calls: list[int] = []

        async def _fake(*_a, **_k):
            calls.append(1)
            try:
                return await on_tick(len(calls))
            finally:
                if len(calls) >= 2:
                    stop.set()

        monkeypatch.setattr(qcs, "repair_quota_config_values_once", _fake)

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        asyncio.get_running_loop().create_task(_stopper())
        await asyncio.wait_for(
            qcs.run_quota_config_sync_loop(
                _settings(), cast(async_sessionmaker[AsyncSession], None), None, stop
            ),
            timeout=10,
        )
        return calls

    async def test_the_first_tick_runs_at_startup_not_one_interval_later(self, monkeypatch):
        async def _ok(_n):
            return []

        # Interval is 3600s; only an immediate first tick can land inside 0.3s,
        # and the second must NOT (it waits the full interval).
        assert await self._run(monkeypatch, leader=True, on_tick=_ok) == [1]

    async def test_a_follower_never_repairs(self, monkeypatch):
        async def _ok(_n):
            return []

        assert await self._run(monkeypatch, leader=False, on_tick=_ok) == []

    async def test_a_failed_tick_is_counted_and_retried_not_fatal(self, monkeypatch):
        from observability import metrics as m

        before = m.quota_config_value_repair_failures_total.labels(
            reason="licences_unreadable"
        )._value.get()

        async def _fail_then_ok(n):
            if n == 1:
                raise LicenceFetchError("down")
            return []

        # Failure retries after leader_retry_interval_seconds, not the interval.
        assert await self._run(monkeypatch, leader=True, on_tick=_fail_then_ok) == [1, 1]
        after = m.quota_config_value_repair_failures_total.labels(
            reason="licences_unreadable"
        )._value.get()
        assert after == before + 1


@pytest.mark.unit
def test_the_loop_is_armed_by_default_with_a_bounded_interval():
    fields = Settings.model_fields
    assert fields["quota_config_sync_enabled"].default is True
    assert fields["quota_config_sync_interval_seconds"].default == 900.0
