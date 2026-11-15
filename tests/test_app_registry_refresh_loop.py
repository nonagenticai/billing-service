"""The periodic app_registry refresh (``services.app_registry.run_app_registry_refresh_loop``).

Before it, ``app_registry`` moved only when ``POST /api/budget/sync-from-licenses``
was called -- fired by two n8n webhooks and nothing else -- so apps registered
in licences after the last call (live 2026-10-24: ``agent``, ``care-bus``) were
absent from the X-App-Id key map and their router spend stored ``app_id`` NULL.

Every assertion reads rows back out of Postgres. Mutations that turn this red:
drop the commit (the upsert test finds no row); let a failed fetch reach
``apply_registry_apps`` with an empty listing or delete on failure (the failure
test loses its row); let a raising tick escape (the raise test's task dies);
wait the interval before the first tick or ignore ``stop_event`` while waiting
(the stop test times out); ignore ``app_registry_refresh_seconds == 0`` (the
disabled test sees a tick); drop the lifespan wiring (the wiring test sees no
call); touch ``request_quota_config`` (the upsert test's count changes);
drop the ``pg_advisory_xact_lock`` (the serialisation test sees two applies
overlap); fetch inside the session (the fetch-outside test sees an open
session); reset the key-map cache before the commit (the reset tests read the
pre-commit table); let the loop remove unlisted rows (the upsert-only test
loses its row).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import func, select

import services.app_registry as ar
from models.db import AppRegistry, RequestQuotaConfig


@pytest.fixture(autouse=True)
def _fresh_resolver_caches():
    from services.license_resolver import reset_cache

    reset_cache()
    yield
    reset_cache()


def _resp(status: int, payload) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload
    return r


def _apps_client(*, status: int = 200, payload=None) -> AsyncMock:
    client = AsyncMock()
    client.get.return_value = _resp(status, payload)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


def _app(key: str, app_id: uuid.UUID | None = None) -> dict:
    return {"id": str(app_id or uuid.uuid4()), "key": key, "name": key, "revoked": False}


async def _rows(db_session_factory) -> dict[str, uuid.UUID]:
    async with db_session_factory() as session:
        result = await session.execute(select(AppRegistry.app_key, AppRegistry.app_id))
        return dict(result.all())


async def _seed(db_session_factory, **rows: uuid.UUID) -> None:
    async with db_session_factory() as session:
        for key, app_id in rows.items():
            session.add(AppRegistry(app_id=app_id, app_key=key))
        await session.commit()


def _settings(test_settings, seconds: int):
    test_settings.app_registry_refresh_seconds = seconds
    return test_settings


def _counter(result: str) -> float:
    from observability import metrics as om

    return om.app_registry_refresh_total.labels(result=result)._value.get()


async def _wait_until(predicate, *, within: float = 5.0) -> None:
    for _ in range(int(within / 0.02)):
        if await predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not reached before the deadline")


async def _stop(task: asyncio.Task, stop_event: asyncio.Event) -> None:
    stop_event.set()
    await asyncio.wait_for(task, timeout=2)


@pytest.mark.unit
async def test_the_loop_upserts_a_newly_registered_key(db_session_factory, test_settings):
    existing = uuid.uuid4()
    await _seed(db_session_factory, **{"core-app": existing})
    async with db_session_factory() as session:
        quota_before = (
            await session.execute(select(func.count()).select_from(RequestQuotaConfig))
        ).scalar_one()
    agent_id = uuid.uuid4()
    client = _apps_client(payload=[_app("core-app", existing), _app("agent", agent_id)])
    ok_before = _counter("ok")
    stop_event = asyncio.Event()

    with patch("services.app_registry.httpx.AsyncClient", return_value=client):
        task = asyncio.create_task(
            ar.run_app_registry_refresh_loop(
                _settings(test_settings, 3600), db_session_factory, None, stop_event
            )
        )

        async def _agent_landed() -> bool:
            return "agent" in await _rows(db_session_factory)

        await _wait_until(_agent_landed)
        await _stop(task, stop_event)

    assert await _rows(db_session_factory) == {"core-app": existing, "agent": agent_id}
    assert client.get.call_args.kwargs["params"] == {"include_revoked": "true"}
    assert _counter("ok") == ok_before + 1
    async with db_session_factory() as session:
        quota_after = (
            await session.execute(select(func.count()).select_from(RequestQuotaConfig))
        ).scalar_one()
    assert quota_after == quota_before, "the refresh must never write request_quota_config"


@pytest.mark.unit
async def test_a_failing_fetch_keeps_the_rows_and_the_loop_keeps_running(
    db_session_factory, test_settings, monkeypatch
):
    kept = uuid.uuid4()
    await _seed(db_session_factory, survivor=kept)
    monkeypatch.setattr(ar, "_MIN_REFRESH_INTERVAL_SECONDS", 0.01)
    client = _apps_client(status=503)
    failed_before = _counter("failed")
    stop_event = asyncio.Event()

    with patch("services.app_registry.httpx.AsyncClient", return_value=client):
        # app_registry_refresh_seconds is an int; 1s with the floor lowered.
        task = asyncio.create_task(
            ar.run_app_registry_refresh_loop(
                _settings(test_settings, 1), db_session_factory, None, stop_event
            )
        )

        async def _two_ticks() -> bool:
            return client.get.await_count >= 2

        await _wait_until(_two_ticks)
        assert not task.done(), "a failed fetch ended the loop"
        await _stop(task, stop_event)

    assert await _rows(db_session_factory) == {"survivor": kept}
    assert _counter("failed") >= failed_before + 2


@pytest.mark.unit
async def test_a_raising_tick_is_counted_and_does_not_kill_the_loop(
    db_session_factory, test_settings, monkeypatch
):
    monkeypatch.setattr(ar, "_MIN_REFRESH_INTERVAL_SECONDS", 0.01)
    provider = MagicMock()
    provider.get_token = AsyncMock(side_effect=RuntimeError("token endpoint exploded"))
    error_before = _counter("error")
    stop_event = asyncio.Event()

    task = asyncio.create_task(
        ar.run_app_registry_refresh_loop(
            _settings(test_settings, 1), db_session_factory, provider, stop_event
        )
    )

    async def _two_ticks() -> bool:
        return provider.get_token.await_count >= 2

    await _wait_until(_two_ticks)
    assert not task.done(), "a raising tick ended the loop"
    await _stop(task, stop_event)
    assert _counter("error") >= error_before + 2


@pytest.mark.unit
async def test_stop_event_ends_the_loop_during_a_long_wait(test_settings, monkeypatch):
    ticks: list[int] = []

    async def _fake_once(_sf, _settings, _provider):  # noqa: ANN001, ANN202
        ticks.append(1)
        return ar.AppRegistrySyncResult()

    monkeypatch.setattr(ar, "refresh_app_registry_once", _fake_once)
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        ar.run_app_registry_refresh_loop(
            _settings(test_settings, 3600), MagicMock(), None, stop_event
        )
    )

    async def _first_tick() -> bool:
        return bool(ticks)

    # First tick is immediate, not after the 3600s interval.
    await _wait_until(_first_tick, within=2)
    await _stop(task, stop_event)
    assert ticks == [1]


@pytest.mark.unit
async def test_interval_zero_disables_the_loop(test_settings, monkeypatch):
    called = AsyncMock()
    monkeypatch.setattr(ar, "refresh_app_registry_once", called)
    await asyncio.wait_for(
        ar.run_app_registry_refresh_loop(
            _settings(test_settings, 0), MagicMock(), None, asyncio.Event()
        ),
        timeout=1,
    )
    called.assert_not_awaited()


async def _lifespan_calls(settings, fake_redis, db_session_factory, monkeypatch) -> list[tuple]:
    import main as main_module

    calls: list[tuple] = []

    async def _fake_loop(s, session_factory, provider, stop_event):  # noqa: ANN001, ANN202
        calls.append((s, session_factory, provider))
        await stop_event.wait()

    async def _fake_tenant_loop(_s, _provider, stop_event):  # noqa: ANN001, ANN202
        await stop_event.wait()

    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    monkeypatch.setattr(ar, "run_app_registry_refresh_loop", _fake_loop)
    with (
        # lifespan's configure_logging mutates process-global structlog/stdlib
        # state that later capsys-based tests assert on (test_budget_stacked,
        # test_pricing); this test is about task wiring, not logging.
        patch("main.configure_logging"),
        # Likewise the tenant overlay loop + demand hook it starts: process-global
        # tenancy state that test_cost_flusher's slug-resolution tests read.
        patch("services.tenant_names.run_tenant_registry_refresh_loop", new=_fake_tenant_loop),
        patch("services.tenant_names.install_demand_refresh_hook"),
        patch("main.init_redis", new_callable=AsyncMock, return_value=fake_redis),
        patch("main.init_db", new_callable=AsyncMock),
        patch("main.dispose_engine", new_callable=AsyncMock),
        patch("main.close_redis", new_callable=AsyncMock),
        patch("db.get_session_factory", return_value=db_session_factory),
    ):
        async with main_module.lifespan(main_module.app):
            await asyncio.sleep(0)
    return calls


@pytest.mark.unit
async def test_lifespan_starts_the_loop_with_the_session_factory(
    test_settings, fake_redis, db_session_factory, monkeypatch
):
    calls = await _lifespan_calls(
        _settings(test_settings, 300), fake_redis, db_session_factory, monkeypatch
    )
    assert len(calls) == 1
    assert calls[0][1] is db_session_factory


@pytest.mark.unit
async def test_lifespan_does_not_start_the_loop_when_disabled(
    test_settings, fake_redis, db_session_factory, monkeypatch
):
    calls = await _lifespan_calls(
        _settings(test_settings, 0), fake_redis, db_session_factory, monkeypatch
    )
    assert calls == []


def _committed_keys() -> set[str]:
    """``app_registry`` keys as seen by a SEPARATE connection, i.e. committed rows only."""
    import psycopg

    url = os.environ["DATABASE_URL"].replace("+psycopg", "")
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute("SELECT app_key FROM app_registry")
        return {k for (k,) in cur.fetchall()}


def _parsed(*apps: dict) -> ar.RegistryApps:
    return ar.parse_registry_apps(list(apps))


@pytest.mark.unit
async def test_the_loop_is_upsert_only_and_never_removes_an_unlisted_app(
    db_session_factory, test_settings
):
    """A valid-but-truncated listing must not wipe the mirror every interval.

    The endpoint path still removes unlisted rows -- see
    tests/test_app_registry.py::test_sync_upserts_registration_only_apps_and_removes_unlisted.
    """
    legacy = uuid.uuid4()
    await _seed(db_session_factory, legacy=legacy)
    agent_id = uuid.uuid4()
    client = _apps_client(payload=[_app("agent", agent_id)])

    with patch("services.app_registry.httpx.AsyncClient", return_value=client):
        result = await ar.refresh_app_registry_once(db_session_factory, test_settings, None)

    assert result.error is None
    assert result.removed == 0
    assert await _rows(db_session_factory) == {"legacy": legacy, "agent": agent_id}


@pytest.mark.unit
async def test_the_fetch_runs_before_any_session_or_transaction_is_opened(
    db_session_factory, test_settings, monkeypatch
):
    opened: list[int] = []

    def _tracking_factory():
        opened.append(1)
        return db_session_factory()

    async def _fetch(_settings, _provider):  # noqa: ANN001, ANN202
        assert opened == [], "the licences fetch ran with a DB session already open"
        return _parsed(_app("agent")), None

    monkeypatch.setattr(ar, "fetch_registry_apps", _fetch)
    result = await ar.refresh_app_registry_once(_tracking_factory, test_settings, None)

    assert result.error is None
    assert opened == [1], "the apply must run in exactly one session"
    assert "agent" in await _rows(db_session_factory)


@pytest.mark.unit
async def test_concurrent_ticks_are_serialised_by_the_advisory_lock(
    db_session_factory, test_settings, monkeypatch
):
    """Two replicas' applies must never interleave; removing the lock makes them overlap."""
    events: list[str] = []
    real_apply = ar.apply_registry_apps

    async def _fetch(_settings, _provider):  # noqa: ANN001, ANN202
        return _parsed(_app("core-app", uuid.UUID(int=1)), _app("agent", uuid.UUID(int=2))), None

    async def _slow_apply(session, parsed, **kwargs):  # noqa: ANN001, ANN202
        events.append("enter")
        await asyncio.sleep(0.2)
        result = await real_apply(session, parsed, **kwargs)
        events.append("exit")
        return result

    monkeypatch.setattr(ar, "fetch_registry_apps", _fetch)
    monkeypatch.setattr(ar, "apply_registry_apps", _slow_apply)

    results = await asyncio.gather(
        ar.refresh_app_registry_once(db_session_factory, test_settings, None),
        ar.refresh_app_registry_once(db_session_factory, test_settings, None),
    )

    assert events == ["enter", "exit", "enter", "exit"], (
        f"the two applies overlapped ({events}): the advisory lock is not taken "
        "in the apply's transaction"
    )
    assert all(r.error is None for r in results)
    assert set(await _rows(db_session_factory)) == {"core-app", "agent"}


@pytest.mark.unit
async def test_the_loop_resets_the_key_map_cache_only_after_its_commit(
    db_session_factory, test_settings, monkeypatch
):
    seen_at_reset: list[set[str]] = []

    async def _fetch(_settings, _provider):  # noqa: ANN001, ANN202
        return _parsed(_app("agent")), None

    monkeypatch.setattr(ar, "fetch_registry_apps", _fetch)
    monkeypatch.setattr(
        "services.license_resolver.reset_cache", lambda: seen_at_reset.append(_committed_keys())
    )
    await ar.refresh_app_registry_once(db_session_factory, test_settings, None)

    assert seen_at_reset == [{"agent"}], (
        "reset_cache ran before the new row was committed: a flusher could cache "
        "the pre-commit map for the full TTL"
    )


@pytest.mark.unit
def test_the_endpoint_resets_the_key_map_cache_only_after_its_commit(app_client, monkeypatch):
    import redis_client
    from tests.test_app_registry import _app as _endpoint_app
    from tests.test_app_registry import _sync
    from tests.test_budget_license_sync import _make_stub_redis

    monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
    seen_at_reset: list[set[str]] = []
    monkeypatch.setattr(
        "services.license_resolver.reset_cache", lambda: seen_at_reset.append(_committed_keys())
    )

    r = _sync(app_client, [_endpoint_app("endpoint-app")])

    assert r.status_code == 200, r.text
    assert seen_at_reset == [{"endpoint-app"}]
