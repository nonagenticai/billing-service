"""X-App-Id keys resolve through the licences APP registry, not the licences.

#258 built the key -> app map from ``request_quota_config.app_key``, which only
holds apps that own a LICENCE. Registration-only platform apps (``core-app``,
``chat-bridge*`` -- seeded by licences' ``scripts/seed_apps.py``, no licence)
were therefore never in the map, and their router traffic stored ``app_id``
NULL. Migration 0050 mirrors the whole registry (``GET /internal/apps``) into
``app_registry`` and the map reads that.

Every assertion reads the row back out of Postgres. Mutations that turn this
red: read the map from ``request_quota_config`` only (the registration-only
test stores NULL); drop the empty-registry fallback (the fallback test stores
NULL); drop the absent-row DELETE (the removal test still finds the app); drop
the invalid-row guard on removal (the invalid test loses its row); stop birthing
in ``_build_app_key_map`` (the birth test reads ``None``).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from prometheus_client import REGISTRY
from sqlalchemy import select

import observability.metrics as om
from models.db import AppRegistry, CostEvent, RequestQuotaConfig
from services.aggregator import flush_costs
from services.app_registry import parse_registry_apps
from tests._aggregator_helpers import _make_cost
from tests.conftest import T, throwaway_db_name
from tests.test_budget_license_sync import _make_stub_redis

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _fresh_resolver_caches():
    """The key map is process-global with a 300s TTL -- never inherit one."""
    from services.license_resolver import reset_cache

    reset_cache()
    yield
    reset_cache()


async def _register(db_session_factory, *, app_id: uuid.UUID, app_key: str) -> None:
    async with db_session_factory() as session:
        session.add(AppRegistry(app_id=app_id, app_key=app_key))
        await session.commit()


async def _stored(db_session_factory, tenant_id: str) -> CostEvent:
    async with db_session_factory() as session:
        rows = (
            (await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant_id)))
            .scalars()
            .all()
        )
    assert len(rows) == 1, f"expected exactly one stored event, got {len(rows)}"
    return rows[0]


def _resolved(app_key: str, stream: str = "cost") -> float | None:
    return REGISTRY.get_sample_value(
        "billing_app_key_resolved_total", {"stream": stream, "app_key": app_key}
    )


def _unknown() -> float:
    return om.app_key_unknown_total.labels(stream="cost")._value.get()


def _fallbacks() -> float:
    return om.app_key_map_fallback_total._value.get()


@pytest.mark.unit
class TestKeyMapReadsTheAppRegistry:
    async def test_a_registration_only_app_key_resolves(self, db_session_factory):
        """THE DEFECT: an app with NO licence (no request_quota_config row at all)."""
        tenant = T("app-registry-core-app")
        router_lic, router_app = uuid.uuid4(), uuid.uuid4()
        core_app = uuid.uuid4()
        key = f"core-app-{uuid.uuid4().hex[:8]}"
        # The event's licence is the ROUTER's; the caller holds no licence at all.
        async with db_session_factory() as session:
            session.add(
                RequestQuotaConfig(
                    license_id=router_lic,
                    tenant_id=tenant,
                    quota_monthly=1_000_000,
                    app_id=router_app,
                    app_name="Inference Router",
                    app_key="inference-router",
                )
            )
            await session.commit()
        await _register(db_session_factory, app_id=core_app, app_key=key)
        await _register(db_session_factory, app_id=router_app, app_key="inference-router")

        await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(router_lic), app_id=key)],
            db_session_factory,
            None,
        )

        row = await _stored(db_session_factory, tenant)
        assert str(row.app_id) == str(core_app)
        assert str(row.license_id) == str(router_lic)
        assert _resolved(key) == 1

    async def test_an_unknown_key_stays_null_and_is_counted(self, db_session_factory):
        """Unchanged semantics with a populated registry: NULL, counted, never licence-guessed."""
        tenant = T("app-registry-unknown")
        lic, licence_app = uuid.uuid4(), uuid.uuid4()
        async with db_session_factory() as session:
            session.add(
                RequestQuotaConfig(
                    license_id=lic,
                    tenant_id=tenant,
                    quota_monthly=1_000_000,
                    app_id=licence_app,
                    app_name="Licensed",
                    app_key="licensed-key",
                )
            )
            await session.commit()
        await _register(db_session_factory, app_id=licence_app, app_key="licensed-key")
        before = _unknown()

        await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="not-registered")],
            db_session_factory,
            None,
        )

        row = await _stored(db_session_factory, tenant)
        assert row.app_id is None
        assert _unknown() - before == 1

    async def test_registry_wins_and_the_licence_mirror_is_not_consulted(self, db_session_factory):
        """A non-empty registry is the ONLY source: a licensed-only key is not in it."""
        from services.license_resolver import get_app_key_map

        async with db_session_factory() as session:
            session.add(
                RequestQuotaConfig(
                    license_id=uuid.uuid4(),
                    tenant_id=T("app-registry-only-source"),
                    quota_monthly=1,
                    app_id=uuid.uuid4(),
                    app_name="x",
                    app_key="only-in-licence-mirror",
                )
            )
            await session.commit()
        registered = uuid.uuid4()
        await _register(db_session_factory, app_id=registered, app_key="in-registry")
        before = _fallbacks()

        async with db_session_factory() as session:
            key_map = await get_app_key_map(session)

        assert key_map == {"in-registry": registered}
        assert _fallbacks() - before == 0

    async def test_an_empty_registry_falls_back_to_the_licence_mirror_and_counts_it(
        self, db_session_factory
    ):
        from services.license_resolver import get_app_key_map

        lic_app = uuid.uuid4()
        async with db_session_factory() as session:
            session.add(
                RequestQuotaConfig(
                    license_id=uuid.uuid4(),
                    tenant_id=T("app-registry-fallback"),
                    quota_monthly=1,
                    app_id=lic_app,
                    app_name="x",
                    app_key="licensed-fallback",
                )
            )
            await session.commit()
        before = _fallbacks()

        async with db_session_factory() as session:
            key_map = await get_app_key_map(session)

        assert key_map == {"licensed-fallback": lic_app}
        assert _fallbacks() - before == 1

    async def test_building_the_map_births_the_resolved_series(self, db_session_factory):
        from services.license_resolver import get_app_key_map

        key = f"born-{uuid.uuid4().hex[:8]}"
        await _register(db_session_factory, app_id=uuid.uuid4(), app_key=key)
        assert _resolved(key) is None

        async with db_session_factory() as session:
            await get_app_key_map(session)

        assert _resolved(key, "cost") == 0
        assert _resolved(key, "metering") == 0


# ---------------------------------------------------------------------------
# sync-from-licenses mirrors GET /internal/apps
# ---------------------------------------------------------------------------


def _resp(status: int, payload) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload
    return r


def _licences_and_apps_client(apps_payload, *, apps_status: int = 200) -> AsyncMock:
    """One mocked client answering /internal/licenses with [] and /internal/apps with the payload."""
    client = AsyncMock()

    async def _get(url, *args, **kwargs):
        if url.endswith("/internal/apps"):
            assert kwargs.get("params") == {"include_revoked": "true"}
            return _resp(apps_status, apps_payload)
        return _resp(200, [])

    client.get.side_effect = _get
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


def _registry_rows() -> dict[str, tuple[str, bool]]:
    import psycopg

    url = os.environ["DATABASE_URL"].replace("+psycopg", "")
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute("SELECT app_key, app_id::text, revoked FROM app_registry")
        return {k: (i, r) for k, i, r in cur.fetchall()}


def _sync(app_client, apps_payload, *, apps_status: int = 200):
    with patch(
        "routers.budget.httpx.AsyncClient",
        return_value=_licences_and_apps_client(apps_payload, apps_status=apps_status),
    ):
        return app_client.post("/api/budget/sync-from-licenses")


def _app(key: str, *, revoked: bool = False, app_id: str | None = None) -> dict:
    return {
        "id": app_id or str(uuid.uuid4()),
        "key": key,
        "name": key,
        "revoked": revoked,
        "audience": "platform",
    }


@pytest.mark.unit
def test_sync_upserts_registration_only_apps_and_removes_unlisted(app_client, monkeypatch):
    import redis_client

    monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
    core, bridge, gone = _app("core-app"), _app("chat-bridge-rmt"), _app("retired-app")

    r = _sync(app_client, [core, bridge, gone])
    assert r.status_code == 200, r.text
    assert r.json()["apps_synced"] == 3
    assert r.json()["app_registry_error"] is None
    assert _registry_rows() == {
        "core-app": (core["id"], False),
        "chat-bridge-rmt": (bridge["id"], False),
        "retired-app": (gone["id"], False),
    }

    # Next run: retired-app no longer listed, chat-bridge-rmt revoked (kept, flagged),
    # and core-app's key renamed (same id) -- must not trip UNIQUE(app_key).
    renamed = dict(core, key="core-app-v2")
    r = _sync(app_client, [renamed, dict(bridge, revoked=True)])
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["apps_synced"], body["apps_removed"], body["apps_revoked"]) == (2, 1, 1)
    assert _registry_rows() == {
        "core-app-v2": (core["id"], False),
        "chat-bridge-rmt": (bridge["id"], True),
    }


@pytest.mark.unit
def test_sync_with_an_invalid_row_upserts_but_does_not_remove(app_client, monkeypatch):
    """Absence cannot be proven from a listing we could not fully read."""
    import redis_client

    monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
    keep = _app("kept-app")
    assert _sync(app_client, [keep]).status_code == 200

    r = _sync(app_client, [_app("new-app"), {"id": "not-a-uuid", "key": "broken"}])
    assert r.status_code == 200, r.text
    assert r.json()["apps_invalid"] == 1
    assert r.json()["apps_removed"] == 0
    assert set(_registry_rows()) == {"kept-app", "new-app"}


@pytest.mark.unit
def test_sync_reports_an_apps_fetch_failure_and_leaves_the_registry(app_client, monkeypatch):
    """licences without /internal/apps (404) must not 502 the licence sync or wipe the mirror."""
    import redis_client

    monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
    assert _sync(app_client, [_app("survivor")]).status_code == 200

    r = _sync(app_client, None, apps_status=404)
    assert r.status_code == 200, r.text
    assert r.json()["app_registry_error"] == "licenses /internal/apps returned 404"
    assert set(_registry_rows()) == {"survivor"}


@pytest.mark.unit
def test_parse_drops_both_rows_of_a_duplicated_key():
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    parsed = parse_registry_apps(
        [{"id": a, "key": "dup"}, {"id": b, "key": "dup"}, {"id": str(uuid.uuid4()), "key": "ok"}]
    )
    assert [k for k, _r in parsed.apps.values()] == ["ok"]
    assert parsed.invalid_count == 2


# ---------------------------------------------------------------------------
# migration 0050
# ---------------------------------------------------------------------------

_DB = throwaway_db_name("_billing_0050_check")


def _cfg(url: sa.engine.URL) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
    return cfg


@pytest.mark.integration
async def test_0050_is_drift_tolerant_when_the_table_already_exists() -> None:
    """The pod's ADDITIVE_MIGRATIONS replay (or create_all) can create app_registry first."""
    from db import ADDITIVE_MIGRATIONS

    ddl = [s for s in ADDITIVE_MIGRATIONS if "CREATE TABLE IF NOT EXISTS app_registry" in s]
    assert len(ddl) == 1
    dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    assert dsn
    server = sa.make_url(dsn).set(database="postgres")
    admin = sa.create_engine(server, isolation_level="AUTOCOMMIT", future=True)
    with admin.connect() as conn:
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        conn.execute(sa.text(f'CREATE DATABASE "{_DB}"'))
    url = server.set(database=_DB)
    saved = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url.render_as_string(hide_password=False)
    eng = sa.create_engine(url, future=True)
    try:
        assert len("0050_app_registry") <= 32
        await asyncio.to_thread(command.upgrade, _cfg(url), "0049_quota_config_app_key")
        with eng.begin() as conn:
            conn.execute(sa.text(ddl[0]))
        # Pinned to 0050, not "head": later revisions must not change what this asserts.
        await asyncio.to_thread(command.upgrade, _cfg(url), "0050_app_registry")
        # And the replay after the migration is a no-op too.
        with eng.begin() as conn:
            conn.execute(sa.text(ddl[0]))
        with eng.connect() as conn:
            version = conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalar()
            cols = conn.execute(
                sa.text(
                    "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                    "WHERE table_name = 'app_registry' ORDER BY ordinal_position"
                )
            ).all()
        assert version == "0050_app_registry"
        with eng.connect() as conn:
            guarded = conn.execute(
                sa.text(
                    "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'app_registry'::regclass "
                    "AND tgname = 'billing_truncate_guard_trg'"
                )
            ).scalar()
        assert guarded == 1
        assert cols == [
            ("app_id", "uuid", "NO"),
            ("app_key", "text", "NO"),
            ("revoked", "boolean", "NO"),
            ("synced_at", "timestamp with time zone", "NO"),
        ]
        await asyncio.to_thread(command.downgrade, _cfg(url), "0049_quota_config_app_key")
        with eng.connect() as conn:
            gone = conn.execute(sa.text("SELECT to_regclass('app_registry')")).scalar()
        assert gone is None
    finally:
        eng.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        admin.dispose()
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
