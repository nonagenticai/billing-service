"""X-App-Id carries the app KEY; billing resolves key -> registry app uuid.

Owner decision 2026-10-07: every inference-router caller is a registered app in
licences, and ``X-App-Id`` carries its key (``core-app``, ``chat-bridge-rmt``,
``workers``). Before this change ``_parse_uuid`` turned the slug into NULL and the
app was then derived from the licence -- which, for router traffic, names the
ROUTER's app, so the caller was lost.

Every assertion reads the row back out of Postgres. Mutations that turn this
red: drop the ``id(row) in asserted_app_key`` guard in the licence-derivation
loop (the unknown-key test stores the licence's app); skip the key-map block
(the resolve test stores the licence's app instead of the keyed one); drop
``app_key`` from the sync upsert (the sync test reads NULL).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy import select

import observability.metrics as om
from models.db import CostEvent, RequestQuotaConfig
from services.aggregator import flush_costs
from tests._aggregator_helpers import _make_cost
from tests.conftest import T, throwaway_db_name
from tests.test_budget_license_sync import _make_stub_redis, _mock_licenses_client

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def _fresh_resolver_caches():
    """The key/app maps are process-global with a 300s TTL -- never inherit one."""
    from services.license_resolver import reset_cache

    reset_cache()
    yield
    reset_cache()


async def _seed(
    db_session_factory,
    *,
    tenant_id: str,
    license_id: uuid.UUID,
    app_id: uuid.UUID,
    app_key: str | None,
) -> None:
    async with db_session_factory() as session:
        session.add(
            RequestQuotaConfig(
                license_id=license_id,
                tenant_id=tenant_id,
                quota_monthly=1_000_000,
                app_id=app_id,
                app_name="Some App",
                app_key=app_key,
            )
        )
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


def _key_resolved(app_key: str) -> float:
    return om.app_key_resolved_total.labels(stream="cost", app_key=app_key)._value.get()


def _key_unknown() -> float:
    return om.app_key_unknown_total.labels(stream="cost")._value.get()


@pytest.mark.unit
class TestAppKeyResolution:
    async def test_a_known_key_resolves_to_the_registry_app_not_the_licence_app(
        self, db_session_factory
    ):
        tenant = T("app-key-resolves")
        router_lic, router_app = uuid.uuid4(), uuid.uuid4()
        caller_lic, caller_app = uuid.uuid4(), uuid.uuid4()
        key = f"core-app-{uuid.uuid4().hex[:8]}"
        # The event's licence maps to the ROUTER app; the key names the CALLER.
        await _seed(
            db_session_factory,
            tenant_id=tenant,
            license_id=router_lic,
            app_id=router_app,
            app_key="inference-router",
        )
        await _seed(
            db_session_factory,
            tenant_id=T("app-key-other-tenant"),
            license_id=caller_lic,
            app_id=caller_app,
            app_key=key,
        )
        before = _key_resolved(key)

        count = await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(router_lic), app_id=key)],
            db_session_factory,
            None,
        )
        assert count == 1

        row = await _stored(db_session_factory, tenant)
        assert str(row.app_id) == str(caller_app)
        assert str(row.license_id) == str(router_lic)
        assert _key_resolved(key) - before == 1

    async def test_a_uuid_app_id_is_kept_as_asserted(self, db_session_factory):
        tenant = T("app-key-uuid-kept")
        lic, mirror_app, presented = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        await _seed(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=mirror_app,
            app_key="whatever",
        )
        before_unknown = _key_unknown()

        await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id=str(presented))],
            db_session_factory,
            None,
        )

        row = await _stored(db_session_factory, tenant)
        assert str(row.app_id) == str(presented)
        assert _key_unknown() - before_unknown == 0

    async def test_an_unknown_key_stays_null_is_counted_and_is_not_licence_derived(
        self, db_session_factory
    ):
        tenant = T("app-key-unknown")
        lic, licence_app = uuid.uuid4(), uuid.uuid4()
        # The licence IS mirrored with an app, so licence derivation WOULD stamp
        # it -- the row must nonetheless stay NULL.
        await _seed(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=licence_app,
            app_key="registered-key",
        )
        before = _key_unknown()

        await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="not-a-registered-app")],
            db_session_factory,
            None,
        )

        row = await _stored(db_session_factory, tenant)
        assert row.app_id is None
        assert str(row.license_id) == str(lic)
        assert _key_unknown() - before == 1

    async def test_an_absent_header_is_still_licence_derived(self, db_session_factory):
        tenant = T("app-key-absent")
        lic, licence_app = uuid.uuid4(), uuid.uuid4()
        await _seed(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=licence_app,
            app_key="some-key",
        )
        before = _key_unknown()

        await flush_costs(
            [_make_cost(tenant_id=tenant, license_id=str(lic), app_id="")],
            db_session_factory,
            None,
        )

        row = await _stored(db_session_factory, tenant)
        assert str(row.app_id) == str(licence_app)
        assert _key_unknown() - before == 0

    async def test_an_ambiguous_key_is_dropped_from_the_map(self, db_session_factory):
        """Exactly one, or nothing: a key on two different apps resolves to neither."""
        from services.license_resolver import get_app_key_map

        key = f"dup-{uuid.uuid4().hex[:8]}"
        for _ in range(2):
            await _seed(
                db_session_factory,
                tenant_id=T("app-key-ambiguous"),
                license_id=uuid.uuid4(),
                app_id=uuid.uuid4(),
                app_key=key,
            )
        same_key, same_app = f"same-{uuid.uuid4().hex[:8]}", uuid.uuid4()
        for _ in range(2):
            await _seed(
                db_session_factory,
                tenant_id=T("app-key-same"),
                license_id=uuid.uuid4(),
                app_id=same_app,
                app_key=same_key,
            )
        async with db_session_factory() as session:
            key_map = await get_app_key_map(session)
        assert key not in key_map
        assert key_map[same_key] == same_app


@pytest.mark.unit
def test_sync_from_licenses_mirrors_the_app_key(app_client, monkeypatch):
    import psycopg

    import redis_client

    monkeypatch.setattr(redis_client, "_redis", _make_stub_redis())
    lic_id, app_id = str(uuid.uuid4()), str(uuid.uuid4())
    payload = [
        {
            "id": lic_id,
            "tenant_id": T("app-key-sync"),
            "status": "active",
            "app": {"id": app_id, "key": "chat-bridge-rmt", "name": "Chat Bridge RMT"},
            "plan": {"quota_monthly": 500},
        }
    ]
    with patch("routers.budget.httpx.AsyncClient", return_value=_mock_licenses_client(payload)):
        r = app_client.post("/api/budget/sync-from-licenses")
    assert r.status_code == 200, r.text

    url = os.environ["DATABASE_URL"].replace("+psycopg", "")
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT app_key, app_id::text, app_name FROM request_quota_config "
            "WHERE license_id = %s",
            (lic_id,),
        )
        assert cur.fetchone() == ("chat-bridge-rmt", app_id, "Chat Bridge RMT")


_DB = throwaway_db_name("_billing_0049_check")


def _cfg(url: sa.engine.URL) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
    return cfg


@pytest.mark.integration
async def test_0049_is_drift_tolerant_when_the_column_already_exists() -> None:
    """The pod's ADDITIVE_MIGRATIONS replay can add app_key before alembic runs."""
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
        assert len("0049_quota_config_app_key") <= 32
        await asyncio.to_thread(command.upgrade, _cfg(url), "0048_rekey_book_fulfillment")
        with eng.begin() as conn:
            conn.execute(sa.text("ALTER TABLE request_quota_config ADD COLUMN app_key text"))
        # Pinned to 0049, not "head": later revisions must not change what this asserts.
        await asyncio.to_thread(command.upgrade, _cfg(url), "0049_quota_config_app_key")
        with eng.connect() as conn:
            version = conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalar()
            cols = conn.execute(
                sa.text(
                    "SELECT data_type, is_nullable FROM information_schema.columns "
                    "WHERE table_name = 'request_quota_config' AND column_name = 'app_key'"
                )
            ).all()
        assert version == "0049_quota_config_app_key"
        assert cols == [("text", "YES")]
        await asyncio.to_thread(command.downgrade, _cfg(url), "0048_rekey_book_fulfillment")
        with eng.connect() as conn:
            gone = conn.execute(
                sa.text(
                    "SELECT count(*) FROM information_schema.columns "
                    "WHERE table_name = 'request_quota_config' AND column_name = 'app_key'"
                )
            ).scalar()
        assert gone == 0
    finally:
        eng.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        admin.dispose()
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
