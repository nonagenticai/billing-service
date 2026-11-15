"""X-App-Id app KEY -> registry app uuid on the METERING path (event_flusher).

Same contract as ``tests/test_cost_flusher_app_key.py``, through the shared
``license_resolver.resolve_asserted_app_keys`` helper: a uuid is kept; a known
key resolves; an unknown key stays NULL, is counted, and is NEVER derived from
the licence; an absent app id still derives from the licence. Rows are read
back out of Postgres.

Mutations that turn this red: drop ``id(row) not in asserted_app_key`` from
``appless_rows`` (unknown-key test stores the licence's app); remove the
``resolve_asserted_app_keys`` call (resolve test reads NULL).
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

import observability.metrics as om
from models.db import MeteringEvent, RequestQuotaConfig
from services.aggregator import flush_events
from services.license_resolver import split_asserted_app
from tests.conftest import T

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _fresh_resolver_caches():
    from services.license_resolver import reset_cache

    reset_cache()
    yield
    reset_cache()


async def _seed(db_session_factory, *, tenant: str, lic: uuid.UUID, app: uuid.UUID, key: str):
    async with db_session_factory() as session:
        session.add(
            RequestQuotaConfig(
                license_id=lic,
                tenant_id=T(tenant),
                quota_monthly=1_000_000,
                app_id=app,
                app_name="Some App",
                app_key=key,
            )
        )
        await session.commit()


def _event(*, tenant: str, license_id: str, app_id: str) -> dict:
    return {
        "tenant_id": T(tenant),
        "license_id": license_id,
        "app_id": app_id,
        "plan_id": "",
        "method": "POST",
        "path": "/inference-router/api/chat",
        "status": "allowed",
        "source_ip": "10.0.0.1",
        "_service_key": "inference-router",  # pragma: allowlist secret
    }


async def _stored(db_session_factory, tenant: str) -> MeteringEvent:
    stmt = select(MeteringEvent).where(MeteringEvent.tenant_id == T(tenant))
    async with db_session_factory() as session:
        rows = (await session.execute(stmt)).scalars().all()
    assert len(rows) == 1, f"expected exactly one stored event, got {len(rows)}"
    return rows[0]


def _unknown() -> float:
    return om.app_key_unknown_total.labels(stream="metering")._value.get()


def _resolved(key: str) -> float:
    return om.app_key_resolved_total.labels(stream="metering", app_key=key)._value.get()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", (None, None)),
        ("   ", (None, None)),
        (None, (None, None)),
        (123, (None, None)),
        ("core-app", (None, "core-app")),
        (" workers ", (None, "workers")),
        (
            "0195f48b-d000-7000-8000-000000000001",
            (uuid.UUID("0195f48b-d000-7000-8000-000000000001"), None),
        ),
    ],
)
def test_split_asserted_app(raw, expected) -> None:
    assert split_asserted_app(raw) == expected


async def test_a_known_key_resolves_to_the_caller_app(db_session_factory):
    tenant = "mk-resolves"
    router_lic, router_app = uuid.uuid4(), uuid.uuid4()
    caller_app = uuid.uuid4()
    key = f"chat-bridge-{uuid.uuid4().hex[:8]}"
    await _seed(db_session_factory, tenant=tenant, lic=router_lic, app=router_app, key="ir")
    await _seed(db_session_factory, tenant="mk-other", lic=uuid.uuid4(), app=caller_app, key=key)
    before = _resolved(key)

    n = await flush_events(
        [_event(tenant=tenant, license_id=str(router_lic), app_id=key)], db_session_factory, None
    )
    assert n == 1
    row = await _stored(db_session_factory, tenant)
    assert str(row.app_id) == str(caller_app)
    assert _resolved(key) - before == 1


async def test_a_uuid_app_id_is_kept(db_session_factory):
    tenant = "mk-uuid"
    lic, mirror_app, presented = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await _seed(db_session_factory, tenant=tenant, lic=lic, app=mirror_app, key="k-uuid")
    before = _unknown()

    await flush_events(
        [_event(tenant=tenant, license_id=str(lic), app_id=str(presented))],
        db_session_factory,
        None,
    )
    row = await _stored(db_session_factory, tenant)
    assert str(row.app_id) == str(presented)
    assert _unknown() - before == 0


async def test_an_unknown_key_stays_null_counted_and_not_licence_derived(db_session_factory):
    tenant = "mk-unknown"
    lic, licence_app = uuid.uuid4(), uuid.uuid4()
    await _seed(db_session_factory, tenant=tenant, lic=lic, app=licence_app, key="registered")
    before = _unknown()

    await flush_events(
        [_event(tenant=tenant, license_id=str(lic), app_id="never-registered")],
        db_session_factory,
        None,
    )
    row = await _stored(db_session_factory, tenant)
    assert row.app_id is None
    assert str(row.license_id) == str(lic)
    assert _unknown() - before == 1


async def test_an_absent_app_id_is_still_licence_derived(db_session_factory):
    tenant = "mk-absent"
    lic, licence_app = uuid.uuid4(), uuid.uuid4()
    await _seed(db_session_factory, tenant=tenant, lic=lic, app=licence_app, key="k-absent")
    before = _unknown()

    await flush_events(
        [_event(tenant=tenant, license_id=str(lic), app_id="")], db_session_factory, None
    )
    row = await _stored(db_session_factory, tenant)
    assert str(row.app_id) == str(licence_app)
    assert _unknown() - before == 0
