"""A licence-less row whose caller ASSERTED the app derives the licence from (tenant, app).

WHY: per-app licensing (live 2026-10-14) makes delegated calls licence-less by
design -- kag calls the shared leaf mlx-backend with its own service licence +
``X-Effective-Tenant-Id``, and the leaf drops the service licence. A shared leaf
cannot sit in ``cost_attribution_app_by_service_key`` (it serves several apps),
so before this change those rows stayed licence-less forever. Measured prod
2026-10-15: mlx-backend rows for a tenant holding an active KAG Module licence
stored ``license_id`` NULL.

Rules pinned here: asserted app (uuid or key) resolves the tenant's OWN active
licence for that app; no licence / another tenant's licence / revoked / ambiguous
-> NULL (ambiguous counted); an asserted app never falls back to the service_key
map; no assertion -> service_key path unchanged. Flusher tests read rows back out
of Postgres.

Mutations that turn this red: call ``resolve_asserted_app_keys`` after licence
derivation again (key test stores NULL); fall back to ``resolve_license`` when
the asserted path returns None (no-fallback tests store the service_key licence);
drop the tenant half of the map key (other-tenant test stores a foreign licence).
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

import observability.metrics as om
from models.db import CostEvent, MeteringEvent, RequestQuotaConfig
from services.aggregator import flush_costs, flush_events
from services.license_resolver import (
    _build_app_licence_map,
    derive_license,
    reset_cache,
)
from tests._aggregator_helpers import _make_cost
from tests.conftest import T

TENANT = "0195f48b-d000-7000-8000-0000000000a1"
OTHER = "0195f48b-d000-7000-8000-0000000000a2"
APP = uuid.uuid4()
LIC = uuid.uuid4()
SVC_LIC = uuid.uuid4()
SVC_MAP = {"kag-graph": "kag module"}


def _ambiguous(service_key: str) -> float:
    return om.license_ambiguous_total.labels(service_key=service_key)._value.get()


_KAG_GRAPH = "kag-graph"  # pragma: allowlist secret


def _derive(**overrides):
    kwargs = {
        "tenant_id": TENANT,
        "service_key": "mlx-backend",  # pragma: allowlist secret
        "app_asserted": True,
        "asserted_app_id": APP,
        "app_by_service_key": SVC_MAP,
        "resolution_map": {(TENANT, "kag module"): SVC_LIC},
        "app_licence_map": {(TENANT, APP): LIC},
    }
    kwargs.update(overrides)
    return derive_license(**kwargs)


# --------------------------------------------------------------------------- unit


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    async def execute(self, _stmt):
        return _FakeResult(self._rows)


@pytest.mark.unit
class TestDeriveLicense:
    def test_asserted_app_resolves_the_tenants_licence(self):
        assert _derive() == LIC

    def test_asserted_app_the_tenant_holds_no_licence_for_stays_null(self):
        assert _derive(app_licence_map={(OTHER, APP): LIC}) is None

    def test_asserted_app_never_falls_back_to_the_service_key_map(self):
        # The service_key map WOULD resolve SVC_LIC for this row.
        assert _derive(service_key=_KAG_GRAPH, app_licence_map={}) is None

    def test_an_unresolved_asserted_key_never_falls_back(self):
        assert _derive(service_key=_KAG_GRAPH, asserted_app_id=None) is None

    def test_an_unbuilt_app_licence_map_resolves_nothing(self):
        assert _derive(service_key=_KAG_GRAPH, app_licence_map=None) is None

    def test_ambiguous_pair_is_null_and_counted(self):
        before = _ambiguous("mlx-backend")
        assert _derive(app_licence_map={(TENANT, APP): None}) is None
        assert _ambiguous("mlx-backend") - before == 1

    def test_no_assertion_uses_the_service_key_path_unchanged(self):
        assert _derive(app_asserted=False, asserted_app_id=None, service_key=_KAG_GRAPH) == (
            SVC_LIC
        )
        assert _derive(app_asserted=False, asserted_app_id=None) is None  # unmapped key

    async def test_map_build_keys_on_tenant_and_app_uuid_and_drops_duplicates(self):
        dup_app = uuid.uuid4()
        built = await _build_app_licence_map(
            _FakeSession(
                [
                    (TENANT, APP, LIC),
                    (TENANT, dup_app, uuid.uuid4()),
                    (TENANT, dup_app, uuid.uuid4()),
                    (OTHER, APP, SVC_LIC),
                    (TENANT, None, uuid.uuid4()),
                ]
            )
        )
        assert built == {(TENANT, APP): LIC, (TENANT, dup_app): None, (OTHER, APP): SVC_LIC}


# ------------------------------------------------------------- through the flushers


@pytest.fixture
def _fresh_caches():
    reset_cache()
    yield
    reset_cache()


async def _seed(
    db_session_factory,
    *,
    tenant_id: str,
    license_id: uuid.UUID,
    app_id: uuid.UUID,
    app_key: str | None = None,
    app_name: str = "Asserted App",
    status: str = "active",
) -> None:
    async with db_session_factory() as session:
        session.add(
            RequestQuotaConfig(
                license_id=license_id,
                tenant_id=tenant_id,
                quota_monthly=1_000_000,
                app_id=app_id,
                app_name=app_name,
                app_key=app_key,
                status=status,
            )
        )
        await session.commit()


async def _stored_cost(db_session_factory, tenant_id: str) -> CostEvent:
    async with db_session_factory() as session:
        rows = (
            (await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant_id)))
            .scalars()
            .all()
        )
    assert len(rows) == 1, f"expected exactly one stored event, got {len(rows)}"
    return rows[0]


async def _flush_cost(db_session_factory, tenant: str, app_id: str, service_key="mlx-backend"):
    count = await flush_costs(
        [_make_cost(tenant_id=tenant, license_id="", app_id=app_id, service_key=service_key)],
        db_session_factory,
        None,
    )
    assert count == 1


@pytest.mark.unit
@pytest.mark.usefixtures("_fresh_caches")
class TestCostFlusherDerivesFromAssertedApp:
    async def test_asserted_key_resolves_the_tenants_licence(self, db_session_factory):
        tenant = T("asserted-app-key-resolves")
        app, lic = uuid.uuid4(), uuid.uuid4()
        key = f"kag-{uuid.uuid4().hex[:8]}"
        await _seed(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app, app_key=key)

        await _flush_cost(db_session_factory, tenant, key)

        row = await _stored_cost(db_session_factory, tenant)
        assert row.license_id == lic
        assert row.app_id == app
        assert row.cost_source.endswith("+resolved")

    async def test_asserted_uuid_resolves_the_tenants_licence(self, db_session_factory):
        tenant = T("asserted-app-uuid-resolves")
        app, lic = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app)

        await _flush_cost(db_session_factory, tenant, str(app))

        row = await _stored_cost(db_session_factory, tenant)
        assert row.license_id == lic
        assert row.cost_source.endswith("+resolved")

    async def test_another_tenants_licence_for_the_app_is_never_used(self, db_session_factory):
        tenant = T("asserted-app-no-licence")
        app = uuid.uuid4()
        await _seed(
            db_session_factory,
            tenant_id=T("asserted-app-owner"),
            license_id=uuid.uuid4(),
            app_id=app,
        )

        await _flush_cost(db_session_factory, tenant, str(app))

        row = await _stored_cost(db_session_factory, tenant)
        assert row.license_id is None
        assert "+resolved" not in row.cost_source

    async def test_a_revoked_licence_does_not_resolve(self, db_session_factory):
        tenant = T("asserted-app-revoked")
        app = uuid.uuid4()
        await _seed(
            db_session_factory,
            tenant_id=tenant,
            license_id=uuid.uuid4(),
            app_id=app,
            status="revoked",
        )

        await _flush_cost(db_session_factory, tenant, str(app))

        assert (await _stored_cost(db_session_factory, tenant)).license_id is None

    async def test_ambiguous_licences_stay_null_and_are_counted(self, db_session_factory):
        tenant = T("asserted-app-ambiguous")
        app = uuid.uuid4()
        for _ in range(2):
            await _seed(db_session_factory, tenant_id=tenant, license_id=uuid.uuid4(), app_id=app)
        before = _ambiguous("mlx-backend")

        await _flush_cost(db_session_factory, tenant, str(app))

        assert (await _stored_cost(db_session_factory, tenant)).license_id is None
        assert _ambiguous("mlx-backend") - before == 1

    async def test_asserted_app_does_not_fall_back_to_the_service_key_map(self, db_session_factory):
        """kag-graph IS in the default service_key map (-> KAG Module); the tenant
        holds a KAG Module licence, but the caller asserted a DIFFERENT app it has
        no licence for. Falling back would invoice the wrong product."""
        tenant = T("asserted-app-no-fallback")
        await _seed(
            db_session_factory,
            tenant_id=tenant,
            license_id=uuid.uuid4(),
            app_id=uuid.uuid4(),
            app_name="KAG Module",
        )

        await _flush_cost(db_session_factory, tenant, str(uuid.uuid4()), service_key=_KAG_GRAPH)

        assert (await _stored_cost(db_session_factory, tenant)).license_id is None

    async def test_no_assertion_keeps_the_service_key_path(self, db_session_factory):
        tenant = T("asserted-app-none-service-key")
        lic = uuid.uuid4()
        await _seed(
            db_session_factory,
            tenant_id=tenant,
            license_id=lic,
            app_id=uuid.uuid4(),
            app_name="KAG Module",
        )

        await _flush_cost(db_session_factory, tenant, "", service_key=_KAG_GRAPH)

        row = await _stored_cost(db_session_factory, tenant)
        assert row.license_id == lic
        assert row.cost_source.endswith("+resolved")


@pytest.mark.unit
@pytest.mark.usefixtures("_fresh_caches")
class TestEventFlusherParity:
    @staticmethod
    def _event(tenant: str, app_id: str) -> dict:
        return {
            "tenant_id": tenant,
            "license_id": "",
            "app_id": app_id,
            "plan_id": "",
            "method": "POST",
            "path": "/mlx-backend/v1/embeddings",
            "status": "allowed",
            "source_ip": "10.0.0.1",
            "_service_key": "mlx-backend",  # pragma: allowlist secret
        }

    async def _stored(self, db_session_factory, tenant: str) -> MeteringEvent:
        stmt = select(MeteringEvent).where(MeteringEvent.tenant_id == tenant)
        async with db_session_factory() as session:
            rows = (await session.execute(stmt)).scalars().all()
        assert len(rows) == 1
        return rows[0]

    async def test_asserted_key_resolves_on_the_metering_path(self, db_session_factory):
        tenant = T("asserted-app-metering-key")
        app, lic = uuid.uuid4(), uuid.uuid4()
        key = f"kag-{uuid.uuid4().hex[:8]}"
        await _seed(db_session_factory, tenant_id=tenant, license_id=lic, app_id=app, app_key=key)

        assert await flush_events([self._event(tenant, key)], db_session_factory, None) == 1

        row = await self._stored(db_session_factory, tenant)
        assert row.license_id == lic
        assert row.app_id == app

    async def test_foreign_licence_stays_null_on_the_metering_path(self, db_session_factory):
        tenant = T("asserted-app-metering-foreign")
        app = uuid.uuid4()
        await _seed(
            db_session_factory,
            tenant_id=T("asserted-app-metering-owner"),
            license_id=uuid.uuid4(),
            app_id=app,
        )

        assert await flush_events([self._event(tenant, str(app))], db_session_factory, None) == 1

        assert (await self._stored(db_session_factory, tenant)).license_id is None


@pytest.mark.unit
@pytest.mark.usefixtures("_fresh_caches")
class TestAppLicenceMapFailureIsIsolated:
    """A failed (tenant, app) map read must cost ONLY the asserted rows; an
    un-asserted row in the same batch keeps its service_key derivation."""

    @staticmethod
    def _boom(monkeypatch, module: str) -> None:
        async def _raise(_session):
            raise RuntimeError("app licence map unavailable")

        monkeypatch.setattr(f"{module}.get_app_licence_map", _raise)

    async def test_cost_path(self, db_session_factory, monkeypatch):
        self._boom(monkeypatch, "services.cost_flusher")
        plain, asserted = T("app-map-fail-plain"), T("app-map-fail-asserted")
        lic = uuid.uuid4()
        await _seed(
            db_session_factory,
            tenant_id=plain,
            license_id=lic,
            app_id=uuid.uuid4(),
            app_name="KAG Module",
        )
        app = uuid.uuid4()
        await _seed(db_session_factory, tenant_id=asserted, license_id=uuid.uuid4(), app_id=app)

        count = await flush_costs(
            [
                _make_cost(tenant_id=plain, license_id="", app_id="", service_key=_KAG_GRAPH),
                _make_cost(
                    tenant_id=asserted, license_id="", app_id=str(app), service_key=_KAG_GRAPH
                ),
            ],
            db_session_factory,
            None,
        )

        assert count == 2
        assert (await _stored_cost(db_session_factory, plain)).license_id == lic
        assert (await _stored_cost(db_session_factory, asserted)).license_id is None

    async def test_metering_path(self, db_session_factory, monkeypatch):
        self._boom(monkeypatch, "services.event_flusher")
        plain, asserted = T("app-map-fail-m-plain"), T("app-map-fail-m-asserted")
        lic = uuid.uuid4()
        await _seed(
            db_session_factory,
            tenant_id=plain,
            license_id=lic,
            app_id=uuid.uuid4(),
            app_name="KAG Module",
        )
        app = uuid.uuid4()
        await _seed(db_session_factory, tenant_id=asserted, license_id=uuid.uuid4(), app_id=app)
        plain_event = {**TestEventFlusherParity._event(plain, ""), "_service_key": _KAG_GRAPH}
        asserted_event = TestEventFlusherParity._event(asserted, str(app))

        assert await flush_events([plain_event, asserted_event], db_session_factory, None) == 2

        parity = TestEventFlusherParity()
        assert (await parity._stored(db_session_factory, plain)).license_id == lic
        assert (await parity._stored(db_session_factory, asserted)).license_id is None
