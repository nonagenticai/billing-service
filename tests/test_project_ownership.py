"""Project -> tenant ownership check at ingest (2026-09-28).

Pure adjudication, the fr tenant-map cache, and the flusher wiring (observe
keeps + counts + stamps metadata; enforce nulls on positive knowledge only).
"""

from __future__ import annotations

import uuid

import httpx
import pytest
from sqlalchemy import select

import observability.metrics as om
from models.db import CostEvent
from services import project_owner_map as pom
from services.aggregator import flush_costs
from services.cost_flusher import _adjudicate_project_owner
from settings import get_settings
from tenancy import tenant_level_project_id
from tests._aggregator_helpers import _make_cost
from tests.conftest import T

TENANT = str(uuid.uuid4())
OTHER = str(uuid.uuid4())


def _count(reason: str) -> float:
    return om.project_id_refused_total.labels(reason=reason)._value.get()


@pytest.fixture(autouse=True)
def _fresh_cache():
    pom.reset_cache()
    yield
    pom.reset_cache()


class TestAdjudicatePure:
    def test_owned_is_unchanged_and_uncounted(self) -> None:
        p = uuid.uuid4()
        assert _adjudicate_project_owner(
            p, tenant_id=TENANT, owner_map={str(p): TENANT}, mode="enforce"
        ) == (p, None, None)

    def test_owned_matches_case_insensitively_on_event_tenant(self) -> None:
        p = uuid.uuid4()
        out = _adjudicate_project_owner(
            p, tenant_id=TENANT.upper(), owner_map={str(p): TENANT}, mode="observe"
        )
        assert out == (p, None, None)

    @pytest.mark.parametrize(
        ("owner", "verdict", "reg"),
        [("foreign", "foreign_tenant", OTHER), ("absent", "unknown_project", None)],
    )
    def test_observe_keeps_value_and_stamps_verdict(self, owner, verdict, reg) -> None:
        p = uuid.uuid4()
        m = {str(p): OTHER} if owner == "foreign" else {str(uuid.uuid4()): TENANT}
        kept, reason, meta = _adjudicate_project_owner(
            p, tenant_id=TENANT, owner_map=m, mode="observe"
        )
        assert kept == p
        assert reason == verdict
        assert meta == {"project_id_ownership": {"verdict": verdict, "registry_tenant": reg}}

    @pytest.mark.parametrize("owner", ["foreign", "absent"])
    def test_enforce_nulls_on_positive_knowledge(self, owner) -> None:
        p = uuid.uuid4()
        m = {str(p): OTHER} if owner == "foreign" else {str(uuid.uuid4()): TENANT}
        kept, reason, meta = _adjudicate_project_owner(
            p, tenant_id=TENANT, owner_map=m, mode="enforce"
        )
        assert kept is None
        assert meta == {"project_id_refused": {"asserted": str(p), "reason": reason}}

    @pytest.mark.parametrize("owner_map", [None, {}])
    @pytest.mark.parametrize("mode", ["observe", "enforce"])
    def test_no_map_keeps_value_and_stamps_unverified(self, owner_map, mode) -> None:
        p = uuid.uuid4()
        assert _adjudicate_project_owner(p, tenant_id=TENANT, owner_map=owner_map, mode=mode) == (
            p,
            "owner_map_unavailable",
            {"project_id_ownership": {"verdict": "unverified", "reason": "owner_map_unavailable"}},
        )

    @pytest.mark.parametrize("mode", ["observe", "enforce"])
    def test_project_with_no_tenant_of_record_is_kept_and_stamped_unverified(self, mode) -> None:
        p = uuid.uuid4()
        kept, reason, meta = _adjudicate_project_owner(
            p, tenant_id=TENANT, owner_map={str(p): None}, mode=mode
        )
        assert kept == p
        assert reason == "project_tenant_unset"
        assert meta == {
            "project_id_ownership": {"verdict": "unverified", "reason": "project_tenant_unset"}
        }

    def test_unrecognised_mode_behaves_as_observe(self) -> None:
        p = uuid.uuid4()
        kept, _, _ = _adjudicate_project_owner(
            p, tenant_id=TENANT, owner_map={str(p): OTHER}, mode="Enforce"
        )
        assert kept == p


class TestTenantMapCache:
    def test_parse_rejects_empty_and_malformed(self) -> None:
        assert pom.parse_tenant_map({"projects": []}) is None
        assert pom.parse_tenant_map({"nope": 1}) is None
        assert pom.parse_tenant_map([]) is None

    def test_parse_canonicalises_and_keeps_null_tenant(self) -> None:
        p, t = uuid.uuid4(), uuid.uuid4()
        out = pom.parse_tenant_map(
            {
                "projects": [
                    {"project_id": str(p).upper(), "tenant_id": str(t).upper()},
                    {"project_id": "not-a-uuid", "tenant_id": str(t)},
                    {"project_id": str(t), "tenant_id": None},
                ],
                "generated_at": "2026-09-28T00:00:00Z",
            }
        )
        assert out == {str(p): str(t), str(t): None}

    async def test_unset_token_or_url_never_fetches(self, monkeypatch) -> None:
        calls: list[str] = []

        async def _fetch(*a, **kw):
            calls.append("x")
            return {"a": "b"}

        monkeypatch.setattr(pom, "_fetch", _fetch)
        assert await pom.get_project_owner_map("", "tok") is None
        assert await pom.get_project_owner_map("http://fr", "") is None
        assert calls == []

    async def test_single_fetch_within_ttl_and_refetch_after(self, monkeypatch) -> None:
        """⭐ MUTATION CONTROL: drop the TTL short-circuit in
        get_project_owner_map and the call count goes 2 at t=299."""
        p = str(uuid.uuid4())
        calls: list[tuple[str, dict]] = []
        real_get = httpx.AsyncClient.get

        async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
            if str(url).endswith(pom.TENANT_MAP_PATH):
                calls.append((str(url), kw.get("headers", {})))
                return httpx.Response(
                    200, json={"projects": [{"project_id": p, "tenant_id": TENANT}]}
                )
            return await real_get(self, url, *a, **kw)

        monkeypatch.setattr(httpx.AsyncClient, "get", _get)
        m1 = await pom.get_project_owner_map("http://fr/", "sekrit", now=1000.0)
        m2 = await pom.get_project_owner_map("http://fr/", "sekrit", now=1299.0)
        assert m1 == m2 == {p: TENANT}
        assert len(calls) == 1
        assert calls[0][0] == "http://fr/api/internal/projects/tenant-map"
        assert calls[0][1] == {"X-Internal-Token": "sekrit"}
        await pom.get_project_owner_map("http://fr/", "sekrit", now=1300.0)
        assert len(calls) == 2

    async def test_failure_is_none_and_cached_briefly(self, monkeypatch) -> None:
        calls: list[int] = []

        async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
            calls.append(1)
            raise httpx.ConnectError("fr down")

        monkeypatch.setattr(httpx.AsyncClient, "get", _get)
        assert await pom.get_project_owner_map("http://fr", "t", now=0.0) is None
        assert await pom.get_project_owner_map("http://fr", "t", now=59.0) is None
        assert len(calls) == 1
        await pom.get_project_owner_map("http://fr", "t", now=60.0)
        assert len(calls) == 2

    async def test_non_200_is_none(self, monkeypatch) -> None:
        async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
            return httpx.Response(401, json={"error": "nope"})

        monkeypatch.setattr(httpx.AsyncClient, "get", _get)
        assert await pom.get_project_owner_map("http://fr", "t", now=0.0) is None


class TestFlusherWiring:
    """DB-backed: the verdict actually lands on the stored row."""

    @staticmethod
    def _arm(monkeypatch, owner_map, mode="observe"):
        monkeypatch.setenv("FR_BASE_URL", "http://fr.test")
        monkeypatch.setenv("FR_INTERNAL_TOKEN", "tok")
        monkeypatch.setenv("BILLING_PROJECT_OWNERSHIP_MODE", mode)
        get_settings.cache_clear()

        async def _map(base, token, *, now=None):
            assert (base, token) == ("http://fr.test", "tok")
            return owner_map

        import services.cost_flusher as cf

        monkeypatch.setattr(cf, "get_project_owner_map", _map)

    async def _flush_one(self, db_session_factory, tenant, project):
        buffer = [_make_cost(license_id="", tenant_id=tenant, project_id=project)]
        assert await flush_costs(buffer, db_session_factory, None) == 1
        async with db_session_factory() as session:
            return (await session.execute(select(CostEvent))).scalar_one()

    async def test_owned_project_unchanged_no_count(self, db_session_factory, monkeypatch):
        tenant, project = T("own-payer"), str(uuid.uuid4())
        self._arm(monkeypatch, {project: tenant}, mode="enforce")
        before = {r: _count(r) for r in om.PROJECT_ID_REFUSED_REASONS}
        row = await self._flush_one(db_session_factory, tenant, project)
        assert str(row.project_id) == project
        assert "project_id_ownership" not in (row.extra_metadata or {})
        assert {r: _count(r) for r in om.PROJECT_ID_REFUSED_REASONS} == before

    @pytest.mark.parametrize("mode", ["observe", "enforce"])
    async def test_own_tenant_level_project_skips_fr_adjudication(
        self, db_session_factory, monkeypatch, mode
    ):
        """Owner ruling 2026-10-07: a tenant-level project id is not an fr
        project; against a non-empty map it would otherwise read
        `unknown_project` and, in enforce mode, be nulled.

        ⭐ MUTATION CONTROL: drop the tenant-level clause on the flusher's
        adjudication branch and this goes RED."""
        tenant = T("own-tenant-level")
        tl = tenant_level_project_id(tenant)
        self._arm(monkeypatch, {str(uuid.uuid4()): tenant}, mode=mode)
        before = {r: _count(r) for r in om.PROJECT_ID_REFUSED_REASONS}
        row = await self._flush_one(db_session_factory, tenant, tl)
        assert str(row.project_id) == tl
        meta = row.extra_metadata or {}
        assert "project_id_ownership" not in meta
        assert "project_id_refused" not in meta
        assert {r: _count(r) for r in om.PROJECT_ID_REFUSED_REASONS} == before

    async def test_foreign_observe_keeps_counts_and_stamps(self, db_session_factory, monkeypatch):
        tenant, project = T("own-payer-2"), str(uuid.uuid4())
        self._arm(monkeypatch, {project: OTHER})
        before = _count("foreign_tenant")
        row = await self._flush_one(db_session_factory, tenant, project)
        assert str(row.project_id) == project
        assert row.extra_metadata["project_id_ownership"] == {
            "verdict": "foreign_tenant",
            "registry_tenant": OTHER,
        }
        assert _count("foreign_tenant") - before == 1

    async def test_foreign_enforce_nulls_and_stamps(self, db_session_factory, monkeypatch):
        tenant, project = T("own-payer-3"), str(uuid.uuid4())
        self._arm(monkeypatch, {project: OTHER}, mode="enforce")
        before = _count("foreign_tenant")
        row = await self._flush_one(db_session_factory, tenant, project)
        assert row.project_id is None
        assert row.extra_metadata["project_id_refused"] == {
            "asserted": project,
            "reason": "foreign_tenant",
        }
        assert _count("foreign_tenant") - before == 1

    async def test_unknown_observe_keeps_enforce_nulls(self, db_session_factory, monkeypatch):
        tenant, project = T("own-payer-4"), str(uuid.uuid4())
        self._arm(monkeypatch, {str(uuid.uuid4()): tenant})
        before = _count("unknown_project")
        row = await self._flush_one(db_session_factory, tenant, project)
        assert str(row.project_id) == project
        assert row.extra_metadata["project_id_ownership"]["verdict"] == "unknown_project"
        assert _count("unknown_project") - before == 1

    async def test_unknown_enforce_nulls(self, db_session_factory, monkeypatch):
        tenant, project = T("own-payer-5"), str(uuid.uuid4())
        self._arm(monkeypatch, {str(uuid.uuid4()): tenant}, mode="enforce")
        row = await self._flush_one(db_session_factory, tenant, project)
        assert row.project_id is None
        assert row.extra_metadata["project_id_refused"]["reason"] == "unknown_project"

    async def test_fr_down_keeps_value_counts_unavailable(self, db_session_factory, monkeypatch):
        tenant, project = T("own-payer-6"), str(uuid.uuid4())
        self._arm(monkeypatch, None, mode="enforce")
        before = _count("owner_map_unavailable")
        tenancy_before = _count("registry_unavailable")
        row = await self._flush_one(db_session_factory, tenant, project)
        assert str(row.project_id) == project
        assert row.extra_metadata["project_id_ownership"] == {
            "verdict": "unverified",
            "reason": "owner_map_unavailable",
        }
        assert _count("owner_map_unavailable") - before == 1
        # The tenancy-registry label is a different failure and must not move.
        assert _count("registry_unavailable") == tenancy_before


def test_settings_default_mode_is_observe(monkeypatch) -> None:
    monkeypatch.delenv("BILLING_PROJECT_OWNERSHIP_MODE", raising=False)
    get_settings.cache_clear()
    assert get_settings().billing_project_ownership_mode == "observe"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("ENFORCE", "enforce"), (" Enforce ", "enforce"), ("OBSERVE", "observe")],
)
def test_settings_mode_is_case_insensitive(monkeypatch, raw, expected) -> None:
    monkeypatch.setenv("BILLING_PROJECT_OWNERSHIP_MODE", raw)
    get_settings.cache_clear()
    try:
        assert get_settings().billing_project_ownership_mode == expected
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize("raw", ["enforc", "block", ""])
def test_settings_unknown_mode_falls_back_to_observe_with_warning(monkeypatch, raw) -> None:
    """⭐ MUTATION CONTROL: drop the mode="before" validator and this raises
    pydantic ValidationError at construction (billing crashes at startup).

    The warning is asserted on a spy of the module logger, not via caplog:
    in the full suite an earlier test reconfigures logging and disables
    existing loggers, which caplog cannot see through (order-dependent red).
    """
    import settings as settings_module

    warnings: list[str] = []
    monkeypatch.setattr(
        settings_module._logger,
        "warning",
        lambda msg, *args, **kw: warnings.append(msg % args),
    )
    monkeypatch.setenv("BILLING_PROJECT_OWNERSHIP_MODE", raw)
    get_settings.cache_clear()
    try:
        assert get_settings().billing_project_ownership_mode == "observe"
        assert [w for w in warnings if "BILLING_PROJECT_OWNERSHIP_MODE" in w]
    finally:
        get_settings.cache_clear()
