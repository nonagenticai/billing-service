"""The cross rollup ``billing_cost_usage_daily`` and ``GET /api/usage/breakdown`` (+ ``/me``).

1. ``rollup_daily`` writes the cross axis and it RECONCILES with ``CostDaily`` (same filters,
   same day); parked rows are excluded; sentinels and ``funding='unknown'`` as documented.
2. The migration's backfill SQL produces exactly what ``rollup_daily`` produces.
3. Authz: per-member data -> a licence token is 403; a proxy pinned by ``X-Tenant-Id`` cannot
   read another tenant; a platform token may read any tenant.
4. ``group_by`` combinations and filters.
5. ``/breakdown/me``: the caller's own actor in the active tenant only.
6. Retention.
"""

from __future__ import annotations

import importlib.util
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, insert, select, text

import auth_deps
from models.db import (
    NULL_ACTOR_ID,
    NULL_APP_ID,
    NULL_PROJECT_ID,
    CostDaily,
    CostEvent,
    CostUsageDaily,
)
from models.db.app_registry import AppRegistry
from services.aggregator import purge_old_usage_rollup, rollup_daily
from tests.conftest import T

_MIG_PATH = (
    Path(__file__).resolve().parent.parent
    / "alembic"
    / "versions"
    / "0053_cost_funding_usage_daily.py"
)

TENANT_A = T("usage-bd-a")
TENANT_B = T("usage-bd-b")
PROJ_1 = uuid.uuid5(uuid.NAMESPACE_URL, "usage-bd/proj-1")
PROJ_2 = uuid.uuid5(uuid.NAMESPACE_URL, "usage-bd/proj-2")
ALICE = uuid.uuid5(uuid.NAMESPACE_URL, "usage-bd/alice")
BOB = uuid.uuid5(uuid.NAMESPACE_URL, "usage-bd/bob")
APP_FR = uuid.uuid5(uuid.NAMESPACE_URL, "usage-bd/app-fr")
BEARER = {"Authorization": "Bearer fake"}
PATH = "/api/usage/breakdown"
ME = "/api/usage/breakdown/me"


def _load_migration():
    spec = importlib.util.spec_from_file_location("_mig_0053_usage", _MIG_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _token(monkeypatch, roles: list[str], sub: str = "test-sa", **claims) -> None:
    payload = {"sub": sub, "realm_access": {"roles": roles}, **claims}

    async def _fake_decode(_request):
        return payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)


def _no_token(monkeypatch) -> None:
    from fastapi import HTTPException

    async def _fail(_request):
        raise HTTPException(status_code=401, detail="Invalid token")

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fail)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fail)


# ---------------------------------------------------------------------------
# 1 + 2. The rollup and its backfill twin
# ---------------------------------------------------------------------------


def _yesterday_ts() -> datetime:
    y = (datetime.now(UTC) - timedelta(days=1)).date()
    return datetime.combine(y, datetime.min.time(), tzinfo=UTC) + timedelta(hours=3)


async def _seed_events(factory) -> None:
    ts = _yesterday_ts()

    def ev(tenant, *, project, actor, app, funding, cost, provider="pgx-local", excluded=False):
        return CostEvent(
            tenant_id=tenant,
            project_id=project,
            actor_id=actor,
            app_id=app,
            funding=funding,
            service_key="worker_management",  # pragma: allowlist secret
            provider=provider,
            served_model="qwen",
            input_tokens=10,
            output_tokens=5,
            cost_usd=Decimal(cost),
            cost_source="wm",
            excluded_from_billing=excluded,
            exclusion_reason="duplicate" if excluded else None,
            created_at=ts,
        )

    async with factory() as s:
        s.add_all(
            [
                ev(
                    TENANT_A, project=PROJ_1, actor=ALICE, app=APP_FR, funding="local_gpu", cost="1"
                ),
                ev(
                    TENANT_A, project=PROJ_1, actor=ALICE, app=APP_FR, funding="local_gpu", cost="2"
                ),
                ev(
                    TENANT_A,
                    project=PROJ_1,
                    actor=BOB,
                    app=None,
                    funding="tenant_byo",
                    cost="4",
                    provider="openai",
                ),
                ev(TENANT_A, project=PROJ_2, actor=None, app=None, funding=None, cost="8"),
                # project == tenant id is the tenant-level fallback -> NULL_PROJECT_ID.
                ev(
                    TENANT_A,
                    project=uuid.UUID(TENANT_A),
                    actor=ALICE,
                    app=None,
                    funding="platform_external",
                    cost="16",
                    provider="claude",
                ),
                # PARKED: must not appear anywhere.
                ev(
                    TENANT_A,
                    project=PROJ_1,
                    actor=ALICE,
                    app=APP_FR,
                    funding="local_gpu",
                    cost="1000",
                    excluded=True,
                ),
                ev(TENANT_B, project=PROJ_1, actor=ALICE, app=None, funding="local_gpu", cost="32"),
            ]
        )
        await s.commit()


async def _usage_snapshot(factory) -> set[tuple]:
    async with factory() as s:
        rows = (await s.execute(select(CostUsageDaily))).scalars().all()
    return {
        (
            r.bucket_date,
            str(r.tenant_id),
            r.project_id,
            r.actor_id,
            r.app_id,
            r.funding,
            r.provider,
            r.served_model,
            r.event_count,
            r.total_input_tokens,
            r.total_output_tokens,
            Decimal(r.total_cost_usd),
        )
        for r in rows
    }


@pytest.mark.unit
async def test_the_cross_rollup_reconciles_with_cost_daily_and_keeps_every_axis(
    db_session_factory,
):
    await _seed_events(db_session_factory)
    await rollup_daily(db_session_factory)

    async with db_session_factory() as s:
        for tenant in (TENANT_A, TENANT_B):
            usage_total = (
                await s.execute(
                    select(func.sum(CostUsageDaily.total_cost_usd)).where(
                        CostUsageDaily.tenant_id == tenant
                    )
                )
            ).scalar_one()
            daily_total = (
                await s.execute(
                    select(func.sum(CostDaily.total_cost_usd)).where(CostDaily.tenant_id == tenant)
                )
            ).scalar_one()
            assert usage_total == daily_total, tenant
    snap = await _usage_snapshot(db_session_factory)
    by_key = {(t, p, a, app, f): (n, c) for (_d, t, p, a, app, f, _pr, _m, n, _i, _o, c) in snap}
    assert by_key[(TENANT_A, PROJ_1, ALICE, APP_FR, "local_gpu")] == (2, Decimal("3"))
    assert by_key[(TENANT_A, PROJ_1, BOB, NULL_APP_ID, "tenant_byo")] == (1, Decimal("4"))
    # NULL funding / actor are the documented sentinels, never dropped.
    assert by_key[(TENANT_A, PROJ_2, NULL_ACTOR_ID, NULL_APP_ID, "unknown")] == (1, Decimal("8"))
    # project == tenant collapses to the "no project" sentinel, like CostDaily.
    assert by_key[(TENANT_A, NULL_PROJECT_ID, ALICE, NULL_APP_ID, "platform_external")] == (
        1,
        Decimal("16"),
    )
    # the parked 1000 is nowhere.
    assert sum(c for (*_x, c) in snap) == Decimal("63")


@pytest.mark.unit
async def test_the_migration_backfill_produces_exactly_what_the_rollup_produces(
    db_session_factory,
):
    """⚠️ MUTATION TARGETS: drop ``excluded_from_billing IS FALSE`` or the
    ``nullif(project_id, tenant_id)`` from ``BACKFILL_USAGE_DAILY_SQL`` -> differs."""
    await _seed_events(db_session_factory)
    await rollup_daily(db_session_factory)
    from_rollup = await _usage_snapshot(db_session_factory)
    assert from_rollup

    mig = _load_migration()
    async with db_session_factory() as s:
        await s.execute(text("DELETE FROM billing_cost_usage_daily"))
        await s.execute(text(mig.BACKFILL_USAGE_DAILY_SQL))
        await s.commit()
    assert await _usage_snapshot(db_session_factory) == from_rollup

    # Re-running the backfill is a no-op (ON CONFLICT DO NOTHING).
    async with db_session_factory() as s:
        await s.execute(text(mig.BACKFILL_USAGE_DAILY_SQL))
        await s.commit()
    assert await _usage_snapshot(db_session_factory) == from_rollup


# ---------------------------------------------------------------------------
# 3-5. The API
# ---------------------------------------------------------------------------


async def _seed_rollup(factory) -> date:
    day = datetime.now(UTC).date() - timedelta(days=1)

    def row(tenant, project, actor, app, funding, cost, provider="pgx-local", model="qwen", d=day):
        return {
            "id": uuid.uuid4(),
            "bucket_date": d,
            "tenant_id": tenant,
            "project_id": project,
            "actor_id": actor,
            "app_id": app,
            "funding": funding,
            "provider": provider,
            "served_model": model,
            "event_count": 1,
            "total_input_tokens": 10,
            "total_output_tokens": 5,
            "total_cost_usd": Decimal(cost),
        }

    async with factory() as s:
        await s.execute(
            insert(CostUsageDaily),
            [
                row(TENANT_A, PROJ_1, ALICE, APP_FR, "local_gpu", "1"),
                row(TENANT_A, PROJ_1, ALICE, APP_FR, "local_gpu", "2", d=day - timedelta(days=1)),
                row(
                    TENANT_A,
                    PROJ_1,
                    BOB,
                    NULL_APP_ID,
                    "tenant_byo",
                    "4",
                    provider="openai",
                    model="gpt",
                ),
                row(TENANT_A, NULL_PROJECT_ID, NULL_ACTOR_ID, NULL_APP_ID, "unknown", "8"),
                row(
                    TENANT_A,
                    PROJ_2,
                    ALICE,
                    NULL_APP_ID,
                    "platform_external",
                    "16",
                    provider="claude",
                    model="sonnet",
                ),
                # Another tenant, same actor and project ids: must never leak into A.
                row(TENANT_B, PROJ_1, ALICE, APP_FR, "local_gpu", "1000"),
            ],
        )
        await s.execute(insert(AppRegistry), [{"app_id": APP_FR, "app_key": "fr"}])
        await s.commit()
    return day


def _dims_cost(body) -> dict[tuple, str]:
    return {tuple(sorted(r["dims"].items())): r["cost_usd"] for r in body["data"]["rows"]}


@pytest.mark.integration
class TestAuthz:
    async def test_a_licence_token_member_is_refused(
        self, app_client, db_session_factory, monkeypatch
    ):
        """⚠️ MUTATION TARGET: swap require_service_caller for require_tenant -> 200."""
        await _seed_rollup(db_session_factory)
        _no_token(monkeypatch)
        r = app_client.get(PATH, headers={**BEARER, "X-Tenant-Id": TENANT_A})
        assert r.status_code == 403, r.text

    async def test_a_token_without_an_internal_role_is_refused(
        self, app_client, db_session_factory, monkeypatch
    ):
        _token(monkeypatch, ["portal-user"])
        r = app_client.get(PATH, headers={**BEARER, "X-Tenant-Id": TENANT_A})
        assert r.status_code == 403, r.text

    async def test_a_pinned_proxy_cannot_read_another_tenant(
        self, app_client, db_session_factory, monkeypatch
    ):
        """⚠️ MUTATION TARGET: delete enforce_tenant_isolation -> 200 with B's 1000."""
        await _seed_rollup(db_session_factory)
        _token(monkeypatch, ["billing-internal"])
        r = app_client.get(
            PATH, params={"tenant_id": TENANT_B}, headers={**BEARER, "X-Tenant-Id": TENANT_A}
        )
        assert r.status_code == 403, r.text

    async def test_a_pinned_proxy_reads_only_its_own_tenant(
        self, app_client, db_session_factory, monkeypatch
    ):
        await _seed_rollup(db_session_factory)
        _token(monkeypatch, ["billing-internal"])
        r = app_client.get(PATH, headers={**BEARER, "X-Tenant-Id": TENANT_A})
        assert r.status_code == 200, r.text
        data = r.json()["data"]
        assert data["tenant_id"] == TENANT_A
        assert Decimal(data["totals"]["cost_usd"]) == Decimal("31")

    async def test_a_platform_token_reads_any_tenant(
        self, app_client, db_session_factory, monkeypatch
    ):
        await _seed_rollup(db_session_factory)
        _token(monkeypatch, ["license-admin"])
        r = app_client.get(PATH, params={"tenant_id": TENANT_B}, headers=BEARER)
        assert r.status_code == 200, r.text
        assert Decimal(r.json()["data"]["totals"]["cost_usd"]) == Decimal("1000")

    async def test_a_platform_token_must_name_the_tenant(self, app_client, monkeypatch):
        _token(monkeypatch, ["license-admin"])
        r = app_client.get(PATH, headers=BEARER)
        assert r.status_code == 400, r.text


READER_AZP = "analytics-app-service-account"
READER_CLAIMS = {"azp": READER_AZP, "preferred_username": f"service-account-{READER_AZP}"}


@pytest.mark.integration
class TestAllowlistedReader:
    """A reader SA with NO internal role: its own tenant only, never the any-tenant path."""

    async def test_reads_its_pinned_tenant(self, app_client, db_session_factory, monkeypatch):
        """⚠️ MUTATION TARGET: revert the route to require_service_caller -> 403."""
        await _seed_rollup(db_session_factory)
        _token(monkeypatch, ["default-roles-secure-apps"], **READER_CLAIMS)
        r = app_client.get(PATH, headers={**BEARER, "X-Tenant-Id": TENANT_A})
        assert r.status_code == 200, r.text
        data = r.json()["data"]
        assert data["tenant_id"] == TENANT_A
        assert Decimal(data["totals"]["cost_usd"]) == Decimal("31")

    async def test_cannot_read_another_tenant(self, app_client, db_session_factory, monkeypatch):
        await _seed_rollup(db_session_factory)
        _token(monkeypatch, ["default-roles-secure-apps"], **READER_CLAIMS)
        r = app_client.get(
            PATH, params={"tenant_id": TENANT_B}, headers={**BEARER, "X-Tenant-Id": TENANT_A}
        )
        assert r.status_code == 403, r.text

    async def test_without_a_pinned_tenant_is_refused(
        self, app_client, db_session_factory, monkeypatch
    ):
        """⚠️ MUTATION TARGET: drop the X-Tenant-Id requirement -> 200 with B's 1000."""
        await _seed_rollup(db_session_factory)
        _token(monkeypatch, ["default-roles-secure-apps"], **READER_CLAIMS)
        r = app_client.get(PATH, params={"tenant_id": TENANT_B}, headers=BEARER)
        assert r.status_code == 403, r.text

    @pytest.mark.parametrize(
        "claims",
        [
            {"azp": "some-other-client", "preferred_username": "service-account-some-other-client"},
            # A HUMAN who logged in through the allow-listed client: azp matches, user does not.
            {"azp": READER_AZP, "preferred_username": "alice@example.com"},
            {"preferred_username": f"service-account-{READER_AZP}"},
        ],
    )
    async def test_a_non_allowlisted_or_human_token_is_refused(
        self, app_client, db_session_factory, monkeypatch, claims
    ):
        await _seed_rollup(db_session_factory)
        _token(monkeypatch, ["default-roles-secure-apps"], **claims)
        r = app_client.get(PATH, headers={**BEARER, "X-Tenant-Id": TENANT_A})
        assert r.status_code == 403, r.text

    async def test_the_reader_gate_does_not_open_breakdown_me_or_costs(self):
        """The allow-list is wired to /api/usage/breakdown ONLY."""
        import inspect

        import routers.usage_breakdown as ub

        src = inspect.getsource(ub)
        assert src.count("Depends(require_usage_breakdown_reader)") == 1


@pytest.mark.integration
class TestGroupBy:
    @pytest.fixture(autouse=True)
    def _sa(self, monkeypatch):
        _token(monkeypatch, ["license-admin"])

    def _get(self, client, **params):
        r = client.get(PATH, params={"tenant_id": TENANT_A, **params}, headers=BEARER)
        assert r.status_code == 200, r.text
        return r.json()

    async def test_default_is_project_actor_funding(self, app_client, db_session_factory):
        await _seed_rollup(db_session_factory)
        body = self._get(app_client)
        assert body["data"]["group_by"] == ["project", "actor", "funding"]
        got = _dims_cost(body)
        assert got[(("actor", str(ALICE)), ("funding", "local_gpu"), ("project", str(PROJ_1)))] == (
            "3.000000"
        )
        # sentinels come back as null, funding 'unknown' verbatim.
        assert got[(("actor", None), ("funding", "unknown"), ("project", None))] == "8.000000"
        assert len(got) == 4
        assert "from" in body["data"] and "to" in body["data"]

    async def test_funding_only_sums_across_everything_else(self, app_client, db_session_factory):
        await _seed_rollup(db_session_factory)
        got = _dims_cost(self._get(app_client, group_by="funding"))
        assert got == {
            (("funding", "platform_external"),): "16.000000",
            (("funding", "unknown"),): "8.000000",
            (("funding", "tenant_byo"),): "4.000000",
            (("funding", "local_gpu"),): "3.000000",
        }

    async def test_app_resolves_to_the_registry_key(self, app_client, db_session_factory):
        await _seed_rollup(db_session_factory)
        got = _dims_cost(self._get(app_client, group_by="app"))
        assert got[(("app", "fr"),)] == "3.000000"
        assert got[(("app", None),)] == "28.000000"

    async def test_day_and_model(self, app_client, db_session_factory):
        day = await _seed_rollup(db_session_factory)
        got = _dims_cost(self._get(app_client, group_by="day,model", provider="pgx-local"))
        assert got[(("day", day.isoformat()), ("model", "qwen"))] == "9.000000"
        assert got[(("day", (day - timedelta(days=1)).isoformat()), ("model", "qwen"))] == (
            "2.000000"
        )

    async def test_empty_group_by_is_one_total_row(self, app_client, db_session_factory):
        await _seed_rollup(db_session_factory)
        body = self._get(app_client, group_by="")
        assert [r["cost_usd"] for r in body["data"]["rows"]] == ["31.000000"]
        assert body["data"]["rows"][0]["dims"] == {}

    async def test_filters(self, app_client, db_session_factory):
        await _seed_rollup(db_session_factory)
        assert _dims_cost(self._get(app_client, group_by="actor", funding="tenant_byo")) == {
            (("actor", str(BOB)),): "4.000000"
        }
        assert _dims_cost(self._get(app_client, group_by="funding", project_id="none")) == {
            (("funding", "unknown"),): "8.000000"
        }
        assert _dims_cost(self._get(app_client, group_by="project", app="fr")) == {
            (("project", str(PROJ_1)),): "3.000000"
        }
        # An unregistered key matches NOTHING; it never widens to all apps.
        body = self._get(app_client, group_by="project", app="no-such-app")
        assert body["data"]["rows"] == []
        assert Decimal(body["data"]["totals"]["cost_usd"]) == 0
        assert _dims_cost(
            self._get(app_client, group_by="model", actor_id=f"{ALICE},{BOB}", model="gpt,sonnet")
        ) == {(("model", "sonnet"),): "16.000000", (("model", "gpt"),): "4.000000"}

    async def test_window_excludes_out_of_range_days(self, app_client, db_session_factory):
        day = await _seed_rollup(db_session_factory)
        body = self._get(
            app_client, group_by="", **{"from": day.isoformat(), "to": day.isoformat()}
        )
        assert Decimal(body["data"]["totals"]["cost_usd"]) == Decimal("29")

    @pytest.mark.parametrize(
        "params",
        [
            {"group_by": "project,colour"},
            {"funding": "free"},
            {"project_id": "not-a-uuid"},
            {"from": "2026-10-05", "to": "2026-10-01"},
            {"from": "2026-01-01", "to": "2026-10-01"},
        ],
    )
    async def test_bad_params_are_422(self, app_client, params):
        r = app_client.get(PATH, params={"tenant_id": TENANT_A, **params}, headers=BEARER)
        assert r.status_code == 422, r.text


@pytest.mark.integration
class TestMe:
    async def test_me_is_the_callers_actor_in_the_active_tenant_only(
        self, app_client, db_session_factory
    ):
        """⚠️ MUTATION TARGET: drop the actor filter -> Bob's 4 and the unattributed 8
        appear; drop the tenant filter -> tenant B's 1000 appears."""
        await _seed_rollup(db_session_factory)
        r = app_client.get(
            ME,
            params={"group_by": "project,funding"},
            headers={"X-Tenant-Id": TENANT_A, "X-Actor-Id": str(ALICE)},
        )
        assert r.status_code == 200, r.text
        data = r.json()["data"]
        assert data["actor_id"] == str(ALICE)
        assert Decimal(data["totals"]["cost_usd"]) == Decimal("19")
        assert _dims_cost(r.json()) == {
            (("funding", "platform_external"), ("project", str(PROJ_2))): "16.000000",
            (("funding", "local_gpu"), ("project", str(PROJ_1))): "3.000000",
        }

    async def test_me_needs_an_actor(self, app_client):
        r = app_client.get(ME, headers={"X-Tenant-Id": TENANT_A})
        assert r.status_code == 401, r.text
        r = app_client.get(ME, headers={"X-Tenant-Id": TENANT_A, "X-Actor-Id": "nope"})
        assert r.status_code == 401, r.text
        r = app_client.get(ME, headers={"X-Tenant-Id": TENANT_A, "X-Actor-Id": str(NULL_ACTOR_ID)})
        assert r.status_code == 401, r.text

    async def test_me_needs_a_tenant(self, app_client):
        r = app_client.get(ME, headers={"X-Actor-Id": str(ALICE)})
        assert r.status_code == 400, r.text


# ---------------------------------------------------------------------------
# 6. Retention
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_usage_rollup_retention_deletes_only_rows_before_the_cutoff(db_session_factory):
    today = date(2026, 10, 7)
    async with db_session_factory() as s:
        for d in (date(2024, 8, 31), date(2024, 10, 1)):
            s.add(
                CostUsageDaily(
                    bucket_date=d,
                    tenant_id=TENANT_A,
                    funding="local_gpu",
                    provider="p",
                    served_model="m",
                    event_count=1,
                    total_input_tokens=0,
                    total_output_tokens=0,
                    total_cost_usd=Decimal("1"),
                )
            )
        await s.commit()
    assert await purge_old_usage_rollup(db_session_factory, 25, today=today) == 1
    async with db_session_factory() as s:
        left = (await s.execute(select(CostUsageDaily.bucket_date))).scalars().all()
    assert left == [date(2024, 10, 1)]
