"""Audit-log tests — every WRITE endpoint must emit a row in the same TX.

The audit row's actor identity comes from ``request.state.audit_actor`` set
by the auth deps; the request_id comes from the RequestIdMiddleware. These
tests verify both, plus transaction integrity (audit and mutation commit or
roll back together) and the GET /api/audit/recent read endpoint.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import AuditLog, TokenBudget
from tests.conftest import T

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def admin_actor(monkeypatch):
    """Force require_admin / require_internal to a known JWT payload.

    Returns the dict used so tests can assert against actor_sub/role.
    """
    fake_payload = {
        "sub": "test-admin-user",
        "realm_access": {"roles": ["license-admin"]},
    }

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


@pytest.fixture
def internal_actor(monkeypatch):
    """Force require_internal to a known billing-internal SA payload."""
    fake_payload = {
        "sub": "billing-sa",
        "realm_access": {"roles": ["billing-internal"]},
    }

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


# ---------------------------------------------------------------------------
# Per-route audit assertions
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBudgetAuditRows:
    async def test_create_budget_emits_audit(
        self, app_client: TestClient, db_engine, internal_actor
    ):
        resp = app_client.post(
            "/api/budget",
            headers={"Authorization": "Bearer fake", "X-Request-ID": "req-create-1"},
            json={"tenant_id": T("audit-t"), "budget_tokens": 1000, "period": "monthly"},
        )
        assert resp.status_code == 201, resp.text
        budget_id = resp.json()["data"]["id"]
        assert resp.headers["X-Request-ID"] == "req-create-1"

        rows = await _fetch_audit(db_engine, action="budget.create")
        assert len(rows) == 1
        row = rows[0]
        assert row.actor_sub == internal_actor["sub"]
        assert row.actor_role == "billing-internal"
        assert row.resource_type == "budget"
        assert row.resource_id == budget_id
        assert row.tenant_id == T("audit-t")
        assert row.before is None
        assert row.after is not None
        assert row.after["tenant_id"] == T("audit-t")
        assert row.after["budget_tokens"] == 1000
        assert row.request_id == "req-create-1"

    async def test_create_budget_license_sync_upsert_emits_update_audit(
        self, app_client: TestClient, db_engine, internal_actor
    ):
        """First license_sync POST emits budget.create; second one (same
        tenant/license/period) emits budget.update — NOT a second create.
        Regression guard for the duplicate-budget bug."""
        lic_id = str(uuid.uuid4())
        body = {
            "tenant_id": T("ls-audit"),
            "license_id": lic_id,
            "budget_tokens": 100_000,
            "period": "monthly",
            "source": "license_sync",
        }
        first = app_client.post(
            "/api/budget",
            headers={"Authorization": "Bearer fake"},
            json=body,
        )
        assert first.status_code == 201
        first_id = first.json()["data"]["id"]

        second = app_client.post(
            "/api/budget",
            headers={"Authorization": "Bearer fake"},
            json={**body, "budget_tokens": 250_000},
        )
        assert second.status_code == 201
        assert second.json()["data"]["id"] == first_id  # same row reconciled

        creates = await _fetch_audit(db_engine, action="budget.create")
        updates = await _fetch_audit(db_engine, action="budget.update")
        assert len(creates) == 1
        assert creates[0].resource_id == first_id
        assert len(updates) == 1
        assert updates[0].resource_id == first_id
        assert updates[0].after is not None
        assert updates[0].after["budget_tokens"] == 250_000

        # And there is exactly one TokenBudget row for the tenant.
        rows = await _fetch_budgets_by_tenant(db_engine, T("ls-audit"))
        assert len(rows) == 1
        assert str(rows[0].id) == first_id
        assert rows[0].budget_tokens == 250_000

    async def test_topup_emits_audit(
        self, app_client: TestClient, db_engine, internal_actor, topup_enabled
    ):
        resp = app_client.post(
            f"/api/budget/topup/{T('topup-tenant')}",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("ignored"), "budget_tokens": 5000, "period": "monthly"},
        )
        assert resp.status_code == 201, resp.text

        rows = await _fetch_audit(db_engine, action="budget.topup")
        assert len(rows) == 1
        row = rows[0]
        assert row.actor_sub == internal_actor["sub"]
        assert row.tenant_id == T("topup-tenant")  # path wins over body
        assert row.after is not None
        assert row.after["source"] == "topup"
        # auto-generated request_id when client doesn't supply one
        assert row.request_id is not None and len(row.request_id) > 0

    async def test_topup_refused_409_when_disabled_and_writes_nothing(
        self, app_client: TestClient, db_engine, internal_actor, monkeypatch
    ):
        """No free top-ups (George 2026-10-02): flag off -> 409, and NOTHING written.

        The default is off, so this test sets nothing — it pins the DEFAULT, not
        just an explicit "false". A 409 that still inserted the row, wrote the
        audit entry would be the same free top-up with a louder status code, so
        each write surface is asserted separately.
        """
        tenant = T("topup-off")

        resp = app_client.post(
            f"/api/budget/topup/{tenant}",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": tenant, "budget_tokens": 5000, "period": "monthly"},
        )

        assert resp.status_code == 409, resp.text
        err = resp.json()["error"]
        assert err["details"]["code"] == "topup_unavailable"
        assert err["details"]["message"] == (
            "Top-ups are unavailable until payment processing is enabled."
        )
        assert await _fetch_budgets_by_tenant(db_engine, tenant) == []
        assert await _fetch_audit(db_engine, action="budget.topup") == []

    async def test_topup_auth_still_checked_before_the_gate(
        self, app_client: TestClient, db_engine, monkeypatch
    ):
        """The gate must not mask auth: a non-internal caller is still 403, not 409."""
        import auth

        async def _fake_decode(_request):
            return {"sub": "tenant-user", "realm_access": {"roles": ["customer"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)

        resp = app_client.post(
            f"/api/budget/topup/{T('topup-auth')}",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("topup-auth"), "budget_tokens": 5000, "period": "monthly"},
        )
        assert resp.status_code == 403, resp.text

    async def test_delete_budget_emits_audit_with_before(
        self, app_client: TestClient, db_engine, internal_actor
    ):
        # First create
        resp = app_client.post(
            "/api/budget",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("del-t"), "budget_tokens": 9000, "period": "monthly"},
        )
        assert resp.status_code == 201
        budget_id = resp.json()["data"]["id"]

        del_resp = app_client.delete(
            f"/api/budget/{budget_id}",
            headers={"Authorization": "Bearer fake"},
        )
        assert del_resp.status_code == 204

        rows = await _fetch_audit(db_engine, action="budget.delete")
        assert len(rows) == 1
        row = rows[0]
        assert row.resource_id == budget_id
        assert row.tenant_id == T("del-t")
        assert row.before is not None
        assert row.before["budget_tokens"] == 9000
        assert row.before["active"] is True
        assert row.after is None

    async def test_sync_from_licenses_emits_summary_audit(
        self, app_client: TestClient, db_engine, fake_redis, monkeypatch, internal_actor
    ):
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)

        lic_id = str(uuid.uuid4())
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {
                "id": lic_id,
                "tenant_id": T("sync-audit-t"),
                "plan": {"token_budget_monthly": 10_000, "quota_monthly": 500},
            },
        ]
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            resp = app_client.post(
                "/api/budget/sync-from-licenses",
                headers={"Authorization": "Bearer fake"},
            )
        assert resp.status_code == 200, resp.text

        # Per-budget create rows + one summary row
        creates = await _fetch_audit(db_engine, action="budget.create")
        assert len(creates) == 1
        assert creates[0].tenant_id == T("sync-audit-t")

        summary = await _fetch_audit(db_engine, action="budget.sync_from_licenses")
        assert len(summary) == 1
        assert summary[0].after is not None
        assert summary[0].after["created_count"] == 1
        assert summary[0].after["license_ids"] == [creates[0].resource_id]


@pytest.mark.unit
class TestQuotaAuditRows:
    async def test_set_quota_config_emits_audit(
        self, app_client: TestClient, db_engine, fake_redis, monkeypatch, internal_actor
    ):
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)

        lic_id = str(uuid.uuid4())
        resp = app_client.post(
            "/api/quota/config",
            headers={"Authorization": "Bearer fake", "X-Request-ID": "req-quota-1"},
            json={"license_id": lic_id, "quota_monthly": 12345, "tenant_id": T("quota-tenant")},
        )
        assert resp.status_code == 201, resp.text

        rows = await _fetch_audit(db_engine, action="quota.config.set")
        assert len(rows) == 1
        row = rows[0]
        assert row.resource_type == "quota_config"
        assert row.resource_id == T("quota-tenant")
        assert row.tenant_id == T("quota-tenant")
        assert row.actor_sub == internal_actor["sub"]
        assert row.actor_role == "billing-internal"
        assert row.after == {"license_id": lic_id, "quota_monthly": 12345}
        assert row.request_id == "req-quota-1"


@pytest.mark.unit
class TestConfigAuditRows:
    async def test_provider_upsert_create_then_update(
        self, app_client: TestClient, db_engine, admin_actor
    ):
        # Create
        resp1 = app_client.put(
            "/api/config/providers/openai",
            headers={"Authorization": "Bearer fake"},
            json={"input_per_1m_usd": "1.50", "output_per_1m_usd": "3.00"},
        )
        assert resp1.status_code == 200, resp1.text

        # Update
        resp2 = app_client.put(
            "/api/config/providers/openai",
            headers={"Authorization": "Bearer fake"},
            json={"input_per_1m_usd": "2.00", "output_per_1m_usd": "4.00"},
        )
        assert resp2.status_code == 200

        rows = await _fetch_audit(db_engine, action="config.provider.upsert")
        assert len(rows) == 2
        # Newest first (ts DESC)
        update_row, create_row = rows[0], rows[1]

        from decimal import Decimal

        assert create_row.before is None
        assert create_row.after is not None
        assert create_row.after["provider"] == "openai"
        assert Decimal(create_row.after["input_per_1m_usd"]) == Decimal("1.50")
        assert create_row.actor_sub == admin_actor["sub"]
        assert create_row.actor_role == "license-admin"

        assert update_row.before is not None
        assert update_row.after is not None
        assert Decimal(update_row.before["input_per_1m_usd"]) == Decimal("1.50")
        assert Decimal(update_row.after["input_per_1m_usd"]) == Decimal("2.00")

    async def test_app_upsert_audit(self, app_client: TestClient, db_engine, admin_actor):
        resp = app_client.put(
            "/api/config/apps/workers",
            headers={"Authorization": "Bearer fake"},
            json={"cost_per_request_usd": "0.001"},
        )
        assert resp.status_code == 200, resp.text
        rows = await _fetch_audit(db_engine, action="config.app.upsert")
        assert len(rows) == 1
        assert rows[0].resource_id == "workers"
        assert rows[0].actor_role == "license-admin"

    async def test_tenant_upsert_audit(self, app_client: TestClient, db_engine, admin_actor):
        resp = app_client.put(
            f"/api/config/tenants/{T('acme')}",
            headers={"Authorization": "Bearer fake"},
            json={"markup_pct": 25.0, "notes": "VIP"},
        )
        assert resp.status_code == 200, resp.text
        rows = await _fetch_audit(db_engine, action="config.tenant.upsert")
        assert len(rows) == 1
        assert rows[0].tenant_id == T("acme")
        assert rows[0].resource_type == "tenant_config"
        assert rows[0].resource_id == T("acme")
        from decimal import Decimal

        assert rows[0].after is not None
        assert Decimal(rows[0].after["markup_pct"]) == Decimal("25")

    async def test_markup_audit(self, app_client: TestClient, db_engine, admin_actor):
        resp = app_client.put(
            "/api/config/markup",
            headers={"Authorization": "Bearer fake"},
            json={"default_markup_pct": 33.0},
        )
        assert resp.status_code == 200, resp.text
        rows = await _fetch_audit(db_engine, action="config.markup.set")
        assert len(rows) == 1
        assert rows[0].resource_id == "global"
        from decimal import Decimal

        assert rows[0].after is not None
        assert Decimal(rows[0].after["markup_pct"]) == Decimal("33")


# ---------------------------------------------------------------------------
# Transaction integrity
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAuditTransactionIntegrity:
    async def test_mutation_failure_rolls_back_audit(
        self, app_client: TestClient, db_engine, internal_actor, monkeypatch
    ):
        """If commit fails after audit_record was issued, the audit row must
        NOT survive — it shares the transaction with the mutation.

        Starlette's BaseHTTPMiddleware re-raises uncaught endpoint
        exceptions through the test client, so we use ``pytest.raises``
        rather than asserting a 500 status. Production traffic still
        sees a 500 because Starlette's outer exception handler converts
        unhandled errors to a 500 response, but BaseHTTPMiddleware
        re-raises into the test transport.
        """

        async def _fail_commit(self):
            raise RuntimeError("simulated commit failure")

        monkeypatch.setattr(AsyncSession, "commit", _fail_commit)

        with pytest.raises(RuntimeError, match="simulated commit failure"):
            app_client.post(
                "/api/budget",
                headers={"Authorization": "Bearer fake"},
                json={"tenant_id": T("rb-t"), "budget_tokens": 999, "period": "monthly"},
            )

        # Restore real commit so assertions can read DB state.
        monkeypatch.undo()

        # Neither the budget nor the audit row should exist.
        budgets = await _fetch_budgets_by_tenant(db_engine, T("rb-t"))
        assert budgets == []
        rows = await _fetch_audit(db_engine, action="budget.create")
        assert rows == []

    async def test_audit_failure_rolls_back_mutation(
        self, app_client: TestClient, db_engine, internal_actor, monkeypatch
    ):
        """If audit_record raises, the mutation must roll back too — we
        never want the API to claim success while the audit trail says
        nothing happened.
        """
        from services import audit as audit_mod

        async def _boom_record(*_a, **_kw):
            raise RuntimeError("audit pipeline down")

        monkeypatch.setattr(audit_mod, "record", _boom_record)
        # The router imported `record` under the alias `audit_record`, so
        # patching the symbol on the router module too.
        from routers import budget as budget_router_mod

        monkeypatch.setattr(budget_router_mod, "audit_record", _boom_record)

        with pytest.raises(RuntimeError, match="audit pipeline down"):
            app_client.post(
                "/api/budget",
                headers={"Authorization": "Bearer fake"},
                json={"tenant_id": T("audit-fail-t"), "budget_tokens": 777, "period": "monthly"},
            )

        monkeypatch.undo()

        budgets = await _fetch_budgets_by_tenant(db_engine, T("audit-fail-t"))
        assert budgets == []
        rows = await _fetch_audit(db_engine, action="budget.create")
        assert all(r.tenant_id != T("audit-fail-t") for r in rows)


# ---------------------------------------------------------------------------
# /api/audit/recent
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAuditRecentEndpoint:
    async def test_returns_rows_newest_first(self, app_client: TestClient, db_engine, admin_actor):
        await _seed_audit(
            db_engine,
            [
                {"action": "a.x", "tenant_id": T("t1"), "ts_offset_seconds": 0},
                {"action": "a.y", "tenant_id": T("t1"), "ts_offset_seconds": -10},
                {"action": "a.z", "tenant_id": T("t2"), "ts_offset_seconds": -20},
            ],
        )

        resp = app_client.get(
            "/api/audit/recent",
            headers={"Authorization": "Bearer fake"},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert [r["action"] for r in data] == ["a.x", "a.y", "a.z"]

    async def test_filter_by_tenant_id(self, app_client: TestClient, db_engine, admin_actor):
        await _seed_audit(
            db_engine,
            [
                {"action": "a.x", "tenant_id": T("t1"), "ts_offset_seconds": 0},
                {"action": "a.y", "tenant_id": T("t2"), "ts_offset_seconds": -1},
            ],
        )
        resp = app_client.get(
            f"/api/audit/recent?tenant_id={T('t1')}",
            headers={"Authorization": "Bearer fake"},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert len(data) == 1
        assert data[0]["tenant_id"] == T("t1")

    async def test_filter_by_action(self, app_client: TestClient, db_engine, admin_actor):
        await _seed_audit(
            db_engine,
            [
                {"action": "a.x", "tenant_id": T("t1"), "ts_offset_seconds": 0},
                {"action": "a.y", "tenant_id": T("t1"), "ts_offset_seconds": -1},
            ],
        )
        resp = app_client.get(
            "/api/audit/recent?action=a.x",
            headers={"Authorization": "Bearer fake"},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert [r["action"] for r in data] == ["a.x"]

    async def test_filter_by_since(self, app_client: TestClient, db_engine, admin_actor):
        # Seed three rows; only newer-than-since rows should be returned.
        now = datetime.now(UTC)
        await _seed_audit_with_explicit_ts(
            db_engine,
            [
                ("a.x", T("t1"), now - timedelta(seconds=5)),
                ("a.y", T("t1"), now - timedelta(minutes=10)),
                ("a.z", T("t1"), now - timedelta(hours=1)),
            ],
        )
        since = (now - timedelta(minutes=15)).isoformat()
        resp = app_client.get(
            "/api/audit/recent",
            params={"since": since},
            headers={"Authorization": "Bearer fake"},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert {r["action"] for r in data} == {"a.x", "a.y"}

    async def test_pagination(self, app_client: TestClient, db_engine, admin_actor):
        await _seed_audit(
            db_engine,
            [{"action": f"a.{i}", "tenant_id": T("t1"), "ts_offset_seconds": -i} for i in range(5)],
        )
        # First page
        page1 = app_client.get(
            "/api/audit/recent?limit=2&offset=0",
            headers={"Authorization": "Bearer fake"},
        ).json()["data"]
        assert len(page1) == 2
        assert [r["action"] for r in page1] == ["a.0", "a.1"]

        # Second page
        page2 = app_client.get(
            "/api/audit/recent?limit=2&offset=2",
            headers={"Authorization": "Bearer fake"},
        ).json()["data"]
        assert [r["action"] for r in page2] == ["a.2", "a.3"]

    async def test_non_admin_rejected(self, app_client: TestClient, db_engine, monkeypatch):
        """A token without license-admin role gets 403."""
        import auth

        async def _fake_decode(_request):
            return {"sub": "tenant-user", "realm_access": {"roles": ["customer"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)

        resp = app_client.get(
            "/api/audit/recent",
            headers={"Authorization": "Bearer fake"},
        )
        assert resp.status_code == 403

    async def test_limit_validation(self, app_client: TestClient, admin_actor):
        # limit must be >= 1
        resp = app_client.get(
            "/api/audit/recent?limit=0",
            headers={"Authorization": "Bearer fake"},
        )
        assert resp.status_code == 422
        # limit must be <= 1000
        resp2 = app_client.get(
            "/api/audit/recent?limit=1001",
            headers={"Authorization": "Bearer fake"},
        )
        assert resp2.status_code == 422


# ---------------------------------------------------------------------------
# RequestIdMiddleware
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRequestIdMiddleware:
    def test_generates_when_absent(self, app_client: TestClient):
        resp = app_client.get("/health")
        rid = resp.headers.get("X-Request-ID")
        assert rid is not None
        # Phase 1 swapped the inline uuid4().hex[:12] middleware for the
        # canonical uuid7() str() — 36 chars in standard UUID textual form.
        import uuid

        uuid.UUID(rid)  # parses as a UUID
        assert len(rid) == 36

    def test_echoes_supplied_value(self, app_client: TestClient):
        resp = app_client.get("/health", headers={"X-Request-ID": "client-supplied"})
        assert resp.headers["X-Request-ID"] == "client-supplied"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _fetch_audit(db_engine, *, action: str | None = None) -> list[AuditLog]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        stmt = select(AuditLog)
        if action is not None:
            stmt = stmt.where(AuditLog.action == action)
        stmt = stmt.order_by(AuditLog.ts.desc())
        result = await session.execute(stmt)
        return list(result.scalars().all())


async def _fetch_budgets_by_tenant(db_engine, tenant_id: str) -> list[TokenBudget]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        stmt = select(TokenBudget).where(TokenBudget.tenant_id == tenant_id)
        result = await session.execute(stmt)
        return list(result.scalars().all())


async def _seed_audit(db_engine, specs: list[dict]) -> None:
    """Insert audit rows with offsets relative to now()."""
    now = datetime.now(UTC)
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        for spec in specs:
            ts = now + timedelta(seconds=spec["ts_offset_seconds"])
            session.add(
                AuditLog(
                    ts=ts,
                    actor_sub="seeder",
                    actor_role="license-admin",
                    action=spec["action"],
                    resource_type="test",
                    resource_id=None,
                    tenant_id=spec["tenant_id"],
                    before=None,
                    after=None,
                    request_id=None,
                )
            )
        await session.commit()


async def _seed_audit_with_explicit_ts(db_engine, specs: list[tuple[str, str, datetime]]) -> None:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        for action, tenant_id, ts in specs:
            session.add(
                AuditLog(
                    ts=ts,
                    actor_sub="seeder",
                    actor_role="license-admin",
                    action=action,
                    resource_type="test",
                    tenant_id=tenant_id,
                )
            )
        await session.commit()
