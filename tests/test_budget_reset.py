"""``POST /api/budget/reset/{tenant_id}`` — the operator's budget reset.

``token_budgets.consumed_tokens`` is the one budget counter: it is what
``/api/budget/status`` reports and what ``/api/budget/check`` (license-authz's
budget gate) decides on. These tests pin WHICH NUMBER the operator is told was cleared. The status
endpoint reads current-period consumption through ``effective_consumed`` (a
monthly row whose stored consumption belongs to an earlier period already
reports 0), so the reset must report the same figure. Reporting the raw column
would tell an operator they cleared spend the screen said had already rolled
over.

⛔ AUTH IS THE INTERNAL GATE, NOT TENANT CONTEXT. Every member of a tenant
holds a licence token, and ext_authz turns one into an ``X-Tenant-Id`` header;
tenant isolation compares those two values, so it would happily authorise a
tenant resetting its OWN consumption. The route depends on ``require_internal``
and the tests below refuse both a plain non-internal caller and that exact
tenant-context regression.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import AuditLog, TokenBudget
from services.budget_period import period_anchor
from tests._budget_factory import make_budget
from tests.conftest import T

# --- Fixtures --------------------------------------------------------------


@pytest.fixture
def tenant_member_actor(monkeypatch):
    """Force the bearer decoder to a NON-internal caller (a tenant member).

    A licence token carries tenant roles, never an internal service-account
    role. Patching the decoder is the only way to exercise the refusal: the
    suite runs with ``DASHBOARD_AUTH_ENABLED=false``, whose dev bypass returns
    ``billing-internal`` and would let every caller through.
    """
    fake_payload = {
        "sub": "tenant-member",
        "realm_access": {"roles": ["tenant-admin"]},
    }

    async def _fake_decode(_request):
        return fake_payload

    import auth
    import auth_deps

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


# --- Helpers ---------------------------------------------------------------


async def _seed(db_engine, *budgets: TokenBudget) -> None:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        session.add_all(budgets)
        await session.commit()


async def _load(db_engine, budget_id: uuid.UUID) -> TokenBudget:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        result = await session.execute(select(TokenBudget).where(TokenBudget.id == budget_id))
        return result.scalar_one()


async def _audit_rows(db_engine, tenant_id: str) -> list[AuditLog]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        result = await session.execute(
            select(AuditLog)
            .where(AuditLog.tenant_id == tenant_id, AuditLog.action == "budget.reset")
            .order_by(AuditLog.ts)
        )
        return list(result.scalars().all())


# --- Behaviour -------------------------------------------------------------


@pytest.mark.unit
class TestResetTenantBudget:
    async def test_resets_only_active_rows(self, app_client, db_engine):
        """A soft-deleted budget keeps its consumption: it is not in the pool.

        ``active=false`` rows are history. Zeroing them would rewrite a closed
        record and, worse, make the UI's history view disagree with itself.
        """
        tenant = T("reset-active-only")
        active = make_budget(tenant=tenant, cap=1_000, consumed=250)
        inactive = make_budget(tenant=tenant, cap=1_000, consumed=700)
        inactive.active = False
        await _seed(db_engine, active, inactive)

        r = app_client.post(f"/api/budget/reset/{tenant}", json={})
        assert r.status_code == 200, r.text
        data = r.json()["data"]
        assert data["tenant_id"] == tenant
        assert data["reset_count"] == 1
        assert data["budget_ids"] == [str(active.id)]
        assert data["previous_consumed_total"] == 250

        assert int((await _load(db_engine, active.id)).consumed_tokens) == 0
        assert int((await _load(db_engine, inactive.id)).consumed_tokens) == 700

    async def test_zeroes_current_period_consumption_and_reports_what_it_cleared(
        self, app_client, db_engine
    ):
        tenant = T("reset-current")
        budget = make_budget(tenant=tenant, cap=10_000, consumed=2_500, period="monthly")
        await _seed(db_engine, budget)

        before = app_client.get(f"/api/budget/status/{tenant}")
        assert before.status_code == 200, before.text
        assert before.json()["data"]["budgets"][0]["used_tokens"] == 2_500

        r = app_client.post(f"/api/budget/reset/{tenant}", json={"reason": "goodwill"})
        assert r.status_code == 200, r.text
        data = r.json()["data"]
        assert data["reset_count"] == 1
        assert data["previous_consumed_total"] == 2_500

        row = await _load(db_engine, budget.id)
        assert int(row.consumed_tokens) == 0
        assert row.consumed_period_start == period_anchor("monthly", datetime.now(UTC))

        # The status endpoint the operator just looked at now agrees.
        after = app_client.get(f"/api/budget/status/{tenant}")
        assert after.json()["data"]["budgets"][0]["used_tokens"] == 0
        assert after.json()["data"]["total_remaining"] == 10_000

    async def test_reports_the_effective_consumption_not_the_stale_column(
        self, app_client, db_engine
    ):
        """A rolled-over row: raw column 5000 from LAST period, screen says 0.

        The reset must report 0 — the same reader the status endpoint uses —
        and its audit ``before`` must agree, or the trail records a number the
        admin never saw.
        """
        tenant = T("reset-stale-period")
        current_anchor = period_anchor("monthly", datetime.now(UTC))
        assert current_anchor is not None
        stale_anchor = current_anchor - timedelta(days=1)
        budget = make_budget(
            tenant=tenant,
            cap=10_000,
            consumed=5_000,
            period="monthly",
            consumed_period_start=stale_anchor,
        )
        await _seed(db_engine, budget)

        before = app_client.get(f"/api/budget/status/{tenant}")
        assert before.json()["data"]["budgets"][0]["used_tokens"] == 0

        r = app_client.post(f"/api/budget/reset/{tenant}", json={})
        assert r.status_code == 200, r.text
        assert r.json()["data"]["previous_consumed_total"] == 0

        row = await _load(db_engine, budget.id)
        assert int(row.consumed_tokens) == 0
        assert row.consumed_period_start == period_anchor("monthly", datetime.now(UTC))

        (audit,) = await _audit_rows(db_engine, tenant)
        audit_before = audit.before
        audit_after = audit.after
        assert audit_before is not None
        assert audit_after is not None
        assert audit_before["consumed_tokens"] == 0
        assert audit_before["consumed_period_start"] == stale_anchor.isoformat()
        assert audit_after["consumed_tokens"] == 0

    async def test_leaves_a_lifetime_budget_without_a_period_anchor(self, app_client, db_engine):
        """Only monthly/weekly have period semantics; anything else is lifetime.

        A lifetime budget resets its consumption but must NOT acquire an anchor
        — an anchor is what makes a row "roll over", and inventing one would
        silently turn a lifetime cap into a monthly one on the next read.
        """
        tenant = T("reset-lifetime")
        budget = make_budget(
            tenant=tenant, cap=5_000, consumed=1_200, period="lifetime", consumed_period_start=None
        )
        await _seed(db_engine, budget)

        r = app_client.post(f"/api/budget/reset/{tenant}", json={})
        assert r.status_code == 200, r.text
        assert r.json()["data"]["previous_consumed_total"] == 1_200

        row = await _load(db_engine, budget.id)
        assert int(row.consumed_tokens) == 0
        assert row.consumed_period_start is None

    async def test_writes_one_audit_row_per_budget(self, app_client, db_engine):
        """Per ROW, not one summary: the trail must name each budget it changed."""
        tenant = T("reset-audit")
        first = make_budget(tenant=tenant, cap=1_000, consumed=100, priority=10)
        second = make_budget(tenant=tenant, cap=2_000, consumed=400, priority=20)
        await _seed(db_engine, first, second)

        r = app_client.post(f"/api/budget/reset/{tenant}", json={"reason": "support ticket 42"})
        assert r.status_code == 200, r.text

        rows = await _audit_rows(db_engine, tenant)
        assert len(rows) == 2
        by_id = {row.resource_id: row for row in rows}
        assert set(by_id) == {str(first.id), str(second.id)}
        for budget in (first, second):
            audit = by_id[str(budget.id)]
            audit_before = audit.before
            audit_after = audit.after
            assert audit.resource_type == "budget"
            assert audit.tenant_id == tenant
            assert audit_before is not None
            assert audit_after is not None
            assert audit_before["consumed_tokens"] == int(budget.consumed_tokens)
            assert audit_after["consumed_tokens"] == 0
            assert audit_after["reason"] == "support ticket 42"

    async def test_404_when_the_tenant_has_no_active_budget(self, app_client, db_engine):
        tenant = T("reset-none")
        r = app_client.post(f"/api/budget/reset/{tenant}", json={})
        assert r.status_code == 404, r.text
        assert r.json()["error"]["message"] == "no_active_budgets"

    async def test_404_and_untouched_when_only_inactive_rows_exist(self, app_client, db_engine):
        tenant = T("reset-only-inactive")
        history = make_budget(tenant=tenant, cap=1_000, consumed=900)
        history.active = False
        await _seed(db_engine, history)

        r = app_client.post(f"/api/budget/reset/{tenant}", json={})
        assert r.status_code == 404, r.text
        assert r.json()["error"]["message"] == "no_active_budgets"
        assert int((await _load(db_engine, history.id)).consumed_tokens) == 900

    async def test_a_caller_without_an_internal_role_is_refused(
        self, app_client, db_engine, tenant_member_actor
    ):
        """The write gate is a SERVICE-ACCOUNT role, like the other budget writes.

        A tenant-scoped token — a licence token, which every member of a tenant
        holds — carries no internal service role, so it cannot reach a reset
        even for a tenant it owns.
        """
        tenant = T("reset-no-internal-role")
        budget = make_budget(tenant=tenant, cap=1_000, consumed=300)
        await _seed(db_engine, budget)

        r = app_client.post(
            f"/api/budget/reset/{tenant}",
            json={},
            headers={"Authorization": "Bearer licence-token"},
        )
        assert r.status_code == 403, r.text
        assert r.json()["error"]["message"] == "Internal service role required"

        assert int((await _load(db_engine, budget.id)).consumed_tokens) == 300
        assert await _audit_rows(db_engine, tenant) == []

    async def test_a_tenant_context_caller_cannot_reset_its_own_consumption(
        self, app_client, db_engine, tenant_member_actor
    ):
        """⛔ THE REGRESSION THIS ROUTE EXISTS TO BE SAFE AGAINST.

        A licence token plus the ``X-Tenant-Id`` ext_authz derives from it,
        naming the SAME tenant as the path. Tenant ISOLATION cannot refuse this
        — the two values match — so a ``require_tenant`` + isolation pair would
        authorise it. Only the internal role gate stands between a tenant and
        handing itself back its whole allowance (free money), and nothing in
        the request's tenant context can satisfy that gate.
        """
        tenant = T("reset-self-serve")
        budget = make_budget(tenant=tenant, cap=1_000, consumed=800)
        await _seed(db_engine, budget)

        r = app_client.post(
            f"/api/budget/reset/{tenant}",
            json={"reason": "self-service"},
            headers={"Authorization": "Bearer licence-token", "X-Tenant-Id": tenant},
        )
        assert r.status_code == 403, r.text
        assert int((await _load(db_engine, budget.id)).consumed_tokens) == 800
        assert await _audit_rows(db_engine, tenant) == []

    async def test_path_accepts_a_registered_tenant_name(self, app_client, db_engine):
        """Same defect class as ``/status`` and ``/list``: the path value must
        be resolved before it reaches a ``uuid`` column, or a registered tenant
        NAME binds as an unparseable literal. ``require_internal`` authorises
        the caller and names no tenant, so nothing upstream did this.
        """
        from tenancy import LEGACY_TENANT_NAMES

        name = "n8n-dev"
        tenant = LEGACY_TENANT_NAMES[name]
        budget = make_budget(tenant=tenant, cap=1_000, consumed=400)
        await _seed(db_engine, budget)

        r = app_client.post(f"/api/budget/reset/{name}", json={})
        assert r.status_code == 200, r.text
        assert r.json()["data"]["tenant_id"] == tenant
        assert int((await _load(db_engine, budget.id)).consumed_tokens) == 0


@pytest.mark.unit
class TestResetIsNotTheDeleteRoute:
    async def test_reset_does_not_deactivate_the_budget(self, app_client, db_engine):
        """Regression guard on the obvious confusion: reset ≠ delete.

        Both mutate the same row, and a reset that also flipped ``active``
        would look correct in the response body while silently removing the
        tenant's cap — the opposite of what an operator granting allowance
        asked for.
        """
        tenant = T("reset-not-delete")
        budget = make_budget(tenant=tenant, cap=1_000, consumed=800)
        await _seed(db_engine, budget)

        r = app_client.post(f"/api/budget/reset/{tenant}", json={})
        assert r.status_code == 200, r.text

        row = await _load(db_engine, budget.id)
        assert row.active is True
        assert row.updated_at is not None
        assert Decimal(row.consumed_tokens) == 0
