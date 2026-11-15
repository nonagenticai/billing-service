"""A 0-token budget is a BLOCK-ALL budget, and it must be expressible.

``TokenBudgetCreate.budget_tokens`` was ``Field(gt=0)``, so a zero-allowance
budget — a suspended plan, a hard stop — could not be created over the API at
all, while every other billing site already treated 0 as block-all:
``_RESERVE_LUA`` guards on ``cap >= 0`` and ``sync-from-licenses`` writes on
``token_budget >= 0``. PR #68 made exactly this ``ge=0`` fix to the sibling
``QuotaConfigRequest.quota_monthly`` and never carried it to this model.

These tests pin the whole consequence set of the flip, not just the 201:

  - the row PERSISTS as 0 (there is no CHECK constraint below the model, so a
    green 201 with a mangled value would be silent);
  - a 0 cap is genuinely REACHABLE by the enforcement path — ``reserve_budget``
    denies with ``over_budget``, NOT ``no_active_budget``. That distinction is
    the whole point: ``no_active_budget`` is how "unlimited" is expressed, so a
    0-cap budget landing there would mean block-all silently reads as unlimited;
  - the watcher, whose ``budget_tokens > 0`` filter is now a load-bearing
    ZeroDivisionError guard, survives a 0-cap row;
  - the read paths report a blocked budget as 100% used, not 0%.
"""

from __future__ import annotations

import os
import uuid
from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from sqlalchemy import select

from models.db import (
    BudgetOverageAlert,
    MeteringDaily,
    TokenBudget,
    TokenUsage,
)
from services.billing import generate_billing
from services.budget import reserve_budget
from services.budget_status import check_budget
from services.budget_watcher import check_and_fire_overages
from tests._budget_factory import make_budget
from tests.conftest import T


def _budget(
    *,
    tenant: str,
    cap: int,
    consumed: int = 0,
    license_id: uuid.UUID | None = None,
    source: str = "manual",
) -> TokenBudget:
    return make_budget(
        tenant=tenant,
        cap=cap,
        consumed=consumed,
        license_id=license_id,
        period="monthly",
        source=source,
    )


def _read_budgets(tenant_id: str) -> list[dict]:
    """Sync psycopg read — the create test drives a sync TestClient."""
    import psycopg

    sync_url = os.environ["DATABASE_URL"].replace("+psycopg", "")
    with psycopg.connect(sync_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, budget_tokens, active, source FROM token_budgets WHERE tenant_id = %s",
            (tenant_id,),
        )
        return [
            {"id": r[0], "budget_tokens": r[1], "active": r[2], "source": r[3]}
            for r in cur.fetchall()
        ]


class TestCreateZeroBudget:
    @pytest.mark.unit
    def test_post_budget_accepts_zero_and_persists_it(self, app_client):
        """422 on main (``gt=0``). A 0 must be accepted AND stored as 0."""
        tenant = T(f"zero-create-{uuid.uuid4().hex[:8]}")
        r = app_client.post(
            "/api/budget",
            json={
                "tenant_id": tenant,
                "license_id": str(uuid.uuid4()),
                "budget_tokens": 0,
                "period": "monthly",
            },
        )
        assert r.status_code == 201, r.text
        assert r.json()["data"]["budget_tokens"] == 0

        # Durability: token_budgets has no CHECK constraint on budget_tokens,
        # so nothing below the model would have caught a value mangled on the
        # way down. Assert the stored row, not just the echoed response.
        rows = _read_budgets(tenant)
        assert len(rows) == 1
        assert rows[0]["budget_tokens"] == 0
        assert rows[0]["active"] is True

    @pytest.mark.unit
    def test_post_budget_still_rejects_negative(self, app_client):
        """``ge=0`` widened the floor to 0 — it must not have removed it."""
        r = app_client.post(
            "/api/budget",
            json={
                "tenant_id": T(f"neg-{uuid.uuid4().hex[:8]}"),
                "license_id": str(uuid.uuid4()),
                "budget_tokens": -1,
                "period": "monthly",
            },
        )
        assert r.status_code == 422, r.text


class TestZeroCapIsEnforced:
    @pytest.mark.unit
    async def test_zero_cap_denies_with_over_budget_not_no_active_budget(
        self, db_session, fake_redis
    ):
        """cap==0 is genuinely reachable, and it BLOCKS.

        The reason code carries the meaning. ``no_active_budget`` is how
        "unlimited" is expressed (it short-circuits before the Lua ever runs),
        so a 0-cap budget resolving to that reason would mean the most
        restrictive state in the system reads as the least restrictive one.
        """
        db_session.add(_budget(tenant=T("t-zero"), cap=0))
        await db_session.commit()

        r = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("t-zero"),
            estimated_tokens=1,
            ttl_seconds=60,
        )

        assert r["allowed"] is False
        assert r["reason"] == "over_budget"
        assert r["cap_tokens"] == 0
        assert r["reserved_tokens"] == 0

    @pytest.mark.unit
    async def test_no_budget_at_all_still_reports_no_active_budget(self, db_session, fake_redis):
        """The paired half: with NO row, the reason must still be
        ``no_active_budget``. Without this, 'always deny with over_budget'
        would satisfy the test above."""
        r = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("t-has-nothing"),
            estimated_tokens=1,
            ttl_seconds=60,
        )
        assert r["allowed"] is False
        assert r["reason"] == "no_active_budget"


class TestWatcherSurvivesZeroCap:
    @pytest.mark.unit
    async def test_zero_cap_row_does_not_crash_a_sweep(self, db_session_factory):
        """The ``budget_tokens > 0`` filter is now a divide-by-zero guard.

        A 0-cap row reaching ``usage_pct = consumed / budget_tokens`` raises
        inside ``check_and_fire_overages``, which costs the ENTIRE cycle — every
        tenant's alerting, not just that budget's. The normal budget below is
        seeded to prove the sweep actually ran to completion rather than
        returning early.

        BOTH 0-cap arms are seeded because the operands are ``Decimal`` and the
        two raise DIFFERENT exceptions: ``Decimal(0)/Decimal(0)`` is
        ``decimal.InvalidOperation`` while ``Decimal(n>0)/Decimal(0)`` is
        ``decimal.DivisionByZero``. Only the latter is a ``ZeroDivisionError``,
        so a test covering just one arm would leave the other unguarded.
        """
        async with db_session_factory() as session:
            # consumed == 0 -> decimal.InvalidOperation arm
            session.add(_budget(tenant=T("t-w-zero"), cap=0, consumed=0))
            # consumed > 0 -> decimal.DivisionByZero arm
            session.add(_budget(tenant=T("t-w-zero-used"), cap=0, consumed=500))
            session.add(_budget(tenant=T("t-w-normal"), cap=1000, consumed=1000))
            await session.commit()

        async with httpx.AsyncClient() as client:
            fired = await check_and_fire_overages(db_session_factory, http_client=client)

        # The 100%-consumed tenant crosses all three thresholds (50/80/100) and
        # has no webhooks -> three 'no_webhooks' records. A non-zero count
        # proves the sweep reached the dispatch stage; had either 0-cap row hit
        # the division, this call would have propagated the arithmetic error.
        assert fired == 3

        # Stronger than "did not crash": the 0-cap budget must be SKIPPED, not
        # merely survived. A blocked budget has no allowance to be a percentage
        # of, so it must produce no threshold alerts at all.
        async with db_session_factory() as session:
            alerts = (await session.execute(select(BudgetOverageAlert.tenant_id))).scalars().all()
        assert set(alerts) == {T("t-w-normal")}


class TestZeroCapReadPaths:
    @pytest.mark.unit
    async def test_check_budget_reports_fully_used(self):
        """0% used on a fully-blocked budget is the most reassuring number
        available for the most restrictive state there is."""
        status = await check_budget(_budget(tenant=T("t-status-zero"), cap=0))

        assert status["budget_tokens"] == 0
        assert status["over_budget"] is True
        assert status["remaining_tokens"] == 0
        assert status["usage_pct"] == 100.0

    @pytest.mark.unit
    async def test_check_budget_normal_pct_unchanged(self):
        """Guards the ordinary arm against a 'return 100.0 always' fix."""
        status = await check_budget(_budget(tenant=T("t-status-normal"), cap=1000, consumed=250))
        assert status["usage_pct"] == 25.0
        assert status["over_budget"] is False

    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_billing_statement_reports_blocked_licence_as_full(
        self, mock_fetch, db_session, test_settings
    ):
        """templates/tenant.html renders ``budget_pct or 0`` as "0% used" behind
        a GREEN bar, so a blocked licence used to look healthiest of all."""
        tenant = T("zero-billing-tenant")
        lic_id = uuid.uuid4()
        mock_fetch.return_value = [
            {
                "id": str(lic_id),
                "app": {"key": "chatbot"},
                "plan": {"code": "basic", "quota_monthly": None},
            },
        ]
        db_session.add(_budget(tenant=tenant, cap=0, license_id=lic_id))
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                service_key="chatbot",
                bucket_date=date(2026, 3, 1),
                request_count=10,
                denied_count=0,
            )
        )
        db_session.add(
            TokenUsage(
                tenant_id=tenant,
                license_id=lic_id,
                period_type="monthly",
                period_start=date(2026, 3, 1),
                input_tokens=0,
                output_tokens=0,
                total_tokens=0,
            )
        )
        await db_session.commit()

        result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        entry = result["licenses"][0]["tokens"]
        assert entry["budget_tokens"] == 0
        assert entry["budget_remaining"] == 0
        assert entry["budget_pct"] == 100.0


class TestTopupRejectsZero:
    @pytest.mark.unit
    def test_zero_topup_is_422(self, app_client, topup_enabled):
        """A 0-token 'top-up' would INVERT the caller's intent.

        It inserts an *active* 0-cap row, which makes ``_select_active_budgets``
        non-empty and so flips the tenant out of the ``no_active_budget``
        short-circuit — i.e. out of 'unlimited' — and into the cap check. The
        operator asked to grant nothing and would instead have blocked a
        previously-unrestricted tenant, through the one endpoint whose entire
        purpose is to give more.
        """
        tenant = T(f"zero-topup-{uuid.uuid4().hex[:8]}")
        r = app_client.post(
            f"/api/budget/topup/{tenant}",
            json={
                "tenant_id": tenant,
                "license_id": str(uuid.uuid4()),
                "budget_tokens": 0,
                "period": "monthly",
            },
        )
        assert r.status_code == 422, r.text

        # Assert the MESSAGE, not just the status. On main this route also
        # 422s at 0 — but from pydantic's ``gt=0``, i.e. from the very
        # constraint this change removes. A status-only assertion would be
        # green before and after while proving nothing about the explicit
        # guard, and would go quietly green again if the guard were deleted.
        assert "top-up must add allowance" in r.json()["error"]["message"]

        # And it wrote nothing: a 422 that still inserted the row would be the
        # same defect with a louder status code.
        assert _read_budgets(tenant) == []

    @pytest.mark.unit
    def test_nonzero_topup_still_works(self, app_client, topup_enabled):
        """Paired: the guard must reject 0 only, not break top-ups."""
        tenant = T(f"ok-topup-{uuid.uuid4().hex[:8]}")
        r = app_client.post(
            f"/api/budget/topup/{tenant}",
            json={
                "tenant_id": tenant,
                "license_id": str(uuid.uuid4()),
                "budget_tokens": 5_000,
                "period": "monthly",
            },
        )
        assert r.status_code == 201, r.text
        assert r.json()["data"]["budget_tokens"] == 5_000
        assert r.json()["data"]["source"] == "topup"
