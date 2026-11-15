"""Tests for token budget service."""

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from models.db import NULL_LICENSE_ID, TokenBudget, TokenUsage
from services.budget import (
    check_budget,
    create_budget,
    delete_budget,
    get_budgets_for_tenant,
)
from tests.conftest import T
from timeutil import current_month_start as _current_month_start
from timeutil import current_week_start as _current_week_start

# ===========================================================================
# create_budget
# ===========================================================================


class TestCreateBudget:
    @pytest.mark.unit
    async def test_create_budget(self, db_session):
        """Should create a new token budget and persist it in the DB."""
        budget = await create_budget(
            tenant_id=T("test-tenant"),
            budget_tokens=1_000_000,
            period="monthly",
            session=db_session,
        )
        assert budget.tenant_id == T("test-tenant")
        assert budget.budget_tokens == 1_000_000
        assert budget.period == "monthly"
        assert budget.active is True
        assert budget.includes_input is True
        assert budget.includes_output is True

        # Verify row actually exists in DB
        stmt = select(TokenBudget).where(TokenBudget.id == budget.id)
        result = await db_session.execute(stmt)
        row = result.scalar_one()
        assert row.budget_tokens == 1_000_000

    @pytest.mark.unit
    async def test_create_budget_always_inserts(self, db_session):
        """Stacked-budget semantics: each create_budget call inserts a fresh row."""
        b1 = await create_budget(
            tenant_id=T("test-tenant-2"),
            budget_tokens=500_000,
            period="monthly",
            session=db_session,
        )
        b2 = await create_budget(
            tenant_id=T("test-tenant-2"),
            budget_tokens=1_000_000,
            period="monthly",
            session=db_session,
        )
        assert b1.id != b2.id  # always inserts; never upserts
        assert b2.budget_tokens == 1_000_000

        stmt = select(TokenBudget).where(
            TokenBudget.tenant_id == T("test-tenant-2"),
            TokenBudget.period == "monthly",
        )
        result = await db_session.execute(stmt)
        rows = list(result.scalars().all())
        assert len(rows) == 2  # both rows coexist (stacked-budget)

    @pytest.mark.unit
    async def test_create_budget_with_license_inserts_each_call(self, db_session):
        """Stacked semantics: same tenant+period+license still produces two rows."""
        lic_id = uuid.uuid4()
        b1 = await create_budget(
            tenant_id=T("lic-upsert"),
            budget_tokens=5000,
            period="monthly",
            license_id=lic_id,
            session=db_session,
        )
        b2 = await create_budget(
            tenant_id=T("lic-upsert"),
            budget_tokens=10_000,
            period="monthly",
            license_id=lic_id,
            session=db_session,
        )
        assert b1.id != b2.id
        assert b2.budget_tokens == 10_000

    @pytest.mark.unit
    async def test_create_budget_different_periods_not_upserted(self, db_session):
        """Monthly and weekly budgets for the same tenant should be separate rows."""
        b1 = await create_budget(
            tenant_id=T("multi-period"),
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )
        b2 = await create_budget(
            tenant_id=T("multi-period"),
            budget_tokens=500,
            period="weekly",
            session=db_session,
        )
        assert b1.id != b2.id

    @pytest.mark.unit
    async def test_create_budget_different_licenses_not_upserted(self, db_session):
        """Same tenant+period but different license_ids should be separate budgets."""
        lic1 = uuid.uuid4()
        lic2 = uuid.uuid4()
        b1 = await create_budget(
            T("multi-lic"), 1000, "monthly", license_id=lic1, session=db_session
        )
        b2 = await create_budget(
            T("multi-lic"), 2000, "monthly", license_id=lic2, session=db_session
        )
        assert b1.id != b2.id

    @pytest.mark.unit
    async def test_create_budget_with_license_id(self, db_session):
        """Budget created with specific license_id should store it correctly."""
        lic_id = uuid.uuid4()
        budget = await create_budget(
            tenant_id=T("lic-tenant"),
            budget_tokens=50_000,
            period="monthly",
            license_id=lic_id,
            session=db_session,
        )
        assert budget.license_id == lic_id

        # Verify in DB
        stmt = select(TokenBudget).where(TokenBudget.id == budget.id)
        result = await db_session.execute(stmt)
        row = result.scalar_one()
        assert row.license_id == lic_id

    @pytest.mark.unit
    async def test_create_budget_null_license_uses_sentinel(self, db_session):
        """Budget without license_id should use NULL_LICENSE_ID sentinel."""
        budget = await create_budget(
            tenant_id=T("null-lic"),
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )
        assert budget.license_id == NULL_LICENSE_ID

    @pytest.mark.unit
    async def test_create_budget_requires_session(self):
        """create_budget should raise ValueError when session is None."""
        with pytest.raises(ValueError, match="session is required"):
            await create_budget(
                tenant_id=T("no-session-tenant"),
                budget_tokens=1000,
                session=None,
            )

    @pytest.mark.unit
    async def test_create_budget_custom_includes(self, db_session):
        """Budget with includes_input=False should be stored correctly."""
        budget = await create_budget(
            tenant_id=T("custom-includes"),
            budget_tokens=10_000,
            period="monthly",
            includes_input=False,
            includes_output=True,
            session=db_session,
        )
        assert budget.includes_input is False
        assert budget.includes_output is True

    @pytest.mark.unit
    async def test_create_after_soft_delete_inserts_new_row(self, db_session):
        """After a soft-delete, a subsequent create_budget produces a NEW row.

        New stacking semantics: deactivated budgets stay deactivated; new
        budgets are fresh rows. The old "upsert reactivates" behavior is gone.
        """
        budget = await create_budget(
            tenant_id=T("reactivate"),
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )
        prior = await delete_budget(budget.id, db_session)
        assert prior is not None
        await db_session.refresh(budget)
        assert budget.active is False

        new = await create_budget(
            tenant_id=T("reactivate"),
            budget_tokens=2000,
            period="monthly",
            session=db_session,
        )
        assert new.id != budget.id
        assert new.active is True
        assert new.budget_tokens == 2000
        # Old row is still soft-deleted.
        await db_session.refresh(budget)
        assert budget.active is False

    @pytest.mark.unit
    async def test_create_includes_flags_per_row(self, db_session):
        """Each insert carries its own includes_input/includes_output."""
        first = await create_budget(
            tenant_id=T("upsert-flags"),
            budget_tokens=1000,
            period="monthly",
            includes_input=True,
            includes_output=True,
            session=db_session,
        )
        assert first.includes_input is True
        assert first.includes_output is True

        second = await create_budget(
            tenant_id=T("upsert-flags"),
            budget_tokens=1000,
            period="monthly",
            includes_input=False,
            includes_output=False,
            session=db_session,
        )
        # Stacked: distinct row, distinct flags.
        assert second.id != first.id
        assert second.includes_input is False
        assert second.includes_output is False

    @pytest.mark.unit
    async def test_create_budget_populates_tenant_name(self, db_session):
        """token_budgets.tenant_name is NOT NULL with no DB DEFAULT in prod
        (added via src/db.py bootstrap). create_budget must populate it,
        mirroring the column's tenant_name = tenant_id backfill semantic."""
        budget = await create_budget(
            tenant_id=T("tenant-name-regression"),
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )
        assert budget.tenant_name == T("tenant-name-regression")


# ===========================================================================
# delete_budget
# ===========================================================================


class TestDeleteBudget:
    @pytest.mark.unit
    async def test_delete_budget(self, db_session):
        """Deleting should soft-deactivate and return the prior-state snapshot."""
        budget = await create_budget(
            tenant_id=T("test-tenant-3"),
            budget_tokens=500_000,
            period="weekly",
            session=db_session,
        )
        assert budget.active is True

        prior = await delete_budget(budget.id, db_session)
        assert prior is not None
        # Snapshot reflects pre-mutation state (active=True).
        assert prior.active is True
        assert prior.budget_tokens == 500_000

        await db_session.refresh(budget)
        assert budget.active is False

        stmt = select(TokenBudget).where(TokenBudget.id == budget.id)
        db_result = await db_session.execute(stmt)
        row = db_result.scalar_one()
        assert row.active is False

    @pytest.mark.unit
    async def test_delete_nonexistent(self, db_session):
        """Deleting a nonexistent budget should return None."""
        result = await delete_budget(uuid.uuid4(), db_session)
        assert result is None

    @pytest.mark.unit
    async def test_delete_sets_updated_at(self, db_session):
        """Deleting should update the updated_at timestamp."""
        budget = await create_budget(
            tenant_id=T("delete-ts"),
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )
        original_updated = budget.updated_at

        await delete_budget(budget.id, db_session)
        await db_session.refresh(budget)

        assert budget.updated_at >= original_updated

    @pytest.mark.unit
    async def test_delete_idempotent(self, db_session):
        """Deleting an already-deactivated budget returns None on second call."""
        budget = await create_budget(
            tenant_id=T("delete-idem"),
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )
        first = await delete_budget(budget.id, db_session)
        assert first is not None  # prior state captured
        # Second delete on now-inactive row is a no-op (returns None).
        second = await delete_budget(budget.id, db_session)
        assert second is None


# ===========================================================================
# get_budgets_for_tenant
# ===========================================================================


class TestGetBudgetsForTenant:
    @pytest.mark.unit
    async def test_get_budgets_for_tenant(self, db_session):
        """Should return only active budgets for the tenant."""
        await create_budget(T("tenant-a"), 1000, "monthly", session=db_session)
        await create_budget(T("tenant-a"), 500, "weekly", session=db_session)
        await create_budget(T("tenant-b"), 2000, "monthly", session=db_session)

        budgets = await get_budgets_for_tenant(T("tenant-a"), db_session)
        assert len(budgets) == 2
        assert all(b.tenant_id == T("tenant-a") for b in budgets)

    @pytest.mark.unit
    async def test_get_budgets_excludes_inactive(self, db_session):
        """Deactivated budgets should not be returned by get_budgets_for_tenant."""
        tenant = T("inactive-tenant")
        b1 = await create_budget(tenant, 1000, "monthly", session=db_session)
        await create_budget(tenant, 500, "weekly", session=db_session)

        # Deactivate one
        await delete_budget(b1.id, db_session)

        budgets = await get_budgets_for_tenant(tenant, db_session)
        assert len(budgets) == 1
        assert budgets[0].period == "weekly"

    @pytest.mark.unit
    async def test_get_budgets_empty_tenant(self, db_session):
        """Tenant with no budgets should return empty list."""
        budgets = await get_budgets_for_tenant(T("nonexistent-tenant"), db_session)
        assert budgets == []

    @pytest.mark.unit
    async def test_get_budgets_multiple_licenses(self, db_session):
        """Should return budgets across multiple licenses for the same tenant."""
        lic1 = uuid.uuid4()
        lic2 = uuid.uuid4()
        await create_budget(
            T("multi-lic-get"), 1000, "monthly", license_id=lic1, session=db_session
        )
        await create_budget(
            T("multi-lic-get"), 2000, "monthly", license_id=lic2, session=db_session
        )
        await create_budget(T("multi-lic-get"), 500, "weekly", session=db_session)

        budgets = await get_budgets_for_tenant(T("multi-lic-get"), db_session)
        assert len(budgets) == 3


# ===========================================================================
# check_budget
# ===========================================================================


class TestCheckBudget:
    @pytest.mark.unit
    async def test_check_budget_no_usage(self, db_session):
        """Budget check with no usage should show 0 used."""
        budget = await create_budget(
            tenant_id=T("check-tenant"),
            budget_tokens=1_000_000,
            period="monthly",
            session=db_session,
        )
        result = await check_budget(budget)
        assert result["budget_tokens"] == 1_000_000
        assert result["used_tokens"] == 0
        assert result["remaining_tokens"] == 1_000_000
        assert result["over_budget"] is False
        assert result["usage_pct"] == 0.0

    @pytest.mark.unit
    async def test_check_budget_with_db_usage(self, db_session):
        """Without Redis, check_budget reads `consumed_tokens` off the row.

        Rewritten with the TokenUsage fallback: this used to seed a whole-period
        TokenUsage row and assert check_budget reported it. That pinned the bug —
        the rollup is tenant-scoped, not budget-scoped. The cascade-drain path
        (services.budget_debit) writes `consumed_tokens`, so that is what a
        per-budget snapshot must report.
        """
        tenant = T("db-usage-tenant")
        budget = await create_budget(
            tenant_id=tenant,
            budget_tokens=10_000,
            period="monthly",
            session=db_session,
        )
        budget.consumed_tokens = Decimal(5000)
        await db_session.commit()

        result = await check_budget(budget)
        assert result["used_tokens"] == 5000  # input(3000) + output(2000)
        assert result["remaining_tokens"] == 5000
        assert result["over_budget"] is False
        assert result["usage_pct"] == 50.0

    @pytest.mark.unit
    async def test_check_budget_weekly(self, db_session):
        """Weekly budget should use week start for period calculation."""
        tenant = T("weekly-budget-tenant")
        budget = await create_budget(
            tenant_id=tenant,
            budget_tokens=5000,
            period="weekly",
            session=db_session,
        )

        budget.consumed_tokens = Decimal(1500)
        await db_session.commit()

        result = await check_budget(budget)
        assert result["period"] == "weekly"
        assert result["used_tokens"] == 1500
        assert result["remaining_tokens"] == 3500
        assert result["over_budget"] is False

    @pytest.mark.unit
    async def test_check_budget_over_budget(self, db_session):
        """When usage exceeds budget, over_budget should be True and remaining should be 0."""
        tenant = T("over-budget-tenant")
        budget = await create_budget(
            tenant_id=tenant,
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )

        budget.consumed_tokens = Decimal(1300)
        await db_session.commit()

        result = await check_budget(budget)
        assert result["used_tokens"] == 1300
        assert result["remaining_tokens"] == 0  # clamped to 0
        assert result["over_budget"] is True
        assert result["usage_pct"] == 130.0

    @pytest.mark.unit
    async def test_check_budget_exactly_at_limit(self, db_session):
        """When usage exactly equals budget, over_budget should be True (remaining <= 0)."""
        tenant = T("exact-budget")
        budget = await create_budget(
            tenant_id=tenant,
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )

        budget.consumed_tokens = Decimal(1000)
        await db_session.commit()

        result = await check_budget(budget)
        assert result["used_tokens"] == 1000
        assert result["remaining_tokens"] == 0
        assert result["over_budget"] is True
        assert result["usage_pct"] == 100.0

    @pytest.mark.parametrize(
        ("period", "licensed"),
        [
            ("monthly", True),
            ("weekly", True),
            ("monthly", False),
            ("weekly", False),
        ],
        ids=["licensed_monthly", "licensed_weekly", "tenant_monthly", "tenant_weekly"],
    )
    @pytest.mark.unit
    async def test_column_is_the_only_usage_source_across_budget_shapes(
        self, db_session, period, licensed
    ):
        """Every budget shape resolves usage from ``consumed_tokens``, and the
        aggregate path reports byte-identical detail for the same row."""
        from services.budget import check_budget_aggregate

        lic_id = uuid.uuid4() if licensed else None
        budget = await create_budget(
            tenant_id=T("shape-tenant"),
            budget_tokens=10_000,
            period=period,
            license_id=lic_id,
            session=db_session,
        )
        budget.consumed_tokens = Decimal(3000)
        await db_session.commit()

        result = await check_budget(budget)
        assert result["used_tokens"] == 3000
        assert result["remaining_tokens"] == 7000
        assert result["period"] == period
        assert result["license_id"] == (str(lic_id) if licensed else None)

        agg = await check_budget_aggregate(db_session, T("shape-tenant"))
        assert agg["budgets"] == [result]
        assert agg["total_remaining"] == 7000

    @pytest.mark.unit
    async def test_check_budget_output_shape(self, db_session):
        """check_budget should return all expected keys with correct types."""
        budget = await create_budget(
            tenant_id=T("shape-check"),
            budget_tokens=1000,
            period="monthly",
            session=db_session,
        )
        result = await check_budget(budget)

        assert isinstance(result["budget_id"], str)
        assert result["tenant_id"] == T("shape-check")
        assert result["license_id"] is None  # NULL_LICENSE_ID mapped to None
        assert result["period"] == "monthly"
        assert isinstance(result["budget_tokens"], int)
        assert isinstance(result["used_tokens"], int)
        assert isinstance(result["remaining_tokens"], int)
        assert isinstance(result["usage_pct"], float)
        assert isinstance(result["over_budget"], bool)

    @pytest.mark.unit
    async def test_check_budget_license_id_in_output(self, db_session):
        """check_budget should include the license_id string for licensed budgets."""
        lic_id = uuid.uuid4()
        budget = await create_budget(
            tenant_id=T("lic-output"),
            budget_tokens=1000,
            period="monthly",
            license_id=lic_id,
            session=db_session,
        )
        result = await check_budget(budget)
        assert result["license_id"] == str(lic_id)

    @pytest.mark.unit
    async def test_check_budget_input_only_reports_the_filtered_total(self, db_session):
        """``includes_input``/``includes_output`` are applied at DEBIT time.

        ``consumed_tokens`` is therefore already the filtered total; the read
        path neither splits it nor re-filters it.
        """
        lic_id = uuid.uuid4()
        budget = await create_budget(
            tenant_id=T("io-filtered"),
            budget_tokens=10_000,
            period="monthly",
            license_id=lic_id,
            includes_input=True,
            includes_output=False,
            session=db_session,
        )
        budget.consumed_tokens = Decimal(5000)
        await db_session.commit()

        result = await check_budget(budget)
        assert result["used_tokens"] == 5000


# ===========================================================================
# consumed_tokens == 0 is a NORMAL state, not a "legacy pre-cascade" marker
# ===========================================================================


class TestZeroConsumedIsNotLegacy:
    """`check_budget` must read `consumed_tokens` — never the whole-period
    `TokenUsage` rollup.

    The removed fallback treated ``consumed_tokens == 0`` as "this budget
    predates the cascade-drain path, go read TokenUsage". Zero is simply the
    state of every budget that has not been debited yet: a freshly-paid
    top-up, and every monthly budget in the opening moments of a new period.
    The rollup it fell back to is scoped only by
    ``(tenant_id, period_type, period_start, license_id)`` — NOT by budget —
    so it reported the tenant's whole-period usage against a brand-new budget
    while ``check_budget_aggregate`` (which reads ``consumed_tokens``)
    reported the same budget unused. Two endpoints, two answers, on the paid
    path.

    The premise was false as well: ``cost_flusher.flush_costs`` upserts
    ``TokenUsage`` on EVERY flush for EVERY tenant, so that table is a live
    rollup, not a pre-cascade relic.
    """

    @staticmethod
    def _seed_usage(session, tenant, period_start, *, period="monthly", inp=3000, out=2000):
        session.add(
            TokenUsage(
                tenant_id=tenant,
                license_id=NULL_LICENSE_ID,
                period_type=period,
                period_start=period_start,
                input_tokens=inp,
                output_tokens=out,
                total_tokens=inp + out,
            )
        )

    @pytest.mark.unit
    async def test_fresh_topup_over_existing_period_usage_reports_zero(self, db_session):
        """A just-created budget has consumed nothing, whatever the period rollup says."""
        tenant = T("fresh-topup")
        self._seed_usage(db_session, tenant, _current_month_start(), inp=54_000, out=36_000)
        budget = await create_budget(
            tenant_id=tenant, budget_tokens=100_000, period="monthly", session=db_session
        )
        await db_session.commit()

        result = await check_budget(budget)
        assert result["used_tokens"] == 0, "whole-period rollup leaked onto a new budget"
        assert result["remaining_tokens"] == 100_000
        assert result["usage_pct"] == 0.0
        assert result["over_budget"] is False

    @pytest.mark.unit
    async def test_check_budget_agrees_with_check_budget_aggregate(self, db_session):
        """THE invariant: the per-budget and aggregate read paths cannot disagree."""
        from services.budget import check_budget_aggregate

        tenant = T("agree-tenant")
        self._seed_usage(db_session, tenant, _current_month_start(), inp=54_000, out=36_000)
        budget = await create_budget(
            tenant_id=tenant, budget_tokens=100_000, period="monthly", session=db_session
        )
        await db_session.commit()

        per_budget = await check_budget(budget)
        agg = await check_budget_aggregate(db_session, tenant)
        assert len(agg["budgets"]) == 1
        assert per_budget["used_tokens"] == agg["budgets"][0]["used_tokens"]
        assert per_budget["remaining_tokens"] == agg["budgets"][0]["remaining_tokens"]
        assert agg["total_remaining"] == per_budget["remaining_tokens"]

    @pytest.mark.unit
    async def test_neither_read_path_accepts_a_redis_connection(self):
        """Structural guard — there is no Redis budget counter to read.

        Deleting the Redis read is not enough on its own: the previous commit
        left ``redis_conn`` on BOTH signatures, all three route handlers kept
        passing ``get_redis()``, and only one of the two functions read it.
        That looked Redis-aware, reviewed as Redis-aware, and silently
        diverged. With no connection parameter, re-introducing a counter read
        cannot be done without also changing a signature this test pins.
        """
        import inspect

        from services.budget import check_budget_aggregate

        assert list(inspect.signature(check_budget).parameters) == ["budget"]
        assert list(inspect.signature(check_budget_aggregate).parameters) == [
            "session",
            "tenant_id",
            "ts",
        ]

    @pytest.mark.unit
    async def test_stacked_zero_consumed_budgets_do_not_multiply_period_usage(self, db_session):
        """Stacked top-ups are the documented design: N zero-consumed budgets on
        the same tenant+license each independently returned the SAME whole-period
        total, so a dashboard multiplied one period's usage by the stack depth."""
        tenant = T("stacked-zero")
        self._seed_usage(db_session, tenant, _current_month_start(), inp=6000, out=4000)
        b1 = await create_budget(
            tenant_id=tenant, budget_tokens=50_000, period="monthly", session=db_session
        )
        b2 = await create_budget(
            tenant_id=tenant, budget_tokens=50_000, period="monthly", session=db_session
        )
        await db_session.commit()

        r1 = await check_budget(b1)
        r2 = await check_budget(b2)
        assert (r1["used_tokens"], r2["used_tokens"]) == (0, 0)
        assert r1["used_tokens"] + r2["used_tokens"] == 0, "period usage double-counted per budget"

    # Two tests lived here that differed only in WHICH Redis arm of check_budget
    # they drove into the TokenUsage fallback (`RedisError` vs key-miss). Both
    # arms are gone — check_budget takes no Redis connection at all — so they
    # had become literal duplicates of test_fresh_topup_... above. The property
    # they protected (a Redis outage cannot change the reported number) is now
    # structural and is pinned by
    # test_neither_read_path_accepts_a_redis_connection.

    @pytest.mark.unit
    async def test_nonzero_consumed_tokens_is_still_honoured(self, db_session):
        """The non-zero column value stays authoritative (no regression)."""
        tenant = T("nonzero-consumed")
        self._seed_usage(db_session, tenant, _current_month_start(), inp=99_000, out=99_000)
        budget = await create_budget(
            tenant_id=tenant, budget_tokens=10_000, period="monthly", session=db_session
        )
        budget.consumed_tokens = Decimal(2500)
        await db_session.commit()

        result = await check_budget(budget)
        assert result["used_tokens"] == 2500
        assert result["remaining_tokens"] == 7500
        assert result["usage_pct"] == 25.0

    @pytest.mark.unit
    async def test_weekly_zero_consumed_budget_reports_zero(self, db_session):
        """Same for the weekly period grain."""
        tenant = T("weekly-zero")
        self._seed_usage(
            db_session, tenant, _current_week_start(), period="weekly", inp=1000, out=500
        )
        budget = await create_budget(
            tenant_id=tenant, budget_tokens=5000, period="weekly", session=db_session
        )
        await db_session.commit()

        result = await check_budget(budget)
        assert result["period"] == "weekly"
        assert result["used_tokens"] == 0
        assert result["remaining_tokens"] == 5000


# ===========================================================================
# Tenant identifiers that did NOT arrive in the X-Tenant-Id header
# ===========================================================================


class TestTenantIdentifiersOutsideTheHeaderAreResolved:
    """Every budget route that takes a tenant from somewhere other than the header.

    ``require_tenant`` resolves the ``X-Tenant-Id`` HEADER, so a registered
    tenant NAME works there. Since migration 0011 every ``tenant_id`` column is
    a real Postgres ``uuid``, so the SAME name arriving as a path parameter, a
    query parameter, or (on ``/status/me``) read straight off the request had no
    resolver at all: it bound into SQL as an unparseable literal and the caller
    got a ``DataError`` rendered as a 400/503 for input the header accepts.

    Each case below fails against the pre-fix router, and the value asserted is
    the canonical uuid — a route that merely stopped erroring while storing or
    querying the name would still be wrong.
    """

    NAME = "n8n-dev"

    @property
    def uuid(self) -> str:
        from tenancy import LEGACY_TENANT_NAMES

        return LEGACY_TENANT_NAMES[self.NAME]

    @pytest.mark.unit
    def test_status_me_accepts_a_registered_tenant_name(self, app_client):
        """/status/me reads the header itself — the one tenant route not behind require_tenant."""
        r = app_client.get(
            "/api/budget/status/me",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": self.NAME},
        )
        assert r.status_code == 200, r.text
        # Resolved, not echoed: the aggregate must be keyed by the uuid.
        assert r.json()["data"]["tenant_id"] == self.uuid

    @pytest.mark.unit
    def test_status_me_rejects_an_unknown_tenant_without_echoing_it(self, app_client):
        """400, and the supplied value never appears in the response body.

        A tenant field has been observed in production carrying a raw
        ``Bearer <jwt>``, so the refusal must not reflect it back.
        """
        r = app_client.get(
            "/api/budget/status/me",
            headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": "no-such-tenant"},
        )
        assert r.status_code == 400, r.text
        assert "no-such-tenant" not in r.text

    @pytest.mark.unit
    def test_status_me_rejects_a_blank_header_as_a_caller_error(self, app_client):
        """license-authz sends ``tenant_id = result.tenant_id or ""`` when it has no tenant.

        Present-but-blank is a caller error (400), not "no header" — and NOT a
        401: this route deliberately does not fall through to require_tenant's
        admin-JWT branch, which would resolve the caller's own subject.
        """
        for blank in ("", "   "):
            r = app_client.get(
                "/api/budget/status/me",
                headers={"X-Subject-Id": str(uuid.uuid4()), "X-Tenant-Id": blank},
            )
            assert r.status_code == 400, f"{blank!r} -> {r.text}"

    @pytest.mark.unit
    def test_status_path_accepts_a_registered_tenant_name(self, app_client):
        r = app_client.get(f"/api/budget/status/{self.NAME}", headers={"X-Tenant-Id": self.NAME})
        assert r.status_code == 200, r.text

    @pytest.mark.unit
    def test_list_path_accepts_a_registered_tenant_name(self, app_client):
        r = app_client.get(f"/api/budget/list/{self.NAME}", headers={"X-Tenant-Id": self.NAME})
        assert r.status_code == 200, r.text

    @pytest.mark.unit
    def test_check_query_param_accepts_a_registered_tenant_name(self, app_client):
        """``/check`` takes the tenant as a QUERY parameter — same defect, same fix."""
        r = app_client.get(
            "/api/budget/check", params={"tenant_id": self.NAME}, headers={"X-Tenant-Id": self.NAME}
        )
        assert r.status_code == 200, r.text
        assert r.json()["data"]["tenant_id"] == self.uuid

    @pytest.mark.unit
    def test_topup_path_accepts_a_registered_tenant_name(
        self, app_client, monkeypatch, topup_enabled
    ):
        """The path tenant wins on /topup and is used for BOTH the insert and the audit row."""
        import redis_client

        stub = AsyncMock()
        stub.set = AsyncMock(return_value=True)
        stub.delete = AsyncMock(return_value=1)
        monkeypatch.setattr(redis_client, "_redis", stub)

        r = app_client.post(
            f"/api/budget/topup/{self.NAME}",
            json={
                "tenant_id": self.uuid,
                "license_id": str(uuid.uuid4()),
                "budget_tokens": 5_000,
                "period": "monthly",
            },
        )
        assert r.status_code == 201, r.text
        assert r.json()["data"]["tenant_id"] == self.uuid

    @pytest.mark.unit
    def test_isolation_is_checked_before_resolution(self, app_client):
        """A junk path tenant from a CUSTOMER caller is a flat 403, never a 400.

        ``enforce_tenant_isolation`` canonicalises both sides itself, so for a
        value that RESOLVES the ordering is immaterial. It matters only for one
        that does not: resolving first would answer 403 for a registered tenant
        name and 400 for an unregistered one, which is a tenant-name enumeration
        oracle on a customer-facing route. This pins the ordering.
        """
        r = app_client.get("/api/budget/status/garbage", headers={"X-Tenant-Id": self.NAME})
        assert r.status_code == 403, r.text
