"""Pre-flight budget reservation tests.

Atomicity is the whole point of this endpoint, so every test runs the real
Lua against fakeredis (which executes Lua locally in-process). Concurrency
is exercised with ``asyncio.gather`` so two concurrent reservations can't
both pass when the budget only has room for one.
"""

from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import TokenBudget
from services.budget import reserve_budget
from tests._budget_factory import make_budget
from tests.conftest import T


def _budget(
    *,
    tenant: str,
    cap: int,
    consumed: int = 0,
    license_id: uuid.UUID | None = None,
    period: str = "monthly",
) -> TokenBudget:
    return make_budget(
        tenant=tenant,
        cap=cap,
        consumed=consumed,
        license_id=license_id,
        period=period,
        source="test",
    )


@pytest.mark.asyncio
async def test_reserve_succeeds_when_under_cap(db_session: AsyncSession, fake_redis):
    db_session.add(_budget(tenant=T("t1"), cap=1000, consumed=100))
    await db_session.commit()

    r = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t1"),
        estimated_tokens=200,
        ttl_seconds=60,
    )

    assert r["allowed"] is True
    assert r["reserved_tokens"] == 200
    assert r["cap_tokens"] == 1000
    assert r["consumed_tokens"] == 100
    assert r["in_flight_reserved_tokens"] == 200
    assert r["remaining_tokens"] == 700
    assert r["reason"] is None


@pytest.mark.asyncio
async def test_reserve_denies_when_over_cap(db_session: AsyncSession, fake_redis):
    db_session.add(_budget(tenant=T("t2"), cap=1000, consumed=900))
    await db_session.commit()

    r = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t2"),
        estimated_tokens=200,
        ttl_seconds=60,
    )

    assert r["allowed"] is False
    assert r["reason"] == "over_budget"
    assert r["reserved_tokens"] == 0
    assert r["remaining_tokens"] == 100  # cap 1000 - consumed 900 - reserved 0


@pytest.mark.asyncio
async def test_reserve_pools_across_stacked_budgets(db_session: AsyncSession, fake_redis):
    """Cap is the SUM of all active budgets, not just the highest-priority one."""
    db_session.add_all(
        [
            _budget(tenant=T("t3"), cap=600, consumed=0),
            _budget(tenant=T("t3"), cap=400, consumed=0),
        ]
    )
    await db_session.commit()

    r = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t3"),
        estimated_tokens=900,
        ttl_seconds=60,
    )

    assert r["allowed"] is True
    assert r["cap_tokens"] == 1000  # 600 + 400


@pytest.mark.asyncio
async def test_concurrent_reservations_cannot_overcommit(db_session: AsyncSession, fake_redis):
    """Two concurrent reservations of 600 tokens against a 1000-token budget:
    exactly one must succeed, the other must be denied. Lua atomicity is the
    whole reason this endpoint exists."""
    db_session.add(_budget(tenant=T("t4"), cap=1000, consumed=0))
    await db_session.commit()

    # Two concurrent calls
    results = await asyncio.gather(
        reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("t4"),
            estimated_tokens=600,
            ttl_seconds=60,
        ),
        reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("t4"),
            estimated_tokens=600,
            ttl_seconds=60,
        ),
    )

    allowed = [r for r in results if r["allowed"]]
    denied = [r for r in results if not r["allowed"]]
    assert len(allowed) == 1, f"exactly one must succeed; got {results}"
    assert len(denied) == 1
    assert denied[0]["reason"] == "over_budget"


@pytest.mark.asyncio
async def test_reserve_no_active_budget_returns_no_active_reason(
    db_session: AsyncSession, fake_redis
):
    r = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t-no-budget"),
        estimated_tokens=100,
        ttl_seconds=60,
    )

    assert r["allowed"] is False
    assert r["reason"] == "no_active_budget"
    assert r["cap_tokens"] == 0


@pytest.mark.asyncio
async def test_reserve_zero_cap_blocks_via_lua(db_session: AsyncSession, fake_redis):
    """An active budget with cap==0 is a ZERO-ALLOWANCE budget → block, not
    unlimited. RED on the old `cap > 0` Lua guard (it skipped the cap check and
    served cap==0 as unlimited/allowed); GREEN under `cap >= 0`. Uses the Redis
    (Lua) path — the DB-fallback path already blocked cap==0."""
    db_session.add(_budget(tenant=T("t-zero-cap"), cap=0, consumed=0))
    await db_session.commit()

    r = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t-zero-cap"),
        estimated_tokens=1,
        ttl_seconds=60,
    )

    assert r["allowed"] is False
    assert r["reason"] == "over_budget"
    assert r["cap_tokens"] == 0
    assert r["reserved_tokens"] == 0
    assert r["remaining_tokens"] == 0


@pytest.mark.asyncio
async def test_reserve_positive_cap_still_allowed_after_zero_cap_fix(
    db_session: AsyncSession, fake_redis
):
    """Regression guard: the cap>=0 flip must NOT block a normal positive-cap
    reservation with room (the whole point is only cap==0 changes)."""
    db_session.add(_budget(tenant=T("t-pos-cap"), cap=1000, consumed=0))
    await db_session.commit()

    r = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t-pos-cap"),
        estimated_tokens=500,
        ttl_seconds=60,
    )

    assert r["allowed"] is True
    assert r["reserved_tokens"] == 500


@pytest.mark.asyncio
async def test_reserve_redis_unavailable_falls_back_to_db_check(db_session: AsyncSession):
    """When Redis is None, enforce against DB-known consumed only."""
    db_session.add(_budget(tenant=T("t5"), cap=1000, consumed=200))
    await db_session.commit()

    r = await reserve_budget(
        session=db_session,
        redis_conn=None,
        tenant_id=T("t5"),
        estimated_tokens=500,
        ttl_seconds=60,
    )

    assert r["allowed"] is True  # 200 + 500 <= 1000
    assert r["in_flight_reserved_tokens"] == 0
    assert r["reason"] is None  # success path returns reason=None even with redis_conn=None


@pytest.mark.asyncio
async def test_reserve_redis_unavailable_denies_when_db_says_over(db_session: AsyncSession):
    db_session.add(_budget(tenant=T("t6"), cap=1000, consumed=900))
    await db_session.commit()

    r = await reserve_budget(
        session=db_session,
        redis_conn=None,
        tenant_id=T("t6"),
        estimated_tokens=200,
        ttl_seconds=60,
    )

    assert r["allowed"] is False
    # DB-only path still reports the semantic reason — the user is over budget.
    # The "we fell back from Redis" detail is in the structured log
    # (reserve_redis_unavailable_db_fallback), not the response.
    assert r["reason"] == "over_budget"
    assert r["remaining_tokens"] == 100


@pytest.mark.asyncio
async def test_reserve_license_filter_excludes_other_licenses(db_session: AsyncSession, fake_redis):
    """When license_id is supplied, budgets scoped to OTHER licenses are excluded."""
    other_license = uuid.uuid4()
    target_license = uuid.uuid4()
    db_session.add_all(
        [
            _budget(tenant=T("t7"), cap=200, consumed=0, license_id=other_license),
            _budget(tenant=T("t7"), cap=600, consumed=0, license_id=target_license),
        ]
    )
    await db_session.commit()

    r = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t7"),
        estimated_tokens=500,
        ttl_seconds=60,
        license_id=target_license,
    )

    assert r["allowed"] is True
    assert r["cap_tokens"] == 600  # other license's 200 not counted


@pytest.mark.asyncio
async def test_reservation_ttl_expires(db_session: AsyncSession, fake_redis):
    """After TTL elapses, the reserved counter zeros out and a new reservation
    that needed the held capacity now succeeds."""
    db_session.add(_budget(tenant=T("t8"), cap=1000, consumed=0))
    await db_session.commit()

    # Reserve 800; second reserve of 500 must be denied (800 + 500 > 1000)
    r1 = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t8"),
        estimated_tokens=800,
        ttl_seconds=5,
    )
    assert r1["allowed"] is True

    r2_blocked = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t8"),
        estimated_tokens=500,
        ttl_seconds=60,
    )
    assert r2_blocked["allowed"] is False

    # Force the TTL to elapse by deleting the reserved key directly
    # (fakeredis honours TTL but waiting 5s in tests is wasteful — same
    # observable end-state via DEL).
    await fake_redis.delete(f"tokens:tenant:{T('t8')}:reserved")

    r3 = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t8"),
        estimated_tokens=500,
        ttl_seconds=60,
    )
    assert r3["allowed"] is True


@pytest.mark.asyncio
async def test_reserve_rejects_after_committed_debit_reaches_cap(
    db_session: AsyncSession, fake_redis
):
    """Accumulated committed spend must keep being enforced across reservations.

    Regression for the stale-``consumed_pool`` gap: the reserve gate used to
    cache consumed in a Redis key seeded once (SET NX, 35d TTL) that real debits
    never updated, so after the first reservation of a window it saw consumed≈0
    forever and let a tenant reserve ~cap per request unboundedly. The fix reads
    committed ``consumed_tokens`` fresh from the DB every call.

    Sequence: reserve 400 (ok) -> a real debit commits consumed to the cap ->
    the in-flight reservation TTL-expires -> a new reserve MUST now be denied.
    Under the old cached-pool code the final reserve was wrongly allowed.
    """
    b = _budget(tenant=T("t-drain"), cap=1000, consumed=0)
    db_session.add(b)
    await db_session.commit()

    r1 = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t-drain"),
        estimated_tokens=400,
        ttl_seconds=60,
    )
    assert r1["allowed"] is True

    # A real debit commits: the tenant has now actually consumed the whole cap.
    b.consumed_tokens = Decimal(1000)
    await db_session.commit()

    # The earlier in-flight reservation auto-releases (TTL); simulate via DEL.
    await fake_redis.delete(f"tokens:tenant:{T('t-drain')}:reserved")

    r2 = await reserve_budget(
        session=db_session,
        redis_conn=fake_redis,
        tenant_id=T("t-drain"),
        estimated_tokens=100,
        ttl_seconds=60,
    )
    # Fresh DB consumed (1000) >= cap -> deny. Old stale-pool code allowed this.
    assert r2["allowed"] is False, "reserve must enforce accumulated committed spend"
    assert r2["reason"] == "over_budget"
    assert r2["consumed_tokens"] == 1000


@pytest.mark.asyncio
async def test_reserve_endpoint_returns_429_on_deny(app_client, db_session: AsyncSession):
    """The HTTP endpoint must return 429 (not 200) when reservation is denied,
    so caller can treat it as a hard signal without inspecting the body."""
    db_session.add(_budget(tenant=T("t-http"), cap=100, consumed=99))
    await db_session.commit()

    r = app_client.post(
        "/api/budget/reserve",
        headers={"X-Tenant-Id": T("t-http")},
        json={"estimated_tokens": 50, "ttl_seconds": 30},
    )

    assert r.status_code == 429
    # The route raises HTTPException(429, detail={"allowed": ..., "reason": ...}).
    # With the canonical envelope, that structured payload is preserved at
    # error.details rather than at the top-level `detail` key.
    envelope = r.json()
    assert envelope["error"]["status"] == 429
    body = envelope["error"]["details"]
    assert body["allowed"] is False
    assert body["reason"] == "over_budget"


@pytest.mark.asyncio
async def test_reserve_endpoint_returns_200_on_allow(app_client, db_session: AsyncSession):
    db_session.add(_budget(tenant=T("t-http-ok"), cap=10000, consumed=0))
    await db_session.commit()

    r = app_client.post(
        "/api/budget/reserve",
        headers={"X-Tenant-Id": T("t-http-ok")},
        json={"estimated_tokens": 500, "ttl_seconds": 30},
    )

    assert r.status_code == 200
    body = r.json()["data"]
    assert body["allowed"] is True
    assert body["reserved_tokens"] == 500
    assert body["remaining_tokens"] == 9500
