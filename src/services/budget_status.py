"""Token budget read paths — per-budget and aggregate status snapshots.

Source-of-truth is the ``consumed_tokens`` column on each ``TokenBudget`` row
(the cascade-drain path in :mod:`services.budget_debit`). It is the ONLY
source either read path consults, and :func:`check_budget_aggregate` builds
each of its per-budget entries by calling :func:`check_budget`, so the two
cannot disagree by construction rather than by convention.

Neither function takes a Redis connection: there is no Redis budget counter.
``GET /api/budget/check`` (license-authz's budget gate) answers from this
module alone. ``tests/test_budget.py`` pins the signatures.

The ``TokenUsage`` rollup is deliberately NOT consulted either: it is scoped by
``(tenant_id, period_type, period_start, license_id)`` — whole-period, not
per-budget — so reading it for a budget attributes the tenant's entire period
to that one budget, and to every stacked sibling as well.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import NULL_LICENSE_ID, TokenBudget
from services.budget_debit import _select_active_budgets
from services.budget_period import effective_consumed

logger = structlog.stdlib.get_logger(__name__)


async def get_budgets_for_tenant(
    tenant_id: str,
    session: AsyncSession,
) -> list[TokenBudget]:
    """Return all active token budgets for a tenant (no soft-deleted rows)."""
    stmt = select(TokenBudget).where(
        TokenBudget.tenant_id == tenant_id,
        TokenBudget.active.is_(True),
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_all_budgets_for_tenant(
    tenant_id: str,
    session: AsyncSession,
) -> list[TokenBudget]:
    """Return ALL budgets for a tenant including soft-deleted (UI history)."""
    stmt = (
        select(TokenBudget)
        .where(TokenBudget.tenant_id == tenant_id)
        .order_by(TokenBudget.created_at.desc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def check_budget(budget: TokenBudget) -> dict:
    """Per-budget status snapshot, rendered from the loaded row alone.

    Usage is ``consumed_tokens`` — the cascade-drain source of truth. Nothing
    else is consulted, and this function is also what
    :func:`check_budget_aggregate` calls for every budget it reports, so the
    per-budget endpoint and the aggregate endpoint cannot return different
    numbers for the same budget.

    It used to read the per-budget Redis counter first and fall back to the
    column, while ``check_budget_aggregate`` accepted a ``redis_conn`` it never
    touched. That made the headline "both paths resolve usage the same way"
    false: with ``consumed_tokens=100`` against a 1000-token budget and a
    counter left at 5000, this endpoint answered ``used=5000,
    over_budget=True`` and the aggregate answered ``used=100,
    over_budget=False``. Reachable, not theoretical —
    the (since deleted) Redis counter mirror ran only AFTER the DB commit and
    swallowed ``RedisError``. The
    column is by construction at least as fresh as the counter, so the read was
    pure downside. ``redis_conn`` and the now-unused ``session`` are gone from
    the signature rather than accepted-and-ignored: a parameter that looks
    consulted and is not is the defect, not the fix.

    There is no ``TokenUsage`` fallback either. The removed one keyed on
    ``consumed_tokens == 0`` as a "created before the cascade path" marker,
    but zero is the ordinary state of any budget that has not been debited
    yet: a freshly-paid top-up, and every monthly budget at the start of its
    period. It then returned the tenant's WHOLE-period rollup — scoped by
    tenant/period/license, not by budget — so a new top-up reported the
    month's usage as its own (~90% spent on a budget nobody had touched),
    every stacked sibling reported that same total again, and
    ``check_budget_aggregate`` reported the budget unused. Its premise was
    wrong too: ``cost_flusher.flush_costs`` upserts ``TokenUsage`` on every
    flush for every tenant, so that table is a live rollup, not a relic.
    """
    has_license = budget.license_id is not None and budget.license_id != NULL_LICENSE_ID
    # effective_consumed, not the raw column: a periodic budget whose stored
    # consumption belongs to an earlier period has already reset (see
    # services/budget_period.py). Reading the column directly here would make
    # the status endpoint disagree with the reserve gate the moment a month
    # rolls over — the report would still show last month's spend.
    used = effective_consumed(budget, datetime.now(UTC))

    remaining = budget.budget_tokens - used
    # A 0 cap is a block-all budget, and it reports 100% used — NOT 0%.
    #
    # The guard exists to avoid dividing by zero, but the value it fell back to
    # matters. It used to yield 0, which rendered a fully-blocked budget as
    # "0% used" in the status endpoint and in every UI reading it — the most
    # reassuring number available for the most restrictive state there is. The
    # rest of this dict already told the truth (``remaining_tokens`` 0,
    # ``over_budget`` True, since ``remaining <= 0`` holds at a 0 cap), so
    # ``usage_pct`` was the one field contradicting its own row.
    #
    # 100.0 is the honest reading: none of the allowance is available, and any
    # request against it is denied by ``_RESERVE_LUA`` (``cap >= 0`` rejects
    # every non-zero request at cap 0).
    usage_pct = (used / budget.budget_tokens * 100) if budget.budget_tokens > 0 else 100.0

    return {
        "budget_id": str(budget.id),
        "tenant_id": budget.tenant_id,
        "license_id": str(budget.license_id) if has_license else None,
        "period": budget.period,
        "budget_tokens": budget.budget_tokens,
        "used_tokens": used,
        "remaining_tokens": max(0, remaining),
        "usage_pct": round(usage_pct, 2),
        "over_budget": remaining <= 0,
        "priority": budget.priority,
        "starts_at": budget.starts_at,
        "expires_at": budget.expires_at,
        "source": budget.source,
    }


async def check_budget_aggregate(
    session: AsyncSession,
    tenant_id: str,
    ts: datetime | None = None,
) -> dict:
    """Aggregate remaining tokens across all currently-active budgets.

    Returns total remaining, count, next expiry, and per-budget detail in
    cascade-drain order so a UI can show "draining now: budget X (Y tokens
    left)".

    Every per-budget entry is produced by :func:`check_budget`, not by a
    parallel dict literal. The duplicate literal that used to live here was
    already subtly different (it clamped ``remaining`` before deriving
    ``over_budget``), and it is what let this function keep answering from
    ``consumed_tokens`` while ``check_budget`` answered from a Redis counter.
    Delegating makes agreement structural. It also takes no ``redis_conn``:
    the old one was accepted, passed by all three route handlers, and never
    read — see the module docstring.
    """
    if ts is None:
        ts = datetime.now(UTC)

    budgets = await _select_active_budgets(session, tenant_id, ts, lock=False)

    per_budget: list[dict] = []
    total_remaining = 0
    next_expiry: datetime | None = None
    for b in budgets:
        entry = await check_budget(b)
        total_remaining += entry["remaining_tokens"]
        if b.expires_at is not None and (next_expiry is None or b.expires_at < next_expiry):
            next_expiry = b.expires_at
        per_budget.append(entry)

    return {
        "tenant_id": tenant_id,
        "total_remaining": total_remaining,
        "budget_count": len(per_budget),
        "next_expiry": next_expiry,
        "budgets": per_budget,
    }
