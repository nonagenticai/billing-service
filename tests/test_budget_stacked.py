"""Stacked-budget cascade-drain tests.

Each test seeds and isolates its own tenant so they don't pollute each other.
The shared `db_engine` fixture truncates between tests, but using distinct
tenant_ids also keeps these readable in isolation.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete, select

from models.db import TokenBudget
from services.budget import (
    BudgetDebit,
    check_budget_aggregate,
    create_budget,
    debit_budget,
    delete_budget,
)
from tests.conftest import T


async def _wipe_tenant(session, tenant_id: str) -> None:
    """Hard-delete all budgets for a tenant — ensures test isolation even if
    the per-test TRUNCATE somehow misses (e.g. ad-hoc runs)."""
    await session.execute(delete(TokenBudget).where(TokenBudget.tenant_id == tenant_id))
    await session.commit()


# ---------------------------------------------------------------------------
# 1. Single budget drain
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_single_budget_drain(db_session):
    tenant = T("stacked-single")
    await _wipe_tenant(db_session, tenant)

    b = await create_budget(tenant, 1000, "monthly", session=db_session)

    debits = await debit_budget(db_session, None, tenant, 200)
    assert len(debits) == 1
    assert debits[0].budget_id == b.id
    assert debits[0].debited_tokens == 200
    assert debits[0].remaining_after == 800

    await db_session.refresh(b)
    assert int(b.consumed_tokens) == 200


# ---------------------------------------------------------------------------
# 2. Cascade by priority
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_cascade_by_priority(db_session):
    tenant = T("stacked-priority")
    await _wipe_tenant(db_session, tenant)

    a = await create_budget(tenant, 100, "monthly", priority=1, session=db_session)
    b = await create_budget(tenant, 100, "monthly", priority=10, session=db_session)

    # Drain 150 — A (priority 1) should empty first, B picks up the rest.
    debits = await debit_budget(db_session, None, tenant, 150)
    assert len(debits) == 2
    assert debits[0].budget_id == a.id
    assert debits[0].debited_tokens == 100
    assert debits[1].budget_id == b.id
    assert debits[1].debited_tokens == 50

    await db_session.refresh(a)
    await db_session.refresh(b)
    assert int(a.consumed_tokens) == 100
    assert int(b.consumed_tokens) == 50


# ---------------------------------------------------------------------------
# 3. Cascade by expiry
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_cascade_by_expiry(db_session):
    tenant = T("stacked-expiry")
    await _wipe_tenant(db_session, tenant)

    tomorrow = datetime.now(UTC) + timedelta(days=1)
    # A expires tomorrow, lower-priority B is open-ended. Cascade ordering
    # puts A first because expires_at is the *primary* sort key.
    a = await create_budget(
        tenant, 100, "monthly", priority=99, expires_at=tomorrow, session=db_session
    )
    b = await create_budget(tenant, 100, "monthly", priority=1, session=db_session)

    debits = await debit_budget(db_session, None, tenant, 50)
    assert len(debits) == 1
    assert debits[0].budget_id == a.id  # despite higher priority number, expires sooner

    await db_session.refresh(a)
    await db_session.refresh(b)
    assert int(a.consumed_tokens) == 50
    assert int(b.consumed_tokens) == 0


# ---------------------------------------------------------------------------
# 4. Top up mid-stream
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_topup_mid_stream(db_session):
    tenant = T("stacked-topup")
    await _wipe_tenant(db_session, tenant)

    a = await create_budget(tenant, 1000, "monthly", priority=1, session=db_session)

    # Drain A to half.
    await debit_budget(db_session, None, tenant, 500)
    await db_session.refresh(a)
    assert int(a.consumed_tokens) == 500

    # Top up: insert B. A still drains first (lower priority).
    b = await create_budget(tenant, 1000, "monthly", priority=2, source="topup", session=db_session)

    # Drain 800 — A has 500 left, then B picks up 300.
    debits = await debit_budget(db_session, None, tenant, 800)
    assert len(debits) == 2
    assert debits[0].budget_id == a.id
    assert debits[0].debited_tokens == 500
    assert debits[1].budget_id == b.id
    assert debits[1].debited_tokens == 300

    await db_session.refresh(a)
    await db_session.refresh(b)
    assert int(a.consumed_tokens) == 1000
    assert int(b.consumed_tokens) == 300


# ---------------------------------------------------------------------------
# 5. Oversold
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_oversold_records_overflow_and_logs(db_session, capsys):
    tenant = T("stacked-oversold")
    await _wipe_tenant(db_session, tenant)

    a = await create_budget(tenant, 60, "monthly", priority=1, session=db_session)
    b = await create_budget(tenant, 40, "monthly", priority=2, session=db_session)

    debits = await debit_budget(db_session, None, tenant, 150)

    # Two real debits + one overflow on the last budget visited.
    assert len(debits) == 3
    assert debits[0].budget_id == a.id and debits[0].debited_tokens == 60
    assert debits[1].budget_id == b.id and debits[1].debited_tokens == 40
    assert debits[2].budget_id == b.id and debits[2].debited_tokens == 50

    await db_session.refresh(a)
    await db_session.refresh(b)
    assert int(a.consumed_tokens) == 60
    # B's consumed_tokens exceeds budget_tokens — that's the overflow signal.
    assert int(b.consumed_tokens) == 90
    assert int(b.consumed_tokens) > b.budget_tokens

    # structlog renders to stdout via the configured stdlib handler.
    captured = capsys.readouterr()
    assert "budget_oversold" in captured.out
    assert "overflow_tokens=50" in captured.out
    assert f"last_budget_id={b.id}" in captured.out


# ---------------------------------------------------------------------------
# 6. Soft-delete excludes from cascade
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_soft_delete_excludes_from_cascade(db_session):
    tenant = T("stacked-softdel")
    await _wipe_tenant(db_session, tenant)

    a = await create_budget(tenant, 100, "monthly", priority=1, session=db_session)
    b = await create_budget(tenant, 100, "monthly", priority=2, session=db_session)

    # Soft-delete A.
    prior = await delete_budget(a.id, db_session)
    assert prior is not None
    assert prior.active is True  # snapshot reflects pre-mutation state

    debits = await debit_budget(db_session, None, tenant, 50)
    assert len(debits) == 1
    assert debits[0].budget_id == b.id  # A is excluded

    # Confirm the soft-deleted row's consumed_tokens is unchanged.
    fresh_a = (
        await db_session.execute(select(TokenBudget).where(TokenBudget.id == a.id))
    ).scalar_one()
    assert int(fresh_a.consumed_tokens) == 0
    assert fresh_a.active is False


@pytest.mark.unit
async def test_soft_delete_no_other_budgets_skip_logged(db_session, capsys):
    tenant = T("stacked-softdel-only")
    await _wipe_tenant(db_session, tenant)

    a = await create_budget(tenant, 100, "monthly", session=db_session)
    await delete_budget(a.id, db_session)

    debits = await debit_budget(db_session, None, tenant, 50)
    assert debits == []

    captured = capsys.readouterr()
    assert "budget_debit_skipped" in captured.out
    assert "no_active_budgets" in captured.out


# ---------------------------------------------------------------------------
# 7. Concurrent debit safety
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_concurrent_debits_no_lost_updates(db_session, db_session_factory):
    tenant = T(f"stacked-concurrent-{uuid.uuid4().hex[:8]}")
    await _wipe_tenant(db_session, tenant)

    b = await create_budget(tenant, 1000, "monthly", priority=1, session=db_session)
    # create_budget flushes only; the parallel debit sessions need the row
    # to be visible to other transactions.
    await db_session.commit()

    async def _one_debit(amount: int) -> list[BudgetDebit]:
        async with db_session_factory() as session:
            return await debit_budget(session, None, tenant, amount)

    # Two concurrent debits totaling 700.
    results = await asyncio.gather(_one_debit(300), _one_debit(400))
    total_debited = sum(d.debited_tokens for r in results for d in r)
    assert total_debited == 700

    # Reload from a fresh session to bypass any stale state.
    async with db_session_factory() as session:
        fresh = (
            await session.execute(select(TokenBudget).where(TokenBudget.id == b.id))
        ).scalar_one()
        assert int(fresh.consumed_tokens) == 700


# ---------------------------------------------------------------------------
# Bonus: aggregate read shape
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_aggregate_returns_cascade_ordered_per_budget(db_session):
    tenant = T("stacked-agg")
    await _wipe_tenant(db_session, tenant)

    tomorrow = datetime.now(UTC) + timedelta(days=1)
    next_week = datetime.now(UTC) + timedelta(days=7)

    a = await create_budget(
        tenant, 500, "monthly", priority=5, expires_at=tomorrow, session=db_session
    )
    b = await create_budget(
        tenant, 1000, "monthly", priority=1, expires_at=next_week, session=db_session
    )
    c = await create_budget(tenant, 200, "monthly", priority=10, session=db_session)

    agg = await check_budget_aggregate(db_session, tenant)
    ids_in_order = [item["budget_id"] for item in agg["budgets"]]
    # A expires soonest, B next, C is open-ended.
    assert ids_in_order == [str(a.id), str(b.id), str(c.id)]
    assert agg["total_remaining"] == 500 + 1000 + 200
    assert agg["budget_count"] == 3
    assert agg["next_expiry"] == tomorrow


@pytest.mark.unit
async def test_includes_input_only_budget_skips_output(db_session):
    """A budget with includes_output=False shouldn't be debited for output tokens."""
    tenant = T("stacked-io-flags")
    await _wipe_tenant(db_session, tenant)

    # Only-input budget; should ignore the output portion.
    a = await create_budget(
        tenant,
        100,
        "monthly",
        includes_input=True,
        includes_output=False,
        priority=1,
        session=db_session,
    )
    # Catch-all that takes both.
    b = await create_budget(tenant, 1000, "monthly", priority=2, session=db_session)

    debits = await debit_budget(db_session, None, tenant, 60, in_tokens=20, out_tokens=40)

    # A took only input (20). B took the rest (40 output).
    a_debit = next((d for d in debits if d.budget_id == a.id), None)
    b_debit = next((d for d in debits if d.budget_id == b.id), None)

    assert a_debit is not None
    assert a_debit.debited_tokens == 20
    assert b_debit is not None
    assert b_debit.debited_tokens == 40

    await db_session.refresh(a)
    await db_session.refresh(b)
    assert int(a.consumed_tokens) == 20
    assert int(b.consumed_tokens) == 40
    # Verify Decimal type roundtrip
    assert isinstance(a.consumed_tokens, Decimal)


# ---------------------------------------------------------------------------
# 9. Leftover of an EXCLUDED token type must not inflate consumed_tokens
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_leftover_excluded_token_type_not_charged(db_session, capsys):
    """Input-only budget: leftover OUTPUT tokens are unbudgeted, not oversold.

    Single active budget accepts input only. Debit 100 in / 500 out. The 100
    input is debited; the 500 output has NO budget that accepts it, so it must
    NOT be dumped onto this input-only budget as "oversold". consumed_tokens
    stays 100 (not 600). Reverting the fix makes this 600 → mutation-proof.
    """
    tenant = T("stacked-excluded-leftover")
    await _wipe_tenant(db_session, tenant)

    a = await create_budget(
        tenant,
        1000,
        "monthly",
        includes_input=True,
        includes_output=False,
        priority=1,
        session=db_session,
    )

    debits = await debit_budget(db_session, None, tenant, 600, in_tokens=100, out_tokens=500)

    # Only the 100 input is debited; no oversold overflow row for the output.
    assert len(debits) == 1
    assert debits[0].budget_id == a.id
    assert debits[0].debited_tokens == 100

    await db_session.refresh(a)
    assert int(a.consumed_tokens) == 100  # NOT 600

    # Excluded-type leftover is logged as unbudgeted, never as oversold.
    captured = capsys.readouterr()
    assert "budget_oversold" not in captured.out
    assert "budget_unbudgeted_tokens" in captured.out


# ---------------------------------------------------------------------------
# 10. Genuine oversold of an ACCEPTED type is still charged
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_genuine_oversold_of_accepted_type_still_charged(db_session, capsys):
    """Output-accepting budget over cap: the accepted-type overflow IS charged.

    Single budget accepts output, cap 30. Debit 100 output. 30 fits, the
    remaining 70 output overflows a budget that DOES accept output → genuine
    oversold; it is still attributed and logged (existing behavior preserved).
    """
    tenant = T("stacked-genuine-oversold")
    await _wipe_tenant(db_session, tenant)

    a = await create_budget(
        tenant,
        30,
        "monthly",
        includes_input=False,
        includes_output=True,
        priority=1,
        session=db_session,
    )

    debits = await debit_budget(db_session, None, tenant, 100, in_tokens=0, out_tokens=100)

    # One in-cap debit (30) + one overflow debit (70) on the same budget.
    assert len(debits) == 2
    assert debits[0].budget_id == a.id and debits[0].debited_tokens == 30
    assert debits[1].budget_id == a.id and debits[1].debited_tokens == 70

    await db_session.refresh(a)
    assert int(a.consumed_tokens) == 100
    assert int(a.consumed_tokens) > a.budget_tokens

    captured = capsys.readouterr()
    assert "budget_oversold" in captured.out
    assert "overflow_tokens=70" in captured.out
    assert f"last_budget_id={a.id}" in captured.out


# ---------------------------------------------------------------------------
# 11. Overflow of an accepted type is charged to the budget that ACCEPTS it,
#     even when the last-iterated budget excludes that type
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_overflow_charged_to_accepting_budget_not_last_iterated(db_session, capsys):
    """Two budgets with disjoint accept-flags; input overflows its budget.

    Budget A accepts input only (cap 100); Budget B accepts output only (cap
    1000). Debit 150 in / 50 out. A takes 100 input (exhausted), B takes 50
    output. The 50 input leftover is a GENUINE oversold of a type a budget DID
    accept (A) — even though B was the last budget iterated. It must be charged
    to A as oversold, not silently dropped as unbudgeted because B excludes
    input. Reverting the fix charges it to nobody (A stays 100) → mutation-proof.
    """
    tenant = T("stacked-overflow-accepting")
    await _wipe_tenant(db_session, tenant)

    a = await create_budget(
        tenant,
        100,
        "monthly",
        includes_input=True,
        includes_output=False,
        priority=1,
        session=db_session,
    )
    b = await create_budget(
        tenant,
        1000,
        "monthly",
        includes_input=False,
        includes_output=True,
        priority=2,
        session=db_session,
    )

    debits = await debit_budget(db_session, None, tenant, 200, in_tokens=150, out_tokens=50)

    await db_session.refresh(a)
    await db_session.refresh(b)
    # A: 100 in-cap + 50 input overflow = 150 (oversold); B: 50 output.
    assert int(a.consumed_tokens) == 150  # NOT 100 — the input overflow is real oversold
    assert int(b.consumed_tokens) == 50

    a_debited = sum(d.debited_tokens for d in debits if d.budget_id == a.id)
    b_debited = sum(d.debited_tokens for d in debits if d.budget_id == b.id)
    assert a_debited == 150
    assert b_debited == 50

    captured = capsys.readouterr()
    assert "budget_oversold" in captured.out
    assert "overflow_tokens=50" in captured.out
    assert f"last_budget_id={a.id}" in captured.out  # charged to A, not B
