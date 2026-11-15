"""``billing_budget_debits_total{outcome="oversold"}`` was a series that never existed.

Two Grafana dashboards chart ``sum by (outcome) (rate(billing_budget_debits_total
[1m]))``; ``observability/metrics.py`` declares the label set as
``ok | oversold | no_budget`` and its module docstring says "oversold paths are
how we detect license configuration errors"; and ``cost_flusher`` carried the
comment *"the alert query uses rate(... {outcome=\"oversold\"}) > 0"*.

Nothing ever emitted it. Every overflow step was counted as ``ok``.

Measured on prod 2026-08-25: 739 ``budget_oversold`` warnings in a 2-hour window,
against a live budget standing at 264,243,479 / 90,000,000 = 294% consumed and
still ``active``. An operator reading the panel saw a flat zero.
"""

from __future__ import annotations

import uuid
from typing import cast

from services.budget import BudgetDebit, split_debit_outcomes


def _debit(*, debited: int, oversold: int = 0) -> BudgetDebit:
    return BudgetDebit(
        budget_id=uuid.uuid4(),
        debited_tokens=debited,
        remaining_after=0,
        oversold_tokens=oversold,
    )


def test_a_normal_cascade_is_all_ok() -> None:
    ok, oversold = split_debit_outcomes([_debit(debited=10), _debit(debited=5)])
    assert (ok, oversold) == (2, 0)


def test_an_overflow_step_is_not_counted_as_ok() -> None:
    """THE DEFECT. The overflow step is the cap failing to cap.

    A mutant that returns ``(len(debits), 0)`` — which is exactly what the code
    did before this change — must fail here.
    """
    ok, oversold = split_debit_outcomes([_debit(debited=10), _debit(debited=7, oversold=7)])
    assert oversold == 1
    # Asserted as an absence: the overflow step must be GONE from the ok count,
    # not merely accompanied by an oversold count. Without this line a mutant
    # that increments both labels for the same step passes.
    assert ok == 1


def test_every_step_oversold_leaves_no_ok() -> None:
    ok, oversold = split_debit_outcomes([_debit(debited=3, oversold=3)])
    assert (ok, oversold) == (0, 1)


def test_no_debits_emits_neither() -> None:
    """An empty cascade is the ``no_budget`` outcome's job, not this one."""
    assert split_debit_outcomes([]) == (0, 0)


def test_zero_oversold_tokens_is_not_oversold() -> None:
    """Guards the boundary: the flag is a token COUNT, not a truthy marker.

    A mutant using ``if d.oversold_tokens is not None`` would count every step.
    """
    ok, oversold = split_debit_outcomes([_debit(debited=4, oversold=0)])
    assert (ok, oversold) == (1, 0)


def test_the_default_is_not_oversold() -> None:
    """A step built without the keyword — every non-overflow call site — is ok."""
    step = BudgetDebit(budget_id=uuid.uuid4(), debited_tokens=1, remaining_after=9)
    assert step.oversold_tokens == 0
    assert split_debit_outcomes([step]) == (1, 0)


# ---------------------------------------------------------------------------
# The classification tests above are not enough on their own.
#
# A mutation that drops ``oversold=oversold`` at the `_step` call inside
# `debit_budget` — i.e. the production wiring that decides an overflow step is an
# overflow step — SURVIVED all of them, because they build BudgetDebit objects by
# hand and so share the implementation's blind spot. These tests drive the real
# cascade instead. `_select_active_budgets` is the only DB touch in
# `debit_budget`, so stubbing it exercises every line of the overflow logic
# without a database.
# ---------------------------------------------------------------------------

from datetime import UTC, datetime  # noqa: E402
from decimal import Decimal  # noqa: E402

import pytest  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: E402

from models.db.budgets import TokenBudget  # noqa: E402
from services import budget_debit as bd  # noqa: E402
from services.budget_period import period_anchor  # noqa: E402


class _StubSession:
    """`debit_budget(commit=False)` only ever flushes; nothing here hits a DB.

    Cast to AsyncSession at the call sites: the real type is only structurally
    needed for `.flush()` here, and a live session would mean truncating a
    database other sessions on this host are using.
    """

    async def flush(self) -> None:
        return None


def _session() -> AsyncSession:
    return cast("AsyncSession", _StubSession())


def _budget(limit: int, consumed: int = 0) -> TokenBudget:
    now = datetime.now(UTC)
    return TokenBudget(
        id=uuid.uuid4(),
        tenant_id=str(uuid.uuid4()),
        license_id=uuid.uuid4(),
        budget_tokens=limit,
        consumed_tokens=Decimal(consumed),
        period="monthly",
        consumed_period_start=period_anchor("monthly", now),
        includes_input=True,
        includes_output=True,
        active=True,
    )


@pytest.mark.asyncio
async def test_the_io_split_cascade_marks_its_overflow_step(monkeypatch) -> None:
    """100-token budget, 150 tokens spent: the 50-token overflow must be marked."""
    budget = _budget(limit=100)

    async def _fake_select(session, tenant_id, ts, *, lock, license_id=None):
        return [budget]

    monkeypatch.setattr(bd, "_select_active_budgets", _fake_select)
    debits = await bd.debit_budget(
        _session(), None, "t", 150, in_tokens=100, out_tokens=50, commit=False
    )

    overflow = [d for d in debits if d.oversold_tokens > 0]
    assert len(overflow) == 1, "the overflow step must be marked, not merely appended"
    assert overflow[0].oversold_tokens == 50
    # Absence control: the in-budget step must NOT be marked. Without this a
    # mutant that marks every step passes the assertion above.
    assert [d.oversold_tokens for d in debits if d not in overflow] == [0]
    assert bd.split_debit_outcomes(debits) == (1, 1)


@pytest.mark.asyncio
async def test_the_legacy_cascade_marks_its_overflow_step(monkeypatch) -> None:
    """The no-io-split branch appends its own overflow step and must mark it too."""
    budget = _budget(limit=100)

    async def _fake_select(session, tenant_id, ts, *, lock, license_id=None):
        return [budget]

    monkeypatch.setattr(bd, "_select_active_budgets", _fake_select)
    debits = await bd.debit_budget(_session(), None, "t", 175, commit=False)

    overflow = [d for d in debits if d.oversold_tokens > 0]
    assert len(overflow) == 1
    assert overflow[0].oversold_tokens == 75
    assert bd.split_debit_outcomes(debits) == (1, 1)


@pytest.mark.asyncio
async def test_a_cascade_that_fits_marks_nothing(monkeypatch) -> None:
    """The negative control. Spend inside the limit must produce zero oversold."""
    budget = _budget(limit=1000)

    async def _fake_select(session, tenant_id, ts, *, lock, license_id=None):
        return [budget]

    monkeypatch.setattr(bd, "_select_active_budgets", _fake_select)
    debits = await bd.debit_budget(
        _session(), None, "t", 150, in_tokens=100, out_tokens=50, commit=False
    )
    assert debits, "the fixture must actually debit — an empty list would pass vacuously"
    assert all(d.oversold_tokens == 0 for d in debits)
    assert bd.split_debit_outcomes(debits)[1] == 0
