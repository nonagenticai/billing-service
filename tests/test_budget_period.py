"""Unit tests for the period-anchoring accessor, and a guard on its fixtures.

``services.budget_period`` is THE read-side accessor for every budget
enforcement, status and mirror path in this service, and until now it had no
direct unit test of its own. It was exercised only indirectly, through fixtures
that turned out to disagree with it about what period their consumption
belonged to -- which is how 16 tests went red on ``main`` on 2026-09-01 and a
merged feature was left stranded out of production.

Two kinds of test live here:

* Unit coverage of ``period_anchor`` / ``consumption_anchor`` /
  ``is_stale_period`` / ``effective_consumed``, including the NULL-anchor
  fallback -- a branch with zero production rows today AND, before this file,
  zero tests.

* A BOUNDARY GUARD over ``tests._budget_factory.make_budget``. It passes a
  synthetic instant to both the factory and the accessor, so it asserts the
  same thing on every calendar day of the year. That matters: the bug it guards
  is visible from the wall clock on roughly 12 days a year, so a test that
  depends on when it runs would be the very trap it is meant to catch.
"""

from __future__ import annotations

import ast
import pathlib
from datetime import UTC, date, datetime, timedelta

import pytest

from models.db import TokenBudget
from services.budget_period import (
    consumption_anchor,
    effective_consumed,
    is_stale_period,
    period_anchor,
)
from tests._budget_factory import make_budget
from timeutil import current_month_start, current_week_start

# Instants chosen so that each one can actually DETECT a reverted fix. Verified
# by walking the arithmetic against a NULL anchor (which falls back to
# ``starts_at`` == ts - 1d):
#
#   ts                         monthly            weekly
#   2026-10-01 Thu, day 1      DETECTS            vacuous (same ISO week)
#   2026-06-15 Mon             vacuous (same mo)  DETECTS
#   2027-02-01 Mon, day 1      DETECTS            DETECTS
#
# A "last instant of a month" case was considered and DELETED: at 2026-09-30
# both anchors land in the same period for monthly AND weekly, so it can never
# fail. It was measured, not assumed -- see the PR body. A cell that cannot go
# red is not evidence.
DAY1 = datetime(2026, 10, 1, 0, 0, 0, tzinfo=UTC)
MONDAY = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
DAY1_AND_MONDAY = datetime(2027, 2, 1, 0, 0, 0, tzinfo=UTC)
BOUNDARIES = [
    pytest.param(DAY1, id="first-instant-of-a-month"),
    pytest.param(MONDAY, id="first-instant-of-an-iso-monday"),
    pytest.param(DAY1_AND_MONDAY, id="day-1-that-is-also-a-monday"),
]


# ---------------------------------------------------------------------------
# period_anchor
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPeriodAnchor:
    def test_monthly_is_the_first_of_that_utc_month(self):
        assert period_anchor("monthly", datetime(2026, 9, 17, 23, 0, tzinfo=UTC)) == date(
            2026, 9, 1
        )

    def test_weekly_is_the_iso_monday(self):
        # 2026-06-15 is itself a Monday; 06-21 is the Sunday that closes that week.
        assert period_anchor("weekly", datetime(2026, 6, 15, 0, 0, tzinfo=UTC)) == date(2026, 6, 15)
        assert period_anchor("weekly", datetime(2026, 6, 21, 23, 59, tzinfo=UTC)) == date(
            2026, 6, 15
        )

    @pytest.mark.parametrize("period", [None, "daily", "lifetime", ""])
    def test_unknown_period_is_none_not_a_guess(self, period):
        """None means 'no period semantics, never resets'. Guessing here would
        silently zero a real cap."""
        assert period_anchor(period, datetime(2026, 9, 1, tzinfo=UTC)) is None

    def test_naive_datetime_is_treated_as_utc(self):
        assert period_anchor("monthly", datetime(2026, 9, 17, 12, 0)) == date(2026, 9, 1)

    def test_anchoring_is_utc_not_local(self):
        """23:30 on the last of the month in UTC+2 is still the OLD month in UTC.

        The module docstring is explicit that anchoring on a local date would
        reintroduce a bug this service already fixed once.
        """
        from datetime import timezone

        amsterdam_ish = timezone(timedelta(hours=2))
        ts = datetime(2026, 10, 1, 1, 30, tzinfo=amsterdam_ish)  # == 2026-09-30 23:30Z
        assert period_anchor("monthly", ts) == date(2026, 9, 1)


# ---------------------------------------------------------------------------
# consumption_anchor / is_stale_period / effective_consumed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConsumptionAnchorFallback:
    """The NULL-anchor branch: 0 production rows today, and 0 tests before this."""

    def test_null_anchor_falls_back_to_the_period_of_starts_at(self):
        b = make_budget(
            tenant="t",
            cap=1000,
            consumed=100,
            starts_at=datetime(2026, 7, 9, tzinfo=UTC),
            consumed_period_start=None,
        )
        assert b.consumed_period_start is None
        assert consumption_anchor(b, datetime(2026, 9, 1, tzinfo=UTC)) == date(2026, 7, 1)

    def test_a_persisted_anchor_wins_over_starts_at(self):
        b = make_budget(
            tenant="t",
            cap=1000,
            consumed=100,
            starts_at=datetime(2026, 7, 9, tzinfo=UTC),
            consumed_period_start=date(2026, 9, 1),
        )
        assert consumption_anchor(b, datetime(2026, 9, 20, tzinfo=UTC)) == date(2026, 9, 1)

    def test_null_anchor_on_a_lifetime_budget_is_none(self):
        b = make_budget(tenant="t", cap=1000, consumed=100, period="daily")
        assert consumption_anchor(b, datetime(2026, 9, 1, tzinfo=UTC)) is None


@pytest.mark.unit
class TestStalenessAndEffectiveConsumed:
    def test_previous_period_is_stale_and_reads_zero(self):
        b = make_budget(tenant="t", cap=1000, consumed=900, now=datetime(2026, 8, 15, tzinfo=UTC))
        ts = datetime(2026, 9, 2, tzinfo=UTC)
        assert is_stale_period(b, ts) is True
        assert effective_consumed(b, ts) == 0

    def test_same_period_is_not_stale_and_reads_through(self):
        b = make_budget(tenant="t", cap=1000, consumed=900, now=datetime(2026, 9, 2, tzinfo=UTC))
        ts = datetime(2026, 9, 20, tzinfo=UTC)
        assert is_stale_period(b, ts) is False
        assert effective_consumed(b, ts) == 900

    def test_a_future_anchor_is_not_stale(self):
        """`<` not `!=`. An equal or future anchor reads through; only a
        strictly OLDER one resets. attribution_audit used to get this wrong."""
        b = make_budget(tenant="t", cap=1000, consumed=900, consumed_period_start=date(2026, 10, 1))
        assert is_stale_period(b, datetime(2026, 9, 20, tzinfo=UTC)) is False
        assert effective_consumed(b, datetime(2026, 9, 20, tzinfo=UTC)) == 900

    def test_a_lifetime_budget_never_goes_stale(self):
        b = make_budget(tenant="t", cap=1000, consumed=900, period="daily")
        far_future = datetime(2031, 1, 1, tzinfo=UTC)
        assert is_stale_period(b, far_future) is False
        assert effective_consumed(b, far_future) == 900


# ---------------------------------------------------------------------------
# The boundary guard
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFactoryIsPeriodStableAtEveryBoundary:
    """The regression guard for 2026-09-01.

    Every assertion here takes ``ts`` explicitly, so this class behaves
    identically whatever day it is run on.
    """

    @pytest.mark.parametrize("ts", BOUNDARIES)
    @pytest.mark.parametrize("period", ["monthly", "weekly"])
    def test_seeded_consumption_belongs_to_the_current_period(self, ts, period):
        b = make_budget(tenant="t", cap=1000, consumed=100, period=period, now=ts)
        # Identity first: prove the factory HONOURED `now`. A derived assertion
        # alone would pass or fail depending on where the real clock happens to
        # sit, which is not a proof of anything.
        assert isinstance(b, TokenBudget), "a stand-in object would take the NULL path silently"
        assert b.period == period
        assert b.starts_at == ts - timedelta(days=1)
        assert b.consumed_period_start == period_anchor(period, ts)
        # Then the property that actually broke.
        assert is_stale_period(b, ts) is False
        assert effective_consumed(b, ts) == 100

    @pytest.mark.parametrize("period", ["monthly", "weekly"])
    def test_guard_is_silent_away_from_a_boundary(self, period):
        """Negative control: mid-period, even a NULL-anchor row is not stale.

        If this went red the guard would be failing everywhere and would not be
        measuring the boundary at all.
        """
        ts = datetime(2026, 6, 17, 12, 0, tzinfo=UTC)  # a Wednesday, mid-month
        legacy = make_budget(
            tenant="t",
            cap=1000,
            consumed=100,
            period=period,
            starts_at=ts - timedelta(days=1),
            consumed_period_start=None,
        )
        assert is_stale_period(legacy, ts) is False


@pytest.mark.unit
class TestTimeutilAgreesWithPeriodAnchor:
    """Two implementations of one boundary exist and nothing coupled them.

    They agree today (both explicitly UTC). This is preventive: it keeps them
    agreeing rather than fixing a live divergence.
    """

    def test_month_start_matches(self):
        now = datetime.now(UTC)
        assert current_month_start() == period_anchor("monthly", now)

    def test_week_start_matches(self):
        now = datetime.now(UTC)
        assert current_week_start() == period_anchor("weekly", now)


# ---------------------------------------------------------------------------
# Structural guard over the whole test tree
# ---------------------------------------------------------------------------


def _starts_at_is_now_minus_a_delta(node: ast.AST) -> bool:
    """True for the ``datetime.now(UTC) - timedelta(...)`` shape specifically.

    That is the shape that fired: a clock reading pushed into the PAST. A bare
    ``now()`` is safe (it is always in the current period) and so is the column
    default, which is why an absent ``starts_at`` is not flagged.
    """
    has_clock = any(
        isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr in {"now", "utcnow", "today"}
        for n in ast.walk(node)
    )
    has_subtraction = any(
        isinstance(n, ast.BinOp) and isinstance(n.op, ast.Sub) for n in ast.walk(node)
    )
    return has_clock and has_subtraction


@pytest.mark.unit
def test_no_test_seeds_a_past_budget_without_saying_which_period_it_consumed():
    """Structural, because a line grep already missed sites in a prior sweep.

    Scope, stated honestly: this catches the literal ``now() - delta`` shape at
    the construction site. It does not chase a `starts_at` computed elsewhere
    and passed in by name -- for that, use the factory, which is the point.
    """
    offenders: list[str] = []
    for path in sorted(pathlib.Path(__file__).parent.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "TokenBudget"
            ):
                continue
            kwargs = {k.arg for k in node.keywords}
            starts_at = next((k.value for k in node.keywords if k.arg == "starts_at"), None)
            if starts_at is None or not _starts_at_is_now_minus_a_delta(starts_at):
                continue
            if "consumed_period_start" not in kwargs:
                offenders.append(f"{path.name}:{node.lineno}")

    assert not offenders, (
        "These build a budget that STARTED in the past but never say which period its "
        "consumed_tokens belong to. consumption_anchor then falls back to starts_at, so on "
        "the 1st of a month (or an ISO Monday) the consumption is correctly read as 0 and "
        "the test fails for a reason that has nothing to do with what it asserts. "
        f"Use tests._budget_factory.make_budget instead: {offenders}"
    )
