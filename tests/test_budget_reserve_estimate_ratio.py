"""billing_budget_reserve_estimate_ratio: making the alert's own escalation
condition expressible (#178 follow-up).

``BillingBudgetReserveOverRelease`` names one falsifiable reason to escalate
itself to ``critical`` — ``prior_pool / released < 0.5`` across a meaningful
share of events — and then documents that condition as UNFIRABLE, because
neither ``prior_pool`` nor ``released`` was exported as a Prometheus series;
both lived only inside the ``reserve_over_release`` structlog line in
``services/budget_redis.py::release_reservation``. A PromQL rule cannot read a
log line, so the escalation criterion existed in prose only.

``budget_reserve_estimate_ratio`` (``src/observability/metrics.py``) closes
that gap: on every over-release where a live pool existed (``prior_pool >
0``), ``release_reservation`` now observes ``prior_pool / released`` into this
histogram, right after it increments ``budget_reserve_over_release_total``
(``src/services/budget_redis.py``, the ``else`` branch of the over-release
split, ``prior_pool != 0``). The measured population sits at ~0.91 median —
the estimate is an UNDER-estimate by ~9%, not a drained pool — which is why
the escalation threshold lives at 0.5: the bulk of real traffic sits well
above it and only a genuinely anomalous event should cross it.

THREE THINGS THIS FILE PROVES, each with its own test:

1. The ratio is observed, and observed with the EXACT value the alert's own
   formula would compute (not merely "a sample exists").
2. The benign ``prior_pool == 0`` class (release-without-reserve, by far the
   majority of debits — see the docstring on
   ``billing_budget_release_without_reserve_total``) must NOT feed this
   histogram. Folding it in would dilute the ratio with a population that was
   never an over-release at all, the exact kind of series-merging defect this
   repo's alert-vacuity audits keep finding elsewhere.
3. A ratio below the alert's own 0.5 escalation threshold is reachable and
   representable by this instrument — otherwise "expressible in PromQL" would
   be true in name only, with no test ever having reached the branch the
   PromQL rule is written to catch.

Conventions copied from ``tests/test_budget_reservation_identity.py``: the
``fake_redis`` fixture, the ``T(...)`` tenant-id helper, ``make_budget`` via a
local ``_budget``, and committing consumed tokens directly via
``_commit_consumed`` rather than through ``debit_budget`` (which would call
``release_reservation`` a second time and confound the histogram deltas this
file asserts on).

HISTOGRAM-READING IDIOM: ``tests/test_pricing.py`` (``_malformed_count``) is
the idiom already established in this repo for reading one
``prometheus_client`` instrument's current value — ``REGISTRY.get_sample_value``
by exact sample name, read as a DELTA around the call because the metric is a
module-level singleton shared across the whole test session. A ``Histogram``
exposes no single "current value" sample; ``prometheus_client`` instead emits
child samples ``<name>_sum`` and ``<name>_count`` (no buckets are asserted on
here — the buckets are a query-time aggregation concern, not a per-observation
one). Reading ``_sum`` and ``_count`` before/after and taking the count delta
as "how many observations" and the sum delta divided by that count as "the
value of the observation(s) just made" is the direct extension of the same
idiom to a Histogram, not a different one.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from prometheus_client import REGISTRY
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import TokenBudget
from services.budget_redis import release_reservation, reserve_budget
from tests._budget_factory import make_budget
from tests.conftest import T


def _budget(*, tenant: str, cap: int, consumed: int = 0) -> TokenBudget:
    """Active monthly budget whose consumption belongs to THIS period.

    Same shape as ``tests/test_budget_reservation_identity.py::_budget`` — the
    factory's default ``consumed_period_start`` anchors consumption to
    ``now``'s period, so these tests cannot go red on the 1st of a month.
    """
    return make_budget(tenant=tenant, cap=cap, consumed=consumed, period="monthly", source="test")


async def _commit_consumed(db_session: AsyncSession, budget: TokenBudget, tokens: int) -> None:
    """Commit ``tokens`` of real spend, the way a settled debit does.

    Written directly rather than via ``debit_budget`` so each test exercises
    exactly ONE call to ``release_reservation`` — ``debit_budget`` calls it
    itself, which would add a second, uncontrolled observation (or a second
    without-reserve increment) on top of the one each test means to measure.
    """
    budget.consumed_tokens = Decimal(tokens)
    await db_session.commit()


def _histogram_sum_and_count() -> tuple[float, float]:
    """(sum, count) of ``billing_budget_reserve_estimate_ratio`` right now.

    Unlabelled Histogram, so the sample names carry no label dict — exactly
    the ``REGISTRY.get_sample_value(name)`` call ``tests/test_pricing.py``
    already uses for a Counter, extended to the two child samples a Histogram
    exposes. Read BEFORE and AFTER a call and take the DELTA: this is a
    module-level ``prometheus_client`` object shared across the whole test
    session, so an absolute value is not a safe assertion.
    """
    total = REGISTRY.get_sample_value("billing_budget_reserve_estimate_ratio_sum")
    count = REGISTRY.get_sample_value("billing_budget_reserve_estimate_ratio_count")
    return float(total or 0.0), float(count or 0.0)


@pytest.mark.unit
class TestBudgetReserveEstimateRatio:
    async def test_over_release_records_the_estimate_ratio(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """An over-release against a LIVE pool must observe prior_pool / released.

        IN PRODUCTION IF THIS FAILS: the one PromQL-expressible signal for
        ``BillingBudgetReserveOverRelease``'s own documented escalation
        condition (``prior_pool / released < 0.5``) either never fires or
        fires with the wrong number, and the alert's annotation — which
        promises this ratio is now exported — becomes false again.

        Pool ends at 500 (A reserves 500); the settlement commits 900 actual
        and releases 900, so ``prior_pool=500``, ``released=900``,
        ``overshoot=400``, and the exact ratio the alert's formula computes is
        500/900.
        """
        tenant = T("ratio-over-release")
        budget = _budget(tenant=tenant, cap=1000)
        db_session.add(budget)
        await db_session.commit()

        reserved = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=500,
            ttl_seconds=300,
        )
        assert reserved["allowed"] is True, reserved

        await _commit_consumed(db_session, budget, 900)
        sum_before, count_before = _histogram_sum_and_count()
        remaining = await release_reservation(fake_redis, tenant, 900)
        sum_after, count_after = _histogram_sum_and_count()

        assert remaining == 0, (
            f"precondition failed: releasing 900 against a 500 pool should clamp "
            f"to 0, got {remaining} — the over-release this test drives did not "
            "actually happen, so the ratio assertion below would be vacuous"
        )
        assert count_after - count_before == 1, (
            "exactly one over-release happened but the histogram's observation "
            f"count moved by {count_after - count_before}, not 1 — either the "
            "observe() call was skipped, or something else is also writing this "
            "series and the count can no longer be trusted as 'one event, one "
            "sample'"
        )
        observed_value = (sum_after - sum_before) / (count_after - count_before)
        expected_ratio = 500 / 900
        assert observed_value == pytest.approx(expected_ratio), (
            f"the histogram recorded {observed_value!r} for this over-release, "
            f"but prior_pool/released is {expected_ratio!r} (500/900) — a wrong "
            "ratio here is worse than a missing one, because "
            "BillingBudgetReserveOverRelease's escalation condition would "
            "evaluate a number that does not describe this event"
        )

    async def test_the_benign_no_pool_case_records_nothing(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """A release with prior_pool == 0 must not touch this histogram at all.

        IN PRODUCTION IF THIS FAILS: the release_without_reserve population —
        by far the majority of debits, since ``reserve_budget`` has one caller
        while ``release_reservation`` runs on every debit — dilutes the
        estimate-ratio histogram with a class of event that was never an
        over-release in the first place. The alert's escalation condition
        (mass below 0.5) would then be evaluated against a mixture of "a live
        pool was drained" and "there was never a pool to drain", which is
        exactly the kind of series-merging defect
        billing_budget_reserve_over_release_total's own split (benign vs
        critical) exists to prevent one level up.

        No reservation is ever made for this tenant, so the FIRST release this
        tenant sees has no pool to find: prior_pool == 0 by construction.
        """
        tenant = T("ratio-no-pool")
        db_session.add(_budget(tenant=tenant, cap=1000))
        await db_session.commit()

        sum_before, count_before = _histogram_sum_and_count()
        remaining = await release_reservation(fake_redis, tenant, 500)
        sum_after, count_after = _histogram_sum_and_count()

        assert remaining == 0, (
            f"precondition failed: releasing against a tenant with no prior "
            f"reservation should return 0, got {remaining}"
        )
        assert count_after == count_before, (
            f"the benign no-pool release moved the histogram's observation count "
            f"{count_before} -> {count_after}; this class of event "
            "(release_without_reserve) must never be charted on the series the "
            "over-release escalation rule reads, or a healthy fleet's majority "
            "traffic would pull the ratio's median toward whatever this benign "
            "case happens to compute"
        )
        assert sum_after == sum_before, (
            f"the histogram's sum moved ({sum_before} -> {sum_after}) with no "
            "matching count change — a sum that moves without a count moving is "
            "not a valid Histogram state and means something wrote to this "
            "series outside the one observe() call this module makes"
        )

    async def test_a_ratio_below_one_half_is_representable(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """A prior_pool/released ratio under the alert's 0.5 threshold must record as such.

        IN PRODUCTION IF THIS FAILS: this is the test that proves the
        escalation condition the alert documents can actually be REACHED by
        this instrument, not just theoretically computed by it. A histogram
        whose only tested value sits near the 0.91 baseline median would leave
        the <0.5 branch — the one the PromQL rule promotes to critical on —
        completely unexercised.

        Pool ends at 300 (A reserves 300); the settlement commits 900 and
        releases 900, giving prior_pool/released = 300/900 = 0.333..., which
        is below the 0.5 threshold and strictly below 1.0 by construction (the
        branch this histogram lives in is reached only when
        ``prior_pool < tokens``, i.e. a real overshoot occurred — see
        ``services/budget_redis.py``'s comment above the ``observe()`` call).
        Not asserting prior_pool == released, per the task's explicit
        constraint: this branch structurally cannot produce that, since it
        only runs when the pool was smaller than what was released.
        """
        tenant = T("ratio-below-half")
        budget = _budget(tenant=tenant, cap=1000)
        db_session.add(budget)
        await db_session.commit()

        reserved = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=300,
            ttl_seconds=300,
        )
        assert reserved["allowed"] is True, reserved

        await _commit_consumed(db_session, budget, 900)
        sum_before, count_before = _histogram_sum_and_count()
        remaining = await release_reservation(fake_redis, tenant, 900)
        sum_after, count_after = _histogram_sum_and_count()

        assert remaining == 0, (
            f"precondition failed: releasing 900 against a 300 pool should "
            f"clamp to 0, got {remaining}"
        )
        assert count_after - count_before == 1, (
            "expected exactly one observation for this over-release; got a "
            f"count delta of {count_after - count_before} instead"
        )
        observed_value = (sum_after - sum_before) / (count_after - count_before)
        assert observed_value < 0.5, (
            f"expected a ratio under the alert's 0.5 escalation threshold, got "
            f"{observed_value!r} — if this instrument cannot represent values "
            "below 0.5, BillingBudgetReserveOverRelease's documented escalation "
            "condition is unfirable in practice even though the series now "
            "exists, which is the same defect the alert's annotation complains "
            "about, just moved one level down"
        )
        assert observed_value == pytest.approx(300 / 900), (
            f"observed {observed_value!r}, expected 300/900 "
            f"({300 / 900!r}) — the value itself must be exact, not merely "
            "'somewhere under 0.5'"
        )
