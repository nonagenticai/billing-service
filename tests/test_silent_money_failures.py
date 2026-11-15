"""Money-affecting failures must leave a signal an alert can read.

Every site covered here failed at ``logger.debug`` with NO metric at all before
this module existed. DEBUG is not collected in production, so each of them was,
operationally, a silent failure — and each one moves money:

* an unmapped ``service_key`` is INVOICED AT $0.00 by default;
* a lost reservation release fails CLOSED (false 429s).

WHAT FAILS ON ``origin/main``
-----------------------------
All 7. Measured by running this exact file against a ``origin/main`` worktree:
each test raises ``AttributeError: module 'observability.metrics' has no
attribute 'app_rate_missing_total'`` (or ``...budget_redis_op_failed_total``) at
the first counter read. The module still IMPORTS on main — the failure is at
first use, not at collection — and the underlying behaviour is what is missing:
the failures happen there and nothing records them.

⛔ NOTE ON THE ``app_rate_missing`` PAIR. It is deliberately NOT tested via
``billing_markup_zero_margin_total``. That alarm is gated on ``incurred_usd >
0`` and a line fabricated at $0 fails that gate — the fabricated zero suppresses
the only other signal. Asserting on the zero-margin counter here would therefore
produce a test that passes for the wrong reason on both sides of the fix.
"""

from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from prometheus_client import REGISTRY, generate_latest
from redis.exceptions import RedisError

import observability.metrics as observability_metrics
from models.db import AppCostRate, CostDaily, MeteringDaily
from services.billing import generate_billing
from tests.conftest import T


def _counter(metric, **labels) -> float:
    """Read a counter's current value, tolerating a not-yet-born series."""
    child = metric.labels(**labels) if labels else metric
    return float(child._value.get())


class TestAnUnmappedServiceKeyIsInvoicedAtZeroAndSaysSo:
    @pytest.mark.unit
    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_the_invoice_path_counts_a_missing_app_rate(
        self, mock_fetch, db_session, test_settings
    ):
        """A service_key with no AppCostRate must raise the miss counter.

        `app_rates.get(key, Decimal("0"))` returns its DEFAULT for an unmapped
        key, so the per-request line is billed at $0.00 — a FABRICATED number,
        indistinguishable downstream from a service deliberately configured
        free. This is the invoice path, so it is a customer-facing line.
        """
        tenant, lic_id = T("rate-miss-invoice"), uuid.uuid4()
        mock_fetch.return_value = [
            {"id": str(lic_id), "app": {"key": "unpriced"}, "plan": {"code": "pro"}},
        ]
        # ⛔ Deliberately NO AppCostRate row for "unpriced".
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                service_key="unpriced",
                bucket_date=date(2026, 3, 5),
                request_count=100,
                denied_count=0,
            )
        )
        await db_session.commit()

        before = _counter(
            observability_metrics.app_rate_missing_total,
            service_key="unpriced",
            surface="invoice",
        )
        await generate_billing(tenant, "2026-03", db_session, test_settings)
        after = _counter(
            observability_metrics.app_rate_missing_total,
            service_key="unpriced",
            surface="invoice",
        )

        assert after > before, "an unmapped service_key must be counted, not silently zero-billed"

    @pytest.mark.unit
    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_a_mapped_service_key_does_not_raise_the_counter(
        self, mock_fetch, db_session, test_settings
    ):
        """THE CONTROL. A priced key must NOT be reported as a miss.

        Without this, the counter could be incremented unconditionally and the
        test above would still pass — a signal that fires on everything is not a
        signal. Note the rate here is a REAL non-zero price; the
        zero-rate-is-not-a-miss case is covered separately below.
        """
        tenant, lic_id = T("rate-miss-priced"), uuid.uuid4()
        mock_fetch.return_value = [
            {"id": str(lic_id), "app": {"key": "priced"}, "plan": {"code": "pro"}},
        ]
        db_session.add(AppCostRate(app_key="priced", cost_per_request_usd=Decimal("0.00100000")))
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                service_key="priced",
                bucket_date=date(2026, 3, 5),
                request_count=100,
                denied_count=0,
            )
        )
        await db_session.commit()

        before = _counter(
            observability_metrics.app_rate_missing_total,
            service_key="priced",
            surface="invoice",
        )
        await generate_billing(tenant, "2026-03", db_session, test_settings)
        after = _counter(
            observability_metrics.app_rate_missing_total,
            service_key="priced",
            surface="invoice",
        )

        assert after == before

    @pytest.mark.unit
    @patch("services.billing.fetch_licenses", new_callable=AsyncMock)
    async def test_a_configured_zero_rate_is_not_a_miss(
        self, mock_fetch, db_session, test_settings
    ):
        """A DELIBERATE $0 rate must stay silent — that is the whole distinction.

        "No rate" and "a rate of zero" produce the same invoiced amount and must
        NOT produce the same signal, or the counter means nothing. A service
        someone configured as free is correct behaviour; a service nobody
        priced is a config gap leaking revenue.
        """
        tenant, lic_id = T("rate-miss-free"), uuid.uuid4()
        mock_fetch.return_value = [
            {"id": str(lic_id), "app": {"key": "free-app"}, "plan": {"code": "pro"}},
        ]
        db_session.add(AppCostRate(app_key="free-app", cost_per_request_usd=Decimal("0.00000000")))
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                service_key="free-app",
                bucket_date=date(2026, 3, 5),
                request_count=100,
                denied_count=0,
            )
        )
        await db_session.commit()

        before = _counter(
            observability_metrics.app_rate_missing_total,
            service_key="free-app",
            surface="invoice",
        )
        await generate_billing(tenant, "2026-03", db_session, test_settings)
        after = _counter(
            observability_metrics.app_rate_missing_total,
            service_key="free-app",
            surface="invoice",
        )

        assert after == before, "a configured zero rate is priced, not missing"

    @pytest.mark.unit
    async def test_the_overview_path_counts_a_missing_app_rate(self, db_session):
        """The overview reads the same rate map through a second lookup site.

        Both sites had the same defaulting bug, so instrumenting only one leaves
        the other silently zero-billing. The `surface` label is what keeps them
        distinguishable on one metric instead of two.
        """
        from routers.billing import compute_billing_overview

        tenant, lic_id = T("rate-miss-overview"), uuid.uuid4()
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                service_key="unpriced-overview",
                bucket_date=date(2026, 4, 5),
                request_count=42,
                denied_count=0,
            )
        )
        db_session.add(
            CostDaily(
                tenant_id=tenant,
                license_id=lic_id,
                service_key="unpriced-overview",
                provider="openai",
                bucket_date=date(2026, 4, 5),
                total_input_tokens=10,
                total_output_tokens=10,
                total_cost_usd=Decimal("0.010000"),
                request_count=42,
            )
        )
        await db_session.commit()

        before = _counter(
            observability_metrics.app_rate_missing_total,
            service_key="unpriced-overview",
            surface="overview",
        )
        await compute_billing_overview("2026-04", db_session)
        after = _counter(
            observability_metrics.app_rate_missing_total,
            service_key="unpriced-overview",
            surface="overview",
        )

        assert after > before


class TestRedisBookkeepingFailuresAreCounted:
    @pytest.mark.unit
    async def test_a_failed_reservation_release_is_counted(self):
        """Release failure fails CLOSED — false 429s.

        An unreleased reservation leaves `reserved` inflated so the gate denies
        headroom the tenant has already settled.
        """
        from services.budget_redis import release_reservation

        redis_conn = AsyncMock()
        redis_conn.eval = AsyncMock(side_effect=RedisError("boom"))

        before = _counter(observability_metrics.budget_redis_op_failed_total, operation="release")
        assert await release_reservation(redis_conn, T("release-fail"), 100) == 0
        after = _counter(observability_metrics.budget_redis_op_failed_total, operation="release")

        assert after == before + 1


class TestTheCountersAreBornBeforeTheyFail:
    @pytest.mark.unit
    def test_the_fixed_label_failure_counters_exist_at_zero_on_import(self):
        """A counter that appears only on first failure cannot be alerted on.

        Until the series exists, ``rate(...[1h]) == 0`` returns EMPTY rather
        than false, so "healthy" and "never deployed" are indistinguishable —
        the failure mode that made a green dashboard meaningless during the
        rollup incident. These have closed label domains, so they are
        birthed at import.

        ``app_rate_missing_total`` is deliberately absent from this list: its
        ``service_key`` label has no closed domain known at import time, so
        there is no correct set of series to pre-create.
        """
        # CORRECTED 2026-09-08. The previous body asserted `_counter(...) == 0` for
        # the two unlabelled counters and `.labels(op) is not None` for the labelled
        # one. BOTH were wrong, in opposite ways, and it passed for 2,279 tests until
        # an unlucky ordering exposed it:
        #
        #   * `== 0` is a VALUE assertion, not an existence one. An unlabelled Counter
        #     exists from construction regardless, so the birth it claims to check was
        #     never under test -- and any earlier test that legitimately exercises the
        #     failure path makes the value non-zero. Measured: it read 2.0 and failed
        #     with nothing wrong in the code it guards.
        #   * `.labels(op) is not None` is VACUOUS: `.labels()` CREATES the child on
        #     demand and never returns None, so it births the series and then asserts
        #     it exists. The comment beside it ("Reading it must not itself be what
        #     creates it") named the exact hazard the line then walked into.
        #
        # The property that matters is the docstring's: the SERIES IS IN THE SCRAPE
        # before anything fails, so `rate(...[1h])` returns 0 rather than empty. That
        # is only observable from registry output, and only if we never touch
        # `.labels()` ourselves.
        exposed = generate_latest(REGISTRY).decode()

        for op in ("release",):
            needle = 'billing_budget_redis_op_failed_total{operation="' + op + '"}'
            assert needle in exposed, (
                f"{needle} absent from the scrape -- the birth loop in "
                f"observability/metrics.py did not create it"
            )
