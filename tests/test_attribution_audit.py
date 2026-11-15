"""The coherence audit: a licence spending against a tenant that does not own it.

Measured on prod 2026-08-25: of 18,969 licence-bearing cost events in a 7-day
window, 11,407 ($363.56) named a licence whose owning tenant is NOT the tenant
the event is billed to. Nothing counted it. Because `debit_budget` scopes the
cascade to the EVENT's tenant, that spend drains the event tenant's shared pool
while the named licence's own budget stays untouched -- licence
`019f82c0-...` read 1,321 consumed against 94.6M tokens in the stream, so the
enforcement gate saw 0.0% of a 10M budget while reality was 946%.

These tests pin the classification, not the SQL. The pure functions take rows so
a regression is caught without a database; the SQL that feeds them is proven
against prod separately.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from models.db import NULL_LICENSE_ID
from services.attribution_audit import (
    AttributionAudit,
    BudgetUtilisation,
    classify_attribution,
    summarise_budget_utilisation,
)


def _lic() -> str:
    return str(uuid.uuid4())


class TestClassifyAttribution:
    def test_matching_owner_is_not_a_mismatch(self) -> None:
        lic, tenant = _lic(), str(uuid.uuid4())
        audit = classify_attribution(
            rows=[(lic, tenant, 3, 1.50)],
            owner_by_license={lic: tenant},
        )
        # Asserted as an ABSENCE: a correct pairing must not register at all.
        assert audit.mismatch_events == 0
        assert audit.mismatch_spend_usd == pytest.approx(0.0)
        assert audit.matched_events == 3

    def test_foreign_owner_is_counted_with_its_spend(self) -> None:
        lic, billed, owner = _lic(), str(uuid.uuid4()), str(uuid.uuid4())
        audit = classify_attribution(
            rows=[(lic, billed, 11407, 363.56)],
            owner_by_license={lic: owner},
        )
        assert audit.mismatch_events == 11407
        assert audit.mismatch_spend_usd == pytest.approx(363.56)
        assert audit.matched_events == 0

    def test_licence_absent_from_the_map_is_unresolved_not_a_mismatch(self) -> None:
        """An unknown licence is honestly unknown.

        Counting it as a mismatch would inflate the signal with rows we cannot
        adjudicate, which is the same defect class as billing a NULL.
        """
        lic = _lic()
        audit = classify_attribution(rows=[(lic, str(uuid.uuid4()), 5, 2.0)], owner_by_license={})
        assert audit.mismatch_events == 0
        assert audit.unresolved_events == 5

    def test_comparison_is_type_agnostic(self) -> None:
        """tenant_id is mid-migration from str to uuid across this repo.

        A UUID object and its string form name the SAME tenant; comparing them
        raw would report every event as a mismatch the moment the column type
        flips under us.
        """
        lic = _lic()
        tenant = uuid.uuid4()
        audit = classify_attribution(
            rows=[(lic, tenant, 4, 1.0)],
            owner_by_license={lic: str(tenant)},
        )
        assert audit.mismatch_events == 0

    def test_empty_population_is_reported_as_empty_not_clean(self) -> None:
        audit = classify_attribution(rows=[], owner_by_license={})
        assert audit.total_events == 0
        assert audit.mismatch_events == 0


class TestSummariseBudgetUtilisation:
    def test_a_budget_over_its_limit_is_reported(self) -> None:
        now = datetime(2026, 8, 25, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="t1",
                    budget_tokens=90_000_000,
                    consumed_tokens=264_243_479,
                    stale=False,
                )
            ],
            now,
        )
        assert util.over_limit == 1
        assert util.max_ratio == pytest.approx(264_243_479 / 90_000_000)

    def test_a_rolled_period_is_zero_not_over_limit(self) -> None:
        """`consumed_tokens` is stale across a period roll.

        models/db/budgets.py says every reader must go through
        `effective_consumed`; reading the raw column would report a budget whose
        month has rolled as permanently over-limit and alert forever.
        """
        now = datetime(2026, 8, 25, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="t1",
                    budget_tokens=90_000_000,
                    consumed_tokens=264_243_479,
                    stale=True,
                )
            ],
            now,
        )
        assert util.over_limit == 0
        assert util.max_ratio == pytest.approx(0.0)

    def test_exactly_at_the_limit_is_not_over(self) -> None:
        now = datetime(2026, 8, 25, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="t1",
                    budget_tokens=100,
                    consumed_tokens=100,
                    stale=False,
                )
            ],
            now,
        )
        assert util.over_limit == 0

    def test_a_zero_budget_does_not_divide_by_zero(self) -> None:
        """budget_tokens == 0 means BLOCK-ALL, not unlimited (see budget_debit)."""
        now = datetime(2026, 8, 25, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="t1",
                    budget_tokens=0,
                    consumed_tokens=5,
                    stale=False,
                )
            ],
            now,
        )
        assert util.over_limit == 1
        assert util.max_ratio >= 1.0


class TestTenantAggregateIsNotThePerRowMax:
    """The per-row arm and the enforceable arm disagree, and must both be right.

    ⛔ This class exists because of a real false alarm on 2026-09-10: the per-row
    gauge read **4.77** and was escalated as "a cap exceeded 4.77x, losing money
    now", with an operator go-ahead obtained to refuse live production traffic.
    The two tenants involved were at **57.2%** and **22.5%** of their aggregate
    caps and nothing was overspending. ``reserve_budget`` enforces
    ``consumed + reserved + estimated <= sum(budget_tokens)``, so the aggregate
    is the only arm that means "over cap".
    """

    def test_the_debit_cascade_does_not_read_as_a_tenant_breach(self) -> None:
        """Reproduces the shape that produced 4.77 against 57.2%.

        ``budget_debit`` walks a tenant's budgets taking ``min(room, wants)`` and
        dumps the remainder on the LAST row visited. So filled non-last rows sit
        at exactly their cap and the overflow row reads far above its own -- while
        the tenant has ample aggregate headroom.
        """
        now = datetime(2026, 9, 10, 11, 0, tzinfo=UTC)
        rows = [
            # Two small rows filled to exactly their caps by the cascade.
            BudgetUtilisation(
                budget_id=uuid.uuid4(),
                tenant_id="over-cap-looking",
                budget_tokens=10_000_000,
                consumed_tokens=10_000_000,
                stale=False,
            ),
            # The last row visited carries the remainder: 4.77x its OWN cap.
            BudgetUtilisation(
                budget_id=uuid.uuid4(),
                tenant_id="over-cap-looking",
                budget_tokens=10_000_000,
                consumed_tokens=47_717_407,
                stale=False,
            ),
            # Headroom the per-row max cannot see.
            BudgetUtilisation(
                budget_id=uuid.uuid4(),
                tenant_id="over-cap-looking",
                budget_tokens=220_000_000,
                consumed_tokens=79_497_988,
                stale=False,
            ),
        ]
        util = summarise_budget_utilisation(rows, now)

        # The per-row arm still reports what it always did -- unchanged behaviour.
        assert util.over_limit == 1
        assert util.max_ratio == pytest.approx(47_717_407 / 10_000_000)
        assert util.max_ratio > 4.0

        # The enforceable arm says the truth: 137,215,395 / 240,000,000 = 0.5717.
        assert util.tenants_considered == 1
        assert util.tenants_over_limit == 0
        assert util.tenant_max_ratio == pytest.approx(137_215_395 / 240_000_000)
        assert util.tenant_max_ratio < 1.0

    def test_a_genuine_aggregate_breach_is_reported(self) -> None:
        """The negative control's mirror: a real overspend must still be caught."""
        now = datetime(2026, 9, 10, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="genuinely-over",
                    budget_tokens=10_000_000,
                    consumed_tokens=9_000_000,
                    stale=False,
                ),
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="genuinely-over",
                    budget_tokens=10_000_000,
                    consumed_tokens=12_000_000,
                    stale=False,
                ),
            ],
            now,
        )
        assert util.tenants_over_limit == 1
        assert util.tenant_max_ratio == pytest.approx(21_000_000 / 20_000_000)

    def test_tenants_are_not_pooled_with_each_other(self) -> None:
        """One tenant's headroom must never absorb another's overspend.

        Without a per-tenant grouping key these two would net to 1.0 and the
        breach would vanish -- the pooling error in the other direction.
        """
        now = datetime(2026, 9, 10, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="spender",
                    budget_tokens=1_000,
                    consumed_tokens=3_000,
                    stale=False,
                ),
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="idler",
                    budget_tokens=1_000_000,
                    consumed_tokens=0,
                    stale=False,
                ),
            ],
            now,
        )
        assert util.tenants_considered == 2
        assert util.tenants_over_limit == 1
        assert util.tenant_max_ratio == pytest.approx(3.0)

    def test_a_stale_row_keeps_its_cap_in_the_denominator(self) -> None:
        """A rolled period contributes no consumption but still holds its cap.

        Dropping the cap too would shrink the denominator and INFLATE the tenant
        ratio -- manufacturing a breach out of a period roll.
        """
        now = datetime(2026, 9, 10, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="rolled",
                    budget_tokens=100,
                    consumed_tokens=500,
                    stale=True,
                ),
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="rolled",
                    budget_tokens=100,
                    consumed_tokens=50,
                    stale=False,
                ),
            ],
            now,
        )
        assert util.tenants_over_limit == 0
        # 50 / 200, NOT 50 / 100.
        assert util.tenant_max_ratio == pytest.approx(0.25)

    def test_a_tenant_whose_every_budget_is_zero_is_blocked_not_unlimited(self) -> None:
        """0 is BLOCK-ALL at the aggregate grain too (see budget_debit)."""
        now = datetime(2026, 9, 10, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="blocked",
                    budget_tokens=0,
                    consumed_tokens=7,
                    stale=False,
                )
            ],
            now,
        )
        assert util.tenants_over_limit == 1
        assert util.tenant_max_ratio >= 1.0

    def test_no_budgets_reports_no_tenants_rather_than_a_zero_breach(self) -> None:
        """An empty estate must not look like a healthy one OR a broken one."""
        util = summarise_budget_utilisation([], datetime(2026, 9, 10, 11, 0, tzinfo=UTC))
        assert util.tenants_considered == 0
        assert util.tenants_over_limit == 0
        assert util.tenant_max_ratio == pytest.approx(0.0)


class TestLicenceGrainIsNotTheEnforcedGrain:
    """The DEBIT grain: real, previously unmeasured, and enforced by nothing.

    ⚠️ This class is the sibling of ``TestTenantAggregateIsNotThePerRowMax``
    above, NOT a replacement for it, and it must never be read as softening it.
    That class pins "the tenant aggregate is the only arm that means over cap".
    This one adds a THIRD number that will routinely stand above 1.0 while the
    gate is working perfectly, and pins that the two are distinguishable.

    Why a third grain exists at all: ``cost_flusher`` debits with the event's
    ``license_id``, so ``_select_active_budgets`` narrows the cascade to that
    licence's own rows PLUS the shared tenant-level pool. ``reserve_budget`` --
    the only thing that can refuse a request -- is reached from inference-router
    with NO ``license_id``, so admission is decided over the whole tenant pool.
    Measured 2026-09-11, each licence with ITS OWN tenant's enforced aggregate:
    205% (tenant at 14.7%), 194% (24.8%), 145% (60.3%). The two series do not
    sort alike -- read them as pairs, never positionally.
    """

    def test_a_licence_over_its_scope_while_the_tenant_is_far_under_cap(self) -> None:
        """⭐ THE EXACT SHAPE THAT PRODUCED THE 2026-09-11 FALSE ALARM.

        One licence oversubscribed, the tenant nowhere near its cap. Both
        figures must come out of one pass, both correct, and the enforced one
        must be the one that says nothing was refused.
        """
        now = datetime(2026, 9, 11, 11, 0, tzinfo=UTC)
        lic_a, lic_b = uuid.uuid4(), uuid.uuid4()
        tenant = "looks-breached-at-the-debit-grain"
        rows = [
            # The licence the debits piled onto: 2.05x its OWN row.
            BudgetUtilisation(
                budget_id=uuid.uuid4(),
                tenant_id=tenant,
                budget_tokens=10_000_000,
                consumed_tokens=20_500_000,
                stale=False,
                license_id=lic_a,
            ),
            # A second licence holding most of the tenant's cap, untouched.
            BudgetUtilisation(
                budget_id=uuid.uuid4(),
                tenant_id=tenant,
                budget_tokens=100_000_000,
                consumed_tokens=0,
                stale=False,
                license_id=lic_b,
            ),
            # The shared pool. EVERY licence of this tenant can drain it, so it
            # belongs to every licence scope as well as being one itself.
            BudgetUtilisation(
                budget_id=uuid.uuid4(),
                tenant_id=tenant,
                budget_tokens=1_000_000,
                consumed_tokens=0,
                stale=False,
                license_id=NULL_LICENSE_ID,
            ),
        ]
        util = summarise_budget_utilisation(rows, now)

        # ---- the ENFORCED arm: nothing is over cap, nothing was refused.
        assert util.tenants_considered == 1
        assert util.tenants_over_limit == 0
        assert util.tenant_max_ratio == pytest.approx(20_500_000 / 111_000_000)
        assert util.tenant_max_ratio < 1.0

        # ---- the DEBIT arm: one licence scope genuinely oversubscribed.
        # Three scopes: lic_a, lic_b, and the pool on its own (licence-less
        # system/S2S spend debits the pool and nothing else).
        assert util.licences_considered == 3
        assert util.licences_over_limit == 1
        assert util.licence_max_ratio == pytest.approx(20_500_000 / 11_000_000)
        assert util.licence_max_ratio > 1.0

        # ---- and the two arms must be DISTINGUISHABLE, which is the whole
        # point. A summariser that folded every row into one bucket would make
        # the licence figure equal the tenant figure and this class vacuous.
        assert util.licence_max_ratio != pytest.approx(util.tenant_max_ratio)

        # ⛔ The licence scope is NOT the bare row. 20.5M/10M = 2.05 is the
        # PER-ROW number; the debit cascade for lic_a can also drain the 1M
        # pool, so its scope denominator is 11M. A grouping that dropped the
        # pool would report 2.05 here and be indistinguishable from the per-row
        # arm -- i.e. a fourth way to manufacture the same false alarm.
        assert util.licence_max_ratio != pytest.approx(20_500_000 / 10_000_000)
        assert util.max_ratio == pytest.approx(20_500_000 / 10_000_000)
        assert util.over_limit == 1

    def test_an_untouched_licence_is_over_when_the_shared_pool_is_exhausted(self) -> None:
        """A licence's headroom is not its own.

        ``_select_active_budgets`` gives licence A the pool too, so a pool the
        rest of the tenant has drained leaves A unable to spend even though A's
        own row is empty. Dropping the pool from the scope would report A at
        0.0 and hide exactly that.
        """
        now = datetime(2026, 9, 11, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="pool-drained",
                    budget_tokens=10_000_000,
                    consumed_tokens=30_000_000,
                    stale=False,
                    license_id=NULL_LICENSE_ID,
                ),
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="pool-drained",
                    budget_tokens=1_000_000,
                    consumed_tokens=0,
                    stale=False,
                    license_id=uuid.uuid4(),
                ),
            ],
            now,
        )
        # BOTH scopes: the pool itself (3.0) and the licence that draws on it
        # (30M / 11M). One of them alone would mean the fold-in was dropped.
        assert util.licences_considered == 2
        assert util.licences_over_limit == 2
        assert util.licence_max_ratio == pytest.approx(3.0)

    def test_a_licence_never_draws_on_another_tenants_pool(self) -> None:
        """The pool fold-in is PER TENANT, and that is the direction that matters.

        ``_select_active_budgets`` filters on ``tenant_id`` before it filters on
        the licence scope, so tenant A's licence can only ever see tenant A's
        pool. A pool accumulated globally instead of per tenant would give this
        spender a 10M denominator it has no claim to and erase the breach --
        the pooling error in the other direction, exactly as
        ``test_tenants_are_not_pooled_with_each_other`` pins for the tenant arm.
        """
        now = datetime(2026, 9, 11, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                # A big untouched pool belonging to a DIFFERENT tenant.
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="idler",
                    budget_tokens=10_000_000,
                    consumed_tokens=0,
                    stale=False,
                    license_id=NULL_LICENSE_ID,
                ),
                # A licence with no pool of its own, 5x over its only cap.
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="spender",
                    budget_tokens=1_000,
                    consumed_tokens=5_000,
                    stale=False,
                    license_id=uuid.uuid4(),
                ),
            ],
            now,
        )
        assert util.licences_considered == 2
        assert util.licences_over_limit == 1
        # 5,000 / 1,000 -- NOT 5,000 / 10,001,000.
        assert util.licence_max_ratio == pytest.approx(5.0)

    def test_a_stale_row_keeps_its_cap_in_the_licence_denominator(self) -> None:
        """Same rule as the tenant arm, for the same reason.

        Dropping a rolled row's CAP as well as its consumption would shrink the
        scope denominator and manufacture a licence breach out of a period roll.
        Also pins that two rows of the SAME licence aggregate into one scope.
        """
        now = datetime(2026, 9, 11, 11, 0, tzinfo=UTC)
        lic = uuid.uuid4()
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="rolled",
                    budget_tokens=10_000_000,
                    consumed_tokens=50_000_000,
                    stale=True,
                    license_id=lic,
                ),
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="rolled",
                    budget_tokens=10_000_000,
                    consumed_tokens=5_000_000,
                    stale=False,
                    license_id=lic,
                ),
            ],
            now,
        )
        assert util.licences_considered == 1
        assert util.licences_over_limit == 0
        # 5M / 20M, NOT 5M / 10M and NOT 55M / 20M.
        assert util.licence_max_ratio == pytest.approx(0.25)

    def test_a_tenant_holding_only_pool_rows_still_reports_one_scope(self) -> None:
        """Licence-less system/S2S spend must be measured at SOME debit grain.

        It debits the pool and only the pool, so the pool is a scope in its own
        right. Reporting zero scopes here would leave that population visible
        only through the per-row arm, which is the artifact arm.
        """
        now = datetime(2026, 9, 11, 11, 0, tzinfo=UTC)
        util = summarise_budget_utilisation(
            [
                BudgetUtilisation(
                    budget_id=uuid.uuid4(),
                    tenant_id="s2s-only",
                    budget_tokens=100,
                    consumed_tokens=40,
                    stale=False,
                    license_id=NULL_LICENSE_ID,
                )
            ],
            now,
        )
        assert util.licences_considered == 1
        assert util.licence_max_ratio == pytest.approx(0.4)
        assert util.licence_max_ratio == pytest.approx(util.tenant_max_ratio)

    def test_no_budgets_reports_no_licence_scopes_rather_than_a_zero_breach(self) -> None:
        util = summarise_budget_utilisation([], datetime(2026, 9, 11, 11, 0, tzinfo=UTC))
        assert util.licences_considered == 0
        assert util.licences_over_limit == 0
        assert util.licence_max_ratio == pytest.approx(0.0)


class TestRoundSixteenSeriesAreBornAtZero:
    """The R16 families must be EXPORTED before anyone calls `.labels()`.

    ⛔ An adversarial audit of PR #220 deleted BOTH pre-materialisation loops
    (`PROJECT_ID_REFUSED_REASONS`, `PROJECT_MULTI_TENANT_SCOPES`) and the whole
    2185-test suite stayed green -- nothing tested the property four docstrings
    in that diff call load-bearing. That is the inert-gauge class this round
    exists to close, committed by the change meant to close it.

    An in-process check would pass whether or not the module
    pre-materialised anything, because the assertion itself creates the series.
    A fresh interpreter that only RENDERS is the honest check.

    What an absent series would mean here, concretely: no
    `reason="registry_unavailable"` reads as "the keep-the-value fallback is
    untested"; no `scope="unknown"` reads as "no tenant was ever unclassifiable"
    -- and gitops #3980's alert selects `scope=~"customer|unknown"`, so a
    missing `unknown` series makes half that selector vacuous.
    """

    def test_both_families_render_without_anyone_touching_labels(self) -> None:
        import subprocess
        import sys

        prog = (
            "import observability.metrics as m;"
            "from prometheus_client import REGISTRY, generate_latest;"
            "out = generate_latest(REGISTRY).decode();"
            "missing = ["
            " 'billing_project_id_refused_total{reason=\"%s\"}' % r"
            " for r in m.PROJECT_ID_REFUSED_REASONS"
            " if 'billing_project_id_refused_total{reason=\"%s\"}' % r not in out"
            "] + ["
            " 'billing_project_id_multi_tenant_projects{scope=\"%s\"}' % s"
            " for s in m.PROJECT_MULTI_TENANT_SCOPES"
            " if 'billing_project_id_multi_tenant_projects{scope=\"%s\"}' % s not in out"
            "];"
            "print('MISSING:' + ','.join(missing))"
        )
        proc = subprocess.run(
            [sys.executable, "-c", prog],
            capture_output=True,
            text=True,
            cwd="src",
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        assert "MISSING:" in proc.stdout, proc.stdout
        missing = proc.stdout.split("MISSING:")[1].strip()
        assert missing == "", f"series absent from a fresh exposition: {missing}"


class TestDerivedSpendShareGauge:
    """The share of ATTRIBUTED dollars whose licence billing INFERRED.

    Measured 2026-09-10: 42.2%-83.3% across five days, while the estate reported
    98.6% "attributed". Both true; only one is about money.

    ⚠️ The first version of these tests asserted `derived_usd / attributed_usd ==
    approx(...)` -- i.e. Python's division operator, not this module. Every mutant
    survived. These drive `run_attribution_audit` and read the GAUGE.
    """

    async def test_the_gauge_is_published_from_the_query(self, monkeypatch) -> None:
        from observability import metrics as observability_metrics

        _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
        session = _StubSession(
            window=[],
            owners=[],
            budgets=[],
            # The measured 2026-09-09 figures.
            derived_spend=[SimpleNamespace(attributed_usd=402.67, derived_usd=169.76)],
        )
        await aa.run_attribution_audit(_factory(session), window_hours=24)
        assert observability_metrics.licence_derived_spend_share._value.get() == pytest.approx(
            169.76 / 402.67
        )

    async def test_an_empty_window_leaves_the_previous_value_standing(self, monkeypatch) -> None:
        """⛔ 0.0 would read "every attributed dollar was asserted by its emitter"
        -- the most flattering possible reading of no data, and indistinguishable
        from the goal state this gauge measures progress toward. Staleness is
        detected by billing:attribution_audit:fresh, as for attribution_rate.
        """
        from observability import metrics as observability_metrics

        _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
        sentinel = 0.6125
        observability_metrics.licence_derived_spend_share.set(sentinel)
        session = _StubSession(
            window=[],
            owners=[],
            budgets=[],
            derived_spend=[SimpleNamespace(attributed_usd=0.0, derived_usd=0.0)],
        )
        await aa.run_attribution_audit(_factory(session), window_hours=24)
        assert observability_metrics.licence_derived_spend_share._value.get() == pytest.approx(
            sentinel
        )

    async def test_the_sql_counts_only_plus_resolved_and_only_attributed_rows(
        self, db_engine
    ) -> None:
        """⚠️ A STUB CANNOT TEST SQL. The two mutations that matter here live in the
        statement -- dropping `license_id IS NOT NULL` and dropping the
        `LIKE '%+resolved'` arm -- and a stub answers by identity without ever
        parsing it, so both survived against the stub. This runs the real query.
        """
        import uuid as _uuid
        from decimal import Decimal

        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        from models.db import CostEvent

        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        lic = _uuid.uuid4()
        tenant = "0195f48b-d000-7000-8000-000000000001"
        rows = [
            # ASSERTED, attributed: counts toward the denominator only.
            ("inference-router", lic, "10.00", False),
            # DERIVED, attributed: counts toward BOTH.
            ("inference-router+resolved", lic, "30.00", False),
            # DERIVED-looking but UNATTRIBUTED: must be excluded entirely, or the
            # share falls whenever attribution gets worse.
            ("inference-router+resolved", None, "100.00", False),
            # Excluded from billing: never a billing figure.
            ("inference-router+resolved", lic, "500.00", True),
        ]
        async with factory() as session:
            for cost_source, license_id, usd, excluded in rows:
                session.add(
                    CostEvent(
                        tenant_id=tenant,
                        license_id=license_id,
                        service_key="inference-router",  # pragma: allowlist secret
                        provider="pgx-local",
                        model="local-llm",
                        served_model="local-llm",
                        input_tokens=1,
                        output_tokens=1,
                        cost_usd=Decimal(usd),
                        cost_source=cost_source,
                        excluded_from_billing=excluded,
                        correlation_id=_uuid.uuid4(),
                    )
                )
            await session.commit()

            floor = datetime.now(UTC) - timedelta(hours=24)
            row = (await session.execute(aa._DERIVED_SPEND_SQL, {"floor": floor})).one()

        # Denominator: the two attributed, non-excluded rows = 10 + 30.
        assert float(row.attributed_usd) == pytest.approx(40.00)
        # Numerator: only the attributed `+resolved` row = 30.
        assert float(row.derived_usd) == pytest.approx(30.00)
        assert float(row.derived_usd) / float(row.attributed_usd) == pytest.approx(0.75)


def test_audit_dataclass_reports_a_ratio_only_when_it_has_a_denominator() -> None:
    """A rate without its denominator invents a defect."""
    assert AttributionAudit(total_events=0).mismatch_ratio is None
    assert AttributionAudit(total_events=4, mismatch_events=1).mismatch_ratio == pytest.approx(0.25)


# ---------------------------------------------------------------------------
# The pure functions above are adjudication. These drive `run_attribution_audit`
# itself with a stub session, so the SQL plumbing and — more importantly — the
# wiring of each result onto its OWN gauge are covered. A mutant that sets
# `mismatch_events` from the matched count would otherwise ship green.
# ---------------------------------------------------------------------------

from contextlib import asynccontextmanager  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from observability import metrics as observability_metrics  # noqa: E402
from services import attribution_audit as aa  # noqa: E402


class _StubResult:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def all(self) -> list:
        return self._rows

    def one(self):
        """Single-row aggregates (_DERIVED_SPEND_SQL) call .one(), not .all()."""
        return self._rows[0]


class _StubSession:
    """Answers each of the module's statements by identity.

    ⚠️ ``owners`` FEEDS ``_OWNERS_SQL``, WHICH IS NO LONGER THE OWNERSHIP SOURCE.
    Since 2026-09-11 that statement reads ``request_quota_config`` purely so the
    audit can report how far the local proxy DISAGREES with the licences registry;
    the verdicts come from ``aa.fetch_licence_owners`` over HTTP. Say what the
    REGISTRY holds with :func:`_registry_says` / :func:`_registry_unavailable`.
    """

    def __init__(
        self,
        window,
        owners,
        budgets,
        unattributed=None,
        unregistered=None,
        derived_spend=None,
        self_tenant=None,
        spend_coverage=None,
        multi_tenant=None,
        emitter_coverage=None,
    ) -> None:
        self._by_sql = {
            id(aa._WINDOW_SQL): window,
            id(aa._OWNERS_SQL): owners,
            id(aa._BUDGETS_SQL): budgets,
            # The complement query: same window, licence-less half. Defaults to
            # an empty population so the tests below keep measuring exactly what
            # they were written to measure.
            id(aa._UNATTRIBUTED_SQL): unattributed
            if unattributed is not None
            else [SimpleNamespace(events=0, spend=0.0)],
            # Tenants absent from billing's own registry. Same defaulting
            # rationale as above.
            id(aa._UNREGISTERED_TENANT_SQL): unregistered if unregistered is not None else [],
            # Derived-vs-asserted licence spend. Defaults to a ZERO-dollar window
            # so every test written before this query keeps measuring exactly what
            # it measured -- and so the gauge's "publish nothing without a
            # denominator" guard is the path those tests take, which is the
            # correct default rather than a convenient one.
            id(aa._DERIVED_SPEND_SQL): derived_spend
            if derived_spend is not None
            else [SimpleNamespace(attributed_usd=0.0, derived_usd=0.0)],
            # The project axis (alembic 0036). Both default to a CLEAN estate --
            # no project id asserted under two tenants, and no project-attributed
            # rollup rows -- because nothing emits a project id yet. That is the
            # honest default: these two gauges publish a MEASURED zero, and every
            # test written before the axis existed keeps measuring exactly what it
            # measured.
            # ⚠️ R16: one row PER multi-tenant project, carrying its tenant_ids
            # (was a single `count(*) AS projects` row) -- see
            # `_PROJECT_MULTI_TENANT_SQL`'s R16 banner. Empty list is the same
            # "no crossing at all" default the old `projects=0` meant.
            id(aa._PROJECT_MULTI_TENANT_SQL): multi_tenant if multi_tenant is not None else [],
            id(aa._PROJECT_ATTRIBUTION_SQL): [SimpleNamespace(project_rows=0, sentinel_rows=0)],
            # The forged-project detector. Defaults to a CLEAN estate for the
            # same reason as the two above -- except that on PROD this one is
            # NOT clean (62 rows / $6.0458 on 2026-09-16). The default is what
            # every pre-existing test wants (measure only what it was written to
            # measure); the tests that care declare a forged population
            # explicitly via the `self_tenant` kwarg.
            id(aa._PROJECT_SELF_TENANT_SQL): self_tenant
            if self_tenant is not None
            else [SimpleNamespace(projects=0, events=0, spend=0.0)],
            # Dollar-denominated project coverage. Defaults to a ZERO-DOLLAR
            # window, not to a plausible split: a default with money in it would
            # let a test that never mentions coverage assert a number it did not
            # set up, and every pre-existing test here drives an empty window.
            id(aa._PROJECT_SPEND_COVERAGE_SQL): spend_coverage
            if spend_coverage is not None
            else [SimpleNamespace(project_usd=0.0, tenant_scoped_usd=0.0, unattributed_usd=0.0)],
            # Per-emitter coverage. Defaults to an EMPTY window (no service_key
            # rows) so pre-existing tests measure only what they were written to.
            id(aa._EMITTER_COVERAGE_SQL): emitter_coverage if emitter_coverage is not None else [],
        }
        self.seen: list[int] = []

    async def execute(self, sql, params=None):
        self.seen.append(id(sql))
        return _StubResult(self._by_sql[id(sql)])


def _factory(session):
    @asynccontextmanager
    async def _cm():
        yield session

    def _make():
        return _cm()

    return _make


# ---------------------------------------------------------------------------
# THE LICENCES REGISTRY, WHICH EVERY PASS NOW DEPENDS ON.
#
# `run_attribution_audit` resolves licence -> owning-tenant from the licences
# registry over HTTP (`aa.fetch_licence_owners`). It used to read
# `request_quota_config`, which is derived FROM the events being audited, so the
# audit could not fail: it reported 0 mismatches on a 24h window the registry
# scored at 188 events / $0.1242 across 4 license_ids.
#
# ⛔ EVERY TEST THAT DRIVES `run_attribution_audit` MUST DECLARE WHAT THE REGISTRY
# SAYS. The autouse guard below makes a test that forgets fail LOUDLY rather than
# quietly taking the abstention path, where the ownership gauges are not written
# at all and an assertion would read whatever the previous test left behind -- a
# flaky green, which is the worst of the three outcomes.
# ---------------------------------------------------------------------------


#: The REAL resolver, captured before any patching. The autouse guard below
#: replaces ``aa.fetch_licence_owners`` for every test in this module, so a test
#: that wants to exercise the resolver ITSELF (its URL, headers, status handling)
#: must call this binding -- otherwise it would only be testing the guard.
_REAL_FETCH_LICENCE_OWNERS = aa.fetch_licence_owners

#: Emitter label, held in a constant: detect-secrets reads a quoted value after
#: `service_key=` as a credential keyword; a name reference is not flagged.
EMITTER_X = "emitter-x"


@pytest.fixture(autouse=True)
def _registry_must_be_declared(monkeypatch):
    """Unpatched, the licences call is a hard error, never a default answer."""

    async def _undeclared(settings, *, token_provider=None, client=None):
        raise AssertionError(
            "this test drives run_attribution_audit without declaring what the "
            "licences registry holds -- call _registry_says(...) or "
            "_registry_unavailable(...)"
        )

    monkeypatch.setattr(aa, "fetch_licence_owners", _undeclared)


def _registry_says(monkeypatch, owners: dict, *, unowned: int = 0) -> list[dict]:
    """The registry answers with ``owners``. Returns the call log.

    The log is returned rather than discarded so a test can assert the call
    HAPPENED: a `_load` that stopped resolving ownership would otherwise leave
    every gauge at a previous test's value and look like a pass.
    """
    calls: list[dict] = []

    async def _fetch(settings, *, token_provider=None, client=None):
        calls.append({"settings": settings, "token_provider": token_provider})
        return dict(owners), unowned

    monkeypatch.setattr(aa, "fetch_licence_owners", _fetch)
    return calls


def _registry_unavailable(monkeypatch, reason: str = "licences unreachable") -> list[str]:
    """The registry cannot state ownership. Returns the call log."""
    calls: list[str] = []

    async def _fetch(settings, *, token_provider=None, client=None):
        calls.append(reason)
        raise aa.LicenceOwnersUnavailableError(reason)

    monkeypatch.setattr(aa, "fetch_licence_owners", _fetch)
    return calls


@pytest.mark.asyncio
async def test_run_audit_sets_each_gauge_from_its_own_number(monkeypatch) -> None:
    lic_bad, lic_ok = _lic(), _lic()
    billed, owner = str(uuid.uuid4()), str(uuid.uuid4())
    now = datetime.now(UTC)
    anchor = now.date().replace(day=1)

    fetched = _registry_says(monkeypatch, {lic_bad: owner, lic_ok: billed})

    session = _StubSession(
        window=[
            SimpleNamespace(
                license_id=lic_bad, tenant_id=billed, events=4828, spend=127.73, delegated=False
            ),
            SimpleNamespace(
                license_id=lic_ok, tenant_id=billed, events=1916, spend=40.0, delegated=False
            ),
        ],
        owners=[(lic_bad, owner), (lic_ok, billed)],
        budgets=[
            # Tenant-level pool rows (license_id = the zero-UUID sentinel), which
            # is what _BUDGETS_SQL now selects. These two assert the per-row and
            # tenant arms; the licence arm has its own tests below.
            SimpleNamespace(
                id=uuid.uuid4(),
                tenant_id="fake-tenant",
                license_id=NULL_LICENSE_ID,
                budget_tokens=90_000_000,
                consumed_tokens=264_243_479,
                period="monthly",
                consumed_period_start=anchor,
            ),
            SimpleNamespace(
                id=uuid.uuid4(),
                tenant_id="fake-tenant",
                license_id=NULL_LICENSE_ID,
                budget_tokens=50_000_000,
                consumed_tokens=1,
                period="monthly",
                consumed_period_start=anchor,
            ),
        ],
    )

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    # All EIGHT statements were actually issued — a stub that silently answered
    # one query for all of them would otherwise look identical.
    # ⚠️ Was 5 until _DERIVED_SPEND_SQL was added. This count is deliberately
    # exact rather than `>=`: it is the only thing that catches a new statement
    # being added to _load and then never issued, or an existing one being
    # quietly dropped. Bump it WITH the query, never to make the suite green.
    #
    # ⚠️ STILL 6 AFTER THE OWNERSHIP SOURCE MOVED TO HTTP: `_OWNERS_SQL` is still
    # issued, because `request_quota_config` is still read — as the SUBJECT of
    # `compare_local_owner_proxy` rather than as the authority. The vacuity floor
    # for the statement it REPLACED is the `fetched` assertion below; without that,
    # a `_load` that stopped resolving ownership entirely would keep this count at
    # 6 and look unchanged.
    # ⚠️ Was 6 until the project axis (alembic 0036) added
    # `_PROJECT_MULTI_TENANT_SQL` and `_PROJECT_ATTRIBUTION_SQL`. Bumped WITH the
    # queries, which is exactly what the note above sanctions — never to make the
    # suite green.
    # ⚠️ Was 8 until the project-axis AUDIT pair — `_PROJECT_SELF_TENANT_SQL`
    # (project_id == tenant_id) and `_PROJECT_SPEND_COVERAGE_SQL` (the dollar
    # coverage split) — was added. Bumped WITH the queries, same sanction.
    # ⚠️ Was 10 until `_EMITTER_COVERAGE_SQL` (per-emitter project/actor
    # coverage, issued beside `_load` in the same session). Same sanction.
    assert len(set(session.seen)) == 11
    # The registry was actually consulted, exactly once.
    assert len(fetched) == 1

    assert audit.owner_source == aa.OWNER_SOURCE_REGISTRY
    assert audit.coherence_measured is True

    assert audit.mismatch_events == 4828
    assert audit.matched_events == 1916
    assert audit.mismatch_spend_usd == pytest.approx(127.73)
    assert audit.unresolved_events == 0

    def _val(gauge):
        return gauge._value.get()

    assert _val(observability_metrics.license_tenant_mismatch_events) == 4828
    # The denominator is on its own gauge, not folded into the mismatch one.
    assert _val(observability_metrics.license_attributed_events) == 1916
    assert _val(observability_metrics.license_tenant_mismatch_spend_usd) == pytest.approx(127.73)
    assert _val(observability_metrics.budgets_over_limit) == 1
    assert _val(observability_metrics.budget_max_utilisation_ratio) == pytest.approx(
        264_243_479 / 90_000_000
    )


@pytest.mark.asyncio
async def test_an_empty_window_reports_no_ratio_and_clears_the_gauges(monkeypatch) -> None:
    """Silence must not read as health, and yesterday's number must not persist.

    ⚠️ The registry deliberately holds a licence that is NOT in the window. An
    empty registry would be a DIFFERENT condition -- `parse_licence_owners`
    refuses it -- and stubbing one here would quietly test that instead.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    observability_metrics.license_tenant_mismatch_events.set(999)
    session = _StubSession(window=[], owners=[], budgets=[])

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert audit.total_events == 0
    assert audit.mismatch_ratio is None
    assert observability_metrics.license_tenant_mismatch_events._value.get() == 0


def test_a_missing_owner_never_compares_equal() -> None:
    assert aa._same_tenant(None, "t") is False
    assert aa._same_tenant("t", None) is False


# ---------------------------------------------------------------------------
# The freshness timestamp — the series panels and alerts actually select on.
#
# Prometheus scrapes every replica, but only the rollup leader writes the six
# coherence gauges. A non-leader does NOT export a default 0: prometheus_client
# gauges are process-lifetime state, so a replica that lost leadership keeps
# exporting the numbers it last wrote, forever. Measured on prod 2026-08-26 --
# leader 4141, follower 4797 frozen 12h earlier -- which is why the dashboard's
# max() plotted the STALE value. These tests pin the discriminator that replaced
# it: a timestamp written LAST and only on a complete pass.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_freshness_timestamp_advances_on_a_complete_pass(monkeypatch) -> None:
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    observability_metrics.audit_last_success_timestamp_seconds.set(0)
    session = _StubSession(window=[], owners=[], budgets=[])

    before = time.time()
    await aa.run_attribution_audit(_factory(session), window_hours=24)
    after = time.time()

    published = observability_metrics.audit_last_success_timestamp_seconds._value.get()
    # Bracketed by real wall-clock, so a hardcoded constant or a frozen value
    # cannot pass this.
    assert before <= published <= after


@pytest.mark.asyncio
async def test_the_pass_publishes_per_emitter_coverage(monkeypatch) -> None:
    """The audit pass reads `_EMITTER_COVERAGE_SQL` and writes both gauges."""
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    session = _StubSession(
        window=[],
        owners=[],
        budgets=[],
        emitter_coverage=[
            SimpleNamespace(
                service_key=EMITTER_X,
                events=10,
                project_events=3,
                actor_events=7,
            )
        ],
    )
    try:
        await aa.run_attribution_audit(_factory(session), window_hours=24)

        assert id(aa._EMITTER_COVERAGE_SQL) in session.seen
        events = observability_metrics.attribution_coverage_events
        covered = observability_metrics.attribution_covered_events
        assert events.labels(service_key=EMITTER_X, axis="project")._value.get() == 10
        assert events.labels(service_key=EMITTER_X, axis="actor")._value.get() == 10
        assert covered.labels(service_key=EMITTER_X, axis="project")._value.get() == 3
        assert covered.labels(service_key=EMITTER_X, axis="actor")._value.get() == 7
    finally:
        aa.publish_emitter_coverage([])


@pytest.mark.asyncio
async def test_a_partial_pass_leaves_the_timestamp_stale(monkeypatch) -> None:
    """THE DEFECT THIS ORDERING EXISTS FOR.

    If the timestamp were published first -- or in a ``finally`` -- a pass that
    blew up halfway would certify a set of gauges in which some are from this
    pass and some are from the last one. The panel would then believe a replica
    whose numbers do not agree with each other. So: a run that raises after some
    gauges are already written must leave the timestamp exactly where it was.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    sentinel = 1234.5
    observability_metrics.audit_last_success_timestamp_seconds.set(sentinel)

    boom = RuntimeError("gauge backend died mid-publish")
    monkeypatch.setattr(
        observability_metrics.budgets_over_limit,
        "set",
        lambda *_a, **_k: (_ for _ in ()).throw(boom),
    )

    session = _StubSession(window=[], owners=[], budgets=[])
    with pytest.raises(RuntimeError):
        await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert observability_metrics.audit_last_success_timestamp_seconds._value.get() == sentinel


@pytest.mark.asyncio
async def test_the_timestamp_is_written_after_every_coherence_gauge(monkeypatch) -> None:
    """Order, asserted directly rather than inferred from the failure case.

    A mutant that moves the timestamp above the six ``.set()`` calls still
    passes both tests above when nothing raises. This one does not: it records
    the actual write order and requires the timestamp to be last.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    order: list[str] = []
    names = [
        "license_tenant_mismatch_events",
        "license_tenant_mismatch_spend_usd",
        "license_tenant_unresolved_events",
        "license_attributed_events",
        "license_unattributed_events",
        "budgets_over_limit",
        "budget_max_utilisation_ratio",
        "audit_last_success_timestamp_seconds",
    ]
    # NOT in `names`: attribution_rate. This drives an EMPTY window, where the
    # rate is deliberately never published at all, so wrapping it would only
    # assert that it stays absent — which
    # tests/test_attribution_unattributed_measurement.py does directly.
    originals = {}
    try:
        for name in names:
            gauge = getattr(observability_metrics, name)
            originals[name] = gauge.set

            def _record(value, _n=name, _orig=gauge.set):
                order.append(_n)
                return _orig(value)

            gauge.set = _record  # type: ignore[method-assign]

        session = _StubSession(window=[], owners=[], budgets=[])
        await aa.run_attribution_audit(_factory(session), window_hours=24)
    finally:
        for name, orig in originals.items():
            getattr(observability_metrics, name).set = orig  # type: ignore[method-assign]

    # Vacuity floor: if the harness silently failed to wrap, `order` would be
    # empty and a bare "timestamp is last" check would pass on nothing.
    assert len(order) == 8, order
    assert order[-1] == "audit_last_success_timestamp_seconds"
    assert set(order[:-1]) == set(names[:-1])


# ---------------------------------------------------------------------------
# Spend booked to an identity billing has no record of.
#
# A DIFFERENT defect from mismatch/unattributed: those adjudicate a licence,
# this asks whether the tenant exists here at all. Measured on prod 2026-08-28,
# 26 of 61 distinct tenant_ids in billing_cost_events have no row in
# licenses.tenants — including a literal `0195f48b-dead-7000-8000-00000000beef`.
# ---------------------------------------------------------------------------

DEAD_BEEF = "0195f48b-dead-7000-8000-00000000beef"


@pytest.mark.asyncio
async def test_unregistered_tenant_spend_is_counted_named_and_gauged(monkeypatch) -> None:
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    session = _StubSession(
        window=[],
        owners=[],
        budgets=[],
        unregistered=[
            SimpleNamespace(tenant_id=DEAD_BEEF, events=3, spend=1.75),
            SimpleNamespace(
                tenant_id="ad976f5b-f3a7-4441-ae87-c2090d038f9c", events=80, spend=0.25
            ),
        ],
    )

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert audit.unregistered_tenant_events == 83
    assert audit.unregistered_tenant_spend_usd == pytest.approx(2.0)
    # NAMED, not just counted. A bare count cannot be acted on, and the whole
    # point of this gauge is that `...-dead-...-beef` reached a financial table.
    assert (DEAD_BEEF, 3) in audit.top_unregistered_tenants

    assert observability_metrics.license_tenant_unregistered_events._value.get() == 83
    assert (
        observability_metrics.license_tenant_unregistered_spend_usd._value.get()
        == pytest.approx(2.0)
    )


@pytest.mark.asyncio
async def test_unregistered_tenants_do_not_contaminate_the_licence_adjudication(
    monkeypatch,
) -> None:
    """The new population must not be folded into the coherence denominators.

    This is the assertion that dies if someone routes the unregistered rows
    through `classify_attribution`: an unregistered tenant makes no ownership
    CLAIM, so counting it as matched/mismatched/unattributed would make each of
    those defects look smaller whenever this one grew.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    session = _StubSession(
        window=[],
        owners=[],
        budgets=[],
        unregistered=[SimpleNamespace(tenant_id=DEAD_BEEF, events=999, spend=99.0)],
    )

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert audit.unregistered_tenant_events == 999
    assert audit.total_events == 0
    assert audit.matched_events == 0
    assert audit.mismatch_events == 0
    assert audit.unattributed_events == 0
    assert audit.billable_events == 0
    # No denominator, so no rate — never a fabricated 0.0.
    assert audit.attribution_rate is None


@pytest.mark.asyncio
async def test_an_empty_unregistered_population_clears_the_gauge(monkeypatch) -> None:
    """A fixed emitter must be able to turn the panel green again.

    Both series are GAUGES for this reason. Were either a counter, a repaired
    gap would keep reading red forever and the fix would be indistinguishable
    from no fix at all.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    observability_metrics.license_tenant_unregistered_events.set(999)
    observability_metrics.license_tenant_unregistered_spend_usd.set(99.0)

    session = _StubSession(window=[], owners=[], budgets=[], unregistered=[])
    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert audit.unregistered_tenant_events == 0
    assert observability_metrics.license_tenant_unregistered_events._value.get() == 0
    assert observability_metrics.license_tenant_unregistered_spend_usd._value.get() == 0


@pytest.mark.asyncio
async def test_a_null_anchor_budget_is_not_reported_stale(monkeypatch) -> None:
    """The audit must decide staleness with services.budget_period, not its own rule.

    It used to compute `consumed_period_start != anchor`. That differs from
    `is_stale_period`'s `<` in two ways, and BOTH make an over-limit budget
    disappear from the audit -- `stale=True` collapses its ratio to 0.0 and
    drops it out of `over_limit`, so the one signal that says "this tenant is
    burning past its cap" reads clean:

      * a NULL anchor (a row predating the column) compared unequal to the
        current anchor and was called stale, where the accessor falls back to
        the period of `starts_at` -- the same rule the backfill migration used;
      * a FUTURE anchor also compared unequal and was called stale, where `<`
        reads through.

    Measured on prod before changing this: 0 of 385 rows carry a NULL anchor,
    so this is preventive, not a live under-report. It is still the accessor's
    job to answer the question.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    now = datetime.now(UTC)
    session = _StubSession(
        window=[],
        owners=[],
        budgets=[
            # Pre-column row: NULL anchor, started THIS period -> not stale.
            SimpleNamespace(
                id=uuid.uuid4(),
                tenant_id="fake-tenant",
                license_id=NULL_LICENSE_ID,
                budget_tokens=100,
                consumed_tokens=250,
                period="monthly",
                consumed_period_start=None,
                starts_at=now,
            ),
            # Anchor ahead of the current period -> `<` reads through, `!=` did not.
            SimpleNamespace(
                id=uuid.uuid4(),
                tenant_id="fake-tenant",
                license_id=NULL_LICENSE_ID,
                budget_tokens=100,
                consumed_tokens=250,
                period="monthly",
                consumed_period_start=(now.date().replace(day=1) + timedelta(days=40)).replace(
                    day=1
                ),
                starts_at=now,
            ),
        ],
    )

    await aa.run_attribution_audit(_factory(session), window_hours=24)

    # Both are 2.5x over a 100-token cap. Under the old `!=` rule both were
    # marked stale and this gauge read 0.
    assert observability_metrics.budgets_over_limit._value.get() == 2


# ---------------------------------------------------------------------------
# The licence (DEBIT) grain, end to end.
#
# TestLicenceGrainIsNotTheEnforcedGrain proves the summariser. These two prove
# the WIRING, which is a separate failure: the field carries a default (the pool
# sentinel), so a _load that forgot to read the column would still construct a
# valid object -- and every licence would silently collapse into the pool, making
# the licence gauge a duplicate of the tenant gauge. That is a wrong number, not
# a crash, so it needs its own assertion.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_licence_gauges_are_published_and_differ_from_the_tenant_gauges(
    monkeypatch,
) -> None:
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})
    now = datetime.now(UTC)
    anchor = now.date().replace(day=1)
    lic_a, lic_b = uuid.uuid4(), uuid.uuid4()

    def _budget(license_id, cap, consumed):
        return SimpleNamespace(
            id=uuid.uuid4(),
            tenant_id="fake-tenant",
            license_id=license_id,
            budget_tokens=cap,
            consumed_tokens=consumed,
            period="monthly",
            consumed_period_start=anchor,
        )

    session = _StubSession(
        window=[],
        owners=[],
        # The 2026-09-11 shape: one licence oversubscribed, tenant far under cap.
        budgets=[
            _budget(lic_a, 10_000_000, 20_500_000),
            _budget(lic_b, 100_000_000, 0),
            _budget(NULL_LICENSE_ID, 1_000_000, 0),
        ],
    )

    await aa.run_attribution_audit(_factory(session), window_hours=24)

    licences_over = observability_metrics.budget_licences_over_limit._value.get()
    licence_ratio = observability_metrics.budget_licence_max_utilisation_ratio._value.get()
    tenants_over = observability_metrics.budget_tenants_over_limit._value.get()
    tenant_ratio = observability_metrics.budget_tenant_max_utilisation_ratio._value.get()

    # The debit grain: one licence scope over its drainable cap.
    assert licences_over == 1
    assert licence_ratio == pytest.approx(20_500_000 / 11_000_000)
    # The ENFORCED grain, published from the same pass: nothing over cap.
    assert tenants_over == 0
    assert tenant_ratio == pytest.approx(20_500_000 / 111_000_000)
    # ⛔ THE MUTANT THIS EXISTS FOR: `license_id=NULL_LICENSE_ID` hardcoded in
    # _load (or the argument dropped so the default applies) collapses every row
    # into the pool and makes these two equal.
    assert licence_ratio != pytest.approx(tenant_ratio)
    assert licence_ratio > 1.0 > tenant_ratio


def test_the_budgets_query_actually_selects_the_licence_column() -> None:
    """⚠️ A STUB CANNOT TEST THE SELECT LIST.

    Every test above feeds `_BUDGETS_SQL` through `_StubSession`, which answers by
    statement identity and never parses the text. Drop `license_id` from the SELECT
    and all of them stay green while the live path raises AttributeError on
    `r.license_id` on the leader's first tick. This is the cheap textual guard for
    that; the column's SEMANTICS are pinned by the tests above.
    """
    assert "license_id" in str(aa._BUDGETS_SQL)


# ===========================================================================
# THE DEFECT: THE AUDIT USED TO VALIDATE AGAINST DATA DERIVED FROM ITS SUBJECT.
#
# `_OWNERS_SQL` read `request_quota_config`, whose `tenant_id` is caller-asserted
# (routers/quota.py `set_quota_config` -> role check only -> body's tenant_id ->
# services/quota.py `on_conflict_do_update`, which sets tenant_id
# UNCONDITIONALLY) and which is refreshed from the same event stream the audit
# audits. Measured 2026-09-11 on one 24h window:
#
#     request_quota_config   ->   0 mismatches,   $0.0000
#     licences `license`     -> 188 mismatches,   $0.1242 of $320.79, 4 licences
#
# and one license_id sat under TWO tenants in request_quota_config while the
# registry maps it to one. A self-derived control cannot refute the thing that
# derived it, so `BillingMisattributedSpendIncreased` was understated by
# CONSTRUCTION rather than by threshold.
# ===========================================================================


@pytest.mark.asyncio
async def test_a_mismatch_the_local_table_would_hide_is_now_detected(monkeypatch) -> None:
    """⭐ THE REGRESSION TEST FOR THE WHOLE CHANGE.

    The exact shape the local table cannot see: the registry says tenant A owns
    the licence, the event is billed to tenant B, and `request_quota_config` --
    refreshed from that same event -- agrees with B. Under the old source this
    classified as MATCHED and published 0; the event's own assertion was its own
    audit.

    ⛔ MUTATION CONTROL: this test is worthless unless it can still fail, and the
    mutant it must catch is precisely the pre-fix code. So it asserts the local
    table's own verdict too -- `local_owner_disagreements == 1` proves the stub
    really does encode the contradiction, i.e. that feeding `_OWNERS_SQL` back into
    `classify_attribution` WOULD have produced matched=5 / mismatch=0 here. Without
    that second assertion a stub that had accidentally agreed with the registry
    would pass while proving nothing.
    """
    licence = _lic()
    owner_a, billed_b = str(uuid.uuid4()), str(uuid.uuid4())

    _registry_says(monkeypatch, {licence: owner_a})
    session = _StubSession(
        window=[
            SimpleNamespace(
                license_id=licence, tenant_id=billed_b, events=188, spend=0.1242, delegated=False
            )
        ],
        # The caller-asserted local row, agreeing with the EVENT rather than the
        # registry. This is the live prod shape, not a contrivance.
        owners=[(licence, billed_b)],
        budgets=[],
    )

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert audit.owner_source == aa.OWNER_SOURCE_REGISTRY
    assert audit.mismatch_events == 188
    assert audit.mismatch_spend_usd == pytest.approx(0.1242)
    assert audit.matched_events == 0
    # NAMED: billed tenant and owning tenant both on the pair, so the finding is
    # actionable rather than a count.
    assert (licence, billed_b, owner_a, 188) in audit.top_pairs
    assert observability_metrics.license_tenant_mismatch_events._value.get() == 188
    assert observability_metrics.license_tenant_mismatch_spend_usd._value.get() == pytest.approx(
        0.1242
    )

    # The control described above: the local proxy contradicts the registry on
    # exactly this licence, which is what made the old source report 0.
    assert audit.local_owner_disagreements == 1
    assert audit.local_owner_only_licences == 0


@pytest.mark.asyncio
async def test_router_delegated_spend_reaches_the_delegated_gauges_not_mismatch(
    monkeypatch,
) -> None:
    """A router-vouched delegated group publishes on the delegated pair ONLY.

    The undelegated group beside it -- same licence, same billed tenant -- must
    still reach the mismatch gauge exactly as before, so the alert keeps firing
    on real misattribution while ignoring intended delegation.
    """
    licence = _lic()
    owner, end_tenant = str(uuid.uuid4()), str(uuid.uuid4())
    _registry_says(monkeypatch, {licence: owner})
    session = _StubSession(
        window=[
            SimpleNamespace(
                license_id=licence, tenant_id=end_tenant, events=40, spend=1.25, delegated=True
            ),
            SimpleNamespace(
                license_id=licence, tenant_id=end_tenant, events=3, spend=0.5, delegated=False
            ),
        ],
        owners=[],
        budgets=[],
    )

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert audit.mismatch_events == 3
    assert audit.delegated_events == 40
    assert observability_metrics.license_tenant_mismatch_events._value.get() == 3
    assert observability_metrics.license_tenant_mismatch_spend_usd._value.get() == pytest.approx(
        0.5
    )
    assert observability_metrics.license_tenant_delegated_events._value.get() == 40
    assert observability_metrics.license_tenant_delegated_spend_usd._value.get() == pytest.approx(
        1.25
    )
    assert audit.top_pairs == [(licence, end_tenant, owner, 3)]
    assert audit.top_delegated_pairs == [(licence, end_tenant, owner, 40)]


@pytest.mark.asyncio
async def test_one_licence_under_two_local_tenants_is_measured_not_believed(monkeypatch) -> None:
    """The measured pathology: `request_quota_config` held one licence twice.

    It is keyed on license_id, so two rows for one licence is the mirror having
    been written twice with different asserted tenants. Neither row can be the
    owner; the registry's single answer is.

    ⛔ MUTATION CONTROL: flip the two local rows so BOTH agree with the registry
    and `local_owner_disagreements` must fall to 0 -- the assertion is on the
    comparison, not on the row count, so a `compare_local_owner_proxy` that merely
    counted local rows would fail it.
    """
    licence = _lic()
    owner, other = str(uuid.uuid4()), str(uuid.uuid4())

    _registry_says(monkeypatch, {licence: owner})
    session = _StubSession(
        window=[],
        # A dict build keeps the LAST row, so the local proxy's answer depends on
        # row order -- which is itself the point: it has no single answer.
        owners=[(licence, owner), (licence, other)],
        budgets=[],
    )

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert audit.local_owner_disagreements == 1


@pytest.mark.asyncio
async def test_licences_unreachable_reports_unknown_and_never_zero_mismatches(
    monkeypatch,
) -> None:
    """⛔ THE FAILURE MODE BEING REMOVED: a 0 that means "I could not check".

    Four things must hold at once, and each is a separate way the old defect
    could come back:

      1. `owner_source` says UNAVAILABLE, so the reading is self-describing;
      2. the four ownership gauges are NOT WRITTEN AT ALL -- asserted on the
         `.set()` CALL, not on the value, because a mutant that re-set the
         previous value would pass a value check;
      3. the freshness timestamp is withheld, so nothing believes the pass;
      4. the DB-only arms ARE still published, because they were genuinely
         measured -- abstention must be scoped to what could not be checked.

    ⛔ MUTATION CONTROL: delete the `if audit.coherence_measured:` guard in front
    of the four gauges and (2) fails with `calls == [0, 0.0, 0, 0]` -- exactly the
    fabricated zero. Delete the guard on the timestamp and (3) fails.
    """
    sentinel_mismatch, sentinel_ts = 4321, 1111.5
    observability_metrics.license_tenant_mismatch_events.set(sentinel_mismatch)
    observability_metrics.audit_last_success_timestamp_seconds.set(sentinel_ts)
    observability_metrics.license_tenant_unregistered_events.set(0)

    attempts = _registry_unavailable(monkeypatch, "licences returned HTTP 503")

    gauges = [
        "license_tenant_mismatch_events",
        "license_tenant_mismatch_spend_usd",
        "license_tenant_unresolved_events",
        "license_attributed_events",
        "audit_last_success_timestamp_seconds",
    ]
    calls: dict[str, list] = {name: [] for name in gauges}
    originals = {}
    try:
        for name in gauges:
            gauge = getattr(observability_metrics, name)
            originals[name] = gauge.set

            def _record(value, _n=name, _orig=gauge.set):
                calls[_n].append(value)
                return _orig(value)

            gauge.set = _record  # type: ignore[method-assign]

        session = _StubSession(
            window=[
                SimpleNamespace(
                    license_id=_lic(),
                    tenant_id=str(uuid.uuid4()),
                    events=188,
                    spend=0.1242,
                    delegated=False,
                )
            ],
            owners=[],
            budgets=[],
            unattributed=[SimpleNamespace(events=12, spend=3.0)],
            unregistered=[SimpleNamespace(tenant_id=DEAD_BEEF, events=7, spend=1.25)],
        )
        audit = await aa.run_attribution_audit(_factory(session), window_hours=24)
    finally:
        for name, orig in originals.items():
            getattr(observability_metrics, name).set = orig  # type: ignore[method-assign]

    assert len(attempts) == 1

    # (1) self-describing
    assert audit.owner_source == aa.OWNER_SOURCE_UNAVAILABLE
    assert audit.coherence_measured is False
    # And no fabricated comparison result either: None, never 0.
    assert audit.local_owner_disagreements is None
    assert audit.local_owner_only_licences is None

    # (2) not written at all — the load-bearing assertion
    for name in gauges[:4]:
        assert calls[name] == [], f"{name} was published on an unchecked window: {calls[name]}"
    assert observability_metrics.license_tenant_mismatch_events._value.get() == sentinel_mismatch

    # (3) the pass is uncertified
    assert calls["audit_last_success_timestamp_seconds"] == []
    assert observability_metrics.audit_last_success_timestamp_seconds._value.get() == sentinel_ts

    # (4) what WAS measured is still published and still correct. `total_events`
    # comes from _WINDOW_SQL and owes the registry nothing, so the attribution
    # RATE survives a licences outage -- only the ownership verdicts abstain.
    assert audit.total_events == 188
    assert audit.unattributed_events == 12
    assert audit.billable_events == 200
    assert audit.attribution_rate == pytest.approx(188 / 200)
    assert audit.unregistered_tenant_events == 7
    assert observability_metrics.license_tenant_unregistered_events._value.get() == 7


@pytest.mark.asyncio
async def test_an_unchecked_window_is_not_mistaken_for_a_clean_one(monkeypatch) -> None:
    """The two readings a reader must be able to tell apart.

    Same window, same rows. Once with a registry that resolves everything to the
    billed tenant (genuinely clean, 0 mismatches, PUBLISHED) and once with the
    registry down (unknown, 0 mismatches NOT published). If `owner_source` were
    dropped these two passes would be indistinguishable on the dataclass -- which
    is the defect, restated as a test.
    """
    licence, tenant = _lic(), str(uuid.uuid4())
    row = SimpleNamespace(
        license_id=licence, tenant_id=tenant, events=9, spend=2.0, delegated=False
    )

    _registry_says(monkeypatch, {licence: tenant})
    clean = await aa.run_attribution_audit(
        _factory(_StubSession(window=[row], owners=[], budgets=[])), window_hours=24
    )

    _registry_unavailable(monkeypatch)
    unknown = await aa.run_attribution_audit(
        _factory(_StubSession(window=[row], owners=[], budgets=[])), window_hours=24
    )

    # The numbers agree...
    assert clean.mismatch_events == unknown.mismatch_events == 0
    assert clean.total_events == unknown.total_events == 9
    # ...and the readings do not.
    assert clean.coherence_measured is True
    assert unknown.coherence_measured is False
    assert clean.matched_events == 9
    assert unknown.matched_events == 0


# ---------------------------------------------------------------------------
# `parse_licence_owners` — pure, so every refusal is testable without a socket.
#
# ⚠️ These matter more than they look. Each refused shape, if HONOURED, yields an
# owners map that resolves nothing and therefore `mismatch_events == 0` — i.e.
# each one is a separate route back to the fabricated zero, arriving through the
# new code path instead of the old SQL.
# ---------------------------------------------------------------------------


class TestParseLicenceOwners:
    def test_the_registry_tenant_id_becomes_the_owner(self) -> None:
        """What the licences response actually carries.

        licences `main.py` `InternalLicenseResponse` declares `tenant_id: str` as a
        REQUIRED field, built as `str(lic.tenant_id)` from the registry's own uuid
        column — so the endpoint `fetch_licenses` already calls does carry
        licence -> owning-tenant. It is read here.
        """
        lic, tenant = _lic(), str(uuid.uuid4())
        owners, unowned = aa.parse_licence_owners(
            [
                {
                    "id": lic,
                    "tenant_id": tenant,
                    "subject_id": str(uuid.uuid4()),
                    "status": "active",
                    "starts_at": "2026-01-01T00:00:00+00:00",
                    "expires_at": None,
                    "metadata": None,
                    "app": {"key": "workers"},
                    "plan": {"code": "pro"},
                }
            ]
        )
        assert owners == {lic: tenant}
        assert unowned == 0

    def test_a_revoked_licence_still_resolves_its_owner(self) -> None:
        """⛔ STATUS IS NOT FILTERED, AND MUST NOT BE.

        `services.billing.fetch_licenses` sends `status=active`. A revoked licence
        that is still carrying spend is exactly the case this audit exists to
        surface; dropping it would turn a MISMATCH into an `unresolved` and lower
        the published number.
        """
        lic, tenant = _lic(), str(uuid.uuid4())
        owners, _ = aa.parse_licence_owners([{"id": lic, "tenant_id": tenant, "status": "revoked"}])
        assert owners == {lic: tenant}

    def test_an_empty_registry_is_refused_not_honoured(self) -> None:
        """A 200 carrying `[]` would resolve nothing and publish 0 mismatches.

        ⛔ MUTATION CONTROL: remove the `if not payload` guard and this raises
        nothing while returning `({}, 0)` — which `classify_attribution` turns into
        unresolved=total, mismatch=**0**. That is the fabricated zero, reached
        through the new path. An estate with zero licences is a state this system
        has never been in; the same refusal is in
        `licence_budget_reconcile._fetch_licence_lifecycle` for the same reason.
        """
        with pytest.raises(aa.LicenceOwnersUnavailableError):
            aa.parse_licence_owners([])

    def test_an_enveloped_payload_is_refused(self) -> None:
        """`for lic in {...}` iterates KEYS, so a paginated envelope would parse to
        zero licences and read as a healthy-but-empty registry."""
        with pytest.raises(aa.LicenceOwnersUnavailableError):
            aa.parse_licence_owners({"items": [{"id": _lic(), "tenant_id": "t"}]})

    def test_a_licence_with_no_id_refuses_the_whole_tick(self) -> None:
        """It cannot be keyed, so it can never match an event's licence — and it
        might be the very licence carrying the mismatch. Omitting it would publish
        a LOWER mismatch count, which reads as an improvement."""
        with pytest.raises(aa.LicenceOwnersUnavailableError):
            aa.parse_licence_owners([{"tenant_id": str(uuid.uuid4()), "status": "active"}])

    def test_a_tenantless_licence_is_counted_and_excluded_not_raised_on(self) -> None:
        """One bad upstream row must not blind the whole estate's coherence read.

        Same blast-radius rule `routers/budget.py` applies to this payload: skip
        and count the row, keep the rest. Excluded from the map, so that licence's
        spend reads `unresolved` — never `matched`.
        """
        good, bad = _lic(), _lic()
        tenant = str(uuid.uuid4())
        owners, unowned = aa.parse_licence_owners(
            [
                {"id": good, "tenant_id": tenant, "status": "active"},
                {"id": bad, "tenant_id": None, "status": "active"},
            ]
        )
        assert owners == {good: tenant}
        assert unowned == 1

    def test_the_string_none_is_not_an_owning_tenant(self) -> None:
        """⚠️ THE SUBTLE ONE. Upstream builds this field as `str(lic.tenant_id)`, so
        a NULL column arrives as the four-character string `"None"` — truthy. Taken
        as an owner it matches no event, so every one of that licence's events
        becomes a MISMATCH. A fabricated mismatch is as wrong as a fabricated match.

        ⛔ MUTATION CONTROL: drop the `{"none", "null"}` check and `unowned` falls
        to 0 while `owners` gains a licence owned by `"None"`.
        """
        lic = _lic()
        owners, unowned = aa.parse_licence_owners([{"id": lic, "tenant_id": "None"}])
        assert owners == {}
        assert unowned == 1


class TestCompareLocalOwnerProxy:
    """`request_quota_config`, measured instead of believed."""

    def test_agreement_disagreement_and_local_only_are_separate_counts(self) -> None:
        """Three licences, three relationships, three distinct outcomes.

        ⛔ MUTATION CONTROL: return `(local_only, disagreements)` swapped and this
        fails, because the two numbers are deliberately different (1 vs 1 would
        hide it, so they are 1 and 1 on DIFFERENT licences — asserted as a pair
        with an agreeing third licence that must count toward neither).
        """
        agree, disagree, local_only = _lic(), _lic(), _lic()
        t_a, t_b = str(uuid.uuid4()), str(uuid.uuid4())
        registry = {agree: t_a, disagree: t_a}
        local = {agree: t_a, disagree: t_b, local_only: t_a}

        assert aa.compare_local_owner_proxy(registry, local) == (1, 1)
        # The agreeing licence counted toward neither — the vacuity floor.
        assert aa.compare_local_owner_proxy({agree: t_a}, {agree: t_a}) == (0, 0)

    def test_a_uuid_and_its_string_form_are_not_a_disagreement(self) -> None:
        """tenant_id is mid-migration from str to uuid across this repo; a raw
        comparison would report the whole estate as disagreeing on a cast."""
        lic = _lic()
        tenant = uuid.uuid4()
        assert aa.compare_local_owner_proxy({lic: str(tenant)}, {lic: tenant}) == (0, 0)


@pytest.mark.asyncio
async def test_the_owners_sql_is_never_the_ownership_source_again(monkeypatch) -> None:
    """⛔ THE ANTI-REGRESSION GUARD FOR THE DEFECT ITSELF.

    `request_quota_config` is still READ (for `compare_local_owner_proxy`), so its
    statement still being issued proves nothing. This pins the thing that matters:
    with the registry UNREACHABLE and the local table holding a perfectly usable
    map, the audit must still refuse to adjudicate. A fallback — however
    well-intentioned — fails here, which is the point: the fallback cannot be
    distinguished from a real measurement on any published series, because
    `observability/metrics.py` carries no gauge for the source.
    """
    licence, tenant = _lic(), str(uuid.uuid4())
    _registry_unavailable(monkeypatch)

    session = _StubSession(
        window=[
            SimpleNamespace(
                license_id=licence, tenant_id=tenant, events=9, spend=2.0, delegated=False
            )
        ],
        # A complete, usable local map. It must NOT be used.
        owners=[(licence, tenant)],
        budgets=[],
    )

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert id(aa._OWNERS_SQL) in session.seen, "the local proxy is still read, as the SUBJECT"
    assert audit.coherence_measured is False
    assert audit.matched_events == 0, "the local table was used as a fallback"


@pytest.mark.asyncio
async def test_the_unconfigured_licences_url_is_unavailable_not_an_empty_map(
    test_settings,
) -> None:
    """`Settings.licenses_url` defaults to `""`.

    Without the guard this attempts `GET "/internal/licenses"` and surfaces as an
    obscure protocol error rather than "you did not configure licences" — and on
    any transport shape that returned a body it would be an empty map, i.e. 0
    mismatches.
    """
    with pytest.raises(aa.LicenceOwnersUnavailableError, match="not configured"):
        await _REAL_FETCH_LICENCE_OWNERS(test_settings.model_copy(update={"licenses_url": ""}))


@pytest.mark.asyncio
async def test_a_non_200_is_unavailable_because_httpx_does_not_raise_on_status(
    test_settings,
) -> None:
    """⚠️ A STATUS CODE IS NOT AN `httpx` EXCEPTION.

    `fetch_licenses` shipped without a status check and fell through to
    `return []`, making an expired S2S token indistinguishable from a
    licence-free tenant. A 401/403 here is the single most likely failure
    (`/internal/` needs the service-account bearer), so it is checked explicitly.

    ⛔ MUTATION CONTROL: delete the `resp.status_code != 200` check and this test
    fails with `({}, 0)` returned from a 403 — zero mismatches from an auth error.
    """
    import httpx

    class _Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):  # noqa: ANN001, ANN201
            return httpx.Response(403, json={"error": "forbidden"})

    settings = test_settings.model_copy(update={"licenses_url": "http://licenses.example.com"})
    async with httpx.AsyncClient(transport=_Transport()) as client:
        with pytest.raises(aa.LicenceOwnersUnavailableError, match="403"):
            await _REAL_FETCH_LICENCE_OWNERS(settings, client=client)


@pytest.mark.asyncio
async def test_a_transport_failure_is_unavailable_not_an_empty_map(test_settings) -> None:
    """A licences outage must not resolve to "nobody owns anything".

    ⛔ MUTATION CONTROL: swallow the `httpx.HTTPError` and return `({}, 0)` instead
    of raising, and the audit publishes 0 mismatches for the whole outage.
    """
    import httpx

    class _Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):  # noqa: ANN001, ANN201
            raise httpx.ConnectError("connection refused")

    settings = test_settings.model_copy(update={"licenses_url": "http://licenses.example.com"})
    async with httpx.AsyncClient(transport=_Transport()) as client:
        with pytest.raises(aa.LicenceOwnersUnavailableError, match="unreachable"):
            await _REAL_FETCH_LICENCE_OWNERS(settings, client=client)


@pytest.mark.asyncio
async def test_the_unscoped_call_sends_no_tenant_header_and_no_status_filter(
    test_settings,
) -> None:
    """⛔ BOTH OMISSIONS ARE LOAD-BEARING AND BOTH HAVE BITTEN THIS ESTATE.

    * `X-Tenant-Id`: licences' `AuthMiddleware.dispatch` runs
      `validate_tenant_uuid(..., source="X-Tenant-Id")` BEFORE it reads the
      Authorization header. `routers/budget.py` shipped with `"__admin__"` there
      and every call it ever made was a 400 folded into a 200 response body. There
      is no tenant to name on a fleet-wide call.
    * `status` / `tenant_id` query params: with either one the response is a
      SUBSET, and a subset silently turns cross-tenant mismatches into
      `unresolved` — a lower published number that reads as an improvement. This is
      why `services.billing.fetch_licenses` (which sends both) cannot be reused
      here despite hitting the same endpoint.

    ⛔ MUTATION CONTROL: add `params={"status": "active"}` or switch to
    `ServiceTokenProvider.build_headers` (which always sets `X-Tenant-Id`) and
    this fails.
    """
    import httpx

    seen: dict = {}

    class _Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):  # noqa: ANN001, ANN201
            seen["url"] = str(request.url)
            seen["headers"] = dict(request.headers)
            return httpx.Response(200, json=[{"id": _lic(), "tenant_id": str(uuid.uuid4())}])

    # A TRAILING SLASH on the base, deliberately: `f"{base}/internal/licenses"`
    # without the `rstrip` would request `//internal/licenses`, which licences
    # answers with a 404 — and a 404 is unavailability, so the audit would abstain
    # forever on a cosmetic config difference.
    settings = test_settings.model_copy(update={"licenses_url": "http://licenses.example.com/"})
    async with httpx.AsyncClient(transport=_Transport()) as client:
        owners, _ = await _REAL_FETCH_LICENCE_OWNERS(settings, client=client)

    assert owners
    assert seen["url"] == "http://licenses.example.com/internal/licenses"
    assert "?" not in seen["url"], f"the call was scoped: {seen['url']}"
    assert "x-tenant-id" not in {k.lower() for k in seen["headers"]}


# ---------------------------------------------------------------------------
# THE PROJECT-AXIS AUDIT PAIR: WIRING ONLY.
#
# What a stub session can grade here is which NUMBER reaches which GAUGE — and
# that is worth grading, because five new numbers arrive from two statements and
# a transposition between any two of them is completely silent.
#
# ⛔ WHAT IT CANNOT GRADE, AND WHERE THAT LIVES. Both additions ARE SQL:
# `project_id = tenant_id` is a uuid comparison, `IS DISTINCT FROM` is
# three-valued logic, `FILTER` is an aggregate filter, and the deliberate
# absence (Addition 1) / presence (Addition 2) of `excluded_from_billing` are
# WHERE clauses. Asserting any of those against this stub would assert only that
# the stub was written correctly. They are pinned against real Postgres in
# tests/test_attribution_project_spend_audit.py — including the property that
# matters most, that the three coverage buckets PARTITION the window.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_forged_project_numbers_each_reach_their_own_gauge(monkeypatch) -> None:
    """Three DISTINCT values, so a gauge fed from the wrong one fails.

    Written as 7 / 62 / 6.0458 rather than three round numbers on purpose: a
    mutant that publishes the row count as the spend, or the distinct-project
    count as the rows, passes any assertion where two of the three agree. The
    row count and the dollars are the shape measured on prod 2026-09-16 (62 rows
    carrying $6.0458), which is the reading these series exist to make sayable.
    """
    lic, tenant = _lic(), str(uuid.uuid4())
    _registry_says(monkeypatch, {lic: tenant})

    session = _StubSession(
        window=[
            SimpleNamespace(
                license_id=lic, tenant_id=tenant, events=414, spend=9.7622, delegated=False
            )
        ],
        owners=[(lic, tenant)],
        budgets=[],
        self_tenant=[SimpleNamespace(projects=7, events=62, spend=6.0458)],
    )

    await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert observability_metrics.project_id_equals_tenant_projects._value.get() == 7
    assert observability_metrics.project_id_equals_tenant_rows._value.get() == 62
    assert observability_metrics.project_id_equals_tenant_spend_usd._value.get() == pytest.approx(
        6.0458
    )


@pytest.mark.asyncio
async def test_a_clean_forged_project_window_publishes_a_measured_zero(monkeypatch) -> None:
    """A zero is WRITTEN, never withheld — and the previous test proves it moved.

    These two run in order against module-global gauges, so the zero here is only
    meaningful because the test above left 7 / 62 / 6.0458 behind. A gauge that
    stopped being written would keep those values and fail this.
    """
    lic, tenant = _lic(), str(uuid.uuid4())
    _registry_says(monkeypatch, {lic: tenant})

    session = _StubSession(
        window=[
            SimpleNamespace(license_id=lic, tenant_id=tenant, events=10, spend=1.0, delegated=False)
        ],
        owners=[(lic, tenant)],
        budgets=[],
    )

    await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert observability_metrics.project_id_equals_tenant_projects._value.get() == 0
    assert observability_metrics.project_id_equals_tenant_rows._value.get() == 0
    assert observability_metrics.project_id_equals_tenant_spend_usd._value.get() == 0


@pytest.mark.asyncio
async def test_each_coverage_bucket_reaches_its_own_label(monkeypatch) -> None:
    """⛔ ALL THREE LABELS ARE WRITTEN, INCLUDING A ZERO ONE.

    `tenant_scoped` is 0.0 here because nothing sends the `spend_scope` header
    yet, and the assertion is that the series is PUBLISHED at 0.0 rather than
    left absent: an absent series reads as full coverage, and it is also what
    makes the partition identity uncheckable in Prometheus.

    The two non-zero values are deliberately unequal and non-round so a mutant
    that swaps the `project` and `unattributed` labels fails.
    """
    lic, tenant = _lic(), str(uuid.uuid4())
    _registry_says(monkeypatch, {lic: tenant})

    session = _StubSession(
        window=[
            SimpleNamespace(
                license_id=lic, tenant_id=tenant, events=414, spend=9.7622, delegated=False
            )
        ],
        owners=[(lic, tenant)],
        budgets=[],
        spend_coverage=[
            SimpleNamespace(project_usd=3.7164, tenant_scoped_usd=0.0, unattributed_usd=6.0458)
        ],
    )

    await aa.run_attribution_audit(_factory(session), window_hours=24)

    def _bucket(name):
        return observability_metrics.cost_events_project_spend_usd.labels(
            attribution=name
        )._value.get()

    assert _bucket("project") == pytest.approx(3.7164)
    assert _bucket("tenant_scoped") == pytest.approx(0.0)
    assert _bucket("unattributed") == pytest.approx(6.0458)


def test_the_three_coverage_labels_are_born_and_bounded() -> None:
    """The label set is exactly three, and every one exists from process start.

    ⛔ MUTATION CONTROL: drop `unattributed` from PROJECT_SPEND_STATES and a
    replica that has never completed an audit pass exports only `project`, which
    reads as 100% coverage — the precise failure
    `billing_cost_daily_project_attribution_rows` was created to end, repeated in
    dollars.
    """
    assert observability_metrics.PROJECT_SPEND_STATES == (
        "project",
        "tenant_scoped",
        "unattributed",
    )

    exported = {
        s.labels["attribution"]
        for metric in observability_metrics.cost_events_project_spend_usd.collect()
        for s in metric.samples
    }
    assert exported == set(observability_metrics.PROJECT_SPEND_STATES)


# ---------------------------------------------------------------------------
# R16 (billing-licensing estate repair, D1): billing_project_id_multi_tenant_
# projects gains a `scope` label -- customer vs internal -- so the alert built
# on it can tell an accepted cross-tenant allocation-tag pattern (every tenant
# involved is internal) apart from a real leak (a customer-kind tenant is one
# of them). classify_multi_tenant_scope is the pure adjudication function;
# the run_attribution_audit test below proves the wiring end to end.
# ---------------------------------------------------------------------------


class TestClassifyMultiTenantScope:
    def test_a_project_with_any_customer_tenant_is_customer_scope(self) -> None:
        t1, t2, t3 = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        kind_by_tenant = {t1: "internal", t2: "customer", t3: "internal"}

        counts = aa.classify_multi_tenant_scope(
            [[t1, t2], [t1, t3]],  # first project crosses into a customer tenant
            kind_by_tenant,
        )

        assert counts == {"customer": 1, "internal": 1, "unknown": 0}

    def test_kind_lookup_is_case_and_source_normalised(self) -> None:
        """Tenant ids from the SQL row are lower-cased before the lookup.

        `_PROJECT_MULTI_TENANT_SQL` returns whatever case Postgres stores a
        uuid column in; `services.tenant_names.parse_tenant_rows` always keys
        its map lowercase. A comparison that forgot to normalise either side
        would silently never match and every project would read "unknown".
        """
        tenant = str(uuid.uuid4())
        counts = aa.classify_multi_tenant_scope([[tenant.upper()]], {tenant.lower(): "customer"})
        assert counts == {"customer": 1, "internal": 0, "unknown": 0}

    def test_a_tenant_absent_from_the_registry_is_unknown_not_internal(self) -> None:
        """⛔ ROUND 16 FIX: an unregistered tenant used to fold into `internal`,
        the alert's non-paging scope -- so an unclassifiable tenant SILENCED a
        real customer crossing instead of merely making the gauge quieter.
        `unknown` is a third, alerting scope so absence can no longer be read
        as "not a customer".
        """
        assert aa.classify_multi_tenant_scope([[str(uuid.uuid4())]], {"other": "customer"}) == {
            "customer": 0,
            "internal": 0,
            "unknown": 1,
        }

    def test_an_unclassified_tenant_is_unknown(self) -> None:
        """`kind=UNCLASSIFIED` must take the same alerting direction as an
        absent tenant -- `services.tenant_names.UNCLASSIFIED`'s own invariant
        is that it must never be silently counted as internal (or customer).
        """
        tenant = str(uuid.uuid4())
        counts = aa.classify_multi_tenant_scope([[tenant]], {tenant: "unclassified"})
        assert counts == {"customer": 0, "internal": 0, "unknown": 1}

    def test_test_internal_and_service_kinds_are_internal(self) -> None:
        """The only three kinds that resolve to the quiet, non-alerting scope."""
        t_internal, t_service, t_test = (str(uuid.uuid4()) for _ in range(3))
        kind_by_tenant = {t_internal: "internal", t_service: "service", t_test: "test"}
        counts = aa.classify_multi_tenant_scope(
            [[t_internal], [t_service], [t_test]], kind_by_tenant
        )
        assert counts == {"customer": 0, "internal": 3, "unknown": 0}

    def test_customer_wins_over_unknown_on_the_same_project(self) -> None:
        """A customer tenant sharing a project with an absent/unclassified one
        must still page -- the crossing touches a paying customer regardless of
        what else could not be classified.
        """
        customer, unclassified = str(uuid.uuid4()), str(uuid.uuid4())
        counts = aa.classify_multi_tenant_scope(
            [[customer, unclassified]], {customer: "customer", unclassified: "unclassified"}
        )
        assert counts == {"customer": 1, "internal": 0, "unknown": 0}

    def test_an_empty_population_reports_all_three_scopes_at_zero(self) -> None:
        """All three label values must exist even when nothing crossed -- the
        same "all born" rule PROJECT_ATTRIBUTION_STATES and
        PROJECT_MULTI_TENANT_SCOPES state in observability/metrics.py.
        """
        assert aa.classify_multi_tenant_scope([], {}) == {
            "customer": 0,
            "internal": 0,
            "unknown": 0,
        }


@pytest.mark.asyncio
async def test_run_audit_labels_multi_tenant_projects_by_scope(monkeypatch) -> None:
    """End to end: the SQL row shape -> classification -> the three gauge series.

    ⭐ MUTATION CONTROL for this test lives in
    tests/test_cost_flusher.py::test_project_id_refused_when_it_is_a_tenant_id
    (the ingest half of D1); this one is the audit-gauge half.

    ⭐ MUTATION CONTROL for the customer/internal/unknown split itself: change
    `classify_multi_tenant_scope`'s `unknown` arm back to `internal` and
    `project_unknown`'s expectation flips this test RED, because
    `tenant_unregistered` is genuinely absent from `kind_by_tenant` here.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})

    tenant_customer_a = str(uuid.uuid4())
    tenant_customer_b = str(uuid.uuid4())
    tenant_internal_a = str(uuid.uuid4())
    tenant_internal_b = str(uuid.uuid4())
    tenant_unregistered = str(uuid.uuid4())
    tenant_unclassified = str(uuid.uuid4())
    project_customer = str(uuid.uuid4())
    project_internal = str(uuid.uuid4())
    project_unknown = str(uuid.uuid4())

    async def _fake_get_tenant_info(settings, token_provider=None):
        return {
            tenant_customer_a: SimpleNamespace(kind="internal"),
            tenant_customer_b: SimpleNamespace(kind="customer"),
            tenant_internal_a: SimpleNamespace(kind="internal"),
            tenant_internal_b: SimpleNamespace(kind="internal"),
            tenant_unclassified: SimpleNamespace(kind="unclassified"),
            # tenant_unregistered is deliberately NOT a key here.
        }

    monkeypatch.setattr(aa, "get_tenant_info", _fake_get_tenant_info)

    session = _StubSession(
        window=[],
        owners=[],
        budgets=[],
        multi_tenant=[
            SimpleNamespace(
                project_id=project_customer,
                tenant_ids=[tenant_customer_a, tenant_customer_b],
            ),
            SimpleNamespace(
                project_id=project_internal,
                tenant_ids=[tenant_internal_a, tenant_internal_b],
            ),
            SimpleNamespace(
                project_id=project_unknown,
                tenant_ids=[tenant_unregistered, tenant_unclassified],
            ),
        ],
    )

    await aa.run_attribution_audit(_factory(session), window_hours=24)

    def _scope(name):
        return observability_metrics.project_id_multi_tenant_projects.labels(
            scope=name
        )._value.get()

    assert _scope("customer") == 1
    assert _scope("internal") == 1
    assert _scope("unknown") == 1


@pytest.mark.asyncio
async def test_run_audit_publishes_measured_zero_scope_on_a_clean_pass_with_registry_up(
    monkeypatch,
) -> None:
    """A clean estate (no crossing at all) publishes MEASURED zeros for all
    three labels, never an absent series -- the same rule the three
    unconditional project-axis gauges above this one already follow. The
    registry is deliberately made to answer (non-empty) so this exercises the
    "measured, and there was nothing to classify" path, not the abstention
    path covered separately below.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})

    async def _fake_get_tenant_info(settings, token_provider=None):
        return {str(uuid.uuid4()): SimpleNamespace(kind="internal")}

    monkeypatch.setattr(aa, "get_tenant_info", _fake_get_tenant_info)
    observability_metrics.project_id_multi_tenant_projects.labels(scope="customer").set(99)
    observability_metrics.project_id_multi_tenant_projects.labels(scope="internal").set(99)
    observability_metrics.project_id_multi_tenant_projects.labels(scope="unknown").set(99)
    session = _StubSession(window=[], owners=[], budgets=[])

    await aa.run_attribution_audit(_factory(session), window_hours=24)

    for scope in ("customer", "internal", "unknown"):
        assert (
            observability_metrics.project_id_multi_tenant_projects.labels(scope=scope)._value.get()
            == 0
        )


@pytest.mark.asyncio
async def test_run_audit_abstains_the_scope_gauges_when_the_tenant_kind_registry_is_down(
    monkeypatch,
) -> None:
    """⛔ ROUND 16 FIX: an EMPTY `kind_by_tenant` (the licences-outage shape
    `get_tenant_info` degrades every failure to) must not classify every
    crossing project `unknown` -- that is the outage wearing the shape of a
    measurement. This gauge abstains exactly like the four ownership gauges:
    nothing published, previous value kept, freshness timestamp withheld.

    ⭐ MUTATION CONTROL: delete the `if not tenant_kind_registry_unavailable:`
    guard in front of the three `.labels(scope=...).set(...)` calls and this
    fails -- the sentinel scope values get overwritten with a real (but
    outage-derived) classification, and the timestamp gets written.
    """
    _registry_says(monkeypatch, {_lic(): str(uuid.uuid4())})

    async def _fake_get_tenant_info(settings, token_provider=None):
        return {}

    monkeypatch.setattr(aa, "get_tenant_info", _fake_get_tenant_info)

    sentinel_customer, sentinel_internal, sentinel_unknown = 11, 22, 33
    sentinel_ts = 2222.5
    observability_metrics.project_id_multi_tenant_projects.labels(scope="customer").set(
        sentinel_customer
    )
    observability_metrics.project_id_multi_tenant_projects.labels(scope="internal").set(
        sentinel_internal
    )
    observability_metrics.project_id_multi_tenant_projects.labels(scope="unknown").set(
        sentinel_unknown
    )
    observability_metrics.audit_last_success_timestamp_seconds.set(sentinel_ts)

    tenant_a, tenant_b = str(uuid.uuid4()), str(uuid.uuid4())
    session = _StubSession(
        window=[],
        owners=[],
        budgets=[],
        multi_tenant=[
            SimpleNamespace(project_id=str(uuid.uuid4()), tenant_ids=[tenant_a, tenant_b])
        ],
    )

    await aa.run_attribution_audit(_factory(session), window_hours=24)

    assert (
        observability_metrics.project_id_multi_tenant_projects.labels(scope="customer")._value.get()
        == sentinel_customer
    )
    assert (
        observability_metrics.project_id_multi_tenant_projects.labels(scope="internal")._value.get()
        == sentinel_internal
    )
    assert (
        observability_metrics.project_id_multi_tenant_projects.labels(scope="unknown")._value.get()
        == sentinel_unknown
    )
    assert observability_metrics.audit_last_success_timestamp_seconds._value.get() == sentinel_ts
