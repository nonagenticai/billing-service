"""The population the coherence audit could not see: events with no licence at all.

Measured on prod 2026-08-27, over BILLABLE rows only (``NOT
excluded_from_billing``): **3,751 of 4,993** cost events in 24h carried
``license_id IS NULL``. None of them appeared in any gauge on the coherence
dashboard, because ``_WINDOW_SQL`` adjudicates ownership and there is no
ownership claim on a NULL -- so every number the audit published described only
the 25% of the population that already worked. An instrument that can only see
the healthy half reports health.

What is pinned here:

* the counts and the RATE over a window holding both populations;
* that an empty window publishes NO rate at all -- not 0.0. This is the one that
  matters operationally: 0.0 means "not one billable event could reach an
  invoice line", i.e. total breakage, and a quiet night would fire every alert
  written against the gauge and teach everyone to ignore it;
* that a PARKED duplicate is excluded from the numerator AND the denominator.
  Parked rows are not billed; counting them in the denominator would dilute the
  rate with money no invoice contains, and counting them in the numerator would
  credit attribution to a row that reaches no invoice line either way.

The first two are proved against the pure classifier -- no database, so a
regression is caught in milliseconds. The third cannot be: the exclusion IS a
SQL predicate, and asserting it against a stub would only assert that the stub
was written correctly. It runs against real Postgres.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlalchemy import text

from observability import metrics as observability_metrics
from services import attribution_audit as aa
from services.attribution_audit import classify_attribution


def _lic() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# The classifier: counts and the rate, without a database.
# ---------------------------------------------------------------------------


class TestMixedWindow:
    def test_both_populations_are_counted_with_an_exact_rate(self) -> None:
        """The prod shape, to scale: 1,242 attributed of 4,993 billable."""
        lic, tenant = _lic(), str(uuid.uuid4())
        audit = classify_attribution(
            rows=[(lic, tenant, 1242, 31.05)],
            owner_by_license={lic: tenant},
            unattributed_events=3751,
            unattributed_spend_usd=118.44,
        )

        assert audit.total_events == 1242
        assert audit.unattributed_events == 3751
        assert audit.unattributed_spend_usd == pytest.approx(118.44)
        # The whole billable population, which no field could express before.
        assert audit.billable_events == 4993
        assert audit.attribution_rate == pytest.approx(1242 / 4993)
        # Asserted as a VALUE, not just "> 0": a mutant that reported the
        # attributed count as the rate, or the unattributed count as the
        # numerator, passes any inequality.
        assert audit.attribution_rate == pytest.approx(0.24875, abs=1e-5)

    def test_the_unattributed_half_does_not_leak_into_the_coherence_counts(self) -> None:
        """A licence-less event is not a mismatch and not an unresolved licence.

        It makes no ownership claim, so folding it into either would invent a
        coherence defect out of an attribution gap -- two different problems
        with two different owners.
        """
        lic, billed, owner = _lic(), str(uuid.uuid4()), str(uuid.uuid4())
        audit = classify_attribution(
            rows=[(lic, billed, 10, 1.0)],
            owner_by_license={lic: owner},
            unattributed_events=9_000,
            unattributed_spend_usd=900.0,
        )

        assert audit.mismatch_events == 10
        assert audit.unresolved_events == 0
        assert audit.mismatch_spend_usd == pytest.approx(1.0)
        # Coherence is still measured over the licence-bearing half only, so a
        # growing attribution gap cannot make a coherence regression look
        # smaller.
        assert audit.mismatch_ratio == pytest.approx(1.0)
        assert audit.attribution_rate == pytest.approx(10 / 9010)

    def test_a_fully_unattributed_window_rates_zero_not_none(self) -> None:
        """Zero attributed out of a REAL population is a true 0.0.

        The empty-window abstention below must not be implemented as "falsy
        numerator" -- that would suppress the one reading that means the
        emitter is completely broken.
        """
        audit = classify_attribution(
            rows=[], owner_by_license={}, unattributed_events=4993, unattributed_spend_usd=157.2
        )
        assert audit.billable_events == 4993
        assert audit.attribution_rate == pytest.approx(0.0)


class TestEmptyWindow:
    def test_the_rate_is_absent_not_zero(self) -> None:
        audit = classify_attribution(rows=[], owner_by_license={})
        assert audit.billable_events == 0
        # `is None`, not falsy: 0.0 is falsy too, and that is the exact bug.
        assert audit.attribution_rate is None


# ---------------------------------------------------------------------------
# The publish path: which gauges actually get written.
# ---------------------------------------------------------------------------


class _StubResult:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def all(self) -> list:
        return self._rows

    def one(self):
        """Single-row aggregates (_DERIVED_SPEND_SQL) call .one(), not .all()."""
        return self._rows[0]


class _StubSession:
    def __init__(
        self, window, owners, budgets, unattributed, unregistered=None, derived_spend=None
    ) -> None:
        self._by_sql = {
            id(aa._WINDOW_SQL): window,
            id(aa._OWNERS_SQL): owners,
            id(aa._BUDGETS_SQL): budgets,
            id(aa._UNATTRIBUTED_SQL): unattributed,
            # Defaults to an empty population so the tests in this file keep
            # measuring exactly what they were written to measure. The
            # unregistered-tenant predicate is exercised against real Postgres
            # at the bottom of this file, not here.
            id(aa._UNREGISTERED_TENANT_SQL): unregistered if unregistered is not None else [],
            # Derived-vs-asserted licence spend. Zero-dollar default for the same
            # reason as above: every test in this file predates the query and must
            # keep measuring what it measured. The share's own behaviour is tested
            # in test_attribution_audit.py, including against real Postgres --
            # a stub cannot test SQL.
            id(aa._DERIVED_SPEND_SQL): derived_spend
            if derived_spend is not None
            else [SimpleNamespace(attributed_usd=0.0, derived_usd=0.0)],
            # The project axis (alembic 0036). Same defaulting rationale as the
            # two above: a CLEAN estate — no project id asserted under two
            # tenants, no project-attributed rollup rows — so every test in this
            # file, all of which predate the axis, keeps measuring exactly what
            # it was written to measure. Nothing emits a project id yet, so this
            # is also the honest production value today.
            # ⚠️ R16: one row PER multi-tenant project, carrying its tenant_ids
            # (was a single `count(*) AS projects` row) -- see
            # `_PROJECT_MULTI_TENANT_SQL`'s R16 banner in attribution_audit.py.
            # Empty list is the same "no crossing at all" default the old
            # `projects=0` meant; a stub row without `tenant_ids` no longer
            # matches the query's shape and blows up `_load`'s comprehension.
            id(aa._PROJECT_MULTI_TENANT_SQL): [],
            id(aa._PROJECT_ATTRIBUTION_SQL): [SimpleNamespace(project_rows=0, sentinel_rows=0)],
            # The project-axis AUDIT pair — forged ids (project_id == tenant_id)
            # and the dollar-denominated coverage split. Same defaulting
            # rationale once more: every test in this file predates both, and a
            # default with money in it would let one of them assert a number it
            # never set up. ⚠️ Unlike the two above, the forged-id default is NOT
            # the honest production value — prod reads 62 rows / $6.0458. It is
            # the value these tests want, which is a different thing; the real
            # behaviour is pinned against Postgres in
            # tests/test_attribution_project_spend_audit.py.
            id(aa._PROJECT_SELF_TENANT_SQL): [SimpleNamespace(projects=0, events=0, spend=0.0)],
            id(aa._PROJECT_SPEND_COVERAGE_SQL): [
                SimpleNamespace(project_usd=0.0, tenant_scoped_usd=0.0, unattributed_usd=0.0)
            ],
            # Per-emitter coverage: an empty window (no service_key rows).
            id(aa._EMITTER_COVERAGE_SQL): [],
        }
        self.seen: list[int] = []

    async def execute(self, sql, params=None):
        self.seen.append(id(sql))
        return _StubResult(self._by_sql[id(sql)])


def _factory(session):
    @asynccontextmanager
    async def _cm():
        yield session

    return lambda: _cm()


@pytest.mark.asyncio
async def test_the_new_gauges_are_published_from_the_complement_query() -> None:
    lic, tenant = _lic(), str(uuid.uuid4())
    session = _StubSession(
        window=[
            SimpleNamespace(
                license_id=lic, tenant_id=tenant, events=1242, spend=31.05, delegated=False
            )
        ],
        owners=[(lic, tenant)],
        budgets=[],
        unattributed=[SimpleNamespace(events=3751, spend=118.44)],
    )

    audit = await aa.run_attribution_audit(_factory(session), window_hours=24)

    # Vacuity floor: the complement statement was actually issued. A `_load`
    # that never ran it would report 0 unattributed and look like a clean
    # estate.
    assert id(aa._UNATTRIBUTED_SQL) in session.seen
    assert audit.unattributed_events == 3751

    assert observability_metrics.license_unattributed_events._value.get() == 3751
    assert observability_metrics.attribution_rate._value.get() == pytest.approx(1242 / 4993)


@pytest.mark.asyncio
async def test_an_empty_window_never_sets_the_rate_gauge_to_zero() -> None:
    """THE DEFECT THIS ABSTENTION EXISTS FOR.

    A 0.0 here says "not one billable event could reach an invoice line". On a
    night with no traffic that is a fabrication, and it is indistinguishable
    from the real catastrophe, so any alert on this gauge would page on silence.

    Scope, stated so this test is not read as more than it is: it pins that the
    AUDIT never writes that zero. It does not — and cannot — make the series
    absent, because prometheus_client exports an unlabelled gauge as 0.0 from
    process start. A replica that has never completed a pass is disqualified by
    the freshness timestamp, not by this.

    Asserted two ways, because either alone is weak: a sentinel proves the value
    was not overwritten, and a recorder proves `.set()` was not CALLED at all
    (a mutant that re-set the sentinel would pass the first check).
    """
    sentinel = 0.6180339
    observability_metrics.attribution_rate.set(sentinel)

    calls: list[float] = []
    original = observability_metrics.attribution_rate.set

    def _record(value, *a, **k):
        calls.append(value)
        return original(value, *a, **k)

    # cast: patching a bound method on the Gauge instance, which its type forbids.
    cast(Any, observability_metrics.attribution_rate).set = _record
    try:
        session = _StubSession(
            window=[],
            owners=[],
            budgets=[],
            unattributed=[SimpleNamespace(events=0, spend=0.0)],
        )
        audit = await aa.run_attribution_audit(_factory(session), window_hours=24)
    finally:
        cast(Any, observability_metrics.attribution_rate).set = original

    assert audit.billable_events == 0
    assert audit.attribution_rate is None
    assert calls == [], f"attribution_rate was published on an empty window: {calls}"
    assert observability_metrics.attribution_rate._value.get() == pytest.approx(sentinel)
    # The COUNT is still published — zero unattributed events is a true and
    # useful reading; only the ratio is undefined without a denominator.
    assert observability_metrics.license_unattributed_events._value.get() == 0


# ---------------------------------------------------------------------------
# Parked rows, against real Postgres.
#
# `excluded_from_billing` is a SQL predicate in two separate statements. A stub
# session cannot prove it holds in both — it would only prove the stub said so.
# ---------------------------------------------------------------------------

_INSERT = text(
    "INSERT INTO billing_cost_events"
    " (id, tenant_id, tenant_name, license_id, service_key, provider,"
    "  input_tokens, output_tokens, cost_usd,"
    "  excluded_from_billing, exclusion_reason, created_at)"
    " VALUES (:id, :tenant_id, 'acme', :license_id, 'inference-router', 'pgx-local',"
    "  100, 50, :cost_usd, :excluded, :reason, :created_at)"
)


async def _insert(session, *, tenant, license_id, cost, excluded, reason=None) -> None:
    # `reason` defaults to the 0013 duplicate park, which is what every existing
    # caller means by `excluded=True`. It is overridable because the two park
    # REASONS are read differently by the unregistered-tenant gauge (#241).
    await session.execute(
        _INSERT,
        {
            "id": uuid.uuid4(),
            "tenant_id": tenant,
            "license_id": license_id,
            "cost_usd": Decimal(cost),
            "excluded": excluded,
            "reason": reason or ("router_duplicate" if excluded else None),
            "created_at": datetime.now(UTC) - timedelta(hours=1),
        },
    )


@pytest.mark.asyncio
async def test_parked_rows_are_excluded_from_both_numerator_and_denominator(
    db_session, db_session_factory
) -> None:
    tenant = str(uuid.uuid4())
    lic = uuid.uuid4()

    # Billable: 2 attributed, 3 unattributed -> rate 2/5.
    for _ in range(2):
        await _insert(db_session, tenant=tenant, license_id=lic, cost="1.00", excluded=False)
    for _ in range(3):
        await _insert(db_session, tenant=tenant, license_id=None, cost="2.00", excluded=False)

    # Parked twins of BOTH halves, deliberately lopsided (7 parked attributed,
    # 11 parked unattributed) so that any leak moves the rate in a direction
    # this assertion can see, whichever query leaks.
    for _ in range(7):
        await _insert(db_session, tenant=tenant, license_id=lic, cost="9.00", excluded=True)
    for _ in range(11):
        await _insert(db_session, tenant=tenant, license_id=None, cost="9.00", excluded=True)
    await db_session.commit()

    audit = await aa.run_attribution_audit(db_session_factory, window_hours=24)

    # Denominator: the 18 parked rows are absent from both halves.
    assert audit.total_events == 2
    assert audit.unattributed_events == 3
    assert audit.billable_events == 5
    assert audit.attribution_rate == pytest.approx(0.4)
    # Spend too — a parked row carries $9.00 precisely so a leak is unmissable.
    assert audit.unattributed_spend_usd == pytest.approx(6.0)


@pytest.mark.asyncio
async def test_rows_older_than_the_window_are_not_counted(db_session, db_session_factory) -> None:
    """The window bound applies to the complement query as well.

    Without this, the new gauge would be a lifetime total wearing a 24h label
    and could never fall when an emitter was fixed — the exact reason it is a
    Gauge rather than a Counter.
    """
    tenant = str(uuid.uuid4())
    await db_session.execute(
        _INSERT,
        {
            "id": uuid.uuid4(),
            "tenant_id": tenant,
            "license_id": None,
            "cost_usd": Decimal("5.00"),
            "excluded": False,
            "reason": None,
            "created_at": datetime.now(UTC) - timedelta(days=9),
        },
    )
    await _insert(db_session, tenant=tenant, license_id=None, cost="1.00", excluded=False)
    await db_session.commit()

    audit = await aa.run_attribution_audit(db_session_factory, window_hours=24)

    assert audit.unattributed_events == 1
    assert audit.unattributed_spend_usd == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Spend booked to a tenant billing has no record of, against real Postgres.
#
# Same reasoning as the parked-rows test above, and it is the WHOLE reason this
# test exists: `_UNREGISTERED_TENANT_SQL`'s definition lives in two NOT EXISTS
# subqueries. The stub-session tests in test_attribution_audit.py hand the audit
# a pre-made list, so they prove the WIRING and are structurally incapable of
# proving the PREDICATE. Drop either subquery and every one of them still passes.
#
# Billing cannot check the real registry — its service account gets HTTP 403 on
# the licences `GET /tenants` (measured in the pod 2026-08-28) — so "registered"
# here means "billing holds a quota row OR a budget row for it". Measured
# against the truth the same day: 28 flagged vs 26 genuinely absent from
# `licenses.tenants`, i.e. 3 false positives and 1 miss.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_tenants_absent_from_both_registries_are_counted(
    db_session, db_session_factory
) -> None:
    from models.db import NULL_LICENSE_ID, RequestQuotaConfig, TokenBudget

    known_by_quota = str(uuid.uuid4())
    known_by_budget = str(uuid.uuid4())
    unknown = str(uuid.uuid4())

    db_session.add(
        RequestQuotaConfig(
            license_id=uuid.uuid4(), tenant_id=known_by_quota, quota_monthly=1_000_000
        )
    )
    # The OTHER arm of the union. A tenant billing holds a BUDGET for is known
    # even with no quota row; without this row the test could not tell a working
    # union from a query that only ever looked at request_quota_config.
    db_session.add(
        TokenBudget(
            tenant_id=known_by_budget,
            tenant_name="known-by-budget",
            license_id=NULL_LICENSE_ID,
            budget_tokens=10_000_000,
        )
    )
    await db_session.flush()

    # 2 billable events each for the two KNOWN tenants...
    for tenant in (known_by_quota, known_by_budget):
        for _ in range(2):
            await _insert(db_session, tenant=tenant, license_id=None, cost="1.00", excluded=False)
    # ...4 billable for the unknown one, at a deliberately distinct unit cost so
    # a leak shows up in the money as well as the count...
    for _ in range(4):
        await _insert(db_session, tenant=unknown, license_id=None, cost="0.25", excluded=False)
    # ...and 5 PARKED rows for the unknown tenant. These must not be counted:
    # auditing attribution against spend the invoice does not contain is the
    # same defect the parked-rows test above pins for the other two statements.
    for _ in range(5):
        await _insert(db_session, tenant=unknown, license_id=None, cost="99.00", excluded=True)
    await db_session.commit()

    audit = await aa.run_attribution_audit(db_session_factory, window_hours=24)

    assert audit.unregistered_tenant_events == 4
    assert audit.unregistered_tenant_spend_usd == pytest.approx(1.00)
    # And it NAMES the one tenant, so the finding is actionable rather than a
    # bare count. The two known tenants must not appear at all.
    named = dict(audit.top_unregistered_tenants)
    assert named == {unknown: 4}


# ---------------------------------------------------------------------------
# #241: arming the unregistered-tenant gate must not blind its own detector.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_arming_the_gate_does_not_blind_the_gauge_that_flags_it(
    db_session, db_session_factory
) -> None:
    """Rows parked as 'unregistered_tenant' stay visible; other parks do not.

    Armed, `billing_exclude_unregistered_tenants` writes an unregistered
    tenant's row with `excluded_from_billing = true`. Had
    `_UNREGISTERED_TENANT_SQL` kept the plain billable filter, the gauge would
    fall to zero on the day of arming and read as "registry gap fixed".

    The discriminator is the REASON, not the flag: both tenants below are absent
    from `request_quota_config` and `token_budgets`, so only the reason
    predicate separates them.

    MUTATION: restore `AND e.excluded_from_billing = false` and the first
    assertions fall to 0.
    """
    unregistered = str(uuid.uuid4())
    duplicate = str(uuid.uuid4())

    # Parked by the armed gate: 5 events, $10.00. Must stay visible.
    for _ in range(5):
        await _insert(
            db_session,
            tenant=unregistered,
            license_id=None,
            cost="2.00",
            excluded=True,
            reason="unregistered_tenant",
        )
    # Parked by 0013 as a router duplicate: 7 events, $63.00. Must stay OUT.
    for _ in range(7):
        await _insert(
            db_session,
            tenant=duplicate,
            license_id=None,
            cost="9.00",
            excluded=True,
            reason="router_duplicate",
        )
    await db_session.commit()

    audit = await aa.run_attribution_audit(db_session_factory, window_hours=24)

    assert audit.unregistered_tenant_events == 5, (
        "the armed gate's own rows vanished from the gauge that exists to find them"
    )
    assert audit.unregistered_tenant_spend_usd == pytest.approx(10.0)
    named = dict(audit.top_unregistered_tenants)
    assert unregistered in named, "the offending tenant is no longer nameable"
    assert duplicate not in named, "a router duplicate was mis-reported as a registry gap"
    # The exported gauge carries it too, not just the audit object.
    assert observability_metrics.license_tenant_unregistered_events._value.get() == 5

    # The money-side statements still drop the parked row: the fix widened the
    # detector only, not the billable population.
    assert audit.billable_events == 0


@pytest.mark.asyncio
async def test_an_unparked_unregistered_tenant_is_still_counted(
    db_session, db_session_factory
) -> None:
    """Control: an UNPARKED unregistered row (gate off, or rolled back) still counts.

    Guards against a predicate that went indifferent to the flag in the wrong
    direction (e.g. one that only admitted parked rows).
    """
    tenant = str(uuid.uuid4())
    for _ in range(3):
        await _insert(db_session, tenant=tenant, license_id=None, cost="1.50", excluded=False)
    await db_session.commit()

    audit = await aa.run_attribution_audit(db_session_factory, window_hours=24)

    assert audit.unregistered_tenant_events == 3
    assert audit.unregistered_tenant_spend_usd == pytest.approx(4.5)
