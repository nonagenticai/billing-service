"""Tests for services.unregistered_tenant_spend.publish_unregistered_tenant_spend.

THE GAP THIS CLOSES: ``billing_tenant_unregistered_total`` already exists and
already fires whenever a cost event resolves to a tenant uuid the licences
registry does not vouch for, but it is a Counter that lives only as long as the
pod does. Measured 2026-09-12, both replicas read 0 for three tenant ids that
had, between them, run up $22.89 across 903 rows in the ledger — because those
ids had gone quiet three days before the pods restarted, and a restart wipes a
per-process counter back to zero. "No unregistered spend since the last deploy"
and "no unregistered spend, ever" are indistinguishable on that instrument.
``publish_unregistered_tenant_spend`` answers the same question from the
durable ledger (``billing_cost_events``) instead of from process memory, so a
restart cannot erase the finding: it re-aggregates the last 30 days on every
call and SETS two Gauges — ``billing_unregistered_tenant_spend_usd`` and
``billing_unregistered_tenant_count`` — to whatever it finds, including an
explicit zero when nothing is wrong.

These four tests pin, in order: the positive case (an unregistered tenant is
found and its money is reported), the negative control (a registered tenant
must NOT be flagged — without this, a version of the function that flagged
every tenant indiscriminately would also pass the positive case), the
explicit-zero case (the whole point of a durable gauge is that "clean" and
"never measured" must read differently), and the deliberate judgement call
that a row parked on ``excluded_from_billing`` is still counted, because it was
parked *for being unregistered* in the first place.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from prometheus_client import REGISTRY
from sqlalchemy.ext.asyncio import AsyncSession

import tenancy
from models.db import CostEvent
from observability import metrics as observability_metrics
from services import unregistered_tenant_spend
from services.unregistered_tenant_spend import publish_unregistered_tenant_spend


def _cost_event(
    *,
    tenant_id: str,
    cost_usd: Decimal,
    days_ago: float = 1.0,
    service_key: str = "test-svc",
    provider: str = "testprov",
    excluded_from_billing: bool = False,
) -> CostEvent:
    """One billable-shaped cost event row, aged inside the 30-day lookback.

    Field set copied from the pattern shared by ``tests/test_export.py::_cost``
    and ``tests/test_backfill_app_attribution.py::_seed_event`` — there is no
    shared factory for ``CostEvent`` in this repo (``tests/_budget_factory.py``
    only covers ``TokenBudget``), so every test module hand-rolls its own local
    builder and this file follows the same convention rather than inventing a
    new shared one.

    ``days_ago`` defaults well inside ``LOOKBACK_DAYS`` (30) so a row is
    visible to the function under test without the caller having to think
    about the window on every call site.
    """
    return CostEvent(
        tenant_id=tenant_id,
        tenant_name=tenant_id,
        service_key=service_key,
        provider=provider,
        input_tokens=100,
        output_tokens=50,
        cost_usd=cost_usd,
        excluded_from_billing=excluded_from_billing,
        created_at=datetime.now(UTC) - timedelta(days=days_ago),
    )


def _gauge(name: str) -> float:
    """Current absolute value of an unlabelled Gauge, never ``None``.

    Same ``REGISTRY.get_sample_value`` idiom as ``tests/test_pricing.py`` (for
    a labelled Counter) and ``tests/test_budget_reserve_estimate_ratio.py``
    (for an unlabelled series) — called with no label dict here because
    ``billing_unregistered_tenant_spend_usd`` and
    ``billing_unregistered_tenant_count`` are both deliberately label-less
    (a per-tenant label would be unbounded cardinality keyed on untrusted
    input, per the comment above their definitions in
    ``src/observability/metrics.py``).

    These ARE the absolute values the gauges were last ``.set()`` to, not a
    delta — correct for this module, since ``publish_unregistered_tenant_spend``
    recomputes and SETS the full 30-day aggregate on every call rather than
    incrementing, and the per-test DB truncation (``tests/conftest.py``'s
    ``db_engine`` fixture) means each test's own call is computed over only
    the rows that test itself inserted.
    """
    value = REGISTRY.get_sample_value(name)
    return float(value or 0.0)


@pytest.fixture(autouse=True)
def _reset_tenant_registry_overlay():
    """Isolate the live-registry overlay so test order cannot change the answer.

    ``tenancy.is_registered_tenant_id`` unions the live overlay
    (``tenancy.set_live_tenant_registry`` / ``reset_live_tenant_registry``)
    with the session-wide ``_REGISTERED_TENANT_IDS`` snapshot that
    ``tests/conftest.py``'s ``T()`` helper only ever WIDENS for the life of the
    test session. This fixture resets the overlay half — the half this file
    actually drives — before and after every test, so installing a tenant as
    "live-registered" in one test cannot leak into the next test's read of
    ``is_registered_tenant_id`` for an unrelated random uuid.
    """
    tenancy.reset_live_tenant_registry()
    yield
    tenancy.reset_live_tenant_registry()


@pytest.mark.unit
class TestUnregisteredTenantSpend:
    async def test_spend_on_an_unregistered_tenant_is_published(
        self, db_session: AsyncSession
    ) -> None:
        """A tenant uuid no registry vouches for is reported, with its money.

        IN PRODUCTION IF THIS FAILS: the gauge this module exists to fix goes
        back to reading a comfortable zero while real spend sits against a
        tenant nobody can invoice — exactly the condition measured on
        2026-09-12 ($22.89 over 903 rows across three silent tenant ids) that
        the per-pod-lifetime counter could not report after a restart.
        """
        tenant = str(uuid.uuid4())
        db_session.add_all(
            [
                _cost_event(tenant_id=tenant, cost_usd=Decimal("1.50")),
                _cost_event(tenant_id=tenant, cost_usd=Decimal("2.25")),
            ]
        )
        await db_session.commit()

        count = await publish_unregistered_tenant_spend(db_session)

        assert count == 1, (
            f"expected exactly one offending tenant id, got {count} -- either "
            "the unregistered uuid was missed entirely, or its two rows were "
            "double-counted as separate tenants"
        )
        assert _gauge("billing_unregistered_tenant_count") == 1.0, (
            "billing_unregistered_tenant_count did not get set to 1 -- an "
            "operator reading this gauge would see no standing unregistered "
            "population even though one genuinely exists in the ledger"
        )
        assert _gauge("billing_unregistered_tenant_spend_usd") == pytest.approx(3.75), (
            "billing_unregistered_tenant_spend_usd does not equal the summed "
            "cost_usd (1.50 + 2.25) of this tenant's two rows -- the dollar "
            "figure an operator would act on is wrong, which is worse than a "
            "merely wrong count"
        )

    async def test_a_registered_tenant_is_not_counted(self, db_session: AsyncSession) -> None:
        """NEGATIVE CONTROL: a tenant the registry vouches for must read clean.

        Without this test, a version of the function that flagged EVERY
        tenant indiscriminately -- never actually consulting
        ``tenancy.is_registered_tenant_id`` -- would still pass the positive
        test above. This is what proves the function discriminates rather
        than just reporting "there was spend."

        IN PRODUCTION IF THIS FAILS: every registered, perfectly legitimate
        tenant would be reported as unregistered, turning a targeted signal
        into noise an operator has to ignore -- which is how a real
        occurrence, like the one this module exists to catch, gets lost in a
        gauge that is always non-zero.
        """
        tenant = str(uuid.uuid4())
        installed = tenancy.set_live_tenant_registry({"test-registered-tenant": tenant})
        assert installed == 1, (
            "precondition failed: the live registry overlay did not install "
            "this tenant id at all, so this test would not be exercising the "
            "registered path it claims to"
        )
        assert tenancy.is_registered_tenant_id(tenant), (
            "precondition failed: is_registered_tenant_id still answers False "
            "for a tenant that was just installed into the live overlay -- "
            "this test cannot be a negative control on a broken precondition"
        )

        db_session.add(_cost_event(tenant_id=tenant, cost_usd=Decimal("5.00")))
        await db_session.commit()

        count = await publish_unregistered_tenant_spend(db_session)

        assert count == 0, (
            f"a REGISTERED tenant was reported as offending (count={count}) -- "
            "the function is flagging tenants the licences registry explicitly "
            "vouches for, which would make every legitimate tenant's spend look "
            "like the defect this gauge exists to surface"
        )
        assert _gauge("billing_unregistered_tenant_count") == 0.0, (
            "billing_unregistered_tenant_count is non-zero for a registered "
            "tenant's spend -- the count gauge does not actually reflect "
            "registry membership"
        )
        assert _gauge("billing_unregistered_tenant_spend_usd") == 0.0, (
            "billing_unregistered_tenant_spend_usd is non-zero for a "
            "registered tenant's spend -- a registered tenant's legitimate "
            "$5.00 would be reported as unbillable money nobody can invoice"
        )

    async def test_a_clean_ledger_publishes_an_explicit_zero(
        self, db_session: AsyncSession
    ) -> None:
        """A scan that finds nothing must SET zero, not leave a stale reading.

        The defect this whole module exists to fix is a metric that cannot
        distinguish "clean" from "never observed" (the per-pod-lifetime
        counter reading 0 after a restart, indistinguishable from reading 0
        because nothing is wrong). An absent or merely-unchanged gauge here
        would reproduce the exact same ambiguity one level up the stack.

        The gauges are poisoned with an obviously-wrong prior value first: if
        the function skipped writing on a clean scan instead of actively
        publishing 0, the poison would still be standing afterwards and this
        test would catch it -- a gauge that simply starts at 0 and is never
        touched would pass this assertion for the wrong reason.
        """
        observability_metrics.unregistered_tenant_spend_usd.set(999.0)
        observability_metrics.unregistered_tenant_count.set(999)

        count = await publish_unregistered_tenant_spend(db_session)

        assert count == 0, f"expected 0 offending tenants against an empty ledger, got {count}"
        assert _gauge("billing_unregistered_tenant_count") == 0.0, (
            "billing_unregistered_tenant_count still reads the poisoned 999 -- "
            "a clean scan left the previous reading standing instead of "
            "actively setting 0, which is precisely the 'clean vs not "
            "observed' ambiguity this module exists to end"
        )
        assert _gauge("billing_unregistered_tenant_spend_usd") == 0.0, (
            "billing_unregistered_tenant_spend_usd still reads the poisoned "
            "999.0 -- same failure as the count gauge: a clean scan must "
            "overwrite the prior value with an explicit 0, not skip the write"
        )

    async def test_rows_excluded_from_billing_are_still_counted(
        self, db_session: AsyncSession
    ) -> None:
        """A row parked on ``excluded_from_billing`` still contributes.

        This pins the one deliberate judgement call documented in
        ``services/unregistered_tenant_spend.py``: the cost flusher parks an
        unregistered tenant's rows on ``excluded_from_billing`` PRECISELY
        BECAUSE they are unregistered. If this gauge filtered those rows out,
        the population it exists to size would shrink to zero exactly as the
        parking mechanism did its job -- reporting "fixed" on a condition that
        is, at best, quarantined rather than resolved (the money still belongs
        to nobody; it is simply no longer on an invoice).

        IN PRODUCTION IF THIS FAILS: once the flusher's parking defence
        engages for an unregistered tenant, this gauge would silently go to
        zero and an operator would believe the standing population had
        cleared, when it had only stopped being billed.
        """
        tenant = str(uuid.uuid4())
        db_session.add(
            _cost_event(
                tenant_id=tenant,
                cost_usd=Decimal("7.77"),
                excluded_from_billing=True,
            )
        )
        await db_session.commit()

        count = await publish_unregistered_tenant_spend(db_session)

        assert count == 1, (
            f"a row excluded_from_billing=True for an unregistered tenant was "
            f"not counted at all (count={count}) -- filtering it out hides the "
            "population this gauge exists to size"
        )
        assert _gauge("billing_unregistered_tenant_spend_usd") == pytest.approx(7.77), (
            "the excluded row's $7.77 did not reach "
            "billing_unregistered_tenant_spend_usd -- excluded_from_billing "
            "rows must still contribute to the reported money, because they "
            "were excluded FOR being unregistered, not despite it"
        )


def _labelled(name: str, tenant_id: str) -> float | None:
    """Value of a per-tenant series, or ``None`` when the series does not exist.

    ⚠️ ``None`` and ``0.0`` are deliberately NOT collapsed here, unlike the
    unlabelled ``_gauge`` helper above. For a labelled Gauge the distinction is
    the whole point of :func:`_publish_named_series`'s ``clear()``: an absent
    series means "this tenant is not currently unregistered", whereas a present
    series reading 0.0 would mean "unregistered, and it has cost nothing yet".
    A helper that returned 0.0 for both would make the staleness test below
    unable to fail.
    """
    return REGISTRY.get_sample_value(name, {"tenant_id": tenant_id})


@pytest.mark.unit
class TestUnregisteredTenantIsNamed:
    """The named breakdown: an alert must be able to say WHICH tenant.

    Before these series existed the only per-tenant rendering was the log line,
    and it passes every id through ``tenancy.redact_tenant_id``, which truncates
    at 24 chars — a uuid is 36. Production therefore logged
    ``ad976f5b…(redacted,len=36)`` and named nobody.
    """

    async def test_the_offending_tenant_is_named_in_its_own_series(
        self, db_session: AsyncSession
    ) -> None:
        """The full uuid appears as a label, with its money and its event count.

        IN PRODUCTION IF THIS FAILS: the alert goes back to saying "four tenants
        are unbillable" without saying which four, and the only way to find out
        is a hand-run query against the prod ledger — the exact defect filed as
        mlx_ingestion#54.
        """
        tenant = str(uuid.uuid4())
        db_session.add_all(
            [
                _cost_event(tenant_id=tenant, cost_usd=Decimal("1.50")),
                _cost_event(tenant_id=tenant, cost_usd=Decimal("2.25")),
            ]
        )
        await db_session.commit()

        await publish_unregistered_tenant_spend(db_session)

        assert _labelled("billing_unregistered_spend_by_tenant_usd", tenant) == pytest.approx(
            3.75
        ), (
            "the unregistered tenant has no per-tenant spend series (or the wrong "
            "amount) -- an operator reading this metric cannot name the tenant "
            "whose spend is unbillable, which is the entire reason it exists"
        )
        assert _labelled("billing_unregistered_events_by_tenant", tenant) == pytest.approx(2.0), (
            "the per-tenant EVENT count is missing or wrong -- money alone hides a "
            "high-volume emitter that is cheap today and will not be after a "
            "model price change"
        )

    async def test_a_registered_tenant_is_never_named(self, db_session: AsyncSession) -> None:
        """POSITIVE CONTROL: a good tenant must get NO series at all.

        Without this, a version of ``_publish_named_series`` that labelled every
        tenant it saw -- never consulting ``is_registered_tenant_id`` -- would
        still pass the test above. A metric that flags everything is not a
        control, and this is the assertion that proves discrimination.

        IN PRODUCTION IF THIS FAILS: every legitimate tenant gets a series on a
        metric whose only meaning is "this tenant cannot be invoiced", and the
        real offenders become invisible in the noise.
        """
        good = str(uuid.uuid4())
        ghost = str(uuid.uuid4())
        installed = tenancy.set_live_tenant_registry({"test-registered-tenant": good})
        assert installed == 1, (
            "precondition failed: the live registry overlay did not install the "
            "good tenant, so this test would not be exercising the registered path"
        )

        db_session.add_all(
            [
                _cost_event(tenant_id=good, cost_usd=Decimal("500.00")),
                _cost_event(tenant_id=ghost, cost_usd=Decimal("0.01")),
            ]
        )
        await db_session.commit()

        await publish_unregistered_tenant_spend(db_session)

        assert _labelled("billing_unregistered_spend_by_tenant_usd", good) is None, (
            "a REGISTERED tenant was given an unregistered-spend series -- this "
            "metric is flagging tenants the licences registry explicitly vouches "
            "for, so it names innocent parties and cannot be alerted on"
        )
        assert _labelled("billing_unregistered_events_by_tenant", good) is None, (
            "a REGISTERED tenant was given an unregistered-events series"
        )
        assert _labelled("billing_unregistered_spend_by_tenant_usd", ghost) == pytest.approx(
            0.01
        ), (
            "the genuinely unregistered tenant in the SAME pass was not named -- "
            "this proves the negative above came from discrimination rather than "
            "from the function publishing nothing at all"
        )

    async def test_a_tenant_that_becomes_registered_loses_its_series(
        self, db_session: AsyncSession
    ) -> None:
        """A resolved tenant must STOP existing, not freeze at its last value.

        A labelled Gauge never forgets a child, so without the ``clear()`` in
        ``_publish_named_series`` the series would keep exporting its last
        non-zero spend forever. Registering the tenant is the outcome this whole
        metric exists to drive, and it would leave the alert permanently firing
        -- the fix would look exactly like the defect.

        IN PRODUCTION IF THIS FAILS: the only way to clear the alert is a pod
        restart, which is also the failure mode
        ``billing_tenant_unregistered_total`` was replaced for.
        """
        tenant = str(uuid.uuid4())
        db_session.add(_cost_event(tenant_id=tenant, cost_usd=Decimal("9.99")))
        await db_session.commit()

        await publish_unregistered_tenant_spend(db_session)
        assert _labelled("billing_unregistered_spend_by_tenant_usd", tenant) == pytest.approx(
            9.99
        ), "precondition failed: the tenant was never named while it was unregistered"

        # The operator does the thing the metric asked for.
        tenancy.set_live_tenant_registry({"now-registered": tenant})
        await publish_unregistered_tenant_spend(db_session)

        assert _labelled("billing_unregistered_spend_by_tenant_usd", tenant) is None, (
            "the series survived the tenant being registered -- a stale child is "
            "still exporting $9.99 of 'unbillable' spend for a tenant that is now "
            "perfectly billable, and the alert it raised can never resolve"
        )
        assert _labelled("billing_unregistered_events_by_tenant", tenant) is None, (
            "the per-tenant events series survived the tenant being registered"
        )

    async def test_ids_past_the_cap_land_in_the_overflow_bucket(
        self, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The cap bounds the SERIES, never the money.

        ``tenant_id`` is caller-supplied, so the label domain is open and the cap
        is what stops a minting emitter turning /metrics into a cardinality
        incident. But dropping the residual outright would make the labelled view
        silently under-report, and the gap against the unlabelled total would look
        like a bug in one of the two. The overflow bucket is what keeps them
        reconcilable.

        IN PRODUCTION IF THIS FAILS: either /metrics grows a series per minted
        uuid, or money vanishes from the per-tenant view with nothing to say it
        was dropped.
        """
        monkeypatch.setattr(unregistered_tenant_spend, "MAX_NAMED_TENANT_SERIES", 2)

        # Descending spend so the cap's ordering is unambiguous: 3.00, 2.00, then
        # 1.00 + 0.50 which must be folded together into __overflow__.
        amounts = [Decimal("3.00"), Decimal("2.00"), Decimal("1.00"), Decimal("0.50")]
        tenants = [str(uuid.uuid4()) for _ in amounts]
        for tenant, amount in zip(tenants, amounts, strict=True):
            db_session.add(_cost_event(tenant_id=tenant, cost_usd=amount))
        await db_session.commit()

        found = await publish_unregistered_tenant_spend(db_session)

        assert found == 4, f"expected all 4 offenders to be FOUND, got {found}"
        assert _gauge("billing_unregistered_tenant_spend_usd") == pytest.approx(6.50), (
            "the unlabelled TOTAL must stay authoritative and complete regardless "
            "of the series cap -- it is the number the cap exists to protect"
        )

        named = [
            t
            for t in tenants
            if _labelled("billing_unregistered_spend_by_tenant_usd", t) is not None
        ]
        assert len(named) == 2, (
            f"expected exactly MAX_NAMED_TENANT_SERIES=2 named series, got "
            f"{len(named)} -- the cap is not bounding label cardinality, so a "
            "minting emitter would mint one permanent time series per request"
        )
        assert set(named) == set(tenants[:2]), (
            "the cap kept the wrong tenants -- it must keep the highest SPEND, so "
            "that a flood of cheap minted uuids evicts nothing that matters"
        )

        overflow = _labelled(
            "billing_unregistered_spend_by_tenant_usd",
            unregistered_tenant_spend.OVERFLOW_TENANT_LABEL,
        )
        assert overflow == pytest.approx(1.50), (
            f"the two capped tenants' 1.00 + 0.50 did not reach the __overflow__ "
            f"bucket (got {overflow}) -- money dropped out of the per-tenant view "
            "entirely, so it no longer reconciles against the unlabelled total"
        )
        assert _labelled(
            "billing_unregistered_events_by_tenant",
            unregistered_tenant_spend.OVERFLOW_TENANT_LABEL,
        ) == pytest.approx(2.0), "the capped tenants' event counts did not reach __overflow__"

    async def test_no_overflow_bucket_when_nothing_is_capped(
        self, db_session: AsyncSession
    ) -> None:
        """__overflow__ must be ABSENT, not zero, in the normal case.

        A permanently present zero-valued bucket would be indistinguishable from
        a real overflow that happened to cost nothing, and would show a non-uuid
        label value to every operator who reads the metric.
        """
        db_session.add(_cost_event(tenant_id=str(uuid.uuid4()), cost_usd=Decimal("1.00")))
        await db_session.commit()

        await publish_unregistered_tenant_spend(db_session)

        assert (
            _labelled(
                "billing_unregistered_spend_by_tenant_usd",
                unregistered_tenant_spend.OVERFLOW_TENANT_LABEL,
            )
            is None
        ), (
            "an __overflow__ series exists even though nothing was capped -- a "
            "standing zero bucket cannot be told apart from a genuine overflow"
        )
