"""Tests for the periodic licence↔budget reconcile.

THE DEFECT THESE PIN, stated as a measurement rather than a description:
on production at 2026-09-10 there were 19 active ``source='license_sync'``
token budgets totalling 430,000,000 tokens whose licence was ``revoked``
upstream. ``licenses`` PR #192 pushes a teardown at explicit-revoke time, but
the nightly ``reap_expired_licences`` CronJob writes ``status='revoked'``
straight to the database and cannot reach that push, so the residue keeps
accruing. Nothing on a timer looked.

The class that matters most here is
:class:`TestUnreadableStatusNeverDeactivates`. This loop is strictly MORE
conservative than the manual ``POST /api/budget/sync-from-licenses``: that
endpoint drops an unrecognised status out of its active set, which for an
unattended loop would mean that the day ``licenses`` grows a ``'suspended'``
status, every budget behind it is silently deactivated. Those tests are the
control on the fail-safe direction, and they fail if someone "simplifies" this
module by passing ``active_ids`` where ``protected_ids`` is passed today.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import cast

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.domain import BudgetPeriod
from services.budget_crud import upsert_license_sync_budget
from services.licence_budget_reconcile import (
    LicenceFetchError,
    _fetch_licence_lifecycle,
    measure_divergence,
    reconcile_once,
)
from settings import Settings

from .conftest import T

pytestmark = pytest.mark.asyncio


def _lic(lic_id: uuid.UUID, status: str | None = "active") -> dict:
    """One licence as ``GET /internal/licenses`` returns it."""
    row: dict = {"id": str(lic_id), "tenant_id": T("t1")}
    if status is not None:
        row["status"] = status
    return row


def _client_returning(payload, status_code: int = 200) -> httpx.AsyncClient:
    """An AsyncClient whose every request is served from memory."""

    def handler(_request: httpx.Request) -> httpx.Response:
        if isinstance(payload, str):
            return httpx.Response(status_code, text=payload)
        return httpx.Response(status_code, json=payload)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class _FakeSettings:
    """Only the fields the reconcile reads."""

    licenses_url = "http://licences.invalid"

    def __init__(self, *, deactivate: bool, max_deactivations: int = 50) -> None:
        self.licence_budget_reconcile_deactivate_enabled = deactivate
        self.licence_budget_reconcile_max_deactivations_per_cycle = max_deactivations


def _Settings(*, deactivate: bool, max_deactivations: int = 50) -> Settings:  # noqa: N802
    """A ``_FakeSettings`` typed as the real ``Settings`` it stands in for."""
    return cast(Settings, _FakeSettings(deactivate=deactivate, max_deactivations=max_deactivations))


class TestFetchClassification:
    @pytest.mark.unit
    async def test_active_and_revoked_are_separated(self):
        live, dead = uuid.uuid4(), uuid.uuid4()
        async with _client_returning([_lic(live), _lic(dead, "revoked")]) as client:
            got = await _fetch_licence_lifecycle(_Settings(deactivate=True), None, client=client)
        assert got.active_ids == {live}
        # `dead` is deliberately absent from BOTH sets: not active, and not
        # protected — that is what permits its budget to be deactivated.
        assert got.protected_ids == {live}
        assert got.non_active_count == 1
        assert got.unreadable_count == 0

    @pytest.mark.unit
    async def test_expired_is_treated_as_non_active(self, db_session_factory):
        """`expired` is not `revoked`. licenses' expire_stale_licences CronJob
        produces exactly this status and no push fires for it, so a reconcile
        that only understood 'revoked' would leave that residue behind.

        ⚠️ THIS TEST USED TO ASSERT ``protected_ids == set()`` AND NOTHING ELSE,
        which pinned the estate-wipe as the specification. The classification
        below is correct and is kept -- an expired licence really is non-active
        and really does belong in neither set -- but on a payload where it is
        the ONLY licence, that empty derived set is the mass-deactivation
        input, and a test that stops at "== set()" says the system is behaving
        as designed at the exact moment it is about to wipe everything.

        So the classification is asserted alongside what the CALLER must do
        with it: refuse. A licence being expired is a fact; acting on a run in
        which EVERY licence is expired is the defect.
        """
        dead = uuid.uuid4()
        async with _client_returning([_lic(dead, "expired")]) as client:
            got = await _fetch_licence_lifecycle(_Settings(deactivate=True), None, client=client)
        assert got.active_ids == set()
        assert got.non_active_count == 1
        # The classifier's honest output -- and, on this payload, the empty
        # keep-set that `deactivate_orphan_license_sync_budgets` HONOURS.
        assert got.protected_ids == set()

        # Which is precisely why reconcile_once must not proceed with it. The
        # payload is NON-EMPTY, so the guard on `if not payload` never fires;
        # only the guard on the derived SET stops this.
        async with _client_returning([_lic(dead, "expired")]) as client:
            with pytest.raises(LicenceFetchError, match="active-or-unreadable"):
                await reconcile_once(
                    _Settings(deactivate=True), db_session_factory, None, client=client
                )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("payload", "code"),
        [
            ([], 500),
            ([], 403),
            ("not json", 200),
            ({"items": []}, 200),  # a paginated envelope
            ([{"tenant_id": "x"}], 200),  # licence with no id
            ([{"id": "not-a-uuid", "status": "active"}], 200),
            (["a string, not an object"], 200),
        ],
    )
    async def test_incomplete_read_refuses_rather_than_returning_empty(self, payload, code):
        """Every one of these would otherwise present as "zero active licences",
        which the orphan helper honours as a legitimate signal — and would
        deactivate every license_sync budget in the estate."""
        async with _client_returning(payload, code) as client:
            with pytest.raises(LicenceFetchError):
                await _fetch_licence_lifecycle(_Settings(deactivate=True), None, client=client)


class TestUnreadableStatusNeverDeactivates:
    """The fail-safe. See the module docstring."""

    @pytest.mark.unit
    async def test_unrecognised_status_is_protected_not_orphaned(self):
        future = uuid.uuid4()
        async with _client_returning([_lic(future, "suspended")]) as client:
            got = await _fetch_licence_lifecycle(_Settings(deactivate=True), None, client=client)
        assert got.active_ids == set(), "an unknown status must not be claimed active"
        assert got.protected_ids == {future}, "but its budget must NOT be deactivated"
        assert got.unreadable_count == 1

    @pytest.mark.unit
    async def test_missing_status_is_protected_not_orphaned(self):
        nostatus = uuid.uuid4()
        async with _client_returning([_lic(nostatus, None)]) as client:
            got = await _fetch_licence_lifecycle(_Settings(deactivate=True), None, client=client)
        assert got.active_ids == set()
        assert got.protected_ids == {nostatus}
        assert got.unreadable_count == 1

    @pytest.mark.unit
    async def test_unknown_status_budget_survives_a_full_reconcile(
        self, db_session, db_session_factory
    ):
        """End-to-end control: the whole point is that this budget is STILL
        active after a reconcile that was armed to deactivate."""
        future = uuid.uuid4()
        budget, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(future),
            period=BudgetPeriod.monthly,
            budget_tokens=10_000,
        )
        await db_session.commit()

        async with _client_returning([_lic(future, "trialing")]) as client:
            divergent, deactivated = await reconcile_once(
                _Settings(deactivate=True), db_session_factory, None, client=client
            )
        assert (divergent, deactivated) == (0, 0)
        await db_session.refresh(budget)
        assert budget.active is True


class TestReconcile:
    @pytest.mark.unit
    async def test_revoked_licence_budget_is_deactivated(self, db_session, db_session_factory):
        """The production defect, reproduced: a live budget behind a revoked
        licence. Fails on origin/main because nothing runs this on a timer."""
        dead = uuid.uuid4()
        budget, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(dead),
            period=BudgetPeriod.monthly,
            budget_tokens=430_000,
        )
        await db_session.commit()
        assert budget.active is True

        # A live licence rides along because a real registry always has one.
        # Without it `protected_ids` is empty and the run is refused outright
        # -- see TestRefusesEmptyDerivedSet. Deactivation is only ever
        # legitimate as a MINORITY of a registry that is otherwise alive.
        async with _client_returning([_lic(uuid.uuid4()), _lic(dead, "revoked")]) as client:
            divergent, deactivated = await reconcile_once(
                _Settings(deactivate=True), db_session_factory, None, client=client
            )

        assert divergent == 1, "measured BEFORE the repair, so the series shows it happening"
        assert deactivated == 1
        await db_session.refresh(budget)
        assert budget.active is False

    @pytest.mark.unit
    async def test_hard_deleted_licence_budget_is_deactivated(self, db_session, db_session_factory):
        """delete_user_licenses HARD-deletes the licence row, so the licence is
        absent from the registry entirely rather than revoked. A status-based
        check would find nothing to compare; the set difference catches it."""
        gone, live = uuid.uuid4(), uuid.uuid4()
        orphan, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(gone),
            period=BudgetPeriod.monthly,
            budget_tokens=1_000,
        )
        kept, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(live),
            period=BudgetPeriod.monthly,
            budget_tokens=1_000,
        )
        await db_session.commit()

        async with _client_returning([_lic(live)]) as client:
            _, deactivated = await reconcile_once(
                _Settings(deactivate=True), db_session_factory, None, client=client
            )

        assert deactivated == 1
        await db_session.refresh(orphan)
        await db_session.refresh(kept)
        assert orphan.active is False
        assert kept.active is True, "a live licence's budget must be untouched"

    @pytest.mark.unit
    async def test_measure_only_mode_reports_without_repairing(
        self, db_session, db_session_factory
    ):
        """The shipped default. Detection must be useful on the merge commit,
        and must NOT quietly mutate before an operator arms it."""
        dead = uuid.uuid4()
        budget, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(dead),
            period=BudgetPeriod.monthly,
            budget_tokens=1_000,
        )
        await db_session.commit()

        async with _client_returning([_lic(uuid.uuid4()), _lic(dead, "revoked")]) as client:
            divergent, deactivated = await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert divergent == 1, "it must still MEASURE"
        assert deactivated == 0, "and must not repair"
        await db_session.refresh(budget)
        assert budget.active is True

    @pytest.mark.unit
    async def test_is_idempotent(self, db_session, db_session_factory):
        dead = uuid.uuid4()
        await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(dead),
            period=BudgetPeriod.monthly,
            budget_tokens=1_000,
        )
        await db_session.commit()

        settings = _Settings(deactivate=True)
        async with _client_returning([_lic(uuid.uuid4()), _lic(dead, "revoked")]) as client:
            first = await reconcile_once(settings, db_session_factory, None, client=client)
            second = await reconcile_once(settings, db_session_factory, None, client=client)

        assert first == (1, 1)
        assert second == (0, 0), "a second pass must find nothing left to do"

    @pytest.mark.unit
    async def test_manual_budget_is_never_deactivated(self, db_session, db_session_factory):
        """A hand-granted budget is not backed by a licence and must survive.
        This is the documented gap: widening the reconcile past license_sync
        would delete credit no licence ever granted."""
        from services.budget_crud import create_budget

        dead = uuid.uuid4()
        manual = await create_budget(
            tenant_id=T("t1"),
            budget_tokens=99_999,
            period="monthly",
            license_id=dead,
            source="manual",
            session=db_session,
        )
        await db_session.commit()

        async with _client_returning([]) as client:
            with pytest.raises(LicenceFetchError):
                # An empty registry is refused outright, so prove the point
                # with a registry that is merely missing this licence.
                await reconcile_once(
                    _Settings(deactivate=True), db_session_factory, None, client=client
                )

        async with _client_returning([_lic(uuid.uuid4())]) as client:
            _, deactivated = await reconcile_once(
                _Settings(deactivate=True), db_session_factory, None, client=client
            )
        assert deactivated == 0
        await db_session.refresh(manual)
        assert manual.active is True

    @pytest.mark.unit
    async def test_unreachable_registry_mutates_nothing(self, db_session, db_session_factory):
        dead = uuid.uuid4()
        budget, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(dead),
            period=BudgetPeriod.monthly,
            budget_tokens=1_000,
        )
        await db_session.commit()

        def boom(_request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("licences down")

        async with httpx.AsyncClient(transport=httpx.MockTransport(boom)) as client:
            with pytest.raises(LicenceFetchError):
                await reconcile_once(
                    _Settings(deactivate=True), db_session_factory, None, client=client
                )

        await db_session.refresh(budget)
        assert budget.active is True, "a paused repair, never a destructive one"


class TestRefusesEmptyDerivedSet:
    """THE ESTATE-WIPE. Reproduced on a scratch database before the guard
    existed: 3 live ``license_sync`` budgets, a 200 carrying
    ``[{revoked}, {expired}]`` -> ``divergent=3, deactivated=3``, every budget
    in the estate deactivated in a single 900s tick.

    The payload is NON-EMPTY, so ``if not payload`` never fires. What is empty
    is the DERIVED set, and ``deactivate_orphan_license_sync_budgets`` honours
    an empty keep-set by design -- its docstring says "the safety gate lives at
    the call site, not here". These tests are that gate.
    """

    @pytest.mark.unit
    async def test_nonempty_payload_of_all_non_active_refuses(self, db_session, db_session_factory):
        """The auditor's exact reproduction."""
        budgets = []
        for _ in range(3):
            budget, _ = await upsert_license_sync_budget(
                db_session,
                tenant_id=T("t1"),
                license_id=str(uuid.uuid4()),
                period=BudgetPeriod.monthly,
                budget_tokens=10_000,
            )
            budgets.append(budget)
        await db_session.commit()

        payload = [_lic(uuid.uuid4(), "revoked"), _lic(uuid.uuid4(), "expired")]
        assert payload, "the payload guard must NOT be what saves us here"

        async with _client_returning(payload) as client:
            with pytest.raises(LicenceFetchError, match="active-or-unreadable"):
                await reconcile_once(
                    _Settings(deactivate=True), db_session_factory, None, client=client
                )

        for budget in budgets:
            await db_session.refresh(budget)
            assert budget.active is True, "the estate must survive"

    @pytest.mark.unit
    async def test_refusal_message_names_which_quantity_was_empty(self, db_session_factory):
        """Two different refusals both raise LicenceFetchError so the loop's
        existing counter/handover wiring fires unchanged. The MESSAGE is what
        keeps them apart in logs -- without it an operator cannot tell "the
        registry returned []" from "the registry returned rows, none alive",
        which have completely different upstream causes."""
        async with _client_returning([]) as client:
            with pytest.raises(LicenceFetchError, match="zero licences"):
                await reconcile_once(
                    _Settings(deactivate=True), db_session_factory, None, client=client
                )

        async with _client_returning([_lic(uuid.uuid4(), "revoked")]) as client:
            with pytest.raises(LicenceFetchError, match="active-or-unreadable"):
                await reconcile_once(
                    _Settings(deactivate=True), db_session_factory, None, client=client
                )

    @pytest.mark.unit
    async def test_measure_only_mode_refuses_too(self, db_session, db_session_factory):
        """Measure-only is not a licence to publish a fabricated number. With
        an empty keep-set the divergence count would read as the whole estate
        and page for a fault that does not exist."""
        budget, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(uuid.uuid4()),
            period=BudgetPeriod.monthly,
            budget_tokens=1_000,
        )
        await db_session.commit()

        async with _client_returning([_lic(uuid.uuid4(), "revoked")]) as client:
            with pytest.raises(LicenceFetchError, match="active-or-unreadable"):
                await reconcile_once(
                    _Settings(deactivate=False), db_session_factory, None, client=client
                )
        await db_session.refresh(budget)
        assert budget.active is True

    @pytest.mark.unit
    async def test_a_single_unreadable_licence_is_enough_to_proceed(
        self, db_session, db_session_factory
    ):
        """The guard must gate on protected_ids, NOT on active_ids. An estate
        whose every licence carries a status this service cannot parse has
        active_ids == set() but is perfectly readable: those licences are
        protected, so nothing is orphaned and the cycle is safe to run.
        Gating on active_ids would refuse this legitimate run forever."""
        budget, _ = await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(uuid.uuid4()),
            period=BudgetPeriod.monthly,
            budget_tokens=1_000,
        )
        await db_session.commit()

        async with _client_returning([_lic(uuid.uuid4(), "suspended")]) as client:
            divergent, deactivated = await reconcile_once(
                _Settings(deactivate=True), db_session_factory, None, client=client
            )
        assert deactivated == 1, "the orphan is still repaired"
        assert divergent == 1


class TestDeactivationCap:
    """A blast-radius bound for the shapes nobody has thought of yet. The
    emptiness guards refuse the KNOWN mass-deactivation inputs; this catches
    any future upstream change that makes a large slice of the registry read as
    non-active at once while still leaving SOMETHING alive."""

    async def _seed(self, db_session, count: int) -> list:
        budgets = []
        for _ in range(count):
            budget, _ = await upsert_license_sync_budget(
                db_session,
                tenant_id=T("t1"),
                license_id=str(uuid.uuid4()),
                period=BudgetPeriod.monthly,
                budget_tokens=1_000,
            )
            budgets.append(budget)
        await db_session.commit()
        return budgets

    @pytest.mark.unit
    async def test_over_cap_deactivates_nothing_at_all(self, db_session, db_session_factory):
        """ALL-OR-NOTHING. A partial repair would still reach the whole estate,
        just spread over enough ticks that the evidence is scattered."""
        budgets = await self._seed(db_session, 4)

        async with _client_returning([_lic(uuid.uuid4())]) as client:
            divergent, deactivated = await reconcile_once(
                _Settings(deactivate=True, max_deactivations=3),
                db_session_factory,
                None,
                client=client,
            )

        assert divergent == 4, "it must still MEASURE the real number"
        assert deactivated == 0, "and repair none of it"
        for budget in budgets:
            await db_session.refresh(budget)
            assert budget.active is True

    @pytest.mark.unit
    async def test_at_cap_still_repairs(self, db_session, db_session_factory):
        """The boundary. `divergent > cap` refuses; `divergent == cap` is the
        largest run that is still allowed, so an operator who sets the ceiling
        to the measured backlog can clear exactly that backlog."""
        budgets = await self._seed(db_session, 3)

        async with _client_returning([_lic(uuid.uuid4())]) as client:
            divergent, deactivated = await reconcile_once(
                _Settings(deactivate=True, max_deactivations=3),
                db_session_factory,
                None,
                client=client,
            )

        assert (divergent, deactivated) == (3, 3)
        for budget in budgets:
            await db_session.refresh(budget)
            assert budget.active is False

    @pytest.mark.unit
    async def test_capped_cycle_counts_a_failure_and_does_not_advance_freshness(
        self, db_session, db_session_factory
    ):
        """A capped cycle did not converge. If it advanced the freshness gauge
        the loop would look healthy while repairing nothing, forever -- so the
        gauge must NOT move, and the counter must fire on a reason that has a
        birth-loop entry (an unborn series reads as a measured zero to any
        alert written over it)."""
        from observability import metrics as observability_metrics

        await self._seed(db_session, 4)

        counter = observability_metrics.licence_budget_reconcile_failures_total.labels(
            reason="deactivation_cap_exceeded"
        )
        before = counter._value.get()
        freshness = observability_metrics.licence_budget_reconcile_last_success_timestamp_seconds
        freshness.set(1234.0)

        async with _client_returning([_lic(uuid.uuid4())]) as client:
            await reconcile_once(
                _Settings(deactivate=True, max_deactivations=3),
                db_session_factory,
                None,
                client=client,
            )

        assert counter._value.get() == before + 1
        assert freshness._value.get() == 1234.0, "a capped cycle must age out, not look fresh"

    @pytest.mark.unit
    async def test_cap_reason_has_a_birth_loop_entry(self):
        """The reason label must exist as a series at import time. Without it
        `increase(...{reason="deactivation_cap_exceeded"}[6h])` returns an
        empty vector on a pod that has never tripped the cap, and an empty
        vector is indistinguishable from a measured zero.

        ⚠️ CORRECTED: this asserted only that the label appears in
        `generate_latest(REGISTRY)`, and
        `test_capped_cycle_counts_a_failure_and_does_not_advance_freshness`
        above increments this exact reason before it runs -- an increment
        births the series by itself, so the assertion passed with the birth
        loop deleted. Measured on the sibling reason: 44/44 green with the
        entry removed. It asserts the birth LIST now; the exported-series
        checks are kept, but they can only confirm, never fail.
        """
        from prometheus_client import REGISTRY, generate_latest

        from observability import metrics as observability_metrics

        assert (
            "deactivation_cap_exceeded"
            in observability_metrics.LICENCE_BUDGET_RECONCILE_FAILURE_REASONS
        )

        exported = generate_latest(REGISTRY).decode()
        assert "billing_licence_budget_reconcile_failures_total" in exported, (
            "precondition: the counter itself is exported"
        )
        assert 'reason="deactivation_cap_exceeded"' in exported

    @pytest.mark.unit
    async def test_default_cap_clears_the_measured_production_backlog(self):
        """The default must be able to repair the 19-row backlog measured on
        production 2026-09-10 without a human raising it first, or the loop
        ships unable to do the job it was written for."""
        from settings import Settings

        field = Settings.model_fields["licence_budget_reconcile_max_deactivations_per_cycle"]
        assert field.default >= 19


class TestMeasureDivergence:
    @pytest.mark.unit
    async def test_counts_only_active_license_sync_rows(self, db_session):
        dead, live = uuid.uuid4(), uuid.uuid4()
        await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(dead),
            period=BudgetPeriod.monthly,
            budget_tokens=1,
        )
        await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(live),
            period=BudgetPeriod.monthly,
            budget_tokens=1,
        )
        assert await measure_divergence(db_session, protected_ids={live}) == 1
        assert await measure_divergence(db_session, protected_ids={live, dead}) == 0


class TestDivergenceGaugeIsPublished:
    """⭐ THE SECOND AUDIT'S HEADLINE SURVIVOR, and it is the whole PR.

    ``licence_budget_reconcile_deactivate_enabled`` ships FALSE, so on the
    merge commit the ONLY production effect of this module is publishing
    ``billing_licence_budget_divergent_total``. Every test above asserts the
    RETURN VALUE of ``reconcile_once``; not one asserted the gauge. Measured:
    mutating ``.set(divergent)`` to ``.set(0)`` left **30/30 green**, i.e. the
    PR's sole shipped behaviour was the one thing the suite could not fail for.

    These read the value back OUT OF THE REGISTRY by metric name rather than
    off the object, so they also fail if the series is renamed -- a rename is
    invisible to ``._value.get()`` and fatal to every alert written over it.
    """

    @staticmethod
    def _published(name: str) -> float | None:
        from prometheus_client import REGISTRY

        return REGISTRY.get_sample_value(name)

    @pytest.mark.unit
    async def test_published_gauge_carries_the_measured_count(self, db_session, db_session_factory):
        """KILLS ``.set(divergent)`` -> ``.set(0)``.

        Two divergent budgets, measure-only mode (the shipped default), so
        nothing is repaired and the gauge must read the real number a human
        would act on.
        """
        dead_a, dead_b = uuid.uuid4(), uuid.uuid4()
        for dead in (dead_a, dead_b):
            await upsert_license_sync_budget(
                db_session,
                tenant_id=T("t1"),
                license_id=str(dead),
                period=BudgetPeriod.monthly,
                budget_tokens=1_000,
            )
        await db_session.commit()

        # Deliberately parked on a WRONG non-zero value first: a test that only
        # asserts `== 2` would also pass against a mutant that never calls
        # .set() at all, if some earlier test happened to leave a 2 behind.
        from observability import metrics as observability_metrics

        observability_metrics.licence_budget_divergent_total.set(999)

        payload = [_lic(uuid.uuid4()), _lic(dead_a, "revoked"), _lic(dead_b, "revoked")]
        async with _client_returning(payload) as client:
            divergent, deactivated = await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert (divergent, deactivated) == (2, 0), "precondition: measure-only found both"
        assert self._published("billing_licence_budget_divergent_total") == 2.0, (
            "the gauge is the PR's only shipped effect; it must carry the "
            "measured count, not a constant"
        )

    @pytest.mark.unit
    async def test_gauge_falls_back_to_zero_once_the_estate_converges(self, db_session_factory):
        """The other half of the same mutant. ``.set(0)`` would pass a test
        that only ever checks the healthy case, so the pair -- non-zero when
        divergent, zero when clean -- is what pins the value to the
        measurement rather than to either constant."""
        from observability import metrics as observability_metrics

        observability_metrics.licence_budget_divergent_total.set(17)

        async with _client_returning([_lic(uuid.uuid4())]) as client:
            divergent, _ = await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert divergent == 0
        assert self._published("billing_licence_budget_divergent_total") == 0.0

    @pytest.mark.unit
    async def test_a_zero_is_only_readable_beside_a_fresh_timestamp(self, db_session_factory):
        """⚠️ THE 0-AMBIGUITY, pinned rather than merely documented.

        0 is both "nothing has diverged" and prometheus_client's import-time
        default on a replica whose loop has never run, so the divergence gauge
        ALONE cannot distinguish a healthy estate from one nobody is watching.
        The freshness gauge is what separates them, and it only separates them
        if the SAME cycle that publishes the 0 also advances it.

        This test fails if anyone splits the two -- moves the freshness write
        off the measure-only path, or returns early before it. The alert in
        gitops-production is written as
        ``... and time() - billing_licence_budget_reconcile_last_success_
        timestamp_seconds < N``, and that AND is load-bearing.
        """
        from observability import metrics as observability_metrics

        freshness = observability_metrics.licence_budget_reconcile_last_success_timestamp_seconds
        freshness.set(0)
        observability_metrics.licence_budget_divergent_total.set(17)

        async with _client_returning([_lic(uuid.uuid4())]) as client:
            await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert self._published("billing_licence_budget_divergent_total") == 0.0
        published_freshness = self._published(
            "billing_licence_budget_reconcile_last_success_timestamp_seconds"
        )
        assert published_freshness is not None and published_freshness > 0, (
            "a 0 on the divergence gauge is only readable next to a freshness "
            "timestamp the same cycle advanced"
        )


class TestUnreadableGaugeIsPublished:
    """⭐ THE SIBLING THE DIVERGENCE FIX WAS NOT APPLIED TO.

    ``TestDivergenceGaugeIsPublished`` above was written because mutating
    ``licence_budget_divergent_total.set(divergent)`` to ``.set(0)`` left the
    suite green. That fix was made on ONE gauge and not on its twin, two lines
    below it in the same function: mutating
    ``licence_budget_reconcile_unreadable_total.set(lifecycle.unreadable_count)``
    to ``.set(0)`` measured **44 passed / 0 failed**, and
    ``grep unreadable_total tests/test_licence_budget_reconcile.py`` returned
    zero hits. The mutant is not a dud -- ``TestUnreadableStatusNeverDeactivates``
    already drives ``reconcile_once`` with a ``"suspended"`` licence, so the
    mutated line is on an executed path; nothing was reading what it wrote.

    This gauge is the one the module docstring nominates by name (see
    ``licence_budget_reconcile.py`` lines 51-61): a licence whose status this
    service cannot parse is deliberately PROTECTED rather than deactivated, so
    the day ``licenses`` grows a ``'trialing'`` status every budget behind it
    is silently kept alive and NOTHING ELSE says so. That is the whole point of
    the fail-safe -- "upstream's vocabulary moved" is supposed to surface as a
    metric instead of as missing money -- and this series is the only place it
    surfaces. Pinned at 0 it reports a perfectly-understood registry forever.

    Read back OUT OF THE REGISTRY by metric NAME, exactly as the divergence
    tests are, rather than off the object: ``._value.get()`` cannot see a
    rename, and a rename is fatal to every alert written over the series.
    """

    @staticmethod
    def _published(name: str) -> float | None:
        from prometheus_client import REGISTRY

        return REGISTRY.get_sample_value(name)

    @pytest.mark.unit
    async def test_published_gauge_carries_the_count_of_licences_we_could_not_read(
        self, db_session_factory
    ):
        """KILLS ``.set(lifecycle.unreadable_count)`` -> ``.set(0)``.

        Both unreadable SHAPES at once, because they are two separate branches
        in ``_fetch_licence_lifecycle`` and each increments the counter: one
        licence with a status the service does not recognise, and one with the
        ``status`` key ABSENT. Two, not one, so the assertion also fails a
        mutant that publishes a constant 1 or ``len(protected_ids)`` (3 here).

        Measure-only mode -- the shipped default -- so this is the gauge as it
        actually reaches production on the merge commit.
        """
        from observability import metrics as observability_metrics

        # Parked on a WRONG non-zero value first, per the convention the
        # divergence tests above set: without this, a mutant that never calls
        # .set() at all could ride a 2 some earlier test left behind.
        observability_metrics.licence_budget_reconcile_unreadable_total.set(999)

        payload = [
            _lic(uuid.uuid4()),  # active: readable, protects itself
            _lic(uuid.uuid4(), "trialing"),  # unrecognised status -> unreadable
            _lic(uuid.uuid4(), None),  # status key absent -> unreadable
        ]
        async with _client_returning(payload) as client:
            divergent, deactivated = await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert (divergent, deactivated) == (0, 0), "precondition: clean estate, measure-only"
        assert self._published("billing_licence_budget_reconcile_unreadable_total") == 2.0, (
            "the only signal that upstream's vocabulary moved must carry the "
            "number of licences that moved, not a constant"
        )

    @pytest.mark.unit
    async def test_gauge_falls_back_to_zero_once_every_status_is_understood(
        self, db_session_factory
    ):
        """The other half of the same mutant, and the reason the pair is
        needed: ``.set(0)`` passes any test that only ever checks a registry
        this service understands completely. Non-zero when we cannot read the
        registry, zero when we can, is what pins the published value to the
        measurement rather than to either constant."""
        from observability import metrics as observability_metrics

        observability_metrics.licence_budget_reconcile_unreadable_total.set(23)

        async with _client_returning([_lic(uuid.uuid4()), _lic(uuid.uuid4(), "revoked")]) as client:
            divergent, _ = await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert divergent == 0
        assert self._published("billing_licence_budget_reconcile_unreadable_total") == 0.0, (
            "every status in this payload is one the service understands, so a "
            "stale non-zero here would report a vocabulary drift that ended"
        )


class _FakeElection:
    """Stands in for ``AsyncLeaderElection`` so the loop's own logic -- leader
    gating, reason routing, handover -- is what is under test rather than
    Postgres advisory-lock behaviour, which ``test_leader_election`` already
    covers."""

    instances: list[_FakeElection] = []

    def __init__(self, *_args, leader: bool = True, **_kwargs) -> None:
        self.leader = leader
        self.releases = 0
        _FakeElection.instances.append(self)

    async def is_leader(self) -> bool:
        return self.leader

    async def release(self) -> None:
        self.releases += 1


class _LoopSettings:
    """Only the fields ``run_licence_budget_reconcile_loop`` reads."""

    licenses_url = "http://licences.invalid"

    def __init__(self, *, deactivate: bool = False, max_deactivations: int = 50) -> None:
        self.licence_budget_reconcile_deactivate_enabled = deactivate
        self.licence_budget_reconcile_max_deactivations_per_cycle = max_deactivations
        # Sub-millisecond so the loop's `asyncio.wait_for(..., timeout=...)`
        # returns immediately and a test costs a few ms rather than a tick.
        self.licence_budget_reconcile_interval_seconds = 0.001
        self.leader_retry_interval_seconds = 0.001

    def resolve_leader_election_dsn(self) -> str:
        return "postgresql://unused.invalid/none"


async def _noop_cycle(_n):
    """An awaitable cycle that does nothing and reports no divergence."""
    return (0, 0)


async def _run_loop_for(monkeypatch, *, cycles: int, on_cycle, leader: bool = True):
    """Drive the real loop for exactly ``cycles`` reconcile attempts.

    ``on_cycle`` replaces ``reconcile_once``; the loop around it -- lines 439-513,
    reported 0% covered in the PR body -- is the real production code.
    """
    from services import licence_budget_reconcile as lbr

    _FakeElection.instances.clear()
    monkeypatch.setattr(
        lbr,
        "AsyncLeaderElection",
        lambda *a, **k: _FakeElection(*a, leader=leader, **k),
    )

    stop_event = asyncio.Event()
    calls: list[int] = []

    async def _fake_reconcile(*args, **kwargs):
        calls.append(len(calls))
        try:
            return await on_cycle(len(calls) - 1)
        finally:
            if len(calls) >= cycles:
                stop_event.set()

    monkeypatch.setattr(lbr, "reconcile_once", _fake_reconcile)

    if not leader:
        # Nothing will ever call _fake_reconcile, so arm the stop from outside.
        async def _stopper():
            await asyncio.sleep(0.05)
            stop_event.set()

        asyncio.get_running_loop().create_task(_stopper())

    await asyncio.wait_for(
        lbr.run_licence_budget_reconcile_loop(
            cast(Settings, _LoopSettings()),
            cast(async_sessionmaker[AsyncSession], None),
            None,
            stop_event,
        ),
        timeout=15,
    )
    return calls


class TestLoopFilesTheRightReason:
    """⭐ THE MISFILED REFUSAL, and the first tests to execute
    ``run_licence_budget_reconcile_loop`` at all.

    An all-revoked registry reads PERFECTLY and yields an empty derived
    keep-set. The module refuses -- correctly -- but until this commit the loop
    filed that refusal as ``reason="licences_unreadable"``, whose own metric
    docstring defines it as "the registry could not be read completely". It
    was. An operator following the label debugs a healthy licences service
    while the real condition goes unnamed, the repair never converges, and
    leadership churns every ``MAX_CONSECUTIVE_RECONCILE_FAILURES`` ticks.
    """

    @staticmethod
    def _reason(name: str) -> float:
        from observability import metrics as observability_metrics

        return observability_metrics.licence_budget_reconcile_failures_total.labels(
            reason=name
        )._value.get()

    @pytest.mark.unit
    async def test_all_revoked_registry_files_no_active_licences_not_unreadable(
        self, monkeypatch, db_session, db_session_factory
    ):
        """End-to-end through the REAL ``reconcile_once``: the only thing
        injected is the HTTP transport. An estate whose every licence is
        revoked must be filed under its own reason."""
        from services import licence_budget_reconcile as lbr

        await upsert_license_sync_budget(
            db_session,
            tenant_id=T("t1"),
            license_id=str(uuid.uuid4()),
            period=BudgetPeriod.monthly,
            budget_tokens=1_000,
        )
        await db_session.commit()

        before_named = self._reason("no_active_licences")
        before_unreadable = self._reason("licences_unreadable")

        real_reconcile = lbr.reconcile_once
        payload = [_lic(uuid.uuid4(), "revoked"), _lic(uuid.uuid4(), "expired")]

        async def _cycle(_n):
            async with _client_returning(payload) as client:
                return await real_reconcile(
                    _Settings(deactivate=True), db_session_factory, None, client=client
                )

        await _run_loop_for(monkeypatch, cycles=1, on_cycle=_cycle)

        assert self._reason("no_active_licences") == before_named + 1, (
            "the refusal must be filed under the condition that actually occurred"
        )
        assert self._reason("licences_unreadable") == before_unreadable, (
            "and must NOT be filed as a registry that could not be read -- it "
            "read perfectly; that label sends the operator to the wrong service"
        )

    @pytest.mark.unit
    async def test_a_genuinely_unreadable_registry_still_files_licences_unreadable(
        self, monkeypatch
    ):
        """The control on the split. Narrowing one reason is only a fix if the
        other reason still fires for its own case -- otherwise this trades a
        wrong label for a missing one."""
        from services import licence_budget_reconcile as lbr

        before_named = self._reason("no_active_licences")
        before_unreadable = self._reason("licences_unreadable")

        async def _cycle(_n):
            raise lbr.LicenceFetchError("licences service returned 503")

        await _run_loop_for(monkeypatch, cycles=1, on_cycle=_cycle)

        assert self._reason("licences_unreadable") == before_unreadable + 1
        assert self._reason("no_active_licences") == before_named

    @pytest.mark.unit
    async def test_the_new_reason_has_a_birth_loop_entry(self):
        """A labelled Counter exports NO series until its first
        ``.labels().inc()``, and an empty vector reads as a measured zero to
        any alert written over it. The cap reason got a birth-loop entry; this
        one must too, or an alert on it is vacuous on every pod that has never
        seen an all-revoked registry -- which is every healthy pod.

        ⚠️ THIS ASSERTS THE BIRTH LIST, NOT THE EXPORTED SERIES, AND THAT
        DISTINCTION IS THE TEST. Scanning ``generate_latest(REGISTRY)`` for
        ``reason="no_active_licences"`` is what this test did first, and it was
        VACUOUS: the routing tests above increment that very reason, and an
        increment births the series by itself. Measured -- deleting the reason
        from the birth loop left the whole file 44/44 GREEN. The exported-series
        check is kept below as a second assertion, but it can only ever confirm;
        the constant is what can fail.
        """
        from prometheus_client import REGISTRY, generate_latest

        from observability import metrics as observability_metrics

        assert (
            "no_active_licences" in observability_metrics.LICENCE_BUDGET_RECONCILE_FAILURE_REASONS
        ), "the reason must be BORN at import, not merely reachable at runtime"

        exported = generate_latest(REGISTRY).decode()
        for reason in observability_metrics.LICENCE_BUDGET_RECONCILE_FAILURE_REASONS:
            assert f'reason="{reason}"' in exported, (
                f"{reason} is in the birth list but was never exported"
            )

    @pytest.mark.unit
    async def test_the_refusal_is_still_a_licencefetcherror(self):
        """⚠️ The fail-safe must not depend on anyone remembering the new type.
        Every existing caller refuses to mutate on ``LicenceFetchError``; if
        the split made this a sibling rather than a subclass, those callers
        would stop refusing and the estate-wipe guard would be reopened by the
        very commit that improved its label."""
        from services import licence_budget_reconcile as lbr

        assert issubclass(lbr.NoActiveLicencesError, lbr.LicenceFetchError)

    @pytest.mark.unit
    async def test_reason_routing_survives_except_arm_reordering(
        self, monkeypatch, db_session_factory
    ):
        """``NoActiveLicencesError`` is a SUBCLASS, so Python's source-order
        except matching is what keeps the two arms apart. Moving the
        ``LicenceFetchError`` arm above it silently restores the misfiling with
        no other symptom. This is the test that fails when someone does."""
        from services import licence_budget_reconcile as lbr

        before_named = self._reason("no_active_licences")

        async def _cycle(_n):
            raise lbr.NoActiveLicencesError("no active-or-unreadable licences")

        await _run_loop_for(monkeypatch, cycles=1, on_cycle=_cycle)

        assert self._reason("no_active_licences") == before_named + 1


class TestLoopLeadershipAndHandover:
    """The rest of lines 439-513, declared 0% covered in the PR body."""

    @pytest.mark.unit
    async def test_a_follower_never_reconciles_and_publishes_is_leader_zero(self, monkeypatch):
        from observability import metrics as observability_metrics

        observability_metrics.licence_budget_reconcile_is_leader.set(1)

        calls = await _run_loop_for(monkeypatch, cycles=1, on_cycle=_noop_cycle, leader=False)

        assert calls == [], "a follower must not fetch the registry or mutate anything"
        assert observability_metrics.licence_budget_reconcile_is_leader._value.get() == 0

    @pytest.mark.unit
    async def test_leadership_is_handed_back_after_max_consecutive_failures(self, monkeypatch):
        """A wedged replica that keeps failing must release the lease so a
        healthy one can reconcile. Without this the loop holds leadership and
        repairs nothing for the process lifetime."""
        from services import licence_budget_reconcile as lbr

        async def _cycle(_n):
            raise lbr.LicenceFetchError("still down")

        await _run_loop_for(
            monkeypatch, cycles=lbr.MAX_CONSECUTIVE_RECONCILE_FAILURES, on_cycle=_cycle
        )

        election = _FakeElection.instances[-1]
        # One release from the handover, one from the `finally` on shutdown.
        assert election.releases >= 2, (
            "the lease must be handed back at "
            f"{lbr.MAX_CONSECUTIVE_RECONCILE_FAILURES} consecutive failures"
        )

    @pytest.mark.unit
    async def test_a_success_resets_the_failure_streak(self, monkeypatch):
        """Otherwise unrelated failures spread over hours accumulate into a
        handover that nothing in the current window justifies."""
        from services import licence_budget_reconcile as lbr

        n_max = lbr.MAX_CONSECUTIVE_RECONCILE_FAILURES

        async def _cycle(n):
            # Fail up to one short of the threshold, succeed, then fail again
            # the same number of times. Never `n_max` in a row.
            if n == n_max - 1:
                return (0, 0)
            raise lbr.LicenceFetchError("intermittent")

        await _run_loop_for(monkeypatch, cycles=2 * n_max - 2, on_cycle=_cycle)

        election = _FakeElection.instances[-1]
        assert election.releases == 1, (
            "only the shutdown `finally` should have released; a success in the "
            "middle must reset the streak"
        )

    @pytest.mark.unit
    async def test_the_loop_always_releases_on_shutdown(self, monkeypatch):
        from observability import metrics as observability_metrics

        await _run_loop_for(monkeypatch, cycles=1, on_cycle=_noop_cycle)

        assert _FakeElection.instances[-1].releases == 1
        assert observability_metrics.licence_budget_reconcile_is_leader._value.get() == 0

    @pytest.mark.unit
    async def test_a_database_error_files_infra_error_not_a_licences_reason(self, monkeypatch):
        """The remaining arms of the same routing. A DB fault must not be
        filed against the licences service either -- same failure shape as the
        one this commit fixed, one arm further down."""
        from sqlalchemy.exc import OperationalError

        before = TestLoopFilesTheRightReason._reason("infra_error")
        before_lic = TestLoopFilesTheRightReason._reason("licences_unreadable")

        async def _cycle(_n):
            raise OperationalError("SELECT 1", {}, Exception("connection refused"))

        await _run_loop_for(monkeypatch, cycles=1, on_cycle=_cycle)

        assert TestLoopFilesTheRightReason._reason("infra_error") == before + 1
        assert TestLoopFilesTheRightReason._reason("licences_unreadable") == before_lic

    @pytest.mark.unit
    async def test_an_unmodelled_error_files_unexpected_and_the_loop_survives(self, monkeypatch):
        """One bad cycle must cost one cycle, not the loop for the process
        lifetime -- a loop that dies on an unmodelled exception stops
        reconciling silently, which is the failure this whole module exists to
        prevent."""
        before = TestLoopFilesTheRightReason._reason("unexpected")

        async def _cycle(n):
            if n == 0:
                raise ValueError("something nobody modelled")
            return (0, 0)

        calls = await _run_loop_for(monkeypatch, cycles=2, on_cycle=_cycle)

        assert len(calls) == 2, "the loop must keep ticking after an unmodelled error"
        assert TestLoopFilesTheRightReason._reason("unexpected") == before + 1


# ===========================================================================
# The two `_enabled` gauges are set UNCONDITIONALLY in main.lifespan, OUTSIDE
# the `if settings.licence_budget_reconcile_enabled:` block that arms the loop.
# ===========================================================================


class TestLicenceBudgetReconcileEnabledGauges:
    """⭐ THE CONJUNCT THAT MAKES THE PAIRED ALERT NON-VACUOUS.

    Deleting BOTH ``_metrics.licence_budget_reconcile_enabled.set(...)`` and
    ``_metrics.licence_budget_reconcile_deactivate_enabled.set(...)`` from
    ``main.lifespan`` measured **2652 passed, 2 skipped** -- a full-suite
    survivor, with the only red being the alembic failures that reproduce
    identically on base. Nothing anywhere read either series.

    Why that is CONSEQUENTIAL rather than cosmetic: both gauges are born at
    prometheus_client's import-time default of 0 by the side-effect import in
    ``observability/metrics.py``, so a deleted ``.set()`` does not produce an
    absent series a ``absent()`` rule could catch -- it produces a confident,
    permanently-published **0**. The gitops rule
    ``BillingLicenceBudgetReconcileStalled`` ANDs on
    ``billing_licence_budget_reconcile_enabled == 1``. Measured with promtool
    against that rule: with the gauge left at its import default, a dead loop
    plus the deactivation cap tripping every minute for three hours fires
    NOTHING. The alert written to catch this module dying is silenced by the
    absence of one line in a different file.

    The precedent for pinning exactly this is ``TestInvoiceCloseEnabledGauge``
    in ``tests/test_invoice_closer.py``, and it sits over the SAME statement
    shape 24 lines further up ``main.py`` -- ``invoice_close_enabled`` got its
    test, the two lines below it did not.

    Like that precedent, this drives the REAL ``main.lifespan`` rather than a
    mock of it or an assertion against a string copied out of ``main.py``:
    ``main.init_redis`` / ``init_db`` / ``dispose_engine`` / ``close_redis``
    patched so no real Redis or Postgres connection setup is attempted,
    ``db.get_session_factory`` pointed at the test database, and
    ``main.get_settings`` patched -- ``lifespan`` calls that module-level
    function itself rather than taking settings as an argument, and it is
    ``functools.lru_cache``d process-wide, so patching it is the only way to
    control the flags it reads without clobbering what other tests cached.

    Read back by metric NAME out of the registry rather than off the object.
    The precedent uses ``._value.get()``; that cannot see a rename, and a
    rename silently breaks the very alert conjunct these tests exist to keep
    non-vacuous.

    Every test poisons both gauges to -1 (neither 0 nor 1) first, per the
    convention in ``TestIsLeaderGauge`` and ``TestDivergenceGaugeIsPublished``:
    these are process-global with no autouse reset fixture, and a mutant that
    deletes a ``.set()`` outright must not be able to pass by inheriting a
    value some earlier test left behind -- least of all the born-at-0 default,
    which is the exact value the disabled case wants.
    """

    _ENABLED = "billing_licence_budget_reconcile_enabled"
    _DEACTIVATE = "billing_licence_budget_reconcile_deactivate_enabled"

    @staticmethod
    def _poison() -> None:
        from observability import metrics as observability_metrics

        observability_metrics.licence_budget_reconcile_enabled.set(-1)
        observability_metrics.licence_budget_reconcile_deactivate_enabled.set(-1)

    @staticmethod
    async def _run_lifespan_and_read_gauges(
        settings, fake_redis, db_session_factory, monkeypatch
    ) -> dict[str, float | None]:
        from unittest.mock import AsyncMock, patch

        from prometheus_client import REGISTRY

        import main as main_module

        monkeypatch.setattr(main_module, "get_settings", lambda: settings)
        with (
            patch("main.init_redis", new_callable=AsyncMock, return_value=fake_redis),
            patch("main.init_db", new_callable=AsyncMock),
            patch("main.dispose_engine", new_callable=AsyncMock),
            patch("main.close_redis", new_callable=AsyncMock),
            patch("db.get_session_factory", return_value=db_session_factory),
        ):
            async with main_module.lifespan(main_module.app):
                # Both gauges are set synchronously inside the `if redis_conn:`
                # block before the yield, so they are already published by the
                # time control is inside this context -- no tick is needed.
                return {
                    name: REGISTRY.get_sample_value(name)
                    for name in (
                        TestLicenceBudgetReconcileEnabledGauges._ENABLED,
                        TestLicenceBudgetReconcileEnabledGauges._DEACTIVATE,
                    )
                }

    @pytest.mark.unit
    async def test_both_gauges_read_one_when_both_flags_are_armed(
        self, test_settings, fake_redis, db_session_factory, monkeypatch
    ):
        """Deleting either ``.set()`` leaves the poisoned -1 standing here.

        This is also the half that a False-only test could never provide: a
        deleted line leaves the gauge at its born-at-0 default, which is
        exactly what the disabled case asserts, so without this direction a
        fully removed line reads as a correctly-firing 0 forever.
        """
        test_settings.licence_budget_reconcile_enabled = True
        test_settings.licence_budget_reconcile_deactivate_enabled = True
        self._poison()

        published = await self._run_lifespan_and_read_gauges(
            test_settings, fake_redis, db_session_factory, monkeypatch
        )

        assert published[self._ENABLED] == 1.0, (
            "the reconcile loop was armed but "
            f"{self._ENABLED} did not read 1 -- the alert conjunct that makes "
            "BillingLicenceBudgetReconcileStalled fire is dead"
        )
        assert published[self._DEACTIVATE] == 1.0, (
            f"deactivation was armed but {self._DEACTIVATE} did not read 1"
        )

    @pytest.mark.unit
    async def test_both_gauges_read_zero_when_both_flags_are_disarmed(
        self, test_settings, fake_redis, db_session_factory, monkeypatch
    ):
        """The direction that also kills MOVING either ``.set()`` inside the
        ``if settings.licence_budget_reconcile_enabled:`` block below it.

        With the flag False that block never runs, so a moved line never
        fires and the poisoned -1 survives instead of becoming 0. A 0 on a
        disabled replica is the entire reason the lines sit outside the
        ``if``: it is what makes "deliberately disarmed" distinguishable from
        "this process never reached the wiring".
        """
        test_settings.licence_budget_reconcile_enabled = False
        test_settings.licence_budget_reconcile_deactivate_enabled = False
        self._poison()

        published = await self._run_lifespan_and_read_gauges(
            test_settings, fake_redis, db_session_factory, monkeypatch
        )

        assert published[self._ENABLED] == 0.0, (
            f"{self._ENABLED} did not read 0 on a disarmed replica -- either "
            "the .set() moved inside the `if "
            "settings.licence_budget_reconcile_enabled:` block, or it never ran"
        )
        assert published[self._DEACTIVATE] == 0.0, (
            f"{self._DEACTIVATE} did not read 0 on a disarmed replica"
        )

    @pytest.mark.unit
    async def test_each_gauge_tracks_its_own_flag_in_the_shipped_config(
        self, test_settings, fake_redis, db_session_factory, monkeypatch
    ):
        """The SHIPPED default -- ``licence_budget_reconcile_enabled`` True,
        ``licence_budget_reconcile_deactivate_enabled`` False -- and the only
        configuration in which the two gauges disagree.

        Both directions above move the two flags together, so both stay green
        against a copy-paste mutant that reads the wrong settings attribute
        for one of the gauges. This is the case that separates them, and it is
        the configuration production actually runs: measure-only. An operator
        reading a standing non-zero
        ``billing_licence_budget_divergent_total`` needs
        ``billing_licence_budget_reconcile_deactivate_enabled == 0`` to tell
        "looking but not repairing, as configured" from "the repair is
        broken", and that answer is wrong if the gauge mirrors the arm flag.
        """
        test_settings.licence_budget_reconcile_enabled = True
        test_settings.licence_budget_reconcile_deactivate_enabled = False
        self._poison()

        published = await self._run_lifespan_and_read_gauges(
            test_settings, fake_redis, db_session_factory, monkeypatch
        )

        assert published[self._ENABLED] == 1.0
        assert published[self._DEACTIVATE] == 0.0, (
            "measure-only is the shipped default; a deactivate gauge that "
            "mirrors the arm flag reports a repair that is not happening"
        )


@pytest.mark.usefixtures("db_session_factory")
class TestTheQuotaValueDriftDetectorActuallyRuns:
    """``billing_quota_config_value_drift`` left -1 for the first time.

    ⛔ THE DEFECT, and it is not a bug in the detector.
    ``services.quota.record_quota_config_value_drift`` is correct, is tested, and
    is deliberately built so it CANNOT self-grade — it takes both sides as
    arguments and holds no DB session. It had NO CALLER. The only path that ever
    reached its sibling ``record_quota_config_drift`` is
    ``routers.budget.sync_from_licenses`` (``routers/budget.py:1029``), which
    passes neither ``expected_quota_monthly`` nor ``mirror_quota_before``, so the
    value comparison stayed opted out and never executed once. The live gauge read
    **-1 on both pods** — the module-level initial in ``observability/metrics.py``,
    whose HELP says in as many words that -1 means NEVER MEASURED.

    A verifier that has never run is not a verifier, and a correct one is worse
    than a broken one, because its correctness gets quoted as coverage.

    ⭐ THE ACCEPTANCE IS THE GAUGE LEAVING -1, so every test below poisons it back
    to -1 first. That is what makes them fail on a DELETED call rather than
    passing on a value some earlier test left behind: with the call removed the
    gauge simply keeps the poison, and -1 equals neither 0 nor 1.
    """

    @staticmethod
    def _poison() -> None:
        from observability import metrics as observability_metrics

        observability_metrics.quota_config_value_drift.set(-1)

    @staticmethod
    def _gauge() -> float:
        from observability import metrics as observability_metrics

        return observability_metrics.quota_config_value_drift._value.get()

    @staticmethod
    def _lic_with_quota(lic_id: uuid.UUID, quota: int | None, status: str = "active") -> dict:
        row: dict = {"id": str(lic_id), "tenant_id": T("t1"), "status": status}
        row["plan"] = {} if quota is None else {"quota_monthly": quota}
        return row

    @staticmethod
    async def _mirror(session_factory, lic_id: uuid.UUID, quota: int) -> None:
        """Write the BILLING-side half: one request_quota_config row."""
        from services.quota import upsert_request_quota_config

        async with session_factory() as session:
            await upsert_request_quota_config(
                session, lic_id, T("t1"), quota, app_id=None, app_name=None
            )
            await session.commit()

    @pytest.mark.unit
    async def test_a_clobbered_limit_takes_the_gauge_off_minus_one(self, db_session_factory):
        """The exact condition the id-set gauges are blind to, end to end.

        Three writers upsert on the ``license_id`` primary key and all three
        assign ``quota_monthly`` unconditionally, so a plan limit landing on top
        of a provisioned one moves NO id: ``quota_config_orphan_total`` and
        ``quota_config_missing_total`` both stay empty while the enforced limit
        has moved. That is why three prior rounds reported "0 divergence" — a
        statement about the instrument, not the data.

        Deleting the ``_measure_quota_config_value_drift`` call from
        ``reconcile_once`` leaves the poisoned -1 standing and fails here.
        """
        lic = uuid.uuid4()
        await self._mirror(db_session_factory, lic, 1_000)
        self._poison()

        async with _client_returning([self._lic_with_quota(lic, 1_000_000)]) as client:
            await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert self._gauge() == 1, (
            "a mirrored 1,000 against an upstream plan of 1,000,000 is one "
            "divergent licence; -1 here means the detector still never ran"
        )

    @pytest.mark.unit
    async def test_an_agreeing_mirror_publishes_a_measured_zero(self, db_session_factory):
        """NEGATIVE CONTROL, and the one that makes the test above mean something.

        A detector that reported 1 unconditionally would pass the first test. It
        must also be able to say "measured, and they agree" — which is a 0, and is
        a DIFFERENT fact from the -1 it starts at.
        """
        lic = uuid.uuid4()
        await self._mirror(db_session_factory, lic, 5_000)
        self._poison()

        async with _client_returning([self._lic_with_quota(lic, 5_000)]) as client:
            await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert self._gauge() == 0

    @pytest.mark.unit
    async def test_a_plan_without_a_quota_is_omitted_not_coerced_to_zero(self, db_session_factory):
        """⛔ NEGATIVE CONTROL ON THE SCOPE RULE. ``check_quota`` reads 0 as BLOCK
        ALL, so coercing an absent/unlimited ``plan.quota_monthly`` to 0 would
        manufacture drift against every licence nobody configured a quota for —
        a detector that invents its own findings. The licence below is mirrored
        at 7,000 and its plan carries no quota at all; it must contribute NO
        divergence.

        The gauge is still expected to leave -1: a second, quota-carrying licence
        that AGREES is present so the run genuinely measures something. Without
        it this test could not tell "omitted correctly" from "never ran".
        """
        no_quota, agreeing = uuid.uuid4(), uuid.uuid4()
        await self._mirror(db_session_factory, no_quota, 7_000)
        await self._mirror(db_session_factory, agreeing, 7_000)
        self._poison()

        payload = [
            self._lic_with_quota(no_quota, None),
            self._lic_with_quota(agreeing, 7_000),
        ]
        async with _client_returning(payload) as client:
            await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert self._gauge() == 0, "an absent plan quota must not be compared as 0"

    @pytest.mark.unit
    async def test_a_run_that_measures_nothing_leaves_the_gauge_untouched(self, db_session_factory):
        """⛔ A CALLER THAT DID NOT MEASURE MUST NOT PUBLISH A REASSURING 0.

        This is ``record_quota_config_drift``'s documented opt-in contract, and it
        is what keeps -1 meaning "never measured" rather than becoming a value any
        tick can overwrite. Here no licence carries a plan quota, so there is no
        upstream half at all — and a 0 published from that state would be the
        instrument grading an empty comparison as agreement.
        """
        lic = uuid.uuid4()
        await self._mirror(db_session_factory, lic, 9_000)
        self._poison()

        async with _client_returning([self._lic_with_quota(lic, None)]) as client:
            await reconcile_once(
                _Settings(deactivate=False), db_session_factory, None, client=client
            )

        assert self._gauge() == -1

    @pytest.mark.unit
    async def test_the_fetch_carries_the_upstream_quota_off_the_same_read(self):
        """The upstream half must ride on the ONE registry read this tick made.

        There is no dblink between billing's Postgres and licences', so the only
        truthful comparison is against a genuinely-just-fetched payload; a second
        fetch would be a second point in time. A bool is rejected explicitly —
        ``isinstance(True, int)`` is True in Python, so a JSON ``true`` would
        otherwise be compared as the integer 1 against a real stored limit.
        """
        good, absent, boolish, negative = (uuid.uuid4() for _ in range(4))
        payload = [
            self._lic_with_quota(good, 250),
            self._lic_with_quota(absent, None),
            {
                "id": str(boolish),
                "tenant_id": T("t1"),
                "status": "active",
                "plan": {"quota_monthly": True},
            },
            {
                "id": str(negative),
                "tenant_id": T("t1"),
                "status": "active",
                "plan": {"quota_monthly": -5},
            },
        ]
        async with _client_returning(payload) as client:
            got = await _fetch_licence_lifecycle(_Settings(deactivate=True), None, client=client)

        assert got.plan_quota_monthly == {good: 250}

    @pytest.mark.unit
    async def test_the_upstream_quota_is_the_effective_one_override_wins(self):
        """``quota_override`` wins over ``plan.quota_monthly`` in the drift input.

        ``sync-from-licenses`` writes the effective (override-wins) quota to the
        mirror, so comparing against the bare plan value would report every
        overridden licence as drift forever. Override 0 is a real block-all
        limit, not "absent"; a None override falls back to the plan.
        """
        overridden, zeroed, none_override = (uuid.uuid4() for _ in range(3))
        payload = [
            {**self._lic_with_quota(overridden, 50_000), "quota_override": 100},
            {**self._lic_with_quota(zeroed, 50_000), "quota_override": 0},
            {**self._lic_with_quota(none_override, 300), "quota_override": None},
        ]
        async with _client_returning(payload) as client:
            got = await _fetch_licence_lifecycle(_Settings(deactivate=True), None, client=client)

        assert got.plan_quota_monthly == {overridden: 100, zeroed: 0, none_override: 300}
