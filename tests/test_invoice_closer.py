"""Tests for services.invoice_closer.run_invoice_close_loop.

Drives ONE (or a small bounded number of) tick(s) per test, never a real
timer -- same idiom tests/test_aggregator.py uses for run_rollup_loop:
``asyncio.create_task`` the loop with a short
``invoice_close_interval_seconds`` / ``leader_retry_interval_seconds``, poll
a bounded deadline for the observable the test cares about, then
``stop_event.set()`` + ``task.cancel()``.

Every test names its mutant in its docstring, per the brief.

MUTANT MEASUREMENT STATUS -- read this before quoting any "catches mutant X"
claim below as evidence:

* UNMEASURED: every mutant described in ``TestLagTimestampGauge``,
  ``TestIsLeaderGauge``, ``TestSettleWindow``, ``TestLeaderElectionDsn``,
  ``TestFailureAccounting`` and ``TestInvoiceCloseEnabledGauge``. Those
  docstrings reason from a full read of ``services/invoice_closer.py``; no
  mutant was ever applied to the source and no red/green was ever observed
  for them. A reasoned assertion is not a verifier.
* ``TestCloseOutcomeAccounting`` is the exception in KIND, not yet in
  evidence: it was written against a real, already-observed production
  defect (``close_due_periods`` returned an already-existing invoice in the
  same flat list as a freshly drawn one, so the loop counted it ``issued``
  on EVERY hourly tick -- ~650 phantom ``issued`` per tenant per month).
  The defect is measured; whether THIS test would have failed against that
  old code is, as of this edit, still unmeasured by the author of this file.

MEASURED RESULT -- 2026-09-08, and it covers ``TestCloseOutcomeAccounting``
AND ``TestVoidedInvoiceIsNotCoverage``, AND NOTHING ELSE. Each mutant below
was genuinely APPLIED to ``services/invoice_closer.py`` and run; these are
observed reds, not reasoned ones:

* Counting ``outcome.unchanged`` as ``issued`` killed
  ``test_all_unchanged_sweep_does_not_move_issued`` and
  ``test_mixed_outcome_splits_across_all_three_series_in_one_tick``.
* Disabling the ``outcome.refused`` increment killed
  ``test_refused_and_failed_land_on_their_own_reasons`` and, again, the mixed
  test.
* The unmutated control was 3 passed.

A third mutant was applied and measured the same day, covering
``TestVoidedInvoiceIsNotCoverage``: deleting
``BillingInvoice.status.in_(("draft", "issued"))`` from the ``closed_tenants``
query killed ``test_voided_period_is_counted_as_lag`` while
``test_issued_period_is_not_counted_as_lag`` stayed GREEN. That asymmetry is
the evidence -- it pins the red to the status filter rather than to the
fixture. The unmutated control was 2 passed.

⛔ That is the whole of what was measured. Every mutant in the UNMEASURED list
above is STILL unmeasured -- this run did not touch them, and nothing here
licenses quoting one of those docstrings as evidence.
"""

import asyncio
import contextlib
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from models.db import BillingInvoice, CostDaily
from observability import metrics as observability_metrics
from services.invoice_closer import _measure_invoice_close_lag, run_invoice_close_loop
from services.invoices import CloseDueOutcome
from settings import Settings
from tests.conftest import TEST_DATABASE_URL, T


class _AlwaysLeader:
    """Stand-in AsyncLeaderElection that always answers "you are the leader"."""

    def __init__(self, *_args, **_kwargs):
        pass

    async def is_leader(self) -> bool:
        return True

    async def release(self) -> None:
        return None


class _NeverLeader:
    """Stand-in AsyncLeaderElection that always answers "you are a follower"."""

    def __init__(self, *_args, **_kwargs):
        pass

    async def is_leader(self) -> bool:
        return False

    async def release(self) -> None:
        return None


async def _spin_until(predicate, *, seconds: float = 5.0) -> None:
    """Poll ``predicate`` until it is true or ``seconds`` elapse.

    Bounded-poll idiom copied from tests/test_aggregator.py: a fixed sleep
    both flakes under CI load and, worse for a couple of tests below, would
    read a loop that silently died as a false green (the predicate simply
    never becomes true and the deadline is what turns that into a failed
    assertion downstream, instead of a hang).
    """
    deadline = asyncio.get_event_loop().time() + seconds
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)


async def _stop(stop_event: asyncio.Event, task: asyncio.Task) -> None:
    stop_event.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5.0)


def _invoice(status: str = "issued") -> BillingInvoice:
    """An UNSAVED real ``BillingInvoice``.

    ⚠️ Deliberately the real model, never a ``SimpleNamespace``. Grading a
    stand-in is exactly what let the phantom-``issued`` defect through: a
    hand-rolled object answers ``.status`` no matter what the real return
    contract is, so the test could not tell a freshly drawn invoice from an
    already-existing one being re-reported. Never added to a session -- the
    loop only ever reads ``.status`` off these.
    """
    return BillingInvoice(status=status)


def _empty_outcome() -> CloseDueOutcome:
    """A sweep that did nothing -- what the stubs below return on every tick
    AFTER the one under test, so a loop ticking every 20ms cannot inflate the
    deltas these tests assert EXACTLY."""
    return CloseDueOutcome(created=[], unchanged=[], refused=0, failed=0)


def _one_shot_close(outcome: CloseDueOutcome, calls: dict[str, int]):
    """Stub ``close_due_periods`` that yields ``outcome`` on the FIRST call and
    an empty outcome forever after.

    Why one-shot rather than "return the same outcome every tick": these tests
    assert EXACT deltas (notably ``issued`` moved by exactly 0), and the loop
    ticks continuously. A stub that answered the same non-empty outcome on
    every tick would make every delta a function of how many ticks the poller
    happened to allow -- i.e. only ``> before`` would be assertable, which is
    precisely the weak shape that could not see the defect.
    """

    async def _close_spy(*_args, **_kwargs) -> CloseDueOutcome:
        calls["n"] += 1
        if calls["n"] == 1:
            return outcome
        return _empty_outcome()

    return _close_spy


def _closed(status: str) -> float:
    return observability_metrics.invoices_closed_total.labels(status=status)._value.get()


def _failed(reason: str) -> float:
    return observability_metrics.invoice_close_failures_total.labels(reason=reason)._value.get()


# ===========================================================================
# 1. The lag timestamp gauge is published ONLY on a successful census.
# ===========================================================================


class TestLagTimestampGauge:
    """billing_invoice_close_lag_timestamp_seconds -- the load-bearing pair.

    An unlabelled Gauge is born at 0.0 at import, before the loop has ever
    ticked, so ``lag_days == 0`` alone cannot distinguish "everything is
    closed" from "this replica never measured". Only a *stale* timestamp
    tells them apart -- which only holds if the timestamp is written
    exclusively on the census's success path.

    Both cases below use ``invoice_close_lag_days = 999999`` so the loop
    always takes the SKIP branch and never touches ``close_due_periods`` --
    keeps the test isolated to the census/timestamp pairing alone.
    """

    @pytest.mark.unit
    async def test_timestamp_does_not_advance_when_census_raises(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """A raising lag census must NOT publish a fresh timestamp.

        Mutant: move the ``.set(time.time())`` call out of the census's
        success (``else``) branch onto the common path (e.g. run it
        unconditionally after the try/except instead of only when the
        ``try`` did not raise). Under that mutant this test goes RED: the
        timestamp would advance even though ``_measure_invoice_close_lag``
        raised on every tick.
        """
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = 999999  # always the skip branch

        calls = {"n": 0}

        async def _raising_census(*_args, **_kwargs):
            calls["n"] += 1
            raise RuntimeError("lag census exploded")

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr("services.invoice_closer._measure_invoice_close_lag", _raising_census)

        before = observability_metrics.invoice_close_lag_timestamp_seconds._value.get()

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(lambda: calls["n"] >= 2, seconds=5.0)
        await _stop(stop_event, task)

        assert calls["n"] >= 2, "census was never driven"
        after = observability_metrics.invoice_close_lag_timestamp_seconds._value.get()
        assert after == before, "timestamp advanced even though the lag census only raised"

    @pytest.mark.unit
    async def test_timestamp_advances_on_successful_census(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """A successful lag census DOES publish a fresh timestamp.

        Paired with the case above on purpose: without this half, a loop
        that never publishes the timestamp gauge AT ALL (e.g. the ``.set()``
        call deleted outright, or a loop that never ticks) would ALSO pass
        "did not advance while raising" -- this half is what forces the
        gauge to actually move on the real success path, closing that
        vacuous-pass hole.
        """
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = 999999  # always the skip branch

        calls = {"n": 0}

        async def _ok_census(*_args, **_kwargs):
            calls["n"] += 1
            return 0

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr("services.invoice_closer._measure_invoice_close_lag", _ok_census)

        before = observability_metrics.invoice_close_lag_timestamp_seconds._value.get()

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(lambda: calls["n"] >= 1, seconds=5.0)
        await _stop(stop_event, task)

        assert calls["n"] >= 1, "census was never driven"
        after = observability_metrics.invoice_close_lag_timestamp_seconds._value.get()
        assert after > before, "timestamp did not advance after a successful census"


# ===========================================================================
# 2. invoice_close_is_leader reflects leadership and is set in try/finally.
# ===========================================================================


class TestIsLeaderGauge:
    """billing_invoice_close_is_leader -- set in a try/finally around
    ``election.is_leader()``.

    Mutant: unwrap the finally so the gauge is only published once the rest
    of the cycle also completes cleanly (i.e. move the ``.set()`` call from
    immediately after ``is_leader()`` returns to somewhere only reached on a
    fully successful cycle). A test that only checks the leader/follower
    happy path stays green under that mutant, because on a clean cycle both
    placements publish the same value -- so the second test below drives a
    tick that BECOMES leader and then raises, and checks the gauge still
    reads the correct value for THAT tick rather than a value carried over
    from the previous (follower) tick.
    """

    @pytest.mark.unit
    async def test_gauge_is_one_for_leader_zero_for_follower(
        self, db_session_factory, test_settings, monkeypatch
    ):
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02

        # Follower half.
        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _NeverLeader)
        observability_metrics.invoice_close_is_leader.set(-1)  # neither 0 nor 1
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(
            lambda: observability_metrics.invoice_close_is_leader._value.get() == 0.0,
            seconds=5.0,
        )
        await _stop(stop_event, task)
        assert observability_metrics.invoice_close_is_leader._value.get() == 0.0

        # Leader half -- skip branch so close_due_periods is never touched.
        test_settings.invoice_close_lag_days = 999999
        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        observability_metrics.invoice_close_is_leader.set(-1)
        stop_event2 = asyncio.Event()
        task2 = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event2)
        )
        await _spin_until(
            lambda: observability_metrics.invoice_close_is_leader._value.get() == 1.0,
            seconds=5.0,
        )
        await _stop(stop_event2, task2)
        assert observability_metrics.invoice_close_is_leader._value.get() == 1.0

    @pytest.mark.unit
    async def test_gauge_reflects_second_tick_leadership_despite_a_later_raise(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """tick 1: follower (gauge -> 0). tick 2: becomes leader, but
        ``close_due_periods`` raises immediately afterward.

        Under the real code the gauge is published in the ``finally``
        wrapped tightly around the ``is_leader()`` call, so it reads 1 for
        tick 2 regardless of what happens later in that same cycle -- the
        replica genuinely does hold the lock. Under the mutant described
        above (gauge only published once the WHOLE cycle succeeds), tick 2's
        raise means the ``.set()`` for that cycle never runs, and the gauge
        is left stuck at tick 1's stale 0 -- which is wrong.
        """
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650  # always the close branch

        class _FalseThenTrueLeader:
            calls = 0

            def __init__(self, *_args, **_kwargs):
                pass

            async def is_leader(self) -> bool:
                type(self).calls += 1
                return type(self).calls >= 2

            async def release(self) -> None:
                return None

        _FalseThenTrueLeader.calls = 0

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("close_due_periods exploded")

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _FalseThenTrueLeader)
        monkeypatch.setattr("services.invoice_closer.close_due_periods", _boom)

        observability_metrics.invoice_close_is_leader.set(-1)
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        # tick 1: prove the follower value actually lands first.
        await _spin_until(
            lambda: observability_metrics.invoice_close_is_leader._value.get() == 0.0,
            seconds=5.0,
        )
        assert observability_metrics.invoice_close_is_leader._value.get() == 0.0
        # tick 2+: leadership flips true; close_due_periods explodes right after.
        await _spin_until(lambda: _FalseThenTrueLeader.calls >= 2, seconds=5.0)
        # No extra sleep needed here: the gauge's `.set()` happens
        # synchronously in the `finally` right after `is_leader()` returns,
        # before any further `await` in that cycle -- so by the time the
        # `_spin_until` above observes `calls >= 2`, the gauge for tick 2 is
        # already published. A fixed sleep here was dead weight that only
        # added flakiness under load.
        await _stop(stop_event, task)

        assert _FalseThenTrueLeader.calls >= 2, "the leader tick was never driven"
        assert observability_metrics.invoice_close_is_leader._value.get() == 1.0, (
            "gauge did not reflect the leader tick's TRUE verdict -- looks "
            "stale from the prior follower tick"
        )


# ===========================================================================
# 3. The settle window is honoured.
# ===========================================================================


class TestSettleWindow:
    """A period whose ``period_end + settings.invoice_close_lag_days`` is
    still in the future must be ``skipped`` and NOT closed; one already past
    it must be closed.

    Mutant: drop the settle comparison (i.e. always take the close branch
    regardless of the date). ``test_not_yet_due_period_is_skipped_not_closed``
    below is the one that must go RED under that mutant -- with the
    comparison gone, ``close_due_periods`` would be called even though the
    period is nowhere near its settle window.
    """

    @pytest.mark.unit
    async def test_not_yet_due_period_is_skipped_not_closed(
        self, db_session_factory, test_settings, monkeypatch
    ):
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = 3650  # settle date far in the future

        close_calls = {"n": 0}

        async def _close_spy(*_args, **_kwargs):
            close_calls["n"] += 1
            return []

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr("services.invoice_closer.close_due_periods", _close_spy)

        before = observability_metrics.invoices_closed_total.labels(status="skipped")._value.get()

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(
            lambda: (
                observability_metrics.invoices_closed_total.labels(status="skipped")._value.get()
                > before
            ),
            seconds=5.0,
        )
        await _stop(stop_event, task)

        after = observability_metrics.invoices_closed_total.labels(status="skipped")._value.get()
        assert after > before, "skipped counter never incremented for a not-yet-due period"
        assert close_calls["n"] == 0, "close_due_periods was called for a not-yet-due period"

    @pytest.mark.unit
    async def test_due_period_is_closed(self, db_session_factory, test_settings, monkeypatch):
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650  # settle date far in the past

        close_calls = {"n": 0}
        outcome = CloseDueOutcome(created=[_invoice("issued")], unchanged=[], refused=0, failed=0)

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr(
            "services.invoice_closer.close_due_periods",
            _one_shot_close(outcome, close_calls),
        )

        before = observability_metrics.invoices_closed_total.labels(status="issued")._value.get()

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(lambda: close_calls["n"] >= 1, seconds=5.0)
        await _stop(stop_event, task)

        after = observability_metrics.invoices_closed_total.labels(status="issued")._value.get()
        assert close_calls["n"] >= 1, "close_due_periods was never called for a due period"
        assert after - before == 1.0, (
            "issued counter did not move by exactly one per CREATED invoice "
            f"(before={before}, after={after})"
        )


# ===========================================================================
# 3b. CloseDueOutcome's four fields land on four DIFFERENT series.
# ===========================================================================


class TestCloseOutcomeAccounting:
    """One tick must split ``CloseDueOutcome`` across the right series.

    ⚠️ THIS CLASS EXISTS BECAUSE OF A REAL, OBSERVED PRODUCTION DEFECT, not a
    reasoned mutant. ``close_period`` returns an ALREADY-EXISTING invoice
    unchanged; the old ``close_due_periods`` appended that object to one flat
    ``list[BillingInvoice]``, and this loop counted every item in that list as
    ``issued``. Because the sweep re-observes the same closed period on every
    hourly tick, one genuinely-closed tenant-month produced ~650 phantom
    ``issued`` per tenant per month -- a counter that looked like healthy
    throughput and was in fact the same invoice being re-reported forever.

    ⛔ The OLD test could not see it, and the reason is the point: it stubbed
    ``close_due_periods`` with a lambda returning ``[SimpleNamespace(
    status="issued")]``. That grades the STUB, not the return contract -- a
    hand-rolled object with a ``.status`` attribute answers identically
    whether the invoice was drawn this sweep or re-read from a previous one,
    so no assertion over it could ever distinguish the two. Every test below
    therefore constructs the REAL ``CloseDueOutcome`` and REAL (unsaved)
    ``BillingInvoice`` rows.

    All three assert EXACT deltas, never ``> before``. ``> before`` is what
    the defect passes: under the old behaviour ``issued`` moved on an
    all-unchanged sweep, which is greater-than and therefore green.
    """

    @pytest.mark.unit
    async def test_all_unchanged_sweep_does_not_move_issued(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """THE REGRESSION TEST FOR THE ACTUAL DEFECT.

        A sweep that closed nothing and merely re-observed three already-closed
        periods must move ``{status="unchanged"}`` by exactly 3 and
        ``{status="issued"}`` by exactly 0.

        Against the OLD behaviour the ``issued`` delta was 3, not 0 -- the
        three already-existing invoices arrived in the same flat list as
        freshly drawn ones and were counted as closes. ``assert issued_delta
        == 0.0`` is the single line that goes red there.
        """
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650  # always the close branch

        close_calls = {"n": 0}
        outcome = CloseDueOutcome(
            created=[],
            unchanged=[_invoice("issued"), _invoice("issued"), _invoice("issued")],
            refused=0,
            failed=0,
        )

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr(
            "services.invoice_closer.close_due_periods",
            _one_shot_close(outcome, close_calls),
        )

        issued_before = _closed("issued")
        unchanged_before = _closed("unchanged")

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        # Spin on the OBSERVABLE the tick publishes, not merely on the stub's
        # call count: a green here has to mean the loop reached the counting
        # code, not just that it entered the stub.
        await _spin_until(lambda: _closed("unchanged") > unchanged_before, seconds=5.0)
        await _stop(stop_event, task)

        assert close_calls["n"] >= 1, "close_due_periods was never called"
        # ⚠️ Note the statuses on the `unchanged` rows above are "issued" --
        # the invoices really are issued invoices. The label must come from
        # WHICH LIST they arrived in, never from `.status`.
        assert _closed("issued") - issued_before == 0.0, (
            'an all-unchanged sweep moved invoices_closed_total{status="issued"} '
            "-- already-closed periods are being re-counted as fresh closes, the "
            "~650-phantom-issued-per-tenant-per-month defect"
        )
        assert _closed("unchanged") - unchanged_before == 3.0, (
            "unchanged counter did not move by len(outcome.unchanged)"
        )

    @pytest.mark.unit
    async def test_refused_and_failed_land_on_their_own_reasons(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """``refused`` -> ``reason="tenant_refused"``, ``failed`` ->
        ``reason="tenant_error"``, each by its own count.

        Kept APART on purpose: a refusal is ``services.invoices`` declining on
        purpose (``InvoiceError``) and a failure is it breaking. Two different
        counts, and the asymmetric values (2 vs 1) below mean a fix that
        merely summed them onto one series, or crossed the two labels, cannot
        pass -- symmetric counts would have let a swap through.

        Against the OLD behaviour neither series moved at all: a flat list
        carried no refusal or failure information, so ``assert
        refused_delta == 2.0`` goes red (0.0 observed).
        """
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650  # always the close branch

        close_calls = {"n": 0}
        outcome = CloseDueOutcome(created=[], unchanged=[], refused=2, failed=1)

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr(
            "services.invoice_closer.close_due_periods",
            _one_shot_close(outcome, close_calls),
        )

        refused_before = _failed("tenant_refused")
        error_before = _failed("tenant_error")
        issued_before = _closed("issued")

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(lambda: _failed("tenant_error") > error_before, seconds=5.0)
        await _stop(stop_event, task)

        assert close_calls["n"] >= 1, "close_due_periods was never called"
        assert _failed("tenant_refused") - refused_before == 2.0, (
            "outcome.refused did not land on invoice_close_failures_total"
            '{reason="tenant_refused"} by its own count'
        )
        assert _failed("tenant_error") - error_before == 1.0, (
            "outcome.failed did not land on invoice_close_failures_total"
            '{reason="tenant_error"} by its own count'
        )
        # A tenant that refused or died closed nothing.
        assert _closed("issued") - issued_before == 0.0, (
            "a sweep that only refused/failed still moved the issued counter"
        )

    @pytest.mark.unit
    async def test_mixed_outcome_splits_across_all_three_series_in_one_tick(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """created=2, unchanged=3, refused=2, failed=0 in ONE tick.

        The three distinct non-zero counts are the point: a loop that read the
        wrong field, summed two fields, or reused one length for another label
        lands on the wrong number rather than merely on the wrong series, so no
        single crossed wire can pass by coincidence.

        Against the OLD behaviour ``assert issued_delta == 2.0`` goes red: the
        flat list held created AND unchanged together, so ``issued`` moved by
        5.
        """
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650  # always the close branch

        close_calls = {"n": 0}
        outcome = CloseDueOutcome(
            created=[_invoice("issued"), _invoice("issued")],
            unchanged=[_invoice("issued"), _invoice("issued"), _invoice("issued")],
            refused=2,
            failed=0,
        )

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr(
            "services.invoice_closer.close_due_periods",
            _one_shot_close(outcome, close_calls),
        )

        issued_before = _closed("issued")
        unchanged_before = _closed("unchanged")
        refused_before = _failed("tenant_refused")
        error_before = _failed("tenant_error")

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(lambda: _failed("tenant_refused") > refused_before, seconds=5.0)
        await _stop(stop_event, task)

        assert close_calls["n"] >= 1, "close_due_periods was never called"
        assert _closed("issued") - issued_before == 2.0, (
            "issued did not move by len(outcome.created) -- if it moved by 5 the "
            "unchanged rows are being counted as fresh closes"
        )
        assert _closed("unchanged") - unchanged_before == 3.0, (
            "unchanged did not move by len(outcome.unchanged)"
        )
        assert _failed("tenant_refused") - refused_before == 2.0, (
            "tenant_refused did not move by outcome.refused"
        )
        # failed=0: the guard on the source is `if outcome.failed:`, so a zero
        # must publish nothing at all rather than an inc(0) -- and, more to the
        # point, refusals must not bleed onto the error series.
        assert _failed("tenant_error") - error_before == 0.0, (
            'refusals bled onto invoice_close_failures_total{reason="tenant_error"}'
        )


# ===========================================================================
# 3c. A VOIDED invoice is a GAP, not coverage -- the lag census's own query.
# ===========================================================================

#: Fixed clock for the census tests. ``_last_completed_month`` of this instant
#: is February 2026, whose settle date under ``invoice_close_lag_days = 3`` is
#: 2026-03-04 -- sixteen days before this ``now``, i.e. comfortably past the
#: window, so the period is unambiguously eligible to be counted as lag and the
#: test cannot pass or fail on where the real calendar happens to be today.
_CENSUS_NOW = datetime(2026, 3, 20, 12, 0, tzinfo=UTC)
_CENSUS_PERIOD_START = date(2026, 2, 1)
_CENSUS_PERIOD_END = date(2026, 3, 1)  # EXCLUSIVE, matching BillingInvoice
#: The one honest answer for a period whose only invoice does not count as
#: coverage: the age of that period's (exclusive) end at ``_CENSUS_NOW``.
_EXPECTED_LAG_DAYS = (_CENSUS_NOW.date() - _CENSUS_PERIOD_END).days  # 19


async def _seed_census_period(session_factory, *, tenant_id: str, invoice_status: str) -> None:
    """Seed ONE active tenant-period plus ONE invoice for it, at ``status``.

    "Active" is spelled the way ``_measure_invoice_close_lag`` counts it -- a
    ``billing_cost_daily`` row with a ``bucket_date`` inside
    ``[period_start, period_end)`` -- rather than by calling any production
    seeding helper, so a change to how the service defines activity shows up
    here as a red test instead of being tracked silently by a shared helper.
    """
    async with session_factory() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                service_key="workers",  # pragma: allowlist secret
                provider="openai",
                bucket_date=date(2026, 2, 14),  # inside [period_start, period_end)
                total_input_tokens=1000,
                total_output_tokens=500,
                total_cost_usd=Decimal("1.250000"),
                request_count=10,
            )
        )
        invoice = BillingInvoice(
            tenant_id=tenant_id,
            tenant_name="acme",
            period_start=_CENSUS_PERIOD_START,
            period_end=_CENSUS_PERIOD_END,
            status=invoice_status,
            invoice_number=f"INV-2026-{invoice_status}",
            subtotal_usd=Decimal("1.62500000"),
            total_usd=Decimal("1.62500000"),
            internal_cost_usd=Decimal("1.25000000"),
            margin_usd=Decimal("0.37500000"),
            billing_mode="standard_markup",
            markup_pct=Decimal("30.00"),
            effective_markup_pct=Decimal("30.0000"),
            snapshot={},
            inputs={},
            snapshot_sha256="0" * 64,
            generator_version="invoice.v1",
        )
        if invoice_status == "void":
            # Written on the INSERT, not by a later UPDATE: the immutability
            # trigger refuses to revive or re-edit a void row, and this test is
            # about the census's read, not about the voiding path.
            invoice.issued_at = datetime(2026, 3, 4, tzinfo=UTC)
            invoice.voided_at = datetime(2026, 3, 6, tzinfo=UTC)
            invoice.voided_by = "operator@test"
            invoice.void_reason = "wrong markup applied"
        else:
            invoice.issued_at = datetime(2026, 3, 4, tzinfo=UTC)
        session.add(invoice)
        await session.commit()


class TestVoidedInvoiceIsNotCoverage:
    """``_measure_invoice_close_lag`` must count a VOIDED tenant-period as an
    OPEN one.

    ⚠️ THIS IS THE ONLY TEST IN THIS FILE THAT CALLS THE CENSUS FOR REAL.
    Every other test above monkeypatches ``_measure_invoice_close_lag`` away
    (or drives it only far enough to move the timestamp gauge), so its query
    -- the thing that decides what the lag gauge actually MEANS -- had no
    coverage at all. A status filter with no test is a claim, not a
    behaviour.

    The defect this catches: voiding an invoice REOPENS the tenant-period (see
    ``ux_billing_invoices_open_period``, whose partial predicate is the same
    positive ``status IN ('draft','issued')`` set), and nothing in this loop
    repairs it -- ``close_due_periods`` only ever acts on the LAST completed
    month, so a period voided after that tick is a gap a human must close by
    hand. If the census counted the dead row as coverage, the one gauge that
    exists to surface that backlog would read a flat, healthy 0 forever and
    the unbilled month would never surface anywhere.

    ⛔ THE TWO TESTS ARE ONE INSTRUMENT AND NEITHER IS MEANINGFUL ALONE.
    ``void -> non-zero`` on its own cannot tell "the status filter works" from
    "some unrelated period in this database is open", and the census scans 24
    periods, so plenty of other rows could supply a non-zero for the wrong
    reason. ``issued -> 0`` is the control: identical seeding, one column
    different, and it pins the non-zero to THAT column. The void case asserts
    the EXACT expected age rather than ``> 0`` for the same reason -- it names
    which period is open, not merely that something is.

    Mutant: delete ``BillingInvoice.status.in_(("draft", "issued"))`` from the
    ``closed_tenants`` query (or weaken it to ``!= 'void'``'s intended
    opposite, i.e. no filter at all). ``test_voided_period_is_counted_as_lag``
    goes RED -- the void row would join ``closed_tenants``, ``active -
    closed_tenants`` would be empty, and the census would return 0 instead of
    19. The control stays GREEN under that mutant, which is what proves the
    red came from the status filter and not from the fixture.
    """

    @pytest.mark.unit
    async def test_voided_period_is_counted_as_lag(self, db_session_factory, test_settings):
        """A tenant-period whose ONLY invoice is ``void`` is still open."""
        test_settings.invoice_close_lag_days = 3

        await _seed_census_period(
            db_session_factory, tenant_id=T("census-void"), invoice_status="void"
        )

        lag = await _measure_invoice_close_lag(
            db_session_factory, settings=test_settings, now=_CENSUS_NOW
        )

        assert lag == _EXPECTED_LAG_DAYS, (
            "a VOIDED invoice was counted as coverage for its tenant-period: "
            f"the census returned {lag}, expected {_EXPECTED_LAG_DAYS} "
            f"(the age of {_CENSUS_PERIOD_END} at {_CENSUS_NOW.date()}). "
            "0 here means the status filter on closed_tenants is gone"
        )

    @pytest.mark.unit
    async def test_issued_period_is_not_counted_as_lag(self, db_session_factory, test_settings):
        """THE CONTROL. Identical seeding, ``issued`` instead of ``void``.

        Without this the test above proves nothing: any open period anywhere
        in the 24-month scan window would satisfy a non-zero assertion. A red
        HERE means the fixture (or the census's notion of "active") is wrong,
        not that the status filter is -- which is a different bug and has to
        be readable as one.
        """
        test_settings.invoice_close_lag_days = 3

        await _seed_census_period(
            db_session_factory, tenant_id=T("census-issued"), invoice_status="issued"
        )

        lag = await _measure_invoice_close_lag(
            db_session_factory, settings=test_settings, now=_CENSUS_NOW
        )

        assert lag == 0, (
            "an ISSUED invoice failed to cover its own tenant-period -- the "
            f"census returned {lag}. The paired void case's non-zero therefore "
            "cannot be attributed to the status filter"
        )


# ===========================================================================
# 4. The advisory lock rides the SESSION-mode pooler, never DATABASE_URL.
# ===========================================================================


class TestLeaderElectionDsn:
    """The lock election MUST be built from
    ``settings.resolve_leader_election_dsn()``, never ``settings.database_url``.

    Mutant: point the election at ``settings.database_url`` instead. Under
    that mutant this test goes RED because ``database_url`` and
    ``leader_election_dsn`` are set to two DIFFERENT values below --
    ``recorded[0]`` would equal ``settings.database_url`` instead of
    ``settings.resolve_leader_election_dsn()``.
    """

    @pytest.mark.unit
    async def test_election_is_built_from_resolve_leader_election_dsn(
        self, db_session_factory, monkeypatch
    ):
        recorded: list[str] = []

        class _RecordingLeader:
            def __init__(self, dsn: str, lock_key: int):
                recorded.append(dsn)

            async def is_leader(self) -> bool:
                return False

            async def release(self) -> None:
                return None

        settings = Settings(
            # Deliberately NOT TEST_DATABASE_URL: proves the loop reads the
            # right field even when the two DSNs genuinely diverge, the way
            # they do in production (transaction pooler vs session pooler).
            database_url="postgresql+psycopg://bogus-transaction-pooler.invalid:6432/billing",
            leader_election_dsn=TEST_DATABASE_URL,
            redis_url="redis://localhost:6379/15",
            log_format="console",
            environment="test",
            licenses_url="http://localhost:9999",
            invoice_close_interval_seconds=0.02,
            leader_retry_interval_seconds=0.02,
        )
        assert settings.resolve_leader_election_dsn() == TEST_DATABASE_URL
        assert settings.database_url != TEST_DATABASE_URL

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _RecordingLeader)

        stop_event = asyncio.Event()
        task = asyncio.create_task(run_invoice_close_loop(settings, db_session_factory, stop_event))
        await _spin_until(lambda: len(recorded) >= 1, seconds=5.0)
        await _stop(stop_event, task)

        assert recorded, "AsyncLeaderElection was never constructed"
        assert recorded[0] == settings.resolve_leader_election_dsn()
        assert recorded[0] != settings.database_url


# ===========================================================================
# 5. Failure accounting.
# ===========================================================================


class TestFailureAccounting:
    """A raising cycle increments ``invoice_close_failures_total{reason=...}``
    and the loop CONTINUES rather than dying.

    Mutant: let the exception escape the loop's own except arms (e.g. narrow
    the broad ``except Exception`` to a specific tuple that does not cover
    ``RuntimeError``, or delete the arm outright). Under that mutant the task
    dies after its first cycle: ``calls["n"]`` never reaches 2 within the
    poll deadline below, so ``assert calls["n"] >= 2`` goes RED (a bounded
    poll, not a hang, so the mutant fails fast rather than wedging the run).
    """

    @pytest.mark.unit
    async def test_raising_cycle_counts_failure_and_loop_survives(
        self, db_session_factory, test_settings, monkeypatch
    ):
        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650  # always the close branch

        calls = {"n": 0}

        async def _always_boom(*_args, **_kwargs):
            calls["n"] += 1
            raise RuntimeError("close_due_periods exploded")

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr("services.invoice_closer.close_due_periods", _always_boom)

        before = observability_metrics.invoice_close_failures_total.labels(
            reason="unexpected_error"
        )._value.get()

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(lambda: calls["n"] >= 2, seconds=5.0)
        await _stop(stop_event, task)

        assert calls["n"] >= 2, f"loop died after {calls['n']} cycle(s) instead of surviving"
        after = observability_metrics.invoice_close_failures_total.labels(
            reason="unexpected_error"
        )._value.get()
        assert after - before >= 2.0, "failure counter did not increment per raising cycle"


# ===========================================================================
# 6. billing_invoice_close_enabled is set UNCONDITIONALLY in main.lifespan,
#    outside the `if settings.invoice_close_enabled:` block.
# ===========================================================================


class TestInvoiceCloseEnabledGauge:
    """billing_invoice_close_enabled must read 1 when
    ``settings.invoice_close_enabled`` is True and 0 when it is False, on
    EVERY replica -- including a disabled one, which only holds if
    ``main.py``'s ``_metrics.invoice_close_enabled.set(...)`` line sits
    OUTSIDE the ``if settings.invoice_close_enabled:`` block that gates the
    actual loop task.

    This drives the REAL ``main.lifespan``, not a mock of it -- the same
    pattern ``tests/test_infra.py::TestLifespan.test_lifespan_with_redis``
    already uses for this exact app: ``main.init_redis`` /
    ``main.init_db`` / ``main.dispose_engine`` / ``main.close_redis`` mocked
    out so no real Redis/Postgres connection setup or teardown is attempted,
    ``db.get_session_factory`` pointed at the test database via
    ``db_session_factory``, and ``main.get_settings`` patched -- ``lifespan``
    calls the module-level ``get_settings()`` itself rather than taking a
    settings object as an argument, and that function is
    ``functools.lru_cache``d process-wide, so patching it (instead of
    fighting the cache or calling ``.cache_clear()``, which would also
    clobber whatever other tests in this process already cached) is the
    only way to control the flag it reads for this run. Both directions use
    a fresh ``Settings`` instance from ``test_settings`` so no cached value
    from another test can leak in.

    This proves the actual statement the brief describes -- the real
    ``.set()`` call at real lifespan startup -- not an assertion against a
    string copied out of ``main.py``'s source.

    Both tests poison the gauge to -1 (neither 0 nor 1) first, per this
    file's existing convention (see ``TestIsLeaderGauge``): the gauge is
    process-global with no autouse reset fixture, so a value another test
    left behind must not be able to make either assertion below pass by
    accident.

    Mutant A: move ``_metrics.invoice_close_enabled.set(...)`` inside the
    ``if settings.invoice_close_enabled:`` block. ``test_gauge_reads_one_
    when_flag_is_true`` stays green under that mutant -- with the flag True
    the `if` body still runs, so the `.set()` still fires with the same
    value either way. ``test_gauge_reads_zero_when_flag_is_false`` is the
    one that goes RED: with the flag False the `if` body never runs, the
    `.set()` never fires, and the poisoned -1 sentinel survives instead of
    becoming 0.

    Mutant B: delete the ``.set()`` call entirely. Both tests below go RED
    against the poisoned sentinel -- but the interesting case is why the
    False-flag test ALONE would not have caught this: a deleted `.set()`
    leaves the gauge at its born-at-0 default, which happens to be the
    exact value the False-flag case wants, so without the paired True-flag
    test a fully deleted line would read as a correctly-firing 0 forever.
    """

    @staticmethod
    async def _run_lifespan_and_read_gauge(
        settings: Settings, fake_redis, db_session_factory, monkeypatch
    ) -> float:
        import main as main_module

        # lifespan() reads settings via the module-level get_settings(),
        # not an argument -- and that function is lru_cache'd, so patching
        # it (rather than e.g. Settings(...) construction or env vars) is
        # what actually reaches the code under test here.
        monkeypatch.setattr(main_module, "get_settings", lambda: settings)
        with (
            patch("main.init_redis", new_callable=AsyncMock, return_value=fake_redis),
            patch("main.init_db", new_callable=AsyncMock),
            patch("main.dispose_engine", new_callable=AsyncMock),
            patch("main.close_redis", new_callable=AsyncMock),
            patch("db.get_session_factory", return_value=db_session_factory),
        ):
            async with main_module.lifespan(main_module.app):
                # The gauge is set synchronously, near the top of the
                # `if redis_conn:` block, before the yield -- no tick or
                # poll needed, it is already published by the time we are
                # inside the context.
                return observability_metrics.invoice_close_enabled._value.get()

    @pytest.mark.unit
    async def test_gauge_reads_one_when_flag_is_true(
        self, test_settings, fake_redis, db_session_factory, monkeypatch
    ):
        test_settings.invoice_close_enabled = True
        observability_metrics.invoice_close_enabled.set(-1)  # neither 0 nor 1

        value = await self._run_lifespan_and_read_gauge(
            test_settings, fake_redis, db_session_factory, monkeypatch
        )

        assert value == 1.0, "flag was True but the gauge did not read 1"

    @pytest.mark.unit
    async def test_gauge_reads_zero_when_flag_is_false(
        self, test_settings, fake_redis, db_session_factory, monkeypatch
    ):
        test_settings.invoice_close_enabled = False
        observability_metrics.invoice_close_enabled.set(-1)  # neither 0 nor 1

        value = await self._run_lifespan_and_read_gauge(
            test_settings, fake_redis, db_session_factory, monkeypatch
        )

        assert value == 0.0, (
            "flag was False but the gauge did not read 0 -- either the "
            ".set() call moved inside the `if settings.invoice_close_enabled:` "
            "block, or it was never reached"
        )


# ===========================================================================
# 7. The non-HTTP close path writes audit provenance.
# ===========================================================================
#
# ⛔ WHY THIS SECTION EXISTS. Until it did, the leader loop was the ONLY
# invoice-minting path in the service that wrote no ``audit_log`` row at all.
# ``audit.record`` was called from exactly three places -- ``routers/billing.py``
# :505 (close), :583 (issue), :647 (void) -- and ``services/invoices.py``
# contained no ``audit`` reference whatsoever, so every invoice the steady-state
# sweep minted existed with its actor surviving only as the literal string
# ``"close_due_periods"`` in ``billing_invoices.created_by``. The estate routes
# money writes through the API rather than SQL precisely so the audit log carries
# provenance for every pricing row; the automated path -- the one that will mint
# nearly every invoice once ``INVOICE_CLOSE_ENABLED`` is armed -- bypassed it.
#
# ⚠️ ``test_leader_close_writes_an_audit_row`` is the KILLING test: on unmodified
# code the invoice is created and ``audit_log`` is EMPTY.
#
# Imports are section-local ON PURPOSE, matching ``tests/test_invoices.py``
# section G: this whole section is additive, so a concurrent edit to this file's
# module-level import block merges without a conflict.


async def _spin_until_async(predicate, *, seconds: float = 10.0) -> None:
    """``_spin_until`` for a predicate that has to hit the database.

    Separate from ``_spin_until`` rather than a widening of it: that helper is
    used by a dozen tests above with a synchronous metric read, and making it
    await an arbitrary callable would change what every one of them does.
    """
    deadline = asyncio.get_event_loop().time() + seconds
    while asyncio.get_event_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.05)


def _previous_calendar_month(today: date) -> tuple[date, date]:
    """``(start, end-exclusive)`` of the calendar month BEFORE ``today``'s.

    ⚠️ Spelled out here rather than imported from
    ``services.invoice_closer._last_completed_month``: a fixture computed BY the
    function under test agrees with it by construction, so a mutation to that
    month arithmetic would move the fixture along with it and stay green.

    ⚠️ And it must be derived from the real clock, not pinned to a literal month
    the way every other test in this file pins ``_CENSUS_NOW``:
    ``run_invoice_close_loop`` calls ``datetime.now(UTC)`` itself and takes no
    clock argument, so the only period it will ever close is the one before the
    month the suite actually runs in. (The one boundary case is a run that
    crosses midnight on the 1st between this call and the loop's own; there is no
    injection point to close that window from here.)
    """
    end = date(today.year, today.month, 1)
    start = date(end.year - 1, 12, 1) if end.month == 1 else date(end.year, end.month - 1, 1)
    return start, end


async def _seed_closable_month(
    session_factory,  # noqa: ANN001
    *,
    tenant_id: str,
    license_id,  # noqa: ANN001
    period_start: date,
    period_end: date,
) -> None:
    """Make ``[period_start, period_end)`` genuinely closable for ``tenant_id``.

    ⚠️ A ``billing_cost_daily`` row for EVERY day of the period, because
    ``assert_no_bucket_gap`` is FLEET-WIDE and refuses a period containing even
    one entirely absent date -- there is no smaller fixture that reaches the real
    close. Plus a tenant-scoped ``MarkupRule``: an all-wildcard rule has
    specificity 0 and ``services.markup.compute_billed_line`` treats that as a
    miss.

    Restated here rather than imported from ``tests/test_invoices.py`` or
    ``tests/routers/test_invoices_routes.py``, which each hold their own copy:
    this file must not red when one of those files changes its own fixture.
    """
    from datetime import timedelta as _timedelta

    from models.db import MarkupRule

    async with session_factory() as session:
        session.add(
            MarkupRule(
                tenant_id=tenant_id,
                markup_pct=Decimal("30.00"),
                effective_from=datetime(2025, 1, 1, tzinfo=UTC),
                effective_until=None,
            )
        )
        d = period_start
        while d < period_end:
            session.add(
                CostDaily(
                    tenant_id=tenant_id,
                    license_id=license_id,
                    service_key="workers",  # pragma: allowlist secret
                    provider="test",
                    bucket_date=d,
                    total_input_tokens=1_000,
                    total_output_tokens=200,
                    total_cost_usd=Decimal("2.500000"),
                    request_count=10,
                )
            )
            d += _timedelta(days=1)
        await session.commit()


def _licence_payload_for(tenant_licences: dict):  # noqa: ANN201
    """Patch BOTH ``fetch_licenses`` bindings with a TENANT-KEYED answer.

    ``services.invoices`` holds ``close_period``'s pre-fetch and
    ``services.billing`` holds ``generate_billing``'s; leaving either unpatched
    sends that call to the real network. Keyed on the first positional argument
    (``fetch_licenses(tenant_id, settings, ...)``) rather than returning one flat
    payload, so a two-tenant test cannot hand tenant A's licence to tenant B --
    which would put a phantom billed line on B's statement and make any
    cross-path comparison compare two different things.
    """

    async def _fetch(tenant_id, *_a, **_kw):  # noqa: ANN001, ANN202
        return tenant_licences.get(tenant_id, [])

    return (
        patch("services.invoices.fetch_licenses", new=AsyncMock(side_effect=_fetch)),
        patch("services.billing.fetch_licenses", new=AsyncMock(side_effect=_fetch)),
    )


@pytest.fixture
def closer_licences_answer_byo_mode(monkeypatch):
    """Answer the licences ``byo-mode`` endpoint; delegate every other GET.

    ``assert_byo_mode_available`` refuses a close whose billing mode licences
    cannot state, so a REAL close cannot be reached without this. URL-scoped and
    delegating rather than a blanket stub, and NOT autouse -- every other test in
    this file stubs ``close_due_periods`` away and must keep reaching no network
    at all. The TTL cache is cleared both sides: it is module state, so one
    test's answer must not decide another's.
    """
    import httpx

    from services import byo_mode as byo

    byo.clear_byo_cache()
    real_get = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if "byo-mode" in str(url):
            return httpx.Response(200, json={"byo_mode": "byo_required"})
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    yield
    byo.clear_byo_cache()


@pytest.fixture
def closer_pricing_writer(monkeypatch):
    """A bearer carrying ``pricing-writer`` -- the role the invoice writes need.

    Same shape as ``tests/routers/test_invoices_routes.py::pricing_writer``;
    needed here because the parity test has to drive the REAL HTTP close to have
    anything to compare the loop's row against.
    """
    import auth
    import auth_deps

    payload = {"sub": "test-pricing-writer", "realm_access": {"roles": ["pricing-writer"]}}

    async def _fake_decode(_request):  # noqa: ANN202
        return payload

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return payload


async def _audit_rows(session_factory, action: str) -> list:  # noqa: ANN001
    """Every ``audit_log`` row for ``action``, read on a session of its own."""
    from sqlalchemy import select as _select

    from models.db import AuditLog

    async with session_factory() as session:
        return list(
            (await session.execute(_select(AuditLog).where(AuditLog.action == action)))
            .scalars()
            .all()
        )


async def _invoices_for(session_factory, tenant_id: str) -> list:  # noqa: ANN001
    from sqlalchemy import select as _select

    async with session_factory() as session:
        return list(
            (
                await session.execute(
                    _select(BillingInvoice).where(BillingInvoice.tenant_id == tenant_id)
                )
            )
            .scalars()
            .all()
        )


class TestLeaderClosePath:
    """⛔ THE LEADER LOOP MUST LEAVE PROVENANCE, NOT JUST AN INVOICE.

    Every test here drives the REAL ``close_due_periods`` through the REAL loop
    against the database. Stubbing the sweep -- what the six sections above all
    do -- cannot say anything about this property: a stub returns a
    ``CloseDueOutcome`` whether or not anything was ever written, and the thing
    under test is a WRITE that happens beside the invoice.
    """

    @pytest.mark.unit
    async def test_leader_close_writes_an_audit_row(
        self,
        db_session_factory,
        test_settings,
        monkeypatch,
        closer_licences_answer_byo_mode,
    ):
        """⛔ THE KILLING TEST. On unmodified code the invoice is minted and
        ``audit_log`` is EMPTY -- ``audit.record`` had exactly three call sites,
        all three in ``routers/billing.py``, none of them on this path.

        Mutant: delete the ``_record_close_audit`` call from
        ``run_invoice_close_loop``. This goes RED on ``len(rows) == 1`` while
        every other test in this file stays green, because every other test
        stubs the sweep away.
        """
        import uuid

        from services.invoice_closer import AUDIT_ACTOR_ROLE, AUDIT_ACTOR_SUB
        from tests.conftest import T as _T

        period_start, period_end = _previous_calendar_month(datetime.now(UTC).date())
        tenant = _T("closer-audit-loop")
        license_id = uuid.uuid4()

        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650  # settle date far in the past

        await _seed_closable_month(
            db_session_factory,
            tenant_id=tenant,
            license_id=license_id,
            period_start=period_start,
            period_end=period_end,
        )

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)

        # CONTROL: a successful sweep must not touch the audit-failure series.
        audit_errors_before = _failed("audit_error")

        close_patch, gen_patch = _licence_payload_for(
            {
                tenant: [
                    {
                        "id": str(license_id),
                        "app": {"key": "workers"},
                        "plan": {"code": "pro", "quota_monthly": None},
                    }
                ]
            }
        )
        stop_event = asyncio.Event()
        with close_patch, gen_patch:
            task = asyncio.create_task(
                run_invoice_close_loop(test_settings, db_session_factory, stop_event)
            )
            # Polled on the AUDIT row, not on the invoices_closed_total counter:
            # that counter is incremented immediately BEFORE the audit write, so
            # stopping the loop on it would cancel the task mid-write and make
            # this test's own race the thing it measures.
            await _spin_until_async(
                lambda: _audit_rows(db_session_factory, "billing.invoice.close"),
                seconds=15.0,
            )
            await _stop(stop_event, task)

        # NON-VACUITY FIRST. Without this, a sweep that closed nothing at all
        # (a fixture that does not satisfy the bucket-gap gate, a licences stub
        # that stopped matching) would red on the audit assertion below and read
        # as "the audit write is broken" -- a different defect entirely.
        invoices = await _invoices_for(db_session_factory, tenant)
        assert len(invoices) == 1, (
            f"the loop minted {len(invoices)} invoices for {period_start}..{period_end}; "
            "this test cannot say anything about audit provenance until it mints "
            "exactly one"
        )
        invoice = invoices[0]

        rows = await _audit_rows(db_session_factory, "billing.invoice.close")
        assert len(rows) == 1, (
            "the leader close path wrote "
            f"{len(rows)} audit_log rows for an invoice it DID mint "
            f"({invoice.invoice_number}). Zero is the pre-fix behaviour: the "
            "actor survives only as billing_invoices.created_by and the close "
            "has no provenance at all"
        )
        row = rows[0]
        assert row.actor_sub == AUDIT_ACTOR_SUB
        assert row.actor_role == AUDIT_ACTOR_ROLE
        assert row.resource_type == "invoice"
        assert row.resource_id == str(invoice.id)
        assert row.tenant_id == tenant
        assert row.before is None
        assert row.after == {
            "status": invoice.status,
            "invoice_number": invoice.invoice_number,
        }
        # A correlation id, minted per sweep the way middleware/request_id.py
        # mints one for a request that arrives without an X-Request-ID.
        assert row.request_id, "the loop's audit row carries no correlation id"

        assert _failed("audit_error") == audit_errors_before, (
            'a SUCCESSFUL sweep moved invoice_close_failures_total{reason="audit_error"}'
        )

    @pytest.mark.unit
    async def test_loop_audit_actor_matches_invoice_created_by(
        self,
        db_session_factory,
        test_settings,
        monkeypatch,
        closer_licences_answer_byo_mode,
    ):
        """``audit_log.actor_sub`` must equal the invoice's own ``created_by``.

        The two literals live in two modules -- ``AUDIT_ACTOR_SUB`` in
        ``services/invoice_closer.py`` and the inline ``actor="close_due_periods"``
        at ``services/invoices.py:1686`` -- and there is no exported constant to
        import, so nothing but this test stops them drifting. If they drift, one
        act acquires two names and no query can join the audit row to the row it
        is the provenance for.

        Mutant: change either literal. This goes RED; nothing else does.
        """
        import uuid

        from tests.conftest import T as _T

        period_start, period_end = _previous_calendar_month(datetime.now(UTC).date())
        tenant = _T("closer-audit-actor")
        license_id = uuid.uuid4()

        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650

        await _seed_closable_month(
            db_session_factory,
            tenant_id=tenant,
            license_id=license_id,
            period_start=period_start,
            period_end=period_end,
        )
        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)

        close_patch, gen_patch = _licence_payload_for(
            {
                tenant: [
                    {
                        "id": str(license_id),
                        "app": {"key": "workers"},
                        "plan": {"code": "pro", "quota_monthly": None},
                    }
                ]
            }
        )
        stop_event = asyncio.Event()
        with close_patch, gen_patch:
            task = asyncio.create_task(
                run_invoice_close_loop(test_settings, db_session_factory, stop_event)
            )
            await _spin_until_async(
                lambda: _audit_rows(db_session_factory, "billing.invoice.close"),
                seconds=15.0,
            )
            await _stop(stop_event, task)

        invoices = await _invoices_for(db_session_factory, tenant)
        assert len(invoices) == 1, "nothing was closed; the comparison has no subject"
        rows = await _audit_rows(db_session_factory, "billing.invoice.close")
        assert len(rows) == 1, "no audit row to compare against created_by"

        assert invoices[0].created_by, (
            "the invoice carries no created_by at all, so this test would pass "
            "vacuously if the audit row were also empty"
        )
        assert rows[0].actor_sub == invoices[0].created_by, (
            "the audit row's actor and the invoice's own created_by disagree: "
            f"{rows[0].actor_sub!r} vs {invoices[0].created_by!r}"
        )

    @pytest.mark.unit
    async def test_loop_audit_row_matches_http_close_audit_row(
        self,
        db_session_factory,
        app_client,
        test_settings,
        monkeypatch,
        closer_licences_answer_byo_mode,
        closer_pricing_writer,
    ):
        """THE PARITY TEST -- this is what stops the two paths drifting.

        One period, two tenants, two close paths: tenant A through
        ``POST /api/billing/invoices/close`` and tenant B through the leader loop
        (which sees A as ``unchanged`` and mints only B). Then the two
        ``audit_log`` rows are compared field by field.

        ⛔ Every field is asserted as EQUAL or as DELIBERATELY DIFFERENT, never
        left unmentioned. An "is not None" parity test would pass against a row
        that named a different action or dropped the ``after`` payload, which is
        exactly the drift this exists to catch.

        Mutants this reds: give the loop its own action literal (e.g.
        ``billing.invoice.close_due``) -- a query for "who closed invoices" would
        then silently miss every automated close; drop or rename a key in the
        loop's ``after`` payload; pass no actor, so ``audit.record``'s
        ``actor_sub = "unknown"`` fallback (services/audit.py:102) fires.
        """
        import uuid

        from services.invoice_closer import AUDIT_ACTOR_ROLE, AUDIT_ACTOR_SUB
        from tests.conftest import T as _T

        period_start, period_end = _previous_calendar_month(datetime.now(UTC).date())
        http_tenant = _T("closer-audit-http")
        loop_tenant = _T("closer-audit-loop-b")
        http_licence = uuid.uuid4()
        loop_licence = uuid.uuid4()

        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650

        for tenant, licence in ((http_tenant, http_licence), (loop_tenant, loop_licence)):
            await _seed_closable_month(
                db_session_factory,
                tenant_id=tenant,
                license_id=licence,
                period_start=period_start,
                period_end=period_end,
            )

        def _payload(licence):  # noqa: ANN001, ANN202
            return [
                {
                    "id": str(licence),
                    "app": {"key": "workers"},
                    "plan": {"code": "pro", "quota_monthly": None},
                }
            ]

        close_patch, gen_patch = _licence_payload_for(
            {http_tenant: _payload(http_licence), loop_tenant: _payload(loop_licence)}
        )

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)

        with close_patch, gen_patch:
            response = app_client.post(
                "/api/billing/invoices/close",
                json={
                    "tenant_id": http_tenant,
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                },
                headers={"Authorization": "Bearer x"},
            )
            assert response.status_code == 200, response.text

            stop_event = asyncio.Event()
            task = asyncio.create_task(
                run_invoice_close_loop(test_settings, db_session_factory, stop_event)
            )
            # TWO rows: the route's, already committed, and the loop's.
            await _spin_until_async(
                lambda: _two_close_audit_rows(db_session_factory),
                seconds=15.0,
            )
            await _stop(stop_event, task)

        http_invoices = await _invoices_for(db_session_factory, http_tenant)
        loop_invoices = await _invoices_for(db_session_factory, loop_tenant)
        assert len(http_invoices) == 1, "the HTTP close minted nothing to compare against"
        assert len(loop_invoices) == 1, (
            "the loop minted nothing; it may have seen the period as already "
            "covered, in which case there is no second row to compare"
        )

        rows = await _audit_rows(db_session_factory, "billing.invoice.close")
        by_resource = {r.resource_id: r for r in rows}
        assert len(rows) == 2, f"expected two close audit rows (one per path), got {len(rows)}"

        http_row = by_resource[str(http_invoices[0].id)]
        loop_row = by_resource[str(loop_invoices[0].id)]

        # --- IDENTICAL across the two paths -------------------------------
        assert loop_row.action == http_row.action == "billing.invoice.close"
        assert loop_row.resource_type == http_row.resource_type == "invoice"
        expected_after_keys = ["invoice_number", "status"]
        assert sorted(loop_row.after) == expected_after_keys
        assert sorted(http_row.after) == expected_after_keys
        assert loop_row.before is None
        assert http_row.before is None

        # --- PRESENT on both, each naming its OWN subject ------------------
        assert loop_row.resource_id == str(loop_invoices[0].id)
        assert http_row.resource_id == str(http_invoices[0].id)
        assert loop_row.tenant_id == loop_tenant
        assert http_row.tenant_id == http_tenant
        assert loop_row.request_id and http_row.request_id
        assert loop_row.after["invoice_number"] == loop_invoices[0].invoice_number
        assert http_row.after["invoice_number"] == http_invoices[0].invoice_number

        # --- DELIBERATELY DIFFERENT: the actor, and only the actor ---------
        # This is the one axis on which the two paths must NOT agree, and it is
        # the only thing that lets a reader tell an automated close from a
        # human one.
        assert http_row.actor_sub == closer_pricing_writer["sub"]
        assert http_row.actor_role == "pricing-writer"
        assert loop_row.actor_sub == AUDIT_ACTOR_SUB
        assert loop_row.actor_role == AUDIT_ACTOR_ROLE
        assert loop_row.actor_sub != "unknown", (
            "audit.record's missing-actor fallback fired (services/audit.py:102) "
            "-- the loop handed it no actor at all"
        )
        assert loop_row.actor_sub != http_row.actor_sub

    @pytest.mark.unit
    async def test_audit_failure_fails_the_cycle_instead_of_being_swallowed(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """⛔ AN AUDIT FAILURE MUST NOT BE SILENT.

        At the three HTTP sites an ``audit.record`` failure propagates out of the
        handler, the route never reaches its ``commit()``, and the invoice write
        is ROLLED BACK -- audit failure ABORTS the operation. This loop cannot
        roll back an invoice ``close_due_periods`` already committed
        (``services/invoices.py:1687``), so the faithful behaviour is the next
        strongest one available: count it, log the invoices that need a row, and
        let the exception reach the loop's own ``except`` arms so the CYCLE fails
        and counts toward ``MAX_CONSECUTIVE_INVOICE_CLOSE_FAILURES``.

        ⛔ Mutant: wrap the ``_record_close_audit`` call in
        ``try/except Exception: logger.warning(...)`` -- i.e. treat it the way
        ``_measure_invoice_close_lag`` is deliberately treated.
        ``{reason="unexpected_error"}`` then stays put and this goes RED on that
        assertion. The ``audit_error`` assertion alone would NOT catch it, since
        that counter is incremented inside the helper either way -- which is the
        whole reason both are asserted here.
        """
        from services import audit as audit_module

        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650

        calls = {"n": 0}
        outcome = CloseDueOutcome(created=[_invoice("issued")], unchanged=[], refused=0, failed=0)

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("audit table is unreachable")

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr(
            "services.invoice_closer.close_due_periods", _one_shot_close(outcome, calls)
        )
        monkeypatch.setattr(audit_module, "record", _boom)

        audit_errors_before = _failed("audit_error")
        unexpected_before = _failed("unexpected_error")

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        # Spin on the LAST of the two increments, so neither assertion below can
        # read a half-finished except arm.
        await _spin_until(lambda: _failed("unexpected_error") > unexpected_before, seconds=5.0)
        await _stop(stop_event, task)

        assert _failed("audit_error") - audit_errors_before == 1.0, (
            "the audit write failed and nothing counted it on "
            'invoice_close_failures_total{reason="audit_error"}'
        )
        assert _failed("unexpected_error") - unexpected_before == 1.0, (
            "the audit failure was SWALLOWED: the cycle was still reported as "
            "successful, so a close with no provenance leaves no trace anywhere "
            "and never counts toward the leadership handover"
        )
        assert calls["n"] >= 1, "close_due_periods was never called; nothing was audited"

    @pytest.mark.unit
    async def test_unchanged_periods_are_not_re_audited(
        self, db_session_factory, test_settings, monkeypatch
    ):
        """An already-closed period must NOT get a second audit row per tick.

        Same defect shape as the phantom-``issued`` counter
        (``TestCloseOutcomeAccounting``), except written into an append-only
        table where it cannot be taken back: the sweep re-observes every closed
        period on every tick, so auditing ``outcome.unchanged`` would add a row
        per tenant per tick forever.

        Mutant: pass ``outcome.created + outcome.unchanged`` (or
        ``outcome.unchanged``) to ``_record_close_audit``. This goes RED.
        """
        calls = {"n": 0}
        outcome = CloseDueOutcome(
            created=[], unchanged=[_invoice("issued"), _invoice("issued")], refused=0, failed=0
        )

        test_settings.invoice_close_interval_seconds = 0.02
        test_settings.leader_retry_interval_seconds = 0.02
        test_settings.invoice_close_lag_days = -3650

        monkeypatch.setattr("services.invoice_closer.AsyncLeaderElection", _AlwaysLeader)
        monkeypatch.setattr(
            "services.invoice_closer.close_due_periods", _one_shot_close(outcome, calls)
        )

        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_invoice_close_loop(test_settings, db_session_factory, stop_event)
        )
        await _spin_until(lambda: calls["n"] >= 2, seconds=5.0)
        await _stop(stop_event, task)

        assert calls["n"] >= 2, "the sweep never ran twice; nothing was re-observed"
        rows = await _audit_rows(db_session_factory, "billing.invoice.close")
        assert rows == [], (
            f"{len(rows)} audit rows were written for a sweep that CREATED nothing "
            "-- an already-closed period is being re-audited on every tick"
        )


async def _two_close_audit_rows(session_factory) -> bool:  # noqa: ANN001
    return len(await _audit_rows(session_factory, "billing.invoice.close")) >= 2


# --------------------------------------------------------------------------- #
# 8. The reproduce() fields must survive the HTTP boundary
#
# `reproduce()` gained the seven frozen components plus money_matches_snapshot /
# money_mismatches. None of that reaches a caller unless InvoiceDocument
# DECLARES them: the model has no `model_config`, so Pydantic v2 defaults to
# extra='ignore' and routers/billing.py's `InvoiceDocument(**doc)` drops an
# undeclared key SILENTLY — a 200 whose body is missing the field, nothing in the
# logs, and a service-level test that passes because it never looks at HTTP.
#
# That is the whole reason this test exists at this layer. The service-level
# tests in tests/test_invoices.py assert reproduce()'s RETURN VALUE; they cannot
# see the envelope drop its claimed contents on the way out.
# --------------------------------------------------------------------------- #


class TestReproduceOverHttp:
    """The frozen components must be in the RESPONSE BODY, not just the dict."""

    #: Everything the digest/components work added to reproduce(). If a future
    #: field is added to the service and not to InvoiceDocument, add it here and
    #: watch this test name the gap.
    ADDED_FIELDS = (
        "llm_usage_usd",
        "llm_markup_usd",
        "seat_fee_usd",
        "seat_count",
        "excluded_unbillable_usd",
        "billing_mode",
        "issued_by",
        "money_matches_snapshot",
        "money_mismatches",
    )

    @pytest.mark.asyncio
    async def test_reproduce_serves_the_components_that_explain_the_total(
        self,
        app_client,  # noqa: ANN001
        db_session_factory,  # noqa: ANN001
        test_settings,  # noqa: ANN001
        closer_licences_answer_byo_mode,  # noqa: ANN001, ARG002
    ) -> None:
        """⭐ THE KILLING TEST for the envelope drop.

        Fails on an InvoiceDocument that does not declare the fields: the route
        returns 200 and the body simply lacks them, which is exactly how this
        defect hides.
        """
        import uuid

        from tests.conftest import T as _T

        period_start, period_end = _previous_calendar_month(datetime.now(UTC).date())
        tenant = _T("reproduce-http")
        licence = uuid.uuid4()

        await _seed_closable_month(
            db_session_factory,
            tenant_id=tenant,
            license_id=licence,
            period_start=period_start,
            period_end=period_end,
        )

        close_patch, gen_patch = _licence_payload_for(
            {
                tenant: [
                    {
                        "id": str(licence),
                        "app": {"key": "workers"},
                        "plan": {"code": "pro", "quota_monthly": None},
                    }
                ]
            }
        )

        with close_patch, gen_patch:
            closed = app_client.post(
                "/api/billing/invoices/close",
                json={
                    "tenant_id": tenant,
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                },
                headers={"Authorization": "Bearer x"},
            )
            assert closed.status_code == 200, closed.text
            invoice_id = closed.json()["data"]["id"]

            rendered = app_client.get(
                f"/api/billing/invoices/{invoice_id}/reproduce",
                headers={"Authorization": "Bearer x", "X-Tenant-Id": tenant},
            )

        assert rendered.status_code == 200, rendered.text
        body = rendered.json()["data"]

        missing = [f for f in self.ADDED_FIELDS if f not in body]
        assert not missing, (
            f"InvoiceDocument dropped {missing} on the way out. The service returns "
            "them; Pydantic's extra='ignore' silently removed them. Declare them on "
            "InvoiceDocument in src/models/domain.py."
        )

        # Not merely present — the integrity verdicts must be MEANINGFUL on an
        # untampered invoice, or a field could be declared and always null.
        assert body["digest_matches"] is True
        assert body["money_matches_snapshot"] is True
        assert body["money_mismatches"] == {}


# --------------------------------------------------------------------------- #
# 9. The lag census counts the population the close ATTEMPTS
#
# `_measure_invoice_close_lag` used to carry its own copy of "an active tenant is
# one with a billing_cost_daily or billing_metering_daily row in the period" —
# identical to `close_due_periods`' candidate set at the time, and identically
# blind: neither consulted `tenant_access_charge`. That is the worst arrangement
# available for a backlog gauge, because the instrument shared its subject's
# defect. A customer on a flat monthly access charge with no usage was not merely
# uncounted here; it COULD NOT be counted, so this gauge read a flat, healthy 0
# while that customer went unbilled every month and the platform overview
# reported their fee as revenue.
#
# Both definitions now come from `services.invoices.find_close_candidates`. The
# test below is what stops them being copies again: it seeds a tenant-period that
# only the ACCESS axis can see, and asserts the census surfaces it as lag.
# --------------------------------------------------------------------------- #


class TestAccessChargeOnlyPeriodIsCountedAsLag:
    """An unclosed period whose only basis is an access charge must read as lag.

    ⛔ THE PAIR IS ONE INSTRUMENT. The positive case cannot distinguish "the
    access axis reached the census" from "some unrelated period in this database
    is open" — the census scans 24 periods. The control is the SAME tenant with
    the SAME charge and an `issued` invoice covering the period: identical
    seeding, one row different, which pins the non-zero to the axis.

    Mutant: revert `_measure_invoice_close_lag` to `cost_tenants |
    metering_tenants` (or delete the access axis from `find_close_candidates`).
    `test_uncovered_access_charge_period_is_lag` goes RED with 0 — the tenant
    would not be in `active` at all, `active` would be EMPTY for that period, and
    the census would `continue` past it. The control stays GREEN under that
    mutant, which is what attributes the red to the axis.
    """

    @staticmethod
    async def _seed(session_factory, *, tenant_id: str, cover_with_invoice: bool) -> None:
        """A `monthly` access charge effective over the census period, no usage.

        ⚠️ NOT ONE `billing_cost_daily` OR `billing_metering_daily` ROW — that is
        the whole construction. `monthly` cadence so the charge bills in every
        month it is effective.

        ⛔ THE CHARGE IS WINDOWED TO THE CENSUS PERIOD ALONE, and this is a
        measured property of the census, not test tidiness. An OPEN-ENDED charge
        (`effective_until=None`, `effective_from` in 2025) is effective in EVERY
        one of the `_LAG_CENSUS_MAX_PERIODS` = 24 months the census walks back
        through, so it makes the tenant active in all 24 and the gauge reports the
        age of the OLDEST — measured here as 412 days rather than 19. That reading
        is not wrong (none of those months was ever invoiced either), but it would
        make the control below unable to read 0, and the pair is the instrument.
        `[2026-02-01, 2026-03-01)` qualifies for the census period only: at
        January's `bill_ts` the row has not started, and at March's its
        `effective_until` fails the strict `> bill_ts`.
        """
        from models.db import TenantAccessCharge

        async with session_factory() as session:
            session.add(
                TenantAccessCharge(
                    tenant_id=tenant_id,
                    service_key=None,
                    amount_usd=Decimal("5000.00"),
                    cadence="monthly",
                    effective_from=datetime(
                        _CENSUS_PERIOD_START.year, _CENSUS_PERIOD_START.month, 1, tzinfo=UTC
                    ),
                    effective_until=datetime(
                        _CENSUS_PERIOD_END.year, _CENSUS_PERIOD_END.month, 1, tzinfo=UTC
                    ),
                )
            )
            if cover_with_invoice:
                session.add(
                    BillingInvoice(
                        tenant_id=tenant_id,
                        tenant_name="acme",
                        period_start=_CENSUS_PERIOD_START,
                        period_end=_CENSUS_PERIOD_END,
                        status="issued",
                        invoice_number="INV-2026-access-covered",
                        subtotal_usd=Decimal("5000.00000000"),
                        total_usd=Decimal("5000.00000000"),
                        internal_cost_usd=Decimal("0"),
                        margin_usd=Decimal("5000.00000000"),
                        billing_mode="standard_markup",
                        markup_pct=Decimal("30.00"),
                        effective_markup_pct=Decimal("30.0000"),
                        snapshot={},
                        inputs={},
                        snapshot_sha256="0" * 64,
                        generator_version="invoice.v1",
                        issued_at=datetime(2026, 3, 4, tzinfo=UTC),
                    )
                )
            await session.commit()

    @pytest.mark.unit
    async def test_uncovered_access_charge_period_is_lag(self, db_session_factory, test_settings):
        """Zero usage, an open $5,000 term, no invoice → the census reports lag."""
        test_settings.invoice_close_lag_days = 3

        await self._seed(
            db_session_factory, tenant_id=T("census-access-open"), cover_with_invoice=False
        )

        lag = await _measure_invoice_close_lag(
            db_session_factory, settings=test_settings, now=_CENSUS_NOW
        )

        assert lag == _EXPECTED_LAG_DAYS, (
            "an unbilled tenant-period whose only basis is an access charge did not "
            f"surface as lag: the census returned {lag}, expected {_EXPECTED_LAG_DAYS}. "
            "0 here means the census still defines an active tenant by usage rows "
            "alone — the same blindness the close had, so the hole is invisible in "
            "both the money and the gauge"
        )

    @pytest.mark.unit
    async def test_covered_access_charge_period_is_not_lag(self, db_session_factory, test_settings):
        """THE CONTROL. Identical seeding plus an ``issued`` invoice → 0.

        Without this the case above is satisfied by any open period anywhere in
        the 24-month scan window. A red HERE means the census's coverage read (or
        the fixture) is wrong, not its notion of who is active.
        """
        test_settings.invoice_close_lag_days = 3

        await self._seed(
            db_session_factory, tenant_id=T("census-access-covered"), cover_with_invoice=True
        )

        lag = await _measure_invoice_close_lag(
            db_session_factory, settings=test_settings, now=_CENSUS_NOW
        )

        assert lag == 0, (
            "an ISSUED invoice failed to cover the access-charge tenant's own period "
            f"— the census returned {lag}. The paired non-zero above therefore cannot "
            "be attributed to the access axis"
        )
