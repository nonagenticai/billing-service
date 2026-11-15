"""Reservation IDENTITY: a settlement may only release what it reserved (#178).

billing-service#178. The in-flight reservation pool is ONE per-tenant integer
(``tokens:tenant:<id>:reserved``), so a reservation has no identity and a
settlement is an anonymous ``DECRBY``. A settlement that releases MORE than its
own reservation therefore takes the excess out of OTHER callers' live
reservations, and the clamp-to-0 in :data:`services.budget_redis._RELEASE_LUA`
hides it: the next pre-flight check sees a pool that is too small and ADMITS
spend it should have denied. The cap fails OPEN by exactly the overshoot.

Measured in prod: **329 reserves/hour against 151.6 harmful over-releases/hour**
(``prior_pool > 0``, i.e. a live pool drained past its end — not the benign
~90% of debits that never reserved at all).

The SETTLEMENT half of the fix is behind
``get_settings().budget_require_reservation_handle``:

- **False — mode A** (the default, and what runs today): the anonymous pooled
  drain, unchanged down to the counters and the clamp.
- **True — mode B**: a settlement releases ONLY the holds it names by handle,
  each for its FULL reserved amount, idempotently. Tokens attributed to no
  handle NEVER touch the pool — they are counted on
  ``billing_budget_release_without_reserve_total`` and dropped. That single
  branch is what makes ``billing_budget_reserve_over_release_total``
  structurally unreachable.

⭐ **THE MINT IS NO LONGER PART OF THAT SWITCH**, and this docstring used to say
it was ("mode B: ``reserve_budget`` mints a handle per hold"). The flag gated
three things at once — the mint, the index, and the release routing — which made
handle coverage impossible to observe until the moment of arming. ``reserve_budget``
now mints and records a hold on EVERY admitted reserve, in both modes; only the
release branch is still selected by the flag. ``TestReservationHandleMintIsNotFlagGated``
at the bottom of this file pins that separation, including the proof that mode A's
money behaviour did not move.

⭐ **TEST 1 step 5 is the assertion that makes this file a PROOF rather than a
description.** Steps 1-4 only observe that the pool still holds B's tokens; a
reader could dismiss that as bookkeeping. Step 5 asserts the CAP CONSEQUENCE —
a third caller is REFUSED in mode B and is ADMITTED in mode A (TEST 2, the
deliberate characterisation of the defect). Without step 5 this file would
describe two different pool arithmetics; with it, it measures the money.

Conventions copied from ``tests/test_money_bug_fixes.py``: the ``fake_redis``
fixture, the ``T(...)`` tenant-id helper, ``make_budget`` via a local
``_budget``, counters read as a DELTA around the call (they are module-level
``Counter`` objects and other tests in the same session move them), and
assertions only on observable state — pool value, ZSET/HASH membership, counter
deltas, ``allowed``/``reason`` — never on log text.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import TokenBudget
from services.budget_redis import (
    _tagged_reserved_key,
    release_reservation,
    reservation_amounts_key,
    reservation_index_key,
    reserve_budget,
    reserved_key,
)
from settings import get_settings
from tests._budget_factory import make_budget
from tests.conftest import T


def _budget(*, tenant: str, cap: int, consumed: int = 0) -> TokenBudget:
    """Active monthly budget whose consumption belongs to THIS period.

    Same shape as ``tests/test_money_bug_fixes.py::_budget`` for the non-rollover
    case: the factory's default ``consumed_period_start`` anchors consumption to
    ``now``'s period, so ``effective_consumed`` returns ``consumed`` verbatim and
    these tests cannot go red on the 1st of a month.
    """
    return make_budget(tenant=tenant, cap=cap, consumed=consumed, period="monthly", source="test")


def _arm_reservation_handles(monkeypatch, *, enabled: bool) -> None:
    """Select the reservation protocol for ONE test.

    ``get_settings`` is ``lru_cache``d, so setting the env var alone does
    nothing. The env + ``get_settings.cache_clear()`` idiom is preferred here over
    patching a cached ``Settings`` instance because ``budget_redis`` reads the
    flag through its own module-level ``get_settings`` name in THREE places
    (``release_reservation``, ``reserve_budget``), and conftest's autouse
    ``_clear_settings_cache`` already rebuilds settings around every test, so the
    environment is the live mechanism rather than a stale object.
    """
    monkeypatch.setenv("BUDGET_REQUIRE_RESERVATION_HANDLE", "true" if enabled else "false")
    get_settings.cache_clear()
    assert get_settings().budget_require_reservation_handle is enabled, (
        "the settings override did not take effect — get_settings() is cached and "
        "the flag still reads the old value, so this test would silently measure "
        "the OTHER protocol"
    )


def _counters() -> dict[str, float]:
    """Every reservation-protocol counter, right now.

    Read BEFORE and AFTER and assert the DELTA: these are module-level
    ``prometheus_client.Counter`` objects shared across the whole session, so an
    absolute value is not a safe assertion.
    """
    from observability import metrics as observability_metrics

    return {
        "over_release": observability_metrics.budget_reserve_over_release_total._value.get(),
        "without_reserve": observability_metrics.budget_release_without_reserve_total._value.get(),
        "minted": observability_metrics.budget_reservation_minted_total._value.get(),
        "settled": observability_metrics.budget_reservation_settled_total._value.get(),
        "expired": observability_metrics.budget_reservation_expired_total._value.get(),
    }


async def _pool(fake_redis, tenant_id: str, *, tagged: bool) -> int:
    """The in-flight pool under whichever key the active protocol writes.

    ⛔ The two protocols use DIFFERENT keys on purpose: mode A writes the
    untagged :func:`reserved_key`, mode B the hash-tagged
    :func:`_tagged_reserved_key` (the Redis Cluster tag that lets one ``EVAL``
    touch pool + index + amounts). Reading the wrong one would report 0 and make
    every pool assertion below vacuously pass.
    """
    key = _tagged_reserved_key(tenant_id) if tagged else reserved_key(tenant_id)
    return int(await fake_redis.get(key) or 0)


async def _commit_consumed(db_session: AsyncSession, budget: TokenBudget, tokens: int) -> None:
    """Commit ``tokens`` of real spend, the way a settled debit does.

    ``reserve_budget`` re-reads committed spend from the DB on EVERY call
    (``effective_consumed`` -> ARGV[1]), so the cap arithmetic in step 5 of TEST
    1 needs this column to actually carry 900. Written directly rather than via
    ``debit_budget`` so the test exercises ONE settlement — ``debit_budget``
    calls ``release_reservation`` itself with no handles, which in mode B counts
    an unattributed release and would confound the counter deltas this file
    asserts on.
    """
    budget.consumed_tokens = Decimal(tokens)
    await db_session.commit()


@pytest.mark.unit
class TestBudgetReservationIdentity:
    # ----------------------------------------------------------------------
    # TEST 1 — the decisive one
    # ----------------------------------------------------------------------

    async def test_settlement_cannot_drain_another_callers_reservation(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """A settlement must not spend another caller's headroom.

        IN PRODUCTION IF THIS FAILS: the spend cap stops enforcing. This is the
        exact trace recorded above ``_RELEASE_LUA`` and measured at 151.6
        harmful over-releases/hour — caller A's settlement deletes the shared
        pool, B's 400 in-flight tokens vanish from the gate's view, and the next
        caller is admitted at a true position of 1400 against a cap of 1000.
        Every token admitted past the cap is unbilled-against-plan spend with no
        refund path, and nothing in the response says it happened.

        ⭐ THE TWO OUTCOMES, stated so a reader sees exactly what changed:
          * mode B (here): pool keeps B's 400, and the third reserve is DENIED
            ``over_budget`` because 900 + 400 + 100 = 1400 > 1000.
          * mode A (``test_mode_a_still_fails_open_and_is_the_documented_baseline``):
            pool is WIPED to 0, and the third reserve is ADMITTED because the
            gate only sees 900 + 0 + 100 = 1000 <= 1000.
        Step 5 below is the assertion that separates a fix from a description.
        """
        _arm_reservation_handles(monkeypatch, enabled=True)
        tenant = T("resv-identity")
        budget = _budget(tenant=tenant, cap=1000)
        db_session.add(budget)
        await db_session.commit()

        # 1. caller A reserves 400
        a = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=400,
            ttl_seconds=300,
        )
        assert a["allowed"] is True, a
        handle_a = a["reservation_id"]
        assert handle_a, "mode B must mint a reservation_id on a successful reserve"

        # 2. caller B reserves 400 -> pool is now 800
        b = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=400,
            ttl_seconds=300,
        )
        assert b["allowed"] is True, b
        handle_b = b["reservation_id"]
        assert handle_b and handle_b != handle_a, "two holds must not share one identity"
        assert await _pool(fake_redis, tenant, tagged=True) == 800

        # 3. A's request really cost 900 (an UNDER-estimate) and settles.
        await _commit_consumed(db_session, budget, 900)
        counters_before = _counters()
        remaining = await release_reservation(fake_redis, tenant, 900, handles={handle_a: 900})
        counters_after = _counters()

        # 4. ⭐ B'S RESERVATION SURVIVES. A's hold is dropped for its own FULL
        #    reserved 400 and nothing more, even though the settlement names 900
        #    against it: the per-handle VALUE only says how much of `tokens` was
        #    attributed at all, it is NEVER the amount subtracted from the pool.
        assert remaining == 400, (
            f"settling A's 400-token hold at an actual 900 left the pool at {remaining}; "
            "B's live 400-token reservation was drained to pay for A's under-estimate"
        )
        assert await _pool(fake_redis, tenant, tagged=True) == 400
        assert await fake_redis.hexists(reservation_amounts_key(tenant), handle_a) == 0, (
            "A's settled hold is still in the amounts hash — a second settlement "
            "could discharge it again"
        )
        assert await fake_redis.zscore(reservation_index_key(tenant), handle_a) is None
        assert await fake_redis.hget(reservation_amounts_key(tenant), handle_b) == "400", (
            "B's hold lost its amount, so the next sweep cannot return B's tokens"
        )
        assert await fake_redis.zscore(reservation_index_key(tenant), handle_b) is not None
        assert counters_after["settled"] - counters_before["settled"] == 1
        assert counters_after["without_reserve"] == counters_before["without_reserve"], (
            "this settlement attributed all 900 of its tokens to handle_a, so NOTHING "
            "here is unattributed and the release-without-reserve series must stay "
            "flat; counting it would make the series mean two different things. The "
            "unattributed case is "
            "`test_tokens_with_no_handle_never_touch_the_pool`"
        )
        assert counters_after["over_release"] == counters_before["over_release"], (
            "the identified path must NOT write billing_budget_reserve_over_release_total "
            "— that series reading a flat zero IS the acceptance measurement for the flip"
        )

        # 5. ⭐ THE CAP NOW REFUSES. This is the money assertion: 900 committed +
        #    400 still in flight + 100 requested = 1400 > cap 1000.
        third = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=100,
            ttl_seconds=300,
        )
        assert third["allowed"] is False, (
            "the gate ADMITTED a third caller at a true position of 1400 against a "
            "cap of 1000 — the cap failed open by exactly the reservation a previous "
            "settlement drained. This is the defect, not a rounding detail"
        )
        assert third["reason"] == "over_budget"
        assert third["reservation_id"] is None, (
            "a REFUSED reserve handed back a handle; a settlement against a hold "
            "that was never created is how tokens nobody reserved get released"
        )
        assert third["consumed_tokens"] == 900
        assert third["in_flight_reserved_tokens"] == 400

    # ----------------------------------------------------------------------
    # TEST 2 — characterisation of the PRE-FIX behaviour mode A retains
    # ----------------------------------------------------------------------

    async def test_mode_a_still_fails_open_and_is_the_documented_baseline(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """⚠️ DELIBERATE CHARACTERISATION OF THE DEFECT — NOT AN ENDORSEMENT.

        Everything asserted here is the BROKEN behaviour, pinned on purpose.
        ``budget_require_reservation_handle`` defaults to False, so this is what
        runs in production today and what the 151.6 harmful over-releases/hour
        were measured against. It is kept green for two reasons: the flip has to
        be reversible at low traffic (arming or disarming strands one pool for up
        to one TTL window), and a test that passes under BOTH modes would prove
        nothing — this is the half that shows the two protocols genuinely differ.

        ⛔ Do not "fix" this test to assert the safe outcome. If mode A ever
        starts refusing the third caller, the flag has stopped selecting
        anything and TEST 1 is no longer measuring the fix.

        IN PRODUCTION IF THIS FAILS: either the default protocol changed without
        the flag (an unannounced behaviour change to every live pool), or the
        flag is no longer read at all.
        """
        _arm_reservation_handles(monkeypatch, enabled=False)
        tenant = T("resv-mode-a")
        budget = _budget(tenant=tenant, cap=1000)
        db_session.add(budget)
        await db_session.commit()

        for _ in range(2):
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=tenant,
                estimated_tokens=400,
                ttl_seconds=300,
            )
            assert r["allowed"] is True, r
            # ⭐ CHANGED DELIBERATELY. This used to assert `reservation_id is
            # None` on the grounds that "mode A must mint NO handle — a handle
            # there could only be a name the caller supplied, with no index to
            # look it up in". That gating is the MEASURE-BEFORE-ARMING DEADLOCK
            # this file's subject now fixes: it made handle coverage unobservable
            # until the flag that changes SETTLEMENT was already armed. Mode A now
            # mints and records a real hold (`_RESERVE_SHADOW_LUA`) and there IS
            # an index to look it up in. What must not change is the MONEY, and
            # every money assertion below is untouched.
            assert r["reservation_id"], (
                "mode A minted no handle, so handle coverage is once again "
                "unmeasurable until the settlement flag is armed — the deadlock"
            )
        assert await _pool(fake_redis, tenant, tagged=False) == 800, (
            "mode A's GATE must still admit against the untagged pool, unchanged"
        )
        # ⭐ The tagged pool is now written in mode A as well — as a SHADOW ledger
        # that nothing consults. "The two protocols must not share a pool" is
        # therefore asserted on what it actually means: mode A's verdicts must be
        # taken against the untagged pool alone. The proof is at the end of this
        # test, where the third caller is ADMITTED even though the tagged pool
        # plainly still shows 800 in flight — if the gate were reading it, that
        # caller would have been denied. That is a strictly stronger check than
        # the old `tagged == 0`, which could also have passed on a mode A that
        # read the tagged pool and found it empty.
        assert await _pool(fake_redis, tenant, tagged=True) == 800, (
            "the shadow ledger did not record mode A's two holds; without it "
            "there is nothing to measure handle coverage against before arming"
        )

        await _commit_consumed(db_session, budget, 900)
        counters_before = _counters()
        remaining = await release_reservation(fake_redis, tenant, 900)
        counters_after = _counters()

        # The pool is WIPED: 800 - 900 clamps to 0 and the key is deleted, so B's
        # 400 in-flight tokens are gone from the gate's view.
        assert remaining == 0
        assert await fake_redis.exists(reserved_key(tenant)) == 0, (
            "the anonymous drain is supposed to clamp to 0 and DELETE; if it stopped "
            "doing that, mode A's documented behaviour changed"
        )
        assert counters_after["over_release"] - counters_before["over_release"] == 1, (
            "the fail-open is at least still MEASURABLE in mode A — that counter is "
            "the only thing standing between this defect and invisibility"
        )
        assert counters_after["settled"] == counters_before["settled"]

        # ⭐ THE FAIL-OPEN, asserted: 900 committed + 0 visible in flight + 100
        #    requested = 1000 <= cap, so this is ADMITTED while B's 400 is still
        #    outstanding. True position 1400 against a cap of 1000.
        third = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=100,
            ttl_seconds=300,
        )
        assert third["allowed"] is True, (
            "mode A unexpectedly DENIED the third caller. That is the safe answer, "
            "but it is not mode A's documented behaviour — see TEST 1 for the mode "
            "that is supposed to produce it"
        )
        assert third["in_flight_reserved_tokens"] == 100, (
            "B's 400 should be invisible to the gate here; that invisibility IS the bug"
        )
        # ⭐ AND THE PROOF THAT THE SHADOW LEDGER CHANGED NO VERDICT. The tagged
        # pool has been carrying 800 throughout — through the settlement that
        # wiped the untagged one and through this admission. If mode A's gate
        # consulted it at all, this third caller could not have been admitted.
        assert await _pool(fake_redis, tenant, tagged=True) == 900, (
            "the shadow pool was drained by a mode A settlement; it must be written "
            "by reserves and swept by expiry only, never touched by `_RELEASE_LUA`"
        )

    # ----------------------------------------------------------------------
    # TEST 3 — expiry is what keeps mode B from becoming a 429 storm
    # ----------------------------------------------------------------------

    async def test_an_expired_reservation_cannot_inflate_the_pool(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """An unsettled hold must stop holding headroom once it expires.

        IN PRODUCTION IF THIS FAILS: mode B becomes a permanent 429 storm, which
        is WORSE than the defect it fixes. Under the pooled protocol the key's
        own TTL was the release path for a hold whose request died. Mode B
        demotes that TTL to a whole-structure GC and makes the per-hold expiry in
        the ZSET score the real release — so if the sweep in
        ``_RESERVE_HANDLE_LUA`` stops reclaiming, one lost settlement holds that
        tenant's headroom FOREVER and every later request is denied
        ``over_budget`` while committed spend is ~0. That is the exact
        fail-CLOSED incident ``release_reservation`` was written to end.

        The expiry is forced by rewriting the handle's ZSET SCORE into the past
        rather than sleeping: the score IS the expiry clock the sweep reads
        (``ZRANGEBYSCORE index -inf now``), and a real sleep would have to be
        longer than the TTL of three live keys.
        """
        _arm_reservation_handles(monkeypatch, enabled=True)
        tenant = T("resv-expiry")
        db_session.add(_budget(tenant=tenant, cap=1000))
        await db_session.commit()

        first = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=400,
            ttl_seconds=5,
        )
        assert first["allowed"] is True, first
        handle = first["reservation_id"]
        assert await _pool(fake_redis, tenant, tagged=True) == 400
        assert await fake_redis.hget(reservation_amounts_key(tenant), handle) == "400"

        # Its request died: nothing will ever settle this hold. Age it out by
        # moving its scored expiry to unix second 1 — strictly in the past for
        # any clock the sweep can read.
        await fake_redis.zadd(reservation_index_key(tenant), {handle: 1})
        score = await fake_redis.zscore(reservation_index_key(tenant), handle)
        assert score is not None and score < datetime.now(UTC).timestamp(), (
            "precondition: the hold must be scored in the past for the sweep to see it"
        )

        counters_before = _counters()
        # ⭐ 700 is chosen so this reserve is only admissible IF the sweep ran:
        #    with the stale 400 still pooled the gate would see 0 + 400 + 700 =
        #    1100 > 1000 and refuse. The admission is the measurement.
        second = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=700,
            ttl_seconds=300,
        )
        counters_after = _counters()

        assert second["allowed"] is True, (
            "a reservation that expired unsettled is STILL holding headroom — the "
            f"gate refused 700 of a 1000 cap at consumed=0 ({second}). Mode B has "
            "no other release path, so this denial is permanent for this tenant"
        )
        assert second["in_flight_reserved_tokens"] == 700, (
            "the expired 400 is still counted in the pool the gate compares to the cap"
        )
        assert await _pool(fake_redis, tenant, tagged=True) == 700
        assert counters_after["expired"] - counters_before["expired"] == 1, (
            "budget_reservation_expired_total did not move; minted - (settled + "
            "expired) is the only way to see handles being stranded, and a silent "
            "sweep breaks that budget"
        )
        # The reclaimed hold must be gone from BOTH structures, or the next sweep
        # re-subtracts the same tokens and the pool goes permanently negative.
        assert await fake_redis.zscore(reservation_index_key(tenant), handle) is None
        assert await fake_redis.hexists(reservation_amounts_key(tenant), handle) == 0

    # ----------------------------------------------------------------------
    # TEST 4 — idempotency is REQUIRED, not a nicety
    # ----------------------------------------------------------------------

    async def test_re_settling_the_same_handle_is_a_harmless_no_op(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """Settlements 2..N of one hold must move nothing.

        IN PRODUCTION IF THIS FAILS: provider failover double-releases. ONE
        request can emit N cost events — ``inference-router``'s
        ``router_service.py`` emits one per attempt — while reserving exactly
        ONCE, so the 2nd..Nth settlements are real, routine traffic. If the 2nd
        one subtracted the hold's amount again it would take those tokens out of
        a DIFFERENT caller's live reservation, which is the very defect #178
        fixes, re-entered through the duplicate-event door.

        Two live holds on purpose: with only one, the pool would already be 0
        after the first settlement and a second subtraction would be invisible
        behind the clamp. B's 300 is the witness that the pool "goes no lower".
        """
        _arm_reservation_handles(monkeypatch, enabled=True)
        tenant = T("resv-idem")
        db_session.add(_budget(tenant=tenant, cap=1000))
        await db_session.commit()

        holds = []
        for amount in (400, 300):
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=tenant,
                estimated_tokens=amount,
                ttl_seconds=300,
            )
            assert r["allowed"] is True, r
            holds.append(r["reservation_id"])
        handle_a, handle_b = holds
        assert await _pool(fake_redis, tenant, tagged=True) == 700

        counters_before = _counters()
        first = await release_reservation(fake_redis, tenant, 400, handles={handle_a: 400})
        after_first = await _pool(fake_redis, tenant, tagged=True)
        second = await release_reservation(fake_redis, tenant, 400, handles={handle_a: 400})
        after_second = await _pool(fake_redis, tenant, tagged=True)
        counters_after = _counters()

        assert first == 300 and after_first == 300, (
            f"the first settlement left the pool at {after_first}, not B's 300"
        )
        assert second == 300, f"the re-settlement returned {second}, not the unchanged 300"
        assert after_second == after_first == 300, (
            f"re-settling the same handle moved the pool {after_first} -> {after_second}; "
            "a duplicate cost event from a provider failover just spent B's reservation"
        )
        assert await fake_redis.hget(reservation_amounts_key(tenant), handle_b) == "300", (
            "B's hold was collateral damage of A's duplicate settlement"
        )
        assert counters_after["settled"] - counters_before["settled"] == 1, (
            "the pool moved once but TWO settlements were counted (or none were); "
            "HDEL's own return value must be the authority on which call found the hold"
        )
        assert counters_after["over_release"] == counters_before["over_release"]
        assert counters_after["without_reserve"] == counters_before["without_reserve"], (
            "a fully-attributed settlement was filed as having no reservation"
        )

    # ----------------------------------------------------------------------
    # TEST 5 — the DOMINANT path: tokens with no handle at all
    # ----------------------------------------------------------------------

    async def test_tokens_with_no_handle_never_touch_the_pool(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """Unattributed settlement tokens must be counted and DROPPED.

        IN PRODUCTION IF THIS FAILS: #178 is not fixed at all, because this is
        where the damage actually comes from. ``reserve_budget`` has ONE caller
        (``POST /budget/reserve``) while ``release_reservation`` runs on EVERY
        debit, so ~90% of settlements carry no reservation whatsoever — and
        today ``release_budget_reservations`` passes NO handles at all, because
        license-authz does not yet round-trip ``reservation_id``. This is the
        common case, not an edge case. If an unattributed token can reduce the
        pool by even one, every one of those settlements is still free to drain a
        live reservation and the cap still fails open.

        Both shapes are exercised: no handle at all, and a handle whose
        ``tokens`` total exceeds what it reserved.
        """
        _arm_reservation_handles(monkeypatch, enabled=True)
        tenant = T("resv-nohandle")
        db_session.add(_budget(tenant=tenant, cap=1000))
        await db_session.commit()

        holds = []
        for amount in (400, 300):
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=tenant,
                estimated_tokens=amount,
                ttl_seconds=300,
            )
            assert r["allowed"] is True, r
            holds.append(r["reservation_id"])
        handle_a, handle_b = holds
        assert await _pool(fake_redis, tenant, tagged=True) == 700

        # (a) Nothing named: the ordinary debit-path settlement today.
        before = _counters()
        remaining = await release_reservation(fake_redis, tenant, 500, handles={})
        after = _counters()

        assert remaining == 700 and await _pool(fake_redis, tenant, tagged=True) == 700, (
            "500 unattributed tokens reduced the pool. Under the pooled protocol that "
            "is exactly how a settlement came to drain other callers' live holds, and "
            "this path runs on ~90% of debits"
        )
        assert after["without_reserve"] - before["without_reserve"] == 1, (
            "an unattributed settlement was not counted on "
            "billing_budget_release_without_reserve_total"
        )
        assert after["over_release"] == before["over_release"], (
            "the benign no-reservation class was filed on the CRITICAL series again — "
            "that is the ~90%-of-debits firing the split exists to end"
        )
        assert after["settled"] == before["settled"]

        # (b) A handle PLUS a larger `tokens`: 900 settled of which only 400 is
        #     attributed to A's hold, so 500 is unattributed. A's hold goes for
        #     its own full 400 and the 500 excess still never reaches the pool.
        #     B's 300 is the witness — mode A would have wiped the whole 700 here.
        before = _counters()
        remaining = await release_reservation(fake_redis, tenant, 900, handles={handle_a: 400})
        after = _counters()

        assert remaining == 300 and await _pool(fake_redis, tenant, tagged=True) == 300, (
            f"settling a 400-token hold at an actual 900 left the pool at {remaining}; "
            "the 500-token excess was taken out of B's live reservation"
        )
        assert await fake_redis.hget(reservation_amounts_key(tenant), handle_b) == "300"
        assert after["without_reserve"] - before["without_reserve"] == 1, (
            "the 500 tokens attributed to no handle were not counted on "
            "billing_budget_release_without_reserve_total; they must be reported, "
            "not silently dropped"
        )
        assert after["settled"] - before["settled"] == 1
        assert after["over_release"] == before["over_release"]

    # ----------------------------------------------------------------------
    # TEST 6 — negative control: the admission arithmetic is UNCHANGED
    # ----------------------------------------------------------------------

    async def test_a_reserve_still_admits_within_cap(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """CONTROL: a reservation that fits must still be GRANTED in mode B.

        IN PRODUCTION IF THIS FAILS: the fix is a global outage rather than a
        fix. Every assertion in this file is about REFUSING or about not moving
        the pool, and all of them would pass trivially against a mode B that
        denies everything. The admission test in ``_RESERVE_HANDLE_LUA`` is
        byte-for-byte the one in ``_RESERVE_LUA`` — ``cap >= 0 and (consumed +
        pool + req) > cap``, with ``consumed`` still DB-authoritative — so
        identity changes the SETTLEMENT, never who gets in.
        """
        _arm_reservation_handles(monkeypatch, enabled=True)
        tenant = T("resv-control")
        db_session.add(_budget(tenant=tenant, cap=1000, consumed=250))
        await db_session.commit()

        before = _counters()
        r = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=250,
            ttl_seconds=300,
        )
        after = _counters()

        assert r["allowed"] is True, f"a reservation well inside the cap was refused: {r}"
        assert r["reason"] is None
        assert r["consumed_tokens"] == 250
        assert r["reserved_tokens"] == 250
        assert r["in_flight_reserved_tokens"] == 250
        assert r["cap_tokens"] == 1000
        assert r["remaining_tokens"] == 500

        rid = r["reservation_id"]
        assert isinstance(rid, str) and rid, (
            "mode B granted a reservation with no identity; the settlement would have "
            "nothing to name and would fall back to draining the shared pool"
        )
        # A uuid4 minted in Python (never in Lua -- a script that invents its own
        # identifier is non-deterministic and unsafe to replicate/replay). Parsed
        # with `hex=`, which accepts both the dashed and undashed spellings, so
        # this assertion is about PROVENANCE only; the canonical-form requirement
        # is pinned separately by
        # `TestReservationHandleMintIsNotFlagGated::test_the_minted_handle_survives_the_producer_round_trip`.
        assert uuid.UUID(hex=rid).version == 4, f"reservation_id is not a uuid4: {rid!r}"

        # The hold is really recorded, in both structures, or nothing can settle
        # or expire it later.
        assert await _pool(fake_redis, tenant, tagged=True) == 250
        assert await fake_redis.hget(reservation_amounts_key(tenant), rid) == "250"
        assert await fake_redis.zscore(reservation_index_key(tenant), rid) is not None
        assert after["minted"] - before["minted"] == 1
        assert after["expired"] == before["expired"], (
            "a fresh tenant's first reserve reclaimed something; the sweep is seeing "
            "holds that are not expired"
        )


@pytest.mark.unit
class TestReservationHandleMintIsNotFlagGated:
    """The MEASURE-BEFORE-ARMING DEADLOCK, and the proof it is gone.

    ``budget_require_reservation_handle`` used to gate THREE things at once:
    minting the handle, maintaining the handle index, and routing settlement
    through ``_release_by_handle``. The consequence was circular — the only
    question worth answering before arming ("are handles actually being minted,
    recorded, and round-tripped by producers?") could be answered ONLY by arming,
    and arming flipped three structures simultaneously on a live money path.
    ``billing_budget_reservation_minted_total`` read a flat **0** in prod for
    exactly this reason: the increment sat inside ``if require_handle:``, so the
    series measured the flag rather than the traffic.

    The separation: mint and bookkeeping run on EVERY admitted reserve
    (``_RESERVE_SHADOW_LUA``, whose keys the pooled gate never reads); only the
    RELEASE branch stays behind the flag. These tests pin all three halves of
    that, each with a negative control:

    - ``test_a_handle_is_minted_and_recorded_with_the_flag_off`` — fails if
      minting ever goes back behind the flag. Control: a REFUSED reserve still
      mints nothing.
    - ``test_the_pooled_release_path_is_unchanged_with_the_flag_off`` — the money
      assertion. Control: the identical call with the flag ARMED behaves
      differently, so the flag is still selecting something.
    - ``test_the_by_handle_path_still_settles_when_armed`` — the armed path is
      intact. Control: naming an unknown handle moves nothing.
    - ``test_the_shadow_index_is_swept_and_cannot_grow_with_the_flag_off`` —
      unconditional bookkeeping must not become an unbounded key.
    - ``test_the_minted_handle_survives_the_producer_round_trip`` — the handle's
      FORM. Control: the previous ``uuid4().hex`` spelling fails the same trip.
    """

    async def test_a_handle_is_minted_and_recorded_with_the_flag_off(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """⭐ THE DEADLOCK TEST. A reserve must mint while the flag is OFF.

        IN PRODUCTION IF THIS FAILS: nobody can size the handle population before
        arming, so the flip becomes exactly what it was before — an unmeasured
        three-structure change on the path that decides whether spend is admitted.
        This is the assertion that goes red if a later edit "tidies" the mint back
        inside ``if require_handle:``.

        The hold must be RECORDED, not merely named: a ``reservation_id`` with no
        entry in the index and amounts keys is a name a settlement could later
        claim against nothing, and it would inflate the mint counter with holds
        that do not exist — an instrument reporting coverage it does not have.
        """
        _arm_reservation_handles(monkeypatch, enabled=False)
        tenant = T("mint-unconditional")
        db_session.add(_budget(tenant=tenant, cap=1000))
        await db_session.commit()

        before = _counters()
        granted = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=400,
            ttl_seconds=300,
        )
        after = _counters()

        assert granted["allowed"] is True, granted
        rid = granted["reservation_id"]
        assert isinstance(rid, str) and rid, (
            "a successful reserve returned NO handle while "
            "budget_require_reservation_handle is False. Minting is back behind "
            "the flag, and handle coverage is unmeasurable until the flag that "
            "changes settlement is armed — the deadlock this test exists to catch"
        )
        assert after["minted"] - before["minted"] == 1, (
            "billing_budget_reservation_minted_total did not move on a real mint. "
            "It measured the FLAG rather than the traffic before this change (a "
            "flat 0 in prod against live reserve volume); if it stops counting "
            "again there is no denominator for coverage"
        )
        # RECORDED, in both structures, or nothing can settle or expire it later.
        assert await fake_redis.hget(reservation_amounts_key(tenant), rid) == "400", (
            "the hold's amount was not recorded, so an armed settlement naming this "
            "handle would find nothing and the census would over-count coverage"
        )
        assert await fake_redis.zscore(reservation_index_key(tenant), rid) is not None, (
            "the hold carries no expiry, so the sweep can never reclaim it"
        )
        # ⭐ AND THE GATE'S OWN POOL IS UNTOUCHED BY ANY OF IT.
        assert await _pool(fake_redis, tenant, tagged=False) == 400, (
            "the untagged pool — the only one `_RESERVE_LUA` admits against while "
            "the flag is off — does not hold exactly this reserve"
        )

        # ⭐ NEGATIVE CONTROL: a REFUSED reserve must still mint nothing. Without
        #    this, a mint that fired unconditionally on entry (before the verdict)
        #    would pass every assertion above while handing out identities for
        #    tokens the gate declined.
        control_before = _counters()
        refused = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=900,  # 0 consumed + 400 in flight + 900 > cap 1000
            ttl_seconds=300,
        )
        control_after = _counters()

        assert refused["allowed"] is False and refused["reason"] == "over_budget", refused
        assert refused["reservation_id"] is None, (
            "a REFUSED reserve handed back a handle. Nothing was held, so a "
            "settlement naming it would discharge a hold that does not exist"
        )
        assert control_after["minted"] == control_before["minted"], (
            "a refusal was counted as a mint; the coverage denominator now includes "
            "reservations that were never granted"
        )
        assert await fake_redis.zcard(reservation_index_key(tenant)) == 1, (
            "the refused reserve was recorded in the index anyway"
        )

    async def test_the_pooled_release_path_is_unchanged_with_the_flag_off(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """⭐ THE MONEY ASSERTION. Settlement must be bit-for-bit what it is today.

        IN PRODUCTION IF THIS FAILS: this change stopped being observation-only
        and started moving money. The whole point of separating the mint from the
        release is that arming later flips EXACTLY ONE thing; if the pooled
        settlement has already drifted, the flip is once again a comparison
        between two unknowns.

        Handles are deliberately PASSED here and must be IGNORED: they are
        genuine now (the mint is unconditional) and acting on them would drain the
        shadow pool — the one nothing enforces — while leaving the enforcing pool
        inflated. That is a fail-CLOSED drift on every debit, reached by
        "helpfully" using data that was only ever meant to be counted.
        """
        _arm_reservation_handles(monkeypatch, enabled=False)
        tenant = T("pooled-release-unchanged")
        budget = _budget(tenant=tenant, cap=1000)
        db_session.add(budget)
        await db_session.commit()

        holds = []
        for amount in (400, 400):
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=tenant,
                estimated_tokens=amount,
                ttl_seconds=300,
            )
            assert r["allowed"] is True, r
            holds.append(r["reservation_id"])
        handle_a, _handle_b = holds
        assert await _pool(fake_redis, tenant, tagged=False) == 800

        await _commit_consumed(db_session, budget, 900)
        before = _counters()
        remaining = await release_reservation(fake_redis, tenant, 900, handles={handle_a: 900})
        after = _counters()

        # The documented pooled behaviour, unchanged: an anonymous DECRBY that
        # clamps to 0, DELETEs the key, and is COUNTED as an over-release.
        assert remaining == 0, f"the pooled drain returned {remaining}, not the clamped 0"
        assert await fake_redis.exists(reserved_key(tenant)) == 0, (
            "the anonymous drain stopped clamping-and-deleting; mode A's documented "
            "behaviour changed under a flag that is supposed to be OFF"
        )
        assert after["over_release"] - before["over_release"] == 1, (
            "the pooled over-release stopped being counted. That series is the only "
            "instrument on this fail-open, and its rate falling to zero is the "
            "acceptance test for arming the flag — it must not fall to zero because "
            "the writer moved"
        )
        assert after["settled"] == before["settled"], (
            "the pooled path settled BY HANDLE while the flag is off. The release "
            "branch is the one thing still behind the flag; if it moved, arming no "
            "longer flips exactly one structure"
        )
        # The shadow ledger is written by reserves and swept by expiry ONLY.
        assert await fake_redis.hget(reservation_amounts_key(tenant), handle_a) == "400", (
            "a pooled settlement discharged a shadow hold"
        )
        assert await _pool(fake_redis, tenant, tagged=True) == 800, (
            "a pooled settlement drained the shadow pool"
        )

        # ⭐ NEGATIVE CONTROL: the SAME call shape with the flag ARMED must behave
        #    DIFFERENTLY. Without this, every assertion above would still pass on a
        #    build where the flag had stopped selecting anything at all — which is
        #    precisely the failure this file's TEST 2 guards in the other direction.
        _arm_reservation_handles(monkeypatch, enabled=True)
        armed_tenant = T("pooled-release-control")
        db_session.add(_budget(tenant=armed_tenant, cap=1000))
        await db_session.commit()
        armed_holds = []
        for amount in (400, 400):
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=armed_tenant,
                estimated_tokens=amount,
                ttl_seconds=300,
            )
            assert r["allowed"] is True, r
            armed_holds.append(r["reservation_id"])
        control_before = _counters()
        armed_remaining = await release_reservation(
            fake_redis, armed_tenant, 900, handles={armed_holds[0]: 900}
        )
        control_after = _counters()

        assert armed_remaining == 400, (
            f"the ARMED path returned {armed_remaining}, the same anonymous wipe as "
            "the pooled one. The flag is no longer selecting a settlement protocol, "
            "so the test above proves nothing"
        )
        assert control_after["settled"] - control_before["settled"] == 1
        assert control_after["over_release"] == control_before["over_release"]

    async def test_the_by_handle_path_still_settles_when_armed(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """The armed path must still discharge exactly the holds it names.

        IN PRODUCTION IF THIS FAILS: arming the flag stops settling anything, so
        every hold waits out its TTL and the gate denies headroom that has already
        been paid for — the fail-CLOSED 429 storm ``release_reservation`` exists
        to prevent, arrived at through the fix rather than the defect.

        Separating the mint from the release must not have disturbed the armed
        protocol, and the handle it now mints (canonical dashed form rather than
        ``uuid4().hex``) must still be the byte string the amounts hash is keyed
        on — ``HDEL`` compares bytes, not uuids.
        """
        _arm_reservation_handles(monkeypatch, enabled=True)
        tenant = T("armed-still-settles")
        db_session.add(_budget(tenant=tenant, cap=1000))
        await db_session.commit()

        holds = []
        for amount in (400, 300):
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=tenant,
                estimated_tokens=amount,
                ttl_seconds=300,
            )
            assert r["allowed"] is True, r
            holds.append(r["reservation_id"])
        handle_a, handle_b = holds
        assert await _pool(fake_redis, tenant, tagged=True) == 700

        before = _counters()
        remaining = await release_reservation(fake_redis, tenant, 400, handles={handle_a: 400})
        after = _counters()

        assert remaining == 300, (
            f"settling A's named 400-token hold left the pool at {remaining}, not B's "
            "300. Either the hold was not found by its handle (a FORM mismatch "
            "between the mint and the amounts key would do exactly this, and would "
            "look identical to 'no producer sends a handle') or a peer was drained"
        )
        assert await fake_redis.hexists(reservation_amounts_key(tenant), handle_a) == 0
        assert await fake_redis.hget(reservation_amounts_key(tenant), handle_b) == "300"
        assert after["settled"] - before["settled"] == 1
        assert after["over_release"] == before["over_release"]

        # ⭐ NEGATIVE CONTROL: a handle nothing minted must move NOTHING. Without
        #    it, a `_release_by_handle` that simply subtracted `tokens` regardless
        #    of what it found would satisfy every assertion above.
        control_before = _counters()
        control_remaining = await release_reservation(
            fake_redis, tenant, 400, handles={str(uuid.uuid4()): 400}
        )
        control_after = _counters()

        assert control_remaining == 300, (
            f"an unknown handle moved the pool to {control_remaining}; a settlement "
            "that names nothing real is spending B's live reservation"
        )
        assert await fake_redis.hget(reservation_amounts_key(tenant), handle_b) == "300"
        assert control_after["settled"] == control_before["settled"]

    async def test_the_shadow_index_is_swept_and_cannot_grow_with_the_flag_off(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """Unconditional bookkeeping must not become an unbounded key.

        IN PRODUCTION IF THIS FAILS: minting on every reserve turns the index and
        amounts keys into a per-tenant leak. Nothing SETTLES a shadow hold — the
        release still routes to ``_RELEASE_LUA`` while the flag is off — so the
        expiry sweep inside ``_RESERVE_SHADOW_LUA`` is the ONLY drain these keys
        have, and if it stops running the structure grows for the whole life of
        the key.

        ⭐ The same assertion doubles as the semantics check on
        ``billing_budget_reservation_expired_total``: in this mode every mint
        lapses unsettled, so ``minted ≈ expired`` with ``settled == 0`` is the
        healthy pre-arm reading. Suppressing the count here would open a gap that
        the metric's own comment tells operators to read as headroom held forever.

        Expiry is forced by rewriting the ZSET SCORE into the past — the score IS
        the clock the sweep reads — rather than by sleeping past a live TTL.
        """
        _arm_reservation_handles(monkeypatch, enabled=False)
        tenant = T("shadow-swept")
        db_session.add(_budget(tenant=tenant, cap=100_000))
        await db_session.commit()

        first = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=400,
            ttl_seconds=300,
        )
        assert first["allowed"] is True, first
        stale = first["reservation_id"]
        await fake_redis.zadd(reservation_index_key(tenant), {stale: 1})
        score = await fake_redis.zscore(reservation_index_key(tenant), stale)
        assert score is not None and score < datetime.now(UTC).timestamp(), (
            "precondition: the hold must be scored in the past for the sweep to see it"
        )

        before = _counters()
        second = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=100,
            ttl_seconds=300,
        )
        after = _counters()

        assert second["allowed"] is True, second
        assert await fake_redis.zcard(reservation_index_key(tenant)) == 1, (
            "the lapsed hold is still in the index after a later reserve. The sweep "
            "is the only drain these keys have while the flag is off, so a tenant's "
            "index now grows once per reserve for the life of the key"
        )
        assert await fake_redis.hexists(reservation_amounts_key(tenant), stale) == 0, (
            "the amounts hash kept the reclaimed hold, so the next sweep re-subtracts "
            "the same tokens and the shadow pool goes permanently negative"
        )
        assert after["expired"] - before["expired"] == 1, (
            "the shadow sweep reclaimed silently. `minted - (settled + expired)` is "
            "the handle budget, and with settlement not yet routed by handle the "
            "sweep is the ONLY way a mint can be discharged — an uncounted sweep "
            "makes every mint look permanently stranded"
        )
        # ⭐ NEGATIVE CONTROL: the gate's own pool must not have been touched by
        #    any of the sweeping. 400 + 100 with nothing released is 500.
        assert await _pool(fake_redis, tenant, tagged=False) == 500, (
            "the shadow sweep reached the untagged pool. Under the pooled protocol "
            "the key's TTL is the release path for a lapsed hold; a sweep that also "
            "decrements it releases the same tokens twice and the cap fails open"
        )

    async def test_the_minted_handle_survives_the_producer_round_trip(
        self, db_session: AsyncSession, fake_redis, monkeypatch
    ) -> None:
        """The handle's FORM must survive the trip through a Postgres uuid column.

        IN PRODUCTION IF THIS FAILS: arming the flag discharges NOTHING, forever,
        and it looks exactly like "no producer sends a handle". The round trip is
        ``reservation_id`` -> the producer's cost event ->
        ``cost_flusher._parse_uuid`` -> ``billing_cost_events.reservation_id`` (a
        Postgres ``uuid`` COLUMN) -> ``str(...)`` on read-back -> the ``handles``
        map -> ``HDEL`` against the amounts key. ``str()`` of a ``uuid.UUID`` is
        ALWAYS dashed, and ``HDEL`` compares BYTES — so a handle minted in the
        undashed ``uuid4().hex`` spelling comes back dashed and misses its own
        index entry on every settlement.

        ⛔ This is not style. It is the difference between a coverage measurement
        that can be non-zero and one that reads a structural zero.
        """
        from services._parsers import _parse_uuid

        _arm_reservation_handles(monkeypatch, enabled=False)
        tenant = T("handle-round-trip")
        db_session.add(_budget(tenant=tenant, cap=1000))
        await db_session.commit()

        granted = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=tenant,
            estimated_tokens=400,
            ttl_seconds=300,
        )
        rid = granted["reservation_id"]
        assert isinstance(rid, str) and rid, granted

        # The exact trip, with the flusher's own parser rather than a second one
        # written here — two parsers for one id is how call sites come to disagree.
        round_tripped = str(_parse_uuid(rid))
        assert round_tripped == rid, (
            f"the minted handle {rid!r} came back from a uuid column as "
            f"{round_tripped!r}. Every armed settlement would HDEL the second "
            "spelling against an index keyed on the first, discharge nothing, and "
            "report the result as an unattributed release"
        )
        assert await fake_redis.hexists(reservation_amounts_key(tenant), round_tripped) == 1, (
            "the amounts hash is not keyed on the form a settlement will actually "
            "present; the index and the producer disagree about this hold's name"
        )

        # ⭐ NEGATIVE CONTROL: the spelling this replaced FAILS the same trip. If
        #    this control ever passes, the round trip has stopped discriminating
        #    and the assertion above is vacuous.
        legacy_spelling = uuid.uuid4().hex
        assert str(_parse_uuid(legacy_spelling)) != legacy_spelling, (
            "an undashed uuid4 hex now survives a uuid column unchanged, so this "
            "test can no longer tell the two spellings apart"
        )
