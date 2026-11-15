"""Money-safety regressions: reserve release, period reset, DLQ dedup, invoice order.

Every test here is written to FAIL against the pre-fix code (mutation baseline)
and pass after. They cover four independent money bugs:

1. ``tokens:tenant:{id}:reserved`` had one writer and no release path, and its
   TTL was re-extended on every success -> the reserve gate eventually 429s
   every request while committed consumption is ~0 (fails CLOSED).
2. ``period='monthly'`` budgets never reset -> a monthly cap behaved as a
   lifetime cap.
3. DLQ replay deduped on the dead-stream ENTRY id, so one event quarantined
   twice (crash between XADD and commit) got two markers -> billed twice.
4. The invoice's LLM cost lines were consumed against the allowance in
   undefined DB row order, so the same data could produce different totals.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import TokenBudget
from services.budget_debit import debit_budget
from services.budget_redis import reserve_budget
from tests._budget_factory import make_budget
from tests.conftest import T

RESERVED_KEY = "tokens:tenant:{}:reserved"


def _budget(
    *,
    tenant: str,
    cap: int,
    consumed: int = 0,
    period: str = "monthly",
    starts_at: datetime | None = None,
) -> TokenBudget:
    if starts_at is not None:
        # Deliberate prior-period rows. Leave consumed_period_start NULL so the
        # rollover tests below keep exercising consumption_anchor's starts_at
        # fallback -- that is the behaviour under test, not an oversight.
        return make_budget(
            tenant=tenant,
            cap=cap,
            consumed=consumed,
            period=period,
            source="test",
            starts_at=starts_at,
            consumed_period_start=None,
        )
    return make_budget(tenant=tenant, cap=cap, consumed=consumed, period=period, source="test")


# --------------------------------------------------------------------------
# 1. reserve gate must not fail closed
# --------------------------------------------------------------------------


@pytest.mark.unit
class TestReserveRelease:
    async def test_settled_reservations_are_released(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """Sustained reserve->debit traffic must not 429 while headroom remains.

        cap=1000, each cycle reserves 100 and then really debits 100. Committed
        consumption alone only reaches the cap on cycle 11, so all 10 cycles
        below MUST be allowed. Pre-fix, `reserved` was never decremented, so
        consumed and reserved DOUBLE-COUNTED the same tokens and cycle 6 was
        already denied with consumed=500.
        """
        db_session.add(_budget(tenant=T("rel"), cap=1000))
        await db_session.commit()

        for cycle in range(10):
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=T("rel"),
                estimated_tokens=100,
                ttl_seconds=300,
            )
            assert r["allowed"] is True, (
                f"cycle {cycle}: denied with consumed={r['consumed_tokens']} "
                f"reserved={r['in_flight_reserved_tokens']} cap={r['cap_tokens']}"
            )
            # The request completes and its real cost is debited (settlement).
            await debit_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=T("rel"),
                tokens_used=100,
            )

        # Everything reserved has been settled -> nothing left in flight.
        left = await fake_redis.get(RESERVED_KEY.format(T("rel")))
        assert int(left or 0) == 0

    async def test_release_never_goes_negative(self, db_session: AsyncSession, fake_redis) -> None:
        """A debit with no matching reservation must clamp the counter at 0,
        never leave a negative `reserved` that silently raises the cap."""
        db_session.add(_budget(tenant=T("neg"), cap=10_000))
        await db_session.commit()

        for _ in range(3):
            await debit_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=T("neg"),
                tokens_used=500,
            )

        raw = await fake_redis.get(RESERVED_KEY.format(T("neg")))
        assert int(raw or 0) >= 0, f"reserved went negative: {raw}"

        # And the gate still enforces the real committed spend.
        r = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("neg"),
            estimated_tokens=20_000,
            ttl_seconds=60,
        )
        assert r["allowed"] is False

    async def test_an_over_release_is_counted_not_hidden(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """A settlement that drains past the WHOLE pool must be measurable.

        The pool is one anonymous per-tenant integer, so a settlement that
        releases more than is pooled takes the excess out of OTHER callers'
        in-flight reservations and the next pre-flight check admits spend it
        should have denied. Confirmed reachable: cap=1000, A reserves 400, B
        reserves 400, A settles at an ACTUAL 900 -> the key is DELETED -> a
        third caller reserving 100 is admitted at consumed=900/reserved=0 while
        B's 400 is still in flight (true position 1400 > cap 1000).

        That cannot be attributed away without giving reservations an identity
        (a license-authz protocol change), and releasing LESS would restore the
        fail-CLOSED 429 storm this path exists to prevent. So the money
        behaviour is unchanged and the fail-open is instead made EXPRESSIBLE.
        This asserts the overshoot is reported, which is the whole fix.
        """
        from observability import metrics as observability_metrics
        from services.budget_redis import release_reservation, reserved_key

        db_session.add(_budget(tenant=T("ovr"), cap=1000))
        await db_session.commit()

        for _ in range(2):
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=T("ovr"),
                estimated_tokens=400,
                ttl_seconds=300,
            )
            assert r["allowed"] is True
        assert int(await fake_redis.get(reserved_key(T("ovr")))) == 800

        before = observability_metrics.budget_reserve_over_release_total._value.get()
        remaining = await release_reservation(fake_redis, T("ovr"), 900)
        after = observability_metrics.budget_reserve_over_release_total._value.get()

        assert remaining == 0
        assert after - before == 1, (
            "an over-release past the entire in-flight pool was not counted; "
            "the cap fails open by exactly that overshoot and nothing says so"
        )

    async def test_an_exact_release_is_not_reported_as_an_over_release(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """CONTROL: a legitimate reserve -> settle must stay silent.

        Draining the pool to exactly 0 is a normal settlement, not an
        over-release; counting it would make the signal useless.
        """
        from observability import metrics as observability_metrics
        from services.budget_redis import release_reservation, reserved_key

        db_session.add(_budget(tenant=T("exact"), cap=1000))
        await db_session.commit()

        r = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("exact"),
            estimated_tokens=400,
            ttl_seconds=300,
        )
        assert r["allowed"] is True

        before = observability_metrics.budget_reserve_over_release_total._value.get()
        # Partial settlement: 150 of the 400 held.
        assert await release_reservation(fake_redis, T("exact"), 150) == 250
        # Exact settlement of the remainder.
        assert await release_reservation(fake_redis, T("exact"), 250) == 0
        after = observability_metrics.budget_reserve_over_release_total._value.get()

        assert after == before, "a legitimate settlement was reported as an over-release"
        assert await fake_redis.get(reserved_key(T("exact"))) is None

    async def test_reserve_ttl_is_not_re_extended_on_every_success(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """The orphan-reservation TTL must be set on CREATION only.

        Pre-fix every successful reserve issued a fresh EXPIRE, so under
        sustained traffic the key never aged out and leaked reservations
        accumulated forever. The TTL is the only release path for a reservation
        whose request died, so it must actually fire.
        """
        db_session.add(_budget(tenant=T("ttl"), cap=1_000_000))
        await db_session.commit()

        async def _reserve() -> None:
            r = await reserve_budget(
                session=db_session,
                redis_conn=fake_redis,
                tenant_id=T("ttl"),
                estimated_tokens=10,
                ttl_seconds=2,
            )
            assert r["allowed"] is True

        await _reserve()  # t=0, key created with TTL 2s
        await asyncio.sleep(1.2)
        await _reserve()  # t=1.2 — pre-fix this re-EXPIREs to t=3.2
        await asyncio.sleep(1.2)  # t=2.4 — past the CREATION TTL

        assert await fake_redis.exists(RESERVED_KEY.format(T("ttl"))) == 0, (
            "reserved key survived its creation TTL — EXPIRE is still being refreshed"
        )


@pytest.mark.unit
class TestReleaseWithoutReserveIsSplitFromOverRelease:
    """``overshoot > 0`` collapsed two opposite populations into one counter.

    ``billing_budget_reserve_over_release_total`` fired CRITICAL on both billing
    pods continuously. It was not a false alarm in the usual sense — the counter
    was incrementing exactly as written — it was that the counter had two
    tenants:

    (a) a release against an ABSENT/empty pool. ``overshoot == released``.
        Nothing is taken from anyone. This is the ordinary debit path, because
        ``reserve_budget`` has exactly one caller (``POST /budget/reserve``)
        while ``release_reservation`` runs on every debit. Measured at ~90% of
        all debits in prod, i.e. the alert fired on normal operation.
    (b) a release that drained a LIVE pool past its end. ``overshoot <
        released``. Those tokens come out of other callers' reservations and
        the cap fails open. This is the real defect the metric was written for.

    A Prometheus LABEL cannot fix this: the live rule is
    ``sum by (instance) (increase(billing_budget_reserve_over_release_total[1h])) > 0``
    which sums across labels, so a labelled (a) would still page. The split has
    to be a separate metric NAME.

    Pre-fix, every assertion below that names the new counter fails at import
    (the metric does not exist) and every "(a) must not touch the critical
    counter" assertion fails on the count.

    These use ``fake_redis`` only — no Postgres. The behaviour under test is
    entirely inside ``_RELEASE_LUA`` and ``release_reservation``; going through
    ``reserve_budget`` would drag in the DB for no added coverage, so the pool
    is seeded directly to state the precondition in one line.
    """

    @staticmethod
    def _counters() -> tuple[float, float]:
        """(critical over-release, benign release-without-reserve) right now."""
        from observability import metrics as observability_metrics

        return (
            observability_metrics.budget_reserve_over_release_total._value.get(),
            observability_metrics.budget_release_without_reserve_total._value.get(),
        )

    async def test_release_against_an_absent_pool_counts_only_the_benign_metric(
        self, fake_redis
    ) -> None:
        """(a) A debit that never reserved must NOT touch the critical counter.

        This is the firing. The key does not exist, so `DECRBY` creates it at
        `-amount`, the clamp deletes it, and `overshoot == amount`. Nothing was
        pooled, so nothing was stolen and no cap failed open — but pre-fix this
        incremented the counter whose alert pages at `> 0`.
        """
        from services.budget_redis import release_reservation, reserved_key

        assert await fake_redis.exists(reserved_key(T("norsv"))) == 0, (
            "precondition: no reservation pool for this tenant"
        )

        crit_before, benign_before = self._counters()
        remaining = await release_reservation(fake_redis, T("norsv"), 500)
        crit_after, benign_after = self._counters()

        assert remaining == 0
        # The CRITICAL counter first: that assertion is the actual firing.
        assert crit_after == crit_before, (
            "a release against an ABSENT pool incremented "
            "billing_budget_reserve_over_release_total, whose alert is CRITICAL at >0. "
            "Nothing was taken from any other caller and no cap failed open, yet this "
            "pages -- and it happens on ~90% of debits, because the debit path settles "
            "whether or not that spend ever reserved"
        )
        assert benign_after - benign_before == 1, (
            "a release against an absent pool was not counted as such"
        )
        assert await fake_redis.exists(reserved_key(T("norsv"))) == 0, (
            "the clamp must still delete the key, so the next reserve sees reserved=0"
        )

    async def test_overshooting_a_live_pool_still_counts_the_critical_metric(
        self, fake_redis
    ) -> None:
        """(b) The genuine fail-open must STILL page, and must not be laundered.

        A live pool of 800 released at 900 takes 100 tokens out of other
        callers' in-flight reservations. The split must not weaken this: the
        investigation sampled 711 log lines and saw only class (a), but
        `kubectl logs` serves the current file only, so that is a large sample
        and not a census. Class (b) at a low rate would have been invisible.
        """
        from services.budget_redis import release_reservation, reserved_key

        await fake_redis.set(reserved_key(T("live")), 800)

        crit_before, benign_before = self._counters()
        remaining = await release_reservation(fake_redis, T("live"), 900)
        crit_after, benign_after = self._counters()

        assert remaining == 0
        assert crit_after - crit_before == 1, (
            "a release that drained a LIVE pool past its end stopped being counted -- "
            "the split must not weaken the genuine fail-open signal"
        )
        assert benign_after == benign_before, (
            "a genuine over-release was misfiled as the benign class, which would "
            "silence the alert on the exact defect it exists to catch"
        )

    async def test_an_unknown_reply_shape_fails_towards_the_alarm(self) -> None:
        """A reply that cannot say which class it was must count as (b).

        `_split_release_result` tolerates two older reply shapes for rolling
        deploys. Neither carries `prior`, so the class is unknown. Unknown must
        resolve to the critical counter: laundering it into the benign one
        would silence a real fail-open during exactly the window when the two
        versions disagree.
        """
        from services.budget_redis import _split_release_result

        assert _split_release_result([0, 100]) == (0, 100, None), (
            "the 2-element legacy reply must report prior as UNKNOWN, not as 0"
        )
        assert _split_release_result(250) == (250, 0, None)
        # And the current shape does carry it.
        assert _split_release_result([0, 100, 800]) == (0, 100, 800)


# --------------------------------------------------------------------------
# 2. periodic budgets must reset
# --------------------------------------------------------------------------


def _first_of_month(d: datetime) -> datetime:
    return d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


@pytest.mark.unit
class TestBudgetPeriodReset:
    async def test_monthly_budget_from_a_previous_month_is_not_enforced(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """A monthly budget fully consumed in an EARLIER month must not block
        this month. Pre-fix `consumed_tokens` was a lifetime counter, so the
        gate stayed shut forever."""
        now = datetime.now(UTC)
        two_months_ago = _first_of_month(_first_of_month(now) - timedelta(days=1)) - timedelta(
            days=1
        )
        db_session.add(_budget(tenant=T("mon"), cap=1000, consumed=1000, starts_at=two_months_ago))
        await db_session.commit()

        r = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("mon"),
            estimated_tokens=100,
            ttl_seconds=60,
        )
        assert r["allowed"] is True, "last month's spend is still blocking this month"
        assert r["consumed_tokens"] == 0

    async def test_debit_zeroes_stale_period_consumption(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """The first debit of a new period rebases consumed_tokens to that
        period's spend only, durably (survives restart: it is a column write)."""
        now = datetime.now(UTC)
        last_month = _first_of_month(now) - timedelta(days=1)
        b = _budget(tenant=T("mon2"), cap=1000, consumed=900, starts_at=last_month)
        db_session.add(b)
        await db_session.commit()

        await debit_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("mon2"),
            tokens_used=200,
        )
        await db_session.refresh(b)
        assert int(b.consumed_tokens) == 200, (
            f"expected this period's 200 tokens, got {b.consumed_tokens}"
        )

    async def test_same_period_consumption_is_preserved(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        """Guard against the opposite failure: a budget already debited THIS
        period must accumulate, not reset on every call."""
        b = _budget(tenant=T("mon3"), cap=1000, consumed=0)
        db_session.add(b)
        await db_session.commit()

        await debit_budget(
            session=db_session, redis_conn=fake_redis, tenant_id=T("mon3"), tokens_used=100
        )
        await debit_budget(
            session=db_session, redis_conn=fake_redis, tenant_id=T("mon3"), tokens_used=150
        )
        await db_session.refresh(b)
        assert int(b.consumed_tokens) == 250

    async def test_weekly_budget_resets_on_a_new_iso_week(
        self, db_session: AsyncSession, fake_redis
    ) -> None:
        now = datetime.now(UTC)
        b = _budget(
            tenant=T("wk"),
            cap=500,
            consumed=500,
            period="weekly",
            starts_at=now - timedelta(days=14),
        )
        db_session.add(b)
        await db_session.commit()

        r = await reserve_budget(
            session=db_session,
            redis_conn=fake_redis,
            tenant_id=T("wk"),
            estimated_tokens=50,
            ttl_seconds=60,
        )
        assert r["allowed"] is True
        assert r["consumed_tokens"] == 0

    async def test_status_reports_current_period_usage(self, db_session: AsyncSession) -> None:
        """The Redis mirror / status read path must agree with the gate."""
        from services.budget_status import check_budget_aggregate

        now = datetime.now(UTC)
        last_month = _first_of_month(now) - timedelta(days=1)
        db_session.add(_budget(tenant=T("st"), cap=1000, consumed=1000, starts_at=last_month))
        await db_session.commit()

        agg = await check_budget_aggregate(db_session, T("st"))
        assert agg["total_remaining"] == 1000, agg


# --------------------------------------------------------------------------
# 3. DLQ replay must dedup on the ORIGINAL event id
# --------------------------------------------------------------------------

_DEAD_STREAM = "billing:costs:dead"
_COSTS_STREAM = "billing:costs"


async def _seed(redis, *, original_id: str) -> str:
    return await redis.xadd(
        _DEAD_STREAM,
        {
            "provider": "claude",
            "model": "sonnet",
            "input_tokens": "100",
            "output_tokens": "50",
            "tenant_id": T("tenant-a"),
            "cost_source": "test",
            "_dlq_reason": "missing_provider",
            "_dlq_original_id": original_id,
            "_dlq_at": "2026-07-24T00:00:00+00:00",
        },
    )


@pytest.mark.unit
class TestDlqReplayOriginalIdDedup:
    async def test_same_event_quarantined_twice_is_replayed_once(self, fake_redis) -> None:
        """cost_flusher XADDs to :dead BEFORE committing the inbox row, so a
        crash in between re-quarantines the SAME source event under a new dead
        id. Both entries carry the same `_dlq_original_id`; replaying both must
        bill once, not twice."""
        from services.dlq_ops import replay_dlq

        a = await _seed(fake_redis, original_id="1700000000000-7")
        b = await _seed(fake_redis, original_id="1700000000000-7")
        assert a != b

        result = await replay_dlq(fake_redis, ids=[a, b], delete_on_success=False)

        injected = await fake_redis.xrange(_COSTS_STREAM)
        assert len(injected) == 1, f"double-billed: {len(injected)} copies on {_COSTS_STREAM}"
        assert result.replayed == [a]
        assert result.already_processed == [b]

    async def test_distinct_original_ids_both_replay(self, fake_redis) -> None:
        from services.dlq_ops import replay_dlq

        a = await _seed(fake_redis, original_id="1-1")
        b = await _seed(fake_redis, original_id="1-2")
        result = await replay_dlq(fake_redis, ids=[a, b], delete_on_success=False)
        assert sorted(result.replayed) == sorted([a, b])
        assert len(await fake_redis.xrange(_COSTS_STREAM)) == 2

    async def test_replay_marker_outlives_a_retained_dead_entry(self, fake_redis) -> None:
        """The dedupe key may NOT expire while a re-drainable entry survives.

        The marker used to be claimed with a 30d TTL on the belief that the
        entry aged out of ``billing:costs:dead`` first. Nothing enforces that:
        the only age bound is ``XTRIM MINID ~``, and ``~`` trims at macro-node
        granularity, so a small dead-letter stream is never trimmed at all.
        Entry outlives marker -> a second drain re-claims and re-XADDs -> the
        inbox keys on the NEW stream_msg_id and cannot dedupe -> billed twice,
        with no refund path.

        Invariant asserted: once the re-inject has durably landed, the marker
        carries NO expiry (``pttl == -1``, "key exists, no TTL"), not merely a
        long one.
        """
        from services.dlq_ops import _replay_marker, replay_dlq

        dead_id = await _seed(fake_redis, original_id="1800000000000-1")
        _, fields = (await fake_redis.xrange(_DEAD_STREAM, min=dead_id, max=dead_id))[0]
        marker = _replay_marker(dead_id, fields)

        result = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)
        assert result.replayed == [dead_id]
        # The entry is deliberately still on :dead...
        assert len(await fake_redis.xrange(_DEAD_STREAM)) == 1
        # ...so its guard must never expire.
        assert await fake_redis.pttl(marker) == -1, (
            "the replay marker still expires while the dead entry it guards survives; "
            "a drain after that expiry re-bills the event"
        )

    async def test_a_single_drain_still_bills_exactly_once(self, fake_redis) -> None:
        """CONTROL: the ordinary drain path is unchanged.

        One entry, one replay, ``delete_on_success=True``: exactly one copy on
        ``billing:costs``, the entry gone from ``:dead``, and a second replay of
        the same id is a no-op rather than a second billing.
        """
        from services.dlq_ops import replay_dlq

        dead_id = await _seed(fake_redis, original_id="1800000000000-2")

        first = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=True)
        assert first.replayed == [dead_id]
        assert first.deleted_from_dlq == [dead_id]
        assert len(await fake_redis.xrange(_COSTS_STREAM)) == 1
        assert await fake_redis.xrange(_DEAD_STREAM) == []

        second = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=True)
        assert second.replayed == []
        assert len(await fake_redis.xrange(_COSTS_STREAM)) == 1

    async def test_legacy_entry_without_original_id_falls_back_to_entry_id(
        self, fake_redis
    ) -> None:
        from services.dlq_ops import replay_dlq

        dead_id = await fake_redis.xadd(
            _DEAD_STREAM,
            {
                "provider": "claude",
                "input_tokens": "1",
                "output_tokens": "1",
                "tenant_id": T("t"),
                "_dlq_reason": "missing_provider",
            },
        )
        first = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)
        second = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)
        assert first.replayed == [dead_id]
        assert second.already_processed == [dead_id]
        assert len(await fake_redis.xrange(_COSTS_STREAM)) == 1


# --------------------------------------------------------------------------
# 4/6. the invoice must not depend on DB row order
#
# ⚠️ RETIRED 2026-09-15 — `test_cost_lines_are_consumed_in_a_deterministic_order`
# USED TO LIVE HERE, AND IT PINNED THE WORKAROUND RATHER THAN THE PROPERTY.
#
# It asserted that `cost_stmt` emitted an ORDER BY and that `consume_allowance`
# saw the lines in that order. Both facts were true and neither was the thing
# anyone cared about: the allowance was drained line by line BEFORE a PER-LINE
# markup rate was applied, so whichever line arrived first was the line whose
# markup was forgone, and the invoice total was a function of a sort key. A fixed
# sort makes that total REPRODUCIBLE; it does not make it RIGHT, and it does not
# make it INDEPENDENT of the sort. Measured on two projects at 30% and 100% with
# a $4 allowance, the same inputs billed $8.00 or $5.20 (+53.8%) depending purely
# on how two UUIDs compared — a green ORDER BY test throughout.
#
# ⛔ So the retired test could not have failed on the defect it was named after,
# and it would have BLOCKED the fix: `allocate_allowance` is a batch allocator,
# so there is no longer a per-line call for it to record. The replacement below
# asserts the property instead — run the identical month under BOTH orderings and
# require the identical total. That one cannot pass on a workaround.
# --------------------------------------------------------------------------


#: Two project ids whose UUID comparison is total and obvious, so a test can say
#: "the cheap line sorts first" and "the dear line sorts first" without relying
#: on a random draw. `cost_stmt` orders on (license, project, ...), so swapping
#: which project carries which rate swaps which line the old code drained first.
_PROJ_FIRST = uuid.UUID("00000000-0000-4000-8000-00000000aaa1")
_PROJ_SECOND = uuid.UUID("00000000-0000-4000-8000-00000000aaa2")
assert _PROJ_FIRST < _PROJ_SECOND

#: $4 of incurred spend on each of two projects, priced 30% and 100%. Chosen so
#: every regime in the operator's movement table is a round number:
#:   no allowance      4*1.30 + 4*2.00                   = 13.20
#:   $4, 30% drained   0      + 4*2.00                   =  8.00   (old, order A)
#:   $4, 100% drained  4*1.30 + 0                        =  5.20   (old, order B)
#:   $4 pro-rata       2*1.30 + 2*2.00                   =  6.60   (new, both)
#:   $8 (full)         0                                 =  0.00
_LINE_INCURRED = Decimal("4.00")
_PCT_CHEAP = Decimal("30")
_PCT_DEAR = Decimal("100")


async def _seed_two_priced_projects(
    session: AsyncSession,
    *,
    tenant: str,
    month: date,
    dear_project: uuid.UUID,
    allowance: Decimal | None,
) -> None:
    """One tenant, two equal cost lines, two project-pinned rates, one allowance.

    ``dear_project`` decides which of the two sort-ordered projects carries the
    100% rate — that is the ONLY thing that differs between the two orderings.
    """
    from models.db import CostDaily, MarkupRule, TenantAccessCharge

    cheap_project = _PROJ_SECOND if dear_project == _PROJ_FIRST else _PROJ_FIRST
    effective_from = datetime(month.year, month.month, 1, tzinfo=UTC) - timedelta(days=1)
    lic = uuid.uuid4()

    for project, pct in ((dear_project, _PCT_DEAR), (cheap_project, _PCT_CHEAP)):
        session.add(
            MarkupRule(
                tenant_id=tenant,
                project_id=project,
                markup_pct=pct,
                effective_from=effective_from,
            )
        )
        session.add(
            CostDaily(
                tenant_id=tenant,
                tenant_name="prorata",
                license_id=lic,
                project_id=project,
                service_key="app",  # pragma: allowlist secret
                provider="prov",
                served_model="model",
                byo=False,
                bucket_date=month,
                request_count=1,
                total_input_tokens=10,
                total_output_tokens=10,
                total_cost_usd=_LINE_INCURRED,
            )
        )
    if allowance is not None:
        # amount_usd 0: this row exists to GRANT the allowance, not to charge a
        # fee, so the totals below are pure usage and the arithmetic is readable.
        session.add(
            TenantAccessCharge(
                tenant_id=tenant,
                service_key=None,
                amount_usd=Decimal("0"),
                cadence="monthly",
                included_cost_usd=allowance,
                effective_from=effective_from,
            )
        )
    await session.commit()


async def _customer_total(session, settings, *, tenant: str, month: date) -> Decimal:
    import services.billing as billing_mod

    payload = await billing_mod.generate_billing(
        tenant_id=tenant,
        month=month.strftime("%Y-%m"),
        session=session,
        settings=settings,
    )
    return Decimal(payload["totals"]["customer_cost_usd"])


@pytest.mark.unit
class TestAllowanceIsAllocatedProRata:
    """⭐ THE INVOICE TOTAL MUST NOT BE A FUNCTION OF A SORT KEY."""

    async def test_the_total_is_identical_under_both_uuid_orderings(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        """Same inputs, both orderings, one answer — and it is strictly between.

        ⛔ THE ASSERTION THAT MATTERS IS THE EQUALITY, not the value: it is the
        only one that can fail for the defect. The `6.60` is pinned beside it so
        a future change that makes both orderings agree on the WRONG number (e.g.
        by draining the allowance from neither line, or from both) is still red.

        Pre-fix this billed 8.00 in one direction and 5.20 in the other; the
        retired ORDER BY test was green throughout, because a fixed order is
        exactly what it asserted and exactly what does not help.
        """
        month = date.today().replace(day=1)
        await _seed_two_priced_projects(
            db_session,
            tenant=T("pro-a"),
            month=month,
            dear_project=_PROJ_SECOND,
            allowance=Decimal("4.00"),
        )
        await _seed_two_priced_projects(
            db_session,
            tenant=T("pro-b"),
            month=month,
            dear_project=_PROJ_FIRST,
            allowance=Decimal("4.00"),
        )

        dear_sorts_last = await _customer_total(
            db_session, test_settings, tenant=T("pro-a"), month=month
        )
        dear_sorts_first = await _customer_total(
            db_session, test_settings, tenant=T("pro-b"), month=month
        )

        assert dear_sorts_last == dear_sorts_first, (
            "the same month billed two different totals depending only on which "
            f"project id sorted first: {dear_sorts_last} vs {dear_sorts_first} — "
            "the allowance is being drained line by line again"
        )
        assert dear_sorts_last == Decimal("6.60"), dear_sorts_last

    async def test_the_pro_rata_total_sits_strictly_between_the_two_old_answers(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        """6.60 is not a tie-break — it is what the shared credit is worth.

        The two order-dependent answers were 8.00 and 5.20. A pro-rata split of
        the SAME $4 against the SAME $8 of demand gives every line half its cost
        back, so the tenant forgoes half of each rate rather than all of one.
        Asserting the strict betweenness is what distinguishes "allocated" from
        "arbitrarily picked one of the old answers and made it deterministic".
        """
        month = date.today().replace(day=1)
        await _seed_two_priced_projects(
            db_session,
            tenant=T("pro-mid"),
            month=month,
            dear_project=_PROJ_SECOND,
            allowance=Decimal("4.00"),
        )
        total = await _customer_total(db_session, test_settings, tenant=T("pro-mid"), month=month)
        assert Decimal("5.20") < total < Decimal("8.00")

    async def test_the_regimes_either_side_of_a_partial_drain_are_unmoved(
        self, db_session: AsyncSession, test_settings
    ) -> None:
        """No allowance and full absorption are the CONTROLS on the change.

        Pro-rata is only allowed to move the PARTIAL case. If it moved the
        no-allowance total, it would be repricing usage; if it moved the
        fully-absorbed total off $0.00, it would be charging for included usage.
        Both endpoints are order-independent under the old code too, so a green
        here proves the change is confined to the case that was broken.
        """
        month = date.today().replace(day=1)
        await _seed_two_priced_projects(
            db_session,
            tenant=T("pro-none"),
            month=month,
            dear_project=_PROJ_SECOND,
            allowance=None,
        )
        await _seed_two_priced_projects(
            db_session,
            tenant=T("pro-full"),
            month=month,
            dear_project=_PROJ_SECOND,
            allowance=Decimal("8.00"),
        )

        no_allowance = await _customer_total(
            db_session, test_settings, tenant=T("pro-none"), month=month
        )
        fully_absorbed = await _customer_total(
            db_session, test_settings, tenant=T("pro-full"), month=month
        )

        assert no_allowance == Decimal("13.20"), no_allowance
        assert fully_absorbed == Decimal("0.00"), fully_absorbed


@pytest.mark.unit
class TestReplayGuardDurabilityIsAtomic:
    """The marker's permanence must be inseparable from the re-injection.

    The fix originally did ``XADD`` and then ``PERSIST`` as two round-trips.
    That left a window in which the event was already billed while its guard
    still carried the finite claim TTL — and when that TTL lapses, a later
    drain re-claims the still-retained dead entry, re-``XADD``s it, and the
    inbox (keyed on the NEW stream_msg_id) cannot dedupe: billed twice,
    permanently, with no refund path.

    The window is ~1 RTT and the fuse is 30 days, so it is worth closing on
    crash risk alone — but it is not only a crash. ``replay_dlq`` is awaited
    inside a FastAPI admin handler and ``CancelledError`` is a
    ``BaseException``, so a client disconnect or gateway timeout between the
    two calls unwinds past the ``except Exception`` recovery with no process
    dying at all.

    Both tests below ARM that window by making a SEPARATE ``persist`` call
    impossible, then assert the marker is permanent anyway — which can only
    hold if the PERSIST happened inside the same script as the XADD.
    """

    async def test_the_marker_is_permanent_even_if_a_separate_persist_cannot_run(
        self, fake_redis, monkeypatch
    ) -> None:
        """A standalone PERSIST that always fails must not change the outcome."""
        from services.dlq_ops import _replay_marker, replay_dlq

        dead_id = await _seed(fake_redis, original_id="1900000000000-1")
        _, fields = (await fake_redis.xrange(_DEAD_STREAM, min=dead_id, max=dead_id))[0]
        marker = _replay_marker(dead_id, fields)

        async def _explode(*_a, **_k):
            raise RuntimeError("a separate PERSIST round-trip is not available")

        monkeypatch.setattr(fake_redis, "persist", _explode)

        result = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)

        assert result.replayed == [dead_id]
        assert len(await fake_redis.xrange(_COSTS_STREAM)) == 1
        assert len(await fake_redis.xrange(_DEAD_STREAM)) == 1  # entry retained
        assert await fake_redis.pttl(marker) == -1, (
            "the event was re-injected but its guard still expires — the PERSIST is a "
            "separate round-trip, so an interruption between the two re-bills the event"
        )

    async def test_a_cancellation_between_inject_and_persist_cannot_arm_the_window(
        self, fake_redis, monkeypatch
    ) -> None:
        """CancelledError is a BaseException: `except Exception` never sees it.

        If a standalone PERSIST existed, cancelling it would unwind out of
        ``replay_dlq`` entirely, leaving a billed event guarded by an expiring
        marker. With the XADD+PERSIST folded into one script there is no
        separate call to cancel.
        """
        from services.dlq_ops import _replay_marker, replay_dlq

        dead_id = await _seed(fake_redis, original_id="1900000000000-2")
        _, fields = (await fake_redis.xrange(_DEAD_STREAM, min=dead_id, max=dead_id))[0]
        marker = _replay_marker(dead_id, fields)

        async def _cancel(*_a, **_k):
            raise asyncio.CancelledError

        monkeypatch.setattr(fake_redis, "persist", _cancel)

        # Must not raise: nothing calls persist separately any more.
        result = await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)

        assert result.replayed == [dead_id]
        assert await fake_redis.pttl(marker) == -1, (
            "a cancellation left the re-injected event guarded by an expiring marker"
        )

    async def test_the_reinject_carries_the_whole_cleaned_payload(self, fake_redis) -> None:
        """CONTROL: folding XADD into Lua must not lose or reorder fields.

        The script flattens the payload into ARGV, so a field dropped or a
        key/value pair transposed there would corrupt every replayed event —
        a silent mis-bill rather than a double-bill.
        """
        from services.dlq_ops import replay_dlq

        dead_id = await _seed(fake_redis, original_id="1900000000000-3")

        await replay_dlq(fake_redis, ids=[dead_id], delete_on_success=False)

        ((_sid, injected),) = await fake_redis.xrange(_COSTS_STREAM)
        assert injected == {
            "provider": "claude",
            "model": "sonnet",
            "input_tokens": "100",
            "output_tokens": "50",
            "tenant_id": T("tenant-a"),
            "cost_source": "test",
        }, f"the re-injected payload was mangled by the Lua flattening: {injected}"
