"""The daily rollup must CATCH UP, not advance one bucket and call it a day.

Forensic context these tests encode (established from ``billing_cost_daily.id``,
which is UUIDv7 and therefore decodes write time): the daily rollup wrote exactly
ONE ``bucket_date`` per run, advancing one day per run, and sat permanently ~3
days behind — 09-01 21:15Z wrote bucket 08-29, 09-02 21:32Z wrote 08-30, 09-03
18:43Z wrote 08-31, 09-04 23:14Z wrote 09-01. A TRUNCATE of
``billing_cost_events`` at 2026-09-05T07:01:56Z then deleted precisely the
un-rolled backlog for 09-02/03/04. ``min(created_at)`` on the surviving events is
09-05T07:01:58Z — two seconds after. Nothing was recoverable.

The TRUNCATE half is closed elsewhere (alembic 0017's ``BEFORE TRUNCATE`` guard,
PR #140). These tests cover the other half: the lag that created the exposure in
the first place. Every assertion below is MULTI-VALUED — distinct per-day costs,
exact date lists in order, exact set equality on what was and was not written —
so a hardcode that happens to agree with one expected value cannot survive.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from models.db import CostDaily, CostEvent
from observability import metrics as om
from services.rollup_engine import catchup_daily, daily_rollup_lag_days
from tests.conftest import T

TODAY = datetime.now(UTC).date()


def _day(n: int) -> date:
    """``n`` days before today (UTC). ``_day(1)`` is the last CLOSED day."""
    return TODAY - timedelta(days=n)


async def _seed(factory, tenant: str, costs: dict[date, str]) -> None:
    """One cost event per day, each with a DISTINCT cost so sums are separable."""
    async with factory() as session:
        for day, amount in costs.items():
            session.add(
                CostEvent(
                    tenant_id=T(tenant),
                    license_id=uuid.uuid4(),
                    service_key="inference-router",  # pragma: allowlist secret
                    provider="deepseek",
                    input_tokens=1000,
                    output_tokens=200,
                    cost_usd=Decimal(amount),
                    created_at=datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC),
                )
            )
        await session.commit()


async def _cost_daily(factory, tenant: str) -> dict[date, Decimal]:
    async with factory() as session:
        rows = (
            await session.execute(
                select(CostDaily.bucket_date, func.sum(CostDaily.total_cost_usd))
                .where(CostDaily.tenant_id == T(tenant))
                .group_by(CostDaily.bucket_date)
            )
        ).all()
    return {r[0]: r[1] for r in rows}


@pytest.mark.unit
async def test_three_bucket_backlog_is_fully_drained_in_one_pass(db_session_factory):
    """A 3-day backlog must be GONE after ONE pass, not one day shorter.

    ``reconcile_days=0`` deliberately empties the trailing reconciliation window,
    so the only thing that can produce these three buckets is the catch-up itself.
    """
    factory = db_session_factory
    backlog = {_day(4): "1.11", _day(3): "2.22", _day(2): "3.33"}
    await _seed(factory, "catchup-drain", backlog)

    result = await catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=10)

    assert result.rolled == [_day(4), _day(3), _day(2)], "all three, oldest first"
    assert result.remaining == 0
    assert result.due == 3
    assert result.complete is True

    written = await _cost_daily(factory, "catchup-drain")
    assert written == {
        _day(4): Decimal("1.11"),
        _day(3): Decimal("2.22"),
        _day(2): Decimal("3.33"),
    }, "each day must carry its OWN sum — a one-bucket pass leaves two of these absent"


@pytest.mark.unit
async def test_the_open_current_day_is_never_rolled(db_session_factory):
    """Rolling a bucket that is still receiving rows writes it permanently SHORT.

    The boundary is the last CLOSED UTC day. Today has events here and must be
    left alone; yesterday has events and must be rolled — asserting only one of
    those would pass under a loop that rolls everything or nothing.
    """
    factory = db_session_factory
    await _seed(factory, "catchup-open", {_day(1): "4.44", _day(0): "9.99"})

    result = await catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=10)

    assert result.boundary == _day(1), "boundary is the last closed UTC day"
    assert TODAY not in result.rolled
    assert result.rolled == [_day(1)]

    written = await _cost_daily(factory, "catchup-open")
    assert written == {_day(1): Decimal("4.44")}, (
        "today's 9.99 must not appear: the day is still accruing rows"
    )


@pytest.mark.unit
async def test_batch_cap_is_respected_reported_and_resumed(db_session_factory):
    """A capped pass must stop, SAY it is still behind, and resume next pass.

    The failure this guards is not "the cap is wrong" — it is a capped pass that
    looks finished. That is the original defect (bounded progress, then a wait)
    wearing a new name.
    """
    factory = db_session_factory
    await _seed(
        factory,
        "catchup-cap",
        {_day(5): "1.00", _day(4): "2.00", _day(3): "3.00", _day(2): "4.00"},
    )

    first = await catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=2)

    assert first.rolled == [_day(5), _day(4)], "cap of 2, OLDEST first"
    assert first.due == 4
    assert first.remaining == 2
    assert first.complete is False, "two due buckets were left — this is not finished"
    assert om.rollup_catchup_remaining_days._value.get() == 2, (
        "the backlog must be PUBLISHED, not just logged"
    )
    assert await _cost_daily(factory, "catchup-cap") == {
        _day(5): Decimal("1.00"),
        _day(4): Decimal("2.00"),
    }, "the cap must actually bite — the two newest days are not written yet"

    second = await catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=2)

    assert second.rolled == [_day(3), _day(2)], "the next pass resumes where it stopped"
    assert second.remaining == 0
    assert second.complete is True
    assert om.rollup_catchup_remaining_days._value.get() == 0
    assert await _cost_daily(factory, "catchup-cap") == {
        _day(5): Decimal("1.00"),
        _day(4): Decimal("2.00"),
        _day(3): Decimal("3.00"),
        _day(2): Decimal("4.00"),
    }


@pytest.mark.unit
async def test_catchup_is_idempotent_across_reruns(db_session_factory):
    """rollup_daily deletes-then-rebuilds per bucket; a re-run must not double.

    Asserts the per-day SUMS *and* the physical row count — a sum-only assertion
    passes even if the rebuild left duplicate rows that a later regrouping would
    double.
    """
    factory = db_session_factory
    await _seed(factory, "catchup-idem", {_day(3): "5.55", _day(2): "6.66"})

    expected = {_day(3): Decimal("5.55"), _day(2): Decimal("6.66")}

    await catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=10)
    assert await _cost_daily(factory, "catchup-idem") == expected

    async with factory() as session:
        first_rows = (
            await session.execute(
                select(func.count())
                .select_from(CostDaily)
                .where(CostDaily.tenant_id == T("catchup-idem"))
            )
        ).scalar_one()

    # Second and third passes: the reconcile window is now what re-rolls these
    # days, which is the path a live pass actually takes every cycle.
    await catchup_daily(factory, floor=_day(10), reconcile_days=5, max_days=10)
    await catchup_daily(factory, floor=_day(10), reconcile_days=5, max_days=10)

    assert await _cost_daily(factory, "catchup-idem") == expected, "sums must not move"
    async with factory() as session:
        later_rows = (
            await session.execute(
                select(func.count())
                .select_from(CostDaily)
                .where(CostDaily.tenant_id == T("catchup-idem"))
            )
        ).scalar_one()
    assert later_rows == first_rows == 2, "no duplicate rows accumulate"


@pytest.mark.unit
async def test_lag_metric_separates_ran_from_caught_up(db_session_factory):
    """The instrument the incident was missing: lag, not "did it run".

    Two readings, not one: a stranded head reports its real distance (5), and a
    caught-up head reports 1. A gauge hardcoded to either value fails the other.
    """
    factory = db_session_factory
    await _seed(factory, "catchup-lag", {_day(5): "7.77"})
    await catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=10)

    lag = await daily_rollup_lag_days(factory)
    assert lag["cost_daily"] == 5, "the head is 5 days old and must say so"
    assert lag["metering_daily"] is None, "an empty table has no head to be behind"
    assert om.rollup_lag_days.labels(table="cost_daily")._value.get() == 5

    # Close the gap; the same instrument must now read healthy.
    await _seed(factory, "catchup-lag", {_day(1): "8.88"})
    await catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=10)

    lag = await daily_rollup_lag_days(factory)
    assert lag["cost_daily"] == 1, "healthy is 1 — the last closed day is rolled"
    assert om.rollup_lag_days.labels(table="cost_daily")._value.get() == 1


@pytest.mark.unit
async def test_overlapping_rollers_of_one_bucket_serialise(db_session_factory):
    """rollup_daily has TWO callers and they are not mutually excluded.

    ``services.aggregator.run_rollup_loop`` is leader-elected, but
    ``services.reprice.reprice_cost_events`` calls rollup_daily from an HTTP
    handler under no lock at all — once per batch-date and again per window-date.
    So an operator-triggered reprice can overlap the leader's pass, and two
    reprices can overlap each other, on the SAME bucket_date.

    Held externally, the per-bucket xact lock must block the roller; released, it
    must let it through. Both halves are asserted — a lock that always blocks and
    a lock that never blocks each fail one of them.
    """
    import asyncio

    from sqlalchemy import text

    from services.rollup_engine import ROLLUP_DAILY_LOCK_CLASSID, rollup_daily

    factory = db_session_factory
    target = _day(2)
    await _seed(factory, "catchup-lock", {target: "3.21"})

    blocker = factory()
    try:
        await blocker.execute(
            text("SELECT pg_advisory_xact_lock(:c, :o)"),
            {"c": ROLLUP_DAILY_LOCK_CLASSID, "o": target.toordinal()},
        )
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(rollup_daily(factory, target), timeout=3)
        assert await _cost_daily(factory, "catchup-lock") == {}, (
            "the blocked roller must not have written a partial bucket"
        )
    finally:
        await blocker.rollback()
        await blocker.close()

    # Lock released — the same call now completes.
    await asyncio.wait_for(rollup_daily(factory, target), timeout=10)
    assert await _cost_daily(factory, "catchup-lock") == {target: Decimal("3.21")}


@pytest.mark.unit
async def test_blocked_roller_waits_on_the_rollup_key_itself(db_session_factory):
    """A timeout proves the roller stopped. It does NOT prove WHY.

    The sibling test above asserts that a held lock makes ``rollup_daily`` hang,
    but a hang is consistent with a dozen unrelated causes (a busy pool, a row
    lock, a stuck connection), so on its own it is a coincidence, not a
    mechanism. This one names the mechanism: while the roller is blocked,
    ``pg_locks`` must show an UNGRANTED ``advisory`` entry whose
    ``(classid, objid)`` is exactly this bucket's key. That is the difference
    between "it waited" and "it waited on the lock this change added".
    """
    import asyncio

    from sqlalchemy import text

    from services.rollup_engine import ROLLUP_DAILY_LOCK_CLASSID, rollup_daily

    factory = db_session_factory
    target = _day(2)
    await _seed(factory, "catchup-lockwho", {target: "1.23"})

    blocker = factory()
    roller: asyncio.Task[int] | None = None
    try:
        await blocker.execute(
            text("SELECT pg_advisory_xact_lock(:c, :o)"),
            {"c": ROLLUP_DAILY_LOCK_CLASSID, "o": target.toordinal()},
        )

        roller = asyncio.create_task(rollup_daily(factory, target))

        waiters = 0
        for _ in range(60):  # up to ~6s for the roller to reach the lock
            await asyncio.sleep(0.1)
            async with factory() as observer:
                waiters = (
                    await observer.execute(
                        text(
                            # ⛔ THE `database` PREDICATE IS LOAD-BEARING, NOT TIDINESS.
                            # pg_locks is CLUSTER-wide: it lists advisory locks held
                            # from every database on the server, and each row carries
                            # the database oid. `classid` here is a CONSTANT and
                            # `objid` is a fixed test date's ordinal, so two lanes
                            # running this suite against the same Postgres SERVER --
                            # which they do whenever they share a container and
                            # separate only by database or TEST_SCHEMA -- generate the
                            # SAME (classid, objid) and each counts the OTHER's waiter.
                            # The assertion below is `waiters == 1`, so the victim sees
                            # 2 and reads it as a defect on its own diff.
                            # MEASURED 2026-09-08: pg_locks on this box spanned 3
                            # distinct databases at once.
                            # Same scoping lesson as the leaked-transaction guard in
                            # conftest, which is deliberately keyed on a pid-stamped
                            # application_name rather than on datname.
                            "SELECT count(*) FROM pg_locks "
                            "WHERE locktype = 'advisory' AND NOT granted "
                            "AND classid = :c AND objid = :o "
                            "AND database = ("
                            "  SELECT oid FROM pg_database WHERE datname = current_database()"
                            ")"
                        ),
                        {"c": ROLLUP_DAILY_LOCK_CLASSID, "o": target.toordinal()},
                    )
                ).scalar_one()
            if waiters:
                break

        assert waiters == 1, (
            "the roller must be parked on advisory key "
            f"({ROLLUP_DAILY_LOCK_CLASSID}, {target.toordinal()}) — a blocked "
            "roller with no waiter on THIS key is blocked on something else"
        )
        assert not roller.done(), "still blocked while the lock is held"
        assert await _cost_daily(factory, "catchup-lockwho") == {}, (
            "nothing written while parked — no partial bucket exists"
        )
    finally:
        await blocker.rollback()
        await blocker.close()
        if roller is not None:
            await asyncio.wait_for(roller, timeout=10)

    assert await _cost_daily(factory, "catchup-lockwho") == {target: Decimal("1.23")}


@pytest.mark.unit
async def test_two_concurrent_catchup_passes_neither_double_nor_shred(db_session_factory):
    """The overlap the code cannot prevent, run for real: TWO passes at once.

    ``services.aggregator.run_rollup_loop`` is leader-elected, but
    ``services.reprice.reprice_cost_events`` reaches ``rollup_daily`` from an
    HTTP handler holding no lock, so a live operator reprice genuinely overlaps
    the leader's pass. This drives both passes concurrently over the SAME four
    buckets and asserts the two properties an overlap can break:

    * NO DOUBLE COUNT — each day's total is its seeded amount, not a multiple.
      Asserted per-day with four DISTINCT values, so a doubling of any one bucket
      fails on that bucket alone.
    * NO SHREDDED BUCKET — exactly one physical row per day survives. A sum-only
      assertion cannot see the delete-then-rebuild overlay that leaves a stale
      duplicate behind, which is the actual failure mode the advisory lock
      exists to stop.

    Both passes must also individually report success: an overlap that makes one
    pass raise is not "safe", it is an outage with a clean table.

    ⚠️ MEASURED, SO IT IS NOT OVERSOLD: this test does NOT detect removal of the
    advisory lock. Deleting the ``pg_advisory_xact_lock`` call from
    ``rollup_daily`` leaves this test GREEN, because the corrupting interleaving
    needs one pass's DELETE to take its READ COMMITTED snapshot before the
    other's INSERT commits, and nothing here forces that ordering. It asserts the
    end-state property under genuine concurrency and is worth having for that.
    The MECHANISM detectors are the two tests above — both go red on that same
    mutant — and they are what stops the lock from being silently deleted. Do not
    read a green run of this test as proof the lock is present.
    """
    import asyncio

    factory = db_session_factory
    seeded = {
        _day(5): Decimal("11.01"),
        _day(4): Decimal("22.02"),
        _day(3): Decimal("33.03"),
        _day(2): Decimal("44.04"),
    }
    await _seed(factory, "catchup-race", {d: str(v) for d, v in seeded.items()})

    passes = await asyncio.gather(
        catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=10),
        catchup_daily(factory, floor=_day(10), reconcile_days=0, max_days=10),
        return_exceptions=True,
    )
    for i, p in enumerate(passes):
        assert not isinstance(p, BaseException), f"concurrent pass {i} raised: {p!r}"

    assert await _cost_daily(factory, "catchup-race") == seeded, (
        "every bucket must carry its own seeded total — a doubled value here is "
        "one pass's rows surviving alongside the other's rebuild"
    )

    async with factory() as session:
        per_day = (
            await session.execute(
                select(CostDaily.bucket_date, func.count())
                .where(CostDaily.tenant_id == T("catchup-race"))
                .group_by(CostDaily.bucket_date)
            )
        ).all()
    assert {r[0]: r[1] for r in per_day} == dict.fromkeys(seeded, 1), (
        "exactly one row per bucket — a second row is a stale overlay the "
        "delete-then-rebuild was supposed to have cleared"
    )
