"""One request's money must not be split across two invoices.

THE DEFECT
----------
A metered request produces money on TWO buses. The COST bus
(``billing_cost_events`` → ``billing_cost_daily``) was moved onto the PRODUCER's
clock by alembic 0028. The METERING bus (``billing_metering_events`` →
``billing_metering_hourly`` → ``billing_metering_daily``), whose ``request_count``
feeds the per-app rate and the per-request surcharge in
``services.billing.compute_billed_line``, was left on INGEST time.

So a request emitted at 23:59:50 on the last day of month M, whose cost flushes
promptly but whose metering event is ingested after midnight, billed its LLM cost
in month M and its per-request surcharge in month M+1. Both invoices are wrong
and neither is amendable — ``billing_invoices`` rows are immutable and gaplessly
numbered.

The producer's event time was on the wire the whole time: ``services/consumer.py``
records a 2026-09-05 live measurement of ``billing:events`` naming ``timestamp``
among its fields. Alembic 0034 adds ``MeteringEvent.event_ts``; ``rollup_engine``
resolves ``coalesce(event_ts, created_at)`` through the SAME factory that builds
the cost side's expression.

WHAT EACH TEST PINS, AND HOW IT FAILS ON THE OLD CODE
------------------------------------------------------
* ``test_both_halves_of_a_month_straddling_request_land_in_the_same_month`` is
  THE KILLING TEST: cost and metering for one request must reach the SAME month.
  On pre-0034 code the metering half lands in month M+1 while the cost half is
  already in M — one request, two invoices.
* ``test_the_metering_hour_comes_from_the_producer_not_from_ingest`` fails the
  instant the hourly rollup reverts to bucketing on ``created_at``.
* ``test_a_row_with_no_event_ts_buckets_exactly_as_before`` is the NO-REGRESSION
  CONTROL: every pre-0034 row has ``event_ts IS NULL`` and must keep billing in
  its ingest hour, bit-for-bit. That is what makes the change unable to move a
  closed period.
* the remaining tests are controls in the directions this could silently go
  wrong: double-counting, and a late arrival vanishing instead of moving.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from models.db import CostDaily, CostEvent, MeteringDaily, MeteringEvent, MeteringHourly
from services.rollup_engine import (
    cost_event_time,
    late_event_hours,
    metering_event_time,
    missing_event_hours,
    rollup_daily,
    rollup_hourly,
)
from tests.conftest import T

# The measured production shape, identical to the cost side's fixture in
# tests/test_rollup_event_time.py so the two buses are compared on ONE request.
EMITTED_AT = datetime(2026, 8, 31, 23, 59, 50, tzinfo=UTC)
INGESTED_AT = datetime(2026, 9, 1, 0, 0, 5, tzinfo=UTC)
AUGUST_HOUR = datetime(2026, 8, 31, 23, 0, tzinfo=UTC)
SEPTEMBER_HOUR = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
AUGUST_DAY = date(2026, 8, 31)
SEPTEMBER_DAY = date(2026, 9, 1)


def _metering_event(
    *,
    tenant_id: str,
    created_at: datetime,
    event_ts: datetime | None = None,
    service_key: str = "fr-backend",
    status: str = "allowed",
) -> MeteringEvent:
    """A raw metering row with BOTH clocks set explicitly.

    ``created_at`` is written directly rather than via the flusher for the same
    reason the cost side's ``_force_created_at`` exists: ``created_at`` is
    ``now()`` at INSERT by construction, so an ingest lag cannot be asked for, it
    can only be stamped. The pair (``event_ts`` before ``created_at``) is exactly
    what a stalled or slow consumer produces.
    """
    return MeteringEvent(
        tenant_id=tenant_id,
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
        app_id=uuid.uuid4(),
        plan_id=uuid.uuid4(),
        method="POST",
        path="/api/agent/run",
        service_key=service_key,
        status=status,
        event_ts=event_ts,
        created_at=created_at,
    )


async def _seed(session_factory, *rows) -> None:
    async with session_factory() as session:
        session.add_all(list(rows))
        await session.commit()


async def _hourly_counts(session_factory, tenant_id: str) -> dict[datetime, int]:
    async with session_factory() as session:
        result = await session.execute(
            select(MeteringHourly.bucket_hour, func.sum(MeteringHourly.request_count))
            .where(MeteringHourly.tenant_id == tenant_id)
            .group_by(MeteringHourly.bucket_hour)
        )
    return {row[0]: int(row[1]) for row in result.all()}


# ---------------------------------------------------------------------------
# THE KILLING TEST — one request, one month
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_both_halves_of_a_month_straddling_request_land_in_the_same_month(
    db_session_factory,
):
    """Cost and metering for ONE request must reach the SAME billing period.

    ⛔ FAILS ON UNMODIFIED CODE, and this is the shape that cannot be corrected
    afterwards: the cost half is already bucketed on producer time (alembic 0028),
    so on pre-0034 code August's invoice carries the LLM cost with NO matching
    request, and September's carries a per-request surcharge with NO matching
    cost. Both rows are immutable and gaplessly numbered.

    Asserted in both directions on both buses — an assertion that only checked
    "August has the request" would also pass if the request were counted twice.
    """
    tenant = T("metering-event-time-split")

    async with db_session_factory() as session:
        session.add(
            CostEvent(
                tenant_id=tenant,
                license_id=uuid.uuid4(),
                subject_id=uuid.uuid4(),
                app_id=uuid.uuid4(),
                service_key="fr-backend",  # pragma: allowlist secret
                provider="anthropic",
                model="claude-sonnet-4-20250514",
                served_model="claude-sonnet-4-20250514",
                input_tokens=1000,
                output_tokens=200,
                cost_usd=Decimal("0.01650000"),
                # The producer's clock — the same instant the metering half carries.
                event_ts=EMITTED_AT,
                created_at=INGESTED_AT,
            )
        )
        session.add(_metering_event(tenant_id=tenant, created_at=INGESTED_AT, event_ts=EMITTED_AT))
        await session.commit()

    # Roll the hour the request HAPPENED in and the hour it ARRIVED in, so the
    # test cannot pass merely because the later hour was never rolled.
    await rollup_hourly(db_session_factory, AUGUST_HOUR)
    await rollup_hourly(db_session_factory, SEPTEMBER_HOUR)
    for day in (AUGUST_DAY, SEPTEMBER_DAY):
        await rollup_daily(db_session_factory, day)

    async with db_session_factory() as session:
        metering = {
            row[0]: int(row[1])
            for row in (
                await session.execute(
                    select(MeteringDaily.bucket_date, func.sum(MeteringDaily.request_count))
                    .where(MeteringDaily.tenant_id == tenant)
                    .group_by(MeteringDaily.bucket_date)
                )
            ).all()
        }
        cost_days = [
            row[0]
            for row in (
                await session.execute(
                    select(CostDaily.bucket_date).where(CostDaily.tenant_id == tenant)
                )
            ).all()
        ]

    assert cost_days == [AUGUST_DAY], (
        "control: the COST half is already on producer time (alembic 0028); if "
        f"this is not August the fixture is wrong, not the fix. Got {cost_days}"
    )
    assert metering == {AUGUST_DAY: 1}, (
        "the per-request surcharge must be billed in the SAME period as the LLM "
        "cost of the same request. Pre-0034 this reads {september: 1} — the "
        f"request lands on September's immutable invoice. Got {metering}"
    )


# ---------------------------------------------------------------------------
# THE BUCKETING TEST — fails if the hourly rollup reverts to created_at
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_the_metering_hour_comes_from_the_producer_not_from_ingest(db_session_factory):
    """Emitted 23:59:50, ingested 00:00:05 → the 23:00 bucket, not 00:00.

    ⛔ THIS IS THE TEST THAT FAILS IF THE ROLLUP REVERTS TO ``created_at``. Both
    hours are rolled and both are asserted, so restoring the old
    ``created_at``-windowed predicate flips the populated bucket and reddens this
    immediately — it cannot be satisfied by widening a range, only by resolving
    the event time.
    """
    tenant = T("metering-hour-from-producer")
    await _seed(
        db_session_factory,
        _metering_event(tenant_id=tenant, created_at=INGESTED_AT, event_ts=EMITTED_AT),
    )

    await rollup_hourly(db_session_factory, AUGUST_HOUR)
    await rollup_hourly(db_session_factory, SEPTEMBER_HOUR)

    assert await _hourly_counts(db_session_factory, tenant) == {AUGUST_HOUR: 1}, (
        "the event belongs to the hour its PRODUCER stamped it in; bucketing on "
        "created_at puts it in the 00:00 hour of the next month"
    )


@pytest.mark.unit
async def test_the_crossing_event_is_not_counted_in_both_hours(db_session_factory):
    """The request moves hour; it is not counted in both.

    Guards the obvious wrong fix — widening the window predicate so the row
    satisfies the old hour and the new one. A double-counted request is a
    double-charged surcharge, on a row nothing can amend.
    """
    tenant = T("metering-hour-no-double")
    await _seed(
        db_session_factory,
        _metering_event(tenant_id=tenant, created_at=INGESTED_AT, event_ts=EMITTED_AT),
    )

    for hour in (AUGUST_HOUR, SEPTEMBER_HOUR):
        await rollup_hourly(db_session_factory, hour)

    async with db_session_factory() as session:
        total = (
            await session.execute(
                select(func.coalesce(func.sum(MeteringHourly.request_count), 0)).where(
                    MeteringHourly.tenant_id == tenant
                )
            )
        ).scalar()
    assert total == 1, f"one request, one billed count — got {total}"


# ---------------------------------------------------------------------------
# THE NO-REGRESSION CONTROL — why no closed period can move
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_a_row_with_no_event_ts_buckets_exactly_as_before(db_session_factory):
    """NULL ``event_ts`` → bucket on ``created_at``, bit-for-bit as pre-0034.

    🔴 THIS IS THE ARGUMENT THAT THE CHANGE CANNOT MOVE A CLOSED PERIOD. Alembic
    0034 is forward-only with no backfill, so EVERY row written before it has
    ``event_ts IS NULL`` and resolves through ``coalesce`` to the identical
    ``created_at`` bucket it already has. Historical hours, days, months and
    invoices are therefore untouched by construction, not by luck.

    It is also load-bearing forward: ``billing:events`` has TWO producers
    (fr-backend ~99.5% via a globally-installed ``withUsageSpans``, and
    license-authz) and neither was confirmed to send a usable wire ``timestamp``
    on every path. If a missing event time dropped the row out of the rollup this
    change would silently stop billing ~99.5% of metered requests.
    """
    tenant = T("metering-no-event-ts")
    await _seed(
        db_session_factory,
        _metering_event(tenant_id=tenant, created_at=INGESTED_AT, event_ts=None),
    )

    await rollup_hourly(db_session_factory, AUGUST_HOUR)
    await rollup_hourly(db_session_factory, SEPTEMBER_HOUR)

    assert await _hourly_counts(db_session_factory, tenant) == {SEPTEMBER_HOUR: 1}, (
        "a row with no producer event time must keep billing on its INGEST hour "
        "— the pre-0034 behaviour, unchanged, for 100% of existing rows"
    )


@pytest.mark.unit
async def test_the_two_clocks_stay_two_clocks(db_session_factory):
    """``created_at`` is not overwritten with event time, nor the reverse.

    A fix that collapsed the two columns would satisfy every bucketing assertion
    above while destroying the ingest clock that retention, the truncate gates and
    ``cost_events_oldest_age_seconds`` are all built on.
    """
    tenant = T("metering-two-clocks")
    await _seed(
        db_session_factory,
        _metering_event(tenant_id=tenant, created_at=INGESTED_AT, event_ts=EMITTED_AT),
    )

    async with db_session_factory() as session:
        row = (
            (await session.execute(select(MeteringEvent).where(MeteringEvent.tenant_id == tenant)))
            .scalars()
            .one()
        )
    assert row.event_ts == EMITTED_AT
    assert row.created_at == INGESTED_AT
    assert row.created_at > row.event_ts


# ---------------------------------------------------------------------------
# THE LATE-ARRIVAL CONTROL — the way this fix could have lost money
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_an_event_arriving_after_its_hour_closed_is_named_and_recovered(
    db_session_factory,
):
    """A row delivered hours late must be counted, not silently dropped.

    🔴 THE FAILURE THIS EXISTS TO PREVENT. Moving the bucket onto the producer's
    clock means a row can arrive for an hour that was rolled long ago, and the
    hourly UPSERT REPLACES a bucket rather than adding to it — so an hour nothing
    revisits stays permanently short. ``missing_event_hours`` structurally cannot
    see it (the bucket EXISTS, it is merely incomplete). ``late_event_hours``
    names it, and ``run_rollup_loop`` re-rolls exactly those hours.

    Under the old ``created_at`` bucketing this class did not exist, so shipping
    event-time bucketing WITHOUT this half would trade a wrong-period charge for a
    silent under-count — the strictly worse of the two.
    """
    tenant = T("metering-late-arrival")
    late_ingest = EMITTED_AT + timedelta(hours=6)

    # First: the hour is rolled while the late row has not arrived yet.
    await _seed(
        db_session_factory,
        _metering_event(
            tenant_id=tenant,
            created_at=EMITTED_AT + timedelta(seconds=1),
            event_ts=EMITTED_AT,
        ),
    )
    await rollup_hourly(db_session_factory, AUGUST_HOUR)
    assert await _hourly_counts(db_session_factory, tenant) == {AUGUST_HOUR: 1}

    # Now the stalled consumer delivers a second row for that same, already-rolled
    # hour, six hours late.
    await _seed(
        db_session_factory,
        _metering_event(tenant_id=tenant, created_at=late_ingest, event_ts=EMITTED_AT),
    )

    assert await _hourly_counts(db_session_factory, tenant) == {AUGUST_HOUR: 1}, (
        "precondition: the already-rolled bucket is short until something "
        "re-rolls it — this is the money that would be lost"
    )

    floor = EMITTED_AT - timedelta(days=1)
    assert AUGUST_HOUR not in await missing_event_hours(db_session_factory, floor), (
        "missing_event_hours must NOT report this hour — the bucket exists. That "
        "blind spot is exactly why late_event_hours had to be a separate function"
    )
    late = await late_event_hours(db_session_factory, floor)
    assert AUGUST_HOUR in late, f"the hour that received a late row must be named; got {late}"

    for hour in late:
        await rollup_hourly(db_session_factory, hour)

    assert await _hourly_counts(db_session_factory, tenant) == {AUGUST_HOUR: 2}, (
        "after the re-roll both requests are counted, in the hour they HAPPENED in"
    )


@pytest.mark.unit
async def test_a_punctual_hour_is_not_reported_as_late(db_session_factory):
    """The negative control: a row that arrived inside its own hour is not named.

    Without this, ``late_event_hours`` returning every hour unconditionally would
    still pass the test above while making the daily pass re-roll the entire
    reconcile window on every run.
    """
    tenant = T("metering-punctual")
    await _seed(
        db_session_factory,
        _metering_event(
            tenant_id=tenant,
            created_at=EMITTED_AT + timedelta(seconds=2),
            event_ts=EMITTED_AT,
        ),
    )
    late = await late_event_hours(db_session_factory, EMITTED_AT - timedelta(days=1))
    assert AUGUST_HOUR not in late, (
        f"an event ingested 2s after it was emitted did not arrive late; got {late}"
    )


# ---------------------------------------------------------------------------
# THE SHARED-EXPRESSION CONTROL
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_both_buses_resolve_event_time_through_the_same_construction() -> None:
    """One factory, two expressions — the two money axes cannot be edited apart.

    The defect was not that either bus was wrong in isolation; it was that they
    disagreed. A future edit that re-spells one COALESCE at a call site
    reintroduces exactly that, so the shape is asserted rather than trusted.
    """
    for expr, model in ((cost_event_time, CostEvent), (metering_event_time, MeteringEvent)):
        compiled = str(expr)
        assert "coalesce" in compiled.lower(), compiled
        assert f"{model.__tablename__}.event_ts" in compiled, compiled
        assert f"{model.__tablename__}.created_at" in compiled, compiled
