"""A cost must be billed on the day it HAPPENED, not the day billing received it.

THE DEFECT
----------
``billing_cost_daily.bucket_date`` was cast from ``billing_cost_events.created_at``,
which is stamped at INGEST. A flush that crosses midnight therefore billed the
usage on the wrong day, and at a month boundary onto the wrong INVOICE. Normally
the lag is seconds; an observed consumer stall made it 62 hours.

The producer's own event time was on the wire the whole time — every measured
emitter sends ``timestamp`` = ``datetime.now(UTC).isoformat()`` and
``consumer._stage_message`` forwards the raw field dict verbatim — and billing
simply never read it. Alembic 0028 adds ``event_ts``; ``cost_flusher`` parses the
wire field into it; ``rollup_engine`` resolves ``coalesce(event_ts, created_at)``.

WHAT EACH TEST HERE PINS, AND HOW IT FAILS ON THE OLD CODE
----------------------------------------------------------
``test_a_midnight_crossing_event_bills_on_its_own_day`` is THE KILLING TEST: an
event emitted 2026-08-31T23:59:50Z and ingested 2026-09-01T00:00:05Z must roll
into the AUGUST bucket. On pre-0028 code the bucket is cast from ``created_at``,
so August gets nothing and September gets the charge — the wrong month's invoice.

The rest are controls in the directions this change could silently go wrong:

* the wire field must actually SURVIVE into the persisted row (the drop site was
  the flusher never reading it, and ``COST_METADATA_WHITELIST`` never listing it);
* a row with NO usable event time must still roll up — on ``created_at`` — and be
  COUNTED as a fallback, because two of the twelve live emitting ``service_key``s
  were never confirmed to send the field;
* a normal same-day event must bucket exactly as it did before;
* the coverage gate and the bucket cast must resolve the timestamp through the
  SAME expression, or the guard that decides whether to DELETE a bucket is
  answering a question about a different set of rows than the one it rewrites.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select, text

import observability.metrics as om
from models.db import CostDaily, CostEvent
from services._parsers import (
    COST_METADATA_WHITELIST,
    MAX_EVENT_TS_BACKDATE_DAYS,
    parse_event_ts,
)
from services.aggregator import flush_costs
from services.rollup_engine import cost_event_time, rollup_daily
from tests._aggregator_helpers import _make_cost
from tests.conftest import T

# The measured production shape: emitted at 23:59:50 on the last day of August,
# persisted 15 seconds later — i.e. in September, and therefore (pre-0028) on
# September's invoice.
EMITTED_AT = datetime(2026, 8, 31, 23, 59, 50, tzinfo=UTC)
INGESTED_AT = datetime(2026, 9, 1, 0, 0, 5, tzinfo=UTC)
AUGUST_DAY = date(2026, 8, 31)
SEPTEMBER_DAY = date(2026, 9, 1)


async def _force_created_at(session_factory, tenant_id: str, when: datetime) -> None:
    """Stamp ``created_at`` to simulate the ingest lag.

    The flusher cannot be asked to write a past ``created_at`` — that is the whole
    point of the column, it is ``now()`` at INSERT — so the lag is applied
    afterwards. This reproduces exactly what a stalled consumer produces: a row
    whose ``event_ts`` precedes its ``created_at``, here by 15 seconds across a
    month boundary (the observed stall was 62 hours, which is the same shape and a
    worse magnitude).
    """
    async with session_factory() as session:
        await session.execute(
            text("UPDATE billing_cost_events SET created_at = :ts WHERE tenant_id = :t"),
            {"ts": when, "t": tenant_id},
        )
        await session.commit()


def _fallback_counter(service_key: str) -> float:
    """Read the per-emitter event-time fallback counter.

    ⚠️ If this raises ``AttributeError``, the counter has not been added to
    ``src/observability/metrics.py`` yet. That file is owned elsewhere on this
    branch; the name needed is::

        cost_event_ts_fallback_total = Counter(
            "billing_cost_event_ts_fallback_total",
            "Cost-event rows bucketed on INGEST time (created_at) because the "
            "producer asserted no usable event_ts.",
            ["service_key"],
        )

    and, like every other series in that module, it must be BORN AT ZERO for the
    known service_keys so an absent series cannot read as "no fallbacks".
    """
    counter = getattr(om, "cost_event_ts_fallback_total", None)
    assert counter is not None, (
        "observability.metrics.cost_event_ts_fallback_total{service_key} does not "
        "exist. rollup_daily counts the event-time fallback through it, and that "
        "counter is the ONLY evidence that w2v-video-analyzer / "
        "w2v-video-compiler (the #2 spender) actually emit a wire timestamp. "
        "Until it exists and reads zero, 'all emitters send it' is an assumption. "
        "See this helper's docstring for the exact definition needed."
    )
    return counter.labels(service_key=service_key)._value.get()


# ---------------------------------------------------------------------------
# THE KILLING TEST
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_a_midnight_crossing_event_bills_on_its_own_day(db_session_factory):
    """Emitted 2026-08-31T23:59:50Z, ingested 2026-09-01T00:00:05Z → AUGUST.

    ⛔ FAILS ON UNMODIFIED CODE, and in the expensive direction: the pre-0028
    bucket is cast from ``created_at``, so August's rollup finds nothing at all
    and the charge appears on September's invoice instead. Both halves are
    asserted — the charge must be IN August and ABSENT from September — because
    asserting only the first would also pass if the row were billed twice.
    """
    tenant = T("event-time-crosser")
    buffer = [
        _make_cost(
            tenant_id=tenant,
            provider="deepseek",
            cost_usd="0.0400",
            timestamp=EMITTED_AT.isoformat(),
        )
    ]
    assert await flush_costs(buffer, db_session_factory, None) == 1
    await _force_created_at(db_session_factory, tenant, INGESTED_AT)

    await rollup_daily(db_session_factory, AUGUST_DAY)
    await rollup_daily(db_session_factory, SEPTEMBER_DAY)

    async with db_session_factory() as session:
        rows = (
            (await session.execute(select(CostDaily).where(CostDaily.tenant_id == tenant)))
            .scalars()
            .all()
        )

    assert [r.bucket_date for r in rows] == [AUGUST_DAY], (
        "the charge must be billed on the UTC day it was EMITTED (2026-08-31), not "
        f"the day it was ingested (2026-09-01); got {[r.bucket_date for r in rows]}"
    )
    assert rows[0].request_count == 1


@pytest.mark.unit
async def test_the_crossing_event_is_not_billed_twice(db_session_factory):
    """The charge moves day; it does not get counted on both.

    Guards the obvious wrong fix — widening the range predicate so the row
    satisfies both days.
    """
    tenant = T("event-time-no-double")
    buffer = [
        _make_cost(tenant_id=tenant, cost_usd="0.0200", timestamp=EMITTED_AT.isoformat()),
    ]
    assert await flush_costs(buffer, db_session_factory, None) == 1
    await _force_created_at(db_session_factory, tenant, INGESTED_AT)

    for day in (AUGUST_DAY, SEPTEMBER_DAY):
        await rollup_daily(db_session_factory, day)

    async with db_session_factory() as session:
        total = (
            await session.execute(
                select(func.coalesce(func.sum(CostDaily.request_count), 0)).where(
                    CostDaily.tenant_id == tenant
                )
            )
        ).scalar()
    assert total == 1, f"one event, one billed request — got {total}"


# ---------------------------------------------------------------------------
# THE WHITELIST CONTROL — the silent drop site
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_the_wire_timestamp_is_on_the_metadata_whitelist() -> None:
    """``timestamp`` must be whitelisted, and NOT for storage — for the counter.

    ``PLACEHOLDER_COUNTED_FIELDS`` is DERIVED from this tuple, so membership here
    is what births ``billing_placeholder_value_total{field="timestamp",
    service_key=…}`` — the only per-emitter measurement of whether the field is
    actually sent. A key the producer sends and this tuple omits is dropped
    silently and the row looks correct; that is exactly how ``worker_name`` went
    missing, and it is why this assertion exists at the tuple rather than only at
    the row.
    """
    assert "timestamp" in COST_METADATA_WHITELIST


@pytest.mark.unit
async def test_the_wire_timestamp_survives_into_the_persisted_row(db_session_factory):
    """Both halves of survival: the parsed column AND the raw wire evidence.

    ⛔ FAILS ON UNMODIFIED CODE — there is no ``event_ts`` column to survive into,
    and ``metadata`` never carried ``timestamp`` either. This is the test that
    would have caught the silent whitelist drop: asserting only ``event_ts`` would
    still pass while the raw value (the ONLY record of what the emitter actually
    sent, and the thing the placeholder counter is derived from) was discarded.
    """
    tenant = T("event-time-survives")
    buffer = [
        _make_cost(tenant_id=tenant, timestamp=EMITTED_AT.isoformat()),
    ]
    assert await flush_costs(buffer, db_session_factory, None) == 1

    async with db_session_factory() as session:
        row = (
            (await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant)))
            .scalars()
            .one()
        )
    assert row.event_ts is not None, "the wire timestamp was dropped before the INSERT"
    assert row.event_ts == EMITTED_AT
    assert (row.extra_metadata or {}).get("timestamp") == EMITTED_AT.isoformat(), (
        "the RAW wire timestamp must survive in metadata — it is the only evidence "
        "of what the emitter sent when parsing refuses the value"
    )
    # The two clocks stay two clocks. created_at is ingest time and must not be
    # overwritten with event time, nor event_ts with ingest time.
    assert row.created_at > row.event_ts


# ---------------------------------------------------------------------------
# THE FALLBACK CONTROL — load-bearing, not cosmetic
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_an_event_with_no_wire_timestamp_still_rolls_up_and_is_counted(
    db_session_factory,
):
    """No ``timestamp`` on the wire → NULL ``event_ts`` → bucket on ``created_at``.

    The fallback is not decoration. ⚠️ Ten of the twelve live emitting
    ``service_key``s were confirmed to use the shared emitter library;
    ``w2v-video-analyzer`` and ``w2v-video-compiler`` (the estate's #2 spender)
    were NOT. If a missing wire timestamp dropped the row out of the rollup, this
    change would silently stop billing the #2 spender.
    """
    tenant = T("event-time-fallback")
    service_key = "w2v-video-compiler"  # pragma: allowlist secret
    before = _fallback_counter(service_key)

    buffer = [_make_cost(tenant_id=tenant, service_key=service_key, cost_usd="0.1000")]
    assert await flush_costs(buffer, db_session_factory, None) == 1

    async with db_session_factory() as session:
        row = (
            (await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant)))
            .scalars()
            .one()
        )
        assert row.event_ts is None, "no wire timestamp must mean NULL, never a guess"
        ingest_day = row.created_at.astimezone(UTC).date()

    await rollup_daily(db_session_factory, ingest_day)

    async with db_session_factory() as session:
        rows = (
            (await session.execute(select(CostDaily).where(CostDaily.tenant_id == tenant)))
            .scalars()
            .all()
        )
    assert [r.bucket_date for r in rows] == [ingest_day], (
        "a row with no event_ts must still be billed, on its ingest day"
    )
    assert _fallback_counter(service_key) - before == 1, (
        "the fallback must be COUNTED per service_key — an uncounted fallback is "
        "how 'every emitter sends it' becomes an unfalsifiable assumption"
    )


@pytest.mark.unit
async def test_an_unparseable_wire_timestamp_falls_back_instead_of_raising(
    db_session_factory,
):
    """Garbage on the wire must not raise and must not guess — NULL, then fallback.

    A raise here would abort the whole batch (one malformed field stopping every
    other tenant's billing), and a guess would move money between months.
    """
    tenant = T("event-time-garbage")
    buffer = [_make_cost(tenant_id=tenant, timestamp="2026-13-45T99:99:99Z")]
    assert await flush_costs(buffer, db_session_factory, None) == 1

    async with db_session_factory() as session:
        row = (
            (await session.execute(select(CostEvent).where(CostEvent.tenant_id == tenant)))
            .scalars()
            .one()
        )
    assert row.event_ts is None
    # The raw value is still recoverable, which is the point of whitelisting it.
    assert (row.extra_metadata or {}).get("timestamp") == "2026-13-45T99:99:99Z"


# ---------------------------------------------------------------------------
# THE REGRESSION CONTROL — the normal case must not move
# ---------------------------------------------------------------------------


@pytest.mark.unit
async def test_a_same_day_event_buckets_exactly_as_before(db_session_factory):
    """The overwhelming majority of traffic: emit and ingest seconds apart.

    Both clocks agree, so the bucket must be identical to the pre-0028 answer.
    This is the control that would catch a resolution change that happens to fix
    the boundary case while moving every ordinary row.
    """
    tenant = T("event-time-same-day")
    emitted = datetime(2026, 8, 20, 12, 0, 0, tzinfo=UTC)
    buffer = [
        _make_cost(tenant_id=tenant, cost_usd="0.0300", timestamp=emitted.isoformat()),
    ]
    assert await flush_costs(buffer, db_session_factory, None) == 1
    await _force_created_at(db_session_factory, tenant, emitted + timedelta(seconds=2))

    await rollup_daily(db_session_factory, emitted.date())

    async with db_session_factory() as session:
        rows = (
            (await session.execute(select(CostDaily).where(CostDaily.tenant_id == tenant)))
            .scalars()
            .all()
        )
    assert [r.bucket_date for r in rows] == [date(2026, 8, 20)]
    assert rows[0].request_count == 1


# ---------------------------------------------------------------------------
# THE GATE MUST ASK THE SAME QUESTION AS THE THING IT GATES
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_the_resolved_expression_is_one_shared_object() -> None:
    """Bucket cast, coverage gate and gap detector must share ONE expression.

    ``rollup_daily`` derives a bucket_date from the resolved event time AND gates
    a DELETE of that bucket on it. A gate that resolves the timestamp differently
    from the thing it gates answers a question about a different population —
    the defect shape that destroyed the 2026-09-02..09-04 cost buckets. Pinned by
    source inspection because the property is "one object, referenced three
    times", which no behavioural assertion can distinguish from "three
    coincidentally-identical spellings".
    """
    import inspect

    import services.rollup_engine as re_mod

    compiled = str(cost_event_time.compile(compile_kwargs={"literal_binds": True}))
    assert "coalesce" in compiled.lower()
    assert "event_ts" in compiled and "created_at" in compiled

    src = inspect.getsource(re_mod.rollup_daily)
    assert "cost_event_time < day_end" in src, (
        "the guarded-delete coverage gate no longer reads the shared resolved "
        "expression; it and the bucket cast can now disagree about which rows "
        "belong to this day"
    )
    assert 'func.cast(func.timezone("UTC", cost_event_time), Date) == target_date' in src, (
        "the bucket-date cast no longer reads the shared resolved expression"
    )
    # The sargable upper bound on the partition key is what made the old query
    # blind to a midnight-crossing event. It must stay gone.
    assert "CostEvent.created_at < day_end" not in src, (
        "a `created_at < day_end` bound is back in rollup_daily — it excludes "
        "exactly the late-ingested rows the resolved cast exists to capture"
    )
    assert "CostEvent.created_at >= day_start" in src, (
        "the sargable LOWER bound is gone; the query no longer prunes partitions. "
        "It is sound because parse_event_ts refuses a future event_ts, so "
        "event_ts <= created_at for every stored row"
    )
    gaps = inspect.getsource(re_mod.missing_daily_dates)
    assert 'func.cast(func.timezone("UTC", cost_event_time), Date)' in gaps, (
        "the gap detector still asks on created_at while rollup_daily writes "
        "buckets by event time — it would report permanent phantom gaps"
    )


@pytest.mark.unit
async def test_coverage_is_proven_by_the_resolved_event_time(db_session_factory):
    """A bucket whose day has no source rows is cleared only if history covers it.

    With only a single late-ingested August event in the table, re-rolling
    September must still clear a stale September bucket: the resolved event time
    (2026-08-31) precedes September's end, so history demonstrably reaches back
    through that day and an empty result is a real measurement, not a truncation.
    """
    tenant = T("event-time-coverage")
    buffer = [_make_cost(tenant_id=tenant, timestamp=EMITTED_AT.isoformat())]
    assert await flush_costs(buffer, db_session_factory, None) == 1
    await _force_created_at(db_session_factory, tenant, INGESTED_AT)

    # A stale September bucket, as a pre-0028 rollup would have written it.
    async with db_session_factory() as session:
        session.add(
            CostDaily(
                tenant_id=tenant,
                tenant_name="",
                license_id=uuid.UUID(int=0),
                subject_id=uuid.UUID(int=0),
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                bucket_date=SEPTEMBER_DAY,
                total_input_tokens=1000,
                total_output_tokens=200,
                total_cost_usd=Decimal("0.0165"),
                request_count=1,
            )
        )
        await session.commit()

    await rollup_daily(db_session_factory, SEPTEMBER_DAY)

    async with db_session_factory() as session:
        september = (
            (
                await session.execute(
                    select(CostDaily).where(
                        CostDaily.tenant_id == tenant,
                        CostDaily.bucket_date == SEPTEMBER_DAY,
                    )
                )
            )
            .scalars()
            .all()
        )
    assert september == [], (
        "the stale September bucket must be cleared: the event belongs to August "
        "and the coverage gate, resolved the same way, proves history covers "
        "September"
    )


# ---------------------------------------------------------------------------
# THE PARSER — refuse, never guess
# ---------------------------------------------------------------------------


class TestParseEventTs:
    """Every refusal is a deliberate answer, not a gap."""

    RECEIVED = datetime(2026, 9, 1, 0, 0, 5, tzinfo=UTC)

    @pytest.mark.unit
    def test_an_iso_string_with_an_offset_is_accepted_and_normalised(self) -> None:
        assert parse_event_ts("2026-08-31T23:59:50+00:00", received_at=self.RECEIVED) == EMITTED_AT
        assert parse_event_ts("2026-08-31T23:59:50Z", received_at=self.RECEIVED) == EMITTED_AT
        # A non-UTC offset is honoured and normalised, not assumed to be UTC.
        assert parse_event_ts("2026-09-01T01:59:50+02:00", received_at=self.RECEIVED) == EMITTED_AT

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("value", "why"),
        [
            (None, "nothing was asserted"),
            ("", "nothing was asserted"),
            ("   ", "whitespace is absence"),
            ("not-a-timestamp", "a malformed instant is not an instant"),
            ("2026-13-45T99:99:99Z", "out-of-range components"),
            ("2026-08-31T23:59:50", "NAIVE — the producer's zone is unknown to billing"),
            (1790899190, "epoch int is ambiguous between seconds and milliseconds"),
            (1790899190.0, "epoch float, same ambiguity"),
        ],
    )
    def test_it_refuses_rather_than_guesses(self, value, why) -> None:
        assert parse_event_ts(value, received_at=self.RECEIVED) is None, why

    @pytest.mark.unit
    def test_it_refuses_an_event_time_after_ingest(self) -> None:
        """⛔ Load-bearing: this refusal is what makes ``event_ts <= created_at``
        an invariant, and ``rollup_daily``'s sargable lower bound on the partition
        key is only sound because of it. Relax this and rows vanish from rollups.
        """
        future = (self.RECEIVED + timedelta(seconds=1)).isoformat()
        assert parse_event_ts(future, received_at=self.RECEIVED) is None
        # The boundary itself is fine: equal is not after.
        assert parse_event_ts(self.RECEIVED.isoformat(), received_at=self.RECEIVED) == self.RECEIVED

    @pytest.mark.unit
    def test_it_refuses_an_event_time_older_than_the_rollup_can_reach(self) -> None:
        """Past retention, no bucket is ever rolled — honouring it means never billing.

        Falling back to ``created_at`` bills the row on its ingest day: the wrong
        day, but billed. Accepting the value would route it to a bucket nothing
        aggregates, which is revenue silently lost.
        """
        too_old = self.RECEIVED - timedelta(days=MAX_EVENT_TS_BACKDATE_DAYS + 1)
        assert parse_event_ts(too_old.isoformat(), received_at=self.RECEIVED) is None
        just_inside = self.RECEIVED - timedelta(days=MAX_EVENT_TS_BACKDATE_DAYS - 1)
        assert parse_event_ts(just_inside.isoformat(), received_at=self.RECEIVED) == just_inside
