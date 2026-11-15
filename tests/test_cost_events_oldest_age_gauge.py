"""Tests for `billing_cost_events_oldest_age_seconds`.

`services.rollup_engine.cost_events_oldest_age_seconds` publishes how far back the
cost ledger actually reaches.

🔴 WHY IT EXISTS. On 2026-09-05T07:01:56Z billing_cost_events was TRUNCATEd and
NOTHING NOTICED FOR SIX DAYS. Every instrument the service had measured FLOW —
events in, events flushed, rollup cycles, the lag of the NEWEST bucket — and flow
was healthy throughout, because ingest resumed within the minute. Nothing
described the ledger's DEPTH, so a ledger holding 90 days and one holding nine
minutes were the same green dashboard.
"""

import inspect
import math
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from models.db import CostEvent
from services.aggregator import run_rollup_loop
from services.rollup_engine import cost_events_oldest_age_seconds
from tests.conftest import T

pytestmark = pytest.mark.unit

_DAY = 86400.0


def _gauge() -> float:
    import observability.metrics as om

    return om.cost_events_oldest_age_seconds._value.get()


async def _add_cost(db_session_factory, *, created_at: datetime, event_ts=None) -> None:
    async with db_session_factory() as session:
        session.add(
            CostEvent(
                tenant_id=T("oldest-age-t"),
                license_id=uuid.uuid4(),
                service_key="inference-router",  # pragma: allowlist secret
                provider="claude",
                input_tokens=100,
                output_tokens=10,
                cost_usd=Decimal("0.01"),
                created_at=created_at,
                event_ts=event_ts,
            )
        )
        await session.commit()


async def test_gauge_reflects_the_oldest_retained_row(db_session_factory):
    """Two rows, ten days apart: the gauge must describe the OLDER one.

    Seeded with two DISTINCT ages rather than one row, so a `max()` where `min()`
    belongs — which would report the ledger as far shallower than it is, the
    reassuring direction — cannot pass.
    """
    now = datetime.now(UTC)
    await _add_cost(db_session_factory, created_at=now - timedelta(days=10))
    await _add_cost(db_session_factory, created_at=now - timedelta(days=2))

    age = await cost_events_oldest_age_seconds(db_session_factory, now=now)

    assert age == pytest.approx(10 * _DAY, abs=60)
    assert _gauge() == pytest.approx(10 * _DAY, abs=60)


async def test_an_empty_ledger_publishes_zero_and_never_nan(db_session_factory):
    """⛔ THE WHOLE POINT. An empty cost ledger must publish a FIREABLE number.

    This is deliberately the INVERSE of `billing_cost_dead_letter_oldest_age_seconds`
    and of `metering_hourly_lag_hours`, which abstain (NaN / no sample) when their
    subject is empty — for them emptiness is healthy and there is no head to
    measure. Here emptiness IS the incident of 2026-09-05, so NaN would be exactly
    wrong: NaN fails every comparison, so an alert could never fire on the one
    state the gauge was added to catch.
    """
    # The table is TRUNCATEd between tests by the db_engine fixture, so this IS
    # the post-truncate state the gauge has to describe.
    age = await cost_events_oldest_age_seconds(db_session_factory)

    assert age == 0.0
    assert not math.isnan(_gauge()), (
        "an empty ledger must publish a comparable 0, not an unfireable NaN"
    )
    assert _gauge() == 0.0


async def test_depth_is_measured_from_created_at_not_from_event_ts(db_session_factory):
    """A backdated producer `event_ts` must not make a just-emptied ledger look deep.

    `parse_event_ts` accepts an event time up to 90 days old, so one backdating
    emitter would pin a `coalesce(event_ts, created_at)` version of this gauge at a
    comfortable ~60 days across a ledger written minutes ago — loudest exactly when
    it was least true. Retention and truncation are properties of `created_at`.
    """
    now = datetime.now(UTC)
    await _add_cost(
        db_session_factory,
        created_at=now - timedelta(minutes=5),
        event_ts=now - timedelta(days=60),
    )

    age = await cost_events_oldest_age_seconds(db_session_factory, now=now)

    assert age == pytest.approx(300, abs=60)
    assert age is not None
    assert age < _DAY, (
        "the ledger was written five minutes ago; reading 60 days here would "
        "report a depth the table does not have"
    )


def test_the_rollup_loop_actually_calls_it():
    """A published gauge nobody refreshes is an instrument that reports one value
    forever — and this estate has shipped that shape before.

    This pins the CALL SITE, not the behaviour of the loop (which needs the full
    leader-election harness in tests/test_aggregator.py). It fails if the
    measurement is ever declared, imported and left unwired — which is the failure
    mode a unit test of the function alone cannot see.
    """
    src = inspect.getsource(run_rollup_loop)
    assert "cost_events_oldest_age_seconds(" in src, (
        "run_rollup_loop does not call cost_events_oldest_age_seconds — the gauge "
        "would hold its import-time NaN for the life of the process"
    )
