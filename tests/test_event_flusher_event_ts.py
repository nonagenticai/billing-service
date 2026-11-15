"""``event_flusher`` must actually WRITE ``MeteringEvent.event_ts``.

WHY THIS FILE EXISTS
--------------------
Alembic ``0034_metering_event_event_time`` added ``billing_metering_events.event_ts``
and moved the metering rollup onto ``coalesce(event_ts, created_at)`` so a
request's per-request surcharge lands in the same billing period as its LLM cost
(alembic 0028 did the cost half; leaving metering on ingest time split ONE
request across TWO immutable invoices).

⛔ THAT FIX WAS INERT. The schema was ready and the READER was ready, but
``services/event_flusher.py`` — the one and only writer of this table — never
read the wire ``timestamp``, so every new row had ``event_ts IS NULL`` and was
bucketed on ``created_at`` exactly as before. A green rollup test proves the
reader handles the column; it cannot prove anything ever populates it. This file
covers the producer half, which is the half that was missing.

The wire genuinely carries the field: measured 2026-09-05 on the live
``billing:events`` stream, whose fields are tenant/subject/license/app/plan,
method, path, status, ``source_ip``, ``timestamp``, latency — recorded at
``services/consumer.py:286``.

⚠️ NULL IS A RESULT, NOT A FAILURE, and the refusal cases below are the point.
``services._parsers.parse_event_ts`` is REUSED rather than reimplemented, and it
returns ``None`` for anything it cannot vouch for. NULL is load-bearing: the
reader falls back to the known-true ingest instant and COUNTS the fallback. A
wrongly-parsed event time is strictly worse, because ``coalesce`` would then
silently prefer the wrong clock and move a request into another calendar month's
immutable invoice with nothing flagged.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from models.db import MeteringEvent
from services.aggregator import flush_events
from tests.conftest import T

pytestmark = pytest.mark.unit


def _event(*, tenant: str, **over) -> dict:
    event = {
        "tenant_id": T(tenant),
        "license_id": str(uuid.uuid4()),
        "app_id": str(uuid.uuid4()),
        "plan_id": str(uuid.uuid4()),
        "method": "GET",
        "path": "/workers/api/tasks",
        "status": "allowed",
        "source_ip": "10.0.0.1",
        "_service_key": "workers",  # pragma: allowlist secret
    }
    event.update(over)
    return event


async def _only_row(session_factory) -> MeteringEvent:
    async with session_factory() as session:
        rows = list((await session.execute(select(MeteringEvent))).scalars().all())
    assert len(rows) == 1
    return rows[0]


async def test_a_wire_timestamp_lands_in_event_ts(db_session_factory):
    """THE PRODUCER HALF. A producer-asserted instant must reach the column.

    Without this, ``0034`` and the rollup's ``coalesce`` are dead weight: the
    column exists, the reader prefers it, and it is NULL on every row forever.
    """
    emitted = datetime.now(UTC) - timedelta(minutes=5)
    event = _event(tenant="ets-a", timestamp=emitted.isoformat())

    assert await flush_events([event], db_session_factory, None) == 1

    row = await _only_row(db_session_factory)
    assert row.event_ts is not None, (
        "the wire carried a usable `timestamp`; a NULL here means the writer "
        "still drops it on the floor and 0034 remains inert"
    )
    assert row.event_ts == emitted


async def test_no_wire_timestamp_leaves_event_ts_null(db_session_factory):
    """NEGATIVE CONTROL — and the documented fallback contract.

    An emitter that asserts nothing must produce NULL, so the reader's
    ``coalesce(event_ts, created_at)`` falls back to the known-true ingest
    instant and counts the fallback. ⛔ Substituting ``now``/``created_at`` here
    would restate ingest time as producer event time under a column that
    promises otherwise, and would make the fallback counter read zero while
    measuring nothing.

    This control also stops the test above passing for the wrong reason: a
    writer that stamped every row with the flush instant would satisfy "not
    NULL" while asserting a value no producer ever sent.
    """
    assert await flush_events([_event(tenant="ets-b")], db_session_factory, None) == 1

    assert (await _only_row(db_session_factory)).event_ts is None


async def test_a_naive_wire_timestamp_is_refused_rather_than_guessed(db_session_factory):
    """NEGATIVE CONTROL for the PARSER being the real one, not a local re-do.

    ``parse_event_ts`` refuses a naive timestamp: billing does not know the
    producer's local zone, and stamping the wrong one on a month boundary moves
    a charge into the wrong INVOICE. A hand-rolled ``fromisoformat`` in the
    flusher would accept it and look green here — which is exactly the drift
    this asserts against.
    """
    naive = (datetime.now(UTC) - timedelta(minutes=5)).replace(tzinfo=None)
    event = _event(tenant="ets-c", timestamp=naive.isoformat())

    assert await flush_events([event], db_session_factory, None) == 1

    assert (await _only_row(db_session_factory)).event_ts is None, (
        "a naive instant is ambiguous; refusing it shows up as a counted "
        "fallback instead of a silently mis-bucketed charge"
    )


async def test_a_future_wire_timestamp_is_refused(db_session_factory):
    """NEGATIVE CONTROL for the skew bound, which the rollup DEPENDS on.

    Every stored ``event_ts`` must satisfy ``event_ts <= received_at <=
    created_at``. That invariant is what lets the daily rollup keep a sargable
    lower bound on ``created_at``; relaxing it silently drops rows from rollups.
    A batch-wide ``received_at`` captured before the loop is what makes this
    verdict deterministic rather than dependent on an event's position.
    """
    ahead = datetime.now(UTC) + timedelta(hours=2)
    event = _event(tenant="ets-d", timestamp=ahead.isoformat())

    assert await flush_events([event], db_session_factory, None) == 1

    assert (await _only_row(db_session_factory)).event_ts is None
