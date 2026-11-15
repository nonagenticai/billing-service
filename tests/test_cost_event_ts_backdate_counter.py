"""Tests for `billing_cost_event_ts_backdated_past_rollup_total`.

`observability.metrics.cost_event_ts_backdated_past_rollup_total`, incremented by
`services.consumer._note_backdated_event_ts` from `_stage_message` — the point at
which a cost-stream entry is ACCEPTED into the flush buffer.

THE GAP. `_parsers.parse_event_ts` accepts a producer-asserted event time up to
`MAX_EVENT_TS_BACKDATE_DAYS` (90) before ingest, and `rollup_daily` buckets the
row on THAT date. But `catchup_daily` only re-rolls the trailing
`settings.daily_reconcile_days` complete days plus whatever `missing_daily_dates`
reports as entirely ABSENT — and `daily_rollup_date_gaps` explicitly declines to
repair. So an event landing in a day that was already rolled AND already has a
billing_cost_daily row is never re-aggregated: stored, real, never invoiced.

⛔ WHY NOT AT THE PARSE SITE. The two places that literally decide to accept the
value are `_parsers.parse_event_ts` and its sole caller `cost_flusher.flush_costs`,
neither of which was in scope for this change. `_stage_message` is the nearest
point upstream that sees the SAME raw wire field — it appends the dict verbatim,
so the `timestamp` the flusher later parses is the one read here — and it reuses
`parse_event_ts` itself rather than restating the rule, so the two can never
disagree about whether a value was usable.

Reads are DELTAS: the default prometheus registry is session-global.
"""

from datetime import UTC, datetime, timedelta

import pytest

from services.consumer import _stage_message
from tests.conftest import T

# `_test_settings` is imported rather than re-implemented ON PURPOSE: it carries
# the TEST_DATABASE_URL fallback and the warning about never pointing these tests
# at another session's Postgres container. A second hand-rolled Settings builder
# here would be the one that goes stale.
from tests.test_consumer import _test_settings

pytestmark = pytest.mark.unit


def _backdated(service_key: str) -> float:
    import observability.metrics as om

    return om.cost_event_ts_backdated_past_rollup_total.labels(service_key=service_key)._value.get()


def _cost_fields(*, timestamp: str | None, service_key: str = "inference-router") -> dict:
    fields = {
        "tenant_id": T("backdate-t"),
        "service_key": service_key,
        "provider": "claude",
        "model": "claude-opus-4",
        "input_tokens": "1000",
        "output_tokens": "200",
        "cost_usd": "0.0165",
    }
    if timestamp is not None:
        fields["timestamp"] = timestamp
    return fields


def _stage_cost(fields: dict, settings) -> list[dict]:
    cost_buffer: list[dict] = []
    _stage_message(fields, "1-0", settings.costs_stream, settings, [], cost_buffer)
    return cost_buffer


def test_event_older_than_the_reconcile_window_is_counted():
    """A cost event dated OUTSIDE the trailing reconcile window increments, labelled
    by the emitting service_key — and is still staged, unchanged."""
    settings = _test_settings(daily_reconcile_days=3)
    before = _backdated("w2v-video-compiler")

    # today - 4: the window covers [today-3, today-1], so this is outside it.
    ts = (datetime.now(UTC) - timedelta(days=4)).isoformat()
    staged = _stage_cost(_cost_fields(timestamp=ts, service_key="w2v-video-compiler"), settings)

    assert _backdated("w2v-video-compiler") == before + 1
    # ⛔ BEHAVIOUR UNCHANGED: the entry is still accepted for flushing. This
    # counter observes; it must not start dropping spend.
    assert len(staged) == 1
    assert staged[0]["timestamp"] == ts


def test_event_inside_the_reconcile_window_is_not_counted():
    """THE CONTROL, and the off-by-one that matters.

    `today - daily_reconcile_days` is the OLDEST day catchup_daily still re-rolls
    (window = {boundary - n for n in range(N)} with boundary = today - 1), so it
    must NOT count. A `<=` instead of `<` at the increment site fires here.
    """
    settings = _test_settings(daily_reconcile_days=3)
    before = _backdated("inference-router")

    ts = (datetime.now(UTC) - timedelta(days=3)).isoformat()
    staged = _stage_cost(_cost_fields(timestamp=ts), settings)

    assert _backdated("inference-router") == before
    assert len(staged) == 1


def test_a_fresh_event_is_not_counted():
    """The overwhelming majority of traffic: an event timestamped now."""
    settings = _test_settings(daily_reconcile_days=3)
    before = _backdated("inference-router")

    staged = _stage_cost(_cost_fields(timestamp=datetime.now(UTC).isoformat()), settings)

    assert _backdated("inference-router") == before
    assert len(staged) == 1


def test_the_threshold_follows_the_setting_and_is_not_hardcoded():
    """Same event, two windows, two verdicts.

    A 5-day-old event is outside a 3-day reconcile window and INSIDE a 7-day one.
    If the increment site hardcoded 3 days, the second half of this test fails —
    and a hardcoded constant is how an instrument silently stops matching the
    rollup it is supposed to describe when `daily_reconcile_days` is tuned.
    """
    ts = (datetime.now(UTC) - timedelta(days=5)).isoformat()
    fields = _cost_fields(timestamp=ts, service_key="kag")

    before_tight = _backdated("kag")
    _stage_cost(dict(fields), _test_settings(daily_reconcile_days=3))
    assert _backdated("kag") == before_tight + 1

    before_wide = _backdated("kag")
    _stage_cost(dict(fields), _test_settings(daily_reconcile_days=7))
    assert _backdated("kag") == before_wide, (
        "with a 7-day reconcile window a 5-day-old event IS still re-rolled; "
        "counting it would make this counter report a loss that cannot happen"
    )


def test_an_absent_timestamp_is_the_other_counters_population():
    """No `timestamp` on the wire → NOT this counter.

    That event is bucketed on ingest time and counted by
    `billing_cost_event_ts_fallback_total` instead: billed late, never lost. The
    two populations must stay disjoint or neither can be read.
    """
    settings = _test_settings(daily_reconcile_days=3)
    before = _backdated("inference-router")

    staged = _stage_cost(_cost_fields(timestamp=None), settings)

    assert _backdated("inference-router") == before
    assert len(staged) == 1


def test_a_timestamp_refused_past_retention_is_not_counted():
    """Backdated past `MAX_EVENT_TS_BACKDATE_DAYS` → parse_event_ts REFUSES it.

    The flusher then stores NULL and buckets the row on ingest time, so the spend
    is billed on its arrival day — late, but billed. Counting it here would
    attribute a loss to the one case the parser already protects against, and
    would double-count against the fallback counter.
    """
    settings = _test_settings(daily_reconcile_days=3)
    before = _backdated("inference-router")

    ts = (datetime.now(UTC) - timedelta(days=120)).isoformat()
    staged = _stage_cost(_cost_fields(timestamp=ts), settings)

    assert _backdated("inference-router") == before
    assert len(staged) == 1


def test_the_usage_stream_is_never_counted():
    """Metering events carry no producer event time at all.

    billing_metering_events has no event_ts column, so there is nothing to
    backdate; a counter that fired on the usage stream would be measuring a field
    that does not exist. Staged with a `timestamp` present to make the check
    meaningful rather than accidentally true.
    """
    settings = _test_settings(daily_reconcile_days=3)
    before = _backdated("workers")
    before_unknown = _backdated("unknown")

    event_buffer: list[dict] = []
    _stage_message(
        {
            "tenant_id": T("backdate-t"),
            "path": "/workers/api/tasks",
            "status": "allowed",
            "timestamp": (datetime.now(UTC) - timedelta(days=30)).isoformat(),
        },
        "1-0",
        settings.events_stream,
        settings,
        event_buffer,
        [],
    )

    assert len(event_buffer) == 1
    assert _backdated("workers") == before
    assert _backdated("unknown") == before_unknown, (
        "nor may it land on the birthed fallback label — that would make the "
        "counter unreadable by emitter, which is its only actionable dimension"
    )
