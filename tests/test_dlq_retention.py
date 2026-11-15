"""DLQ operability: age-retention + depth gauge for billing:costs:dead.

The dead-letter stream (``billing:costs:dead``) collects malformed cost events
that failed the minimum billable schema. Left unbounded it grows forever and its
depth is invisible. The consumer loop now (a) age-trims it on the same periodic
cadence as the ingest streams and (b) samples its XLEN into a Prometheus gauge
once per trim interval. These tests drive the REAL consumer loop path.
"""

import asyncio
import math
from datetime import UTC, datetime, timedelta

import pytest

from services.consumer import consume_events
from settings import Settings
from tenancy import NIL_TENANT_ID
from tests.test_consumer import _test_settings


async def _run_one_trim_tick(fake_redis, settings: Settings) -> None:
    """Drive the real consumer loop long enough for one interval-gated trim tick.

    With ``cost_stream_trim_interval_seconds=0.0`` the trim/sample block fires on
    the first iteration; a short stop keeps the loop from spinning past the point
    under test.
    """
    event_buffer: list[dict] = []
    cost_buffer: list[dict] = []
    stop_event = asyncio.Event()

    async def _stop_soon() -> None:
        await asyncio.sleep(0.3)
        stop_event.set()

    await asyncio.gather(
        consume_events(fake_redis, settings, event_buffer, cost_buffer, stop_event),
        _stop_soon(),
    )


@pytest.mark.integration
async def test_dlq_stream_age_trimmed_on_periodic_tick(fake_redis, db_session_factory, monkeypatch):
    """One periodic tick age-trims billing:costs:dead: a far-old entry is removed,
    a fresh one survives -- and the removed entry is in billing_dlq_archive
    first (#243). Goes RED if the DLQ trim is removed OR becomes a plain trim."""
    import services.consumer as consumer_mod

    monkeypatch.setattr(consumer_mod, "get_session_factory", lambda: db_session_factory)
    settings = _test_settings(cost_stream_trim_interval_seconds=0.0)
    dlq = settings.cost_dead_letter_stream

    # id "1-0" == 1ms since epoch: far below the 30d (720h) age cutoff → must go.
    await fake_redis.xadd(dlq, {"payload": "old"}, id="1-0")
    # A fresh entry (server clock) is well within the horizon → must survive.
    fresh_id = await fake_redis.xadd(dlq, {"payload": "fresh"}, id="*")

    await _run_one_trim_tick(fake_redis, settings)

    remaining = await fake_redis.xrange(dlq)
    ids = [entry_id for entry_id, _ in (remaining or [])]
    assert "1-0" not in ids, "far-old DLQ entry was not age-trimmed"
    assert ids == [fresh_id], "fresh DLQ entry must survive the age trim"

    from sqlalchemy import text

    async with db_session_factory() as session:
        archived = (
            await session.execute(
                text("SELECT dead_entry_id, fields FROM billing_dlq_archive WHERE stream = :s"),
                {"s": dlq},
            )
        ).all()
    assert [(r[0], r[1]) for r in archived] == [("1-0", {"payload": "old"})], (
        "the trimmed entry must be archived, and only it"
    )


@pytest.mark.unit
async def test_dlq_depth_gauge_reflects_xlen(fake_redis):
    """The depth gauge equals XLEN of billing:costs:dead after a periodic tick."""
    import observability.metrics as om

    settings = _test_settings(cost_stream_trim_interval_seconds=0.0)
    dlq = settings.cost_dead_letter_stream

    # Seed N fresh (in-horizon) entries so the trim keeps them all; XLEN == N.
    n = 3
    for i in range(n):
        await fake_redis.xadd(dlq, {"payload": f"quarantined-{i}"}, id="*")

    await _run_one_trim_tick(fake_redis, settings)

    assert om.cost_dead_letter_depth._value.get() == n


# ===========================================================================
# billing_cost_dead_letter_oldest_age_seconds
#
# THE GAP THIS CLOSES. The depth gauge above and its alert
# (BillingCostDeadLetterBacklog: depth > 500 for 30m) watch the WRONG axis. The
# trim exercised by test_dlq_stream_age_trimmed_on_periodic_tick destroys
# quarantined cost events at 720h, and Redis is their only copy — they were
# rejected at the schema seam and never written to Postgres. A stream resting
# below the threshold can expire them one at a time, forever, without ever
# reaching 500 (depth was 321 on 2026-09-08 and is 14 on 2026-09-14; both are
# illustrations of the gap, not standing values — re-measure before quoting).
# That is why this gauge exists, and it is unaffected by the correction below.
#
# ⚠️ CORRECTED 2026-09-14 — this block used to read: "Measured on the live
# stream 2026-09-08: entries-added 1375, XLEN 321, max-deleted-entry-id set =>
# ~1,054 cost events already destroyed, 3x the surviving population, with the
# depth alert green throughout." The arithmetic is right and the ATTRIBUTION is
# wrong: the age trim destroyed none of them. What DID is unmeasured — see the
# ALSO CANNOT clause below before naming a mechanism.
#
# ⛔ Why the original reading was possible, and the trap to avoid repeating:
# ``max-deleted-entry-id`` is set by ``XDEL`` exactly as it is by ``XTRIM``, so
# it proves entries were REMOVED and can never say BY WHAT. 1,054 is likewise
# just ``1375 - 321`` — a subtraction, not an observation of a trim.
#
# What refutes the attribution is the hourly depth history, which dates the
# removals against the 720h cutoff:
#     08-27 18:18 -> 1006
#     08-28 05:18 -> 1007
#     08-28 15:18 ->   36
#     08-28 16:18 ->    0
# Those entries were created 08-26/08-27, so they were 1-2 DAYS old when they
# vanished, against a 720h (30d) cutoff. The age trim cannot have removed them.
# The 09-06 321 -> 09-09 14 drop has the same shape (entries from 09-04/09-05).
# Corroborated by the 30d reason totals, which account for the whole population
# without any trim: nil_tenant_id 1053.9 + invalid_tenant_id 308.1 = 1362,
# against entries-added 1375.
#
# So, stated as what the numbers CAN and CANNOT establish:
#   CAN: ~1,054 quarantined cost events were removed from the stream between
#        2026-08-26 and 2026-09-08, and the depth alert stayed green throughout
#        — which is the point this gauge was built to fix, and still stands.
#   CANNOT: that the 720h age trim removed any of them. On the evidence it
#        removed none.
#   ALSO CANNOT: name what DID remove them. Nothing here measures that, and
#        replacing one confident mechanism with another is the defect being
#        fixed, not the fix. The candidates are an admin discard, a replay with
#        delete_on_success, a key deletion, and a Redis flush or data loss —
#        NONE of them measured. Note the cohort resists the easiest guess: it
#        was ~1,053.9 nil_tenant_id, and _discard_refusal refuses a
#        nil_tenant_id entry unless cost_usd == 0 exactly, so a discard could
#        only have taken them if EVERY one was zero-cost, which nobody checked.
#        Depth falling to exactly 0 and holding there ~4 days fits a key
#        deletion or flush at least as well. Attribution is an open question.
# Do not re-derive a trim-loss rate from these figures. As of 2026-09-14 the
# stream holds 14 entries whose oldest is 11.67d old (1,008,250s) — the first
# entries this trim will actually destroy, due ~2026-10-02.
# ===========================================================================

_HOUR = 3600.0


def _oldest_age() -> float:
    import observability.metrics as om

    return om.cost_dead_letter_oldest_age_seconds._value.get()


def _poison_oldest_age(value: float = 999_999.0) -> None:
    """Write a value nothing under test could reach, so a passing assertion
    cannot be the coincidence of an already-correct reading."""
    import observability.metrics as om

    om.cost_dead_letter_oldest_age_seconds.set(value)


async def _seed_dlq(fake_redis, stream: str, *, age_hours: float | None, **extra) -> str:
    """One dead-letter entry in cost_flusher's on-wire shape, stamped
    ``age_hours`` in the PAST. ``age_hours=None`` omits ``_dlq_at`` entirely.

    The stream id is always fresh (``*``) so the 720h age-trim keeps the entry:
    the id and the stamp are deliberately decoupled, which is what makes these
    tests prove the gauge reads ``_dlq_at`` and not the entry id.
    """
    fields = {
        "provider": "claude",
        "model": "sonnet",
        "tenant_id": NIL_TENANT_ID,
        "cost_usd": "0.00",
        "_dlq_reason": "nil_tenant_id",
        "_dlq_original_id": "10-1",
    }
    if age_hours is not None:
        fields["_dlq_at"] = (datetime.now(UTC) - timedelta(hours=age_hours)).isoformat()
    fields.update(extra)
    return await fake_redis.xadd(stream, fields, id="*")


@pytest.mark.unit
async def test_oldest_age_gauge_is_the_oldest_entrys_dlq_at(fake_redis):
    """A populated stream yields the OLDEST entry's age, not the newest's.

    Two entries, 48h and 1h old. Reading the last entry instead of the first —
    or reading XLEN, or the entry id's millisecond component — all give a
    different number, so this pins the one that matters.
    """
    settings = _test_settings(cost_stream_trim_interval_seconds=0.0)
    dlq = settings.cost_dead_letter_stream
    _poison_oldest_age()

    await _seed_dlq(fake_redis, dlq, age_hours=48)  # oldest: first id in the stream
    await _seed_dlq(fake_redis, dlq, age_hours=1)

    await _run_one_trim_tick(fake_redis, settings)

    assert _oldest_age() == pytest.approx(48 * _HOUR, abs=60), (
        "gauge must report the oldest entry's age"
    )


@pytest.mark.unit
async def test_empty_stream_reports_nan_never_zero(fake_redis):
    """An empty DLQ must NOT read 0 — that says "the oldest entry arrived this
    instant", which is the confident-wrong reading an age alert would act on.
    NaN fails every comparison, so no rule can fire on it, while the series
    still EXISTS so no rule is left selecting an empty vector."""
    settings = _test_settings(cost_stream_trim_interval_seconds=0.0)
    _poison_oldest_age()

    await _run_one_trim_tick(fake_redis, settings)

    value = _oldest_age()
    assert math.isnan(value), f"empty stream must report NaN, got {value!r}"


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_stamp",
    [
        pytest.param(None, id="absent"),
        pytest.param("", id="empty"),
        pytest.param("not-a-timestamp", id="unparseable"),
        pytest.param("2026-13-45T99:99:99+00:00", id="out-of-range"),
    ],
)
async def test_unreadable_dlq_at_reports_nan_never_zero(fake_redis, bad_stamp):
    """An oldest entry whose ``_dlq_at`` cannot be trusted emits NaN, not a
    number. Falling back to the stream id's millisecond component would always
    produce a number and so would hide a flusher that stopped stamping."""
    settings = _test_settings(cost_stream_trim_interval_seconds=0.0)
    dlq = settings.cost_dead_letter_stream
    _poison_oldest_age()

    if bad_stamp is None:
        await _seed_dlq(fake_redis, dlq, age_hours=None)
    else:
        await _seed_dlq(fake_redis, dlq, age_hours=None, _dlq_at=bad_stamp)
    # A perfectly good NEWER entry must not rescue the reading: the oldest is
    # the one whose expiry is imminent, so its unreadability is the finding.
    await _seed_dlq(fake_redis, dlq, age_hours=1)

    await _run_one_trim_tick(fake_redis, settings)

    value = _oldest_age()
    assert math.isnan(value), f"unreadable _dlq_at must report NaN, got {value!r}"


@pytest.mark.unit
async def test_naive_dlq_at_is_read_as_utc(fake_redis):
    """The only writer stamps ``datetime.now(UTC).isoformat()``, so a naive
    value can only be UTC. Reading it is correct; rejecting it would blind the
    gauge on a legacy entry for no gain."""
    settings = _test_settings(cost_stream_trim_interval_seconds=0.0)
    dlq = settings.cost_dead_letter_stream
    _poison_oldest_age()

    naive = (datetime.now(UTC) - timedelta(hours=6)).replace(tzinfo=None).isoformat()
    await _seed_dlq(fake_redis, dlq, age_hours=None, _dlq_at=naive)

    await _run_one_trim_tick(fake_redis, settings)

    assert _oldest_age() == pytest.approx(6 * _HOUR, abs=60)


@pytest.mark.unit
async def test_a_stamp_from_the_future_is_clamped_not_negative(fake_redis):
    """Clock skew must not export a negative age — safe for an `> threshold`
    rule either way, but a negative reading is filed as a broken instrument."""
    settings = _test_settings(cost_stream_trim_interval_seconds=0.0)
    dlq = settings.cost_dead_letter_stream
    _poison_oldest_age()

    await _seed_dlq(fake_redis, dlq, age_hours=-2)  # stamped two hours ahead

    await _run_one_trim_tick(fake_redis, settings)

    assert _oldest_age() == 0.0


@pytest.mark.unit
async def test_the_gauge_is_recovered_after_a_process_roll(fake_redis):
    """ROLL-DURABILITY — the property ``billing_cost_dead_letter_total`` lacks.

    That Counter is incremented at quarantine time, so what it reads depends on
    when the exporting process was born rather than on what the stream holds: a
    pod roll resets it to 0 while the quarantined entries sit there untouched.
    Any age instrument built the same way (stamp the first quarantine in a module
    global, subtract later) inherits exactly that.

    ⚠️ CORRECTED 2026-09-14 — the ARGUMENT above is unchanged and is why this test
    exists; only its illustration was stale. It used to read "reads 0 for every
    reason against a stream holding 321 entries". Both figures have moved: depth
    is 14, and ``sum by (reason) (increase(billing_cost_dead_letter_total[30d]))``
    reads nil_tenant_id 1053.9 / invalid_tenant_id 308.1. Neither refutes the
    roll hazard — an ``increase()`` over a range stitches across process births
    and so conceals the very reset this test pins.

    A fresh process starts with this Gauge at its birth value, 0.0, and holds no
    memory of the quarantine. Resetting it to 0.0 and sampling again reproduces
    that state precisely: if the value came from process state it stays 0.0; it
    is recovered only because ``sample_oldest_age`` re-reads Redis every time.
    """
    import observability.metrics as om
    from services.dlq_ops import sample_oldest_age

    settings = _test_settings()
    dlq = settings.cost_dead_letter_stream
    await _seed_dlq(fake_redis, dlq, age_hours=72)

    await sample_oldest_age(fake_redis, dlq)
    before_roll = _oldest_age()
    assert before_roll == pytest.approx(72 * _HOUR, abs=60)

    # The roll: a brand-new process, gauge at its birth value, no quarantine has
    # been observed by THIS process. Redis is untouched — it is the durable copy.
    om.cost_dead_letter_oldest_age_seconds.set(0.0)
    assert _oldest_age() == 0.0

    await sample_oldest_age(fake_redis, dlq)

    assert _oldest_age() == pytest.approx(72 * _HOUR, abs=60), (
        "the age must be re-derived from the stream, not carried in process state"
    )


@pytest.mark.unit
async def test_the_read_is_one_entry_regardless_of_depth(fake_redis, monkeypatch):
    """O(1): exactly one XRANGE, bounded by ``count=1``, whatever the depth.

    A full-stream read would be correct and unnoticed in a test — and would put
    an O(depth) Redis scan on a 100k-maxlen stream every trim interval.
    """
    from services.dlq_ops import sample_oldest_age

    settings = _test_settings()
    dlq = settings.cost_dead_letter_stream
    for _ in range(25):
        await _seed_dlq(fake_redis, dlq, age_hours=5)

    calls: list[tuple[tuple, dict]] = []
    real_xrange = fake_redis.xrange

    async def _spy(*args, **kwargs):
        calls.append((args, kwargs))
        return await real_xrange(*args, **kwargs)

    monkeypatch.setattr(fake_redis, "xrange", _spy)
    await sample_oldest_age(fake_redis, dlq)

    assert len(calls) == 1, "one read per sample"
    assert calls[0][1].get("count") == 1, f"must be bounded to COUNT 1, got {calls[0]}"


@pytest.mark.unit
async def test_a_confirmed_discard_resamples_the_oldest_age(fake_redis):
    """THE SECOND SAMPLE POINT. ``_resample_depth`` is what replay and discard
    both call (proven for each in tests/routers/test_admin_dlq*.py); without the
    age riding along, an operator who drains the head of the queue reads a fresh
    depth beside an age up to a full trim interval stale.

    Discarding the oldest entry must move the gauge to the SURVIVOR's age — a
    resample that merely re-wrote the same number would pass a NaN/emptiness
    test but not this one.
    """
    from services.dlq_ops import discard_dlq

    settings = _test_settings()
    dlq = settings.cost_dead_letter_stream
    oldest_id = await _seed_dlq(fake_redis, dlq, age_hours=100, _dlq_original_id="90-0")
    survivor_id = await _seed_dlq(fake_redis, dlq, age_hours=10, _dlq_original_id="90-1")
    _poison_oldest_age()

    result = await discard_dlq(
        fake_redis, ids=[oldest_id], reason="oldest-age resample check", confirm=True
    )

    assert result.discarded == [oldest_id], f"discard did not delete: {result}"
    remaining = [eid for eid, _ in (await fake_redis.xrange(dlq) or [])]
    assert remaining == [survivor_id]
    assert _oldest_age() == pytest.approx(10 * _HOUR, abs=60), (
        "the post-discard resample must re-derive the age, not just the depth"
    )
