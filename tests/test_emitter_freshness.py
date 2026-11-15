"""Tests for `billing_cost_events_last_seen_timestamp_seconds`.

`services.emitter_freshness.publish_emitter_freshness` is the ONLY producer of
that gauge, and the gitops rule `BillingEmitterLedgerSilent` is the only consumer.

🔴 WHY THESE TESTS EXIST AT ALL. The alert is written as
`time() - max by (service_key, provider) (billing_cost_events_last_seen_timestamp_seconds)`.
If this publisher is broken, that expression evaluates over an EMPTY VECTOR and
the rule reads `inactive` — which is byte-for-byte what a healthy estate looks
like. So a silently-dead publisher does not make the alert wrong, it makes it
**permanently vacuous**, and the whole point of the change (making a dead metering
emitter fireable) is lost while every dashboard stays green. That is strictly
worse than shipping no alert, because an alert that exists is assumed to work.
This module was at 0% coverage — imported by nothing — when these were written.

⚠️ THE METRICS ARE MODULE-LEVEL AND PROCESS-LIFETIME. A labelled Gauge never
forgets a child and the drop Counter only ever climbs, so nothing here may assert
an ABSOLUTE series count or an absolute counter value: another test in the same
session may have published children, and pytest ordering is not a contract. Every
assertion below is scoped by a per-test `service_key` prefix, and the drop counter
is read as a before/after DELTA.
"""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import CostEvent
from services.emitter_freshness import MAX_FRESHNESS_SERIES, publish_emitter_freshness
from tests.conftest import T

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------- helpers


def _event(*, service_key: str, provider: str, created_at: datetime, event_ts=None) -> CostEvent:
    """A minimal billable cost row.

    `created_at` is always anchored a few minutes/seconds behind `now` by the
    callers: `billing_cost_events` is RANGE-partitioned on `created_at` and
    partitions are minted per month by `create_cost_events_partition_for`, so a
    hardcoded date could land outside every existing partition and fail the
    INSERT for a reason that has nothing to do with this gauge. `reservation_id`
    is left unset on purpose — it is nullable and NULL is its correct permanent
    value for a debit that was never pre-flighted.
    """
    return CostEvent(
        tenant_id=T("emitter-freshness"),
        license_id=uuid.uuid4(),
        service_key=service_key,
        provider=provider,
        input_tokens=100,
        output_tokens=10,
        cost_usd=Decimal("0.01"),
        created_at=created_at,
        event_ts=event_ts,
    )


def _published(prefix: str) -> dict[tuple[str, str], float]:
    """Every published `(service_key, provider) -> value`, scoped to one prefix.

    Read through `collect()` rather than `.labels(...)._value.get()` because
    `.labels()` CREATES the child on access: probing for absence that way would
    mint the very series the test is asserting is not there, and the assertion
    could never fail. `collect()` is read-only.
    """
    import observability.metrics as om

    out: dict[tuple[str, str], float] = {}
    for metric in om.cost_events_last_seen_timestamp_seconds.collect():
        for sample in metric.samples:
            service_key = sample.labels.get("service_key", "")
            if service_key.startswith(prefix):
                out[(service_key, sample.labels["provider"])] = sample.value
    return out


def _dropped_total() -> float:
    import observability.metrics as om

    return om.cost_events_last_seen_series_dropped_total._value.get()


class _StubResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _StubSession:
    """Minimal stand-in for an AsyncSession: returns canned rows, or raises."""

    def __init__(self, *, rows=None, raises: Exception | None = None):
        self._rows = rows or []
        self._raises = raises
        self.calls = 0

    async def execute(self, *_args, **_kwargs):
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return _StubResult(self._rows)


# ---------------------------------------------------------------- tests


async def test_each_emitting_pair_gets_its_newest_created_at_as_a_unix_timestamp(db_session):
    """The gauge must carry the pair's LAST-SEEN INSTANT, not merely exist.

    WHAT BREAKS IF THIS FAILS: the alert subtracts this value from `time()`, so a
    gauge set to the wrong instant yields a wrong AGE — and the failure is
    one-directional and silent. A publisher that set an age (say 300) instead of a
    timestamp, or that published the *oldest* row, would still produce a full,
    healthy-looking series set; `time() - 300` is ~1970, so every emitter in the
    estate would page as 56 years dormant, and `time() - oldest` would make a dead
    emitter look alive. Asserting only "a series exists for this pair" passes in
    every one of those cases, which is why the VALUE is asserted here.

    Two pairs with DIFFERENT `created_at`, and two rows inside one pair, so a
    publisher that grouped wrongly (one global max shared by both pairs) or
    aggregated with `min()` cannot pass.
    """
    prefix = "ef-happy"
    alpha = f"{prefix}-alpha"
    beta = f"{prefix}-beta"
    now = datetime.now(UTC)
    alpha_newest = now - timedelta(minutes=2)
    beta_newest = now - timedelta(minutes=30)

    db_session.add_all(
        [
            # Deliberately inserted oldest-first so row order cannot stand in for
            # a real max(): the newest row of each pair is not the last inserted.
            _event(service_key=alpha, provider="claude", created_at=alpha_newest),
            _event(service_key=alpha, provider="claude", created_at=now - timedelta(minutes=45)),
            _event(service_key=beta, provider="mflux", created_at=beta_newest),
        ]
    )
    await db_session.commit()

    published = await publish_emitter_freshness(db_session)

    series = _published(prefix)
    assert published >= 2, "the publisher reported fewer series than the pairs it was given"
    assert set(series) == {
        (alpha, "claude"),
        (beta, "mflux"),
    }, "one series per (service_key, provider) pair, no more and no fewer"
    assert series[(alpha, "claude")] == pytest.approx(alpha_newest.timestamp(), abs=0.001), (
        "alpha must carry its NEWEST created_at, not its oldest and not an age"
    )
    assert series[(beta, "mflux")] == pytest.approx(beta_newest.timestamp(), abs=0.001)
    # A timestamp, not an age: a 2026 unix time is ~1.78e9, an age in seconds is
    # three orders of magnitude smaller. This is the cheap cross-check that the
    # module's central design decision (docstring: "IT PUBLISHES A TIMESTAMP, NOT
    # AN AGE") actually survived into the code.
    assert series[(alpha, "claude")] > 1.7e9, (
        "value is not a unix timestamp — an age here reads as 'last seen 1970' "
        "and pages for every emitter in the estate"
    )


async def test_last_seen_is_created_at_because_event_ts_is_null_for_95_percent_of_the_ledger(
    db_session,
):
    """⛔ KEYING THIS ON `event_ts` WOULD PAGE FOR THE ENTIRE FLEET, AT ONCE.

    `event_ts` arrived with alembic 0028 and was only populated from
    2026-09-11 13:57Z; it is NULL on 237,826 of 248,809 prod rows (95.6%) and is
    NEVER backfilled — that history is gone. So `max(event_ts)` is NULL for any
    emitter that has not written since the migration, and a publisher keyed on it
    would either publish nothing for them (alert vacuous) or report the MIGRATION
    DATE as the liveness of the whole fleet.

    `coalesce(event_ts, created_at)` is wrong for a worse reason and is checked
    here too: `event_ts` is PRODUCER-ASSERTED and may legitimately be back-dated
    up to 90 days, or forward-dated by a skewed clock. The row below has a
    90-day-old `event_ts` with a 3-minute-old `created_at`; under a COALESCE the
    gauge would read 90 days dormant for an emitter that wrote three minutes ago.

    WHAT BREAKS IF THIS FAILS: `BillingEmitterLedgerSilent` pages on live,
    healthy emitters — and an alert that cries wolf on the whole fleet gets
    silenced, which deletes the detector this change exists to add.
    """
    prefix = "ef-createdat"
    nullts = f"{prefix}-nullts"
    backdated = f"{prefix}-backdated"
    now = datetime.now(UTC)
    recent_ingest = now - timedelta(minutes=3)

    db_session.add_all(
        [
            # NULL event_ts — the shape of 95.6% of the ledger.
            _event(
                service_key=nullts,
                provider="kokoro",
                created_at=recent_ingest,
                event_ts=None,
            ),
            # Present but heavily back-dated event_ts — the COALESCE trap.
            _event(
                service_key=backdated,
                provider="mflux",
                created_at=recent_ingest,
                event_ts=now - timedelta(days=90),
            ),
        ]
    )
    await db_session.commit()

    await publish_emitter_freshness(db_session)

    series = _published(prefix)
    assert series[(nullts, "kokoro")] == pytest.approx(recent_ingest.timestamp(), abs=0.001), (
        "a NULL event_ts must not suppress or shift the series; created_at is the clock"
    )
    assert series[(backdated, "mflux")] == pytest.approx(recent_ingest.timestamp(), abs=0.001), (
        "a 90-day-old producer event_ts leaked into the gauge — this is the "
        "coalesce(event_ts, created_at) defect, and it reports a live emitter as "
        "three months dead"
    )


async def test_an_empty_ledger_publishes_nothing_rather_than_an_epoch_zero(db_session):
    """An empty ledger must publish NO SERIES — never a 0.

    WHAT BREAKS IF THIS FAILS: 0 is a perfectly valid unix timestamp, so
    `time() - 0` is ~1.78e9 seconds and the rule fires instantly, naming every
    emitter as silent since 1970 for no reason other than a cold start or a
    truncated table. An abstention and a measurement are different claims; this
    function abstains by publishing nothing, and the return value (0) means
    "published nothing", distinguished from "no emitters exist" only by the log.

    `db_engine` TRUNCATEs every table before each test, so this IS the
    post-truncate / cold-start state the gauge has to describe.
    """
    before = _published("")

    published = await publish_emitter_freshness(db_session)

    assert published == 0
    assert _published("") == before, (
        "an empty ledger minted or moved a series; a 0 here reads as "
        "'last seen 1970' and pages immediately"
    )


async def test_a_null_max_is_skipped_rather_than_published_as_zero():
    """The `last_seen is None` guard must skip, never publish 0.

    ⚠️ UNREACHABLE THROUGH POSTGRES, AND TESTED ANYWAY. `max(created_at)` grouped
    over a NOT NULL column cannot return NULL, which the module itself says — so
    this guard can only be exercised with a stub session, and a stub is used here
    for exactly that reason and nothing else. It is worth pinning because the
    guard is the last line of defence on the same failure the empty-ledger test
    covers: a 0 published here would be read as "last seen at the epoch" and fire
    as the oldest emitter in the estate. If the column ever becomes nullable, or
    the SQL grows an outer join, this is the test that keeps the behaviour.
    """
    prefix = "ef-nullmax"
    dropped_before = _dropped_total()
    session = _StubSession(rows=[(f"{prefix}-ghost", "nowhere", None)])

    published = await publish_emitter_freshness(cast(AsyncSession, session))

    assert session.calls == 1, "the publisher did not run its query"
    assert published == 0, "a NULL max must not be counted as a published series"
    assert _published(prefix) == {}, (
        "a NULL last_seen was published; as a Gauge value that is 0, i.e. "
        "'last seen 1970', and it would fire as the estate's oldest emitter"
    )
    assert _dropped_total() == dropped_before, "a skipped NULL row is not a cardinality drop"


async def test_the_cardinality_cap_holds_and_the_overflow_is_counted(db_session):
    """Over the cap: publish exactly the cap, and COUNT the rest.

    WHAT BREAKS IF THIS FAILS: both labels are producer-fed `varchar(64)`, so the
    label domain is OPEN. One emitter minting a per-request `service_key` would
    mint one permanent Prometheus series per request — a cardinality incident in
    the SCRAPE TARGET, where nothing in this process looks unhealthy. The cap
    bounds that. The drop counter is the other half and is not optional: rows
    arrive NEWEST-FIRST, so a flood of freshly-minted labels sorts to the top and
    EVICTS the dormant emitters this gauge exists to find — without the counter
    the surviving series set looks complete while being exactly inverted.

    The cap is read from the module, never hardcoded: a test carrying its own
    copy of the number passes after someone edits the constant, which is the one
    change that needs to redden something.
    """
    prefix = "ef-cap"
    overflow = 5
    pairs = MAX_FRESHNESS_SERIES + overflow
    keys = [f"{prefix}-{i:03d}" for i in range(pairs)]
    now = datetime.now(UTC)

    # Strictly descending created_at, one second apart, so the newest-first
    # ordering is total and deterministic: index 0..cap-1 must survive and the
    # tail must be evicted. Ties would make the eviction set unpredictable and
    # the eviction assertion below flaky rather than wrong.
    db_session.add_all(
        [
            _event(
                service_key=key,
                provider="claude",
                created_at=now - timedelta(seconds=i + 1),
            )
            for i, key in enumerate(keys)
        ]
    )
    await db_session.commit()

    dropped_before = _dropped_total()
    published = await publish_emitter_freshness(db_session)
    dropped_delta = _dropped_total() - dropped_before

    series = _published(prefix)
    assert published == MAX_FRESHNESS_SERIES, (
        f"publisher reported {published} series against a cap of {MAX_FRESHNESS_SERIES}"
    )
    assert len(series) == MAX_FRESHNESS_SERIES, (
        f"{len(series)} series reached the registry with {pairs} pairs in the "
        f"ledger and a cap of {MAX_FRESHNESS_SERIES}"
    )
    assert dropped_delta == overflow, (
        f"drop counter moved by {dropped_delta}, expected exactly {overflow}; a "
        "0 here is the dangerous direction — it reports a bounded, complete "
        f"series set while {overflow} emitters are missing from it"
    )
    # The cap keeps the NEWEST pairs. Pinned explicitly because it is also the
    # cap's weakness, and the drop counter is only interpretable if this holds.
    assert (keys[0], "claude") in series, "the newest pair was evicted"
    assert (keys[-1], "claude") not in series, (
        "the oldest pair survived a full cap — the cap is not keeping newest-first"
    )


async def test_a_query_failure_returns_zero_and_never_aborts_the_rollup_tick():
    """A measurement may not break the billing work it rides on.

    WHAT BREAKS IF THIS FAILS: this runs inside the rollup loop's leader tick —
    the same tick that does the real rollups. An exception escaping here would
    abort that tick, so a transient DB hiccup on a REPORTING-ONLY read would stop
    actual billing aggregation. The escalation direction is wrong in every sense:
    the instrument would take down the thing it observes.

    ⚠️ A raising stub session is the only way to reach this branch — a real
    Postgres error cannot be provoked from the fixture without corrupting the
    schema for every later test. It is kept to one method (`execute`).

    The return of 0 means "published nothing", NOT "no emitters exist". Callers
    must not treat it as a measurement — and on this path every existing series
    KEEPS its previous value and therefore keeps ageing, which is why the rule is
    paired with a separate freshness selector rather than trusting these values.
    """
    session = _StubSession(raises=RuntimeError("connection reset by peer"))
    dropped_before = _dropped_total()

    published = await publish_emitter_freshness(cast(AsyncSession, session))

    assert published == 0, "a failed read must report zero series published"
    assert session.calls == 1
    assert _dropped_total() == dropped_before, (
        "a read failure counted as a cardinality drop — that would make the "
        "drop counter fire an upstream-emitter investigation for a DB blip"
    )
