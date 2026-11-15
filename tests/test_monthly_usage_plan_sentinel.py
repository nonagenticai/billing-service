"""``billing_metering_monthly_usage`` must get a row for a licence with no plan.

THE DEFECT THESE TESTS CLOSE
----------------------------
Request-quota enforcement reads a per-licence monthly usage counter that was
almost never written. Measured live: this table held **6 rows / 5 distinct
licences** against **160 rows in ``request_quota_config``** — so **155 of 160
(96.9%) configured licences had NO usage row at all**. With no row,
``services.quota._get_db_usage`` returns 0, so the check falls through
fail-open: a licence capped at 100k/month could serve 400k and still read
``used=0, over_quota=False`` for the rest of the month.

The single writer was gated by ``if license_id and plan_id:``, and **billing
cannot obtain a plan_id**. It arrives only off the wire; ``request_quota_config``
(the only local licence mirror) has no ``plan_id``/``plan_code`` column; there is
no Plan model under ``models/``; and there is no ``resolve_plan`` beside
``resolve_app`` because nothing is mirrored to resolve from. The counter was
conditioned on a field the service has no way of producing.

The guard now requires ``license_id`` alone and stores ``NULL_PLAN_ID`` when no
plan was asserted — the same sentinel ``rollup_engine`` already coalesces to for
the hourly/daily grain, and the same device this table already uses for
``subject_id``.

⛔ WHAT THIS DOES **NOT** DO, so nobody reads these greens as "quota now bites".
``monthly_counts`` is assigned only in the flusher's denied branch and its
``status == "allowed"`` branch. fr writes HTTP CODES into the same ``status``
column (``200``/``201``/``404``/``422``; measured 159,085 rows over 30d), which
land in a third branch that assigns nothing — so a licensed fr request still
produces NO ROW, refused now by the status vocabulary instead of by the plan.
``rollup_engine`` meanwhile bills the daily grain as "everything that is NOT
denied" and explicitly warns against the ``status == "allowed"`` spelling the
monthly writer uses, so the two writers disagree about the same events. Closing
that changes MONEY (``services.billing.generate_billing`` takes
``max(daily_requests, monthly_usage)`` per licence) and belongs in its own
change. ``test_a_licensed_http_status_event_clears_this_guard`` pins only the
half this change owns.

⛔ NO ENFORCEMENT DECISION IS TESTED OR CHANGED HERE. No threshold, no
fail-open direction. These tests are about the COUNTER EXISTING.

Every test carries a NEGATIVE CONTROL that is green both before and after this
change, so a fixture that silently wrote rows unconditionally — or one that
wrote none at all — could not produce this file's pass.
"""

import uuid
from datetime import date

import pytest
from sqlalchemy import func, select

from models.db import NULL_PLAN_ID, NULL_SUBJECT_ID, MeteringMonthlyUsage
from services.aggregator import flush_events
from services.quota import _get_db_usage
from tests.conftest import T
from timeutil import current_month_start

pytestmark = pytest.mark.unit


def _event(*, license_id: str, tenant: str, plan_id: str = "", **over) -> dict:
    """An event dict with license_id / plan_id set EXACTLY as given.

    Deliberately not ``tests._aggregator_helpers._make_event``: that builder does
    ``plan_id or str(uuid.uuid4())``, so an empty plan_id is silently replaced by
    a fresh uuid and the premise of every test here becomes unreachable. A test
    that cannot construct its own premise passes for the wrong reason.
    """
    event = {
        "tenant_id": T(tenant),
        "license_id": license_id,
        "app_id": str(uuid.uuid4()),
        "plan_id": plan_id,
        "method": "GET",
        "path": "/workers/api/tasks",
        "status": "allowed",
        "source_ip": "10.0.0.1",
        "_service_key": "workers",  # pragma: allowlist secret
    }
    event.update(over)
    return event


async def _flush(session_factory, *events: dict) -> None:
    """Flush a batch and assert every event was persisted as a raw row."""
    assert await flush_events(list(events), session_factory, None) == len(events)


async def _rows(session_factory) -> list[MeteringMonthlyUsage]:
    async with session_factory() as session:
        return list((await session.execute(select(MeteringMonthlyUsage))).scalars().all())


async def _count(session_factory) -> int:
    async with session_factory() as session:
        return (
            await session.execute(select(func.count()).select_from(MeteringMonthlyUsage))
        ).scalar()


def _skips() -> dict[str, float]:
    """Every skip-reason child, read as a DELTA source.

    The default prometheus registry is session-global and other modules
    increment these series, so every read here is compared before/after — the
    idiom tests/test_metering_usage_skipped_counter.py already uses.
    """
    import observability.metrics as om

    return {
        reason: om.metering_usage_skipped_total.labels(reason=reason)._value.get()
        for reason in om.METERING_USAGE_SKIP_REASONS
    }


# ---------------------------------------------------------------------------
# 1. The row must exist for a licence that has usage but no plan.
# ---------------------------------------------------------------------------


async def test_licensed_event_with_no_plan_gets_a_row_under_the_sentinel(db_session_factory):
    """THE DEFECT ITSELF. A licence with usage and no plan must be counted.

    Before this change the event was INSERTED into billing_metering_events and
    then contributed nothing to the usage counter, which is how 96.9% of
    configured licences ended up with no row to enforce against.
    """
    lic = str(uuid.uuid4())

    await _flush(db_session_factory, _event(license_id=lic, tenant="sentinel-a"))

    rows = await _rows(db_session_factory)
    assert len(rows) == 1, "a licensed event with no plan must produce a usage row"
    assert str(rows[0].license_id) == lic
    assert rows[0].request_count == 1
    assert rows[0].plan_id == NULL_PLAN_ID, (
        "the plan must be the explicit 'no plan asserted' sentinel — never a "
        "real plan id, which would misrepresent which plan was in force"
    )
    assert rows[0].subject_id == NULL_SUBJECT_ID


async def test_an_event_with_no_licence_still_writes_no_row(db_session_factory):
    """NEGATIVE CONTROL — green before AND after this change.

    The guard was RELAXED, not removed. A row with genuinely no licence has
    nothing to key a per-licence counter on and must still write nothing; if
    this ever goes green-by-writing, the fix has become "count everything
    against something", which is worse than the defect.
    """
    await _flush(db_session_factory, _event(license_id="", tenant="sentinel-b"))

    assert await _count(db_session_factory) == 0


async def test_the_db_fallback_now_finds_usage_it_previously_could_not(db_session_factory):
    """The READER's view — the reason the row mattered in the first place.

    ``_get_db_usage`` is the fail-open path: on a cold or lost Redis key
    ``check_quota`` falls back to it. With no row it returned 0 for 96.9% of
    licences and the check could never bite. This asserts the reader, not the
    writer, because a row the reader's own filters miss would be no fix.
    """
    lic = uuid.uuid4()
    batch = [_event(license_id=str(lic), tenant="sentinel-c") for _ in range(3)]

    await _flush(db_session_factory, *batch)

    async with db_session_factory() as session:
        used = await _get_db_usage(lic, current_month_start(), session)
    assert used == 3, "the DB fallback must see the sentinel row's usage"


async def test_the_db_fallback_reads_zero_for_an_unrelated_licence(db_session_factory):
    """NEGATIVE CONTROL for the reader — green before AND after.

    Without it, a ``_get_db_usage`` that summed the whole table regardless of
    its predicates would satisfy the test above.
    """
    lic = uuid.uuid4()
    await _flush(db_session_factory, _event(license_id=str(lic), tenant="sentinel-d"))

    async with db_session_factory() as session:
        assert await _get_db_usage(uuid.uuid4(), current_month_start(), session) == 0
        assert await _get_db_usage(lic, date(2020, 1, 1), session) == 0


# ---------------------------------------------------------------------------
# 2. The unique constraint must still de-duplicate.
# ---------------------------------------------------------------------------


async def test_sentinel_rows_still_upsert_onto_one_row_across_flushes(db_session_factory):
    """``uq_monthly_usage`` must keep collapsing repeat usage onto ONE row.

    ⛔ THIS IS WHY plan_id IS A SENTINEL AND NOT NULLABLE. Postgres treats NULLs
    as DISTINCT in a plain UNIQUE constraint, so a nullable plan_id would make
    the flusher's ``ON CONFLICT ON CONSTRAINT uq_monthly_usage`` MISS every
    plan-less row: each flush would INSERT a new row instead of incrementing,
    the counter would fan out into one row per flush, and ``_get_db_usage`` —
    which SUMs — would add up the duplicates.

    Separate flushes, not one batch: a single batch is aggregated in Python
    before it reaches the statement, so it would never exercise the constraint.
    """
    lic = str(uuid.uuid4())

    for _ in range(3):
        await _flush(db_session_factory, _event(license_id=lic, tenant="sentinel-e"))

    rows = await _rows(db_session_factory)
    assert len(rows) == 1, (
        "three flushes for one (licence, subject, sentinel plan, month) is ONE row"
    )
    assert rows[0].request_count == 3, "the upsert must INCREMENT, not overwrite"


async def test_a_different_subject_still_gets_its_own_row(db_session_factory):
    """NEGATIVE CONTROL for the constraint — green before AND after.

    The key still discriminates. Without this, a de-duplication test would also
    pass against a broken constraint that collapsed everything onto one row —
    which would silently merge per-user and S2S usage.
    """
    lic = str(uuid.uuid4())
    sub = str(uuid.uuid4())

    await _flush(db_session_factory, _event(license_id=lic, tenant="sentinel-f"))
    await _flush(
        db_session_factory,
        _event(license_id=lic, tenant="sentinel-f", subject_id=sub),
    )

    assert await _count(db_session_factory) == 2


async def test_a_real_plan_is_not_flattened_onto_the_sentinel(db_session_factory):
    """⚠️ ACCOUNTING IS PRESERVED: plan stays IN the unique key.

    Dropping plan_id from the key would have CHANGED ACCOUNTING — a licence that
    legitimately changes plan mid-month would stop splitting into two rows. It
    was not dropped. A row that asserts a real plan keeps its own grain; only
    rows asserting NO plan collapse onto one another, which is correct because
    they are the same unknown.

    The reader sums across the grain, so the licence total is unaffected either
    way — which is exactly why the split can be kept for free.
    """
    lic = str(uuid.uuid4())
    plan = str(uuid.uuid4())

    await _flush(db_session_factory, _event(license_id=lic, tenant="sentinel-g"))
    await _flush(
        db_session_factory,
        _event(license_id=lic, plan_id=plan, tenant="sentinel-g"),
    )

    rows = await _rows(db_session_factory)
    assert len(rows) == 2, "a real plan and 'no plan asserted' are different rows"
    assert {str(r.plan_id) for r in rows} == {str(NULL_PLAN_ID), plan}

    async with db_session_factory() as session:
        total = await _get_db_usage(uuid.UUID(lic), current_month_start(), session)
    assert total == 2, "the licence total must sum ACROSS the plan grain"


# ---------------------------------------------------------------------------
# 3. The guard must not regress to requiring a plan.
# ---------------------------------------------------------------------------


async def test_a_licensed_http_status_event_clears_this_guard(db_session_factory):
    """⭐ THE HALF THIS CHANGE OWNS, for the population that actually matters.

    fr meters with HTTP codes, not the decision vocabulary. Such an event used
    to be refused by the OUTER guard because it carried no plan; it must now
    CLEAR that guard — no skip reason may move.

    ⛔ IT STILL WRITES NO ROW, and that is the remaining blocker, NOT something
    this test blesses: the flusher's ``status`` dispatch assigns
    ``monthly_counts`` only for ``allowed`` and ``denied*``, so an HTTP code
    falls into a branch that assigns nothing. That gap is deliberately NOT
    asserted here — pinning it would encode the defect as the spec and outvote
    the lane that fixes it. It is counted instead, by
    ``billing_metering_status_unrecognised_total``, which was structurally
    unreachable for live traffic until this change (it needed BOTH ids) and so
    reported zero because of the GUARD, not the population.
    """
    before = _skips()

    await _flush(
        db_session_factory,
        _event(license_id=str(uuid.uuid4()), tenant="sentinel-h", status="200"),
    )

    assert _skips() == before, (
        "a licensed event must clear the outer guard whether or not it carries a "
        "plan; a moved skip counter means the `and plan_id` conjunct is back"
    )


async def test_reason_plan_id_is_structurally_unreachable(db_session_factory):
    """⭐ THE REVERT DETECTOR. ``reason='plan_id'`` must stay a flat zero.

    The label is deliberately RETAINED in ``METERING_USAGE_SKIP_REASONS`` rather
    than deleted: it is pre-seeded, so it exports 0, and an absent series and a
    zero series are indistinguishable to ``increase()``. This series reading
    zero — where it once carried the majority of events — IS the evidence the
    repair holds.

    Restoring ``if license_id and plan_id:`` makes this test fail, which is the
    entire point of writing it.
    """
    import observability.metrics as om

    assert "plan_id" in om.METERING_USAGE_SKIP_REASONS, (
        "the child must stay birthed; deleting it makes the flat zero unreadable"
    )
    before = _skips()["plan_id"]

    await _flush(
        db_session_factory,
        _event(license_id=str(uuid.uuid4()), tenant="sentinel-i"),
        _event(license_id=str(uuid.uuid4()), tenant="sentinel-i", status="200"),
    )

    assert _skips()["plan_id"] == before


async def test_an_unlicensed_event_still_counts_a_skip(db_session_factory):
    """NEGATIVE CONTROL for the detector above — green before AND after.

    Without it, a skip counter that had been accidentally disconnected would
    make ``reason='plan_id'`` stay flat for the wrong reason and the two tests
    above would pass over a dead instrument.
    """
    before = _skips()["both"]

    await _flush(db_session_factory, _event(license_id="", tenant="sentinel-j"))

    assert _skips()["both"] == before + 1
