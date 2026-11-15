"""Tests for `billing_metering_usage_skipped_total`.

`observability.metrics.metering_usage_skipped_total`, incremented in the `else`
of the OUTER guard `if license_id and plan_id:` in `services/event_flusher.py`.

WHAT THE COUNTER IS FOR. An event that takes that false branch is still INSERTED
into billing_metering_events and then contributes NOTHING to
metering_monthly_usage and nothing to the per-licence quota delta. Measured
2026-09-04 over 30d: 159,085 rows carry an HTTP-code status and not one of them
carries both ids, so this branch is the normal population today — quota
under-counts for all of them and license-authz fails OPEN on the resulting key.
The INNER `else` (unknown `status`, both ids present) already had
`billing_metering_status_unrecognised_total`; this outer one had nothing.

⛔ EVERY TEST HERE ASSERTS THE SKIP AS WELL AS THE COUNT. A counter test that
only reads the counter cannot tell "the guard skipped this event and we counted
it" from "the guard accepted the event and we counted it anyway" — so each case
also pins the absence (or presence) of the metering_monthly_usage row the guard
decides. That is what makes these tests about the DEFECT and not about the
instrument.

The default prometheus registry is session-global and other modules increment
these series, so every read is a DELTA (before/after), the idiom
tests/test_cost_tokens_coverage_counter.py and tests/test_cost_flusher.py
already use.
"""

import uuid

import pytest
from sqlalchemy import func, select

from models.db import MeteringEvent, MeteringMonthlyUsage
from services.aggregator import flush_events
from tests.conftest import T

pytestmark = pytest.mark.unit


def _skipped(reason: str) -> float:
    import observability.metrics as om

    return om.metering_usage_skipped_total.labels(reason=reason)._value.get()


def _all_skipped() -> dict[str, float]:
    import observability.metrics as om

    return {r: _skipped(r) for r in om.METERING_USAGE_SKIP_REASONS}


def _raw_event(*, license_id: str, plan_id: str, tenant: str) -> dict:
    """An event dict with license_id / plan_id set EXACTLY as given.

    Deliberately not `tests._aggregator_helpers._make_event`: that builder does
    `plan_id or str(uuid.uuid4())`, so an empty plan_id is silently replaced by a
    fresh uuid and the case this file exists to drive becomes unreachable. A test
    that cannot construct its own premise passes for the wrong reason.
    """
    return {
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


async def _monthly_rows(db_session_factory) -> int:
    async with db_session_factory() as session:
        return (
            await session.execute(select(func.count()).select_from(MeteringMonthlyUsage))
        ).scalar()


async def _metering_rows(db_session_factory) -> int:
    async with db_session_factory() as session:
        return (await session.execute(select(func.count()).select_from(MeteringEvent))).scalar()


async def test_healthy_event_counts_usage_and_skips_nothing(db_session_factory):
    """THE CONTROL. Both ids present → the guard's TRUE branch.

    The monthly row must be written and not one of the three reason series may
    move. Without this case a counter wired to fire unconditionally would pass
    every other test in this file.
    """
    before = _all_skipped()
    event = _raw_event(license_id=str(uuid.uuid4()), plan_id=str(uuid.uuid4()), tenant="skip-ok")

    assert await flush_events([event], db_session_factory, None) == 1

    # The guard took its TRUE branch: usage was counted.
    assert await _monthly_rows(db_session_factory) == 1
    assert _all_skipped() == before, (
        "a fully-attributed event must not increment any skip reason; the counter "
        "lives in the guard's FALSE branch only"
    )


async def test_missing_license_id_counts_reason_license_id(db_session_factory):
    """license_id absent, plan_id present → reason='license_id'."""
    before = _all_skipped()
    event = _raw_event(license_id="", plan_id=str(uuid.uuid4()), tenant="skip-lic")

    assert await flush_events([event], db_session_factory, None) == 1

    # THE DEFECT ITSELF: the raw event is stored, the usage counter is not written.
    assert await _metering_rows(db_session_factory) == 1
    assert await _monthly_rows(db_session_factory) == 0

    after = _all_skipped()
    assert after["license_id"] == before["license_id"] + 1
    assert after["plan_id"] == before["plan_id"]
    assert after["both"] == before["both"], (
        "only ONE id was missing; counting it as 'both' would name the wrong fix"
    )


async def test_missing_plan_id_is_no_longer_a_skip_at_all(db_session_factory):
    """plan_id absent, license_id present → NOT a skip. The row is WRITTEN.

    ⛔ THIS TEST USED TO ASSERT THE OPPOSITE (``reason='plan_id'``, zero monthly
    rows) AND THAT EXPECTATION WAS THE DEFECT WRITTEN DOWN AS A SPEC. The guard
    required ``license_id and plan_id``, and billing can never obtain a plan_id:
    it arrives only off the wire, ``request_quota_config`` mirrors no plan_id or
    plan_code, there is no Plan model, and there is no ``resolve_plan``. So the
    counter was conditioned on a field this service cannot produce, and the
    table sat at 6 rows / 5 licences against 160 configured licences — 96.9% of
    them with no usage row, every quota check falling through fail-open.

    The guard is now ``if license_id:`` and the plan is stored as the
    ``NULL_PLAN_ID`` sentinel, so this event counts usage instead of vanishing.

    ⭐ THIS IS THE REVERT DETECTOR. Restoring the ``and plan_id`` conjunct fails
    it twice over: no monthly row, and a moved skip counter.
    """
    before = _all_skipped()
    event = _raw_event(license_id=str(uuid.uuid4()), plan_id="", tenant="skip-plan")

    assert await flush_events([event], db_session_factory, None) == 1

    assert await _metering_rows(db_session_factory) == 1
    assert await _monthly_rows(db_session_factory) == 1, (
        "a licensed event with no plan must now be COUNTED, not skipped; this is "
        "the whole defect — with no row the Redis seed has nothing to seed from "
        "and the DB fallback finds nothing, so quota can never bite"
    )

    assert _all_skipped() == before, (
        "the guard no longer tests plan_id, so NO skip reason may move — "
        "reason='plan_id' is structurally unreachable and must stay a flat zero"
    )


async def test_both_ids_missing_counts_reason_both(db_session_factory):
    """Neither id present → reason='both', exactly once, not once per id."""
    before = _all_skipped()
    event = _raw_event(license_id="", plan_id="", tenant="skip-both")

    assert await flush_events([event], db_session_factory, None) == 1

    assert await _metering_rows(db_session_factory) == 1
    assert await _monthly_rows(db_session_factory) == 0

    after = _all_skipped()
    assert after["both"] == before["both"] + 1
    assert after["license_id"] == before["license_id"]
    assert after["plan_id"] == before["plan_id"]


async def test_unrecognised_status_with_both_ids_is_the_other_counter(db_session_factory):
    """⛔ THE BOUNDARY BETWEEN THE TWO COUNTERS, and the easiest thing to get wrong.

    An HTTP-code `status` with BOTH ids present takes the guard's TRUE branch and
    lands in the INNER else — `billing_metering_status_unrecognised_total`. It
    must NOT also increment the outer skip counter, or the two series overlap and
    neither can be read as a population.

    This is the case the 159,085-row measurement is about: today those rows carry
    neither id, so they take the OUTER branch. The day fr meters a licensed
    request with an HTTP status they move to this one instead, and the two
    counters are how that migration becomes visible.
    """
    import observability.metrics as om

    before_skip = _all_skipped()
    before_status = om.metering_status_unrecognised_total.labels(status="200")._value.get()

    event = _raw_event(
        license_id=str(uuid.uuid4()), plan_id=str(uuid.uuid4()), tenant="skip-status"
    )
    event["status"] = "200"

    assert await flush_events([event], db_session_factory, None) == 1

    after_status = om.metering_status_unrecognised_total.labels(status="200")._value.get()
    assert after_status == before_status + 1
    assert _all_skipped() == before_skip, (
        "both ids were present, so this is the INNER unrecognised-status branch, "
        "not the outer skip; one event must never increment both families"
    )
    # ⚠️ This used to assert 0: "NEITHER branch writes usage — which is exactly
    # why both need counting." That was the defect, not the contract.
    # services.metering_status made everything not starting with "denied"
    # billable, matching what rollup_engine already billed daily, so a licensed
    # HTTP-status request now produces its monthly row. The counter above still
    # fires, because an HTTP code is still an UNUSUAL status worth seeing — it
    # is just no longer a reason to drop the usage.
    assert await _monthly_rows(db_session_factory) == 1
