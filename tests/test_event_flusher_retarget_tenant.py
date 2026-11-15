"""``event_flusher`` must persist the optional wire ``retarget_tenant_id``.

license-authz#65 adds ``retarget_tenant_id`` (a uuid string, present only when
the request was retargeted, never ``""``) to ``billing:events``. Alembic 0042
added ``billing_metering_events.retarget_tenant_id``; these tests cover the
writer (billing-service#235), including the two controls that stop the positive
case passing for the wrong reason: absence stays NULL, and a malformed value is
refused to NULL and counted WITHOUT failing the flush or dropping the event.
"""

import uuid

import pytest
from sqlalchemy import select

import observability.metrics as observability_metrics
from models.db import MeteringEvent
from services.aggregator import flush_events
from services.consumer import _stage_message
from settings import get_settings
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


async def _rows(session_factory) -> list[MeteringEvent]:
    async with session_factory() as session:
        return list((await session.execute(select(MeteringEvent))).scalars().all())


def _invalid_count() -> float:
    return observability_metrics.metering_retarget_tenant_invalid_total._value.get()


def test_consumer_stages_retarget_tenant_id_through_unchanged():
    """The consumer must not strip the field before the flusher sees it."""
    settings = get_settings()
    target = str(uuid.uuid4())
    events: list[dict] = []
    costs: list[dict] = []
    fields = {"tenant_id": T("rt-stage"), "path": "/workers/x", "retarget_tenant_id": target}

    _stage_message(fields, "1-0", settings.events_stream, settings, events, costs)

    assert len(events) == 1 and not costs
    assert events[0]["retarget_tenant_id"] == target


async def test_a_wire_retarget_tenant_id_is_persisted(db_session_factory):
    target = uuid.uuid4()
    before = _invalid_count()
    # Upper-case on the wire: the stored value must be the canonical uuid.
    event = _event(tenant="rt-a", retarget_tenant_id=str(target).upper())

    assert await flush_events([event], db_session_factory, None) == 1

    (row,) = await _rows(db_session_factory)
    assert row.retarget_tenant_id == str(target)
    assert row.tenant_id == T("rt-a"), "the retarget must not overwrite tenant_id"
    assert _invalid_count() == before


async def test_no_wire_retarget_tenant_id_persists_null(db_session_factory):
    """NEGATIVE CONTROL: absence is NULL, never a sentinel, and is not counted."""
    before = _invalid_count()

    assert await flush_events([_event(tenant="rt-b")], db_session_factory, None) == 1

    (row,) = await _rows(db_session_factory)
    assert row.retarget_tenant_id is None
    assert _invalid_count() == before


@pytest.mark.parametrize(
    "bad",
    ["not-a-uuid", "Bearer eyJhbGciOiJIUzI1NiJ9.x.y", "00000000-0000-0000-0000-000000000000"],
)
async def test_a_malformed_retarget_tenant_id_persists_null_without_failing(
    db_session_factory, bad
):
    """A bad value must not fail the bulk INSERT (``uuid`` column) or drop the
    event — nor the good event batched beside it. It stores NULL and is counted."""
    good_target = uuid.uuid4()
    before = _invalid_count()
    batch = [
        _event(tenant="rt-c", retarget_tenant_id=bad),
        _event(tenant="rt-d", retarget_tenant_id=str(good_target)),
    ]

    assert await flush_events(batch, db_session_factory, None) == 2

    by_tenant = {r.tenant_id: r for r in await _rows(db_session_factory)}
    assert by_tenant[T("rt-c")].retarget_tenant_id is None
    assert by_tenant[T("rt-d")].retarget_tenant_id == str(good_target)
    assert _invalid_count() == before + 1
