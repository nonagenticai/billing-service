"""``event_flusher`` must persist the optional wire ``actor_id`` (alembic 0052).

license-authz#73 sends ``actor_id`` (a UUID Keycloak ``sub``, present only when
it is a UUID, never ``""``) on ``billing:events``. Controls: absence stays NULL
and is not counted; a malformed non-empty value is stored NULL, counted on
``billing_actor_malformed_total{service_key}``, and fails neither the flush nor
the good event batched beside it.
"""

import uuid

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import select

from models.db import MeteringEvent
from services.aggregator import flush_events
from services.consumer import _stage_message
from settings import get_settings
from tests.conftest import T

pytestmark = pytest.mark.unit


def _event(*, tenant: str, service_key: str = "workers", **over) -> dict:
    event = {
        "tenant_id": T(tenant),
        "license_id": str(uuid.uuid4()),
        "app_id": str(uuid.uuid4()),
        "plan_id": str(uuid.uuid4()),
        "method": "GET",
        "path": "/workers/api/tasks",
        "status": "allowed",
        "source_ip": "10.0.0.1",
        "_service_key": service_key,
    }
    event.update(over)
    return event


async def _rows(session_factory) -> list[MeteringEvent]:
    async with session_factory() as session:
        return list((await session.execute(select(MeteringEvent))).scalars().all())


def _malformed(service_key: str) -> float:
    return (
        REGISTRY.get_sample_value("billing_actor_malformed_total", {"service_key": service_key})
        or 0.0
    )


def test_consumer_stages_actor_id_through_unchanged():
    """The consumer must not strip the field before the flusher sees it."""
    settings = get_settings()
    actor = str(uuid.uuid4())
    events: list[dict] = []
    costs: list[dict] = []
    fields = {"tenant_id": T("actor-stage"), "path": "/workers/x", "actor_id": actor}

    _stage_message(fields, "1-0", settings.events_stream, settings, events, costs)

    assert len(events) == 1 and not costs
    assert events[0]["actor_id"] == actor


async def test_a_wire_actor_id_is_persisted(db_session_factory):
    """⛔ FAILS WHEN REVERTED: origin/main dropped actor_id on the floor."""
    actor = uuid.uuid4()
    sk = f"actor-ok-{uuid.uuid4().hex[:8]}"
    # Upper-case on the wire: the stored value must be the canonical uuid.
    event = _event(tenant="actor-a", service_key=sk, actor_id=str(actor).upper())

    assert await flush_events([event], db_session_factory, None) == 1

    (row,) = await _rows(db_session_factory)
    assert row.actor_id == actor
    assert row.subject_id is None, "actor_id must not be conflated with subject_id"
    assert _malformed(sk) == 0.0


@pytest.mark.parametrize("absent", [None, ""])
async def test_no_wire_actor_id_persists_null_uncounted(db_session_factory, absent):
    """NEGATIVE CONTROL: absent/empty is NULL, never a sentinel, and not malformed."""
    sk = f"actor-none-{uuid.uuid4().hex[:8]}"
    over = {} if absent is None else {"actor_id": absent}

    assert (
        await flush_events(
            [_event(tenant="actor-b", service_key=sk, **over)], db_session_factory, None
        )
        == 1
    )

    (row,) = await _rows(db_session_factory)
    assert row.actor_id is None
    assert _malformed(sk) == 0.0


@pytest.mark.parametrize("bad", ["not-a-uuid", "00000000-0000-0000-0000-00000000000Z"])
async def test_a_malformed_actor_id_persists_null_counted_without_failing(db_session_factory, bad):
    sk = f"actor-bad-{uuid.uuid4().hex[:8]}"
    good_actor = uuid.uuid4()
    batch = [
        _event(tenant="actor-c", service_key=sk, actor_id=bad),
        _event(tenant="actor-d", service_key=sk, actor_id=str(good_actor)),
    ]

    assert await flush_events(batch, db_session_factory, None) == 2

    by_tenant = {r.tenant_id: r for r in await _rows(db_session_factory)}
    assert by_tenant[T("actor-c")].actor_id is None
    assert by_tenant[T("actor-d")].actor_id == good_actor
    assert _malformed(sk) == 1.0
