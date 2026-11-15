"""Per-emitter project/actor attribution COVERAGE gauges.

``billing_attribution_coverage_events{service_key, axis}`` (denominator) and
``billing_attribution_covered_events{service_key, axis}`` (numerator), set by
the attribution audit pass from ONE statement over RAW ``billing_cost_events``
in a ROLLING window.

The SQL half is graded against real Postgres (``db_session``): the predicates
(``IS NOT NULL`` per axis, the billable filter, the rolling lower bound) are
things Postgres evaluates, and grading them against a stub would only grade the
stub. The publish half (label bookkeeping, vanished-key removal) is pure.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from observability import metrics as m
from services import attribution_audit as aa

#: Emitter labels. Held in constants (not `service_key="..."` literals) because
#: detect-secrets' keyword detector reads a quoted value after `*_key=` as a
#: credential; a name reference is not flagged and needs no pragma chase.
EMITTER_A = "emitter-a"
EMITTER_B = "emitter-b"
EMITTER_OLD = "emitter-old"

_INSERT = text(
    "INSERT INTO billing_cost_events"
    " (id, tenant_id, tenant_name, license_id, project_id, actor_id, service_key,"
    "  provider, input_tokens, output_tokens, cost_usd,"
    "  excluded_from_billing, exclusion_reason, metadata, created_at)"
    " VALUES (:id, :tenant_id, 'acme', NULL, :project_id, :actor_id, :service_key,"
    "  'pgx-local', 100, 50, 0.01, :excluded, :reason, CAST(:meta AS json), :created_at)"
)


async def _insert(
    session,
    *,
    service_key: str,
    project: bool = False,
    actor: bool = False,
    excluded: bool = False,
    meta: dict | None = None,
    age: timedelta = timedelta(hours=1),
) -> None:
    await session.execute(
        _INSERT,
        {
            "id": uuid.uuid4(),
            "tenant_id": str(uuid.uuid4()),
            "project_id": uuid.uuid4() if project else None,
            "actor_id": uuid.uuid4() if actor else None,
            "service_key": service_key,
            "excluded": excluded,
            "reason": "router_duplicate" if excluded else None,
            "meta": json.dumps(meta) if meta is not None else None,
            "created_at": datetime.now(UTC) - age,
        },
    )


def _series(gauge) -> dict[tuple[str, str], float]:
    return {
        (s.labels["service_key"], s.labels["axis"]): s.value
        for metric in gauge.collect()
        for s in metric.samples
    }


@pytest.fixture(autouse=True)
def _empty_gauges():
    """Start and end every test with no coverage series (module-global gauges)."""
    aa.publish_emitter_coverage([])
    yield
    aa.publish_emitter_coverage([])


# ---------------------------------------------------------------------------
# SQL, against Postgres.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_counts_per_emitter_and_axis_in_the_rolling_window(db_session) -> None:
    # emitter-a: 4 billable in-window rows -> 2 with project, 3 with actor.
    await _insert(
        db_session,
        service_key=EMITTER_A,
        project=True,
        actor=True,
    )
    await _insert(db_session, service_key=EMITTER_A, project=True, actor=True)
    await _insert(db_session, service_key=EMITTER_A, actor=True)
    # tenant_scoped: complete attribution, but NOT project coverage.
    await _insert(db_session, service_key=EMITTER_A, meta={"spend_scope": "tenant"})
    # Excluded: a parked duplicate is in no population.
    await _insert(db_session, service_key=EMITTER_A, project=True, actor=True, excluded=True)
    # Outside the rolling 24h window: in no population.
    await _insert(
        db_session, service_key=EMITTER_A, project=True, actor=True, age=timedelta(hours=30)
    )
    # emitter-b: 1 bare row.
    await _insert(db_session, service_key=EMITTER_B)
    # emitter-old: only rows older than the window -> absent entirely.
    await _insert(db_session, service_key=EMITTER_OLD, project=True, age=timedelta(hours=25))
    await db_session.commit()

    rows = sorted(await aa.load_emitter_coverage(db_session, 24))

    assert rows == [("emitter-a", 4, 2, 3), ("emitter-b", 1, 0, 0)]


@pytest.mark.asyncio
async def test_an_empty_window_returns_no_rows(db_session) -> None:
    await _insert(db_session, service_key=EMITTER_OLD, project=True, age=timedelta(hours=48))
    await db_session.commit()

    assert await aa.load_emitter_coverage(db_session, 24) == []


# ---------------------------------------------------------------------------
# Publishing: values, label symmetry, vanished-key removal.
# ---------------------------------------------------------------------------


def test_publish_sets_counts_on_both_axes() -> None:
    aa.publish_emitter_coverage([("emitter-a", 4, 2, 3)])

    assert _series(m.attribution_coverage_events) == {
        ("emitter-a", "project"): 4.0,
        ("emitter-a", "actor"): 4.0,
    }
    assert _series(m.attribution_covered_events) == {
        ("emitter-a", "project"): 2.0,
        ("emitter-a", "actor"): 3.0,
    }


def test_a_vanished_service_key_is_removed_not_zeroed() -> None:
    aa.publish_emitter_coverage([("emitter-a", 4, 2, 3), ("emitter-b", 1, 0, 0)])
    aa.publish_emitter_coverage([("emitter-b", 5, 5, 1)])

    assert _series(m.attribution_coverage_events) == {
        ("emitter-b", "project"): 5.0,
        ("emitter-b", "actor"): 5.0,
    }
    assert _series(m.attribution_covered_events) == {
        ("emitter-b", "project"): 5.0,
        ("emitter-b", "actor"): 1.0,
    }


def test_an_empty_window_removes_every_series() -> None:
    aa.publish_emitter_coverage([("emitter-a", 4, 2, 3)])
    aa.publish_emitter_coverage([])

    assert _series(m.attribution_coverage_events) == {}
    assert _series(m.attribution_covered_events) == {}
