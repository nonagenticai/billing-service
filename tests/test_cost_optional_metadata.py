"""Optional conveyor attribution: ``card_id`` / ``execution_id`` on cost events.

inference-router stamps both as flat wire fields on conveyor (n8n) calls only.
They are STORED in ``metadata`` (``by_card`` reads ``metadata->>'card_id'``) but
are NOT on ``COST_METADATA_WHITELIST`` and therefore NOT placeholder-counted:
their absence on every non-conveyor emitter is correct, not a defect.
"""

import uuid

import pytest
from sqlalchemy import select, text

import observability.metrics as om
from models.db import CostEvent
from services._parsers import (
    COST_METADATA_WHITELIST,
    OPTIONAL_COST_METADATA_FIELDS,
    OPTIONAL_COST_METADATA_MAX_LEN,
    PLACEHOLDER_COUNTED_FIELDS,
    parse_optional_metadata_str,
)
from services.aggregator import flush_costs
from tests._aggregator_helpers import _make_cost
from tests.conftest import T

PROJECT = uuid.UUID("0ad49b9c-0000-4000-8000-0000000000c1")


def _counter(field: str, service_key: str) -> float:
    return om.placeholder_value_total.labels(field=field, service_key=service_key)._value.get()


@pytest.mark.unit
def test_optional_fields_are_not_placeholder_counted() -> None:
    for k in OPTIONAL_COST_METADATA_FIELDS:
        assert k not in COST_METADATA_WHITELIST
        assert k not in PLACEHOLDER_COUNTED_FIELDS


@pytest.mark.unit
@pytest.mark.parametrize(
    ("val", "expected"),
    [
        ("  card-123  ", "card-123"),
        ("x" * OPTIONAL_COST_METADATA_MAX_LEN, "x" * OPTIONAL_COST_METADATA_MAX_LEN),
        ("x" * (OPTIONAL_COST_METADATA_MAX_LEN + 1), None),
        ("", None),
        ("   ", None),
        ("unknown", None),
        (None, None),
        (12345, None),
    ],
)
def test_parse_optional_metadata_str(val, expected) -> None:
    assert parse_optional_metadata_str(val) == expected


@pytest.mark.unit
async def test_stored_when_present(db_session_factory) -> None:
    buffer = [
        _make_cost(
            tenant_id=T("opt-present"),
            schema_id="s",
            card_id=" 1234567890123456789 ",
            execution_id="98765",
        )
    ]
    assert await flush_costs(buffer, db_session_factory, None) == 1
    async with db_session_factory() as session:
        row = (await session.execute(select(CostEvent))).scalar_one()
        assert row.extra_metadata is not None
        assert row.extra_metadata["schema_id"] == "s"  # CONTROL
        assert row.extra_metadata["card_id"] == "1234567890123456789"
        assert row.extra_metadata["execution_id"] == "98765"
        # Readable by the exact expression by_card groups on.
        got = (
            await session.execute(text("SELECT metadata->>'card_id' AS c FROM billing_cost_events"))
        ).one()
        assert got.c == "1234567890123456789"


@pytest.mark.unit
async def test_absent_is_not_stored_and_not_counted(db_session_factory) -> None:
    sk = "opt-absent-emitter"
    before = {k: _counter(k, sk) for k in OPTIONAL_COST_METADATA_FIELDS}
    before_ctl = _counter("job_id", sk)
    buffer = [_make_cost(tenant_id=T("opt-absent"), service_key=sk, schema_id="s")]
    assert await flush_costs(buffer, db_session_factory, None) == 1
    async with db_session_factory() as session:
        row = (await session.execute(select(CostEvent))).scalar_one()
        assert row.extra_metadata is not None and row.extra_metadata["schema_id"] == "s"
        for k in OPTIONAL_COST_METADATA_FIELDS:
            assert k not in row.extra_metadata
    # CONTROL: a whitelisted key absent on the same event IS counted, so the
    # counter is live for this service_key and the zero delta below means something.
    assert _counter("job_id", sk) == before_ctl + 1
    for k in OPTIONAL_COST_METADATA_FIELDS:
        assert _counter(k, sk) == before[k]


@pytest.mark.unit
async def test_over_long_is_dropped(db_session_factory) -> None:
    buffer = [
        _make_cost(
            tenant_id=T("opt-long"),
            schema_id="s",
            card_id="c" * (OPTIONAL_COST_METADATA_MAX_LEN + 1),
            execution_id="e" * (OPTIONAL_COST_METADATA_MAX_LEN + 1),
        )
    ]
    assert await flush_costs(buffer, db_session_factory, None) == 1
    async with db_session_factory() as session:
        row = (await session.execute(select(CostEvent))).scalar_one()
        assert row.extra_metadata is not None and row.extra_metadata["schema_id"] == "s"
        assert "card_id" not in row.extra_metadata
        assert "execution_id" not in row.extra_metadata


@pytest.mark.unit
async def test_by_card_end_to_end_from_flushed_event(db_session_factory, app_client) -> None:
    tenant = T("opt-e2e")
    buffer = [
        _make_cost(
            tenant_id=tenant,
            project_id=str(PROJECT),
            card_id="planka-card-1",
            execution_id="4711",
            cost_usd="0.5",
        ),
        _make_cost(tenant_id=tenant, project_id=str(PROJECT), cost_usd="0.25"),
    ]
    assert await flush_costs(buffer, db_session_factory, None) == 2
    async with db_session_factory() as session:
        rows = (await session.execute(select(CostEvent))).scalars().all()
        assert {r.project_id for r in rows} == {PROJECT}  # CONTROL: project survived

    r = app_client.get(f"/api/costs/projects/{PROJECT}", headers={"X-Tenant-Id": tenant})
    assert r.status_code == 200, r.text
    cards = {c["card_id"]: c["calls"] for c in r.json()["data"]["by_card"]}
    assert cards == {None: 1, "planka-card-1": 1}
