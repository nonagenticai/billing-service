"""The accept path was fail-open; an unregistered tenant is now PARKED, not billed.

THE DEFECT
----------
``tenancy.canonical_tenant_id`` resolves step (a) — "anything ``uuid.UUID``
accepts" — with NO registry lookup at all. It counted the miss on
``billing_tenant_unregistered_total`` and returned the id anyway. So any caller
could mint a tenant by putting a random uuid in ``X-Tenant-Id`` and be metered as
one, and ``validate_cost_event`` did not catch it either: that validator rejects
a tenant that fails to RESOLVE, and an unregistered uuid resolves perfectly.

WHY THE FIX IS NOT A REJECTION
------------------------------
Dead-lettering would destroy the money record, which is worse than
mis-attributing it and is unrecoverable — billing is append-only with no refund
path. And the nil sentinel is itself dead-lettered by ``_cost_schema``, so
routing there deletes the events outright. The row is STORED and PARKED instead:
``excluded_from_billing = true``, ``exclusion_reason = 'unregistered_tenant'``,
reversible with one UPDATE, and dropped from every billable aggregate by the
filter those aggregates already carry.

⛔ THE GATE DEFAULTS ON — `billing_exclude_unregistered_tenants` defaults True
(armed in production since 2026-09-16; code default flipped 2026-10-15). Every
test below PINS the flag explicitly rather than inheriting the default: the
"parked" tests use ``armed``, the disarmed tests use ``disarmed`` and assert
that, disarmed, the check runs and the counter moves and NOTHING ELSE changes:
no park, no skipped rollup, no skipped debit.

⚠️ THE KNOWN LIMIT OF THIS CONTROL
-----------------------------------
``_REGISTERED_TENANT_IDS`` is a hand-refreshed SNAPSHOT, not a live view.
Billing cannot see ``licenses.tenants`` at all — its service account gets HTTP
403 on ``GET /tenants`` (measured 2026-08-28, recorded in the
``billing_license_tenant_unregistered_events`` help text). So a tenant
registered upstream but not yet in the snapshot is parked here and goes
UNBILLED until someone refreshes it. That is not hypothetical: on 2026-09-05 the
snapshot was 11 days old and missing SIX of 76 registered names.

The exclusion is therefore deliberately reversible and deliberately counted.
``billing_cost_events_excluded_total{reason="unregistered_tenant"}`` rising for a
service_key that should be legitimate is the signal to refresh the snapshot
(``scripts/refresh_tenant_registry_fixture.sh``) and un-park with one UPDATE.

NOTE ON TENANT VALUES IN THIS FILE
----------------------------------
Every test here uses a raw ``uuid.uuid4()``, NEVER ``T()``. ``T()`` registers
what it mints (see ``tests/conftest.py``), so using it would test the billing
path while claiming to test the exclusion path. These uuid4 values are the only
thing keeping that helper from hiding this control.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

import observability.metrics as om
from models.db import CostEvent, TokenUsage
from services.aggregator import flush_costs
from tenancy import LEGACY_TENANT_NAMES, is_registered_tenant_id
from tests._aggregator_helpers import _make_cost

#: A registered tenant, taken from the shim rather than invented — the control
#: group. ``fr-test`` is a real row in the licences registry.
REGISTERED_TENANT = LEGACY_TENANT_NAMES["fr-test"]


def _counter(service_key: str, action: str) -> float:
    return om.cost_events_unregistered_tenant_total.labels(
        service_key=service_key, action=action
    )._value.get()


def _pin_gate(monkeypatch, value: bool):
    """Pin ``billing_exclude_unregistered_tenants`` for one test.

    Patches the cached Settings instance rather than the environment, because
    ``get_settings`` is lru_cached and ``flush_costs`` reads the flag once per
    call — an env change alone would not be seen.
    """
    from settings import get_settings

    get_settings.cache_clear()
    settings = get_settings()
    monkeypatch.setattr(settings, "billing_exclude_unregistered_tenants", value)
    monkeypatch.setattr("settings.get_settings", lambda: settings)
    monkeypatch.setattr("services.cost_flusher.get_settings", lambda: settings)
    return settings


@pytest.fixture
def armed(monkeypatch):
    """Arm the gate (the code default, pinned so the test does not depend on it)."""
    from settings import get_settings

    yield _pin_gate(monkeypatch, True)
    get_settings.cache_clear()


@pytest.fixture
def disarmed(monkeypatch):
    """Disarm the gate, as ``BILLING_EXCLUDE_UNREGISTERED_TENANTS=false`` would."""
    from settings import get_settings

    yield _pin_gate(monkeypatch, False)
    get_settings.cache_clear()


async def _rows(session_factory) -> list[CostEvent]:
    async with session_factory() as session:
        return list((await session.execute(select(CostEvent))).scalars())


# ---------------------------------------------------------------------------
# The predicate itself
# ---------------------------------------------------------------------------


def test_a_random_uuid_is_not_a_registered_tenant() -> None:
    """The premise. If this were false every assertion below would be vacuous."""
    assert not is_registered_tenant_id(str(uuid.uuid4()))


def test_every_shim_uuid_except_a_sentinel_is_registered() -> None:
    """The other direction — a predicate that always says False parks everyone.

    ``SENTINEL_TENANT_IDS`` is skipped, and the skip is the POINT rather than an
    exemption for convenience: ``"system"`` is a placeholder an emitter sends
    when it has no tenant to name, so it is a key of the shim (the name must
    keep resolving, or the event is dead-lettered and its money destroyed) while
    deliberately NOT being vouched for (or it bills as a tenant nobody owns).
    Those two facts are only expressible as a hole in this loop.

    The hole is bounded from the other side by
    ``tests/test_system_sentinel_parked.py::test_the_sentinel_set_is_exactly_the_system_id``,
    so a future addition to the skip list cannot pass unnoticed here.
    """
    from tenancy import SENTINEL_TENANT_IDS

    for name, tenant_id in LEGACY_TENANT_NAMES.items():
        if tenant_id in SENTINEL_TENANT_IDS:
            continue
        assert is_registered_tenant_id(tenant_id), f"{name} is not vouched for"


def test_the_nil_tenant_is_not_treated_as_unregistered() -> None:
    """It is a REAL registry row (``default-anon``), and it is already dead-lettered.

    Answering False for it would route one condition to two different controls.
    """
    from tenancy import NIL_TENANT_ID

    assert is_registered_tenant_id(NIL_TENANT_ID)


# ---------------------------------------------------------------------------
# DISARMED (gate OFF): measure only — behaviour identical to before this code
# ---------------------------------------------------------------------------


async def test_default_is_on(monkeypatch) -> None:
    """The shipped default matches production (armed since 2026-09-16)."""
    from settings import Settings

    monkeypatch.delenv("BILLING_EXCLUDE_UNREGISTERED_TENANTS", raising=False)
    assert Settings().billing_exclude_unregistered_tenants is True


async def test_off_does_not_park_the_row(disarmed, db_session_factory) -> None:
    """⛔ BYTE-IDENTICAL TO TODAY. The row bills exactly as it did before."""
    tenant = str(uuid.uuid4())
    count = await flush_costs([_make_cost(tenant_id=tenant)], db_session_factory, None)
    assert count == 1

    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert str(rows[0].tenant_id) == tenant
    assert rows[0].excluded_from_billing is False, "the gate parked a row while OFF"
    assert rows[0].exclusion_reason is None


async def test_off_still_rolls_up_and_debits(disarmed, db_session_factory) -> None:
    """The other half of "identical to today": the billable side effects still run.

    A gate that is "off" but silently withholds the rollup would be the same
    under-billing defect wearing a different name.
    """
    tenant = str(uuid.uuid4())
    await flush_costs([_make_cost(tenant_id=tenant)], db_session_factory, None)

    async with db_session_factory() as session:
        usage = list(
            (
                await session.execute(select(TokenUsage).where(TokenUsage.tenant_id == tenant))
            ).scalars()
        )
    assert usage, "an unregistered tenant's spend did not roll up while the gate was OFF"


async def test_off_still_counts(disarmed, db_session_factory) -> None:
    """THE WHOLE POINT OF SHIPPING IT OFF: measure before arming.

    A flag that suppressed the measurement too would make the arming criterion
    ("the counter must read 0") permanently unanswerable — the gate could never
    be safely closed, and the flag would become permanent by accident.
    """
    before = _counter("inference-router", "counted")
    await flush_costs([_make_cost(tenant_id=str(uuid.uuid4()))], db_session_factory, None)
    assert _counter("inference-router", "counted") - before == 1


async def test_off_does_not_touch_the_excluded_series(disarmed, db_session_factory) -> None:
    """action="excluded" must mean a row was actually parked, in every state."""
    before = _counter("inference-router", "excluded")
    await flush_costs([_make_cost(tenant_id=str(uuid.uuid4()))], db_session_factory, None)
    assert _counter("inference-router", "excluded") - before == 0


async def test_off_leaves_a_registered_tenant_uncounted(disarmed, db_session_factory) -> None:
    """THE CONTROL GROUP for the OFF state: the check still discriminates."""
    before = _counter("inference-router", "counted")
    await flush_costs([_make_cost(tenant_id=REGISTERED_TENANT)], db_session_factory, None)
    assert _counter("inference-router", "counted") - before == 0


# ---------------------------------------------------------------------------
# ARMED (gate ON): the row is STORED — the money record survives
# ---------------------------------------------------------------------------


async def test_an_unregistered_tenant_event_is_still_persisted(armed, db_session_factory) -> None:
    """⛔ THE ROW MUST SURVIVE. Rejecting would destroy an unrecoverable record."""
    tenant = str(uuid.uuid4())
    count = await flush_costs([_make_cost(tenant_id=tenant)], db_session_factory, None)
    assert count == 1, "the event was dropped instead of parked"

    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert str(rows[0].tenant_id) == tenant, "the tenant was rewritten, not recorded"


async def test_it_is_parked_with_a_reason(armed, db_session_factory) -> None:
    """Parked, and the reason is machine-readable so the park is reversible."""
    tenant = str(uuid.uuid4())
    await flush_costs([_make_cost(tenant_id=tenant)], db_session_factory, None)

    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is True
    assert rows[0].exclusion_reason == "unregistered_tenant"


async def test_a_registered_tenant_is_not_parked(armed, db_session_factory) -> None:
    """THE CONTROL GROUP.

    Without this the exclusion could be unconditional — parking every event —
    and every assertion above would still pass. A guard that only ever refuses
    is as broken as one that only ever allows.
    """
    await flush_costs([_make_cost(tenant_id=REGISTERED_TENANT)], db_session_factory, None)

    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert rows[0].excluded_from_billing is False
    assert rows[0].exclusion_reason is None


# ---------------------------------------------------------------------------
# ARMED: ...but it does not COUNT — no rollup, no debit
# ---------------------------------------------------------------------------


async def test_a_parked_row_does_not_roll_up_tokens(armed, db_session_factory) -> None:
    """Otherwise the exclusion is a lie told only to the aggregates.

    The row would still drain a budget and still inflate token_usage — still be
    billed — while every report said it was not.
    """
    tenant = str(uuid.uuid4())
    await flush_costs([_make_cost(tenant_id=tenant)], db_session_factory, None)

    async with db_session_factory() as session:
        usage = list(
            (
                await session.execute(select(TokenUsage).where(TokenUsage.tenant_id == tenant))
            ).scalars()
        )
    assert usage == [], f"a parked row rolled up tokens: {usage}"


async def test_a_registered_tenant_still_rolls_up(armed, db_session_factory) -> None:
    """The control group again: the skip must be conditional on the park."""
    await flush_costs([_make_cost(tenant_id=REGISTERED_TENANT)], db_session_factory, None)

    async with db_session_factory() as session:
        usage = list(
            (
                await session.execute(
                    select(TokenUsage).where(TokenUsage.tenant_id == REGISTERED_TENANT)
                )
            ).scalars()
        )
    assert usage, "a registered tenant's spend did not roll up"


# ---------------------------------------------------------------------------
# ARMED: ...and it is COUNTABLE, under the other action label
# ---------------------------------------------------------------------------


async def test_the_exclusion_is_counted(armed, db_session_factory) -> None:
    """Deltas, not absolutes: the prometheus registry is session-global."""
    before = _counter("inference-router", "excluded")
    await flush_costs([_make_cost(tenant_id=str(uuid.uuid4()))], db_session_factory, None)
    assert _counter("inference-router", "excluded") - before == 1


async def test_arming_moves_the_label_not_just_the_value(armed, db_session_factory) -> None:
    """The two actions are mutually exclusive for one event.

    If both fired, the "counted" series would keep rising after arming and the
    arming criterion could never read 0 — the instrument would contradict the
    decision it exists to inform.
    """
    before = _counter("inference-router", "counted")
    await flush_costs([_make_cost(tenant_id=str(uuid.uuid4()))], db_session_factory, None)
    assert _counter("inference-router", "counted") - before == 0


async def test_a_registered_tenant_does_not_move_the_counter(armed, db_session_factory) -> None:
    """A counter that only goes up measures nothing."""
    before = _counter("inference-router", "excluded")
    await flush_costs([_make_cost(tenant_id=REGISTERED_TENANT)], db_session_factory, None)
    assert _counter("inference-router", "excluded") - before == 0


# ---------------------------------------------------------------------------
# ARMED: the park must be invisible to every billable read
# ---------------------------------------------------------------------------


async def test_parked_rows_are_dropped_by_the_billable_filter(armed, db_session_factory) -> None:
    """The exclusion only means anything because the readers already filter on it.

    Asserted through the same predicate the aggregates use, so this cannot pass
    while ``rollup_engine`` / ``routers.costs`` / ``routers.export`` disagree.
    """
    await flush_costs(
        [
            _make_cost(tenant_id=str(uuid.uuid4())),
            _make_cost(tenant_id=REGISTERED_TENANT),
        ],
        db_session_factory,
        None,
    )

    async with db_session_factory() as session:
        billable = list(
            (
                await session.execute(
                    select(CostEvent).where(CostEvent.excluded_from_billing.is_(False))
                )
            ).scalars()
        )
        everything = list((await session.execute(select(CostEvent))).scalars())

    assert len(everything) == 2, "a row was destroyed rather than parked"
    assert len(billable) == 1, "the parked row is still billable"
    assert str(billable[0].tenant_id) == REGISTERED_TENANT


# ---------------------------------------------------------------------------
# ARMED: the park must NOT be invisible to the detector that sizes it (#241)
# ---------------------------------------------------------------------------


async def test_a_parked_row_is_still_seen_by_the_unregistered_tenant_audit(
    armed, db_session_factory
) -> None:
    """Arming the gate must not make the registry gap read as fixed.

    End-to-end through the REAL write path: the armed flusher parks the row
    (``excluded_from_billing = true``, ``exclusion_reason =
    'unregistered_tenant'``), and ``attribution_audit._UNREGISTERED_TENANT_SQL``
    must still count it. With a plain ``excluded_from_billing = false`` filter
    there, ``billing_license_tenant_unregistered_events`` would drop to 0 the day
    the gate was armed. The test above proves the row is dropped from the
    BILLABLE reads; this one proves it is not dropped from the DETECTOR.
    """
    from services import attribution_audit as aa

    tenant = str(uuid.uuid4())
    await flush_costs([_make_cost(tenant_id=tenant)], db_session_factory, None)

    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is True, "precondition: the gate did not park the row"

    audit = await aa.run_attribution_audit(db_session_factory, window_hours=24)

    named = dict(audit.top_unregistered_tenants)
    assert named.get(tenant) == 1, "the parked unregistered row vanished from the detector"
    assert audit.unregistered_tenant_events == 1
    # Spend is not asserted here: this flush path stores cost_usd NULL (no price
    # for the helper's event), so the count is the signal. Spend through the same
    # predicate is pinned in test_attribution_unattributed_measurement.py.
