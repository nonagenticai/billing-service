"""``"system"`` is not a customer: it resolves, it is never vouched for, it parks.

THE DEFECT
----------
An emitter with no tenant to name sends the literal string ``"system"``.
``tenancy.LEGACY_TENANT_NAMES`` holds ``"system"`` as a KEY, so it resolved to
``00000000-0000-0000-0000-000000000001`` — and because
``_REGISTERED_TENANT_IDS`` was built as ``frozenset(LEGACY_TENANT_NAMES.values())``,
that very uuid was also VOUCHED FOR. The round trip closed on itself: a
fabricated name became a registered tenant, and every such event billed.

Measured on the prod billing primary 2026-09-16:
1,207 rows, $23.53950500, every one of them ``service_key='inference-router'``,  # pragma: allowlist secret
and ``excluded_from_billing`` false on all 1,207.

⛔ WHY THE FIX IS NOT "DELETE THE KEY"
--------------------------------------
Dropping ``"system"`` from ``LEGACY_TENANT_NAMES`` does not park these events.
It DROPS them:

    resolve_tenant_name_to_id("system") -> None
      -> validate_cost_event -> "invalid_tenant_id"
      -> cost_flusher: dead_letters.append(); continue

That ``continue`` writes no Postgres row at all. The registry/park check lives
127 lines further down the same loop and is never reached, so the money record
is destroyed rather than parked — and the DLQ it lands in (Redis
``billing:costs:dead``) is capped at 100k, trimmed at 720h, and redacted on the
way in. Unrecoverable, in a ledger that is append-only with no refund path.

So the name KEEPS RESOLVING and the ID STOPS BEING VOUCHED FOR. The event passes
validation with its money intact, reaches the flusher, fails
``is_registered_tenant_id``, and is parked in ``billing_cost_events`` with
``excluded_from_billing = true`` / ``exclusion_reason = 'unregistered_tenant'`` —
visible in SQL and reversible with one UPDATE.

⚠️ THIS IS WHAT MAKES THE ARMED GATE DO ANYTHING
------------------------------------------------
``BILLING_EXCLUDE_UNREGISTERED_TENANTS=true`` is armed in production, and
``exclusion_reason = 'unregistered_tenant'`` has parked ZERO rows all-time
(verified on the primary, 2026-09-16). Until this change every id that reached
the check was vouched for by the snapshot, so the armed gate was inert. These
1,207 rows are the first population it acts on.

NOTE ON THE GATE FLAG
---------------------
``billing_exclude_unregistered_tenants`` defaults TRUE (matching production),
but every test pins it explicitly rather than inheriting the default: the park
is asserted under the ``armed`` fixture, exactly as
``tests/test_unregistered_tenant_exclusion.py`` does, and the OFF test below
uses ``disarmed`` to pin the other half: with the gate off, a sentinel event is
counted and BILLS, which is byte-identical to before the gate existed.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from models.db import CostEvent
from services._cost_schema import validate_cost_event
from services.aggregator import flush_costs
from tenancy import (
    LEGACY_TENANT_NAMES,
    SENTINEL_TENANT_IDS,
    canonical_tenant_id,
    is_registered_tenant_id,
    registration_source,
)
from tests._aggregator_helpers import _make_cost

#: The literal the emitter puts on the wire.
SENTINEL_NAME = "system"

#: The uuid that name has always resolved to.
SENTINEL_ID = "00000000-0000-0000-0000-000000000001"

#: A genuinely registered tenant — the control group for every assertion here.
REGISTERED_TENANT = LEGACY_TENANT_NAMES["fr-test"]


def _pin_gate(monkeypatch, value: bool):
    """Pin ``billing_exclude_unregistered_tenants`` for one test.

    Patches the cached Settings instance rather than the environment:
    ``get_settings`` is lru_cached and ``flush_costs`` reads the flag once per
    call, so an env change alone would not be seen.
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
# The premise: the name still resolves. If this breaks, the events are DROPPED.
# ---------------------------------------------------------------------------


def test_the_sentinel_name_still_resolves() -> None:
    """⛔ THE LOAD-BEARING ASSERTION OF THE WHOLE CHANGE.

    The moment this returns None the events stop being parked and start being
    dead-lettered, which destroys the money record. Every other test in this
    file is about what happens AFTER resolution succeeds.
    """
    assert canonical_tenant_id(SENTINEL_NAME) == SENTINEL_ID


def test_the_sentinel_name_is_still_a_key_of_the_shim() -> None:
    """The mechanism behind the test above, pinned separately.

    Stated as its own test so a future regeneration of the snapshot that drops
    the key fails HERE, naming the cause, rather than only failing the resolver
    test and sending the reader hunting.
    """
    assert LEGACY_TENANT_NAMES[SENTINEL_NAME] == SENTINEL_ID


def test_a_sentinel_event_is_not_dead_lettered() -> None:
    """The validator must let it through — the DLQ is where the money dies."""
    assert validate_cost_event({"tenant_id": SENTINEL_NAME, "provider": "anthropic"}) is None


# ---------------------------------------------------------------------------
# The predicate: it resolves, and it is still not vouched for
# ---------------------------------------------------------------------------


def test_the_sentinel_id_is_not_a_registered_tenant() -> None:
    assert not is_registered_tenant_id(SENTINEL_ID)


def test_the_sentinel_id_is_absent_from_the_vouched_set() -> None:
    """It is a VALUE of the shim, so it is in the union unless subtracted."""
    from tenancy import _REGISTERED_TENANT_IDS

    assert SENTINEL_ID not in _REGISTERED_TENANT_IDS


def test_the_source_is_sentinel_not_unknown() -> None:
    """A source of its own, so the counter can separate it from genuine unknowns.

    The two call for different actions — fix the emitter vs. register the
    tenant — and only ``"unknown"`` asks for a registry refresh.
    """
    assert registration_source(SENTINEL_ID) == "sentinel"


def test_a_live_registry_cannot_re_vouch_for_the_sentinel() -> None:
    """⛔ THE ROBUSTNESS TEST. Subtracting from the snapshot alone is not enough.

    ``registration_source`` consults the LIVE overlay before the snapshot, so if
    licences ever holds a row named ``system`` the overlay would vouch for
    …0001 and these rows would quietly resume billing. The sentinel check runs
    FIRST for exactly this reason.
    """
    from tenancy import reset_live_tenant_registry, set_live_tenant_registry

    installed = set_live_tenant_registry({SENTINEL_NAME: SENTINEL_ID, "fr-test": REGISTERED_TENANT})
    assert installed >= 1, "the overlay refused the payload; the test would be vacuous"
    try:
        assert registration_source(SENTINEL_ID) == "sentinel"
        assert not is_registered_tenant_id(SENTINEL_ID)
        # The control: the overlay IS otherwise in force.
        assert is_registered_tenant_id(REGISTERED_TENANT)
    finally:
        # ⛔ RESET, never "restore by re-installing the snapshot". The overlay is
        # SESSION-WIDE process state and nothing else in the suite installs one,
        # so re-installing LEGACY_TENANT_NAMES here does not restore a previous
        # state — it INVENTS one, and leaves it behind for every test that runs
        # after this file. It is also not a harmless one: the shim maps
        # `default-anon` to the nil tenant, so an overlay built from it gives the
        # nil uuid a display LABEL, and `live_tenant_label` reads the overlay and
        # nothing else. Every "a missing tenant falls back to ..." test in
        # tests/test_tenant_name.py then stores `default-anon` instead of its own
        # expected fallback — a failure that appears only in full-suite order and
        # points at a file that is not the cause.
        reset_live_tenant_registry()


def test_every_other_shim_uuid_is_still_registered() -> None:
    """The blast-radius bound: exactly the sentinels lost their vouching.

    A predicate that started answering False more widely would park real
    customers' spend as unregistered — the failure the gate's own docs call the
    expensive direction.
    """
    for name, tenant_id in LEGACY_TENANT_NAMES.items():
        if tenant_id in SENTINEL_TENANT_IDS:
            continue
        assert is_registered_tenant_id(tenant_id), f"{name} stopped being vouched for"


def test_the_sentinel_set_is_exactly_the_system_id() -> None:
    """Pins the blast radius from the other side, so a future addition is deliberate."""
    assert frozenset({SENTINEL_ID}) == SENTINEL_TENANT_IDS


def test_the_nil_tenant_is_not_a_sentinel() -> None:
    """They are different controls: nil is dead-lettered, the sentinel is parked.

    Folding nil in here would route one condition to two controls — and nil is a
    REAL registry row (``default-anon``).
    """
    from tenancy import NIL_TENANT_ID

    assert NIL_TENANT_ID not in SENTINEL_TENANT_IDS
    assert is_registered_tenant_id(NIL_TENANT_ID)


# ---------------------------------------------------------------------------
# End to end through the flusher: the row is PARKED, not dropped
# ---------------------------------------------------------------------------


async def test_a_sentinel_event_is_parked_not_dropped(db_session_factory, armed) -> None:
    """THE KILLING TEST. The row must EXIST, carry its money, and not bill."""
    count = await flush_costs(
        [_make_cost(tenant_id=SENTINEL_NAME, service_key="inference-router")],
        db_session_factory,
        None,
    )
    assert count == 1, "the sentinel event was dead-lettered instead of parked"

    rows = await _rows(db_session_factory)
    assert len(rows) == 1, "no Postgres row was written — the money record was destroyed"
    row = rows[0]
    assert str(row.tenant_id) == SENTINEL_ID
    assert row.excluded_from_billing is True
    assert row.exclusion_reason == "unregistered_tenant"
    # The money must SURVIVE the park — parking withholds the billable side
    # effects, it does not blank the record. Asserted on the wire-carried hint
    # rather than on ``cost_usd``: this suite seeds no pricing row, so the
    # flusher logs ``pricing_miss`` and leaves the authoritative figure NULL.
    # That is a fixture property, not the behaviour under test, and asserting
    # ``cost_usd is not None`` here would only pin how the test DB is seeded.
    assert row.cost_usd_hint is not None and row.cost_usd_hint > 0, (
        "the parked row lost its money; a park must withhold billing, not erase the record"
    )
    assert row.input_tokens > 0 and row.output_tokens > 0, "the usage did not survive the park"


async def test_the_park_is_reversible_in_sql(db_session_factory, armed) -> None:
    """The park must be one UPDATE away from being undone — nothing is deleted.

    This is the property that makes the change safe to ship against live money:
    if ``system`` turns out to be a real tenant after all, the 1,207 rows are
    still there, still priced, and still correlatable.
    """
    await flush_costs([_make_cost(tenant_id=SENTINEL_NAME)], db_session_factory, None)

    async with db_session_factory() as session:
        row = (await session.execute(select(CostEvent))).scalars().one()
        row.excluded_from_billing = False
        row.exclusion_reason = None
        await session.commit()

    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is False
    assert rows[0].exclusion_reason is None
    # Un-parking restores a row that still carries its usage and its money — see
    # the note in the test above for why the hint, not ``cost_usd``, is the
    # figure this suite can assert on.
    assert rows[0].cost_usd_hint is not None
    assert rows[0].input_tokens > 0


async def test_a_registered_tenant_is_untouched(db_session_factory, armed) -> None:
    """THE CONTROL GROUP. Arming this must not park anybody else.

    Without this, a predicate that simply answered False for everything would
    pass every assertion above.
    """
    await flush_costs([_make_cost(tenant_id=REGISTERED_TENANT)], db_session_factory, None)

    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert rows[0].excluded_from_billing is False
    assert rows[0].exclusion_reason is None


async def test_a_random_uuid_still_parks_the_same_way(db_session_factory, armed) -> None:
    """The sentinel joins the existing population; it does not replace it."""
    await flush_costs([_make_cost(tenant_id=str(uuid.uuid4()))], db_session_factory, None)

    rows = await _rows(db_session_factory)
    assert rows[0].excluded_from_billing is True
    assert rows[0].exclusion_reason == "unregistered_tenant"


# ---------------------------------------------------------------------------
# Gate OFF: counted, and still billing — byte-identical to today
# ---------------------------------------------------------------------------


async def test_off_the_sentinel_still_bills(db_session_factory, disarmed) -> None:
    """⛔ Disarmed, the gate must not change behaviour for anyone.

    With the gate off this row bills exactly as the 1,207 live rows did before
    the sentinel was parked. The park is what the armed flag buys, and it is
    the default (production has had it armed since 2026-09-16).
    """
    count = await flush_costs([_make_cost(tenant_id=SENTINEL_NAME)], db_session_factory, None)
    assert count == 1

    rows = await _rows(db_session_factory)
    assert len(rows) == 1
    assert str(rows[0].tenant_id) == SENTINEL_ID
    assert rows[0].excluded_from_billing is False, "the gate parked a row while OFF"
    assert rows[0].exclusion_reason is None
