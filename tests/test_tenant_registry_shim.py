"""``LEGACY_TENANT_NAMES`` must agree with the licences registry, in BOTH directions.

THE DEFECT
----------
``tenancy.LEGACY_TENANT_NAMES`` is an INGRESS resolver: a name it does not hold
takes a hard 400 from this service. It is a hand-maintained SNAPSHOT of
``licenses.tenants``, and nothing checked it against the thing it snapshots.

Measured against the live registry on 2026-09-05, it had drifted both ways at
once, while its own docstring claimed "all 72 registered names" over a map of 75
keys:

* SIX registered names were MISSING — ``claude``, ``procedure-improvement``,
  ``prompt-improvement``, ``svc-kag``, ``svc-kag-eval-r46``, ``svc-w2v-shared``.
  Every caller sending one was taking a 400 from a tenant the registry vouches
  for. This is the *exact* condition the 2026-08-25 widening (six hand-picked
  names → the full snapshot) was written to end; it came straight back, because
  that change fixed the instance and left the class open.
* TWO keys the registry no longer holds — ``platform-cp-4``, ``platform-cp-5``
  — were present but UNDECLARED as renames. They resolved correctly, so nothing
  was broken; they were one regeneration away from being deleted in silence,
  which would have 400'd every caller still sending them.

WHY A FIXTURE AND NOT THE LIVE REGISTRY
---------------------------------------
CI cannot reach the licences database. The archetype gives each job an ephemeral
``postgres:18`` container with this service's own schema in it; there is no
``licenses`` database, no tailnet, and the one test in this suite with outbound
network (``test_licenses_integration.py``) self-skips without an explicit env
var. So the comparison runs against ``tests/fixtures/licenses_tenants.txt``, a
checked-in snapshot of the registry query, refreshed by
``scripts/refresh_tenant_registry_fixture.sh``.

⛔ THE FIXTURE IS NOT DERIVED FROM ``tenancy.py``. That is the whole reason it
can fail. A fixture generated from the map it checks agrees with that map by
construction — it would pass against a shim that had drifted arbitrarily far,
which is a test that cannot go red. This file is the output of an independent
query against the live registry, and the refresh script refuses to write an
empty one.

WHAT THIS CANNOT CATCH
----------------------
Drift between the REGISTRY and the FIXTURE, in the window between refreshes.
Nothing in CI can catch that, because CI cannot see the registry. What it does
guarantee is that a refresh cannot be pasted in and forgotten: the moment the
fixture moves, this test names every key that must be added, aliased, or
justified — including the retired spellings the refresh silently drops.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tenancy import (
    LEGACY_TENANT_NAMES,
    NIL_TENANT_ID,
    RENAMED_TENANT_ALIASES,
    canonical_tenant_id,
    is_registered_tenant_id,
    registration_source,
    reset_live_tenant_registry,
    set_live_tenant_registry,
)

FIXTURE = Path(__file__).parent / "fixtures" / "licenses_tenants.txt"

#: The counts stated in ``LEGACY_TENANT_NAMES``'s own docstring. Asserted here so
#: the prose cannot go stale the way "all 72 registered names" did over a map of
#: 75 keys — a wrong comment is now a red test.
DOCUMENTED_SHIM_KEYS = 165
DOCUMENTED_REGISTRY_NAMES = 160
DOCUMENTED_RETIRED_SPELLINGS = 5


def _registry() -> dict[str, str]:
    """The registry snapshot: ``name -> uuid``, comments and blanks stripped."""
    rows: dict[str, str] = {}
    for raw in FIXTURE.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        name, _, tenant_id = line.partition("|")
        assert tenant_id, f"malformed fixture row (expected 'name|uuid'): {raw!r}"
        assert name not in rows, f"duplicate name in registry fixture: {name!r}"
        rows[name] = tenant_id
    return rows


def test_fixture_is_not_empty_and_is_not_generated_from_the_shim() -> None:
    """A vacuous fixture would make every assertion below pass against anything.

    Two failure shapes this closes. An EMPTY fixture makes both directional
    checks iterate nothing. A fixture that is merely a dump of
    ``LEGACY_TENANT_NAMES`` agrees with it by construction. The second is not
    detectable by inspection alone, but it has one observable consequence: a
    generated fixture would contain the retired spellings too, because they are
    keys of the map. The registry does not hold them.
    """
    registry = _registry()
    assert len(registry) > 50, (
        f"registry fixture holds only {len(registry)} rows — refusing to treat that as "
        "a snapshot of a registry that had 111. Re-run scripts/refresh_tenant_registry_fixture.sh."
    )
    leaked = sorted(set(RENAMED_TENANT_ALIASES) & set(registry))
    assert not leaked, (
        "The registry fixture contains RETIRED spellings "
        f"{leaked} that only tenancy.py holds. That means the fixture was generated FROM "
        "LEGACY_TENANT_NAMES rather than from the licences registry, so every assertion "
        "in this file is vacuous. Regenerate it with "
        "scripts/refresh_tenant_registry_fixture.sh, which queries the registry."
    )


def test_every_registered_tenant_name_resolves() -> None:
    """Direction 1: a name the registry holds must NOT 400.

    This is the live-caller-facing half. A registered tenant absent from the
    shim is rejected outright by ``canonical_tenant_id`` — measured as six real
    names on 2026-09-05.
    """
    missing = {
        name: tenant_id
        for name, tenant_id in _registry().items()
        if LEGACY_TENANT_NAMES.get(name) != tenant_id
    }
    assert not missing, (
        "LEGACY_TENANT_NAMES disagrees with the licences registry for "
        f"{len(missing)} name(s) — every caller sending one takes a hard 400, or is "
        f"resolved to the WRONG tenant: {missing}. Add each to LEGACY_TENANT_NAMES with "
        "the uuid the registry holds."
    )


def test_every_shim_key_is_either_registered_or_a_declared_rename() -> None:
    """Direction 2: a key nothing vouches for must be DECLARED, not merely present.

    ⛔ The remedy is never "delete it". Removing a key 400s any live caller still
    sending that spelling, and age is not evidence of absence — the bar (set by
    licences #170) is a MEASURED zero in live traffic. An undeclared key is
    recorded in RENAMED_TENANT_ALIASES so a regeneration cannot drop it silently.
    """
    registry = _registry()
    undeclared = sorted(set(LEGACY_TENANT_NAMES) - set(registry) - set(RENAMED_TENANT_ALIASES))
    assert not undeclared, (
        f"{len(undeclared)} key(s) in LEGACY_TENANT_NAMES that the licences registry does "
        f"not hold and RENAMED_TENANT_ALIASES does not declare: {undeclared}. Do NOT delete "
        "them — that 400s any caller still sending the old spelling. Declare each in "
        "RENAMED_TENANT_ALIASES mapping it to the name the registry holds for the same uuid."
    )


def test_every_declared_rename_still_resolves_to_its_current_name() -> None:
    """A declared alias must point at a name the registry still holds, same uuid.

    Catches a rename-of-a-rename going stale: the alias survives, its target
    does not, and the old spelling silently starts resolving to nothing.
    ``platform-cp-3 -> george-dev-portal-realm -> aa-agents`` is a live chain of
    exactly that shape, which is why the assertion follows the map rather than
    requiring the target to be a registry name directly.
    """
    for retired, current in RENAMED_TENANT_ALIASES.items():
        assert retired in LEGACY_TENANT_NAMES, (
            f"alias {retired!r} is declared retired but is not a key of LEGACY_TENANT_NAMES, "
            "so the old spelling does not resolve at all."
        )
        assert current in LEGACY_TENANT_NAMES, (
            f"alias {retired!r} points at {current!r}, which LEGACY_TENANT_NAMES does not "
            "hold — the rename chain is broken."
        )
        assert LEGACY_TENANT_NAMES[retired] == LEGACY_TENANT_NAMES[current], (
            f"alias {retired!r} resolves to {LEGACY_TENANT_NAMES[retired]} but its current "
            f"name {current!r} resolves to {LEGACY_TENANT_NAMES[current]}. Two spellings of "
            "ONE tenant must be two doors onto the same uuid; these are two tenants."
        )


def test_documented_counts_match_reality() -> None:
    """The docstring arithmetic is asserted, so a stale comment is a red test.

    ``LEGACY_TENANT_NAMES``'s docstring claimed "all 72 registered names" while
    the map held 75 keys. Nobody noticed, because prose is not executable. It is
    now.
    """
    registry = _registry()
    assert len(LEGACY_TENANT_NAMES) == DOCUMENTED_SHIM_KEYS
    assert len(registry) == DOCUMENTED_REGISTRY_NAMES
    assert len(RENAMED_TENANT_ALIASES) == DOCUMENTED_RETIRED_SPELLINGS
    assert DOCUMENTED_SHIM_KEYS == DOCUMENTED_REGISTRY_NAMES + DOCUMENTED_RETIRED_SPELLINGS, (
        "the documented decomposition does not add up"
    )


@pytest.mark.parametrize("name", sorted(_registry()))
def test_canonical_tenant_id_resolves_each_registered_name(name: str) -> None:
    """End to end through the real resolver, not just the dict.

    ``canonical_tenant_id`` parses a uuid BEFORE consulting the map, so a
    registry row whose NAME is itself a uuid string would be unreachable through
    the shim however correct the dict is. Asserting through the public function
    is what makes that observable rather than assumed.
    """
    expected = _registry()[name]
    resolved = canonical_tenant_id(name)
    assert resolved == expected, (
        f"canonical_tenant_id({name!r}) returned {resolved!r}, registry says {expected!r}"
    )


def test_default_anon_is_the_nil_tenant() -> None:
    """The registry's own answer for the unattributed bucket, not an invention."""
    assert _registry()["default-anon"] == NIL_TENANT_ID
    assert LEGACY_TENANT_NAMES["default-anon"] == NIL_TENANT_ID


# ---------------------------------------------------------------------------
# The shim is now a FALLBACK, and that is what this section pins
# ---------------------------------------------------------------------------
#
# Everything above compares the checked-in snapshot to a checked-in fixture and
# is unchanged: drift between them is still a red test, and the snapshot is
# still what answers during a licences outage. What CANNOT be asserted above is
# the thing ``docs/UNREGISTERED_TENANT_GATE.md`` precondition 2 actually asks
# for — that a registered tenant the snapshot has never heard of is nonetheless
# vouched for, because the LIVE registry is consulted first.
#
# Without this, a green file above is compatible with a service that reads the
# hand-pasted map and nothing else, which is precisely the state the gate must
# not be armed in.


def test_a_registered_tenant_the_shim_does_not_hold_is_still_registered_live() -> None:
    """The six missing names of 2026-09-05, in the general case.

    Uses a synthetic id: any real one is a candidate for the next regeneration
    of the snapshot, which would silently turn "absent from the shim" into
    "present" and make this assert nothing.
    """
    absent_from_shim = "01a0aaaa-0000-7000-8000-0000000000b7"
    assert absent_from_shim not in set(LEGACY_TENANT_NAMES.values())

    reset_live_tenant_registry()
    try:
        assert is_registered_tenant_id(absent_from_shim) is False
        set_live_tenant_registry({"registered-after-the-last-paste": absent_from_shim})
        assert is_registered_tenant_id(absent_from_shim) is True
        assert registration_source(absent_from_shim) == "live"
    finally:
        reset_live_tenant_registry()


def test_with_no_live_registry_every_shim_id_still_answers_registered() -> None:
    """FAIL SAFE. The snapshot is the outage answer, not a dead file.

    Reported as ``snapshot_degraded`` — the fallback is nameable in production,
    so "the live path is working" is a measurement rather than an assumption.

    A SENTINEL is the one shim id this does not apply to: ``"system"`` is a
    placeholder an emitter sends when it has no tenant to name, so it resolves
    (or the event would be dead-lettered and its money destroyed) while never
    being vouched for (or it bills as a tenant nobody owns). It must answer
    "not registered" during an outage too — see
    ``tests/test_system_sentinel_parked.py``.
    """
    from tenancy import SENTINEL_TENANT_IDS

    reset_live_tenant_registry()
    try:
        for name, tenant_id in _registry().items():
            if tenant_id in SENTINEL_TENANT_IDS:
                assert is_registered_tenant_id(tenant_id) is False, name
                assert registration_source(tenant_id) == "sentinel", name
                continue
            assert is_registered_tenant_id(tenant_id) is True, name
            assert registration_source(tenant_id) == "snapshot_degraded", name
    finally:
        reset_live_tenant_registry()
