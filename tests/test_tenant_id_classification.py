"""tenancy.classify_tenant_id + canonical_tenant_id's R15 observe/enforce guard.

THE MEASUREMENT THIS BUILDS ON: the original ask was "backfill the 1,536
divergent tenant_id rows to canonical". Measured 2026-09-16 that this is
IMPOSSIBLE (no canonical value exists to backfill TO — see
``alembic/versions/0039_tenant_id_divergence.py`` for the full evidence), so
this module instead CLASSIFIES what kind of unregistered id each is and
counts it, behind a write-path guard that defaults to observe-only and can be
armed to reject.

⛔ THE DEFAULT MUST STAY OBSERVE-ONLY. Every test in the "default (disarmed)"
section below asserts BYTE-IDENTICAL behaviour to before this classifier
existed: canonical_tenant_id still returns the id for every well-formed uuid,
registered or not. Only the explicit "armed" fixture flips that.
"""

from __future__ import annotations

import uuid

import pytest
from prometheus_client import REGISTRY

import tenancy
from tenancy import (
    LEGACY_TENANT_NAMES,
    NIL_TENANT_ID,
    SYNTHETIC_TENANT_IDS,
    TENANT_ID_CLASSIFICATIONS,
    TENANT_ID_DIVERGENCE_CLASSES,
    canonical_tenant_id,
    classify_tenant_id,
    derived_tenant_id,
)

#: A registered tenant, taken from the shim rather than invented — same
#: control tests/test_unregistered_tenant_exclusion.py uses.
REGISTERED_TENANT = LEGACY_TENANT_NAMES["fr-test"]


def _counter(name: str, classification: str) -> float:
    return float(REGISTRY.get_sample_value(name, {"classification": classification}) or 0.0)


def _v7_uuid() -> str:
    """A syntactically-v7 uuid that is not in SYNTHETIC_TENANT_IDS and not registered."""
    return str(uuid.UUID(bytes=uuid.uuid4().bytes, version=7))


def _v1_uuid() -> str:
    """A uuid version this classifier does not name — exercises the 'other' rung."""
    return str(uuid.uuid1())


@pytest.fixture
def enforced(monkeypatch):
    """Arm ``billing_tenant_id_classification_enforce`` for one test.

    Same idiom as ``tests/test_unregistered_tenant_exclusion.py``'s ``armed``
    fixture: patch the cached ``Settings`` instance (``tenancy._tenant_id_
    classification_enforced`` reads ``get_settings()`` fresh on every call, so
    patching the instance's attribute is enough — no lru_cache to clear on
    this path since ``tenancy`` never caches the settings object itself).
    """
    from settings import get_settings

    get_settings.cache_clear()
    settings = get_settings()
    monkeypatch.setattr(settings, "billing_tenant_id_classification_enforce", True)
    monkeypatch.setattr("settings.get_settings", lambda: settings)
    yield settings
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# classify_tenant_id — the pure predicate
# ---------------------------------------------------------------------------


def test_registered_wins_over_every_other_rung() -> None:
    """A registered id is 'registered', whatever its bit pattern looks like.

    fr-test's uuid is v5-shaped (derived_tenant_id's own namespace) -- if
    precedence were wrong this would read 'v5_derived' instead.
    """
    assert classify_tenant_id(REGISTERED_TENANT) == "registered"


def test_nil_tenant_is_registered() -> None:
    assert classify_tenant_id(NIL_TENANT_ID) == "registered"


@pytest.mark.parametrize("synthetic_id", sorted(SYNTHETIC_TENANT_IDS))
def test_every_synthetic_id_classifies_as_synthetic(synthetic_id: str) -> None:
    """All 6 R15-measured fixture values, by name, not just one representative."""
    assert classify_tenant_id(synthetic_id) == "synthetic"


def test_synthetic_wins_over_a_syntactic_v7_match() -> None:
    """0195f48b-dead-...-beef parses as a real v7 uuid. Synthetic must still win."""
    v7_shaped_synthetic = "0195f48b-dead-7000-8000-00000000beef"  # pragma: allowlist secret
    assert uuid.UUID(v7_shaped_synthetic).version == 7
    assert classify_tenant_id(v7_shaped_synthetic) == "synthetic"


def test_synthetic_wins_over_a_syntactic_v4_match() -> None:
    """0000abcd-...-aa parses as a real v4 uuid. Synthetic must still win."""
    v4_shaped_synthetic = "0000abcd-0000-4000-8000-0000000000aa"  # pragma: allowlist secret
    assert uuid.UUID(v4_shaped_synthetic).version == 4
    assert classify_tenant_id(v4_shaped_synthetic) == "synthetic"


def test_unregistered_v5_derived_classifies_as_v5_derived() -> None:
    """derived_tenant_id for a label NO registry entry owns — genuinely divergent."""
    unowned = derived_tenant_id("r15-estate-repair-test-label-nobody-owns")
    assert unowned not in tenancy._REGISTERED_TENANT_IDS
    assert classify_tenant_id(unowned) == "v5_derived"


def test_unregistered_v7_classifies_as_v7() -> None:
    assert classify_tenant_id(_v7_uuid()) == "v7"


def test_unregistered_v4_classifies_as_v4() -> None:
    assert classify_tenant_id(str(uuid.uuid4())) == "v4"


def test_a_version_this_classifier_does_not_name_is_other() -> None:
    assert classify_tenant_id(_v1_uuid()) == "other"


def test_classification_vocabulary_matches_the_divergence_subset() -> None:
    """TENANT_ID_DIVERGENCE_CLASSES is exactly TENANT_ID_CLASSIFICATIONS minus 'registered'.

    Pins the relationship the write-path guard and the two gauge pre-seed
    loops in observability/metrics.py both depend on.
    """
    assert TENANT_ID_CLASSIFICATIONS[0] == "registered"
    assert TENANT_ID_CLASSIFICATIONS[1:] == TENANT_ID_DIVERGENCE_CLASSES
    assert "registered" not in TENANT_ID_DIVERGENCE_CLASSES


# ---------------------------------------------------------------------------
# canonical_tenant_id — DEFAULT (disarmed): observe only, byte-identical
# ---------------------------------------------------------------------------


def test_default_is_disarmed() -> None:
    from settings import Settings

    assert Settings().billing_tenant_id_classification_enforce is False


def test_disarmed_synthetic_id_still_resolves() -> None:
    synthetic_id = next(iter(SYNTHETIC_TENANT_IDS))
    assert canonical_tenant_id(synthetic_id) == synthetic_id


def test_disarmed_unregistered_v4_still_resolves() -> None:
    raw = str(uuid.uuid4())
    assert canonical_tenant_id(raw) == raw


def test_disarmed_registered_id_still_resolves() -> None:
    assert canonical_tenant_id(REGISTERED_TENANT) == REGISTERED_TENANT


def test_disarmed_counts_the_classification_without_rejecting() -> None:
    """The observe-mode counter moves; the rejected-mode counter does NOT."""
    raw = str(uuid.uuid4())
    before = _counter("billing_tenant_id_classification_total", "v4")
    before_rejected = _counter("billing_tenant_id_classification_rejected_total", "v4")

    result = canonical_tenant_id(raw)

    assert result == raw
    assert _counter("billing_tenant_id_classification_total", "v4") == before + 1
    assert _counter("billing_tenant_id_classification_rejected_total", "v4") == before_rejected


# ---------------------------------------------------------------------------
# canonical_tenant_id — ARMED: reject anything not 'registered'
# ---------------------------------------------------------------------------


def test_armed_synthetic_id_is_rejected(enforced) -> None:
    synthetic_id = next(iter(SYNTHETIC_TENANT_IDS))
    assert canonical_tenant_id(synthetic_id) is None


def test_armed_unregistered_v4_is_rejected(enforced) -> None:
    assert canonical_tenant_id(str(uuid.uuid4())) is None


def test_armed_unregistered_v7_is_rejected(enforced) -> None:
    assert canonical_tenant_id(_v7_uuid()) is None


def test_armed_unregistered_v5_derived_is_rejected(enforced) -> None:
    unowned = derived_tenant_id("r15-estate-repair-test-label-nobody-owns-2")
    assert canonical_tenant_id(unowned) is None


def test_armed_registered_id_still_resolves(enforced) -> None:
    """The gate must never refuse a real tenant — only the unregistered rungs."""
    assert canonical_tenant_id(REGISTERED_TENANT) == REGISTERED_TENANT
    assert canonical_tenant_id(NIL_TENANT_ID) == NIL_TENANT_ID


def test_armed_counts_the_rejection(enforced) -> None:
    raw = str(uuid.uuid4())
    before_total = _counter("billing_tenant_id_classification_total", "v4")
    before_rejected = _counter("billing_tenant_id_classification_rejected_total", "v4")

    result = canonical_tenant_id(raw)

    assert result is None
    assert _counter("billing_tenant_id_classification_total", "v4") == before_total + 1
    assert _counter("billing_tenant_id_classification_rejected_total", "v4") == before_rejected + 1


def test_armed_never_counts_a_registered_rejection(enforced) -> None:
    """'registered' can never appear on the rejected counter, by construction."""
    before = _counter("billing_tenant_id_classification_rejected_total", "registered")
    canonical_tenant_id(REGISTERED_TENANT)
    assert _counter("billing_tenant_id_classification_rejected_total", "registered") == before


# ---------------------------------------------------------------------------
# Instrument pre-seeding — "never happened" must not read like "not wired"
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("classification", TENANT_ID_CLASSIFICATIONS)
def test_classification_total_is_preseeded(classification: str) -> None:
    """Every member of TENANT_ID_CLASSIFICATIONS is a real series from import time."""
    assert (
        REGISTRY.get_sample_value(
            "billing_tenant_id_classification_total", {"classification": classification}
        )
        is not None
    )


@pytest.mark.parametrize("classification", TENANT_ID_DIVERGENCE_CLASSES)
def test_rejected_total_is_preseeded(classification: str) -> None:
    assert (
        REGISTRY.get_sample_value(
            "billing_tenant_id_classification_rejected_total", {"classification": classification}
        )
        is not None
    )


@pytest.mark.parametrize("classification", TENANT_ID_DIVERGENCE_CLASSES)
def test_divergence_gauges_are_preseeded(classification: str) -> None:
    assert (
        REGISTRY.get_sample_value(
            "billing_tenant_id_divergence_rows", {"classification": classification}
        )
        is not None
    )
    assert (
        REGISTRY.get_sample_value(
            "billing_tenant_id_divergence_spend_usd", {"classification": classification}
        )
        is not None
    )
