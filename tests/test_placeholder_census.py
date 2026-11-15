"""A defaulted sentinel must be COUNTABLE, not laundered into an honest-looking value.

THE DEFECT
----------
A placeholder is indistinguishable from a measured value once it is in the row.
Measured on the prod billing primary 2026-09-05::

    service_key       | rows | metadata.job_id = 'unknown' | job_id IS NULL
    inference-router  |  968 |                         968 |              0
    mlx-backend       | 4510 |                           0 |           4510

968 of 968 — 100% — of inference-router cost events carry a fabricated job id,
and nothing anywhere said so, because ``'unknown'`` is stored verbatim and every
reader treats it as a job id.

WHAT THIS DOES NOT DO
---------------------
⛔ IT DOES NOT CHANGE THE COERCION. The sentinel is still written. Coercing it to
NULL would only move the laundering — from "a fake value" to "an honest-looking
absence" — and NULL is not even available for ``service_key`` / ``provider`` /
``cost_source``, which are NOT NULL since the 0001 baseline. The value is left
exactly as it was and COUNTED instead, on
``billing_placeholder_value_total{field, service_key}``.

WHY ABSENCE IS COUNTED TOO
--------------------------
A field no emitter ever sends would otherwise read as 0% placeholder — perfect
health — purely because it never appears at all. ``caller_client_id`` is exactly
that case: measured 2026-09-04, no emitter puts one on the wire, so its true
placeholder rate is 100% and a sentinel-only counter would report 0. Absence and
a sentinel are the same fact about the emitter, so they share a series.
"""

from __future__ import annotations

import pytest

import observability.metrics as om
from services._parsers import (
    COST_METADATA_WHITELIST,
    DEFAULTED_COST_FIELDS,
    PLACEHOLDER_COUNTED_FIELDS,
    RESOLVED_COST_FIELDS,
    is_placeholder,
)
from services.aggregator import flush_costs
from tenancy import LEGACY_TENANT_NAMES
from tests._aggregator_helpers import _make_cost

REGISTERED_TENANT = LEGACY_TENANT_NAMES["fr-test"]


def _counter(field: str, service_key: str) -> float:
    return om.placeholder_value_total.labels(field=field, service_key=service_key)._value.get()


# ---------------------------------------------------------------------------
# The predicate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value", ["unknown", "UNKNOWN", " Unknown ", "none", "null", "n/a", "-", None]
)
def test_placeholders_are_recognised(value) -> None:
    """Casing and padding must not buy honesty.

    An emitter writing "Unknown" is doing the same thing as one writing
    "unknown"; if only the exact lowercase spelling counted, the census would
    under-report by whatever fraction of emitters happen to capitalise.
    """
    assert is_placeholder(value)


@pytest.mark.parametrize("value", ["job-123", "0", "false", "inference-router", "a"])
def test_real_values_are_not_placeholders(value) -> None:
    """THE CONTROL. A predicate that always says True makes the census meaningless.

    ``"0"`` and ``"false"`` are here deliberately: they are falsy-looking but are
    real values, and a predicate written with ``if not val`` would wrongly flag
    them.
    """
    assert not is_placeholder(value)


@pytest.mark.parametrize("value", ["", "   ", "\t", None])
def test_emptiness_is_a_placeholder(value) -> None:
    """Absence and a sentinel word are one fact about the emitter.

    REGRESSION: an earlier version excluded "" on the theory that the flusher's
    truthiness tests already caught it. That is true of the metadata whitelist
    and FALSE of the defaulted columns, whose whole coercion is ``or "unknown"``
    — so an empty ``service_key``, the single most likely defaulted field, was
    silently never counted. Caught by
    ``test_a_defaulted_service_key_is_counted`` below, which is why that test
    passes a bare "".
    """
    assert is_placeholder(value)


def test_the_sentinel_set_holds_words_not_emptiness() -> None:
    """ "" is absence, not a spelling of "unknown".

    Kept out of the word set so the set reads as what a human actually typed;
    emptiness is handled by its own branch in :func:`is_placeholder`.
    """
    from services._parsers import PLACEHOLDER_VALUES

    assert "" not in PLACEHOLDER_VALUES
    assert all(v and v.strip() for v in PLACEHOLDER_VALUES)


async def test_an_empty_whitelisted_key_is_counted_exactly_once(
    db_session_factory,
) -> None:
    """The double-count this arrangement has to avoid.

    In the whitelist loop an empty value fails ``if c.get(k)`` and is counted in
    the ``else`` branch — it must not ALSO be counted by an ``is_placeholder``
    call on the same event.
    """
    before = _counter("job_id", "inference-router")
    await flush_costs(
        [_make_cost(tenant_id=REGISTERED_TENANT, job_id="")],
        db_session_factory,
        None,
    )
    assert _counter("job_id", "inference-router") - before == 1


def test_the_label_domain_is_derived_not_restated() -> None:
    """One list, not two.

    The counter's ``field`` label domain must come FROM the whitelist the flusher
    actually iterates, or a key added to the whitelist gets a series that is
    never born and never counted — invisible in exactly the way this instrument
    exists to prevent.
    """
    assert (
        PLACEHOLDER_COUNTED_FIELDS
        == COST_METADATA_WHITELIST + DEFAULTED_COST_FIELDS + RESOLVED_COST_FIELDS
    )
    assert "job_id" in COST_METADATA_WHITELIST
    assert "service_key" in DEFAULTED_COST_FIELDS
    # The third family: a field the flusher RESOLVES from the live tenant
    # registry, whose fallback is not a fixed "unknown" literal but the tenant
    # uuid echoed back — a value that looks populated and is not.
    assert "tenant_name" in RESOLVED_COST_FIELDS


def test_the_flusher_iterates_the_shared_whitelist() -> None:
    """Pins the single-definition property against a future inline re-listing.

    If someone pastes the tuple back inline in ``cost_flusher``, this goes red
    rather than the census silently going blind to the new key.
    """
    import inspect

    import services.cost_flusher as cf

    source = inspect.getsource(cf.flush_costs)
    assert "for k in COST_METADATA_WHITELIST:" in source, (
        "cost_flusher no longer iterates the shared whitelist; the placeholder "
        "census can now go blind to a newly whitelisted key"
    )


# ---------------------------------------------------------------------------
# The census, end to end through the flusher
# ---------------------------------------------------------------------------


async def test_a_sentinel_job_id_is_counted(db_session_factory) -> None:
    """THE MEASURED PRODUCTION CASE: metadata.job_id = 'unknown', 968/968 rows."""
    before = _counter("job_id", "inference-router")
    await flush_costs(
        [_make_cost(tenant_id=REGISTERED_TENANT, job_id="unknown")],
        db_session_factory,
        None,
    )
    assert _counter("job_id", "inference-router") - before == 1


async def test_the_sentinel_is_still_stored_verbatim(db_session_factory) -> None:
    """⛔ THE COERCION IS UNCHANGED. Counting it must not start rewriting it.

    Silently turning 'unknown' into NULL would move the laundering rather than
    end it, and would rewrite the meaning of 891+ existing rows' worth of
    incoming traffic without anyone deciding to.
    """
    from sqlalchemy import select

    from models.db import CostEvent

    await flush_costs(
        [_make_cost(tenant_id=REGISTERED_TENANT, job_id="unknown")],
        db_session_factory,
        None,
    )
    async with db_session_factory() as session:
        row = (await session.execute(select(CostEvent))).scalars().one()
    assert row.extra_metadata["job_id"] == "unknown"


async def test_a_real_job_id_is_not_counted(db_session_factory) -> None:
    """THE CONTROL GROUP. Without it the counter could fire unconditionally."""
    before = _counter("job_id", "inference-router")
    await flush_costs(
        [_make_cost(tenant_id=REGISTERED_TENANT, job_id="job-abc-123")],
        db_session_factory,
        None,
    )
    assert _counter("job_id", "inference-router") - before == 0


async def test_an_absent_field_is_counted_as_a_placeholder(db_session_factory) -> None:
    """``caller_client_id``: no emitter sends one, so its true rate is 100%.

    A sentinel-only counter would report 0 here — perfect health for a field
    that has never once carried a value.
    """
    before = _counter("caller_client_id", "inference-router")
    await flush_costs([_make_cost(tenant_id=REGISTERED_TENANT)], db_session_factory, None)
    assert _counter("caller_client_id", "inference-router") - before == 1


async def test_a_present_field_stops_being_counted(db_session_factory) -> None:
    """The other direction: fixing the emitter must drive the series DOWN."""
    before = _counter("caller_client_id", "inference-router")
    await flush_costs(
        [_make_cost(tenant_id=REGISTERED_TENANT, caller_client_id="svc-requirements")],
        db_session_factory,
        None,
    )
    assert _counter("caller_client_id", "inference-router") - before == 0


async def test_a_defaulted_service_key_is_counted(db_session_factory) -> None:
    """The flusher's own ``or "unknown"`` substitution, counted where it happens.

    Labelled with the same ``service_key`` expression the whitelist half uses —
    which for a missing service_key is itself "unknown" — so the two halves of
    the counter can never drift apart.
    """
    before = _counter("service_key", "unknown")
    await flush_costs(
        [_make_cost(tenant_id=REGISTERED_TENANT, service_key="")],
        db_session_factory,
        None,
    )
    assert _counter("service_key", "unknown") - before == 1


async def test_a_real_service_key_is_not_counted(db_session_factory) -> None:
    before = _counter("service_key", "inference-router")
    await flush_costs([_make_cost(tenant_id=REGISTERED_TENANT)], db_session_factory, None)
    assert _counter("service_key", "inference-router") - before == 0


async def test_the_census_labels_by_the_emitting_service(db_session_factory) -> None:
    """Without service_key the census names a gap but not who to talk to.

    And the rate is only readable per-service: 'unknown' on 3 of 40,000 rows is a
    hiccup, on 968 of 968 it is an emitter that has never populated the field.
    """
    before_a = _counter("job_id", "svc-a")
    before_b = _counter("job_id", "svc-b")
    await flush_costs(
        [
            _make_cost(tenant_id=REGISTERED_TENANT, service_key="svc-a", job_id="unknown"),
            _make_cost(tenant_id=REGISTERED_TENANT, service_key="svc-b", job_id="real-1"),
        ],
        db_session_factory,
        None,
    )
    assert _counter("job_id", "svc-a") - before_a == 1
    assert _counter("job_id", "svc-b") - before_b == 0
