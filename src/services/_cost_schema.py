"""Minimum-billable-schema validation for inbound cost-stream events.

Internal to the services package — the leading underscore signals not-for-import
outside services/ (same convention as :mod:`services._parsers`).

A malformed event must never be silently billed at $0. The lenient
``_parse_int`` / ``_parse_decimal`` helpers coerce garbage to ``0`` /
``Decimal("0")`` so a corrupt token count or cost silently becomes a free row
with no trace. :func:`validate_cost_event` returns a short machine-readable
reason when an event fails the minimum billable schema; the flusher routes
those to the dead-letter stream instead of inserting a fail-open row.

Boundary (verified against 30d of prod traffic, 2026-07-23):

* ``provider`` is REQUIRED — it is the anchor of every pricing-lookup rung, so
  a provider-less event cannot be priced or repriced. (0 prod events lack it.)
* ``model`` is NOT required — ~41% of prod events legitimately carry no model
  and price via the provider-default / per-request rung. Validating it would
  quarantine real traffic, so a missing model is explicitly allowed.
* numeric fields, when present and non-empty, MUST parse to a non-negative
  number. This is the actual fail-open-to-$0 guard: ``_parse_int("abc")`` → 0
  and ``int("-5")`` → -5 both slip through the lenient parsers today.
* ``tenant_id`` is REQUIRED and must resolve to a REAL tenant (see below).

tenant_id (CONTRACT CHANGE, 2026-08-26)
---------------------------------------

This module used to say tenant_id was "deliberately NOT validated — handled by
``_normalize_tenant_id`` (unattributed sentinel) and repriceable /
re-attributable, not malformed". That was measured wrong. The nil-tenant sink is
not a re-attribution queue, it is a write-only hole: nothing reads it back,
nothing re-attributes it, and ``cost_flusher`` deliberately skips the token
rollup and the budget debit for it. A cost that lands there is spend that is
never billed and never enforced against any budget — and it is not a historical
trickle. Measured on the live ``billing_cost_events`` table, 2026-08-26:

    22,226 nil-tenant rows / $353.0916, 2026-05-01 → 2026-08-26 (ongoing),
    against 441,575 real-tenant rows / $4150.8684 — ~4.8% of all volume.
    Monthly: 2026-05 = 19 · 2026-06 = 5,981 ($2.92)
             2026-07 = 11,530 ($232.61) · 2026-08 = 4,696 ($117.57)

So an unattributable tenant IS malformed, in exactly the sense this module
already uses for a provider: the event cannot be billed, and a silently-unbilled
row is the failure the dead-letter stream exists to make loud. Resolution is
delegated to :func:`tenancy.canonical_tenant_id` — the same function the flusher
and the HTTP boundary use — so the three ways to fail are:

* absent / blank                          → ``missing_tenant_id``
* unresolvable (a slug that is not a
  REGISTERED legacy name, or a leaked
  ``Bearer`` credential)                  → ``invalid_tenant_id``
* the nil uuid, i.e. the emitter's own
  "I have no tenant" fallback             → ``nil_tenant_id``

A REGISTERED legacy name ("n8n-dev") is NOT rejected: ``canonical_tenant_id``
resolves it to the uuid ``licenses.tenants`` holds for it, the flusher bills it
against that real tenant today, and quarantining it would throw away attributable
traffic. Only genuinely unattributable spend is quarantined.
"""

from decimal import Decimal, InvalidOperation
from typing import Any

from tenancy import NIL_TENANT_ID, canonical_tenant_id

# Token/latency fields that must be a non-negative integer when present.
_NON_NEGATIVE_INT_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
    "latency_ms",
)

# Every reason string :func:`validate_cost_event` can return, DERIVED from the
# field tuple above rather than hand-listed so adding a validated field cannot
# leave the set stale. This is the bounded label domain promised in that
# function's docstring, made explicit because something else now has to
# enumerate it: ``observability.metrics`` zero-initialises one
# ``billing_cost_dead_letter_total`` series per entry at import (see the note
# beside that Counter for why an absent series is a hazard).
COST_DEAD_LETTER_REASONS: tuple[str, ...] = (
    "missing_provider",
    "missing_tenant_id",
    "invalid_tenant_id",
    "nil_tenant_id",
    *(f"negative_{_f}" for _f in _NON_NEGATIVE_INT_FIELDS),
    *(f"invalid_{_f}" for _f in _NON_NEGATIVE_INT_FIELDS),
    "negative_cost_usd",
    "invalid_cost_usd",
)


def validate_cost_event(fields: dict[str, Any]) -> str | None:
    """Return a malformed-reason string, or ``None`` if the event is billable.

    The reason string is used as a low-cardinality Prometheus label and as the
    ``_dlq_reason`` on the dead-letter record, so keep the set bounded.
    """
    provider = fields.get("provider")
    if provider is None or str(provider).strip() == "":
        return "missing_provider"

    # An unattributable tenant is unbillable spend — quarantine it rather than
    # sink it to the nil tenant, where the flusher skips both the token rollup
    # and the budget debit and the cost is never billed to anyone. See the
    # module docstring for the live blast radius this closes.
    raw_tenant = fields.get("tenant_id")
    if raw_tenant is None or str(raw_tenant).strip() == "":
        return "missing_tenant_id"
    resolved_tenant = canonical_tenant_id(raw_tenant)
    if resolved_tenant is None:
        return "invalid_tenant_id"
    if resolved_tenant == NIL_TENANT_ID:
        return "nil_tenant_id"

    for key in _NON_NEGATIVE_INT_FIELDS:
        raw = fields.get(key)
        if raw is None or raw == "":
            continue
        try:
            if int(raw) < 0:
                return f"negative_{key}"
        except (ValueError, TypeError):  # fmt: skip
            return f"invalid_{key}"

    raw_cost = fields.get("cost_usd")
    if raw_cost is not None and raw_cost != "":
        try:
            if Decimal(raw_cost) < 0:
                return "negative_cost_usd"
        except (InvalidOperation, TypeError, ValueError):  # fmt: skip
            return "invalid_cost_usd"

    return None
