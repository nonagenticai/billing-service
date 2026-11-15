"""Single source of truth for "is this metering event billable?"

WHY THIS EXISTS
----------------
Two writers each turned ``billing_metering_events.status`` into a
billed-or-denied verdict, INDEPENDENTLY:

* ``event_flusher`` (monthly aggregation, :data:`MeteringMonthlyUsage`) used to
  test ``status.startswith("denied_") or status == "denied"`` for denial and
  ``status == "allowed"`` for billability — leaving a THIRD case, neither
  spelling, that incremented nothing at all.
* ``rollup_engine`` (hourly/daily aggregation, :data:`MeteringHourly` /
  :data:`MeteringDaily`) tests ``status.like("denied%")`` for denial and bills
  everything else — deliberately, because it already knew the two-writer shape
  documented below.

``status`` has two writers with two different vocabularies. license-authz
writes decisions: ``allowed``, ``denied_invalid_token``, ``denied_app_mismatch``,
etc. fr (the dominant caller, ``fr/backend/src/services/metering-emit.ts``)
writes bare HTTP status codes as strings: ``"200"``, ``"403"``, ``"429"``,
``"500"``. An HTTP code satisfies NEITHER of event_flusher's two spellings, so
it fell into a silent third branch that billed nothing on the monthly side
while ``rollup_engine`` billed it on the daily side as an ordinary request —
the same event, billed by one writer and dropped by the other.

THE RULE, taken from ``rollup_engine`` because that is the writer that already
billed correctly: **an event is DENIED iff its status starts with
``"denied"``; everything else — every HTTP status code, the empty string, and
NULL/None — is BILLABLE.** There is no third case.

Exposed in two forms so both writers can share the identical rule instead of
each re-deriving it:

* :func:`is_denied_status` — plain Python, for event_flusher's in-process loop
  over a decoded ``str | None``.
* :func:`denied_sql_expr` — the same rule as a SQLAlchemy boolean expression,
  for rollup_engine's aggregate queries over the ``MeteringEvent.status``
  column.

Keeping exactly one ``"denied"``-prefix literal (here) is the point: before
this module the literal existed twice, in two different spellings
(``startswith("denied_") or == "denied"`` vs ``like("denied%")``), and only
one of the two spellings matched fr's traffic correctly.

WHAT THIS DOES NOT COVER
-------------------------
This module answers ONLY "denied vs billable". It says nothing about which
denials are budget denials (``status in ("denied_budget", "denied_quota")``,
tracked separately by each caller) and nothing about settlement arithmetic,
rates, or the reservation/budget protocol.
"""

from __future__ import annotations

from typing import Any

#: The one denial-prefix literal. Both forms below derive from this constant
#: so a future change to the vocabulary (e.g. a new denial reason) only ever
#: needs to be made here.
_DENIED_PREFIX = "denied"


def is_denied_status(status: str | None) -> bool:
    """True iff ``status`` denotes a denied request; False iff it is billable.

    ``None`` and ``""`` are BILLABLE (not denied) — same as any status that
    does not start with ``"denied"``, including every HTTP status code fr
    emits. This is the plain-Python mirror of :func:`denied_sql_expr`; the two
    must never be allowed to diverge; see
    ``tests/test_metering_status_predicate.py`` for the table-driven proof
    that they agree on every value in the known vocabulary.
    """
    return bool(status) and status.startswith(_DENIED_PREFIX)


def denied_sql_expr(status_column: Any) -> Any:
    """SQLAlchemy boolean expression form of :func:`is_denied_status`.

    Pass ``MeteringEvent.status`` (or any column/expression of the same
    shape). Returns the ``LIKE 'denied%'`` predicate that both the billed
    ``request_count`` (``NOT denied_sql_expr(...)``) and ``denied_count``
    (``denied_sql_expr(...)``) are built from, so the two can never disagree
    about what "denied" means within one query.

    A SQL ``LIKE`` on a NULL column evaluates to NULL (neither true nor
    false), which is why callers that need the billable count as an exact
    complement build it via ``count() - count().filter(denied_sql_expr(...))``
    rather than ``count().filter(~denied_sql_expr(...))`` — the same
    NULL-safety reasoning already documented at the call site in
    ``rollup_engine.rollup_hourly``.
    """
    return status_column.like(f"{_DENIED_PREFIX}%")
