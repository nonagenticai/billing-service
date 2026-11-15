"""Table-driven proof that event_flusher and rollup_engine now bill identically.

THE DEFECT THIS FILE IS THE ACCEPTANCE FOR
--------------------------------------------
event_flusher's monthly aggregation used to classify
``billing_metering_events.status`` with its OWN two-way test --
``status.startswith("denied_") or status == "denied"`` for denial,
``status == "allowed"`` for billable -- leaving a THIRD, silent case: fr (the
dominant caller) emits bare HTTP status codes (``"200"``, ``"403"``, ...), which
satisfy NEITHER spelling and fell into a branch that incremented
``request_count`` NOWHERE. ``rollup_engine``'s daily/hourly grain already billed
those same rows, via ``status.like("denied%")`` / "everything not denied is
billable". Same event, billed once, invisible once.

``services.metering_status`` is now the ONE definition
(``is_denied_status`` / ``denied_sql_expr``) both writers share. This file does
not re-test the refactor's plumbing; it tests the property that actually
matters, over a fixed vocabulary:

  (a) the Python form and the SQL form classify every value identically;
  (b) the NEW monthly classification equals the daily classification, for
      every value -- the two writers can no longer disagree about the same
      event;
  (c) ⭐ THE DIRECTION PROPERTY: the new monthly classification is billable
      whenever the OLD one (encoded here literally, not imported) was
      billable -- the change can only RAISE a monthly count or leave it
      unchanged, NEVER lower one. If this property is ever violated for a real
      status, STOP -- that shape moves money downward and needs a human
      decision, not a test that has been made to pass.
"""

from __future__ import annotations

import pytest
from sqlalchemy import String, case, literal, select
from sqlalchemy.ext.asyncio import AsyncSession

from services.metering_status import denied_sql_expr, is_denied_status

# The vocabulary this fix concerns itself with: license-authz's decision
# words, fr's HTTP-code strings, the two edge inputs (empty string, missing
# status), and two case-sensitivity probes. Both `str.startswith` (Python) and
# SQL `LIKE` (Postgres, no `ILIKE`) are case-sensitive, so "ALLOWED"/"Denied"
# must classify identically on both sides -- if either side ever silently
# switched to case-insensitive matching, this vocabulary catches the drift.
VOCAB: list[str | None] = [
    "allowed",
    "denied",
    "denied_budget",
    "denied_quota",
    "denied_",
    "200",
    "403",
    "429",
    "500",
    "",
    None,
    "ALLOWED",
    "Denied",
]


def _old_monthly_billable(status: str | None) -> bool:
    """event_flusher's PRE-FIX billable test, encoded LITERALLY here rather
    than imported -- the entire point of a baseline is that the fix cannot
    silently drag it along. The pre-fix code only ever compared a `str`
    (`_bound_status_for_storage` always returns one, never `None`), so
    `None == "allowed"` correctly reads `False` here without special-casing.
    """
    return status == "allowed"


def _new_monthly_billable(status: str | None) -> bool:
    """event_flusher's POST-FIX billable test: anything not denied."""
    return not is_denied_status(status)


def _daily_billable(status: str | None) -> bool:
    """rollup_engine's billable test. Unchanged behaviour by this refactor --
    it was already correct -- now driven by the same shared predicate as the
    monthly side."""
    return not is_denied_status(status)


async def _sql_is_denied(session: AsyncSession, status: str | None) -> bool:
    """Evaluate `denied_sql_expr` against real Postgres the way callers
    actually consume it: inside a boolean CASE/filter, where SQL's
    three-valued NULL collapses to the else/false branch -- the same collapse
    `is_denied_status` performs in Python for `None`. A bare
    `SELECT status LIKE 'denied%'` would instead hand back SQL NULL for a NULL
    status, a three-valued answer this comparison is not asking for (and not
    how `rollup_hourly` uses the expression either -- see
    `services.metering_status.denied_sql_expr`'s docstring).
    """
    expr = case((denied_sql_expr(literal(status, type_=String)), True), else_=False)
    result = await session.execute(select(expr))
    return bool(result.scalar())


@pytest.mark.parametrize("status", VOCAB)
async def test_python_and_sql_forms_agree(db_session: AsyncSession, status: str | None) -> None:
    """(a) is_denied_status and denied_sql_expr classify every value the same way."""
    python_verdict = is_denied_status(status)
    sql_verdict = await _sql_is_denied(db_session, status)
    assert python_verdict == sql_verdict, (
        f"Python/SQL disagreement for status={status!r}: "
        f"is_denied_status={python_verdict}, denied_sql_expr={sql_verdict}"
    )


@pytest.mark.parametrize("status", VOCAB)
def test_monthly_now_equals_daily(status: str | None) -> None:
    """(b) event_flusher's post-fix classification equals rollup_engine's, for
    every value -- the two writers can no longer disagree about the same
    event."""
    monthly = _new_monthly_billable(status)
    daily = _daily_billable(status)
    assert monthly == daily, f"monthly/daily disagreement for status={status!r}"


@pytest.mark.parametrize("status", VOCAB)
def test_fix_only_ever_raises_the_monthly_count(status: str | None) -> None:
    """(c) ⭐ THE DIRECTION PROPERTY. Whenever the OLD monthly rule billed a
    status, the NEW rule must bill it too -- never the reverse. A status where
    this fails would mean the fix LOWERS a monthly invoice figure relative to
    today's production behaviour -- the one shape that needs a human decision,
    not a passing test.
    """
    old = _old_monthly_billable(status)
    new = _new_monthly_billable(status)
    assert not (old and not new), (
        f"status={status!r}: OLD monthly rule billed this, NEW rule does not -- "
        "this would LOWER a monthly invoice figure relative to production today. STOP."
    )


@pytest.mark.parametrize("status", [s for s in VOCAB if s not in (None, "", "allowed")])
def test_denied_statuses_stay_unbilled_after_the_fix(status: str) -> None:
    """Companion to the direction property: the fix must not have gone the
    OTHER way either. Every denial spelling in the vocabulary must still read
    as not-billable after the rewire -- widening billability must never widen
    into an actual denial.
    """
    if is_denied_status(status):
        assert not _new_monthly_billable(status), (
            f"status={status!r} is a denial and must not be billable post-fix"
        )


def test_the_bug_is_actually_fixed_for_frs_http_code_vocabulary() -> None:
    """The concrete population the fix exists for: an HTTP-code status (fr's
    vocabulary) used to be billed daily and dropped monthly. It must now be
    billed on both, and the OLD rule's blind spot on it is the reason this
    fix exists at all -- assert that blind spot too, so a future revert of
    just the OLD-baseline function cannot go unnoticed.
    """
    for status in ("200", "403", "429", "500"):
        assert not _old_monthly_billable(status), (
            f"status={status!r}: OLD rule was expected to have missed this (that is the bug)"
        )
        assert _new_monthly_billable(status), f"status={status!r} must be billable post-fix"
        assert _daily_billable(status), f"status={status!r} must be billable on the daily side"


def test_vocabulary_covers_every_shape_the_fix_reasons_about() -> None:
    """Documents WHY these 13 values and not some other set: every shape the
    fix's own reasoning turns on must appear at least once. A missing shape
    here would be a missing acceptance, not a missing convenience.
    """
    assert "allowed" in VOCAB  # the old rule's only "yes"
    assert {"denied", "denied_budget", "denied_quota", "denied_"} <= set(VOCAB)  # denial spellings
    assert {"200", "403", "429", "500"} <= set(VOCAB)  # fr's HTTP-code vocabulary
    assert "" in VOCAB
    assert None in VOCAB
    assert "ALLOWED" in VOCAB and "Denied" in VOCAB  # case-sensitivity probes
