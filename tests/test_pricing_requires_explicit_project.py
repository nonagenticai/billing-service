"""`project_id` must be REQUIRED and KEYWORD-ONLY on every public pricing entry
point — omitting it must be a loud `TypeError`, never a quiet wrong price.

⭐ WHY THIS FILE EXISTS. The per-project markup axis (0036) shipped `project_id`
as a DEFAULTED keyword on every pricing entry point in `services/markup.py`. A
caller that forgot it did not fail: it silently resolved at the TENANT-WIDE
rate, with no error, no log and no counter — just a wrong number on an invoice.
The omission is a *valid call*, so nothing in CI could see it.

FOUR of the six call sites shipped exactly that bug at least once:

  - ``services/billing.py``          (original 0036 commit)
  - ``routers/billing.py``           (original 0036 commit)
  - ``routers/usage.py``             (original 0036 commit)
  - ``services/admin_aggregate.py``  (landed mid-sweep with ``proj_uuid``
                                      computed and never passed — caught only
                                      incidentally, by ruff F841)

This is therefore not a test of one defect but of the CLASS: the seventh caller
would have failed identically. Removing the default converts the whole class
from a silent wrong price into an immediate `TypeError` at the call site.

⚠️ A caller that genuinely has no project MUST write ``project_id=None``
explicitly. That explicitness is the point — it forces a decision and leaves the
decision visible in the diff. ``services/billing.py``'s app-request line is the
precedent: it passes ``None`` because ``billing_metering_daily`` deliberately
has no project column (entitlement is per licence, not per project). Never
substitute a NIL/sentinel uuid: ``None`` is the "no project" value, and a
sentinel would match a project-pinned rule.

⚠️ EVERY negative here is paired with a POSITIVE CONTROL that makes the same
call WITH ``project_id=None``. Without the control a `TypeError` would prove
nothing — a renamed function, a different missing argument, or an unrelated
signature change all raise `TypeError` too, and the file would pass green while
testing nothing. The control is what pins the `TypeError` to *this* parameter.

This file needs NO DATABASE: a missing keyword-only argument fails at argument
binding, before the function body (or, for a coroutine function, before the
coroutine object is even created).
"""

from __future__ import annotations

import inspect
from datetime import datetime
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from services.markup import (
    BILLING_MODE_STANDARD,
    BilledLine,
    MarkupResolver,
    compute_billed_line,
    compute_billed_line_detailed,
    resolve_markup_rule,
)
from tests.conftest import T

#: The public pricing entry points. Omitting ``project_id`` on any of these
#: changes the PRICE, which is why each one is required-with-no-default.
#: ``list_markup_history`` is deliberately NOT here — it is a read-only history
#: filter, where omitting the dimension widens the result set rather than
#: mispricing anything.
ENTRY_POINTS = [
    ("compute_billed_line", compute_billed_line),
    ("compute_billed_line_detailed", compute_billed_line_detailed),
    ("MarkupResolver.resolve", MarkupResolver.resolve),
    ("resolve_markup_rule", resolve_markup_rule),
]


# --------------------------------------------------------------------------
# The signature contract, read straight off the function objects.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("name,fn", ENTRY_POINTS, ids=[n for n, _ in ENTRY_POINTS])
def test_project_id_is_keyword_only_with_no_default(name, fn):
    """``project_id`` is KEYWORD_ONLY (no accidental positional) and has NO
    default (no silent tenant-wide fallback).

    This is the assertion that goes red if someone "tidies" ``= None`` back in.
    """
    param = inspect.signature(fn).parameters.get("project_id")
    assert param is not None, f"{name} lost its project_id parameter entirely"
    assert param.kind is inspect.Parameter.KEYWORD_ONLY, (
        f"{name}.project_id is {param.kind}, not KEYWORD_ONLY — a positional "
        f"slot lets a caller pass it by accident in the wrong position"
    )
    assert param.default is inspect.Parameter.empty, (
        f"{name}.project_id has default {param.default!r}. It must have NO "
        f"default: a defaulted project_id prices silently at the tenant-wide "
        f"rate when a caller forgets it. Four call sites shipped that bug."
    )


# --------------------------------------------------------------------------
# Negative: omitting project_id raises. Each is paired with a positive control.
# --------------------------------------------------------------------------


def _resolver() -> MarkupResolver:
    """An empty resolver — resolves to a miss, which is fine: these tests are
    about argument binding, not about which rule wins."""
    return MarkupResolver([])


def test_compute_billed_line_without_project_id_raises():
    with pytest.raises(TypeError, match="project_id"):
        compute_billed_line(  # ty: ignore[missing-argument]  # deliberate: the omission under test
            _resolver(),
            BILLING_MODE_STANDARD,
            tenant_id=T("t1"),
            incurred_usd=Decimal("10"),
            fallback_pct=20.0,
        )


def test_compute_billed_line_with_explicit_none_is_accepted():
    """POSITIVE CONTROL — the identical call with an explicit ``None`` prices
    normally, proving the TypeError above is about project_id and nothing else."""
    billed, pct, rule = compute_billed_line(
        _resolver(),
        BILLING_MODE_STANDARD,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=20.0,
        project_id=None,
    )
    assert billed == Decimal("12.000000")
    assert pct == 20.0
    assert rule is None


def test_compute_billed_line_detailed_without_project_id_raises():
    with pytest.raises(TypeError, match="project_id"):
        compute_billed_line_detailed(  # ty: ignore[missing-argument]  # deliberate: the omission under test
            _resolver(),
            BILLING_MODE_STANDARD,
            tenant_id=T("t1"),
            incurred_usd=Decimal("10"),
            fallback_pct=20.0,
        )


def test_compute_billed_line_detailed_with_explicit_none_is_accepted():
    """POSITIVE CONTROL for :func:`compute_billed_line_detailed`."""
    line = compute_billed_line_detailed(
        _resolver(),
        BILLING_MODE_STANDARD,
        tenant_id=T("t1"),
        incurred_usd=Decimal("10"),
        fallback_pct=20.0,
        project_id=None,
    )
    assert isinstance(line, BilledLine)
    assert line.billed_usd == Decimal("12.000000")


def test_markup_resolver_resolve_without_project_id_raises():
    with pytest.raises(TypeError, match="project_id"):
        _resolver().resolve(tenant_id=T("t1"))  # ty: ignore[missing-argument]  # deliberate: the omission under test


def test_markup_resolver_resolve_with_explicit_none_is_accepted():
    """POSITIVE CONTROL for :meth:`MarkupResolver.resolve`."""
    assert _resolver().resolve(tenant_id=T("t1"), project_id=None) is None


def test_resolve_markup_rule_without_project_id_raises():
    """⚠️ A coroutine FUNCTION binds its arguments when it is CALLED, so this
    raises without any await and without any database — no session is touched.
    """
    with pytest.raises(TypeError, match="project_id"):
        resolve_markup_rule(  # ty: ignore[missing-argument, unused-awaitable]  # deliberate omission
            cast(AsyncSession, None), tenant_id=T("t1"), ts=cast(datetime, None)
        )


def test_resolve_markup_rule_with_explicit_none_is_accepted():
    """POSITIVE CONTROL for :func:`resolve_markup_rule` — with the argument
    supplied, binding succeeds and a coroutine is produced (closed unawaited;
    the DB is never reached, which is why no session is needed)."""
    coro = resolve_markup_rule(
        cast(AsyncSession, None),
        tenant_id=T("t1"),
        project_id=None,
        ts=cast(datetime, None),
    )
    assert inspect.iscoroutine(coro)
    coro.close()
