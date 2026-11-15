"""One TokenBudget factory for the whole test suite.

WHY THIS EXISTS. Four test modules each carried their own near-identical
``_budget`` helper that built a row with a clock-relative ``starts_at`` and left
``consumed_period_start`` NULL. That is not merely duplication — it is a monthly
time bomb. ``services.budget_period.consumption_anchor`` resolves a NULL anchor
from ``starts_at``, so "a budget that started yesterday" means "consumption
belongs to yesterday's period". On the 1st of a month yesterday is the PREVIOUS
period, ``effective_consumed`` correctly returns 0, and every assertion about
consumed tokens, usage percentage or an overage crossing fails at once.

It fired on 2026-09-01: 16 tests red on ``main``, deploy skipped, a merged
feature stranded out of production. The service was right and the fixtures were
wrong. The trap was armed by #69 (2026-08-03), which made ``period``
load-bearing without revisiting fixtures written when it was decoration.

THE FIX is to say which period the seeded consumption belongs to instead of
letting it be inferred from ``starts_at``. ``starts_at`` stays clock-relative —
it is doing real work, the budget must have started — but consumption now
carries its own anchor, which is stable at EVERY clock position, including
00:00:00 on the 1st and on an ISO Monday.

``now`` is an injected seam so a test can ask what this factory produces at an
arbitrary instant without touching the wall clock. ``tests/test_budget_period.py``
uses it to hold the boundary shut on every calendar day of the year.

Pass ``consumed_period_start=None`` EXPLICITLY to get the legacy NULL-anchor
row — that is a real production shape (rows predating the column) and the
rollover tests in ``test_money_bug_fixes.py`` deliberately exercise it.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from models.db import NULL_LICENSE_ID, TokenBudget
from services.budget_period import period_anchor

# Sentinel: "derive the anchor from ``now``". Distinct from an explicit None,
# which means "leave it NULL and exercise the starts_at fallback".
_DERIVE: Any = object()


def make_budget(
    *,
    tenant: str,
    cap: int,
    consumed: int = 0,
    license_id: uuid.UUID | None = None,
    period: str = "monthly",
    source: str = "test",
    priority: int = 100,
    expires_at: datetime | None = None,
    starts_at: datetime | None = None,
    consumed_period_start: date | None = _DERIVE,
    now: datetime | None = None,
) -> TokenBudget:
    """Build an active TokenBudget whose consumption belongs to ``now``'s period.

    ``now`` defaults to the wall clock and is the ONLY clock this factory reads,
    so passing it makes the result fully deterministic.
    """
    now = now or datetime.now(UTC)
    if consumed_period_start is _DERIVE:
        consumed_period_start = period_anchor(period, now)
    return TokenBudget(
        tenant_id=tenant,
        license_id=license_id if license_id is not None else NULL_LICENSE_ID,
        budget_tokens=cap,
        consumed_tokens=Decimal(consumed),
        period=period,
        includes_input=True,
        includes_output=True,
        active=True,
        priority=priority,
        starts_at=starts_at if starts_at is not None else (now - timedelta(days=1)),
        expires_at=expires_at,
        consumed_period_start=consumed_period_start,
        source=source,
    )
