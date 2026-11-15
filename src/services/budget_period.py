"""Period anchoring for token budgets — what ``consumed_tokens`` MEANS.

``TokenBudget.period`` ('monthly' / 'weekly') used to be decoration: it picked a
Redis key suffix and an alert bucket, but nothing ever reset
``consumed_tokens``. The column only ever grew (``current + take`` in
:mod:`services.budget_debit`) and was zeroed exactly once, at row creation. A
'monthly' budget was therefore a LIFETIME cap: once a tenant burned it in
January, every reservation for the rest of the row's life was denied.

The fix is period ANCHORING, not a reset cron. A scheduled job that misses a run
silently over-enforces (the same class of money bug), and it cannot be made
correct across restarts without leader election. Instead each row records WHICH
period its ``consumed_tokens`` belongs to (``consumed_period_start``), and every
reader derives the current-period figure:

    effective_consumed(budget, ts) == 0   when the stored consumption belongs
                                          to an earlier period than ``ts``

That is correct with no background process at all, correct across a restart
(it is a pure function of two persisted columns plus the clock), and correct
under concurrency: the actual rebase-to-zero write happens inside the debit
path's ``SELECT ... FOR UPDATE`` transaction, and any reader that races ahead of
that write computes the same 0 anyway.

``consumed_period_start`` is NULL on rows that predate the column. NULL is
resolved lazily from ``starts_at`` — the period the budget was created in — which
is the same value the backfill migration writes. So a June-created monthly budget
read in July is correctly seen as rolled over whether or not the backfill ran.

Anchors are computed in UTC. This service's rollups were already moved to
explicit UTC bucketing (the DB session TZ is Europe/Amsterdam in prod), so
anchoring on the DB session's local date would reintroduce that bug.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol


class PeriodBearing(Protocol):
    """What the anchoring functions actually need — three attributes, no ORM.

    They read via ``getattr`` and never touch a session, so a raw SQLAlchemy
    ``Row`` satisfies them exactly as well as a ``TokenBudget``. Saying that in
    the type is not a formality: ``attribution_audit`` had a hand-rolled copy of
    ``is_stale_period`` for precisely the stated reason "without needing the ORM
    object", and the copy drifted (it used ``!=`` where this module uses ``<``).
    A signature that admits a Row is what lets that caller use the accessor
    instead of re-deriving it.
    """

    period: Any
    starts_at: Any
    consumed_tokens: Any
    consumed_period_start: Any


def period_anchor(period: str | None, ts: datetime) -> date | None:
    """First day of the ``period`` containing ``ts`` (UTC).

    ``monthly`` -> the 1st of that month. ``weekly`` -> the ISO Monday of that
    week. Any other/absent period -> ``None``, meaning "no period semantics,
    never resets" — a lifetime budget. Returning None rather than guessing keeps
    an unknown period from silently zeroing a real cap.
    """
    if period is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    d = ts.astimezone(UTC).date()
    if period == "monthly":
        return d.replace(day=1)
    if period == "weekly":
        return d - timedelta(days=d.weekday())
    return None


def consumption_anchor(budget: PeriodBearing, ts: datetime) -> date | None:
    """The period ``budget.consumed_tokens`` currently belongs to.

    Prefers the persisted ``consumed_period_start``; falls back to the period of
    ``starts_at`` for rows written before that column existed (identical to the
    backfill rule, so lazy and eager resolution agree).
    """
    anchor = period_anchor(budget.period, ts)
    if anchor is None:
        return None
    stored = getattr(budget, "consumed_period_start", None)
    if stored is not None:
        return stored
    starts_at = getattr(budget, "starts_at", None)
    if starts_at is None:
        return anchor
    return period_anchor(budget.period, starts_at)


def is_stale_period(budget: PeriodBearing, ts: datetime) -> bool:
    """True when ``consumed_tokens`` belongs to a period BEFORE the one at ``ts``."""
    anchor = period_anchor(budget.period, ts)
    if anchor is None:
        return False
    stored = consumption_anchor(budget, ts)
    return stored is not None and stored < anchor


def effective_consumed(budget: PeriodBearing, ts: datetime) -> int:
    """Current-period consumption for ``budget`` — 0 once the period rolled over.

    This is THE read-side accessor. Every enforcement, status and mirror path
    must use it instead of ``budget.consumed_tokens`` directly, or the gate and
    the report disagree about whether a monthly budget has reset.
    """
    if is_stale_period(budget, ts):
        return 0
    return int(budget.consumed_tokens or 0)
