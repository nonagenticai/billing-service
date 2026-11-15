"""Shared time utilities for period calculations."""

from datetime import UTC, date, datetime, timedelta

_DEFAULT_WINDOW_DAYS = 30


def current_month_start() -> date:
    """First day of the current UTC month."""
    now = datetime.now(UTC)
    return date(now.year, now.month, 1)


def current_week_start() -> date:
    """ISO week start (Monday) in UTC."""
    today = datetime.now(UTC).date()
    return today - timedelta(days=today.weekday())


def default_start() -> datetime:
    """Default lower bound for time-windowed queries: now − 30 days, UTC."""
    return datetime.now(UTC) - timedelta(days=_DEFAULT_WINDOW_DAYS)


def default_end() -> datetime:
    """Default upper bound for time-windowed queries: now, UTC."""
    return datetime.now(UTC)
