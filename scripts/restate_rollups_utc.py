#!/usr/bin/env python3
"""One-shot restatement: rebuild every historical daily rollup on UTC buckets.

WHY
---
Until the UTC-bucketing fix, ``rollup_daily`` grouped cost/usage with a bare
``cast(created_at AS date)`` that resolved in the DB session's timezone (prod's
pgbouncer session is Europe/Amsterdam), so ~8.8% of events near a day boundary
were written to the wrong ``billing_cost_daily`` / ``billing_metering_daily``
bucket. The forward fix corrects new rollups, and the aggregator loop re-rolls
the last ``daily_reconcile_days`` automatically, but historical rows OLDER than
that window keep their wrong Amsterdam buckets until re-rolled.

This walks every day from the earliest retained event up to yesterday (UTC) and
calls the fixed ``rollup_daily``, which now DELETE-then-rebuilds each day — so a
combo whose events moved to a different UTC day cannot leave a stale over-counted
row behind. Idempotent: safe to re-run.

SAFETY
------
Bounded to ``[floor, yesterday]`` where ``floor = today - retention_days``. Do
NOT roll a day whose ``cost_events`` partition has already been dropped — the
delete-then-rebuild would empty that day's ``cost_daily``. The floor bound keeps
us inside the retained window (partitions are pruned to the same retention).

USAGE (in the billing pod / a Job cloned from the billing deployment env, AFTER
the UTC-bucketing image is live)::

    python scripts/restate_rollups_utc.py            # restate floor..yesterday
    python scripts/restate_rollups_utc.py --days 30  # restate only the last 30 days
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime, timedelta

from sqlalchemy import Date, func, select

# Reuse the app's engine/session wiring and the fixed rollup so this script can
# never drift from production behaviour.
from db import get_session_factory, init_db  # type: ignore[import-not-found]
from models.db import CostEvent  # type: ignore[import-not-found]
from services.rollup_engine import rollup_daily  # type: ignore[import-not-found]
from settings import get_settings  # type: ignore[import-not-found]


async def _run(days_override: int | None) -> int:
    settings = get_settings()
    await init_db(settings)
    factory = get_session_factory()

    today = datetime.now(UTC).date()
    yesterday = today - timedelta(days=1)
    floor = today - timedelta(days=settings.retention_days)

    # Earliest retained event, bucketed by the SAME UTC-explicit expression the
    # rollup groups on, so we never start below the first real day.
    async with factory() as session:
        earliest = (
            await session.execute(
                select(func.min(func.cast(func.timezone("UTC", CostEvent.created_at), Date)))
            )
        ).scalar_one_or_none()

    start = floor
    if earliest is not None and earliest > start:
        start = earliest
    if days_override is not None:
        override_start = yesterday - timedelta(days=days_override - 1)
        if override_start > start:
            start = override_start

    if start > yesterday:
        print("Nothing to restate (no retained days).")
        return 0

    n_days = (yesterday - start).days + 1
    print(f"Restating {n_days} day(s): {start} .. {yesterday} (UTC)")
    day = start
    done = 0
    while day <= yesterday:
        await rollup_daily(factory, day)
        done += 1
        if done % 10 == 0 or day == yesterday:
            print(f"  restated {done}/{n_days} (through {day})")
        day += timedelta(days=1)

    print(f"Done: {done} day(s) restated on UTC buckets.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Restate only the last N days (default: the whole retention window).",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args.days))


if __name__ == "__main__":
    sys.exit(main())
