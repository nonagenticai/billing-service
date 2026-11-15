#!/usr/bin/env python3
"""Reverse ONLY the loose-heuristic parks of migration 0013, and restate.

WHY THIS EXISTS
---------------
0013 parks worker-management duplicates under two rules with two different
``exclusion_reason`` values:

    duplicate_of_inference_router          STRICT  — a PROVEN correlation twin.
    duplicate_of_inference_router_loose    LOOSE   — a HEURISTIC twin, matched on
                                                     tokens + cost + the same UTC
                                                     minute, for rows carrying no
                                                     correlation_id at all.

The loose rule cannot tell a duplicate from two genuinely identical calls made
in the same minute, so parking under it risks an UNDER-bill. The owner accepted
that risk on 2026-08-26 — on the condition that it stays independently
reversible. This script is that condition: it un-parks exactly the loose set and
leaves every strict park untouched. It is NOT ``alembic downgrade``, which would
reverse both.

Nothing is deleted here either. Un-parking is the same plain UPDATE that parking
is, in the other direction.

USAGE (in the billing pod, or a Job cloned from the billing deployment env)::

    python scripts/unpark_loose_duplicates.py            # DRY RUN — measure only
    python scripts/unpark_loose_duplicates.py --apply    # un-park and restate

The UN-PARK SQL is imported from the migration itself rather than retyped, so this
script cannot drift from what 0013 wrote.

THE RESTATEMENT IS NOT 0013's, AND MUST NOT BE
----------------------------------------------
Un-parking changes which events are billable, so ``billing_cost_daily`` has to be
restated for the affected dates. This used to run 0013's frozen
``DELETE_STALE_DAILY_SQL`` + ``REBUILD_DAILY_SQL``, which is wrong for the same
two reasons it was wrong in ``backfill_license_attribution.py`` (see that
script's docstring): 0013 predates the ``project_id`` axis (alembic 0036), so
every rebuilt row lands on ``NULL_PROJECT_ID`` and the per-project grain of EVERY
tenant on the date is destroyed; and it buckets on ``created_at`` where the live
engine buckets on ``coalesce(event_ts, created_at)``.

It now calls ``services.rollup_engine.rollup_daily`` — the live engine — once per
affected date. ``rollup_daily`` opens its own session, takes
``pg_advisory_xact_lock`` and commits, so it cannot run inside this script's
transaction: the un-park is committed first, then the dates are rolled, exactly
as ``rollup_engine.catchup_daily`` does it.

THE GRAIN CONTROL
-----------------
This script had NO invariant check at all. It cannot borrow the backfill's money
invariant — un-parking legitimately INCREASES billable spend, that is its whole
purpose — so the control is the grain instead: after the restatement, the
per-``(bucket_date, project_id)`` money/request tuple in ``billing_cost_daily``
must equal the same tuple derived directly from ``billing_cost_events``. That is
exactly the assertion a 0013-shaped restatement fails (one sentinel project
bucket holding many projects' money), and it holds regardless of how much the
un-park moved. On a mismatch the un-park is reverted (the loose parks are
re-applied) and the dates are re-rolled.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import sys
from pathlib import Path

from sqlalchemy import text

from db import get_session_factory, init_db  # type: ignore[import-not-found]
from services.rollup_engine import rollup_daily  # type: ignore[import-not-found]
from settings import get_settings  # type: ignore[import-not-found]

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "0013_park_router_duplicates.py"
)


def _load_migration():
    """Load 0013 by path — it is not an importable package module."""
    spec = importlib.util.spec_from_file_location("_migration_0013", _MIGRATION_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError(f"cannot load {_MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MEASURE_SQL = """
SELECT exclusion_reason, count(*), coalesce(sum(cost_usd), 0)
FROM billing_cost_events
WHERE excluded_from_billing = true
GROUP BY exclusion_reason
ORDER BY 2 DESC
"""

# ⛔ `coalesce(event_ts, created_at)`, NOT bare `created_at`. This is the day the
# LIVE rollup puts the row on (`rollup_engine.cost_event_time`); 0013's
# `AFFECTED_DATES_SQL` used ingest time, so a row that crossed UTC midnight
# between emit and flush re-rolled a day it is not in and left its real day stale.
LOOSE_DATES_SQL = """
SELECT DISTINCT (coalesce(event_ts, created_at) AT TIME ZONE 'UTC')::date AS d
FROM billing_cost_events
WHERE exclusion_reason = :reason
ORDER BY d
"""

# The ids being un-parked, captured before the write so the un-park is exactly
# reversible if the grain control trips. `rollup_daily` commits, so a trip cannot
# be a ROLLBACK — it has to be a compensating re-park plus a re-roll.
LOOSE_IDS_SQL = """
SELECT id, duplicate_of_event_id
FROM billing_cost_events
WHERE exclusion_reason = :reason
"""

REPARK_SQL = """
UPDATE billing_cost_events
   SET excluded_from_billing = true,
       exclusion_reason      = :reason,
       duplicate_of_event_id = :duplicate_of_event_id
 WHERE id = :id
"""

# ⛔ MIRRORS `rollup_engine.rollup_daily`'s cost aggregate — see the identical
# block in scripts/backfill_license_attribution.py for why each expression is
# what it is. If the engine's grouping changes, this must change with it.
EVENT_GRAIN_SQL = """
SELECT (coalesce(event_ts, created_at) AT TIME ZONE 'UTC')::date AS bucket_date,
       coalesce(nullif(project_id, tenant_id),
                '00000000-0000-0000-0000-000000000004'::uuid) AS project_id,
       coalesce(sum(cost_usd), 0)       AS total_cost_usd,
       count(*)                         AS request_count
  FROM billing_cost_events
 WHERE (coalesce(event_ts, created_at) AT TIME ZONE 'UTC')::date = ANY(:dates)
   AND excluded_from_billing = false
 GROUP BY 1, 2
 ORDER BY 1, 2
"""

DAILY_GRAIN_SQL = """
SELECT bucket_date,
       project_id,
       coalesce(sum(total_cost_usd), 0) AS total_cost_usd,
       coalesce(sum(request_count), 0)  AS request_count
  FROM billing_cost_daily
 WHERE bucket_date = ANY(:dates)
 GROUP BY 1, 2
 ORDER BY 1, 2
"""


class RollupError(Exception):
    """Raised when one or more dates could not be rolled after the un-park was
    already committed. Mirrors ``backfill_license_attribution.RollupError`` —
    see there for why the succeeded/failed split matters."""

    def __init__(self, succeeded: list, failed: list):
        self.succeeded = succeeded
        self.failed = failed
        super().__init__(f"{len(failed)}/{len(succeeded) + len(failed)} date(s) failed to roll")


async def _rollup_all_or_raise(factory, dates: list) -> None:
    """Call `rollup_daily` for every date, never abandoning the tail of the list
    because one date raised. See the identical helper and its docstring in
    ``scripts/backfill_license_attribution.py`` — this must not drift from it."""
    succeeded: list = []
    failed: list = []
    for day in dates:
        try:
            await rollup_daily(factory, day)
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            failed.append((day, exc))
        else:
            succeeded.append(day)
    if failed:
        raise RollupError(succeeded, failed)


def _report_rollup_failure(exc: RollupError) -> None:
    failed_dates = sorted(d for d, _ in exc.failed)
    py_list = "[" + ", ".join(f"'{d}'" for d in failed_dates) + "]"
    print(
        "\nABORTED — rollup failed for "
        f"{len(failed_dates)}/{len(exc.succeeded) + len(failed_dates)} date(s) AFTER "
        "the un-park was already committed. The rows for EVERY date (including "
        "the ones below) are already un-parked — this is a STALE ROLLUP, not an "
        "un-reversed park. Re-running this script will NOT fix it: the rows no "
        "longer carry exclusion_reason=loose, so LOOSE_DATES_SQL cannot find "
        "them again and nothing will recompute these dates.\n"
        f"  unrolled date(s): {py_list}\n"
        "  recovery — re-roll ONLY these dates with the same idempotent, "
        "DELETE-then-rebuild engine (services.rollup_engine.rollup_daily is "
        "safe to re-run, see scripts/restate_rollups_utc.py):\n"
        '    python -c "\n'
        "import asyncio\n"
        "from db import get_session_factory, init_db\n"
        "from services.rollup_engine import rollup_daily\n"
        "from settings import get_settings\n"
        "async def main():\n"
        "    s = get_settings(); await init_db(s); f = get_session_factory()\n"
        f"    for d in {py_list}:\n"
        "        await rollup_daily(f, d)\n"
        'asyncio.run(main())"\n'
    )
    for day, err in exc.failed:
        print(f"    {day}: {err!r}")


def _grain(rows) -> dict:
    """``(bucket_date, project_id) -> (money, requests)`` from either grain query."""
    return {(r[0], r[1]): (r[2], r[3]) for r in rows}


async def _report(session, label: str) -> None:
    rows = (await session.execute(text(MEASURE_SQL))).all()
    print(f"  parked, {label}:")
    if not rows:
        print("    (nothing parked)")
    for reason, n, dollars in rows:
        print(f"    {reason!r}: {n} rows, ${dollars}")


async def _run(apply: bool) -> int:
    migration = _load_migration()
    loose = migration.EXCLUSION_REASON_LOOSE

    settings = get_settings()
    await init_db(settings)
    factory = get_session_factory()

    async with factory() as session:
        await _report(session, "before")

        # Capture the dates BEFORE the un-park — afterwards the rows no longer
        # carry the reason, so they could not be found again.
        dates = [r[0] for r in await session.execute(text(LOOSE_DATES_SQL), {"reason": loose})]

        if not apply:
            print(
                f"\nDRY RUN. Would un-park exclusion_reason={loose!r} and restate "
                f"billing_cost_daily for {len(dates)} UTC date(s)"
                + (f" ({dates[0]}..{dates[-1]})." if dates else ".")
                + "\nRe-run with --apply to do it. Strict parks are never touched."
            )
            return 0

        parked_rows = (await session.execute(text(LOOSE_IDS_SQL), {"reason": loose})).all()
        unparked = (await session.execute(text(migration.UNPARK_LOOSE_SQL))).rowcount
        # ⛔ COMMIT FIRST — `rollup_daily` opens its own session and commits, so it
        # cannot see (or participate in) this transaction. Same ordering as
        # `rollup_engine.catchup_daily`.
        await session.commit()

    try:
        await _rollup_all_or_raise(factory, dates)
    except RollupError as exc:
        _report_rollup_failure(exc)
        return 1

    async with factory() as session:
        params = {"dates": dates}
        event_grain = _grain((await session.execute(text(EVENT_GRAIN_SQL), params)).all())
        daily_grain = _grain((await session.execute(text(DAILY_GRAIN_SQL), params)).all())

        # THE CONTROL. Not a money invariant — un-parking is SUPPOSED to move
        # money — but a grain one: the restated rollup must reproduce the events'
        # own per-(bucket_date, project_id) tuple. A 0013-shaped restatement fails
        # this, because it writes no project_id and every row lands on the
        # NULL_PROJECT_ID sentinel.
        if dates and daily_grain != event_grain:
            reparked = 0
            for row_id, dup_of in parked_rows:
                reparked += (
                    await session.execute(
                        text(REPARK_SQL),
                        {"id": row_id, "reason": loose, "duplicate_of_event_id": dup_of},
                    )
                ).rowcount
            await session.commit()
            try:
                await _rollup_all_or_raise(factory, dates)
            except RollupError as revert_exc:
                print(
                    "\nDOUBLE FAULT — the compensating re-park's own re-roll failed for "
                    "some dates. The rows have been re-parked in the database, but the "
                    "dates below still carry a rollup consistent with the OLD "
                    "(un-parked) state, not the reverted one:"
                )
                _report_rollup_failure(revert_exc)
                return 1
            keys = sorted(set(daily_grain) | set(event_grain), key=lambda k: (str(k[0]), str(k[1])))
            diff = [
                f"    {k}: rollup={daily_grain.get(k)} events={event_grain.get(k)}"
                for k in keys
                if daily_grain.get(k) != event_grain.get(k)
            ]
            print(
                "\nABORTED AND REVERTED — the restated billing_cost_daily does not "
                "reproduce the events' per-(bucket_date, project_id) grain:\n"
                + "\n".join(diff[:10])
                + (f"\n    ... and {len(diff) - 10} more" if len(diff) > 10 else "")
                + f"\n  {reparked} row(s) re-parked and {len(dates)} date(s) re-rolled."
            )
            return 1

        print(
            f"\nUn-parked {unparked} row(s) with exclusion_reason={loose!r} and "
            f"restated billing_cost_daily for {len(dates)} UTC date(s) via "
            "services.rollup_engine.rollup_daily.\n"
            f"  grain invariant HELD: {len(event_grain)} (bucket_date, project_id) "
            "bucket(s) reproduced exactly from the events."
        )
        await _report(session, "after")
        print("Strict parks (duplicate_of_inference_router) are unchanged.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually un-park. Without it this only measures.",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args.apply))


if __name__ == "__main__":
    sys.exit(main())
