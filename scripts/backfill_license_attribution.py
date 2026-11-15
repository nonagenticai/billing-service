#!/usr/bin/env python3
"""Attribute a licence to historical cost events that were written without one.

WHY THIS EXISTS
---------------
``services/license_resolver`` closes the attribution gap for NEW events, at flush
time. It cannot reach backwards: a row written with ``license_id IS NULL`` stays
NULL forever, because nothing in the estate ever UPDATEs that column outside a
migration. Measured on prod 2026-08-28: **354,380 billable rows** carry a NULL
licence, $2,815.01 of spend that can never reach an invoice line.

WHAT THIS WILL AND WILL NOT RECOVER — READ THIS BEFORE QUOTING A NUMBER
----------------------------------------------------------------------
The honest ceiling is **~3,805 rows, about 1.1%**, and effectively all of it is
``w2v-tts``. It is not 14%. An earlier estimate said 49,659 (14.0%) by applying
only the resolver's own matching rule; that number is wrong twice over:

1. It counted rows whose ``subject_id`` is the TENANT ECHOED BACK. 50,234 rows
   (kag-graph and karma, 100% of theirs) carry ``subject_id == tenant_id``
   because ``send_chunks_to_kag_graph`` literally writes ``"subject_id":
   tenant_id``. That is not an identity, and admitting it would turn a backfill
   into a fabrication. Hence the ``subject_id <> tenant_id`` half of the
   predicate — the naive ``subject_id IS NOT NULL`` lets all 50,234 in.
2. It ignored the licence's own validity window (below), which correctly
   declines rows that predate the licence they would be attributed to.

298,615 of the NULL rows have no ``subject_id`` at all — no credential was ever
presented. **No billing change can attribute those**, and this script must never
try. They are an identity problem at the emitter.

THE RULES
---------
1. **Never fabricate.** Reuse ``license_resolver.resolve_license`` rather than
   re-implementing the match, so the "exactly one candidate, or nothing"
   abstention is the same code that runs at flush time and cannot drift from it.
2. **Only a genuinely identified row.** ``subject_id IS NOT NULL AND
   subject_id <> tenant_id``.
3. **Only inside the licence's own validity window.** Never attribute spend that
   happened before its candidate licence existed. Out-of-window rows are
   DECLINED and counted, never stretched to fit.
4. **Only an ACTIVE licence.** Billing's resolver keys on ``app_id IS NOT NULL``
   alone and has no notion of licence status — it can resolve to a revoked one.
   Measured 2026-08-28: of the 26 quota rows with a NULL ``app_id``, 6 are
   revoked and 2 expired. This script does not inherit that blindness: the
   window/status data comes from the licences service, and only ``status=active``
   licences are eligible.
5. **Distinguishable.** A backfilled attribution appends ``+backfilled`` to
   ``cost_source``, distinct from the flusher's ``+resolved``. One was derived
   live from the map in force at the time; the other was applied retroactively
   months later. A number that cannot be told apart from an asserted one is how a
   derived number later gets quoted as evidence.

THE ROLLUP MUST BE RESTATED, AND THAT IS THE RISKY PART
-------------------------------------------------------
``billing_cost_daily``'s unique key includes BOTH ``license_id`` and
``cost_source`` (see ``models/db/rollups.py`` ``uq_cost_daily``), and it is the
invoice source. Changing either on an event without restating the affected
buckets leaves the rollup permanently disagreeing with the events.

The restatement is ``services.rollup_engine.rollup_daily`` — the LIVE engine, the
same function the nightly loop and ``services.reprice`` call. It used to be the
frozen SQL imported from migration 0013, and that was a money-grade defect:

* 0013 predates the ``project_id`` axis (alembic 0036). Its INSERT lists no
  ``project_id`` column at all, so every restated row landed on the table's
  ``NULL_PROJECT_ID`` default. Measured live on 2026-09-16: restating that one
  date collapsed **107 rows into 51**, destroying the per-project grain of
  **2,230 events / $28.23 across 4 tenants and 50 projects** — for ALL tenants
  on the date, not only the rows this script attributes.
* 0013 buckets on ``created_at``; the live engine buckets on
  ``coalesce(event_ts, created_at)`` (``rollup_engine.cost_event_time``). A row
  emitted at 23:59:50 and ingested at 00:00:05 therefore landed on a different
  DAY than the engine puts it on, so the two restatements disagreed about which
  bucket a charge belongs to.

Neither shows up in a money total — regrouping and re-dating preserve
``sum(total_cost_usd)`` and ``sum(request_count)`` over the affected date set —
which is exactly why the original invariant read HELD while the grain was being
destroyed. The guard is therefore in two parts now (see ``_run``):

1. the money control, unchanged: the affected dates' daily totals must not move;
2. the GRAIN control: the per-``(bucket_date, project_id)`` money/request tuple
   derived FROM THE EVENTS must be identical before and after the UPDATE (this
   script writes only ``license_id``/``cost_source``, so it can never move a row
   between projects or days), and the restated ``billing_cost_daily`` must agree
   with that events-derived grain. Part 2 is what a 0013-shaped restatement
   fails.

⛔ Row COUNT is deliberately NOT asserted: re-attribution genuinely merges and
splits rows as the ``license_id``/``cost_source`` grouping changes.

``rollup_daily`` opens its OWN session, takes ``pg_advisory_xact_lock`` and
commits, so it cannot run inside this script's transaction. The event UPDATEs are
therefore committed FIRST and the rollup is then called once per affected date —
the same ordering ``rollup_engine.catchup_daily`` uses. That means a guard trip
cannot be a plain ``ROLLBACK``; it is a COMPENSATING revert (the attributions are
undone and the dates re-rolled), which leaves the same end state.

The blast radius is still much wider than the rows this script updates: a whole
``bucket_date`` is rebuilt for every affected date, all tenants.

USAGE (in the billing pod, or a Job cloned from the billing deployment env)::

    python scripts/backfill_license_attribution.py            # DRY RUN — measure only
    python scripts/backfill_license_attribution.py --apply    # attribute and restate
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import text

from db import get_session_factory, init_db  # type: ignore[import-not-found]
from service_auth import ServiceTokenProvider  # type: ignore[import-not-found]
from services.license_resolver import (  # type: ignore[import-not-found]
    get_resolution_map,
    parse_service_key_app_map,
    resolve_license,
)
from services.rollup_engine import rollup_daily  # type: ignore[import-not-found]
from settings import get_settings  # type: ignore[import-not-found]

# Appended to cost_source, deliberately NOT the flusher's "+resolved" — see rule 5.
BACKFILL_MARKER = "+backfilled"

# The candidate set. Note what is NOT here: no service_key filter. A service_key
# with no app mapping simply resolves to None and is counted as declined, which
# is the honest outcome and keeps the audit line's denominator meaningful.
# NOTE the fourth and fifth columns. `created_at` is what rule 3's validity-window
# check has always used (ingest time). `bucket_date` is what the LIVE rollup buckets
# on — `(coalesce(event_ts, created_at) AT TIME ZONE 'UTC')::date`, i.e.
# `rollup_engine.cost_event_time` — and is the ONLY thing the restatement may key
# on. They are not the same day for a row that crossed UTC midnight between emit
# and flush; using `created_at` for the restatement (as this script did while it
# ran 0013's SQL) re-rolls a day that does not contain the changed row and leaves
# the day that does stale.
CANDIDATES_SQL = """
SELECT id, tenant_id, service_key, created_at, cost_source,
       (coalesce(event_ts, created_at) AT TIME ZONE 'UTC')::date AS bucket_date
  FROM billing_cost_events
 WHERE license_id IS NULL
   AND excluded_from_billing = false
   AND subject_id IS NOT NULL
   AND subject_id::text <> tenant_id::text
   AND id > :after
 ORDER BY id
 LIMIT :batch
"""

ATTRIBUTE_SQL = """
UPDATE billing_cost_events
   SET license_id = :license_id,
       cost_source = :cost_source
 WHERE id = :id
   AND license_id IS NULL
"""

TOTALS_SQL = """
SELECT count(*), coalesce(sum(total_cost_usd), 0), coalesce(sum(request_count), 0)
  FROM billing_cost_daily
 WHERE bucket_date = ANY(:dates)
"""

# THE GRAIN CONTROL — the half the money invariant is structurally blind to.
#
# This script writes ONLY `license_id` and `cost_source` (ATTRIBUTE_SQL). It
# cannot move a row to another project and it cannot change when the row
# happened. So the per-(bucket_date, project_id) money/request tuple derived from
# the EVENTS is an invariant of the whole run, and the restated
# `billing_cost_daily` must reproduce it exactly.
#
# ⛔ The expressions here MUST mirror `rollup_engine.rollup_daily`'s cost
# aggregate, not approximate it:
#   * the day is `coalesce(event_ts, created_at)` at UTC — `cost_event_time`;
#   * the project is `coalesce(nullif(project_id, tenant_id), NULL_PROJECT_ID)`
#     — the `_cproj` expression, including the nullif that reclassifies a
#     project_id forging its own tenant id onto the project-less sentinel;
#   * the money is `sum(cost_usd)` (NULL, i.e. unpriced, is SKIPPED by sum, as in
#     the engine) and the request count is `count(*)`;
#   * excluded (parked) rows are out, matching the engine's
#     `excluded_from_billing IS false`.
# If the engine's grouping changes, this must change with it — a drifting control
# is worse than none, because it reports green about a different question.
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

# The compensating revert. `rollup_daily` commits, so a guard trip after the
# restatement cannot be a ROLLBACK of the enclosing transaction — the
# attributions are already durable. Undoing them by hand and re-rolling the same
# dates reaches the identical end state, and it is exact rather than approximate
# because the pre-UPDATE `cost_source` is captured per row before the write.
REVERT_SQL = """
UPDATE billing_cost_events
   SET license_id = NULL,
       cost_source = :prev_cost_source
 WHERE id = :id
   AND license_id = :license_id
"""


class RollupError(Exception):
    """Raised when one or more dates could not be rolled after the attribution
    events were already committed. Carries the FULL partition — succeeded and
    failed — because the caller must know both: succeeded dates are safe to
    invariant-check, failed dates are not (their `billing_cost_daily` is
    whatever it was before this run and must not be compared against events
    that have since changed)."""

    def __init__(self, succeeded: list[Any], failed: list[tuple[Any, BaseException]]):
        self.succeeded = succeeded
        self.failed = failed
        super().__init__(f"{len(failed)}/{len(succeeded) + len(failed)} date(s) failed to roll")


async def _rollup_all_or_raise(factory: Any, dates: list[Any]) -> None:
    """Call `rollup_daily` for every date, never abandoning the tail of the list
    because one date raised.

    This does NOT try to restore the old single-transaction atomicity — that is
    what destroyed the project axis in the first place, and the two-phase
    (commit events, then roll each date) shape stays. What changes is that a
    deadlock / connection reset / advisory-lock contention on date N no longer
    silently strands dates N+1..M with a stale `billing_cost_daily` and a bare
    traceback: every date is attempted, and the caller gets the exact list of
    what failed so it can be reported and re-rolled without re-running the
    (now-blind) backfill.
    """
    succeeded: list[Any] = []
    failed: list[tuple[Any, BaseException]] = []
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
        "the attribution events were already committed. The events for EVERY "
        "date (including the ones below) are already attributed — this is a "
        "STALE ROLLUP, not an unattributed one. Re-running this backfill will "
        "NOT fix it: CANDIDATES_SQL filters license_id IS NULL, so the "
        "now-attributed rows are invisible to the next run and nothing will "
        "recompute these dates.\n"
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


def _grain(rows: Any) -> dict[tuple[Any, Any], tuple[Any, Any]]:
    """``(bucket_date, project_id) -> (money, requests)`` from either grain query."""
    return {(r[0], r[1]): (r[2], r[3]) for r in rows}


def _grain_diff(before: dict[Any, Any], after: dict[Any, Any], limit: int = 10) -> str:
    keys = sorted(set(before) | set(after), key=lambda k: (str(k[0]), str(k[1])))
    lines = [
        f"    {k}: {before.get(k)} -> {after.get(k)}" for k in keys if before.get(k) != after.get(k)
    ]
    extra = len(lines) - limit
    return "\n".join(lines[:limit] + ([f"    ... and {extra} more"] if extra > 0 else []))


def _parse_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return datetime.fromisoformat(raw)


async def fetch_license_windows(
    settings: Any,
) -> dict[str, tuple[datetime | None, datetime | None]]:
    """``license_id -> (starts_at, expires_at)`` for ACTIVE licences only.

    Fail-CLOSED, and loudly. `licenses` is a separate database — there is no
    cross-database join available — so this is the only source for the window and
    the status. If it is unreachable, or returns an empty list, every row would
    silently decline and the run would report "0 attributed" as though the work
    were done. An empty result is therefore an ERROR, not an answer: it is the
    difference between "nothing to attribute" and "I could not see anything".
    """
    if not settings.licenses_url:
        raise RuntimeError("licenses_url is not configured — cannot verify licence windows")

    provider = ServiceTokenProvider(settings)
    headers: dict[str, str] = {}
    token = await provider.get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{settings.licenses_url}/internal/licenses",
            params={"status": "active"},
            headers=headers,
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"licences service returned {resp.status_code} — refusing to guess windows"
        )

    windows: dict[str, tuple[datetime | None, datetime | None]] = {}
    for lic in resp.json():
        lic_id = lic.get("id")
        if not lic_id:
            continue
        windows[str(lic_id)] = (_parse_ts(lic.get("starts_at")), _parse_ts(lic.get("expires_at")))

    if not windows:
        raise RuntimeError(
            "licences service returned ZERO active licences — refusing to run. Every row would "
            "decline and the audit line would read like a clean no-op."
        )
    return windows


def _in_window(created_at: datetime, window: tuple[datetime | None, datetime | None]) -> bool:
    """True iff the event happened inside the licence's own validity window.

    A NULL ``starts_at`` is treated as UNBOUNDED-BELOW only because the licences
    schema allows it; every licence measured on prod carries one. A NULL
    ``expires_at`` is a perpetual licence and genuinely has no upper bound.
    """
    starts_at, expires_at = window
    if starts_at is not None and created_at < starts_at:
        return False
    return expires_at is None or created_at <= expires_at


async def _run(apply: bool, batch: int) -> int:
    settings = get_settings()
    app_by_key = parse_service_key_app_map(settings.cost_attribution_app_by_service_key)
    if not app_by_key:
        print("ABORT: cost_attribution_app_by_service_key is empty — nothing could ever resolve.")
        return 1

    windows = await fetch_license_windows(settings)
    print(f"  {len(windows)} ACTIVE licence(s) with a validity window from the licences service")

    await init_db(settings)
    factory = get_session_factory()

    counts: Counter[str] = Counter()
    per_service: Counter[str] = Counter()
    dates: set[Any] = set()
    updates: list[dict[str, Any]] = []

    async with factory() as session:
        resolution_map = await get_resolution_map(session)
        print(
            f"  {len(resolution_map)} (tenant, app) -> licence candidate(s) in the resolution map"
        )

        after = "00000000-0000-0000-0000-000000000000"
        while True:
            rows = (
                await session.execute(text(CANDIDATES_SQL), {"after": after, "batch": batch})
            ).all()
            if not rows:
                break
            for row_id, tenant_id, service_key, created_at, cost_source, bucket_date in rows:
                after = str(row_id)
                counts["examined"] += 1

                if service_key not in app_by_key:
                    counts["declined_unmapped_service_key"] += 1
                    continue

                resolved = resolve_license(
                    tenant_id=str(tenant_id) if tenant_id else None,
                    service_key=service_key,
                    app_by_service_key=app_by_key,
                    resolution_map=resolution_map,
                )
                if resolved is None:
                    counts["declined_no_licence"] += 1
                    continue

                window = windows.get(str(resolved))
                if window is None:
                    # The map produced a licence the licences service does not
                    # list as active: revoked, expired, or deleted. Rule 4.
                    counts["declined_not_active"] += 1
                    continue

                if not _in_window(created_at, window):
                    counts["declined_out_of_window"] += 1
                    continue

                counts["resolved"] += 1
                per_service[service_key] += 1
                # The ROLLUP's day, not `created_at.date()` — see CANDIDATES_SQL.
                dates.add(bucket_date)
                updates.append(
                    {
                        "id": row_id,
                        "license_id": resolved,
                        "cost_source": f"{cost_source or 'unknown'}{BACKFILL_MARKER}",
                        # Captured for REVERT_SQL: the exact pre-UPDATE value, so a
                        # compensating undo restores the row rather than guessing.
                        "prev_cost_source": cost_source,
                    }
                )

        ordered_dates = sorted(dates)
        print("\n  audit: " + " ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        print(
            "  resolved by service_key: "
            + (" ".join(f"{k}={v}" for k, v in sorted(per_service.items())) or "(none)")
        )
        print(
            f"  affected UTC date(s): {len(ordered_dates)}"
            + (f" ({ordered_dates[0]}..{ordered_dates[-1]})" if ordered_dates else "")
        )

        if not apply:
            print("\nDRY RUN. Nothing was written. Re-run with --apply.")
            return 0

        if not updates:
            print("\nNothing to attribute. No write, no restatement.")
            return 0

        params_totals = {"dates": ordered_dates}
        before_totals = (await session.execute(text(TOTALS_SQL), params_totals)).one()
        before_grain = _grain((await session.execute(text(EVENT_GRAIN_SQL), params_totals)).all())

        written = 0
        for chunk_start in range(0, len(updates), batch):
            chunk = updates[chunk_start : chunk_start + batch]
            for params in chunk:
                written += (
                    await session.execute(
                        text(ATTRIBUTE_SQL),
                        {k: params[k] for k in ("id", "license_id", "cost_source")},
                    )
                ).rowcount

        # ⛔ COMMIT BEFORE THE RESTATEMENT, AND THAT ORDERING IS FORCED.
        # `rollup_daily` opens its own session from the factory, takes
        # `pg_advisory_xact_lock` and commits — it CANNOT be called inside this
        # transaction (it would read a snapshot without these updates and its
        # commit would not be ours to roll back). This mirrors
        # `rollup_engine.catchup_daily`, which rolls each date in its own
        # transaction for exactly the same reason.
        await session.commit()

    # One call, one date, one transaction, one advisory lock — the
    # `catchup_daily` loop. This is the LIVE aggregate: it carries the project
    # axis, buckets on `coalesce(event_ts, created_at)`, derives
    # `attribution_status`, and guards its own delete against a truncated
    # source. The frozen 0013 SQL this replaces did none of those.
    #
    # Every date is attempted even if an earlier one raises (deadlock,
    # connection reset, advisory-lock contention with the nightly aggregator,
    # a kill -9) — see `_rollup_all_or_raise`. The events for ALL dates are
    # already committed above, so a partial failure here is a STALE ROLLUP for
    # the dates that failed, never an unattributed event.
    try:
        await _rollup_all_or_raise(factory, ordered_dates)
    except RollupError as exc:
        _report_rollup_failure(exc)
        return 1

    async with factory() as session:
        after_totals = (await session.execute(text(TOTALS_SQL), params_totals)).one()
        after_grain = _grain((await session.execute(text(EVENT_GRAIN_SQL), params_totals)).all())
        daily_grain = _grain((await session.execute(text(DAILY_GRAIN_SQL), params_totals)).all())

        # ── CONTROL 1: the money. Re-attribution moves a charge between grouping
        # keys; it can never change what was charged.
        money_ok = (before_totals[1], before_totals[2]) == (after_totals[1], after_totals[2])

        # ── CONTROL 2: the grain. Two assertions, and they fail for different
        # reasons — keep both.
        #   (a) the events' own per-project grain must be untouched by the UPDATE.
        #       This script writes license_id/cost_source only, so a difference
        #       here means the write predicate started moving rows between
        #       projects or days.
        #   (b) the restated rollup must REPRODUCE that grain. This is the one
        #       that fails on a 0013-shaped restatement: with no project_id in
        #       the INSERT every row lands on NULL_PROJECT_ID, so (b) sees one
        #       sentinel bucket holding the money of many projects.
        events_grain_ok = before_grain == after_grain
        rollup_grain_ok = daily_grain == after_grain

        if not (money_ok and events_grain_ok and rollup_grain_ok):
            # A COMPENSATING revert, not a ROLLBACK: `rollup_daily` already
            # committed. Undo the attributions with their captured previous
            # cost_source and re-roll the same dates, reaching the pre-run state.
            reverted = 0
            for params in updates:
                reverted += (
                    await session.execute(
                        text(REVERT_SQL),
                        {
                            "id": params["id"],
                            "license_id": params["license_id"],
                            "prev_cost_source": params["prev_cost_source"],
                        },
                    )
                ).rowcount
            await session.commit()
            try:
                await _rollup_all_or_raise(factory, ordered_dates)
            except RollupError as revert_exc:
                print(
                    "\nDOUBLE FAULT — the compensating revert's own re-roll failed for "
                    "some dates. The attributions have been undone in the database, but "
                    "the dates below still carry a rollup consistent with the OLD "
                    "(reverted) attribution state, not a fresh one:"
                )
                _report_rollup_failure(revert_exc)
                return 1

            why = []
            if not money_ok:
                why.append(
                    f"MONEY moved: usd {before_totals[1]} -> {after_totals[1]}, "
                    f"requests {before_totals[2]} -> {after_totals[2]}"
                )
            if not events_grain_ok:
                why.append(
                    "the EVENTS' per-(bucket_date, project_id) grain changed — this run "
                    "moved rows between projects or days, which it must never do:\n"
                    + _grain_diff(before_grain, after_grain)
                )
            if not rollup_grain_ok:
                why.append(
                    "the RESTATED billing_cost_daily does not reproduce the events' "
                    "per-(bucket_date, project_id) grain — the restatement lost the "
                    "project axis (left = rollup, right = events):\n"
                    + _grain_diff(daily_grain, after_grain)
                )
            print(
                "\nABORTED AND REVERTED — "
                + "; ".join(why)
                + f"\n  {reverted} attribution(s) undone and {len(ordered_dates)} "
                "date(s) re-rolled."
            )
            return 1

        print(
            f"\nAttributed {written} row(s) and restated billing_cost_daily for "
            f"{len(ordered_dates)} UTC date(s) via services.rollup_engine.rollup_daily.\n"
            f"  money invariant HELD: usd {before_totals[1]} -> {after_totals[1]}, "
            f"requests {before_totals[2]} -> {after_totals[2]} "
            f"(daily rows {before_totals[0]} -> {after_totals[0]}, regrouping only)\n"
            f"  grain invariant HELD: {len(after_grain)} (bucket_date, project_id) "
            "bucket(s) reproduced exactly from the events"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually attribute and restate. Without it this only measures.",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=1000,
        help="Rows per read/write chunk (default 1000). ~354k candidates is a scan, not a one-shot.",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args.apply, args.batch))


if __name__ == "__main__":
    sys.exit(main())
