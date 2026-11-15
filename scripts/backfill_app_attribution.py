#!/usr/bin/env python3
"""Derive ``app_id`` for historical cost events that were written without one.

WHY THIS EXISTS
---------------
``services/license_resolver.resolve_app`` closes the app-grain gap for NEW
events, at flush time (``services/cost_flusher``). It cannot reach backwards: a
row written with ``app_id IS NULL`` stays NULL forever, because nothing in the
estate ever UPDATEs that column outside a migration.

Measured on prod 2026-09-02: **0 of 404,407** ``billing_cost_events`` rows carry
an ``app_id`` — 100% NULL since the column existed, so per-application cost is
not merely inaccurate, it is arithmetically impossible. The grain we cannot
report is already in the database; nothing propagated it onto the event. This
script is the historical half of that same fix — the flusher handles everything
from now on, this handles everything before.

THE HONEST CEILING — READ THIS BEFORE CALLING A FINISHED RUN "BROKEN"
--------------------------------------------------------------------
**A complete, fully successful run still leaves about 52% of the table NULL,
and that is the correct outcome.** Of those 404,407 rows, **209,114 (51.7%)
carry no ``license_id`` at all**: there is nothing to derive an app FROM, so
this script can never reach them (they are an identity problem at the emitter —
see ``CANDIDATES_SQL``). The reachable population is the other **195,293
(48.3%)**, of which **194,272 (48.0% of all time)** actually resolve against
the local ``request_quota_config`` mirror.

The frequently quoted **~86% is a RECENT-TRAFFIC figure and does not describe
history.** Over the most recent 146,495 rows, 128,356 (**87.6%**) carry a
licence, and all 128,356 of those resolve — because the estate only started
forwarding ``x-license-id`` part-way through this table's life, so the older a
row is the less likely it is reachable. Quoting 86% as the target and then
seeing ~48% is how a completed backfill gets reported as a failed one.

⚠️ **Every number above is a moving window, not a constant.**
``billing_cost_events`` is written continuously by the cost flusher, so these
counts drift between any two measurements: the audit's dry run minutes before
this one saw 404,167 / 194,990 / 209,114 for the same three figures. Do not
reconcile a run against them. ``CENSUS_SQL`` below is the authority and the
script prints its result on every run, dry or applied — re-measure, never quote.

THREE THINGS THIS DOES **NOT** DO, AND WHY
------------------------------------------
This script is deliberately modelled on ``backfill_license_attribution.py`` and
deliberately drops three of its parts. Each omission is stated here because an
absent safety rail that merely LOOKS like an oversight is worse than one that is
explained — the next reader must be able to tell "considered and unnecessary"
from "forgotten".

1. **No HTTP call. No external dependency of any kind.**
   The licence backfill calls ``fetch_license_windows(settings)`` against the
   licences service, because a licence's validity window and status live in a
   SEPARATE database that billing cannot join against. This backfill needs none
   of that: ``license_id -> app_id`` is a single column pair inside billing's
   OWN ``request_quota_config`` mirror. There is no window to honour (an app is
   a property of the licence, not of a time range) and no status to check (a
   revoked licence still names the app whose spend this row genuinely was —
   suppressing it would misreport history, not protect it). So there is no
   ``licenses_url``, no ``ServiceTokenProvider``, no ``httpx``, and therefore no
   fail-closed path for an unreachable service: this script cannot be
   half-blinded by a network partition, because it never leaves the database.

2. **No rollup restatement — because the one rollup this script can affect has
   no app grain.**
   The licence backfill restates ``billing_cost_daily`` because ``license_id``
   and ``cost_source`` are both part of ``uq_cost_daily``: changing either moves
   a charge between grouping keys, so the rollup must be rebuilt and the money
   must be shown not to have moved.

   Be precise about which rollup, because the sweeping form of this claim — "no
   rollup has an ``app_id`` grain" — is simply FALSE. ``billing_metering_hourly``
   and ``billing_metering_daily`` BOTH carry an ``app_id`` column and both name
   it in their unique keys (``uq_hourly`` / ``uq_daily``, ``models/db/rollups.py``
   lines 60 and 100). Nothing moves in either of them, but for a reason that has
   nothing to do with grain: they are built from ``MeteringEvent.app_id`` — a
   different table — and this script writes to ``billing_cost_events`` and to
   nothing else. It never touches a row those two are derived from, so it cannot
   move one of their buckets.

   The rollup that IS derived from ``billing_cost_events`` is
   ``billing_cost_daily``, and that one has no ``app_id`` column and does not
   name it in ``uq_cost_daily``. Writing ``app_id`` therefore cannot change a
   single ``billing_cost_daily`` row, so there is nothing to restate.

   That is CHECKED rather than trusted, and checked on the right table:
   ``assert_rollup_has_no_app_grain`` below queries ``information_schema`` at
   startup for ``billing_cost_daily`` — deliberately NOT the two metering
   rollups, whose app grain is legitimate and permanent, so watching them would
   fail every run for no reason. The guard fails CLOSED: it raises before a
   single candidate is read, so a ``billing_cost_daily`` that has gained app
   grain stops the run instead of silently leaving the invoice source
   disagreeing with the events.
   The money invariant itself is NOT dropped — it is enforced per batch, inside
   the batch's own transaction, under a row lock on exactly the rows being
   written, and a mismatch rolls that batch back and aborts the run
   (``LOCK_BATCH_SQL`` + ``MONETARY_SQL``).

3. **No ambiguity pass — impossible by construction.**
   ``resolve_license`` must drop ambiguous ``(tenant, app)`` keys because a
   tenant can hold two active licences for one app (measured: 7 such pairs, 16
   licences), and "pick one" is guessing. That failure mode CANNOT occur here:
   ``license_id`` is the PRIMARY KEY of ``request_quota_config``, so a licence
   maps to exactly one app or to none. "Exactly one, or nothing" is satisfied by
   the schema itself rather than by a filter, which is why this backfill cannot
   mis-attribute the way a ``(tenant, app)``-keyed one could.

WHAT IS KEPT UNCHANGED FROM THE LICENCE BACKFILL
------------------------------------------------
* **Never fabricate (rule 1).** The mapping is imported from
  ``services.license_resolver`` (``get_app_map`` / ``resolve_app``) rather than
  re-implemented, so it is the same code that runs at flush time and cannot
  drift from it. A licence absent from the mirror leaves ``app_id`` NULL and is
  counted; an invented app is an invented product line and is strictly worse
  than a missing one.
* **DRY RUN by default.** Without ``--apply`` this measures and writes nothing.
* **Keyset pagination by ``id``**, so a 400k-row scan is a walk and not one
  giant result set.
* **A final audit line** with the full denominator: examined, updated, and every
  class of row left NULL together with the reason it was left.

TWO SMALL DIVERGENCES, BOTH DELIBERATE
--------------------------------------
* **Commits per batch, not once at the end.** The licence backfill resolved only
  ~3,805 of 354,380 candidates, so it could hold every pending update in memory
  and commit once. Here ~85% of candidates resolve, so the same shape would
  accumulate ~190k pending updates and one enormous transaction. Because no
  money moves and no rollup is restated, a batch is independently safe to
  commit, and the run is **resumable — for rows it actually WROTE, and only
  those**. The candidate predicate is ``app_id IS NULL``, so a stamped row drops
  out of the candidate set and an interrupted run does not redo it.

  Note the limit, because "resumable" overstates it: a DECLINED row is still
  NULL, so it stays a candidate forever. Every re-run restarts the cursor at
  ``_ZERO_UUID`` and re-reads all of them from the beginning — measured on the
  audit's dry run, 1,071 ``licence_not_in_mirror`` declines that no number of
  re-runs will ever shrink (only a licences→mirror re-sync can). Resumption
  skips work already DONE; it does not skip work already REFUSED, so the SCAN
  cost of a re-run is the whole remaining candidate set, not the remainder of
  the previous pass.
* **No ``cost_source`` marker.** The licence backfill appends ``+backfilled``.
  Doing that here would change ``cost_source``, which IS part of
  ``uq_cost_daily`` — it would drag the rollup restatement back in for a purely
  cosmetic label, and it would multiply an already-suffixed value
  (``...+resolved``) into combinations nobody filters on. This matches the
  flusher, which deliberately does not suffix ``cost_source`` for app
  derivation either (see ``cost_flusher``): the derived-vs-asserted distinction
  is carried by ``billing_app_resolved_total``. Historically the distinction is
  unambiguous anyway — every ``app_id`` on a row older than this run is derived,
  because 100% of them were NULL before it.

USAGE (in the billing pod, or a Job cloned from the billing deployment env)::

    python scripts/backfill_app_attribution.py              # DRY RUN — measure only
    python scripts/backfill_app_attribution.py --apply      # derive and write
    python scripts/backfill_app_attribution.py --apply --batch 5000

⚠️ PRECONDITION — THE IMAGE MUST ALREADY CARRY PR #109.
``get_app_map`` and ``resolve_app`` were added to ``services.license_resolver``
by #109 (*derive app_id from the event's own licence*). The deployed billing
image at the time of writing predates it, so the import at the top of this file
raises ``ImportError`` before a single row is read and the usage above cannot
run as printed. That direction is safe — it cannot half-run — but it is not
obvious from the traceback, so check first::

    python -c "from services.license_resolver import get_app_map, resolve_app"

If that raises, roll the billing deployment onto an image built from a commit
containing #109 and re-check. Do NOT work around it by copying the two
functions into this file: rule 1 below depends on this being the same mapping
code the flusher runs, and a copy is free to drift from it.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from collections import Counter
from typing import Any

import structlog
from sqlalchemy import text

from db import get_session_factory, init_db  # type: ignore[import-not-found]
from logging_config import configure_logging  # type: ignore[import-not-found]
from models.db import NULL_APP_ID  # type: ignore[import-not-found]
from services.license_resolver import (  # type: ignore[import-not-found]
    get_app_map,
    resolve_app,
)
from settings import get_settings  # type: ignore[import-not-found]

logger = structlog.get_logger(__name__)

# Keyset cursor seed, typed as `uuid.UUID` so psycopg binds it to the native
# uuid OID and `id > :after` needs no inference step — the type is explicit at
# the call site rather than decided by the server.
#
# This is a preference, NOT a fix. The sibling `backfill_license_attribution.py`
# seeds the same cursor with the string form and is equally correct: measured
# in-pod 2026-09-02, psycopg3 sends an untyped `str` as `unknown` and postgres
# resolves it to `uuid` from the other operand, so both scans page identically.
# An earlier version of this comment claimed the sibling was broken by that
# choice. It is not, and nothing here should be read as a reason to change it.
_ZERO_UUID = uuid.UUID("00000000-0000-0000-0000-000000000000")

# The candidate set.
#
# `license_id IS NOT NULL` is the whole premise: with no licence there is
# nothing to derive an app FROM, and those rows are an identity problem at the
# emitter that no billing-side change can fix. They are counted in the census
# below so the audit line keeps an honest denominator, but they are never
# scanned here — that would be ~209k rows (measured 2026-09-02; a moving window)
# of guaranteed declines, and they are the whole reason a complete run still
# leaves about half the table NULL. See "THE HONEST CEILING" above.
#
# Note what is NOT here: no `excluded_from_billing = false` filter. The licence
# backfill needs one because it feeds an invoice aggregate and a parked row must
# never re-enter it. `app_id` feeds no aggregate at all, and a parked row's app
# is still a true fact about that spend — omitting it would make per-app audits
# of PARKED spend impossible, which is precisely when someone needs them. Parked
# rows are counted separately (`updated_parked`) so the choice stays visible.
CANDIDATES_SQL = """
SELECT id, created_at, license_id, service_key, excluded_from_billing
  FROM billing_cost_events
 WHERE app_id IS NULL
   AND license_id IS NOT NULL
   AND id > :after
 ORDER BY id
 LIMIT :batch
"""

# `created_at` is in the predicate for partition pruning, not for correctness:
# billing_cost_events is RANGE-partitioned on it and its PK is (id, created_at),
# so without it every UPDATE probes every partition.
# `app_id IS NULL` is the idempotency guard — a concurrent flusher write always
# wins, and a re-run can never overwrite an app someone else established.
ATTRIBUTE_SQL = """
UPDATE billing_cost_events
   SET app_id = :app_id
 WHERE id = :id
   AND created_at = :created_at
   AND app_id IS NULL
"""

# Lock exactly the batch's own rows BEFORE the monetary snapshot is taken.
#
# This is what makes the guard below mean anything. Without it the two snapshots
# are two separate statements under READ COMMITTED, each taking its own
# snapshot, so ANY concurrent commit that touches one of these rows in between —
# the aggregator stamping `cost_usd`/`pricing_id` on a freshly flushed row, the
# duplicate parker setting `excluded_from_billing` — appears as "money moved"
# and aborts a run that did nothing wrong. That is a guard that cannot catch its
# stated target but CAN halt a good run on unrelated live traffic, which is
# worse than no guard at all. Holding the lock closes the window: from here to
# COMMIT the only writer that can change one of these rows is us.
#
# `FOR NO KEY UPDATE`, not `FOR UPDATE`: it is exactly the lock strength our own
# UPDATE takes (`app_id` is in no key), so it blocks precisely what the UPDATE
# would have blocked and nothing more — an FK check on another table can still
# take its KEY SHARE lock and is not stalled behind the backfill. `ORDER BY id`
# so every batch acquires locks in the same order.
#
# `app_id IS NULL` is returned PER ROW rather than merely counted: that subset
# is the only thing ATTRIBUTE_SQL is allowed to change (it carries the same
# predicate), and it is the denominator VERIFY_SQL must be given — see
# `apply_batch`.
LOCK_BATCH_SQL = """
SELECT id, (app_id IS NULL) AS attributable
  FROM billing_cost_events
 WHERE id = ANY(:ids)
 ORDER BY id
   FOR NO KEY UPDATE
"""

# The money control: every column that is or feeds a charge, summed over exactly
# the rows one batch touches, before and after that batch's UPDATE, inside that
# batch's transaction and UNDER THE ROW LOCKS taken above. Because the rows are
# locked, any difference is attributable to OUR write and to nothing else in the
# estate.
#
# WHAT IT ACTUALLY CATCHES, stated plainly — a guard whose target is
# misdescribed reads as protection nobody has. Today it cannot fire from normal
# operation: `ATTRIBUTE_SQL` SETs `app_id` and only `app_id`, and there are zero
# user triggers on `billing_cost_events` or any of its partitions (measured
# 2026-09-02: `pg_trigger` WHERE NOT `tgisinternal` returns 0). It is a
# REGRESSION tripwire for the two ways that stops being true:
#
#   * someone adds a column to ATTRIBUTE_SQL's SET list, or widens its WHERE so
#     it writes rows this batch never resolved;
#   * someone installs a BEFORE/AFTER UPDATE trigger on the table — a repricing
#     hook, an audit trigger that rewrites a column.
#
# In either case the batch is rolled back and the run aborts on the FIRST batch,
# before a second can compound it. That path is exercised rather than asserted:
# `tests/test_backfill_app_attribution.py` installs such a trigger and proves
# both the raise and the rollback. A money guard nothing has ever tripped is a
# guard nobody has shown to work.
MONETARY_SQL = """
SELECT count(*),
       coalesce(sum(cost_usd), 0),
       coalesce(sum(cost_usd_hint), 0),
       coalesce(sum(input_tokens), 0),
       coalesce(sum(output_tokens), 0),
       coalesce(sum(cache_creation_input_tokens), 0),
       coalesce(sum(cache_read_input_tokens), 0),
       count(*) FILTER (WHERE excluded_from_billing)
  FROM billing_cost_events
 WHERE id = ANY(:ids)
"""

# Truth for "did the write land", independent of driver rowcount semantics under
# executemany. A number is not verified by the statement that produced it.
#
# ⚠️ Fed the LOCKED-AND-STILL-NULL subset of the batch, never the whole batch.
# Over the whole batch this counts every row that has an `app_id` after the
# UPDATE — including rows the concurrent flusher had already stamped between the
# candidate scan and our lock, which ATTRIBUTE_SQL then correctly skipped. Those
# are someone else's writes, and counting them here inflated the run's `updated`
# total with work this script did not do. Scoped to the rows observed NULL under
# our own row lock, the count is exactly what we changed, because nothing else
# can write those rows while the lock is held.
VERIFY_SQL = """
SELECT count(*)
  FROM billing_cost_events
 WHERE id = ANY(:ids)
   AND app_id IS NOT NULL
"""

# The denominator for the audit line, so "updated N" is never quoted without the
# population it came out of.
CENSUS_SQL = """
SELECT count(*) FILTER (WHERE app_id IS NOT NULL)                          AS has_app,
       count(*) FILTER (WHERE app_id IS NULL AND license_id IS NOT NULL)   AS null_app_with_licence,
       count(*) FILTER (WHERE app_id IS NULL AND license_id IS NULL)       AS null_app_no_licence
  FROM billing_cost_events
"""

# The structural proof that omission 2 above is safe. Asks postgres, not memory.
ROLLUP_GRAIN_SQL = """
SELECT count(*)
  FROM information_schema.columns
 WHERE table_name = 'billing_cost_daily'
   AND column_name = 'app_id'
"""


class MoneyMovedError(RuntimeError):
    """A batch changed something monetary. The batch is rolled back, run aborts."""


async def assert_rollup_has_no_app_grain(session: Any) -> None:
    """Refuse to run if ``billing_cost_daily`` has grown an ``app_id`` column.

    This is the entire justification for having no restatement step, so it is
    checked against the live schema rather than asserted in a comment. Today the
    rollup carries no app grain, which is why writing ``app_id`` onto an event
    cannot change one row of the invoice source. The day someone adds it, this
    script becomes silently wrong — every backfilled row would belong in a
    different rollup bucket and nothing would rebuild them. Fail here instead.
    """
    (found,) = (await session.execute(text(ROLLUP_GRAIN_SQL))).one()
    if found:
        raise RuntimeError(
            "billing_cost_daily now has an app_id column. This backfill has NO "
            "restatement step because that column did not exist; writing app_id "
            "would now leave the rollup disagreeing with the events. Add a "
            "restatement (see backfill_license_attribution.py) before running."
        )


async def census(session: Any) -> dict[str, int]:
    """Population counts for the audit line, taken BEFORE anything is written."""
    row = (await session.execute(text(CENSUS_SQL))).one()
    return {
        "has_app": row[0],
        "null_app_with_licence": row[1],
        "null_app_no_licence": row[2],
    }


async def apply_batch(session: Any, updates: list[dict[str, Any]]) -> int:
    """Write one batch, prove no money moved, commit. Returns rows WE stamped.

    Ordering is the whole design and is not rearrangeable:

    1. Lock this batch's own rows (``LOCK_BATCH_SQL``) and record which of them
       are still ``app_id IS NULL``. The lock makes every later read in this
       function immune to concurrent commits on these rows; the NULL subset is
       the only thing our UPDATE may change.
    2. Snapshot the monetary columns, UPDATE, snapshot again. Scoped to this
       batch's ids and taken under the lock, so a difference means OUR write
       changed money — not that the flusher inserted somewhere else, and not
       that the aggregator repriced one of these rows mid-transaction. A global
       before/after would false-alarm on all of that while proving nothing about
       ours; an unlocked per-batch one still false-alarms on the second case.
    3. Verify against the database over the NULL subset only, then commit.

    The returned count is deliberately the number of rows this run stamped, not
    the number it intended to. The two differ when a concurrent flusher wins a
    race for the same row between the candidate scan and step 1, and reporting
    the intended figure would credit the backfill with someone else's write.
    """
    ids = [u["id"] for u in updates]

    locked = (await session.execute(text(LOCK_BATCH_SQL), {"ids": ids})).all()
    attributable = [row_id for row_id, is_null in locked if is_null]

    before = (await session.execute(text(MONETARY_SQL), {"ids": ids})).one()
    await session.execute(text(ATTRIBUTE_SQL), updates)
    after = (await session.execute(text(MONETARY_SQL), {"ids": ids})).one()

    if tuple(before) != tuple(after):
        await session.rollback()
        raise MoneyMovedError(
            f"batch of {len(ids)} row(s) changed a monetary column: "
            f"before={tuple(before)} after={tuple(after)}"
        )

    if not attributable:
        # Every row in this batch was stamped by someone else between the scan
        # and the lock. Nothing of ours to verify; commit to release the locks.
        # (VERIFY_SQL is not run with an empty array: an empty Python list gives
        # psycopg no element to infer the array's element type from.)
        stamped = 0
    else:
        (stamped,) = (await session.execute(text(VERIFY_SQL), {"ids": attributable})).one()
    await session.commit()

    if len(attributable) != len(updates):
        # Not fatal and not a rollback: the gap is a concurrent flusher writing
        # an app_id first, which is a correct outcome and the reason
        # ATTRIBUTE_SQL carries `app_id IS NULL`. Logged with both numbers so
        # the run is never reported as the larger of the two.
        logger.info(
            "app_backfill_batch_raced",
            intended=len(updates),
            attributable=len(attributable),
        )
    if stamped != len(attributable):
        # This one SHOULD be impossible: these rows were NULL under our lock and
        # only we can write them. If it ever fires, the lock assumption above is
        # wrong and the run's `updated` total cannot be trusted.
        logger.warning(
            "app_backfill_batch_short",
            attributable=len(attributable),
            stamped=stamped,
        )
    return int(stamped)


async def _run(apply: bool, batch: int) -> int:
    settings = get_settings()
    configure_logging(settings)

    await init_db(settings)
    factory = get_session_factory()

    counts: Counter[str] = Counter()
    per_service: Counter[str] = Counter()
    updated = 0

    async with factory() as session:
        await assert_rollup_has_no_app_grain(session)

        population = await census(session)
        logger.info("app_backfill_census", mode="apply" if apply else "dry-run", **population)

        app_map = await get_app_map(session)
        if not app_map:
            # Fail CLOSED and loudly, for the same reason the licence backfill
            # refuses an empty licence list: with an empty map every row declines
            # and the run reports "0 updated" as though the work were done. An
            # empty mirror is "I could not see anything", not "nothing to do".
            logger.error("app_backfill_empty_map")
            print(
                "ABORT: request_quota_config yielded ZERO licence->app mappings. Every row "
                "would decline and the audit line would read like a clean no-op."
            )
            return 1
        logger.info("app_backfill_map_built", licences_with_app=len(app_map))

        after = _ZERO_UUID
        while True:
            rows = (
                await session.execute(text(CANDIDATES_SQL), {"after": after, "batch": batch})
            ).all()
            if not rows:
                break

            pending: list[dict[str, Any]] = []
            for row_id, created_at, license_id, service_key, excluded in rows:
                after = row_id
                counts["examined"] += 1

                # NOTE — there is deliberately NO "sentinel licence" branch here
                # and no `declined_sentinel_licence` counter. Neither of the two
                # values such a branch would test for can reach this loop:
                #   * `None` is excluded by CANDIDATES_SQL (`license_id IS NOT
                #     NULL`), so it never appears in `rows`;
                #   * the all-zero `NULL_LICENSE_ID` is a ROLLUP-table sentinel,
                #     standing in for NULL where a NOT NULL column needs a
                #     value. It is never written onto an EVENT — cost_flusher
                #     keeps `None` on the raw CostEvent and substitutes the
                #     sentinel only for the rollup rows (see its comment on the
                #     `"license_id": license_id` insert row) — and prod carries
                #     0 such events (measured 2026-09-02).
                # It was reported as `declined_sentinel_licence=0` on every run,
                # and a counter that cannot be non-zero is not measured absence,
                # it is a claim to have checked something that was never checked.
                # If the sentinel ever did reach an event, it is looked up in the
                # mirror like any other licence and declines through
                # `licence_not_in_mirror` below, leaving app_id NULL and counted
                # — rule 1 holds either way, with or without the branch.
                derived = resolve_app(license_id=license_id, app_map=app_map)
                if derived is None:
                    # A licence the mirror does not carry, or carries with a NULL
                    # app_id (synced before migration 0014, or the upstream
                    # omitted it). Rule 1: leave it NULL and keep counting it.
                    counts["declined_licence_not_in_mirror"] += 1
                    continue

                if derived == NULL_APP_ID:
                    # The rollup tables use this sentinel in place of NULL because
                    # NULL breaks their ON CONFLICT keys. On an EVENT it would be
                    # a fabricated app id that looks real to every downstream
                    # reader. Events use a true NULL; decline it.
                    #
                    # Unlike the licence sentinel above, this one is KEPT even
                    # though it also reads 0 today (measured 2026-09-02: 0 of the
                    # 190 `request_quota_config` rows carry it; 26 carry NULL and
                    # are excluded from the map upstream). The difference is that
                    # nothing forbids it: `request_quota_config.app_id` is a
                    # plain nullable uuid mirrored from the licences service, so
                    # one upstream row spelling "no app" as the sentinel is a
                    # value the map can genuinely carry. This counter measures a
                    # real possibility; the removed one measured nothing.
                    counts["declined_sentinel_app"] += 1
                    continue

                counts["resolved"] += 1
                per_service[service_key or "unknown"] += 1
                if excluded:
                    counts["resolved_parked"] += 1
                pending.append({"id": row_id, "created_at": created_at, "app_id": derived})

            if apply and pending:
                written = await apply_batch(session, pending)
                updated += written
                logger.info(
                    "app_backfill_batch_committed",
                    written=written,
                    examined_so_far=counts["examined"],
                    updated_so_far=updated,
                )

        left_null = {
            "no_licence_on_event": population["null_app_no_licence"],
            "licence_not_in_mirror": counts["declined_licence_not_in_mirror"],
            "sentinel_app": counts["declined_sentinel_app"],
        }
        if not apply:
            # In a dry run nothing was written, so every resolvable row is also
            # still NULL. Say so rather than reporting it as attributed.
            left_null["resolvable_but_dry_run"] = counts["resolved"]

        logger.info(
            "app_backfill_complete",
            mode="apply" if apply else "dry-run",
            examined=counts["examined"],
            resolved=counts["resolved"],
            updated=updated,
            left_null=left_null,
            by_service_key=dict(per_service),
        )

        print("\n  audit: " + " ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        print(
            "  resolved by service_key: "
            + (" ".join(f"{k}={v}" for k, v in sorted(per_service.items())) or "(none)")
        )
        print(
            f"  population: {population['has_app']} row(s) already had an app, "
            f"{population['null_app_with_licence']} NULL-with-licence (the candidates), "
            f"{population['null_app_no_licence']} NULL-with-no-licence (unreachable here)"
        )

        if not apply:
            print(
                f"\nDRY RUN. Nothing was written. Examined {counts['examined']} candidate "
                f"row(s); {counts['resolved']} would be attributed "
                f"({counts['resolved_parked']} of those parked). "
                f"Still NULL after this run would be {sum(left_null.values())} row(s): "
                + ", ".join(f"{k}={v}" for k, v in left_null.items())
                + ".\nRe-run with --apply."
            )
            return 0

        print(
            f"\nExamined {counts['examined']} candidate row(s); attributed an app to {updated} "
            f"of them ({counts['resolved_parked']} of those parked). "
            "Left NULL: " + ", ".join(f"{k}={v}" for k, v in left_null.items()) + ".\n"
            "  invariant HELD per batch: with each batch's own rows locked FOR NO KEY "
            "UPDATE, no cost, token or exclusion column changed across the UPDATE — so "
            "nothing but app_id was written, and nothing else could have written these "
            "rows meanwhile. billing_cost_daily carries no app grain (checked against "
            "information_schema at startup), so no rollup was restated."
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write app_id. Without it this only measures.",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=1000,
        help=(
            "Rows per read/write chunk (default 1000). ~195k candidates (measured "
            "2026-09-02; a moving window) is a scan, not a one-shot; each chunk is its "
            "own transaction and a re-run skips rows already stamped."
        ),
    )
    args = parser.parse_args()
    if args.batch < 1:
        parser.error("--batch must be >= 1")
    try:
        return asyncio.run(_run(args.apply, args.batch))
    except MoneyMovedError as exc:
        print(f"\nABORTED AND ROLLED BACK — {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
