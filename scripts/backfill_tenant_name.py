#!/usr/bin/env python3
"""Repair historical ``tenant_name`` rows that hold the uuid, or nothing at all.

WHY THIS EXISTS — A DEPLOY CUTOVER, NOT A RACE
----------------------------------------------
``services._parsers.stored_tenant_name`` resolves the display name at WRITE time
and has done since commit ``ada5eef`` ("resolve tenant_name at write time",
billing-service #138), merged **2026-09-06 09:20Z**. It cannot reach backwards:
nothing in the estate UPDATEs ``tenant_name`` after the row is written, and a
later registry refresh cannot revisit a written row (the "BOOT WINDOW, known and
unsolved here" paragraph in that helper's docstring is exactly this gap).

FIRST reading on the prod billing DB, 2026-09-09: ``billing_cost_events`` holds
186,878 rows, **72,277 of them (38.7%) carrying the bare tenant uuid as
``tenant_name``** and 975 carrying ``''``. Sixteen tenant_ids appear under more
than one name, so any report GROUPed by tenant name silently undercounts. The
same split exists in ``billing_metering_events`` and ``billing_cost_daily``.
⚠️ These are one instant, not a standing fact — the SECOND reading, hours later
the same day, is in the next section and it does not agree with this one.

⛔ ONE HALF OF THE DAMAGE IS CLOSED. THE OTHER HALF IS STILL GROWING.
--------------------------------------------------------------------
An earlier version of this docstring said "the damaged set is CLOSED — it is
history, it is not still growing". That is TRUE of the impersonation half and
FALSE of the absence half, and the difference decides whether ``--apply`` is
safe to run right now.

* **The uuid-as-name half IS closed.** Every bare-uuid population ENDS at
  2026-09-06 09:24-09:28Z and every resolved-name population BEGINS in those
  same minutes, with zero interleaving; the boundary is ``ada5eef``. Nothing
  UPDATEs ``tenant_name`` after a row is written, so that population is history.
  Measured 187,180-row snapshot: 72,277 rows, unchanged across both measurements
  below.
* **The ``''`` half is NOT closed — it is still being produced.** Measured twice
  on ``billing_cost_events``: **975 ``''`` rows at 186,878 total**, then
  **1,005 ``''`` rows at 187,180 total** — +30 in +302 rows, and the newest
  ``''`` row carried ``created_at = 2026-09-09 14:26:29Z``, minutes before the
  second reading. The producer is the archived-tenant defect on the LIVE write
  path (``services.tenant_names`` asked ``/internal/tenants`` without
  ``include_archived``, so 17 of 76 tenants resolved to ``''`` forever). That
  fix is in this same change and is **not deployed yet**.

⛔ **PRECONDITION FOR ``--apply``: the ``services/tenant_names.py``
``include_archived`` fix MUST BE DEPLOYED AND SERVING FIRST.** Until it is,
every archived tenant this script repairs is being re-damaged behind it by the
running pods, so the run repairs rows that reappear, the after-count will not
equal before-minus-repaired, and the batch loop is chasing a moving target.
Repairing a population whose producer is still running is not a backfill, it is
a race. Deploy the write-path fix, confirm the ``''`` count has stopped rising,
then ``--apply``.

Two independent guards make that failure LOUD instead of endless — see
"THE BATCH LOOP TERMINATES" below.

WHAT IS REPAIRED, AND WHAT IS DELIBERATELY LEFT ALONE
-----------------------------------------------------
Only two shapes are touched, and both are non-answers this codebase already
classifies as such:

1. **The impersonation** — ``lower(btrim(tenant_name)) = tenant_id::text``. A
   uuid is non-empty and distinct per tenant, so it satisfies every downstream
   truthiness guard and READS AS DATA. ``stored_tenant_name`` calls it "worse
   than an absence" for that reason.
2. **The absence** — ``btrim(tenant_name) = ''``. The canonical "the registry
   could not resolve a name" marker; the column is ``NOT NULL`` on all eight
   tables so ``''``, not NULL, is what an absence looks like.

Everything else is left exactly as it is. A row carrying ``"unknown"`` or any
other emitter-chosen sentinel keeps it verbatim — ``stored_tenant_name``'s
"EVERY OTHER SENTINEL IS PRESERVED, NOT COERCED" rule applies here too, and
rewriting one would move the laundering rather than end it. A row carrying a
real-but-stale spelling of a name keeps that too: it is a fact about what the
registry said at the time, and this script has no basis for overruling it.

"NO NAME EXISTS" IS NOT "LOOKUP FAILED" — HOW THEY ARE TOLD APART
------------------------------------------------------------------
⛔ These two must never be treated alike: the first licenses leaving a row at
``''`` forever, the second licenses nothing at all.

The discriminator is the HTTP exchange, not the contents of the map:

* **Lookup failed** — ``licenses_url`` unset, a transport error, a non-200, a
  body that is not a list, or a list with zero usable rows. Every one of these
  RAISES out of :func:`fetch_tenant_names` and the script writes nothing,
  anywhere. It does **not** degrade to "then nothing resolves", because that
  degradation is indistinguishable, in the audit line, from a genuinely clean
  run: both print "0 repaired".
* **No name exists** — the licences service ANSWERED 200 with a populated
  tenant list, and this tenant_id is simply not in it. That is a positive fact
  about the registry, and the honest repair is NONE: the rows keep ``''`` and
  are reported under ``no_registry_row`` so the operator can see how much of the
  damaged population is unrepairable rather than unrepaired. Measured 2026-09-09,
  ``6c6a0da6-…`` and ``ad976f5b-…`` are exactly this — billing has cost rows for
  them and ``licenses.tenants`` has no row at all. **Do not invent a name.**

Note the deliberate use of a fail-LOUD fetch here rather than
``services.tenant_names.get_tenant_info``. That helper is fail-SOFT on purpose —
its callers render a dashboard label and must degrade rather than 500 — and
``{}`` there means "401, timeout, bad shape, or genuinely empty" all at once.
A write path may not accept an answer that ambiguous.

ARCHIVED TENANTS ARE INCLUDED, AND MUST BE
-------------------------------------------
``GET /internal/tenants`` EXCLUDES archived tenants by default; 17 of the 76
tenants are archived (measured 2026-09-09). Those tenants own historical cost
rows — that is licences' own stated reason for the ``include_archived`` flag —
so omitting them would leave the largest and oldest part of the damaged
population permanently unnameable. The same flag is now passed by
``services.tenant_names`` on the live path.

THE BATCH LOOP TERMINATES — TWO GUARDS, BECAUSE ONE DOES NOT COVER EVERY TABLE
------------------------------------------------------------------------------
``_repair_table`` re-SELECTs the damage predicate every pass (it cannot page by
OFFSET; the UPDATE removes rows from the predicate). A writer still producing
damaged rows for the same tenant would therefore feed the loop forever.

1. **A time clamp, where the table has a creation timestamp.** ``run_started_at``
   is captured ONCE at start and every statement — census, select, update — is
   restricted to ``created_at < :run_started_at``, so the run targets a set fixed
   at that instant and rows written behind it are out of scope by construction.
   Measured 2026-09-09, ``created_at`` exists on exactly three of the eight
   tables: ``billing_cost_events`` (187,180 rows — 99% of the damage and the one
   that is still growing), ``billing_metering_events`` (37,563) and
   ``token_budgets`` (404). The other five have only bucket/period columns
   (``bucket_date``, ``bucket_hour``, ``period_start``) or a MUTABLE
   ``updated_at``; none of those is a row-creation time, and clamping on one
   would silently exclude a legitimately damaged row. They are left unclamped
   ON PURPOSE and rely on guard 2.
2. **An iteration cap on every table, clamped or not.** The census already knows
   how many damaged rows the tenant has, so the loop can never legitimately need
   more than ``ceil(census / batch)`` write passes. Exceeding that budget raises
   :class:`BackfillDidNotConvergeError` — named, non-zero exit, and the message says
   what it means: rows are arriving faster than they are repaired, which is the
   undeployed-writer-fix precondition above being violated. Spinning silently is
   the failure this replaces.

SAFETY PROPERTIES
-----------------
* **Dry run by DEFAULT.** ``--apply`` is required to write anything.
* **Batched.** Ids are selected ``LIMIT``-ed and updated in chunks with an
  explicit commit per chunk. 72k+ rows on a live prod database shared with other
  services is never one unbounded UPDATE.
* **Idempotent.** The repair predicate stops matching a row the moment it is
  repaired, so a second run reports only the genuinely unrepairable rows and
  writes nothing. A resolved name that would itself re-match the predicate
  (empty, or equal to the uuid) is refused up front by :func:`_usable_name`;
  that plus the two guards above is what makes the batch loop terminate.
* **Auditable.** Every table prints a before count, a repaired count and an
  after count, all three over the SAME clamped set. ``after == before -
  repaired`` is the check; anything else means something other than this script
  moved rows. Damage that ARRIVED after ``run_started_at`` is reported
  separately as ``arrived_since_start`` rather than being folded into that
  arithmetic — it is a fact about the writer, not about this run.
* **No monetary column is touched, and no unique key contains ``tenant_name``**
  (verified against ``uq_hourly`` / ``uq_daily`` / ``uq_cost_daily`` /
  ``uq_token_usage`` and ``tenant_config``'s unique ``tenant_id`` on 2026-09-09),
  so this cannot merge, split or collide rows. It is a display column repair.

USAGE (in the billing pod, or a Job cloned from the billing deployment env)::

    python scripts/backfill_tenant_name.py            # DRY RUN — measure only
    python scripts/backfill_tenant_name.py --apply    # repair — READ THE
                                                      # PRECONDITION ABOVE FIRST
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from typing import Any

import httpx
from sqlalchemy import text

from db import get_session_factory, init_db  # type: ignore[import-not-found]
from service_auth import ServiceTokenProvider  # type: ignore[import-not-found]
from settings import get_settings  # type: ignore[import-not-found]

#: Every table carrying a ``(tenant_id, tenant_name)`` pair. Verified against
#: ``information_schema`` on the prod billing DB 2026-09-09: all eight exist,
#: with ``tenant_id uuid NOT NULL`` and ``tenant_name character varying NOT
#: NULL``. :func:`_existing_tables` re-verifies at RUN time and drops anything
#: missing rather than aborting the whole run on one absent table — a schema this
#: script cannot see is a reason to skip that table, not to leave the other seven
#: broken.
#:
#: ``audit_log`` is deliberately NOT here. Its ``tenant_name`` is nullable
#: (mirroring its nullable ``tenant_id``), it is an append-only record of what
#: was observed at the time, and rewriting an audit row to say something other
#: than what was recorded is the one edit an audit log must never accept.
TABLES: tuple[str, ...] = (
    "billing_cost_events",
    "billing_metering_events",
    "billing_cost_daily",
    "billing_metering_hourly",
    "billing_metering_daily",
    "token_usage",
    "token_budgets",
    "tenant_config",
)

#: Guards the f-string interpolation below. Table names come from the literal
#: tuple above and never from input, but the assertion is what keeps that true
#: for the next person who adds an entry.
_SAFE_IDENT = re.compile(r"\A[a-z_][a-z0-9_]*\Z")

#: THE repair predicate, in one place so the count, the select and the update
#: cannot drift apart — a backfill whose "before" query and "write" query
#: disagree reports a number about a population it did not touch.
#:
#: ``lower(btrim(...))`` on the left of the impersonation test: Postgres renders
#: ``uuid::text`` lowercase, but the stored string came off the wire from an
#: emitter and may not be. ``btrim`` also folds a whitespace-only value into the
#: absence case, where it belongs.
DAMAGED = "(lower(btrim(tenant_name)) = tenant_id::text OR btrim(tenant_name) = '')"

#: The ONE column accepted as a clamp. It must be a ROW-CREATION time, and it
#: must be immutable after insert, or clamping on it either excludes a
#: legitimately damaged row or fails to fix the set at all.
#:
#: Measured on the prod billing DB 2026-09-09 (``information_schema.columns``,
#: date/timestamp columns per table):
#:   billing_cost_events      created_at                     <- clamped
#:   billing_metering_events  created_at                     <- clamped
#:   token_budgets            created_at, updated_at, ...    <- clamped
#:   billing_cost_daily       bucket_date                    <- NOT a row time
#:   billing_metering_daily   bucket_date                    <- NOT a row time
#:   billing_metering_hourly  bucket_hour                    <- NOT a row time
#:   token_usage              period_start                   <- NOT a row time
#:   tenant_config            updated_at                     <- MUTABLE
#: The five unclamped tables hold 2,843 rows between them versus 224,743 in the
#: three clamped ones, and rely on the iteration cap instead. See the module
#: docstring, "THE BATCH LOOP TERMINATES".
_CLAMP_COLUMN = "created_at"

#: Substituted for the clamp when the table has no :data:`_CLAMP_COLUMN`.
#: ``TRUE`` and not an omitted conjunct so every statement keeps one shape.
_NO_CLAMP = "TRUE"

#: Damaged rows per tenant, over the CLAMPED set — the same set the select and
#: the update work on, so ``after == before - repaired`` is arithmetic about one
#: population rather than a comparison across two.
_CENSUS_SQL = f"""
SELECT tenant_id::text, count(*)
  FROM {{table}}
 WHERE {{clamp}}
   AND {DAMAGED}
 GROUP BY 1
"""

#: Damage that arrived AFTER ``run_started_at``. Reported, never repaired and
#: never folded into the before/after arithmetic: a non-zero value here is the
#: writer still producing, which is the ``--apply`` precondition being violated.
_ARRIVALS_SQL = f"""
SELECT count(*)
  FROM {{table}}
 WHERE {_CLAMP_COLUMN} >= :cutoff
   AND {DAMAGED}
"""

#: One batch of ids to repair. Re-selected each pass rather than paged with an
#: offset: the UPDATE removes rows from this predicate, so an offset would skip
#: exactly as many rows as it just fixed.
_SELECT_BATCH_SQL = f"""
SELECT id
  FROM {{table}}
 WHERE tenant_id = CAST(:tid AS uuid)
   AND {{clamp}}
   AND {DAMAGED}
 LIMIT :batch
"""

#: The write. Re-asserts the tenant, the clamp AND the damage predicate rather
#: than trusting the id list alone — ``billing_cost_events`` is RANGE-partitioned,
#: so its primary key is ``(id, created_at)`` and ``id`` carries no uniqueness
#: guarantee of its own across partitions. Re-stating the predicate also makes
#: the statement safe against a concurrent writer and keeps it idempotent.
_UPDATE_BATCH_SQL = f"""
UPDATE {{table}}
   SET tenant_name = :name
 WHERE tenant_id = CAST(:tid AS uuid)
   AND id = ANY(CAST(:ids AS uuid[]))
   AND {{clamp}}
   AND {DAMAGED}
"""

#: ⚠️ The ``table_schema`` filter is NOT optional, and it is NOT ``'public'``.
#:
#: ``information_schema.columns`` spans EVERY schema in the database. Without a
#: filter, a table name present in two schemas returns 4 instead of 2, ``== 2``
#: is false, and the table is SILENTLY SKIPPED while the run still exits 0 — an
#: absence that reads as a clean no-op. Prod measures ``public|2`` for all eight
#: tables today (2026-09-09), so this is latent rather than firing; it is armed
#: by the second schema nobody announces.
#:
#: ``current_schema()`` and not the literal ``'public'`` because every other
#: statement in this file names its table UNQUALIFIED — so the schema this check
#: must ask about is, by definition, the one the same connection would resolve
#: those names in. Hardcoding ``'public'`` would make the existence check and the
#: repair disagree the moment anyone runs with a ``search_path`` (which the test
#: suite does: ``conftest.TEST_SCHEMA``). In prod ``current_schema()`` IS
#: ``public``.
_TABLE_EXISTS_SQL = """
SELECT count(*)
  FROM information_schema.columns
 WHERE table_schema = current_schema()
   AND table_name = :table
   AND column_name IN ('tenant_id', 'tenant_name')
"""

#: Does this table carry :data:`_CLAMP_COLUMN`? Same schema filter, same reason.
_CLAMP_COLUMN_EXISTS_SQL = """
SELECT count(*)
  FROM information_schema.columns
 WHERE table_schema = current_schema()
   AND table_name = :table
   AND column_name = :column
"""


class BackfillDidNotConvergeError(RuntimeError):
    """The batch loop spent its whole budget without draining the tenant.

    Named, not a bare ``RuntimeError``, because the diagnosis is specific: the
    census said N damaged rows and the loop was given ``ceil(N / batch)`` write
    passes to clear them, so overrunning means rows are being CREATED into the
    predicate faster than they are repaired. On an unclamped table that is the
    ``--apply`` precondition in the module docstring being violated — the
    ``services/tenant_names.py`` ``include_archived`` fix is not deployed and
    the live pods are still writing ``''``. Deploy it, confirm the ``''`` count
    has stopped rising, and re-run. This replaces a bare ``while True`` that
    would have spun on the prod primary instead of saying so.
    """


def _check_ident(table: str) -> str:
    if not _SAFE_IDENT.match(table):  # pragma: no cover - defensive
        raise ValueError(f"refusing to interpolate {table!r} as an identifier")
    return table


def _max_passes(census_rows: int, batch: int) -> int:
    """Write-pass budget for one tenant in one table.

    ``ceil(census / batch)`` is exactly the number of full batches the census
    justifies. No slack is added: any extra pass is a row that was not in the
    census, which is the thing this is here to catch.
    """
    return max(1, -(-census_rows // batch))


async def fetch_tenant_names(settings: Any) -> dict[str, str]:
    """``{tenant_id_lowercase: name}`` from licences, ARCHIVED INCLUDED.

    Fail-CLOSED and loud, deliberately unlike
    ``services.tenant_names.get_tenant_info``. See the module docstring: the
    whole point of this function is that a caller can tell "the registry has no
    row for this tenant" (a fact, returned as an absent key in a POPULATED map)
    from "I could not ask" (raises, and nothing is written). A fail-soft ``{}``
    collapses those two into one value and would turn an outage into an audit
    line reading "0 repaired", which is what a clean run looks like.
    """
    if not settings.licenses_url:
        raise RuntimeError("licenses_url is not configured — cannot resolve any tenant name")

    provider = ServiceTokenProvider(settings)
    headers: dict[str, str] = {}
    token = await provider.get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{settings.licenses_url}/internal/tenants",
            # See services.tenant_names._TENANT_QUERY. 17 of 76 tenants are
            # archived and the endpoint hides them by default; without this the
            # oldest rows — the ones a backfill exists for — stay unnameable.
            params={"include_archived": "true"},
            headers=headers,
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"licences /internal/tenants returned {resp.status_code} — refusing to run. "
            "Every tenant would look unresolvable and the run would report a clean no-op."
        )

    payload = resp.json()
    if not isinstance(payload, list):
        raise RuntimeError(
            f"licences /internal/tenants returned a {type(payload).__name__}, not a list — "
            "refusing to guess at a shape this script does not control."
        )

    names: dict[str, str] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        tenant_id = row.get("id")
        name = row.get("name")
        if not (isinstance(tenant_id, str) and isinstance(name, str) and name.strip()):
            continue
        names[tenant_id.strip().lower()] = name.strip()

    if not names:
        raise RuntimeError(
            "licences returned ZERO nameable tenants — refusing to run. That is "
            "indistinguishable from an outage, and treating it as 'nothing resolves' "
            "would print the same audit line as a successful no-op."
        )
    return names


def _usable_name(tenant_id: str, name: str) -> bool:
    """True iff writing ``name`` actually repairs the row.

    A name that is empty, or that IS the uuid, would still match
    :data:`DAMAGED` after the write — which is both a non-repair and an infinite
    batch loop. Licences has no such row today; this is what guarantees it can
    never become one silently.
    """
    stripped = name.strip()
    return bool(stripped) and stripped.lower() != tenant_id.strip().lower()


def _clamp_sql(clamped: bool) -> str:
    """The clamp conjunct for a table, or ``TRUE`` when it has no row-creation time."""
    return f"{_CLAMP_COLUMN} < :cutoff" if clamped else _NO_CLAMP


async def _is_clamped(session: Any, table: str) -> bool:
    """Does ``table`` carry :data:`_CLAMP_COLUMN`? Asked at RUN time, not assumed."""
    found = (
        await session.execute(
            text(_CLAMP_COLUMN_EXISTS_SQL), {"table": table, "column": _CLAMP_COLUMN}
        )
    ).scalar_one()
    return int(found) == 1


async def _census(session: Any, table: str, clamped: bool, cutoff: Any) -> dict[str, int]:
    params = {"cutoff": cutoff} if clamped else {}
    rows = (
        await session.execute(
            text(_CENSUS_SQL.format(table=table, clamp=_clamp_sql(clamped))), params
        )
    ).all()
    return {str(tid).lower(): int(n) for tid, n in rows}


async def _arrivals(session: Any, table: str, cutoff: Any) -> int:
    """Damaged rows CREATED at or after ``cutoff``. Clamped tables only."""
    return int(
        (
            await session.execute(text(_ARRIVALS_SQL.format(table=table)), {"cutoff": cutoff})
        ).scalar_one()
    )


async def _existing_tables(session: Any) -> list[str]:
    """The subset of :data:`TABLES` that really has both columns, right now."""
    present: list[str] = []
    for table in TABLES:
        found = (await session.execute(text(_TABLE_EXISTS_SQL), {"table": table})).scalar_one()
        if int(found) == 2:
            present.append(table)
        else:
            print(
                f"  SKIP {table}: tenant_id/tenant_name columns found = {found}/2 "
                f"in current_schema()"
            )
    return present


async def _repair_table(
    session: Any,
    table: str,
    tenant_id: str,
    name: str,
    batch: int,
    clamped: bool,
    cutoff: Any,
    census_rows: int,
) -> int:
    """Repair one tenant's rows in one table, committing per batch.

    Bounded twice over: the statements are clamped to ``cutoff`` where the table
    has a row-creation time, and the pass count is capped at what the census
    justifies on every table. Overrunning raises :class:`BackfillDidNotConvergeError`
    rather than looping — the loop this replaced was a bare ``while True``.
    """
    repaired = 0
    clamp = _clamp_sql(clamped)
    base: dict[str, Any] = {"cutoff": cutoff} if clamped else {}
    # +1: the budget counts WRITE passes; one further SELECT must be allowed to
    # come back empty, which is how a fully-drained tenant terminates.
    budget = _max_passes(census_rows, batch) + 1
    for _ in range(budget):
        ids = (
            (
                await session.execute(
                    text(_SELECT_BATCH_SQL.format(table=table, clamp=clamp)),
                    {**base, "tid": tenant_id, "batch": batch},
                )
            )
            .scalars()
            .all()
        )
        if not ids:
            return repaired
        result = await session.execute(
            text(_UPDATE_BATCH_SQL.format(table=table, clamp=clamp)),
            {**base, "tid": tenant_id, "name": name, "ids": [str(i) for i in ids]},
        )
        # Commit per batch on purpose: one transaction over 72k rows on the
        # prod primary holds row locks for the whole scan and gives the run no
        # resumable midpoint. Each batch is independently correct — the
        # predicate is idempotent — so an interrupted run is simply a partial
        # one that the next invocation finishes.
        await session.commit()
        repaired += int(result.rowcount)
    raise BackfillDidNotConvergeError(
        f"{table}: tenant {tenant_id} still matches the damage predicate after "
        f"{budget} pass(es) of {batch} (census said {census_rows} row(s), "
        f"{repaired} repaired so far). "
        + (
            "This table is CLAMPED to the run's start instant, so the target set "
            "cannot grow — a genuine overrun here means the census and the write "
            "disagree; reconcile before re-running."
            if clamped
            else "This table has no row-creation column and is therefore UNCLAMPED: "
            "rows are being written into the damage predicate faster than they are "
            "repaired. Deploy the services/tenant_names.py include_archived fix, "
            "confirm the '' count has stopped rising, then re-run."
        )
    )


async def _run(apply: bool, batch: int) -> int:
    settings = get_settings()

    # Captured ONCE, before anything is read or written, and used as the upper
    # bound of every clamped statement for the rest of the run. Taken from the
    # DATABASE, not from this process's clock: the comparison happens in
    # Postgres against Postgres-written timestamps, and a pod whose clock is
    # skewed would otherwise clamp to an instant that never existed there.
    run_started_at: Any = None

    names = await fetch_tenant_names(settings)
    print(f"  {len(names)} nameable tenant(s) from licences (archived included)")

    await init_db(settings)
    factory = get_session_factory()

    exit_code = 0
    async with factory() as session:
        run_started_at = (await session.execute(text("SELECT now()"))).scalar_one()
        print(f"  run_started_at (db clock) = {run_started_at}")

        tables = await _existing_tables(session)
        if not tables:
            print("ABORT: none of the expected tables exist.")
            return 1

        print(f"\n{'APPLY' if apply else 'DRY RUN'} over {len(tables)} table(s)\n")
        for table in tables:
            _check_ident(table)
            clamped = await _is_clamped(session, table)
            before = await _census(session, table, clamped, run_started_at)
            before_total = sum(before.values())

            repairable = {t: n for t, n in before.items() if t in names}
            # POSITIVE fact, not a failure: licences answered and has no row for
            # these. They keep '' — see the module docstring.
            unnameable = {t: n for t, n in before.items() if t not in names}
            # Guarded, not assumed. See _usable_name.
            refused = {t: n for t, n in repairable.items() if not _usable_name(t, names[t])}
            for tid in refused:
                repairable.pop(tid, None)

            scope = "created_at < run_started_at" if clamped else "UNCLAMPED (no created_at)"
            print(
                f"{table} [{scope}]: damaged={before_total} rows over {len(before)} tenant(s) "
                f"| repairable={sum(repairable.values())} ({len(repairable)} tenants) "
                f"| no_registry_row={sum(unnameable.values())} ({len(unnameable)} tenants) "
                f"| refused_unusable_name={sum(refused.values())}"
            )
            if clamped:
                arrived = await _arrivals(session, table, run_started_at)
                # NOT folded into before/repaired/after: it is out of this run's
                # scope by construction. It is the writer still producing.
                print(
                    f"    arrived_since_start={arrived} row(s) — out of scope for this run. "
                    + (
                        "NON-ZERO means the write-path fix is not deployed yet; see the "
                        "--apply precondition."
                        if arrived
                        else "Zero: nothing was written into the damage predicate during the run."
                    )
                )
            for tid, n in sorted(unnameable.items(), key=lambda kv: -kv[1]):
                print(f"    left as-is (licences has no row): {tid} — {n} row(s)")
            for tid, n in sorted(refused.items(), key=lambda kv: -kv[1]):
                print(f"    REFUSED (name is empty or is the uuid): {tid} — {n} row(s)")

            if not apply:
                continue

            repaired = 0
            for tid in sorted(repairable, key=lambda t: -repairable[t]):
                got = await _repair_table(
                    session,
                    table,
                    tid,
                    names[tid],
                    batch,
                    clamped,
                    run_started_at,
                    repairable[tid],
                )
                repaired += got
                print(f"    repaired {got} row(s) -> {names[tid]!r} ({tid})")

            after_total = sum((await _census(session, table, clamped, run_started_at)).values())
            expected = before_total - repaired
            verdict = "OK" if after_total == expected else "MISMATCH"
            if after_total != expected:
                exit_code = 1
            print(
                f"  {table}: before={before_total} repaired={repaired} "
                f"after={after_total} (expected {expected}) [{verdict}]"
            )

    if not apply:
        print(
            "\nDRY RUN. Nothing was written. Re-run with --apply — but ONLY after the "
            "services/tenant_names.py include_archived fix is deployed and the '' "
            "population has stopped growing. See the module docstring."
        )
    elif exit_code:
        print(
            "\nAt least one table's after-count did not equal before-repaired, over the "
            "SAME clamped set. Another writer moved rows, or the predicate and the write "
            "disagree. Re-run the dry run and reconcile before quoting any number from "
            "this run."
        )
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually write. Without it this only measures (the default). "
            "PRECONDITION: the services/tenant_names.py include_archived fix must be "
            "DEPLOYED AND SERVING first. Until it is, the live pods are still writing "
            "'' for the 17 archived tenants, so this repairs rows that are being "
            "re-damaged behind it — a race, not a backfill. The uuid-as-name half of "
            "the damage is closed; the '' half measurably is not (975 -> 1,005 rows on "
            "billing_cost_events between two readings on 2026-09-09)."
        ),
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=1000,
        help="Rows per select/update chunk, one commit each (default 1000).",
    )
    args = parser.parse_args()
    if args.batch < 1:
        parser.error("--batch must be >= 1")
    try:
        return asyncio.run(_run(args.apply, args.batch))
    except BackfillDidNotConvergeError as exc:
        # Loud, named, non-zero. Deliberately NOT a bare traceback and
        # deliberately NOT a retry: the batches already committed are correct
        # and idempotent, so the fix is to remove the cause and re-run, not to
        # keep pushing against a writer.
        print(f"\nDID NOT CONVERGE — {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
