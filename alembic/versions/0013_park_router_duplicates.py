"""Park the worker-management duplicate of every router-metered LLM call.

WHAT IS BROKEN
--------------
`billing_cost_events` records some LLM calls TWICE. inference-router meters
every call it serves (`service_key='inference-router'`). worker_management also
emitted a cost event for the same call whenever the tenant's `tenant_llm_config`
row named the `local` preset — whose provider IS inference-router — because the
emit gate keyed on "the tenant has a BYO row", not on "the call left the
platform". The second row was additionally flagged `byo=true`, so it reached
`billing_cost_daily` as a BYO line for tokens that burned on owner hardware.

Measured on the live billing PRIMARY (pg_is_in_recovery=f)
2026-08-26, read-only, running BOTH of this file's pairing rules as SELECTs.
(The standby cancelled the query with "conflict with recovery"; the numbers
below are the primary's.)

    service_key='worker-management', all of it           37,396 rows  $675.429586
      STRICT would park (proven correlation twin)        14,853 rows  $342.163656
      LOOSE  would park (tokens+cost+minute, no corr.)   21,853 rows  $323.707827
      still billable after both                             690 rows    $9.558103

    …of which, the correlation-less population the LOOSE rule aims at:
      no correlation_id at all                           22,536 rows  $333.247240
      LOOSE reaches                                      21,853 rows  $323.707827
      LOOSE deliberately leaves                             683 rows    $9.539413

The 683 are the price of `date_trunc('minute')` plus the capacity ceiling: a
pair straddling a minute boundary, or a minute-group with no router leg left to
pair against. A ±60s window would have reached 22,478 ($332.70) but is not an
equivalence relation and therefore cannot be made idempotent. Under-parking is
the safe direction.

LOOSE false-positive shape, same run: of 21,431 loose-matched groups, 7,490
(35%) contain a parked row whose `latency_ms` differs from its twin's by more
than 5% of the larger. Read that as a caution, not as 35% false positives —
worker_management WRAPS the router call, so a roughly constant wrapper overhead
is a large RELATIVE gap on a short call and a tiny one on a long call (the twins
first examined differ ~339ms on a ~66s call, 0.5%). The number is printed by the
migration so an operator sees it at apply time.

WHAT THIS MIGRATION DOES
------------------------
1. Adds a REVERSIBLE exclusion to `billing_cost_events`
   (`excluded_from_billing`, `exclusion_reason`, `duplicate_of_event_id`) plus
   the producer-minted `event_key` and its non-partitioned claim table. Nothing
   is deleted — these are financial records, and a park can be undone with one
   UPDATE.
2. PARKS, under the STRICT rule, only rows with a PROVEN twin matched on the
   correlation key. Stamped `duplicate_of_inference_router`.
3. PARKS, under the LOOSE rule, the correlation-less remainder that the strict
   key cannot reach — matched on tokens + cost + the same UTC minute. Stamped
   `duplicate_of_inference_router_loose`, a DIFFERENT reason, so the two sets
   never mix and the loose one can be reversed on its own.
4. RESTATES `billing_cost_daily` for every affected UTC date, by the same
   delete-then-rebuild-from-events the rollup engine itself uses, and reports
   each rule's rows and dollars SEPARATELY — never as one merged total.

THE LOOSE RULE'S RISK, STATED PLAINLY
-------------------------------------
The loose rule CANNOT distinguish a duplicate from two genuinely identical
calls made in the same minute. Parking a legitimate row is an UNDER-bill. The
owner has taken that decision knowingly (2026-08-26). What the code owes in
return is containment, and that is exactly the discriminator: the loose set is
independently reversible via `scripts/unpark_loose_duplicates.py`, which
un-parks `duplicate_of_inference_router_loose` and restates the rollup while
leaving every strict park in place. Reversal is a targeted operation, not a
full rollback of the migration.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
* It does not delete. There is no DELETE statement in this file.
* It does not stop the leak. worker_management PR #197 does that. Until that
  ships, new duplicates keep arriving and this backfill can be re-run — it is
  idempotent by construction.

Revision ID: 0013_park_router_duplicates
Revises: 0012_runtime_ddl_schema_drift
Create Date: 2026-08-26
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy import exc as sa_exc

revision = "0013_park_router_duplicates"
down_revision = "0012_runtime_ddl_schema_drift"
branch_labels = None
depends_on = None


EXCLUSION_REASON = "duplicate_of_inference_router"
EXCLUSION_REASON_LOOSE = "duplicate_of_inference_router_loose"

# The DISCRIMINATOR. `exclusion_reason` is the column that records WHY a row was
# parked, and these two values are the whole of it:
#
#   'duplicate_of_inference_router'        strict — a PROVEN twin, matched on
#                                          the correlation key. Certain.
#   'duplicate_of_inference_router_loose'  loose — a HEURISTIC twin, matched on
#                                          tokens + cost + the same UTC minute,
#                                          for rows that carry no correlation_id
#                                          at all. Probable, not certain.
#
# They are kept apart so the loose set is INDEPENDENTLY REVERSIBLE: if the
# heuristic later proves wrong, `scripts/unpark_loose_duplicates.py` un-parks
# exactly the loose rows and restates the rollup, leaving every strict park
# untouched. A blanket downgrade is not the only lever.

# ---------------------------------------------------------------------------
# The DDL. Byte-identical to the corresponding entries of `ADDITIVE_MIGRATIONS`
# in `src/db.py`, because a divergence between the two IS the defect 0012 was
# written to close (and `TestStatementsTrackSrcDbPy` enforces the equality).
# ---------------------------------------------------------------------------

STATEMENTS: tuple[str, ...] = (
    "ALTER TABLE billing_cost_events "
    "ADD COLUMN IF NOT EXISTS excluded_from_billing BOOLEAN NOT NULL DEFAULT false",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS exclusion_reason VARCHAR(64)",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS duplicate_of_event_id UUID",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS event_key VARCHAR(64)",
    "CREATE TABLE IF NOT EXISTS processed_cost_event_keys ("
    "event_key VARCHAR(64) PRIMARY KEY, "
    "processed_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    "CREATE INDEX IF NOT EXISTS ix_processed_cost_event_keys_processed_at "
    "ON processed_cost_event_keys (processed_at)",
)


# ---------------------------------------------------------------------------
# The park.
#
# The match is the STRICT correlation key:
#   (tenant_id, correlation_id, model, input_tokens, output_tokens, cost_usd)
#
# correlation_id alone is NOT a per-call id — it is the caller's operation id,
# and one operation legitimately contains many calls (measured: up to 8
# worker-management and 128 inference-router rows share one). So the match is
# done at GROUP level with a counted pairing: within each key group at most
# `ir_n` worker-management rows are parked, ordered deterministically by
# (created_at, id). A group with 3 wm rows and 2 ir rows parks 2 and leaves 1 —
# it can never park more legs than there are proven twins.
#
# IDEMPOTENCE: row_number() is computed over ALL worker-management rows in the
# group, not just the unparked ones, so a second run assigns the same ranks to
# the same rows and the `excluded_from_billing = false` predicate on the UPDATE
# makes it a 0-row no-op. (Filtering the ranked set instead would re-rank the
# survivors and park them on the next pass — the subtle bug this avoids.)
# ---------------------------------------------------------------------------

PARK_SQL = f"""
WITH ir AS (
    SELECT tenant_id, correlation_id, model, input_tokens, output_tokens, cost_usd,
           count(*) AS ir_n,
           (array_agg(id ORDER BY created_at, id))[1] AS ir_id
    FROM billing_cost_events
    WHERE service_key = 'inference-router'
      AND correlation_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
),
wm AS (
    SELECT id, created_at, tenant_id, correlation_id, model,
           input_tokens, output_tokens, cost_usd,
           row_number() OVER (
               PARTITION BY tenant_id, correlation_id, model,
                            input_tokens, output_tokens, cost_usd
               ORDER BY created_at, id
           ) AS rn
    FROM billing_cost_events
    WHERE service_key = 'worker-management'
      AND correlation_id IS NOT NULL
),
pairs AS (
    SELECT wm.id, wm.created_at, ir.ir_id
    FROM wm
    JOIN ir
      ON  ir.tenant_id      = wm.tenant_id
      AND ir.correlation_id = wm.correlation_id
      AND ir.model            IS NOT DISTINCT FROM wm.model
      AND ir.input_tokens   = wm.input_tokens
      AND ir.output_tokens  = wm.output_tokens
      AND ir.cost_usd         IS NOT DISTINCT FROM wm.cost_usd
    WHERE wm.rn <= ir.ir_n
)
UPDATE billing_cost_events e
   SET excluded_from_billing = true,
       exclusion_reason      = '{EXCLUSION_REASON}',
       duplicate_of_event_id = pairs.ir_id
  FROM pairs
 WHERE e.id = pairs.id
   AND e.created_at = pairs.created_at
   AND e.excluded_from_billing = false
"""


# ---------------------------------------------------------------------------
# The LOOSE park — the correlation-less remainder.
#
# 22,536 worker-management rows ($333.25, measured 2026-08-26) carry NO
# correlation_id, so the strict key above cannot reach them. They are the same
# leak: the same emitter, the same tenant, the same window. The loose key is
#
#   (tenant_id, model, input_tokens, output_tokens, cost_usd, UTC minute)
#
# STATE THE RISK PLAINLY: this rule CANNOT distinguish a duplicate from two
# genuinely identical calls made in the same minute. Parking a legitimate row is
# an UNDER-bill. The owner has taken that decision knowingly; the job of the SQL
# is to contain it:
#
#   * every loose park is stamped EXCLUSION_REASON_LOOSE, never the strict
#     reason, so the set is independently reversible (see the discriminator
#     note above and `scripts/unpark_loose_duplicates.py`);
#   * it only ever touches `correlation_id IS NULL` rows, which the strict pass
#     by definition never touches (it requires `correlation_id IS NOT NULL`).
#     The two sets are DISJOINT by construction — no row can be double-parked,
#     and no dollar can be subtracted twice;
#   * the pairing is COUNTED, like the strict one, and the available twin count
#     is reduced by the worker-management legs in the group that are ALREADY
#     parked (`parked_n`) — including the ones the strict pass just parked in
#     this same transaction. A minute-group can therefore never surrender more
#     worker-management legs, across BOTH rules, than it has router legs. That
#     subtraction is deliberately conservative: it can under-park (a strict twin
#     may have been matched on a router row in a neighbouring minute), never
#     over-park.
#
# "Same minute" is `date_trunc('minute', …)`, not a +/-60s window: a sliding
# window is not an equivalence relation, so it cannot be grouped, counted, or
# ranked deterministically — and therefore cannot be made idempotent. A pair
# straddling a minute boundary is simply not parked. Under-parking is the safe
# direction.
#
# IDEMPOTENCE: row_number() ranks ALL correlation-less worker-management rows in
# the group (parked or not), so ranks are stable across runs; on a second run
# `parked_n` has grown by exactly what the first run parked, the available count
# collapses to zero, and the `excluded_from_billing = false` predicate makes the
# UPDATE a 0-row no-op.
# ---------------------------------------------------------------------------

LOOSE_PARK_SQL = f"""
WITH ir AS (
    SELECT tenant_id, model, input_tokens, output_tokens, cost_usd,
           date_trunc('minute', created_at AT TIME ZONE 'UTC') AS minute,
           count(*) AS ir_n,
           (array_agg(id ORDER BY created_at, id))[1] AS ir_id
    FROM billing_cost_events
    WHERE service_key = 'inference-router'
    GROUP BY 1, 2, 3, 4, 5, 6
),
wm_group AS (
    SELECT tenant_id, model, input_tokens, output_tokens, cost_usd,
           date_trunc('minute', created_at AT TIME ZONE 'UTC') AS minute,
           count(*) FILTER (WHERE excluded_from_billing) AS parked_n
    FROM billing_cost_events
    WHERE service_key = 'worker-management'
    GROUP BY 1, 2, 3, 4, 5, 6
),
wm AS (
    SELECT id, created_at, tenant_id, model, input_tokens, output_tokens, cost_usd,
           date_trunc('minute', created_at AT TIME ZONE 'UTC') AS minute,
           row_number() OVER (
               PARTITION BY tenant_id, model, input_tokens, output_tokens, cost_usd,
                            date_trunc('minute', created_at AT TIME ZONE 'UTC')
               ORDER BY created_at, id
           ) AS rn
    FROM billing_cost_events
    WHERE service_key = 'worker-management'
      AND correlation_id IS NULL
),
pairs AS (
    SELECT wm.id, wm.created_at, ir.ir_id
    FROM wm
    JOIN ir
      ON  ir.tenant_id      = wm.tenant_id
      AND ir.model            IS NOT DISTINCT FROM wm.model
      AND ir.input_tokens   = wm.input_tokens
      AND ir.output_tokens  = wm.output_tokens
      AND ir.cost_usd         IS NOT DISTINCT FROM wm.cost_usd
      AND ir.minute         = wm.minute
    JOIN wm_group g
      ON  g.tenant_id       = wm.tenant_id
      AND g.model             IS NOT DISTINCT FROM wm.model
      AND g.input_tokens    = wm.input_tokens
      AND g.output_tokens   = wm.output_tokens
      AND g.cost_usd          IS NOT DISTINCT FROM wm.cost_usd
      AND g.minute          = wm.minute
    WHERE wm.rn <= ir.ir_n - g.parked_n
)
UPDATE billing_cost_events e
   SET excluded_from_billing = true,
       exclusion_reason      = '{EXCLUSION_REASON_LOOSE}',
       duplicate_of_event_id = pairs.ir_id
  FROM pairs
 WHERE e.id = pairs.id
   AND e.created_at = pairs.created_at
   AND e.correlation_id IS NULL
   AND e.excluded_from_billing = false
"""


# ---------------------------------------------------------------------------
# The targeted reversal of the LOOSE set only.
#
# This is the lever requirement (1) exists for. It leaves every strict park in
# place; `scripts/unpark_loose_duplicates.py` runs it and restates the rollup.
# ---------------------------------------------------------------------------

UNPARK_LOOSE_SQL = f"""
UPDATE billing_cost_events
   SET excluded_from_billing = false,
       exclusion_reason      = NULL,
       duplicate_of_event_id = NULL
 WHERE exclusion_reason = '{EXCLUSION_REASON_LOOSE}'
"""


# ---------------------------------------------------------------------------
# The false-positive SHAPE of the loose rule.
#
# The decision is taken, but the operator should still see what the heuristic
# bought. An outer/inner leg of ONE call has near-identical latency; two
# GENUINELY different calls that happen to burn the same tokens in the same
# minute would not.
#
# MEASURED ON THE LIVE PRIMARY, 2026-08-26, over the 21,853 loose-matched pairs.
# Coverage is total — 0 of the 21,853 are missing a latency on either leg, read
# back from the DB rather than assumed:
#
#     wm.latency_ms >= ir.latency_ms   21,650 of 21,853   99.07%
#     median delta                         183 ms  (median ir latency 4,718 ms)
#     p90 / p99 delta                  456 / 1,216 ms
#     |delta| > 1s                         377 pairs   1.7%
#     |delta| > 5s                          21 pairs   0.1%
#
# That is CONSTANT WRAPPER OVERHEAD, not two different calls: worker_management
# wraps the router call, so its leg is ~183 ms longer, essentially always.
#
# It also kills a metric shipped in the first draft of this file. A purely
# RELATIVE test (|delta| > 5% of the larger) flags 7,490 of 21,431 groups — 35%
# — but that is the test measuring its own scale-sensitivity: a fixed 183 ms
# trips 5% on a short call and is negligible on a long one. Reporting 35% alone
# would have told the owner the heuristic was a third wrong. It is not.
#
# So the instrument reports FOUR numbers, and the two that carry signal are the
# ABSOLUTE ones:
#
#   pairs                   how many loose parks the instrument can see at all
#   wm_faster_than_router   the wm leg FASTER than the leg it supposedly wraps —
#                           physically impossible for one call, so this is the
#                           genuinely unexplained population. 203 in prod.
#   delta_over_1s           an absolute gap too large for wrapper overhead
#   relative_dissent_groups the 5%-relative count, kept for continuity and
#                           explicitly NOT a false-positive rate
#
# `latency_ms` is NULL on rows that never carried it; those pairs are invisible
# to every other number here, which is exactly why `pairs` is reported too.
# ---------------------------------------------------------------------------

LOOSE_LATENCY_SHAPE_SQL = f"""
SELECT
    count(*) AS pairs,
    count(*) FILTER (WHERE w.latency_ms < r.latency_ms) AS wm_faster_than_router,
    count(*) FILTER (WHERE abs(w.latency_ms - r.latency_ms) > 1000) AS delta_over_1s,
    count(DISTINCT (w.tenant_id, w.model, w.input_tokens, w.output_tokens,
                    w.cost_usd,
                    date_trunc('minute', w.created_at AT TIME ZONE 'UTC')))
        FILTER (WHERE abs(w.latency_ms - r.latency_ms)
                      > 0.05 * greatest(w.latency_ms, r.latency_ms))
        AS relative_dissent_groups
FROM billing_cost_events w
JOIN billing_cost_events r ON r.id = w.duplicate_of_event_id
WHERE w.exclusion_reason = '{EXCLUSION_REASON_LOOSE}'
  AND w.latency_ms IS NOT NULL
  AND r.latency_ms IS NOT NULL
"""


# ---------------------------------------------------------------------------
# The rollup restatement.
#
# `billing_cost_daily` is DERIVED — `rollup_engine.rollup_daily` deletes a whole
# bucket_date and rebuilds it from events. This reproduces exactly that, for the
# affected dates only, with the exclusion filter applied. The grain is the
# `uq_cost_daily` key; `tests/test_migration_0013_park_router_duplicate.py`
# pins this SQL against the real `rollup_daily` output rather than trusting the
# two to stay in step.
#
# The sentinels are `models/db/_common.py`: NULL_LICENSE_ID …0000,
# NULL_SUBJECT_ID …0003, served_model '__unknown__'.
# ---------------------------------------------------------------------------

AFFECTED_DATES_SQL = f"""
SELECT DISTINCT (created_at AT TIME ZONE 'UTC')::date AS d
FROM billing_cost_events
WHERE exclusion_reason IN ('{EXCLUSION_REASON}', '{EXCLUSION_REASON_LOOSE}')
ORDER BY d
"""

DELETE_STALE_DAILY_SQL = """
DELETE FROM billing_cost_daily WHERE bucket_date = ANY(:dates)
"""

REBUILD_DAILY_SQL = """
INSERT INTO billing_cost_daily (
    id, tenant_id, tenant_name, license_id, subject_id, service_key, provider,
    served_model, byo, owner_provided, cost_source, bucket_date,
    total_input_tokens, total_output_tokens, total_cost_usd, request_count
)
SELECT gen_random_uuid(),
       e.tenant_id,
       max(e.tenant_name),
       coalesce(e.license_id, '00000000-0000-0000-0000-000000000000'::uuid),
       coalesce(e.subject_id, '00000000-0000-0000-0000-000000000003'::uuid),
       e.service_key,
       e.provider,
       coalesce(e.served_model, '__unknown__'),
       coalesce((e.usage_extra ->> 'byo') = 'true', false),
       coalesce((e.usage_extra ->> 'owner_provided') = 'true', false),
       e.cost_source,
       (e.created_at AT TIME ZONE 'UTC')::date,
       coalesce(sum(e.input_tokens), 0),
       coalesce(sum(e.output_tokens), 0),
       coalesce(sum(e.cost_usd), 0),
       count(*)
  FROM billing_cost_events e
 WHERE (e.created_at AT TIME ZONE 'UTC')::date = ANY(:dates)
   AND e.excluded_from_billing = false
 GROUP BY e.tenant_id,
          coalesce(e.license_id, '00000000-0000-0000-0000-000000000000'::uuid),
          coalesce(e.subject_id, '00000000-0000-0000-0000-000000000003'::uuid),
          e.service_key,
          e.provider,
          coalesce(e.served_model, '__unknown__'),
          coalesce((e.usage_extra ->> 'byo') = 'true', false),
          coalesce((e.usage_extra ->> 'owner_provided') = 'true', false),
          e.cost_source,
          (e.created_at AT TIME ZONE 'UTC')::date
"""


RACE_CLASS_ORIG_NAMES: frozenset[str] = frozenset(
    {"DuplicateObject", "DuplicateTable", "DuplicateColumn", "UniqueViolation"}
)


def _is_race_class_error(exc: BaseException) -> bool:
    """True when ``exc`` wraps a duplicate-object / unique-violation driver error."""
    orig = getattr(exc, "orig", None)
    return orig is not None and type(orig).__name__ in RACE_CLASS_ORIG_NAMES


def _apply_ddl(conn: Any) -> None:
    """Apply STATEMENTS, one SAVEPOINT each (mirrors 0012's race handling).

    `alembic/env.py` runs the whole migration in ONE transaction, and `src/db.py`
    replays the same DDL on every pod start, so a benign concurrent-DDL race
    would otherwise poison the transaction and roll the entire upgrade back.
    """
    for statement in STATEMENTS:
        try:
            with conn.begin_nested():
                conn.execute(sa.text(statement))
        except sa_exc.DBAPIError as exc:
            if not _is_race_class_error(exc):
                raise
            print(
                "0013: lost a benign DDL race, continuing (the concurrent "
                f"creator wins; postcondition still enforced): {statement[:120]}",
                flush=True,
            )


def _verify_ddl(conn: Any) -> None:
    """Read the postcondition back from the CATALOGUE, by definition not by name.

    Every statement above is a no-op when an object of that NAME already exists,
    so "created it", "did nothing" and "found something else wearing the name"
    are otherwise the same exit code.
    """
    expected = {
        ("billing_cost_events", "excluded_from_billing"): ("boolean", "NO"),
        ("billing_cost_events", "exclusion_reason"): ("character varying", "YES"),
        ("billing_cost_events", "duplicate_of_event_id"): ("uuid", "YES"),
        ("billing_cost_events", "event_key"): ("character varying", "YES"),
        ("processed_cost_event_keys", "event_key"): ("character varying", "NO"),
    }
    for (table, column), (data_type, nullable) in expected.items():
        row = conn.execute(
            sa.text(
                "SELECT data_type, is_nullable FROM information_schema.columns "
                "WHERE table_name = :t AND column_name = :c"
            ),
            {"t": table, "c": column},
        ).first()
        if row is None:
            raise RuntimeError(f"0013: {table}.{column} is missing after upgrade")
        if (row[0], row[1]) != (data_type, nullable):
            raise RuntimeError(
                f"0013: {table}.{column} is {row[0]}/{row[1]}, expected "
                f"{data_type}/{nullable} — an object of that NAME exists but is "
                "not the one this migration needs"
            )
    # The PRIMARY KEY is the whole point of the claim table; a plain table
    # wearing the name would silently dedupe nothing.
    pk = conn.execute(
        sa.text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'processed_cost_event_keys'::regclass AND contype = 'p'"
        )
    ).scalar()
    if pk != "PRIMARY KEY (event_key)":
        raise RuntimeError(
            f"0013: processed_cost_event_keys has no usable PK (got {pk!r}) — "
            "the idempotency claim would accept a replayed emit"
        )


BILLABLE_WM_SQL = (
    "SELECT count(*), coalesce(sum(cost_usd), 0) FROM billing_cost_events "
    "WHERE service_key = 'worker-management' AND excluded_from_billing = false"
)


def _billable(conn: Any) -> Any:
    """(rows, dollars) still billable for worker-management, right now."""
    return conn.execute(sa.text(BILLABLE_WM_SQL)).one()


def upgrade() -> None:
    conn = op.get_bind()
    _apply_ddl(conn)
    _verify_ddl(conn)

    before = _billable(conn)

    # STRICT first. It requires `correlation_id IS NOT NULL`; the loose pass
    # requires `correlation_id IS NULL`. Disjoint by construction, so the strict
    # reason always wins and no row is parked — or subtracted — twice.
    parked_strict = conn.execute(sa.text(PARK_SQL)).rowcount
    mid = _billable(conn)

    parked_loose = conn.execute(sa.text(LOOSE_PARK_SQL)).rowcount
    after = _billable(conn)

    shape = conn.execute(sa.text(LOOSE_LATENCY_SHAPE_SQL)).one()

    dates = [r[0] for r in conn.execute(sa.text(AFFECTED_DATES_SQL))]
    if dates:
        conn.execute(sa.text(DELETE_STALE_DAILY_SQL), {"dates": dates})
        conn.execute(sa.text(REBUILD_DAILY_SQL), {"dates": dates})

    # print(..., flush=True), never logger: alembic's logging config swallows a
    # logger call from inside a migration, and the `migrate` init container's
    # stdout is the one channel an operator actually reads.
    #
    # The two rules are reported SEPARATELY and never merged into one total.
    # They carry different certainty, and the owner has to be able to see what
    # each one cost.
    print(
        "0013 STRICT (proven correlation twin, "
        f"reason={EXCLUSION_REASON!r}): parked {parked_strict} rows, "
        f"billable worker-management {before[0]} -> {mid[0]} rows, "
        f"${before[1]} -> ${mid[1]}",
        flush=True,
    )
    print(
        "0013 LOOSE (tokens+cost+minute heuristic, correlation-less rows only, "
        f"reason={EXCLUSION_REASON_LOOSE!r}): parked {parked_loose} rows, "
        f"billable worker-management {mid[0]} -> {after[0]} rows, "
        f"${mid[1]} -> ${after[1]}. REVERSIBLE ON ITS OWN: "
        "python scripts/unpark_loose_duplicates.py --apply",
        flush=True,
    )
    print(
        f"0013 LOOSE false-positive shape over {shape[0]} pair(s) with latency on "
        f"both legs: {shape[1]} where the worker-management leg is FASTER than "
        "the router leg it wraps (physically impossible for one call — the only "
        f"genuinely unexplained population); {shape[2]} with an absolute gap "
        f">1s; {shape[3]} group(s) over the 5%-RELATIVE threshold. The relative "
        "number is NOT a false-positive rate: prod's median gap is 183ms against "
        "a 4,718ms median call, i.e. constant wrapper overhead, which trips 5% "
        "on short calls and nothing on long ones",
        flush=True,
    )
    print(
        f"0013: restated billing_cost_daily for {len(dates)} UTC date(s)"
        + (f" ({dates[0]}..{dates[-1]})" if dates else ""),
        flush=True,
    )


def downgrade() -> None:
    """Un-park the rows and restate the rollup. The COLUMNS are deliberately kept.

    The exclusion is reversed — that is the whole point of parking rather than
    deleting, and it is a plain UPDATE with no data loss in either direction.
    BOTH reasons are reversed here, because a downgrade of 0013 means "undo
    0013". To reverse ONLY the loose heuristic and keep the proven strict parks,
    do NOT downgrade — run `python scripts/unpark_loose_duplicates.py --apply`,
    which is what the reason discriminator exists for.

    The columns are NOT dropped:

    1. ``src/db.py``'s ``ADDITIVE_MIGRATIONS`` recreates all six objects on EVERY
       pod start, so a drop would be undone by the next replica to boot — and,
       until then, would leave running replicas writing against a schema that no
       longer matches their models (``models/db/events.py`` declares them
       unconditionally).
    2. ``event_key`` and ``processed_cost_event_keys`` hold the ingest
       idempotency claim. Dropping them mid-flight would let an in-flight
       redelivery re-bill, i.e. the downgrade itself would create the class of
       defect this migration exists to close.

    Dropping the columns is therefore an operator decision, taken with the pods
    stopped — not something a downgrade does automatically.
    """
    conn = op.get_bind()
    unparked = conn.execute(
        sa.text(
            "UPDATE billing_cost_events "
            "   SET excluded_from_billing = false, "
            "       exclusion_reason = NULL, "
            "       duplicate_of_event_id = NULL "
            " WHERE exclusion_reason = ANY(:reasons)"
        ),
        {"reasons": [EXCLUSION_REASON, EXCLUSION_REASON_LOOSE]},
    ).rowcount

    dates = [
        r[0]
        for r in conn.execute(
            sa.text(
                "SELECT DISTINCT (created_at AT TIME ZONE 'UTC')::date AS d "
                "FROM billing_cost_events "
                "WHERE service_key = 'worker-management' ORDER BY d"
            )
        )
    ]
    if dates:
        conn.execute(sa.text(DELETE_STALE_DAILY_SQL), {"dates": dates})
        conn.execute(sa.text(REBUILD_DAILY_SQL), {"dates": dates})

    print(
        f"0013 downgrade: un-parked {unparked} cost events and restated "
        f"billing_cost_daily for {len(dates)} UTC date(s). Columns and "
        "processed_cost_event_keys are intentionally KEPT — see the docstring.",
        flush=True,
    )
