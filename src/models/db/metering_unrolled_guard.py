"""A ``BEFORE TRUNCATE`` state gate on ``billing_metering_events`` — the half 0019 left open.

WHAT THIS CLOSES
----------------
The 2026-09-05T07:01:56Z TRUNCATE named ``billing_metering_events`` in the SAME
statement that emptied ``billing_cost_events`` and its 12 partitions. Alembic
0019 (#140) gave the cost ledger a **state** gate — refuse while raw rows exist
newer than the rollup watermark, even for a session that has already armed
0017's **intent** hatch. The metering ledger got 0017's intent gate in 0018 and
nothing else. Measured on the production primary (worker4) before this change::

    billing_cost_events       billing_delete_audit_trg
                              billing_truncate_audit_trg
                              billing_truncate_guard_trg            <- intent
                              billing_unrolled_truncate_guard_trg   <- state
    billing_metering_events   billing_delete_audit_trg
                              billing_truncate_audit_trg
                              billing_truncate_guard_trg            <- intent
                              (no state gate)

All 12 cost partitions carry the intent, state and TRUNCATE-audit triggers;
``billing_metering_events`` is ``relkind = 'r'`` — an ordinary table, not
partitioned — so this control attaches to exactly one relation.

WHY 0019's FUNCTION COULD NOT SIMPLY BE ATTACHED HERE
-----------------------------------------------------
``billing_forbid_unrolled_truncate()`` HARDCODES ``public.billing_cost_daily``
as its watermark and ignores ``TG_TABLE_NAME`` when reading it. Attaching that
function to ``billing_metering_events`` would compare metering rows against the
COST rollup's watermark: it would pass whenever cost happened to be caught up
and metering was not, and refuse whenever the reverse held. A gate that answers
for the wrong table is worse than an absent one, because it reports a property
it never measured. Hence a second function, not a second trigger.

⚠️ CORRECTED 2026-09-08 — 0019's docstring said this gap was "Separately
tracked". It was not. Measured: ``gh pr list --search`` and ``gh issue list``
over ``nonagenticai/billing-service`` returned **zero** PRs and **zero** issues
naming it, against a positive control of **6** issues in the same listing (two
of which — #134, #135 — are about ``billing_metering_events`` itself, so the
query was capable of finding metering work). The phrase was the only thing
standing between this gap and an owner, and it asserted an owner that did not
exist. 0019's line is corrected in place in ``unrolled_guard.py``.

THE PREDICATE IS COMPOUND, AND WHICH WATERMARK BINDS
-----------------------------------------------------
Cost aggregates in ONE stage (``billing_cost_events -> billing_cost_daily``), so
0019's watermark is a single ``max(bucket_date)``. Metering aggregates in TWO
(``billing_metering_events -> billing_metering_hourly ->
billing_metering_daily``), and the two stages can be behind INDEPENDENTLY, in
both directions. Neither stage alone is a correct gate:

* **``billing_metering_daily`` alone is not sufficient.** ``rollup_daily``
  builds a day's daily rows by aggregating whatever ``billing_metering_hourly``
  holds for that day (``rollup_engine.py``: it selects ``MeteringHourly`` rows
  with ``cast(timezone('UTC', bucket_hour) as date) == target_date``). It does
  not verify that all 24 hours are present. So a day whose hourly coverage is
  partial still advances ``max(bucket_date)``, and a daily-only watermark then
  reports "rolled" over raw events that reached NO aggregate at all. The hourly
  catch-up in ``aggregator.run_rollup_loop`` exists precisely because hourly can
  hole; ``missing_event_hours`` is its gap-finder.

* **``billing_metering_hourly`` alone is not sufficient either.** It is the
  stage that decides RECOVERABILITY — a raw event already folded into an hourly
  bucket can still be re-rolled into daily after the raw rows are gone — but the
  2026-09-05 statement named the raw table AND the rollup tables together. A
  gate that trusts hourly as a surviving witness trusts a witness the same
  statement is about to destroy. And the hourly stage is measured in hours,
  daily in days, so on a healthy system daily is the LATER of the two to catch
  up and is therefore the binding one most of the time.

⭐ **So the cutoff is the EARLIER of the two stages' coverage ends**, and the
gate refuses while any raw event is at or after it::

    hourly_end := max(bucket_hour) + interval '1 hour'         -- hourly's frontier
    daily_end  := (max(bucket_date) + 1)::timestamp AT TIME ZONE 'UTC'
    cutoff     := least(hourly_end, daily_end)

``least()`` in Postgres IGNORES NULLs, which would be fail-OPEN: an entirely
empty ``billing_metering_daily`` would silently hand the decision to the hourly
stage. So each end is coalesced to ``-infinity`` BEFORE ``least()`` is applied.
``least('-infinity', anything)`` is ``-infinity``, so an unreadable or empty
stage refuses everything — the same fail-CLOSED reading 0019 takes, for the same
reason: a watermark this control cannot read is not evidence that deletion is
safe.

The consequence in normal operation is a deliberate over-refusal: rows that ARE
in hourly but whose day is not yet in daily are refused although they are
technically re-derivable. That is the correct direction of error for a control
whose failure mode is permanent revenue loss, and the override below is how an
operator says so.

WHY THE HOURLY END IS ``+ 1 hour`` AND THE DAILY END IS ``+ 1 day``
-------------------------------------------------------------------
``rollup_hourly`` buckets an event into ``window_start`` and covers
``[window_start, window_start + 1 hour)``, so ``max(bucket_hour)`` names the
START of the last covered hour and the first uncovered instant is one hour
later. ``rollup_daily`` writes ``bucket_date`` for a whole UTC day, so the first
uncovered instant is the start of the next day. Both are expressed as sargable
``created_at >= <instant>`` range predicates rather than per-row function calls,
so the ALLOWED arm — the one a healthy system takes — costs an index probe on
``ix_events_created`` and not a scan of the ledger. The ``count(*)``/``min``/
``max`` runs only once a refusal or an override is already certain, and both of
those are abnormal by definition.

THE FUNCTION REFUSES TO ANSWER FOR A TABLE IT WAS NOT WRITTEN FOR
------------------------------------------------------------------
The defect that made this a separate function in the first place — a watermark
hardcoded for one table, silently applied to another — is trivially repeatable
by anyone who attaches this trigger somewhere new. So the function checks the
relation it fired on and RAISEs if it is neither ``billing_metering_events`` nor
a partition of it. A gate asked about the wrong table now says so loudly instead
of returning a confident answer about a relation it never measured.

THE ESCAPE HATCH — A THIRD, DISTINCT GUC
-----------------------------------------
::

    SET LOCAL billing.allow_truncate = 'on';                     -- 0017: intent
    SET LOCAL billing.allow_unrolled_metering_truncate = 'on';   -- THIS: state
    SET LOCAL billing.delete_reason = 'why, in words';
    TRUNCATE billing_metering_events;

⭐ Deliberately NOT ``billing.allow_unrolled_truncate`` (0019's hatch), for a
reason that is about OPERATORS rather than about the test harness. The harness
argument is inert here: ``tests/conftest.py`` arms every hatch it needs on every
test, so a third GUC costs it one more line and buys it nothing. The argument
that decides it is the shape of the incident — ONE statement naming MANY tables.
An operator draining the cost ledger arms 0019's GUC; if metering shared it,
that single armed line would authorise destroying un-aggregated metering the
operator never looked at, in the same statement, silently. Two ledgers, two
independent assertions of intent. Nobody deletes un-aggregated metering revenue
without having said the word "metering".

Taking the override writes a ``billing_delete_audit`` row **before** the rows go,
with ``operation = 'TRUNCATE_UNROLLED_METERING_OVERRIDE'``, both watermarks, the
row count and the ``created_at`` window about to be destroyed — the numbers that
were unreconstructable on 2026-09-05 and had to be recovered by decoding UUIDv7
timestamps. It still ends in ``OVERRIDE``, so the existing
``WHERE operation LIKE '%OVERRIDE'`` review query keeps finding it.

⚠️ Same known limit 0019 states: if ``billing_delete_audit`` is itself named in
the same TRUNCATE — it was eleventh in the 2026-09-05 table list — this row is
written by the BEFORE trigger and destroyed by the same statement. That is a
property of TRUNCATE, not of this gate, and closing it needs an audit sink
outside the database.

PARTITIONING
------------
``billing_metering_events`` is NOT partitioned (``pg_class.relkind = 'r'``,
measured on the production primary; only ``billing_cost_events`` is ``'p'``), so
there is no partition-minting function to extend and no children to attach to —
0019's whole ``ATTACH_GATE_TO_EXISTING_PARTITIONS_DDL`` /
``PARTITION_FN_WITH_GATE_DDL`` machinery has no counterpart here. ⛔ That is a
fact about today's schema, not a property of the design: Postgres does NOT clone
statement-level TRUNCATE triggers to partitions, so the day this table is
range-partitioned, every child needs the trigger attached explicitly or the
control acquires a hole in exactly the place the 2026-09-05 statement landed.
``test_metering_events_is_not_partitioned`` fails the moment that changes, and
its message says what to add.

WHY A DATABASE TRIGGER
----------------------
Unchanged from 0019, and it is the incident that decides it: there is no
TRUNCATE anywhere in ``src/``, so an application-level guard would have had
nothing to intercept. The 2026-09-05 statement arrived as raw SQL on a
connection as ``billing_user``, who OWNS this table and therefore holds TRUNCATE
rights by ownership — ``REVOKE`` from an owner executes and changes nothing. The
trigger is the only mechanism available, and it is client-agnostic by
construction.
"""

from __future__ import annotations

from sqlalchemy import event

from .events import MeteringEvent

#: The GUC that overrides the metering watermark gate.
#:
#: ⭐ Deliberately a THIRD GUC — not 0017's ``billing.allow_truncate`` and not
#: 0019's ``billing.allow_unrolled_truncate``. See "THE ESCAPE HATCH" above: the
#: incident's shape is one statement naming many tables, so arming the cost
#: ledger's override must not carry permission over the metering ledger.
ALLOW_UNROLLED_METERING_TRUNCATE_GUC = "billing.allow_unrolled_metering_truncate"

#: The one value that arms it. Same single-exact-spelling rule as 0017's and
#: 0019's hatches: 'true', '1', 'yes' and '' all refuse, so a fat-fingered ``SET``
#: cannot arm the override and a psql transcript reads unambiguously at review.
ALLOW_UNROLLED_METERING_TRUNCATE_VALUE = "on"

#: ``operation`` written to ``billing_delete_audit`` when the override is taken.
#: Distinct from 0019's value so a metering override is greppable on its own,
#: and still ``LIKE '%OVERRIDE'`` so the existing review query finds it.
UNROLLED_METERING_OVERRIDE_OPERATION = "TRUNCATE_UNROLLED_METERING_OVERRIDE"

#: The relation this gate is written for. The function refuses to answer for any
#: other, because answering for the wrong table with the right-looking watermark
#: is exactly the defect that made 0019's function unusable here.
METERING_EVENTS_TABLE = "billing_metering_events"

#: The two rollup stages whose coverage the cutoff is the EARLIER of.
METERING_HOURLY_TABLE = "billing_metering_hourly"
METERING_DAILY_TABLE = "billing_metering_daily"

#: Trigger name. Sorts AFTER 0017's ``billing_truncate_guard_trg`` ('u' > 't'),
#: and Postgres fires BEFORE TRUNCATE triggers in name order, so an un-armed
#: session is refused by the cheap intent gate first and never pays for this
#: one's two watermark reads. Either order refuses; this order refuses more
#: cheaply and with the more useful message. Same reasoning as 0019's name.
METERING_GATE_TRIGGER = "billing_unrolled_metering_truncate_guard_trg"

METERING_UNROLLED_GUARD_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_unrolled_metering_truncate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $fn$
DECLARE
    v_hourly_ts  timestamptz;
    v_daily_max  date;
    v_hourly_end timestamptz;
    v_daily_end  timestamptz;
    v_cutoff     timestamptz;
    v_unrolled   boolean;
    v_rows       bigint;
    v_min        timestamptz;
    v_max        timestamptz;
BEGIN
    -- ⛔ ANSWER ONLY FOR THE TABLE THIS WATERMARK DESCRIBES. 0019's function
    -- hardcodes billing_cost_daily and ignores TG_TABLE_NAME, which is why it
    -- could not be reused here: attached to the wrong relation it returns a
    -- confident verdict about a table it never measured. This one refuses.
    IF TG_TABLE_NAME <> 'billing_metering_events'
       AND to_regclass('public.' || quote_ident(TG_TABLE_NAME)) NOT IN (
            SELECT i.inhrelid::regclass
              FROM pg_inherits i
             WHERE i.inhparent = to_regclass('public.billing_metering_events'))
    THEN
        RAISE EXCEPTION
            'billing_forbid_unrolled_metering_truncate fired on %.%, which is not '
            'billing_metering_events or a partition of it', TG_TABLE_SCHEMA, TG_TABLE_NAME
            USING
                ERRCODE = 'P0001',
                HINT =
                    'This function reads the METERING rollup watermarks '
                    '(billing_metering_hourly, billing_metering_daily). Attaching it to '
                    'another relation would judge that relation against the wrong '
                    'aggregate and pass or refuse for the wrong reason. Write a gate for '
                    'that table''s own rollup chain instead.';
    END IF;

    -- STAGE 1 of 2: hourly. to_regclass, not a cast, for the same reason 0019
    -- uses it: this function is also installed by the metadata after_create
    -- hook, where a rollup table may not exist yet. A missing stage is treated
    -- as "nothing has been rolled up" -- the fail-CLOSED reading.
    IF to_regclass('public.billing_metering_hourly') IS NULL THEN
        v_hourly_ts := NULL;
    ELSE
        SELECT max(bucket_hour) INTO v_hourly_ts FROM public.billing_metering_hourly;
    END IF;

    -- STAGE 2 of 2: daily.
    IF to_regclass('public.billing_metering_daily') IS NULL THEN
        v_daily_max := NULL;
    ELSE
        SELECT max(bucket_date) INTO v_daily_max FROM public.billing_metering_daily;
    END IF;

    -- rollup_hourly covers [bucket_hour, bucket_hour + 1 hour), so the first
    -- instant hourly does NOT cover is one hour after the newest bucket.
    v_hourly_end := coalesce(v_hourly_ts + interval '1 hour', '-infinity'::timestamptz);

    -- rollup_daily covers a whole UTC day, so the first instant daily does NOT
    -- cover is the start of the day after the newest bucket. UTC-explicit: this
    -- is exactly how rollup_daily assigns bucket_date, and the session timezone
    -- must not be able to shift the boundary.
    v_daily_end := coalesce(
        (v_daily_max + 1)::timestamp AT TIME ZONE 'UTC', '-infinity'::timestamptz);

    -- ⭐ THE COMPOUND PREDICATE: the EARLIER of the two stages' coverage ends.
    -- The stages lag independently -- a day rolled from an incomplete hourly
    -- advances max(bucket_date) over hours that reached no aggregate at all --
    -- so neither alone is a gate. coalesce BEFORE least(), never after: least()
    -- ignores NULLs, so an empty stage would otherwise silently hand the whole
    -- decision to the other one, which is fail-OPEN.
    v_cutoff := least(v_hourly_end, v_daily_end);

    EXECUTE format(
        'SELECT EXISTS (SELECT 1 FROM public.%I WHERE created_at >= $1)',
        TG_TABLE_NAME)
      INTO v_unrolled
      USING v_cutoff;

    IF NOT v_unrolled THEN
        -- Both stages have caught up with this relation. Whatever intent-level
        -- control ran before us has already had its say; this gate has no
        -- further objection. THIS ARM MATTERS: a gate that refuses everything is
        -- as broken as one that refuses nothing.
        RETURN NULL;
    END IF;

    -- From here a scan is affordable: either we abort the statement, or the
    -- caller is about to destroy the table anyway.
    EXECUTE format(
        'SELECT count(*), min(created_at), max(created_at) '
        'FROM public.%I WHERE created_at >= $1',
        TG_TABLE_NAME)
      INTO v_rows, v_min, v_max
      USING v_cutoff;

    IF coalesce(current_setting('billing.allow_unrolled_metering_truncate', true), '')
       <> 'on' THEN
        RAISE EXCEPTION
            'TRUNCATE of %.% would delete % un-aggregated metering event(s)',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, v_rows
            USING
                ERRCODE = 'P0001',
                DETAIL = format(
                    'metering rollup covers up to %s (hourly frontier %s, daily frontier '
                    '%s -- the EARLIER binds); %s row(s) with created_at in [%s, %s] have '
                    'not been aggregated by both stages. Deleting them destroys usage '
                    'that no other table witnesses.',
                    v_cutoff,
                    coalesce(v_hourly_ts::text, '(no hourly bucket has ever been written)'),
                    coalesce(v_daily_max::text, '(no daily bucket has ever been written)'),
                    v_rows, coalesce(v_min::text, '-'), coalesce(v_max::text, '-')),
                HINT =
                    'On 2026-09-05 one statement truncated this table alongside the cost '
                    'ledger and NOTHING was recoverable. Run the rollup until BOTH '
                    'billing_metering_hourly and billing_metering_daily cover these rows, '
                    'then retry. If you MEAN to delete un-aggregated metering, say so '
                    'explicitly in the same transaction: '
                    'SET LOCAL billing.allow_unrolled_metering_truncate = ''on''; '
                    'SET LOCAL billing.delete_reason = ''<why>''; -- then TRUNCATE. '
                    'That is a DIFFERENT GUC from the cost ledger''s '
                    'billing.allow_unrolled_truncate, deliberately: arming one ledger '
                    'must not silently authorise draining the other in the same '
                    'statement. That path is audited into billing_delete_audit as '
                    'TRUNCATE_UNROLLED_METERING_OVERRIDE.';
    END IF;

    -- Override taken. Record it BEFORE the rows go, with the numbers that are
    -- otherwise unreconstructable -- the 2026-09-05 loss had to be characterised
    -- by decoding UUIDv7 timestamps because no row like this existed.
    INSERT INTO public.billing_delete_audit (
        table_name, rows_deleted, operation, reason, db_user,
        session_user_name, application_name, client_addr, backend_pid,
        txid, statement, created_at_min, created_at_max
    ) VALUES (
        TG_TABLE_NAME,
        -- The UN-AGGREGATED count, not the total the statement removes. 0016's
        -- AFTER TRUNCATE row carries the statement; this row carries the loss.
        v_rows,
        'TRUNCATE_UNROLLED_METERING_OVERRIDE',
        format(
            'unrolled-metering-truncate override: %s row(s) after cutoff %s '
            '(hourly %s, daily %s). operator reason: %s',
            v_rows,
            v_cutoff,
            coalesce(v_hourly_ts::text, '(none)'),
            coalesce(v_daily_max::text, '(none)'),
            coalesce(nullif(current_setting('billing.delete_reason', true), ''),
                     'unspecified')),
        current_user, session_user,
        nullif(current_setting('application_name', true), ''),
        inet_client_addr(), pg_backend_pid(),
        pg_current_xact_id_if_assigned()::text::bigint,
        left(current_query(), 8000),
        v_min, v_max
    );

    RETURN NULL;
END
$fn$;
"""


def _metering_gate_attach_ddl(relation: str) -> str:
    """Idempotent BEFORE TRUNCATE gate attachment for one relation."""
    return f"""
DROP TRIGGER IF EXISTS {METERING_GATE_TRIGGER} ON public.{relation};
CREATE TRIGGER {METERING_GATE_TRIGGER}
    BEFORE TRUNCATE ON public.{relation}
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.billing_forbid_unrolled_metering_truncate();
"""


#: Attach to any partition of ``billing_metering_events`` that exists.
#:
#: ⚠️ Today this loop finds NOTHING: the table is ``relkind = 'r'``. It is here
#: anyway because Postgres does not clone statement-level TRUNCATE triggers to
#: partitions, so the day the table is partitioned this DDL is the difference
#: between a covered control and a hole in the exact place the 2026-09-05
#: statement landed. An empty loop costs one catalog scan at migration time.
ATTACH_METERING_GATE_TO_EXISTING_PARTITIONS_DDL = r"""
DO $do$
DECLARE
    v_parent oid;
    part     text;
BEGIN
    v_parent := to_regclass('public.billing_metering_events');
    IF v_parent IS NULL THEN
        RETURN;
    END IF;
    FOR part IN
        SELECT c.relname
          FROM pg_inherits i
          JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent = v_parent
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS billing_unrolled_metering_truncate_guard_trg '
            'ON public.%I', part);
        EXECUTE format(
            'CREATE TRIGGER billing_unrolled_metering_truncate_guard_trg '
            'BEFORE TRUNCATE ON public.%I FOR EACH STATEMENT '
            'EXECUTE FUNCTION public.billing_forbid_unrolled_metering_truncate()', part);
    END LOOP;
END
$do$;
"""


#: The whole control, in application order. Alembic 0023 executes exactly this.
METERING_UNROLLED_GUARD_DDL: tuple[str, ...] = (
    METERING_UNROLLED_GUARD_FUNCTION_DDL,
    _metering_gate_attach_ddl("billing_metering_events"),
    ATTACH_METERING_GATE_TO_EXISTING_PARTITIONS_DDL,
)


def _arm(target, connection, **kw) -> None:  # noqa: ANN001, ARG001
    for stmt in METERING_UNROLLED_GUARD_DDL:
        # Same driver-boundary escaping as delete_audit._arm, truncate_guard._arm
        # and unrolled_guard._arm: psycopg3 reads "%" in a driver-level statement
        # as a placeholder and this DDL is full of plpgsql format() specifiers.
        # Escaped HERE, never in the constants, because alembic's op.execute()
        # does not interpolate.
        connection.exec_driver_sql(stmt.replace("%", "%%"))


# Armed on the metadata create hook for the reason delete_audit, truncate_guard
# and unrolled_guard all spell out: the test suite builds its schema with
# create_all rather than alembic, so without this the gate would exist only in
# the migration and every test of it would be vacuous.
#
# ⚠️ Unlike unrolled_guard this module replaces NO other module's function --
# billing_metering_events has no partition-minting function -- so it carries no
# import-order constraint of its own. It is still imported after unrolled_guard
# in ``models/db/__init__``, purely so the three truncate controls read in the
# order they were added.
event.listen(MeteringEvent.__table__, "after_create", _arm)
