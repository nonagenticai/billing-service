"""The invoice as a persisted, immutable OBJECT (+ a gapless number counter).

WHAT THIS CREATES
-----------------
``billing_invoices`` -- one rendered statement per (tenant, period), carrying
its own totals, the INPUTS it was computed from, a digest of the resolved rate
basis and the generator version -- and ``billing_invoice_counters``, an ordinary
row-per-(entity, year) allocator.

The reasoning behind both -- why an invoice is a record rather than a query, why
the legal/tax columns are present-and-NULL rather than added later, and why a
Postgres SEQUENCE cannot number a legal invoice series -- lives in
``src/models/db/invoices.py``. The reasoning behind the trigger lives in
``src/models/db/invoice_immutability.py``. This migration imports nothing: the
DDL is duplicated here verbatim so the migration stays frozen in time, and a
test asserts the copies are byte-identical so they cannot drift. Same discipline
as 0016, 0017 and 0018 (0012 and 0015 exist only because this repo once let two
schema sources drift; that is not repeated here).

⚠️ THE TRUNCATE GUARD IS ATTACHED EXPLICITLY, AND THIS IS A FLEET-WIDE HOLE
---------------------------------------------------------------------------
0018 widened ``billing_truncate_guard_trg`` from 14 relations to every billing
base table with a catalog-driven ``DO`` block --
``ATTACH_GUARD_TO_ALL_BASE_TABLES_DDL``. That block is ONE-SHOT. It ran once,
at 0018, against the tables that existed at 0018, and alembic will never run it
again. It is not a policy, it is a historical event.

So **every table added after 0018 is born UNGUARDED**, and nothing anywhere
says so: no test fails, no alert fires, and 0018's own docstring reads as
though the coverage is a standing property ("Re-running it after adding a table
re-closes the hole" -- true, and nothing re-runs it). ``billing_invoices`` is
the first table to hit this, and it is exactly the wrong table to leave
truncatable: it is the statement of record.

This migration therefore attaches the guard to both new tables itself, with its
own frozen copy of 0017's ``_guard_attach_ddl``. That closes the hole for THESE
two tables and no others.

⛔ IT DOES NOT CLOSE THE CLASS. Any table added by 0025+ will be unguarded again
unless its own migration remembers to do this. The durable fix is a check that
FAILS when a permanent ``public`` base table carries no
``billing_truncate_guard_trg`` -- a control that reads the catalog at test time
rather than a DO block that ran once in the past. That is deliberately not
smuggled into this PR; it is a fleet-wide change with its own blast radius. It
is recorded here so the next person adding a table finds the reason rather than
rediscovering the gap.

BOTH GUARDS ARE NEEDED, AND THEY DO NOT OVERLAP
-----------------------------------------------
``billing_invoice_immutability_trg`` is ``FOR EACH ROW``, and TRUNCATE fires no
row triggers at all -- so it cannot see a truncation. ``billing_truncate_guard_trg``
is ``FOR EACH STATEMENT ... BEFORE TRUNCATE`` and cannot see an UPDATE. A table
carrying only one of them is open on the other axis.

WHY THIS IS SAFE ON A LIVE DATABASE
-----------------------------------
Purely additive: two new tables, one new function, three new triggers. No
existing column is altered, no existing row is touched, no existing function is
replaced (in particular ``create_cost_events_partition_for`` is untouched, so
0016's audit half and 0017's guard half are both undisturbed). Every statement
is idempotent (``IF NOT EXISTS`` / ``DROP TRIGGER IF EXISTS`` /
``CREATE OR REPLACE``), so a re-run is a no-op.

LOCK DISCIPLINE
---------------
Same bound as 0016/0017/0018 and for the same reason, though the exposure here
is smaller: ``CREATE TRIGGER`` takes ``ShareRowExclusiveLock``, which conflicts
with the ``RowExclusiveLock`` every ``INSERT`` holds, and the lock queue is
FIFO, so a blocked DDL puts every subsequent INSERT behind itself. The two
tables this creates are brand new and nothing can be writing to them -- but the
migration still bounds its own wait rather than relying on that, because the
next person to add a statement here will not re-derive the argument.

⛔ NOT APPLIED TO PROD BY THIS PR.

Revision ID: 0024_billing_invoices
Revises: 0022_provider_pricing_cost_basis
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

# Parent re-pointed 2026-09-08 once billing-service #151 merged
# (88890ea8398e65b3ab4153b2c58acee4768829f0). This revision was authored against
# 0022 while #151 was still open; both claiming down_revision="0022_…" would have
# put TWO HEADS on main, which breaks `alembic upgrade head` for every lane and
# every deploy — it does not fail for the author, it fails for whoever migrates
# next. Verified before re-pointing by parsing every revision/down_revision on
# origin/main and taking those never named as a parent: 23 revisions, exactly one
# head, "0023_metering_unrolled_gate". Not the filename sort, which is not the
# graph, and not the merge SHA, which is not the revision id.
revision: str = "0024_billing_invoices"  # <= 32 chars: alembic_version.version_num
down_revision: str | None = "0023_metering_unrolled_gate"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# --- DDL, kept byte-identical to src/models/db/invoice_immutability.py -----
# A test asserts this equality. Edit both or neither.

INVOICE_IMMUTABILITY_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_invoice_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
DECLARE
    -- The ALLOWLIST: the only columns a void may move. Everything else in the
    -- row is compared. A column added to billing_invoices later is guarded the
    -- moment it exists, without editing this function -- see the module
    -- docstring in src/models/db/invoice_immutability.py for why a per-column
    -- list was rejected.
    v_old_frozen jsonb;
    v_new_frozen jsonb;
BEGIN
    -- ⛔ THIS FUNCTION BODY CARRIES NO PERCENT SIGN, DELIBERATELY. Every
    -- message is built with || and handed to RAISE as USING MESSAGE/DETAIL
    -- rather than through a RAISE format string. This DDL ships two ways:
    -- alembic's op.execute(), which passes the text to the server untouched,
    -- and exec_driver_sql() from the create_all hook in
    -- invoice_immutability.py, which crosses the psycopg3 boundary where a
    -- percent sign is a parameter placeholder and has to be doubled. With none
    -- in the body both paths carry byte-identical SQL, there is no escaping
    -- left to get wrong, and there is no way to land a "too many parameters
    -- specified for RAISE" -- a failure that would surface at VOID time, in
    -- production, rather than at install time. Do not reintroduce format() or
    -- a RAISE format string here.
    --
    -- coalesce() on every interpolated value for the same class of reason:
    -- '||' with a NULL operand yields NULL, and USING MESSAGE = NULL raises
    -- 22004 instead of the refusal the caller has to be able to read.
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION
            USING
                ERRCODE = 'P0001',
                MESSAGE =
                    'DELETE of billing invoice ' || coalesce(OLD.id::text, '<NULL>')
                    || ' (status ' || coalesce(OLD.status, '<NULL>') || ') is blocked',
                HINT =
                    'An issued statement is a record, not a row: it is the only '
                    'evidence that this amount was shown to this customer. Deleting '
                    'it leaves nothing behind that says it ever existed. VOID it '
                    'instead -- UPDATE billing_invoices SET status = ''void'', '
                    'void_reason = ''<why>'', voided_at = now(), voided_by = ''<who>'' '
                    'WHERE id = ''<id>''; -- then issue a replacement invoice.';
    END IF;

    -- A draft is working state: the generator writes it, an operator inspects
    -- it, it is re-rendered. Nothing to freeze yet.
    IF OLD.status = 'draft' THEN
        IF NEW.status = 'issued'
           AND (NEW.invoice_number IS NULL OR NEW.issued_at IS NULL) THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' cannot be issued without invoice_number and issued_at',
                    HINT =
                        'Issuing is the act that makes this row a document. Allocate '
                        'the next number from billing_invoice_counters in THIS '
                        'transaction (SELECT ... FOR UPDATE -- the series must stay '
                        'gapless) and set issued_at in the same UPDATE.';
        END IF;
        RETURN NEW;
    END IF;

    -- Void is terminal. Reviving one would let a single invoice number describe
    -- two different amounts, with nothing on the row recording that it had.
    IF OLD.status = 'void' THEN
        RAISE EXCEPTION
            USING
                ERRCODE = 'P0001',
                MESSAGE =
                    'invoice ' || coalesce(OLD.id::text, '<NULL>')
                    || ' is void; void is terminal and admits no UPDATE',
                HINT =
                    'A voided statement is superseded by a NEW invoice for the same '
                    'period, never revived. ux_billing_invoices_open_period excludes '
                    'void rows precisely so the period can be re-issued.';
    END IF;

    IF OLD.status = 'issued' THEN
        IF NEW.status <> 'void' THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' is issued; the only permitted change is to ''void'''
                        || ' (attempted status ' || coalesce(NEW.status, '<NULL>') || ')',
                    HINT =
                        'An issued statement has been shown to a customer and must '
                        'keep saying the same thing. To correct it: void this row '
                        'with a reason, then issue a replacement invoice.';
        END IF;

        -- ⚠️ btrim(NEW.void_reason) = '' WAS THE BUG. DO NOT SIMPLIFY BACK TO IT.
        -- Single-argument btrim() strips the SPACE character and nothing else --
        -- not tab, not newline, not U+00A0 -- so E'\t', E'\n' and U&'\00A0' all
        -- read as a "non-empty" reason, and an issued statement could be
        -- withdrawn with a justification that renders blank to every human who
        -- later asks why. The test is now "does this reason consist ENTIRELY of
        -- whitespace or invisible formatting characters".
        --
        -- The Unicode code points are listed explicitly ALONGSIDE [[:space:]]
        -- rather than relying on it: [[:space:]] resolves through the server's
        -- ctype table, glibc does not classify U+00A0 as space in every locale,
        -- and a control whose coverage depends on the database's LC_CTYPE is a
        -- control that is silently weaker on some hosts than on the one it was
        -- tested on. The listed set is Unicode White_Space plus the zero-width
        -- and BOM characters, which are not White_Space but are just as blank
        -- on a rendered invoice.
        IF NEW.void_reason IS NULL
           OR NEW.void_reason ~ '^[[:space:]\u0085\u00a0\u1680\u2000-\u200d\u2028\u2029\u202f\u205f\u2060\u3000\ufeff]*$' THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' requires a non-empty void_reason',
                    HINT =
                        'The reason is the whole audit value of a void: without it '
                        'the record says a statement was withdrawn and nothing about '
                        'why, which is what an auditor will ask. A reason that is '
                        'only whitespace is refused for exactly the same reason a '
                        'NULL one is -- it answers nothing.';
        END IF;

        IF NEW.voided_at IS NULL OR NEW.voided_by IS NULL THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' requires voided_at and voided_by',
                    HINT =
                        'WHEN and WHO, recorded on the row itself. Set voided_at = '
                        'now() and voided_by to the acting identity in the same '
                        'UPDATE that sets status.';
        END IF;

        v_old_frozen := to_jsonb(OLD) - 'status' - 'voided_at' - 'voided_by' - 'void_reason';
        v_new_frozen := to_jsonb(NEW) - 'status' - 'voided_at' - 'voided_by' - 'void_reason';

        IF v_old_frozen IS DISTINCT FROM v_new_frozen THEN
            RAISE EXCEPTION
                USING
                    ERRCODE = 'P0001',
                    MESSAGE =
                        'voiding invoice ' || coalesce(OLD.id::text, '<NULL>')
                        || ' may not change any other column',
                    -- Both sides are to_jsonb() of the SAME table, so the key
                    -- sets are identical and comparing OLD's keys against
                    -- NEW's values finds every difference. IS DISTINCT FROM,
                    -- again, so a column that became NULL is reported.
                    DETAIL =
                        'changed: '
                        || (SELECT coalesce(string_agg(o.key, ', ' ORDER BY o.key), '(none)')
                              FROM jsonb_each(v_old_frozen) AS o
                             WHERE v_new_frozen -> o.key IS DISTINCT FROM o.value),
                    HINT =
                        'A void withdraws a statement; it does not restate it. Only '
                        'status, void_reason, voided_at and voided_by may move. To '
                        'change a figure, void this row and issue a replacement.';
        END IF;

        RETURN NEW;
    END IF;

    -- Unreachable while ck_billing_invoices_status holds. Reached only if a
    -- future migration widens that CHECK without teaching this function what
    -- the new status means -- in which case refusing is the correct default:
    -- an unrecognised status must not inherit 'draft''s freedom.
    RAISE EXCEPTION
        USING
            ERRCODE = 'P0001',
            MESSAGE =
                'invoice ' || coalesce(OLD.id::text, '<NULL>')
                || ' carries unrecognised status ' || coalesce(OLD.status, '<NULL>'),
            HINT =
                'billing_forbid_invoice_mutation() knows draft, issued and void. A '
                'status added to ck_billing_invoices_status must be given an '
                'explicit branch here (and considered for '
                'ux_billing_invoices_open_period''s predicate) before it is used.';
END
$fn$;
"""


def _immutability_attach_ddl(relation: str) -> str:
    """Idempotent BEFORE UPDATE OR DELETE guard attachment for one relation."""
    return f"""
DROP TRIGGER IF EXISTS billing_invoice_immutability_trg ON public.{relation};
CREATE TRIGGER billing_invoice_immutability_trg
    BEFORE UPDATE OR DELETE ON public.{relation}
    FOR EACH ROW EXECUTE FUNCTION public.billing_forbid_invoice_mutation();
"""


GUARDED_INVOICE_TABLES = ("billing_invoices",)


INVOICE_IMMUTABILITY_DDL: tuple[str, ...] = (
    INVOICE_IMMUTABILITY_FUNCTION_DDL,
    *(_immutability_attach_ddl(t) for t in GUARDED_INVOICE_TABLES),
)


INVOICE_IMMUTABILITY_DROP_DDL: tuple[str, ...] = (
    *(
        f"DROP TRIGGER IF EXISTS billing_invoice_immutability_trg ON public.{t};"
        for t in GUARDED_INVOICE_TABLES
    ),
    "DROP FUNCTION IF EXISTS public.billing_forbid_invoice_mutation();",
)


# --- truncate guard attachment, byte-identical to 0017 / truncate_guard.py --
#
# ⛔ DUPLICATED, NOT IMPORTED, and that is this repo's rule rather than laziness:
# 0016's docstring states it outright ("This migration imports nothing: the DDL
# is duplicated here verbatim so the migration stays frozen in time"), and 0017
# carries its own copy of exactly this helper for the same reason. A migration
# that imports from ``src/models/db`` replays whatever that module says TODAY,
# not what it said when the revision was written -- which is how a historical
# migration silently changes meaning.
#
# The trigger NAME is a literal here, as it is in 0017 and 0018. 0018's
# idempotency predicate matches on that exact spelling; a shared constant that
# could be edited on one side would let the two drift, and the failure would be
# a second trigger on every already-guarded ledger.
_TRUNCATE_GUARDED_TABLES = ("billing_invoices", "billing_invoice_counters")


def _guard_attach_ddl(relation: str) -> str:
    """Idempotent BEFORE TRUNCATE guard attachment for one relation."""
    return f"""
DROP TRIGGER IF EXISTS billing_truncate_guard_trg ON public.{relation};
CREATE TRIGGER billing_truncate_guard_trg
    BEFORE TRUNCATE ON public.{relation}
    FOR EACH STATEMENT EXECUTE FUNCTION public.billing_forbid_truncate();
"""


#: Fail loudly if 0017's function is missing rather than emitting CREATE
#: TRIGGERs that each fail with a bare "function does not exist". Copied from
#: 0018, which added it for the same reason.
REQUIRE_GUARD_FUNCTION_DDL = r"""
DO $do$
BEGIN
    IF to_regprocedure('public.billing_forbid_truncate()') IS NULL THEN
        RAISE EXCEPTION
            'alembic 0024 requires public.billing_forbid_truncate() from 0017'
            USING HINT =
                'The guard function is missing, so there is nothing to attach. '
                'Run alembic 0017 first, or downgrade past 0024 as well.';
    END IF;
END
$do$;
"""


# --- lock discipline (see "LOCK DISCIPLINE" in the module docstring) --------

#: 3 seconds, the same bound 0016, 0017 and 0018 chose against the same hot path.
LOCK_TIMEOUT = "3s"

#: Catalog-only work once the locks are held; 60s cannot be hit by healthy work.
STATEMENT_TIMEOUT = "60s"

#: 55P03 lock_not_available / 57014 query_canceled.
_TIMEOUT_SQLSTATES = frozenset({"55P03", "57014"})

_RETRY_IN_A_QUIETER_WINDOW = f"""
alembic 0024 could not acquire its DDL locks within {LOCK_TIMEOUT}. The whole
migration has ROLLED BACK - nothing was applied, and it is safe to re-run.

Find the blocking transaction with:

    SELECT pid, state, xact_start, wait_event_type, left(query, 120)
      FROM pg_stat_activity
     WHERE datname = current_database() AND xact_start IS NOT NULL
     ORDER BY xact_start;

RETRY IN A QUIETER WINDOW (or after that transaction ends).

Do NOT raise LOCK_TIMEOUT to get past this. CREATE TRIGGER takes
ShareRowExclusiveLock, which conflicts with the RowExclusiveLock every INSERT
holds, and the lock queue is FIFO: while this migration waits, every NEW insert
queues behind it. A longer timeout is a longer stall, not a better chance of
success.
""".strip()


def _bound_the_wait() -> None:
    """Bound this migration's lock wait, and prove the bound actually took.

    ``SET LOCAL`` outside a transaction block is a WARNING and a no-op, which
    would leave this guard vacuous -- passing while protecting nothing. Read
    back rather than trusted, same as 0016, 0017 and 0018.
    """
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    op.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    effective = op.get_bind().exec_driver_sql("SHOW lock_timeout").scalar()
    if effective in (None, "0", 0):
        raise RuntimeError(
            "alembic 0024 refuses to run without a bounded lock_timeout: SET LOCAL "
            f"reported lock_timeout={effective!r}, which means this is not running "
            "inside a transaction block and the DDL below could queue the billing "
            "INSERT path behind itself indefinitely."
        )


def _fail_fast(fn):  # noqa: ANN001, ANN202
    """Turn a lock/statement timeout into an instruction the operator can act on."""
    try:
        fn()
    except DBAPIError as exc:
        sqlstate = getattr(exc.orig, "sqlstate", None)
        if sqlstate in _TIMEOUT_SQLSTATES:
            raise RuntimeError(_RETRY_IN_A_QUIETER_WINDOW) from exc
        raise


def upgrade() -> None:
    _bound_the_wait()
    _fail_fast(_upgrade_ddl)


def _upgrade_ddl() -> None:
    op.create_table(
        "billing_invoices",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", sa.Uuid(as_uuid=False), nullable=False),
        sa.Column("tenant_name", sa.Text(), server_default="", nullable=False),
        sa.Column("period_start", sa.Date(), nullable=False),
        # EXCLUSIVE end: [period_start, period_end).
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("status", sa.Text(), server_default="draft", nullable=False),
        sa.Column("invoice_number", sa.Text(), nullable=True),
        # --- present-and-null legal set. NOTHING IN THIS PR WRITES THESE. ---
        sa.Column("currency", sa.Text(), server_default="USD", nullable=False),
        sa.Column("fx_rate", sa.Numeric(20, 8), nullable=True),
        sa.Column("fx_base_currency", sa.Text(), nullable=True),
        sa.Column("tax_scheme", sa.Text(), nullable=True),
        sa.Column("tax_rate", sa.Numeric(9, 6), nullable=True),
        sa.Column("tax_amount", sa.Numeric(20, 8), nullable=True),
        sa.Column("legal_entity", sa.Text(), nullable=True),
        sa.Column("customer_legal_name", sa.Text(), nullable=True),
        sa.Column("billing_address", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        # --- money ---------------------------------------------------------
        sa.Column("subtotal_usd", sa.Numeric(20, 8), nullable=False),
        sa.Column("total_usd", sa.Numeric(20, 8), nullable=False),
        sa.Column("internal_cost_usd", sa.Numeric(20, 8), nullable=False),
        sa.Column("margin_usd", sa.Numeric(20, 8), nullable=False),
        sa.Column("access_charge_usd", sa.Numeric(20, 8), server_default="0", nullable=False),
        sa.Column("platform_fee_usd", sa.Numeric(20, 8), server_default="0", nullable=False),
        sa.Column("llm_usage_usd", sa.Numeric(20, 8), server_default="0", nullable=False),
        sa.Column("llm_markup_usd", sa.Numeric(20, 8), server_default="0", nullable=False),
        sa.Column("seat_fee_usd", sa.Numeric(20, 8), server_default="0", nullable=False),
        sa.Column("seat_count", sa.Integer(), nullable=True),
        # --- disclosure of what was NOT billed -------------------------------
        sa.Column("excluded_unbillable_usd", sa.Numeric(20, 8), server_default="0", nullable=False),
        sa.Column("excluded_unbillable_rows", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "excluded_by_status",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        # --- provenance ------------------------------------------------------
        sa.Column("billing_mode", sa.Text(), nullable=False),
        sa.Column("markup_pct", sa.Numeric(6, 2), nullable=False),
        sa.Column("effective_markup_pct", sa.Numeric(9, 4), nullable=False),
        sa.Column("snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("inputs", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("snapshot_sha256", sa.Text(), nullable=False),
        sa.Column(
            "lines",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("generator_version", sa.Text(), nullable=False),
        # --- lifecycle -------------------------------------------------------
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("issued_by", sa.Text(), nullable=True),
        sa.Column("voided_by", sa.Text(), nullable=True),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("voided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("void_reason", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('draft', 'issued', 'void')",
            name="ck_billing_invoices_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_table(
        "billing_invoice_counters",
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("period_year", sa.Integer(), nullable=False),
        sa.Column("next_seq", sa.BigInteger(), server_default="1", nullable=False),
        sa.PrimaryKeyConstraint("scope"),
        if_not_exists=True,
    )

    op.create_index(
        "ux_billing_invoices_number",
        "billing_invoices",
        ["invoice_number"],
        unique=True,
        postgresql_where=sa.text("invoice_number IS NOT NULL"),
        if_not_exists=True,
    )
    # ⛔ POSITIVE PREDICATE, never `status <> 'void'`. A status added later (a
    # plausible 'credited' / 'superseded') would JOIN the uniqueness set under
    # `<>` with no edit here and no test going red, and the table would start
    # refusing legitimate re-issues. See src/models/db/invoices.py.
    op.create_index(
        "ux_billing_invoices_open_period",
        "billing_invoices",
        ["tenant_id", "period_start", "period_end"],
        unique=True,
        postgresql_where=sa.text("status IN ('draft', 'issued')"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_billing_invoices_tenant_period",
        "billing_invoices",
        ["tenant_id", sa.text("period_start DESC")],
        if_not_exists=True,
    )

    for stmt in INVOICE_IMMUTABILITY_DDL:
        op.execute(stmt)

    # See "THE TRUNCATE GUARD IS ATTACHED EXPLICITLY" in the module docstring:
    # 0018's catalog sweep is a one-shot DO block that already ran, so it cannot
    # see a table created here.
    op.execute(REQUIRE_GUARD_FUNCTION_DDL)
    for relation in _TRUNCATE_GUARDED_TABLES:
        op.execute(_guard_attach_ddl(relation))


def downgrade() -> None:
    # DROP TRIGGER / DROP TABLE take the same ShareRowExclusiveLock (and
    # AccessExclusiveLock) as the upgrade's DDL, so the downgrade can stall
    # exactly the way the upgrade can. Same bound, same instruction.
    _bound_the_wait()
    _fail_fast(_downgrade_ddl)


def _downgrade_ddl() -> None:
    # ORDER. The trigger REFERENCES the function, so the function cannot be
    # dropped while the trigger exists (without CASCADE, which would be a
    # blunter instrument than this needs). Triggers first, then the function --
    # which is exactly INVOICE_IMMUTABILITY_DROP_DDL's order -- and only then
    # the tables. Dropping the tables first would take the triggers with them
    # and leave the DROP TRIGGER statements erroring on a missing relation.
    for stmt in INVOICE_IMMUTABILITY_DROP_DDL:
        op.execute(stmt)

    # The truncate-guard triggers on these two tables need no explicit DROP:
    # DROP TABLE removes a relation's own triggers with it. The guard FUNCTION
    # is emphatically NOT dropped -- it belongs to 0017 and is still attached to
    # every other billing table; 0017's own downgrade is what removes it.
    op.drop_table("billing_invoice_counters", if_exists=True)
    op.drop_table("billing_invoices", if_exists=True)
    # Indexes go with the table they are on; no separate drop_index needed.
