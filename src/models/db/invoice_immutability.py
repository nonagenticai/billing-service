"""A ``BEFORE UPDATE OR DELETE`` guard making an ISSUED invoice a record.

WHY A TRIGGER AND NOT APPLICATION CODE
--------------------------------------
Same reasoning as :mod:`.truncate_guard`, and the same measured precedent behind
it. ``billing_user`` OWNS these relations, so no GRANT can stop it; and on
2026-09-05 the thing that emptied the cost ledger was not this service's code
but an unidentified client holding the same credentials. A rule that lives only
in ``services.billing`` protects only callers that go through
``services.billing`` -- which is the one population that was never the risk.

A row-level ``BEFORE`` trigger sits between EVERY writer and the row, including
a psql session, a migration and a client with the wrong DSN.

THE FOUR STATES
---------------
``DELETE``  -- always refused, whatever the status. An invoice is the only
record that a statement was ever shown to a customer; deleting a DRAFT is
recoverable in the sense that nobody saw it, but the trigger cannot know that
and a per-status DELETE rule would be an exemption someone reaches for. Void
instead, or (for a never-issued draft) leave it.

``draft``   -- freely mutable WHILE IT STAYS A DRAFT. A draft is working state:
the generator writes it, an operator inspects it, it gets re-rendered. Two rules
attach to the UPDATE that LEAVES draft for ``issued``, because that single
statement is the act that mints the document:

1. It requires the two things that make the row a statement --
   ``invoice_number`` and ``issued_at``. An "issued" invoice with a NULL number
   is not a document, it is a row that will fail an audit later, far from the
   code that wrote it.
2. ⛔ AND IT MAY CHANGE NOTHING ELSE. Issuance is a STAMP, not a restatement:
   the only columns that may move are ``status``, ``invoice_number``,
   ``issued_at`` and ``issued_by``, which is exactly the set
   :func:`services.invoices.issue` writes. Without this, ``issued`` being frozen
   bought nothing: the guard froze the row one instant AFTER the only moment at
   which a rewritten total could be smuggled into a statement of record, so a
   single ``UPDATE ... SET status = 'issued', invoice_number = ..., issued_at =
   ..., total_usd = <anything>`` produced a frozen, immutable, audited document
   carrying a figure no generator ever computed -- and the digest in
   ``snapshot_sha256``, which covers the money columns, would then disagree with
   the row forever with nothing recording when they diverged.

``draft -> void`` keeps a draft's freedom, deliberately and with a narrower
justification than the above: a draft has never been shown to anyone, so
abandoning one with its figures edited destroys no claim anybody relied on, and
tightening it is a separate decision from closing the issuance hole. What it is
NOT is an oversight -- if it is ever tightened, note that ``services.invoices.void``
writes ``status``, ``void_reason``, ``voided_at`` and ``voided_by``, i.e. the
same allowlist the ``issued`` branch below already uses.

``issued``  -- frozen except for voiding. The one permitted UPDATE sets
``status`` to ``'void'`` with a non-empty ``void_reason`` and both
``voided_at`` and ``voided_by`` present. Everything else must be byte-identical.

``void``    -- terminal. Any UPDATE raises. A void statement is superseded by a
NEW invoice, never revived: reviving one means the same invoice number
described two different amounts at two different times, and nothing on the row
would record that it had.

⚠️ WHY "EVERYTHING ELSE UNCHANGED" IS ONE EXPRESSION, NOT A COLUMN LIST
-----------------------------------------------------------------------
The comparison appears TWICE, once per permitted transition, with a different
allowlist and the same shape. For a void::

    to_jsonb(OLD) - 'status' - 'voided_at' - 'voided_by' - 'void_reason'
      IS DISTINCT FROM
    to_jsonb(NEW) - 'status' - 'voided_at' - 'voided_by' - 'void_reason'

and for issuance::

    to_jsonb(OLD) - 'status' - 'invoice_number' - 'issued_at' - 'issued_by'
      IS DISTINCT FROM
    to_jsonb(NEW) - 'status' - 'invoice_number' - 'issued_at' - 'issued_by'

i.e. an ALLOWLIST of the four columns that transition may touch, applied to the
whole row. The obvious alternative -- ``IF NEW.total_usd <> OLD.total_usd OR
NEW.subtotal_usd <> OLD.subtotal_usd OR ...`` -- is a DENYLIST enumerating the
~40 columns that exist TODAY, and it rots in the worst possible direction: the
next ``ALTER TABLE ... ADD COLUMN`` produces a column the guard does not
mention, therefore does not compare, therefore silently permits an issued
invoice to be edited through. Nothing goes red; the control just quietly
develops a hole in the newest field. Under the jsonb form a new column is
guarded the instant it exists, and OPENING a column requires adding it to the
subtraction list here -- a visible, reviewable act.

⭐ THAT IS ALSO THE REASON THE ISSUANCE ALLOWLIST WAS WRITTEN THIS WAY RATHER
THAN AS "the money columns may not change". A money-only check would have been
the denylist again, in its most tempting costume: the set of columns worth
protecting on an invoice is not knowable in advance (``snapshot``, ``inputs``,
``snapshot_sha256``, ``generator_version``, ``tenant_name``, ``period_start`` and
the whole present-and-NULL legal set are each as load-bearing as ``total_usd``,
and ``_MONEY_COLUMNS`` in ``services/invoices.py`` is closed against exactly this
kind of drift), and a column added by a later migration would again arrive
unguarded. The allowlist names the four columns issuance is FOR; everything
else, including every column that does not exist yet, is frozen by default.

``IS DISTINCT FROM`` rather than ``<>`` so a NULL on either side compares as a
value: ``NULL <> NULL`` is NULL, which is not TRUE, which would make every
change involving a NULL column read as "unchanged".

RELATIONSHIP TO THE OTHER GUARDS
--------------------------------
Orthogonal, and all three are needed. ``billing_delete_audit_trg`` (0016)
RECORDS deletes on the event ledgers; ``billing_truncate_guard_trg`` (0017/0018)
REFUSES an unarmed TRUNCATE. Neither sees an UPDATE, and a rewritten total is
precisely the shape of tampering an invoice table has to refuse. Conversely
this trigger is ``FOR EACH ROW`` and TRUNCATE fires no row triggers at all, so
it does not overlap the truncate guard -- ``billing_invoices`` needs both.

⚠️ AND BOTH SCHEMA SOURCES MUST ATTACH BOTH, or the claim above is only true of
one of them. Alembic 0024 attaches both to ``billing_invoices`` and the truncate
guard to ``billing_invoice_counters``; the ``after_create`` hooks at the bottom
of this module do exactly the same for the ``create_all`` schema the test suite
runs against. Neither source can be left to the other: 0018's catalog sweep is a
one-shot ``DO`` block that cannot see a table created after it, and
``truncate_guard.py``'s own hooks arm only ``CostEvent`` and ``MeteringEvent``.
Without the hook here, ``TRUNCATE billing_invoices`` would succeed against every
test database while this docstring said it could not.

⛔ NO ESCAPE HATCH, DELIBERATELY -- and unlike the truncate guard, that
asymmetry is not an oversight. The truncate guard has a GUC because partition
maintenance, restores and the test suite all truncate legitimately. There is no
legitimate caller that edits an issued invoice: the correct move is always
void-and-reissue, which this trigger permits. A genuine restore replaces the
row via a path that recreates the table, not via UPDATE.
"""

from __future__ import annotations

from sqlalchemy import event

from .invoices import BillingInvoice, BillingInvoiceCounter
from .truncate_guard import TRUNCATE_GUARD_FUNCTION_DDL, _guard_attach_ddl

INVOICE_IMMUTABILITY_FUNCTION_DDL = r"""
CREATE OR REPLACE FUNCTION public.billing_forbid_invoice_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
DECLARE
    -- The row MINUS the allowlist for the transition in hand: the columns
    -- issuance may move in the draft branch, the columns a void may move in the
    -- issued branch. Everything else in the row is compared. A column added to
    -- billing_invoices later is guarded the moment it exists, without editing
    -- this function -- see the module docstring in
    -- src/models/db/invoice_immutability.py for why a per-column list was
    -- rejected. One pair of variables serves both branches because the branches
    -- are mutually exclusive on OLD.status and each assigns before it reads.
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
    -- it, it is re-rendered. Nothing to freeze yet -- UNLESS this is the UPDATE
    -- that leaves draft for 'issued', which is the act that mints the document
    -- and is therefore the one statement on a draft that has rules.
    IF OLD.status = 'draft' THEN
        IF NEW.status = 'issued' THEN
            IF NEW.invoice_number IS NULL OR NEW.issued_at IS NULL THEN
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

            -- ⛔ ISSUANCE IS A STAMP, NOT A RESTATEMENT. THIS BRANCH USED TO
            -- RETURN NEW HERE, and that made the whole 'issued' branch below
            -- worth very little: it froze the row one instant AFTER the only
            -- moment at which a figure could still be rewritten, so ONE
            -- statement --
            --   UPDATE billing_invoices
            --      SET status = 'issued', invoice_number = 'NA-2026-000001',
            --          issued_at = now(), total_usd = <anything>
            --    WHERE status = 'draft'
            -- -- minted a frozen, immutable, permanently unalterable document
            -- carrying a total no generator ever computed. Everything downstream
            -- then agreed with it: the row is issued, so the void-only guard
            -- defends it, and snapshot_sha256 (which DOES cover the money
            -- columns -- see _money_from_snapshot in services/invoices.py)
            -- disagrees with the row forever with nothing on the row recording
            -- when or why they diverged.
            --
            -- The allowlist is the four columns services.invoices.issue writes
            -- and nothing else: status, invoice_number, issued_at, issued_by
            -- (invoices.py, in the block that allocates the number). Same jsonb
            -- shape as the void check below, for the same reason -- a column
            -- added to this table later is frozen across issuance the moment it
            -- exists, and opening one means editing this subtraction list.
            --
            -- ⚠️ An operator who genuinely needs a different figure edits the
            -- DRAFT and issues afterwards; both are permitted, in that order.
            -- The refusal is only of doing both in the same statement, which is
            -- the spelling under which the edit leaves no reviewable trace.
            v_old_frozen := to_jsonb(OLD) - 'status' - 'invoice_number' - 'issued_at' - 'issued_by';
            v_new_frozen := to_jsonb(NEW) - 'status' - 'invoice_number' - 'issued_at' - 'issued_by';

            IF v_old_frozen IS DISTINCT FROM v_new_frozen THEN
                RAISE EXCEPTION
                    USING
                        ERRCODE = 'P0001',
                        MESSAGE =
                            'issuing invoice ' || coalesce(OLD.id::text, '<NULL>')
                            || ' may not change any other column',
                        -- Both sides are to_jsonb() of the SAME table, so the
                        -- key sets are identical and comparing OLD's keys
                        -- against NEW's values finds every difference. IS
                        -- DISTINCT FROM, again, so a column that became NULL is
                        -- reported rather than read as unchanged.
                        DETAIL =
                            'changed: '
                            || (SELECT coalesce(string_agg(o.key, ', ' ORDER BY o.key), '(none)')
                                  FROM jsonb_each(v_old_frozen) AS o
                                 WHERE v_new_frozen -> o.key IS DISTINCT FROM o.value),
                        HINT =
                            'Issuing freezes the draft AS IT STANDS; it is not an '
                            'opportunity to restate it. Only status, invoice_number, '
                            'issued_at and issued_by may move in this UPDATE. To change '
                            'a figure: UPDATE the row while it is still a draft, then '
                            'issue it in a SEPARATE statement -- or, if it is already '
                            'issued, void it with a reason and issue a replacement. A '
                            'total that arrives in the same statement as the invoice '
                            'number was never produced by the generator whose output '
                            'snapshot_sha256 attests to.';
            END IF;
        END IF;

        -- draft -> draft (freely mutable working state) and draft -> void (a
        -- statement nobody was ever shown, abandoned) both land here. The
        -- latter is NOT tightened, deliberately: no claim was relied on, so
        -- there is nothing to have been laundered, and narrowing it is a
        -- separate decision from closing the issuance hole above. See the
        -- module docstring.
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


#: The whole control, in application order.
#:
#: ⚠️ THE FUNCTION AND THE ATTACHMENTS NOW HAVE DIFFERENT NEWEST INSTALLERS, and
#: a reader checking one source against the other has to know which. Alembic 0024
#: executed this whole tuple and still owns element [1:] -- the CREATE TRIGGER
#: statements, which no later migration has touched. Element [0], the function
#: body, was REPLACED by alembic 0030, which CREATE OR REPLACEs it to close the
#: draft->issued hole; 0024 keeps its own frozen pre-0030 copy because a
#: migration is a historical record of what ran, not a description of the current
#: schema. ``tests/test_invoice_immutability.py`` asserts both halves against the
#: right migration and explains the anchoring rule.
INVOICE_IMMUTABILITY_DDL: tuple[str, ...] = (
    INVOICE_IMMUTABILITY_FUNCTION_DDL,
    *(_immutability_attach_ddl(t) for t in GUARDED_INVOICE_TABLES),
)


#: The inverse, in reverse dependency order: triggers first, then the function
#: they reference. Alembic 0024's downgrade executes exactly this.
INVOICE_IMMUTABILITY_DROP_DDL: tuple[str, ...] = (
    *(
        f"DROP TRIGGER IF EXISTS billing_invoice_immutability_trg ON public.{t};"
        for t in GUARDED_INVOICE_TABLES
    ),
    "DROP FUNCTION IF EXISTS public.billing_forbid_invoice_mutation();",
)


#: The tables alembic 0024 attaches ``billing_truncate_guard_trg`` to, in the
#: same order and with the same DDL. The names are repeated here rather than
#: imported from the migration for the reason 0016 gives: a migration is a
#: historical record and nothing may import from it.
#:
#: ⚠️ ``billing_forbid_truncate()`` is 0017's, and this module does NOT own it.
#: It is re-emitted (``CREATE OR REPLACE``, byte-identical, from
#: ``truncate_guard``) rather than assumed present because ``create_all``
#: creates tables in ``Base.metadata.sorted_tables`` order and may reach
#: ``billing_invoices`` before ``billing_cost_events`` -- i.e. before
#: truncate_guard's own hook has created the function. That is exactly what
#: ``truncate_guard._install_for`` does for its two tables, and this is the same
#: mechanism rather than a second one: same constant, same helper, same order.
TRUNCATE_GUARDED_INVOICE_TABLES = ("billing_invoices", "billing_invoice_counters")


def _install_for(relation: str) -> tuple[str, ...]:
    """The DDL that arms ONE relation under ``create_all``, in dependency order.

    Both controls, because the table needs both and they do not overlap: the
    immutability trigger is ``FOR EACH ROW`` and cannot see a TRUNCATE, and the
    truncate guard is ``FOR EACH STATEMENT ... BEFORE TRUNCATE`` and cannot see
    an UPDATE. ``billing_invoice_counters`` takes the truncate guard only -- it
    is an allocator, not a statement of record, and 0024 attaches only that one
    to it.
    """
    ddl: tuple[str, ...] = ()
    if relation in GUARDED_INVOICE_TABLES:
        ddl += INVOICE_IMMUTABILITY_DDL
    if relation in TRUNCATE_GUARDED_INVOICE_TABLES:
        ddl += (TRUNCATE_GUARD_FUNCTION_DDL, _guard_attach_ddl(relation))
    return ddl


def _arm(relation: str):  # noqa: ANN202
    def _listener(target, connection, **kw) -> None:  # noqa: ANN001, ARG001
        for stmt in _install_for(relation):
            # Same driver-boundary escaping as delete_audit._arm and
            # truncate_guard._arm: psycopg3 reads "%" in a driver-level
            # statement as a placeholder. It is a no-op for the immutability
            # DDL, which deliberately carries no percent sign at all (see the
            # comment at the top of billing_forbid_invoice_mutation), and it is
            # REQUIRED for 0017's billing_forbid_truncate(), whose RAISE does
            # use a format string. Escaped HERE, never in the constants,
            # because alembic's op.execute() does not interpolate -- doubling
            # them in the constant would put literal "%%" into production.
            connection.exec_driver_sql(stmt.replace("%", "%%"))

    return _listener


# Armed PER TABLE, for the reason delete_audit spells out: the test suite builds
# its schema with ``create_all`` rather than alembic, so without this hook the
# trigger would exist only in the migration and every test of it would be
# vacuous -- passing against a schema that has no trigger in it.
#
# ⛔ AND THE TRUNCATE GUARD IS ARMED FROM HERE TOO. 0018's catalog sweep is a
# one-shot DO block that already ran, so no table created after it is guarded by
# it; alembic 0024 therefore attaches billing_truncate_guard_trg to both new
# tables itself. truncate_guard.py's own hooks only cover CostEvent and
# MeteringEvent, so without this the ORM-built schema and the alembic-built
# schema would disagree about billing_invoices -- and a test asserting only the
# immutability trigger would pass while "TRUNCATE billing_invoices is refused"
# was untrue of the schema every test runs against.
#
# Unlike delete_audit/truncate_guard, this module redefines NO shared function
# (it owns billing_forbid_invoice_mutation() outright, re-emits 0017's
# billing_forbid_truncate() unchanged, and never touches
# create_cost_events_partition_for), so its import position in
# ``models/db/__init__.py`` is NOT load-bearing the way theirs is.
for _model in (BillingInvoice, BillingInvoiceCounter):
    event.listen(_model.__table__, "after_create", _arm(_model.__tablename__))
