"""The BEFORE UPDATE OR DELETE guard on ``billing_invoices`` (alembic 0024, body
replaced by 0030).

Everything asserted here is currently a CLAIM made only in a commit message
(``6151183``: "close the trigger's whitespace bypass, arm the ORM path, add the
close service"). Nothing before this file held any of it: not the byte-identity
of the two DDL copies, not the trigger's state machine, and not the ORM-arming
path (``invoice_immutability.py``'s ``after_create`` hooks) that the alembic
migration alone cannot exercise, because the test suite's schema comes from
``create_all``, never from a migration.

See ``src/models/db/invoice_immutability.py`` for the trigger's own reasoning
(the four states, why "everything else unchanged" is one jsonb expression
rather than a column denylist, why voiding has no escape hatch).

⛔ AND SECTION B COULD NOT SEE THE MINT. Everything in section B defends a row
that is ALREADY issued; the UPDATE that MAKES it issued was unguarded except for
two NULL checks, so one statement could set ``status = 'issued'`` and rewrite
``total_usd`` at the same time and the result was a frozen, void-only-defended
statement of record carrying a figure no generator produced. Alembic 0030 closed
that with the same allowlist-diff shape the void branch already used; section B2
holds it.
"""

from __future__ import annotations

import importlib.util
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

import models.db.invoice_immutability as ii

_VERSIONS = Path(__file__).parent.parent / "alembic" / "versions"

#: The migration that CREATED the function and attached the trigger. FROZEN.
MIGRATION_0024 = _VERSIONS / "0024_billing_invoices.py"
#: The NEWEST installer of the function body: 0030 ``CREATE OR REPLACE``s it to
#: close the draft->issued hole. This is the drift anchor — see the test below.
MIGRATION_0030 = _VERSIONS / "0030_draft_to_issued_money_diff.py"


def _load_migration(path: Path = MIGRATION_0024):  # noqa: ANN202
    spec = importlib.util.spec_from_file_location(f"_mig_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# A. Byte-identity — asserted only in a commit message until now
# ---------------------------------------------------------------------------


def test_migration_ddl_is_byte_identical_to_the_module() -> None:
    """0012/0015 exist only because this repo let two schema sources drift once.

    0024's own docstring states the migration "imports nothing: the DDL is
    duplicated here verbatim so the migration stays frozen in time, and a test
    asserts the copies are byte-identical so they cannot drift" -- but nothing
    ran that test. Verified equal by sha256 by hand on 2026-09-08; this is what
    holds it, going forward, rather than relying on someone re-checking by hand.

    ⚠️ THE ANCHOR IS THE *NEWEST INSTALLER*, NOT 0024, AND RE-POINTING IT IS THE
    ONLY CORRECT RESPONSE TO A FUNCTION CHANGE — NOT EDITING 0024.
    ----------------------------------------------------------------------------
    This test used to load 0024 alone and assert its
    ``INVOICE_IMMUTABILITY_FUNCTION_DDL`` equalled the module's. That pairing was
    only coherent while the function never changed again, and it put a session
    that fixes the function in front of a false choice: red this test forever, or
    edit 0024's copy to match. **The second is the destructive one.** A migration
    is a historical record of DDL that has already run on live databases: editing
    0024 makes this tree describe a past it did not have, it does not re-run
    anywhere, and it silently desynchronises this repo from every database that
    applied the real 0024. 0024 stays byte-frozen, permanently, including its
    pre-0030 copy of the function with the draft->issued hole in it.

    So the anchor moves instead. What this test protects is unchanged and is the
    only thing it ever protected: **the two schema sources must not silently
    diverge.** A database built by ``alembic upgrade head`` and one built by
    ``Base.metadata.create_all`` (which is what every test in this file runs
    against, via ``invoice_immutability.py``'s ``after_create`` hooks) have to end
    up with the SAME function body. There are two copies of that body because the
    convention here is that a migration imports nothing, and two copies of
    anything drift unless something compares them. The copy that decides what a
    migrated database actually ends up with is the one in the LAST migration to
    ``CREATE OR REPLACE`` the function — today 0030.

    ⚠️ WHEN 0031 REPLACES THIS FUNCTION AGAIN: point ``MIGRATION_0030`` at it,
    assert its ``DOWNGRADE_DDL`` restores **0030's** body (not 0024's), and leave
    both older migrations untouched. The chain below is what keeps that honest:

    1. module == newest installer            -- create_all and alembic agree
    2. newest installer's downgrade == the body it superseded
                                            -- the downgrade genuinely reverses
    3. 0024's copy != the module's           -- 0024 was NOT "fixed" to match;
                                               if someone edits it, this fails
    4. everything 0030 does not touch (the CREATE TRIGGER statements, the DROP
       DDL, the guarded-table list) is still byte-identical to 0024's copies
                                            -- 0024 remains the live anchor for
                                               the half it still owns
    """
    newest = _load_migration(MIGRATION_0030)
    frozen = _load_migration(MIGRATION_0024)

    # 1. The body a migrated database ends up with, versus the body create_all
    #    installs. This is the assertion that actually stops the two schema
    #    sources from diverging.
    assert newest.INVOICE_IMMUTABILITY_FUNCTION_DDL == ii.INVOICE_IMMUTABILITY_FUNCTION_DDL
    assert newest.UPGRADE_DDL == (ii.INVOICE_IMMUTABILITY_FUNCTION_DDL,)

    # 2. 0030's downgrade claims to "restore the 0029-era definition". That is a
    #    measurement here rather than a claim in a comment: the restored body has
    #    to be 0024's, byte for byte, or a downgrade leaves the database running
    #    a function that no migration in this tree describes.
    assert newest.DOWNGRADE_DDL == (frozen.INVOICE_IMMUTABILITY_FUNCTION_DDL,)

    # 3. And 0024 must NOT have been quietly edited to match the module — the
    #    cheap way to make assertion 1 pass while rewriting history. 0030 changed
    #    the body, so these two are necessarily different; if they are ever equal
    #    again, someone reached for 0024 instead of writing a new migration.
    assert frozen.INVOICE_IMMUTABILITY_FUNCTION_DDL != ii.INVOICE_IMMUTABILITY_FUNCTION_DDL

    # 4. 0030 replaces the FUNCTION only. Everything else about this control is
    #    still 0024's, and still has to match: the CREATE TRIGGER statements
    #    (element [1:] of the tuple — [0] is the function body, which 0030 owns
    #    now), the inverse DDL, and the table list the attachments are generated
    #    from.
    assert frozen.INVOICE_IMMUTABILITY_DDL[1:] == ii.INVOICE_IMMUTABILITY_DDL[1:]
    assert frozen.INVOICE_IMMUTABILITY_DROP_DDL == ii.INVOICE_IMMUTABILITY_DROP_DDL
    assert frozen.GUARDED_INVOICE_TABLES == ii.GUARDED_INVOICE_TABLES


def test_0030_is_chained_and_not_a_sibling_head() -> None:
    """0030 must EXTEND 0029, not fork from it.

    Two revisions sharing a ``down_revision`` give alembic two heads; ``upgrade
    head`` then fails, and a single-head check run against either branch cannot
    see the other. This repo has shipped that defect before, which is why 0029
    carries the same test.
    """
    mig = _load_migration(MIGRATION_0030)
    assert mig.revision == "0030_draft_to_issued_money_diff"
    assert mig.down_revision == "0029_audit_log_tamper_evidence"


# ---------------------------------------------------------------------------
# Row helpers — raw SQL against the create_all schema, deliberately not the ORM
# ---------------------------------------------------------------------------


def _insert_draft_sql(invoice_id: uuid.UUID) -> tuple[sa.TextClause, dict]:
    return (
        sa.text(
            "INSERT INTO billing_invoices ("
            "id, tenant_id, period_start, period_end, subtotal_usd, total_usd, "
            "internal_cost_usd, margin_usd, billing_mode, markup_pct, "
            "effective_markup_pct, snapshot, inputs, snapshot_sha256, generator_version"
            ") VALUES ("
            ":id, gen_random_uuid(), '2026-01-01', '2026-02-01', 100, 130, 100, 30, "
            "'standard_markup', 30.00, 30.0000, '{}'::jsonb, '{}'::jsonb, "
            "'deadbeef', 'invoice.v1'"
            ")"
        ),
        {"id": invoice_id},
    )


async def _seed_draft(conn) -> uuid.UUID:  # noqa: ANN001
    invoice_id = uuid.uuid4()
    stmt, params = _insert_draft_sql(invoice_id)
    await conn.execute(stmt, params)
    return invoice_id


async def _issue(conn, invoice_id: uuid.UUID, *, number: str = "INV-TEST-000001") -> None:  # noqa: ANN001
    """draft -> issued in one UPDATE, satisfying the trigger's own requirement
    that invoice_number and issued_at move together."""
    await conn.execute(
        sa.text(
            "UPDATE billing_invoices SET status = 'issued', invoice_number = :num, "
            "issued_at = now() WHERE id = :id"
        ),
        {"id": invoice_id, "num": number},
    )


@pytest.fixture
async def issued_invoice_id(db_engine):
    """A billing_invoices row taken from draft to issued, on the create_all schema."""
    async with db_engine.begin() as conn:
        invoice_id = await _seed_draft(conn)
        await _issue(conn, invoice_id)
    return invoice_id


def _sqlstate(exc: DBAPIError) -> str | None:
    return getattr(exc.orig, "sqlstate", None)


def _diagnostics(exc: DBAPIError) -> str:
    """MESSAGE + DETAIL + HINT as one lower-cased haystack.

    ⚠️ ``str(exc)`` alone is NOT enough for the column-naming assertions below.
    The trigger puts the changed-column list in ``USING DETAIL`` (so the primary
    message stays a stable one-liner), and psycopg3 keeps DETAIL and HINT in
    ``diag``, which a plain ``str()`` of the wrapped SQLAlchemy error is not
    guaranteed to carry. Reading ``diag`` explicitly and falling back to
    ``str()`` means a test that claims "the error names the changed column" is
    actually looking where the column name is written -- otherwise it would pass
    or fail on the formatting of an exception wrapper rather than on the control.
    """
    diag = getattr(exc.orig, "diag", None)
    parts = [str(exc)]
    for attr in ("message_primary", "message_detail", "message_hint"):
        parts.append(str(getattr(diag, attr, "") or ""))
    return " ".join(parts).lower()


# ---------------------------------------------------------------------------
# B. The trigger's state machine
# ---------------------------------------------------------------------------


async def test_update_of_an_issued_row_is_refused(db_engine, issued_invoice_id) -> None:
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await conn.execute(
                sa.text("UPDATE billing_invoices SET total_usd = total_usd + 1000 WHERE id = :id"),
                {"id": issued_invoice_id},
            )
    assert _sqlstate(exc.value) == "P0001", exc.value


async def test_delete_of_an_issued_row_is_refused(db_engine, issued_invoice_id) -> None:
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await conn.execute(
                sa.text("DELETE FROM billing_invoices WHERE id = :id"), {"id": issued_invoice_id}
            )
    assert _sqlstate(exc.value) == "P0001", exc.value


@pytest.mark.parametrize(
    "void_reason",
    [None, "", "\t"],
    ids=["null", "empty", "tab-only"],
)
async def test_voiding_with_a_blank_reason_is_refused(
    db_engine, issued_invoice_id, void_reason: str | None
) -> None:
    """The real bypass: single-argument ``btrim`` trims the SPACE character
    only, so a NULL check alone let a tab- or newline-only reason through and
    withdraw a statement with a justification that renders blank to every human
    who later reads it."""
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await conn.execute(
                sa.text(
                    "UPDATE billing_invoices SET status = 'void', void_reason = :reason, "
                    "voided_at = now(), voided_by = 'tester' WHERE id = :id"
                ),
                {"id": issued_invoice_id, "reason": void_reason},
            )
    assert _sqlstate(exc.value) == "P0001", exc.value


async def test_reviving_a_void_invoice_is_refused(db_engine, issued_invoice_id) -> None:
    async with db_engine.begin() as conn:
        await conn.execute(
            sa.text(
                "UPDATE billing_invoices SET status = 'void', void_reason = 'test void', "
                "voided_at = now(), voided_by = 'tester' WHERE id = :id"
            ),
            {"id": issued_invoice_id},
        )
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await conn.execute(
                sa.text("UPDATE billing_invoices SET status = 'issued' WHERE id = :id"),
                {"id": issued_invoice_id},
            )
    assert _sqlstate(exc.value) == "P0001", exc.value


async def test_voiding_an_issued_invoice_with_a_real_reason_succeeds(
    db_engine, issued_invoice_id
) -> None:
    """⛔ REQUIRED. Without this, every refusal test above would still pass if
    the trigger were a blanket refusal on any UPDATE of an issued row — this is
    the one case that proves it is not."""
    async with db_engine.begin() as conn:
        await conn.execute(
            sa.text(
                "UPDATE billing_invoices SET status = 'void', "
                "void_reason = 'customer requested credit', voided_at = now(), "
                "voided_by = 'ops-tester' WHERE id = :id"
            ),
            {"id": issued_invoice_id},
        )
        row = (
            await conn.execute(
                sa.text(
                    "SELECT status, void_reason, voided_by FROM billing_invoices WHERE id = :id"
                ),
                {"id": issued_invoice_id},
            )
        ).one()
    assert row.status == "void"
    assert row.void_reason == "customer requested credit"
    assert row.voided_by == "ops-tester"


async def test_voiding_while_also_changing_total_usd_is_refused(
    db_engine, issued_invoice_id
) -> None:
    """Voiding must not be a laundering route for an otherwise-forbidden edit."""
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await conn.execute(
                sa.text(
                    "UPDATE billing_invoices SET status = 'void', "
                    "void_reason = 'trying to sneak an edit in', voided_at = now(), "
                    "voided_by = 'tester', total_usd = total_usd + 1 WHERE id = :id"
                ),
                {"id": issued_invoice_id},
            )
    assert _sqlstate(exc.value) == "P0001", exc.value


# ---------------------------------------------------------------------------
# B2. ISSUANCE — the mint, which is the edge the 'issued' branch cannot see
# ---------------------------------------------------------------------------
# Everything in section B defends a row that is ALREADY issued. None of it could
# see the statement that makes it issued, and that statement used to be able to
# change anything: the draft branch checked invoice_number/issued_at were
# non-NULL and then returned NEW. So the guard froze the row one instant AFTER
# the only moment a figure could still be rewritten -- and the result was a
# frozen, immutable, void-only-defended statement of record carrying a total no
# generator ever computed, permanently disagreeing with its own
# snapshot_sha256. Closed by alembic 0030.


async def test_issuing_while_also_changing_total_usd_is_refused(db_engine) -> None:
    """⛔ THE DEFECT 0030 CLOSES. One statement, a valid-looking issuance, and a
    rewritten total.

    ``invoice_number`` and ``issued_at`` are BOTH supplied deliberately: without
    them the old NULL check would refuse this UPDATE too, and the test would pass
    against the unfixed function while measuring nothing about the money. The
    assertion on the message text is what separates the two refusals -- this must
    fail because a column outside the issuance allowlist moved, not because the
    row was missing its number.
    """
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            invoice_id = await _seed_draft(conn)
            await conn.execute(
                sa.text(
                    "UPDATE billing_invoices SET status = 'issued', "
                    "invoice_number = 'INV-TEST-000900', issued_at = now(), "
                    "issued_by = 'tester', total_usd = 999999 WHERE id = :id"
                ),
                {"id": invoice_id},
            )
    assert _sqlstate(exc.value) == "P0001", exc.value
    diagnostics = _diagnostics(exc.value)
    # The refusal must name the column that moved -- an operator who cannot see
    # WHICH column the guard objected to has to bisect the UPDATE by hand.
    assert "total_usd" in diagnostics, diagnostics
    # ...and it must be the allowlist refusal, not the older "cannot be issued
    # without invoice_number and issued_at" one. If this assertion ever fails
    # while the one above passes, the test has stopped measuring 0030.
    assert "may not change any other column" in diagnostics, diagnostics
    assert "without invoice_number" not in diagnostics, diagnostics


async def test_a_legitimate_draft_to_issued_update_succeeds(db_engine) -> None:
    """⛔ REQUIRED, and for the reason the void test above gives: without it,
    ``test_issuing_while_also_changing_total_usd_is_refused`` would pass just as
    happily against a function that refused EVERY draft->issued transition --
    i.e. against a change that bricks issuance entirely.

    The four columns are exactly what ``services.invoices.issue`` writes
    (``invoice_number``, ``status``, ``issued_at``, ``issued_by``), which is where
    the allowlist came from; this is the test that would red if that set and the
    trigger's ever disagree.
    """
    async with db_engine.begin() as conn:
        invoice_id = await _seed_draft(conn)
        await conn.execute(
            sa.text(
                "UPDATE billing_invoices SET status = 'issued', "
                "invoice_number = 'INV-TEST-000901', issued_at = now(), "
                "issued_by = 'ops-tester' WHERE id = :id"
            ),
            {"id": invoice_id},
        )
        row = (
            await conn.execute(
                sa.text(
                    "SELECT status, invoice_number, issued_by, issued_at, total_usd "
                    "FROM billing_invoices WHERE id = :id"
                ),
                {"id": invoice_id},
            )
        ).one()
    assert row.status == "issued"
    assert row.invoice_number == "INV-TEST-000901"
    assert row.issued_by == "ops-tester"
    assert row.issued_at is not None
    # The draft's figure, carried through untouched. Issuance is a stamp.
    assert row.total_usd == Decimal("130.00000000")


# ---------------------------------------------------------------------------
# C. Controls — a refusal for the wrong reason is not a pass
# ---------------------------------------------------------------------------


async def test_the_same_update_against_a_draft_row_succeeds(db_engine) -> None:
    """A draft is WORKING STATE and must stay freely mutable.

    ⚠️ ALSO THE CONTROL ON 0030, and the reason that migration tightened the
    draft->issued TRANSITION rather than the draft branch as a whole. This UPDATE
    rewrites ``total_usd`` and leaves ``status`` at ``'draft'``; it must keep
    succeeding. A guard that refused it would have made the generator, the
    re-render path and every operator correction illegal -- which is how a
    "tighten the draft branch" change gets reverted wholesale, taking the real
    fix with it.
    """
    async with db_engine.begin() as conn:
        invoice_id = await _seed_draft(conn)
        await conn.execute(
            sa.text("UPDATE billing_invoices SET total_usd = total_usd + 1000 WHERE id = :id"),
            {"id": invoice_id},
        )
        row = (
            await conn.execute(
                sa.text("SELECT total_usd FROM billing_invoices WHERE id = :id"), {"id": invoice_id}
            )
        ).one()
    assert row.total_usd == Decimal("1130.00000000")


async def test_without_the_trigger_the_same_update_succeeds(db_engine, issued_invoice_id) -> None:
    """If this raises, every refusal asserted above was measuring something
    else. Run inside a SAVEPOINT, explicitly rolled back afterwards -- the
    schema is session-scoped, and leaving the trigger dropped would silently
    disarm it for every later test in the session."""
    async with db_engine.begin() as conn:
        nested = await conn.begin_nested()
        try:
            await conn.execute(
                sa.text("DROP TRIGGER billing_invoice_immutability_trg ON public.billing_invoices")
            )
            # Must not raise.
            await conn.execute(
                sa.text("UPDATE billing_invoices SET total_usd = total_usd + 1000 WHERE id = :id"),
                {"id": issued_invoice_id},
            )
            row = (
                await conn.execute(
                    sa.text("SELECT total_usd FROM billing_invoices WHERE id = :id"),
                    {"id": issued_invoice_id},
                )
            ).one()
        finally:
            await nested.rollback()
    assert row.total_usd == Decimal("1130.00000000")


# ---------------------------------------------------------------------------
# D. TRUNCATE on the ORM-built (create_all) schema
# ---------------------------------------------------------------------------


async def test_truncate_of_billing_invoices_is_refused_under_create_all(db_engine) -> None:
    """The ORM-arming path: ``invoice_immutability.py``'s ``after_create``
    listener attaches ``billing_truncate_guard_trg`` to ``billing_invoices`` the
    same way alembic 0024 does, so a plain ``TRUNCATE`` against the schema the
    whole test suite runs against is refused too. This was broken until
    ``6151183`` and a migration-only test cannot see it: the test suite never
    runs alembic, only ``Base.metadata.create_all``.

    This guarantee is proven only for ``billing_invoices`` as ``create_all``
    builds it here: an ordinary, unpartitioned table. See
    ``test_the_fixture_schema_cannot_express_partitioning_so_truncate_coverage_is_unproven``
    below — Postgres does not clone a statement-level ``BEFORE TRUNCATE``
    trigger to a table's partitions, so if ``billing_invoices`` were ever
    partitioned by a future migration, this test would keep passing (it would
    still refuse TRUNCATE on the unpartitioned fixture table) while the real,
    partitioned production table silently lost the guard on every child. This
    file cannot detect that regression; nothing here can.
    """
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await conn.execute(sa.text("TRUNCATE billing_invoices"))
    assert "blocked" in str(exc.value).lower(), exc.value


# ---------------------------------------------------------------------------
# E. Known-positive blindness probe — what this fixture schema CANNOT see
# ---------------------------------------------------------------------------


async def test_the_fixture_schema_cannot_express_partitioning_so_truncate_coverage_is_unproven(
    db_engine,
) -> None:
    """A PASS here does not mean "partitioning is fine" — it means the exact
    opposite: this file's TRUNCATE/DDL assertions are blind to partitioning and
    always will be, no matter what ``billing_invoices`` looks like in production.

    ``billing_cost_events`` is a known positive: alembic 0001 (baseline)
    declares it ``PARTITION BY RANGE (created_at)`` and 0001 itself attaches 14
    monthly child partitions (``billing_cost_events_2026_04`` through
    ``..._2027_05``) via ``ALTER TABLE ... ATTACH PARTITION``. So in a
    migration-built database ``pg_class.relkind`` for ``billing_cost_events`` is
    ``'p'`` (partitioned table), not ``'r'`` (ordinary table).

    But the ORM model backing it, ``CostEvent`` in
    ``src/models/db/events.py``, is a plain ``Base`` subclass with no
    partitioning directive at all -- ``Base.metadata.create_all()`` (what
    ``db_engine`` above is built from, see ``tests/conftest.py``'s
    ``_db_engine_session``) can only ever emit an ordinary
    ``CREATE TABLE billing_cost_events (...)``. It has no mechanism to express
    ``PARTITION BY`` or to attach children -- SQLAlchemy's DDL compiler does not
    round-trip that from an ORM mapping.

    Querying the SAME engine/session every test in this file uses, against the
    SAME table name production partitions, and getting ``'r'`` back proves this
    fixture schema is INCAPABLE of representing a partitioned table -- not just
    that this one happens not to be partitioned today. Consequently: every
    property this file asserts about a table under ``create_all`` (the TRUNCATE
    refusal above, and -- if ``billing_invoices`` is ever partitioned by a
    future migration -- the BEFORE UPDATE/DELETE trigger tests in section B)
    is proven ONLY for the unpartitioned shape and is UNPROVEN for whatever
    ``billing_invoices`` becomes if it is ever partitioned. This matters
    specifically because Postgres does not clone a statement-level TRUNCATE (or
    UPDATE/DELETE) trigger from a partitioned parent to its children: a trigger
    declared ``FOR EACH STATEMENT`` on the parent fires for a statement issued
    directly against the parent, but covers no child partition on its own, so a
    real partitioning migration could silently leave every child unguarded
    while every test in this file kept passing against its unpartitioned
    fixture copy.
    """
    async with db_engine.begin() as conn:
        relkind = (
            await conn.execute(
                sa.text("SELECT relkind FROM pg_class WHERE relname = 'billing_cost_events'")
            )
        ).scalar_one()
    assert relkind == "r", (
        f"billing_cost_events reports relkind={relkind!r} under create_all; expected 'r'. "
        "If this ever starts returning 'p', the blindness this probe documents no longer "
        "holds and the docstring above (and the comment on "
        "test_truncate_of_billing_invoices_is_refused_under_create_all) need rewriting."
    )
