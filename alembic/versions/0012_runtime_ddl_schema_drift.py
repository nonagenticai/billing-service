"""Bring the objects that only ``src/db.py`` creates into the migration chain.

WHAT IS BROKEN
--------------
This service builds part of its schema TWICE. ``alembic/versions/*`` is one
source of truth; the ``ADDITIVE_MIGRATIONS`` tuple in ``src/db.py`` — replayed on
every pod start — is the other. Six objects exist ONLY in the second one, so **a
fresh alembic-only database is a genuinely different schema from production**:

===============================================  ===================
object                                            created at
===============================================  ===================
``billing_cost_daily.total_cost_usd`` @ 20,6      ``src/db.py:77``
``billing_cost_daily.owner_provided``             ``src/db.py:179``
``provider_pricing.rate_class``                   ``src/db.py:207``
table ``tenant_access_charge``                    ``src/db.py:219``
index ``ix_accesscharge_lookup``                  ``src/db.py:230``
index ``ix_accesscharge_open``                    ``src/db.py:232``
===============================================  ===================

This is not a tidiness complaint. It has already cost a migration:
``0011_tenant_id_is_a_uuid`` ABORTED on a brand-new database because
``_merge('billing_cost_daily', …)`` asserted a column list containing
``owner_provided``, which no migration creates — see the comment at
``0011_tenant_id_is_a_uuid.py:725`` and its "empty table" workaround, and the
one at ``:518`` where ``_tenant_columns`` has to tolerate ``tenant_access_charge``
simply not existing. Every future migration that touches these objects carries
the same landmine, and every fresh environment silently diverges from prod until
the first pod boots and replays ``ADDITIVE_MIGRATIONS``.

THE TWO NON-OBVIOUS MEMBERS OF THAT LIST
----------------------------------------
``ix_accesscharge_open`` (``src/db.py:232``) is not just another index: it is the
UNIQUE index whose exact key ``0011`` encodes in
``NO_MERGE_KEYS['tenant_access_charge']`` (``0011_tenant_id_is_a_uuid.py:413``)
in order to decide which post-remap collisions must RAISE rather than merge.
Creating the table without it hands a fresh database a ``tenant_access_charge``
that silently accepts two OPEN access charges for one (tenant, service_key) —
two rows of record for one bill.

``billing_cost_daily.total_cost_usd`` is a TYPE divergence rather than a missing
object, and it is included DELIBERATELY (it was previously written off as
out-of-scope, wrongly). The baseline creates it ``numeric(12,6)``
(``0001_billing_baseline.py:120``); ``src/db.py:70-77`` widens it to
``numeric(20,6)`` because the old precision caps ONE tuple's daily aggregate at
$999,999.999999 and a high-volume tenant's rollup — the invoice source — then
fails the day with a numeric overflow. It belongs here, unlike the two objects
below, because widening is purely additive: ``numeric(20,6)`` is a strict
superset of ``numeric(12,6)`` (no value can fail to fit, no rounding changes,
the scale is untouched), Postgres applies it as a catalogue-only change via
``numeric``'s typmod transform (no table rewrite, no scan), and re-running it
against an already-widened column is a no-op. It needs no ``DROP``, unlike:

DELIBERATELY OUT OF SCOPE (measured, not overlooked)
-----------------------------------------------------
Two more objects differ between the alembic-only schema and production, and this
migration does NOT close them, because closing them requires a ``DROP``:

* ``uq_cost_daily``. Baseline ``0001_billing_baseline.py:1252`` creates it
  WITHOUT ``owner_provided``; ``src/db.py:193-196`` drops and recreates it WITH.
  Adding the column here does not widen the constraint.
* ``ix_providerpricing_lookup`` / ``ix_providerpricing_open``.
  ``0001_billing_baseline.py:2131``/``:2138`` create them without ``rate_class``;
  ``src/db.py:208-214`` drops and recreates them with ``rate_class`` leading, so
  a ``'list'`` row can coexist with the ``'cost'`` row for the same tuple.

Both are index/constraint REPLACEMENTS, not additions. A ``DROP INDEX`` against a
live database whose pods are mid-roll is a different risk class from an additive
statement, and it belongs in its own reviewed migration. They are named here so
the next reader inherits the measurement instead of re-deriving it.

WHY EVERY STATEMENT IS RE-RUNNABLE, AND WHY THAT IS *NOT* RACE-SAFE BY ITSELF
-----------------------------------------------------------------------------
Production ALREADY HAS all six objects — ``ADDITIVE_MIGRATIONS`` has been putting
them there on every pod start for months — so this migration must be an exact
no-op there. Postgres' ``IF NOT EXISTS`` (and, for the widening, the fact that
``ALTER … TYPE`` to the type a column already has is a no-op) gives that.

``IF NOT EXISTS`` does **NOT** additionally make the statement safe against a
CONCURRENT creator, and an earlier version of this docstring claimed it did.
``IF NOT EXISTS`` only consults COMMITTED catalogue rows. If another session has
created the object but has not committed yet, this session sees nothing, proceeds
to insert its own ``pg_type``/``pg_class``/``pg_attribute`` row, blocks on that
catalogue's unique index, and — when the other session commits — fails::

    ERROR: duplicate key value violates unique constraint "pg_type_typname_nsp_index"
    DETAIL: Key (typname, typnamespace)=(tenant_access_charge, 2200) already exists.

That race is not hypothetical here: ``src/db.py`` replays ``ADDITIVE_MIGRATIONS``
on EVERY pod start, so a surviving replica rebooting while the ``migrate`` init
container runs is the normal case, not the pathological one. And because
``alembic/env.py:75-76`` wraps the whole run in ONE transaction, a single failed
statement aborts EVERYTHING: measured on a scratch database, the loser did not
merely skip the table, it rolled back to ``0011`` with even the unrelated
``owner_provided`` column gone.

Catching the error in Python is therefore not enough on its own — after a failed
statement the enclosing transaction is poisoned (``current transaction is
aborted``) until something rolls back. Each statement consequently runs inside
its own SAVEPOINT (``conn.begin_nested()``): a race-class failure rolls back
THAT statement only, the migration continues, and :func:`_verify` afterwards
proves the object the race lost is actually there (it will be — the session that
won the race committed it; if it instead rolled back, ``_verify`` RAISES, which
is the correct outcome).

Which SQLSTATEs count as "a race" is not re-derived here: ``src/db.py:250``
already answers it for the runtime replay (``_RACE_CLASS_ORIG_NAMES`` — 42710
duplicate_object, 42P07 duplicate_table, 42701 duplicate_column, 23505
unique_violation, matched on the psycopg exception class name). The same set is
restated below rather than imported, so this migration cannot be broken by a
later refactor of a private name in application code; a test in
``tests/test_migration_0012_schema_drift.py`` asserts the two sets are equal, so
they cannot drift apart in silence.

The DDL text below is COPIED VERBATIM from ``src/db.py``. If the two ever
disagree, a fresh database and production disagree again — which is the entire
defect this migration closes — so the types, nullability, defaults and index
expressions here must not be "cleaned up" independently of ``src/db.py``.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy import exc as sa_exc

revision = "0012_runtime_ddl_schema_drift"
down_revision = "0011_tenant_id_is_a_uuid"
branch_labels = None
depends_on = None


# ---------------------------------------------------------------------------
# The DDL. Byte-identical to the corresponding entries of `ADDITIVE_MIGRATIONS`
# in `src/db.py` (line numbers in the comments), because a divergence between
# the two IS the defect.
# ---------------------------------------------------------------------------

STATEMENTS: tuple[str, ...] = (
    # src/db.py:70-77 — widen billing_cost_daily.total_cost_usd 12,6 -> 20,6.
    # Additive (superset domain, catalogue-only); see the module docstring.
    "ALTER TABLE billing_cost_daily ALTER COLUMN total_cost_usd TYPE NUMERIC(20,6)",
    # src/db.py:179 — 0033 owner_provided (managed BYO) on billing_cost_daily.
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS owner_provided BOOLEAN NOT NULL DEFAULT false",
    # src/db.py:206-207 — 0031 ProviderPricing.rate_class: 'cost' vs 'list'.
    "ALTER TABLE provider_pricing "
    "ADD COLUMN IF NOT EXISTS rate_class VARCHAR(16) NOT NULL DEFAULT 'cost'",
    # src/db.py:219-229 — 0032 tenant_access_charge, the ACCESS-billing axis.
    # tenant_id is UUID (alembic 0011) — must track models/db/config.py, whose
    # `TenantAccessCharge.tenant_id` is `Uuid(as_uuid=False), nullable=False`.
    "CREATE TABLE IF NOT EXISTS tenant_access_charge ("
    "id BIGSERIAL PRIMARY KEY, "
    "tenant_id UUID NOT NULL, "
    "service_key VARCHAR(64), "
    "amount_usd NUMERIC(20,8) NOT NULL, "
    "cadence VARCHAR(16) NOT NULL DEFAULT 'monthly', "
    "included_cost_usd NUMERIC(20,8), "
    "effective_from TIMESTAMPTZ NOT NULL, "
    "effective_until TIMESTAMPTZ, "
    "notes TEXT)",
    # src/db.py:230-231
    "CREATE INDEX IF NOT EXISTS ix_accesscharge_lookup "
    "ON tenant_access_charge (tenant_id, coalesce(service_key, ''), effective_from DESC)",
    # src/db.py:232-234 — the open-row uniqueness 0011's NO_MERGE_KEYS depends on.
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_accesscharge_open "
    "ON tenant_access_charge (tenant_id, coalesce(service_key, '')) "
    "WHERE effective_until IS NULL",
)


# ---------------------------------------------------------------------------
# Race handling. Mirrors `_RACE_CLASS_ORIG_NAMES` / `_is_race_class_error` in
# `src/db.py:250` — see the module docstring for why it is restated, not
# imported, and where the equality of the two sets is enforced.
# ---------------------------------------------------------------------------

RACE_CLASS_ORIG_NAMES: frozenset[str] = frozenset(
    {"DuplicateObject", "DuplicateTable", "DuplicateColumn", "UniqueViolation"}
)


def _is_race_class_error(exc: BaseException) -> bool:
    """True when ``exc`` wraps a duplicate-object / unique-violation driver error."""
    orig = getattr(exc, "orig", None)
    # `orig` is None for SQLAlchemy-level errors that never reached the driver,
    # so test it explicitly rather than relying on the getattr default.
    return orig is not None and type(orig).__name__ in RACE_CLASS_ORIG_NAMES


# ---------------------------------------------------------------------------
# Postconditions. Read back from the catalogue rather than trusted, because
# every statement above is a no-op when an object of that NAME already exists:
# "created it", "silently did nothing", and "found something else wearing the
# name" are otherwise the same exit code.
#
# The check is on the DEFINITION, never the name. A database that already held
# `rate_class` as `text`, or `ix_accesscharge_open` as a NON-unique index with
# no `WHERE` clause, passed a name-only check while still being the schema this
# migration exists to prevent — measured: after such a "successful" upgrade,
# two OPEN access charges for one (tenant, service_key) inserted fine.
# ---------------------------------------------------------------------------

# "measured, and deliberately not asserted": `tenant_access_charge.id`'s default
# is `nextval('<sequence>'::regclass)`, whose sequence name depends on how the
# table was created (BIGSERIAL vs an IDENTITY/`create_all` variant). The column
# being `bigint NOT NULL` is what matters; pinning the sequence's spelling would
# turn a cosmetic difference into a production abort.
_ANY: Any = object()

_COLUMN_ATTRS: tuple[str, ...] = (
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "is_nullable",
    "column_default",
)

EXPECTED_COLUMNS: tuple[tuple[str, str, dict[str, Any]], ...] = (
    (
        "billing_cost_daily",
        "total_cost_usd",
        {
            "data_type": "numeric",
            "character_maximum_length": None,
            "numeric_precision": 20,
            "numeric_scale": 6,
            "is_nullable": "NO",
            "column_default": None,
        },
    ),
    (
        "billing_cost_daily",
        "owner_provided",
        {
            "data_type": "boolean",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "NO",
            "column_default": "false",
        },
    ),
    (
        "provider_pricing",
        "rate_class",
        {
            "data_type": "character varying",
            "character_maximum_length": 16,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "NO",
            "column_default": "'cost'::character varying",
        },
    ),
    # The whole `tenant_access_charge` shape: `CREATE TABLE IF NOT EXISTS` is a
    # no-op against ANY table of that name, so "the table exists" says nothing
    # about the table being the one 0011 and services/access_charge.py assume.
    (
        "tenant_access_charge",
        "id",
        {
            "data_type": "bigint",
            "character_maximum_length": None,
            "numeric_precision": 64,
            "numeric_scale": 0,
            "is_nullable": "NO",
            "column_default": _ANY,
        },
    ),
    (
        "tenant_access_charge",
        "tenant_id",
        {
            "data_type": "uuid",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "NO",
            "column_default": None,
        },
    ),
    (
        "tenant_access_charge",
        "service_key",
        {
            "data_type": "character varying",
            "character_maximum_length": 64,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "YES",
            "column_default": None,
        },
    ),
    (
        "tenant_access_charge",
        "amount_usd",
        {
            "data_type": "numeric",
            "character_maximum_length": None,
            "numeric_precision": 20,
            "numeric_scale": 8,
            "is_nullable": "NO",
            "column_default": None,
        },
    ),
    (
        "tenant_access_charge",
        "cadence",
        {
            "data_type": "character varying",
            "character_maximum_length": 16,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "NO",
            "column_default": "'monthly'::character varying",
        },
    ),
    (
        "tenant_access_charge",
        "included_cost_usd",
        {
            "data_type": "numeric",
            "character_maximum_length": None,
            "numeric_precision": 20,
            "numeric_scale": 8,
            "is_nullable": "YES",
            "column_default": None,
        },
    ),
    (
        "tenant_access_charge",
        "effective_from",
        {
            "data_type": "timestamp with time zone",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "NO",
            "column_default": None,
        },
    ),
    (
        "tenant_access_charge",
        "effective_until",
        {
            "data_type": "timestamp with time zone",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "YES",
            "column_default": None,
        },
    ),
    (
        "tenant_access_charge",
        "notes",
        {
            "data_type": "text",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "is_nullable": "YES",
            "column_default": None,
        },
    ),
)

EXPECTED_TABLE = "tenant_access_charge"

# Index definitions, not index names. `unique` comes from `pg_index.indisunique`
# (it is rendered BEFORE the name in `pg_get_indexdef`, so it is not in the tail
# below) and `definition_tail` is everything from `USING ` onwards in
# `pg_get_indexdef` — the access method, the key expressions in order, and the
# partial-index predicate. Schema- and name-qualification are deliberately cut
# off the front: they vary with `search_path`, and the identity of the index is
# already fixed by the lookup.
EXPECTED_INDEXES: tuple[tuple[str, str, bool, str], ...] = (
    (
        "ix_accesscharge_lookup",
        EXPECTED_TABLE,
        False,
        "btree (tenant_id, COALESCE(service_key, ''::character varying), effective_from DESC)",
    ),
    (
        "ix_accesscharge_open",
        EXPECTED_TABLE,
        True,
        "btree (tenant_id, COALESCE(service_key, ''::character varying)) "
        "WHERE (effective_until IS NULL)",
    ),
)


def _verify(conn: sa.Connection) -> None:
    """RAISE naming anything the statements failed to leave behind, or left WRONG.

    Two failure classes, both fatal and both named precisely:

    * ABSENT — the object is simply not there. Either a statement did not run
      or the session that won a DDL race rolled back instead of committing.
    * MISMATCHED — an object of that name exists but is not the object
      ``src/db.py`` builds. This is the case a name-only postcondition let
      through, and it is the more dangerous of the two, because the migration
      then reports success over exactly the schema divergence it exists to
      close. ``tenant_access_charge.tenant_id`` being ``uuid`` is part of this
      check: ``0011``'s ``NO_MERGE_KEYS`` guard for the table depends on it, and
      finding out at bill time is strictly worse than finding out here.
    """
    absent: list[str] = []
    mismatched: list[str] = []

    for table, column, expected in EXPECTED_COLUMNS:
        row = (
            conn.execute(
                sa.text(
                    f"SELECT {', '.join(_COLUMN_ATTRS)} FROM information_schema.columns "  # noqa: S608
                    "WHERE table_schema = current_schema() "
                    "  AND table_name = :t AND column_name = :c"
                ),
                {"t": table, "c": column},
            )
            .mappings()
            .first()
        )
        if row is None:
            absent.append(f"column {table}.{column}")
            continue
        for attr in _COLUMN_ATTRS:
            want = expected[attr]
            if want is _ANY:
                continue
            got = row[attr]
            if got != want:
                mismatched.append(f"column {table}.{column}: {attr} is {got!r}, expected {want!r}")

    if conn.execute(sa.text("SELECT to_regclass(:t)"), {"t": EXPECTED_TABLE}).scalar() is None:
        absent.append(f"table {EXPECTED_TABLE}")

    for name, table, unique, definition_tail in EXPECTED_INDEXES:
        row = (
            conn.execute(
                sa.text(
                    "SELECT rel.relname AS table_name, idx.indisunique AS is_unique, "
                    "       pg_get_indexdef(idx.indexrelid) AS indexdef "
                    "FROM pg_index idx "
                    "JOIN pg_class ind ON ind.oid = idx.indexrelid "
                    "JOIN pg_class rel ON rel.oid = idx.indrelid "
                    "JOIN pg_namespace nsp ON nsp.oid = ind.relnamespace "
                    "WHERE ind.relname = :n AND nsp.nspname = current_schema()"
                ),
                {"n": name},
            )
            .mappings()
            .first()
        )
        if row is None:
            absent.append(f"index {name}")
            continue
        if row["table_name"] != table:
            mismatched.append(f"index {name}: on table {row['table_name']!r}, expected {table!r}")
        if row["is_unique"] != unique:
            mismatched.append(
                f"index {name}: {'UNIQUE' if row['is_unique'] else 'NON-UNIQUE'}, expected "
                f"{'UNIQUE' if unique else 'NON-UNIQUE'}"
            )
        _, _, tail = row["indexdef"].partition(" USING ")
        if tail != definition_tail:
            mismatched.append(
                f"index {name}: definition is USING {tail!r}, expected USING {definition_tail!r}"
            )

    if absent or mismatched:
        parts = []
        if absent:
            parts.append("ABSENT after the additive DDL ran: " + ", ".join(absent))
        if mismatched:
            parts.append(
                "WRONG DEFINITION (an object of the right NAME is not the object "
                "src/db.py builds — see ADDITIVE_MIGRATIONS): " + "; ".join(mismatched)
            )
        raise RuntimeError("0012 postcondition FAILED — " + " | ".join(parts))


def upgrade() -> None:
    conn = op.get_bind()
    for statement in STATEMENTS:
        # One SAVEPOINT per statement. `alembic/env.py:75-76` runs the whole
        # migration in a single transaction, so without this a benign DDL race
        # (see the module docstring) does not merely skip one statement — it
        # poisons the transaction and rolls the entire upgrade back to 0011.
        try:
            with conn.begin_nested():
                conn.execute(sa.text(statement))
        except sa_exc.DBAPIError as exc:
            if not _is_race_class_error(exc):
                raise
            # The other session created it; the SAVEPOINT rollback discarded our
            # half-built copy and left the outer transaction usable. _verify()
            # below is what proves the object really is there afterwards.
            print(
                "0012: lost a benign DDL race, continuing (the concurrent "
                f"creator wins; postcondition still enforced): {statement[:120]}",
                flush=True,
            )
    _verify(conn)
    # `print(..., flush=True)`, never `logger`: alembic's logging config
    # swallows a logger call from inside a migration, and the `migrate` init
    # container's stdout is the one channel an operator actually reads. Same
    # rationale as 0011's guards.
    print(
        "0012: additive DDL applied + verified (definitions, not just names) — "
        "billing_cost_daily.total_cost_usd@20,6, billing_cost_daily.owner_provided, "
        "provider_pricing.rate_class, tenant_access_charge, "
        "ix_accesscharge_lookup, ix_accesscharge_open",
        flush=True,
    )


def downgrade() -> None:
    """DELIBERATE NO-OP. Dropping these objects would break a RUNNING pod.

    This is not laziness; a real reversal is unsafe and would not hold anyway:

    1. ``src/db.py``'s ``ADDITIVE_MIGRATIONS`` recreates all six objects on
       EVERY pod start. A ``downgrade`` that dropped them would be undone by the
       next replica to boot — and, until then, would leave the running replicas
       writing against a schema that no longer matches their models.
    2. ``owner_provided`` is a member of the live ``uq_cost_daily`` constraint
       (``src/db.py:193-196``) and of ``0011``'s ``MERGE_TABLES`` key for
       ``billing_cost_daily``. ``DROP COLUMN`` takes the unique constraint with
       it, so "reversing" this migration would silently remove a uniqueness
       guarantee the cost rollup depends on.
    3. ``rate_class`` is the leading column of both live ``provider_pricing``
       indexes (``src/db.py:208-214``); dropping it drops both.
    4. ``tenant_access_charge`` holds CONFIGURATION — per-tenant access charges
       provisioned from offerings — not a derivable rollup. ``DROP TABLE`` is
       unrecoverable revenue configuration loss, and ``0011`` explicitly
       classes this table as one where losing a row is an operator decision,
       never an automatic one.
    5. Narrowing ``total_cost_usd`` back to ``numeric(12,6)`` is the one
       statement here that is NOT reversible even in principle: any row whose
       daily aggregate exceeded $999,999.999999 while the column was wide would
       make the ``ALTER`` fail outright — and if it did not fail, it would mean
       silently re-imposing the overflow that closes the day's rollup.

    Going back to ``0011`` therefore means "stop asserting these objects", not
    "remove them". Re-running ``upgrade`` after a ``downgrade`` is already safe
    with nothing to undo.
    """
    print(
        "0012: downgrade is a deliberate no-op — these objects are also created "
        "at runtime by src/db.py's ADDITIVE_MIGRATIONS on every pod start; dropping "
        "them would break a running pod and destroy access-charge configuration. "
        "See the downgrade() docstring.",
        flush=True,
    )
