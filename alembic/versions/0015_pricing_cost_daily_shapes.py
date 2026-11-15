"""Bring the last three runtime-DDL-only object shapes into the alembic chain.

WHAT IS BROKEN
--------------
This service builds part of its schema twice: ``alembic/versions/*`` is one
source of truth and the ``ADDITIVE_MIGRATIONS`` tuple in ``src/db.py``, replayed
on every pod start, is the other. 0012 closed most of that gap. Three objects
were deliberately left out of its scope because they are REPLACEMENTS, not
additions -- they need a DROP, which is a different risk class from an additive
``IF NOT EXISTS``.

Measured 2026-09-01 by building a fresh database with ``alembic upgrade head``
and diffing it against the production ``billing`` DB on worker4. Every COLUMN
matches -- 0 differences either way. Exactly three object SHAPES differ:

    object                     alembic chain              production
    uq_cost_daily              without ``owner_provided`` with it
    ix_providerpricing_lookup  (provider, model, ...)     (rate_class, ...)
    ix_providerpricing_open    (provider, coalesce...)    (rate_class, ...)

``ix_providerpricing_open`` is the one that matters semantically: with
``rate_class`` leading, open-row uniqueness is per (rate_class, provider, model,
service_key), so a ``'list'`` row may coexist with the ``'cost'`` row for the
same tuple. Without it, inserting the ``'list'`` row conflicts.

WHY THIS IS SAFE ON A LIVE DATABASE
-----------------------------------
It does not blindly drop anything. Each object is compared against its target
shape first and SKIPPED when it already matches, so on production -- where all
three are already correct, applied by the runtime replay -- this migration is a
no-op that takes no lock at all. It only does work on a database built from the
chain alone (CI, a fresh environment, ``validate_migrations``).

The comparison is against Postgres' own normalised ``pg_get_indexdef`` /
``pg_get_constraintdef`` output, so both sides are formatted by the same code
and a textual match is meaningful. After any replacement the new definition is
re-read and asserted, so a silent mis-shape fails the migration rather than
reaching a pod.

Revises: 0014_quota_config_app
"""

import sqlalchemy as sa
from alembic import op

revision = "0015_pricing_cost_daily_shapes"
down_revision = "0014_quota_config_app"
branch_labels = None
depends_on = None

# Target shapes, verbatim as Postgres renders them (captured from production).
_TARGET_INDEXES: dict[str, str] = {
    "ix_providerpricing_lookup": (
        "CREATE INDEX ix_providerpricing_lookup ON public.provider_pricing "
        "USING btree (rate_class, provider, model, effective_from DESC)"
    ),
    "ix_providerpricing_open": (
        "CREATE UNIQUE INDEX ix_providerpricing_open ON public.provider_pricing "
        "USING btree (rate_class, provider, COALESCE(model, ''::character varying), "
        "COALESCE(service_key, ''::character varying)) WHERE (effective_until IS NULL)"
    ),
}

_COST_DAILY_COLUMNS = (
    "tenant_id, license_id, subject_id, service_key, provider, "
    "served_model, byo, owner_provided, cost_source, bucket_date"
)
_TARGET_CONSTRAINT = f"UNIQUE ({_COST_DAILY_COLUMNS})"


def _indexdef(conn: sa.Connection, name: str) -> str | None:
    return conn.execute(
        sa.text("SELECT pg_get_indexdef(c.oid) FROM pg_class c WHERE c.relname = :n"),
        {"n": name},
    ).scalar()


def _constraintdef(conn: sa.Connection, name: str) -> str | None:
    return conn.execute(
        sa.text("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = :n"),
        {"n": name},
    ).scalar()


def upgrade() -> None:
    conn = op.get_bind()

    for name, target in _TARGET_INDEXES.items():
        current = _indexdef(conn, name)
        if current == target:
            print(f"0015: {name} already correct -- skipped, no lock taken")
            continue
        print(f"0015: {name} REPLACED\n      was: {current}\n      now: {target}")
        op.execute(f"DROP INDEX IF EXISTS {name}")
        op.execute(target)
        after = _indexdef(conn, name)
        if after != target:
            raise RuntimeError(f"0015: {name} is {after!r} after replacement, expected {target!r}")

    current = _constraintdef(conn, "uq_cost_daily")
    if current == _TARGET_CONSTRAINT:
        print("0015: uq_cost_daily already correct -- skipped, no lock taken")
        return
    print(f"0015: uq_cost_daily REPLACED\n      was: {current}\n      now: {_TARGET_CONSTRAINT}")
    op.execute("ALTER TABLE billing_cost_daily DROP CONSTRAINT IF EXISTS uq_cost_daily")
    op.execute(
        f"ALTER TABLE billing_cost_daily ADD CONSTRAINT uq_cost_daily UNIQUE ({_COST_DAILY_COLUMNS})"
    )
    after = _constraintdef(conn, "uq_cost_daily")
    if after != _TARGET_CONSTRAINT:
        raise RuntimeError(f"0015: uq_cost_daily is {after!r}, expected {_TARGET_CONSTRAINT!r}")


def downgrade() -> None:
    """Deliberately not implemented.

    Reverting would restore shapes that production has not had for months and
    that ``ix_providerpricing_open`` in particular would make un-insertable for
    any existing ``rate_class='list'`` row.
    """
    raise NotImplementedError("0015 is forward-only; see the docstring")
