"""Assert the alembic chain and the ORM models describe the SAME schema.

WHAT THIS REPLACED, AND WHY
---------------------------
The previous version ran: ``Base.metadata.create_all()`` -> ``alembic stamp
0001_billing_baseline`` -> ``alembic upgrade head``. That premise is incoherent,
and it is why the script could not pass on ``main``. ``create_all`` builds the
CURRENT model schema; stamping 0001 then claims that schema IS revision 0001.
It is not -- every migration from 0002 on exists precisely because it is not.

The failures were not incidental. 0003 tried ``ALTER TABLE cost_events_2026_04
RENAME TO billing_cost_events_2026_04`` against a database where the partitions
already had the new names (``UndefinedTable``); guarding that just moved the
failure to 0007, which does ``min(tenant_id)`` on a column that is ``varchar``
at that point in the chain but ``uuid`` in the current model (``UndefinedFunction:
function min(uuid) does not exist``). No amount of "already done" guarding fixes
a scenario that replays historical DDL against a present-day schema.

WHAT IT CHECKS NOW
------------------
The invariant that is actually worth holding, and that nothing else holds:

    alembic upgrade head   ==   Base.metadata.create_all()

Two throwaway databases, one built each way, then a diff of columns,
constraints and indexes. Divergence between those two is exactly the class of
bug that produced ``0015_pricing_cost_daily_shapes`` -- three objects whose
shape existed only in ``src/db.py``'s runtime replay, so a fresh
alembic-built database was silently different from production.

Partition children are excluded: migrations create ``billing_cost_events_YYYY_MM``
by SQL and ``create_all`` does not, which is a real and intended difference.

Usage (point DATABASE_URL at a THROWAWAY server -- this creates and drops
databases):

    DATABASE_URL=postgresql+psycopg://postgres:postgres@127.0.0.1:5507/postgres \
        uv run python scripts/validate_migrations.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
os.chdir(REPO)

import sqlalchemy as sa  # noqa: E402

import models.db  # noqa: E402, F401
from db import Base  # noqa: E402

CHAIN_DB = "_vm_chain"
MODELS_DB = "_vm_models"

# The previous version defaulted DATABASE_URL to a hard-coded DSN when unset,
# and once did destructive work in another session's container as a result. The
# guard against that is now structural: DATABASE_URL is REQUIRED (see main()),
# and the two databases this creates are named distinctly so they cannot collide
# with a repo test database that happens to live on the same server.

_TABLES = """
SELECT tablename FROM pg_tables
WHERE schemaname = 'public' AND tablename NOT LIKE 'billing\\_cost\\_events\\_%'
  AND tablename <> 'alembic_version'
ORDER BY 1
"""
_COLUMNS = """
SELECT table_name, column_name || ' ' || data_type || ' null=' || is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name NOT LIKE 'billing\\_cost\\_events\\_%'
  AND table_name <> 'alembic_version'
"""
# Keyed on (table, DEFINITION) with the object's NAME deliberately dropped.
# 0003 renamed the tables but not their indexes/constraints, so the chain calls
# billing_cost_daily's primary key `cost_daily_pkey` while create_all calls it
# `billing_cost_daily_pkey`. That is cosmetic; comparing names would drown the
# real signal -- a differing SHAPE -- in ~38 rows of rename noise.
_CONSTRAINTS = """
SELECT r.relname, pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class r ON r.oid = c.conrelid
JOIN pg_namespace n ON n.oid = r.relnamespace
WHERE n.nspname = 'public' AND r.relname NOT LIKE 'billing\\_cost\\_events\\_%'
  AND r.relname <> 'alembic_version' AND c.contype IN ('p', 'u', 'f', 'c')
"""
_INDEXES = """
SELECT tablename,
       regexp_replace(indexdef, '^CREATE (UNIQUE )?INDEX [a-z0-9_]+ ON ', '\\1ON ')
FROM pg_indexes
WHERE schemaname = 'public' AND tablename NOT LIKE 'billing\\_cost\\_events\\_%'
  AND tablename <> 'alembic_version'
"""


def _server_url(dsn: str) -> sa.engine.URL:
    return sa.make_url(dsn).set(database="postgres")


def _recreate(server: sa.engine.URL, name: str) -> sa.engine.URL:
    eng = sa.create_engine(server, isolation_level="AUTOCOMMIT", future=True)
    with eng.connect() as conn:
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    eng.dispose()
    return server.set(database=name)


def _snapshot(url: sa.engine.URL) -> dict[str, object]:
    eng = sa.create_engine(url, future=True)
    with eng.connect() as conn:
        tables = {r[0] for r in conn.execute(sa.text(_TABLES))}
        snap: dict[str, object] = {"tables": tables}
        for kind, q in (
            ("columns", _COLUMNS),
            ("constraints", _CONSTRAINTS),
            ("indexes", _INDEXES),
        ):
            snap[kind] = {(r[0], r[1]) for r in conn.execute(sa.text(q))}
    eng.dispose()
    return snap


def main() -> int:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required; see this file's docstring.")
    server = _server_url(dsn)

    chain_url = _recreate(server, CHAIN_DB)
    models_url = _recreate(server, MODELS_DB)

    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", chain_url.render_as_string(hide_password=False))
    os.environ["DATABASE_URL"] = chain_url.render_as_string(hide_password=False)
    command.upgrade(cfg, "head")
    print(f"chain:  alembic upgrade head -> {CHAIN_DB}")

    eng = sa.create_engine(models_url, future=True)
    Base.metadata.create_all(eng)
    eng.dispose()
    print(f"models: Base.metadata.create_all -> {MODELS_DB}")

    chain, models = _snapshot(chain_url), _snapshot(models_url)

    # A table the chain creates with no ORM model (bookkeeping like
    # `tenant_id_crosswalk`) is legitimate and is reported, not failed.
    only_chain_t = sorted(chain["tables"] - models["tables"])
    only_models_t = sorted(models["tables"] - chain["tables"])
    if only_chain_t:
        print(f"INFO migration-only tables (no ORM model): {only_chain_t}")
    if only_models_t:
        print(f"FAIL model-only tables (no migration creates them): {only_models_t}")
    # `billing_cost_events` is RANGE-partitioned by the chain and cannot be by
    # create_all, so its structures differ for reasons that are correct: the
    # partition key forces `PRIMARY KEY (id, created_at)`, and every index on a
    # partitioned parent renders as `ON ONLY`. Comparing it would fail forever.
    shared = (chain["tables"] & models["tables"]) - {"billing_cost_events"}

    failures = 1 if only_models_t else 0
    for kind in ("columns", "constraints", "indexes"):
        c = {row for row in chain[kind] if row[0] in shared}
        m = {row for row in models[kind] if row[0] in shared}
        only_c, only_m = sorted(c - m), sorted(m - c)
        # ASYMMETRIC ON PURPOSE. `models-only` means the chain does NOT build
        # something the running code declares -- a database built by
        # `alembic upgrade head` is missing it. That is the failure this script
        # exists for, and it is the shape of the 0015 drift: the ORM declared
        # ix_providerpricing_open with rate_class leading while the chain built
        # it without.
        # `chain-only` means a migration creates something the ORM does not
        # declare (a partial index, a bookkeeping structure). Worth surfacing --
        # a create_all-built test database will not have it -- but not a defect.
        if only_m:
            failures += 1
            print(f"FAIL {kind}: {len(only_m)} declared by the models, NOT built by the chain")
            for t, d in only_m:
                print(f"       models-only: {t} | {d}")
        if only_c:
            print(f"INFO {kind}: {len(only_c)} built by the chain, not declared by the models")
            for t, d in only_c:
                print(f"       chain-only : {t} | {d}")
        if not only_c and not only_m:
            print(f"OK   {kind}: {len(c)} identical across {len(shared)} shared tables")

    if failures:
        print(
            "\nFAIL: the migration chain and the ORM models disagree. A database built "
            "by `alembic upgrade head` is not the schema this service's code expects.",
            file=sys.stderr,
        )
        return 1
    print("\nPASS: chain and models agree on columns, constraints and indexes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
