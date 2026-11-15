#!/usr/bin/env python3
"""Populate ``tenant_id_divergence_audit`` from the 8 tables the R15 estate audit found divergence in.

⛔ THIS SCRIPT RECORDS. IT NEVER BACKFILLS ``tenant_id``. The original ask
("backfill the 1,536 divergent rows to canonical") was refused — see
``alembic/versions/0039_tenant_id_divergence.py`` for the full evidence that
no canonical value exists to backfill TO. This script is read-only against
the 8 source tables and INSERT-only against the ledger.

WHY THIS IS A SCRIPT AND NOT PART OF ``alembic upgrade``
----------------------------------------------------------
Classification (``tenancy.classify_tenant_id``) delegates to
``tenancy.is_registered_tenant_id``, which can reach the LIVE licences
registry over HTTP. A schema migration must stay deterministic and
network-independent, so the actual recording is this deliberate, observable,
operator-run step instead.

Idempotent: the ledger's ``(table_name, row_pk)`` unique constraint means a
second run records only rows new since the last run (a grown table, or a
newly-diverged row) — see ``services.tenant_id_divergence.
record_tenant_id_divergence_ledger``.

Usage
-----
    DATABASE_URL=<billing postgres DSN> \\
    LICENSES_URL=<licences base URL, for the live registry consult>  \\
        uv run python scripts/record_tenant_id_divergence.py [--dry-run]

``--dry-run`` (default) reports what WOULD be recorded, per table, and writes
nothing. Pass ``--apply`` to actually insert.

Exit 0 = ran to completion (prints per-table counts); non-zero = a hard failure.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from services.tenant_id_divergence import (  # noqa: E402
    DEFAULT_LEDGER_TABLES,
    record_tenant_id_divergence_ledger,
)
from tenancy import classify_tenant_id  # noqa: E402


async def _preview(session: AsyncSession) -> dict[str, int]:
    """Dry-run: count what WOULD be recorded, without writing anything."""
    would_record: dict[str, int] = {}
    for table_name, _pk_column in DEFAULT_LEDGER_TABLES:
        rows = (
            await session.execute(
                text(
                    f"SELECT tenant_id::text AS tenant_id FROM {table_name} "  # noqa: S608
                    "WHERE tenant_id IS NOT NULL"
                )
            )
        ).all()
        would_record[table_name] = sum(
            1 for r in rows if classify_tenant_id(r.tenant_id) != "registered"
        )
    return would_record


async def _run(*, apply: bool) -> int:
    db_url = os.environ["DATABASE_URL"]
    engine = create_async_engine(db_url)
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with factory() as session:
        if not apply:
            counts = await _preview(session)
            print("DRY RUN — nothing written. Would record, by table:")
            for table_name, count in counts.items():
                print(f"  {table_name}: {count}")
            print(f"  TOTAL: {sum(counts.values())}")
            return 0

        inserted = await record_tenant_id_divergence_ledger(session)
        print("Recorded, by table (new rows only — idempotent on already-known rows):")
        for table_name, count in inserted.items():
            print(f"  {table_name}: {count}")
        print(f"  TOTAL: {sum(inserted.values())}")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write to tenant_id_divergence_audit. Default is dry-run.",
    )
    args = parser.parse_args()
    return asyncio.run(_run(apply=args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
