"""alembic 0048 against the REAL chain: re-key ``book-fulfillment``.

* head is single and descends from 0048;
* rows seeded on the OLD id at 0047 are on the NEW id after ``upgrade head`` —
  ``token_budgets`` (incl. a ``tenant_name`` that held the old uuid as text),
  ``request_quota_config``, ``tenant_config``;
* a real ``tenant_name`` and a bystander tenant are untouched;
* ``audit_log`` history stays on the OLD id, byte for byte;
* a second run moves nothing;
* a unique slot held on BOTH ids is refused and nothing is written;
* the downgrade applies the reverse mapping.

MUTATION THAT TURNS THIS RED: delete the ``token_budgets.tenant_name`` entry
from ``_UPDATES`` in the migration (``names`` assertion fails), or any
``*.tenant_id`` entry (its count assertion fails); add ``audit_log`` to
``_UPDATES`` and the upgrade itself raises on the tamper guard / the audit
snapshot assertion fails.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.orm import Session

from models.db.budgets import TokenBudget
from models.db.config import TenantConfig
from tests.conftest import throwaway_db_name

REPO_ROOT = Path(__file__).resolve().parent.parent
_DB = throwaway_db_name("_billing_0048_check")

# Literals, NOT imported from the migration: importing them would make this
# test agree with any typo in either constant.
OLD = "1d0090ac-adbf-4c07-b704-bc66061b572e"
NEW = "01a1109c-f895-7766-ba28-6ea56e56a5f7"
BYSTANDER = "0195f48b-d000-7000-8000-000000000099"
REVISION = "0048_rekey_book_fulfillment"
PREVIOUS = "0047_dlq_reattribute_archive"


def _cfg(url: sa.engine.URL | None = None) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    if url is not None:
        cfg.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
    return cfg


def test_single_head_descends_from_0048() -> None:
    script = ScriptDirectory.from_config(_cfg())
    heads = script.get_heads()
    assert len(heads) == 1
    chain = {rev.revision for rev in script.walk_revisions(base="base", head=heads[0])}
    assert REVISION in chain
    rev = script.get_revision(REVISION)
    assert rev is not None
    assert rev.down_revision == PREVIOUS
    assert len(REVISION) <= 32
    assert (rev.module.OLD_TENANT_ID, rev.module.NEW_TENANT_ID) == (OLD, NEW)


def _count(conn: sa.Connection, table: str, tenant_id: str) -> int:
    return conn.execute(
        sa.text(f"SELECT count(*) FROM {table} WHERE tenant_id = CAST(:t AS uuid)"),
        {"t": tenant_id},
    ).scalar_one()


def _audit_snapshot(conn: sa.Connection) -> list[tuple]:
    return [
        tuple(r)
        for r in conn.execute(
            sa.text(
                "SELECT id, ts, action, resource_type, tenant_id::text, tenant_name "
                "FROM audit_log ORDER BY id"
            )
        )
    ]


def _seed(eng: sa.Engine) -> None:
    with Session(eng) as s, s.begin():
        # The two prod shapes: tenant_name holding the uuid AS TEXT, and a real name.
        s.add(TokenBudget(tenant_id=OLD, tenant_name=OLD, budget_tokens=1000))
        s.add(TokenBudget(tenant_id=OLD, tenant_name="book-fulfillment", budget_tokens=2000))
        s.add(TokenBudget(tenant_id=BYSTANDER, tenant_name="fr-test-user", budget_tokens=3000))
        # Explicit SQL, NOT the ORM model: this seeds a 0047-shaped table, and
        # the head model names columns added later (0049 app_key) that do not
        # exist yet at 0047.
        for tenant in (OLD, OLD, BYSTANDER):
            s.execute(
                sa.text(
                    "INSERT INTO request_quota_config "
                    "(license_id, tenant_id, quota_monthly, created_at, updated_at) "
                    "VALUES (CAST(:l AS uuid), CAST(:t AS uuid), 10, now(), now())"
                ),
                {"l": str(uuid.uuid4()), "t": tenant},
            )
        s.add(TenantConfig(tenant_id=OLD, tenant_name=OLD))
        for _ in range(4):
            s.execute(
                sa.text(
                    "INSERT INTO audit_log (action, resource_type, tenant_id, tenant_name) "
                    "VALUES ('budget.create', 'token_budget', CAST(:t AS uuid), :t)"
                ),
                {"t": OLD},
            )


@pytest.mark.integration
async def test_0048_rekeys_live_rows_and_leaves_history() -> None:
    dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    assert dsn
    server = sa.make_url(dsn).set(database="postgres")
    admin = sa.create_engine(server, isolation_level="AUTOCOMMIT", future=True)
    with admin.connect() as conn:
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        conn.execute(sa.text(f'CREATE DATABASE "{_DB}"'))
    url = server.set(database=_DB)
    saved = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url.render_as_string(hide_password=False)
    eng = sa.create_engine(url, future=True)
    migration = ScriptDirectory.from_config(_cfg()).get_revision(REVISION)
    assert migration is not None
    rekey = migration.module.rekey
    try:
        await asyncio.to_thread(command.upgrade, _cfg(url), PREVIOUS)
        _seed(eng)
        with eng.connect() as conn:
            audit_before = _audit_snapshot(conn)
        assert len(audit_before) == 4

        # Fresh-database no-op is the upgrade above reaching 0047 on empty
        # tables; this is the upgrade WITH rows.
        await asyncio.to_thread(command.upgrade, _cfg(url), "head")

        with eng.connect() as conn:
            for table, moved, bystander in (
                ("token_budgets", 2, 1),
                ("request_quota_config", 2, 1),
                ("tenant_config", 1, 0),
            ):
                assert _count(conn, table, OLD) == 0, table
                assert _count(conn, table, NEW) == moved, table
                assert _count(conn, table, BYSTANDER) == bystander, table
            names = sorted(
                conn.execute(
                    sa.text(
                        "SELECT tenant_name FROM token_budgets WHERE tenant_id = CAST(:t AS uuid)"
                    ),
                    {"t": NEW},
                ).scalars()
            )
            assert names == sorted([NEW, "book-fulfillment"])
            assert (
                conn.execute(sa.text("SELECT tenant_name FROM tenant_config")).scalar_one() == NEW
            )
            # History: same rows, same id, still on the OLD tenant.
            assert _audit_snapshot(conn) == audit_before
            assert _count(conn, "audit_log", OLD) == 4
            assert _count(conn, "audit_log", NEW) == 0

        # Re-run: nothing left to move.
        with eng.begin() as conn:
            assert set(rekey(conn, old=OLD, new=NEW).values()) == {0}

        # Collision: tenant_config is unique on tenant_id and NEW now holds the
        # slot. A fresh row on OLD must be REFUSED, and nothing may be written —
        # including the token_budgets row seeded alongside it.
        with Session(eng) as s, s.begin():
            s.add(TenantConfig(tenant_id=OLD, tenant_name="late"))
            s.add(TokenBudget(tenant_id=OLD, tenant_name=OLD, budget_tokens=1))
        with pytest.raises(RuntimeError, match="tenant_config"), eng.begin() as conn:
            rekey(conn, old=OLD, new=NEW)
        with eng.connect() as conn:
            assert _count(conn, "tenant_config", OLD) == 1
            assert _count(conn, "token_budgets", OLD) == 1
            assert _count(conn, "token_budgets", NEW) == 2
        with eng.begin() as conn:
            conn.execute(sa.text("SET LOCAL billing.delete_reason = 'test 0048 cleanup'"))
            conn.execute(
                sa.text("DELETE FROM tenant_config WHERE tenant_name = 'late'"),
            )
            conn.execute(
                sa.text("DELETE FROM token_budgets WHERE tenant_id = CAST(:t AS uuid)"),
                {"t": OLD},
            )

        # Downgrade = reverse mapping.
        await asyncio.to_thread(command.downgrade, _cfg(url), PREVIOUS)
        with eng.connect() as conn:
            assert _count(conn, "token_budgets", OLD) == 2
            assert _count(conn, "token_budgets", NEW) == 0
            assert _count(conn, "request_quota_config", OLD) == 2
            assert _audit_snapshot(conn) == audit_before
    finally:
        eng.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        admin.dispose()
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
