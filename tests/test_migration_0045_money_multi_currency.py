"""alembic 0045 against the REAL chain (not ``Base.metadata.create_all``).

* head is single and is 0045;
* a USD and a CAD budget are accepted, GBP is refused by the database;
* fx_rate refuses USD->USD and any non-USD base;
* the adjustment table exists and carries the truncate guard;
* the downgrade refuses while a non-EUR budget exists (the 0044 schema could
  not hold it), and succeeds once none does.
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
from sqlalchemy.exc import DBAPIError

from tests.conftest import throwaway_db_name

REPO_ROOT = Path(__file__).resolve().parent.parent
_DB = throwaway_db_name("_billing_0045_check")


def _cfg(url: sa.engine.URL | None = None) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    if url is not None:
        cfg.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
    return cfg


def test_single_head_descends_from_0045() -> None:
    script = ScriptDirectory.from_config(_cfg())
    heads = script.get_heads()
    assert len(heads) == 1
    chain = {rev.revision for rev in script.walk_revisions(base="base", head=heads[0])}
    assert "0045_money_multi_currency" in chain


def _budget_sql(currency: str) -> sa.TextClause:
    return sa.text(
        "INSERT INTO tenant_money_budget (id, tenant_id, currency, amount) "
        f"VALUES ('{uuid.uuid4()}', '{uuid.uuid4()}', '{currency}', 10)"
    )


@pytest.mark.integration
async def test_0045_constraints_guard_and_downgrade() -> None:
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
    try:
        await asyncio.to_thread(command.upgrade, _cfg(url), "head")

        with eng.begin() as conn:
            conn.execute(_budget_sql("USD"))
            conn.execute(_budget_sql("CAD"))
            conn.execute(
                sa.text(
                    "INSERT INTO fx_rate (base, quote, rate, effective_from) "
                    "VALUES ('USD', 'CAD', 1.37, now())"
                )
            )
        for bad in (
            _budget_sql("GBP"),
            sa.text(
                "INSERT INTO fx_rate (base, quote, rate, effective_from) "
                "VALUES ('USD', 'USD', 1, now())"
            ),
            sa.text(
                "INSERT INTO fx_rate (base, quote, rate, effective_from) "
                "VALUES ('EUR', 'CAD', 1.5, now())"
            ),
        ):
            with pytest.raises(DBAPIError), eng.begin() as conn:
                conn.execute(bad)

        with eng.connect() as conn:
            guarded = conn.execute(
                sa.text(
                    "SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                    "WHERE c.relname = 'tenant_money_budget_adjustment' "
                    "AND t.tgname = 'billing_truncate_guard_trg'"
                )
            ).first()
        assert guarded is not None

        with pytest.raises(DBAPIError):
            await asyncio.to_thread(command.downgrade, _cfg(url), "0044_money_budgets")

        with eng.begin() as conn:
            conn.execute(sa.text("DELETE FROM tenant_money_budget WHERE currency <> 'EUR'"))
            conn.execute(sa.text("DELETE FROM fx_rate WHERE quote <> 'EUR'"))
        await asyncio.to_thread(command.downgrade, _cfg(url), "0044_money_budgets")
        with eng.connect() as conn:
            assert (
                conn.execute(
                    sa.text("SELECT to_regclass('tenant_money_budget_adjustment')")
                ).scalar()
                is None
            )
    finally:
        eng.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        admin.dispose()
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
