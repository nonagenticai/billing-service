"""alembic 0056 against the REAL chain: ``tenant_config.tenant_name`` has a default.

The prod failure (2026-10-13) was a ``NotNullViolation`` on the FIRST
``tenant_config`` row for a brand-new tenant: the ORM model says
``server_default=""`` and so omits the column, but the chain-built column had no
default. The rest of the suite cannot see that — ``conftest`` builds its schema
with ``Base.metadata.create_all``, i.e. from the model, which HAS the default.
So these tests build the schema from ``alembic upgrade`` instead.

* single head, descends from 0055, revision id fits ``varchar(32)``;
* at head, inserting ``TenantConfig`` exactly the way the commercial-terms PUT
  did (no ``tenant_name``) succeeds and stores ``''``;
* at 0055 the SAME insert raises ``NotNullViolation`` — the test discriminates;
* RATCHET: every NOT NULL model column declaring a ``server_default`` has a DB
  default at head, except an explicitly listed set of known drifts. A NEW drift
  fails here, at the class level, instead of in prod one handler at a time.

MUTATION THAT TURNS THIS RED: delete ``op.execute(SET_DEFAULT)`` from
``upgrade()`` — the head insert raises and the ratchet reports ``tenant_config``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from db import Base
from models.db import TenantConfig
from tests.conftest import throwaway_db_name

REPO_ROOT = Path(__file__).resolve().parent.parent
_DB = throwaway_db_name("_billing_0056_check")
REVISION = "0056_tenant_config_name_default"
PREVIOUS = "0055_money_budget_topup"

#: NOT NULL columns whose MODEL declares a ``server_default`` the chain-built
#: column does not have, measured at 0056 head. Every writer of these tables
#: names ``tenant_name`` explicitly today, so they are latent, not live — but
#: each is the same trap 0056 closed for ``tenant_config``. Shrink this set by
#: migrating them; never grow it.
KNOWN_DEFAULT_DRIFT: frozenset[tuple[str, str]] = frozenset(
    {
        ("billing_cost_daily", "tenant_name"),
        ("billing_metering_daily", "tenant_name"),
        ("billing_metering_events", "tenant_name"),
        ("billing_metering_hourly", "tenant_name"),
        ("token_usage", "tenant_name"),
    }
)


def _cfg(url: sa.engine.URL | None = None) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    if url is not None:
        cfg.set_main_option(
            "sqlalchemy.url", url.render_as_string(hide_password=False)
        )  # pragma: allowlist secret
    return cfg


def test_single_head_descends_from_0056() -> None:
    script = ScriptDirectory.from_config(_cfg())
    heads = script.get_heads()
    assert len(heads) == 1
    chain = {rev.revision for rev in script.walk_revisions(base="base", head=heads[0])}
    assert REVISION in chain
    rev = script.get_revision(REVISION)
    assert rev is not None
    assert rev.down_revision == PREVIOUS
    assert len(REVISION) <= 32


def _insert_like_the_handler(eng: sa.engine.Engine) -> str:
    """Create a ``tenant_config`` row the way ``put_commercial_terms`` did before
    the fix: every column but ``tenant_name``. Returns the tenant id."""
    tid = str(uuid.uuid4())
    with Session(eng) as session:
        session.add(
            TenantConfig(
                tenant_id=tid,
                markup_pct=Decimal("20"),
                billing_mode="standard_markup",
                notes="0056 check",
            )
        )
        session.commit()
    return tid


@pytest.mark.integration
async def test_0056_default_lets_an_unnamed_tenant_config_row_insert() -> None:
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
        # --- at 0055: the prod failure, reproduced ---------------------------
        await asyncio.to_thread(command.upgrade, _cfg(url), PREVIOUS)
        with pytest.raises(IntegrityError) as exc:
            _insert_like_the_handler(eng)
        assert "tenant_name" in str(exc.value), str(exc.value)
        assert "NotNullViolation" in type(exc.value.orig).__name__, repr(exc.value.orig)

        # --- at head: the same insert succeeds and stores '' -----------------
        await asyncio.to_thread(command.upgrade, _cfg(url), "head")
        tid = _insert_like_the_handler(eng)
        with eng.connect() as conn:
            stored = conn.execute(
                sa.text("SELECT tenant_name FROM tenant_config WHERE tenant_id = CAST(:t AS uuid)"),
                {"t": tid},
            ).scalar_one()
        assert stored == ""

        # --- ratchet: model server_default vs chain default -------------------
        with eng.connect() as conn:
            db_defaults = {
                (t, c): d
                for t, c, d in conn.execute(
                    sa.text(
                        "SELECT table_name, column_name, column_default "
                        "FROM information_schema.columns WHERE table_schema = 'public'"
                    )
                )
            }
        drift = {
            (table.name, col.name)
            for table in Base.metadata.sorted_tables
            for col in table.columns
            if col.server_default is not None
            and not col.nullable
            and (table.name, col.name) in db_defaults
            and db_defaults[(table.name, col.name)] is None
        }
        assert ("tenant_config", "tenant_name") not in drift
        assert drift == KNOWN_DEFAULT_DRIFT, (
            "model server_default vs alembic chain drift changed. NEW (fix with a "
            f"migration): {sorted(drift - KNOWN_DEFAULT_DRIFT)}; FIXED (remove from "
            f"KNOWN_DEFAULT_DRIFT): {sorted(KNOWN_DEFAULT_DRIFT - drift)}"
        )

        # --- downgrade removes the default again -----------------------------
        await asyncio.to_thread(command.downgrade, _cfg(url), PREVIOUS)
        with eng.connect() as conn:
            assert (
                conn.execute(
                    sa.text(
                        "SELECT column_default FROM information_schema.columns WHERE "
                        "table_name = 'tenant_config' AND column_name = 'tenant_name'"
                    )
                ).scalar_one()
                is None
            )
    finally:
        eng.dispose()
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        admin.dispose()
