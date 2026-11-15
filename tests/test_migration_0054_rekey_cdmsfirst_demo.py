"""alembic 0054 against the REAL chain: re-key the CDMSFIRST demo tenant, audit included.

* head is single, descends from 0054, and 0054 sits on 0053;
* with only a bystander tenant present the re-key moves nothing;
* rows seeded on the OLD id at 0053 are on the NEW id after ``upgrade head`` in
  every table 0051 moves AND in ``audit_log`` (``tenant_id``, ``resource_id``,
  ``after.tenant_id`` / ``after.tenant_name``) and
  ``billing_metering_events.path`` (``/budget/<id>``), plus
  ``billing_cost_usage_daily.tenant_id`` (0053) and ``billing_metering_events.actor_id``
  (0052) when it holds the synthetic OLD id;
* the same uuid in a USER sense (``subject_id``, ``actor_id``, ``actor_sub``) and
  every other ``after`` key is untouched; the bystander's audit row is untouched;
* ``billing_invoices`` is untouched (left on OLD, logged);
* NEGATIVE CONTROL: after the upgrade ``billing_audit_log_immutability_trg`` is
  ENABLED again — an UPDATE that changes an audit row is still refused — and it
  was refusing before the upgrade too (so the migration, not a missing trigger,
  is what let the rewrite through);
* a second run moves nothing; a ROLLUP slot held on both ids is refused and
  nothing is written;
* the downgrade applies the reverse mapping, audit rows and path included.

Seeds use Core inserts naming only the columns they set (as #262/#263 did for
0051's test): an ORM add names every mapped column, so a column added by a LATER
migration would break the seed at PREVIOUS.

MUTATIONS THAT TURN THIS RED: drop the ``ENABLE TRIGGER`` statement and the
revision itself raises (tgenabled check) — and with that check also removed the
negative control's ``pytest.raises`` fails. Drop ``_AUDIT_UPDATES`` and the audit
assertions fail on ``audit_log.tenant_id``. Drop ``_PATH_UPDATE`` and the
``_REMAINING`` assertion aborts the upgrade with ``billing_metering_events.path``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from models.db import NULL_LICENSE_ID, NULL_SUBJECT_ID
from models.db.budgets import TokenBudget, TokenUsage
from models.db.config import MarkupRule
from models.db.events import CostEvent, MeteringEvent
from models.db.quota import RequestQuotaConfig
from models.db.rollups import CostDaily, CostUsageDaily
from tests.conftest import throwaway_db_name


def _table(model: Any) -> sa.Table:
    """An ORM model's ``__table__`` as the ``Table`` it is at runtime."""
    return cast(sa.Table, model.__table__)


REPO_ROOT = Path(__file__).resolve().parent.parent
_DB = throwaway_db_name("_billing_0054_check")

# Literals, NOT imported from the migration.
OLD = "0195f48b-d000-7000-8000-000000000100"
NEW = "01a1117c-d9b0-72fc-aeda-a9d1187f2a36"
BYSTANDER = "0195f48b-d000-7000-8000-000000000099"
USER = "9897e262-100a-4487-adc3-121b2705568d"
REVISION = "0054_rekey_cdmsfirst_demo"
PREVIOUS = "0053_cost_funding_usage_daily"

_DAY = date(2026, 9, 1)
_NOW = datetime(2026, 9, 1, 10, 30, tzinfo=UTC)

# (table, uuid column, rows seeded on OLD, rows seeded on BYSTANDER).
_TENANT_COLUMNS: tuple[tuple[str, str, int, int], ...] = (
    ("token_budgets", "tenant_id", 1, 1),
    ("request_quota_config", "tenant_id", 1, 1),
    ("markup_rules", "tenant_id", 1, 0),
    ("billing_cost_events", "tenant_id", 1, 1),
    ("billing_metering_events", "tenant_id", 2, 1),
    ("billing_metering_events", "actor_id", 1, 0),
    ("billing_cost_usage_daily", "tenant_id", 1, 1),
    ("billing_cost_daily", "tenant_id", 1, 1),
    ("token_usage", "tenant_id", 1, 1),
    ("audit_log", "tenant_id", 3, 1),
)


def _cfg(url: sa.engine.URL | None = None) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    if url is not None:
        cfg.set_main_option(
            "sqlalchemy.url", url.render_as_string(hide_password=False)
        )  # pragma: allowlist secret
    return cfg


def test_single_head_descends_from_0054() -> None:
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


def _count(conn: sa.Connection, table: str, tenant_id: str, column: str = "tenant_id") -> int:
    return conn.execute(
        sa.text(f"SELECT count(*) FROM {table} WHERE {column} = CAST(:t AS uuid)"),
        {"t": tenant_id},
    ).scalar_one()


def _rows(conn: sa.Connection, sql: str, **params: object) -> list[tuple]:
    return [tuple(r) for r in conn.execute(sa.text(sql), params)]


def _audit(conn: sa.Connection) -> list[tuple]:
    return _rows(
        conn,
        "SELECT action, tenant_id::text, resource_id, actor_sub, after ->> 'tenant_id', "
        "after ->> 'tenant_name', after ->> 'actor', after ->> 'budget_tokens' "
        "FROM audit_log ORDER BY id",
    )


def _paths(conn: sa.Connection) -> list[tuple]:
    return _rows(
        conn, "SELECT tenant_id::text, path FROM billing_metering_events ORDER BY tenant_id, path"
    )


def _invoices(conn: sa.Connection) -> list[tuple]:
    return _rows(conn, "SELECT tenant_id::text, tenant_name, snapshot::text FROM billing_invoices")


def _insert_audit(s: Session, tenant: str, action: str, resource_id: str | None) -> None:
    s.execute(
        sa.text(
            "INSERT INTO audit_log (action, resource_type, resource_id, tenant_id, actor_sub, "
            " after) VALUES (:a, 'budget', :r, CAST(:t AS uuid), CAST(:u AS text), "
            " jsonb_build_object('tenant_id', CAST(:t AS text), 'tenant_name', CAST(:t AS text), "
            "                    'actor', CAST(:u AS text), 'budget_tokens', 1000))"
        ),
        {"a": action, "r": resource_id, "t": tenant, "u": USER},
    )


def _seed_tenant(s: Session, tenant: str, *, full: bool) -> None:
    user = uuid.UUID(USER)
    s.add(TokenBudget(tenant_id=tenant, tenant_name="cdmsfirst", budget_tokens=1000))
    s.add(RequestQuotaConfig(license_id=uuid.uuid4(), tenant_id=tenant, quota_monthly=10))
    s.execute(
        sa.insert(_table(CostEvent)).values(
            tenant_id=tenant,
            tenant_name=tenant,
            service_key="inference-router",  # pragma: allowlist secret
            provider="p",
            subject_id=user,
            actor_id=user,
            created_at=_NOW,
        )
    )
    s.execute(
        sa.insert(_table(MeteringEvent)).values(
            tenant_id=tenant,
            tenant_name=tenant,
            subject_id=user,
            actor_id=user,
            method="GET",
            path="/x",
            service_key="billing",  # pragma: allowlist secret
            status="ok",
            created_at=_NOW,
        )
    )
    s.execute(
        sa.insert(_table(CostDaily)).values(
            tenant_id=tenant,
            tenant_name=tenant,
            license_id=NULL_LICENSE_ID,
            subject_id=NULL_SUBJECT_ID,
            service_key="inference-router",  # pragma: allowlist secret
            provider="p",
            served_model="m",
            byo=False,
            owner_provided=False,
            bucket_date=_DAY,
            total_input_tokens=1,
            total_output_tokens=1,
            total_cost_usd=Decimal("0.5"),
            request_count=1,
        )
    )
    s.add(
        TokenUsage(
            tenant_id=tenant,
            tenant_name=tenant,
            period_type="monthly",
            period_start=_DAY,
            total_tokens=7,
        )
    )
    s.execute(
        sa.insert(_table(CostUsageDaily)).values(
            bucket_date=_DAY,
            tenant_id=tenant,
            actor_id=user,
            funding="platform_external",
            provider="p",
            served_model="m",
            event_count=1,
            total_cost_usd=Decimal("0.5"),
        )
    )
    _insert_audit(s, tenant, "budget.create", None)
    if not full:
        return
    s.add(MarkupRule(tenant_id=tenant, markup_pct=Decimal("10"), effective_from=_NOW))
    # The admin budget endpoint, where the tenant id IS the path parameter.
    # actor_id = the synthetic tenant id (never a Keycloak sub): moves with it.
    s.execute(
        sa.insert(_table(MeteringEvent)).values(
            tenant_id=tenant,
            tenant_name="cdmsfirst",
            subject_id=user,
            actor_id=tenant,
            method="PUT",
            path=f"/api/admin/licenses/budget/{tenant}",
            service_key="billing",  # pragma: allowlist secret
            status="ok",
            created_at=_NOW,
        )
    )
    _insert_audit(s, tenant, "budget.update", None)
    _insert_audit(s, tenant, "quota.config.set", tenant)
    s.execute(
        sa.text(
            "INSERT INTO billing_invoices (id, tenant_id, tenant_name, period_start, period_end, "
            " subtotal_usd, total_usd, internal_cost_usd, margin_usd, billing_mode, markup_pct, "
            " effective_markup_pct, snapshot, inputs, snapshot_sha256, generator_version) "
            "VALUES (gen_random_uuid(), CAST(:t AS uuid), :t, '2026-09-01', '2026-10-01', "
            " 0, 0, 0, 0, 'markup', 0, 0, jsonb_build_object('tenant_id', CAST(:t AS text)), "
            " '{}', 'x', 'test')"
        ),
        {"t": tenant},
    )


def _assert_audit_update_refused(eng: sa.Engine) -> None:
    with pytest.raises(DBAPIError, match="is blocked"), eng.begin() as conn:
        conn.execute(sa.text("UPDATE audit_log SET action = 'tampered'"))


@pytest.mark.integration
async def test_0054_rekeys_config_history_and_audit() -> None:
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

        with Session(eng) as s, s.begin():
            _seed_tenant(s, BYSTANDER, full=False)
        with eng.begin() as conn:
            assert set(rekey(conn, old=OLD, new=NEW).values()) == {0}

        with Session(eng) as s, s.begin():
            _seed_tenant(s, OLD, full=True)
        # Control: the trigger refuses an audit edit BEFORE the migration runs.
        _assert_audit_update_refused(eng)
        with eng.connect() as conn:
            audit_before = _audit(conn)
            invoices_before = _invoices(conn)
        assert len(audit_before) == 4

        await asyncio.to_thread(command.upgrade, _cfg(url), "head")

        with eng.connect() as conn:
            for table, column, moved, bystander in _TENANT_COLUMNS:
                where = f"{table}.{column}"
                assert _count(conn, table, OLD, column) == 0, f"{where}: rows still on OLD"
                assert _count(conn, table, NEW, column) == moved, where
                assert _count(conn, table, BYSTANDER, column) == bystander, where
            # tenant_name: a real name stays, the uuid-as-text moves.
            assert _rows(conn, "SELECT DISTINCT tenant_name FROM token_budgets") == [("cdmsfirst",)]
            assert sorted(
                _rows(conn, "SELECT tenant_name FROM billing_cost_events ORDER BY 1")
            ) == sorted([(NEW,), (BYSTANDER,)])

            def moved_row(row: tuple) -> tuple:
                swap = {OLD: NEW}
                return tuple(swap.get(v, v) if isinstance(v, str) else v for v in row)

            assert _audit(conn) == [moved_row(r) for r in audit_before]
            # USER sense survives in the audit rows (actor_sub and after.actor).
            assert all(r[3] == USER and r[6] == USER for r in _audit(conn))
            assert _paths(conn) == sorted(
                [
                    (BYSTANDER, "/x"),
                    (NEW, "/api/admin/licenses/budget/" + NEW),
                    (NEW, "/x"),
                ]
            )
            # USER-sense actor ids are untouched (cost events and metering events).
            for table in ("billing_cost_events", "billing_metering_events"):
                assert _rows(conn, f"SELECT count(*) FROM {table} WHERE actor_id = :u", u=USER) == [
                    (2,)
                ], table
            # Invoices: never written, still on OLD.
            assert _invoices(conn) == invoices_before
            assert _count(conn, "billing_invoices", OLD) == 1
            assert (
                conn.execute(
                    sa.text(
                        "SELECT t.tgenabled FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                        "WHERE c.relname = 'audit_log' "
                        "AND t.tgname = 'billing_audit_log_immutability_trg'"
                    )
                ).scalar_one()
                == "O"
            )

        # NEGATIVE CONTROL: the immutability trigger is back on.
        _assert_audit_update_refused(eng)

        with eng.begin() as conn:
            assert set(rekey(conn, old=OLD, new=NEW).values()) == {0}

        # ROLLUP collision on token_usage (monthly, _DAY): refused, nothing written.
        with Session(eng) as s, s.begin():
            s.add(
                TokenUsage(tenant_id=OLD, tenant_name=OLD, period_type="monthly", period_start=_DAY)
            )
            s.execute(
                sa.insert(_table(CostEvent)).values(
                    tenant_id=OLD, service_key="late", provider="p", created_at=_NOW
                )
            )
        with pytest.raises(RuntimeError, match=r"uq_token_usage"), eng.begin() as conn:
            rekey(conn, old=OLD, new=NEW)
        with eng.connect() as conn:
            assert _count(conn, "token_usage", OLD) == 1
            assert _count(conn, "billing_cost_events", OLD) == 1
        with eng.begin() as conn:
            conn.execute(sa.text("SET LOCAL billing.delete_reason = 'test 0054 cleanup'"))
            for table in ("token_usage", "billing_cost_events"):
                conn.execute(
                    sa.text(f"DELETE FROM {table} WHERE tenant_id = CAST(:t AS uuid)"), {"t": OLD}
                )

        # Downgrade = reverse mapping, audit rows and path included.
        await asyncio.to_thread(command.downgrade, _cfg(url), PREVIOUS)
        with eng.connect() as conn:
            for table, column, moved, _bystander in _TENANT_COLUMNS:
                assert _count(conn, table, OLD, column) == moved, f"{table}.{column}"
                assert _count(conn, table, NEW, column) == 0, f"{table}.{column}"
            assert _audit(conn) == audit_before
            assert (OLD, "/api/admin/licenses/budget/" + OLD) in _paths(conn)
        _assert_audit_update_refused(eng)
    finally:
        eng.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        admin.dispose()
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
