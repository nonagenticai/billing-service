"""alembic 0051 against the REAL chain: re-key ``portal-render-e2e``, history included.

* head is single and descends from 0051;
* with only a bystander tenant present the re-key moves nothing;
* rows seeded on the OLD id at 0050 are on the NEW id after ``upgrade head`` in
  EVERY table the revision moves — the live-config set AND the ledger: cost and
  metering events, the five rollups, alerts, the money-budget adjustment ledger,
  the DLQ receipts/archive, ``billing_delete_audit.tenant_ids`` and the crosswalk;
* ``metadata.attributed_from_tenant_id`` moves and NOTHING else in that document;
* the same uuid in a USER sense (``subject_id``, ``actor_id``, ``actor_sub``,
  ``reattributed_by``, JSON ``actor``/``subject_id`` keys) is untouched;
* ``audit_log`` and ``billing_invoices`` (immutability triggers) are untouched;
* a second run moves nothing;
* a ROLLUP slot held on both ids is refused and nothing is written;
* the downgrade applies the reverse mapping.

MUTATION THAT TURNS THIS RED: delete the history entries from ``_UPDATES`` (i.e.
make 0051 a copy of 0048) and the ``_TENANT_COLUMNS`` loop fails on its first
ledger entry — ``billing_cost_events.tenant_id: 2 rows still on OLD``. Delete
only the ``metadata`` statement and ``attributed_from`` fails. Replace it with a
blanket text replace and the same assertion fails on its ``actor`` column. Delete a
rollup entry from ``_COLLISION_CHECKS`` and the refusal turns into a raw
``IntegrityError`` instead of the ``RuntimeError`` the test matches.
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
from sqlalchemy.orm import Session

from models.db import (
    NULL_APP_ID,
    NULL_LICENSE_ID,
    NULL_PLAN_ID,
    NULL_SUBJECT_ID,
)
from models.db.audit import DlqArchive, DlqReattribution
from models.db.budgets import (
    BudgetOverageAlert,
    MoneyBudget,
    MoneyBudgetAdjustment,
    TenantWebhook,
    TokenBudget,
    TokenUsage,
)
from models.db.config import AttributionRule, MarkupRule, TenantAccessCharge, TenantConfig
from models.db.events import CostEvent, MeteringEvent
from models.db.quota import RequestQuotaConfig
from models.db.rollups import CostActorDaily, CostDaily, MeteringDaily, MeteringHourly
from tests.conftest import throwaway_db_name


def _table(model: Any) -> sa.Table:
    """An ORM model's ``__table__`` as the ``Table`` it is at runtime."""
    return cast(sa.Table, model.__table__)


REPO_ROOT = Path(__file__).resolve().parent.parent
_DB = throwaway_db_name("_billing_0051_check")

# Literals, NOT imported from the migration: importing them would make this
# test agree with any typo in either constant.
OLD = "e148e8af-3751-49bf-a0fa-56ab46d31ddc"
NEW = "01a1113b-674f-72b0-a84b-dc746d39afca"
BYSTANDER = "0195f48b-d000-7000-8000-000000000099"
REVISION = "0051_rekey_portal_render_e2e"
PREVIOUS = "0050_app_registry"

_DAY = date(2026, 9, 1)
_HOUR = datetime(2026, 9, 1, 10, tzinfo=UTC)
_NOW = datetime(2026, 9, 1, 10, 30, tzinfo=UTC)

# (table, uuid column, rows seeded on OLD, rows seeded on BYSTANDER). The first
# eleven are 0048's live-config set; everything after is history.
_TENANT_COLUMNS: tuple[tuple[str, str, int, int], ...] = (
    ("token_budgets", "tenant_id", 2, 1),
    ("tenant_money_budget", "tenant_id", 1, 0),
    ("request_quota_config", "tenant_id", 1, 1),
    ("tenant_config", "tenant_id", 1, 0),
    ("tenant_webhooks", "tenant_id", 1, 0),
    ("markup_rules", "tenant_id", 1, 0),
    ("tenant_access_charge", "tenant_id", 1, 0),
    ("billing_attribution_rules", "source_tenant_id", 1, 1),
    ("billing_attribution_rules", "bill_tenant_id", 1, 1),
    ("billing_cost_events", "tenant_id", 2, 2),
    ("billing_metering_events", "tenant_id", 1, 1),
    ("billing_metering_events", "retarget_tenant_id", 1, 0),
    ("billing_metering_hourly", "tenant_id", 1, 1),
    ("billing_metering_daily", "tenant_id", 1, 1),
    ("billing_cost_daily", "tenant_id", 1, 1),
    ("billing_cost_actor_daily", "tenant_id", 1, 1),
    ("token_usage", "tenant_id", 2, 1),
    ("budget_overage_alerts", "tenant_id", 1, 0),
    ("tenant_money_budget_adjustment", "tenant_id", 1, 0),
    ("billing_dlq_reattributions", "tenant_id", 1, 0),
    ("tenant_id_crosswalk", "tenant_uuid", 1, 0),
)

# Tables whose ``tenant_name`` was seeded once as the uuid-as-text (must move)
# next to a real name where a second row exists (must not).
_NAME_TABLES = (
    "token_budgets",
    "tenant_config",
    "billing_cost_events",
    "billing_metering_events",
    "billing_metering_hourly",
    "billing_metering_daily",
    "billing_cost_daily",
    "token_usage",
)

# The same uuid in a USER sense: (label, FROM/WHERE tail, rows holding OLD — on
# the re-keyed tenant's rows and the bystander's alike).
_USER_SENSE: tuple[tuple[str, str, int], ...] = (
    ("billing_cost_events.subject_id", "billing_cost_events WHERE subject_id = :o", 4),
    ("billing_cost_events.actor_id", "billing_cost_events WHERE actor_id = :o", 4),
    ("metadata.actor", "billing_cost_events WHERE metadata ->> 'actor' = :t", 4),
    ("usage_extra.user", "billing_cost_events WHERE usage_extra ->> 'user' = :t", 4),
    ("billing_metering_events.subject_id", "billing_metering_events WHERE subject_id = :o", 2),
    ("billing_cost_daily.subject_id", "billing_cost_daily WHERE subject_id = :o", 2),
    ("billing_cost_actor_daily.actor_id", "billing_cost_actor_daily WHERE actor_id = :o", 2),
    ("attribution_rules.actor_id", "billing_attribution_rules WHERE actor_id = :o", 2),
    ("money_adjustment.actor_sub", "tenant_money_budget_adjustment WHERE actor_sub = :t", 1),
    ("dlq.reattributed_by", "billing_dlq_reattributions WHERE reattributed_by = :t", 1),
    (
        "dlq.original_fields.subject_id",
        "billing_dlq_reattributions WHERE original_fields ->> 'subject_id' = :t",
        1,
    ),
    ("dlq_archive.fields.actor_id", "billing_dlq_archive WHERE fields ->> 'actor_id' = :t", 1),
)


def _cfg(url: sa.engine.URL | None = None) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    if url is not None:
        cfg.set_main_option(
            "sqlalchemy.url", url.render_as_string(hide_password=False)
        )  # pragma: allowlist secret
    return cfg


def test_single_head_descends_from_0051() -> None:
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


def _scalar(conn: sa.Connection, sql: str, **params: object) -> object:
    return conn.execute(sa.text(sql), params).scalar_one()


def _user_sense(conn: sa.Connection) -> dict[str, int]:
    return {
        label: conn.execute(
            sa.text(f"SELECT count(*) FROM {tail}".replace(":o", "CAST(:t AS uuid)")), {"t": OLD}
        ).scalar_one()
        for label, tail, _n in _USER_SENSE
    }


def _protected_snapshot(conn: sa.Connection) -> list[tuple]:
    rows = conn.execute(
        sa.text(
            "SELECT 'audit', id::text, tenant_id::text, tenant_name, after::text FROM audit_log "
            "UNION ALL "
            "SELECT 'invoice', id::text, tenant_id::text, tenant_name, snapshot::text "
            "FROM billing_invoices ORDER BY 1, 2"
        )
    )
    return [tuple(r) for r in rows]


def _cost_event(tenant: str, *, attributed_from: str) -> sa.Insert:
    """Core insert naming ONLY columns 0050 has (an ORM add names every mapped column,
    so a column added by a LATER migration -- 0053 ``funding`` -- breaks the seed)."""
    user = uuid.UUID(OLD)
    return sa.insert(CostEvent).values(
        tenant_id=tenant,
        tenant_name=tenant,
        service_key="w2v-tts",
        provider="p",
        subject_id=user,
        actor_id=user,
        created_at=_NOW,
        usage_extra={"user": OLD},
        extra_metadata={"attributed_from_tenant_id": attributed_from, "actor": OLD},
    )


def _seed_tenant(s: Session, tenant: str, *, full: bool) -> None:
    """One row per moved table for ``tenant``. ``full`` adds the OLD-only shapes."""
    user = uuid.UUID(OLD)  # the SAME uuid, in its user sense, on every tenant's rows
    s.add(TokenBudget(tenant_id=tenant, tenant_name=tenant, budget_tokens=1000))
    s.add(RequestQuotaConfig(license_id=uuid.uuid4(), tenant_id=tenant, quota_monthly=10))
    s.add(
        AttributionRule(
            source_tenant_id=tenant,
            match_project="any",
            bill_tenant_id=tenant,
            actor_id=user,
            reason="seed",
        )
    )
    s.execute(_cost_event(tenant, attributed_from=BYSTANDER))
    if full:
        s.execute(_cost_event(tenant, attributed_from=OLD))
    # Core insert naming ONLY the columns 0050 has: an ORM add names every
    # mapped column, so a column added by a LATER migration (0052 actor_id)
    # breaks the seed at PREVIOUS. Python-side defaults (id) still apply.
    s.execute(
        sa.insert(_table(MeteringEvent)).values(
            tenant_id=tenant,
            tenant_name=tenant,
            retarget_tenant_id=tenant if full else None,
            subject_id=user,
            method="POST",
            path="/x",
            service_key="w2v-tts",
            status="ok",
            created_at=_NOW,
        )
    )
    rollup = {
        "tenant_id": tenant,
        "tenant_name": tenant,
        "license_id": NULL_LICENSE_ID,
        "app_id": NULL_APP_ID,
        "plan_id": NULL_PLAN_ID,
        "subject_id": NULL_SUBJECT_ID,
        "service_key": "w2v-tts",
        "request_count": 1,
        "denied_count": 0,
    }
    s.add(MeteringHourly(bucket_hour=_HOUR, **rollup))
    s.add(MeteringDaily(bucket_date=_DAY, **rollup))
    cost = {
        "tenant_id": tenant,
        "license_id": NULL_LICENSE_ID,
        "service_key": "w2v-tts",
        "provider": "p",
        "served_model": "m",
        "byo": False,
        "owner_provided": False,
        "bucket_date": _DAY,
        "total_input_tokens": 1,
        "total_output_tokens": 1,
        "total_cost_usd": Decimal("0.5"),
        "request_count": 1,
    }
    s.add(CostDaily(tenant_name=tenant, subject_id=user, **cost))
    s.add(CostActorDaily(subject_id=NULL_SUBJECT_ID, actor_id=user, **cost))
    s.add(
        TokenUsage(
            tenant_id=tenant,
            tenant_name=tenant,
            period_type="monthly",
            period_start=_DAY,
            total_tokens=7,
        )
    )
    if not full:
        return
    # Real-name twins: the name must survive on the moved row.
    s.add(TokenBudget(tenant_id=tenant, tenant_name="portal-render-e2e", budget_tokens=2000))
    s.add(
        TokenUsage(
            tenant_id=tenant,
            tenant_name="portal-render-e2e",
            period_type="weekly",
            period_start=_DAY,
        )
    )
    s.add(TenantConfig(tenant_id=tenant, tenant_name=tenant))
    budget = MoneyBudget(tenant_id=tenant, currency="EUR", amount=100)
    s.add(budget)
    s.flush()
    s.add(TenantWebhook(tenant_id=tenant, url="https://example.invalid/h", secret="s"))
    s.add(MarkupRule(tenant_id=tenant, markup_pct=Decimal("10"), effective_from=_NOW))
    s.add(TenantAccessCharge(tenant_id=tenant, amount_usd=Decimal("1"), effective_from=_NOW))
    s.add(
        BudgetOverageAlert(
            tenant_id=tenant, budget_id=uuid.uuid4(), threshold_pct=80, period_start=_NOW
        )
    )
    s.add(
        MoneyBudgetAdjustment(
            tenant_id=tenant,
            budget_id=budget.id,
            kind="currency_conversion",
            from_currency="EUR",
            to_currency="USD",
            fx_rate=Decimal("1.1"),
            usd_rate_from=Decimal("1.1"),
            usd_rate_to=Decimal("1"),
            amount_before=100,
            amount_after=110,
            consumed_before=Decimal("0"),
            consumed_after=Decimal("0"),
            remaining_before=Decimal("100"),
            remaining_after=Decimal("110"),
            actor_sub=OLD,
        )
    )
    s.add(
        DlqReattribution(
            dead_entry_id="1-0",
            replay_marker="m-1",
            reattributed_from=tenant,
            tenant_id=tenant,
            reattributed_by=OLD,
            reason="seed",
            original_fields={"tenant_id": tenant, "subject_id": OLD},
            reinjected_stream_id="2-0",
        )
    )
    s.add(
        DlqArchive(
            stream="billing:costs:dead",
            dead_entry_id="3-0",
            fields={"tenant_id": tenant, "actor_id": OLD},
        )
    )
    s.execute(
        sa.text(
            "INSERT INTO billing_delete_audit "
            "(table_name, operation, db_user, session_user_name, backend_pid, "
            " distinct_tenant_count, tenant_ids) VALUES "
            "('billing_cost_events', 'DELETE', 'u', 'u', 1, 2, "
            " ARRAY[CAST(:t AS uuid), CAST(:b AS uuid)])"
        ),
        {"t": tenant, "b": BYSTANDER},
    )
    s.execute(
        sa.text(
            "INSERT INTO tenant_id_crosswalk (legacy_value, tenant_uuid, source) "
            "VALUES ('portal-render-e2e', CAST(:t AS uuid), 'seed')"
        ),
        {"t": tenant},
    )
    # Protected tables — must come out byte-identical.
    s.execute(
        sa.text(
            "INSERT INTO audit_log (action, resource_type, tenant_id, tenant_name, after) "
            "VALUES ('budget.create', 'token_budget', CAST(:t AS uuid), :t, "
            " jsonb_build_object('tenant_id', CAST(:t AS text)))"
        ),
        {"t": tenant},
    )
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


@pytest.mark.integration
async def test_0051_rekeys_config_and_history() -> None:
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

        # OLD absent: only a bystander tenant (whose rows carry the uuid in its
        # USER sense). Nothing moves.
        with Session(eng) as s, s.begin():
            _seed_tenant(s, BYSTANDER, full=False)
        with eng.begin() as conn:
            assert set(rekey(conn, old=OLD, new=NEW).values()) == {0}

        with Session(eng) as s, s.begin():
            _seed_tenant(s, OLD, full=True)
            # A bystander's event that was re-attributed AWAY from OLD: its
            # tenant_id stays, its metadata key is tenant-sense and moves.
            s.execute(_cost_event(BYSTANDER, attributed_from=OLD))
        with eng.connect() as conn:
            protected_before = _protected_snapshot(conn)
            user_before = _user_sense(conn)
        assert len(protected_before) == 2
        assert user_before == {label: n for label, _t, n in _USER_SENSE}

        await asyncio.to_thread(command.upgrade, _cfg(url), "head")

        with eng.connect() as conn:
            for table, column, moved, bystander in _TENANT_COLUMNS:
                where = f"{table}.{column}"
                assert _count(conn, table, OLD, column) == 0, f"{where}: rows still on OLD"
                assert _count(conn, table, NEW, column) == moved, where
                assert _count(conn, table, BYSTANDER, column) == bystander, where
            for table in _NAME_TABLES:
                names = set(
                    conn.execute(
                        sa.text(
                            f"SELECT tenant_name FROM {table} WHERE tenant_id = CAST(:t AS uuid)"
                        ),
                        {"t": NEW},
                    ).scalars()
                )
                assert names - {"portal-render-e2e"} == {NEW}, table
            assert (
                _scalar(
                    conn,
                    "SELECT count(*) FROM token_budgets WHERE tenant_name = 'portal-render-e2e'",
                )
                == 1
            )

            # The one metadata key, on OLD's rows AND on the bystander's.
            attributed_from = [
                tuple(r)
                for r in conn.execute(
                    sa.text(
                        "SELECT tenant_id::text, metadata ->> 'attributed_from_tenant_id', "
                        "metadata ->> 'actor' FROM billing_cost_events ORDER BY 1, 2"
                    )
                )
            ]
            assert attributed_from == sorted(
                [
                    (NEW, NEW, OLD),
                    (NEW, BYSTANDER, OLD),
                    (BYSTANDER, NEW, OLD),
                    (BYSTANDER, BYSTANDER, OLD),
                ]
            )
            assert (
                _scalar(
                    conn, "SELECT original_fields ->> 'tenant_id' FROM billing_dlq_reattributions"
                )
                == NEW
            )
            assert _scalar(conn, "SELECT reattributed_from FROM billing_dlq_reattributions") == NEW
            assert _scalar(conn, "SELECT fields ->> 'tenant_id' FROM billing_dlq_archive") == NEW
            assert _scalar(conn, "SELECT tenant_ids::text[] FROM billing_delete_audit") == [
                NEW,
                BYSTANDER,
            ]
            assert (
                _scalar(
                    conn,
                    "SELECT legacy_value FROM tenant_id_crosswalk WHERE tenant_uuid = CAST(:t AS uuid)",
                    t=NEW,
                )
                == "portal-render-e2e"
            )

            # USER sense: every one of them still holds the uuid.
            assert _user_sense(conn) == user_before
            # Immutable tables: byte-identical, still on OLD.
            assert _protected_snapshot(conn) == protected_before
            assert _count(conn, "audit_log", OLD) == 1
            assert _count(conn, "billing_invoices", OLD) == 1

        # Re-run: nothing left to move.
        with eng.begin() as conn:
            assert set(rekey(conn, old=OLD, new=NEW).values()) == {0}

        # ROLLUP collision: NEW now holds the (monthly, _DAY) token_usage slot
        # and the cost_daily slot. A late row on OLD in the same slot must be
        # REFUSED, and nothing may be written — including the cost event
        # seeded alongside it.
        with Session(eng) as s, s.begin():
            s.add(
                TokenUsage(tenant_id=OLD, tenant_name=OLD, period_type="monthly", period_start=_DAY)
            )
            s.add(CostEvent(tenant_id=OLD, service_key="late", provider="p", created_at=_NOW))
        with pytest.raises(RuntimeError, match=r"uq_token_usage"), eng.begin() as conn:
            rekey(conn, old=OLD, new=NEW)
        with eng.connect() as conn:
            assert _count(conn, "token_usage", OLD) == 1
            assert _count(conn, "billing_cost_events", OLD) == 1
            assert _count(conn, "billing_cost_events", NEW) == 2
        with eng.begin() as conn:
            conn.execute(sa.text("SET LOCAL billing.delete_reason = 'test 0051 cleanup'"))
            for table in ("token_usage", "billing_cost_events"):
                conn.execute(
                    sa.text(f"DELETE FROM {table} WHERE tenant_id = CAST(:t AS uuid)"), {"t": OLD}
                )

        # Downgrade = reverse mapping, history included.
        await asyncio.to_thread(command.downgrade, _cfg(url), PREVIOUS)
        with eng.connect() as conn:
            for table, column, moved, _bystander in _TENANT_COLUMNS:
                assert _count(conn, table, OLD, column) == moved, f"{table}.{column}"
                assert _count(conn, table, NEW, column) == 0, f"{table}.{column}"
            assert _user_sense(conn) == user_before
            assert _protected_snapshot(conn) == protected_before
    finally:
        eng.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        admin.dispose()
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
