"""Alembic 0021 — actor grain, quantity_unit, cash-vs-book on billing_cost_events.

⚠️ ``conftest`` builds the schema with ``Base.metadata.create_all``, so — same
caveat as ``tests/test_attribution_status.py`` — nothing else in this suite
executes alembic 0021 unless a test does it explicitly. The backfill tests
below do that (mirroring ``TestMigration0020ActuallyRuns``); the trigger-survival
test at the bottom runs the FULL chain from 0001 against a throwaway database,
because the truncate/audit guard triggers this migration must not disturb are
only real (with a genuine partitioned parent + a genuine partition) when the
chain itself builds the schema — ``create_all`` never partitions
``billing_cost_events`` at all (see ``scripts/validate_migrations.py``).
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy import insert, select, text

from models.db import CostEvent, ProviderPricing
from services.pricing import (
    QUANTITY_UNIT_BY_COST_MODEL,
    QUANTITY_UNIT_BY_PROVIDER,
)
from tests.conftest import T, throwaway_db_name

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
REVISION = "0021_cost_event_actor_pricing"
PREVIOUS_REVISION = "0020_cost_daily_attribution"
_INDEX = "ix_costs_actor_created"
_NEW_COLUMNS = ("actor_id", "quantity_unit", "cost_basis", "vendor_cost_usd", "currency")

_TRIGGER_NAMES = (
    "billing_truncate_guard_trg",
    "billing_unrolled_truncate_guard_trg",
    "billing_truncate_audit_trg",
    "billing_delete_audit_trg",
)


def _cfg() -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    return cfg


def _upgrade_to_0021() -> None:
    cfg = _cfg()
    command.stamp(cfg, PREVIOUS_REVISION)
    command.upgrade(cfg, REVISION)


# ---------------------------------------------------------------------------
# 1. The chain itself
# ---------------------------------------------------------------------------


class TestMigration0021ChainShape:
    @pytest.mark.integration
    def test_the_chain_is_linear_and_0021_follows_0020(self):
        """LINEARITY is the invariant, not that 0021 is forever the tip.

        A literal ``get_heads() == [REVISION]`` assertion is a tripwire every
        later migration has to come back and edit (see the same fix already
        applied to tests/test_migration_00{10,12,13}*.py and
        tests/test_attribution_status.py for 0020) — pin the single-head
        invariant and this revision's own position instead.
        """
        from alembic.script import ScriptDirectory

        cfg = _cfg()
        script = ScriptDirectory.from_config(cfg)
        heads = list(script.get_heads())
        assert len(heads) == 1, f"alembic chain has branched: {heads}"
        assert script.get_revision(REVISION).down_revision == PREVIOUS_REVISION


# ---------------------------------------------------------------------------
# 2. The backfill actually runs, against real rows
# ---------------------------------------------------------------------------
#
# create_all has already added the 5 new columns (they are on the ORM model),
# so a test that only upgrades and checks "the column exists" would pass
# against a migration that did nothing. Drop them first — same discipline as
# test_attribution_status.py's TestMigration0020ActuallyRuns.


async def _seed_events(db_session_factory) -> dict[str, uuid.UUID]:
    """One row per backfill rung, plus one genuinely un-derivable row."""
    ts = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    ids = {
        "pgx_local": uuid.uuid4(),  # provider-name rung -> 'tokens'
        "mlx_embed": uuid.uuid4(),  # provider-name rung -> 'characters'
        "cost_model_rung": uuid.uuid4(),  # unmapped provider, pricing_id -> per_second
        "unknown": uuid.uuid4(),  # no provider match, no pricing_id -> stays NULL
    }
    tenant = T("attrib-backfill")
    async with db_session_factory() as session:
        pricing_row = ProviderPricing(
            provider="asr-unmapped",
            model=None,
            cost_model="per_second",
            input_per_1m_usd=Decimal("100"),
            effective_from=ts,
        )
        session.add(pricing_row)
        await session.flush()

        await session.execute(
            insert(CostEvent),
            [
                {
                    "id": ids["pgx_local"],
                    "tenant_id": tenant,
                    "tenant_name": "",
                    "service_key": "inference-router",
                    "provider": "pgx-local",
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cost_usd": Decimal("0.002"),
                    "pricing_id": None,
                    "created_at": ts,
                },
                {
                    "id": ids["mlx_embed"],
                    "tenant_id": tenant,
                    "tenant_name": "",
                    "service_key": "mlx-backend",
                    "provider": "mlx-embed",
                    "input_tokens": 512,
                    "output_tokens": 0,
                    "cost_usd": Decimal("0.0001"),
                    "pricing_id": None,
                    "created_at": ts,
                },
                {
                    "id": ids["cost_model_rung"],
                    "tenant_id": tenant,
                    "tenant_name": "",
                    "service_key": "asr-service",
                    "provider": "asr-unmapped",
                    "input_tokens": 30,
                    "output_tokens": 0,
                    "cost_usd": Decimal("0.05"),
                    "pricing_id": pricing_row.id,
                    "created_at": ts,
                },
                {
                    "id": ids["unknown"],
                    "tenant_id": tenant,
                    "tenant_name": "",
                    "service_key": "mystery-service",
                    "provider": "totally-unrecognised-provider",
                    "input_tokens": 7,
                    "output_tokens": 0,
                    "cost_usd": Decimal("0.001"),
                    "pricing_id": None,
                    "created_at": ts,
                },
            ],
        )
        await session.commit()
    return ids


class TestMigration0021BackfillActuallyRuns:
    @pytest.mark.integration
    async def test_quantity_unit_backfill_agrees_with_the_python_mapping(
        self, db_session, db_session_factory
    ):
        ids = await _seed_events(db_session_factory)

        await db_session.execute(text(f"DROP INDEX IF EXISTS {_INDEX}"))
        for col in _NEW_COLUMNS:
            await db_session.execute(
                text(f"ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS {col}")
            )
        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()

        await asyncio.to_thread(_upgrade_to_0021)

        async with db_session_factory() as session:
            rows = (
                await session.execute(
                    select(
                        CostEvent.id,
                        CostEvent.quantity_unit,
                        CostEvent.cost_basis,
                        CostEvent.currency,
                    )
                )
            ).all()
        by_id = {r[0]: r for r in rows}

        # Provider-name rung — the operator's own measured mapping, and it
        # must agree with services.pricing.QUANTITY_UNIT_BY_PROVIDER exactly.
        assert by_id[ids["pgx_local"]][1] == QUANTITY_UNIT_BY_PROVIDER["pgx-local"] == "tokens"
        assert by_id[ids["mlx_embed"]][1] == QUANTITY_UNIT_BY_PROVIDER["mlx-embed"] == "characters"
        # cost_model fallback rung — provider not in the measured table, but
        # its pricing_id resolves to a per_second row.
        assert (
            by_id[ids["cost_model_rung"]][1]
            == QUANTITY_UNIT_BY_COST_MODEL["per_second"]
            == "seconds"
        )
        # Neither rung reaches this one — must stay NULL, never guessed.
        assert by_id[ids["unknown"]][1] is None

        # cost_basis / currency: single-default backfill, correct for all rows.
        for _id, _unit, cost_basis, currency in rows:
            assert cost_basis == "internal_transfer"
            assert currency == "USD"

        index_exists = (
            await db_session.execute(
                text("SELECT count(*) FROM pg_indexes WHERE indexname = :n"), {"n": _INDEX}
            )
        ).scalar()
        assert index_exists == 1, f"{_INDEX} missing after upgrade"

        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()

    @pytest.mark.integration
    async def test_actor_id_and_vendor_cost_usd_stay_null_after_backfill(
        self, db_session, db_session_factory
    ):
        """Decisions 1 and 3: nothing here is ever guessed for history."""
        await _seed_events(db_session_factory)

        await db_session.execute(text(f"DROP INDEX IF EXISTS {_INDEX}"))
        for col in _NEW_COLUMNS:
            await db_session.execute(
                text(f"ALTER TABLE billing_cost_events DROP COLUMN IF EXISTS {col}")
            )
        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()

        await asyncio.to_thread(_upgrade_to_0021)

        async with db_session_factory() as session:
            rows = (
                await session.execute(select(CostEvent.actor_id, CostEvent.vendor_cost_usd))
            ).all()
        assert rows, "nothing rolled up -- the comparison would be vacuous"
        assert all(actor_id is None for actor_id, _ in rows)
        assert all(vendor_cost is None for _, vendor_cost in rows)

        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()


# ---------------------------------------------------------------------------
# 3. The truncate guard survives -- proved on a REAL partitioned parent AND a
#    REAL partition, which only exist when the chain builds the schema from
#    scratch (create_all never partitions billing_cost_events).
# ---------------------------------------------------------------------------

_THROWAWAY_DB = throwaway_db_name("_billing_0021_trigger_check")
_PARTITION = "billing_cost_events_2026_09"  # created by 0001 baseline for this month


def _server_url(dsn: str) -> sa.engine.URL:
    return sa.make_url(dsn).set(database="postgres")


def _recreate(server: sa.engine.URL, name: str) -> sa.engine.URL:
    eng = sa.create_engine(server, isolation_level="AUTOCOMMIT", future=True)
    with eng.connect() as conn:
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    eng.dispose()
    return server.set(database=name)


def _upgrade_head_on(url: sa.engine.URL) -> None:
    cfg = _cfg()
    cfg.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
    os.environ["DATABASE_URL"] = url.render_as_string(hide_password=False)
    command.upgrade(cfg, "head")


class TestTruncateGuardSurvivesMigration0021:
    @pytest.mark.integration
    async def test_all_four_triggers_are_present_and_enabled_on_parent_and_a_partition(self):
        dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
        assert dsn, "TEST_DATABASE_URL/DATABASE_URL must be set"
        server = _server_url(dsn)
        db_url = _recreate(server, _THROWAWAY_DB)

        saved_database_url = os.environ.get("DATABASE_URL")
        try:
            await asyncio.to_thread(_upgrade_head_on, db_url)

            eng = sa.create_engine(db_url, future=True)
            with eng.connect() as conn:
                for relation in ("billing_cost_events", _PARTITION):
                    rows = conn.execute(
                        sa.text(
                            "SELECT t.tgname, t.tgenabled FROM pg_trigger t "
                            "JOIN pg_class c ON c.oid = t.tgrelid "
                            "WHERE c.relname = :rel AND NOT t.tgisinternal"
                        ),
                        {"rel": relation},
                    ).all()
                    by_name = {r[0]: r[1] for r in rows}
                    for trg in _TRIGGER_NAMES:
                        assert trg in by_name, f"{trg} missing on {relation}"
                        assert by_name[trg] == "O", (
                            f"{trg} on {relation} is not enabled (tgenabled={by_name[trg]!r})"
                        )

                # The new columns exist on the parent too, post full-chain build.
                cols = {
                    r[0]
                    for r in conn.execute(
                        sa.text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_name = 'billing_cost_events'"
                        )
                    ).all()
                }
                assert set(_NEW_COLUMNS) <= cols
            eng.dispose()
        finally:
            if saved_database_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = saved_database_url
            cleanup_eng = sa.create_engine(server, isolation_level="AUTOCOMMIT", future=True)
            with cleanup_eng.connect() as conn:
                conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_THROWAWAY_DB}" WITH (FORCE)'))
            cleanup_eng.dispose()
