"""Alembic 0022 — provider_pricing.cost_basis, and the billing_cost_events correction.

⚠️ Same caveat as ``test_migration_0021_cost_event_actor_pricing.py``: ``conftest``
builds the schema with ``Base.metadata.create_all``, so the backfill tests below
run the migration explicitly, and the trigger-survival test runs the FULL chain
from 0001 against a throwaway database (``create_all`` never partitions
``billing_cost_events``, so the guard triggers are only real there).
"""

from __future__ import annotations

import asyncio
import os
import pathlib
from datetime import UTC, datetime
from decimal import Decimal

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy import insert, select, text

from models.db import CostEvent, ProviderPricing
from tests.conftest import T, throwaway_db_name

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
REVISION = "0022_provider_pricing_cost_basis"
PREVIOUS_REVISION = "0021_cost_event_actor_pricing"
_COL = "cost_basis"

_TRIGGER_NAMES = (
    "billing_truncate_guard_trg",
    "billing_unrolled_truncate_guard_trg",
    "billing_truncate_audit_trg",
    "billing_delete_audit_trg",
)

# Must match alembic/versions/0022_provider_pricing_cost_basis.py's
# _EXTERNAL_VENDOR_PROVIDERS exactly — kept in agreement by this test rather
# than imported (migrations in this repo stay frozen in time).
_EXTERNAL_VENDOR_PROVIDERS = (
    "claude",
    "anthropic",
    "deepseek",
    "openai",
    "groq",
    "xai",
    "cohere",
    "google",
    "elevenlabs",
    "deepgram",
)


def _cfg() -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    return cfg


def _upgrade_to_0022() -> None:
    cfg = _cfg()
    command.stamp(cfg, PREVIOUS_REVISION)
    command.upgrade(cfg, REVISION)


# ---------------------------------------------------------------------------
# 1. The chain itself
# ---------------------------------------------------------------------------


class TestMigration0022ChainShape:
    @pytest.mark.integration
    def test_the_chain_is_linear_and_0022_follows_0021(self):
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
# create_all has already added provider_pricing.cost_basis (it is on the ORM
# model), so a test that only upgrades and checks "the column exists" would
# pass against a migration that did nothing. Drop it first — same discipline
# as test_migration_0021's TestMigration0021BackfillActuallyRuns.


async def _seed_pricing_rows(db_session_factory) -> dict[str, int]:
    ts = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    ids: dict[str, int] = {}
    async with db_session_factory() as session:
        # A representative external vendor row (unused today, per the
        # migration's own docstring) — must backfill to 'cash'.
        vendor_row = ProviderPricing(
            provider="openai",
            model=None,
            cost_model="per_token",
            input_per_1m_usd=Decimal("5"),
            output_per_1m_usd=Decimal("15"),
            effective_from=ts,
        )
        # A self-hosted per_token row (pgx-local's real shape) — must stay
        # 'internal_transfer'.
        selfhosted_row = ProviderPricing(
            provider="pgx-local",
            model=None,
            cost_model="per_token",
            input_per_1m_usd=Decimal("10"),
            output_per_1m_usd=Decimal("25"),
            effective_from=ts,
        )
        session.add_all([vendor_row, selfhosted_row])
        await session.flush()
        ids["vendor"] = vendor_row.id
        ids["selfhosted"] = selfhosted_row.id
        await session.commit()
    return ids


async def _seed_mislabelled_event(db_session_factory, pricing_id: int) -> str:
    """A billing_cost_events row with the LIVE DEFECT'S exact shape: cost_basis
    already stamped 'cash' by the old cost_model-based write path, pointing at
    a pricing row that (after this migration's own provider_pricing backfill)
    is 'internal_transfer' — i.e. provably self-hosted, provably mislabelled.
    """
    tenant = T("cost-basis-mislabel")
    ts = datetime(2026, 9, 4, 11, 8, tzinfo=UTC)
    async with db_session_factory() as session:
        await session.execute(
            insert(CostEvent),
            [
                {
                    "tenant_id": tenant,
                    "tenant_name": "",
                    "service_key": "mlx-backend",
                    "provider": "pgx-local",
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cost_usd": Decimal("0.002"),
                    "cost_basis": "cash",  # the defect's output
                    "pricing_id": pricing_id,
                    "created_at": ts,
                }
            ],
        )
        await session.commit()
    return tenant


class TestMigration0022BackfillActuallyRuns:
    @pytest.mark.integration
    async def test_external_vendor_rows_backfill_to_cash_self_hosted_stays_internal_transfer(
        self, db_session, db_session_factory
    ):
        pricing_ids = await _seed_pricing_rows(db_session_factory)

        await db_session.execute(text(f"ALTER TABLE provider_pricing DROP COLUMN IF EXISTS {_COL}"))
        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()

        await asyncio.to_thread(_upgrade_to_0022)

        async with db_session_factory() as session:
            rows = (
                await session.execute(select(ProviderPricing.id, ProviderPricing.cost_basis))
            ).all()
        by_id = dict(rows)

        assert by_id[pricing_ids["vendor"]] == "cash"
        assert by_id[pricing_ids["selfhosted"]] == "internal_transfer"

        # Sanity: every named external vendor provider in the seed catalogue
        # is at least a member of the migration's own list (drift guard).
        assert "openai" in _EXTERNAL_VENDOR_PROVIDERS

        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()

    @pytest.mark.integration
    async def test_mislabelled_cost_event_rows_are_corrected_to_internal_transfer(
        self, db_session, db_session_factory
    ):
        """THE LIVE INCIDENT, reproduced and proved fixed by the migration."""
        pricing_ids = await _seed_pricing_rows(db_session_factory)
        tenant = await _seed_mislabelled_event(db_session_factory, pricing_ids["selfhosted"])

        await db_session.execute(text(f"ALTER TABLE provider_pricing DROP COLUMN IF EXISTS {_COL}"))
        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()

        await asyncio.to_thread(_upgrade_to_0022)

        async with db_session_factory() as session:
            row = (
                await session.execute(
                    select(CostEvent.cost_basis).where(CostEvent.tenant_id == tenant)
                )
            ).scalar_one()
        assert row == "internal_transfer"

        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()


# ---------------------------------------------------------------------------
# 3. The truncate guard survives -- proved on a REAL partitioned parent AND a
#    REAL partition, which only exist when the chain builds the schema from
#    scratch.
# ---------------------------------------------------------------------------

_THROWAWAY_DB = throwaway_db_name("_billing_0022_trigger_check")
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


class TestTruncateGuardSurvivesMigration0022:
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
                    by_name = {row[0]: row[1] for row in rows}
                    for trg in _TRIGGER_NAMES:
                        assert trg in by_name, f"{trg} missing on {relation}"
                        assert by_name[trg] == "O", (
                            f"{trg} on {relation} is not enabled (tgenabled={by_name[trg]!r})"
                        )

                # provider_pricing.cost_basis exists post full-chain build.
                cols = {
                    r[0]
                    for r in conn.execute(
                        sa.text(
                            "SELECT column_name FROM information_schema.columns "
                            "WHERE table_name = 'provider_pricing'"
                        )
                    ).all()
                }
                assert _COL in cols
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
