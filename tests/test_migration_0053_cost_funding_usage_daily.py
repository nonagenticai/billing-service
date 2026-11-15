"""alembic 0053 against the REAL chain: the funding column, the cross rollup, both backfills.

* single head, descends from 0053, revision id fits ``varchar(32)``;
* events seeded at 0052 (no funding column yet) carry the derived funding after
  ``upgrade head`` -- byo / owner_provided / cash / internal_transfer / unknown;
* ``billing_cost_usage_daily`` is backfilled for COMPLETE days only (today is
  left to ``rollup_daily``) and parked rows are excluded;
* the new table carries ``billing_truncate_guard_trg`` (TRUNCATE refused);
* the CHECK refuses a value outside the set;
* downgrade drops both.

MUTATION THAT TURNS THIS RED: delete ``backfill_funding(bind)`` from ``upgrade()``
and every funding assertion fails; delete ``_ARM_TRUNCATE_GUARD`` and the TRUNCATE
succeeds; drop the ``< today`` predicate and today's row is backfilled.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.exc import DBAPIError

from tests.conftest import T, throwaway_db_name

REPO_ROOT = Path(__file__).resolve().parent.parent
_DB = throwaway_db_name("_billing_0053_check")
REVISION = "0053_cost_funding_usage_daily"
PREVIOUS = "0052_metering_actor_id"


def _cfg(url: sa.engine.URL | None = None) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    if url is not None:
        cfg.set_main_option(
            "sqlalchemy.url", url.render_as_string(hide_password=False)
        )  # pragma: allowlist secret
    return cfg


def test_single_head_descends_from_0053() -> None:
    script = ScriptDirectory.from_config(_cfg())
    heads = script.get_heads()
    assert len(heads) == 1
    chain = {rev.revision for rev in script.walk_revisions(base="base", head=heads[0])}
    assert REVISION in chain
    rev = script.get_revision(REVISION)
    assert rev is not None
    assert rev.down_revision == PREVIOUS
    assert len(REVISION) <= 32


_INSERT_EVENT = sa.text(
    "INSERT INTO billing_cost_events (id, tenant_id, tenant_name, service_key, provider, "
    "served_model, input_tokens, output_tokens, cache_creation_input_tokens, "
    "cache_read_input_tokens, cost_usd, usage_extra, pricing_id, cost_source, "
    "excluded_from_billing, created_at) VALUES (gen_random_uuid(), CAST(:t AS uuid), '', "
    "'sk', :prov, 'm', 1, 1, 0, 0, :cost, CAST(:ux AS jsonb), :pid, 'src', "
    ":excl, :ts)"
)


@pytest.mark.integration
async def test_0053_backfills_funding_and_the_cross_rollup() -> None:
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
    yesterday = datetime.now(UTC) - timedelta(days=1)
    now = datetime.now(UTC)
    cases = {
        # label: (provider, usage_extra, pricing basis or None, expected funding)
        "byo": ("openai", '{"byo": true}', "cash", "tenant_byo"),
        "owner": (
            "pgx-local",
            '{"owner_provided": true}',
            "internal_transfer",
            "platform_external",
        ),
        "cash": ("openai", "{}", "cash", "platform_external"),
        "local": ("mlx-nlp", "{}", "internal_transfer", "local_gpu"),
        "unknown": ("brand-new-vendor", "{}", None, None),
    }
    try:
        await asyncio.to_thread(command.upgrade, _cfg(url), PREVIOUS)
        with eng.begin() as conn:
            pids: dict[str, int] = {}
            for prov, basis in {(c[0], c[2]) for c in cases.values() if c[2]}:
                pids[prov] = conn.execute(
                    sa.text(
                        "INSERT INTO provider_pricing (provider, cost_model, cost_basis, "
                        "input_per_1m_usd, output_per_1m_usd, effective_from) VALUES "
                        "(:p, 'per_token', :b, 1, 1, '2026-01-01') RETURNING id"
                    ),
                    {"p": prov, "b": basis},
                ).scalar_one()
            for label, (prov, ux, basis, _f) in cases.items():
                conn.execute(
                    _INSERT_EVENT,
                    {
                        "t": T(f"mig53-{label}"),
                        "prov": prov,
                        "cost": 1,
                        "ux": ux,
                        "pid": pids.get(prov) if basis else None,
                        "excl": False,
                        "ts": yesterday,
                    },
                )
            # parked (excluded) and today's rows: neither may be backfilled into the rollup.
            conn.execute(
                _INSERT_EVENT,
                {
                    "t": T("mig53-local"),
                    "prov": "mlx-nlp",
                    "cost": 500,
                    "ux": "{}",
                    "pid": pids["mlx-nlp"],
                    "excl": True,
                    "ts": yesterday,
                },
            )
            conn.execute(
                _INSERT_EVENT,
                {
                    "t": T("mig53-today"),
                    "prov": "mlx-nlp",
                    "cost": 7,
                    "ux": "{}",
                    "pid": pids["mlx-nlp"],
                    "excl": False,
                    "ts": now,
                },
            )

        await asyncio.to_thread(command.upgrade, _cfg(url), "head")

        with eng.connect() as conn:
            assert conn.execute(sa.text("SELECT version_num FROM alembic_version")).scalar_one()
            for label, (_p, _u, _b, want) in cases.items():
                got = (
                    conn.execute(
                        sa.text(
                            "SELECT DISTINCT funding FROM billing_cost_events "
                            "WHERE tenant_id = CAST(:t AS uuid)"
                        ),
                        {"t": T(f"mig53-{label}")},
                    )
                    .scalars()
                    .all()
                )
                assert got == [want], (label, got)
            rollup = {
                (str(t), f): (n, c)
                for t, f, n, c in conn.execute(
                    sa.text(
                        "SELECT tenant_id, funding, event_count, total_cost_usd "
                        "FROM billing_cost_usage_daily"
                    )
                )
            }
            assert rollup[(T("mig53-local"), "local_gpu")] == (1, 1)
            assert rollup[(T("mig53-unknown"), "unknown")] == (1, 1)
            assert rollup[(T("mig53-byo"), "tenant_byo")] == (1, 1)
            assert not any(t == T("mig53-today") for t, _f in rollup)
            assert len(rollup) == len(cases)

        with pytest.raises(DBAPIError) as exc, eng.begin() as conn:
            conn.execute(sa.text("TRUNCATE billing_cost_usage_daily"))
        assert "blocked" in str(exc.value).lower(), str(exc.value)

        with pytest.raises(DBAPIError), eng.begin() as conn:
            conn.execute(sa.text("UPDATE billing_cost_events SET funding = 'gratis' WHERE true"))

        await asyncio.to_thread(command.downgrade, _cfg(url), PREVIOUS)
        with eng.connect() as conn:
            assert (
                conn.execute(
                    sa.text("SELECT to_regclass('public.billing_cost_usage_daily')")
                ).scalar_one()
                is None
            )
            assert not conn.execute(
                sa.text(
                    "SELECT 1 FROM information_schema.columns WHERE table_name = "
                    "'billing_cost_events' AND column_name = 'funding'"
                )
            ).first()
    finally:
        eng.dispose()
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{_DB}" WITH (FORCE)'))
        admin.dispose()
