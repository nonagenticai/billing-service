"""The BEFORE TRUNCATE guard on the cost ledger (alembic 0017).

THE DEFECT
----------
2026-09-05 07:01:56.876899Z: an automated client TRUNCATEd
``billing_cost_events`` and all 12 partitions on the prod billing primary, along
with ``billing_metering_events`` and every rollup table. ``billing_user`` OWNS
those relations, so ``REVOKE TRUNCATE`` is inert — TRUNCATE rights flow from
ownership. A ``BEFORE TRUNCATE`` trigger is the only thing that sits between an
owner and the truncation.

WHAT IS ASSERTED HERE
---------------------
Both directions, because a guard that only ever refuses is as broken as one that
only ever allows: an unarmed TRUNCATE must RAISE, and an armed one must still
succeed *and* still produce 0016's audit row. And the partition path, because
the incident's blast radius was the partitions: a trigger on the parent alone
does not fire for a TRUNCATE that names a child.

WHAT IS NOT ASSERTED HERE
-------------------------
That the control stops the incident. It does not, on its own — see
``test_conftest_refuses_a_production_database`` below and the ⛔ section in
``src/models/db/truncate_guard.py``. ``tests/conftest.py`` must arm the hatch for
any DB test to run at all, so this suite pointed at prod would arm it and
truncate prod again. The second half of the control is conftest's refusal to arm
against a production database name, and it is asserted here too.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import DBAPIError

import models.db.truncate_guard as tg
from models.db import ALLOW_TRUNCATE_GUC, ALLOW_TRUNCATE_VALUE, NULL_LICENSE_ID
from tests.conftest import throwaway_db_name

MIGRATION = Path(__file__).parent.parent / "alembic" / "versions" / "0017_truncate_guard.py"
REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_migration():  # noqa: ANN202
    spec = importlib.util.spec_from_file_location("_mig_0017", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tripwire():  # noqa: ANN202
    """Load the production tripwire BY PATH.

    ``tests/`` is deliberately NOT on ``sys.path`` — putting it there shadows
    ``src/routers`` with ``tests/routers`` and breaks every import of
    ``routers.admin``. conftest loads this module the same way, so this is the
    same file the running suite is actually guarded by, not a second copy.
    """
    spec = importlib.util.spec_from_file_location(
        "_production_tripwire_under_test",
        Path(__file__).parent / "_production_tripwire.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cost_event_row(tenant: str, created_at: datetime) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "tenant_id": tenant,
        "tenant_name": tenant,
        "service_key": "guard-test",  # pragma: allowlist secret
        "provider": "test",
        "input_tokens": 1,
        "output_tokens": 1,
        "created_at": created_at,
    }


async def _seed(conn, table: str, rows: list[dict]) -> None:
    cols = ", ".join(rows[0])
    binds = ", ".join(f":{c}" for c in rows[0])
    await conn.execute(sa.text(f"INSERT INTO {table} ({cols}) VALUES ({binds})"), rows)


async def _rolled_up_through(conn, tenant: str, moment: datetime) -> None:
    """Seed a billing_cost_daily bucket covering ``moment``'s UTC day.

    Satisfies the alembic 0019 watermark gate: with the rollup caught up there
    is no un-aggregated revenue to lose, so the truncate is the ordinary allowed
    case. See tests/test_unrolled_truncate_gate.py.
    """
    await conn.execute(
        sa.text(
            "INSERT INTO billing_cost_daily (id, tenant_id, tenant_name, license_id, "
            "service_key, provider, bucket_date, total_input_tokens, "
            "total_output_tokens, total_cost_usd, request_count) "
            "VALUES (gen_random_uuid(), CAST(:t AS uuid), CAST(:t AS varchar), CAST(:lic AS uuid), 'guard-test', 'test', :d, 0, 0, 0, 0)"
        ),
        {"t": tenant, "lic": str(NULL_LICENSE_ID), "d": moment.astimezone(UTC).date()},
    )


async def _arm(conn) -> None:
    await conn.exec_driver_sql(f"SET LOCAL {ALLOW_TRUNCATE_GUC} = '{ALLOW_TRUNCATE_VALUE}'")


# ---------------------------------------------------------------------------
# The two copies of the DDL cannot drift
# ---------------------------------------------------------------------------


def test_migration_ddl_is_byte_identical_to_the_module() -> None:
    """0012 and 0015 exist only because this repo let two schema sources drift.

    The migration duplicates the DDL rather than importing it, so it stays frozen
    in time — which is correct, and is exactly why it needs a test. Same
    discipline 0016 applies to ``delete_audit``.
    """
    mig = _load_migration()
    assert mig.TRUNCATE_GUARD_DDL == tg.TRUNCATE_GUARD_DDL
    assert mig.ALLOW_TRUNCATE_GUC == tg.ALLOW_TRUNCATE_GUC
    assert mig.ALLOW_TRUNCATE_VALUE == tg.ALLOW_TRUNCATE_VALUE


def test_downgrade_restores_0016s_partition_function_exactly() -> None:
    """Removing the guard must leave #92's audit control fully intact.

    ``create_cost_events_partition_for`` is a single function that carries BOTH
    controls. If the downgrade wrote a body it invented, dropping the guard would
    silently also stop new partitions inheriting the AUDIT trigger — a hole in a
    different control, opened by reverting this one, and visible only months
    later on the next partition mint.
    """
    import models.db.delete_audit as da

    mig = _load_migration()
    assert mig._FN_WITH_AUDIT_ONLY == da.PARTITION_FN_WITH_AUDIT_DDL
    assert "billing_delete_audit_trg" in mig._FN_WITH_AUDIT_ONLY
    assert "billing_truncate_guard_trg" not in mig._FN_WITH_AUDIT_ONLY


def test_the_guarded_partition_function_keeps_both_controls() -> None:
    """The guard's body is DERIVED from 0016's, so it cannot drop the audit half."""
    fn = tg.PARTITION_FN_WITH_GUARD_DDL
    assert "billing_truncate_guard_trg" in fn, "guard attachment missing"
    assert "billing_delete_audit_trg" in fn, "0016 DELETE audit attachment was lost"
    assert "billing_truncate_audit_trg" in fn, "0016 TRUNCATE audit attachment was lost"


# ---------------------------------------------------------------------------
# The guard refuses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", ["billing_cost_events", "billing_metering_events"])
async def test_unarmed_truncate_is_refused(db_engine, table: str) -> None:
    """THE DEFECT: an owner could empty a financial ledger with one statement."""
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await conn.execute(sa.text(f"TRUNCATE {table}"))
    assert "blocked" in str(exc.value).lower(), str(exc.value)


async def test_the_refusal_names_the_escape_hatch(db_engine) -> None:
    """An error that does not say what to do next teaches people to work around it.

    The operator who MEANS to truncate must be able to act on the message
    without reading this repo — so the HINT carries the exact statement.
    """
    with pytest.raises(DBAPIError) as exc:
        async with db_engine.begin() as conn:
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))
    message = str(exc.value)
    assert ALLOW_TRUNCATE_GUC in message, message
    assert "billing.delete_reason" in message, "the audit reason GUC is not suggested"


async def test_no_rows_are_lost_when_a_truncate_is_refused(db_engine) -> None:
    """The point of the control: the money record survives the attempt."""
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _arm(conn)
        await conn.execute(sa.text("TRUNCATE billing_cost_events"))
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, datetime.now(UTC))])

    with pytest.raises(DBAPIError):
        async with db_engine.begin() as conn:
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))

    async with db_engine.begin() as conn:
        n = (await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))).scalar()
    assert n == 1, "the refused TRUNCATE still destroyed the row"


@pytest.mark.parametrize("value", ["", "off", "true", "1", "yes", "ON", "On"])
async def test_only_the_exact_value_arms_the_hatch(db_engine, value: str) -> None:
    """A near-miss must refuse.

    If 'true'/'1'/'ON' also armed it, an accidental or generic
    ``SET billing.allow_truncate`` anywhere in a session would silently disarm
    the control. One exact spelling also reads unambiguously in a psql
    transcript during an incident review.
    """
    with pytest.raises(DBAPIError):
        async with db_engine.begin() as conn:
            await conn.exec_driver_sql(f"SET LOCAL {ALLOW_TRUNCATE_GUC} = '{value}'")
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))


# ---------------------------------------------------------------------------
# The guard allows — and does not break 0016
# ---------------------------------------------------------------------------


async def test_an_armed_truncate_succeeds_and_is_still_audited(db_engine) -> None:
    """Both controls on one statement.

    Postgres runs BEFORE triggers first; the guard returns, the truncation
    happens, and 0016's AFTER TRUNCATE trigger writes its row exactly as before.
    A guard that suppressed the audit row would be trading one control for
    another.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _arm(conn)
        await conn.execute(sa.text("TRUNCATE billing_cost_events"))
        now = datetime.now(UTC)
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, now)])
        # alembic 0019 adds a SECOND BEFORE TRUNCATE gate: an armed truncate is
        # still refused while raw events sit newer than max(bucket_date) in
        # billing_cost_daily. This test is about 0017's allow path, so it puts
        # the rollup in the caught-up state rather than arming 0019's override —
        # which would add an audit row and change what this test measures.
        await _rolled_up_through(conn, tenant, now)
        await conn.execute(sa.text("DELETE FROM billing_delete_audit"))

    async with db_engine.begin() as conn:
        await _arm(conn)
        await conn.execute(sa.text("TRUNCATE billing_cost_events"))
        rows = list(
            (
                await conn.execute(
                    sa.text("SELECT table_name, operation FROM billing_delete_audit ORDER BY id")
                )
            ).mappings()
        )
        remaining = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))
        ).scalar()

    assert remaining == 0, "the armed TRUNCATE did not actually truncate"
    assert [r["operation"] for r in rows] == ["TRUNCATE"], rows
    assert rows[0]["table_name"] == "billing_cost_events"


async def test_the_guard_does_not_touch_insert_or_delete(db_engine) -> None:
    """A TRUNCATE trigger fires on TRUNCATE and nothing else.

    Asserted rather than assumed: this migration lands on the table the ingest
    hot path writes to, and 'it only affects TRUNCATE' is the claim that makes
    it safe to apply to a live billing primary.
    """
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, datetime.now(UTC))])
        n = (await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))).scalar()
        assert n == 1
        await conn.execute(
            sa.text("DELETE FROM billing_cost_events WHERE tenant_id = :t"), {"t": tenant}
        )
        n = (await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events"))).scalar()
        assert n == 0


# ---------------------------------------------------------------------------
# Partitions — where the incident actually landed
# ---------------------------------------------------------------------------


@pytest.fixture
async def partitioned_cost_events(db_engine):
    """Rebuild ``billing_cost_events`` as a real partitioned table, armed as 0017 arms it.

    The ORM builds a plain table, so without this the partition assertions would
    be vacuous — passing against a table that has no partitions to be blind to.
    Adapted from ``tests/test_delete_audit_trigger.py``'s fixture of the same
    name; the teardown restores the plain table via ``Table.create``, which fires
    the ``after_create`` listeners and re-arms it for the rest of the session.
    """
    import models.db.delete_audit as da
    from db import Base

    async def _apply(conn, statements) -> None:
        for stmt in statements:
            await conn.exec_driver_sql(stmt.replace("%", "%%"))

    async with db_engine.begin() as conn:
        await conn.exec_driver_sql(
            "ALTER TABLE billing_cost_events RENAME TO billing_cost_events_orm_shape"
        )
        await conn.exec_driver_sql(
            "CREATE TABLE billing_cost_events "
            "(LIKE billing_cost_events_orm_shape INCLUDING DEFAULTS) "
            "PARTITION BY RANGE (created_at)"
        )
        await conn.exec_driver_sql(
            "ALTER TABLE billing_cost_events ADD PRIMARY KEY (id, created_at)"
        )
        await conn.exec_driver_sql("DROP TABLE billing_cost_events_orm_shape CASCADE")
        await conn.exec_driver_sql(
            "CREATE TABLE billing_cost_events_2030_01 PARTITION OF billing_cost_events "
            "FOR VALUES FROM ('2030-01-01') TO ('2030-02-01')"
        )
        # Both controls, in the order models/db/__init__ registers them.
        await _apply(conn, da.DELETE_AUDIT_DDL)
        await _apply(conn, tg.TRUNCATE_GUARD_DDL)

    yield db_engine

    async with db_engine.begin() as conn:
        await conn.exec_driver_sql("DROP TABLE IF EXISTS billing_cost_events CASCADE")
        await conn.run_sync(Base.metadata.tables["billing_cost_events"].create)


async def test_a_truncate_aimed_at_a_child_partition_is_refused(
    partitioned_cost_events,
) -> None:
    """THE INCIDENT'S ACTUAL SHAPE: all 12 partitions were named in the statement.

    A statement trigger on the parent does not fire for a TRUNCATE that names a
    partition, so a parent-only attachment would be blind to exactly the thing
    this control exists for. Remove ``ATTACH_GUARD_TO_EXISTING_PARTITIONS_DDL``
    and this goes red while every parent-only test above stays green.
    """
    engine = partitioned_cost_events
    with pytest.raises(DBAPIError) as exc:
        async with engine.begin() as conn:
            await conn.execute(sa.text("TRUNCATE billing_cost_events_2030_01"))
    assert "blocked" in str(exc.value).lower()


async def test_a_freshly_minted_partition_carries_both_controls(
    partitioned_cost_events,
) -> None:
    """A partition created NEXT MONTH must be guarded and audited, or the control rots.

    The incident emptied 12 partitions. If ``create_cost_events_partition_for``
    minted unguarded ones, the hole would reopen silently, one month at a time.
    This also pins the ordering constraint in ``models/db/__init__``: the guard's
    function body is derived from 0016's, so if the two listeners ran in the
    other order the audit triggers would be missing here.
    """
    engine = partitioned_cost_events
    async with engine.begin() as conn:
        minted = (
            await conn.execute(sa.text("SELECT create_cost_events_partition_for(2031, 3)"))
        ).scalar()
        triggers = sorted(
            r[0]
            for r in (
                await conn.execute(
                    sa.text(
                        "SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                        "WHERE c.relname = :p AND NOT t.tgisinternal"
                    ),
                    {"p": minted},
                )
            ).all()
        )

    assert minted == "billing_cost_events_2031_03"
    # THREE, not four: ``partitioned_cost_events`` above rebuilds the table with
    # 0016's and 0017's DDL only, so the function under test here is 0017's. The
    # 0019 watermark gate adds a fourth layer on the create_all schema and is
    # asserted against its own fixture in tests/test_unrolled_truncate_gate.py.
    assert triggers == [
        "billing_delete_audit_trg",
        "billing_truncate_audit_trg",
        "billing_truncate_guard_trg",
    ], f"minted partition {minted} carries {triggers}"


async def test_an_unarmed_truncate_of_the_parent_spares_every_partition(
    partitioned_cost_events,
) -> None:
    """All-or-nothing. A partial truncation would be the worst outcome of the three."""
    engine = partitioned_cost_events
    tenant = str(uuid.uuid4())
    ts = datetime(2030, 1, 15, 12, 0, tzinfo=UTC)
    async with engine.begin() as conn:
        await _seed(conn, "billing_cost_events", [_cost_event_row(tenant, ts)])

    with pytest.raises(DBAPIError):
        async with engine.begin() as conn:
            await conn.execute(sa.text("TRUNCATE billing_cost_events"))

    async with engine.begin() as conn:
        n = (
            await conn.execute(sa.text("SELECT count(*) FROM billing_cost_events_2030_01"))
        ).scalar()
    assert n == 1


# ---------------------------------------------------------------------------
# The half of the control that lives outside the database
# ---------------------------------------------------------------------------


def test_conftest_refuses_a_production_database() -> None:
    """WITHOUT THIS THE TRIGGER IS VACUOUS AGAINST ITS OWN MOTIVATING INCIDENT.

    ``conftest.db_engine`` must arm the escape hatch or no DB test can run, so
    this suite pointed at the prod DSN would arm it and truncate prod for real.
    That is this refusal's justification — NOT that it caused the 2026-09-05
    incident, which it did not: that statement carried no CASCADE and named a
    table set ``sorted_tables`` cannot produce (see
    ``tests/_production_tripwire.py``). The guard and this refusal are one
    control in two halves; neither is sufficient.
    """
    tripwire = _tripwire()

    for name in ("billing", "billing_prod", "metering"):
        url = f"postgresql+psycopg://u:p@postgres.example.com:5432/{name}"
        assert tripwire.database_name(url) == name
        assert name in tripwire.PRODUCTION_DB_NAMES
        with pytest.raises(RuntimeError, match="REFUSING TO RUN"):
            tripwire.refuse_production_database(url)

    # And the URL this very run is actually using must not be one of them.
    # conftest exports the resolved URL into DATABASE_URL, so this reads the
    # real value rather than re-deriving the default and checking a second copy.
    import os  # noqa: PLC0415

    live_url = os.environ["DATABASE_URL"]
    assert tripwire.database_name(live_url) not in tripwire.PRODUCTION_DB_NAMES, (
        f"this run is pointed at a production database: {live_url}"
    )
    tripwire.refuse_production_database(live_url)


def test_the_production_denylist_does_not_reject_the_ci_or_local_databases() -> None:
    """A guard that blocks legitimate runs gets deleted, not fixed.

    CI uses ``<image>_ci``; the local container is ``metering_test``. Both must
    pass, or the pressure to relax the denylist lands on the control itself.
    """
    tripwire = _tripwire()

    for name in ("metering_test", "billing_service_ci", "billing_ci", "metering_ci"):
        assert name not in tripwire.PRODUCTION_DB_NAMES, name
        # Must not raise.
        tripwire.refuse_production_database(f"postgresql+psycopg://u:p@127.0.0.1:5502/{name}")


# ---------------------------------------------------------------------------
# billing-service#238 -- alembic 0043 re-attaches the guard to tables that
# were born AFTER 0018's one-time catalog sweep: billing_cost_actor_daily
# (0040) and billing_attribution_rules (0041).
#
# ⚠️ These cannot be added to ``test_unarmed_truncate_is_refused`` above.
# ``db_engine``'s schema is ``Base.metadata.create_all`` plus the narrow
# per-module ``after_create`` listeners registered in ``models/db/__init__``
# (``truncate_guard.py`` arms only ``billing_cost_events`` /
# ``billing_metering_events`` / partitions -- 0017's original scope). Neither
# 0018's nor 0043's catalog-wide sweep ever runs against that schema, so these
# two tables would read as unguarded there even with 0043 correctly applied,
# and as guarded if 0043 were deleted entirely reading the wrong schema proves
# nothing either way. Same caveat as the 0021/0022 trigger-survival tests:
# provable only by building the schema from the REAL migration chain.
# ---------------------------------------------------------------------------

_REATTACH_THROWAWAY_DB = throwaway_db_name("_billing_0043_reattach_check")
_REATTACH_TABLES = ("billing_attribution_rules", "billing_cost_actor_daily")


def _reattach_cfg() -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    return cfg


def _reattach_server_url(dsn: str) -> sa.engine.URL:
    return sa.make_url(dsn).set(database="postgres")


def _reattach_recreate(server: sa.engine.URL, name: str) -> sa.engine.URL:
    eng = sa.create_engine(server, isolation_level="AUTOCOMMIT", future=True)
    with eng.connect() as conn:
        conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    eng.dispose()
    return server.set(database=name)


def _reattach_upgrade_head_on(url: sa.engine.URL) -> None:
    cfg = _reattach_cfg()
    cfg.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))
    os.environ["DATABASE_URL"] = url.render_as_string(hide_password=False)
    command.upgrade(cfg, "head")


#: Same predicate as 0018's/0043's ATTACH-IF-ABSENT sweep, as a plain SELECT
#: instead of a DO block: any row returned here is a public base table the
#: sweep should have armed but did not.
_UNGUARDED_ELIGIBLE_TABLES_SQL = """
SELECT c.relname
  FROM pg_class c
  JOIN pg_namespace ns ON ns.oid = c.relnamespace
 WHERE ns.nspname = 'public'
   AND c.relkind IN ('r', 'p')
   AND c.relpersistence = 'p'
   AND c.relname <> 'alembic_version'
   AND NOT EXISTS (
           SELECT 1
             FROM pg_depend d
            WHERE d.classid = to_regclass('pg_class')
              AND d.objid = c.oid
              AND d.deptype = 'e')
   AND NOT EXISTS (
           SELECT 1
             FROM pg_trigger t
            WHERE t.tgrelid = c.oid
              AND t.tgname = 'billing_truncate_guard_trg')
 ORDER BY c.relname
"""


class TestTruncateGuardReattach0043:
    """billing-service#238, proved against the real chain, not ``db_engine``."""

    @pytest.mark.integration
    async def test_unarmed_truncate_is_refused_for_tables_born_after_0018(self) -> None:
        """THE GAP #238 CLOSES: these two tables were created after 0018 ran."""
        dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
        assert dsn, "TEST_DATABASE_URL/DATABASE_URL must be set"
        server = _reattach_server_url(dsn)
        db_url = _reattach_recreate(server, _REATTACH_THROWAWAY_DB)

        saved_database_url = os.environ.get("DATABASE_URL")
        try:
            await asyncio.to_thread(_reattach_upgrade_head_on, db_url)

            eng = sa.create_engine(db_url, future=True)
            try:
                for table in _REATTACH_TABLES:
                    with pytest.raises(DBAPIError) as exc, eng.begin() as conn:
                        conn.execute(sa.text(f"TRUNCATE {table}"))
                    assert "blocked" in str(exc.value).lower(), (table, str(exc.value))
            finally:
                eng.dispose()
        finally:
            if saved_database_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = saved_database_url

    @pytest.mark.integration
    async def test_every_eligible_public_table_has_the_guard_trigger(self) -> None:
        """The catalog-wide assertion #238 was missing: no table slips through.

        Re-derives 0018's/0043's own ATTACH-IF-ABSENT predicate and asserts it
        selects nothing once the real chain has run to head -- i.e. every
        public base table eligible for the guard actually carries it,
        ``billing_attribution_rules`` and ``billing_cost_actor_daily``
        included. A future table born after a later migration and never
        reattached would show up here by name, not just by omission.
        """
        dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
        assert dsn, "TEST_DATABASE_URL/DATABASE_URL must be set"
        server = _reattach_server_url(dsn)
        db_url = _reattach_recreate(server, f"{_REATTACH_THROWAWAY_DB}_catalog")

        saved_database_url = os.environ.get("DATABASE_URL")
        try:
            await asyncio.to_thread(_reattach_upgrade_head_on, db_url)

            eng = sa.create_engine(db_url, future=True)
            try:
                with eng.connect() as conn:
                    unguarded = [
                        r[0] for r in conn.execute(sa.text(_UNGUARDED_ELIGIBLE_TABLES_SQL))
                    ]
                assert unguarded == [], f"eligible public tables missing the guard: {unguarded}"

                # And the two #238 tables are positively present and enabled,
                # not merely absent from the unguarded list above.
                with eng.connect() as conn:
                    for table in _REATTACH_TABLES:
                        row = conn.execute(
                            sa.text(
                                "SELECT t.tgenabled FROM pg_trigger t "
                                "JOIN pg_class c ON c.oid = t.tgrelid "
                                "WHERE c.relname = :rel AND t.tgname = 'billing_truncate_guard_trg'"
                            ),
                            {"rel": table},
                        ).first()
                        assert row is not None, f"billing_truncate_guard_trg missing on {table}"
                        assert row[0] == "O", (
                            f"billing_truncate_guard_trg on {table} is not enabled "
                            f"(tgenabled={row[0]!r})"
                        )
            finally:
                eng.dispose()
        finally:
            if saved_database_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = saved_database_url
