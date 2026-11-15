"""scripts/tenant_purge.py against a REAL alembic-``head`` schema.

⛔ Not ``db_engine``: that schema is ``Base.metadata.create_all``, which never
partitions ``billing_cost_events`` and so cannot show what the delete-audit
trigger records for a DELETE through the partitioned parent — the exact path
this script takes in production. The full chain is built ONCE into a template
database; every test gets a fresh copy (``CREATE DATABASE … TEMPLATE``), so no
test needs a TRUNCATE hatch to clean up after another.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from psycopg import sql
from psycopg.types.json import Jsonb

from tests.conftest import throwaway_db_name

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "tenant_purge.py"

_spec = importlib.util.spec_from_file_location("_tenant_purge", SCRIPT)
assert _spec is not None and _spec.loader is not None
tp = importlib.util.module_from_spec(_spec)
# Registered before exec: @dataclass resolves annotations via sys.modules.
sys.modules["_tenant_purge"] = tp
_spec.loader.exec_module(tp)

CUTOFF = "2026-09-20T00:00:00Z"
REASON = "test purge: TENANCY Q4 rehearsal"
ACTOR = "purge-test"

BEFORE_TS = datetime(2026, 9, 18, 12, tzinfo=UTC)
AFTER_TS = datetime(2026, 9, 25, 12, tzinfo=UTC)

#: (before-cutoff value, at/after-cutoff value) per ts_kind. Day/hour/month
#: "after" values straddle or follow the cutoff, so they must be kept.
_TS_VALUES = {
    "instant": (BEFORE_TS, AFTER_TS),
    "hour": (datetime(2026, 9, 19, 23, tzinfo=UTC), datetime(2026, 9, 20, 0, tzinfo=UTC)),
    "day": (date(2026, 9, 19), date(2026, 9, 20)),
    "month": (date(2026, 8, 1), date(2026, 9, 1)),
    # August ends 09-01 <= cutoff (deleted); September straddles 09-20 (kept).
    "month_start": (date(2026, 8, 1), date(2026, 9, 1)),
}

TENANT_A = str(uuid.UUID("aaaaaaaa-0000-4000-8000-000000000001"))
TENANT_B = str(uuid.UUID("bbbbbbbb-0000-4000-8000-000000000002"))
TENANT_C = str(uuid.UUID("cccccccc-0000-4000-8000-000000000003"))
TENANTS = (TENANT_A, TENANT_B, TENANT_C)
#: One licence per tenant, for the tenant-less billing_metering_monthly_usage.
LICENSE_OF = {t: str(uuid.uuid5(uuid.NAMESPACE_URL, f"licence/{t}")) for t in TENANTS}
TENANTED = tuple(t for t in tp.ALLOWLIST if t.tenant_col is not None)
UNTENANTED = tuple(t for t in tp.ALLOWLIST if t.tenant_col is None)
MONTHLY = "billing_metering_monthly_usage"

_TEMPLATE = throwaway_db_name("_billing_purge_tmpl")


def _server_url() -> sa.engine.URL:
    dsn = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    assert dsn, "TEST_DATABASE_URL/DATABASE_URL must be set"
    return sa.make_url(dsn).set(database="postgres")


def _admin(stmt: str) -> None:
    engine = sa.create_engine(_server_url(), isolation_level="AUTOCOMMIT", future=True)
    with engine.connect() as conn:
        conn.execute(sa.text(stmt))
    engine.dispose()


@pytest.fixture(scope="module")
def template_db() -> Iterator[str]:
    _admin(f'DROP DATABASE IF EXISTS "{_TEMPLATE}" WITH (FORCE)')
    _admin(f'CREATE DATABASE "{_TEMPLATE}"')
    rendered = _server_url().set(database=_TEMPLATE).render_as_string(hide_password=False)
    saved = os.environ.get("DATABASE_URL")
    try:
        cfg = Config(str(REPO / "alembic.ini"))
        cfg.set_main_option("script_location", str(REPO / "alembic"))
        cfg.set_main_option("sqlalchemy.url", rendered)
        os.environ["DATABASE_URL"] = rendered
        command.upgrade(cfg, "head")
        yield _TEMPLATE
    finally:
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
        _admin(f'DROP DATABASE IF EXISTS "{_TEMPLATE}" WITH (FORCE)')


@pytest.fixture
def db(template_db, monkeypatch) -> Iterator[str]:
    """A fresh, seeded copy of the alembic-head template; returns a libpq DSN."""
    name = f"{_TEMPLATE}_{uuid.uuid4().hex[:8]}"
    _admin(f'CREATE DATABASE "{name}" TEMPLATE "{template_db}"')
    url = _server_url().set(database=name, drivername="postgresql")
    dsn = url.render_as_string(hide_password=False)
    with psycopg.connect(dsn) as conn:
        _seed(conn)
    monkeypatch.setenv("BILLING_PURGE_DSN", dsn)
    yield dsn
    _admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


# --------------------------------------------------------------------------- #
# Seeding — generic over NOT NULL columns so the test tracks the real schema
# --------------------------------------------------------------------------- #


def _fill(data_type: str):  # noqa: ANN202
    if data_type == "uuid":
        return str(uuid.uuid4())
    if data_type in ("integer", "bigint", "smallint", "numeric", "double precision", "real"):
        return 0
    if data_type == "boolean":
        return False
    if data_type in ("jsonb", "json"):
        return Jsonb({})
    if data_type == "date":
        return date(2026, 9, 18)
    if data_type.startswith("timestamp"):
        return BEFORE_TS
    if data_type == "ARRAY":
        return []
    return "x"


def _insert(conn: psycopg.Connection, table: str, values: dict) -> None:
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = %s "
        "AND is_nullable = 'NO' AND column_default IS NULL",
        (table,),
    ).fetchall()
    row = {c: _fill(t) for c, t in rows}
    row.update(values)
    conn.execute(
        sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in row),
            sql.SQL(", ").join(sql.Placeholder() for _ in row),
        ),
        list(row.values()),
    )


def _seed(conn: psycopg.Connection) -> None:
    """Per tenant, per allowlisted table: one row before the cutoff, one kept."""
    for t in tp.ALLOWLIST:
        before, after = _TS_VALUES[t.ts_kind]
        for tenant in TENANTS:
            for ts in (before, after):
                extra = {"period_type": "monthly"} if t.ts_kind == "month" else {}
                if t.name == "request_quota_config":
                    extra = {"status": "active", "quota_monthly": 10}
                # No tenant column: the row is the tenant's licence's.
                key = t.tenant_col or "license_id"
                owner = tenant if t.tenant_col else LICENSE_OF[tenant]
                _insert(conn, t.name, {key: owner, t.ts_col: ts, **extra})
    for tenant in TENANTS:
        _invoice(conn, tenant, "issued", date(2026, 8, 1))
        _invoice(conn, tenant, "draft", date(2026, 9, 1))
        _invoice(conn, tenant, "issued", date(2026, 9, 21))  # period after cutoff: kept
    conn.commit()


def _invoice(conn: psycopg.Connection, tenant: str, status: str, start: date) -> None:
    values = {
        "tenant_id": tenant,
        "status": status,
        "period_start": start,
        "period_end": date(start.year + (start.month == 12), start.month % 12 + 1, 1),
    }
    if status == "issued":
        values |= {"invoice_number": f"INV-{uuid.uuid4().hex[:8]}", "issued_at": BEFORE_TS}
    _insert(conn, "billing_invoices", values)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _scalar(conn: psycopg.Connection, query, params=None):  # noqa: ANN001, ANN202
    row = conn.execute(query, params).fetchone()
    assert row is not None
    return row[0]


def _counts(dsn: str) -> dict:
    out = {}
    with psycopg.connect(dsn) as conn:
        for t in UNTENANTED:
            out[(t.name, "*")] = _scalar(
                conn, sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(t.name))
            )
        for t in TENANTED:
            for tenant in TENANTS:
                out[(t.name, tenant)] = _scalar(
                    conn,
                    sql.SQL("SELECT count(*) FROM {} WHERE {} = %s").format(
                        sql.Identifier(t.name), sql.Identifier(t.tenant_col)
                    ),
                    (tenant,),
                )
        out["billing_delete_audit"] = _scalar(conn, "SELECT count(*) FROM billing_delete_audit")
        out["invoices"] = conn.execute(
            "SELECT id::text, status FROM billing_invoices ORDER BY id"
        ).fetchall()
        out["audit_log_total"] = _scalar(conn, "SELECT count(*) FROM audit_log")
    return out


def _guards(dsn: str) -> list:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT c.relname, t.tgname, t.tgenabled, md5(p.prosrc) "
            "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_proc p ON p.oid = t.tgfoid WHERE NOT t.tgisinternal ORDER BY 1, 2"
        ).fetchall()


def _run(tmp_path: Path, *extra: str, receipt: str = "r.json") -> tuple[int, dict]:
    path = tmp_path / receipt
    code = tp.main(
        ["--cutoff", CUTOFF, "--reason", REASON, "--actor", ACTOR, "--receipt", str(path), *extra]
    )
    return code, (json.loads(path.read_text()) if path.exists() else {})


ALL = ("--all-tenants", "--include-config", "--void-invoices")


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


def test_dry_run_writes_nothing(db, tmp_path):
    before, guards = _counts(db), _guards(db)
    code, receipt = _run(tmp_path, *ALL)
    assert code == 0, receipt
    assert _counts(db) == before
    assert _guards(db) == guards
    assert receipt["dry_run"] is True
    assert receipt["outcome"] == "dry_run_rolled_back"
    assert receipt["transaction_read_only"] == "on"
    assert receipt["billing_delete_audit"]["delta"] == 0
    assert receipt["invoices"]["voided"] == []
    assert len(receipt["invoices"]["candidates"]) == 6
    for entry in receipt["tables"]:
        assert entry["eligible"] == 3, entry
        assert entry["deleted"] == 0
        assert entry["untouched_after_cutoff"] == 3
    assert sorted(receipt["resolved_scope"]["with_eligible_rows"]) == sorted(TENANTS)
    assert "token_budgets_absent" in receipt["notes"]


def test_execute_deletes_only_rows_before_cutoff(db, tmp_path):
    code, receipt = _run(tmp_path, "--execute", *ALL)
    assert code == 0, receipt
    assert receipt["outcome"] == "committed"
    after = _counts(db)
    assert after[(MONTHLY, "*")] == 3  # the straddling September row of each licence
    for t in TENANTED:
        for tenant in TENANTS:
            # One seeded "kept" row per tenant; audit_log additionally gains the
            # void audit rows written now (ts > cutoff), 2 per tenant.
            expected = 1 + (2 if t.name == "audit_log" else 0)
            assert after[(t.name, tenant)] == expected, (t.name, tenant)
    with psycopg.connect(db) as conn:
        for t in tp.ALLOWLIST:
            elig = _scalar(
                conn,
                sql.SQL("SELECT count(*) FROM {} WHERE ").format(sql.Identifier(t.name))
                + tp._eligible(t),
                {"cutoff": datetime(2026, 9, 20, tzinfo=UTC)},
            )
            assert elig == 0, t.name
    for entry in receipt["tables"]:
        assert entry["deleted"] == 3
        # audit_log's kept set includes the 6 void rows written earlier in the txn.
        kept = 9 if entry["table"] == "audit_log" else 3
        assert entry["untouched_after_cutoff_before"] == entry["untouched_after_cutoff"] == kept
        assert entry["eligible_remaining"] == 0


def test_tenant_scope_leaves_other_tenants_intact(db, tmp_path):
    # --tenants cannot scope billing_metering_monthly_usage (no tenant column), so
    # it only proceeds when that table holds nothing before the cutoff.
    with psycopg.connect(db) as conn:
        conn.execute(f"DELETE FROM {MONTHLY} WHERE month_start < '2026-09-01'")
    before = _counts(db)
    code, receipt = _run(tmp_path, "--execute", "--tenants", TENANT_A, "--include-config")
    assert code == 0, receipt
    after = _counts(db)
    assert after[(MONTHLY, "*")] == before[(MONTHLY, "*")] == 3
    monthly = next(e for e in receipt["tables"] if e["table"] == MONTHLY)
    assert monthly["before"] == monthly["deleted"] == 0
    assert "unresolvable" in monthly["tenant_scope"]
    for t in TENANTED:
        assert after[(t.name, TENANT_A)] == 1, t.name
        for other in (TENANT_B, TENANT_C):
            assert after[(t.name, other)] == before[(t.name, other)] == 2, (t.name, other)
    assert receipt["resolved_scope"]["with_eligible_rows"] == [TENANT_A]
    assert after["invoices"] == before["invoices"]  # --void-invoices not given


def test_config_tables_only_with_include_config(db, tmp_path):
    code, receipt = _run(tmp_path, "--execute", "--all-tenants")
    assert code == 0, receipt
    after = _counts(db)
    assert after[(MONTHLY, "*")] == 3
    for t in TENANTED:
        for tenant in TENANTS:
            assert after[(t.name, tenant)] == (2 if t.config else 1), t.name
    assert {e["table"] for e in receipt["tables"]} == {t.name for t in tp.ALLOWLIST if not t.config}


def test_delete_audit_gains_rows_with_reason(db, tmp_path):
    before = _counts(db)["billing_delete_audit"]
    code, receipt = _run(tmp_path, "--execute", *ALL)
    assert code == 0, receipt
    with psycopg.connect(db) as conn:
        rows = conn.execute(
            "SELECT table_name, rows_deleted, reason, application_name "
            "FROM billing_delete_audit ORDER BY id"
        ).fetchall()
    assert len(rows) - before == receipt["billing_delete_audit"]["delta"] > 0
    assert all(REASON in r[2] for r in rows)
    # 0016's rows carry the reason verbatim; 0029 (audit_log) wraps it.
    assert {r[2] for r in rows if r[0] != "audit_log"} == {REASON}
    assert {r[3] for r in rows} == {tp.APPLICATION_NAME}
    tables = {r[0] for r in rows}
    assert "audit_log" in tables
    assert "billing_metering_events" in tables
    assert any(t.startswith("billing_cost_events") for t in tables)
    by_name = {e["table"]: e for e in receipt["tables"]}
    # #244: the two config tables are audited since alembic 0046.
    assert {"token_budgets", "request_quota_config"} <= tables
    for name in (
        "billing_cost_events",
        "billing_metering_events",
        "audit_log",
        "token_budgets",
        "request_quota_config",
    ):
        assert by_name[name]["delete_audited"] is True
        assert "unaudited" not in by_name[name]
    for name in ("billing_cost_daily", "token_usage", MONTHLY):
        assert by_name[name]["unaudited"] == "counts are the only record"


def test_active_token_budget_is_never_purged(db, tmp_path):
    """#244: --include-config deleted netcare's ACTIVE budget on 2026-09-30.

    An active row created before the cutoff must survive and be reported; the
    seeded inactive rows before the cutoff are still deleted, and audited.
    """
    active_id = str(uuid.uuid4())
    with psycopg.connect(db) as conn:
        _insert(
            conn,
            "token_budgets",
            {"id": active_id, "tenant_id": TENANT_A, "created_at": BEFORE_TS, "active": True},
        )
    code, receipt = _run(tmp_path, "--execute", "--all-tenants", "--include-config")
    assert code == 0, receipt
    entry = next(e for e in receipt["tables"] if e["table"] == "token_budgets")
    assert entry["active_skipped"] == 1
    assert entry["eligible"] == entry["deleted"] == 3  # the seeded inactive rows
    assert entry["delete_audited"] is True
    with psycopg.connect(db) as conn:
        survivors = conn.execute(
            "SELECT id::text, active FROM token_budgets WHERE created_at < %s",
            (datetime(2026, 9, 20, tzinfo=UTC),),
        ).fetchall()
        audit = conn.execute(
            "SELECT rows_deleted, operation, reason FROM billing_delete_audit "
            "WHERE table_name = 'token_budgets'"
        ).fetchall()
    assert survivors == [(active_id, True)]
    assert audit == [(3, "DELETE", REASON)]


def test_failure_mid_transaction_rolls_back_everything(db, tmp_path, monkeypatch):
    before, guards = _counts(db), _guards(db)
    real = tp._audited

    def broken(cur, table):  # noqa: ANN001, ANN202
        # audit_log is purged LAST, after every other table's DELETE ran: a real
        # server-side error at that point, inside the same transaction.
        if table == "audit_log":
            cur.execute("SELECT 1 / 0")
        return real(cur, table)

    monkeypatch.setattr(tp, "_audited", broken)
    code, receipt = _run(tmp_path, "--execute", *ALL)
    assert code == 1
    assert receipt["outcome"] == "error_rolled_back"
    # Earlier tables DID delete inside the transaction before the failure …
    assert any(e["deleted"] == 3 for e in receipt["tables"])
    # … and none of it survived, voids included.
    assert _counts(db) == before
    assert _guards(db) == guards


def test_count_mismatch_aborts_and_rolls_back(db, tmp_path, monkeypatch):
    before = _counts(db)
    real = tp._count
    calls = {"n": 0}

    def lying(cur, t, where, params):  # noqa: ANN001, ANN202
        calls["n"] += 1
        n = real(cur, t, where, params)
        # Inflate billing_metering_daily's "eligible" count so deleted != eligible.
        if t.name == "billing_metering_daily" and " AND " in where.as_string(cur):
            return n + 1
        return n

    monkeypatch.setattr(tp, "_count", lying)
    code, receipt = _run(tmp_path, "--execute", "--all-tenants")
    assert code == 3
    assert receipt["outcome"] == "aborted_rolled_back"
    assert "billing_metering_daily" in receipt["error"]
    assert _counts(db) == before


def test_unknown_tenant_table_aborts_before_touching_anything(db, tmp_path):
    with psycopg.connect(db) as conn:
        conn.execute("CREATE TABLE rogue_usage (id serial, bill_tenant_id uuid)")
    before = _counts(db)
    code, receipt = _run(tmp_path, "--execute", *ALL)
    assert code == 3
    assert "rogue_usage" in receipt["error"]
    assert _counts(db) == before


def test_pgbouncer_dsn_is_refused(tmp_path, monkeypatch):
    for dsn in (
        "postgresql://u:p@pgbouncer.pgbouncer.svc.cluster.local:6432/billing",
        "postgresql+psycopg://u:p@PGBouncer.local:5432/billing",
        "host=pgbouncer port=5432 dbname=billing",
        "postgresql://u:p@10.0.0.1:6432/billing",
    ):
        monkeypatch.setenv("BILLING_PURGE_DSN", dsn)
        code, receipt = _run(tmp_path, "--all-tenants")
        assert code == 2, dsn
        assert receipt == {}  # refused before connecting; no receipt written
    assert tp.check_dsn("postgresql+psycopg://u:p@127.0.0.1:5432/x").startswith("postgresql://")


def test_argument_refusals(db, tmp_path):
    with pytest.raises(SystemExit):
        tp.main(
            [
                "--cutoff",
                CUTOFF,
                "--reason",
                " ",
                "--actor",
                ACTOR,
                "--all-tenants",
                "--receipt",
                str(tmp_path / "x.json"),
            ]
        )
    with pytest.raises(SystemExit):  # both scopes
        tp.main(
            [
                "--cutoff",
                CUTOFF,
                "--reason",
                REASON,
                "--actor",
                ACTOR,
                "--all-tenants",
                "--tenants",
                TENANT_A,
                "--receipt",
                str(tmp_path / "x.json"),
            ]
        )
    with pytest.raises(SystemExit):  # naive timestamp
        tp.main(
            [
                "--cutoff",
                "2026-09-20T00:00:00",
                "--reason",
                REASON,
                "--actor",
                ACTOR,
                "--all-tenants",
                "--receipt",
                str(tmp_path / "x.json"),
            ]
        )
    assert (
        tp.main(
            [
                "--cutoff",
                "2999-01-01T00:00:00Z",
                "--reason",
                REASON,
                "--actor",
                ACTOR,
                "--all-tenants",
                "--receipt",
                str(tmp_path / "x.json"),
            ]
        )
        == 2
    )
    (tmp_path / "exists.json").write_text("{}")
    assert _run(tmp_path, "--all-tenants", receipt="exists.json")[0] == 2


def test_invoices_are_voided_not_deleted_and_guards_untouched(db, tmp_path):
    before, guards = _counts(db), _guards(db)
    code, receipt = _run(tmp_path, "--execute", *ALL)
    assert code == 0, receipt
    assert _guards(db) == guards
    with psycopg.connect(db) as conn:
        rows = conn.execute(
            "SELECT id::text, status, void_reason, voided_by, voided_at, period_start "
            "FROM billing_invoices ORDER BY id"
        ).fetchall()
        void_audit = _scalar(
            conn,
            "SELECT count(*) FROM audit_log WHERE action = 'billing.invoice.void' "
            "AND actor_sub = %s",
            (ACTOR,),
        )
    assert len(rows) == len(before["invoices"]) == 9  # nothing deleted
    voided = [r for r in rows if r[1] == "void"]
    assert len(voided) == 6 == len(receipt["invoices"]["voided"])
    for _id, _status, reason, by, at, start in voided:
        assert reason == REASON and by == ACTOR and at is not None
        assert start < date(2026, 9, 20)
    kept = [r for r in rows if r[1] != "void"]
    assert len(kept) == 3 and all(r[5] >= date(2026, 9, 20) for r in kept)
    assert void_audit == 6
    with psycopg.connect(db) as conn:
        audit_rows = conn.execute(
            "SELECT request_id, actor_role, resource_type, resource_id, tenant_id::text, after "
            "FROM audit_log WHERE action = 'billing.invoice.void'"
        ).fetchall()
    assert {r[0] for r in audit_rows} == {receipt["run_id"]}  # the route's request_id slot
    assert {r[1] for r in audit_rows} == {tp.APPLICATION_NAME}
    by_id = {v[0]: v for v in voided}
    for _rid, _role, rtype, rid, tenant, after_json in audit_rows:
        assert rtype == "invoice" and rid in by_id and tenant in TENANTS
        assert after_json == {"status": "void", "void_reason": REASON}
    # A second run finds nothing left to void (void is terminal; not re-selected).
    code, receipt2 = _run(tmp_path, "--execute", *ALL, receipt="r2.json")
    assert code == 0, receipt2
    assert receipt2["invoices"]["voided"] == []
    assert all(e["deleted"] == 0 for e in receipt2["tables"])


def test_void_invoices_reverses_the_prepaid_credit_drawdown(db, tmp_path):
    """A purge-void gives back the credit the voided statement drew, like void() does.

    #267 re-audit: the raw-SQL void wrote no ``drawdown_reversal``, so the paid
    credit a voided statement had spent stayed spent.
    """
    with psycopg.connect(db) as conn:
        row = conn.execute(
            "SELECT id::text, tenant_id::text FROM billing_invoices "
            "WHERE status <> 'void' AND period_start < '2026-09-20' ORDER BY id LIMIT 1"
        ).fetchone()
        assert row is not None
        inv_id, tenant = row
        conn.execute(
            "INSERT INTO tenant_credit_ledger (id, tenant_id, kind, entry_key, amount_usd, "
            "source_amount, source_currency, fx_rate) VALUES "
            "(gen_random_uuid(), %s, 'topup', 'topup:purge-test-key', 100, 100, 'USD', 1)",
            (tenant,),
        )
        conn.execute(
            "INSERT INTO tenant_credit_ledger (id, tenant_id, kind, entry_key, amount_usd, "
            "period_start, invoice_id) VALUES "
            "(gen_random_uuid(), %s, 'usage_drawdown', 'usage:' || %s, 30, '2026-08-01', %s)",
            (tenant, inv_id, inv_id),
        )
    code, receipt = _run(tmp_path, "--execute", *ALL)
    assert code == 0, receipt
    assert inv_id in {v["id"] for v in receipt["invoices"]["voided"]}
    with psycopg.connect(db) as conn:
        rows = conn.execute(
            "SELECT kind, amount_usd, invoice_id::text FROM tenant_credit_ledger "
            "WHERE tenant_id = %s AND kind = 'drawdown_reversal'",
            (tenant,),
        ).fetchall()
    assert rows == [("drawdown_reversal", Decimal("30.00000000"), inv_id)]


def test_purge_void_serialises_with_a_close_on_the_credit_lock(db, tmp_path, monkeypatch):
    """A purge-void WAITS for a close holding the tenant's credit lock; balance stays >= 0.

    The holder below is exactly what ``close_period`` holds while it reads and
    freezes credit. With ``lock_timeout`` set, a purge that takes the lock fails
    and rolls back (nothing voided, nothing reversed). A purge that skipped the
    lock would void under the close's feet, and this test would go red.
    """
    from services import prepaid_credit

    with psycopg.connect(db) as conn:
        row = conn.execute(
            "SELECT id::text, tenant_id::text FROM billing_invoices "
            "WHERE status <> 'void' AND period_start < '2026-09-20' ORDER BY id LIMIT 1"
        ).fetchone()
        assert row is not None
        inv_id, tenant = row
        conn.execute(
            "INSERT INTO tenant_credit_ledger (id, tenant_id, kind, entry_key, amount_usd, "
            "source_amount, source_currency, fx_rate) VALUES "
            "(gen_random_uuid(), %s, 'topup', 'topup:purge-lock-key', 100, 100, 'USD', 1)",
            (tenant,),
        )
        conn.execute(
            "INSERT INTO tenant_credit_ledger (id, tenant_id, kind, entry_key, amount_usd, "
            "period_start, invoice_id) VALUES "
            "(gen_random_uuid(), %s, 'usage_drawdown', 'usage:' || %s, 30, '2026-08-01', %s)",
            (tenant, inv_id, inv_id),
        )

    def _balance() -> Decimal:
        with psycopg.connect(db) as conn:
            return _scalar(
                conn,
                "SELECT coalesce(sum(CASE WHEN kind = 'usage_drawdown' THEN -amount_usd "
                "ELSE amount_usd END), 0) FROM tenant_credit_ledger WHERE tenant_id = %s",
                (tenant,),
            )

    holder = psycopg.connect(db)  # a close in progress: lock held, transaction open
    try:
        holder.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"{prepaid_credit.CREDIT_LOCK_PREFIX}{tenant}",),
        )
        monkeypatch.setenv(
            "PGOPTIONS", f"{os.environ.get('PGOPTIONS', '')} -c lock_timeout=300".strip()
        )
        code, receipt = _run(tmp_path, "--execute", *ALL)
        assert code == tp.EXIT_ERROR, receipt
        assert "lock" in receipt["error"].lower(), receipt["error"]
        with psycopg.connect(db) as conn:
            status = _scalar(conn, "SELECT status FROM billing_invoices WHERE id = %s", (inv_id,))
        assert status != "void"  # rolled back, not voided under the close
        assert _balance() == Decimal("70")
    finally:
        holder.rollback()
        holder.close()

    code, receipt = _run(tmp_path, "--execute", *ALL, receipt="r2.json")
    assert code == 0, receipt
    assert _balance() == Decimal("100") >= 0  # the drawdown was reversed, once


def test_purge_void_uses_the_same_credit_lock_as_the_service():
    from services import prepaid_credit

    assert tp.CREDIT_LOCK_PREFIX == prepaid_credit.CREDIT_LOCK_PREFIX


def test_script_emits_no_guard_bypass_or_ddl():
    """No SQL string in the script may TRUNCATE/DROP/ALTER, disable a trigger, or arm a hatch.

    Docstrings are exempt (they explain what is never done); every other string
    constant — which is where SQL lives — is scanned.
    """
    import ast
    import re

    tree = ast.parse(SCRIPT.read_text())
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    forbidden = re.compile(
        r"\b(TRUNCATE|DROP|ALTER|DISABLE\s+TRIGGER|session_replication_role)\b"
        r"|allow_(unrolled_)?(metering_)?truncate",
        re.IGNORECASE,
    )
    hits = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and forbidden.search(node.value)
    ]
    assert hits == []


# --------------------------------------------------------------------------- #
# Licence/subject-keyed tables (no tenant column)
# --------------------------------------------------------------------------- #


def test_license_only_table_aborts_unless_classified(db, tmp_path, monkeypatch):
    """The completeness guard must see a table keyed ONLY by license_id."""
    with psycopg.connect(db) as conn:
        conn.execute("CREATE TABLE rogue_licence_usage (id serial, license_id uuid, n int)")
    before = _counts(db)
    code, receipt = _run(tmp_path, "--execute", *ALL)
    assert code == 3
    assert "rogue_licence_usage" in receipt["error"]
    assert _counts(db) == before
    # A prefixed spelling is caught too.
    with psycopg.connect(db) as conn:
        conn.execute("CREATE TABLE rogue_subject_usage (id serial, source_subject_id uuid)")
    code, receipt = _run(tmp_path, "--all-tenants", receipt="r2.json")
    assert code == 3
    assert "rogue_subject_usage" in receipt["error"]
    # Classifying both lets the run proceed; neither is touched.
    monkeypatch.setitem(tp.EXCLUDED, "rogue_licence_usage", "test: classified")
    monkeypatch.setitem(tp.EXCLUDED, "rogue_subject_usage", "test: classified")
    code, receipt = _run(tmp_path, "--all-tenants", receipt="r3.json")
    assert code == 0, receipt
    assert "rogue_licence_usage" in receipt["identity_keyed_tables_seen"]
    assert MONTHLY in receipt["identity_keyed_tables_seen"]


def test_monthly_usage_purged_by_period_straddling_month_kept(db, tmp_path):
    with psycopg.connect(db) as conn:
        # A month ending EXACTLY at a cutoff of 2026-09-01T00:00Z is eligible;
        # add an older one and one after the cutoff to pin both edges.
        for tenant in TENANTS:
            _insert(
                conn, MONTHLY, {"license_id": LICENSE_OF[tenant], "month_start": date(2026, 7, 1)}
            )
            _insert(
                conn, MONTHLY, {"license_id": LICENSE_OF[tenant], "month_start": date(2026, 10, 1)}
            )
    code, receipt = _run(tmp_path, "--execute", "--all-tenants")
    assert code == 0, receipt
    with psycopg.connect(db) as conn:
        left = sorted(
            r[0] for r in conn.execute(f"SELECT DISTINCT month_start FROM {MONTHLY}").fetchall()
        )
        per_licence = conn.execute(
            f"SELECT license_id::text, count(*) FROM {MONTHLY} GROUP BY 1"
        ).fetchall()
    # July and August ended on/before 09-20 -> deleted; September straddles -> kept.
    assert left == [date(2026, 9, 1), date(2026, 10, 1)]
    assert dict(per_licence) == {LICENSE_OF[t]: 2 for t in TENANTS}
    entry = next(e for e in receipt["tables"] if e["table"] == MONTHLY)
    assert entry["keyed_by"] == ["license_id", "subject_id"]
    assert entry["tenant_col"] is None
    assert entry["deleted"] == entry["eligible"] == 6
    assert entry["untouched_after_cutoff"] == 6
    assert entry["unaudited"] == "counts are the only record"
    assert receipt["resolved_scope"]["untenanted_eligible_rows_any_licence"] == {MONTHLY: 6}
    # The exact edge: month_start 08-01 with cutoff 09-01T00:00Z is eligible.
    with psycopg.connect(db) as conn:
        _insert(
            conn, MONTHLY, {"license_id": LICENSE_OF[TENANT_A], "month_start": date(2026, 8, 1)}
        )
        t = next(x for x in tp.ALLOWLIST if x.name == MONTHLY)
        assert (
            _scalar(
                conn,
                sql.SQL("SELECT count(*) FROM {} WHERE ").format(sql.Identifier(MONTHLY))
                + tp._eligible(t),
                {"cutoff": datetime(2026, 9, 1, tzinfo=UTC)},
            )
            == 1
        )
        assert (
            _scalar(
                conn,
                sql.SQL("SELECT count(*) FROM {} WHERE ").format(sql.Identifier(MONTHLY))
                + tp._eligible(t),
                {"cutoff": datetime(2026, 8, 31, 23, 59, tzinfo=UTC)},
            )
            == 0
        )


def test_tenants_scope_aborts_when_monthly_usage_has_history(db, tmp_path):
    """No authoritative licence->tenant mapping: --tenants must not guess."""
    before, guards = _counts(db), _guards(db)
    for extra in ((), ("--execute", "--include-config", "--void-invoices")):
        code, receipt = _run(tmp_path, "--tenants", TENANT_A, *extra, receipt=f"r{len(extra)}.json")
        assert code == 3, receipt
        assert receipt["outcome"] == "aborted_rolled_back"
        assert MONTHLY in receipt["error"] and "--all-tenants" in receipt["error"]
        assert "tables" not in receipt  # aborted before any per-table work
        assert "invoices" not in receipt  # ... and before any invoice was voided
    assert _counts(db) == before  # nothing deleted, no invoice voided
    assert _guards(db) == guards
