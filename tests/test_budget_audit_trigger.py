"""Issue #244: token_budgets / request_quota_config removal is audited.

2026-09-30: ``scripts/tenant_purge.py --include-config`` hard-DELETEd netcare's
ACTIVE budget and nothing recorded it. These run against the suite's
``create_all`` schema, armed by ``models.db.budget_audit``'s after_create hooks;
the drift test at the bottom pins those hooks' DDL to alembic 0046's.
"""

from __future__ import annotations

import importlib.util
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import sqlalchemy as sa

import models.db.budget_audit as ba
from models.db import NULL_LICENSE_ID


def _load_migration():  # noqa: ANN202
    path = (
        Path(ba.__file__).resolve().parents[3]
        / "alembic"
        / "versions"
        / "0046_token_budget_audit.py"
    )
    spec = importlib.util.spec_from_file_location("_mig_0046", path)
    assert spec and spec.loader
    mig = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mig)
    return mig


def _budget_row(tenant_id: str, *, active: bool) -> dict:
    now = datetime.now(UTC) - timedelta(days=3)
    return {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "license_id": NULL_LICENSE_ID,
        "budget_tokens": 1000,
        "period": "monthly",
        "includes_input": True,
        "includes_output": True,
        "active": active,
        "created_at": now,
        "updated_at": now,
    }


def _quota_row(tenant_id: str) -> dict:
    now = datetime.now(UTC) - timedelta(days=3)
    return {
        "license_id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "quota_monthly": 10,
        "created_at": now,
        "updated_at": now,
    }


async def _seed(conn, table: str, rows: list[dict]) -> None:
    cols = ", ".join(rows[0])
    binds = ", ".join(f":{c}" for c in rows[0])
    await conn.execute(sa.text(f"INSERT INTO {table} ({cols}) VALUES ({binds})"), rows)


async def _fresh(conn) -> None:
    """Drop the rows conftest's per-test TRUNCATE legitimately wrote (AFTER TRUNCATE)."""
    await conn.execute(sa.text("DELETE FROM billing_delete_audit"))


async def _audit(conn) -> list:
    result = await conn.execute(sa.text("SELECT * FROM billing_delete_audit ORDER BY id"))
    return list(result.mappings())


async def test_delete_from_token_budgets_lands_one_audit_row(db_engine) -> None:
    """THE DEFECT: the purge's DELETE FROM token_budgets left no record."""
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "token_budgets", [_budget_row(tenant, active=True) for _ in range(2)])
        assert await _audit(conn) == [], "seeding must not write an audit row"
        await conn.execute(sa.text("SET LOCAL billing.delete_reason = 'purge-244'"))
        await conn.execute(sa.text("DELETE FROM token_budgets WHERE tenant_id = :t"), {"t": tenant})
        rows = await _audit(conn)

    assert len(rows) == 1, rows
    row = rows[0]
    assert row["table_name"] == "token_budgets"
    assert row["operation"] == "DELETE"
    assert row["rows_deleted"] == 2
    assert row["reason"] == "purge-244"
    assert [str(t) for t in row["tenant_ids"]] == [tenant]
    assert row["distinct_tenant_count"] == 1
    assert len(row["sample_ids"]) == 2
    assert row["statement"] and "DELETE FROM token_budgets" in row["statement"]
    assert row["created_at_min"] is not None and row["created_at_max"] is not None


async def test_deactivating_a_budget_lands_one_deactivate_row(db_engine) -> None:
    """active true -> false refuses spend exactly like a delete; recorded once per statement."""
    tenant = str(uuid.uuid4())
    other = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(
            conn,
            "token_budgets",
            [_budget_row(tenant, active=True) for _ in range(3)]
            + [_budget_row(other, active=False)],
        )
        # Touches 4 rows; only the 3 active ones flip.
        await conn.execute(sa.text("UPDATE token_budgets SET active = false"))
        rows = await _audit(conn)

    assert len(rows) == 1, rows
    row = rows[0]
    assert row["table_name"] == "token_budgets"
    assert row["operation"] == ba.DEACTIVATE_OPERATION == "DEACTIVATE"
    assert row["rows_deleted"] == 3
    assert [str(t) for t in row["tenant_ids"]] == [tenant]
    assert len(row["sample_ids"]) == 3
    assert row["reason"] == "unspecified"
    assert row["statement"] and "UPDATE token_budgets" in row["statement"]


async def test_update_that_does_not_flip_active_writes_no_row(db_engine) -> None:
    """The debit path UPDATEs token_budgets constantly; it must stay silent."""
    tenant = str(uuid.uuid4())
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(
            conn,
            "token_budgets",
            [_budget_row(tenant, active=True), _budget_row(tenant, active=False)],
        )
        await conn.execute(
            sa.text("UPDATE token_budgets SET consumed_tokens = consumed_tokens + 5")
        )
        await conn.execute(sa.text("UPDATE token_budgets SET active = true"))  # re-activation
        await conn.execute(sa.text("UPDATE token_budgets SET active = active"))
        rows = await _audit(conn)

    assert rows == []


async def test_delete_from_request_quota_config_lands_one_audit_row(db_engine) -> None:
    """No ``id`` column, so it has its own function; sample_ids carries license_id."""
    tenant = str(uuid.uuid4())
    seeded = [_quota_row(tenant) for _ in range(2)]
    async with db_engine.begin() as conn:
        await _fresh(conn)
        await _seed(conn, "request_quota_config", seeded)
        await conn.execute(
            sa.text("DELETE FROM request_quota_config WHERE tenant_id = :t"), {"t": tenant}
        )
        rows = await _audit(conn)

    assert len(rows) == 1, rows
    row = rows[0]
    assert row["table_name"] == "request_quota_config"
    assert row["operation"] == "DELETE"
    assert row["rows_deleted"] == 2
    assert [str(t) for t in row["tenant_ids"]] == [tenant]
    assert sorted(str(i) for i in row["sample_ids"]) == sorted(str(r["license_id"]) for r in seeded)


async def test_triggers_are_attached(db_engine) -> None:
    """Attachment from the catalog, so a trigger on the wrong relation cannot pass."""
    async with db_engine.begin() as conn:
        result = await conn.execute(
            sa.text(
                """
                SELECT c.relname, t.tgname, p.proname
                  FROM pg_trigger t
                  JOIN pg_class c ON c.oid = t.tgrelid
                  JOIN pg_proc p ON p.oid = t.tgfoid
                 WHERE NOT t.tgisinternal
                   AND c.relname IN ('token_budgets', 'request_quota_config')
                """
            )
        )
        attached = {(r[0], r[1], r[2]) for r in result}

    assert attached >= {
        ("token_budgets", "billing_delete_audit_trg", "billing_audit_deleted_rows"),
        ("token_budgets", "billing_truncate_audit_trg", "billing_audit_deleted_rows"),
        (
            "token_budgets",
            "billing_budget_deactivate_audit_trg",
            "billing_audit_deactivated_budgets",
        ),
        (
            "request_quota_config",
            "billing_quota_config_delete_audit_trg",
            "billing_audit_deleted_quota_config",
        ),
        (
            "request_quota_config",
            "billing_quota_config_truncate_audit_trg",
            "billing_audit_deleted_quota_config",
        ),
    }


def test_migration_ddl_is_identical_to_the_module_ddl() -> None:
    """Tests exercise the module's DDL; production runs 0046's frozen copy."""
    mig = _load_migration()
    assert mig.LEDGER_SHAPED_CONFIG_TABLES == ba.LEDGER_SHAPED_CONFIG_TABLES
    assert mig.BUDGET_AUDIT_DDL == ba.BUDGET_AUDIT_DDL
    assert mig.BUDGET_AUDIT_DROP_DDL == ba.BUDGET_AUDIT_DROP_DDL
    assert mig.down_revision == "0045_money_multi_currency"
