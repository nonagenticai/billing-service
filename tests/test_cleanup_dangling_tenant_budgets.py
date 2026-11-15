"""Unit tests for the pure classification logic in
``scripts/cleanup_dangling_tenant_budgets.py``.

Only ``plan_cleanup`` (which budgets are safe to soft-delete) and
``load_deleted_tenant_ids`` are exercised — the DB/HTTP plumbing is operator
territory, verified by a real dry-run after deploy (mirrors
``test_cleanup_duplicate_budgets.py``).
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "cleanup_dangling_tenant_budgets.py"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("cleanup_dangling_tenant_budgets", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["cleanup_dangling_tenant_budgets"] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_module()
BudgetRow = _mod.BudgetRow
plan_cleanup = _mod.plan_cleanup
load_deleted_tenant_ids = _mod.load_deleted_tenant_ids
DELETED_TENANT_IDS = _mod.DELETED_TENANT_IDS


def _row(
    *,
    rid: str = "00000000-0000-0000-0000-000000000001",
    tenant: str = "8382f0b3-c082-4f97-8d47-120ce5a18e05",
    license_id: str = "11111111-1111-1111-1111-111111111111",
    source: str = "license_sync",
    active: bool = True,
    budget_tokens: int = 10_000_000,
    consumed: int = 0,
    created_at: datetime | None = None,
) -> BudgetRow:
    return BudgetRow(
        id=rid,
        tenant_id=tenant,
        tenant_name=tenant,
        license_id=license_id,
        source=source,
        active=active,
        budget_tokens=budget_tokens,
        consumed_tokens=consumed,
        created_at=created_at or datetime(2026, 4, 26, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Classification.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_active_zero_consumed_is_deletable():
    r = _row(active=True, consumed=0)
    plan = plan_cleanup([r])
    assert plan.to_delete == (r,)
    assert plan.skipped_consumed == ()
    assert plan.already_inactive == ()
    assert plan.is_clean is True


@pytest.mark.unit
def test_inactive_row_is_left_alone():
    r = _row(active=False, consumed=0)
    plan = plan_cleanup([r])
    assert plan.already_inactive == (r,)
    assert plan.to_delete == ()
    assert plan.is_clean is True


@pytest.mark.unit
def test_active_consumed_is_skipped_not_deleted():
    """Money-safety: a budget with usage is NEVER auto-deleted."""
    r = _row(active=True, consumed=1)
    plan = plan_cleanup([r])
    assert plan.skipped_consumed == (r,)
    assert plan.to_delete == ()
    assert plan.is_clean is False


@pytest.mark.unit
def test_inactive_consumed_is_left_alone_not_flagged():
    """An already-inactive budget with historical usage is inert history."""
    r = _row(active=False, consumed=5_000)
    plan = plan_cleanup([r])
    assert plan.already_inactive == (r,)
    assert plan.skipped_consumed == ()
    assert plan.is_clean is True


@pytest.mark.unit
def test_mixed_set_partitions_correctly():
    deletable = _row(rid="a", active=True, consumed=0)
    consumed = _row(rid="b", active=True, consumed=42)
    inactive = _row(rid="c", active=False, consumed=0)
    plan = plan_cleanup([deletable, consumed, inactive])
    assert plan.to_delete == (deletable,)
    assert plan.skipped_consumed == (consumed,)
    assert plan.already_inactive == (inactive,)
    assert plan.is_clean is False


@pytest.mark.unit
def test_empty_input_is_clean_and_empty():
    plan = plan_cleanup([])
    assert plan.to_delete == ()
    assert plan.skipped_consumed == ()
    assert plan.already_inactive == ()
    assert plan.is_clean is True


# ---------------------------------------------------------------------------
# Deleted-tenant set loading.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_builtin_set_has_23_unique_ids():
    assert len(DELETED_TENANT_IDS) == 23
    assert len(set(DELETED_TENANT_IDS)) == 23


@pytest.mark.unit
def test_load_defaults_to_builtin_when_no_file():
    assert load_deleted_tenant_ids(None) == list(DELETED_TENANT_IDS)


@pytest.mark.unit
def test_load_from_file_skips_blanks_and_comments(tmp_path):
    p = tmp_path / "ids.txt"
    p.write_text(
        "# deleted tenants\naaaa-1111\n\n  bbbb-2222  \n# trailing comment\n",
        encoding="utf-8",
    )
    assert load_deleted_tenant_ids(str(p)) == ["aaaa-1111", "bbbb-2222"]
