"""Unit tests for the resolution-decision logic in
``scripts/cleanup_duplicate_budgets.py``.

The script's network/DB plumbing is intentionally NOT exercised here — only the
pure function ``resolve_group`` that picks which row to keep when several
active TokenBudget rows exist for the same (tenant, license, period). Network
behavior is operator territory and is verified by re-running the script in
dry-run mode after a real deploy.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.conftest import T

# The scripts/ directory is not on sys.path during test collection (only src/
# is per pyproject.toml), so we load the module by file path.
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "cleanup_duplicate_budgets.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("cleanup_duplicate_budgets", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["cleanup_duplicate_budgets"] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_module()
BudgetRow = _mod.BudgetRow
resolve_group = _mod.resolve_group
group_rows = _mod.group_rows


def _row(
    *,
    rid: str = "00000000-0000-0000-0000-000000000001",
    tenant: str = T("t"),
    license_id: str = "11111111-1111-1111-1111-111111111111",
    period: str = "monthly",
    source: str = "manual",
    consumed: int = 0,
    created_at: datetime | None = None,
) -> BudgetRow:
    return BudgetRow(
        id=rid,
        tenant_id=tenant,
        license_id=license_id,
        period=period,
        source=source,
        consumed_tokens=consumed,
        created_at=created_at or datetime(2026, 1, 1, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Single-row groups: nothing to do.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_resolve_single_row_keeps_it():
    only = _row(rid="aaaa", source="manual", consumed=42)
    res = resolve_group([only])
    assert res.keeper is only
    assert res.delete == ()
    assert res.unresolved_reason is None


@pytest.mark.unit
def test_resolve_empty_group_returns_unresolved():
    res = resolve_group([])
    assert res.keeper is None
    assert res.unresolved_reason == "empty group"


# ---------------------------------------------------------------------------
# The expected B2 shape: 1 manual + 1 license_sync.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_resolve_picks_manual_when_it_has_more_consumed():
    manual = _row(rid="m", source="manual", consumed=5_000)
    license_sync = _row(rid="ls", source="license_sync", consumed=1_000)
    res = resolve_group([manual, license_sync])
    assert res.keeper is manual
    assert res.delete == (license_sync,)
    assert res.unresolved_reason is None


@pytest.mark.unit
def test_resolve_picks_license_sync_when_it_has_more_consumed():
    manual = _row(rid="m", source="manual", consumed=100)
    license_sync = _row(rid="ls", source="license_sync", consumed=9_000)
    res = resolve_group([manual, license_sync])
    assert res.keeper is license_sync
    assert res.delete == (manual,)


@pytest.mark.unit
def test_resolve_input_order_does_not_matter():
    manual = _row(rid="m", source="manual", consumed=5_000)
    license_sync = _row(rid="ls", source="license_sync", consumed=1_000)
    a = resolve_group([manual, license_sync])
    b = resolve_group([license_sync, manual])
    assert a.keeper is b.keeper
    assert a.delete == b.delete


@pytest.mark.unit
def test_resolve_tie_keeps_older_created_at():
    older = datetime(2026, 1, 1, tzinfo=UTC)
    newer = older + timedelta(days=10)
    manual = _row(rid="m", source="manual", consumed=0, created_at=older)
    license_sync = _row(rid="ls", source="license_sync", consumed=0, created_at=newer)
    res = resolve_group([manual, license_sync])
    assert res.keeper is manual
    assert res.delete == (license_sync,)


@pytest.mark.unit
def test_resolve_tie_keeps_older_when_license_sync_is_older():
    older = datetime(2026, 1, 1, tzinfo=UTC)
    newer = older + timedelta(days=10)
    license_sync = _row(rid="ls", source="license_sync", consumed=42, created_at=older)
    manual = _row(rid="m", source="manual", consumed=42, created_at=newer)
    res = resolve_group([manual, license_sync])
    assert res.keeper is license_sync
    assert res.delete == (manual,)


# ---------------------------------------------------------------------------
# Unresolved shapes — operator must investigate; script skips.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_resolve_two_manuals_unresolved():
    res = resolve_group(
        [
            _row(rid="m1", source="manual", consumed=1),
            _row(rid="m2", source="manual", consumed=2),
        ]
    )
    assert res.keeper is None
    assert res.unresolved_reason is not None
    assert "manual" in res.unresolved_reason


@pytest.mark.unit
def test_resolve_two_license_syncs_unresolved():
    res = resolve_group(
        [
            _row(rid="ls1", source="license_sync", consumed=10),
            _row(rid="ls2", source="license_sync", consumed=20),
        ]
    )
    assert res.keeper is None
    assert res.unresolved_reason is not None


@pytest.mark.unit
def test_resolve_three_rows_unresolved():
    res = resolve_group(
        [
            _row(rid="m", source="manual", consumed=1),
            _row(rid="ls", source="license_sync", consumed=2),
            _row(rid="t", source="topup", consumed=3),
        ]
    )
    assert res.keeper is None
    assert res.unresolved_reason is not None


@pytest.mark.unit
def test_resolve_unknown_source_pair_unresolved():
    res = resolve_group(
        [
            _row(rid="m", source="manual", consumed=1),
            _row(rid="x", source="topup", consumed=2),
        ]
    )
    assert res.keeper is None
    assert res.unresolved_reason is not None


# ---------------------------------------------------------------------------
# Grouping helper.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_group_rows_keys_on_tenant_license_period():
    a = _row(rid="a", tenant=T("t1"), license_id="L1", period="monthly")
    b = _row(rid="b", tenant=T("t1"), license_id="L1", period="monthly")
    c = _row(rid="c", tenant=T("t1"), license_id="L1", period="weekly")
    d = _row(rid="d", tenant=T("t2"), license_id="L1", period="monthly")
    grouped = group_rows([a, b, c, d])
    assert grouped[(T("t1"), "L1", "monthly")] == [a, b]
    assert grouped[(T("t1"), "L1", "weekly")] == [c]
    assert grouped[(T("t2"), "L1", "monthly")] == [d]
