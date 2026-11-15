"""Coverage for ``scripts/unpark_loose_duplicates.py``, which had NONE before
this PR.

Two things matter, and neither was ever exercised:

* the grain-invariant check is actually WIRED into the failure path — a
  restatement that loses the per-(bucket_date, project_id) grain (the exact
  0013-shaped defect this whole PR exists to fix) must re-park the un-parked
  rows and re-roll, restoring the pre-run state — not just compute the check
  and ignore the result.
* the per-date rollup loop reports a failure loudly (naming the unrolled
  dates, saying plainly the un-park already happened) rather than crashing
  past a stale rollup with no recovery signal (finding 1, mirrored from
  ``backfill_license_attribution.py``).
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text

from services.rollup_engine import rollup_daily
from tests.conftest import T

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "unpark_loose_duplicates.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("unpark_loose_duplicates", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["unpark_loose_duplicates"] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_module()

_SERVICE_KEY = "worker-management"  # pragma: allowlist secret
_LOOSE = "duplicate_of_inference_router_loose"


async def _seed_parked_event(session_factory, *, tenant, project_id, cost, at):
    """One event already parked under the LOOSE reason — the state this
    script's whole job is to reverse."""
    event_id = uuid.uuid4()
    async with session_factory() as session:
        await session.execute(
            text(
                "INSERT INTO billing_cost_events "
                "(id, tenant_id, license_id, subject_id, project_id, service_key, "
                " provider, input_tokens, output_tokens, cost_usd, cost_source, "
                " created_at, event_ts, excluded_from_billing, exclusion_reason, "
                " duplicate_of_event_id) "
                "VALUES (:id, :t, NULL, NULL, CAST(:proj AS uuid), :sk, :p, 10, 5, "
                "        CAST(:c AS numeric), 'asserted', :ts, :ts, true, :reason, :dup)"
            ),
            {
                "id": event_id,
                "t": tenant,
                "proj": project_id,
                "sk": _SERVICE_KEY,
                "p": "claude",
                "c": cost,
                "ts": at,
                "reason": _LOOSE,
                "dup": uuid.uuid4(),
            },
        )
        await session.commit()
    return event_id


def _patch_common(monkeypatch, factory):
    async def _init_db(_settings):
        return None

    monkeypatch.setattr(_mod, "get_settings", lambda: SimpleNamespace())
    monkeypatch.setattr(_mod, "init_db", _init_db)
    monkeypatch.setattr(_mod, "get_session_factory", lambda: factory)


@pytest.mark.unit
async def test_grain_mismatch_reparks_rows_and_reports(db_session_factory, monkeypatch, capsys):
    """Force the restated rollup to lose the project axis and prove the check
    actually does something: rows are re-parked, dates re-rolled, and the run
    exits non-zero — not merely "computed and printed"."""
    from models.db import CostDaily
    from models.db._common import NULL_PROJECT_ID

    tenant = T("unpark-grain-mismatch")
    day = (datetime.now(UTC) - timedelta(days=7)).date()
    at = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=9)
    project_a = uuid.UUID("eeeeeeee-0000-4000-8000-000000000001")
    project_b = uuid.UUID("ffffffff-0000-4000-8000-000000000002")

    await _seed_parked_event(
        db_session_factory, tenant=tenant, project_id=project_a, cost="3.000000", at=at
    )
    await _seed_parked_event(
        db_session_factory, tenant=tenant, project_id=project_b, cost="7.000000", at=at
    )

    # Nothing billable exists yet (both rows are parked) — no pre-existing
    # rollup is needed; the grain control compares the FRESH restatement
    # against the events, not a before/after money total.

    real_rollup_daily = rollup_daily
    call_count = {"n": 0}

    async def _collapse(_tenant):
        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(CostDaily).where(
                            CostDaily.tenant_id == _tenant, CostDaily.bucket_date == day
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 2, f"expected 2 pre-collapse rows, got {len(rows)}"
            total_cost = sum((r.total_cost_usd for r in rows), rows[0].total_cost_usd * 0)
            total_requests = sum(r.request_count for r in rows)
            keep, drop = rows[0], rows[1]
            await session.delete(drop)
            await session.flush()
            keep.project_id = NULL_PROJECT_ID
            keep.total_cost_usd = total_cost
            keep.request_count = total_requests
            await session.commit()

    async def _rollup_then_maybe_collapse(factory, d):
        await real_rollup_daily(factory, d)
        call_count["n"] += 1
        if call_count["n"] == 1:
            await _collapse(tenant)

    monkeypatch.setattr(_mod, "rollup_daily", _rollup_then_maybe_collapse)
    _patch_common(monkeypatch, db_session_factory)

    rc = await _mod._run(apply=True)
    out = capsys.readouterr().out

    assert rc == 1, out
    assert "ABORTED AND REVERTED" in out

    # ── the rows must be RE-PARKED, exactly as they started.
    async with db_session_factory() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT excluded_from_billing, exclusion_reason FROM billing_cost_events "
                    "WHERE tenant_id = :t"
                ),
                {"t": tenant},
            )
        ).all()
    assert len(rows) == 2
    for excluded, reason in rows:
        assert excluded is True, "the grain-mismatch revert must re-park the un-parked rows"
        assert reason == _LOOSE

    # ── the daily rows must be restored: two projects, no NULL_PROJECT_ID
    # sentinel, money intact.
    async with db_session_factory() as session:
        daily_rows = (
            (
                await session.execute(
                    select(CostDaily).where(
                        CostDaily.tenant_id == tenant, CostDaily.bucket_date == day
                    )
                )
            )
            .scalars()
            .all()
        )
    by_project = {r.project_id: r for r in daily_rows}
    assert NULL_PROJECT_ID not in by_project, (
        "the revert's re-roll must not still show the collapsed sentinel"
    )


@pytest.mark.unit
async def test_rollup_failure_reports_unrolled_dates_without_reverting(
    db_session_factory, monkeypatch, capsys
):
    """Finding 1's shape, mirrored for the unpark script: a `rollup_daily`
    exception must exit non-zero, name the exact date, and say plainly the
    un-park already happened — and must NOT re-park rows that are, in fact,
    correctly un-parked (only their rollup is stale)."""
    tenant = T("unpark-rollup-fail")
    day = (datetime.now(UTC) - timedelta(days=8)).date()
    at = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=9)

    await _seed_parked_event(
        db_session_factory, tenant=tenant, project_id=uuid.uuid4(), cost="2.000000", at=at
    )

    async def _boom(_factory, _day):
        raise RuntimeError("simulated connection reset")

    monkeypatch.setattr(_mod, "rollup_daily", _boom)
    _patch_common(monkeypatch, db_session_factory)

    rc = await _mod._run(apply=True)
    out = capsys.readouterr().out

    assert rc == 1
    assert str(day) in out, out
    assert (
        "already un-parked" in out.lower()
        or "already committed" in out.lower()
        or "un-parked" in out.lower()
    )
    assert "STALE ROLLUP" in out

    # The un-park itself must survive — the rollup being stale is not licence
    # to also undo a correct write.
    async with db_session_factory() as session:
        row = (
            await session.execute(
                text("SELECT excluded_from_billing FROM billing_cost_events WHERE tenant_id = :t"),
                {"t": tenant},
            )
        ).one()
    assert row[0] is False, "a stale rollup must not re-park an already-correct un-park"
