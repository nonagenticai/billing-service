"""Unit tests for the pure decision logic in
``scripts/backfill_license_attribution.py``.

Two things are worth testing here and the DB plumbing is not one of them:

* ``_in_window`` — rule 3, "never attribute spend that predates its licence".
  Measured on prod 2026-08-28 this rule is not academic: two ``019de842`` rows
  from 2026-05-22 would otherwise be attributed to a licence that did not start
  until 2026-06-26.
* ``fetch_license_windows`` failing CLOSED. This is the one that actually
  protects the run. If the licences service is unreachable, or answers with an
  empty list, EVERY row declines and the audit line reads exactly like a clean
  no-op — "0 attributed" looks identical whether there was nothing to do or the
  script could not see anything. An absence must raise, not return.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "backfill_license_attribution.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_license_attribution", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["backfill_license_attribution"] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_module()


def _dt(y: int, m: int, d: int, hh: int = 0, mm: int = 0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=UTC)


# --------------------------------------------------------------------------
# rule 3 — the validity window
# --------------------------------------------------------------------------

STARTS = _dt(2026, 7, 20, 12, 24)
EXPIRES = _dt(2026, 8, 20, 12, 24)


@pytest.mark.parametrize(
    ("created_at", "window", "expected", "why"),
    [
        (_dt(2026, 7, 20, 21, 11), (STARTS, None), True, "after start, perpetual"),
        (_dt(2026, 5, 22, 12, 20), (STARTS, None), False, "the real 019de842 case: 2 months early"),
        (_dt(2026, 9, 1), (STARTS, EXPIRES), False, "after a bounded expiry"),
        (_dt(2026, 8, 1), (STARTS, EXPIRES), True, "inside a bounded window"),
        (_dt(2026, 5, 1), (None, None), True, "unbounded both ways"),
        (_dt(2026, 1, 1), (None, EXPIRES), True, "no start bound, before expiry"),
    ],
)
def test_in_window(created_at, window, expected, why):
    assert _mod._in_window(created_at, window) is expected, why


def test_window_boundaries_are_inclusive():
    """An event AT the boundary instant belongs to the licence.

    Pinned deliberately: this is the assertion that dies if ``<`` is mutated to
    ``<=`` (or ``>`` to ``>=``). Without it, both comparisons could be flipped
    and every other case above would still pass.
    """
    assert _mod._in_window(STARTS, (STARTS, EXPIRES)) is True
    assert _mod._in_window(EXPIRES, (STARTS, EXPIRES)) is True
    # ...and one microsecond outside it does not. timedelta, not float
    # arithmetic on .timestamp(): at 1.8e9 seconds a float64 ulp is ~240ns, so a
    # 1us delta is only two ulps away from rounding back onto the boundary and
    # turning this into a test of nothing.
    one_us = timedelta(microseconds=1)
    assert _mod._in_window(STARTS - one_us, (STARTS, EXPIRES)) is False
    assert _mod._in_window(EXPIRES + one_us, (STARTS, EXPIRES)) is False


# --------------------------------------------------------------------------
# fail-closed: an absence must raise, never read as "nothing to do"
# --------------------------------------------------------------------------


class _Resp:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        return self._payload


class _Client:
    """Minimal async-context httpx stand-in. Records what was asked for."""

    last_params: dict[str, str] | None = None

    def __init__(self, response: _Resp) -> None:
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None, headers=None):
        type(self).last_params = params
        return self._response


def _patch(monkeypatch, response: _Resp) -> None:
    monkeypatch.setattr(
        _mod, "ServiceTokenProvider", lambda settings: SimpleNamespace(get_token=_token)
    )
    monkeypatch.setattr(_mod.httpx, "AsyncClient", lambda **kw: _Client(response))


async def _token() -> str:
    return "tok"


def _settings(url: str = "http://licenses.example.com") -> SimpleNamespace:
    return SimpleNamespace(licenses_url=url)


@pytest.mark.asyncio
async def test_empty_active_list_raises_rather_than_returning_nothing(monkeypatch):
    """THE important one. Zero active licences is not an answer, it is a blindness."""
    _patch(monkeypatch, _Resp(200, []))
    with pytest.raises(RuntimeError, match="ZERO active licences"):
        await _mod.fetch_license_windows(_settings())


@pytest.mark.asyncio
async def test_non_200_raises(monkeypatch):
    _patch(monkeypatch, _Resp(503, []))
    with pytest.raises(RuntimeError, match="503"):
        await _mod.fetch_license_windows(_settings())


@pytest.mark.asyncio
async def test_unconfigured_url_raises(monkeypatch):
    with pytest.raises(RuntimeError, match="licenses_url"):
        await _mod.fetch_license_windows(_settings(url=""))


@pytest.mark.asyncio
async def test_windows_are_parsed_and_only_active_are_requested(monkeypatch):
    _patch(
        monkeypatch,
        _Resp(
            200,
            [
                {
                    "id": "019f7f7c-5006-72c0-8fa7-e6317340c0c9",
                    "starts_at": "2026-07-20T12:24:41+00:00",
                    "expires_at": None,
                },
                {
                    "id": "019f03a1-a7e0-7316-9270-e1775b0515ea",
                    "starts_at": None,
                    "expires_at": None,
                },
                {"starts_at": None, "expires_at": None},  # no id — skipped, not crashed
            ],
        ),
    )
    windows = await _mod.fetch_license_windows(_settings())

    assert set(windows) == {
        "019f7f7c-5006-72c0-8fa7-e6317340c0c9",
        "019f03a1-a7e0-7316-9270-e1775b0515ea",
    }
    assert windows["019f7f7c-5006-72c0-8fa7-e6317340c0c9"][0] == _dt(2026, 7, 20, 12, 24).replace(
        second=41
    )
    assert windows["019f7f7c-5006-72c0-8fa7-e6317340c0c9"][1] is None
    # Rule 4: a revoked/expired licence must never even be offered as a candidate,
    # so the status filter is part of the contract, not an optimisation.
    assert _Client.last_params == {"status": "active"}


def test_backfill_marker_is_distinct_from_the_flushers(monkeypatch):
    """Rule 5. If these two ever collide, a retroactive attribution becomes
    indistinguishable from one derived live at flush time."""
    assert _mod.BACKFILL_MARKER == "+backfilled"
    assert _mod.BACKFILL_MARKER != "+resolved"


# --------------------------------------------------------------------------
# THE RESTATEMENT — a real Postgres, because the defect is only visible there
# --------------------------------------------------------------------------
#
# Everything above this line runs on monkeypatched pure functions and would have
# stayed green through the entire lifetime of the bug being fixed here. The
# script used to restate ``billing_cost_daily`` with migration 0013's frozen SQL,
# which predates the ``project_id`` column (alembic 0036): its INSERT never names
# the column, so every rebuilt row takes the table's ``NULL_PROJECT_ID`` default
# and the per-project grain of EVERY tenant on the affected date is destroyed.
# Measured live on 2026-09-16: 107 rows collapsed to 51.
#
# ⛔ THE SCRIPT'S OWN GUARD COULD NOT SEE IT, AND THAT IS THE POINT. Regrouping
# preserves ``sum(total_cost_usd)`` and ``sum(request_count)``, so the money
# invariant read HELD on every one of those runs. A test that only checks money
# reproduces the same blindness — this one asserts the per-PROJECT tuple.
#
# ⭐ MUTATION-CHECKED, NOT ASSUMED. With the 0013 SQL put back in ``_run``, this
# test fails on:
#     assert len(rows) == 2 ... (got 1)
# in ``test_restatement_preserves_per_project_grain`` — the two projects' rows
# merge into one sentinel row. See the PR body for the run.

import uuid as _uuid  # noqa: E402
from datetime import UTC as _UTC  # noqa: E402
from datetime import date as _date  # noqa: E402
from decimal import Decimal as _Decimal  # noqa: E402

from sqlalchemy import select as _select  # noqa: E402
from sqlalchemy import text as _text  # noqa: E402

from services.rollup_engine import rollup_daily  # noqa: E402
from tests.conftest import T  # noqa: E402

_DAY: _date = (datetime.now(_UTC) - timedelta(days=2)).date()
_AT = datetime.combine(_DAY, datetime.min.time(), tzinfo=_UTC) + timedelta(hours=12)

_SERVICE_KEY = "inference-router"  # pragma: allowlist secret
_APP = "Inference Router"


async def _seed_event(session_factory, *, tenant, project_id, subject_id, license_id, cost):
    """One licence-less cost event. Raw SQL for the reason given in
    ``tests/test_rollup_project_axis.py::_insert_cost_event``: going through the
    flusher would make the stored cost depend on whatever pricing rows another
    lane happens to have seeded."""
    async with session_factory() as session:
        await session.execute(
            _text(
                "INSERT INTO billing_cost_events "
                "(id, tenant_id, license_id, subject_id, project_id, service_key, "
                " provider, input_tokens, output_tokens, cost_usd, cost_source, "
                " created_at, event_ts) "
                "VALUES (:id, :t, NULL, :s, CAST(:proj AS uuid), :sk, :p, 100, 50, "
                "        CAST(:c AS numeric), 'asserted', :ts, :ts)"
            ),
            {
                "id": _uuid.uuid4(),
                "t": tenant,
                "s": subject_id,
                "proj": project_id,
                "sk": _SERVICE_KEY,
                "p": "claude",
                "c": cost,
                "ts": _AT,
            },
        )
        await session.commit()
    return license_id


@pytest.mark.unit
async def test_restatement_preserves_per_project_grain(db_session_factory, monkeypatch, capsys):
    """Two projects in, two projects out — with their money intact.

    The whole vertical: seed one UTC date with licence-less events under TWO
    distinct ``project_id``s, run ``_run(apply=True)``, and assert the restated
    ``billing_cost_daily`` still carries both projects with unchanged per-project
    money. With 0013's SQL restoring the day, both projects land on
    ``NULL_PROJECT_ID`` and this collapses to a single row.
    """
    from models.db._common import NULL_PROJECT_ID

    tenant = T("backfill-grain")
    licence = _uuid.uuid4()
    subject = _uuid.uuid4()
    project_a = _uuid.UUID("aaaaaaaa-0000-4000-8000-000000000001")
    project_b = _uuid.UUID("bbbbbbbb-0000-4000-8000-000000000002")

    await _seed_event(
        db_session_factory,
        tenant=tenant,
        project_id=project_a,
        subject_id=subject,
        license_id=licence,
        cost="1.250000",
    )
    await _seed_event(
        db_session_factory,
        tenant=tenant,
        project_id=project_b,
        subject_id=subject,
        license_id=licence,
        cost="2.750000",
    )

    # Establish the PRE-EXISTING rollup, the way prod always has one: the nightly
    # loop has already rolled this day before any backfill runs. Without it the
    # script's money control compares an EMPTY billing_cost_daily against a freshly
    # populated one and correctly refuses — which would be a test artefact, not the
    # condition under test.
    await rollup_daily(db_session_factory, _DAY)

    # The script's I/O edges, and only those: the licences service, the settings
    # map, and the engine bootstrap. The DB work, the restatement and the
    # invariant are the code under test and are NOT patched.
    monkeypatch.setattr(
        _mod, "get_settings", lambda: SimpleNamespace(cost_attribution_app_by_service_key="x")
    )
    monkeypatch.setattr(_mod, "parse_service_key_app_map", lambda _raw: {_SERVICE_KEY: _APP})

    async def _windows(_settings):
        return {str(licence): (None, None)}

    monkeypatch.setattr(_mod, "fetch_license_windows", _windows)

    async def _resolution_map(_session):
        return {(str(tenant), _APP): licence}

    monkeypatch.setattr(_mod, "get_resolution_map", _resolution_map)

    async def _init_db(_settings):
        return None

    monkeypatch.setattr(_mod, "init_db", _init_db)
    monkeypatch.setattr(_mod, "get_session_factory", lambda: db_session_factory)

    rc = await _mod._run(apply=True, batch=100)
    out = capsys.readouterr().out
    assert rc == 0, out

    from models.db import CostDaily

    async with db_session_factory() as session:
        rows = list(
            (
                await session.execute(
                    _select(CostDaily)
                    .where(CostDaily.tenant_id == tenant, CostDaily.bucket_date == _DAY)
                    .order_by(CostDaily.total_cost_usd)
                )
            )
            .scalars()
            .all()
        )

    # THE ASSERTION THE 0013 SQL FAILS. Not a money total — money is identical
    # either way, which is exactly why the script's own guard read HELD.
    assert len(rows) == 2, (
        "the restatement collapsed the project axis: expected one daily row per "
        f"project, got {len(rows)} — {[(str(r.project_id), str(r.total_cost_usd)) for r in rows]}"
    )
    by_project = {r.project_id: r for r in rows}
    assert set(by_project) == {project_a, project_b}, (
        f"projects did not survive: {sorted(str(p) for p in by_project)}"
    )
    assert NULL_PROJECT_ID not in by_project
    assert by_project[project_a].total_cost_usd == _Decimal("1.250000")
    assert by_project[project_b].total_cost_usd == _Decimal("2.750000")
    assert by_project[project_a].request_count == 1
    assert by_project[project_b].request_count == 1

    # …and the attribution itself actually happened, so this is not green on a
    # run that did nothing.
    async with db_session_factory() as session:
        attributed = (
            await session.execute(
                _text(
                    "SELECT count(*) FROM billing_cost_events "
                    "WHERE tenant_id = :t AND license_id = :l "
                    "AND cost_source LIKE '%' || :marker"
                ),
                {"t": tenant, "l": licence, "marker": _mod.BACKFILL_MARKER},
            )
        ).scalar_one()
    assert attributed == 2, "no row was attributed — the grain assertion above is vacuous"


# --------------------------------------------------------------------------
# FINDING 1 (adversarial review of PR #224) — a failing date must be LOUD and
# RECOVERABLE, not a bare traceback that strands every later date.
# --------------------------------------------------------------------------
#
# Events are committed for ALL dates before the per-date rollup loop runs, so a
# `rollup_daily` exception on date N must not (a) crash silently past the
# operator's ability to know which dates are stale, or (b) revert the
# already-committed, already-correct attributions — those are fine, only the
# rollup is stale.


async def _setup_single_project_run(db_session_factory, monkeypatch, *, tenant, day):
    """Shared plumbing for the finding-1 tests: one licence-less event, one
    UTC date, everything patched except the DB work and the rollup loop."""
    licence = _uuid.uuid4()
    subject = _uuid.uuid4()
    project = _uuid.uuid4()
    at = datetime.combine(day, datetime.min.time(), tzinfo=_UTC) + timedelta(hours=6)

    async with db_session_factory() as session:
        await session.execute(
            _text(
                "INSERT INTO billing_cost_events "
                "(id, tenant_id, license_id, subject_id, project_id, service_key, "
                " provider, input_tokens, output_tokens, cost_usd, cost_source, "
                " created_at, event_ts) "
                "VALUES (:id, :t, NULL, :s, CAST(:proj AS uuid), :sk, :p, 10, 5, "
                "        1.000000, 'asserted', :ts, :ts)"
            ),
            {
                "id": _uuid.uuid4(),
                "t": tenant,
                "s": subject,
                "proj": project,
                "sk": _SERVICE_KEY,
                "p": "claude",
                "ts": at,
            },
        )
        await session.commit()

    monkeypatch.setattr(
        _mod, "get_settings", lambda: SimpleNamespace(cost_attribution_app_by_service_key="x")
    )
    monkeypatch.setattr(_mod, "parse_service_key_app_map", lambda _raw: {_SERVICE_KEY: _APP})

    async def _windows(_settings):
        return {str(licence): (None, None)}

    monkeypatch.setattr(_mod, "fetch_license_windows", _windows)

    async def _resolution_map(_session):
        return {(str(tenant), _APP): licence}

    monkeypatch.setattr(_mod, "get_resolution_map", _resolution_map)

    async def _init_db(_settings):
        return None

    monkeypatch.setattr(_mod, "init_db", _init_db)
    monkeypatch.setattr(_mod, "get_session_factory", lambda: db_session_factory)


@pytest.mark.unit
async def test_rollup_failure_is_reported_and_events_stay_attributed(
    db_session_factory, monkeypatch, capsys
):
    """A `rollup_daily` failure on one date must exit non-zero, name the exact
    unrolled date in a pasteable form, say plainly that events are already
    attributed, and — unlike a guard trip — must NOT revert them: only the
    rollup for that date is stale, the attribution itself is correct."""
    tenant = T("backfill-rollup-fail")
    day = (datetime.now(_UTC) - timedelta(days=3)).date()
    await _setup_single_project_run(db_session_factory, monkeypatch, tenant=tenant, day=day)

    async def _boom(_factory, _day):
        raise RuntimeError("simulated advisory-lock contention")

    monkeypatch.setattr(_mod, "rollup_daily", _boom)

    rc = await _mod._run(apply=True, batch=100)
    out = capsys.readouterr().out

    assert rc == 1
    assert str(day) in out, out
    assert "already attributed" in out.lower()
    assert "STALE ROLLUP" in out
    assert "rollup_daily" in out  # the pasteable recovery command names the entry point

    # The attribution itself must survive — reporting a stale rollup is not
    # licence to also undo work that was already correct and already committed.
    async with db_session_factory() as session:
        row = (
            await session.execute(
                _text("SELECT license_id FROM billing_cost_events WHERE tenant_id = :t"),
                {"t": tenant},
            )
        ).one()
    assert row[0] is not None, "the rollup failure must not revert an already-correct attribution"


@pytest.mark.unit
async def test_rollup_failure_continues_past_the_first_bad_date(
    db_session_factory, monkeypatch, capsys
):
    """Two affected dates, the FIRST fails: the second must still be attempted
    (not abandoned), and both a success and a failure can be reported together."""
    tenant = T("backfill-rollup-fail-2")
    day_bad = (datetime.now(_UTC) - timedelta(days=5)).date()
    day_ok = (datetime.now(_UTC) - timedelta(days=4)).date()

    licence = _uuid.uuid4()
    subject = _uuid.uuid4()
    project = _uuid.uuid4()

    async def _seed(day):
        at = datetime.combine(day, datetime.min.time(), tzinfo=_UTC) + timedelta(hours=6)
        async with db_session_factory() as session:
            await session.execute(
                _text(
                    "INSERT INTO billing_cost_events "
                    "(id, tenant_id, license_id, subject_id, project_id, service_key, "
                    " provider, input_tokens, output_tokens, cost_usd, cost_source, "
                    " created_at, event_ts) "
                    "VALUES (:id, :t, NULL, :s, CAST(:proj AS uuid), :sk, :p, 10, 5, "
                    "        1.000000, 'asserted', :ts, :ts)"
                ),
                {
                    "id": _uuid.uuid4(),
                    "t": tenant,
                    "s": subject,
                    "proj": project,
                    "sk": _SERVICE_KEY,
                    "p": "claude",
                    "ts": at,
                },
            )
            await session.commit()

    await _seed(day_bad)
    await _seed(day_ok)

    monkeypatch.setattr(
        _mod, "get_settings", lambda: SimpleNamespace(cost_attribution_app_by_service_key="x")
    )
    monkeypatch.setattr(_mod, "parse_service_key_app_map", lambda _raw: {_SERVICE_KEY: _APP})

    async def _windows(_settings):
        return {str(licence): (None, None)}

    monkeypatch.setattr(_mod, "fetch_license_windows", _windows)

    async def _resolution_map(_session):
        return {(str(tenant), _APP): licence}

    monkeypatch.setattr(_mod, "get_resolution_map", _resolution_map)

    async def _init_db(_settings):
        return None

    monkeypatch.setattr(_mod, "init_db", _init_db)
    monkeypatch.setattr(_mod, "get_session_factory", lambda: db_session_factory)

    real_rollup_daily = _mod.rollup_daily
    calls: list = []

    async def _selective_boom(factory, day):
        calls.append(day)
        if day == day_bad:
            raise RuntimeError("simulated failure on the first date")
        await real_rollup_daily(factory, day)

    monkeypatch.setattr(_mod, "rollup_daily", _selective_boom)

    rc = await _mod._run(apply=True, batch=100)
    out = capsys.readouterr().out

    assert rc == 1
    # BOTH dates must have been attempted — the second is not abandoned because
    # the first raised.
    assert set(calls) == {day_bad, day_ok}
    assert str(day_bad) in out
    assert str(day_ok) not in out.split("unrolled date(s):")[1].split("\n")[0], (
        "only the FAILED date belongs in the recovery list, not the one that succeeded"
    )


# --------------------------------------------------------------------------
# FINDING 2 (adversarial review of PR #224) — the abort/revert path must
# actually be exercised: a grain mismatch must restore the DATABASE, not just
# print a message.
# --------------------------------------------------------------------------


@pytest.mark.unit
async def test_revert_path_restores_exact_pre_run_state_on_grain_mismatch(
    db_session_factory, monkeypatch, capsys
):
    """Force the RESTATED rollup to lose the project axis (the exact 0013-shaped
    failure this PR exists to catch) and assert the compensating revert puts
    the database back EXACTLY where it started: license_id/cost_source on the
    events, and the daily rows.

    This does not touch `_run`'s guard logic — it proves the EXISTING revert
    code path, which shipped with only a happy-path test
    (`test_restatement_preserves_per_project_grain`) covering it.
    """
    from decimal import Decimal as _Dec

    from models.db import CostDaily
    from models.db._common import NULL_PROJECT_ID as _NULL_PROJECT

    tenant = T("backfill-revert-path")
    licence = _uuid.uuid4()
    subject = _uuid.uuid4()
    project_a = _uuid.UUID("cccccccc-0000-4000-8000-000000000001")
    project_b = _uuid.UUID("dddddddd-0000-4000-8000-000000000002")
    day = (datetime.now(_UTC) - timedelta(days=6)).date()
    at = datetime.combine(day, datetime.min.time(), tzinfo=_UTC) + timedelta(hours=12)

    async def _seed_at(project_id, cost):
        async with db_session_factory() as session:
            await session.execute(
                _text(
                    "INSERT INTO billing_cost_events "
                    "(id, tenant_id, license_id, subject_id, project_id, service_key, "
                    " provider, input_tokens, output_tokens, cost_usd, cost_source, "
                    " created_at, event_ts) "
                    "VALUES (:id, :t, NULL, :s, CAST(:proj AS uuid), :sk, :p, 100, 50, "
                    "        CAST(:c AS numeric), 'asserted', :ts, :ts)"
                ),
                {
                    "id": _uuid.uuid4(),
                    "t": tenant,
                    "s": subject,
                    "proj": project_id,
                    "sk": _SERVICE_KEY,
                    "p": "claude",
                    "c": cost,
                    "ts": at,
                },
            )
            await session.commit()

    await _seed_at(project_a, "1.500000")
    await _seed_at(project_b, "4.500000")

    real_rollup_daily = rollup_daily
    # Establish the pre-existing, CORRECT rollup exactly like the happy-path
    # test does — the nightly loop has already rolled this day.
    await real_rollup_daily(db_session_factory, day)

    async def _collapse_project_axis(_tenant):
        """Mimic 0013: merge this tenant's two per-project daily rows for
        `day` into one row on NULL_PROJECT_ID, money summed. Silent — no
        exception — which is exactly how the real defect behaved: the money
        invariant held, only the grain was destroyed."""
        async with db_session_factory() as session:
            from sqlalchemy import select

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
            assert len(rows) == 2, f"setup expected 2 pre-collapse rows, got {len(rows)}"
            total_cost = sum((r.total_cost_usd for r in rows), _Dec("0"))
            total_requests = sum(r.request_count for r in rows)
            keep, drop = rows[0], rows[1]
            await session.delete(drop)
            await session.flush()
            keep.project_id = _NULL_PROJECT
            keep.total_cost_usd = total_cost
            keep.request_count = total_requests
            await session.commit()

    call_count = {"n": 0}

    async def _rollup_then_maybe_collapse(factory, d):
        await real_rollup_daily(factory, d)
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Only the FIRST restatement is broken — the compensating revert's
            # own re-roll (the second call for this date) must use the real,
            # correct engine so the test can assert exact pre-run restoration.
            await _collapse_project_axis(tenant)

    monkeypatch.setattr(_mod, "rollup_daily", _rollup_then_maybe_collapse)
    monkeypatch.setattr(
        _mod, "get_settings", lambda: SimpleNamespace(cost_attribution_app_by_service_key="x")
    )
    monkeypatch.setattr(_mod, "parse_service_key_app_map", lambda _raw: {_SERVICE_KEY: _APP})

    async def _windows(_settings):
        return {str(licence): (None, None)}

    monkeypatch.setattr(_mod, "fetch_license_windows", _windows)

    async def _resolution_map(_session):
        return {(str(tenant), _APP): licence}

    monkeypatch.setattr(_mod, "get_resolution_map", _resolution_map)

    async def _init_db(_settings):
        return None

    monkeypatch.setattr(_mod, "init_db", _init_db)
    monkeypatch.setattr(_mod, "get_session_factory", lambda: db_session_factory)

    rc = await _mod._run(apply=True, batch=100)
    out = capsys.readouterr().out

    assert rc == 1, out
    assert "ABORTED AND REVERTED" in out
    assert "lost the project axis" in out or "grain" in out.lower()

    # ── the events must be restored EXACTLY: license_id back to NULL,
    # cost_source back to the pre-run value.
    async with db_session_factory() as session:
        ev_rows = (
            await session.execute(
                _text(
                    "SELECT license_id, cost_source FROM billing_cost_events "
                    "WHERE tenant_id = :t ORDER BY cost_usd"
                ),
                {"t": tenant},
            )
        ).all()
    assert len(ev_rows) == 2
    for license_id, cost_source in ev_rows:
        assert license_id is None, "the revert must clear the attribution it applied"
        assert cost_source == "asserted", "the revert must restore the EXACT pre-run cost_source"

    # ── the daily rows must be restored EXACTLY: two projects, correct money,
    # no NULL_PROJECT_ID sentinel — i.e. identical to the state established by
    # the pre-existing rollup before `_run` ever touched the table.
    async with db_session_factory() as session:
        daily_rows = list(
            (
                await session.execute(
                    _select(CostDaily).where(
                        CostDaily.tenant_id == tenant, CostDaily.bucket_date == day
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(daily_rows) == 2, (
        "the revert's re-roll must restore two per-project rows, not the collapsed one: "
        f"{[(str(r.project_id), str(r.total_cost_usd)) for r in daily_rows]}"
    )
    by_project = {r.project_id: r for r in daily_rows}
    assert _NULL_PROJECT not in by_project
    assert set(by_project) == {project_a, project_b}
    assert by_project[project_a].total_cost_usd == _Decimal("1.500000")
    assert by_project[project_b].total_cost_usd == _Decimal("4.500000")
