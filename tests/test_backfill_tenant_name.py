"""Tests for ``scripts/backfill_tenant_name.py``.

An adversarial audit of the first version of this script returned NO on exactly
one ground: **it had zero tests**, while carrying the two pieces of logic that
decide whether ``--apply`` is safe to run against 72,277 rows on the prod
primary.

WHAT IS ACTUALLY AT RISK, AND WHY EACH THING BELOW IS HERE
-----------------------------------------------------------
* :data:`DAMAGED` decides which rows get REWRITTEN. If it matched a genuine
  name, the script would overwrite real data; if it stopped matching ``''``, the
  archived-tenant population would silently never be repaired. It is evaluated
  HERE BY POSTGRES, over the real constant, because the whole predicate is
  ``lower()`` / ``btrim()`` / ``uuid::text`` semantics — a Python re-implementation
  of it would be a test of the re-implementation.
* :func:`_usable_name` is the sole guard against writing a name that would
  RE-MATCH :data:`DAMAGED`. That is not a cosmetic check: the batch loop
  re-selects the predicate every pass, so a name that keeps matching is an
  infinite loop against production. The load-bearing property is a JOIN of the
  two — "every name ``_usable_name`` accepts produces a row Postgres no longer
  calls damaged" — and that is asserted directly, not assumed.
* The census, the arrivals count, the batch select and the batch update must all
  use the SAME predicate. A dry run that measures a different ``WHERE`` than the
  apply rewrites is worse than no dry run: it reports a confident number about a
  population it did not touch.
* The batch loop's cap and clamp (added after the audit) are what stop
  ``--apply`` spinning while the still-undeployed write-path fix keeps producing
  ``''`` rows behind it. Both are exercised, including the failure direction.

⚠️ WHAT IS DELIBERATELY NOT ASSERTED
------------------------------------
``DAMAGED`` matches a tenant whose REAL name happens to be its own uuid. That is
the worst case the audit attacked, and it survives on purpose: the predicate
cannot tell that row from the laundering it exists to repair, and
:func:`_usable_name` is what stops the repair looping on it. The test below PINS
that behaviour rather than removing it, so nobody "fixes" the predicate into
something that leaves 72,277 rows unrepairable.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.unit

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "backfill_tenant_name.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_tenant_name", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["backfill_tenant_name"] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_module()

TENANT = "019fb1f4-0000-7000-8000-00000000645f"


# ---------------------------------------------------------------------------
# _usable_name — the infinite-loop guard
# ---------------------------------------------------------------------------


class TestUsableName:
    def test_a_normal_name_is_usable(self) -> None:
        assert _mod._usable_name(TENANT, "rmt-e2e") is True

    def test_the_tenants_own_uuid_is_refused(self) -> None:
        """The exact value the whole change exists to stop being written."""
        assert _mod._usable_name(TENANT, TENANT) is False

    def test_the_uuid_is_refused_case_insensitively(self) -> None:
        """``uuid::text`` renders lowercase in Postgres; the wire may not.

        A registry answering ``019FB1F4-…`` must be refused just as hard — it is
        the same value, and writing it would leave the row matching
        :data:`DAMAGED` (whose left side is ``lower(btrim(...))``) forever.
        """
        assert _mod._usable_name(TENANT, TENANT.upper()) is False
        assert _mod._usable_name(TENANT.upper(), TENANT) is False

    def test_the_uuid_is_refused_through_surrounding_whitespace(self) -> None:
        assert _mod._usable_name(TENANT, f"  {TENANT}  ") is False
        assert _mod._usable_name(f" {TENANT} ", TENANT) is False

    @pytest.mark.parametrize("blank", ["", " ", "   ", "\t", "\n", " \t\n ", "\xa0"])
    def test_blank_and_whitespace_only_names_are_refused(self, blank: str) -> None:
        """Writing one of these repairs nothing — the row stays ``''``-damaged.

        ``\\xa0`` (non-breaking space) is in here on purpose. Python's
        ``str.strip()`` removes it and Postgres's default ``btrim()`` does NOT,
        so this is the one input where the two disagree — and the disagreement
        runs in the SAFE direction: Python refuses a name Postgres would have
        accepted as non-empty. Refusing too much costs one unrepaired row;
        accepting too much costs an infinite loop.
        """
        assert _mod._usable_name(TENANT, blank) is False

    def test_a_name_merely_containing_the_uuid_is_usable(self) -> None:
        """Equality, not substring. ``"tenant <uuid>"`` is a name, if an ugly one."""
        assert _mod._usable_name(TENANT, f"tenant {TENANT}") is True

    def test_a_sentinel_is_usable(self) -> None:
        """``"unknown"`` is not empty and is not the uuid, so it repairs the row.

        Licences never answers this, but the guard's job is to reason about the
        VALUE, not about who sent it.
        """
        assert _mod._usable_name(TENANT, "unknown") is True


# ---------------------------------------------------------------------------
# DAMAGED — evaluated by Postgres, over the real constant
# ---------------------------------------------------------------------------

#: The predicate under test, applied to one synthetic ``(tenant_name, tenant_id)``
#: pair. A VALUES list rather than a table: this asserts the PREDICATE, and a
#: table would drag in NOT NULL columns that have nothing to do with it.
_EVAL_DAMAGED = f"""
SELECT {_mod.DAMAGED}
  FROM (VALUES (CAST(:name AS varchar), CAST(:tid AS uuid))) AS t(tenant_name, tenant_id)
"""


async def _is_damaged(session: Any, tenant_id: str, name: str) -> bool:
    return bool(
        (await session.execute(text(_EVAL_DAMAGED), {"tid": tenant_id, "name": name})).scalar_one()
    )


class TestDamagedPredicate:
    async def test_a_bare_uuid_name_is_damaged(self, db_session) -> None:
        """The impersonation. 72,277 rows on prod look exactly like this."""
        assert await _is_damaged(db_session, TENANT, TENANT) is True

    async def test_an_uppercase_uuid_name_is_damaged(self, db_session) -> None:
        """``lower()`` on the left is what makes this match; without it the
        emitter's casing decides whether a laundered row is repairable."""
        assert await _is_damaged(db_session, TENANT, TENANT.upper()) is True

    async def test_a_uuid_name_with_padding_is_damaged(self, db_session) -> None:
        assert await _is_damaged(db_session, TENANT, f"  {TENANT} ") is True

    async def test_an_empty_name_is_damaged(self, db_session) -> None:
        """The absence. 1,005 rows on prod at the last reading, still growing."""
        assert await _is_damaged(db_session, TENANT, "") is True

    async def test_a_whitespace_only_name_is_damaged(self, db_session) -> None:
        """``btrim`` folds this into the absence case, where it belongs."""
        assert await _is_damaged(db_session, TENANT, "   ") is True

    @pytest.mark.parametrize("name", ["rmt-e2e", "n8n-dev", "unknown", "Acme B.V."])
    async def test_a_genuine_name_is_not_damaged(self, db_session, name: str) -> None:
        """The control. If this ever goes True the script overwrites real data.

        ``"unknown"`` is in here deliberately: it is an emitter-chosen sentinel
        and ``stored_tenant_name``'s "EVERY OTHER SENTINEL IS PRESERVED, NOT
        COERCED" rule means this script must leave it alone, not launder it.
        """
        assert await _is_damaged(db_session, TENANT, name) is False

    async def test_another_tenants_uuid_is_not_damaged(self, db_session) -> None:
        """The predicate is per-ROW: it compares the name to THIS row's tenant_id.

        A name that is some *other* tenant's uuid is a data-quality problem this
        script has no mandate over, and rewriting it would be inventing a fact.
        """
        assert await _is_damaged(db_session, TENANT, str(uuid.uuid4())) is False

    async def test_a_name_containing_the_uuid_is_not_damaged(self, db_session) -> None:
        assert await _is_damaged(db_session, TENANT, f"tenant {TENANT}") is False

    async def test_a_tenant_whose_real_name_is_its_uuid_matches_on_purpose(
        self, db_session
    ) -> None:
        """⛔ PINNED, NOT A BUG. Do not "fix" this.

        This is the worst case an adversarial audit could construct: a tenant
        whose genuine registered name happens to be its own uuid string. The
        predicate cannot distinguish it from the laundering, and it must not try
        — any narrowing that spared this row would spare a large share of the
        72,277 real ones too, because they are byte-identical.

        What makes it SAFE is the other half: :func:`_usable_name` refuses the
        replacement, so the row is reported under ``refused_unusable_name`` and
        left exactly as it was. Nothing is written and nothing loops.
        """
        assert await _is_damaged(db_session, TENANT, TENANT) is True
        assert _mod._usable_name(TENANT, TENANT) is False

    @pytest.mark.parametrize(
        "name",
        ["rmt-e2e", "unknown", f"tenant {TENANT}", "  padded  ", "Ünïcode Ltd", "\xa0nbsp-led"],
    )
    async def test_every_name_usable_name_accepts_leaves_the_row_undamaged(
        self, db_session, name: str
    ) -> None:
        """⭐ THE LOAD-BEARING JOIN, and the reason the batch loop terminates.

        ``_usable_name`` (Python) and ``DAMAGED`` (Postgres) are two different
        implementations of "is this a non-answer", written in two languages with
        different whitespace rules. The batch loop is only finite if they agree
        in one direction: a name accepted by the first must produce a row
        rejected by the second. Asserting each side alone would leave that seam
        untested — which is precisely where an infinite UPDATE loop lives.
        """
        assert _mod._usable_name(TENANT, name) is True
        assert await _is_damaged(db_session, TENANT, name) is False


# ---------------------------------------------------------------------------
# One predicate, four statements
# ---------------------------------------------------------------------------


class TestTheDryRunMeasuresWhatTheApplyRewrites:
    @pytest.mark.parametrize(
        "sql_name",
        ["_CENSUS_SQL", "_ARRIVALS_SQL", "_SELECT_BATCH_SQL", "_UPDATE_BATCH_SQL"],
    )
    def test_every_statement_embeds_the_same_predicate_constant(self, sql_name: str) -> None:
        """Literally the same constant, once, in each of the four statements.

        The census is what a ``--dry-run`` prints and the update is what
        ``--apply`` writes. If they can drift, the dry run is a confident number
        about a population the apply never touches — worse than no dry run, since
        it is quotable. This asserts the string itself, so editing any one of the
        four by hand fails here rather than in production.
        """
        sql = getattr(_mod, sql_name)
        assert sql.count(_mod.DAMAGED) == 1, f"{sql_name} does not embed DAMAGED verbatim"

    def test_the_census_and_the_update_agree_after_formatting(self) -> None:
        """Not just before formatting — the clamp is interpolated into both."""
        clamp = _mod._clamp_sql(True)
        census = _mod._CENSUS_SQL.format(table="billing_cost_events", clamp=clamp)
        update = _mod._UPDATE_BATCH_SQL.format(table="billing_cost_events", clamp=clamp)
        select = _mod._SELECT_BATCH_SQL.format(table="billing_cost_events", clamp=clamp)
        for rendered in (census, update, select):
            assert _mod.DAMAGED in rendered
            assert clamp in rendered

    def test_audit_log_is_not_in_tables(self) -> None:
        """Append-only. The one edit an audit log must never accept.

        ``src/db.py`` has no ``audit_log`` backfill either — its
        ``UPDATE audit_log SET tenant_name = tenant_id::text`` was DELETED
        2026-09-15 for exactly this reason, once 55 prod rows made it start
        laundering uuids into a display column. This pins the script's half of
        that policy.
        """
        assert "audit_log" not in _mod.TABLES
        assert len(_mod.TABLES) == 8


# ---------------------------------------------------------------------------
# Finding 4 — the existence check must be schema-scoped
# ---------------------------------------------------------------------------


class TestTableExistenceIsSchemaScoped:
    @pytest.mark.parametrize("sql_name", ["_TABLE_EXISTS_SQL", "_CLAMP_COLUMN_EXISTS_SQL"])
    def test_the_query_filters_on_a_schema(self, sql_name: str) -> None:
        """Without this, a name present in two schemas counts 4, ``== 2`` is
        false, and the table is SKIPPED while the run exits 0."""
        sql = getattr(_mod, sql_name)
        assert "table_schema = current_schema()" in sql

    async def test_a_real_table_is_found_in_the_current_schema(self, db_session) -> None:
        """The regression control for the filter itself.

        The test suite runs under ``conftest.TEST_SCHEMA``, which is not always
        ``public`` — so this also proves why the filter is ``current_schema()``
        and not the literal ``'public'`` a first fix would reach for.
        """
        found = (
            await db_session.execute(text(_mod._TABLE_EXISTS_SQL), {"table": "billing_cost_events"})
        ).scalar_one()
        assert int(found) == 2

    async def test_an_absent_table_counts_zero(self, db_session) -> None:
        found = (
            await db_session.execute(
                text(_mod._TABLE_EXISTS_SQL), {"table": "no_such_table_anywhere"}
            )
        ).scalar_one()
        assert int(found) == 0

    async def test_the_clamp_column_is_detected_where_it_exists_and_not_where_it_does_not(
        self, db_session
    ) -> None:
        """Measured on prod 2026-09-09: ``created_at`` on
        ``billing_cost_events`` / ``billing_metering_events`` / ``token_budgets``
        and on none of the other five."""
        assert await _mod._is_clamped(db_session, "billing_cost_events") is True
        assert await _mod._is_clamped(db_session, "billing_metering_events") is True
        assert await _mod._is_clamped(db_session, "token_budgets") is True
        assert await _mod._is_clamped(db_session, "billing_cost_daily") is False
        assert await _mod._is_clamped(db_session, "billing_metering_hourly") is False
        assert await _mod._is_clamped(db_session, "billing_metering_daily") is False
        assert await _mod._is_clamped(db_session, "token_usage") is False
        assert await _mod._is_clamped(db_session, "tenant_config") is False


# ---------------------------------------------------------------------------
# Finding 2 — the clamp and the cap
# ---------------------------------------------------------------------------


class TestTheClamp:
    def test_a_clamped_table_bounds_every_statement_by_the_run_start(self) -> None:
        assert _mod._clamp_sql(True) == f"{_mod._CLAMP_COLUMN} < :cutoff"

    def test_an_unclamped_table_gets_a_shape_preserving_true(self) -> None:
        """``TRUE`` rather than an omitted conjunct: one statement shape, and
        no ``AND`` dangling next to a f-string hole."""
        assert _mod._clamp_sql(False) == "TRUE"
        assert ":cutoff" not in _mod._clamp_sql(False)

    def test_the_arrivals_count_is_the_complement_of_the_clamp(self) -> None:
        """``>= :cutoff`` against the clamp's ``< :cutoff``.

        The two must partition the table with no gap and no overlap, or
        ``arrived_since_start`` would either double-count repaired rows or hide
        some of the writer's output.
        """
        assert f"{_mod._CLAMP_COLUMN} >= :cutoff" in _mod._ARRIVALS_SQL
        assert f"{_mod._CLAMP_COLUMN} < :cutoff" in _mod._clamp_sql(True)


class TestTheIterationCap:
    @pytest.mark.parametrize(
        ("rows", "batch", "expected"),
        [
            (0, 1000, 1),
            (1, 1000, 1),
            (999, 1000, 1),
            (1000, 1000, 1),
            (1001, 1000, 2),
            (72277, 1000, 73),
            (5, 1, 5),
        ],
    )
    def test_the_budget_is_exactly_what_the_census_justifies(
        self, rows: int, batch: int, expected: int
    ) -> None:
        """``ceil(rows / batch)``, with no slack.

        Slack would be the thing that lets a slowly-growing population pass
        unnoticed, which is the failure the cap exists to name.
        """
        assert _mod._max_passes(rows, batch) == expected

    async def test_a_normal_drain_returns_without_raising(self) -> None:
        """The control. A finite population terminates inside its budget."""
        session = _DrainingSession(total=2500)
        repaired = await _mod._repair_table(
            session, "billing_cost_events", TENANT, "rmt-e2e", 1000, True, "CUTOFF", 2500
        )
        assert repaired == 2500
        assert session.commits == 3

    async def test_an_endless_writer_raises_backfill_did_not_converge(self) -> None:
        """⭐ THE LIVE-LOCK GUARD, in its failure direction.

        This session never runs out of damaged rows — which is exactly what the
        prod ``''`` population looks like while the ``services/tenant_names.py``
        fix is undeployed and the live pods keep writing it. The old
        ``while True`` would have hammered the prod primary indefinitely; this
        must stop, and stop by NAME.
        """
        session = _EndlessSession()
        with pytest.raises(_mod.BackfillDidNotConvergeError) as excinfo:
            await _mod._repair_table(
                session, "billing_cost_daily", TENANT, "rmt-e2e", 1000, False, None, 1000
            )
        message = str(excinfo.value)
        assert "billing_cost_daily" in message
        assert TENANT in message
        assert "UNCLAMPED" in message
        assert "include_archived" in message
        # Bounded, and bounded by the census: ceil(1000/1000) writes + 1 probe.
        assert session.updates == 2

    async def test_the_cap_scales_with_the_census_not_with_a_constant(self) -> None:
        """A big tenant is not throttled by the guard meant for a runaway one."""
        session = _DrainingSession(total=9000)
        repaired = await _mod._repair_table(
            session, "billing_cost_events", TENANT, "rmt-e2e", 1000, True, "CUTOFF", 9000
        )
        assert repaired == 9000
        assert session.commits == 9

    async def test_a_clamped_run_binds_the_cutoff_and_an_unclamped_one_does_not(self) -> None:
        """The clamp is not decoration: the parameter has to reach the wire.

        A ``:cutoff`` in the SQL with no ``cutoff`` in the params raises at
        execute time; a ``cutoff`` in the params with no ``:cutoff`` in the SQL
        is a clamp that silently does nothing. Both directions are checked.
        """
        clamped = _DrainingSession(total=1)
        await _mod._repair_table(
            clamped, "billing_cost_events", TENANT, "rmt-e2e", 1000, True, "CUTOFF", 1
        )
        assert all("cutoff" in p for p in clamped.params)
        assert all(":cutoff" in s for s in clamped.statements)

        unclamped = _DrainingSession(total=1)
        await _mod._repair_table(
            unclamped, "tenant_config", TENANT, "rmt-e2e", 1000, False, None, 1
        )
        assert all("cutoff" not in p for p in unclamped.params)
        assert all(":cutoff" not in s for s in unclamped.statements)


# ---------------------------------------------------------------------------
# Fakes for the batch loop. Deliberately NOT AsyncMock: the loop's behaviour
# depends on WHICH statement it just issued, and a mock that answers everything
# identically cannot express "the select drains" versus "the select never does".
# ---------------------------------------------------------------------------


class _Result:
    def __init__(self, ids: list[str] | None = None, rowcount: int = 0) -> None:
        self._ids = ids or []
        self.rowcount = rowcount

    def scalars(self) -> _Result:
        return self

    def all(self) -> list[str]:
        return self._ids


class _RecordingSession:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.commits = 0
        self.updates = 0

    async def commit(self) -> None:
        self.commits += 1

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        sql = str(statement)
        self.statements.append(sql)
        self.params.append(params or {})
        if sql.lstrip().upper().startswith("UPDATE"):
            self.updates += 1
            return self._update(params or {})
        return self._select(params or {})

    def _select(self, params: dict[str, Any]) -> _Result:  # pragma: no cover - overridden
        raise NotImplementedError

    def _update(self, params: dict[str, Any]) -> _Result:  # pragma: no cover - overridden
        raise NotImplementedError


class _DrainingSession(_RecordingSession):
    """A finite population: each batch really removes rows from the predicate."""

    def __init__(self, total: int) -> None:
        super().__init__()
        self.remaining = total

    def _select(self, params: dict[str, Any]) -> _Result:
        take = min(int(params["batch"]), self.remaining)
        return _Result(ids=[str(uuid.uuid4()) for _ in range(take)])

    def _update(self, params: dict[str, Any]) -> _Result:
        n = len(params["ids"])
        self.remaining -= n
        return _Result(rowcount=n)


class _EndlessSession(_RecordingSession):
    """A writer that outruns the repair: the select never drains."""

    def _select(self, params: dict[str, Any]) -> _Result:
        return _Result(ids=[str(uuid.uuid4()) for _ in range(int(params["batch"]))])

    def _update(self, params: dict[str, Any]) -> _Result:
        return _Result(rowcount=len(params["ids"]))
