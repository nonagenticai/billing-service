"""``NULL_LICENSE_ID`` meant two different things; these pin the separation.

THE DEFECT UNDER TEST
---------------------
``NULL_LICENSE_ID`` is written by one rule -- ``license_id or NULL_LICENSE_ID``
-- so it means "no licence reached billing". ``license_attributed_clause`` read
it as "system/S2S cost" and dropped it from every customer aggregate, which
wrote off real customers' spend alongside genuine system cost. Measured on prod
2026-09-06: $3,386.52 of the $3,652.80 dropped belonged to 22 tenants that DO
own licences.

WHAT EACH SIDE MUST KEEP DOING
------------------------------
* A genuine system row (no licence, no customer relationship) is STILL excluded
  from the invoice/overview totals -- that exclusion is correct and stays.
* An unlicensed REAL-tenant row is now visible and attributable to its tenant.

Every money value below is DISTINCT (11.11 / 22.22 / 33.33 / 44.44 / 55.55), so
an implementation that hardcodes a total, sums the wrong bucket, or swaps the two
populations cannot agree with these assertions by coincidence.
"""

from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import types
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import insert, select, text

from models.db import (
    ATTRIBUTION_ATTRIBUTED,
    ATTRIBUTION_SYSTEM,
    ATTRIBUTION_UNKNOWN,
    ATTRIBUTION_UNLICENSED,
    NULL_LICENSE_ID,
    CostDaily,
    CostEvent,
    RequestQuotaConfig,
    classify_cost_attribution,
    is_system_license,
)
from routers.billing import compute_billing_overview
from services.aggregator import rollup_daily
from settings import get_settings
from tests.conftest import T


# This module pins the SYSTEM/UNLICENSED/ATTRIBUTED split in isolation from
# `cost_attribution_app_by_service_key` (app/service scoping is
# tests/test_rollup_attribution_app_scope.py's job, on purpose -- see that
# file). `_seed` below uses `service_key` values ("inference-router",
# "kag-graph", "karma", "worker-management") that collide with real entries in
# that map's PRODUCTION DEFAULT, but mirrors them with fake `app_name`s
# ("app-a"/"app-b") that do not match those entries' real app names. Left
# alone, `services.rollup_engine`'s app-scoped predicate would read
# owner_a/owner_b as owning NO relevant licence for those service_keys and every
# 'unlicensed' assertion below would read 'system' instead -- not because this
# file's own invariant broke, but because its fixtures were never written
# against that map. Blanking the map here restores the fallback this predicate
# uses for an unmapped `service_key`: the pre-existing tenant-wide check, which
# is exactly what this file's fixtures were built to exercise.
@pytest.fixture(autouse=True)
def _no_service_key_app_map(monkeypatch):
    monkeypatch.setenv("COST_ATTRIBUTION_APP_BY_SERVICE_KEY", "")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


NIL_TENANT = "00000000-0000-0000-0000-000000000000"

# Distinct per-tenant costs. Nothing shares a value with anything else, and no
# subtotal equals another subtotal.
COST_ATTRIBUTED = Decimal("11.11")  # owner tenant, real licence     -> attributed
COST_UNLICENSED_A = Decimal("22.22")  # owner tenant, no licence     -> unlicensed
COST_UNLICENSED_B = Decimal("33.33")  # 2nd owner tenant, no licence -> unlicensed
COST_SYSTEM_NO_LIC = Decimal("44.44")  # tenant owns nothing         -> system

# ⚠️ The NIL-tenant branch is proved in the pure-function table above and NOT in
# the DB tests: conftest's nil-tenant tripwire fails any DB test that leaves a
# nil-tenant row, and `@pytest.mark.nil_tenant` is policed as narrow. Seeding one
# here to re-prove a branch already pinned would widen that marker for no gain.


# ---------------------------------------------------------------------------
# 1. The rule itself
# ---------------------------------------------------------------------------


class TestClassifyCostAttribution:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("license_id", "tenant_id", "owns", "expected"),
        [
            # A real licence is attributed regardless of what the tenant owns.
            (uuid.uuid4(), T("owner"), True, ATTRIBUTION_ATTRIBUTED),
            (uuid.uuid4(), T("stranger"), False, ATTRIBUTION_ATTRIBUTED),
            (uuid.uuid4(), NIL_TENANT, False, ATTRIBUTION_ATTRIBUTED),
            # No licence + a tenant that owns licences = a real customer whose
            # header was dropped. THIS is the population that used to vanish.
            (NULL_LICENSE_ID, T("owner"), True, ATTRIBUTION_UNLICENSED),
            (None, T("owner"), True, ATTRIBUTION_UNLICENSED),
            # No licence + no customer relationship = genuine system/S2S.
            (NULL_LICENSE_ID, T("stranger"), False, ATTRIBUTION_SYSTEM),
            (None, T("stranger"), False, ATTRIBUTION_SYSTEM),
            # The nil tenant sentinel is system even if a mirror row exists for
            # it (prod has one: $237.30 sat under the nil tenant).
            (NULL_LICENSE_ID, NIL_TENANT, True, ATTRIBUTION_SYSTEM),
            (NULL_LICENSE_ID, None, True, ATTRIBUTION_SYSTEM),
        ],
    )
    def test_four_branches(self, license_id, tenant_id, owns, expected):
        assert (
            classify_cost_attribution(license_id, tenant_id=tenant_id, tenant_owns_licence=owns)
            == expected
        )

    @pytest.mark.unit
    def test_absence_and_system_are_now_distinguishable(self):
        """The whole point: one sentinel, two verdicts.

        ``is_system_license`` cannot tell these apart -- it returns True for both
        -- which is exactly why it must not be the only thing deciding whether
        money is reportable.
        """
        absent = classify_cost_attribution(
            NULL_LICENSE_ID, tenant_id=T("owner"), tenant_owns_licence=True
        )
        systemic = classify_cost_attribution(
            NULL_LICENSE_ID, tenant_id=T("stranger"), tenant_owns_licence=False
        )
        assert absent != systemic
        assert is_system_license(NULL_LICENSE_ID) is True  # unchanged, still the invoice gate


# ---------------------------------------------------------------------------
# 2. The rollup writes it, and the two clauses select disjoint populations
# ---------------------------------------------------------------------------


def _yesterday() -> date:
    return (datetime.now(UTC) - timedelta(days=1)).date()


def _event(tenant_id: str, license_id, cost: Decimal, service_key: str) -> dict:
    return {
        "tenant_id": tenant_id,
        "tenant_name": "",
        "license_id": license_id,
        "service_key": service_key,
        "provider": "claude",
        "input_tokens": 100,
        "output_tokens": 50,
        "cost_usd": cost,
        "created_at": datetime(
            _yesterday().year, _yesterday().month, _yesterday().day, 12, 0, tzinfo=UTC
        ),
    }


async def _seed(session_factory, *, owner_b_has_licence: bool = True) -> dict[str, str]:
    """Four tenants covering all three verdicts; returns the tenant ids."""
    owner_a, owner_b, stranger = T("own-a"), T("own-b"), T("stranger")
    real_licence = uuid.uuid4()
    async with session_factory() as session:
        mirror = [
            {
                "license_id": real_licence,
                "tenant_id": owner_a,
                "quota_monthly": 1000,
                "app_name": "app-a",
            },
        ]
        if owner_b_has_licence:
            mirror.append(
                {
                    "license_id": uuid.uuid4(),
                    "tenant_id": owner_b,
                    "quota_monthly": 1000,
                    "app_name": "app-b",
                }
            )
        await session.execute(insert(RequestQuotaConfig), mirror)
        await session.execute(
            insert(CostEvent),
            [
                _event(owner_a, real_licence, COST_ATTRIBUTED, "inference-router"),
                _event(owner_a, None, COST_UNLICENSED_A, "kag-graph"),
                _event(owner_b, None, COST_UNLICENSED_B, "karma"),
                _event(stranger, None, COST_SYSTEM_NO_LIC, "worker-management"),
            ],
        )
        await session.commit()
    return {"owner_a": owner_a, "owner_b": owner_b, "stranger": stranger}


async def _by_status(session_factory) -> dict[str, Decimal]:
    async with session_factory() as session:
        rows = (
            await session.execute(select(CostDaily.attribution_status, CostDaily.total_cost_usd))
        ).all()
    out: dict[str, Decimal] = {}
    for status, cost in rows:
        out[status] = out.get(status, Decimal("0")) + cost
    return out


class TestRollupStampsAttribution:
    @pytest.mark.integration
    async def test_three_populations_land_on_three_distinct_totals(self, db_session_factory):
        await _seed(db_session_factory)
        await rollup_daily(db_session_factory, target_date=_yesterday())

        totals = _by_status
        got = await totals(db_session_factory)
        assert got[ATTRIBUTION_ATTRIBUTED] == COST_ATTRIBUTED
        assert got[ATTRIBUTION_UNLICENSED] == COST_UNLICENSED_A + COST_UNLICENSED_B
        assert got[ATTRIBUTION_SYSTEM] == COST_SYSTEM_NO_LIC
        # No fourth bucket, and nothing left 'unknown'.
        assert set(got) == {ATTRIBUTION_ATTRIBUTED, ATTRIBUTION_UNLICENSED, ATTRIBUTION_SYSTEM}

    @pytest.mark.integration
    async def test_system_cost_is_still_excluded_and_unlicensed_is_now_selectable(
        self, db_session_factory
    ):
        """The exclusion that must NOT regress, and the visibility that is new.

        ``license_attributed_clause`` is the invoice gate: it must still drop
        every sentinel row, unlicensed ones included -- who pays for those is a
        business call this change does not make.
        """
        ids = await _seed(db_session_factory)
        await rollup_daily(db_session_factory, target_date=_yesterday())

        async with db_session_factory() as session:
            invoiceable = (
                (
                    await session.execute(
                        select(CostDaily.total_cost_usd).where(
                            CostDaily.license_attributed_clause()
                        )
                    )
                )
                .scalars()
                .all()
            )
            unlicensed = (
                await session.execute(
                    select(CostDaily.tenant_id, CostDaily.total_cost_usd).where(
                        CostDaily.unlicensed_attributable_clause()
                    )
                )
            ).all()
            systemic = (
                (
                    await session.execute(
                        select(CostDaily.total_cost_usd).where(CostDaily.system_cost_clause())
                    )
                )
                .scalars()
                .all()
            )

        # Unchanged: only the really-licensed row is invoiceable.
        assert sorted(invoiceable) == [COST_ATTRIBUTED]
        # New: unlicensed cost is visible AND carries the tenant it belongs to.
        assert dict(unlicensed) == {
            ids["owner_a"]: COST_UNLICENSED_A,
            ids["owner_b"]: COST_UNLICENSED_B,
        }
        # Unchanged in effect, newly nameable: system cost stays out of the
        # invoice, and is now countable on its own.
        assert sorted(systemic) == [COST_SYSTEM_NO_LIC]
        # The two new clauses are disjoint and neither overlaps the invoice one.
        assert {c for _, c in unlicensed}.isdisjoint(set(systemic))
        assert set(invoiceable).isdisjoint(set(systemic))

    @pytest.mark.integration
    async def test_reclassifies_when_the_tenant_acquires_a_licence(self, db_session_factory):
        """The mirror moves, so a frozen first verdict would be a lie.

        owner_b starts with NO mirror row -> its licence-less spend reads
        'system'. Give it a licence and re-run the same day's rollup: it must
        come back 'unlicensed'.

        ⚠️ The re-stamp is delivered by DELETE+INSERT, not by ON CONFLICT, and
        this docstring used to name the wrong mechanism. ``rollup_daily`` issues
        a guarded ``DELETE FROM billing_cost_daily WHERE bucket_date = …`` before
        the INSERT, so re-running the same day never reaches the ON CONFLICT
        path. Mutation M4 measured it: deleting ``attribution_status`` from the
        ON CONFLICT SET leaves this test -- and every other -- green. What this
        test actually proves is that the verdict is recomputed from the live
        mirror on every rollup rather than frozen at first write.
        """
        ids = await _seed(db_session_factory, owner_b_has_licence=False)
        await rollup_daily(db_session_factory, target_date=_yesterday())
        before = await _by_status(db_session_factory)
        assert before[ATTRIBUTION_SYSTEM] == COST_UNLICENSED_B + COST_SYSTEM_NO_LIC
        assert before[ATTRIBUTION_UNLICENSED] == COST_UNLICENSED_A

        async with db_session_factory() as session:
            await session.execute(
                insert(RequestQuotaConfig),
                [
                    {
                        "license_id": uuid.uuid4(),
                        "tenant_id": ids["owner_b"],
                        "quota_monthly": 1000,
                        "app_name": "app-b",
                    }
                ],
            )
            await session.commit()

        await rollup_daily(db_session_factory, target_date=_yesterday())
        after = await _by_status(db_session_factory)
        assert after[ATTRIBUTION_UNLICENSED] == COST_UNLICENSED_A + COST_UNLICENSED_B
        assert after[ATTRIBUTION_SYSTEM] == COST_SYSTEM_NO_LIC


# ---------------------------------------------------------------------------
# 3. The report where the money used to vanish
# ---------------------------------------------------------------------------


class TestOverviewReportsTheSplit:
    @pytest.mark.integration
    async def test_unlicensed_is_visible_per_tenant_without_moving_an_invoice_line(
        self, db_session_factory, db_session
    ):
        ids = await _seed(db_session_factory)
        await rollup_daily(db_session_factory, target_date=_yesterday())
        month = _yesterday().strftime("%Y-%m")

        overview = await compute_billing_overview(month, db_session)
        entries = {e.tenant_id: e for e in overview.tenants}

        # VISIBLE: each owner's unlicensed spend, attributed to that owner, with
        # two different values so a single-bucket implementation cannot pass.
        assert entries[ids["owner_a"]].unlicensed_cost_usd == COST_UNLICENSED_A
        assert entries[ids["owner_b"]].unlicensed_cost_usd == COST_UNLICENSED_B
        assert overview.platform_total_unlicensed_usd == COST_UNLICENSED_A + COST_UNLICENSED_B
        # System cost is reported separately and is NOT folded in with it.
        assert overview.platform_total_system_usd == COST_SYSTEM_NO_LIC
        assert overview.platform_total_unlicensed_usd != overview.platform_total_system_usd

        # NOT BILLED: internal cost still counts only the licence-attributed row.
        # If unlicensed cost had leaked onto the invoice side this would be
        # COST_ATTRIBUTED + 22.22 + 33.33.
        assert overview.platform_total_internal_usd == COST_ATTRIBUTED
        assert entries[ids["owner_a"]].internal_cost_usd == COST_ATTRIBUTED
        assert entries[ids["owner_b"]].internal_cost_usd == Decimal("0")
        # The system-only tenant contributes nothing billable anywhere.
        assert ids["stranger"] not in entries or entries[ids["stranger"]].internal_cost_usd == 0

    @pytest.mark.integration
    async def test_a_tenant_whose_only_spend_is_unlicensed_still_appears(
        self, db_session_factory, db_session
    ):
        """The invisibility being fixed, in its purest form.

        owner_b has NO licence-attributed cost and no metering rows, so before
        this change it was absent from the overview entirely and its $33.33 was
        reported by nothing at all.
        """
        ids = await _seed(db_session_factory)
        await rollup_daily(db_session_factory, target_date=_yesterday())
        overview = await compute_billing_overview(_yesterday().strftime("%Y-%m"), db_session)

        entry = next(e for e in overview.tenants if e.tenant_id == ids["owner_b"])
        assert entry.internal_cost_usd == Decimal("0")
        assert entry.customer_cost_usd == Decimal("0")
        assert entry.unlicensed_cost_usd == COST_UNLICENSED_B


# ---------------------------------------------------------------------------
# 4. The wire values, and the one place they are NOT written as constants
# ---------------------------------------------------------------------------
#
# ⚠️ Everything above imports the ATTRIBUTION_* constants to say what it expects.
# That makes every assertion above BLIND to a rename: change
# ATTRIBUTION_UNLICENSED to "unlicenced" and the expectation renames itself, so
# the suite stays green while the value on the wire, in the database, and in the
# JSON the dashboard reads all silently change. Mutation M6 measured exactly
# that -- it SURVIVED, 38 passed, 0 failed.
#
# A test that imports the value it is checking cannot detect that the value
# changed. These pin the literals instead.


class TestTheWireValuesThemselves:
    @pytest.mark.unit
    def test_the_four_status_values_are_exactly_these_strings(self):
        """The stored/serialised vocabulary, written out, not imported.

        ⛔ Do NOT "fix" a failure here by editing the right-hand side. These are
        persisted in ``billing_cost_daily.attribution_status`` on every rollup
        row and are backfilled by alembic 0020 as bare SQL literals. Changing
        one is a data migration, not a rename.
        """
        assert ATTRIBUTION_ATTRIBUTED == "attributed"
        assert ATTRIBUTION_UNLICENSED == "unlicensed"
        assert ATTRIBUTION_SYSTEM == "system"
        assert ATTRIBUTION_UNKNOWN == "unknown"
        # varchar(16): a longer value raises StringDataRightTruncation on write.
        for value in (
            ATTRIBUTION_ATTRIBUTED,
            ATTRIBUTION_UNLICENSED,
            ATTRIBUTION_SYSTEM,
            ATTRIBUTION_UNKNOWN,
        ):
            assert len(value) <= 16

    @pytest.mark.unit
    def test_the_migration_backfills_the_same_literals_the_code_imports(self):
        """The stranded-literal check.

        Migration 0020's backfill is raw SQL: the statuses are bare string
        literals, so a rename of the Python constant leaves them behind and
        every importing test renames its own expectation and stays green. This
        joins the two sides.
        """
        sql = _migration_0020()._BACKFILL
        for value in (ATTRIBUTION_ATTRIBUTED, ATTRIBUTION_UNLICENSED, ATTRIBUTION_SYSTEM):
            assert f"'{value}'" in sql, (
                f"alembic 0020 backfills no {value!r} literal -- the constant was "
                f"renamed and the migration SQL was left stranded."
            )

    @pytest.mark.integration
    async def test_the_value_the_rollup_actually_stores_is_the_literal(self, db_session_factory):
        """Read the column back as raw text and compare to literals.

        Proves the string that reaches Postgres -- not the symbol the test
        imported -- is the one the dashboard, the API and the backfill agree on.
        """
        await _seed(db_session_factory)
        await rollup_daily(db_session_factory, target_date=_yesterday())
        async with db_session_factory() as session:
            stored = {
                row[0]
                for row in (
                    await session.execute(
                        text("SELECT DISTINCT attribution_status::text FROM billing_cost_daily")
                    )
                ).all()
            }
        assert stored == {"attributed", "unlicensed", "system"}


# ---------------------------------------------------------------------------
# 5. The migration is actually RUN -- the only test in the suite that runs one
# ---------------------------------------------------------------------------
#
# ⚠️ conftest builds the schema with ``Base.metadata.create_all``, so NO test in
# this repository executes alembic 0020 unless it does what this one does. The
# migration's docstring used to claim its backfill was "checked against
# [the Python rule] by running the migration in the test database". It was not.
# This is that check, made real.

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
REVISION = "0020_cost_daily_attribution"
PREVIOUS_REVISION = "0019_unrolled_truncate_gate"
_INDEX = "ix_cost_daily_attribution_status"


def _migration_0020() -> types.ModuleType:
    """Import the migration file directly; it is not on the import path."""
    path = REPO_ROOT / "alembic" / "versions" / f"{REVISION}.py"
    spec = importlib.util.spec_from_file_location(REVISION, path)
    assert spec and spec.loader, f"cannot load {path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _upgrade_to_0020() -> None:
    cfg = Config()
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    command.stamp(cfg, PREVIOUS_REVISION)
    command.upgrade(cfg, REVISION)


class TestMigration0020ActuallyRuns:
    @pytest.mark.integration
    async def test_the_chain_is_linear_and_0020_follows_0019(self):
        """Two branches both authored a ``0019`` off ``0018_truncate_guard_widen``.

        git merged the two files with no textual conflict and left TWO alembic
        heads, so ``alembic upgrade head`` failed outright. Only running alembic
        showed it.

        The invariant is LINEARITY, not that 0020 is forever the tip. This used
        to assert ``get_heads() == [REVISION]``, which made it a tripwire every
        later migration had to come back and edit — and alembic
        0021_cost_event_actor_pricing is exactly that later migration. A test
        whose only failure mode is "someone did the normal thing" gets edited
        on autopilot, which is how a real branch would eventually be waved
        through. The hazard worth pinning is a BRANCHED chain (two heads = an
        ambiguous upgrade path); 0020's own position in it is pinned by its
        ``down_revision`` below — same fix already applied to the 0010/0012/0013
        migration tests for the same reason.
        """
        from alembic.script import ScriptDirectory

        cfg = Config(str(REPO_ROOT / "alembic.ini"))
        cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
        script = ScriptDirectory.from_config(cfg)
        heads = list(script.get_heads())
        assert len(heads) == 1, f"alembic chain has branched: {heads}"
        assert script.get_revision(REVISION).down_revision == PREVIOUS_REVISION

    @pytest.mark.integration
    async def test_0020_backfill_reproduces_the_python_rule(self, db_session, db_session_factory):
        """Run the real migration and compare its SQL verdict to Python's.

        ⛔ The column is DROPPED first. ``create_all`` has already added it, so a
        test that only upgrades and then asserts "the column exists" passes
        against a migration that did nothing.

        Rows are seeded through the ordinary rollup (so they are real rollup
        rows), then the column is dropped -- discarding every value the Python
        rule wrote -- and the migration re-adds and re-derives it from
        ``request_quota_config`` alone. Agreement is therefore the SQL twin
        agreeing with ``classify_cost_attribution``, not with itself.
        """
        await _seed(db_session_factory)
        await rollup_daily(db_session_factory, target_date=_yesterday())

        # What the Python rule says, captured before the column is destroyed.
        async with db_session_factory() as session:
            owners = {
                str(r[0])
                for r in (
                    await session.execute(
                        text(
                            "SELECT DISTINCT tenant_id::text FROM request_quota_config"
                            f" WHERE license_id <> '{NIL_TENANT}'::uuid"
                        )
                    )
                ).all()
            }
            rows = (
                await session.execute(
                    text(
                        "SELECT tenant_id::text, license_id::text, total_cost_usd"
                        " FROM billing_cost_daily"
                    )
                )
            ).all()
        assert rows, "nothing rolled up -- the comparison would be vacuous"
        # ⚠️ license_id arrives as ::text; classify_cost_attribution compares
        # against a uuid.UUID sentinel, so a str would never equal it and every
        # row would read 'attributed'. Convert, or the comparison is vacuous.
        expected = {
            (tenant, lic): classify_cost_attribution(
                uuid.UUID(lic), tenant_id=tenant, tenant_owns_licence=tenant in owners
            )
            for tenant, lic, _ in rows
        }
        # The three verdicts must all be represented, or agreement proves little.
        assert set(expected.values()) == {
            ATTRIBUTION_ATTRIBUTED,
            ATTRIBUTION_UNLICENSED,
            ATTRIBUTION_SYSTEM,
        }

        await db_session.execute(text(f"DROP INDEX IF EXISTS {_INDEX}"))
        await db_session.execute(
            text("ALTER TABLE billing_cost_daily DROP COLUMN IF EXISTS attribution_status")
        )
        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()

        await asyncio.to_thread(_upgrade_to_0020)

        async with db_session_factory() as session:
            got = {
                (t, lic): status
                for t, lic, status in (
                    await session.execute(
                        text(
                            "SELECT tenant_id::text, license_id::text,"
                            " attribution_status::text FROM billing_cost_daily"
                        )
                    )
                ).all()
            }
            index_exists = (
                await session.execute(
                    text("SELECT count(*) FROM pg_indexes WHERE indexname = :n"),
                    {"n": _INDEX},
                )
            ).scalar()

        assert got == expected, (
            "the migration's SQL backfill disagrees with classify_cost_attribution -- "
            "the two are written in different languages and have drifted"
        )
        assert ATTRIBUTION_UNKNOWN not in set(got.values()), (
            "the backfill left rows at 'unknown'; it did not run"
        )
        assert index_exists == 1, f"{_INDEX} missing after upgrade"

        await db_session.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await db_session.commit()
