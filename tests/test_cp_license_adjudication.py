"""CommonPractice asserts a ``license_id``; billing may not simply believe it.

THE DEFECT UNDER TEST
---------------------
``services.cost_flusher`` accepted ANY well-formed uuid as ``license_id`` --
``_parse_uuid`` is a stdlib parse and nothing else -- so an emitter could name
any licence in the estate and be billed against it.
``services.attribution_audit`` measured the consequence on prod 2026-08-25 over
a 7-day window:

    licence present AND owned by the event's tenant      7,562 events   $86.76
    licence present AND owned by SOMEONE ELSE           11,407 events  $363.56

i.e. **81% of attributed spend named a foreign licence**. Per that module: "That
is worse than a NULL: a NULL is honestly unknown, a mismatch is confidently
wrong."

WHAT THE FIX MUST DO, AND THE PART THAT IS EASY TO GET WRONG
-----------------------------------------------------------
The row must be INGESTED (the provider was already paid; dropping it converts an
attribution fault into an unmetered one, which is unrecoverable), VISIBLE,
COUNTED, and NOT INVOICED.

⛔ ``attribution_status`` IS NOT WHAT MAKES IT NOT-INVOICED, and a test suite
that only checked the label would pass against a change that still bills the
row. The invoice gate is ``CostDaily.license_attributed_clause()`` --
``license_id IS NOT NULL AND license_id != NULL_LICENSE_ID`` -- a DENYLIST ON
``license_id`` that never reads ``attribution_status``. So these tests assert the
REMOVAL of the id and the STATUS separately, and section 3 asserts the invoice
predicate itself rather than the label that explains it.

Every money value below is DISTINCT (61.61 / 62.62 / 63.63 / 64.64), so an
implementation that sums the wrong bucket or swaps two populations cannot agree
with these assertions by coincidence.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import insert, select, text

from models.db import (
    ACTIVE_LICENSE_STATUS,
    ATTRIBUTION_ATTRIBUTED,
    ATTRIBUTION_REFUSED_LICENSE,
    ATTRIBUTION_SYSTEM,
    ATTRIBUTION_UNLICENSED,
    LICENSE_REFUSED_COST_SOURCE_MARKER,
    CostDaily,
    CostEvent,
    RequestQuotaConfig,
    classify_cost_attribution,
    cost_source_marks_refusal,
)
from services import license_resolver
from services.aggregator import rollup_daily
from services.cost_flusher import (
    LICENSE_ADJUDICATED_SERVICE_KEYS,
    LICENSE_REFUSAL_REASONS,
    adjudicate_asserted_license,
    birth_license_refused_series,
    flush_costs,
)
from settings import get_settings
from tests.conftest import T

CP = "commonpractice"
OTHER_EMITTER = "inference-router"

# Distinct per-population costs. Nothing shares a value with anything else.
COST_ACCEPTED = Decimal("61.61")  # CP, own active licence      -> attributed
COST_FOREIGN = Decimal("62.62")  # CP, another tenant's licence -> refused
COST_UNKNOWN = Decimal("63.63")  # CP, licence not in the mirror-> refused
COST_OTHER_EMITTER = Decimal("64.64")  # not CP                 -> untouched


@pytest.fixture(autouse=True)
def _clear_resolver_cache():
    """The resolver's maps are MODULE-GLOBAL and TTL'd for 300s.

    ⛔ Without this every test after the first reads the first one's mirror.
    ``get_owner_map`` caches on ``time.monotonic()``, which does not advance
    between tests in any useful way, so a stale map would make a licence seeded
    in THIS test read as ``unknown_license`` -- a green "refusal" produced by
    the fixture rather than by the code under test, which is the most dangerous
    kind of pass.
    """
    license_resolver.reset_cache()
    yield
    license_resolver.reset_cache()


@pytest.fixture(autouse=True)
def _no_service_key_app_map(monkeypatch):
    """Blank the app map, for the reason ``tests/test_attribution_status.py`` states.

    ``commonpractice=Common Practice`` IS in the production default of
    ``COST_ATTRIBUTION_APP_BY_SERVICE_KEY``, so left alone the rollup's
    app-scoped ownership predicate would ask "does this tenant own a *Common
    Practice* licence?" against fixtures that never named an app, and the
    accepted/unlicensed assertions below would read 'system' -- a failure of the
    fixture, not of the invariant.
    """
    monkeypatch.setenv("COST_ATTRIBUTION_APP_BY_SERVICE_KEY", "")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# 1. The adjudication rule itself -- pure, no database
# ---------------------------------------------------------------------------


OWNER = T("cp-owner")
STRANGER = T("cp-stranger")
LIC_OWNED = uuid.UUID("019f82c0-0000-7000-8000-00000000aa01")
LIC_FOREIGN = uuid.UUID("019f82c0-0000-7000-8000-00000000aa02")
LIC_REVOKED = uuid.UUID("019f82c0-0000-7000-8000-00000000aa03")
LIC_ABSENT = uuid.UUID("019f82c0-0000-7000-8000-00000000aa99")

OWNER_MAP = {
    LIC_OWNED: (OWNER, ACTIVE_LICENSE_STATUS),
    LIC_FOREIGN: (STRANGER, ACTIVE_LICENSE_STATUS),
    LIC_REVOKED: (OWNER, "revoked"),
}


class TestAdjudicateAssertedLicense:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("license_id", "tenant_id", "service_key", "expected"),
        [
            # --- REFUSALS, the three reasons kept apart on purpose ---------
            (LIC_FOREIGN, OWNER, CP, "foreign_tenant"),
            (LIC_ABSENT, OWNER, CP, "unknown_license"),
            (LIC_REVOKED, OWNER, CP, "inactive_license"),
            # --- ACCEPTS ---------------------------------------------------
            # The tenant's own active licence.
            (LIC_OWNED, OWNER, CP, None),
            # ⛔ NOT CP: every other emitter is still believed. This is the
            # narrowing that makes the change shippable.
            (LIC_FOREIGN, OWNER, OTHER_EMITTER, None),
            (LIC_ABSENT, OWNER, OTHER_EMITTER, None),
            # ⛔ ABSENCE IS NOT A REFUSAL. Nothing was asserted, so there is
            # nothing to be wrong about; this population is already owned by
            # license_resolver + billing_license_unattributed_persisted_total.
            (None, OWNER, CP, None),
            # A missing service_key cannot be in the adjudicated set.
            (LIC_FOREIGN, OWNER, None, None),
            (LIC_FOREIGN, OWNER, "", None),
        ],
    )
    def test_every_branch(self, license_id, tenant_id, service_key, expected):
        """⚠️ MUTATION TARGET: any early-return in ``adjudicate_asserted_license``.

        Deleting the ``service_key not in adjudicated_service_keys`` guard turns
        rows 5-6 into refusals; deleting the ``license_id is None`` guard turns
        row 7 into ``unknown_license``; swapping the tenant and status checks
        turns row 3 into ``foreign_tenant``.
        """
        assert (
            adjudicate_asserted_license(
                license_id,
                tenant_id=tenant_id,
                service_key=service_key,
                owner_map=OWNER_MAP,
            )
            == expected
        )

    @pytest.mark.unit
    def test_a_tenant_spelled_in_upper_case_is_still_its_own_licence(self):
        """⚠️ MUTATION TARGET: the ``.casefold()`` on either side of the compare.

        A bare ``!=`` refuses a tenant for spelling its own uuid in upper case --
        the same hole ``auth_deps.enforce_tenant_isolation`` canonicalises both
        sides to avoid.
        """
        assert (
            adjudicate_asserted_license(
                LIC_OWNED,
                tenant_id=OWNER.upper(),
                service_key=CP,
                owner_map=OWNER_MAP,
            )
            is None
        )

    @pytest.mark.unit
    def test_an_unrecognised_upstream_status_fails_closed(self):
        """⚠️ MUTATION TARGET: ``status != ACTIVE`` rewritten as ``status == 'revoked'``.

        ``license_resolver`` documents this choice on its own predicate: "``==
        active`` and never ``!= revoked``, so an unrecognised upstream status
        fails CLOSED". A status nobody has seen before must not resolve by
        default.
        """
        weird = {LIC_OWNED: (OWNER, "quiescent")}
        assert (
            adjudicate_asserted_license(LIC_OWNED, tenant_id=OWNER, service_key=CP, owner_map=weird)
            == "inactive_license"
        )

    @pytest.mark.unit
    def test_every_reason_it_can_return_is_in_the_declared_domain(self):
        """The metric's ``reason`` label domain and the function cannot drift.

        ``LICENSE_REFUSAL_REASONS`` is what ``birth_license_refused_series``
        births, so a reason the function returns but the tuple omits is a series
        that is never born and an alert that is vacuously green for that class.
        """
        produced = {
            adjudicate_asserted_license(lic, tenant_id=OWNER, service_key=CP, owner_map=OWNER_MAP)
            for lic in (LIC_FOREIGN, LIC_ABSENT, LIC_REVOKED)
        }
        assert produced == set(LICENSE_REFUSAL_REASONS)

    @pytest.mark.unit
    def test_the_birthed_series_is_the_reachable_set(self):
        born = birth_license_refused_series()
        assert set(born) == {
            (key, reason)
            for key in LICENSE_ADJUDICATED_SERVICE_KEYS
            for reason in LICENSE_REFUSAL_REASONS
        }
        assert born, "nothing birthed -- every alert over this counter is vacuous"


# ---------------------------------------------------------------------------
# 2. The classifier, and the marker that carries the verdict to the rollup
# ---------------------------------------------------------------------------


class TestRefusedLicenseStatus:
    @pytest.mark.unit
    def test_refusal_wins_over_every_other_branch(self):
        """⚠️ MUTATION TARGET: the position of the ``if license_refused`` branch.

        A refused row arrives at the rollup with ``license_id`` ALREADY NULLED --
        that removal is what withholds the invoice line -- so if this branch is
        moved below the others the row reads 'unlicensed' or 'system' and the
        refusal is erased by the very mechanism that enforced it.
        """
        # Nulled id + a tenant that owns licences would otherwise be 'unlicensed'.
        assert (
            classify_cost_attribution(
                None, tenant_id=OWNER, tenant_owns_licence=True, license_refused=True
            )
            == ATTRIBUTION_REFUSED_LICENSE
        )
        # ...and a tenant that owns nothing would otherwise be 'system'.
        assert (
            classify_cost_attribution(
                None, tenant_id=STRANGER, tenant_owns_licence=False, license_refused=True
            )
            == ATTRIBUTION_REFUSED_LICENSE
        )

    @pytest.mark.unit
    def test_the_default_leaves_every_existing_caller_byte_identical(self):
        """⚠️ MUTATION TARGET: ``license_refused: bool = False`` -> ``= True``.

        Every pre-existing call site omits the argument. A default of True would
        silently reclassify the entire estate.
        """
        assert (
            classify_cost_attribution(LIC_OWNED, tenant_id=OWNER, tenant_owns_licence=True)
            == ATTRIBUTION_ATTRIBUTED
        )
        assert (
            classify_cost_attribution(None, tenant_id=OWNER, tenant_owns_licence=True)
            == ATTRIBUTION_UNLICENSED
        )
        assert (
            classify_cost_attribution(None, tenant_id=STRANGER, tenant_owns_licence=False)
            == ATTRIBUTION_SYSTEM
        )

    @pytest.mark.unit
    def test_the_marker_predicate_is_substring_not_suffix(self):
        """⚠️ MUTATION TARGET: ``in`` rewritten as ``endswith``.

        ``cost_source`` already carries one composable suffix (``+resolved``), so
        a marker followed by anything must still match. ``endswith`` fails toward
        "not refused", which puts the row back on an invoice.
        """
        assert cost_source_marks_refusal(f"unknown{LICENSE_REFUSED_COST_SOURCE_MARKER}")
        assert cost_source_marks_refusal(f"unknown{LICENSE_REFUSED_COST_SOURCE_MARKER}+resolved")
        assert not cost_source_marks_refusal("unknown")
        assert not cost_source_marks_refusal("unknown+resolved")
        assert not cost_source_marks_refusal(None)
        assert not cost_source_marks_refusal("")

    @pytest.mark.unit
    def test_the_wire_value_and_the_marker_fit_their_columns(self):
        """The stored vocabulary, written out, not imported.

        ⛔ Do NOT "fix" a failure here by editing the right-hand side.
        ``billing_cost_daily.attribution_status`` is ``varchar(16)`` and
        ``billing_cost_events.cost_source`` is ``varchar(64)``; a longer value
        raises StringDataRightTruncation on write. Same discipline (and the same
        surviving-mutant lesson, M6) as
        ``tests/test_attribution_status.py::TestTheWireValuesThemselves``: a test
        that imports the value it is checking cannot detect that the value
        changed.
        """
        assert ATTRIBUTION_REFUSED_LICENSE == "refused_license"
        assert len(ATTRIBUTION_REFUSED_LICENSE) <= 16
        assert LICENSE_REFUSED_COST_SOURCE_MARKER == "+license_refused"
        # The longest cost_source this can produce: the flusher's own default
        # 'unknown', plus the marker, plus the resolver's other suffix.
        assert len(f"unknown{LICENSE_REFUSED_COST_SOURCE_MARKER}+resolved") <= 64


# ---------------------------------------------------------------------------
# 3. End to end: the flusher removes the id, the rollup names the reason,
#    and the INVOICE PREDICATE -- not the label -- drops the row
# ---------------------------------------------------------------------------


def _yesterday() -> date:
    return (datetime.now(UTC) - timedelta(days=1)).date()


def _noon_yesterday() -> datetime:
    d = _yesterday()
    return datetime(d.year, d.month, d.day, 12, 0, tzinfo=UTC)


async def _seed_mirror(session_factory) -> None:
    """The licence mirror: one licence for OWNER, one for STRANGER, one revoked."""
    async with session_factory() as session:
        await session.execute(
            insert(RequestQuotaConfig),
            [
                {
                    "license_id": LIC_OWNED,
                    "tenant_id": OWNER,
                    "quota_monthly": 1000,
                    "app_name": "cp",
                    "status": ACTIVE_LICENSE_STATUS,
                },
                {
                    "license_id": LIC_FOREIGN,
                    "tenant_id": STRANGER,
                    "quota_monthly": 1000,
                    "app_name": "cp",
                    "status": ACTIVE_LICENSE_STATUS,
                },
            ],
        )
        await session.commit()


def _wire_event(license_id, cost: Decimal, service_key: str) -> dict:
    """One raw cost event as it arrives off the ``billing:costs`` Redis stream."""
    return {
        "tenant_id": OWNER,
        "license_id": str(license_id),
        "service_key": service_key,
        "provider": "claude",
        "model": "claude-3-5-sonnet-20241022",
        "input_tokens": "100",
        "output_tokens": "50",
        "cost_usd": str(cost),
        "cost_source": "unknown",
    }


@pytest.fixture
def _no_archival_lookup(monkeypatch):
    """``is_tenant_archived`` makes an HTTP call to licences; stub it.

    Not under test here and documented fail-open, so a stub that answers "not
    archived" is the same answer an unreachable licences service would produce --
    it removes a network dependency without removing a branch.
    """

    async def _never_archived(_tenant_id, _settings):
        return False

    monkeypatch.setattr("services.cost_flusher.is_tenant_archived", _never_archived)


class TestFlusherRemovesTheId:
    @pytest.mark.integration
    async def test_a_foreign_licence_is_stripped_and_marked(
        self, db_session_factory, _no_archival_lookup
    ):
        """⚠️ MUTATION TARGET: ``row["license_id"] = None`` in the adjudication block.

        Deleting that ONE line leaves the label intact and every status assertion
        in this file green -- while the row keeps a real licence in ``license_id``
        and is therefore invoiced by ``license_attributed_clause()``. That is why
        this test reads the ID back and not the status.
        """
        await _seed_mirror(db_session_factory)
        buf = [_wire_event(LIC_FOREIGN, COST_FOREIGN, CP)]
        assert await flush_costs(buf, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalars().one()
        assert row.license_id is None, "the refused licence is still on the row"
        assert cost_source_marks_refusal(row.cost_source)
        # ⭐ The MONEY RECORD SURVIVES. The provider was already paid; a refusal
        # that deleted the row would be an unmetered charge, which is worse.
        assert row.tenant_id == OWNER
        assert row.input_tokens == 100

    @pytest.mark.integration
    async def test_an_unknown_licence_is_stripped_and_marked(
        self, db_session_factory, _no_archival_lookup
    ):
        await _seed_mirror(db_session_factory)
        buf = [_wire_event(LIC_ABSENT, COST_UNKNOWN, CP)]
        assert await flush_costs(buf, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalars().one()
        assert row.license_id is None
        assert cost_source_marks_refusal(row.cost_source)

    @pytest.mark.integration
    async def test_the_tenants_own_licence_is_untouched(
        self, db_session_factory, _no_archival_lookup
    ):
        """The control. Without it every assertion above is satisfied by a
        flusher that strips EVERY licence, which would be a far bigger defect
        wearing this change's clothes."""
        await _seed_mirror(db_session_factory)
        buf = [_wire_event(LIC_OWNED, COST_ACCEPTED, CP)]
        assert await flush_costs(buf, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalars().one()
        assert row.license_id == LIC_OWNED
        assert not cost_source_marks_refusal(row.cost_source)
        assert row.cost_source == "unknown"

    @pytest.mark.integration
    async def test_a_non_cp_emitter_keeps_a_foreign_licence(
        self, db_session_factory, _no_archival_lookup
    ):
        """⚠️ MUTATION TARGET: ``LICENSE_ADJUDICATED_SERVICE_KEYS`` widened.

        The narrowing is what makes this shippable: every other emitter is still
        believed. Arming fleet-wide against a mirror that is 5-minutes-stale by
        construction would strip real attribution off real invoices on a sync
        lag.
        """
        await _seed_mirror(db_session_factory)
        buf = [_wire_event(LIC_FOREIGN, COST_OTHER_EMITTER, OTHER_EMITTER)]
        assert await flush_costs(buf, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalars().one()
        assert row.license_id == LIC_FOREIGN, (
            "adjudication leaked outside LICENSE_ADJUDICATED_SERVICE_KEYS"
        )
        assert not cost_source_marks_refusal(row.cost_source)

    @pytest.mark.integration
    async def test_a_mirror_read_failure_accepts_and_never_loses_the_batch(
        self, db_session_factory, _no_archival_lookup, monkeypatch
    ):
        """⛔ THE FAIL-SOFT DIRECTION IS ACCEPT, AND IT IS A DECISION.

        The events are already priced and claimed by the time this block runs, so
        a map-build error must cost the attribution HINT, never the batch --
        the same contract the licence-resolution and app-derivation blocks either
        side of it carry.

        ⚠️ MUTATION TARGET: remove the ``try/except`` around the adjudication
        block, or make the failure path strip the licence anyway.

        Removing the guard loses the whole flush (the row count below goes to 0
        or the call raises). Stripping on failure is the more tempting mistake
        and the more expensive one: a transient mirror read failure would then
        pull real attribution off real invoices estate-wide, for every CP event
        in flight, with nothing saying why.
        """

        async def _boom(_session, **_kwargs):
            raise RuntimeError("mirror unavailable")

        monkeypatch.setattr("services.cost_flusher.get_owner_map", _boom)

        await _seed_mirror(db_session_factory)
        buf = [_wire_event(LIC_FOREIGN, COST_FOREIGN, CP)]
        assert await flush_costs(buf, db_session_factory, None) == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(CostEvent))).scalars().one()
        assert row.license_id == LIC_FOREIGN, (
            "a failed mirror read stripped attribution -- the expensive direction"
        )
        assert not cost_source_marks_refusal(row.cost_source)


async def _insert_post_flusher_rows(session_factory) -> None:
    """The four populations AS THE FLUSHER LEAVES THEM, ready for the rollup.

    Inserted directly rather than through ``flush_costs`` so the rollup's
    behaviour is pinned independently of the flusher's -- the two halves of this
    change must each fail on their own, or a single test could go green with one
    of them deleted.
    """
    async with session_factory() as session:
        await session.execute(
            insert(CostEvent),
            [
                # accepted: real licence, untouched cost_source
                {
                    "tenant_id": OWNER,
                    "tenant_name": "",
                    "license_id": LIC_OWNED,
                    "service_key": CP,
                    "provider": "claude",
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cost_usd": COST_ACCEPTED,
                    "cost_source": "unknown",
                    "created_at": _noon_yesterday(),
                },
                # refused (foreign): id stripped, marker present
                {
                    "tenant_id": OWNER,
                    "tenant_name": "",
                    "license_id": None,
                    "service_key": CP,
                    "provider": "claude",
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cost_usd": COST_FOREIGN,
                    "cost_source": f"unknown{LICENSE_REFUSED_COST_SOURCE_MARKER}",
                    "created_at": _noon_yesterday(),
                },
                # honestly unattributed: no licence, NO marker. The population
                # this must never be collapsed into.
                {
                    "tenant_id": OWNER,
                    "tenant_name": "",
                    "license_id": None,
                    "service_key": CP,
                    "provider": "gemma",
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cost_usd": COST_UNKNOWN,
                    "cost_source": "unknown",
                    "created_at": _noon_yesterday(),
                },
            ],
        )
        await session.commit()


class TestRollupNamesTheRefusalAndTheInvoiceDropsIt:
    @pytest.mark.integration
    async def test_refused_is_its_own_status_and_not_folded_into_unlicensed(
        self, db_session_factory
    ):
        await _seed_mirror(db_session_factory)
        await _insert_post_flusher_rows(db_session_factory)
        await rollup_daily(db_session_factory, target_date=_yesterday())

        async with db_session_factory() as session:
            rows = (
                await session.execute(
                    select(CostDaily.attribution_status, CostDaily.total_cost_usd)
                )
            ).all()
        totals: dict[str, Decimal] = {}
        for status, cost in rows:
            totals[status] = totals.get(status, Decimal("0")) + cost

        assert totals[ATTRIBUTION_REFUSED_LICENSE] == COST_FOREIGN
        assert totals[ATTRIBUTION_ATTRIBUTED] == COST_ACCEPTED
        # ⛔ THE SEPARATION THAT IS THE WHOLE POINT. OWNER owns a licence, so its
        # licence-less, UNMARKED row is 'unlicensed' -- an honest absence. The
        # marked row must NOT join it.
        assert totals[ATTRIBUTION_UNLICENSED] == COST_UNKNOWN
        assert totals[ATTRIBUTION_REFUSED_LICENSE] != totals[ATTRIBUTION_UNLICENSED]

    @pytest.mark.integration
    async def test_the_invoice_predicate_drops_it_and_the_accepted_row_survives(
        self, db_session_factory
    ):
        """⛔ THE ASSERTION THAT ACTUALLY PROVES "NOT INVOICED".

        Every other test in this file reads a LABEL. This one reads the gate:
        ``CostDaily.license_attributed_clause()`` is what
        ``services.invoices`` and ``services.billing`` bill on, and it never
        looks at ``attribution_status``.
        """
        await _seed_mirror(db_session_factory)
        await _insert_post_flusher_rows(db_session_factory)
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
        assert sorted(invoiceable) == [COST_ACCEPTED], (
            "a refused licence reached the invoiceable population"
        )
        assert COST_FOREIGN not in invoiceable

    @pytest.mark.integration
    async def test_the_value_that_reaches_postgres_is_the_literal(self, db_session_factory):
        """Read the column back as raw text.

        Proves the string in the database -- not the symbol this file imported --
        is the one the invoice's ``excluded_by_status`` disclosure will carry.
        """
        await _seed_mirror(db_session_factory)
        await _insert_post_flusher_rows(db_session_factory)
        await rollup_daily(db_session_factory, target_date=_yesterday())

        async with db_session_factory() as session:
            stored = {
                r[0]
                for r in (
                    await session.execute(
                        text("SELECT DISTINCT attribution_status::text FROM billing_cost_daily")
                    )
                ).all()
            }
        assert "refused_license" in stored
        assert stored == {"attributed", "unlicensed", "refused_license"}
