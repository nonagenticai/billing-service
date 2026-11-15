"""Tests for services.event_flusher (flush_events + monthly-usage upserts)."""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from models.db import (
    NULL_PLAN_ID,
    MeteringEvent,
    MeteringMonthlyUsage,
    ProcessedEventMessage,
)
from services.aggregator import (
    _parse_decimal,
    _parse_int,
    _parse_uuid,
    flush_events,
)
from services.event_flusher import (
    _STATUS_COLUMN_MAX,
    _STATUS_TOO_LONG,
    _bound_status_for_storage,
)
from services.metering_status import is_denied_status
from tenancy import NIL_TENANT_ID
from tests._aggregator_helpers import _make_event
from tests.conftest import T


class TestFlushEvents:
    @pytest.mark.unit
    async def test_flush_empty_buffer(self, db_session_factory):
        """Empty buffer should be a no-op -- zero rows returned, no DB writes."""
        result = await flush_events([], db_session_factory, None)
        assert result == 0

        async with db_session_factory() as session:
            count = (
                await session.execute(select(func.count()).select_from(MeteringEvent))
            ).scalar()
            assert count == 0

    @pytest.mark.unit
    async def test_flush_events_inserts_rows(self, db_session_factory):
        """Two valid events should produce two MeteringEvent rows with correct fields."""
        lic_id = str(uuid.uuid4())
        app_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())

        buffer = [
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                method="GET",
                path="/workers/api/tasks",
            ),
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                method="POST",
                path="/workers/api/tasks",
            ),
        ]

        count = await flush_events(buffer, db_session_factory, None)
        assert count == 2
        assert len(buffer) == 0  # Buffer must be drained

        async with db_session_factory() as session:
            stmt = select(MeteringEvent).where(MeteringEvent.tenant_id == T("test-tenant"))
            rows = (await session.execute(stmt)).scalars().all()
            assert len(rows) == 2
            methods = {r.method for r in rows}
            assert methods == {"GET", "POST"}
            for r in rows:
                assert r.service_key == "workers"
                assert r.license_id == uuid.UUID(lic_id)

    @pytest.mark.unit
    async def test_flush_persists_events_with_invalid_uuids(self, db_session_factory):
        """Events with unparseable UUIDs should still be inserted with NULL fields."""
        buffer = [
            {
                "tenant_id": T("test-tenant"),
                "license_id": "not-a-uuid",
                "app_id": "also-not-uuid",
                "plan_id": "nope",
                "method": "GET",
                "path": "/workers/api/tasks",
                "status": "allowed",
            },
        ]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.license_id is None
            assert row.app_id is None
            assert row.plan_id is None

            # No monthly usage since license_id is NULL
            usage = (await session.execute(select(MeteringMonthlyUsage))).scalar_one_or_none()
            assert usage is None

    @pytest.mark.unit
    async def test_flush_denied_not_counted_in_quota(self, db_session_factory):
        """Denied events should be inserted and track denied_count, not request_count."""
        lic_id = str(uuid.uuid4())
        app_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())

        buffer = [
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                app_id=app_id,
                plan_id=plan_id,
                status="denied",
            ),
        ]

        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            # Event row exists
            row = (
                await session.execute(
                    select(MeteringEvent).where(MeteringEvent.license_id == uuid.UUID(lic_id))
                )
            ).scalar_one()
            assert row.status == "denied"

            # Monthly usage tracks denied_count, not request_count
            usage = (
                await session.execute(
                    select(MeteringMonthlyUsage).where(
                        MeteringMonthlyUsage.license_id == uuid.UUID(lic_id)
                    )
                )
            ).scalar_one()
            assert usage.request_count == 0
            assert usage.denied_count == 1

    @pytest.mark.unit
    async def test_http_code_status_is_counted_as_unrecognised(self, db_session_factory):
        """An HTTP-code status is BILLABLE, and is still recorded as unusual.

        `billing_metering_events.status` has two writers with different
        vocabularies: license-authz writes decisions (`allowed`, `denied_*`)
        and fr writes HTTP codes (`200`, `201`, `404`).

        ⚠️ THIS TEST USED TO PIN THE DEFECT. It asserted that an HTTP-code row
        incremented NEITHER request_count NOR denied_count, and its own
        docstring named the consequence: "the day fr meters a LICENSED request
        with an HTTP status, the usage vanishes from billing silently."
        Measured on prod 2026-09-04 over 30 days, 159,085 such rows existed and
        none carried both a license_id and a plan_id — so it was latent, and
        latency was a property of the DATA, not of this code.

        `services.metering_status` closed it: denial is `status` starting with
        "denied", and EVERYTHING else — every HTTP code, "" and NULL — is
        billable, which is the rule `rollup_engine` already used for the daily
        figure. The same event is therefore no longer billed by one writer and
        dropped by the other.

        Two assertions, and both matter:
          * the unrecognised-status counter STILL moves — it survived as pure
            observability, and deleting it would have removed a live detector;
          * the monthly row now EXISTS with request_count == 1, which is the
            usage that used to vanish.
        """
        from observability.metrics import metering_status_unrecognised_total

        lic_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())
        before = metering_status_unrecognised_total.labels(status="200")._value.get()

        buffer = [
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                app_id=str(uuid.uuid4()),
                plan_id=plan_id,
                status="200",
            ),
        ]

        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        after = metering_status_unrecognised_total.labels(status="200")._value.get()
        assert after == before + 1, "an unrecognised status must be counted, not swallowed"

        async with db_session_factory() as session:
            # The usage that used to vanish is now counted, under the same rule
            # rollup_engine bills the daily figure with.
            usage = (
                await session.execute(
                    select(MeteringMonthlyUsage).where(
                        MeteringMonthlyUsage.license_id == uuid.UUID(lic_id)
                    )
                )
            ).scalar_one_or_none()
            assert usage is not None, (
                "an HTTP-code status on a LICENSED request must produce a monthly "
                "row — this is the usage that previously vanished silently"
            )
            assert usage.request_count == 1, "billable, exactly once"
            assert usage.denied_count == 0, "an HTTP code is not a denial"

    @pytest.mark.unit
    async def test_allowed_still_counts_and_is_not_flagged(self, db_session_factory):
        """Negative control: a recognised status must NOT trip the counter.

        If this ever fires, the guard has widened into normal traffic and the
        metric becomes noise — the failure mode that makes an instrument
        ignorable, which is worse than not having it.
        """
        from observability.metrics import metering_status_unrecognised_total

        lic_id = str(uuid.uuid4())
        before = metering_status_unrecognised_total.labels(status="allowed")._value.get()

        buffer = [
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic_id,
                app_id=str(uuid.uuid4()),
                plan_id=str(uuid.uuid4()),
                status="allowed",
            ),
        ]
        await flush_events(buffer, db_session_factory, None)

        after = metering_status_unrecognised_total.labels(status="allowed")._value.get()
        assert after == before, "a recognised status must not be flagged as unrecognised"

        async with db_session_factory() as session:
            usage = (
                await session.execute(
                    select(MeteringMonthlyUsage).where(
                        MeteringMonthlyUsage.license_id == uuid.UUID(lic_id)
                    )
                )
            ).scalar_one()
            assert usage.request_count == 1

    @pytest.mark.unit
    async def test_flush_events_service_key_from_path(self, db_session_factory):
        """When _service_key is missing, extract_service_key should derive it from path."""
        buffer = [
            {
                "tenant_id": T("test-tenant"),
                "license_id": str(uuid.uuid4()),
                "app_id": str(uuid.uuid4()),
                "plan_id": str(uuid.uuid4()),
                "method": "GET",
                "path": "/kanban/api/boards",
                "status": "allowed",
            },
        ]

        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.service_key == "kanban"

    @pytest.mark.unit
    async def test_flush_events_multiple_licenses_separate_monthly(self, db_session_factory):
        """Different license_ids in one batch should create separate monthly usage rows."""
        lic1 = str(uuid.uuid4())
        lic2 = str(uuid.uuid4())
        app_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())

        buffer = [
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic1,
                app_id=app_id,
                plan_id=plan_id,
                status="allowed",
            ),
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic1,
                app_id=app_id,
                plan_id=plan_id,
                status="allowed",
            ),
            _make_event(
                tenant_id=T("test-tenant"),
                license_id=lic2,
                app_id=app_id,
                plan_id=plan_id,
                status="allowed",
            ),
        ]

        count = await flush_events(buffer, db_session_factory, None)
        assert count == 3

    @pytest.mark.unit
    async def test_flush_events_multiple_tenants(self, db_session_factory):
        """Events from different tenants should all be stored."""
        buffer = [
            _make_event(tenant_id=T("tenant-a")),
            _make_event(tenant_id=T("tenant-b")),
            _make_event(tenant_id=T("tenant-c")),
        ]

        count = await flush_events(buffer, db_session_factory, None)
        assert count == 3

        async with db_session_factory() as session:
            rows = (await session.execute(select(MeteringEvent))).scalars().all()
            tenant_ids = {r.tenant_id for r in rows}
            assert tenant_ids == {T("tenant-a"), T("tenant-b"), T("tenant-c")}

    @pytest.mark.unit
    async def test_flush_events_source_ip_stored(self, db_session_factory):
        """Source IP should be correctly stored on the event row."""
        buffer = [_make_event(tenant_id=T("test-tenant"), source_ip="192.168.1.42")]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.source_ip == "192.168.1.42"

    @pytest.mark.unit
    async def test_flush_events_missing_license_id_persisted(self, db_session_factory):
        """Event with empty license_id should be persisted with NULL, no monthly counter."""
        buffer = [
            {
                "tenant_id": T("t"),
                "license_id": "",
                "app_id": str(uuid.uuid4()),
                "plan_id": str(uuid.uuid4()),
                "method": "GET",
                "path": "/workers/api/x",
                "status": "allowed",
            },
        ]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.license_id is None
            # No monthly usage since license_id is NULL
            usage = (await session.execute(select(MeteringMonthlyUsage))).scalar_one_or_none()
            assert usage is None

    @pytest.mark.unit
    async def test_flush_events_missing_app_id_persisted(self, db_session_factory):
        """Event with empty app_id should be persisted with NULL app_id."""
        lic_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())
        buffer = [
            {
                "tenant_id": T("t"),
                "license_id": lic_id,
                "app_id": "",
                "plan_id": plan_id,
                "method": "GET",
                "path": "/workers/api/x",
                "status": "allowed",
            },
        ]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.app_id is None
            assert row.license_id == uuid.UUID(lic_id)

    @pytest.mark.unit
    async def test_flush_events_missing_plan_id_persisted(self, db_session_factory):
        """Empty plan_id: the EVENT keeps NULL, the monthly row uses the SENTINEL.

        ⚠️ CORRECTED. This test used to assert "No monthly usage since plan_id is
        NULL" and it was pinning the DEFECT as the spec. `plan_id` reaches billing
        only off the wire — there is no Plan model and no `resolve_plan` — and
        measured over 30 days, ZERO of 159,085 HTTP-status rows carried both ids.
        So the old writer guard `if license_id and plan_id:` was conditioned on a
        field billing cannot produce, and 155 of 160 configured licences (96.9%)
        had no usage row at all. That is why a quota could never bite.

        The two `plan_id`s are deliberately different and that is the point:
        `MeteringEvent.plan_id` stays NULL because the event records what actually
        arrived, while the monthly row substitutes `NULL_PLAN_ID` because Postgres
        UNIQUE is NULLS DISTINCT — a real NULL there would stop `ON CONFLICT`
        de-duplicating and mint a fresh row on every flush.
        """
        lic_id = str(uuid.uuid4())
        buffer = [
            {
                "tenant_id": T("t"),
                "license_id": lic_id,
                "app_id": str(uuid.uuid4()),
                "plan_id": "",
                "method": "GET",
                "path": "/workers/api/x",
                "status": "allowed",
            },
        ]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            assert row.plan_id is None, "the event must record what arrived, not a sentinel"
            usage = (await session.execute(select(MeteringMonthlyUsage))).scalar_one_or_none()
            assert usage is not None, (
                "a licensed event with no plan_id must still be counted — refusing it "
                "here is the defect that left 96.9% of configured licences uncounted"
            )
            assert str(usage.plan_id) == str(NULL_PLAN_ID)
            assert str(usage.license_id) == lic_id

    @pytest.mark.unit
    @pytest.mark.nil_tenant
    async def test_flush_events_defaults_for_missing_fields(self, db_session_factory):
        """Events with minimal fields should use defaults for method, path, status."""
        lic_id = str(uuid.uuid4())
        app_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())

        buffer = [
            {
                "license_id": lic_id,
                "app_id": app_id,
                "plan_id": plan_id,
            },
        ]

        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            row = (await session.execute(select(MeteringEvent))).scalar_one()
            # CONTRACT CHANGE (0011 / tenancy.py): a missing tenant_id used to be
            # stored as the literal identifier "unknown", which minted a fake tenant
            # (13,670 live rows). tenant_id is a uuid column now, so it sinks to the
            # nil tenant instead; the honest "unknown" survives in the free-text
            # display column, which is where it always belonged.
            assert row.tenant_id == NIL_TENANT_ID
            assert row.tenant_name == "unknown"
            assert row.method == "GET"
            assert row.path == "/"
            assert row.status == "allowed"

    @pytest.mark.unit
    async def test_flush_events_valid_mixed_with_invalid(self, db_session_factory):
        """Both valid and invalid-UUID events should be inserted; only valid gets monthly."""
        valid_lic = str(uuid.uuid4())
        valid_app = str(uuid.uuid4())
        valid_plan = str(uuid.uuid4())

        buffer = [
            # Bad UUIDs: persisted with NULLs, no monthly counter
            {
                "tenant_id": T("t"),
                "license_id": "bad",
                "app_id": str(uuid.uuid4()),
                "plan_id": str(uuid.uuid4()),
                "method": "GET",
                "path": "/x",
                "status": "allowed",
            },
            # Valid
            _make_event(
                tenant_id=T("t"), license_id=valid_lic, app_id=valid_app, plan_id=valid_plan
            ),
        ]

        count = await flush_events(buffer, db_session_factory, None)
        assert count == 2  # Both persisted

        async with db_session_factory() as session:
            rows = (await session.execute(select(MeteringEvent))).scalars().all()
            assert len(rows) == 2
            valid_rows = [r for r in rows if r.license_id is not None]
            assert len(valid_rows) == 1
            assert valid_rows[0].license_id == uuid.UUID(valid_lic)

            # Only one monthly usage row (for the valid event)
            usage = (await session.execute(select(MeteringMonthlyUsage))).scalars().all()
            assert len(usage) == 1
            assert usage[0].license_id == uuid.UUID(valid_lic)


# ===========================================================================
# _bound_status_for_storage — FIX 2: 20 -> 64 char column, alembic 0038
# ===========================================================================


class TestBoundStatusForStorage:
    """The status column widened 20 -> 64 (alembic 0038). See that migration's
    docstring and models/db/events.py:MeteringEvent.status for the full case.
    """

    def test_short_status_passes_through_unchanged(self):
        assert _bound_status_for_storage("allowed") == "allowed"

    def test_status_at_new_column_width_is_not_truncated(self):
        """Exactly _STATUS_COLUMN_MAX (64) chars must survive verbatim."""
        status = "x" * _STATUS_COLUMN_MAX
        assert _bound_status_for_storage(status) == status

    def test_previously_truncated_status_now_survives_verbatim(self):
        """'allowed_service_principal' (25 chars) is the ONE value measured
        live hitting the old 20-char sentinel (1,509 rows/24h, 2026-09-16).
        It must now be stored in full.
        """
        status = "allowed_service_principal"
        assert len(status) == 25
        assert _bound_status_for_storage(status) == status

    def test_denied_service_principal_no_longer_misclassifies(self):
        """THE BILLING-INTEGRITY CASE. license-authz emits
        'denied_service_principal' (24 chars) -- a DENIED outcome. Under the
        old 20-char bound this would have been stored as '_too_long', which
        does not start with 'denied', so is_denied_status would wrongly say
        False (billable). After the fix it is stored verbatim and correctly
        classifies as denied.
        """
        status = "denied_service_principal"
        assert len(status) == 24
        stored = _bound_status_for_storage(status)
        assert stored == status  # not "_too_long"
        assert is_denied_status(stored) is True

    def test_denied_invalid_tenant_no_longer_misclassifies(self):
        """Second live denial reason over the old 20-char bound (21 chars)."""
        status = "denied_invalid_tenant"
        assert len(status) == 21
        stored = _bound_status_for_storage(status)
        assert stored == status
        assert is_denied_status(stored) is True

    def test_genuinely_hostile_status_still_bounded(self):
        """The defence this bound exists for is unchanged, only its threshold
        moved. An exception-message-shaped status well past 64 chars must
        still be caught and sentinelled, not inserted verbatim.
        """
        status = "ValueError: connection reset by peer at host=10.0.0.1 req=abc123xyz"
        assert len(status) > _STATUS_COLUMN_MAX
        stored = _bound_status_for_storage(status)
        assert stored == _STATUS_TOO_LONG
        # And the sentinel itself must never satisfy the billing branches.
        assert is_denied_status(stored) is False
        assert stored != "allowed"

    @pytest.mark.unit
    async def test_denied_service_principal_persists_and_bills_correctly(self, db_session_factory):
        """End-to-end: flush a real event through flush_events and read the
        row back. Proves the DB COLUMN (not just the Python helper) accepts
        the widened value -- a stale-schema mismatch here would raise
        StringDataRightTruncation on INSERT, not just fail an assertion.
        """
        status = "denied_service_principal"
        buffer = [
            _make_event(
                tenant_id=T("test-tenant-status-width"),
                path="/api/deliverability-signals",
                status=status,
                service_key="autonomy",  # pragma: allowlist secret
            ),
        ]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 1

        async with db_session_factory() as session:
            stmt = select(MeteringEvent).where(
                MeteringEvent.tenant_id == T("test-tenant-status-width")
            )
            row = (await session.execute(stmt)).scalar_one()
            assert row.status == status
            assert is_denied_status(row.status) is True


# ===========================================================================
# flush_costs
# ===========================================================================


class TestParseHelpers:
    @pytest.mark.unit
    def test_parse_uuid_valid(self):
        uid = uuid.uuid4()
        assert _parse_uuid(str(uid)) == uid

    @pytest.mark.unit
    def test_parse_uuid_empty_string(self):
        assert _parse_uuid("") is None

    @pytest.mark.unit
    def test_parse_uuid_invalid_string(self):
        """Should trigger the ValueError branch."""
        assert _parse_uuid("not-a-uuid") is None

    @pytest.mark.unit
    def test_parse_uuid_non_string(self):
        """Should trigger the AttributeError branch (None has no UUID form)."""
        # Pass an integer which will raise AttributeError when uuid.UUID() tries str ops
        assert _parse_uuid(12345) is None

    @pytest.mark.unit
    def test_parse_uuid_valid_uppercase(self):
        """UUID strings in uppercase should still parse."""
        uid = uuid.uuid4()
        assert _parse_uuid(str(uid).upper()) == uid

    @pytest.mark.unit
    def test_parse_uuid_with_braces(self):
        """UUID strings with curly braces should parse (Python's uuid.UUID handles this)."""
        uid = uuid.uuid4()
        assert _parse_uuid("{" + str(uid) + "}") == uid

    @pytest.mark.unit
    def test_parse_decimal_valid(self):
        assert _parse_decimal("1.23") == Decimal("1.23")

    @pytest.mark.unit
    def test_parse_decimal_invalid(self):
        """Unparseable decimal should return Decimal('0')."""
        assert _parse_decimal("not-a-number") == Decimal("0")

    @pytest.mark.unit
    def test_parse_decimal_none(self):
        """None input should return Decimal('0')."""
        assert _parse_decimal(None) == Decimal("0")

    @pytest.mark.unit
    def test_parse_decimal_negative(self):
        """Negative decimal should parse correctly."""
        assert _parse_decimal("-5.67") == Decimal("-5.67")

    @pytest.mark.unit
    def test_parse_decimal_zero(self):
        assert _parse_decimal("0") == Decimal("0")

    @pytest.mark.unit
    def test_parse_decimal_large(self):
        """Large decimal values should parse."""
        assert _parse_decimal("999999.99999999") == Decimal("999999.99999999")

    @pytest.mark.unit
    def test_parse_int_valid(self):
        assert _parse_int("42") == 42

    @pytest.mark.unit
    def test_parse_int_invalid_string(self):
        """Should trigger the ValueError branch."""
        assert _parse_int("not-a-number") == 0

    @pytest.mark.unit
    def test_parse_int_none(self):
        """Should trigger the TypeError branch."""
        assert _parse_int(None) == 0

    @pytest.mark.unit
    def test_parse_int_negative(self):
        assert _parse_int("-10") == -10

    @pytest.mark.unit
    def test_parse_int_zero(self):
        assert _parse_int("0") == 0

    @pytest.mark.unit
    def test_parse_int_float_string(self):
        """Float string should fail to parse as int and return 0."""
        assert _parse_int("3.14") == 0

    @pytest.mark.unit
    def test_parse_int_empty_string(self):
        """Empty string should return 0."""
        assert _parse_int("") == 0

    # -----------------------------------------------------------------
    # PEP 758 multi-type except coverage
    #
    # aggregator._parse_uuid uses `except ValueError, AttributeError:`
    # aggregator._parse_int  uses `except ValueError, TypeError:`
    # Both are the parenthesis-less PEP 758 form, valid on Python 3.14.
    # The tests below explicitly raise each concrete exception type inside
    # the except's tuple so we have recorded intent per branch.
    # -----------------------------------------------------------------

    @pytest.mark.unit
    def test_parse_uuid_pep758_value_error_branch(self):
        """PEP 758 branch for _parse_uuid: ValueError path (malformed UUID string)."""
        # A string that reaches uuid.UUID() but fails its internal format check
        assert _parse_uuid("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx") is None

    @pytest.mark.unit
    def test_parse_uuid_pep758_attribute_error_branch(self):
        """PEP 758 branch for _parse_uuid: AttributeError path (non-string truthy input)."""
        # A truthy non-string value like an int causes uuid.UUID() to call
        # .replace on a type that does not have it, raising AttributeError.
        # Both exception types must be caught by the PEP 758 except clause.
        assert _parse_uuid(12345) is None

    @pytest.mark.unit
    def test_parse_int_pep758_value_error_branch(self):
        """PEP 758 branch for _parse_int: ValueError path (non-numeric string)."""
        assert _parse_int("not-a-number") == 0

    @pytest.mark.unit
    def test_parse_int_pep758_type_error_branch(self):
        """PEP 758 branch for _parse_int: TypeError path (None input)."""
        # int(None) raises TypeError, not ValueError — only the PEP 758
        # tuple form covers both.
        assert _parse_int(None) == 0


# ===========================================================================
# Redis failure paths
# ===========================================================================


class TestSubjectIdMonthlyUsage:
    """Two subjects on the same license+plan => two monthly_usage rows."""

    @pytest.mark.unit
    async def test_two_subjects_create_two_monthly_rows(self, db_session_factory):
        lic_id = str(uuid.uuid4())
        plan_id = str(uuid.uuid4())
        sub_a = str(uuid.uuid4())
        sub_b = str(uuid.uuid4())

        buffer = [
            _make_event(tenant_id=T("test-tenant"), license_id=lic_id, plan_id=plan_id)
            | {"subject_id": sub_a},
            _make_event(tenant_id=T("test-tenant"), license_id=lic_id, plan_id=plan_id)
            | {"subject_id": sub_a},
            _make_event(tenant_id=T("test-tenant"), license_id=lic_id, plan_id=plan_id)
            | {"subject_id": sub_b},
        ]
        count = await flush_events(buffer, db_session_factory, None)
        assert count == 3

        async with db_session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(MeteringMonthlyUsage).where(
                            MeteringMonthlyUsage.license_id == uuid.UUID(lic_id)
                        )
                    )
                )
                .scalars()
                .all()
            )
            by_sub = {str(r.subject_id): r.request_count for r in rows}
            assert by_sub[sub_a] == 2
            assert by_sub[sub_b] == 1


class TestFlushEventsIdempotency:
    """At-least-once → exactly-once dedup on the Redis usage-stream message id.

    Mirrors ``test_cost_flusher.TestFlushCostsIdempotency``: the EVENTS path's
    monthly rollup UPSERT is additive, so without the ProcessedEventMessage inbox a
    redelivered message would double-count request_count/denied_count.
    """

    def _stream_event(self, msg_id: str, tenant_id: str = T("test-tenant"), **kw):
        # Mirror what the consumer tags onto each event before buffering.
        # tenant_id is a real uuid5 (T(...)), never a readable label: a label
        # would sink to NIL_TENANT_ID in _normalize_tenant_id and every dedup
        # assertion below would pass against the nil tenant instead of this one.
        return _make_event(tenant_id=tenant_id, **kw) | {
            "_msg_id": msg_id,
            "_stream_name": "usage-events",
        }

    @pytest.mark.unit
    async def test_redelivery_does_not_double_count_usage(self, db_session_factory, fake_redis):
        """Re-flushing an identical batch (same _msg_ids) counts everything once."""
        lic = str(uuid.uuid4())
        plan = str(uuid.uuid4())
        batch = [
            self._stream_event("1700000000000-0", license_id=lic, plan_id=plan, status="allowed"),
            self._stream_event("1700000000001-0", license_id=lic, plan_id=plan, status="denied"),
        ]

        first = await flush_events(list(batch), db_session_factory, fake_redis)
        assert first == 2

        # Redeliver the identical batch (crash-after-commit-before-XACK / NOGROUP replay).
        second = await flush_events(list(batch), db_session_factory, fake_redis)
        assert second == 0  # every event was a redelivery — nothing new persisted

        async with db_session_factory() as session:
            # Raw events inserted exactly once (no second-flush duplicates).
            n_events = (
                await session.execute(select(func.count()).select_from(MeteringEvent))
            ).scalar()
            assert n_events == 2

            # Monthly rollup reflects the batch ONCE, not twice.
            usage = (
                await session.execute(
                    select(MeteringMonthlyUsage).where(
                        MeteringMonthlyUsage.license_id == uuid.UUID(lic)
                    )
                )
            ).scalar_one()
            assert usage.request_count == 1
            assert usage.denied_count == 1

            # Both stream ids claimed exactly once.
            claimed = (
                await session.execute(select(func.count()).select_from(ProcessedEventMessage))
            ).scalar()
            assert claimed == 2

    @pytest.mark.unit
    async def test_intra_batch_duplicate_msg_id_counted_once(self, db_session_factory, fake_redis):
        """The SAME stream msg id appearing TWICE in one batch (both newly claimed)
        must insert ONE raw event and count request_count ONCE.

        Robust-by-construction dedup: the flusher collapses repeated msg ids
        before the idempotency claim, so a doubled id can never double-count —
        defended by construction, not by the 300s XAUTOCLAIM idle window.

        MUTATION-PROOF: revert to the set-membership `survived` filter
        (`p[1] in claimed`) and BOTH copies pass — `claimed` holds the id once,
        but membership is true for both — inserting two raw rows and a monthly
        request_count of 2. This test then fails.
        """
        lic = str(uuid.uuid4())
        plan = str(uuid.uuid4())
        dup_id = "1700000000099-0"
        batch = [
            self._stream_event(dup_id, license_id=lic, plan_id=plan, status="allowed"),
            self._stream_event(dup_id, license_id=lic, plan_id=plan, status="allowed"),
        ]

        count = await flush_events(batch, db_session_factory, fake_redis)
        assert count == 1  # NOT 2 — the duplicate collapsed

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(MeteringEvent))).scalar()
            assert n == 1
            usage = (
                await session.execute(
                    select(MeteringMonthlyUsage).where(
                        MeteringMonthlyUsage.license_id == uuid.UUID(lic)
                    )
                )
            ).scalar_one()
            assert usage.request_count == 1  # NOT 2

    @pytest.mark.unit
    async def test_events_without_msg_id_always_process(self, db_session_factory):
        """Direct (non-stream) events have no dedup key and are never dropped."""
        a = await flush_events([_make_event(tenant_id=T("test-tenant"))], db_session_factory, None)
        b = await flush_events([_make_event(tenant_id=T("test-tenant"))], db_session_factory, None)
        assert a == 1 and b == 1
        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(MeteringEvent))).scalar()
            assert n == 2
            claimed = (
                await session.execute(select(func.count()).select_from(ProcessedEventMessage))
            ).scalar()
            assert claimed == 0

    @pytest.mark.unit
    async def test_mixed_batch_only_new_ids_persist(self, db_session_factory):
        """A batch mixing an already-processed id with a fresh one inserts only the fresh one."""
        lic = str(uuid.uuid4())
        plan = str(uuid.uuid4())
        await flush_events(
            [self._stream_event("1700000000010-0", license_id=lic, plan_id=plan)],
            db_session_factory,
            None,
        )

        count = await flush_events(
            [
                self._stream_event("1700000000010-0", license_id=lic, plan_id=plan),  # duplicate
                self._stream_event("1700000000011-0", license_id=lic, plan_id=plan),  # new
            ],
            db_session_factory,
            None,
        )
        assert count == 1

        async with db_session_factory() as session:
            n = (await session.execute(select(func.count()).select_from(MeteringEvent))).scalar()
            assert n == 2  # one from each flush, the duplicate dropped

            # Two distinct allowed events on the same license → request_count 2, once each.
            usage = (
                await session.execute(
                    select(MeteringMonthlyUsage).where(
                        MeteringMonthlyUsage.license_id == uuid.UUID(lic)
                    )
                )
            ).scalar_one()
            assert usage.request_count == 2


@pytest.mark.unit
class TestTheTenantIngressCounterIsAliasedNotRenamed:
    """`billing_tenant_unattributed_total` is MISNAMED, and the fix is an alias.

    It fires the instant the wire is read, BEFORE normalisation decides what the
    row is actually stored under, so it measures "the emitter sent a non-UUID
    tenant_id" — a statement about the EMITTER — not "the row is unattributed".
    Measured over a 10h window on 2026-09-05, strictly after that day's
    truncate: +1,100 here against 27,941 stored rows with ZERO NULL tenant_id.
    Every one of those 1,100 was normalised to a real tenant downstream.

    The old name is kept EMITTED rather than renamed away because a metric name
    is a published interface: a hard rename does not error, it empties the
    series, and a query over an empty vector answers nothing while looking fine.
    """

    @staticmethod
    def _old() -> float:
        import observability.metrics as om

        return om.tenant_unattributed_total.labels(stream="usage")._value.get()

    @staticmethod
    def _new() -> float:
        import observability.metrics as om

        return om.tenant_caller_unasserted_total.labels(stream="usage")._value.get()

    # INTENDED: a non-UUID tenant sinking to NIL_TENANT_ID is the exact
    # condition under test — it is what makes the ingress counter fire.
    @pytest.mark.nil_tenant
    async def test_both_names_move_together_on_a_non_uuid_tenant(self, db_session_factory):
        old_before, new_before = self._old(), self._new()
        buffer = [_make_event(tenant_id="not-a-uuid-at-all", license_id=str(uuid.uuid4()))]
        assert await flush_events(buffer, db_session_factory, None) == 1
        assert self._old() - old_before == 1
        assert self._new() - new_before == 1

    async def test_neither_name_moves_on_a_resolvable_tenant(self, db_session_factory):
        """The other half — a counter that only goes up measures nothing."""
        old_before, new_before = self._old(), self._new()
        buffer = [_make_event(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4()))]
        assert await flush_events(buffer, db_session_factory, None) == 1
        assert self._old() - old_before == 0
        assert self._new() - new_before == 0

    # INTENDED: a non-UUID tenant sinking to NIL_TENANT_ID is the exact
    # condition under test — it is what makes the ingress counter fire.
    @pytest.mark.nil_tenant
    async def test_the_alias_is_exact_over_a_mixed_batch(self, db_session_factory):
        """Not "both rose" — both rose by the SAME amount. An alias that drifts
        is worse than none: the two names would disagree and nothing would say
        which one a dashboard is reading.
        """
        old_before, new_before = self._old(), self._new()
        buffer = [
            _make_event(tenant_id="nope-one", license_id=str(uuid.uuid4())),
            _make_event(tenant_id="nope-two", license_id=str(uuid.uuid4())),
            _make_event(tenant_id=T("test-tenant"), license_id=str(uuid.uuid4())),
        ]
        assert await flush_events(buffer, db_session_factory, None) == 3
        old_delta = self._old() - old_before
        assert old_delta == 2
        assert self._new() - new_before == old_delta

    def test_both_stream_series_are_birthed_at_import(self) -> None:
        """A labelled counter exports NO samples until its first `.labels()`, so
        without the birth "no unasserted tenants" and "the metric does not
        exist" are the same empty vector.
        """
        import observability.metrics as om

        exported = {
            sample.labels["stream"]
            for metric in om.tenant_caller_unasserted_total.collect()
            for sample in metric.samples
        }
        assert {"cost", "usage"} <= exported

    def test_the_deprecated_help_no_longer_asserts_the_wrong_meaning(self) -> None:
        import observability.metrics as om

        doc = om.tenant_unattributed_total._documentation
        assert "DEPRECATED" in doc
        assert "billing_tenant_caller_unasserted_total" in doc
