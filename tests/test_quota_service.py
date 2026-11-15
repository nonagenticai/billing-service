"""Tests for quota check service."""

import uuid
from datetime import date

from models.db import NULL_SUBJECT_ID, MeteringMonthlyUsage
from services.quota import _get_db_usage, check_quota
from timeutil import current_month_start as _current_month_start


class TestCheckQuota:
    async def test_no_usage_no_quota(self, db_session):
        """No usage, no quota limit => remaining is None, over_quota is False."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        result = await check_quota(lic_id, plan_id, None, db_session)
        assert result["used_this_month"] == 0
        assert result["remaining"] is None
        assert result["over_quota"] is False
        assert result["license_id"] == str(lic_id)
        assert result["plan_id"] == str(plan_id)
        assert result["quota_monthly"] is None

    async def test_no_usage_with_quota(self, db_session):
        """No usage, quota set => full remaining."""
        result = await check_quota(uuid.uuid4(), uuid.uuid4(), 1000, db_session)
        assert result["used_this_month"] == 0
        assert result["remaining"] == 1000
        assert result["over_quota"] is False

    async def test_check_quota_with_db_usage(self, db_session):
        """Usage in DB should be reflected in check_quota result."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=750,
            )
        )
        await db_session.commit()

        result = await check_quota(lic_id, plan_id, 1000, db_session)
        assert result["used_this_month"] == 750
        assert result["remaining"] == 250
        assert result["over_quota"] is False

    async def test_check_quota_over_quota(self, db_session):
        """Usage exceeds quota_monthly => over_quota should be True."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=1200,
            )
        )
        await db_session.commit()

        result = await check_quota(lic_id, plan_id, 1000, db_session)
        assert result["used_this_month"] == 1200
        assert result["remaining"] == -200
        assert result["over_quota"] is True

    async def test_check_quota_unlimited(self, db_session):
        """quota_monthly=None => remaining=None, over_quota=False even with usage."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=999999,
            )
        )
        await db_session.commit()

        result = await check_quota(lic_id, plan_id, None, db_session)
        assert result["used_this_month"] == 999999
        assert result["remaining"] is None
        assert result["over_quota"] is False

    async def test_check_quota_exact_quota(self, db_session):
        """Usage exactly equal to quota => over_quota=True (remaining <= 0)."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=1000,
            )
        )
        await db_session.commit()

        result = await check_quota(lic_id, plan_id, 1000, db_session)
        assert result["used_this_month"] == 1000
        assert result["remaining"] == 0
        assert result["over_quota"] is True

    async def test_zero_quota_monthly(self, db_session):
        """quota_monthly=0 => even 0 usage is at limit (remaining=0, over_quota=True)."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        result = await check_quota(lic_id, plan_id, 0, db_session)
        assert result["used_this_month"] == 0
        assert result["remaining"] == 0
        assert result["over_quota"] is True
        assert result["quota_monthly"] == 0

    async def test_single_request_under_quota(self, db_session):
        """1 request with quota=1 => over_quota=False (remaining=0 is over, but 1-1=0)."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=1,
            )
        )
        await db_session.commit()

        result = await check_quota(lic_id, plan_id, 2, db_session)
        assert result["used_this_month"] == 1
        assert result["remaining"] == 1
        assert result["over_quota"] is False

    async def test_usage_is_counted_per_license_not_per_plan(self, db_session):
        """A license's usage is its usage, whichever plan the caller asks about.

        Inverted deliberately. This test used to assert that querying with a
        different plan_id reported 0 usage. The license total is the contract.
        """
        lic_id = uuid.uuid4()
        plan_id_actual = uuid.uuid4()
        plan_id_query = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id_actual,
                month_start=month_start,
                request_count=500,
            )
        )
        await db_session.commit()

        result = await check_quota(lic_id, plan_id_query, 1000, db_session)
        assert result["used_this_month"] == 500
        assert result["remaining"] == 500
        assert result["plan_id"] == str(plan_id_query)  # echoed, not a filter

    async def test_wrong_license_id_no_match(self, db_session):
        """Usage with different license_id should not be found."""
        lic_id_actual = uuid.uuid4()
        lic_id_query = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id_actual,
                plan_id=plan_id,
                month_start=month_start,
                request_count=500,
            )
        )
        await db_session.commit()

        result = await check_quota(lic_id_query, plan_id, 1000, db_session)
        assert result["used_this_month"] == 0

    async def test_large_usage_numbers(self, db_session):
        """Large usage values should work without overflow."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=10_000_000,
            )
        )
        await db_session.commit()

        result = await check_quota(lic_id, plan_id, 50_000_000, db_session)
        assert result["used_this_month"] == 10_000_000
        assert result["remaining"] == 40_000_000
        assert result["over_quota"] is False


class TestGetDbUsage:
    """Direct tests for the internal _get_db_usage function."""

    async def test_no_data_returns_zero(self, db_session):
        """No matching record should return 0."""
        lic_id = uuid.uuid4()
        month_start = _current_month_start()

        result = await _get_db_usage(lic_id, month_start, db_session)
        assert result == 0

    async def test_matching_record_returns_count(self, db_session):
        """Matching record should return its request_count."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=42,
            )
        )
        await db_session.commit()

        result = await _get_db_usage(lic_id, month_start, db_session)
        assert result == 42

    async def test_wrong_month_returns_zero(self, db_session):
        """Record for different month should not match."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=date(2025, 1, 1),
                request_count=100,
            )
        )
        await db_session.commit()

        result = await _get_db_usage(lic_id, date(2025, 2, 1), db_session)
        assert result == 0

    async def test_zero_request_count_returns_zero(self, db_session):
        """Record with request_count=0 should return 0 (not None)."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month_start = _current_month_start()

        db_session.add(
            MeteringMonthlyUsage(
                license_id=lic_id,
                plan_id=plan_id,
                month_start=month_start,
                request_count=0,
            )
        )
        await db_session.commit()

        result = await _get_db_usage(lic_id, month_start, db_session)
        # row is 0 which is falsy, but `row or 0` should still return 0
        assert result == 0


class TestMultiSubjectQuota:
    """The DB fallback must sum every rollup row for the license+month.

    ``uq_monthly_usage`` is (license_id, subject_id, plan_id, month_start) and
    ``event_flusher`` writes one row PER SUBJECT, but ``_get_db_usage`` used to
    ``scalar_one_or_none()`` a (license, plan, month) filter. Two subjects on
    one license raised ``MultipleResultsFound`` — a ``SQLAlchemyError``, so
    ``main._sqlalchemy_exception_handler`` returned **503 DATABASE_UNAVAILABLE**,
    blaming a perfectly healthy Postgres. One subject silently under-counted.
    """

    @staticmethod
    def _usage(lic, plan, month, *, subject, count):
        return MeteringMonthlyUsage(
            license_id=lic,
            subject_id=subject,
            plan_id=plan,
            month_start=month,
            request_count=count,
        )

    async def test_two_subjects_on_one_license_sum(self, db_session):
        """Today: MultipleResultsFound -> 503. After: 300 + 450 = 750."""
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month = _current_month_start()
        db_session.add(self._usage(lic_id, plan_id, month, subject=uuid.uuid4(), count=300))
        db_session.add(self._usage(lic_id, plan_id, month, subject=uuid.uuid4(), count=450))
        await db_session.commit()

        assert await _get_db_usage(lic_id, month, db_session) == 750

    async def test_s2s_and_user_subjects_both_count(self, db_session):
        """NULL_SUBJECT_ID (S2S/system) traffic sits on its own row and counts.

        This is the fail-OPEN case: quota 1000, real usage 1200. Under the old
        single-row read the license looked 400-used and ``over_quota`` was
        False, so enforcement fell open.
        """
        lic_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        month = _current_month_start()
        db_session.add(self._usage(lic_id, plan_id, month, subject=NULL_SUBJECT_ID, count=800))
        db_session.add(self._usage(lic_id, plan_id, month, subject=uuid.uuid4(), count=400))
        await db_session.commit()

        result = await check_quota(lic_id, plan_id, 1000, db_session)
        assert result["used_this_month"] == 1200
        assert result["remaining"] == -200
        assert result["over_quota"] is True

    async def test_usage_spans_plans(self, db_session):
        """A mid-month plan change splits the rows; the license total must not."""
        lic_id = uuid.uuid4()
        old_plan = uuid.uuid4()
        new_plan = uuid.uuid4()
        month = _current_month_start()
        db_session.add(self._usage(lic_id, old_plan, month, subject=NULL_SUBJECT_ID, count=600))
        db_session.add(self._usage(lic_id, new_plan, month, subject=NULL_SUBJECT_ID, count=150))
        await db_session.commit()

        # Same answer whichever plan the caller happens to be on.
        for plan in (old_plan, new_plan):
            result = await check_quota(lic_id, plan, 1000, db_session)
            assert result["used_this_month"] == 750
            assert result["plan_id"] == str(plan)

    async def test_no_rows_still_returns_zero_not_none(self, db_session):
        """coalesce(sum(...), 0) — an empty set is 0, never None."""
        assert await _get_db_usage(uuid.uuid4(), _current_month_start(), db_session) == 0


class TestDbFailureFailsClosed:
    async def test_a_db_failure_raises_rather_than_reporting_zero_usage(self):
        """A DB error must propagate (503 upstream), never read as 0 usage.

        Guards against a later ``except SQLAlchemyError: used = 0`` in
        ``check_quota``, which would report every licence under quota while
        Postgres is down, i.e. fail OPEN.
        """
        from unittest.mock import AsyncMock

        import pytest
        from sqlalchemy.exc import OperationalError

        broken_session = AsyncMock()
        broken_session.execute = AsyncMock(
            side_effect=OperationalError("SELECT 1", {}, Exception("no connection"))
        )
        with pytest.raises(OperationalError):
            await check_quota(uuid.uuid4(), uuid.uuid4(), 1000, broken_session)
