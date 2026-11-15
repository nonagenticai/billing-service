"""``quota_pct`` must distinguish UNLIMITED (``quota_monthly is None``) from
BLOCK-ALL (``quota_monthly == 0``).

``round(req_count / quota_monthly * 100, 1) if quota_monthly else None`` used
``if quota_monthly`` — Python-falsy on ``0`` — so a licence suspended to
``quota_monthly = 0`` (BLOCK-ALL; every request denied by license-authz)
rendered ``quota_pct: None``, identical to a genuinely unlimited licence.
``templates/tenant.html`` renders a ``None`` percentage as "0% used" behind a
GREEN bar, so the most restricted licence on the invoice looked healthiest.

This mirrors ``tests/test_budget_zero_block_all.py::TestZeroCapReadPaths
::test_billing_statement_reports_blocked_licence_as_full``, the sibling fix
for ``budget_pct`` in the same function. ``tenant_id`` is a UUID column here
(``Uuid(as_uuid=False)``), so tenants below are real UUID strings, not
readable slugs.
"""

from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from models.db import MeteringDaily, TenantConfig, TokenUsage
from services.billing import generate_billing


def _seed(db_session, *, tenant, lic_id, request_count):
    # A TenantConfig row keyed on the real tenant UUID makes get_markup_pct's
    # first (per-tenant) lookup hit, short-circuiting before its second lookup
    # (keyed on the legacy string sentinel "__default__", pre-existing/
    # out-of-scope breakage against the now-UUID tenant_id column — reproduced
    # identically against the unmodified tests/test_billing_service.py
    # ::test_generate_billing_quota_pct, so not something this fix introduced).
    db_session.add(TenantConfig(tenant_id=tenant, markup_pct=Decimal("30.00")))
    db_session.add(
        MeteringDaily(
            tenant_id=tenant,
            license_id=lic_id,
            app_id=uuid.uuid4(),
            plan_id=uuid.uuid4(),
            service_key="chatbot",  # pragma: allowlist secret
            bucket_date=date(2026, 3, 1),
            request_count=request_count,
            denied_count=0,
        )
    )
    db_session.add(
        TokenUsage(
            tenant_id=tenant,
            license_id=lic_id,
            period_type="monthly",
            period_start=date(2026, 3, 1),
            input_tokens=0,
            output_tokens=0,
            total_tokens=0,
        )
    )


@pytest.mark.unit
@patch("services.billing.fetch_licenses", new_callable=AsyncMock)
async def test_quota_pct_none_is_unlimited(mock_fetch, db_session, test_settings):
    tenant = str(uuid.uuid4())
    lic_id = uuid.uuid4()
    mock_fetch.return_value = [
        {
            "id": str(lic_id),
            "app": {"key": "chatbot"},
            "plan": {"code": "basic", "quota_monthly": None},
        },
    ]
    _seed(db_session, tenant=tenant, lic_id=lic_id, request_count=10)
    await db_session.commit()

    result = await generate_billing(tenant, "2026-03", db_session, test_settings)

    usage = result["licenses"][0]["usage"]
    assert usage["quota_monthly"] is None
    assert usage["quota_pct"] is None


@pytest.mark.unit
@patch("services.billing.fetch_licenses", new_callable=AsyncMock)
async def test_quota_pct_zero_is_block_all_not_none(mock_fetch, db_session, test_settings):
    """The defect: quota_monthly=0 must report 100.0, not None."""
    tenant = str(uuid.uuid4())
    lic_id = uuid.uuid4()
    mock_fetch.return_value = [
        {
            "id": str(lic_id),
            "app": {"key": "chatbot"},
            "plan": {"code": "basic", "quota_monthly": 0},
        },
    ]
    _seed(db_session, tenant=tenant, lic_id=lic_id, request_count=0)
    await db_session.commit()

    result = await generate_billing(tenant, "2026-03", db_session, test_settings)

    usage = result["licenses"][0]["usage"]
    assert usage["quota_monthly"] == 0
    assert usage["quota_pct"] == 100.0


@pytest.mark.unit
@patch("services.billing.fetch_licenses", new_callable=AsyncMock)
async def test_quota_pct_normal_computation_unchanged(mock_fetch, db_session, test_settings):
    tenant = str(uuid.uuid4())
    lic_id = uuid.uuid4()
    mock_fetch.return_value = [
        {
            "id": str(lic_id),
            "app": {"key": "chatbot"},
            "plan": {"code": "basic", "quota_monthly": 1000},
        },
    ]
    _seed(db_session, tenant=tenant, lic_id=lic_id, request_count=250)
    await db_session.commit()

    result = await generate_billing(tenant, "2026-03", db_session, test_settings)

    usage = result["licenses"][0]["usage"]
    assert usage["quota_monthly"] == 1000
    assert usage["quota_pct"] == 25.0
