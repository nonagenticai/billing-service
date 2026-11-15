"""Per-licence ``quota_override`` wins on the two READ paths, and 0 renders.

``services.licence_limits.effective_limit`` is applied at four call sites. The
sync writer and the reconcile drift input are covered elsewhere; these pin the
other two, each in isolation, so reverting EITHER call site to the bare
``plan.quota_monthly`` read fails here:

  - ``GET /api/quota/status/{tenant}`` — the plan-value FALLBACK used when no
    durable ``request_quota_config`` row exists (routers/quota.py);
  - the billing statement's per-licence ``usage.quota_monthly``
    (services/billing.py ``generate_billing``).

Each is checked three ways: a positive override wins, an override of **0**
wins (zero allowance, never "unlimited" and never "falsy, use the plan"), and
an absent override falls back to the plan value unchanged.

The template half: ``templates/tenant.html`` gated the quota and the token
budget on Jinja truthiness, so a zero-allowance licence rendered exactly like
an unlimited one. Both are now ``is not none`` tests.
"""

from __future__ import annotations

import uuid
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from models.db import MeteringDaily
from services.billing import generate_billing
from tests.conftest import T

PLAN_QUOTA = 1000

#: (override sent by licences, expected effective quota)
OVERRIDE_CASES = [
    pytest.param(250, 250, id="override-wins"),
    pytest.param(0, 0, id="zero-override-wins"),
    pytest.param(None, PLAN_QUOTA, id="absent-falls-back-to-plan"),
]


def _licence(lic_id: uuid.UUID, override: int | None) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": str(lic_id),
        "app": {"key": "chatbot"},
        "plan": {"id": str(uuid.uuid4()), "code": "basic", "quota_monthly": PLAN_QUOTA},
    }
    # Absent, not null: an older licences deploy does not send the key at all.
    if override is not None:
        item["quota_override"] = override
    return item


class TestQuotaStatusFallbackHonoursOverride:
    @pytest.mark.unit
    @pytest.mark.parametrize(("override", "expected"), OVERRIDE_CASES)
    def test_status_limit(self, app_client, override, expected):
        tenant = T("override-status-tenant")
        lic_id = uuid.uuid4()
        seen: list[int | None] = []

        async def _fake_check_quota(lic_uuid, plan_uuid, limit, _session):
            seen.append(limit)
            return {
                "license_id": str(lic_uuid),
                "plan_id": str(plan_uuid),
                "quota_monthly": limit,
                "used_this_month": 0,
                "remaining": limit,
                "over_quota": False,
            }

        with (
            patch(
                "routers.quota.fetch_licenses",
                AsyncMock(return_value=[_licence(lic_id, override)]),
            ),
            # No durable config row: the licences-derived value is the limit.
            patch("routers.quota.resolve_quota_limits", AsyncMock(return_value={})),
            patch("routers.quota.check_quota", side_effect=_fake_check_quota),
        ):
            response = app_client.get(f"/api/quota/status/{tenant}")

        assert response.status_code == 200, response.text
        assert seen == [expected]
        (status,) = response.json()["data"]["licenses"]
        assert status["license_id"] == str(lic_id)
        assert status["quota_monthly"] == expected


class TestStatementQuotaHonoursOverride:
    @pytest.mark.unit
    @pytest.mark.parametrize(("override", "expected"), OVERRIDE_CASES)
    async def test_statement_quota(self, db_session, test_settings, override, expected):
        tenant = T("override-statement-tenant")
        lic_id = uuid.uuid4()
        db_session.add(
            MeteringDaily(
                tenant_id=tenant,
                license_id=lic_id,
                app_id=uuid.uuid4(),
                plan_id=uuid.uuid4(),
                service_key="chatbot",  # pragma: allowlist secret
                bucket_date=date(2026, 3, 1),
                request_count=10,
                denied_count=0,
            )
        )
        await db_session.commit()

        with patch(
            "services.billing.fetch_licenses",
            AsyncMock(return_value=[_licence(lic_id, override)]),
        ):
            result = await generate_billing(tenant, "2026-03", db_session, test_settings)

        (entry,) = result["licenses"]
        assert entry["license_id"] == str(lic_id)
        assert entry["usage"]["quota_monthly"] == expected


def _render_tenant_row(*, quota_monthly: int | None, budget_tokens: int | None) -> str:
    from routers.dashboard import templates

    lic = SimpleNamespace(
        license_id="0123456789abcdef",
        app_key="chatbot",
        plan_code="basic",
        usage=SimpleNamespace(request_count=10, quota_monthly=quota_monthly),
        costs=SimpleNamespace(llm_cost_usd="0.00", app_request_cost_usd="0.00"),
        tokens=SimpleNamespace(
            input_tokens=0,
            output_tokens=0,
            budget_tokens=budget_tokens,
            budget_pct=None if budget_tokens is None else 100.0,
        ),
    )
    billing = SimpleNamespace(
        licenses=[lic],
        markup_pct=0,
        totals=SimpleNamespace(
            total_requests=10,
            internal_cost_usd="0.00",
            customer_cost_usd="0.00",
            margin_usd="0.00",
        ),
    )
    html = templates.env.get_template("tenant.html").render(
        active="overview", tenant_id="t", month="2026-03", billing=billing, budgets=[]
    )
    # Only the licence row: the summary cards also print request totals.
    start = html.index("0123456789ab...")
    return html[start : html.index("</tr>", start)]


class TestTenantTemplateRendersZeroLimits:
    @pytest.mark.unit
    def test_zero_quota_is_shown(self):
        assert "10 / 0" in _render_tenant_row(quota_monthly=0, budget_tokens=None)

    @pytest.mark.unit
    def test_absent_quota_shows_count_only(self):
        row = _render_tenant_row(quota_monthly=None, budget_tokens=None)
        assert "10" in row
        assert " / " not in row.split("chatbot", 1)[1].split("$", 1)[0]

    @pytest.mark.unit
    def test_zero_budget_shows_bar_not_dash(self):
        row = _render_tenant_row(quota_monthly=None, budget_tokens=0)
        assert "100.0% used" in row
        assert "budget-bar" in row

    @pytest.mark.unit
    def test_absent_budget_shows_dash(self):
        row = _render_tenant_row(quota_monthly=None, budget_tokens=None)
        assert "budget-bar" not in row
        assert "—" in row
