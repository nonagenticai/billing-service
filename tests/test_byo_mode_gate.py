"""Phase 4 byo_mode billing gate — internal/demo tenants bill $0.

Pure-function tests for the gate primitives:
- ``effective_billing_mode``: byo_mode → the mode used for the invoice.
- ``compute_billed_line`` under ``BILLING_MODE_INTERNAL``: every line bills $0.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from services.byo_mode import effective_billing_mode
from services.markup import (
    BILLING_MODE_INTERNAL,
    BILLING_MODE_STANDARD,
    MarkupResolver,
    MarkupRule,
    compute_billed_line,
)
from tests.conftest import T

PAST = datetime(2026, 1, 1, tzinfo=UTC)


def _rule(**kw) -> MarkupRule:
    kw.setdefault("effective_from", PAST)
    return MarkupRule(**kw)


def test_effective_mode_internal_for_non_commercial():
    # local_allowed + platform_only → internal ($0), regardless of configured mode.
    assert effective_billing_mode("standard_markup", "local_allowed") == BILLING_MODE_INTERNAL
    assert effective_billing_mode("standard_markup", "platform_only") == BILLING_MODE_INTERNAL
    assert effective_billing_mode("byo_platform_fee", "local_allowed") == BILLING_MODE_INTERNAL


def test_effective_mode_keeps_configured_for_commercial():
    # byo_required keeps the tenant's configured mode (default standard_markup).
    assert effective_billing_mode("standard_markup", "byo_required") == "standard_markup"
    assert effective_billing_mode("byo_platform_fee", "byo_required") == "byo_platform_fee"
    # Unknown/garbage byo_mode is treated as commercial (revenue-safe: bill).
    assert effective_billing_mode("standard_markup", "") == "standard_markup"
    assert effective_billing_mode("standard_markup", "weird") == "standard_markup"


def test_internal_mode_bills_zero_even_with_markup_rule():
    # A 30% global rule would normally bill 13.00 on $10; internal forces $0.
    resolver = MarkupResolver([_rule(markup_pct=Decimal("30"))])
    billed, pct, rule = compute_billed_line(
        resolver,
        BILLING_MODE_INTERNAL,
        project_id=None,
        tenant_id=T("demo-tenant"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="mlx-embed",
    )
    assert billed == Decimal("0.00")
    assert pct == 0.0
    assert rule is None


def test_standard_mode_still_bills_for_commercial():
    # Sanity: the commercial path is unchanged (30% markup on $10 = 13.00).
    resolver = MarkupResolver([_rule(markup_pct=Decimal("30"))])
    billed, _pct, _ru = compute_billed_line(
        resolver,
        BILLING_MODE_STANDARD,
        project_id=None,
        tenant_id=T("subhankar"),
        incurred_usd=Decimal("10"),
        fallback_pct=30.0,
        provider="mlx-embed",
    )
    assert billed == Decimal("13.00")
