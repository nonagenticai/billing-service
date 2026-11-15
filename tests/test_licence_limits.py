"""Unit tests for ``services.licence_limits``: per-licence override wins over plan."""

from __future__ import annotations

import pytest

from services.licence_limits import effective_limit


@pytest.mark.unit
@pytest.mark.parametrize(
    ("override", "plan_value", "expected"),
    [
        # override present → override, including 0 (zero-allowance, not unlimited)
        (100_000, 50_000_000, 100_000),
        (0, 50_000_000, 0),
        (7, None, 7),
        # override None / absent (older licences deploy) → plan value, raw
        (None, 50_000_000, 50_000_000),
        (None, 0, 0),
        # unlimited stays the ABSENCE of the plan field, never coerced
        (None, None, None),
    ],
)
def test_effective_limit(override, plan_value, expected):
    assert effective_limit(override, plan_value, field="quota_override") == expected


@pytest.mark.unit
@pytest.mark.parametrize("bad", [-1, True, False, "100", 1.5])
def test_malformed_override_falls_back_to_plan(bad):
    assert effective_limit(bad, 9, field="token_budget_override", license_id="x") == 9
