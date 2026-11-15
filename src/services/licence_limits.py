"""Effective per-licence limits from a ``GET /internal/licenses`` item.

Licences carries two numbers per limit: the PLAN default
(``plan.token_budget_monthly`` / ``plan.quota_monthly``) and a per-licence
OVERRIDE (``token_budget_override`` / ``quota_override``, top-level on the
licence item, ``int | None``). The override is what an operator set for THIS
licence, so it wins over the plan's default. Reading only the plan is how a
``sync-from-licenses`` run on 2026-10-07 rewrote licence 01a0fdb9-…'s budget
from its 100,000 override to the plan's 50,000,000 — and why licences' own
nightly reconcile (which pushes override values) and that endpoint would keep
overwriting each other.

Every billing path that derives a licence limit from the licences payload goes
through :func:`effective_limit`, so the sync writer, the drift detector and the
read paths all agree on ONE effective value.

⛔ It takes the two VALUES, not the licence row, on purpose: callers keep their
literal ``lic.get("quota_override")`` / ``plan.get("quota_monthly")`` reads so
``tests/support/licence_reads.py`` (which parses only ``.get("literal")`` chains
in the consumer modules) still sees every licence field billing consumes. A
helper reading the row with variable keys would make the contract gate blind.

SEMANTICS (kept identical to the existing writers):

* An override counts only when it is a real non-negative integer. ``0`` is a
  ZERO-ALLOWANCE limit (block all), never "unlimited" — same rule the writers
  apply to the plan value.
* ``None`` / absent override (an older licences deploy that does not send the
  field, or no override set) falls back to the plan value EXACTLY as before —
  returned raw, so callers keep their own ``is not None and >= 0`` guard and
  "unlimited" stays the ABSENCE of the plan field.
* A malformed override (negative, bool, non-int) is ignored with a warning and
  the plan value is used: honouring it would either fail open or crash a
  reconcile loop over one bad row.
"""

from __future__ import annotations

from typing import Any

import structlog

logger = structlog.stdlib.get_logger(__name__)


def effective_limit(override: Any, plan_value: Any, *, field: str, license_id: Any = None) -> Any:
    """``override`` if it is a non-negative int (0 included), else ``plan_value`` raw."""
    if override is None:
        return plan_value
    if isinstance(override, int) and not isinstance(override, bool) and override >= 0:
        return override
    logger.warning("licence_override_ignored_malformed", license_id=license_id, field=field)
    return plan_value
