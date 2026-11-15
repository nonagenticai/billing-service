"""Token budget service — facade re-exporting the split implementation.

The 913-line module was split by responsibility so each concern lives next to
its peers and can be edited without touching the others:

- :mod:`.budget_debit` — cascade drain across stacked budgets + the shared
  ``_select_active_budgets`` cascade-ordered query (``BudgetDebit``,
  ``debit_budget``).
- :mod:`.budget_status` — per-budget + aggregate read paths
  (``get_budgets_for_tenant``, ``check_budget``, ``check_budget_aggregate``).
  Both resolve usage from ``TokenBudget.consumed_tokens`` only — no Redis
  counter (it cannot be fresher than the column and can be stale) and never
  the whole-period ``TokenUsage`` rollup. ``check_budget_aggregate`` renders
  its per-budget entries BY CALLING ``check_budget``, so the two endpoints
  cannot drift apart.
- :mod:`.budget_crud` — insert / soft-delete / license-sync upsert.
- :mod:`.budget_redis` — the atomic pre-flight ``reserve_budget`` Lua path.

Stacked-budget semantics: a tenant can hold multiple concurrent active budgets
(baseline monthly + top-ups). On debit they drain in cascade order
``(expires_at ASC NULLS LAST, priority ASC, starts_at ASC)`` — soonest-to-expire
first. Source-of-truth is the ``consumed_tokens`` column on each ``TokenBudget``.
Redis holds only the in-flight reservation pool for ``reserve_budget``; no
token-budget limit or consumption is mirrored there.

External code keeps importing ``from services.budget import X`` — the names are
re-exported here so the split is invisible to call sites.
"""

from __future__ import annotations

from services.budget_crud import (
    EmptyActiveSetError,
    UnregisteredTenantError,
    create_budget,
    deactivate_orphan_license_sync_budgets,
    delete_absent_quota_configs,
    delete_budget,
    upsert_license_sync_budget,
)
from services.budget_debit import (
    BudgetDebit,
    _select_active_budgets,
    _split_tokens_for_budget,
    debit_budget,
    release_budget_reservations,
    split_debit_outcomes,
)
from services.budget_redis import (
    _RESERVE_LUA,
    reserve_budget,
)
from services.budget_status import (
    check_budget,
    check_budget_aggregate,
    get_all_budgets_for_tenant,
    get_budgets_for_tenant,
)

__all__ = [
    "_RESERVE_LUA",
    "BudgetDebit",
    "split_debit_outcomes",
    "_select_active_budgets",
    "_split_tokens_for_budget",
    "release_budget_reservations",
    "check_budget",
    "check_budget_aggregate",
    "create_budget",
    "EmptyActiveSetError",
    "UnregisteredTenantError",
    "deactivate_orphan_license_sync_budgets",
    "delete_absent_quota_configs",
    "debit_budget",
    "delete_budget",
    "get_all_budgets_for_tenant",
    "get_budgets_for_tenant",
    "reserve_budget",
    "upsert_license_sync_budget",
]
