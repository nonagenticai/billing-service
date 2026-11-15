"""SQLAlchemy ORM models for billing tables.

The package was split out of a single 886-line ``models/db.py`` so each
group of tables lives next to its peers:

- :mod:`._common` — shared sentinels, ``DBUUID``, ``ASSET_KIND_ENUM``
- :mod:`.events` — raw event tables (``MeteringEvent``, ``CostEvent``)
- :mod:`.rollups` — aggregated rollups (hourly/daily/monthly)
- :mod:`.budgets` — token budgets + legacy ``TokenUsage`` + overage tables
- :mod:`.config` — pricing + per-app rates + per-tenant markup
- :mod:`.audit` — append-only ``AuditLog`` + ``DlqReattribution`` receipts + ``DlqArchive``
- :mod:`.audit_log_guard` — tamper-evidence ON ``audit_log`` itself: DELETE and
  TRUNCATE are recorded into ``billing_delete_audit``, UPDATE is refused
- :mod:`.delete_audit` — the #92 DELETE-audit trigger + its table
- :mod:`.budget_audit` — #244: DELETE/TRUNCATE/DEACTIVATE audit on
  ``token_budgets`` and ``request_quota_config``
- :mod:`.invoices` — the persisted invoice OBJECT + its gapless number counter
- :mod:`.invoice_immutability` — the BEFORE UPDATE OR DELETE guard on invoices

External code keeps using ``from models.db import X`` — the names are
re-exported here so the split is invisible to call sites.
"""

from __future__ import annotations

from ._common import (
    ASSET_KIND_ENUM,
    ATTRIBUTION_ATTRIBUTED,
    ATTRIBUTION_REFUSED_LICENSE,
    ATTRIBUTION_SYSTEM,
    ATTRIBUTION_UNKNOWN,
    ATTRIBUTION_UNLICENSED,
    DBUUID,
    LICENSE_REFUSED_COST_SOURCE_MARKER,
    NULL_ACTOR_ID,
    NULL_APP_ID,
    NULL_LICENSE_ID,
    NULL_PLAN_ID,
    NULL_PROJECT_ID,
    NULL_SUBJECT_ID,
    _new_uuid,
    _utcnow,
    classify_cost_attribution,
    cost_source_marks_refusal,
    is_system_license,
)
from .app_registry import AppRegistry
from .audit import AuditLog, DlqArchive, DlqReattribution

# Where ruff's isort puts it, and that is SAFE rather than merely convenient.
# Unlike .delete_audit / .truncate_guard / .unrolled_guard below, this module
# registers NO listener that touches create_cost_events_partition_for: its
# after_create hook hangs off AuditLog.__table__, it owns
# billing_audit_log_deleted_rows() and billing_forbid_audit_log_update()
# outright, and it replaces no function any other module defines. audit_log is a
# plain table with no minting function, so there is nothing here that a later
# import could overwrite or that could overwrite a later import -- the position
# of this line is not load-bearing in either direction. If audit_log is ever
# partitioned and gains a minting function, read the ordering comments further
# down this file before moving it.
from .audit_log_guard import (
    AUDIT_LOG_DELETE_REASON_PREFIX,
    AUDIT_LOG_GUARD_DDL,
    AUDIT_LOG_GUARD_DROP_DDL,
    AUDIT_LOG_TRUNCATE_REASON_PREFIX,
    GUARDED_AUDIT_TABLES,
)

# Position NOT load-bearing: .budget_audit's hooks hang off token_budgets and
# request_quota_config only, it owns its two functions outright, installs 0016's
# ledger function only when it is ABSENT, and never touches
# create_cost_events_partition_for.
from .budget_audit import BUDGET_AUDIT_DDL, BUDGET_AUDIT_DROP_DDL
from .budgets import (
    BudgetOverageAlert,
    CreditLedgerEntry,
    MoneyBudget,
    MoneyBudgetAdjustment,
    TenantWebhook,
    TokenBudget,
    TokenUsage,
)
from .config import (
    ATTRIBUTION_MATCH_PROJECT_VALUES,
    AppCostRate,
    AttributionRule,
    FxRate,
    MarkupRule,
    ProviderCost,
    ProviderPricing,
    TenantAccessCharge,
    TenantConfig,
)
from .delete_audit import (
    AUDITED_TABLES,
    DELETE_AUDIT_DDL,
    DELETE_REASON_UNSPECIFIED,
    BillingDeleteAudit,
)
from .events import (
    CostEvent,
    MeteringEvent,
    ProcessedCostEventKey,
    ProcessedCostMessage,
    ProcessedEventMessage,
)

# Position in this list is NOT load-bearing, unlike .truncate_guard and
# .unrolled_guard below. .invoice_immutability owns
# billing_forbid_invoice_mutation() outright and redefines no shared function
# (in particular it never touches create_cost_events_partition_for), so nothing
# can overwrite it and it can overwrite nothing. Plain alphabetical order.
from .invoice_immutability import (
    INVOICE_IMMUTABILITY_DDL,
    INVOICE_IMMUTABILITY_DROP_DDL,
)
from .invoices import INVOICE_STATUSES, BillingInvoice, BillingInvoiceCounter

# Sorted here by ruff's isort, and unlike .truncate_guard / .unrolled_guard this
# module carries NO import-order constraint: its after_create listener hangs off
# MeteringEvent.__table__ and replaces no other module's function
# (billing_metering_events has no partition-minting function to extend). If that
# ever changes -- e.g. the table is partitioned and gains a minting function --
# re-read the ordering comments below before moving this line.
from .metering_unrolled_guard import (
    ALLOW_UNROLLED_METERING_TRUNCATE_GUC,
    ALLOW_UNROLLED_METERING_TRUNCATE_VALUE,
    METERING_UNROLLED_GUARD_DDL,
    UNROLLED_METERING_OVERRIDE_OPERATION,
)
from .quota import ACTIVE_LICENSE_STATUS, LICENSE_STATUSES, RequestQuotaConfig
from .rollups import (
    CostActorDaily,
    CostDaily,
    CostUsageDaily,
    MeteringDaily,
    MeteringHourly,
    MeteringMonthlyUsage,
)
from .tenant_id_divergence import TenantIdDivergenceAudit

# Imported LAST on purpose. Both .delete_audit and .truncate_guard install an
# after_create listener that CREATE OR REPLACEs create_cost_events_partition_for;
# truncate_guard's body is DERIVED from delete_audit's and is the superset, so it
# must be registered second or a minted partition loses its audit trigger. The
# alphabetical ordering ruff enforces happens to give the right answer here --
# this comment exists so a rename does not silently break it.
from .truncate_guard import (
    ALLOW_TRUNCATE_GUC,
    ALLOW_TRUNCATE_VALUE,
    TRUNCATE_GUARD_DDL,
)

# Imported after .truncate_guard, and LAST of the three, for the same reason:
# this module's create_cost_events_partition_for is derived from
# truncate_guard's (which is derived from delete_audit's) and is the superset of
# all three, so registering it earlier would let a later listener overwrite it
# and mint partitions without the watermark gate.
from .unrolled_guard import (
    ALLOW_UNROLLED_TRUNCATE_GUC,
    ALLOW_UNROLLED_TRUNCATE_VALUE,
    UNROLLED_GUARD_DDL,
    UNROLLED_OVERRIDE_OPERATION,
)

__all__ = [
    "ASSET_KIND_ENUM",
    "DBUUID",
    "LICENSE_REFUSED_COST_SOURCE_MARKER",
    "cost_source_marks_refusal",
    "NULL_APP_ID",
    "ATTRIBUTION_ATTRIBUTED",
    "ATTRIBUTION_REFUSED_LICENSE",
    "ATTRIBUTION_SYSTEM",
    "ATTRIBUTION_UNKNOWN",
    "ATTRIBUTION_UNLICENSED",
    "NULL_LICENSE_ID",
    "NULL_PLAN_ID",
    "NULL_ACTOR_ID",
    "NULL_PROJECT_ID",
    "NULL_SUBJECT_ID",
    "AppCostRate",
    "ALLOW_TRUNCATE_GUC",
    "ALLOW_TRUNCATE_VALUE",
    "ALLOW_UNROLLED_METERING_TRUNCATE_GUC",
    "ALLOW_UNROLLED_METERING_TRUNCATE_VALUE",
    "ALLOW_UNROLLED_TRUNCATE_GUC",
    "ALLOW_UNROLLED_TRUNCATE_VALUE",
    "AUDITED_TABLES",
    "AUDIT_LOG_DELETE_REASON_PREFIX",
    "AUDIT_LOG_GUARD_DDL",
    "AUDIT_LOG_GUARD_DROP_DDL",
    "AUDIT_LOG_TRUNCATE_REASON_PREFIX",
    "GUARDED_AUDIT_TABLES",
    "BUDGET_AUDIT_DDL",
    "BUDGET_AUDIT_DROP_DDL",
    "DELETE_AUDIT_DDL",
    "INVOICE_IMMUTABILITY_DDL",
    "INVOICE_IMMUTABILITY_DROP_DDL",
    "INVOICE_STATUSES",
    "TRUNCATE_GUARD_DDL",
    "METERING_UNROLLED_GUARD_DDL",
    "UNROLLED_GUARD_DDL",
    "UNROLLED_METERING_OVERRIDE_OPERATION",
    "UNROLLED_OVERRIDE_OPERATION",
    "DELETE_REASON_UNSPECIFIED",
    "AuditLog",
    "DlqReattribution",
    "DlqArchive",
    "BillingDeleteAudit",
    "ATTRIBUTION_MATCH_PROJECT_VALUES",
    "AttributionRule",
    "BillingInvoice",
    "BillingInvoiceCounter",
    "BudgetOverageAlert",
    "CreditLedgerEntry",
    "CostActorDaily",
    "CostUsageDaily",
    "CostDaily",
    "CostEvent",
    "FxRate",
    "MarkupRule",
    "MeteringDaily",
    "MeteringEvent",
    "MeteringHourly",
    "MeteringMonthlyUsage",
    "MoneyBudget",
    "MoneyBudgetAdjustment",
    "ProcessedCostEventKey",
    "ProcessedCostMessage",
    "ProcessedEventMessage",
    "ProviderCost",
    "ProviderPricing",
    "ACTIVE_LICENSE_STATUS",
    "AppRegistry",
    "LICENSE_STATUSES",
    "RequestQuotaConfig",
    "TenantAccessCharge",
    "TenantConfig",
    "TenantIdDivergenceAudit",
    "TenantWebhook",
    "TokenBudget",
    "TokenUsage",
    "_new_uuid",
    "_utcnow",
    "classify_cost_attribution",
    "is_system_license",
]
