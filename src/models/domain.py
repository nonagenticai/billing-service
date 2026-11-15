"""Pydantic schemas for API request/response models."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel, BeforeValidator, Field, field_validator, model_validator

from tenancy import canonical_tenant_id


def _canonical_tenant(v: Any) -> Any:
    """Resolve a request-body tenant identifier, or refuse it.

    WHY THIS EXISTS. Since ``0011`` every ``tenant_id`` column is a real
    Postgres ``uuid``. ``require_tenant`` resolves the ``X-Tenant-Id`` HEADER —
    a registered tenant NAME resolves to its uuid, junk is a clean 400 — but a
    tenant id arriving in a request BODY reached the column with no resolver at
    all. Measured 2026-08-25 against the running app:

        POST /api/budget {"tenant_id": "n8n-dev"}     -> 400 INVALID_IDENTIFIER
        POST /api/budget {"tenant_id": "019de842-…"}  -> 201

    ...while the very same name resolves fine in the header. That asymmetry is
    the defect: the same identifier meant two different things depending on
    which part of the request carried it, and the body path failed as an opaque
    database error rather than a stated refusal.

    ``None`` passes through — a nullable tenant column means "no tenant", which
    is not the same as an unresolvable one.
    """
    if v is None:
        return None
    resolved = canonical_tenant_id(v)
    if resolved is None:
        # Deliberately does NOT echo the value: this field has been observed
        # carrying a raw bearer token in production.
        raise ValueError("not a known tenant (send a tenant UUID or a registered tenant name)")
    return resolved


#: A tenant identifier in a request body: any uuid spelling, or a registered
#: tenant NAME. Always stored as the canonical lowercase uuid; the name is
#: resolved, never stored. An unknown value is rejected at validation.
TenantId = Annotated[str, BeforeValidator(_canonical_tenant)]

#: The nullable form. ``None`` means "no tenant", and is preserved.
OptionalTenantId = Annotated[str | None, BeforeValidator(_canonical_tenant)]


# --- Response envelope (T3.2) ---
#
# Every JSON-returning endpoint wraps its payload in ``Envelope[T]`` so
# codegen clients can write a single generic ``Result<T>`` unwrap path.
# Endpoints OUTSIDE the envelope (intentional):
#   * /health, /health/live, /health/ready — infra contracts
#   * /openapi.json, /metrics — Prometheus + OpenAPI tools
#   * /api/export/audit.csv, /api/export/billing.csv — text/csv streams
class Meta(BaseModel):
    """Per-response metadata. Always carries ``request_id`` from
    ``RequestIdMiddleware`` (src/main.py); optional fields populated by
    handlers that page or stream."""

    request_id: str | None = None
    next_cursor: str | None = None
    total: int | None = None


class Envelope[T](BaseModel):
    """Uniform `{data, meta}` wrapper for JSON responses."""

    data: T
    meta: Meta = Field(default_factory=Meta)


def envelope(data: Any, request: Request | None = None, **meta_kwargs: Any) -> dict:
    """Helper for handlers — returns a plain dict that FastAPI will
    serialize through the route's ``response_model=Envelope[X]``.

    Returning a dict (rather than ``Envelope(data=..., meta=...)``) avoids
    a pydantic round-trip; the response_model handles the validation +
    schema generation."""
    rid = None
    if request is not None:
        rid = getattr(request.state, "request_id", None)
    return {"data": data, "meta": Meta(request_id=rid, **meta_kwargs).model_dump()}


# --- Health ---


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


class LivenessResponse(BaseModel):
    status: str = "alive"


class ReadinessResponse(BaseModel):
    """Readiness body.

    ``status`` is tri-state: ``ready`` | ``degraded`` | ``not_ready``.
    ``degraded`` names every dependency that is unhealthy but NOT fatal to
    readiness, so a 200 never hides a fault — see :func:`main.readiness`.
    """

    status: str
    database: str
    redis: str
    degraded: list[str] = Field(default_factory=list)


# --- Enums ---


class Granularity(StrEnum):
    hourly = "hourly"
    daily = "daily"


class BudgetPeriod(StrEnum):
    monthly = "monthly"
    weekly = "weekly"


# --- Usage ---


class ServiceUsage(BaseModel):
    service_key: str
    request_count: int
    denied_count: int = 0


class LicenseUsage(BaseModel):
    license_id: str
    request_count: int
    denied_count: int = 0


class UsageResponse(BaseModel):
    tenant_id: str | None = None
    tenant_name: str | None = None
    license_id: str | None = None
    app_id: str | None = None
    start: datetime
    end: datetime
    granularity: str
    total_requests: int
    total_denied: int
    by_service: list[ServiceUsage] = []
    by_license: list[LicenseUsage] = []


class UsageSummary(BaseModel):
    tenant_id: str
    tenant_name: str | None = None
    start: datetime
    end: datetime
    total_requests: int
    total_denied: int
    by_service: list[ServiceUsage] = []


# --- Per-user time-series + breakdown (self-service /api/usage/me/*) ---


class UsageSeriesBucket(BaseModel):
    """One bucket in the per-user time-series.

    `cost_usd` is the raw provider/internal cost (no markup) — kept for
    backwards compatibility and for super-admin consumers. `billed_cost_usd`
    applies the active tenant's markup and is what the user-facing dashboard
    should render. Both are string-encoded decimals; `"0"` when cost data
    isn't available (top-level `cost_unavailable` flag will also be true).
    """

    ts: datetime
    requests: int
    tokens: int = 0
    cost_usd: str = "0"
    billed_cost_usd: str = "0"
    # Estimated public *provider* cost for BYO usage in this bucket (public list
    # price × tokens). None when the bucket has no BYO usage or no list rate
    # covers the provider — distinct from "0". See services.list_pricing.
    provider_estimate_usd: str | None = None


class UsageSeriesResponse(BaseModel):
    window: str
    granularity: str
    start: datetime
    end: datetime
    buckets: list[UsageSeriesBucket] = []
    # True when SOME OR ALL of the cost data behind this window is missing:
    # the granularity has no cost rollup (hourly today), the cost_daily table
    # is empty for the whole window, OR at least one day that HAS metered
    # requests has no cost_daily row at all. UI should render `—` for the
    # affected cost cells instead of a misleading $0.00.
    #
    # The partial case used to leave this False, so a window with one good day
    # and twenty-nine gap days reported `cost_usd:"0"` on the gaps as if they
    # were measured zeros. The gap is now expressible rather than fatal — the
    # request still succeeds and still returns every bucket it has.
    cost_unavailable: bool = False
    # The specific days (ISO ``YYYY-MM-DD``, ascending) that had metered
    # requests but no cost row. Empty when there is no partial gap — including
    # when cost is unavailable for the WHOLE window, which `cost_unavailable`
    # already says on its own.
    cost_unavailable_days: list[str] = []
    # Markup percentage applied to produce `billed_cost_usd`. Null when
    # markup couldn't be resolved (no tenant context); buckets will then
    # have billed_cost_usd == cost_usd.
    markup_pct: float | None = None


class UsageBreakdownEntry(BaseModel):
    service: str
    requests: int
    tokens: int = 0
    cost_usd: str = "0"
    billed_cost_usd: str = "0"
    # Estimated public provider cost for BYO usage on this service; None when
    # the service has no BYO usage or no list rate covers the provider.
    provider_estimate_usd: str | None = None


class UsageBreakdownResponse(BaseModel):
    window: str
    start: datetime
    end: datetime
    services: list[UsageBreakdownEntry] = []
    # Same partial-gap semantics as UsageSeriesResponse, one dimension over:
    # True when the whole window has no cost rows OR at least one service with
    # metered requests has no cost row, so its `cost_usd:"0"` is an ABSENCE and
    # not a measured zero.
    cost_unavailable: bool = False
    # The specific service keys (ascending) that had requests but no cost row.
    cost_unavailable_services: list[str] = []
    markup_pct: float | None = None


# --- System-wide super-admin usage (admin dashboard) -----------------------


class SystemTenantUsage(BaseModel):
    """One row in the system-wide per-tenant breakdown.

    `incurred_cost_usd` is raw provider/internal cost (no markup, no allowance
    netting). `effective_incurred_cost_usd` is that cost AFTER the tenant's
    included-usage allowance has been consumed -- the base markup is actually
    applied to. `billed_cost_usd` is what the tenant is charged.

    `markup_pct` is the blended markup RATE over the NETTED base, so the row is
    self-checking:

        billed_cost_usd  == effective_incurred_cost_usd * (1 + markup_pct/100)
        incurred - effective == the allowance credited this window

    It is deliberately NOT `billed/incurred - 1`: that is the realised uplift on
    raw cost, a different quantity that goes NEGATIVE whenever an allowance is
    granted, and this field's fallback arm publishes a configured rate. The
    reader can still compute realised uplift from the two cost columns.
    """

    tenant_id: str
    tenant_name: str | None = None
    requests: int
    input_tokens: int
    output_tokens: int
    incurred_cost_usd: str
    effective_incurred_cost_usd: str
    markup_pct: float
    billed_cost_usd: str


class SystemProviderUsage(BaseModel):
    """One row in the system-wide per-provider breakdown.

    Incurred-only: providers are external entities we pay, not parties
    we bill, so a "billed" column would be meaningless here.
    """

    provider: str
    requests: int
    input_tokens: int
    output_tokens: int
    incurred_cost_usd: str


class SystemUsageGrandTotal(BaseModel):
    requests: int
    input_tokens: int
    output_tokens: int
    incurred_cost_usd: str
    billed_cost_usd: str


class SystemUsageResponse(BaseModel):
    """Aggregated system-wide usage + cost for the super-admin dashboard.

    Always returns "system-wide" — ignores X-Tenant-Id. Use `/api/usage/me`
    when scoped data is wanted; this endpoint deliberately crosses tenants.
    """

    window: str
    start: datetime
    end: datetime
    generated_at: datetime
    grand_total: SystemUsageGrandTotal
    by_tenant: list[SystemTenantUsage] = []
    by_provider: list[SystemProviderUsage] = []


# --- Quota ---


class QuotaStatus(BaseModel):
    license_id: str
    plan_id: str | None
    quota_monthly: int | None
    used_this_month: int
    remaining: int | None
    over_quota: bool


class TenantQuotaStatus(BaseModel):
    tenant_id: str
    licenses: list[QuotaStatus]


# --- Costs ---


class ProviderCostBreakdown(BaseModel):
    provider: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: Decimal = Decimal("0")
    request_count: int = 0


class ProjectCostBreakdown(BaseModel):
    """One project's slice of cost — the project-axis twin of ProviderCostBreakdown.

    ⛔ THE UUID ONLY, NEVER A NAME. Billing stores ``project_id`` and nothing
    else about a project; resolving a uuid to a human-readable project name
    stays in fr, where the lookup is scoped by the caller's tenant. A project id
    is caller-supplied and unverified, so if THIS model carried a ``name`` the
    resolver behind it would turn a forged uuid into a read of another tenant's
    project name — the one way a field that otherwise cannot cross a tenant
    boundary would cross one.

    ``project_id`` is a str for the same reason ``LicenseUsage.license_id`` is:
    these DTOs serialise uuids as strings at the edge.
    """

    project_id: str
    # ⭐ ADDITIVE, and the reason the row is safe to emit at all: a project uuid
    # is minted by kanban, not by billing, so it is unique only WITHIN a tenant.
    # `_aggregation.aggregate_by_project` groups by `(tenant_id, project_id)` so
    # two tenants asserting the same uuid can never merge into one row; this
    # field is what keeps those two rows TELLABLE APART in the response. Optional
    # with a `None` default so no existing reader or serialised payload changes
    # shape — it is populated on every row the helper produces.
    tenant_id: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: Decimal = Decimal("0")
    request_count: int = 0


class CostResponse(BaseModel):
    tenant_id: str | None = None
    tenant_name: str | None = None
    start: datetime
    end: datetime
    total_cost_usd: Decimal
    total_input_tokens: int
    total_output_tokens: int
    by_provider: list[ProviderCostBreakdown] = []
    # Two breakdowns of the SAME rows over the same filters, so each sums to
    # total_cost_usd independently. Defaults to [] so a caller reading only
    # by_provider is unaffected.
    by_project: list[ProjectCostBreakdown] = []


class ActorCostLine(BaseModel):
    """One ACTING person's spend in a tenant over the window (billing_cost_actor_daily).

    ``actor_id`` is WHO ACTED (the Keycloak sub); ``subject_id`` is WHO PAID (the licences Subject).
    ``actor_id = 00000000-0000-0000-0000-000000000005`` is spend no person initiated.
    Split by ``byo`` because a BYO line's cost is the customer's own provider bill, not the platform's.
    """

    actor_id: str
    byo: bool
    cost_usd: Decimal
    #: Provider-reported charge; None when no event in the window reported one (never 0 for "unknown").
    vendor_cost_usd: Decimal | None = None
    input_tokens: int
    output_tokens: int
    request_count: int


class ActorCostResponse(BaseModel):
    tenant_id: str
    start: datetime
    end: datetime
    #: Equal to the tenant's CostDaily total over the same window — both rollups share one filter.
    total_cost_usd: Decimal
    by_actor: list[ActorCostLine] = []


class UsageBreakdownTotals(BaseModel):
    events: int
    input_tokens: int
    output_tokens: int
    cost_usd: Decimal


class UsageBreakdownRow(BaseModel):
    """One group of ``/api/usage/breakdown``.

    ``dims`` holds exactly the requested ``group_by`` dimensions. ``project`` /
    ``actor`` / ``app`` are ``null`` for spend with no project / no acting person /
    no app; ``app`` is the registry app KEY when registered, else the app uuid.
    ``funding`` is one of local_gpu / platform_external / tenant_byo / unknown.
    """

    dims: dict[str, str | None]
    events: int
    input_tokens: int
    output_tokens: int
    cost_usd: Decimal


class UsageCrossBreakdownResponse(BaseModel):
    tenant_id: str
    #: Inclusive UTC days. Data is the daily rollup: complete days only (through yesterday).
    from_date: date = Field(serialization_alias="from")
    to_date: date = Field(serialization_alias="to")
    group_by: list[str]
    actor_id: str | None = None
    totals: UsageBreakdownTotals
    rows: list[UsageBreakdownRow]
    #: True when more groups matched than the row cap; ``totals`` still covers ALL of them.
    truncated: bool = False


class CostSummary(BaseModel):
    start: datetime
    end: datetime
    total_internal_cost_usd: Decimal
    total_input_tokens: int
    total_output_tokens: int
    by_provider: list[ProviderCostBreakdown] = []
    by_project: list[ProjectCostBreakdown] = []


class CorrelationCostGroup(BaseModel):
    """One grouped slice of cost for a set of correlation ids.

    ``key`` holds the requested group-by dimensions (e.g.
    ``{"stage": "narration", "model": "claude-opus-4-8"}``); empty when no
    group_by was requested (single all-up total).
    """

    key: dict[str, str] = {}
    cost_usd: Decimal = Decimal("0")
    input_tokens: int = 0
    output_tokens: int = 0
    event_count: int = 0


class CorrelationCostResponse(BaseModel):
    """Priced cost for one or more correlation ids (e.g. w2v run_ids).

    Read over raw ``cost_events`` (correlation_id indexed) so cost can be sliced
    by the caller's operation — what the per-tenant CostDaily rollup cannot do.
    """

    correlation_ids: list[str] = []
    group_by: list[str] = []
    total_cost_usd: Decimal = Decimal("0")
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    event_count: int = 0
    groups: list[CorrelationCostGroup] = []


# --- Per-project cost (fr project cockpit) ---


class ProjectCostDay(BaseModel):
    """One SETTLED day of a project's cost, read from ``billing_cost_daily``."""

    day: date
    cost: Decimal = Decimal("0")
    calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0


class ProjectCostProvisional(BaseModel):
    """Raw ``billing_cost_events`` AFTER the rollup watermark — not yet settled.

    ``since`` is the first UTC day the rollup has NOT covered (watermark + 1);
    ``None`` when nothing has ever been rolled up, in which case every retained
    raw event is provisional.
    """

    since: date | None = None
    cost: Decimal = Decimal("0")
    calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0


class ProjectCardCost(BaseModel):
    """Cost grouped by ``metadata->>'card_id'``; ``card_id=None`` = no card asserted."""

    card_id: str | None = None
    cost: Decimal = Decimal("0")
    calls: int = 0


class ProjectCostTotals(BaseModel):
    """settled + provisional. ``by_card`` is NOT guaranteed to sum to this — see
    :class:`ProjectCostResponse`."""

    cost: Decimal = Decimal("0")
    calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    settled_cost: Decimal = Decimal("0")
    provisional_cost: Decimal = Decimal("0")


class ProjectCostResponse(BaseModel):
    """Same-day per-project cost for the fr project cockpit.

    ``settled`` comes from the daily rollup (the invoice source); ``provisional``
    is the raw events on days the rollup has not reached yet, so it is NOT final
    (a re-roll / reprice / duplicate park can still move it). ``by_card`` is read
    from retained RAW events only (the rollup carries no card dimension), so for
    a project older than raw-event retention it covers less than ``totals``;
    ``by_card_since`` is the earliest UTC day it covers.
    """

    project_id: str
    tenant_id: str
    currency: str = "USD"
    settled: list[ProjectCostDay] = []
    provisional: ProjectCostProvisional = Field(default_factory=ProjectCostProvisional)
    by_card: list[ProjectCardCost] = []
    by_card_since: date | None = None
    totals: ProjectCostTotals = Field(default_factory=ProjectCostTotals)
    basis: str
    as_of: datetime


class ProvisionalCostResponse(ProjectCostProvisional):
    """Tenant (or one subject's) cost on UTC days the rollup has NOT covered yet.

    The figures are :class:`ProjectCostProvisional`'s (inherited, so the two
    cannot drift). ``settled_through`` is the rollup watermark (last settled UTC
    day) and ``since`` = watermark + 1; both ``None`` when nothing has been
    rolled up, in which case every retained raw event is provisional. NOT
    final: a re-roll, reprice or duplicate park can still move it.
    """

    tenant_id: str
    subject_id: str | None = None
    currency: str = "USD"
    settled_through: date | None = None
    basis: str
    as_of: datetime


# --- Token Budget ---


class TokenBudgetCreate(BaseModel):
    tenant_id: TenantId
    license_id: str | None = None
    # ``ge=0``: a configured **0 means block-all** (a zero-allowance / suspended
    # plan), not unlimited. Unlimited is the ABSENCE of an active budget — the
    # ``no_active_budget`` short-circuit in ``reserve_budget`` — never a stored
    # 0. Negatives stay rejected. Was ``gt=0``, which made a genuine block-all
    # unrepresentable over the API while EVERY other billing site already
    # treated 0 as block-all: ``_RESERVE_LUA`` guards on ``cap >= 0`` and
    # ``sync-from-licenses`` writes on ``token_budget >= 0``. So a plan
    # suspended upstream to 0 could be persisted by the sync path but not by an
    # operator through this model — the same asymmetry PR #68 fixed on the
    # sibling ``QuotaConfigRequest.quota_monthly`` (routers/quota.py) and did
    # not carry across to here.
    budget_tokens: int = Field(ge=0)
    period: BudgetPeriod = BudgetPeriod.monthly
    includes_input: bool = True
    includes_output: bool = True
    # Stacking fields — lower priority drains first.
    priority: int = 100
    starts_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None
    source: str = "manual"


class TokenBudgetResponse(BaseModel):
    id: str
    tenant_id: str
    license_id: str | None
    budget_tokens: int
    period: str
    includes_input: bool
    includes_output: bool
    active: bool
    priority: int = 100
    starts_at: datetime | None = None
    expires_at: datetime | None = None
    consumed_tokens: Decimal = Decimal("0")
    source: str = "manual"


class TokenBudgetStatus(BaseModel):
    budget_id: str
    tenant_id: str
    license_id: str | None
    period: str
    budget_tokens: int
    used_tokens: int
    remaining_tokens: int
    usage_pct: float
    over_budget: bool
    priority: int = 100
    starts_at: datetime | None = None
    expires_at: datetime | None = None
    source: str = "manual"


class BudgetAggregateStatus(BaseModel):
    """Tenant-level aggregate across all currently-active stacked budgets."""

    tenant_id: str
    total_remaining: int
    budget_count: int
    next_expiry: datetime | None = None
    budgets: list[TokenBudgetStatus] = []


class BudgetReserveRequest(BaseModel):
    """Pre-flight reservation against a tenant's budget pool.

    license-authz calls this on the request hot path to atomically check
    that the tenant has enough budget headroom for the request and reserve
    the tokens for a short window (TTL). The reservation auto-expires; the
    actual cost is debited post-fact via the cost-event flush, so a
    reservation that doesn't end up incurring the full estimated cost
    self-corrects within ``ttl_seconds``.
    """

    license_id: str | None = Field(
        default=None,
        description="Optional license scope. If unset, all active budgets for the tenant are pooled.",
    )
    estimated_tokens: int = Field(
        gt=0,
        description="Caller's worst-case token estimate (input + output).",
    )
    ttl_seconds: int = Field(
        default=60,
        ge=5,
        le=600,
        description="How long the reservation is held before auto-expiring (clamped 5-600s).",
    )


class BudgetReserveResponse(BaseModel):
    """Result of a budget reservation attempt."""

    allowed: bool
    tenant_id: str
    reservation_id: str | None = Field(
        default=None,
        description="Opaque per-reservation handle minted on success, in BOTH settlement modes. ⭐ CORRECTED: this used to read 'NULL while BUDGET_REQUIRE_RESERVATION_HANDLE is off', which was the measure-before-arming deadlock — the handle could not be observed end-to-end until the flag that changes settlement was already armed. A handle is now minted and recorded on every ADMITTED reserve; the flag selects only whether a settlement drains by identity or by anonymous pool. NULL on deny (nothing was held, so there is nothing to discharge) and NULL if the hold could not be recorded — a non-NULL value attests that a hold EXISTS, never that one was intended. Pass it back at settlement (on the cost event's `reservation_id`) so a release drains only this reservation. It is the canonical dashed UUID form and MUST be round-tripped verbatim: it is stored in a Postgres `uuid` column and compared byte-for-byte at settlement.",
    )
    reserved_tokens: int = Field(
        description="Tokens actually held by this reservation (== estimated_tokens on success, 0 on deny)."
    )
    remaining_tokens: int = Field(
        description="Headroom *after* this reservation. Caller can decide whether to retry shortly."
    )
    cap_tokens: int = Field(description="Sum of cap (budget_tokens) across the active budget pool.")
    consumed_tokens: int = Field(description="Already-debited tokens (from completed flushes).")
    in_flight_reserved_tokens: int = Field(
        description="Sum of currently-held reservations (this one + others)."
    )
    reason: str | None = Field(
        default=None,
        description="When allowed=False, machine-readable reason: 'over_budget' | 'no_active_budget' | 'redis_unavailable' | 'tenant_archived'. ⚠️ 'tenant_archived' is the only one that is an AUTHORISATION verdict rather than an accounting one, and it is the only one a caller must treat as terminal: retrying with a smaller estimate cannot help. It is raised in routers/budget.py BEFORE reserve_budget runs, because an archived tenant with no budget row would otherwise be ALLOWED by 'no_active_budget'. ⛔ The other three are bare string literals constructed in services/budget_redis.py and nothing enforces agreement with this list or with metrics.BUDGET_RESERVE_REFUSAL_REASONS -- a new reason must be added in all three places.",
    )


class BudgetResetRequest(BaseModel):
    """Body of ``POST /api/budget/reset/{tenant_id}`` — an operator reset.

    ``reason`` is optional and is recorded on every audit row the reset writes.
    It is the only human context the trail will ever carry for a write that
    hands a tenant back its whole allowance, so a caller that has one should
    send it.
    """

    reason: str | None = None


class BudgetResetResponse(BaseModel):
    """What a successful reset reports.

    ``previous_consumed_total`` is summed from ``effective_consumed`` — the same
    reader the status endpoint uses — so it equals the spend the operator saw on
    screen, not the raw column (which can still hold a stale earlier period).
    """

    tenant_id: str
    reset_count: int
    budget_ids: list[str]
    previous_consumed_total: int


# --- EUR money budgets (alembic 0044; PLAN rev E §3.1) ---
#
# ⛔ CROSS-REPO CONTRACT. inference-router (reserve) and fr/provisioning (put,
# quote, status) are built against these shapes. Amounts on the wire are
# STRINGS with 8 decimals (``"0.00412000"``), except the whole-EUR cap/amount.

#: The reasons a money reserve may refuse with. Closed set (§3.1).
MONEY_RESERVE_REASONS: tuple[str, ...] = (
    "over_budget",
    "no_active_budget",
    "tenant_archived",
    "fx_rate_missing",
    "price_missing",
)
#: The reasons a money quote may 422 with (§3.1).
MONEY_QUOTE_REASONS: tuple[str, ...] = ("price_missing", "fx_rate_missing", "markup_missing")

LlmSetup = Literal[1, 2, 3]

#: A tenant's money-budget currency (PLAN REV F, owner ruling 2026-10-02 late).
#: Every money figure on the wire is in the currency named next to it.
MoneyCurrency = Literal["EUR", "USD", "CAD"]
#: FX quote currencies: rates are stored USD -> X; USD itself is implicitly 1.
FxQuoteCurrency = Literal["EUR", "CAD"]


class MoneyReserveRequest(BaseModel):
    """IR -> billing, once per hop, before dispatch (§3.1)."""

    setup: LlmSetup
    provider: str = Field(min_length=1, max_length=64)
    model: str = Field(min_length=1, max_length=128)
    byo: bool = False
    estimated_input_tokens: int = Field(ge=0)
    max_output_tokens: int = Field(ge=0)
    hop_index: int = Field(default=0, ge=0)
    inference_id: str | None = Field(default=None, max_length=64)
    ttl_seconds: int = Field(default=60, ge=5, le=600)


class MoneyReserveResponse(BaseModel):
    """Body of both the 200 (``data``) and the 429 (``detail``)."""

    allowed: bool
    reservation_id: str | None = None
    currency: MoneyCurrency = Field(
        default="EUR",
        description="The tenant's budget currency; every *_amount is in it. "
        "EUR on a no_active_budget refusal (there is no budget to name one).",
    )
    reserved_amount: str
    cap_amount: str
    consumed_amount: str
    in_flight_amount: str
    remaining_amount: str
    reason: str | None = Field(
        default=None,
        description="On a 429 only: over_budget | no_active_budget | tenant_archived | "
        "fx_rate_missing | price_missing. no_active_budget is a HARD STOP here (unlike the "
        "token path).",
    )


class MoneyBudgetPut(BaseModel):
    """PUT /api/budget/money/{tenant_id}. Whole units only: 10.5 and "10" are 422.

    ``currency`` CREATES the budget in that currency. Against an existing budget
    in a different currency the PUT is a 409
    ``{"detail":{"reason":"currency_change_requires_conversion",...}}``: only
    ``POST /api/budget/money/{tenant_id}/currency`` (owner) may change it.
    """

    amount: int = Field(strict=True, ge=0)
    currency: MoneyCurrency
    period: Literal["monthly", "weekly"] = "monthly"
    source: Literal["provisioning", "conversion", "manual"] = "manual"


class MoneyBudgetPutResponse(BaseModel):
    budget_id: str
    amount: int
    currency: MoneyCurrency
    budget_unit: Literal["money"] = "money"


class MoneyQuote(BaseModel):
    """GET /api/budget/money/quote 200 body. ``fx_rate`` is USD -> ``currency`` ("1" for USD)."""

    currency: MoneyCurrency
    amount: str
    markup_pct: str
    fx_rate: str


class MoneyQuoteRefusal(BaseModel):
    reason: str


class MoneyConsumedSegment(BaseModel):
    currency: MoneyCurrency
    consumed: str


class MoneyCurrencyConversion(BaseModel):
    adjustment_id: str
    at: datetime
    from_currency: MoneyCurrency
    to_currency: MoneyCurrency
    fx_rate: str
    amount_before: int
    amount_after: int
    consumed_before: str
    remaining_before: str
    remaining_after: str


class MoneyCurrencyChange(BaseModel):
    """POST /api/budget/money/{tenant_id}/currency (owner / license-admin only)."""

    currency: MoneyCurrency
    reason: str | None = Field(default=None, max_length=500)


class MoneyCurrencyChangeResponse(BaseModel):
    """``changed`` False = already in that currency; nothing written."""

    changed: bool
    budget_id: str
    adjustment_id: str | None = None
    from_currency: MoneyCurrency
    to_currency: MoneyCurrency
    fx_rate: str
    amount_before: int
    amount: int
    consumed_before: str
    consumed: str
    remaining_before: str
    remaining_after: str
    holds_converted: int | None = Field(
        default=None,
        description="In-flight Redis holds re-denominated; null = Redis unavailable "
        "(the next reserve converts them, see services.money_budget).",
    )


class PrepaidCreditBalance(BaseModel):
    """A tenant's PREPAID CREDIT, in USD (alembic 0055 ledger).

    ``remaining_usd`` = every top-up minus every FROZEN period drawdown. Usage in
    a period not closed yet is not in it: the invoice preview's
    ``prepaid_credit_remaining_usd`` is the provisional figure after the
    current period.
    """

    currency: Literal["USD"] = "USD"
    granted_usd: str
    drawn_usd: str
    remaining_usd: str
    settled_through: str | None = Field(
        description="Last month (YYYY-MM) whose drawdown is frozen by a closed invoice."
    )


class MoneyBudgetStatus(BaseModel):
    """GET /api/budget/money/status/{tenant_id}.

    The money-budget CAP (``amount`` / ``consumed`` / ``remaining``, a per-period
    spend guard in the budget currency) and, beside it, the tenant's PREPAID
    CREDIT (``credit``). They are independent; see ``services.prepaid_credit``.
    """

    amount: int
    consumed: str
    remaining: str
    currency: MoneyCurrency
    thresholds_crossed: list[int]
    consumed_by_currency: list[MoneyConsumedSegment] = Field(
        default_factory=list,
        description="This period's consumption per denomination, in the ORIGINAL "
        "currency of each segment, oldest first. One entry unless the currency was "
        "changed this period; the last entry is the current currency.",
    )
    conversions: list[MoneyCurrencyConversion] = Field(
        default_factory=list, description="This period's currency changes, oldest first."
    )
    credit: PrepaidCreditBalance


#: Idempotency keys: opaque caller-chosen tokens (CP sends a uuid). Bounded and
#: charset-limited because the key is stored, logged and echoed back.
_IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9._:-]+$"

#: A fat-finger guard, not a commercial limit: one top-up of more than a million
#: whole units of any supported currency is refused at the wire (422) rather
#: than invoiced. Raise it deliberately if a real purchase ever needs to.
MONEY_TOPUP_MAX_AMOUNT = 1_000_000


class MoneyTopupRequest(BaseModel):
    """POST /api/budget/money/topup. Whole units only: 10.5 and "10" are 422.

    Buys PREPAID CREDIT: ``amount`` whole units of ``currency``, converted to USD
    at the FX rate effective now (USD needs no rate). ``idempotency_key`` is per
    tenant: a replay with the same key and body returns the original top-up and
    writes nothing; the same key with a different amount or currency is a 409.
    """

    tenant_id: TenantId
    amount: int = Field(strict=True, ge=1, le=MONEY_TOPUP_MAX_AMOUNT)
    currency: MoneyCurrency
    idempotency_key: str = Field(min_length=8, max_length=128, pattern=_IDEMPOTENCY_KEY_PATTERN)


class MoneyTopupInvoiceLine(BaseModel):
    """The billable line a top-up created: a ``one_time`` access charge.

    Itemised by the invoice generator in the month of ``effective_from`` as an
    access-charge line with ``service_key`` = this ``service_key``. The invoice
    ledger is USD-only, so ``amount_usd`` = ``amount / fx_rate`` (USD ->
    currency, 1 for USD) ROUND_HALF_UP to cents.
    """

    access_charge_id: int
    service_key: str
    cadence: Literal["one_time"] = "one_time"
    amount_usd: str
    fx_rate: str
    effective_from: datetime


class MoneyTopupResponse(BaseModel):
    """POST /api/budget/money/topup 200 body.

    ``credit_usd`` is what this top-up added to the credit ledger; ``credit`` is
    the tenant's balance after it (the same body as
    ``GET /api/budget/money/credit/{tenant_id}``).
    """

    topup_id: str
    tenant_id: str
    idempotency_key: str
    replayed: bool = Field(
        description="True when this key was already applied: nothing was written this call."
    )
    amount: int
    currency: MoneyCurrency
    credit_usd: str
    invoice_line: MoneyTopupInvoiceLine
    created_at: datetime
    credit: PrepaidCreditBalance


class RateCard(BaseModel):
    """GET /api/config/rate-card: the STANDARD commercial rate card.

    The exact values ``POST /api/config/tenants/{id}/terms`` materialises for a
    tenant when no override is given (``settings.default_*``). It is a template
    for forms, not any tenant's terms: those are ``GET
    /api/config/tenants/{id}/commercial-terms``.
    """

    currency: Literal["USD"] = "USD"
    platform_fee_usd: Decimal = Field(description="Monthly flat platform fee (USD).")
    seat_fee_usd: Decimal = Field(description="Monthly fee per seat (USD).")
    markup_pct: Decimal = Field(description="Markup % on platform-provided usage.")
    byo_markup_pct: Decimal = Field(description="Markup % on BYO (tenant-key) usage.")
    source: Literal["settings_default"] = "settings_default"


class FxRatePut(BaseModel):
    """PUT /api/config/fx-rates: USD -> EUR | CAD. USD needs no row (it is 1)."""

    base: Literal["USD"] = "USD"
    quote: FxQuoteCurrency = "EUR"
    rate: Decimal = Field(gt=0, max_digits=20, decimal_places=10)
    effective_from: datetime | None = Field(
        default=None,
        description=(
            "Defaults to now. Closes the open row at this instant. The FIRST row "
            "must be back-dated to before the earliest money event that can still "
            "settle: an event older than the first rate settles with NULL "
            "consumption (counted as billing_money_unpriced_events_total"
            '{reason="before_first_fx_rate"}).'
        ),
    )


class FxRateResponse(BaseModel):
    id: int
    base: str
    quote: str
    rate: str
    effective_from: datetime
    effective_until: datetime | None
    set_by: str | None


# --- Billing ---


class LicenseUsageBilling(BaseModel):
    """One license's usage block, as ``services.billing.generate_billing`` emits it.

    ⛔ Same ``extra='ignore'`` drop as ``LicenseCostBilling`` below — see that
    docstring. Measured against the generator's ``licenses[].usage`` dict:
    ``denied_count`` and ``budget_denied_count`` were emitted on every request
    and discarded on the way out, so the REFUSAL axis of a licence's month never
    reached the wire while the admitted-request count did. The two are not
    interchangeable and neither is derivable from the other: ``denied_count`` is
    every refusal (``metering_daily.denied``) and ``budget_denied_count`` is the
    subset refused specifically for want of token budget, which is the one an
    operator can fix by raising a budget.
    """

    request_count: int
    #: Total refusals for this licence this month. Default 0 to match the
    #: ``ServiceUsage`` / ``LicenseUsage`` siblings above, so no existing
    #: construction site changes shape; the generator always emits it.
    denied_count: int = 0
    #: The BUDGET-denied subset of ``denied_count`` (``metering_daily.
    #: budget_denied``). A bare int, never None: the generator coalesces a NULL
    #: column to 0 before accumulating, so "no budget refusals" and "not
    #: measured" are already indistinguishable upstream and widening this to
    #: ``int | None`` would be a claim the producer does not make.
    budget_denied_count: int = 0
    quota_monthly: int | None
    quota_pct: float | None


class LicenseCostBilling(BaseModel):
    """One license's cost block, as ``services.billing.generate_billing`` emits it.

    ⛔ EVERY KEY THE GENERATOR PUTS IN ``licenses[].costs`` MUST BE DECLARED HERE.
    This model has no ``model_config``, so Pydantic v2 defaults to
    ``extra='ignore'`` and an undeclared key is dropped SILENTLY at
    ``routers/billing.py`` — a 200 whose body is missing the field, with nothing
    in the logs. That is the same failure ``InvoiceDocument`` records below, and
    it had already happened here: the INCURRED half of every figure was declared
    and the BILLED half was not, so ``billed_cost_usd``, ``llm_billed_usd``,
    ``app_request_billed_usd``, ``markup_pct`` and ``billed_by_provider`` — the
    entire billed/markup axis, i.e. what the tenant is actually CHARGED — were
    computed on every request and discarded on the way out. A caller could read
    what the spend cost us and never what it cost them.

    ``tests/test_invoice_itemisation_is_self_consistent.py`` derives the expected
    key set from the generator's own output rather than a hardcoded list, so the
    next field added to the generator and forgotten here reds rather than
    vanishes.
    """

    app_request_cost_usd: Decimal = Decimal("0")
    #: The BILLED counterpart of ``app_request_cost_usd`` — incurred × markup.
    #: ⚠️ SUB-CENT ON PURPOSE, like its llm sibling below. ``generate_billing``
    #: rounds ONCE per emitted total, never per line, and
    #: ``services.invoices._money_from_snapshot`` rebuilds the
    #: ``app_request_usage_usd`` / ``app_request_markup_usd`` invoice columns
    #: from exactly this key. Coercing it to cents here would put a figure on the
    #: wire that the frozen statement contradicts.
    app_request_billed_usd: Decimal = Decimal("0")
    llm_cost_usd: Decimal = Decimal("0")
    #: The BILLED counterpart of ``llm_cost_usd``, net of any allowance credit
    #: (``included_applied_usd``) and inclusive of ``platform_fee_usd``. Sub-cent
    #: for the same reason as the app-request pair above.
    llm_billed_usd: Decimal = Decimal("0")
    #: ⭐ ``app_request_billed_usd + llm_billed_usd``, AND IT EQUALS THEM. The
    #: generator renders all three unrounded so a displayed total is the sum of
    #: the displayed components it is built from; rendering only this one in
    #: cents made a bill whose own two addends did not reach it.
    #: ⚠️ NOT a component column of the invoice — the close path sums the two
    #: halves above, never this — so do not reconcile a statement against it.
    billed_cost_usd: Decimal = Decimal("0")
    #: Effective BLENDED markup on this license's incurred usage, as a
    #: percentage. DISPLAY ONLY: it is derived from the two figures above, not
    #: the rate any rule set, and a license billed under several rules has no
    #: single rate. ⚠️ ``0.0`` is overloaded — it means "no markup" and "no
    #: internal cost to measure a markup against" alike, because the generator
    #: emits a bare float rather than None. Declared ``float`` to match what is
    #: actually sent; widening it to ``float | None`` here would be a claim the
    #: producer does not make.
    markup_pct: float = 0.0
    by_provider: dict[str, Decimal] = {}
    #: The BILLED split of ``by_provider``, same keys. Both are needed to read a
    #: per-provider margin off the response; with only the incurred half a caller
    #: has to re-derive the billed side from a blended percentage, which is wrong
    #: the moment two providers on one license bill at different rates.
    billed_by_provider: dict[str, Decimal] = {}
    # ACCESS-axis fee attributed to this license's product (0 when none). Pure
    # access revenue — not an internal cost. See TenantAccessCharge.
    access_charge_usd: Decimal = Decimal("0")
    # ALLOWANCE axis: included-usage credit applied to this license's LLM lines
    # before markup (0 when none). Reduces billed_cost_usd, not internal cost.
    included_applied_usd: Decimal = Decimal("0")
    # PLATFORM-FEE axis: the byo_platform_fee absolute surcharge on this
    # license's BYO lines. ⚠️ A COMPONENT of the billed LLM lines, not an
    # addition to them — do not sum it onto the cost when rendering.
    platform_fee_usd: Decimal = Decimal("0")


class LicenseTokenBilling(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    budget_tokens: int | None = None
    budget_remaining: int | None = None
    budget_pct: float | None = None


class LicenseBilling(BaseModel):
    license_id: str
    app_key: str
    plan_code: str | None = None
    usage: LicenseUsageBilling
    costs: LicenseCostBilling
    tokens: LicenseTokenBilling


class AccessChargeLine(BaseModel):
    """One ACCESS / platform-fee line on a bill (product-scoped or tenant-wide).

    ⭐ A line is ALWAYS present and its amount MAY BE $0. When no
    ``tenant_access_charge`` row matches, the generator emits an explicit zero
    line rather than an empty array, so the itemisation agrees with the
    ``access_charge_usd`` aggregate it summarises. An absent row cannot be
    audited or reconciled; a $0.00 row can.
    """

    service_key: str | None = None
    amount_usd: Decimal
    #: ⚠️ NULLABLE because of the explicit zero line: there is no billing
    #: cadence when there is no charge, and inventing one ("monthly", "none")
    #: would put a fact on the statement that nothing decided.
    cadence: str | None = None
    #: ⭐ THE PER-SEAT LINE'S "$25 x 3". The per-seat producer in
    #: ``services.billing`` emits these three beside ``amount_usd`` with the
    #: comment *"a line reading '$75' without '$25 x 3' is not reconcilable"* —
    #: and undeclared they were dropped silently, so the statement carried
    #: exactly the unreconcilable "$75" that comment forbids.
    #:
    #: ⚠️ ALL THREE ARE ``None`` ON A NON-SEAT LINE, which is what makes them
    #: nullable: a flat monthly charge and the explicit-zero line have no rate
    #: and no quantity, and a 0 there would read as "priced at $0 for 0 seats".
    #: ``seat_rate_usd`` is the RATE, not a charge — ``amount_usd`` is
    #: ``q(rate x count)`` — so it must never be summed into a total.
    seat_rate_usd: Decimal | None = None
    #: The seat QUANTITY this line was resolved at. ``None`` on a non-seat line
    #: AND when licences could not be asked (``seat_basis == "unavailable"``,
    #: where the line falls back to $0.00); ``seat_basis`` is what tells those
    #: two apart, which is why this stays int-or-None rather than flattening
    #: to 0.
    seat_count: int | None = None
    #: How ``seat_count`` was obtained: ``active_licences`` (counted) or
    #: ``unavailable`` (the report path's fail-soft — ``close_period`` refuses
    #: instead). ``None`` on a non-seat line.
    seat_basis: str | None = None
    #: WHY this line exists — `tenant_access_charge` (a real charge matched),
    #: `no_matching_access_charge` (the axis was evaluated and matched nothing),
    #: or `not_applicable_internal_tenant` (the axis does not apply). Required,
    #: with no default: a line that cannot say which of those it is returns the
    #: reader to exactly the ambiguity the zero line was added to remove.
    basis: str


class BillingTotals(BaseModel):
    """The tenant-month rollup, as ``services.billing.generate_billing`` emits it.

    ⛔ EVERY KEY THE GENERATOR PUTS IN ``totals`` MUST BE DECLARED HERE, for the
    ``extra='ignore'`` reason ``LicenseCostBilling`` spells out above.

    ⭐ THE SEAT AXIS IS WHY THIS ONE MATTERED MOST. ``seat_fee_usd`` is in
    ``services.invoices._TOTAL_COMPONENT_COLUMNS`` and the generator's own comment
    calls the seat axis *"disjoint from access_charge_usd above, and both are
    components of total_usd"*. Undeclared, a caller reconciling
    ``customer_cost_usd`` off this response was short by the ENTIRE seat fee with
    nothing in the payload naming the gap — the same shape alembic ``0033`` fixed
    on the invoice side when the app-request stream was missing from
    ``InvoiceDocument``.

    Every field added here carries a default so no existing construction site
    changes shape; the generator emits all of them on every call.
    """

    total_requests: int
    #: The REFUSAL axis of the month, the tenant-level sum of the per-licence
    #: ``LicenseUsageBilling`` counters. Emitted as bare ints.
    total_denied: int = 0
    total_budget_denied: int = 0
    internal_cost_usd: Decimal
    customer_cost_usd: Decimal
    margin_usd: Decimal
    cost_by_provider: dict[str, Decimal] = {}
    #: The BILLED split of ``cost_by_provider``, same keys — the tenant-level
    #: twin of ``LicenseCostBilling.billed_by_provider``, and needed for the same
    #: reason: with only the incurred half a reader has to re-derive the billed
    #: side from a blended percentage, which is wrong the moment two providers
    #: bill at different rates.
    billed_by_provider: dict[str, Decimal] = {}
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    # ACCESS-billing axis: fixed access fees applied this month. Folded into
    # customer_cost_usd + margin_usd above; broken out here for display.
    access_charge_usd: Decimal = Decimal("0")
    access_charges: list[AccessChargeLine] = []
    #: SEAT axis: ``q(seat_rate x seat_count)`` summed over the per-seat rows.
    #: ⚠️ DISJOINT from ``access_charge_usd``, and BOTH are components of
    #: ``customer_cost_usd`` — see the generator at the ``access_total_charged``
    #: derivation, which adds exactly these two quantised figures to the billed
    #: stream. Summing it ONTO ``customer_cost_usd`` double-charges; omitting it
    #: from a reconciliation under-counts by the whole seat fee.
    seat_fee_usd: Decimal = Decimal("0")
    #: ⚠️ THREE DISTINCT MEANINGS, which is why this is ``int | None`` and not an
    #: int defaulting to 0 — read with ``seat_basis``:
    #:   * ``seat_count=None``, ``seat_basis=None``   — no per-seat term applies.
    #:   * ``seat_count=N``, ``seat_basis="active_licences"`` — counted, N seats.
    #:   * ``seat_count=None``, ``seat_basis="unavailable"`` — a per-seat term
    #:     applies but licences could not be asked, so ``seat_fee_usd`` is $0.00
    #:     on the REPORT path only (``services.invoices.close_period`` refuses to
    #:     freeze a statement in this state rather than billing zero seats).
    #: Flattening either to 0 / "" erases the distinction
    #: ``billing_invoices.seat_count`` is nullable precisely to preserve.
    seat_count: int | None = None
    seat_basis: str | None = None
    # PLATFORM-FEE axis: the byo_platform_fee absolute surcharge charged this
    # month. Already inside customer_cost_usd via the billed lines; broken out
    # here because the relative and absolute components are indistinguishable
    # once summed, which is why it was unreportable before.
    platform_fee_usd: Decimal = Decimal("0")
    # BYO REFERENCE axis (billing_mode byo_markup_only): provider cost of BYO lines the customer
    # paid directly — the base the markup was taken from. NOT inside internal_cost_usd (the
    # platform paid nothing) and NOT inside customer_cost_usd (only the markup on it is billed).
    byo_reference_cost_usd: Decimal = Decimal("0")
    # Every markup provenance actually applied to a line this month.
    markup_sources_applied: list[str] = []
    # ALLOWANCE axis: total included-usage credit netted against LLM usage this
    # month (before markup). Already reflected in customer_cost_usd/margin_usd
    # via the reduced per-line billing; broken out here for display.
    included_applied_usd: Decimal = Decimal("0")
    #: PREPAID CREDIT (alembic 0055) netted out of this month's billed LLM usage.
    #: ALREADY DEDUCTED from ``customer_cost_usd``; ⛔ subtracting it again
    #: under-bills by the same amount.
    prepaid_credit_applied_usd: Decimal = Decimal("0")
    prepaid_credit: PrepaidCreditMonth | None = None


class PrepaidCreditMonth(BaseModel):
    """Where ``BillingTotals.prepaid_credit_applied_usd`` came from (USD).

    ``available_usd`` = credit entering this month (top-ups granted by its end
    minus drawdowns frozen for earlier months); ``remaining_usd`` = after this
    month's drawdown, PROVISIONAL until the month is closed; ``frozen`` = the
    month is closed and ``applied_usd`` is its ledger drawdown.
    """

    currency: Literal["USD"] = "USD"
    available_usd: Decimal
    applied_usd: Decimal
    remaining_usd: Decimal
    billed_llm_usd: Decimal
    frozen: bool


BillingTotals.model_rebuild()


class BillingResponse(BaseModel):
    tenant_id: str
    month: str
    markup_pct: float
    #: ⭐ WHERE `markup_pct` came from: `markup_rule:<id>`, `tenant_config`,
    #: `tenant_config_default`, or `settings_default` (a code constant). Same
    #: rate from a signed commercial rule and from a default nobody chose are
    #: the same number and different facts.
    #:
    #: ⚠️ The literal default is duplicated rather than imported from
    #: `services.markup`: that module imports `MarkupRuleCreate` from THIS one,
    #: so importing back would be circular.
    markup_source: str = "unknown"
    #: ⭐ THE REALISED BLENDED RATE, and a DIFFERENT FACT from ``markup_pct``
    #: above: that one is the nominal rate the fallback resolved to, this one is
    #: ``(billed / incurred − 1) x 100`` over the USAGE axis actually billed, so
    #: the two diverge the moment a per-model rule, a BYO line or an absolute
    #: surcharge is in play. ``InvoiceResponse`` carries both for exactly this
    #: reason; the live response carried only the nominal one.
    #:
    #: ⚠️ A BARE FLOAT, and ``0.0`` is overloaded the same way
    #: ``LicenseCostBilling.markup_pct`` documents: the generator emits ``0.0``
    #: both for "no markup realised" and for "no internal cost to measure a
    #: markup against". Widening to ``float | None`` would be a claim the
    #: producer does not make. Rounded to 2dp by the generator, not a Decimal —
    #: it is a displayed rate, not money.
    effective_markup_pct: float = 0.0
    #: ``standard_markup`` | ``byo_platform_fee`` | ``pass_through`` (and the
    #: internal mode), AFTER ``services.byo_mode.effective_billing_mode`` has
    #: resolved it — i.e. the mode this bill was actually computed under, not the
    #: one stored on ``tenant_config``. Without it a reader cannot tell a $0
    #: markup that is the BYO product from a $0 markup that is a misconfiguration.
    #: The default mirrors ``get_billing_mode``'s own no-config-row fallback, so
    #: it invents nothing this service would not already have chosen.
    billing_mode: str = "standard_markup"
    licenses: list[LicenseBilling] = []
    totals: BillingTotals


class BillingOverviewEntry(BaseModel):
    tenant_id: str
    total_requests: int
    internal_cost_usd: Decimal
    customer_cost_usd: Decimal
    #: The UNROUNDED internal cost behind ``internal_cost_usd``.
    #:
    #: ⭐ IT EXISTS SO "NO INTERNAL COST" AND "LESS THAN A CENT OF INTERNAL COST"
    #: STAY DIFFERENT FACTS. ``internal_cost_usd`` is the cent the page displays,
    #: and quantising it is what makes ``customer − internal = margin`` hold at
    #: the cent — but it maps every tenant-month under half a cent onto the same
    #: ``0.00`` as a tenant-month with no spend at all. The margin percentage is
    #: taken against the DISPLAYED denominator (it must be: a ratio of two
    #: figures the page does not show is not reconcilable), so both cases render
    #: with no percentage, and without this field the renderer cannot tell them
    #: apart and asserts the wrong one. Measured on prod 2026-09-15: of 60
    #: tenant-months with any licence-attributed cost, **12 are positive and
    #: sub-cent**, one of them in the current month.
    #:
    #: ⛔ NEVER a denominator or an addend. Nothing bills, totals or divides by
    #: this — it carries information the display rounding destroys, nothing more.
    #: Additive with a default so no existing consumer changes shape.
    #:
    #: ⛔ THE DEFAULT IS THE HAZARD, AND IT FAILS TOWARDS A FALSE STATEMENT. A
    #: producer that builds a ``BillingOverviewEntry`` and forgets this field
    #: gets ``Decimal("0")`` — pydantic raises nothing, because the default makes
    #: it optional. ``routers/dashboard.py`` then computes
    #: ``sub_cent_internal = internal_charged == 0 and internal_exact > 0``,
    #: which is False, and the row renders the plain ``n/a`` whose tooltip says
    #: **"No internal cost this month"** about a tenant that incurred real
    #: sub-cent cost. The omission is therefore INVISIBLE at the type level and
    #: shows up only as a confident false claim on the page.
    #:
    #: ⚠️ Exactly ONE producer populates it today — ``routers/billing.py``'s
    #: ``internal_cost_exact_usd=internal`` in ``compute_billing_overview`` — and
    #: only that one is pinned, by ``tests/test_project_pricing_readers_agree.py``
    #: (the exact-pre-image section, which measures that deleting that single
    #: kwarg turns the assertion red). A SECOND producer would be graded by
    #: nothing. ⛔ If you add one, populate this field and extend that test to
    #: cover it; do not rely on the default being harmless, because it is not.
    internal_cost_exact_usd: Decimal = Decimal("0")
    # REPORT-ONLY, and additive with a default so no existing consumer changes
    # shape. Cost this tenant incurred with no licence attached, which
    # `CostDaily.license_attributed_clause` drops from `internal_cost_usd` and
    # `customer_cost_usd` above and which was previously invisible everywhere.
    # ⛔ It is NOT folded into either total: making the write-off visible is this
    # field's whole job, deciding who pays for it is a business call.
    unlicensed_cost_usd: Decimal = Decimal("0")


class BillingOverview(BaseModel):
    month: str
    tenants: list[BillingOverviewEntry] = []
    platform_total_internal_usd: Decimal = Decimal("0")
    platform_total_customer_usd: Decimal = Decimal("0")
    # The two halves the sentinel used to conflate, reported separately and
    # excluded from the platform totals above exactly as before. Their sum is
    # what `license_attributed_clause` drops; the split says how much of it is
    # a real customer's spend rather than system/S2S cost.
    platform_total_unlicensed_usd: Decimal = Decimal("0")
    platform_total_system_usd: Decimal = Decimal("0")


# --- Config ---


class ProviderCostConfig(BaseModel):
    provider: str
    input_per_1m_usd: Decimal
    output_per_1m_usd: Decimal


class ProviderCostUpdate(BaseModel):
    input_per_1m_usd: Decimal = Field(ge=0)
    output_per_1m_usd: Decimal = Field(ge=0)


class AppCostRateConfig(BaseModel):
    app_key: str
    cost_per_request_usd: Decimal


class AppCostRateUpdate(BaseModel):
    cost_per_request_usd: Decimal = Field(ge=0)


class TenantConfigResponse(BaseModel):
    tenant_id: str
    markup_pct: float
    billing_mode: str = "standard_markup"
    notes: str | None = None


class TenantConfigUpdate(BaseModel):
    markup_pct: float = Field(ge=0)
    billing_mode: (
        Literal["standard_markup", "byo_platform_fee", "byo_markup_only", "pass_through"] | None
    ) = Field(
        default=None,
        description=(
            "One of 'standard_markup', 'byo_platform_fee', 'byo_markup_only', 'pass_through'. "
            "'byo_markup_only' bills a BYO line only its markup percentage (the customer paid "
            "the provider), never the provider cost itself."
        ),
    )
    notes: str | None = None


class MarkupConfig(BaseModel):
    default_markup_pct: float


class MarkupUpdate(BaseModel):
    default_markup_pct: float = Field(ge=0)


class DateRange(BaseModel):
    start: date
    end: date


# --- Audit log ---


class AuditLogEntry(BaseModel):
    id: int
    ts: datetime
    actor_sub: str | None = None
    actor_role: str | None = None
    action: str
    resource_type: str
    resource_id: str | None = None
    tenant_id: str | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    request_id: str | None = None


# --- Webhooks (T1.6) ---


class TenantWebhookCreate(BaseModel):
    url: str = Field(..., description="HTTPS URL to POST overage events to.")
    secret: str = Field(
        ..., min_length=16, description="HMAC signing key (>=16 chars). Verifier-side secret."
    )
    active: bool = True


class TenantWebhookResponse(BaseModel):
    id: str
    tenant_id: str
    url: str
    active: bool
    created_at: datetime
    updated_at: datetime
    # ``secret`` is intentionally omitted from responses — write-only.


# --- Provider pricing (effective-dated) ---


class ProviderPricingCreate(BaseModel):
    provider: str
    model: str | None = None
    # T1.4 — set to scope this pricing row to a specific service_key.
    # NULL = applies to any service_key for this (provider, model).
    service_key: str | None = None
    # 'cost' (default) = authoritative internal rate; 'list' = public provider
    # list price used only for the read-time BYO estimate.
    rate_class: Literal["cost", "list"] = "cost"
    # Constrained: a typo like 'per_tokens' used to be accepted here and then
    # matched no branch in services.pricing._cost_from_row, which falls through
    # to the per_token maths — silently pricing real usage at $0.00.
    #
    # per_second / per_character share per_token's arithmetic but record the unit
    # the emitter packed into the token counts (audio-seconds for ASR,
    # characters for TTS). They exist so a $100/1M-second ASR rate stops sharing
    # a column with a $2.50/1M-token text rate — see services.pricing and
    # tests/test_pricing_ceiling.py. per_megapixel is deliberately absent: every
    # image-gen row is flat per_request, so it would have no user.
    cost_model: Literal[
        "per_token",
        "per_second",
        "per_character",
        "per_page",
        "per_request",
        "self_hosted_amortized",
    ] = Field(
        description=(
            "One of 'per_token', 'per_second', 'per_character', 'per_page', 'per_request', "
            "'self_hosted_amortized'"
        )
    )
    # cost_basis: the PROVIDER CONTRACT fact — 'cash' (a real vendor charge) or
    # 'internal_transfer' (a book entry, no cash leaves the business).
    # Deliberately NOT derived from cost_model (alembic 0022 — see
    # services.pricing's COST_BASIS_BY_COST_MODEL comment for the defect that
    # derivation caused). Defaults 'internal_transfer': every provider live
    # today is self-hosted, so an admin adding a genuine external vendor row
    # must set this explicitly to 'cash' rather than relying on silence.
    cost_basis: Literal["cash", "internal_transfer"] = "internal_transfer"
    input_per_1m_usd: Decimal | None = None
    output_per_1m_usd: Decimal | None = None
    cache_creation_per_1m_usd: Decimal | None = None
    cache_read_per_1m_usd: Decimal | None = None
    per_request_usd: Decimal | None = None
    monthly_fixed_cost_usd: Decimal | None = None
    effective_from: datetime = Field(default_factory=lambda: datetime.now(UTC))
    effective_until: datetime | None = None
    notes: str | None = None

    @model_validator(mode="after")
    def _require_rates_for_cost_model(self) -> ProviderPricingCreate:
        """Reject rows that cannot possibly price correctly.

        A malformed pricing row is worse than a pricing *miss*: a miss writes
        NULL cost_usd and increments ``billing_pricing_miss_total``, so it is
        detectable. A row with cost_model='per_token' and no rates at all
        prices real token usage at $0.00 and looks like a successful charge.
        This is a create-only model (see the ``upsert_pricing`` /
        ``POST /api/config/provider-pricing`` path — there is no partial-update
        model for provider_pricing), so requiring the rate fields here is safe.
        """
        if self.cost_model in ("per_token", "per_second", "per_character", "per_page"):
            if self.input_per_1m_usd is None and self.output_per_1m_usd is None:
                raise ValueError(
                    f"cost_model={self.cost_model!r} requires at least one of "
                    "'input_per_1m_usd' or 'output_per_1m_usd'; both are null, "
                    "which would price all usage at $0.00"
                )
        elif self.cost_model == "per_request":
            if self.per_request_usd is None:
                raise ValueError(
                    "cost_model='per_request' requires 'per_request_usd'; it is "
                    "null, which would price every request at $0.00"
                )
        elif self.cost_model == "self_hosted_amortized" and self.monthly_fixed_cost_usd is None:
            raise ValueError(
                "cost_model='self_hosted_amortized' requires "
                "'monthly_fixed_cost_usd'; it is null, so nothing could be "
                "amortized at billing-report time"
            )
        return self


class ProviderPricingResponse(BaseModel):
    id: int
    provider: str
    model: str | None = None
    service_key: str | None = None
    rate_class: str = "cost"
    cost_model: str
    cost_basis: str = "internal_transfer"
    input_per_1m_usd: Decimal | None = None
    output_per_1m_usd: Decimal | None = None
    cache_creation_per_1m_usd: Decimal | None = None
    cache_read_per_1m_usd: Decimal | None = None
    per_request_usd: Decimal | None = None
    monthly_fixed_cost_usd: Decimal | None = None
    effective_from: datetime
    effective_until: datetime | None = None
    notes: str | None = None


# --- Repricing (backfill cost_usd for events flushed during a pricing gap) ---
#
# `cost_usd` is computed exactly once, at flush time. An event that arrived
# before its provider's pricing row existed is written with cost_usd NULL and
# nothing ever recomputes it — the money is invisible to every report forever.
# These models drive the operator-triggered backfill (POST /api/admin/reprice
# -> services.reprice.reprice_null_costs).

# Hard ceiling on the [since, until) window. billing_cost_events is monthly
# RANGE-partitioned on created_at, so an unbounded window would full-scan every
# partition. 400 days is a little over a year — enough for any real restatement.
MAX_REPRICE_WINDOW_DAYS = 400


class RepriceRequest(BaseModel):
    """Operator request to reprice NULL-cost events in ``[since, until)``.

    ``dry_run`` defaults to **True**: writing money data has to be an explicit
    opt-in, so the default invocation only reports what *would* change.
    """

    # Required, never optional — the window is what makes partition pruning
    # possible on billing_cost_events.
    since: datetime
    until: datetime
    provider: str | None = Field(
        default=None, description="Narrow to a single provider (e.g. 'kokoro'). None = all."
    )
    limit: int = Field(default=1000, ge=1, le=10000, description="Rows scanned per batch.")
    dry_run: bool = True

    @model_validator(mode="after")
    def _validate_window(self) -> RepriceRequest:
        # A caller may send an offset-naive ISO string ("2026-05-21T00:00:00").
        # Normalize to UTC BEFORE comparing — mixing naive and aware datetimes
        # raises TypeError, which would surface as a 500 instead of a 422.
        if self.since.tzinfo is None:
            self.since = self.since.replace(tzinfo=UTC)
        if self.until.tzinfo is None:
            self.until = self.until.replace(tzinfo=UTC)
        if self.since >= self.until:
            raise ValueError("since must be strictly before until")
        if (self.until - self.since) > timedelta(days=MAX_REPRICE_WINDOW_DAYS):
            raise ValueError(f"window must not exceed {MAX_REPRICE_WINDOW_DAYS} days")
        return self


class RepriceProviderStat(BaseModel):
    """Per-provider breakdown of one reprice run."""

    provider: str
    scanned: int
    repriced: int
    still_unpriced: int
    cost_usd_added: Decimal


class RepriceResult(BaseModel):
    """Outcome of one reprice run — identical shape for dry and real runs.

    ``total_cost_usd_added`` is a pure addition: every row counted was
    ``cost_usd IS NULL`` (i.e. contributing $0) before the run.
    """

    scanned: int
    repriced: int
    still_unpriced: int
    total_cost_usd_added: Decimal
    # Distinct cost_daily bucket_dates the repriced events fall into, computed
    # DB-side with the same expression rollup_daily groups by.
    affected_bucket_dates: list[date]
    # Dates actually re-rolled into billing_cost_daily. A SUPERSET of
    # affected_bucket_dates: a real run restates every date in [since, until)
    # that has any cost event, so that re-running the same request after a crash
    # converges the rollup even though the repriced rows are no longer NULL.
    # Always empty on a dry run.
    restated_bucket_dates: list[date]
    by_provider: list[RepriceProviderStat]
    dry_run: bool


# --- Dead-letter operability (inspect + replay the billing:costs:dead stream) ---
#
# The cost consumer quarantines malformed cost events to the ``billing:costs:dead``
# Redis stream instead of billing them at $0 (services.cost_flusher). The
# BillingCostDeadLetter alert runbook tells an operator to inspect that stream and
# replay entries once the root cause (e.g. a missing provider) is fixed — these
# models drive the two admin endpoints that do it safely
# (GET /api/admin/dlq/inspect, POST /api/admin/dlq/replay -> services.dlq_ops).


class DlqEntry(BaseModel):
    """One quarantined cost event as it sits on the dead-letter stream.

    ``fields`` is the original wire payload with the ``_dlq_*`` bookkeeping keys
    (and any leading-underscore internal key) stripped out — i.e. exactly what a
    replay would re-inject once the malformed condition is fixed.
    """

    id: str
    reason: str | None
    dlq_at: str | None
    original_id: str | None
    fields: dict[str, str]


class DlqInspectResponse(BaseModel):
    """Page of dead-letter entries. ``has_more`` is set when the fetch hit the
    (capped) limit, so the operator knows the stream is larger than one page."""

    entries: list[DlqEntry]
    count: int
    has_more: bool


class DlqReplayRequest(BaseModel):
    """Operator request to replay specific dead-letter entries by stream id.

    ``delete_on_success`` defaults to **True**: a successfully re-injected entry
    is removed from ``billing:costs:dead`` so a second replay is a no-op and the
    stream shrinks as it is drained. Set False to re-inject while leaving the
    quarantine record in place (audit / dry-drain).
    """

    ids: list[str] = Field(min_length=1, description="Dead-letter stream ids to replay.")
    delete_on_success: bool = True


class DlqReplayResult(BaseModel):
    """Per-entry outcome of one replay run.

    An id lands in exactly one of the first three lists (or none, if it was not
    found on the stream). ``deleted_from_dlq`` is a subset of ``replayed``. An
    entry that STILL fails validation is left on the dead stream and reported in
    ``still_malformed`` — never re-injected — so a premature replay cannot loop a
    malformed event back through the consumer.
    """

    replayed: list[str]
    already_processed: list[str]
    still_malformed: list[dict[str, str]]
    deleted_from_dlq: list[str]


class DlqDiscardRequest(BaseModel):
    """Operator request to PERMANENTLY drop specific dead-letter entries.

    ``confirm`` defaults to **False**: a plain call is a DRY RUN that reports
    exactly what *would* be discarded and deletes nothing. The operator must opt
    in to the destructive path, the same way ``RepriceRequest.dry_run`` makes an
    operator opt in to writing money data.

    ``reason`` is required and must be non-blank. Once an entry is XDEL'd it is
    gone; this string is the only record of *why* it was dropped, and it is
    written to the audit log and to every per-entry discard log line.
    """

    ids: list[str] = Field(min_length=1, description="Dead-letter stream ids to discard.")
    reason: str = Field(
        min_length=1,
        description="Why these entries are being discarded. Recorded in the audit trail.",
    )
    confirm: bool = False

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, v: str) -> str:
        """``min_length=1`` alone admits ``" "``. A whitespace reason is the same
        as no reason for anyone reading the audit row later."""
        if not v.strip():
            raise ValueError("reason must be non-empty")
        return v


class DlqDiscardResult(BaseModel):
    """Per-entry outcome of one discard run — counts via ``len()`` of each list.

    ``examined`` counts the requested ids actually FOUND on the dead stream (an
    id already gone is examined 0 times and appears in no list). Every examined
    id lands in AT MOST one of ``eligible`` / ``refused``.

    ⚠️ CORRECTED 2026-08-29: this said "exactly one", and that is not what the
    code does. ``examined`` is incremented before the per-id work (dlq_ops, right
    after the XRANGE lookup), and the per-id ``except`` below the XDEL swallows
    and ``continue``s — deliberately, so one bad entry cannot abort a batch. An
    id that fails there is counted in ``examined`` and lands in NEITHER list, so
    ``len(eligible) + len(refused) <= examined``, with equality only when nothing
    failed. A caller asserting equality turns a single Redis hiccup into a hard
    stop on an otherwise-good sweep. ``discarded`` is a
    subset of ``eligible`` and is ALWAYS empty on a dry run — so a dry run and a
    confirmed run that deleted nothing are distinguishable only by the request,
    which is why the audit row records ``confirm``.

    ``refused`` mirrors ``DlqReplayResult.still_malformed``: ``{"id", "reason"}``
    with a low-cardinality reason string.
    """

    examined: int
    eligible: list[str]
    discarded: list[str]
    refused: list[dict[str, str]]


class DlqReattributeRequest(BaseModel):
    """Operator request to re-attribute nil/blank-tenant dead-letter entries (#243).

    ``tenant_id`` is the tenant the spend is billed to. It must resolve to a
    registered, non-nil tenant — checked in
    :func:`services.dlq_ops.resolve_reattribute_target` (400 otherwise), not
    here, because registration is a live-registry question.

    ``confirm`` defaults to **False**: a plain call is a DRY RUN that reports
    what would be re-attributed and writes nothing. ``reason`` is required and
    non-blank; it is stamped onto the re-injected event, the receipt row and the
    audit log.
    """

    ids: list[str] = Field(min_length=1, description="Dead-letter stream ids to re-attribute.")
    tenant_id: str = Field(
        min_length=1,
        description="Registered, non-nil tenant (uuid or registered name) to bill.",
    )
    reason: str = Field(
        min_length=1,
        description="Why this spend belongs to tenant_id. Recorded in the receipt and audit trail.",
    )
    confirm: bool = False

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, v: str) -> str:
        """Same rule as :class:`DlqDiscardRequest`: whitespace is no reason."""
        if not v.strip():
            raise ValueError("reason must be non-empty")
        return v


class DlqReattributeResult(BaseModel):
    """Per-entry outcome of one re-attribution run.

    ``tenant_id`` is the CANONICAL target. Every examined id lands in at most one
    of ``eligible`` / ``already_processed`` / ``still_malformed`` / ``refused``.
    ``reattributed`` (re-injected onto ``billing:costs``) is a subset of
    ``eligible`` and is ALWAYS empty on a dry run. ``deleted_from_dlq`` and
    ``receipt_failed`` partition ``reattributed``: an entry whose Postgres receipt
    could not be committed is billed (marker permanent) but NOT deleted.
    """

    tenant_id: str
    examined: int
    eligible: list[str]
    reattributed: list[str]
    already_processed: list[str]
    still_malformed: list[dict[str, str]]
    refused: list[dict[str, str]]
    receipt_failed: list[str]
    deleted_from_dlq: list[str]


# --- Markup rules (effective-dated; the north-star billing markup source) ---

# The fields that make a markup rule economically meaningful. A rule setting
# none of these prices nothing, yet still closes the open row for its match
# tuple — see MarkupRuleCreate._require_an_economic_field.
MARKUP_ECONOMIC_FIELDS = (
    "markup_pct",
    "surcharge_input_per_1m_usd",
    "surcharge_output_per_1m_usd",
    "surcharge_cache_creation_per_1m_usd",
    "surcharge_cache_read_per_1m_usd",
    "surcharge_per_request_usd",
)


class MarkupRuleCreate(BaseModel):
    # Match dimensions. Omit (NULL) = wildcard. tenant_id NULL = global default.
    tenant_id: OptionalTenantId = None
    license_id: UUID | None = None
    # ⭐ The KANBAN-PROJECT axis (0036). Omit/None = tenant-wide (the rule applies
    # to every project); a value scopes it to that one kanban project. Weighted
    # BETWEEN license and tenant in `services.markup._specificity` (4 > 2 > 1): a
    # project rule overrides the tenant-wide default, a per-licence/app rule
    # still overrides the project rule.
    #
    # ⛔ Requires `tenant_id` — see `_project_rule_requires_a_tenant` below.
    project_id: UUID | None = None
    service_key: str | None = None
    provider: str | None = None
    model: str | None = None
    # ⭐ The COMMERCIAL axis (0027). Omit/None = the rule applies to BOTH BYO and
    # platform-provided lines; true = BYO lines only; false = platform-provided
    # only. A byo=true rule is the ONLY way to put a non-zero percentage on BYO
    # spend under billing_mode byo_platform_fee — see services.markup, which
    # otherwise forces that percentage to 0.
    byo: bool | None = None
    # Relative markup; NULL/omitted = no pct component (surcharge-only rule).
    markup_pct: Decimal | None = Field(default=None, ge=0)
    # Absolute per-1M-token / per-request surcharges (the platform fee).
    # ge=0 on every one: services.markup.compute_billed_usd ADDS these into the
    # billed total, so a negative value is a silent discount (and, with enough
    # tokens, a negative invoice line). Mirrors AccessChargeCreate, which bounds
    # both its money fields. A discount is expressed as a lower markup_pct or an
    # access-charge allowance, never as negative money here.
    surcharge_input_per_1m_usd: Decimal | None = Field(default=None, ge=0)
    surcharge_output_per_1m_usd: Decimal | None = Field(default=None, ge=0)
    surcharge_cache_creation_per_1m_usd: Decimal | None = Field(default=None, ge=0)
    surcharge_cache_read_per_1m_usd: Decimal | None = Field(default=None, ge=0)
    surcharge_per_request_usd: Decimal | None = Field(default=None, ge=0)
    effective_from: datetime = Field(default_factory=lambda: datetime.now(UTC))
    effective_until: datetime | None = None
    notes: str | None = None

    @model_validator(mode="after")
    def _require_an_economic_field(self) -> MarkupRuleCreate:
        """Reject a rule that prices nothing.

        Every field on this model has a default, so before this validator a body
        with **no fields at all** validated cleanly and POST /api/config/
        markup-rules returned 201 for ``{}`` (reproduced in production
        2026-08-08, markup_rules id=3). The junk row itself is arithmetically
        harmless — specificity 0 means services.markup falls through to
        ``fallback_pct`` — but the upsert **closes the currently-open row for the
        same match tuple**, so an empty POST silently retires the seeded global
        default, and there is no DELETE endpoint to undo it.

        Deliberately NOT a selector requirement: an all-wildcard rule is the
        legitimate shape of the global default (markup_rules id 1 pins no
        dimension and carries markup_pct=30.00), and
        ``services.markup._specificity`` scores it 0 precisely so it can act as
        the fall-through. Requiring a selector would forbid the global default.

        ``is not None``, never truthiness: a 0 markup or a 0 surcharge is an
        explicit "charge nothing on this dimension", not an absent field.
        """
        if all(getattr(self, name) is None for name in MARKUP_ECONOMIC_FIELDS):
            raise ValueError(
                "a markup rule must set at least one economic field: "
                "'markup_pct' or one of 'surcharge_input_per_1m_usd', "
                "'surcharge_output_per_1m_usd', "
                "'surcharge_cache_creation_per_1m_usd', "
                "'surcharge_cache_read_per_1m_usd', "
                "'surcharge_per_request_usd'; all are null, so this rule would "
                "price nothing while closing the currently-open rule for the "
                "same match tuple"
            )
        return self

    @model_validator(mode="after")
    def _project_rule_requires_a_tenant(self) -> MarkupRuleCreate:
        """Refuse a project-scoped rule that pins no tenant, at the WIRE.

        ``services.markup._matches`` refuses such a row outright (``if
        row.project_id is not None and row.tenant_id is None: return False``)
        because project ids are minted by kanban, not here, so a tenantless
        project rule would otherwise reprice any tenant that happened to carry
        the same project id. That guard makes the row INERT, not rejected: it
        would be accepted with 201, listed by GET /markup-rules, shown with a
        non-zero ``specificity``, and never match a single line — a configured
        term that silently does nothing, which is exactly the shape
        ``_per_seat_carries_no_allowance`` (AccessChargeCreate) refuses.

        Worse than inert: the row still takes the open slot for its tuple under
        ``ix_markuprules_open``, so a later, CORRECT tenant+project rule is a
        different tuple and both sit open with the broken one visible in every
        listing. Refusing here is the honest answer.
        """
        if self.project_id is not None and self.tenant_id is None:
            raise ValueError(
                "'project_id' requires 'tenant_id': project ids are minted by "
                "kanban, not by this service, so a tenantless project rule "
                "would name a project under every tenant. "
                "services.markup._matches refuses that shape, so such a rule "
                "would be stored, listed, and never match anything"
            )
        return self


class MarkupRuleResponse(BaseModel):
    id: int
    tenant_id: str | None = None
    license_id: UUID | None = None
    # ⭐ The KANBAN-PROJECT axis (0036). NULL = tenant-wide.
    project_id: UUID | None = None
    service_key: str | None = None
    provider: str | None = None
    model: str | None = None
    byo: bool | None = None
    markup_pct: Decimal | None = None
    surcharge_input_per_1m_usd: Decimal | None = None
    surcharge_output_per_1m_usd: Decimal | None = None
    surcharge_cache_creation_per_1m_usd: Decimal | None = None
    surcharge_cache_read_per_1m_usd: Decimal | None = None
    surcharge_per_request_usd: Decimal | None = None
    effective_from: datetime
    effective_until: datetime | None = None
    notes: str | None = None
    # Specificity score of this rule under the consumption-dominant ladder —
    # populated on resolution responses so callers can see why a rule won.
    specificity: int | None = None


# --- Cost attribution rules (alembic 0041) ---


class AttributionRulePut(BaseModel):
    """Upsert one ``billing_attribution_rules`` row, keyed on its match tuple.

    Match tuple: ``(source_tenant_id, source_subject_id, match_project)``.
    ``source_subject_id`` omitted = any subject under the source tenant.
    ``actor_id`` omitted = re-tenant only, keep whatever actor arrived.
    """

    source_tenant_id: TenantId
    source_subject_id: UUID | None = None
    match_project: Literal["any", "with_project", "without_project"]
    bill_tenant_id: TenantId
    actor_id: UUID | None = None
    reason: str = Field(..., min_length=1, max_length=2000)
    enabled: bool = True


class AttributionRuleResponse(BaseModel):
    id: UUID
    source_tenant_id: str
    source_subject_id: UUID | None = None
    match_project: str
    bill_tenant_id: str
    actor_id: UUID | None = None
    reason: str
    enabled: bool
    created_at: datetime


# --- Access charges (effective-dated; the ACCESS-billing axis) ---


class AccessChargeCreate(BaseModel):
    # tenant_id is required — an access charge is inherently tenant-scoped.
    tenant_id: TenantId
    # NULL/omitted = tenant-wide (all products); a value scopes it to one product.
    service_key: str | None = None
    amount_usd: Decimal = Field(ge=0)
    # `per_seat` (2026-09-10): amount_usd is a PER-SEAT rate multiplied by the
    # tenant's seat count at bill time, not a flat charge. Bills every effective
    # month like `monthly`. One seat = one active licence (services.seats).
    cadence: Literal["monthly", "one_time", "per_seat"] = "monthly"
    # Included-usage allowance: a USD credit that offsets incurred LLM cost
    # BEFORE markup. cadence='monthly' → recurring, resets each billed month;
    # cadence='one_time' → durable prepaid credit drawn down across months.
    # ⛔ REFUSED on cadence='per_seat': an allowance that scales with seats is a
    # different product and is undesigned. Accepting it silently would grant one
    # flat allowance for any seat count, so it is rejected at the wire rather
    # than quietly ignored downstream.
    included_cost_usd: Decimal | None = Field(default=None, ge=0)
    effective_from: datetime = Field(default_factory=lambda: datetime.now(UTC))
    effective_until: datetime | None = None
    notes: str | None = None

    @model_validator(mode="after")
    def _per_seat_carries_no_allowance(self) -> AccessChargeCreate:
        """Refuse `included_cost_usd` on a per-seat row at the WIRE.

        `services.access_charge.active_allowances_all_tenants` only folds in
        `cadence == "monthly"` rows, so a per-seat allowance would be accepted,
        stored, shown by the read-back endpoint, and then never applied — a
        configured term that silently does nothing. Refusing is the honest
        answer; a seat-scaled allowance is a product decision nobody has made.
        """
        if self.cadence == "per_seat" and self.included_cost_usd is not None:
            raise ValueError(
                "included_cost_usd is not supported on cadence='per_seat' "
                "(a seat-scaled allowance is undesigned); omit it, or use a "
                "separate cadence='monthly' row for the allowance"
            )
        return self


class AccessChargeResponse(BaseModel):
    id: int
    tenant_id: str
    service_key: str | None = None
    amount_usd: Decimal
    cadence: str
    included_cost_usd: Decimal | None = None
    effective_from: datetime
    effective_until: datetime | None = None
    notes: str | None = None


class CommercialTermsRequest(BaseModel):
    """Overrides for `POST /api/config/tenants/{id}/terms`. Omit = use the default.

    Every field is optional on purpose: the common call is an empty body meaning
    "put this tenant on the standard rate card", and a provisioning caller that
    knows nothing about pricing can make exactly that call.
    """

    platform_fee_usd: Decimal | None = Field(default=None, ge=0)
    seat_fee_usd: Decimal | None = Field(default=None, ge=0)
    markup_pct: Decimal | None = Field(default=None, ge=0)
    byo_markup_pct: Decimal | None = Field(default=None, ge=0)
    notes: str | None = None


class CommercialTerm(BaseModel):
    """One term, with the PROVENANCE of its value.

    ⭐ The provenance is not decoration. A `value` of 20 means something
    different when it came from an effective-dated rule an operator wrote than
    when it fell back to a code constant nobody decided — and the second case is
    what every tenant in this estate was silently on before 2026-09-10. A terms
    page that shows only numbers cannot tell an operator which tenants have
    actually been priced.

    `value` is None when the term does not exist for this tenant. That is NOT
    zero: no platform fee configured and a $0.00 platform fee are different
    commercial facts, and `source` distinguishes them.
    """

    value: Decimal | None = None
    source: str
    effective_from: datetime | None = None
    effective_until: datetime | None = None


class CommercialTermsResponse(BaseModel):
    tenant_id: str
    platform_fee: CommercialTerm
    seat_fee: CommercialTerm
    markup: CommercialTerm
    byo_markup: CommercialTerm
    #: Live seat count and how it was obtained — `active_licences`, or
    #: `unavailable` when licences could not be asked. None when the tenant has
    #: no per-seat term, which is why this is nullable rather than 0.
    seat_count: int | None = None
    seat_basis: str | None = None
    #: True when every term came from a real row rather than a code default —
    #: the one-field answer to "has this tenant been priced?"
    fully_configured: bool


# --------------------------------------------------------------------------- #
# The WHOLE rate card as ONE atomic operation
#
# ⛔ THE DEFECT THIS CLOSES IS NOT "there was no endpoint" — IT WAS THAT THERE
# WERE FOUR. Configuring a billed tenant (CommonPractice: flat platform fee +
# seats + BYO markup, nothing else) took `PUT /tenants/{id}` for the
# `billing_mode`, `POST /access-charges` for the platform fee, a second
# `POST /access-charges` for the seat rate and `POST /markup-rules` for the
# markup — four requests, four transactions. `licenses`'
# `provision_tenant_offering` saga drives them, and it rolls back only its OWN
# local posture: billing has no DELETE for a markup rule and no DELETE for an
# access charge, so a saga that dies after request three leaves a tenant with a
# platform fee and a seat rate and NO markup, permanently, with nothing in
# billing able to tell that apart from a deliberately markup-free tenant.
#
# ⚠️ `POST /tenants/{id}/terms` (above) is NOT this endpoint and does not
# supersede it. It materialises the STANDARD rate card — a tenant-wide markup
# and a tenant-wide `__seat__` rate — and it deliberately SKIPS any term that
# already exists, which is the right shape for provisioning-by-default. It sets
# no `billing_mode` at all, and its terms pin no `service_key`, so it cannot
# express "CommonPractice is billed 20% on its BYO spend and other products are
# not". Both exist on purpose; see `CommercialTermsPut.__doc__` for which to
# reach for.
# --------------------------------------------------------------------------- #


class CommercialTermsPut(BaseModel):
    """The COMPLETE commercial terms for one tenant. Every field is required.

    ⭐ REQUIRED, NOT OPTIONAL, AND THAT IS THE WHOLE CONTRACT. This is a PUT:
    it states what the tenant's terms ARE, not which of them to nudge. A
    partial body is a PATCH, and a PATCH is precisely the shape that produced
    the half-configured tenants this endpoint exists to make impossible — a
    caller that may omit the markup can forget the markup, and a forgotten
    markup is silent lost revenue rather than a failed request. If a caller
    wants to change one term it re-states all four, which is cheap, because it
    can read them back from `GET` on the same path first.

    `seat_app_key` is the PRODUCT the per-seat rate is counted over — it becomes
    the access charge's `service_key` as `seat:<app_key>`, which is the form
    :func:`services.seats.seat_scope_for_charge` recognises and turns into a
    period-scoped, per-app seat count. A bare app key, never a pre-prefixed one;
    see `_seat_app_key_is_not_already_prefixed`.

    `markup_service_key` scopes the BYO markup rule to one product, so pricing
    CommonPractice's BYO spend does not re-price every other product the tenant
    consumes. It is a separate field from `seat_app_key` on purpose: licences'
    app key and billing's `service_key` are different namespaces that happen to
    agree today, and collapsing them here would bake that coincidence in.

    `effective_from` is honoured rather than forced to `now` because both
    underlying stores are effective-dated and an operator agreeing terms that
    start on the 1st must be able to say so. Omit it and every term starts now.
    """

    billing_mode: Literal[
        "standard_markup", "byo_platform_fee", "byo_markup_only", "pass_through"
    ] = Field(
        description=(
            "The OUTER gate over every resolved markup rule. 'byo_markup_only' is "
            "CommonPractice's mode: a BYO line bills its markup percentage and NOT "
            "the provider cost, because the customer already paid the provider."
        )
    )
    platform_fee_usd: Decimal = Field(
        ge=0,
        description="Flat monthly platform fee. Becomes a tenant-wide "
        "cadence='monthly' access charge with NO allowance — usage bills on top.",
    )
    seat_app_key: str = Field(
        min_length=1,
        max_length=58,  # 64 (the column) minus len("seat:") minus headroom
        description="Product whose ACTIVE LICENCES are counted as seats, e.g. "
        "'commonpractice'. Stored as service_key 'seat:<app_key>'.",
    )
    seat_fee_usd: Decimal = Field(
        ge=0, description="Per-seat monthly RATE (not a charge) — multiplied by the seat count."
    )
    markup_service_key: str = Field(
        min_length=1,
        max_length=64,
        description="Product the BYO markup rule is scoped to.",
    )
    byo_markup_pct: Decimal = Field(
        ge=0, description="Markup percentage charged on that product's BYO lines."
    )
    effective_from: datetime | None = None
    notes: str | None = None

    @model_validator(mode="after")
    def _seat_app_key_is_not_already_prefixed(self) -> CommercialTermsPut:
        """Refuse a `seat_app_key` that already carries the `seat:` prefix.

        ⛔ THIS ONE IS INVISIBLE UNTIL A CLOSE REFUSES, WEEKS LATER. The router
        writes `service_key = "seat:" + seat_app_key`, and  # pragma: allowlist secret
        :func:`services.seats.seat_scope_for_charge` strips exactly ONE prefix to
        recover the app key. So `seat_app_key="seat:cp"` stores `seat:seat:cp`
        and asks licences for an app called `seat:cp` — which is not an app key,
        so licences answers 422, `get_seat_count` raises
        `SeatCountUnavailableError`, and `close_period` REFUSES that tenant's
        invoice. The row looks perfectly well-formed in every listing until then.

        Refused at the wire rather than silently normalised: "did you mean the
        app 'cp'?" is a guess, and a guess about which product a customer's seats
        are counted over is a guess about money.

        ⚠️ THE IMPORT IS FUNCTION-LOCAL DELIBERATELY. `models.domain` is imported
        by every router and by `services.billing`; a module-level
        `from services.seats import ...` would invert the layering (models
        depending on services) and put `service_auth` + `httpx` on the import
        path of every schema-only consumer, including the OpenAPI generator. The
        constant is READ from its owner rather than copied so the two spellings
        cannot drift — a local copy that fell out of step with
        `seat_scope_for_charge` would re-open exactly the defect below.
        """
        from services.seats import SCOPED_SEAT_KEY_PREFIX

        if self.seat_app_key.startswith(SCOPED_SEAT_KEY_PREFIX):
            raise ValueError(
                f"'seat_app_key' must be the bare app key, not the stored "
                f"service_key: drop the {SCOPED_SEAT_KEY_PREFIX!r} prefix "
                f"(got {self.seat_app_key!r}). The router adds it, and a doubled "
                f"prefix names an app licences does not have, which makes every "
                f"close for this tenant refuse"
            )
        return self


class CommercialTermsView(BaseModel):
    """A tenant's currently-effective commercial terms, each with provenance.

    The response of BOTH `PUT` and `GET /tenants/{id}/commercial-terms`, so a
    caller never needs a second request to learn what it just wrote.

    `changed` names the terms this PUT actually re-dated. It is `[]` on a GET and
    `[]` on an idempotent replay — which is the observable difference between
    "the terms already said this" and "I re-opened four effective-dated
    periods", and is the only way a provisioning caller can tell a retry that did
    nothing from a retry that shredded a live tenant's history.
    """

    tenant_id: str
    #: The mode that will gate every markup resolution for this tenant.
    billing_mode: str
    #: `tenant_config` when a row states it, `settings_default` when the tenant
    #: has no row and is riding the column's server default. Same distinction
    #: :class:`CommercialTerm` draws, and for the same reason.
    billing_mode_source: str
    platform_fee: CommercialTerm
    seat_fee: CommercialTerm
    #: The stored `service_key` of the per-seat term (`seat:<app>`), or None.
    seat_service_key: str | None = None
    byo_markup: CommercialTerm
    #: The `service_key` the BYO markup rule is scoped to, or None.
    markup_service_key: str | None = None
    #: ⚠️ OTHER open `per_seat` lineages for this tenant, which billing WILL ALSO
    #: charge. `services.billing.generate_billing` discriminates a seat term by
    #: `cadence == "per_seat"` ALONE and sums every one it finds, while
    #: `ix_accesscharge_open` is unique per `(tenant, service_key)` — so a legacy
    #: `__seat__` row and a `seat:<app>` row are two OPEN slots and two seat fees
    #: on one invoice. Non-empty here means this tenant is double-billed for
    #: seats. Surfaced rather than silently picked past.
    extra_seat_service_keys: list[str] = []
    #: True when all four terms come from a real row rather than a code default.
    fully_configured: bool
    #: Terms this call re-dated. Always `[]` on GET.
    changed: list[str] = []


class TenantAllowanceUsageEntry(BaseModel):
    """One product's live allowance position for the billed month.

    ``service_key`` is ``None`` for the tenant-wide (NULL) bucket.
    """

    service_key: str | None = None
    used_usd: Decimal
    allowance_usd: Decimal
    remaining_usd: Decimal


class TenantAllowanceUsageResponse(BaseModel):
    """Live used / allowance / remaining per product for a tenant-month.

    A pure compute-on-read fold over ``cost_daily`` that reconciles with the
    invoice's allowance netting. ``entries`` is empty when the tenant has neither
    usage nor an allowance this month.
    """

    tenant_id: str
    month: str
    entries: list[TenantAllowanceUsageEntry]


# --------------------------------------------------------------------------- #
# Invoices
# --------------------------------------------------------------------------- #


class CloseInvoiceRequest(BaseModel):
    """Freeze one tenant-month into a persisted statement.

    ``period_end`` is EXCLUSIVE and both bounds are required rather than derived
    from a ``YYYY-MM`` string. The generator takes a month, so a period that is
    not exactly one calendar month is refused by the service rather than quietly
    widened to the containing month — a statement covering a different span than
    the one asked for is worse than an error.

    ``issue_immediately`` defaults to True. A draft is freely mutable by design,
    so a long-lived draft is not a statement; the automated closer wants the
    number allocated in the same transaction that froze the figures.
    """

    tenant_id: TenantId
    period_start: date
    period_end: date
    issue_immediately: bool = True

    @model_validator(mode="after")
    def _period_is_forward(self) -> CloseInvoiceRequest:
        if self.period_end <= self.period_start:
            raise ValueError("period_end is exclusive and must be after period_start")
        return self


class VoidInvoiceRequest(BaseModel):
    """Void an issued statement, with a reason that is not optional.

    The immutability trigger refuses a void whose ``void_reason`` is null or
    blank, so this bound is the same rule stated one layer earlier and with a
    better error. A void with no recorded cause is a statement that vanished
    with nothing saying why.
    """

    reason: str = Field(min_length=1, max_length=2000)

    @field_validator("reason")
    @classmethod
    def _not_only_whitespace(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("void reason cannot be blank")
        return v


class InvoiceResponse(BaseModel):
    """A persisted statement, as returned by the invoice routes.

    ⚠️ ``excluded_unbillable_usd`` is DISCLOSED, NOT CHARGED. It is spend the
    invoice gate drops (no licence reached billing) and it is deliberately not a
    component of ``subtotal_usd`` or ``total_usd``. Rendering it as if it were a
    line item would invert its meaning.
    """

    id: UUID
    tenant_id: str
    tenant_name: str
    period_start: date
    period_end: date
    status: str
    invoice_number: str | None
    currency: str
    subtotal_usd: Decimal
    total_usd: Decimal
    # --- always-present billing lines --------------------------------------
    # ⭐ AN ABSENT LINE IS NOT A ZERO LINE. These are present on EVERY statement
    # and may be 0.00; a bill with no platform-fee field cannot be audited or
    # reconciled, one with an explicit 0.00 can. They were previously absent
    # from the wire model entirely, so close/issue/void never showed them even
    # though the columns existed.
    #
    # ⚠️ access_charge_usd and platform_fee_usd are COMPONENTS of total_usd, not
    # additions to it: the access fee is folded into the total and the platform
    # fee lives inside the billed usage lines. Summing them onto total_usd would
    # double-charge.
    access_charge_usd: Decimal
    platform_fee_usd: Decimal
    # --- markup, and where it came from ------------------------------------
    #: Nominal rate. ``effective_markup_pct`` is what was actually REALISED
    #: (margin / internal cost), which diverges whenever per-model rules, BYO
    #: lines or absolute surcharges are in play.
    markup_pct: Decimal
    effective_markup_pct: Decimal
    #: ⭐ Provenance of ``markup_pct``: ``markup_rule:<id>``, ``tenant_config``,
    #: ``tenant_config_default``, ``settings_default``, or ``unknown`` for a row
    #: frozen before provenance was recorded.
    markup_source: str
    excluded_unbillable_usd: Decimal
    excluded_unbillable_rows: int
    excluded_by_status: dict[str, Any]
    snapshot_sha256: str
    generator_version: str
    created_at: datetime
    issued_at: datetime | None
    voided_at: datetime | None
    void_reason: str | None


class InvoiceSummary(InvoiceResponse):
    """A persisted statement as the READ routes return it — ``InvoiceResponse``
    plus the component columns a rate-card panel has to render.

    ⭐ A SUBCLASS, NOT A SECOND MODEL, AND NOT A WIDENING OF THE PARENT. Both
    halves of that are deliberate:

    * It SUBCLASSES so there is still exactly one declaration of each inherited
      field and one projector expression behind it
      (``routers.billing._invoice_summary`` builds on ``_invoice_response``).
      A parallel model repeating ``id``/``status``/``total_usd`` would be a
      second definition of the same row, free to drift from the first with
      nothing going red.
    * It does NOT add these fields to ``InvoiceResponse`` itself, because that
      model is the body of ``close``/``issue``/``void`` — three routes this
      change was not asked to touch. Additive is not free on a wire contract
      that ``docs/openapi.json`` pins and that license-authz, fr-backend and
      licenses generate clients from.

    ⭐ THE SIX COMPONENT COLUMNS ARE HERE IN FULL, AND THAT IS THE POINT. They
    are exactly ``services.invoices._TOTAL_COMPONENT_COLUMNS``, the set
    ``assert_total_is_the_sum_of_its_components`` gates a close on, so a panel
    rendering them can show a total that its own displayed lines add up to:

        access_charge_usd + seat_fee_usd + llm_usage_usd + llm_markup_usd
          + app_request_usage_usd + app_request_markup_usd  ==  total_usd

    (to within the one cent ``_TOTAL_IDENTITY_TOLERANCE_USD`` allows — the
    residual is ``q(B) − B`` over the billed stream, bounded by half a cent).

    ⛔ DO NOT ADD ``platform_fee_usd`` OR ``excluded_unbillable_usd`` TO THAT
    SUM. Both are inherited from the parent and both would INVERT if summed:
    the platform fee is a surcharge already folded INSIDE
    ``llm_markup_usd``/``app_request_markup_usd``, and the excluded figure is
    spend the statement deliberately did NOT bill. ``_TOTAL_COMPONENT_COLUMNS``
    names its exclusions for exactly these reasons; read it before changing the
    set here.

    ⚠️ THE TWO TIMESTAMPS A PANEL WANTS ARE INHERITED AND MEAN DIFFERENT THINGS.
    ``created_at`` is the CLOSE instant — the moment the tenant-month was frozen
    — and it is always present. ``issued_at`` is when the number was minted and
    is ``None`` on a draft, so "closed but not yet issued" is a real, renderable
    state and not a missing field. ``voided_at`` + ``void_reason`` say a
    statement was withdrawn; it keeps its number and stays readable.

    ⚠️ EVERY FIELD IS REQUIRED — none carries a default. ``BillingInvoice``
    documents that a money column with no value "is not a zero-value invoice, it
    is a generator that failed to fill one in", and ``BillingOverviewEntry
    .internal_cost_exact_usd`` records what happens when a money field on a wire
    model gets a default: the omission becomes invisible at the type level and
    surfaces only as a confident false claim on the page. A required field makes
    a forgetful producer raise.
    """

    #: What the platform PAID (pre-markup) and what it kept. The COST axis —
    #: never components of ``total_usd``; see the ⛔ above.
    internal_cost_usd: Decimal
    margin_usd: Decimal
    #: The LLM stream, split incurred/markup. ⭐ ``llm_markup_usd`` IS THE BYO
    #: MARKUP LINE for a ``byo_markup_only`` tenant (CommonPractice's mode): the
    #: customer paid the provider directly, so ``llm_usage_usd`` is 0 and the
    #: whole charge is the markup. Reading the fee off ``total_usd`` minus the
    #: fees would re-derive by subtraction a number the row already states.
    #: ⚠️ ``llm_markup_usd`` IS SIGNED: under ``billing_mode == "internal"``
    #: every line bills $0 against a real incurred cost, so the pair reads
    #: ``+X``/``−X`` and nets to zero. Do not render it with a ``>= 0`` assumption.
    llm_usage_usd: Decimal
    llm_markup_usd: Decimal
    #: The APP-REQUEST stream, same split and signed for the same reason.
    app_request_usage_usd: Decimal
    app_request_markup_usd: Decimal
    #: ``q(seat_rate × seat_count)`` summed over this statement's per-seat terms.
    #: ⚠️ DISJOINT from ``access_charge_usd``; both are components of
    #: ``total_usd``. Summing the seat fee onto the access fee double-charges.
    seat_fee_usd: Decimal
    #: ⚠️ ``None`` means seats were NOT part of this statement's basis at all,
    #: which is a different fact from 0 seats — ``billing_invoices.seat_count``
    #: is nullable precisely to keep the two apart, and a renderer that
    #: ``?? 0``s it asserts a zero-seat tenant where none was counted.
    seat_count: int | None
    #: PREPAID CREDIT netted out of this statement's LLM usage (alembic 0055). A
    #: DEDUCTION: ``Σ the six components - prepaid_credit_applied_usd == total_usd``.
    prepaid_credit_applied_usd: Decimal
    #: The mode this statement was computed under, frozen on the row.
    #: ⚠️ It is the TENANT-WIDE mode and is no longer the whole truth: since the
    #: per-app BYO posture landed, an allowlisted line can be priced under a
    #: different one (``services.billing._line_billing_mode``). Do not infer
    #: "every line was priced as X" from this field.
    billing_mode: str


# --------------------------------------------------------------------------- #
# Invoice PREVIEW — the current period, computed and not frozen
#
# ⛔ THE THING THIS MODEL EXISTS TO PREVENT IS A FABRICATED ZERO. A preview is
# read by a tenant admin deciding whether a bill looks right, and "$0.00" is the
# one answer that is indistinguishable between four completely different states:
# nothing was owed; nothing was measured; something was measured and
# deliberately not billed; or the basis could not be read at all. The repo has
# already been bitten by every neighbouring form of this — `app_rates.get(key,
# 0)` inventing a price, `seat_count` flattening to 0, `coalesce(sum(cost), 0)`
# freezing "free" where the truth was "unknown" — so the preview states which
# state it is in rather than leaving a renderer to guess from the number.
# --------------------------------------------------------------------------- #

#: Rollup rows exist for this period, so the money below is a computed bill —
#: which may legitimately be $0.00.
PREVIEW_BASIS_ROLLUP = "rollup"
#: NOTHING underlies this period: no ``billing_cost_daily`` row, no
#: ``billing_metering_daily`` row and no effective ``tenant_access_charge`` term.
#: Every money field is ``None`` in this state — never 0.00.
#:
#: ⚠️ A REAL STATE, NOT A HYPOTHETICAL: an authorised purge left
#: ``billing_cost_daily`` empty for August and most of September 2026, so this
#: branch is reachable today rather than merely defensive.
#:
#: ⛔ BUT IT IS A FACT ABOUT THE TABLE RIGHT NOW, NEVER A FACT ABOUT THOSE
#: DATES, AND NOTHING MAY ENCODE IT AS ONE. A nightly logical dump taken 16
#: hours before that truncate is retained and holds 720 August rollup rows, so
#: the period is RESTORABLE; whether it is restored is an operator's decision
#: that may be taken at any time. A short-circuit returning ``no_basis`` for
#: pre-2026-09-17 periods without querying — or a test asserting emptiness as a
#: property of those dates — would keep answering "no data" after the rows came
#: back, and would be a cached absence rather than a measurement. Decide this
#: verdict from the counts taken in THIS request, every time.
PREVIEW_BASIS_NONE = "no_basis"

#: The excluded figure was measured from ``billing_cost_events``.
EXCLUSION_BASIS_RAW_EVENTS = "raw_cost_events"
#: It could NOT be measured: ``billing_cost_events`` holds no row at all for this
#: tenant-window, so the excluded amount is UNKNOWN.
#:
#: ⛔ REPORTED AS ``None``, NEVER 0 — "nothing was excluded" and "we can no
#: longer tell what was excluded" are different facts and only one of them is
#: reassuring. ⚠️ THIS STATE IS GENUINELY AMBIGUOUS AND IS NOT ALLOWED TO
#: PRETEND OTHERWISE: an empty raw window means either the partition was pruned
#: (retention; the 2026-09-05 truncation) or the tenant really emitted nothing,
#: and the raw table cannot tell those apart by itself. It is only when the
#: ROLLUP has rows and the raw window does not that pruning is certain — and the
#: verdict is the same either way, so the endpoint does not branch on it.
#:
#: ⛔ THIS IS WHY AN ``exclusions`` VERDICT IS NEVER DERIVED FROM
#: ``PREVIEW_BASIS_NONE``. An all-excluded window has ZERO rollup rows by
#: construction — ``rollup_daily`` filters excluded events out — so "no basis"
#: and "everything was excluded" produce the identical empty rollup. Answering
#: "not applicable, there was no basis" would hide exactly the spend this model
#: exists to disclose. The two questions are measured independently, against
#: two different tables, and there is deliberately no constant for
#: "not applicable".
EXCLUSION_BASIS_UNMEASURABLE = "raw_events_absent"


class InvoicePreviewExclusions(BaseModel):
    """Spend that was metered and then deliberately NOT billed.

    ⛔ THIS CANNOT BE READ OFF THE ROLLUP, AND THAT IS WHY IT IS ITS OWN QUERY.
    ``excluded_from_billing`` / ``exclusion_reason`` live on
    ``billing_cost_events`` ONLY; ``services.rollup_engine`` filters those rows
    out on the way into ``billing_cost_daily``, so the rollup every other
    billing reader consumes is already net of them and has no column that could
    say by how much. A preview built from the rollup alone therefore shows a
    smaller number than the tenant's raw spend with nothing naming the gap —
    measured 2026-09-17 fleet-wide: $118.708744 raw against $118.184744 rolled
    up, the $0.524 difference being 18 ``unregistered_tenant`` rows ($0.441435)
    plus 2,750 ``health_probe`` rows ($0.082500).

    ⚠️ ``billing_metering_events`` HAS NO EXCLUSION COLUMN, so this covers the
    COST axis only. A per-request app charge cannot be excluded this way today,
    and this model must not be read as a census of everything unbilled.

    ⚠️ AND IT IS BOUNDED BY RAW RETENTION, WHICH IS THE WHOLE REASON ``basis``
    IS A FIELD. ``billing_cost_events`` is RANGE-partitioned on ``created_at``
    with retention pruning, and was truncated 2026-09-05 — so for an older
    period the honest answer is "unknown", not "none". ``basis`` is decided by
    MEASUREMENT (are there any raw rows in this window at all?), never by
    comparing the period against a retention setting, because the setting would
    not have predicted the truncation.
    """

    #: One of ``raw_cost_events`` / ``raw_events_absent`` /
    #: ``not_applicable_no_basis``. Read it BEFORE the three fields below.
    basis: str
    #: ⚠️ ``None`` whenever ``basis != "raw_cost_events"``. Not 0.
    excluded_usd: Decimal | None = None
    excluded_rows: int | None = None
    #: ``{exclusion_reason: usd}`` — ``unregistered_tenant`` and
    #: ``health_probe`` are different facts with different remedies, so the
    #: split is carried rather than one summed figure. ``None``, not ``{}``,
    #: when unmeasurable: an empty map reads as "measured, nothing excluded".
    by_reason: dict[str, Decimal] | None = None


class InvoicePreviewResponse(BaseModel):
    """What this tenant's CURRENT period would bill, computed and frozen nowhere.

    ⛔ IT IS NOT AN INVOICE AND MUST NEVER BECOME ONE. No row is written, no
    number is drawn from ``billing_invoice_counters``, no status moves. The
    figures here are a REPORT over mutable inputs — markup rules, the rollup,
    the licences seat count — and they will move when those move. That is the
    exact property ``billing_invoices`` exists to remove, which is why a preview
    carries no ``id`` and no ``invoice_number``: there is nothing here to cite.

    ⚠️ READ ``basis`` BEFORE ANY MONEY FIELD. Every money field is ``None`` when
    ``basis == "no_basis"``, and a consumer that renders ``total_usd ?? 0`` puts
    a fabricated $0.00 in front of a tenant admin for a period nothing was
    measured in.
    """

    tenant_id: str
    #: ``YYYY-MM``, as asked for.
    period: str
    period_start: date
    #: EXCLUSIVE, matching ``BillingInvoice``'s half-open interval.
    period_end: date

    #: ``rollup`` or ``no_basis`` — see the two constants above.
    basis: str
    #: The three counts ``basis`` is decided from, disclosed so a reader can see
    #: WHICH of them was empty rather than only that the verdict was ``no_basis``.
    #: ⭐ An access term with no usage is a REAL bill (the one tenant with an
    #: open row carries a $5,000.00/month flat fee against which a month of zero
    #: usage still owes $5,000.00), so the term count is part of the predicate
    #: and not merely decoration.
    cost_rollup_rows: int
    metering_rollup_rows: int
    access_charge_terms: int

    # --- the computed bill; ALL None when basis == "no_basis" ---------------
    total_usd: Decimal | None = None
    internal_cost_usd: Decimal | None = None
    margin_usd: Decimal | None = None
    access_charge_usd: Decimal | None = None
    platform_fee_usd: Decimal | None = None
    seat_fee_usd: Decimal | None = None
    #: ⚠️ THREE MEANINGS, read with ``seat_basis`` — the same triple
    #: ``BillingTotals.seat_count`` documents: ``None``+``None`` = no per-seat
    #: term applies; ``N``+a basis = counted; ``None``+``"unavailable"`` = a term
    #: applies but licences could not be asked, so ``seat_fee_usd`` is $0.00 on
    #: this REPORT path (``close_period`` refuses to freeze that state instead).
    seat_count: int | None = None
    #: ``active_during_period`` (a ``seat:<app>`` term, counted over
    #: ``[period_start, period_end)`` as distinct PEOPLE), ``active_licences`` (an
    #: unpinned term, counted at the LAST INSTANT of the period), or
    #: ``unavailable``. ⛔ It is never a count taken at ``now()``; see the route.
    seat_basis: str | None = None
    #: The realised blended rate on billed usage, and the billing mode it was
    #: realised under. ``None`` in the ``no_basis`` state for the same reason the
    #: money is: a 0.0% markup nobody measured is not a fact.
    effective_markup_pct: float | None = None
    billing_mode: str | None = None
    #: PREPAID CREDIT (alembic 0055): what this period nets out of its LLM usage
    #: (already deducted from ``total_usd``), and the PROVISIONAL credit left
    #: after it. ``None`` in the ``no_basis`` state like every money field.
    prepaid_credit_applied_usd: Decimal | None = None
    prepaid_credit_remaining_usd: Decimal | None = None

    #: Always present, in every state — see ``InvoicePreviewExclusions``.
    exclusions: InvoicePreviewExclusions


class InvoiceDocument(BaseModel):
    """The frozen pair, returned verbatim by ``/reproduce``.

    Read from ``billing_invoices`` and nothing else — no rollup, no markup rule,
    no licences call. If re-rendering a statement needed any mutable table, the
    statement would not be a record.

    ⚠️ MONEY IS A STRING HERE, DELIBERATELY. These are the frozen figures; a
    float round-trip is a silent edit to something that must not change. The
    service emits ``str(Decimal)`` and this model preserves it rather than
    coercing back through a numeric type.

    ``digest_matches`` recomputes sha256 over ``{snapshot, inputs, money}`` at
    read time and compares it to the stored column, so "was this row edited?" is
    a boolean a caller can act on rather than a diff of two JSONB documents.
    ⚠️ It used to cover ``{snapshot, inputs}`` ONLY, which made it blind to an
    edited ``total_usd`` — the money columns were stored copies nothing checked.

    ⛔ EVERY FIELD ``reproduce()`` RETURNS MUST BE DECLARED HERE. This model has
    no ``model_config``, so Pydantic v2 defaults to ``extra='ignore'`` and an
    undeclared key is dropped SILENTLY at ``routers/billing.py`` — a 200 whose
    body is missing the field, with nothing in the logs. That is exactly how the
    seven frozen components below reached HTTP as nothing on their first attempt.
    """

    invoice_id: str
    tenant_id: str
    tenant_name: str
    period_start: str
    period_end: str
    status: str
    invoice_number: str | None
    currency: str
    subtotal_usd: str
    total_usd: str
    internal_cost_usd: str
    margin_usd: str
    #: Frozen components + provenance. Strings for the same reason as the money
    #: above: these are recorded figures and a float round-trip would edit them.
    access_charge_usd: str
    platform_fee_usd: str
    markup_pct: str
    effective_markup_pct: str
    markup_source: str
    #: The seven components that explain ``total_usd``, plus who issued it.
    #: Without these the only re-render of a statement returned a total it could
    #: not account for, by an issuer it could not name. ``seat_count`` stays an
    #: int-or-None because it is a QUANTITY: "no per-seat term" must not render
    #: as the string "None" nor flatten to 0.
    llm_usage_usd: str
    llm_markup_usd: str
    #: The APP-REQUEST stream's incurred/markup pair (alembic 0033). Until it
    #: existed the components above could not add up to ``total_usd`` — the
    #: generator bills TWO streams into the customer total and only the llm one
    #: was carried — so a caller reconciling a statement from this document was
    #: short by the whole app-request charge with nothing naming the gap.
    app_request_usage_usd: str
    app_request_markup_usd: str
    seat_fee_usd: str
    seat_count: int | None
    excluded_unbillable_usd: str
    prepaid_credit_applied_usd: str
    billing_mode: str
    issued_by: str | None
    lines: list[Any]
    snapshot: dict[str, Any]
    inputs: dict[str, Any]
    snapshot_sha256: str
    generator_version: str
    issued_at: str | None
    voided_at: str | None
    void_reason: str | None
    digest_matches: bool
    recomputed_sha256: str
    #: The SECOND, independent check: stored money column vs the same figure
    #: re-derived from the frozen snapshot. ``digest_matches`` answers "were the
    #: bytes edited"; this answers "do the bytes agree with each other". A
    #: mismatch names each column with its ``stored`` and ``from_snapshot``
    #: values, so an operator sees which figure moved and by how much.
    money_matches_snapshot: bool
    money_mismatches: dict[str, dict[str, str]]
