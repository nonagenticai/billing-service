"""Token budget endpoints — stacked-budget CRUD, aggregate status, cascade-aware."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import (
    enforce_tenant_isolation,
    get_subject_id,
    require_internal,
    require_tenant,
    resolve_tenant_or_400,
)
from db import get_session
from models.db import ACTIVE_LICENSE_STATUS, LICENSE_STATUSES, NULL_LICENSE_ID
from models.domain import (
    BudgetAggregateStatus,
    BudgetPeriod,
    BudgetReserveRequest,
    BudgetReserveResponse,
    BudgetResetRequest,
    BudgetResetResponse,
    Envelope,
    TokenBudgetCreate,
    TokenBudgetResponse,
    TokenBudgetStatus,
    envelope,
)
from observability.metrics import BUDGET_RESERVE_REFUSAL_REASONS, budget_reserve_refused_total
from redis_client import get_redis
from service_auth import ServiceTokenProvider
from services.app_registry import sync_app_registry
from services.audit import _to_audit_dict
from services.audit import record as audit_record
from services.budget import (
    EmptyActiveSetError,
    UnregisteredTenantError,
    check_budget_aggregate,
    create_budget,
    deactivate_orphan_license_sync_budgets,
    delete_absent_quota_configs,
    delete_budget,
    get_all_budgets_for_tenant,
    reserve_budget,
    upsert_license_sync_budget,
)
from services.budget_debit import _select_active_budgets
from services.budget_period import effective_consumed, period_anchor
from services.licence_limits import effective_limit
from services.quota import (
    mark_quota_config_status,
    record_quota_config_drift,
    record_quota_config_sync_success,
    upsert_request_quota_config,
)
from services.tenant_names import is_tenant_archived
from settings import get_settings
from tenancy import canonical_tenant_id, redact_tenant_id

logger = structlog.stdlib.get_logger(__name__)

TOPUP_UNAVAILABLE_CODE = "topup_unavailable"
TOPUP_UNAVAILABLE_MESSAGE = "Top-ups are unavailable until payment processing is enabled."


def _parse_optional_uuid(raw: object) -> uuid.UUID | None:
    """Parse an id that ANOTHER SERVICE supplied, returning None when it cannot.

    ``app.id`` comes out of the licences service's JSON, so it has never passed
    through this service's validation. Every other consumer of that response in
    this loop already skips-and-counts rather than raising, for the reason the
    surrounding comments give: all the writes here share ONE transaction and ONE
    commit, so letting a single malformed upstream field raise would roll back
    the reconcile for EVERY other tenant.

    A missing/unparseable app is therefore not an error. It means the licence
    stays unresolvable, which the resolver treats exactly like "no match".
    """
    if not raw:
        return None
    try:
        return uuid.UUID(str(raw))
    except ValueError, AttributeError, TypeError:
        return None


router = APIRouter(prefix="/api/budget", tags=["budget"])


def _budget_to_response(budget) -> TokenBudgetResponse:
    return TokenBudgetResponse(
        id=str(budget.id),
        tenant_id=budget.tenant_id,
        license_id=str(budget.license_id) if budget.license_id != NULL_LICENSE_ID else None,
        budget_tokens=budget.budget_tokens,
        period=budget.period,
        includes_input=budget.includes_input,
        includes_output=budget.includes_output,
        active=budget.active,
        priority=budget.priority,
        starts_at=budget.starts_at,
        expires_at=budget.expires_at,
        # Current-period consumption (0 once a periodic budget has rolled over),
        # so the API response agrees with the reserve gate and the status view.
        consumed_tokens=Decimal(effective_consumed(budget, datetime.now(UTC))),
        source=budget.source,
    )


def _aggregate_to_response(agg: dict) -> BudgetAggregateStatus:
    return BudgetAggregateStatus(
        tenant_id=agg["tenant_id"],
        total_remaining=agg["total_remaining"],
        budget_count=agg["budget_count"],
        next_expiry=agg["next_expiry"],
        budgets=[TokenBudgetStatus(**b) for b in agg["budgets"]],
    )


@router.get(
    "/check",
    response_model=Envelope[BudgetAggregateStatus],
    operation_id="check_budget",
)
async def budget_check(
    request: Request,
    tenant_id: str | None = None,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Aggregate token-budget status for a tenant across all stacked budgets."""
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id is required")

    # QUERY parameter, so `require_tenant` never saw it: resolve it here or a
    # registered tenant NAME — which the X-Tenant-Id header accepts — reaches a
    # `uuid` column as a raw string and comes back as a DataError.
    #
    # Isolation is checked BEFORE resolution, deliberately. `enforce_tenant_
    # isolation` canonicalises both sides itself, so for anything that resolves
    # the order is immaterial; for anything that does NOT, checking first keeps a
    # customer caller's answer a flat 403 for every tenant that is not their own.
    # Resolving first would answer 403 for a registered name and 400 for an
    # unregistered one — a tenant-name enumeration oracle on a customer-facing
    # route. An admin caller (no header) is not gated and reaches the 400.
    enforce_tenant_isolation(request, tenant_id, _tenant)
    tenant_id = resolve_tenant_or_400(tenant_id)

    agg = await check_budget_aggregate(session, tenant_id)
    return envelope(_aggregate_to_response(agg), request)


@router.post(
    "/reserve",
    response_model=Envelope[BudgetReserveResponse],
    operation_id="reserve_budget",
    responses={
        429: {
            "model": BudgetReserveResponse,
            "description": (
                "Reservation denied: not enough headroom in the active budget pool, "
                "or no active budget exists. The body still has the BudgetReserveResponse "
                "shape so callers can inspect remaining_tokens / reason. Error path is "
                "NOT enveloped — FastAPI's HTTPException returns it as `detail`."
            ),
        }
    },
)
async def reserve_tenant_budget(
    request: Request,
    body: BudgetReserveRequest,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Pre-flight token reservation against the tenant's active budget pool.

    license-authz calls this on the request hot path. Atomically (via Redis
    Lua) checks ``consumed + reserved + estimated <= sum(budget_tokens)`` and
    decrements a short-TTL ``reserved`` counter on success. Actual cost is
    debited post-fact via the cost-event flush (existing path); the
    ``reserved`` counter auto-expires so over-estimates self-correct.

    Status codes:
      * 200 — reservation granted.
      * 429 — denied (over budget OR no active budget). Response body still
        carries the cap/consumed/remaining numbers so the caller can decide
        whether to retry with a smaller estimate or 402 the user.
    """
    actor_tenant = _tenant
    enforce_tenant_isolation(request, actor_tenant, actor_tenant)

    license_uuid: uuid.UUID | None = None
    if body.license_id:
        try:
            license_uuid = uuid.UUID(body.license_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="license_id must be a UUID") from exc

    # ── ARCHIVAL IS AN OFF-SWITCH, AND UNTIL NOW IT WAS NOT ONE ──────────────
    # Measured 2026-09-10: `archived_at` is enforced at EXACTLY ONE place in the
    # estate — licences' own license-MINT path (licenses/main.py:3331, whose
    # comment says verbatim "archiving still revokes nothing: this blocks new
    # licences only, and the existing ones keep working exactly as before"). It
    # has ZERO hits in license-authz, in this service, in inference-router and in
    # provisioning. 17 tenants are archived and all 19 of their licences are
    # `revoked`, yet `rmt-e2e` (019fb1f4-…-645f, archived 2026-09-03) was still
    # emitting — 405 cost events in 14 days, last one inside the preceding 24 h.
    #
    # ⭐ WHY THE REFUSAL BELONGS HERE. The spend that continues is spend that
    # never presents a licence at all: all 405 rows carry `license_id IS NULL`,
    # because Istio ext_authz attaches `context: GATEWAY` and cannot see
    # east-west traffic. So no licence check can stop it, and the revocation that
    # already happened is not a control. The one thing every caller DOES present
    # is a tenant, and this endpoint is the pre-spend point that sees it.
    #
    # ⛔ AND ARMING THE BUDGET CAP DOES NOT CLOSE THIS. `reserve_budget` ALLOWS
    # when a tenant has no budget row (`no_active_budget`), deliberately, so an
    # archived tenant with no budget sails straight through the cap check. The
    # refusal therefore needs its own reason, not a cap.
    #
    # ⚠️ FAIL-OPEN, AND THE DEGRADE DIRECTION IS LOAD-BEARING. `is_tenant_archived`
    # answers False for every unknown — licences unreachable, non-200, tenant
    # absent from the map, cold cache. An archived tenant keeps spending during a
    # licences outage, which is a bounded loss; refusing live inference because a
    # dependency blipped is not. It reads a 5-min TTL cache shared with the
    # display path, so this costs no HTTP call of its own on most requests —
    # the reason the check is affordable here at all.
    #
    # ⚠️ BLAST RADIUS, MEASURED, NOT ASSUMED: 17 tenants, whose licences are
    # already 19/19 revoked, carrying $1.60 of `internal_transfer` between them,
    # all of it one test tenant. No `kind='customer'` tenant appears in 30 days
    # of spend at all. This refuses no revenue today — it closes the gap while
    # it is still free.
    if await is_tenant_archived(
        actor_tenant,
        get_settings(),
        getattr(request.app.state, "service_token_provider", None),
    ):
        # `check_budget_aggregate` returns a DICT with `total_remaining` and a
        # `budgets` list; it carries no total cap or total consumed, so both are
        # summed over the active rows here. That sum IS the grain `reserve_budget`
        # enforces (`consumed + reserved + estimated <= sum(budget_tokens)`), so
        # the refusal body reports the same quantity a non-archived refusal would
        # — not the per-row maximum, which is pinned at its cap by the debit
        # cascade and would read as a fake overspend.
        agg = await check_budget_aggregate(session, actor_tenant)
        rows = agg["budgets"]
        refused = BudgetReserveResponse(
            allowed=False,
            tenant_id=actor_tenant,
            # A refused reservation holds nothing, so it can never carry a handle.
            reservation_id=None,
            reserved_tokens=0,
            remaining_tokens=agg["total_remaining"],
            cap_tokens=sum(int(b["budget_tokens"]) for b in rows),
            consumed_tokens=sum(int(b["used_tokens"]) for b in rows),
            in_flight_reserved_tokens=0,
            reason="tenant_archived",
        )
        # Real numbers in the body rather than zeros: a human reading this 429
        # must not be told the tenant has a cap of 0, which would read as a
        # configuration fault instead of a deliberate refusal.
        budget_reserve_refused_total.labels(reason="tenant_archived").inc()
        logger.info(
            "budget_reserve_refused_tenant_archived",
            tenant_id=redact_tenant_id(actor_tenant),
        )
        raise HTTPException(status_code=429, detail=refused.model_dump(mode="json"))

    result = await reserve_budget(
        session=session,
        redis_conn=get_redis(),
        tenant_id=actor_tenant,
        estimated_tokens=body.estimated_tokens,
        ttl_seconds=body.ttl_seconds,
        license_id=license_uuid,
    )

    response = BudgetReserveResponse(**result)
    if not response.allowed:
        # Count the accounting refusals on the same series as the authorisation
        # one, labelled apart. `reason` is clamped to the known tuple so a new
        # literal in budget_redis.py cannot silently create an unbounded label.
        reason = response.reason if response.reason in BUDGET_RESERVE_REFUSAL_REASONS else "other"
        budget_reserve_refused_total.labels(reason=reason).inc()
        # FastAPI doesn't let us return a non-200 status while keeping the
        # response_model body shape via plain return; use HTTPException with
        # the model dumped as detail. Caller still gets full JSON body
        # (raw, not enveloped — error convention).
        raise HTTPException(status_code=429, detail=response.model_dump(mode="json"))
    return envelope(response, request)


# NOTE: /status/me MUST be declared BEFORE /status/{tenant_id} — FastAPI
# does first-match wins on the routing table and "me" would otherwise be
# caught as a tenant_id by the dynamic route, hitting enforce_tenant_isolation
# and returning "Tenant mismatch" 403.
@router.get(
    "/status/me",
    response_model=Envelope[BudgetAggregateStatus],
    operation_id="get_my_budget_status",
)
async def budget_status_me(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _subject: uuid.UUID = Depends(get_subject_id),
) -> dict:
    """Self-service budget status — uses the caller's active X-Tenant-Id.

    Mirrors /check but doesn't require the caller to pass tenant_id (the
    UI already knows it from the active workspace). 400 if the tenant
    header is missing — without it there's no budget to look up.

    This route deliberately does NOT depend on ``require_tenant`` like its
    siblings, for two reasons that are specific to ``/me``:

    * ``require_tenant`` treats an ABSENT header as "maybe an admin JWT" and
      falls through to ``verify_admin_token``, which answers **401** ("Tenant
      context required"). This route's contract is a **400**: the caller is
      authenticated, they just have no active workspace selected, and telling
      them to re-authenticate cannot help.
    * The admin-JWT branch resolves the caller's own ``sub``. A Keycloak
      service-account ``sub`` is a uuid, so it resolves cleanly and ``/me``
      would silently answer for "the tenant whose id happens to equal this
      service account" — the ``__admin__`` pseudo-tenant defect re-entering by
      another door. On ``/me`` there is no such thing as an admin caller: "me"
      IS the header.

    So the header is read directly, but resolved through the SAME resolver the
    header path uses, ONCE, at the top — a registered tenant name has to reach
    ``check_budget_aggregate`` as a uuid or Postgres answers with a DataError.
    """
    raw_tenant_id = request.headers.get("x-tenant-id")
    if not raw_tenant_id or not raw_tenant_id.strip():
        # Present-but-empty is a caller error too, not "no header": license-authz
        # sends `tenant_id = result.tenant_id or ""` when introspection returns no
        # tenant, and `"" `must not be handed on to the resolver as if it were an id.
        raise HTTPException(
            status_code=400,
            detail="Active tenant context required (X-Tenant-Id header)",
        )
    tenant_id = resolve_tenant_or_400(raw_tenant_id, field="X-Tenant-Id")
    agg = await check_budget_aggregate(session, tenant_id)
    return envelope(_aggregate_to_response(agg), request)


@router.get(
    "/status/{tenant_id}",
    response_model=Envelope[BudgetAggregateStatus],
    operation_id="get_tenant_budget_status",
)
async def budget_status(
    request: Request,
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Admin variant of /check — same aggregate shape, path-keyed."""
    # PATH parameter: `require_tenant` resolved the HEADER, not this. Isolation
    # first, then resolve — see the note in `budget_check`.
    enforce_tenant_isolation(request, tenant_id, _tenant)
    tenant_id = resolve_tenant_or_400(tenant_id)

    agg = await check_budget_aggregate(session, tenant_id)
    return envelope(_aggregate_to_response(agg), request)


@router.get(
    "/list/{tenant_id}",
    response_model=Envelope[list[TokenBudgetResponse]],
    operation_id="list_tenant_budgets",
)
async def list_tenant_budgets(
    request: Request,
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """All budgets for a tenant including inactive (UI history view)."""
    # PATH parameter — same shape as `budget_status`; resolve after the
    # isolation check, before the value is bound into SQL.
    enforce_tenant_isolation(request, tenant_id, _tenant)
    tenant_id = resolve_tenant_or_400(tenant_id)
    budgets = await get_all_budgets_for_tenant(tenant_id, session)
    payload = [_budget_to_response(b) for b in budgets]
    return envelope(payload, request, total=len(payload))


@router.post(
    "",
    response_model=Envelope[TokenBudgetResponse],
    status_code=201,
    operation_id="create_budget",
)
async def create_token_budget(
    request: Request,
    data: TokenBudgetCreate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_internal),
) -> dict:
    """Create or upsert a token budget.

    Stacking semantics by source:
      - source='license_sync': idempotent upsert keyed on
        (tenant_id, license_id, period, source='license_sync'). Preserves
        consumed_tokens. Audit-logs as `budget.update` on reconcile, else
        `budget.create`. This is the dedup point that prevents the
        license-issuance + sync-from-licenses double-budget bug.
      - any other source ('manual', 'topup', etc.): always inserts a new
        row — multiple stacked budgets are expected.
    """
    lic_id = uuid.UUID(data.license_id) if data.license_id else None

    # 422, NOT 400. `resolve_tenant_or_400` already owns 400 on this service and
    # it means "this value does not parse or resolve to any tenant shape". The
    # refusal below is the opposite situation: the value parsed, resolved, and is
    # simply not a tenant the registry has. Collapsing the two would make the
    # single most useful diagnostic — "is my id malformed, or is my tenant not
    # registered?" — unanswerable from the status code. Matches the existing 422
    # on the top-up route.
    try:
        if data.source == "license_sync":
            if data.license_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="license_id is required when source='license_sync'",
                )
            budget, created = await upsert_license_sync_budget(
                session=session,
                tenant_id=data.tenant_id,
                license_id=data.license_id,
                period=data.period,
                budget_tokens=data.budget_tokens,
                includes_input=data.includes_input,
                includes_output=data.includes_output,
            )
        else:
            budget = await create_budget(
                tenant_id=data.tenant_id,
                budget_tokens=data.budget_tokens,
                period=data.period.value,
                license_id=lic_id,
                includes_input=data.includes_input,
                includes_output=data.includes_output,
                priority=data.priority,
                starts_at=data.starts_at,
                expires_at=data.expires_at,
                source=data.source,
                session=session,
            )
            created = True
    except UnregisteredTenantError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                "tenant_id is not a registered tenant; register the tenant before "
                f"creating a budget for it: {redact_tenant_id(exc.tenant_id)}"
            ),
        ) from exc

    await audit_record(
        session,
        request,
        action="budget.create" if created else "budget.update",
        resource_type="budget",
        resource_id=str(budget.id),
        tenant_id=data.tenant_id,
        before=None,
        after=_to_audit_dict(budget),
    )
    await session.commit()
    await session.refresh(budget)
    return envelope(_budget_to_response(budget), request)


@router.post(
    "/topup/{tenant_id}",
    response_model=Envelope[TokenBudgetResponse],
    status_code=201,
    operation_id="topup_tenant_budget",
)
async def topup_tenant_budget(
    request: Request,
    tenant_id: str,
    data: TokenBudgetCreate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_internal),
) -> dict:
    """Convenience wrapper — create a stacking budget with source='topup'.

    The path tenant_id wins over the body's tenant_id (admin-style override),
    and `source` is forced to `'topup'` regardless of payload.

    A 0-token top-up is rejected (422) — see the guard below.

    Refused with 409 ``topup_unavailable`` while ``BUDGET_TOPUP_ENABLED`` is
    off (the default): no free top-ups until payment processing exists.
    """
    # ⛔ FIRST statement of the body, deliberately: auth (`require_internal`)
    # has already run as a dependency, so 401/403 keep their meaning, and
    # nothing below — budget INSERT, audit row, Redis sync — can be reached
    # while top-ups are unpaid. George ruling 2026-10-02; fr hides the UI
    # (fr#1351) but this route is callable directly (n8n metering node op
    # `topupTenantBudget`), so the refusal has to live at the source.
    if not get_settings().budget_topup_enabled:
        raise HTTPException(
            status_code=409,
            detail={"code": TOPUP_UNAVAILABLE_CODE, "message": TOPUP_UNAVAILABLE_MESSAGE},
        )
    # `TokenBudgetCreate.budget_tokens` is `ge=0` so that a block-all budget is
    # representable on `POST /api/budget`. A top-up is the one route where 0
    # must NOT be accepted, and rejecting it here is not mere input hygiene —
    # it prevents an inversion of the caller's intent.
    #
    # A top-up ADDS allowance; it stacks a new active row. A 0-token one would
    # insert an *active* 0-cap budget, which makes `_select_active_budgets`
    # non-empty for that tenant. That flips the tenant out of the
    # `no_active_budget` short-circuit — which is how "unlimited" is expressed —
    # and into the cap check, where the stacked cap now includes a 0. The
    # operator asked to grant nothing and would instead have BLOCKED a tenant
    # that was previously unrestricted, via the one endpoint whose entire
    # purpose is to give more.
    #
    # Block-all is a deliberate act and has its own route: `POST /api/budget`
    # with `budget_tokens: 0`.
    if data.budget_tokens == 0:
        raise HTTPException(
            status_code=422,
            detail=("a top-up must add allowance; use POST /api/budget to set a block-all budget"),
        )

    # PATH parameter, and this route has no tenant dependency at all
    # (`require_internal` authorises the CALLER, it does not resolve a tenant),
    # so nothing upstream has turned a registered tenant NAME into its uuid.
    # Resolve once here; `tenant_id` is used twice below — the INSERT and the
    # audit row — and both must carry the same canonical value or the audit
    # trail points at a tenant that owns no rows. NB `data.tenant_id` is NOT
    # touched: the path wins on this route (see docstring) and the body field is
    # resolved by its own model validator.
    tenant_id = resolve_tenant_or_400(tenant_id)

    lic_id = uuid.UUID(data.license_id) if data.license_id else None
    try:
        budget = await create_budget(
            tenant_id=tenant_id,
            budget_tokens=data.budget_tokens,
            period=data.period.value,
            license_id=lic_id,
            includes_input=data.includes_input,
            includes_output=data.includes_output,
            priority=data.priority,
            starts_at=data.starts_at,
            expires_at=data.expires_at,
            source="topup",
            session=session,
        )
    except UnregisteredTenantError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                "tenant_id is not a registered tenant; a top-up cannot create a "
                f"budget for an unowned tenant: {redact_tenant_id(exc.tenant_id)}"
            ),
        ) from exc
    await audit_record(
        session,
        request,
        action="budget.topup",
        resource_type="budget",
        resource_id=str(budget.id),
        tenant_id=tenant_id,
        before=None,
        after=_to_audit_dict(budget),
    )
    await session.commit()
    await session.refresh(budget)
    return envelope(_budget_to_response(budget), request)


@router.post(
    "/reset/{tenant_id}",
    response_model=Envelope[BudgetResetResponse],
    operation_id="reset_tenant_budget",
)
async def reset_tenant_budget(
    request: Request,
    tenant_id: str,
    body: BudgetResetRequest,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_internal),
) -> dict:
    """Zero the current-period consumption of every ACTIVE budget a tenant holds.

    ⛔ INTERNAL BY CONSTRUCTION — ``require_internal`` (a verified Keycloak
    SERVICE-ACCOUNT token), never ``require_tenant``. A licence token is held
    by every member of a tenant, and ext_authz turns one into an ``X-Tenant-Id``
    header, so ``require_tenant`` + tenant isolation would let a CUSTOMER zero
    their own consumption — free money. This route therefore reads no tenant
    header at all and pairs with no isolation check: with ``require_internal``
    there is nothing to isolate against, and a guard that can never fire is
    worse than none. Same gate as the two other budget writes in this file
    (``create_token_budget`` / ``topup_tenant_budget``).

    Scope is the SAME set the status endpoint reports
    (``_select_active_budgets(..., lock=False)``, i.e. active AND inside its
    current window — see ``check_budget_aggregate``): the operator resets what
    they just looked at, and a row the gate cannot spend against (expired, or
    not started yet) is not part of that view.

    404 ``no_active_budgets`` when there is nothing to reset — distinct from an
    empty 200, which would tell a caller who mistyped the tenant that their
    reset succeeded.
    """
    # PATH parameter: nothing upstream resolved it (``require_internal``
    # authorises the CALLER; it never names a tenant), and a registered tenant
    # NAME arriving here must not bind into a ``uuid`` column as a raw string.
    tenant_id = resolve_tenant_or_400(tenant_id)

    now = datetime.now(UTC)
    rows = await _select_active_budgets(session, tenant_id, now, lock=False)
    if not rows:
        raise HTTPException(status_code=404, detail="no_active_budgets")

    previous_total = 0
    reset_ids: list[str] = []
    for row in rows:
        # effective_consumed, NOT row.consumed_tokens. The raw column can still
        # carry an EARLIER period's spend (see services/budget_period.py), which
        # the status endpoint already reports as 0; reporting the column here
        # would tell the operator they reset a number they never saw on screen.
        previous = effective_consumed(row, now)
        previous_total += previous
        # Both sides of the audit are captured BEFORE the row is mutated —
        # reading `consumed_period_start` off the row after the assignment below
        # would record the POST-reset anchor as the "before" state.
        before = {
            "consumed_tokens": previous,
            "consumed_period_start": row.consumed_period_start,
        }
        row.consumed_tokens = Decimal(0)
        # The anchor says which period that zero belongs to. monthly/weekly get
        # the current period's first day; any other period is a lifetime budget
        # with no period semantics, so it gets NULL (period_anchor's contract).
        row.consumed_period_start = period_anchor(row.period, now)
        row.updated_at = now
        await audit_record(
            session,
            request,
            action="budget.reset",
            resource_type="budget",
            resource_id=str(row.id),
            tenant_id=tenant_id,
            before=before,
            after={
                "consumed_tokens": 0,
                "consumed_period_start": row.consumed_period_start,
                # Accepted-and-ignored would make `reason` a lie: the field
                # exists to say WHY the allowance was handed back.
                "reason": body.reason,
            },
        )
        reset_ids.append(str(row.id))

    await session.commit()

    return envelope(
        {
            "tenant_id": tenant_id,
            "reset_count": len(reset_ids),
            "budget_ids": reset_ids,
            "previous_consumed_total": previous_total,
        },
        request,
    )


@router.delete(
    "/{budget_id}",
    status_code=204,
    operation_id="delete_budget",
)
async def remove_budget(
    request: Request,
    budget_id: str,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_internal),
) -> None:
    """Soft-delete a token budget (sets active=False)."""
    try:
        bid = uuid.UUID(budget_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID") from None

    prior = await delete_budget(bid, session)
    if prior is None:
        raise HTTPException(status_code=404, detail="Budget not found or already inactive")

    await audit_record(
        session,
        request,
        action="budget.delete",
        resource_type="budget",
        resource_id=budget_id,
        tenant_id=prior.tenant_id,
        before=_to_audit_dict(prior),
        after=None,
    )
    await session.commit()


@router.post(
    "/sync-from-licenses",
    status_code=200,
    operation_id="sync_budgets_from_licenses",
)
async def sync_from_licenses(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_internal),
) -> dict:
    """Backfill budgets and request quotas from the licenses service.

    ⚠️ OPERATOR-TRIGGERED ONLY. This is a **manual reconcile of last resort**,
    not a background job. Nothing calls it on a timer: there is no scheduler, no
    CronJob, no watcher loop, and no startup hook anywhere in this service that
    invokes it. It runs if and only if a human (or a runbook step) POSTs to it.

    Do not read this route's existence as "budgets track licences
    automatically". They do not. Between two manual invocations, a plan change
    upstream — a new licence, a changed ``token_budget_monthly``, a
    downgrade-to-0, a deactivation that should orphan a budget — is simply not
    reflected here. The steady-state writer is licence issuance itself; this
    endpoint exists to repair the drift that accumulates when that path missed
    something.

    Consequently a failure here is silent by default. Treat a response with a
    non-null ``error`` (or zero counts where rows were expected) as an incident,
    not as a no-op: nothing else will notice. The same goes for a non-zero
    ``skipped_count``: those licences were deliberately passed over because they
    name no resolvable tenant, and no later run will fix them by itself.
    """
    settings = get_settings()

    licenses_synced = 0
    created_count = 0
    updated_count = 0
    quotas_synced = 0
    # Licences this run declined to process because they carry no usable
    # tenant-of-record (absent, or an identifier that resolves to no tenant).
    # Counted rather than raised — see the skip inside the loop.
    skipped_count = 0
    created_budget_ids: list[str] = []
    touched_budget_ids: list[str] = []
    orphaned_budget_ids: list[str] = []
    # Mirror rows removed because their licence is no longer active upstream.
    deleted_quota_config_ids: list[str] = []
    # Every ACTIVE license id the licenses service returned this run. Used to
    # reconcile away orphaned license_sync budgets (deactivated licenses).
    active_license_ids: set[uuid.UUID] = set()
    # Every license id the registry returned this run, WHATEVER its status.
    #
    # These two sets used to be the same thing because the fetch was filtered to
    # active. They must not be conflated now: a revoked licence is KNOWN to the
    # registry (so its mirror row should be kept and marked, not deleted), while
    # a licence absent from this set does not exist upstream at all (so its
    # mirror row is genuine junk). ``delete_absent_quota_configs`` is keyed on
    # THIS set; the budget orphan reconcile stays keyed on the active one.
    known_license_ids: set[uuid.UUID] = set()
    # Licences whose mirrored status is not 'active'. Counted so a run reports
    # how much of the mirror it just made non-resolvable rather than leaving
    # that to be inferred from a diff.
    non_active_count = 0
    # Licences the registry returned with a status this service does not know.
    # A separate counter from `skipped_count`: that one means "upstream handed us
    # a row we could not WRITE", this one means "upstream's lifecycle vocabulary
    # has changed and attribution is now degraded until someone widens
    # LICENSE_STATUSES". Different cause, different fix, different operator.
    unrecognised_status_count = 0
    # Licences the registry returned with NO status field at all. Defaulted to
    # 'active' (see the branch in the loop) and counted, because that default is
    # the one fail-open step in this path and must never become invisible.
    missing_status_count = 0
    # Mirror rows actually transitioned out of 'active' by this run.
    marked_non_active_ids: list[str] = []
    error: str | None = None
    try:
        provider: ServiceTokenProvider | None = getattr(
            request.app.state, "service_token_provider", None
        )
        # NO tenant header. Do not re-add one — least of all a sentinel.
        #
        # This call used to send ``X-Tenant-Id: "__admin__"``, which made the
        # whole endpoint inert in production. ``/internal`` is in the licenses
        # service's ``PROTECTED_PREFIXES`` (licenses/auth/middleware.py) and its
        # ``AuthMiddleware.dispatch`` runs
        # ``validate_tenant_uuid(raw_tenant_id, source="X-Tenant-Id")`` BEFORE
        # it ever looks at the Authorization header. ``"__admin__"`` is not a
        # UUID, so licences answered 400 to every call we ever made — and the
        # ``except`` below folds that into the ``error=`` field of a 200
        # response, so the operator saw "ok, 0 synced" rather than a failure.
        # There is no admin-sentinel convention on that side to opt into.
        #
        # Nothing is lost by omitting it: ``internal_list_licenses`` takes NO
        # tenant dependency. It is authorised purely by
        # ``require_internal_service`` (i.e. the bearer below) and scoped by an
        # optional ``?tenant_id=`` query param we deliberately do not send,
        # because this endpoint reconciles ALL tenants in one pass.
        headers: dict[str, str] = {}
        if provider:
            token = await provider.get_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"

        async with httpx.AsyncClient(timeout=10.0) as client:
            # NO ``status`` filter. This used to send ``{"status": "active"}``,
            # which made it structurally impossible for the mirror to learn that
            # a licence had been REVOKED: the only signal was the licence's
            # ABSENCE from the page, and absence is also what a licence that was
            # never created looks like. ``internal_list_licenses`` returns every
            # status when the param is omitted (its filter is a truthiness-
            # guarded ``License.status == status``), so dropping it is what lets
            # migration 0025's ``request_quota_config.status`` carry a real
            # value instead of a constant ``'active'`` — i.e. what stops the
            # resolver's new predicate from being a guard that can never fire.
            #
            # ⚠️ The ACTIVE-SET SEMANTICS BELOW ARE UNCHANGED. ``active_license_ids``
            # is still built from ``status == 'active'`` ONLY, so
            # ``deactivate_orphan_license_sync_budgets`` still orphans exactly
            # the budgets it orphaned before. Widening the fetch must not widen
            # what counts as active; those are two different sets and this
            # function now keeps both.
            resp = await client.get(
                f"{settings.licenses_url}/internal/licenses",
                headers=headers,
            )
            if resp.status_code != 200:
                logger.warning("licenses_fetch_failed", status=resp.status_code)
                error = f"licenses service returned {resp.status_code}"
            else:
                licenses = resp.json()
                for lic in licenses:
                    lic_id = lic.get("id")
                    raw_tenant_id = lic.get("tenant_id")
                    plan = lic.get("plan", {})
                    if not lic_id:
                        continue
                    if not raw_tenant_id:
                        # No tenant-of-record → we cannot create a tenant-scoped
                        # budget OR a request_quota_config row (tenant_id is NOT
                        # NULL, migration 0007). Inserting a NULL/empty tenant would
                        # 500 the constraint AND re-open the cross-tenant-read gap,
                        # so skip the license with a loud warning instead.
                        logger.warning("sync_from_licenses_skip_no_tenant", license_id=lic_id)
                        skipped_count += 1
                        continue

                    # The tenant here comes out of ANOTHER SERVICE'S JSON, so
                    # `require_tenant` never saw it and it may be a registered
                    # tenant NAME. Since 0011 `token_budgets.tenant_id` and
                    # `request_quota_config.tenant_id` are real `uuid` columns, so a
                    # name binds into SQL as an unparseable literal and Postgres
                    # raises DataError.
                    #
                    # SKIP-AND-COUNT, not fail-the-request. Every write in this loop
                    # — budgets, quota configs, the orphan reconcile after it —
                    # shares ONE transaction and ONE commit at the bottom. Letting
                    # this raise (which is what `resolve_tenant_or_400` would do,
                    # hence `canonical_tenant_id` directly) means one malformed row
                    # from upstream rolls back the reconcile for EVERY other tenant,
                    # turning a bad row into a fleet-wide no-op. This is a
                    # reconcile-of-last-resort whose whole value is repairing as much
                    # drift as it can reach; the correct blast radius for one bad
                    # licence is that one licence. It is counted into
                    # `skipped_count` (returned AND written to the summary audit row)
                    # and logged per-row, so "skipped 3" is visible to the operator
                    # rather than silently folded into a smaller `processed_count`.
                    #
                    # The value is REDACTED in the log: a tenant field has been
                    # observed in production carrying a raw `Bearer <jwt>`.
                    tenant_id = canonical_tenant_id(raw_tenant_id)
                    if tenant_id is None:
                        logger.warning(
                            "sync_from_licenses_skip_unresolvable_tenant",
                            license_id=lic_id,
                            raw_tenant_id=redact_tenant_id(raw_tenant_id),
                        )
                        skipped_count += 1
                        continue

                    # Same skip-and-count treatment for an unparseable LICENCE id,
                    # for the same reason and via the same counter. This used to
                    # log `license_id_unparseable` and fall through, which defeated
                    # the tenant skip above: `upsert_license_sync_budget` parses
                    # `license_id` itself (`uuid.UUID(license_id)`, budget_crud.py),
                    # so a malformed id raised a bare ValueError that NOTHING here
                    # catches — `except httpx.RequestError` does not — and the whole
                    # endpoint 500'd with the transaction rolled back. One bad row
                    # from upstream still discarded every other tenant's reconcile,
                    # just through `license_id` instead of `tenant_id`.
                    #
                    # ONE counter, not two: `skipped_count` answers the only
                    # question the response body decides — "did upstream hand us
                    # rows we could not write?" — and the operator's next action is
                    # identical either way. The two causes ARE distinguishable, by
                    # the distinct log event names, which is where the diagnosis
                    # belongs. Splitting the body field would make callers sum two
                    # numbers to learn one fact.
                    #
                    # Parsed ONCE here and reused below, so the quota branch cannot
                    # disagree with this decision about the same licence.
                    try:
                        lic_uuid = uuid.UUID(str(lic_id))
                    except ValueError, TypeError:
                        logger.warning(
                            "sync_from_licenses_skip_unparseable_license_id",
                            license_id=redact_tenant_id(lic_id),
                        )
                        skipped_count += 1
                        continue

                    # Licence lifecycle, mirrored verbatim from the registry.
                    # Two failure shapes, deliberately handled DIFFERENTLY,
                    # because conflating them either loses budgets or fails open.
                    raw_status = lic.get("status")
                    lic_status = str(raw_status).strip().lower() if raw_status else ""

                    # (a) PRESENT BUT UNRECOGNISED — upstream's lifecycle
                    # vocabulary has grown a value this service does not model.
                    # Neither assumed active nor coerced to revoked: both would
                    # fabricate a lifecycle fact and one of them fails OPEN. The
                    # licence is counted, logged, and left ENTIRELY alone this
                    # run — no budget upsert —
                    # but it IS added to `known_license_ids` so the reconcile
                    # below cannot delete its row on the strength of information
                    # we just admitted we could not read.
                    if lic_status and lic_status not in LICENSE_STATUSES:
                        logger.warning(
                            "sync_from_licenses_unrecognised_status",
                            license_id=lic_id,
                            status=lic_status,
                        )
                        unrecognised_status_count += 1
                        known_license_ids.add(lic_uuid)
                        continue

                    # (b) ABSENT — treated as 'active', which is the ONLY value
                    # that preserves pre-0025 behaviour for a caller that does
                    # not speak lifecycle.
                    #
                    # ⚠️ THIS IS A FAIL-OPEN DEFAULT AND IT IS INSTRUMENTED SO IT
                    # CANNOT ROT SILENTLY. It is safe today for one specific,
                    # checkable reason: `status` is a REQUIRED field on the
                    # upstream `InternalLicenseResponse` pydantic model, so a
                    # real registry response cannot omit it — FastAPI would fail
                    # to serialise the row first. Absence therefore means a
                    # hand-built payload, not a revoked licence.
                    #
                    # If that ever stops being true this default would provision
                    # budgets for revoked licences, so it is COUNTED
                    # (`missing_status_count`) and surfaced in the response
                    # rather than being a silent `or "active"`. A non-zero value
                    # in production means the upstream contract changed and this
                    # branch must become a skip.
                    if not lic_status:
                        missing_status_count += 1
                        lic_status = ACTIVE_LICENSE_STATUS

                    known_license_ids.add(lic_uuid)
                    if lic_status != ACTIVE_LICENSE_STATUS:
                        # Not active upstream. Mark the EXISTING mirror row so
                        # the attribution resolver stops treating this licence as
                        # a candidate, and do nothing else: no budget is
                        # provisioned, no quota row is created, no Redis
                        # enforcement key is seeded for a licence the registry
                        # has withdrawn.
                        #
                        # UPDATE-IF-PRESENT, deliberately not an upsert. Creating
                        # a row here would manufacture a `request_quota_config`
                        # for every revoked licence in the registry (92 of 258
                        # measured 2026-09-08) — rows that never existed, that
                        # `quota_check` would then read as configured limits.
                        # Marking only what is already mirrored changes exactly
                        # the rows whose absence of a status was the defect.
                        non_active_count += 1
                        if await mark_quota_config_status(session, lic_uuid, lic_status):
                            marked_non_active_ids.append(str(lic_uuid))
                        continue

                    # Record the active id (regardless of token_budget) so a live
                    # license with a 0/absent budget is never treated as an orphan.
                    active_license_ids.add(lic_uuid)

                    # The per-licence OVERRIDE wins over the plan default: it is
                    # what an operator set for THIS licence, and licences' own
                    # nightly reconcile pushes it. Reading only the plan made
                    # this endpoint undo it (2026-10-07: an override of 100,000
                    # rewritten to the plan's 50,000,000). Absent/None override
                    # (older licences deploy) → plan value, exactly as before.
                    # See services.licence_limits.
                    token_budget = effective_limit(
                        lic.get("token_budget_override"),
                        plan.get("token_budget_monthly"),
                        field="token_budget_override",
                        license_id=lic_id,
                    )
                    # `is not None and >= 0`, NOT a truthiness test. A plan
                    # suspended to token_budget_monthly=0 is a ZERO-ALLOWANCE
                    # plan (block all), which is the same semantics PR #68
                    # established for quota_monthly and which _RESERVE_LUA
                    # already implements (`cap >= 0`, with a comment noting the
                    # `> 0` guard HERE was the reason cap==0 was unreachable).
                    # Under the old truthiness test a 0 skipped the upsert
                    # entirely, so a license downgraded 1_000_000 → 0 kept its
                    # stale million-token row active forever (the active-id
                    # bookkeeping above deliberately spares it from the orphan
                    # reconcile) and kept spending against a budget its plan no
                    # longer grants. "Unlimited" is the ABSENCE of the field
                    # (None → skip), never a 0.
                    if token_budget is not None and token_budget >= 0:
                        # SKIP-AND-COUNT, never raise out of this loop. The guard
                        # in `budget_crud` refuses a budget whose tenant no
                        # registry vouches for, and on THIS path that refusal must
                        # cost exactly one licence. Letting it propagate would
                        # roll back every other tenant's budget and quota write in
                        # the run -- the identical failure the
                        # `sync_from_licenses_skip_unparseable_license_id` comment
                        # above records, where one malformed id from upstream
                        # discarded the whole reconcile. Reuses `skipped_count`
                        # for the same reason that comment gives: the body field
                        # answers one question ("did upstream hand us rows we
                        # could not write?") and the operator's next action is the
                        # same either way; the distinct log event name is where the
                        # diagnosis lives.
                        try:
                            budget, created = await upsert_license_sync_budget(
                                session=session,
                                tenant_id=tenant_id,
                                license_id=lic_id,
                                period=BudgetPeriod.monthly,
                                budget_tokens=token_budget,
                            )
                        except UnregisteredTenantError:
                            logger.warning(
                                "sync_from_licenses_skip_unregistered_tenant",
                                license_id=lic_id,
                                tenant_id=redact_tenant_id(tenant_id),
                            )
                            skipped_count += 1
                            continue
                        await audit_record(
                            session,
                            request,
                            action="budget.create" if created else "budget.update",
                            resource_type="budget",
                            resource_id=str(budget.id),
                            tenant_id=tenant_id,
                            before=None,
                            after=_to_audit_dict(budget),
                        )
                        if created:
                            created_budget_ids.append(str(budget.id))
                            created_count += 1
                        else:
                            updated_count += 1
                        touched_budget_ids.append(str(budget.id))
                        licenses_synced += 1

                    # Override-wins, same rule and reason as token_budget above.
                    quota_monthly = effective_limit(
                        lic.get("quota_override"),
                        plan.get("quota_monthly"),
                        field="quota_override",
                        license_id=lic_id,
                    )
                    # Same falsy-zero trap as token_budget above. PR #68 fixed
                    # the READER ("quota_monthly=0 blocks all; unlimited is
                    # NULL/absent") but this WRITER still dropped a 0 on the
                    # floor: a suspended plan never persisted its
                    # request_quota_config row, so the license was served as
                    # UNLIMITED — the exact fail-open #68 set out to close, one
                    # layer upstream.
                    if quota_monthly is not None and quota_monthly >= 0:
                        # Persist the request_quota_config row — the only copy of
                        # the limit; GET /api/quota/check reads it.
                        # tenant_id is guaranteed here to be a canonical tenant
                        # uuid (the loop skips licences with no tenant, and
                        # licences whose tenant does not resolve, above) — which is
                        # what the row's NOT NULL `uuid` column requires.
                        #
                        # `lic_uuid` is the parse done once at the top of the loop
                        # body; the private re-parse that used to sit here (with its
                        # own `sync_from_licenses_quota_unparseable_id` warning) is
                        # gone. It could not fire any more — an unparseable id no
                        # longer reaches this line, it is skipped and counted — and
                        # a second, independently-handled parse of the SAME value is
                        # exactly how the two branches came to disagree: this one
                        # skipped the licence quietly while the budget branch above
                        # blew the whole request up on it.
                        #
                        # The app travels with the licence in the upstream
                        # response (``InternalLicenseResponse.app``) and is what
                        # makes ``(tenant_id, app)`` -> licence a DETERMINISTIC
                        # lookup for cost attribution: every tenant that matters
                        # owns several licences (platform-core 20, platform-cp-1
                        # 5, platform-cp-3 4), so tenant alone cannot pick one
                        # and guessing would fabricate an invoice line.
                        #
                        # ⚠️ COUPLING, stated rather than discovered later: the
                        # app is only recorded on licences that reach this
                        # branch, i.e. those carrying a plan quota. Measured
                        # 2026-08-27 that is ALL 163 active licences, so coverage
                        # is currently complete -- but a licence with no plan
                        # quota would be silently unresolvable rather than
                        # wrong. `_resolve_license_for` treats that as "no
                        # match" and leaves the event unattributed.
                        app = lic.get("app") or {}
                        await upsert_request_quota_config(
                            session,
                            lic_uuid,
                            tenant_id,
                            quota_monthly,
                            app_id=_parse_optional_uuid(app.get("id")),
                            app_name=(app.get("name") or None),
                            app_key=(app.get("key") or None),
                        )
                        quotas_synced += 1

                # Read-only drift measurement, BEFORE the delete/mark reconcile
                # below mutates the mirror. Uses the SAME known_license_ids /
                # active_license_ids this run just built from the live fetch
                # above -- see services.quota.record_quota_config_drift for why
                # that pairing is the only truthful comparison available (no
                # dblink between this service's and licences' Postgres). Does
                # not delete or mark anything itself; it only publishes gauges.
                await record_quota_config_drift(
                    session,
                    known_license_ids=known_license_ids,
                    active_license_ids=active_license_ids,
                )

                # Reconcile away orphaned license_sync budgets. This runs ONLY on
                # a fully successful 200 fetch (inside the else + after the loop
                # completed without raising), so a failed/partial fetch can never
                # deactivate live budgets. An empty active set is a legitimate
                # "all licenses deactivated" signal.
                orphan_priors = await deactivate_orphan_license_sync_budgets(
                    session, active_license_ids=active_license_ids
                )

                # Same gate, same transaction, for the MIRROR itself. Nothing
                # removed a request_quota_config row when its licence stopped
                # being active upstream, so the mirror only ever grew stale --
                # 26 junk rows of 183 on prod at 2026-09-02, 18 of them for
                # licences that no longer exist in the registry at all. A stale
                # row can collide with a live licence on
                # (tenant_id, app_name), and the resolver DROPS an ambiguous
                # key, so the tenant's spend silently stops being attributable.
                #
                # delete_absent_quota_configs refuses an empty active set rather
                # than honouring it (unlike the budget reconcile above): this is
                # a hard DELETE of the row carrying the enforcement limit, and a
                # missing row makes quota_check fall back to a CALLER-SUPPLIED
                # limit. Fail open is the one outcome not worth risking on an
                # ambiguous signal.
                #
                # ⚠️ KEYED ON `known_license_ids`, NOT `active_license_ids`.
                # The fetch is no longer filtered to active, so "not in the
                # active set" no longer means "gone from the registry" — it now
                # also covers every REVOKED licence, and deleting those is
                # exactly the lossy behaviour migration 0025 replaced. A revoked
                # licence's row is MARKED (see mark_quota_config_status above)
                # and kept; only a licence the registry did not return AT ALL is
                # genuine junk and still deleted here. Passing the active set to
                # this call after widening the fetch would hard-DELETE 92 rows
                # that the status column exists to preserve.
                try:
                    absent_quota_ids = await delete_absent_quota_configs(
                        session, active_license_ids=known_license_ids
                    )
                except EmptyActiveSetError:
                    logger.warning("quota_config_reconcile_skipped_empty_active_set")
                    absent_quota_ids = []
                for absent_id in absent_quota_ids:
                    deleted_quota_config_ids.append(str(absent_id))
                if absent_quota_ids:
                    logger.info("quota_config_absent_rows_deleted", count=len(absent_quota_ids))
                for prior in orphan_priors:
                    await audit_record(
                        session,
                        request,
                        action="budget.deactivate_orphan",
                        resource_type="budget",
                        resource_id=str(prior.id),
                        tenant_id=prior.tenant_id,
                        before=_to_audit_dict(prior),
                        after=None,
                    )
                    orphaned_budget_ids.append(str(prior.id))

    except httpx.RequestError:
        logger.warning("licenses_service_unreachable", exc_info=True)
        error = "licenses service unreachable"

    # The APP registry, mirrored for X-App-Id key resolution (migration 0050).
    # Separate fetch, separate SAVEPOINT, and deliberately NOT gated on the
    # licence fetch above: the two are independent upstream reads, and the key
    # map must also cover apps that hold no licence at all. Its failure is
    # reported in `app_registry_error` and does not 502 a licence sync that
    # succeeded -- nor does a failed licence fetch stop the registry refresh.
    app_registry = await sync_app_registry(
        session, settings, getattr(request.app.state, "service_token_provider", None)
    )

    # One summary audit row per sync run regardless of outcome.
    await audit_record(
        session,
        request,
        action="budget.sync_from_licenses",
        resource_type="sync_job",
        resource_id=None,
        tenant_id=None,
        before=None,
        after={
            "created_count": created_count,
            "updated_count": updated_count,
            "processed_count": licenses_synced,
            "quotas_synced": quotas_synced,
            "skipped_count": skipped_count,
            # Licences the registry returned as revoked/expired/pending, and the
            # subset whose mirror row this run actually transitioned out of
            # 'active'. Reported separately from skipped_count: a non-active
            # licence was read and understood, it was not passed over.
            "non_active_count": non_active_count,
            "quota_configs_marked_non_active": len(marked_non_active_ids),
            "marked_non_active_ids": marked_non_active_ids,
            # Upstream returned a lifecycle value this service does not know.
            # NON-ZERO IS AN INCIDENT: those licences were left entirely
            # untouched this run, so their attribution is frozen at whatever the
            # mirror last held until LICENSE_STATUSES is widened.
            "unrecognised_status_count": unrecognised_status_count,
            "missing_status_count": missing_status_count,
            "orphans_deactivated": len(orphaned_budget_ids),
            "quota_configs_deleted": len(deleted_quota_config_ids),
            "license_ids": created_budget_ids,
            "touched_license_ids": touched_budget_ids,
            "orphaned_budget_ids": orphaned_budget_ids,
            "deleted_quota_config_ids": deleted_quota_config_ids,
            "error": error,
            "apps_synced": app_registry.upserted,
            "apps_removed": app_registry.removed,
            "apps_revoked": app_registry.revoked,
            "apps_invalid": app_registry.invalid,
            "app_registry_error": app_registry.error,
        },
    )
    await session.commit()
    if app_registry.error is None:
        # The X-App-Id key map is TTL-cached (300s). Reset only AFTER the commit:
        # a reset before it lets a concurrent flusher rebuild the map from the
        # pre-commit rows and keep that stale map for the whole TTL.
        from services.license_resolver import reset_cache

        reset_cache()

    if error is not None:
        # Total-fetch-failure branch: the licenses fetch either returned non-200
        # or the service was unreachable, so ZERO licenses were processed
        # (licenses_synced/quotas_synced are still 0 — the create loop never ran).
        # Returning 200 here made a CronJob/caller that checks only the HTTP
        # status see success while nothing synced. Fail loud with 502 so the
        # caller retries/alerts, while the audit row above still records the
        # attempt.
        raise HTTPException(
            status_code=502,
            detail={
                "status": "error",
                "licenses_synced": licenses_synced,
                "processed_count": licenses_synced,
                "created_count": created_count,
                "updated_count": updated_count,
                "quotas_synced": quotas_synced,
                "skipped_count": skipped_count,
                "orphans_deactivated": len(orphaned_budget_ids),
                "quota_configs_deleted": len(deleted_quota_config_ids),
                "error": error,
                "apps_synced": app_registry.upserted,
                "app_registry_error": app_registry.error,
            },
        )

    # Reached only when the licences fetch succeeded (the branch above raises
    # otherwise) -- i.e. exactly the condition the drift gauges above were
    # computed under. Marks "as of now" for the age-since-last-success gauge.
    record_quota_config_sync_success()

    return {
        "status": "ok",
        "licenses_synced": licenses_synced,
        "processed_count": licenses_synced,
        "created_count": created_count,
        "updated_count": updated_count,
        "quotas_synced": quotas_synced,
        # Non-zero means the licences service handed us rows this endpoint could
        # not attribute to a tenant. The run is still a 200 (it repaired
        # everything it could reach) — treat a persistent non-zero as an
        # upstream-data incident, per the docstring's "failure here is silent".
        "skipped_count": skipped_count,
        # Licences the registry reported as revoked/expired/pending, and the
        # mirror rows this run actually transitioned out of 'active' as a
        # result. Since migration 0025 those rows are MARKED and kept, not
        # deleted, so `quota_configs_deleted` no longer counts revocations —
        # it counts only licences the registry has forgotten entirely.
        "non_active_count": non_active_count,
        "quota_configs_marked_non_active": len(marked_non_active_ids),
        # ⚠️ NON-ZERO IS AN INCIDENT, not a tolerance. Upstream returned a
        # lifecycle value this service does not recognise, so those licences
        # were left untouched and their attribution is frozen at whatever the
        # mirror last held. Widen models.db.quota.LICENSE_STATUSES.
        "unrecognised_status_count": unrecognised_status_count,
        # Licences with no `status` field. Defaulted to 'active'. Upstream marks
        # the field REQUIRED, so a non-zero value here in production means the
        # contract changed and this default must become a skip.
        "missing_status_count": missing_status_count,
        "orphans_deactivated": len(orphaned_budget_ids),
        "quota_configs_deleted": len(deleted_quota_config_ids),
        # The X-App-Id key map's source (app_registry, migration 0050).
        # ⚠️ A non-null `app_registry_error` is an incident: the map keeps its
        # previous contents (or falls back to licensed apps only when empty).
        "apps_synced": app_registry.upserted,
        "apps_removed": app_registry.removed,
        "apps_revoked": app_registry.revoked,
        "apps_invalid": app_registry.invalid,
        "app_registry_error": app_registry.error,
    }
