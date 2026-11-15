"""Configuration endpoints — provider rates, app rates, markup, tenant config."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import (
    adopt_forwarded_owner_actor,
    require_admin,
    require_pricing_admin,
    require_pricing_writer,
    resolve_tenant_or_400,
)
from db import get_session
from models.db import (
    AppCostRate,
    FxRate,
    MarkupRule,
    ProviderCost,
    TenantAccessCharge,
    TenantConfig,
)
from models.domain import (
    AccessChargeCreate,
    AccessChargeResponse,
    AppCostRateConfig,
    AppCostRateUpdate,
    AttributionRulePut,
    AttributionRuleResponse,
    CommercialTerm,
    CommercialTermsPut,
    CommercialTermsRequest,
    CommercialTermsResponse,
    CommercialTermsView,
    Envelope,
    FxRatePut,
    FxRateResponse,
    MarkupConfig,
    MarkupRuleCreate,
    MarkupRuleResponse,
    MarkupUpdate,
    ProviderCostConfig,
    ProviderCostUpdate,
    ProviderPricingCreate,
    ProviderPricingResponse,
    RateCard,
    TenantAllowanceUsageEntry,
    TenantAllowanceUsageResponse,
    TenantConfigResponse,
    TenantConfigUpdate,
    envelope,
)
from services import access_charge as access_charge_svc
from services import attribution_rules as attribution_svc
from services import markup as markup_svc
from services import pricing as pricing_svc
from services.audit import _to_audit_dict
from services.audit import record as audit_record
from services.billing import ACCESS_BASIS_CHARGE, ACCESS_BASIS_NO_MATCH
from services.seats import (
    SCOPED_SEAT_KEY_PREFIX,
    SEAT_BASIS_ACTIVE_LICENCES,
    SEAT_BASIS_UNAVAILABLE,
    SeatCountUnavailableError,
    get_seat_count,
)
from settings import Settings, get_settings
from tenancy import DEFAULT_CONFIG_TENANT_ID, is_registered_tenant_id, live_tenant_label

router = APIRouter(prefix="/api/config", tags=["config"])


logger = structlog.stdlib.get_logger(__name__)


def _current_month() -> str:
    return datetime.now(UTC).strftime("%Y-%m")


# --- Provider costs ---


@router.get(
    "/providers",
    response_model=Envelope[list[ProviderCostConfig]],
    operation_id="list_provider_configs",
)
async def list_provider_costs(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """List all provider cost rates."""
    stmt = select(ProviderCost).order_by(ProviderCost.provider)
    result = await session.execute(stmt)
    payload = [
        ProviderCostConfig(
            provider=p.provider,
            input_per_1m_usd=p.input_per_1m_usd,
            output_per_1m_usd=p.output_per_1m_usd,
        )
        for p in result.scalars().all()
    ]
    return envelope(payload, request, total=len(payload))


@router.put(
    "/providers/{provider}",
    response_model=Envelope[ProviderCostConfig],
    operation_id="upsert_provider_config",
)
async def update_provider_cost(
    request: Request,
    provider: str,
    data: ProviderCostUpdate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Update or create a provider cost rate."""
    stmt = select(ProviderCost).where(ProviderCost.provider == provider)
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    before_dict = _to_audit_dict(existing)

    if existing:
        existing.input_per_1m_usd = data.input_per_1m_usd
        existing.output_per_1m_usd = data.output_per_1m_usd
        existing.updated_at = datetime.now(UTC)
    else:
        existing = ProviderCost(
            provider=provider,
            input_per_1m_usd=data.input_per_1m_usd,
            output_per_1m_usd=data.output_per_1m_usd,
        )
        session.add(existing)

    await session.flush()
    await audit_record(
        session,
        request,
        action="config.provider.upsert",
        resource_type="provider",
        resource_id=provider,
        tenant_id=None,
        before=before_dict,
        after=_to_audit_dict(existing),
    )
    await session.commit()
    return envelope(
        ProviderCostConfig(
            provider=existing.provider,
            input_per_1m_usd=existing.input_per_1m_usd,
            output_per_1m_usd=existing.output_per_1m_usd,
        ),
        request,
    )


# --- App cost rates ---


@router.get(
    "/apps",
    response_model=Envelope[list[AppCostRateConfig]],
    operation_id="list_app_cost_rates",
)
async def list_app_cost_rates(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """List all per-app cost rates."""
    stmt = select(AppCostRate).order_by(AppCostRate.app_key)
    result = await session.execute(stmt)
    payload = [
        AppCostRateConfig(app_key=a.app_key, cost_per_request_usd=a.cost_per_request_usd)
        for a in result.scalars().all()
    ]
    return envelope(payload, request, total=len(payload))


@router.put(
    "/apps/{app_key}",
    response_model=Envelope[AppCostRateConfig],
    operation_id="upsert_app_cost_rate",
)
async def update_app_cost_rate(
    request: Request,
    app_key: str,
    data: AppCostRateUpdate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Update or create an app cost rate."""
    stmt = select(AppCostRate).where(AppCostRate.app_key == app_key)
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    before_dict = _to_audit_dict(existing)

    if existing:
        existing.cost_per_request_usd = data.cost_per_request_usd
        existing.updated_at = datetime.now(UTC)
    else:
        existing = AppCostRate(
            app_key=app_key,
            cost_per_request_usd=data.cost_per_request_usd,
        )
        session.add(existing)

    await session.flush()
    await audit_record(
        session,
        request,
        action="config.app.upsert",
        resource_type="app",
        resource_id=app_key,
        tenant_id=None,
        before=before_dict,
        after=_to_audit_dict(existing),
    )
    await session.commit()
    return envelope(
        AppCostRateConfig(
            app_key=existing.app_key, cost_per_request_usd=existing.cost_per_request_usd
        ),
        request,
    )


# --- Tenant config ---


@router.get(
    "/tenants/{tenant_id}",
    response_model=Envelope[TenantConfigResponse],
    operation_id="get_tenant_config",
)
async def get_tenant_config(
    request: Request,
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Get tenant config (markup, etc.)."""
    # PATH parameter, so ``require_tenant`` never saw it: resolve a registered
    # tenant NAME to its uuid here or the WHERE below is a raw DataError -> 400
    # with no stated reason. Also canonicalises the value echoed back on a miss.
    tenant_id = resolve_tenant_or_400(tenant_id)
    stmt = select(TenantConfig).where(TenantConfig.tenant_id == tenant_id)
    result = await session.execute(stmt)
    tc = result.scalar_one_or_none()

    if tc:
        return envelope(
            TenantConfigResponse(
                tenant_id=tc.tenant_id,
                # NULL = never configured (a money-budget PUT row): report the
                # same settings default a tenant with no row is reported with.
                markup_pct=(
                    float(tc.markup_pct)
                    if tc.markup_pct is not None
                    else get_settings().default_markup_pct
                ),
                billing_mode=tc.billing_mode,
                notes=tc.notes,
            ),
            request,
        )

    settings = get_settings()
    return envelope(
        TenantConfigResponse(
            tenant_id=tenant_id,
            markup_pct=settings.default_markup_pct,
            billing_mode="standard_markup",
            notes=None,
        ),
        request,
    )


def _registry_tenant_name(tenant_id: str) -> str:
    """The display name to store on a NEW ``tenant_config`` row: the licences
    registry's name for this uuid, else ``''``.

    ⛔ ALWAYS PASS THIS EXPLICITLY when constructing ``TenantConfig``. The model
    declares ``server_default=""`` and so SQLAlchemy omits an unset column, but
    the chain-built column had NO default until alembic 0056 — every first row
    for a brand-new tenant inserted NULL and 503'd (prod, 2026-10-13). Naming the
    column here keeps the write correct even on a database that predates 0056.

    ``''`` (not the uuid) on a registry miss: ``tenant_name`` is a display label,
    and echoing the uuid into it is the placeholder ``live_tenant_label``'s own
    docstring counts as noise.
    """
    return live_tenant_label(tenant_id) or ""


@router.put(
    "/tenants/{tenant_id}",
    response_model=Envelope[TenantConfigResponse],
    operation_id="upsert_tenant_config",
)
async def update_tenant_config(
    request: Request,
    tenant_id: str,
    data: TenantConfigUpdate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Update or create tenant config."""
    # PATH parameter — the same resolve the header gets. This value is both a
    # WHERE value and an INSERT value into a ``uuid`` column, so an unresolved
    # tenant NAME here was the reported `PUT /api/config/tenants/n8n-dev` -> 400.
    tenant_id = resolve_tenant_or_400(tenant_id)
    stmt = select(TenantConfig).where(TenantConfig.tenant_id == tenant_id)
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    before_dict = _to_audit_dict(existing)

    if existing:
        existing.markup_pct = Decimal(str(data.markup_pct))
        existing.notes = data.notes
        if data.billing_mode is not None:
            existing.billing_mode = data.billing_mode
        existing.updated_at = datetime.now(UTC)
    else:
        existing = TenantConfig(
            tenant_id=tenant_id,
            tenant_name=_registry_tenant_name(tenant_id),
            markup_pct=Decimal(str(data.markup_pct)),
            billing_mode=data.billing_mode or "standard_markup",
            notes=data.notes,
        )
        session.add(existing)

    await session.flush()
    await audit_record(
        session,
        request,
        action="config.tenant.upsert",
        resource_type="tenant_config",
        resource_id=tenant_id,
        tenant_id=tenant_id,
        before=before_dict,
        after=_to_audit_dict(existing),
    )
    await session.commit()
    return envelope(
        TenantConfigResponse(
            tenant_id=existing.tenant_id,
            markup_pct=float(Decimal(str(data.markup_pct))),
            billing_mode=existing.billing_mode,
            notes=existing.notes,
        ),
        request,
    )


# --- Global markup ---


@router.get(
    "/markup",
    response_model=Envelope[MarkupConfig],
    operation_id="get_markup_config",
)
async def get_default_markup(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Get default markup percentage (DB override > env var default)."""
    stmt = select(TenantConfig.markup_pct).where(TenantConfig.tenant_id == DEFAULT_CONFIG_TENANT_ID)
    result = await session.execute(stmt)
    db_val = result.scalar_one_or_none()
    if db_val is not None:
        return envelope(MarkupConfig(default_markup_pct=float(db_val)), request)
    settings = get_settings()
    return envelope(MarkupConfig(default_markup_pct=settings.default_markup_pct), request)


@router.put(
    "/markup",
    response_model=Envelope[MarkupConfig],
    operation_id="set_markup_config",
)
async def update_default_markup(
    request: Request,
    data: MarkupUpdate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_admin),
) -> dict:
    """Update default markup. Persisted in tenant_config under the reserved
    DEFAULT_CONFIG_TENANT_ID row (uuid5 of the old '__default__' sentinel — the
    column is a real uuid since 0011, so a string sentinel no longer fits)."""
    stmt = select(TenantConfig).where(TenantConfig.tenant_id == DEFAULT_CONFIG_TENANT_ID)
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    before_dict = _to_audit_dict(existing)

    if existing:
        existing.markup_pct = Decimal(str(data.default_markup_pct))
        existing.updated_at = datetime.now(UTC)
    else:
        existing = TenantConfig(
            tenant_id=DEFAULT_CONFIG_TENANT_ID,
            # The reserved global-default row, not a tenant: no registry name.
            tenant_name="",
            markup_pct=Decimal(str(data.default_markup_pct)),
            notes="Global default markup",
        )
        session.add(existing)

    await session.flush()
    await audit_record(
        session,
        request,
        action="config.markup.set",
        resource_type="markup",
        resource_id="global",
        tenant_id=None,
        before=before_dict,
        after=_to_audit_dict(existing),
    )
    await session.commit()
    return envelope(MarkupConfig(default_markup_pct=data.default_markup_pct), request)


# --- Provider pricing (effective-dated) ---


def _pricing_to_response(row) -> ProviderPricingResponse:
    return ProviderPricingResponse(**pricing_svc.pricing_to_response_dict(row))


@router.get(
    "/provider-pricing",
    response_model=Envelope[list[ProviderPricingResponse]],
    operation_id="list_current_provider_pricing",
)
async def list_provider_pricing(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """List currently-effective provider pricing rows (effective_until IS NULL).

    Sorted by (provider, model NULLS FIRST) so the provider-default row appears
    before per-model rows.
    """
    rows = await pricing_svc.list_current_pricing(session)
    payload = [_pricing_to_response(r) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.get(
    "/provider-pricing/history",
    response_model=Envelope[list[ProviderPricingResponse]],
    operation_id="list_provider_pricing_history",
)
async def list_provider_pricing_history(
    request: Request,
    provider: str = Query(..., description="Provider name (e.g. anthropic, openai)"),
    model: str | None = Query(
        None, description="Specific model name; omit to query the provider-default row"
    ),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """Full pricing history for a (provider, model) pair, ordered by effective_from DESC."""
    rows = await pricing_svc.list_pricing_history(session, provider=provider, model=model)
    payload = [_pricing_to_response(r) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.post(
    "/provider-pricing",
    response_model=Envelope[ProviderPricingResponse],
    operation_id="upsert_provider_pricing",
    status_code=status.HTTP_201_CREATED,
)
async def upsert_provider_pricing(
    request: Request,
    body: ProviderPricingCreate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """Create a new effective-dated pricing row.

    Closes any currently-open row for the same ``(provider, model)`` by setting
    its ``effective_until`` to the new row's ``effective_from``, then inserts
    the new open-ended row. Old events keep pointing at the old pricing_id —
    this is the historical-correctness primitive.
    """
    # 422, not 400: ``resolve_tenant_or_400`` has already taken 400 on this
    # service to mean "does not parse or resolve". This body parses and every
    # field resolves — it is the RELATIONSHIP to the row it would supersede that
    # is unsatisfiable, which is a different condition, and collapsing the two
    # destroys the distinction a caller needs to tell a malformed request from a
    # well-formed impossible one.
    try:
        new_row, prior = await pricing_svc.upsert_pricing(session, body)
    except pricing_svc.PricingSupersedeBeforePriorError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                "effective_from precedes the open pricing row it would supersede "
                f"({exc.prior_effective_from.isoformat()}); a superseding rate "
                "cannot start before the rate it replaces"
            ),
        ) from exc
    before_dict = pricing_svc.pricing_to_response_dict(prior) if prior is not None else None
    after_dict = pricing_svc.pricing_to_response_dict(new_row)
    await audit_record(
        session,
        request,
        action="config.pricing.upsert",
        resource_type="provider_pricing",
        resource_id=str(new_row.id),
        tenant_id=None,
        before=before_dict,
        after=after_dict,
    )
    await session.commit()
    return envelope(_pricing_to_response(new_row), request)


# --- Markup rules (effective-dated; north-star billing markup) ---


def _markup_to_response(row) -> MarkupRuleResponse:
    return MarkupRuleResponse(**markup_svc.markup_to_response_dict(row))


@router.get(
    "/markup-rules",
    response_model=Envelope[list[MarkupRuleResponse]],
    operation_id="list_current_markup_rules",
)
async def list_markup_rules(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """List currently-effective markup rules (effective_until IS NULL).

    Sorted most-specific-first under the consumption-dominant ladder, so the
    rule that would win for the most specific event appears at the top.
    """
    rows = await markup_svc.list_current_markup_rules(session)
    payload = [_markup_to_response(r) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.get(
    "/markup-rules/history",
    response_model=Envelope[list[MarkupRuleResponse]],
    operation_id="list_markup_rule_history",
)
async def list_markup_rule_history(
    request: Request,
    tenant_id: str | None = Query(None, description="Tenant; omit for the global scope"),
    provider: str | None = Query(None),
    model: str | None = Query(None),
    service_key: str | None = Query(None),
    license_id: UUID | None = Query(None),
    project_id: UUID | None = Query(
        None, description="Kanban project; omit for the tenant-wide lineage"
    ),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """Effective-dated history for one exact match-tuple, newest first.

    Each omitted dimension is matched as NULL, so the history is scoped to a
    single logical rule's lineage rather than every rule sharing a value.
    """
    # QUERY parameter. ``None`` is meaningful here — it selects the global
    # (NULL-tenant) lineage — so only a PRESENT value is resolved; collapsing
    # None through the resolver would turn "global scope" into a 400.
    if tenant_id is not None:
        tenant_id = resolve_tenant_or_400(tenant_id)
    rows = await markup_svc.list_markup_history(
        session,
        tenant_id=tenant_id,
        provider=provider,
        model=model,
        service_key=service_key,
        license_id=license_id,
        project_id=project_id,
    )
    payload = [_markup_to_response(r) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.post(
    "/markup-rules",
    response_model=Envelope[MarkupRuleResponse],
    operation_id="upsert_markup_rule",
    status_code=status.HTTP_201_CREATED,
)
async def upsert_markup_rule(
    request: Request,
    body: MarkupRuleCreate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_writer),
) -> dict:
    """Create a new effective-dated markup rule.

    Closes any currently-open row for the same full match-tuple
    ``(tenant, license, project, service_key, provider, model, byo)`` by setting
    its ``effective_until`` to the new row's ``effective_from``, then inserts the
    new open-ended row. Other tuples (incl. the global default) are untouched.

    ⭐ ``project_id`` (0036) is part of that tuple, so pricing one kanban project
    leaves the tenant-wide rule OPEN — it does not re-price the tenant's other
    projects. A project rule outranks the tenant-wide default and is itself
    outranked by a per-licence/app rule; ``specificity`` on the response shows
    where the new row sits. ``project_id`` without ``tenant_id`` is a 422.
    """
    new_row, prior = await markup_svc.upsert_markup_rule(session, body)
    before_dict = markup_svc.markup_to_response_dict(prior) if prior is not None else None
    after_dict = markup_svc.markup_to_response_dict(new_row)
    await audit_record(
        session,
        request,
        action="config.markup_rule.upsert",
        resource_type="markup_rule",
        resource_id=str(new_row.id),
        tenant_id=new_row.tenant_id,
        before=before_dict,
        after=after_dict,
    )
    await session.commit()
    return envelope(_markup_to_response(new_row), request)


# --- FX rates (alembic 0044; PLAN rev E §3.1) ---
#
# USD->EUR, owner-set, effective-dated. A money-budget tenant cannot be priced
# without one (`fx_rate_missing`), so this is billed-money config: both verbs
# carry the money-config write gate, per the plan.


def _fx_to_response(row: FxRate) -> FxRateResponse:
    return FxRateResponse(
        id=row.id,
        base=row.base,
        quote=row.quote,
        rate=f"{Decimal(row.rate).normalize():f}",
        effective_from=row.effective_from,
        effective_until=row.effective_until,
        set_by=row.set_by,
    )


def _fx_audit_dict(row: FxRate | None) -> dict | None:
    if row is None:
        return None
    return _fx_to_response(row).model_dump(mode="json")


@router.get(
    "/fx-rates",
    response_model=Envelope[list[FxRateResponse]],
    operation_id="list_fx_rates",
)
async def list_fx_rates(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_writer),
) -> dict:
    """Every FX row, newest effective_from first (the open row has no effective_until)."""
    rows = (
        (await session.execute(select(FxRate).order_by(FxRate.effective_from.desc())))
        .scalars()
        .all()
    )
    payload = [_fx_to_response(r) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.put(
    "/fx-rates",
    response_model=Envelope[FxRateResponse],
    operation_id="put_fx_rate",
)
async def put_fx_rate(
    request: Request,
    body: FxRatePut,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_writer),
) -> dict:
    """Open a new rate at ``effective_from`` (default now); close the open row there.

    Effective-dated like provider_pricing / markup_rules: the prior rate stays
    on record for events that happened under it.

    ⚠️ BACK-DATE THE FIRST ROW. Events settle at their own event time and there
    is no rate before the first row, so the FIRST rate must be PUT with an
    explicit ``effective_from`` at or before the earliest money event that can
    still settle (before the first money tenant's first reservation, with margin
    for delayed cost events). Defaulting it to now leaves earlier events with NULL
    consumption, undebited, counted as
    ``billing_money_unpriced_events_total{reason="before_first_fx_rate"}``.
    """
    # set_by names the HUMAN: fr relays its super_admin's portal JWT in
    # X-Forwarded-User-Authorization (the currency-change contract); without the
    # header it stays the authenticated caller's own sub. Verified before any
    # row is locked or written.
    await adopt_forwarded_owner_actor(request)
    effective_from = body.effective_from or datetime.now(UTC)
    if effective_from.tzinfo is None:
        effective_from = effective_from.replace(tzinfo=UTC)
    prior = (
        await session.execute(
            select(FxRate)
            .where(
                FxRate.base == body.base,
                FxRate.quote == body.quote,
                FxRate.effective_until.is_(None),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if prior is not None and prior.effective_from >= effective_from:
        raise HTTPException(
            status_code=422,
            detail="effective_from must be after the currently open rate's effective_from",
        )
    before = _fx_audit_dict(prior)
    if prior is not None:
        prior.effective_until = effective_from
    actor = getattr(request.state, "audit_actor", None) or {}
    row = FxRate(
        base=body.base,
        quote=body.quote,
        rate=body.rate,
        effective_from=effective_from,
        effective_until=None,
        set_by=actor.get("sub"),
    )
    session.add(row)
    await session.flush()
    await audit_record(
        session,
        request,
        action="config.fx_rate.put",
        resource_type="fx_rate",
        resource_id=str(row.id),
        tenant_id=None,
        before=before,
        after=_fx_audit_dict(row),
    )
    await session.commit()
    return envelope(_fx_to_response(row), request)


# --- Cost attribution rules (alembic 0041) ---
#
# Decide WHO PAYS for a cost event (bill tenant + member actor), so the writes
# carry the money-config gate (`require_pricing_writer`), not the broader
# `require_pricing_admin` the reads use — same split as markup-rules.


@router.get(
    "/attribution-rules",
    response_model=Envelope[list[AttributionRuleResponse]],
    operation_id="list_attribution_rules",
)
async def list_attribution_rules(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """List every attribution rule, enabled or not."""
    rows = await attribution_svc.list_rules(session)
    payload = [AttributionRuleResponse(**attribution_svc.rule_to_dict(r)) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.put(
    "/attribution-rules",
    response_model=Envelope[AttributionRuleResponse],
    operation_id="upsert_attribution_rule",
)
async def upsert_attribution_rule(
    request: Request,
    body: AttributionRulePut,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_writer),
) -> dict:
    """Create or replace the rule for ``(source_tenant_id, source_subject_id, match_project)``.

    Applies FORWARD ONLY to cost events ingested after the change (within the
    flusher's rule-cache TTL on other replicas). No stored event is rewritten.
    ``bill_tenant_id`` must be a REGISTERED tenant: `TenantId` accepts any
    well-formed uuid, and re-billing spend onto a tenant nothing vouches for
    would manufacture an unregistered payer.
    """
    if not is_registered_tenant_id(body.bill_tenant_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="bill_tenant_id is not a registered tenant",
        )
    row, before = await attribution_svc.upsert_rule(
        session,
        source_tenant_id=body.source_tenant_id,
        source_subject_id=body.source_subject_id,
        match_project=body.match_project,
        bill_tenant_id=body.bill_tenant_id,
        actor_id=body.actor_id,
        reason=body.reason,
        enabled=body.enabled,
    )
    await audit_record(
        session,
        request,
        action="config.attribution_rule.upsert",
        resource_type="attribution_rule",
        resource_id=str(row.id),
        tenant_id=row.source_tenant_id,
        before=before,
        after=attribution_svc.audit_dict(row),
    )
    await session.commit()
    attribution_svc.reset_cache()
    return envelope(AttributionRuleResponse(**attribution_svc.rule_to_dict(row)), request)


@router.delete(
    "/attribution-rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="delete_attribution_rule",
)
async def delete_attribution_rule(
    request: Request,
    rule_id: UUID,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_writer),
) -> None:
    """Delete one rule. Forward-only: events already attributed keep their tenant."""
    before = await attribution_svc.delete_rule(session, rule_id)
    if before is None:
        raise HTTPException(status_code=404, detail="attribution rule not found")
    await audit_record(
        session,
        request,
        action="config.attribution_rule.delete",
        resource_type="attribution_rule",
        resource_id=str(rule_id),
        tenant_id=before["source_tenant_id"],
        before=before,
        after=None,
    )
    await session.commit()
    attribution_svc.reset_cache()


# --- Access charges (effective-dated; the ACCESS-billing axis) ---


def _access_charge_to_response(row) -> AccessChargeResponse:
    return AccessChargeResponse(**access_charge_svc.access_charge_to_response_dict(row))


@router.get(
    "/access-charges",
    response_model=Envelope[list[AccessChargeResponse]],
    operation_id="list_current_access_charges",
)
async def list_access_charges(
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """List currently-effective access charges (effective_until IS NULL)."""
    rows = await access_charge_svc.list_current_access_charges(session)
    payload = [_access_charge_to_response(r) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.get(
    "/access-charges/history",
    response_model=Envelope[list[AccessChargeResponse]],
    operation_id="list_access_charge_history",
)
async def list_access_charge_history(
    request: Request,
    tenant_id: str = Query(..., description="Tenant whose charge lineage to fetch"),
    service_key: str | None = Query(
        None, description="Product; omit for the tenant-wide (NULL) charge"
    ),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """Effective-dated history for one (tenant, service_key) charge, newest first."""
    # QUERY parameter, required: reaches ``TenantAccessCharge.tenant_id`` (uuid).
    tenant_id = resolve_tenant_or_400(tenant_id)
    rows = await access_charge_svc.list_access_charge_history(
        session, tenant_id=tenant_id, service_key=service_key
    )
    payload = [_access_charge_to_response(r) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.get(
    "/tenant-allowance-usage",
    response_model=Envelope[TenantAllowanceUsageResponse],
    operation_id="get_tenant_allowance_usage",
)
async def get_tenant_allowance_usage(
    request: Request,
    tenant_id: str = Query(..., description="Tenant whose live usage/allowance to fold"),
    month: str = Query(
        default_factory=_current_month,
        # Bound the month segment to 01-12 at validation time. A bare `\d{2}`
        # accepts `2026-13` / `2026-00`, which then blow up in date(...) as an
        # unhandled ValueError -> 500; this rejects them as a 422 instead.
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="Billed month YYYY-MM; defaults to the current month",
    ),
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """Live used / allowance / remaining per product for a tenant-month.

    A pure compute-on-read fold over ``cost_daily`` (no persistence) that uses the
    same specificity ladder as :func:`services.billing.generate_billing`, so the
    figures reconcile with the invoice. Powers the fr "Your plan" tile.
    """
    # QUERY parameter, required: reaches both ``TenantAccessCharge.tenant_id``
    # and ``CostDaily.tenant_id`` (both uuid) inside the fold. Resolved before
    # the fold so the value echoed in the response is canonical too.
    tenant_id = resolve_tenant_or_400(tenant_id)
    month_start = date(int(month[:4]), int(month[5:7]), 1)
    folded = await access_charge_svc.tenant_allowance_usage(
        session, tenant_id=tenant_id, month_start=month_start
    )
    entries = [
        TenantAllowanceUsageEntry(
            service_key=key or None,
            used_usd=vals["used_usd"],
            allowance_usd=vals["allowance_usd"],
            remaining_usd=vals["remaining_usd"],
        )
        for key, vals in sorted(folded.items())
    ]
    payload = TenantAllowanceUsageResponse(tenant_id=tenant_id, month=month, entries=entries)
    return envelope(payload, request, total=len(entries))


@router.post(
    "/access-charges",
    response_model=Envelope[AccessChargeResponse],
    operation_id="upsert_access_charge",
    status_code=status.HTTP_201_CREATED,
)
async def upsert_access_charge(
    request: Request,
    body: AccessChargeCreate,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_writer),
) -> dict:
    """Create a new effective-dated access charge.

    Closes any currently-open row for the same ``(tenant_id, service_key)`` by
    setting its ``effective_until`` to the new row's ``effective_from``, then
    inserts the new open-ended row. Other tuples (the tenant-wide charge, other
    products) are untouched. Audit-logged, like markup-rules.
    """
    new_row, prior = await access_charge_svc.upsert_access_charge(session, body)
    before_dict = (
        access_charge_svc.access_charge_to_response_dict(prior) if prior is not None else None
    )
    after_dict = access_charge_svc.access_charge_to_response_dict(new_row)
    await audit_record(
        session,
        request,
        action="config.access_charge.upsert",
        resource_type="access_charge",
        resource_id=str(new_row.id),
        tenant_id=new_row.tenant_id,
        before=before_dict,
        after=after_dict,
    )
    await session.commit()
    return envelope(_access_charge_to_response(new_row), request)


# --------------------------------------------------------------------------- #
# Commercial terms — the per-tenant rate card, as one idempotent operation
#
# George, 2026-09-10: "i want to be able to configure at provisioning a new
# tenant and after they are live what their platform fee, seat fee, and local/byo
# and markup percentages are."
#
# The four terms already have homes (`tenant_access_charge` ×2, `markup_rules`
# ×2) and each has its own effective-dated, audit-logged upsert endpoint above.
# What did not exist was a way to ask for the whole rate card at once, which is
# the only shape a provisioning caller can use: it knows a tenant was born, not
# how billing models a platform fee.
#
# ⛔ WHY THIS IS FOUR ROWS AND NOT A SETTINGS LOOKUP. `tenant_access_charge.
# tenant_id` is NOT NULL, so there is no global default row to inherit from —
# a default can only become real by being materialised per tenant. And the
# markup defaults MUST be materialised too, even though `settings.
# default_markup_pct` would already produce 20%: a tenant riding the settings
# constant reports `markup_source=settings_default`, which is indistinguishable
# from "nobody has priced this tenant". Writing the rows is what makes the
# difference legible.
# --------------------------------------------------------------------------- #

#: The reserved `service_key` the per-seat access charge occupies.
#:
#: ⚠️ IT CANNOT BE NULL, and that is forced by the schema, not chosen for taste:
#: `ix_accesscharge_open` is UNIQUE on `(tenant_id, COALESCE(service_key, ''))`
#: for open rows, and the tenant-wide platform fee already owns the NULL slot.
#: Two NULL-service_key rows cannot both be open, so the seat charge needs its
#: own key or materialising the second term would close the first.
SEAT_SERVICE_KEY = "__seat__"  # pragma: allowlist secret - a service_key, not a credential


async def _open_access_charge(
    session: AsyncSession, tenant_id: str, service_key: str | None
) -> object | None:
    """The currently-open access charge for one (tenant, service_key), or None."""
    rows = await access_charge_svc.list_current_access_charges(session)
    return next(
        (r for r in rows if r.tenant_id == tenant_id and r.service_key == service_key),
        None,
    )


async def _open_markup_rule(
    session: AsyncSession, tenant_id: str, byo: bool | None
) -> object | None:
    """The currently-open markup rule for one (tenant, byo) tuple, or None.

    Scoped to rules that pin ONLY tenant (+byo) — a rule that also pins a
    provider or model is a different commercial statement and must not be
    mistaken for the tenant's baseline rate.

    ⚠️ ``project_id IS NULL`` belongs in that scope for the same reason (0036),
    and its absence would have been SILENT in both directions: this helper is
    both the idempotence guard for POST /tenants/{id}/terms and the reader for
    GET /tenants/{id}/terms. A tenant with a project-scoped rule and no baseline
    would have matched here, so provisioning would SKIP creating the baseline
    markup ("already has terms") and the terms response would publish ONE
    project's rate as the tenant's headline rate. ``.first()`` over an
    unordered SELECT makes which of the two it returns arbitrary.
    """
    stmt = select(MarkupRule).where(
        MarkupRule.effective_until.is_(None),
        MarkupRule.tenant_id == tenant_id,
        MarkupRule.license_id.is_(None),
        MarkupRule.project_id.is_(None),
        MarkupRule.service_key.is_(None),
        MarkupRule.provider.is_(None),
        MarkupRule.model.is_(None),
        MarkupRule.byo.is_(None) if byo is None else MarkupRule.byo == byo,
    )
    return (await session.execute(stmt)).scalars().first()


@router.post(
    "/tenants/{tenant_id}/terms",
    response_model=Envelope[CommercialTermsResponse],
    operation_id="materialise_tenant_terms",
    status_code=status.HTTP_201_CREATED,
)
async def materialise_tenant_terms(
    request: Request,
    tenant_id: str,
    body: CommercialTermsRequest | None = None,
    force: bool = Query(
        default=False,
        description=(
            "Re-price a tenant that already has terms: close each open row and "
            "open a new effective-dated one. Without it, existing terms are left "
            "exactly as they are."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    # Money-config WRITE gate, not the read gate. This route drives the very same
    # `upsert_access_charge` / `upsert_markup_rule` helpers as POST /access-charges
    # and POST /markup-rules, so it inherits #55's contract verbatim:
    # `billing-internal` alone must never set a customer's fees. Contract and its
    # reasoning live at `_PRICING_WRITER_ROLES` in auth_deps.py.
    _user: dict = Depends(require_pricing_writer),
) -> dict:
    """Put a tenant on the standard rate card, or on the overrides supplied.

    **Idempotent by construction.** Each of the four terms is created only when
    no open row already exists for its tuple, so a provisioning retry, a replay,
    or an operator running this twice produces no second row and no re-pricing.
    That matters more than it looks: `upsert_access_charge` and
    `upsert_markup_rule` CLOSE the prior open row, so a naive "always upsert"
    version of this endpoint would silently re-date a live tenant's terms on
    every retry — each replay ending the previous period and starting a new one,
    shredding the history the effective dating exists to keep.

    `?force=true` is the deliberate re-price: it goes through the same upsert
    helpers, so the prior row is closed rather than edited and the change is
    auditable.

    Returns the tenant's terms AFTER the call, in the same shape as
    `GET /tenants/{id}/terms`, so a caller never has to make a second request to
    learn what it just created.
    """
    tid = resolve_tenant_or_400(tenant_id)
    overrides = body or CommercialTermsRequest()
    settings = get_settings()
    now = datetime.now(UTC)

    platform_fee = (
        overrides.platform_fee_usd
        if overrides.platform_fee_usd is not None
        else Decimal(str(settings.default_platform_fee_usd))
    )
    seat_fee = (
        overrides.seat_fee_usd
        if overrides.seat_fee_usd is not None
        else Decimal(str(settings.default_seat_fee_usd))
    )
    markup_pct = (
        overrides.markup_pct
        if overrides.markup_pct is not None
        else Decimal(str(settings.default_markup_pct))
    )
    byo_markup_pct = (
        overrides.byo_markup_pct
        if overrides.byo_markup_pct is not None
        else Decimal(str(settings.default_byo_markup_pct))
    )
    note = overrides.notes or "Materialised by POST /api/config/tenants/{id}/terms"

    created: list[str] = []

    # --- platform fee: tenant-wide monthly access charge, NO allowance -------
    # included_cost_usd stays NULL deliberately: George chose "flat, usage billed
    # on top". A non-null allowance here would silently make the first N dollars
    # of usage free.
    if force or await _open_access_charge(session, tid, None) is None:
        row, prior = await access_charge_svc.upsert_access_charge(
            session,
            AccessChargeCreate(
                tenant_id=tid,
                service_key=None,
                amount_usd=platform_fee,
                cadence="monthly",
                included_cost_usd=None,
                effective_from=now,
                notes=note,
            ),
        )
        created.append("platform_fee")
        await audit_record(
            session,
            request,
            action="config.terms.platform_fee",
            resource_type="access_charge",
            resource_id=str(row.id),
            tenant_id=tid,
            before=access_charge_svc.access_charge_to_response_dict(prior) if prior else None,
            after=access_charge_svc.access_charge_to_response_dict(row),
        )

    # --- seat fee: per_seat cadence on the reserved service_key --------------
    if force or await _open_access_charge(session, tid, SEAT_SERVICE_KEY) is None:
        row, prior = await access_charge_svc.upsert_access_charge(
            session,
            AccessChargeCreate(
                tenant_id=tid,
                service_key=SEAT_SERVICE_KEY,
                amount_usd=seat_fee,
                cadence="per_seat",
                included_cost_usd=None,
                effective_from=now,
                notes=note,
            ),
        )
        created.append("seat_fee")
        await audit_record(
            session,
            request,
            action="config.terms.seat_fee",
            resource_type="access_charge",
            resource_id=str(row.id),
            tenant_id=tid,
            before=access_charge_svc.access_charge_to_response_dict(prior) if prior else None,
            after=access_charge_svc.access_charge_to_response_dict(row),
        )

    # --- markup: one byo-agnostic rule and one BYO-scoped rule ---------------
    for byo_flag, pct, label in (
        (None, markup_pct, "markup"),
        (True, byo_markup_pct, "byo_markup"),
    ):
        if not force and await _open_markup_rule(session, tid, byo_flag) is not None:
            continue
        row, prior = await markup_svc.upsert_markup_rule(
            session,
            MarkupRuleCreate(
                tenant_id=tid,
                byo=byo_flag,
                markup_pct=pct,
                effective_from=now,
                notes=note,
            ),
        )
        created.append(label)
        await audit_record(
            session,
            request,
            action=f"config.terms.{label}",
            resource_type="markup_rule",
            resource_id=str(row.id),
            tenant_id=tid,
            before=markup_svc.markup_to_response_dict(prior) if prior else None,
            after=markup_svc.markup_to_response_dict(row),
        )

    await session.commit()
    logger.info("tenant_terms_materialised", tenant_id=tid, created=created, force=force)
    payload = await _read_tenant_terms(session, tid, settings)
    return envelope(payload, request)


async def _read_tenant_terms(
    session: AsyncSession, tid: str, settings: Settings
) -> CommercialTermsResponse:
    """Assemble a tenant's currently-effective terms, each with its provenance."""
    fee_row = await _open_access_charge(session, tid, None)
    seat_row = await _open_access_charge(session, tid, SEAT_SERVICE_KEY)
    markup_row = await _open_markup_rule(session, tid, None)
    byo_row = await _open_markup_rule(session, tid, True)

    def _charge_term(row) -> CommercialTerm:
        if row is None:
            # NOT Decimal("0"): "no platform fee configured" and "a $0.00
            # platform fee" are different commercial facts.
            return CommercialTerm(value=None, source=ACCESS_BASIS_NO_MATCH)
        return CommercialTerm(
            value=Decimal(row.amount_usd),
            source=ACCESS_BASIS_CHARGE,
            effective_from=row.effective_from,
            effective_until=row.effective_until,
        )

    def _markup_term(row, fallback: float) -> CommercialTerm:
        if row is None:
            # The value IS what would be billed — but the source says nobody
            # decided it for this tenant, which is the distinction that matters.
            return CommercialTerm(
                value=Decimal(str(fallback)), source=markup_svc.MARKUP_SOURCE_SETTINGS
            )
        return CommercialTerm(
            value=row.markup_pct,
            source=f"{markup_svc.MARKUP_SOURCE_RULE_PREFIX}{row.id}",
            effective_from=row.effective_from,
            effective_until=row.effective_until,
        )

    seat_count: int | None = None
    seat_basis: str | None = None
    if seat_row is not None:
        # Only ask licences when a per-seat term exists — an outage must not make
        # a usage-only tenant's terms unreadable.
        try:
            seat_count = await get_seat_count(tid, settings)
            seat_basis = SEAT_BASIS_ACTIVE_LICENCES
        except SeatCountUnavailableError:
            seat_basis = SEAT_BASIS_UNAVAILABLE

    return CommercialTermsResponse(
        tenant_id=tid,
        platform_fee=_charge_term(fee_row),
        seat_fee=_charge_term(seat_row),
        markup=_markup_term(markup_row, settings.default_markup_pct),
        byo_markup=_markup_term(byo_row, settings.default_byo_markup_pct),
        seat_count=seat_count,
        seat_basis=seat_basis,
        fully_configured=all(r is not None for r in (fee_row, seat_row, markup_row, byo_row)),
    )


@router.get(
    "/tenants/{tenant_id}/terms",
    response_model=Envelope[CommercialTermsResponse],
    operation_id="get_tenant_terms",
)
async def get_tenant_terms(
    request: Request,
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """A tenant's currently-effective commercial terms, with provenance.

    `fully_configured` is the one-field answer to "has this tenant been priced?".
    A term whose `source` is `settings_default` carries the number that WOULD be
    billed while stating that no operator chose it for this tenant — the state
    every tenant in the estate was in before terms existed.

    ⚠️ The gate asymmetry with the POST on this same path — which is
    `require_pricing_writer` — is DELIBERATE, not an oversight: reading terms
    DISCLOSES, it does not write money, and provisioning/onboarding (which carry
    `billing-internal`) need to verify what a tenant is on. Do not "fix" this into
    symmetry; #55's contract is about who may CHANGE billed money.
    """
    tid = resolve_tenant_or_400(tenant_id)
    payload = await _read_tenant_terms(session, tid, get_settings())
    return envelope(payload, request)


@router.get(
    "/rate-card",
    response_model=Envelope[RateCard],
    operation_id="get_standard_rate_card",
)
async def get_rate_card(
    request: Request,
    # Same READ gate as GET /tenants/{id}/terms and /commercial-terms: reading
    # the price book discloses, it does not move money.
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """The STANDARD commercial rate card -- what `POST /tenants/{id}/terms` applies by default.

    Read from the same `settings.default_*` values that endpoint materialises, so a
    form prefilled from here and a tenant provisioned with an empty body agree by
    construction. There is no other "standard rate" store in billing:
    `tenant_access_charge.tenant_id` is NOT NULL, so no global default row can
    exist (see the commercial-terms section above). `source` says so.
    """
    settings = get_settings()
    card = RateCard(
        platform_fee_usd=Decimal(str(settings.default_platform_fee_usd)),
        seat_fee_usd=Decimal(str(settings.default_seat_fee_usd)),
        markup_pct=Decimal(str(settings.default_markup_pct)),
        byo_markup_pct=Decimal(str(settings.default_byo_markup_pct)),
    )
    return envelope(card, request)


# --------------------------------------------------------------------------- #
# ONE ATOMIC COMMERCIAL-TERMS CALL
#
# ⛔ THE DEFECT: A SAGA THAT DIES HALFWAY LEAVES A TENANT PRICED WRONG, FOREVER.
# A billed tenant (CommonPractice: flat platform fee + seats + BYO markup, and
# nothing else) was configured by FOUR separate requests in FOUR separate
# transactions — `PUT /tenants/{id}` for `billing_mode`, `POST /access-charges`
# twice, `POST /markup-rules` once. `licenses`' `provision_tenant_offering` saga
# drives them and can fail between any two. Its compensation rolls back only its
# OWN local posture, and billing exposes no DELETE for a markup rule or an access
# charge, so there is no way to undo the requests that DID land.
#
# What a half-apply actually costs, in both directions:
#   * dies after the access charges  -> tenant carries a $5,000 platform fee and
#     a seat rate with NO markup rule. It bills, plausibly, at
#     `settings.default_markup_pct` with `markup_source=settings_default` — the
#     exact signature of a tenant nobody has ever priced — so the error is
#     indistinguishable from the estate's baseline state.
#   * dies after the markup rule     -> a markup with no platform fee at all.
#     `GET /terms` reports `platform_fee.value = None`, which is honest, and
#     nothing bills. Silent lost revenue, every month, until someone reads it.
#
# This route writes all four terms inside ONE `session.commit()`. The FastAPI
# session dependency (`db.get_session`) opens the session with `async with` and
# never commits on its own, so ANY exception raised between here and the commit
# closes the session with the transaction unfinished and Postgres rolls the whole
# thing back. There is exactly one commit in this function and it is the last
# statement before the read-back; do not add a second.
#
# ⚠️ IT IS NOT `POST /tenants/{id}/terms` WITH MORE FIELDS, AND MUST NOT BE
# COLLAPSED INTO IT. That route materialises the STANDARD rate card and SKIPS any
# term that already exists — right for provisioning-by-default, wrong for stating
# terms. It writes no `billing_mode`, its seat term takes the reserved
# `__seat__` key (no per-app seat scope) and its markup rules pin no
# `service_key` (so they re-price every product the tenant consumes). Neither
# shape can express CommonPractice's contract.
# --------------------------------------------------------------------------- #

#: Term names reported in `CommercialTermsView.changed`, and the audit action
#: suffixes. One per writable term — a name that appears here and nowhere in the
#: writer below is a term this endpoint claims to set and does not.
_TERM_BILLING_MODE = "billing_mode"
_TERM_PLATFORM_FEE = "platform_fee"
_TERM_SEAT_FEE = "seat_fee"
_TERM_BYO_MARKUP = "byo_markup"


def _seat_service_key(app_key: str) -> str:
    """The stored `service_key` for a per-app seat term.

    ⭐ `seat:<app_key>`, NOT the `__seat__` that `POST /terms` uses, and the
    difference is a real billing behaviour rather than a naming preference:
    :func:`services.seats.seat_scope_for_charge` recognises ONLY this prefix and
    turns it into a PERIOD-SCOPED, PER-APP seat count (a mid-month joiner and a
    mid-month leaver each count). An unprefixed key falls through to the all-apps
    point-in-time count, which for a tenant licensed for three products bills
    CommonPractice's seat rate over all three products' seats.
    """
    return f"{SCOPED_SEAT_KEY_PREFIX}{app_key}"


async def _open_charges_for(session: AsyncSession, tenant_id: str) -> list[TenantAccessCharge]:
    """Every currently-open access charge for one tenant."""
    stmt = select(TenantAccessCharge).where(
        TenantAccessCharge.effective_until.is_(None),
        TenantAccessCharge.tenant_id == tenant_id,
    )
    return list((await session.execute(stmt)).scalars().all())


async def _open_scoped_byo_markup_rules(session: AsyncSession, tenant_id: str) -> list[MarkupRule]:
    """Open BYO markup rules for one tenant that pin a `service_key` and nothing else.

    Scoped as tightly as :func:`_open_markup_rule` and for the same reason: a
    rule that also pins a provider, a model, a licence or a kanban project is a
    DIFFERENT commercial statement, and mistaking one for the tenant's per-product
    BYO rate would make this endpoint's idempotence check skip a write it owed
    and make its read-back publish one provider's rate as the product's rate.

    Ordered by `service_key` so a tenant carrying rules for two products reads
    back deterministically rather than by whatever order the planner returned —
    an unordered `.first()` is the shape that makes a read-back answer a
    different question on each call.
    """
    stmt = (
        select(MarkupRule)
        .where(
            MarkupRule.effective_until.is_(None),
            MarkupRule.tenant_id == tenant_id,
            MarkupRule.byo.is_(True),
            MarkupRule.service_key.is_not(None),
            MarkupRule.license_id.is_(None),
            MarkupRule.project_id.is_(None),
            MarkupRule.provider.is_(None),
            MarkupRule.model.is_(None),
        )
        .order_by(MarkupRule.service_key)
    )
    return list((await session.execute(stmt)).scalars().all())


def _pick_seat_row(
    charges: list[TenantAccessCharge],
) -> tuple[TenantAccessCharge | None, list[str]]:
    """The tenant's per-seat term, plus the OTHER open per-seat lineages.

    ⛔ THE SECOND RETURN VALUE IS THE POINT. `generate_billing` discriminates a
    seat term by `cadence == "per_seat"` ALONE and ADDS every one it finds into
    `seat_fee_total`, while `ix_accesscharge_open` is unique per
    `(tenant_id, coalesce(service_key,''))` — so a legacy `__seat__` row and a
    `seat:<app>` row are two different OPEN slots and the tenant is billed TWO
    seat fees on one invoice. Returning only the "best" row would hide exactly
    that state behind a response that looks correct.

    Prefers a `seat:`-prefixed row because that is the only form carrying a
    per-app seat scope; ties break on `service_key` so the answer does not depend
    on row order.
    """

    def _rank(c: TenantAccessCharge) -> tuple[bool, str]:
        key = c.service_key or ""
        return (not key.startswith(SCOPED_SEAT_KEY_PREFIX), key)

    seats = sorted((c for c in charges if c.cadence == "per_seat"), key=_rank)
    if not seats:
        return None, []
    return seats[0], [c.service_key or "" for c in seats[1:]]


async def _read_commercial_terms(
    session: AsyncSession,
    tid: str,
    settings: Settings,
    *,
    changed: list[str] | None = None,
) -> CommercialTermsView:
    """Assemble a tenant's currently-effective commercial terms, with provenance.

    Discovers the seat and markup terms FROM STORAGE rather than from a request
    body, because `GET` has no body and because a read that only finds the rows
    it was told to look for cannot report the ones nobody meant to leave behind
    (see :func:`_pick_seat_row`).
    """
    charges = await _open_charges_for(session, tid)
    fee_row = next((c for c in charges if c.service_key is None), None)
    seat_row, extra_seat_keys = _pick_seat_row(charges)
    markup_rows = await _open_scoped_byo_markup_rules(session, tid)
    markup_row = markup_rows[0] if markup_rows else None

    tc = (
        await session.execute(select(TenantConfig).where(TenantConfig.tenant_id == tid))
    ).scalar_one_or_none()

    def _charge_term(row: TenantAccessCharge | None) -> CommercialTerm:
        if row is None:
            # NOT Decimal("0") — "no fee configured" and "a $0.00 fee" are
            # different commercial facts, and only the first means somebody
            # still has to decide. Same rule `_read_tenant_terms` applies.
            return CommercialTerm(value=None, source=ACCESS_BASIS_NO_MATCH)
        return CommercialTerm(
            value=Decimal(row.amount_usd),
            source=ACCESS_BASIS_CHARGE,
            effective_from=row.effective_from,
            effective_until=row.effective_until,
        )

    if markup_row is None:
        # ⚠️ The value is what WOULD be billed on a BYO line today; the source is
        # what says nobody chose it for this tenant. Publishing the number alone
        # is what made an unpriced tenant indistinguishable from a priced one.
        byo_markup = CommercialTerm(
            value=Decimal(str(settings.default_byo_markup_pct)),
            source=markup_svc.MARKUP_SOURCE_SETTINGS,
        )
    else:
        byo_markup = CommercialTerm(
            value=markup_row.markup_pct,
            source=f"{markup_svc.MARKUP_SOURCE_RULE_PREFIX}{markup_row.id}",
            effective_from=markup_row.effective_from,
            effective_until=markup_row.effective_until,
        )

    return CommercialTermsView(
        tenant_id=tid,
        # ⚠️ `TenantConfig.billing_mode` carries `server_default='standard_markup'`,
        # so a tenant with no row bills as `standard_markup` — a real, live
        # posture that nobody decided. Reported with its source rather than as a
        # bare string for the same reason every other term here is.
        billing_mode=tc.billing_mode if tc is not None else markup_svc.BILLING_MODE_STANDARD,
        billing_mode_source=(
            markup_svc.MARKUP_SOURCE_TENANT_CONFIG
            if tc is not None
            else markup_svc.MARKUP_SOURCE_SETTINGS
        ),
        platform_fee=_charge_term(fee_row),
        seat_fee=_charge_term(seat_row),
        seat_service_key=seat_row.service_key if seat_row is not None else None,
        byo_markup=byo_markup,
        markup_service_key=markup_row.service_key if markup_row is not None else None,
        extra_seat_service_keys=extra_seat_keys,
        fully_configured=all(x is not None for x in (tc, fee_row, seat_row, markup_row)),
        changed=list(changed or []),
    )


@router.put(
    "/tenants/{tenant_id}/commercial-terms",
    response_model=Envelope[CommercialTermsView],
    operation_id="put_tenant_commercial_terms",
)
async def put_commercial_terms(
    request: Request,
    tenant_id: str,
    body: CommercialTermsPut,
    session: AsyncSession = Depends(get_session),
    # Money-config WRITE gate, identical to `POST /tenants/{id}/terms` and to the
    # `POST /access-charges` / `POST /markup-rules` helpers this route drives:
    # `billing-internal` alone must never set a customer's fees (#55).
    _user: dict = Depends(require_pricing_writer),
) -> dict:
    """Set a tenant's ENTIRE commercial rate card in one database transaction.

    Writes, in this order and under one commit:

    1. `tenant_config.billing_mode` — the outer gate over every markup resolution.
    2. a tenant-wide `cadence='monthly'` access charge — the flat platform fee,
       with `included_cost_usd` left NULL so usage bills ON TOP of it rather than
       the first N dollars being silently free.
    3. a `cadence='per_seat'` access charge on `seat:<seat_app_key>` — a RATE,
       multiplied by that product's seat count at bill time.
    4. a `byo=True` markup rule scoped to `markup_service_key`.

    ⭐ IDEMPOTENT ON VALUE, NOT ON EXISTENCE, and the distinction is the whole
    reason this is safe to retry. `upsert_access_charge` and `upsert_markup_rule`
    CLOSE the prior open row and open a new one — that is what makes the history
    auditable, and it is also what makes a naive "a PUT always writes" version
    catastrophic: every provisioning retry would end the current period and start
    a fresh one, so a tenant would appear re-priced on whatever schedule the saga
    happened to retry on, and the effective-dated history the two stores exist to
    keep would be shredded into noise. So each term is compared to the OPEN row
    first and rewritten only when the requested value actually differs. A replay
    with the same body writes nothing, commits nothing, and returns
    `changed: []`.

    ⚠️ AND `changed` IS THE OBSERVABLE THAT MAKES THAT CHECKABLE. Without it a
    caller cannot tell "the terms already said this" from "I just re-dated four
    periods", which are the two outcomes whose difference matters.

    ⛔ THE LEGACY `__seat__` ROW IS CLOSED WHEN THIS WRITES `seat:<app>`. They are
    DIFFERENT open slots under `ix_accesscharge_open`, and
    `services.billing.generate_billing` sums EVERY `cadence='per_seat'` row it
    finds — so leaving both open bills the tenant twice for seats, silently, on a
    line that looks correct in isolation. A tenant previously put on the standard
    card by `POST /tenants/{id}/terms` is exactly the tenant this endpoint is
    called on next, so this is the common path, not an edge case.
    """
    tid = resolve_tenant_or_400(tenant_id)
    settings = get_settings()
    now = body.effective_from or datetime.now(UTC)
    seat_key = _seat_service_key(body.seat_app_key)
    note = body.notes or "Set by PUT /api/config/tenants/{id}/commercial-terms"
    changed: list[str] = []

    # --- 1. billing_mode ----------------------------------------------------
    tc = (
        await session.execute(select(TenantConfig).where(TenantConfig.tenant_id == tid))
    ).scalar_one_or_none()
    tc_before = _to_audit_dict(tc)
    if tc is None:
        # ⚠️ `markup_pct` is NOT NULL on this table and is NOT a term of this
        # endpoint — the markup that actually bills is the effective-dated
        # `markup_rules` row written at step 4, and `services.markup` prefers a
        # resolved rule over this column. Seeded from settings so the row is
        # well-formed; writing `byo_markup_pct` here instead would put a BYO rate
        # in the column that prices NON-BYO lines.
        tc = TenantConfig(
            tenant_id=tid,
            tenant_name=_registry_tenant_name(tid),
            markup_pct=Decimal(str(settings.default_markup_pct)),
            billing_mode=body.billing_mode,
            notes=note,
        )
        session.add(tc)
        changed.append(_TERM_BILLING_MODE)
    elif tc.billing_mode != body.billing_mode:
        tc.billing_mode = body.billing_mode
        tc.updated_at = now
        changed.append(_TERM_BILLING_MODE)
    if _TERM_BILLING_MODE in changed:
        await session.flush()
        await audit_record(
            session,
            request,
            action=f"config.commercial_terms.{_TERM_BILLING_MODE}",
            resource_type="tenant_config",
            resource_id=tid,
            tenant_id=tid,
            before=tc_before,
            after=_to_audit_dict(tc),
        )

    # --- 2 + 3. the two access charges --------------------------------------
    open_charges = await _open_charges_for(session, tid)

    async def _ensure_charge(
        *,
        service_key: str | None,
        amount: Decimal,
        # The LITERAL, not `str`: `AccessChargeCreate.cadence` is a closed set, and a plain `str`
        # here means the call site's typo would only be caught at runtime by pydantic, inside the
        # transaction this function exists to keep atomic.
        cadence: Literal["monthly", "one_time", "per_seat"],
        term: str,
    ) -> None:
        """Write one access charge iff the open row does not already say this.

        Compared on `(amount, cadence)` — the two fields this endpoint sets that
        change what is billed. `notes` is deliberately NOT part of the
        comparison: re-dating a live tenant's fee because somebody fixed a typo
        in a note would be the very re-pricing this idempotence exists to
        prevent.
        """
        prior_open = next((c for c in open_charges if c.service_key == service_key), None)
        if (
            prior_open is not None
            and Decimal(prior_open.amount_usd) == amount
            and prior_open.cadence == cadence
        ):
            return
        row, prior = await access_charge_svc.upsert_access_charge(
            session,
            AccessChargeCreate(
                tenant_id=tid,
                service_key=service_key,
                amount_usd=amount,
                cadence=cadence,
                # ⛔ NULL, always. A non-null allowance on the platform fee makes
                # the first N dollars of usage free, which is a different product
                # nobody agreed to; on a `per_seat` row `AccessChargeCreate`
                # refuses it outright (a seat-scaled allowance is undesigned).
                included_cost_usd=None,
                effective_from=now,
                notes=note,
            ),
        )
        changed.append(term)
        await audit_record(
            session,
            request,
            action=f"config.commercial_terms.{term}",
            resource_type="access_charge",
            resource_id=str(row.id),
            tenant_id=tid,
            before=access_charge_svc.access_charge_to_response_dict(prior) if prior else None,
            after=access_charge_svc.access_charge_to_response_dict(row),
        )

    await _ensure_charge(
        service_key=None,
        amount=body.platform_fee_usd,
        cadence="monthly",
        term=_TERM_PLATFORM_FEE,
    )
    await _ensure_charge(
        service_key=seat_key,
        amount=body.seat_fee_usd,
        cadence="per_seat",
        term=_TERM_SEAT_FEE,
    )

    # --- 3b. retire any OTHER open per-seat lineage --------------------------
    # See the ⛔ in this function's docstring: two open `per_seat` rows are two
    # seat fees on one invoice, because `generate_billing` keys on the CADENCE
    # and `ix_accesscharge_open` keys on the `service_key`. Closed in place
    # rather than through `upsert_access_charge`, because there is no successor
    # row for the retired lineage — the successor IS the `seat:<app>` term above,
    # under a different key.
    for c in open_charges:
        if c.cadence == "per_seat" and c.service_key != seat_key:
            # ⚠️ SNAPSHOT BEFORE MUTATING. `access_charge_to_response_dict` reads
            # the LIVE object, so building the `before` dict after assigning
            # `effective_until` records the closed row as its own prior state —
            # an audit entry whose before and after are identical, i.e. a record
            # that says nothing happened while something did.
            before_dict = access_charge_svc.access_charge_to_response_dict(c)
            c.effective_until = now
            changed.append(f"retired_seat_term:{c.service_key}")
            await audit_record(
                session,
                request,
                action="config.commercial_terms.retire_seat_term",
                resource_type="access_charge",
                resource_id=str(c.id),
                tenant_id=tid,
                before=before_dict,
                after=access_charge_svc.access_charge_to_response_dict(c),
            )

    # --- 4. the BYO markup rule, scoped to one product ----------------------
    prior_rule = next(
        (
            r
            for r in await _open_scoped_byo_markup_rules(session, tid)
            if r.service_key == body.markup_service_key
        ),
        None,
    )
    if prior_rule is None or prior_rule.markup_pct != body.byo_markup_pct:
        rule, closed = await markup_svc.upsert_markup_rule(
            session,
            MarkupRuleCreate(
                tenant_id=tid,
                service_key=body.markup_service_key,
                # ⭐ `byo=True`, not NULL. Under `byo_markup_only` a BYO line
                # bills its percentage and NOT the provider cost; a byo-agnostic
                # rule at the same tuple would also re-price the tenant's
                # platform-provided lines for this product, which is a different
                # contract. `byo` is part of `ix_markuprules_open`'s tuple, so
                # the two can coexist and neither closes the other.
                byo=True,
                markup_pct=body.byo_markup_pct,
                effective_from=now,
                notes=note,
            ),
        )
        changed.append(_TERM_BYO_MARKUP)
        await audit_record(
            session,
            request,
            action=f"config.commercial_terms.{_TERM_BYO_MARKUP}",
            resource_type="markup_rule",
            resource_id=str(rule.id),
            tenant_id=tid,
            before=markup_svc.markup_to_response_dict(closed) if closed else None,
            after=markup_svc.markup_to_response_dict(rule),
        )

    # ⛔ THE ONLY COMMIT IN THIS FUNCTION. Everything above is flush-only, so any
    # exception raised before this line leaves the transaction unfinished and the
    # session dependency's `async with` rolls it back in full — which is the
    # atomicity this endpoint exists to provide. Do not add a second commit, and
    # do not move this one earlier to "save" partial progress: partial progress
    # IS the defect.
    await session.commit()
    logger.info(
        "tenant_commercial_terms_set",
        tenant_id=tid,
        billing_mode=body.billing_mode,
        changed=changed,
    )
    payload = await _read_commercial_terms(session, tid, settings, changed=changed)
    return envelope(payload, request)


@router.get(
    "/tenants/{tenant_id}/commercial-terms",
    response_model=Envelope[CommercialTermsView],
    operation_id="get_tenant_commercial_terms",
)
async def get_commercial_terms(
    request: Request,
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_pricing_admin),
) -> dict:
    """A tenant's currently-effective commercial terms, with provenance.

    ⚠️ The gate asymmetry with the PUT on this same path is DELIBERATE and
    identical to the one on `/terms`: reading terms DISCLOSES, it does not move
    money, and provisioning/onboarding callers (which carry `billing-internal`)
    must be able to verify what a tenant is on without being able to change it.

    `changed` is always `[]` here — it reports what a PUT did, and a GET does
    nothing.
    """
    tid = resolve_tenant_or_400(tenant_id)
    payload = await _read_commercial_terms(session, tid, get_settings())
    return envelope(payload, request)
