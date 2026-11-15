"""Quota check and configuration endpoints."""

import uuid
from typing import Any, cast

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import delete as sql_delete
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import (
    enforce_tenant_isolation,
    require_internal,
    require_tenant,
    resolve_tenant_or_400,
)
from db import get_session
from models.db import NULL_PLAN_ID, RequestQuotaConfig
from models.domain import Envelope, QuotaStatus, TenantId, TenantQuotaStatus, envelope
from service_auth import ServiceTokenProvider
from services.audit import record as audit_record
from services.billing import LicensesUnavailableError, fetch_licenses
from services.licence_limits import effective_limit
from services.quota import check_quota, resolve_quota_limits, upsert_request_quota_config
from settings import get_settings

logger = structlog.stdlib.get_logger(__name__)

router = APIRouter(prefix="/api/quota", tags=["quota"])


class QuotaConfigRequest(BaseModel):
    license_id: str
    # ``ge=0``: a configured **0 means block-all** (a zero-allowance / suspended
    # plan), not unlimited. Unlimited is the ABSENCE of a config (no row / NULL
    # limit), never a stored 0. Negatives stay rejected. Was ``gt=0``, which made
    # a genuine block-all unrepresentable and left 0 to leak through as a
    # cap==0=unlimited signal.
    quota_monthly: int = Field(ge=0)
    # Required (min_length=1): the durable row's tenant_id is NOT NULL (a
    # NULL/absent tenant-of-record is cross-tenant-readable — see the model
    # docstring and migration 0007). Enforcing it here turns a tenant-less
    # config into a clear 422 client error rather than a 503 NOT-NULL violation.
    #
    # ``TenantId`` (not ``str``): this model is declared HERE, not in
    # models/domain.py, so the BeforeValidator that resolves every other body
    # tenant field did not reach it and `{"tenant_id": "n8n-dev"}` — a name the
    # X-Tenant-Id header accepts — died as an opaque DataError on the uuid
    # column. The annotated type resolves it exactly once; do NOT also call
    # resolve_tenant_or_400 on this field.
    tenant_id: TenantId = Field(min_length=1)
    # ⭐ WHY THESE TWO ARE HERE — the licence resolver is BLIND without app_id.
    #
    # ``license_resolver._build_map`` (services/license_resolver.py:114-118) selects
    # candidates with ``where(RequestQuotaConfig.app_id.is_not(None))``. A row whose
    # ``app_id`` is NULL is not a candidate and can never resolve, so every cost event
    # for that licence's tenant stays unattributed forever.
    #
    # Before this field existed, ``app_id`` had exactly ONE writer in the estate:
    # ``POST /api/budget/sync-from-licenses``, which nothing schedules. The armed daily
    # CronJob (``cronjob-metering-reconcile``, ``41 3 * * *``) pushes HERE instead, and
    # its payload could not carry an app because this model had no field for it — so
    # every licence created since migration 0014 landed with ``app_id IS NULL`` and was
    # invisible to the resolver. Measured 2026-08-28: 26 of 189 rows.
    #
    # ⛔ BOTH ARE OPTIONAL AND MUST STAY OPTIONAL. ``upsert_request_quota_config`` uses
    # ``func.coalesce(excluded.app_id, RequestQuotaConfig.app_id)`` (services/quota.py:71),
    # so an omitted value CANNOT clobber a good one — it only declines to set it. Making
    # either field required would 422 every existing caller, including the reconcile.
    app_id: uuid.UUID | None = None
    app_name: str | None = None


@router.get(
    "/check",
    response_model=Envelope[QuotaStatus],
    operation_id="check_quota",
)
async def quota_check(
    request: Request,
    license_id: str = Query(...),
    plan_id: str | None = Query(default=None),
    quota_monthly: int | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Check quota status for a license.

    Tenant isolation: when the caller carries an ``X-Tenant-Id`` header — a
    customer / license-authz-forwarded tenant context, as opposed to an admin
    JWT — it may only read a license that belongs to its OWN tenant. The durable
    ``request_quota_config`` row is the tenant-of-record (provisioning always
    writes ``tenant_id`` on ``register_quota``), so a row owned by a DIFFERENT
    tenant is reported as 404 — a foreign license must be indistinguishable from
    a non-existent one (do not confirm B's license exists to tenant A). This
    mirrors ``enforce_tenant_isolation`` on ``quota_status`` (header-gated) and
    the 404-for-foreign-resource pattern.

    We deliberately ONLY 404 a CONFIRMED foreign owner: the caller's own license
    (matching row tenant_id, or no row at all) always passes. ``tenant_id`` is
    NOT NULL (migration 0007), so every durable row carries an owner to compare
    against — the old "row with no tenant_id" escape hatch (served cross-tenant
    to anyone) no longer exists. A wrongful 404 here would become a wrongful
    customer-facing 402 the instant license-authz runs with
    ``METERING_FAIL_OPEN=false`` (its ``check`` turns a 4xx into a deny), so the
    check never blocks anything but a tenant proven to differ from the durable
    row's owner.
    """
    try:
        lic_uuid = uuid.UUID(license_id)
        plan_uuid = uuid.UUID(plan_id) if plan_id else None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid UUID format") from exc

    # Durable config is the source of truth (limit AND tenant-of-record); the
    # query param is only a fallback for a license with no configured row (e.g.
    # the license-authz HTTP path, which sends no quota_monthly).
    config = await session.get(RequestQuotaConfig, lic_uuid)

    # Customer callers (X-Tenant-Id present) may only read their own licenses. A
    # config row owned by a DIFFERENT tenant → 404 (foreign, and we do not even
    # confirm it exists). A missing row cannot be proven foreign, so it is served
    # — never 404 the caller's own license. tenant_id is NOT NULL (0007), so a
    # present row always has an owner to compare (no NULL-tenant escape hatch).
    if request.headers.get("x-tenant-id") and config is not None and config.tenant_id != _tenant:
        raise HTTPException(status_code=404, detail="License not found")

    limit = config.quota_monthly if config is not None else quota_monthly

    result = await check_quota(lic_uuid, plan_uuid, limit, session)
    return envelope(QuotaStatus(**result), request)


@router.get(
    "/status/{tenant_id}",
    response_model=Envelope[TenantQuotaStatus],
    operation_id="get_tenant_quota_status",
)
async def quota_status(
    request: Request,
    tenant_id: str,
    session: AsyncSession = Depends(get_session),
    _tenant: str = Depends(require_tenant),
) -> dict:
    """Get quota status for all active licenses in a tenant.

    Fetches the tenant's active licenses from the licenses service (S2S) and
    reports per-license monthly-quota usage via ``check_quota``. The limit is
    the durable ``request_quota_config`` row when one exists, else the licenses-service ``plan.quota_monthly``; a license is reported
    when it has EITHER a configured quota OR a plan (a configured-but-planless
    license is no longer silently dropped). If the licenses service cannot be
    consulted this returns **503**, because an empty list here is a real answer
    ("no licenses, therefore no quotas") and reporting it for an outage is a
    fail-OPEN read dressed as a healthy one. The docstring used to call that
    "fails closed"; it was not.
    """
    enforce_tenant_isolation(request, tenant_id, _tenant)
    # PATH parameter. Resolved AFTER the isolation check on purpose: that check
    # canonicalises both sides itself, so a caller naming ANOTHER tenant must
    # still get the 403 rather than have a 400 leak whether that tenant exists.
    # Past this point the value is the S2S lookup key for the licences service
    # and is echoed in the response body, so both want the canonical uuid.
    tenant_id = resolve_tenant_or_400(tenant_id)

    settings = get_settings()
    provider: ServiceTokenProvider | None = getattr(
        request.app.state, "service_token_provider", None
    )
    try:
        licenses = await fetch_licenses(tenant_id, settings, token_provider=provider)
    except LicensesUnavailableError as exc:
        # 503, not an empty 200. The caller cannot distinguish "you have no
        # quotas" from "I could not find out" if both render as licenses: [].
        raise HTTPException(
            status_code=503,
            detail="licenses service unavailable; quota status cannot be determined",
        ) from exc

    # Pass 1: parse ids. plan is optional — a license with a durable quota but no
    # plan must still be reported (plan_id echoes NULL_PLAN_ID; it is never a usage
    # predicate, see services.quota._get_db_usage).
    parsed: list[tuple[uuid.UUID, uuid.UUID, int | None]] = []
    for lic in licenses:
        lic_id = lic.get("id")
        if not lic_id:
            continue
        plan = lic.get("plan") or {}
        plan_id = plan.get("id")
        try:
            lic_uuid = uuid.UUID(str(lic_id))
            plan_uuid = uuid.UUID(str(plan_id)) if plan_id else NULL_PLAN_ID
        except ValueError, TypeError:
            logger.warning("quota_status_unparseable_ids", license_id=lic_id, plan_id=plan_id)
            continue
        # Per-licence override wins over the plan default (services.licence_limits).
        quota = effective_limit(
            lic.get("quota_override"),
            plan.get("quota_monthly"),
            field="quota_override",
            license_id=lic_id,
        )
        parsed.append((lic_uuid, plan_uuid, quota))

    # Durable config wins over the plan value; one query for the whole batch.
    configured = await resolve_quota_limits([p[0] for p in parsed], session)

    statuses: list[QuotaStatus] = []
    for lic_uuid, plan_uuid, plan_quota in parsed:
        limit = configured.get(lic_uuid, plan_quota)
        if limit is None:
            # Neither a configured quota nor a plan-defined one → nothing to report.
            continue
        result = await check_quota(lic_uuid, plan_uuid, limit, session)
        statuses.append(QuotaStatus(**result))

    return envelope(
        TenantQuotaStatus(tenant_id=tenant_id, licenses=statuses), request, total=len(statuses)
    )


@router.post(
    "/config",
    status_code=201,
    operation_id="set_quota_config",
)
async def set_quota_config(
    request: Request,
    data: QuotaConfigRequest,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_internal),
) -> dict[str, str]:
    """Persist a license's request quota limit (``request_quota_config``)."""
    try:
        uuid.UUID(data.license_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid license_id UUID") from exc

    after_payload = {
        "license_id": data.license_id,
        "quota_monthly": data.quota_monthly,
    }
    await audit_record(
        session,
        request,
        action="quota.config.set",
        resource_type="quota_config",
        resource_id=data.tenant_id or data.license_id,
        tenant_id=data.tenant_id,
        before=None,
        after=after_payload,
    )
    # Upsert the durable row in the SAME tx as the audit record via the shared
    # helper (same helper POST /api/budget/sync-from-licenses uses).
    # ``app_id``/``app_name`` are threaded through so the licence resolver can see this
    # row at all — see the field comments on QuotaConfigRequest. Passing ``None`` is safe
    # and is the pre-existing behaviour: the helper COALESCEs onto the stored value.
    await upsert_request_quota_config(
        session,
        uuid.UUID(data.license_id),
        data.tenant_id,
        data.quota_monthly,
        app_id=data.app_id,
        app_name=data.app_name,
    )
    await session.commit()
    return {"status": "ok", "license_id": data.license_id, "quota_monthly": str(data.quota_monthly)}


@router.delete(
    "/config/{license_id}",
    operation_id="delete_quota_config",
)
async def delete_quota_config(
    request: Request,
    license_id: str,
    session: AsyncSession = Depends(get_session),
    _user: dict = Depends(require_internal),
) -> dict[str, str]:
    """Remove a license's request-quota config row.

    Called by provisioning on deprovision. Idempotent: deleting an absent config
    is a no-op success.
    """
    try:
        lic_uuid = uuid.UUID(license_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid license_id UUID") from exc

    # CursorResult.rowcount is concrete on DELETE; the abstract Result return
    # type loses this attribute, so cast to the concrete type.
    result = cast(
        "CursorResult[Any]",
        await session.execute(
            sql_delete(RequestQuotaConfig).where(RequestQuotaConfig.license_id == lic_uuid)
        ),
    )
    deleted = result.rowcount or 0
    await audit_record(
        session,
        request,
        action="quota.config.delete",
        resource_type="quota_config",
        resource_id=license_id,
        tenant_id=None,
        before={"license_id": license_id},
        after=None,
    )
    await session.commit()
    return {"status": "ok", "license_id": license_id, "deleted": str(deleted)}
