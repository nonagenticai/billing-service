"""Tenant webhook CRUD (T1.6).

Endpoints (all require a tenant header from ext_authz / admin JWT):

    POST   /api/webhooks         register a new webhook
    GET    /api/webhooks         list this tenant's webhooks
    DELETE /api/webhooks/{id}    deactivate a webhook (soft — flips active=False)

The shared admin-JWT path is allowed via ``require_tenant``'s normal
contract (it returns whichever tenant the JWT scopes to).
"""

from __future__ import annotations

import uuid

import httpx
import structlog
from fastapi import APIRouter, Depends, HTTPException, Path, Request
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import require_tenant
from db import get_session
from models.db import TenantWebhook
from models.domain import (
    Envelope,
    TenantWebhookCreate,
    TenantWebhookResponse,
    envelope,
)

logger = structlog.stdlib.get_logger(__name__)

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


def _to_response(row: TenantWebhook) -> TenantWebhookResponse:
    return TenantWebhookResponse(
        id=str(row.id),
        tenant_id=row.tenant_id,
        url=row.url,
        active=row.active,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@router.post(
    "",
    operation_id="register_webhook",
    response_model=Envelope[TenantWebhookResponse],
    status_code=201,
)
async def register_webhook(
    body: TenantWebhookCreate,
    request: Request,
    tenant_id: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> dict:
    if not body.url.lower().startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="webhook url must be http(s)")
    # The scheme check above is NOT enough — "https://" passes it. Such a value
    # is stored, and the budget watcher then POSTs it with httpx, which raises a
    # bare ``ValueError: unknown url type: '/'`` (measured; it is NOT an
    # httpx.HTTPError and NOT httpx.InvalidURL). That escapes _dispatch_one's
    # handlers and, before this fix, killed the watcher loop for EVERY tenant
    # until the pod restarted. httpx.URL() is too lenient to detect it —
    # it parses "https://" happily — so assert the thing that actually differs:
    # a non-empty host. Anything accepted here is guaranteed dispatchable.
    if not httpx.URL(body.url).host:
        raise HTTPException(status_code=400, detail="webhook url must include a host")
    row = TenantWebhook(
        tenant_id=tenant_id,
        url=body.url,
        secret=body.secret,
        active=body.active,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    logger.info("webhook_registered", tenant_id=tenant_id, webhook_id=str(row.id))
    return envelope(_to_response(row), request)


@router.get(
    "",
    operation_id="list_webhooks",
    response_model=Envelope[list[TenantWebhookResponse]],
)
async def list_webhooks(
    request: Request,
    tenant_id: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> dict:
    stmt = select(TenantWebhook).where(TenantWebhook.tenant_id == tenant_id)
    rows = list((await session.execute(stmt)).scalars().all())
    payload = [_to_response(r) for r in rows]
    return envelope(payload, request, total=len(payload))


@router.delete(
    "/{webhook_id}",
    operation_id="delete_webhook",
    status_code=204,
)
async def delete_webhook(
    webhook_id: str = Path(..., description="Webhook UUID"),
    tenant_id: str = Depends(require_tenant),
    session: AsyncSession = Depends(get_session),
) -> None:
    try:
        wid = uuid.UUID(webhook_id)
    except ValueError as err:
        raise HTTPException(status_code=400, detail="invalid webhook_id") from err

    stmt = (
        update(TenantWebhook)
        .where(TenantWebhook.id == wid, TenantWebhook.tenant_id == tenant_id)
        .values(active=False)
        .returning(TenantWebhook.id)
    )
    result = await session.execute(stmt)
    if result.scalar_one_or_none() is None:
        await session.rollback()
        raise HTTPException(status_code=404, detail="webhook not found")
    await session.commit()
    logger.info("webhook_deactivated", tenant_id=tenant_id, webhook_id=webhook_id)
