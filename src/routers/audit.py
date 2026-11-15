"""Audit-log read endpoint (admin only)."""

from datetime import datetime

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from auth_deps import require_admin
from db import get_session
from models.domain import AuditLogEntry, Envelope, envelope
from services.audit import read_recent

router = APIRouter(prefix="/api/audit", tags=["audit"])


def _row_to_entry(row) -> AuditLogEntry:
    return AuditLogEntry(
        id=row.id,
        ts=row.ts,
        actor_sub=row.actor_sub,
        actor_role=row.actor_role,
        action=row.action,
        resource_type=row.resource_type,
        resource_id=row.resource_id,
        tenant_id=row.tenant_id,
        before=row.before,
        after=row.after,
        request_id=row.request_id,
    )


@router.get(
    "/recent",
    response_model=Envelope[list[AuditLogEntry]],
    operation_id="get_recent_audit_log",
)
async def get_recent_audit_log(
    request: Request,
    tenant_id: str | None = None,
    action: str | None = None,
    since: datetime | None = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
    _admin: dict = Depends(require_admin),
) -> dict:
    """Recent audit-log rows, newest-first. License-admin only."""
    rows = await read_recent(
        session,
        tenant_id=tenant_id,
        action=action,
        since=since,
        limit=limit,
        offset=offset,
    )
    payload = [_row_to_entry(r) for r in rows]
    return envelope(payload, request, total=len(payload))
