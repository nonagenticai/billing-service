"""Audit-log service.

Every mutation through metering's WRITE surface (POST/PUT/DELETE on budget,
quota config, provider/app/tenant config, markup) records an `audit_log` row
in the SAME DB transaction as the mutation. The route handler is responsible
for committing — `record(...)` only flushes its INSERT into the session.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import sqlalchemy as sa
import structlog
from fastapi import Request
from sqlalchemy import select
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.inspection import inspect

from models.db import AuditLog

logger = structlog.stdlib.get_logger(__name__)


def _to_audit_dict(instance: Any | None) -> dict[str, Any] | None:
    """Convert a SQLAlchemy ORM instance to a JSON-serializable dict.

    - Skips relationship attrs (only column attrs are walked).
    - Decimal -> str (preserves precision).
    - datetime/date -> isoformat string.
    - UUID -> str.
    - Other primitives passed through.

    Returns None if `instance` is None.
    """
    if instance is None:
        return None
    if isinstance(instance, dict):
        return _sanitize_dict(instance)

    mapper = inspect(instance).mapper
    out: dict[str, Any] = {}
    for col in mapper.column_attrs:
        out[col.key] = _coerce(getattr(instance, col.key))
    return out


def _sanitize_dict(d: dict[str, Any]) -> dict[str, Any]:
    return {k: _coerce(v) for k, v in d.items()}


def _coerce(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return _sanitize_dict(value)
    if isinstance(value, (list, tuple)):
        return [_coerce(v) for v in value]
    # uuid.UUID, bytes, etc. — fall through to str() for non-primitives.
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


async def record(
    session: AsyncSession,
    request: Request,
    *,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    tenant_id: str | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
) -> None:
    """Record one audit row in the caller's session.

    Caller controls the transaction boundary. We `flush()` so the INSERT
    becomes visible to subsequent queries within the same transaction, but
    we never commit. If the caller's commit fails, the audit row rolls back
    too — that's the whole point.
    """
    actor = getattr(request.state, "audit_actor", None) or {}
    actor_sub = actor.get("sub")
    actor_role = actor.get("role")
    request_id = getattr(request.state, "request_id", None)

    if actor_sub is None:
        logger.warning(
            "audit_actor_missing",
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        actor_sub = "unknown"

    row = AuditLog(
        ts=datetime.now(UTC),
        actor_sub=actor_sub,
        actor_role=actor_role,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        tenant_id=tenant_id,
        before=_sanitize_dict(before) if before is not None else None,
        after=_sanitize_dict(after) if after is not None else None,
        request_id=request_id,
    )
    session.add(row)
    await session.flush()


async def purge_old_audit_log(session: AsyncSession, retention_days: int) -> int:
    """Drop audit_log rows older than ``retention_days``.

    Audit rows accumulate forever otherwise — every config / budget / pricing
    write adds one. 180-day retention is the default; shorter retentions can be
    configured via ``AUDIT_RETENTION_DAYS`` for environments that
    forward the events to a warehouse and don't need on-line history.

    Returns the number of rows actually deleted. Idempotent: a second call
    with the same cutoff drops zero rows. Caller controls commit (we ``commit``
    here because retention is owned end-to-end by this function and isn't
    interleaved with other work).
    """
    if retention_days <= 0:
        # 0 or negative disables retention — useful for tests / manual runs.
        return 0
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    # CursorResult.rowcount is concrete on DELETE; the abstract Result return
    # type loses this attribute, so cast to the concrete type.
    result = cast(
        "CursorResult[Any]",
        await session.execute(sa.delete(AuditLog).where(AuditLog.ts < cutoff)),
    )
    await session.commit()
    deleted = result.rowcount or 0
    if deleted:
        logger.info(
            "audit_log_purged",
            retention_days=retention_days,
            cutoff=cutoff.isoformat(),
            deleted=deleted,
        )
    return deleted


async def read_recent(
    session: AsyncSession,
    *,
    tenant_id: str | None = None,
    action: str | None = None,
    since: datetime | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[AuditLog]:
    """Return recent audit rows newest-first, with optional filters."""
    stmt = select(AuditLog)
    if tenant_id is not None:
        stmt = stmt.where(AuditLog.tenant_id == tenant_id)
    if action is not None:
        stmt = stmt.where(AuditLog.action == action)
    if since is not None:
        stmt = stmt.where(AuditLog.ts >= since)
    stmt = stmt.order_by(AuditLog.ts.desc()).limit(limit).offset(offset)
    result = await session.execute(stmt)
    return list(result.scalars().all())
