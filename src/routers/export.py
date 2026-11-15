"""Streaming CSV export endpoints for compliance + auditor reconciliation.

Two surfaces:

* ``GET /api/export/audit.csv``      — every audit_log row in a window
* ``GET /api/export/billing.csv``    — every cost_event row in a window
                                       (per-tenant invoice reconciliation)

Both stream the response so a multi-MB result doesn't load into memory.
Both are admin-gated (Keycloak ``license-admin`` role) — these are
operator + auditor surfaces, not customer-facing.

Pagination cursor is ``id`` (BIGINT, autoincrement) for audit_log and
``(created_at, id)`` for cost_events. Page size is bounded
(``MAX_PAGE_SIZE``) so a single transaction doesn't hold an enormous
result-set lock; chunks are streamed immediately.
"""

from __future__ import annotations

import csv
import io
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import StreamingResponse

from auth_deps import require_admin
from db import get_session
from models.db import AuditLog, CostEvent
from service_auth import ServiceTokenProvider
from services.tenant_names import get_tenant_names, resolve_tenant_name
from settings import get_settings

logger = structlog.stdlib.get_logger(__name__)

router = APIRouter(prefix="/api/export", tags=["export"])

# Maximum rows per DB fetch. A single user-visible export is typically
# 1k–100k rows; we stream them out as they're fetched so the client sees
# bytes flow within sub-second even on multi-MB exports.
MAX_PAGE_SIZE = 1000

# Maximum window an export can cover. 90 days is the raw-event retention
# window in metering, so anything older won't have backing data anyway —
# attempting to export it just gives a misleading short result.
MAX_WINDOW_DAYS = 92


def _parse_window(
    since: datetime | None,
    until: datetime | None,
) -> tuple[datetime, datetime]:
    """Default window: last 30d. Reject windows wider than MAX_WINDOW_DAYS."""
    now = datetime.now(UTC)
    end = until or now
    start = since or (end - timedelta(days=30))
    if end <= start:
        raise HTTPException(status_code=400, detail="until must be > since")
    if (end - start) > timedelta(days=MAX_WINDOW_DAYS):
        raise HTTPException(
            status_code=400,
            detail=f"window > {MAX_WINDOW_DAYS} days; chunk it (raw event retention is 90d)",
        )
    return start, end


def _csv_line(row: Sequence[object]) -> str:
    """Render one CSV line with the stdlib writer (handles quoting/escaping)."""
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerow(row)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Audit export
# ---------------------------------------------------------------------------

_AUDIT_HEADER = [
    "id",
    "ts",
    "actor_sub",
    "actor_role",
    "action",
    "resource_type",
    "resource_id",
    "tenant_id",
    "tenant_name",
    "request_id",
    "before_json",
    "after_json",
]


async def _stream_audit(
    session: AsyncSession,
    start: datetime,
    end: datetime,
    tenant_id: str | None,
    action: str | None,
) -> AsyncIterator[str]:
    yield _csv_line(_AUDIT_HEADER)

    cursor_id: int | None = None
    while True:
        stmt = select(AuditLog).where(AuditLog.ts >= start, AuditLog.ts < end)
        if tenant_id is not None:
            stmt = stmt.where(AuditLog.tenant_id == tenant_id)
        if action is not None:
            stmt = stmt.where(AuditLog.action == action)
        if cursor_id is not None:
            stmt = stmt.where(AuditLog.id > cursor_id)
        stmt = stmt.order_by(AuditLog.id.asc()).limit(MAX_PAGE_SIZE)

        result = await session.execute(stmt)
        rows = list(result.scalars().all())
        if not rows:
            return

        for row in rows:
            yield _csv_line(
                [
                    row.id,
                    row.ts.isoformat() if row.ts else "",
                    row.actor_sub or "",
                    row.actor_role or "",
                    row.action,
                    row.resource_type,
                    row.resource_id or "",
                    row.tenant_id or "",
                    row.tenant_name or "",
                    row.request_id or "",
                    "" if row.before is None else _json_compact(row.before),
                    "" if row.after is None else _json_compact(row.after),
                ]
            )
        if len(rows) < MAX_PAGE_SIZE:
            return
        cursor_id = rows[-1].id


def _json_compact(value: object) -> str:
    """Serialize a JSONB value as a compact string for the CSV cell."""
    import json

    return json.dumps(value, separators=(",", ":"), default=str)


@router.get(
    "/audit.csv",
    operation_id="export_audit_csv",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/csv": {}}}},
)
async def export_audit_csv(
    since: datetime | None = Query(default=None, description="Inclusive lower bound (UTC)."),
    until: datetime | None = Query(default=None, description="Exclusive upper bound (UTC)."),
    tenant_id: str | None = Query(default=None, description="Optional tenant filter."),
    action: str | None = Query(default=None, description="Optional action filter."),
    session: AsyncSession = Depends(get_session),
    _admin: dict = Depends(require_admin),
) -> StreamingResponse:
    """Stream the audit_log within a window as CSV.

    Default window is the last 30 days. Maximum window is
    ``MAX_WINDOW_DAYS`` (92d) — chunk longer requests caller-side.
    Filename ``audit-YYYYMMDD-YYYYMMDD.csv`` so consumers can group.
    """
    start, end = _parse_window(since, until)
    fname = f"audit-{start:%Y%m%d}-{end:%Y%m%d}.csv"
    return StreamingResponse(
        _stream_audit(session, start, end, tenant_id, action),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ---------------------------------------------------------------------------
# Billing export
# ---------------------------------------------------------------------------

_BILLING_HEADER = [
    "id",
    "created_at",
    "tenant_id",
    "tenant_name",
    "license_id",
    "subject_id",
    "service_key",
    "cost_source",
    "provider",
    "model",
    "served_model",
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
    "cost_usd",
    "pricing_id",
    "provider_request_id",
    "correlation_id",
    "latency_ms",
    # APPENDED, not grouped next to license_id/subject_id, deliberately. Since
    # markup prices per (tenant, project) rule, two events byte-identical in
    # every other exported column can bill different amounts, and this is the
    # SOLE markup match-tuple dimension that exists on a CostEvent row — without
    # it the export cannot be reconciled to the invoice at all. But column
    # ORDER is a published contract: this CSV leaves the estate and an auditor's
    # sheet or script may index positionally, and appending is the only position
    # that leaves all 20 existing indices untouched. Grouping it beside the other
    # identity uuids would shift 14 columns to buy readability that no consumer
    # can enforce — in-repo readers go through ``csv.DictReader`` and key by name.
    "project_id",
]


async def _billing_tenant_names(request: Request) -> dict[str, str]:
    """Best-effort ``{tenant_id_lowercase: display_name}`` for the CSV label column.

    FAIL SOFT, always. ``get_tenant_names`` already swallows every failure and
    caps itself at a 3s timeout, but this export is a compliance surface an
    auditor pulls under deadline: a licences outage may cost it its labels, not
    its 200. The extra ``except Exception`` is deliberate belt-and-braces — an
    empty map degrades every row to the uuid, which is byte-identical to what
    this column emitted before resolution existed, so the worst case here is
    exactly today's output and never a failed or hung download.
    """
    try:
        token_provider: ServiceTokenProvider | None = getattr(
            request.app.state, "service_token_provider", None
        )
        return await get_tenant_names(get_settings(), token_provider)
    except Exception:  # pragma: no cover - get_tenant_names is already blind
        logger.info("export_tenant_names_unavailable", exc_info=True)
        return {}


async def _stream_billing(
    session: AsyncSession,
    start: datetime,
    end: datetime,
    tenant_id: str | None,
    include_unpriced: bool,
    tenant_names: dict[str, str],
) -> AsyncIterator[str]:
    yield _csv_line(_BILLING_HEADER)

    cursor_ts: datetime | None = None
    cursor_id: object | None = None
    while True:
        stmt = select(CostEvent).where(
            CostEvent.created_at >= start,
            CostEvent.created_at < end,
            # This export is the invoice-reconciliation surface: it must sum to
            # what the customer is billed, so a parked duplicate is not a row here.
            CostEvent.excluded_from_billing.is_(False),
        )
        if tenant_id is not None:
            stmt = stmt.where(CostEvent.tenant_id == tenant_id)
        if not include_unpriced:
            stmt = stmt.where(CostEvent.cost_usd.is_not(None))
        if cursor_ts is not None and cursor_id is not None:
            # Composite cursor: (created_at, id) > (cursor_ts, cursor_id)
            stmt = stmt.where(
                (CostEvent.created_at > cursor_ts)
                | ((CostEvent.created_at == cursor_ts) & (CostEvent.id > cursor_id))
            )
        stmt = stmt.order_by(CostEvent.created_at.asc(), CostEvent.id.asc()).limit(MAX_PAGE_SIZE)

        result = await session.execute(stmt)
        rows = list(result.scalars().all())
        if not rows:
            return

        for row in rows:
            yield _csv_line(
                [
                    str(row.id),
                    row.created_at.isoformat() if row.created_at else "",
                    row.tenant_id,
                    # Resolve, do not echo. The emitters write the uuid itself
                    # into cost_events.tenant_name when they only know the uuid
                    # (services/_parsers.py:original_tenant_label), so the raw
                    # column is a uuid on 29,883 of 29,883 live prod rows — this
                    # export was handing a customer their own tenant uuid in the
                    # column labelled "tenant_name". `resolve_tenant_name` is
                    # the SAME helper /dashboard/costs uses, and it already
                    # discards a stored name that merely equals the uuid, so the
                    # two surfaces cannot drift apart. Falls back to the uuid.
                    resolve_tenant_name(row.tenant_id, row.tenant_name, tenant_names),
                    str(row.license_id) if row.license_id else "",
                    str(row.subject_id) if row.subject_id else "",
                    row.service_key,
                    row.cost_source,
                    row.provider,
                    row.model or "",
                    row.served_model or "",
                    row.input_tokens,
                    row.output_tokens,
                    row.cache_creation_input_tokens,
                    row.cache_read_input_tokens,
                    "" if row.cost_usd is None else f"{row.cost_usd}",
                    row.pricing_id if row.pricing_id is not None else "",
                    row.provider_request_id or "",
                    str(row.correlation_id) if row.correlation_id else "",
                    row.latency_ms if row.latency_ms is not None else "",
                    # Same rendering as license_id / subject_id above: the uuid
                    # stringified, and "" when absent. ``CostEvent.project_id``
                    # is nullable and NEVER defaulted — a raw cost event either
                    # carried a project or genuinely had none — so "" is the
                    # honest cell. ⛔ No sentinel and no "unknown" here: the
                    # project sentinel exists only on ``rollups.CostDaily``,
                    # where the ON CONFLICT key forces it.
                    str(row.project_id) if row.project_id else "",
                ]
            )

        if len(rows) < MAX_PAGE_SIZE:
            return
        cursor_ts = rows[-1].created_at
        cursor_id = rows[-1].id


@router.get(
    "/billing.csv",
    operation_id="export_billing_csv",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/csv": {}}}},
)
async def export_billing_csv(
    request: Request,
    since: datetime | None = Query(default=None, description="Inclusive lower bound (UTC)."),
    until: datetime | None = Query(default=None, description="Exclusive upper bound (UTC)."),
    tenant_id: str | None = Query(default=None, description="Optional tenant filter."),
    include_unpriced: bool = Query(
        default=False,
        description=(
            "Include rows where cost_usd is NULL. Defaults False so invoice "
            "exports don't accidentally include events that bypassed pricing."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    _admin: dict = Depends(require_admin),
) -> StreamingResponse:
    """Stream cost_events within a window as CSV.

    Default window is the last 30 days; max ``MAX_WINDOW_DAYS`` (92d).
    By default unpriced rows (``cost_usd IS NULL``) are EXCLUDED so the
    CSV matches what billing.generate_billing() would actually invoice.
    Pass ``?include_unpriced=true`` to dump everything (auditing the
    pricing-coverage gap).
    """
    start, end = _parse_window(since, until)
    fname_tenant = f"-{tenant_id}" if tenant_id else ""
    fname = f"billing{fname_tenant}-{start:%Y%m%d}-{end:%Y%m%d}.csv"
    # Resolved ONCE for the whole export, before streaming starts: the licences
    # endpoint returns every tenant in one call and the result is TTL-cached, so
    # a 100k-row export costs at most one 3s lookup, not one per row.
    tenant_names = await _billing_tenant_names(request)
    return StreamingResponse(
        _stream_billing(session, start, end, tenant_id, include_unpriced, tenant_names),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )
