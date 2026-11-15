"""``project_id -> owning tenant`` map, fetched whole from fr and cached.

WHY (decided 2026-09-28): per-project and per-member billing numbers are now a
product surface (George's per-member + per-project ruling for fr tenant pages),
so a ``project_id`` on a cost event is no longer an allocation tag nobody reads.
Any trusted pod that can publish to the cost stream can stamp ANY project id
and move spend onto another tenant's project page. Billing therefore checks the
asserted project against its owner of record.

Source of truth: fr's ``projects.project_data->>'keycloak_tenant_id'``, served
by ``GET {fr_base_url}/api/internal/projects/tenant-map`` behind an
``X-Internal-Token`` header. Response shape::

    {"projects": [{"project_id": "<uuid>", "tenant_id": "<uuid>|null"}],
     "generated_at": "..."}

Same cache shape as ``services.license_resolver.get_owner_map``: module globals,
whole-map fetch, ``_MAP_TTL_SECONDS`` TTL, :func:`reset_cache` for tests. There
is NO per-event HTTP — one fetch per TTL window, consulted once per flush batch.

``None`` from :func:`get_project_owner_map` means "no answer" (token or URL
unset, fr unreachable, non-200, unparseable, or an EMPTY map). Callers must
treat that as absence of knowledge, never as "no project is owned by anyone":
refuse only on positive knowledge. A failure is cached for
``_FAILURE_TTL_SECONDS`` so a down fr costs one timeout per minute, not one per
flush batch on the hot ingest path.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)

_MAP_TTL_SECONDS = 300
_FAILURE_TTL_SECONDS = 60
_TIMEOUT_SECONDS = 3.0
TENANT_MAP_PATH = "/api/internal/projects/tenant-map"

#: ``project_id (canonical str) -> owning tenant (canonical str) | None``.
#: A ``None`` VALUE means fr knows the project but it has no tenant of record.
ProjectOwnerMap = dict[str, str | None]

_cached_map: ProjectOwnerMap | None = None
_cached_at: float | None = None
_cached_ok: bool = False


def reset_cache() -> None:
    """Drop the cached map. For tests and a forced refresh."""
    global _cached_map, _cached_at, _cached_ok
    _cached_map = None
    _cached_at = None
    _cached_ok = False


def _canon(raw: Any) -> str | None:
    if raw is None:
        return None
    try:
        return str(uuid.UUID(str(raw).strip()))
    except ValueError, AttributeError, TypeError:
        return None


def parse_tenant_map(payload: Any) -> ProjectOwnerMap | None:
    """Parse fr's response. ``None`` when the shape is not the documented one
    or when it carries no usable rows (an empty map is not knowledge)."""
    if not isinstance(payload, dict):
        return None
    rows = payload.get("projects")
    if not isinstance(rows, list):
        return None
    out: ProjectOwnerMap = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        pid = _canon(row.get("project_id"))
        if pid is None:
            continue
        out[pid] = _canon(row.get("tenant_id"))
    return out or None


async def _fetch(base_url: str, token: str) -> ProjectOwnerMap | None:
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            resp = await client.get(
                f"{base_url.rstrip('/')}{TENANT_MAP_PATH}",
                headers={"X-Internal-Token": token},
            )
        if resp.status_code != 200:
            logger.warning("project_owner_map_fetch_non_200", status=resp.status_code)
            return None
        parsed = parse_tenant_map(resp.json())
        if parsed is None:
            logger.warning("project_owner_map_empty_or_malformed")
        return parsed
    except Exception as exc:  # fail soft: absence of knowledge, never a raise
        logger.warning("project_owner_map_fetch_failed", error=type(exc).__name__)
        return None


async def get_project_owner_map(
    base_url: str, token: str, *, now: float | None = None
) -> ProjectOwnerMap | None:
    """Return the cached map (``None`` = no answer), refetching on expiry."""
    global _cached_map, _cached_at, _cached_ok
    if not base_url or not token:
        return None
    stamp = time.monotonic() if now is None else now
    ttl = _MAP_TTL_SECONDS if _cached_ok else _FAILURE_TTL_SECONDS
    if _cached_at is not None and (stamp - _cached_at) < ttl:
        return _cached_map
    fetched = await _fetch(base_url, token)
    _cached_map = fetched
    _cached_ok = fetched is not None
    _cached_at = stamp
    return fetched
