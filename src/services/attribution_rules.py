"""Cost attribution rules — bill autonomous spend to a tenant MEMBER (alembic 0041).

Operator policy (2026-09-25): every improvement stream (workflow / procedure /
prompt) is billed as a member of the fr-test tenant; execution projects bill to
that tenant's execution member; asset-authoring spend follows the project it
happens in. All of those streams call through ONE shared licence subject, so
neither the bill tenant nor the member is derivable from the wire — they are
configured here, keyed on (source tenant, optional subject, project presence).

Where this runs, and why there
------------------------------
:func:`apply_attribution` is called by ``services.cost_flusher.flush_costs``
AFTER licence resolution, licence adjudication and app derivation. Running it
earlier would make those three see the BILL tenant: the resolver would derive a
different licence (or none), and ``adjudicate_asserted_license`` would refuse
the source tenant's own licence as ``foreign_tenant`` — nulling ``license_id``
and silently dropping the spend from every invoice line
(``CostDaily.license_attributed_clause``). So the licence stays the one the
source tenant actually used; only ``tenant_id`` / ``actor_id`` move.

What deliberately does NOT move (see the flusher's call site)
-------------------------------------------------------------
Budget debit, the ``token_usage`` rollup and the pre-flight reservation
settlement stay on the SOURCE tenant: the reservation was taken against the
source tenant's Redis pool, so settling it on the bill tenant would leak the
source pool and over-release the bill tenant's. Exclusion flags
(``excluded_from_billing``) are left as computed on the source.

Cache
-----
Rules are read once per ``_RULES_TTL_SECONDS`` per process, same module-TTL
shape as ``services.license_resolver``. A write through the admin API resets
the local cache; other replicas pick it up within the TTL.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import NULL_PROJECT_ID, AttributionRule

#: Short on purpose: a new rule should take effect within a minute on every
#: replica, and the read is one tiny indexed query.
_RULES_TTL_SECONDS = 60

#: metadata keys stamped on a re-attributed cost event.
ATTRIBUTED_FROM_KEY = "attributed_from_tenant_id"
ATTRIBUTION_RULE_ID_KEY = "attribution_rule_id"


@dataclass(frozen=True)
class Rule:
    id: uuid.UUID
    source_tenant_id: str
    source_subject_id: uuid.UUID | None
    match_project: str
    bill_tenant_id: str
    actor_id: uuid.UUID | None


RulesMap = Mapping[str, tuple[Rule, ...]]

_cached: dict[str, tuple[Rule, ...]] | None = None
_cached_at: float = 0.0


def reset_cache() -> None:
    global _cached, _cached_at
    _cached = None
    _cached_at = 0.0


async def _build_rules_map(session: AsyncSession) -> dict[str, tuple[Rule, ...]]:
    rows = (
        await session.execute(select(AttributionRule).where(AttributionRule.enabled.is_(True)))
    ).scalars()
    out: dict[str, list[Rule]] = {}
    for r in rows:
        src = str(r.source_tenant_id).lower()
        out.setdefault(src, []).append(
            Rule(
                id=r.id,
                source_tenant_id=src,
                source_subject_id=r.source_subject_id,
                match_project=r.match_project,
                bill_tenant_id=str(r.bill_tenant_id).lower(),
                actor_id=r.actor_id,
            )
        )
    return {k: tuple(v) for k, v in out.items()}


async def get_rules_map(session: AsyncSession, *, now: float | None = None) -> RulesMap:
    """Enabled rules keyed by source tenant, TTL-cached per process."""
    global _cached, _cached_at
    stamp = time.monotonic() if now is None else now
    if _cached is not None and (stamp - _cached_at) < _RULES_TTL_SECONDS:
        return _cached
    _cached = await _build_rules_map(session)
    _cached_at = stamp
    return _cached


def has_real_project(project_id: Any) -> bool:
    """A resolved project that is an actual project: not NULL, not the placeholder."""
    if project_id is None:
        return False
    try:
        return uuid.UUID(str(project_id)) != NULL_PROJECT_ID
    except TypeError, ValueError:
        return False


def select_rule(
    rules_map: RulesMap,
    *,
    tenant_id: str | None,
    subject_id: uuid.UUID | None,
    project_id: Any,
) -> Rule | None:
    """The winning rule for one event, or ``None``. Pure.

    Precedence (highest first): subject-specific + project-specific,
    subject-specific + ``any``, tenant-wide + project-specific, tenant-wide +
    ``any``. ``with_project`` and ``without_project`` are mutually exclusive for
    one event, and the unique index allows one rule per (tenant, subject-or-any,
    match_project), so the order is total.
    """
    if not tenant_id:
        return None
    candidates = rules_map.get(str(tenant_id).lower())
    if not candidates:
        return None
    with_project = has_real_project(project_id)
    best: Rule | None = None
    best_score = -1
    for rule in candidates:
        if rule.source_subject_id is not None and rule.source_subject_id != subject_id:
            continue
        if rule.match_project == "with_project":
            if not with_project:
                continue
        elif rule.match_project == "without_project":
            if with_project:
                continue
        elif rule.match_project != "any":
            continue
        score = (2 if rule.source_subject_id is not None else 0) + (
            1 if rule.match_project != "any" else 0
        )
        if score > best_score:
            best, best_score = rule, score
    return best


def apply_rule(row: dict[str, Any], rule: Rule) -> None:
    """Re-attribute one cost-event row dict in place.

    ``tenant_id := bill_tenant_id`` (a no-op for a member-only rule); ``actor_id := COALESCE(actor_id,
    rule.actor_id)``; the original tenant and the rule id are recorded in
    ``metadata``. ``tenant_name`` is left to the caller (it needs the registry).
    """
    meta = dict(row.get("metadata") or {})
    if rule.bill_tenant_id != row["tenant_id"]:
        # Only a real move is recorded as one: a member-only rule (source ==
        # bill tenant) must not make the row read as re-tenanted.
        meta[ATTRIBUTED_FROM_KEY] = str(row["tenant_id"])
        row["tenant_id"] = rule.bill_tenant_id
    meta[ATTRIBUTION_RULE_ID_KEY] = str(rule.id)
    row["metadata"] = meta
    if row.get("actor_id") is None and rule.actor_id is not None:
        row["actor_id"] = rule.actor_id


# --- Admin CRUD (routers/config.py: /api/config/attribution-rules) ----------


def rule_to_dict(row: AttributionRule) -> dict[str, Any]:
    return {
        "id": row.id,
        "source_tenant_id": str(row.source_tenant_id),
        "source_subject_id": row.source_subject_id,
        "match_project": row.match_project,
        "bill_tenant_id": str(row.bill_tenant_id),
        "actor_id": row.actor_id,
        "reason": row.reason,
        "enabled": row.enabled,
        "created_at": row.created_at,
    }


def _audit_dict(row: AttributionRule) -> dict[str, Any]:
    d = rule_to_dict(row)
    return {k: (str(v) if v is not None and not isinstance(v, bool) else v) for k, v in d.items()}


async def list_rules(session: AsyncSession) -> list[AttributionRule]:
    stmt = select(AttributionRule).order_by(
        AttributionRule.source_tenant_id,
        AttributionRule.source_subject_id.is_(None),
        AttributionRule.match_project,
    )
    return list((await session.execute(stmt)).scalars().all())


async def upsert_rule(
    session: AsyncSession,
    *,
    source_tenant_id: str,
    source_subject_id: uuid.UUID | None,
    match_project: str,
    bill_tenant_id: str,
    actor_id: uuid.UUID | None,
    reason: str,
    enabled: bool,
) -> tuple[AttributionRule, dict[str, Any] | None]:
    """Insert or update the rule for one match tuple. Returns ``(row, before)``.

    The tuple is ``(source_tenant_id, source_subject_id, match_project)`` — the
    same key ``uq_attribution_rules_match`` enforces (NULL subject compared as a
    value, not as "unknown"). The caller commits.
    """
    subject_clause = (
        AttributionRule.source_subject_id.is_(None)
        if source_subject_id is None
        else AttributionRule.source_subject_id == source_subject_id
    )
    existing = (
        await session.execute(
            select(AttributionRule)
            .where(
                AttributionRule.source_tenant_id == source_tenant_id,
                subject_clause,
                AttributionRule.match_project == match_project,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    before = _audit_dict(existing) if existing is not None else None
    if existing is None:
        existing = AttributionRule(
            source_tenant_id=source_tenant_id,
            source_subject_id=source_subject_id,
            match_project=match_project,
            bill_tenant_id=bill_tenant_id,
            actor_id=actor_id,
            reason=reason,
            enabled=enabled,
        )
        session.add(existing)
    else:
        existing.bill_tenant_id = bill_tenant_id
        existing.actor_id = actor_id
        existing.reason = reason
        existing.enabled = enabled
    await session.flush()
    await session.refresh(existing)
    return existing, before


async def delete_rule(session: AsyncSession, rule_id: uuid.UUID) -> dict[str, Any] | None:
    """Delete one rule by id. Returns its audit dict, or ``None`` if absent."""
    row = await session.get(AttributionRule, rule_id)
    if row is None:
        return None
    before = _audit_dict(row)
    await session.delete(row)
    await session.flush()
    return before


def audit_dict(row: AttributionRule) -> dict[str, Any]:
    return _audit_dict(row)
