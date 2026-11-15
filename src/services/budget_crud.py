"""Token budget mutations — always insert / soft delete.

Stacked-budget semantics: ``create_budget`` inserts a fresh row every call;
multiple concurrent active budgets per tenant are expected. ``delete_budget``
soft-deletes (active=False). ``upsert_license_sync_budget`` is the one
reconcile-in-place path, keyed on (license, period, source='license_sync', active)
— the same key the partial unique index from alembic 0010 enforces.

All mutations FLUSH (not commit) — the caller owns the transaction boundary so
audit-log writes land in the same transaction as the mutation.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import NULL_LICENSE_ID, RequestQuotaConfig, TokenBudget
from models.domain import BudgetPeriod
from observability.metrics import (
    BUDGET_WRITE_SOURCES,
    budget_writes_refused_unregistered_tenant_total,
)
from settings import get_settings
from tenancy import redact_tenant_id, write_gate_refuses_tenant_after_refresh

logger = structlog.stdlib.get_logger(__name__)


class UnregisteredTenantError(RuntimeError):
    """A budget write named a tenant the licences registry does not have.

    Carries the offending id so the HTTP boundary can report it and the bulk
    reconcile can count it without re-deriving anything.
    """

    def __init__(self, tenant_id: str) -> None:
        super().__init__(f"tenant_id is not a registered tenant: {tenant_id}")
        self.tenant_id = tenant_id


async def _require_registered_tenant(tenant_id: str, *, source: str) -> None:
    """Refuse to create a payer row for an id no registry vouches for.

    ⛔ THE REFUSAL IS THE POINT, AND IT BELONGS HERE RATHER THAN AT THE ROUTE.
    There are four budget-write call sites (``routers/budget.py`` 341, 351, 435,
    811) and only two functions in this module that they reach. Putting the check
    on the route would mean four chances to forget it and would leave the bulk
    reconcile — the path that actually produced the bad rows — covered by whoever
    edited it last. One guard, both writers.

    ⚠️ It raises rather than returning a verdict BECAUSE the three single-write
    callers must fail the request, and the fourth must skip one licence and carry
    on. An exception is the only shape that lets the caller choose; a bool would
    have every caller re-implement the decision, and the bulk path would get it
    wrong in the direction that 500s the whole run (see the
    ``sync_from_licenses_skip_unparseable_license_id`` comment in
    ``routers/budget.py`` for the last time a bare ``ValueError`` from this module
    rolled back every other tenant's reconcile).

    The verdict itself lives in :func:`tenancy.write_gate_refuses_tenant`, which
    fails OPEN whenever the live registry overlay is stale or was never loaded —
    so a licences outage degrades to today's behaviour, never to a refusal.

    ⚠️ A REFUSAL IS RE-CHECKED LIVE FIRST
    (:func:`tenancy.write_gate_refuses_tenant_after_refresh`): a tenant created
    since the last 300s registry tick used to be refused for up to five minutes
    (prod, 2026-10-13). The re-check is rate-limited and fails closed.
    """
    settings = get_settings()
    if not settings.budget_write_requires_registered_tenant:
        return
    # A multiple of the refresh interval: one missed tick still permits a
    # refusal, a sustained outage stops permitting one. Derived, not invented,
    # so the two cannot drift apart in config.
    max_age = 3.0 * float(settings.tenant_registry_refresh_seconds)
    if await write_gate_refuses_tenant_after_refresh(tenant_id, max_overlay_age_s=max_age):
        # Counter BEFORE the raise: the log line alone is not a detector, and
        # this is the only trace an operator can alert on.
        #
        # CLAMPED, because `source` originates in `TokenBudgetCreate.source` --
        # a bare `str` on the request body. An unclamped label here would let a
        # caller mint an unbounded number of series through a public field.
        label = source if source in BUDGET_WRITE_SOURCES else "other"
        budget_writes_refused_unregistered_tenant_total.labels(source=label).inc()
        logger.warning(
            "budget_write_refused_unregistered_tenant",
            tenant_id=redact_tenant_id(tenant_id),
            source=source,
        )
        raise UnregisteredTenantError(tenant_id)


async def create_budget(
    tenant_id: str,
    budget_tokens: int,
    period: str = "monthly",
    license_id: uuid.UUID | None = None,
    includes_input: bool = True,
    includes_output: bool = True,
    *,
    priority: int = 100,
    starts_at: datetime | None = None,
    expires_at: datetime | None = None,
    source: str = "manual",
    session: AsyncSession | None = None,
) -> TokenBudget:
    """Insert a new TokenBudget row.

    Stacked-budget semantics: each call inserts a fresh row. Multiple
    concurrent active budgets per tenant are expected; cascade-drain order
    is determined by `(expires_at, priority, starts_at)`.

    The session is FLUSHED, not committed — the caller controls the
    transaction boundary so audit-log writes can land in the same
    transaction as the mutation.
    """
    if session is None:
        raise ValueError("session is required")

    await _require_registered_tenant(tenant_id, source=source)

    effective_license_id = license_id or NULL_LICENSE_ID
    effective_starts_at = starts_at or datetime.now(UTC)

    budget = TokenBudget(
        tenant_id=tenant_id,
        tenant_name=tenant_id,
        license_id=effective_license_id,
        budget_tokens=budget_tokens,
        period=period,
        includes_input=includes_input,
        includes_output=includes_output,
        active=True,
        priority=priority,
        starts_at=effective_starts_at,
        expires_at=expires_at,
        consumed_tokens=Decimal("0"),
        source=source,
    )
    session.add(budget)
    await session.flush()
    await session.refresh(budget)
    return budget


async def _select_active_license_sync_budget(
    session: AsyncSession,
    license_id: uuid.UUID,
    period: str,
) -> TokenBudget | None:
    """The active license_sync row for a licence+period, if any.

    This predicate is the Python mirror of the partial unique index
    ``uq_token_budgets_license_sync_active`` (alembic 0010). Keep them identical:
    if this SELECT is narrower than the index, the write path INSERTs into a
    constraint violation; if it is wider, the index rejects a row the code
    believed was new.
    """
    stmt = select(TokenBudget).where(
        TokenBudget.license_id == license_id,
        TokenBudget.period == period,
        TokenBudget.source == "license_sync",
        TokenBudget.active.is_(True),
    )
    return (await session.execute(stmt)).scalars().first()


async def upsert_license_sync_budget(
    session: AsyncSession,
    *,
    tenant_id: str,
    license_id: str,
    period: BudgetPeriod,
    budget_tokens: int,
    includes_input: bool = True,
    includes_output: bool = True,
) -> tuple[TokenBudget, bool]:
    """Idempotent upsert for the license-sync code path.

    The idempotency key is (license_id, period, source='license_sync', active) —
    LICENCE-keyed, deliberately NOT tenant-keyed. Stacking is preserved for
    source='manual' / 'topup'; only license_sync rows get reconciled in place.

    Behavior:
      - If an active source='license_sync' row exists for that key, update
        budget_tokens / includes_input / includes_output. consumed_tokens is
        NOT reset (usage history survives plan changes). Returns (row, False).
      - Otherwise insert a new row with source='license_sync',
        consumed_tokens=0, default priority, no expiry. Returns (row, True).

    A licence belongs to exactly one tenant, so if the row's tenant_id no longer
    matches the tenant the licences service reports, the licence MOVED and the
    row is corrected in place (tenant_name follows tenant_id, exactly as a fresh
    insert would set it). This key used to include tenant_id, which made a
    tenant change upstream miss the SELECT and INSERT a duplicate — the defect
    alembic 0010 repairs. Since 0010 also adds the partial unique index
    ``uq_token_budgets_license_sync_active`` on (license_id, period) WHERE
    source='license_sync' AND active, that same miss would now raise
    IntegrityError instead; ``routers/budget.py`` catches only
    ``httpx.RequestError``, so it would 500 the whole reconcile run and roll back
    every budget and quota write in it. The SELECT below MUST stay aligned with
    that index — one key, enforced in one place.

    The INSERT is wrapped in a SAVEPOINT so that losing a race to a concurrent
    writer (two reconcile runs, or issuance racing the sweep) degrades to
    "re-select and update" rather than poisoning the caller's transaction.

    The session is FLUSHED, not committed — the caller controls the transaction
    boundary so audit-log writes can land in the same transaction as the
    mutation.
    """
    await _require_registered_tenant(tenant_id, source="license_sync")

    lic_uuid = uuid.UUID(license_id)
    period_value = period.value if isinstance(period, BudgetPeriod) else period

    def _apply(row: TokenBudget) -> None:
        if row.tenant_id != tenant_id:
            # The licence moved tenants upstream. One licence, one tenant.
            row.tenant_id = tenant_id
            row.tenant_name = tenant_id
        row.budget_tokens = budget_tokens
        row.includes_input = includes_input
        row.includes_output = includes_output
        row.updated_at = datetime.now(UTC)

    existing = await _select_active_license_sync_budget(session, lic_uuid, period_value)
    if existing is not None:
        _apply(existing)
        await session.flush()
        await session.refresh(existing)
        return existing, False

    budget = TokenBudget(
        tenant_id=tenant_id,
        tenant_name=tenant_id,
        license_id=lic_uuid,
        budget_tokens=budget_tokens,
        period=period_value,
        includes_input=includes_input,
        includes_output=includes_output,
        active=True,
        priority=100,
        starts_at=datetime.now(UTC),
        expires_at=None,
        consumed_tokens=Decimal("0"),
        source="license_sync",
    )
    try:
        async with session.begin_nested():
            session.add(budget)
            await session.flush()
    except IntegrityError:
        # A concurrent writer inserted the row between our SELECT and INSERT.
        # The SAVEPOINT rollback leaves the caller's transaction usable, so
        # converge on the winner instead of propagating a 500.
        existing = await _select_active_license_sync_budget(session, lic_uuid, period_value)
        if existing is None:
            # Not our uniqueness key — do not swallow an unrelated violation.
            raise
        _apply(existing)
        await session.flush()
        await session.refresh(existing)
        return existing, False

    await session.refresh(budget)
    return budget, True


def _snapshot_budget(budget: TokenBudget) -> TokenBudget:
    """Detached copy of a budget's current column values, for audit `before`."""
    return TokenBudget(
        id=budget.id,
        tenant_id=budget.tenant_id,
        license_id=budget.license_id,
        budget_tokens=budget.budget_tokens,
        period=budget.period,
        includes_input=budget.includes_input,
        includes_output=budget.includes_output,
        active=budget.active,
        priority=budget.priority,
        starts_at=budget.starts_at,
        expires_at=budget.expires_at,
        consumed_tokens=budget.consumed_tokens,
        source=budget.source,
        created_at=budget.created_at,
        updated_at=budget.updated_at,
    )


class EmptyActiveSetError(RuntimeError):
    """Raised rather than deleting every mirror row on an empty active set."""


async def delete_absent_quota_configs(
    session: AsyncSession,
    *,
    active_license_ids: set[uuid.UUID],
) -> list[uuid.UUID]:
    """Delete ``request_quota_config`` rows whose licence is no longer active upstream.

    ``request_quota_config`` is a local MIRROR of the licence registry, written
    only by ``POST /api/budget/sync-from-licenses``. Nothing removed a row when a
    licence was revoked, expired, or deleted, so the mirror only ever grew stale.
    Measured on prod 2026-09-02: 183 rows, of which 26 were junk -- 18 whose
    ``license_id`` did not exist in the registry at all and 8 whose licence was
    ``expired`` or ``revoked``.

    That matters because the mirror is what the attribution resolver reads. Two
    rows for one ``(tenant_id, app_name)`` make the key ambiguous and it is
    DROPPED, so ``license_id`` -> NULL and the spend becomes unattributable --
    and a stale row can manufacture that collision against a live licence.

    DELETE-BY-ABSENCE, never truncate-and-reload. A full replace on a partial
    fetch would drop rows for licences that are simply not in this page and fail
    enforcement OPEN (``quota_check`` falls back to a CALLER-SUPPLIED
    ``quota_monthly`` when no row exists). FLUSH only -- the caller owns the
    transaction boundary.

    ⚠️ ``active_license_ids`` IS A KEEP-SET, AND SINCE MIGRATION 0025 IT IS NOT
    THE ACTIVE SET. The name is retained only because renaming a public kwarg
    would churn every call site; read it as "every licence the registry
    acknowledged this run, whatever its status".

    ``POST /api/budget/sync-from-licenses`` used to fetch ``?status=active``
    exclusively, so "absent from the set" and "not active" were the same fact and
    this function was the ONLY thing that ever removed a revoked licence's row.
    It now fetches every status and passes ``known_license_ids``, because those
    two facts have come apart: a REVOKED licence is acknowledged by the registry
    and its row is MARKED ``status='revoked'`` (``services.quota.
    mark_quota_config_status``) and KEPT, so the resolver excludes it without the
    limit being discarded. Only a licence the registry did not return at all is
    genuine junk, and that is what reaches the DELETE below.

    ⛔ Do NOT "restore" the active set here. Passing it would hard-delete every
    revoked licence's row -- 92 of 258 measured 2026-09-08 -- which is precisely
    the lossy behaviour 0025 exists to replace.

    Unlike :func:`deactivate_orphan_license_sync_budgets`, an empty
    ``active_license_ids`` is REFUSED rather than honoured. That asymmetry is
    deliberate: a soft-delete of a budget is recoverable and reversible, whereas
    this is a hard DELETE of the row that carries the enforcement limit. "The
    registry legitimately has zero active licences" and "the fetch silently came
    back empty" are indistinguishable here, and only one of them is survivable.

    Raises:
        EmptyActiveSetError: if ``active_license_ids`` is empty.
    """
    if not active_license_ids:
        raise EmptyActiveSetError(
            "refusing to delete every request_quota_config row on an empty active set"
        )

    stmt = select(RequestQuotaConfig).where(
        RequestQuotaConfig.license_id.not_in(active_license_ids)
    )
    rows = (await session.execute(stmt)).scalars().all()

    deleted: list[uuid.UUID] = []
    for row in rows:
        deleted.append(row.license_id)
        await session.delete(row)
    await session.flush()
    return deleted


async def deactivate_orphan_license_sync_budgets(
    session: AsyncSession,
    *,
    active_license_ids: set[uuid.UUID],
) -> list[TokenBudget]:
    """Soft-delete active ``source='license_sync'`` budgets whose license is no
    longer in the licenses-service active set.

    When a license is deactivated upstream it drops out of the active list, but
    its license_sync budget would otherwise stay ``active=True`` forever. This
    diffs the passed-in active set against the active license_sync rows and
    soft-deletes (active=False) the orphans, returning a prior-state snapshot of
    each (like :func:`delete_budget`) so the caller can audit it. FLUSH only —
    the caller owns the transaction boundary.

    SAFETY: the caller MUST invoke this only after a **fully successful** fetch of
    the active set — passing a partial/failed set would wrongly orphan live
    budgets. An empty ``active_license_ids`` is honoured (it legitimately means the
    licenses service returned zero active licenses on a 200), so the safety gate
    lives at the call site, not here.
    """
    stmt = select(TokenBudget).where(
        TokenBudget.source == "license_sync",
        TokenBudget.active.is_(True),
        TokenBudget.license_id.not_in(active_license_ids),
    )
    rows = (await session.execute(stmt)).scalars().all()

    now = datetime.now(UTC)
    priors: list[TokenBudget] = []
    for budget in rows:
        priors.append(_snapshot_budget(budget))
        budget.active = False
        budget.updated_at = now
    await session.flush()
    return priors


async def delete_budget(
    budget_id: uuid.UUID,
    session: AsyncSession,
) -> TokenBudget | None:
    """Soft-delete a budget (active=False).

    Returns the row as it looked **before** mutation so the caller can
    audit-log the prior state. Returns None if no row matches or the row
    is already inactive.

    The session is FLUSHED, not committed — the caller controls the
    transaction boundary so audit-log writes can land in the same
    transaction as the mutation.
    """
    stmt = select(TokenBudget).where(TokenBudget.id == budget_id)
    result = await session.execute(stmt)
    budget = result.scalar_one_or_none()
    if budget is None or not budget.active:
        return None

    # Snapshot prior state for the caller (audit-log will use this).
    prior = _snapshot_budget(budget)

    budget.active = False
    budget.updated_at = datetime.now(UTC)
    await session.flush()
    return prior
