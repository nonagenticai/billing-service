"""Cascade debit — drain stacked token budgets in expiry/priority order.

A tenant can hold multiple concurrent active budgets (e.g. baseline monthly +
top-up). On debit they drain in cascade order:

    ORDER BY (expires_at ASC NULLS LAST, priority ASC, starts_at ASC)

i.e. soonest-to-expire first; ties broken by lowest priority; ties by
earliest-created. When one budget exhausts, the next picks up the remaining
tokens of the same call. Oversold (more tokens than total budget) is recorded
on the last budget visited and surfaced via a structured-log entry — the
request is never failed by metering on this path.

``_select_active_budgets`` is the shared cascade-ordered query used here and by
the status and reservation read paths.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import redis.asyncio as aioredis
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import NULL_LICENSE_ID, TokenBudget
from services.budget_period import is_stale_period, period_anchor

logger = structlog.stdlib.get_logger(__name__)


@dataclass(frozen=True)
class BudgetDebit:
    """One step of a cascade debit — how much was taken from a single budget."""

    budget_id: uuid.UUID
    debited_tokens: int
    remaining_after: int
    #: Tokens on this step that the budget had NO room for. Non-zero only on the
    #: overflow step appended below, where consumed_tokens is pushed past
    #: budget_tokens on purpose. The flusher reads it to emit
    #: ``billing_budget_debits_total{outcome="oversold"}`` -- the series two
    #: Grafana panels chart and this module's own comment promises an alert on,
    #: which nothing has ever emitted (measured 2026-08-25: 739 oversold
    #: warnings in 2h, all reported to Prometheus as ``outcome="ok"``).
    oversold_tokens: int = 0


def split_debit_outcomes(debits: list[BudgetDebit]) -> tuple[int, int]:
    """Split cascade steps into ``(ok, oversold)`` for the debit-outcome metric.

    An overflow step is NOT an ``ok`` debit — it is the cap failing to cap, and
    counting it as ok is what left ``billing_budget_debits_total{outcome=
    "oversold"}`` a series that has never existed while two Grafana panels chart
    it. Kept here, next to the code that creates the overflow step, so the two
    definitions of "oversold" cannot drift apart.
    """
    oversold = sum(1 for d in debits if d.oversold_tokens > 0)
    return len(debits) - oversold, oversold


async def _select_active_budgets(
    session: AsyncSession,
    tenant_id: str,
    ts: datetime,
    *,
    lock: bool,
    license_id: uuid.UUID | None = None,
) -> list[TokenBudget]:
    """Return active, currently-windowed budgets in cascade-drain order.

    Ordering:
        expires_at ASC NULLS LAST, priority ASC, starts_at ASC

    When `lock=True`, takes a row-level FOR UPDATE SKIP LOCKED so concurrent
    debits don't double-spend the same row.

    ``license_id`` — THE SCOPE OF THE SPEND, and the single place the rule is
    expressed so no caller can drift from it:

      * ``None``  → UNSCOPED. Every active budget of the tenant. This is what a
        read-only aggregate view wants (``check_budget_aggregate`` shows the
        whole pool) and what a reserve call that named no license asks for.
      * a real license UUID → that license's own budgets PLUS the tenant-level
        pool (``license_id = NULL_LICENSE_ID``), which every license shares.
      * :data:`NULL_LICENSE_ID` → the tenant-level pool ONLY. This is the
        license-LESS case (system/S2S spend) and it falls out of the same
        predicate: a license-less event's scope IS the tenant level, so it can
        never reach a license-attributed budget.

    The last bullet is the money invariant. ``CostDaily.license_attributed_clause``
    /``is_system_license`` already drop license-less rows from every
    license-keyed billing aggregate ("system/S2S cost is never attributed to a
    customer invoice line"); debiting a customer's license budget with that same
    spend contradicted it. Now both paths agree on what "license cost" means.

    Note the column is NOT NULL (application code substitutes
    ``NULL_LICENSE_ID`` for None), so an ``IN`` predicate is total — there is no
    SQL NULL to leak past it.
    """
    stmt = (
        select(TokenBudget)
        .where(
            TokenBudget.tenant_id == tenant_id,
            TokenBudget.active.is_(True),
            TokenBudget.starts_at <= ts,
            (TokenBudget.expires_at.is_(None)) | (TokenBudget.expires_at > ts),
        )
        .order_by(
            TokenBudget.expires_at.asc().nulls_last(),
            TokenBudget.priority.asc(),
            TokenBudget.starts_at.asc(),
        )
    )
    if license_id is not None:
        # {license_id, NULL_LICENSE_ID} — a set literal collapses to the single
        # tenant-level sentinel when license_id IS NULL_LICENSE_ID, which is
        # exactly the license-less scope.
        stmt = stmt.where(TokenBudget.license_id.in_({license_id, NULL_LICENSE_ID}))
    if lock:
        # Plain FOR UPDATE (no SKIP LOCKED): cascade-drain must walk the full
        # ordered set per call. SKIP LOCKED would let a concurrent debit
        # silently bypass a contended row, breaking total-debit correctness.
        # Concurrent debits queue briefly on the same lock; safe and serial.
        stmt = stmt.with_for_update()
    result = await session.execute(stmt)
    return list(result.scalars().all())


def _split_tokens_for_budget(
    budget: TokenBudget,
    in_tokens: int,
    out_tokens: int,
) -> int:
    """How many tokens this budget will accept, honoring includes_input/output."""
    accepted = 0
    if budget.includes_input:
        accepted += in_tokens
    if budget.includes_output:
        accepted += out_tokens
    return accepted


async def release_budget_reservations(
    redis_conn: aioredis.Redis | None,
    tenant_id: str,
    debits: list[BudgetDebit],
    *,
    handles: dict[str, int] | None = None,
) -> None:
    """Settle the pre-flight reservations a committed cascade debit consumed.

    Runs AFTER the debit committed. The ``consumed_tokens`` column is the one
    budget counter; Redis only holds the in-flight reservation pool, which this
    releases by ``sum(d.debited_tokens for d in debits)``. No-op without a Redis
    connection or with an empty debit list.

    Split out of :func:`debit_budget` so a caller that owns the transaction
    (``commit=False``) can run it itself after its commit.

    ``handles`` — WHICH RESERVATIONS these tokens settle, as
    ``{reservation_id: tokens}``. A MAP and not a scalar id, because of the shape
    of the only caller that can supply one: ``services.cost_flusher`` settles
    per-(tenant, licence) over a WHOLE FLUSH BATCH of N cost events, so one call
    to this function covers every reservation that batch touched.

    ``None``/empty is the legitimate "no handle was on the wire" case — the
    standalone ``commit=True`` path in :func:`debit_budget` never has one — and
    it makes ``release_reservation`` fall back to its anonymous per-tenant
    release.
    """
    if redis_conn is None or not debits:
        return
    redis_total_debited = sum(d.debited_tokens for d in debits)

    # SETTLE the pre-flight reservation these tokens were reserved against.
    # Imported lazily: services.budget_redis imports _select_active_budgets from
    # this module, so a top-level import here would be a cycle.
    from services.budget_redis import release_reservation

    # ⛔ ``handles`` IS A MAP AND CANNOT BE COLLAPSED TO A SCALAR — do not
    # "simplify" it to one reservation_id. THE RELEASE IS PER-BATCH, NOT
    # PER-EVENT. ``redis_total_debited`` above is already
    # ``sum(d.debited_tokens for d in debits)`` over a cascade whose input was
    # itself summed across EVERY cost event in one flush batch for one
    # (tenant, licence) group (``cost_flusher`` accumulates that into
    # ``license_io``). So this single call settles N reservations at once, and a
    # scalar handle has nowhere to map onto the N of them: the callee needs to
    # know how many tokens belong to EACH reservation to decrement each one's own
    # key instead of draining an anonymous per-tenant pool — which is the
    # over-release billing-service #178 measured at 151.6/hour.
    #
    # ``tokens`` keeps its existing meaning and position (the batch total) so the
    # handle-less path is bit-for-bit unchanged; ``handles`` is additive.
    await release_reservation(redis_conn, tenant_id, redis_total_debited, handles=handles)


async def debit_budget(
    session: AsyncSession,
    redis_conn: aioredis.Redis | None,
    tenant_id: str,
    tokens_used: int,
    ts: datetime | None = None,
    *,
    in_tokens: int | None = None,
    out_tokens: int | None = None,
    license_id: uuid.UUID | None = None,
    commit: bool = True,
) -> list[BudgetDebit]:
    """Cascade-debit `tokens_used` across the tenant's active budgets.

    `in_tokens`/`out_tokens` allow per-budget honoring of
    `includes_input`/`includes_output` flags. If only `tokens_used` is given,
    we assume all of it counts for every budget (legacy behavior).

    ``license_id`` scopes WHICH budgets this spend may drain — see
    :func:`_select_active_budgets`. Callers that know the license the cost was
    incurred under (the cost-event flush does) MUST pass it: without it the
    cascade drains every budget of the tenant, so one license's traffic silently
    consumes another license's paid allowance. Pass :data:`NULL_LICENSE_ID` for
    genuinely license-less system/S2S spend (tenant-level pool only). ``None``
    means "unscoped" and is kept only for standalone/administrative callers that
    really do mean the whole pool.

    Algorithm:
      1. SELECT FOR UPDATE on cascade-ordered active budgets.
      2. Walk each in order, taking min(remaining_in_budget, tokens_left).
      3. Persist `consumed_tokens += debited` on the row.
      4. If `tokens_left > 0` after exhausting all budgets: oversold. Record
         the overflow on the last budget visited (consumed_tokens > budget_tokens
         on that row); emit `budget_oversold` log; do not raise.

    Transaction boundary — `commit`:
      - ``commit=True`` (default): own the transaction. After staging the
        row mutations, ``session.commit()`` then settle reservations.
        This preserves every standalone/test caller unchanged.
      - ``commit=False``: the CALLER owns the transaction. Only
        ``session.flush()`` the staged mutations (so the SELECT FOR UPDATE +
        ``consumed_tokens`` writes participate in the caller's unit of work)
        and do NOT touch Redis. The returned `debits` roll back or commit with
        the caller, who must run :func:`release_budget_reservations` after it
        commits.

    Returns a list of `BudgetDebit` describing each step.
    """
    if ts is None:
        ts = datetime.now(UTC)
    if tokens_used <= 0:
        return []

    budgets = await _select_active_budgets(session, tenant_id, ts, lock=True, license_id=license_id)
    if not budgets:
        logger.info(
            "budget_debit_skipped",
            tenant_id=tenant_id,
            license_id=str(license_id) if license_id else None,
            reason="no_active_budgets",
            tokens_requested=tokens_used,
        )
        return []

    # PERIOD ROLL — run under the FOR UPDATE lock taken above, before anything
    # reads consumed_tokens. `period` used to be decoration: consumed_tokens only
    # ever grew, so a 'monthly' budget was a LIFETIME cap. Here the first debit of
    # a new period rebases the column to that period's spend and re-stamps the
    # anchor, durably and atomically (it commits/rolls back with this debit).
    # A concurrent reader that races ahead of the commit derives the same 0 from
    # services.budget_period.effective_consumed, so the gate never disagrees.
    for b in budgets:
        anchor = period_anchor(b.period, ts)
        if anchor is None:
            continue  # lifetime budget — never resets
        if is_stale_period(b, ts):
            b.consumed_tokens = Decimal(0)
        if b.consumed_period_start != anchor:
            b.consumed_period_start = anchor

    def _step(budget: TokenBudget, current: int, take: int, *, oversold: int = 0) -> BudgetDebit:
        """Build one cascade step.

        ``oversold`` is non-zero only for the overflow step appended after the
        cascade has run dry — it is what the flusher counts. BudgetDebit is
        frozen, so it has to come in through the constructor.
        """
        return BudgetDebit(
            budget_id=budget.id,
            debited_tokens=take,
            remaining_after=max(0, budget.budget_tokens - (current + take)),
            oversold_tokens=oversold,
        )

    has_io_split = in_tokens is not None and out_tokens is not None

    debits: list[BudgetDebit] = []
    last_budget: TokenBudget = budgets[-1]

    if has_io_split:
        # Per-budget honoring of includes_input/output. The "tokens_left" we
        # owe each budget is computed per-flag; but the *overall* call-level
        # tokens_used is what we walk down. We approximate by taking the
        # budget's accepted slice from the call and draining that slice.
        in_left = in_tokens or 0
        out_left = out_tokens or 0
        for b in budgets:
            if in_left <= 0 and out_left <= 0:
                break
            this_in = in_left if b.includes_input else 0
            this_out = out_left if b.includes_output else 0
            wants = this_in + this_out
            if wants <= 0:
                continue
            current = int(b.consumed_tokens or 0)
            room = max(0, b.budget_tokens - current)
            take = min(room, wants)
            if take > 0:
                # Apportion the take back to the in/out buckets in proportion.
                if wants > 0 and (this_in + this_out) > 0:
                    in_take = min(this_in, take * this_in // wants if wants else 0)
                    out_take = take - in_take
                else:
                    in_take = 0
                    out_take = 0
                in_left -= in_take
                out_left -= out_take
                b.consumed_tokens = Decimal(current + take)
                debits.append(_step(b, current, take))
            last_budget = b
        # Anything left after walking every budget is overspend. Attribute each
        # token TYPE's overflow to the last budget that ACCEPTS that type — the
        # one that would have taken it had it had room — and mark it oversold.
        # Crucially this is decided per-type against *any* budget in the cascade,
        # not against the final-iterated budget: an input-only budget that ran
        # out still owns the input overflow even if the last budget visited was
        # an output-only one. Only a type that NO budget accepts is genuinely
        # unbudgeted spend and must not inflate any budget's consumed_tokens.
        in_leftover = max(0, in_left)
        out_leftover = max(0, out_left)
        last_in_budget = next((b for b in reversed(budgets) if b.includes_input), None)
        last_out_budget = next((b for b in reversed(budgets) if b.includes_output), None)

        overflow_by_budget: dict[uuid.UUID, int] = {}
        unbudgeted = 0
        if in_leftover > 0:
            if last_in_budget is not None:
                overflow_by_budget[last_in_budget.id] = (
                    overflow_by_budget.get(last_in_budget.id, 0) + in_leftover
                )
            else:
                unbudgeted += in_leftover
        if out_leftover > 0:
            if last_out_budget is not None:
                overflow_by_budget[last_out_budget.id] = (
                    overflow_by_budget.get(last_out_budget.id, 0) + out_leftover
                )
            else:
                unbudgeted += out_leftover

        budgets_by_id = {b.id: b for b in budgets}
        for budget_id, oversold in overflow_by_budget.items():
            b = budgets_by_id[budget_id]
            current = int(b.consumed_tokens or 0)
            b.consumed_tokens = Decimal(current + oversold)
            debits.append(_step(b, current, oversold, oversold=oversold))
            logger.warning(
                "budget_oversold",
                tenant_id=tenant_id,
                overflow_tokens=oversold,
                last_budget_id=str(b.id),
            )
        if unbudgeted > 0:
            logger.info(
                "budget_unbudgeted_tokens",
                tenant_id=tenant_id,
                unbudgeted_tokens=unbudgeted,
                last_budget_id=str(last_budget.id),
            )
    else:
        tokens_left = tokens_used
        for b in budgets:
            if tokens_left <= 0:
                break
            current = int(b.consumed_tokens or 0)
            room = max(0, b.budget_tokens - current)
            take = min(room, tokens_left)
            if take > 0:
                tokens_left -= take
                b.consumed_tokens = Decimal(current + take)
                debits.append(_step(b, current, take))
            last_budget = b
        if tokens_left > 0:
            current = int(last_budget.consumed_tokens or 0)
            last_budget.consumed_tokens = Decimal(current + tokens_left)
            debits.append(_step(last_budget, current, tokens_left, oversold=tokens_left))
            logger.warning(
                "budget_oversold",
                tenant_id=tenant_id,
                overflow_tokens=tokens_left,
                last_budget_id=str(last_budget.id),
            )

    if commit:
        # Standalone caller: own the transaction, then settle reservations.
        await session.commit()
        await release_budget_reservations(redis_conn, tenant_id, debits)
    else:
        # Caller owns the transaction. Flush so the FOR UPDATE + consumed_tokens
        # writes join the caller's unit of work; the caller commits and then
        # runs release_budget_reservations. Redis is untouched here.
        await session.flush()

    return debits
