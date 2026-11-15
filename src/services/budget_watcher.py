"""Budget overage watcher (T1.6).

Periodic task that scans active budgets, detects 50/80/100% threshold
crossings, and fires HMAC-signed webhooks to tenant-registered URLs.

Idempotency: each (budget_id, threshold_pct, period_start, target_key) is
dispatched at most once via the ``budget_overage_alerts`` UNIQUE constraint —
note the key is per DESTINATION, so a crossing that reached one webhook but
timed out on another retries only the one that failed. Even if the watcher races
on multiple replicas (it shouldn't — it runs under the T1.2 leader-election
``async`` lock), the DB enforces single-fire.

Outbound payload:

    POST <webhook.url>
    Content-Type: application/json
    X-Metering-Signature: sha256=<hex(HMAC-SHA256(secret, body))>

    {
        "event": "budget.threshold_crossed",
        "tenant_id": "<id>",
        "budget_id": "<uuid>",
        "threshold_pct": 80,
        "period_start": "2026-05-01T00:00:00+00:00",
        "budget_tokens": 1000000,
        "consumed_tokens": 832145,
        "usage_pct": 83.21,
        "fired_at": "2026-05-04T03:55:00+00:00"
    }
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import BudgetOverageAlert, MoneyBudget, TenantWebhook, TokenBudget
from observability import metrics as observability_metrics
from services.budget_period import effective_consumed, period_anchor
from services.money_budget import effective_money_consumed

logger = structlog.stdlib.get_logger(__name__)

# Threshold percentages, in ascending order. Each crossed-but-not-yet-
# dispatched threshold fires once per period.
THRESHOLDS: tuple[int, ...] = (50, 80, 100)


class _Dispatchable(Protocol):
    """The only surface _dispatch_one reads off a webhook — lets the platform
    sink stand in for a persisted TenantWebhook without being an ORM row.

    Read-only members: _dispatch_one never assigns them, and a settable
    protocol attribute would reject the frozen _PlatformWebhook."""

    @property
    def url(self) -> str: ...

    @property
    def secret(self) -> str: ...


@dataclass(frozen=True)
class _PlatformWebhook:
    """A configured platform-wide sink (env-driven), fanned to on every tenant's
    crossing alongside that tenant's own tenant_webhooks rows."""

    url: str
    secret: str


def _platform_webhook_from(settings: Any) -> _PlatformWebhook | None:
    """Build the platform sink from settings, or None when not fully configured
    (both url AND secret required — a url without a secret is inert)."""
    url = (getattr(settings, "platform_budget_webhook_url", "") or "").strip()
    secret = (getattr(settings, "platform_budget_webhook_secret", "") or "").strip()
    if url and secret:
        return _PlatformWebhook(url=url, secret=secret)
    return None


# Dedupe key for a pre-0005 row: covers the ENTIRE crossing (all destinations).
LEGACY_TARGET = ""


def target_key_for(url: str) -> str:
    """Stable per-destination dedupe key: SHA-256 hex of the URL.

    Hashed rather than stored raw so the key is bounded (varchar(64)), survives a
    webhook row being deleted and recreated with the same URL, and so webhook
    URLs — which can carry tokens in query params — are not persisted into a
    long-lived audit table.
    """
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def sign_payload(secret: str, body: bytes) -> str:
    """HMAC-SHA256 signature in the form expected by the X-Metering-Signature header."""
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _period_start_for(budget: TokenBudget, ts: datetime) -> datetime:
    """The period ``ts`` falls in, for this budget's period type.

    ``ts`` is threaded in rather than read here so that ONE clock decides both
    the usage figure and the dedupe key for a given sweep. Read separately, the
    two could straddle a period roll: a crossing computed from the OLD period's
    consumption would be stamped with the NEW period, taking that period's
    dedupe slot and suppressing the real crossing when it arrived. The window is
    only the gap between two calls and there is no evidence it ever fired --
    this is single-clock discipline, not an incident fix.
    """
    # Deliberately the SAME rule as before -- monthly anchors to the month,
    # everything else to the ISO week. Only the clock changed, not the mapping;
    # widening it (e.g. letting a lifetime budget anchor to the day) would be a
    # behaviour change riding along with a discipline fix.
    d = period_anchor("monthly" if budget.period == "monthly" else "weekly", ts)
    assert d is not None  # both literals always resolve
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def _crossed_thresholds(usage_pct: float) -> list[int]:
    return [t for t in THRESHOLDS if usage_pct >= t]


async def _fetch_active_webhooks(session: AsyncSession, tenant_id: str) -> list[TenantWebhook]:
    stmt = select(TenantWebhook).where(
        TenantWebhook.tenant_id == tenant_id,
        TenantWebhook.active.is_(True),
    )
    return list((await session.execute(stmt)).scalars().all())


async def _dispatch_one(
    client: httpx.AsyncClient,
    webhook: _Dispatchable,
    payload: dict[str, Any],
) -> str:
    """POST a single webhook. Returns an outcome tag for the dedupe row."""
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = sign_payload(webhook.secret, body)
    try:
        resp = await client.post(
            webhook.url,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Metering-Signature": sig,
            },
            timeout=10.0,
        )
        if 200 <= resp.status_code < 300:
            return "ok"
        logger.warning(
            "webhook_http_error",
            tenant_id=payload["tenant_id"],
            url=webhook.url,
            status=resp.status_code,
        )
        return "http_error"
    except httpx.TimeoutException:
        logger.warning("webhook_timeout", tenant_id=payload["tenant_id"], url=webhook.url)
        return "timeout"
    except Exception:
        # Deliberately broad. httpx raises OUTSIDE HTTPError for some inputs —
        # measured: a stored url of "https://" (no host) raises a bare
        # ``ValueError: unknown url type: '/'``, which is neither HTTPError nor
        # httpx.InvalidURL. Anything escaping here propagates out of the sweep and
        # out of run_watcher_loop's while, permanently stopping budget alerting
        # for EVERY tenant until the pod restarts. One bad destination must never
        # sink the sweep, so every failure mode becomes a per-destination outcome
        # tag instead.
        logger.warning(
            "webhook_request_failed", tenant_id=payload["tenant_id"], url=webhook.url, exc_info=True
        )
        return "http_error"


async def _delivered_targets(
    session: AsyncSession,
    *,
    budget_id: Any,
    threshold_pct: int,
    period_start: datetime,
) -> set[str]:
    """The ``target_key``s already delivered for this crossing.

    A row is written ONLY on a successful delivery (``ok``) or when there is
    nothing to deliver (``no_webhooks``) — see the dispatch loop. Transient
    failures (``http_error`` / ``timeout``) deliberately leave NO row, so a
    destination missing from this set is re-dispatched on the next tick until it
    2xx's. That is what makes the watcher honour its "dispatched at most once
    per destination" contract: the rows gate DISPATCH, not just duplicate inserts.

    A ``LEGACY_TARGET`` ('') member means a pre-0005 row recorded the whole
    crossing as handled — callers must treat that as "every destination done"
    so enabling per-target dedupe does not replay history.
    """
    stmt = select(BudgetOverageAlert.target_key).where(
        BudgetOverageAlert.budget_id == budget_id,
        BudgetOverageAlert.threshold_pct == threshold_pct,
        BudgetOverageAlert.period_start == period_start,
    )
    return set((await session.execute(stmt)).scalars().all())


async def _try_record_alert(
    session: AsyncSession,
    *,
    tenant_id: str,
    budget_id: Any,
    threshold_pct: int,
    period_start: datetime,
    outcome: str,
    target_key: str,
) -> bool:
    """Insert the dedupe row. Returns True iff this is the first insert."""
    row = BudgetOverageAlert(
        tenant_id=tenant_id,
        budget_id=budget_id,
        threshold_pct=threshold_pct,
        period_start=period_start,
        outcome=outcome,
        target_key=target_key,
    )
    session.add(row)
    try:
        await session.commit()
        return True
    except IntegrityError:
        await session.rollback()
        return False


async def _dispatch_crossings(
    session_factory: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    platform_webhook: _Dispatchable | None,
    *,
    tenant_id: str,
    budget_id: Any,
    period_start: datetime,
    crossed: list[int],
    payload_base: dict[str, Any],
) -> int:
    """Fan one budget's crossed thresholds out to its destinations; returns new alerts.

    Shared by the token sweep and the EUR money sweep: both dedupe on the same
    ``budget_overage_alerts`` per-(budget, threshold, period, destination) key.
    """
    fired = 0
    for threshold in crossed:
        # Gate on the dedupe rows BEFORE dispatching, PER DESTINATION. A
        # row exists only for a destination that was delivered (ok) or a
        # crossing that had nothing to deliver (no_webhooks); a transient
        # failure leaves none. So this skip is what stops the watcher
        # re-POSTing an already-delivered destination every tick (the
        # pre-#39 code dispatched then recorded, deduping ROWS not
        # dispatches), while still retrying only the ones that failed.
        async with session_factory() as session:
            delivered = await _delivered_targets(
                session,
                budget_id=budget_id,
                threshold_pct=threshold,
                period_start=period_start,
            )
            # A legacy (pre-0005) row covers the whole crossing — never
            # replay history when per-target dedupe is introduced.
            if LEGACY_TARGET in delivered:
                continue
            tenant_webhooks = await _fetch_active_webhooks(session, tenant_id)

        webhooks: list[_Dispatchable] = list(tenant_webhooks)
        if platform_webhook is not None:
            webhooks.append(platform_webhook)

        if not webhooks:
            async with session_factory() as session:
                first = await _try_record_alert(
                    session,
                    tenant_id=tenant_id,
                    budget_id=budget_id,
                    threshold_pct=threshold,
                    period_start=period_start,
                    outcome="no_webhooks",
                    target_key=LEGACY_TARGET,
                )
            if first:
                fired += 1
            continue

        # Only the destinations that have not yet 2xx'd for this crossing.
        pending = [w for w in webhooks if target_key_for(w.url) not in delivered]
        if not pending:
            continue

        payload = {
            **payload_base,
            "threshold_pct": threshold,
            "fired_at": datetime.now(UTC).isoformat(),
        }
        for webhook in pending:
            outcome = await _dispatch_one(client, webhook, payload)
            # Record (→ stop dispatching to THIS destination) only on a
            # successful delivery. A transient failure records nothing for
            # it, so the next tick retries just that destination — delivery
            # survives a receiver that is briefly down, without re-POSTing
            # the destinations that already succeeded.
            if outcome != "ok":
                continue
            async with session_factory() as session:
                first = await _try_record_alert(
                    session,
                    tenant_id=tenant_id,
                    budget_id=budget_id,
                    threshold_pct=threshold,
                    period_start=period_start,
                    outcome=outcome,
                    target_key=target_key_for(webhook.url),
                )
            if first:
                fired += 1
    return fired


async def _sweep_money_budgets(
    session_factory: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    platform_webhook: _Dispatchable | None,
    *,
    now: datetime,
) -> int:
    """EUR money budgets (alembic 0044): same 50/80/100% thresholds, money payload.

    Payload: ``{event, tenant_id, budget_id, currency, amount, consumed, pct,
    threshold_pct, period_start, fired_at}``. ``amount > 0`` is the same
    load-bearing ZeroDivisionError guard as the token sweep's.
    """
    async with session_factory() as session:
        stmt = select(MoneyBudget).where(MoneyBudget.active.is_(True), MoneyBudget.amount > 0)
        budgets = list((await session.execute(stmt)).scalars().all())
    fired = 0
    for budget in budgets:
        consumed = effective_money_consumed(budget, now)
        pct = float(consumed / Decimal(budget.amount) * 100)
        crossed = _crossed_thresholds(pct)
        if not crossed:
            continue
        d = period_anchor("monthly" if budget.period == "monthly" else "weekly", now)
        assert d is not None
        period_start = datetime(d.year, d.month, d.day, tzinfo=UTC)
        fired += await _dispatch_crossings(
            session_factory,
            client,
            platform_webhook,
            tenant_id=budget.tenant_id,
            budget_id=budget.id,
            period_start=period_start,
            crossed=crossed,
            payload_base={
                "event": "budget.money_threshold_crossed",
                "tenant_id": budget.tenant_id,
                "budget_id": str(budget.id),
                "period_start": period_start.isoformat(),
                "currency": budget.currency,
                "amount": int(budget.amount),
                "consumed": str(consumed),
                "pct": round(pct, 2),
            },
        )
    return fired


async def check_and_fire_overages(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    http_client: httpx.AsyncClient | None = None,
    platform_webhook: _Dispatchable | None = None,
) -> int:
    """One full sweep — returns the number of newly-dispatched alerts.

    ``platform_webhook``, when set, is dispatched to on every tenant's crossing
    in addition to that tenant's own registered webhooks — a single sink that
    covers all tenants (incl. those with no tenant_webhooks rows).
    """
    fired = 0
    # ONE clock for the whole sweep — see _period_start_for.
    now = datetime.now(UTC)
    own_client = http_client is None
    client = http_client or httpx.AsyncClient(timeout=10.0)
    try:
        async with session_factory() as session:
            # ⚠️ ``budget_tokens > 0`` is a LOAD-BEARING ZeroDivisionError
            # guard, not an optimisation. Do not relax it to ``>= 0`` to "also
            # alert on blocked budgets": the sweep below divides by
            # ``budget.budget_tokens`` to compute usage_pct, so a single 0-cap
            # row reaching that line raises inside ``check_and_fire_overages``
            # and costs the whole cycle — every tenant's alerting, not just
            # that budget's.
            #
            # It used to be merely an optimisation (a 0 cap was unrepresentable:
            # domain ``Field(gt=0)``). Now that a 0 cap is a legitimate
            # block-all budget, active 0-cap rows genuinely exist and this
            # filter is the only thing keeping them out of the division.
            #
            # Skipping them is also the RIGHT behaviour: the thresholds are
            # 50/80/100% of an allowance, and a block-all budget has no
            # allowance to be a percentage of. There is nothing to warn about
            # approaching — it is already, and permanently, fully blocked.
            stmt = select(TokenBudget).where(
                TokenBudget.active.is_(True),
                TokenBudget.budget_tokens > 0,
            )
            budgets = list((await session.execute(stmt)).scalars().all())

        for budget in budgets:
            # consumed_tokens is Numeric(20,6) -> Decimal. Compute usage_pct from
            # the FULL precision value; int() is only applied to the reported
            # payload field, so a fractional remainder can't drag a genuine
            # threshold crossing just under the line.
            # Current-PERIOD consumption: a monthly budget that rolled over has
            # reset, so alerting on the raw lifetime column would re-fire (or
            # stay latched at 100%) forever after the first month it was burned.
            consumed_exact = Decimal(effective_consumed(budget, now))
            consumed = int(consumed_exact)
            usage_pct = float(consumed_exact / Decimal(budget.budget_tokens) * 100)
            crossed = _crossed_thresholds(usage_pct)
            if not crossed:
                continue

            period_start = _period_start_for(budget, now)
            payload_base: dict[str, Any] = {
                "event": "budget.threshold_crossed",
                "tenant_id": budget.tenant_id,
                "budget_id": str(budget.id),
                "period_start": period_start.isoformat(),
                "budget_tokens": budget.budget_tokens,
                "consumed_tokens": consumed,
                "usage_pct": round(usage_pct, 2),
            }

            fired += await _dispatch_crossings(
                session_factory,
                client,
                platform_webhook,
                tenant_id=budget.tenant_id,
                budget_id=budget.id,
                period_start=period_start,
                crossed=crossed,
                payload_base=payload_base,
            )

        fired += await _sweep_money_budgets(session_factory, client, platform_webhook, now=now)
    finally:
        if own_client:
            await client.aclose()

    if fired:
        logger.info("budget_overage_alerts_fired", count=fired)
    return fired


def _clear_leader_gauge() -> None:
    """Demotion hook for :class:`~services.leader_election.AsyncLeaderElection`.

    ``leader_election`` does not own the ``billing_*_is_leader`` gauges — the loops
    do — so it cannot publish a demotion itself. Without this hook the gauge is
    only rewritten once per tick, so a demoted replica keeps publishing 1 for up to
    a whole ``budget_watcher_interval_seconds`` while its successor publishes 1 too:
    ``sum()`` reads 2 and split-brain detection sees a phantom.

    A module-level function rather than a lambda so it is greppable and directly
    testable, and so the wiring is identical in all four leader-elected loops.
    """
    observability_metrics.budget_watcher_is_leader.set(0)


async def run_watcher_loop(
    settings: Any,
    session_factory: async_sessionmaker[AsyncSession],
    stop_event: asyncio.Event,
) -> None:
    """Leader-elected background loop for budget overage webhooks.

    Uses its own advisory lock (``WATCHER_LOCK_KEY``) so it can run on a
    different replica from the rollup loop. ``settings.budget_watcher_interval_seconds``
    controls the LEADER's cadence (default 60s).

    A FOLLOWER instead waits ``settings.leader_retry_interval_seconds`` (default
    15s) before re-racing for the lock — same roll-window fix as the rollup loop
    (see :func:`services.aggregator.run_rollup_loop`): a follower that idled its
    full interval left the overage watcher leaderless for up to that long after
    anything dropped the leader's session. ``is_leader()`` fails closed, so the
    error path is the follower path and takes the same short retry.

    ``billing_budget_watcher_is_leader`` is published on BOTH branches every tick
    (a follower that never writes is indistinguishable from a dead pod, so
    ``sum()==0`` could not mean "nobody watching"), and dropped to 0 the instant
    leadership is lost via the ``on_demote`` hook rather than a tick later.
    """
    from services.leader_election import (
        WATCHER_LOCK_KEY,
        AsyncLeaderElection,
    )

    # Validated at boot in main.lifespan — see Settings.resolve_leader_election_dsn.
    direct_dsn = settings.resolve_leader_election_dsn()
    election = AsyncLeaderElection(
        direct_dsn,
        WATCHER_LOCK_KEY,
        loop="budget_watcher",
        on_demote=_clear_leader_gauge,
    )
    interval = getattr(settings, "budget_watcher_interval_seconds", 60)
    # Clamped to the watcher interval: a follower must never wait LONGER than the
    # leader's own cadence.
    retry_interval = min(getattr(settings, "leader_retry_interval_seconds", 15.0), interval)

    platform_webhook = _platform_webhook_from(settings)
    if platform_webhook is not None:
        logger.info("budget_watcher_platform_sink_enabled", url=platform_webhook.url)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            while not stop_event.is_set():
                # Fail-safe default: a cycle that raised has not proven leadership
                # → short retry, not a full interval on the bench.
                is_leader = False
                try:
                    try:
                        is_leader = await election.is_leader()
                    finally:
                        # try/finally, not a plain call after the await: a raise
                        # inside is_leader() must not leave the PREVIOUS tick's 1
                        # standing. A replica that cannot tell whether it is leader
                        # publishes 0 — same fail-closed rule as the wait interval
                        # below, and the same shape as the rollup and invoice-close
                        # loops.
                        observability_metrics.budget_watcher_is_leader.set(1 if is_leader else 0)
                    if is_leader:
                        await check_and_fire_overages(
                            session_factory,
                            http_client=client,
                            platform_webhook=platform_webhook,
                        )
                except asyncio.CancelledError:
                    break
                except SQLAlchemyError:
                    logger.warning("budget_watcher_db_error", exc_info=True)
                except Exception:
                    # Deliberately broad, and it must stay that way. Previously
                    # only SQLAlchemyError was caught, so ANY other exception
                    # (a bad webhook URL, a serialization bug, a KeyError)
                    # escaped this while-loop, ran the finally: release() and
                    # left budget alerting dead fleet-wide until the pod
                    # restarted — the follower re-hitting it on promotion. A
                    # single bad sweep must cost one cycle, not the watcher.
                    logger.error("budget_watcher_sweep_failed", exc_info=True)
                try:
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=interval if is_leader else retry_interval
                    )
                except TimeoutError:
                    continue
    finally:
        # release() fires on_demote, which already zeroes the gauge — set it again
        # explicitly so this loop's shutdown contract does not depend on the
        # election's internals, matching run_reconcile_loop. A pod that exits its
        # watcher loop must never leave a 1 exported for the rest of the process's
        # life (prometheus_client keeps serving the last value written).
        await election.release()
        observability_metrics.budget_watcher_is_leader.set(0)
