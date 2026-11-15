"""Money budgets: pricing, FX, consumption, reserve and settlement (alembic 0044/0045).

Tenant inference provisioning, PLAN rev E §2.1 / §3.1 / §3.2 (ledger T-24), and
REV F (owner ruling 2026-10-02 late): multi-currency, EUR | USD | CAD.

THE CONSUMPTION RULE (§3.1), the one formula every path here applies::

    amount[C] = fx(USD->C @ event_time) x base      C = the tenant's budget currency
                (fx(USD->USD) = 1, no fx_rate row needed)
      setup 1: base = cost price   (provider_pricing rate_class='cost')
      setup 2: base = list x (1 + pct/100)
      setup 3: base = list x pct/100
    pct = the resolved MarkupRule for tenant x provider x model x byo

THE FLOW
--------
1. ``reserve_money`` — IR's pre-flight per hop. Prices the worst case
   (``estimated_input_tokens`` in + ``max_output_tokens`` out), then an atomic
   Redis Lua check ``consumed + in_flight + est <= amount`` and a hold under a
   minted handle (``reservation_id``). Mirrors the identified-reservation
   protocol in :mod:`services.budget_redis` (handle index + per-handle amounts +
   a bounded sweep of expired holds), on its own ``budget:money:{tenant}:*``
   keys so it can never touch a token pool.
2. There is NO commit call. Settlement is the cost event carrying
   ``reservation_id`` (§3.2): :func:`settle_money_batch` (called by
   ``services.cost_flusher`` inside the flush transaction) computes the
   consumption, writes it on the row, debits the pooled ``tenant_money_budget``
   FOR UPDATE, and the flusher releases the handles post-commit
   (:func:`release_money_reservations`). An unsettled hold expires at its TTL.

CURRENCY CHANGE (REV F)
-----------------------
:func:`change_money_currency` converts the budget row IN PLACE (same id) at the
cross rate ``rate(USD->to) / rate(USD->from)``: the REMAINING amount converts
ROUND_HALF_UP to minor units, the cap ROUND_HALF_UP to whole units, and
``consumed`` becomes the opening equivalent ``cap - remaining`` (negative =
opening credit) so the converted remaining is preserved exactly. It is REFUSED
(409 ``holds_in_flight``) while any reservation is open. The before-figures (original currency) are kept in a
``tenant_money_budget_adjustment`` row; status derives per-currency consumption
from those rows. Cost events already settled keep the currency they were
stamped with; an event that settles AFTER the change prices in the NEW currency.

IN-FLIGHT HOLDS across a currency change: the change is refused while any
unexpired hold exists. A reserve that races the change (it read the old
currency, its hold lands after the hold count) is still kept consistent: Redis
carries a currency marker (``budget:money:{t}:ccy``), the change moves it after
COMMIT (re-denominating any such straggler at the cross rate, rounded UP so a
worst case never shrinks, same reservation_id and TTL), and should that step
not run (Redis blip) the next reserve sees the marker disagree with the
authoritative DB currency and performs the same conversion before admitting --
the pool is never summed across two currencies. Such a straggler settles in the
NEW currency (settlement prices in the currency the budget holds at flush).

UNITS
-----
Redis holds money as INTEGER units of 1e-8 of the budget currency (the column precision), so the
Lua arithmetic is exact integer math: 90 million EUR still fits a double's 53-bit
mantissa exactly, far above any budget this service will carry.

⛔ BACK-DATE THE FIRST FX ROW. Settlement prices each event at ITS OWN
``event_ts``, and the FX lookup has no "nearest earlier" fallback before the
first row. So when money budgets are switched on, the FIRST ``fx_rate`` row
(``PUT /api/config/fx-rates``) must carry an ``effective_from`` at or before the
earliest event that can still settle (in practice: before the first money
tenant's first reservation, with margin for delayed/redelivered cost events).
An event older than the first row is stored with NULL consumption, is NOT
debited, and is counted as
``billing_money_unpriced_events_total{reason="before_first_fx_rate"}`` -- never
silently.

IDEMPOTENCY. ``reserve_money`` is idempotent per ``(inference_id, hop_index)``
while that hold is live: an IR retry gets the same ``reservation_id`` back.

⛔ A MISSING INPUT IS A REFUSAL, NEVER A GUESS: no price row -> ``price_missing``,
no FX row -> ``fx_rate_missing``, no markup rule for setup 2/3 ->
``markup_missing`` (quote) / ``price_missing`` (reserve, whose reason set is
closed by the contract).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any, cast

import structlog
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models.db import FxRate, MoneyBudget, MoneyBudgetAdjustment, TenantConfig
from observability.metrics import UNPRICED_BEFORE_FIRST_FX, money_unpriced_events_total
from services import markup as markup_svc
from services import pricing as pricing_svc
from services.budget_period import period_anchor
from services.budget_redis import _RELEASE_BY_HANDLE_LUA, _RESERVATION_SWEEP_LIMIT

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.stdlib.get_logger(__name__)

#: The currency of a refusal body with no budget to take one from, and of a
#: quote for a tenant with no budget when none is asked for.
DEFAULT_CURRENCY = "EUR"
#: The budget currencies (REV F). USD is the FX base: its rate is always 1.
SUPPORTED_CURRENCIES: tuple[str, ...] = ("EUR", "USD", "CAD")
FX_BASE = "USD"
#: How often reserve re-reads the budget when Redis holds turn out to be in a
#: different currency than the DB (a currency change racing the reserve).
_CURRENCY_RETRIES = 3
MONEY_SCALE = Decimal(100_000_000)
_Q8 = Decimal("0.00000001")
#: Thresholds reported by status and evaluated by the watcher (same as tokens).
MONEY_THRESHOLDS: tuple[int, ...] = (50, 80, 100)


class MoneyRefusal(Exception):  # noqa: N818 - a verdict, not an error
    """A priced figure could not be produced; ``reason`` is the contract literal."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class MoneyRedisUnavailable(Exception):  # noqa: N818
    """No Redis to hold a reservation in. The route answers 503 (billing outage)."""


class MoneyCurrencyMismatch(Exception):  # noqa: N818
    """A PUT named a currency other than the active budget's (409; never converts)."""

    def __init__(self, current: str, requested: str) -> None:
        super().__init__(f"{current} != {requested}")
        self.current = current
        self.requested = requested


def q8(value: Decimal) -> Decimal:
    return value.quantize(_Q8, rounding=ROUND_HALF_UP)


def fmt8(value: Decimal) -> str:
    return f"{q8(value):.8f}"


def _to_units(value: Decimal) -> int:
    return int(q8(value) * MONEY_SCALE)


def _from_units(units: int) -> Decimal:
    return q8(Decimal(units) / MONEY_SCALE)


# --- keys ------------------------------------------------------------------
# One hash tag ({tenant}) so the three keys share a Redis Cluster slot and a
# single EVAL may touch them -- the same reasoning as budget_redis's tagged keys.


def money_pool_key(tenant_id: str) -> str:
    return f"budget:money:{{{tenant_id}}}:reserved"


def money_index_key(tenant_id: str) -> str:
    return f"budget:money:{{{tenant_id}}}:resv"


def money_amounts_key(tenant_id: str) -> str:
    return f"budget:money:{{{tenant_id}}}:resv:amt"


def money_currency_key(tenant_id: str) -> str:
    """The currency the pool / amounts are denominated in (REV F)."""
    return f"budget:money:{{{tenant_id}}}:ccy"


def _keys(tenant_id: str) -> list[str]:
    return [money_pool_key(tenant_id), money_index_key(tenant_id), money_amounts_key(tenant_id)]


def money_idempotency_key(tenant_id: str, inference_id: str | None, hop_index: int) -> str:
    """``(inference_id, hop_index)`` -> the handle holding that hop's reservation.

    Keyed per HOP, not per inference: IR reserves once per hop under ONE
    inference_id, and hop 1 must not be answered with hop 0's hold. With no
    inference_id the key is still well-formed (same hash slot) but never read:
    the Lua script is told there is no idempotency key via an empty ARGV.
    """
    return f"budget:money:{{{tenant_id}}}:idem:{inference_id or ''}:{hop_index}"


# Sweep-check-and-reserve. The admission test is the token path's
# (`consumed + pool + req > cap` -> deny); the sweep reclaims expired holds
# first so the pool cannot grow from holds nobody settled. Handle minted in
# Python (determinism under replication). See budget_redis._RESERVE_HANDLE_LUA
# for the full reasoning; this is that script on the money keys.
#
# KEYS[1] pool (INT units), KEYS[2] index (ZSET handle->expiry), KEYS[3] amounts (HASH),
# KEYS[4] idempotency (STRING (inference_id, hop) -> handle, TTL'd with the hold)
# KEYS[5] currency marker (STRING, REV F)
# ARGV: consumed_units, req_units, cap_units, ttl, now, handle, sweep_limit, use_idem, currency
#
# Currency: when the marker exists and names another currency than the caller
# priced in, nothing is touched and {3, 0, marker} is returned -- the caller
# re-reads the budget and re-denominates (see module docstring). An absent
# marker is adopted: pre-REV-F holds were all EUR, and every EUR-era budget is
# EUR, so the first reserve after deploy names the pool correctly.
#
# Idempotency: a retry carrying the same (inference_id, hop_index) while its hold
# is still LIVE (in the index, unexpired) gets {2, pool, handle, amount} -- the
# existing hold, no second one. A settled/released or expired hold no longer
# answers, so a genuinely new attempt after that reserves afresh.
_MONEY_RESERVE_LUA = """
local consumed = tonumber(ARGV[1])
local req = tonumber(ARGV[2])
local cap = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local handle = ARGV[6]
local sweep_limit = tonumber(ARGV[7])
local use_idem = ARGV[8] == '1'
local ccy = ARGV[9]

local marker = redis.call('GET', KEYS[5])
if marker and marker ~= ccy then
  return {3, 0, marker}
end

local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, sweep_limit)
local reclaimed = 0
for i = 1, #expired do
  local amt = tonumber(redis.call('HGET', KEYS[3], expired[i]) or '0')
  if redis.call('HDEL', KEYS[3], expired[i]) == 1 then
    reclaimed = reclaimed + amt
  end
  redis.call('ZREM', KEYS[2], expired[i])
end
if reclaimed > 0 then
  if redis.call('DECRBY', KEYS[1], reclaimed) <= 0 then
    redis.call('DEL', KEYS[1])
  end
end

local pool = tonumber(redis.call('GET', KEYS[1]) or '0')
if pool < 0 then pool = 0 end
if use_idem then
  local prior = redis.call('GET', KEYS[4])
  if prior then
    local exp = redis.call('ZSCORE', KEYS[2], prior)
    if exp and tonumber(exp) > now then
      local amt = redis.call('HGET', KEYS[3], prior) or '0'
      return {2, pool, prior, amt}
    end
  end
end
if (consumed + pool + req) > cap then
  return {0, pool}
end
redis.call('ZADD', KEYS[2], now + ttl, handle)
redis.call('HSET', KEYS[3], handle, req)
redis.call('INCRBY', KEYS[1], req)
if use_idem then
  redis.call('SET', KEYS[4], handle, 'EX', ttl)
end
if not marker then
  redis.call('SET', KEYS[5], ccy)
end
for _, i in ipairs({1, 2, 3, 5}) do
  if redis.call('TTL', KEYS[i]) < ttl then
    redis.call('EXPIRE', KEYS[i], ttl)
  end
end
return {1, pool + req}
"""

# Re-denominate every hold of one tenant (REV F currency change).
# KEYS[1] pool, KEYS[2] index, KEYS[3] amounts, KEYS[4] currency marker
# ARGV: from_ccy, to_ccy, cross_rate (decimal string)
# Returns {1, n_converted} | {0, marker} when the marker names neither side
# (somebody else re-denominated to a third currency: the caller re-reads).
# A hold is multiplied and rounded UP (math.ceil): a worst case never shrinks.
# The pool is RESET to the sum of the converted amounts, which is also what the
# token path's invariant (pool == sum of live per-handle amounts) requires.
_MONEY_CONVERT_LUA = """
local marker = redis.call('GET', KEYS[4])
if marker == ARGV[2] then
  return {1, 0}
end
if marker and marker ~= ARGV[1] then
  return {0, marker}
end
local rate = tonumber(ARGV[3])
local flat = redis.call('HGETALL', KEYS[3])
local total = 0
local n = 0
for i = 1, #flat, 2 do
  local new = math.ceil(tonumber(flat[i + 1]) * rate)
  redis.call('HSET', KEYS[3], flat[i], new)
  total = total + new
  n = n + 1
end
local ttl = redis.call('TTL', KEYS[3])
if total > 0 then
  redis.call('SET', KEYS[1], total)
  if ttl > 0 then redis.call('EXPIRE', KEYS[1], ttl) end
else
  redis.call('DEL', KEYS[1])
end
redis.call('SET', KEYS[4], ARGV[2])
if ttl > 0 then redis.call('EXPIRE', KEYS[4], ttl) end
return {1, n}
"""


# --- reads -----------------------------------------------------------------


async def get_active_money_budget(
    session: AsyncSession, tenant_id: str, *, for_update: bool = False
) -> MoneyBudget | None:
    stmt = select(MoneyBudget).where(
        MoneyBudget.tenant_id == tenant_id, MoneyBudget.active.is_(True)
    )
    if for_update:
        stmt = stmt.with_for_update()
    return (await session.execute(stmt)).scalar_one_or_none()


def _consumption_anchor(budget: MoneyBudget, ts: datetime) -> date | None:
    if budget.consumed_period_start is not None:
        return budget.consumed_period_start
    created = budget.created_at or ts
    return period_anchor(budget.period, created)


def is_stale_money_period(budget: MoneyBudget, ts: datetime) -> bool:
    anchor = period_anchor(budget.period, ts)
    if anchor is None:
        return False
    stored = _consumption_anchor(budget, ts)
    return stored is not None and stored < anchor


def effective_money_consumed(budget: MoneyBudget, ts: datetime) -> Decimal:
    """Current-period EUR consumption: 0 once the period rolled over.

    Same anchoring rule as :func:`services.budget_period.effective_consumed`;
    every read of ``consumed`` must go through here.
    """
    if is_stale_money_period(budget, ts):
        return Decimal(0)
    return Decimal(budget.consumed or 0)


async def fx_rate_at(
    session: AsyncSession, ts: datetime, *, base: str = FX_BASE, quote: str = DEFAULT_CURRENCY
) -> Decimal | None:
    """USD -> ``quote`` effective at ``ts``; exactly 1 for USD (no row needed)."""
    if quote == base:
        return Decimal(1)
    stmt = (
        select(FxRate.rate)
        .where(
            FxRate.base == base,
            FxRate.quote == quote,
            FxRate.effective_from <= ts,
            or_(FxRate.effective_until.is_(None), FxRate.effective_until > ts),
        )
        .order_by(FxRate.effective_from.desc())
        .limit(1)
    )
    rate = (await session.execute(stmt)).scalar_one_or_none()
    return Decimal(rate) if rate is not None else None


async def oldest_fx_effective_from(
    session: AsyncSession, *, base: str = FX_BASE, quote: str = DEFAULT_CURRENCY
) -> datetime | None:
    """``effective_from`` of the FIRST FX row, or None when there is none at all."""
    if quote == base:
        return None
    return (
        await session.execute(
            select(func.min(FxRate.effective_from)).where(
                FxRate.base == base, FxRate.quote == quote
            )
        )
    ).scalar_one_or_none()


async def tenant_budget_units(session: AsyncSession, tenant_ids: set[str]) -> dict[str, str]:
    """``{tenant_id: budget_unit}`` for the tenants that HAVE a config row."""
    if not tenant_ids:
        return {}
    rows = await session.execute(
        select(TenantConfig.tenant_id, TenantConfig.budget_unit).where(
            TenantConfig.tenant_id.in_(sorted(tenant_ids))
        )
    )
    return {str(t): u for t, u in rows.all()}


# --- pricing ----------------------------------------------------------------


async def price_estimate(
    session: AsyncSession,
    *,
    setup: int,
    provider: str,
    model: str | None,
    input_tokens: int,
    output_tokens: int,
    ts: datetime,
    service_key: str | None = None,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
) -> Decimal | None:
    """USD base price: setup 1 = the COST row, setups 2/3 = the LIST row."""
    rate_class = "cost" if setup == 1 else "list"
    cost, _pricing_id = await pricing_svc.compute_cost_usd(
        session,
        provider=provider,
        served_model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens,
        service_key=service_key,
        rate_class=rate_class,
        ts=ts,
    )
    return cost


async def resolve_pct(
    session: AsyncSession,
    *,
    tenant_id: str,
    setup: int,
    provider: str,
    model: str | None,
    byo: bool,
    ts: datetime,
) -> Decimal | None:
    """Markup % for setups 2/3 (MarkupRule tenant x provider x model x byo); 0 for setup 1.

    ``None`` = no rule resolved at all (not even the global default).
    """
    if setup == 1:
        return Decimal(0)
    rule = await markup_svc.resolve_markup_rule(
        session,
        tenant_id=tenant_id,
        project_id=None,
        provider=provider,
        model=model,
        byo=byo,
        ts=ts,
    )
    if rule is None:
        return None
    return Decimal(rule.markup_pct if rule.markup_pct is not None else 0)


def consumption_usd(setup: int, cost: Decimal, pct: Decimal | None) -> Decimal:
    """The §3.1 base, in USD, before FX."""
    if setup == 1:
        return cost
    p = pct if pct is not None else Decimal(0)
    if setup == 2:
        return cost * (Decimal(1) + p / Decimal(100))
    if setup == 3:
        return cost * p / Decimal(100)
    raise ValueError(f"unknown llm setup {setup!r}")


def convert_usd(usd: Decimal, rate: Decimal) -> Decimal:
    """USD base -> the budget currency at ``rate`` (USD -> currency), 8 dp."""
    return q8(usd * rate)


async def cross_rate(
    session: AsyncSession, from_currency: str, to_currency: str, ts: datetime
) -> tuple[Decimal, Decimal, Decimal] | None:
    """``(from->to, USD->from, USD->to)`` at ``ts``; None when either leg is missing."""
    rate_from = await fx_rate_at(session, ts, quote=from_currency)
    rate_to = await fx_rate_at(session, ts, quote=to_currency)
    if rate_from is None or rate_to is None:
        return None
    return rate_to / rate_from, rate_from, rate_to


@dataclass(frozen=True)
class Priced:
    amount: Decimal
    currency: str
    usd_base: Decimal
    markup_pct: Decimal
    fx_rate: Decimal


async def price_in_currency(
    session: AsyncSession,
    *,
    currency: str,
    tenant_id: str,
    setup: int,
    provider: str,
    model: str | None,
    byo: bool,
    input_tokens: int,
    output_tokens: int,
    ts: datetime,
    service_key: str | None = None,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
) -> Priced:
    """Apply the consumption rule end to end, in ``currency``. Raises :class:`MoneyRefusal`."""
    cost = await price_estimate(
        session,
        setup=setup,
        provider=provider,
        model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        ts=ts,
        service_key=service_key,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens,
    )
    if cost is None:
        raise MoneyRefusal("price_missing")
    rate = await fx_rate_at(session, ts, quote=currency)
    if rate is None:
        raise MoneyRefusal("fx_rate_missing")
    pct = await resolve_pct(
        session, tenant_id=tenant_id, setup=setup, provider=provider, model=model, byo=byo, ts=ts
    )
    if pct is None:
        raise MoneyRefusal("markup_missing")
    base = consumption_usd(setup, cost, pct)
    return Priced(
        amount=convert_usd(base, rate),
        currency=currency,
        usd_base=base,
        markup_pct=pct,
        fx_rate=rate,
    )


# --- reserve / release -------------------------------------------------------


async def in_flight_amount(redis_conn: aioredis.Redis, tenant_id: str) -> Decimal:
    raw = await redis_conn.get(money_pool_key(tenant_id))
    return _from_units(max(int(raw or 0), 0))


def _response(
    *,
    currency: str,
    allowed: bool,
    reservation_id: str | None,
    reserved: Decimal,
    cap: int,
    consumed: Decimal,
    in_flight: Decimal,
    reason: str | None,
) -> dict[str, Any]:
    return {
        "allowed": allowed,
        "reservation_id": reservation_id,
        "currency": currency,
        "reserved_amount": fmt8(reserved),
        "cap_amount": str(int(cap)),
        "consumed_amount": fmt8(consumed),
        "in_flight_amount": fmt8(in_flight),
        "remaining_amount": fmt8(Decimal(cap) - consumed - in_flight),
        "reason": reason,
    }


async def snapshot_refusal(
    session: AsyncSession,
    redis_conn: aioredis.Redis | None,
    *,
    tenant_id: str,
    reason: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """A refusal body carrying the tenant's REAL numbers (never a fake cap of 0)."""
    now = now or datetime.now(UTC)
    budget = await get_active_money_budget(session, tenant_id)
    in_flight = Decimal(0)
    if redis_conn is not None:
        try:
            in_flight = await in_flight_amount(redis_conn, tenant_id)
        except Exception:  # noqa: BLE001 - a refusal body must not fail on Redis
            in_flight = Decimal(0)
    return _response(
        currency=budget.currency if budget is not None else DEFAULT_CURRENCY,
        allowed=False,
        reservation_id=None,
        reserved=Decimal(0),
        cap=budget.amount if budget is not None else 0,
        consumed=effective_money_consumed(budget, now) if budget is not None else Decimal(0),
        in_flight=in_flight,
        reason=reason,
    )


async def convert_money_holds(
    redis_conn: aioredis.Redis,
    tenant_id: str,
    *,
    from_currency: str,
    to_currency: str,
    rate: Decimal,
) -> int | None:
    """Re-denominate every live hold ``from -> to`` at ``rate``. Returns holds converted.

    None = the marker names a third currency (someone else converted; re-read).
    Idempotent: a marker already on ``to_currency`` converts nothing.
    """
    raw = await cast("Any", redis_conn.eval)(
        _MONEY_CONVERT_LUA,
        4,
        *_keys(tenant_id),
        money_currency_key(tenant_id),
        from_currency,
        to_currency,
        f"{rate:.18f}",
    )
    if int(raw[0]) != 1:
        return None
    return int(raw[1])


async def _reread_budget(session: AsyncSession, tenant_id: str) -> MoneyBudget | None:
    """The active budget as COMMITTED now (bypasses the identity map's stale copy)."""
    stmt = (
        select(MoneyBudget)
        .where(MoneyBudget.tenant_id == tenant_id, MoneyBudget.active.is_(True))
        .execution_options(populate_existing=True)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def reserve_money(
    session: AsyncSession,
    redis_conn: aioredis.Redis | None,
    *,
    tenant_id: str,
    setup: int,
    provider: str,
    model: str,
    byo: bool,
    input_tokens: int,
    output_tokens: int,
    ttl_seconds: int,
    inference_id: str | None = None,
    hop_index: int = 0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Price the worst case in the budget currency and hold it atomically (§3.1 body).

    ``no_active_budget`` is a HARD STOP (unlike the token path): a money tenant
    with no money budget must not spend.

    Idempotent per ``(inference_id, hop_index)``: a retry while that hop's hold
    is live returns the SAME ``reservation_id`` and amount and holds nothing
    more (otherwise every IR retry would double-hold until the TTL).

    Currency (REV F): the hold is taken in the budget's currency. If the Redis
    holds are denominated in another currency (a currency change raced this
    call, or its Redis step never ran), the budget is re-read and the holds are
    re-denominated to the DB's currency before admitting; after
    ``_CURRENCY_RETRIES`` disagreements the call is a 503 (billing outage).
    """
    now = now or datetime.now(UTC)
    budget = await get_active_money_budget(session, tenant_id)
    for _attempt in range(_CURRENCY_RETRIES):
        if budget is None:
            return await snapshot_refusal(
                session, redis_conn, tenant_id=tenant_id, reason="no_active_budget", now=now
            )
        currency = budget.currency
        cap = int(budget.amount)
        consumed = effective_money_consumed(budget, now)
        try:
            priced = await price_in_currency(
                session,
                currency=currency,
                tenant_id=tenant_id,
                setup=setup,
                provider=provider,
                model=model,
                byo=byo,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                ts=now,
            )
        except MoneyRefusal as refusal:
            # The reserve reason set is closed (§3.1); an unresolvable markup is an
            # unpriceable request.
            reason = "price_missing" if refusal.reason == "markup_missing" else refusal.reason
            return await snapshot_refusal(
                session, redis_conn, tenant_id=tenant_id, reason=reason, now=now
            )
        if redis_conn is None:
            raise MoneyRedisUnavailable
        handle = str(uuid.uuid4())
        raw = await cast("Any", redis_conn.eval)(
            _MONEY_RESERVE_LUA,
            5,
            *_keys(tenant_id),
            money_idempotency_key(tenant_id, inference_id, hop_index),
            money_currency_key(tenant_id),
            _to_units(consumed),
            _to_units(priced.amount),
            _to_units(Decimal(cap)),
            int(ttl_seconds),
            int(now.timestamp()),
            handle,
            _RESERVATION_SWEEP_LIMIT,
            "1" if inference_id else "0",
            currency,
        )
        ok, pool_units = int(raw[0]), int(raw[1])
        if ok == 3:
            marker = raw[2].decode() if isinstance(raw[2], bytes) else str(raw[2])
            budget = await _reread_budget(session, tenant_id)
            if budget is not None and budget.currency != marker:
                rates = await cross_rate(session, marker, budget.currency, now)
                if rates is None:
                    return await snapshot_refusal(
                        session, redis_conn, tenant_id=tenant_id, reason="fx_rate_missing", now=now
                    )
                converted = await convert_money_holds(
                    redis_conn,
                    tenant_id,
                    from_currency=marker,
                    to_currency=budget.currency,
                    rate=rates[0],
                )
                logger.warning(
                    "money_holds_redenominated_on_reserve",
                    tenant_id=tenant_id,
                    from_currency=marker,
                    to_currency=budget.currency,
                    holds=converted,
                )
            continue
        if ok == 2:
            prior = raw[2].decode() if isinstance(raw[2], bytes) else str(raw[2])
            return _response(
                currency=currency,
                allowed=True,
                reservation_id=prior,
                reserved=_from_units(int(raw[3])),
                cap=cap,
                consumed=consumed,
                in_flight=_from_units(pool_units),
                reason=None,
            )
        if not ok:
            return _response(
                currency=currency,
                allowed=False,
                reservation_id=None,
                reserved=Decimal(0),
                cap=cap,
                consumed=consumed,
                in_flight=_from_units(pool_units),
                reason="over_budget",
            )
        return _response(
            currency=currency,
            allowed=True,
            reservation_id=handle,
            reserved=priced.amount,
            cap=cap,
            consumed=consumed,
            in_flight=_from_units(pool_units),
            reason=None,
        )
    logger.error("money_reserve_currency_unsettled", tenant_id=tenant_id)
    raise MoneyRedisUnavailable


async def release_money_reservations(
    redis_conn: aioredis.Redis, tenant_id: str, handles: list[str]
) -> int:
    """Discharge exactly these holds (and nothing else). Returns holds found."""
    if not handles:
        return 0
    raw = await cast("Any", redis_conn.eval)(_RELEASE_BY_HANDLE_LUA, 3, *_keys(tenant_id), *handles)
    return int(raw[3])


# --- settlement (called from services.cost_flusher) --------------------------


def parse_llm_setup(usage_extra: dict | None) -> int | None:
    """``usage_extra.setup`` as 1/2/3, else None (absent or malformed = legacy path)."""
    if not usage_extra:
        return None
    raw = usage_extra.get("setup")
    # bool is an int subclass; True must not read as setup 1.
    if isinstance(raw, bool) or not isinstance(raw, int | str):
        return None
    text_value = str(raw).strip()
    return int(text_value) if text_value in ("1", "2", "3") else None


def _byo_flag(usage_extra: dict | None, setup: int) -> bool:
    raw = (usage_extra or {}).get("byo")
    if raw is None:
        return setup == 3
    return str(raw).strip().lower() in ("1", "true")


async def lock_money_budget_currencies(
    session: AsyncSession, tenant_ids: set[str]
) -> dict[str, str]:
    """``{tenant: budget currency}``, locking each active budget FOR UPDATE.

    Taken in canonical (sorted) order BEFORE settlement prices anything, so a
    concurrent currency change (which also locks the row) cannot land between
    "priced in EUR" and "debited a budget that is now USD".
    """
    out: dict[str, str] = {}
    for tid in sorted(tenant_ids):
        budget = await get_active_money_budget(session, tid, for_update=True)
        if budget is not None:
            out[tid] = budget.currency
    return out


async def price_row_consumption(
    session: AsyncSession,
    row: dict[str, Any],
    *,
    tenant_id: str,
    fallback_ts: datetime,
    currency: str = DEFAULT_CURRENCY,
) -> Decimal | None:
    """Compute and stamp the consumption (in ``currency``) on one cost row. None = unpriceable."""
    setup = int(row["llm_setup"])
    ts = row.get("event_ts") or fallback_ts
    try:
        priced = await price_in_currency(
            session,
            currency=currency,
            tenant_id=tenant_id,
            setup=setup,
            provider=row["provider"],
            model=row.get("served_model") or row.get("model"),
            byo=_byo_flag(row.get("usage_extra"), setup),
            input_tokens=row["input_tokens"],
            output_tokens=row["output_tokens"],
            ts=ts,
            service_key=row.get("service_key"),
            cache_creation_input_tokens=row.get("cache_creation_input_tokens") or 0,
            cache_read_input_tokens=row.get("cache_read_input_tokens") or 0,
        )
    except MoneyRefusal as refusal:
        # The spend already happened; refusing here would only destroy the
        # record. Store the row with NULL consumption, loudly AND counted.
        reason = refusal.reason
        if reason == "fx_rate_missing":
            oldest = await oldest_fx_effective_from(session, quote=currency)
            if oldest is not None and ts < oldest:
                # Not a gap in the FX table: the event predates the FIRST rate.
                # Named separately because the fix is different (back-date the
                # first row), and a NULL consumption must never be silent.
                reason = UNPRICED_BEFORE_FIRST_FX
        money_unpriced_events_total.labels(reason=reason).inc()
        logger.warning(
            "money_settlement_unpriced",
            tenant_id=tenant_id,
            provider=row["provider"],
            served_model=row.get("served_model"),
            setup=setup,
            currency=currency,
            reason=reason,
            event_ts=ts.isoformat() if isinstance(ts, datetime) else str(ts),
        )
        return None
    row["consumption_amount"] = priced.amount
    row["consumption_currency"] = priced.currency
    return priced.amount


async def debit_money_budget(
    session: AsyncSession, tenant_id: str, amount: Decimal, *, now: datetime
) -> MoneyBudget | None:
    """Debit the tenant's pooled money budget, FOR UPDATE, in the caller's transaction.

    Rebases a rolled-over period to 0 inside the lock (same rule as the token
    debit). Returns None when the tenant has no active money budget.
    """
    budget = await get_active_money_budget(session, tenant_id, for_update=True)
    if budget is None:
        logger.warning("money_debit_no_active_budget", tenant_id=tenant_id, amount=str(amount))
        return None
    anchor = period_anchor(budget.period, now)
    if is_stale_money_period(budget, now):
        budget.consumed = Decimal(0)
    if anchor is not None:
        budget.consumed_period_start = anchor
    budget.consumed = q8(Decimal(budget.consumed or 0) + amount)
    budget.updated_at = now
    await session.flush()
    return budget


# --- write / status ------------------------------------------------------------


async def put_money_budget(
    session: AsyncSession,
    tenant_id: str,
    *,
    amount: int,
    period: str,
    source: str,
    currency: str = DEFAULT_CURRENCY,
    now: datetime | None = None,
) -> tuple[MoneyBudget, dict[str, Any] | None]:
    """Idempotent upsert of the ONE active budget; flips budget_unit to 'money'.

    Returns ``(budget, before)`` where ``before`` is the prior state for audit.

    ⛔ Writes ONLY the columns it owns: the budget row, and ``budget_unit`` (+
    ``updated_at``) on ``tenant_config``. When it has to CREATE the config row it
    leaves ``markup_pct`` NULL -- "not configured" -- so
    :func:`services.billing.get_markup_pct_with_source` keeps falling through to
    the DB default / settings, exactly as for a tenant with no row. (Seeding the
    code default here would report a markup nobody chose as tenant-configured.)

    Currency: ``currency`` is used only when the row is CREATED. An existing
    budget in another currency raises :class:`MoneyCurrencyMismatch` (409) and
    changes nothing -- converting is :func:`change_money_currency`'s job, behind
    the owner role, never a side effect of a provisioning replay.

    Concurrency: both inserts are ``INSERT ... ON CONFLICT DO NOTHING`` against
    the unique keys (``uq_money_budget_active_tenant`` partial index,
    ``tenant_config.tenant_id``), then the row is re-read FOR UPDATE. Two
    simultaneous first PUTs therefore serialise on the row instead of one of
    them dying on a unique violation (a 500).
    """
    now = now or datetime.now(UTC)
    inserted_id = (
        await session.execute(
            pg_insert(MoneyBudget)
            .values(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                currency=currency,
                amount=amount,
                period=period,
                consumed=Decimal(0),
                consumed_period_start=period_anchor(period, now),
                active=True,
                source=source,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(
                index_elements=[MoneyBudget.tenant_id],
                index_where=text("active"),  # the partial index predicate, verbatim
            )
            .returning(MoneyBudget.id)
        )
    ).scalar_one_or_none()
    budget = await get_active_money_budget(session, tenant_id, for_update=True)
    if budget is None:  # pragma: no cover - the row was just inserted or already there
        raise RuntimeError(f"money budget for {tenant_id} vanished inside its own upsert")
    before: dict[str, Any] | None = None
    if inserted_id is None:
        if budget.currency != currency:
            raise MoneyCurrencyMismatch(budget.currency, currency)
        before = money_budget_audit_dict(budget)
        if (budget.amount, budget.period, budget.source) != (amount, period, source):
            budget.amount = amount
            budget.period = period
            budget.source = source
            budget.updated_at = now
    await session.execute(
        pg_insert(TenantConfig)
        .values(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            markup_pct=None,
            budget_unit="money",
            notes="budget_unit=money via PUT /api/budget/money (markup not configured)",
            updated_at=now,
        )
        .on_conflict_do_nothing(index_elements=[TenantConfig.tenant_id])
    )
    await session.execute(
        update(TenantConfig)
        .where(TenantConfig.tenant_id == tenant_id, TenantConfig.budget_unit != "money")
        .values(budget_unit="money", updated_at=now)
        .execution_options(synchronize_session=False)
    )
    await session.flush()
    return budget, before


def money_budget_audit_dict(budget: MoneyBudget) -> dict[str, Any]:
    return {
        "id": str(budget.id),
        "tenant_id": str(budget.tenant_id),
        "currency": budget.currency,
        "amount": int(budget.amount),
        "period": budget.period,
        "consumed": str(budget.consumed),
        "active": bool(budget.active),
        "source": budget.source,
    }


def thresholds_crossed(amount: int, consumed: Decimal) -> list[int]:
    if amount <= 0:
        return []
    pct = consumed / Decimal(amount) * 100
    return [t for t in MONEY_THRESHOLDS if pct >= t]


# --- currency change (REV F) ------------------------------------------------------


_Q2 = Decimal("0.01")


def _round_units(value: Decimal) -> int:
    """Whole units, ROUND_HALF_UP (the cap is a whole-unit integer by contract)."""
    return int(value.to_integral_value(rounding=ROUND_HALF_UP))


def _round_minor(value: Decimal) -> Decimal:
    """Minor units (2 dp for EUR / USD / CAD), ROUND_HALF_UP."""
    return value.quantize(_Q2, rounding=ROUND_HALF_UP)


class MoneyHoldsInFlight(Exception):  # noqa: N818 - a verdict
    """A currency change was asked for while reservations are open (409)."""

    def __init__(self, holds: int) -> None:
        super().__init__(f"{holds} hold(s) in flight")
        self.holds = holds


async def count_live_holds(redis_conn: aioredis.Redis, tenant_id: str, now: datetime) -> int:
    """Unexpired holds in the tenant's money index (expired-but-unswept ones do not count)."""
    return int(
        await redis_conn.zcount(money_index_key(tenant_id), f"({int(now.timestamp())}", "+inf")
    )


@dataclass(frozen=True)
class CurrencyChange:
    changed: bool
    budget: MoneyBudget
    adjustment: MoneyBudgetAdjustment | None
    from_currency: str
    to_currency: str
    fx_rate: Decimal
    amount_before: int
    consumed_before: Decimal
    remaining_before: Decimal
    remaining_after: Decimal
    before_audit: dict[str, Any]


async def change_money_currency(
    session: AsyncSession,
    redis_conn: aioredis.Redis | None,
    tenant_id: str,
    *,
    to_currency: str,
    actor_sub: str | None,
    actor_role: str | None,
    via_client: str | None,
    reason: str | None,
    now: datetime | None = None,
) -> CurrencyChange | None:
    """Re-denominate the active budget (owner only; the route enforces who).

    None = no active budget. Same currency = ``changed=False``, nothing written.
    Refusals (nothing written):

    * :class:`MoneyHoldsInFlight` -- any unexpired reservation is open (409
      ``holds_in_flight``): a hold priced in the old currency must settle first.
    * :class:`MoneyRedisUnavailable` -- the open holds cannot be counted (503).
    * :class:`MoneyRefusal` ``fx_rate_missing`` -- either USD leg has no rate.

    In the caller's transaction, FOR UPDATE on the budget row, at the NEWEST FX
    rates effective ``now`` (USD -> X; USD = 1), cross = rate(to) / rate(from):

    * ``remaining_after = round_half_up((amount - consumed) x cross, 2 dp)``.
    * ``amount_after = round_half_up(amount x cross, 0 dp)`` -- the cap for later
      periods (whole units by contract).
    * ``consumed_after = amount_after - remaining_after`` -- the opening
      consumed-equivalent (negative = opening credit), so ``remaining`` after
      the change is exactly ``remaining_after``.
    * one ``tenant_money_budget_adjustment`` row: before-figures in the ORIGINAL
      currency, the cross rate and both USD legs used, owner + relaying SA.
    """
    if to_currency not in SUPPORTED_CURRENCIES:
        raise ValueError(f"unsupported currency {to_currency!r}")
    now = now or datetime.now(UTC)
    budget = await get_active_money_budget(session, tenant_id, for_update=True)
    if budget is None:
        return None
    from_currency = budget.currency
    amount_before = int(budget.amount)
    consumed_before = effective_money_consumed(budget, now)
    remaining_before = q8(Decimal(amount_before) - consumed_before)
    before_audit = money_budget_audit_dict(budget)
    if from_currency == to_currency:
        return CurrencyChange(
            changed=False,
            budget=budget,
            adjustment=None,
            from_currency=from_currency,
            to_currency=to_currency,
            fx_rate=Decimal(1),
            amount_before=amount_before,
            consumed_before=consumed_before,
            remaining_before=remaining_before,
            remaining_after=remaining_before,
            before_audit=before_audit,
        )
    if redis_conn is None:
        raise MoneyRedisUnavailable
    holds = await count_live_holds(redis_conn, tenant_id, now)
    if holds:
        raise MoneyHoldsInFlight(holds)
    rates = await cross_rate(session, from_currency, to_currency, now)
    if rates is None:
        raise MoneyRefusal("fx_rate_missing")
    cross, rate_from, rate_to = rates
    remaining_after = q8(_round_minor(remaining_before * cross))
    amount_after = _round_units(Decimal(amount_before) * cross)
    consumed_after = q8(Decimal(amount_after) - remaining_after)
    anchor = period_anchor(budget.period, now)

    budget.currency = to_currency
    budget.amount = amount_after
    budget.consumed = consumed_after
    if anchor is not None:
        budget.consumed_period_start = anchor
    budget.updated_at = now
    adjustment = MoneyBudgetAdjustment(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        budget_id=budget.id,
        kind="currency_conversion",
        period_start=anchor,
        from_currency=from_currency,
        to_currency=to_currency,
        fx_rate=cross,
        usd_rate_from=rate_from,
        usd_rate_to=rate_to,
        amount_before=amount_before,
        amount_after=amount_after,
        consumed_before=q8(consumed_before),
        consumed_after=consumed_after,
        remaining_before=remaining_before,
        remaining_after=remaining_after,
        actor_sub=actor_sub,
        actor_role=actor_role,
        via_client=via_client,
        reason=reason,
        created_at=now,
    )
    session.add(adjustment)
    await session.flush()
    return CurrencyChange(
        changed=True,
        budget=budget,
        adjustment=adjustment,
        from_currency=from_currency,
        to_currency=to_currency,
        fx_rate=cross,
        amount_before=amount_before,
        consumed_before=consumed_before,
        remaining_before=remaining_before,
        remaining_after=remaining_after,
        before_audit=before_audit,
    )


async def period_conversions(
    session: AsyncSession, budget: MoneyBudget, now: datetime
) -> list[MoneyBudgetAdjustment]:
    """This period's currency changes of ``budget``, oldest first (none once rolled over)."""
    if is_stale_money_period(budget, now):
        return []
    anchor = _consumption_anchor(budget, now)
    rows = await session.execute(
        select(MoneyBudgetAdjustment)
        .where(
            MoneyBudgetAdjustment.budget_id == budget.id,
            MoneyBudgetAdjustment.kind == "currency_conversion",
            MoneyBudgetAdjustment.period_start == anchor,
        )
        .order_by(MoneyBudgetAdjustment.created_at, MoneyBudgetAdjustment.id)
    )
    return list(rows.scalars().all())


def consumed_by_currency(
    budget: MoneyBudget, consumed: Decimal, conversions: list[MoneyBudgetAdjustment]
) -> list[tuple[str, Decimal]]:
    """This period's consumption per denomination, each in its ORIGINAL currency.

    Segment i (before conversion i) = ``consumed_before_i - consumed_after_{i-1}``
    (both in that segment's currency); the open segment = current consumed minus
    the last opening equivalent.
    """
    out: list[tuple[str, Decimal]] = []
    opening = Decimal(0)
    for adj in conversions:
        out.append((adj.from_currency, q8(Decimal(adj.consumed_before) - opening)))
        opening = Decimal(adj.consumed_after)
    out.append((budget.currency, q8(consumed - opening)))
    return out
