"""Redis budget state — pre-flight reservation.

An atomic check-and-reserve Lua script so two concurrent reservations can't
both pass the cap check. The committed spend it checks against is
``token_budgets.consumed_tokens`` read from Postgres.

Nothing else is mirrored to Redis: token budgets live in
``token_budgets.consumed_tokens`` and request quotas in
``request_quota_config`` / ``metering_monthly_usage``; license-authz asks
``GET /api/budget/check`` and ``GET /api/quota/check``.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any, cast

import redis.asyncio as aioredis
import structlog
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession

from observability import metrics as observability_metrics
from services.budget_debit import _select_active_budgets
from services.budget_period import effective_consumed
from settings import get_settings
from tenancy import redact_tenant_id

logger = structlog.stdlib.get_logger(__name__)


# Atomic check-and-reserve Lua script. Runs server-side in Redis so two
# concurrent reservations can't both pass the cap check.
#
# `consumed` is passed in as ARGV, not read from Redis: the authoritative
# committed spend is `sum(TokenBudget.consumed_tokens)` read fresh from the DB
# on every call (debit_budget writes it), so Redis only has to hold the
# in-flight `reserved` counter — the one value that needs cross-caller
# atomicity. The previous design cached consumed in a `consumed_pool` key seeded
# once per 35d window with SET NX and never refreshed by debits, so after the
# first reservation the gate saw consumed≈0 forever and stopped enforcing
# accumulated spend (a tenant could reserve ~cap per request unboundedly).
#
# KEYS[1]   = reserved key    (tokens:tenant:{id}:reserved)
# ARGV[1]   = consumed tokens (integer; authoritative committed spend from DB)
# ARGV[2]   = requested tokens (integer)
# ARGV[3]   = cap tokens       (integer; sum of active budget_tokens)
# ARGV[4]   = ttl seconds      (integer)
#
# `cap == 0` means a **zero-allowance** budget (block all) — NOT unlimited. The
# guard is `cap >= 0` so a 0 cap enforces `(consumed + reserved + req) > 0`,
# rejecting any non-zero request. Previously the guard was `cap > 0`, which
# SKIPPED the cap check entirely at cap==0 and silently served it as unlimited.
# "Unlimited" is expressed upstream by the ABSENCE of an active budget
# (`no_active_budget` short-circuits reserve_budget before the Lua runs), never
# by a 0 cap.
#
# ⚠️ cap==0 is REACHABLE. This comment used to claim the opposite — "no write
# path ever persists a 0/negative budget_tokens (domain `Field(gt=0)` on create
# + the `> 0` sync guard), so cap==0 is unreachable today ... a latent
# fail-open, not a live path". Both halves of that parenthetical are now wrong,
# and the sync half was already wrong when it was written:
#   - `sync-from-licenses` gates on `token_budget is not None and >= 0`, so a
#     plan suspended upstream to `token_budget_monthly: 0` has always been able
#     to persist a 0-cap row through `upsert_license_sync_budget`;
#   - `TokenBudgetCreate.budget_tokens` is now `ge=0`, so an operator can set a
#     block-all budget directly via `POST /api/budget`.
# `token_budgets.budget_tokens` carries no CHECK constraint either, so nothing
# below the model rejects a 0.
#
# So `cap >= 0` here is LIVE ENFORCEMENT, not a latent-hole close. Do not
# "simplify" it back to `cap > 0` on the belief that 0 cannot occur: that would
# serve every block-all budget as unlimited — a fail-open on the exact rows
# that exist to deny.
#
# Returns: cjson string {"ok": bool, "consumed": int, "reserved": int, "cap": int}
_RESERVE_LUA = """
local reserved = tonumber(redis.call('GET', KEYS[1]) or '0')
local consumed = tonumber(ARGV[1])
local req = tonumber(ARGV[2])
local cap = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
if cap >= 0 and (consumed + reserved + req) > cap then
  return cjson.encode({ok=false, consumed=consumed, reserved=reserved, cap=cap})
end
redis.call('INCRBY', KEYS[1], req)
if redis.call('TTL', KEYS[1]) < 0 then
  redis.call('EXPIRE', KEYS[1], ttl)
end
return cjson.encode({ok=true, consumed=consumed, reserved=reserved + req, cap=cap})
"""

#: How many expired handles ONE reserve may reclaim. The sweep runs inside the
#: reserve script, and Redis is single-threaded: an unbounded
#: ``ZRANGEBYSCORE -inf now`` on a tenant that has accumulated 100k stale
#: handles would hold the whole server for the length of that scan, stalling
#: EVERY other tenant's reserve — a latency incident caused by the garbage
#: collector, not by the traffic. Bounded, the backlog still cannot grow from
#: reserves: each reserve adds exactly ONE handle and may reclaim up to 256, so
#: the drain rate dominates the fill rate by 256:1 and a backlog can only be
#: inherited (from a flag flip, or from a burst whose releases were lost), never
#: created here.
_RESERVATION_SWEEP_LIMIT = 256

# Atomic sweep-check-and-reserve Lua script — the IDENTIFIED reservation
# protocol, used only when ``budget_require_reservation_handle`` is armed.
#
# ⭐ WHAT THIS FIXES, and it is the ONLY thing it changes about the money
# arithmetic: nothing. The admission test below is byte-for-byte the one in
# :data:`_RESERVE_LUA` (``cap >= 0 and (consumed + pool + req) > cap``), for all
# the reasons recorded above it — ``cap == 0`` is a REACHABLE block-all budget,
# not "unlimited", and ``consumed`` stays DB-authoritative (ARGV, never a cached
# Redis pool). What changes is that the reservation now has an IDENTITY, so the
# SETTLEMENT can name which hold it is discharging instead of subtracting an
# anonymous number from a shared integer. See :data:`_RELEASE_BY_HANDLE_LUA`.
#
# Three keys, ONE hash tag. ``tokens:tenant:{<id>}:reserved`` carries literal
# braces around the tenant id so all three keys hash to the same Redis Cluster
# slot and a single ``EVAL`` may touch them. ⛔ This is a DIFFERENT key from the
# untagged ``tokens:tenant:<id>:reserved`` that :func:`reserved_key` returns and
# that :data:`_RESERVE_LUA` uses; the untagged name must not be reshaped in
# place, because that would orphan every pool that is live at the moment of the
# change. The consequence is explicit and accepted: arming the flag strands the
# untagged pool, whose in-flight holds then lapse on its own TTL (<= 600s), and
# the tagged pool starts empty — so for up to one TTL window the gate is more
# permissive by whatever was in flight at the flip. Disarming strands the tagged
# side the same way. Flip it at low traffic, once, in one direction.
#
# The sweep is what makes the handles' expiry REAL. In the pooled protocol the
# key's own TTL was the only release for an over-estimate's residue, which is
# why :data:`_RESERVE_LUA` sets that TTL on creation ONLY (re-``EXPIRE``-ing on
# every success is what let ``reserved`` grow monotonically until the gate 429'd
# everything). Here each hold carries its OWN expiry in the ZSET, the sweep
# subtracts expired holds from the pool explicitly, and the keys' TTL is
# demoted to a whole-structure garbage collector. ⚠️ So this script DOES refresh
# the TTL on every success — the opposite of its sibling — and that is safe only
# because the sweep, not the TTL, is now the release path. It refreshes via
# ``TTL < ttl`` rather than a bare ``EXPIRE``: a bare one would SHORTEN the
# window when a 5s reserve follows a live 600s hold, dropping the index while
# that hold is still outstanding and silently freeing it early.
#
# ⛔ The handle is minted in PYTHON and passed in as ARGV. Generating it in Lua
# (``math.random``/``redis.sha1hex`` of a clock) is non-deterministic, which is
# exactly what makes a script unsafe to replicate and unsafe to replay from the
# AOF.
#
# KEYS[1] = pool key    (tokens:tenant:{id}:reserved   INT)
# KEYS[2] = index key   (tokens:tenant:{id}:resv       ZSET handle -> expiry_unix)
# KEYS[3] = amounts key (tokens:tenant:{id}:resv:amt   HASH handle -> tokens)
# ARGV[1] = consumed tokens (integer; authoritative committed spend from DB)
# ARGV[2] = requested tokens (integer)
# ARGV[3] = cap tokens       (integer; sum of active budget_tokens)
# ARGV[4] = ttl seconds      (integer)
# ARGV[5] = now              (unix seconds, from Python — see the mint note)
# ARGV[6] = handle           (uuid4 hex, from Python)
# ARGV[7] = sweep limit      (integer; _RESERVATION_SWEEP_LIMIT)
#
# Returns: cjson string
#   {"ok": bool, "consumed": int, "reserved": int, "cap": int,
#    "expired": int, "handle": str}   -- "handle" is ABSENT when ok=false,
# because no hold was created and handing back a dead handle would invite a
# settlement against nothing. ``expired`` is the number of handles this sweep
# reclaimed and is reported on BOTH branches: the sweep ran either way, and a
# refusal is precisely when its size matters most.
_RESERVE_HANDLE_LUA = """
local consumed = tonumber(ARGV[1])
local req = tonumber(ARGV[2])
local cap = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local handle = ARGV[6]
local sweep_limit = tonumber(ARGV[7])

local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, sweep_limit)
local reclaimed = 0
local reclaimed_tokens = 0
for i = 1, #expired do
  local amt = tonumber(redis.call('HGET', KEYS[3], expired[i]) or '0')
  if redis.call('HDEL', KEYS[3], expired[i]) == 1 then
    reclaimed = reclaimed + 1
    reclaimed_tokens = reclaimed_tokens + amt
  end
  redis.call('ZREM', KEYS[2], expired[i])
end
if reclaimed_tokens > 0 then
  if redis.call('DECRBY', KEYS[1], reclaimed_tokens) <= 0 then
    redis.call('DEL', KEYS[1])
  end
end

local pool = tonumber(redis.call('GET', KEYS[1]) or '0')
if pool < 0 then pool = 0 end
if cap >= 0 and (consumed + pool + req) > cap then
  return cjson.encode({ok=false, consumed=consumed, reserved=pool, cap=cap, expired=reclaimed})
end
redis.call('ZADD', KEYS[2], now + ttl, handle)
redis.call('HSET', KEYS[3], handle, req)
redis.call('INCRBY', KEYS[1], req)
for i = 1, 3 do
  if redis.call('TTL', KEYS[i]) < ttl then
    redis.call('EXPIRE', KEYS[i], ttl)
  end
end
return cjson.encode({
  ok=true, consumed=consumed, reserved=pool + req, cap=cap,
  expired=reclaimed, handle=handle
})
"""

# The SHADOW half of the identified protocol: mint and bookkeeping, NO verdict.
#
# ⭐ WHY THIS EXISTS — a MEASURE-BEFORE-ARMING DEADLOCK.
# ``budget_require_reservation_handle`` used to gate THREE things at once: minting
# the handle, maintaining the handle index, and routing settlement through
# :func:`_release_by_handle`. So the one question an operator must answer before
# arming — "do settlements actually arrive naming a hold that exists?" — could
# only be answered BY arming, and arming flipped three structures in one step.
#
# This script is :data:`_RESERVE_HANDLE_LUA` WITH THE ADMISSION TEST REMOVED. It
# decides nothing. The verdict has already been taken by :data:`_RESERVE_LUA`
# against the untagged pool, exactly as it is today, and this runs afterwards
# ONLY for a reserve that was already ADMITTED. What it does is keep the
# identified protocol's three keys warm and truthful while the flag is off:
#
#   - ``ZADD``/``HSET`` the hold, so the index is a real census of live holds;
#   - ``INCRBY`` the TAGGED pool, so arming later finds a pool that already
#     carries exactly the live holds instead of an empty one;
#   - run the same bounded sweep, so the index cannot grow.
#
# ⛔ IT NEVER TOUCHES THE UNTAGGED POOL, and it cannot: ``tokens:tenant:<id>:
# reserved`` — the key :data:`_RESERVE_LUA` admits against and :data:`_RELEASE_LUA`
# drains — is not among these KEYS. Every key here carries the ``{<tenant>}`` hash
# tag, so all three hash to one Redis Cluster slot (no CROSSSLOT) and not one of
# them is a key the gate reads while the flag is off. THAT is what keeps the
# pooled protocol byte-for-byte unchanged: the money arithmetic runs in a
# different script against a different key, and this one writes a ledger that
# nothing consults yet.
#
# ⚠️ Run in its OWN ``eval``, deliberately, not folded into the admission script.
# Two reasons, and the first is the important one: a bug or a Redis failure in
# observational bookkeeping must not be able to change or abort an admission
# verdict, and a separate best-effort call is the only shape that guarantees it.
# The second is CROSSSLOT — the untagged pool and these tagged keys cannot share
# one EVAL on a sharded Redis at all.
#
# ⚠️ NOTHING SETTLES A SHADOW HOLD, by design: ``release_reservation`` still
# routes to :data:`_RELEASE_LUA` while the flag is off, and no discharge path
# reaches these keys. So every shadow hold lapses and is reclaimed by this sweep,
# which is exactly the pre-arm reading an operator wants —
# ``minted ≈ expired, settled == 0`` reads "handles mint fine and nothing is
# naming them yet".
#
# ⚠️ THE TTL IS REFRESHED ON EVERY RESERVE, like its armed sibling and unlike
# :data:`_RESERVE_LUA`. Safe for the same reason it is safe there: the SWEEP, not
# the TTL, is the release path for these keys, so the TTL is a whole-structure
# garbage collector rather than the thing that returns headroom. It refreshes via
# ``TTL < ttl`` rather than a bare ``EXPIRE`` so a short reserve cannot shorten
# the window out from under a live long hold.
#
# KEYS[1] = pool key    (tokens:tenant:{id}:reserved    INT, TAGGED)
# KEYS[2] = index key   (tokens:tenant:{id}:resv        ZSET handle -> expiry_unix)
# KEYS[3] = amounts key (tokens:tenant:{id}:resv:amt    HASH handle -> tokens)
# ARGV[1] = reserved tokens (integer; what the ADMITTED reserve actually held)
# ARGV[2] = ttl seconds     (integer)
# ARGV[3] = now             (unix seconds, from Python)
# ARGV[4] = handle          (uuid4, from Python — never minted in Lua, see above)
# ARGV[5] = sweep limit     (integer; _RESERVATION_SWEEP_LIMIT)
#
# Returns: {handles_reclaimed_by_the_sweep}
_RESERVE_SHADOW_LUA = """
local req = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local handle = ARGV[4]
local sweep_limit = tonumber(ARGV[5])

local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, sweep_limit)
local reclaimed = 0
local reclaimed_tokens = 0
for i = 1, #expired do
  local amt = tonumber(redis.call('HGET', KEYS[3], expired[i]) or '0')
  if redis.call('HDEL', KEYS[3], expired[i]) == 1 then
    reclaimed = reclaimed + 1
    reclaimed_tokens = reclaimed_tokens + amt
  end
  redis.call('ZREM', KEYS[2], expired[i])
end
if reclaimed_tokens > 0 then
  if redis.call('DECRBY', KEYS[1], reclaimed_tokens) <= 0 then
    redis.call('DEL', KEYS[1])
  end
end

redis.call('ZADD', KEYS[2], now + ttl, handle)
redis.call('HSET', KEYS[3], handle, req)
redis.call('INCRBY', KEYS[1], req)
for i = 1, 3 do
  if redis.call('TTL', KEYS[i]) < ttl then
    redis.call('EXPIRE', KEYS[i], ttl)
  end
end
return {reclaimed}
"""

# Release the settled portion of the in-flight reservation pool.
#
# `DECRBY` (not GET-then-SET) so the release is atomic against a concurrent
# reserve, and so the existing TTL is preserved untouched. Clamped: a debit with
# no matching reservation (or a debit larger than what was reserved) would drive
# the counter negative, and a NEGATIVE `reserved` silently RAISES the effective
# cap in the reserve check — a fail-open on money. At <= 0 the key is deleted
# instead, which is the same state as "nothing in flight" and also drops a
# stale TTL.
#
# ⚠️ THE CLAMP IS A FLOOR, NOT AN ATTRIBUTION. The pool is ONE per-tenant
# integer: reservations carry no identity (``BudgetReserveResponse`` returns no
# handle, and ``release_reservation`` has exactly one caller — the anonymous
# cost-flush settlement in ``release_budget_reservations``). So when a
# settlement releases MORE than is pooled, the tokens it takes past its own
# reservation come out of OTHER callers' in-flight reservations, and the clamp
# to 0 hides that. Confirmed reachable: cap=1000, A reserves 400, B reserves 400
# (pool 800), A settles at an ACTUAL 900 -> pool deleted -> a third caller
# reserving 100 is ADMITTED at consumed=900 + reserved=0, while B's 400 is still
# in flight (true position 1400 > cap). The same wipe happens for any debit from
# a producer that never reserved at all.
#
# Clamping "per reservation" is NOT implementable at this keying — there is no
# per-reservation key to clamp against — and releasing LESS would restore the
# fail-CLOSED 429 storm that the release path exists to prevent. So the money
# behaviour is unchanged and the overshoot is instead made EXPRESSIBLE: the
# script returns it, and `release_reservation` logs + counts it, so the size and
# rate of the fail-open are measurable in prod before anyone changes the
# reserve/settle protocol (which needs a license-authz change, not a change here).
#
# ⭐ EVERY WORD ABOVE IS STILL TRUE OF THIS SCRIPT, and this script is still what
# runs by default. The keying it describes as impossible to clamp against now
# EXISTS — see :data:`_RESERVE_HANDLE_LUA` / :data:`_RELEASE_BY_HANDLE_LUA` — but
# only behind ``budget_require_reservation_handle``, and only this service's half
# of it: license-authz has to start round-tripping ``reservation_id`` from
# ``POST /budget/reserve`` into its settlement before the identified path can
# discharge anything. Until both are true, the measured 151.6 harmful
# over-releases/hour are still measured HERE, by this clamp and these counters.
#
# KEYS[1] = reserved key (tokens:tenant:{id}:reserved)
# ARGV[1] = tokens to release (integer > 0)
# Returns: {remaining in-flight reservation (never negative), overshoot, prior}.
# ``overshoot`` is how far past the pooled reservations this release reached —
# 0 for an exact drain, > 0 only for a release that outran the pool.
#
# ``prior`` is the pool as it stood BEFORE this release, and it is the field
# that separates the two utterly different populations that ``overshoot > 0``
# used to collapse into one number:
#
#   prior == 0  -> there was no pool at all (key absent, TTL-expired, or the
#                  spend simply never reserved). ``overshoot == amount``.
#                  Nothing is taken from anybody; the next reserve correctly
#                  sees ``reserved=0``. BENIGN, and the ordinary state of the
#                  debit path, which does not reserve.
#   prior > 0   -> a real pool was drained past its end. ``overshoot < amount``
#                  and those ``overshoot`` tokens came out of OTHER callers'
#                  live reservations. This is the fail-open on money described
#                  above, and the only one worth paging on.
#
# ``prior`` is derived from ``DECRBY``'s own return value (``remaining +
# amount``) — NOT a second read. So the script still performs exactly one
# mutation, needs no GET/DECRBY pair, and stays atomic against a concurrent
# reserve exactly as before; the extra field costs nothing at runtime.
_RELEASE_LUA = """
local amount = tonumber(ARGV[1])
if amount <= 0 then
  local pool = tonumber(redis.call('GET', KEYS[1]) or '0')
  return {pool, 0, pool}
end
local remaining = redis.call('DECRBY', KEYS[1], amount)
local prior = remaining + amount
if remaining <= 0 then
  redis.call('DEL', KEYS[1])
  return {0, -remaining, prior}
end
return {remaining, 0, prior}
"""

# Discharge NAMED reservations. The identified half of the settlement, used only
# when ``budget_require_reservation_handle`` is armed.
#
# ⭐ THIS SCRIPT IS THE FIX. The pooled release above subtracts a number; this one
# deletes specific holds. Tokens that belong to NO hold never reach the pool at
# all — they are counted in Python on
# ``billing_budget_release_without_reserve_total`` and dropped. That single
# branch is what makes `billing_budget_reserve_over_release_total` unreachable:
# there is no longer any path by which a settlement can reduce the pool by more
# than the holds it names, so no settlement can take tokens out of another
# caller's live reservation. Its rate going to zero IS the acceptance test.
#
# ⭐ FULL AMOUNT, NEVER ``min(actual, reserved)``. A hold is removed for its whole
# reserved amount regardless of what the request actually cost, because by the
# time this runs the real cost is committed to ``TokenBudget.consumed_tokens``,
# which the reserve path re-reads from the DB on EVERY call (ARGV[1] there).
# Leaving any part of the hold behind would therefore double-count that spend
# against the cap — the exact fail-CLOSED over-denial the release path exists to
# prevent. The residue of an over-estimate is not "unaccounted" here the way it
# was under the pooled protocol; it is accounted by the hold's deletion.
#
# ⭐ IDEMPOTENT BY CONSTRUCTION, and that is REQUIRED, not a nicety. One request
# can emit N cost events (provider failover re-bills the same attempt) while
# reserving exactly ONCE, so settlements 2..N must be harmless no-ops.
# ``HDEL``'s own return value — not an ``HGET``, not a Python-side membership
# check — is the authority on whether this settlement is the one that found the
# hold, so a concurrent duplicate cannot both subtract the same amount.
#
# ⚠️ ``settled > prior`` (the pool is smaller than the holds it carries) is
# reachable only through an inconsistency this protocol does not create: a
# partial expiry of the three keys, or a hand-edited pool. It is clamped to 0 and
# logged as ``reservation_pool_underflow``; it is deliberately NOT counted on
# ``budget_reserve_over_release_total``, because that series' whole value after
# the flip is that it reads a flat zero — feeding a different defect into it
# would re-arm a CRITICAL alert on a condition it does not name.
#
# No ``EXPIRE`` anywhere: ``DECRBY``/``HDEL``/``ZREM`` all preserve the existing
# TTLs, exactly as the pooled release does, and only the reserve path is allowed
# to extend the structure's life.
#
# KEYS[1] = pool key    (tokens:tenant:{id}:reserved)
# KEYS[2] = index key   (tokens:tenant:{id}:resv)
# KEYS[3] = amounts key (tokens:tenant:{id}:resv:amt)
# ARGV[*] = handles to discharge (0..N; zero is legitimate — an unattributed
#           settlement still wants the pool read back, and doing it in the same
#           EVAL keeps this path at exactly one round trip, like its sibling)
# Returns: {remaining, 0, prior, handles_dropped, tokens_dropped}
#
# ⚠️ The first three slots hold the SAME positional contract as
# :data:`_RELEASE_LUA`'s ``{remaining, overshoot, prior}`` on purpose, so
# :func:`_split_release_result` reads this reply truthfully rather than
# accidentally. Slot 2 is structurally 0 here (see above). The two extra slots
# are read by :func:`_split_handle_release_result`.
_RELEASE_BY_HANDLE_LUA = """
local settled = 0
local found = 0
for i = 1, #ARGV do
  local amt = tonumber(redis.call('HGET', KEYS[3], ARGV[i]) or '0')
  if redis.call('HDEL', KEYS[3], ARGV[i]) == 1 then
    found = found + 1
    settled = settled + amt
  end
  redis.call('ZREM', KEYS[2], ARGV[i])
end
local prior = tonumber(redis.call('GET', KEYS[1]) or '0')
local remaining = prior
if settled > 0 then
  remaining = redis.call('DECRBY', KEYS[1], settled)
  if remaining <= 0 then
    redis.call('DEL', KEYS[1])
    remaining = 0
  end
end
return {remaining, 0, prior, found, settled}
"""


def reserved_key(tenant_id: str) -> str:
    """Redis key holding a tenant's in-flight (reserved, not yet debited) tokens.

    ⛔ UNTAGGED, and it must stay that way. This is the key every live pool in
    prod is under, and the only key the default (pooled) protocol touches.
    Reshaping it — to add the Redis Cluster hash tag the identified protocol
    needs, or for any other reason — would orphan every reservation in flight at
    the moment of deploy and hand the gate an empty pool. The tagged form is a
    SEPARATE key, :func:`_tagged_reserved_key`, reached only when
    ``budget_require_reservation_handle`` is armed.
    """
    return f"tokens:tenant:{tenant_id}:reserved"


def _tagged_reserved_key(tenant_id: str) -> str:
    """Hash-tagged pool key — the identified protocol's counterpart to :func:`reserved_key`.

    ``tokens:tenant:{<id>}:reserved``, with LITERAL braces. The braces are a
    Redis Cluster hash tag: every key whose tag matches hashes to one slot, which
    is what lets :data:`_RESERVE_HANDLE_LUA` touch the pool, the index and the
    amounts hash in a single ``EVAL``. Without it a cluster would refuse the
    script with CROSSSLOT — billing's Redis is single-node today, so the untagged
    key works and nothing has forced the issue, but a three-key script is exactly
    the thing that turns that into an outage on the day it is sharded.

    ⚠️ Private on purpose: nothing outside this module should be able to read the
    identified pool under the pooled protocol's name, or vice versa.
    """
    return f"tokens:tenant:{{{tenant_id}}}:reserved"


def reservation_index_key(tenant_id: str) -> str:
    """ZSET of a tenant's live reservation handles -> their unix expiry.

    The expiry lives in the SCORE rather than in a per-handle key with its own
    TTL, because the sweep has to be able to ask "which holds are dead" in one
    bounded range read; N expiring keys answer that question only by being
    absent, which is unobservable from inside a Lua script and would leave the
    pool permanently inflated by every hold that lapsed.
    """
    return f"tokens:tenant:{{{tenant_id}}}:resv"


def reservation_amounts_key(tenant_id: str) -> str:
    """HASH of a tenant's live reservation handles -> tokens held.

    Separate from the index rather than packed into the ZSET member, because the
    member is the lookup key on the settlement path: a composite
    ``handle:amount`` member could not be ``ZREM``'d by handle alone, so a
    settlement would have to scan the index to find its own hold.
    """
    return f"tokens:tenant:{{{tenant_id}}}:resv:amt"


async def _record_reservation_shadow(
    redis_conn: aioredis.Redis,
    tenant_id: str,
    *,
    handle: str,
    tokens: int,
    now_unix: int,
    ttl_seconds: int,
) -> tuple[bool, int]:
    """Record an ALREADY-ADMITTED reserve in the identified protocol's keys.

    Observation only — see :data:`_RESERVE_SHADOW_LUA` for why none of this can
    reach the key the gate admits against while
    ``budget_require_reservation_handle`` is off. Called only on the pooled path,
    only after :data:`_RESERVE_LUA` has returned ``ok=true``, so the verdict is
    already final and unaffected by anything that happens here.

    Returns ``(recorded, expired_reclaimed)``.

    ⭐ ``recorded`` is False when Redis refused the write, and the caller MUST then
    hand back no ``reservation_id``. The invariant is the one the armed path
    already holds through the script's echo: an id we return is evidence a hold
    EXISTS, never evidence we intended one. A name with no hold behind it is a
    name a later settlement could claim against nothing — the exact class of
    defect the identified protocol exists to remove — and it would also corrupt
    the coverage measurement this function exists to enable, by counting a mint
    that never landed.

    ⚠️ BEST-EFFORT, and it must stay that way. A ``RedisError`` here is logged at
    debug and swallowed: the reservation itself succeeded, the money arithmetic
    is committed to the untagged pool, and failing an admitted reserve because a
    measurement write missed would let an instrument deny real traffic. The
    consequence of a swallow is bounded and self-announcing — the mint counter
    does not move for that reserve, so a Redis problem shows up as mint volume
    falling BELOW successful-reserve volume rather than as anything the caller
    can feel.
    """
    try:
        raw = await cast("Any", redis_conn.eval)(
            _RESERVE_SHADOW_LUA,
            3,
            _tagged_reserved_key(tenant_id),
            reservation_index_key(tenant_id),
            reservation_amounts_key(tenant_id),
            str(int(tokens)),
            str(int(ttl_seconds)),
            str(int(now_unix)),
            handle,
            str(_RESERVATION_SWEEP_LIMIT),
        )
    except RedisError:
        logger.debug(
            "reservation_shadow_record_failed",
            tenant_id=redact_tenant_id(tenant_id),
            note=(
                "the reserve itself SUCCEEDED and the pooled gate is unaffected; only "
                "the pre-arm handle census missed this hold, so no reservation_id is "
                "returned for it and billing_budget_reservation_minted_total does not move"
            ),
            exc_info=True,
        )
        return False, 0
    # Tolerant in the same direction as the release readers, and towards doing
    # nothing: a reply that cannot say how many holds it reclaimed reports ZERO
    # rather than inventing expiries. The WRITE is what `recorded` attests to, and
    # the script performs it unconditionally once it runs at all.
    reclaimed = 0
    if isinstance(raw, list | tuple) and len(raw) > 0:
        reclaimed = int(raw[0] or 0)
    elif isinstance(raw, int):
        reclaimed = int(raw)
    return True, reclaimed


async def release_reservation(
    redis_conn: aioredis.Redis | None,
    tenant_id: str,
    tokens: int,
    *,
    handles: dict[str, int] | None = None,
) -> int:
    """Release ``tokens`` from the tenant's in-flight reservation pool.

    THE MISSING HALF of :func:`reserve_budget`. ``tokens:tenant:{id}:reserved``
    had exactly one writer and no release path anywhere in the service, while
    the reserve script re-``EXPIRE``d it on every success — so under sustained
    traffic the key never aged out, ``reserved`` grew monotonically, and the gate
    eventually returned 429 ``over_budget`` on EVERY request while committed
    ``consumed_tokens`` was still ~0. The gate failed CLOSED.

    Called when a reservation SETTLES (its real cost is debited — see
    :func:`services.budget_debit.release_budget_reservations`) or when a caller
    explicitly cancels one. Releasing the DEBITED amount is exactly right: once a
    debit commits, those tokens are represented in ``consumed_tokens``, which the
    gate re-reads fresh from the DB on every call, so leaving them in ``reserved``
    too would double-count them.

    The residue of an OVER-estimate (estimated minus actual) is genuinely
    unaccounted and is released by the key's TTL, which now expires for real
    because :data:`_RESERVE_LUA` only sets it on creation. That bounds the
    in-flight pool at roughly one TTL window of traffic instead of forever.

    The reverse residue — an UNDER-estimate, or a debit that never reserved at
    all — cannot be attributed away at this keying, so it is COUNTED instead of
    hidden. It splits into two populations that must NOT share a series:

    - **Released against a live pool** (``prior > 0``): the excess is taken out
      of other callers' in-flight reservations, so the next reserve sees a pool
      that is too small and admits spend it should have denied. The cap fails
      OPEN by exactly the overshoot. Counted as
      ``billing_budget_reserve_over_release_total`` with a
      ``reserve_over_release`` **warning**. This is the one that pages.
    - **Released against no pool at all** (``prior == 0``): key absent, TTL
      lapsed, or the spend never reserved — which is the NORMAL debit path,
      since :func:`reserve_budget` has one caller (``POST /budget/reserve``)
      while this function runs on every debit. Nothing is taken from anyone.
      Counted as ``billing_budget_release_without_reserve_total`` with a
      ``release_without_reserve`` **info** line.

    Before the split both incremented the first counter, so it ran at ~90% of
    all debits and its CRITICAL alert fired continuously on healthy traffic —
    the defect signal was buried in its own benign majority.

    Best-effort: a Redis failure is logged at debug and swallowed — the DB
    ``consumed_tokens`` column remains the source of truth for the cap.
    Returns the remaining in-flight reservation (0 if unknown/failed).

    ⭐ **TWO PROTOCOLS, ONE SWITCH.** Everything above describes the POOLED
    protocol, which is what runs while ``budget_require_reservation_handle`` is
    False — unchanged, down to the counters and the clamp. With the flag armed,
    settlement is by IDENTITY instead (:func:`_release_by_handle`):

    - ``tokens`` keeps its meaning and its POSITION — the total settled in this
      batch — so every existing caller and test is untouched by the new
      protocol's existence.
    - ``handles`` maps ``reservation_id`` -> tokens settled against that hold.
      Each named hold is dropped for its FULL reserved amount (see
      :data:`_RELEASE_BY_HANDLE_LUA`); the per-handle VALUE is used only to work
      out how much of ``tokens`` was attributed at all.
    - ``max(0, tokens - sum(handles.values()))`` is the unattributed remainder
      and it NEVER touches the pool. It is counted on
      ``billing_budget_release_without_reserve_total`` and dropped. ⭐ That one
      branch is the entire fix: the pool can no longer be reduced by a settlement
      that did not reserve, so no settlement can drain another caller's live
      hold, so ``billing_budget_reserve_over_release_total`` becomes structurally
      unreachable and its rate falling to zero is the acceptance measurement.

    ⚠️ ``handles`` IS STILL IGNORED WHILE THE FLAG IS FALSE, and that is now the
    deliberate point rather than a consequence. ⭐ **CORRECTED** — this paragraph
    used to justify it by claiming "no handle was ever minted in that mode and no
    index exists to look one up in". Both halves of that are false since the mint
    was separated from the release routing: :func:`reserve_budget` mints on every
    successful reserve and :data:`_RESERVE_SHADOW_LUA` keeps a real index. The
    handles arriving here in pooled mode are therefore genuine, and acting on them
    would still be WRONG, because it would change the settlement arithmetic — the
    one thing the flag exists to switch. Draining the tagged pool from here while
    the gate admits against the untagged one would settle against a pool nothing
    enforces and leave the enforcing pool inflated: a fail-CLOSED drift on every
    debit, arrived at by "helpfully" using data that was only meant to be counted.
    ⛔ The release branch is the ONLY thing left behind this flag. Keep it that
    way: the whole value of the separation is that arming flips exactly one
    structure.
    """
    if redis_conn is None:
        return 0
    if get_settings().budget_require_reservation_handle:
        return await _release_by_handle(redis_conn, tenant_id, tokens, handles or {})
    if tokens <= 0:
        return 0
    try:
        raw = await cast("Any", redis_conn.eval)(
            _RELEASE_LUA,
            1,
            reserved_key(tenant_id),
            str(int(tokens)),
        )
    except RedisError:
        # ⚠️ FAILS CLOSED, unlike its seed/debit siblings. An unreleased
        # reservation leaves `tokens:tenant:{id}:reserved` inflated, and the
        # reserve gate compares `consumed + reserved + estimate` against the
        # cap -- so the tenant is denied headroom it has actually settled. Left
        # unobserved this is the exact shape of the incident this function was
        # written to fix: `reserved` grew monotonically until every request
        # returned 429 while committed consumption was still ~0. The key's TTL
        # bounds it, but only to one TTL window of over-denial. Was
        # logger.debug with no metric.
        observability_metrics.budget_redis_op_failed_total.labels(operation="release").inc()
        logger.warning(
            "reserve_release_failed",
            tenant_id=tenant_id,
            note=(
                "settled reservation not released; `reserved` stays inflated "
                "and the budget gate will deny early until the key's TTL lapses"
            ),
            exc_info=True,
        )
        return 0
    remaining, overshoot, prior_pool = _split_release_result(raw)
    if overshoot > 0:
        if prior_pool == 0:
            # (a) There was no pool to take from. Benign, and the ORDINARY case
            # on the debit path, which settles every debit whether or not that
            # spend ever reserved. Counted under its own name so the critical
            # over-release series stays a genuine defect signal.
            observability_metrics.budget_release_without_reserve_total.inc()
            logger.info(
                "release_without_reserve",
                tenant_id=redact_tenant_id(tenant_id),
                released_tokens=int(tokens),
                overshoot_tokens=overshoot,
            )
        else:
            # (b) A live pool was drained past its end, so `overshoot` tokens
            # came out of OTHER callers' in-flight reservations and the cap
            # fails open by exactly that much. This is the one that pages.
            #
            # `prior_pool is None` lands here too, on purpose: a reply shape
            # that cannot say which class this was must NEVER be laundered into
            # the benign counter. Unknown fails towards the alarm.
            #
            # ⭐ THIS IS THE ONLY WRITER OF THIS COUNTER, AND IT IS ON THE POOLED
            # PATH ONLY. With `budget_require_reservation_handle` armed,
            # `release_reservation` returns through `_release_by_handle` before
            # reaching here, and that path cannot reduce the pool by more than the
            # holds it names — so the series becomes structurally unreachable and
            # its rate going to ZERO is the acceptance test for the flip, not a
            # sign that settlement stopped. ⛔ Do not add a second writer (the
            # identified path's own pool-underflow case deliberately logs instead)
            # or that measurement stops meaning anything.
            observability_metrics.budget_reserve_over_release_total.inc()
            # ⭐ Export the RATIO this alert's own escalation condition is written
            # against. Its annotation names "prior_pool / released < 0.5 across a
            # meaningful share of events" as the one falsifiable reason to take
            # this back to `critical`, then states the condition is unfirable
            # because neither term is exported as a series — both live only in the
            # structlog line below. This makes it expressible in PromQL.
            #
            # Purely additive: no pool arithmetic, no early return, and the counter
            # above keeps its single writer and its exact meaning.
            #
            # `prior_pool > 0` is guaranteed by this branch (the `== 0` case is the
            # benign sibling above) but `tokens` is guarded anyway — a zero
            # denominator would be a reply-shape bug and must not raise on a
            # settlement. `prior_pool is None` reaches this branch deliberately
            # (unknown fails towards the alarm) and is skipped here rather than
            # being charted as a 0.0, which would fake the escalation signal.
            if prior_pool is not None and int(tokens) > 0:
                observability_metrics.budget_reserve_estimate_ratio.observe(
                    prior_pool / int(tokens)
                )
            logger.warning(
                "reserve_over_release",
                tenant_id=redact_tenant_id(tenant_id),
                released_tokens=int(tokens),
                overshoot_tokens=overshoot,
                prior_pool_tokens=prior_pool,
            )
    return remaining


async def _release_by_handle(
    redis_conn: aioredis.Redis,
    tenant_id: str,
    tokens: int,
    handles: dict[str, int],
) -> int:
    """Settle NAMED reservations — the ``budget_require_reservation_handle`` path.

    Runs :data:`_RELEASE_BY_HANDLE_LUA` in ONE ``eval``, exactly like the pooled
    path, including when ``handles`` is empty: the script then performs a single
    ``GET`` and returns the pool, so the documented return contract (the remaining
    pool, as an int) holds on the unattributed path too without a second round
    trip. An empty-``handles`` call is the COMMON case today — the cost-flush
    settlement in :func:`services.budget_debit.release_budget_reservations` has no
    handle to pass until license-authz round-trips ``reservation_id`` — and in
    this mode it correctly moves nothing at all.

    Three populations, kept apart deliberately:

    - **Handles found and dropped** — counted on
      ``billing_budget_reservation_settled_total``, one per hold, not per token.
    - **Handles named but absent** — already settled (a duplicate cost event from
      a provider failover), or swept after expiry. A silent no-op by design; see
      the idempotency note on the script. Reported on the log line as
      ``stale_handles`` so a caller settling the same hold N times is visible
      without a metric that would alarm on normal failover.
    - **Unattributed tokens** — ``max(0, tokens - sum(handles.values()))``, which
      NEVER touches the pool. Counted on
      ``billing_budget_release_without_reserve_total``, the same series the pooled
      path uses for the same meaning (a settlement with no matching reservation),
      so the flip does not break that series' continuity.
    """
    if not handles and tokens <= 0:
        # Nothing named and nothing settled — no hold to discharge and no
        # unattributed remainder to count, so the round trip would buy only a
        # pool read that no caller on this path reads. 0 is the documented
        # "unknown" return, exactly as it is for the same case in the pooled path.
        return 0
    named = list(handles)
    attributed = sum(max(0, int(v)) for v in handles.values())
    unattributed = max(0, int(tokens) - attributed)
    try:
        raw = await cast("Any", redis_conn.eval)(
            _RELEASE_BY_HANDLE_LUA,
            3,
            _tagged_reserved_key(tenant_id),
            reservation_index_key(tenant_id),
            reservation_amounts_key(tenant_id),
            *named,
        )
    except RedisError:
        # ⚠️ Same direction as the pooled path — an undischarged hold keeps
        # denying the tenant headroom it has already settled — but bounded
        # differently and better: the hold carries its own expiry in the index, so
        # the next reserve's sweep reclaims it rather than the whole key having to
        # lapse. Still a warning, because between now and that sweep the gate is
        # denying real headroom.
        observability_metrics.budget_redis_op_failed_total.labels(operation="release").inc()
        logger.warning(
            "reserve_release_failed",
            tenant_id=tenant_id,
            handle_count=len(named),
            note=(
                "named reservations not discharged; the tenant's pool stays "
                "inflated until the next reserve's sweep reclaims them at expiry"
            ),
            exc_info=True,
        )
        return 0
    remaining, prior_pool, dropped, dropped_tokens = _split_handle_release_result(raw)
    if dropped > 0:
        observability_metrics.budget_reservation_settled_total.inc(dropped)
        logger.debug(
            "reservation_settled",
            tenant_id=redact_tenant_id(tenant_id),
            handles_dropped=dropped,
            tokens_dropped=dropped_tokens,
            remaining_pool_tokens=remaining,
            stale_handles=len(named) - dropped,
        )
    if dropped_tokens > prior_pool:
        # The pool held LESS than the holds it carried. This protocol cannot
        # create that state (every INCRBY is paired with an HSET under one EVAL);
        # it means the three keys expired unevenly or something outside this
        # module wrote the pool. ⛔ NOT counted on
        # `budget_reserve_over_release_total` — that series' value after the flip
        # is reading a flat zero, and feeding an unrelated defect into it would
        # re-arm a CRITICAL alert on a condition its name does not describe.
        logger.warning(
            "reservation_pool_underflow",
            tenant_id=redact_tenant_id(tenant_id),
            prior_pool_tokens=prior_pool,
            dropped_tokens=dropped_tokens,
            note=(
                "reservation pool was smaller than the holds it carried; the "
                "reserved/index/amount keys are inconsistent for this tenant"
            ),
        )
    if unattributed > 0:
        # ⭐ THE FIX, in one branch: these tokens belong to no hold, so they are
        # counted and DROPPED. Under the pooled protocol they were subtracted from
        # the shared integer, which is precisely how a settlement came to drain
        # other callers' live reservations.
        observability_metrics.budget_release_without_reserve_total.inc()
        logger.info(
            "release_without_reserve",
            tenant_id=redact_tenant_id(tenant_id),
            released_tokens=int(tokens),
            unattributed_tokens=unattributed,
            handle_count=len(named),
            note="unattributed settlement tokens left the reservation pool untouched",
        )
    return remaining


def _split_handle_release_result(raw: Any) -> tuple[int, int, int, int]:
    """Unpack :data:`_RELEASE_BY_HANDLE_LUA`'s 5-element reply.

    Returns ``(remaining, prior, handles_dropped, tokens_dropped)``. Tolerant in
    the same direction as :func:`_split_release_result` and for the same reason —
    a Redis holding an older cached script, or a half-rolled deploy, must degrade
    rather than raise inside a best-effort settlement — but the tolerance fails
    the OTHER way here, towards doing nothing: a reply that cannot say how many
    holds it dropped reports **zero** dropped, so the settled counter
    under-reports instead of inventing settlements that may not have happened.
    A missing ``prior`` reads as ``0``, which suppresses the underflow warning
    rather than manufacturing it.
    """
    if not isinstance(raw, list | tuple):
        return int(raw or 0), 0, 0, 0
    remaining = int(raw[0] or 0) if len(raw) > 0 else 0
    prior = int(raw[2] or 0) if len(raw) > 2 else 0
    dropped = int(raw[3] or 0) if len(raw) > 3 else 0
    dropped_tokens = int(raw[4] or 0) if len(raw) > 4 else 0
    return remaining, prior, dropped, dropped_tokens


def _split_release_result(raw: Any) -> tuple[int, int, int | None]:
    """Unpack :data:`_RELEASE_LUA`'s ``{remaining, overshoot, prior}`` reply.

    Tolerates the two older reply shapes so a rolling deploy (or a Redis that
    already has an older script cached) degrades rather than raising inside a
    best-effort settlement path:

    - a bare scalar (the original) -> ``(remaining, 0, None)``
    - a 2-element list (overshoot, no prior) -> ``(remaining, overshoot, None)``

    ``prior`` is ``None`` for BOTH, meaning "this reply cannot say which class
    the over-release was". The caller must treat that as the genuine defect,
    never as the benign one — an unknown that silently became benign would let
    a real fail-open stop paging during exactly the window (a rolling deploy)
    when the two versions are most likely to disagree.

    It also tolerates the LONGER reply of :data:`_RELEASE_BY_HANDLE_LUA`
    (``{remaining, 0, prior, handles_dropped, tokens_dropped}``) by reading the
    first three slots and ignoring the rest. That is safe rather than lucky: the
    identified script deliberately holds this exact positional contract in its
    first three slots, its slot 2 is structurally 0, and a 0 overshoot takes the
    caller down neither counting branch — so a mode-B reply reaching this reader
    (a mis-wired call site, or a future refactor) degrades to "nothing
    over-released", which is TRUE of that protocol, instead of misfiling a
    settlement. The two extra slots belong to
    :func:`_split_handle_release_result`.

    ⛔ Unknown shapes still fail towards the alarm: anything that is neither a
    scalar nor an indexable sequence of >= 1 elements reports ``prior=None``, and
    the caller routes ``None`` to the CRITICAL counter. Do not "tidy" that into a
    0 default — ``tests/test_money_bug_fixes.py`` pins it.
    """
    if isinstance(raw, list | tuple):
        remaining = int(raw[0] or 0) if len(raw) > 0 else 0
        overshoot = int(raw[1] or 0) if len(raw) > 1 else 0
        prior = int(raw[2] or 0) if len(raw) > 2 else None
        return remaining, overshoot, prior
    return int(raw or 0), 0, None


async def reserve_budget(
    session: AsyncSession,
    redis_conn: aioredis.Redis | None,
    tenant_id: str,
    estimated_tokens: int,
    ttl_seconds: int,
    license_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """Pre-flight token reservation. Atomic check + reserve via Redis Lua.

    Treats the tenant's active-budget pool as one bucket: cap = sum of
    ``budget_tokens`` across all active TokenBudget rows that match the
    optional ``license_id`` scope. Reservation succeeds when
    ``consumed_pool + reserved + estimated <= cap``; otherwise denied.

    Successful reservations get an ``EXPIRE`` so they auto-release. The
    actual cost is debited via the existing post-fact path
    (``debit_budget`` from ``aggregator.flush_costs``), which writes
    ``TokenBudget.consumed_tokens``; every reservation re-reads that committed
    spend from the DB, so accumulated consumption is always enforced (no cached
    Redis pool that can go stale between debits).

    Returns a dict shaped to populate ``BudgetReserveResponse``. On Redis
    failure or no active budget, returns ``allowed=False`` with a
    machine-readable ``reason``.

    ⭐ **A HANDLE IS MINTED ON EVERY SUCCESSFUL RESERVE, IN BOTH MODES, AND THE
    MONEY ARITHMETIC IS IDENTICAL EITHER WAY.** This used to be gated on
    ``budget_require_reservation_handle`` together with the settlement routing,
    which produced a measure-before-arming deadlock: handle coverage could not be
    observed until the flag was armed, and arming it flipped the mint, the index
    and the release semantics in one step. The flag now selects ONE thing — which
    script takes the ADMISSION VERDICT, and therefore which pool a settlement
    drains.

    - **Flag False (the default, and what runs in prod).** The verdict is taken by
      :data:`_RESERVE_LUA` against the untagged :func:`reserved_key`, with the
      same ARGV, the same ``cap >= 0`` test and the same set-TTL-on-creation-only
      semantics as before this change. An ADMITTED reserve is then additionally
      recorded in the identified protocol's hash-tagged keys by
      :data:`_RESERVE_SHADOW_LUA` (best-effort, its own ``eval``, no key the gate
      reads), and ``reservation_id`` carries that handle.
      :func:`release_reservation` still routes to :data:`_RELEASE_LUA` and still
      ignores ``handles``, so **settlement behaviour is byte-for-byte unchanged**.
    - **Flag True.** :data:`_RESERVE_HANDLE_LUA` takes the verdict against the
      tagged pool, the sweep runs inside it, and settlement routes to
      :func:`_release_by_handle`.

    A REFUSED reserve returns ``None`` in BOTH modes — nothing was held, so there
    is nothing to discharge, and echoing a handle for a reservation that does not
    exist is how a settlement comes to subtract tokens nobody reserved. So does a
    reserve whose shadow record was refused by Redis: ``reservation_id`` attests
    that a hold EXISTS, never that one was intended.

    ⭐ **WHAT AN OPERATOR SHOULD MEASURE BEFORE ARMING THE FLAG**, now that it can
    be measured at all:

    1. ``increase(billing_budget_reservation_minted_total[1h])`` should track
       successful reserves. It read a flat 0 in prod before this change because
       the increment lived inside ``if require_handle:`` — it measured the flag,
       not the traffic. A persistent shortfall now means the shadow write is
       failing (see ``reservation_shadow_record_failed``), and a handle that was
       not recorded is one a settlement will never be able to name.
    2. ``minted`` vs ``expired`` while the flag is OFF: they should be roughly
       equal with ``settled`` flat at 0, because nothing can name a shadow hold.
       That equality is the healthy pre-arm reading, not a stranding signal.
    3. ⛔ **THE ACTUAL GATE, AND IT IS NOT YET A SERIES.** Arming is safe for the
       CAP at any coverage — the per-reservation TTL bounds a hold that nobody
       settles (see the settings field) — but the thing worth knowing first is
       what share of settlements arrive naming a hold that exists, i.e. whether
       the producer round-trip (``POST /budget/reserve`` ->
       ``billing_cost_events.reservation_id`` -> ``handles``) works end to end at
       all. ``cost_flusher`` already builds that map and
       ``release_reservation`` already accepts it, so the wire is in place; what
       is missing is a counter on the POOLED release path splitting settlements
       into named/unnamed. Until that exists, read it from the
       ``reservation_id IS NOT NULL`` share of ``billing_cost_events`` over the
       same window, which is the same population one query earlier.
    """
    # Find active budgets, optionally filtered to one license. The scope rule
    # (this license + the shared tenant-level pool) lives in
    # _select_active_budgets and is now applied IN SQL rather than re-derived
    # here: the debit path used to have no scoping at all while this path had
    # its own hand-rolled copy, and that asymmetry is precisely how one
    # license's spend came to drain another's budget. Behaviourally identical —
    # TokenBudget.license_id is NOT NULL, so the old `is None` arm was dead.
    now = datetime.now(UTC)
    budgets = await _select_active_budgets(
        session, tenant_id, now, lock=False, license_id=license_id
    )
    if not budgets:
        return {
            "allowed": False,
            "tenant_id": tenant_id,
            "reserved_tokens": 0,
            "remaining_tokens": 0,
            "cap_tokens": 0,
            "consumed_tokens": 0,
            "in_flight_reserved_tokens": 0,
            "reason": "no_active_budget",
            "reservation_id": None,
        }

    cap = sum(int(b.budget_tokens or 0) for b in budgets)
    # effective_consumed, NOT consumed_tokens: a periodic budget whose stored
    # consumption belongs to a previous period has already reset (see
    # services/budget_period.py). Reading the raw column here would keep a
    # 'monthly' budget enforcing last month's spend forever.
    consumed_db = sum(effective_consumed(b, now) for b in budgets)

    if redis_conn is None:
        # Best-effort: enforce against DB-known consumed only (no atomic
        # protection across concurrent callers). License-authz fail-open
        # is the safety net. Log the degraded path so operators see it.
        logger.warning(
            "reserve_redis_unavailable_db_fallback",
            tenant_id=tenant_id,
            estimated_tokens=estimated_tokens,
        )
        over = consumed_db + estimated_tokens > cap
        return {
            "allowed": not over,
            "tenant_id": tenant_id,
            "reserved_tokens": 0 if over else estimated_tokens,
            "remaining_tokens": max(0, cap - consumed_db - (0 if over else estimated_tokens)),
            "cap_tokens": cap,
            "consumed_tokens": consumed_db,
            "in_flight_reserved_tokens": 0,
            "reason": "over_budget" if over else None,
            "reservation_id": None,
        }

    # `reserved` is the only value that needs to live in Redis (in-flight
    # reservations from concurrent callers, atomically summed in the Lua). The
    # committed spend `consumed_db` is read fresh from the DB above every call,
    # so it is always current — no cached pool key to go stale.
    reserve_key = reserved_key(tenant_id)
    # ONE read of the flag for the whole call: re-reading it between the eval and
    # the metrics could straddle a settings reload and mint a handle that the
    # counters then deny ever existed.
    #
    # ⭐ WHAT THIS FLAG STILL SELECTS, AND WHAT IT NO LONGER DOES. It used to gate
    # THREE things at once — minting the handle, maintaining the handle index, and
    # routing settlement through `_release_by_handle` — so handle coverage was
    # unmeasurable until the moment of arming, and arming flipped three structures
    # simultaneously. It now selects exactly ONE thing: WHICH SCRIPT TAKES THE
    # ADMISSION VERDICT, and therefore which pool a settlement drains. Minting and
    # the whole of the handle bookkeeping happen on EVERY reserve either way (see
    # `_RESERVE_SHADOW_LUA`), so the flip is now a one-structure change.
    require_handle = get_settings().budget_require_reservation_handle
    # ⛔ Minted in PYTHON, not in Lua. A script that generates its own identifier
    # is non-deterministic, which makes it unsafe to replicate and unsafe to
    # replay from the AOF.
    #
    # ⭐ MINTED UNCONDITIONALLY — this is the half of the deadlock that lived here.
    # It is still minted BEFORE the eval on both paths, and on both paths it is
    # the HOLD'S EXISTENCE that promotes it to a `reservation_id`: the script's
    # own echo when the flag is armed, the shadow write's success when it is not.
    # A handle is never handed out for a hold that was not created.
    #
    # ⚠️ CANONICAL DASHED FORM, not `uuid4().hex`, and that is load-bearing rather
    # than cosmetic. The round trip is: this string -> `BudgetReserveResponse.
    # reservation_id` -> the producer's cost event -> `cost_flusher._parse_uuid`
    # -> `billing_cost_events.reservation_id`, a Postgres `uuid` COLUMN -> read
    # back and re-rendered by `str(...)` in `cost_flusher` -> the `handles` map ->
    # `HDEL` against `reservation_amounts_key`. `str()` of a `uuid.UUID` is ALWAYS
    # dashed, so a 32-char hex mint comes back dashed and misses an index keyed
    # undashed: `_release_by_handle` would discharge exactly NOTHING, forever,
    # and the coverage measurement this mint exists to enable would read a
    # structural zero indistinguishable from "no producer sends a handle".
    # `uuid.UUID(hex=...)` accepts both spellings, so nothing that parses the
    # value cares; only the byte-equality `HDEL` does.
    handle = str(uuid.uuid4())
    try:
        # redis-py types `eval()` as `Awaitable[Any] | Any` because the same
        # method exists on the sync client. The async client always returns
        # an awaitable; cast the bound method to Any so ty doesn't flag the
        # await as a possible non-awaitable.
        if require_handle:
            raw = await cast("Any", redis_conn.eval)(
                _RESERVE_HANDLE_LUA,
                3,
                _tagged_reserved_key(tenant_id),
                reservation_index_key(tenant_id),
                reservation_amounts_key(tenant_id),
                str(consumed_db),
                str(estimated_tokens),
                str(cap),
                str(ttl_seconds),
                # The clock is Python's, for the same determinism reason as the
                # handle — and it is the SAME `now` the cap was derived from
                # above (`effective_consumed(b, now)`), so a handle's expiry and
                # the period boundary that admitted it cannot disagree.
                str(int(now.timestamp())),
                handle,
                str(_RESERVATION_SWEEP_LIMIT),
            )
        else:
            raw = await cast("Any", redis_conn.eval)(
                _RESERVE_LUA,
                1,
                reserve_key,
                str(consumed_db),
                str(estimated_tokens),
                str(cap),
                str(ttl_seconds),
            )
        result = json.loads(raw)
    except RedisError, json.JSONDecodeError:
        logger.warning("reserve_lua_failed", tenant_id=tenant_id, exc_info=True)
        return {
            "allowed": False,
            "tenant_id": tenant_id,
            "reserved_tokens": 0,
            "remaining_tokens": max(0, cap - consumed_db),
            "cap_tokens": cap,
            "consumed_tokens": consumed_db,
            "in_flight_reserved_tokens": 0,
            "reason": "redis_unavailable",
            "reservation_id": None,
        }

    consumed = int(result["consumed"])
    reserved = int(result["reserved"])
    cap_returned = int(result["cap"])
    allowed = bool(result["ok"])
    remaining = max(0, cap_returned - consumed - reserved)
    # ``reservation_id`` is evidence the hold EXISTS, never evidence we intended
    # one — and that rule is now enforced on BOTH paths by different witnesses:
    # the script's own echo when armed (present only on the branch that ran
    # ZADD/HSET/INCRBY), the shadow write's success when not.
    reservation_id: str | None = None
    expired = 0
    if require_handle:
        minted = result.get("handle")
        reservation_id = str(minted) if minted else None
        # The sweep ran on BOTH branches, so it is counted on both — a refusal is
        # exactly when the size of the reclaimed backlog matters most, and
        # counting it only on success would hide a tenant whose pool is held up by
        # stale holds while it is being denied.
        expired = int(result.get("expired") or 0)
    elif allowed:
        # ⭐ THE POOLED PATH MINTS TOO, AND MOVES NO MONEY DOING IT. The verdict
        # above was taken by `_RESERVE_LUA` against the UNTAGGED pool and is
        # already final and already returned-shaped; this only records the
        # admitted hold in the identified protocol's own hash-tagged keys, so that
        # handle coverage is measurable BEFORE the flag is armed instead of only
        # by arming it. `_RESERVE_SHADOW_LUA` documents why none of it can reach
        # the key the gate reads.
        #
        # ⛔ ONLY ON `allowed`. A refused reserve created no hold, exactly as on
        # the armed path, and recording one would mean minting an identity for
        # tokens the gate declined — a settlement could then name it and reduce a
        # pool for spend that was never admitted.
        recorded, expired = await _record_reservation_shadow(
            redis_conn,
            tenant_id,
            handle=handle,
            tokens=estimated_tokens,
            # The SAME `now` the cap was derived from above
            # (`effective_consumed(b, now)`), for the same reason the armed path
            # passes it: a hold's expiry and the period boundary that admitted it
            # must not be able to disagree.
            now_unix=int(now.timestamp()),
            ttl_seconds=ttl_seconds,
        )
        reservation_id = handle if recorded else None
    if expired > 0:
        # ⚠️ ONE SERIES, ONE MEANING, in both modes: a hold lapsed without any
        # settlement naming it, and the sweep returned its tokens to whichever
        # pool that mode's holds live in. In the pooled mode that is TRUE OF
        # EVERY MINT — nothing can name a shadow hold, because settlement still
        # routes to `_RELEASE_LUA` — so `minted ≈ expired` with `settled == 0` is
        # the CORRECT pre-arm reading, not a stranding signal. It is also what
        # keeps the three-series handle budget (`minted == settled + expired`)
        # closed while the flag is off; suppressing it here would open a gap that
        # the metric's own comment tells operators to read as headroom held
        # forever.
        observability_metrics.budget_reservation_expired_total.inc(expired)
        logger.info(
            "reservation_sweep_reclaimed",
            tenant_id=redact_tenant_id(tenant_id),
            handles_reclaimed=expired,
            sweep_limit=_RESERVATION_SWEEP_LIMIT,
            # The one field that tells a log reader which pool was credited. The
            # shadow pool enforces nothing yet; the armed one is the gate's own.
            mode="handle" if require_handle else "shadow",
            note=(
                "expired reservations returned to the pool; a count at the "
                "sweep limit means more remain and the next reserve continues"
            ),
        )
    if reservation_id is not None:
        # ⭐ COUNTS REAL MINTS, NOT THE FLAG. This increment used to sit inside
        # `if require_handle:`, so the series measured whether the flag was on and
        # read a flat 0 in production no matter how much traffic reserved — which
        # is precisely why nobody could size the handle population before arming.
        # It now fires once per reserve that actually created a hold, in either
        # mode, and `reservation_id is not None` is the guard that keeps it
        # attached to a hold that exists rather than to an intention.
        observability_metrics.budget_reservation_minted_total.inc()
    return {
        "allowed": allowed,
        "tenant_id": tenant_id,
        "reserved_tokens": estimated_tokens if allowed else 0,
        "remaining_tokens": remaining,
        "cap_tokens": cap_returned,
        "consumed_tokens": consumed,
        "in_flight_reserved_tokens": reserved,
        "reason": None if allowed else "over_budget",
        "reservation_id": reservation_id,
    }
