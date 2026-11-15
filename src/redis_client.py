"""Redis async connection pool for streams and counters, plus a supervisor.

── WHY THERE IS A SUPERVISOR (fr#947/#978, ported 2026-09-06) ──────────────
`fr` and `kag-frontend/kag` served ZERO endpoints for 2h45m (01:59:58Z →
04:44Z). Measured, not reasoned:

  * A control-plane blip reset every long-lived TCP connection.
  * Postgres recovered on its own — the pool re-dialled.
  * The Redis client did NOT. Both pods logged a connection failure every
    ~10s for 2.5h (that cadence was the readiness probe, not a retry loop).
  * DECISIVE: a fresh TCP connect + AUTH + PING issued from INSIDE each
    wedged container answered `+PONG` in 4-8ms. Mesh healthy, Redis healthy,
    socket available — and the client in the process still could not use it.
  * `/health/live` kept returning 200, so kubelet never restarted anything.
    A human had to notice.

`billing-service` carried BOTH halves of that defect on the revenue path:

  1. `main.readiness()` returned 503 unless Redis answered — and with
     `replicas: 2` sharing ONE single-replica Redis Deployment, that is not a
     per-pod condition at all. It is a correlated fleet-wide kill switch with
     nothing left to route to.
  2. `init_redis()`'s `except` set `_redis = None`, and NOTHING ever
     recreated it. `init_redis` is called exactly once, from `main.lifespan`.
     So one Redis blip removed Redis from this process permanently, and the
     static `/health/live` guaranteed no restart would clear it.

Half 1 is fixed in `main.readiness()` (Redis is a degraded, non-fatal field;
Postgres stays fatal). Half 2 is fixed here.

── THE LEVER, AND THE ONE fr EXPLICITLY REJECTS ───────────────────────────
fr's `redis-connection-options.ts` rejects "discard the cached client on a
disconnect event" as a client LEAK: the disconnect fires while the client is
still retrying, so discarding on it builds a second client racing the first,
and another per disconnect.

This is not that. The rebuild is gated on TIME and on OBSERVED FAILURE, not
on an event:

  * A failed command starts a clock (`_failing_since`). It discards nothing.
  * A SUCCESSFUL command clears the clock, so this stays behavioural rather
    than a one-way latch a single old error could leave armed forever.
  * A rebuild happens only when a command has actually FAILED *and* the
    client has been continuously unusable for longer than the grace window
    (60s) — i.e. long past the point where redis-py's own reconnect would
    have succeeded if it were still running.
  * `_last_rebuild_at` floors the rate at one rebuild per window, so a Redis
    that is genuinely gone produces a slow trickle, not a storm.
  * The old client is `aclose()`d — disposed, not leaked.
  * The rebuild is single-flight: N concurrent failures produce ONE client.

A blip that redis-py heals inside the window therefore rebuilds NOTHING.
That window is exactly what buys immunity from the leak fr rejects.

Detection is deliberately NOT based on any redis-py internal state flag. It
is behavioural — "a command failed AND we have been down too long" — because
the failure being fixed is precisely that the client's own notion of its
state stopped tracking reality. fr's earlier attempt raised
`maxRetries` to 2_147_483_647 and still lost 2h45m: a retry BUDGET only
binds a process that is still spending, and this client had stopped.

── WHAT DRIVES IT ─────────────────────────────────────────────────────────
`check_redis()`. The readinessProbe calls it every 10s (`periodSeconds: 10`)
whether or not the service is taking traffic, so it is a livelier driver than
any request path. The two halves reinforce each other: readiness no longer
evicts the pod for a Redis fault, so the pod stays up to run the probe that
heals it.

── ⚠️ KNOWN RESIDUAL, deliberately out of scope. IT HAS TWO HALVES ────────
A rebuild swaps the module-global `_redis`, so every call site that resolves
the client per-use through `get_redis()` — every HTTP handler in `routers/`,
including the `Depends(get_redis)` ones — heals automatically. What does not:

**(a) The background loops hold a boot-captured client.** `main.lifespan`
resolves `redis_conn` once (main.py:143) and passes that object to
`run_flush_loop` and `run_rollup_loop`, each of which holds it for the life
of the process. A rebuild replaces `_redis`; it cannot reach a reference already handed out. Closing this means re-resolving
through `get_redis()` inside each loop's `while not stop_event.is_set():`.

**(b) Worse, and found while writing (a): the loops may never START.** The
whole block is gated `if redis_conn:` (main.py:169) with an
`else: logger.warning("redis_unavailable_background_tasks_skipped")`
(main.py:293). So a pod that boots while Redis is down runs NO flush loop, NO
rollup and NO budget watcher — for its entire life, because
`init_redis` is never called again. The supervisor below repairs the client;
it does not un-skip tasks that were never created.

⚠️ INTERACTION WITH THE READINESS CHANGE, stated plainly so it is not a
surprise. Before, such a pod 503'd forever and sat out of rotation. Now it is
Ready and serves HTTP — which is a strict improvement for the request path
(the durable writes proceed without Redis by design) but it means a pod with no background work stays in
the endpoints. That is why Redis being unhealthy is NOT silent here: it is
named in `degraded[]` and WARN-logged on every probe, so the condition is
alertable rather than invisible. Fixing (a) and (b) properly means
restructuring `lifespan` to start the loops unconditionally and let each one
resolve Redis per iteration — a change to four revenue-path modules that
deserves its own PR and its own tests, not a rider on this one.
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from typing import TYPE_CHECKING

import redis.asyncio as aioredis
import structlog
from redis.exceptions import RedisError

if TYPE_CHECKING:
    from settings import Settings

logger = structlog.stdlib.get_logger(__name__)

#: How long a client may stay continuously unusable before a failing command
#: is read as terminal rather than as in-flight recovery. 60s, because
#: redis-py's reconnect plus a 5s connect timeout puts an honest recovery well
#: inside it, while the production wedge sat in this state for 9,900s.
REDIS_TERMINAL_GRACE_S = 60.0

#: Ceiling for a single supervised command. `socket_timeout=5` already bounds
#: the socket, but a client wedged ABOVE the socket is the exact failure being
#: fixed: if the command never settles, the terminal check is never reached
#: and the client can never be rebuilt — the wedge again, one level up.
REDIS_OP_TIMEOUT_S = 10.0

#: Hard floor on how often a client may be rebuilt, independent of the grace
#: window. The rate floor in `_is_terminal` is expressed in the grace window,
#: so a zero window would remove the floor along with the window. This keeps
#: it. It never blocks the FIRST rebuild (`_last_rebuild_at` starts at 0.0, and
#: `time.monotonic()` is far above it), so a collapsed window still heals
#: instantly; it only stops the Nth rebuild in the same instant.
MIN_REBUILD_INTERVAL_S = 0.05

_redis: aioredis.Redis | None = None

# Supervisor state.
_settings: Settings | None = None
_failing_since: float | None = None
_last_rebuild_at: float = 0.0
_rebuild_count: int = 0
_rebuild_lock: asyncio.Lock | None = None
_rebuild_lock_loop: asyncio.AbstractEventLoop | None = None

#: Injected clock. Monotonic so a wall-clock step cannot fake or defer a
#: terminal state. Tests substitute it.
_now = time.monotonic


def grace_from_env(raw: str | None) -> float:
    """Grace window from the environment, or the default.

    ⚠️ `float("")` raises but a naive `float(raw or 0)` reads an EMPTY env var
    as a ZERO grace window, and `REDIS_TERMINAL_GRACE_S: ""` is an ordinary
    ConfigMap shape. A zero window makes the rate floor in `_is_terminal`
    always true, so EVERY failing command would build a new client — exactly
    the storm fr's `redis-connection-options.ts` rejects, arrived at by an
    empty string.

    Two separate guards, because they answer different questions: this one
    stops an ACCIDENTALLY empty variable from configuring a zero window, and
    :data:`MIN_REBUILD_INTERVAL_S` stops any zero window — however it arrives
    — from turning into a storm. An explicit "0" IS honoured; a test collapses
    the window that way.
    """
    if raw is None or raw.strip() == "":
        return REDIS_TERMINAL_GRACE_S
    try:
        value = float(raw)
    except ValueError:
        return REDIS_TERMINAL_GRACE_S
    if not math.isfinite(value) or value < 0:
        return REDIS_TERMINAL_GRACE_S
    return value


def _grace_s() -> float:
    return grace_from_env(os.environ.get("REDIS_TERMINAL_GRACE_S"))


def _get_rebuild_lock() -> asyncio.Lock:
    """Single-flight lock, bound lazily to the running loop.

    An `asyncio.Lock` built at import time attaches to whichever loop first
    awaits it; a second loop (every async test gets its own) then raises. So
    the lock is rebuilt whenever the running loop changes.
    """
    global _rebuild_lock, _rebuild_lock_loop  # noqa: PLW0603
    loop = asyncio.get_running_loop()
    if _rebuild_lock is None or _rebuild_lock_loop is not loop:
        _rebuild_lock = asyncio.Lock()
        _rebuild_lock_loop = loop
    return _rebuild_lock


def _build_client(settings: Settings) -> aioredis.Redis:
    """Construct a client. Lazy — `from_url` performs no I/O."""
    return aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
    )


def _reset_supervisor_state() -> None:
    global _failing_since, _last_rebuild_at, _rebuild_count  # noqa: PLW0603
    _failing_since = None
    _last_rebuild_at = 0.0
    _rebuild_count = 0


async def init_redis(settings: Settings) -> aioredis.Redis | None:
    """Create and test the Redis connection.

    A failure here is no longer permanent: the settings are retained so the
    supervisor can build a client later. Before this, `_redis = None` in the
    `except` below was terminal for the life of the process — `init_redis` is
    called exactly once, from `main.lifespan`.
    """
    global _redis, _settings, _failing_since  # noqa: PLW0603
    _settings = settings
    _reset_supervisor_state()
    try:
        _redis = _build_client(settings)
        await _redis.ping()
        logger.info("redis_connected", url=settings.redis_url.split("@")[-1])
        return _redis
    except RedisError, OSError, TimeoutError:
        logger.warning("redis_unavailable", url=settings.redis_url.split("@")[-1], exc_info=True)
        _redis = None
        # Start the clock at boot so the supervisor can heal a Redis that was
        # simply not up yet when this process started.
        _failing_since = _now()
        return None


def get_redis() -> aioredis.Redis | None:
    """Return the cached Redis connection.

    Resolve through this at USE time rather than caching the return value: a
    supervised rebuild replaces the object, and a caller holding the old one
    keeps talking to a dead client.
    """
    return _redis


async def close_redis() -> None:
    """Close the Redis connection."""
    global _redis  # noqa: PLW0603
    if _redis is not None:
        await _redis.aclose()
        _redis = None
    _reset_supervisor_state()


def _is_terminal(now: float) -> bool:
    """True when the client has stopped recovering on its own.

    Both clauses matter. The first is the actual signal; the second floors the
    rebuild rate at one per window, so a Redis that is genuinely gone does not
    spawn a client per failed command.
    """
    if _failing_since is None:
        return False
    grace = _grace_s()
    return now - _failing_since >= grace and now - _last_rebuild_at >= max(
        grace, MIN_REBUILD_INTERVAL_S
    )


async def _rebuild(generation: int) -> bool:
    """Replace the live client. Single-flight; the old client is disposed.

    `generation` is `_rebuild_count` read BEFORE the failing command. Under
    concurrency the first failure rebuilds and every later one would otherwise
    build a client of its own; the generation says "the client I failed
    against is not the client that is live now", which is a retry, not a
    second rebuild.
    """
    global _redis, _failing_since, _last_rebuild_at, _rebuild_count  # noqa: PLW0603
    async with _get_rebuild_lock():
        if _rebuild_count > generation:
            return True  # someone else already replaced it while we waited
        if _settings is None:
            # Never initialised; there is nothing to rebuild from. Floor the
            # rate anyway so this cannot become a hot loop.
            _last_rebuild_at = _now()
            return False
        previous = _redis
        try:
            nxt = _build_client(_settings)
        except RedisError, OSError, ValueError:
            logger.warning("redis_rebuild_failed", exc_info=True)
            _last_rebuild_at = _now()
            return False
        _redis = nxt
        _failing_since = None
        _last_rebuild_at = _now()
        _rebuild_count += 1
        logger.warning(
            "redis_client_rebuilt",
            reason="client stopped recovering past the grace window",
            grace_s=_grace_s(),
            rebuild=_rebuild_count,
            ref="fr#947/#978",
        )
        if previous is not None:
            try:
                await previous.aclose()
            except Exception:  # noqa: BLE001 - a terminal client may throw on close; it is going away regardless
                logger.debug("redis_old_client_close_failed", exc_info=True)
        return True


async def _ping(client: aioredis.Redis) -> None:
    """One PING with a ceiling. A timeout is a failure like any other."""
    await asyncio.wait_for(client.ping(), timeout=REDIS_OP_TIMEOUT_S)


async def check_redis() -> bool:
    """Check if Redis is reachable, and supervise the client while doing it.

    Returns True/False; it never raises. Beyond the old behaviour it drives
    the supervisor: a failed PING starts (or continues) the failure clock, a
    successful one clears it, and once the clock passes the grace window the
    client is rebuilt and the PING retried ONCE against the fresh one.
    """
    global _failing_since  # noqa: PLW0603

    generation = _rebuild_count
    client = _redis
    if client is not None:
        try:
            await _ping(client)
            # A command that SUCCEEDS is the only honest evidence the client
            # works. Clearing here keeps the clock behavioural.
            _failing_since = None
            return True
        except RedisError, OSError, TimeoutError:
            if _failing_since is None:
                _failing_since = _now()

    if _failing_since is None:
        # No client and no clock (never initialised, or closed). Start it so a
        # later tick can cross the window rather than sitting dead forever.
        _failing_since = _now()

    if _rebuild_count > generation:
        # A concurrent caller already rebuilt; retry against the fresh client.
        return await _retry_after_rebuild()

    if not _is_terminal(_now()):
        return False

    if not await _rebuild(generation):
        return False

    return await _retry_after_rebuild()


async def _retry_after_rebuild() -> bool:
    global _failing_since  # noqa: PLW0603
    client = _redis
    if client is None:
        return False
    try:
        await _ping(client)
    except RedisError, OSError, TimeoutError:
        if _failing_since is None:
            _failing_since = _now()
        return False
    _failing_since = None
    return True


def redis_supervisor_stats() -> dict[str, float | int | None]:
    """Observability + test assertions."""
    return {
        "rebuilds": _rebuild_count,
        "failing_since": _failing_since,
        "grace_s": _grace_s(),
    }
