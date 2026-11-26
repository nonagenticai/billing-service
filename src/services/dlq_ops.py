"""Inspect and safely replay the ``billing:costs:dead`` dead-letter stream.

The cost consumer quarantines malformed cost events to a Redis dead-letter
stream instead of billing them at $0 (see
:func:`services.cost_flusher.flush_costs` and :func:`services._cost_schema.validate_cost_event`).
The BillingCostDeadLetter alert runbook tells an operator to look at that stream
and, once the root cause is fixed (typically a provider that was missing on the
wire), replay the entries back onto the live ``billing:costs`` stream. This
module is the tool the runbook assumes exists.

Three operations, all driven from ``src/routers/admin.py``:

* :func:`inspect_dlq` — read-only page of the dead stream, split into business
  fields vs the ``_dlq_*`` bookkeeping the flusher stamped on quarantine.
* :func:`replay_dlq` — for each requested id: reverse the quarantine (strip the
  ``_dlq_*`` / internal keys), RE-VALIDATE, and only re-inject onto
  ``billing:costs`` if it now passes. Three guards make this safe to run from a
  runbook under pressure:

  1. **Idempotency.** A Redis marker (``SET NX``) keyed on the ORIGINAL event id
     (``dlq:replayed:orig:{_dlq_original_id}``, falling back to
     ``dlq:replayed:{entry_id}`` for legacy entries that predate the stamp)
     claims the entry before any XADD, so a double-submit, an operator hitting
     the button twice, OR the same source event quarantined twice under two dead
     ids cannot double-bill. A claimed-but-not-yet-replayed id is reported as
     ``already_processed``. The claim is taken with a finite TTL so an
     abandoned replay self-heals, and is ``PERSIST``ed ATOMICALLY WITH the
     re-inject — the guard may never expire while a re-drainable entry can
     still exist, and there is no round-trip in between for an interruption to
     land in. See :func:`_replay_marker`, :data:`_REINJECT_LUA` and
     :data:`_REPLAY_MARKER_TTL_FLOOR_SECONDS`.
  2. **Infinite-loop guard.** If the cleaned payload STILL fails
     ``validate_cost_event`` (e.g. the operator replayed before actually fixing
     the emitter), the entry is left on the dead stream, reported in
     ``still_malformed``, and its idempotency marker is *released* — so a later,
     genuine fix can retry it. It is never re-injected, so a malformed event can
     never loop dead -> live -> dead forever.
  3. **Per-id isolation.** Each id runs in its own try/except; one failure never
     aborts the rest of the batch.

* :func:`discard_dlq` — PERMANENTLY drop entries that no replay can ever drain.
  Dry-run by default; see that function's docstring for the deletion principle it
  excepts and the two conditions an entry must meet before it can be dropped.
* :func:`reattribute_dlq` — the operator's third move for an entry whose stored
  tenant is nil or blank (#243): rewrite ``tenant_id`` to an explicit,
  registered tenant, stamp the provenance, and re-inject through replay's own
  marker claim, validation and :data:`_REINJECT_LUA`. Dry-run by default; a
  Postgres receipt (``billing_dlq_reattributions``) is committed BEFORE the XDEL.

The inbox (``processed_cost_messages``) keys on the Redis *stream_msg_id*, and a
replay produces a brand-new msg id, so the inbox would NOT dedupe a replay — the
Redis marker above, keyed on the ORIGINAL event id, is the only replay-idempotency
barrier.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, cast

import structlog

from models.db import DlqReattribution
from models.domain import (
    DlqDiscardResult,
    DlqEntry,
    DlqInspectResponse,
    DlqReattributeResult,
    DlqReplayResult,
)
from observability import metrics as observability_metrics
from services._cost_schema import validate_cost_event
from settings import get_settings
from tenancy import NIL_TENANT_ID, canonical_tenant_id, is_registered_tenant_id

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.stdlib.get_logger(__name__)

# A page bigger than this hits a lot of Redis at once and returns an unwieldy
# response; the runbook drains in bounded passes, so cap defensively.
_INSPECT_LIMIT_CAP = 500
# Floor for the idempotency marker TTL while a replay is IN PROGRESS. The
# effective claim TTL is max(this, dead_letter_stream_max_age_hours*3600).
#
# ⚠️ A TTL — any TTL — is NOT sufficient on its own, and the reasoning this
# constant used to carry was wrong. It assumed "once the entry ages out of
# :dead there is nothing left to re-claim", i.e. that the entry's lifetime is
# bounded by ``dead_letter_stream_max_age_hours``. It is not. The only age bound
# on ``billing:costs:dead`` is ``rollup_engine.trim_stream``'s
# ``XTRIM MINID ~ <cutoff>``, and ``~`` lets Redis stop at macro-node
# granularity, so that bound is CONDITIONAL and cannot be relied on.
#
# ⚠️ CORRECTED 2026-09-14 — this used to say ``~`` "removes NOTHING, however old
# the entries are" on a single still-open node. That is FALSE, and the direction
# matters: it understates how readily the trim fires. Measured on redis 7.4.11,
# stream-node-max-entries=100, one node (radix-tree-keys=1) in both arms:
#
#   14 entries, ALL below the cutoff   -> XTRIM MINID ~  removed 14  (XLEN 14->0)
#   13 below + 1 above the cutoff      -> XTRIM MINID ~  removed  0  (XLEN 14->14)
#                                      -> XTRIM MINID    removed 13  (XLEN 14->1)
#
# THE REAL RULE: approximate MINID deletes a whole macro node once that node's
# LAST entry is below the cutoff — open or closed, node size irrelevant. It
# returns 0 only for a PARTIALLY aged node, i.e. while entries newer than the
# cutoff keep the node's tail young. Exact MINID has no such floor.
#
# So an entry's lifetime is bounded only when its node ages out WHOLE, which
# depends on the arrival pattern of unrelated later quarantines — not on
# anything this module controls. That is why the TTL reasoning below still does
# not hold, and the invariant is unchanged. (Concretely: all 14 entries live on
# 2026-09-14 were created 2026-09-02, so they cross 30 days together, the node
# will be fully aged, and the trim WILL fire ~2026-10-02.)
#
# So in ``delete_on_success=False`` mode the entry can outlive the
# 30d marker, a later drain re-claims it, re-``XADD``s it, and the inbox
# (``processed_cost_messages``, keyed on the NEW stream_msg_id) cannot dedupe
# it: the tenant is billed twice, permanently, with no refund path.
#
# INVARIANT CHOSEN: **the dedupe key's lifetime must be >= the entry's.** The
# TTL below is only the crash-safety window for a claim that has not yet
# durably re-injected (a claim abandoned mid-flight must self-heal, or a
# genuinely un-replayed event is silently dropped to $0). The moment a re-inject
# lands the marker is ``PERSIST``ed — no expiry — so it can never
# outlive-by-less-than the thing it guards. That ``PERSIST`` is ATOMIC with the
# re-inject (:data:`_REINJECT_LUA`); see there for why a second round-trip was
# not good enough. The alternative invariant (force the entry to be removed
# whenever the marker is written) was rejected: it would delete under
# ``delete_on_success=False``, whose whole point is to keep the quarantine
# record for audit.
_REPLAY_MARKER_TTL_FLOOR_SECONDS = 86400

# Re-inject the cleaned payload AND make its replay marker permanent, in ONE
# atomic script.
#
# ⚠️ THIS MUST NOT BE TWO ROUND-TRIPS. It was, and that made the invariant above
# nearly-true instead of true: `XADD` landed, then a separate `PERSIST` did not,
# leaving the marker on its finite claim TTL while the event was already billed
# and the dead entry was still re-drainable. The marker expires, a later drain
# re-claims and re-`XADD`s, and the inbox (keyed on the NEW stream_msg_id)
# cannot dedupe it — billed twice, permanently, with no refund path.
#
# The window is ~1 RTT, but the fuse is 30 days, and it is NOT only a crash:
# `replay_dlq` is awaited inside a FastAPI admin handler, and `CancelledError`
# is a **BaseException**, so a client disconnect or gateway timeout between the
# two calls unwinds straight past the `except Exception` recovery below without
# any process dying. Redis runs a script to completion, so folding both commands
# into one makes the guard's durability inseparable from the re-injection: after
# this returns, the event is on the stream AND its marker has no expiry; if it
# does not run, neither happened and the claim TTL correctly lets a retry
# re-attempt.
#
# KEYS[1] = costs stream, KEYS[2] = replay marker
# ARGV    = flattened field/value pairs of the cleaned payload
# Returns: the new stream id.
_REINJECT_LUA = """
local unpack = unpack or table.unpack
local args = {KEYS[1], '*'}
for i = 1, #ARGV do
  args[#args + 1] = ARGV[i]
end
local id = redis.call('XADD', unpack(args))
redis.call('PERSIST', KEYS[2])
return id
"""

# The ONLY cost a discard may drop. Compared as a Decimal, so "0", "0.00" and
# "0.000000" all match while "0.0000001" does not — a string compare against
# "0.00" would silently admit the others' sibling forms or reject the real ones.
_DISCARDABLE_COST_USD = Decimal("0")


def _business_fields(fields: dict[str, str]) -> dict[str, str]:
    """Reverse the quarantine: the flusher stamps ``_dlq_reason``/``_dlq_original_id``/
    ``_dlq_at`` onto a payload that had every leading-underscore internal key
    (``_msg_id``/``_stream_name``) already stripped. Dropping ALL leading-underscore
    keys therefore yields exactly the original wire payload."""
    return {k: v for k, v in fields.items() if not k.startswith("_")}


def _replay_marker(entry_id: str, fields: dict[str, str]) -> str:
    """Idempotency-marker key for one dead-letter entry — keyed on the EVENT.

    Keying on the dead-stream ENTRY id (the old behaviour) deduped the wrong
    thing. ``cost_flusher.flush_costs`` ``XADD``s the quarantine record BEFORE it
    commits the inbox row, so a crash (OOM-kill, pod eviction) in that window
    lets the SAME source event be re-consumed and quarantined a SECOND time
    under a brand-new dead id. Two dead ids -> two distinct entry-id markers ->
    replaying both re-injects the event twice and the tenant is billed twice.
    That is the exact failure this marker exists to prevent, and the flusher's
    own comment already claimed ``_dlq_original_id`` was what guarded it.

    ``_dlq_original_id`` is the id the event carried on ``billing:costs``, which
    is stable across re-quarantine, so it is the correct dedup key. Entries
    written before the flusher stamped it fall back to the dead-stream entry id
    (unchanged legacy behaviour — no worse than before, and those entries can
    only ever have one copy anyway).

    The two forms live in SEPARATE key namespaces: both are Redis stream ids of
    the same ``<ms>-<seq>`` shape but come from different streams, so a shared
    prefix could collide an original id with an unrelated dead entry id and
    silently drop a real event.
    """
    original_id = fields.get("_dlq_original_id")
    if original_id:
        return f"dlq:replayed:orig:{original_id}"
    return f"dlq:replayed:{entry_id}"


async def inspect_dlq(
    redis_conn: aioredis.Redis,
    *,
    reason: str | None = None,
    limit: int = 100,
) -> DlqInspectResponse:
    """Return a page of dead-letter entries, newest-oldest by stream order.

    ``limit`` is clamped to ``[1, 500]``. When ``reason`` is given, only entries
    whose ``_dlq_reason`` matches are returned (the filter is applied after the
    fetch, so ``count`` reflects the filtered set while ``has_more`` reflects
    whether the underlying stream fetch hit the cap).
    """
    settings = get_settings()
    stream = settings.cost_dead_letter_stream
    capped = min(max(limit, 1), _INSPECT_LIMIT_CAP)

    raw = await cast("Any", redis_conn.xrange)(stream, count=capped)

    entries: list[DlqEntry] = []
    for entry_id, fields in raw:
        entry_reason = fields.get("_dlq_reason")
        if reason is not None and entry_reason != reason:
            continue
        entries.append(
            DlqEntry(
                id=entry_id,
                reason=entry_reason,
                dlq_at=fields.get("_dlq_at"),
                original_id=fields.get("_dlq_original_id"),
                fields=_business_fields(fields),
            )
        )

    # has_more reflects the raw stream depth, not the reason-filtered view: the
    # page returns the oldest `capped` entries, so more remain iff the stream
    # holds strictly more than one page. XLEN keeps this truthful at an exactly
    # full page — len(raw) == capped does NOT imply anything lies beyond it, so
    # the fetch count alone would be a false positive (billing:costs:dead with
    # exactly `capped` entries would wrongly report has_more=True).
    total = await cast("Any", redis_conn.xlen)(stream)
    has_more = total > capped
    return DlqInspectResponse(entries=entries, count=len(entries), has_more=has_more)


async def replay_dlq(
    redis_conn: aioredis.Redis,
    *,
    ids: list[str],
    delete_on_success: bool = True,
) -> DlqReplayResult:
    """Replay the given dead-letter ids back onto ``billing:costs``.

    See the module docstring for the three safety guards. Returns the aggregated
    outcome; a caller is expected to audit-log the result.
    """
    settings = get_settings()
    dead_stream = settings.cost_dead_letter_stream
    costs_stream = settings.costs_stream
    # The marker must outlive the dead entry it guards (see the floor constant).
    marker_ttl = max(
        _REPLAY_MARKER_TTL_FLOOR_SECONDS,
        settings.dead_letter_stream_max_age_hours * 3600,
    )

    replayed: list[str] = []
    already_processed: list[str] = []
    still_malformed: list[dict[str, str]] = []
    deleted_from_dlq: list[str] = []

    for entry_id in ids:
        # The marker is computed AFTER the fetch, from the ORIGINAL event id —
        # see _replay_marker. Bound here only so the except block can release it.
        marker = ""
        claimed_here = False
        injected = False
        try:
            # (a) Fetch the single entry. Gone already (drained by a prior run) =>
            # nothing to do; it belongs to none of the result buckets.
            found = await cast("Any", redis_conn.xrange)(
                dead_stream, min=entry_id, max=entry_id, count=1
            )
            if not found:
                continue
            _, fields = found[0]
            marker = _replay_marker(entry_id, fields)

            # (b) Idempotency claim BEFORE any XADD. NX fails (returns falsy) if a
            # prior replay already claimed this EVENT -> report, don't re-inject.
            claimed = await redis_conn.set(marker, "1", nx=True, ex=marker_ttl)
            if not claimed:
                already_processed.append(entry_id)
                continue
            claimed_here = True

            # (c) Reverse the quarantine.
            cleaned = _business_fields(fields)

            # (d) Re-validate. STILL malformed => leave on the dead stream, release
            # the marker so a real fix can retry, and NEVER re-inject. This is the
            # infinite-loop guard.
            malformed_reason = validate_cost_event(cleaned)
            if malformed_reason is not None:
                still_malformed.append({"id": entry_id, "reason": malformed_reason})
                await redis_conn.delete(marker)
                claimed_here = False
                continue

            # (e) Re-inject onto the live cost stream AND make the marker
            # permanent, atomically. Once the event is durably back on
            # billing:costs the marker stops being a crash-recovery claim and
            # becomes the permanent record that this EVENT has been replayed:
            # from here it is the only thing between a surviving dead entry and
            # a second billing, and no expiry can be justified, because nothing
            # bounds how long such an entry survives (approximate XTRIM does not
            # trim a small stream) and a re-quarantined copy under a DIFFERENT
            # dead id may exist that this call knows nothing about. Unconditional
            # — deleting THIS entry does not prove there is no other copy.
            #
            # Both halves are ONE script (see _REINJECT_LUA): as two round-trips
            # an interruption between them left the event billed and its guard
            # still expiring.
            flat_payload: list[str] = []
            for field, value in cleaned.items():
                flat_payload.append(field)
                flat_payload.append(value)
            await cast("Any", redis_conn.eval)(
                _REINJECT_LUA, 2, costs_stream, marker, *flat_payload
            )
            injected = True
            replayed.append(entry_id)

            # (f) Optionally drain from the dead stream.
            if delete_on_success:
                await redis_conn.xdel(dead_stream, entry_id)
                deleted_from_dlq.append(entry_id)
        except Exception:  # noqa: BLE001 — per-id isolation: one bad entry must
            # not abort the batch. The failure is logged; the id simply lands in
            # none of the buckets and the operator can re-submit it.
            logger.warning("dlq_replay_entry_failed", entry_id=entry_id, exc_info=True)
            # If we claimed the marker but the re-injection did NOT durably land,
            # release it so a retry can re-attempt — otherwise the entry stays on
            # :dead marked-as-replayed and is silently dropped until the marker
            # expires (fail-open to $0, the very loss this tool exists to close).
            # If injection DID succeed (a later step like xdel failed), KEEP the
            # marker: the event is already billed, so re-injecting would double
            # it — and there is nothing to repair, because the marker was made
            # permanent in the SAME script that injected (see _REINJECT_LUA).
            # `injected` therefore implies "guard is already permanent"; a
            # compensating PERSIST here would be a handler that cannot fire.
            if claimed_here and not injected:
                try:
                    await redis_conn.delete(marker)
                except Exception:  # noqa: BLE001 — best-effort release; the marker
                    # self-expires at marker_ttl regardless.
                    logger.warning("dlq_replay_marker_release_failed", entry_id=entry_id)
            continue

    result = DlqReplayResult(
        replayed=replayed,
        already_processed=already_processed,
        still_malformed=still_malformed,
        deleted_from_dlq=deleted_from_dlq,
    )
    logger.info(
        "dlq_replayed",
        requested=len(ids),
        replayed=len(replayed),
        already_processed=len(already_processed),
        still_malformed=len(still_malformed),
        deleted_from_dlq=len(deleted_from_dlq),
        delete_on_success=delete_on_success,
    )
    # Replay XDELs too (dlq_ops:234), so it moves the depth exactly as a discard
    # does and inherits the same five-minute staleness without this.
    if deleted_from_dlq:
        await _resample_depth(redis_conn, dead_stream)
    return result


async def _resample_depth(redis_conn: aioredis.Redis, stream: str) -> None:
    """Re-read XLEN and refresh ``billing_cost_dead_letter_depth``.

    WHY THIS IS NOT ONLY IN THE CONSUMER. The gauge's only other writer is the
    consumer's trim block, which runs once per ``cost_stream_trim_interval_seconds``
    — default 300s, no live override. So after an operator drains the queue the
    gauge keeps reporting the OLD depth for up to five minutes.

    Measured 2026-08-28: a confirmed discard emptied the stream at 16:13:59Z and
    the gauge still read 36 a minute later. That reading was nearly filed as "the
    metric does not track reality" — it tracked reality perfectly, on a five
    minute delay, and the delay is indistinguishable from a failed discard to
    anyone who does not know the sampling period.

    Best-effort by construction: this runs AFTER the XDELs have already
    succeeded, so a failure here means the gauge is stale, never that the drain
    is in doubt. Swallowing is therefore correct — raising would turn a cosmetic
    lag into a failed operation and invite a re-run of a destructive command.
    """
    try:
        observability_metrics.cost_dead_letter_depth.set(await redis_conn.xlen(stream))
        # Depth and age are two axes of the SAME quarantine and are read
        # together (depth==0 + age NaN means empty; depth>0 + age NaN means the
        # oldest entry is unstamped). Sampling them apart would let an operator
        # pair a fresh depth with a five-minute-old age.
        await sample_oldest_age(redis_conn, stream)
    except Exception:  # noqa: BLE001 — see docstring: cosmetic, never load-bearing.
        logger.warning("dlq_depth_resample_failed", stream=stream, exc_info=True)


def _parse_dlq_at(raw: str | None) -> datetime | None:
    """Parse a ``_dlq_at`` stamp, or ``None`` if it cannot be trusted.

    The only writer is ``cost_flusher.flush_costs``, which stamps
    ``datetime.now(UTC).isoformat()`` — always tz-aware. A NAIVE value is still
    accepted as UTC rather than rejected: no writer has ever emitted local time,
    so reading one as UTC is correct rather than a guess. Everything else —
    absent, empty, or unparseable — returns None, and the caller emits NaN.
    """
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except TypeError, ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


async def sample_oldest_age(redis_conn: aioredis.Redis, stream: str) -> None:
    """Re-derive ``billing_cost_dead_letter_oldest_age_seconds`` FROM THE STREAM.

    WHY IT IS DERIVED AND NOT ACCUMULATED. ``billing_cost_dead_letter_total`` is
    a per-process Counter incremented at quarantine time, so its reading depends
    on when the exporting process was born, not on what the stream holds.

    ⚠️ CORRECTED 2026-09-14 — this used to say the Counter "currently reads 0 for
    every reason on a stream holding 321 entries". Both figures are stale: depth
    is 14, and ``sum by (reason) (increase(billing_cost_dead_letter_total[30d]))``
    reads nil_tenant_id 1053.9 and invalid_tenant_id 308.1. The stale reading is
    not evidence against the argument, though — it was one process's live value,
    while an ``increase()`` over a range stitches across births and so hides
    exactly the reset being warned about. The hazard is unchanged: a Counter does
    not survive a restart.
    Any age instrument built the same way (stamp the first quarantine in a
    module global, subtract at scrape) inherits that: a roll would reset the
    clock and the 30-day expiry would go dark again exactly when it matters.
    This function holds NO state between calls. Every sample re-reads the oldest
    surviving entry off Redis, so a pod one second old reports the same age as
    the pod it replaced. Redis is the durable copy; the process is not.

    O(1) BY CONSTRUCTION: ``XRANGE <stream> - + COUNT 1``. Stream order is id
    order, so the first entry IS the oldest; there is no scan and no XLEN-sized
    cost. This is deliberately NOT on the scrape path — Redis I/O per scrape
    would put an external dependency inside ``/metrics``. It rides the two
    cadences ``billing_cost_dead_letter_depth`` already uses: the consumer trim
    block (once per ``cost_stream_trim_interval_seconds``) and
    :func:`_resample_depth` after a replay/discard that XDELed.

    WHAT IT EMITS (see the metric's own comment block for the reasoning):

    * empty stream                      -> NaN
    * oldest entry, no usable _dlq_at   -> NaN + a WARNING naming the entry id
    * otherwise                         -> now - _dlq_at, in seconds

    Never 0 for "no reading". A clock-skewed stamp from the future is clamped to
    0.0 and logged rather than exported negative: both are safe for an
    ``> threshold`` rule, but a negative age reads as a broken instrument.

    Raises on Redis errors — both call sites already guard (the consumer's trim
    block catches RedisError; ``_resample_depth`` swallows). Nothing here is
    load-bearing enough to fail an operation for.
    """
    gauge = observability_metrics.cost_dead_letter_oldest_age_seconds
    oldest = await cast("Any", redis_conn.xrange)(stream, count=1)
    if not oldest:
        gauge.set(math.nan)
        return

    entry_id, fields = oldest[0]
    raw = fields.get("_dlq_at")
    stamped = _parse_dlq_at(raw)
    if stamped is None:
        # Do not fall back to the entry id's millisecond component: it would
        # always produce a number and so would hide the missing stamp.
        logger.warning("dlq_oldest_age_unreadable", stream=stream, entry_id=entry_id, dlq_at=raw)
        gauge.set(math.nan)
        return

    age_seconds = (datetime.now(UTC) - stamped).total_seconds()
    if age_seconds < 0:
        logger.warning(
            "dlq_oldest_age_negative",
            stream=stream,
            entry_id=entry_id,
            age_seconds=age_seconds,
        )
        age_seconds = 0.0
    gauge.set(age_seconds)


#: Tenant literals an EMITTER writes when it has no tenant at all. These are the
#: one class of unresolved tenant that can never become attributable: registering
#: one as a real tenant would silently turn an unauthenticated call into a
#: billable customer, which is exactly what the product ruling on asr-backend
#: forbids. A legacy SLUG is different — registering it in LEGACY_TENANT_NAMES
#: makes the event attributable again — and is still refused below.
#:
#: Measured 2026-11-26: asr-backend's Rust port writes ``unauthenticated``
#: whenever its licence gate is disabled
#: (``rust/asr-backend-rs/src/auth.rs::LicenceGate::authenticate``), and its
#: cost event carried no provider either, so the entry is zero-cost, unattributable
#: and undrainable by replay or re-attribute. Keep this set EXACT and lowercase.
EMITTER_SENTINEL_TENANTS: frozenset[str] = frozenset({"unauthenticated"})


def _discard_refusal(fields: dict[str, str]) -> str | None:
    """Return a low-cardinality refusal reason, or ``None`` if the entry may be
    discarded. Refuse-by-default: every path that cannot PROVE both conditions
    returns a reason.

    Both conditions must hold (see :func:`discard_dlq` for why):

    1. ``cost_usd`` is present and parses to exactly zero. Absent or unparseable
       is NOT treated as zero — an entry whose cost cannot be read is an entry
       whose value cannot be ruled out, and the safe direction is to keep it.
    2. the tenant is genuinely unattributable — absent/blank, or resolving to
       :data:`tenancy.NIL_TENANT_ID` (the emitter's own "I have no tenant"
       fallback). These are the two states no operator action can undo.

    Deliberately NARROWER than ``_cost_schema``'s unattributable set: an
    *unresolvable* tenant (``canonical_tenant_id`` -> ``None``) is REFUSED, not
    discarded, because that includes a legacy slug that is merely not registered
    yet — registering it in ``LEGACY_TENANT_NAMES`` makes the event attributable
    again, so the attribution is recoverable and the entry must survive.

    ⚠️ KNOWN DEAD END (recorded 2026-09-14) — CLOSED by :func:`reattribute_dlq`
    (``POST /api/admin/dlq/reattribute``, #243). A ``nil_tenant_id`` (or blank
    tenant) entry whose ``cost_usd != 0`` is drained by RE-ATTRIBUTING it to an
    explicit, registered tenant an operator names, with a Postgres receipt. Do
    NOT loosen this refusal instead: discard is still the wrong move for money.
    The history below is why the third path exists.

    Before #243 such an entry had NO drain at all, and this refusal was one of
    its two walls. Neither wall was the bug; the absence of a third path was.

      * replay cannot take it: ``replay_dlq`` re-injects the STORED fields and
        ``validate_cost_event`` re-rejects them identically (nil uuid ->
        ``nil_tenant_id``, :mod:`services._cost_schema`), so it lands in
        ``still_malformed``, the marker is released and nothing is injected.
        See the ``/api/admin/dlq/discard`` docstring, which names this exact
        case as the reason discard exists at all.
      * discard cannot take it either: the entry fails condition 1 above and is
        refused ``cost_usd_nonzero``. That refusal is CORRECT — silently
        deleting money is the worse error — so do not loosen it to close the
        gap.

    The consequence is that such an entry can only sit until the 720h age trim
    destroys it, unrecoverably (Redis is its only copy; it was rejected at the
    schema seam and never written to Postgres). The live stream is in exactly
    this state: measured 2026-09-14, all 14 entries are ``nil_tenant_id``, the
    oldest is 11.67d old, and the trim is due to destroy them ~2026-10-02.
    ``BillingCostDeadLetterAgeing`` (>14d) is the deliberate early warning for
    it and fires ~2026-09-16.

    Closing this needed a THIRD move that neither replay nor discard can be bent
    into — re-attributing the spend to a real tenant before re-injection — not a
    weaker refusal here. That move is :func:`reattribute_dlq`.
    """
    raw_cost = fields.get("cost_usd")
    if raw_cost is None or str(raw_cost).strip() == "":
        return "cost_usd_absent"
    try:
        cost = Decimal(str(raw_cost).strip())
    except (InvalidOperation, TypeError, ValueError):  # fmt: skip
        return "cost_usd_unparseable"
    if cost != _DISCARDABLE_COST_USD:
        return "cost_usd_nonzero"

    raw_tenant = fields.get("tenant_id")
    if raw_tenant is None or str(raw_tenant).strip() == "":
        return None
    resolved_tenant = canonical_tenant_id(raw_tenant)
    if resolved_tenant is None:
        # An emitter sentinel ("unauthenticated") is unattributable FOR GOOD --
        # no operator action can turn it into a tenant, so it carries the same
        # "provably unattributable" property as the nil tenant above. Every other
        # unresolved string stays refused: it may be a legacy slug that a
        # registration would make attributable again.
        if str(raw_tenant).strip().lower() in EMITTER_SENTINEL_TENANTS:
            return None
        return "tenant_unresolved"
    if resolved_tenant != NIL_TENANT_ID:
        return "tenant_attributable"
    return None


async def discard_dlq(
    redis_conn: aioredis.Redis,
    *,
    ids: list[str],
    reason: str,
    confirm: bool = False,
) -> DlqDiscardResult:
    """Permanently drop dead-letter entries that NO replay can ever drain.

    THE PRINCIPLE THIS EXCEPTS
    --------------------------
    Migration ``0013_park_router_duplicates`` states it as the rule of this
    service's money data:

        "Nothing is deleted — these are financial records, and a park can be
        undone with one UPDATE."

    and, in its own WHAT IT DELIBERATELY DOES NOT DO section:

        "It does not delete. There is no DELETE statement in this file."

    That rule stands everywhere else. ``billing_cost_events`` rows are never
    deleted; a duplicate is PARKED behind a reversible ``excluded_from_billing``
    flag precisely so the decision can be undone. This function is the one
    exception, and it is exception-shaped for three reasons:

    * **It is not a financial record.** A quarantined entry never reached
      ``billing_cost_events``; it is a Redis quarantine record of an event the
      consumer refused to bill. Nothing invoices it, no rollup sums it, no
      report reads it.
    * **The value it carries is provably zero.** An entry is only eligible when
      ``cost_usd`` parses to exactly 0 — so nothing that could ever have become
      money is dropped. What is lost is ATTRIBUTION, not dollars.
    * **The attribution is already unrecoverable.** Eligibility also requires an
      absent tenant or the nil tenant — the emitter's own "I have no tenant"
      fallback. There is no operator action that turns those back into a real
      tenant, and ``replay_dlq`` cannot drain them: the stored payload still
      carries the nil tenant, so ``validate_cost_event`` re-rejects it with the
      same ``nil_tenant_id`` and it re-quarantines forever. Without this the only
      exit is Redis retention expiry, with the BillingCostDeadLetterBacklog alert
      firing the whole way.

    An entry that fails EITHER condition is refused and counted, never deleted —
    see :func:`_discard_refusal`. This is a narrow carve-out for provably
    worthless, provably unattributable quarantine records. It is NOT a delete
    API, and it must not become one.

    SAFETY
    ------
    * ``confirm`` defaults to **False**. A plain call is a DRY RUN: it examines,
      classifies and reports, and issues no ``XDEL``.
    * ``reason`` is required and non-blank (enforced by
      :class:`models.domain.DlqDiscardRequest`) and is written to every
      per-entry log line, because the entry itself will not survive to explain
      itself. The origin of this tool is 22,438 rows deleted with no audit trail
      (#92).
    * Per-id isolation, as in :func:`replay_dlq`: one failing entry never aborts
      the batch.

    Returns the aggregated outcome; the caller is expected to audit-log it.
    """
    settings = get_settings()
    dead_stream = settings.cost_dead_letter_stream

    examined = 0
    eligible: list[str] = []
    discarded: list[str] = []
    refused: list[dict[str, str]] = []

    for entry_id in ids:
        try:
            # (a) Fetch the single entry. Gone already (aged out, or a prior
            # confirmed run) => nothing to do; it is not even examined.
            found = await cast("Any", redis_conn.xrange)(
                dead_stream, min=entry_id, max=entry_id, count=1
            )
            if not found:
                continue
            _, fields = found[0]
            examined += 1

            # (b) The carve-out gate. Refused entries are counted and LEFT ALONE.
            refusal = _discard_refusal(fields)
            if refusal is not None:
                refused.append({"id": entry_id, "reason": refusal})
                continue
            eligible.append(entry_id)

            # (c) Dry run by default — report what would go, delete nothing.
            if not confirm:
                continue

            # (d) The destructive step. Log BEFORE the XDEL: after it the fields
            # are gone, and a crash between the two must still leave the trail.
            logger.info(
                "dlq_entry_discarded",
                entry_id=entry_id,
                original_id=fields.get("_dlq_original_id"),
                service_key=fields.get("service_key") or "unknown",
                dlq_reason=fields.get("_dlq_reason"),
                cost_usd=fields.get("cost_usd"),
                discard_reason=reason,
            )
            await redis_conn.xdel(dead_stream, entry_id)
            discarded.append(entry_id)
        except Exception:  # noqa: BLE001 — per-id isolation, as in replay_dlq:
            # one bad entry must not abort the batch. The id lands in none of the
            # result buckets and the operator can re-submit it.
            logger.warning("dlq_discard_entry_failed", entry_id=entry_id, exc_info=True)
            continue

    result = DlqDiscardResult(
        examined=examined,
        eligible=eligible,
        discarded=discarded,
        refused=refused,
    )
    logger.info(
        "dlq_discarded",
        requested=len(ids),
        examined=examined,
        eligible=len(eligible),
        discarded=len(discarded),
        refused=len(refused),
        confirm=confirm,
        discard_reason=reason,
    )
    # Guarded on `discarded`, not on `confirm`: a dry run deletes nothing, so
    # resampling there would spend a round trip to write back the value the
    # gauge already holds.
    if discarded:
        await _resample_depth(redis_conn, dead_stream)
    return result


class ReattributeTargetError(ValueError):
    """The target tenant of a re-attribution is not one money may be moved to.

    ``code`` is low-cardinality and is surfaced verbatim in the 400 detail.
    """

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def resolve_reattribute_target(raw: str) -> str:
    """Canonicalise the operator-named target tenant, or refuse it.

    Three refusals, each a way to move money to nobody:

    * ``tenant_id_unresolvable`` — :func:`tenancy.canonical_tenant_id` answers
      ``None`` (blank, a slug that is not a registered name, a leaked credential).
    * ``tenant_id_nil`` — the nil uuid. Re-attributing nil spend TO nil is the
      dead end itself, and ``validate_cost_event`` would re-quarantine it.
    * ``tenant_id_unregistered`` — a well-formed uuid the licences registry does
      not vouch for (:func:`tenancy.is_registered_tenant_id`). The flusher would
      accept it and then PARK the row ``excluded_from_billing``, so the event
      would leave the DLQ and still never be billed — a silent $0 under a green
      response. Refusing here keeps the entry on the stream for a correct retry.
    """
    canon = canonical_tenant_id(raw)
    if canon is None:
        raise ReattributeTargetError("tenant_id_unresolvable")
    if canon == NIL_TENANT_ID:
        raise ReattributeTargetError("tenant_id_nil")
    if not is_registered_tenant_id(canon):
        raise ReattributeTargetError("tenant_id_unregistered")
    return canon


def _reattribute_refusal(fields: dict[str, str]) -> str | None:
    """Return a refusal reason, or ``None`` if the STORED tenant is nil or blank.

    Re-attribution overrides who pays, so it may only ever fill a tenant that is
    provably absent. Anything else is refused and left on the stream:

    * ``tenant_attributable`` — the stored tenant resolves to a real tenant.
      Moving that spend would bill the wrong customer; replay is its path.
    * ``tenant_unresolved`` — a stored value that is not a uuid and not a
      registered name. Registering the name makes it attributable again (see
      :func:`_discard_refusal`), so the ORIGINAL attribution is recoverable and
      an operator override would destroy it.
    """
    raw_tenant = fields.get("tenant_id")
    if raw_tenant is None or str(raw_tenant).strip() == "":
        return None
    resolved = canonical_tenant_id(raw_tenant)
    if resolved is None:
        return "tenant_unresolved"
    if resolved != NIL_TENANT_ID:
        return "tenant_attributable"
    return None


async def reattribute_dlq(
    redis_conn: aioredis.Redis,
    session: AsyncSession,
    *,
    ids: list[str],
    tenant_id: str,
    reason: str,
    reattributed_by: str,
    request_id: str | None = None,
    confirm: bool = False,
) -> DlqReattributeResult:
    """Re-attribute nil/blank-tenant dead-letter entries to an explicit tenant.

    This is the third move :func:`_discard_refusal`'s KNOWN DEAD END asks for
    (#243): a ``nil_tenant_id`` entry with ``cost_usd != 0`` can be neither
    replayed (the stored nil tenant re-fails validation) nor discarded (deleting
    money is the worse error), so without this it is destroyed by the 720h trim.

    GUARDS, in order:

    1. The TARGET must be a registered, non-nil tenant
       (:func:`resolve_reattribute_target`); refused targets raise before any
       entry is read. ``reason`` must be non-blank.
    2. Each entry's STORED tenant must be nil or blank
       (:func:`_reattribute_refusal`); any other entry is refused, untouched.
    3. The REWRITTEN payload (target tenant + provenance stamps) must pass
       :func:`validate_cost_event` — an entry that is also e.g.
       ``missing_provider`` stays quarantined in ``still_malformed``.
    4. ``confirm`` defaults to **False**: a dry run classifies and reports, and
       writes nothing — no marker, no XADD, no receipt, no XDEL.

    CONFIRMED PATH (per entry), reusing replay's machinery unchanged:

    a. claim :func:`_replay_marker` with ``SET NX`` — the SAME key replay uses,
       keyed on the original event id, so a second reattribute call, a replay,
       or a re-quarantined twin of the same event all report
       ``already_processed`` instead of billing it again;
    b. re-inject with :data:`_REINJECT_LUA`, which makes the marker permanent in
       the same atomic script;
    c. commit a ``billing_dlq_reattributions`` receipt (full original fields,
       target, actor, reason, new stream id) BEFORE
    d. ``XDEL``. If the receipt cannot be committed the entry is NOT deleted: it
       is already billed and its marker is permanent, so it cannot be billed
       twice; it lands in ``receipt_failed`` and the quarantine record survives
       as evidence alongside the ``dlq_entry_reattributed`` log line.

    Per-id isolation as in :func:`replay_dlq`. Commits ``session`` once per
    receipt; the caller owns the run-level audit row.
    """
    if not reason or not reason.strip():
        raise ValueError("reason must be non-empty")
    target = resolve_reattribute_target(tenant_id)

    settings = get_settings()
    dead_stream = settings.cost_dead_letter_stream
    costs_stream = settings.costs_stream
    marker_ttl = max(
        _REPLAY_MARKER_TTL_FLOOR_SECONDS,
        settings.dead_letter_stream_max_age_hours * 3600,
    )

    examined = 0
    eligible: list[str] = []
    reattributed: list[str] = []
    already_processed: list[str] = []
    still_malformed: list[dict[str, str]] = []
    refused: list[dict[str, str]] = []
    receipt_failed: list[str] = []
    deleted_from_dlq: list[str] = []

    for entry_id in ids:
        marker = ""
        claimed_here = False
        injected = False
        try:
            found = await cast("Any", redis_conn.xrange)(
                dead_stream, min=entry_id, max=entry_id, count=1
            )
            if not found:
                continue
            _, fields = found[0]
            examined += 1

            refusal = _reattribute_refusal(fields)
            if refusal is not None:
                refused.append({"id": entry_id, "reason": refusal})
                continue

            raw_from = fields.get("tenant_id")
            reattributed_from = (
                str(raw_from).strip() if raw_from is not None and str(raw_from).strip() else None
            )
            rewritten = _business_fields(fields)
            rewritten["tenant_id"] = target
            # Provenance travels WITH the event on billing:costs. "" (not absent)
            # for a blank source tenant: a Redis stream field cannot be None, and
            # the receipt below records the NULL precisely.
            rewritten["reattributed_from"] = reattributed_from or ""
            rewritten["reattributed_by"] = reattributed_by
            rewritten["reattribution_reason"] = reason

            malformed_reason = validate_cost_event(rewritten)
            if malformed_reason is not None:
                still_malformed.append({"id": entry_id, "reason": malformed_reason})
                continue

            marker = _replay_marker(entry_id, fields)
            if not confirm:
                # Read-only peek so a dry run reports what a confirm would do.
                if await redis_conn.exists(marker):
                    already_processed.append(entry_id)
                else:
                    eligible.append(entry_id)
                continue

            claimed = await redis_conn.set(marker, "1", nx=True, ex=marker_ttl)
            if not claimed:
                already_processed.append(entry_id)
                continue
            claimed_here = True
            eligible.append(entry_id)

            flat_payload: list[str] = []
            for field, value in rewritten.items():
                flat_payload.append(field)
                flat_payload.append(value)
            new_id = await cast("Any", redis_conn.eval)(
                _REINJECT_LUA, 2, costs_stream, marker, *flat_payload
            )
            injected = True
            reattributed.append(entry_id)
            # Backstop trail: logged the moment money moved, before the receipt,
            # so an interruption between the two still leaves a record.
            logger.info(
                "dlq_entry_reattributed",
                entry_id=entry_id,
                original_id=fields.get("_dlq_original_id"),
                new_stream_id=str(new_id),
                reattributed_from=reattributed_from,
                tenant_id=target,
                reattributed_by=reattributed_by,
                cost_usd=fields.get("cost_usd"),
                service_key=fields.get("service_key") or "unknown",
                reattribution_reason=reason,
            )

            try:
                session.add(
                    DlqReattribution(
                        dead_entry_id=entry_id,
                        original_id=fields.get("_dlq_original_id") or None,
                        replay_marker=marker,
                        reattributed_from=reattributed_from,
                        tenant_id=target,
                        reattributed_by=reattributed_by,
                        reason=reason,
                        original_fields=dict(fields),
                        reinjected_stream_id=str(new_id),
                        request_id=request_id,
                    )
                )
                await session.commit()
            except Exception:  # noqa: BLE001 — see docstring step (d): keep the entry.
                await session.rollback()
                logger.error(
                    "dlq_reattribute_receipt_failed",
                    entry_id=entry_id,
                    new_stream_id=str(new_id),
                    exc_info=True,
                )
                receipt_failed.append(entry_id)
                continue

            await redis_conn.xdel(dead_stream, entry_id)
            deleted_from_dlq.append(entry_id)
        except Exception:  # noqa: BLE001 — per-id isolation, as in replay_dlq.
            logger.warning("dlq_reattribute_entry_failed", entry_id=entry_id, exc_info=True)
            if claimed_here and not injected:
                try:
                    await redis_conn.delete(marker)
                except Exception:  # noqa: BLE001 — best-effort; self-expires at marker_ttl.
                    logger.warning("dlq_reattribute_marker_release_failed", entry_id=entry_id)
            continue

    result = DlqReattributeResult(
        tenant_id=target,
        examined=examined,
        eligible=eligible,
        reattributed=reattributed,
        already_processed=already_processed,
        still_malformed=still_malformed,
        refused=refused,
        receipt_failed=receipt_failed,
        deleted_from_dlq=deleted_from_dlq,
    )
    logger.info(
        "dlq_reattributed",
        requested=len(ids),
        examined=examined,
        eligible=len(eligible),
        reattributed=len(reattributed),
        already_processed=len(already_processed),
        still_malformed=len(still_malformed),
        refused=len(refused),
        receipt_failed=len(receipt_failed),
        confirm=confirm,
        tenant_id=target,
        reattribution_reason=reason,
    )
    if deleted_from_dlq:
        await _resample_depth(redis_conn, dead_stream)
    return result


#: Entries per archive batch. One multi-row INSERT binds ~6 parameters per row
#: and psycopg caps a statement at 65535, so an unbounded batch wedges at ~10.9k
#: rows -- and then every later tick retries an even bigger set.
_ARCHIVE_BATCH = 1000
#: Batches per call, so one tick on a huge expired backlog stays bounded; the
#: next tick continues where this one stopped.
_ARCHIVE_MAX_BATCHES = 50


def _pg_safe(value: Any) -> str:
    """A Redis field value Postgres can store: NUL is not representable in
    text/jsonb (``UntranslatableCharacter``), and a quarantined entry is
    malformed by definition, so escape it rather than let it wedge the trim."""
    return str(value).replace("\x00", "\\u0000")


def _archive_row(stream: str, entry_id: str, fields: Mapping[str, Any]) -> dict[str, Any]:
    safe = {_pg_safe(k): _pg_safe(v) for k, v in fields.items()}
    reason = safe.get("_dlq_reason")
    dlq_at = safe.get("_dlq_at")
    return {
        "stream": stream,
        "dead_entry_id": entry_id,
        "dlq_reason": reason[:128] if reason is not None else None,
        "dlq_at": dlq_at[:64] if dlq_at is not None else None,
        "fields": safe,
    }


async def _insert_archive_rows(session_factory: Any, rows: list[dict[str, Any]]) -> None:
    """INSERT ... ON CONFLICT DO NOTHING + COMMIT; one poison row cannot block the
    batch: on a batch failure each row is retried alone, and a row that still
    fails is archived as its ``repr`` so it is kept, never skipped."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from models.db import DlqArchive

    async def _write(batch: list[dict[str, Any]]) -> None:
        async with session_factory() as session:
            stmt = pg_insert(DlqArchive).values(batch)
            stmt = stmt.on_conflict_do_nothing(constraint="uq_dlq_archive_entry")
            await session.execute(stmt)
            await session.commit()

    try:
        await _write(rows)
        return
    except Exception:
        if len(rows) == 1 and rows[0]["fields"].get("_archive_error"):
            raise
        logger.warning("dlq_archive_batch_failed_retrying_per_row", rows=len(rows), exc_info=True)
    for row in rows:
        try:
            await _write([row])
        except Exception as exc:
            fallback = {
                **row,
                "dlq_reason": None,
                "dlq_at": None,
                "fields": {
                    "_archive_error": type(exc).__name__,
                    "_raw_repr": _pg_safe(repr(row["fields"])),
                },
            }
            # If even this fails the exception propagates and nothing is trimmed.
            await _write([fallback])


async def archive_then_trim_dead_letter(
    redis_conn: aioredis.Redis | None,
    session_factory: Any,
    stream: str,
    max_age_hours: int,
) -> int:
    """Age-bound the dead-letter stream WITHOUT losing what the bound removes.

    The plain ``rollup_engine.trim_stream`` destroys every expired entry, and on
    ``billing:costs:dead`` Redis is the entry's ONLY copy: 11 quarantined cost
    events were lost that way between 2026-09-30 and 2026-10-02 while
    ``BillingCostDeadLetterNearExpiry`` was firing. This is the dead-letter
    stream's age trim instead, in bounded batches of :data:`_ARCHIVE_BATCH`:

    1. ``XRANGE - <cutoff> COUNT n`` -- the oldest expired entries.
    2. INSERT each into ``billing_dlq_archive`` (``ON CONFLICT DO NOTHING`` on
       ``(stream, dead_entry_id)``, so a re-run is a no-op) and COMMIT. Values
       are NUL-escaped, and a row Postgres still refuses is archived as its
       ``repr`` (:func:`_insert_archive_rows`), so one bad entry cannot wedge it.
    3. Only then ``XTRIM MINID <last id + 1>`` -- EXACT, not ``~``, so it removes
       precisely the entries just archived and never one it did not read. New
       XADD ids are always above the cutoff (Redis time, cutoff is 720h ago).

    If step 2 raises, step 3 never runs for that batch: the entries stay in
    Redis and the next tick retries. Returns entries trimmed (also counted on
    ``billing_cost_dead_letter_expired_total``, whose meaning for this stream
    narrows from "destroyed" to "archived then removed").

    ⚠️ This closes the AGE path only. The quarantine XADD in
    ``cost_flusher`` still caps the stream at ``MAXLEN ~ 100000``, and that cap
    drops the oldest entries without archiving them.
    """
    if redis_conn is None:
        return 0
    cutoff_ms = int(datetime.now(UTC).timestamp() * 1000) - max_age_hours * 3600 * 1000
    total = 0
    archived = 0
    for _ in range(_ARCHIVE_MAX_BATCHES):
        # decode_responses=True on the shared client (redis_client.py), so ids and
        # fields are already str -- same assumption as inspect_dlq above.
        raw = await cast("Any", redis_conn.xrange)(
            stream, min="-", max=str(cutoff_ms), count=_ARCHIVE_BATCH
        )
        if not raw:
            break
        rows = [_archive_row(stream, entry_id, fields) for entry_id, fields in raw]
        await _insert_archive_rows(session_factory, rows)
        archived += len(rows)
        last_ms, last_seq = (int(p) for p in raw[-1][0].split("-"))
        # Exact MINID removes ids strictly below last+1, i.e. <= the last id read
        # -- exactly this batch, all of which is now durably archived.
        removed = await redis_conn.xtrim(
            stream, minid=f"{last_ms}-{last_seq + 1}", approximate=False
        )
        total += removed or 0
        if len(raw) < _ARCHIVE_BATCH:
            break
    observability_metrics.cost_dead_letter_expired_total.labels(stream=stream).inc(total)
    if archived:
        logger.info(
            "dlq_archived_then_trimmed",
            stream=stream,
            archived=archived,
            removed=total,
            max_age_hours=max_age_hours,
        )
    return total
