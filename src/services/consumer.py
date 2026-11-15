"""Redis Stream consumer — reads from the billing:events and billing:costs streams.

HA multi-consumer, inbox-deduped (per-pod consumer name). The consumer runs on
EVERY replica (not a leader-elected singleton): exactly-once is already
guaranteed by the idempotency inbox (``processed_cost_messages``, claimed via
``INSERT ... ON CONFLICT DO NOTHING`` inside the persist tx) plus the FIX A2
XAUTOCLAIM PEL-reclaim, so leader-gating the consumer was only an optimization
to avoid two replicas sharing one consumer name. Each replica instead derives a
per-pod-unique consumer name (Kubernetes ``HOSTNAME`` = pod name), so the two
replicas are distinct consumers in the shared group and the stream is split
between them (no double-read), while a duplicate redelivery to either is deduped
by the inbox.
"""

import asyncio
import os
import re
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import cast

import redis.asyncio as aioredis
import structlog
from redis.exceptions import RedisError

import observability.metrics as observability_metrics
from db import get_session_factory
from services._parsers import parse_event_ts
from services.dlq_ops import archive_then_trim_dead_letter, sample_oldest_age
from services.rollup_engine import EVENTS_STREAM_MAX_AGE_HOURS, trim_stream
from settings import Settings

logger = structlog.stdlib.get_logger(__name__)

# Stand-in id for a PEL entry evicted on the legacy (Redis 6.2 / redis-py < 5)
# XAUTOCLAIM reply, where redis-py parses a nil entry to ``(None, None)`` and the
# id is genuinely gone. Never log ``None`` here — it reads as a real id.
_LOST_ID_UNAVAILABLE = "<unavailable>"

# Bounded metric vocabulary for the ``stream`` label on the PEL counters, mirroring
# ``observability.metrics.PEL_STREAMS`` and the values
# ``billing_tenant_caller_unasserted_total{stream}`` already carries. NOT the raw
# Redis stream names: those come from settings, so they differ per environment and
# cannot be birthed at import inside observability.metrics (which must stay
# settings-free — it is imported by this module). The mapping from configured name
# to label happens only where both are known.
_STREAM_USAGE = "usage"
_STREAM_COST = "cost"


def _note_backdated_event_ts(fields: Mapping[str, str], settings: Settings) -> None:
    """Count a cost entry whose asserted ``event_ts`` predates the rollup window.

    ⛔ OBSERVABILITY ONLY — this function makes no decision and changes nothing
    about the entry, which is staged and billed exactly as before.

    THE GAP IT MEASURES. ``_parsers.parse_event_ts`` accepts a producer-asserted
    event time up to ``MAX_EVENT_TS_BACKDATE_DAYS`` (90) before ingest, and
    ``rollup_engine.rollup_daily`` buckets the row on THAT date. But
    ``rollup_engine.catchup_daily`` only ever re-rolls the trailing
    ``settings.daily_reconcile_days`` complete days, plus whatever
    ``missing_daily_dates`` reports as entirely ABSENT. So an event landing in a
    day that has already been rolled AND already has a ``billing_cost_daily`` row
    is never re-aggregated: real spend, stored, and invisible to every invoice.
    ``daily_rollup_date_gaps`` deliberately declines to repair (re-rolling a day
    whose sources are gone DELETES the surviving bucket), so nothing downstream
    closes it either.

    ⚠️ IT SAYS "LOOK", NOT "MONEY WAS LOST". Whether the loss is real depends on
    something not knowable here — whether that day's bucket already exists. If it
    does not, ``missing_daily_dates`` still picks the day up and the row is billed
    LATE rather than never. The honest measurement available at ingest is "an event
    arrived for a day the reconcile window no longer revisits", and that is what
    this counts.

    WHY HERE. The two places that literally decide to accept the value are
    ``_parsers.parse_event_ts`` and its sole caller
    ``cost_flusher.flush_costs``. This is the nearest point upstream that sees the
    same raw field — ``_stage_message`` appends the wire dict verbatim, so the
    ``timestamp`` the flusher later parses is the one read here — and it reuses
    ``parse_event_ts`` itself rather than re-implementing the rule, so the two can
    never disagree about whether a value was usable. The only divergence is
    ``received_at`` (stage time here, flush time there), seconds apart against a
    multi-day threshold.

    Never raises: the module convention is that a measurement must not be able to
    break the ingest path it rides on.
    """
    try:
        now = datetime.now(UTC)
        event_ts = parse_event_ts(fields.get("timestamp"), received_at=now)
        if event_ts is None:
            # Refused (absent, naive, unparseable, future, or backdated past
            # retention) — the flusher stores NULL and counts the ingest-time
            # fallback on `billing_cost_event_ts_fallback_total`. A different
            # population; see that counter's HELP.
            return
        # The oldest day `catchup_daily`'s trailing window still re-rolls:
        # boundary = today - 1, window = {boundary - n for n in range(N)} =
        # [today - N, today - 1]. Strictly older than today - N is outside it.
        # `max(..., 0)` keeps a nonsensical negative setting from inverting the
        # comparison; with N = 0 the trailing window is genuinely empty and only
        # `missing_daily_dates` covers yesterday, which this then reports.
        oldest_reconciled = now.date() - timedelta(days=max(settings.daily_reconcile_days, 0))
        if event_ts.date() < oldest_reconciled:
            observability_metrics.cost_event_ts_backdated_past_rollup_total.labels(
                service_key=fields.get("service_key") or "unknown"
            ).inc()
            logger.warning(
                "cost_event_ts_backdated_past_rollup",
                service_key=fields.get("service_key") or "unknown",
                event_date=event_ts.date().isoformat(),
                oldest_reconciled_date=oldest_reconciled.isoformat(),
                note=(
                    "event_ts predates the daily rollup's reconcile window; if that "
                    "day already has a billing_cost_daily row this spend will never "
                    "be re-aggregated and never invoiced"
                ),
            )
    except Exception:  # pragma: no cover - observability must not break ingest
        logger.warning("cost_event_ts_backdate_check_failed", exc_info=True)


def resolve_consumer_name(settings: Settings) -> str:
    """Per-pod-unique Redis consumer name.

    Kubernetes always sets ``HOSTNAME`` to the pod name, so each replica gets a
    distinct consumer in the shared group (the two replicas are no longer both
    ``worker-0`` sharing one consumer). Falls back to the configured
    ``settings.consumer_name`` for local / unit runs where ``HOSTNAME`` is unset.
    This MUST be the single source of the name so the steady-state XREADGROUP and
    the XAUTOCLAIM PEL-reclaim operate on the SAME consumer's pending list.
    """
    return os.environ.get("HOSTNAME") or settings.consumer_name


SERVICE_KEY_MAP: dict[str, str] = {
    # KAG Ecosystem (longer prefixes first — /kag-* must match before /kag)
    "/kag-graph": "kag-graph",
    "/kag-orchestrator": "kag-orchestrator",
    "/graph-adapter": "graph-adapter",
    "/karma": "karma",
    "/hirag": "hirag",
    "/nlp": "nlp-service",
    # Core API
    "/workers": "workers",
    "/kanban": "kanban",
    "/kag": "kag",
    # ``/query/*`` is the SAME kag app as ``/kag`` above, and it is where almost
    # all of kag's real traffic lives. ``src/main.py`` of nonagenticai/kag mounts
    # ``query.router`` and ``ask.router`` at ``prefix="/query"`` (lines 887-888)
    # into the same FastAPI app that mounts ``orchestration.router`` at
    # ``prefix="/kag"`` (line 890) — one app, one deployment, one service key.
    # Adding this maps a SECOND prefix onto an EXISTING key; it invents no
    # service and chooses no price.
    #
    # MEASURED 2026-09-18 on prod (``billing_metering_events``, worker4): of
    # 3,660 rows in the whole retention window, 2,352 (64%) had a ``service_key``
    # starting ``unmapped``, and 1,201 of those were ``unmapped:query`` over
    # exactly two paths — ``/query/chunks`` (692) and ``/query/ask`` (509), both
    # ``status=allowed``, i.e. licensed traffic that was served. In the same
    # window a ``service_key`` of ``kag`` had ZERO rows: the ``/kag`` entry above only
    # ever catches the near-idle orchestration router, so kag's two busiest
    # routes fell through to the miss branch and accumulated under a defect
    # placeholder.
    #
    # ⚠️ THIS IS NOT THE ``/api`` CASE BELOW, and the difference is the whole
    # justification. ``/api`` stays unmapped because two-plus real backends share
    # that bare prefix and a path-prefix map cannot disambiguate them. ``/query``
    # has exactly one owner: a fleet-wide grep over every repo for
    # ``prefix=["']/query`` returns only those two kag lines, and both observed
    # paths are routes in that app (``@router.post("/ask")`` and
    # ``@router.post("/chunks")`` in ``src/routers/ask.py``). kag also names the
    # key itself — ``src/routers/query.py``'s billing docstring says the row is
    # "kag's own served, licensed request under the ``kag`` service key — the same
    # flat ``per_request`` unit ``/query/ask`` already emits", and its cost-bus
    # emitter posts under ``KAG_SERVICE``.
    #
    # ⛔ AND IT DECIDES NO PRICE. ``billing_app_cost_rates`` has no ``kag`` row
    # before or after this change (live: only ``fr-backend`` and ``workers``,
    # both 0.00000000), so services.billing's unpriced-key gate keeps REFUSING
    # the close exactly as it does today. All that changes is the name the
    # refusal and the stored row carry: a real service instead of
    # ``unmapped:query``. Setting a rate for ``kag`` is a separate, deliberate
    # pricing decision and is NOT made here.
    "/query": "kag",
    "/secrets": "secrets",
    "/tools": "tools",
    "/artifacts": "artifacts",
    "/procedures": "procedures",
    "/requirements": "requirements",
    "/mcp": "mcp",
    "/coding": "coding",
    "/skills": "skill-builder",
    "/licenses": "licenses",
    "/signal": "signal",
    # ``/autonomy/<capability>`` — control_service's Autonomy Control Service
    # (deployment `control-service`, repo nonagenticai/control_service) writes
    # DIRECTLY to the `metering:events` stream via its own
    # `services/billing/metering_stream.py` (a THIRD producer this map's own
    # design doc didn't know about, alongside license-authz and fr). It builds
    # the path itself as `f"/autonomy/{capability}"` and its own module
    # docstring says it expects the consumer to derive `service_key="autonomy"`  # pragma: allowlist secret
    # from the first path segment. Confirmed by live evidence, not by name:
    # `/autonomy/demandgen.track_funnel`, `/autonomy/recon.discover_sources`,
    # `/autonomy/strategy.draft_positioning` etc match 1:1 with capability
    # module names (`demandgen`/`recon`/`strategy`/`placement`) that exist ONLY
    # in that repo, and `grep`-ing every local repo for a producer of this path
    # shape found exactly one hit: metering_stream.py. No source_ip is present
    # on these rows (control_service writes to Redis directly, bypassing
    # license-authz's HTTP layer, so there is no access log to cross-check) —
    # the capability-name match plus the single-producer grep is the evidence.
    "/autonomy": "autonomy",
    # ⛔ "/api" is DELIBERATELY NOT MAPPED HERE — see the FIX 1 write-up. Live
    # evidence (2026-09-16) contradicts the premise that it belongs to
    # fr-backend alone: of 1,571 `unmapped:api` events in the last 24h, 1,506
    # (96%) have `source_ip` set to `control-service-waypoint`'s pod IP, and
    # every one of those paths (`/api/action-intents/decide`,
    # `/api/agent-run-steps`, `/api/deliverability-signals`,
    # `/api/provider-health-signals`, `/api/tenants/{id}/tier`, ...) is a route
    # DEFINED IN control_service's own `api/*.py` with a literal `/api/...`
    # prefix baked into the decorator — a different backend than fr, colliding
    # on the same bare first segment. fr's own `/api/*` traffic (source_ip
    # literally the string `"fr-backend"`, per its metering-emit.ts) is real
    # but has emitted ZERO events since 2026-09-11 — 5 days of silence as of
    # this writing. A handful more come from a marketing-owned waypoint
    # (a marketing-owned waypoint pod IP) and from master's public IP hitting scanner-shaped paths
    # (`/api/__authz_probe_nonexistent`, `/api/chat/completions`) that belong
    # to neither. A path-prefix map cannot disambiguate two-plus real backends
    # sharing one bare prefix; mapping "/api" to any single one of them would
    # silently misattribute the others' spend — the exact hazard this fix's
    # brief warns about for /autonomy, found instead on the branch that was
    # handed in as "already located". Left `unmapped:api` on purpose: the
    # alert stays honest and firing. The real fix is upstream — control_service
    # (and whichever other callers land here) should stop sharing fr's bare
    # `/api` prefix or should send an explicit `service_key` field; neither is
    # a change this map can make for them.
    # Flywheel / ML Pipeline
    "/inference-router": "inference-router",
    "/model-registry": "model-registry",
    "/data-collector": "data-collector",
    "/model-evaluator": "model-evaluator",
    "/training-data-generator": "training-data-generator",
    "/improvement-loop": "improvement-loop",
    "/rtx": "vllm",
    "/vllm": "vllm",
    # ``/laya/*`` -- the laya Jev bridge on pgx (host service, port 9210), the
    # licence-gated public route https://api.nonagentic.ai/laya/. gitops
    # clusters/strato/route-registry.yaml declares it ``path_prefix: /laya/``,
    # ``metering_service_key: laya`` (added 2026-10-10); this map is the only
    # place that declaration has to be mirrored. Missing it filed every served
    # /laya/v1/systemone call under ``unmapped:laya`` (9 rows, 2026-10-24
    # 16:15-17:59Z, all status=allowed) and fired
    # BillingServiceKeyUnmappedSustained. One owner, one key, like ``/query``
    # above; it decides no price (no ``laya`` rate row exists).
    "/laya": "laya",
    # Storage & Ingestion
    "/storage": "storage",
    "/ingestion": "ingestion",
    # Infrastructure
    "/npm": "npm",
}


#: What a path no prefix maps and no payload names is filed under. The value is
#: deliberately NOT a plausible service name: it has to be impossible to mistake
#: for one of the ~30 real keys beside it in a dashboard, an alert or a stored
#: row. The bare form is used when the first segment is unusable; otherwise the
#: segment is appended so the offender stays diagnosable.
UNMAPPED_SERVICE_KEY = "unmapped"  # pragma: allowlist secret
_UNMAPPED_PREFIX = f"{UNMAPPED_SERVICE_KEY}:"

#: The first path segment is CALLER-CONTROLLED and ends up both in a
#: ``String(64)`` column and in the ``service_key`` label of four Prometheus
#: counters, so it is admitted only in a slug shape and bounded in length. The
#: old code appended it with no check at all; this is strictly narrower.
_SEGMENT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,39}$")

#: ``metering_events.service_key`` is ``String(64)``.
_SERVICE_KEY_MAX = 64


#: How many distinct ``unmapped:<segment>`` keys one process will give their own
#: Prometheus series before collapsing the rest into :data:`_UNMAPPED_LABEL_OTHER`.
#:
#: The segment is CALLER-CONTROLLED, so its domain is unbounded by construction —
#: that is why the counter carried no label at all until 2026-09-05. But a bare
#: counter answered "how many" and never "who": 957 increments in 24h that could
#: not be attributed to any source service. The clamp is what makes the label
#: safe rather than the absence of a label. A hostile caller minting a fresh path
#: per request adds at most this many series per pod and then costs nothing,
#: while the realistic case — a handful of genuinely unmapped emitters — stays
#: individually named. 25 is ~2x the number of distinct prefixes in
#: SERVICE_KEY_MAP that could plausibly be missing one, and is a limit on SERIES,
#: not on counting: an overflowed key is still counted, under `unmapped:other`.
_UNMAPPED_LABEL_CAP = 25

#: The overflow bucket. A REAL value, not a sentinel, and deliberately inside
#: the ``unmapped:`` namespace so it can never be mistaken for a service name —
#: same rule as UNMAPPED_SERVICE_KEY itself.
_UNMAPPED_LABEL_OTHER = f"{_UNMAPPED_PREFIX}other"

#: Process-local admission set for the clamp above. Never pruned: a key that has
#: already been admitted must keep resolving to its own series for the life of
#: the process, or its counter would appear to reset when it fell out.
_unmapped_label_seen: set[str] = set()


def _clamp_unmapped_label(service_key: str) -> str:
    """Bound the label domain of ``billing_service_key_unmapped_total``.

    First :data:`_UNMAPPED_LABEL_CAP` distinct keys win their own series; every
    later one becomes :data:`_UNMAPPED_LABEL_OTHER`. No lock: this runs on the
    single-threaded asyncio consumer path and contains no await, so there is no
    interleaving point between the membership test and the insert.
    """
    if service_key in _unmapped_label_seen:
        return service_key
    if len(_unmapped_label_seen) < _UNMAPPED_LABEL_CAP:
        _unmapped_label_seen.add(service_key)
        return service_key
    return _UNMAPPED_LABEL_OTHER


def _reset_unmapped_label_clamp() -> None:
    """Drop the admission set. For tests only."""
    _unmapped_label_seen.clear()


def _note_unmapped_service_key(service_key: str) -> None:
    """Count a path that no prefix mapped and no payload named, BY key.

    ``service_key`` is the same honest ``unmapped:<segment>`` value the row is
    filed under, so the counter and the stored row agree on who the offender is
    — clamped through :func:`_clamp_unmapped_label` first, because that value is
    caller-controlled.

    Function-local import and a blind ``except`` for the same reason
    :func:`tenancy._note_unregistered_tenant` uses them: an observability defect
    must never be able to drop a metering event.
    """
    try:
        from observability import metrics as observability_metrics

        observability_metrics.service_key_unmapped_total.labels(
            service_key=_clamp_unmapped_label(service_key)
        ).inc()
    except Exception:  # pragma: no cover - observability must not break ingest
        pass


def extract_service_key(path: str, fields: Mapping[str, str] | None = None) -> str:
    """Decide which service an event is filed under.

    ⛔ THE MISS BRANCH USED TO FABRICATE. It returned ``parts[0]`` — the first
    path segment, verbatim and unchecked — so every request to fr's
    ``/api/...`` surface, ~2,733 events/day including the n8n webhooks, was
    filed under a service called ``api``. There is no service called ``api``.
    The value was indistinguishable from the ~30 real keys next to it: it is a
    lowercase slug, it appears in the same ``service_key`` column and the same
    Prometheus label, and nothing anywhere recorded that it had been invented
    rather than resolved. That is the same class of defect as the ``"unknown"``
    pseudo-tenant in :mod:`tenancy` — a plausible-looking value minted for an
    input nothing recognised, which then accumulates real rows.

    Resolution order:

    a. a prefix in :data:`SERVICE_KEY_MAP` → its key. UNCHANGED, and first, so
       every currently-mapped path resolves exactly as it did before.
    b. an explicit ``service_key`` on the payload → that value. The emitter
       naming ITSELF is evidence; deriving a name from its path is a guess.
       Measured 2026-09-05 on the live streams: ``billing:costs`` entries carry
       ``service_key`` (e.g. ``mlx-backend``) but ``billing:events`` entries do
       not — their fields are tenant/subject/license/app/plan, method, path,
       status, ``source_ip``, timestamp, latency. So this branch is dormant on
       the events stream today and exists so that an emitter that starts naming
       itself is believed rather than re-guessed.
    c. otherwise → ``unmapped:<first-segment>``, and a counter. ``source_ip`` is
       deliberately NOT used as a fallback even though it carries a service NAME
       rather than an IP for the fr traffic (``"fr-backend"``, 400/400 sampled):
       it names the CALLER, while every key in :data:`SERVICE_KEY_MAP` names the
       service being CONSUMED. Silently mixing the two under one label would
       replace a visible fabrication with an invisible one.
    """
    for prefix, key in SERVICE_KEY_MAP.items():
        if path.startswith(prefix):
            return key

    if fields:
        declared = fields.get("service_key")
        if isinstance(declared, str) and declared.strip():
            return declared.strip()[:_SERVICE_KEY_MAX]

    # Resolve the honest key FIRST, then count it. The counter used to fire
    # before this block and therefore could not name what it had just counted;
    # counting the resolved value is what lets the metric and the stored
    # service_key column be joined on the same offender.
    segment = path.strip("/").split("/")[0].lower()
    if _SEGMENT_PATTERN.match(segment):
        unmapped_key = f"{_UNMAPPED_PREFIX}{segment}"
    else:
        unmapped_key = UNMAPPED_SERVICE_KEY
    _note_unmapped_service_key(unmapped_key)
    return unmapped_key


async def ensure_consumer_groups(
    redis_conn: aioredis.Redis,
    settings: Settings,
) -> None:
    """Create consumer groups if they don't exist."""
    for stream in (settings.events_stream, settings.costs_stream):
        try:
            await redis_conn.xgroup_create(
                stream,
                settings.consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info("consumer_group_created", stream=stream, group=settings.consumer_group)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                pass  # Group already exists
            else:
                raise


def _stage_message(
    fields: dict[str, str],
    msg_id: str,
    stream_name: str,
    settings: Settings,
    event_buffer: list[dict],
    cost_buffer: list[dict],
) -> None:
    """Normalize one stream entry and append it to the right flush buffer.

    Shared by the steady-state XREADGROUP path and the XAUTOCLAIM PEL-reclaim
    path so a reclaimed (stranded) message goes through the exact same persist
    pipeline — the idempotency inbox in ``flush_costs`` then dedups any overlap.
    """
    # Empty-string sentinel from license-authz means "no subject" — normalize
    # to absent so the aggregator's _parse_uuid returns None and the raw row's
    # subject_id stays NULL.
    if fields.get("subject_id") == "":
        fields.pop("subject_id", None)
    # Tag with the Redis ack coordinates so the flusher can XACK after the DB
    # commit succeeds. Acking before persistence would lose events if the replica
    # restarts between buffer-append and the next flush — Redis has already
    # forgotten them and Postgres never saw them.
    fields["_msg_id"] = msg_id
    fields["_stream_name"] = stream_name
    if stream_name == settings.events_stream:
        fields["_service_key"] = extract_service_key(fields.get("path", ""), fields)
        event_buffer.append(fields)
        observability_metrics.events_in_total.inc()
    elif stream_name == settings.costs_stream:
        # Measure BEFORE the append, so the counter cannot be skipped by anything
        # the buffer does later. Cost stream only: metering events carry no
        # producer event time at all (billing_metering_events has no event_ts
        # column), so there is no backdating to observe on the usage stream.
        _note_backdated_event_ts(fields, settings)
        cost_buffer.append(fields)
        observability_metrics.cost_events_in_total.inc()


async def reclaim_pending(
    redis_conn: aioredis.Redis,
    settings: Settings,
    event_buffer: list[dict],
    cost_buffer: list[dict],
    consumer_name: str | None = None,
) -> int:
    """Drain stranded PEL entries via XAUTOCLAIM back through the persist path.

    FIX A2 — an ungraceful crash (OOM/SIGKILL/node-loss) leaves messages this
    consumer already XREADGROUP'd in its pending-entries-list forever; the
    steady-state ``">"`` read only returns never-delivered messages, so nothing
    reclaims them = silent under-bill. This claims entries idle longer than
    ``consumer_pel_reclaim_min_idle_seconds`` (so a freshly-read in-flight entry
    is never stolen mid-flush) and stages them like a normal read. The flusher
    then persists + XACKs them, and the idempotency inbox makes a re-claim of an
    already-persisted entry a no-op. Returns the number of entries staged.

    ``consumer_name`` is the per-pod-unique name (see :func:`resolve_consumer_name`);
    each replica reclaims under its OWN name, so with the min-idle window (~300s)
    a replica never steals an entry still genuinely in-flight in its peer. When
    None it is resolved from settings (local / unit path).

    The cursor loop terminates on the real-Redis ``"0-0"`` sentinel; it also
    breaks when a batch yields no entries NOT already seen this pass, which both
    bounds the loop on real Redis and tolerates fakeredis's non-converging
    cursor. A hard iteration cap is the final backstop.
    """
    if consumer_name is None:
        consumer_name = resolve_consumer_name(settings)
    min_idle_ms = settings.consumer_pel_reclaim_min_idle_seconds * 1000
    total = 0
    evicted = 0
    # The `stream_label` half is the bounded metric vocabulary (`usage` | `cost`,
    # the same values `billing_tenant_caller_unasserted_total{stream}` carries),
    # NOT the raw Redis name: the raw names come from settings, so they are
    # environment-dependent and cannot be birthed at import in
    # observability.metrics (which is deliberately settings-free and is imported
    # BY this module). Paired here because this is the one site that knows which
    # configured stream is which. Same streams, same order — iteration is
    # unchanged.
    for stream_name, stream_label in (
        (settings.events_stream, _STREAM_USAGE),
        (settings.costs_stream, _STREAM_COST),
    ):
        cursor: str = "0-0"
        seen: set[str] = set()
        for _ in range(10_000):  # backstop against a pathological cursor
            reply = await redis_conn.xautoclaim(
                stream_name,
                settings.consumer_group,
                consumer_name,
                min_idle_ms,
                start_id=cursor,
                count=100,
            )
            # An id XAUTOCLAIM could not claim because the entry no longer
            # exists in the stream is DATA LOSS, not housekeeping: the producer
            # bounds both ingest streams with `XADD MAXLEN ~ 100_000` and Redis
            # trims by COUNT regardless of PEL membership, so a long enough
            # persistence stall silently destroys un-acked entries. Redis drops
            # them from the PEL and reports them in one of two shapes:
            #
            #   Redis >= 7 (PRODUCTION: redis-server 7.4 + redis-py 8) — a THIRD
            #     reply element listing the dropped ids. reply[1] carries only
            #     live entries, so the nil-fields branch below never fires here.
            #   Redis 6.2 / redis-py < 5 — inline in reply[1] with nil fields.
            #     redis-py's parse_stream_list maps a nil entry to (None, None),
            #     so on that path the id itself is unrecoverable.
            #
            # Measured 2026-08-25: ingest runs at ~0.39 events/s against ~87k of
            # headroom under the cap, so reaching it needs a ~62-hour stall —
            # but if that ever changes, this must be loud.
            next_cursor = reply[0]
            claimed = cast("list[tuple[str | None, dict[str, str] | None]]", reply[1])
            deleted_ids = list(reply[2]) if len(reply) > 2 else []
            for lost_id in deleted_ids:
                evicted += 1
                # ⛔ COUNTED, not just logged. The comment above says this "must be
                # loud" if it ever happens; a structlog WARNING nobody greps on a
                # schedule is not loud, cannot be alerted on and cannot be read as
                # a rate. The log still names the individual id — that is its job;
                # the counter's job is to exist at zero so an `increase() > 0` rule
                # is fireable. All three reply shapes feed ONE series: they are one
                # population (a destroyed entry), and which shape Redis used is a
                # version question the log answers.
                observability_metrics.pel_entries_evicted_total.labels(stream=stream_label).inc()
                logger.warning(
                    "pel_entry_evicted_before_reclaim", stream=stream_name, msg_id=lost_id
                )
            # Nil inline entries are counted positionally, NOT through `seen`:
            # each one is a distinct loss and they all share the id `None`, so
            # deduping them would collapse N losses into 1.
            legacy_nils = [entry for entry in claimed if entry[0] is None]
            for _ in legacy_nils:
                evicted += 1
                observability_metrics.pel_entries_evicted_total.labels(stream=stream_label).inc()
                logger.warning(
                    "pel_entry_evicted_before_reclaim",
                    stream=stream_name,
                    msg_id=_LOST_ID_UNAVAILABLE,
                )
            new_entries = [(mid, f) for (mid, f) in claimed if mid is not None and mid not in seen]
            for msg_id, fields in new_entries:
                seen.add(msg_id)
                if not fields:  # nil entry that did keep its id — same loss
                    evicted += 1
                    observability_metrics.pel_entries_evicted_total.labels(
                        stream=stream_label
                    ).inc()
                    logger.warning(
                        "pel_entry_evicted_before_reclaim",
                        stream=stream_name,
                        msg_id=msg_id,
                    )
                    continue
                _stage_message(fields, msg_id, stream_name, settings, event_buffer, cost_buffer)
                total += 1
                # The DENOMINATOR for the eviction counter above. Incremented at
                # the same point, in the same loop, off the same `stream_label`, so
                # the two can never describe different populations and a loss RATE
                # is safe to take. Staged ≠ persisted — see the counter's HELP.
                observability_metrics.pel_entries_reclaimed_total.labels(stream=stream_label).inc()
            if next_cursor in ("0-0", "0", b"0-0", b"0"):
                break
            if not new_entries and not deleted_ids and not legacy_nils:
                # No progress (all already seen this pass) — done with this
                # stream. Protects against fakeredis returning the last id as a
                # non-terminal cursor forever. A batch that was ENTIRELY evicted
                # IS progress and must not stop the walk: on either shape the
                # live-entry list can be empty while the loss list is full.
                break
            cursor = next_cursor if isinstance(next_cursor, str) else next_cursor.decode()
    if total or evicted:
        # An all-evicted batch stages nothing; gating on `total` alone made the
        # total-loss case the one that logged NOTHING.
        logger.info("pel_entries_reclaimed", count=total, evicted=evicted)
    return total


async def consume_events(
    redis_conn: aioredis.Redis,
    settings: Settings,
    event_buffer: list[dict],
    cost_buffer: list[dict],
    stop_event: asyncio.Event,
) -> None:
    """XREADGROUP loop consuming from both streams.

    HA multi-consumer: the loop runs on EVERY replica (NOT leader-gated). It is
    correct without leader election because exactly-once is enforced downstream
    by the idempotency inbox in ``flush_costs`` + the FIX A2 XAUTOCLAIM reclaim.
    Each replica reads under a per-pod-unique consumer name
    (:func:`resolve_consumer_name`), so the two replicas are distinct consumers
    in the shared group and the stream is split between them (no double-read).

    On startup (and on ``consumer_pel_reclaim_interval_seconds`` thereafter) the
    loop runs an XAUTOCLAIM drain (FIX A2) to recover any PEL entries stranded by
    a crashed consumer (its own previous incarnation or a dead peer).

    FIX A1 — the loop also age-trims the cost stream (XTRIM MINID ~ now-36h) on
    ``cost_stream_trim_interval_seconds``. This runs here, independent of the
    leader-gated rollup loop, because that loop's advisory-lock election can be
    unavailable; XTRIM MINID is idempotent so both replicas trimming is harmless.
    """
    consumer_name = resolve_consumer_name(settings)
    streams: dict[str | bytes | memoryview, int | str | bytes | memoryview] = {
        settings.events_stream: ">",
        settings.costs_stream: ">",
    }

    logger.info(
        "consumer_started",
        events_stream=settings.events_stream,
        costs_stream=settings.costs_stream,
        group=settings.consumer_group,
        consumer=consumer_name,
    )

    groups_ready = False
    last_reclaim = 0.0
    reclaim_interval = settings.consumer_pel_reclaim_interval_seconds
    last_trim = 0.0
    trim_interval = settings.cost_stream_trim_interval_seconds

    try:
        while not stop_event.is_set():
            try:
                if not groups_ready:
                    await ensure_consumer_groups(redis_conn, settings)
                    groups_ready = True
                    # Startup PEL drain: reclaim anything a crashed consumer
                    # left stranded before reading new messages.
                    await reclaim_pending(
                        redis_conn, settings, event_buffer, cost_buffer, consumer_name
                    )
                    last_reclaim = time.monotonic()

                # Periodic PEL drain under this replica's own consumer name.
                now_m = time.monotonic()
                if now_m - last_reclaim >= reclaim_interval:
                    await reclaim_pending(
                        redis_conn, settings, event_buffer, cost_buffer, consumer_name
                    )
                    last_reclaim = now_m

                # FIX A1: age-bound BOTH ingest streams on their own interval,
                # decoupled from rollup leadership. Guarded so a Redis hiccup can't
                # abort the consume loop; the outer RedisError handler already
                # retries reads.
                if now_m - last_trim >= trim_interval:
                    try:
                        await trim_stream(redis_conn, settings.costs_stream)
                        await trim_stream(
                            redis_conn,
                            settings.events_stream,
                            max_age_hours=EVENTS_STREAM_MAX_AGE_HOURS,
                        )
                        # DLQ operability: age-bound the dead-letter stream too (its
                        # own longer horizon — quarantined events await manual
                        # replay/discard) and sample its depth once per interval so
                        # a growing quarantine backlog is alertable, not silent.
                        # #243: the dead-letter stream is ARCHIVED to Postgres
                        # before it is trimmed (Redis is its only copy), never
                        # plain-trimmed -- see archive_then_trim_dead_letter.
                        # An archive failure skips the trim and is logged; the
                        # next tick retries.
                        try:
                            await archive_then_trim_dead_letter(
                                redis_conn,
                                get_session_factory(),
                                settings.cost_dead_letter_stream,
                                max_age_hours=settings.dead_letter_stream_max_age_hours,
                            )
                        except RedisError:
                            raise
                        except Exception:
                            logger.warning("dlq_archive_failed_trim_skipped", exc_info=True)
                        depth = await redis_conn.xlen(settings.cost_dead_letter_stream)
                        observability_metrics.cost_dead_letter_depth.set(depth)
                        # ...and sample the OLDEST entry's age on the same tick.
                        # The trim two lines up is what destroys quarantined cost
                        # events at 30d, and depth cannot see that: a sub-threshold
                        # backlog can expire one entry at a time without ever
                        # crossing the depth alert's threshold of 500 (depth was
                        # 321 on 2026-09-08, 14 on 2026-09-14 — illustrations, not
                        # standing values). Age is the axis the loss
                        # actually moves along. Re-derived from the stream every
                        # tick, so it survives a pod roll (see sample_oldest_age).
                        await sample_oldest_age(redis_conn, settings.cost_dead_letter_stream)
                    except RedisError:
                        logger.warning("stream_trim_failed", exc_info=True)
                    last_trim = now_m

                # decode_responses=True → all stream payloads come back as str.
                results = cast(
                    "list[tuple[str, list[tuple[str, dict[str, str]]]]] | None",
                    await redis_conn.xreadgroup(
                        groupname=settings.consumer_group,
                        consumername=consumer_name,
                        streams=streams,
                        count=100,
                        block=1000,
                    ),
                )

                if not results:
                    # A real Redis honors the XREADGROUP `block` timeout
                    # server-side: it suspends this coroutine for up to `block`
                    # ms when the streams are empty, which yields the event loop.
                    # Some clients return instantly with no data instead (fakeredis
                    # >= 2.36 in tests; also a server that returns early). Without
                    # an explicit yield this poll loop would then spin without ever
                    # ceding control, starving every other task on the loop — the
                    # stop_event setter, the periodic flusher, the heartbeat — and
                    # pinning a CPU core. Yield once so the loop stays cooperative
                    # regardless of whether `block` actually suspended.
                    await asyncio.sleep(0)
                    continue

                for stream_name, entries in results:
                    for msg_id, fields in entries:
                        _stage_message(
                            fields, msg_id, stream_name, settings, event_buffer, cost_buffer
                        )

            except asyncio.CancelledError:
                break
            except aioredis.ResponseError as e:
                if "NOGROUP" in str(e):
                    logger.warning("consumer_group_lost_recreating", exc_info=True)
                    await ensure_consumer_groups(redis_conn, settings)
                else:
                    logger.warning("consumer_error", exc_info=True)
                await asyncio.sleep(1)
            except RedisError:
                # Transient Redis connectivity — sleep and retry.
                logger.warning("consumer_redis_error", exc_info=True)
                await asyncio.sleep(1)
            except (TimeoutError, OSError, KeyError, ValueError, TypeError):  # fmt: skip
                logger.error("consumer_error_unexpected", exc_info=True)
                await asyncio.sleep(1)
    finally:
        logger.info("consumer_stopped")
