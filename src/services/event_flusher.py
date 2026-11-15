"""Batch insert raw metering events into Postgres + update monthly usage."""

import uuid
from datetime import UTC, date, datetime
from typing import Any, cast

import redis.asyncio as aioredis
import structlog
from redis.exceptions import RedisError
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import observability.metrics as observability_metrics
from models.db import (
    NULL_PLAN_ID,
    NULL_SUBJECT_ID,
    MeteringEvent,
    MeteringMonthlyUsage,
    ProcessedEventMessage,
)
from services._parsers import (
    _normalize_tenant_id,
    _parse_uuid,
    _redact_tenant_id,
    note_metering_placeholder_value,
    parse_event_ts,
    stored_tenant_name,
)
from services.consumer import extract_service_key
from services.license_resolver import (
    derive_license,
    get_app_licence_map,
    get_app_map,
    get_resolution_map,
    parse_service_key_app_map,
    resolve_app,
    resolve_asserted_app_keys,
    split_asserted_app,
)
from services.metering_status import is_denied_status
from settings import get_settings
from timeutil import current_month_start

logger = structlog.stdlib.get_logger(__name__)


#: The ONLY ``status`` values admitted VERBATIM into the ``status`` label of
#: ``billing_metering_status_unrecognised_total``. Everything else becomes
#: :data:`_STATUS_LABEL_OTHER`.
#:
#: ⛔ WHY A CLAMP AT ALL. ``status`` is read straight off the wire
#: (``e.get("status", "allowed")``) and was passed UNBOUNDED into a Prometheus
#: label — the counter's own help text still says "labelled by the raw value".
#: A licensed emitter that puts a per-request string in that field (a message, an
#: exception class, a correlation id) mints one Prometheus series per request,
#: for the life of the process, on every replica. That is precisely the
#: cardinality bomb the 25-key clamp in :mod:`services.consumer` exists to stop
#: — arriving through the front door of a different counter.
#:
#: ⚠️ AN ALLOWLIST, NOT A FIRST-N-SEEN CAP, and the difference is deliberate.
#: ``services.consumer._clamp_unmapped_label`` admits the first 25 DISTINCT keys
#: it happens to see, because its domain (the first path segment) is genuinely
#: open and no list could be written. This domain is NOT open: only two writers
#: reach this branch, and the one that produces unrecognised values (fr) emits
#: HTTP status codes. So the admissible set can be ENUMERATED, which is strictly
#: tighter than a seen-cap: a hostile caller gets no series at all rather than
#: the first 25, there is no process-local mutable state, and every replica
#: agrees on the label domain (a seen-cap does not — two pods that saw different
#: values first disagree about which value is `other`, so a fleet-wide
#: ``sum by (status)`` over it is not well defined).
#:
#: ⚠️ NOTE, AND DELIBERATELY NOT FIXED HERE: consumer's ``_unmapped_label_seen``
#: is process-local and never pruned, so its clamp state resets on every pod
#: restart and differs per replica. That is a real wart in that module; it is out
#: of scope for this change and must not be "tidied" as a side effect.
_KNOWN_UNRECOGNISED_STATUSES: frozenset[str] = frozenset(
    {
        # fr's HTTP-code vocabulary — the whole reason this branch is reachable.
        # Measured 2026-09-04 over 30d: 159,085 rows carry an HTTP-code status.
        "200", "201", "202", "204", "206",
        "301", "302", "304",
        "400", "401", "403", "404", "405", "409", "410", "422", "429",
        "500", "501", "502", "503", "504",
    }
)  # fmt: skip

#: The overflow bucket. Leading underscore so it cannot collide with an HTTP
#: code or with a decision word, i.e. it can never be mistaken for a real status
#: the way a bare ``other`` could — the same rule that keeps consumer's overflow
#: bucket inside its ``unmapped:`` namespace.
_STATUS_LABEL_OTHER = "_other"


def _clamp_status_label(status: str) -> str:
    """Bound the label domain of ``billing_metering_status_unrecognised_total``.

    A known HTTP code keeps its own series; anything else — including the empty
    string and any caller-authored text — collapses into
    :data:`_STATUS_LABEL_OTHER`. This is a limit on SERIES, never on counting: an
    overflowed value is still counted, under ``_other``.
    """
    return status if status in _KNOWN_UNRECOGNISED_STATUSES else _STATUS_LABEL_OTHER


#: The width of the ``status`` COLUMN -- ``String(64)`` at
#: ``models/db/events.py:78`` (widened from 20 by alembic 0038; see that
#: column's comment). 20 was too narrow for real license-authz output:
#: measured live, ``allowed_service_principal`` (25) and its denied
#: counterpart ``denied_service_principal`` (24) both exceeded it, and the
#: latter is a genuine billing-integrity risk under the OLD bound -- see
#: :data:`_STATUS_TOO_LONG` below. 64 mirrors the sibling ``service_key``
#: column's bound and leaves headroom over every status literal seen live.
_STATUS_COLUMN_MAX = 64

#: What an over-long ``status`` is STORED as. Leading underscore for the same
#: reason as :data:`_STATUS_LABEL_OTHER`: it cannot be mistaken for an HTTP code
#: or a decision word, and critically it does NOT start with ``denied_`` and is
#: not ``allowed``, so it cannot accidentally satisfy the billing branches below
#: (``status.startswith("denied_")``, ``status in ("denied_budget", ...)``,
#: ``status == "allowed"``). Truncating at the column width would NOT be safe
#: for exactly that reason -- a hostile string beginning ``denied_`` and longer
#: than the column could survive truncation still matching, and a caller could
#: choose what the billing branch sees. The bound itself moved (20 -> 64,
#: alembic 0038) because 20 was cutting off REAL, non-hostile license-authz
#: vocabulary -- see :data:`_STATUS_COLUMN_MAX`; this sentinel and the defence
#: it embodies are unchanged and still needed for genuinely unbounded input
#: (e.g. an exception message, see below).
_STATUS_TOO_LONG = "_too_long"


def _bound_status_for_storage(status: str) -> str:
    """Bound ``status`` to what the column can physically hold.

    ⛔ FOUND BY A TEST WRITTEN FOR THE METRIC CLAMP, WHICH IS A DIFFERENT AND
    MILDER DEFECT. :func:`_clamp_status_label` bounds the Prometheus LABEL, but
    the value below goes into the row dict VERBATIM and the column is
    ``String(64)``. So a wire ``status`` longer than the column width does not
    just mint a series -- it raises ``psycopg.errors.StringDataRightTruncation``
    on the batch ``INSERT``.

    ⚠️ AND THE BLAST RADIUS IS THE BATCH, NOT THE ROW. These events are inserted
    in bulk, so ONE hostile or buggy emitter sending a stack-trace-shaped status
    (measured: ``"ValueError: connection reset by peer at ...req=abc123"``, 62
    chars) fails the whole flush, and every legitimate event batched with it is
    lost with it. `status` is read straight off the wire and no emitter contract
    constrains its length, so this needs no attacker -- an exception string
    passed where a status was expected is enough.

    Bounding rather than dropping the event: the row is billing volume, and the
    other columns are still trustworthy. The substitution is counted, so a
    misbehaving emitter is findable instead of merely survivable.
    """
    if len(status) <= _STATUS_COLUMN_MAX:
        return status
    observability_metrics.metering_status_overlong_total.inc()
    logger.warning(
        "metering_status_overlong",
        status_len=len(status),
        status_prefix=status[:32],
        stored_as=_STATUS_TOO_LONG,
    )
    return _STATUS_TOO_LONG


def _parse_retarget_tenant_id(raw: Any) -> str | None:
    """Canonicalise the optional wire ``retarget_tenant_id`` (billing-service#235).

    license-authz#65 emits the field ONLY when it retargeted the request, as a
    uuid string, never as ``""``. So:

    - absent / empty           -> ``None`` (no retarget; not counted)
    - a non-nil uuid           -> its canonical string form
    - anything else, incl. nil -> ``None``, counted and logged (redacted)

    ⛔ Deliberately NOT :func:`_normalize_tenant_id`: that sinks a bad value to
    the nil tenant, which here would assert a retarget nobody made. A refused
    value stores NULL and the event is still persisted; one malformed field must
    never fail the bulk INSERT (column is ``uuid``) and lose the whole batch.
    """
    if raw is None or raw == "":
        return None
    parsed = _parse_uuid(raw.strip() if isinstance(raw, str) else raw)
    if parsed is None or parsed.int == 0:
        observability_metrics.metering_retarget_tenant_invalid_total.inc()
        logger.warning(
            "metering_retarget_tenant_id_invalid",
            raw_retarget_tenant_id=_redact_tenant_id(raw),
            stored_as=None,
        )
        return None
    return str(parsed)


def _parse_actor_id(raw: Any, service_key: str) -> uuid.UUID | None:
    """Parse the optional wire ``actor_id`` (license-authz#73).

    Same contract as ``cost_flusher``: absent / empty -> ``None``, not counted;
    a UUID -> stored; anything else NON-EMPTY -> ``None`` and counted on
    ``billing_actor_malformed_total{service_key}``. Never guessed, and one bad
    value never fails the bulk INSERT.
    """
    if raw is None or raw == "":
        return None
    parsed = _parse_uuid(raw.strip() if isinstance(raw, str) else raw)
    if parsed is None:
        observability_metrics.actor_malformed_total.labels(service_key=service_key).inc()
    return parsed


def _note_license_resolution(result: str) -> None:
    """Count one licence-resolution OUTCOME on the usage path.

    ``result`` is ``resolved`` / ``unresolved`` / ``error`` and the three
    PARTITION the licence-less survivors of a batch — every row that arrived
    without a ``license_id`` increments exactly one of them, so
    ``resolved / (resolved + unresolved + error)`` is a true attribution rate.

    Function-local import and a blind ``except`` for the same reason
    :func:`services.consumer._note_unmapped_service_key` uses them, and here it
    is load-bearing rather than defensive habit: this runs on the metering flush
    path, and the failure direction that matters is that an unattributed row is
    bad while a LOST row is billing volume. An observability defect — including
    this counter not yet existing in :mod:`observability.metrics` — must never be
    able to drop a metering event.

    ⚠️ THE COST OF THAT CHOICE, STATED: if the counter is undefined this reads as
    a flat zero rather than raising, which is indistinguishable from "nothing was
    resolved". ``tests/test_event_flusher_license_resolution.py`` therefore
    asserts the metric names EXIST, so the absence is loud in CI instead of
    silent in production.
    """
    try:
        from observability import metrics as om

        om.metering_license_resolved_total.labels(result=result).inc()
    except Exception:  # pragma: no cover - observability must not drop an event
        pass


def _note_app_resolution(result: str) -> None:
    """Count one app-derivation OUTCOME on the usage path.

    Same contract, same partition and same blind-except reasoning as
    :func:`_note_license_resolution`. Counted on its OWN series rather than
    folded into that one: the two answer different questions over different
    populations (every app-less survivor vs every licence-less survivor), and
    one metric name carrying two populations makes every ratio taken against it
    wrong — the defect ``billing_placeholder_value_total`` documents at length.
    """
    try:
        from observability import metrics as om

        om.metering_app_resolved_total.labels(result=result).inc()
    except Exception:  # pragma: no cover - observability must not drop an event
        pass


async def flush_events(
    events: list[dict[str, Any]],
    session_factory: async_sessionmaker[AsyncSession],
    redis_conn: aioredis.Redis | None,
    consumer_group: str | None = None,
) -> int:
    """Batch insert metering events and update monthly usage counters.

    ``consumer_group`` enables ACK-after-commit: events carry ``_msg_id`` +
    ``_stream_name`` set by ``services.consumer.consume_events``; after the
    DB commit succeeds, the Redis Streams entries are XACK'd so they leave
    the pending-entries-list. Pass ``None`` (tests, direct callers) to skip
    acking — events without those tags are also skipped.
    """
    if not events:
        return 0

    batch = list(events)
    events.clear()
    # Snapshot ack coordinates parallel to the batch *before* any await; the
    # consumer mutates `events` between awaits.
    pending_acks: list[tuple[Any, Any]] = []
    if consumer_group is not None and redis_conn is not None:
        for e in batch:
            stream = e.get("_stream_name")
            msg_id = e.get("_msg_id")
            if stream is not None and msg_id is not None:
                pending_acks.append((stream, msg_id))

    # Pre-build each event's raw-event row dict alongside its Redis stream
    # message id (2nd tuple element) and the fields needed to compute the
    # monthly key. Aggregation is deferred until AFTER the idempotency claim
    # inside the session so a redelivered message never double-counts. None as
    # the msg_id means the event did not arrive via the stream (a direct test
    # insert), so it always survives.
    pre_rows: list[
        tuple[dict[str, Any], str | None, uuid.UUID | None, uuid.UUID | None, uuid.UUID | None, str]
    ] = []

    # ONE receipt instant for the whole batch, captured BEFORE the loop — the
    # same shape `cost_flusher` uses. It is `parse_event_ts`'s skew reference:
    # a producer timestamp AFTER `received_at` is refused as a skewed clock. Read
    # per-event instead, `now` would creep forward inside the loop, so two events
    # from the same producer instant could get different verdicts depending on
    # where they sat in the batch — a non-deterministic refusal, and the later
    # events would be judged against a laxer bound than the earlier ones.
    now = datetime.now(UTC)

    # id(row) -> the app KEY the producer asserted in `app_id` (X-App-Id slug).
    # Resolved through the mirror; a keyed row is NEVER derived from its licence
    # (for router traffic the licence names the router, not the caller). Keys
    # are rows kept alive by `pre_rows`, so id() is stable for the batch.
    asserted_app_key: dict[int, str] = {}

    for e in batch:
        license_id = _parse_uuid(e.get("license_id", ""))
        subject_id = _parse_uuid(e.get("subject_id", ""))
        # A uuid is kept as asserted; a non-uuid string is an app KEY (X-App-Id
        # slug) resolved after the claim -- see `asserted_app_key`.
        app_id, app_key = split_asserted_app(e.get("app_id"))
        plan_id = _parse_uuid(e.get("plan_id", ""))
        # NULLABLE, NEVER DEFAULTED. A producer that has a project stamps a
        # top-level `project_id` and omits the field entirely when it has none,
        # so absence must stay absence — no sentinel, no "unknown". Cf.
        # metadata.job_id, which was defaulted to the literal "unknown" and so
        # read as 100% coverage of a value no caller ever populated.
        #
        # ⚠️ DELIBERATELY UNPOPULATED TODAY — NOT A DEFECT, AND NOT A TODO.
        # NOTHING writes `project_id` to the USAGE stream (`billing:events`).
        # inference-router stamps it only in `services/metering.py`'s
        # `emit_cost_event` (~:932), which xadds to the COST stream
        # (`billing:costs`, drained by `cost_flusher`); the only two usage-stream
        # producers — `license-authz/src/main.py` and
        # `fr/backend/src/services/metering-emit.ts` — never mention the field at
        # all. So `billing_metering_events.project_id` is NULL on every row, and
        # will stay NULL until a usage-bus producer starts sending one.
        #
        # This parse is kept anyway: it is receiving-end preparation, it costs
        # one dict lookup, and it is the ONE place a future usage producer's
        # value can land. ⛔ Do NOT read the permanently-NULL column as a bug and
        # ⛔ do NOT "fix" it with a default or a sentinel — that is exactly the
        # job_id failure above, and it would manufacture coverage of a value no
        # caller ever sent. If the column must be populated, fix the PRODUCER.
        project_id = _parse_uuid(e.get("project_id", ""))

        service_key = e.get("_service_key") or extract_service_key(e.get("path", ""), e)
        status = _bound_status_for_storage(e.get("status", "allowed"))

        tenant_id, tenant_invalid = _normalize_tenant_id(e.get("tenant_id", ""))
        if tenant_invalid:
            # INGRESS, and both names are incremented at this one site as exact
            # aliases. `tenant_unattributed_total` is MISNAMED: it fires the
            # moment the wire is read, before normalisation decides what the
            # row is stored under, so it measures "the emitter sent a non-UUID
            # tenant_id", not "the row is unattributed". Measured 10h on
            # 2026-09-05: +1,100 here against ZERO stored rows with a NULL
            # tenant_id. The old name is kept emitted rather than renamed away
            # because a metric name is a published interface and a hard rename
            # makes every query on it vacuously empty rather than erroring.
            observability_metrics.tenant_caller_unasserted_total.labels(stream="usage").inc()
            observability_metrics.tenant_unattributed_total.labels(stream="usage").inc()
            logger.warning(
                "tenant_id_unattributed",
                stream="usage",
                raw_tenant_id=_redact_tenant_id(e.get("tenant_id", "")),
                license_id=str(license_id) if license_id else None,
                service_key=service_key,
            )

        # ⚠️ DO NOT FABRICATE, and keep the miss VISIBLE. Counted on this
        # stream's OWN counter, deliberately NOT on
        # billing_placeholder_value_total: that one's help text names a cost
        # denominator (billing_cost_events_by_service_total), so folding metering
        # rows into it would give one series two populations and make every ratio
        # taken against it wrong. Same shape, same labels, separate denominator.
        tenant_name, tenant_name_unresolved = stored_tenant_name(
            tenant_id, e.get("tenant_name", ""), e.get("tenant_id", "")
        )
        if tenant_name_unresolved:
            note_metering_placeholder_value("tenant_name", service_key)

        row = {
            "tenant_id": tenant_id,
            # Optional retarget target from license-authz#65; NULL when absent
            # or malformed (counted). See `_parse_retarget_tenant_id`.
            "retarget_tenant_id": _parse_retarget_tenant_id(e.get("retarget_tenant_id")),
            # WHO ACTED; NULL when absent or malformed (counted). See `_parse_actor_id`.
            "actor_id": _parse_actor_id(e.get("actor_id"), service_key),
            # See cost_flusher: tenant_id is a uuid now, so the display column
            # falls back to the RAW emitted label (credential-redacted), never
            # to the id.
            #
            # ⛔ AND THAT FALLBACK STORED THE UUID ITSELF (billing-service #135).
            # Since 0011 the emitters send a uuid as tenant_id and no tenant_name,
            # so `original_tenant_label` preserved the uuid into a display column:
            # 9 of 10 tenants, 4,469 rows, and not one honest absence among them.
            # Worse than an absence, because a uuid is non-empty and per-tenant
            # distinct, so it satisfies downstream truthiness guards and reads as
            # data — fr's metering page guards its id column with
            # `{#if t.tenant_name}` and printed the same id twice.
            #
            # `stored_tenant_name` is the ONE helper cost_flusher also calls: the
            # two paths carried the identical expression and therefore the
            # identical defect, so the fix is at the shared point rather than
            # copied. It resolves from the live registry first, never fabricates,
            # never raises, and flags a non-answer for the counter below.
            "tenant_name": tenant_name,
            "license_id": license_id,
            "subject_id": subject_id,
            "app_id": app_id,
            "plan_id": plan_id,
            # NULLABLE, NEVER DEFAULTED, and NULL on every row today because no
            # usage-stream producer sends the field — see the parse site above.
            "project_id": project_id,
            "plan_code": e.get("plan_code"),
            "method": e.get("method", "GET"),
            "path": e.get("path", "/"),
            "service_key": service_key,
            "status": status,
            "source_ip": e.get("source_ip"),
            # ⭐ THE PRODUCER'S EVENT TIME — the half that made alembic 0034 and
            # the rollup's `coalesce(event_ts, created_at)` INERT. The schema and
            # the reader were both ready; nothing wrote the column, so every new
            # row was still bucketed on ingest time exactly as before.
            #
            # The wire genuinely carries it: measured 2026-09-05 on the live
            # `billing:events` stream, whose fields are
            # tenant/subject/license/app/plan, method, path, status, source_ip,
            # `timestamp`, latency (recorded at services/consumer.py:286). It
            # arrived intact and was dropped on the floor here.
            #
            # ⛔ `parse_event_ts` IS REUSED, NOT REIMPLEMENTED. It is the same
            # function `cost_flusher` calls, and its refusals are the load-bearing
            # part: it returns None — storing NULL — for anything it cannot vouch
            # for (missing, malformed, NAIVE, epoch-numeric, ahead of
            # `received_at`, or backdated past MAX_EVENT_TS_BACKDATE_DAYS). A
            # second parser here would drift from those rules silently, and a
            # wrongly-parsed event time is WORSE than a NULL one: NULL makes
            # `coalesce` fall back to the known-true ingest instant and be
            # counted, whereas a wrong instant moves a request into another
            # calendar month's immutable invoice with nothing flagged.
            #
            # ⛔ NEVER SUBSTITUTE `now`/`created_at` FOR A REFUSAL. That restates
            # ingest time as producer event time under a column that promises
            # otherwise, and it makes the fallback counter read zero while
            # measuring nothing.
            "event_ts": parse_event_ts(e.get("timestamp"), received_at=now),
        }
        pre_rows.append((row, e.get("_msg_id"), license_id, subject_id, plan_id, status))
        if app_key is not None:
            asserted_app_key[id(row)] = app_key

    # Robust-by-construction dedup: collapse any repeated stream msg id
    # (``p[1]``) to its FIRST occurrence BEFORE the idempotency claim + the
    # ``survived`` filter below. The claim yields a SET of claimed ids, so the
    # membership test ``p[1] in claimed`` would let BOTH copies of a doubled id
    # survive → double raw-insert + double additive monthly rollup. Today the 300s XAUTOCLAIM min-idle window makes an
    # intra-batch duplicate unreachable, but this defends it by construction,
    # not by timing. None-id rows (direct/test inserts) carry no dedup key and
    # are ALL kept.
    seen_msg_ids: set[str] = set()
    deduped_pre_rows = []
    for pr in pre_rows:
        mid = pr[1]
        if mid is not None:
            if mid in seen_msg_ids:
                continue
            seen_msg_ids.add(mid)
        deduped_pre_rows.append(pr)
    pre_rows = deduped_pre_rows

    if not pre_rows:
        return 0

    rows: list[dict[str, Any]] = []
    # monthly_counts tracks (license_id, subject_id, plan_id, month) ->
    # (allowed, denied, budget_denied). subject_id is the sentinel string when
    # the raw event lacks one — keeps S2S/system rows on a separate row from
    # per-user rows for the same license+plan.
    monthly_counts: dict[tuple[str, str, str, date], tuple[int, int, int]] = {}

    async with session_factory() as session:
        # --- Idempotency claim (SAME transaction as the raw-event insert +
        # monthly rollup) ---
        # Claim each Redis stream message id with INSERT … ON CONFLICT DO
        # NOTHING. A redelivered message — crash-after-commit-before-XACK, or a
        # NOGROUP recovery that recreates the group at id=0 against a now-durable
        # stream — conflicts and is dropped, so it is never re-counted. The claim
        # and the event insert + additive rollup commit atomically, so a crash
        # mid-flush rolls back all three (the message stays un-ACKed and replays).
        claim_ids = [mid for (_r, mid, *_rest) in pre_rows if mid]
        if claim_ids:
            claim_stmt = pg_insert(ProcessedEventMessage).values(
                [{"stream_msg_id": m} for m in claim_ids]
            )
            claim_stmt = claim_stmt.on_conflict_do_nothing(index_elements=["stream_msg_id"])
            claimed = {
                r[0]
                for r in await session.execute(
                    claim_stmt.returning(ProcessedEventMessage.stream_msg_id)
                )
            }
            dup = len(claim_ids) - len(claimed)
            if dup:
                observability_metrics.usage_events_deduplicated_total.inc(dup)
                logger.info("usage_events_deduplicated", skipped=dup, batch=len(pre_rows))
            survived = [p for p in pre_rows if p[1] is None or p[1] in claimed]
        else:
            survived = list(pre_rows)

        # --- Resolve a licence + app for events that arrived without one ------
        #
        # ⛔ WHAT WAS BROKEN: THERE WAS NO LOOKUP AT ALL. This function took
        # `license_id` and `app_id` straight off the wire and stored whatever
        # arrived, while `services.license_resolver` — which exists precisely to
        # answer "which of this tenant's licences does this event belong to?" —
        # was called from `services.cost_flusher` ONLY. The usage stream never
        # consulted it. Measured on prod over 7 days: billing_metering_events held
        # 70,658 rows, 100% tenant-attributed, with license_id AND app_id non-null
        # on 444 of them — 0.628%. Lifetime, 499 of 78,353.
        #
        # It is not fixable upstream: fr's emitter hardcodes `license_id: ''` /
        # `app_id: ''` (backend/src/services/metering-emit.ts) because fr-backend
        # does not sit behind license-authz and genuinely does not hold either id
        # at its emit point. The tenant IS known, and the tenant plus the service
        # is exactly what the resolver keys on.
        #
        # WHAT THE 99.4% COSTS DOWNSTREAM: rollup_engine coalesces the NULL to
        # NULL_LICENSE_ID, and every money path then skips that sentinel
        # (services.billing, services.invoices), so the traffic produces no
        # invoice line — and `assert_all_service_keys_priced` filters the sentinel
        # out BEFORE testing rates, so the month-close gate cannot even refuse on
        # it. The row is stored, billable, and unreachable by billing.
        #
        # ⚠️ MIRRORS cost_flusher's call site DELIBERATELY, including its ordering,
        # its caching and its failure handling — that site is reviewed and
        # correct, and a second pattern for the same job is how the two paths
        # drift. Same TTL'd process-local maps (`get_resolution_map` /
        # `get_app_map`, 300s), same SAVEPOINT per map build, same
        # licence-then-app order so a licence resolved here also yields an app.
        #
        # PLACED HERE for cost_flusher's reason: AFTER the idempotency claim (so a
        # dropped redelivery is never resolved or counted) and BEFORE the monthly
        # aggregation below, which keys `monthly_counts` on the licence. Resolving
        # after that block would store one licence on the raw row and roll the
        # month up against a different one — the two must never disagree.
        #
        # ⚠️ AN EXPLICIT VALUE ON THE WIRE WINS, ALWAYS. Both loops run only over
        # rows whose id is None, so a licence or app the EMITTER asserted is never
        # overwritten by one we inferred. license-authz and control_service do
        # send real ids; they know more than we can derive, and a derived value
        # silently replacing an asserted one would make the stored column
        # unreadable as evidence of either.
        #
        # FAIL-SOFT, AND WHAT THAT BUYS: each map build sits in its own
        # `session.begin_nested()` SAVEPOINT. A statement error rolls back the
        # nested scope only, so the outer transaction stays usable and the raw
        # INSERT below still commits — `except Exception` alone would merely defer
        # the death to that INSERT as a PendingRollbackError naming the wrong
        # statement. ⛔ A RESOLUTION MISS OR ERROR MUST NEVER DROP THE EVENT: an
        # unattributed metering row is bad, a lost one is billing volume. So every
        # failure path leaves the id NULL — today's exact behaviour — and COUNTS.
        # Not covered, stated rather than implied: if the CONNECTION dies the
        # enclosing transaction is gone and no savepoint rescues it; the flush
        # raises, the claim rolls back, the messages stay un-ACKed and replay.
        # That costs a retry, never the data.

        # Computed once, before either loop, so the three `result` values
        # PARTITION this population exactly — one increment per licence-less
        # survivor, never two and never none.
        #
        # ⭐ The asserted app KEY (X-App-Id) is resolved FIRST, in parity with
        # cost_flusher: licence derivation keys on the asserted app, because a
        # delegated call to a shared leaf (licence deliberately dropped by the
        # leaf) is attributable only via (tenant, asserted app) — see
        # license_resolver.derive_license.
        keyed_rows = [
            (row, asserted_app_key[id(row)])
            for row, *_rest in survived
            if id(row) in asserted_app_key
        ]
        await resolve_asserted_app_keys(session, keyed_rows, stream="metering")
        unlicensed_rows = [row for row, *_rest in survived if row["license_id"] is None]
        if unlicensed_rows:
            # A ``None`` VALUE in the map means "several active licences for
            # that (tenant, app), deliberately unresolved"; the map being None
            # means "not built". ``resolve_license`` distinguishes the two and
            # counts the first on billing_license_ambiguous_total.
            resolution_map: dict[tuple[str, str], uuid.UUID | None] | None = None
            app_licence_map: dict[tuple[str, uuid.UUID], uuid.UUID | None] | None = None
            app_by_key: dict[str, str] = {}
            resolution_errored = False

            def _asserted(row: dict[str, Any]) -> bool:
                return row["app_id"] is not None or id(row) in asserted_app_key

            try:
                app_by_key = parse_service_key_app_map(
                    get_settings().cost_attribution_app_by_service_key
                )
                if app_by_key:
                    async with session.begin_nested():
                        resolution_map = await get_resolution_map(session)
            except Exception:
                # `error` and `unresolved` are kept APART on purpose. An empty
                # operator map, an ambiguous (tenant, app) pair and an unknown
                # service_key are all "we correctly declined to guess"; a raised
                # map build is "the resolver did not run". Folding them would make
                # a broken resolver look like a fleet of honestly-unattributable
                # events — the failure `attribution_ambiguous_pairs` already
                # records as its own blind spot.
                resolution_errored = True
                logger.warning("metering_license_resolution_failed", exc_info=True)

            # Own try, same reason as cost_flusher: a failed (tenant, app) map
            # read errors only the ASSERTED rows; un-asserted rows keep their
            # service_key derivation.
            app_map_errored = False
            if any(_asserted(row) for row in unlicensed_rows):
                try:
                    async with session.begin_nested():
                        app_licence_map = await get_app_licence_map(session)
                except Exception:
                    app_map_errored = True
                    logger.warning("metering_app_licence_map_failed", exc_info=True)

            for row in unlicensed_rows:
                if resolution_errored or (app_map_errored and _asserted(row)):
                    _note_license_resolution("error")
                    continue
                resolved = derive_license(
                    tenant_id=str(row["tenant_id"]) if row["tenant_id"] else None,
                    service_key=row["service_key"],
                    app_asserted=_asserted(row),
                    asserted_app_id=row["app_id"],
                    app_by_service_key=app_by_key,
                    resolution_map=resolution_map,
                    app_licence_map=app_licence_map,
                )
                if resolved is None:
                    # Rule 1 of the resolver: never fabricate. No match, an
                    # ambiguous match, or an unmapped service_key all leave the
                    # column NULL and stay counted. A wrong license_id is an
                    # invented invoice line and is strictly worse than a missing
                    # one.
                    _note_license_resolution("unresolved")
                    continue
                row["license_id"] = resolved
                _note_license_resolution("resolved")

        # App derivation runs AFTER the block above so a licence resolved there
        # also yields an app, and in its OWN try/except because it consults only
        # the event's licence — it must NOT be skipped when the operator has left
        # `cost_attribution_app_by_service_key` empty, which gates the licence
        # half. Unambiguous by construction: license_id is the PRIMARY KEY of
        # request_quota_config, so a licence maps to exactly one app and the
        # "exactly one, or nothing" rule that drops duplicate (tenant, app) pairs
        # cannot apply here.
        # (Asserted app keys were resolved above, before licence derivation.)
        for row, _key in keyed_rows:
            # Same partition as below: a keyed row the mirror could not resolve
            # is stored app-less, and counted, but never licence-derived.
            _note_app_resolution("resolved" if row["app_id"] is not None else "unresolved")
        appless_rows = [
            row
            for row, *_rest in survived
            if row["app_id"] is None and id(row) not in asserted_app_key
        ]
        if appless_rows:
            app_map: dict[uuid.UUID, uuid.UUID] = {}
            app_errored = False
            try:
                async with session.begin_nested():
                    app_map = await get_app_map(session)
            except Exception:
                app_errored = True
                logger.warning("metering_app_resolution_failed", exc_info=True)

            for row in appless_rows:
                if app_errored:
                    _note_app_resolution("error")
                    continue
                derived_app = resolve_app(license_id=row["license_id"], app_map=app_map)
                if derived_app is None:
                    # No licence at all, or a licence whose app the local mirror
                    # does not know (synced before migration 0014). Keep it NULL
                    # and keep counting it.
                    _note_app_resolution("unresolved")
                    continue
                row["app_id"] = derived_app
                _note_app_resolution("resolved")

        # ⛔ RE-READ THE IDS OFF `row` INTO THE TUPLE. `survived`'s elements carry
        # license_id/plan_id as tuple slots that were built from the WIRE in the
        # pre_rows loop, and the aggregation below reads the TUPLE, not the dict.
        # Without this line a licence resolved above would be stored on the raw
        # event and still be invisible to `monthly_counts` — the raw row and the
        # rollup would disagree about the same event, which is the exact hazard
        # the ordering note above exists to prevent.
        survived = [
            (row, mid, row["license_id"], subject_id, plan_id, status)
            for row, mid, _lic, subject_id, plan_id, status in survived
        ]

        # Build the raw-event rows AND the monthly aggregation from SURVIVORS
        # ONLY, so a deduped redelivery never double-inserts a raw event nor
        # double-increments request_count/denied_count.
        month_start = current_month_start()
        for row, _mid, license_id, subject_id, plan_id, status in survived:
            rows.append(row)
            # ⭐ THE GUARD REQUIRES `license_id` ALONE. It used to require
            # `license_id and plan_id`, and that second conjunct is why
            # `billing_metering_monthly_usage` was effectively EMPTY: measured
            # live, 6 rows / 5 distinct licences against 160 rows in
            # `request_quota_config` — 155 of 160 (96.9%) configured licences
            # had no usage row at ALL. With no row `services.quota._get_db_usage`
            # finds nothing, so the quota check fails open forever: a licence
            # capped at 100k/month serves 400k and still reads `used=0`.
            #
            # `plan_id` was never obtainable here, so the conjunct could only
            # ever be false for real traffic. It arrives ONLY off the wire
            # (`_parse_uuid(e.get("plan_id", ""))` at :304) — billing mirrors no
            # plan: `request_quota_config` has no plan_id/plan_code column,
            # there is no Plan model under models/, and there is no
            # `resolve_plan` beside `resolve_app` because nothing is mirrored to
            # resolve FROM. So the gate was conditioned on a field this service
            # cannot produce.
            #
            # Substituting NULL_PLAN_ID is NOT inventing a plan. It is the same
            # sentinel `rollup_engine` already coalesces to for the hourly/daily
            # grain (`func.coalesce(MeteringEvent.plan_id, NULL_PLAN_ID)`), and
            # it means "no plan was asserted", never a real plan. A row that DOES
            # carry a plan keeps its own grain, so a mid-month plan change still
            # splits into two rows — nothing is flattened and no accounting
            # changes. Both readers SUM across the plan grain, so the sentinel
            # rows land in exactly the aggregate quota already reads.
            #
            # ⛔ NO ENFORCEMENT DECISION CHANGES HERE. No threshold moves, no
            # fail-open direction flips, and `check_quota` is untouched. This
            # makes the COUNTER EXIST; ARMING it is a separate, later decision.
            # ⛔ AND IT IS NOT SUFFICIENT ON ITS OWN — see the `else` of the
            # status dispatch below, which is the remaining blocker for the
            # population that matters.
            if license_id:
                # ⛔ FIXED: THIS USED TO BE A THREE-WAY SPLIT WITH A SILENT
                # THIRD CASE. `billing_metering_events.status` has two writers:
                # license-authz writes decisions (`allowed`, `denied_invalid_
                # token`, `denied_app_mismatch`, …) and fr — the dominant
                # caller — writes bare HTTP codes (`200`, `201`, `404`, `422`).
                # The OLD code tested `status.startswith("denied_") or
                # status == "denied"` for denial and `status == "allowed"` for
                # billable, and an HTTP code satisfies NEITHER spelling — so it
                # fell into a bare `else` that incremented `request_count`
                # NOWHERE while `rollup_engine`'s daily grain billed the exact
                # same row as ordinary traffic (it treats "everything that is
                # NOT denied" as billable, deliberately, for this reason).
                # MEASURED 2026-09-04 (30d): 159,085 rows carried an HTTP-code
                # status; that whole population was invisible to
                # `billing_metering_monthly_usage` (and so to quota usage),
                # while still counted daily.
                #
                # `is_denied_status` is `services.metering_status`'s ONE
                # definition of "denied", reused verbatim by `rollup_engine` —
                # see that module's docstring for the full rationale. There is
                # no longer a third case: anything not denied is billable,
                # matching the daily grain exactly.
                is_denied = is_denied_status(status)
                is_budget_denied = status in ("denied_budget", "denied_quota")
                sub_key = str(subject_id) if subject_id else str(NULL_SUBJECT_ID)
                plan_key = str(plan_id) if plan_id else str(NULL_PLAN_ID)
                key = (str(license_id), sub_key, plan_key, month_start)
                prev = monthly_counts.get(key, (0, 0, 0))
                if is_denied:
                    monthly_counts[key] = (
                        prev[0],
                        prev[1] + 1,
                        prev[2] + (1 if is_budget_denied else 0),
                    )
                else:
                    # BILLABLE — every non-denied status, including every HTTP
                    # code fr emits. This is the branch that used to be split
                    # into `status == "allowed"` (counted) and everything else
                    # (silently dropped); both now count identically.
                    monthly_counts[key] = (prev[0] + 1, prev[1], prev[2])

                    if status != "allowed":
                        # ⚠️ OBSERVABILITY ONLY FROM HERE DOWN. This used to be
                        # the branch that suppressed the count; it no longer
                        # does — `billing_metering_status_unrecognised_total`
                        # stays a live detector for "a status outside the known
                        # vocabulary arrived", it just no longer controls
                        # whether the request gets billed.
                        #
                        # ⛔ CLAMPED. `status` is caller-controlled and passed
                        # VERBATIM into this label, so a licensed emitter
                        # sending a per-request string would mint an unbounded
                        # number of Prometheus series — see
                        # _KNOWN_UNRECOGNISED_STATUSES for why an enumerated
                        # allowlist is the right clamp here and a
                        # first-N-seen cap (consumer's) is not. The LOG still
                        # carries the raw value, so the offender stays
                        # diagnosable at full fidelity; only the label is
                        # bounded.
                        observability_metrics.metering_status_unrecognised_total.labels(
                            status=_clamp_status_label(status)
                        ).inc()
                        # An enumerated HTTP code is fully described by the
                        # metric's own per-status series, so a WARNING adds
                        # nothing it does not already carry: logged at debug,
                        # counted at full fidelity. Anything OUTSIDE that set
                        # is genuinely surprising — an emitter inventing a
                        # vocabulary — and stays a WARNING; it stays rare by
                        # construction because `_clamp_status_label` collapses
                        # it to `_other`.
                        _log = (
                            logger.debug
                            if status in _KNOWN_UNRECOGNISED_STATUSES
                            else logger.warning
                        )
                        _log(
                            "metering_status_unrecognised",
                            status=status,
                            license_id=str(license_id),
                            plan_id=str(plan_id),
                        )
            else:
                # ⛔ THE OUTER GUARD, AND UNTIL NOW IT WAS COUNTED BY NOTHING.
                #
                # The row above was APPENDED to `rows` and will be INSERTED into
                # billing_metering_events. It then contributes nothing to
                # metering_monthly_usage (and so nothing to quota usage),
                # because one or both of the two ids this aggregation keys on is
                # absent. The INNER `else` above
                # (an unrecognised `status` with both ids present) already has
                # `billing_metering_status_unrecognised_total`; this branch — the
                # far larger population — had nothing at all, so the gap was
                # visible only by reading this function.
                #
                # MEASURED 2026-09-04 (30d): 159,085 rows carry an HTTP-code
                # status and NOT ONE of them carries both a license_id and a
                # plan_id. That is this branch, and it is the NORMAL population
                # today — which is precisely why it is dangerous: quota
                # under-counts for every one of them, and nothing anywhere said so.
                #
                # ⛔ BEHAVIOUR IS UNCHANGED. This `else` did not exist; nothing
                # was executed here before and nothing is executed here now
                # except the counter. The guard still decides exactly what it
                # decided — the decision is merely countable.
                #
                # NO LOG LINE HERE, deliberately, and the asymmetry with the
                # inner `else` is the point: that branch is rare, so a WARNING
                # per occurrence is readable; this one fires on the majority of
                # events, so a WARNING per occurrence would bury every other
                # line in the log. The reason label carries the whole diagnosis.
                # ⚠️ ROUND 9 — `plan_id` NO LONGER GATES THIS BRANCH. The guard
                # above now requires `license_id` ALONE, so a licensed event
                # with no plan is WRITTEN (against NULL_PLAN_ID) instead of
                # being refused here. `reason="plan_id"` is therefore
                # STRUCTURALLY UNREACHABLE from this point on.
                #
                # ⛔ THE LABEL IS KEPT ANYWAY, AND DELETING IT WOULD BE THE
                # MISTAKE. It is pre-seeded in `METERING_USAGE_SKIP_REASONS`, so
                # it exports a flat 0 — and a flat 0 on a series that USED to
                # carry the majority of events is the evidence that this repair
                # holds. Dropping the child would make the before/after
                # unreadable (an absent series and a zero series look identical
                # to `increase()`), and it would silently re-home any future
                # regression into `_other`-shaped ambiguity.
                #
                # The round-8 diagnosis below is RETAINED because it is still
                # the correct account of WHY plan_id is unobtainable — that has
                # not changed and no plan is now being invented. What changed is
                # only that the counter no longer depends on obtaining it:
                #
                #   * `request_quota_config` — the ONLY local licence mirror, and
                #     the table both resolver maps are built from — has columns
                #     license_id, tenant_id, quota_monthly, app_id, app_name,
                #     status, created_at, updated_at. There is no plan_id and no
                #     plan_code. `resolve_app` exists BECAUSE app_id is mirrored;
                #     there is no `resolve_plan` because nothing is mirrored to
                #     resolve from.
                #   * There is no Plan/LicensePlan model anywhere in models/. The
                #     authoritative licence->plan edge lives in the licences
                #     service, and billing's only access to it is an outbound HTTP
                #     `fetch_licenses()` on the REPORT path (services.billing,
                #     routers.quota) — never on the flush path.
                #   * `MeteringMonthlyUsage.plan_id` is NOT NULL and is part of
                #     `uq_monthly_usage`. ⚠️ THIS LINE USED TO CONCLUDE "so there
                #     is no 'write it without a plan' option that does not change
                #     the table's grain", and that conclusion was WRONG — it is
                #     what kept the counter empty for 155 of 160 licences. NOT
                #     NULL forbids a NULL; it does not forbid a SENTINEL, and the
                #     two sibling rollups (MeteringHourly/MeteringDaily) plus this
                #     table's OWN `subject_id` had already been using exactly that
                #     device for the identical problem. The grain is not changed:
                #     a row carrying a real plan still gets its own row, so a
                #     mid-month plan change still splits, and NULL_PLAN_ID means
                #     "no plan asserted" rather than standing in for a real one.
                #     A sentinel is also strictly BETTER than making the column
                #     nullable here, because Postgres treats NULLs as distinct in
                #     a unique key and the `ON CONFLICT` would stop de-duplicating
                #     (see the note on the column in models/db/rollups.py).
                #
                # What round 8 could not close, and round 9 does NOT close either:
                # a licensed fr request carries an HTTP-code `status`, so it now
                # passes this guard and is refused one level in, by the status
                # vocabulary in the INNER `else` above — which writes no row at
                # all. Quota still cannot bite for fr's traffic, and the reason is
                # now the vocabulary, NOT the plan. Mirroring
                # `request_quota_config.plan_id` is consequently NOT the fix it
                # was believed to be.
                #
                # ⛔ AND NO SECOND COUNTER WAS ADDED. This skip is ALREADY counted,
                # by this very line — `billing_metering_usage_skipped_total`
                # {reason=license_id|plan_id|both}. A new
                # `billing_metering_usage_unwritten_total{reason}` would be a
                # second series over the identical population with a coarser
                # label, i.e. two names for one measurement that must then be kept
                # in agreement by hand. The gate is NOT weakened either: a row with
                # genuinely no licence still writes no usage counter.
                #
                # ⛔ ONLY `license_id` CAN BE ABSENT HERE NOW — the guard tests
                # nothing else — so the `plan_id` arm is gone rather than left
                # as dead code that reads like a live case. The `both`/
                # `license_id` split is KEPT and still means what it always
                # meant: whether the emitter asserted a plan it could not
                # attach to a licence, or asserted nothing at all.
                skip_reason = "both" if not plan_id else "license_id"
                observability_metrics.metering_usage_skipped_total.labels(reason=skip_reason).inc()

        if not rows:
            # Every event in the batch was a redelivery — nothing new to persist.
            # pending_acks still get XACK'd below so the duplicates leave the PEL.
            await session.commit()
        else:
            # Batch insert raw events
            await session.execute(cast("Table", MeteringEvent.__table__).insert(), rows)

            # Upsert monthly usage counters as a single batched statement.
            # monthly_counts keys are unique, so ON CONFLICT can't fire twice for
            # the same row within the batch.
            if monthly_counts:
                monthly_rows = [
                    {
                        "license_id": uuid.UUID(lic_id),
                        "subject_id": uuid.UUID(sub_id),
                        "plan_id": uuid.UUID(plan_id),
                        "month_start": month,
                        "request_count": allowed,
                        "denied_count": denied,
                        "budget_denied_count": budget_denied,
                    }
                    for (lic_id, sub_id, plan_id, month), (
                        allowed,
                        denied,
                        budget_denied,
                    ) in monthly_counts.items()
                ]
                monthly_stmt = pg_insert(MeteringMonthlyUsage).values(monthly_rows)
                monthly_stmt = monthly_stmt.on_conflict_do_update(
                    constraint="uq_monthly_usage",
                    set_={
                        "request_count": MeteringMonthlyUsage.request_count
                        + monthly_stmt.excluded.request_count,
                        "denied_count": MeteringMonthlyUsage.denied_count
                        + monthly_stmt.excluded.denied_count,
                        "budget_denied_count": MeteringMonthlyUsage.budget_denied_count
                        + monthly_stmt.excluded.budget_denied_count,
                    },
                )
                await session.execute(monthly_stmt)

            await session.commit()

    # ACK the Redis Streams entries now that the DB commit has succeeded. If the
    # XACK itself fails (or a crash lands between commit and XACK), the next
    # restart redelivers the message; the ProcessedEventMessage claim above — in
    # the same transaction as the raw-event insert + additive rollup — drops that
    # redelivery, so request_count/denied_count are NOT
    # double-incremented (the additive UPSERT alone does NOT deduplicate). All
    # pending_acks, including any that turned out to be duplicates, are still
    # XACK'd so they leave the pending-entries-list. The reverse (ack before
    # commit) is what we cannot afford: an OOM/SIGKILL between ack and commit
    # silently loses events.
    if consumer_group is not None and redis_conn is not None and pending_acks:
        by_stream: dict[Any, list[Any]] = {}
        for stream, msg_id in pending_acks:
            by_stream.setdefault(stream, []).append(msg_id)
        for stream, msg_ids in by_stream.items():
            try:
                await redis_conn.xack(stream, consumer_group, *msg_ids)
            except RedisError:
                logger.warning(
                    "xack_after_flush_failed",
                    stream=stream,
                    count=len(msg_ids),
                    exc_info=True,
                )

    logger.info("events_flushed", count=len(rows))
    return len(rows)
