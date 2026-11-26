"""Prometheus counters/gauges/histograms for billing-specific signals.

The HTTP-level metrics (request count, latency histogram, status codes) come
from prometheus-fastapi-instrumentator and live under ``http_*``. These custom
metrics expose the things the instrumentator can't see:

* event ingest rate (vs flush rate) — drift indicates buffer backlog
* pricing miss rate per provider/model — silently NULL cost_usd is a money leak
* budget debit volume + outcome — oversold paths are how we detect license
  configuration errors

All metric names follow Prometheus convention: snake_case, suffix indicates
type (``_total`` for Counter, ``_seconds`` for Histogram, no suffix for Gauge).
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

from health_probes import HEALTH_PROBE_EMITTERS
from services._cost_schema import COST_DEAD_LETTER_REASONS
from services._parsers import (
    EVENT_KEY_SOURCES,
    METERING_PLACEHOLDER_COUNTED_FIELDS,
    PLACEHOLDER_COUNTED_FIELDS,
)
from tenancy import TENANT_ID_CLASSIFICATIONS, TENANT_ID_DIVERGENCE_CLASSES

# Event ingest from the Redis stream consumer
events_in_total = Counter(
    "billing_events_in_total",
    "Raw API-request events read from the Redis stream and buffered.",
)
events_flushed_total = Counter(
    "billing_events_flushed_total",
    "Events written from the buffer to the metering_events table.",
    ["status"],  # ok | error
)

# Cost ingest from the Redis stream consumer
cost_events_in_total = Counter(
    "billing_cost_events_in_total",
    "Raw cost events read from the Redis stream and buffered.",
)
cost_events_flushed_total = Counter(
    "billing_cost_events_flushed_total",
    "Cost events written from the buffer to the cost_events table.",
    ["status"],  # ok | error
)
cost_events_priced_total = Counter(
    "billing_cost_events_priced_total",
    "Cost events that received a non-NULL cost_usd at flush time.",
)
cost_dead_letter_total = Counter(
    "billing_cost_dead_letter_total",
    "Malformed cost events routed to the dead-letter stream and NOT billed "
    "(failed the minimum billable schema — see services._cost_schema). A "
    "non-zero rate means a producer is emitting corrupt events; without this "
    "guard they were silently billed at $0.",
    ["reason"],  # missing_provider | invalid_<field> | negative_<field> | invalid_cost_usd | ...
)
# THIS is billing-service's reason-labelled dropped-event counter -- the
# equivalent of karma's `karma_metering_events_dropped_total{reason=...}`.
# Before adding a second one: `services._cost_schema.validate_cost_event` is
# the ONLY place a cost event is dropped rather than billed, and it already
# covers both a schema failure (missing_provider, invalid/negative numeric
# fields) AND tenant-normalisation failure (missing_tenant_id,
# invalid_tenant_id, nil_tenant_id -- folded in by #91, 2026-08-26, because an
# unattributable tenant is unbillable spend the same way a missing provider
# is). `services.cost_flusher.flush_costs` increments this Counter with the
# returned reason for every dead-lettered event (see the `reason=reason`
# call near the dead-letter XADD). There is no second drop path in
# cost_flusher.py: `_normalize_tenant_id` still runs after validation but its
# tenant-invalid signal is dead there on purpose -- see the comment at its
# call site -- because validate_cost_event has already guaranteed a real
# tenant uuid reaches that line. Verified 2026-09-04: every COST_DEAD_LETTER_REASONS
# member has its own test in tests/test_cost_flusher.py asserting this exact
# counter, by reason, including the tenant ones
# (test_missing_tenant_id_dead_lettered, test_bearer_token_tenant_id_is_dead_lettered_and_redacted,
# test_nil_uuid_tenant_id_dead_lettered).
# Birth every reason series at 0 at import time.
#
# A LABELLED prometheus_client Counter exports NO series at all until its first
# ``.labels(...).inc()``. So a pod that simply has not dead-lettered anything
# since boot is indistinguishable, on the wire, from one where the instrument
# was deleted — "no dead letters" and "no instrument" are the same empty
# vector. Measured 2026-08-28: an operator ran
# ``sum(increase(billing_cost_dead_letter_total{reason="nil_tenant_id"}[6h]))``,
# got an empty vector, and read it as a measured zero. It was not a measurement.
#
# This is the same series-birth hazard ``services/cost_flusher.py:876`` refuses
# to re-open at the other end of the metric's life: that comment declines to add
# a ``service_key`` LABEL because birthing new series zeroes the old ones and
# ``increase(cost_dead_letter_total[...])`` reads 0 across a birth. That keeps
# the series from being re-born mid-life; this keeps them from never being born.
#
# The reason set is imported, not restated, so it cannot drift from the
# validator that emits it (``services._cost_schema`` is package-internal by
# convention, but a second hand-copied list is the worse failure here).
for _reason in COST_DEAD_LETTER_REASONS:
    cost_dead_letter_total.labels(reason=_reason)

cost_dead_letter_depth = Gauge(
    "billing_cost_dead_letter_depth",
    "Current XLEN of billing:costs:dead — quarantined cost events awaiting "
    "replay/discard. SAMPLED, not live: written by the consumer's trim block "
    "once per cost_stream_trim_interval_seconds (default 300s) and again "
    "immediately after any admin replay/discard that actually XDELed. Between "
    "those it is stale by up to the trim interval, so a non-zero reading right "
    "after a drain is a lagging sample, NOT a failed drain — that exact reading "
    "(36, one minute after the stream emptied at 2026-08-28T16:13:59Z) was "
    "nearly filed as a broken metric. The post-drain resample exists to close "
    "that window; the periodic one still covers arrivals nobody triggered.",
)
cost_dead_letter_oldest_age_seconds = Gauge(
    "billing_cost_dead_letter_oldest_age_seconds",
    "Age in seconds of the OLDEST entry on billing:costs:dead, derived from "
    "that entry's _dlq_at stamp. NaN when the stream is empty, and NaN when the "
    "oldest entry's _dlq_at is absent or unparseable — never 0, which would read "
    "as 'nothing is expiring'. Sampled beside billing_cost_dead_letter_depth, on "
    "the same two cadences (the consumer's trim block, and after any admin "
    "replay/discard that XDELed).",
)
# WHY A SECOND DLQ METRIC EXISTS AT ALL — the depth gauge cannot see this loss.
#
# billing:costs:dead is age-trimmed at dead_letter_stream_max_age_hours (720h /
# 30d; settings.py -> consumer.py's trim block -> rollup_engine.trim_stream), so
# a quarantined cost event is DESTROYED on its 30th day. Redis is its only copy:
# these events were rejected at the schema seam and never written to Postgres.
# The only alert on the stream, BillingCostDeadLetterBacklog, is
# `billing_cost_dead_letter_depth > 500 for 30m`.
#
# Those two facts do not meet. The alert watches DEPTH; the loss mechanism is
# AGE. A stream resting below the threshold can expire them one at a time,
# forever, and never approach 500 — so nothing fires. This gauge is the missing
# axis. (Depth was 321 on 2026-09-08 and 14 on 2026-09-14; both illustrate the
# gap and neither is a standing value — re-measure before quoting either.)
#
# ⚠️ CORRECTED 2026-09-14 — this used to end: "Measured 2026-09-08 on the live
# stream: entries-added 1375, XLEN 321, max-deleted-entry-id set. ~1,054
# rejected cost events had ALREADY been trimmed away." ~1,054 events were indeed
# removed with the depth alert green throughout, but NOT by the trim: they were
# 1-2 days old against a 720h cutoff when they vanished. What DID remove them is
# UNMEASURED — discard, replay-with-delete, a key deletion and a flush all fit,
# and ``max-deleted-entry-id`` is set by ``XDEL`` as well as ``XTRIM``, so it can
# never say which. Full refutation + depth history in tests/test_dlq_retention.py
# (the ``billing_cost_dead_letter_oldest_age_seconds`` header comment).
#
# WHY NaN AND NOT 0 FOR "NO READING". An unlabelled prometheus_client Gauge
# always exports a sample, so genuine absence is not on the menu (and buying it
# with a label would re-open the series-birth hazard documented above, where an
# alert selects an empty vector and is structurally unfireable). NaN is the
# honest third value: the series exists, so an `> threshold` rule over it is
# never vacuous, and NaN fails every comparison, so an empty or unreadable
# stream can never fire an age alert. A 0 would be the confident-wrong reading —
# "the oldest entry was quarantined this instant" — which is precisely the shape
# of failure billing_cost_dead_letter_total already demonstrates elsewhere.
#
# EMPTY vs UNREADABLE are distinguished by PAIRING with the depth gauge, not by
# a second series:
#     depth == 0 and age is NaN  -> stream empty, nothing is expiring
#     depth >  0 and age is NaN  -> the oldest entry carries no usable _dlq_at;
#                                   go read it (dlq_oldest_age_unreadable is
#                                   logged at WARNING with the entry id)
#
# WHY NOT FALL BACK TO THE STREAM ENTRY ID. A Redis stream id is `<ms>-<seq>`
# and its ms IS the XADD wall clock, so it would always yield a number. That is
# the objection: it would yield a number even for an entry the flusher failed to
# stamp, silently papering over a producer regression in the one field the DLQ
# runbook reads (dlq_ops.inspect_dlq surfaces _dlq_at to the operator). _dlq_at
# is the authored quarantine time and the only field whose absence is a defect
# worth seeing; NaN + a WARNING makes that defect visible instead of plausible.

cost_dead_letter_expired_total = Counter(
    "billing_cost_dead_letter_expired_total",
    "Entries DESTROYED by services.rollup_engine.trim_stream's age trim "
    "(`XTRIM <stream> MINID ~ <cutoff>`), by stream. This is the counter the "
    "gap above names: billing_cost_dead_letter_depth watches DEPTH and the "
    "existing alert BillingCostDeadLetterBacklog fires on depth > 500 for "
    "30m, but the loss mechanism on billing:costs:dead is AGE "
    "(dead_letter_stream_max_age_hours, default 720h/30d) — a backlog "
    "resting below 500 can expire one entry at a time, forever, and never "
    "trip that alert. This counter is the missing axis: a nonzero rate here "
    "on the dead-letter stream is quarantined cost events being destroyed, "
    "full stop, regardless of depth. "
    "⚠️ COUNTS THE RETURNED VALUE, NEVER AN ESTIMATE: XTRIM's `~` "
    "(approximate) means Redis may retain a few entries slightly older than "
    "the cutoff for trim efficiency, so the number of entries actually gone "
    "is exactly what the XTRIM call itself reports removing — trim_stream "
    "already returns that value (`removed`) rather than computing one, and "
    "this counter is incremented with that same return value, at the same "
    "call, so the metric and the deletion can never disagree. "
    "trim_stream is stream-agnostic and is also called on billing:costs and "
    "billing:events (their own, shorter age bound — see "
    "COST_STREAM_MAX_AGE_HOURS / EVENTS_STREAM_MAX_AGE_HOURS) — those trims "
    "are routine backpressure relief on streams with a durable Postgres copy, "
    "not loss, but the label still separates them so a rate here can be "
    "read per-stream rather than conflating a DLQ-only signal with routine "
    "trimming of live streams.",
    ["stream"],
)
# The three (and, as of this writing, only) streams trim_stream() is ever
# called on -- services.consumer's trim block, guarded by
# settings.cost_stream_trim_interval_seconds. Spelled as literals matching
# their Settings field defaults (src/settings.py: costs_stream, events_stream,
# cost_dead_letter_stream) rather than importing/instantiating Settings here,
# for the same reason INVOICE_STATUSES is a frozen literal rather than a
# read of the live DB constraint: pre-seeding must not depend on constructing
# the app's full settings object at metrics-import time, and a stream this
# helper is ever called on again needs adding here too.
_TRIMMED_STREAM_NAMES = ("billing:costs", "billing:events", "billing:costs:dead")
# Birth every stream's series at 0 at import time -- the identical hazard
# documented on cost_dead_letter_total above: a labelled Counter exports no
# series until its first `.labels(...)`, so a fleet that has not yet trimmed a
# stream since boot is indistinguishable, on the wire, from one where this
# counter does not exist. The dead-letter stream in particular can go quiet
# for long stretches (nothing to trim below the age cutoff) and quiet must
# read as a measured 0, not as an absent instrument.
for _trimmed_stream in _TRIMMED_STREAM_NAMES:
    cost_dead_letter_expired_total.labels(stream=_trimmed_stream)

cost_events_repriced_total = Counter(
    "billing_cost_events_repriced_total",
    "Cost events whose NULL cost_usd was retroactively filled by the admin "
    "reprice backfill (services.reprice). Dry runs do NOT increment this.",
    ["provider"],
)
cost_events_deduplicated_total = Counter(
    "billing_cost_events_deduplicated_total",
    "Cost-stream messages skipped at flush time because their Redis stream "
    "message id was already processed (at-least-once redelivery dedup).",
)
usage_events_deduplicated_total = Counter(
    "billing_usage_events_deduplicated_total",
    "Usage/metering-stream messages skipped at flush time because their Redis "
    "stream message id was already processed (at-least-once redelivery dedup).",
)
pricing_miss_total = Counter(
    "billing_pricing_miss_total",
    "Cost events with no matching ProviderPricing row (cost_usd left NULL).",
    ["provider", "model"],
)
# Birth the catch-all miss series at 0 at import time.
#
# Same series-birth hazard as billing_cost_dead_letter_total above, but this one
# had a live consumer downstream. The PrometheusRule `MeteringPricingMiss`
# (group `metering.slo`, gitops-production) is
#     sum by (provider, model) (increase(billing_pricing_miss_total[1h])) > 0
# and the counter has exactly ONE increment site (services/pricing.py, the
# `row is None` branch of compute_cost_usd) with no zero-init. A labelled
# prometheus_client Counter exports the HELP/TYPE header but NO SAMPLES until
# its first `.labels(...)`, and Prometheus ingests samples, not headers — so a
# fleet that had simply not missed a price since boot created no series at all.
# Measured 2026-09-03:
#     /api/v1/series?match[]=billing_pricing_miss_total  ->  []
# with no metric name containing "pricing" anywhere in the retained window,
# against code that has defined this counter since 2026-07-16 (#33) and
# incremented it since 2026-06-21. It is not image lag; it is birth.
#
# The consequence is worse than a blank dashboard: the alert could never fire,
# so "no pricing misses" and "the alert is structurally dead" were the same
# empty vector — which is exactly what the alert-vacuity audit exists to catch.
#
# (provider, model) is an OPEN label domain — a miss is by definition a pair the
# price table does not carry — so there is no finite set to enumerate the way
# COST_DEAD_LETTER_REASONS is enumerated above. What is birthed instead is the
# one pair the increment site can always produce: its own `or "unknown"`
# fallbacks. That is a real bucket, not a sentinel, it births the metric NAME
# into Prometheus at 0, and it can never fire the alert on a resting pod.
# Per-(provider, model) misses still birth their own child on first occurrence;
# a child born mid-life does not disturb its siblings' `increase()`.
pricing_miss_total.labels(provider="unknown", model="unknown")

pricing_row_malformed_total = Counter(
    "billing_pricing_row_malformed_total",
    "Cost events that MATCHED a ProviderPricing row but were priced at $0.00 "
    "because the row carries no usable rates (e.g. cost_model='per_token' with "
    "NULL input/output rates). Distinct from billing_pricing_miss_total: a miss "
    "leaves cost_usd NULL and is detectable; this looks like a successful $0 "
    "charge. self_hosted_amortized rows are excluded — their $0 is by design.",
    ["provider", "model", "cost_model"],
)
markup_miss_total = Counter(
    "billing_markup_miss_total",
    "Markup resolutions that matched no rule — not even the global default "
    "(indicates the default markup row was not seeded).",
    ["provider"],
)
markup_zero_margin_total = Counter(
    "billing_markup_zero_margin_total",
    "Billable lines (non-internal, non-pass_through) that fell back to a 0% "
    "markup and were charged AT COST — zero margin. Should sit at flat zero; "
    "any increase means a paying tenant has no effective markup.",
    ["billing_mode", "service_key"],
)

# BIRTH THE SERIES. Same reasoning as `pricing_miss_total` below, and here it is
# load-bearing in the strongest form this file has: the HEALTHY reading of this
# counter is FLAT ZERO, so without this block the metric has no samples at all
# until the first zero-margin line — i.e. it comes into existence at exactly the
# moment an operator needs it and never before. `BillingMarkupZeroMargin` alerts
# on `increase(...[1h]) > 0`, and over a non-existent series that is an EMPTY
# VECTOR, which is vacuously green. So "no paying tenant is billed at cost" and
# "this alert is structurally dead" were the SAME reading — measured live
# 2026-09-13: zero series in Prometheus, against an increment site that has
# existed since 2026-07-24 (services/markup.py, #56). It is not image lag; it is
# birth, exactly as recorded for pricing_miss_total.
#
# ⛔ ONLY THE TWO MODES THAT CAN REACH THE INCREMENT ARE BIRTHED, and the two
# omissions are deliberate rather than an oversight. `compute_billed_line`
# RETURNS EARLY for `internal` (services/markup.py:498) and for `pass_through`
# (services/markup.py:509), both far above the counter, so a series birthed for
# either would be pinned at 0 forever — which reads as a working instrument
# watching a healthy population — "a value birthed that no code path can ever
# produce".
# The HELP text above already says it in words: "non-internal, non-pass_through".
#
# The `service_key` half is an OPEN label domain (it comes off the wire), so what
# is birthed is the increment site's own `service_key or "none"` fallback — a
# real bucket, not a sentinel. A per-key child born mid-life does not disturb its
# siblings' `increase()`.
#
# ⚠️ A NAMED, EXPORTED CONSTANT rather than an inline tuple, for the reason
# recorded at length on LICENCE_BUDGET_RECONCILE_FAILURE_REASONS below: a test
# that proves birth by scanning the registry is VACUOUS the moment any earlier
# test in the process increments the same child, so the non-vacuous assertion has
# to be over the BIRTH LIST itself. The literals are NOT imported from
# services.markup, which owns them, because metrics is imported BY markup
# (services/markup.py:47) and the reverse edge would be a cycle — the same
# constraint, and the same remedy, as the unmapped service-key birth list far
# below: a test pins the two definitions in agreement instead.
MARKUP_ZERO_MARGIN_BIRTH_MODES: tuple[str, ...] = (
    "standard_markup",
    "byo_platform_fee",
    "byo_markup_only",
)
#: The increment site's own fallback for an absent service_key
#: (``service_key or "none"``). Bound to a NAME so the literal never sits on a
#: ``service_key=`` keyword argument — detect-secrets' "Secret Keyword" rule
#: fires on that shape, which is why the unmapped birth list below is written as
#: a loop rather than as two direct calls.
MARKUP_ZERO_MARGIN_BIRTH_SERVICE: str = "none"
for _zero_margin_mode in MARKUP_ZERO_MARGIN_BIRTH_MODES:
    markup_zero_margin_total.labels(
        billing_mode=_zero_margin_mode, service_key=MARKUP_ZERO_MARGIN_BIRTH_SERVICE
    )

app_rate_missing_total = Counter(
    "billing_app_rate_missing_total",
    "Per-request app charges billed at $0 because the service_key has NO row in "
    "billing_app_cost_rates at all. ⚠️ NOT the same as a rate row whose value is "
    "0: a deliberate free service does NOT increment this. services/billing.py "
    "resolves the rate with app_rates.get(key, 0), so an unmapped key invoices "
    "at $0 silently — and services/markup.py gates its zero-margin alarm on "
    "incurred_usd > 0, so that fabricated $0 also SUPPRESSES the alarm that "
    "would have named it. The two compose into a charge of nothing that reports "
    "nothing. Measured 2026-09-08: billing_app_cost_rates has 0 rows in prod, so "
    "every service_key with usage takes this path. `surface` names WHERE the "
    "miss was observed: `invoice` = services.billing.generate_billing, the "
    "customer-facing line; `overview` = the routers.billing admin overview. The "
    "two are separate code paths over the same gap, and collapsing them would "
    "hide a miss that only one of them reaches.",
    ["service_key", "surface"],
)
# BIRTH THE SERIES. Same reasoning as pricing_miss_total above, and the same
# trap: service_key is an OPEN label domain (an unpriced key is by definition
# one the rate table does not carry), so there is no finite set to enumerate.
# Without this the metric NAME does not exist in Prometheus until the first
# increment, and "no unpriced service keys" and "this alert is structurally
# dead" are the SAME empty vector. The bucket birthed is the increment site's
# own `or "unknown"` fallback — a real bucket, not a sentinel — so it can never
# fire the alert on a resting pod, and a per-key child born later does not
# disturb its siblings' increase().
app_rate_missing_total.labels(service_key="unknown", surface="invoice")
app_rate_missing_total.labels(service_key="unknown", surface="overview")

cost_event_ts_fallback_total = Counter(
    "billing_cost_event_ts_fallback_total",
    "Cost events whose bucket_date was stamped from INGEST time (created_at) "
    "because no usable event_ts was available — the wire carried no `timestamp`, "
    "or carried one that was refused as unparseable, naive, clock-skewed or "
    "back-dated past retention. "
    "⚠️ THIS IS NOT AN ERROR RATE, IT IS A COVERAGE GAP. Every emitter is "
    "believed to send `timestamp` (inference-router's own metering, w2v-common "
    "and kag-common all set it at emit), but two service_keys — "
    "w2v-video-analyzer and w2v-video-compiler, the second of which is the "
    "estate's #2 spender — could not be verified against their source on "
    '2026-09-11. Until this counter rests at zero FOR THOSE KEYS, "all emitters '
    'send it" is an assumption and not a measurement: a nonzero bucket names '
    "exactly which emitter is still being billed on the clock that can move its "
    "revenue across a month boundary.",
    ["service_key"],
)
# BIRTH THE SERIES — same trap as app_rate_missing_total above, and here it is
# the whole point of the counter. service_key is an OPEN label domain, so without
# this the metric NAME does not exist until the first fallback, and "every
# emitter sends a usable event_ts" is the SAME empty vector as "this counter was
# never wired up". Since the question this answers is precisely "is the fallback
# being taken", a counter that cannot report zero would answer it by staying
# silent either way. The bucket birthed is the increment site's own
# `c.service_key or "unknown"` fallback (rollup_engine.py:648) — a real bucket,
# not a sentinel.
cost_event_ts_fallback_total.labels(service_key="unknown")

cost_event_ts_backdated_past_rollup_total = Counter(
    "billing_cost_event_ts_backdated_past_rollup_total",
    "Cost-stream entries ACCEPTED into the flush buffer carrying a usable, "
    "producer-asserted `event_ts` that is older than the daily rollup's trailing "
    "reconcile window (settings.daily_reconcile_days, default 3 complete days). "
    "THE GAP: _parsers.parse_event_ts accepts an event time up to "
    "MAX_EVENT_TS_BACKDATE_DAYS (90) before ingest, and rollup_daily buckets the "
    "row on THAT date — but services.rollup_engine.catchup_daily only ever "
    "re-rolls the last `daily_reconcile_days` complete days plus whatever "
    "missing_daily_dates reports ABSENT. A row that lands in a day which has "
    "already been rolled and already HAS a billing_cost_daily row is therefore "
    "never re-aggregated: it is stored, it is real spend, and it reaches no "
    "invoice. daily_rollup_date_gaps explicitly declines to repair, and repair is "
    "genuinely the wrong move there (re-rolling a day whose sources are gone "
    "DELETES the surviving bucket), so nothing downstream closes this. "
    "⚠️ IT SAYS 'LOOK', NOT 'MONEY WAS LOST' — the same posture as "
    "billing_rollup_date_gaps. If the backdated day had NO cost_daily row, "
    "missing_daily_dates still picks it up and the row is billed late rather than "
    "never; this counter cannot see which of the two happened, because the "
    "bucket's existence is not knowable at ingest. A non-zero value names an "
    "emitter to go ask about. "
    "⚠️ INGRESS COUNTER, over entries STAGED by services.consumer._stage_message — "
    "BEFORE schema validation, BEFORE the idempotency claim, and before "
    "cost_flusher re-parses the same field with its own `received_at`. So a "
    "dead-lettered entry is counted here, and a Redis redelivery or an XAUTOCLAIM "
    "reclaim counts the same entry again. Read it as a trend, not a ledger — the "
    "identical caveat billing_license_caller_unasserted_total carries, and for the "
    "identical reason. It uses the SAME parse_event_ts helper the flusher uses, so "
    "the two can never disagree about whether a value was usable. "
    "Labelled by service_key (the wire field, as its sibling "
    "billing_cost_event_ts_fallback_total is) so the backdating emitter is "
    "nameable without reading logs. NOT the same population as that sibling and "
    "NOT comparable to it as a ratio: it counts event times that were REFUSED and "
    "fell back to ingest time (late, but billed), this one counts event times that "
    "were ACCEPTED and route the row somewhere nothing will aggregate.",
    ["service_key"],
)
# BIRTH THE SERIES — same trap as its sibling above: service_key is an OPEN label
# domain, so without this the metric NAME does not exist until the first backdated
# event, and "no emitter backdates past the rollup window" is the SAME empty vector
# as "this counter was never wired up". The bucket birthed is the increment site's
# own `or "unknown"` fallback, a real bucket rather than a sentinel.
cost_event_ts_backdated_past_rollup_total.labels(service_key="unknown")
tenant_caller_unasserted_total = Counter(
    "billing_tenant_caller_unasserted_total",
    "INGRESS counter. Events whose emitter put a non-UUID value in tenant_id, "
    "counted at the moment the wire is read and BEFORE normalisation decides "
    "what the row is actually stored under. It measures 'the caller did not "
    "assert a usable tenant', which is a statement about the EMITTER, not "
    "about the ledger. "
    "⛔ IT IS NOT A COUNT OF UNATTRIBUTED ROWS, and reading it as one has "
    "caused false alarm in at least three sessions. Measured over a 10h window "
    "on 2026-09-05, strictly after that day's truncate: this counter rose "
    "+1,100 while billing_cost_events over the same window held 27,941 rows "
    "with ZERO NULL tenant_id. Every one of those 1,100 was normalised to a "
    "real tenant downstream. The number an operator actually wants — rows "
    "PERSISTED with no tenant — is billing_tenant_unattributed_persisted_total "
    "below, and on the cost stream it is structurally 0 because "
    "validate_cost_event dead-letters an unresolvable tenant before it can "
    "become a row at all.",
    ["stream"],  # cost | usage
)
for _stream in ("cost", "usage"):
    tenant_caller_unasserted_total.labels(stream=_stream)

#: ⚠️ DEPRECATED ALIAS of tenant_caller_unasserted_total, kept EMITTED and
#: incremented at the identical site so nothing that already queries this name
#: breaks. It is retained rather than renamed away because a metric name is a
#: published interface: a hard rename silently empties every dashboard panel
#: and makes every alert rule on it go vacuously green, which is a worse
#: failure than a badly-named series. Prefer the new name in anything written
#: from now on; this one may be removed once no consumer references it.
tenant_unattributed_total = Counter(
    "billing_tenant_unattributed_total",
    "DEPRECATED — MISNAMED, use billing_tenant_caller_unasserted_total, which "
    "this is an exact alias of (same increment sites, same {stream} label). "
    "The name says 'unattributed' but the measurement is 'the emitter sent a "
    "non-UUID tenant_id at ingress'; normalisation then attributes almost all "
    "of them. Measured 10h on 2026-09-05: +1,100 here against 0 rows stored "
    "with a NULL tenant_id. For the persisted gap, read "
    "billing_tenant_unattributed_persisted_total.",
    ["stream"],  # cost | usage
)

tenant_unattributed_persisted_total = Counter(
    "billing_tenant_unattributed_persisted_total",
    "POST-RESOLUTION counter, and the one an operator should page on. Rows "
    "that SURVIVED the idempotency claim and were written to billing_cost_events "
    "still carrying the nil/unattributed tenant sentinel — i.e. spend that is "
    "in the ledger and belongs to nobody. Counted over the same `survived` set "
    "and at the same point as billing_app_unattributed_total, so it shares that "
    "counter's denominator and its one honest caveat: it is incremented in "
    "memory BEFORE the batch transaction commits, so a rollback un-claims the "
    "messages and the replayed batch counts the same rows again. It over-counts "
    "on a failed flush and is a trend, not a ledger. "
    "Expected to sit at FLAT ZERO on the cost stream — validate_cost_event "
    "dead-letters an unresolvable tenant before it becomes a row — and that is "
    "the point: it is the control that proves the ingress counter's rise is not "
    "a ledger gap.",
    ["service_key"],
)
metering_status_unrecognised_total = Counter(
    "billing_metering_status_unrecognised_total",
    "Metering events whose `status` matched NEITHER the decision vocabulary "
    "(`allowed` / `denied*`) nor anything the monthly counters understand. Such "
    "a row increments no counter at all, so if it ever carries a license it is "
    "usage that silently does not get counted. Labelled by the raw value.",
    ["status"],
)

#: What ``services.event_flusher`` needed and did not get. The guard is now
#: ``if license_id:``, so the live values are ``license_id`` (a plan was
#: asserted, the licence was not) and ``both`` (neither was) — mutually
#: exclusive, and between them they exhaust the guard's false branch.
#:
#: ⛔ ``plan_id`` IS RETAINED THOUGH THE WRITER CAN NO LONGER EMIT IT, and that
#: is deliberate, not leftover. It is the BIRTH LIST, so removing the child
#: would stop the series being exported at all — and an absent series and a
#: zero series are the same empty vector to ``increase()``. This series falling
#: to a flat zero is the measurement that the plan conjunct is gone; delete the
#: child and that evidence becomes unreadable. A non-zero ``plan_id`` after
#: this means the guard regressed.
#: Bounded and closed by construction — the writer derives the value from its
#: own booleans, so no caller-supplied string can ever reach this label.
METERING_USAGE_SKIP_REASONS: tuple[str, ...] = ("license_id", "plan_id", "both")

metering_usage_skipped_total = Counter(
    "billing_metering_usage_skipped_total",
    "Metering events that survived the idempotency claim, were INSERTED into "
    "billing_metering_events, and then contributed NOTHING to "
    "billing_metering_monthly_usage (so nothing to request-quota usage) — because "
    "the OUTER guard "
    "`if license_id:` in services.event_flusher was false. "
    "⛔ THIS IS THE OUTER GUARD, NOT THE INNER ONE. "
    "billing_metering_status_unrecognised_total above covers the case where BOTH "
    "ids are present and the `status` vocabulary is not understood; that branch "
    "was already counted. This branch — either id absent — was counted by nothing "
    "at all, and it is by far the larger population. "
    "⚠️ THE GUARD NO LONGER TESTS plan_id, AND THIS SERIES SHOULD HAVE FALLEN "
    "SHARPLY AS A RESULT. It used to read `if license_id and plan_id:`, and "
    "because billing mirrors no plan at all (no plan_id on request_quota_config, "
    "no Plan model, no resolve_plan) the second conjunct was false for essentially "
    "all live traffic: measured live, billing_metering_monthly_usage held 6 rows / "
    "5 distinct licences against 160 rows in request_quota_config — 155 of 160 "
    "(96.9%) configured licences had NO usage row, so every quota check fell "
    "through fail-open. A licensed event with no plan is now written against the "
    "NULL_PLAN_ID sentinel instead of being skipped here. "
    "⛔ A DROP IN THIS COUNTER IS NOT THE SAME AS QUOTA WORKING. Those events now "
    "clear THIS guard and are refused one level in, by the status vocabulary in "
    "the inner `else` (fr writes HTTP codes; only `allowed`/`denied…` assign a "
    "monthly count), which writes no row either and is counted by "
    "billing_metering_status_unrecognised_total. Read the two together: usage that "
    "left this series should APPEAR there, not vanish. "
    "⛔ DO NOT ALERT ON `> 0`: it would fire from the first scrape forever, get "
    "silenced, and a silenced alert is a deleted detector. Read it as a RATIO "
    'against billing_events_flushed_total{status="ok"}, which is incremented with '
    "the SAME survivor count this counter is incremented over, so the two can "
    "never describe different row sets. A RISE in that ratio means more usage is "
    "disappearing from quota than yesterday. "
    "`reason` names THE MISSING ID: `license_id` (a plan was asserted but no "
    "licence) or `both` (the emitter asserted neither). "
    "⛔ `plan_id` IS NOW STRUCTURALLY UNREACHABLE and is DELIBERATELY RETAINED AS A "
    "FLAT ZERO. The guard tests only license_id, so a missing plan can no longer "
    "reach this branch. The child is kept pre-seeded because an absent series and "
    "a zero series are indistinguishable to `increase()`, and this series reading "
    "zero — where it once carried the majority of events — IS the evidence the "
    "repair holds. Any non-zero value on it means the plan conjunct came back. "
    "⚠️ NO PAIRED LOG LINE, DELIBERATELY — the one place in this module where that "
    "is the right call. At this volume a per-event WARNING would be the dominant "
    "line in the log and would bury the sibling warnings that are genuinely rare. "
    "A counter is the correct instrument for a high-volume NORMAL condition; a log "
    "line is for the exception.",
    ["reason"],
)
# Born at zero for every reason, same rule as COST_DEAD_LETTER_REASONS above: a
# labelled Counter exports HELP/TYPE but NO SAMPLES until its first `.labels()`,
# so on a replica that has not yet skipped one, "quota is fully counted" and
# "this instrument was never wired up" would be the same empty vector. Two of the
# three are reachable from the writer's own booleans; `plan_id` is birthed here
# precisely BECAUSE it is not — see the ⛔ note on METERING_USAGE_SKIP_REASONS.
for _usage_skip_reason in METERING_USAGE_SKIP_REASONS:
    metering_usage_skipped_total.labels(reason=_usage_skip_reason)

tenant_unregistered_total = Counter(
    "billing_tenant_unregistered_total",
    "Requests whose tenant_id was a well-formed, non-nil uuid that NO entry in "
    "the licences registry vouches for — the LIVE overlay first, the "
    "tenancy.LEGACY_TENANT_NAMES snapshot as fallback. canonical_tenant_id parses a uuid "
    "with the stdlib and returns it unchecked, so such a caller is metered as a "
    "tenant that the licences registry has never heard of. This counter FAILS "
    "OPEN — nothing is rejected — because closing the gate first would silently "
    "drop live spend; two such ids (from worker_management's "
    "principle_consistency) were already spending ~$30/30d when this was added. "
    "Deliberately UNLABELLED: the input is the caller-controlled X-Tenant-Id "
    "header, so a per-id label is unbounded cardinality fed straight from the "
    "edge. WHICH ids is answered by the paired WARNING log in tenancy.py, which "
    "names each one once per process. Its own series, distinct from "
    "billing_tenant_unattributed_total (non-UUID input, bucketed to nil) and "
    "from the license_tenant_unregistered_* gauges (billing's own registry), so "
    "the denominator can be read on its own before any gate is closed.",
)
license_caller_unasserted_total = Counter(
    "billing_license_caller_unasserted_total",
    "INGRESS counter. Cost events that ARRIVED carrying no license_id, counted "
    "in the pre_rows loop the moment the wire is parsed — BEFORE the idempotency "
    "claim and BEFORE services.license_resolver gets a chance to attribute them. "
    "It measures 'the caller did not assert a licence', which is a statement "
    "about the EMITTER's transport, not about the ledger. "
    "⛔ IT IS NOT A COUNT OF UNATTRIBUTED SPEND. Reading it as one has caused "
    "false alarm in at least three sessions, including the one that renamed it. "
    "MEASURED over a 10h window on 2026-09-05, strictly after that day's "
    "truncate, against billing_cost_events for the same window (27,941 rows, "
    "113 with NULL license_id): this counter rose +2,089 while only 113 rows "
    "were genuinely unlicensed. Broken down by cost_source, inference-router's "
    "share of that window was 1,989 rows `unknown+resolved` (arrived bare, "
    "RESOLVED successfully) + 3,150 `unknown` that already carried a licence + "
    "113 genuinely unlicensed. The resolver fills ~94% of what this counter "
    "counts. A rise here is still real signal — it means the x-license-id "
    "header is not reaching an emitter — but it is a HEADER-PROPAGATION signal, "
    "not an invoicing gap. The invoicing gap is "
    "billing_license_unattributed_persisted_total below.",
    ["service_key"],
)

#: ⚠️ DEPRECATED ALIAS of license_caller_unasserted_total, kept EMITTED and
#: incremented at the identical site. NOT hard-renamed on purpose: this name is
#: load-bearing for two live PrometheusRules in gitops-production
#: (BillingAttributionRateRegressed's triage query and
#: BillingUnattributedVolumeSustained's expr, clusters/strato/services/
#: prometheus/rules.yaml) plus three rule-test fixtures under ci/
#: prometheus-rule-tests/. A hard rename would not error anywhere — it would
#: empty those series, and an alert over an empty vector is vacuously green,
#: which is the exact failure mode the alert-vacuity audit exists to catch. The
#: alias makes the rename a no-op for every existing consumer; re-pointing
#: those rules at the honest names is a gitops change and is deliberately NOT
#: smuggled into a billing_service PR.
#: A cost or metering event whose wire ``actor_id`` was NON-EMPTY but did not
#: parse as a UUID. ``cost_flusher`` and ``event_flusher`` store such an actor
#: as NULL — exactly like an absent one — so without this counter a broken emitter and an emitter that sends no actor
#: produce identical rows. An empty/absent actor_id is NOT malformed and is not
#: counted here. Counted per flushed event, before dedup, like the
#: ``{service_key}`` counters below.
actor_malformed_total = Counter(
    "billing_actor_malformed_total",
    "Cost and metering events whose non-empty actor_id failed to parse as a UUID "
    "and was stored as NULL.",
    ["service_key"],
)

funding_unknown_total = Counter(
    "billing_funding_unknown_total",
    "Cost events stored with funding NULL: the emitter sent a funding value "
    "outside {local_gpu, platform_external, tenant_byo}, or sent none and it "
    "could not be derived (services.funding).",
    ["service_key"],
)

license_unattributed_total = Counter(
    "billing_license_unattributed_total",
    "DEPRECATED — MISNAMED, use billing_license_caller_unasserted_total, which "
    "this is an exact alias of (same increment site, same {service_key} label). "
    "The name says 'unattributed' but the measurement is taken at INGRESS, "
    "before license_resolver runs, so it counts events the caller did not "
    "assert a licence for — ~94% of which are then successfully attributed. "
    "Measured 10h on 2026-09-05: +2,089 here against 113 rows actually stored "
    "with a NULL license_id. For the invoicing gap, read "
    "billing_license_unattributed_persisted_total. Its DENOMINATOR remains "
    "billing_cost_events_by_service_total, incremented in the same iteration.",
    ["service_key"],
)

license_unattributed_persisted_total = Counter(
    "billing_license_unattributed_persisted_total",
    "POST-RESOLUTION counter, and the number an operator actually wants. Cost "
    "events that SURVIVED the idempotency claim, went through licence "
    "resolution, and were still written to billing_cost_events with "
    "license_id IS NULL. THIS is the invoicing gap: such spend is billed and "
    "capped (it drains the tenant-level budget pool) but can never be attributed "
    "to a customer invoice line, because CostDaily.license_attributed_clause "
    "drops it from every licence-keyed aggregate. "
    "Counted over `survived`, at the same point and on the same denominator as "
    "billing_app_unattributed_total, and it inherits that counter's caveat "
    "verbatim: incremented in memory BEFORE the batch transaction commits, so a "
    "rollback un-claims the messages and the replayed batch counts the same rows "
    "twice. It over-counts on a failed flush and is a trend, not a ledger. "
    "⚠️ NOT comparable to billing_license_caller_unasserted_total as a ratio — "
    "the two count over DIFFERENT denominators for the same reason "
    "billing_app_unattributed_total records: the ingress counter runs over "
    "pre_rows BEFORE the claim, so a stream redelivery inflates it, while this "
    "one runs after the claim and the event_key dedup. Read this one against "
    "billing_license_resolved_total + itself.",
    ["service_key"],
)

#: Birth list for ``billing_license_unattributed_persisted_total``.
#:
#: ⚠️ THIS COUNTER IS ALIVE AND INCREMENTING — 2 series measured live
#: 2026-09-14 — and this block adds NO increment. An earlier reading of "zero
#: series" was taken at 2026-09-13T19:45Z, inside a window in which the path had
#: simply not executed yet; it was a statement about the window, not about the
#: instrument, and it is recorded here so nobody re-derives the wrong fix from it.
#:
#: What birth buys is therefore narrower than that reading suggested, and is
#: worth stating exactly. A labelled Counter exports the HELP/TYPE header but NO
#: SAMPLES until its first ``.labels(...)``, and that is a PER-POD, PER-BOOT
#: property: a freshly rolled pod exports nothing for this name until its first
#: flush stores a licence-less row. Over that gap the ENTIRE
#: ``billing.attribution-volume`` group — ``BillingUnattributedVolumeSustained``
#: divides ``increase(...[6h])`` by ``billing_cost_events_by_service_total`` — has
#: an EMPTY VECTOR for a numerator and is vacuously green, so "no spend is
#: stranded off an invoice line" and "this pod has not booted far enough to say"
#: read identically. Birth closes that gap and nothing wider. It is the same
#: argument, on the same shape, that ``cost_tokens_by_service_total`` below
#: already records for its own ``unknown``/``unknown`` pair.
#:
#: ⛔ ONLY THE ONE VALUE THE INCREMENT SITE CAN PRODUCE WITHOUT LIVE TRAFFIC IS
#: BIRTHED. ``service_key`` is an OPEN domain — it comes off the wire — so there
#: is no finite roster to enumerate the way COST_DEAD_LETTER_REASONS is
#: enumerated. What is birthed is the flusher's own fallback: ``cost_flusher.py``
#: normalises every staged row to ``c.get("service_key") or "unknown"`` before it
#: is ever counted, and the counter re-applies the same ``or "unknown"`` at its
#: own call site. So this is a REAL bucket that real events land in (an emitter
#: that sends no service_key), not a sentinel invented to make the series exist.
#: A per-key child born mid-life does not disturb its siblings' ``increase()``.
#:
#: ⚠️ A NAMED, EXPORTED CONSTANT rather than an inline literal, for the reason
#: recorded on MARKUP_ZERO_MARGIN_BIRTH_MODES: a test that proves birth by
#: scanning the live registry is VACUOUS the moment any earlier test in the
#: process increments the same child, so the non-vacuous assertion has to be over
#: the BIRTH LIST itself. Written as a loop over a tuple rather than a direct
#: call so the literal never sits on a ``service_key=`` keyword argument, which
#: is the shape detect-secrets' "Secret Keyword" rule fires on.
LICENSE_UNATTRIBUTED_PERSISTED_BIRTH_SERVICES: tuple[str, ...] = ("unknown",)
for _persisted_birth in LICENSE_UNATTRIBUTED_PERSISTED_BIRTH_SERVICES:
    license_unattributed_persisted_total.labels(service_key=_persisted_birth)

license_ambiguous_total = Counter(
    "billing_license_ambiguous_total",
    "Cost events left unattributed because the licence mirror holds MORE THAN "
    "ONE active licence for the event's (tenant_id, app_name) — so "
    "services.license_resolver refused to guess between them and returned no "
    "licence.\n\n"
    "WHY THIS IS ITS OWN COUNTER. billing_license_unattributed_persisted_total "
    "already counts the rows that end up with license_id IS NULL, but it cannot "
    "say WHY, and the three causes need three different actions: no licence "
    "exists for the tenant (sell one / fix the sync), the service_key is not in "
    "cost_attribution_app_by_service_key (fix the operator setting), or the "
    "registry holds duplicates (deduplicate the licences). Only the third is a "
    "DATA DEFECT UPSTREAM, and it was previously indistinguishable from the "
    "other two at the event level — the sole signal was an unlabelled gauge "
    "(billing_attribution_ambiguous_pairs) and an info log emitted at most once "
    "per 300s map build, neither of which names the affected events.\n\n"
    "Measured on the prod billing primary 2026-09-16: ONE ambiguous pair "
    "(tenant e4f70a83-81ee-429a-abca-6999e3a0d24f, app 'RaiseMyTickets', 2 "
    "active licences) accounting for 49 rows carrying NULL license_id.\n\n"
    "⚠️ IT PAIRS WITH THE GAUGE, IT DOES NOT REPLACE IT. The gauge counts "
    "PAIRS in the registry and can fall to 0 when duplicates are cleaned up; "
    "this counts EVENTS actually lost to them and only ever rises. A duplicate "
    "that no traffic hits moves the gauge and never this; a duplicate on a hot "
    "service_key moves both. The gauge is also the one that freezes silently "
    "when the resolver stops running at all (see _build_map), so an alert "
    "wanting liveness reads this counter.",
    ["service_key"],
)

#: Birth list for ``billing_license_ambiguous_total``.
#:
#: ⛔ DELIBERATELY EMPTY, and that is the opposite choice from the sibling
#: counter above — so the reason has to be written down or somebody will
#: "fix" the inconsistency. Birth exists to separate "this population went
#: quiet" from "this population never existed", and it is worth paying for when
#: the healthy state is a NON-ZERO rate. Here the healthy state is ZERO: no
#: ambiguous pairs in the registry means no events lost to them. Birthing a
#: child per mapped service_key would pin a fleet of series at 0 that are
#: SUPPOSED to sit at 0 forever, and an alert of the form `> 0` works perfectly
#: over an absent vector while `rate(...) == 0` over a pinned one reads as a
#: healthy instrument watching nothing. Absence IS the signal for this one.
LICENSE_AMBIGUOUS_BIRTH_SERVICES: tuple[str, ...] = ()

cost_tokens_by_service_total = Counter(
    "billing_cost_tokens_by_service_total",
    "TOKENS on every schema-valid cost event taken off the buffer — input + "
    "output + cache_creation + cache_read — labelled by the same service_key as "
    "billing_cost_events_by_service_total and additionally by provider.\n\n"
    "WHY THIS EXISTS: this estate could not answer 'is all LLM usage measured?' "
    "because nothing exported token VOLUME. Event counts cannot answer it — a "
    "300-token classification and a 90,000-token document pass are one increment "
    "each — and the only token figures available were inside billing's Postgres, "
    "which no alert rule can read. The backends DO count (vllm:prompt_tokens_total "
    "+ vllm:generation_tokens_total measured 26.9M/24h on 2026-09-10), so this is "
    "the missing HALF of a ratio: backend tokens vs tokens that reached billing. "
    "An audit spent a day unable to verify a claimed '62% of tokens bypass the "
    "router' for exactly this reason, and relabelled it unmeasurable.\n\n"
    "⚠️ IT COUNTS TOKENS THAT ARRIVED, NOT TOKENS PERSISTED. It fires while the "
    "wire is being parsed, in the same iteration and off the same service_key "
    "expression as billing_cost_events_by_service_total — deliberately, so the "
    "two cannot drift and a rate over them is safe. The cost is that a rolled-back "
    "batch over-counts and a stream REPLAY re-counts (the DB dedupes on event_key; "
    "this counter cannot). For the coverage question that is the right numerator — "
    "'did the metering pipeline see these tokens?' — but it is NOT a billable "
    "total and must never be used as one. The billable figure is in "
    "billing_cost_events.\n\n"
    "⚠️ THE GAP IT MEASURES IS A UNION, NOT A CAUSE. Backend-minus-booked conflates "
    "at least four populations: traffic that bypassed a metering emitter entirely, "
    "failed/fallback/canary attempts that generated real tokens and emit nothing, "
    "streaming responses without include_usage (which book 0 tokens), and rows "
    "dead-lettered before this point. Use it to SIZE the problem and to watch it "
    "shrink; naming which of the four is responsible needs a different instrument.\n\n"
    "⚠️ PROVIDER IS LOAD-BEARING FOR THE COMPARISON and is why this carries a label "
    "its sibling does not: external-provider tokens (anthropic, openai) never touch "
    "a local vLLM backend, so summing across providers and comparing to "
    "vllm:*_tokens_total is apples to oranges. Filter to the local providers first. "
    "Both labels originate in the emitter's payload, as its sibling's already does, "
    "and the emitter roster is bounded — see billing_pricing_miss_total for the "
    "same provider-label precedent.",
    ["service_key", "provider"],
)

# (service_key, provider) is an OPEN label domain -- both come off the wire -- so
# there is no finite set to enumerate the way BUDGET_WRITE_SOURCES is enumerated.
# Birthed instead is the one pair the increment site can always produce: its own
# `or "unknown"` / default-"unknown" fallbacks, exactly as pricing_miss_total does
# below for the same reason. A real bucket, not a sentinel; it births the metric
# NAME at 0 so a fresh pod's first scrape does not look like an absent series, and
# a per-pair child born mid-life does not disturb its siblings' `increase()`.
#
# ⛔ This matters MORE here than for most counters: the whole purpose of this series
# is to be the denominator-side half of a coverage ratio, and a ratio whose
# numerator series does not exist yet silently evaluates to nothing rather than to
# a visible zero.
cost_tokens_by_service_total.labels(
    service_key="unknown", provider="unknown"
)  # pragma: allowlist secret


cost_events_by_service_total = Counter(
    "billing_cost_events_by_service_total",
    "Every schema-valid cost event taken off the buffer for flushing, labelled "
    "by the SAME service_key the two counters below label. THE DENOMINATOR: "
    "billing_license_unattributed_total counts a subset of this population, so "
    "without it only an absolute unattributed count is readable and no per-"
    "service RATE is computable — a service emitting 10 unattributed events out "
    "of 10 was indistinguishable from one emitting 10 out of 10,000. "
    "billing_cost_events_flushed_total deliberately does NOT gain a service_key "
    "label instead: its {status} label set is already consumed by the alert "
    "rules in gitops-production, and widening a label set silently re-partitions "
    "every existing series. Counted at the same point in the same loop as the "
    "unattributed counter (services.cost_flusher) so the two can never drift "
    "apart — both include stream redeliveries that a later dedup drops, which "
    "cancels in the ratio.",
    ["service_key"],
)

license_resolved_total = Counter(
    "billing_license_resolved_total",
    "Cost events that arrived with NO license_id and were attributed by "
    "RESOLVING the billed tenant's own licence for the emitting service's app "
    "(services.license_resolver). Counted separately from an asserted licence on "
    "purpose: this attribution is DERIVED by billing, not presented by the "
    "caller, and a derived number must never be indistinguishable from evidence. "
    "It is an accounting stamp only — the tenant was already charged; this names "
    "which invoice line the existing charge belongs to. Read it against "
    "billing_license_unattributed_total, which still counts everything left "
    "unresolved (no match, ambiguous, or an unmapped service_key).",
    ["service_key"],
)

# ⛔ NOT BIRTHED HERE, AND THE OMISSION IS THE DESIGN — see
# `services.license_resolver.birth_resolved_series`, which is called once from
# `main.lifespan`. No increment is added anywhere: the one at
# `license_resolver.record_resolved` is live and correct.
#
# WHY BIRTH AT ALL, stated as the measurement rather than as a worry. Live
# 2026-09-14 this counter has exactly ONE series, against an operator map
# (`cost_attribution_app_by_service_key`) carrying 14 service_keys that day —
# read the setting, never a remembered count. A child is created only when that
# key actually resolves a licence, so for every unborn key "this service stopped
# resolving" and "this service has never resolved" are the SAME empty vector —
# and both `billing:licence_derived_share:1h` and
# `BillingLicenceAssertionRegressed` are evaluated `by (service_key)`. The unborn
# keys are precisely the ones an operator would want to notice going quiet, and
# they are the ones that cannot be.
#
# What CANNOT be done is birthing it at import on `"unknown"`, the way
# billing_license_unattributed_persisted_total above legitimately is. The two
# counters do NOT share a label domain even though they share a label NAME:
# `record_resolved` is reached ONLY from inside cost_flusher's
# `if app_by_key:` block, and only after `resolve_license` returned a licence —
# which it can only do when `app_by_service_key.get(service_key)` hit. So the
# producible domain is exactly the KEYS of the operator's
# `cost_attribution_app_by_service_key`, and `"unknown"` is not one of them
# unless an operator literally configures that key. Birthing `"unknown"` here
# would pin a series at 0 forever — an instrument that reads as though it is
# watching a healthy population while watching nothing, which is the failure
# this file records on the two markup modes above.
#
# That domain is only knowable once settings are resolved, so the birth lives at
# boot instead of at import. It is NOT a hand-copied roster: it is derived from
# the same `parse_service_key_app_map` expression that gates the increment, so
# the birth list and the reachable set cannot drift apart.

app_resolved_total = Counter(
    "billing_app_resolved_total",
    "Cost events that arrived with NO app_id and were stamped by deriving the "
    "app from the event's own licence (services.license_resolver.resolve_app). "
    "This derivation is UNAMBIGUOUS in a way licence resolution is not: "
    "license_id is the PRIMARY KEY of request_quota_config, so a licence maps "
    "to exactly one app by construction and the 'exactly one, or nothing' rule "
    "that drops duplicate (tenant, app) pairs cannot apply here. Counted "
    "separately from an asserted app_id for the same reason as "
    "billing_license_resolved_total: a derived number must never be "
    "indistinguishable from one the caller presented. Read it against "
    "billing_app_unattributed_total.",
    ["service_key"],
)

app_unattributed_total = Counter(
    "billing_app_unattributed_total",
    "Cost events stored with app_id IS NULL after every derivation attempt. It "
    "exists so the gap stays measurable rather than being masked by the "
    "derivation above: an event whose licence is itself unknown, or whose "
    "licence predates migration 0014 and carries a NULL app_id, can never be "
    "attributed to an app and must keep being counted. NOT interchangeable with "
    "billing_license_unattributed_total, and NOT its per-app equivalent: the "
    "two count over DIFFERENT DENOMINATORS. license_unattributed is incremented "
    "at intake over pre_rows, BEFORE the idempotency claim, so a stream "
    "redelivery inflates it; this counter is incremented over `survived`, AFTER "
    "the claim and the event_key dedup, so a redelivery it drops is never "
    "counted here. Their ratio to each other is therefore meaningless -- read "
    "each against its own denominator (billing_cost_events_by_service_total for "
    "the licence counter, billing_app_resolved_total + this one for the app "
    "gap). One honest caveat on this one: it is incremented in memory BEFORE "
    "the batch transaction commits, so a rollback un-claims the messages and "
    "the replayed batch increments it a second time for the same events. It "
    "over-counts on a failed flush and is a trend, not a ledger.",
    ["service_key"],
)

app_key_resolved_total = Counter(
    "billing_app_key_resolved_total",
    "Cost events whose app_id arrived as an app KEY (the X-App-Id slug, e.g. "
    "core-app) and was resolved to the registry app uuid through the "
    "app_registry mirror of the licences app registry (migration 0050; "
    "services.license_resolver.get_app_key_map). "
    "The label is bounded: it is only ever a key present in that map; anything "
    "else is counted on billing_app_key_unknown_total instead. `stream` is "
    "cost (cost_flusher) or metering (event_flusher).",
    ["stream", "app_key"],
)

app_key_unknown_total = Counter(
    "billing_app_key_unknown_total",
    "Cost events whose app_id arrived as a non-uuid app key that the mirror "
    "does not know (or maps ambiguously). The row is stored with app_id NULL "
    "and is deliberately NOT derived from its licence: for inference-router "
    "traffic the licence names the router, not the caller, so a derived app "
    "would be a guessed caller. Unlabelled on purpose: the key is caller-"
    "controlled free text. Non-zero means a caller is not registered in "
    "licences, or app_registry has not been synced since migration 0050. "
    "`stream` is cost (cost_flusher) or metering (event_flusher).",
    ["stream"],
)
for _stream in ("cost", "metering"):
    app_key_unknown_total.labels(stream=_stream)

app_registry_refresh_total = Counter(
    "billing_app_registry_refresh_total",
    "Periodic app_registry refreshes (services.app_registry."
    "run_app_registry_refresh_loop) by outcome. `ok` = /internal/apps was read "
    "and mirrored. `failed` = sync_app_registry reported an error (licences "
    "unreachable / non-200 / no usable apps / DB write failed); the mirror KEEPS "
    "its previous rows. `error` = the tick raised, which should be unreachable. "
    "Sustained `failed` means newly registered apps' spend is stored app_id NULL.",
    ["result"],  # ok | failed | error
)
for _result in ("ok", "failed", "error"):
    app_registry_refresh_total.labels(result=_result)

app_registry_refresh_last_success_timestamp_seconds = Gauge(
    "billing_app_registry_refresh_last_success_timestamp_seconds",
    "Unix time of the last SUCCESSFUL periodic app_registry refresh in this "
    "process, 0 when none has landed (or the loop is disabled). Alert on "
    "time() - this, not on the counter.",
)
app_registry_refresh_last_success_timestamp_seconds.set(0)

#: The two values ``app_key_resolved_total``'s ``stream`` label can take. Its
#: ``app_key`` label is bounded by the key map, so those children are born when
#: the map is built (``license_resolver.birth_app_key_resolved_series``), not
#: here: the domain is only knowable from the database.
APP_KEY_STREAMS: tuple[str, ...] = ("cost", "metering")

app_key_map_fallback_total = Counter(
    "billing_app_key_map_fallback_total",
    "Times the X-App-Id key map was built from request_quota_config.app_key "
    "(licensed apps only) because the app_registry mirror (migration 0050) was "
    "EMPTY. Non-zero means POST /api/budget/sync-from-licenses has not yet "
    "filled app_registry from GET {licenses_url}/internal/apps, so platform "
    "apps that hold no licence (core-app, chat-bridge*) still resolve as "
    "unknown.",
)

cost_events_health_probe_total = Counter(
    "billing_cost_events_health_probe_total",
    "Cost events minted by a SELF-METERING HEALTH PROBE, counted at the row "
    "level and labelled by the allowlist identity that matched plus what was "
    "DONE about it. "
    "THE GAP: readiness probes across the estate deliberately call REAL "
    "data-plane endpoints — that is what makes them readiness probes — and "
    "every such call goes through a metered route and mints a billable cost "
    "event. Measured on the prod billing primary 2026-09-07: 38,494 of 39,519 "
    "rows carrying the mlx-backend service key in 24h (99.96%) were probe "
    "traffic, and "
    "probe rows were ~82% of billing_cost_events carrying 0.41% of the money. "
    "It is a DATA-INTEGRITY AND VOLUME defect, not an over-billing one. "
    'action="excluded" means the row was PARKED — stored and auditable, '
    "excluded_from_billing=true, exclusion_reason='health_probe', dropped from "
    "every billable aggregate, withheld from the token rollup and the budget "
    "debit. "
    'action="counted" means the kill switch '
    "settings.billing_exclude_health_probes was set False and NOTHING was done: "
    "the row billed, rolled up and debited exactly as before, and this counter "
    "is the only trace. "
    "⛔ THE ACTION LABEL IS THE POINT, and is why this is not named "
    "billing_cost_events_probe_excluded_total: the same series must stay honest "
    "in both states of a flag the dashboard cannot see. Same shape and same "
    "reasoning as billing_cost_events_unregistered_tenant_total below. "
    "⛔ THIS COUNTER IS THE ANTI-SINK. An exclusion nobody can see is a silent "
    "hole in the ledger, so the excluded population must stay measurable AFTER "
    "it stops being billable — that is the entire reason the row is parked "
    "rather than dropped and the entire reason this series carries subject_id "
    "and endpoint. Label cardinality is bounded by construction: the counter is "
    "incremented ONLY on an allowlist hit, so its label domain can never exceed "
    "health_probes.HEALTH_PROBE_EMITTERS (3 entries as of 2026-09-07). "
    "⚠️ subject_id AND tenant_id are BOTH labels because subject_id alone is not "
    "an identity: the karma subject 01a0138e-… serves two tenants, only one of "
    "which is the probe, and a series keyed on the subject alone would report "
    "the exclusion of one as the exclusion of the other. "
    "WHAT TO WATCH: this series going FLAT for a subject that is still probing "
    "means the allowlist has rotted (a rotated service account, a renamed "
    "endpoint) and probe rows have quietly resumed billing — an allowlist that "
    "has stopped matching looks exactly like one that has nothing to match. "
    "tests/test_health_probe_exclusion.py::test_every_allowlist_entry_is_a_"
    "measured_subject is the CI half of that guard; this series is the "
    "production half.",
    ["subject_id", "tenant_id", "service_key", "endpoint", "action"],
)

budget_writes_refused_unregistered_tenant_total = Counter(
    "billing_budget_writes_refused_unregistered_tenant_total",
    "Token-budget WRITES refused because the tenant_id no registry vouches for, "
    "labelled by the write's `source` (manual / topup / license_sync). "
    "⚠️ NOT the same population as billing_cost_events_unregistered_tenant_total "
    "and deliberately a separate series: that one counts a MONEY RECORD arriving "
    'for an unknown tenant, which is billed anyway (action="counted") because '
    "dropping revenue is worse than mis-attributing it. This one counts a PAYER "
    "ROW being refused, which loses nothing -- the caller is told and the licence "
    "is still there to retry. Summing them would add a thing that happened to a "
    "thing that was prevented. "
    "WHY IT EXISTS AS A COUNTER AND NOT JUST A LOG LINE: a refusal whose only "
    "trace is a logger.warning is undetectable -- nobody greps logs on a "
    "schedule. The whole purpose of the guard is that an unowned tenant becomes "
    "LOUD rather than silently acquiring a budget, and an alert needs a series. "
    "EXPECTED VALUE IS ZERO. A non-zero rate means some service is syncing "
    "licences for a tenant that is not in the registry -- either the tenant was "
    "never registered, or it was archived, or the id is not a tenant at all (a "
    "Keycloak sub or a licences subject.id forwarded as X-Tenant-Id). All three "
    "are real and all three have been observed. "
    "⚠️ A SUSTAINED ZERO IS AMBIGUOUS and must not be read as "
    '"no unowned tenants": the guard is freshness-gated, so it also reads zero '
    "while the live tenant overlay is stale or was never loaded. Pair it with "
    "the overlay's own age before concluding anything from a flat line.",
    ["source"],
)

#: The ONLY values ``budget_writes_refused_unregistered_tenant_total`` may carry.
#: ⛔ ``source`` reaches ``budget_crud`` from ``TokenBudgetCreate.source``, which is
#: a bare ``str`` on the request body — i.e. CALLER-SUPPLIED FREE TEXT. Labelling
#: with it unclamped would be an unbounded-cardinality series, the exact hazard the
#: cost counters in this module are bounded to two labels to avoid. So the writer
#: clamps to this tuple and everything else becomes ``"other"``, which keeps the
#: label space enumerable AND makes the series birthable below.
BUDGET_WRITE_SOURCES: tuple[str, ...] = ("manual", "topup", "license_sync", "other")

# Born at zero for the reason the loop below gives: absence and zero are the same
# observation to a dashboard, and an alert on a series that does not exist yet is
# vacuous. Every value is reachable -- three are real `source` literals and
# "other" is the writer's own fallback.
for _source in BUDGET_WRITE_SOURCES:
    budget_writes_refused_unregistered_tenant_total.labels(source=_source)


budget_reserve_refused_total = Counter(
    "billing_budget_reserve_refused_total",
    "Pre-spend budget RESERVATIONS refused by POST /api/budget/reserve, labelled "
    "by `reason`. ⛔ THIS COUNTS A REFUSAL, NOT AN ADJUDICATION -- the denominator "
    "lives in the caller (inference_router_budget_gate_adjudications_total), "
    "deliberately, because billing cannot see how many requests the gate decided "
    "NOT to ask about. Reading this alone tells you refusals happened, never what "
    "share of traffic they were. "
    "⚠️ `tenant_archived` is the only reason here that is an AUTHORISATION verdict "
    "rather than an accounting one. The other values mirror "
    "BudgetReserveResponse.reason and are produced inside budget_redis.py. "
    "EXPECTED VALUE IS ZERO for tenant_archived in a healthy estate: an archived "
    "tenant should have stopped sending traffic. Measured 2026-09-10 it would NOT "
    "have been zero -- `rmt-e2e` (019fb1f4-...-645f), archived 2026-09-03, was "
    "still emitting 405 cost events ($1.60, all cost_basis=internal_transfer) "
    "within the preceding 14 days, which is the whole reason this refusal exists.",
    ["reason"],
)

#: The ONLY values ``budget_reserve_refused_total`` may carry.
#: ⛔ Born at zero below, for the reason its sibling above gives: absence and zero
#: are the same observation to a dashboard, and an `increase(...) > 0` alert on a
#: series that does not exist yet is vacuous.
#: ⚠️ These mirror the literals constructed in ``services/budget_redis.py`` and
#: documented on ``BudgetReserveResponse.reason``; nothing enforces the agreement,
#: so a new reason must be added in all three places.
BUDGET_RESERVE_REFUSAL_REASONS: tuple[str, ...] = (
    "over_budget",
    "no_active_budget",
    "redis_unavailable",
    "tenant_archived",
    # The writer's own fallback for a reason it does not recognise -- including
    # `None`, which BudgetReserveResponse.reason permits. Present here so the
    # series is birthable; the same role "other" plays in BUDGET_WRITE_SOURCES.
    "other",
)

for _reason in BUDGET_RESERVE_REFUSAL_REASONS:
    budget_reserve_refused_total.labels(reason=_reason)


#: Label domain for billing_money_reserve_refused_total -- the §3.1 closed set
#: plus "other" (the clamp target), birthed below so every series exists at 0.
MONEY_RESERVE_REFUSAL_REASONS: tuple[str, ...] = (
    "over_budget",
    "no_active_budget",
    "tenant_archived",
    "fx_rate_missing",
    "price_missing",
    "other",
)

money_reserve_refused_total = Counter(
    "billing_money_reserve_refused_total",
    "EUR money-budget reservations refused (POST /api/budget/money/reserve), by "
    "reason. no_active_budget is a hard stop on this path, unlike the token gate.",
    ["reason"],
)
for _reason in MONEY_RESERVE_REFUSAL_REASONS:
    money_reserve_refused_total.labels(reason=_reason)


#: The cost event predates the FIRST fx_rate row (the first row was not
#: back-dated far enough); distinct from a gap, because the fix differs.
UNPRICED_BEFORE_FIRST_FX = "before_first_fx_rate"
#: Label domain for billing_money_unpriced_events_total -- the MoneyRefusal
#: reasons settlement can hit, plus the before-first-FX refinement.
MONEY_UNPRICED_REASONS: tuple[str, ...] = (
    "price_missing",
    "fx_rate_missing",
    "markup_missing",
    UNPRICED_BEFORE_FIRST_FX,
)

money_unpriced_events_total = Counter(
    "billing_money_unpriced_events_total",
    "Money-tenant cost events stored with NULL EUR consumption at settlement "
    "(spend happened, could not be priced, NOT debited from the money budget), "
    "by reason. Any increase is unbilled spend.",
    ["reason"],
)
for _reason in MONEY_UNPRICED_REASONS:
    money_unpriced_events_total.labels(reason=_reason)


cost_events_unregistered_tenant_total = Counter(
    "billing_cost_events_unregistered_tenant_total",
    "Cost events whose tenant_id is a well-formed, non-nil uuid that the "
    "licences-registry snapshot in tenancy.LEGACY_TENANT_NAMES does NOT vouch "
    "for, counted at the row level and labelled by what was DONE about it. "
    "THE GAP: canonical_tenant_id parses a uuid with the stdlib and returns it "
    "unchecked, so a caller could mint a tenant just by putting a random uuid in "
    "X-Tenant-Id and be billed as one; validate_cost_event does not catch it "
    "either, because an unregistered uuid RESOLVES perfectly. "
    'action="counted" means the gate was DISARMED '
    "(BILLING_EXCLUDE_UNREGISTERED_TENANTS=false) and nothing was done: the "
    "row was billed, rolled up and debited exactly as before, and this counter "
    'is the only trace. action="excluded" (the default, armed in prod since '
    "2026-09-16) means settings.billing_exclude_unregistered_tenants was armed "
    "and the row was "
    "PARKED — stored and auditable, excluded_from_billing=true, dropped from "
    "every billable aggregate, withheld from the token rollup and the budget "
    "debit. "
    "⛔ THE ACTION LABEL IS THE POINT, AND IS WHY THIS IS NOT NAMED "
    "billing_cost_events_excluded_total. That name asserts an exclusion that "
    "does not happen when the gate is disarmed, so it would read as a fleet of "
    "parked rows when in fact nothing was parked — one metric name meaning two "
    "opposite things depending on a flag the dashboard cannot see. One series "
    "family, honest in both states, and the arming decision is readable off it "
    "directly. "
    'ARMING CRITERION: this must read 0 for action="counted" — or name only '
    "service_keys whose tenants are confirmed absent from the licences registry "
    "— over a representative window BEFORE the flag is set True. It is a "
    "DENOMINATOR-FIRST instrument, and its input got BETTER on 2026-09-05: the "
    "gate now reads the LIVE licences registry (tenancy's overlay, refreshed "
    "from GET /internal/tenants — measured 200 with 59 tenants from a billing "
    "pod; the 'billing gets HTTP 403' note that stood here was stale) and falls "
    "back to the hand-refreshed snapshot. That matters because the snapshot was "
    "missing SIX genuinely registered names on 2026-09-05, so arming this on a "
    "snapshot-only input would have parked real customers' rows. Read "
    'billing_tenant_registry_drift{kind="missing_from_snapshot"} and '
    "billing_tenant_registry_live_entries BEFORE arming: a zero live-entry count "
    "means the overlay never landed and the gate is back on the snapshot alone. "
    "NOT interchangeable with billing_tenant_unregistered_total: that one fires "
    "at RESOLUTION time on every canonical_tenant_id call, including the HTTP "
    "boundary and calls that never become a row. This one fires once per stored "
    "ROW. Different denominators; a ratio between them means nothing. Labelled "
    "by service_key so an offender is nameable without reading logs — the tenant "
    "uuid is deliberately NOT a label, being caller-controlled and unbounded. "
    "⚠️ ONE HONEST CAVEAT, the same one billing_license_unattributed_total "
    "carries: it is incremented at INTAKE, over pre_rows, BEFORE the idempotency "
    "claim and the event_key dedup — so a Redis stream redelivery inflates it "
    "while the stored ROW count does not move. Read it as a trend, not a ledger; "
    "the authoritative count of PARKED rows is SELECT count(*) FROM "
    "billing_cost_events WHERE exclusion_reason = 'unregistered_tenant'.",
    ["service_key", "action"],
)

#: What the flusher can DO about an unregistered tenant. Bounded, and both are
#: born at zero: "counted" must be visible from the first scrape (it is the
#: default state, and an absent series would read as "no unregistered tenants"
#: rather than "the instrument is off"), and "excluded" must be visible so the
#: flip is observable the moment the flag is armed.
UNREGISTERED_TENANT_ACTIONS: tuple[str, ...] = ("counted", "excluded")

project_id_refused_total = Counter(
    "billing_project_id_refused_total",
    "Cost events whose incoming `project_id` was refused before the row was "
    "stored, labelled by why. "
    'reason="is_tenant_id" -- the caller-supplied `project_id` is ITSELF a uuid '
    "the licences registry vouches for as a TENANT, checked with the SAME "
    "`tenancy.is_registered_tenant_id` billing_cost_events_unregistered_tenant_total "
    "above already runs against the row's own tenant_id. D1 (billing-licensing "
    "estate repair, round 16): measured on prod 2026-09-17, two distinct tenant "
    "uuids were being used as project_id under a DIFFERENT payer tenant -- "
    "0195f48b-...-0005 (129 rows) and -0004 (37 rows) -- a class "
    "billing_project_id_equals_tenant_projects CANNOT see, because that detector "
    "only compares a row's project_id against ITS OWN tenant_id, and these rows "
    "carry someone else's. `project_id` is NULLED before the row is stored -- "
    "never the whole event -- so the money survives with no project "
    'attribution instead of silently keeping a "project" that is actually '
    "another tenant's identity. Owner ruling 2026-10-07: also counted here when "
    "project_id is ANOTHER registered tenant's tenant_level_project_id (the "
    "reversed-hex id project-less apps send); the event's OWN tenant-level "
    "project id is kept, covered, and not counted. "
    'reason="registry_unavailable" -- the tenant-registry check itself could '
    "not answer (a defensive fallback: `is_registered_tenant_id` / "
    "`classify_tenant_id` are documented never to raise, but this ingest path "
    "refuses to null a caller's project_id on anything less than a POSITIVE "
    'match). `project_id` is KEPT, unchanged, in this case -- "refuse only on '
    'positive knowledge, never on an absent answer" is the same rule '
    "billing_cost_events_unregistered_tenant_total's own arming criterion is "
    "built on, and inverting it here would let a transient classifier fault "
    "erase real caller-supplied project attribution. "
    'reason="foreign_tenant" / "unknown_project" / "project_tenant_unset" -- '
    "(2026-09-28) the project -> tenant OWNERSHIP check against fr's "
    "projects.keycloak_tenant_id map (services.project_owner_map): the project "
    "belongs to a DIFFERENT tenant than the event's, is absent from a non-empty "
    "map, or is known with no tenant of record. BILLING_PROJECT_OWNERSHIP_MODE="
    "observe (default) COUNTS these and KEEPS project_id (stamping "
    "metadata.project_id_ownership); =enforce NULLS it for foreign_tenant/"
    "unknown_project only (metadata.project_id_refused). NB in observe mode this "
    "counter is therefore a count of ADJUDICATIONS, not only of nulled values. "
    "No map (fr down, FR_BASE_URL/FR_INTERNAL_TOKEN unset, empty map) is counted "
    'as reason="owner_map_unavailable" (distinct from the tenancy-registry '
    'reason="registry_unavailable" above) and the value is KEPT in both modes, '
    'stamped metadata.project_id_ownership={"verdict":"unverified"}; '
    'project_tenant_unset is likewise KEPT + stamped "unverified" in both modes. '
    "At most one reason per event. "
    "⚠️ project_id is NOT a label -- same cardinality/disclosure reason as "
    "billing_project_id_multi_tenant_projects.",
    ["reason"],
)

#: Bounded, both born at zero for the same reason UNREGISTERED_TENANT_ACTIONS
#: states above: an absent `is_tenant_id` series reads as "never happens" and
#: an absent `registry_unavailable` series reads as "the fallback is untested",
#: and neither is distinguishable from "the counter was never wired" without
#: this.
PROJECT_ID_REFUSED_REASONS: tuple[str, ...] = (
    "is_tenant_id",
    "registry_unavailable",
    "owner_map_unavailable",
    "foreign_tenant",
    "unknown_project",
    "project_tenant_unset",
)
for _reason in PROJECT_ID_REFUSED_REASONS:
    project_id_refused_total.labels(reason=_reason)

placeholder_value_total = Counter(
    "billing_placeholder_value_total",
    "Fields that carried a PLACEHOLDER rather than a value on a stored cost "
    "event — either the emitter sent a sentinel string ('unknown', 'none', "
    "'n/a', …) or the flusher substituted one for an absent key. THE DEFECT "
    "THIS CLOSES: a defaulted sentinel is indistinguishable from a measured "
    "value once it is in the row. Measured on the prod billing primary "
    "2026-09-05: 968 of 968 inference-router cost events carry "
    "metadata.job_id = 'unknown' — 100%, and nothing anywhere said so, because "
    "the string is stored verbatim and every reader treats it as a job id. The "
    "COERCION IS UNCHANGED — the sentinel is still written, deliberately, "
    "because a NULL would merely move the laundering from 'a fake value' to 'an "
    "honest-looking absence'. This counter makes the fabrication COUNTABLE "
    "instead. Read it against billing_cost_events_by_service_total for the "
    "same service_key to get a rate: 'unknown' on 3 of 40,000 rows is an "
    "emitter hiccup, on 968 of 968 it is an emitter that has never populated "
    "the field at all. Labelled by field (bounded: the metadata whitelist in "
    "cost_flusher plus the three columns that carry an 'or unknown' default) "
    "and by service_key, the same bounded label the counters above carry.",
    ["field", "service_key"],
)

cost_event_key_source_total = Counter(
    "billing_cost_event_key_source_total",
    "How a stored cost event's per-call idempotency key (`event_key`) was "
    "obtained, labelled by the emitting service_key. THE DEFECT THIS MAKES "
    "VISIBLE: the event_key claim in cost_flusher is the ONLY guard that can "
    "catch a producer re-emitting a call billing has already charged for (the "
    "processed_cost_messages inbox dedupes the Redis MESSAGE id, which is minted "
    "fresh on every re-XADD). A producer that sends no key silently opts out of "
    "that guard, and a NULL column says nothing about who. "
    "Measured on the prod billing primary over the window "
    "2026-09-05T07:01:58Z … 2026-09-06T08:50:30Z (the table was TRUNCATED at "
    "07:01:56Z, so no earlier reading is valid): 58,249 of 70,492 rows — 82.6% — "
    "carried NO identity field at ALL (no event_key, no provider_request_id, no "
    "correlation_id), at a flat ~2,260 rows/hour. Producers: mlx-backend "
    "(57,711 rows, 100% keyless), kag (528, 100%), ingestion (7), "
    "mlx-ingestion-backend (3). "
    "sources: 'producer' = stamped on the wire; 'provider_request_id' = derived "
    "by billing from the upstream provider's per-call id; 'absent' = the "
    "producer supplied nothing a key could be grounded in, so the row is stored "
    "and billed with a NULL key and NO per-call guarantee. "
    "⛔ THIS IS THE ARMING CRITERION for ever REJECTING a keyless event: the "
    "estate rule is DO NOT CLOSE A GATE BEFORE ITS COUNTER READS 0, and at 82.6% "
    "a rejection today is an outage, not a fix. Read `absent` against "
    "billing_cost_events_by_service_total to find which emitter to fix first.",
    ["service_key", "source"],
)

# Birth the `absent` series on the fallback service_key, same rule as every
# other counter in this module: a source that has never fired must read 0 rather
# than be absent, or "every producer stamps a key" and "the derivation never ran"
# are the same observation. service_key is an OPEN domain, so only the literal
# the flusher substitutes for an absent one is born.
for _src in EVENT_KEY_SOURCES:
    cost_event_key_source_total.labels(service_key="unknown", source=_src)

# Birth both label sets at zero, same rule as COST_DEAD_LETTER_REASONS above: a
# series that has never fired must read 0, not be absent, or "no exclusions" and
# "the instrument was never wired up" are the same observation.
#
# service_key is an OPEN domain (it is whatever an emitter puts on the wire), so
# only the fallback value is born — the same compromise pricing_miss_total makes
# with provider/model. "unknown" is not an arbitrary pick: it is the literal the
# flusher substitutes for an absent service_key, so it is the one value that is
# certain to be reachable.
for _action in UNREGISTERED_TENANT_ACTIONS:
    cost_events_unregistered_tenant_total.labels(
        # detect-secrets' "Secret Keyword" rule fires on any `*_key = "<literal>"`,
        # and this is a metric LABEL VALUE naming the emitting service, not a
        # credential. Pragma'd at the site rather than absorbed into
        # .secrets.baseline: a baseline entry is an opaque hash that says nothing
        # about why it is safe, and every one added makes the next real finding
        # easier to wave through.
        service_key="unknown",  # pragma: allowlist secret
        action=_action,
    )

cost_events_billed_while_archived_total = Counter(
    "billing_cost_events_billed_while_archived_total",
    "Cost events stored for a tenant `services.tenant_names.is_tenant_archived` "
    "answered True for AT FLUSH TIME, labelled by service_key. "
    "⛔ THIS COUNTS A BILLED ROW, NOT A REFUSAL — there is no action label, "
    "unlike its neighbour billing_cost_events_unregistered_tenant_total, "
    "because archival never parks a row here: `archived_at` is enforced "
    "pre-spend in FOUR other places (licences' licence-mint and token-mint "
    "paths, and routers/budget.py's reservation gate); this ingestion path is "
    "reached AFTER the compute already happened, so refusing would only "
    "destroy the money record, not the spend. Every row this counter fires "
    "for is billed, rolled up and debited exactly like any other — see "
    "billing_cost_events.billed_while_archived for the same fact, durable, "
    "on the row. "
    "⚠️ PER-POD-LIFETIME, LIKE THE OLDER billing_tenant_unregistered_total — "
    "a restart resets it to 0, which reads identically to 'this has never "
    "happened'. It is a live trend signal, never the standing proof; the "
    "standing proof is the ledger-derived gauge in "
    "services.archived_tenant_spend, which reads billed_while_archived from "
    "the table itself and survives a restart.",
    ["service_key"],
)
# Born at zero on the fallback service_key, same rule as every other counter
# in this module: a series that has never fired must read 0, not be absent.
cost_events_billed_while_archived_total.labels(
    service_key="unknown",  # pragma: allowlist secret
)

for _field in PLACEHOLDER_COUNTED_FIELDS:
    placeholder_value_total.labels(field=_field, service_key="unknown")  # pragma: allowlist secret

metering_placeholder_value_total = Counter(
    "billing_metering_placeholder_value_total",
    "The metering-stream twin of billing_placeholder_value_total: fields that "
    "carried a PLACEHOLDER rather than a value on a stored METERING event "
    "(billing_metering_events), counted at the moment the row is minted. "
    "SEPARATE FROM THE COST COUNTER ON PURPOSE — that one's documented "
    "denominator is billing_cost_events_by_service_total, so folding metering "
    "rows into it would put two populations on one series and make every rate "
    "taken against that denominator wrong. THE DEFECT THIS CLOSES "
    "(billing-service #135): billing_metering_events.tenant_name stored the "
    "tenant's own UUID whenever no name could be resolved — 9 of 10 tenants, "
    "4,469 rows, and ZERO honest absences ever written. A uuid is WORSE than an "
    "empty string there: it is non-empty and distinct per tenant, so it "
    "satisfies downstream truthiness guards and reads as data. fr's metering "
    "page guards its id column with {#if t.tenant_name} and therefore printed "
    "the same id twice (narrowed server-side in fr#980). The uuid case is now "
    "stored as '' and counted here; an emitter's own sentinel ('unknown', …) is "
    "still stored VERBATIM and counted here too, because nulling it would move "
    "the laundering rather than end it. Labelled by field (bounded: "
    "METERING_PLACEHOLDER_COUNTED_FIELDS) and by service_key.",
    ["field", "service_key"],
)

# Born at zero, same rule as its twin above: an unfired series must read 0
# rather than be absent, or "no placeholders" and "the instrument was never
# wired up" become the same observation.
for _field in METERING_PLACEHOLDER_COUNTED_FIELDS:
    metering_placeholder_value_total.labels(
        field=_field,
        service_key="unknown",  # pragma: allowlist secret
    )

cost_actor_daily_distinct_actors = Gauge(
    "billing_cost_actor_daily_distinct_actors",
    "Distinct REAL actor_id values (i.e. excluding NULL_ACTOR_ID, the "
    "00000000-0000-0000-0000-000000000005 sentinel models.db.rollups.CostActorDaily "
    "coalesces a missing actor to) currently present in billing_cost_actor_daily, "
    "set by services.rollup_engine.daily_actor_axis_cardinality on every daily pass. "
    "WHY THIS EXISTS: alembic 0021 made cost_events.actor_id nullable and NEVER "
    "defaulted -- 'no emitter sends this yet; this is the wire contract future "
    "emitters land on' (src/services/cost_flusher.py). Adopting emitters are landing "
    "on that contract gradually, so this axis can sit at 0 (or near it) for a long "
    "stretch while every row still carries a correct sentinel and every dollar is "
    "still fully attributed on subject_id (WHO PAYS, a DIFFERENT axis from actor_id, "
    "WHO ACTED -- see CostActorDaily's docstring). A dashboard grouping by actor_id "
    "renders one bar in that state and looks like a working per-user breakdown; it "
    "is not one until emitters adopt the field. THIS GAUGE IS THE AXIS'S OWN "
    "CARDINALITY, not a fix and not a defect signal by itself -- 0 is the correct "
    "reading for a fleet with no actor-aware emitters onboarded yet. Read it "
    "alongside a deploy of an actor-sending emitter: if it does not move afterward, "
    "THAT is the defect to chase, not this number in isolation.",
)

rollup_date_gaps = Gauge(
    "billing_rollup_date_gaps",
    "Complete days inside a daily rollup table's own covered span that have NO "
    "row in it, as measured by the last daily pass (services.rollup_engine."
    "daily_rollup_date_gaps). THE DEFECT THIS CLOSES: missing_daily_dates -- the "
    "repair list -- derives its candidate dates FROM metering_hourly / "
    "cost_events, so a day with no source rows at all was never a candidate and "
    "the engine could not notice the day was absent. The evidence of the gap and "
    "the evidence needed to detect it were the same rows. Measured 2026-09-05: "
    "billing_cost_daily holds zero rows for 2026-09-02..09-04 and would never "
    "have reported them, because the 07:01:56Z TRUNCATE removed the events those "
    "dates would have been derived from. This gauge is derived from a CALENDAR "
    "range instead and is therefore blind to the source tables entirely. "
    "REPORTING ONLY -- nothing repairs these days, deliberately: re-rolling a day "
    "whose source is gone DELETES the surviving bucket rather than rebuilding it "
    "(the guard at rollup_daily's delete is what protects the $5,613.14 of "
    "rollups cost_events can no longer substantiate). NON-ZERO IS NOT "
    "AUTOMATICALLY A BUG: a day with no billable activity legitimately has no "
    "row, so read this against the raw sources before acting -- it says 'look', "
    "not 'this is broken'. AGGREGATE WITH max(), NEVER sum(): the daily pass is "
    "leader-elected, but a leadership handover inside a scrape window can leave "
    "two replicas holding a value, and sum() would then double the gap count.",
    ["table"],
)

# Born at zero for both tables, same rule as the counters above: an absent series
# and "no gaps" must not be the same observation -- the whole failure mode here
# was a gap that nothing reported.
for _rollup_table in ("cost_daily", "metering_daily"):
    rollup_date_gaps.labels(table=_rollup_table)

rollup_lag_days = Gauge(
    "billing_rollup_lag_days",
    "Days between today (UTC) and the NEWEST bucket_date in a daily rollup table, "
    "set by services.rollup_engine.daily_rollup_lag_days on every daily pass. "
    "HEALTHY IS 1 (the last closed UTC day is rolled); >= 2 means closed, "
    "rollable days have not been rolled. THE DEFECT THIS CLOSES: every signal "
    "this service had answered 'did the job run?' -- billing_rollup_cycles_total "
    "counted successful leader cycles, the loop logged daily_rollup_completed on "
    "every pass, pods stayed Ready -- and all of them were GREEN while the daily "
    "rollup advanced ONE bucket per run and sat permanently ~3 days behind. That "
    "un-rolled backlog is exactly what the 2026-09-05T07:01:56Z TRUNCATE "
    "destroyed. A last_successful_run timestamp is the instrument that hid this: "
    "it was correct every single day of the incident. ALERT ON THIS VALUE, not on "
    "run freshness -- 'it ran' and 'it is caught up' are different propositions "
    "and only this one measures the second. NOT a substitute for "
    "billing_rollup_date_gaps: that counts days ABSENT INSIDE the table's covered "
    "span (is my history holed?) and its own docs say non-zero is not necessarily "
    "a bug, so it cannot be alerted tightly; this measures whether the HEAD has "
    "stopped moving and is 1 in every healthy state. A table with no rows at all "
    "publishes NO sample (there is no head to be behind) -- cover that with "
    "absent(), not a sentinel, because every sentinel is also a readable lag. "
    "AGGREGATE WITH max(), NEVER sum(): same leadership-handover reason as "
    "billing_rollup_date_gaps.",
    ["table"],
)

rollup_unrolled_source_days = Gauge(
    "billing_rollup_unrolled_source_days",
    "Closed days in [floor, yesterday] that HAVE source rows but NO daily row, "
    "per table; set by services.rollup_engine.unrolled_source_days on every "
    "daily pass. THE QUESTION THIS ANSWERS, WHICH NEITHER SIBLING CAN: "
    "billing_rollup_lag_days and billing_rollup_date_gaps are both derived from "
    "the DAILY TABLE's own span, so a day on which nothing was billed makes the "
    "lag grow and the gap count tick up with no defect at all -- measured "
    "2026-11-26, metering_daily read lag=2 because billing_metering_events held "
    "ZERO rows for 2026-11-25, while 285 rollup cycles ran in 24h and catch-up "
    "remaining was 0. Zero here on a legitimately empty day; non-zero ONLY when "
    "a closed day that has source rows lacks its aggregate. That makes it the "
    "one rollup gauge an alert can gate on without crying wolf. "
    "⚠️ DO NOT PRE-CREATE THE CHILDREN. A labelled Gauge publishes nothing until "
    "a child exists, and that is deliberately the state before the first pass: "
    "the alerting gate is `unless (... == 0)`, so an unpublished 0 must not read "
    "as 'healthy' before anything has been measured -- absent here means the "
    "gate is inert and the older alerts keep their previous behaviour. "
    "AGGREGATE WITH max(), NEVER sum(): leadership handover, as with its "
    "siblings.",
    ["table"],
)

metering_hourly_lag_hours = Gauge(
    "billing_metering_hourly_lag_hours",
    "Hours between the last CLOSED UTC hour and the NEWEST bucket_hour in "
    "billing_metering_hourly, set by services.rollup_engine."
    "metering_hourly_lag_hours on every leader tick. HEALTHY IS 1 (the last "
    "closed hour is rolled); >= 2 means closed, rollable hours have not been "
    "rolled. THE GAP THIS CLOSES: billing_rollup_lag_days covers the DAILY "
    "tables only -- {table='cost_daily'} and {table='metering_daily'} -- and "
    "NOTHING measured the hourly stage, which is the FIRST of metering's two "
    "aggregation stages and the one that decides whether a raw metering event "
    "has reached any aggregate at all. A metering rollup stalled in the hourly "
    "stage was therefore exactly as invisible as the 3-day cost-daily lag that "
    "the 2026-09-05T07:01:56Z TRUNCATE turned into permanent loss: "
    "billing_rollup_cycles_total counted successful cycles, the loop logged "
    "completion, pods stayed Ready. Worse than invisible, in fact: "
    "rollup_daily builds a day from whatever hourly holds WITHOUT requiring all "
    "24 hours, so billing_rollup_lag_days{table='metering_daily'} can read a "
    "healthy 1 over a day whose hourly coverage is holed. ⛔ This series and "
    "that one are NOT substitutes and neither implies the other. "
    "billing_unrolled_metering_truncate_guard_trg (alembic 0023) refuses a "
    "TRUNCATE while this lag is real, and it refuses SILENTLY -- from the "
    "operator's side a blocked truncate and a caught-up rollup look nothing "
    "alike, but from the monitoring side, without this gauge, they looked "
    "identical. An empty billing_metering_hourly publishes NO sample (there is "
    "no head to be behind) -- never a sentinel, because every sentinel is also "
    "a readable lag. ⚠️ CORRECTED 2026-09-08: this used to say 'cover that with "
    "absent()'. THAT CANNOT WORK FOR THIS SERIES and it was the more dangerous "
    "half of the advice. Measured with prometheus_client: an UNLABELLED Gauge "
    "is exported from the moment it is CONSTRUCTED (`probe_unlabelled 0.0` "
    "appears in generate_latest before any .set()), whereas a LABELLED one "
    "exports nothing until a child exists. So this series is NEVER absent -- it "
    "reads 0.0 on a replica that has never measured, and 0 is LOWER than the "
    "healthy 1, i.e. the never-measured state renders as better-than-healthy. "
    "billing_rollup_lag_days CAN use absent() because it is labelled; this one "
    "cannot. ⇒ SELECT ON THE PAIR: this value may only be believed together "
    "with a RECENT billing_metering_hourly_lag_timestamp_seconds below. That "
    "is the same construction billing_attribution_audit_last_success_timestamp_"
    "seconds already uses for the coherence gauges. AGGREGATE WITH max(), NEVER "
    "sum(): billing-service runs replicas and leadership hands over, same "
    "reason as billing_rollup_lag_days. ⚠️ NO ALERT RULE SHIPS WITH THIS GAUGE: "
    "a threshold has to come from an observed distribution and none has been "
    "measured yet. A rule with a guessed threshold is worse than a gauge with "
    "none.",
)

metering_hourly_lag_timestamp_seconds = Gauge(
    "billing_metering_hourly_lag_timestamp_seconds",
    "Unix time at which the billing_metering_hourly_lag_hours value beside it "
    "was written. Set ONLY when a lag sample was actually published, and "
    "written AFTER it, so the two are never out of step. "
    "🔴 WHY IT EXISTS: a prometheus_client Gauge is process-lifetime state. It "
    "keeps exporting the last value it was given for as long as the process "
    "lives, whether or not anyone is still measuring. The FIRST version of "
    "billing_metering_hourly_lag_hours shipped in this PR could be left at a "
    "healthy 1 indefinitely: run_rollup_loop called rollup_hourly() UNGUARDED "
    "immediately before the measurement, so the exception that makes the hourly "
    "stage fall behind -- the one thing the gauge exists to expose -- was also "
    "the exception that skipped the measurement. Rollup starts throwing at "
    "09:00Z, at 15:00Z the true lag is 6, and the series still reads 1. "
    "STALE-BUT-PLAUSIBLE is the worst of the three states: worse than a real "
    "number, and worse than an honest gap. The loop now measures in a `finally` "
    "so a raising rollup_hourly cannot skip it, and this timestamp covers "
    "everything a `finally` cannot -- the measurement query itself failing, the "
    "table being empty, leadership moving to another replica, the pod wedging. "
    "⛔ DIRECTION: a stalled measurement must read as UNKNOWN, never as a lag of "
    "0 (would look healthier than healthy) and never as a large lag (a false "
    "alarm on a system that is fine). Freezing the value and letting this "
    "timestamp go stale is the only reading that is neither. "
    "0 means this replica has never published a lag sample -- a follower, a pod "
    "that has not ticked yet, or a table with no rows at all. Same construction "
    "as billing_attribution_audit_last_success_timestamp_seconds.",
)

rollup_catchup_remaining_days = Gauge(
    "billing_rollup_catchup_remaining_days",
    "Daily buckets that were DUE and were NOT rolled because the last catch-up "
    "pass hit settings.rollup_catchup_max_days (services.rollup_engine."
    "catchup_daily). NON-ZERO MEANS THE PASS SUCCEEDED WITHOUT FINISHING -- it is "
    "the difference between 'no error' and 'caught up', which is the exact "
    "distinction whose absence let a permanently-behind rollup look healthy. It "
    "is NOT an error state on its own: a catch-up after a long outage is expected "
    "to be non-zero for a few cycles and to fall monotonically, because "
    "run_rollup_loop declines to mark the calendar day done while this is "
    "non-zero and therefore resumes next cycle rather than next day. Read it as a "
    "derivative: non-zero AND not falling is the alert. Unlabelled -- one "
    "catch-up pass covers both daily tables. AGGREGATE WITH max(), never sum().",
)

# Born at zero: an absent series and 'nothing left behind' must not be the same
# observation -- see billing_rollup_date_gaps above.
rollup_catchup_remaining_days.set(0)

attribution_ambiguous_pairs = Gauge(
    "billing_attribution_ambiguous_pairs",
    "How many (tenant, app) pairs the licence resolver had to DROP on its last "
    "map build because the tenant holds two or more licences for the same app. "
    "Every event for such a pair stays license_id NULL forever -- 'exactly one, "
    "or nothing' is deliberate (picking one would invent an invoice line), so "
    "this is not a bug to fix in code but a REGISTRY condition to clean up. It "
    "was invisible before this gauge: the pairs were logged once per build at "
    "info level and nothing counted them, so a duplicate licence could silently "
    "switch a tenant's attribution off. Measured 2026-09-02: 7 pairs across 16 "
    "licences, which is why tenant …099 stopped resolving on 2026-08-30 and why "
    "…101 never has. Unlabelled on purpose -- the pairs are named in the "
    "cost_attribution_ambiguous_pairs log line, and tenant ids here would be "
    "unbounded cardinality. AGGREGATE WITH max(), NEVER sum(): billing-service "
    "runs replicas: 2 and this path has no leader election, so every replica "
    "builds the same map and .set()s the same value -- sum() over 2 replicas "
    "reports 14 for the 7 real pairs, and the number changes when a pod is "
    "added or restarted. Zero is also NOT proof the resolver ran; see the "
    "_build_map comment in services.license_resolver.",
)

#: The INGEST-PATH counterpart to the audit gauges below, and deliberately a
#: COUNTER where those are Gauges. They measure a standing property of the stored
#: window and must be able to FALL when an emitter is fixed; this records an
#: EVENT — "a caller named a licence and billing declined it" — which happened
#: whether or not the emitter is later corrected, so it must only climb.
#:
#: ``reason`` is the adjudication verdict and is bounded by
#: ``services.cost_flusher.LICENSE_REFUSAL_REASONS``. The three are kept apart
#: because they have different owners: ``unknown_license`` is a forged or stale
#: id at the emitter (or a mirror that has not synced), ``foreign_tenant`` is the
#: 81%-of-attributed-spend class ``services.attribution_audit`` measured on prod
#: 2026-08-25 (11,407 events / $363.56 naming someone else's licence), and
#: ``inactive_license`` is a customer whose licence lapsed while their traffic
#: did not.
#:
#: ⚠️ IT COUNTS ARRIVALS, NOT STORED ROWS. Incremented in memory inside the flush
#: transaction, so a rollback un-claims the batch and the replay counts the same
#: events again — the same caveat ``billing_license_unattributed_persisted_total``
#: carries. It is a trend, not a ledger; the ledger is the
#: ``attribution_status='refused_license'`` population on billing_cost_daily.
cp_license_refused_total = Counter(
    "billing_license_refused_total",
    "Cost events whose caller-ASSERTED license_id billing refused, by the "
    "emitter and the reason. The row is still STORED and still debits — the "
    "provider was already paid and destroying the money record is strictly "
    "worse than mis-labelling it — but its license_id is REMOVED, which is what "
    "keeps it out of CostDaily.license_attributed_clause() and therefore off "
    "every invoice line. Zero on an emitter billing is adjudicating is the "
    "healthy reading; zero on one it is NOT adjudicating means nothing at all.",
    ["service_key", "reason"],
)

# Licence/tenant coherence — measured out of band by the rollup leader
# (services.attribution_audit). Gauges, not counters: this is a STANDING
# property of the stored population, so it must be able to fall when an emitter
# is fixed. A counter could only ever climb.
license_tenant_mismatch_events = Gauge(
    "billing_license_tenant_mismatch_events",
    "Cost events in the audit window whose license_id is owned by a tenant "
    "OTHER than the tenant the event is billed to. Worse than an unattributed "
    "event: debit_budget scopes the cascade to the EVENT's tenant, so this "
    "spend drains the billed tenant's pool while the named licence's own budget "
    "stays untouched and its enforcement gate reads near zero.",
)
license_tenant_mismatch_spend_usd = Gauge(
    "billing_license_tenant_mismatch_spend_usd",
    "cost_usd carried by the mismatched events above, over the same window.",
)
license_tenant_delegated_events = Gauge(
    "billing_license_tenant_delegated_events",
    "Cost events in the audit window whose licence owner differs from the "
    "billed tenant BECAUSE inference-router honoured a delegated call (a "
    "service licence spending on behalf of an end tenant; metadata.delegated_by "
    "set by the router, which vouches only for allowlisted subjects). Intended "
    "traffic, so EXCLUDED from billing_license_tenant_mismatch_events. A "
    "delegated_by from any other cost_source is not counted here.",
)
license_tenant_delegated_spend_usd = Gauge(
    "billing_license_tenant_delegated_spend_usd",
    "cost_usd carried by the delegated events above, over the same window.",
)
license_tenant_unresolved_events = Gauge(
    "billing_license_tenant_unresolved_events",
    "Cost events whose license_id is absent from the local ownership map, so "
    "coherence could not be adjudicated either way. Should sit at zero; a rise "
    "means the licence sync is stale, NOT that attribution is wrong.",
)
license_attributed_events = Gauge(
    "billing_license_attributed_events",
    "Cost events in the window whose licence IS owned by the billed tenant. "
    "The denominator — without it a mismatch count cannot be read as a rate.",
)
license_unattributed_events = Gauge(
    "billing_license_unattributed_events",
    "Billable cost events in the audit window carrying NO license_id at all — "
    "the population the coherence gauges above are structurally blind to, "
    "because they can only adjudicate an ownership claim that exists. Measured "
    "on prod 2026-08-27 over billable rows only: 3,751 of 4,993 events in 24h. "
    "Such spend is billed and capped but can never reach an invoice line. "
    "GAUGE, not a counter, on purpose: this is a STANDING property of the "
    "stored window, so it must be able to FALL when a broken emitter is fixed. "
    "A counter could only ever climb, so a repaired gap would keep the panel "
    "red forever and the fix would be indistinguishable from no fix at all.",
)
license_tenant_unregistered_events = Gauge(
    "billing_license_tenant_unregistered_events",
    "Billable cost events in the window whose TENANT appears nowhere in "
    "billing's own registry — no request_quota_config row and no token_budgets "
    "row. Spend booked against an identity billing holds no entitlement record "
    "for at all. Measured on prod 2026-08-28: 26 of the 61 distinct tenant_ids "
    "in billing_cost_events have no row in licenses.tenants either, including a "
    "literal 0195f48b-dead-7000-8000-00000000beef. Nothing validated tenant "
    "identity at the financial boundary. "
    "⚠️ READ THE NAME LITERALLY: this measures absence from BILLING'S LOCAL "
    "registry, NOT from licenses.tenants, which billing cannot see — its "
    "service account gets HTTP 403 on GET /tenants (measured 2026-08-28), so "
    "the truth is not reachable from here. As a proxy for the real thing it was "
    "measured at 28 flagged vs 26 true: 3 false positives (a real tenant with "
    "neither a quota nor a budget row) and 1 miss (billing holds a quota or "
    "budget row for a tenant the licences registry does not have). Quote it as "
    "what it is. "
    "GAUGE for the same reason as billing_license_unattributed_events: a "
    "standing property of the stored window that must be able to FALL when an "
    "emitter is fixed.",
)
license_tenant_unregistered_spend_usd = Gauge(
    "billing_license_tenant_unregistered_spend_usd",
    "cost_usd carried by the unregistered-tenant events above, over the same "
    "window. The money booked to an identity billing cannot account for.",
)
attribution_rate = Gauge(
    "billing_attribution_rate",
    "Share of the billable cost events in the audit window that carry a "
    "license_id at all, 0.0-1.0 (0.25 on prod 2026-08-27). Answers 'can this "
    "spend reach an invoice line', NOT 'is the licence the right one' — "
    "coherence of the licence that IS present has its own gauges above. "
    "GAUGE for the same reason as the count above: a fixed emitter must be "
    "able to drive this UP, which a counter cannot express. "
    "NEVER WRITTEN ON AN EMPTY WINDOW: 0.0 would read as 'not one event could "
    "be attributed' — total breakage — on a quiet night whose true answer is "
    "'there was nothing to attribute', and would fire every alert written "
    "against this gauge. ⚠️ That is an abstention by the AUDIT, not an absent "
    "series: prometheus_client exports an unlabelled gauge as 0.0 from process "
    "start, so a replica that has not yet completed a pass DOES scrape 0.0 "
    "here. Alert on this only together with a recent "
    "billing_attribution_audit_last_success_timestamp_seconds — which is "
    "likewise 0 on such a replica — exactly as the coherence gauges above are "
    "already selected. See services.attribution_audit.",
)
budgets_over_limit = Gauge(
    "billing_budgets_over_limit",
    "PER-ROW, NOT per tenant: active token budget ROWS whose current-period "
    "consumption exceeds that ROW's own budget_tokens. "
    "⛔ This is NOT a count of tenants over their cap and must not be read as "
    "one. debit_budget walks a tenant's budgets taking min(room, wants) and "
    "dumps the remainder on the LAST row, so a filled non-last row sits at "
    "exactly its own cap BY DESIGN. For the enforceable question use "
    "billing_budget_tenants_over_limit. Still useful on its own terms: it "
    "detects one row mis-seeded above its cap, which the aggregate hides when "
    "the tenant has headroom elsewhere.",
)
budget_max_utilisation_ratio = Gauge(
    "billing_budget_max_utilisation_ratio",
    "PER-ROW, NOT per tenant: highest current-period consumed/limit ratio over "
    "individual budget ROWS. "
    "⛔ DO NOT read this as a breach. Measured 2026-09-10 it stood at 4.77 "
    "while both tenants involved were at 57.2% and 22.5% of their AGGREGATE "
    "caps, which is what reserve_budget enforces "
    "(consumed + reserved + estimated <= sum(budget_tokens)). A per-row max of "
    "the debit cascade is an artifact of the cascade. Use "
    "billing_budget_tenant_max_utilisation_ratio for the enforceable quantity.",
)
licence_derived_spend_share = Gauge(
    "billing_licence_derived_spend_share",
    "Share of ATTRIBUTED cost_usd in the audit window whose licence billing "
    "DERIVED (cost_source carries the `+resolved` suffix from "
    "services.cost_flusher) rather than the emitter presenting one.\n\n"
    "WHY A SECOND INSTRUMENT WHEN billing_license_resolved_total EXISTS: that "
    "counter counts EVENTS, and in this estate events and dollars diverge "
    "violently -- 14% of rows has been 0.04% of dollars, and 80% of rows 0.5% of "
    "the money. An event-count share answers 'are emitters asserting?'; only a "
    "dollar share answers 'how much REVENUE rests on an inference'. Measured "
    "2026-09-10 on billing_cost_daily, the dollar share ran 42.2%-83.3% across "
    "five days while the estate reported 98.6% 'attributed' -- the two numbers "
    "are both true and only one of them is about money.\n\n"
    "⚠️ NOT a defect rate. The derivation is a deliberate safety net for emitters "
    "that present no licence; removing it would re-expose the gap, not close it. "
    "This gauge exists so the share is LEGIBLE and a rise is detectable. "
    "Published by the rollup leader only -- read it with "
    "`and on(instance) billing:attribution_audit:fresh`, like every gauge in this "
    "block.",
)
budget_tenants_over_limit = Gauge(
    "billing_budget_tenants_over_limit",
    "TENANTS whose summed current-period consumption exceeds the SUM of their "
    "active budget_tokens -- the quantity reserve_budget actually enforces, and "
    "the only one of this family that means 'over cap'. A tenant whose every "
    "active budget is 0 counts as over-limit on any consumption (0 is "
    "BLOCK-ALL, not unlimited).",
)
budget_tenant_max_utilisation_ratio = Gauge(
    "billing_budget_tenant_max_utilisation_ratio",
    "Highest summed-consumed / summed-cap ratio across tenants. 1.0 is exactly "
    "exhausted. This is the enforceable grain; measured 0.572 on 2026-09-10 "
    "when the per-row gauge beside it read 4.77.",
)
budget_licences_over_limit = Gauge(
    "billing_budget_licences_over_limit",
    "PER-LICENCE-SCOPE, and ⛔ NOT ENFORCED BY ANYTHING. Licence scopes whose "
    "summed current-period consumption exceeds the summed cap of the rows ONE "
    "debit call can drain -- that licence's own active budgets PLUS the shared "
    "tenant-level pool (license_id = the zero-UUID sentinel), which is the scope "
    "rule in services.budget_debit._select_active_budgets. The tenant-level pool "
    "counts as a scope in its own right: licence-less system/S2S spend debits it "
    "and nothing else.\n\n"
    "WHY IT IS NOT A BREACH: the only thing that can REFUSE a request is "
    "reserve_budget, and inference-router calls it with NO license_id, so "
    "admission is decided against sum(budget_tokens) for the WHOLE tenant -- "
    "billing_budget_tenants_over_limit. This gauge measures where the debits "
    "LAND afterwards, which is a different and previously unmeasured question. "
    "Non-zero means one licence's share of a pool is oversubscribed; it does NOT "
    "mean the cap stopped enforcing, and it does not mean money was lost.",
)
budget_licence_max_utilisation_ratio = Gauge(
    "billing_budget_licence_max_utilisation_ratio",
    "Highest summed-consumed / summed-cap ratio over LICENCE SCOPES (the "
    "licence's own rows + the shared tenant pool). "
    "⛔ DO NOT READ THIS AS A BREACH, and do not compare it to 1.0 as though it "
    "were. Measured 2026-09-11, each licence paired with ITS OWN tenant: 2.05 "
    "(that tenant at 14.7%), 1.94 (24.8%) and 1.45 (60.3%) of the aggregate cap "
    "reserve_budget actually enforces. Both sets of numbers are true and only "
    "the tenant one is about admission. Read this gauge beside "
    "billing_budget_tenant_max_utilisation_ratio (the enforced grain) and "
    "billing_budget_max_utilisation_ratio (per ROW, an artifact of the debit "
    "cascade); alert on the tenant one.",
)

# --------------------------------------------------------------------------
# Which of the coherence gauges above may be believed.
#
# Prometheus scrapes EVERY replica, but only the rollup leader writes those
# gauges. A non-leader does NOT export a default 0 -- prometheus_client gauges
# are process-lifetime state, so a replica that was leader an hour ago keeps
# exporting the values it last wrote, indefinitely. Measured on prod
# 2026-08-26: the leader read mismatch_events 4141 while the follower still
# read 4797, frozen at the moment it lost leadership 12h earlier. A dashboard
# that took max() across replicas therefore plotted the STALE value -- the
# exact failure the panels were added to prevent.
#
# The discriminator has to be FRESHNESS, not leadership. `is_leader()` fails
# closed on a connection error and a wedged loop keeps reporting 1 forever, so
# a leader flag can only ever bless a stale series. A timestamp cannot: it is
# written by the audit itself, so it stops advancing exactly when the
# measurement stops being true.
truncate_guard_relations = Gauge(
    "billing_truncate_guard_relations",
    "Ledger relations currently carrying the BEFORE TRUNCATE guard trigger "
    "`billing_truncate_guard_trg`. Measured 2026-09-10 on prod: present on "
    "billing_cost_events, billing_metering_events and every cost-events "
    "partition. THE ALERT IS A FALL, NOT A THRESHOLD -- the absolute count grows "
    "by one each month as `create_cost_events_partition_for` attaches the guard "
    "to a new partition, so any fixed floor either goes stale or was never "
    "tight. A DECREASE means a migration or a hand-edit detached the only control "
    "standing between a wrong DSN and an empty cost ledger, which is how "
    "2026-09-05 happened. 0 means the guard is gone entirely.",
)

delete_audit_rows = Gauge(
    "billing_delete_audit_rows",
    "Rows in `billing_delete_audit` by operation. `TRUNCATE` > 0 means a truncate "
    "of the cost ledger was ALLOWED -- the BEFORE TRUNCATE guard refuses unless a "
    "session deliberately sets `billing.allow_truncate`, so a row here is either "
    "the 2026-09-05 incident (ids 1-14, billing_user from the shared node egress, this "
    "repo's own conftest isolation step pointed at prod) or a later deliberate "
    "one. ⚠️ ALERT ON AN INCREASE, NOT ON > 0: the 14 historical rows pin it "
    "above zero permanently, so a `> 0` rule fires forever and gets silenced, "
    "which is how a kept exception turns into a disabled detector. The DELETE "
    "label carries no incident meaning and is published as liveness -- it is the "
    "evidence that the audit trigger is still writing at all, since a deployment "
    "with the audit trigger dropped reads zero TRUNCATEs and looks identical to a "
    "healthy ledger.",
    ["operation"],  # TRUNCATE | DELETE (pg tg_op values; bounded by postgres)
)

cost_events_oldest_age_seconds = Gauge(
    "billing_cost_events_oldest_age_seconds",
    "Seconds between now and the OLDEST `created_at` in billing_cost_events — how "
    "far back the cost ledger actually reaches. Published by the rollup leader on "
    "every tick (services.rollup_engine.cost_events_oldest_age_seconds, called "
    "from services.aggregator.run_rollup_loop). "
    "THE GAP THIS CLOSES: on 2026-09-05T07:01:56Z this table was TRUNCATEd and "
    "NOTHING NOTICED FOR SIX DAYS. Every instrument the service had was about "
    "FLOW — events in, events flushed, rollup cycles, lag of the newest bucket — "
    "and flow was perfectly healthy the whole time, because ingest resumed within "
    "the minute. Not one series described the ledger's DEPTH, so an empty history "
    "and a full one were the same green dashboard. billing_truncate_guard_relations "
    "and billing_delete_audit_rows beside it watch the GUARD and its audit trail; "
    "this watches the CONSEQUENCE, which is the thing an operator actually cares "
    "about and the only one that also catches a loss no trigger mediated "
    "(a dropped partition, a wrong-DSN DELETE, a restored-from-empty volume). "
    "HEALTHY IS LARGE: with settings.retention_days at 90 a steady estate sits near "
    "90 days (~7.8e6 s) and drifts up and down by one partition's width as "
    "drop_old_cost_partitions runs. ⛔ THE ALERT IS A COLLAPSE, NOT A THRESHOLD: "
    "watch for a large DROP (`delta` over a few hours), and floor it (e.g. "
    "`< 86400` once the estate is older than a day) to catch an outright emptying. "
    "A young estate legitimately reads small, so a fixed floor alone is wrong until "
    "the ledger is older than the floor. "
    "⚠️ READ IT WITH A FRESHNESS SELECTOR. prometheus_client keeps exporting the "
    "last value set for the life of the process and only the LEADER writes this, so "
    "an ex-leader publishes a frozen reading forever. Pair it with a recent "
    "billing_metering_hourly_lag_timestamp_seconds — written by the same loop on "
    "the same leader tick — exactly as billing_metering_hourly_lag_hours instructs. "
    "⛔ THE TWO NO-DATA CASES ARE DELIBERATELY OPPOSITE, AND THIS IS THE DESIGN "
    "DECISION TO UNDERSTAND BEFORE CHANGING ANYTHING: "
    "an EMPTY table publishes 0, because here emptiness IS the incident and must be "
    "able to fire an alert; a replica that has NEVER MEASURED (or whose query "
    "raised) publishes NaN, which fails every comparison and therefore cannot "
    "false-alarm. That is the INVERSE of "
    "billing_cost_dead_letter_oldest_age_seconds, where an empty stream is healthy "
    "and is therefore the NaN case — and the inversion is the whole point: the "
    "honest no-reading value depends on whether absence of rows is the good state "
    "or the catastrophe. Set to NaN at import so 'never measured' can never present "
    "as a measured 0; the writer never resets it to NaN once a real sample exists, "
    "because a transient query failure must not erase the last true depth.",
)
# NaN at import — see the last paragraph above. A 0 here would mean "the ledger is
# empty", which is the loudest thing this gauge can say, and saying it on every
# fresh pod (and on every follower, forever) would make the one reading that
# matters indistinguishable from a pod that has not looked yet.
cost_events_oldest_age_seconds.set(float("nan"))

audit_last_success_timestamp_seconds = Gauge(
    "billing_attribution_audit_last_success_timestamp_seconds",
    "Unix time of the last attribution audit that completed and published "
    "every coherence gauge above. THE SELECTOR: a replica's coherence gauges "
    "may only be believed while this timestamp is recent. Written last and only "
    "on success, so a partial pass can never present itself as fresh.",
)
# Did this replica start with its background tasks at all?
#
# `main.lifespan` gates the WHOLE background block on `if redis_conn:`
# (main.py:169) and otherwise logs `redis_unavailable_background_tasks_skipped`
# and moves on. `init_redis` is called exactly once, so a pod that BOOTS while
# Redis is down runs no flush loop, no rollup, no quota resync and no budget
# watcher for its entire life. The Redis supervisor in `redis_client` repairs
# the client underneath a loop that already exists; it cannot un-skip a task
# that was never created.
#
# That condition used to be observable only as a single WARN line at startup —
# a log nobody alerts on, that a LOG_LEVEL change deletes outright. A log line
# is not a control. This gauge is, and it is per-pod: prometheus_client exports
# it from the scraped process, so two replicas in different states are two
# series distinguished by the scrape target, with no pod label of our own to go
# stale on a dead pod.
#
# ⚠️ INITIALISED TO 1, NOT 0, AND THAT IS THE POINT. An unlabelled Gauge
# exports its value from creation, so it can never be ABSENT the way a labelled
# Counter can — but a 0 default would trade one indistinguishability for a
# worse one: a process that died, hung, or was killed before `lifespan` reached
# the branch would export a confident "0, tasks running". Starting at 1 means
# the series reads "not started" until something has actually started them, so
# the only way to publish an all-clear is to run the code that earns it.
# `lifespan` then sets it explicitly on BOTH paths.
background_tasks_skipped = Gauge(
    "billing_background_tasks_skipped",
    "1 on a replica that started WITHOUT its background tasks (flush, rollup, "
    "quota resync, budget watcher) because Redis was down at boot; 0 once they "
    "have been created. Set explicitly on both paths in lifespan, and 1 before "
    "lifespan runs so 'never got there' cannot present as healthy. Such a pod "
    "still serves HTTP and is deliberately kept Ready (see main.readiness), so "
    "this gauge — not readiness — is what says its background work is missing. "
    "Alert on max_over_time(...) == 1: it never self-heals without a restart.",
)
background_tasks_skipped.set(1)

rollup_is_leader = Gauge(
    "billing_rollup_is_leader",
    "1 on the replica that holds the rollup lease, 0 otherwise, refreshed every "
    "tick by every replica. DIAGNOSTIC ONLY -- never use it to select which "
    "replica's gauges to trust (a wedged loop reports 1 forever). Its jobs are "
    "to alert on sum()==0 (nobody rolling up) and sum()>1 (split brain).",
)

# Budget debit volume from the cost flusher
budget_debits_total = Counter(
    "billing_budget_debits_total",
    "TokenBudget rows debited per cost event flush.",
    ["outcome"],  # ok | oversold | no_budget
)

# Reservation settlements that released MORE than the tenant's whole in-flight
# pool held. The pool is one anonymous per-tenant integer, so the excess is
# taken from OTHER callers' reservations and the next pre-flight check sees too
# small a `reserved` — i.e. the spend cap fails OPEN by exactly that overshoot.
# It cannot be attributed away without giving reservations an identity (a
# license-authz protocol change), so it is measured here instead of hidden
# behind the clamp. See services/budget_redis.py::_RELEASE_LUA. Label-less, so
# the series is born at zero on import: a flat 0 is the healthy reading, and
# absence would be indistinguishable from it.
budget_reserve_over_release_total = Counter(
    "billing_budget_reserve_over_release_total",
    "Budget settlements that released past the tenant's entire in-flight reservation pool.",
)

# The BENIGN half of what the counter above used to count on its own.
#
# A release against a pool that was ABSENT or already at zero — the key never
# existed, its TTL lapsed, or (overwhelmingly) the spend simply never reserved,
# because `reserve_budget` has exactly one caller (the explicit
# POST /budget/reserve) while `release_reservation` runs on EVERY debit. The
# release takes nothing from anyone: there was nothing pooled to take, and the
# next pre-flight check correctly sees `reserved=0`. No cap fails open here.
#
# It is split out under its own NAME rather than as a label on the counter
# above because the live alert is
# `sum by (instance) (increase(billing_budget_reserve_over_release_total[1h])) > 0`,
# which sums ACROSS labels — a labelled split would still fire on this benign
# class and fix nothing. A separate series lets the existing critical rule
# become correct with no change to the alerting repo at all.
#
# EXPECT THIS TO BE NON-ZERO AND BUSY. It ran at ~90% of all debits when it was
# measured, which is why it drowned the real signal. Alert on it only if you
# want to know that the debit path stopped settling at all; a step to flat zero
# is the interesting direction, not growth. Label-less, so the series is born
# at zero on import and absence is distinguishable from a measured zero.
budget_release_without_reserve_total = Counter(
    "billing_budget_release_without_reserve_total",
    "Budget releases against an absent or already-empty reservation pool "
    "(nothing was taken from other callers; expected on the debit path, "
    "which does not reserve).",
)

# The three series that make reservation identity observable. Together they are
# the budget of handles: `minted` is the only producer, and `settled` + `expired`
# are the only two ways a handle leaves. A persistent gap between minted and
# (settled + expired) means handles are being stranded somewhere other than the
# sweep, which is the one shape that can hold headroom forever.
#
# All three are LABEL-LESS, so each series is born at zero on import: a flat 0 is
# a readable measurement, where an absent series is indistinguishable from a
# healthy one. Read `settled` against `minted` as the producer-coverage ratio —
# it is NOT what makes BUDGET_REQUIRE_RESERVATION_HANDLE safe to arm (the
# per-reservation TTL is), it only says how much headroom is being returned
# early rather than waited out.
budget_reservation_minted_total = Counter(
    "billing_budget_reservation_minted_total",
    "Budget reservations minted with a per-reservation handle.",
)

budget_reservation_settled_total = Counter(
    "billing_budget_reservation_settled_total",
    "Reservations released by naming their own handle.",
)

budget_reservation_expired_total = Counter(
    "billing_budget_reservation_expired_total",
    "Reservations reclaimed by the expiry sweep because no settlement ever named them.",
)

# ─── The one term #178's escalation condition needs, and cannot currently read ──
#
# ⭐ WHY THIS EXISTS, and why it is a histogram rather than a split counter.
# `billing_budget_reserve_over_release_total` establishes exactly one thing: a
# settlement released more than the tenant's pool held while that pool was
# NON-ZERO. It does NOT establish that the non-empty pool contained any OTHER
# caller's reservation — and that second step is the whole of the "budget cap
# failed open" reading, which was tested against the live series on 2026-09-12 and
# did not survive. Two independent measurements over different windows agree:
# `prior_pool` concentrates on exactly 350 and 805, median `prior_pool / released`
# = 0.91-0.92, and `prior_pool` sits within 10% of the settling request's own
# `released` in ~57% of events. That is the settling request's OWN pre-flight
# estimate coming in ~8-9% under actual cost. At the enforcing grain the impact is
# nil: `budget_tenants_over_limit` = 0 and `budget_redis_op_failed_total` = 0 for
# every op.
#
# That alert names ONE falsifiable escalation condition — a sustained run with
# `prior_pool / released < 0.5`, the only population where a drained peer hold is
# arithmetically possible — and then says it is **unfirable**, because NEITHER
# `prior_pool` NOR `released` is exported as a series: both exist only inside the
# structlog event, so the condition can be reached only by grepping logs by hand.
# This exports the ratio and nothing else, which makes the documented condition
# expressible in PromQL for the first time.
#
# ⛔ NOT a second writer on `budget_reserve_over_release_total`. That counter has
# exactly one writer by design, the alert depends on its meaning being unchanged,
# and its own comment forbids a second. This records an ADDITIONAL property of the
# same event.
#
# ⚠️ WHAT THIS DELIBERATELY DOES *NOT* DO: it does not say whether a PEER's
# reservation was live. That question is unanswerable in the current mode by
# construction — the pooled protocol keeps one anonymous integer per tenant and no
# per-reservation state, so there is nothing to interrogate. Interrogating it needs
# `budget_require_reservation_handle` armed, and arming it is a money-path change
# that strands the untagged pool for up to one TTL window (see budget_redis.py's
# key-tagging note). A `peer_held` label computed against the unarmed protocol
# would read "no peer" for every event and would be a FALSE REASSURANCE rather
# than a measurement — which is worse than the gap it appears to close.
# ─── Unbillable spend: tenant ids no registry vouches for ─────────────────────
#
# ⛔ THESE ARE NOT A DUPLICATE OF `billing_tenant_unregistered_total`, and the
# difference is the entire reason they exist. That counter is per-POD-lifetime and
# is incremented at RESOLUTION time by `tenancy._note_unregistered_tenant`.
# Measured 2026-09-12: both replicas read 0 while the LEDGER carried $22.89 over
# 903 rows on three such tenant ids, because those ids last emitted 09-09 17:4xZ
# and the pods started 09-12 07:42Z. The zero was correct and useless — it means
# "not since the last deploy", which is indistinguishable from "never happens".
# A pod restart resets the only instrument that could report a standing condition.
# These gauges are read from the durable ledger instead
# (services.unregistered_tenant_spend), so a restart cannot erase the finding.
#
# ⚠️ There was also no ALERT anywhere in gitops on the existing counter (verified
# across all three prometheus rules ConfigMaps, 2026-09-12). A metric that cannot
# answer and a metric nobody reads are different failures; this had both.
#
# ⚠️ AGGREGATE WITH max(), NEVER sum(): leader-only publisher, and a follower
# publishes NOTHING rather than a zero, so sum() is a half-reading and avg()
# halves the money.
#
# ⛔ NO TENANT LABEL, deliberately. The offending id is attacker-influenced input
# (any caller can assert a tenant header), so a per-tenant label is an unbounded
# cardinality bomb keyed on untrusted data. The ids are logged, redacted, where
# they are searchable without becoming series.
unregistered_tenant_spend_usd = Gauge(
    "billing_unregistered_tenant_spend_usd",
    "USD of cost events in the last 30 days whose tenant_id the licences registry "
    "does not vouch for — i.e. money that can be invoiced to nobody. "
    "⚠️ Includes rows already parked on `excluded_from_billing`, deliberately: the "
    "flusher parks them BECAUSE they are unregistered, so filtering those out "
    "would hide the population as the parking mechanism worked. "
    "⛔ A rise is a signal to fix the EMITTER or register the tenant, never to "
    "start rejecting at the resolution boundary — that drops the money record as "
    "well as the bill.",
)

unregistered_tenant_count = Gauge(
    "billing_unregistered_tenant_count",
    "Distinct tenant ids with spend in the last 30 days that the licences registry "
    "does not vouch for. 3 when first measured (2026-09-12). Read alongside "
    "`billing_unregistered_tenant_spend_usd` — a count without its money says "
    "nothing about exposure, and 80% of rows was once 0.5% of the money.",
)

# ---------------------------------------------------------------------------
# The same population as the two gauges above, but NAMED.
#
# ⛔ WHY THESE EXIST WHEN THE AGGREGATES ALREADY DO: neither gauge above carries
# a tenant label, and the log line that does carry one runs every id through
# `tenancy.redact_tenant_id`, which truncates anything over 24 chars — a uuid is
# 36, so the log renders `ad976f5b…(redacted,len=36)` and the full id appears
# NOWHERE. Measured live 2026-09-13 on the leader replica. An operator paged by
# `billing_unregistered_tenant_count > 0` therefore learns that four tenants are
# unbillable and cannot learn WHICH, and an alert that cannot name its subject
# has already been filed as a defect in this estate (mlx_ingestion#54).
#
# ⚠️ FULL DISCLOSURE IS SAFE HERE, and that is a property of the SOURCE, not a
# relaxation of the redaction rule. `redact_tenant_id` guards the INGRESS
# boundary, where `tenant_id` may still be a raw header and therefore a bearer
# token. These series are read from `billing_cost_events.tenant_id`, a Postgres
# `uuid` column (alembic 0011) — the value has already survived a uuid cast, so
# it is a uuid and cannot be a credential. `tenant_id` is an established label
# in this file already (see karma_subject_charge_excluded_total).
# ---------------------------------------------------------------------------

unregistered_spend_by_tenant_usd = Gauge(
    "billing_unregistered_spend_by_tenant_usd",
    "USD of cost events in the last 30 days, PER TENANT ID, for tenant ids the "
    "licences registry does not vouch for. The named breakdown of "
    "`billing_unregistered_tenant_spend_usd`; the unlabelled gauge stays the "
    "authoritative TOTAL, because this one is capped (see the overflow bucket "
    "below) and their sum can differ. "
    "⚠️ Capped at the top "
    "`services.unregistered_tenant_spend.MAX_NAMED_TENANT_SERIES` ids BY SPEND, "
    'with the residual folded into a single `tenant_id="__overflow__"` series '
    "so the money is never lost from this view even when the names are. "
    "⛔ Aggregate with max(), never sum(): only the rollup LEADER publishes, and "
    "summing across replicas double-counts the money. "
    "⛔ A rise is a signal to fix the EMITTER or register the tenant, never to "
    "start rejecting at the resolution boundary — that drops the money record as "
    "well as the bill.",
    ["tenant_id"],
)

unregistered_events_by_tenant = Gauge(
    "billing_unregistered_events_by_tenant",
    "Cost EVENTS in the last 30 days, PER TENANT ID, for tenant ids the licences "
    "registry does not vouch for. Same population, cap and `__overflow__` bucket "
    "as `billing_unregistered_spend_by_tenant_usd`, and published in the same "
    "pass — read the two together. A row count alone is not an exposure (80% of "
    "rows was once 0.5% of the money) and money alone hides a high-volume emitter "
    "that is cheap today and will not be after a model change.",
    ["tenant_id"],
)

unregistered_tenant_series_dropped_total = Counter(
    "billing_unregistered_tenant_series_dropped_total",
    "Distinct unregistered tenant ids that existed in the ledger but were NOT "
    "given their own series this pass, because the cap was reached. Their money "
    "and events are still visible in the `__overflow__` bucket and in the "
    "unlabelled total; only their NAMES were dropped. "
    "⛔ Non-zero means an emitter is minting tenant uuids, which is an upstream "
    "defect — fix the emitter. Do NOT raise the cap to make this quiet: the cap "
    "is the only thing standing between a caller-supplied uuid and an unbounded "
    "label domain.",
)

# ---------------------------------------------------------------------------
# Archived-tenant spend, LEDGER-DERIVED — see services.archived_tenant_spend.
#
# ⛔ NOT A BARE PER-PROCESS COUNTER, DELIBERATELY. This estate already has one
# of those for the neighbouring problem
# (services.unregistered_tenant_spend's own docstring measures it: both
# replicas read 0 while the ledger carried $22.89 over 903 rows, because the
# offending ids had gone quiet 3 days before the pods last restarted). A
# restart resets a Counter to 0, which reads identically to "never happened".
# These four gauges instead READ billing_cost_events.billed_while_archived —
# a durable, ledger-persisted fact — so they report a STANDING condition, not
# a since-last-restart trend. cost_events_billed_while_archived_total above
# is the trend signal; these are the proof.
#
# tenant_id here is a REGISTERED tenant the licences registry has actually
# vouched for as archived — a materially smaller and licences-bounded set
# than unregistered_tenant_spend's caller-supplied, unbounded uuid domain —
# but it is still capped the same way, on the same principle: nothing here
# should assume the bound holds forever just because it holds today.
# ---------------------------------------------------------------------------

archived_tenant_spend_usd = Gauge(
    "billing_archived_tenant_spend_usd",
    "USD of cost events in the last 30 days flagged `billed_while_archived` — "
    "spend billing recorded for a tenant the licences registry said was "
    "archived AT FLUSH TIME. Not a refusal count: every one of these rows was "
    "billed normally: `excluded_from_billing` is independent of this flag. "
    "⛔ Aggregate with max(), never sum(): only the rollup LEADER publishes, "
    "and summing across replicas double-counts the money. "
    "A rise here is a collections/attribution question, not a bug: the row is "
    "correct money for compute that already happened on an account that is no "
    "longer active — see the four PRE-SPEND archival gates (licences licence- "
    "and token-mint, routers/budget.py's reservation gate) for where to close "
    "the loop upstream instead.",
)

archived_tenant_count = Gauge(
    "billing_archived_tenant_count",
    "Distinct tenant ids with spend in the last 30 days flagged "
    "`billed_while_archived`. Read alongside "
    "`billing_archived_tenant_spend_usd` — a count without its money says "
    "nothing about exposure, same caveat as its unregistered-tenant sibling.",
)

archived_spend_by_tenant_usd = Gauge(
    "billing_archived_spend_by_tenant_usd",
    "USD of cost events in the last 30 days, PER TENANT ID, for tenant ids "
    "flagged `billed_while_archived`. The named breakdown of "
    "`billing_archived_tenant_spend_usd`; the unlabelled gauge stays the "
    "authoritative TOTAL because this one is capped, mirroring "
    "`billing_unregistered_spend_by_tenant_usd`. "
    "⚠️ Capped at the top "
    "`services.archived_tenant_spend.MAX_NAMED_TENANT_SERIES` ids BY SPEND, "
    'with the residual folded into a single `tenant_id="__overflow__"` series.',
    ["tenant_id"],
)

archived_events_by_tenant = Gauge(
    "billing_archived_events_by_tenant",
    "Cost EVENTS in the last 30 days, PER TENANT ID, for tenant ids flagged "
    "`billed_while_archived`. Same population, cap and `__overflow__` bucket "
    "as `billing_archived_spend_by_tenant_usd`, published in the same pass.",
    ["tenant_id"],
)

archived_tenant_series_dropped_total = Counter(
    "billing_archived_tenant_series_dropped_total",
    "Distinct archived tenant ids that existed in the ledger but were NOT "
    "given their own series this pass, because the cap was reached. Their "
    "money and events are still visible in the `__overflow__` bucket and in "
    "the unlabelled total; only their NAMES were dropped.",
)

budget_reserve_estimate_ratio = Histogram(
    "billing_budget_reserve_estimate_ratio",
    "prior_pool / released on an over-release — how much of the settled amount the "
    "reserve-time estimate had actually covered. 1.0 = exact; the measured "
    "population sits at ~0.91 median, i.e. the estimate runs ~9% under actual. "
    "⭐ Mass moving BELOW 0.5 is the documented escalation condition for "
    "`BillingBudgetReserveOverRelease` (3.8-8.1% of events at baseline) — that is "
    "the population where the overshoot is too large to be the settling request's "
    "own estimate. ⛔ Strictly < 1.0 by construction: this is observed only where "
    "prior_pool > 0 and overshoot = released - prior_pool. "
    "⚠️ It measures estimator calibration, NOT whether a peer's hold was drained; "
    "the pooled protocol has no per-reservation state to answer that.",
    buckets=(0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0),
)

# Rollup-loop cycle outcomes (leader-elected; followers do not count).
# Without this the rollup loop was the one background loop whose failures were
# INVISIBLE: the broad `except Exception` keeps it alive, so a permanently
# poisoned leader produced no rollups, no retention cleanup, no cost-partition
# creation — and no metric, no crash, no un-Ready pod. Labelled so an alert can
# read the ERROR RATE (error / (ok+error)) rather than an absolute count.
rollup_cycles_total = Counter(
    "billing_rollup_cycles_total",
    "Leader rollup-loop cycles by outcome.",
    ["status"],  # ok | error
)
rollup_leadership_handovers_total = Counter(
    "billing_rollup_leadership_handovers_total",
    (
        "Times the rollup leader released the advisory lock after "
        "MAX_CONSECUTIVE_ROLLUP_FAILURES consecutive failed cycles, so another "
        "replica can take over."
    ),
)

# Buffer-to-flush latency
flush_lag_seconds = Histogram(
    "billing_flush_lag_seconds",
    "Wallclock seconds between event arrival and successful flush.",
    buckets=(0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0),
)


# ---------------------------------------------------------------------------
# Tenant-name resolution: the live licences registry vs the checked-in snapshot
# ---------------------------------------------------------------------------

#: The four ways a NAME (not a uuid) can leave ``tenancy.canonical_tenant_id``.
#: Bounded and closed — every branch of the resolver increments exactly one.
#: ``ambiguous`` = the live registry holds the name on MORE THAN ONE tenant
#: (licences names are unique per owning app, not globally), so no id was
#: returned; see billing_tenant_name_ambiguous_total for which name.
TENANT_NAME_RESOLUTION_SOURCES: tuple[str, ...] = ("live", "snapshot", "unknown", "ambiguous")

tenant_name_resolution_total = Counter(
    "billing_tenant_name_resolution_total",
    "Tenant identifiers that arrived as a NAME rather than a uuid, labelled by "
    "what answered. `live` = the live licences registry "
    "(GET /internal/tenants, refreshed on a loop). `snapshot` = the live map "
    "did not hold the name and the checked-in "
    "tenancy.LEGACY_TENANT_NAMES snapshot did — the FAIL-SOFT path, which is "
    "the whole of the behaviour that existed before the live overlay shipped. "
    "`unknown` = neither held it, so canonical_tenant_id returned None and the "
    "caller took a 400 or attributed the row to the nil tenant. "
    "WHY IT EXISTS: the snapshot is hand-regenerated from a psql query, and "
    "between 2026-08-25 and 2026-09-05 it drifted by eight entries in BOTH "
    "directions at once while looking perfectly healthy. Nothing measured that. "
    "A steady `snapshot` rate now means the live registry is unreachable (or "
    "the name is a retired spelling only the snapshot holds); a steady "
    "`unknown` rate names real callers being rejected. "
    "NOT a superset of billing_tenant_unregistered_total: that one fires on the "
    "UUID path, which never consults either map. Disjoint populations — a "
    "single input increments exactly one of the two families, never both.",
    ["source"],
)
# Born at zero: `snapshot` is the pre-existing behaviour and MUST be visible
# from the first scrape, or "the live registry is answering everything" and
# "the overlay was never wired up" render identically as an absent series.
for _source in TENANT_NAME_RESOLUTION_SOURCES:
    tenant_name_resolution_total.labels(source=_source)

tenant_name_ambiguous_total = Counter(
    "billing_tenant_name_ambiguous_total",
    "Tenant identifiers that arrived as a NAME which the live licences registry "
    "holds on MORE THAN ONE tenant. Since the licences tenant-ownership change, "
    "tenant names are unique per OWNING APP (commonpractice, fr, platform, ...) "
    "rather than globally, so two apps may each own a tenant called `netcare`. "
    "A bare name then cannot say which one is meant, and resolution returns NO "
    "id rather than picking one — picking would land one customer's spend on "
    "another customer's ledger. The caller takes a 400 (HTTP) or the nil "
    "tenant (stream), and must move to sending the tenant UUID. "
    "Labelled by `name`: bounded, because only names the live registry itself "
    "lists on two or more tenants can ever reach this counter — an arbitrary "
    "caller-supplied string cannot. A non-zero series names the caller to fix. "
    "Per-pod-lifetime: a restart resets it.",
    ["name"],
)

#: What vouched for a tenant UUID on the registration check. Bounded, four
#: values, and the split between the two `snapshot*` ones is the whole point:
#: "the snapshot happens to be the only map holding a retired spelling" and
#: "the live registry has not answered since this pod booted" are different
#: facts that were previously the same silence.
TENANT_REGISTRATION_SOURCES: tuple[str, ...] = (
    "live",
    "snapshot",
    "snapshot_degraded",
    "unknown",
)

tenant_registration_source_total = Counter(
    "billing_tenant_registration_source_total",
    "Registration checks (tenancy.is_registered_tenant_id — the check the "
    "cost flusher's unregistered-tenant gate is built on) labelled by WHAT "
    "ANSWERED. `live` = the id is in the live licences-registry overlay "
    "(GET /internal/tenants, refreshed on a loop and on demand). `snapshot` = "
    "only the checked-in tenancy._REGISTERED_TENANT_IDS snapshot vouches for "
    "it while the overlay is FRESH — genuine drift, or one of the retired "
    "spellings the snapshot deliberately keeps. `snapshot_degraded` = only the "
    "snapshot vouches for it AND the overlay is stale or was never loaded — "
    "THE DEGRADATION, i.e. billing is answering out of a hand-pasted file. "
    "`unknown` = nothing vouches for it; this is the population the gate would "
    "park when armed. "
    "⭐ WHY IT EXISTS: docs/UNREGISTERED_TENANT_GATE.md refuses to arm "
    "BILLING_EXCLUDE_UNREGISTERED_TENANTS until billing consults the registry "
    "live. Code that does so is not evidence that it IS doing so — a token "
    "misconfiguration degrades to the snapshot in total silence, and the "
    "service keeps working, which is exactly why nobody would notice. A "
    "sustained `live` rate is the proof; a sustained `snapshot_degraded` rate "
    "says precondition 2 is NOT met today, whatever the code says. "
    "NOT interchangeable with billing_tenant_name_resolution_total: that one "
    "fires when a tenant arrives as a NAME, this one when the resolved UUID is "
    "checked against the registry. Disjoint questions, and a name resolution "
    "normally produces a registration check as well. "
    "Deliberately UNLABELLED by tenant: the id is caller-controlled "
    "X-Tenant-Id input, so a per-id label is unbounded cardinality fed from the "
    "edge — the same reason billing_tenant_unregistered_total carries none.",
    ["source"],
)
# Born at zero for all four. `snapshot_degraded` and `unknown` MUST be visible
# from the first scrape or an alert on them cannot distinguish "never happened"
# from "the instrument is not wired", which is the failure this counter exists
# to make impossible.
for _source in TENANT_REGISTRATION_SOURCES:
    tenant_registration_source_total.labels(source=_source)

# ---------------------------------------------------------------------------
# R15 estate repair — tenancy.classify_tenant_id write-path guard (observe
# mode by default; see settings.Settings.billing_tenant_id_classification_
# enforce). Distinct from TENANT_REGISTRATION_SOURCES above: that answers
# "which registry answered" (live / snapshot / snapshot_degraded / unknown /
# sentinel); this answers "of the ids no registry vouches for, what KIND of
# id is it" (a leftover test fixture, a name-derived v5 with no current owner,
# a stray v7/v4, or something this classifier cannot even version-tag) — see
# tenancy.classify_tenant_id's own docstring for the full precedence order.
# ---------------------------------------------------------------------------

tenant_id_classification_total = Counter(
    "billing_tenant_id_classification_total",
    "Every canonical_tenant_id resolution of a well-formed uuid, labelled by "
    "tenancy.classify_tenant_id's verdict: 'registered' (a real, vouched-for "
    "tenant), 'synthetic' (an exact match in tenancy.SYNTHETIC_TENANT_IDS — "
    "known test/fixture values found in real tables by the 2026-09-16 estate "
    "audit), 'v5_derived' (RFC4122 v5, the shape a legacy-name derivation "
    "produces, but for no name any registry currently owns), 'v7' or 'v4' "
    "(time-ordered / random uuids nothing vouches for), or 'other' (parses as "
    "a uuid but is none of the above -- a different RFC4122 version, or a "
    "shape this classifier could not read). "
    "⭐ THIS IS AN OBSERVE-MODE COUNTER: a non-'registered' label here does NOT "
    "mean the id was refused -- resolution still returns it unless "
    "billing_tenant_id_classification_enforce is armed (default False), in "
    "which case the same event ALSO increments "
    "billing_tenant_id_classification_rejected_total. Compare the two: this "
    "one is the population that WOULD be affected by arming; the rejected one "
    "is the population that WAS.",
    ["classification"],
)
# Pre-seeded for every member of TENANT_ID_CLASSIFICATIONS, imported from
# tenancy rather than restated, so this cannot drift from the classifier that
# emits it. Same series-birth hazard as tenant_registration_source_total
# above: an unlabelled Counter exports nothing until its first .labels().inc(),
# so "never happened" and "the instrument is not wired" must not read the same.
for _tid_classification in TENANT_ID_CLASSIFICATIONS:
    tenant_id_classification_total.labels(classification=_tid_classification)

tenant_id_classification_rejected_total = Counter(
    "billing_tenant_id_classification_rejected_total",
    "Subset of billing_tenant_id_classification_total that "
    "canonical_tenant_id actually REJECTED (returned None instead of the "
    "id) because billing_tenant_id_classification_enforce was armed at the "
    "moment of the call. Labelled by the same classification. ⛔ ARMING "
    "CRITERION: this counter is the shadow of what arming the gate would cost "
    "-- it increments identically whether or not the gate is armed, so it is "
    "reviewable BEFORE arming. Only 'synthetic', 'v5_derived', 'v7', 'v4' and "
    "'other' can appear here; 'registered' never can, by construction "
    "(tenancy.canonical_tenant_id only evaluates the enforce branch for a "
    "non-'registered' classification).",
    ["classification"],
)
for _tid_rejectable in TENANT_ID_DIVERGENCE_CLASSES:
    tenant_id_classification_rejected_total.labels(classification=_tid_rejectable)

tenant_id_divergence_rows = Gauge(
    "billing_tenant_id_divergence_rows",
    "billing_cost_events rows (created_at within the audit window; see "
    "services.tenant_id_divergence.DEFAULT_WINDOW_HOURS) whose tenant_id "
    "classifies as anything other than 'registered', labelled by "
    "tenancy.classify_tenant_id's verdict. ⛔ ROWS ALONE ARE MISLEADING ON "
    "THIS ESTATE: measured 2026-09-16 across the full (unwindowed) divergent "
    "population, one bucket was 54% of rows and 0.2% of the money -- see "
    "billing_tenant_id_divergence_spend_usd beside it, and read the two "
    "together, never this one alone. Set UNCONDITIONALLY every leader tick, "
    "including 0 for a classification with nothing this window -- a measured "
    "zero, not a withheld one; see "
    "billing_tenant_id_divergence_audit_last_success_timestamp_seconds for "
    "whether a given reading is fresh.",
    ["classification"],
)
tenant_id_divergence_spend_usd = Gauge(
    "billing_tenant_id_divergence_spend_usd",
    "sum(cost_usd) of billing_cost_events rows (same window and same "
    "classification labelling as billing_tenant_id_divergence_rows) whose "
    "tenant_id classifies as anything other than 'registered'. The "
    "DOLLAR-DENOMINATED partner to the rows gauge -- required precisely "
    "because rows and dollars can disagree by two orders of magnitude on this "
    "estate (see that gauge's help text). Also set unconditionally every "
    "leader tick.",
    ["classification"],
)
# Pre-seeded for every DIVERGENCE class (never 'registered' -- a gauge named
# "divergence" reporting a registered tenant's spend would be a contradiction
# in terms, not a measurement).
for _tid_divergence_class in TENANT_ID_DIVERGENCE_CLASSES:
    tenant_id_divergence_rows.labels(classification=_tid_divergence_class)
    tenant_id_divergence_spend_usd.labels(classification=_tid_divergence_class)

tenant_id_divergence_audit_last_success_timestamp_seconds = Gauge(
    "billing_tenant_id_divergence_audit_last_success_timestamp_seconds",
    "unix time of the last COMPLETED "
    "services.tenant_id_divergence.run_tenant_id_divergence_audit pass on "
    "this replica. Written LAST, only on success, exactly as "
    "attribution_audit's audit_last_success_timestamp_seconds -- a follower "
    "replica or a failed pass leaves this stale rather than resetting it, so "
    "'fresh' means 'these two gauges are from the same completed pass', not "
    "'this replica claims to be leader'.",
)

tenant_registry_live_entries = Gauge(
    "billing_tenant_registry_live_entries",
    "Names in the live licences-registry overlay after its last SUCCESSFUL "
    "refresh. Zero means no refresh has ever landed and every name resolution "
    "is being served by the checked-in snapshot — the service still works, "
    "which is exactly why this needs a number rather than a log line.",
)

tenant_registry_refresh_age_seconds = Gauge(
    "billing_tenant_registry_refresh_age_seconds",
    "Seconds since the live tenant registry was last refreshed SUCCESSFULLY, "
    "or -1 when it never has been. The overlay is never expired on age — an "
    "old live map is still better than the snapshot, and expiring it would "
    "turn a licences outage into a resolution change. Age is therefore "
    "reported instead of enforced.",
)

#: How the checked-in snapshot differs from the live registry. Bounded.
#: ``absent_from_live`` is EXPECTED to be non-zero — it counts the retired
#: spellings in RENAMED_TENANT_ALIASES, which the registry has renamed away and
#: which the snapshot deliberately keeps so old callers still resolve.
TENANT_REGISTRY_DRIFT_KINDS: tuple[str, ...] = (
    "missing_from_snapshot",
    "conflicting_uuid",
    "absent_from_live",
)

tenant_registry_drift = Gauge(
    "billing_tenant_registry_drift",
    "Disagreement between the live licences registry and the checked-in "
    "tenancy.LEGACY_TENANT_NAMES snapshot, measured on every refresh and "
    "labelled by shape. `missing_from_snapshot` = registered names the snapshot "
    "does not hold (before the live overlay these took a hard 400; there were "
    "SIX on 2026-09-05). `conflicting_uuid` = a name both hold, mapped to "
    "DIFFERENT uuids — the dangerous one, because the snapshot resolved it to "
    "the WRONG tenant rather than rejecting it, so spend was attributed to "
    "somebody else. `absent_from_live` = snapshot keys the registry no longer "
    "holds; non-zero is EXPECTED and equals the declared retired spellings. "
    "This is the instrument the hand-regeneration never had: drift used to be "
    "visible only when tests/fixtures/licenses_tenants.txt was refreshed.",
    ["kind"],
)
for _kind in TENANT_REGISTRY_DRIFT_KINDS:
    tenant_registry_drift.labels(kind=_kind).set(0)

tenant_registry_refresh_total = Counter(
    "billing_tenant_registry_refresh_total",
    "Live tenant-registry refresh attempts by outcome. `ok` = a non-empty map "
    "was installed. `empty` = licences answered but with nothing usable (a 401, "
    "a transport error and a genuinely empty registry are INDISTINGUISHABLE "
    "here, because services.tenant_names.get_tenant_info returns {} for all "
    "three by design) — the previous map is KEPT and the snapshot keeps "
    "answering. `error` = the refresh itself raised, which should be "
    "unreachable. Read `empty` as the licences-reachability signal for this "
    "service: it was answering billing with 401 in prod until 2026-09-05.",
    ["result"],  # ok | empty | error
)
for _result in ("ok", "empty", "error"):
    tenant_registry_refresh_total.labels(result=_result)

tenant_registry_miss_refresh_total = Counter(
    "billing_tenant_registry_miss_refresh_total",
    "Budget-write gate misses (an id nothing vouches for, inside the fresh "
    "window) that were re-checked against licences BEFORE refusing "
    "(tenancy.write_gate_refuses_tenant_after_refresh). `found` = the re-check "
    "vouched for it and the write was ALLOWED -- each one is a 422 a tenant "
    "created since the last 300s tick would have got before 2026-10-13. "
    "`absent` = a refresh landed and the id is still unknown (refused). "
    "`rate_limited` = refused without a fetch because another miss refresh "
    "started under the floor interval. `negative_cached` = refused without a "
    "fetch because a refresh for this id found nothing within the TTL. "
    "`failed` = the refresh errored, timed out or installed nothing; the "
    "refusal stands (fail closed). `no_hook` = no refresh hook installed "
    "(tests, scripts).",
    ["result"],  # found | absent | rate_limited | negative_cached | failed | no_hook
)
for _result in ("found", "absent", "rate_limited", "negative_cached", "failed", "no_hook"):
    tenant_registry_miss_refresh_total.labels(result=_result)

service_key_unmapped_total = Counter(
    "billing_service_key_unmapped_total",
    "Metering events whose request path matched NO prefix in "
    "services.consumer.SERVICE_KEY_MAP and whose payload named no emitter, so "
    "the row was filed under an honest `unmapped:<segment>` key instead of a "
    "fabricated one. THE DEFECT THIS REPLACED: the miss branch returned the "
    "FIRST PATH SEGMENT verbatim, which invented a service called `api` and "
    "filed ~2,733 events/day of fr's traffic under it — a service_key that "
    "reads exactly like the 30-odd real ones beside it in every dashboard, "
    "alert and stored row, while naming nothing. "
    "⭐ LABELLED BY service_key SINCE 2026-09-05, and the previous HELP's "
    "reason for leaving it bare was wrong on its own terms. It claimed the "
    "segment 'already travels in the service_key label of "
    "billing_cost_events_by_service_total' — but that counter is incremented "
    "in the COST flusher, and this one counts METERING/usage events, so for an "
    "emitter that only ever produces usage rows there was no other series "
    "carrying the value at all. MEASURED: 957 increments in 24h that could not "
    "be attributed to any source service, because a bare counter answers 'how "
    "many' and never 'who'. The paired WARNING log names each offender, but a "
    "log cannot be alerted on, ranked, or read as a rate. "
    "CARDINALITY IS CLAMPED, NOT TRUSTED — the previous HELP was right that the "
    "segment is caller-controlled and its domain unbounded. The label value is "
    "the SAME `unmapped:<segment>` key the row is filed under, admitted only "
    "through services.consumer._clamp_unmapped_label: the first "
    "_UNMAPPED_LABEL_CAP distinct keys a process sees get their own series, "
    "every later one collapses into the single `unmapped:other` bucket. So a "
    "hostile caller minting a fresh path per request can add at most that many "
    "series per pod and then costs nothing, while the handful of real "
    "offenders — which is what 957/24h actually is — stay individually named.",
    ["service_key"],
)
# Birth the metric NAME at import, for exactly the reason recorded at length on
# billing_pricing_miss_total above: a labelled prometheus_client Counter exports
# the HELP/TYPE header but NO SAMPLES until its first `.labels(...)`, and
# Prometheus ingests samples, not headers. This counter was UNLABELLED until
# now, so it self-birthed and any query against it was always answerable;
# adding a label would silently take that away and turn "no unmapped events"
# and "the series does not exist" back into the same empty vector.
#
# `unmapped:other` is birthed rather than a sentinel because it is a REAL
# bucket — the overflow value the clamp returns — and the bare `unmapped` key
# is birthed too, being what extract_service_key returns when the first path
# segment is unusable. Both can genuinely occur; neither can fabricate a name.
#
# Written as a loop over a tuple rather than two direct calls ONLY to keep the
# literals off a `service_key=` keyword: detect-secrets' "Secret Keyword" rule
# fires on `*_key`-named arguments assigned a string literal, and an inline
# `# pragma: allowlist secret` is fragile here because ruff-format may reflow
# the call and move the comment off the flagged line — which is exactly how
# tests/test_service_key_is_not_fabricated.py once went red in CI while passing
# the local hook. The values are NOT imported from services.consumer, which owns
# them, because metrics is imported BY consumer and the reverse edge would be a
# cycle; tests pin the two definitions in agreement instead.
for _unmapped_birth in ("unmapped", "unmapped:other"):
    service_key_unmapped_total.labels(service_key=_unmapped_birth)

# Birth every allowlisted probe identity at import, on BOTH action values, for
# the reason recorded at length on billing_pricing_miss_total: a labelled
# Counter exports the HELP/TYPE header but NO SAMPLES until its first
# `.labels(...)`, and Prometheus ingests samples, not headers.
#
# It matters more here than anywhere else in this file, because the failure this
# counter exists to catch is a series going FLAT. Without birthing, an allowlist
# entry that has rotted — rotated service account, renamed endpoint, probe
# removed — produces NO SERIES AT ALL, and "the rule matched nothing" is then
# indistinguishable from "the rule was never deployed". Birthed at 0, the same
# rot is a visible zero-valued series with a timestamp, and
# `rate(...[1h]) == 0` becomes answerable instead of empty.
#
# The domain is HEALTH_PROBE_EMITTERS itself, not a hand-copied list beside it,
# so an entry added to the allowlist is born automatically and the two can never
# disagree about which identities exist.
for _probe_rule in HEALTH_PROBE_EMITTERS:
    for _probe_action in ("excluded", "counted"):
        cost_events_health_probe_total.labels(
            subject_id=_probe_rule.subject_id,
            tenant_id=_probe_rule.tenant_id or "",
            # `service_key=` on a string literal is what trips detect-secrets'
            # "Secret Keyword" rule elsewhere in this file; these are variables,
            # not literals, so the rule does not fire and no pragma is needed.
            service_key=_probe_rule.service_key or "",
            endpoint=_probe_rule.endpoint or "",
            action=_probe_action,
        )


# ---------------------------------------------------------------------------
# Monthly invoice-close loop (see services.invoice_closer)
# ---------------------------------------------------------------------------

invoices_closed_total = Counter(
    "billing_invoices_closed_total",
    "Billing periods the invoice-close loop finished processing this cycle, "
    "by outcome. `issued` = the period was closed by THIS sweep and drew a "
    "number from the gapless series. `unchanged` = the period already had a "
    "live invoice when the sweep ran, so services.invoices.close_period "
    "returned that invoice untouched and no number was drawn. "
    "⚠️ `unchanged` EXISTS BECAUSE THAT POPULATION USED TO BE COUNTED AS "
    "`issued`: the loop ticks hourly and close_period returns a pre-existing "
    "invoice unchanged, so a month's real closes were re-counted on every "
    "tick for the rest of the month — roughly 650 phantom `issued` per "
    "tenant per month. Splitting the label is what makes a rate or "
    "increase() over status=issued mean periods actually closed, and a "
    "reading taken before that split is inflated by that factor. "
    "`skipped` = services.invoice_closer declined the "
    "period because it is not yet past its settings.invoice_close_lag_days "
    "settle window. "
    "⚠️ `draft` IS RESERVED AND CANNOT FIRE TODAY: nothing closes a period "
    "without issuing it. The label is declared so the set does not change if "
    "issue_immediately ever becomes configurable — a zero here means the code "
    "has no such path, NOT that the path exists and was not taken. "
    "(An earlier version of this help text cited a services.invoices."
    "create_draft that has never existed; if you are reading a cached copy "
    "that names it, the function is not there.)",
    ["status"],  # issued | unchanged | skipped ; `draft` reserved, see above
)

# ⭐ PRE-SEED. Same birth hazard as billing_invoice_close_failures_total below:
# a labelled Counter exports HELP/TYPE but no SAMPLES until its first
# `.labels(...)`, so a fleet that has never closed a period exports no series
# at all. Measured 2026-09-18: `git grep "invoices_closed_total.labels" --
# src` finds this counter incremented at exactly three call sites in
# services/invoice_closer.py -- `status="skipped"` (the settle-window gate),
# `status="unchanged"` (a period already had a live invoice), and
# `status=status_label` where `status_label = invoice.status if invoice.status
# else "issued"`. gitops PR #4008's BillingInvoiceIssued alert
# (`sum(increase(billing_invoices_closed_total{status="issued"}[1h])) > 0 and
# on() (max(billing_invoice_close_enabled) == 1)`) exists to announce the
# FIRST invoice this estate ever mints -- on an unborn series `increase()` has
# nothing to evaluate, so that one event is exactly the one it cannot see.
#
# ⚠️ `draft` is deliberately NOT pre-seeded here, for the identical reason
# `tenant_registry_unavailable` is withheld from invoice_close_refused_total's
# pre-seed below: the docstring above already establishes it "CANNOT FIRE
# TODAY" -- close_due_periods' only call to close_period never passes
# `issue_immediately=False`, so `invoice.status` reaching that third call site
# is always "issued" (confirmed: `issue()` at src/services/invoices.py:2464
# sets `invoice.status = "issued"`, and that is the only path into `created`).
# Pre-creating `draft` here would be a series that can only ever read 0 --
# worse than absent, because it looks like a working detector. If
# `issue_immediately` ever becomes reachable as False from this loop, add it
# here in the same change.
for _closed_status in ("issued", "unchanged", "skipped"):
    invoices_closed_total.labels(status=_closed_status)

invoice_close_refused_total = Counter(
    "billing_invoice_close_refused_total",
    "Invoice closes refused by a named gate in services.invoices, "
    "labelled by WHICH refusal fired. Originally assert_tenant_invoiceable "
    "only (tenant_unregistered/tenant_archived); widened 2026-09-18 to cover "
    "every other gate whose raise the sweep folds into the aggregate "
    "tenant_refused bucket, on the same argument one step further: "
    "assert_no_bucket_gap (bucket_gap), assert_seat_count_available "
    "(seat_count_unavailable), assert_byo_mode_available "
    "(byo_mode_unavailable), and close_period's fetch_licenses call "
    "(licenses_unavailable). "
    "⭐ WHY THIS EXISTS BESIDE billing_invoice_close_failures_total"
    '{reason="tenant_refused"} RATHER THAN INSIDE IT: that label is an '
    "aggregate across every gate in the module, and for EACH gate the "
    "distinction between its reasons is the entire operational signal — most "
    "urgently for bucket_gap, which a September armed with "
    "INVOICE_CLOSE_ENABLED=true will raise for every current period (the "
    "billing basis was truncated 2026-09-17) and which must read as a correct "
    "decline, not as the module or licences being down. "
    "`tenant_unregistered` and `tenant_archived` mean the gate caught exactly "
    "what it was built for — a cost-ledger tenant that is in no licences "
    "registry row, or one that is retired — and a handful per month is the "
    "expected steady state, not an incident. A SPIKE to the size of the tenant "
    "set means something else entirely: the registry answered with a "
    "near-empty map, and every tenant in the estate is being called a phantom. "
    "⛔ Those readings demand different responses (ignore vs page vs retry) and "
    "an undifferentiated refusal count reports them identically — which is the "
    "failure this counter exists to make impossible. "
    "⚠️ A registry that could not answer AT ALL is NOT counted here: "
    "TenantRegistryUnavailableError is raised before any tenant is judged, so "
    "it lands on the aggregate counter's `tenant_refused` and on "
    "tenant_registry_strict_* in the log. A zero here during a licences outage "
    "is therefore correct and is not evidence the gate ran. Deliberately not "
    "given its own reason label here for the same cause: pre-creating a series "
    "this gate never increments would be an instrument that can only ever read "
    "0 — worse than absent, because it looks like a working detector.",
    ["reason"],
    # bucket_gap | tenant_unregistered | tenant_archived | seat_count_unavailable
    # | byo_mode_unavailable | licenses_unavailable
)

invoice_close_skipped_total = Counter(
    "billing_invoice_close_skipped_total",
    "Tenant-periods the close declined to NUMBER because nothing in them "
    "charges the customer (services.invoices.PeriodNotBillableError). "
    "⭐ WHAT IT PROTECTS IS A GAPLESS, IRREVERSIBLE SERIES. An invoice number "
    "is drawn from a counter ROW precisely so it cannot have gaps, which means "
    "a number spent on an empty statement is audit evidence no later fix "
    "retracts. Measured 2026-09-13: the estate's only invoice, "
    "INV-2026-000001, is $0.00 for an internal cost_centre=ci tenant, and ~10 "
    "further internal cost centres had activity in the same period — so an "
    "armed loop would spend a number a month on each of them. "
    "⛔ NOT THE SAME AS billing_invoices_closed_total"
    '{status="skipped"}, which means the loop declined the whole PERIOD for '
    "not yet being past its settle window (invoice_close_lag_days). That one "
    "is about TIME and resolves itself on a later tick; this one is about "
    "MONEY and will skip the same tenant again every month, correctly. "
    "⚠️ A high and steady value here is HEALTH, not backlog. The signal worth "
    "alerting on is the opposite — this falling to zero while "
    "billing_invoices_closed_total rises would mean empty statements started "
    "being numbered again.",
    ["reason"],  # no_billable_basis
)

invoice_close_failures_total = Counter(
    "billing_invoice_close_failures_total",
    "Invoice-close attempts that raised instead of landing on "
    "issued/draft/skipped, labelled by the failure reason "
    "services.invoice_closer assigns. Non-zero means a period the loop could "
    "neither close nor cleanly decline — the case the loop's broad "
    "exception handling exists to keep the leader alive through, but must "
    "not keep invisible. "
    "⚠️ WHAT THIS COUNTER SEES: both LOOP-level and per-TENANT failures. "
    "`infra_error` and `unexpected_error` are the loop-level reasons — an "
    "exception that reached run_invoice_close_loop's own except arms "
    "(SQLAlchemyError, and the deliberately broad arm respectively). "
    "`tenant_refused` and `tenant_error` are per-tenant: "
    "services.invoices.close_due_periods still catches a refusal inside its "
    "own loop, logs invoice_close_due_refused and continues to the next "
    "tenant, but it now RETURNS those counts and the loop increments this "
    "counter with them — `tenant_refused` when a GATE DECLINED (InvoiceError, "
    "or an upstream that could not state a figure the invoice needs: "
    "SeatCountUnavailableError, LicensesUnavailableError, "
    "ByoModeUnavailableError — all honest, expected, self-healing refusals that "
    "drew no invoice number), `tenant_error` for anything else, which means a "
    "DEFECT with a traceback to read. ⚠️ Keep that split: the "
    "BillingInvoiceCloseErrored alert pages on `tenant_error`, so classifying "
    "an upstream outage there pages on a designed refusal and dilutes the one "
    "signal that means the module broke. So a cycle in "
    "which every tenant refused — licences service down for the whole cycle, "
    "say — is now VISIBLE here rather than presenting as a clean cycle at "
    "zero. "
    "⚠️ That does not promote this counter to proof of the opposite: it is "
    "the sweep's own bookkeeping, and a sweep that never ran, never reached "
    "a tenant, or miscounted reports nothing to contradict itself. "
    "billing_invoice_close_lag_days remains the INDEPENDENT signal, because "
    "it re-queries billing_invoices rather than trusting the sweep — an "
    "all-refused or never-ran cycle shows up there as a lag that does not "
    "fall.",
    ["reason"],
)

# ⭐ PRE-SEED, for exactly the reason documented above `pricing_miss_total`: a
# labelled Counter exports HELP/TYPE but NO SAMPLES until its first
# `.labels(...)`, and Prometheus ingests samples, not headers. So a fleet that
# has not yet failed a close creates no series at all, and
# `increase(billing_invoice_close_failures_total{reason="tenant_error"}[1h]) > 0`
# cannot fire — the FIRST failure is invisible, which is the one that matters.
#
# Measured 2026-09-11: this metric had NEVER appeared in 24 days of retention,
# while its siblings `_enabled`, `_is_leader` and `_lag_days` each carried 2
# series from the same process — so the exporter works and the absence is birth,
# not scrape. `BillingInvoiceCloseErrored` (gitops #3806) shipped that same day
# against this series and was therefore born blind; its sibling
# `InferenceRouterCostEventsMalformedTenant` carries an `unless … offset 1h`
# birth clause, this one does not.
#
# ⚠️ All four reasons, not just the paged one. `tenant_error` is what the alert
# pages on, but a `tenant_refused` series that only appears on the first refusal
# makes "no refusals" and "never asked" indistinguishable — and this estate has
# a documented history of reading the second as the first.
for _close_failure_reason in ("infra_error", "unexpected_error", "tenant_refused", "tenant_error"):
    invoice_close_failures_total.labels(reason=_close_failure_reason)

# ⭐ SAME BIRTH PROBLEM, SAME FIX, for the two 2026-09-13 counters. Born at 0 at
# import so that "this gate has never refused anything" is a READABLE zero rather
# than an absent series, and so an alert written against either one cannot be
# vacuously green on the day it ships. The estate has now paid for this lesson
# twice on this very metric family (see the note above), so the pre-init goes in
# with the counters rather than after the first incident.
#
# ⚠️ `tenant_registry_unavailable` is deliberately NOT in this list: that
# condition raises before any tenant is judged and is counted on the aggregate
# `invoice_close_failures_total{reason="tenant_refused"}`. Pre-creating a series
# this gate never increments would be an instrument that can only ever read 0 —
# worse than absent, because it looks like a working detector.
#
# ⭐ WIDENED 2026-09-18 with the same pre-seed obligation: a labelled Counter
# exports no series until first `.labels(...)`, so `bucket_gap` — the one an
# INVOICE_CLOSE_ENABLED=true September will actually hit, every period, on
# every tenant — must exist and read 0 BEFORE the first refusal, or the first
# refusal (indistinguishable from an outage on the aggregate counter alone) is
# also the first time this series appears at all. `seat_count_unavailable`,
# `byo_mode_unavailable` and `licenses_unavailable` are pre-seeded alongside it
# for the identical reason the two 2026-09-13 labels above were.
for _close_refusal_reason in (
    "tenant_unregistered",
    "tenant_archived",
    "bucket_gap",
    "seat_count_unavailable",
    "byo_mode_unavailable",
    "licenses_unavailable",
):
    invoice_close_refused_total.labels(reason=_close_refusal_reason)

invoice_close_skipped_total.labels(reason="no_billable_basis")

invoice_close_enabled = Gauge(
    "billing_invoice_close_enabled",
    "1 when settings.invoice_close_enabled armed the monthly invoice-close "
    "loop on this replica, 0 when it did not. "
    "⭐ THIS EXISTS TO KEEP THE LAG ALERT HONEST WHILE THE FEATURE IS OFF. "
    "The lag gauges below are module-level, so they are constructed — and "
    "exported at 0.0 — by the side-effect import in main.py on EVERY "
    "replica, whether or not the loop was scheduled. Without this gauge, "
    "`time() - billing_invoice_close_lag_timestamp_seconds` reads as an "
    "enormous staleness on a service that is deliberately not closing "
    "anything, and BillingInvoiceCloseBehindSchedule fires continuously from "
    "the first deploy until somebody arms the flag. An alert that fires for "
    "weeks against a known-off feature gets silenced, and a silenced alert "
    "is a deleted detector — so the rule ANDs on this series being 1. "
    "⚠️ The cost of that is stated rather than hidden: turning the flag OFF "
    "also silences the alert. That is a deliberate config change with an "
    "audit trail; a permanently-firing alarm is not. "
    "Born at 0 like any Gauge, and here 0 is the CORRECT birth value — "
    '"not armed" is the true state before anything sets it — which is the '
    "opposite of the hazard the lag pair below documents. main.py sets it "
    "unconditionally from settings, OUTSIDE the `if`, so a 0 always means "
    "the flag is false and never means the setting line was skipped.",
)

# ⭐ billing_invoice_close_lag_days and billing_invoice_close_lag_timestamp_seconds
# are a PAIR — read one without the other and the reading inverts.
#
# Both are unlabelled Gauges, and prometheus_client exports an unlabelled
# Gauge at 0.0 from construction: the series exists at import, before the
# loop has ever run. For the two gauges below that fact cuts in OPPOSITE
# directions:
#
# * lag_days born at 0 is a HAZARD. "The loop never ran, so nothing has ever
#   been closed" and "the loop is fully caught up, nothing is behind" are
#   byte-identical zeros. absent() can never fire on this series to catch
#   the first case, because the series is never absent — it is just wrong.
# * lag_timestamp_seconds born at 0 is CORRECT, and strictly better than
#   absent. The alert shape is `time() - <gauge>`: an unset (0) gauge yields
#   an enormous age that fires immediately and reads "never succeeded". If
#   this series were absent instead, the expression would return no series
#   at all and the alert could not fire — the exact hazard the lag gauge has.
#
# So the timestamp is written ONLY alongside a real publish of lag_days,
# never on its own, and it is what separates "measured and fine" from
# "never measured" — a distinction the value gauge cannot make by itself.
#
# ⭐ MEASURED, not theoretical: on a sibling deploy, two pods read
#   leader    lag_hours = 1   lag_timestamp_seconds = 1788855478 (~26s old)
#   follower  lag_hours = 0   lag_timestamp_seconds = 0
# The follower row alone reads "zero lag, healthy". It has never ticked.
# The wrong reading is the reassuring one, which is exactly why nobody goes
# looking for it — and why billing_invoice_close_lag_days = 0 on a
# non-leader replica that has never run this loop will look identical to a
# healthy, fully-closed book unless the timestamp partner is read beside it.
invoice_close_lag_days = Gauge(
    "billing_invoice_close_lag_days",
    "How many days behind the invoice-close loop is: age in days of the "
    "oldest billing period that is already past its settle window "
    "(settings.invoice_close_lag_days) but has not been closed. 0 is the "
    "healthy value. ⛔ Also prometheus_client's default at import, before "
    "the loop has run even once — never read this gauge alone; pair it with "
    "billing_invoice_close_lag_timestamp_seconds below. See the block "
    "comment above this gauge for why. "
    "⛔ PER-REPLICA LAST MEASUREMENT, NOT A FLEET FACT: it is written only on "
    "a tick where THIS replica held leadership, and prometheus_client keeps "
    "exporting the last value set. A replica that has LOST leadership "
    "therefore keeps publishing its final reading — which is typically the "
    "high one taken just before it stopped closing anything — for the rest "
    "of its process lifetime, with nothing to age it out. An alert must gate "
    "on billing_invoice_close_is_leader == 1 (e.g. "
    "`billing_invoice_close_lag_days and on(...) "
    "billing_invoice_close_is_leader == 1`) rather than taking a bare max() "
    "across replicas: a bare max() lets an ex-leader's stale high value pin "
    "the alert firing long after the NEW leader has closed everything.",
)

invoice_close_lag_timestamp_seconds = Gauge(
    "billing_invoice_close_lag_timestamp_seconds",
    "Unix time billing_invoice_close_lag_days was last published from a real "
    "invoice-close pass. Written ONLY alongside that gauge, never on its "
    "own, so a partial cycle can never present its lag reading as fresh. "
    "Alert on time() - this: an unset (0) gauge yields a huge age and fires "
    "immediately, which is the correct failure mode — see the block comment "
    "above this gauge for why an absent series would be worse.",
)

invoice_close_is_leader = Gauge(
    "billing_invoice_close_is_leader",
    "1 on the replica that holds the invoice-close lease, 0 otherwise, "
    "refreshed every tick by every replica. DIAGNOSTIC ONLY -- never use it "
    "to select which replica's lag gauges to trust (a wedged loop reports 1 "
    "forever; billing_invoice_close_lag_timestamp_seconds is the real "
    "selector, same reasoning as audit_last_success_timestamp_seconds "
    "above). Same shape as billing_rollup_is_leader: alert on sum()==0 "
    "(nobody closing invoices) and sum()>1 (split brain).",
)
# Silent-failure instrumentation
#
# Every counter below covers a site that, before this block existed, failed at
# `logger.debug` with no metric at all -- i.e. a money-affecting failure whose
# only trace was a log line nobody collects at DEBUG. They are grouped here
# rather than beside their subject areas so the SET is visible: the defect they
# share is not any one of them, it is that a whole class of failure was
# unobservable.
#
# ⛔ THE UNLABELLED AND FIXED-LABEL ONES ARE BIRTHED AT ZERO below. A counter
# that only appears on first failure produces NO SERIES until the thing breaks,
# so `rate(...[1h]) == 0` returns empty rather than false and an alert on it
# cannot distinguish "healthy" from "never deployed". See the health-probe birth
# block above for the same reasoning applied to a different family.
#
# ⚠️ `app_rate_missing_total` is declared far ABOVE this block, next to
# `pricing_miss_total`, and IS birthed there -- on the increment site's own
# `or "unknown"` fallback, once per `surface`. An earlier revision of this
# branch carried a SECOND declaration of that counter here, asserting the
# opposite (that it is deliberately not birthed). Two `Counter()` calls for one
# metric name raise `Duplicated timeseries in CollectorRegistry` at IMPORT, so
# the whole test session errored during collection rather than failing a test --
# a defect that cannot reach production because nothing would start.
# ⛔ Do not re-add a declaration here. One metric, one declaration, and a label
# domain that is open (`service_key`) still gets a birthed bucket so that
# "no unpriced keys" and "this alert is structurally dead" stay distinguishable.
# ---------------------------------------------------------------------------

budget_redis_op_failed_total = Counter(
    "billing_budget_redis_op_failed_total",
    "Failures of a Redis budget bookkeeping operation, by operation. "
    "`release` -- a settled reservation was not released, so `reserved` stays "
    "inflated and the reserve gate drifts CLOSED, eventually 429-ing every "
    "request while committed consumption is still near zero.",
    ["operation"],  # release
)


# Birth every series at 0 -- see the block comment above.
budget_redis_op_failed_total.labels(operation="release")


# ---------------------------------------------------------------------------
# PEL reclaim (services.consumer.reclaim_pending)
#
# The two counters below are the pair: one counts entries RECOVERED from a
# crashed consumer's pending-entries-list, the other counts entries that were
# already DESTROYED by the time the reclaim reached them. Only together can a
# rate be read; the loss counter alone answers "how many" and never "out of
# how many".
#
# ⚠️ `stream` uses the SAME bounded vocabulary as
# `billing_tenant_caller_unasserted_total{stream}` above (`usage` | `cost`)
# rather than the raw Redis stream name. The raw names come from
# settings.events_stream / settings.costs_stream, which this module must not
# import (it is imported BY the consumer, and `observability.metrics` is
# deliberately settings-free), so they are neither birthable here nor stable
# across environments. The writer maps them at the one site that iterates them.
# ---------------------------------------------------------------------------

#: The ONLY values the two PEL counters may carry. Same vocabulary as
#: ``tenant_caller_unasserted_total``, bounded by the two streams
#: ``reclaim_pending`` walks.
PEL_STREAMS: tuple[str, ...] = ("usage", "cost")

pel_entries_reclaimed_total = Counter(
    "billing_pel_entries_reclaimed_total",
    "Redis stream entries RECOVERED from a pending-entries-list by "
    "services.consumer.reclaim_pending (XAUTOCLAIM) and re-staged through the "
    "normal persist path. These are entries a consumer had already read and never "
    "acked — stranded by an ungraceful crash (OOM / SIGKILL / node loss), because "
    "the steady-state `>` read only ever returns never-delivered messages. "
    "NON-ZERO IS NOT A FAULT: it is the recovery working, and the idempotency inbox "
    "makes a re-claim of an already-persisted entry a no-op. "
    "⛔ ITS JOB IS TO BE THE DENOMINATOR for "
    "billing_pel_entries_evicted_total below. A loss count with no recovered count "
    "beside it cannot be read as a rate — 3 lost out of 3 and 3 lost out of 30,000 "
    "are different incidents and were previously the same number. "
    "⚠️ Counted at STAGING, not at persistence: an entry counted here has been "
    "appended to a flush buffer, not committed. A flush that then fails leaves the "
    "entry un-acked, and the next reclaim counts it again.",
    ["stream"],  # usage | cost
)

pel_entries_evicted_total = Counter(
    "billing_pel_entries_evicted_total",
    "CONFIRMED DATA LOSS. Pending-entries-list ids that XAUTOCLAIM could not claim "
    "because the entry NO LONGER EXISTS in the stream: the consumer had read it, "
    "never acked it, and the entry was trimmed away underneath the PEL. Redis was "
    "its only copy — nothing was ever written to Postgres — so there is nothing to "
    "recover and no later pass can repair it. On the `cost` stream that is billable "
    "spend that will never be invoiced; on `usage` it is metering rows plus the "
    "per-licence quota delta they would have carried. "
    "⛔ EXPECTED VALUE IS FLAT ZERO, AND AN INCREASE IS AN INCIDENT. This is one of "
    "the few counters in this module that may safely be alerted as "
    "`increase(...[1h]) > 0`. "
    "HOW IT HAPPENS: the producer bounds both ingest streams with "
    "`XADD MAXLEN ~ 100_000`, and — independently — this service AGE-trims both to "
    "36h (rollup_engine.EVENTS_STREAM_MAX_AGE_HOURS / COST_STREAM_MAX_AGE_HOURS via "
    "trim_stream, run from the consumer's trim block and the rollup loop). BOTH "
    "trims are blind to PEL membership, so a persistence stall longer than the "
    "tighter bound destroys pending entries. The COUNT bound needs a ~62h stall at "
    "the ingest rate measured 2026-08-25 (~0.39 events/s against ~87k of headroom); "
    "⚠️ the AGE bound needs only 36h and is therefore the nearer edge. "
    "WHY IT IS A COUNTER AND NOT JUST A LOG LINE: until this existed the ONLY trace "
    "of a confirmed loss was the `pel_entry_evicted_before_reclaim` structlog "
    "warning plus an `evicted=` field on the reclaim summary. The code comment at "
    "the site said 'if that ever changes, this must be loud' — and a log line "
    "nobody greps on a schedule is not loud. It cannot be alerted on, ranked, or "
    "read as a rate. "
    "The paired WARNING still names each lost id individually; on the legacy "
    "Redis 6.2 / redis-py < 5 reply shape the id is genuinely unrecoverable and is "
    "logged as `<unavailable>` rather than as a fabricated id. This counter counts "
    "all three reply shapes (the Redis >= 7 third element, an inline nil entry, and "
    "a nil-fields entry that kept its id) on one series, because they are one "
    "population — which shape was seen is a Redis-version question answered by the "
    "log, not a billing question.",
    ["stream"],  # usage | cost
)

# Birth both series on both streams. Same rule as every counter in this module,
# and it bites hardest here: the healthy reading of the eviction counter is a
# PERMANENT ZERO, so without birth an `increase(...) > 0` alert selects an empty
# vector on every healthy pod and is vacuously green — indistinguishable from a
# clean estate, which is exactly the alert-vacuity failure this file documents at
# length on billing_pricing_miss_total.
for _pel_stream in PEL_STREAMS:
    pel_entries_reclaimed_total.labels(stream=_pel_stream)
    pel_entries_evicted_total.labels(stream=_pel_stream)


# ---------------------------------------------------------------------------
# request_quota_config mirror-drift instrumentation
#
# request_quota_config (services.quota) is a MIRROR of the licences registry,
# and the two directions that keep it in sync are asymmetric:
#
#   PUSH (licences -> billing): the licences-metering-reconcile CronJob,
#   INSERT/UPDATE only -- it never deletes and never marks a row non-active.
#   PULL (billing <- licences): POST /api/budget/sync-from-licenses, the ONLY
#   thing that deletes an absent row or marks one non-active. Its own
#   docstring says it verbatim: OPERATOR-TRIGGERED ONLY, no scheduler.
#
# So the mirror can only ever grow stale between manual runs, and nothing
# before this block could tell "clean by design" apart from "clean because
# nobody has looked in three weeks". These three gauges are READ-ONLY --
# nothing here deletes a row, marks a status, or adds a scheduler. They are
# published from inside services.quota.record_quota_config_drift, which
# services.routers.budget.sync_from_licenses calls with the SAME
# known_license_ids / active_license_ids sets it already built from that
# run's live `GET {licenses_url}/internal/licenses` fetch -- there is no
# dblink between the two Postgres databases, so this is the only truthful
# comparison available, and it exists only at the moment of an operator-
# triggered sync. Between syncs the gauges simply hold their last-measured
# value, which is the honest answer: a stale gauge paired with a stale
# `quota_config_sync_last_success_timestamp_seconds` tells an operator BOTH
# "here is the drift we last measured" and "here is how much to trust it".
# ---------------------------------------------------------------------------

quota_config_orphan_total = Gauge(
    "billing_quota_config_orphan_total",
    "request_quota_config mirror rows whose licence is no longer resolvable "
    "upstream, as measured at the last successful sync-from-licenses run. "
    "`reason=absent` -- the licence id was not returned by licences AT ALL "
    "(deleted/never existed); these rows are genuine junk and are what "
    "delete_absent_quota_configs removes on the SAME run this gauge is "
    "computed from, so a persistently non-zero value after several runs means "
    "the delete step is failing, not that it never ran. `reason=non_active` -- "
    "the licence exists upstream but its status is not 'active' (revoked, "
    "expired, pending); migration 0025 marks these rows rather than deleting "
    "them, so a non-zero value here is EXPECTED to persist and is not on its "
    "own an incident -- it becomes one only if it grows without a "
    "corresponding sync ever running (see the age gauge below). This is the "
    "live measurement of the LIMITATION services.license_resolver's module "
    "docstring records: a revoked licence stays resolvable until an operator "
    "manually reconciles it.",
    ["reason"],  # absent | non_active
)
for _quota_orphan_reason in ("absent", "non_active"):
    quota_config_orphan_total.labels(reason=_quota_orphan_reason).set(0)

quota_config_missing_total = Gauge(
    "billing_quota_config_missing_total",
    "Active licences (per the last successful sync-from-licenses run) with NO "
    "request_quota_config mirror row at all. Unlike the orphan gauge above, "
    "this direction is not repaired by sync-from-licenses -- a missing row "
    "only appears in the first place when a licence's plan carries no "
    "quota_monthly, which upsert_request_quota_config never writes for (see "
    "routers.budget.sync_from_licenses). Non-zero means those licences are "
    "unattributable by services.license_resolver, which reads this table as "
    "its sole candidate set.",
)

quota_config_sync_last_success_timestamp_seconds = Gauge(
    "billing_quota_config_sync_last_success_timestamp_seconds",
    "Unix timestamp (services.quota._utcnow) of the last sync-from-licenses "
    "run that completed with no fetch/transport error. Exposed as a "
    "TIMESTAMP, not a running age, because nothing in this service ticks a "
    "clock for a manual, operator-triggered endpoint between invocations -- "
    "adding a loop to do so is exactly the scheduler this instrumentation is "
    "forbidden from adding. Compute the age in the alerting rule instead: "
    "`time() - billing_quota_config_sync_last_success_timestamp_seconds`. "
    "Birthed at 0 (the Unix epoch) at import, deliberately: `time() - 0` "
    "reads as ~56 years immediately on a fresh deploy that has never seen a "
    "successful sync, so 'never synced' and 'synced recently' cannot be "
    "confused the way an unlabelled series born only on first success would "
    "allow.",
)
quota_config_sync_last_success_timestamp_seconds.set(0)


# ---------------------------------------------------------------------------
# Licence↔budget reconcile (services/licence_budget_reconcile.py)
#
# The pairing rule for the gauges below is the same one the invoice-close block
# above spells out, and it exists here for a sharper reason: the healthy value
# of billing_licence_budget_divergent_total is 0, and 0 is ALSO what
# prometheus_client exports at import on a replica whose loop has never run a
# single cycle. Read alone, the gauge cannot tell "nothing has diverged" from
# "nothing has looked". Always gate it on the freshness of
# billing_licence_budget_reconcile_last_success_timestamp_seconds.
# ---------------------------------------------------------------------------

licence_budget_divergent_total = Gauge(
    "billing_licence_budget_divergent_total",
    "Active source='license_sync' token budgets whose licence is NOT backed by "
    "an active (or protectively-unreadable) licence upstream, as measured at "
    "the START of the last successful reconcile cycle -- i.e. BEFORE that "
    "cycle repaired anything. Measured pre-mutation on purpose: a run that "
    "finds 19 and deactivates 19 publishes 19, and the next run publishes 0, "
    "so the series shows the repair happening. A post-mutation gauge would "
    "read 0 forever and could never distinguish a healthy estate from a loop "
    "that never ran. ⛔ Never alert on this alone: 0 is also the import-time "
    "default. Pair it with the freshness gauge below, e.g. "
    "`billing_licence_budget_divergent_total > 0 and "
    "time() - billing_licence_budget_reconcile_last_success_timestamp_seconds "
    "< 7200`. A persistently non-zero value across several cycles means the "
    "deactivate step is disabled "
    "(licence_budget_reconcile_deactivate_enabled=false, the shipped default) "
    "or failing -- not that it has not run yet.",
)
licence_budget_divergent_total.set(0)

licence_budget_reconcile_unreadable_total = Gauge(
    "billing_licence_budget_reconcile_unreadable_total",
    "Licences the last successful cycle could not classify: status absent, or "
    "a status value not in models.db.quota.LICENSE_STATUSES. These are "
    "PROTECTED from deactivation rather than treated as orphans, so a non-zero "
    "value is not lost money -- it is degraded coverage, and it means "
    "upstream's lifecycle vocabulary has moved ahead of this service's. The "
    "fix is to widen LICENSE_STATUSES, not to widen the reconcile. ⚠️ This "
    "gauge is the ONLY signal that the fail-safe fired; without it a "
    "reconcile that silently protects every budget looks identical to a "
    "healthy one.",
)
licence_budget_reconcile_unreadable_total.set(0)

licence_budget_reconcile_last_success_timestamp_seconds = Gauge(
    "billing_licence_budget_reconcile_last_success_timestamp_seconds",
    "Unix time of the last reconcile cycle that read the licence registry "
    "COMPLETELY and published the gauges above. Written only on that path, "
    "never on a cycle that refused to act on a partial read. Alert on "
    "`time() - this`: born at 0 so a fresh deploy that has never reconciled "
    "reads as ~56 years old and fires immediately, which is the correct "
    "failure mode -- silence from this loop means budgets behind revoked "
    "licences are accumulating unwatched, which is the exact condition it "
    "exists to prevent. ⚠️ PER-REPLICA: written only while this replica holds "
    "the lease, and prometheus_client keeps exporting the last value set, so "
    "an ex-leader publishes a frozen timestamp for the rest of its process "
    "lifetime. Take max() across replicas for 'has ANYONE reconciled "
    "recently', which is the question that matters here.",
)
licence_budget_reconcile_last_success_timestamp_seconds.set(0)

licence_budget_reconcile_is_leader = Gauge(
    "billing_licence_budget_reconcile_is_leader",
    "1 on the replica holding the licence-budget reconcile lease, 0 otherwise, "
    "refreshed every tick by every replica. DIAGNOSTIC ONLY -- a wedged loop "
    "reports 1 forever, so never use it to pick whose gauges to trust; the "
    "freshness timestamp above is the real selector. Alert on sum()==0 "
    "(nobody reconciling) and sum()>1 (split brain, which on this loop means "
    "the advisory lock was taken on a transaction-mode pooler).",
)
licence_budget_reconcile_is_leader.set(0)

licence_budget_reconcile_failures_total = Counter(
    "billing_licence_budget_reconcile_failures_total",
    "Reconcile cycles that ended in an error, by reason. `licences_unreadable` "
    "-- the registry could not be read completely, so the cycle deliberately "
    "mutated NOTHING (a truncated read would orphan live budgets); repair is "
    "PAUSED, not broken, and the freshness gauge is what should page. "
    "`no_active_licences` -- the registry read PERFECTLY and classified not "
    "one licence as active-or-unreadable, so the derived keep-set was empty "
    "and the cycle refused rather than deactivate the whole estate. ⛔ Do NOT "
    "debug the licences service on this reason: it answered, and it answered "
    "'nothing is active'. Debug the QUERY that produced that answer -- a "
    "mis-scoped or tenant-filtered response, a half-migrated status column, or "
    "a genuinely fully-revoked estate. It is split from `licences_unreadable` "
    "precisely because that reason says 'could not be read', which is the "
    "opposite of what happened, and an operator who trusts the label loses the "
    "cycle. `infra_error` -- a database error. `unexpected` -- anything else, "
    "which should be treated as a bug in this loop. `deactivation_cap_exceeded` -- "
    "the cycle would have deactivated more budgets than "
    "settings.licence_budget_reconcile_max_deactivations_per_cycle allows, so "
    "it deactivated NOTHING and did not advance the freshness gauge. That last "
    "one is not an error in the loop: it is the loop refusing a blast radius, "
    "and it needs a HUMAN to read the divergent count and either clear the "
    "backlog or raise the ceiling. It will repeat every tick until one of "
    "those happens.",
    ["reason"],
)
# Birth every reason series at 0 at import time -- same hazard as
# COST_DEAD_LETTER_REASONS above: a labelled Counter exports NO series until its
# first .labels().inc(), so `increase(...{reason="deactivation_cap_exceeded"})`
# would return an empty vector on a pod that has never tripped the cap, and an
# empty vector reads as a measured zero to anyone writing an alert.
#
# ⚠️ A NAMED CONSTANT rather than an inline tuple, and the reason is a measured
# one. A test that proves birth by scanning `generate_latest(REGISTRY)` for the
# label is VACUOUS the moment any earlier test in the process has incremented
# that reason, because the increment births the series by itself -- the
# assertion then passes with this loop deleted entirely. Measured: dropping
# "no_active_licences" from here left 44/44 green. Exporting the tuple lets the
# test assert the BIRTH LIST, which is the thing that must not shrink, instead
# of a side effect any other test can supply.
#
# ⛔ Bounded on purpose: every member is a literal here and the loop maps
# exception types to these names. No caller-supplied reason string may ever
# reach this label -- an unbounded label is a cardinality incident, and it also
# makes this birth list unwriteable.
LICENCE_BUDGET_RECONCILE_FAILURE_REASONS: tuple[str, ...] = (
    "licences_unreadable",
    "no_active_licences",
    "infra_error",
    "unexpected",
    "deactivation_cap_exceeded",
)
for _lbr_reason in LICENCE_BUDGET_RECONCILE_FAILURE_REASONS:
    licence_budget_reconcile_failures_total.labels(reason=_lbr_reason)

licence_budget_reconcile_enabled = Gauge(
    "billing_licence_budget_reconcile_enabled",
    "1 when settings.licence_budget_reconcile_enabled armed the reconcile loop "
    "on this replica, 0 when it did not. Set unconditionally at startup so a "
    "deliberately-disabled loop is distinguishable from one whose process "
    "never reached the wiring. Any alert over the divergence or freshness "
    "gauges should AND on this being 1.",
)

licence_budget_reconcile_deactivate_enabled = Gauge(
    "billing_licence_budget_reconcile_deactivate_enabled",
    "1 when the reconcile loop is permitted to deactivate divergent budgets, 0 "
    "when it runs measure-only (the shipped default). ⚠️ A 0 here is what "
    "explains a billing_licence_budget_divergent_total that never falls: the "
    "loop is looking and reporting but not repairing. An alert on standing "
    "divergence must read this gauge before concluding the reconcile is "
    "broken.",
)

cost_events_last_seen_timestamp_seconds = Gauge(
    "billing_cost_events_last_seen_timestamp_seconds",
    "Unix time of the NEWEST billing_cost_events.created_at per emitting "
    "(service_key, provider) pair, published by the rollup LEADER on every tick "
    "(services.emitter_freshness.publish_emitter_freshness). THE GAP IT CLOSES: a "
    "metering emitter can die silently. Measured over 7 days the ledger carries 13 "
    "providers over 12 service_keys, while billing_cost_events_by_service_total -- "
    "a per-pod Counter born at process start whose child series appears only when "
    "that key emits AGAIN -- had live series for 6, so 'no series' and 'dead since "
    "May' were the same empty vector. Four emitters (asr, tts, image-gen, "
    "lean4-prover) are launchd services with NO Prometheus scrape target at all, so "
    "the ledger is the only place their liveness can be observed. Real cases: mflux "
    "(image-gen) last emitted 2026-09-10 18:39; asr dormant since 2026-05-27. "
    "⭐ IT IS A TIMESTAMP, NOT AN AGE, ON PURPOSE: alert with "
    "`time() - max by (service_key, provider) (...) > <seconds>`. An age computed "
    "in-process is true only at the instant it is set and then goes stale silently, "
    "because prometheus_client keeps exporting the last value for the life of the "
    "process; a timestamp is re-evaluated correctly on every rule evaluation. "
    "⛔ AGGREGATE WITH max(), NEVER sum()/avg(): only the leader publishes, a "
    "follower publishes NOTHING (never 0 and never NaN -- a 0 was once read as an "
    "ancient success rather than as an abstention), and an ex-leader keeps "
    "exporting its last reading forever, so pair the rule with a recent "
    "billing_metering_hourly_lag_timestamp_seconds, written by the same loop on the "
    "same tick, as the freshness selector. "
    "⚠️ A SERIES IS NEVER DELETED, so a stopped emitter's age climbs forever -- "
    "that IS the signal, and it is also why a DELIBERATELY RETIRED emitter must be "
    "retired by silencing or by excluding its label, not by making the publisher "
    "forget it (retention dropping its rows does not clear the series; it is "
    "process-lifetime state). "
    "⚠️ CAPPED at services.emitter_freshness.MAX_FRESHNESS_SERIES (50) pairs, "
    "newest first: both labels are varchar(64) producer input, so the domain is "
    "OPEN and an emitter minting per-request label values would be a cardinality "
    "incident here. Overflow is counted on "
    "billing_cost_events_last_seen_series_dropped_total -- read that before "
    "trusting this series set.",
    ["service_key", "provider"],
)

cost_events_last_seen_series_dropped_total = Counter(
    "billing_cost_events_last_seen_series_dropped_total",
    "(service_key, provider) pairs that existed in billing_cost_events but were "
    "NOT published to billing_cost_events_last_seen_timestamp_seconds because the "
    "50-series cardinality cap was reached. DELIBERATELY LABEL-LESS: naming the "
    "dropped pairs would re-introduce exactly the unbounded label domain the cap "
    "exists to bound. ⚠️ ANY increase means the freshness gauge is INCOMPLETE and "
    "a dormant emitter may be missing from it entirely -- the cap keeps the "
    "newest-first pairs, so freshly-minted label values evict the dormant emitters "
    "this instrument exists to find. The fix is upstream in the emitter that is "
    "minting values, NOT a larger cap. It is born at 0 below so `increase()` "
    "returns a visible zero on a healthy pod instead of an empty vector.",
)
# Birth the series at import: an unlabelled Counter is exported from construction,
# but .inc(0) makes the intent explicit and matches the birth discipline above --
# this series' whole job is to be readable as a measured 0 on a healthy estate.
cost_events_last_seen_series_dropped_total.inc(0)


# --------------------------------------------------------------------------- #
# Round 8 (2026-09-13) — instruments for the close-candidate, usage-attribution,
# quota-drift, byo-mode and leader-keepalive fixes landed in the same change.
#
# Grouped here rather than filed into each family above BECAUSE they were added
# together and their birth discipline is the point: every one of them exists to
# make a condition that was previously SILENT readable, and several of them are
# expected to read a flat 0 forever on a healthy estate. A 0 is only evidence if
# the series exists, so each is born at import below.
# --------------------------------------------------------------------------- #

invoice_close_candidates_total = Counter(
    "billing_invoice_close_candidates_total",
    "Tenants nominated for a monthly invoice close, by which axis nominated them. "
    "⛔ THE LABELS DO NOT PARTITION -- a tenant with usage AND a paid term "
    "increments two of them, so the candidate count is the UNION, never the sum. "
    "`access_charge` is the axis that did not exist before round 8: the sweep built "
    "its candidate set from billing_cost_daily UNION billing_metering_daily only, so "
    "a subscription customer with a monthly access charge and zero API calls that "
    "month was silently never invoiced at all -- no row, no refusal, nothing to "
    "alert on -- while the platform overview counted the same term as revenue. "
    "Watch for `access_charge` sitting at 0 while paid terms exist: that is the "
    "original defect returning.",
    ["source"],
)
for _src in ("cost", "metering", "access_charge"):
    invoice_close_candidates_total.labels(source=_src)

metering_license_resolved_total = Counter(
    "billing_metering_license_resolved_total",
    "Outcome of resolving a licence for a USAGE-bus event that arrived without "
    "one. Before round 8 the usage path read license_id off the wire only and "
    "never called license_resolver -- measured 0.628% licence coverage over 7 days "
    "(444 of 70,658) against 99.4% on the cost bus, which DOES resolve. "
    "⚠️ `unresolved` is NOT a failure: many emitters are legitimately licence-less "
    "and the row is still stored with a NULL licence rather than dropped. A rising "
    "`error` is the real signal.",
    ["result"],
)
for _res in ("resolved", "unresolved", "error"):
    metering_license_resolved_total.labels(result=_res)

metering_app_resolved_total = Counter(
    "billing_metering_app_resolved_total",
    "Outcome of resolving an app for a usage-bus event that arrived without one. "
    "⛔ DELIBERATELY SEPARATE FROM billing_metering_license_resolved_total rather "
    "than a second label on it: the two resolutions run over different populations "
    "(app resolution is attempted for rows whose licence resolution already "
    "succeeded), so folding them would give one series two denominators and make "
    "any ratio built on it meaningless.",
    ["result"],
)
for _res in ("resolved", "unresolved", "error"):
    metering_app_resolved_total.labels(result=_res)

quota_config_value_drift = Gauge(
    "billing_quota_config_value_drift",
    "Licences whose mirrored request_quota_config.quota_monthly disagreed with the "
    "upstream plan.quota_monthly at the last sync-from-licenses run. "
    "⛔ -1 MEANS NEVER MEASURED, and the distinction is the whole point: the "
    "measurement is operator-triggered only, so a replica that never ran the sync "
    "must not export a 0 that reads as 'measured, agreed'. Alert on `> 0`; treat "
    "`== -1` as UNWATCHED, not healthy. "
    "⚠️ The quota_config_orphan/missing gauges above cannot see this condition: "
    "three writers upsert on the license_id primary key, so a clobbered LIMIT moves "
    "no id and an id-set comparison is blind to it by construction. That is why "
    "three prior rounds measured '0 divergence' -- a statement about the "
    "instrument, not about the data.",
)
quota_config_value_drift.set(-1)

byo_mode_indeterminate_total = Counter(
    "billing_byo_mode_indeterminate_total",
    "Times billing could not determine a tenant's byo_mode and said so, instead of "
    "silently substituting a commercial verdict. ⛔ THE DEFECT THIS REPLACES: any "
    "failure to reach the licences service -- a missing auth header, a timeout, or a "
    "DB fault on the licences side that returns HTTP 200 carrying a fail-closed "
    "guess -- used to be converted into `byo_required`, which effective_billing_mode "
    "maps to the configured COMMERCIAL mode. Measured: 49 of 59 tenants carry "
    "local_allowed or platform_only, so a single blip could have billed all of them "
    "with markup and access charges, and an invoice is immutable once frozen. "
    "⚠️ The live ACCESS gate still fails closed to bill, deliberately and "
    "correctly -- a lookup failure must not hand out free platform GPU. This counts "
    "only what a FROZEN INVOICE was asked to rely on.",
    ["reason"],
)
for _r in ("auth", "timeout", "http_error", "unavailable"):
    byo_mode_indeterminate_total.labels(reason=_r)

leader_keepalive_failures_total = Counter(
    "billing_leader_keepalive_failures_total",
    "Leader-election keepalive pings that failed or found the backend swapped, by "
    "loop. The keepalive exists because Postgres `idle_session_timeout` is 600s on "
    "the primary (measured from pg_settings, source=configuration file) while the "
    "election only pinged once per TICK -- so every loop with an interval above 600s "
    "had its lock-holding session reaped each cycle: invoice close at 3600s and "
    "licence-budget reconcile at 900s. Caught live: leader_acquired at 10:53:28Z, "
    "leader_election_error at 11:08:28Z, exactly 900s later. "
    "⛔ NOT A SPLIT-BRAIN INSTRUMENT -- mutual exclusion was never broken (each "
    "advisory lock had exactly one holder). What broke was the *_is_leader gauges, "
    "which a demoted pod kept reporting as 1 for up to a full interval. "
    "⚠️ SILENCE IS NOT HEALTH here: a keepalive task that never STARTS emits zero, "
    "which is indistinguishable from one that runs cleanly. Read it beside the "
    "*_is_leader gauges summing to exactly 1.",
    ["loop"],
)
for _loop in ("rollup", "invoice_close", "licence_budget_reconcile", "budget_watcher"):
    leader_keepalive_failures_total.labels(loop=_loop)

budget_watcher_is_leader = Gauge(
    "billing_budget_watcher_is_leader",
    "1 on the replica holding the budget-watcher lease, 0 otherwise. This loop had "
    "NO leader gauge at all before round 8 -- it was the one leader-elected loop "
    "whose leadership was entirely unobservable, so neither 'nobody is watching "
    "budgets' nor a split brain could be seen. DIAGNOSTIC ONLY, same caveat as "
    "billing_invoice_close_is_leader: alert on sum()==0 and sum()>1, and never use "
    "it to pick whose gauges to trust.",
)
budget_watcher_is_leader.set(0)

metering_status_overlong_total = Counter(
    "billing_metering_status_overlong_total",
    "Usage-bus events whose wire `status` exceeded the 20-character column and was "
    "stored as the bounded sentinel `_too_long` instead. "
    "⛔ WHY THIS IS NOT A COSMETIC CLAMP: the value went into a BULK insert "
    "verbatim, so one over-long status raised StringDataRightTruncation and failed "
    "the WHOLE batch -- every legitimate event flushed alongside it was lost with "
    "it. No attacker is needed; an exception string passed where a status was "
    "expected is enough (measured shape: a 62-character 'ValueError: connection "
    "reset by peer ... req=abc123'). "
    "⚠️ Found by a test written for the metric-label clamp, which is a different "
    "and much milder defect -- the label domain was bounded while the stored column "
    "was not. Any non-zero value names an emitter to fix upstream; the sentinel is "
    "deliberately not `denied_*` or `allowed` so it cannot satisfy a billing branch.",
)
metering_status_overlong_total.inc(0)

metering_retarget_tenant_invalid_total = Counter(
    "billing_metering_retarget_tenant_invalid_total",
    "Usage-bus events whose optional wire `retarget_tenant_id` (license-authz#65) "
    "was present but not a non-nil uuid, and was therefore stored as NULL. The "
    "event itself is still persisted; only the retarget is refused, because the "
    "column is `uuid` and one bad value would otherwise fail the whole bulk "
    "INSERT. Any non-zero value names an emitter to fix upstream.",
)
metering_retarget_tenant_invalid_total.inc(0)

project_id_multi_tenant_projects = Gauge(
    "billing_project_id_multi_tenant_projects",
    "DISTINCT `project_id`s seen under MORE THAN ONE tenant inside the "
    "attribution audit's window -- i.e. the same project uuid arriving on two "
    "tenants' traffic. "
    "WHAT THIS IS AND IS NOT. A project id is caller-supplied and billing never "
    "verifies it, but it is only ever a SUB-DIMENSION of an ALREADY-VERIFIED "
    "tenant: every write carries the tenant resolved from X-Tenant-Id, and "
    "the project axis only splits rows INSIDE it. So a forged project id can at "
    "worst move a caller's own spend between the caller's own projects -- it "
    "can never move money across a tenant boundary, never select who pays, and "
    "never surface another tenant's rows. The blast radius is one tenant's own "
    "reporting. "
    "⛔ THE PROTECTION IS THE GROUP KEY, NOT A TENANT FILTER ON EVERY READ. "
    "This docstring used to assert 'every read is filtered on that tenant "
    "first'. That is FALSE, and an audit measured it in `routers/costs.py`: "
    "`GET /api/costs/provider` (`costs_by_provider`) and `GET "
    "/api/costs/summary` (`costs_summary`) are operator-scope and build their "
    "filter list from `bucket_date` ALONE -- no tenant predicate at all; and "
    "`GET /api/costs/user/{subject_id}` (`costs_by_user`) takes `tenant_id` as "
    "an OPTIONAL query param, with `_costs_for_subject` appending the predicate "
    "only `if tenant_id is not None`, so the DEFAULT call sums a subject across "
    "every tenant they bill under. Only `/api/costs/me` (`costs_me`) is "
    "unconditionally tenant-scoped, and only because it resolves the tenant "
    "itself via `active_tenant_or_400`. WHAT ACTUALLY HOLDS THE LINE is that "
    "the project breakdown's GROUP KEY is now `(tenant_id, project_id)` and no "
    "longer `project_id` alone (`routers._aggregation.aggregate_by_project`): "
    "two tenants asserting the same project uuid can no longer MERGE into one "
    "row even on an unfiltered operator route -- they stay two rows, and this "
    "gauge is what names the condition rather than the grouping silently "
    "absorbing it. "
    "⭐ AND THAT IS WHY A DETECTOR IS NEEDED RATHER THAN A SUM CHECK: A "
    "CROSS-TENANT MERGE PRESERVES THE REPORTED TOTAL. The merged row carries "
    "the sum of both tenants' spend, so the breakdown still reconciles to "
    "`total_cost_usd` to the cent -- every totals check, every invoice "
    "reconciliation and every 'does it add up' test passes while one tenant's "
    "money sits inside another tenant's row. No sum can ever see it; only a "
    "distinctness measurement over (tenant, project), which is this gauge, can. "
    "THE POINT OF THIS "
    "GAUGE is that a defect with a bounded blast radius is still a defect, and "
    "without an instrument this one is entirely SILENT: nothing in the row, the "
    "rollup or the API distinguishes a project id the caller owns from one it "
    "guessed. This makes forgery OBSERVABLE rather than silent. "
    "⭐ A PERIODIC GAUGE, NOT A HOT-PATH COUNTER, AND THAT IS THE WHOLE DESIGN. "
    "Incrementing a counter at ingest needs a 'which tenant asserted this "
    "project FIRST' store, and both hot-path answers are wrong: an in-process "
    "dict is PER-REPLICA (each of two replicas sees only its own share of the "
    "traffic, so the same cross-tenant reuse reads as no reuse on either) and "
    "RESETS ON RESTART, i.e. it systematically under-counts and cannot say by "
    "how much; a per-event DB lookup puts a query on a path taking ~283k "
    "rows/month. So this is measured instead by "
    "`services.attribution_audit.run_attribution_audit`, which is read-only, "
    "runs on the rollup leader's tick and reads the DB -- where EVERY replica's "
    "writes already are. Correct by construction, and free on the hot path. "
    "Consequences a reader must know: it is a LEVEL over the audit window, not "
    "a monotonic total, so `rate()`/`increase()` on it are meaningless -- alert "
    "on `> 0`; and it is written by the LEADER only, so aggregate with max(), "
    "NEVER sum(), and read it with "
    "`and on(instance) billing:attribution_audit:fresh` like every other gauge "
    "in that block. A replica that has never completed an audit pass scrapes "
    "0.0 here from process start; the freshness timestamp is the single "
    "discriminator between that and a measured zero, exactly as for "
    "billing_attribution_rate. "
    "⛔ DELIBERATELY NOT THE `x-app-key` DEFECT CLASS. That one was an UNSIGNED "
    "HEADER CHOOSING WHICH LICENCE PAID -- the payer was selected by unverified "
    "caller input, so forging it moved real money onto someone else's invoice. A "
    "project id NEVER selects the payer. Do not alert on this gauge with that "
    "severity. ⚠️ SUPERSEDED 2026-09-28: this said do not 'fix' it by verifying "
    "project ids against a registry billing does not own. That no longer holds: "
    "George's per-member + per-project ruling made per-project numbers a product "
    "surface on fr tenant pages, and any trusted pod could forge them by stamping "
    "another tenant's project. Ingest now checks project -> tenant against fr's "
    "map (services.project_owner_map, billing_project_id_refused_total"
    "{reason=foreign_tenant|unknown_project}); BILLING_PROJECT_OWNERSHIP_MODE "
    "decides whether a mismatch is only counted (observe, default) or nulled "
    "(enforce). A non-zero value here still means: go and find out WHICH caller "
    "is stamping a project it does not own -- the check does not fix the emitter. "
    "⚠️ LABELLED BY `scope` SINCE ROUND 16 (was wholly unlabelled before -- "
    "project_id and tenant_id themselves are STILL never a label, for the "
    "cardinality/disclosure reason this docstring used to give in full: both are "
    "unbounded customer-supplied uuids and a label on either is a cardinality "
    "bomb that would also publish one tenant's identifiers on a scrape endpoint "
    "every tenant's dashboards read; they stay in the audit's log line). "
    "`scope` has exactly THREE values (a third, `unknown`, was added in a "
    "follow-up fix within round 16 -- see the correction below) and answers a "
    "DIFFERENT question than the count does: not 'how many projects leak across "
    "a tenant boundary' but 'does any of that crossing touch a paying customer, "
    'or could this pass not tell\'. `scope="customer"` -- at least one of the '
    "tenants sharing the project is `kind='customer'` in the licences registry "
    '(wins over `unknown` if both are present); `scope="unknown"` -- no tenant '
    "sharing it is `customer`, but at least one is ABSENT from the registry or "
    '`kind=services.tenant_names.UNCLASSIFIED`; `scope="internal"` -- every '
    "tenant sharing it resolved to a real non-customer kind (`internal`, "
    "`service`, `test`, ...). D1 (billing-licensing estate repair, round 16) is "
    "what makes this split load-bearing -- a cross-tenant project is an accepted "
    "allocation-tag pattern when every tenant involved is internal, and a real "
    "leak when a customer-kind tenant is one of them; before this label the "
    "gauge could not tell the two apart. "
    "⛔ CORRECTED WITHIN ROUND 16: this docstring used to say an unregistered or "
    "unclassified tenant was folded into `internal` as a 'deliberate fail-open' "
    "that could 'only make the alarm quieter'. That was FALSE -- the alert built "
    "on this gauge fires on non-`internal` scopes only, so folding an "
    "unclassifiable tenant into `internal` SILENCED a real customer crossing for "
    "as long as that tenant (or a registry blip) stayed unclassified or "
    "unreachable, and it contradicted `services.tenant_names.UNCLASSIFIED`'s own "
    "invariant that an unclassified tenant must never be silently counted as "
    "internal. `unknown` now takes the alerting direction "
    '(`billing:project_id_multi_tenant_projects:leader{scope=~"customer|unknown"}'
    " > 0`) instead. "
    "⚠️ AN ENTIRELY EMPTY REGISTRY IS NOT `unknown` FOR EVERY PROJECT -- IT IS NO "
    "GAUGE WRITE AT ALL. `services.tenant_names.get_tenant_info` folds a 401, a "
    "transport error and a genuinely empty registry into the same `{}` (its own "
    "documented contract); classifying against an empty map would mark every "
    "single crossing project `unknown` on every total outage, which is the "
    "outage wearing the shape of a measurement, not one. So "
    "`services.attribution_audit.run_attribution_audit` abstains on a totally "
    "empty map -- same mechanism the four licence-ownership gauges above use: "
    "publish nothing this tick, keep the previous value, and withhold "
    "`billing_attribution_audit_last_success_timestamp_seconds` so the panels "
    "and alerts that AND on freshness do not trust a stale reading as current. A "
    "map that is merely PARTIAL (some tenants present, others absent) is not "
    "this case -- those tenants correctly resolve to `unknown` per-project, "
    "which is the fix, not the abstention. "
    "⚠️ ALL THREE LABEL VALUES ARE BORN, AT ZERO, UNCONDITIONALLY -- see the "
    "pre-seed loop below -- because on every tick where the registry DID answer, "
    "the three counts are computed together in the same pass as the rest of the "
    "project axis and none of them is ever individually 'unmeasured'; the state "
    "in which the WHOLE split is unmeasured (the empty-registry abstention "
    "above) is carried by the freshness timestamp, not by omitting a label.",
    ["scope"],
)

#: The three states of `billing_project_id_multi_tenant_projects{scope=...}`.
#: Bounded, and ALL THREE must be born: a missing series would read the
#: quietest possible number as an absent metric rather than a measured zero.
#: `unknown` was added in a round-16 follow-up fix -- see the gauge's docstring
#: correction above for why folding it into `internal` was unsafe.
PROJECT_MULTI_TENANT_SCOPES: tuple[str, ...] = ("customer", "internal", "unknown")

# Born at zero for both scopes -- same rationale as PROJECT_ATTRIBUTION_STATES
# below: prometheus_client would export 0.0 for a never-.labels()'d series
# anyway, but a reader must be able to tell "measured zero" from "this scope
# was never wired" without grepping the source.
for _scope in PROJECT_MULTI_TENANT_SCOPES:
    project_id_multi_tenant_projects.labels(scope=_scope)

#: The two states a cost_daily row's project attribution can be in. Bounded, and
#: BOTH must be born: a missing `sentinel` series would read as full coverage,
#: which is precisely the failure this gauge exists to end.
PROJECT_ATTRIBUTION_STATES: tuple[str, ...] = ("project", "sentinel")

cost_daily_project_attribution_rows = Gauge(
    "billing_cost_daily_project_attribution_rows",
    "billing_cost_daily rows split by whether their spend is attributed to a "
    "real project or carries the NULL_PROJECT_ID sentinel "
    "(00000000-0000-0000-0000-000000000004). attribution='project' counts rows "
    "with a project a caller actually asserted; attribution='sentinel' counts "
    "rows where NO project was asserted. The uncovered share is "
    "sentinel / (project + sentinel) -- A MEASURED NUMBER, which is the whole "
    "point. "
    "THE DEFECT THIS IS THE DIRECT ANTIDOTE TO: metadata.job_id was wired "
    "end-to-end and DEFAULTED to the literal string 'unknown', so every row had "
    "a job id, every reader treated it as one, and the field read as 100% "
    "coverage of a value no caller had ever populated -- 968 of 968 "
    "inference-router events, and nothing anywhere said so (see "
    "billing_placeholder_value_total). The project axis refuses to repeat that: "
    "the raw event columns are NULLABLE and NEVER DEFAULTED, the rollup's "
    "sentinel exists ONLY because a NULL cannot participate in uq_cost_daily -- "
    "Postgres NULLS DISTINCT, so a NULL never matches itself and the constraint "
    "stops refusing duplicate project-less rows (it is NOT that the rollup "
    "upserts onto these rows each cycle: services.rollup_engine DELETEs the "
    "whole bucket_date before inserting, so ON CONFLICT is never reached) -- and "
    "it is a value that means 'absent' and is COUNTED as "
    "absent here. A missing project id stays missing and gets counted. "
    "⚠️ ROWS, NOT MONEY. A row share is not a spend share -- the un-attributed "
    "rows can be 80% of the count and 0.5% of the bill, or the reverse. For the "
    "money split read the sentinel group in the by_project breakdown from "
    "/api/costs/* (routers._aggregation.aggregate_by_project returns it as an "
    "ordinary group, never dropped). Do not quote this ratio as a share of "
    "spend. "
    "⚠️ NON-ZERO sentinel IS NOT AUTOMATICALLY A BUG: traffic that genuinely has "
    "no project (S2S, batch, anything not raised from a kanban card) belongs "
    "there. It says 'this share is un-attributable', not 'this is broken' -- the "
    "actionable signal is the ratio MOVING, or staying at 1.0 for an emitter "
    "that is supposed to stamp projects. "
    "AGGREGATE WITH max(), NEVER sum(): set by the leader-elected rollup pass, "
    "and a leadership handover inside a scrape window can leave two replicas "
    "holding a value -- sum() would then double the row counts, same caveat as "
    "billing_rollup_date_gaps.",
    ["attribution"],
)

# Born at zero for both states. Birthing `sentinel` is the load-bearing half:
# with only `project` published, a service that has never attributed anything
# and one that attributes everything are the same reading.
for _attribution in PROJECT_ATTRIBUTION_STATES:
    cost_daily_project_attribution_rows.labels(attribution=_attribution)


# ---------------------------------------------------------------------------
# THE FORGED-PROJECT DETECTOR, AND WHY IT IS THREE SERIES AND NOT ONE.
#
# Measured on prod 2026-09-16 over a 24h window: 62 of 414 attributed rows
# carried a `project_id` BYTE-IDENTICAL to their own `tenant_id`, and those 62
# rows were $6.0458 of $9.7622 -- 15.0% of the rows and 61.9% of the money.
# One series cannot say that. `_rows` alone would have reported a small
# nuisance; `_spend_usd` alone would have reported a large one; only the pair
# says "a small number of rows carry most of the dollars", which is the
# sentence an operator has to act on. `_projects` is the third because the
# same forged id repeated 62 times and 62 distinct forged ids are the same row
# count and completely different defects (one emitter with one bug, versus a
# whole tenant population doing it).
#
# ⛔ NOTHING ELSE IN THE ESTATE DETECTS THIS CLASS.
# `billing_project_id_multi_tenant_projects` cannot: it looks for ONE project
# under TWO tenants, and a tenant stamping its OWN tenant_id as the project is
# one project under exactly one tenant -- perfectly clean by that test.
# `billing_cost_daily_project_attribution_rows` cannot either: the row IS
# attributed, to a non-sentinel uuid, so it counts as COVERAGE. A forged id of
# this shape therefore reads as health on both existing project gauges
# simultaneously, which is why it survived unnoticed.
#
# ⚠️ A tenant_id is NOT a project id, even when the caller means "all of it".
# The whole tenant is the rollup's own parent grain; re-asserting it on the
# project axis produces a project that does not exist in any project registry,
# so every per-project report silently invents a line item. The correct way to
# say "this spend is not project-scoped" is to leave project_id NULL, which is
# what `billing_cost_events_project_spend_usd`'s `tenant_scoped` bucket counts.
#
# ⛔ LEVELS OVER THE AUDIT WINDOW, NOT COUNTERS -- all three. `rate()` and
# `increase()` on any of them are NONSENSE: the value falls back to zero the
# moment the offending rows age out of the window, so a rate would report a
# NEGATIVE derivative as a counter reset and an increase() would invent traffic
# that never happened. Alert on `> 0`. Written by the LEADER only, so aggregate
# with max(), NEVER sum(), and read with
# `and on(instance) billing:attribution_audit:fresh` like every other gauge
# `services.attribution_audit` publishes.
#
# ⚠️ MEASURED WITHOUT the `excluded_from_billing = false` filter the money
# queries carry -- see `_PROJECT_SELF_TENANT_SQL`'s own banner. These audit
# CALLER BEHAVIOUR, so `_spend_usd` is "dollars that arrived carrying a forged
# project id", NOT "dollars an invoice will contain". Do not reconcile it
# against an invoice total; it can legitimately exceed one.
project_id_equals_tenant_projects = Gauge(
    "billing_project_id_equals_tenant_projects",
    "DISTINCT `project_id`s inside the attribution audit's window that are "
    "byte-identical to the `tenant_id` on the same row -- i.e. a caller that "
    "stamped its own tenant uuid on the project axis instead of a real project "
    "(or instead of leaving it NULL). "
    "⛔ INVISIBLE TO BOTH OTHER PROJECT GAUGES: it is one project under exactly "
    "one tenant, so billing_project_id_multi_tenant_projects reads 0, and it is "
    "a non-sentinel uuid, so billing_cost_daily_project_attribution_rows counts "
    "it as ATTRIBUTED. A forged id of this shape reads as health on both. "
    "Read this beside billing_project_id_equals_tenant_rows and "
    "billing_project_id_equals_tenant_spend_usd: one forged id on 62 rows and "
    "62 forged ids on 62 rows are the same row count and different defects. "
    "⛔ A LEVEL OVER THE WINDOW, NOT A COUNTER -- `rate()`/`increase()` on it "
    "are meaningless; it falls to 0 when the rows age out. Alert on `> 0`. "
    "Leader-written: aggregate with max(), NEVER sum(), and gate on "
    "billing:attribution_audit:fresh. "
    "⚠️ WHOLLY UNLABELLED, on purpose -- project_id and tenant_id are unbounded "
    "customer-supplied uuids; a label on either is a cardinality bomb and "
    "publishes one tenant's identifiers on an endpoint every tenant reads. The "
    "uuids belong on the audit's log line.",
)

project_id_equals_tenant_rows = Gauge(
    "billing_project_id_equals_tenant_rows",
    "billing_cost_events rows inside the attribution audit's window whose "
    "`project_id` equals their own `tenant_id`. The row-count half of the "
    "forged-project detector. "
    "⚠️ ROWS, NOT MONEY -- read it beside "
    "billing_project_id_equals_tenant_spend_usd, never instead of it. Measured "
    "on prod 2026-09-16: 62 rows (15.0% of attributed rows) carried 61.9% of "
    "the attributed dollars, so this series ALONE understates the defect by 4x. "
    "⛔ A LEVEL OVER THE WINDOW, NOT A COUNTER -- `rate()`/`increase()` on it "
    "are meaningless. Alert on `> 0`. Leader-written: max(), never sum().",
)

project_id_equals_tenant_spend_usd = Gauge(
    "billing_project_id_equals_tenant_spend_usd",
    "USD summed over the billing_cost_events rows inside the attribution "
    "audit's window whose `project_id` equals their own `tenant_id`. UNIT: US "
    "DOLLARS, not rows -- the dollar half of the forged-project detector, and "
    "the reason it exists is that its row-count sibling understated the same "
    "defect 4x on prod (15.0% of rows, 61.9% of the dollars). "
    "⚠️ NOT AN INVOICE FIGURE. Measured WITHOUT the `excluded_from_billing = "
    "false` filter every money query carries, because this audits CALLER "
    "BEHAVIOUR: a forged project id is the same evidence whether or not the row "
    "it rode in on was later parked as a duplicate, and filtering would let a "
    "forger hide behind the dedupe. So this is 'dollars that ARRIVED carrying a "
    "forged project id' and may legitimately exceed any invoice total. Do not "
    "reconcile it against one. "
    "⛔ A LEVEL OVER THE WINDOW, NOT A COUNTER -- `rate()`/`increase()` on it "
    "are meaningless. Alert on `> 0`. Leader-written: max(), never sum().",
)

# Explicitly born at zero, all three. An unlabelled gauge would export 0.0 from
# process start anyway; stating it makes "no caller forged a project id" a
# readable number rather than an absent series, and keeps the three in lockstep
# so a reader never sees rows without its dollars.
project_id_equals_tenant_projects.set(0)
project_id_equals_tenant_rows.set(0)
project_id_equals_tenant_spend_usd.set(0)


#: The three states a cost EVENT's spend attribution can be in. Bounded, exactly
#: three, and MUTUALLY EXCLUSIVE BY CONSTRUCTION -- see
#: `_PROJECT_SPEND_COVERAGE_SQL`, where they are three FILTERs over one scan
#: whose predicates are exact complements. All three must be born: a missing
#: `unattributed` series would read as full coverage, which is the exact failure
#: this gauge exists to end.
#:
#: ⚠️ DELIBERATELY NOT `PROJECT_ATTRIBUTION_STATES`. That tuple has two states
#: over the ROLLUP, whose project column is NOT NULL and carries a sentinel;
#: this one has three over the RAW EVENTS, whose project column is nullable and
#: where "no project, and the caller SAID so" is a different and much better
#: state than "no project, and nobody said anything". Merging them would erase
#: that distinction.
PROJECT_SPEND_STATES: tuple[str, ...] = ("project", "tenant_scoped", "unattributed")

cost_events_project_spend_usd = Gauge(
    "billing_cost_events_project_spend_usd",
    "USD of billable billing_cost_events spend inside the attribution audit's "
    "window, split by how it is attributed on the project axis. UNIT: US "
    "DOLLARS. "
    "⭐ THE WHOLE REASON THIS EXISTS: its row-counting sibling "
    "billing_cost_daily_project_attribution_rows counts ROWS, and a row share "
    "is not a spend share. This estate has already seen 80% of rows be 0.5% of "
    "the bill, and the project axis has already seen the reverse -- 15.0% of "
    "rows carrying 61.9% of the dollars (prod, 2026-09-16). Anyone quoting "
    "project coverage as a percentage of spend needs THIS series; quoting the "
    "row gauge for it is the false finding this was written to prevent, and the "
    "row gauge had to be re-read today to avoid exactly that. "
    "attribution='project': project_id IS NOT NULL -- the caller named a "
    "project. attribution='tenant_scoped': project_id IS NULL and the event "
    "metadata carries spend_scope='tenant' -- the caller EXPLICITLY said this "
    "spend belongs to the whole tenant, which is a correct and complete "
    "attribution, not a gap. attribution='unattributed': everything else -- no "
    "project and nothing said about it, which is the only bucket that is "
    "actually a coverage gap. "
    "⚠️ tenant_scoped LEGITIMATELY READS 0 until emitters start sending the "
    "spend_scope header; a zero there is a measurement, not a fault, and it "
    "does NOT mean the bucket is broken. "
    "⭐ THE THREE BUCKETS PARTITION THE WINDOW EXACTLY: their predicates are "
    "exact complements over one scan, so sum without `attribution` equals the "
    "window's billable total. That identity is the integrity check -- if it "
    "ever fails, a bucket is dropping rows. "
    "⚠️ BILLABLE ONLY (`excluded_from_billing = false`), unlike the "
    "billing_project_id_equals_tenant_* family beside it: this one is a MONEY "
    "measurement, so it must see only what an invoice can contain, and a parked "
    "duplicate counted here would double the same dollar. "
    "⛔ A LEVEL OVER THE WINDOW, NOT A COUNTER -- `rate()`/`increase()` are "
    "meaningless. Leader-written: aggregate with max(), NEVER sum() across "
    "instances, and gate on billing:attribution_audit:fresh.",
    ["attribution"],
)

# Born at zero for all three states. Birthing `unattributed` is the load-bearing
# one: with only `project` published, a service that has attributed nothing and
# one that has attributed everything are the same reading.
for _spend_state in PROJECT_SPEND_STATES:
    cost_events_project_spend_usd.labels(attribution=_spend_state)


# ---------------------------------------------------------------------------
# PER-EMITTER ATTRIBUTION COVERAGE -- the instrument every attribution fix is
# graded against.
#
# Two COUNTS per (service_key, axis), never a ratio: the ratio is taken in
# PromQL, where an emitter with no events is simply absent instead of a NaN
# (0/0) that a panel renders as a gap and an alert silently skips.
#
#   covered / coverage  per service_key, per axis
#
# ⚠️ axis='project' IS STRICTLY `project_id IS NOT NULL`. The `tenant_scoped`
# bucket of billing_cost_events_project_spend_usd (project_id NULL + the
# caller's explicit spend_scope='tenant') is a COMPLETE attribution but it is
# NOT project coverage -- its rows have no project id -- so it is counted as
# UNcovered here. Read the two together: this says which emitters stamp a
# project; the spend gauge says how many dollars are knowingly tenant-wide.
#
# ⚠️ ROLLING window (`created_at >= now - window_hours`) over the RAW events,
# never the UTC `day_floor` the rollup-row gauge uses: a calendar-day floor
# resets the population at midnight UTC and reads as a coverage step change
# that no emitter caused.
#
# ⚠️ BILLABLE ROWS ONLY (`excluded_from_billing = false`), the same population
# as billing_cost_events_project_spend_usd, so a parked duplicate cannot count
# the same physical call twice.
#
# ⛔ NO tenant label: tenant_id is an unbounded customer uuid (cardinality, and
# one tenant's ids on an endpoint every tenant reads). service_key is already a
# label on billing_cost_events_by_service_total.
#
# ⛔ LEVELS OVER THE WINDOW, NOT COUNTERS -- rate()/increase() are meaningless.
# Leader-written by services.attribution_audit: aggregate with max(), NEVER
# sum(), and gate on billing:attribution_audit:fresh. A service_key that leaves
# the window has its series REMOVED on the next pass (not zeroed), so a
# retired emitter cannot hold a stale ratio forever.
# ---------------------------------------------------------------------------

#: The two attribution axes measured per emitter. Bounded.
ATTRIBUTION_COVERAGE_AXES: tuple[str, ...] = ("project", "actor")

attribution_coverage_events = Gauge(
    "billing_attribution_coverage_events",
    "DENOMINATOR: billable billing_cost_events rows inside the attribution "
    "audit's ROLLING window (created_at >= now - window_hours, default 24h), per "
    "emitting service_key. Identical for both axis values -- the label exists so "
    "`billing_attribution_covered_events / billing_attribution_coverage_events` "
    "matches one-to-one on (service_key, axis). Counts, not a ratio: compute the "
    "ratio in PromQL. Leader-written level: max(), never sum(); gate on "
    "billing:attribution_audit:fresh. Series for a service_key absent from the "
    "window are removed.",
    ["service_key", "axis"],
)

attribution_covered_events = Gauge(
    "billing_attribution_covered_events",
    "NUMERATOR: of billing_attribution_coverage_events, the rows attributed on "
    "`axis`. axis='project': project_id IS NOT NULL (STRICT -- tenant_scoped "
    "spend, i.e. project_id NULL with spend_scope='tenant', is NOT project "
    "coverage; see billing_cost_events_project_spend_usd for that bucket). "
    "axis='actor': actor_id IS NOT NULL. Same rolling window, same billable "
    "filter, same statement as the denominator. Leader-written level: max(), "
    "never sum(); gate on billing:attribution_audit:fresh.",
    ["service_key", "axis"],
)


# ── request_quota_config value repair loop (services/quota_config_sync.py) ──
QUOTA_CONFIG_VALUE_REPAIR_FAILURE_REASONS = ("licences_unreadable", "infra_error", "unexpected")
quota_config_value_repair_failures_total = Counter(
    "billing_quota_config_value_repair_failures_total",
    "Quota-config value repair ticks that ended in an error, by reason. "
    "`licences_unreadable` -- the registry read was incomplete, so NOTHING was "
    "written; `infra_error` -- a database error; `unexpected` -- a bug in the loop.",
    ["reason"],
)
for _reason in QUOTA_CONFIG_VALUE_REPAIR_FAILURE_REASONS:
    quota_config_value_repair_failures_total.labels(reason=_reason)
quota_config_value_repair_repaired_total = Counter(
    "billing_quota_config_value_repair_repaired_total",
    "request_quota_config rows whose quota_monthly the repair loop moved to the "
    "registry's effective limit (quota_override wins over plan.quota_monthly).",
)
quota_config_value_repair_last_success_timestamp_seconds = Gauge(
    "billing_quota_config_value_repair_last_success_timestamp_seconds",
    "Unix time of the last completed quota-config value repair tick on the leader. "
    "0 = never completed on this replica (a follower stays 0).",
)
quota_config_value_repair_last_success_timestamp_seconds.set(0)
quota_config_value_repair_is_leader = Gauge(
    "billing_quota_config_value_repair_is_leader",
    "1 on the replica holding the quota-config value repair lease, 0 otherwise.",
)
quota_config_value_repair_is_leader.set(0)
