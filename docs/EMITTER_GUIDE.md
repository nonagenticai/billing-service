# Metering Emitter Guide

This is the contract every service in the platform follows to record usage
+ cost into metering. The same shape works for LLM calls, VLM calls, image
generation, TTS, transcription, or any other paid/measurable workload. If
you're wiring a new app (w2v service, fr-backend new feature, etc.) into
metering, read this — don't crib from inference-router/services/metering.py
because it carries router-specific concerns you don't need.

## The two streams

billing-service consumes two Redis streams. **You only ever write to one of them.**

⚠️ **The stream names changed on 2026-06-25 and this guide was not updated until
now.** `metering:events` / `metering:costs` are **dead names**. Nothing reads
them — `src/services/consumer.py` subscribes to exactly `settings.events_stream`
and `settings.costs_stream`, and `metering:` appears nowhere in `src/`. An
emitter that follows the old guide XADDs into a stream with no consumer and its
usage **never appears anywhere, with no error**. If you find `metering:*` in a
code comment elsewhere in the estate, that comment is stale too — check the
behaviour, not the prose.

| Stream           | What it carries                          | Who writes today                     |
|------------------|------------------------------------------|--------------------------------------|
| `billing:events` | API request envelopes (count + denial)   | **fr-backend (~99.5%)**, license-authz |
| `billing:costs`  | Cost events (token counts, USD, latency) | inference-router, you                |

⚠️ **`billing:events` is NOT "owned by license-authz".** That claim stood in this
guide and it is wrong: measured 2026-09-03,
`select source_ip, service_key, count(*) from billing_metering_events where
tenant_id='00000000-0000-0000-0000-000000000000' group by 1,2` returns
**fr-backend 47,934 rows (99.5%)** against 221 for license-authz. fr writes via
`withUsageSpans`, which is installed **globally**, so every route it fronts is
metered — including routes that authenticate with a shared secret rather than a
Keycloak token and therefore carry no tenant at all. Treat this stream as having
**two producers with different identity guarantees**, not one.

You still do NOT write to `billing:events` yourself. If you are called through
license-authz the request is already metered.

`billing:costs` is the one to write to when your app consumes external provider
quota (Sora, ElevenLabs, OpenAI image, Anthropic, etc.) or local-infra quota
(vLLM, mflux, MLX) and you want it on the dashboard.

⚠️ **Set the stream name from env and set it explicitly in your deployment.** A
code default is not enough: `inference-router` defaults to
`improvement-loop:costs` in `settings.py` and only reaches billing because its
Deployment sets `INFERENCE_ROUTER_METERING_COSTS_STREAM=billing:costs`. Verify
the live pod env, not the default.

## The event shape (canonical)

Every record on `billing:costs` is a flat string→string map. Required
fields and their semantics:

| Field                          | Required | Type   | Notes                                                                |
|--------------------------------|----------|--------|----------------------------------------------------------------------|
| `tenant_id`                    | yes      | string | Customer tenant UUID — from `X-Tenant-Id` header. **When no tenant resolves, send the nil-UUID sentinel `00000000-0000-0000-0000-000000000000` (NOT `""`).** See "Unattributed tenant" below. |
| `license_id`                   | yes      | string | UUID of the active license. From `X-License-Id` header.              |
| `subject_id`                   | yes      | string | ⛔ **WHO PAYS: the licences-service Subject UUID of the licence holder — NOT the Keycloak `sub`.** (Corrected 2026-09-17: this row said "Keycloak `sub`", which contradicts `CostEvent.subject_id` in `src/models/db/events.py`; an emitter that followed it wrote subs into a Subject-UUID column.) Empty for service-to-service calls with no licence. |
| `actor_id`                     | no       | string | **WHO ACTED: the verified Keycloak `sub` of the person who initiated the work.** Distinct from `subject_id` — a service licence can pay for a human's call. Empty for platform/system work, where no person acted. Rolled up separately; never defaulted. |
| `app_id`                       | yes      | string | UUID of the app the user is in. From `X-App-Id` header.              |
| `service_key`                  | yes      | string | Short stable id of your service, e.g. `w2v-tts`, `fr-agent`.         |
| `cost_source`                  | yes      | string | `inference-router` \| `worker-management` \| `w2v-<svc>` \| ...      |
| `provider`                     | yes      | string | External provider name (`openai`, `anthropic`, `elevenlabs`) or local infra (`vllm`, `mflux`). |
| `model`                        | yes      | string | Provider's model id (`gpt-4o-mini`, `claude-sonnet-4-...`, `tts-1`). |
| `served_model`                 | no       | string | If you aliased the model (e.g. `deepseek-v4-flash → deepseek-chat`), put the resolved id here. Defaults to `model`. |
| `input_tokens`                 | yes      | int    | For non-text workloads, use the natural unit (audio seconds, image megapixels) cast to int. Document the unit in `usage_extra`. |
| `output_tokens`                | yes      | int    | Same.                                                                |
| `cache_creation_input_tokens`  | no       | int    | Anthropic prompt-cache write. Default 0.                             |
| `cache_read_input_tokens`      | no       | int    | Anthropic prompt-cache hit. Default 0.                               |
| `cost_usd`                     | no       | float8 | Your locally-computed USD. metering recomputes from `provider_pricing` as source of truth, so this is a cross-check only. Omit (or "0") for non-LLM workloads where pricing isn't centrally configured yet. |
| `latency_ms`                   | yes      | int    | Provider response time. Useful for SLO analytics.                    |
| `provider_request_id`          | no       | string | Provider's correlation id (Anthropic `request-id`, OpenAI `x-request-id`). Empty = "".  |
| `usage_extra`                  | no       | json   | JSON-encoded dict for unit/dimension metadata: `{"unit":"audio_seconds","duration_s":12.5}`. Default `{}`. ⭐ Ownership flags the rollup reads: `{"byo": true}` = the TENANT's own provider credential paid (billed only the markup under `byo_markup_only`); `{"owner_provided": true}` = a managed-BYO/owner credential. Set at most one. `{"origin": "system"}` marks platform-initiated work so it is not mistaken for a member whose attribution went missing. |
| `vendor_cost_usd`              | no       | float8 | The PROVIDER's own reported charge for this call (e.g. OpenRouter `usage.cost`), when it reports one. Never your price-card estimate — omit rather than guess. |
| `event_key`                    | no       | string | Producer-minted idempotency key, unique per physical call (e.g. your local ledger row id). Billing dedups on it; a content hash is refused. Send it whenever you can re-send (outbox / retry). |
| `timestamp`                    | yes      | string | ISO-8601 UTC (`datetime.now(UTC).isoformat()`).                      |

If your workload doesn't have natural "tokens" (image gen, TTS, video) —
use input_tokens/output_tokens as numeric proxies (e.g. width×height for
image; characters for TTS) and record the real unit in `usage_extra`.
billing-service treats them as opaque numeric quantities for rollups;
the dashboard renders them generically.

## Unattributed tenant — fail safe to the nil sentinel, never `""`

When a request carries no resolvable tenant (internal/trusted-network call,
auth-middleware edge, missing `X-Tenant-Id`), emit the **nil-UUID sentinel**:

```python
UNATTRIBUTED_TENANT_ID = "00000000-0000-0000-0000-000000000000"
DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", UNATTRIBUTED_TENANT_ID)
# at the call site / header default / param default:
tenant_id = resolved_tenant or DEFAULT_TENANT_ID
```

⚠️ **CORRECTED 2026-10-15 — this section used to say an unattributed event is
stored as a nil-tenant row "kept for audit, not billed". That has been false
since 2026-08-26.** The flusher runs `validate_cost_event`
(`src/services/_cost_schema.py`) on every event *before* the insert
(`src/services/cost_flusher.py`, `flush_costs`), and an event whose tenant does
not resolve to a real tenant is **dead-lettered to `billing:costs:dead`, not
stored**. No `billing_cost_events` row exists for it.

The sentinel is still the right thing to send — it is the honest "I have no
tenant", it can never resolve to somebody else's tenant, and it gets its own
dead-letter reason so it is distinguishable from a broken emitter. But it does
**not** get your spend recorded. What happens to each value
(resolution in `src/tenancy.py`, `canonical_tenant_id`):

| Emitted `tenant_id`                        | Outcome | `billing_cost_dead_letter_total{reason}` | Billed? |
|--------------------------------------------|---------|------------------------------------------|---------|
| nil UUID `00000000-…-0`                     | dead-lettered, no row | `nil_tenant_id` | **NO** |
| `""` / `None` / absent                      | dead-lettered, no row | `missing_tenant_id` | **NO** |
| credential-shaped (`Bearer …`, >48 chars, whitespace) | dead-lettered, no row | `invalid_tenant_id` | **NO** — a leaked token is never stored |
| any slug not in `LEGACY_TENANT_NAMES` (`"anonymous"`, typo) | dead-lettered, no row | `invalid_tenant_id` | **NO** |
| a **registered** legacy name (`"n8n-dev"`)  | stored under that tenant's registered UUID, `tenant_name` = the name | — | **YES** |
| real tenant UUID, **registered** in licences | stored, canonical lowercase | — | **YES** |
| well-formed UUID **not** in the licences registry | stored but **parked**: `excluded_from_billing = true`, `exclusion_reason = 'unregistered_tenant'` | — (`billing_cost_events_unregistered_tenant_total{action="excluded"}`) | **NO** — withheld from the rollup and the budget debit; reversible with one `UPDATE` |

The last row is the unregistered-tenant gate, **armed in production since
2026-09-16** (`BILLING_EXCLUDE_UNREGISTERED_TENANTS=true` in the gitops
Deployment); see `docs/UNREGISTERED_TENANT_GATE.md`. A random UUID in
`X-Tenant-Id` no longer mints a billable tenant.

Dead-lettered events are **not lost immediately but are not safe either**: the
dead-letter stream is age-trimmed at `dead_letter_stream_max_age_hours`
(default 720h = 30 days). Inside that window a nil/blank-tenant entry can be
moved to a registered tenant with `POST /api/admin/dlq/reattribute`, and a
fixed-producer entry replayed with `POST /api/admin/dlq/replay`; after it the
money record is gone. Triage: `docs/RUNBOOK.md` § *BillingCostDeadLetter*.

Practical consequence for an emitter: **resolve the tenant**. Sending the nil
sentinel is the safe failure, not a way to get spend recorded — send the
tenant whose licence pays for the call.

The one remaining way to **silently mis-bill** is emitting a real fallback
UUID (e.g. the n8n-dev tenant's uuid) for unattributed traffic: that resolves,
so it bills a real tenant for traffic that was not theirs. Default the
env-driven `DEFAULT_TENANT_ID` to the nil sentinel so an emitter fails safe
even when the env var is unset. Reference implementations:
`mlx_ingestion_backend`, `video_compiler_service`, `inference-router`,
`w2v_common` (auth middleware).

Note for callers sending `X-Tenant-Id` over HTTP rather than emitting to the
stream: `src/auth_deps.py` returns **400** for a header that does not resolve.
A slug that is not in `LEGACY_TENANT_NAMES` fails the request.

`BILLING_TENANT_ID_CLASSIFICATION_ENFORCE` is a second, stricter switch that
would also *reject* (400 / dead-letter) a well-formed but unregistered UUID
instead of parking it. It is **deliberately off**: the park above already keeps
that spend out of billing reversibly, and rejecting it would destroy the money
record. Before ever arming it, review its shadow counter
`billing_tenant_id_classification_rejected_total` by class over a window with
real traffic.

## Minimum viable emitter

The whole emitter is ~30 lines of Python. Wrap it in
`asyncio.create_task(emit(...))` so it never blocks your hot path; the
write is fire-and-forget by design and any error is logged + dropped.

```python
import json
import os
from datetime import UTC, datetime

import redis.asyncio as aioredis
import structlog

logger = structlog.get_logger(__name__)
REDIS = aioredis.from_url(os.environ["METERING_REDIS_URL"], decode_responses=True)
STREAM = os.environ.get("METERING_COSTS_STREAM", "billing:costs")
# ⚠️ "your-service-tts" below is a PLACEHOLDER, not a value to copy-paste.
# A prior revision of this example used the real, already-owned "w2v-tts"
# (see the registry table further down: owned by w2v tts_service) here, and
# that is exactly how it ended up on TWO services at once --
# modality_gateway/src/services/metering.py and
# tts_backend/src/services/metering.py both shipped with the identical
# literal, so a cost_daily row carrying it cannot say which one served it.
# Pick your OWN slug and register it in the table below before you ship.
COST_SOURCE = "your-service-tts"  # Set per service. See registry below.


async def emit_cost(
    *,
    tenant_id: str,
    license_id: str,
    subject_id: str,
    app_id: str,
    service_key: str,
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    latency_ms: int,
    cost_usd: float = 0.0,
    usage_extra: dict | None = None,
) -> None:
    """Fire-and-forget cost event. Never raises; logs and drops on failure."""
    try:
        fields = {
            "tenant_id": tenant_id,
            "license_id": license_id,
            "subject_id": subject_id,
            "app_id": app_id,
            "service_key": service_key,
            "cost_source": COST_SOURCE,
            "provider": provider,
            "model": model,
            "served_model": model,
            "input_tokens": str(input_tokens),
            "output_tokens": str(output_tokens),
            "cost_usd": f"{cost_usd:.8f}",
            "latency_ms": str(latency_ms),
            "provider_request_id": "",
            "usage_extra": json.dumps(usage_extra or {}, sort_keys=True),
            "timestamp": datetime.now(UTC).isoformat(),
        }
        await REDIS.xadd(STREAM, fields, maxlen=100_000, approximate=True)
    except Exception:  # noqa: BLE001 — fire-and-forget; never block the hot path
        logger.warning("cost_event_emit_failed", exc_info=True)
```

Call it from your provider wrapper, e.g.:

```python
import asyncio
import time


async def synthesize_speech(text: str, ctx: RequestContext) -> bytes:
    t0 = time.monotonic()
    audio, request_id = await elevenlabs.synthesize(text)
    asyncio.create_task(
        emit_cost(
            tenant_id=ctx.tenant_id,
            license_id=ctx.license_id,
            subject_id=ctx.subject_id,
            app_id=ctx.app_id,
            service_key="your-service-tts",  # pragma: allowlist secret -- PLACEHOLDER, pick your own
            provider="elevenlabs",
            model="eleven_multilingual_v2",
            input_tokens=len(text),
            output_tokens=0,
            latency_ms=int((time.monotonic() - t0) * 1000),
            usage_extra={"unit": "characters", "audio_bytes": len(audio)},
        )
    )
    return audio
```

## What you MUST capture; what you MUST NOT compute

**MUST capture** at the call site (these can't be reconstructed later):
`tenant_id`, `license_id`, `subject_id`, `app_id`, `provider`, `model`,
`input_tokens`, `output_tokens`, `latency_ms`, `service_key`, `cost_source`,
`provider_request_id` if the provider supplies one.

**MUST NOT compute** (or rather, your number is treated as advisory):
- **USD cost.** billing-service is the source of truth for USD. It joins
  your event to the effective-dated `provider_pricing` table by
  `(provider, served_model)` and recomputes USD from token counts. Your
  local `cost_usd` is a sanity check, not the billing number. For
  workloads without a `provider_pricing` row yet, just send `0`.
- **Markup / billed cost.** Markup is per-tenant and applied at the
  read-path (dashboards) or at `/api/billing/*`. Never emit a marked-up
  number; never bake markup into `cost_usd`. The dashboard does this.

## `cost_source` registry (reserved values)

| Value                  | Owner                           |
|------------------------|---------------------------------|
| `inference-router`     | inference-router                |
| `worker-management`    | worker-management               |
| `fr-direct`            | fr-backend (rare; usually routes via inference-router) |
| `w2v-ai-content`       | w2v ai_content_generation_service — ⏳ RESERVED, has never emitted (0 rows, all time) |
| `w2v-tts`              | w2v tts_service `src/services/metering.py:25` — ✅ LIVE (2,773 rows / $1.47). ⚠️ also the `COST_SOURCE` of modality_gateway's BYO leg (`src/services/metering.py:55`), whose `service_key` is `w2v-tts-byo` |
| `w2v-image-gen`        | w2v image_gen_backend `src/services/metering.py:44` — ✅ LIVE (73 rows / $0.73). ⚠️ its `service_key` is `w2v-image` (`:50`), NOT this value |
| `w2v-transcription`    | w2v transcription_worker_service — ⏳ RESERVED, has never emitted (0 rows, all time) |
| `w2v-video-analyzer`   | w2v video_analyzer_service `src/services/metering.py:48` — ✅ LIVE (486 rows / $4.22) |
| `w2v-video-compiler`   | w2v video_compiler_service `src/services/metering.py:45` — ✅ LIVE (318 rows / $168.69) |
| `w2v-html-render`      | w2v html_render_service `src/services/metering.py:40` — ✅ LIVE (36 rows / $0.72, since 2026-09-13) |
| `mlx-backend`          | mlx_backend `src/services/metering.py:62` — ✅ LIVE (194,752 rows / $6.41), the highest-volume emitter in the ledger |
| `mlx-ingestion-backend`| mlx_ingestion_backend `src/services/metering.py:67` — ✅ LIVE (318 rows / $0.11) |
| `kag`                  | kag `src/services/metering.py:186` — ✅ LIVE (6,588 rows / $76.02) |
| `kag-graph`            | kag_graph_service `src/services/metering.py:107` — ✅ LIVE (131 rows / $1.51) |
| `hirag`                | hirag_service `src/services/metering.py:119` — ✅ LIVE (60 rows / $3.69) |
| `karma`                | karma_service `src/services/metering.py:78` — ✅ LIVE (72 rows / $1.66) |
| `ingestion`            | ingestion_service `src/services/scrape_metering.py:129` — ✅ LIVE (319 rows / $0.74) |
| `w2v-tts-byo`          | modality_gateway `src/services/metering.py:56` — ⏳ ARMED, 0 rows. This is a **`service_key`**, not a `cost_source`: the same module pins `COST_SOURCE = "w2v-tts"` (`:55`) so the BYO leg and tts_backend share a cost_source but never a service_key. Reserved here so nothing else claims it |
| `w2v-asr`              | modality_gateway `src/routers/asr_routes.py:41` — ⏳ ARMED, 0 rows. `service_key` only; the BYO emit path sets `cost_source=route.cost_source` (`src/services/byo_cost.py:62`), i.e. per-route config, so no fixed `cost_source` literal exists |
| `w2v-embedding`        | modality_gateway `src/routers/embedding_routes.py:35` + `src/routers/ml_embedding_routes.py:47` — ⏳ ARMED, 0 rows. Same `service_key`-only shape as `w2v-asr` |
| `w2v-vlm`              | modality_gateway `src/routers/vlm_routes.py:47` — ⏳ ARMED, 0 rows. ⚠️ already has 4 `provider_pricing` rows keyed on this `service_key`: priced, no app, no events |
| `commonpractice`       | commonpractice `backend/src/host/metering-emit.ts` (`COST_SOURCE`/`SERVICE_KEY`) via a transactional outbox (`backend/src/host/metering-outbox-drainer.ts`). Sends `license_id` (the member's own licence), `subject_id` = licences Subject UUID, `actor_id` = Keycloak sub, `event_key` = `usage_events.id`, `usage_extra.byo` for a tenant key. ⛔ **Sole emitter for its LLM calls:** CP's default LLM is inference-router, which emits only when a caller presents `X-Tenant-Id` — CP must never forward identity headers to it, or every call is billed twice (pinned by `backend/tests/inference-router-single-emitter.test.ts`). `app_id` is sent empty on purpose (billing derives it from the licence). |
| `strata-rag`           | strata_rag (Rust, ns `strata-rag`) `src/metering.rs` — ⏳ RESERVED, emitter in flight (strata_rag `feat/rag-chat-hnsw-reembed-metering-cache`), 0 rows. Fire-and-forget `XADD billing:costs`; no-op while `METERING_REDIS_URL` is unset. One event per provider call; `service_key` = `strata-rag-<cap>` (`strata-rag-embed` / `strata-rag-rerank` / `strata-rag-chat`), `provider`/`model` = the provider that served it. `funding`: `local_gpu` for mlx_api / vLLM (no `provider_pricing` row → $0), `tenant_byo` + `usage_extra.byo` for a tenant-registered provider, else `platform_external`. `project_id` from `X-Project-Id`, else the tenant-level project; `app_id`/`actor_id`/`license_id` only when the caller sends `X-App-Id`/`X-Actor-Id`/`X-License-Id`; `event_key` = uuid7. Query-embedding cache hits emit nothing |
| `strata-ingest`        | strata_ingest `src/metering.rs:35` (Rust, ns `strata-ingest`; strata_ingest #7) — ⏳ ARMED, 0 rows. `service_key` = `strata-ingest-ocr` / `strata-ingest-vision` / `strata-ingest-transcribe`. Fire-and-forget XADD after the step commit (fenced per attempt), so a Redis outage DROPS the event (logged). `event_key` = `<job_id>:<kind>/<key>:<attempt>`. OCR pages carry no `quantity_unit` (`usage_extra.unit="pages"`; billing prices them via a `per_page` row and stamps `quantity_unit=pages`, and `pages` is also accepted on the wire since 2026-10-18); `license_id`/`subject_id`/`app_id` empty (tenant comes from a static API token); `funding` = `tenant_byo` for tenant-registered providers, `local_gpu` for in-cluster/private hosts (provider `mlx-api`, priced per model in `scripts/initial_pricing.yaml`), else `platform_external`. |
| (your service)         | (pick a `w2v-*` or `<app>-<svc>` slug; document here in your PR) |

All file:line references verified against each repo's `origin/main`, and all
counts measured on prod `billing_cost_events` on **2026-09-16** (whole table,
not a window). ✅ LIVE = has emitted; ⏳ = registered/armed but zero rows.

Pick one before you ship. The dashboard uses `cost_source` to attribute
spend to a service when `provider` alone is ambiguous (multiple services
calling OpenAI, for example).

### Three values in this column that are NOT service slugs

Do not register these and do not copy them into an emitter:

- **`unknown`** — billing's own fallback for an event that arrived with no
  `cost_source` at all. 23,382 rows, all `service_key = inference-router`,
  all between 2026-09-05 and 2026-09-07; nothing has produced it since.
- **`self_hosted`** — a PROVIDER-shaped value the kag family emitted before it
  began naming itself. 1,180 rows across `kag`/`kag-graph`/`karma`/`ingestion`,
  last seen 2026-09-08; superseded by the four slugs above. Retired, not
  reserved.
- **`<slug>+resolved`** — a suffix *billing* appends (`src/services/cost_flusher.py:957`)
  when it DERIVED the licence for a licence-less row. It is never on the wire,
  so `cost_source = 'x'` and `cost_source = 'x+resolved'` are the same emitter;
  query with `split_part(cost_source, '+', 1)` when you want the producer.

## Known discrepancies: `service_key` -> app (investigated 2026-09-16, deliberately NOT fixed)

Two readings of `cost_attribution_app_by_service_key` (`src/settings.py:403-516`)
look like the map and the data disagree. Neither does. **That map does not
assign `app_id` at all**, and mistaking it for the thing that does is the whole
confusion — recorded here so the next session does not re-derive it.

### What actually stamps `billing_cost_events.app_id`

Three sources, strict precedence, first hit wins:

1. **The wire.** `src/services/cost_flusher.py:666` reads `app_id` straight off
   the event. An app the caller genuinely presented is never overwritten —
   `src/services/cost_flusher.py:986` (`if row["app_id"] is not None: continue`).
2. **The event's LICENCE.** `src/services/cost_flusher.py:984-995` calls
   `resolve_app` (`src/services/license_resolver.py:370`) against a
   `license_id -> app_id` map built from the local licence mirror
   `request_quota_config` (`src/services/license_resolver.py:325`; ACTIVE
   licences carrying a non-NULL `app_id` only). `event_flusher` does the same at
   `src/services/event_flusher.py:628-644`.
3. **NULL, and counted** — `record_app_unattributed`. Never fabricated.

`cost_attribution_app_by_service_key` appears in NONE of those three. Its only
consumer is `resolve_license` (`src/services/cost_flusher.py:934-950`), gated on
`if row["license_id"] is not None: continue` (`src/services/cost_flusher.py:941`).
It fills a **missing licence**, never an app.

### Discrepancy 1 — analyzer/compiler are absent from the map yet carry the w2v app

Measured on prod, whole table, 2026-09-16:

| `service_key`        | rows | `app_id`    | rows with a `license_id` |
|----------------------|------|-------------|--------------------------|
| `w2v-video-analyzer` | 486  | `f825f6b0-bb2f-4386-bce2-e6091a7b98cd` | 486 of 486 |
| `w2v-video-compiler` | 318  | `f825f6b0-bb2f-4386-bce2-e6091a7b98cd` | 318 of 318 |
| `w2v-html-render`    | 36   | `f825f6b0-bb2f-4386-bce2-e6091a7b98cd` | 36 of 36  |

Every one of those rows carries licence
`019f7f7c-5006-72c0-8fa7-e6317340c0c9`, which `request_quota_config` maps to
app `f825f6b0-…` / **"Web to Video"**. So the app came from the LICENCE at
`src/services/cost_flusher.py:988`, not from any service_key mapping. **Zero of
these rows are licence-less**, so the map could not have run for these keys even
if the entries existed. Map absence is not a defect here; it is irrelevant.

### Discrepancy 2 — `w2v-image` IS in the map (-> Web to Video) yet its rows carry the `workers` app

73 rows, `app_id = 06954722-cebc-7637-881a-91e8f7cca802` (**Workers Module**),
and **73 of 73 carry a `license_id`** — licence
`019f414d-76bb-70d8-9ab9-cec8ebd1fba0`, which `request_quota_config` maps to
that same Workers Module app.

**Which wins at runtime: the LICENCE, and the map never competed.** Same gate at
`src/services/cost_flusher.py:941` — with no licence-NULL row, `resolve_license`
is skipped for every `w2v-image` event, so `w2v-image=Web to Video` has been
INERT for all 73 rows. The two answer different questions: the map says *which
licence* to attribute a licence-LESS row to; `app_id` says *which app the row's
own licence belongs to*. The entry would only ever fire for a future `w2v-image`
event arriving with no licence — and then it would resolve a Web-to-Video
licence, producing a Web-to-Video `app_id` that disagrees with every row stored
so far. That latent split is the thing worth watching; it is not what is
happening today.

⛔ **Not fixed here, on purpose.** Editing `cost_attribution_app_by_service_key`
moves live attribution and is a deliberate decision, not a side effect of a docs
commit. See also the settings-level rule already recorded there: *the evidence is
the licence the emitting tenants actually hold, not the name* — a distinctly
named app with zero licence grants fleet-wide is a decoy, not a target.

### Why these services have no `licenses.app` row of their own — and why that is correct

⛔ **Do not insert rows into `licenses.app` for `w2v-video-analyzer`,
`w2v-video-compiler`, `w2v-html-render` or the other w2v sub-services.** The
umbrella attribution is **deliberate, not a fallback**: all three resolve
through ONE licence (`019f7f7c-…`) granting the single umbrella app
**"Web to Video"**, which is why 100% of their rows are attributed today.

A per-service app row does not attribute anything by existing. `resolve_app`
(`src/services/license_resolver.py:370-385`) keys on `license_id`, and
`_build_app_map` (`:325-349`) only admits licences that are ACTIVE *and* carry a
non-NULL `app_id`. So a new app row attributes a row only once a **licence for
that app** exists AND the emitting tenant holds it. Until both are true, minting
the app row and repointing attribution at it turns a 100%-attributed key into a
NULL-`app_id` key, and the spend stops reaching an invoice line. `src/settings.py`
already records this failure shape measured twice (the "W2V Image (P1 E2E)" and
"MLX Ingestion Backend" app rows, each with ZERO licence grants fleet-wide).

**This question is closed. Re-open it only as an explicit licensing decision**
— mint the app, grant the licences, migrate the tenants — never as a
side effect of registering a slug.

## Provider-pricing rows

If you add a new external provider that metering doesn't yet price, also
add a `provider_pricing` row so the recomputation produces a real USD
number rather than $0. Example (run as metering-internal SA):

```bash
curl -X POST https://metering.example.com/api/config/provider-pricing \
  -H "Authorization: Bearer $METERING_INTERNAL_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "elevenlabs",
    "model": "eleven_multilingual_v2",
    "input_per_1m_usd": 180.0,
    "output_per_1m_usd": 0.0,
    "effective_date": "2026-05-17",
    "service_key": "your-service-tts" # pragma: allowlist secret -- PLACEHOLDER, your own routing slug
  }'
```

For workloads priced per-second / per-image rather than per-token, encode
the rate in `input_per_1m_usd` against the unit you put in `input_tokens`
(e.g. cost per 1M audio seconds → cost per 1 audio second). Always
document the unit in `usage_extra.unit`.

## How to verify your emitter is working

After your service emits a few events, in one terminal tail the consumer
logs (metering pod) for the batch flush and in another psql against the
billing database (directly, or via pgbouncer):

```sql
-- Most recent cost events from your service:
SELECT created_at, tenant_id, provider, model, input_tokens, output_tokens, cost_usd
FROM cost_events
WHERE cost_source = 'your-service-tts'  -- your service's actual value
ORDER BY created_at DESC
LIMIT 20;

-- Did the daily rollup pick them up? (consumer flushes every 5s; rollup
-- runs every minute.)
SELECT bucket_date, provider, request_count, total_cost_usd
FROM cost_daily
WHERE cost_source = 'your-service-tts'
ORDER BY bucket_date DESC
LIMIT 7;
```

Then load the super-admin metering dashboard (fr `/admin/metering`):
- **By provider** section should list your `provider` with its incurred
  cost (markup-free).
- **By tenant** section should attribute that cost to the tenant_id you
  sent on the event, with the right markup applied to produce billed cost.
- **Grand total** should reflect the new spend.

If nothing appears after ~30s: check the consumer pod logs
(`billing:costs xreadgroup`) for parse errors. The most common mistake
is missing one of the required string fields (consumer drops the event
and logs a warning).

## Failure mode + back-pressure

The stream is bounded (`MAXLEN ~100_000` via approximate trimming). If
your service emits faster than the consumer drains, old events are
dropped from the head — not your newest ones. In practice the consumer
batch-flushes every 5s or 500 events, so a backlog only forms during a
metering-pod restart or a Postgres slowdown.

If you absolutely cannot afford to lose events (you usually can — the
dashboard is for spend analytics, not for accounting that has to balance
to the cent), don't reach for a different transport. Instead: persist a
local audit row alongside the XADD and have a reconciliation job
backfill into `cost_events` from there. That work isn't built today
and shouldn't be your first move — it's only needed when the answer to
"do we need exact reconciliation?" is yes.
