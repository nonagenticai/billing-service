# Fleet-wide LLM cost correlation

How any service attributes its LLM cost back to a single operation (a run, a
chat, a graph execution) and reads priced dollars back. Implemented for w2v
2026-06-28; this doc is the pointer for **fr** and **commonpractice** to adopt
and extend it.

## The standard

One header convention, fleet-wide: **`X-Correlation-Id`** (+ optional
**`X-Stage`**).

- A caller stamps `X-Correlation-Id` (its operation id) on every outbound LLM
  call. `X-Stage` optionally names a sub-step.
- The LLM emitter (worker_management for BYO, inference-router for platform)
  reads those headers and writes them onto the cost event it XADDs to the
  `metering:costs` Redis stream.
- billing-service's cost-flusher stores `correlation_id` (indexed column) and
  `stage` (in the `metadata` JSON), and **prices every event** from the
  effective-dated `provider_pricing` table.
- The caller reads its priced cost back from
  **`GET /api/costs/by-correlation?ids=<uuid,…>&group_by=stage,model,provider,correlation_id,day`**
  (tenant-scoped; admin JWT reads cross-tenant).

Everything is additive/optional: no header ⇒ empty correlation ⇒ today's
behaviour. Pricing is unchanged — billing remains the single source of truth.

## Pipeline (where each piece lives)

| Step | Service | Code |
|------|---------|------|
| stamp `X-Correlation-Id`/`X-Stage` outbound | caller | (w2v: `w2v_common` `identity_event_hook`; orchestrator sets it per DAG stage in `dag_executor`) |
| read inbound → ContextVars → emit on cost event | worker_management | `api/auth_middleware.py`, `api/identity_context.py`, `telemetry/cost_events.py` |
| emit on cost event (platform path) | inference-router | `routers/completions.py` → `services/{router_service,telemetry_sink,metering}.py` |
| forward downstream | worker_management → inference-router | `worker/agents/pydantic_ai_agent.py` outbound hook |
| store `correlation_id` + `stage`, price | billing-service | `services/cost_flusher.py`, `models/db/events.py` |
| read priced cost grouped | billing-service | `GET /api/costs/by-correlation` (`routers/costs.py`) |

## Reading cost back

`GET /api/costs/by-correlation`

- `ids` (required): comma-separated correlation UUIDs (≤500).
- `group_by` (optional): any of `stage, model, provider, correlation_id, day`.
- Auth: `Authorization: Bearer <license/SA token>` (admitted by license-authz)
  **plus** a plain `X-Tenant-Id: <uuid>` to scope to that tenant. (An admin JWT
  with no `X-Tenant-Id` reads across tenants.) This is fr's billing-auth shape —
  do **not** send the w2v `X-Tenant-Id: Bearer <token>` form; billing reads
  `X-Tenant-Id` as the tenant value.
- Returns `{ total_cost_usd, total_input_tokens, total_output_tokens,
  event_count, groups: [{ key: {<dim>: <val>}, cost_usd, input_tokens,
  output_tokens, event_count }] }`.

## Pointer: fr

fr is the closest to done — it **already** generates `X-Correlation-Id` per
request and auto-injects it on every outbound call
(`backend/src/middleware/request-context.ts`,
`backend/src/integrations/_lib/http-client.ts`), and already reads billing
(`backend/src/integrations/metering/usage.ts`). To light up per-operation cost:

1. Set `correlation_id` = the operation you want to price (e.g. a graph-execution
   id) rather than a per-HTTP-request UUID — fr already passes `correlation_id`
   in the graph-execution body; align the **header** it sends with that id.
2. Add a `by-correlation` read in `usage.ts` and surface per-chat / per-graph
   cost in the UI. No new infra — the emit + store + read already exist.

## Pointer: commonpractice

CP is self-contained today: its own `tenant_llm_settings`, prices LLM calls
in-process, and writes a local `usage_events` ledger it reads for `/usage`
(`backend/src/host/{llm-broker,usage-metrics}.ts`). It does **not** emit to the
shared `metering:costs` stream. Two options:

- **Keep self-contained** (simplest) — CP's `/usage` stays correct from its own
  ledger; no fleet integration.
- **Join the fleet rollup** (extend) — also emit a cost event to `metering:costs`
  per LLM call with `correlation_id` set (see `docs/EMITTER_GUIDE.md` for the
  field schema), so cross-app cost reporting + the per-correlation read include
  CP. Only worthwhile if you need CP cost alongside the other apps in billing.

## Pointer: n8n

The Workers/Metering nodes pass `correlation_id` as a **body field** but do not
send the `X-Correlation-Id` **header**. To get per-execution cost attribution,
inject `X-Correlation-Id` (= the n8n execution id) in the node HTTP layer, matching
fr's auto-injection.

## Note: forcing BYO (kept separate)

"Real customers must bring their own key" = set the tenant's
`byo_mode = byo_required` (the worker_management resolver then raises
`BYORequiredError` without a tenant config and the provider catalog hides
`local`). Confirm where `byo_mode` is sourced per tenant (license claim vs tenant
config) before flipping. provisioning (`src/orchestration.py`) is the natural
place to set it on onboarding for paid plans. This is a policy lever, separate
from cost correlation.
