# Billing Service

> **Phase 1 carve-out.** `billing-service` was carved out of `metering-service`
> (Phase 1 of a metering→billing split). It keeps the BILLING domain — usage
> metering, cost tracking, pricing/markup, budgets, quota, audit, dashboards and
> webhooks — while the TELEMETRY / improvement-loop domain (exec_quality,
> detector, provenance, tournament, governor, asset rollups, the Loki ETL, etc.)
> was pruned. The alembic history is a single squashed baseline
> (`0001_billing_baseline`) reflecting the 16 billing tables; the old 0001–0030
> metering migrations are not replayed. Service identity (db user/name, keycloak
> client-id, k8s namespace, port) is renamed to `billing`; the shared Redis
> streams (`metering:events` / `metering:costs`) and consumer group keep their
> byte-stable names so consumption continuity is preserved.

Usage metering, cost tracking, and billing for the nonagentic.ai platform. Port 8157.

| | |
|---|---|
| **Stack** | Python 3.14 · FastAPI · SQLAlchemy + psycopg 3 · Redis Streams |
| **Data** | PostgreSQL via PgBouncer · Redis DB 2 |
| **Runs on** | Kubernetes (Istio ambient) |

## What Metering Service Does

Every authenticated API request flows through `license-authz`, which emits a metering event to a Redis stream (`metering:events`) containing tenant, license, app, plan, method, path, and status. When a request reaches the inference-router and completes, a second event is emitted to `metering:costs` with provider, model, token counts, and calculated USD cost. Both streams are capped at 100K entries.

The metering service runs a background consumer that reads both streams via `XREADGROUP`, buffers events, and flushes them in batches (every 5s or 500 events) to PostgreSQL. Raw events go into `metering_events` and `cost_events` tables. A rollup loop runs every 5 minutes, aggregating raw rows into `metering_hourly`, `metering_daily`, `cost_daily`, and `metering_monthly_usage` tables. Request quotas and token budgets are counted only in Postgres (`metering_monthly_usage.request_count`, `token_budgets.consumed_tokens`); license-authz asks `GET /api/quota/check` and `GET /api/budget/check`.

The API layer provides six endpoint groups: usage queries (by tenant, license, or app with hourly/daily granularity), quota checks (Postgres), cost queries (by tenant or provider), billing reports (usage + costs + configurable markup), token budget management (CRUD + status), and admin configuration (provider rates, app rates, per-tenant markup).

## Features

- **Dual-stream Redis consumer** -- reads `metering:events` and `metering:costs` via consumer groups so that events are delivered at-least-once and survive service restarts
- **Batch insert + rollup** -- raw events are batch-flushed to PostgreSQL every 5s, then rolled up into hourly, daily, and monthly aggregates for fast queries
- **Quota enforcement** -- `metering_monthly_usage` tracks monthly request counts per license; `/api/quota/check` returns usage vs the `request_quota_config` limit
- **Token budget tracking** -- configurable per-tenant or per-license token budgets (weekly or monthly) with Redis counters and DB fallback
- **Cost calculation** -- provider cost rates (USD per 1M tokens) applied to inference token counts; local providers (Qwen3-4B-AWQ, mlx_ring) are zero-cost
- **Configurable markup** -- per-tenant markup percentage (default 30%) applied to internal costs for customer-facing billing; stored in DB and editable via API
- **Per-app cost rates** -- internal infrastructure cost per API request, configurable per service key (workers, kanban, kag, etc.)
- **Billing reports** -- combines usage counts, LLM costs, app costs, and markup into a single tenant bill; calls the licenses service at query time for plan details
- **90-day retention** -- background task cleans raw event tables older than 90 days; rollup tables are kept indefinitely
- **Fire-and-forget emission** -- both license-authz and inference-router emit events in non-blocking try/except blocks so metering never impacts request latency

## Quick Start

```bash
uv sync
uv run uvicorn main:app --host 0.0.0.0 --port 8156
uv run pytest tests/ -v
uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/
```

## API Endpoints

### Usage

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/usage/tenant/{tenant_id}` | Usage by tenant (hourly or daily granularity, filter by service_key) |
| GET | `/api/usage/license/{license_id}` | Usage by license |
| GET | `/api/usage/app/{app_id}` | Usage by app |
| GET | `/api/usage/summary` | Aggregate summary for a tenant |

### Quota

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/quota/check` | Quota status for a license (license_id + plan_id required) |
| GET | `/api/quota/status/{tenant_id}` | Quota status for all licenses in a tenant |

### Costs

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/costs/tenant/{tenant_id}` | Internal costs by tenant, broken down by provider |
| GET | `/api/costs/provider` | Costs aggregated by provider (operator view) |
| GET | `/api/costs/summary` | Total platform cost summary |

### Billing

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/billing/tenant/{tenant_id}` | Tenant bill for a month (usage + costs + markup) |
| GET | `/api/billing/overview` | All-tenant billing overview (operator view) |

### Token Budgets

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/budget/check` | Token budget status (tenant_id or license_id) |
| GET | `/api/budget/status/{tenant_id}` | All budgets for a tenant |
| POST | `/api/budget` | Create or update a token budget |
| DELETE | `/api/budget/{budget_id}` | Deactivate a token budget |

### Configuration

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/config/providers` | List provider cost rates |
| PUT | `/api/config/providers/{provider}` | Update provider cost rate |
| GET | `/api/config/apps` | List per-app cost rates |
| PUT | `/api/config/apps/{app_key}` | Update app cost rate |
| GET | `/api/config/tenants/{tenant_id}` | Get tenant config (markup, etc.) |
| PUT | `/api/config/tenants/{tenant_id}` | Update tenant config |
| GET | `/api/config/markup` | Get default markup percentage |
| PUT | `/api/config/markup` | Update default markup percentage |
| GET | `/api/config/provider-pricing` | List currently-effective provider pricing rows |
| GET | `/api/config/provider-pricing/history` | Effective-dated history for a (provider, model) |
| POST | `/api/config/provider-pricing` | Insert a new effective-dated pricing row |

### Initial pricing seed

Pricing rows must exist before metering can authoritatively compute LLM
costs — without them, every event recompute yields `cost_usd=NULL` and
billing reports $0. After deploy, run the seed script once from a host
that can reach the billing service (or via port-forward):

```bash
kubectl -n billing port-forward svc/billing-service 8157:8157 &
KEYCLOAK_TOKEN_URL=https://auth.nonagentic.ai/realms/portal/protocol/openid-connect/token \
SERVICE_CLIENT_ID=licenses \
SERVICE_CLIENT_SECRET=... \
  uv run python scripts/seed_initial_pricing.py
```

The seed reads `scripts/initial_pricing.yaml` and inserts each row via
`POST /api/config/provider-pricing`, so every insert creates an
`audit_log` entry. The script is idempotent: rows that already match are
skipped, and rows that diverge from the YAML cause it to exit non-zero
(operator must reconcile via a fresh POST so the audit log captures
their identity).

Subsequent rate changes go through the same endpoint — never edit the
YAML to record a price change, that just diverges the seed from the DB.

### Health

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Basic health check |
| GET | `/health/live` | Liveness (always 200) |
| GET | `/health/ready` | Readiness (checks DB + Redis) |

## Configuration

All settings via environment variables prefixed with `METERING_`.

| Variable | Default | Description |
|----------|---------|-------------|
| `METERING_PORT` | `8157` | HTTP listen port |
| `METERING_DATABASE_URL` | -- | Async SQLAlchemy URL (`postgresql+psycopg://...`; `+asyncpg`/bare `postgresql://` are rewritten to `+psycopg` at startup) |
| `METERING_REDIS_URL` | -- | Redis for streams and counters (e.g. `redis://redis:6379/2`) |
| `METERING_LICENSES_URL` | -- | Licenses service base URL for billing queries (required in production) |
| `METERING_EVENTS_STREAM` | `metering:events` | Redis stream for API request events |
| `METERING_COSTS_STREAM` | `metering:costs` | Redis stream for cost events |
| `METERING_CONSUMER_GROUP` | `metering-service` | Redis consumer group name |
| `METERING_CONSUMER_NAME` | `worker-0` | Consumer name (unique per replica) |
| `METERING_BATCH_SIZE` | `500` | Max events per batch flush |
| `METERING_FLUSH_INTERVAL_SECONDS` | `5.0` | Seconds between batch flushes |
| `METERING_ROLLUP_INTERVAL_SECONDS` | `300.0` | Seconds between hourly/daily rollups |
| `METERING_RETENTION_DAYS` | `90` | Days to keep raw events |
| `METERING_DEFAULT_MARKUP_PCT` | `30.0` | Default markup on internal costs |
| `METERING_LOG_LEVEL` | `INFO` | Root log level |
| `METERING_LOG_FORMAT` | `json` | `json` for production, `console` for local |
| `METERING_OTLP_TRACES_ENDPOINT` | `http://grafana-agent:4318/v1/traces` | OTLP HTTP endpoint |

## Architecture

```
src/
├── main.py                # FastAPI app, lifespan, health endpoints
├── settings.py            # Pydantic BaseSettings with METERING_ prefix
├── db.py                  # Async SQLAlchemy engine and session factory
├── redis_client.py        # Redis connection pool (graceful degradation)
├── logging_config.py      # Structlog configuration
├── telemetry.py           # OpenTelemetry setup
├── timeutil.py            # Month/week start helpers
├── models/
│   ├── db.py              # 11 ORM tables (events, rollups, config)
│   └── domain.py          # Pydantic request/response schemas
├── routers/
│   ├── usage.py           # GET /api/usage/*
│   ├── quota.py           # GET /api/quota/*
│   ├── costs.py           # GET /api/costs/*
│   ├── billing.py         # GET /api/billing/*
│   ├── budget.py          # GET/POST/DELETE /api/budget/*
│   └── config.py          # GET/PUT /api/config/*
└── services/
    ├── consumer.py        # Redis Stream XREADGROUP (both streams)
    ├── aggregator.py      # Batch INSERT + hourly/daily rollup
    ├── quota.py            # Quota checks (request_quota_config + metering_monthly_usage)
    ├── budget.py           # Token budget tracking + CRUD
    └── billing.py          # Cross-service billing calculation
```

## Database Tables

| Table | Purpose | Retention |
|-------|---------|-----------|
| `metering_events` | Raw API request events | 90 days |
| `cost_events` | Raw LLM inference cost events | 90 days |
| `metering_hourly` | Hourly request aggregates | Permanent |
| `metering_daily` | Daily request aggregates | Permanent |
| `cost_daily` | Daily cost aggregates | Permanent |
| `metering_monthly_usage` | Monthly quota tracking | Permanent |
| `provider_costs` | LLM provider cost rates (editable) | Config |
| `app_cost_rates` | Per-app infrastructure cost rates | Config |
| `tenant_config` | Per-tenant markup overrides | Config |
| `token_budgets` | Token budget definitions | Config |
| `token_usage` | Token consumption tracking | Permanent |

## Design Principles

1. **Never block the request path** -- event emission is fire-and-forget with try/except; if Redis is down, emitters log a warning and continue
2. **PostgreSQL is the truth** -- quota and budget checks read Postgres; Redis holds only the event streams and the in-flight budget reservation pool
3. **Two cost layers** -- internal cost (what we pay) and customer cost (internal * markup) are tracked separately so margins are always visible
4. **Schema auto-creation** -- tables are created via `Base.metadata.create_all` on startup so no migration tooling is needed for this service

## Testing

```bash
uv run pytest tests/ -v                                   # All tests
uv run ruff check src/ tests/                               # Lint
uv run ruff format --check src/ tests/                      # Format check
uv run ty check src/                                        # Type checking
```

Tests use PostgreSQL and fakeredis. Coverage threshold: 90%.

## CI/CD

CI is [`.github/workflows/ci-public.yml`](.github/workflows/ci-public.yml):
GitHub Actions on GitHub-hosted `ubuntu-latest` runners, with no self-hosted
runners and no repository secrets. It runs on pull requests, pushes to `main`,
and `workflow_dispatch`:

1. **Secret scan** -- `betterleaks` (pinned and checksum-verified) over the tree
2. **Private-reference scan** -- `scripts/ci/no-private-refs.sh` over tracked files
3. **Checks** -- a Postgres 18 container, `uv sync --frozen --all-extras`,
   `ruff check` + `ruff format --check`, `pytest`, and `pip-audit`
4. **Image** -- `docker build` of the `Dockerfile`. Disabled unless the
   repository variable `PUBLISH_IMAGE` is `true`; when enabled, the optional
   `SMOKE_CMD` repository variable is executed inside the built image (unset
   means no smoke test)
5. **Publish** -- on `main` only, the image is pushed to
   `ghcr.io/<owner>/<repo>` tagged `sha-<commit>`, `main-<run>`, and `latest`,
   using the automatic `GITHUB_TOKEN`

The workflow publishes the image and stops there; deployment is not performed
by CI.

## Docker

```bash
docker build -t metering-service .
docker run -p 8156:8156 \
  -e METERING_DATABASE_URL="postgresql+psycopg://user:pass@host:5432/billing" \
  -e METERING_REDIS_URL="redis://host:6379/2" \
  metering-service
```

Multi-stage build: Python 3.14-slim, non-root user (uid 1000), single uvicorn worker.

## Dependencies

- **license-authz** (port 9001) -- emits `metering:events` to Redis on every auth decision
- **inference-router** (port 8150) -- emits `metering:costs` to Redis after each inference call
- **licenses** (port 8544) -- queried at billing time for plan details and quota limits
- **PostgreSQL** via PgBouncer (port 6432) -- stores all events, rollups, and configuration
- **Redis** (port 6379, DB 2) -- streams for event buffering, in-flight budget reservations
- **FastAPI** -- web framework
- **SQLAlchemy 2.0 + psycopg 3** -- async database access
- **redis[hiredis]** -- async Redis client
- **structlog** -- structured JSON logging
- **OpenTelemetry** -- distributed tracing

## Licensing

The code in this repository is Apache-2.0 and needs no licence key to build,
test or run.

billing-service does not enforce licence tokens itself: it is an internal
service that sits behind the platform's enforcement point and authenticates
callers with `INTERNAL_SECRET`. The only licence-related configuration it
needs is `METERING_LICENSES_URL`, the base URL of the licences service it
queries for tenant and plan data. That setting defaults to empty and is
required only in production; while it is unset, no licence request is made.

Licence enforcement is opt-in at the platform level. The public
[`license-authz`](https://github.com/nonagenticai/license-authz) enforcement
point takes `LICENSE_INTROSPECT_URL`, which defaults to empty and is required
only by a production deployment that enables enforcement.

The licensing stack is open source too — [`licenses`](https://github.com/nonagenticai/licenses)
(entitlements, plans and tokens) and [`license-authz`](https://github.com/nonagenticai/license-authz)
(the enforcement point). Neither is required to run this code.

## License

Apache 2.0 — see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
