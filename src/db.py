"""SQLAlchemy async engine and session factory (psycopg3 driver)."""

from collections.abc import AsyncGenerator

import structlog
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from settings import Settings

# Idempotent additive migrations applied on every startup. Each statement
# must be safe to repeat — pods restart often and there is no Alembic.
ADDITIVE_MIGRATIONS: tuple[str, ...] = (
    # --- existing budget_denied_count columns on rollup tables ---
    "ALTER TABLE billing_metering_hourly "
    "ADD COLUMN IF NOT EXISTS budget_denied_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE billing_metering_daily "
    "ADD COLUMN IF NOT EXISTS budget_denied_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE billing_metering_monthly_usage "
    "ADD COLUMN IF NOT EXISTS budget_denied_count INTEGER NOT NULL DEFAULT 0",
    # --- TokenBudget: drop legacy unique, add stacking columns + drain index ---
    "ALTER TABLE token_budgets DROP CONSTRAINT IF EXISTS uq_token_budget",
    "ALTER TABLE token_budgets ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 100",
    "ALTER TABLE token_budgets "
    "ADD COLUMN IF NOT EXISTS starts_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    "ALTER TABLE token_budgets ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ",
    "ALTER TABLE token_budgets "
    "ADD COLUMN IF NOT EXISTS consumed_tokens NUMERIC(20,6) NOT NULL DEFAULT 0",
    "ALTER TABLE token_budgets "
    "ADD COLUMN IF NOT EXISTS source VARCHAR(64) NOT NULL DEFAULT 'manual'",
    # Period anchor for consumed_tokens (see services/budget_period.py). Without
    # it a 'monthly' budget never reset and behaved as a lifetime cap.
    "ALTER TABLE token_budgets ADD COLUMN IF NOT EXISTS consumed_period_start DATE",
    # Backfill: the period the row was CREATED in. Idempotent (only touches
    # NULLs), and it deliberately marks long-lived rows as belonging to an old
    # period so their stale lifetime consumption stops being enforced.
    "UPDATE token_budgets SET consumed_period_start = CASE "
    "WHEN period = 'monthly' THEN date_trunc('month', starts_at AT TIME ZONE 'UTC')::date "
    "WHEN period = 'weekly' THEN date_trunc('week', starts_at AT TIME ZONE 'UTC')::date "
    "END WHERE consumed_period_start IS NULL AND period IN ('monthly', 'weekly')",
    "CREATE INDEX IF NOT EXISTS ix_tokenbudget_active_drain "
    "ON token_budgets (tenant_id, active, priority, expires_at)",
    # --- CostEvent: cost_usd_hint and pricing_id ---
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS cost_usd_hint NUMERIC(20,8)",
    "ALTER TABLE billing_cost_events "
    "ADD COLUMN IF NOT EXISTS pricing_id BIGINT "
    "REFERENCES provider_pricing(id) ON DELETE SET NULL",
    # --- CostEvent: served_model + cache token classes + provider_request_id + usage_extra ---
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS served_model VARCHAR(128)",
    "ALTER TABLE billing_cost_events "
    "ADD COLUMN IF NOT EXISTS cache_creation_input_tokens BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE billing_cost_events "
    "ADD COLUMN IF NOT EXISTS cache_read_input_tokens BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS provider_request_id VARCHAR(255)",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS usage_extra JSONB",
    # cost_usd flips to nullable — pricing-miss events keep the hint but no
    # authoritative cost. Idempotent: DROP NOT NULL is a no-op if already NULL.
    "ALTER TABLE billing_cost_events ALTER COLUMN cost_usd DROP NOT NULL",
    # Widen cost_usd Numeric(12,8)->Numeric(20,8): the old precision capped a
    # single event at ~$10k, so a computed cost >= $10k overflowed on INSERT and
    # aborted the whole cost_flusher batch (stuck consumer). Matches cost_usd_hint
    # + provider_pricing rates. Idempotent no-op if already 20,8. (ALTER COLUMN on
    # this RANGE-partitioned table cascades to partitions, same as the DROP NOT
    # NULL above.)
    "ALTER TABLE billing_cost_events ALTER COLUMN cost_usd TYPE NUMERIC(20,8)",
    # Widen billing_cost_daily.total_cost_usd Numeric(12,6)->Numeric(20,6): the
    # old precision capped a per-tuple daily aggregate at $999,999.999999, so a
    # high-volume tenant's daily total could overflow the uq_cost_daily upsert and
    # fail the day's rollup (the invoice source). Idempotent no-op if already 20,6.
    "ALTER TABLE billing_cost_daily ALTER COLUMN total_cost_usd TYPE NUMERIC(20,6)",
    # --- ProviderPricing: Anthropic-style cache rates ---
    "ALTER TABLE provider_pricing ADD COLUMN IF NOT EXISTS cache_creation_per_1m_usd NUMERIC(20,8)",
    "ALTER TABLE provider_pricing ADD COLUMN IF NOT EXISTS cache_read_per_1m_usd NUMERIC(20,8)",
    # --- Backfill provider_pricing from billing_provider_costs (idempotent) ---
    "INSERT INTO provider_pricing "
    "(provider, model, cost_model, input_per_1m_usd, output_per_1m_usd, effective_from) "
    "SELECT provider, NULL, 'per_token', input_per_1m_usd, output_per_1m_usd, "
    "'2026-01-01T00:00:00+00:00'::timestamptz FROM billing_provider_costs pc "
    "WHERE NOT EXISTS ("
    "SELECT 1 FROM provider_pricing pp "
    "WHERE pp.provider = pc.provider AND pp.model IS NULL"
    ")",
    # --- MeteringEvent: plan_code (model declared it, live tables predate it) ---
    # Without this, every license-authz event raw insert errors with
    # `column "plan_code" of relation "billing_metering_events" does not exist`,
    # which silently drops the entire flush batch.
    "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS plan_code VARCHAR(64)",
    # --- MeteringMonthlyUsage: denied_count (model declared it, live tables predate it) ---
    # Same shape as the other rollup denial counters. The model exposes both
    # `denied_count` and `budget_denied_count`; the latter migrated earlier
    # but `denied_count` was missed, so the monthly quota upsert failed
    # silently the same way the events insert did.
    "ALTER TABLE billing_metering_monthly_usage "
    "ADD COLUMN IF NOT EXISTS denied_count INTEGER NOT NULL DEFAULT 0",
    # --- MeteringEvent: relax NOT NULL on FK columns the model declared as nullable ---
    # Model says license_id/app_id/plan_id are `nullable=True` (denied-missing-token
    # rows have all three null), but the live table was created with NOT NULL.
    # Every denied auth event silently failed insert with NotNullViolation.
    # Idempotent — DROP NOT NULL on an already-nullable column is a no-op.
    "ALTER TABLE billing_metering_events ALTER COLUMN license_id DROP NOT NULL",
    "ALTER TABLE billing_metering_events ALTER COLUMN app_id DROP NOT NULL",
    "ALTER TABLE billing_metering_events ALTER COLUMN plan_id DROP NOT NULL",
    # --- Per-user metering: subject_id on raw + rollup tables ---
    # Raw event tables: nullable subject_id + index.
    "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS subject_id UUID",
    "CREATE INDEX IF NOT EXISTS ix_events_subject_created "
    "ON billing_metering_events (subject_id, created_at)",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS subject_id UUID",
    "CREATE INDEX IF NOT EXISTS ix_costs_subject_created ON billing_cost_events (subject_id, created_at)",
    # Rollup tables: NOT NULL subject_id with sentinel default. The sentinel
    # value matches NULL_SUBJECT_ID in src/models/db.py.
    "ALTER TABLE billing_metering_hourly ADD COLUMN IF NOT EXISTS subject_id UUID NOT NULL "
    "DEFAULT '00000000-0000-0000-0000-000000000003'::uuid",
    "ALTER TABLE billing_metering_daily ADD COLUMN IF NOT EXISTS subject_id UUID NOT NULL "
    "DEFAULT '00000000-0000-0000-0000-000000000003'::uuid",
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS subject_id UUID NOT NULL "
    "DEFAULT '00000000-0000-0000-0000-000000000003'::uuid",
    # --- 0031: billing_cost_daily.unpriced_event_count ---
    # The cost rollup's UPSERT NAMES this column, so a pod booting against a
    # pre-0031 database fails EVERY cost rollup, not merely the unpriced ones.
    # ⚠️ An existing row reads 0, and that 0 means "NOT MEASURED" — not
    # "measured, and everything was priced". Every bucket written before 0031
    # predates the counter, so do not read a pre-0031 zero as a clean bill.
    "ALTER TABLE billing_cost_daily "
    "ADD COLUMN IF NOT EXISTS unpriced_event_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE billing_metering_monthly_usage ADD COLUMN IF NOT EXISTS subject_id UUID NOT NULL "
    "DEFAULT '00000000-0000-0000-0000-000000000003'::uuid",
    # --- 0035: billing_metering_monthly_usage.plan_id gets the NULL_PLAN_ID default ---
    # ⛔ MIRRORS alembic 0035_monthly_usage_plan_default VERBATIM. Editing one
    # without the other re-opens the 0012 drift (a fresh alembic-only database
    # and production diverging silently), which is the defect that whole
    # migration exists to close.
    #
    # WHY IT MATTERS: `event_flusher`'s monthly-usage guard used to require BOTH
    # a license_id and a plan_id, and billing mirrors NO plan anywhere (no
    # plan_id on request_quota_config, no Plan model, no resolve_plan), so the
    # second conjunct was false for essentially all live traffic. Measured: this
    # table held 6 rows / 5 distinct licences against 160 rows in
    # request_quota_config — 155 of 160 configured licences had no usage row, so
    # every request-quota check fell through fail-open. The guard now requires
    # license_id alone and substitutes the NULL_PLAN_ID sentinel, which is the
    # same value the hourly/daily rollups already default to and the same device
    # this table already uses for subject_id.
    #
    # A SENTINEL, NOT A NULLABLE COLUMN — deliberately. plan_id is part of
    # uq_monthly_usage, and Postgres treats NULLs as DISTINCT in a unique key, so
    # a nullable plan_id would make the flusher's ON CONFLICT upsert MISS every
    # plan-less row: one new row per flush instead of an increment, and readers
    # that SUM across the plan grain would add up the duplicates. The column
    # stays NOT NULL.
    #
    # SET DEFAULT is catalogue-only — no rewrite, no scan, existing rows
    # untouched (a DEFAULT applies only to INSERTs that OMIT the column) — so it
    # is a genuine no-op on replay, which is what makes it safe here.
    "ALTER TABLE billing_metering_monthly_usage "
    "ALTER COLUMN plan_id SET DEFAULT '00000000-0000-0000-0000-000000000002'::uuid",
    # --- Phase 3: tenant_name VARCHAR(255) on every table that has tenant_id ---
    # Backfill: tenant_name := '' — the CONTRACT's absence marker, NOT the uuid.
    # NOT NULL after backfill. Named indexes for admin queries.
    #
    # ⛔ THIS USED TO READ `SET tenant_name = tenant_id::text`, and it was the
    # schema layer manufacturing the exact rows `stored_tenant_name`
    # (services._parsers) exists to prevent. Its stated justification —
    # "the existing string WAS the name" — was TRUE when tenant_id was a varchar
    # slug and ROTTED at alembic 0011, which made tenant_id a uuid. Since then a
    # pod start on any table where this column was still NULL stamped a uuid into
    # a display column: non-empty, distinct per tenant, and therefore passing
    # every downstream truthiness guard as if it were a name. `""` asserts
    # nothing, which is the honest content of "this row predates the column".
    # (`resolve_tenant_name` renders both identically — it already discards a
    # stored name equal to the uuid — so no consumer sees a change.)
    #
    # The ::text cast is gone with the uuid it existed to make assignable. The
    # trap it warned about is still real for anyone reintroducing one: tenant_id
    # is uuid and tenant_name is varchar, so `SET <varchar> = <uuid>` is rejected
    # at PARSE time and would fail on every pod start even with zero matching
    # rows. A string literal has no such problem.
    #
    # Measured on the prod billing DB 2026-09-09, `count(*) FILTER (WHERE
    # tenant_name IS NULL)` vs `count(*)` — every one of these eight UPDATEs
    # matches ZERO rows today, so changing the value they write rewrites nothing:
    #   billing_cost_events     0 / 187,180    billing_metering_daily  0 /    92
    #   billing_metering_events 0 /  37,563    token_usage             0 /    95
    #   billing_cost_daily      0 /   1,370    token_budgets           0 /   404
    #   billing_metering_hourly 0 /     882    tenant_config           0 /     0
    # (All eight carry tenant_name NOT NULL, which is why.) This changes what a
    # FUTURE add-column backfill writes, and what CI writes on a fresh schema; it
    # does not rewrite existing rows. Repairing the rows already stamped is
    # `scripts/backfill_tenant_name.py`.
    #
    # ⛔ audit_log is NOT part of this conversion and must not be folded into it.
    # Its own UPDATE was DELETED 2026-09-15 (the record sits just above
    # `ix_audit_log_tenant_name` further down): on an append-only audit table a
    # `''` is as much a rewrite of a recorded row as a uuid is, so audit_log gets
    # NO backfill statement here at all, not a corrected one.
    "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    "UPDATE billing_metering_events SET tenant_name = '' WHERE tenant_name IS NULL",
    "ALTER TABLE billing_metering_events ALTER COLUMN tenant_name SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_metering_events_tenant_name ON billing_metering_events (tenant_name)",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    "UPDATE billing_cost_events SET tenant_name = '' WHERE tenant_name IS NULL",
    "ALTER TABLE billing_cost_events ALTER COLUMN tenant_name SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_cost_events_tenant_name ON billing_cost_events (tenant_name)",
    "ALTER TABLE billing_metering_hourly ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    "UPDATE billing_metering_hourly SET tenant_name = '' WHERE tenant_name IS NULL",
    "ALTER TABLE billing_metering_hourly ALTER COLUMN tenant_name SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_metering_hourly_tenant_name ON billing_metering_hourly (tenant_name)",
    "ALTER TABLE billing_metering_daily ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    "UPDATE billing_metering_daily SET tenant_name = '' WHERE tenant_name IS NULL",
    "ALTER TABLE billing_metering_daily ALTER COLUMN tenant_name SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_metering_daily_tenant_name ON billing_metering_daily (tenant_name)",
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    "UPDATE billing_cost_daily SET tenant_name = '' WHERE tenant_name IS NULL",
    "ALTER TABLE billing_cost_daily ALTER COLUMN tenant_name SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_cost_daily_tenant_name ON billing_cost_daily (tenant_name)",
    "ALTER TABLE token_budgets ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    "UPDATE token_budgets SET tenant_name = '' WHERE tenant_name IS NULL",
    "ALTER TABLE token_budgets ALTER COLUMN tenant_name SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_token_budgets_tenant_name ON token_budgets (tenant_name)",
    "ALTER TABLE token_usage ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    "UPDATE token_usage SET tenant_name = '' WHERE tenant_name IS NULL",
    "ALTER TABLE token_usage ALTER COLUMN tenant_name SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_token_usage_tenant_name ON token_usage (tenant_name)",
    "ALTER TABLE tenant_config ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    "UPDATE tenant_config SET tenant_name = '' WHERE tenant_name IS NULL",
    "ALTER TABLE tenant_config ALTER COLUMN tenant_name SET NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_tenant_config_tenant_name ON tenant_config (tenant_name)",
    # 0029_markup_rules — BYO/pass-through billing mode gate on existing DBs
    # (fresh DBs get it from baseline create_all). See models/db/config.py.
    "ALTER TABLE tenant_config ADD COLUMN IF NOT EXISTS billing_mode VARCHAR(32) "
    "NOT NULL DEFAULT 'standard_markup'",
    # 0030 — served_model + byo on billing_cost_daily so per-model markup resolves at
    # billing time. uq_cost_daily is widened to include them (and cost_source)
    # in the constraint block below.
    "ALTER TABLE billing_cost_daily "
    "ADD COLUMN IF NOT EXISTS served_model VARCHAR(128) NOT NULL DEFAULT '__unknown__'",
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS byo BOOLEAN NOT NULL DEFAULT false",
    # 0033 — owner_provided (managed BYO) on billing_cost_daily so owner-provided spend
    # rolls up as its own dimension, distinct from tenant-supplied byo. Added to
    # uq_cost_daily below. Default false → existing rows keep their unique key
    # (an extra false column can only split rows, never collide).
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS owner_provided BOOLEAN NOT NULL DEFAULT false",
    # --- 0034: billing_metering_events.event_ts (+ its partial index) ---------
    # The PRODUCER's event time on the metering bus, so a request's per-request
    # surcharge lands in the same billing period as its LLM cost (alembic 0028 did
    # the cost half; leaving metering on ingest time split ONE request across TWO
    # immutable invoices). ⚠️ NOTHING WRITES THIS COLUMN YET — the
    # `services.event_flusher` half is a separate change in flight. It is mirrored
    # here in advance because the moment that lands the flusher NAMES this column
    # on the raw insert, and a pod booting against a pre-0034 database would then
    # fail EVERY metering insert with UndefinedColumn — a full ingest outage, not
    # a degradation. Mirroring it makes the two deployable in either order.
    # Nullable with no default: catalogue-only, no rewrite, no row touched. ⛔ NO BACKFILL — see the migration; `event_ts = created_at` would
    # restate ingest time as event time AND silently move historical buckets.
    "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS event_ts TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS ix_events_event_ts "
    "ON billing_metering_events (event_ts) WHERE event_ts IS NOT NULL",
    # 0025 — licence lifecycle status on the attribution mirror. Without it a
    # REVOKED licence stayed resolvable and kept attracting attributed invoice
    # lines (services.license_resolver's "LIMITATION" block). DEFAULT 'active'
    # backfills every existing row in the same statement: that is the status
    # they were written under (the sync fetched ?status=active exclusively) and
    # the only value that preserves attribution for live licences on deploy.
    "ALTER TABLE request_quota_config "
    "ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'active'",
    "CREATE INDEX IF NOT EXISTS ix_request_quota_config_active "
    "ON request_quota_config (tenant_id, app_name) "
    "WHERE status = 'active' AND app_id IS NOT NULL",
    # 0049 — the app's registry KEY (X-App-Id slug), resolved to app_id by the
    # cost flusher. Mirrored here so a pod booting before the migration does not
    # fail every sync/flush that names the column. Nullable, no default.
    "ALTER TABLE request_quota_config ADD COLUMN IF NOT EXISTS app_key text",
    "ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS tenant_name VARCHAR(255)",
    # ⛔ DELETED 2026-09-15 — and it must never come back, in any spelling.
    #
    # This slot held one statement, replayed on EVERY pod start:
    #
    #     UPDATE audit_log SET tenant_name = tenant_id::text WHERE tenant_name IS NULL
    #
    # WHAT IT WAS FOR. It was the audit_log half of the Phase 3 tenant_name
    # rollout above: when tenant_id was still a varchar slug, "the existing
    # string WAS the name" was true and stamping it into the new display column
    # read as a backfill. That justification ROTTED at alembic 0011, which made
    # tenant_id a uuid — the same rot the eight-table block above records.
    #
    # WHY IT SURVIVED ANYWAY. Only as an ACCIDENT OF THE DATA. It was a VALUE
    # no-op (NULL::text written over NULL) because on prod every row with
    # `tenant_name IS NULL` also had `tenant_id IS NULL`. The comment that stood
    # here named that precondition as a precondition, and named its own remedy:
    # "If an audit row ever appears with `tenant_id` set and `tenant_name` NULL,
    # this stamps the uuid into a display column — the exact laundering
    # `services._parsers.stored_tenant_name` exists to prevent. The answer then
    # is to DELETE this statement, never to change it to `''`."
    #
    # ⭐ THE PRECONDITION BROKE, MEASURED ON THE PROD BILLING DB 2026-09-15:
    # audit_log holds 4,186 rows; 286 have `tenant_name IS NULL`; and 55 OF THOSE
    # HAVE `tenant_id` SET. Every pod start would have stamped 55 uuids into a
    # human-facing display column. It did not, because
    # `billing_forbid_audit_log_update()` (models/db/audit_log_guard.py, installed
    # by alembic 0029) REFUSES any UPDATE that moves a byte of an audit row. So
    # this statement had already stopped applying: it failed on every boot —
    # SOFT, at `additive_migration_failed` WARNING, no outage — and the guard was
    # the only thing between this tuple and an irreversible write to an
    # APPEND-ONLY table. The guard's own HINT says the same thing the comment
    # did: "DELETE the statement, as the comment above it instructs. Do not
    # widen this guard."
    #
    # ⛔ SO: DELETED, and this is the list of things that are NOT the answer.
    # NOT reinstating it. NOT rewriting it as `SET tenant_name = ''` — that turns
    # "this event had no tenant" into "this event had a tenant with no name" and
    # silently disarms every `COALESCE(tenant_name, …)` reader downstream. NOT
    # widening, disabling or excepting `billing_forbid_audit_log_update()`. And
    # NOT reaching those rows by some other route — a migration, a script, a
    # one-off UPDATE — because the objection was never to this statement's
    # spelling, it was to the write. NULL in `audit_log.tenant_name` is
    # MEANINGFUL and those 55 rows must keep it: an audit row is the record OF a
    # change, and rewriting one to say something other than what was recorded is
    # the one edit an audit log must never accept. Same principle on which
    # `scripts/backfill_tenant_name.py` keeps audit_log out of its TABLES tuple.
    # Rendering a name at READ time is `services._parsers.stored_tenant_name`'s
    # job, not the schema layer's.
    #
    # `audit_log.tenant_name` stays NULLABLE (mirroring its nullable tenant_id)
    # with no `SET NOT NULL` anywhere, unlike the eight tables above, so nothing
    # forces a value into it at all. The ADD COLUMN above and the index below
    # stay — those are genuinely additive.
    #
    # ⛔ `tests/test_audit_log_tamper_evidence.py::
    # test_no_startup_statement_updates_audit_log` fails if ANY statement in this
    # tuple UPDATEs audit_log again, so this cannot be reinstated silently.
    "CREATE INDEX IF NOT EXISTS ix_audit_log_tenant_name ON audit_log (tenant_name)",
    # The four unique constraints (uq_hourly / uq_daily / uq_cost_daily /
    # uq_monthly_usage) used to be DROPped and re-ADDed here on every pod start.
    # They are gone from this tuple, which is named ADDITIVE_MIGRATIONS and is
    # now actually additive -- `tests/test_additive_migrations_are_additive.py`
    # enforces that.
    #
    # Two reasons, and the second is the load-bearing one:
    #  * Redundant. Measured 2026-09-01 by diffing a fresh `alembic upgrade head`
    #    database against production: uq_hourly, uq_daily and uq_monthly_usage
    #    were already IDENTICAL on both sides, so the drop/re-add was pure churn.
    #    uq_cost_daily was the one that genuinely differed and migration
    #    `0015_pricing_cost_daily_shapes` now owns it.
    #  * Every boot opened a window with no uniqueness enforcement, and each
    #    statement here fails SOFT (see _apply_additive below) -- so a failed
    #    re-ADD left the constraint simply GONE and the pod started anyway. On
    #    uq_cost_daily that is the upsert key for cost aggregation. Today the
    #    tables are small (6.8k rows, 2.7MB) so the window is milliseconds and
    #    there is no evidence it ever bit; it grows with the data.
    #
    # The comment that used to sit here claimed "the ADD is guarded by a
    # sub-statement check via `DO $$ ... $$`". There was no such guard.
    # 0031 — ProviderPricing.rate_class: 'cost' (default, hot path) vs 'list'
    # (public provider list price, read-time BYO estimate only). Existing rows
    # default to 'cost' so the cost resolver is unchanged. Both pricing indexes
    # are dropped + recreated with rate_class as the leading column so open-row
    # uniqueness becomes per (rate_class, provider, model, service_key) — a
    # 'list' row can coexist with the 'cost' row for the same tuple.
    "ALTER TABLE provider_pricing "
    "ADD COLUMN IF NOT EXISTS rate_class VARCHAR(16) NOT NULL DEFAULT 'cost'",
    # Both pricing indexes moved to `0015_pricing_cost_daily_shapes` for the same
    # reason: they were REPLACEMENTS (drop + recreate), not additions, and
    # ix_providerpricing_open is UNIQUE -- dropping it on every boot removed the
    # only thing stopping a second open row for the same
    # (rate_class, provider, model, service_key).
    # 0032 — tenant_access_charge: the ACCESS-billing axis. Fresh DBs get the
    # table + indexes from baseline create_all; these are defensive/idempotent
    # so an existing DB (or a future create_all that only adds missing tables)
    # still gets the effective-dated open-row uniqueness. See models/db/config.py.
    "CREATE TABLE IF NOT EXISTS tenant_access_charge ("
    "id BIGSERIAL PRIMARY KEY, "
    # tenant_id is uuid (alembic 0011) — must track models/db/config.py.
    "tenant_id UUID NOT NULL, "
    "service_key VARCHAR(64), "
    "amount_usd NUMERIC(20,8) NOT NULL, "
    "cadence VARCHAR(16) NOT NULL DEFAULT 'monthly', "
    "included_cost_usd NUMERIC(20,8), "
    "effective_from TIMESTAMPTZ NOT NULL, "
    "effective_until TIMESTAMPTZ, "
    "notes TEXT)",
    "CREATE INDEX IF NOT EXISTS ix_accesscharge_lookup "
    "ON tenant_access_charge (tenant_id, coalesce(service_key, ''), effective_from DESC)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_accesscharge_open "
    "ON tenant_access_charge (tenant_id, coalesce(service_key, '')) "
    "WHERE effective_until IS NULL",
    # 0034 — reversible billing exclusion on billing_cost_events, plus the
    # producer-minted per-call idempotency key. ADD COLUMN on a RANGE-partitioned
    # parent cascades to every existing partition and is inherited by every
    # future one (create_cost_events_partition_for uses CREATE TABLE … PARTITION
    # OF), so no per-partition DDL and no change to the partition function.
    # Mirrored byte-identically in alembic 0013 — see models/db/events.py.
    "ALTER TABLE billing_cost_events "
    "ADD COLUMN IF NOT EXISTS excluded_from_billing BOOLEAN NOT NULL DEFAULT false",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS exclusion_reason VARCHAR(64)",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS duplicate_of_event_id UUID",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS event_key VARCHAR(64)",
    # The idempotency inbox for event_key. NON-partitioned on purpose: a UNIQUE
    # on billing_cost_events would have to include created_at (the partition
    # key), which is ingest-time and therefore differs between an emit and its
    # replay — vacuous for the case it exists to catch.
    "CREATE TABLE IF NOT EXISTS processed_cost_event_keys ("
    "event_key VARCHAR(64) PRIMARY KEY, "
    "processed_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    "CREATE INDEX IF NOT EXISTS ix_processed_cost_event_keys_processed_at "
    "ON processed_cost_event_keys (processed_at)",
    # Attribution grain, quantity unit, and cash-vs-book columns on
    # billing_cost_events. Mirrored byte-identically (safety-net shape) in
    # alembic 0021_cost_event_actor_pricing — see models/db/events.py.
    # ⚠️ NOT a replacement of the alembic migration: this tuple only adds each
    # column NULLABLE with the SAME literal default alembic 0021 uses for the
    # two NOT NULL ones, so a pod that boots before the migration has run can
    # still insert a row. It does NOT run the quantity_unit per-row backfill —
    # that data-dependent UPDATE runs exactly once, reviewed, in the alembic
    # chain. A pod that only ever sees this replay (never the migration) keeps
    # working; it simply never gets quantity_unit backfilled for history,
    # which is a strictly better failure than "cannot start".
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS actor_id UUID",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS quantity_unit VARCHAR(16)",
    "ALTER TABLE billing_cost_events "
    "ADD COLUMN IF NOT EXISTS cost_basis VARCHAR(20) NOT NULL DEFAULT 'internal_transfer'",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS vendor_cost_usd NUMERIC(20,8)",
    "ALTER TABLE billing_cost_events "
    "ADD COLUMN IF NOT EXISTS currency VARCHAR(3) NOT NULL DEFAULT 'USD'",
    "CREATE INDEX IF NOT EXISTS ix_costs_actor_created "
    "ON billing_cost_events (actor_id, created_at) WHERE actor_id IS NOT NULL",
    # --- CostEvent.event_ts (alembic 0028) ---
    # ⛔ THE OMISSION HERE IS AN INGEST OUTAGE, NOT A COSMETIC GAP. cost_flusher
    # names event_ts in every INSERT, so a pod carrying the new flusher that
    # reaches a database still on pre-0028 fails EVERY cost INSERT with
    # UndefinedColumn -- the whole cost ingest path, not a degraded subset. That
    # is the scenario this tuple exists for: it is the safety net for
    # "code rolled before the migration ran", and an additive column absent from
    # it has no net.
    # Mirrored byte-compatibly with 0028, including the partial index, because a
    # boot-replayed schema that differs from the migrated one is how the two
    # drift apart silently.
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS event_ts TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS ix_costs_event_ts "
    "ON billing_cost_events (event_ts) WHERE event_ts IS NOT NULL",
    # --- CostEvent.reservation_id (alembic 0032) ---
    # Same outage class as event_ts directly above: the flusher NAMES
    # reservation_id in every cost INSERT, so a pod carrying the new code against
    # a pre-0032 database fails EVERY cost INSERT with UndefinedColumn — the whole
    # cost ingest path, not a degraded subset.
    # Byte-identical to alembic 0032's own _ADD_COLUMN string. ⚠️ NO index here,
    # matching 0032: the column is written on insert and read only in-process
    # during the same settlement batch, never queried by itself. ⚠️ A NULL is NOT
    # a gap — it is the correct permanent value for the ~90% of debits that were
    # never pre-flighted through a reservation, and there is no backfill.
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS reservation_id UUID",
    # provider_pricing.cost_basis — the PROVIDER CONTRACT fact backing
    # billing_cost_events.cost_basis (alembic 0022 — see models/db/config.py
    # + services/pricing.py). Mirrored byte-identically (safety-net shape,
    # same NOT NULL default) so a pod that boots before the migration has run
    # can still insert a pricing row. ⚠️ NOT a replacement of the alembic
    # migration: this does NOT run 0022's external-vendor backfill
    # (provider IN ('claude', 'anthropic', ...) -> 'cash') or its
    # billing_cost_events mislabelled-row correction — both are data-dependent
    # UPDATEs that run exactly once, reviewed, in the alembic chain, same
    # precedent as quantity_unit's backfill above.
    "ALTER TABLE provider_pricing "
    "ADD COLUMN IF NOT EXISTS cost_basis VARCHAR(20) NOT NULL DEFAULT 'internal_transfer'",
    # --- project_id, the per-project pricing axis (alembic 0036) ---
    # Same outage class as event_ts / reservation_id above: once a producer NAMES
    # project_id in its INSERT, a pod carrying that code against a pre-0036
    # database fails EVERY such INSERT with UndefinedColumn -- the whole ingest
    # path, not a degraded subset. That is the scenario this tuple exists for,
    # and an additive column absent from it has no net.
    # Byte-identical to alembic 0036's own _ADD_*_COLUMN strings.
    #
    # ⚠️ NO index on the raw event tables, matching 0036: raw events are read by
    # partition + time, never grouped by project -- the per-project question is
    # answered off billing_cost_daily. A bare btree here would be paid for on
    # every ingest INSERT and serve nothing.
    # billing_cost_events is RANGE-partitioned on created_at: a nullable ADD
    # COLUMN on the PARENT is catalogue-only, cascades to every existing
    # partition and is inherited by every future one, so there is no
    # per-partition DDL. billing_metering_events is not partitioned.
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS project_id UUID",
    "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS project_id UUID",
    # ⛔ NOT NULL + sentinel on the rollup, and it cannot be nullable: project_id
    # joins uq_cost_daily, which is both the rollup's declared ON CONFLICT target
    # and the only database-side guard on the daily grain. Postgres NULL never
    # matches itself (NULLS DISTINCT), so a nullable column there would leave
    # project-less rows unprotected against duplication by any writer.
    # ⚠️ NOT because the rollup upserts onto them every cycle -- it does not:
    # services/rollup_engine.py deletes the whole bucket_date before inserting
    # (~:818), so a re-rollup never reaches ON CONFLICT. This is the net if that
    # DELETE is ever removed or narrowed. Literal tracks models/db/_common.py
    # NULL_PROJECT_ID.
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS project_id UUID "
    "NOT NULL DEFAULT '00000000-0000-0000-0000-000000000004'::uuid",
    # Nullable on markup_rules: NULL = the rule is tenant-wide (the wildcard).
    "ALTER TABLE markup_rules ADD COLUMN IF NOT EXISTS project_id UUID",
    # ⚠️ ONLY the ADD COLUMNs are mirrored. 0036 also WIDENS uq_cost_daily
    # (10 -> 11 columns) and REBUILDS ix_markuprules_open (6 -> 7 terms). Those
    # are REPLACEMENTS -- drop + recreate -- and they stay in the alembic chain
    # where they run once, in a transaction, under review. Replaying a
    # drop/recreate here would open a window with no uniqueness on every pod
    # start and, because each statement fails SOFT, a failed recreate would leave
    # the object simply GONE with the pod healthy. See
    # tests/test_additive_migrations_are_additive.py and
    # 0015_pricing_cost_daily_shapes.
    # ⚠️ Consequence, stated so it is not discovered by surprise: a pod that
    # boots against a pre-0036 database gets the COLUMNS from this replay but NOT
    # the widened key or the rebuilt open-rule unique. Ingest keeps working
    # (which is the point); per-project aggregation and project-scoped markup
    # rules are simply not yet enforced until the migration runs.
    # --- billed_while_archived (alembic 0037) ------------------------------
    # Same outage class as reservation_id / project_id above: the flusher NAMES
    # billed_while_archived in its cost INSERT, so a pod carrying that code
    # against a pre-0037 database fails EVERY cost INSERT with UndefinedColumn --
    # a full billing-ingest outage. Byte-identical to alembic 0037's own
    # _ADD_COLUMN string.
    #
    # NULLABLE, NO DEFAULT, NO INDEX -- see 0037's module docstring: NULL means
    # "the archived-tenant check never ran on this row" (every pre-0037 row,
    # forever -- there is nothing true to backfill from), never "checked and
    # clean". billing_cost_events is RANGE-partitioned on created_at: a nullable
    # ADD COLUMN on the PARENT is catalogue-only, cascades to every existing
    # partition and is inherited by every future one.
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS billed_while_archived BOOLEAN",
    # --- vendor cost + per-actor daily rollup (alembic 0040) --------------
    # The daily rollup NAMES both objects, so a pod on that code against a pre-0040 database
    # would fail every daily rollup. Byte-identical to alembic 0040's strings; all idempotent.
    "ALTER TABLE billing_cost_daily ADD COLUMN IF NOT EXISTS total_vendor_cost_usd NUMERIC(20, 8)",
    "CREATE TABLE IF NOT EXISTS billing_cost_actor_daily ("
    "id UUID PRIMARY KEY, "
    "bucket_date DATE NOT NULL, "
    "tenant_id UUID NOT NULL, "
    "license_id UUID NOT NULL, "
    "subject_id UUID NOT NULL, "
    "actor_id UUID NOT NULL, "
    "service_key VARCHAR(64) NOT NULL, "
    "provider VARCHAR(64) NOT NULL, "
    "served_model VARCHAR(128) NOT NULL, "
    "byo BOOLEAN NOT NULL, "
    "owner_provided BOOLEAN NOT NULL, "
    "total_input_tokens BIGINT NOT NULL, "
    "total_output_tokens BIGINT NOT NULL, "
    "total_cost_usd NUMERIC(20, 6) NOT NULL, "
    "total_vendor_cost_usd NUMERIC(20, 8), "
    "request_count INTEGER NOT NULL, "
    "CONSTRAINT uq_cost_actor_daily UNIQUE (bucket_date, tenant_id, license_id, subject_id, "
    "actor_id, service_key, provider, served_model, byo, owner_provided))",
    "CREATE INDEX IF NOT EXISTS ix_cost_actor_daily_tenant_actor_date "
    "ON billing_cost_actor_daily (tenant_id, actor_id, bucket_date)",
    # --- cost attribution rules (alembic 0041) -----------------------------
    # Read by services.attribution_rules on the cost ingest path (inside a
    # SAVEPOINT, fail-soft). Byte-identical to alembic 0041's strings; both
    # idempotent, nothing dropped. Schema only — rules arrive via the admin API.
    "CREATE TABLE IF NOT EXISTS billing_attribution_rules ("
    "id UUID PRIMARY KEY, "
    "source_tenant_id UUID NOT NULL, "
    "source_subject_id UUID, "
    "match_project TEXT NOT NULL, "
    "bill_tenant_id UUID NOT NULL, "
    "actor_id UUID, "
    "reason TEXT NOT NULL, "
    "enabled BOOLEAN NOT NULL DEFAULT true, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT ck_attribution_rules_match_project CHECK "
    "(match_project IN ('any', 'with_project', 'without_project')))",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_attribution_rules_match "
    "ON billing_attribution_rules (source_tenant_id, "
    "coalesce(source_subject_id, '00000000-0000-0000-0000-000000000000'::uuid), "
    "match_project)",
    # --- billing_metering_events.retarget_tenant_id (alembic 0042) ---------
    # Same outage class as project_id / event_ts above: the flusher NAMES this
    # column in every INSERT, so a pod on this code against a pre-0042 database
    # must add it before the first flush. Byte-identical to alembic 0042's
    # string; nullable, no default, catalogue-only. ⛔ No backfill, no index.
    "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS retarget_tenant_id UUID",
    # --- EUR money budgets (alembic 0044) -----------------------------------
    # Same outage class as retarget_tenant_id above: the cost flusher NAMES the
    # three new billing_cost_events columns in every INSERT, and every ORM read
    # of tenant_config selects budget_unit. Byte-identical to 0044's strings;
    # all idempotent, nothing dropped. ⚠️ The truncate-guard sweep is NOT
    # mirrored here -- that runs once, in the alembic chain, under its bounded
    # lock_timeout.
    "CREATE TABLE IF NOT EXISTS tenant_money_budget ("
    "id UUID PRIMARY KEY, "
    "tenant_id UUID NOT NULL, "
    "currency CHAR(3) NOT NULL, "
    "amount BIGINT NOT NULL, "
    "period VARCHAR(10) NOT NULL DEFAULT 'monthly', "
    "consumed NUMERIC(20, 8) NOT NULL DEFAULT 0, "
    "consumed_period_start DATE, "
    "active BOOLEAN NOT NULL DEFAULT true, "
    "source VARCHAR(32) NOT NULL DEFAULT 'manual', "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT ck_money_budget_currency CHECK (currency = 'EUR'), "
    "CONSTRAINT ck_money_budget_amount CHECK (amount >= 0), "
    "CONSTRAINT ck_money_budget_source CHECK "
    "(source IN ('provisioning', 'conversion', 'manual')))",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_money_budget_active_tenant "
    "ON tenant_money_budget (tenant_id) WHERE active",
    "CREATE TABLE IF NOT EXISTS fx_rate ("
    "id BIGSERIAL PRIMARY KEY, "
    "base CHAR(3) NOT NULL DEFAULT 'USD', "
    "quote CHAR(3) NOT NULL DEFAULT 'EUR', "
    "rate NUMERIC(20, 10) NOT NULL, "
    "effective_from TIMESTAMP WITH TIME ZONE NOT NULL, "
    "effective_until TIMESTAMP WITH TIME ZONE, "
    "set_by TEXT, "
    "CONSTRAINT ck_fx_rate_positive CHECK (rate > 0))",
    "CREATE INDEX IF NOT EXISTS ix_fx_rate_lookup ON fx_rate (base, quote, effective_from)",
    "ALTER TABLE tenant_config ADD COLUMN IF NOT EXISTS budget_unit VARCHAR(8) "
    "NOT NULL DEFAULT 'tokens' "
    "CONSTRAINT ck_tenant_config_budget_unit CHECK (budget_unit IN ('tokens', 'money'))",
    # A one-way relaxation (allowed by test_additive_migrations_are_additive):
    # the money PUT creates tenant_config rows with markup_pct NULL = "not
    # configured", which must not fail on a pod ahead of alembic 0044.
    "ALTER TABLE tenant_config ALTER COLUMN markup_pct DROP NOT NULL",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS consumption_amount NUMERIC(20, 8)",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS consumption_currency CHAR(3)",
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS llm_setup SMALLINT "
    "CONSTRAINT ck_cost_events_llm_setup CHECK (llm_setup IN (1, 2, 3))",
    # --- multi-currency money budgets (alembic 0045, PLAN REV F) -----------
    # Byte-identical to 0045's strings. The ISO-currency and fx pair CHECKs
    # are NOT mirrored (ADD CONSTRAINT has no IF NOT EXISTS; they live in the
    # alembic chain only). The drop below is a ONE-WAY relaxation of 0044's
    # EUR-only check, so a pod ahead of 0045 can still write a USD/CAD budget.
    "CREATE TABLE IF NOT EXISTS tenant_money_budget_adjustment ("
    "id UUID PRIMARY KEY, "
    "tenant_id UUID NOT NULL, "
    "budget_id UUID NOT NULL, "
    "kind VARCHAR(32) NOT NULL, "
    "period_start DATE, "
    "from_currency CHAR(3) NOT NULL, "
    "to_currency CHAR(3) NOT NULL, "
    "fx_rate NUMERIC(30, 18) NOT NULL, "
    "usd_rate_from NUMERIC(20, 10) NOT NULL, "
    "usd_rate_to NUMERIC(20, 10) NOT NULL, "
    "amount_before BIGINT NOT NULL, "
    "amount_after BIGINT NOT NULL, "
    "consumed_before NUMERIC(20, 8) NOT NULL, "
    "consumed_after NUMERIC(20, 8) NOT NULL, "
    "remaining_before NUMERIC(20, 8) NOT NULL, "
    "remaining_after NUMERIC(20, 8) NOT NULL, "
    "actor_sub TEXT, "
    "actor_role TEXT, "
    "via_client TEXT, "
    "reason TEXT, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT ck_money_adj_kind CHECK (kind IN ('currency_conversion')), "
    "CONSTRAINT ck_money_adj_currency CHECK "
    "(from_currency IN ('EUR', 'USD', 'CAD') AND to_currency IN ('EUR', 'USD', 'CAD')), "
    "CONSTRAINT ck_money_adj_rate CHECK (fx_rate > 0))",
    "CREATE INDEX IF NOT EXISTS ix_money_adj_tenant_created "
    "ON tenant_money_budget_adjustment (tenant_id, created_at)",
    "ALTER TABLE tenant_money_budget DROP CONSTRAINT IF EXISTS ck_money_budget_currency",
    # --- billing_dlq_reattributions + billing_dlq_archive (alembic 0047) ---
    # Receipts for POST /api/admin/dlq/reattribute (#243), and the archive the
    # dead-letter age trim writes BEFORE it XTRIMs. Both are INSERTed ahead of
    # a destructive Redis op, so a pod on this code against a pre-0047 database
    # must have the tables first. Byte-identical to alembic 0047; idempotent.
    "CREATE TABLE IF NOT EXISTS billing_dlq_reattributions ("
    "id BIGSERIAL PRIMARY KEY, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "dead_entry_id VARCHAR(64) NOT NULL, "
    "original_id VARCHAR(64), "
    "replay_marker VARCHAR(128) NOT NULL, "
    "reattributed_from TEXT, "
    "tenant_id UUID NOT NULL, "
    "reattributed_by VARCHAR(255) NOT NULL, "
    "reason TEXT NOT NULL, "
    "original_fields JSONB NOT NULL, "
    "reinjected_stream_id VARCHAR(64) NOT NULL, "
    "request_id VARCHAR(64), "
    "CONSTRAINT uq_dlq_reattributions_marker UNIQUE (replay_marker))",
    "CREATE INDEX IF NOT EXISTS ix_dlq_reattributions_tenant_created "
    "ON billing_dlq_reattributions (tenant_id, created_at)",
    "CREATE TABLE IF NOT EXISTS billing_dlq_archive ("
    "id BIGSERIAL PRIMARY KEY, "
    "archived_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "stream VARCHAR(128) NOT NULL, "
    "dead_entry_id VARCHAR(64) NOT NULL, "
    "dlq_reason VARCHAR(128), "
    "dlq_at VARCHAR(64), "
    "fields JSONB NOT NULL, "
    "CONSTRAINT uq_dlq_archive_entry UNIQUE (stream, dead_entry_id))",
    "CREATE INDEX IF NOT EXISTS ix_dlq_archive_archived_at ON billing_dlq_archive (archived_at)",
    # 0050 — the licences APP registry mirror (app_key -> app_id for X-App-Id).
    # Mirrored here so a pod booting before the migration does not fail the
    # key-map read or sync-from-licenses' upsert. Same DDL as the migration.
    "CREATE TABLE IF NOT EXISTS app_registry ("
    "app_id uuid PRIMARY KEY, "
    "app_key text NOT NULL, "
    "revoked boolean NOT NULL DEFAULT false, "
    "synced_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT app_registry_app_key_key UNIQUE (app_key))",
    # --- billing_metering_events.actor_id (alembic 0052) --------------------
    # Same outage class as retarget_tenant_id: the flusher NAMES this column in
    # every INSERT. Byte-identical to 0052's string; nullable, no default,
    # catalogue-only. ⛔ No backfill, no index.
    "ALTER TABLE billing_metering_events ADD COLUMN IF NOT EXISTS actor_id UUID",
    # --- funding + billing_cost_usage_daily (alembic 0053) -----------------
    # The flusher NAMES `funding` in every cost INSERT and rollup_daily writes
    # the cross rollup, so both must exist before the migration has run.
    # Byte-identical to 0053's strings. The BACKFILLS live in 0053 only.
    "ALTER TABLE billing_cost_events ADD COLUMN IF NOT EXISTS funding VARCHAR(32) "
    "CONSTRAINT ck_cost_events_funding "
    "CHECK (funding IN ('local_gpu', 'platform_external', 'tenant_byo'))",
    "CREATE TABLE IF NOT EXISTS billing_cost_usage_daily ("
    "id UUID PRIMARY KEY, "
    "bucket_date DATE NOT NULL, "
    "tenant_id UUID NOT NULL, "
    "project_id UUID NOT NULL, "
    "actor_id UUID NOT NULL, "
    "app_id UUID NOT NULL, "
    "funding VARCHAR(32) NOT NULL, "
    "provider VARCHAR(64) NOT NULL, "
    "served_model VARCHAR(128) NOT NULL, "
    "event_count INTEGER NOT NULL, "
    "total_input_tokens BIGINT NOT NULL, "
    "total_output_tokens BIGINT NOT NULL, "
    "total_cost_usd NUMERIC(20, 6) NOT NULL, "
    "CONSTRAINT uq_cost_usage_daily UNIQUE (bucket_date, tenant_id, project_id, actor_id, "
    "app_id, funding, provider, served_model))",
    "CREATE INDEX IF NOT EXISTS ix_cost_usage_daily_tenant_date "
    "ON billing_cost_usage_daily (tenant_id, bucket_date)",
    # --- tenant_credit_ledger + billing_invoices.prepaid_credit_applied_usd
    # (alembic 0055). Prepaid credit: close_period writes the invoice column
    # and the ledger's drawdown row, so both must exist before the migration
    # has run. Byte-identical to 0055's strings; the truncate guard is 0055's.
    "CREATE TABLE IF NOT EXISTS tenant_credit_ledger ("
    "id UUID PRIMARY KEY, "
    "tenant_id UUID NOT NULL, "
    "kind VARCHAR(24) NOT NULL, "
    "entry_key VARCHAR(160) NOT NULL, "
    "amount_usd NUMERIC(20, 8) NOT NULL, "
    "period_start DATE, "
    "invoice_id UUID, "
    "source_amount BIGINT, "
    "source_currency CHAR(3), "
    "fx_rate NUMERIC(20, 10), "
    "access_charge_id BIGINT, "
    "actor_sub TEXT, "
    "actor_role TEXT, "
    "via_client TEXT, "
    "created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "
    "CONSTRAINT ck_credit_ledger_kind CHECK "
    "(kind IN ('topup', 'usage_drawdown', 'drawdown_reversal')), "
    "CONSTRAINT ck_credit_ledger_amount CHECK (amount_usd >= 0), "
    "CONSTRAINT ck_credit_ledger_shape CHECK "
    "((kind = 'topup' AND amount_usd > 0 AND source_amount > 0 "
    "AND source_currency IN ('EUR', 'USD', 'CAD') AND fx_rate > 0) "
    "OR (kind IN ('usage_drawdown', 'drawdown_reversal') "
    "AND period_start IS NOT NULL AND invoice_id IS NOT NULL)), "
    "CONSTRAINT uq_credit_ledger_entry UNIQUE (tenant_id, entry_key))",
    "CREATE INDEX IF NOT EXISTS ix_credit_ledger_tenant_created "
    "ON tenant_credit_ledger (tenant_id, created_at)",
    "ALTER TABLE billing_invoices ADD COLUMN IF NOT EXISTS prepaid_credit_applied_usd "
    "NUMERIC(20, 8) NOT NULL DEFAULT 0",
)

logger = structlog.stdlib.get_logger(__name__)

# Race-class driver errors: the SQLSTATEs a benign multi-replica DDL race
# produces — 42710 duplicate_object, 42P07 duplicate_table, 42701
# duplicate_column, 23505 unique_violation.
#
# Matched on the *class name* of SQLAlchemy's wrapped driver exception
# (``exc.orig``) rather than on a SQLSTATE/pgcode. This repo has no existing
# SQLSTATE-inspection idiom in ``src/`` (no ``sqlstate``/``pgcode`` reference
# anywhere); the only established precedent is reaching through the SQLAlchemy
# wrapper to ``.orig`` for the driver exception, so we follow that instead of
# inventing a pgcode map. psycopg3 names these classes after the SQLSTATE
# condition, so the two are one-to-one.
_RACE_CLASS_ORIG_NAMES: frozenset[str] = frozenset(
    {"DuplicateObject", "DuplicateTable", "DuplicateColumn", "UniqueViolation"}
)


def _is_race_class_error(exc: BaseException) -> bool:
    """True when ``exc`` wraps a duplicate-object / unique-violation driver error."""
    orig = getattr(exc, "orig", None)
    # `orig` is None for SQLAlchemy-level errors that never reached the driver,
    # so test it explicitly rather than relying on the getattr default.
    return orig is not None and type(orig).__name__ in _RACE_CLASS_ORIG_NAMES


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


async def init_db(settings: Settings) -> None:
    """Create the async engine and session factory."""
    global _engine, _session_factory  # noqa: PLW0603

    engine_kwargs: dict[str, object] = {
        "echo": False,
        "pool_size": 5,
        "max_overflow": 10,
        "pool_pre_ping": True,
    }

    _engine = create_async_engine(settings.database_url, **engine_kwargs)
    _session_factory = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    import models.db  # noqa: F401

    try:
        async with _engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except SQLAlchemyError:
        # Benign in multi-replica startups: another replica may have
        # created the tables between our check and our CREATE TABLE. Any
        # other SQLAlchemy-level DDL failure is also non-fatal here because
        # the readiness probe will catch a real connectivity problem.
        logger.debug("table_creation_skipped_likely_race_condition", exc_info=True)

    # Idempotent additive migrations for tables created before later
    # columns were declared on the model. create_all only creates missing
    # tables — never adds columns. Each statement is safe to repeat.
    await apply_additive_migrations(_engine)


async def apply_additive_migrations(engine: AsyncEngine) -> None:
    """Apply the idempotent additive migrations against the given engine.

    Extracted into a function so tests can call it directly.

    Uses a single AUTOCOMMIT connection so each statement still commits in its
    own implicit transaction (preserving the per-statement-isolation contract
    that lets a single benign failure not abort the rest), while collapsing the
    ~80 connection-acquire + BEGIN + COMMIT round-trips that the prior
    per-statement `engine.begin()` loop incurred. On the Tailnet-fronted CI
    postgres this cut the schema-backfill test cascade that was causing
    pytest-timeout pile-ups.

    Sets a 5min `statement_timeout` so a truly-hung ALTER cannot hold an
    AccessExclusiveLock indefinitely (deadlock, dead connection holding the
    lock, etc.) and block the next test's TRUNCATE. 5min is generous enough
    to cover legitimately heavy CREATE INDEX / data-backfill statements
    while still catching the truly-stuck case.
    """
    async with engine.connect() as raw_conn:
        conn = await raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        try:
            await conn.exec_driver_sql("SET statement_timeout = '5min'")
        except SQLAlchemyError:
            logger.debug("statement_timeout_set_failed", exc_info=True)
        try:
            await _run_additive_migrations(conn)
        finally:
            # ⛔ RESET, NOT OPTIONAL — and it cannot be `SET LOCAL` instead.
            #
            # This connection is AUTOCOMMIT, so `SET LOCAL` would die with the
            # implicit single-statement transaction and never reach the ALTERs
            # below. A plain `SET` is therefore correct here — but a plain SET
            # is SESSION-scoped, and SQLAlchemy's `reset_on_return="rollback"`
            # only issues a ROLLBACK on check-in, which does not undo a GUC set
            # outside a transaction. MEASURED on postgres 18: set 5min, close
            # the connection, check it out again from the same pool — it still
            # reads 5min.
            #
            # In production that is merely untidy (one long-lived engine). In
            # the SUITE it is the timeout half of the truncate mystery:
            # tests/test_schema_backfill.py calls this with the SHARED,
            # session-scoped `db_engine`, so from then on a pooled connection
            # carries a statement_timeout that nothing in the run asked for.
            # Any later test that draws it — including `db_engine`'s
            # `TRUNCATE ... CASCADE`, which blocks on ACCESS EXCLUSIVE if any
            # connection is sitting in a leaked transaction — dies with
            # `canceling statement due to statement timeout` in SETUP, on a
            # test that has nothing to do with the backfill.
            #
            # Resetting on the way out keeps the guard where it belongs (these
            # statements) and hands the pool back a connection with the
            # server's own default.
            try:
                await conn.exec_driver_sql("RESET statement_timeout")
            except SQLAlchemyError:
                logger.debug("statement_timeout_reset_failed", exc_info=True)


async def _run_additive_migrations(conn: AsyncConnection) -> None:
    """Execute every additive statement, failing soft on each."""
    for stmt in ADDITIVE_MIGRATIONS:
        try:
            await conn.exec_driver_sql(stmt)
        except SQLAlchemyError as exc:
            # Keep failing soft either way — one statement must never take
            # down startup — but split the LEVEL, because the two cases are
            # not the same event.
            #
            # Race class (duplicate object/table/column, unique violation):
            # benign in multi-replica startups, another replica already did
            # it, the end state is correct. Stays at debug to keep prod logs
            # quiet.
            #
            # Anything else: the statement did NOT apply and nothing else
            # will notice. A silent additive failure is indistinguishable
            # from success — the schema just stays wrong until some later,
            # unrelated-looking query fails against the missing column or
            # the wrong type. That must be visible in prod, so it logs at
            # warning with the (truncated) statement and the driver
            # exception class.
            if _is_race_class_error(exc):
                logger.debug("additive_migration_skipped", stmt=stmt, exc_info=True)
            else:
                logger.warning(
                    "additive_migration_failed",
                    stmt=stmt[:120],
                    error_class=type(getattr(exc, "orig", None) or exc).__name__,
                    exc_info=True,
                )


async def get_session() -> AsyncGenerator[AsyncSession]:
    """FastAPI dependency that yields an async session."""
    if _session_factory is None:
        raise RuntimeError("Database not initialised - call init_db() first")
    async with _session_factory() as session:
        yield session


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the session factory for background tasks."""
    if _session_factory is None:
        raise RuntimeError("Database not initialised - call init_db() first")
    return _session_factory


async def get_engine_for_check() -> bool:
    """Execute a trivial query to verify the database connection."""
    if _engine is None:
        return False
    try:
        async with _engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        logger.debug("health_check_failed", exc_info=True)
        return False


async def dispose_engine() -> None:
    """Dispose of the engine pool on shutdown."""
    global _engine, _session_factory  # noqa: PLW0603
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
