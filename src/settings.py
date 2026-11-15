"""Application settings using Pydantic BaseSettings."""

from __future__ import annotations

import logging
import re
from datetime import UTC, date, datetime
from functools import lru_cache
from typing import ClassVar, Literal
from urllib.parse import urlsplit

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Substrings that mark a database_url as a development placeholder rather than
# a real production credential. Production must refuse these explicitly so a
# misconfigured pod fails fast at startup instead of routing billing data
# through credentials that won't actually authenticate.
_PLACEHOLDER_DSN_MARKERS = (":password@", "db.example.com")

# Keycloak hosts that issue PRODUCTION tokens. A process wired to one of these
# is serving real tenants, whatever its ENVIRONMENT tag says, so bearer auth
# may never be disabled there (see ``_refuse_auth_disabled_in_production``).
_PRODUCTION_KEYCLOAK_HOSTS = frozenset({"auth.nonagentic.ai"})

_logger = logging.getLogger(__name__)

#: Accepted values of ``BILLING_PROJECT_OWNERSHIP_MODE`` (case-insensitive).
PROJECT_OWNERSHIP_MODES: frozenset[str] = frozenset({"observe", "enforce"})

#: Floor on :attr:`Settings.retention_days`. ``retention_days`` does not only
#: gate a row DELETE — ``rollup_engine.drop_old_cost_partitions`` turns it into
#: ``DROP TABLE`` against whole ``cost_events`` partitions, and a DROP is seen by
#: NEITHER post-incident control: ``truncate_guard`` is a BEFORE TRUNCATE trigger
#: and ``delete_audit`` is DELETE/TRUNCATE-only (its own module docstring says so:
#: "DROP TABLE fires no row or statement DELETE trigger, so retention is invisible
#: to this control by construction"). So an unsafe value here is unrecoverable
#: financial data loss that leaves ``billing_delete_audit`` correctly empty — a
#: destroyed day is indistinguishable from a healthy one. ``retention_days = 0``
#: makes the cutoff ``now``, which ages out EVERY completed month at once.
#: 30 = one whole month, the shortest window the monthly RANGE partitioning can
#: express at all; below it the drop set can only grow. This is the belt; the
#: braces is the per-run bound inside ``drop_old_cost_partitions`` itself, which
#: a caller passing an argument directly cannot bypass.
MIN_RETENTION_DAYS = 30

#: Floor on :attr:`Settings.actor_rollup_retention_months`.
#:
#: ⛔ DERIVED FROM THE PURPOSE, NOT PICKED. The actor rollup exists so a
#: per-member chargeback line on an ISSUED invoice can still be evidenced when
#: the customer disputes it. Invoices are immutable and gaplessly numbered
#: (``services.invoices.issue``), so the evidence has to outlive the dispute
#: window, not the reporting window. A full PRIOR calendar year is disputable
#: right up to the last day of the current one, which is 12 (prior year) + 12
#: (current year) + 1 (the month in hand) = 25 — the default. 13 is the point
#: below which a January dispute about the previous January can no longer be
#: answered at all, so anything under it defeats the table's reason to exist.
#:
#: ⚠️ UNLIKE :data:`MIN_RETENTION_DAYS` THIS GUARDS A ROW ``DELETE``, NOT A
#: ``DROP TABLE``. ``billing_cost_actor_daily`` is a plain table (see
#: ``ADDITIVE_MIGRATIONS`` in ``src/db.py`` — ``CREATE TABLE``, no ``PARTITION
#: BY``), so the purge is visible to ``billing_delete_audit`` and to the alembic
#: 0017 truncate guard in a way a partition drop is NOT. That makes an unsafe
#: value here loud rather than silent — but it is still unrecoverable, and the
#: rollup cannot be rebuilt once ``billing_cost_events`` has aged out from under
#: it (raw retention is 90d; this window is ~760d), so it is refused at
#: construction for the same reason.
MIN_ACTOR_ROLLUP_RETENTION_MONTHS = 13

_FIRST_BILLABLE_PERIOD_RE = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


def _first_billable_period_start(raw: str) -> date:
    """``YYYY-MM`` -> the first day of that month; anything else raises.

    Shared by the ``invoice_first_billable_period`` validator and the property
    that reads it, so "is it well-formed" and "what date is it" cannot disagree.
    """
    if not isinstance(raw, str) or not _FIRST_BILLABLE_PERIOD_RE.fullmatch(raw):
        raise ValueError(
            f"invoice_first_billable_period must be YYYY-MM (month 01-12), got {raw!r}"
        )
    # date() itself refuses year 0000, which the pattern cannot.
    return date(int(raw[:4]), int(raw[5:7]), 1)


class Settings(BaseSettings):
    """Billing service configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    service_name: str = "billing-service"
    port: int = Field(default=8157, description="HTTP listen port")
    metrics_port: int = Field(
        default=0,
        description=(
            "When non-zero, ALSO serve the Prometheus registry on this dedicated "
            "port. `0` (the default) disables the second listener and preserves "
            "today's behaviour exactly. "
            "⭐ WHY IT EXISTS: `/metrics` is currently mounted on the SAME FastAPI "
            "app as the whole API on port 8157, and namespace `prometheus` is NOT "
            "in the ambient mesh, so its scrape traffic can never match a SPIFFE "
            "`principals:` rule. Any east-west authorization policy written to "
            "permit the scrape therefore has to permit the port — which permits "
            "EVERY billing endpoint on it, including the budget and cost-event "
            "writes. Serving the registry on its own port is what lets 8157 be "
            "narrowed to the real callers while the scrape keeps working."
        ),
    )
    database_url: str = Field(
        default="postgresql+psycopg://billing_user:password@db.example.com:6432/billing",  # pragma: allowlist secret
        description="PostgreSQL connection string (psycopg3 driver)",
    )
    # Leader election (T1.2) needs a DIRECT postgres connection — pgbouncer
    # txn-mode would lose the session-level advisory lock between checkouts.
    # Production: leave as None; the loop derives one from database_url via
    # services.leader_election.direct_postgres_dsn (rewrites the host to
    # `direct_postgres_host`). Tests + non-prod environments where postgres
    # is reachable directly on the same URL set this explicitly to bypass
    # the rewrite.
    leader_election_dsn: str | None = Field(
        default=None,
        description=(
            "Direct-postgres DSN for advisory-lock leader election. "
            "When None, derived from database_url (pooler host -> "
            "direct_postgres_host)."
        ),
    )
    redis_url: str = Field(
        default="",
        description="Redis URL for streams and counters",
    )
    licenses_url: str = Field(
        default="",
        description="Licenses service base URL for billing queries",
    )
    fr_base_url: str = Field(
        default="",
        description=(
            "fr backend base URL. Billing reads the project -> tenant map from "
            "`GET {fr_base_url}/api/internal/projects/tenant-map` "
            "(services.project_owner_map). Empty = ownership check has no answer "
            "(counted as billing_project_id_refused_total{reason=owner_map_unavailable}, "
            "project_id KEPT)."
        ),
    )
    fr_internal_token: str = Field(
        default="",
        description="Sent as `X-Internal-Token` to fr's internal tenant-map endpoint.",
    )
    billing_project_ownership_mode: Literal["observe", "enforce"] = Field(
        default="observe",
        description=(
            "What to do when fr's registry POSITIVELY says an event's project_id "
            "belongs to a different tenant (`foreign_tenant`) or is not a known "
            "project (`unknown_project`). `observe` (default): count + stamp "
            "metadata.project_id_ownership, KEEP project_id. `enforce`: NULL "
            "project_id and stamp metadata.project_id_refused. Shipped in observe "
            "because on 2026-09-28 ~94% of project-attributed $ was mismatched by "
            "an emitter-side tenant defect (conveyor bills …0003 projects under the "
            "n8n-dev licence); enforcing then would erase the signal. Arm only once "
            "foreign_tenant is near zero. Case-insensitive; an unrecognised "
            "value degrades to `observe` with a WARNING (never a startup crash: "
            "a typo must not take billing down, and must not start erasing "
            "attribution either)."
        ),
    )
    # Direct-postgres host:port that ``direct_postgres_dsn`` rewrites the
    # pgbouncer DSN to. Required in production; tests pass `leader_election_dsn`
    # explicitly so this can stay empty in dev/test.
    direct_postgres_host: str = Field(
        default="",
        description=(
            "host:port of the session-mode pooler used by leader election "
            "(NOT the transaction pooler). Example: "
            "'pgbouncer-session.example.com:6432'."
        ),
    )

    def resolve_leader_election_dsn(self) -> str:
        """Return the DSN the advisory-lock leader election must use.

        Single source of truth for all three leader-elected loops
        (``run_rollup_loop``, ``run_watcher_loop`` and ``run_invoice_close_loop``),
        which previously each re-derived it.

        Raises :class:`ValueError` when the configuration cannot produce a valid
        DSN. **Call this at BOOT** (see ``main.lifespan``): the loops that use it
        are ``asyncio.create_task``'d, so a ValueError raised inside one of them
        would kill that task *silently* — the ``tasks`` list holds a reference so
        asyncio never reports "exception was never retrieved", and the shutdown
        ``gather`` uses ``return_exceptions=True``. A misconfigured DSN would then
        mean the hourly rollup and the budget-overage watcher simply never run,
        with no log line at all.

        ``main.lifespan`` logs CRITICAL on failure and, in ``production``,
        re-raises so the pod crashloops rather than sitting Ready with two dead
        financial loops. Outside production it logs and continues: dev/test/CI run
        with no session-mode pooler (``DIRECT_POSTGRES_HOST`` empty) and do not
        need leader election.
        """
        from services.leader_election import direct_postgres_dsn

        if self.leader_election_dsn:
            return self.leader_election_dsn
        return direct_postgres_dsn(self.database_url, self.direct_postgres_host)

    # Keycloak base URL for JWKS lookup in dashboard auth (auth.py). The legacy
    # ``KEYCLOAK_URL`` plain env var is still honored at the call site for
    # backwards compatibility with the existing deployment manifest.
    keycloak_url: str = Field(
        default="",
        description="Keycloak base URL for JWKS verification",
    )

    # Portal clients whose access token may be RELAYED as the owner in
    # ``X-Forwarded-User-Authorization`` (currency change + PUT /api/config/fx-rates).
    # Matched against the verified token's ``azp`` -- the Keycloak client the
    # super_admin signed in through. Every relay today originates in fr's SPA:
    # fr backend forwards the browser's own bearer (``ownerPortalToken``), and
    # provisioning relays that same token unchanged. ``fr-frontend`` is the
    # VITE_KEYCLOAK_CLIENT_ID baked into fr's prod build (fr ci-cd.yaml) and the
    # portal-realm public client whose redirect URI is https://fr.nonagentic.ai/*.
    # A super_admin token minted for any OTHER portal client (commonpractice,
    # kag-frontend, w2v-frontend, ...) is refused. Comma-separated; empty refused.
    forwarded_owner_allowed_azp: str = Field(
        default="fr-frontend",
        description="Comma-separated portal client ids (azp) allowed to relay an owner token",
    )

    @property
    def forwarded_owner_allowed_azps(self) -> frozenset[str]:
        return frozenset(
            p.strip() for p in self.forwarded_owner_allowed_azp.split(",") if p.strip()
        )

    @field_validator("forwarded_owner_allowed_azp")
    @classmethod
    def _forwarded_owner_azp_not_empty(cls, v: str) -> str:
        if not any(p.strip() for p in v.split(",")):
            raise ValueError("forwarded_owner_allowed_azp must name at least one portal client")
        return v

    # Stream config
    events_stream: str = Field(default="billing:events", description="Redis stream for API events")
    costs_stream: str = Field(default="billing:costs", description="Redis stream for cost events")
    cost_dead_letter_stream: str = Field(
        default="billing:costs:dead",
        description="Redis stream for malformed cost events (quarantined, never billed)",
    )
    dead_letter_stream_max_age_hours: int = Field(
        default=720,
        description="Age-trim window (hours) for billing:costs:dead; 30d default.",
    )
    consumer_group: str = Field(default="billing-service", description="Redis consumer group name")
    consumer_name: str = Field(
        default="worker-0", description="Redis consumer name (unique per replica)"
    )
    # Active-mode gate. When False the Redis stream consumer AND the budget
    # overage watcher do NOT start — used for the metering→billing carve Phase 2
    # read-only deploy, so billing serves carved data without (a) joining the
    # shared consumer group "metering-service" and stealing events from the live
    # metering consumer, or (b) re-firing overage webhooks metering already sent.
    # Flip to True only at Phase 3 cutover, after the metering consumer is stopped.
    consumer_enabled: bool = Field(
        default=True, description="Start the stream consumer + budget watcher (active mode)"
    )
    # PEL reclaim (durability FIX A2). An ungraceful pod kill (OOM/SIGKILL/
    # node-loss) leaves already-XREADGROUP'd messages stranded in the consumer's
    # pending-entries-list forever — the steady-state ">" read skips pending, so
    # nothing reclaims them = silent under-bill. The consumer runs an XAUTOCLAIM
    # drain on startup and on this interval to pull stranded entries back through
    # the same persist path (the idempotency inbox makes reprocessing safe).
    consumer_pel_reclaim_min_idle_seconds: int = Field(
        default=300,
        description=(
            "Minimum idle time before a pending stream entry is reclaimed via "
            "XAUTOCLAIM. Must exceed the normal read→flush→ack latency so a "
            "freshly-read in-flight entry is never stolen mid-flush; only entries "
            "stranded by a crashed consumer (idle past this) are reclaimed."
        ),
    )
    consumer_pel_reclaim_interval_seconds: float = Field(
        default=300.0,
        description=(
            "How often the consumer runs the XAUTOCLAIM PEL-drain. Kept "
            ">= the min-idle window so a just-claimed entry (idle reset to 0) is "
            "not re-staged before the flusher acks it."
        ),
    )
    # FIX A1 — age-bound the cost stream from the consumer loop (not only the
    # leader-gated rollup loop, whose pgbouncer-session advisory-lock election
    # can be down). XTRIM MINID is idempotent, so both replicas trimming is
    # harmless. Must keep the resulting stream age (COST_STREAM_MAX_AGE_HOURS,
    # 36h) strictly below the inbox prune horizon (PROCESSED_MESSAGE_RETENTION_
    # DAYS, 48h) — see services.rollup_engine.
    cost_stream_trim_interval_seconds: float = Field(
        default=300.0,
        description=(
            "How often the consumer loop age-trims the cost Redis stream "
            "(XTRIM MINID ~ now-COST_STREAM_MAX_AGE_HOURS). Runs on every "
            "replica; idempotent. Independent of the rollup-loop leadership so "
            "the trim still happens when leader election is unavailable."
        ),
    )

    # Batching
    batch_size: int = Field(default=500, description="Max events per batch flush")
    flush_interval_seconds: float = Field(default=5.0, description="Seconds between batch flushes")
    rollup_interval_seconds: float = Field(
        default=300.0, description="Seconds between hourly rollups"
    )
    leader_retry_interval_seconds: float = Field(
        default=15.0,
        description=(
            "How long a NON-leader replica waits before re-attempting to acquire the "
            "advisory lock, in the leader-elected loops (rollup + budget watcher). "
            "Deliberately much shorter than the loop's own interval: anything that "
            "drops the leader's session (pod rollout, pgbouncer-session restart) makes "
            "every survivor a follower, and if a follower slept its FULL interval "
            "before re-racing for the lock nobody would do the work for up to that "
            "long. Observed 2026-07-13: pgbouncer-session was restarted, both billing "
            "pods logged leader_election_error, the ROLLUP lock erred at 18:47:41 and "
            "no replica reacquired until 18:52:42 — 301s (= one rollup_interval_seconds) "
            "with no rollup leader. Cost of the shorter poll is one "
            "pg_try_advisory_lock per follower per retry interval: a sub-millisecond "
            "in-memory lock-table probe. 10-30s is the sensible band."
        ),
    )
    retention_days: int = Field(
        default=90,
        description=(
            "Days to keep raw events. Drives BOTH a row DELETE (metering_events) "
            "and an irreversible DROP TABLE of whole cost_events partitions, which "
            "no audit trigger can see — floored at MIN_RETENTION_DAYS so an unsafe "
            "value cannot be constructed."
        ),
    )
    actor_rollup_retention_months: int = Field(
        default=25,
        description=(
            "Months of ``billing_cost_actor_daily`` to keep. ⭐ ITS OWN WINDOW, "
            "DELIBERATELY DECOUPLED FROM `retention_days`: that one is 90 days "
            "and drives an irreversible DROP of whole `billing_cost_events` "
            "partitions, so tying the per-actor rollup to it would age out "
            "per-member chargeback evidence three months after the invoice was "
            "issued. ⛔ The rollup table shipped (alembic 0040) with NO retention "
            "at all -- it grew forever, which is not 'safe', it is an unbounded "
            "table on the same worker4 volume as the prod primary. "
            "25 months = a full PRIOR calendar year (12) + the current one (12) "
            "+ the month in hand (1), so a dispute raised on 31 December about "
            "the previous January can still be evidenced. Purged by the SAME "
            "daily rollup tick as `drop_old_cost_partitions` "
            "(`services.aggregator.run_rollup_loop`), so the two retention "
            "windows cannot drift onto different schedules. Floored at "
            "MIN_ACTOR_ROLLUP_RETENTION_MONTHS."
        ),
    )
    cost_partition_months_ahead: int = Field(
        default=3,
        description=(
            "How many months ahead the rollup loop pre-creates cost_events "
            "partitions (cost_events is monthly RANGE-partitioned; an INSERT "
            "with no covering partition hard-fails). 0012 documented a gitops "
            "cron for this that was never built. 3 keeps a rolling runway."
        ),
    )
    daily_reconcile_days: int = Field(
        default=3,
        description=(
            "Trailing window of complete days the daily rollup re-rolls every pass. "
            "missing_daily_dates only fills ABSENT dates; a daily row first written "
            "from incomplete hourly (early 'yesterday' roll) and later corrected by a "
            "hourly backfill would otherwise never reconcile. rollup_daily UPSERTs the "
            "recomputed sum, so re-rolling a complete day is idempotent."
        ),
    )
    rollup_catchup_max_days: int = Field(
        default=14,
        ge=1,
        description=(
            "Hard cap on how many daily buckets ONE catch-up pass rolls. The "
            "candidate set is bounded only by retention (up to "
            "min(retention_days, MAX_CATCHUP_DAYS)=92 dates), and each date costs a "
            "delete + a partition-scanning re-aggregate, so an uncapped pass after a "
            "long outage is an unbounded unit of work inside a leader tick that also "
            "owns retention cleanup and partition creation. The cap is NOT a ceiling "
            "on catch-up: services.rollup_engine.catchup_daily rolls OLDEST-FIRST and "
            "reports `complete=False` when it stops short, and "
            "services.aggregator.run_rollup_loop then declines to mark the calendar "
            "day done — so the next cycle resumes draining minutes later instead of "
            "waiting a day. 14 drains a 92-day backlog in 7 cycles."
        ),
    )
    audit_retention_days: int = Field(
        default=180,
        description=(
            "Days to keep audit_log rows. Purged daily from the rollup loop. "
            "Set to 0 to disable retention (tests / manual runs)."
        ),
    )

    # Service account (Keycloak client credentials) for internal API calls
    keycloak_token_url: str = Field(
        default="",
        description="Keycloak token endpoint for client credentials grant",
    )
    service_client_id: str = Field(
        default="billing-service",
        description="Keycloak client ID for service-to-service auth",
    )
    service_client_secret: str = Field(
        default="",
        description="Keycloak client secret for service-to-service auth",
    )

    # --- Commercial terms: the DEFAULTS a new tenant is born on -------------
    #
    # Decided by George 2026-09-10: markup 20% (local AND BYO), platform fee
    # $5,000/month flat with usage billed on top, seat fee $25, one seat = one
    # active licence. These four are the *fallbacks*; the per-tenant truth is
    # effective-dated rows in `markup_rules` / `tenant_access_charge`,
    # materialised by `POST /api/config/tenants/{id}/terms`.
    #
    # ⛔ `default_markup_pct` IS THE ONLY PLACE A GLOBAL MARKUP CAN LIVE, and
    # that is not a style choice. An all-wildcard `markup_rules` row looks like
    # the obvious home and is SILENTLY IGNORED: `resolve_billed_line` only
    # honours a resolved rule when `_specificity(rule) > 0` (services/markup.py),
    # so a global row matches, scores 0, and loses to the fallback it was meant
    # to replace. Measured 2026-09-10: `markup_rules` held 4 rows and 0 open, and
    # `tenant_config` held 0 rows, so every line in the estate billed at this
    # constant with `markup_source=settings_default`.
    default_markup_pct: float = Field(
        default=20.0, description="Default markup percentage on internal costs"
    )
    default_byo_markup_pct: float = Field(
        default=20.0,
        description=(
            "Default markup percentage applied to BYO lines. Separate from "
            "default_markup_pct because BYO is its own commercial axis: under "
            "billing_mode byo_platform_fee a BYO line historically carried a "
            "FORCED 0% and only the absolute surcharge, so a tenant's BYO rate "
            "was not expressible at all. Materialised as a byo-scoped "
            "markup_rules row; this is only the seed."
        ),
    )
    default_platform_fee_usd: float = Field(
        default=5000.0,
        description=(
            "Default monthly tenant access charge in USD -- what George calls "
            "the platform fee. ⚠️ NOT the same field as the code's "
            "`platform_fee_usd`, which is the per-request absolute surcharge a "
            "BYO tenant pays under byo_platform_fee mode. This one becomes a "
            "`tenant_access_charge` row (cadence=monthly, included_cost_usd "
            "NULL => flat, usage billed on top) and lands on the invoice as "
            "`access_charge_usd`."
        ),
    )
    default_seat_fee_usd: float = Field(
        default=25.0,
        description=(
            "Default per-seat monthly charge in USD. One seat = one ACTIVE "
            "LICENCE owned by a non-archived kind='customer' tenant (see "
            "services.seats). Materialised as a tenant_access_charge row with "
            "cadence='per_seat'."
        ),
    )

    # Observability
    otlp_traces_endpoint: str = Field(
        default="",
        description="OTLP HTTP endpoint for trace export",
    )
    environment: str = Field(default="production", description="Deployment environment tag")
    # Read here ONLY so startup can refuse an unsafe combination; the bypass
    # itself is evaluated in auth.py from the same env var. Kept as a raw
    # string and parsed with auth.py's exact expression (``.lower() == "true"``)
    # so the two can never disagree: a pydantic ``bool`` would read "1" as
    # enabled while auth.py reads it as DISABLED.
    dashboard_auth_enabled: str = Field(
        default="true",
        description="DASHBOARD_AUTH_ENABLED; anything but 'true' disables bearer auth",
    )
    log_level: str = Field(default="INFO", description="Root log level")
    log_format: str = Field(
        default="json",
        description="Log format: 'json' for production, 'console' for local dev",
    )

    budget_watcher_interval_seconds: int = Field(
        default=60,
        description=(
            "How often the T1.6 budget overage watcher ticks on the leader. "
            "Lower for faster threshold detection at the cost of DB load."
        ),
    )
    platform_budget_webhook_url: str = Field(
        default="",
        description=(
            "Optional platform-wide sink for budget threshold-crossing webhooks. "
            "When set (together with platform_budget_webhook_secret), EVERY tenant's "
            "crossing also POSTs here in addition to any per-tenant tenant_webhooks "
            "rows — used to fan budget alerts into the notification hub for all "
            "tenants (incl. future ones) without per-tenant registration. Empty "
            "disables the sink."
        ),
    )
    platform_budget_webhook_secret: str = Field(
        default="",
        description=(
            "HMAC-SHA256 signing key for platform_budget_webhook_url; must match the "
            "receiver's verification secret. The sink is inert unless BOTH the url "
            "and this secret are set."
        ),
    )
    cost_attribution_app_by_service_key: str = Field(
        default=(
            "inference-router=Workers Module,"
            "worker-management=Workers Module,"
            "kag-graph=KAG Module,"
            "hirag=KAG Module,"
            "karma=KAG Module,"
            "ingestion=KAG Module,"
            # `kag` (bare, distinct from `kag-graph`) was absent from this map
            # entirely -- measured 2026-09-04, 30d: 4 events, 100% license-NULL.
            # Every sibling service_key in the kag family already resolves to
            # KAG Module, so this closes the one gap in that family rather than
            # introducing a new mapping decision.
            "kag=KAG Module,"
            "w2v-tts=Web to Video,"
            # `w2v-image` was absent from this map -- measured 2026-09-04, 30d:
            # 247 events / $2.47, 19% license-NULL. Evidence for the target app,
            # not a name guess: the two emitting tenants
            # (0195f48b-d000-7000-8000-000000000099, and portal-render-e2e —
            # e148e8af-3751-49bf-a0fa-56ab46d31ddc when measured, re-keyed to
            # 01a1113b-674f-72b0-a84b-dc746d39afca by alembic 0049) each hold
            # an ACTIVE "Web to Video" licence. The
            # licenses DB also carries a distinctly-named "W2V Image (P1 E2E)"
            # app (019f4198-fbed-717a-92ee-97a5ce8868bd) that the service_key
            # string superficially matches -- checked and ruled out: it has
            # ZERO license grants fleet-wide (`select count(*) from license
            # where app_id = ...` = 0), so mapping to it would send every
            # w2v-image event to NULL instead of resolving the 81% that
            # currently do resolve by other means. A name match is not
            # evidence; the licence a tenant actually holds is.
            "w2v-image=Web to Video,"
            # `mlx-ingestion-backend` maps to KAG Module even though a distinct
            # "MLX Ingestion Backend" app row exists (01a06d42-5a10-700b-8041-
            # cb07ae9da638) -- investigated, not blind-changed. Of its 4
            # emitting tenants (30d, 484 events): the two carrying 92.6% of the
            # volume (099: 444 events, 101: 4 events) each hold an ACTIVE KAG
            # Module licence, so KAG Module resolves them correctly today. The
            # other two (001: 35 events, 3f7c1e02...: 1 event) hold NO KAG
            # Module licence -- but "MLX Ingestion Backend" has ZERO licence
            # grants fleet-wide (same check as w2v-image above), so remapping
            # would not attribute those tenants either; it would only break the
            # 92.6% that currently resolves. Left unchanged.
            "mlx-ingestion-backend=KAG Module,"
            # commonpractice emits its OWN cost events with `license_id: ''`
            # hardcoded -- `commonpractice backend/src/host/metering-emit.ts:80`,
            # comment "CP has no per-call license id" -- and its outbound LLM
            # call attaches no identity either: `ai/llm/adapters/openai.ts`
            # `endpoint()` sets only content-type + authorization, so
            # inference-router cannot attribute it on the wire either. (Positive
            # control that this is a real absence and not a bad grep: the same
            # repo DOES send `X-Tenant-Id` from `host/storage-writer.ts:20-22`.)
            # Measured 2026-09-02 over 12h: 154 events, 154 unattributed (100%).
            # This entry is the only lever that needs no change in that repo.
            # Safe because the resolution is UNAMBIGUOUS, verified live: the two
            # tenants carrying 105 of those 154 rows each hold EXACTLY ONE active
            # "Common Practice" licence (candidates_for_pair = 1), and the
            # resolver refuses an ambiguous pair by design rather than guessing.
            # It does NOT close the remaining 49 rows: those 7 tenants carry
            # subject_id == tenant_id (a tenant echoed into the subject field,
            # not a real identity) and hold no licence at all, so nothing can
            # attribute them and they must stay visibly unattributed.
            "commonpractice=Common Practice,"
            # `fr-backend` was absent from this map -- measured 2026-09-13, 24h:
            # 8,257 events, 100% license-NULL, and it is now 99.6% of ALL usage-bus
            # volume (fr's emitter began self-identifying on 2026-09-11 via fr #1094;
            # before that the same traffic landed under `unmapped:api`, which is why a
            # 7-day aggregate still shows the old key). Adding the entry is what makes
            # billing_service's own usage-path licence resolution -- called from
            # `event_flusher` as of round 8 -- able to attribute that volume at all.
            #
            # ⛔ EVIDENCE IS THE LICENCE THE EMITTING TENANTS ACTUALLY HOLD, not the
            # name, and here the name would NOT have been enough: unlike the w2v-image
            # and mlx-ingestion decoys above, this decoy is NOT empty. A distinctly
            # named "fr Frontend" app exists (9611059e-dafa-455a-a6c2-2a6a6a0be4d1) and
            # it DOES carry live grants, so "it has zero licences" cannot rule it out.
            # The coverage measurement does, decisively. Of the 6 real tenants emitting
            # `fr-backend` (…0001 2890, …0002 2453, …0099 2156, …0003 349, …0004 210,
            # …0005 105 events/24h):
            #     FR Module    12 active grants fleet-wide -- held by 6 of 6
            #     fr Frontend   3 active grants fleet-wide -- held by 1 of 6
            # So FR Module resolves every emitter and fr Frontend would resolve one.
            # Each of the 6 holds EXACTLY ONE active FR Module licence, so every pair is
            # unambiguous (candidates_for_pair = 1) and the resolver's refuse-on-
            # ambiguity branch is not relied upon. The service_key is fr's BACKEND and
            # "fr Frontend" is the separate frontend product; that reading agrees with
            # the measurement but is not what decides it.
            #
            # ⚠️ It does NOT close the whole key. A 7th emitter is the nil-UUID sentinel
            # (94 events/24h): no tenant, so nothing can attribute it and it must stay
            # visibly unattributed -- same shape as commonpractice's residual 49 above.
            "fr-backend=FR Module,"
            # `w2v-tts-byo` -- `w2v-tts` was a live collision:
            # modality_gateway/src/services/metering.py and
            # tts_backend/src/services/metering.py both shipped the identical
            # literal `w2v-tts`, and the two have different billing semantics
            # (tts_backend meters every call; modality_gateway meters only its
            # BYO leg). Measured 2026-09-07, 7d: all 146 rows under `w2v-tts`
            # were `provider='kokoro'` (tts_backend only) -- zero
            # modality_gateway rows existed under that key, so renaming
            # modality_gateway's BYO leg was safe. CONFIRMED, not speculative:
            # modality_gateway#37 (`a4431d23a36940c65addb764513cad7f81b82980`,
            # merged to its `main` 2026-09-07) renamed the gateway's
            # `SERVICE_KEY` to exactly this literal, and added
            # `tests/test_service_key_drift.py` there pinning
            # `EXPECTED_SERVICE_KEYS["tts"] == "w2v-tts-byo"` so it can never
            # silently drift back to `w2v-tts`. tts_backend is unchanged and
            # keeps `w2v-tts` -- it is the canonical/original owner.
            "w2v-tts-byo=Web to Video,"
            # `w2v-html-render` was absent from this map entirely -- pricing
            # exists for it (scripts/initial_pricing.yaml) but with no
            # attribution entry `license_resolver` can never map its cost
            # events to a licence, so `license_id` resolves NULL and the spend
            # never reaches an invoice line. html_render_service is a Web to
            # Video sub-service, the same umbrella app that already covers
            # `w2v-tts`/`w2v-tts-byo`/`w2v-image`, so it maps to the same App.
            "w2v-html-render=Web to Video,"
            # `w2v-video-compiler` and `w2v-video-analyzer` were absent from
            # this map. Both emitters are w2v_common receivers, and w2v_common
            # `middleware.py:965-972` DELIBERATELY binds no licence on a
            # DELEGATED (customer-tenant) call (issue #170, 724 mis-billed
            # rows) -- so a delegated row arrives here with license_id NULL and
            # this map is the ONLY thing that can attribute it. Measured
            # 2026-09-16, 30d: merqi's (639c333a-...) one w2v run produced a
            # `w2v-tts` row that resolved (`w2v-tts+resolved`, licence
            # 01a09ac6-...) and two `w2v-video-compiler` rows that stayed
            # license_id/app_id NULL -- same tenant, same run, same licence.
            #
            # Evidence for the target app, not a name guess (verified live
            # 2026-09-16, billing + licenses DBs): every self-attributed row
            # under both keys carries app_id f825f6b0-bb2f-4386-bce2-
            # e6091a7b98cd = `licenses.app` key `w2v`, name "Web to Video" --
            # w2v-video-compiler 365/367 (the other 2 are the NULL merqi rows),
            # w2v-video-analyzer 600/600. No distinctly-named decoy app exists
            # for either key. Literals confirmed on each repo's origin/main:
            # video_compiler_service src/services/metering.py:46,
            # video_analyzer_service src/services/metering.py:49.
            #
            # NOT added, on purpose: `w2v-ai-content` (ai_content_generation_
            # service metering.py:30) and `w2v-transcription` (transcription_
            # worker_service metering.py:29) -- zero cost events under either
            # key in the whole billing_cost_events table, so there is no
            # measured licence to map them to; a map entry here decides which
            # invoice line real money lands on and is not made from a name.
            "w2v-video-compiler=Web to Video,"
            "w2v-video-analyzer=Web to Video"
        ),
        description=(
            "CSV of `service_key=App Name` pairs used to resolve a licence for "
            "cost events that arrive with NO license_id. The mapping is EXPLICIT "
            "and deliberately not inferred from the service_key: an inferred "
            "mapping is how a charge silently lands on the wrong invoice line, "
            "and this value decides which licence a real cost is billed against. "
            "A service_key absent from this map is never resolved -- its events "
            "stay unattributed and keep incrementing "
            "billing_license_unattributed_persisted_total, which is the honest "
            "outcome rather than a guess. That is deliberately NOT "
            "billing_license_unattributed_total: despite its name, that one "
            "fires at INGRESS, before this map is consulted at all, so it counts "
            "the events this setting is ABOUT TO resolve as well as the ones it "
            "cannot."
        ),
    )
    budget_write_requires_registered_tenant: bool = Field(
        default=True,
        description=(
            "Refuse a token-budget write whose tenant_id no registry vouches for "
            "(422 on POST /api/budget; skip-and-count inside "
            "POST /api/budget/sync-from-licenses). ON by default, which is safe "
            "ONLY because the verdict is freshness-gated: "
            "`tenancy.write_gate_refuses_tenant` allows the write whenever the "
            "live tenant overlay is stale or was never loaded, so a licences "
            "outage degrades to the old behaviour rather than refusing every "
            "budget write in the estate. The staleness threshold is derived as "
            "3x `tenant_registry_refresh_seconds`, so one missed refresh still "
            "permits a refusal and a sustained outage does not. "
            "WHY THE STAKES DIFFER FROM `billing_exclude_unregistered_tenants` "
            "(also ON): that flag withholds a MONEY RECORD from billing, so a "
            "false positive there under-bills until someone reverses the park. "
            "This one refuses to create a PAYER ROW, and a false positive costs "
            "one retry. Set to "
            "False only to unblock a writer while a registry problem is "
            "diagnosed -- and expect unowned budget rows while it is off."
        ),
    )
    app_registry_refresh_seconds: int = Field(
        default=300,
        description=(
            "Interval for `services.app_registry.run_app_registry_refresh_loop`, "
            "which mirrors `GET {licenses_url}/internal/apps` into `app_registry` "
            "-- the table the X-App-Id key -> app uuid map is built from. Without "
            "it the mirror only moves when POST /api/budget/sync-from-licenses is "
            "called, so an app registered in licences afterwards stores its spend "
            "with app_id NULL. Matches the key map's 300s TTL. First tick runs at "
            "startup; floored at 30s; 0 (or negative) disables the loop. The loop "
            "writes ONLY app_registry, never request_quota_config."
        ),
    )
    tenant_registry_refresh_seconds: int = Field(
        default=300,
        description=(
            "Interval for `services.tenant_names.run_tenant_registry_refresh_loop`, "
            "which refreshes `tenancy`'s live NAME -> uuid overlay from "
            "`GET {licenses_url}/internal/tenants`. Matches the 5-min TTL on the "
            "map's own cache, so a tick is always a genuine fetch rather than a "
            "re-read. Floored at 30s in the loop. This is NOT a freshness "
            "guarantee for resolution: a missed refresh degrades to the "
            "checked-in `LEGACY_TENANT_NAMES` snapshot, never to a rejection, so "
            "the cost of a long interval is a slower pickup of newly registered "
            "tenants and nothing else."
        ),
    )
    billing_money_budgets_enabled: bool = Field(
        default=False,
        description=(
            "Expose the EUR money-budget routes (POST /api/budget/money/reserve, "
            "GET /api/budget/money/quote, PUT /api/budget/money/{tenant_id}, GET "
            "/api/budget/money/status/{tenant_id}, POST /api/budget/money/{tenant_id}/"
            "currency, POST /api/budget/money/topup, GET /api/budget/money/credit/"
            "{tenant_id}); off -> 404 on all seven. NB the prepaid-credit top-up is NOT "
            "gated by BUDGET_TOPUP_ENABLED (that flag refuses the FREE token top-up; "
            "this one bills a one_time access charge). ⚠️ Invoice-side netting of "
            "existing credit (services.prepaid_credit) is NOT gated by this flag either: "
            "credit that was paid for keeps being honoured while the routes are off. "
            "Tenant inference provisioning, PLAN rev E §2.1. ⚠️ Settlement of an "
            "already-money tenant's cost events is NOT gated by this flag: turning "
            "the routes off is the documented rollback (IR fails open on the money "
            "gate for setups 1/3, closed for setup 2), and the ledger must keep "
            "debiting what was actually spent while it is off."
        ),
    )
    budget_topup_enabled: bool = Field(
        default=False,
        description=(
            "Allow POST /api/budget/topup/{tenant_id}. Off (the default) -> 409 "
            "`topup_unavailable` BEFORE any budget row, audit row or Redis write. "
            "⛔ George ruling 2026-10-02: no free top-ups until a real payment "
            "gateway exists. fr already hides/refuses top-up (fr#1351), but this "
            "route is reachable directly (e.g. the n8n metering node op "
            "`topupTenantBudget`), so the refusal lives here, at the source. Arm "
            "only once top-ups are paid for. NB scope: this gates the top-up "
            "ROUTE only; `POST /api/budget` is a separate internal admin route "
            "and is not gated by this flag."
        ),
    )
    budget_require_reservation_handle: bool = Field(
        default=False,
        description=(
            "Route settlement by reservation IDENTITY instead of by anonymous "
            "pooled drain. ⭐ CORRECTED: this flag NO LONGER gates minting the "
            "handle or maintaining the handle index — `reserve_budget` mints on "
            "every successful reserve and records the hold in the identified "
            "protocol's hash-tagged keys in BOTH modes (see "
            "`services/budget_redis._RESERVE_SHADOW_LUA`). It gated all three at "
            "once, which made handle coverage impossible to observe until the "
            "moment of arming and made arming flip three structures in one step. "
            "What it selects now is WHICH SCRIPT TAKES THE ADMISSION VERDICT, and "
            "therefore which pool a settlement drains — one structure. "
            "⭐ MEASURE BEFORE ARMING, which is now possible: "
            "`billing_budget_reservation_minted_total` counts real mints rather "
            "than the flag (it read a flat 0 in prod because the increment sat "
            "inside the flag's own branch), so it should track successful "
            "reserves; `..._expired_total` should roughly equal it while this is "
            "False, with `..._settled_total` flat at 0, because nothing can name "
            "a shadow hold. The end-to-end producer round-trip "
            "(POST /budget/reserve -> billing_cost_events.reservation_id -> the "
            "`handles` map) is read from the `reservation_id IS NOT NULL` share of "
            "`billing_cost_events` until a named/unnamed counter exists on the "
            "pooled release path. "
            "When "
            "False (the default) release behaves EXACTLY as today: a settlement is "
            "an anonymous drain against the one pooled per-tenant `reserved` "
            "integer, so a release can take tokens another caller reserved "
            "(measured: 151.6 harmful over-releases/hour against 329 reserves/hour) "
            "and the spend cap fails OPEN by the overshoot. When True a settlement "
            "releases ONLY the reservations it names by handle, and tokens that "
            "arrive with no handle never touch the pool at all. "
            "⚠️ FLIPPING THIS TRUE IS THE MOMENT THE CAP STARTS FAILING CLOSED: "
            "until then every unattributed release is forgiven, and after it every "
            "unattributed release is ignored, so in-flight tokens that no "
            "settlement claims stay held instead of being handed back. "
            "⭐ THE SAFETY OF THE TRUE MODE RESTS ON PER-RESERVATION TTL EXPIRY "
            "(`BudgetReserveRequest.ttl_seconds`, default 60s), NOT on producer "
            "coverage. Do not reason about it as 'safe once every caller passes a "
            "handle' — a caller that never settles, crashes, or is a version behind "
            "costs at most one TTL of held headroom, which the expiry sweep "
            "reclaims (counted on "
            "billing_budget_reservation_expired_total). That bound holds with zero "
            "producers updated, which is why this can be armed before the fleet is."
        ),
    )
    billing_exclude_health_probes: bool = Field(
        default=True,
        description=(
            "Park self-metering health-probe cost events instead of billing "
            "them. When True (the default) an event whose "
            "(subject_id, service_key, endpoint) matches an entry in "
            "health_probes.HEALTH_PROBE_EMITTERS is STORED with "
            "excluded_from_billing=true, exclusion_reason='health_probe', is "
            "withheld from the token rollup and the budget debit, and is "
            'counted on billing_cost_events_health_probe_total{action="excluded"}. '
            "When False the match is still computed and still counted "
            '(action="counted") but nothing else changes — behaviour is '
            "byte-identical to before this code existed. "
            "⛔ THIS IS A KILL SWITCH, NOT AN ARMING GATE, and it shipped ON "
            "from day one, whereas billing_exclude_unregistered_tenants shipped "
            "OFF and was only armed (2026-09-16) once its input became "
            "trustworthy. The two are not the same shape and must not be "
            "reasoned about together. That gate's INPUT was, at the time, a "
            "hand-refreshed registry snapshot covering every tenant in the "
            "estate, which was 11 days stale and missing SIX registered names on "
            "2026-09-05, so arming it then would have parked real customers' "
            "spend. THIS rule's "
            "input is a THREE-ENTRY hand-verified allowlist of two internal "
            "service accounts, each measured directly against the prod billing "
            "primary on 2026-09-07, and it cannot match anything else: a "
            "subject absent from that list bills unconditionally, whatever it "
            "sends. The estate rule 'DO NOT CLOSE A GATE BEFORE ITS COUNTER "
            "READS 0' is about gates that fail in the expensive direction on "
            "input they cannot trust; a gate whose entire input is three "
            "measured uuids has no such denominator to wait on, and shipping it "
            "off would leave 82% of billing_cost_events synthetic for no "
            "safety gained. "
            "MEASURED COST OF THE DEFAULT (7d, prod): ~104,000 rows leave the "
            "billable population, of which ~$0.18 across 347 rows is genuine "
            "non-probe traffic from those same two service accounts — all of it "
            "cost_basis='internal_transfer', none of it customer cash, all of "
            "it still stored and reversible with one UPDATE. See "
            "src/health_probes.py for the full derivation and for why the "
            "residue exists at all."
        ),
    )
    billing_exclude_unregistered_tenants: bool = Field(
        default=True,
        description=(
            "PARK cost events whose tenant_id no registry vouches for. ON by "
            "default, and ARMED IN PRODUCTION SINCE 2026-09-16 (gitops #3906, "
            "BILLING_EXCLUDE_UNREGISTERED_TENANTS=true). When True the cost "
            "flusher stores the row with excluded_from_billing=true, "
            "exclusion_reason='unregistered_tenant', withholds it from the token "
            "rollup and the budget debit, and labels "
            'billing_cost_events_unregistered_tenant_total{action="excluded"}. '
            "When False the registry check still runs and the counter still "
            'moves with action="counted", but the row bills, rolls up and '
            "debits exactly as if the check did not exist. "
            "WHY PARK, NOT REJECT: dead-lettering an unregistered tenant's event "
            "would destroy the money record with no un-park path (billing is "
            "append-only with no refund path); a parked row stays in the "
            "ledger, stays auditable, and is reversible with one UPDATE "
            "(docs/UNREGISTERED_TENANT_GATE.md). The stricter reject-at-"
            "resolution switch is billing_tenant_id_classification_enforce "
            "below, and it stays OFF. "
            "WHY ON BY DEFAULT: the gate's input is now trustworthy. "
            "tenancy.is_registered_tenant_id consults the live licences overlay "
            "(GET {licenses_url}/internal/tenants) FIRST, with the hand-refreshed "
            "tenancy._REGISTERED_TENANT_IDS snapshot only as the outage fallback, "
            "reported per check on billing_tenant_registration_source_total"
            "{source}. Arming criterion met in prod, measured 2026-10-15: over "
            '7d {source="live"}=47,238, {source="snapshot_degraded"}=0, '
            '{source="snapshot"}=1; over 14d '
            "billing_cost_events_unregistered_tenant_total=0. "
            "TO DISARM: set BILLING_EXCLUDE_UNREGISTERED_TENANTS=false (e.g. "
            '{source="snapshot_degraded"} climbing during a sustained licences '
            "outage, which would make the stale snapshot the verdict). Rows "
            "already parked are reversed with the UPDATE in the doc. "
            "Pre-arming history (it shipped OFF until the live overlay landed): "
            "docs/UNREGISTERED_TENANT_GATE.md."
        ),
    )
    billing_tenant_id_classification_enforce: bool = Field(
        default=False,
        description=(
            "Arm tenancy.canonical_tenant_id to REJECT (return None -> the "
            "caller's normal 'unresolvable tenant' path: a 400 at an HTTP "
            "boundary, dead-lettered to billing:costs:dead from the Redis "
            "consumer) any well-formed uuid that "
            "tenancy.classify_tenant_id names as anything other than "
            "'registered' -- i.e. 'synthetic', 'v5_derived', 'v7', 'v4' or "
            "'other'. When False (the default) classification still runs and "
            "still counts on "
            "billing_tenant_id_classification_total{classification}, but "
            "resolution is BYTE-IDENTICAL to before this setting existed: "
            "canonical_tenant_id keeps returning the id, counted-not-rejected, "
            "exactly as tenancy._note_unregistered_tenant's docstring requires. "
            "⛔ SHIPPED OFF DELIBERATELY, AND FOR THE MOST EXPENSIVE POSSIBLE "
            "REASON: this is a REJECTION at the resolution boundary, one layer "
            "upstream of billing_exclude_unregistered_tenants (which parks an "
            "already-stored row) -- turning this on for a tenant that turns out "
            "to be real production spend does not park it, it DEAD-LETTERS it, "
            "which destroys the money record with no un-park path, because "
            "billing is append-only with no refund path. See "
            "tenancy.canonical_tenant_id's own module-level ⛔ for why the "
            "function was written to never do this unconditionally. "
            "ARMING CRITERION, same shape as billing_exclude_unregistered_"
            "tenants above: "
            "billing_tenant_id_classification_rejected_total (a shadow counter "
            "that increments on every id this WOULD refuse, whether or not the "
            "gate is armed) must be reviewed by class over a representative "
            "window first -- 'synthetic' rejecting is close to free (those "
            "values are fixture leakage by definition), but 'v5_derived' and "
            "'v7' are exactly the shape of a tenant that was legitimately "
            "minted and simply never made it into the licences registry, and "
            "rejecting THOSE by default is how a registry sync lag becomes a "
            "revenue-destroying outage. Arm per-class if the fleet ever needs "
            "that granularity; today it is one switch for all four unregistered "
            "classes because none of them has yet been reviewed enough to be "
            "trusted alone."
        ),
    )
    internal_secret: str = Field(
        default="",
        description=(
            "Shared secret guarding /api/internal/* endpoints used by internal "
            "callers as an alternative to the Keycloak SA-JWT. When empty, the "
            "secret-based fallback rejects every request — set this via "
            "INTERNAL_SECRET in the deployment."
        ),
    )

    # Monthly invoice-close loop (see services.invoice_closer)
    invoice_close_enabled: bool = Field(
        default=False,
        description=(
            "Arm the monthly invoice-close loop to actually run. When False "
            "(the default) the loop is not scheduled at all. "
            "⛔ DEFAULT FALSE ON FIRST DEPLOY, DELIBERATELY: closing a period "
            "issues an invoice by drawing the next value from a GAPLESS "
            "invoice-number counter row (services.invoices.issue), and a gap "
            "burned into that series is not something a later fix can undo — "
            "it is audit evidence, not noise. A buggy loop shipped already "
            "armed can burn numbers, or close the wrong period, before anyone "
            "has watched it complete a single successful run. Do not arm a gate "
            "that can consume an irreversible resource before its behaviour "
            "has been observed. Arm only after a dry/manual run has been "
            "watched to behave."
        ),
    )
    invoice_close_interval_seconds: float = Field(
        default=3600.0,
        description="Seconds between invoice-close loop ticks on the leader.",
    )
    licence_budget_reconcile_enabled: bool = Field(
        default=True,
        description=(
            "Arm the licence↔budget reconcile loop "
            "(services/licence_budget_reconcile.py). Default TRUE, unlike the "
            "mutating gates below it, because with "
            "licence_budget_reconcile_deactivate_enabled left at its own "
            "default the loop only MEASURES: it reads the licence registry and "
            "publishes billing_licence_budget_divergent_total. A detector that "
            "ships disarmed is a detector nobody arms, and the failure this "
            "guards accumulated silently for months precisely because nothing "
            "was looking."
        ),
    )
    licence_budget_reconcile_deactivate_enabled: bool = Field(
        default=False,
        description=(
            "Let the reconcile loop actually deactivate budgets whose licence "
            "is no longer active upstream. Default FALSE, same convention as "
            "invoice_close_enabled: do not "
            "arm an unattended mutation before a real cycle has been watched. "
            "⚠️ Leaving this false is NOT the safe steady state -- it means "
            "divergence is measured and then left in place, so "
            "billing_licence_budget_divergent_total will sit non-zero forever "
            "and the money keeps being spendable. Arm it once one measure-only "
            "cycle has been observed to report the count you expect. The "
            "mutation is a soft delete (active=False) and is reversible by "
            "setting active=true on the audited ids."
        ),
    )
    licence_budget_reconcile_interval_seconds: float = Field(
        default=900.0,
        description=(
            "Seconds between licence↔budget reconcile ticks on the leader. "
            "This is the upper bound on how long a budget can stay spendable "
            "after its licence stopped being active via a path the push-side "
            "teardown does not cover -- most importantly the nightly "
            "reap_expired_licences CronJob, which writes status='revoked' "
            "straight to the database."
        ),
    )
    ledger_truncate_acknowledged_through: datetime | None = Field(
        default=datetime(2026, 9, 17, 17, 18, 12, 655017, tzinfo=UTC),
        description=(
            "High-water mark for ALLOWED ledger TRUNCATEs a human has already "
            "investigated. ledger_guard_audit warns only on billing_delete_audit "
            "TRUNCATE rows with ts AFTER this instant; the all-time gauge "
            "billing_delete_audit_rows{operation='TRUNCATE'} is unaffected. The "
            "default is the exact ts of the last known TRUNCATE row (2026-09-17 "
            "17:18:12.655017Z, measured in prod; incidents 2026-09-05 + 09-17). Move "
            "it forward only after investigating a newer one; null = warn on every "
            "TRUNCATE row ever recorded."
        ),
    )
    quota_config_sync_enabled: bool = Field(
        default=True,
        description=(
            "Arm the leader-elected loop that moves request_quota_config.quota_monthly "
            "to the licence registry's EFFECTIVE limit (quota_override wins over "
            "plan.quota_monthly) for existing, active mirror rows. It never inserts, "
            "deletes or deactivates a row. Runs once at startup, then every "
            "quota_config_sync_interval_seconds."
        ),
    )
    quota_config_sync_interval_seconds: float = Field(
        default=900.0,
        gt=0,
        description=(
            "Seconds between quota-config value repair ticks on the leader: the upper "
            "bound on how long a changed quota_override stays unenforced."
        ),
    )
    licence_budget_reconcile_max_deactivations_per_cycle: int = Field(
        default=50,
        description=(
            "Hard ceiling on how many budgets one reconcile tick may "
            "deactivate. A cycle that would exceed it deactivates NOTHING, "
            "logs loudly and increments "
            "billing_licence_budget_reconcile_failures_total{reason="
            "'deactivation_cap_exceeded'}; the divergence gauge still "
            "publishes and the freshness gauge is deliberately NOT advanced. "
            "⚠️ THAT ONLY 'AGES OUT AND PAGES' BECAUSE A RULE SELECTS THOSE "
            "SERIES, AND THIS SENTENCE USED TO CLAIM IT WITHOUT ONE. Measured "
            "by an audit: `git grep licence_budget` in gitops-production "
            "@origin/main returned ZERO hits -- no rule selected the freshness "
            "gauge, the divergence gauge or the cap reason, so a capped cycle "
            "stalled in silence and this comment was the only thing saying "
            "otherwise. A cap with no alert converts a loud wipe into a quiet "
            "one. The rule that makes the claim true is "
            "BillingLicenceBudgetReconcileStalled, in "
            "clusters/strato/services/prometheus/rules-b.yaml key "
            "billing-licence-budget-reconcile.yml. ⛔ If that rule is ever "
            "removed, delete this sentence with it rather than leaving the "
            "claim standing. "
            "This is a blast-radius bound, not a rate limit: the set-emptiness "
            "guards refuse the KNOWN mass-deactivation shapes, and this "
            "catches the ones nobody has thought of yet -- any future change "
            "upstream that makes a large slice of the registry read as "
            "non-active at once. Default 50: comfortably above the 19-row "
            "backlog measured on production 2026-09-10 that the first armed "
            "cycle must clear, and above the handful the nightly "
            "reap_expired_licences CronJob produces, while an estate-wide wipe "
            "(258 licences measured 2026-09-08) is hundreds and is refused. "
            "Raise it deliberately and temporarily to clear a genuine large "
            "backlog, having first read the ids the measure-only mode logged."
        ),
    )
    invoice_close_lag_days: int = Field(
        default=3,
        description=(
            "Settle delay for the invoice-close loop: a billing period is "
            "only closed once period_end + invoice_close_lag_days is in the "
            "past. Rollup rows for a period can still land after period_end "
            "(late flush, retried consumer, clock skew across replicas), so "
            "closing strictly on period_end would risk drawing an invoice "
            "number before that period's usage has fully arrived. This many "
            "days of slack lets late rollup rows settle before the period is "
            "treated as final."
        ),
    )
    invoice_first_billable_period: str | None = Field(
        default=None,
        description=(
            "First BILLABLE calendar month, as YYYY-MM (env "
            "INVOICE_FIRST_BILLABLE_PERIOD). Periods STRICTLY BEFORE it are not "
            "billable: the scheduled sweep (services.invoices.close_due_periods) "
            "never attempts to close them, and the backlog census "
            "(services.invoice_closer._measure_invoice_close_lag) never counts "
            "them as lag. Unset (the default) means no floor -- behaviour is "
            "exactly as before the setting existed. "
            "Why it exists: the source cost events for 2026-08 and 2026-09 were "
            "purged on 2026-09-30 with owner approval (billing was "
            "pre-production, no tenants), so billing_cost_daily has no rows for "
            "2026-09-01..29. assert_no_bucket_gap refuses that close -- "
            "correctly -- and no backfill can ever satisfy it, so without a "
            "floor BillingInvoiceCloseBehindSchedule fires forever over a "
            "month nobody will ever bill. "
            "⛔ This is NOT a gap bypass. A period AT or AFTER the floor goes "
            "through every gate unchanged and is still refused on a bucket "
            "gap; a MANUAL close (POST via routers/billing.py -> close_period) "
            "does not consult this setting at all. "
            "⚠️ Raising it past a month that genuinely owes money silences "
            "the only signal that the month was never invoiced. "
            "A malformed value refuses to construct Settings (the pod crashes "
            "at startup) rather than being read as 'no floor'."
        ),
    )

    # Service URLs / DSN hosts that MUST be set in production via the
    # Deployment env block. Per gitops-production/docs/config-externalization-policy.md:
    # source code carries empty-string placeholders; the manifest is the only
    # place real values live.
    _REQUIRED_IN_PRODUCTION: ClassVar[tuple[str, ...]] = (
        "redis_url",
        "licenses_url",
        "direct_postgres_host",
        "keycloak_url",
        "otlp_traces_endpoint",
    )

    @field_validator("billing_project_ownership_mode", mode="before")
    @classmethod
    def _tolerant_project_ownership_mode(cls, v: object) -> str:
        """Normalise case; an unknown value falls back to ``observe`` + WARNING.

        A ``Literal`` alone made ``ENFORCE`` (or any typo) a pydantic
        ValidationError at settings construction, i.e. billing crashed at
        startup over a flag whose safe default is well defined. Runs once per
        Settings construction (``get_settings`` is cached), so the warning is
        logged once at startup.
        """
        norm = str(v).strip().lower() if v is not None else ""
        if norm in PROJECT_OWNERSHIP_MODES:
            return norm
        _logger.warning(
            "BILLING_PROJECT_OWNERSHIP_MODE=%r is not one of %s; falling back to 'observe'",
            v,
            sorted(PROJECT_OWNERSHIP_MODES),
        )
        return "observe"

    @field_validator("default_markup_pct", "default_byo_markup_pct")
    @classmethod
    def _markup_pct_in_range(cls, v: float, info: ValidationInfo) -> float:
        if v < 0 or v > 1000:
            raise ValueError(f"{info.field_name} must be in [0, 1000], got {v}")
        return v

    @field_validator("default_platform_fee_usd", "default_seat_fee_usd")
    @classmethod
    def _commercial_fee_not_negative(cls, v: float, info: ValidationInfo) -> float:
        """Refuse a negative fee at CONSTRUCTION, like the markup bound above.

        A negative access charge would be a credit nobody decided, applied to
        every tenant materialised after it, and `markup_pct` is already
        `Field(ge=0)` on the wire (models/domain.py) -- so accepting a negative
        here would make the env the only unguarded way in.
        """
        if v < 0:
            raise ValueError(f"{info.field_name} must be >= 0, got {v}")
        return v

    @field_validator("invoice_first_billable_period")
    @classmethod
    def _first_billable_period_is_a_calendar_month(cls, v: str | None) -> str | None:
        """Refuse anything that is not exactly ``YYYY-MM`` at CONSTRUCTION.

        ⛔ No stripping, no empty-string-means-unset, no ``YYYY-MM-DD``
        truncation. A floor that silently reads as "no floor" re-arms a pager
        nobody can clear; one silently read as a DIFFERENT month stops billing
        a month that owes money. Either is worse than a pod that will not start.
        """
        if v is None:
            return None
        _first_billable_period_start(v)
        return v

    @property
    def invoice_first_billable_period_start(self) -> date | None:
        """First day of :attr:`invoice_first_billable_period`, or ``None`` if unset.

        The ONE place the string becomes a date, so the two consumers compare
        ``period_start < floor`` against the same value. Re-checks the shape
        rather than trusting the validator: this model does not validate on
        assignment, so a value set after construction never passed through it.
        """
        raw = self.invoice_first_billable_period
        if raw is None:
            return None
        return _first_billable_period_start(raw)

    @field_validator("retention_days")
    @classmethod
    def _retention_days_at_least_one_month(cls, v: int) -> int:
        """Refuse a retention window that would DROP live cost partitions.

        See :data:`MIN_RETENTION_DAYS`. This is a construction-time refusal on
        purpose: the pod crashes at startup on a misconfigured RETENTION_DAYS
        rather than silently destroying months of cost attribution on the first
        daily rollup tick and logging one INFO line about it.
        """
        if v < MIN_RETENTION_DAYS:
            raise ValueError(
                f"retention_days must be >= {MIN_RETENTION_DAYS} (one whole month), got {v}. "
                "It drives DROP TABLE of cost_events partitions, which is irreversible "
                "and invisible to both the truncate guard and the delete audit."
            )
        return v

    @field_validator("actor_rollup_retention_months")
    @classmethod
    def _actor_rollup_retention_covers_a_prior_year(cls, v: int) -> int:
        """Refuse a window too short to answer a dispute about the prior year.

        Same construction-time-refusal discipline as
        :meth:`_retention_days_at_least_one_month` directly above, and for the
        same reason: the pod crashes at startup on a misconfigured
        ``ACTOR_ROLLUP_RETENTION_MONTHS`` rather than silently deleting months of
        per-member chargeback evidence on the first daily rollup tick and logging
        one INFO line about it.

        ⛔ 0 IS NOT "DISABLED" HERE, and that asymmetry with
        ``audit_retention_days`` (where 0 genuinely disables the purge) is
        deliberate: this window is expressed in MONTHS, so 0 would place the
        cutoff at the start of the CURRENT month and delete every completed
        month in one tick. A value that reads like an off-switch and behaves like
        a wipe is refused rather than interpreted.
        """
        if v < MIN_ACTOR_ROLLUP_RETENTION_MONTHS:
            raise ValueError(
                f"actor_rollup_retention_months must be >= "
                f"{MIN_ACTOR_ROLLUP_RETENTION_MONTHS}, got {v}. Below it a dispute "
                "about the prior calendar year can no longer be evidenced, which "
                "is the only reason billing_cost_actor_daily is retained at all."
            )
        return v

    @model_validator(mode="after")
    def _refuse_auth_disabled_in_production(self) -> Settings:
        """Never boot a production process with bearer auth switched off.

        ``DASHBOARD_AUTH_ENABLED=false`` makes every bearer-protected route
        accept anonymous callers as an admin (auth.py ``_decode_bearer_token``).
        Refused when EITHER signal says production, so one mis-set value cannot
        arm the bypass on its own:

        * ``ENVIRONMENT`` is production — it DEFAULTS to production, so an
          unset tag fails closed (normalised, so "Production " counts too);
        * ``KEYCLOAK_URL`` points at a production issuer host — a pod
          verifying real tenants' tokens is production even if mis-tagged.

        Raising here aborts the FastAPI lifespan, so uvicorn logs the reason
        and exits non-zero instead of serving unauthenticated.
        """
        if self.dashboard_auth_enabled.lower() == "true":
            return self
        reasons = []
        if self.environment.strip().lower() in {"production", "prod"}:
            reasons.append(f"ENVIRONMENT={self.environment!r}")
        host = (urlsplit(self.keycloak_url.strip()).hostname or "").lower()
        if host in _PRODUCTION_KEYCLOAK_HOSTS:
            reasons.append(f"KEYCLOAK_URL host {host!r} is a production issuer")
        if reasons:
            raise ValueError(
                "FATAL: DASHBOARD_AUTH_ENABLED="
                f"{self.dashboard_auth_enabled!r} disables bearer auth, which is "
                f"refused in production ({'; '.join(reasons)}). Remove "
                "DASHBOARD_AUTH_ENABLED or set it to 'true'; auth may only be "
                "disabled for local dev/test with a non-production ENVIRONMENT "
                "and KEYCLOAK_URL."
            )
        return self

    @model_validator(mode="after")
    def _enforce_production_required_fields(self) -> Settings:
        """Production must not boot with placeholder defaults.

        ``database_url`` and ``internal_secret`` both have permissive defaults
        for local dev. In production those defaults would either fail to
        authenticate or silently disable the internal API. Refuse to construct
        a Settings instance for ``ENVIRONMENT=production`` if either
        is unset/placeholder — the pod crashes at startup, and we know.

        Service URLs in :attr:`_REQUIRED_IN_PRODUCTION` carry empty defaults
        and likewise must be supplied via the Deployment env block.
        """
        if self.environment != "production":
            return self
        if not self.database_url or any(
            marker in self.database_url for marker in _PLACEHOLDER_DSN_MARKERS
        ):
            raise ValueError(
                "database_url is required in production and must not contain a "
                f"placeholder ({', '.join(_PLACEHOLDER_DSN_MARKERS)})"
            )
        if not self.internal_secret:
            raise ValueError(
                "internal_secret is required in production — set INTERNAL_SECRET in the deployment"
            )
        missing = [n for n in self._REQUIRED_IN_PRODUCTION if not getattr(self, n)]
        if missing:
            raise ValueError(
                f"Settings missing required fields in production: {missing}. "
                "Set them via env vars on the Deployment (see gitops-production)."
            )
        return self

    def model_post_init(self, __context):
        """Normalize database URL to use psycopg3 async driver."""
        if self.database_url.startswith("postgresql+asyncpg://"):
            self.database_url = self.database_url.replace(
                "postgresql+asyncpg://", "postgresql+psycopg://", 1
            )
        elif self.database_url.startswith("postgresql://"):
            self.database_url = self.database_url.replace(
                "postgresql://", "postgresql+psycopg://", 1
            )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
