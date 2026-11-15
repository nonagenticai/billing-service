"""FastAPI application with async lifespan for the billing service."""

import asyncio
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path
from typing import Any, cast

import structlog
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from prometheus_client import start_http_server
from redis.exceptions import RedisError
from sqlalchemy.exc import DataError, SQLAlchemyError
from starlette.middleware import Middleware
from starlette.status import HTTP_200_OK, HTTP_503_SERVICE_UNAVAILABLE

# Importing observability.metrics registers the custom collectors with the
# default Prometheus registry as a side effect; the Instrumentator below picks
# them up automatically.
import observability.metrics as _metrics  # noqa: F401, E402  -- side-effect import
from db import dispose_engine, get_engine_for_check, init_db
from logging_config import configure_logging
from middleware.logging import RequestLoggingMiddleware
from middleware.request_id import RequestIdMiddleware
from middleware.security_headers import SecurityHeadersMiddleware
from models.domain import HealthResponse, LivenessResponse, ReadinessResponse
from redis_client import check_redis, close_redis, init_redis
from routers.admin import router as admin_router
from routers.audit import router as audit_router
from routers.billing import router as billing_router
from routers.budget import router as budget_router
from routers.config import router as config_router
from routers.costs import router as costs_router
from routers.dashboard import router as dashboard_router
from routers.export import router as export_router
from routers.money_budget import router as money_budget_router
from routers.quota import router as quota_router
from routers.usage import router as usage_router
from routers.usage_breakdown import router as usage_breakdown_router
from routers.webhooks import router as webhooks_router
from service_auth import ServiceTokenProvider
from services.cost_flusher import birth_license_refused_series
from services.license_resolver import birth_resolved_series, parse_service_key_app_map
from settings import get_settings
from telemetry import setup_telemetry

logger = structlog.stdlib.get_logger(__name__)


def _validate_redis_at_boot(settings: Any, redis_conn: Any) -> None:
    """Redis at boot is fatal-in-production, like the leader-election DSN.

    Redis is the source of the cost/metering event streams AND the sink for every
    background loop (consumer, flush, rollup, budget-watcher, quota resync) — all
    created ONLY inside ``if redis_conn:`` in :func:`lifespan`, once, at boot. If
    Redis is briefly unavailable during startup those loops are never created for
    the process lifetime, yet :func:`readiness` re-pings Redis live and flips to
    Ready once it recovers → the pod takes traffic while cost/metering events pile
    up unconsumed (silent under-billing) until someone restarts it.

    In ``production`` this must crashloop so k8s retries and the pod self-heals
    when Redis returns. Dev/test/CI legitimately boot without Redis, so there it
    only logs CRITICAL and keeps going (the loops are skipped, as before).
    """
    if redis_conn is not None:
        return
    is_prod = settings.environment == "production"
    logger.critical("redis_unavailable_at_boot", environment=settings.environment, fatal=is_prod)
    if is_prod:
        raise RuntimeError(
            "Redis unavailable at startup: the cost/metering consumer and flush loops "
            "would never start, so the pod would serve Ready while silently dropping "
            "billing events. Crashlooping so k8s retries instead."
        )


def _validate_leader_election(settings: Any) -> None:
    """Resolve the leader-election DSN at BOOT so a bad one cannot go silent.

    Both leader-elected loops (``run_rollup_loop``, ``run_watcher_loop``) resolve
    this DSN *inside* the coroutine, and they are ``asyncio.create_task``'d — so a
    ValueError raised in there kills the task silently (see :func:`_log_task_death`)
    and the hourly rollup + budget-overage watcher simply never run, with no log
    line at all. A silent no-op in a financial path.

    In ``production`` this is fatal: a pod that cannot elect a leader must
    crashloop, not sit Ready with two dead loops. Outside production we log
    CRITICAL and keep booting, because dev/test/CI legitimately run with no
    session-mode pooler configured (``DIRECT_POSTGRES_HOST`` empty).
    """
    try:
        settings.resolve_leader_election_dsn()
    except ValueError:
        is_prod = settings.environment == "production"
        logger.critical(
            "leader_election_dsn_invalid",
            environment=settings.environment,
            fatal=is_prod,
            exc_info=True,
        )
        if is_prod:
            raise


def _log_task_death(task: asyncio.Task) -> None:
    """Done-callback: make a background task dying LOUD.

    Without this, a task that raises dies silently: the ``tasks`` list holds a
    reference for the process lifetime, so asyncio never emits its
    "exception was never retrieved" warning, and the only ``gather`` is at
    shutdown with ``return_exceptions=True``, which swallows it. A crashed
    rollup or budget-overage loop would produce ZERO log output while the pod
    stayed happily Ready — a silent no-op in a financial path.
    """
    if task.cancelled():  # expected on shutdown
        return
    exc = task.exception()
    if exc is not None:
        logger.error("background_task_died", task=task.get_name(), exc_info=exc)


#: The port the dedicated metrics listener is bound to, or ``None`` while unbound.
#: Module-level so :func:`_start_metrics_listener` is IDEMPOTENT. ``lifespan`` runs
#: once per process in production but MANY times in the test suite (every
#: ``TestClient(app)``/``LifespanManager`` entry runs it again in the same
#: interpreter), and a second ``start_http_server`` on the same port would raise
#: EADDRINUSE against this process's own previous listener.
_metrics_listener_port: int | None = None


def _start_metrics_listener(port: int) -> None:
    """Also serve the Prometheus registry on its own dedicated ``port``.

    ⭐ IT SERVES THE DEFAULT ``prometheus_client.REGISTRY``, AND THAT IS THE WHOLE
    POINT. ``start_http_server``'s ``registry`` argument defaults to ``REGISTRY``,
    which is the SAME registry everything this service exports already lands in:
    ``observability.metrics`` constructs every ``billing_*`` instrument as a bare
    ``Counter``/``Gauge``/``Histogram`` with NO ``registry=`` keyword (verified:
    zero occurrences of ``registry=`` in that module), so each one self-registers
    on the default registry at import time — which is exactly why the side-effect
    import at the top of this file is all the wiring the Instrumentator needs, and
    why the Instrumentator's own ``http_*`` request metrics land there too. So no
    separate ``CollectorRegistry`` and no ``multiprocess.MultiProcessCollector`` is
    used here: either one would expose a SUBSET, and a scrape moved to a subset
    would silently blind every existing alert instead of failing visibly.

    (a) ``/metrics`` REMAINS MOUNTED ON THE API PORT AS WELL, DELIBERATELY. This
        adds a listener; it removes nothing. Moving the Prometheus scrape to the
        new port is therefore a separate, independently revertible gitops change,
        and the two endpoints can serve the same registry side by side for as long
        as the transition needs. Narrowing the API port's authorization policy is a
        third step, after the scrape has moved.
    (b) THE ACCEPTANCE TEST for that gitops step: the set of ``billing_*`` series
        exposed on the new port must be IDENTICAL TO OR A SUPERSET OF the set
        exposed on the API port. Compare the two scrapes before re-pointing the
        ServiceMonitor, not after — a missing series reads downstream as a measured
        zero, and an alert over an empty vector is vacuously green.

    Non-fatal by construction: a bind failure (``EADDRINUSE`` from a port already
    in use, a permission error, an unusable address) logs a warning and returns, so
    the API still boots. A metrics port must never be able to take billing down.
    """
    global _metrics_listener_port
    if port == 0:
        # The shipped default. Behaviour is byte-identical to before this existed:
        # no listener, and nothing above DEBUG in the log.
        logger.debug("metrics_listener_disabled")
        return
    if _metrics_listener_port is not None:
        logger.debug("metrics_listener_already_started", metrics_port=_metrics_listener_port)
        return
    try:
        start_http_server(port)
    except OSError:
        logger.warning("metrics_listener_bind_failed", metrics_port=port, exc_info=True)
        return
    _metrics_listener_port = port
    logger.info("metrics_listener_started", metrics_port=port)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
    """Manage startup and shutdown resources."""
    settings = get_settings()

    configure_logging(settings)
    logger.info("starting", service=settings.service_name, port=settings.port)

    # Birth billing_license_resolved_total BEFORE the first scrape can reach the
    # registry — i.e. before the listener two blocks down, and before any flush.
    #
    # NOT a fix for a dead counter: that counter increments correctly and was
    # measured doing so on 2026-09-14. What it had was ONE series against the
    # whole `cost_attribution_app_by_service_key` map — 14 keys on the shipped
    # default that day; read the setting, never a remembered count — because a
    # child appears only once that key has actually resolved a licence. Both
    # `billing:licence_derived_share:1h` and `BillingLicenceAssertionRegressed`
    # are `by (service_key)`, so for every key still unborn an empty vector reads
    # as health.
    #
    # This is the ONE birth in this service that cannot live at import in
    # observability/metrics.py: the reachable label domain is the operator's
    # `cost_attribution_app_by_service_key`, which is not known until settings
    # are resolved. Deriving it from the SAME `parse_service_key_app_map`
    # expression `cost_flusher` gates the increment on is what keeps the birthed
    # set equal to the reachable set — see `license_resolver.birth_resolved_series`
    # for why `"unknown"` is deliberately NOT among them.
    birth_resolved_series(parse_service_key_app_map(settings.cost_attribution_app_by_service_key))

    # Birth billing_license_refused_total for the same reason, and it matters
    # MORE here: the HEALTHY steady state of that counter is ZERO. Without a
    # birth, "no emitter has ever asserted a licence we had to refuse" and "the
    # adjudication is not running at all" are the same empty vector, and any
    # alert written over it is vacuously green — which is exactly the state the
    # fleet would be in if LICENSE_ADJUDICATED_SERVICE_KEYS were ever emptied.
    #
    # ⚠️ IT SITS HERE RATHER THAN AT IMPORT IN observability/metrics.py, and NOT
    # for the reason the block above does. Its label domain is NOT operator
    # config — both halves are module constants in services.cost_flusher, so on
    # the stated rule it belongs at import time with the ~29 other births. It
    # cannot go there: services.cost_flusher imports observability.metrics, so
    # metrics.py naming those constants would be a cycle. This is the nearest
    # honest home, not a considered preference, and it costs one property the
    # import-time births have — a scrape that arrives before lifespan runs sees
    # no samples.
    birth_license_refused_series()

    # Second, dedicated metrics listener (no-op unless METRICS_PORT is set). Started
    # here, before the slow boot work below, because the registry is fully populated
    # at import time — so a scrape can reach it even while init_db/telemetry are
    # still going, which is when a failing pod is most worth scraping.
    _start_metrics_listener(settings.metrics_port)

    # setup_telemetry does a blocking socket.create_connection(timeout=2) probe of
    # the OTLP endpoint; run it off the event loop so an unreachable collector
    # can't stall the whole async lifespan for 2s at boot.
    await asyncio.to_thread(setup_telemetry, settings)

    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(application)
    except (ImportError, RuntimeError):  # fmt: skip
        logger.warning("fastapi_otel_instrumentation_failed", exc_info=True)

    await init_db(settings)
    logger.info("database_initialised", url=settings.database_url.split("@")[-1])

    application.state.service_token_provider = ServiceTokenProvider(settings)

    # Birth billing_app_key_resolved_total{stream, app_key} for every key the
    # X-App-Id map can resolve, so the series exist from boot rather than from the
    # first resolved event. The label domain is the map itself (app_registry,
    # migration 0050), which only the database knows -- hence here, not at import.
    # Building the map does the birth (license_resolver._build_app_key_map) and
    # warms its TTL cache. Fail-soft: a cold map is rebuilt by the first flush.
    try:
        from db import get_session_factory as _get_session_factory
        from services.license_resolver import get_app_key_map

        async with _get_session_factory()() as _session:
            _app_keys = await get_app_key_map(_session)
        logger.info("startup_app_key_map_built", app_keys=len(_app_keys))
    except (SQLAlchemyError, OSError):  # fmt: skip
        logger.warning("startup_app_key_map_failed", exc_info=True)

    redis_conn = await init_redis(settings)
    _validate_redis_at_boot(settings, redis_conn)

    # Background task state
    stop_event = asyncio.Event()
    event_buffer: list[dict] = []
    cost_buffer: list[dict] = []
    tasks: list[asyncio.Task] = []

    # Keep tenancy's live tenant-name overlay fresh. Started OUTSIDE the
    # `if redis_conn:` block and NOT gated on consumer_enabled on purpose: the
    # resolver it feeds sits on the HTTP boundary, which is live in every mode
    # including the read-only deploy and a Redis-less pod. Not awaited here —
    # a licences timeout at boot would delay readiness for a map this service
    # is designed to run without, and the loop's first tick is immediate.
    from services.tenant_names import install_demand_refresh_hook, run_tenant_registry_refresh_loop

    # Let the resolver ask for a refresh when it meets a uuid NOTHING vouches
    # for. Installed before the loop starts so the very first such id — which
    # can arrive before the loop's first tick completes — already triggers one.
    # Rate-limited inside tenancy; never awaited on the request path.
    install_demand_refresh_hook(settings, application.state.service_token_provider)

    registry_task = asyncio.create_task(
        run_tenant_registry_refresh_loop(
            settings, application.state.service_token_provider, stop_event
        ),
        name="tenant_registry_refresh",
    )
    registry_task.add_done_callback(_log_task_death)
    tasks.append(registry_task)
    # Tasks started here carry their death callback already; the loop at the
    # end of the `if redis_conn:` block must not add it a second time.
    early_tasks: list[asyncio.Task] = [registry_task]

    # Keep the X-App-Id key map's source (app_registry) fresh from licences'
    # /internal/apps. Outside `if redis_conn:` for the same reason as the
    # tenant loop above: key resolution runs in every mode. Not awaited — the
    # first tick is immediate. Writes ONLY app_registry; it is NOT
    # POST /api/budget/sync-from-licenses, which also writes request_quota_config.
    if settings.app_registry_refresh_seconds > 0:
        from db import get_session_factory as _app_registry_session_factory
        from services.app_registry import run_app_registry_refresh_loop

        app_registry_task = asyncio.create_task(
            run_app_registry_refresh_loop(
                settings,
                _app_registry_session_factory(),
                application.state.service_token_provider,
                stop_event,
            ),
            name="app_registry_refresh",
        )
        app_registry_task.add_done_callback(_log_task_death)
        tasks.append(app_registry_task)
        early_tasks.append(app_registry_task)
    else:
        logger.info("app_registry_refresh_disabled")

    if redis_conn:
        from db import get_session_factory
        from services.aggregator import run_flush_loop, run_rollup_loop
        from services.consumer import consume_events

        session_factory = get_session_factory()

        # consumer_enabled gates the stream consumer (Phase 2 read-only deploy
        # must NOT join the shared "metering-service" group while the live
        # metering consumer still owns it — that would split the event stream).
        if settings.consumer_enabled:
            # HA multi-consumer, inbox-deduped (per-pod consumer name). The
            # consumer runs on EVERY replica — it does NOT use leader election.
            # Exactly-once is guaranteed downstream by the idempotency inbox
            # (processed_cost_messages, INSERT ... ON CONFLICT DO NOTHING in the
            # persist tx) + the FIX A2 XAUTOCLAIM PEL-reclaim, so leader-gating
            # was only an optimization to avoid two replicas sharing one consumer
            # name. consume_events instead derives a per-pod-unique consumer name
            # from $HOSTNAME (the K8s pod name), so the two replicas are distinct
            # consumers in the group and the stream is split between them.
            #
            # (This decouples the cost consumer from the rollup/watcher loops'
            # Postgres advisory-lock election: the consumer does not depend on
            # that election being healthy.)
            tasks.append(
                asyncio.create_task(
                    consume_events(
                        redis_conn,
                        settings,
                        event_buffer,
                        cost_buffer,
                        stop_event,
                    ),
                    name="consumer",
                )
            )
        else:
            logger.warning("consumer_disabled_read_only_mode")
        tasks.append(
            asyncio.create_task(
                run_flush_loop(
                    settings, session_factory, redis_conn, event_buffer, cost_buffer, stop_event
                ),
                name="flush",
            )
        )
        # Fail LOUD and EARLY on a bad leader-election DSN — see
        # _validate_leader_election. Must run BEFORE the loops below are
        # create_task'd, because a ValueError inside one of them dies silently.
        _validate_leader_election(settings)

        tasks.append(
            asyncio.create_task(
                run_rollup_loop(settings, session_factory, stop_event, redis_conn),
                name="rollup",
            )
        )
        # T1.6 budget overage watcher — own leader-elected loop so it can
        # run on a different replica from the rollup leader. Gated by
        # consumer_enabled so the Phase 2 read-only deploy does not re-fire
        # overage webhooks the live metering watcher already sent.
        if settings.consumer_enabled:
            from services.budget_watcher import run_watcher_loop

            tasks.append(
                asyncio.create_task(
                    run_watcher_loop(settings, session_factory, stop_event),
                    name="budget_watcher",
                )
            )
        # Monthly invoice-close loop — own leader-elected lock (INVOICE_LOCK_KEY)
        # so it can run on a different replica from the rollup and watcher
        # leaders. Gated on invoice_close_enabled, default False: closing a
        # period draws from a gapless invoice-number counter, so a buggy loop
        # must not be armed by default — see Settings.invoice_close_enabled.
        # DSN validity for this lock was already proven above by
        # _validate_leader_election, shared with the other leader-elected loops.
        # Published UNCONDITIONALLY, outside the `if` on purpose: the lag
        # gauges are module-level and are exported at 0 on every replica by
        # the side-effect import at the top of this file, armed or not. The
        # gitops rule BillingInvoiceCloseBehindSchedule ANDs on this series
        # being 1, so a 0 here is what stops it firing forever against a
        # deliberately-disabled loop. Setting it inside the `if` would make
        # "flag is false" indistinguishable from "this line never ran".
        _metrics.invoice_close_enabled.set(1 if settings.invoice_close_enabled else 0)
        if settings.invoice_close_enabled:
            from services.invoice_closer import run_invoice_close_loop

            tasks.append(
                asyncio.create_task(
                    run_invoice_close_loop(settings, session_factory, stop_event),
                    name="invoice_close",
                )
            )
        # Licence↔budget reconcile — own leader-elected lock
        # (LICENCE_BUDGET_RECONCILE_LOCK_KEY) so it can run on a different
        # replica from the loops above. This is the only CLOSED-LOOP guard that
        # a budget behind a revoked licence stops being spendable: the
        # push-side teardown in licenses#192 fires only on an explicit
        # POST /license/revoke, while the nightly reap_expired_licences CronJob
        # mints status='revoked' rows with a direct DB write that structurally
        # cannot reach it.
        #
        # Published UNCONDITIONALLY, outside the `if`, for the same reason as
        # invoice_close_enabled above: the gauges are module-level and are
        # exported on every replica whether or not the loop is armed, so an
        # alert can distinguish "deliberately disabled" from "this line never
        # ran". Without this a disarmed loop and a dead one look identical.
        _metrics.licence_budget_reconcile_enabled.set(
            1 if settings.licence_budget_reconcile_enabled else 0
        )
        _metrics.licence_budget_reconcile_deactivate_enabled.set(
            1 if settings.licence_budget_reconcile_deactivate_enabled else 0
        )
        if settings.licence_budget_reconcile_enabled:
            from services.licence_budget_reconcile import run_licence_budget_reconcile_loop

            tasks.append(
                asyncio.create_task(
                    run_licence_budget_reconcile_loop(
                        settings,
                        session_factory,
                        application.state.service_token_provider,
                        stop_event,
                    ),
                    name="licence_budget_reconcile",
                )
            )
        # request_quota_config value repair — own leader-elected lock
        # (QUOTA_CONFIG_SYNC_LOCK_KEY); first tick at startup. See
        # services/quota_config_sync.py for what it may and may not write.
        if settings.quota_config_sync_enabled:
            from services.quota_config_sync import run_quota_config_sync_loop

            tasks.append(
                asyncio.create_task(
                    run_quota_config_sync_loop(
                        settings,
                        session_factory,
                        application.state.service_token_provider,
                        stop_event,
                    ),
                    name="quota_config_sync",
                )
            )
        # The early tasks above already carry the callback; adding it twice
        # would log a task's death twice.
        for task in tasks:
            if task not in early_tasks:
                task.add_done_callback(_log_task_death)
        # Explicit on BOTH paths. The healthy case must publish a real 0
        # rather than rely on a default, so an all-clear is always something
        # this code actually asserted.
        _metrics.background_tasks_skipped.set(0)
        logger.info("background_tasks_started", count=len(tasks))
    else:
        # A WARN here is not a control — it fires once, into a log nobody
        # alerts on, that a LOG_LEVEL change deletes. The gauge is the control:
        # this pod runs no flush loop, no rollup, no quota resync and no budget
        # watcher for its entire life, because init_redis is never called
        # again. It stays Ready and serves HTTP on purpose (see readiness), so
        # nothing else in the system reports this.
        _metrics.background_tasks_skipped.set(1)
        logger.warning("redis_unavailable_background_tasks_skipped")

    yield

    # Shutdown
    logger.info("shutting_down", service=settings.service_name)
    stop_event.set()

    # Uninstall the demand-refresh hook BEFORE cancelling the tasks: a late
    # resolution during shutdown must not schedule a fetch onto a loop that is
    # closing. tenancy swallows the failure either way; this makes it a no-op
    # rather than a swallowed error.
    from tenancy import set_registry_refresh_hook

    set_registry_refresh_hook(None)

    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    await close_redis()
    await dispose_engine()


#: The ONE place this service's version is written down.
#:
#: ⚠️ It was three separate ``"0.1.0"`` literals — the FastAPI app, ``/health``
#: and ``/`` — none of which was connected to ``pyproject.toml``. They agreed
#: only because nobody had bumped the package yet, so the first release would
#: have left every endpoint confidently reporting a version the artefact no
#: longer had, with no test able to notice: the tests asserted the same literal.
#:
#: Read from the installed distribution so a bump moves it. The fallback covers
#: a source checkout that was never `pip install`ed; ``test_version_is_not_a_
#: third_copy`` pins that fallback to pyproject so the two cannot drift.
_FALLBACK_VERSION = "0.1.0"
try:
    SERVICE_VERSION = _pkg_version("billing-service")
except PackageNotFoundError:  # pragma: no cover - installed in every real run
    SERVICE_VERSION = _FALLBACK_VERSION

app = FastAPI(
    title="Billing Service",
    description="Usage metering, cost tracking, and billing for the nonagentic.ai platform",
    version=SERVICE_VERSION,
    lifespan=lifespan,
    # Explicit docs URLs (match FastAPI defaults) so orval / SDK generators
    # always have a stable spec endpoint and can't be broken by a refactor.
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    # CORS removed: billing is server-to-server only behind license-authz
    # (the billing host is in the deployment's AUTH_REQUIRED_HOSTS) and is not
    # exposed via any Cloudflare tunnel. Internal callers (license-authz,
    # inference-router, worker_management, n8n nodes) hit it server-side; the
    # /dashboard/* Jinja2 HTML is same-origin browser navigation. No
    # cross-origin browser reach exists, so no CORS middleware is needed.
    # Middleware order (execution = reverse of registration):
    #   RequestId -> SecurityHeaders -> RequestLogging -> handler.
    # RequestId runs first so 401/422/500 envelopes can reference a stable id.
    # SecurityHeaders runs late so it can stamp every response (including
    # 4xx/5xx envelopes emitted by the exception handlers below).
    middleware=[
        Middleware(cast(Any, RequestIdMiddleware)),
        Middleware(cast(Any, SecurityHeadersMiddleware)),
        Middleware(cast(Any, RequestLoggingMiddleware)),
    ],
    openapi_tags=[
        {"name": "health", "description": "Liveness, readiness, build info."},
        {"name": "usage", "description": "Usage and metering event ingestion + queries."},
        {"name": "costs", "description": "Cost rollups + per-tenant cost analytics."},
        {"name": "billing", "description": "Invoicing and billing run management."},
        {"name": "budget", "description": "Budget provisioning + overage policy."},
        {"name": "quota", "description": "Quota checks + adjustments."},
        {"name": "config", "description": "Service + tenant configuration."},
        {"name": "audit", "description": "Audit log retrieval."},
        {"name": "dashboard", "description": "Server-rendered admin dashboard (Jinja2+HTMX)."},
        {
            "name": "internal",
            "description": "Service-to-service endpoints used by internal callers.",
        },
        {"name": "export", "description": "Bulk usage / cost export."},
        {"name": "webhooks", "description": "Outbound webhook configuration."},
    ],
    swagger_ui_parameters={"persistAuthorization": True},
    generate_unique_id_function=lambda route: (
        f"{route.tags[0]}_{route.name}" if route.tags else route.name
    ),
    redirect_slashes=False,
)

# Prometheus instrumentation. ``instrument`` adds the per-request middleware
# (counts + latency histogram + status code labels). ``expose`` mounts the
# ``/metrics`` route with ``include_in_schema=False`` so it doesn't pollute
# the public OpenAPI spec consumed by license-authz / fr-backend / orval.
# Custom counters in ``observability.metrics`` are picked up automatically
# because they register on the default Prometheus REGISTRY at import time.
from prometheus_fastapi_instrumentator import Instrumentator  # noqa: E402

# ``latency_lowr_buckets`` is EXPLICIT because the library default is
# ``(0.1, 0.5, 1)`` -- four boundaries with +Inf -- and the SLO alert
# ``MeteringQuotaBudgetLatencyHigh`` fires at ``> 0.5``, i.e. exactly ON a
# bucket edge. Measured in Prometheus 2026-08-31, the live boundaries were
# ``0.1, 0.5, 1.0, +Inf`` across 1056 series, confirming the default was in
# force. With that geometry ``histogram_quantile`` can only ever return a
# linear interpolation inside ``(0.5, 1.0]`` once the tail crosses 0.5, so it
# cannot tell 0.51s from 0.99s: the 2026-08-27 reading of p95=0.820 is exactly
# reproducible as ``0.5 + (41/64)*0.5``, i.e. pure bucket geometry rather than
# a measurement. The alert's own description says so and prescribes this fix:
# "To get one, add explicit buckets in billing_service (src/main.py
# Instrumentator(...)) -- until then do not chase the reported figure."
#
# The set below is a strict SUPERSET of the default, so every existing
# selector keeps resolving -- notably the alert's non-vacuity guard, which
# anchors on ``le="+Inf"``. It adds resolution either side of the 0.5
# threshold (0.25 / 0.5 / 0.75) so a p95 in that band becomes an actionable
# latency instead of an artefact of where the boundaries happen to fall.
#
# NOTE: changing buckets re-labels the histogram series. The old ``le`` series
# age out of the retention window; ``histogram_quantile`` over ``rate`` is
# unaffected from the first scrape onward.
Instrumentator(
    excluded_handlers=["/metrics", "/health", "/health/live", "/health/ready"],
).instrument(
    app,
    latency_lowr_buckets=(0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0),
).expose(
    app,
    endpoint="/metrics",
    include_in_schema=False,
    tags=["health"],
)


def custom_openapi() -> dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema

    from fastapi.openapi.utils import get_openapi

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=app.openapi_tags,
    )

    components = openapi_schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})
    security_schemes.setdefault(
        "BearerAuth",
        {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": (
                "Paste the raw JWT. Swagger UI sends it as Authorization: Bearer <token>."
            ),
        },
    )

    for path, path_item in openapi_schema.get("paths", {}).items():
        normalized = path.rstrip("/") or "/"
        requires_auth = normalized.startswith("/dashboard")

        for operation in list(path_item.values()):
            if not isinstance(operation, dict):
                continue
            if requires_auth:
                operation.setdefault("security", [{"BearerAuth": []}])
            else:
                operation.pop("security", None)

    app.openapi_schema = openapi_schema
    return app.openapi_schema


object.__setattr__(app, "openapi", custom_openapi)


# --- Canonical error envelope handlers -----------------------------------
# Every error response leaves metering as `{"error": {code, message, status,
# request_id, timestamp}}` matching the fleet standard (nlp_service /
# worker_management / etc.). `Envelope[T]` in `models/domain.py` remains
# the SUCCESS shape; this only normalises the failure path.

import os  # noqa: E402
import uuid as _uuid_module  # noqa: E402
from datetime import UTC, datetime  # noqa: E402

import httpx  # noqa: E402
from fastapi import Request  # noqa: E402
from fastapi.exceptions import RequestValidationError  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from starlette.exceptions import HTTPException as StarletteHTTPException  # noqa: E402

_LEAK_DETAILS = os.getenv("LEAK_EXCEPTION_DETAILS", "false").lower() == "true"


def _request_id(request: Request) -> str:
    rid = getattr(request.state, "request_id", None)
    return rid if isinstance(rid, str) and rid else str(_uuid_module.uuid7())


def _error_envelope(
    *,
    code: str,
    message: str,
    status_code: int,
    request_id: str,
    details: str | dict | list | None = None,
    leak_details: bool = False,
) -> dict[str, dict[str, Any]]:
    body: dict[str, Any] = {
        "code": code,
        "message": message,
        "status": status_code,
        "request_id": request_id,
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    if details is not None and (leak_details or _LEAK_DETAILS):
        body["details"] = details
    return {"error": body}


@app.exception_handler(StarletteHTTPException)
async def _http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    rid = _request_id(request)
    # When a route raises ``HTTPException(detail={...})`` to return structured
    # data alongside the status (e.g. a 429 quota-denial with allowed/reason),
    # preserve that dict/list payload at ``error.details`` so callers can still
    # branch on it. String details become the human-readable message.
    if isinstance(exc.detail, str):
        msg = exc.detail
        structured: str | dict | list | None = None
    elif isinstance(exc.detail, dict | list):
        msg = "Request error"
        structured = exc.detail
    else:
        msg = "Request error"
        structured = None
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_envelope(
            code=f"HTTP_{exc.status_code}",
            message=msg,
            status_code=exc.status_code,
            request_id=rid,
            details=structured,
            leak_details=structured is not None,
        ),
        headers=getattr(exc, "headers", None),
    )


# Cap on how many field-level entries a 422 body carries. A pathological
# request (a large list body, a deeply nested model) can produce hundreds of
# pydantic errors; an operator can only act on the first few, and an unbounded
# list would let any caller inflate the response. See _sanitised_validation_details.
_MAX_VALIDATION_DETAILS = 20


def _sanitised_validation_details(
    errors: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    """Reduce pydantic's raw error list to a safe, stable, renderable shape.

    Returns a JSON array of ``{"field", "message", "type"}`` objects — **all
    three keys always present**, all three always strings.

    ``field`` is the dotted path built from pydantic's ``loc`` tuple with a
    leading ``"body"`` element dropped, so ``("body", "surcharge_input_per_1m_usd")``
    becomes ``"surcharge_input_per_1m_usd"`` — the name the caller actually put
    in its JSON. A whole-body (model-level) error has ``loc == ("body",)`` and
    yields ``field == ""``: the key is still present, and an **empty string is
    the contract for "this error is about the request as a whole, not one
    field"**. Consumers must branch on emptiness, never on the key's absence.
    Non-body locations (``query`` / ``path`` / ``header``) keep their prefix,
    because a bad query param ``n`` is not the same thing as a body field ``n``.

    **What is deliberately NOT emitted, and why.** A validation error describes
    the caller's *own* request, so field names and constraint messages disclose
    nothing about server state — that is why this handler may return them where
    the 5xx handlers must not. But pydantic's raw entries also carry:

    * ``input`` — the caller's submitted value, echoed back. On this service a
      rejected body is money configuration (surcharges, markup percentages,
      tenant ids); reflecting it into a response body that is logged, toasted
      and screenshotted turns a validation failure into a data-exposure path.
    * ``ctx`` — validator context, an unbounded and arbitrarily-typed mapping
      that can hold live objects (Decimal bounds, enum classes, exception
      instances) and is not reliably JSON-serialisable. Passing it through
      risks a serialisation 500 raised *from inside the error handler*.
    * ``url`` — a pydantic.dev documentation link; noise for an operator.

    All three are dropped here rather than filtered downstream, so no future
    caller of this function can reintroduce them by accident.

    The list is capped at ``_MAX_VALIDATION_DETAILS`` field entries. When it is
    truncated a final entry with ``type == "too_many_errors"`` and an empty
    ``field`` says how many were suppressed, so the payload is at most
    ``_MAX_VALIDATION_DETAILS + 1`` elements and the truncation is visible in
    the response rather than only in this docstring.
    """
    details: list[dict[str, str]] = []
    for err in errors[:_MAX_VALIDATION_DETAILS]:
        loc = err.get("loc") or ()
        parts = [str(p) for p in loc]
        if parts and parts[0] == "body":
            parts = parts[1:]
        details.append(
            {
                "field": ".".join(parts),
                "message": str(err.get("msg", "")),
                "type": str(err.get("type", "")),
            }
        )
    suppressed = len(errors) - len(details)
    if suppressed > 0:
        details.append(
            {
                "field": "",
                "message": (
                    f"{suppressed} further validation error(s) were suppressed; "
                    f"showing the first {_MAX_VALIDATION_DETAILS}."
                ),
                "type": "too_many_errors",
            }
        )
    return details


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Return the field-level reasons a 422 happened, not just that it happened.

    ``leak_details=True`` is passed **for this handler only**, independently of
    the ``LEAK_EXCEPTION_DETAILS`` env flag that gates the 5xx handlers. Those
    handlers stringify a server-side exception, so they stay off in production;
    this one describes the caller's own request and is safe to return always —
    see :func:`_sanitised_validation_details` for exactly what is and is not
    included. Without it, the only thing an operator on fr's pricing admin page
    could be told about a rejected markup rule was "Request validation failed",
    while the message naming the field to fix was computed and discarded.
    """
    rid = _request_id(request)
    return JSONResponse(
        status_code=422,
        content=_error_envelope(
            code="VALIDATION_ERROR",
            message="Request validation failed",
            status_code=422,
            request_id=rid,
            details=_sanitised_validation_details(exc.errors()),
            leak_details=True,
        ),
    )


@app.exception_handler(DataError)
async def _data_error_exception_handler(request: Request, exc: DataError) -> JSONResponse:
    """A malformed identifier is a CLIENT fault — 400, not 503.

    Since migration 0011 every ``tenant_id`` column is a Postgres ``uuid``, so a
    caller who supplies a non-uuid identifier makes Postgres raise
    ``invalid input syntax for type uuid``, which SQLAlchemy surfaces as
    :class:`sqlalchemy.exc.DataError`. ``DataError`` is a subclass of
    ``SQLAlchemyError``, so without this handler it fell through to the one below
    and was reported as ``503 DATABASE_UNAVAILABLE`` — blaming the database for a
    caller's bad input, telling the client to retry something that can never
    succeed, and hiding the real cause behind an infrastructure alert during an
    incident.

    Dispatch is by exception MRO, not registration order: Starlette's
    ``_lookup_exception_handler`` walks ``type(exc).__mro__`` and returns the
    first registered class it finds, and ``DataError`` precedes
    ``SQLAlchemyError`` in that MRO. Registering it above the ``SQLAlchemyError``
    handler is therefore belt-and-braces rather than load-bearing.

    ``details`` is deliberately omitted: ``str(exc)`` embeds the offending
    parameter, which may be a leaked credential (the ``X-Tenant-Id`` header has
    been observed carrying a raw ``Bearer`` token). The full exception is on the
    server-side log line, correlated by request id.
    """
    rid = _request_id(request)
    logger.warning("invalid_identifier", request_id=rid, exc_info=True)
    return JSONResponse(
        status_code=400,
        content=_error_envelope(
            code="INVALID_IDENTIFIER",
            message="A supplied identifier is not a valid UUID.",
            status_code=400,
            request_id=rid,
        ),
    )


@app.exception_handler(SQLAlchemyError)
async def _sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError) -> JSONResponse:
    rid = _request_id(request)
    logger.error("database_error", request_id=rid, exc_info=True)
    return JSONResponse(
        status_code=503,
        content=_error_envelope(
            code="DATABASE_UNAVAILABLE",
            message="Database temporarily unavailable",
            status_code=503,
            request_id=rid,
            details=str(exc),
        ),
    )


@app.exception_handler(RedisError)
async def _redis_exception_handler(request: Request, exc: RedisError) -> JSONResponse:
    rid = _request_id(request)
    logger.error("redis_error", request_id=rid, exc_info=True)
    return JSONResponse(
        status_code=503,
        content=_error_envelope(
            code="CACHE_UNAVAILABLE",
            message="Cache temporarily unavailable",
            status_code=503,
            request_id=rid,
            details=str(exc),
        ),
    )


@app.exception_handler(httpx.HTTPError)
async def _httpx_exception_handler(request: Request, exc: httpx.HTTPError) -> JSONResponse:
    rid = _request_id(request)
    logger.error("upstream_error", request_id=rid, exc_info=True)
    return JSONResponse(
        status_code=502,
        content=_error_envelope(
            code="UPSTREAM_UNAVAILABLE",
            message="Upstream service unavailable",
            status_code=502,
            request_id=rid,
            details=str(exc),
        ),
    )


@app.exception_handler(Exception)
async def _generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    rid = _request_id(request)
    logger.error("unhandled_error", request_id=rid, exc_info=True)
    return JSONResponse(
        status_code=500,
        content=_error_envelope(
            code="INTERNAL_SERVER_ERROR",
            message="Internal server error",
            status_code=500,
            request_id=rid,
            details=str(exc),
        ),
    )


app.include_router(usage_router)
app.include_router(usage_breakdown_router)
app.include_router(admin_router)
app.include_router(quota_router)
app.include_router(costs_router)
app.include_router(billing_router)
app.include_router(budget_router)
app.include_router(money_budget_router)
app.include_router(config_router)
app.include_router(audit_router)
app.include_router(dashboard_router)
app.include_router(export_router)
app.include_router(webhooks_router)

# Serve static files (CSS)
_static_dir = Path(__file__).parent / "static"
if _static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["health"],
    operation_id="health_check",
)
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="billing-service", version=SERVICE_VERSION)


@app.get(
    "/health/live",
    response_model=LivenessResponse,
    tags=["health"],
    operation_id="health_live",
)
async def liveness() -> LivenessResponse:
    return LivenessResponse()


@app.get(
    "/health/ready",
    response_model=ReadinessResponse,
    tags=["health"],
    operation_id="health_ready",
    responses={HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
)
async def readiness() -> JSONResponse:
    """Readiness probe — 503 when a FATAL dependency is down.

    Two separate lessons are encoded here; do not collapse them.

    **The status code has to move.** This used to return a bare
    ReadinessResponse, which FastAPI serialises at the decorator's status code
    (200) for *every* outcome, ready or not. The Deployment wires this path as
    an httpGet readinessProbe, and an httpGet probe reads only the status code
    — never the body. So a billing pod that could not reach Postgres still
    reported Ready and stayed in the Service's endpoint rotation indefinitely,
    taking revenue-path traffic it could not serve. That part stands.

    **But not every dependency is fatal, and Redis is not** (fr#947/#978,
    2026-09-06). fr and kag served zero endpoints for 2h45m because their
    readiness awaited Redis and 503'd on it: a control-plane blip wedged the
    Redis client in every replica at once, `/health/live` stayed 200 so
    kubelet restarted nothing, and a human had to notice. Redis was healthy
    the whole time — a fresh PING from inside a wedged container answered in
    4-8ms.

    Readiness expresses a **per-pod** condition: "route to me, not to my
    siblings". A dependency that every replica shares cannot express that. It
    turns readiness into a correlated fleet-wide kill switch with nothing left
    to route to. `billing-service` runs `replicas: 2` against a
    **single-replica** Redis Deployment, so gating readiness on Redis made a
    1-replica SPOF able to empty the endpoints of the entire revenue path.

    And this service is already designed to run without Redis. See
    `routers/quota.py`'s `get_redis()  # may be None - the durable write below
    still proceeds`, the `redis_conn is None` branch in every service module,
    and `lifespan`'s own note about "a Redis-less pod". Readiness treating
    Redis as fatal contradicted the code it gates.

    So: **Postgres stays fatal** — without it this service cannot read or
    write anything and a 503 correctly sheds traffic to a sibling that can.
    **Redis degrades instead**: `status` becomes `degraded`, `redis` still
    reads `disconnected`, the dependency is named in `degraded[]`, and a WARN
    is logged every probe. The signal is preserved and alertable; only the
    fatality is dropped. Deleting the check would have been the wrong fix.

    Recovery is not left to luck either: `check_redis()` supervises the client
    and rebuilds it once it has demonstrably stopped recovering (see
    `redis_client`). Staying Ready through a Redis fault is what keeps this
    probe ticking so that repair can run.
    """
    db_ok = await get_engine_for_check()
    redis_ok = await check_redis()

    degraded: list[str] = [] if redis_ok else ["redis"]
    if degraded:
        logger.warning(
            "readiness_degraded",
            degraded=degraded,
            database="connected" if db_ok else "disconnected",
            fatal=not db_ok,
            note="non-fatal: a shared single-replica dependency must not evict every pod (fr#947/#978)",
        )

    if not db_ok:
        status = "not_ready"
    elif degraded:
        status = "degraded"
    else:
        status = "ready"

    body = ReadinessResponse(
        status=status,
        database="connected" if db_ok else "disconnected",
        redis="connected" if redis_ok else "disconnected",
        degraded=degraded,
    )
    return JSONResponse(
        content=body.model_dump(),
        status_code=HTTP_200_OK if db_ok else HTTP_503_SERVICE_UNAVAILABLE,
    )


@app.get("/", tags=["health"], operation_id="root")
async def root() -> dict[str, str]:
    return {
        "service": "billing-service",
        "version": SERVICE_VERSION,
        "docs": "/docs",
        "health": "/health",
    }
