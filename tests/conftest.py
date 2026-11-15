"""Shared test fixtures for metering-service.

Requires PostgreSQL. TEST_DATABASE_URL (or DATABASE_URL) is MANDATORY: there
is deliberately no default DSN — see ``resolve_test_database_url`` in
``tests/_production_tripwire.py`` for why a default cannot fail closed.

TEST_SCHEMA (default ``public``) selects the Postgres schema used for the test
run. It is a LOCAL/DEV knob for running concurrent sessions against one
database — **CI never sets it**. The fleet archetype
(``gitops-production/.github/workflows/python-service.yml``) gives every job its
own ephemeral ``postgres:18`` container and exports ``PG_SCHEMA=public``
unconditionally: an ephemeral *database* per job, not a per-job *schema*
(see that file's line ~1356 comment "PG_SCHEMA=public always (ephemeral DB = no
per-job schema)" and the ``echo "PG_SCHEMA=public"`` two lines below it).
``TEST_SCHEMA`` appears nowhere in the archetype, and this repo's
``.github/workflows/ci-cd.yaml`` is a thin wrapper that only calls it.

Schema selection is plumbed via the libpq ``PGOPTIONS`` env var rather than the
URL ``?options=`` query param — alembic stores the URL through configparser
which would interpret ``%3D`` as failed ``%``-interpolation. PGOPTIONS is read
by libpq at connect time for every psycopg connection in the process, so the
test engine, the app's engine, and any one-off ``psycopg.connect()`` calls all
land on the same schema without each having to mangle the URL.

⚠️ An explicit libpq ``options`` connection parameter *replaces* ``PGOPTIONS``
wholesale — libpq does not merge them. Any engine that needs its own GUC must
build its ``options`` string with :func:`pgoptions_with` below, never a bare
literal. Enforced by ``tests/test_pgoptions_inheritance.py``.
"""

# ---------------------------------------------------------------------------
# Production tripwire — the half of the truncate guard that lives outside the DB
# ---------------------------------------------------------------------------
#
# Runs at IMPORT time, before any engine is built, so a mis-pointed
# TEST_DATABASE_URL cannot reach the session fixture's DROP SCHEMA either. The
# rationale, and why the trigger from alembic 0017 is vacuous without this,
# is in tests/_production_tripwire.py.
# Loaded BY PATH, not by name. `tests/` is not on sys.path yet when pytest
# executes this conftest (pyproject sets pythonpath = ["src"] only), and putting
# it there is not a harmless workaround: `tests/routers/` is a package, so
# prepending `tests/` SHADOWS `src/routers` and every import of `routers.admin`
# fails. Measured — it broke tests/test_metrics_endpoint.py outright.
#
# The check lives in its own module rather than inline here because conftest is
# not importable by name from a test, and an unasserted guard is one nobody
# notices has stopped working. tests/test_truncate_guard.py asserts it.
import importlib.util as _importlib_util  # noqa: E402
import os
import re
from collections.abc import AsyncGenerator
from pathlib import Path as _Path  # noqa: E402

_tripwire_spec = _importlib_util.spec_from_file_location(
    "_production_tripwire", _Path(__file__).parent / "_production_tripwire.py"
)
assert _tripwire_spec is not None and _tripwire_spec.loader is not None
_production_tripwire = _importlib_util.module_from_spec(_tripwire_spec)
_tripwire_spec.loader.exec_module(_production_tripwire)

# Resolve the test DSN through the guard, THEN check it is not production.
#
# ⛔ THERE IS NO DEFAULT DSN IN THIS FILE, AND NONE MAY BE ADDED BACK.
# A hardcoded fallback does not fail closed. It connects to whatever is
# listening on that host:port, which on this box is another lane's test
# database, and the session-scoped engine fixture below DROP/CREATEs the schema
# in it. The victim then debugs a plausible red suite on their own diff. Three
# lanes were corrupted that way on 2026-09-07; the previous default here was
# ``127.0.0.1:5502/metering_test`` and the one before it ``:5436``, a different
# session's container entirely. Replacing one literal port with another is not
# the fix — the fix is that the caller must say which database it owns.
#
# Enforced by tests/test_test_database_url_required.py, which asserts (a) the
# guard's behaviour on all four env combinations and (b) that this file
# contains no DSN string literal at all.
TEST_DATABASE_URL = _production_tripwire.resolve_test_database_url(
    os.getenv("TEST_DATABASE_URL"),
    os.getenv("DATABASE_URL"),
)
TEST_SCHEMA = os.getenv("TEST_SCHEMA", "public")

_production_tripwire.refuse_production_database(TEST_DATABASE_URL)

# pytest-xdist parallel: each worker is a separate session so the
# session-scoped engine fixture runs once per worker. If they share the same
# TEST_SCHEMA they race on DROP SCHEMA / CREATE SCHEMA (one worker drops the
# schema mid-session of another) and unrelated tests trip with
# `relation "X" does not exist`. Suffix the schema with the xdist worker id
# (`gw0`, `gw1`, …) so each worker gets a private schema. Single-worker runs
# (no xdist) keep TEST_SCHEMA as-is.
_xdist_worker = os.getenv("PYTEST_XDIST_WORKER")
if _xdist_worker and _xdist_worker != "master":
    TEST_SCHEMA = f"{TEST_SCHEMA}_{_xdist_worker}"

# Defensive identifier check — schema name comes from env and is interpolated
# into DDL. Match Postgres unquoted identifier rules: letters, digits, underscore.
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", TEST_SCHEMA):
    raise RuntimeError(f"TEST_SCHEMA must match [A-Za-z_][A-Za-z0-9_]*, got {TEST_SCHEMA!r}")

if TEST_SCHEMA != "public":
    # Append (don't overwrite) so a caller-supplied PGOPTIONS still applies.
    _existing = os.environ.get("PGOPTIONS", "").strip()
    _sep = " " if _existing else ""
    os.environ["PGOPTIONS"] = f"{_existing}{_sep}-c search_path={TEST_SCHEMA}"


# ---------------------------------------------------------------------------
# Per-process libpq application_name — the label the leaked-transaction guard
# uses to tell OUR backends from every other session's.
# ---------------------------------------------------------------------------
#
# Read by libpq at connect time for EVERY psycopg connection in this process
# (same mechanism as PGOPTIONS above), so the async test engine, the app's
# engine, alembic, and any one-off ``psycopg.connect()`` all carry it without
# each having to thread a connect_arg through.
#
# Why the pid is in the name: this box runs dozens of concurrent sessions
# against the same Postgres, and pytest-xdist runs several worker PROCESSES
# against the same database (each with its own TEST_SCHEMA). A guard that
# matched on ``datname`` alone would report another lane's connection as this
# run's leak — an unfixable red on someone else's diff. One process = one
# label = only backends this test process opened.
PG_APPLICATION_NAME = f"billing-tests-{os.getpid()}"
os.environ["PGAPPNAME"] = PG_APPLICATION_NAME


def pgoptions_with(*gucs: str) -> str:
    """Compose ``-c <guc>=<value>`` settings ON TOP OF the inherited ``PGOPTIONS``.

    The ONLY sanctioned way to build a ``connect_args={"options": ...}`` string
    in this test suite::

        connect_args={"options": pgoptions_with("timezone=Europe/Amsterdam")}

    Why a helper rather than a literal: libpq's explicit ``options`` connection
    parameter does not merge with the ``PGOPTIONS`` environment variable, it
    REPLACES it. An engine built with a bare ``"-c timezone=Europe/Amsterdam"``
    therefore silently drops the ``-c search_path=<TEST_SCHEMA>`` installed above
    (and, in CI, the archetype's ``-c statement_timeout=...``) and lands in
    ``public`` — where under a schema-isolated run the tables do not exist, so
    the test fails with a misleading ``UndefinedTable`` instead of anything that
    names the real cause.

    Reads ``os.environ`` at call time, so it must be called when the engine is
    built (fixture body), not at import time.

    Enforced by ``tests/test_pgoptions_inheritance.py`` — an AST scan that fails
    if any ``create_engine``/``create_async_engine`` under ``tests/`` passes an
    ``options`` connect_arg that is not routed through this function.
    """
    inherited = os.environ.get("PGOPTIONS", "").strip()
    added = " ".join(f"-c {guc}" for guc in gucs)
    return f"{inherited} {added}".strip()


def throwaway_db_name(stem: str) -> str:
    """A THROWAWAY database name that no other pytest process can collide with.

    ⛔ A FIXED NAME IS A COLLISION HAZARD BY CONSTRUCTION HERE, not a
    theoretical one. Three test modules build a full alembic chain onto a
    scratch database and tear it down with
    ``DROP DATABASE … WITH (FORCE)`` — and FORCE means "SIGTERM every backend
    attached to it". This box runs dozens of concurrent agent sessions, and
    ``tests/conftest.py`` already documents lanes landing on one another's
    database. Two lanes whose ``TEST_DATABASE_URL`` points at the same
    Postgres SERVER — which they do whenever they share a container and
    separate only by ``TEST_SCHEMA`` — share the scratch name too. Lane A's
    setup then DROPs the database lane B is mid-``alembic upgrade`` on, and
    lane B's connection is terminated inside its transaction. The victim reads
    it as a failing CREATE DATABASE, a deadlock, or a migration that half
    applied, on its own diff. ``tests/_production_tripwire.py`` records
    exactly that shape hitting three lanes on 2026-09-07.

    The pid is what makes it private: one pytest process = one name. That
    covers pytest-xdist for free (every worker is its own process, so the
    older ``PYTEST_XDIST_WORKER`` suffix was a strictly narrower fix — it
    isolated workers from each other and left the lanes colliding), and it
    keeps the name inside Postgres's 63-byte identifier limit.

    An interrupted run leaves its scratch database behind, which the fixed
    name used to self-clean. That is deliberate and bounded: every caller
    still issues ``DROP DATABASE IF EXISTS`` before ``CREATE``, and pids
    recycle, so a later run reclaims the name rather than accumulating
    forever. An orphan named after a dead pid is also legible as an orphan,
    which a shared name never was.
    """
    return f"{stem}_{os.getpid()}"


import fakeredis.aioredis  # noqa: E402
import httpx  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Test environment — set before any app code imports.
os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")
os.environ.setdefault("LICENSES_URL", "http://localhost:9999")
os.environ.setdefault("LOG_FORMAT", "console")
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("OTEL_SDK_DISABLED", "true")
os.environ.setdefault("DASHBOARD_AUTH_ENABLED", "false")
# The periodic app_registry refresh is started by lifespan and writes app_registry
# from whatever httpx.AsyncClient answers. Several tests patch that class GLOBALLY
# (`routers.budget.httpx.AsyncClient` IS `httpx.AsyncClient`), so a live loop
# would race them on the same table. The loop is exercised directly in
# tests/test_app_registry_refresh_loop.py, which also proves the lifespan wiring.
os.environ.setdefault("APP_REGISTRY_REFRESH_SECONDS", "0")

# Force import so coverage tracks these modules
import logging_config as _lc  # noqa: E402, F401
import models.db as _models  # noqa: E402, F401 — register ORM models with Base.metadata
import telemetry as _tel  # noqa: E402, F401
from db import Base  # noqa: E402
from models.db import (  # noqa: E402
    ALLOW_TRUNCATE_GUC,
    ALLOW_TRUNCATE_VALUE,
    ALLOW_UNROLLED_METERING_TRUNCATE_GUC,
    ALLOW_UNROLLED_METERING_TRUNCATE_VALUE,
    ALLOW_UNROLLED_TRUNCATE_GUC,
    ALLOW_UNROLLED_TRUNCATE_VALUE,
)
from tenancy import NIL_TENANT_ID, derived_tenant_id  # noqa: E402


def T(label: str) -> str:  # noqa: N802 - single-letter on purpose: ~500 call sites
    """Deterministic tenant UUID for a readable test label.

    Since ``0011_tenant_id_is_a_uuid`` every ``tenant_id`` column is a real
    Postgres ``uuid``. Seeding a readable label such as ``"t1"`` reproduces the
    production defect INSIDE the suite — it is precisely the value shape the
    migration exists to remove — and now fails with
    ``invalid input syntax for type uuid``.

    ``T("t1")`` keeps the test's intent readable while putting on the wire the
    same value production carries: ``uuid5`` under the fleet-wide tenant
    namespace, byte-identical to what ``src/tenancy.py`` and the migration
    derive for the same label. Deterministic, so a value seeded in one place and
    asserted in another still matches.

    ``T`` MEANS "A REGISTERED TENANT". Since the cost flusher began parking rows
    whose tenant no registry entry vouches for
    (``excluded_from_billing = 'unregistered_tenant'``), a bare derived uuid is
    no longer a normal billable tenant — it is an EXCLUDED one, and a test that
    asserts a token rollup or a budget debit against it would be asserting the
    exclusion path while claiming to test the billing path. Sixteen tests in
    ``test_cost_flusher.py`` did exactly that the moment the check was added, and
    they were right to go red: they had been relying on the fail-open this change
    closes.

    So ``T`` registers what it mints — see :func:`_register_test_tenant`. To
    exercise the EXCLUSION path, use a raw ``uuid.uuid4()``, which is never
    registered; ``tests/test_cost_flusher.py`` does exactly that, and those tests
    are what keep this helper from hiding the control it works around.
    """
    tenant_id = derived_tenant_id(label)
    _register_test_tenant(tenant_id)
    return tenant_id


def _register_test_tenant(tenant_id: str) -> None:
    """Vouch for a ``T()``-minted tenant for the rest of the session.

    ⛔ THIS IS A TEST-ONLY WIDENING OF A PRODUCTION SET, and it is the kind of
    thing that hides a control if done carelessly. Two reasons it is the right
    call here rather than editing ~500 call sites:

    1. What those call sites MEAN is "an ordinary registered tenant". Rewriting
       them to hard-code real registry uuids would make every test depend on the
       licences registry's current contents, so a tenant being renamed upstream
       would redden unrelated tests.
    2. The alternative — leaving them unregistered — would silently convert most
       of the flusher suite into tests of the exclusion path, passing for the
       wrong reason.

    The widening is additive and never removes a real id, so nothing that IS
    registered in production stops being registered here. And it cannot mask the
    control: the exclusion path is asserted directly against ``uuid.uuid4()``
    values, which this never touches.
    """
    import tenancy

    if tenant_id not in tenancy._REGISTERED_TENANT_IDS:
        tenancy._REGISTERED_TENANT_IDS = frozenset(tenancy._REGISTERED_TENANT_IDS) | {tenant_id}


@pytest.fixture(autouse=True)
def _licences_answers_the_tenant_registry(monkeypatch):
    """Serve ``GET /internal/tenants`` from the set :func:`T` registers.

    ⚠️ A DEPENDENCY THE SUITE ACQUIRED ON 2026-09-13, stated here rather than in
    each test. ``services.invoices.assert_tenant_invoiceable`` refuses to close a
    period for a tenant the licences registry does not vouch for, and it asks over
    HTTP via :func:`services.tenant_names.get_tenant_info_strict`. Nothing listens
    on ``test_settings.licenses_url``, so without this every close in the suite
    would refuse with ``TenantRegistryUnavailableError`` — a uniform red that says
    nothing about the code under test.

    ⭐ THE SET IS ``tenancy._REGISTERED_TENANT_IDS``, WHICH IS EXACTLY WHAT ``T()``
    WIDENS, so the documented convention keeps holding across the new gate without
    a single call site changing: ``T("t1")`` is a REGISTERED tenant and closes;
    a raw ``uuid.uuid4()`` is an UNREGISTERED one and is refused. A test that wants
    the refusal asks for it the same way it already asks for the flusher's
    exclusion path.

    ⚠️ URL-SCOPED AND DELEGATING, never a blanket stub — same shape as
    ``tests/test_invoices.py``'s ``_licences_answers_byo_mode``, and the chaining
    is deliberate: this fixture lives in conftest so it is installed FIRST, and
    that module fixture then captures THIS wrapper as its ``real_get``. Each
    handles its own URL and hands everything else on, so neither can silently
    satisfy an expectation the other test wrote.

    ⛔ ``archived`` IS ALWAYS FALSE HERE. A test that wants the archived refusal
    must say so explicitly (override this fixture or patch
    ``get_tenant_info_strict``) — defaulting it from some ambient flag would make
    the archived path reachable by accident and therefore untestable on purpose.

    The module TTL cache is cleared on both sides: it is 5-minute process state
    keyed for the whole registry, so one test's view must not decide another's.
    """
    import tenancy
    from services import tenant_names

    tenant_names.reset_cache()
    real_get = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if str(url).endswith("/internal/tenants"):
            rows = [
                # A synthetic but DETERMINISTIC and UNIQUE name per id: a shared
                # constant would mask any name-resolution assertion, and an empty
                # one would be dropped by `parse_tenant_rows`.
                #
                # ⚠️ `tid[:8]` IS NOT ACTUALLY UNIQUE, so the promise above is
                # only mostly true — and it is left as-is DELIBERATELY. Most
                # registered ids are uuid7, which embeds the mint timestamp, so
                # ids minted near each other share a long prefix; where two
                # collide, `set_live_tenant_registry` sees one name mapping to
                # two uuids and drops it ENTIRELY (correctly — picking one would
                # mis-attribute spend), and NEITHER id resolves to a label.
                # Several tests now depend on that suppression, so widening this
                # to the full id switches overlay labels ON for whole groups of
                # tenants and reddens them. That cleanup is real, and it is its
                # own unit of work — not a side effect of this one.
                {"id": tid, "name": f"t-{tid[:8]}", "kind": "internal", "archived": False}
                for tid in sorted(tenancy._REGISTERED_TENANT_IDS)
                # ⛔ THE NIL TENANT IS NOT SERVED A DISPLAY LABEL, and it used to
                # get that property BY ACCIDENT. `default-anon` (...0000) and the
                # `system` sentinel (...0001) are both shim values and both
                # truncated to `t-00000000`, so the collision above dropped the
                # name and the nil tenant resolved to nothing —
                # `TestFlushEvents::test_flush_events_defaults_for_missing_fields`
                # reads exactly that as "the registry knows no name for a missing
                # tenant". Now that the sentinel is no longer a REGISTERED id
                # (tenancy.SENTINEL_TENANT_IDS — it must resolve by name but
                # never bill), nothing collides with the nil tenant any more and
                # the accident is gone, so the property is stated on purpose.
                #
                # It is the right property on its own terms: the nil tenant is
                # the canonical "no tenant was supplied" marker on both flush
                # paths, and `stored_tenant_name`'s whole contract is that an
                # absence must never be stored as something that reads like data.
                # It stays in `_REGISTERED_TENANT_IDS` — it IS a real registry
                # row and `is_registered_tenant_id` must keep saying so — it
                # simply gets no invented display name here.
                if tid != tenancy.NIL_TENANT_ID
            ]
            return httpx.Response(200, json=rows)
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    yield
    tenant_names.reset_cache()


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Reset cached settings between tests to avoid cross-test contamination."""
    from settings import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def _db_engine_session():
    """Session-scoped engine: schema is created once and dropped at the end.

    Recreating the entire public schema per test adds 8-15s of setup to every
    test and pushes the full unit suite to ~25 minutes. We now create the
    schema once per session and let the per-test `db_engine` fixture truncate
    tables between tests.

    When TEST_SCHEMA is set to a non-public name, we create that schema fresh
    for the session and drop it at teardown. CI uses this for per-run isolation
    against the shared metering_ci database; concurrent runs cannot collide.
    """
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)

    async with engine.begin() as conn:
        if TEST_SCHEMA == "public":
            # CASCADE drop handles FK ordering and stale state from interrupted runs
            await conn.exec_driver_sql("DROP SCHEMA public CASCADE")
            await conn.exec_driver_sql("CREATE SCHEMA public")
        else:
            # Drop+recreate so a previous interrupted run with the same schema
            # name doesn't leave stale tables. CREATE SCHEMA does not depend on
            # search_path, so it works even though search_path points at a
            # not-yet-existing schema at connection time.
            await conn.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{TEST_SCHEMA}" CASCADE')
            await conn.exec_driver_sql(f'CREATE SCHEMA "{TEST_SCHEMA}"')
            # Stamp the schema with a `created_at` comment so any external
            # sweeper can tell an in-flight schema from an abandoned one.
            # Historical note: this used to claim it was feeding a
            # `postgres-ci-gc` CronJob at
            # clusters/strato/services/postgres-ci-gc/cronjob.yaml. That
            # CronJob no longer exists anywhere in gitops-production (the
            # shared CI Postgres it garbage-collected was decommissioned in
            # favour of per-job ephemeral containers), so nothing consumes
            # this stamp today. Kept because it is free and self-describing.
            from datetime import UTC, datetime

            created_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            await conn.exec_driver_sql(
                f"COMMENT ON SCHEMA \"{TEST_SCHEMA}\" IS 'created_at={created_at}'"
            )
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        if TEST_SCHEMA == "public":
            await conn.exec_driver_sql("DROP SCHEMA public CASCADE")
            await conn.exec_driver_sql("CREATE SCHEMA public")
        else:
            await conn.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{TEST_SCHEMA}" CASCADE')
    await engine.dispose()


@pytest.fixture
async def db_engine(_db_engine_session, request):
    """Per-test engine handle with table truncation for isolation.

    Truncates all tables (TRUNCATE ... CASCADE) before handing the engine to
    the test. This is orders of magnitude faster than dropping and recreating
    the schema (which also has to re-register every ORM model).
    """
    # Build the TRUNCATE statement once per call from the metadata registry.
    tables = ", ".join(f'"{t.name}"' for t in Base.metadata.sorted_tables)
    if tables:
        async with _db_engine_session.begin() as conn:
            # Arm the BEFORE TRUNCATE guard installed by
            # src/models/db/truncate_guard.py (alembic 0017). Without this every
            # DB test dies on its own isolation step.
            #
            # SET LOCAL, not SET: the value dies with this transaction, so it
            # cannot survive on a pooled connection into a test that did not ask
            # for it. A plain SET would leave the ledger unguarded for the rest
            # of the session.
            await conn.exec_driver_sql(f"SET LOCAL {ALLOW_TRUNCATE_GUC} = '{ALLOW_TRUNCATE_VALUE}'")
            # Arm the WATERMARK gate installed by src/models/db/unrolled_guard.py
            # (alembic 0019) as well. It is a SECOND, DELIBERATELY SEPARATE GUC:
            # 0017's hatch says "I mean to truncate", this one says "I mean to
            # delete cost events the rollup has not aggregated yet". A test
            # fixture seeds raw events and almost never seeds a matching
            # billing_cost_daily bucket, so from the gate's point of view every
            # test teardown IS an un-rolled truncate — and it should be, which is
            # exactly why the suite has to say so out loud rather than inherit
            # the permission from the line above.
            await conn.exec_driver_sql(
                f"SET LOCAL {ALLOW_UNROLLED_TRUNCATE_GUC} = '{ALLOW_UNROLLED_TRUNCATE_VALUE}'"
            )
            # Arm the METERING watermark gate installed by
            # src/models/db/metering_unrolled_guard.py (alembic 0023). A THIRD
            # GUC, and it has to be armed separately here for the same reason it
            # exists at all: the line above authorises deleting un-aggregated
            # COST events, and this statement also empties
            # billing_metering_events. Two ledgers, two assertions — an operator
            # draining one must not silently drain the other, and neither must a
            # fixture.
            await conn.exec_driver_sql(
                f"SET LOCAL {ALLOW_UNROLLED_METERING_TRUNCATE_GUC} = "
                f"'{ALLOW_UNROLLED_METERING_TRUNCATE_VALUE}'"
            )
            await conn.exec_driver_sql(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
    yield _db_engine_session
    await _assert_no_nil_tenant_rows(_db_engine_session, request)


# ---------------------------------------------------------------------------
# nil-tenant tripwire
# ---------------------------------------------------------------------------

#: Every mapped table that carries a ``tenant_id``. Computed once at import
#: from the same registry ``db_engine`` truncates from, so a new tenant-scoped
#: table is policed the moment it is mapped — nothing to hand-maintain.
_TENANT_SCOPED_TABLES = [t for t in Base.metadata.sorted_tables if "tenant_id" in t.c]

#: One statement, one round trip: count the nil-tenant rows in every
#: tenant-scoped table. Table names come from SQLAlchemy metadata (not user
#: input) and NIL_TENANT_ID is a module constant, so interpolation is safe.
_NIL_TENANT_COUNT_SQL = " UNION ALL ".join(
    f"SELECT '{t.name}' AS table_name, count(*) AS n "
    f"FROM \"{t.name}\" WHERE tenant_id = '{NIL_TENANT_ID}'::uuid"
    for t in _TENANT_SCOPED_TABLES
)


async def _assert_no_nil_tenant_rows(engine, request) -> None:
    """Fail any DB test that silently parked rows on the nil tenant.

    The class this closes
    ---------------------
    ``tenant_id`` is a real Postgres ``uuid`` since ``0011``. Readable labels
    ("t1", "test-tenant") were mechanically wrapped in :func:`T`, but a MISSED
    site is just as green as a converted one: ``_normalize_tenant_id`` sinks an
    unresolvable label to :data:`NIL_TENANT_ID` instead of raising, the row
    lands on the nil tenant rather than the tenant the test names, and every
    assertion the test makes still holds. Nothing anywhere says so.

    ``db_engine`` TRUNCATEs every table before handing the engine over, so the
    baseline for each test is a guaranteed zero. Any nil-tenant row visible
    after the test was therefore written BY that test.

    This runs in ``db_engine``'s teardown rather than as an autouse fixture on
    purpose: an autouse fixture would have to reach the engine through
    ``request.getfixturevalue("db_engine")``, and resolving an ASYNC fixture
    that way from inside a running event loop raises
    ``RuntimeError: Runner.run() cannot be called from a running event loop``.
    Hanging it off ``db_engine`` also keeps the cost where it belongs: the
    roughly half of the suite that never touches Postgres pays nothing.

    Opt out with ``@pytest.mark.nil_tenant`` — for the handful of tests whose
    subject IS the nil-sink path. That marker is deliberately narrow and is
    itself policed by ``tests/test_nil_tenant_marker_is_narrow.py``.

    HOW A TRIP READS. Because this fires in TEARDOWN, pytest reports the
    offending test as ``1 passed ... 1 error`` — the call phase really did pass,
    the teardown is what failed — and the run exits non-zero. That is not the
    guard being swallowed; measured 2026-08-25 with a deliberate nil-writing
    probe: ``PYTEST_EXIT=1``. Read the exit code, not the word "passed".
    """
    if not _NIL_TENANT_COUNT_SQL:
        return
    if request.node.get_closest_marker("nil_tenant") is not None:
        return

    async with engine.connect() as conn:
        rows = (await conn.exec_driver_sql(_NIL_TENANT_COUNT_SQL)).all()

    offenders = {name: n for name, n in rows if n}
    if offenders:
        pytest.fail(
            f"rows landed on NIL_TENANT_ID: {offenders}; if that is intended, "
            "mark the test @pytest.mark.nil_tenant. Otherwise a tenant literal "
            'in this test is not a uuid — wrap it in T("label") — and the row '
            "was written against the nil tenant, not the tenant the test names."
        )


@pytest.fixture
async def db_session(db_engine) -> AsyncGenerator[AsyncSession]:
    """Single session for direct DB operations."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture
async def db_session_factory(db_engine) -> async_sessionmaker[AsyncSession]:
    """Session factory for services that create their own sessions."""
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture
def fake_redis():
    """Fakeredis instance for testing Redis-dependent code."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture(autouse=True)
def _reset_redis_supervisor():
    """Clear the redis_client supervisor globals between tests.

    `_failing_since` / `_last_rebuild_at` / `_rebuild_count` / `_settings` are
    module-level, so a test that leaves a stale failure clock behind can make
    an unrelated later test cross the terminal grace window and rebuild a
    client it never asked for. Reset before AND after, so this holds whichever
    order tests run in.
    """
    import redis_client as rc

    def _clear():
        # getattr-guarded: this fixture must not turn a MISSING supervisor into
        # a setup ERROR. A test that proves the fix is needed has to fail on its
        # own assertion, not on this fixture.
        rc._settings = None
        reset = getattr(rc, "_reset_supervisor_state", None)
        if reset is not None:
            reset()

    _clear()
    yield
    _clear()


@pytest.fixture
def topup_enabled(monkeypatch):
    """Arm ``BUDGET_TOPUP_ENABLED`` for a test that exercises a successful top-up.

    The route refuses with 409 by default (no free top-ups until payment
    processing exists), so every test that expects a top-up to WRITE must opt in
    explicitly. The cache clear is what makes the env change visible:
    ``get_settings`` is ``lru_cache``d.
    """
    from settings import get_settings

    monkeypatch.setenv("BUDGET_TOPUP_ENABLED", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def test_settings():
    """Test-configured Settings instance."""
    from settings import Settings

    # Tests run against direct Postgres on TEST_DATABASE_URL (no pgbouncer);
    # set leader_election_dsn explicitly so the run_rollup_loop's DSN
    # rewrite doesn't try to reach the production primary.
    return Settings(
        database_url=TEST_DATABASE_URL,
        leader_election_dsn=TEST_DATABASE_URL,
        redis_url="redis://localhost:6379/15",
        log_format="console",
        environment="test",
        licenses_url="http://localhost:9999",
    )


@pytest.fixture
def app_client(db_engine, monkeypatch):
    """FastAPI TestClient with test database, no Redis."""
    import redis_client
    from db import get_session
    from main import app

    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)

    async def _test_session() -> AsyncGenerator[AsyncSession]:
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = _test_session
    monkeypatch.setattr(redis_client, "_redis", None)

    yield TestClient(app)

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Leaked-transaction guard — fail AT the leaking test, not at a random victim
# ---------------------------------------------------------------------------
#
# The mechanism, the two Postgres states, and why the scope is
# ``application_name`` and not ``datname`` are documented in
# ``tests/_leaked_transaction_guard.py``. Loaded BY PATH for the same reason
# ``_production_tripwire`` is (``tests/`` must not go on ``sys.path``; it
# shadows ``src/routers``), and kept in its own module so that
# ``tests/test_leaked_transaction_guard.py`` can import the SAME code it
# asserts — an unasserted guard is one nobody notices has stopped working.
_leak_spec = _importlib_util.spec_from_file_location(
    "_leaked_transaction_guard", _Path(__file__).parent / "_leaked_transaction_guard.py"
)
assert _leak_spec is not None and _leak_spec.loader is not None  # loader failure must be loud
_leaked_transaction_guard = _importlib_util.module_from_spec(_leak_spec)
_leak_spec.loader.exec_module(_leaked_transaction_guard)

LEAK_GUARD = _leaked_transaction_guard.LeakedTransactionGuard(
    TEST_DATABASE_URL, PG_APPLICATION_NAME
)


@pytest.fixture(autouse=True)
def _no_leaked_transactions(request):
    """Fail the test that leaves a transaction open on a pooled connection.

    Autouse and function-scoped, so it is set up BEFORE the test's own fixtures
    and therefore finalised AFTER them — after ``db_engine``'s TRUNCATE
    teardown and after any test-local engine's ``dispose()``. A leak that only
    surfaces once a fixture unwinds is still attributed to the right test.

    HOW A TRIP READS: like the nil-tenant tripwire above, this fires in
    TEARDOWN, so pytest reports ``1 passed ... 1 error`` and exits non-zero.
    Read the exit code, not the word "passed".
    """
    yield

    rows = LEAK_GUARD.settled()
    if rows:
        pytest.fail(
            f"{request.node.nodeid} leaked {len(rows)} transaction(s) on a pooled "
            "connection. They still hold their locks, so the NEXT test's TRUNCATE "
            "blocks and dies at statement_timeout — and the failure lands on a test "
            "that shares nothing with this one:\n"
            f"{_leaked_transaction_guard.describe(rows)}\n"
            "Close or roll back whatever this test opened. An async engine needs "
            "`await engine.dispose()`; a garbage-collected async connection can "
            "never run its rollback, so the transaction survives the whole run."
        )
