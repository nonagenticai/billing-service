"""Mirror the licences APP registry into ``app_registry`` (migration 0050).

``services.license_resolver.get_app_key_map`` resolves the app KEY a caller
sends in ``X-App-Id`` through this table. It must hold EVERY registered app,
not just the ones that own a licence: platform apps registered by licences'
``scripts/seed_apps.py`` with no licence (``core-app``, ``chat-bridge``,
``chat-bridge-rmt``, ``chat-bridge-merqi``) are invisible to
``/internal/licenses``, and a key map built from licences left their spend with
``app_id`` NULL.

Source: ``GET {licenses_url}/internal/apps?include_revoked=true`` (licences'
``require_internal_service`` -- the ``service-internal`` role billing's service
account carries). ``GET /apps`` lists the same rows but needs
``license-admin``/``license-reader``, which billing's account does not hold
(403, measured in the pod 2026-10-07).

Called by ``POST /api/budget/sync-from-licenses`` AND, on a timer, by
:func:`run_app_registry_refresh_loop` (``Settings.app_registry_refresh_seconds``).
The endpoint alone was not enough: it is fired only by two n8n webhook
workflows, so the mirror froze at its last call (live 2026-10-24: all 53 rows
synced 2026-10-07, and ``agent``/``care-bus``, registered after that, were
absent -- their router spend stored ``app_id`` NULL). The endpoint cannot be the
timer either: it also writes ``request_quota_config``. The loop touches ONLY
``app_registry``.

Every failure is returned as an ``error`` string in the result, never raised:
the app registry is one part of a reconcile whose licence half must not be
rolled back by it.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
import structlog
from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import AppRegistry
from service_auth import ServiceTokenProvider
from settings import Settings

logger = structlog.stdlib.get_logger(__name__)

INTERNAL_APPS_PATH = "/internal/apps"
_TIMEOUT_SECONDS = 10.0

#: "ME_APREG" -- a TRANSACTION-scoped advisory lock serialising the refresh
#: loop's write across replicas. Transaction-scoped on purpose (unlike the
#: session-scoped leader keys in services/leader_election.py): it is held only
#: for the one tx and released by its COMMIT/ROLLBACK, which is correct on the
#: transaction-mode pooler. It serialises rather than elects: every replica still
#: runs its own sync, because ``reset_cache`` below only clears THIS process's
#: key-map cache.
APP_REGISTRY_REFRESH_LOCK_KEY: int = 0x4D45_5F41_5052_4547

#: Floor on a non-zero refresh interval. Module-level so tests can lower it.
_MIN_REFRESH_INTERVAL_SECONDS: float = 30.0


@dataclass
class RegistryApps:
    """A parsed ``/internal/apps`` listing: ``app_id -> (app_key, revoked)``."""

    apps: dict[uuid.UUID, tuple[str, bool]] = field(default_factory=dict)
    #: Rows that carried no parseable uuid ``id`` or no non-empty ``key``.
    invalid_count: int = 0


@dataclass
class AppRegistrySyncResult:
    upserted: int = 0
    removed: int = 0
    revoked: int = 0
    invalid: int = 0
    error: str | None = None


def parse_registry_apps(payload: Any) -> RegistryApps:
    """Parse the listing; a row without a uuid ``id`` and a non-empty ``key`` is invalid.

    A duplicate ``key`` across two ids cannot come from the registry (``apps.key``
    is UNIQUE upstream); if it ever does, BOTH rows are counted invalid rather
    than one being picked -- exactly-one-or-nothing, as the key map requires.
    """
    result = RegistryApps()
    if not isinstance(payload, list):
        return result
    by_key: dict[str, list[uuid.UUID]] = {}
    for row in payload:
        if not isinstance(row, dict):
            result.invalid_count += 1
            continue
        key = row.get("key")
        try:
            app_id = uuid.UUID(str(row.get("id")))
        except ValueError:
            result.invalid_count += 1
            continue
        if not isinstance(key, str) or not key.strip():
            result.invalid_count += 1
            continue
        key = key.strip()
        result.apps[app_id] = (key, row.get("revoked") is True)
        by_key.setdefault(key, []).append(app_id)
    for key, ids in by_key.items():
        if len(ids) > 1:
            logger.warning("app_registry_duplicate_key_dropped", app_key=key)
            for app_id in ids:
                result.apps.pop(app_id, None)
                result.invalid_count += 1
    return result


async def fetch_registry_apps(
    settings: Settings, provider: ServiceTokenProvider | None
) -> tuple[RegistryApps | None, str | None]:
    """``GET /internal/apps?include_revoked=true`` -> ``(parsed, None)`` or ``(None, error)``."""
    base = (settings.licenses_url or "").strip().rstrip("/")
    if not base:
        return None, "licenses_url is not configured"
    headers: dict[str, str] = {}
    if provider is not None:
        token = await provider.get_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT_SECONDS) as client:
            resp = await client.get(
                f"{base}{INTERNAL_APPS_PATH}",
                headers=headers,
                params={"include_revoked": "true"},
            )
    except httpx.RequestError:
        logger.warning("app_registry_fetch_unreachable", exc_info=True)
        return None, "licenses service unreachable (apps)"
    if resp.status_code != 200:
        logger.warning("app_registry_fetch_failed", status=resp.status_code)
        return None, f"licenses /internal/apps returned {resp.status_code}"
    return parse_registry_apps(resp.json()), None


async def apply_registry_apps(
    session: AsyncSession, parsed: RegistryApps, *, remove_missing: bool = True
) -> AppRegistrySyncResult:
    """Upsert every listed app and (by default) delete the rows the registry no longer lists.

    Runs in its own SAVEPOINT so a failure here cannot poison the caller's
    transaction. Removal is skipped -- never guessed -- when the listing is
    empty or carried any invalid row: absence cannot be proven from a listing
    that we could not fully read (the same reason ``delete_absent_quota_configs``
    refuses an empty active set). A revoked app is KEPT, flagged.

    ``remove_missing=False`` makes the apply UPSERT-ONLY: no row is deleted for
    being absent from the listing. The periodic loop
    (:func:`run_app_registry_refresh_loop`) uses it, because a valid-but-
    TRUNCATED listing (a paginated or partial upstream answer) would otherwise
    wipe every unlisted app on every replica every interval, with nobody
    watching. Removal stays with the operator-invoked
    ``POST /api/budget/sync-from-licenses`` (the default, ``True``). A key that
    moved to another id is still re-paired either way: that delete only touches
    rows whose id or key IS in the listing.
    """
    result = AppRegistrySyncResult(invalid=parsed.invalid_count)
    if not parsed.apps:
        result.error = "licenses /internal/apps returned no usable apps; app_registry untouched"
        logger.warning("app_registry_sync_no_usable_apps", invalid=parsed.invalid_count)
        return result
    pairs = [(app_id, key) for app_id, (key, _r) in parsed.apps.items()]
    try:
        async with session.begin_nested():
            # Upsert-only callers skip removal entirely: absence from this
            # listing proves nothing to them (see the docstring).
            if remove_missing and parsed.invalid_count == 0:
                removed = await session.execute(
                    delete(AppRegistry)
                    .where(AppRegistry.app_id.not_in(list(parsed.apps)))
                    .returning(AppRegistry.app_id)
                )
                result.removed = len(removed.all())
            elif remove_missing:
                logger.warning(
                    "app_registry_removal_skipped_invalid_rows", invalid=parsed.invalid_count
                )
            # A listed app whose key CHANGED (or a key that moved to another id)
            # would trip the UNIQUE(app_key) on upsert; drop the stale pairing
            # first. Not counted as removed: the app is re-inserted just below.
            await session.execute(
                delete(AppRegistry).where(
                    AppRegistry.app_id.in_(list(parsed.apps))
                    | AppRegistry.app_key.in_([k for _i, k in pairs]),
                    tuple_(AppRegistry.app_id, AppRegistry.app_key).not_in(pairs),
                )
            )
            stmt = pg_insert(AppRegistry).values(
                [
                    {"app_id": app_id, "app_key": key, "revoked": revoked}
                    for app_id, (key, revoked) in parsed.apps.items()
                ]
            )
            await session.execute(
                stmt.on_conflict_do_update(
                    index_elements=[AppRegistry.app_id],
                    set_={
                        "app_key": stmt.excluded.app_key,
                        "revoked": stmt.excluded.revoked,
                        "synced_at": func.now(),
                    },
                )
            )
    except SQLAlchemyError:
        logger.warning("app_registry_sync_failed", exc_info=True)
        return AppRegistrySyncResult(
            invalid=parsed.invalid_count, error="app_registry write failed"
        )
    result.upserted = len(parsed.apps)
    result.revoked = sum(1 for _k, revoked in parsed.apps.values() if revoked)
    return result


async def sync_app_registry(
    session: AsyncSession, settings: Settings, provider: ServiceTokenProvider | None
) -> AppRegistrySyncResult:
    """Fetch the licences app registry and mirror it. Never raises for upstream/DB failure.

    Does NOT commit and does NOT reset the key-map cache: the caller owns the
    transaction, and must call ``services.license_resolver.reset_cache()``
    AFTER its commit when ``result.error is None``. Resetting before the commit
    lets a concurrent flusher rebuild the TTL-cached map from the pre-commit
    rows and hold that stale map for the full 300s TTL.
    """
    parsed, error = await fetch_registry_apps(settings, provider)
    if parsed is None:
        return AppRegistrySyncResult(error=error)
    result = await apply_registry_apps(session, parsed)
    logger.info(
        "app_registry_synced",
        upserted=result.upserted,
        removed=result.removed,
        revoked=result.revoked,
        invalid=result.invalid,
    )
    return result


async def refresh_app_registry_once(
    session_factory: Callable[[], AsyncSession],
    settings: Settings,
    provider: ServiceTokenProvider | None,
) -> AppRegistrySyncResult:
    """One loop tick. Writes ONLY ``app_registry``, UPSERT-ONLY (never removes).

    Order matters:
      1. fetch ``/internal/apps`` with NO session open -- a 10s HTTP call held
         inside a transaction would pin an idle-in-transaction pooler connection
         and make every other replica queue on the lock behind our network wait;
      2. in ONE transaction: ``pg_advisory_xact_lock`` -> apply -> commit (the
         lock is released by the commit);
      3. ``reset_cache()`` only AFTER the commit, so no flusher can rebuild the
         map from pre-commit rows and cache it for the TTL.
    """
    from observability import metrics as om
    from services.license_resolver import reset_cache

    parsed, error = await fetch_registry_apps(settings, provider)
    if parsed is None:
        result = AppRegistrySyncResult(error=error)
    else:
        async with session_factory() as session:
            await session.execute(select(func.pg_advisory_xact_lock(APP_REGISTRY_REFRESH_LOCK_KEY)))
            result = await apply_registry_apps(session, parsed, remove_missing=False)
            # Commit even on a reported apply error: the apply ran in its own
            # SAVEPOINT, already rolled back, so this only ends the transaction
            # and releases the xact lock.
            await session.commit()
    if result.error is None:
        reset_cache()
        om.app_registry_refresh_total.labels(result="ok").inc()
        om.app_registry_refresh_last_success_timestamp_seconds.set(time.time())
        logger.info(
            "app_registry_refreshed",
            upserted=result.upserted,
            revoked=result.revoked,
            invalid=result.invalid,
        )
    else:
        om.app_registry_refresh_total.labels(result="failed").inc()
        logger.warning("app_registry_refresh_failed", error=result.error)
    return result


async def run_app_registry_refresh_loop(
    settings: Settings,
    session_factory: Callable[[], AsyncSession],
    provider: ServiceTokenProvider | None,
    stop_event: asyncio.Event,
) -> None:
    """Keep ``app_registry`` (the X-App-Id key map's source) fresh for the life of the process.

    Modelled on ``services.tenant_names.run_tenant_registry_refresh_loop``:
    first tick immediately (not awaited by ``lifespan``, so a licences timeout
    cannot delay readiness), every iteration wrapped so a failure is logged and
    counted and the loop carries on, and a failed fetch/apply leaves the
    previous rows in place (``apply_registry_apps`` never touches the table on a
    failed or unusable listing). UPSERT-ONLY: an app missing from a listing is
    never deleted by the loop (see ``apply_registry_apps(remove_missing=...)``).
    Runs on every replica; see
    :data:`APP_REGISTRY_REFRESH_LOCK_KEY` for why that is safe.

    ``settings.app_registry_refresh_seconds <= 0`` disables the loop.
    """
    from observability import metrics as om

    if settings.app_registry_refresh_seconds <= 0:
        logger.info("app_registry_refresh_loop_disabled")
        return
    interval = max(_MIN_REFRESH_INTERVAL_SECONDS, float(settings.app_registry_refresh_seconds))
    logger.info("app_registry_refresh_loop_started", interval_seconds=interval)
    while not stop_event.is_set():
        try:
            await refresh_app_registry_once(session_factory, settings, provider)
        except Exception:
            # Defence in depth: sync_app_registry returns its failures, but the
            # lock/commit (or the token provider) can still raise. The loop
            # dying would freeze the mirror again with nothing but a task-death
            # log to say so.
            with contextlib.suppress(Exception):  # metrics must never kill the loop
                om.app_registry_refresh_total.labels(result="error").inc()
            logger.warning("app_registry_refresh_loop_error", exc_info=True)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except TimeoutError:
            continue
