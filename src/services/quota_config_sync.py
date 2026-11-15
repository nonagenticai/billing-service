"""Periodic repair of ``request_quota_config.quota_monthly`` from the licence registry.

Why this exists
---------------
The mirrored request quota is written by provisioning (``POST /api/quota/config``)
and by the operator-only ``POST /api/budget/sync-from-licenses``. Nothing ran on a
timer, so a per-licence ``quota_override`` set upstream AFTER provisioning (#277)
never reached billing: measured 2026-10-21, licence ``01a0fdb9…`` mirrored
1,000,000 while its override was 10,000, and the reconcile loop's read-only
detector (``billing_quota_config_value_drift`` / ``quota_config_value_drift``)
reported it every tick with nothing to act on it.

What one tick does, and what it deliberately does NOT do
---------------------------------------------------------
It reads the registry through the reconcile module's ``_fetch_licence_lifecycle``
(same payload guards: an incomplete read raises and nothing is written), takes the
EFFECTIVE limit (override wins over plan) for every ACTIVE licence, and updates
the ``quota_monthly`` of mirror rows that already exist, are ``active``, and
disagree.

It never inserts a row (creation stays with provisioning / the manual sync,
which know the tenant and app) and never deletes or deactivates one (that is
the manual sync's and the reconcile loop's gated job). So turning it on cannot
widen enforcement to a licence nobody configured, nor remove a limit.

Leader-elected on its own advisory lock (``QUOTA_CONFIG_SYNC_LOCK_KEY``) so two
replicas never both fetch the registry and write the same rows. The first tick
runs immediately at startup rather than one interval later.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import TYPE_CHECKING, Any, cast

import httpx
import structlog
from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db.quota import RequestQuotaConfig
from observability import metrics as observability_metrics
from services.leader_election import QUOTA_CONFIG_SYNC_LOCK_KEY, AsyncLeaderElection
from services.licence_budget_reconcile import LicenceFetchError, _fetch_licence_lifecycle
from settings import Settings

if TYPE_CHECKING:
    from sqlalchemy.engine import CursorResult

logger = structlog.stdlib.get_logger(__name__)

_ACTIVE = "active"


async def repair_quota_config_values_once(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    token_provider: Any | None,
    *,
    client: httpx.AsyncClient | None = None,
) -> list[uuid.UUID]:
    """One repair pass. Returns the licence ids whose mirrored quota was changed.

    Raises ``LicenceFetchError`` (from the shared fetch) on any incomplete
    registry read, before touching the database.
    """
    owns_client = client is None
    http_client = client or httpx.AsyncClient(timeout=10.0)
    try:
        lifecycle = await _fetch_licence_lifecycle(settings, token_provider, client=http_client)
    finally:
        if owns_client:
            await http_client.aclose()

    expected = {
        lid: quota
        for lid, quota in lifecycle.plan_quota_monthly.items()
        if lid in lifecycle.active_ids
    }
    repaired: list[tuple[uuid.UUID, int, int]] = []
    if expected:
        async with session_factory() as session:
            rows = await session.execute(
                select(RequestQuotaConfig.license_id, RequestQuotaConfig.quota_monthly).where(
                    RequestQuotaConfig.license_id.in_(list(expected)),
                    RequestQuotaConfig.status == _ACTIVE,
                )
            )
            for license_id, mirrored in rows.all():
                want = expected[license_id]
                if mirrored == want:
                    continue
                # Re-check status and the value we read, so a row revoked (or
                # re-synced) by another writer since the SELECT is left alone.
                result = cast(
                    "CursorResult[Any]",
                    await session.execute(
                        update(RequestQuotaConfig)
                        .where(
                            RequestQuotaConfig.license_id == license_id,
                            RequestQuotaConfig.status == _ACTIVE,
                            RequestQuotaConfig.quota_monthly == mirrored,
                        )
                        .values(quota_monthly=want)
                    ),
                )
                if result.rowcount:
                    repaired.append((license_id, mirrored, want))
            await session.commit()

    for license_id, mirrored, want in repaired:
        logger.info(
            "quota_config_value_repaired",
            license_id=str(license_id),
            mirrored=mirrored,
            effective=want,
        )
    observability_metrics.quota_config_value_repair_repaired_total.inc(len(repaired))
    observability_metrics.quota_config_value_repair_last_success_timestamp_seconds.set(time.time())
    return [license_id for license_id, _, _ in repaired]


async def run_quota_config_sync_loop(
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    token_provider: Any | None,
    stop_event: asyncio.Event,
) -> None:
    """Leader-elected periodic :func:`repair_quota_config_values_once`.

    The first attempt happens at startup (wait 0); afterwards the leader waits
    ``quota_config_sync_interval_seconds`` and a follower re-races for the lock
    every ``min(leader_retry_interval_seconds, interval)``.
    """
    election = AsyncLeaderElection(
        settings.resolve_leader_election_dsn(), QUOTA_CONFIG_SYNC_LOCK_KEY
    )
    interval = settings.quota_config_sync_interval_seconds
    retry_seconds = min(settings.leader_retry_interval_seconds, interval)
    wait_seconds = 0.0
    try:
        while not stop_event.is_set():
            try:
                if wait_seconds > 0:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                    if stop_event.is_set():
                        break
                wait_seconds = retry_seconds

                leader = False
                try:
                    leader = await election.is_leader()
                finally:
                    observability_metrics.quota_config_value_repair_is_leader.set(
                        1 if leader else 0
                    )
                if not leader:
                    continue

                await repair_quota_config_values_once(settings, session_factory, token_provider)
                # Only a COMPLETED tick earns the full interval; a failed one
                # retries after retry_seconds.
                wait_seconds = interval
            except asyncio.CancelledError:
                break
            except LicenceFetchError:
                observability_metrics.quota_config_value_repair_failures_total.labels(
                    reason="licences_unreadable"
                ).inc()
                logger.warning("quota_config_sync_fetch_failed", exc_info=True)
            except SQLAlchemyError:
                observability_metrics.quota_config_value_repair_failures_total.labels(
                    reason="infra_error"
                ).inc()
                logger.warning("quota_config_sync_error_infra", exc_info=True)
            except Exception:
                observability_metrics.quota_config_value_repair_failures_total.labels(
                    reason="unexpected"
                ).inc()
                logger.exception("quota_config_sync_error")
    finally:
        with contextlib.suppress(Exception):
            await election.release()
