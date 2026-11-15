"""A drift DETECTOR for the request_quota_config <-> licences mirror.

THE DEFECT
----------
``request_quota_config`` (services.quota) mirrors the licences registry, and
the two directions that keep it in sync are asymmetric:

* PUSH (licences -> billing): ``licenses-metering-reconcile``, ``41 3 * * *``
  UTC. INSERT/UPDATE only -- it never deletes and never marks a row non-active.
* PULL (billing <- licences): ``POST /api/budget/sync-from-licenses``. This is
  the ONLY thing that deletes an absent row or marks one non-active, and its
  own docstring says verbatim: "OPERATOR-TRIGGERED ONLY... no scheduler, no
  CronJob, no watcher loop, no startup hook."
* Revocation (``expire_stale_licences.py``, ``revoke_expired_tokens.py``) makes
  NO outbound call to billing at all.

So the mirror can only ever grow stale, and before this module existed nothing
could tell "clean by design" apart from "clean because nobody has looked in
three weeks" -- the exact ambiguity that let 43 unrevoked tokens keep
attracting invoice attribution off revoked licences (measured on the licences
registry 2026-09-08, see services.license_resolver's module docstring).

WHAT FAILS ON ``origin/main``
------------------------------
Every test below. ``services.quota.record_quota_config_drift`` and
``record_quota_config_sync_success`` do not exist there, and neither do the
three gauges (``quota_config_orphan_total``, ``quota_config_missing_total``,
``quota_config_sync_last_success_timestamp_seconds``) in
``observability.metrics`` -- so the module-level import below raises
``ImportError`` at collection, and the subprocess in
``TestTheGaugesAreBornAtZero`` exits non-zero instead of printing JSON.

WHY THIS IS READ-ONLY
----------------------
``record_quota_config_drift`` deletes nothing and marks nothing; it only
computes a set comparison against the mirror's current rows and publishes
gauges. Nothing here adds a scheduler -- the gauges are computed and set
exactly where ``routers.budget.sync_from_licenses`` already has a live,
just-fetched ``known_license_ids`` / ``active_license_ids`` pair from
``GET {licenses_url}/internal/licenses``, because there is no dblink between
billing's and licences' separate Postgres databases and that operator-
triggered fetch is the only truthful comparison available.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

import observability.metrics as observability_metrics
from services.quota import (
    record_quota_config_drift,
    record_quota_config_sync_success,
    upsert_request_quota_config,
)
from tests.conftest import T

# Read the three series out of a FRESHLY imported observability.metrics, in a
# clean interpreter that has run no test and mutated nothing. The default
# prometheus registry is module-level state shared by the whole pytest
# session (see tests/test_metrics_endpoint.py's identical rationale), so "the
# value at birth" cannot be measured in-process without becoming
# order-dependent on which test module happens to collect first.
_COLLECT_AT_BOOT = """
import json
from observability.metrics import (
    quota_config_missing_total,
    quota_config_orphan_total,
    quota_config_sync_last_success_timestamp_seconds,
)
print(json.dumps({
    "orphan_absent": quota_config_orphan_total.labels(reason="absent")._value.get(),
    "orphan_non_active": quota_config_orphan_total.labels(reason="non_active")._value.get(),
    "missing": quota_config_missing_total._value.get(),
    "last_success": quota_config_sync_last_success_timestamp_seconds._value.get(),
}))
"""


def _gauge(metric, **labels) -> float:
    child = metric.labels(**labels) if labels else metric
    return float(child._value.get())


async def _seed(session, license_id: uuid.UUID, *, tenant: str, quota: int = 1000) -> None:
    await upsert_request_quota_config(
        session,
        license_id,
        T(tenant),
        quota,
        app_id=uuid.uuid4(),
        app_name="KAG Module",
    )


class TestTheGaugesAreBornAtZero:
    @pytest.mark.unit
    def test_a_fresh_interpreter_exports_all_three_series_at_zero(self):
        """A never-fired gauge must read 0, not be absent.

        Absence and zero are the same observation to a dashboard: a gauge that
        has never been set is indistinguishable from one that was never wired
        up. Birthing at zero (epoch, for the timestamp) is what makes
        ``quota_config_orphan_total`` alertable with a plain ``> 0`` and makes
        ``time() - quota_config_sync_last_success_timestamp_seconds`` read as
        a huge, obviously-"never synced" age immediately on a fresh deploy
        rather than as an empty vector.
        """
        src_root = Path(__file__).parent.parent / "src"
        proc = subprocess.run(  # noqa: S603
            [sys.executable, "-c", _COLLECT_AT_BOOT],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
            cwd=str(Path(__file__).parent.parent),
            env={**os.environ, "PYTHONPATH": str(src_root)},
        )
        born = json.loads(proc.stdout.strip().splitlines()[-1])
        assert born == {
            "orphan_absent": 0.0,
            "orphan_non_active": 0.0,
            "missing": 0.0,
            "last_success": 0.0,
        }, f"not born at zero: {born}"


class TestOrphanDetection:
    @pytest.mark.unit
    async def test_a_revoked_and_an_absent_licence_are_both_reported_as_orphans(self, db_session):
        """The exact production shape: one live row, one revoked-but-known
        row, one row whose licence licences no longer returns at all.

        ``known_license_ids`` / ``active_license_ids`` are passed exactly as
        ``routers.budget.sync_from_licenses`` builds them from one live fetch:
        every id licences returned (regardless of status) vs. only the active
        ones. ``revoked_id`` is a member of the first set but not the second --
        that is what distinguishes a revoked licence (marked, not deleted,
        per migration 0025) from one that has vanished from the registry
        entirely.
        """
        active_id, revoked_id, absent_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, active_id, tenant="acme-active")
        await _seed(db_session, revoked_id, tenant="acme-revoked")
        await _seed(db_session, absent_id, tenant="acme-gone")

        orphan_count, missing_count = await record_quota_config_drift(
            db_session,
            known_license_ids={active_id, revoked_id},  # absent_id: licences has no record
            active_license_ids={active_id},  # revoked_id: known, but not active
        )

        assert orphan_count == 2
        assert missing_count == 0
        assert _gauge(observability_metrics.quota_config_orphan_total, reason="absent") == 1
        assert _gauge(observability_metrics.quota_config_orphan_total, reason="non_active") == 1
        assert _gauge(observability_metrics.quota_config_missing_total) == 0

    @pytest.mark.unit
    async def test_a_clean_mirror_reports_zero_orphans(self, db_session):
        """The birth case with real rows in play: a mirror that fully agrees
        with licences must report exactly 0, not merely "no series"."""
        clean_id = uuid.uuid4()
        await _seed(db_session, clean_id, tenant="acme-clean")

        orphan_count, missing_count = await record_quota_config_drift(
            db_session,
            known_license_ids={clean_id},
            active_license_ids={clean_id},
        )

        assert orphan_count == 0
        assert missing_count == 0
        assert _gauge(observability_metrics.quota_config_orphan_total, reason="absent") == 0
        assert _gauge(observability_metrics.quota_config_orphan_total, reason="non_active") == 0
        assert _gauge(observability_metrics.quota_config_missing_total) == 0


class TestMissingDetection:
    @pytest.mark.unit
    async def test_an_active_licence_with_no_mirror_row_is_reported_as_missing(self, db_session):
        present_id, missing_id = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, present_id, tenant="acme-present")
        # missing_id deliberately has NO request_quota_config row.

        orphan_count, missing_count = await record_quota_config_drift(
            db_session,
            known_license_ids={present_id, missing_id},
            active_license_ids={present_id, missing_id},
        )

        assert orphan_count == 0
        assert missing_count == 1
        assert _gauge(observability_metrics.quota_config_missing_total) == 1


class TestSyncSuccessTimestamp:
    @pytest.mark.unit
    def test_recording_success_sets_a_recent_unix_timestamp(self):
        """Exposed as a raw timestamp, not a ticking age -- see the gauge's
        HELP text for why: nothing in this operator-triggered endpoint ticks
        a clock between invocations, and adding a loop to do so would be the
        scheduler this instrumentation is explicitly forbidden from adding.
        The alerting rule computes the age itself via
        ``time() - billing_quota_config_sync_last_success_timestamp_seconds``.
        """
        import time

        before = time.time()
        record_quota_config_sync_success()
        after = _gauge(observability_metrics.quota_config_sync_last_success_timestamp_seconds)

        assert before <= after <= time.time()
