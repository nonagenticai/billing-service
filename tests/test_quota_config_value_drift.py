"""The mirrored quota LIMIT can diverge without any id diverging.

THE DEFECT
----------
``request_quota_config`` has TWO writers, both upserting on the ``license_id``
primary key:

1. ``routers/quota.py`` -- ``POST /api/quota/config``, the PROVISIONED limit.
2. ``routers/budget.py`` -- ``POST /api/budget/sync-from-licenses``, the PLAN
   limit taken from the upstream ``plan.quota_monthly``.

``upsert_request_quota_config`` assigned ``quota_monthly`` UNCONDITIONALLY, so
writer 2 landing after writer 1 replaced a provisioned 1,000 with a plan
1,000,000 -- silently, in the same column ``check_quota`` treats as the source of
truth for enforcement.

⛔ WHY THE PREVIOUS AUDIT ROUND MISSED IT
-----------------------------------------
``record_quota_config_drift`` compared ID SETS ONLY: which licences are present
in the mirror vs which the registry returned. A clobbered limit changes no id,
adds no row and removes none, so that detector reported 0 for a table whose
enforced limits had moved by three orders of magnitude. The round's finding --
"164 rows, 0 divergence" -- was a statement about what the instrument LOOKED AT,
not about the data. ``TestIdSetDriftCannotSeeAValueChange`` pins that gap so it
cannot be re-closed by accident and then reported as proof of health.

WHAT FAILS ON UNMODIFIED CODE
-----------------------------
Every test in this module.

* ``services.quota.record_quota_config_value_drift`` and
  ``snapshot_quota_config_values`` do not exist, so the module-level import
  raises ``ImportError`` at COLLECTION and every test in the file errors.
* ``observability.metrics.quota_config_value_drift`` does not exist either, so
  the gauge assertions would raise ``AttributeError`` even with the import
  satisfied.
* ``upsert_request_quota_config`` has no ``set_quota_monthly`` keyword, so
  ``TestARefreshOnlyUpsertDoesNotMoveTheLimit`` raises ``TypeError``; and with
  the keyword accepted but the assignment left unconditional, the refresh-only
  test FAILS on the assertion (1000 became 1_000_000).

⭐ WHY THE COMPARISON IS NOT SELF-GRADING
-----------------------------------------
An earlier round shipped an audit that joined the very table it was auditing, so
it could never disagree. The two sides here come from different databases:

* EXPECTED -- ``plan.quota_monthly`` from the caller's live
  ``GET {licenses_url}/internal/licenses`` fetch (the LICENCES service's
  Postgres, over HTTP).
* MIRRORED -- ``request_quota_config.quota_monthly`` from BILLING's Postgres,
  snapshotted BEFORE this run writes anything.

``record_quota_config_value_drift`` takes no ``AsyncSession``, so it is
structurally incapable of querying its own subject. The ordering half matters
just as much and has its own test: ``sync_from_licenses`` upserts the plan quota
BEFORE it measures drift, so a snapshot taken after the loop equals the expected
values by construction and reports 0 forever
(``TestASnapshotTakenAfterTheUpsertLoopIsSelfGrading``).
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
    record_quota_config_value_drift,
    snapshot_quota_config_values,
    upsert_request_quota_config,
)
from tests.conftest import T

# Read the gauge out of a FRESHLY imported observability.metrics, in a clean
# interpreter that has run no test and mutated nothing -- same rationale as
# tests/test_quota_config_drift.py: the default prometheus registry is
# module-level state shared by the whole pytest session, so "the value at birth"
# cannot be measured in-process without becoming order-dependent on which test
# module happens to collect first.
_COLLECT_AT_BOOT = """
import json
from observability.metrics import quota_config_value_drift
print(json.dumps({"value_drift": quota_config_value_drift._value.get()}))
"""


def _gauge(metric, **labels) -> float:
    child = metric.labels(**labels) if labels else metric
    return float(child._value.get())


async def _seed(
    session,
    license_id: uuid.UUID,
    *,
    tenant: str,
    quota: int,
) -> None:
    """Provision a row the way ``POST /api/quota/config`` does.

    ``set_quota_monthly=True`` because this IS the provisioning writer: it is the
    authority on the limit. Every test that needs a limit to already be stored
    goes through the real helper rather than raw SQL, so a regression in the
    helper's INSERT path cannot hide behind hand-built fixtures.
    """
    await upsert_request_quota_config(
        session,
        license_id,
        T(tenant),
        quota,
        app_id=uuid.uuid4(),
        app_name="KAG Module",
    )
    await session.commit()


class TestTheGaugeIsBornNotMeasured:
    @pytest.mark.unit
    def test_a_fresh_interpreter_exports_the_gauge_at_minus_one(self):
        """-1, NOT 0. 0 is a measurement; this series must start as an absence.

        ``record_quota_config_value_drift`` runs only inside the
        operator-triggered ``POST /api/budget/sync-from-licenses``, so a replica
        that has never served that endpoint has measured nothing. Born at 0 it
        would export the healthy value, and "no licence disagrees" would be
        indistinguishable from "nobody has ever looked" -- which is the exact
        ambiguity the three gauges beside it were added to remove. -1 is outside
        the range a real measurement can produce (a count is >= 0), so an alert
        reads `> 0` for drift and `== -1` for never-measured.
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
        assert born == {"value_drift": -1.0}, f"not born at -1 (never measured): {born}"


class TestValueDriftIsDetected:
    @pytest.mark.unit
    def test_two_configs_agreeing_on_ids_but_not_on_quota_are_reported_as_drift(self):
        """⭐ THE REGRESSION THIS MODULE EXISTS FOR.

        Identical keysets on both sides -- no orphan, no missing, nothing an id
        comparison can see -- and one licence whose stored limit is 1,000 while
        the plan says 1,000,000. That is the production shape: writer 2's plan
        quota landing on writer 1's provisioned quota.
        """
        clobbered, agreed = uuid.uuid4(), uuid.uuid4()

        count, sample = record_quota_config_value_drift(
            mirror_quota_before={clobbered: 1_000, agreed: 500},
            expected_quota_monthly={clobbered: 1_000_000, agreed: 500},
        )

        assert count == 1
        assert _gauge(observability_metrics.quota_config_value_drift) == 1
        # Actionable, not just countable: the licence AND both values, so an
        # operator can tell which way the limit moved without a second query
        # against a table the caller is about to overwrite.
        assert len(sample) == 1
        assert sample[0].license_id == clobbered
        assert sample[0].mirrored == 1_000
        assert sample[0].expected == 1_000_000

    @pytest.mark.unit
    def test_an_agreeing_mirror_reports_zero_not_never_measured(self):
        """0 must be reachable, or `> 0` alerting is vacuous and `== -1` lies."""
        lic = uuid.uuid4()

        count, sample = record_quota_config_value_drift(
            mirror_quota_before={lic: 750},
            expected_quota_monthly={lic: 750},
        )

        assert count == 0
        assert sample == []
        assert _gauge(observability_metrics.quota_config_value_drift) == 0

    @pytest.mark.unit
    def test_a_zero_limit_against_a_positive_plan_is_drift_not_a_falsy_miss(self):
        """A stored 0 is BLOCK ALL, not "unset" -- see check_quota's semantics.

        The same falsy-zero trap that made ``sync_from_licenses`` drop a
        suspended plan's 0 on the floor would make a comparison written as
        ``if mirrored and mirrored != expected`` skip the single most important
        divergence there is: a deliberately blocked licence being handed a live
        limit.
        """
        blocked = uuid.uuid4()

        count, sample = record_quota_config_value_drift(
            mirror_quota_before={blocked: 0},
            expected_quota_monthly={blocked: 5_000},
        )

        assert count == 1
        assert sample[0].mirrored == 0

    @pytest.mark.unit
    def test_the_sample_is_bounded_and_deterministic(self):
        """A bounded sample must be the SAME bounded sample on a re-run.

        Unbounded, a fleet-wide divergence writes one log line per licence;
        unsorted, two consecutive syncs over identical drift report different
        rows and an operator cannot tell a spreading problem from dict order.
        """
        ids = [uuid.uuid4() for _ in range(12)]
        mirror = dict.fromkeys(ids, 1)
        expected = dict.fromkeys(ids, 2)

        count, sample = record_quota_config_value_drift(
            mirror_quota_before=mirror,
            expected_quota_monthly=expected,
            sample_limit=3,
        )

        assert count == 12, "the COUNT must be complete even though the sample is not"
        assert len(sample) == 3
        assert [d.license_id for d in sample] == sorted(ids, key=str)[:3]

    @pytest.mark.unit
    def test_only_the_intersection_is_compared(self):
        """A missing row and an orphan row are already counted by other gauges.

        Comparing values outside the intersection would count one defect twice,
        under two gauges, and make the pair unreadable together.
        """
        both, mirror_only, expected_only = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

        count, _ = record_quota_config_value_drift(
            mirror_quota_before={both: 100, mirror_only: 999},
            expected_quota_monthly={both: 100, expected_only: 999},
        )

        assert count == 0


class TestIdSetDriftCannotSeeAValueChange:
    @pytest.mark.unit
    async def test_the_id_gauges_report_clean_while_the_limit_has_moved(self, db_session):
        """Pins the gap that made "164 rows, 0 divergence" worthless.

        Same licence on both sides, so every set comparison is empty and the two
        pre-existing gauges correctly read 0 -- while the mirrored limit and the
        plan limit are three orders of magnitude apart. The id detector is not
        wrong; it answers a different question, and this test is what stops the
        next round from reading its 0 as a verdict on the values.
        """
        lic = uuid.uuid4()
        await _seed(db_session, lic, tenant="acme-clobbered", quota=1_000)
        mirror_before = await snapshot_quota_config_values(db_session)

        orphan_count, missing_count = await record_quota_config_drift(
            db_session,
            known_license_ids={lic},
            active_license_ids={lic},
        )

        assert orphan_count == 0, "the id detector sees nothing -- that is the point"
        assert missing_count == 0
        # ...and the value comparison, given the same run's two sides, does.
        count, _ = record_quota_config_value_drift(
            mirror_quota_before=mirror_before,
            expected_quota_monthly={lic: 1_000_000},
        )
        assert count == 1

    @pytest.mark.unit
    async def test_the_existing_gauges_keep_their_meaning_when_value_drift_is_measured(
        self, db_session
    ):
        """The new measurement must not repurpose or perturb the old gauges."""
        orphan, lic = uuid.uuid4(), uuid.uuid4()
        await _seed(db_session, lic, tenant="acme-live", quota=10)
        await _seed(db_session, orphan, tenant="acme-gone", quota=10)
        mirror_before = await snapshot_quota_config_values(db_session)

        orphan_count, missing_count = await record_quota_config_drift(
            db_session,
            known_license_ids={lic},
            active_license_ids={lic},
            mirror_quota_before=mirror_before,
            expected_quota_monthly={lic: 20},
        )

        assert orphan_count == 1, "orphan still counts the absent licence, not the limit"
        assert missing_count == 0
        assert _gauge(observability_metrics.quota_config_orphan_total, reason="absent") == 1
        assert _gauge(observability_metrics.quota_config_value_drift) == 1

    @pytest.mark.unit
    async def test_one_mapping_without_the_other_raises_rather_than_measuring_nothing(
        self, db_session
    ):
        """A half-configured detector that reports "no drift" is the failure mode.

        Silently skipping the comparison when only one side is supplied is how a
        caller ships a value-drift gauge that is wired to nothing and reads as
        healthy forever.
        """
        with pytest.raises(ValueError, match="must be supplied"):
            await record_quota_config_drift(
                db_session,
                known_license_ids=set(),
                active_license_ids=set(),
                expected_quota_monthly={uuid.uuid4(): 1},
            )


class TestASnapshotTakenAfterTheUpsertLoopIsSelfGrading:
    @pytest.mark.unit
    async def test_measuring_after_the_sync_writes_reports_zero_by_construction(self, db_session):
        """⛔ WHY ``snapshot_quota_config_values`` MUST BE CALLED BEFORE THE LOOP.

        ``routers.budget.sync_from_licenses`` upserts every licence's plan quota
        and only THEN calls ``record_quota_config_drift``. A detector that read
        the table at measurement time would be comparing rows the same run had
        just written against the values it wrote them from -- 0 divergence,
        unconditionally, forever. That is the "instrument grading its own
        subject" shape, and this test demonstrates it rather than trusting a
        comment not to be refactored away.
        """
        lic = uuid.uuid4()
        await _seed(db_session, lic, tenant="acme-ordering", quota=1_000)
        plan_quota = {lic: 1_000_000}

        before = await snapshot_quota_config_values(db_session)

        # The sync loop: writer 2 asserting the plan limit (the only caller that
        # legitimately passes set_quota_monthly=True besides provisioning).
        await upsert_request_quota_config(
            db_session,
            lic,
            T("acme-ordering"),
            plan_quota[lic],
        )
        await db_session.commit()

        after = await snapshot_quota_config_values(db_session)

        drift_before, _ = record_quota_config_value_drift(
            mirror_quota_before=before, expected_quota_monthly=plan_quota
        )
        drift_after, _ = record_quota_config_value_drift(
            mirror_quota_before=after, expected_quota_monthly=plan_quota
        )

        assert drift_before == 1, "the pre-loop snapshot is the only one that can disagree"
        assert drift_after == 0, "the post-loop snapshot is the table grading itself"


# ⛔ `TestARefreshOnlyUpsertDoesNotMoveTheLimit` WAS DELETED FROM THIS FILE, not
# moved or skipped, and the reason belongs next to the tests that survived it.
#
# It pinned an opt-in `set_quota_monthly` flag that made `upsert_request_quota_config`
# leave an existing `quota_monthly` alone unless a caller asked to set it. That
# looked like drift safety and was the wrong remedy: `quota_monthly` is a REQUIRED
# POSITIONAL argument, so defaulting the flag to False silently turned every
# existing caller into a no-op writer — the provisioning route stopped applying
# the quota it had just been handed.
#
# The full suite refuted it in one test, and that test is the specification:
# `test_license_resolver_revoked.py::TestProvisioningCannotReactivateARevokedLicence
# ::test_a_status_less_upsert_does_not_clobber_a_revoked_row` asserts BOTH that a
# status-less upsert must not re-activate a revoked licence AND that "the quota
# itself must still be updated". Those are deliberately different axes:
# status/app_id/app_name carry the never-clobber-with-None contract because a
# caller can legitimately not know them; the limit is always supplied.
#
# The clobber the flag was aimed at is real — three writers upsert on this primary
# key and the plan-seeded sync can move a provisioned limit. The remedy is the
# rest of this file: DETECT the value change, because the id-set gauges cannot.
