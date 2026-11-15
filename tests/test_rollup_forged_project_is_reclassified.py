"""A `project_id` equal to its own `tenant_id` must not reach the rollup as a project.

THE DEFECT THESE TESTS PIN
--------------------------
62 rows in production's `billing_cost_events` carry a `project_id` that is
byte-identical to their own `tenant_id` — a project id forged from the tenant's
own uuid, naming a project that exists in no registry. A guard in
`requirements_management` now refuses new ones, but it is FORWARD-ONLY: the 62
events are already in the ledger, and `rollup_daily` folded them into
`billing_cost_daily` as though the tenant uuid were a project (measured: 3 daily
rows, $5.994795, `bucket_date = 2026-09-15`). From there they are visible in
every `/api/costs/*` per-project breakdown, unbounded in time —
`routers/_aggregation.py::aggregate_by_project` applies no licence clause and
has four call sites.

⛔ WHY THE ROLLUP AND NOT A DELETE. `rollup_daily(target_date)` guarded-DELETEs a
`bucket_date` and rebuilds it from `cost_events`, over a rolling
`daily_reconcile_days` window. Hand-deleting the three daily rows would not hold:
the next reconcile rebuilds the bucket from the 62 UNCHANGED source events and
re-mints them. The rule has to live in the aggregate, where it also makes the
repair self-applying — every subsequent reconcile of an affected bucket rewrites
those rows onto the sentinel with no operator action.

⭐ RECLASSIFICATION, NOT A COVER-UP. `project_id == tenant_id` asserts no project
by definition, so `NULL_PROJECT_ID` ("spend no project was asserted for") is the
CORRECT value, reached by the same branch the genuinely-absent case takes. And
detection is preserved where it is actually measured:
`attribution_audit._PROJECT_SELF_TENANT_SQL` reads the RAW `billing_cost_events`
ledger, which this change does not touch —
`test_the_raw_ledger_still_shows_the_forgery_after_the_rollup` is that claim,
asserted rather than asserted-in-prose.

⛔ RE-KEYED, NEVER DROPPED — AND THAT IS THE LOAD-BEARING PROPERTY.
Filtering forged events out of the aggregate would make `billing_cost_daily`
stop summing to `billing_cost_events`, which is a worse defect than the one
being fixed: a rollup that does not reconcile with its own ledger is unusable
for every reader, not just the project axis. Two tests exist only to state that
the DOLLARS are unchanged and only the BUCKET moves —
`..._with_its_dollars_intact` and `..._merges_into_the_tenants_project_less_row`.
A drop passes neither.

WHAT EACH TEST PINS, AND THE MUTATION THAT KILLS IT
---------------------------------------------------
Reverting `_cproj` in `services/rollup_engine.py` to the plain
`func.coalesce(CostEvent.project_id, NULL_PROJECT_ID)` must turn these RED:

  * `..._rolls_up_under_the_sentinel_with_its_dollars_intact` — stores the
    tenant uuid on the project axis instead of the sentinel.
  * `..._merges_into_the_tenants_project_less_row` — two rows instead of one.
  * `..._a_genuine_project_id_is_untouched_beside_a_forged_one` — the forged
    half lands under the tenant uuid.
  * `..._the_raw_ledger_still_shows_the_forgery_after_the_rollup` — its daily
    half only; its LEDGER half must stay green in both directions, which is what
    proves the detector is independent of the rollup rather than riding on it.

and must leave `..._a_row_already_carrying_the_sentinel_is_unaffected` GREEN.
That test is the false-positive control: it passes before and after, so it
cannot be carrying any of the others. Measured both directions — see the PR body.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select, text

from models.db import CostDaily
from models.db._common import NULL_PROJECT_ID
from services.attribution_audit import _PROJECT_SELF_TENANT_SQL
from services.rollup_engine import rollup_daily
from tenancy import tenant_level_project_id
from tests.conftest import T

# One complete day in the past — the only shape `rollup_daily` is ever handed
# (`catchup_daily` / `missing_daily_dates` only ever pass complete days).
DAY = (datetime.now(UTC) - timedelta(days=1)).date()
AT = datetime.combine(DAY, datetime.min.time(), tzinfo=UTC) + timedelta(hours=12)
# Floor for the raw-ledger detector, comfortably below AT so the seeded events
# are inside its window. Expressed relative to AT rather than to "now" so a run
# that straddles midnight cannot drop the population under the floor.
LEDGER_FLOOR = AT - timedelta(days=1)


async def _insert_cost_event(
    session_factory,
    tenant: str,
    *,
    project_id: uuid.UUID | None,
    cost: str,
    license_id: uuid.UUID,
    subject_id: uuid.UUID,
) -> None:
    """Insert ONE cost event, honouring an explicit NULL ``project_id``.

    ⚠️ RAW SQL, deliberately — the same reasoning as the sibling file
    ``tests/test_rollup_project_axis.py``. Going through ``flush_costs`` would
    make the stored cost depend on whatever ``provider_pricing`` rows happen to
    exist in the test database, so a peer lane seeding a rate could change this
    file's population underneath it. Stating every column explicitly makes each
    test's population its own, and lets the NULL case actually BE null.

    ``license_id`` / ``subject_id`` are parameters because the merge tests need
    events identical in every ``uq_cost_daily`` dimension EXCEPT ``project_id``
    — if they differed on subject too, the row count would prove nothing about
    the project axis.
    """
    async with session_factory() as session:
        await session.execute(
            text(
                "INSERT INTO billing_cost_events "
                "(id, tenant_id, license_id, subject_id, project_id, service_key, "
                " provider, input_tokens, output_tokens, cost_usd, created_at) "
                "VALUES (:id, :t, :l, :s, "
                + ("NULL" if project_id is None else "CAST(:proj AS uuid)")
                + ", :sk, :p, 100, 50, CAST(:c AS numeric), :ts)"
            ),
            {
                "id": uuid.uuid4(),
                "t": tenant,
                "l": license_id,
                "s": subject_id,
                "sk": "inference-router",
                "p": "claude",
                "c": cost,
                "ts": AT,
                **({} if project_id is None else {"proj": project_id}),
            },
        )
        await session.commit()


async def _cost_daily_rows(session_factory, tenant: str) -> list[CostDaily]:
    async with session_factory() as session:
        return list(
            (
                await session.execute(
                    select(CostDaily)
                    .where(CostDaily.tenant_id == tenant)
                    .order_by(CostDaily.total_cost_usd)
                )
            )
            .scalars()
            .all()
        )


async def _ledger_forgeries(session_factory) -> tuple[int, int, float]:
    """Run the SHIPPED raw-event detector, imported, not re-typed here.

    ``_PROJECT_SELF_TENANT_SQL`` is referenced as the module object rather than
    copied into this file so that an edit to the detector is followed by this
    test instead of silently diverging from it. That matters more than usual
    here: this assertion is the entire evidence that the rollup change is a
    reclassification and not a disappearance.
    """
    async with session_factory() as session:
        row = (await session.execute(_PROJECT_SELF_TENANT_SQL, {"floor": LEDGER_FLOOR})).one()
    return int(row.projects or 0), int(row.events or 0), float(row.spend or 0.0)


@pytest.mark.unit
async def test_a_forged_project_id_rolls_up_under_the_sentinel_with_its_dollars_intact(
    db_session_factory,
):
    """⛔ THE KILLING TEST — the forged id is re-keyed, and the money survives.

    Both halves are asserted because either alone is satisfied by a bug: a
    sentinel with no row would pass a "not the tenant uuid" check, and a correct
    dollar total would pass on the pre-fix code that stores the forgery.
    """
    tenant = T("forged-project-sentinel")
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=uuid.UUID(tenant),  # THE FORGERY: the tenant's own uuid.
        cost="0.250000",
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
    )

    await rollup_daily(db_session_factory, DAY)

    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert len(rows) == 1, (
        f"the forged event must still produce a daily row, got {len(rows)} — a "
        "count of 0 means it was DROPPED from the aggregate, which breaks the "
        "rollup's reconciliation with its own ledger"
    )
    assert rows[0].project_id != uuid.UUID(tenant), (
        "the rollup stored the tenant's own uuid on the project axis — a forged "
        "project id reached billing_cost_daily and is now visible in every "
        "/api/costs/* per-project breakdown"
    )
    assert rows[0].project_id == NULL_PROJECT_ID
    # THE RE-KEYING PROPERTY: the dollars are unchanged, only the bucket moved.
    assert rows[0].total_cost_usd == Decimal("0.250000")
    assert rows[0].request_count == 1
    assert rows[0].total_input_tokens == 100


@pytest.mark.unit
async def test_a_forged_row_merges_into_the_tenants_project_less_row(db_session_factory):
    """⭐ THE SUM IS THE PROOF — re-keyed, not removed.

    A forged event and a genuinely project-less event, identical in every other
    ``uq_cost_daily`` dimension, must land in ONE row whose total is the SUM.
    A rollup that instead FILTERED the forgery out would emit one row of
    0.500000 here and pass every "the project axis is clean" assertion while
    quietly making billing_cost_daily stop summing to billing_cost_events.
    0.750000 is the only value that distinguishes the two.
    """
    tenant = T("forged-project-merge")
    lic = uuid.uuid4()
    sub = uuid.uuid4()
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=uuid.UUID(tenant),
        cost="0.250000",
        license_id=lic,
        subject_id=sub,
    )
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=None,
        cost="0.500000",
        license_id=lic,
        subject_id=sub,
    )

    await rollup_daily(db_session_factory, DAY)

    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert len(rows) == 1, (
        "forged and absent are the same state — 'no project was asserted' — so "
        f"they must fold into one group; got {len(rows)} rows, which means the "
        "tenant uuid is still acting as a distinct project key"
    )
    assert rows[0].project_id == NULL_PROJECT_ID
    assert rows[0].total_cost_usd == Decimal("0.750000"), (
        "the forged event's dollars did not reach the daily row — it was "
        "dropped from the aggregate rather than re-keyed, and the rollup no "
        "longer sums to the ledger"
    )
    assert rows[0].request_count == 2


@pytest.mark.unit
async def test_a_genuine_project_id_is_untouched_beside_a_forged_one(db_session_factory):
    """The rule is SELECTIVE — it re-keys the forgery and nothing else.

    Both events sit under ONE tenant so the two outcomes are produced by a
    single aggregate over a single population. A blanket "send every project to
    the sentinel" would collapse these into one row and is caught here; a
    do-nothing leaves the forged half under the tenant uuid and is caught here
    too. The costs differ so the merged total (0.375000) cannot appear silently.
    """
    tenant = T("forged-project-selective")
    lic = uuid.uuid4()
    sub = uuid.uuid4()
    genuine = uuid.uuid4()
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=genuine,
        cost="0.125000",
        license_id=lic,
        subject_id=sub,
    )
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=uuid.UUID(tenant),
        cost="0.250000",
        license_id=lic,
        subject_id=sub,
    )

    await rollup_daily(db_session_factory, DAY)

    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert len(rows) == 2, f"expected the genuine project and the sentinel, got {len(rows)} rows"
    by_project = {r.project_id: r for r in rows}
    assert set(by_project) == {genuine, NULL_PROJECT_ID}, (
        "the project axis must carry the genuine project under ITSELF and the "
        f"forged one under the sentinel; got {sorted(str(k) for k in by_project)}"
    )
    assert by_project[genuine].total_cost_usd == Decimal("0.125000")
    assert by_project[NULL_PROJECT_ID].total_cost_usd == Decimal("0.250000")
    assert Decimal("0.375000") not in {r.total_cost_usd for r in rows}


@pytest.mark.unit
async def test_a_tenant_level_project_rolls_up_under_itself(db_session_factory):
    """Owner ruling 2026-10-07: `tenant_level_project_id(tenant)` is a real
    project id, NOT the forged `project_id == tenant_id` shape, so the
    rollup keeps it on the project axis instead of the no-project sentinel."""
    tenant = T("tenant-level-project-rollup")
    tl = uuid.UUID(tenant_level_project_id(tenant) or "")
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=tl,
        cost="0.125000",
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
    )

    await rollup_daily(db_session_factory, DAY)

    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert [r.project_id for r in rows] == [tl]
    assert rows[0].total_cost_usd == Decimal("0.125000")


@pytest.mark.unit
async def test_a_row_already_carrying_the_sentinel_is_unaffected(db_session_factory):
    """THE CONTROL, and it must NOT move.

    An event whose ``project_id`` is already ``NULL_PROJECT_ID`` is neither
    forged nor absent — it is a third, pre-existing state, and the whole point
    of this test is that the new ``nullif`` does not reach it. It passes before
    and after the change, so it can carry none of the other tests; if it ever
    goes red, the re-keying has become a blanket rewrite of the project column.
    """
    tenant = T("forged-project-already-sentinel")
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=NULL_PROJECT_ID,
        cost="0.100000",
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
    )

    await rollup_daily(db_session_factory, DAY)

    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert len(rows) == 1
    assert rows[0].project_id == NULL_PROJECT_ID
    assert rows[0].total_cost_usd == Decimal("0.100000")


@pytest.mark.unit
async def test_the_raw_ledger_still_shows_the_forgery_after_the_rollup(db_session_factory):
    """⭐ THE CLAIM THAT MAKES THIS A RECLASSIFICATION AND NOT A COVER-UP.

    The rollup hides the forged id from the per-project breakdown; the LEDGER
    must still name it. This runs the shipped detector
    (``attribution_audit._PROJECT_SELF_TENANT_SQL``) AFTER the rollup and
    asserts it still counts the event with its dollars — i.e. that the rollup
    change touched no row of ``billing_cost_events``.

    The two halves are asserted in ONE test on purpose: separately, "the daily
    row is the sentinel" and "the ledger still counts it" are each satisfiable
    by a different bug, and it is their conjunction over the SAME population
    that is the property. Costs are distinguishable powers so the ledger sum
    names WHICH rows it counted rather than merely that it counted some.
    """
    tenant = T("forged-project-ledger")
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=uuid.UUID(tenant),
        cost="0.250000",
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
    )
    # An honest project under a DIFFERENT tenant: the detector must not count it,
    # so the sum below names the forged row alone.
    honest_tenant = T("forged-project-ledger-honest")
    await _insert_cost_event(
        db_session_factory,
        honest_tenant,
        project_id=uuid.uuid4(),
        cost="0.500000",
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
    )

    before_projects, before_events, before_spend = await _ledger_forgeries(db_session_factory)
    assert (before_projects, before_events) == (1, 1), (
        "precondition: the seeded population must contain exactly one forgery "
        f"before the rollup runs, got {before_projects} projects / {before_events} events"
    )
    assert before_spend == pytest.approx(0.25)

    await rollup_daily(db_session_factory, DAY)

    # Half one: the contamination stopped at the rollup.
    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert len(rows) == 1
    assert rows[0].project_id == NULL_PROJECT_ID

    # Half two: the ledger is untouched, so the forgery stays detectable.
    after = await _ledger_forgeries(db_session_factory)
    assert after == (before_projects, before_events, before_spend), (
        "the rollup changed what the raw-event detector sees — this change must "
        "write nothing to billing_cost_events, or the forgery has been hidden "
        f"rather than reclassified: {after} != "
        f"{(before_projects, before_events, before_spend)}"
    )
    assert after[1] == 1
    assert after[2] == pytest.approx(0.25)
