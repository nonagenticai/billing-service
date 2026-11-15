"""The project axis must survive the daily rollup, not die at the sentinel.

THE DEFECT THESE TESTS PIN
--------------------------
``billing_cost_events.project_id`` is populated by the flusher (alembic 0036) and
``billing_cost_daily.project_id`` exists, is ``NOT NULL``, and is the 11th member
of ``uq_cost_daily``. Between the two, ``services/rollup_engine.py`` did not
mention ``project_id`` at all: it was absent from the cost aggregate's
``GROUP BY`` and absent from the ``cost_daily_rows`` values dict, and
``rollup_engine.py:876`` is the ONLY writer to ``CostDaily`` in the repo
(``services/reprice.py`` does not write the table — it re-invokes
``rollup_daily``, so it inherits whatever this aggregate does).

⛔ THE FAILURE MODE WAS SILENT AND TOTAL. A column that is never named in an
INSERT takes its ``server_default``, so **every** row that would ever exist
carried ``NULL_PROJECT_ID``. Nothing errors, nothing is NULL, no count is wrong,
and every money total is correct — the axis is simply constant. That makes
``ix_cost_daily_tenant_project_date`` an index over one value, the ``by_project``
breakdown a single bucket, the widened ``uq_cost_daily`` a no-op that splits
nothing, and the attribution gauge a permanent 0% attributed. It is the
``metadata.job_id`` defect with the sign flipped: there a sentinel was read as
real, here a real value is written as the sentinel.

⭐ WHY A TEST AND NOT AN EYEBALL. The fix is four short lines and every one of
them looks right on its own. What cannot be checked by reading is whether the
value that reaches the STORED ROW is the event's project, and whether the GRAIN
actually splits — and those are the only two things that matter.

WHAT EACH TEST PINS, AND THE MUTATION THAT KILLS IT
---------------------------------------------------
``test_a_real_project_id_reaches_the_daily_row`` is the direct statement of the
defect: one event carrying a real project uuid must produce a daily row carrying
THAT uuid. Pre-fix it stores ``NULL_PROJECT_ID``.

``test_a_null_project_id_lands_on_the_sentinel`` is the CONTROL, and it is the
test that must NOT move. ``CostEvent.project_id`` is nullable while
``CostDaily.project_id`` is ``NOT NULL``; the coalesce is what bridges them. This
test passes both before and after the fix (pre-fix by accident, via the
``server_default``; post-fix by the ``func.coalesce`` in the aggregate) — which
is exactly why it cannot be the only test, and why it is kept: it is what stops
the fix from being "write the raw column and make it nullable".

``test_two_projects_under_one_tenant_produce_two_rows`` pins the GRAIN, which is
the half a values-dict-only fix would miss. Two events identical in every other
``uq_cost_daily`` dimension, differing only in ``project_id``, must produce TWO
rows whose costs are NOT summed together. Without ``_cproj`` in the ``GROUP BY``
the aggregate emits one group; with ``project_id`` then written from that single
group the two projects' spend is merged and unattributable.

⛔ MUTATION CONTROL — RUN, NOT ASSERTED. Reverting the fix in
``rollup_engine.py`` (drop ``_cproj`` from the ``GROUP BY`` and from the select
list, and drop the ``"project_id"`` key from ``cost_daily_rows``, i.e. exactly
the pre-fix file) must make ``test_a_real_project_id_reaches_the_daily_row`` and
``test_two_projects_under_one_tenant_produce_two_rows`` FAIL, while
``test_a_null_project_id_lands_on_the_sentinel`` still PASSES. The two halves are
therefore separable and neither is carrying the other. Measured both directions —
see the PR body.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select, text

from models.db import CostDaily
from models.db._common import NULL_PROJECT_ID
from services.rollup_engine import rollup_daily
from tests.conftest import T

# One complete day in the past, which is what `rollup_daily` is ever asked for
# (`catchup_daily`/`missing_daily_dates` only ever hand it complete days).
DAY = (datetime.now(UTC) - timedelta(days=1)).date()
AT = datetime.combine(DAY, datetime.min.time(), tzinfo=UTC) + timedelta(hours=12)


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

    ⚠️ RAW SQL, deliberately — the same reasoning as
    ``tests/test_rollup_unpriced.py::_insert_cost_events`` and
    ``tests/test_reprice.py::_seed``. Going through ``flush_costs`` would make the
    stored cost depend on whatever ``provider_pricing`` rows happen to exist in
    the test database, so a peer lane seeding a rate could change this file's
    population underneath it. Stating every column explicitly makes each test's
    population its own, and lets the NULL case actually BE null.

    ``license_id`` / ``subject_id`` are parameters rather than fresh uuids per
    call because the grain test needs two events that are identical in every
    ``uq_cost_daily`` dimension EXCEPT ``project_id`` — if they differed on
    subject too, two rows would prove nothing about the project axis.
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


@pytest.mark.unit
async def test_a_real_project_id_reaches_the_daily_row(db_session_factory):
    """⛔ THE KILLING TEST — the event's project must BE the row's project.

    Asserted as an identity against the uuid this test minted, and explicitly
    ``!= NULL_PROJECT_ID`` beside it. The second assertion is not redundant: if
    the sentinel were ever changed to something that happened to equal a test
    uuid the first would still pass, and more usefully it names the exact failure
    the pre-fix code produces, so the diff a future reader sees is the defect.
    """
    tenant = T("project-axis-real")
    project = uuid.uuid4()
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=project,
        cost="0.250000",
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
    )

    await rollup_daily(db_session_factory, DAY)

    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert len(rows) == 1, f"expected exactly one cost_daily group, got {len(rows)}"
    assert rows[0].project_id != NULL_PROJECT_ID, (
        "the rollup stored the 'no project asserted' sentinel for an event that "
        "DID assert a project — the project axis never reached the rollup"
    )
    assert rows[0].project_id == project
    assert rows[0].total_cost_usd == Decimal("0.250000")


@pytest.mark.unit
async def test_a_null_project_id_lands_on_the_sentinel(db_session_factory):
    """THE CONTROL: a project-less event must store the sentinel, never NULL.

    ``CostEvent.project_id`` is NULLABLE and ``CostDaily.project_id`` is NOT NULL
    and part of ``uq_cost_daily``, where Postgres treats NULL != NULL — a NULL
    reaching that key would never match itself and each rollup cycle would INSERT
    a duplicate instead of restating the row. This test is what stops the fix
    from being "just write the raw column through".

    ⚠️ It passes on the PRE-FIX code too, by accident, via the column's
    ``server_default``. That is stated rather than hidden: it is the false-
    positive control for the other two tests, not evidence of the fix.
    """
    tenant = T("project-axis-null")
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=None,
        cost="0.100000",
        license_id=uuid.uuid4(),
        subject_id=uuid.uuid4(),
    )

    await rollup_daily(db_session_factory, DAY)

    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert len(rows) == 1
    assert rows[0].project_id is not None, "NOT NULL column; a NULL here is a schema breach"
    assert rows[0].project_id == NULL_PROJECT_ID
    assert rows[0].total_cost_usd == Decimal("0.100000")


@pytest.mark.unit
async def test_two_projects_under_one_tenant_produce_two_rows(db_session_factory):
    """⛔ THE GRAIN SPLIT — the half a values-dict-only fix would miss.

    Two events identical in EVERY other ``uq_cost_daily`` dimension (same tenant,
    licence, subject, service_key, provider, served_model, byo, owner_provided,
    cost_source, day), differing ONLY in ``project_id``. Without ``_cproj`` in the
    aggregate's ``GROUP BY`` these fold into one group and their spend is summed
    into a single unattributable row.

    The costs are deliberately DIFFERENT and distinguishable, and the merged
    total (0.750000) is asserted NOT to appear, so the test fails loudly on the
    merge rather than only on the row count.
    """
    tenant = T("project-axis-grain")
    lic = uuid.uuid4()
    sub = uuid.uuid4()
    project_a = uuid.uuid4()
    project_b = uuid.uuid4()
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=project_a,
        cost="0.250000",
        license_id=lic,
        subject_id=sub,
    )
    await _insert_cost_event(
        db_session_factory,
        tenant,
        project_id=project_b,
        cost="0.500000",
        license_id=lic,
        subject_id=sub,
    )

    await rollup_daily(db_session_factory, DAY)

    rows = await _cost_daily_rows(db_session_factory, tenant)
    assert len(rows) == 2, (
        "two projects under one tenant must produce two daily rows; got "
        f"{len(rows)} — the aggregate folded the project axis away and merged "
        "two projects' spend into one unattributable row"
    )
    by_project = {r.project_id: r for r in rows}
    assert set(by_project) == {project_a, project_b}
    assert by_project[project_a].total_cost_usd == Decimal("0.250000")
    assert by_project[project_b].total_cost_usd == Decimal("0.500000")
    assert Decimal("0.750000") not in {r.total_cost_usd for r in rows}, (
        "the two projects' spend was summed into one row — the grain, not just "
        "the stored value, is what the GROUP BY term protects"
    )
    # Each row still carries its own half of the usage, not the tenant's total.
    assert all(r.request_count == 1 for r in rows)
    assert all(r.total_input_tokens == 100 for r in rows)
