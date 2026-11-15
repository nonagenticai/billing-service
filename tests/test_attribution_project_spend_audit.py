"""The two project-axis audits that had no instrument, against real Postgres.

Measured on prod 2026-09-16 over the audit's own 24h window:

* **62 of 414 attributed rows carried a ``project_id`` byte-identical to that
  row's own ``tenant_id``** -- $6.0458 of $9.7622. 15.0% of the rows, **61.9% of
  the money**. Nothing anywhere detected this class, and both pre-existing
  project gauges scored it as HEALTH: it is one project under exactly one
  tenant, so ``billing_project_id_multi_tenant_projects`` reads 0, and it is a
  non-sentinel uuid, so ``billing_cost_daily_project_attribution_rows`` counts
  it as COVERAGE.
* **there was no dollar-denominated coverage number at all.** Both existing
  project gauges are ROW COUNTS, and a row share is not a spend share -- this
  estate has already seen 80% of rows be 0.5% of the bill, and the line above is
  the same trap pointing the other way.

⛔ WHY THIS FILE IS DATABASE-BACKED AND ``test_attribution_audit.py`` IS NOT.
Both additions ARE SQL predicates. ``project_id = tenant_id`` is a uuid
comparison Postgres performs, ``IS DISTINCT FROM`` is Postgres' three-valued
logic, ``FILTER`` is Postgres' aggregate filter, and ``excluded_from_billing``
is a WHERE clause. Asserting any of them against the stub session in
``test_attribution_audit.py`` would assert only that the stub was written
correctly -- the exact vacuity that file's own comments warn about. That file
keeps what a stub CAN grade: which number reaches which gauge. This file grades
the SQL.

⭐ THE PROPERTY WORTH PINNING for the coverage split is that the three buckets
PARTITION the window -- their sum equals the window's billable total, computed
here by an INDEPENDENT query rather than by re-adding the same three FILTERs.
A bucket that silently drops rows is the failure mode, and it is invisible to
any assertion that only checks each bucket's own value.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text

from services import attribution_audit as aa

# The same column list `test_attribution_unattributed_measurement.py` uses, plus
# the two columns these audits are ABOUT. `metadata` is a `json` column (not
# jsonb), so the bind is CAST explicitly rather than relying on a driver to
# guess a dict's target type.
_INSERT = text(
    "INSERT INTO billing_cost_events"
    " (id, tenant_id, tenant_name, license_id, project_id, service_key, provider,"
    "  input_tokens, output_tokens, cost_usd,"
    "  excluded_from_billing, exclusion_reason, metadata, created_at)"
    " VALUES (:id, :tenant_id, 'acme', NULL, :project_id, 'inference-router', 'pgx-local',"
    "  100, 50, :cost_usd, :excluded, :reason, CAST(:meta AS json), :created_at)"
)

#: Independent of the three FILTERs under test. If this were expressed as
#: `project_usd + tenant_scoped_usd + unattributed_usd` the partition assertion
#: would be a tautology -- it would hold for any set of predicates, including a
#: set that drops rows, because both sides would drop the same ones.
_BILLABLE_TOTAL_SQL = text(
    "SELECT coalesce(sum(cost_usd), 0)::float8 AS total"
    " FROM billing_cost_events"
    " WHERE created_at >= :floor AND excluded_from_billing = false"
)


async def _insert(
    session,
    *,
    tenant: str,
    cost: str,
    project: str | None = None,
    meta: dict | None = None,
    excluded: bool = False,
) -> None:
    await session.execute(
        _INSERT,
        {
            "id": uuid.uuid4(),
            "tenant_id": tenant,
            "project_id": project,
            "cost_usd": Decimal(cost),
            "excluded": excluded,
            "reason": "router_duplicate" if excluded else None,
            "meta": json.dumps(meta) if meta is not None else None,
            "created_at": datetime.now(UTC) - timedelta(hours=1),
        },
    )


async def _load(session):
    """`_load`'s project arms, named.

    `_load` returns an unnamed positional tuple; unpacking it here rather than
    indexing means a future insertion in the middle fails loudly at the unpack
    instead of silently re-pointing an assertion at a neighbouring number.
    """
    (
        _rows,
        _local_owners,
        _budgets,
        _unattributed,
        _unregistered,
        _derived,
        _multi_tenant,
        _attribution,
        self_tenant,
        coverage,
    ) = await aa._load(session, 24)
    return self_tenant, coverage


# ---------------------------------------------------------------------------
# Addition 1 -- the forged-id detector.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_forged_project_id_is_counted_with_its_dollars(db_session) -> None:
    """project_id == tenant_id is counted; a genuine project id is not.

    Costs are powers of two so that the summed dollars NAME which rows were
    counted. A plain "spend > 0" would pass for any non-empty subset, and the
    whole finding here is about WHICH dollars, not whether there are any.
    """
    forger = str(uuid.uuid4())
    other_forger = str(uuid.uuid4())
    honest = str(uuid.uuid4())

    # The defect: three rows stamping the tenant's own uuid on the project axis.
    for cost in ("1.00", "2.00", "4.00"):
        await _insert(db_session, tenant=forger, project=forger, cost=cost)
    # A SECOND tenant doing the same thing -- this is what makes the distinct
    # PROJECT count differ from the ROW count. One emitter with one bug and a
    # whole population doing it are the same row count and different defects.
    await _insert(db_session, tenant=other_forger, project=other_forger, cost="8.00")

    # Genuine attribution: a real project uuid that is NOT the tenant's.
    await _insert(db_session, tenant=honest, project=str(uuid.uuid4()), cost="16.00")
    # No project at all. Not a forgery either -- an absent project is honest.
    await _insert(db_session, tenant=honest, project=None, cost="32.00")

    await db_session.commit()

    (projects, rows, spend), _coverage = await _load(db_session)

    # Two distinct forged ids across four rows, and $15.00 == 1+2+4+8 exactly.
    # Any of the three honest rows leaking in moves the sum to 31 / 47 / 63,
    # which no rounding can hide.
    assert projects == 2
    assert rows == 4
    assert spend == pytest.approx(15.00)


@pytest.mark.asyncio
async def test_a_parked_forged_row_is_still_counted(db_session) -> None:
    """⚠️ The deliberate ABSENCE of the `excluded_from_billing` filter.

    This is the one assertion that would flip if someone "fixed" the query by
    making it consistent with the money queries around it. It must not be
    relaxed: filtering would let a forger hide behind the dedupe, because a
    parked row is the same evidence of caller behaviour as a billable one.
    """
    forger = str(uuid.uuid4())

    await _insert(db_session, tenant=forger, project=forger, cost="1.00")
    # Same forgery, parked as a router duplicate. $64 so its presence or absence
    # is unmistakable in the sum.
    await _insert(db_session, tenant=forger, project=forger, cost="64.00", excluded=True)
    await db_session.commit()

    (projects, rows, spend), _coverage = await _load(db_session)

    assert projects == 1
    assert rows == 2, "a parked row is still evidence of what the caller sent"
    assert spend == pytest.approx(65.00)


@pytest.mark.asyncio
async def test_a_clean_window_reads_zero_rather_than_nothing(db_session) -> None:
    """A measured zero, on a window that is NOT empty.

    The population is deliberately non-empty and honestly attributed, so this
    zero distinguishes "nobody forged anything" from "the query matched nothing
    because there was nothing to match" -- which an empty fixture could not.
    """
    tenant = str(uuid.uuid4())
    await _insert(db_session, tenant=tenant, project=str(uuid.uuid4()), cost="5.00")
    await _insert(db_session, tenant=tenant, project=None, cost="7.00")
    await db_session.commit()

    (projects, rows, spend), _coverage = await _load(db_session)

    assert (projects, rows) == (0, 0)
    assert spend == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Addition 2 -- dollar-denominated coverage.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_three_buckets_partition_the_window_exactly(db_session) -> None:
    """The property, not the three values: the buckets sum to the window total.

    A bucket that silently drops rows is the failure mode -- and it is invisible
    to any assertion that only checks each bucket against a number the test
    itself computed, because the test would drop the same rows. The total comes
    from `_BILLABLE_TOTAL_SQL`, which shares no predicate with the three FILTERs.

    The fixture covers every way a row can land in `unattributed`, because they
    are three different SQL paths: metadata NULL, metadata present with the key
    absent, and metadata present with the key set to something else. `->>`
    yields NULL for the first two, so a `<> 'tenant'` spelling would drop both
    out of ALL THREE buckets and break the partition without changing any single
    bucket's plausibility.
    """
    tenant = str(uuid.uuid4())
    floor = datetime.now(UTC) - timedelta(hours=24)

    await _insert(db_session, tenant=tenant, project=str(uuid.uuid4()), cost="1.00")
    await _insert(
        db_session, tenant=tenant, project=None, meta={"spend_scope": "tenant"}, cost="2.00"
    )
    # unattributed, three ways.
    await _insert(db_session, tenant=tenant, project=None, meta=None, cost="4.00")
    await _insert(db_session, tenant=tenant, project=None, meta={"job_id": "x"}, cost="8.00")
    await _insert(
        db_session, tenant=tenant, project=None, meta={"spend_scope": "project"}, cost="16.00"
    )
    # Parked. In NO bucket, and absent from the independent total as well -- so
    # the partition identity holds only if BOTH sides exclude it.
    await _insert(db_session, tenant=tenant, project=None, meta=None, cost="99.00", excluded=True)

    await db_session.commit()

    _self_tenant, (project_usd, tenant_scoped_usd, unattributed_usd) = await _load(db_session)
    total = float((await db_session.execute(_BILLABLE_TOTAL_SQL, {"floor": floor})).one().total)

    # THE PROPERTY.
    assert project_usd + tenant_scoped_usd + unattributed_usd == pytest.approx(total)
    # And the parked $99.00 is in neither side of it.
    assert total == pytest.approx(31.00)

    # The individual buckets, as powers of two, so a misplaced row names itself.
    assert project_usd == pytest.approx(1.00)
    assert tenant_scoped_usd == pytest.approx(2.00)
    assert unattributed_usd == pytest.approx(28.00)


@pytest.mark.asyncio
async def test_tenant_scoped_reads_zero_when_no_row_carries_the_header(db_session) -> None:
    """The `spend_scope` header is being added by another change set this round.

    Until it lands the bucket legitimately reads $0, and that zero must be a
    MEASUREMENT rather than a dead code path. The other two buckets are non-zero
    in the same window precisely so this assertion cannot pass because the query
    returned nothing.
    """
    tenant = str(uuid.uuid4())

    await _insert(db_session, tenant=tenant, project=str(uuid.uuid4()), cost="3.00")
    await _insert(db_session, tenant=tenant, project=None, meta=None, cost="5.00")
    await db_session.commit()

    _self_tenant, (project_usd, tenant_scoped_usd, unattributed_usd) = await _load(db_session)

    assert tenant_scoped_usd == pytest.approx(0.0)
    # The vacuity floor: the query DID run and DID see this window.
    assert project_usd == pytest.approx(3.00)
    assert unattributed_usd == pytest.approx(5.00)


@pytest.mark.asyncio
async def test_rows_older_than_the_window_are_in_no_bucket(db_session) -> None:
    """Both additions are LEVELS over the window, never lifetime totals.

    Without this, either could be a lifetime total wearing a 24h label and could
    never fall when an emitter was fixed -- the exact reason both are Gauges.
    """
    tenant = str(uuid.uuid4())

    await db_session.execute(
        _INSERT,
        {
            "id": uuid.uuid4(),
            "tenant_id": tenant,
            "project_id": tenant,  # forged, and OLD.
            "cost_usd": Decimal("50.00"),
            "excluded": False,
            "reason": None,
            "meta": None,
            "created_at": datetime.now(UTC) - timedelta(hours=30),
        },
    )
    await _insert(db_session, tenant=tenant, project=tenant, cost="1.00")
    await db_session.commit()

    (projects, rows, spend), (project_usd, _tenant_scoped, unattributed_usd) = await _load(
        db_session
    )

    assert (projects, rows) == (1, 1)
    assert spend == pytest.approx(1.00)
    # The coverage split is bounded by the same window.
    assert project_usd == pytest.approx(1.00)
    assert unattributed_usd == pytest.approx(0.0)
