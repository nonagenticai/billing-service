"""An unknown cost must not be billed as $0.00.

THE GAP THESE TESTS PIN
-----------------------
``billing_cost_events.cost_usd`` is NULLABLE and the NULL is load-bearing:
``services/pricing.py`` refuses to price an event it cannot price honestly
("Refuse instead: return None so the caller records a loud pricing_miss") and the
flusher stores that refusal as NULL, because — ``models/db/events.py`` on the
column — "a default of 0 would silently record a pricing miss as 'priced at $0'
and bypass the pricing_miss signal".

``rollup_engine`` then aggregated those rows with ``coalesce(sum(cost_usd), 0)``
into ``billing_cost_daily.total_cost_usd`` (``nullable=False``), and the
distinction died there. ``unpriced_event_count`` (alembic 0031) restores it.

⭐ THE SUM WAS NEVER THE DEFECT, AND THESE TESTS DO NOT CHANGE IT. SQL ``sum()``
skips NULLs, so the stored total always held exactly the spend that IS known.
What was missing was any way to say how much of the group's usage that total does
not describe — and on a group where EVERY row is unpriced the ``coalesce`` makes
that a hard ``0.000000`` beside a real token count, which reads as "free".

⚠️ STATUS: LATENT. Measured 2026-09-11, zero rows in ``billing_cost_events``
carry a NULL ``cost_usd``, so no production total is wrong today. These tests
exist so the first unpriced provider on a paying customer cannot silently reach
an immutable invoice line at $0.00.

WHAT EACH TEST PINS, AND THE MUTATION THAT KILLS IT
---------------------------------------------------
``test_a_genuinely_free_day_reads_zero_unpriced`` is the FALSE-POSITIVE control:
a deliberate $0 rate is a price, not a gap, and must not be counted. It passes on
unmodified code's behaviour (modulo the column existing) and is what stops the
new signal from being implemented as ``total_cost_usd == 0``.

``test_a_wholly_unpriced_day_is_counted_and_not_reported_as_free`` is THE KILLING
TEST. It rolls a free tenant and an entirely-unpriced tenant on the SAME day with
the SAME tokens and asserts their money columns are byte-identical — which is the
old defect, stated as an assertion — while the new counter separates them.

``test_a_partly_unpriced_group_keeps_the_known_total_and_counts_the_rest`` pins
the mixed group: the total must still be the KNOWN cost (the sum is not made
NULL-poisoned, and no row is dropped from ``request_count``), with the unknown
rows counted beside it.

``test_a_fully_priced_day_rolls_up_exactly_as_before`` is the REGRESSION control
in the direction this change could silently go wrong: every ordinary bucket —
which is every bucket in production today — must be unchanged.

⛔ MUTATION CONTROL. The one-line change that makes the two counting tests fail
is to neuter the case expression in ``rollup_engine``'s cost aggregate::

    func.coalesce(func.sum(case((CostEvent.cost_usd.is_(None), 1), else_=0)), 0)
                                                             ^-- 1 -> 0

With that single character changed, ``unpriced_event_count`` is 0 everywhere:
``test_a_wholly_unpriced_day_is_counted_and_not_reported_as_free`` and
``test_a_partly_unpriced_group_keeps_the_known_total_and_counts_the_rest`` both
fail, while the free-day control and the fully-priced regression still pass — so
the two halves are separable and neither is carrying the other. Deleting the
label entirely instead is caught the same way (the column then keeps its
``server_default`` 0).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select, text

from models.db import CostDaily
from services.rollup_engine import rollup_daily
from tests.conftest import T

# One complete day in the past, which is what `rollup_daily` is ever asked for
# (`catchup_daily`/`missing_daily_dates` only ever hand it complete days).
DAY = (datetime.now(UTC) - timedelta(days=1)).date()
AT = datetime.combine(DAY, datetime.min.time(), tzinfo=UTC) + timedelta(hours=12)


async def _insert_cost_events(session_factory, tenant: str, costs: list[str | None]) -> None:
    """Insert cost events in ONE rollup group, honouring an explicit NULL cost.

    ⚠️ RAW SQL, deliberately, and for two independent reasons:

    * ``CostEvent.cost_usd`` carries an ORM ``default=0``, so
      ``session.add(CostEvent(cost_usd=None))`` silently lands a **0** and the
      case under test becomes unreproducible. ``cost_flusher`` writes through
      ``CostEvent.__table__.insert()``, which honours an explicit None — that is
      how the NULL-cost rows in prod came to exist. The same note is on
      ``tests/test_reprice.py::_seed`` and ``tests/test_export.py``.
    * Going through ``flush_costs`` would make the stored cost depend on whatever
      ``provider_pricing`` rows happen to exist in the shared test database, so a
      peer lane seeding a rate for ``claude`` could flip a NULL into a number and
      this file's subject would vanish. Stating the cost explicitly makes each
      test's population its own.

    Every row shares tenant / licence / subject / service_key / provider, so they
    land in exactly one ``uq_cost_daily`` group — otherwise "the total excludes
    the unpriced rows" would be asserted across several rows and prove nothing.
    """
    lic = uuid.uuid4()
    sub = uuid.uuid4()
    async with session_factory() as session:
        for cost in costs:
            await session.execute(
                text(
                    "INSERT INTO billing_cost_events "
                    "(id, tenant_id, license_id, subject_id, service_key, provider, "
                    " input_tokens, output_tokens, cost_usd, created_at) "
                    "VALUES (:id, :t, :l, :s, :sk, :p, 100, 50, "
                    + ("NULL" if cost is None else "CAST(:c AS numeric)")
                    + ", :ts)"
                ),
                {
                    "id": uuid.uuid4(),
                    "t": tenant,
                    "l": lic,
                    "s": sub,
                    "sk": "inference-router",
                    "p": "claude",
                    "ts": AT,
                    **({} if cost is None else {"c": cost}),
                },
            )
        await session.commit()


async def _cost_daily(session_factory, tenant: str) -> CostDaily:
    async with session_factory() as session:
        rows = (
            (await session.execute(select(CostDaily).where(CostDaily.tenant_id == tenant)))
            .scalars()
            .all()
        )
    assert len(rows) == 1, f"expected exactly one cost_daily group, got {len(rows)}"
    return rows[0]


@pytest.mark.unit
async def test_a_genuinely_free_day_reads_zero_unpriced(db_session_factory):
    """A deliberate $0 is a PRICE. It must not be counted as a gap.

    This is the false-positive control. The tempting implementation of the new
    signal is ``total_cost_usd == 0``, which would flag every genuinely free
    service and make the signal useless — the same trap
    ``UnpricedServiceKeyError`` documents for app rates ("a stated 0 is a price,
    a missing row is an unanswered question").
    """
    tenant = T("unpriced-free-day")
    await _insert_cost_events(db_session_factory, tenant, ["0.000000", "0.000000"])

    await rollup_daily(db_session_factory, DAY)

    row = await _cost_daily(db_session_factory, tenant)
    assert row.total_cost_usd == Decimal("0")
    assert row.request_count == 2
    assert row.unpriced_event_count == 0, (
        "priced-at-zero usage is KNOWN to be free; counting it as unpriced would "
        "make the signal indistinguishable from 'total is zero' and unactionable"
    )


@pytest.mark.unit
async def test_a_wholly_unpriced_day_is_counted_and_not_reported_as_free(db_session_factory):
    """⛔ THE KILLING TEST — two tenants whose money columns are IDENTICAL.

    One tenant's usage is priced at $0 on purpose; the other could not be priced
    at all (``cost_usd IS NULL`` — no ``provider_pricing`` row matched). Same
    tokens, same day, same request count. Every column that existed before
    alembic 0031 is the same on both rows, which is the defect expressed as an
    assertion: ``billing_cost_daily`` could not tell "this was free" from "we do
    not know what this cost", and the second one is the row that would put
    $0.000000 on an immutable invoice line.
    """
    free = T("unpriced-twin-free")
    unknown = T("unpriced-twin-unknown")
    await _insert_cost_events(db_session_factory, free, ["0.000000", "0.000000"])
    await _insert_cost_events(db_session_factory, unknown, [None, None])

    await rollup_daily(db_session_factory, DAY)

    free_row = await _cost_daily(db_session_factory, free)
    unknown_row = await _cost_daily(db_session_factory, unknown)

    # The pre-0031 schema, restated: indistinguishable.
    assert unknown_row.total_cost_usd == free_row.total_cost_usd == Decimal("0")
    assert unknown_row.total_input_tokens == free_row.total_input_tokens == 200
    assert unknown_row.total_output_tokens == free_row.total_output_tokens == 100
    assert unknown_row.request_count == free_row.request_count == 2

    # The one column that now separates them.
    assert unknown_row.unpriced_event_count == 2, (
        "an entirely unpriced provider/day must not be stored as a bare 0.000000 "
        "beside 200 real input tokens — that reads as 'this spend was free'"
    )
    assert free_row.unpriced_event_count == 0


@pytest.mark.unit
async def test_a_partly_unpriced_group_keeps_the_known_total_and_counts_the_rest(
    db_session_factory,
):
    """Mixed group: the total stays the KNOWN cost, the unknown rows are counted.

    Two directions are pinned at once, and both are ways this could go wrong:

    * the total must NOT be NULL-poisoned or zeroed by the presence of an unpriced
      row — SQL ``sum()`` skips NULLs and that behaviour is correct and relied on;
    * the unpriced row must still be IN ``request_count`` (it is a real request)
      while being named by the new counter, so the reader can see that the total
      describes only 2 of the 3 requests.
    """
    tenant = T("unpriced-mixed-group")
    await _insert_cost_events(db_session_factory, tenant, ["0.250000", None, "0.100000"])

    await rollup_daily(db_session_factory, DAY)

    row = await _cost_daily(db_session_factory, tenant)
    assert row.total_cost_usd == Decimal("0.350000"), (
        "the stored total must be the sum of the KNOWN costs only — neither "
        "zeroed nor poisoned by the NULL row"
    )
    assert row.request_count == 3, "the unpriced call still happened"
    assert row.unpriced_event_count == 1
    assert row.total_input_tokens == 300


@pytest.mark.unit
async def test_a_fully_priced_day_rolls_up_exactly_as_before(db_session_factory):
    """THE REGRESSION CONTROL: every ordinary bucket must be untouched.

    This is every bucket in production today (zero NULL-cost rows, measured
    2026-09-11), so a change that moved this would be a change to all of the
    money and none of the gap. The expected figures are the pre-0031 answers,
    written out rather than computed, so a drift in the aggregate cannot quietly
    agree with itself.
    """
    tenant = T("unpriced-regression")
    await _insert_cost_events(db_session_factory, tenant, ["0.016500", "0.030000"])

    await rollup_daily(db_session_factory, DAY)

    row = await _cost_daily(db_session_factory, tenant)
    assert row.total_cost_usd == Decimal("0.046500")
    assert row.total_input_tokens == 200
    assert row.total_output_tokens == 100
    assert row.request_count == 2
    assert row.bucket_date == DAY
    assert row.unpriced_event_count == 0
