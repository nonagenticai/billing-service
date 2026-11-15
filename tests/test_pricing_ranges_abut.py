"""A superseded rate must stop being valid exactly where its successor starts.

THE DEFECT, measured on prod 2026-09-10. ``provider_pricing`` held **51 pairs of
rows overlapping on their FULL key** (provider, model, rate_class, cost_basis,
service_key) — two rates simultaneously valid, so which one priced a usage row in
that window was decided by row order rather than by the timeline.

``upsert_pricing`` read the clock TWICE. The prior row was closed at
``closed_at = ts or datetime.now(UTC)``, a server-side read, while the new row
began at ``payload.effective_from`` — computed by the caller, before the POST.
Whenever those differed, both rows were valid for exactly the difference. The
shape appears at two scales and one fix covers both:

* **sub-second**, from ordinary clock skew between caller and server. ``hirag``'s
  window was 566 ms (prior ``effective_until`` 11:03:31.618063 against the new
  row's ``effective_from`` 11:03:31.052081); ``kag-graph``'s was 506 ms.
* **months**, from a deliberately backdated ``effective_from``. ``mlx-whisper``
  id 116 starts 2026-04-26 and overlaps ids 25 and 27 for their entire life, at
  **3.00 against 283.33 per 1M input — a 94x spread** with no determinate answer.

⚠️ REALISED DAMAGE WAS $0.00, AND THAT IS NOT A REASON TO LEAVE IT. Every overlap
window had closed by 2026-07-21 and the oldest surviving event is 2026-09-05
(``billing_cost_events`` was truncated at 07:01:56Z that day), so no event ever
fell inside one; the latent repricing swing was $1.99, worst ratio 10.0x on
``kag-graph``. What was wrong is that the price of a unit was not a function of
the timeline. That does not need to be expensive to be worth removing — and the
route's own docstring in ``routers/config.py`` already promised the correct
behaviour ("by setting its ``effective_until`` to the new row's
``effective_from``"), so the documented contract and the code disagreed.

⛔ AND THE INVERTED-INTERVAL CASE IS NOT A SUBSET OF IT. Closing the prior row at
a new ``effective_from`` that precedes the prior row's OWN start would write
``effective_until < effective_from`` onto it — valid over an inverted interval,
which every range predicate reads as "valid never". The superseded rate would
vanish from the timeline instead of being closed, and the only symptom would be
usage silently pricing against whatever row matched next. That is refused, not
clamped: a clamp would pick a timeline the caller did not ask for.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import select

from models.db import ProviderPricing
from models.domain import ProviderPricingCreate
from services.pricing import PricingSupersedeBeforePriorError, upsert_pricing

pytestmark = pytest.mark.asyncio

_PROVIDER = "abut-fixture-provider"
_MODEL = "abut-fixture-model"


def _payload(*, effective_from: datetime, rate: str) -> ProviderPricingCreate:
    return ProviderPricingCreate(
        provider=_PROVIDER,
        model=_MODEL,
        service_key=None,
        cost_model="per_token",
        input_per_1m_usd=Decimal(rate),
        output_per_1m_usd=Decimal(rate),
        effective_from=effective_from,
    )


@pytest.mark.unit
async def test_a_backdated_supersede_leaves_no_overlap(db_session_factory) -> None:
    """THE REGRESSION GUARD — the production shape, reproduced.

    The caller supplies an ``effective_from`` in the past, exactly as the seeder
    and ``scripts/reconcile_pricing_units.py`` do. Before the fix the prior row
    was closed at the server's own ``now()``, which is LATER, so both rows were
    valid across that span. The assertion is equality, not ``<=``: abutting is the
    property, and ``<=`` would also pass on a GAP, which is the opposite defect
    (an instant with no rate at all, pricing at $0 and looking like success).
    """
    backdated = datetime.now(UTC) - timedelta(days=30)
    async with db_session_factory() as s:
        first, _ = await upsert_pricing(s, _payload(effective_from=backdated, rate="3"))
        await s.commit()

        second, prior = await upsert_pricing(
            s, _payload(effective_from=backdated + timedelta(days=10), rate="283.33")
        )
        await s.commit()

        assert prior is not None and prior.id == first.id
        refreshed = (
            await s.execute(select(ProviderPricing).where(ProviderPricing.id == first.id))
        ).scalar_one()
        assert refreshed.effective_until == second.effective_from, (
            "the superseded row does not stop exactly where its successor starts: "
            f"until={refreshed.effective_until} vs from={second.effective_from}"
        )
        assert refreshed.effective_until > refreshed.effective_from, (
            "the superseded row was closed before it opened"
        )


@pytest.mark.unit
async def test_no_instant_resolves_to_two_rows(db_session_factory) -> None:
    """The property stated as the query that found the defect.

    This is the same self-join that measured 51 overlapping pairs on prod, run
    against the two rows this test wrote. It asserts the absence of overlap
    directly rather than inferring it from the boundary, so a future edit that
    satisfies the equality above by some other route still cannot reintroduce a
    simultaneously-valid pair.
    """
    backdated = datetime.now(UTC) - timedelta(days=30)
    async with db_session_factory() as s:
        await upsert_pricing(s, _payload(effective_from=backdated, rate="3"))
        await s.commit()
        await upsert_pricing(
            s, _payload(effective_from=backdated + timedelta(days=10), rate="283.33")
        )
        await s.commit()

        rows = list(
            (
                await s.execute(
                    select(ProviderPricing)
                    .where(ProviderPricing.provider == _PROVIDER)
                    .order_by(ProviderPricing.effective_from)
                )
            ).scalars()
        )
        assert len(rows) == 2, f"expected exactly two rows, got {len(rows)}"
        overlaps = [
            (a.id, b.id)
            for a in rows
            for b in rows
            if a.id != b.id
            and a.effective_from < (b.effective_until or datetime.max.replace(tzinfo=UTC))
            and b.effective_from < (a.effective_until or datetime.max.replace(tzinfo=UTC))
        ]
        assert not overlaps, f"two rates valid at one instant: {overlaps}"


@pytest.mark.unit
async def test_a_supersede_that_predates_its_predecessor_is_refused(db_session_factory) -> None:
    """⛔ Refused, not clamped — an inverted interval reads as "valid never"."""
    now = datetime.now(UTC)
    async with db_session_factory() as s:
        first, _ = await upsert_pricing(s, _payload(effective_from=now, rate="3"))
        await s.commit()

        with pytest.raises(PricingSupersedeBeforePriorError) as caught:
            await upsert_pricing(s, _payload(effective_from=now - timedelta(days=1), rate="283.33"))

        assert caught.value.prior_effective_from == first.effective_from
        assert caught.value.provider == _PROVIDER


@pytest.mark.unit
async def test_an_equal_effective_from_is_not_refused(db_session_factory) -> None:
    """NEGATIVE CONTROL. A supersede at the SAME instant is legitimate.

    Without this, the guard above could be tightened to ``<=`` by a later reader
    "for safety" and would then refuse the ordinary same-tick case — a gate that
    rejects valid work is not a stricter version of one that rejects invalid work.
    The prior row is closed to a zero-width interval, which is correct: it was
    never in effect.
    """
    now = datetime.now(UTC)
    async with db_session_factory() as s:
        first, _ = await upsert_pricing(s, _payload(effective_from=now, rate="3"))
        await s.commit()

        second, prior = await upsert_pricing(s, _payload(effective_from=now, rate="283.33"))
        await s.commit()

        assert prior is not None and prior.id == first.id
        assert second.effective_from == now
        refreshed = (
            await s.execute(select(ProviderPricing).where(ProviderPricing.id == first.id))
        ).scalar_one()
        assert refreshed.effective_until == now


# ⛔ NO TEST FOR THE ``payload.effective_from or closed_at`` FALLBACK, AND THAT IS
# NOT AN OMISSION. ``ProviderPricingCreate.effective_from`` is a REQUIRED
# datetime, so pydantic refuses ``None`` before ``upsert_pricing`` is entered and
# the ``or`` can only fire on a falsy datetime, which does not exist. I wrote that
# test first and it failed with ``Input should be a valid datetime`` — the branch
# is vestigial, not untested. Deliberately left in place rather than deleted: it
# costs nothing, and removing it would make a future widening of the model to
# ``datetime | None`` silently fall back to a SECOND clock read, which is exactly
# the defect this file exists to close.
