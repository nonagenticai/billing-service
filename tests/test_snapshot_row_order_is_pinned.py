"""The rollup ORDER BY still has a job, and this is the thing that holds it.

⭐ WHY THIS FILE EXISTS. Three rollup aggregates carry an ORDER BY over the
whole GROUP BY key:

* ``services.billing.generate_billing``          — the invoice snapshot
* ``routers.billing.compute_billing_overview``   — ``/billing/overview``
* ``services.admin_aggregate.system_usage``      — the admin system report

Those clauses used to defend MONEY: the allowance was drained line by line
before per-line markup, so whichever line the planner emitted first was the line
whose markup rate was forgone. ``allocate_allowance`` closed that at the source
by splitting each bucket pro-rata across every line at once, so the total is now
identical under every permutation. The money argument is genuinely dead, and
``test_cost_lines_are_consumed_in_a_deterministic_order`` was retired with it —
correctly, because it pinned the WORKAROUND rather than the property.

⛔ BUT A SECOND CLAIM SURVIVED THE FIRST ONE AND WAS LEFT PINNED BY NOTHING.
The comments at all three sites still assert that a fixed order makes a re-run's
emitted DOCUMENT byte-comparable rather than merely arithmetically equal. That
is a real, useful property — it is what lets two invoice runs be diffed — and
after the retirement the only thing defending it was prose. A future column
added to a GROUP BY and not to its ORDER BY reintroduces ties, hands the tie
order back to the planner, and breaks byte-comparability SILENTLY: every total
still reconciles, every money test stays green, and nothing says a word.

⚠️ WHY A TEST AND NOT A RESTORED WARNING. A comment cannot fail. The defect
being guarded against is precisely one a careful reader would not notice, which
is the case where a comment is worth least.

⚠️ WHAT EACH TEST BELOW CAN AND CANNOT CATCH — they are not interchangeable:

* ``test_cost_rollup_order_by_covers_its_group_by`` is the REAL gate for the
  named mutation. It is static because the mutation is static: adding a GROUP BY
  column does not change what a small fixture returns, since the planner is
  deterministic for identical input, so a runtime "run it twice" check would
  stay green through the whole defect and be worth nothing.
* ``test_the_emitted_licenses_list_follows_the_ordered_key`` pins the emitted
  claim end to end, which the static check cannot: it would catch the list being
  built through an unordered structure, or from a second row source that appends
  out of key order, neither of which is visible in the ORDER BY clause.
"""

from __future__ import annotations

import ast
import json
import pathlib
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from models.db import NULL_PROJECT_ID, CostDaily, TenantConfig
from services.billing import generate_billing
from tests.conftest import T

SRC = pathlib.Path(__file__).resolve().parent.parent / "src"

#: The three aggregates whose emitted row order is claimed to be reproducible.
#: All three build their statement into a local named ``cost_stmt``.
TARGETS = {
    "services/billing.py": "the invoice snapshot's `licenses` list",
    "routers/billing.py": "the /billing/overview tenant list",
    "services/admin_aggregate.py": "the admin system report's by_tenant list",
}

#: Every one of the three groups on at least this many columns today. The floor
#: is here so that a refactor which empties or relocates the clause is reported
#: as a blinded gate rather than passing an empty-vs-empty comparison.
MIN_KEY_COLUMNS = 6


def _columns(call: ast.Call) -> list[str]:
    """The ``CostDaily.x`` arguments of one ``.group_by(...)``/``.order_by(...)``.

    Anything that is not a plain attribute access (``func.sum(...).desc()``, a
    label, a literal) is rendered as its dump so it compares unequal to a column
    rather than silently reading as one.
    """
    out = []
    for arg in call.args:
        if isinstance(arg, ast.Attribute) and isinstance(arg.value, ast.Name):
            out.append(f"{arg.value.id}.{arg.attr}")
        else:
            out.append(ast.dump(arg))
    return out


def _cost_stmt_chain(relpath: str) -> dict[str, ast.Call]:
    """Every chained method call of the ``cost_stmt = select(...)...`` expression.

    ``select(...).where(...).group_by(...).order_by(...)`` nests outermost-last,
    so walking ``.func.value`` from the top collects the whole chain.
    """
    tree = ast.parse((SRC / relpath).read_text(encoding="utf-8"))
    value = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "cost_stmt" for t in node.targets
        ):
            value = node.value
            break
    assert value is not None, (
        f"no `cost_stmt = ...` assignment found in src/{relpath}. This gate has "
        "been BLINDED by a rename, not satisfied by a fix — point it at the new "
        "name rather than deleting it."
    )
    chain: dict[str, ast.Call] = {}
    cur = value
    while isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
        chain.setdefault(cur.func.attr, cur)
        cur = cur.func.value
    return chain


@pytest.mark.unit
@pytest.mark.parametrize("relpath", sorted(TARGETS))
def test_cost_rollup_order_by_covers_its_group_by(relpath):
    """⛔ THE GATE. Every GROUP BY column must appear in the ORDER BY.

    Ordering on the full group key is what makes the order TOTAL — no ties left
    for the planner to break however it likes. Ordering on a strict subset
    leaves rows that compare equal, and which of those the planner emits first
    is not defined, not stable across plans, and not stable across a version
    upgrade.

    ⚠️ MUTATION TARGET: delete ``CostDaily.byo`` from any one of the three
    ``.order_by(...)`` clauses, leaving it in the ``.group_by(...)``.
    """
    chain = _cost_stmt_chain(relpath)
    assert "group_by" in chain, f"src/{relpath}: cost_stmt no longer groups at all"
    assert "order_by" in chain, (
        f"src/{relpath}: cost_stmt groups but no longer orders. {TARGETS[relpath]} "
        "is built by iterating these rows, so its order is now whatever the "
        "planner chose — see this module's docstring before removing the clause."
    )

    group_cols = _columns(chain["group_by"])
    order_cols = _columns(chain["order_by"])
    missing = [c for c in group_cols if c not in order_cols]

    assert not missing, (
        f"src/{relpath}: {missing} is grouped but not ordered. Rows differing "
        f"only in {missing} now tie, and the planner picks which comes first, so "
        f"{TARGETS[relpath]} stops being reproducible across re-runs while every "
        "money total still reconciles. Add the column to the ORDER BY too."
    )


@pytest.mark.unit
def test_this_gate_can_actually_see_all_three_statements():
    """⭐ NON-VACUITY. A parity check that finds nothing passes perfectly.

    The check above compares two lists pulled out of source. If a rename, a
    refactor into a helper, or a moved file made those lists empty, every
    parametrisation would pass on ``[] ⊆ []`` and the gate would be green
    precisely because it had gone blind. So assert the shape it is measuring.
    """
    found = {}
    for relpath in TARGETS:
        chain = _cost_stmt_chain(relpath)
        found[relpath] = (len(_columns(chain["group_by"])), len(_columns(chain["order_by"])))

    assert len(found) == 3, found
    assert all(g >= MIN_KEY_COLUMNS and o >= MIN_KEY_COLUMNS for g, o in found.values()), (
        f"a rollup key shrank below {MIN_KEY_COLUMNS} columns: {found}. That may "
        "be legitimate, but confirm the parity check is still measuring a real "
        "key and lower the floor deliberately."
    )


def _cost_row(
    *, tenant: str, license_id: uuid.UUID, subject: uuid.UUID, provider: str
) -> CostDaily:
    return CostDaily(
        tenant_id=tenant,
        tenant_name="row-order",
        license_id=license_id,
        subject_id=subject,
        project_id=NULL_PROJECT_ID,
        service_key="inference-router",  # pragma: allowlist secret
        provider=provider,
        served_model="gpt-5",
        byo=False,
        cost_source="inference-router",
        bucket_date=datetime.now(UTC).date(),
        total_input_tokens=1000,
        total_output_tokens=500,
        total_cost_usd=Decimal("1.500000"),
        request_count=1,
    )


def _month(d: date | None = None) -> str:
    return (d or datetime.now(UTC).date()).strftime("%Y-%m")


@pytest.mark.unit
async def test_the_emitted_licenses_list_follows_the_ordered_key(db_session, test_settings):
    """The snapshot's `licenses` list is a function of the DATA, not of arrival.

    ``license_id`` leads the ORDER BY, and the list is built by first appearance
    while iterating the ordered rows — so the emitted licences come out in
    ascending ``license_id``. That is the observable form of the claim the three
    comments make, and unlike "the two runs matched" it is a property of ONE run:
    it fails if the list is ever built through something unordered or fed by a
    second row source that appends out of key order.

    The re-run byte comparison is asserted too, because byte-comparability is
    what the comments literally promise — but it is the weaker half, and on its
    own it would be near-vacuous: identical input to a deterministic planner
    returns identical order whether or not the ORDER BY is complete.
    """
    tenant = T("roworder-licenses")
    subject = uuid.uuid4()
    # Six licences, generated then shuffled out of sort order, so "sorted on the
    # way out" cannot be an accident of insertion order.
    licences = sorted(uuid.uuid4() for _ in range(6))
    insertion = [licences[i] for i in (4, 0, 5, 2, 1, 3)]
    assert insertion != licences, "the fixture must insert out of sorted order"

    db_session.add(
        TenantConfig(
            tenant_id=tenant,
            tenant_name="row-order",
            markup_pct=Decimal("10"),
            billing_mode="standard_markup",
        )
    )
    for lic in insertion:
        # Two providers per licence, so the key has real ties below its lead
        # column and the trailing ORDER BY columns are doing visible work.
        for provider in ("openai", "anthropic"):
            db_session.add(
                _cost_row(tenant=tenant, license_id=lic, subject=subject, provider=provider)
            )
    await db_session.commit()

    first = await generate_billing(tenant, _month(), db_session, test_settings)
    emitted = [str(e["license_id"]) for e in first["licenses"]]

    assert len(emitted) == len(licences), emitted
    assert emitted == [str(lic) for lic in licences], (
        "the snapshot's `licenses` list is no longer in ordered-key order, so a "
        "re-run's document is not byte-comparable with this one."
    )

    second = await generate_billing(tenant, _month(), db_session, test_settings)
    assert json.dumps(first["licenses"], default=str, sort_keys=False) == json.dumps(
        second["licenses"], default=str, sort_keys=False
    ), "two runs over identical rows emitted different documents"
