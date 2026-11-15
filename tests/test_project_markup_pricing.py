"""The project axis must reach the PRICER, not just the rule table (0036).

``services.markup`` gained a ``project_id`` dimension — ``_matches`` pairs it,
``_specificity`` scores it at ``_W_PROJECT`` — and ``markup_rules`` gained the
column. None of that prices a single line unless ``services.billing`` actually
HANDS the pricer the project of the row it is billing.

The defect these tests exist to hold closed: ``generate_billing`` called
``compute_billed_line_detailed`` without ``project_id``, so it defaulted to
``None``; ``_matches`` then evaluated ``row.project_id (set) == None`` → False
and a project-scoped rule could never win. An operator POSTing "tenant T,
project X = 20%" got a 201 with a published specificity of 3 and every line for
project X kept billing at the tenant-wide rate — silently, and with the whole
64/32/16/8/4/2/1 rescale buying nothing.

⚠️ Fixtures here write ``CostDaily`` rows DIRECTLY rather than going through the
rollup. The rollup's own project plumbing is a peer's lane and is in flight; a
test that reads the axis out of the rollup would be measuring that lane, and
would read GREEN-because-sentinel if the rollup were not yet writing projects.
Writing the row explicitly is what makes these assertions about the PRICER.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

from models.db import (
    NULL_PROJECT_ID,
    NULL_SUBJECT_ID,
    CostDaily,
    MarkupRule,
    TenantConfig,
)
from services.billing import generate_billing
from tests.conftest import T

PAST = datetime(2026, 1, 1, tzinfo=UTC)

# The tenant's legacy `tenant_config.markup_pct`. Deliberately DIFFERENT from
# every rule percentage below, so a line that fell through to the fallback is
# distinguishable from one that matched the tenant-wide RULE — otherwise "the
# tenant-wide rate" would be two different mechanisms wearing one number.
FALLBACK_PCT = Decimal("10")
TENANT_WIDE_PCT = Decimal("30")  # tenant-only rule      → specificity 1
PROJECT_PCT = Decimal("100")  # tenant + project rule → specificity 2+1 = 3
LICENCE_PCT = Decimal("50")  # tenant + licence rule → specificity 4+1 = 5
SENTINEL_TRAP_PCT = Decimal("80")  # a rule pinned to the SENTINEL uuid


def _rule(**kw) -> MarkupRule:
    kw.setdefault("effective_from", PAST)
    return MarkupRule(**kw)


def _this_month() -> tuple[str, date]:
    today = datetime.now(UTC).date()
    return today.strftime("%Y-%m"), today


def _cost_row(*, tenant, tenant_name, license_id, project_id, cost) -> CostDaily:
    """One day of LLM spend. Every dimension but the project is held constant,
    so a difference in billed amount can only have come from the project."""
    _, day = _this_month()
    return CostDaily(
        tenant_id=tenant,
        tenant_name=tenant_name,
        license_id=license_id,
        subject_id=NULL_SUBJECT_ID,
        project_id=project_id,
        service_key="kag",  # pragma: allowlist secret
        provider="openai",
        served_model="gpt-5",
        byo=False,
        cost_source="inference-router",
        bucket_date=day,
        total_input_tokens=0,
        total_output_tokens=0,
        total_cost_usd=Decimal(cost),
        request_count=1,
    )


def _tenant_config(tenant: str, name: str) -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant,
        tenant_name=name,
        markup_pct=FALLBACK_PCT,
        billing_mode="standard_markup",
    )


def _billed(result: dict) -> Decimal:
    return Decimal(result["totals"]["customer_cost_usd"])


async def test_project_rule_prices_its_project_and_only_its_project(db_session, test_settings):
    """Two lines, same tenant/licence/app/provider/model, differing ONLY in
    project. The project-X rule prices X; project Y keeps the tenant-wide rate.

    ⭐ This also pins the GRAIN. Before the fix the cost aggregate did not group
    by project, so these two rows summed into ONE line before any rule was
    resolved — at which point no per-project rate is even expressible.
    """
    month, _ = _this_month()
    name = "t-proj-split"
    tenant, lic = T(name), uuid.uuid4()
    project_x, project_y = uuid.uuid4(), uuid.uuid4()

    db_session.add(_tenant_config(tenant, name))
    db_session.add(_rule(tenant_id=tenant, markup_pct=TENANT_WIDE_PCT))
    db_session.add(_rule(tenant_id=tenant, project_id=project_x, markup_pct=PROJECT_PCT))
    db_session.add_all(
        [
            _cost_row(
                tenant=tenant,
                tenant_name=name,
                license_id=lic,
                project_id=project_x,
                cost="10",
            ),
            _cost_row(
                tenant=tenant,
                tenant_name=name,
                license_id=lic,
                project_id=project_y,
                cost="10",
            ),
        ]
    )
    await db_session.commit()

    result = await generate_billing(tenant, month, db_session, test_settings)

    # X at 100% = 20.00, Y at the tenant-wide 30% = 13.00.
    assert _billed(result) == Decimal("33.00")
    # Negative control on the bug's exact symptom: if project_id never reached
    # the pricer, BOTH lines take the tenant-wide rate and the total is 26.00.
    assert _billed(result) != Decimal("26.00")
    assert Decimal(result["totals"]["internal_cost_usd"]) == Decimal("20")


async def test_licence_rule_still_outranks_a_project_rule(db_session, test_settings):
    """⭐ THE LADDER'S DELIBERATE ORDER: `_W_LICENSE` (4) beats
    `_W_PROJECT` + `_W_TENANT` (2 + 1 = 3).

    A line that carries BOTH a licence with its own rule and a project with its
    own rule must bill at the LICENCE rate. This is the operator's decision, not
    an accident of the weights, so it gets its own assertion: a future rescale
    that made PROJECT outrank LICENCE would silently reprice these lines.
    """
    month, _ = _this_month()
    name = "t-proj-vs-lic"
    tenant = T(name)
    lic_special, lic_plain = uuid.uuid4(), uuid.uuid4()
    project_x = uuid.uuid4()

    db_session.add(_tenant_config(tenant, name))
    db_session.add(_rule(tenant_id=tenant, markup_pct=TENANT_WIDE_PCT))
    db_session.add(_rule(tenant_id=tenant, project_id=project_x, markup_pct=PROJECT_PCT))
    db_session.add(_rule(tenant_id=tenant, license_id=lic_special, markup_pct=LICENCE_PCT))
    db_session.add_all(
        [
            # Licence rule AND project rule both match → licence must win.
            _cost_row(
                tenant=tenant,
                tenant_name=name,
                license_id=lic_special,
                project_id=project_x,
                cost="10",
            ),
            # Same project, no licence rule → the project rule wins here, which
            # proves the licence line above was not simply a project-rule MISS.
            _cost_row(
                tenant=tenant,
                tenant_name=name,
                license_id=lic_plain,
                project_id=project_x,
                cost="10",
            ),
        ]
    )
    await db_session.commit()

    result = await generate_billing(tenant, month, db_session, test_settings)

    per_licence = {e["license_id"]: e["costs"]["billed_cost_usd"] for e in result["licenses"]}
    assert Decimal(per_licence[str(lic_special)]) == Decimal("15.00")
    assert Decimal(per_licence[str(lic_plain)]) == Decimal("20.00")
    # 15.00 (licence 50%) + 20.00 (project 100%) — NOT 40.00, which is what a
    # project-outranks-licence ladder would produce.
    assert _billed(result) == Decimal("35.00")


async def test_sentinel_project_resolves_to_the_tenant_wide_rate(db_session, test_settings):
    """⛔ `NULL_PROJECT_ID` IS STORAGE, NOT A PROJECT.

    `billing_cost_daily.project_id` is NOT NULL and carries the sentinel when no
    project was asserted — a device for the rollup's `ON CONFLICT`, since a NULL
    never equals itself in a plain UNIQUE. `markup_rules.project_id` is nullable
    and uses NULL as the wildcard. Passing the raw sentinel through would (a)
    stop the tenant-wide rule matching project-less spend and (b) let a rule
    pinned to the sentinel uuid price it. The trap rule below exists so this
    test FAILS rather than passes-by-luck if the unwrap is dropped.
    """
    month, _ = _this_month()
    name = "t-proj-sentinel"
    tenant, lic = T(name), uuid.uuid4()

    db_session.add(_tenant_config(tenant, name))
    db_session.add(_rule(tenant_id=tenant, markup_pct=TENANT_WIDE_PCT))
    db_session.add(
        _rule(tenant_id=tenant, project_id=NULL_PROJECT_ID, markup_pct=SENTINEL_TRAP_PCT)
    )
    db_session.add(
        _cost_row(
            tenant=tenant,
            tenant_name=name,
            license_id=lic,
            project_id=NULL_PROJECT_ID,
            cost="10",
        )
    )
    await db_session.commit()

    result = await generate_billing(tenant, month, db_session, test_settings)

    # Tenant-wide 30% → 13.00. NOT 18.00, which is the sentinel-pinned trap rule
    # winning because the sentinel was handed to the matcher as a real project.
    assert _billed(result) == Decimal("13.00")
    assert _billed(result) != Decimal("18.00")
