"""The project breakdown must never merge two tenants into one row.

A `project_id` is a uuid minted by kanban and supplied by the caller; billing
never mints or verifies it. It is therefore unique only WITHIN a tenant —
nothing stops two tenants asserting the same uuid, which is precisely the
condition `billing_project_id_multi_tenant_projects` exists to detect.

`routers._aggregation.aggregate_by_project` is reached by four call sites, and
three of them can carry more than one tenant's rows:

* ``GET /api/costs/provider``  — operator scope, NO tenant predicate
* ``GET /api/costs/summary``   — operator scope, NO tenant predicate
* ``GET /api/costs/user/{id}`` — admin scope; `tenant_id` is OPTIONAL and the
  default sums the subject across every tenant they hold a licence in

With ``GROUP BY project_id`` alone those three sum two tenants' spend into ONE
row, so one tenant's money appears inside another's breakdown with nothing in
the response saying so. The fix is the group key ``(tenant_id, project_id)``.

⭐ NON-VACUITY: these tests were run against the pre-fix
``GROUP BY CostDaily.project_id`` and FAIL there (one merged row carrying the
sum of both tenants), then pass with the fix restored.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from models.db import NULL_PROJECT_ID, CostDaily
from tests.conftest import T

# One project uuid, asserted by BOTH tenants — the whole point of the fixture.
SHARED_PROJECT_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")

TENANT_A = T("bpt-tenant-a")
TENANT_B = T("bpt-tenant-b")


async def _seed(
    db_engine,
    *,
    tenant_id,
    project_id,
    cost_usd,
    subject_id=None,
    input_tokens=1000,
    output_tokens=200,
    request_count=5,
    provider="claude",
):
    """Insert one cost_daily row. Returns nothing the caller needs to thread."""
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    kwargs = {}
    if project_id is not None:
        kwargs["project_id"] = project_id
    if subject_id is not None:
        kwargs["subject_id"] = subject_id
    async with factory() as session:
        session.add(
            CostDaily(
                tenant_id=tenant_id,
                license_id=uuid.uuid4(),
                service_key="inference-router",  # pragma: allowlist secret
                provider=provider,
                bucket_date=datetime.now(UTC).date(),
                total_input_tokens=input_tokens,
                total_output_tokens=output_tokens,
                total_cost_usd=Decimal(str(cost_usd)),
                request_count=request_count,
                **kwargs,
            )
        )
        await session.commit()


def _rows_for(data, project_id) -> list[dict]:
    return [p for p in data["by_project"] if p["project_id"] == str(project_id)]


@pytest.mark.unit
class TestByProjectTenantScope:
    @pytest.mark.parametrize("path", ["/api/costs/summary", "/api/costs/provider"])
    async def test_same_project_uuid_on_two_tenants_stays_two_rows(
        self, db_engine, app_client, path
    ):
        """Operator breakdown: one uuid, two tenants -> TWO rows, own amounts."""
        await _seed(db_engine, tenant_id=TENANT_A, project_id=SHARED_PROJECT_ID, cost_usd="3.00")
        await _seed(db_engine, tenant_id=TENANT_B, project_id=SHARED_PROJECT_ID, cost_usd="7.00")

        response = app_client.get(path)
        assert response.status_code == 200
        data = response.json()["data"]

        rows = _rows_for(data, SHARED_PROJECT_ID)
        # The defect: GROUP BY project_id alone returns ONE row of 10.00 here.
        assert len(rows) == 2, f"project rows merged across tenants: {rows}"

        by_tenant = {r["tenant_id"]: r for r in rows}
        assert set(by_tenant) == {TENANT_A, TENANT_B}
        assert Decimal(by_tenant[TENANT_A]["cost_usd"]) == Decimal("3.00")
        assert Decimal(by_tenant[TENANT_B]["cost_usd"]) == Decimal("7.00")
        # Tokens/requests must split with the money, not follow it in bulk.
        assert by_tenant[TENANT_A]["input_tokens"] == 1000
        assert by_tenant[TENANT_B]["input_tokens"] == 1000
        assert by_tenant[TENANT_A]["request_count"] == 5

    async def test_breakdown_still_sums_to_the_reported_total(self, db_engine, app_client):
        """Splitting the group must not lose or double money."""
        await _seed(db_engine, tenant_id=TENANT_A, project_id=SHARED_PROJECT_ID, cost_usd="3.00")
        await _seed(db_engine, tenant_id=TENANT_B, project_id=SHARED_PROJECT_ID, cost_usd="7.00")

        data = app_client.get("/api/costs/summary").json()["data"]
        summed = sum((Decimal(p["cost_usd"]) for p in data["by_project"]), Decimal("0"))
        assert summed == Decimal(data["total_internal_cost_usd"]) == Decimal("10.00")

    async def test_sentinel_group_is_still_reported_per_tenant(self, db_engine, app_client):
        """⛔ The NULL_PROJECT_ID group must remain IN the output.

        Dropping it would make by_project silently sum to LESS than the reported
        total. After the fix it is per-tenant rather than one global row; it is
        still returned, and the breakdown still reconciles exactly.
        """
        await _seed(db_engine, tenant_id=TENANT_A, project_id=SHARED_PROJECT_ID, cost_usd="3.00")
        # project_id omitted -> the model default puts NULL_PROJECT_ID on the row.
        await _seed(db_engine, tenant_id=TENANT_A, project_id=None, cost_usd="1.00")
        await _seed(db_engine, tenant_id=TENANT_B, project_id=None, cost_usd="2.00")

        data = app_client.get("/api/costs/summary").json()["data"]

        sentinel_rows = _rows_for(data, NULL_PROJECT_ID)
        assert len(sentinel_rows) == 2, f"sentinel group not reported per tenant: {sentinel_rows}"
        by_tenant = {r["tenant_id"]: Decimal(r["cost_usd"]) for r in sentinel_rows}
        assert by_tenant == {TENANT_A: Decimal("1.00"), TENANT_B: Decimal("2.00")}

        summed = sum((Decimal(p["cost_usd"]) for p in data["by_project"]), Decimal("0"))
        assert summed == Decimal(data["total_internal_cost_usd"]) == Decimal("6.00")

    async def test_tenant_scoped_route_is_unaffected_and_carries_its_own_tenant(
        self, db_engine, app_client
    ):
        """/api/costs/tenant/{id} already filters by tenant — it sees one row."""
        await _seed(db_engine, tenant_id=TENANT_A, project_id=SHARED_PROJECT_ID, cost_usd="3.00")
        await _seed(db_engine, tenant_id=TENANT_B, project_id=SHARED_PROJECT_ID, cost_usd="7.00")

        data = app_client.get(f"/api/costs/tenant/{TENANT_A}").json()["data"]
        rows = _rows_for(data, SHARED_PROJECT_ID)
        assert len(rows) == 1
        assert rows[0]["tenant_id"] == TENANT_A
        assert Decimal(rows[0]["cost_usd"]) == Decimal("3.00")

    async def test_admin_subject_route_without_tenant_id_keeps_the_tenants_apart(
        self, db_engine, app_client
    ):
        """The THIRD unfiltered call site.

        `/api/costs/user/{subject_id}` makes `tenant_id` optional, so the default
        spans every tenant the subject bills under. Same uuid, same subject, two
        tenants -> still two rows.
        """
        subject_id = uuid.uuid4()
        await _seed(
            db_engine,
            tenant_id=TENANT_A,
            project_id=SHARED_PROJECT_ID,
            cost_usd="3.00",
            subject_id=subject_id,
        )
        await _seed(
            db_engine,
            tenant_id=TENANT_B,
            project_id=SHARED_PROJECT_ID,
            cost_usd="7.00",
            subject_id=subject_id,
        )

        data = app_client.get(f"/api/costs/user/{subject_id}").json()["data"]
        rows = _rows_for(data, SHARED_PROJECT_ID)
        assert len(rows) == 2, f"subject route merged across tenants: {rows}"
        assert {r["tenant_id"] for r in rows} == {TENANT_A, TENANT_B}

        # And with the optional filter supplied it narrows to the one tenant.
        scoped = app_client.get(
            f"/api/costs/user/{subject_id}", params={"tenant_id": TENANT_B}
        ).json()["data"]
        scoped_rows = _rows_for(scoped, SHARED_PROJECT_ID)
        assert len(scoped_rows) == 1
        assert scoped_rows[0]["tenant_id"] == TENANT_B
        assert Decimal(scoped_rows[0]["cost_usd"]) == Decimal("7.00")
