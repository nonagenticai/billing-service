"""Money bugs: cross-license budget drain, falsy-zero plan sync, silent licenses outage.

Three defects, one theme — a value that carried real money meaning was thrown
away before it reached the code that needed it:

A. ``flush_costs`` aggregated cost events to ``(tenant,)`` and ``debit_budget``
   had no license parameter at all, so a tenant's cascade drained EVERY active
   budget regardless of which license incurred the cost. The reserve path had
   always scoped by license; only the debit path had not.
B. ``sync-from-licenses`` gated its two upserts on truthiness, so a plan
   suspended to ``0`` was indistinguishable from a plan with no field at all.
C. ``fetch_licenses`` returned ``[]`` for every failure, so "the licenses
   service is down" was indistinguishable from "this tenant has no licenses".
"""

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import select

from models.db import NULL_LICENSE_ID, RequestQuotaConfig, TokenBudget
from services.aggregator import flush_costs
from services.billing import LicensesUnavailableError, fetch_licenses
from services.budget import create_budget
from tests._aggregator_helpers import _make_cost
from tests.conftest import T


async def _mk_budget(
    session, tenant_id, license_id, tokens, *, source="license_sync", priority=100
):
    """Create one active monthly budget and commit it.

    ``priority`` is ALWAYS passed explicitly by these tests. Cascade order is
    ``(expires_at, priority, starts_at)``; rows created in the same test share a
    ``starts_at`` to the microsecond, so without a distinct priority there is no
    total order and Postgres may return them either way round. That is not
    hypothetical — prod's two ``n8n-dev`` budgets carry identical
    ``starts_at``/``priority``, which is why the phantom spend landed on them in
    an arbitrary 36 228 / 48 929 split rather than filling the first one.
    """
    budget = await create_budget(
        tenant_id=tenant_id,
        budget_tokens=tokens,
        period="monthly",
        license_id=license_id,
        source=source,
        priority=priority,
        session=session,
    )
    await session.commit()
    return budget.id


async def _consumed(session, budget_id) -> Decimal:
    """Re-read a budget's persisted consumption straight from the DB."""
    session.expire_all()
    row = (
        await session.execute(select(TokenBudget).where(TokenBudget.id == budget_id))
    ).scalar_one()
    return row.consumed_tokens


class TestLicenseScopedBudgetDebit:
    """DEFECT A — a cost event may only drain the budgets of ITS OWN license.

    Prod evidence this reproduces (tenant ``n8n-dev``, worker4 ``billing`` DB):
    two active budgets, licenses ``069c6c62…`` and ``069c6d9f…``, consumed
    36 228 and 48 929 tokens. Neither license had a single row in
    ``billing_cost_events``. The tenant's actual traffic was 87 license-LESS
    system events (84 658 tokens) plus one event for a THIRD license
    ``51826167…`` (499 tokens) that holds no budget at all —
    ``84 658 + 499 = 85 157 = 36 228 + 48 929`` exactly.
    """

    @pytest.mark.unit
    async def test_cost_for_one_license_does_not_drain_another_licenses_budget(
        self, db_session_factory, db_session
    ):
        """THE DEFECT. L1's traffic must leave L2's budget at zero.

        L2's budget is deliberately given the LOWER priority, i.e. it sits FIRST
        in cascade-drain order. That is what makes this a real detector: with an
        unscoped cascade the 1200 tokens are taken from whichever budget comes
        first, so if L1's budget led, the assertion would pass on the broken code
        by pure ordering luck and prove nothing.
        """
        tenant = T(f"scoped-{uuid.uuid4().hex[:8]}")
        lic1, lic2 = uuid.uuid4(), uuid.uuid4()
        b2 = await _mk_budget(db_session, tenant, lic2, 100_000, priority=1)
        b1 = await _mk_budget(db_session, tenant, lic1, 100_000, priority=200)

        await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant,
                    license_id=str(lic1),
                    input_tokens="1000",
                    output_tokens="200",
                )
            ],
            db_session_factory,
            None,
        )

        assert await _consumed(db_session, b2) == Decimal("0.000000"), (
            "L2's budget was drained by a cost event belonging to L1"
        )
        # Control: the license that DID incur the cost is debited, in full.
        assert await _consumed(db_session, b1) == Decimal("1200.000000")

    @pytest.mark.unit
    async def test_license_spend_still_cascades_into_the_shared_tenant_pool(
        self, db_session_factory, db_session
    ):
        """Scoping must not be over-narrow.

        A license's own budget PLUS the tenant-level pool is the scope — the
        same rule ``reserve_budget`` has always applied — so an over-cap spend
        spills into the shared pool and stops there. A second license's budget
        is still untouched.
        """
        tenant = T(f"cascade-{uuid.uuid4().hex[:8]}")
        lic1, lic2 = uuid.uuid4(), uuid.uuid4()
        # The unrelated license leads the cascade, so an unscoped drain would
        # take from it first — the assertion below is a real detector.
        other = await _mk_budget(db_session, tenant, lic2, 100_000, priority=1)
        small = await _mk_budget(db_session, tenant, lic1, 500, priority=2)
        pool = await _mk_budget(db_session, tenant, None, 100_000, source="manual", priority=3)

        await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant,
                    license_id=str(lic1),
                    input_tokens="1000",
                    output_tokens="200",
                )
            ],
            db_session_factory,
            None,
        )

        drained = await _consumed(db_session, small) + await _consumed(db_session, pool)
        assert drained == Decimal("1200.000000"), "the 1200 tokens must land somewhere in scope"
        assert await _consumed(db_session, pool) > 0, "the shared tenant pool must absorb the spill"
        assert await _consumed(db_session, other) == Decimal("0.000000"), (
            "an unrelated license's budget was drained by the cascade"
        )

    @pytest.mark.unit
    async def test_licenseless_system_event_debits_only_the_tenant_level_pool(
        self, db_session_factory, db_session
    ):
        """THE DESIGN DECISION for license-less (system/S2S) spend.

        A license-less event has no license to attribute to, so its scope IS the
        tenant level: it debits the tenant-level pool (``license_id =
        NULL_LICENSE_ID``) and can never reach a license-attributed budget.

        This is the debit-side counterpart of ``is_system_license`` /
        ``CostDaily.license_attributed_clause``, which already drop these rows
        from every license-keyed billing aggregate ("system/S2S cost is never
        attributed to a customer invoice line"). Charging a customer's license
        budget for cost that is never invoiced to that customer was the
        contradiction; charging the tenant-level pool — the bucket that exists
        precisely for non-license-attributed spend — keeps the cap honest
        without inventing an attribution.
        """
        tenant = T(f"sysless-{uuid.uuid4().hex[:8]}")
        lic1 = uuid.uuid4()
        # The license budget leads the cascade: an unscoped drain hits it first.
        licensed = await _mk_budget(db_session, tenant, lic1, 100_000, priority=1)
        pool = await _mk_budget(db_session, tenant, None, 100_000, source="manual", priority=2)

        await flush_costs(
            [_make_cost(tenant_id=tenant, license_id="", input_tokens="700", output_tokens="300")],
            db_session_factory,
            None,
        )

        assert await _consumed(db_session, pool) == Decimal("1000.000000"), (
            "license-less system spend must debit the tenant-level pool"
        )
        assert await _consumed(db_session, licensed) == Decimal("0.000000"), (
            "license-less system spend reached a license-attributed budget"
        )

    @pytest.mark.unit
    async def test_licenseless_system_event_debits_nothing_without_a_tenant_pool(
        self, db_session_factory, db_session
    ):
        """The exact prod shape: only license budgets exist, so there is no
        in-scope budget for license-less spend and NOTHING is debited.

        "Charge nothing" here is not a fail-open — the tenant simply never
        configured a tenant-level cap. Draining the license budgets instead is
        what produced the 84 658 phantom tokens on ``n8n-dev``.
        """
        tenant = T(f"noload-{uuid.uuid4().hex[:8]}")
        b1 = await _mk_budget(db_session, tenant, uuid.uuid4(), 100_000, priority=1)
        b2 = await _mk_budget(db_session, tenant, uuid.uuid4(), 100_000, priority=2)

        await flush_costs(
            [_make_cost(tenant_id=tenant, license_id="", input_tokens="700", output_tokens="300")],
            db_session_factory,
            None,
        )

        assert await _consumed(db_session, b1) == Decimal("0.000000")
        assert await _consumed(db_session, b2) == Decimal("0.000000")

    @pytest.mark.unit
    async def test_prod_shape_third_licenses_traffic_leaves_both_budgets_at_zero(
        self, db_session_factory, db_session
    ):
        """Replay the n8n-dev shape: 87 license-less events + 1 event for a
        third, budget-less license. Both budgeted licenses must stay at zero.

        On main this summed to 85 157 tokens spread across the two budgets.
        """
        tenant = T(f"n8ndev-{uuid.uuid4().hex[:8]}")
        b1 = await _mk_budget(db_session, tenant, uuid.uuid4(), 100_000, priority=1)
        b2 = await _mk_budget(db_session, tenant, uuid.uuid4(), 100_000, priority=2)
        third = uuid.uuid4()  # a real license, but it holds no budget

        buffer = [
            _make_cost(tenant_id=tenant, license_id="", input_tokens="900", output_tokens="100")
            for _ in range(87)
        ]
        buffer.append(
            _make_cost(
                tenant_id=tenant, license_id=str(third), input_tokens="400", output_tokens="99"
            )
        )
        await flush_costs(buffer, db_session_factory, None)

        assert await _consumed(db_session, b1) == Decimal("0.000000")
        assert await _consumed(db_session, b2) == Decimal("0.000000")

    @pytest.mark.unit
    async def test_two_licenses_in_one_batch_are_debited_independently(
        self, db_session_factory, db_session
    ):
        """One flush carrying both licenses' traffic debits each to its own
        budget — the per-(tenant, license) grouping must not smear them.
        """
        tenant = T(f"batch-{uuid.uuid4().hex[:8]}")
        lic1, lic2 = uuid.uuid4(), uuid.uuid4()
        b1 = await _mk_budget(db_session, tenant, lic1, 100_000, priority=1)
        b2 = await _mk_budget(db_session, tenant, lic2, 100_000, priority=2)

        await flush_costs(
            [
                _make_cost(
                    tenant_id=tenant, license_id=str(lic1), input_tokens="100", output_tokens="0"
                ),
                _make_cost(
                    tenant_id=tenant, license_id=str(lic2), input_tokens="0", output_tokens="50"
                ),
            ],
            db_session_factory,
            None,
        )

        assert await _consumed(db_session, b1) == Decimal("100.000000")
        assert await _consumed(db_session, b2) == Decimal("50.000000")


class TestZeroIsNotAbsentOnLicenseSync:
    """DEFECT B — ``if x and x > 0`` made a suspended (0) plan look unset.

    PR #68 established the semantics on the READ side: ``quota_monthly=0``
    blocks all; unlimited is NULL/absent. Both writers in
    ``sync-from-licenses`` still dropped a 0 on the floor.
    """

    @pytest.mark.unit
    async def test_quota_monthly_zero_writes_a_durable_config_row(self, app_client, db_session):
        """A plan suspended to quota_monthly=0 must persist a 0 config row — not be
        skipped, which ``/api/quota/check`` would read as UNLIMITED."""
        lic_uuid = uuid.uuid4()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"id": str(lic_uuid), "tenant_id": T("susp-row"), "plan": {"quota_monthly": 0}}
        ]
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            app_client.post("/api/budget/sync-from-licenses")

        row = (
            await db_session.execute(
                select(RequestQuotaConfig).where(RequestQuotaConfig.license_id == lic_uuid)
            )
        ).scalar_one_or_none()
        assert row is not None, "quota_monthly=0 persisted no config row"
        assert row.quota_monthly == 0

    @pytest.mark.unit
    async def test_token_budget_downgraded_to_zero_overwrites_the_stale_row(
        self, app_client, fake_redis, monkeypatch, db_session
    ):
        """A license downgraded 1_000_000 -> 0 must have its license_sync budget
        rewritten to 0, not left enforcing the stale million.

        The orphan reconcile deliberately spares this row (its license is still
        active), so a skipped upsert meant the old grant lived forever.
        """
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)
        tenant = T(f"downgrade-{uuid.uuid4().hex[:8]}")
        lic_uuid = uuid.uuid4()
        budget_id = await _mk_budget(db_session, tenant, lic_uuid, 1_000_000)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {
                "id": str(lic_uuid),
                "tenant_id": tenant,
                "plan": {"token_budget_monthly": 0},
            }
        ]
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            response = app_client.post("/api/budget/sync-from-licenses")

        assert response.status_code == 200
        db_session.expire_all()
        row = (
            await db_session.execute(select(TokenBudget).where(TokenBudget.id == budget_id))
        ).scalar_one()
        assert row.budget_tokens == 0, (
            "a plan downgraded to 0 left a stale 1_000_000-token budget active"
        )

    @pytest.mark.unit
    async def test_absent_plan_values_are_still_skipped_as_unlimited(
        self, app_client, fake_redis, monkeypatch, db_session
    ):
        """Guard against over-correcting: ABSENT still means unlimited, so a
        plan carrying neither field must write nothing at all."""
        import redis_client

        monkeypatch.setattr(redis_client, "_redis", fake_redis)
        lic_uuid = uuid.uuid4()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"id": str(lic_uuid), "tenant_id": T("unlimited-tenant"), "plan": {}}
        ]
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("routers.budget.httpx.AsyncClient", return_value=mock_client):
            response = app_client.post("/api/budget/sync-from-licenses")

        data = response.json()
        assert data["licenses_synced"] == 0
        assert data["quotas_synced"] == 0
        row = (
            await db_session.execute(
                select(RequestQuotaConfig).where(RequestQuotaConfig.license_id == lic_uuid)
            )
        ).scalar_one_or_none()
        assert row is None


class TestFetchLicensesFailsLoud:
    """DEFECT C — an outage must not read as "this tenant has no licenses"."""

    @staticmethod
    def _client_returning(status_code: int):
        mock_resp = httpx.Response(
            status_code, request=httpx.Request("GET", "http://licenses/internal/licenses")
        )
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        return mock_client

    @pytest.mark.unit
    @pytest.mark.parametrize("status_code", [401, 403, 404, 500, 503])
    async def test_error_status_raises_instead_of_returning_empty(self, test_settings, status_code):
        """A 401 is a RESPONSE, not an ``httpx.HTTPError`` — the old ``except``
        never fired for it and the bare ``return []`` at the bottom answered
        instead. Every non-2xx must now raise."""
        with (
            patch(
                "services.billing.httpx.AsyncClient",
                return_value=self._client_returning(status_code),
            ),
            pytest.raises(LicensesUnavailableError),
        ):
            await fetch_licenses(T("test-tenant"), test_settings)

    @pytest.mark.unit
    async def test_transport_failure_raises_instead_of_returning_empty(self, test_settings):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.ReadTimeout("timed out"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("services.billing.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(LicensesUnavailableError),
        ):
            await fetch_licenses(T("test-tenant"), test_settings)

    @pytest.mark.unit
    async def test_genuine_empty_license_set_still_returns_empty(self, test_settings):
        """The one case that legitimately yields ``[]``: the service answered
        200 with an empty list. Without this, "raise on everything" would be
        indistinguishable from the defect in the other direction."""
        mock_resp = httpx.Response(
            200, json=[], request=httpx.Request("GET", "http://licenses/internal/licenses")
        )
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("services.billing.httpx.AsyncClient", return_value=mock_client):
            assert await fetch_licenses(T("test-tenant"), test_settings) == []

    @pytest.mark.unit
    def test_quota_status_surfaces_503_not_an_empty_license_list(self, app_client):
        """End-to-end: the route whose docstring claimed it "fails closed" must
        answer 503, not 200 with ``licenses: [], total: 0``.

        Sent with NO ``X-Tenant-Id``, deliberately: that header would make this
        a CUSTOMER call (``require_tenant`` takes the header branch and
        ``enforce_tenant_isolation`` becomes live), which is not the path this
        test is about. The dev-mode admin JWT is the subject, and it works
        because ``auth.DEV_SUBJECT_TENANT_ID = derived_tenant_id("dev")`` is a
        real tenant uuid — the header was a workaround for a since-fixed window
        where ``sub="dev"`` made ``require_tenant`` answer 400 and mask the 503.
        """
        tenant = T("test-tenant")
        with patch(
            "routers.quota.fetch_licenses",
            AsyncMock(side_effect=LicensesUnavailableError(tenant, "HTTP 401")),
        ):
            response = app_client.get(f"/api/quota/status/{tenant}")

        assert response.status_code == 503, (
            "a licenses outage was reported as a tenant with zero licenses"
        )

    @pytest.mark.unit
    async def test_invoice_degrades_but_does_not_fail_on_a_licenses_outage(
        self, db_session, test_settings
    ):
        """The DELIBERATE asymmetry with ``quota_status``.

        An invoice's figures come from our own CostDaily/MeteringDaily rows;
        the licenses fetch only ENRICHES lines with app/plan metadata. Hard
        failing every invoice on a licenses blip would be a worse trade than
        losing labels, so ``generate_billing`` catches and logs. Asserting it
        here stops a later "make it consistent" change from silently turning a
        metadata outage into a billing outage.
        """
        from services.billing import generate_billing

        with patch(
            "services.billing.fetch_licenses",
            AsyncMock(side_effect=LicensesUnavailableError(T("degrade-tenant"), "HTTP 500")),
        ):
            report = await generate_billing(
                T("degrade-tenant"), "2026-07", db_session, test_settings
            )
        assert report["tenant_id"] == T("degrade-tenant")


class TestScopeConstantsAreWired:
    """The sentinel really is the tenant-level scope, not a magic zero UUID."""

    @pytest.mark.unit
    async def test_tenant_level_budget_stores_the_null_license_sentinel(self, db_session):
        tenant = T(f"sentinel-{uuid.uuid4().hex[:8]}")
        bid = await _mk_budget(db_session, tenant, None, 10, source="manual")
        row = (
            await db_session.execute(select(TokenBudget).where(TokenBudget.id == bid))
        ).scalar_one()
        assert row.license_id == NULL_LICENSE_ID
