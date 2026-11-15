"""Streaming CSV export endpoint tests.

Both endpoints are admin-gated and stream paginated CSV. Tests verify:
* CSV header + row format
* Window validation (default 30d, max 92d, until > since)
* Tenant filter
* Pagination (insert > MAX_PAGE_SIZE rows, count rows out)
* Billing default excludes unpriced; opt-in includes them
* CSV quoting (commas / quotes in cell values)

Auth bypass (``DASHBOARD_AUTH_ENABLED=false`` in conftest) means tests
hit the endpoints without a JWT — auth-gate tests live in
``tests/test_auth_*.py`` and exercise the dep itself, not these routes.

Real DB (per-test schema), real CSV parsing on the response body.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import quote

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from models.db import AuditLog, CostEvent
from routers.export import MAX_PAGE_SIZE, MAX_WINDOW_DAYS
from tests.conftest import T


def _audit(*, action: str = "test.x", days_ago: float = 1.0, tenant: str | None = None) -> AuditLog:
    return AuditLog(
        ts=datetime.now(UTC) - timedelta(days=days_ago),
        actor_sub="test-actor",
        actor_role="license-admin",
        action=action,
        resource_type="r",
        resource_id="rid",
        tenant_id=tenant,
        tenant_name=None,
        before=None,
        after={"k": "v"},
        request_id="req",
    )


def _cost(
    *, days_ago: float = 1.0, tenant: str, cost_usd: Decimal | None = Decimal("0.001")
) -> CostEvent:
    return CostEvent(
        tenant_id=tenant,
        tenant_name=tenant,
        license_id=None,
        subject_id=None,
        app_id=None,
        service_key="test-svc",
        provider="testprov",
        model="testmodel",
        served_model="testmodel",
        input_tokens=100,
        output_tokens=50,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=0,
        cost_usd=cost_usd,
        cost_usd_hint=None,
        pricing_id=None,
        provider_request_id=None,
        usage_extra=None,
        latency_ms=200,
        extra_metadata=None,
        correlation_id=None,
        created_at=datetime.now(UTC) - timedelta(days=days_ago),
    )


def _parse_csv(body: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(body)))


# ---------------------------------------------------------------------------
# Audit export
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_export_streams_header_and_rows(app_client, db_session: AsyncSession) -> None:
    db_session.add_all(
        [
            _audit(action="config.update", days_ago=1, tenant=T("acme")),
            _audit(action="budget.create", days_ago=2, tenant=T("acme")),
            _audit(action="budget.create", days_ago=3, tenant=T("other")),
        ]
    )
    await db_session.commit()

    r = app_client.get(
        "/api/export/audit.csv",
        headers={
            "Authorization": "Bearer test-admin-jwt"
        },  # DASHBOARD_AUTH_ENABLED=false in tests bypasses the JWT validation
    )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert 'attachment; filename="audit-' in r.headers["content-disposition"]

    rows = _parse_csv(r.text)
    assert {r["action"] for r in rows} == {"config.update", "budget.create"}
    # Header sanity
    assert "ts" in rows[0]
    assert "actor_sub" in rows[0]
    assert "before_json" in rows[0]


@pytest.mark.asyncio
async def test_audit_export_tenant_filter(app_client, db_session: AsyncSession) -> None:
    db_session.add_all(
        [
            _audit(action="x", tenant=T("acme")),
            _audit(action="x", tenant=T("other")),
        ]
    )
    await db_session.commit()

    r = app_client.get(
        f"/api/export/audit.csv?tenant_id={T('acme')}",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    rows = _parse_csv(r.text)
    assert all(row["tenant_id"] == T("acme") for row in rows)
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_audit_export_window_must_be_ascending(app_client) -> None:
    r = app_client.get(
        "/api/export/audit.csv?since="
        + quote("2026-05-03T00:00:00+00:00")
        + "&until="
        + quote("2026-05-01T00:00:00+00:00"),
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 400
    assert "until must be > since" in r.text


@pytest.mark.asyncio
async def test_audit_export_window_max_enforced(app_client) -> None:
    # URL-encode so the `+00:00` timezone doesn't get parsed as a space.
    long_ago = quote((datetime.now(UTC) - timedelta(days=MAX_WINDOW_DAYS + 5)).isoformat())
    r = app_client.get(
        f"/api/export/audit.csv?since={long_ago}",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 400
    assert "window" in r.text.lower()


@pytest.mark.asyncio
async def test_audit_export_pagination_does_not_truncate(
    app_client, db_session: AsyncSession
) -> None:
    """Insert MAX_PAGE_SIZE + 50 rows; verify we get them all."""
    n = MAX_PAGE_SIZE + 50
    rows = [_audit(action=f"a-{i:04d}", days_ago=1) for i in range(n)]
    db_session.add_all(rows)
    await db_session.commit()

    r = app_client.get(
        "/api/export/audit.csv",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    parsed = _parse_csv(r.text)
    assert len(parsed) == n


# ---------------------------------------------------------------------------
# Billing export
# ---------------------------------------------------------------------------


async def _insert_unpriced(db_session: AsyncSession, tenant: str) -> None:
    """Insert a cost_event with cost_usd=NULL.

    The CostEvent ORM column has ``default=0``, so passing
    ``cost_usd=None`` to the constructor does NOT result in NULL after
    flush — SQLAlchemy fills in 0. To exercise the unpriced filter we
    need a real NULL, so insert via raw SQL.
    """
    await db_session.execute(
        text(
            "INSERT INTO billing_cost_events "
            "(id, tenant_id, tenant_name, service_key, provider, model, "
            " served_model, input_tokens, output_tokens, "
            " cache_creation_input_tokens, cache_read_input_tokens, "
            " cost_usd, created_at) "
            "VALUES (gen_random_uuid(), CAST(:t AS uuid), :t, 'svc', 'p', 'm', 'm', "
            " 100, 50, 0, 0, NULL, now() - interval '1 day')"
        ),
        {"t": tenant},
    )
    await db_session.commit()


@pytest.mark.asyncio
async def test_billing_export_default_excludes_unpriced(
    app_client, db_session: AsyncSession
) -> None:
    db_session.add_all(
        [
            _cost(days_ago=1, tenant=T("t-a"), cost_usd=Decimal("0.005")),
            _cost(days_ago=1, tenant=T("t-b"), cost_usd=Decimal("0.010")),
        ]
    )
    await db_session.commit()
    await _insert_unpriced(db_session, T("t-a"))

    r = app_client.get(
        "/api/export/billing.csv",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    rows = _parse_csv(r.text)
    assert len(rows) == 2
    assert all(row["cost_usd"] for row in rows)


@pytest.mark.asyncio
async def test_billing_export_include_unpriced_opt_in(app_client, db_session: AsyncSession) -> None:
    db_session.add(_cost(days_ago=1, tenant=T("t-a"), cost_usd=Decimal("0.005")))
    await db_session.commit()
    await _insert_unpriced(db_session, T("t-a"))

    r = app_client.get(
        "/api/export/billing.csv?include_unpriced=true",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    rows = _parse_csv(r.text)
    assert len(rows) == 2
    # Unpriced row's cost_usd cell is empty string
    assert any(row["cost_usd"] == "" for row in rows)


@pytest.mark.asyncio
async def test_billing_export_tenant_filter(app_client, db_session: AsyncSession) -> None:
    db_session.add_all(
        [
            _cost(days_ago=1, tenant=T("t-a")),
            _cost(days_ago=1, tenant=T("t-b")),
        ]
    )
    await db_session.commit()

    r = app_client.get(
        f"/api/export/billing.csv?tenant_id={T('t-a')}",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    rows = _parse_csv(r.text)
    assert all(row["tenant_id"] == T("t-a") for row in rows)
    assert len(rows) == 1
    # Tenant-scoped filename
    assert f"billing-{T('t-a')}-" in r.headers["content-disposition"]


@pytest.mark.asyncio
async def test_billing_export_pagination_does_not_truncate(
    app_client, db_session: AsyncSession
) -> None:
    n = MAX_PAGE_SIZE + 25
    # Spread timestamps across a microsecond-distinct window so the
    # composite (created_at, id) cursor advances cleanly.
    base = datetime.now(UTC) - timedelta(days=2)
    db_session.add_all(
        [
            CostEvent(
                tenant_id=T("bulk"),
                tenant_name="bulk",
                license_id=None,
                subject_id=None,
                app_id=None,
                service_key="svc",
                provider="p",
                model="m",
                served_model="m",
                input_tokens=1,
                output_tokens=1,
                cache_creation_input_tokens=0,
                cache_read_input_tokens=0,
                cost_usd=Decimal("0.0001"),
                cost_usd_hint=None,
                pricing_id=None,
                provider_request_id=None,
                usage_extra=None,
                latency_ms=10,
                extra_metadata=None,
                correlation_id=None,
                created_at=base + timedelta(microseconds=i),
            )
            for i in range(n)
        ]
    )
    await db_session.commit()

    r = app_client.get(
        "/api/export/billing.csv",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    parsed = _parse_csv(r.text)
    assert len(parsed) == n


@pytest.mark.asyncio
async def test_billing_export_csv_cells_handle_quoting(
    app_client, db_session: AsyncSession, monkeypatch
) -> None:
    """A tenant_name with a comma must be CSV-quoted, not split into 2 cells.

    NOTE: this used to seed a literal ``"evil,tenant"`` as the tenant_id as
    well, so the comma-escaping property was asserted on BOTH cells. Since
    ``0011_tenant_id_is_a_uuid`` the id column is a real Postgres ``uuid`` and
    cannot hold a comma or a quote at all — id-side escaping is no longer
    representable, and the id assertion below is now only a row-identity check.
    The escaping coverage is therefore carried entirely by ``tenant_name``,
    which is still free text and still carries both a comma AND a double quote
    (the harder of the two cases).
    """
    tenant = T("evil,tenant")
    row = _cost(days_ago=1, tenant=tenant, cost_usd=Decimal("0.001"))
    # Overwrite the display name (``_cost`` mirrors it from the id) with the
    # hostile literal — this is the cell the escaping assertion rides on.
    row.tenant_name = 'evil"tenant,quoted'
    db_session.add(row)
    await db_session.commit()

    # ⭐ THE HOSTILE NAME HAS TO COME FROM THE REGISTRY, NOT THE ROLLUP COLUMN, and
    # that is a correction rather than a workaround. `resolve_tenant_name`'s
    # documented order is (1) the name licences reported, (2) `CostDaily.tenant_name`,
    # (3) the uuid — so in PRODUCTION, where `GET /internal/tenants` answers 200, the
    # exported cell is the REGISTRY's name and step 2 is unreachable for any
    # registered tenant. This test asserted step 2 and passed only because the suite
    # had no registry at all to out-rank it, which the 2026-09-13 conftest fixture
    # (added for `assert_tenant_invoiceable`) supplies. Putting the hostile literal
    # where production reads it from keeps the escaping property and moves it onto
    # the path that actually renders the cell.
    real_get = httpx.AsyncClient.get

    async def _get(self, url, *a, **kw):  # noqa: ANN001, ANN202
        if str(url).endswith("/internal/tenants"):
            return httpx.Response(
                200,
                json=[
                    {
                        "id": tenant,
                        "name": 'evil"tenant,quoted',
                        "kind": "internal",
                        "archived": False,
                    }
                ],
            )
        return await real_get(self, url, *a, **kw)

    monkeypatch.setattr(httpx.AsyncClient, "get", _get)
    from services import tenant_names as _tn

    _tn.reset_cache()

    r = app_client.get(
        "/api/export/billing.csv",
        headers={"Authorization": "Bearer test-admin-jwt"},
    )
    assert r.status_code == 200
    rows = _parse_csv(r.text)
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == T("evil,tenant")
    assert rows[0]["tenant_name"] == 'evil"tenant,quoted'


# ---------------------------------------------------------------------------
# Billing export: tenant_name is a NAME, not the uuid echoed back
# ---------------------------------------------------------------------------


class TestBillingExportResolvesTenantName:
    """The CSV column labelled ``tenant_name`` must not contain a tenant uuid.

    Measured in prod 2026-09-05: ``billing_cost_events.tenant_name`` equals the
    tenant uuid on 29,883 of 29,883 rows. That is not corruption — the emitters
    write the uuid there deliberately when the uuid is all they know
    (``services._parsers.original_tenant_label``, via ``cost_flusher`` and
    ``event_flusher``). ``/dashboard/costs`` has always resolved it through
    :func:`services.tenant_names.resolve_tenant_name`; this export read the raw
    column, so the same ledger rendered a human name on one surface and a uuid
    on the other — and the uuid is the one that went to customers.

    The blocker recorded in ``tenant_names``' docstring ("the endpoint answers
    billing with 401 in prod") is stale: measured from inside
    ``deploy/billing-service`` on 2026-09-05, ``GET /internal/tenants`` with
    billing's own service-account JWT returns **200**, and 14 of the 16 distinct
    tenant_ids in ``billing_cost_events`` resolve to a real name.
    """

    @pytest.mark.asyncio
    async def test_billing_export_renders_the_resolved_name(
        self, app_client, db_session: AsyncSession, monkeypatch
    ) -> None:
        """A tenant licences can name renders as that name, not as its uuid."""
        tenant = T("named-tenant")
        db_session.add(_cost(tenant=tenant))
        await db_session.commit()

        async def _names(*_a, **_kw):
            return {tenant.lower(): "acme-corp"}

        monkeypatch.setattr("routers.export.get_tenant_names", _names)

        r = app_client.get(
            "/api/export/billing.csv",
            headers={"Authorization": "Bearer test-admin-jwt"},
        )
        assert r.status_code == 200
        rows = _parse_csv(r.text)
        assert len(rows) == 1
        assert rows[0]["tenant_name"] == "acme-corp"
        # The uuid still has a column of its own — resolving the label does not
        # cost the export its join key.
        assert rows[0]["tenant_id"] == tenant

    @pytest.mark.asyncio
    async def test_billing_export_falls_back_to_uuid_when_licences_has_no_name(
        self, app_client, db_session: AsyncSession, monkeypatch
    ) -> None:
        """An unresolvable tenant keeps today's output byte-for-byte.

        ``resolve_tenant_name`` discards a stored name that merely equals the
        uuid, so this row has nothing left but the uuid — which is exactly what
        the column contained before this change. The fix can only ever improve a
        cell, never blank one.
        """
        tenant = T("unknown-tenant")
        db_session.add(_cost(tenant=tenant))
        await db_session.commit()

        async def _empty(*_a, **_kw):
            return {}

        monkeypatch.setattr("routers.export.get_tenant_names", _empty)

        r = app_client.get(
            "/api/export/billing.csv",
            headers={"Authorization": "Bearer test-admin-jwt"},
        )
        assert r.status_code == 200
        rows = _parse_csv(r.text)
        assert rows[0]["tenant_name"] == tenant

    @pytest.mark.asyncio
    async def test_billing_export_survives_a_raising_resolver(
        self, app_client, db_session: AsyncSession, monkeypatch
    ) -> None:
        """A licences failure costs the export its labels, never its 200.

        This is the load-bearing half. ``get_tenant_names`` is documented as
        never raising, but the export is a compliance surface pulled under
        deadline: it must not acquire a new way to fail because a display column
        got nicer. Without the ``except Exception`` in
        ``routers.export._billing_tenant_names`` this raises out of the handler
        and the auditor gets a 500 instead of a CSV.
        """
        tenant = T("resolver-boom")
        db_session.add(_cost(tenant=tenant))
        await db_session.commit()

        async def _boom(*_a, **_kw):
            raise RuntimeError("licences is down")

        monkeypatch.setattr("routers.export.get_tenant_names", _boom)

        r = app_client.get(
            "/api/export/billing.csv",
            headers={"Authorization": "Bearer test-admin-jwt"},
        )
        assert r.status_code == 200
        rows = _parse_csv(r.text)
        assert rows[0]["tenant_name"] == tenant
