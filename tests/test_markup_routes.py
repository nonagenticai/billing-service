"""CRUD endpoints for /api/config/markup-rules + tenant_config.billing_mode."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import auth_deps
from models.db import AuditLog, MarkupRule
from models.domain import MarkupRuleCreate
from tests.conftest import T


@pytest.fixture
def admin_actor(monkeypatch):
    """Force require_admin / require_pricing_admin to a known admin payload."""
    fake_payload = {"sub": "test-admin-user", "realm_access": {"roles": ["license-admin"]}}

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


async def _fetch_markup_rows(db_engine) -> list[MarkupRule]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        return list((await s.execute(select(MarkupRule))).scalars().all())


async def _fetch_audit(db_engine, action: str) -> list[AuditLog]:
    factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        stmt = select(AuditLog).where(AuditLog.action == action)
        return list((await s.execute(stmt)).scalars().all())


@pytest.mark.unit
class TestMarkupRuleRoutes:
    async def test_upsert_creates_and_audits(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        resp = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={
                "tenant_id": T("t1"),
                "service_key": "fr",
                "provider": "openai",
                "model": "gpt5",
                "markup_pct": "40",
                "surcharge_output_per_1m_usd": "2",
            },
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()["data"]
        assert data["provider"] == "openai"
        assert Decimal(data["markup_pct"]) == Decimal("40")
        # consumption-dominant: model16+provider8+service_key4+tenant1 = 29
        # model32 + provider16 + service_key8 + tenant1 = 57.
        # ⚠️ Was 29 before alembic 0036 left-shifted every weight above TENANT
        # to free the slot for the new `project` dimension (no integer sits
        # between 2 and 1). Monotone, so ORDERING is unchanged — and note this
        # value is PUBLISHED on the API response, so it is externally visible.
        assert data["specificity"] == 57
        assert data["effective_until"] is None

        audit = await _fetch_audit(db_engine, "config.markup_rule.upsert")
        assert len(audit) == 1
        after = audit[0].after
        assert after is not None and after["provider"] == "openai"

    async def test_upsert_closes_prior_same_tuple(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        for pct in ("20", "33"):
            resp = app_client.post(
                "/api/config/markup-rules",
                headers={"Authorization": "Bearer fake"},
                json={"tenant_id": T("t1"), "provider": "openai", "markup_pct": pct},
            )
            assert resp.status_code == 201, resp.text

        rows = [r for r in await _fetch_markup_rows(db_engine) if r.provider == "openai"]
        assert len(rows) == 2
        open_rows = [r for r in rows if r.effective_until is None]
        assert len(open_rows) == 1 and open_rows[0].markup_pct == Decimal("33")

    async def test_list_current_sorted_most_specific_first(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "markup_pct": "20"},
        )
        app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "provider": "openai", "model": "gpt5", "markup_pct": "44"},
        )
        resp = app_client.get("/api/config/markup-rules", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 200, resp.text
        rules = resp.json()["data"]
        assert rules[0]["specificity"] >= rules[-1]["specificity"]
        assert rules[0]["model"] == "gpt5"

    async def test_history_scoped_to_exact_tuple(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        for pct in ("10", "12"):
            app_client.post(
                "/api/config/markup-rules",
                headers={"Authorization": "Bearer fake"},
                json={"tenant_id": T("t1"), "provider": "openai", "markup_pct": pct},
            )
        resp = app_client.get(
            "/api/config/markup-rules/history",
            headers={"Authorization": "Bearer fake"},
            params={"tenant_id": T("t1"), "provider": "openai"},
        )
        assert resp.status_code == 200, resp.text
        hist = resp.json()["data"]
        assert len(hist) == 2  # both the closed and the open row

    async def test_non_admin_rejected(self, app_client: TestClient, db_engine, monkeypatch) -> None:
        import auth

        async def _fake_decode(_request):
            return {"sub": "tenant-user", "realm_access": {"roles": ["customer"]}}

        monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
        monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
        monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)

        resp = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "markup_pct": "10"},
        )
        assert resp.status_code == 403


@pytest.mark.unit
class TestTenantBillingMode:
    async def test_billing_mode_round_trips(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        resp = app_client.put(
            f"/api/config/tenants/{T('t1')}",
            headers={"Authorization": "Bearer fake"},
            json={"markup_pct": 25, "billing_mode": "byo_platform_fee"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["data"]["billing_mode"] == "byo_platform_fee"

        got = app_client.get(
            f"/api/config/tenants/{T('t1')}", headers={"Authorization": "Bearer fake"}
        )
        assert got.json()["data"]["billing_mode"] == "byo_platform_fee"

    async def test_billing_mode_defaults_when_omitted(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        resp = app_client.put(
            f"/api/config/tenants/{T('t2')}",
            headers={"Authorization": "Bearer fake"},
            json={"markup_pct": 10},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["data"]["billing_mode"] == "standard_markup"


@pytest.fixture
def billing_internal_actor(monkeypatch):
    """Force auth decode to a billing-internal-ONLY SA payload (no license-admin).

    Mirrors the token the licenses/n8n service accounts mint — accepted on the
    looser require_pricing_admin pricing-seed endpoints, but it must NOT satisfy
    the strict require_admin gate the money-config writes now use.
    """
    fake_payload = {"sub": "billing-sa", "realm_access": {"roles": ["billing-internal"]}}

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


@pytest.fixture
def pricing_writer_actor(monkeypatch):
    """Force auth decode to a `pricing-writer`-ONLY SA payload (no license-admin).

    Same mechanism as ``billing_internal_actor`` above, different role list — the
    licences SA once the Keycloak grant is in place. This is the OTHER half of the
    gate: the strict money-write guard must admit this identity, or the contract
    would be "license-admin only" and the `pricing-writer` role pointless.
    """
    fake_payload = {"sub": "licenses-sa", "realm_access": {"roles": ["pricing-writer"]}}

    async def _fake_decode(_request):
        return fake_payload

    import auth

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


@pytest.mark.unit
class TestMoneyConfigRequiresLicenseAdmin:
    """POST /markup-rules and /access-charges set customer-billed multipliers —
    they are NOT part of the provider-pricing seed that justified the looser
    billing-internal-tolerant guard, so `billing-internal` alone must NOT satisfy
    them (unlike the GET siblings on require_pricing_admin).

    Since 2026-08-10 the gate is require_pricing_writer — license-admin OR the
    dedicated single-purpose `pricing-writer` role, granted only to the licences
    SA so it can provision a tenant offering. That widening is deliberate and is
    covered by tests/test_pricing_write_gate.py; the assertions below are the
    unchanged #55 contract and still hold: billing-internal is refused, a genuine
    license-admin is admitted.

    ⛔ `POST /tenants/{id}/terms` joined this class 2026-09-11. It shipped on
    `require_pricing_admin` and therefore ACCEPTED billing-internal — measured live
    on billing-service's own in-pod SA token, which holds billing-internal and no
    license-admin. It writes the same two money-config tables via the same two
    upsert helpers as the POSTs above, so #55's contract always covered it; nothing
    asserted that, which is why it regressed silently for a day. It is now gated by
    require_pricing_writer and asserted here. Its sibling GET deliberately stays on
    require_pricing_admin — disclosure is not a money write."""

    async def test_markup_rules_requires_license_admin_not_billing_internal(
        self, app_client: TestClient, db_engine, billing_internal_actor
    ) -> None:
        # Mutation guard: if either route reverts to require_pricing_admin, the
        # billing-internal token is accepted (201) and these 403s fail.
        mr = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "provider": "openai", "markup_pct": "40"},
        )
        assert mr.status_code == 403, mr.text

        ac = app_client.post(
            "/api/config/access-charges",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "amount_usd": "49", "cadence": "monthly"},
        )
        assert ac.status_code == 403, ac.text

    async def test_money_config_allows_license_admin(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        """The strict gate still admits a genuine license-admin token (no regression)."""
        mr = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "provider": "openai", "markup_pct": "40"},
        )
        assert mr.status_code == 201, mr.text

        ac = app_client.post(
            "/api/config/access-charges",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "amount_usd": "49", "cadence": "monthly"},
        )
        assert ac.status_code == 201, ac.text

    async def test_terms_requires_pricing_writer_not_billing_internal(
        self, app_client: TestClient, db_engine, billing_internal_actor
    ) -> None:
        """The regression this case exists for, and the one that mattered most.

        `POST /terms` materialises a tenant's WHOLE rate card — platform fee, seat
        fee, markup, BYO markup — through `upsert_access_charge` and
        `upsert_markup_rule`, the same helpers the two POSTs above use. On
        `require_pricing_admin` this token was ACCEPTED, so billing-service's own SA
        (and the nightly pricing-seed CronJob sharing it) could set any tenant's
        fees. Mutation guard: revert the route to require_pricing_admin and this 403
        becomes a 2xx.
        """
        resp = app_client.post(
            f"/api/config/tenants/{T('t1')}/terms",
            headers={"Authorization": "Bearer fake"},
            json={"platform_fee_usd": "5000", "markup_pct": "20"},
        )
        assert resp.status_code == 403, resp.text

        # The refusal must be the GATE, not a no-op that wrote anyway: nothing may
        # have reached markup_rules. (The access-charge half is covered by the
        # status code — both writes live behind the same dependency.)
        assert await _fetch_markup_rows(db_engine) == []

    async def test_terms_allows_pricing_writer(
        self, app_client: TestClient, db_engine, pricing_writer_actor
    ) -> None:
        """The other direction: the gate must not refuse /terms' intended caller.

        ⚠️ ASSERTS THE NON-403 BOUNDARY, NOT A 201, AND DELIBERATELY SO — and the
        reason is NOT that a 201 is unreachable here. It probably is reachable: once
        the seat term is written, `_read_tenant_terms` calls
        `services.seats.get_seat_count`, which HTTP-calls the licences service;
        unreachable in this class (no `seats_say` stub — that lives in
        tests/test_commercial_terms.py), it raises `SeatCountUnavailableError`, and
        `materialise_tenant_terms` CATCHES that and still returns 201 with
        `seat_basis="unavailable"`.

        Which is exactly why this must not assert 201: that 201 is manufactured by
        the licences call FAILING, so pinning it would make an error path the
        success criterion of a ROLE test, and the assertion would keep passing for
        reasons that have nothing to do with the gate. What this class pins is the
        gate boundary; anything other than 403 proves the role check passed, which
        is the whole claim. The 403 case above is NOT weakened by this.
        """
        resp = app_client.post(
            f"/api/config/tenants/{T('t1')}/terms",
            headers={"Authorization": "Bearer fake"},
            json={"platform_fee_usd": "5000", "markup_pct": "20"},
        )
        assert resp.status_code != 403, resp.text
        # Nor may it be refused as unauthenticated — that would pass the line above
        # while still locking the intended caller out.
        assert resp.status_code != 401, resp.text


@pytest.mark.unit
class TestMarkupRuleMustCarryARule:
    """A markup rule with no economic content is not a rule — it must 422.

    `POST /api/config/markup-rules` used to accept a body with **no fields at
    all** and return 201, writing a live all-NULL row. The damage is not the junk
    row itself (specificity 0 → services.markup falls through to `fallback_pct`,
    so the arithmetic is unharmed) but the **upsert side effect**: it closes the
    currently-open row for the same match tuple. An empty POST therefore
    silently retires the seeded global default, and there is no DELETE endpoint
    to undo it. Reproduced in production 2026-08-08 (markup_rules id=3).

    The sibling `POST /api/config/access-charges` already 422s the identical
    body because `AccessChargeCreate` requires `tenant_id` + `amount_usd`.
    """

    # --- RED half: bodies that carry no economic field must be refused --------

    @pytest.mark.parametrize(
        "body",
        [
            {},
            {"__probe_invalid_body__": True},
            {"tenant_id": T("t1"), "service_key": "fr", "provider": "openai", "model": "gpt5"},
            {"notes": "just a note", "effective_from": "2026-04-01T00:00:00+00:00"},
        ],
        ids=["empty_body", "prod_probe_body", "selectors_only", "metadata_only"],
    )
    async def test_rule_without_economic_field_rejected(
        self, app_client: TestClient, db_engine, admin_actor, body
    ) -> None:
        resp = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json=body,
        )
        assert resp.status_code == 422, resp.text
        # The 422 must reach the wire AND nothing may be persisted.
        assert await _fetch_markup_rows(db_engine) == []

    async def test_empty_body_cannot_close_the_open_global_default(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        """The actual production harm: an empty POST retiring the global default.

        This is the assertion that matters most — it fails on the *side effect*,
        not merely on the status code, so a validator that 422s for the wrong
        reason cannot make it pass.
        """
        seeded = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"markup_pct": "30", "notes": "global default"},
        )
        assert seeded.status_code == 201, seeded.text

        empty = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={},
        )
        assert empty.status_code == 422, empty.text

        rows = await _fetch_markup_rows(db_engine)
        assert len(rows) == 1, "the empty POST must not have written a row"
        assert rows[0].effective_until is None, "the global default was retired by an empty POST"
        assert rows[0].markup_pct == Decimal("30")

    def test_rejection_message_says_what_to_supply(self) -> None:
        """The message must name the fields a caller can send to fix the body.

        The model-level half. This used to be the ONLY half that could be
        written: main.py's `_error_envelope` dropped `details` unless
        _LEAK_DETAILS was on, so the message never left the process. Since
        2026-08-11 `_validation_exception_handler` passes `leak_details=True`
        and the same text reaches the caller over HTTP — pinned end-to-end in
        tests/test_error_envelope.py::TestValidationErrorDetails.
        """
        with pytest.raises(ValidationError) as exc:
            MarkupRuleCreate()
        rendered = str(exc.value)
        assert "markup_pct" in rendered
        assert "surcharge" in rendered

    async def test_rejection_message_reaches_the_http_caller(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        """The HTTP half of the assertion above — what the fr admin page renders.

        Reproduced live 2026-08-11 on /settings/admin/pricing: saving an empty
        markup rule showed only "Request validation failed", because the
        field-level reasons were computed and then discarded.
        """
        resp = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={},
        )
        assert resp.status_code == 422, resp.text
        details = resp.json()["error"]["details"]
        assert isinstance(details, list) and len(details) == 1
        assert "markup_pct" in details[0]["message"]
        assert "surcharge" in details[0]["message"]

    # --- GREEN half: legitimate rules must still be accepted -----------------

    async def test_fully_wildcard_rule_with_markup_pct_is_accepted(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        """A rule with NO selectors at all is legitimate — it IS the global default.

        `markup_rules` id 1 (the seeded 30% default) has every selector NULL, and
        `services.markup._specificity` scores such a row 0 precisely so it can act
        as the fall-through. Requiring a selector would forbid the global default,
        so this test is the guard against "fix" the over-strict way.
        """
        resp = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"markup_pct": "30"},
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()["data"]
        assert data["specificity"] == 0
        assert data["tenant_id"] is None
        assert data["provider"] is None
        assert Decimal(data["markup_pct"]) == Decimal("30")

    async def test_surcharge_only_rule_is_accepted(
        self, app_client: TestClient, db_engine, admin_actor
    ) -> None:
        """markup_pct is nullable by design: a rule may carry only an absolute
        surcharge (a per-token platform fee on a $0 BYO line). See
        models/db/config.py MarkupRule docstring."""
        resp = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), "surcharge_per_request_usd": "0.002"},
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()["data"]
        assert data["markup_pct"] is None
        assert Decimal(data["surcharge_per_request_usd"]) == Decimal("0.002")

    @pytest.mark.parametrize(
        "field",
        [
            "surcharge_input_per_1m_usd",
            "surcharge_output_per_1m_usd",
            "surcharge_cache_creation_per_1m_usd",
            "surcharge_cache_read_per_1m_usd",
            "surcharge_per_request_usd",
        ],
    )
    def test_zero_is_a_valid_economic_field(self, field) -> None:
        """0 is explicit ("charge nothing here"), not absent — `is not None`, not
        truthiness. A truthiness check would reject an intentional zero rule."""
        kwargs: dict[str, Any] = {field: Decimal("0")}
        rule = MarkupRuleCreate(**kwargs)
        assert getattr(rule, field) == Decimal("0")

    def test_zero_markup_pct_is_a_valid_economic_field(self) -> None:
        rule = MarkupRuleCreate(markup_pct=Decimal("0"))
        assert rule.markup_pct == Decimal("0")


@pytest.mark.unit
class TestMarkupRuleNoNegativeMoney:
    """No field on this surface may be negative.

    `compute_billed_usd` ADDS every surcharge into the billed total, so a
    negative surcharge is a silent discount — and with enough tokens, a negative
    invoice line. `markup_pct` already carried `ge=0`; the five surcharge columns
    did not. `AccessChargeCreate` bounds both its money fields (`amount_usd`,
    `included_cost_usd`) with `ge=0`, so this is a parity gap, not a new policy.
    Discounts are expressed as a lower markup_pct or an access-charge allowance.
    """

    @pytest.mark.parametrize(
        "field",
        [
            "markup_pct",
            "surcharge_input_per_1m_usd",
            "surcharge_output_per_1m_usd",
            "surcharge_cache_creation_per_1m_usd",
            "surcharge_cache_read_per_1m_usd",
            "surcharge_per_request_usd",
        ],
    )
    async def test_negative_money_rejected_at_the_wire(
        self, app_client: TestClient, db_engine, admin_actor, field
    ) -> None:
        resp = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), field: "-1"},
        )
        assert resp.status_code == 422, resp.text
        assert await _fetch_markup_rows(db_engine) == []

    @pytest.mark.parametrize(
        "field",
        [
            "markup_pct",
            "surcharge_input_per_1m_usd",
            "surcharge_output_per_1m_usd",
            "surcharge_cache_creation_per_1m_usd",
            "surcharge_cache_read_per_1m_usd",
            "surcharge_per_request_usd",
        ],
    )
    async def test_positive_money_still_accepted(
        self, app_client: TestClient, db_engine, admin_actor, field
    ) -> None:
        """The other direction: the bound must not reject a legitimate value."""
        resp = app_client.post(
            "/api/config/markup-rules",
            headers={"Authorization": "Bearer fake"},
            json={"tenant_id": T("t1"), field: "1.5"},
        )
        assert resp.status_code == 201, resp.text
        assert Decimal(resp.json()["data"][field]) == Decimal("1.5")
