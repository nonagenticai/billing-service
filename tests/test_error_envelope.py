"""Guardrail tests for metering's error envelope.

Phase 1 of the API compliance rollout (see plan Tier D #12) adds the
canonical fleet error envelope alongside the existing ``Envelope[T]``
success shape. Every error response leaves metering as
``{"error": {code, message, status, request_id, timestamp}}`` — matching
nlp_service, worker_management, and the rest of the fleet.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


def test_404_uses_canonical_envelope(client: TestClient) -> None:
    """An unknown path returns the canonical envelope, not raw FastAPI default."""
    resp = client.get("/this/path/does/not/exist")
    assert resp.status_code == 404
    body = resp.json()
    assert "error" in body
    err = body["error"]
    assert err["code"] == "HTTP_404"
    assert err["status"] == 404
    assert err.get("request_id"), "request_id must be populated on error envelope"
    assert err.get("timestamp"), "timestamp must be populated on error envelope"


def test_validation_error_uses_canonical_envelope() -> None:
    """A 422 validation error from RequestValidationError must use the
    canonical envelope. We construct a minimal app pinned to the same
    handler so we don't drag in metering's lifespan-dependent DB init.
    """
    import os
    import uuid as _uuid

    from fastapi import FastAPI, Query
    from fastapi.exceptions import RequestValidationError

    # Reuse the canonical handler directly so the test pins its envelope shape.
    from main import _validation_exception_handler  # type: ignore[attr-defined]

    iso_app = FastAPI()
    iso_app.add_exception_handler(RequestValidationError, _validation_exception_handler)  # ty: ignore[invalid-argument-type]

    @iso_app.get("/echo")
    async def _echo(n: int = Query(...)) -> dict[str, int]:
        return {"n": n}

    iso_client = TestClient(iso_app)
    resp = iso_client.get("/echo?n=not-an-int")
    assert resp.status_code == 422
    body = resp.json()
    assert "error" in body
    err = body["error"]
    assert err["code"] == "VALIDATION_ERROR"
    assert err["status"] == 422
    assert err.get("request_id") or not os.environ.get("FAIL_ON_MISSING_RID")
    # request_id may be empty because RequestIdMiddleware isn't wired into
    # this minimal app, but the envelope SHAPE is what we're pinning.
    assert isinstance(err.get("timestamp"), str)
    # request_id is uuid7 when present
    rid = err.get("request_id")
    if rid:
        _uuid.UUID(rid)  # parses


# --- 422 detail payload ---------------------------------------------------
#
# Until 2026-08-11 the validation handler computed `str(exc.errors())` and then
# threw it away: `_error_envelope` only attaches `details` when `leak_details`
# or the `LEAK_EXCEPTION_DETAILS` env flag is on, and this call passed neither
# while the flag is false in production. Every 422 billing returned said only
# "Request validation failed". Reproduced live on
# https://fr.nonagentic.ai/settings/admin/pricing: an operator saving an empty
# markup rule could not be told which field to set. These tests pin the payload
# that gives the UI something to render, and — just as importantly — pin what
# must NEVER appear in it.

# A value that appears NOWHERE in the app or its error messages, so finding it
# in a response can only mean the handler echoed the caller's submitted body.
_SENTINEL = "WSD-SENTINEL-9c3f-do-not-echo"

# A credential-SHAPED tenant_id carrying a token unique to this test run.
#
# Since the tenant-uuid change a body `tenant_id` is resolved by the `TenantId` /
# `OptionalTenantId` BeforeValidator in models.domain, so an UNRESOLVABLE value
# now fails as a pydantic **422** rather than the old 400 — which routes it
# through _validation_exception_handler, where pydantic attaches the rejected
# value as `input`. A `tenant_id` has been observed in production carrying a raw
# bearer token, so this is precisely where a credential would surface the day
# `_sanitised_validation_details` stops dropping `input`/`ctx`. Built here from a
# fresh uuid4 — never a real credential, and unique so a match can only mean the
# response echoed THIS request.
_CANARY = f"cnry{uuid4().hex}"
_CREDENTIAL_TENANT_ID = f"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.{_CANARY}.c2ln"


@pytest.fixture
def pricing_writer(monkeypatch):
    """Force require_pricing_writer to a known admin payload.

    The markup-rules route depends on it, and FastAPI resolves dependencies
    before returning body-validation errors — an unauthenticated call 403s and
    never reaches the 422 path under test. Mirrors `admin_actor` in
    tests/test_markup_routes.py.
    """
    import auth
    import auth_deps

    fake_payload = {"sub": "test-admin-user", "realm_access": {"roles": ["license-admin"]}}

    async def _fake_decode(_request):
        return fake_payload

    monkeypatch.setattr(auth, "DASHBOARD_AUTH_ENABLED", True)
    monkeypatch.setattr(auth, "_decode_bearer_token", _fake_decode)
    monkeypatch.setattr(auth_deps, "_decode_bearer_token", _fake_decode)
    return fake_payload


def _post_markup(app_client: TestClient, body: dict):
    return app_client.post(
        "/api/config/markup-rules",
        headers={"Authorization": "Bearer fake"},
        json=body,
    )


def _post_credential_tenant(app_client: TestClient):
    """POST a body whose ONLY fault is a credential-shaped tenant_id.

    `markup_pct` is valid and satisfies `_require_an_economic_field`, so the
    model-level validator never runs and `tenant_id` is the single error — which
    is what lets the non-vacuity control below assert an exact detail entry.
    """
    return _post_markup(app_client, {"tenant_id": _CREDENTIAL_TENANT_ID, "markup_pct": "10"})


@pytest.mark.unit
class TestValidationErrorDetails:
    """The exact case reproduced in the browser: POST /api/config/markup-rules {}."""

    async def test_model_level_error_reaches_the_caller(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        resp = _post_markup(app_client, {})
        assert resp.status_code == 422, resp.text
        err = resp.json()["error"]
        assert err["code"] == "VALIDATION_ERROR"

        details = err.get("details")
        assert isinstance(details, list), (
            f"error.details must be a JSON array, got {type(details).__name__}: {details!r}"
        )
        assert len(details) == 1
        entry = details[0]
        assert set(entry) == {"field", "message", "type"}, (
            f"details entries carry exactly field/message/type, got {sorted(entry)}"
        )
        # A whole-body error: loc is exactly ("body",), so field is the empty
        # string. This is the contract fr renders against — the key is ALWAYS
        # present and "" means "about the request as a whole", never absent.
        assert entry["field"] == ""
        assert entry["type"] == "value_error"
        # The message the operator actually needs, which used to be discarded.
        assert "at least one economic field" in entry["message"]
        assert "markup_pct" in entry["message"]
        assert "surcharge_input_per_1m_usd" in entry["message"]

    async def test_field_level_error_names_the_field(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        resp = _post_markup(app_client, {"surcharge_input_per_1m_usd": "-1"})
        assert resp.status_code == 422, resp.text
        details = resp.json()["error"]["details"]
        assert isinstance(details, list) and len(details) == 1
        entry = details[0]
        # The leading "body" element of pydantic's loc is stripped, so the
        # field is the name the caller put in its JSON — not "body.surcharge_…".
        assert entry["field"] == "surcharge_input_per_1m_usd"
        assert entry["type"] == "greater_than_equal"
        assert "greater than or equal to 0" in entry["message"]

    async def test_every_field_error_is_reported_not_just_the_first(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        """A form with two bad fields must name both, or the operator fixes one
        at a time through two more round trips."""
        resp = _post_markup(
            app_client,
            {"surcharge_input_per_1m_usd": "-1", "surcharge_output_per_1m_usd": "-2"},
        )
        assert resp.status_code == 422, resp.text
        fields = {e["field"] for e in resp.json()["error"]["details"]}
        assert fields == {"surcharge_input_per_1m_usd", "surcharge_output_per_1m_usd"}

    # --- Negative controls: what must never come back ---------------------

    async def test_model_level_error_does_not_echo_the_submitted_body(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        """Positive-control form: a sentinel we PUT IN the request must not come back.

        pydantic attaches the whole submitted dict as `input` on a model-level
        error, so a naive passthrough of `exc.errors()` would reflect every
        field the caller sent — on this service, money configuration.
        """
        resp = _post_markup(app_client, {"notes": _SENTINEL, "tenant_id": _SENTINEL})
        assert resp.status_code == 422, resp.text
        assert _SENTINEL not in resp.text, (
            "the submitted body was echoed back in the 422 response: " + resp.text
        )
        for entry in resp.json()["error"]["details"]:
            assert "input" not in entry
            assert "ctx" not in entry
            assert "url" not in entry

    async def test_field_level_error_does_not_echo_the_rejected_value(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        """The rejected value itself is `input` on a field-level error.

        `ctx` here holds ``{"ge": Decimal("0")}`` — not JSON-serialisable, so
        passing it through would raise *inside* the error handler.
        """
        resp = _post_markup(
            app_client,
            {"tenant_id": _SENTINEL, "surcharge_input_per_1m_usd": "-777.111222"},
        )
        assert resp.status_code == 422, resp.text
        assert _SENTINEL not in resp.text
        assert "-777.111222" not in resp.text, (
            "the rejected value was echoed back in the 422 response: " + resp.text
        )
        for entry in resp.json()["error"]["details"]:
            assert set(entry) == {"field", "message", "type"}

    # --- The tenant_id credential canary ----------------------------------
    #
    # A body tenant_id is now resolved by a BeforeValidator, so an unresolvable
    # one is a 422 instead of a 400 — safe ONLY because the handler reduces every
    # pydantic error to {field, message, type} and drops `input`/`ctx`. A careful
    # reviewer of that change measured the opposite and reported the token WAS
    # echoed; it is not (raw `exc.errors()` carries it, the response does not).
    # That a careful reader reached the wrong conclusion is why the property is
    # pinned here rather than described in a docstring.

    async def test_credential_shaped_tenant_id_is_never_echoed(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        resp = _post_credential_tenant(app_client)
        assert resp.status_code == 422, resp.text
        # The WHOLE serialised body, not just `details`: a leak could surface in
        # `message`, or under a key nobody thought to look at.
        body = resp.text
        for forbidden, what in (
            (_CANARY, "the canary token"),
            (_CANARY[:16], "the canary's 16-character prefix"),
            ("Bearer", "the credential scheme word"),
            (_CREDENTIAL_TENANT_ID, "the whole submitted tenant_id"),
        ):
            assert forbidden not in body, f"{what} was echoed in the 422 response body: {body}"

    async def test_credential_refusal_still_names_the_field_and_the_reason(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        """Non-vacuity control for the canary test above.

        Emptying `details` would silence the canary AND silently undo the whole
        point of the handler — the state main was in before 2026-08-11, when
        every 422 said only "Request validation failed" and the field-level
        reasons were computed and discarded. The caller must still be told WHICH
        field was wrong and WHY.
        """
        resp = _post_credential_tenant(app_client)
        assert resp.status_code == 422, resp.text
        err = resp.json()["error"]
        assert err["code"] == "VALIDATION_ERROR"

        details = err.get("details")
        assert isinstance(details, list), (
            f"error.details must be a JSON array, got {type(details).__name__}: {details!r}"
        )
        assert len(details) == 1, f"tenant_id is the only bad field, got {details!r}"
        entry = details[0]
        assert entry["field"] == "tenant_id", (
            f"the caller must be told which field was rejected, got {entry!r}"
        )
        assert entry["message"].strip(), "the reason must not be blank"
        assert entry["message"] != err["message"], (
            "a detail that only repeats the envelope's generic "
            f"{err['message']!r} tells the caller nothing new"
        )
        assert entry["type"].strip(), "the machine-readable error type must not be blank"

    async def test_credential_refusal_details_have_exactly_the_three_keys(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        """Shape, not substring: a change that starts passing pydantic's `input`
        (or `ctx`, or `url`) through is caught here even if the value it carries
        happens not to match the canary."""
        resp = _post_credential_tenant(app_client)
        assert resp.status_code == 422, resp.text
        for entry in resp.json()["error"]["details"]:
            assert set(entry) == {"field", "message", "type"}, (
                f"details entries carry exactly field/message/type, got {sorted(entry)}"
            )

    async def test_nothing_is_written_on_a_rejected_body(
        self, app_client: TestClient, db_engine, pricing_writer
    ) -> None:
        """Detail-rendering must not have turned a rejection into an accept."""
        from sqlalchemy import select
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        from models.db import MarkupRule

        assert _post_markup(app_client, {}).status_code == 422
        factory = async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as s:
            assert list((await s.execute(select(MarkupRule))).scalars().all()) == []


@pytest.mark.unit
class TestValidationDetailSanitiser:
    """Direct tests of the reducer, for cases a real route cannot easily produce."""

    def test_details_are_capped_with_a_visible_marker(self) -> None:
        from main import _MAX_VALIDATION_DETAILS, _sanitised_validation_details

        raw = [
            {"type": "missing", "loc": ("body", f"f{i}"), "msg": "Field required", "input": {}}
            for i in range(_MAX_VALIDATION_DETAILS + 7)
        ]
        out = _sanitised_validation_details(raw)
        assert len(out) == _MAX_VALIDATION_DETAILS + 1
        assert [e["field"] for e in out[:-1]] == [f"f{i}" for i in range(_MAX_VALIDATION_DETAILS)]
        marker = out[-1]
        assert marker["type"] == "too_many_errors"
        assert marker["field"] == ""
        assert "7 further validation error(s) were suppressed" in marker["message"]

    def test_no_marker_when_exactly_at_the_cap(self) -> None:
        from main import _MAX_VALIDATION_DETAILS, _sanitised_validation_details

        raw = [
            {"type": "missing", "loc": ("body", f"f{i}"), "msg": "m"}
            for i in range(_MAX_VALIDATION_DETAILS)
        ]
        out = _sanitised_validation_details(raw)
        assert len(out) == _MAX_VALIDATION_DETAILS
        assert all(e["type"] != "too_many_errors" for e in out)

    def test_nested_body_locations_become_a_dotted_path(self) -> None:
        from main import _sanitised_validation_details

        out = _sanitised_validation_details(
            [{"type": "missing", "loc": ("body", "ids", 3, "name"), "msg": "Field required"}]
        )
        assert out[0]["field"] == "ids.3.name"

    def test_non_body_locations_keep_their_prefix(self) -> None:
        """Only a leading "body" is stripped: a bad query param `n` is not the
        same thing as a body field `n`, and the caller needs to know which."""
        from main import _sanitised_validation_details

        out = _sanitised_validation_details(
            [{"type": "int_parsing", "loc": ("query", "n"), "msg": "bad int"}]
        )
        assert out[0]["field"] == "query.n"

    def test_every_value_is_a_string(self) -> None:
        """JSONResponse serialises with json.dumps; a stray Decimal or enum in
        the payload would raise from inside the error handler."""
        import json

        from main import _sanitised_validation_details

        out = _sanitised_validation_details(
            [{"type": "greater_than_equal", "loc": ("body", "x"), "msg": "m", "ctx": {"ge": 0}}]
        )
        assert all(isinstance(v, str) for e in out for v in e.values())
        json.dumps(out)  # must not raise


@pytest.mark.unit
class TestOtherHandlersUnchanged:
    """WS-D touches the RequestValidationError handler ONLY."""

    def test_404_still_carries_no_details(self, client: TestClient) -> None:
        resp = client.get("/this/path/does/not/exist")
        assert resp.status_code == 404
        err = resp.json()["error"]
        assert err["code"] == "HTTP_404"
        assert "details" not in err

    def test_string_detail_http_exception_still_carries_no_details(
        self, client: TestClient
    ) -> None:
        """A 405 raises StarletteHTTPException with a STRING detail, which the
        HTTP handler folds into `message` and does not attach as `details`."""
        resp = client.delete("/api/config/markup-rules")
        assert resp.status_code == 405
        err = resp.json()["error"]
        assert err["code"] == "HTTP_405"
        assert "details" not in err
        assert err["message"]

    def test_leak_details_flag_still_gates_the_5xx_handlers(self, monkeypatch) -> None:
        """`leak_details=True` was added to the 422 handler only — the env flag
        that gates the server-exception handlers must be untouched.
        """
        import main

        monkeypatch.setattr(main, "_LEAK_DETAILS", False)
        body = main._error_envelope(
            code="DATABASE_UNAVAILABLE",
            message="Database temporarily unavailable",
            status_code=503,
            request_id="rid",
            details="connection to server at ... failed",
        )
        assert "details" not in body["error"], (
            "a 5xx handler leaked its exception string with LEAK_EXCEPTION_DETAILS off"
        )

        monkeypatch.setattr(main, "_LEAK_DETAILS", True)
        leaked = main._error_envelope(
            code="DATABASE_UNAVAILABLE",
            message="Database temporarily unavailable",
            status_code=503,
            request_id="rid",
            details="connection to server at ... failed",
        )
        assert leaked["error"]["details"] == "connection to server at ... failed", (
            "LEAK_EXCEPTION_DETAILS=true must still opt the 5xx handlers in"
        )
