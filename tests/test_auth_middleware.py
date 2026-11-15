"""Guardrail tests for metering's auth surface.

NOTE: metering_service is mid-conversion from gateway-only ext_authz to
fleet-standard in-service license-token middleware (see plan
``done-summary-of-iterative-zebra.md`` Tier D #12). Phase 1 (this PR)
adds the canonical middleware/envelope/handler stack; Phase 2 will
introduce ``src.auth.middleware.AuthMiddleware`` (license-token
introspection) and convert ``auth_deps.require_tenant`` from
header-reads to ``request.state.tenant_id``.

For Phase 1, this file exercises the existing ``Depends``-based auth
helpers — ``require_admin`` (Keycloak dashboard auth) and
``require_tenant`` (X-Tenant-Id header from ext_authz) — so the
guardrail is real and not a placeholder. Phase 2 swaps the assertions
to the AuthMiddleware shape and adds introspection-cache coverage.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException, Request

from auth_deps import require_tenant
from tests.conftest import T


@pytest.fixture
def request_with_header() -> Request:
    """Synthesize a Request with X-Tenant-Id pre-set by ext_authz."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/usage",
        "headers": [(b"x-tenant-id", T("mw-tenant").encode())],
        "query_string": b"",
    }
    return Request(scope)


@pytest.fixture
def request_without_tenant() -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/usage",
        "headers": [],
        "query_string": b"",
    }
    return Request(scope)


async def test_require_tenant_admits_when_header_present(
    request_with_header: Request,
) -> None:
    """A valid X-Tenant-Id header from ext_authz is admitted."""
    result = await require_tenant(request_with_header)
    # "tenant-uuid-1" used to pass here — a label that merely LOOKS
    # uuid-shaped. tenant_id is a real uuid column now, so the header
    # has to carry one; ext_authz sends the resolved id, never a name.
    assert result == T("mw-tenant")


async def test_require_tenant_rejects_when_header_missing_and_jwt_invalid(
    monkeypatch: pytest.MonkeyPatch,
    request_without_tenant: Request,
) -> None:
    """Without an X-Tenant-Id header AND with admin-JWT verification
    raising, require_tenant must wrap the inner 401 in a canonical
    'Tenant context required' error."""
    import auth_deps

    async def _fail_verify(_request: Request) -> dict[str, str]:
        raise HTTPException(status_code=401, detail="No admin JWT")

    monkeypatch.setattr(auth_deps, "verify_admin_token", _fail_verify)
    with pytest.raises(HTTPException) as exc_info:
        await require_tenant(request_without_tenant)
    assert exc_info.value.status_code == 401
    assert "tenant" in str(exc_info.value.detail).lower()


def test_security_headers_middleware_handles_dashboard_csp() -> None:
    """Dashboard pages need a permissive CSP allowing the HTMX CDN. The
    default profile would block it."""
    from middleware.security_headers import _headers_for

    dashboard = _headers_for("/dashboard/")
    api = _headers_for("/api/usage")

    assert "https://unpkg.com" in dashboard["Content-Security-Policy"]
    assert "frame-ancestors 'self'" in dashboard["Content-Security-Policy"]
    assert "frame-ancestors 'none'" in api["Content-Security-Policy"]
