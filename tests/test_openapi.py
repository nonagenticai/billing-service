"""Guardrail tests for the OpenAPI surface (canonical fleet shape).

Complements the existing ``test_openapi_drift.py`` (which pins specific
ops the FE relies on) and ``test_openapi_metadata.py`` (CORS / Bearer
auth scheme) by enforcing the **fleet standard** invariants:

- every route has an explicit ``operation_id`` (no auto-generated ids)
- operationIds are globally unique
- ``BearerAuth`` security scheme is declared
- the required tag groups exist
"""

from __future__ import annotations

import pytest

from main import app


@pytest.fixture(scope="module")
def openapi_schema() -> dict:
    app.openapi_schema = None
    return app.openapi()


def test_every_route_has_explicit_operation_id(openapi_schema: dict) -> None:
    """FastAPI's auto-generated operationId ends in ``_<method>`` — fail any."""
    auto_generated_suffixes = ("_post", "_get", "_put", "_delete", "_patch")
    offenders: list[tuple[str, str, str]] = []
    for path, item in openapi_schema.get("paths", {}).items():
        for method, op in item.items():
            if not isinstance(op, dict):
                continue
            op_id: str = op.get("operationId", "")
            if not op_id:
                offenders.append((method.upper(), path, "<empty>"))
            elif op_id.endswith(auto_generated_suffixes):
                # `_post`/`_get` suffix is the auto-fallback shape. metering
                # uses explicit ids everywhere so any such id is a regression.
                offenders.append((method.upper(), path, op_id))
    assert not offenders, f"Routes without explicit operation_id: {offenders}"


def test_operation_ids_are_unique(openapi_schema: dict) -> None:
    seen: dict[str, tuple[str, str]] = {}
    duplicates: list = []
    for path, item in openapi_schema.get("paths", {}).items():
        for method, op in item.items():
            if not isinstance(op, dict):
                continue
            op_id = op.get("operationId")
            if not op_id:
                continue
            if op_id in seen:
                duplicates.append((op_id, seen[op_id], (method.upper(), path)))
            else:
                seen[op_id] = (method.upper(), path)
    assert not duplicates, f"Duplicate operationIds: {duplicates}"


def test_bearer_auth_scheme_is_declared(openapi_schema: dict) -> None:
    schemes = openapi_schema.get("components", {}).get("securitySchemes", {})
    assert "BearerAuth" in schemes
    scheme = schemes["BearerAuth"]
    assert scheme["type"] == "http"
    assert scheme["scheme"] == "bearer"


def test_tag_groups_are_defined(openapi_schema: dict) -> None:
    tag_names = {t["name"] for t in openapi_schema.get("tags", [])}
    required = {"health", "usage", "costs", "billing", "budget", "internal", "dashboard"}
    assert required <= tag_names, f"Missing required tag groups; have {sorted(tag_names)}"
