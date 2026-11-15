"""Billing-side half of the licenses ↔ billing request-contract handshake.

``nonagenticai/licenses`` vendors five of this repo's request schemas into its
``tests/support/billing_contract.json`` and validates every body it POSTs here
against that copy. Its freshness check can only diff against this repo when a
checkout of it is present — never on a licenses CI runner — so a schema change
made HERE used to leave that copy stale with nothing anywhere going red
(measured 2026-10-21: 8 ``pattern`` entries, a ``per_seat`` cadence and two
``MarkupRuleCreate`` properties behind).

The only CI that always sees this repo's schemas is this repo's. So the pin
lives here: ``docs/consumer_contracts/licenses.json`` records the sha256 of each
schema licenses vendors, and this test recomputes it from ``docs/openapi.json``
(which ``test_openapi_drift.py`` ties to the live app). Change one of those
schemas and this test fails on YOUR PR, naming the consumer to refresh. It has
no skip path: everything it reads is in this repo.

licenses records the same digests in its vendored copy's
``_meta.schema_sha256`` and, whenever both checkouts are present, asserts the
two files agree — so the pin cannot quietly move without its consumer.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
_SPEC_PATH = _ROOT / "docs" / "openapi.json"
_PIN_PATH = _ROOT / "docs" / "consumer_contracts" / "licenses.json"


def schema_digest(schema: dict[str, Any]) -> str:
    """Canonical digest. MUST match licenses' ``billing_contract.schema_digest``."""
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _drift(pins: dict[str, str], schemas: dict[str, Any]) -> list[str]:
    problems = []
    for name, pinned in sorted(pins.items()):
        if name not in schemas:
            problems.append(f"{name}: licenses vendors it but it is gone from docs/openapi.json")
        elif schema_digest(schemas[name]) != pinned:
            problems.append(f"{name}: sha256 {schema_digest(schemas[name])} != pinned {pinned}")
    return problems


def _load() -> tuple[dict[str, str], dict[str, Any]]:
    entries = json.loads(_PIN_PATH.read_text())["schema_sha256"]
    pins = {entry["schema"]: entry["sha256"] for entry in entries}
    assert len(pins) == len(entries), "duplicate schema in the pin list"
    schemas = json.loads(_SPEC_PATH.read_text())["components"]["schemas"]
    return pins, schemas


def test_licenses_vendored_schemas_are_unchanged_or_their_pin_was_moved() -> None:
    pins, schemas = _load()
    # Precondition: an empty pin map would pass having checked nothing.
    assert set(pins) == {
        "AccessChargeCreate",
        "BudgetPeriod",
        "MarkupRuleCreate",
        "QuotaConfigRequest",
        "TokenBudgetCreate",
    }, sorted(pins)
    problems = _drift(pins, schemas)
    assert not problems, (
        "A request schema nonagenticai/licenses vendors has changed:\n  "
        + "\n  ".join(problems)
        + "\n"
        + json.loads(_PIN_PATH.read_text())["_meta"]["on_failure"]
    )


def test_the_pin_check_fails_when_a_vendored_schema_changes() -> None:
    """Non-vacuity: one changed ``pattern`` must be reported, by name."""
    pins, schemas = _load()
    mutated = copy.deepcopy(schemas)
    mutated["MarkupRuleCreate"]["properties"]["markup_pct"]["anyOf"][1]["pattern"] = "^x$"
    assert _drift(pins, mutated) == [
        f"MarkupRuleCreate: sha256 {schema_digest(mutated['MarkupRuleCreate'])} "
        f"!= pinned {pins['MarkupRuleCreate']}"
    ]
    del mutated["BudgetPeriod"]
    assert any(p.startswith("BudgetPeriod: licenses vendors it") for p in _drift(pins, mutated))


def test_digest_ignores_key_order_but_not_values() -> None:
    a = {"type": "string", "enum": ["a", "b"]}
    assert schema_digest(a) == schema_digest({"enum": ["a", "b"], "type": "string"})
    assert schema_digest(a) != schema_digest({"type": "string", "enum": ["b", "a"]})


# ---------------------------------------------------------------------------
# write_gates: WHO may call the two money POSTs licenses makes
# ---------------------------------------------------------------------------
#
# licenses vendors ``write_gates`` (route -> auth dependency -> role set) and
# asserts its docstrings against it. It derives that map by AST-reading THIS
# repo's ``src/routers/config.py`` + ``src/auth_deps.py``, which it can only do
# with a checkout present. Here the same facts are read from the live app on
# every run, so a changed gate or role set fails on the billing PR.

_GATED_POSTS = ("/api/config/markup-rules", "/api/config/access-charges")


def _role_constants(func: Any, module: Any) -> list[str]:
    """Union of the module-level string collections ``func``'s code references.

    Mirrors licenses' ``_dependency_roles``: a module-level set / frozenset /
    list / tuple made only of strings, referenced anywhere in the function
    (nested code objects included).
    """
    names: set[str] = set()
    stack = [func.__code__]
    while stack:
        code = stack.pop()
        names.update(code.co_names)
        stack.extend(c for c in code.co_consts if hasattr(c, "co_names"))
    roles: set[str] = set()
    for name in names:
        value = getattr(module, name, None)
        if (
            isinstance(value, set | frozenset | list | tuple)
            and value
            and all(isinstance(v, str) for v in value)
        ):
            roles |= set(value)
    return sorted(roles)


def derive_write_gates(router: Any) -> dict[str, dict[str, Any]]:
    """Read from the ``APIRouter`` itself: the app's ``routes`` list holds
    lazily-included router wrappers, not ``APIRoute`` objects."""
    import inspect

    from fastapi.routing import APIRoute

    gates: dict[str, dict[str, Any]] = {}
    for path in _GATED_POSTS:
        routes = [
            r
            for r in router.routes
            if isinstance(r, APIRoute) and r.path == path and "POST" in r.methods
        ]
        assert len(routes) == 1, f"expected exactly one POST {path}, found {len(routes)}"
        deps = [d.call for d in routes[0].dependant.dependencies if d.call is not None]
        assert deps, f"POST {path} declares no Depends(...) gate"
        dependency = deps[-1]  # licenses takes the LAST Depends in the signature
        roles = _role_constants(dependency, inspect.getmodule(dependency))
        assert len(roles) >= 2, f"POST {path}: {dependency.__name__} resolved to {roles}"
        gates[f"POST {path}"] = {"dependency": dependency.__name__, "roles": roles}
    return gates


def _pinned_write_gates_sha() -> str:
    return json.loads(_PIN_PATH.read_text())["write_gates_sha256"]


def test_the_money_write_gates_licenses_documents_are_unchanged_or_their_pin_was_moved() -> None:
    from routers.config import router

    gates = derive_write_gates(router)
    assert schema_digest(gates) == _pinned_write_gates_sha(), (
        f"the auth gate on a POST licenses documents has changed: {gates}\n"
        + json.loads(_PIN_PATH.read_text())["_meta"]["on_failure"]
    )


def test_the_write_gates_pin_fails_when_a_role_is_added() -> None:
    """Non-vacuity: one extra admitted role must move the digest."""
    from routers.config import router

    gates = derive_write_gates(router)
    mutated = copy.deepcopy(gates)
    mutated["POST /api/config/markup-rules"]["roles"].append("billing-internal")
    assert schema_digest(mutated) != _pinned_write_gates_sha()
    assert schema_digest(gates) == _pinned_write_gates_sha()
