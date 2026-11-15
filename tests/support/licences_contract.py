"""Offline contract guard for ``GET /internal/licenses`` — the licences → billing response.

Why this exists
---------------
Four production paths read fields straight out of the JSON licences returns:
``src/services/billing.py``, ``src/routers/budget.py``,
``src/services/licence_budget_reconcile.py`` and
``src/services/attribution_audit.py``.

⭐ WHICH fields, and at which lines, is NOT written down here — it is PARSED out
of those four modules by ``tests.support.licence_reads`` on every run. The first
draft of this module did hand-list them and was already wrong: it missed
``app.id`` and ``app.name`` (``routers/budget.py:1016-1017``), and its
``billing.py`` line numbers were stale because #195 had moved them. See
``REQUIRED_BY_BILLING`` below for the one thing source cannot state.

Nothing in this repo imports licences' models, so a field licences renames is a
silent behaviour change here, not an error: every read is ``.get()``, so a
missing key becomes ``None``, a skipped row, or an unpriced licence — i.e. the
estate quietly bills less, and no test anywhere goes red.

Why this guard is OFFLINE, and what it replaces
-----------------------------------------------
``tests/test_licenses_integration.py`` was the only check on this response, and
it could not fail. It ran in ZERO environments: a module-level
``skipif(not LICENSES_INTEGRATION_URL)`` whose docstring promised "a nightly
cron'd CI workflow sets the env vars" — measured on ``origin/main``,
``.github/workflows/ci-cd.yaml`` has no ``schedule:``, no cron, and the string
``LICENSES_INTEGRATION`` appears nowhere in the repo outside that one file. On
top of that it skipped on 401/403, so a rotated service-account token — which
IS the outage — silently disarmed the shape checks too.

⚠️ The ``pytest.mark.integration`` on that module is NOT a second gate in this
repo, though it looks like one. Measured: this repo's ``addopts`` carries no
``-m`` expression, and the archetype's unit pass is
``pytest ${test_dir} -v --tb=short ...``
(``gitops-production/.github/workflows/python-service.yml:2849``) with no marker
expression either, and ``ci-cd.yaml`` sets no ``integration_test_select``. The
module is COLLECTED on every run and stopped solely by the ``skipif``. One
closed gate, not two — which matters, because removing the marker would have
changed nothing.

A live check cannot be the primary guard, because every precondition it needs
is also a way for it to disarm itself. So the primary guard is this one: the
schema is VENDORED (``licences_contract.json``, refreshed by
``refresh_licences_contract.py``, source commit recorded) and there is NO skip
path. It runs on every PR, on a runner with no network and no token.

The reverse direction of exactly this pattern already exists and works:
``licenses/tests/support/billing_contract.py`` vendors billing's request
schemas for the same reason, and its docstring is the argument for vendoring.

Freshness is a SEPARATE, explicitly-skipping check
--------------------------------------------------
``test_vendored_schema_matches_sibling_checkout`` diffs the vendored copy
against ``../licenses/docs/openapi.json`` when that repo is present and fails
loudly on drift. That is the only thing here allowed to skip, and it says why.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from tests.support.licence_reads import consumed_reads

_CONTRACT_PATH = Path(__file__).with_name("licences_contract.json")
_SAMPLE_PATH = Path(__file__).with_name("licences_active_response.json")


class UncheckableSchemaError(NotImplementedError):
    """The vendored schema uses a construct this validator does not implement.

    RAISED, never returned as a violation string. An unimplemented keyword is a
    defect in the GUARD, not in the payload; conflating the two is how a guard
    reports green while checking nothing. If licences adds ``pattern``,
    ``allOf``, ``minItems`` or a ``format`` not listed below, every test using
    this module errors with "I cannot check this" until it is implemented —
    rather than silently narrowing its own scope.
    """


# Carry no validation semantics; skipping them checks nothing away.
_ANNOTATION_KEYWORDS = frozenset(
    {
        "title",
        "description",
        "default",
        "example",
        "examples",
        "deprecated",
        "readOnly",
        "writeOnly",
        "$comment",
        "discriminator",
    }
)

_CHECKED_KEYWORDS = frozenset(
    {
        "$ref",
        "anyOf",
        "oneOf",
        "type",
        "enum",
        "const",
        "format",
        "items",
        "properties",
        "required",
        "additionalProperties",
    }
)

_CHECKED_FORMATS = frozenset({"date-time", "date", "uuid"})

_JSON_TYPES: dict[str, tuple[type, ...]] = {
    "object": (dict,),
    "array": (list,),
    "string": (str,),
    "integer": (int,),
    "number": (int, float),
    "boolean": (bool,),
    "null": (type(None),),
}


# ---------------------------------------------------------------------------
# The fields production reads — DERIVED from source, not hand-listed.
# ---------------------------------------------------------------------------
#
# ⚠️ THIS TABLE USED TO CARRY THE PATHS AND THEIR LINE NUMBERS BY HAND, AND IT
# DRIFTED BEFORE ITS OWN PR MERGED. `app.id` and `app.name`
# (`routers/budget.py:1016-1017`) were missing, so renaming either in licences
# left every leg green — 19 passed, 0 red, on both mutations — while live
# traffic would write `app_name=NULL` into `request_quota_config` and leave
# events unattributed. That is `licenses#171`, which has hit this field pair
# once already. The four `billing.py` line numbers were stale too: #195 moved
# them and nothing noticed.
#
# So the SET and its PROVENANCE now come from `tests.support.licence_reads`,
# which parses the four consumer modules. This table declares only the one
# thing source cannot state: whether billing can proceed WITHOUT the field.
# `test_declared_table_matches_what_source_actually_reads` asserts the two
# agree in both directions, so a new production read that nobody declared is
# red, and a declared entry nobody reads any more is red.
REQUIRED_BY_BILLING: dict[str, bool] = {
    # Absence makes the row unusable: it is skipped or mis-attributed.
    "id": True,
    "tenant_id": True,
    "status": True,
    # Absence leaves the licence merely unpriced/unattributed, not dropped.
    "plan": False,
    "plan.code": False,
    # `routers/quota.py:196` — absent, `plan_uuid` falls to NULL_PLAN_ID for
    # every licence of every tenant in GET /quota/status/{tenant_id}.
    "plan.id": False,
    "plan.quota_monthly": False,
    "plan.token_budget_monthly": False,
    # Per-licence overrides (licences migration 041); win over the plan values
    # above via services.licence_limits.effective_limit. Optional and nullable:
    # absent (older licences deploy) or null falls back to the plan value.
    "quota_override": False,
    "token_budget_override": False,
    "app": False,
    "app.id": False,
    "app.key": False,
    "app.name": False,
}

#: The licence row fields whose ABSENCE makes the row unusable rather than
#: merely unpriced, and which licences must therefore mark ``required``.
MUST_BE_REQUIRED = tuple(name for name, required in REQUIRED_BY_BILLING.items() if required)


def consumed_fields() -> tuple[tuple[str, bool, str], ...]:
    """``(dotted path, billing-requires-it, "file:line, file:line")`` for every read.

    Paths and provenance are parsed out of the four consumer modules on every
    call, so they cannot go stale the way the hand-written list did.
    """
    reads = consumed_reads()
    return tuple(
        (path, REQUIRED_BY_BILLING.get(path, False), ", ".join(sites))
        for path, sites in reads.items()
    )


def load_contract() -> dict[str, Any]:
    """The vendored licences schemas. Never optional — a missing file is an error."""
    return json.loads(_CONTRACT_PATH.read_text())


def schema_digest(schema: Any) -> str:
    """Canonical digest. MUST match licenses' ``tests/unit/test_consumer_contract_pins.py``."""
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def contract_digests(contract: dict[str, Any]) -> dict[str, Any]:
    """``{"response_sha256": ..., "schema_sha256": [{schema, sha256}, ...]}`` recomputed
    from a contract's ``response`` + ``schemas``.

    The same shape licenses pins in ``docs/consumer_contracts/billing-service.json``
    and checks on every licenses run, so a change to what this copy mirrors fails
    THERE even though this repo's CI never sees a licenses checkout.
    """
    return {
        "response_sha256": schema_digest(contract["response"]),
        "schema_sha256": [
            {"schema": name, "sha256": schema_digest(spec)}
            for name, spec in sorted(contract["schemas"].items())
        ],
    }


def load_sample_response() -> Any:
    """The golden ``200`` body for ``GET /internal/licenses?status=active``."""
    return json.loads(_SAMPLE_PATH.read_text())


def _resolve(node: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    """Follow a ``$ref`` (one hop is all the vendored spec ever uses)."""
    ref = node.get("$ref")
    if ref is None:
        return node
    name = ref.rsplit("/", 1)[-1]
    try:
        return contract["schemas"][name]
    except KeyError as exc:  # pragma: no cover - a broken vendored file
        raise UncheckableSchemaError(f"unresolvable $ref: {ref}") from exc


def _assert_checkable(schema: dict[str, Any]) -> None:
    unknown = set(schema) - _CHECKED_KEYWORDS - _ANNOTATION_KEYWORDS
    if unknown:
        raise UncheckableSchemaError(
            f"schema keyword(s) this guard does not implement: {sorted(unknown)}"
        )


def _check_format(value: Any, fmt: str, where: str) -> list[str]:
    if fmt not in _CHECKED_FORMATS:
        raise UncheckableSchemaError(f"unimplemented format {fmt!r} at {where}")
    if not isinstance(value, str):
        return [f"{where}: format {fmt} needs a string, got {type(value).__name__}"]
    try:
        if fmt == "uuid":
            UUID(value)
        elif fmt == "date":
            date.fromisoformat(value)
        else:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return [f"{where}: not a valid {fmt}: {value!r}"]
    return []


def _violations(
    value: Any, schema: dict[str, Any], contract: dict[str, Any], where: str
) -> list[str]:
    schema = _resolve(schema, contract)
    _assert_checkable(schema)
    out: list[str] = []

    branches = schema.get("anyOf") or schema.get("oneOf")
    if branches is not None:
        # ⚠️ Report the FAILING BRANCH's own violations, not a summary line.
        # A summary ("matches none of 2 alternatives") names only the wrapper,
        # so for licences' pervasive `anyOf: [$ref PlanInfo, null]` nullable
        # shape it reports `response[0].plan` and swallows the actual defect
        # (`…plan.quota_monthly: expected integer, got str`). Found by
        # `test_a_retyped_field_is_caught`, which the summary version passed
        # only by accident of substring matching — i.e. this branch was
        # reporting a real drift under a name nobody could act on.
        attempts: list[tuple[dict[str, Any], list[str]]] = []
        for branch in branches:
            found = _violations(value, branch, contract, where)
            if not found:
                return []
            attempts.append((_resolve(branch, contract), found))
        # A bare `{"type": "null"}` alternative always "fails" for a non-null
        # value and its complaint is never the interesting one.
        informative = [f for br, f in attempts if br.get("type") != "null"] or [
            f for _br, f in attempts
        ]
        return min(informative, key=len)

    if "const" in schema and value != schema["const"]:
        out.append(f"{where}: expected const {schema['const']!r}, got {value!r}")
    if "enum" in schema and value not in schema["enum"]:
        out.append(f"{where}: {value!r} not in {schema['enum']!r}")

    declared = schema.get("type")
    if declared is not None:
        expected = _JSON_TYPES.get(declared)
        if expected is None:
            raise UncheckableSchemaError(f"unimplemented type {declared!r} at {where}")
        # bool is an int in Python; JSON does not agree.
        if isinstance(value, bool) and declared in {"integer", "number"}:
            return out + [f"{where}: expected {declared}, got boolean"]
        if not isinstance(value, expected):
            return out + [f"{where}: expected {declared}, got {type(value).__name__}"]

    if "format" in schema and value is not None:
        out += _check_format(value, schema["format"], where)

    if declared == "array" and "items" in schema:
        for index, item in enumerate(value):
            out += _violations(item, schema["items"], contract, f"{where}[{index}]")

    if isinstance(value, dict):
        props = schema.get("properties", {})
        for name in schema.get("required", []):
            if name not in value:
                out.append(f"{where}: missing required field {name!r}")
        for name, sub in props.items():
            if name in value:
                out += _violations(value[name], sub, contract, f"{where}.{name}")
        extra_schema = schema.get("additionalProperties")
        if extra_schema is False:
            for name in set(value) - set(props):
                out.append(f"{where}: unexpected field {name!r}")
        elif isinstance(extra_schema, dict):
            for name in sorted(set(value) - set(props)):
                out += _violations(value[name], extra_schema, contract, f"{where}.{name}")

    return out


def response_violations(body: Any) -> list[str]:
    """Everything wrong with the SHAPE of ``body`` as a ``GET /internal/licenses`` 200.

    ⛔ THIS DELIBERATELY SAYS NOTHING ABOUT *WHICH* LICENCES COME BACK, AND
    THAT OMISSION IS LOAD-BEARING — DO NOT "COMPLETE" IT WITH A NON-EMPTY CHECK.

    An earlier draft of this guard asserted ``body`` was non-empty, reasoning
    that zero active licences means metering bills nothing. That reasoning holds
    only under TODAY's predicate. ``internal_list_licenses`` currently filters on
    the STORED ``status`` column, while licences' own ``seat-count`` applies a
    three-part expiry predicate, so one licence can be a seat in one endpoint and
    not the other — and billing reads both for the same invoice. An open
    ``nonagenticai/licenses`` PR collapses the two onto one expiry-aware
    predicate, under which an empty active set is a legitimate answer.

    Asserting non-emptiness here would pin the looser stored-status semantics as
    the contract and make that fix read as a regression: a test encoding the bug
    as the spec, then outvoting its own fix. So the set semantics are asserted
    NOWHERE in this repo until that PR lands. Field names, types and
    required-ness hold identically under both predicates, and they are what this
    function checks.

    (``licence_budget_reconcile._fetch_licence_lifecycle`` does refuse an empty
    payload at ``src/services/licence_budget_reconcile.py:208``. That is a
    RUNTIME policy on an unscoped, all-status read — a different question from
    what the ``?status=active`` response contract permits.)
    """
    contract = load_contract()
    return _violations(body, contract["response"], contract, "response")


def schema_violations() -> list[str]:
    """Every field in :data:`CONSUMED_FIELDS` the vendored schema fails to declare."""
    contract = load_contract()
    row = _resolve(contract["response"]["items"], contract)
    out: list[str] = []

    for path, _needed, sites in consumed_fields():
        node = row
        walked = []
        for segment in path.split("."):
            node = _resolve(node, contract)
            # Step through a nullable wrapper (``anyOf: [$ref, null]``).
            if "properties" not in node:
                for branch in node.get("anyOf") or node.get("oneOf") or []:
                    resolved = _resolve(branch, contract)
                    if "properties" in resolved:
                        node = resolved
                        break
            props = node.get("properties", {})
            if segment not in props:
                out.append(
                    f"licences no longer declares {path!r} "
                    f"(missing {segment!r} under {'.'.join(walked) or 'the licence row'}); "
                    f"read by {sites}"
                )
                node = None
                break
            walked.append(segment)
            node = props[segment]
        if node is None:
            continue

    required = set(row.get("required", []))
    for name in MUST_BE_REQUIRED:
        if name not in required:
            out.append(
                f"licences no longer marks {name!r} required; every row missing it is "
                "dropped or mis-attributed by billing"
            )
    return out
