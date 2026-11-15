"""Guard: ``@pytest.mark.nil_tenant`` stays on exactly the tests that earn it.

The class this closes
---------------------
``tests/conftest.py::_nil_tenant_tripwire`` fails any DB test that leaves rows
on ``NIL_TENANT_ID`` — the sink ``src/services/_parsers.py::_normalize_tenant_id``
drops an unresolvable tenant label into. That sink is why a MISSED ``T(label)``
conversion is green: the row lands on the nil tenant instead of the tenant the
test names, and every assertion still passes.

``@pytest.mark.nil_tenant`` switches the tripwire off for one test. It exists
because a small, enumerated set of tests legitimately assert the sink itself —
see ``EXPECTED_MARKED``, which was derived by arming the tripwire and running
the whole suite, not by reading. The failure mode
of any opt-out marker is that it becomes the fix: the next agent hits the
tripwire on a genuinely mis-converted literal, adds the marker, and the guard is
silently back to zero coverage on that test. Growth of the marker set must
therefore be a deliberate edit to this file, reviewable on its own.

Two parts, both written so they can actually fail:

1. ``test_scan_is_not_vacuous`` — positive control. An AST scan that parses
   nothing, or whose decorator shapes stopped matching, finds zero markers and
   passes for free. Pin the floors.
2. ``test_nil_tenant_marker_set_is_exactly_the_sanctioned_set`` — the guard
   proper, in both directions: nothing added, and neither sanctioned test lost
   its marker (which would flip it to failing under the tripwire for a reason
   nobody would connect to this file).

Shape copied from ``tests/test_pgoptions_inheritance.py``.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

TESTS_ROOT = Path(__file__).resolve().parent

#: The marker under audit.
MARKER = "nil_tenant"

#: The ONLY tests allowed to carry it, as ``(file name, function name)``.
#:
#: Every entry asserts the nil-sink contract DIRECTLY — the sink is the
#: subject, not an accident of an unconverted literal. Adding an entry means
#: claiming the same of another test. Do not add one to silence the tripwire on
#: a test whose real problem is a tenant literal that is not a uuid — wrap that
#: literal in ``T("label")`` instead.
#:
#: The set was derived by MEASUREMENT, not by reading: the full suite was run
#: with the tripwire armed and every test it caught was inspected. Four of the
#: six were found that way; the initial hand-picked list of two was wrong.
EXPECTED_MARKED = frozenset(
    {
        # No tenant_id in the buffer entry at all: must persist as
        # NIL_TENANT_ID with the honest "unknown" kept in the free-text
        # tenant_name column (the 0011 / tenancy.py contract change away from
        # minting a fake "unknown" tenant).
        ("test_event_flusher.py", "test_flush_events_defaults_for_missing_fields"),
        # The tenant_name fallback test: an UNRESOLVABLE label is its subject.
        # tenant_id sinks to NIL while the human label survives in the free-text
        # tenant_name column — the 0011 contract, asserted directly.
        ("test_tenant_name.py", "test_tenant_name_falls_back_to_tenant_id_when_absent"),
        # ADDED 2026-09-05, both USAGE-stream (`flush_events`) and both asserting
        # the sink itself rather than tripping over it. Their subject is
        # `billing_tenant_caller_unasserted_total` and its deprecated alias
        # `billing_tenant_unattributed_total`, and that counter fires ON the
        # `tenant_invalid` branch — i.e. it fires if and only if the label was
        # unresolvable and the row therefore sank to NIL_TENANT_ID. The nil row
        # is not incidental to these tests, it is the precondition they exist to
        # exercise: wrapping the literal in T("label") would resolve the tenant,
        # the branch would not run, and both tests would assert nothing.
        #
        # This is the same reason the two survivors above earn it and the four
        # cost-stream entries lost it in 2026-08-26: the usage stream still HAS a
        # nil sink. `_cost_schema.validate_cost_event` dead-letters an
        # unresolvable tenant on the cost stream, which is precisely why the
        # matching cost-side counter (billing_tenant_unattributed_persisted_total)
        # is asserted to stay at ZERO and needs no marker at all.
        ("test_event_flusher.py", "test_both_names_move_together_on_a_non_uuid_tenant"),
        ("test_event_flusher.py", "test_the_alias_is_exact_over_a_mixed_batch"),
        # ADDED with the #135 fix. USAGE-stream, and the sink is its precondition
        # rather than an unconverted literal: a leaked auth header is
        # unresolvable, so tenant_id sinks to NIL while the human label must
        # still survive REDACTED in tenant_name. That redaction path is what the
        # #135 change could most easily have collapsed into the new "store an
        # honest absence" path, so it is asserted directly. Wrapping the literal
        # in T("label") would make the tenant resolvable and the test vacuous.
        ("test_tenant_name.py", "test_a_redacted_credential_label_still_survives"),
    }
)

# SET SHRANK on 2026-08-26, from six entries to two. All four removed entries
# were COST-stream tests, and they were removed because the nil sink they
# asserted no longer exists on that stream, not to weaken the guard:
# ``_cost_schema.validate_cost_event`` now dead-letters an absent / unresolvable
# / nil / credential tenant, so a cost event can no longer reach the flusher's
# INSERT with the nil tenant at all. Each test was rewritten to assert the
# quarantine instead, writes no nil-tenant row, and therefore no longer earns an
# opt-out — leaving the marker on would have disabled the tripwire for free.
#
#   test_cost_flusher.py::test_flush_costs_tenant_defaults
#       → test_flush_costs_missing_tenant_dropped_without_dlq
#   test_cost_flusher.py::test_bearer_token_tenant_id_is_redacted_to_sentinel
#       → test_bearer_token_tenant_id_is_dead_lettered_and_redacted
#   test_cost_flusher.py::test_nil_uuid_tenant_id_is_unattributed_no_billing
#       → test_nil_uuid_tenant_id_is_dead_lettered_not_billed
#   test_tenant_name.py::test_cost_tenant_name_falls_back_to_tenant_id
#       (kept its name; now uses a REGISTERED legacy slug, which bills normally)
#
# The two survivors are both USAGE-stream (``flush_events``) tests. That stream
# is deliberately unchanged, so its nil sink — and this opt-out — is still real.


@dataclass(frozen=True)
class MarkedTest:
    """One test function decorated with ``@pytest.mark.<MARKER>``."""

    path: Path
    lineno: int
    func: str

    @property
    def key(self) -> tuple[str, str]:
        return (self.path.name, self.func)

    @property
    def where(self) -> str:
        return f"{self.path.relative_to(TESTS_ROOT.parent)}:{self.lineno} {self.func}"


def _decorator_marker_name(node: ast.expr) -> str | None:
    """Return the pytest marker name a decorator expression applies, if any.

    Handles all three spellings a marker can take::

        @pytest.mark.nil_tenant           # Attribute
        @pytest.mark.nil_tenant(...)      # Call wrapping an Attribute
        @mark.nil_tenant                  # `from pytest import mark`
    """
    if isinstance(node, ast.Call):
        node = node.func
    if not isinstance(node, ast.Attribute):
        return None
    parent = node.value
    # `<something>.mark.<name>` — accept any module alias for pytest itself.
    if isinstance(parent, ast.Attribute) and parent.attr == "mark":
        return node.attr
    if isinstance(parent, ast.Name) and parent.id == "mark":
        return node.attr
    return None


def _scan_file(path: Path) -> tuple[list[MarkedTest], int, int]:
    """Scan one file. Returns (marked tests, test-function count, marker count).

    ``marker count`` counts EVERY pytest marker decorator seen, not just
    ``MARKER`` — it is what makes the non-vacuity control meaningful. If the
    decorator AST shapes above ever stop matching (an import-alias change, a
    pytest API change), that count collapses to zero and the control fires,
    instead of this guard reporting "no marked tests" and going green.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    marked: list[MarkedTest] = []
    test_funcs = 0
    markers_seen = 0

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not node.name.startswith("test_"):
            continue
        test_funcs += 1
        for deco in node.decorator_list:
            name = _decorator_marker_name(deco)
            if name is None:
                continue
            markers_seen += 1
            if name == MARKER:
                marked.append(MarkedTest(path=path, lineno=node.lineno, func=node.name))

    return marked, test_funcs, markers_seen


def scan_tests_tree() -> tuple[list[MarkedTest], int, int, list[Path]]:
    """Scan every ``.py`` under ``tests/``. Returns (marked, tests, markers, files)."""
    files = sorted(TESTS_ROOT.rglob("*.py"))
    marked: list[MarkedTest] = []
    test_funcs = 0
    markers_seen = 0
    for path in files:
        file_marked, funcs, markers = _scan_file(path)
        marked.extend(file_marked)
        test_funcs += funcs
        markers_seen += markers
    return sorted(marked, key=lambda m: m.key), test_funcs, markers_seen, files


# ---------------------------------------------------------------------------
# 1. positive control — the scan must not be vacuously green
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_scan_is_not_vacuous():
    """The scanner must really parse the tree and really see marker decorators."""
    marked, test_funcs, markers_seen, files = scan_tests_tree()

    assert len(files) >= 30, (
        f"scanned only {len(files)} files under tests/ — the tree walk is broken, "
        "so this guard is looking at almost nothing."
    )
    assert test_funcs >= 300, (
        f"scanner found {test_funcs} test functions under tests/; the suite is far "
        "larger than that. The FunctionDef/AsyncFunctionDef match has regressed."
    )
    assert markers_seen >= 300, (
        f"scanner found {markers_seen} pytest marker decorators under tests/; "
        "nearly every test in this repo carries @pytest.mark.unit, so a count "
        "this low means _decorator_marker_name stopped matching and the "
        f"{MARKER!r} scan below would find zero for the wrong reason."
    )
    assert marked, (
        f"scanner found no @pytest.mark.{MARKER} decorators at all. Either the "
        "tripwire's opt-out was removed everywhere (then delete this guard) or "
        "the scan regressed."
    )


# ---------------------------------------------------------------------------
# 2. the guard proper
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_nil_tenant_marker_set_is_exactly_the_sanctioned_set():
    """No test may opt out of the nil-tenant tripwire without landing here."""
    marked, _, _, _ = scan_tests_tree()
    found = {m.key for m in marked}

    unexpected = sorted(m.where for m in marked if m.key not in EXPECTED_MARKED)
    assert not unexpected, (
        f"@pytest.mark.{MARKER} appeared on tests that are not sanctioned for it. "
        "The marker disables the conftest tripwire that catches rows silently "
        "parked on NIL_TENANT_ID — it is not the fix for a tripwire failure. If "
        'the test seeds a readable tenant label, wrap it in T("label") from '
        "tests/conftest.py. Only if the test's SUBJECT is the nil-sink path, add "
        "it to EXPECTED_MARKED here with a justification:\n"
        + "\n".join(f"  {u}" for u in unexpected)
    )

    missing = sorted(EXPECTED_MARKED - found)
    assert not missing, (
        f"these tests are listed as needing @pytest.mark.{MARKER} but no longer "
        "carry it — they will now fail under the conftest tripwire for a reason "
        "that names neither this file nor the marker. Either restore the marker "
        f"or drop the entry from EXPECTED_MARKED: {missing}"
    )


@pytest.mark.unit
def test_marker_is_registered_in_pyproject():
    """An unregistered marker is silently inert under ``--strict-markers``.

    Without the registration the opt-out either warns and works, or (with
    strict markers) errors — both of which are discovered at the worst moment.
    """
    pyproject = (TESTS_ROOT.parent / "pyproject.toml").read_text(encoding="utf-8")
    assert f"{MARKER}:" in pyproject, (
        f"marker {MARKER!r} is not registered in pyproject.toml's "
        "[tool.pytest.ini_options] markers list."
    )
