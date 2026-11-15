"""Guard: no test engine may REPLACE the inherited ``PGOPTIONS``.

The class this closes
---------------------
``tests/conftest.py`` plumbs the schema selection for a non-``public``
``TEST_SCHEMA`` through the libpq ``PGOPTIONS`` environment variable. libpq's
explicit ``options`` *connection parameter* does not merge with ``PGOPTIONS`` —
it replaces it wholesale. So an engine built with a bare literal::

    create_async_engine(URL, connect_args={"options": "-c timezone=Europe/Amsterdam"})

silently discards ``-c search_path=<TEST_SCHEMA>`` (and, in CI, the archetype's
``-c statement_timeout=...``), lands in ``public``, and every query in that
fixture fails with ``UndefinedTable`` — a message that names the table, never
the real cause. Two fixtures did exactly this from #49 (2026-07-23) until this
guard, breaking three tests under any non-``public`` ``TEST_SCHEMA``.

This module has three parts, and each is written so it can actually fail:

1. ``test_scan_is_not_vacuous`` — positive control. A scan that matches nothing
   is green for free. This asserts the scan really found engine constructions
   AND really found ``options`` connect_args, in the files that are known to
   have them.
2. ``test_no_engine_replaces_inherited_pgoptions`` — the guard proper.
3. the two behavioural tests — live proof against Postgres that
   ``pgoptions_with`` preserves the inherited settings and that the bare form
   does not. These make the libpq claim above falsifiable rather than folklore.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.conftest import TEST_DATABASE_URL, pgoptions_with

TESTS_ROOT = Path(__file__).resolve().parent

#: The sanctioned composer. An ``options`` connect_arg must be built by it.
SANCTIONED_HELPER = "pgoptions_with"

#: Engine constructors whose ``connect_args`` we police.
ENGINE_FACTORIES = frozenset({"create_engine", "create_async_engine"})

#: Files excluded from the scan. Exactly one: this module deliberately builds a
#: bare-``options`` engine as the CONTROL that proves libpq's replacement
#: semantics. ``test_scan_skip_list_is_minimal_and_justified`` asserts both that
#: this is the only exemption and that the exempted file really does contain the
#: bare construction it is exempted for — so the hole cannot silently widen.
SCAN_SKIPS = frozenset({"test_pgoptions_inheritance.py"})


@dataclass(frozen=True)
class OptionsSite:
    """One place in ``tests/`` that sets a libpq ``options`` connect param."""

    path: Path
    lineno: int
    compliant: bool
    source: str

    @property
    def where(self) -> str:
        return f"{self.path.relative_to(TESTS_ROOT.parent)}:{self.lineno}"


def _is_sanctioned(node: ast.AST) -> bool:
    """True iff the value is a call to :func:`pgoptions_with`."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == SANCTIONED_HELPER
    if isinstance(func, ast.Attribute):
        return func.attr == SANCTIONED_HELPER
    return False


def _options_value(dict_node: ast.Dict) -> ast.expr | None:
    """Return the value bound to an ``"options"`` key, if present."""
    for key, value in zip(dict_node.keys, dict_node.values, strict=True):
        if isinstance(key, ast.Constant) and key.value == "options":
            return value
    return None


def _scan_file(path: Path) -> tuple[list[OptionsSite], int]:
    """Scan one file. Returns (options sites, engine-construction count).

    Two prongs, because ``connect_args`` need not be an inline literal:

    * prong A — an engine factory call carrying a literal ``connect_args={...}``
      dict with an ``"options"`` key;
    * prong B — ANY dict literal in the file with an ``"options"`` key whose
      value is a string starting with ``-c ``. This catches the obvious evasion
      of building the dict in a local variable first.

    Prong B may double-count a site already found by prong A; sites are
    de-duplicated by (path, lineno) in :func:`scan_tests_tree`.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    lines = path.read_text(encoding="utf-8").splitlines()
    sites: list[OptionsSite] = []
    engine_calls = 0

    def _record(value: ast.expr) -> None:
        sites.append(
            OptionsSite(
                path=path,
                lineno=value.lineno,
                compliant=_is_sanctioned(value),
                source=lines[value.lineno - 1].strip() if value.lineno <= len(lines) else "",
            )
        )

    for node in ast.walk(tree):
        # prong A
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            if name in ENGINE_FACTORIES:
                engine_calls += 1
                for kw in node.keywords:
                    if kw.arg == "connect_args" and isinstance(kw.value, ast.Dict):
                        value = _options_value(kw.value)
                        if value is not None:
                            _record(value)
        # prong B
        if isinstance(node, ast.Dict):
            value = _options_value(node)
            if (
                value is not None
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
                and value.value.lstrip().startswith("-c ")
            ):
                _record(value)

    return sites, engine_calls


def scan_tests_tree() -> tuple[list[OptionsSite], int, list[Path]]:
    """Scan every ``.py`` under ``tests/``. Returns (sites, engine calls, files)."""
    files = sorted(p for p in TESTS_ROOT.rglob("*.py") if p.name not in SCAN_SKIPS)
    seen: dict[tuple[Path, int], OptionsSite] = {}
    engine_calls = 0
    for path in files:
        sites, calls = _scan_file(path)
        engine_calls += calls
        for site in sites:
            seen[(site.path, site.lineno)] = site
    return sorted(seen.values(), key=lambda s: (str(s.path), s.lineno)), engine_calls, files


# ---------------------------------------------------------------------------
# 1. positive control — the scan must not be vacuously green
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_scan_is_not_vacuous():
    """The scanner must actually see the code it claims to police.

    A guard that matches zero sites passes for free and would keep passing if
    the AST shapes it looks for ever stopped matching (an import-alias change, a
    SQLAlchemy 2.x factory rename, a walk that silently returns nothing). Pin
    the floors to what is really in the tree.
    """
    sites, engine_calls, files = scan_tests_tree()

    assert len(files) >= 40, f"scanned only {len(files)} test files — tree walk is broken"
    assert engine_calls >= 3, (
        f"scanner found {engine_calls} create_(async_)engine calls under tests/; "
        "expected at least 3 (conftest's session engine + the two Amsterdam "
        "fixtures). A count this low means the AST match stopped working."
    )
    assert len(sites) >= 2, (
        f"scanner found {len(sites)} libpq `options` connect_args under tests/; "
        "expected at least 2. Zero or one means this guard is not actually "
        "looking at the sites it exists for."
    )

    found_in = {s.path.name for s in sites}
    for expected in ("test_rollup_timezone.py", "test_access_charge.py"):
        assert expected in found_in, (
            f"{expected} no longer shows an `options` connect_arg to the scanner. "
            f"Either the Amsterdam fixture was removed (then drop it from this "
            f"list) or the scan regressed. Sites seen: {sorted(found_in)}"
        )


@pytest.mark.unit
def test_scan_skip_list_is_minimal_and_justified():
    """The one exempted file must exist and must really need the exemption."""
    assert {"test_pgoptions_inheritance.py"} == SCAN_SKIPS, (
        "the scan exemption list grew. Every entry is a place the guard cannot "
        f"see; justify it here or remove it. Got: {sorted(SCAN_SKIPS)}"
    )
    me = TESTS_ROOT / "test_pgoptions_inheritance.py"
    assert me.exists()
    my_sites, _ = _scan_file(me)
    assert any(not s.compliant for s in my_sites), (
        "this module is exempted from the scan because it builds a deliberate "
        "bare-`options` engine as the libpq control — but no such construction "
        "was found, so the exemption is unjustified. Remove it from SCAN_SKIPS."
    )


# ---------------------------------------------------------------------------
# 2. the guard proper
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_no_engine_replaces_inherited_pgoptions():
    """Every ``options`` connect_arg under ``tests/`` must compose, not replace."""
    sites, _, _ = scan_tests_tree()
    offenders = [s for s in sites if not s.compliant]
    assert not offenders, (
        "libpq's `options` connect parameter REPLACES the PGOPTIONS env var, it "
        "does not merge with it. These sites therefore discard conftest's "
        f"`-c search_path=<TEST_SCHEMA>` and break under any non-public "
        f"TEST_SCHEMA. Build the string with `{SANCTIONED_HELPER}(...)` from "
        "tests/conftest.py instead:\n" + "\n".join(f"  {s.where}: {s.source}" for s in offenders)
    )


# ---------------------------------------------------------------------------
# 3. behavioural proof against a real Postgres
# ---------------------------------------------------------------------------

_PROBE_SCHEMA = "zz_pgoptions_probe"
_PROBE_TZ = "Europe/Amsterdam"


@pytest.mark.unit
async def test_pgoptions_with_preserves_inherited_settings(monkeypatch):
    """The helper's output carries BOTH the inherited GUC and the added one.

    Connects for real: ``search_path`` never has to name an existing schema for
    Postgres to report it back, so this needs no DDL and cannot collide with a
    concurrent session.
    """
    monkeypatch.setenv("PGOPTIONS", f"-c search_path={_PROBE_SCHEMA}")

    composed = pgoptions_with(f"timezone={_PROBE_TZ}")
    assert f"search_path={_PROBE_SCHEMA}" in composed
    assert f"timezone={_PROBE_TZ}" in composed

    engine = create_async_engine(TEST_DATABASE_URL, connect_args={"options": composed})
    try:
        async with engine.connect() as conn:
            search_path = (await conn.execute(text("SHOW search_path"))).scalar_one()
            timezone = (await conn.execute(text("SHOW timezone"))).scalar_one()
    finally:
        await engine.dispose()

    assert search_path == _PROBE_SCHEMA, (
        f"the composed options string lost the inherited search_path (got {search_path!r})"
    )
    assert timezone == _PROBE_TZ, (
        f"the composed options string lost the added GUC (got {timezone!r})"
    )


@pytest.mark.unit
async def test_bare_options_really_does_replace_pgoptions(monkeypatch):
    """CONTROL: proves the failure mode this guard exists for is real.

    Without this, the whole module rests on a claim about libpq that nobody has
    checked. Here the bare form is built deliberately — which is why this file
    is in ``SCAN_SKIPS``.
    """
    monkeypatch.setenv("PGOPTIONS", f"-c search_path={_PROBE_SCHEMA}")

    # deliberately bare: no pgoptions_with, so PGOPTIONS is replaced wholesale
    bare_connect_args = {"options": f"-c timezone={_PROBE_TZ}"}
    engine = create_async_engine(TEST_DATABASE_URL, connect_args=bare_connect_args)
    try:
        async with engine.connect() as conn:
            search_path = (await conn.execute(text("SHOW search_path"))).scalar_one()
            timezone = (await conn.execute(text("SHOW timezone"))).scalar_one()
    finally:
        await engine.dispose()

    assert timezone == _PROBE_TZ, "control is broken: the bare options string did not apply at all"
    assert search_path != _PROBE_SCHEMA, (
        "libpq merged `options` with PGOPTIONS — the premise of this guard no "
        f"longer holds (search_path={search_path!r}). Re-derive before relaxing "
        "anything."
    )


@pytest.mark.unit
def test_pgoptions_with_is_clean_when_nothing_is_inherited(monkeypatch):
    """No inherited PGOPTIONS ⇒ no leading whitespace, no empty ``-c``."""
    monkeypatch.delenv("PGOPTIONS", raising=False)
    assert pgoptions_with(f"timezone={_PROBE_TZ}") == f"-c timezone={_PROBE_TZ}"

    monkeypatch.setenv("PGOPTIONS", "   ")
    assert pgoptions_with("a=1", "b=2") == "-c a=1 -c b=2"

    monkeypatch.setenv("PGOPTIONS", "-c statement_timeout=300000")
    assert pgoptions_with("a=1") == "-c statement_timeout=300000 -c a=1"


@pytest.mark.unit
def test_conftest_does_not_claim_ci_sets_test_schema():
    """The docstring must not resurrect the false 'CI sets TEST_SCHEMA' claim.

    It sent readers straight into the bug: CI never sets TEST_SCHEMA (the
    archetype exports PG_SCHEMA=public unconditionally and gives each job an
    ephemeral DATABASE), so the broken path only ever bit local devs, silently.
    """
    conftest_src = (TESTS_ROOT / "conftest.py").read_text(encoding="utf-8")
    docstring = ast.get_docstring(ast.parse(conftest_src)) or ""
    assert docstring, "tests/conftest.py lost its module docstring"
    assert "ci_run_" not in docstring, (
        "tests/conftest.py again claims CI sets a per-run TEST_SCHEMA. It does "
        "not: gitops-production/.github/workflows/python-service.yml:1364 "
        "echoes PG_SCHEMA=public and TEST_SCHEMA appears nowhere in it."
    )
    assert "metering_ci" not in docstring, (
        "the shared metering_ci database is decommissioned; CI uses a per-job "
        "ephemeral postgres:18 container."
    )
    assert "PG_SCHEMA=public" in docstring, (
        "the docstring should state what CI actually does so the next reader "
        "can check it against the archetype."
    )
