"""DERIVE, from source, every licence-row field billing reads.

Why this exists
---------------
``CONSUMED_FIELDS`` started life hand-maintained, and it drifted before its own
PR was even merged: `app.id` and `app.name` (``routers/budget.py:1016-1017``)
were missing, so renaming either in licences left the whole gate green — 19
passed, 0 red, twice — while live traffic would have written ``app_name=NULL``
into ``request_quota_config`` and left events unattributed. That is the shape of
``licenses#171``, which has already hit this exact field pair once.

A gate whose completeness depends on someone remembering to extend a list has
the failure mode the gate exists to remove. So the list is no longer the source
of truth: this module parses the four consumer modules and extracts the reads,
and ``tests/test_licences_contract.py`` asserts the derived set and the declared
table agree IN BOTH DIRECTIONS. A new production read that nobody declared is
red; a declared entry nobody reads any more is red.

⛔ WHAT THIS EXTRACTOR CANNOT SEE — READ BEFORE TRUSTING A GREEN RUN
--------------------------------------------------------------------
"Which fields does production read" is not fully decidable by static analysis,
and three successive audits each found a hole one layer up. Rather than widen
indefinitely, the blind spots are enumerated here. A green gate means "none of
the ORDINARY drift shapes are present", not "no consumer exists anywhere":

1. **A consumer reached through a further wrapper.** Discovery keys on calling
   ``fetch_licenses`` or naming ``/internal/licenses``. If someone wraps the
   fetcher in ``get_active_licences()`` and callers use only THAT, those callers
   are not derived. (The wrapper itself is, so its own reads are seen.)
2. **``getattr(lic, ...)`` / ``lic[key]`` / dynamic attribute access.** Only
   ``.get("literal")`` chains are parsed. ``lic.get(FIELD)`` with a variable key,
   ``getattr``, ``operator.itemgetter`` and ``**kwargs`` splats are invisible.
   DECLARED OUT OF SCOPE, not overlooked.
3. **A row variable not in :data:`ROW_VARS`.** Fail-loud, not silent: the module
   raises :class:`ExtractorBlindError`. The exception is a module in
   :data:`CONDUIT_EXEMPTIONS`, where a rename WOULD pass unnoticed — which is
   why that dict has one entry and a written reason.
4. **Reads in a module that neither calls the fetcher nor names the endpoint**,
   e.g. one handed a licence row as a plain function argument from elsewhere.
5. **Anything outside ``src/``** — scripts, alembic revisions, notebooks.
6. **Runtime shape.** This checks the SCHEMA licences publishes, not what the
   service actually returns; a service that contradicts its own OpenAPI is
   caught only by the opt-in live probe.
7. **Four alias-binding spellings, INSIDE what this parser claims to handle.**
   Found by the round-4 audit; latent today (zero hits across all six derived
   consumers) but silent, and unlike 1-6 these are plain ``.get("literal")``
   chains the extractor is supposed to see:

   - an alias assigned two or more nesting levels BELOW its read — ``ast.walk``
     is breadth-first, not source order, so the binding is visited after the use;
   - the walrus, ``plan := lic.get("plan")`` — ``ast.NamedExpr`` is not
     ``ast.Assign``;
   - the annotated assignment, ``plan: dict = lic.get("plan")`` — ``ast.AnnAssign``
     is unhandled;
   - tuple unpacking, ``plan, app = lic.get("plan"), lic.get("app")``.

   ⚠️ Also: the alias map is MODULE-GLOBAL and never scoped per function, so an
   unrelated local named ``plan`` or ``lic`` yields a false read. That direction
   fails loud (a declared field nothing reads reds the both-directions check),
   which is why it is recorded rather than fixed here.

If a future audit finds a hole in this list, that is the list working. The bar
this gate has to clear is the status quo it replaced — a test that ran in zero
environments — not omniscience.

What it understands
-------------------
The four call sites use exactly four spellings, all of them ``.get()`` on a
licence row or on something taken out of one::

    lic.get("id", "")                            -> id
    lic.get("plan", {})            (assigned)    -> plan          (alias `plan`)
    lic.get("app") or {}           (assigned)    -> app           (alias `app`)
    lic_data.get("app", {}).get("key", ...)      -> app.key
    plan.get("token_budget_monthly")             -> plan.token_budget_monthly

so it tracks assignment aliases: a name bound to a path under a licence row
becomes a root for further ``.get()`` calls.

⛔ THE ONE HAND-MAINTAINED INPUT IS :data:`ROW_VARS` — the variable names that
hold a licence row. That is deliberately the smallest possible surface, and it
is FAIL-LOUD rather than fail-quiet: :func:`consumed_reads` raises if any
consumer module yields ZERO reads, so renaming ``lic`` to something else breaks
the build with "the extractor is blind" instead of silently reporting an empty
set and passing. A guard that reports zero because it was blinded is
indistinguishable from a guard that found nothing wrong.
"""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

#: The function every indirect consumer goes through.
_FETCHER = "fetch_licenses"

#: The endpoint every direct consumer names.
_ENDPOINT = "/internal/licenses"


def _fetcher_local_names(tree: ast.AST) -> set[str]:
    """Local names bound to ``fetch_licenses`` in this module, aliases included.

    ⚠️ MATCHING THE LITERAL SPELLING WAS NOT ENOUGH. ``from services.billing
    import fetch_licenses`` was detected; ``... import fetch_licenses as _fetch``
    was NOT, and a consumer importing it under any other name was invisible to
    the whole gate. The binding is resolved from the module's own imports
    instead, so the local name it is called by does not matter.
    """
    names = {_FETCHER}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == _FETCHER:
                    names.add(alias.asname or alias.name)
    return names


def _is_fetcher_call(node: ast.AST, local_names: set[str] | None = None) -> bool:
    """A real CALL to ``fetch_licenses`` — never a mention of its name.

    ⚠️ Deliberately AST, not grep. ``services/tenant_names.py`` names
    ``fetch_licenses`` four times in prose and calls it zero times; a text search
    puts it on the consumer list, where it contributes no reads and would then
    trip the blindness check. A substring of a dump cannot tell a call from a
    mention.

    Attribute calls match on the attribute alone (``sb.fetch_licenses(...)``),
    which is already immune to aliasing the MODULE; ``local_names`` is what makes
    it immune to aliasing the FUNCTION.
    """
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr == _FETCHER
    if isinstance(func, ast.Name):
        return func.id in (local_names if local_names is not None else {_FETCHER})
    return False


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """``id()`` of every Constant that is a docstring, so prose is not evidence."""
    out: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = getattr(node, "body", [])
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                out.add(id(body[0].value))
    return out


def _names_the_endpoint(tree: ast.AST) -> bool:
    """A non-docstring string literal naming the endpoint.

    ``services/seats.py`` names it only in its module docstring, to say it must
    NOT count it — the exact opposite of being a consumer.
    """
    skip = _docstring_nodes(tree)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and _ENDPOINT in node.value
            and id(node) not in skip
        ):
            return True
    return False


def consumer_modules(root: Path | None = None) -> tuple[str, ...]:
    """Every module under ``src/`` that consumes the licences response — DERIVED.

    ⚠️ THIS USED TO BE A HAND-WRITTEN TUPLE OF FOUR, AND IT WAS INCOMPLETE.
    ``routers/quota.py`` calls ``fetch_licenses`` (:178) and reads
    ``plan.get("id")`` (:196) for the live ``GET /quota/status/{tenant_id}``
    route; it was not on the list, so dropping ``PlanInfo.id`` from the schema
    passed the whole gate — 24 passed, 1 skipped, identical to baseline — while
    live traffic would fall back to ``NULL_PLAN_ID`` for every licence of every
    tenant. ``services/invoices.py`` (:1872) was missing too.

    ⭐ AND `ExtractorBlindError` COULD NOT HAVE CAUGHT IT: that check only fires
    for modules already ON the list. Closing the "field we forgot" hole left the
    "module we forgot" hole, and the guard built for the first is structurally
    blind to the second. So the list is derived too, and the last
    hand-maintained input is gone.
    """
    base = root or _REPO_ROOT
    src = base / "src"
    found: list[str] = []
    for path in sorted(src.rglob("*.py")):
        tree = ast.parse(path.read_text())
        local = _fetcher_local_names(tree)
        if any(_is_fetcher_call(n, local) for n in ast.walk(tree)) or _names_the_endpoint(tree):
            found.append(path.relative_to(base).as_posix())
    return tuple(found)


#: Variable names that hold a whole licence row (one element of the response).
ROW_VARS = frozenset({"lic", "lic_data", "licence", "license"})


class ExtractorBlindError(AssertionError):
    """A consumer module yielded zero licence-row reads.

    Raised, never returned as "no reads". Zero is the reading a renamed row
    variable produces, and it is also the reading a module with genuinely no
    reads produces — but all four modules in :data:`CONSUMER_MODULES` are on
    this list precisely because they DO read licence rows, so zero can only mean
    the extractor stopped seeing them.
    """


def _const_str(node: ast.expr) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _path_of(node: ast.expr, aliases: dict[str, str]) -> str | None:
    """The dotted licence-row path ``node`` evaluates to, or ``None``.

    ``""`` means "the licence row itself" — the root every path extends.
    """
    if isinstance(node, ast.Name):
        if node.id in ROW_VARS:
            return ""
        return aliases.get(node.id)
    if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
        # `lic.get("app") or {}` — the fallback says nothing about the contract.
        return _path_of(node.values[0], aliases)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr != "get" or not node.args:
            return None
        key = _const_str(node.args[0])
        if key is None:
            return None
        base = _path_of(node.func.value, aliases)
        if base is None:
            return None
        return f"{base}.{key}" if base else key
    return None


def _reads_in(source: str, label: str) -> dict[str, list[str]]:
    tree = ast.parse(source)
    aliases: dict[str, str] = {}
    found: dict[str, list[str]] = {}

    # One ordered pass: an alias is always assigned before it is read.
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            path = _path_of(node.value, aliases)
            if path:
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        aliases[target.id] = path
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            path = _path_of(node, aliases)
            if path:
                found.setdefault(path, []).append(f"{label}:{node.lineno}")
    return found


#: Modules that provably consume the licences response but read no field from it.
#:
#: ⛔ A NAMED EXEMPTION, ONE ENTRY, WITH ITS REASON — NOT A GENERAL RULE.
#:
#: This replaces a `_forwards_the_payload()` heuristic that waived the reads
#: requirement for any module passing the bound payload into ANY call. That was
#: the defect it was written to avoid: it counted `len(rows)` and
#: `logger.info(..., rows)` as forwards, so ONE LOG LINE made a module reading
#: licence fields on every row invisible to the entire gate. Measured, one line
#: apart: reads with no log -> ExtractorBlindError at collection; the same reads
#: plus `logger.info(..., len(rows))` -> 26 passed, zero red.
#:
#: A general loosening exempts everything nobody thought of. A named exemption
#: exempts exactly one thing and shows the next reader why. The pattern is
#: `ci/external-probe-exempt.txt` — 1 of 51, with its justification written out.
#:
#: ⚠️ This waives the requirement that the MODULE yield reads. It never waives
#: checking the reads themselves: every module on the derived list is always
#: parsed, and anything it does read is still asserted against the declared
#: table. `test_conduit_exemptions_are_still_earned` fails if an exempted module
#: starts reading fields, so the entry cannot quietly outlive its reason.
CONDUIT_EXEMPTIONS: dict[str, str] = {
    "src/services/invoices.py": (
        "close_period fetches the licences payload at :1872 and hands it straight "
        "to _capture_inputs and generate_billing without indexing a single field "
        "of it — deliberately, so the frozen invoice inputs and the billing "
        "snapshot are labelled from ONE fetch rather than two that could disagree "
        "across a blip. It must stay on the derived consumer list (it is how a "
        "reader downstream of it stays discoverable), but it has no reads of its "
        "own to find, and demanding some would be a permanent false alarm."
    ),
}


def consumed_reads(root: Path | None = None) -> dict[str, list[str]]:
    """``{dotted path: ["file:line", ...]}`` for every licence-row read in source.

    Modules come from :func:`consumer_modules`, so a newly-added consumer is
    scanned without anyone remembering to list it.
    """
    base = root or _REPO_ROOT
    modules = consumer_modules(base)
    if not modules:
        raise ExtractorBlindError(
            f"found NO licences consumers under {base / 'src'}. Either the tree moved or "
            f"the detector broke; an empty module list would make this gate vacuous."
        )
    out: dict[str, list[str]] = {}
    for rel in modules:
        reads = _reads_in((base / rel).read_text(), rel)
        if not reads and rel not in CONDUIT_EXEMPTIONS:
            raise ExtractorBlindError(
                f"{rel} consumes the licences response but yields NO field read. The row "
                f"variable was probably renamed — extend ROW_VARS (currently "
                f"{sorted(ROW_VARS)}). If it genuinely reads nothing, add it to "
                "CONDUIT_EXEMPTIONS with a written reason; do not loosen this check. "
                "Reporting an empty set here would make this gate silently vacuous."
            )
        for key, sites in reads.items():
            out.setdefault(key, []).extend(sites)
    return {k: sorted(v) for k, v in sorted(out.items())}
