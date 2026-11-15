"""The always-on contract gate on ``GET /internal/licenses`` (licences → billing).

⛔ NOTHING IN THIS FILE MAY SKIP except ``test_vendored_schema_matches_sibling_checkout``,
and that one is a FRESHNESS check, not the contract check. Everything else runs
on a runner with no network, no licences service and no service-account token —
which is the entire reason it exists. See ``tests/support/licences_contract.py``
for why the live test it supersedes could not fail.

The gate is three-way, and each leg catches a different drift:

1. **schema ⟷ consumers** — every dotted path a production call site reads must
   be DECLARED by the vendored licences schema. Licences renaming ``tenant_id``
   reds here.
2. **sample ⟷ schema** — the golden response must validate against the vendored
   schema, so the sample cannot drift into fiction.
3. **sample ⟷ consumers** — the production reads are replayed over the golden
   response and must each yield a usable value.

Plus negative controls (below) that prove leg 2 and leg 3 can actually fail,
because a validator that returns ``[]`` for everything passes all three legs.

⛔ WHAT THIS GATE DELIBERATELY DOES NOT ASSERT: *which* licences come back.
No test here requires a non-empty set, a particular licence, or anything
expiry-dependent. ``internal_list_licenses`` filters the stored ``status``
column today while licences' ``seat-count`` applies a three-part expiry
predicate, and an open ``nonagenticai/licenses`` PR collapses the two onto one
expiry-aware predicate — under which an empty active set is legitimate. Pinning
today's cardinality would encode that mismatch as the spec and make the fix read
as a regression. Field names, types and required-ness hold under BOTH
predicates; that is the whole of what is asserted. See
``response_violations`` for the long form.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from tests.support.licence_reads import (
    CONDUIT_EXEMPTIONS,
    ExtractorBlindError,
    consumed_reads,
    consumer_modules,
)
from tests.support.licences_contract import (
    MUST_BE_REQUIRED,
    REQUIRED_BY_BILLING,
    UncheckableSchemaError,
    consumed_fields,
    contract_digests,
    load_contract,
    load_sample_response,
    response_violations,
    schema_violations,
)
from tests.support.refresh_licences_contract import (
    git_env,
    read_source_spec,
    resolved_origin_main_sha,
    source_repo,
)

pytestmark = pytest.mark.unit


def _sample() -> list[dict]:
    return load_sample_response()["body"]


def _walk(row: object, path: str) -> object:
    """Read a dotted path the way production does — ``.get()`` all the way down."""
    node = row
    for segment in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(segment)
    return node


# ---------------------------------------------------------------------------
# Leg 1 — the vendored schema still declares what billing reads
# ---------------------------------------------------------------------------


def test_vendored_schema_declares_every_field_metering_reads() -> None:
    """Licences dropping or renaming a consumed field is a red build, not an unpriced invoice."""
    assert schema_violations() == []


def test_the_fields_billing_cannot_proceed_without_are_marked_required() -> None:
    contract = load_contract()
    row = contract["schemas"]["InternalLicenseResponse"]
    missing = [name for name in MUST_BE_REQUIRED if name not in row.get("required", [])]
    assert missing == [], f"licences no longer requires {missing}"


# ---------------------------------------------------------------------------
# Leg 2 — the golden response conforms to the vendored schema
# ---------------------------------------------------------------------------


def test_sample_response_conforms_to_vendored_schema() -> None:
    assert response_violations(_sample()) == []


# ---------------------------------------------------------------------------
# Leg 3 — the production field reads survive the golden response
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "sites"),
    [(path, sites) for path, _required, sites in consumed_fields()],
)
def test_production_read_resolves_against_the_golden_response(path: str, sites: str) -> None:
    """The first golden row must supply every field, or the read silently yields ``None``."""
    value = _walk(_sample()[0], path)
    assert value is not None, f"{path!r} resolves to None; read by {sites}"


def test_an_empty_list_is_shape_valid_and_that_is_deliberate() -> None:
    """⛔ A NEGATIVE REQUIREMENT — do not "fix" this into a non-empty assertion.

    Under licences' in-flight expiry-aware predicate an empty active set is a
    legitimate answer, so the SHAPE guard must stay silent about cardinality.
    This test exists so that adding a non-empty rule to ``response_violations``
    turns red and forces the author to read why.
    """
    assert response_violations([]) == []


def test_a_non_list_body_is_a_contract_violation() -> None:
    """A paginated envelope iterates KEYS, i.e. reads as "nothing is active"."""
    assert response_violations({"items": []}), "an enveloped body must not pass"


# ---------------------------------------------------------------------------
# Negative controls — proof the validator is not an identity function
# ---------------------------------------------------------------------------


def test_a_renamed_required_field_is_caught() -> None:
    body = copy.deepcopy(_sample())
    body[0]["tenantId"] = body[0].pop("tenant_id")
    violations = response_violations(body)
    assert any("tenant_id" in v for v in violations), violations


def test_a_retyped_field_is_caught() -> None:
    body = copy.deepcopy(_sample())
    body[0]["plan"]["quota_monthly"] = "250000"
    assert any("quota_monthly" in v for v in response_violations(body)), "string quota accepted"


def test_a_plan_that_is_not_an_object_is_caught() -> None:
    body = copy.deepcopy(_sample())
    body[0]["plan"] = "pro"
    assert response_violations(body), "scalar plan accepted"


def test_the_validator_is_loud_about_keywords_it_cannot_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unimplemented keyword must ERROR, never quietly validate nothing."""
    contract = load_contract()
    contract["schemas"]["InternalLicenseResponse"]["properties"]["status"]["pattern"] = "^a.*$"
    monkeypatch.setattr(
        "tests.support.licences_contract.load_contract", lambda: contract, raising=True
    )
    with pytest.raises(UncheckableSchemaError):
        response_violations(_sample())


# ---------------------------------------------------------------------------
# The derived-vs-declared ratchet — why the table cannot drift again
# ---------------------------------------------------------------------------


def test_declared_table_matches_what_source_actually_reads() -> None:
    """⭐ The control that would have caught `app.id` / `app.name`.

    The hand-written table missed both (`routers/budget.py:1016-1017`), so
    renaming either in licences left every leg green while live traffic wrote
    `app_name=NULL` into `request_quota_config`. Now the set is parsed out of
    the four consumer modules, and both directions are asserted: an undeclared
    new read is red, a declared-but-unread entry is red.
    """
    derived = consumed_reads()
    undeclared = {p: s for p, s in derived.items() if p not in REQUIRED_BY_BILLING}
    assert not undeclared, (
        "production reads licence fields this gate does not know about — add them "
        f"to REQUIRED_BY_BILLING: {undeclared}"
    )
    unread = set(REQUIRED_BY_BILLING) - set(derived)
    assert not unread, f"declared but no longer read anywhere in src/: {sorted(unread)}"


def test_every_licences_consumer_under_src_is_discovered() -> None:
    """⭐ The control that would have caught `routers/quota.py`.

    The module list used to be a hand-written tuple of four and was incomplete:
    `quota.py` calls `fetch_licenses` (:178) and reads `plan.get("id")` (:196)
    for the live `GET /quota/status/{tenant_id}` route, so dropping
    `PlanInfo.id` passed the entire gate while live traffic fell back to
    `NULL_PLAN_ID`. `ExtractorBlindError` could never have caught that — it only
    fires for modules already ON the list.

    So the list is derived, and this asserts the derivation actually finds every
    caller rather than quietly returning a short one.
    """
    modules = consumer_modules()
    callers = {
        "src/services/billing.py",  # defines and calls fetch_licenses
        "src/routers/budget.py",
        "src/routers/quota.py",
        "src/services/invoices.py",
        "src/services/licence_budget_reconcile.py",
        "src/services/attribution_audit.py",
    }
    assert callers <= set(modules), f"derivation missed: {sorted(callers - set(modules))}"

    # Prose is not evidence: these NAME the endpoint or the fetcher and consume
    # neither. Listing them would add zero reads and trip the blindness check.
    for prose_only in ("src/services/seats.py", "src/services/tenant_names.py"):
        assert prose_only not in modules, f"{prose_only} only mentions it; it is not a consumer"


def test_the_extractor_is_not_blind() -> None:
    """Reads must actually come out, and out of the modules that do the reading."""
    derived = consumed_reads()
    reading = {site.split(":")[0] for sites in derived.values() for site in sites}
    assert "src/routers/quota.py" in reading, "quota.py contributes no reads"
    # Positive controls on every field a round of this PR shipped blind to.
    # Line numbers were re-pinned when the Redis budget- and quota-key writers
    # were removed; the READS they point at are unchanged.
    assert "src/routers/budget.py:1127" in derived["app.id"]
    assert "src/routers/budget.py:1128" in derived["app.name"]
    assert "src/routers/quota.py:190" in derived["plan.id"]
    assert "src/routers/quota.py:200" in derived["plan.quota_monthly"]
    # The per-licence overrides must be seen at the sync writer, not only the readers.
    assert "src/routers/budget.py:1006" in derived["token_budget_override"]
    assert "src/routers/budget.py:1075" in derived["quota_override"]


def test_conduit_exemptions_are_still_earned(tmp_path: Path) -> None:
    """An exemption must name a real consumer that still reads nothing.

    ⛔ THE EXEMPTION WAIVES THE READS *REQUIREMENT*, NEVER THE READS THEMSELVES.
    It replaced a `_forwards_the_payload()` heuristic that waived the requirement
    for any module passing the payload into any call at all — so `len(rows)` or
    one `logger.info(..., rows)` made a module reading licence fields on every
    row invisible. This test is what stops the one remaining entry from rotting
    into that: if the exempted module starts reading fields, the exemption is
    obsolete and must go.
    """
    modules = set(consumer_modules())
    derived = consumed_reads()
    for module, reason in CONDUIT_EXEMPTIONS.items():
        assert module in modules, f"{module} is exempted but is not a derived consumer"
        assert len(reason) > 100, f"{module}'s exemption has no real justification"
        reading = [s for sites in derived.values() for s in sites if s.startswith(module + ":")]
        assert not reading, (
            f"{module} is exempted as a read-free conduit but now reads {reading}. "
            "Remove the exemption rather than keeping a waiver over real reads."
        )


def test_a_logging_call_does_not_exempt_a_module_from_the_reads_requirement(
    tmp_path: Path,
) -> None:
    """⭐ The control pair for the hole a forwarding heuristic opened.

    Both files bind the licences payload FROM the fetcher (which is what the old
    `_forwards_the_payload` keyed on) and read a field under a row variable
    nobody declared. They differ by ONE log line. Under the old rule the second
    was waived entirely — the bound name reached a call, so the whole module was
    exempted from the reads requirement, and a module reading licence fields on
    every row became invisible. Both must raise.
    """
    head = (
        "from services.billing import fetch_licenses\n\n\n"
        "async def f(t, s):\n"
        "    rows = await fetch_licenses(t, s)\n"
    )
    tail = '    return [entitlement.get("seat_entitlement") for entitlement in rows]\n'
    for suffix, extra in (("plain", ""), ("logged", '    logger.info("n", len(rows))\n')):
        root = tmp_path / suffix
        target = root / "src" / "services" / "consumer.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(head + extra + tail)
        with pytest.raises(ExtractorBlindError):
            consumed_reads(root)


def test_the_fetcher_is_found_through_an_import_alias(tmp_path: Path) -> None:
    """⭐ The control pair for the aliasing hole.

    `from services.billing import fetch_licenses` was detected;
    `... as _fetch` was not, so a consumer importing it under another name was
    invisible to the entire gate. Both spellings must be discovered.
    """
    for suffix, imp, call in (
        ("plain", "from services.billing import fetch_licenses", "fetch_licenses"),
        ("aliased", "from services.billing import fetch_licenses as _fetch", "_fetch"),
    ):
        root = tmp_path / suffix
        target = root / "src" / "routers" / "thing.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            f"{imp}\n\n\nasync def h(t, s):\n"
            f"    rows = await {call}(t, s)\n"
            '    return [lic.get("id") for lic in rows]\n'
        )
        assert "src/routers/thing.py" in consumer_modules(root), (
            f"the {suffix} import spelling was not discovered"
        )


def test_a_renamed_row_variable_fails_loudly_instead_of_reporting_nothing(
    tmp_path: Path,
) -> None:
    """A blinded extractor must raise, never return an empty set."""
    target = tmp_path / "src" / "services" / "billing.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    # A real consumer (names the endpoint) whose row variable is not in ROW_VARS,
    # and which forwards nothing — the shape a rename produces.
    target.write_text(
        "def f(rows):\n"
        '    url = "/internal/licenses"\n'
        '    return [zzz.get("id") for zzz in rows], url\n'
    )
    with pytest.raises(ExtractorBlindError):
        consumed_reads(tmp_path)


# ---------------------------------------------------------------------------
# Freshness — the ONLY check here permitted to skip, and it says why
# ---------------------------------------------------------------------------


def test_vendored_schema_matches_sibling_checkout() -> None:
    """Diff the vendored copy against licences' committed ``origin/main``.

    Skips only when the sibling repo is absent (a CI runner checks out this repo
    alone) -- grep of ``.github/workflows/`` confirms this repo's CI never
    checks out a ``licenses`` sibling, so this gate is CI-inert either way: it
    is a freshness check going unrun there, never a false red. The contract
    legs above have already run unconditionally.

    ⛔ READS ``origin/main`` VIA :func:`read_source_spec`, NOT THE WORKING TREE.
    A locally stale sibling checkout (measured 2026-09-17: 37 commits behind)
    used to make this report drift that did not exist against the actual
    merged schema, and its own failure message told a reader to re-run the
    refresh script -- which would have vendored the STALE local tree over a
    correct vendored copy. See :func:`read_source_spec`'s docstring.
    """
    repo = source_repo()
    if repo is None:
        pytest.skip(
            "no sibling `licenses` checkout; freshness unverifiable, contract still checked"
        )

    spec = read_source_spec(repo)
    paths = spec.get("paths", {})
    endpoint = paths.get("/internal/licenses")
    if endpoint is None:
        sha = resolved_origin_main_sha(repo)
        pytest.fail(
            f"resolved sibling repo {repo} @ origin/main {sha} has {len(paths)} "
            "path(s) in its spec and none is '/internal/licenses' -- this is not "
            "a schema drift, it means read_source_spec did not read the real "
            f"licences repo's committed schema. Paths seen: {sorted(paths)!r}"
        )
    live = endpoint["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    vendored = load_contract()

    assert live == vendored["response"], "licences changed the 200 response envelope"
    for name, schema in vendored["schemas"].items():
        assert schema == spec["components"]["schemas"][name], (
            f"{name} drifted; re-run tests/support/refresh_licences_contract.py"
        )


def test_read_source_spec_reads_the_committed_ref_not_the_dirty_working_tree(
    tmp_path: Path,
) -> None:
    """The gate must diff against what actually merged, not a local checkout's
    working-tree state.

    Builds a throwaway git repo, commits ``docs/openapi.json`` once, points a
    manually-created ``refs/remotes/origin/main`` at that commit (no real
    ``origin`` remote is configured, so ``read_source_spec``'s best-effort
    fetch fails and falls back to this ref -- exercising that fallback path
    too), then DIRTIES the working copy of the same file without committing.
    ``read_source_spec`` must still return the COMMITTED content.

    ⭐ MUTATION CONTROL: change ``read_source_spec`` back to
    ``json.loads((repo / _SOURCE_RELATIVE).read_text())`` and this test goes
    RED (it would return the dirty content instead); restore the ``git show
    origin/main:...`` read and it goes GREEN. This is the exact regression
    measured 2026-09-17 in the shared ``~/Movies/licenses`` checkout.
    """
    import subprocess

    repo = tmp_path / "licenses"
    docs = repo / "docs"
    docs.mkdir(parents=True)

    def _git(*args: str) -> None:
        # env=git_env(): a pre-push hook exports GIT_DIR/GIT_INDEX_FILE, which a
        # child `git` inherits and then obeys INSTEAD of `-C`. Without this the
        # lines below run against the repo being pushed -- measured once, and it
        # committed into it. See git_env()'s docstring.
        subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            env=git_env(),
        )

    _git("init", "-q")
    # Fail CLOSED before any write: the repo git resolves for `-C repo` must be
    # the throwaway one, or the config/commit below would land in a real repo.
    git_dir = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--absolute-git-dir"],
        check=True,
        capture_output=True,
        text=True,
        env=git_env(),
    ).stdout.strip()
    assert Path(git_dir).resolve() == (repo / ".git").resolve(), git_dir
    _git("config", "--local", "user.email", "test@example.com")
    _git("config", "--local", "user.name", "test")

    committed = {"paths": {}, "components": {"schemas": {}}, "committed": True}
    (docs / "openapi.json").write_text(json.dumps(committed))
    _git("add", ".")
    _git("commit", "-q", "-m", "initial")
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        env=git_env(),
    ).stdout.strip()
    # Fabricate the remote-tracking ref directly -- no real `origin` remote is
    # configured, so `read_source_spec`'s `git fetch origin main` fails and it
    # falls back to whatever `origin/main` already resolves to, exactly as it
    # would offline against a previously-fetched checkout.
    _git("update-ref", "refs/remotes/origin/main", head)

    # Dirty the working tree WITHOUT committing -- this must be ignored.
    (docs / "openapi.json").write_text(json.dumps({"dirty": True}))

    result = read_source_spec(repo)

    assert result == committed


def test_the_vendored_contract_records_where_it_came_from() -> None:
    source = load_contract()["_source"]
    assert source["repo"] == "nonagenticai/licenses"
    assert source["path"] == "/internal/licenses"
    commit = source["commit"]
    assert len(commit) == 40 and all(c in "0123456789abcdef" for c in commit), (
        f"vendored copy has no real source commit: {commit!r}"
    )
    assert Path(__file__).parent.joinpath("support", "licences_contract.json").is_file()


def test_git_env_strips_every_repo_and_config_redirecting_var(monkeypatch) -> None:
    """A pre-push hook (and a worktree) exports these; any one of them left in
    place lets the throwaway-repo test above write into the REAL repository.

    ⭐ MUTATION CONTROL: drop the ``GIT_CONFIG`` prefix check in
    ``_is_git_scoping_var`` and the GIT_CONFIG* keys survive -> RED.
    """
    redirecting = [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_NAMESPACE",
        "GIT_PREFIX",
        "GIT_CONFIG",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
    ]
    for key in redirecting:
        monkeypatch.setenv(key, "/somewhere/else")
    monkeypatch.setenv("GIT_AUTHOR_NAME", "kept")
    monkeypatch.setenv("PATH", os.environ.get("PATH", ""))
    env = git_env()
    assert [k for k in redirecting if k in env] == []
    # Only the redirecting vars go; ordinary env (PATH, author identity) stays.
    assert env["GIT_AUTHOR_NAME"] == "kept"
    assert "PATH" in env


def test_the_vendored_copy_still_hashes_to_its_recorded_digests() -> None:
    """Always runs, no licenses checkout needed.

    licenses pins ``_source.response_sha256`` / ``_source.schema_sha256`` in its
    ``docs/consumer_contracts/billing-service.json`` and fails its own CI when the
    ``GET /internal/licenses`` response or a schema it references changes. That
    link only holds if this copy still IS what those digests describe: a
    hand-edited or partially refreshed copy breaks it here.
    """
    contract = load_contract()
    source = contract["_source"]
    assert {e["schema"] for e in source["schema_sha256"]} == set(contract["schemas"]), (
        "recorded digests do not cover exactly the vendored schemas"
    )
    assert contract_digests(contract) == {
        "response_sha256": source["response_sha256"],
        "schema_sha256": source["schema_sha256"],
    }, "vendored copy no longer matches its recorded digests; re-run refresh_licences_contract.py"


def test_a_hand_edited_vendored_schema_breaks_the_recorded_digest() -> None:
    """Non-vacuity: one changed field type in one schema is detected, by name."""
    contract = copy.deepcopy(load_contract())
    name = sorted(contract["schemas"])[0]
    contract["schemas"][name]["properties"] = {"injected": {"type": "string"}}
    recomputed = {e["schema"]: e["sha256"] for e in contract_digests(contract)["schema_sha256"]}
    recorded = {e["schema"]: e["sha256"] for e in contract["_source"]["schema_sha256"]}
    assert {n for n in recorded if recorded[n] != recomputed[n]} == {name}
