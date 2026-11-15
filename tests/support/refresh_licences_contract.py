#!/usr/bin/env python3
"""Re-vendor the licences ``GET /internal/licenses`` response schema.

Run from the repo root, with the ``licenses`` repo checked out as a sibling::

    python3 tests/support/refresh_licences_contract.py

Writes ``tests/support/licences_contract.json``. The vendored copy is what the
always-on gate validates against; this script is the ONLY way it should ever
change, so that the recorded source commit stays honest.

⚠️ THE RECORDED COMMIT SHA TRIPS ``detect-secrets`` AS A "Hex High Entropy
String", so a refresh needs a matching ``.secrets.baseline`` update or CI reds
on the secret scan (measured: PR #198's first run). This script prints the exact
command afterwards. The baseline entry is keyed by the HASH OF THAT SHA, so it
allowlists that one public git identifier and nothing else — a real secret
pasted into this file would hash differently and still turn CI red.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

# Imported by pytest as ``tests.support.refresh_licences_contract`` AND run as a
# bare script; package import first so pytest keeps ONE module object, repo
# root on sys.path only for the bare-script case.
try:
    from tests.support.licences_contract import contract_digests
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from tests.support.licences_contract import contract_digests

_HERE = Path(__file__).resolve().parent
_OUT = _HERE / "licences_contract.json"

#: Sibling checkout of ``nonagenticai/licenses``. Mirrors the reverse guard
#: (``licenses/tests/support/refresh_billing_contract.py``), which reads
#: ``billing_service/docs/openapi.json`` the same way.
#:
#: Walked UPWARDS rather than pinned to a fixed number of ``parents[]``: a
#: ``git worktree`` puts the repo root two levels deeper than the shared
#: checkout, so a fixed index resolves to ``.worktrees/licenses`` and reports
#: "not found" for a repo that is sitting right there.
_SOURCE_RELATIVE = Path("docs") / "openapi.json"

# ⛔ A PRE-PUSH HOOK RUNS WITH `GIT_DIR` (and often `GIT_INDEX_FILE`) EXPORTED,
# and a `git` subprocess INHERITS them and then IGNORES its own `-C <dir>`.
# Measured 2026-09-17: under the pre-push gate the throwaway-repo test below
# ran `git init`/`add .`/`commit`/`update-ref` against THIS repository instead
# of its temp directory -- it wrote a commit titled "initial" onto the branch
# being pushed (deleting every tracked file in that commit) and repointed this
# repo's `refs/remotes/origin/main` at it. The freshness test then read THAT
# ref and reported a schema with no `/internal/licenses`, which looked exactly
# like licences drift. Every git call in this module and its tests must run
# with the repo-scoping variables stripped, so `-C` is actually obeyed.
_GIT_SCOPING_VARS = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_NAMESPACE",
    "GIT_PREFIX",
)

#: Any variable with this prefix redirects or injects git CONFIG
#: (``GIT_CONFIG``, ``GIT_CONFIG_GLOBAL``/``_SYSTEM``/``_NOSYSTEM``,
#: ``GIT_CONFIG_PARAMETERS``, ``GIT_CONFIG_COUNT``/``_KEY_<n>``/``_VALUE_<n>``).
#: ``GIT_CONFIG`` in particular makes ``git config <k> <v>`` WRITE to that file
#: regardless of ``-C``. Found 2026-09-28: ``user.email=test@example.com`` (the
#: throwaway repo's value below) in the REAL billing_service ``.git/config``.
_GIT_CONFIG_PREFIX = "GIT_CONFIG"


def _is_git_scoping_var(key: str) -> bool:
    return key in _GIT_SCOPING_VARS or key.startswith(_GIT_CONFIG_PREFIX)


def git_env() -> dict[str, str]:
    """``os.environ`` without the variables that pin git to another repo or
    another config file (see ``_GIT_SCOPING_VARS`` / ``_GIT_CONFIG_PREFIX``)."""
    return {k: v for k, v in os.environ.items() if not _is_git_scoping_var(k)}


def _is_licences_checkout(candidate: Path) -> bool:
    """True only if ``candidate`` really is the ``nonagenticai/licenses`` repo.

    Presence of ``docs/openapi.json`` alone is not identity -- any directory
    literally named ``licenses`` sitting above this file (an orphaned
    ``git worktree``, a differently-purposed checkout, a stray clone left by
    another lane) can carry an unrelated file at that path and be silently
    accepted as "the sibling". Checking the ``origin`` remote is a cheap,
    local, no-network way to confirm this is actually that repo before ever
    trusting what it reads.
    """
    try:
        result = subprocess.run(  # noqa: S603
            ["git", "-C", str(candidate), "remote", "get-url", "origin"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=5,
            env=git_env(),
        )
    except subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired, OSError:
        return False
    if result.returncode != 0:
        return False
    url = result.stdout.strip().removesuffix(".git")
    return url.endswith("nonagenticai/licenses")


def source_repo() -> Path | None:
    """First ancestor holding a real ``nonagenticai/licenses`` checkout at
    ``<ancestor>/licenses``, else ``None``.
    """
    for parent in _HERE.parents:
        candidate = parent / "licenses"
        if (candidate / _SOURCE_RELATIVE).is_file() and _is_licences_checkout(candidate):
            return candidate
    return None


def resolved_origin_main_sha(repo: Path) -> str:
    """``origin/main``'s current commit in ``repo``, or a placeholder.

    Diagnostic-only: never raises, so a failure message that calls this to
    explain ITSELF cannot itself blow up with a second, more confusing error.
    """
    try:
        result = subprocess.run(  # noqa: S603
            ["git", "-C", str(repo), "rev-parse", "origin/main"],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=5,
            env=git_env(),
        )
    except subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired, OSError:
        return "<unresolvable>"
    return result.stdout.strip() if result.returncode == 0 else "<unresolvable>"


def _fetch_origin_main(repo: Path) -> None:
    """Best-effort ``git fetch origin main``. Never raises.

    A failure here (offline, no network in this shell) is a warning, not fatal:
    :func:`read_source_spec` falls back to whatever ``origin/main`` the last
    successful fetch left in this checkout's remote-tracking refs.
    """
    try:
        subprocess.run(  # noqa: S603
            ["git", "-C", str(repo), "fetch", "origin", "main", "--quiet"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
            env=git_env(),
        )
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as exc:
        print(
            f"warning: `git fetch origin main` failed in {repo} ({exc}); "
            "using the last-known origin/main ref instead",
            file=sys.stderr,
        )


def read_source_spec(repo: Path) -> dict:
    """Read ``docs/openapi.json`` from the sibling's ``origin/main``, never its
    working tree.

    ⛔ THE WORKING TREE IS NOT THE SOURCE OF TRUTH. This function and
    :func:`main` used to read ``<repo>/docs/openapi.json`` straight off disk --
    whatever commit, branch or dirty state the sibling checkout happened to be
    sitting in. Measured 2026-09-17: the shared ``~/Movies/licenses`` checkout
    was 37 commits behind ``origin/main``, so ``test_vendored_schema_matches_
    sibling_checkout`` (in ``tests/test_licences_contract.py``) reported drift
    that did not exist -- the vendored schema matched ``origin/main`` exactly --
    and told a reader to re-run this refresh script, which would have vendored
    the STALE schema over a correct one. Reading the committed ``origin/main``
    ref instead makes the gate and this script agree with what actually merged,
    regardless of what a human happens to have checked out locally.

    Best-effort ``fetch``s first so a stale local remote-tracking ref does not
    silently pin an old commit; see :func:`_fetch_origin_main` for the offline
    fallback.
    """
    _fetch_origin_main(repo)
    content = subprocess.run(  # noqa: S603
        ["git", "-C", str(repo), "show", f"origin/main:{_SOURCE_RELATIVE.as_posix()}"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
        env=git_env(),
    ).stdout
    return json.loads(content)


_PATH = "/internal/licenses"
_METHOD = "get"


def main() -> int:
    repo = source_repo()
    if repo is None:
        print("no sibling `licenses` checkout found above this repo", file=sys.stderr)
        return 2

    spec = read_source_spec(repo)
    components = spec["components"]["schemas"]
    response = spec["paths"][_PATH][_METHOD]["responses"]["200"]["content"]["application/json"][
        "schema"
    ]

    # Transitive closure of $refs reachable from the 200 response schema.
    wanted: set[str] = set()
    frontier = [json.dumps(response)]
    while frontier:
        blob = frontier.pop()
        for name in re.findall(r"#/components/schemas/(\w+)", blob):
            if name not in wanted:
                wanted.add(name)
                frontier.append(json.dumps(components[name]))

    try:
        sha = subprocess.run(  # noqa: S603
            ["git", "-C", str(repo), "rev-parse", "origin/main"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
            env=git_env(),
        ).stdout.strip()
    except subprocess.CalledProcessError, FileNotFoundError:
        sha = "unknown"

    payload = {
        "_source": {
            "repo": "nonagenticai/licenses",
            "file": str(_SOURCE_RELATIVE),
            "commit": sha,
            "path": _PATH,
            "method": _METHOD,
            "status": "200",
        },
        "response": response,
        "schemas": {name: components[name] for name in sorted(wanted)},
    }
    # The handshake value: licenses pins these digests in
    # docs/consumer_contracts/billing-service.json and fails its own CI when
    # what this copy mirrors changes. Copy them there in the same change.
    payload["_source"].update(contract_digests(payload))
    _OUT.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(f"wrote {_OUT} from {repo} origin/main @ {sha}")
    print(
        "licenses docs/consumer_contracts/billing-service.json must pin:\n"
        + json.dumps(contract_digests(payload), indent=2, sort_keys=True)
    )
    print(
        "\nNEXT (the new commit sha is a fresh Hex High Entropy String; without this\n"
        "the CI secret scan goes red on a public git sha):\n"
        "  uv run detect-secrets scan --baseline .secrets.baseline \\\n"
        "    --force-use-all-plugins \\\n"
        "    --exclude-files '\\.secrets\\.baseline$|^\\.github/workflows/'\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
