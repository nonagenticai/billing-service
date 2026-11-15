#!/usr/bin/env bash
# Refresh tests/fixtures/licenses_tenants.txt from the LIVE licences registry.
#
# WHY THIS EXISTS
# ---------------
# tenancy.LEGACY_TENANT_NAMES is an INGRESS resolver: a name it does not hold
# takes a hard 400 from this service. It is a hand-maintained snapshot of the
# `licenses.tenants` registry, and it had silently drifted from it — the shim
# held 75 keys under a docstring claiming 72, was MISSING six genuinely
# registered names (claude, procedure-improvement, prompt-improvement, svc-kag,
# svc-kag-eval-r46, svc-w2v-shared — every one of them a live 400), and carried
# two retired spellings (platform-cp-4, platform-cp-5) that nothing declared as
# aliases. Nothing detected any of it.
#
# tests/test_tenant_registry_shim.py now does, against the fixture this writes.
#
# ⛔ THE FIXTURE IS NOT GENERATED FROM src/tenancy.py, AND MUST NEVER BE.
# A fixture derived from the code it checks agrees with that code by
# construction: the test would pass against a shim that had drifted arbitrarily
# far. The query below is the independent source.
#
# ⛔ AFTER RUNNING THIS, RUN THE TEST. The registry query returns only the names
# the registry holds TODAY, so a rename makes the old spelling vanish from the
# output. Pasting that in and calling it done is exactly how a live caller
# starts taking a 400. The test fails naming every shim key the refreshed
# fixture no longer vouches for; each must be declared in
# RENAMED_TENANT_ALIASES, and may be DELETED only once the old spelling is
# measured absent from live traffic.
#
# Requires network access to the fixture-source host: set WORKER4_HOST
# (user@host) and SSH_KEY (defaults to ~/.ssh/id_ed25519).
set -euo pipefail

WORKER4="${WORKER4_HOST:?set WORKER4_HOST to the fixture-source host (user@host)}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE="$REPO_ROOT/tests/fixtures/licenses_tenants.txt"

echo "Querying licences registry on $WORKER4 ..." >&2
rows="$(ssh -o BatchMode=yes -i "$SSH_KEY" "$WORKER4" \
  "sudo -u postgres psql -d licenses -Atc \"select name||'|'||id from tenants order by name\"")"

if [ -z "$rows" ]; then
    echo "ERROR: registry query returned nothing. Refusing to write an empty fixture," >&2
    echo "which would make the shim test vacuously pass against any shim at all." >&2
    exit 1
fi

# Keep the existing header (every '#' line) so the provenance notes survive a
# refresh; only the data rows are replaced. The capture date is re-stamped.
header="$(grep '^#' "$FIXTURE" | grep -v '^# Captured:')"
{
    printf '%s\n' "$header"
    printf '# Captured: %s from worker4 (%s), database `licenses`.\n' \
        "$(date -u +%Y-%m-%d)" "${WORKER4#*@}"
    printf '%s\n' "$rows"
} > "$FIXTURE"

echo "Wrote $(printf '%s\n' "$rows" | wc -l | tr -d ' ') registry rows to $FIXTURE" >&2
echo "NOW RUN: uv run pytest tests/test_tenant_registry_shim.py -q" >&2
