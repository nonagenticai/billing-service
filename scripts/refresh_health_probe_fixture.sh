#!/usr/bin/env bash
# Refresh tests/fixtures/health_probe_subjects.txt from the LIVE billing ledger.
#
# WHY THIS EXISTS
# ---------------
# health_probes.HEALTH_PROBE_EMITTERS decides which cost events STOP BEING
# BILLED. It is a hand-written allowlist of service-account identities, and an
# allowlist has exactly two ways to be wrong, both of them silent:
#
#   1. An entry that never matched anything — a typo'd uuid, a service_key that
#      was guessed rather than measured. The rule does nothing, probe rows keep
#      billing, and the code reads as if the problem were solved.
#   2. An entry that STOPPED matching — a rotated service account, a renamed
#      endpoint, a probe that moved. Identical symptom, and it appears months
#      after the change, in a repo nobody is looking at.
#
# Neither shows up in a diff, in a test that only exercises the matcher against
# its own constants, or in a log. tests/test_health_probe_exclusion.py checks
# every entry against the fixture this script writes, so both failures become a
# red CI run naming the offending entry.
#
# ⛔ THE FIXTURE IS NOT GENERATED FROM src/health_probes.py, AND MUST NEVER BE.
# The query below does not know the allowlist exists: it censuses the ledger and
# emits EVERY (subject_id, service_key, endpoint) triple carrying real volume.
# A fixture derived from the code it checks agrees with that code by
# construction, and the test would then pass against an allowlist that had
# drifted arbitrarily far — the whole point is an independent source.
#
# ⛔ AFTER RUNNING THIS, RUN THE TEST, AND READ WHAT IT SAYS. An entry the
# refreshed census no longer vouches for is NOT automatically a stale entry to
# delete. It may equally be a probe that is currently DOWN, in which case
# deleting the entry is how it silently resumes billing when it comes back. Find
# out which before editing the allowlist.
#
# ⚠️ AND NOTE WHAT THE FIXTURE DELIBERATELY DOES NOT CARRY: any assertion that a
# triple is probe-ONLY. `nonprobe_rows` and `distinct_input_tokens` are recorded
# precisely so a reader can see that neither live probe subject is pure (both
# carry real traffic that only the service_key/endpoint narrowing protects — see
# src/health_probes.py). A census cannot tell a probe from a customer; only the
# hand-verified allowlist claims that, and it claims it for three triples.
#
# Requires network access to the fixture-source host: set WORKER4_HOST
# (user@host) and SSH_KEY (defaults to ~/.ssh/id_ed25519).
set -euo pipefail

WORKER4="${WORKER4_HOST:?set WORKER4_HOST to the fixture-source host (user@host)}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
WINDOW="${WINDOW:-7 days}"
# Volume floor for the census. Bounds the fixture to triples that carry enough
# traffic for the counts beside them to mean anything; a triple below this is
# noise, and an allowlist entry that only ever matched noise SHOULD fail the
# drift guard.
MIN_ROWS="${MIN_ROWS:-100}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE="$REPO_ROOT/tests/fixtures/health_probe_subjects.txt"

echo "Censusing billing_cost_events on $WORKER4 (window: $WINDOW, floor: $MIN_ROWS rows) ..." >&2

# NB: no `excluded_from_billing` filter. Once this fix is deployed the probe rows
# ARE excluded, so filtering on it would make the census go empty exactly when
# the rule starts working — the fixture would then "prove" every entry stale and
# the drift guard would invert into a demand to delete the rule that is working.
read -r -d '' SQL <<'EOSQL' || true
SELECT subject_id
       || '|' || tenant_id
       || '|' || coalesce(nullif(tenant_name, ''), '?')
       || '|' || service_key
       || '|' || coalesce(usage_extra->>'endpoint', '')
       || '|' || count(*)
       || '|' || count(DISTINCT input_tokens)
       || '|' || count(*) FILTER (WHERE cost_basis <> 'internal_transfer')
       || '|' || mode() WITHIN GROUP (ORDER BY input_tokens)
  FROM billing_cost_events
 WHERE created_at > now() - interval 'WINDOW_PLACEHOLDER'
   AND subject_id IS NOT NULL
 GROUP BY subject_id, tenant_id, tenant_name, service_key, usage_extra->>'endpoint'
HAVING count(*) >= MIN_ROWS_PLACEHOLDER
 ORDER BY count(*) DESC, subject_id, service_key, usage_extra->>'endpoint'
EOSQL
SQL="${SQL//WINDOW_PLACEHOLDER/$WINDOW}"
SQL="${SQL//MIN_ROWS_PLACEHOLDER/$MIN_ROWS}"

rows="$(ssh -o BatchMode=yes -i "$SSH_KEY" "$WORKER4" \
  "sudo -u postgres psql -d billing -Atc \"$(printf '%s' "$SQL" | tr '\n' ' ')\"")"

if [[ -z "$rows" ]]; then
  echo "REFUSING TO WRITE: the census returned NOTHING." >&2
  echo "An empty result is not evidence that no probe exists — it is far more" >&2
  echo "likely that the query, the window or the connection is wrong. Writing it" >&2
  echo "would fail every allowlist entry at once and read as total rot." >&2
  exit 1
fi

{
  echo "# billing ledger census — the INDEPENDENT source for the probe allowlist drift guard."
  echo "#"
  echo "# SOURCE OF TRUTH, not a derivative. This file is the OUTPUT of a query against"
  echo "# billing_cost_events on the prod postgres primary (worker4). It is NOT generated"
  echo "# from src/health_probes.py and must never be: a fixture derived from the thing it"
  echo "# checks can only ever agree with it, and the test reading it would pass against an"
  echo "# allowlist that had drifted arbitrarily far."
  echo "#"
  echo "# Format, one row per (subject, tenant, service_key, endpoint) tuple:"
  echo "#   <subject_id>|<tenant_id>|<tenant_name>|<service_key>|<endpoint>|<rows>|<distinct_input_tokens>|<non_internal_transfer_rows>|<modal_input_tokens>"
  echo "#"
  echo "# ⚠️ tenant_id is in the key because subject_id ALONE IS NOT AN IDENTITY: the karma"
  echo "# subject 01a0138e-… serves both svc-kag-karma (the probe) and nonagentic-gtm (real"
  echo "# traffic, 44 distinct input_tokens) on the SAME service_key and endpoints."
  echo "#"
  echo "# ⚠️ modal_input_tokens is what backs the input_tokens CONJUNCT on each allowlist"
  echo "# entry. Pinning a payload size makes the rule narrower and safer, but it also makes"
  echo "# it ROT SILENTLY: change the probe text by one character and the rule stops matching"
  echo "# while nothing looks wrong. This column is what turns that into a red CI build."
  echo "#"
  echo "# ⚠️ distinct_input_tokens IS EVIDENCE, NOT THE RULE. A triple showing 1 distinct"
  echo "# value across tens of thousands of rows is almost certainly a probe — but billing"
  echo "# NEVER keys the exclusion on it. See the '⛔ THE RULE IS KEYED ON IDENTITY' section"
  echo "# of src/health_probes.py for why a payload-shape rule is the dangerous fix."
  echo "#"
  echo "# Census window: $WINDOW. Volume floor: $MIN_ROWS rows."
  echo "# REFRESH (from a host with tailnet access to worker4):"
  echo "#     scripts/refresh_health_probe_fixture.sh"
  echo "# Then run: pytest tests/test_health_probe_exclusion.py"
  echo "#"
  printf '%s\n' "$rows"
} >"$FIXTURE"

echo "Wrote $(grep -cv '^#' "$FIXTURE") triples to $FIXTURE" >&2
echo "NOW RUN: pytest tests/test_health_probe_exclusion.py" >&2
