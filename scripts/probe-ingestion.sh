#!/usr/bin/env bash
# Metering ingestion probe — Phase F.2 of the metering finish-the-job plan.
#
# Purpose
#   Sends one synthetic chat-completion request through inference-router, then
#   polls the billing Postgres `billing_cost_events` table until a matching row
#   appears (or 30s elapses). Used to confirm the end-to-end ingestion pipeline
#       inference-router  → Redis (metering:costs)
#                         → metering-service consumer
#                         → billing_cost_events row
#   is actually emitting rows. After the license-authz tenant_id cross-wire
#   fix (Phase F.1) lands, the operator runs this to confirm new events flow
#   with correct tenant_id values.
#
# How matching works
#   The probe sends a unique synthetic ``X-Job-Id`` (e.g. ``probe-1715283600-12345``)
#   and polls ``billing_cost_events`` rows by ``metadata->>'job_id' = '<job_id>'``.
#   The ``job_id`` is captured by inference-router from the ``X-Job-Id``
#   header (src/services/metering.py emit_cost_event_task) and persisted by
#   the metering-service cost flusher into the ``metadata`` JSONB column
#   (src/services/cost_flusher.py — see ``extra_meta["job_id"]``). There is
#   no top-level ``billing_cost_events.job_id`` column.
#
#   We also send a ``X-Correlation-Id`` UUID and a synthetic subject_id, but
#   inference-router does NOT currently propagate ``X-Correlation-Id`` onto
#   the cost-event Redis stream (verified 2026-05-09 — recent rows in
#   billing_cost_events have correlation_id NULL). Matching by job_id in metadata
#   is therefore the only reliable handle. Follow-up: extend
#   inference-router's emit_cost_event to forward correlation_id so this
#   probe can match by the dedicated indexed column.
#
# Environment
#   Requires SSH access to master (k3s control plane) — runs kubectl-exec'd curl pods
#   for the inference-router call and `sudo -u postgres psql -d billing` for
#   the DB read. No local credentials needed; everything goes through master.
#
#   Override SSH target with the SSH env var, or set SSH_KEY and SSH_TARGET
#   (default: `ssh -i $SSH_KEY $SSH_TARGET`; no private host ships here).
#
# Usage
#   scripts/probe-ingestion.sh                       # run with defaults
#   scripts/probe-ingestion.sh --verbose             # print full curl/psql output
#   scripts/probe-ingestion.sh --help                # this help
#
# Exit codes
#   0 — billing_cost_events row found within 30s
#   1 — no matching row within 30s, or upstream call failed
#   2 — usage / argument error
#
# Manual end-to-end run (post-merge, after Flux promotes license-authz):
#   ssh -i $SSH_KEY $SSH_TARGET 'cd /tmp && curl -sS -o probe.sh \
#     https://raw.githubusercontent.com/nonagenticai/billing-service/main/scripts/probe-ingestion.sh \
#     && chmod +x probe.sh && SSH=" " ./probe.sh --verbose'

set -euo pipefail

# -- arg parsing --------------------------------------------------------------
VERBOSE=0
case "${1:-}" in
  -h|--help)
    awk '/^set -euo pipefail/{exit} NR>1{sub(/^# ?/,""); print}' "$0"
    exit 0
    ;;
  -v|--verbose)
    VERBOSE=1
    ;;
  "")
    ;;
  *)
    echo "unknown argument: $1 (try --help)" >&2
    exit 2
    ;;
esac

# No private host or key path ships here: set SSH_TARGET (user@host) and
# SSH_KEY, or pass SSH=" " to run on the target host itself.
SSH="${SSH:-ssh -i ${SSH_KEY:-$HOME/.ssh/id_ed25519} ${SSH_TARGET:?set SSH_TARGET to the control-plane host (user@host)}}"
SUDO="sudo"
INFERENCE_URL="http://inference-router.inference-router.svc.cluster.local:8150/api/chat/completions"
BILLING_DB="billing"
TIMEOUT_SECONDS=30
POLL_INTERVAL=2

NONCE="$(date +%s)-$$"
JOB_ID="probe-ingestion-${NONCE}"
SUBJECT_ID="probe-ingestion-${NONCE}"
CORRELATION_ID="$(uuidgen 2>/dev/null || python3 -c 'import uuid;print(uuid.uuid4())')"

# X-Tenant-Id must be a value the service can RESOLVE. Since the tenant_id
# varchar->uuid conversion, auth_deps returns 400 for anything that is neither a
# uuid nor a key of LEGACY_TENANT_NAMES — the old slug 'probe-ingestion-tenant'
# now fails the probe before it reaches ingestion. This is that slug's derived
# uuid (src/tenancy.py: derived_tenant_id), reproducible with:
#   python3 -c "import uuid; print(uuid.uuid5(uuid.UUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8'), 'probe-ingestion-tenant'))"
# The human label survives in the tenant_name column, not in this header.
PROBE_TENANT_LABEL="probe-ingestion-tenant"
PROBE_TENANT_ID="54c57c69-0803-5cc3-b751-ef827403afb7"

vlog() { [[ $VERBOSE -eq 1 ]] && printf '  %s\n' "$*" >&2 || true; }
ok() { printf '\033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; }

printf '\033[1mMetering ingestion probe\033[0m\n'
printf '  job_id        = %s\n' "$JOB_ID"
printf '  correlation_id = %s\n' "$CORRELATION_ID"
printf '  tenant_id     = %s (derived from %s)\n' "$PROBE_TENANT_ID" "$PROBE_TENANT_LABEL"
printf '  inference url = %s\n' "$INFERENCE_URL"

# -- step 1: send chat-completion --------------------------------------------
printf '\n[1/2] Sending chat-completion through inference-router...\n'

REQUEST_BODY=$(cat <<JSON
{"model":"gpt-3.5-turbo","messages":[{"role":"user","content":"probe-ingestion ping"}],"max_tokens":4,"temperature":0}
JSON
)

# kubectl run a one-shot curl pod in the inference-router namespace so the
# Service DNS resolves and Istio mesh policy applies. Pod name is randomized
# so concurrent probes don't collide.
POD_NAME="probe-ir-$RANDOM-$$"
RESP=$(
  $SSH "$SUDO kubectl run $POD_NAME -n inference-router --rm -i --restart=Never \
    --image=curlimages/curl:8.10.1 --quiet -- \
    curl -sS -m 20 -w '\n__HTTP_STATUS__:%{http_code}\n' \
      -X POST '$INFERENCE_URL' \
      -H 'Content-Type: application/json' \
      -H 'X-Job-Id: $JOB_ID' \
      -H 'X-Subject-Id: $SUBJECT_ID' \
      -H 'X-Correlation-Id: $CORRELATION_ID' \
      -H 'X-Tenant-Id: $PROBE_TENANT_ID' \
      -H 'X-License-Id: probe-ingestion-license' \
      -H 'X-App-Id: probe-ingestion-app' \
      -d '$REQUEST_BODY'" 2>&1
) || {
  fail "kubectl-exec'd curl failed (see verbose for output)"
  vlog "$RESP"
  exit 1
}

HTTP_STATUS=$(printf '%s' "$RESP" | sed -n 's/^__HTTP_STATUS__:\([0-9]*\)$/\1/p' | tail -1)
vlog "HTTP $HTTP_STATUS"
vlog "$RESP"

# Inference-router may legitimately fail (e.g. no provider quota) but should
# still emit a cost_event for the attempt. We only HARD-fail on no-status
# (network/transport error). 4xx/5xx are warnings.
if [[ -z "$HTTP_STATUS" ]]; then
  fail "no HTTP status returned — kubectl/curl pipeline broken"
  exit 1
fi
if [[ "$HTTP_STATUS" =~ ^2 ]]; then
  ok "inference-router accepted request (HTTP $HTTP_STATUS)"
else
  printf '\033[33m!\033[0m  inference-router returned HTTP %s — continuing to poll billing_cost_events anyway (non-2xx may still emit)\n' "$HTTP_STATUS"
fi

# -- step 2: poll billing_cost_events -------------------------------------------------
printf '\n[2/2] Polling billing_cost_events for job_id=%s (timeout %ss)...\n' "$JOB_ID" "$TIMEOUT_SECONDS"

DEADLINE=$(( $(date +%s) + TIMEOUT_SECONDS ))
# job_id lives in billing_cost_events.metadata->>'job_id' (no top-level column).
SQL="SELECT json_build_object(
       'tenant_id', tenant_id,
       'tenant_name', tenant_name,
       'license_id', license_id,
       'subject_id', subject_id,
       'job_id', metadata->>'job_id',
       'correlation_id', correlation_id,
       'provider', provider,
       'model', model,
       'input_tokens', input_tokens,
       'output_tokens', output_tokens,
       'cost_usd', cost_usd,
       'cost_usd_hint', cost_usd_hint,
       'created_at', created_at
     )::text
     FROM billing_cost_events
     WHERE metadata->>'job_id' = '$JOB_ID'
     ORDER BY created_at DESC LIMIT 1;"

while [[ $(date +%s) -lt $DEADLINE ]]; do
  ROW=$($SSH "$SUDO -u postgres psql -d $BILLING_DB -tAc \"$SQL\"" 2>&1 | tail -1)
  if [[ -n "$ROW" && "$ROW" != *"ERROR"* && "$ROW" != *"FATAL"* ]]; then
    if [[ "$ROW" == \{* ]]; then
      printf '\n'
      ok "billing_cost_events row found:"
      printf '   %s\n' "$ROW"
      exit 0
    fi
  fi
  printf '.'
  vlog "psql: ${ROW:0:120}"
  sleep "$POLL_INTERVAL"
done
printf '\n'

fail "no billing_cost_events row matched job_id='$JOB_ID' within ${TIMEOUT_SECONDS}s"
fail "this means the inference-router → Redis → metering-service ingestion path is NOT producing rows"
fail "(or the inference-router request itself was rejected before cost emission — re-run with --verbose)"
exit 1
