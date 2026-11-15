#!/usr/bin/env bash
# End-to-end smoke for the CI Plan 1 telemetry pipeline.
#
# Asserts:
# 1. /api/internal/kanban-bounce returns 200 with X-Internal-Secret AND a row
#    is persisted in metering.kanban_bounce_log within 5 seconds.
# 2. /api/internal/watchdog-event returns 200 AND a row is persisted in
#    metering.watchdog_event_log within 5 seconds.
# 3. The 3 telemetry pg_cron jobs (rollup_asset_daily_stats,
#    compute_asset_baselines, emit_cron_metric_rollups) have at least one
#    `succeeded` run in cron.job_run_details (or are too new to have fired,
#    in which case skipped with a note).
# 4. The 3 telemetry n8n workflows (kanban-bounce-recorder, watchdog-doing-2min,
#    watchdog-qa-5min) have active=true and a populated activeVersionId.
# 5. The n8n credential `planka-readonly` exists.
# 6. The Planka outbound webhook `telemetry-card-bounce` exists with target
#    URL pointing at /webhook/planka-card-update.
#
# Exits non-zero if any assertion fails. Use `--verbose` for the full output.

set -euo pipefail

# Estate access is operator-supplied; no private host or key path ships here.
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
SSH_TARGET="${SSH_TARGET:-admin@${PG_HOST:?set PG_HOST to the cluster control-plane host}}"
SSH="${SSH:-ssh -i $SSH_KEY $SSH_TARGET}"
SUDO="sudo"
VERBOSE=0; [[ "${1:-}" == "--verbose" ]] && VERBOSE=1

PASS=0; FAIL=0
pass() { PASS=$((PASS+1)); printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail() { FAIL=$((FAIL+1)); printf "  \033[31m✗\033[0m %s\n" "$*"; [[ $VERBOSE -eq 1 ]] && shift && printf "    %s\n" "$@"; }
section() { printf "\n\033[1m%s\033[0m\n" "$*"; }

section "1) /api/internal/kanban-bounce → DB"
SECRET=$($SSH "$SUDO kubectl get secret metering-secrets -n metering -o jsonpath='{.data.METERING_INTERNAL_SECRET}' | base64 -d" 2>&1 | tail -1)
NONCE="smoke-$(date +%s)-$$"
RESP=$($SSH "$SUDO kubectl run sm-kb-\$RANDOM -n metering --rm -i --restart=Never --image=curlimages/curl:latest --quiet -- curl -sS -X POST -H 'X-Internal-Secret: $SECRET' -H 'Content-Type: application/json' http://metering-service.metering.svc.cluster.local:8156/api/internal/kanban-bounce -d '{\"tenant_id\":\"smoke\",\"card_id\":\"00000000-0000-0000-0000-000000000001\",\"board_id\":\"00000000-0000-0000-0000-000000000002\",\"from_list\":\"Doing\",\"to_list\":\"Blocked\",\"from_list_kind\":\"DOING\",\"to_list_kind\":\"BLOCKED\",\"reason\":\"$NONCE\"}'" 2>&1 | tail -1)
if echo "$RESP" | grep -q '"ok":true'; then pass "endpoint returned ok=true"; else fail "endpoint did not return ok=true" "$RESP"; fi
sleep 1
ROW=$($SSH "$SUDO -u postgres psql -d metering -tAc \"SELECT 1 FROM kanban_bounce_log WHERE reason='$NONCE'\"" 2>&1 | tail -1)
if [[ "$ROW" == "1" ]]; then pass "row persisted in kanban_bounce_log (reason=$NONCE)"; else fail "row missing in kanban_bounce_log" "expected reason=$NONCE"; fi

section "2) /api/internal/watchdog-event → DB"
NONCE2="smoke-wd-$(date +%s)-$$"
CARD_ID="00000000-0000-0000-0000-$(printf '%012x' $((RANDOM*RANDOM)))"
RESP=$($SSH "$SUDO kubectl run sm-wd-\$RANDOM -n metering --rm -i --restart=Never --image=curlimages/curl:latest --quiet -- curl -sS -X POST -H 'X-Internal-Secret: $SECRET' -H 'Content-Type: application/json' http://metering-service.metering.svc.cluster.local:8156/api/internal/watchdog-event -d '{\"tenant_id\":\"smoke\",\"card_id\":\"$CARD_ID\",\"board_id\":\"00000000-0000-0000-0000-000000000004\",\"watchdog_kind\":\"DOING_2MIN\",\"list_at_fire\":\"DOING\",\"action_taken\":\"NOTIFY\"}'" 2>&1 | tail -1)
if echo "$RESP" | grep -q '"ok":true'; then pass "endpoint returned ok=true"; else fail "endpoint did not return ok=true" "$RESP"; fi
sleep 1
ROW=$($SSH "$SUDO -u postgres psql -d metering -tAc \"SELECT 1 FROM watchdog_event_log WHERE card_id='$CARD_ID'\"" 2>&1 | tail -1)
if [[ "$ROW" == "1" ]]; then pass "row persisted in watchdog_event_log (card_id=$CARD_ID)"; else fail "row missing in watchdog_event_log" "expected card_id=$CARD_ID"; fi

section "3) pg_cron jobs"
for JOB in rollup_asset_daily_stats compute_asset_baselines emit_cron_metric_rollups; do
  STATUS=$($SSH "$SUDO -u postgres psql -d postgres -tAc \"SELECT status FROM cron.job_run_details r JOIN cron.job j ON r.jobid=j.jobid WHERE j.jobname='$JOB' ORDER BY r.start_time DESC LIMIT 1\"" 2>&1 | tail -1)
  case "$STATUS" in
    succeeded) pass "$JOB last run: succeeded";;
    "") pass "$JOB scheduled but not yet fired (new install)";;
    failed) fail "$JOB last run: failed (check return_message)";;
    *) fail "$JOB last run: $STATUS";;
  esac
done

section "4) n8n telemetry workflows active"
for NAME in kanban-bounce-recorder watchdog-doing-2min watchdog-qa-5min; do
  ROW=$($SSH "$SUDO -u postgres psql -d n8n -tAc \"SELECT active::text || '|' || COALESCE(\\\"activeVersionId\\\", 'null') FROM workflow_entity WHERE name='$NAME'\"" 2>&1 | tail -1)
  if [[ "$ROW" == true\|* ]]; then
    if [[ "$ROW" == *\|null ]]; then fail "$NAME active but no activeVersionId" "$ROW"; else pass "$NAME active=true with activeVersionId"; fi
  else fail "$NAME not active" "$ROW"; fi
done

section "5) n8n planka-readonly credential"
ROW=$($SSH "$SUDO -u postgres psql -d n8n -tAc \"SELECT name FROM credentials_entity WHERE id='planka-readonly'\"" 2>&1 | tail -1)
if [[ -n "$ROW" && "$ROW" != *Defaulted* ]]; then pass "credential planka-readonly exists (name='$ROW')"; else fail "credential planka-readonly missing" "$ROW"; fi

section "6) Planka outbound webhook"
ADMIN_EMAIL=$($SSH "$SUDO kubectl get deploy kanban -n kanban -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"DEFAULT_ADMIN_EMAIL\")].value}'" 2>&1 | tail -1)
ADMIN_PW=$($SSH "$SUDO kubectl get secret kanban-credentials -n kanban -o jsonpath='{.data.default-admin-password}' | base64 -d" 2>&1 | tail -1)
WEBHOOKS=$($SSH "$SUDO kubectl run wh-list-\$RANDOM -n kanban --rm -i --restart=Never --image=curlimages/curl:latest --quiet -- sh -c 'TOKEN=\$(curl -sS -X POST -H \"Content-Type: application/json\" -d \"{\\\"emailOrUsername\\\":\\\"$ADMIN_EMAIL\\\",\\\"password\\\":\\\"$ADMIN_PW\\\"}\" http://kanban.kanban.svc.cluster.local:3333/api/access-tokens | sed \"s/.*\\\"item\\\":\\\"//;s/\\\".*//\"); curl -sS -H \"Authorization: Bearer \$TOKEN\" http://kanban.kanban.svc.cluster.local:3333/api/webhooks'" 2>&1 | tail -1)
if echo "$WEBHOOKS" | grep -q 'telemetry-card-bounce' && echo "$WEBHOOKS" | grep -q '/webhook/planka-card-update'; then
  pass "Planka webhook telemetry-card-bounce → /webhook/planka-card-update configured"
else
  fail "Planka webhook telemetry-card-bounce missing or wrong URL"
fi

printf "\n\033[1mResult:\033[0m %d passed, %d failed\n" "$PASS" "$FAIL"
[[ $FAIL -eq 0 ]] || exit 1
