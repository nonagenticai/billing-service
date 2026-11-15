#!/usr/bin/env bash
# Bootstrap the CI Plan 1 telemetry pipeline end-to-end.
#
# Idempotent. Runs from any host with SSH access to mf1. Re-running on a
# already-provisioned cluster is a no-op (each step is guarded by "exists?"
# checks). Prints a single line per logical step so the output is grep-friendly.
#
# Pre-requisites in production (one-time, manual):
# - alembic upgrade head against the metering DB (migrations 0001-0005)
# - METERING_INTERNAL_SECRET wired into deploy/metering-service env from
#   secret/metering-secrets (key METERING_INTERNAL_SECRET)
# - install_pg_cron.sql applied to the postgres DB on mf1
# - watchdog-*.json node type fixed to @nonagentic/n8n-nodes-kanban.planka
#
# What this script does:
# 1. Ensures the planka_readonly Postgres role exists in `kanban` DB.
# 2. Adds planka_readonly to the pgbouncer userlist (SCRAM verifier).
# 3. Stores the readonly credentials in k8s secret kanban/planka-readonly-db.
# 4. Imports the planka-readonly n8n credential (id preserved via CLI).
# 5. Inserts workflow_history rows for the 3 telemetry workflows so n8n's
#    "active" flag persists across restarts.
# 6. Activates kanban-bounce-recorder + watchdog-doing-2min + watchdog-qa-5min.
# 7. Configures Planka outbound webhook for cardUpdate → n8n.
#
# Exits non-zero on the first failure. Safe to interrupt and re-run.

set -euo pipefail

# Estate access is operator-supplied; no private host or key path ships here.
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
SSH_TARGET="${SSH_TARGET:-admin@${PG_HOST:?set PG_HOST to the cluster control-plane host}}"
SSH="${SSH:-ssh -i $SSH_KEY $SSH_TARGET}"
SUDO="sudo"

WORKFLOW_NAMES=("kanban-bounce-recorder" "watchdog-doing-2min" "watchdog-qa-5min")
PLANKA_WEBHOOK_NAME="telemetry-card-bounce"
PLANKA_WEBHOOK_URL="http://n8n.n8n.svc.cluster.local:5678/webhook/planka-card-update"
N8N_KANBAN_NODE_TYPE="@nonagentic/n8n-nodes-kanban.planka"

log() { printf '[bootstrap] %s\n' "$*"; }

step_planka_readonly_role() {
  log "step 1/7: ensure planka_readonly Postgres role"
  local existing
  existing=$($SSH "$SUDO -u postgres psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='planka_readonly'\"" 2>&1 | tail -1)
  if [[ "$existing" == "1" ]]; then
    log "  planka_readonly role already exists, skipping create+grant"
    return 0
  fi
  local pw
  pw=$(openssl rand -hex 24)
  $SSH "$SUDO -u postgres psql -d kanban <<SQL
CREATE ROLE planka_readonly LOGIN PASSWORD '$pw';
GRANT CONNECT ON DATABASE kanban TO planka_readonly;
GRANT USAGE ON SCHEMA public TO planka_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO planka_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO planka_readonly;
SQL
"
  log "  created planka_readonly with random password"
  printf '%s' "$pw" > /tmp/.planka-ro.pw
  chmod 600 /tmp/.planka-ro.pw
}

step_pgbouncer_userlist() {
  log "step 2/7: ensure pgbouncer userlist contains planka_readonly"
  local entry
  entry=$($SSH "$SUDO kubectl get secret pgbouncer-userlist -n pgbouncer -o jsonpath='{.data.userlist\\.txt}' | base64 -d | grep '^\"planka_readonly\"' || true" 2>&1 | tail -1)
  if [[ -n "$entry" && "$entry" != "Defaulted"* ]]; then
    log "  pgbouncer userlist already has planka_readonly, skipping reload"
    return 0
  fi
  local scram
  scram=$($SSH "$SUDO -u postgres psql -tAc \"SELECT rolpassword FROM pg_authid WHERE rolname='planka_readonly'\"" 2>&1 | tail -1)
  cat > /tmp/_pgb_update.sh <<SCRIPT
#!/bin/bash
set -e
SCRAM='$scram'
CURRENT=\$(sudo kubectl get secret pgbouncer-userlist -n pgbouncer -o jsonpath="{.data.userlist\\.txt}" | base64 -d)
NEW=\$(echo "\$CURRENT" | grep -v '^"planka_readonly"')
NEW="\${NEW}
\"planka_readonly\" \"\${SCRAM}\""
ENCODED=\$(printf '%s' "\$NEW" | base64 -w0)
sudo kubectl patch secret pgbouncer-userlist -n pgbouncer --type=json -p="[{\"op\":\"replace\",\"path\":\"/data/userlist.txt\",\"value\":\"\${ENCODED}\"}]"
sudo kubectl rollout restart deploy/pgbouncer -n pgbouncer
sudo kubectl rollout status deploy/pgbouncer -n pgbouncer --timeout=60s
SCRIPT
  scp -i "$SSH_KEY" -q /tmp/_pgb_update.sh "$SSH_TARGET:/tmp/"
  $SSH "bash /tmp/_pgb_update.sh && rm /tmp/_pgb_update.sh" >/dev/null
  rm -f /tmp/_pgb_update.sh
  log "  added planka_readonly SCRAM verifier and reloaded pgbouncer"
}

step_k8s_secret() {
  log "step 3/7: ensure k8s secret kanban/planka-readonly-db"
  if [[ ! -f /tmp/.planka-ro.pw ]]; then
    log "  password not in /tmp/.planka-ro.pw — skipping (role already existed; secret should be there)"
    return 0
  fi
  local pw
  pw=$(cat /tmp/.planka-ro.pw)
  $SSH "$SUDO bash -c 'kubectl create secret generic planka-readonly-db -n kanban \
    --from-literal=DB_HOST=pgbouncer.pgbouncer.svc.cluster.local \
    --from-literal=DB_PORT=6432 \
    --from-literal=DB_NAME=kanban \
    --from-literal=DB_USER=planka_readonly \
    --from-literal=DB_PASSWORD=\"$pw\" \
    --dry-run=client -o yaml | kubectl apply -f -'" >/dev/null
  log "  applied planka-readonly-db secret"
}

step_n8n_credential() {
  log "step 4/7: ensure n8n planka-readonly credential"
  local exists
  exists=$($SSH "$SUDO -u postgres psql -d n8n -tAc \"SELECT 1 FROM credentials_entity WHERE id='planka-readonly'\"" 2>&1 | tail -1)
  if [[ "$exists" == "1" ]]; then
    log "  n8n credential planka-readonly already exists, skipping import"
    return 0
  fi
  local pw
  pw=$($SSH "$SUDO kubectl get secret planka-readonly-db -n kanban -o jsonpath='{.data.DB_PASSWORD}' | base64 -d" 2>&1 | tail -1)
  cat > /tmp/_planka-ro.json <<JSON
[{"id":"planka-readonly","name":"Planka Readonly","type":"postgres","data":{"host":"pgbouncer.pgbouncer.svc.cluster.local","port":6432,"database":"kanban","user":"planka_readonly","password":"$pw","ssl":"disable","allowUnauthorizedCerts":false}}]
JSON
  scp -i "$SSH_KEY" -q /tmp/_planka-ro.json "$SSH_TARGET:/tmp/"
  rm -f /tmp/_planka-ro.json
  local pod
  pod=$($SSH "$SUDO kubectl get pod -n n8n -l app.kubernetes.io/component=server -o jsonpath='{.items[0].metadata.name}'" 2>&1 | tail -1)
  $SSH "$SUDO kubectl cp /tmp/_planka-ro.json n8n/$pod:/tmp/_planka-ro.json && \
        $SUDO kubectl exec -n n8n $pod -- n8n import:credentials --input=/tmp/_planka-ro.json && \
        $SUDO kubectl exec -n n8n $pod -- rm -f /tmp/_planka-ro.json && \
        rm -f /tmp/_planka-ro.json" >/dev/null
  log "  imported planka-readonly credential"
}

step_workflow_history() {
  log "step 5/7: ensure workflow_history rows for telemetry workflows"
  $SSH "$SUDO -u postgres psql -d n8n <<SQL
INSERT INTO workflow_history (\"versionId\", \"workflowId\", authors, nodes, connections, name, autosaved)
SELECT gen_random_uuid()::text, w.id, 'system', w.nodes, w.connections, w.name, false
FROM workflow_entity w
WHERE w.name IN ('kanban-bounce-recorder','watchdog-doing-2min','watchdog-qa-5min')
  AND NOT EXISTS (SELECT 1 FROM workflow_history h WHERE h.\"workflowId\" = w.id);
UPDATE workflow_entity w
SET \"activeVersionId\" = (SELECT h.\"versionId\" FROM workflow_history h WHERE h.\"workflowId\" = w.id ORDER BY h.\"createdAt\" DESC LIMIT 1)
WHERE w.name IN ('kanban-bounce-recorder','watchdog-doing-2min','watchdog-qa-5min')
  AND w.\"activeVersionId\" IS NULL;
SQL
" >/dev/null
  log "  workflow_history rows present, activeVersionId set"
}

step_activate_workflows() {
  log "step 6/7: activate the 3 telemetry workflows"
  $SSH "$SUDO -u postgres psql -d n8n -c \"UPDATE workflow_entity SET active=true, \\\"updatedAt\\\"=NOW() WHERE name IN ('kanban-bounce-recorder','watchdog-doing-2min','watchdog-qa-5min') AND active=false\"" 2>&1 | tail -1 | grep -q UPDATE && {
    log "  rows flipped to active=true; restarting n8n to register triggers"
    $SSH "$SUDO kubectl rollout restart deploy/n8n -n n8n && $SUDO kubectl rollout status deploy/n8n -n n8n --timeout=120s" >/dev/null
  } || log "  all three workflows already active"
}

step_planka_webhook() {
  log "step 7/7: ensure Planka outbound webhook"
  local admin_email admin_pw
  admin_email=$($SSH "$SUDO kubectl get deploy kanban -n kanban -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"DEFAULT_ADMIN_EMAIL\")].value}'" 2>&1 | tail -1)
  admin_pw=$($SSH "$SUDO kubectl get secret kanban-credentials -n kanban -o jsonpath='{.data.default-admin-password}' | base64 -d" 2>&1 | tail -1)
  local check
  check=$($SSH "$SUDO kubectl run wh-check-\$RANDOM -n kanban --rm -i --restart=Never --image=curlimages/curl:latest --quiet -- sh -c 'TOKEN=\$(curl -sS -X POST -H \"Content-Type: application/json\" -d \"{\\\"emailOrUsername\\\":\\\"$admin_email\\\",\\\"password\\\":\\\"$admin_pw\\\"}\" http://kanban.kanban.svc.cluster.local:3333/api/access-tokens | sed \"s/.*\\\"item\\\":\\\"//;s/\\\".*//\"); curl -sS -H \"Authorization: Bearer \$TOKEN\" http://kanban.kanban.svc.cluster.local:3333/api/webhooks' 2>&1" 2>&1 | tail -1)
  if echo "$check" | grep -q "$PLANKA_WEBHOOK_NAME"; then
    log "  webhook $PLANKA_WEBHOOK_NAME already configured, skipping create"
    return 0
  fi
  $SSH "$SUDO kubectl run wh-create-\$RANDOM -n kanban --rm -i --restart=Never --image=curlimages/curl:latest --quiet -- sh -c 'TOKEN=\$(curl -sS -X POST -H \"Content-Type: application/json\" -d \"{\\\"emailOrUsername\\\":\\\"$admin_email\\\",\\\"password\\\":\\\"$admin_pw\\\"}\" http://kanban.kanban.svc.cluster.local:3333/api/access-tokens | sed \"s/.*\\\"item\\\":\\\"//;s/\\\".*//\"); curl -sS -X POST -H \"Authorization: Bearer \$TOKEN\" -H \"Content-Type: application/json\" -d \"{\\\"name\\\":\\\"$PLANKA_WEBHOOK_NAME\\\",\\\"url\\\":\\\"$PLANKA_WEBHOOK_URL\\\",\\\"events\\\":\\\"cardUpdate\\\"}\" http://kanban.kanban.svc.cluster.local:3333/api/webhooks'" >/dev/null 2>&1
  log "  created Planka outbound webhook $PLANKA_WEBHOOK_NAME → $PLANKA_WEBHOOK_URL"
}

main() {
  step_planka_readonly_role
  step_pgbouncer_userlist
  step_k8s_secret
  step_n8n_credential
  step_workflow_history
  step_activate_workflows
  step_planka_webhook
  rm -f /tmp/.planka-ro.pw
  log "done — telemetry pipeline provisioned"
}

main "$@"
