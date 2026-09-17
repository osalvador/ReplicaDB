#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-cleanup-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
LOG_FILE="$TEMP_ROOT/commands.log"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLEANUP_LOG:?}"
case "$*" in
    *'services list'*) printf 'api-owned\n' ;;
    *'worker-pools list'*) printf 'worker-owned\n' ;;
esac
exit 0
EOF
chmod 755 "$STUB_BIN/gcloud"
export PATH="$STUB_BIN:$PATH" CLEANUP_LOG="$LOG_FILE"
export PROJECT_ID=test-project REGION=europe-west4 DEPLOYMENT_ID=cleanup-test
export STATE_FILE="$TEMP_ROOT/deployment.state"
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/state.sh"
source "$TEST_DIR/../lib/cloud_run_service.sh"
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/cleanup.sh"

state_init "$STATE_FILE"
state_put deploymentId cleanup-test
state_put projectId test-project
state_put region europe-west4
state_put mode distributed
state_put publicAccess true
state_put apiServiceName api-owned
state_put workerPoolName worker-owned
state_put cloudSqlInstance sql-owned
state_put cloudSqlOwned true
state_put secretsOwned owned-a,owned-b
state_save

if cleanup_destroy >/dev/null 2>&1; then exit 1; fi
[[ -f "$STATE_FILE" ]] || exit 1
CONFIRMATION='DESTROY REPLICADB'
cleanup_destroy >/dev/null
[[ ! -f "$STATE_FILE" ]] || exit 1
grep -Eq 'worker-pools delete' "$LOG_FILE"
grep -Eq 'services delete' "$LOG_FILE"
remove_line=$(grep -n 'remove-iam-policy-binding' "$LOG_FILE" | head -1 | cut -d: -f1)
delete_line=$(grep -n 'services delete' "$LOG_FILE" | head -1 | cut -d: -f1)
(( remove_line < delete_line ))
grep -Eq 'secrets delete owned-a' "$LOG_FILE"
grep -Eq 'sql instances delete sql-owned' "$LOG_FILE"

: >"$LOG_FILE"
state_init "$STATE_FILE"
state_put deploymentId cleanup-test
state_put projectId test-project
state_put region europe-west4
state_put mode simple
state_put apiServiceName api-owned
state_put cloudSqlInstance sql-shared
state_put cloudSqlOwned false
state_put secretsOwned owned-a
state_save
CLEANUP_KEEP_CLOUD_SQL=true
CLEANUP_KEEP_SECRETS=true
cleanup_destroy >/dev/null
if grep -Eq 'sql instances delete sql-shared|secrets delete owned-a' "$LOG_FILE"; then exit 1; fi

rm -f "$STATE_FILE"
CLEANUP_ORPHAN_REPORT=true
cleanup_collect
cleanup_print_plan | grep -Eq 'api-owned|worker-owned'

printf 'cleanup tests passed\n'
