#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-stop-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
LOG_FILE="$TEMP_ROOT/commands.log"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STOP_LOG:?}"
printf 'python=%s sitepackages=%s\n' "${CLOUDSDK_PYTHON:-}" "${CLOUDSDK_PYTHON_SITEPACKAGES:-}" >>"${STOP_LOG:?}"
case "$*" in
    *'run services describe'*|*'beta run worker-pools describe'*) exit 0 ;;
    *) exit 0 ;;
esac
EOF
chmod 755 "$STUB_BIN/gcloud"
export PATH="$STUB_BIN:$PATH" STOP_LOG="$LOG_FILE"
export STATE_FILE="$TEMP_ROOT/deployment.state"

# shellcheck disable=SC1091
source "$TEST_DIR/../lib/state.sh"
state_init "$STATE_FILE"
state_put deploymentId stop-test
state_put projectId test-project
state_put region europe-west4
state_put mode distributed
state_put apiServiceName api-owned
state_put workerPoolName worker-owned
state_put cloudSqlInstance sql-shared
state_put cloudSqlOwned false
state_save

if REPLICADB_DEPLOYMENT_ID= CLOUDSDK_PYTHON=/test/python CLOUDSDK_PYTHON_SITEPACKAGES= bash "$TEST_DIR/../stop.sh" --state-file "$STATE_FILE" --confirmation 'WRONG' >/dev/null 2>&1; then exit 1; fi
REPLICADB_DEPLOYMENT_ID= CLOUDSDK_PYTHON=/test/python CLOUDSDK_PYTHON_SITEPACKAGES= bash "$TEST_DIR/../stop.sh" --state-file "$STATE_FILE" --confirmation 'STOP REPLICADB' --non-interactive >/dev/null
grep -Eq 'python=/test/python sitepackages=1' "$LOG_FILE"
grep -Eq 'run services update api-owned.*--min=0' "$LOG_FILE"
grep -Eq 'run services update api-owned.*--min-instances=0' "$LOG_FILE"
grep -Eq 'worker-pools update worker-owned.*--instances=0' "$LOG_FILE"
if grep -Eq 'sql instances patch|secrets (delete|versions)' "$LOG_FILE"; then exit 1; fi

: >"$LOG_FILE"
state_init "$STATE_FILE"
state_put deploymentId stop-test
state_put projectId test-project
state_put region europe-west4
state_put mode simple
state_put apiServiceName api-owned
state_put cloudSqlInstance sql-owned
state_put cloudSqlOwned true
state_save
REPLICADB_DEPLOYMENT_ID= CLOUDSDK_PYTHON=/test/python CLOUDSDK_PYTHON_SITEPACKAGES= bash "$TEST_DIR/../stop.sh" --state-file "$STATE_FILE" --confirmation 'STOP REPLICADB' --non-interactive >/dev/null
grep -Eq 'sql instances patch sql-owned.*activation-policy=NEVER' "$LOG_FILE"
if grep -Eq 'worker-pools update' "$LOG_FILE"; then exit 1; fi

printf 'stop tests passed\n'
