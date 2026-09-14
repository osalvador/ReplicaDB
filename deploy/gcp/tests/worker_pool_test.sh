#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-worker-pool-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
LOG_FILE="$TEMP_ROOT/commands.log"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${WORKER_POOL_LOG:?}"
[[ "${WORKER_POOL_SCENARIO:-valid}" == missing-beta ]] && exit 1
exit 0
EOF
chmod 755 "$STUB_BIN/gcloud"
export PATH="$STUB_BIN:$PATH" WORKER_POOL_LOG="$LOG_FILE"
export PROJECT_ID=test-project REGION=europe-west4 MODE=distributed DEPLOYMENT_ID=worker-test
export FINAL_IMAGE=osalvador/replicadb-server:1.0.0@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export WORKER_SERVICE_ACCOUNT=worker@test-project.iam.gserviceaccount.com
export NETWORK=network SUBNET=subnet WORKER_INSTANCES=3
export DB_USERNAME_SECRET_NAME=db-user DB_USERNAME_SECRET_VERSION=3
export DB_PASSWORD_SECRET_NAME=db-password DB_PASSWORD_SECRET_VERSION=4
export KEYRING_VERSION_SECRET_NAME=keyring-version KEYRING_VERSION_SECRET_VERSION=5
export KEYRING_KEY_SECRET_NAME=keyring-key KEYRING_KEY_SECRET_VERSION=6
export REPLICADB_WORKER_PLATFORM_PROBE=true
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/naming.sh"
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/worker_pool.sh"

worker_pool_deploy
rg -q 'worker-pools deploy' "$LOG_FILE"
rg -q 'SERVER_PORT=-1' "$LOG_FILE"
rg -q 'REPLICADB_WORKER_MANAGEMENT_ADDRESS=0.0.0.0' "$LOG_FILE"
rg -q 'DB_USERNAME=db-user:3' "$LOG_FILE"
rg -q 'tcpSocket.port=9091' "$LOG_FILE"
if rg -q 'allow-unauthenticated|run.invoker' "$LOG_FILE"; then exit 1; fi

WORKER_INSTANCES=0
worker_pool_deploy
rg -q 'worker-pools update.*instances=0' "$LOG_FILE"

WORKER_INSTANCES=1
REPLICADB_WORKER_PLATFORM_PROBE=false
WORKER_MANAGEMENT_ADDRESS=0.0.0.0
if worker_pool_deploy >/dev/null 2>&1; then exit 1; fi

WORKER_MANAGEMENT_ADDRESS=127.0.0.1
export WORKER_POOL_SCENARIO=missing-beta
if worker_pool_deploy >/dev/null 2>&1; then exit 1; fi

printf 'worker pool tests passed\n'