#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-cloud-run-service-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
mkdir -p "$TEMP_ROOT/bin"
cat >"$TEMP_ROOT/bin/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLOUD_RUN_LOG:?}"
case "$*" in
	*'services describe'*) printf 'https://api.example.invalid\n' ;;
esac
EOF
chmod 755 "$TEMP_ROOT/bin/gcloud"
export PATH="$TEMP_ROOT/bin:$PATH" CLOUD_RUN_LOG="$TEMP_ROOT/gcloud.log"
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/naming.sh"
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/cloud_run_service.sh"

export DEPLOYMENT_ID=service-test PROJECT_ID=test-project REGION=europe-west4
export FINAL_IMAGE=osalvador/replicadb-server:1.0.0@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export SERVICE_ACCOUNT=api@test-project.iam.gserviceaccount.com
export NETWORK=network SUBNET=subnet DATABASE=replicadb CLOUD_SQL_PRIVATE_IP=10.0.0.4
export API_MIN_INSTANCES=1 API_MAX_INSTANCES=7
export DB_USERNAME_SECRET_NAME=db-user DB_USERNAME_SECRET_VERSION=3
export DB_PASSWORD_SECRET_NAME=db-password DB_PASSWORD_SECRET_VERSION=4
export KEYRING_VERSION_SECRET_NAME=keyring-version KEYRING_VERSION_SECRET_VERSION=5
export KEYRING_KEY_SECRET_NAME=keyring-key KEYRING_KEY_SECRET_VERSION=6
export BOOTSTRAP_USERNAME_SECRET_NAME=bootstrap-user BOOTSTRAP_USERNAME_SECRET_VERSION=7
export BOOTSTRAP_PASSWORD_SECRET_NAME=bootstrap-password BOOTSTRAP_PASSWORD_SECRET_VERSION=8

output="$TEMP_ROOT/service.yaml"
cloud_run_render_service "$output" true
rg -q 'SPRING_PROFILES_ACTIVE' "$output"
rg -q 'REPLICADB_SERVER_LOCAL_EXECUTION_ENABLED' "$output"
rg -q 'value: "true"' "$output"
rg -q 'path: /actuator/health/liveness' "$output"
rg -q 'path: /actuator/health/readiness' "$output"
rg -q 'key: "8"' "$output"
rg -q 'cpu-throttling: "false"' "$output"
rg -q 'minScale: "1"' "$output"
rg -q 'maxScale: "7"' "$output"
if rg -q 'name: PORT' "$output"; then exit 1; fi
if rg -q 'allow-unauthenticated|roles/run.invoker' "$output"; then exit 1; fi
if rg -q 'secret|password' "$output" && rg -q 'value: .*password|value: .*secret' "$output"; then exit 1; fi

cloud_run_deploy_service true >/dev/null
first_name=$CLOUD_RUN_SERVICE_NAME
cloud_run_deploy_service true >/dev/null
[[ "$CLOUD_RUN_SERVICE_NAME" == "$first_name" ]] || exit 1
[[ "$(rg -c 'run services replace' "$CLOUD_RUN_LOG")" == 2 ]] || exit 1

printf 'cloud run service tests passed\n'