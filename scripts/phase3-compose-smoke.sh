#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
project_name=${COMPOSE_PROJECT_NAME:-replicadb-phase3-$PPID}
state_directory="$repository_root/.phase3-compose/$project_name"
cookie_file="$state_directory/cookies.txt"

mkdir -p "$state_directory"

for required_command in docker curl awk sed tail tr openssl; do
    command -v "$required_command" >/dev/null 2>&1 || {
        printf 'Required command not found: %s\n' "$required_command" >&2
        exit 2
    }
done

if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
    POSTGRES_PASSWORD=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
    export POSTGRES_PASSWORD
fi
export POSTGRES_DB=${POSTGRES_DB:-replicadb}
export POSTGRES_USER=${POSTGRES_USER:-postgres}
export REPLICADB_BOOTSTRAP_ADMIN_USERNAME=${REPLICADB_BOOTSTRAP_ADMIN_USERNAME:-phase3-admin}
if [[ -z "${REPLICADB_BOOTSTRAP_ADMIN_PASSWORD:-}" ]]; then
    REPLICADB_BOOTSTRAP_ADMIN_PASSWORD=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
    export REPLICADB_BOOTSTRAP_ADMIN_PASSWORD
fi

if [[ -z "${REPLICADB_SECURITY_MASTER_KEY_FILE:-}" ]]; then
    REPLICADB_SECURITY_MASTER_KEY_FILE="$state_directory/replicadb-master-key.json"
    key_material=$(openssl rand -base64 32)
    printf '{"currentVersion":"local","keys":{"local":"%s"}}\n' "$key_material" \
        >"$REPLICADB_SECURITY_MASTER_KEY_FILE"
    chmod 600 "$REPLICADB_SECURITY_MASTER_KEY_FILE"
    export REPLICADB_SECURITY_MASTER_KEY_FILE
fi

compose() {
    docker compose -p "$project_name" -f "$repository_root/docker-compose.server.yml" "$@"
}

step() {
    printf 'phase3 compose smoke: %s\n' "$1"
}

assert_worker_topology() {
    local worker_service
    local container_id
    local worker_port_bindings
    for worker_service in worker-one; do
        container_id=$(compose ps -q "$worker_service")
        test -n "$container_id"
        worker_port_bindings=$(docker inspect "$container_id" \
            --format '{{json .HostConfig.PortBindings}}')
        test "$worker_port_bindings" = "{}" -o "$worker_port_bindings" = "null"
    done
    if [[ "${PHASE3_EXPECT_WORKERS:-1}" -ge 2 ]]; then
        container_id=$(compose ps -q worker-two)
        test -n "$container_id"
        worker_port_bindings=$(docker inspect "$container_id" \
            --format '{{json .HostConfig.PortBindings}}')
        test "$worker_port_bindings" = "{}" -o "$worker_port_bindings" = "null"
    fi
}

cleanup() {
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$state_directory"
}
trap cleanup EXIT

step 'start services'
compose up -d --build --wait >/dev/null

api_one_port=$(compose port api-one 8080 | awk -F: '{print $NF}')
api_two_port=$(compose port api-two 8080 | awk -F: '{print $NF}')
api_one="http://127.0.0.1:$api_one_port"
api_two="http://127.0.0.1:$api_two_port"

step 'wait for liveness'
curl --retry 120 --retry-delay 1 --retry-all-errors -fsS "$api_one/actuator/health/liveness" >/dev/null
curl --retry 120 --retry-delay 1 --retry-all-errors -fsS "$api_two/actuator/health/liveness" >/dev/null

step 'load fixture'
compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    < "$repository_root/replicadb-server/src/test/resources/phase3/fixture.sql" >/dev/null

step 'bootstrap csrf'
csrf_response=$(curl -fsS -c "$cookie_file" "$api_one/api/v1/auth/csrf")
csrf_token=$(printf '%s' "$csrf_response" | sed -nE 's/.*"token":"([^"]+)".*/\1/p')
test -n "$csrf_token"

step 'login'
curl -fsS -c "$cookie_file" -b "$cookie_file" \
    -H 'Content-Type: application/json' \
    --data "{\"username\":\"$REPLICADB_BOOTSTRAP_ADMIN_USERNAME\",\"password\":\"$REPLICADB_BOOTSTRAP_ADMIN_PASSWORD\"}" \
    "$api_one/api/v1/auth/login" >/dev/null

step 'create source datasource'
source_datasource_response=$(curl -fsS -b "$cookie_file" \
    -H 'Content-Type: application/json' \
    -H "X-XSRF-TOKEN: $csrf_token" \
    --data-binary @- "$api_one/api/v1/datasources" <<EOF
{"name":"phase3 compose source","connectorType":"postgres","technicalParams":{},"security":{"connect":"jdbc:postgresql://postgres:5432/$POSTGRES_DB","user":"$POSTGRES_USER","password":"$POSTGRES_PASSWORD"},"clearSecurityKeys":[]}
EOF
)
source_datasource_id=$(printf '%s' "$source_datasource_response" | sed -nE 's/.*"id":"([^\"]+)".*/\1/p')
test -n "$source_datasource_id"

step 'create sink datasource'
sink_datasource_response=$(curl -fsS -b "$cookie_file" \
    -H 'Content-Type: application/json' \
    -H "X-XSRF-TOKEN: $csrf_token" \
    --data-binary @- "$api_one/api/v1/datasources" <<EOF
{"name":"phase3 compose sink","connectorType":"postgres","technicalParams":{},"security":{"connect":"jdbc:postgresql://postgres:5432/$POSTGRES_DB","user":"$POSTGRES_USER","password":"$POSTGRES_PASSWORD"},"clearSecurityKeys":[]}
EOF
)
sink_datasource_id=$(printf '%s' "$sink_datasource_response" | sed -nE 's/.*"id":"([^\"]+)".*/\1/p')
test -n "$sink_datasource_id"

step 'create job'
job_response=$(curl -fsS -b "$cookie_file" \
    -H 'Content-Type: application/json' \
    -H "X-XSRF-TOKEN: $csrf_token" \
    --data-binary @- "$api_one/api/v1/jobs" <<EOF
{"name":"phase3 compose smoke","sourceDatasourceId":"$source_datasource_id","sourceDatasourceUseEnabled":true,"sourceTable":"phase3_source","sinkDatasourceId":"$sink_datasource_id","sinkDatasourceUseEnabled":true,"sinkTable":"phase3_sink","mode":"complete-atomic","jobs":1}
EOF
)
job_id=$(printf '%s' "$job_response" | jq -er '.id')
test -n "$job_id"

step 'share schedule'
schedule_response=$(curl -sS -w '\n%{http_code}' -b "$cookie_file" \
    -H 'Content-Type: application/json' \
    -H "X-XSRF-TOKEN: $csrf_token" \
    --data '{"cronExpression":"0 0 1 1 1 ?","timeZone":"UTC","enabled":true}' \
    -X PUT \
    "$api_one/api/v1/jobs/$job_id/schedule" || true)
schedule_status=$(printf '%s' "$schedule_response" | tail -1)
if [[ "$schedule_status" != 200 ]]; then
    printf 'schedule_status=%s\n' "$schedule_status" >&2
    printf '%s\n' "$schedule_response" | sed '$d' | sed -E 's/(password|token|jdbc:[^" ]*)/[redacted]/Ig' | cut -c1-500 >&2
    exit 1
fi
curl -fsS -b "$cookie_file" "$api_two/api/v1/jobs/$job_id/schedule" >/dev/null

step 'trigger run'
run_response=$(curl -fsS -b "$cookie_file" \
    -H 'Content-Type: application/json' \
    -H "X-XSRF-TOKEN: $csrf_token" \
    -H 'Idempotency-Key: phase3-compose-smoke-run' \
    -X POST "$api_one/api/v1/jobs/$job_id/runs")
run_id=$(printf '%s' "$run_response" | sed -nE 's/.*"runId":"([^"]+)".*/\1/p')
if [[ -z "$run_id" ]]; then
    run_id=$(printf '%s' "$run_response" | sed -nE 's/.*"id":"([^"]+)".*/\1/p')
fi
test -n "$run_id"

step 'wait for worker run'
run_status=
for _ in $(seq 1 120); do
    run_status=$(curl -fsS -b "$cookie_file" "$api_two/api/v1/runs/$run_id" \
        | sed -nE 's/.*"status":"([^"]+)".*/\1/p')
    case "$run_status" in
        SUCCEEDED) break ;;
        FAILED|CANCELLED) printf 'unexpected run status: %s\n' "$run_status" >&2; exit 1 ;;
    esac
done
test "$run_status" = SUCCEEDED

step 'verify sink'
sink_count=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    'SELECT count(*) FROM phase3_sink' | tr -d '[:space:]')
assert_worker_topology
printf 'sink_count=%s worker_management_published=%s\n' "$sink_count" \
    no
test "$sink_count" = 3
if [[ "${PHASE3_EXPECT_WORKERS:-1}" -ge 2 ]]; then
    printf 'worker_processes=2\n'
fi

printf 'phase3 compose smoke passed with two APIs and one worker\n'
