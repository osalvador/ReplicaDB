#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
project_name=${COMPOSE_PROJECT_NAME:-replicadb-phase3-load-$PPID}
state_directory="$repository_root/.phase3-compose/$project_name"
cookie_file="$state_directory/cookies.txt"
jobs_to_run=${PHASE3_LOAD_RUNS:-8}
load_seed=${PHASE3_LOAD_SEED:-phase3-load}

if [[ "$jobs_to_run" -lt 1 || "$jobs_to_run" -gt 32 ]]; then
    printf 'PHASE3_LOAD_RUNS must be between 1 and 32\n' >&2
    exit 2
fi

export COMPOSE_PROFILES=multinode
export REPLICADB_WORKER_LEASE_DURATION=${REPLICADB_WORKER_LEASE_DURATION:-5s}
export REPLICADB_WORKER_HEARTBEAT_INTERVAL=${REPLICADB_WORKER_HEARTBEAT_INTERVAL:-1s}
export REPLICADB_WORKER_POLL_INTERVAL=${REPLICADB_WORKER_POLL_INTERVAL:-1s}

mkdir -p "$state_directory"

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

compose() {
    docker compose -p "$project_name" -f "$repository_root/docker-compose.server.yml" "$@"
}

step() {
    printf 'phase3 load: %s\n' "$1"
}

cleanup() {
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$state_directory"
}
trap cleanup EXIT

wait_for_liveness() {
    local endpoint=$1
    curl --retry 120 --retry-delay 1 --retry-all-errors -fsS "$endpoint" >/dev/null
}

load_fixture() {
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        < "$repository_root/replicadb-server/src/test/resources/phase3/fixture.sql" >/dev/null
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL >/dev/null
TRUNCATE TABLE phase3_source, phase3_sink;
INSERT INTO phase3_source (id, payload)
SELECT value, '$load_seed-' || value
FROM generate_series(1, 3) AS values(value);
SQL
}

authenticate() {
    local api_url=$1
    local csrf_response
    csrf_response=$(curl -fsS -c "$cookie_file" "$api_url/api/v1/auth/csrf")
    csrf_token=$(printf '%s' "$csrf_response" | jq -r '.token')
    test -n "$csrf_token"
    curl -fsS -c "$cookie_file" -b "$cookie_file" \
        -H 'Content-Type: application/json' \
        --data "{\"username\":\"$REPLICADB_BOOTSTRAP_ADMIN_USERNAME\",\"password\":\"$REPLICADB_BOOTSTRAP_ADMIN_PASSWORD\"}" \
        "$api_url/api/v1/auth/login" >/dev/null
}

create_load_sinks() {
    local index
    for index in $(seq 1 "$jobs_to_run"); do
        compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
            "CREATE TABLE phase3_load_sink_${index} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)" >/dev/null
    done
}

create_job() {
    local api_url=$1
    local index=$2
    local payload
    payload=$(jq -n \
        --arg name "phase3 load $load_seed $index" \
        --arg sink_table "phase3_load_sink_$index" \
        --arg source_password '${env:DB_PASSWORD}' \
        --arg sink_password '${env:DB_PASSWORD}' \
        '{name: $name, sourceConnect: "jdbc:postgresql://postgres:5432/replicadb", sourceUser: "postgres", sourcePassword: $source_password, sourceTable: "phase3_source", sourceColumns: "id,payload", sinkConnect: "jdbc:postgresql://postgres:5432/replicadb", sinkUser: "postgres", sinkPassword: $sink_password, sinkTable: $sink_table, mode: "complete-atomic", jobs: 1, maxAttempts: 1, retryBackoffSeconds: 0, automaticRetryEnabled: false}')
    curl -fsS -b "$cookie_file" \
        -H 'Content-Type: application/json' \
        -H "X-XSRF-TOKEN: $csrf_token" \
        --data "$payload" \
        "$api_url/api/v1/jobs" | jq -r '.id'
}

trigger_job() {
    local api_url=$1
    local job_id=$2
    local index=$3
    curl -fsS -b "$cookie_file" \
        -H 'Content-Type: application/json' \
        -H "X-XSRF-TOKEN: $csrf_token" \
        -H "Idempotency-Key: $load_seed-$index" \
        -X POST "$api_url/api/v1/jobs/$job_id/runs" | jq -r '.id'
}

read_run_status() {
    local api_url=$1
    local run_id=$2
    curl --retry 20 --retry-delay 1 --retry-all-errors -fsS -b "$cookie_file" \
        "$api_url/api/v1/runs/$run_id" | jq -r '.status'
}

assert_run_completion() {
    local api_url=$1
    local run_id=$2
    local status
    for attempt in $(seq 1 180); do
        status=$(read_run_status "$api_url" "$run_id")
        if [[ "$status" = SUCCEEDED ]]; then
            return
        fi
        if [[ "$status" = FAILED || "$status" = CANCELLED ]]; then
            printf 'load run failed with status=%s\n' "$status" >&2
            exit 1
        fi
    done
    printf 'load run did not reach terminal success\n' >&2
    exit 1
}

step 'start two APIs and two workers'
compose up -d --build --wait >/dev/null
api_one_port=$(compose port api-one 8080 | awk -F: '{print $NF}')
api_two_port=$(compose port api-two 8080 | awk -F: '{print $NF}')
api_one="http://127.0.0.1:$api_one_port"
api_two="http://127.0.0.1:$api_two_port"
step 'wait for liveness and load fixture'
wait_for_liveness "$api_one/actuator/health/liveness"
wait_for_liveness "$api_two/actuator/health/liveness"
load_fixture
create_load_sinks
authenticate "$api_one"

job_file="$state_directory/jobs.txt"
run_file="$state_directory/runs.txt"
for index in $(seq 1 "$jobs_to_run"); do
    job_id=$(create_job "$api_one" "$index")
    printf '%s|%s\n' "$job_id" "$index" >> "$job_file"
done

step "dispatch $jobs_to_run runs concurrently"
start_ns=$(date +%s%N)
while IFS='|' read -r job_id index; do
    api_url=$api_one
    if (( index % 2 == 0 )); then
        api_url=$api_two
    fi
    (
        run_id=$(trigger_job "$api_url" "$job_id" "$index")
        printf '%s|%s|%s\n' "$run_id" "$job_id" "$index" >> "$run_file"
    ) &
done < "$job_file"
wait

while IFS='|' read -r run_id job_id index; do
    assert_run_completion "$api_two" "$run_id" &
done < "$run_file"
wait
end_ns=$(date +%s%N)
elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))

actual_runs=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    "SELECT count(*) FROM job_run WHERE job_definition_id IN (SELECT id FROM job_definition WHERE name LIKE 'phase3 load $load_seed %')" \
    | tr -d '[:space:]')
test "$actual_runs" = "$jobs_to_run"

for index in $(seq 1 "$jobs_to_run"); do
    sink_count=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
        "SELECT count(*) FROM phase3_load_sink_${index}" | tr -d '[:space:]')
    test "$sink_count" = 3
done

worker_metrics=$(for worker_service in worker-one worker-two; do
    compose exec -T "$worker_service" curl -fsS http://127.0.0.1:9091/actuator/prometheus
done)
printf '%s\n' "$worker_metrics" | grep -Fq 'replicadb_managed_claims_total'
printf '%s\n' "$worker_metrics" | grep -Fq 'replicadb_managed_terminal_outcomes_total'
printf '%s\n' "$worker_metrics" | grep -Fq 'replicadb_managed_notification_claim_latency'
printf '%s\n' "$worker_metrics" | grep -Fq 'replicadb_managed_polling_scans_total'
if printf '%s\n' "$worker_metrics" | grep -Eq 'job_id=|run_id=|lease_token=|password=|jdbc:'; then
    printf 'high-cardinality or secret-bearing metric label found\n' >&2
    exit 1
fi

printf 'phase3 load validation passed runs=%s elapsed_ms=%s metrics=present\n' "$actual_runs" "$elapsed_ms"
