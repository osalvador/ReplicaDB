#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
scenario=${1:-all}
project_name=${COMPOSE_PROJECT_NAME:-replicadb-phase3-resilience-$PPID}
state_directory="$repository_root/.phase3-compose/$project_name"
cookie_file="$state_directory/cookies.txt"

export COMPOSE_PROFILES=multinode
export REPLICADB_WORKER_LEASE_DURATION=${REPLICADB_WORKER_LEASE_DURATION:-5s}
export REPLICADB_WORKER_HEARTBEAT_INTERVAL=${REPLICADB_WORKER_HEARTBEAT_INTERVAL:-1s}
export REPLICADB_WORKER_POLL_INTERVAL=${REPLICADB_WORKER_POLL_INTERVAL:-1s}
export REPLICADB_WORKER_ONE_MAX_CONCURRENT_RUNS=${PHASE3_RESILIENCE_WORKER_ONE_CAPACITY:-1}
export REPLICADB_WORKER_TWO_MAX_CONCURRENT_RUNS=${PHASE3_RESILIENCE_WORKER_TWO_CAPACITY:-1}
export REPLICADB_WORKER_ONE_ADMISSION_JITTER_MAX=${PHASE3_RESILIENCE_JITTER_MAX:-25ms}
export REPLICADB_WORKER_TWO_ADMISSION_JITTER_MAX=${PHASE3_RESILIENCE_JITTER_MAX:-25ms}
export REPLICADB_WORKER_ONE_ADMISSION_GENERIC_COOLDOWN=${PHASE3_RESILIENCE_COOLDOWN:-100ms}
export REPLICADB_WORKER_TWO_ADMISSION_GENERIC_COOLDOWN=${PHASE3_RESILIENCE_COOLDOWN:-100ms}
export REPLICADB_WORKER_ONE_ADMISSION_ADAPTIVE_BACKOFF_ENABLED=${PHASE3_RESILIENCE_BACKOFF_ENABLED:-true}
export REPLICADB_WORKER_TWO_ADMISSION_ADAPTIVE_BACKOFF_ENABLED=${PHASE3_RESILIENCE_BACKOFF_ENABLED:-true}

case "$scenario" in
    all|restart|notification|duplicate)
        ;;
    *)
        printf 'usage: %s [all|restart|notification|duplicate]\n' "$0" >&2
        exit 2
        ;;
esac

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
    printf 'phase3 resilience: %s\n' "$1"
}

cleanup() {
    if [[ "${PHASE3_KEEP_COMPOSE:-false}" != true ]]; then
        compose down --volumes --remove-orphans >/dev/null 2>&1 || true
        rm -rf "$state_directory"
    fi
}
trap cleanup EXIT

wait_for_liveness() {
    local endpoint=$1
    curl --retry 120 --retry-delay 1 --retry-all-errors -fsS "$endpoint" >/dev/null
}

wait_for_postgres() {
    local container_id
    local health_status
    container_id=$(compose ps -q postgres)
    compose up -d --wait postgres >/dev/null
    for attempt in $(seq 1 120); do
        health_status=$(docker inspect "$container_id" --format '{{.State.Health.Status}}' 2>/dev/null || true)
        if [[ "$health_status" = healthy ]]; then
            return
        fi
    done
    printf 'PostgreSQL did not become healthy\n' >&2
    exit 1
}

load_fixture() {
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        < "$repository_root/replicadb-server/src/test/resources/phase3/fixture.sql" >/dev/null
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL' >/dev/null
TRUNCATE TABLE phase3_source, phase3_sink;
INSERT INTO phase3_source (id, payload)
SELECT value, 'resilience-' || value
FROM generate_series(1, 20) AS values(value);
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

create_job() {
    local api_url=$1
    local name=$2
    local source_query=$3
    local payload
    payload=$(jq -n \
        --arg name "$name" \
        --arg source_query "$source_query" \
        --arg source_password '${env:DB_PASSWORD}' \
        --arg sink_password '${env:DB_PASSWORD}' \
        '{name: $name, sourceConnect: "jdbc:postgresql://postgres:5432/replicadb", sourceUser: "postgres", sourcePassword: $source_password, sourceTable: "phase3_source", sourceColumns: "id,payload", sourceQuery: $source_query, sinkConnect: "jdbc:postgresql://postgres:5432/replicadb", sinkUser: "postgres", sinkPassword: $sink_password, sinkTable: "phase3_sink", mode: "complete-atomic", jobs: 1, maxAttempts: 2, retryBackoffSeconds: 0, automaticRetryEnabled: true}')
    curl -fsS -b "$cookie_file" \
        -H 'Content-Type: application/json' \
        -H "X-XSRF-TOKEN: $csrf_token" \
        --data "$payload" \
        "$api_url/api/v1/jobs" | jq -r '.id'
}

trigger_job() {
    local api_url=$1
    local job_id=$2
    local key=$3
    curl -fsS -b "$cookie_file" \
        -H 'Content-Type: application/json' \
        -H "X-XSRF-TOKEN: $csrf_token" \
        -H "Idempotency-Key: $key" \
        -X POST "$api_url/api/v1/jobs/$job_id/runs" | jq -r '.id'
}

read_run() {
    local api_url=$1
    local run_id=$2
    curl --retry 20 --retry-delay 1 --retry-all-errors -fsS -b "$cookie_file" \
        "$api_url/api/v1/runs/$run_id"
}

wait_for_terminal() {
    local api_url=$1
    local run_id=$2
    local run
    local status
    for attempt in $(seq 1 180); do
        run=$(read_run "$api_url" "$run_id")
        status=$(printf '%s' "$run" | jq -r '.status')
        if [[ "$status" = SUCCEEDED ]]; then
            return
        fi
        if [[ "$status" = FAILED || "$status" = CANCELLED ]]; then
            executor=$(printf '%s' "$run" | jq -r '.executorIdentity // "none"')
            printf 'run reached unexpected terminal status: %s executor=%s\n' "$status" "$executor" >&2
            exit 1
        fi
    done
    run=$(read_run "$api_url" "$run_id")
    printf 'run did not reach SUCCEEDED: %s status=%s executor=%s\n' "$run_id" \
        "$(printf '%s' "$run" | jq -r '.status')" \
        "$(printf '%s' "$run" | jq -r '.executorIdentity // "none"')" >&2
    exit 1
}

wait_for_running() {
    local api_url=$1
    local run_id=$2
    local run
    for attempt in $(seq 1 120); do
        run=$(read_run "$api_url" "$run_id")
        if [[ "$(printf '%s' "$run" | jq -r '.status')" = RUNNING ]]; then
            return
        fi
        case "$(printf '%s' "$run" | jq -r '.status')" in
            SUCCEEDED|FAILED|CANCELLED)
                printf 'run completed before resilience action\n' >&2
                exit 1
                ;;
        esac
    done
    printf 'run did not reach RUNNING\n' >&2
    exit 1
}

sink_count() {
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
        'SELECT count(*) FROM phase3_sink' | tr -d '[:space:]'
}

job_run_count() {
    local job_id=$1
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
        "SELECT count(*) FROM job_run WHERE job_definition_id = '$job_id'" | tr -d '[:space:]'
}

run_postgres_restart() {
    local api_url=$1
    local job_id
    local run_id
    step 'dispatch durable run while workers are stopped'
    compose stop worker-one worker-two >/dev/null
    job_id=$(create_job "$api_url" 'phase3 postgres restart' 'SELECT id, payload FROM phase3_source')
    run_id=$(trigger_job "$api_url" "$job_id" 'phase3-postgres-restart')
    step 'restart PostgreSQL'
    compose stop postgres >/dev/null
    compose start postgres >/dev/null
    wait_for_postgres
    step 'restart workers and recover after database restart'
    compose up -d --wait --no-deps worker-one worker-two >/dev/null
    wait_for_terminal "$api_url" "$run_id"
    test "$(sink_count)" = 20
}

run_notification_loss() {
    local api_url=$1
    local job_id
    local run_id
    step 'dispatch while notifications have no listeners'
    compose stop worker-one worker-two >/dev/null
    job_id=$(create_job "$api_url" 'phase3 notification loss' 'SELECT id, payload FROM phase3_source')
    run_id=$(trigger_job "$api_url" "$job_id" 'phase3-notification-loss')
    compose up -d --wait --no-deps worker-one worker-two >/dev/null
    wait_for_terminal "$api_url" "$run_id"
    test "$(sink_count)" = 20
}

run_duplicate_polling() {
    local api_url=$1
    local job_id
    local run_id
    step 'restart one listener and dispatch through the shared queue'
    compose up -d --wait --no-deps --force-recreate worker-one >/dev/null
    job_id=$(create_job "$api_url" 'phase3 duplicate polling' \
        'SELECT id, payload FROM phase3_source CROSS JOIN LATERAL pg_sleep(0.2)')
    run_id=$(trigger_job "$api_url" "$job_id" 'phase3-duplicate-polling')
    step 'publish duplicate run notifications'
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
        "SELECT pg_notify('replicadb_runs', '$run_id'); SELECT pg_notify('replicadb_runs', '$run_id');" \
        >/dev/null
    wait_for_terminal "$api_url" "$run_id"
    test "$(job_run_count "$job_id")" = 1
    test "$(sink_count)" = 20
}

step 'start two APIs and two workers'
compose up -d --build --wait >/dev/null
api_one_port=$(compose port api-one 8080 | awk -F: '{print $NF}')
api_two_port=$(compose port api-two 8080 | awk -F: '{print $NF}')
api_one="http://127.0.0.1:$api_one_port"
api_two="http://127.0.0.1:$api_two_port"
step 'wait for API liveness'
wait_for_liveness "$api_one/actuator/health/liveness"
wait_for_liveness "$api_two/actuator/health/liveness"
step 'load fixture and authenticate'
load_fixture
authenticate "$api_one"

    if [[ "$scenario" = all || "$scenario" = restart ]]; then
        run_postgres_restart "$api_two"
    fi
    if [[ "$scenario" = all || "$scenario" = notification ]]; then
        load_fixture
        run_notification_loss "$api_two"
    fi
    if [[ "$scenario" = all || "$scenario" = duplicate ]]; then
        load_fixture
        run_duplicate_polling "$api_two"
    fi

printf 'phase3 resilience validation passed\n'
