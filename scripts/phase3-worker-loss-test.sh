#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
scenario=${1:-all}
project_name=${COMPOSE_PROJECT_NAME:-replicadb-phase3-worker-loss-$PPID}
state_directory="$repository_root/.phase3-compose/$project_name"
cookie_file="$state_directory/cookies.txt"
lease_duration=${REPLICADB_WORKER_LEASE_DURATION:-5s}
poll_interval=${REPLICADB_WORKER_POLL_INTERVAL:-1s}
heartbeat_interval=${REPLICADB_WORKER_HEARTBEAT_INTERVAL:-1s}

case "$scenario" in
    all|copy|merge)
        ;;
    *)
        printf 'usage: %s [all|copy|merge]\n' "$0" >&2
        exit 2
        ;;
esac

export REPLICADB_WORKER_LEASE_DURATION="$lease_duration"
export REPLICADB_WORKER_POLL_INTERVAL="$poll_interval"
export REPLICADB_WORKER_HEARTBEAT_INTERVAL="$heartbeat_interval"
export REPLICADB_WORKER_ONE_MAX_CONCURRENT_RUNS=${PHASE3_WORKER_LOSS_WORKER_ONE_CAPACITY:-1}
export REPLICADB_WORKER_TWO_MAX_CONCURRENT_RUNS=${PHASE3_WORKER_LOSS_WORKER_TWO_CAPACITY:-1}
export REPLICADB_WORKER_ONE_ADMISSION_JITTER_MAX=${PHASE3_WORKER_LOSS_JITTER_MAX:-25ms}
export REPLICADB_WORKER_TWO_ADMISSION_JITTER_MAX=${PHASE3_WORKER_LOSS_JITTER_MAX:-25ms}
export REPLICADB_WORKER_ONE_ADMISSION_GENERIC_COOLDOWN=${PHASE3_WORKER_LOSS_COOLDOWN:-100ms}
export REPLICADB_WORKER_TWO_ADMISSION_GENERIC_COOLDOWN=${PHASE3_WORKER_LOSS_COOLDOWN:-100ms}
export REPLICADB_WORKER_ONE_ADMISSION_ADAPTIVE_BACKOFF_ENABLED=${PHASE3_WORKER_LOSS_BACKOFF_ENABLED:-true}
export REPLICADB_WORKER_TWO_ADMISSION_ADAPTIVE_BACKOFF_ENABLED=${PHASE3_WORKER_LOSS_BACKOFF_ENABLED:-true}
unset COMPOSE_PROFILES

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
    printf 'phase3 worker-loss: %s\n' "$1"
}

cleanup() {
    if [[ -n "${lock_holder_pid:-}" ]]; then
        kill "$lock_holder_pid" 2>/dev/null || true
        wait "$lock_holder_pid" 2>/dev/null || true
    fi
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
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL' >/dev/null
TRUNCATE TABLE phase3_source, phase3_sink;
INSERT INTO phase3_source (id, payload)
SELECT value, 'worker-loss-' || value
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
    local mode=$2
    local source_query=$3
    local sink_disable_truncate=$4
    local payload
    payload=$(jq -n \
        --arg source_query "$source_query" \
        --arg source_password '${env:DB_PASSWORD}' \
        --arg sink_password '${env:DB_PASSWORD}' \
        --arg mode "$mode" \
        --arg sink_disable_truncate "$sink_disable_truncate" \
        '{name: ("phase3 worker loss " + $mode), sourceConnect: "jdbc:postgresql://postgres:5432/replicadb", sourceUser: "postgres", sourcePassword: $source_password, sourceTable: "phase3_source", sourceColumns: "id,payload", sourceQuery: $source_query, sinkConnect: "jdbc:postgresql://postgres:5432/replicadb", sinkUser: "postgres", sinkPassword: $sink_password, sinkTable: "phase3_sink", mode: $mode, jobs: 1, sinkDisableTruncate: ($sink_disable_truncate == "true"), maxAttempts: 2, retryBackoffSeconds: 0, automaticRetryEnabled: true}')
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
    curl -fsS -b "$cookie_file" "$api_url/api/v1/runs/$run_id"
}

wait_for_running_owner() {
    local api_url=$1
    local run_id=$2
    local run
    for attempt in $(seq 1 100); do
        run=$(read_run "$api_url" "$run_id")
        if [[ "$(printf '%s' "$run" | jq -r '.status')" = RUNNING \
                && "$(printf '%s' "$run" | jq -r '.executorIdentity')" = worker-one ]]; then
            return
        fi
        case "$(printf '%s' "$run" | jq -r '.status')" in
            SUCCEEDED|FAILED|CANCELLED)
                printf 'run completed before worker loss: %s\n' "$run_id" >&2
                exit 1
                ;;
        esac
    done
    printf 'run did not reach RUNNING on worker-one\n' >&2
    exit 1
}

wait_for_lock() {
    for attempt in $(seq 1 100); do
        granted=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
            "SELECT count(*) FROM pg_locks locks JOIN pg_class classes ON classes.oid = locks.relation WHERE classes.relname = 'phase3_sink' AND locks.mode = 'AccessExclusiveLock' AND locks.granted" \
            | tr -d '[:space:]')
        if [[ "$granted" = 1 ]]; then
            return
        fi
    done
    printf 'sink lock was not acquired\n' >&2
    exit 1
}

wait_for_merge_wait() {
    for attempt in $(seq 1 100); do
        waiting=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%phase3_sink%'" \
            | tr -d '[:space:]')
        if [[ "$waiting" -ge 1 ]]; then
            return
        fi
    done
    printf 'worker did not reach a sink-lock wait\n' >&2
    exit 1
}

wait_for_replacement() {
    local api_url=$1
    local job_id=$2
    local original_run_id=$3
    local page
    local original_status
    local replacement_status
    for attempt in $(seq 1 160); do
        page=$(curl -fsS -b "$cookie_file" "$api_url/api/v1/jobs/$job_id/runs?page=0&size=100")
        original_status=$(printf '%s' "$page" | jq -r --arg id "$original_run_id" \
            '.content[] | select(.id == $id) | .status' | head -1)
        replacement_status=$(printf '%s' "$page" | jq -r --arg id "$original_run_id" \
            '.content[] | select(.previousRunId == $id) | .status' | head -1)
        if [[ "$original_status" = RETRY_SCHEDULED \
                && "$replacement_status" = SUCCEEDED ]]; then
            replacement=$(printf '%s' "$page" | jq -c --arg id "$original_run_id" \
                '.content[] | select(.previousRunId == $id) | select(.status == "SUCCEEDED")' | head -1)
            printf '%s\n' "$replacement"
            return
        fi
    done
    printf 'replacement was not observed for run %s\n' "$original_run_id" >&2
    exit 1
}

run_copy_loss() {
    local api_url=$1
    local job_id
    local run_id
    local replacement
    step 'copy-loss job'
    job_id=$(create_job "$api_url" complete-atomic \
        'SELECT id, payload FROM phase3_source CROSS JOIN LATERAL pg_sleep(1)' false)
    run_id=$(trigger_job "$api_url" "$job_id" "phase3-copy-loss-$PPID")
    wait_for_running_owner "$api_url" "$run_id"
    step 'kill worker during source copy'
    compose kill -s SIGKILL worker-one >/dev/null
    compose up -d --wait --no-deps worker-one >/dev/null
    replacement=$(wait_for_replacement "$api_url" "$job_id" "$run_id")
    test "$(printf '%s' "$replacement" | jq -r '.attempt')" = 2
    test "$(printf '%s' "$replacement" | jq -r '.previousRunId')" = "$run_id"
    test "$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc 'SELECT count(*) FROM phase3_sink' | tr -d '[:space:]')" = 20
    worker_metrics=$(compose exec -T worker-one curl -fsS http://127.0.0.1:9091/actuator/prometheus)
    grep -F 'replicadb_worker_admission_events_total' <<< "$worker_metrics" >/dev/null
    if grep -Eq 'job_id=|run_id=|lease_token=|password=|jdbc:' <<< "$worker_metrics"; then
        printf 'high-cardinality or secret-bearing worker metric found\n' >&2
        exit 1
    fi
}

run_merge_loss() {
    local api_url=$1
    local job_id
    local run_id
    local replacement
    step 'merge-loss job'
    job_id=$(create_job "$api_url" incremental \
        'SELECT id, payload FROM phase3_source CROSS JOIN LATERAL pg_sleep(1)' true)
    run_id=$(trigger_job "$api_url" "$job_id" "phase3-merge-loss-$PPID")
    wait_for_running_owner "$api_url" "$run_id"
    step 'hold sink lock'
    (printf "BEGIN; LOCK TABLE phase3_sink IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(60);\n" \
        | compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1) &
    lock_holder_pid=$!
    wait_for_lock
    wait_for_merge_wait
    step 'kill worker during merge'
    compose kill -s SIGKILL worker-one >/dev/null
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE '%pg_sleep(60)%' AND pid <> pg_backend_pid()" \
        >/dev/null 2>&1 || true
    kill "$lock_holder_pid" 2>/dev/null || true
    wait "$lock_holder_pid" 2>/dev/null || true
    unset lock_holder_pid
    compose up -d --wait --no-deps worker-one >/dev/null
    replacement=$(wait_for_replacement "$api_url" "$job_id" "$run_id")
    test "$(printf '%s' "$replacement" | jq -r '.attempt')" = 2
    test "$(printf '%s' "$replacement" | jq -r '.previousRunId')" = "$run_id"
    test "$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc 'SELECT count(*) FROM phase3_sink' | tr -d '[:space:]')" = 20
    worker_metrics=$(compose exec -T worker-one curl -fsS http://127.0.0.1:9091/actuator/prometheus)
    grep -F 'replicadb_worker_admission_events_total' <<< "$worker_metrics" >/dev/null
    if grep -Eq 'job_id=|run_id=|lease_token=|password=|jdbc:' <<< "$worker_metrics"; then
        printf 'high-cardinality or secret-bearing worker metric found\n' >&2
        exit 1
    fi
}

step 'start services'
compose up -d --build --wait >/dev/null
api_one_port=$(compose port api-one 8080 | awk -F: '{print $NF}')
api_two_port=$(compose port api-two 8080 | awk -F: '{print $NF}')
api_one="http://127.0.0.1:$api_one_port"
api_two="http://127.0.0.1:$api_two_port"
step 'wait for liveness'
wait_for_liveness "$api_one/actuator/health/liveness"
wait_for_liveness "$api_two/actuator/health/liveness"
step 'load fixture'
load_fixture
step 'authenticate'
authenticate "$api_one"

case "$scenario" in
    copy)
        run_copy_loss "$api_two"
        ;;
    merge)
        run_merge_loss "$api_two"
        ;;
    all)
        run_copy_loss "$api_two"
        load_fixture
        run_merge_loss "$api_two"
        ;;
esac

printf 'phase3 worker-loss validation passed: %s\n' "$scenario"
