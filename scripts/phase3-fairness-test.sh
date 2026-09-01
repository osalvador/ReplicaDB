#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
source "$script_dir/phase4-compose-common.sh"
project_name=${COMPOSE_PROJECT_NAME:-replicadb-phase3-fairness-$PPID}
state_directory="$repository_root/.phase3-compose/$project_name"
cookie_file="$state_directory/cookies.txt"
run_file="$state_directory/runs.txt"
jobs_to_run=${PHASE3_FAIRNESS_RUNS:-24}
source_sleep=${PHASE3_FAIRNESS_SOURCE_SLEEP:-0.05}
first_capacity=${PHASE3_FAIRNESS_WORKER_ONE_CAPACITY:-1}
second_capacity=${PHASE3_FAIRNESS_WORKER_TWO_CAPACITY:-2}
tolerance=${PHASE3_FAIRNESS_TOLERANCE:-0.50}

if [[ "$jobs_to_run" -lt 6 || "$jobs_to_run" -gt 64 ]]; then
    printf 'PHASE3_FAIRNESS_RUNS must be between 6 and 64\n' >&2
    exit 2
fi
if [[ "$first_capacity" -lt 1 || "$second_capacity" -lt 1 ]]; then
    printf 'worker capacities must be positive\n' >&2
    exit 2
fi
if [[ ! "$source_sleep" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    printf 'PHASE3_FAIRNESS_SOURCE_SLEEP must be a non-negative number of seconds\n' >&2
    exit 2
fi

export COMPOSE_PROFILES=multinode
export REPLICADB_WORKER_ONE_MAX_CONCURRENT_RUNS="$first_capacity"
export REPLICADB_WORKER_TWO_MAX_CONCURRENT_RUNS="$second_capacity"
export REPLICADB_WORKER_ONE_ADMISSION_JITTER_MAX=${REPLICADB_WORKER_ONE_ADMISSION_JITTER_MAX:-25ms}
export REPLICADB_WORKER_TWO_ADMISSION_JITTER_MAX=${REPLICADB_WORKER_TWO_ADMISSION_JITTER_MAX:-25ms}
export REPLICADB_WORKER_ONE_ADMISSION_GENERIC_COOLDOWN=${REPLICADB_WORKER_ONE_ADMISSION_GENERIC_COOLDOWN:-100ms}
export REPLICADB_WORKER_TWO_ADMISSION_GENERIC_COOLDOWN=${REPLICADB_WORKER_TWO_ADMISSION_GENERIC_COOLDOWN:-100ms}
export REPLICADB_WORKER_ONE_ADMISSION_ADAPTIVE_BACKOFF_ENABLED=${REPLICADB_WORKER_ONE_ADMISSION_ADAPTIVE_BACKOFF_ENABLED:-true}
export REPLICADB_WORKER_TWO_ADMISSION_ADAPTIVE_BACKOFF_ENABLED=${REPLICADB_WORKER_TWO_ADMISSION_ADAPTIVE_BACKOFF_ENABLED:-true}
export REPLICADB_WORKER_ONE_ADMISSION_INITIAL_DELAY=${REPLICADB_WORKER_ONE_ADMISSION_INITIAL_DELAY:-25ms}
export REPLICADB_WORKER_TWO_ADMISSION_INITIAL_DELAY=${REPLICADB_WORKER_TWO_ADMISSION_INITIAL_DELAY:-25ms}
export REPLICADB_WORKER_ONE_ADMISSION_MAX_DELAY=${REPLICADB_WORKER_ONE_ADMISSION_MAX_DELAY:-2s}
export REPLICADB_WORKER_TWO_ADMISSION_MAX_DELAY=${REPLICADB_WORKER_TWO_ADMISSION_MAX_DELAY:-2s}
export REPLICADB_WORKER_ONE_ADMISSION_DECAY_HALF_LIFE=${REPLICADB_WORKER_ONE_ADMISSION_DECAY_HALF_LIFE:-30s}
export REPLICADB_WORKER_TWO_ADMISSION_DECAY_HALF_LIFE=${REPLICADB_WORKER_TWO_ADMISSION_DECAY_HALF_LIFE:-30s}
export REPLICADB_WORKER_LEASE_DURATION=${REPLICADB_WORKER_LEASE_DURATION:-5m}
export REPLICADB_WORKER_HEARTBEAT_INTERVAL=${REPLICADB_WORKER_HEARTBEAT_INTERVAL:-30s}
export REPLICADB_WORKER_POLL_INTERVAL=${REPLICADB_WORKER_POLL_INTERVAL:-1s}

if [[ -z "${JAVA_HOME:-}" && "$(uname)" = Darwin && -x /usr/libexec/java_home ]]; then
    JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || true)
    export JAVA_HOME
fi

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
phase4_require_commands
phase4_prepare_keyring

compose() {
    docker compose -p "$project_name" -f "$repository_root/docker-compose.server.yml" "$@"
}

cleanup() {
    local exit_code=$?
    if [[ "$exit_code" -ne 0 ]]; then
        compose ps > "$state_directory/compose-status.txt" 2>/dev/null || true
        compose logs --no-color worker-one worker-two > "$state_directory/worker-diagnostics.log" 2>/dev/null || true
        sed -Ei 's/(password|token|jdbc:[^[:space:]]+)/[redacted]/Ig' \
            "$state_directory/worker-diagnostics.log" 2>/dev/null || true
    fi
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$state_directory"
    return "$exit_code"
}
trap cleanup EXIT

wait_for_liveness() {
    local endpoint=$1
    curl --retry 120 --retry-delay 1 --retry-all-errors -fsS "$endpoint" >/dev/null
}

metric_value() {
    local metrics=$1
    local metric_name=$2
    local label=${3:-}
    awk -v metric_name="$metric_name" -v label="$label" '
        $0 !~ /^#/ && $0 ~ "^" metric_name "(\\{| |$)" && (label == "" || index($0, label) > 0) {
            print $NF
            found = 1
            exit
        }
        END {
            if (!found) print 0
        }
    ' <<< "$metrics"
}

worker_metrics() {
    local worker_service=$1
    compose exec -T "$worker_service" curl -fsS http://127.0.0.1:9091/actuator/prometheus
}

assert_capacity() {
    local metrics=$1
    local capacity=$2
    local active
    active=$(metric_value "$metrics" replicadb_worker_active_slots)
    awk -v active="$active" -v capacity="$capacity" 'BEGIN { exit !(active >= 0 && active <= capacity) }'
}

authenticate() {
    local api_url=$1
    local csrf_response
    csrf_response=$(curl -fsS -c "$cookie_file" "$api_url/api/v1/auth/csrf")
    csrf_token=$(jq -r '.token' <<< "$csrf_response")
    test -n "$csrf_token"
    curl -fsS -c "$cookie_file" -b "$cookie_file" \
        -H 'Content-Type: application/json' \
        --data "{\"username\":\"$REPLICADB_BOOTSTRAP_ADMIN_USERNAME\",\"password\":\"$REPLICADB_BOOTSTRAP_ADMIN_PASSWORD\"}" \
        "$api_url/api/v1/auth/login" >/dev/null
}

load_fixture() {
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        < "$repository_root/replicadb-server/src/test/resources/phase3/fixture.sql" >/dev/null
    compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL >/dev/null
TRUNCATE TABLE phase3_source, phase3_sink;
INSERT INTO phase3_source (id, payload)
SELECT value, 'fairness-' || value
FROM generate_series(1, 3) AS values(value);
SQL
}

create_sinks() {
    local index
    for index in $(seq 1 "$jobs_to_run"); do
        compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
            "DROP TABLE IF EXISTS phase3_fair_sink_${index}; CREATE TABLE phase3_fair_sink_${index} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)" \
            >/dev/null
    done
}

create_job() {
    local api_url=$1
    local index=$2
    local source_query="SELECT id, payload FROM phase3_source CROSS JOIN LATERAL pg_sleep($source_sleep)"
    jq -n \
        --arg name "phase3 fairness $index" \
        --arg sink_table "phase3_fair_sink_$index" \
        --arg source_query "$source_query" \
        --arg source_datasource_id "$phase4_source_datasource_id" \
        --arg sink_datasource_id "$phase4_sink_datasource_id" \
        '{name: $name, sourceDatasourceId: $source_datasource_id, sourceDatasourceUseEnabled: true, sourceTable: "phase3_source", sourceColumns: "id,payload", sourceQuery: $source_query, sinkDatasourceId: $sink_datasource_id, sinkDatasourceUseEnabled: true, sinkTable: $sink_table, mode: "complete-atomic", jobs: 1, maxAttempts: 1, retryBackoffSeconds: 0, automaticRetryEnabled: false}' \
        | curl -fsS -b "$cookie_file" \
            -H 'Content-Type: application/json' \
            -H "X-XSRF-TOKEN: $csrf_token" \
            --data-binary @- "$api_url/api/v1/jobs" \
        | jq -r '.id'
}

trigger_job() {
    local api_url=$1
    local job_id=$2
    local index=$3
    curl -fsS -b "$cookie_file" \
        -H 'Content-Type: application/json' \
        -H "X-XSRF-TOKEN: $csrf_token" \
        -H "Idempotency-Key: phase3-fairness-$index" \
        -X POST "$api_url/api/v1/jobs/$job_id/runs" \
        | jq -r '.id'
}

read_status() {
    local api_url=$1
    local run_id=$2
    curl -fsS --retry 5 --retry-delay 1 --retry-all-errors -b "$cookie_file" \
        "$api_url/api/v1/runs/$run_id" | jq -r '.status'
}

step() {
    printf 'phase3 fairness: %s\n' "$1"
}

step 'package CLI and server artifacts'
mvn -B install -DskipTests -f "$repository_root/pom.xml" >/dev/null
mvn -B package -DskipTests -f "$repository_root/replicadb-server/pom.xml" >/dev/null

step 'start two APIs and two workers'
compose up -d --build --wait >/dev/null
api_one_port=$(compose port api-one 8080 | awk -F: '{print $NF}')
api_two_port=$(compose port api-two 8080 | awk -F: '{print $NF}')
api_one="http://127.0.0.1:$api_one_port"
api_two="http://127.0.0.1:$api_two_port"
wait_for_liveness "$api_one/actuator/health/liveness"
wait_for_liveness "$api_two/actuator/health/liveness"

step 'load fixture and capture baseline metrics'
load_fixture
create_sinks
authenticate "$api_one"
phase4_create_postgres_datasources "$api_one" "phase3 fairness"
baseline_one=$(worker_metrics worker-one)
baseline_two=$(worker_metrics worker-two)
assert_capacity "$baseline_one" "$first_capacity"
assert_capacity "$baseline_two" "$second_capacity"

step "create and dispatch $jobs_to_run durable runs"
: > "$run_file"
for index in $(seq 1 "$jobs_to_run"); do
    job_id=$(create_job "$api_one" "$index")
    run_id=$(trigger_job "$api_one" "$job_id" "$index")
    printf '%s|%s\n' "$run_id" "$job_id" >> "$run_file"
done

step 'wait for terminal runs while checking slot bounds'
for attempt in $(seq 1 600); do
    current_one=$(worker_metrics worker-one)
    current_two=$(worker_metrics worker-two)
    assert_capacity "$current_one" "$first_capacity"
    assert_capacity "$current_two" "$second_capacity"
    terminal_count=$(while IFS='|' read -r run_id job_id; do
        if [[ "$(read_status "$api_two" "$run_id")" = SUCCEEDED ]]; then
            printf '1\n'
        fi
    done < "$run_file" | wc -l | tr -d '[:space:]')
    if [[ "$terminal_count" -eq "$jobs_to_run" ]]; then
        break
    fi
    sleep 0.2
done
test "$terminal_count" = "$jobs_to_run"

final_one=$(worker_metrics worker-one)
final_two=$(worker_metrics worker-two)
assert_capacity "$final_one" "$first_capacity"
assert_capacity "$final_two" "$second_capacity"

actual_runs=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    "SELECT count(*) FROM job_run WHERE job_definition_id IN (SELECT id FROM job_definition WHERE name LIKE 'phase3 fairness %')" \
    | tr -d '[:space:]')
test "$actual_runs" = "$jobs_to_run"

one_completed=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    "SELECT count(*) FROM job_run WHERE status = 'SUCCEEDED' AND executor_identity = 'worker-one' AND job_definition_id IN (SELECT id FROM job_definition WHERE name LIKE 'phase3 fairness %')" \
    | tr -d '[:space:]')
two_completed=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    "SELECT count(*) FROM job_run WHERE status = 'SUCCEEDED' AND executor_identity = 'worker-two' AND job_definition_id IN (SELECT id FROM job_definition WHERE name LIKE 'phase3 fairness %')" \
    | tr -d '[:space:]')

one_busy_before=$(metric_value "$baseline_one" replicadb_worker_busy_slot_seconds 'worker_identity="worker-one"')
two_busy_before=$(metric_value "$baseline_two" replicadb_worker_busy_slot_seconds 'worker_identity="worker-two"')
one_busy_after=$(metric_value "$final_one" replicadb_worker_busy_slot_seconds 'worker_identity="worker-one"')
two_busy_after=$(metric_value "$final_two" replicadb_worker_busy_slot_seconds 'worker_identity="worker-two"')
one_normalized_before=$(metric_value "$baseline_one" replicadb_worker_normalized_busy_slot_seconds 'worker_identity="worker-one"')
two_normalized_before=$(metric_value "$baseline_two" replicadb_worker_normalized_busy_slot_seconds 'worker_identity="worker-two"')
one_normalized_after=$(metric_value "$final_one" replicadb_worker_normalized_busy_slot_seconds 'worker_identity="worker-one"')
two_normalized_after=$(metric_value "$final_two" replicadb_worker_normalized_busy_slot_seconds 'worker_identity="worker-two"')

printf 'phase3 fairness observations raw_runs=%s/%s raw_busy=%s/%s normalized_busy=%s/%s tolerance=%s\n' \
    "$one_completed" "$two_completed" "$one_busy_after" "$two_busy_after" \
    "$one_normalized_after" "$two_normalized_after" "$tolerance"

if ! awk -v one="$one_completed" -v two="$two_completed" -v first_capacity="$first_capacity" \
    -v second_capacity="$second_capacity" -v tolerance="$tolerance" '
    BEGIN {
        if (one < 1 || two < 1) exit 1
        ratio = two / one
        expected = second_capacity / first_capacity
        exit !(ratio >= expected * (1 - tolerance) && ratio <= expected * (1 + tolerance))
    }
'
then
    printf 'raw worker share outside tolerance: worker-one=%s worker-two=%s capacities=%s/%s tolerance=%s\n' \
        "$one_completed" "$two_completed" "$first_capacity" "$second_capacity" "$tolerance" >&2
    exit 1
fi
if ! awk -v one_before="$one_normalized_before" -v two_before="$two_normalized_before" \
    -v one_after="$one_normalized_after" -v two_after="$two_normalized_after" \
    -v tolerance="$tolerance" '
    BEGIN {
        one_delta = one_after - one_before
        two_delta = two_after - two_before
        if (one_delta <= 0 || two_delta <= 0) exit 1
        ratio = two_delta / one_delta
        exit !(ratio >= 1 - tolerance && ratio <= 1 + tolerance)
    }
'
then
    printf 'normalized worker utilization outside tolerance: worker-one=%s worker-two=%s tolerance=%s\n' \
        "$one_normalized_after" "$two_normalized_after" "$tolerance" >&2
    exit 1
fi

metrics_one="$final_one"
metrics_two="$final_two"
grep -F 'replicadb_worker_admission_events_total' <<< "$metrics_one" >/dev/null
grep -F 'replicadb_worker_admission_events_total' <<< "$metrics_two" >/dev/null
if grep -Eq 'job_id=|run_id=|lease_token=|password=|jdbc:' <<< "$metrics_one$metrics_two"; then
    printf 'high-cardinality or secret-bearing metric label found\n' >&2
    exit 1
fi

printf 'phase3 fairness validation passed runs=%s worker_runs=%s/%s normalized_busy=%s/%s raw_busy=%s/%s\n' \
    "$actual_runs" "$one_completed" "$two_completed" \
    "$one_normalized_after" "$two_normalized_after" "$one_busy_after" "$two_busy_after"
