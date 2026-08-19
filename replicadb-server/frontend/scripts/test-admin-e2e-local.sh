#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FRONTEND_DIR="$ROOT_DIR/replicadb-server/frontend"
START_LOCAL="$FRONTEND_DIR/scripts/start-local.sh"

ADMIN_USERNAME="${REPLICADB_BOOTSTRAP_ADMIN_USERNAME:-e2e-admin}"
ADMIN_PASSWORD="${REPLICADB_BOOTSTRAP_ADMIN_PASSWORD:-}"
if [[ -z "$ADMIN_PASSWORD" ]]; then
    ADMIN_PASSWORD="$(node -e "process.stdout.write(require('crypto').randomBytes(24).toString('hex'))")"
fi

find_available_port() {
    local requested_port="$1"
    local selected_port="$requested_port"

    while lsof -nP -iTCP:"$selected_port" -sTCP:LISTEN >/dev/null 2>&1; do
        selected_port=$((selected_port + 1))
    done

    printf '%s\n' "$selected_port"
}

POSTGRES_PORT="$(find_available_port "${REPLICADB_POSTGRES_PORT:-15432}")"
API_PORT="$(find_available_port "${REPLICADB_API_PORT:-18080}")"
FRONTEND_PORT="$(find_available_port "${REPLICADB_FRONTEND_PORT:-15173}")"
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/replicadb-admin-e2e.XXXXXX")"
START_LOG="$RUN_DIR/start-local.log"
START_PID=""

sanitize_log() {
    sed -E \
        -e 's/(REPLICADB_BOOTSTRAP_ADMIN_PASSWORD=)[^[:space:]]+/\1[redacted]/g' \
        -e 's/(password[=:])[[:space:]]*[^,[:space:]]+/\1[redacted]/Ig' \
        "$START_LOG" | tail -80
}

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    set +e

    if [[ -n "$START_PID" ]]; then
        kill "$START_PID" >/dev/null 2>&1
        wait "$START_PID" >/dev/null 2>&1
    fi

    rm -rf "$RUN_DIR"
    exit "$exit_code"
}

trap cleanup EXIT
trap 'exit 130' INT TERM

REPLICADB_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME" \
REPLICADB_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
REPLICADB_POSTGRES_PORT="$POSTGRES_PORT" \
REPLICADB_API_PORT="$API_PORT" \
REPLICADB_FRONTEND_PORT="$FRONTEND_PORT" \
"$START_LOCAL" >"$START_LOG" 2>&1 &
START_PID=$!

for attempt in $(seq 1 180); do
    if curl -fsS "http://localhost:$FRONTEND_PORT" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$START_PID" >/dev/null 2>&1; then
        printf '%s\n' 'The local ReplicaDB stack stopped before the frontend became ready.' >&2
        sanitize_log >&2
        exit 1
    fi
    if [[ "$attempt" == 180 ]]; then
        printf '%s\n' 'The local ReplicaDB stack did not become ready.' >&2
        sanitize_log >&2
        exit 1
    fi
    sleep 1
done

REPLICADB_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME" \
REPLICADB_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
PLAYWRIGHT_BASE_URL="http://localhost:$FRONTEND_PORT" \
npm --prefix "$FRONTEND_DIR" run test:e2e -- e2e/admin-management.spec.ts
