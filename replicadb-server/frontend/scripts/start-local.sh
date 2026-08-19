#!/usr/bin/env bash

set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SERVER_DIR="$ROOT_DIR/replicadb-server"
FRONTEND_DIR="$SERVER_DIR/frontend"
POSTGRES_CONTAINER="replicadb-dev-postgres"
POSTGRES_PORT="${REPLICADB_POSTGRES_PORT:-5432}"
API_PORT="${REPLICADB_API_PORT:-8080}"
FRONTEND_PORT="${REPLICADB_FRONTEND_PORT:-5173}"
CONTAINER_ENGINE="${CONTAINER_ENGINE:-docker}"
ADMIN_USERNAME="${REPLICADB_BOOTSTRAP_ADMIN_USERNAME:-admin}"

if [[ -z "${REPLICADB_BOOTSTRAP_ADMIN_PASSWORD:-}" ]]; then
    printf '%s\n' 'REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set.' >&2
    exit 1
fi
export REPLICADB_BOOTSTRAP_ADMIN_PASSWORD

for command_name in mvn npm node curl lsof "$CONTAINER_ENGINE"; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        printf 'Required command not found: %s\n' "$command_name" >&2
        exit 1
    fi
done

if [[ "$(uname -s)" == "Darwin" && -z "${JAVA_HOME:-}" ]]; then
    JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
    export JAVA_HOME
fi

if [[ -z "${JAVA_HOME:-}" || ! -x "$JAVA_HOME/bin/java" ]]; then
    printf '%s\n' 'Java 17 is required. Set JAVA_HOME to a Java 17 installation.' >&2
    exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"

find_available_port() {
    local requested_port="$1"
    local service_name="$2"
    local selected_port="$requested_port"

    while lsof -nP -iTCP:"$selected_port" -sTCP:LISTEN >/dev/null 2>&1; do
        selected_port=$((selected_port + 1))
    done

    if [[ "$selected_port" != "$requested_port" ]]; then
        printf '%s port %s is occupied; using port %s instead.\n' \
            "$service_name" "$requested_port" "$selected_port" >&2
    fi
    printf '%s\n' "$selected_port"
}

POSTGRES_PORT="$(find_available_port "$POSTGRES_PORT" 'PostgreSQL')"
API_PORT="$(find_available_port "$API_PORT" 'API')"
FRONTEND_PORT="$(find_available_port "$FRONTEND_PORT" 'Frontend')"

RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/replicadb-frontend-local.XXXXXX")"
SERVER_LOG="$RUN_DIR/server.log"
FRONTEND_LOG="$RUN_DIR/frontend.log"
SERVER_PID=""
FRONTEND_PID=""

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    set +e

    if [[ -n "$FRONTEND_PID" ]]; then
        kill "$FRONTEND_PID" >/dev/null 2>&1
        wait "$FRONTEND_PID" >/dev/null 2>&1
    fi

    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" >/dev/null 2>&1
        wait "$SERVER_PID" >/dev/null 2>&1
    fi

    "$CONTAINER_ENGINE" rm -f -v "$POSTGRES_CONTAINER" >/dev/null 2>&1
    rm -rf "$RUN_DIR"
    exit "$exit_code"
}

trap cleanup EXIT
trap 'exit 130' INT TERM

printf 'Cleaning PostgreSQL container %s...\n' "$POSTGRES_CONTAINER"
"$CONTAINER_ENGINE" rm -f -v "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true

printf 'Starting PostgreSQL on port %s...\n' "$POSTGRES_PORT"
"$CONTAINER_ENGINE" run -d --name "$POSTGRES_CONTAINER" \
    -e POSTGRES_DB=replicadb \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p "$POSTGRES_PORT:5432" \
    postgres:16-alpine >/dev/null

for attempt in $(seq 1 60); do
    if "$CONTAINER_ENGINE" exec "$POSTGRES_CONTAINER" pg_isready -U postgres -d replicadb >/dev/null 2>&1; then
        break
    fi
    if [[ "$attempt" == 60 ]]; then
        printf '%s\n' 'PostgreSQL did not become ready.' >&2
        exit 1
    fi
    sleep 1
done

printf '%s\n' 'Installing the CLI artifact in the local Maven repository...'
mvn -B -DskipTests install --file "$ROOT_DIR/pom.xml"

printf 'Starting Spring Boot API on http://localhost:%s...\n' "$API_PORT"
(
    export DB_URL="jdbc:postgresql://localhost:$POSTGRES_PORT/replicadb"
    export DB_USERNAME=postgres
    export DB_PASSWORD=
    export REPLICADB_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME"
    mvn -B -f "$SERVER_DIR/pom.xml" spring-boot:run \
        -Dspring-boot.run.profiles=api \
        -Dspring-boot.run.arguments="--server.port=$API_PORT" \
        -Dskip.installnodenpm=true \
        -Dskip.npm=true
) >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for attempt in $(seq 1 120); do
    if curl -fsS "http://localhost:$API_PORT/actuator/health" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
        printf '%s\n' 'Spring Boot API stopped before becoming ready:' >&2
        cat "$SERVER_LOG" >&2
        exit 1
    fi
    if [[ "$attempt" == 120 ]]; then
        printf '%s\n' 'Spring Boot API did not become ready:' >&2
        cat "$SERVER_LOG" >&2
        exit 1
    fi
    sleep 1
done

printf '%s\n' 'Seeding local job fixtures...'
REPLICADB_API_URL="http://localhost:$API_PORT" \
REPLICADB_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USERNAME" \
REPLICADB_POSTGRES_PORT="$POSTGRES_PORT" \
node "$FRONTEND_DIR/scripts/seed-local-jobs.mjs"

printf '%s\n' 'Installing frontend dependencies...'
(cd "$FRONTEND_DIR" && npm ci)

printf 'Starting Vite on http://localhost:%s...\n' "$FRONTEND_PORT"
(
    cd "$FRONTEND_DIR"
    export REPLICADB_API_PROXY_TARGET="http://localhost:$API_PORT"
    npm run dev -- --host 127.0.0.1 --port "$FRONTEND_PORT"
) >"$FRONTEND_LOG" 2>&1 &
FRONTEND_PID=$!

for attempt in $(seq 1 30); do
    if curl -fsS "http://localhost:$FRONTEND_PORT" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$FRONTEND_PID" >/dev/null 2>&1; then
        printf '%s\n' 'Vite stopped before becoming ready:' >&2
        cat "$FRONTEND_LOG" >&2
        exit 1
    fi
    if [[ "$attempt" == 30 ]]; then
        printf '%s\n' 'Vite did not become ready:' >&2
        cat "$FRONTEND_LOG" >&2
        exit 1
    fi
    sleep 1
done

printf '\nReplicaDB frontend is ready for local development:\n'
printf '  Frontend: http://localhost:%s\n' "$FRONTEND_PORT"
printf '  API:      http://localhost:%s\n' "$API_PORT"
printf '  Username: %s\n' "$ADMIN_USERNAME"
printf '%s\n' 'Press Ctrl+C to stop API, Vite, and the clean PostgreSQL container.'

while kill -0 "$SERVER_PID" >/dev/null 2>&1 && kill -0 "$FRONTEND_PID" >/dev/null 2>&1; do
    sleep 1
done

if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    printf '%s\n' 'Spring Boot API stopped unexpectedly:' >&2
    cat "$SERVER_LOG" >&2
else
    printf '%s\n' 'Vite stopped unexpectedly:' >&2
    cat "$FRONTEND_LOG" >&2
fi
exit 1
