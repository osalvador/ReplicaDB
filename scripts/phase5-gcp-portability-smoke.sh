#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
project_name=${COMPOSE_PROJECT_NAME:-replicadb-gcp-smoke-$PPID}
state_directory="$repository_root/.phase5-gcp-smoke/$project_name"
compose_file="$repository_root/docker-compose.server.yml"
override_file="$script_dir/fixtures/gcp-smoke.override.yml"
server_version=${REPLICADB_SERVER_VERSION:-}

if [[ ! "$server_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    server_version=$(awk '
        /<artifactId>replicadb-server<\/artifactId>/ { found = 1; next }
        found && match($0, /<version>[^<]+<\/version>/) {
            value = substr($0, RSTART, RLENGTH)
            sub(/^<version>/, "", value)
            sub(/<\/version>$/, "", value)
            print value
            exit
        }
    ' "$repository_root/replicadb-server/pom.xml")
fi
if [[ ! "$server_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'Error: unable to determine a valid server version: %s\n' "$server_version" >&2
    exit 1
fi
export REPLICADB_SERVER_VERSION="$server_version"

mkdir -p "$state_directory"

for required_command in docker curl openssl od tr awk; do
    command -v "$required_command" >/dev/null 2>&1 || {
        printf 'Required command not found: %s\n' "$required_command" >&2
        exit 2
    }
done

export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')}
export POSTGRES_DB=${POSTGRES_DB:-replicadb}
export POSTGRES_USER=${POSTGRES_USER:-postgres}
export REPLICADB_BOOTSTRAP_ADMIN_USERNAME=${REPLICADB_BOOTSTRAP_ADMIN_USERNAME:-gcp-smoke-admin}
export REPLICADB_BOOTSTRAP_ADMIN_PASSWORD=${REPLICADB_BOOTSTRAP_ADMIN_PASSWORD:-$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')}

unset REPLICADB_SECURITY_KEYRING_FILE
REPLICADB_SECURITY_MASTER_KEY_FILE="$state_directory/replicadb-master-key.json"
key_material=$(openssl rand -base64 32)
export REPLICADB_SECURITY_KEYRING_CURRENT_VERSION=smoke
export REPLICADB_SECURITY_KEYRING_CURRENT_KEY="$key_material"
printf '{"currentVersion":"smoke","keys":{"smoke":"%s"}}\n' "$key_material" \
    >"$REPLICADB_SECURITY_MASTER_KEY_FILE"
chmod 600 "$REPLICADB_SECURITY_MASTER_KEY_FILE"
export REPLICADB_SECURITY_MASTER_KEY_FILE

compose() {
    docker compose -p "$project_name" -f "$compose_file" -f "$override_file" "$@"
}

cleanup() {
    local exit_code=$?
    if [[ "$exit_code" -ne 0 ]]; then
        compose ps >&2 2>/dev/null || true
        compose logs --no-color 2>/dev/null \
            | sed -E 's/(password|token|jdbc:[^[:space:]]+)/[redacted]/Ig' \
            | tail -200 >&2 || true
    fi
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$state_directory"
    return "$exit_code"
}
trap cleanup EXIT

if ! compose up -d --build >/dev/null; then
    compose logs --no-color 2>/dev/null \
        | sed -E 's/(password|token|jdbc:[^[:space:]]+)/[redacted]/Ig' \
        | tail -200 >&2 || true
    exit 1
fi

curl --retry 60 --retry-delay 1 --retry-all-errors --retry-max-time 60 -fsS \
    http://127.0.0.1:9500/actuator/health/liveness >/dev/null

if curl --connect-timeout 2 --max-time 3 -fsS \
    http://127.0.0.1:8080/actuator/health/liveness >/dev/null 2>&1; then
    printf 'API unexpectedly accepted traffic on port 8080\n' >&2
    exit 1
fi

compose exec -T worker-one curl --retry 60 --retry-delay 1 --retry-all-errors --retry-max-time 60 -fsS \
    http://127.0.0.1:9091/actuator/health/liveness >/dev/null

if compose exec -T worker-one curl --connect-timeout 2 --max-time 3 -fsS \
    http://127.0.0.1:8080/actuator/health >/dev/null 2>&1; then
    printf 'Worker unexpectedly accepted HTTP traffic on port 8080\n' >&2
    exit 1
fi

printf 'GCP Cloud Run portability smoke passed\n'