#!/usr/bin/env bash

set -euo pipefail

project=${REPLICADB_PUBLIC_SMOKE_PROJECT:-}
region=${REPLICADB_PUBLIC_SMOKE_REGION:-europe-west4}
service=${REPLICADB_PUBLIC_SMOKE_SERVICE:-}
base_url=${REPLICADB_PUBLIC_SMOKE_URL:-}
playwright=false
connect_timeout=${REPLICADB_PUBLIC_SMOKE_CONNECT_TIMEOUT:-5}
max_time=${REPLICADB_PUBLIC_SMOKE_MAX_TIME:-20}
smoke_temp_dir=

usage() {
    cat <<'EOF'
Usage: phase5-gcp-frontend-smoke.sh [options]

Options:
  --project PROJECT       Google Cloud project used to resolve --service
  --region REGION         Cloud Run region used to resolve --service
  --service SERVICE       Cloud Run service used to resolve --url
  --url URL               Existing Cloud Run service URL
  --playwright            Run the opt-in browser/login smoke
  --help                  Show this help

The smoke is read-only. Credentialed browser execution additionally requires
REPLICADB_PUBLIC_SMOKE_USERNAME and REPLICADB_PUBLIC_SMOKE_PASSWORD_FILE.
EOF
}

fail() {
    printf 'Frontend smoke error: %s\n' "$*" >&2
    return 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command is not installed: $1"
}

resolve_url() {
    if [[ -n "$base_url" ]]; then
        return 0
    fi
    [[ -n "$project" && -n "$service" ]] || {
        fail 'provide --url, or provide both --project and --service'
        return 1
    }
    base_url=$(gcloud run services describe "$service" --project="$project" --region="$region" \
        --format='value(status.url)') || {
        fail 'could not resolve the Cloud Run service URL'
        return 1
    }
    [[ -n "$base_url" ]] || { fail 'Cloud Run service URL is empty'; return 1; }
}

smoke_request() {
    local path=$1
    local body_file=$2
    local headers_file=$3
    local status
    status=$(curl -sS --connect-timeout "$connect_timeout" --max-time "$max_time" \
        -o "$body_file" -D "$headers_file" -w '%{http_code}' "${base_url%/}${path}") || {
        fail "request failed: $path"
        return 1
    }
    printf '%s' "$status"
}

assert_spa_route() {
    local path=$1
    local body_file=$2
    local headers_file=$3
    local status
    status=$(smoke_request "$path" "$body_file" "$headers_file") || return 1
    [[ "$status" == 200 ]] || { fail "$path returned HTTP $status; expected 200"; return 1; }
    grep -Fq 'ReplicaDB Control Plane' "$body_file" || {
        fail "$path did not return the ReplicaDB SPA shell"
        return 1
    }
}

assert_csrf_bootstrap() {
    local body_file=$1
    local headers_file=$2
    local status
    status=$(smoke_request /api/v1/auth/csrf "$body_file" "$headers_file") || return 1
    [[ "$status" == 200 ]] || { fail "CSRF bootstrap returned HTTP $status; expected 200"; return 1; }
    grep -Eiq '^set-cookie:[[:space:]].*XSRF-TOKEN=' "$headers_file" || {
        fail 'CSRF bootstrap did not set the XSRF-TOKEN cookie'
        return 1
    }
}

assert_protected_api() {
    local body_file=$1
    local headers_file=$2
    local status content_type
    status=$(smoke_request /api/v1/jobs "$body_file" "$headers_file") || return 1
    [[ "$status" == 401 ]] || { fail "protected jobs API returned HTTP $status; expected 401"; return 1; }
    content_type=$(awk 'tolower($0) ~ /^content-type:/ { sub(/^[^:]*:[[:space:]]*/, ""); print; exit }' "$headers_file")
    [[ "$content_type" == *application/problem+json* ]] || {
        fail 'protected jobs API did not return application/problem+json'
        return 1
    }
}

run_playwright() {
    local frontend_dir=${REPLICADB_FRONTEND_DIR:-$(cd "$(dirname "$0")/../replicadb-server/frontend" && pwd)}
    [[ -n "${REPLICADB_PUBLIC_SMOKE_USERNAME:-}" && -n "${REPLICADB_PUBLIC_SMOKE_PASSWORD_FILE:-}" ]] || {
        fail '--playwright requires REPLICADB_PUBLIC_SMOKE_USERNAME and REPLICADB_PUBLIC_SMOKE_PASSWORD_FILE'
        return 1
    }
    [[ -f "$REPLICADB_PUBLIC_SMOKE_PASSWORD_FILE" ]] || {
        fail 'REPLICADB_PUBLIC_SMOKE_PASSWORD_FILE does not exist'
        return 1
    }
    REPLICADB_PUBLIC_SMOKE_URL="$base_url" \
    REPLICADB_PUBLIC_SMOKE_USERNAME="$REPLICADB_PUBLIC_SMOKE_USERNAME" \
    REPLICADB_PUBLIC_SMOKE_PASSWORD_FILE="$REPLICADB_PUBLIC_SMOKE_PASSWORD_FILE" \
        npm --prefix "$frontend_dir" exec -- playwright test e2e/public-cloud-run-smoke.spec.ts
}

main() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --project) [[ $# -ge 2 ]] || { fail '--project requires a value'; return 1; }; project=$2; shift 2 ;;
            --region) [[ $# -ge 2 ]] || { fail '--region requires a value'; return 1; }; region=$2; shift 2 ;;
            --service) [[ $# -ge 2 ]] || { fail '--service requires a value'; return 1; }; service=$2; shift 2 ;;
            --url) [[ $# -ge 2 ]] || { fail '--url requires a value'; return 1; }; base_url=$2; shift 2 ;;
            --playwright) playwright=true; shift ;;
            --help|-h) usage; return 0 ;;
            *) fail "unknown option: $1"; return 1 ;;
        esac
    done

    require_command curl || return 1
    if [[ -z "$base_url" ]]; then
        require_command gcloud || return 1
    fi
    resolve_url || return 1

    local body_file headers_file
    smoke_temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-public-smoke.XXXXXX")
    body_file="$smoke_temp_dir/body"
    headers_file="$smoke_temp_dir/headers"
    trap 'rm -rf "$smoke_temp_dir"' EXIT

    assert_spa_route / "$body_file" "$headers_file" || return 1
    assert_spa_route /login "$body_file" "$headers_file" || return 1
    assert_csrf_bootstrap "$body_file" "$headers_file" || return 1
    assert_protected_api "$body_file" "$headers_file" || return 1
    if [[ "$playwright" == true ]]; then
        run_playwright || return 1
    fi
    printf 'GCP public frontend smoke passed: %s\n' "$base_url"
}

main "$@"
