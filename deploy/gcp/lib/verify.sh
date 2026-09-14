#!/usr/bin/env bash

set -euo pipefail

VERIFY_PUBLIC_ACCESS="${REPLICADB_PUBLIC_ACCESS:-false}"
VERIFY_SMOKE="${REPLICADB_VERIFY_SMOKE:-false}"

verify_fail() {
    printf 'Verification error: %s\n' "$*" >&2
    return 1
}

verify_token() {
    local url=$1
    if [[ "$VERIFY_PUBLIC_ACCESS" == true ]]; then
        printf ''
    else
        gcloud auth print-identity-token --audiences="$url" 2>/dev/null || {
            verify_fail 'could not obtain an identity token for authenticated Cloud Run ingress'
            return 1
        }
    fi
}

verify_curl_health() {
    local url=$1
    local token=$2
    local body
    if [[ -n "$token" ]]; then
        body=$(curl -fsS -H "Authorization: Bearer ${token}" "$url") || {
            verify_fail "health endpoint is unavailable: $url"
            return 1
        }
    else
        body=$(curl -fsS "$url") || {
            verify_fail "health endpoint is unavailable: $url"
            return 1
        }
    fi
    printf '%s' "$body"
}

verify_api_health() {
    local token=$1
    local liveness readiness
    liveness=$(verify_curl_health "${API_SERVICE_URL%/}/actuator/health/liveness" "$token") || return 1
    printf '%s\n' "$liveness" | jq -e '.status == "UP"' >/dev/null || {
        verify_fail 'API liveness is not UP'
        return 1
    }
    readiness=$(verify_curl_health "${API_SERVICE_URL%/}/actuator/health/readiness" "$token") || return 1
    printf '%s\n' "$readiness" | jq -e '.status == "UP" and (.components.db.status == "UP") and ((.components.quartz.status // "UP") == "UP")' >/dev/null || {
        verify_fail 'API readiness is degraded; inspect database, Quartz, and control-plane components'
        return 1
    }
}

verify_worker_pool() {
    local status logs
    [[ -n "${WORKER_POOL_NAME:-}" ]] || { verify_fail 'distributed deployment has no Worker Pool name'; return 1; }
    status=$(gcloud beta run worker-pools describe "$WORKER_POOL_NAME" --project="$PROJECT_ID" --region="$REGION" \
        --format='value(status)' 2>/dev/null) || { verify_fail 'Worker Pool status could not be read'; return 1; }
    [[ "$status" == READY || "$status" == Ready || "$status" == ready ]] || {
        verify_fail 'Worker Pool is not ready'; return 1;
    }
    logs=$(gcloud logging read "resource.type=cloud_run_worker_pool AND resource.labels.worker_pool_name=${WORKER_POOL_NAME}" \
        --project="$PROJECT_ID" --limit=20 --format='value(textPayload)' 2>/dev/null) || {
        verify_fail 'Worker Pool logs could not be read'; return 1;
    }
    [[ -n "$logs" ]] || { verify_fail 'Worker Pool has no recent logs'; return 1; }
}

verify_bootstrap_smoke() {
    local username password payload_file response token=$1
    username=$(gcloud secrets versions access "${BOOTSTRAP_USERNAME_SECRET_VERSION}" \
        --secret="$BOOTSTRAP_USERNAME_SECRET_NAME" --project="$PROJECT_ID" 2>/dev/null) || {
        verify_fail 'bootstrap username secret could not be read for optional smoke verification'; return 1;
    }
    password=$(gcloud secrets versions access "${BOOTSTRAP_PASSWORD_SECRET_VERSION}" \
        --secret="$BOOTSTRAP_PASSWORD_SECRET_NAME" --project="$PROJECT_ID" 2>/dev/null) || {
        unset username
        verify_fail 'bootstrap password secret could not be read for optional smoke verification'; return 1;
    }
    payload_file=$(mktemp "${TMPDIR:-/tmp}/replicadb-bootstrap.XXXXXX.json")
    chmod 600 "$payload_file"
    jq -n --arg username "$username" --arg password "$password" \
        '{username: $username, password: $password}' >"$payload_file"
    unset username password
    response=$(curl -fsS -H "Authorization: Bearer ${token}" -H 'Content-Type: application/json' \
        --data-binary "@$payload_file" "${API_SERVICE_URL%/}/api/v1/auth/login") || {
        rm -f "$payload_file"
        verify_fail 'optional bootstrap login smoke failed'
        return 1
    }
    rm -f "$payload_file"
    printf '%s\n' "$response" | jq -e '.username // .id // .authenticated' >/dev/null || {
        verify_fail 'bootstrap login returned an unexpected response'; return 1;
    }
}

verify_deployment() {
    local token
    API_SERVICE_URL=${API_SERVICE_URL:-$(gcloud run services describe "${CLOUD_RUN_SERVICE_NAME:-$(naming_resource_name api "${REPLICADB_GCP_PREFIX:-replicadb}" "$DEPLOYMENT_ID")}" \
        --project="$PROJECT_ID" --region="$REGION" --format='value(status.url)')}
    [[ -n "$API_SERVICE_URL" ]] || { verify_fail 'API service URL is unavailable'; return 1; }
    token=$(verify_token "$API_SERVICE_URL") || return 1
    verify_api_health "$token" || return 1
    if [[ "$MODE" == distributed ]]; then
        verify_worker_pool || return 1
    fi
    if [[ "$VERIFY_SMOKE" == true ]]; then
        verify_bootstrap_smoke "$token" || return 1
    fi
    unset token
    printf 'Verification passed: API=%s mode=%s\n' "$API_SERVICE_URL" "$MODE"
}