#!/usr/bin/env bash

set -euo pipefail

PREFLIGHT_REQUIRED_APIS=(
    run.googleapis.com
    artifactregistry.googleapis.com
    sqladmin.googleapis.com
    secretmanager.googleapis.com
    compute.googleapis.com
    servicenetworking.googleapis.com
    serviceusage.googleapis.com
    iamcredentials.googleapis.com
)

preflight_fail() {
    printf 'Preflight error: %s\n' "$*" >&2
    return 1
}

preflight_has_line() {
    local expected=$1
    local values=$2
    printf '%s\n' "$values" | awk -v expected="$expected" '$0 == expected { found = 1 } END { exit !found }'
}

preflight_run_command() {
    local description=$1
    shift
    if ! "$@"; then
        preflight_fail "$description failed; check the active account and project permissions"
    fi
}

preflight_check_image() {
    if [[ "$IMAGE" == *@sha256:* || -n "$IMAGE_DIGEST" ]]; then
        return 0
    fi
    if command -v docker >/dev/null 2>&1; then
        if docker manifest inspect "$IMAGE" >/dev/null 2>&1; then
            return 0
        fi
        preflight_fail "image cannot be inspected: $IMAGE; provide --image-digest or mirror it to Artifact Registry"
        return 1
    fi
    preflight_fail "docker is required to inspect a mutable image tag; provide --image-digest or mirror it to Artifact Registry"
}

preflight_check_apis() {
    local enabled api
    enabled=$(gcloud services list --enabled --project="$PROJECT_ID" --format='value(config.name)') || {
        preflight_fail "cannot list enabled APIs in project $PROJECT_ID"
        return 1
    }
    for api in "${PREFLIGHT_REQUIRED_APIS[@]}"; do
        preflight_has_line "$api" "$enabled" || {
            preflight_fail "required API is not enabled: $api"
            return 1
        }
    done
}

preflight_check_permissions() {
    local permissions granted required missing access_token payload response
    permissions='run.services.create,run.services.update,run.services.get,iam.serviceAccounts.actAs,secretmanager.secrets.get'
    command -v curl >/dev/null 2>&1 || {
        preflight_fail 'curl is required to test deployment permissions'
        return 1
    }
    command -v jq >/dev/null 2>&1 || {
        preflight_fail 'jq is required to test deployment permissions'
        return 1
    }
    access_token=$(gcloud auth print-access-token 2>/dev/null) || {
        preflight_fail 'cannot obtain an access token to test deployment permissions'
        return 1
    }
    payload=$(jq -cn --arg permissions "$permissions" '{permissions: ($permissions | split(","))}') || {
        preflight_fail 'cannot build the deployment permission request'
        return 1
    }
    response=$(curl -fsS -X POST \
        -H "Authorization: Bearer ${access_token}" \
        -H 'Content-Type: application/json' \
        --data "$payload" \
        "https://cloudresourcemanager.googleapis.com/v1/projects/${PROJECT_ID}:testIamPermissions" 2>/dev/null) || {
        preflight_fail "cannot test deployment permissions in project $PROJECT_ID"
        return 1
    }
    unset access_token payload
    granted=$(printf '%s\n' "$response" | jq -r '.permissions[]?') || {
        preflight_fail "cannot parse deployment permissions in project $PROJECT_ID"
        return 1
    }
    IFS=',' read -r -a required_permissions <<<"$permissions"
    for required in "${required_permissions[@]}"; do
        if ! preflight_has_line "$required" "$granted"; then
            missing=${missing:+"$missing,"}$required
        fi
    done
    [[ -z "${missing:-}" ]] || preflight_fail "active account lacks required permissions: $missing"
}

preflight_check_worker_pool() {
    [[ "$MODE" == distributed ]] || return 0
    gcloud beta run worker-pools --help >/dev/null 2>&1 || {
        preflight_fail 'distributed mode requires gcloud beta run worker-pools; install/update gcloud without using sudo from this bundle'
        return 1
    }
}

preflight_check_network() {
    if [[ -n "$NETWORK" ]]; then
        gcloud compute networks describe "$NETWORK" --project="$PROJECT_ID" --format='value(name)' >/dev/null || {
            preflight_fail "VPC network is unavailable or inaccessible: $NETWORK"
            return 1
        }
    fi
    if [[ -n "$SUBNET" ]]; then
        [[ -n "$NETWORK" ]] || { preflight_fail '--subnet requires --network'; return 1; }
        gcloud compute networks subnets describe "$SUBNET" --region="$REGION" \
            --project="$PROJECT_ID" --format='value(name)' >/dev/null || {
            preflight_fail "VPC subnet is unavailable in $REGION: $SUBNET"
            return 1
        }
    fi
}

preflight_run() {
    local active_account target_project regions
    command -v gcloud >/dev/null 2>&1 || { preflight_fail 'required command is not installed: gcloud'; return 1; }
    active_account=$(gcloud auth list --filter='status:ACTIVE' --format='value(account)' 2>/dev/null) || {
        preflight_fail 'gcloud has no active authenticated account'
        return 1
    }
    [[ -n "$active_account" ]] || { preflight_fail 'gcloud has no active authenticated account'; return 1; }
    target_project=$(gcloud projects describe "$PROJECT_ID" --format='value(projectId)' 2>/dev/null) || {
        preflight_fail "project does not exist or is inaccessible: $PROJECT_ID"
        return 1
    }
    [[ "$target_project" == "$PROJECT_ID" ]] || {
        preflight_fail "gcloud returned a different project: $target_project"
        return 1
    }
    preflight_check_apis || return 1
    regions=$(gcloud run regions list --project="$PROJECT_ID" --format='value(locationId)' 2>/dev/null) || {
        preflight_fail 'cannot inspect Cloud Run regional availability'
        return 1
    }
    preflight_has_line "$REGION" "$regions" || { preflight_fail "Cloud Run region is unavailable: $REGION"; return 1; }
    preflight_check_permissions || return 1
    preflight_check_network || return 1
    preflight_check_worker_pool || return 1
    preflight_check_image || return 1
    printf 'Preflight passed for project %s in %s (%s mode)\n' "$PROJECT_ID" "$REGION" "$MODE"
}
