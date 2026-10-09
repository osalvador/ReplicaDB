#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${REPLICADB_STATE_FILE:-}"
PROJECT_ID="${REPLICADB_GCP_PROJECT:-}"
REGION="${REPLICADB_GCP_REGION:-}"
DEPLOYMENT_ID="${REPLICADB_DEPLOYMENT_ID:-}"
CONFIRMATION="${REPLICADB_CONFIRMATION:-}"
NON_INTERACTIVE=false

usage() {
    cat <<'EOF'
Usage: stop.sh [options]

Stops billable compute for one ReplicaDB deployment without deleting data:
  - Cloud Run API service: scale minimum instances to zero
  - Cloud Run Worker Pool: scale instances to zero
  - Owned Cloud SQL: set activation policy to NEVER

The deployment state file is required. Secret Manager secrets, shared Cloud SQL,
networks, subnets, service accounts, and IAM bindings are left untouched.

Options:
  --project PROJECT         Google Cloud project ID
  --region REGION           Cloud Run region
  --deployment-id ID        Deployment identifier for confirmation and validation
  --state-file PATH         Deployment state file (required unless configured)
  --confirmation TEXT       Must be: STOP REPLICADB
  --non-interactive         Require --confirmation instead of reading stdin
  --help                    Show this help
EOF
}

die() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is not installed: $1"
}

configure_cloud_sdk_python() {
    if [[ -z "${CLOUDSDK_PYTHON:-}" ]]; then
        local asdf_python="${HOME:-}/.asdf/installs/python/3.11.16/bin/python"
        if [[ -x "$asdf_python" ]]; then
            export CLOUDSDK_PYTHON="$asdf_python"
        fi
    fi
    if [[ -n "${CLOUDSDK_PYTHON:-}" ]]; then
        export CLOUDSDK_PYTHON_SITEPACKAGES="${CLOUDSDK_PYTHON_SITEPACKAGES:-1}"
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --project) [[ $# -ge 2 ]] || die '--project requires a value'; PROJECT_ID=$2; shift 2 ;;
            --region) [[ $# -ge 2 ]] || die '--region requires a value'; REGION=$2; shift 2 ;;
            --deployment-id) [[ $# -ge 2 ]] || die '--deployment-id requires a value'; DEPLOYMENT_ID=$2; shift 2 ;;
            --state-file) [[ $# -ge 2 ]] || die '--state-file requires a value'; STATE_FILE=$2; shift 2 ;;
            --confirmation) [[ $# -ge 2 ]] || die '--confirmation requires a value'; CONFIRMATION=$2; shift 2 ;;
            --non-interactive) NON_INTERACTIVE=true; shift ;;
            --help|-h) usage; exit 0 ;;
            *) die "unknown option: $1" ;;
        esac
    done
}

load_state() {
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/state.sh"
    [[ -n "$STATE_FILE" ]] || die 'state file is required; use --state-file'
    [[ -f "$STATE_FILE" ]] || die "state file is missing: $STATE_FILE"
    state_load "$STATE_FILE"

    local state_project state_region state_deployment
    state_project=$(state_get projectId)
    state_region=$(state_get region)
    state_deployment=$(state_get deploymentId)
    [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "$state_project" ]] || die 'project does not match the deployment state'
    [[ -z "$REGION" || "$REGION" == "$state_region" ]] || die 'region does not match the deployment state'
    [[ -z "$DEPLOYMENT_ID" || "$DEPLOYMENT_ID" == "$state_deployment" ]] || die 'deployment ID does not match the deployment state'
    PROJECT_ID=$state_project
    REGION=$state_region
    DEPLOYMENT_ID=$state_deployment
    [[ -n "$PROJECT_ID" && -n "$REGION" && -n "$DEPLOYMENT_ID" ]] || die 'state file lacks project, region, or deployment ID'
}

confirm_stop() {
    if [[ -z "$CONFIRMATION" && "$NON_INTERACTIVE" == false && -t 0 ]]; then
        printf 'Type STOP REPLICADB to continue: ' >&2
        IFS= read -r CONFIRMATION
    fi
    [[ "$CONFIRMATION" == 'STOP REPLICADB' ]] || die 'stopping requires the exact confirmation: STOP REPLICADB'
}

service_exists() {
    local name=$1
    if gcloud run services describe "$name" --project="$PROJECT_ID" --region="$REGION" >/dev/null 2>&1; then
        return 0
    fi
    printf 'Resource not found; leaving it untouched: %s\n' "$name" >&2
    return 1
}

worker_pool_exists() {
    local name=$1
    if gcloud beta run worker-pools describe "$name" --project="$PROJECT_ID" --region="$REGION" >/dev/null 2>&1; then
        return 0
    fi
    printf 'Resource not found; leaving it untouched: %s\n' "$name" >&2
    return 1
}

stop_api_service() {
    local name=$1
    gcloud run services update "$name" --project="$PROJECT_ID" --region="$REGION" \
        --min=0 --min-instances=0 --quiet >/dev/null || die "could not scale API service to zero: $name"
}

stop_deployment() {
    local api_service worker_pool cloud_sql_instance cloud_sql_owned mode
    api_service=$(state_get apiServiceName)
    worker_pool=$(state_get workerPoolName)
    cloud_sql_instance=$(state_get cloudSqlInstance)
    cloud_sql_owned=$(state_get cloudSqlOwned)
    mode=$(state_get mode)
    [[ -n "$api_service" ]] || die 'state file lacks apiServiceName'

    printf 'Stopping ReplicaDB deployment %s\n' "$DEPLOYMENT_ID"
    if service_exists "$api_service"; then
        stop_api_service "$api_service"
        printf '  API service scaled to zero: %s\n' "$api_service"
    fi

    if [[ "$mode" == distributed && -n "$worker_pool" ]]; then
        if worker_pool_exists "$worker_pool"; then
            gcloud beta run worker-pools update "$worker_pool" --project="$PROJECT_ID" --region="$REGION" \
                --instances=0 --quiet >/dev/null || die "could not scale Worker Pool to zero: $worker_pool"
            printf '  Worker Pool scaled to zero: %s\n' "$worker_pool"
        fi
    fi

    if [[ "$cloud_sql_owned" == true && -n "$cloud_sql_instance" ]]; then
        gcloud sql instances patch "$cloud_sql_instance" --project="$PROJECT_ID" \
            --activation-policy=NEVER --quiet >/dev/null || die "could not stop owned Cloud SQL: $cloud_sql_instance"
        printf '  Owned Cloud SQL stopped: %s\n' "$cloud_sql_instance"
    else
        printf '  Shared/external Cloud SQL left untouched\n'
    fi
    printf 'Secrets, network, subnet, service accounts, and IAM bindings left untouched\n'
}

main() {
    parse_args "$@"
    configure_cloud_sdk_python
    require_command gcloud
    load_state
    confirm_stop
    stop_deployment
}

main "$@"
