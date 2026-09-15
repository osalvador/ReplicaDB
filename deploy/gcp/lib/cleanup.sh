#!/usr/bin/env bash

set -euo pipefail

CLEANUP_KEEP_CLOUD_SQL=false
CLEANUP_KEEP_SECRETS=false
CLEANUP_ORPHAN_REPORT=false
CLEANUP_SERVICES=()
CLEANUP_WORKER_POOLS=()
CLEANUP_SECRETS=()
CLEANUP_CLOUD_SQL_INSTANCE=""
CLEANUP_CLOUD_SQL_OWNED=false

cleanup_fail() {
    printf 'Cleanup error: %s\n' "$*" >&2
    return 1
}

cleanup_collect() {
    local owned_secrets secret
    CLEANUP_SERVICES=()
    CLEANUP_WORKER_POOLS=()
    CLEANUP_SECRETS=()
    if [[ -f "$STATE_FILE" ]]; then
        state_load "$STATE_FILE"
        PROJECT_ID=$(state_get projectId)
        REGION=$(state_get region)
        MODE=$(state_get mode)
        DEPLOYMENT_ID=$(state_get deploymentId)
        PUBLIC_ACCESS=$(state_get publicAccess)
        CLEANUP_CLOUD_SQL_INSTANCE=$(state_get cloudSqlInstance)
        CLEANUP_CLOUD_SQL_OWNED=$(state_get cloudSqlOwned)
        [[ -n "$(state_get apiServiceName)" ]] && CLEANUP_SERVICES+=("$(state_get apiServiceName)")
        [[ -n "$(state_get workerPoolName)" ]] && CLEANUP_WORKER_POOLS+=("$(state_get workerPoolName)")
        owned_secrets=$(state_get secretsOwned)
        IFS=',' read -r -a secret_names <<<"$owned_secrets"
        for secret in "${secret_names[@]}"; do
            [[ -n "$secret" ]] && CLEANUP_SECRETS+=("$secret")
        done
    else
        [[ "$CLEANUP_ORPHAN_REPORT" == true ]] || cleanup_fail "state file is missing: $STATE_FILE; use --orphan-report to discover labeled resources"
        local service_names worker_names
        service_names=$(gcloud run services list --project="$PROJECT_ID" --region="$REGION" \
            --filter="metadata.labels.replicadb-deployment=$DEPLOYMENT_ID" --format='value(metadata.name)') || return 1
        worker_names=$(gcloud beta run worker-pools list --project="$PROJECT_ID" --region="$REGION" \
            --filter="metadata.labels.replicadb-deployment=$DEPLOYMENT_ID" --format='value(metadata.name)') || return 1
        [[ -n "$service_names" ]] && while IFS= read -r service; do CLEANUP_SERVICES+=("$service"); done <<<"$service_names"
        [[ -n "$worker_names" ]] && while IFS= read -r worker; do CLEANUP_WORKER_POOLS+=("$worker"); done <<<"$worker_names"
    fi
}

cleanup_print_plan() {
    local item
    printf 'Cleanup plan for deployment %s\n' "$DEPLOYMENT_ID"
    if [[ ${#CLEANUP_SERVICES[@]} -gt 0 ]]; then
        for item in "${CLEANUP_SERVICES[@]}"; do printf '  Cloud Run service: %s\n' "$item"; done
    fi
    if [[ ${#CLEANUP_WORKER_POOLS[@]} -gt 0 ]]; then
        for item in "${CLEANUP_WORKER_POOLS[@]}"; do printf '  Worker Pool: %s\n' "$item"; done
    fi
    if [[ "$CLEANUP_KEEP_SECRETS" == false ]]; then
        if [[ ${#CLEANUP_SECRETS[@]} -gt 0 ]]; then
            for item in "${CLEANUP_SECRETS[@]}"; do printf '  Secret Manager secret: %s\n' "$item"; done
        fi
    fi
    if [[ "$CLEANUP_KEEP_CLOUD_SQL" == false && "$CLEANUP_CLOUD_SQL_OWNED" == true ]]; then
        printf '  Cloud SQL instance: %s\n' "$CLEANUP_CLOUD_SQL_INSTANCE"
    fi
}

cleanup_destroy() {
    local item
    cleanup_collect || return 1
    if [[ "$CLEANUP_ORPHAN_REPORT" == true ]]; then
        cleanup_print_plan
        printf 'Orphan report only; no resources were deleted.\n'
        return 0
    fi
    cleanup_print_plan
    if [[ "${CONFIRMATION:-}" != 'DESTROY REPLICADB' ]]; then
        cleanup_fail 'destruction requires the exact confirmation: DESTROY REPLICADB'
        return 1
    fi
    if [[ ${#CLEANUP_WORKER_POOLS[@]} -gt 0 ]]; then
    for item in "${CLEANUP_WORKER_POOLS[@]}"; do
        gcloud beta run worker-pools delete "$item" --project="$PROJECT_ID" --region="$REGION" --quiet >/dev/null || {
            cleanup_fail "could not delete Worker Pool: $item"; return 1;
        }
    done
    fi
    if [[ ${#CLEANUP_SERVICES[@]} -gt 0 ]]; then
    for item in "${CLEANUP_SERVICES[@]}"; do
        cloud_run_set_public_access "$item" false || {
            cleanup_fail "could not remove public Invoker access: $item"; return 1;
        }
        gcloud run services delete "$item" --project="$PROJECT_ID" --region="$REGION" --quiet >/dev/null || {
            cleanup_fail "could not delete Cloud Run service: $item"; return 1;
        }
    done
    fi
    if [[ "$CLEANUP_KEEP_SECRETS" == false ]]; then
        if [[ ${#CLEANUP_SECRETS[@]} -gt 0 ]]; then
        for item in "${CLEANUP_SECRETS[@]}"; do
            gcloud secrets delete "$item" --project="$PROJECT_ID" --quiet >/dev/null || {
                cleanup_fail "could not delete owned secret: $item"; return 1;
            }
        done
        fi
    fi
    if [[ "$CLEANUP_KEEP_CLOUD_SQL" == false && "$CLEANUP_CLOUD_SQL_OWNED" == true ]]; then
        gcloud sql instances delete "$CLEANUP_CLOUD_SQL_INSTANCE" --project="$PROJECT_ID" --quiet >/dev/null || {
            cleanup_fail "could not delete owned Cloud SQL instance: $CLEANUP_CLOUD_SQL_INSTANCE"; return 1;
        }
    fi
    rm -f "$STATE_FILE"
    printf 'Cleanup completed for deployment %s\n' "$DEPLOYMENT_ID"
}
