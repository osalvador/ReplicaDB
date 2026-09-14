#!/usr/bin/env bash

set -euo pipefail

WORKER_POOL_NAME=""
WORKER_MANAGEMENT_PORT="${REPLICADB_WORKER_MANAGEMENT_PORT:-9091}"
WORKER_MANAGEMENT_ADDRESS="${REPLICADB_WORKER_MANAGEMENT_ADDRESS:-127.0.0.1}"
WORKER_IDENTITY="${REPLICADB_WORKER_IDENTITY:-}"

worker_pool_fail() {
    printf 'Cloud Run Worker Pool error: %s\n' "$*" >&2
    return 1
}

worker_pool_prepare() {
    WORKER_POOL_NAME=${WORKER_POOL_NAME:-$(naming_resource_name worker "${REPLICADB_GCP_PREFIX:-replicadb}" "$DEPLOYMENT_ID")}
    WORKER_IDENTITY=${WORKER_IDENTITY:-"${WORKER_POOL_NAME}-worker"}
    [[ "$WORKER_MANAGEMENT_PORT" =~ ^[0-9]+$ && "$WORKER_MANAGEMENT_PORT" -ge 1024 && "$WORKER_MANAGEMENT_PORT" -le 65535 ]] || {
        worker_pool_fail 'worker management port must be between 1024 and 65535'; return 1;
    }
    if [[ "${REPLICADB_WORKER_PLATFORM_PROBE:-false}" == true ]]; then
        WORKER_MANAGEMENT_ADDRESS=0.0.0.0
    elif [[ "$WORKER_MANAGEMENT_ADDRESS" == 0.0.0.0 ]]; then
        worker_pool_fail '0.0.0.0 management binding requires REPLICADB_WORKER_PLATFORM_PROBE=true'; return 1
    fi
}

worker_pool_deploy() {
    local env_vars secret_refs
    worker_pool_prepare || return 1
    [[ "$MODE" == distributed ]] || { worker_pool_fail 'Worker Pool deployment requires distributed mode'; return 1; }
    [[ -n "${WORKER_SERVICE_ACCOUNT:-}" ]] || { worker_pool_fail 'worker service account is required'; return 1; }
    [[ -n "${FINAL_IMAGE:-}" ]] || { worker_pool_fail 'immutable worker image is required'; return 1; }
    if [[ "${WORKER_INSTANCES:-1}" == 0 ]]; then
        gcloud beta run worker-pools update "$WORKER_POOL_NAME" --project="$PROJECT_ID" --region="$REGION" \
            --instances=0 >/dev/null || { worker_pool_fail 'could not scale Worker Pool to zero'; return 1; }
        printf 'Cloud Run Worker Pool disabled: %s\n' "$WORKER_POOL_NAME"
        return 0
    fi
    [[ "$WORKER_INSTANCES" =~ ^[0-9]+$ ]] || { worker_pool_fail 'worker instances must be a non-negative integer'; return 1; }
    secret_refs="DB_USERNAME=${DB_USERNAME_SECRET_NAME}:${DB_USERNAME_SECRET_VERSION},DB_PASSWORD=${DB_PASSWORD_SECRET_NAME}:${DB_PASSWORD_SECRET_VERSION},REPLICADB_SECURITY_KEYRING_CURRENT_VERSION=${KEYRING_VERSION_SECRET_NAME}:${KEYRING_VERSION_SECRET_VERSION},REPLICADB_SECURITY_KEYRING_CURRENT_KEY=${KEYRING_KEY_SECRET_NAME}:${KEYRING_KEY_SECRET_VERSION}"
    env_vars="SPRING_PROFILES_ACTIVE=worker,SERVER_PORT=-1,REPLICADB_WORKER_MANAGEMENT_PORT=${WORKER_MANAGEMENT_PORT},REPLICADB_WORKER_MANAGEMENT_ADDRESS=${WORKER_MANAGEMENT_ADDRESS},REPLICADB_WORKER_IDENTITY=${WORKER_IDENTITY}"
    gcloud beta run worker-pools deploy "$WORKER_POOL_NAME" --project="$PROJECT_ID" --region="$REGION" \
        --image="$FINAL_IMAGE" --service-account="$WORKER_SERVICE_ACCOUNT" --instances="$WORKER_INSTANCES" \
        --scaling=manual --set-env-vars="$env_vars" --set-secrets="$secret_refs" \
        --network="$NETWORK" --subnet="$SUBNET" \
        --startup-probe="tcpSocket.port=${WORKER_MANAGEMENT_PORT},initialDelaySeconds=10,periodSeconds=10,failureThreshold=30" \
        >/dev/null || { worker_pool_fail "could not deploy Worker Pool: $WORKER_POOL_NAME"; return 1; }
    printf 'Cloud Run Worker Pool deployed: %s instances=%s\n' "$WORKER_POOL_NAME" "$WORKER_INSTANCES"
}