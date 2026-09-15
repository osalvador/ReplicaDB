#!/usr/bin/env bash

set -euo pipefail

KEYRING_FILE_SECRET_NAME="${KEYRING_FILE_SECRET_NAME:-replicadb-master-key}"
KEYRING_FILE_SECRET_VERSION="${KEYRING_FILE_SECRET_VERSION:-1}"

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
    local env_vars secret_refs database_url
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
    if [[ -n "${CLOUD_SQL_PRIVATE_IP:-}" ]]; then
        database_url="jdbc:postgresql://${CLOUD_SQL_PRIVATE_IP}:5432/${DATABASE:-replicadb}"
    elif [[ -n "${DB_URL:-}" && "$DB_URL" != *'@'* && "$DB_URL" != *'password='* ]]; then
        database_url=$DB_URL
    else
        worker_pool_fail 'a private database IP or credential-free DB_URL is required for the worker'; return 1
    fi
    secret_refs="DB_USERNAME=${DB_USERNAME_SECRET_NAME}:${DB_USERNAME_SECRET_VERSION},DB_PASSWORD=${DB_PASSWORD_SECRET_NAME}:${DB_PASSWORD_SECRET_VERSION},REPLICADB_SECURITY_KEYRING_CURRENT_VERSION=${KEYRING_VERSION_SECRET_NAME}:${KEYRING_VERSION_SECRET_VERSION},REPLICADB_SECURITY_KEYRING_CURRENT_KEY=${KEYRING_KEY_SECRET_NAME}:${KEYRING_KEY_SECRET_VERSION},REPLICADB_SECURITY_MASTER_KEY_JSON=${KEYRING_FILE_SECRET_NAME}:${KEYRING_FILE_SECRET_VERSION}"
    env_vars="SPRING_PROFILES_ACTIVE=worker,SERVER_PORT=-1,DB_URL=${database_url},REPLICADB_SECURITY_MASTER_KEY_FILE=/tmp/replicadb-master-key,REPLICADB_WORKER_MANAGEMENT_PORT=${WORKER_MANAGEMENT_PORT},REPLICADB_WORKER_MANAGEMENT_ADDRESS=${WORKER_MANAGEMENT_ADDRESS},REPLICADB_WORKER_IDENTITY=${WORKER_IDENTITY}"
    worker_args='-c,umask 077; printf "%s" "$REPLICADB_SECURITY_MASTER_KEY_JSON" > "$REPLICADB_SECURITY_MASTER_KEY_FILE"; exec java ${JAVA_OPTS:-} -Dreplicadb.embedded-postgres.enabled=false -Dspring.profiles.active=${SPRING_PROFILES_ACTIVE:-worker} -jar /opt/replicadb/replicadb-server.jar'
    gcloud beta run worker-pools deploy "$WORKER_POOL_NAME" --project="$PROJECT_ID" --region="$REGION" \
        --image="$FINAL_IMAGE" --service-account="$WORKER_SERVICE_ACCOUNT" --instances="$WORKER_INSTANCES" \
        --command=sh --args="$worker_args" --set-env-vars="$env_vars" --set-secrets="$secret_refs" \
        --network="$NETWORK" --subnet="$SUBNET" \
        >/dev/null || { worker_pool_fail "could not deploy Worker Pool: $WORKER_POOL_NAME"; return 1; }
    printf 'Cloud Run Worker Pool deployed: %s instances=%s\n' "$WORKER_POOL_NAME" "$WORKER_INSTANCES"
}
