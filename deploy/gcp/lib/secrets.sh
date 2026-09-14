#!/usr/bin/env bash

set -euo pipefail

SECRET_CREATED_NAMES=()
SECRET_ACCESSOR_ACCOUNTS=()
SECRET_LAST_VERSION=""
DB_USERNAME_SECRET_NAME="${REPLICADB_DB_USERNAME_SECRET:-replicadb-db-username}"
DB_PASSWORD_SECRET_NAME="${REPLICADB_DB_PASSWORD_SECRET:-replicadb-db-password}"
BOOTSTRAP_USERNAME_SECRET_NAME="${REPLICADB_BOOTSTRAP_USERNAME_SECRET:-replicadb-bootstrap-admin-username}"
BOOTSTRAP_PASSWORD_SECRET_NAME="${REPLICADB_BOOTSTRAP_PASSWORD_SECRET:-replicadb-bootstrap-admin-password}"
KEYRING_VERSION_SECRET_NAME="${REPLICADB_KEYRING_VERSION_SECRET:-replicadb-keyring-current-version}"
KEYRING_KEY_SECRET_NAME="${REPLICADB_KEYRING_KEY_SECRET:-replicadb-keyring-current-key}"
DB_USERNAME_SECRET_VERSION="${REPLICADB_DB_USERNAME_SECRET_VERSION:-}"
DB_PASSWORD_SECRET_VERSION="${REPLICADB_DB_PASSWORD_SECRET_VERSION:-}"
BOOTSTRAP_USERNAME_SECRET_VERSION="${REPLICADB_BOOTSTRAP_USERNAME_SECRET_VERSION:-}"
BOOTSTRAP_PASSWORD_SECRET_VERSION="${REPLICADB_BOOTSTRAP_PASSWORD_SECRET_VERSION:-}"
KEYRING_VERSION_SECRET_VERSION="${REPLICADB_KEYRING_VERSION_SECRET_VERSION:-}"
KEYRING_KEY_SECRET_VERSION="${REPLICADB_KEYRING_KEY_SECRET_VERSION:-}"

secrets_fail() {
    printf 'Secret Manager error: %s\n' "$*" >&2
    return 1
}

secrets_require_tools() {
    command -v gcloud >/dev/null 2>&1 || { secrets_fail 'required command is not installed: gcloud'; return 1; }
}

secrets_validate_name() {
    [[ "$1" =~ ^[a-zA-Z0-9_-]{1,255}$ ]] || { secrets_fail "invalid Secret Manager name: $1"; return 1; }
}

secrets_generate_value() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 24
    else
        od -An -N24 -tx1 /dev/urandom | tr -d ' \n'
    fi
}

secrets_latest_version() {
    local name=$1
    gcloud secrets versions list "$name" --project="$PROJECT_ID" \
        --filter='state=ENABLED' --sort-by='~createTime' --limit=1 --format='value(name)' 2>/dev/null \
        | sed 's#^.*/##' | awk 'NF { print; exit }'
}

secrets_create_or_reuse() {
    local name=$1
    local value=$2
    local requested_version=${3:-}
    local version
    secrets_validate_name "$name"
    if gcloud secrets describe "$name" --project="$PROJECT_ID" >/dev/null 2>&1; then
        version=${requested_version:-$(secrets_latest_version "$name")}
        [[ "$version" =~ ^[0-9]+$ ]] || { secrets_fail "existing secret has no explicit enabled version: $name"; return 1; }
    else
        gcloud secrets create "$name" --project="$PROJECT_ID" --replication-policy=automatic >/dev/null || {
            secrets_fail "could not create secret: $name"
            return 1
        }
        SECRET_CREATED_NAMES+=("$name")
        version=$(printf '%s' "$value" | gcloud secrets versions add "$name" --project="$PROJECT_ID" --data-file=- 2>/dev/null \
            | sed 's#^.*/##' | awk 'NF { print; exit }') || {
            secrets_fail "could not add a version to secret: $name"
            return 1
        }
        version=${version:-1}
    fi
    SECRET_LAST_VERSION=$version
}

secrets_grant_accessor() {
    local account=$1
    local name=$2
    [[ -n "$account" ]] || return 0
    gcloud secrets add-iam-policy-binding "$name" --project="$PROJECT_ID" \
        --member="serviceAccount:${account}" --role=roles/secretmanager.secretAccessor >/dev/null 2>&1 || {
        secrets_fail "could not grant Secret Accessor to ${account} for ${name}"
        return 1
    }
}

secrets_prepare() {
    local db_username_value=${DB_USER:-replicadb}
    local db_password_value=${CLOUD_SQL_GENERATED_PASSWORD:-}
    local bootstrap_username_value=${REPLICADB_BOOTSTRAP_ADMIN_USERNAME:-admin}
    local bootstrap_password_value=${REPLICADB_BOOTSTRAP_ADMIN_PASSWORD:-}
    local keyring_version_value=${REPLICADB_SECURITY_KEYRING_CURRENT_VERSION_VALUE:-1}
    local keyring_key_value=${REPLICADB_SECURITY_KEYRING_CURRENT_KEY_VALUE:-}
    local api_account=${SERVICE_ACCOUNT:-}
    local worker_account=${WORKER_SERVICE_ACCOUNT:-}
    secrets_require_tools
    [[ -n "$db_password_value" || -n "${REPLICADB_DB_PASSWORD_SECRET_VERSION:-}" ]] || {
        secrets_fail 'database password requires Cloud SQL creation output or an explicit existing secret version'
        return 1
    }
    bootstrap_password_value=${bootstrap_password_value:-$(secrets_generate_value)}
    keyring_key_value=${keyring_key_value:-$(secrets_generate_value)}
    secrets_create_or_reuse "$DB_USERNAME_SECRET_NAME" "$db_username_value" "$DB_USERNAME_SECRET_VERSION" || { secrets_rollback_created; return 1; }
    DB_USERNAME_SECRET_VERSION=$SECRET_LAST_VERSION
    secrets_create_or_reuse "$DB_PASSWORD_SECRET_NAME" "$db_password_value" "$DB_PASSWORD_SECRET_VERSION" || { secrets_rollback_created; return 1; }
    DB_PASSWORD_SECRET_VERSION=$SECRET_LAST_VERSION
    secrets_create_or_reuse "$BOOTSTRAP_USERNAME_SECRET_NAME" "$bootstrap_username_value" "$BOOTSTRAP_USERNAME_SECRET_VERSION" || { secrets_rollback_created; return 1; }
    BOOTSTRAP_USERNAME_SECRET_VERSION=$SECRET_LAST_VERSION
    secrets_create_or_reuse "$BOOTSTRAP_PASSWORD_SECRET_NAME" "$bootstrap_password_value" "$BOOTSTRAP_PASSWORD_SECRET_VERSION" || { secrets_rollback_created; return 1; }
    BOOTSTRAP_PASSWORD_SECRET_VERSION=$SECRET_LAST_VERSION
    secrets_create_or_reuse "$KEYRING_VERSION_SECRET_NAME" "$keyring_version_value" "$KEYRING_VERSION_SECRET_VERSION" || { secrets_rollback_created; return 1; }
    KEYRING_VERSION_SECRET_VERSION=$SECRET_LAST_VERSION
    secrets_create_or_reuse "$KEYRING_KEY_SECRET_NAME" "$keyring_key_value" "$KEYRING_KEY_SECRET_VERSION" || { secrets_rollback_created; return 1; }
    KEYRING_KEY_SECRET_VERSION=$SECRET_LAST_VERSION
    for secret_name in "$DB_USERNAME_SECRET_NAME" "$DB_PASSWORD_SECRET_NAME" "$BOOTSTRAP_USERNAME_SECRET_NAME" "$BOOTSTRAP_PASSWORD_SECRET_NAME" "$KEYRING_VERSION_SECRET_NAME" "$KEYRING_KEY_SECRET_NAME"; do
        secrets_grant_accessor "$api_account" "$secret_name" || { secrets_rollback_created; return 1; }
        if [[ "$MODE" == distributed ]]; then
            secrets_grant_accessor "$worker_account" "$secret_name" || { secrets_rollback_created; return 1; }
        fi
    done
    unset db_password_value bootstrap_password_value keyring_key_value
    printf 'Secret Manager references prepared for API and worker runtime identities\n'
}

secrets_rollback_created() {
    local name
    for name in "${SECRET_CREATED_NAMES[@]}"; do
        gcloud secrets delete "$name" --project="$PROJECT_ID" --quiet >/dev/null 2>&1 || true
    done
    SECRET_CREATED_NAMES=()
}