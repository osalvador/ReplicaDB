#!/usr/bin/env bash

set -euo pipefail

CLOUD_SQL_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$CLOUD_SQL_LIB_DIR/rollback.sh"

REPLICADB_REQUIRE_PRIVATE_IP="${REPLICADB_REQUIRE_PRIVATE_IP:-true}"
CLOUD_SQL_SOURCE=""
CLOUD_SQL_CONNECTION_NAME=""
CLOUD_SQL_PRIVATE_IP=""
CLOUD_SQL_CREATED_INSTANCE=false
CLOUD_SQL_CREATED_DATABASE=false
CLOUD_SQL_CREATED_USER=false
CLOUD_SQL_GENERATED_PASSWORD=""
CLOUD_SQL_TIER="${REPLICADB_CLOUD_SQL_TIER:-db-custom-2-7680}"
CLOUD_SQL_STORAGE_GB="${REPLICADB_CLOUD_SQL_STORAGE_GB:-20}"
CLOUD_SQL_HA="${REPLICADB_CLOUD_SQL_HA:-false}"
CLOUD_SQL_BACKUPS="${REPLICADB_CLOUD_SQL_BACKUPS:-true}"
CLOUD_SQL_DELETION_PROTECTION="${REPLICADB_CLOUD_SQL_DELETION_PROTECTION:-false}"

cloud_sql_fail() {
    printf 'Cloud SQL error: %s\n' "$*" >&2
    return 1
}

cloud_sql_require_tools() {
    command -v gcloud >/dev/null 2>&1 || { cloud_sql_fail 'required command is not installed: gcloud'; return 1; }
    command -v jq >/dev/null 2>&1 || { cloud_sql_fail 'required command is not installed: jq'; return 1; }
}

cloud_sql_resolve_connection() {
    if [[ -n "${CLOUD_SQL_CONNECTION:-}" ]]; then
        IFS=: read -r connection_project connection_region connection_instance <<<"$CLOUD_SQL_CONNECTION"
        [[ "$connection_project" == "$PROJECT_ID" ]] || cloud_sql_fail "Cloud SQL connection belongs to a different project: $connection_project" || return 1
        CLOUD_SQL_INSTANCE=$connection_instance
        CLOUD_SQL_CONNECTION_NAME=$CLOUD_SQL_CONNECTION
    fi
    [[ -n "${CLOUD_SQL_INSTANCE:-}" ]] || return 0
    [[ -n "$CLOUD_SQL_CONNECTION_NAME" ]] || CLOUD_SQL_CONNECTION_NAME="${PROJECT_ID}:${REGION}:${CLOUD_SQL_INSTANCE}"
}

cloud_sql_validate_existing() {
    local instance_json sql_region sql_version sql_state private_ip database_names user_names
    cloud_sql_require_tools
    if [[ -z "${CLOUD_SQL_INSTANCE:-}" && -z "${CLOUD_SQL_CONNECTION:-}" && -n "${DB_URL:-}" ]]; then
        CLOUD_SQL_SOURCE=db-url
        printf 'Using externally managed database credentials\n'
        return 0
    fi
    cloud_sql_resolve_connection || return 1
    [[ -n "${CLOUD_SQL_INSTANCE:-}" ]] || { cloud_sql_fail 'an existing Cloud SQL instance or DB_URL is required'; return 1; }
    instance_json=$(gcloud sql instances describe "$CLOUD_SQL_INSTANCE" --project="$PROJECT_ID" --format=json 2>/dev/null) || {
        cloud_sql_fail "cannot describe Cloud SQL instance: $CLOUD_SQL_INSTANCE"
        return 1
    }
    sql_region=$(printf '%s\n' "$instance_json" | jq -r '.region // empty')
    sql_version=$(printf '%s\n' "$instance_json" | jq -r '.databaseVersion // empty')
    sql_state=$(printf '%s\n' "$instance_json" | jq -r '.state // empty')
    CLOUD_SQL_PRIVATE_IP=$(printf '%s\n' "$instance_json" | jq -r '[.ipAddresses[]? | select(.type == "PRIVATE") | .ipAddress][0] // empty')
    CLOUD_SQL_CONNECTION_NAME=$(printf '%s\n' "$instance_json" | jq -r '.connectionName // empty')
    [[ "$sql_region" == "$REGION" ]] || { cloud_sql_fail "Cloud SQL region is $sql_region, expected $REGION"; return 1; }
    [[ "$sql_version" == POSTGRES* ]] || { cloud_sql_fail "Cloud SQL instance is not PostgreSQL: $sql_version"; return 1; }
    [[ "$sql_state" == RUNNABLE ]] || { cloud_sql_fail "Cloud SQL instance is not runnable: $sql_state"; return 1; }
    if [[ "$REPLICADB_REQUIRE_PRIVATE_IP" == true && -z "$CLOUD_SQL_PRIVATE_IP" ]]; then
        cloud_sql_fail 'Cloud SQL has no private IP; Direct VPC egress requires a private-IP instance'
        return 1
    fi
    database_names=$(gcloud sql databases list --instance="$CLOUD_SQL_INSTANCE" --project="$PROJECT_ID" --format='value(name)' 2>/dev/null) || {
        cloud_sql_fail 'cannot list Cloud SQL databases; check Cloud SQL Admin permissions'
        return 1
    }
    printf '%s\n' "$database_names" | awk -v expected="${DATABASE:-replicadb}" '$0 == expected { found = 1 } END { exit !found }' || {
        cloud_sql_fail "Cloud SQL database does not exist: ${DATABASE:-replicadb}"
        return 1
    }
    user_names=$(gcloud sql users list --instance="$CLOUD_SQL_INSTANCE" --project="$PROJECT_ID" --format='value(name)' 2>/dev/null) || {
        cloud_sql_fail 'cannot list Cloud SQL users; check Cloud SQL Admin permissions'
        return 1
    }
    printf '%s\n' "$user_names" | awk -v expected="${DB_USER:-replicadb}" '$0 == expected { found = 1 } END { exit !found }' || {
        cloud_sql_fail "Cloud SQL user does not exist: ${DB_USER:-replicadb}"
        return 1
    }
    CLOUD_SQL_SOURCE=instance
    printf 'Cloud SQL validated: instance %s in %s\n' "$CLOUD_SQL_INSTANCE" "$REGION"
}

cloud_sql_generate_password() {
    if command -v openssl >/dev/null 2>&1; then
        CLOUD_SQL_GENERATED_PASSWORD=$(openssl rand -hex 24)
    else
        CLOUD_SQL_GENERATED_PASSWORD=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
    fi
    [[ -n "$CLOUD_SQL_GENERATED_PASSWORD" ]] || { cloud_sql_fail 'could not generate a database password'; return 1; }
}

cloud_sql_create() {
    local availability='ZONAL'
    local backup_flag='--enable-bin-log'
    local created_instance_json
    cloud_sql_require_tools
    [[ "${CONFIRMATION:-}" == 'CREATE CLOUD SQL' ]] || { cloud_sql_fail 'Cloud SQL creation requires the exact confirmation: CREATE CLOUD SQL'; return 1; }
    [[ -n "${CLOUD_SQL_INSTANCE:-}" ]] || { cloud_sql_fail 'Cloud SQL instance name is required for creation'; return 1; }
    [[ "$CLOUD_SQL_INSTANCE" =~ ^[a-z][a-z0-9-]{0,62}[a-z0-9]$ ]] || { cloud_sql_fail 'invalid Cloud SQL instance name'; return 1; }
    [[ -n "${NETWORK:-}" ]] || { cloud_sql_fail 'Cloud SQL creation requires an existing --network for private IP'; return 1; }
    if gcloud sql instances describe "$CLOUD_SQL_INSTANCE" --project="$PROJECT_ID" >/dev/null 2>&1; then
        cloud_sql_fail "Cloud SQL instance already exists; reuse it instead of creating: $CLOUD_SQL_INSTANCE"
        return 1
    fi
    if [[ "$CLOUD_SQL_HA" == true ]]; then availability='REGIONAL'; fi
    if [[ "$CLOUD_SQL_BACKUPS" != true ]]; then backup_flag='--no-enable-bin-log'; fi
    printf 'Creating Cloud SQL: instance=%s region=%s tier=%s HA=%s backups=%s deletion-protection=%s\n' \
        "$CLOUD_SQL_INSTANCE" "$REGION" "$CLOUD_SQL_TIER" "$CLOUD_SQL_HA" "$CLOUD_SQL_BACKUPS" "$CLOUD_SQL_DELETION_PROTECTION"
    gcloud sql instances create "$CLOUD_SQL_INSTANCE" --project="$PROJECT_ID" \
        --database-version=POSTGRES_15 --region="$REGION" --tier="$CLOUD_SQL_TIER" \
        --storage-size="$CLOUD_SQL_STORAGE_GB" --availability-type="$availability" \
        "$backup_flag" --no-assign-ip --network="projects/${PROJECT_ID}/global/networks/${NETWORK}" \
        --deletion-protection="$CLOUD_SQL_DELETION_PROTECTION" >/dev/null || {
        cloud_sql_fail 'Cloud SQL instance creation failed'
        rollback_run
        return 1
    }
    CLOUD_SQL_CREATED_INSTANCE=true
    rollback_register instance "$CLOUD_SQL_INSTANCE"
    created_instance_json=$(gcloud sql instances describe "$CLOUD_SQL_INSTANCE" --project="$PROJECT_ID" --format=json 2>/dev/null) || {
        cloud_sql_fail 'Cloud SQL instance was created but its metadata could not be read'
        rollback_run
        return 1
    }
    CLOUD_SQL_PRIVATE_IP=$(printf '%s\n' "$created_instance_json" | jq -r '[.ipAddresses[]? | select(.type == "PRIVATE") | .ipAddress][0] // empty')
    [[ -n "$CLOUD_SQL_PRIVATE_IP" ]] || {
        cloud_sql_fail 'Cloud SQL instance was created without a private IP'
        rollback_run
        return 1
    }
    gcloud sql databases create "$DATABASE" --project="$PROJECT_ID" --instance="$CLOUD_SQL_INSTANCE" >/dev/null || {
        cloud_sql_fail 'Cloud SQL database creation failed; rolling back resources created by this run'
        rollback_run
        return 1
    }
    CLOUD_SQL_CREATED_DATABASE=true
    rollback_register database "$DATABASE"
    cloud_sql_generate_password || { rollback_run; return 1; }
    printf '%s\n' "$CLOUD_SQL_GENERATED_PASSWORD" | gcloud sql users create "$DB_USER" \
        --project="$PROJECT_ID" --instance="$CLOUD_SQL_INSTANCE" >/dev/null || {
        cloud_sql_fail 'Cloud SQL user creation failed; rolling back resources created by this run'
        rollback_run
        return 1
    }
    CLOUD_SQL_CREATED_USER=true
    rollback_register user "$DB_USER"
    CLOUD_SQL_SOURCE=created
    CLOUD_SQL_CONNECTION_NAME="${PROJECT_ID}:${REGION}:${CLOUD_SQL_INSTANCE}"
    printf 'Cloud SQL created: instance=%s database=%s user=%s\n' "$CLOUD_SQL_INSTANCE" "$DATABASE" "$DB_USER"
}