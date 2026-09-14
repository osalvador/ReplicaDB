#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${REPLICADB_GCP_CONFIG_FILE:-${SCRIPT_DIR}/config.env}"

usage() {
    cat <<'EOF'
Usage: deploy.sh <command> [options]

Commands:
  preflight                 Validate local and Google Cloud prerequisites
  deploy                    Deploy ReplicaDB in simple or distributed mode
  verify                    Verify an existing deployment
  destroy                   Remove resources owned by a deployment

Common options:
  --project PROJECT         Google Cloud project ID (required for cloud commands)
  --region REGION           Cloud Run region (default: europe-west4)
  --mode MODE               simple or distributed (default: simple)
  --image IMAGE             Complete immutable image reference
  --image-tag TAG           Release tag for the default Docker Hub image
  --image-digest DIGEST     Immutable sha256 digest for the default image
    --artifact-registry-image IMAGE
                                                        Optional Artifact Registry mirror destination
    --allow-latest            Explicitly allow the mutable latest tag
  --deployment-id ID        Stable deployment identifier
  --state-file PATH         Redacted deployment state path
  --network NAME            Existing VPC network
  --subnet NAME             Existing VPC subnet
  --cloud-sql-instance NAME Existing Cloud SQL instance
  --cloud-sql-connection ID Existing Cloud SQL connection name
  --database NAME           PostgreSQL database name
  --db-user NAME            PostgreSQL application user
  --db-url URL              Existing database URL (never printed)
  --service-account EMAIL   API service account
  --worker-service-account EMAIL
                            Worker service account
  --api-min-instances N     API minimum instances (default: 1)
  --api-max-instances N     API maximum instances (default: 10)
  --worker-instances N      Worker count (default: 1 in distributed mode)
  --create-cloud-sql        Permit Cloud SQL creation after confirmation
    --keep-cloud-sql          Preserve the owned Cloud SQL instance during destroy
    --keep-secrets             Preserve owned Secret Manager secrets during destroy
    --orphan-report            Report labeled resources without deleting them
  --confirmation TEXT       Confirmation text for durable resource creation
  --non-interactive         Do not read confirmations from stdin
  --help                    Show this help

Environment variables are documented in config.example.env.
EOF
}

die() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is not installed: $1"
}

load_config() {
    if [[ -f "$CONFIG_FILE" ]]; then
        # The example uses shell assignments only; users may keep values in a local ignored file.
        # shellcheck disable=SC1090
        source "$CONFIG_FILE"
    fi
}

initialize_config() {
    PROJECT_ID="${REPLICADB_GCP_PROJECT:-}"
    REGION="${REPLICADB_GCP_REGION:-europe-west4}"
    MODE="${REPLICADB_GCP_MODE:-simple}"
    IMAGE="${REPLICADB_SERVER_IMAGE:-}"
    IMAGE_TAG="${REPLICADB_SERVER_VERSION:-1.0.0}"
    IMAGE_DIGEST="${REPLICADB_SERVER_DIGEST:-}"
    DEPLOYMENT_ID="${REPLICADB_DEPLOYMENT_ID:-replicadb-$(date -u +%Y%m%d%H%M%S)}"
    STATE_FILE="${REPLICADB_STATE_FILE:-${HOME:-/tmp}/.replicadb-gcp/${DEPLOYMENT_ID}.state}"
    NETWORK="${REPLICADB_GCP_NETWORK:-}"
    SUBNET="${REPLICADB_GCP_SUBNET:-}"
    CLOUD_SQL_INSTANCE="${REPLICADB_CLOUD_SQL_INSTANCE:-}"
    CLOUD_SQL_CONNECTION="${REPLICADB_CLOUD_SQL_CONNECTION:-}"
    DATABASE="${REPLICADB_DATABASE:-replicadb}"
    DB_USER="${REPLICADB_DB_USER:-replicadb}"
    DB_URL="${REPLICADB_DB_URL:-}"
    SERVICE_ACCOUNT="${REPLICADB_API_SERVICE_ACCOUNT:-}"
    WORKER_SERVICE_ACCOUNT="${REPLICADB_WORKER_SERVICE_ACCOUNT:-}"
    API_MIN_INSTANCES="${REPLICADB_API_MIN_INSTANCES:-1}"
    API_MAX_INSTANCES="${REPLICADB_API_MAX_INSTANCES:-10}"
    WORKER_INSTANCES="${REPLICADB_WORKER_INSTANCES:-1}"
    CREATE_CLOUD_SQL=false
    CONFIRMATION="${REPLICADB_CONFIRMATION:-}"
    NON_INTERACTIVE=false
}

initialize_config

source_modules() {
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/preflight.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/image.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/cloud_sql.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/secrets.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/naming.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/cloud_run_service.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/worker_pool.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/verify.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/state.sh"
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/lib/cleanup.sh"
}

is_valid_identifier() {
    [[ "$1" =~ ^[a-z][a-z0-9-]{0,62}[a-z0-9]$|^[a-z]$ ]]
}

is_valid_image() {
    local image=$1
    [[ -n "$image" && "$image" != *[[:space:]]* && "$image" != *'"'* && "$image" != *"'"* && "$image" != *';'* ]] || return 1
    [[ "$image" == */* || "$image" == *@sha256:* ]] || return 1
    if [[ "$image" == *:latest || "$image" == *@latest ]]; then
        [[ "${ALLOW_MUTABLE_IMAGE:-false}" == true ]] || return 1
    fi
}

validate_common() {
    [[ -n "$PROJECT_ID" ]] || die 'project is required; use --project or REPLICADB_GCP_PROJECT'
    [[ "$MODE" == simple || "$MODE" == distributed ]] || die "mode must be simple or distributed: $MODE"
    [[ "$REGION" =~ ^[a-z][a-z0-9-]+[0-9]$ ]] || die "invalid region: $REGION"
    is_valid_identifier "$DEPLOYMENT_ID" || die "invalid deployment ID: $DEPLOYMENT_ID"
    [[ "$API_MIN_INSTANCES" =~ ^[0-9]+$ ]] || die 'api minimum instances must be a non-negative integer'
    [[ "$API_MAX_INSTANCES" =~ ^[0-9]+$ ]] || die 'api maximum instances must be a non-negative integer'
    (( API_MIN_INSTANCES <= API_MAX_INSTANCES )) || die 'api minimum instances cannot exceed maximum instances'
    [[ "$WORKER_INSTANCES" =~ ^[0-9]+$ ]] || die 'worker instances must be a non-negative integer'
    if [[ "$MODE" == distributed && "$WORKER_INSTANCES" -lt 1 ]]; then
        die 'distributed mode requires at least one worker; use simple mode to disable workers'
    fi
    if [[ -n "$IMAGE" ]]; then
        is_valid_image "$IMAGE" || die "invalid image reference: $IMAGE"
    elif [[ -n "$IMAGE_DIGEST" ]]; then
        [[ "$IMAGE_DIGEST" =~ ^sha256:[a-f0-9]{64}$ ]] || die 'image digest must use sha256:<64 lowercase hex characters>'
    else
        IMAGE="osalvador/replicadb-server:${IMAGE_TAG}"
        is_valid_image "$IMAGE" || die "invalid image reference: $IMAGE"
    fi
    if [[ -n "$CLOUD_SQL_CONNECTION" && ! "$CLOUD_SQL_CONNECTION" =~ ^[^:]+:[^:]+:[^:]+$ ]]; then
        die "invalid Cloud SQL connection name: $CLOUD_SQL_CONNECTION"
    fi
}

redacted_summary() {
    printf 'Deployment summary\n'
    printf '  project: %s\n' "$PROJECT_ID"
    printf '  region: %s\n' "$REGION"
    printf '  mode: %s\n' "$MODE"
    printf '  image: %s\n' "${IMAGE}${IMAGE_DIGEST:+@$IMAGE_DIGEST}"
    printf '  deployment ID: %s\n' "$DEPLOYMENT_ID"
    printf '  state file: %s\n' "$STATE_FILE"
    printf '  Cloud SQL: %s\n' "${CLOUD_SQL_INSTANCE:-existing resource required}"
    printf '  database credentials: %s\n' "$([[ -n "$DB_URL" ]] && printf 'configured' || printf 'Secret Manager references required')"
    printf '  network: %s\n' "${NETWORK:-not configured}"
    printf '  subnet: %s\n' "${SUBNET:-not configured}"
    printf '  public unauthenticated access: disabled\n'
}

require_cloud_tools() {
    require_command gcloud
}

read_confirmation() {
    if [[ -z "$CONFIRMATION" && "$NON_INTERACTIVE" == false && -t 0 ]]; then
        printf 'Type CREATE CLOUD SQL to continue: ' >&2
        IFS= read -r CONFIRMATION
    fi
    [[ "$CONFIRMATION" == 'CREATE CLOUD SQL' ]] || die 'Cloud SQL creation requires the exact confirmation: CREATE CLOUD SQL'
}

parse_args() {
    local command=${1:-help}
    if [[ $# -gt 0 ]]; then
        shift
    fi
    case "$command" in
        help|-h|--help) [[ $# -eq 0 ]] || die "unknown option: $1"; usage; exit 0 ;;
        preflight|deploy|verify|destroy) ;;
        *) die "unknown command: $command" ;;
    esac
    COMMAND=$command

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --project) [[ $# -ge 2 ]] || die '--project requires a value'; PROJECT_ID=$2; shift 2 ;;
            --region) [[ $# -ge 2 ]] || die '--region requires a value'; REGION=$2; shift 2 ;;
            --mode) [[ $# -ge 2 ]] || die '--mode requires a value'; MODE=$2; shift 2 ;;
            --image) [[ $# -ge 2 ]] || die '--image requires a value'; IMAGE=$2; shift 2 ;;
            --image-tag) [[ $# -ge 2 ]] || die '--image-tag requires a value'; IMAGE_TAG=$2; IMAGE=''; shift 2 ;;
            --image-digest) [[ $# -ge 2 ]] || die '--image-digest requires a value'; IMAGE_DIGEST=$2; shift 2 ;;
            --artifact-registry-image) [[ $# -ge 2 ]] || die '--artifact-registry-image requires a value'; IMAGE_MIRROR=$2; shift 2 ;;
            --allow-latest) ALLOW_MUTABLE_IMAGE=true; shift ;;
            --deployment-id) [[ $# -ge 2 ]] || die '--deployment-id requires a value'; DEPLOYMENT_ID=$2; shift 2 ;;
            --state-file) [[ $# -ge 2 ]] || die '--state-file requires a value'; STATE_FILE=$2; shift 2 ;;
            --network) [[ $# -ge 2 ]] || die '--network requires a value'; NETWORK=$2; shift 2 ;;
            --subnet) [[ $# -ge 2 ]] || die '--subnet requires a value'; SUBNET=$2; shift 2 ;;
            --cloud-sql-instance) [[ $# -ge 2 ]] || die '--cloud-sql-instance requires a value'; CLOUD_SQL_INSTANCE=$2; shift 2 ;;
            --cloud-sql-connection) [[ $# -ge 2 ]] || die '--cloud-sql-connection requires a value'; CLOUD_SQL_CONNECTION=$2; shift 2 ;;
            --database) [[ $# -ge 2 ]] || die '--database requires a value'; DATABASE=$2; shift 2 ;;
            --db-user) [[ $# -ge 2 ]] || die '--db-user requires a value'; DB_USER=$2; shift 2 ;;
            --db-url) [[ $# -ge 2 ]] || die '--db-url requires a value'; DB_URL=$2; shift 2 ;;
            --service-account) [[ $# -ge 2 ]] || die '--service-account requires a value'; SERVICE_ACCOUNT=$2; shift 2 ;;
            --worker-service-account) [[ $# -ge 2 ]] || die '--worker-service-account requires a value'; WORKER_SERVICE_ACCOUNT=$2; shift 2 ;;
            --api-min-instances) [[ $# -ge 2 ]] || die '--api-min-instances requires a value'; API_MIN_INSTANCES=$2; shift 2 ;;
            --api-max-instances) [[ $# -ge 2 ]] || die '--api-max-instances requires a value'; API_MAX_INSTANCES=$2; shift 2 ;;
            --worker-instances) [[ $# -ge 2 ]] || die '--worker-instances requires a value'; WORKER_INSTANCES=$2; shift 2 ;;
            --create-cloud-sql) CREATE_CLOUD_SQL=true; shift ;;
            --keep-cloud-sql) CLEANUP_KEEP_CLOUD_SQL=true; shift ;;
            --keep-secrets) CLEANUP_KEEP_SECRETS=true; shift ;;
            --orphan-report) CLEANUP_ORPHAN_REPORT=true; shift ;;
            --confirmation) [[ $# -ge 2 ]] || die '--confirmation requires a value'; CONFIRMATION=$2; shift 2 ;;
            --non-interactive) NON_INTERACTIVE=true; shift ;;
            --help|-h) usage; exit 0 ;;
            *) die "unknown option: $1" ;;
        esac
    done
}

main() {
    load_config
    initialize_config
    source_modules
    parse_args "$@"
    validate_common

    case "$COMMAND" in
        help) ;;
        preflight)
            redacted_summary
            preflight_run
            ;;
        verify)
            require_cloud_tools
            redacted_summary
            verify_deployment
            ;;
        destroy)
            require_cloud_tools
            redacted_summary
            cleanup_destroy
            ;;
        deploy)
            require_cloud_tools
            redacted_summary
            preflight_run
            image_resolve >/dev/null
            if [[ "$CREATE_CLOUD_SQL" == true ]]; then
                read_confirmation
                cloud_sql_create
                printf 'Cloud SQL creation confirmed.\n'
            elif [[ -z "$CLOUD_SQL_INSTANCE" && -z "$CLOUD_SQL_CONNECTION" && -z "$DB_URL" ]]; then
                die 'existing Cloud SQL credentials are required, or use --create-cloud-sql'
            else
                cloud_sql_validate_existing
            fi
            secrets_prepare
            if [[ "$MODE" == simple ]]; then
                cloud_run_deploy_service true
            else
                cloud_run_deploy_service false
                worker_pool_deploy
            fi
            state_init "$STATE_FILE"
            state_put deploymentId "$DEPLOYMENT_ID"
            state_put projectId "$PROJECT_ID"
            state_put region "$REGION"
            state_put mode "$MODE"
            state_put image "$FINAL_IMAGE"
            state_put imageDigest "$FINAL_DIGEST"
            state_put apiServiceName "$CLOUD_RUN_SERVICE_NAME"
            state_put workerPoolName "${WORKER_POOL_NAME:-}"
            state_put cloudSqlInstance "${CLOUD_SQL_INSTANCE:-}"
            state_put cloudSqlConnection "$CLOUD_SQL_CONNECTION_NAME"
            state_put databaseName "$DATABASE"
            state_put dbUser "$DB_USER"
            state_put network "${NETWORK:-}"
            state_put subnet "${SUBNET:-}"
            state_put apiServiceAccount "${SERVICE_ACCOUNT:-}"
            state_put workerServiceAccount "${WORKER_SERVICE_ACCOUNT:-}"
            state_put apiMinInstances "$API_MIN_INSTANCES"
            state_put apiMaxInstances "$API_MAX_INSTANCES"
            state_put workerInstances "$WORKER_INSTANCES"
            state_put cloudSqlOwned "$CLOUD_SQL_CREATED_INSTANCE"
            state_put secretsOwned "$(IFS=,; printf '%s' "${SECRET_CREATED_NAMES[*]:-}")"
            state_put dbUsernameSecretName "$DB_USERNAME_SECRET_NAME"
            state_put dbUsernameSecretVersion "$DB_USERNAME_SECRET_VERSION"
            state_put dbPasswordSecretName "$DB_PASSWORD_SECRET_NAME"
            state_put dbPasswordSecretVersion "$DB_PASSWORD_SECRET_VERSION"
            state_put bootstrapUsernameSecretName "$BOOTSTRAP_USERNAME_SECRET_NAME"
            state_put bootstrapUsernameSecretVersion "$BOOTSTRAP_USERNAME_SECRET_VERSION"
            state_put bootstrapPasswordSecretName "$BOOTSTRAP_PASSWORD_SECRET_NAME"
            state_put bootstrapPasswordSecretVersion "$BOOTSTRAP_PASSWORD_SECRET_VERSION"
            state_put keyringVersionSecretName "$KEYRING_VERSION_SECRET_NAME"
            state_put keyringVersionSecretVersion "$KEYRING_VERSION_SECRET_VERSION"
            state_put keyringKeySecretName "$KEYRING_KEY_SECRET_NAME"
            state_put keyringKeySecretVersion "$KEYRING_KEY_SECRET_VERSION"
            state_save
            printf 'Deployment execution will be enabled by the next bundle stages.\n'
            ;;
    esac
}

main "$@"