#!/usr/bin/env bash

set -euo pipefail

CLOUD_RUN_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$CLOUD_RUN_LIB_DIR/naming.sh"

CLOUD_RUN_SERVICE_NAME=""
API_SERVICE_URL=""
CLOUD_RUN_SERVICE_YAML=""
KEYRING_FILE_SECRET_NAME="${KEYRING_FILE_SECRET_NAME:-replicadb-master-key}"
KEYRING_FILE_SECRET_VERSION="${KEYRING_FILE_SECRET_VERSION:-1}"

cloud_run_fail() {
    printf 'Cloud Run service error: %s\n' "$*" >&2
    return 1
}

cloud_run_secret_env() {
    local env_name=$1
    local secret_name=$2
    local secret_version=$3
    cat <<EOF
            - name: ${env_name}
              valueFrom:
                secretKeyRef:
                  name: ${secret_name}
                  key: "${secret_version}"
EOF
}

cloud_run_database_url() {
    if [[ -n "${CLOUD_SQL_PRIVATE_IP:-}" ]]; then
        printf 'jdbc:postgresql://%s:5432/%s' "$CLOUD_SQL_PRIVATE_IP" "${DATABASE:-replicadb}"
    elif [[ -n "${DB_URL:-}" && "$DB_URL" != *'@'* && "$DB_URL" != *'password='* ]]; then
        printf '%s' "$DB_URL"
    else
        cloud_run_fail 'a private Cloud SQL IP or credential-free DB_URL is required for the service contract'
        return 1
    fi
}

cloud_run_set_public_access() {
  local service_name=${1:-$CLOUD_RUN_SERVICE_NAME}
  local public_access=${2:-${PUBLIC_ACCESS:-false}}
  local ingress=internal-and-cloud-load-balancing
  if [[ "$public_access" == true ]]; then
    ingress=all
  fi
  gcloud run services update "$service_name" --project="$PROJECT_ID" --region="$REGION" \
    --update-annotations="run.googleapis.com/ingress=${ingress}" --quiet >/dev/null || {
    cloud_run_fail "could not set Cloud Run ingress for service: $service_name"
    return 1
  }
  if [[ "$public_access" == true ]]; then
    gcloud run services add-iam-policy-binding "$service_name" --project="$PROJECT_ID" --region="$REGION" \
      --member=allUsers --role=roles/run.invoker --quiet >/dev/null || {
      cloud_run_fail "could not grant public Invoker access for service: $service_name"
      return 1
    }
  else
    gcloud run services remove-iam-policy-binding "$service_name" --project="$PROJECT_ID" --region="$REGION" \
      --member=allUsers --role=roles/run.invoker --quiet >/dev/null || {
      cloud_run_fail "could not remove public Invoker access for service: $service_name"
      return 1
    }
  fi
}

cloud_run_render_service() {
    local output_path=$1
    local local_execution=$2
    local service_account=${SERVICE_ACCOUNT:-}
    local database_url
    local network_annotation=''
    local ingress=internal-and-cloud-load-balancing
    [[ -n "$FINAL_IMAGE" ]] || { cloud_run_fail 'immutable FINAL_IMAGE is required'; return 1; }
    [[ -n "$service_account" ]] || { cloud_run_fail 'API service account is required'; return 1; }
    [[ -n "${DB_USERNAME_SECRET_VERSION:-}" && -n "${DB_PASSWORD_SECRET_VERSION:-}" ]] || {
        cloud_run_fail 'database secret versions are required'; return 1;
    }
    database_url=$(cloud_run_database_url) || return 1
    [[ "${PUBLIC_ACCESS:-false}" == true ]] && ingress=all
    if [[ -n "${NETWORK:-}" && -n "${SUBNET:-}" ]]; then
      network_annotation="  run.googleapis.com/network-interfaces: '[{\"network\":\"${NETWORK}\",\"subnetwork\":\"${SUBNET}\"}]'"
    fi
    CLOUD_RUN_SERVICE_NAME=${CLOUD_RUN_SERVICE_NAME:-$(naming_resource_name api "${REPLICADB_GCP_PREFIX:-replicadb}" "$DEPLOYMENT_ID")}
    mkdir -p "$(dirname "$output_path")"
    umask 077
    cat >"$output_path" <<EOF
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: ${CLOUD_RUN_SERVICE_NAME}
  labels:
    replicadb-deployment: ${DEPLOYMENT_ID}
  annotations:
    run.googleapis.com/ingress: ${ingress}
spec:
  template:
    metadata:
      labels:
        replicadb-deployment: ${DEPLOYMENT_ID}
      annotations:
        autoscaling.knative.dev/minScale: "${API_MIN_INSTANCES:-1}"
        autoscaling.knative.dev/maxScale: "${API_MAX_INSTANCES:-10}"
        run.googleapis.com/cpu-throttling: "false"
        run.googleapis.com/startup-cpu-boost: "true"
        run.googleapis.com/vpc-access-egress: all-traffic
      ${network_annotation}
    spec:
      serviceAccountName: ${service_account}
      containerConcurrency: 80
      timeoutSeconds: 300
      containers:
        - image: ${FINAL_IMAGE}
          env:
            - name: SPRING_PROFILES_ACTIVE
              value: api
            - name: REPLICADB_SERVER_LOCAL_EXECUTION_ENABLED
              value: "${local_execution}"
            - name: DB_URL
              value: "${database_url}"
$(cloud_run_secret_env DB_USERNAME "$DB_USERNAME_SECRET_NAME" "$DB_USERNAME_SECRET_VERSION")
$(cloud_run_secret_env DB_PASSWORD "$DB_PASSWORD_SECRET_NAME" "$DB_PASSWORD_SECRET_VERSION")
$(cloud_run_secret_env REPLICADB_SECURITY_KEYRING_CURRENT_VERSION "$KEYRING_VERSION_SECRET_NAME" "$KEYRING_VERSION_SECRET_VERSION")
$(cloud_run_secret_env REPLICADB_SECURITY_KEYRING_CURRENT_KEY "$KEYRING_KEY_SECRET_NAME" "$KEYRING_KEY_SECRET_VERSION")
$(cloud_run_secret_env REPLICADB_SECURITY_MASTER_KEY_JSON "$KEYRING_FILE_SECRET_NAME" "$KEYRING_FILE_SECRET_VERSION")
$(cloud_run_secret_env REPLICADB_BOOTSTRAP_ADMIN_USERNAME "$BOOTSTRAP_USERNAME_SECRET_NAME" "$BOOTSTRAP_USERNAME_SECRET_VERSION")
$(cloud_run_secret_env REPLICADB_BOOTSTRAP_ADMIN_PASSWORD "$BOOTSTRAP_PASSWORD_SECRET_NAME" "$BOOTSTRAP_PASSWORD_SECRET_VERSION")
          startupProbe:
            httpGet:
              path: /actuator/health/liveness
            initialDelaySeconds: 10
            timeoutSeconds: 5
            periodSeconds: 10
            failureThreshold: 30
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
            timeoutSeconds: 5
            periodSeconds: 15
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
            timeoutSeconds: 5
            periodSeconds: 10
            failureThreshold: 3
          volumeMounts:
            - name: replicadb-master-key
              mountPath: /run/secrets
              readOnly: true
      volumes:
        - name: replicadb-master-key
          secret:
            secretName: ${KEYRING_FILE_SECRET_NAME}
            items:
              - key: "${KEYRING_FILE_SECRET_VERSION}"
                path: replicadb-master-key
EOF
    CLOUD_RUN_SERVICE_YAML=$output_path
}

cloud_run_deploy_service() {
    local local_execution=$1
    local yaml_path
    yaml_path=$(mktemp "${TMPDIR:-/tmp}/replicadb-cloud-run-service.XXXXXX.yaml")
    cloud_run_render_service "$yaml_path" "$local_execution" || { rm -f "$yaml_path"; return 1; }
    gcloud run services replace "$yaml_path" --project="$PROJECT_ID" --region="$REGION" >/dev/null || {
      rm -f "$yaml_path"
        cloud_run_fail "could not deploy Cloud Run service: $CLOUD_RUN_SERVICE_NAME"
        return 1
    }
    API_SERVICE_URL=$(gcloud run services describe "$CLOUD_RUN_SERVICE_NAME" --project="$PROJECT_ID" \
        --region="$REGION" --format='value(status.url)') || {
      rm -f "$yaml_path"
        cloud_run_fail 'Cloud Run service was deployed but its URL could not be read'
        return 1
    }
      cloud_run_set_public_access "$CLOUD_RUN_SERVICE_NAME" "${PUBLIC_ACCESS:-false}" || {
        rm -f "$yaml_path"
        return 1
      }
    rm -f "$yaml_path"
    printf 'Cloud Run API service deployed: %s\n' "$CLOUD_RUN_SERVICE_NAME"
}
