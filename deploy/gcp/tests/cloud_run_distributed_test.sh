#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-cloud-run-distributed-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/cloud_run_service.sh"

export DEPLOYMENT_ID=distributed-test PROJECT_ID=test-project REGION=europe-west4
export FINAL_IMAGE=osalvador/replicadb-server:1.0.0@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export SERVICE_ACCOUNT=api@test-project.iam.gserviceaccount.com
export NETWORK=network SUBNET=subnet DATABASE=replicadb CLOUD_SQL_PRIVATE_IP=10.0.0.4
export DB_USERNAME_SECRET_NAME=db-user DB_USERNAME_SECRET_VERSION=3
export DB_PASSWORD_SECRET_NAME=db-password DB_PASSWORD_SECRET_VERSION=4
export KEYRING_VERSION_SECRET_NAME=keyring-version KEYRING_VERSION_SECRET_VERSION=5
export KEYRING_KEY_SECRET_NAME=keyring-key KEYRING_KEY_SECRET_VERSION=6
export BOOTSTRAP_USERNAME_SECRET_NAME=bootstrap-user BOOTSTRAP_USERNAME_SECRET_VERSION=7
export BOOTSTRAP_PASSWORD_SECRET_NAME=bootstrap-password BOOTSTRAP_PASSWORD_SECRET_VERSION=8

cloud_run_render_service "$TEMP_ROOT/service.yaml" false
rg -q 'value: "false"' "$TEMP_ROOT/service.yaml"
rg -q 'SPRING_PROFILES_ACTIVE' "$TEMP_ROOT/service.yaml"
if rg -q 'worker-pools|roles/run.invoker|allow-unauthenticated' "$TEMP_ROOT/service.yaml"; then exit 1; fi

printf 'distributed cloud run service tests passed\n'