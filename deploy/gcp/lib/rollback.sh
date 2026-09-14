#!/usr/bin/env bash

set -euo pipefail

ROLLBACK_KINDS=()
ROLLBACK_VALUES=()

rollback_reset() {
    ROLLBACK_KINDS=()
    ROLLBACK_VALUES=()
}

rollback_register() {
    ROLLBACK_KINDS+=("$1")
    ROLLBACK_VALUES+=("$2")
}

rollback_run() {
    local index kind value
    for ((index=${#ROLLBACK_KINDS[@]} - 1; index >= 0; index--)); do
        kind=${ROLLBACK_KINDS[$index]}
        value=${ROLLBACK_VALUES[$index]}
        case "$kind" in
            database) gcloud sql databases delete "$value" --project="$PROJECT_ID" --instance="$CLOUD_SQL_INSTANCE" --quiet >/dev/null 2>&1 || true ;;
            user) gcloud sql users delete "$value" --project="$PROJECT_ID" --instance="$CLOUD_SQL_INSTANCE" --quiet >/dev/null 2>&1 || true ;;
            instance) gcloud sql instances delete "$value" --project="$PROJECT_ID" --quiet >/dev/null 2>&1 || true ;;
        esac
    done
    rollback_reset
}