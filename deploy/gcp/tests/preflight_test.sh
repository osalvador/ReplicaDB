#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-preflight-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
LOG_FILE="$TEMP_ROOT/commands.log"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${PREFLIGHT_LOG:?}"
case "$*" in
    *'auth list'*) [[ "${PREFLIGHT_FAIL_AUTH:-false}" == true ]] && exit 1 || printf 'operator@example.invalid\n' ;;
    *'projects describe'*) printf '%s\n' "${PREFLIGHT_PROJECT_RESULT:-$PREFLIGHT_PROJECT}" ;;
    *'services list'*) printf '%s\n' \
        run.googleapis.com artifactregistry.googleapis.com sqladmin.googleapis.com \
        secretmanager.googleapis.com compute.googleapis.com servicenetworking.googleapis.com \
        serviceusage.googleapis.com; [[ "${PREFLIGHT_MISSING_API:-false}" == true ]] || printf 'iamcredentials.googleapis.com\n' ;;
    *'run regions list'*) [[ "${PREFLIGHT_MISSING_REGION:-false}" == true ]] || printf 'europe-west4\n' ;;
    *'test-iam-permissions'*) printf '%s\n' \
        run.services.create run.services.update run.services.get iam.serviceAccounts.actAs \
        run.workerPools.create run.workerPools.update; \
        [[ "${PREFLIGHT_MISSING_PERMISSION:-false}" == true ]] || printf '%s\n' secretmanager.secrets.get sql.instances.get ;;
    *'worker-pools --help'*) [[ "${PREFLIGHT_MISSING_WORKER:-false}" == true ]] && exit 1 || exit 0 ;;
    *'networks describe'*) [[ "${PREFLIGHT_MISSING_NETWORK:-false}" == true ]] && exit 1 || printf 'network\n' ;;
    *'subnets describe'*) [[ "${PREFLIGHT_MISSING_SUBNET:-false}" == true ]] && exit 1 || printf 'subnet\n' ;;
esac
EOF
chmod 755 "$STUB_BIN/gcloud"
cat >"$STUB_BIN/docker" <<'EOF'
#!/usr/bin/env bash
exit "${PREFLIGHT_DOCKER_EXIT:-0}"
EOF
chmod 755 "$STUB_BIN/docker"

export PATH="$STUB_BIN:$PATH"
export PREFLIGHT_LOG="$LOG_FILE"
export PREFLIGHT_PROJECT=test-project
export PROJECT_ID=test-project
export REGION=europe-west4
export MODE=simple
export IMAGE=osalvador/replicadb-server:1.0.0
export IMAGE_DIGEST=
export NETWORK=network
export SUBNET=subnet
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/preflight.sh"

preflight_run
if grep -Eq '(^| )(run deploy|worker-pools deploy|sql instances create|secrets create|delete)' "$LOG_FILE"; then
    printf 'preflight invoked a mutating-looking gcloud command\n' >&2
    cat "$LOG_FILE" >&2
    exit 1
fi

export PREFLIGHT_DOCKER_EXIT=1
if preflight_run >/dev/null 2>&1; then
    printf 'blocked registry image unexpectedly passed\n' >&2
    exit 1
fi

MODE=distributed
export PREFLIGHT_DOCKER_EXIT=0
export PREFLIGHT_MISSING_WORKER=true
if preflight_run >/dev/null 2>&1; then
    printf 'distributed worker capability unexpectedly passed\n' >&2
    exit 1
fi

export PREFLIGHT_MISSING_WORKER=false
export PREFLIGHT_FAIL_AUTH=true
if preflight_run >/dev/null 2>&1; then exit 1; fi
export PREFLIGHT_FAIL_AUTH=false
export PREFLIGHT_PROJECT_RESULT=wrong-project
if preflight_run >/dev/null 2>&1; then exit 1; fi
export PREFLIGHT_PROJECT_RESULT="$PREFLIGHT_PROJECT"
export PREFLIGHT_MISSING_API=true
if preflight_run >/dev/null 2>&1; then exit 1; fi
export PREFLIGHT_MISSING_API=false
export PREFLIGHT_MISSING_REGION=true
if preflight_run >/dev/null 2>&1; then exit 1; fi
export PREFLIGHT_MISSING_REGION=false
export PREFLIGHT_MISSING_PERMISSION=true
if preflight_run >/dev/null 2>&1; then exit 1; fi
export PREFLIGHT_MISSING_PERMISSION=false
export PREFLIGHT_MISSING_SUBNET=true
if preflight_run >/dev/null 2>&1; then exit 1; fi

printf 'preflight tests passed\n'