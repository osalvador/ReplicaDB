#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-verify-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
case "$*" in
    *'print-identity-token'*) [[ "${VERIFY_SCENARIO:-healthy}" == token-failure ]] && exit 1 || printf 'identity-token-must-not-print\n' ;;
    *'services describe'*) printf 'https://api.example.invalid\n' ;;
    *'worker-pools describe'*) printf '%s\n' "${VERIFY_WORKER_STATUS:-READY}" ;;
    *'logging read'*) [[ "${VERIFY_WORKER_LOGS:-present}" == present ]] && printf 'worker started\n' ;;
    *'secrets versions access'*) printf 'bootstrap-value\n' ;;
esac
EOF
cat >"$STUB_BIN/curl" <<'EOF'
#!/usr/bin/env bash
case "$*" in
    *liveness*) printf '{"status":"%s"}\n' "${VERIFY_LIVENESS_STATUS:-UP}" ;;
    *readiness*) printf '{"status":"%s","components":{"db":{"status":"%s"},"quartz":{"status":"%s"}}}\n' \
        "${VERIFY_READINESS_STATUS:-UP}" "${VERIFY_DB_STATUS:-UP}" "${VERIFY_QUARTZ_STATUS:-UP}" ;;
    *auth/login*) printf '{"username":"admin"}\n' ;;
esac
EOF
cat >"$STUB_BIN/jq" <<'EOF'
#!/usr/bin/env bash
input=$(cat)
if [[ "$*" == *'-n'* ]]; then printf '{"username":"bootstrap","password":"redacted"}\n'; exit 0; fi
[[ "$input" == *'"status":"UP"'* && "$input" != *'"status":"DOWN"'* && "$input" != *'"status":"DEGRADED"'* ]]
EOF
chmod 755 "$STUB_BIN"/*
export PATH="$STUB_BIN:$PATH"
export PROJECT_ID=test-project REGION=europe-west4 MODE=simple DEPLOYMENT_ID=verify-test
export API_SERVICE_URL=https://api.example.invalid VERIFY_PUBLIC_ACCESS=false
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/verify.sh"

VERIFY_SCENARIO=healthy
verify_deployment
if rg -q 'identity-token-must-not-print' <<<"$(verify_deployment)"; then exit 1; fi

for scenario in token-failure degraded; do
    export VERIFY_SCENARIO="$scenario"
    if [[ "$scenario" == degraded ]]; then export VERIFY_READINESS_STATUS=DEGRADED; fi
    if verify_deployment >/dev/null 2>&1; then exit 1; fi
done

unset VERIFY_READINESS_STATUS
export VERIFY_QUARTZ_STATUS=DOWN
if verify_deployment >/dev/null 2>&1; then exit 1; fi
unset VERIFY_QUARTZ_STATUS
export VERIFY_DB_STATUS=DOWN
if verify_deployment >/dev/null 2>&1; then exit 1; fi
unset VERIFY_DB_STATUS

MODE=distributed
WORKER_POOL_NAME=worker-pool
VERIFY_SCENARIO=healthy
unset VERIFY_READINESS_STATUS
verify_deployment
export VERIFY_WORKER_STATUS=NOT_READY
if verify_deployment >/dev/null 2>&1; then exit 1; fi
export VERIFY_WORKER_STATUS=READY VERIFY_WORKER_LOGS=empty
if verify_deployment >/dev/null 2>&1; then exit 1; fi

printf 'verify tests passed\n'