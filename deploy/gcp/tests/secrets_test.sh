#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-secrets-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
LOG_FILE="$TEMP_ROOT/commands.log"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${SECRETS_LOG:?}"
case "$*" in
    *'secrets describe'*) [[ "${SECRETS_SCENARIO:-create}" == reuse ]] && exit 0 || exit 1 ;;
    *'versions list'*) printf 'projects/test-project/secrets/existing/versions/7\n' ;;
    *'secrets create'*) [[ "${SECRETS_SCENARIO:-create}" == create-failure ]] && exit 1 || exit 0 ;;
    *'versions add'*) [[ "${SECRETS_SCENARIO:-create}" == version-failure ]] && exit 1 || printf 'projects/test-project/secrets/new/versions/1\n' ;;
    *'add-iam-policy-binding'*) [[ "${SECRETS_SCENARIO:-create}" == access-failure ]] && exit 1 || exit 0 ;;
    *'secrets delete'*) exit 0 ;;
esac
exit 0
EOF
cat >"$STUB_BIN/openssl" <<'EOF'
#!/usr/bin/env bash
printf 'secret-payload-must-not-leak\n'
EOF
chmod 755 "$STUB_BIN"/*
export PATH="$STUB_BIN:$PATH"
export SECRETS_LOG="$LOG_FILE"
export PROJECT_ID=test-project MODE=distributed DB_USER=replicadb
export SERVICE_ACCOUNT=api@test-project.iam.gserviceaccount.com
export WORKER_SERVICE_ACCOUNT=worker@test-project.iam.gserviceaccount.com
export CLOUD_SQL_GENERATED_PASSWORD=database-password-must-not-leak
unset REPLICADB_DB_USERNAME_SECRET_VERSION REPLICADB_DB_PASSWORD_SECRET_VERSION \
    REPLICADB_BOOTSTRAP_USERNAME_SECRET_VERSION REPLICADB_BOOTSTRAP_PASSWORD_SECRET_VERSION \
    REPLICADB_KEYRING_VERSION_SECRET_VERSION REPLICADB_KEYRING_KEY_SECRET_VERSION
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/secrets.sh"

SECRETS_SCENARIO=reuse
export SECRETS_SCENARIO
secrets_prepare >/dev/null
[[ "$DB_PASSWORD_SECRET_VERSION" == 7 ]] || exit 1
[[ "$KEYRING_KEY_SECRET_VERSION" == 7 ]] || exit 1

SECRETS_SCENARIO=create
export SECRETS_SCENARIO
SECRET_CREATED_NAMES=()
secrets_prepare >/dev/null
[[ ${#SECRET_CREATED_NAMES[@]} -eq 7 ]] || exit 1
if grep -Eq 'database-password-must-not-leak|secret-payload-must-not-leak' "$LOG_FILE"; then
    printf 'secret payload appeared in command diagnostics\n' >&2
    exit 1
fi

for scenario in version-failure access-failure; do
    export SECRETS_SCENARIO="$scenario"
    SECRET_CREATED_NAMES=()
    if secrets_prepare >/dev/null 2>&1; then exit 1; fi
done

printf 'secret manager tests passed\n'
