#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-cloud-sql-create-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
LOG_FILE="$TEMP_ROOT/commands.log"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLOUD_SQL_CREATE_LOG:?}"
case "$*" in
    *'instances describe'*) [[ "$*" == *'--format=json'* ]] && printf '%s\n' '{"ipAddresses":[{"type":"PRIVATE","ipAddress":"10.0.0.4"}]}' || exit 1 ;;
    *'databases create'*) [[ "${CLOUD_SQL_CREATE_SCENARIO:-valid}" == database-failure ]] && exit 1 || exit 0 ;;
    *'users create'*) [[ "${CLOUD_SQL_CREATE_SCENARIO:-valid}" == user-failure ]] && exit 1 || exit 0 ;;
    *'databases delete'|*'users delete'|*'instances delete'*) exit 0 ;;
esac
exit 0
EOF
cat >"$STUB_BIN/openssl" <<'EOF'
#!/usr/bin/env bash
printf 'do-not-print-this-password\n'
EOF
cat >"$STUB_BIN/jq" <<'EOF'
#!/usr/bin/env bash
printf '10.0.0.4\n'
EOF
chmod 755 "$STUB_BIN"/*
export PATH="$STUB_BIN:$PATH"
export CLOUD_SQL_CREATE_LOG="$LOG_FILE"
export PROJECT_ID=test-project REGION=europe-west4 MODE=simple
export CLOUD_SQL_INSTANCE=new-instance DATABASE=replicadb DB_USER=replicadb NETWORK=existing-network
export REPLICADB_REQUIRE_PRIVATE_IP=true
export CONFIRMATION='CREATE CLOUD SQL'
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/cloud_sql.sh"

cloud_sql_create
[[ "$CLOUD_SQL_CREATED_INSTANCE" == true && "$CLOUD_SQL_CREATED_DATABASE" == true && "$CLOUD_SQL_CREATED_USER" == true ]] || exit 1
if grep -q 'do-not-print-this-password' "$LOG_FILE"; then
    printf 'password appeared in the command log fixture\n' >&2
    exit 1
fi
order=$(grep -En 'instances create|databases create|users create' "$LOG_FILE" | cut -d: -f1 | tr '\n' ' ')
[[ "$order" == '2 4 5 ' ]] || { printf 'unexpected create ordering: %s\n' "$order" >&2; exit 1; }

for scenario in database-failure user-failure; do
    rollback_reset
    export CLOUD_SQL_CREATE_SCENARIO="$scenario"
    if cloud_sql_create >/dev/null 2>&1; then exit 1; fi
    grep -Eq 'instances delete' "$LOG_FILE" || { printf 'rollback did not delete instance\n' >&2; exit 1; }
done

export CLOUD_SQL_CREATE_SCENARIO=valid
if (CONFIRMATION='no' cloud_sql_create >/dev/null 2>&1); then
    printf 'creation function should not infer confirmation\n' >&2
    exit 1
fi

printf 'cloud sql creation tests passed\n'
