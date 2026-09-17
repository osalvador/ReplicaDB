#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-cloud-sql-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
case "$*" in
    *'instances describe'*)
        [[ "${CLOUD_SQL_SCENARIO:-valid}" == permission ]] && exit 1
        printf '%s\n' '{"region":"europe-west4","databaseVersion":"POSTGRES_15","state":"RUNNABLE","connectionName":"test-project:europe-west4:replicadb","ipAddresses":[{"type":"PRIVATE","ipAddress":"10.0.0.4"}]}' ;;
    *'databases list'*) [[ "${CLOUD_SQL_SCENARIO:-valid}" == missing-database ]] || printf 'replicadb\n' ;;
    *'users list'*) [[ "${CLOUD_SQL_SCENARIO:-valid}" == missing-user ]] || printf 'replicadb\n' ;;
esac
EOF
cat >"$STUB_BIN/jq" <<'EOF'
#!/usr/bin/env bash
input=$(cat)
case "$*" in
    *'.region'*) printf '%s\n' "$(printf '%s' "$input" | sed -n 's/.*"region":"\([^"]*\)".*/\1/p')" ;;
    *'.databaseVersion'*) printf 'POSTGRES_15\n' ;;
    *'.state'*) printf 'RUNNABLE\n' ;;
    *'.connectionName'*) printf 'test-project:europe-west4:replicadb\n' ;;
    *'PRIVATE'*) [[ "${CLOUD_SQL_SCENARIO:-valid}" == public-only ]] || printf '10.0.0.4\n' ;;
esac
EOF
chmod 755 "$STUB_BIN/gcloud" "$STUB_BIN/jq"
export PATH="$STUB_BIN:$PATH"
export PROJECT_ID=test-project REGION=europe-west4 MODE=simple
export CLOUD_SQL_INSTANCE=replicadb DATABASE=replicadb DB_USER=replicadb
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/cloud_sql.sh"

cloud_sql_validate_existing
[[ "$CLOUD_SQL_SOURCE" == instance ]] || exit 1
[[ "$CLOUD_SQL_PRIVATE_IP" == 10.0.0.4 ]] || exit 1

for scenario in permission missing-database missing-user public-only; do
    export CLOUD_SQL_SCENARIO="$scenario"
    if cloud_sql_validate_existing >/dev/null 2>&1; then
        printf 'scenario unexpectedly passed: %s\n' "$scenario" >&2
        exit 1
    fi
done

unset CLOUD_SQL_INSTANCE CLOUD_SQL_CONNECTION
export DB_URL='postgres://user:secret@example.invalid/db'
export CLOUD_SQL_SCENARIO=valid
cloud_sql_validate_existing >/dev/null
unset DB_URL

printf 'cloud sql validation tests passed\n'
