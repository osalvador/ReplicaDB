#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/../../.." && pwd)"
TEMP_HOME="$(mktemp -d "${TMPDIR:-/tmp}/replicadb-gcp-tests.XXXXXX")"
STUB_BIN="$TEMP_HOME/bin"
LOG_FILE="$TEMP_HOME/commands.log"

cleanup() {
    rm -rf "$TEMP_HOME"
}
trap cleanup EXIT

mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/gcloud" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${REPLICADB_TEST_COMMAND_LOG:?}"
case "$*" in
    *'auth list'*) printf 'operator@example.invalid\n' ;;
    *'projects describe'*) printf 'test-project\n' ;;
    *'services list'*) printf '%s\n' \
        run.googleapis.com artifactregistry.googleapis.com sqladmin.googleapis.com \
        secretmanager.googleapis.com compute.googleapis.com servicenetworking.googleapis.com \
        serviceusage.googleapis.com iamcredentials.googleapis.com ;;
    *'run regions list'*) printf 'europe-west4\n' ;;
    *'test-iam-permissions'*) printf '%s\n' \
        run.services.create run.services.update run.services.get iam.serviceAccounts.actAs \
        secretmanager.secrets.get sql.instances.get ;;
    *'sql instances describe'*) if [[ "${REPLICADB_TEST_CREATE:-false}" == true && "$*" != *'--format=json'* ]]; then exit 1; else printf '%s\n' '{"region":"europe-west4","databaseVersion":"POSTGRES_15","state":"RUNNABLE","connectionName":"test-project:europe-west4:existing","ipAddresses":[{"type":"PRIVATE","ipAddress":"10.0.0.4"}]}' ; fi ;;
    *'sql databases list'*) printf 'replicadb\n' ;;
    *'sql users list'*) printf 'replicadb\n' ;;
    *'secrets describe'*) [[ "${REPLICADB_TEST_CREATE:-false}" == true ]] && exit 1; [[ "${REPLICADB_TEST_REUSE_SECRETS:-false}" == true ]] && exit 0 || exit 1 ;;
    *'secrets versions list'*) printf 'projects/test-project/secrets/existing/versions/1\n' ;;
    *'secrets create'*) exit 0 ;;
    *'secrets versions add'*) printf 'projects/test-project/secrets/new/versions/1\n' ;;
    *'add-iam-policy-binding'*) exit 0 ;;
    *'run services describe'*) printf 'https://replicadb-api.example.invalid\n' ;;
esac
exit 0
EOF
chmod 755 "$STUB_BIN/gcloud"
cat >"$STUB_BIN/docker" <<'EOF'
#!/usr/bin/env bash
case "$*" in
    *'manifest inspect'*) printf '{"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","architecture":"amd64","os":"linux"}' ;;
    *) exit 0 ;;
esac
EOF
chmod 755 "$STUB_BIN/docker"
cat >"$STUB_BIN/jq" <<'EOF'
#!/usr/bin/env bash
input=$(cat)
case "$*" in
    *'.region'*) printf 'europe-west4\n' ;;
    *'.databaseVersion'*) printf 'POSTGRES_15\n' ;;
    *'.state'*) printf 'RUNNABLE\n' ;;
    *'.connectionName'*) printf 'test-project:europe-west4:existing\n' ;;
    *'PRIVATE'*) printf '10.0.0.4\n' ;;
    *)
        if printf '%s\n' "$input" | rg -q 'architecture.*amd64'; then
            case "$*" in
                *'unique'*) printf 'amd64\n' ;;
                *) printf 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' ;;
            esac
        else
            printf '\n'
        fi
        ;;
esac
EOF
chmod 755 "$STUB_BIN/jq"

export HOME="$TEMP_HOME/home"
export PATH="$STUB_BIN:$PATH"
export REPLICADB_TEST_COMMAND_LOG="$LOG_FILE"
export REPLICADB_GCP_PROJECT=test-project
export REPLICADB_STATE_FILE="$TEMP_HOME/state"
export REPLICADB_DEPLOYMENT_ID=test-deployment
export REPLICADB_TEST_REUSE_SECRETS=true
export REPLICADB_API_SERVICE_ACCOUNT=api@test-project.iam.gserviceaccount.com
export REPLICADB_DB_USERNAME_SECRET_VERSION=1
export REPLICADB_DB_PASSWORD_SECRET_VERSION=1
export REPLICADB_BOOTSTRAP_USERNAME_SECRET_VERSION=1
export REPLICADB_BOOTSTRAP_PASSWORD_SECRET_VERSION=1
export REPLICADB_KEYRING_VERSION_SECRET_VERSION=1
export REPLICADB_KEYRING_KEY_SECRET_VERSION=1

assert_contains() {
    local needle=$1
    local haystack=$2
    [[ "$haystack" == *"$needle"* ]] || {
        printf 'test failed: expected %s in output\n%s\n' "$needle" "$haystack" >&2
        exit 1
    }
}

assert_failure() {
    if "$@" >/dev/null 2>&1; then
        printf 'test failed: command unexpectedly succeeded: %s\n' "$*" >&2
        exit 1
    fi
}

DEPLOY="$ROOT_DIR/deploy/gcp/deploy.sh"

help_output="$($DEPLOY help)"
assert_contains 'Usage: deploy.sh' "$help_output"
assert_contains 'preflight' "$help_output"

assert_failure "$DEPLOY" unknown
assert_failure env -u REPLICADB_GCP_PROJECT "$DEPLOY" deploy --cloud-sql-instance existing
assert_failure env PATH=/usr/bin:/bin REPLICADB_GCP_PROJECT=test-project "$DEPLOY" deploy --cloud-sql-instance existing
assert_failure "$DEPLOY" deploy --mode invalid
assert_failure "$DEPLOY" deploy --api-min-instances nope
assert_failure "$DEPLOY" deploy --worker-instances 0 --mode distributed
assert_failure "$DEPLOY" deploy --image 'not-an-image'

summary_output="$($DEPLOY deploy --mode simple --cloud-sql-instance existing --db-url 'postgres://user:secret@example.invalid/db')"
assert_contains 'database credentials: configured' "$summary_output"
if [[ "$summary_output" == *secret* || "$summary_output" == *example.invalid* ]]; then
    printf 'test failed: database URL leaked in summary\n' >&2
    exit 1
fi

assert_failure "$DEPLOY" deploy --mode simple --create-cloud-sql --non-interactive
if rg -q '(^| )(run deploy|worker-pools deploy|sql instances create|sql databases create|sql users create|secrets create|delete)' "$LOG_FILE"; then
    printf 'test failed: refused Cloud SQL creation was mutating\n' >&2
    cat "$LOG_FILE" >&2
    exit 1
fi
export REPLICADB_TEST_CREATE=true
confirmation_output="$($DEPLOY deploy --mode simple --create-cloud-sql --non-interactive \
    --confirmation 'CREATE CLOUD SQL' --cloud-sql-instance new-instance --network existing-network)"
unset REPLICADB_TEST_CREATE
assert_contains 'Cloud SQL creation confirmed.' "$confirmation_output"

for test_file in "$TEST_DIR"/*_test.sh; do
    "$test_file"
done

printf 'task 1.1 deployment contract tests passed\n'