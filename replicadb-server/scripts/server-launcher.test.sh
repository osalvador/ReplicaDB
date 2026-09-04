#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCHER="${SCRIPT_DIR}/../bin/replicadb-server"
TEST_HOME="$(mktemp -d "${TMPDIR:-/tmp}/replicadb-launcher.XXXXXX")"
FAKE_JAR="${TEST_HOME}/replicadb-server-0.19.0.jar"
FAKE_JAVA="${TEST_HOME}/fake-java"
FAKE_CURL="${TEST_HOME}/fake-curl"
FAKE_ARGS="${TEST_HOME}/java.args"
touch "$FAKE_JAR"
cat > "$FAKE_JAVA" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == -version ]]; then
	printf 'openjdk version "17.0.0"\n' >&2
	exit 0
fi
printf '%s\n' "$*" > "$FAKE_ARGS"
exec -a "$FAKE_JAR" sleep 60
EOF
cat > "$FAKE_CURL" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$FAKE_JAVA" "$FAKE_CURL"
export FAKE_ARGS FAKE_JAR
trap 'rm -rf "$TEST_HOME"' EXIT

chmod +x "$LAUNCHER"

[[ "$($LAUNCHER help)" == *"start local|api|worker"* ]]
set +e
REPLICADB_SERVER_HOME="$TEST_HOME" "$LAUNCHER" start invalid >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 2 ]]

mkdir -p "$TEST_HOME/run" "$TEST_HOME/logs"
printf '1234567890' > "$TEST_HOME/logs/server.log"
set +e
REPLICADB_SERVER_HOME="$TEST_HOME" \
REPLICADB_SERVER_VERSION=0.19.0 \
REPLICADB_SERVER_JAR="$FAKE_JAR" \
JAVA_BIN="$FAKE_JAVA" \
CURL_BIN="$FAKE_CURL" \
DB_URL=jdbc:postgresql://localhost/metadata \
DB_USERNAME=metadata-user \
DB_PASSWORD=hidden-value \
"$LAUNCHER" start api >/tmp/replicadb-launcher-test.log 2>&1
status=$?
set -e
[[ $status -eq 0 ]]
[[ "$(cat "$TEST_HOME/run/server.mode")" == api ]]
[[ -f "$TEST_HOME/logs/server.log.1" ]]
rg -q -- '--spring.profiles.active=api' "$FAKE_ARGS"
[[ "$(REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" "$LAUNCHER" status)" == *"running (mode=api"* ]]
! rg -q 'hidden-value|metadata-user|jdbc:postgresql' /tmp/replicadb-launcher-test.log

set +e
REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" \
JAVA_BIN="$FAKE_JAVA" CURL_BIN="$FAKE_CURL" DB_URL=metadata DB_USERNAME=user DB_PASSWORD=password \
"$LAUNCHER" start api >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 1 ]]

set +e
REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" \
"$LAUNCHER" start worker >/tmp/replicadb-launcher-worker.log 2>&1
status=$?
set -e
[[ $status -eq 1 ]]
! rg -q 'metadata|password|DB_URL' /tmp/replicadb-launcher-worker.log

REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" "$LAUNCHER" stop >/dev/null
REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" \
JAVA_BIN="$FAKE_JAVA" CURL_BIN="$FAKE_CURL" REPLICADB_BOOTSTRAP_ADMIN_USERNAME=admin \
REPLICADB_BOOTSTRAP_ADMIN_PASSWORD=hidden-value "$LAUNCHER" start local >/dev/null
rg -q -- '--replicadb.embedded-postgres.enabled=true' "$FAKE_ARGS"
! rg -q 'hidden-value' "$FAKE_ARGS"

REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" "$LAUNCHER" stop >/dev/null
set +e
REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" "$LAUNCHER" status >/dev/null
status=$?
set -e
[[ $status -eq 3 ]]

printf '%s\n' "$$" > "$TEST_HOME/run/server.pid"
printf '%s\n' api > "$TEST_HOME/run/server.mode"
set +e
REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" "$LAUNCHER" status >/dev/null
status=$?
set -e
[[ $status -eq 3 ]]

set +e
REPLICADB_SERVER_HOME="$TEST_HOME" REPLICADB_SERVER_VERSION=0.19.0 REPLICADB_SERVER_JAR="$FAKE_JAR" \
JAVA_BIN="$FAKE_JAVA" CURL_BIN=false REPLICADB_READINESS_TIMEOUT=1 DB_URL=metadata DB_USERNAME=user DB_PASSWORD=password \
"$LAUNCHER" start api >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 1 ]]
[[ ! -f "$TEST_HOME/run/server.pid" ]]

echo "server launcher contract checks passed"
