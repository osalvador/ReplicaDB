#!/usr/bin/env bash

set -Eeuo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
frontend_directory="$repository_root/replicadb-server/frontend"

if [[ -z "${JAVA_HOME:-}" && "$(uname)" = Darwin && -x /usr/libexec/java_home ]]; then
    JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || true)
    export JAVA_HOME
fi
if [[ -z "${JAVA_HOME:-}" || ! -x "$JAVA_HOME/bin/java" ]]; then
    printf '%s\n' 'A Java 17 JAVA_HOME is required for Phase 4 acceptance.' >&2
    exit 2
fi
export PATH="$JAVA_HOME/bin:$PATH"

for required_command in mvn npm node docker curl jq; do
    command -v "$required_command" >/dev/null 2>&1 || {
        printf 'Required command not found: %s\n' "$required_command" >&2
        exit 2
    }
done

step() {
    printf 'phase4 acceptance: %s\n' "$1"
}

step 'validate documentation, shell syntax, and datasource-only payloads'
"$repository_root/scripts/check-phase3-docs.sh"
for script in \
    "$repository_root/scripts/phase3-compose-smoke.sh" \
    "$repository_root/scripts/phase3-load-test.sh" \
    "$repository_root/scripts/phase3-resilience-test.sh" \
    "$repository_root/scripts/phase3-worker-loss-test.sh" \
    "$repository_root/scripts/phase3-fairness-test.sh" \
    "$repository_root/scripts/phase4-compose-common.sh" \
    "$frontend_directory/scripts/start-local.sh"; do
    bash -n "$script"
done
(cd "$frontend_directory" && node --test scripts/seed-local-jobs.test.mjs)

step 'install the standalone CLI artifact'
mvn -B -f "$repository_root/pom.xml" install -DskipTests

step 'run the complete managed server suite'
mvn -B -f "$repository_root/replicadb-server/pom.xml" test

step 'package the server artifact and inspect the image'
mvn -B -f "$repository_root/replicadb-server/pom.xml" package -DskipTests
"$repository_root/scripts/phase3-image-smoke.sh"

step 'run the standalone CLI compatibility gate'
"$repository_root/scripts/phase3-cli-compatibility.sh"

step 'run the complete frontend unit and build gate'
(cd "$frontend_directory" && npm ci && npm test -- --run && npm run build)

step 'run the distributed Compose smoke with two APIs and two workers'
env \
    COMPOSE_PROJECT_NAME="phase4-smoke-$PPID" \
    COMPOSE_PROFILES=multinode \
    PHASE3_EXPECT_WORKERS=2 \
    "$repository_root/scripts/phase3-compose-smoke.sh"

step 'run distributed load validation'
env \
    COMPOSE_PROJECT_NAME="phase4-load-$PPID" \
    PHASE3_LOAD_RUNS="${PHASE4_LOAD_RUNS:-8}" \
    "$repository_root/scripts/phase3-load-test.sh"

step 'run notification, restart, and duplicate-polling resilience validation'
env \
    COMPOSE_PROJECT_NAME="phase4-resilience-$PPID" \
    "$repository_root/scripts/phase3-resilience-test.sh" all

step 'run worker-loss copy and merge validation'
env \
    COMPOSE_PROJECT_NAME="phase4-worker-loss-$PPID" \
    REPLICADB_WORKER_LEASE_DURATION=5s \
    REPLICADB_WORKER_HEARTBEAT_INTERVAL=1s \
    REPLICADB_WORKER_POLL_INTERVAL=1s \
    "$repository_root/scripts/phase3-worker-loss-test.sh" all

step 'run mixed-capacity fairness validation'
env \
    COMPOSE_PROJECT_NAME="phase4-fairness-$PPID" \
    PHASE3_FAIRNESS_RUNS="${PHASE4_FAIRNESS_RUNS:-24}" \
    "$repository_root/scripts/phase3-fairness-test.sh"

step 'run authenticated browser acceptance with local datasource fixtures'
"$frontend_directory/scripts/test-admin-e2e-local.sh"

step 'check the final patch'
git -C "$repository_root" diff --check
printf '%s\n' 'phase4 acceptance passed'
