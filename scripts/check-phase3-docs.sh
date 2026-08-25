#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=${1:-$(CDPATH= cd -- "$script_dir/.." && pwd)}

require_text() {
    local file=$1
    local pattern=$2
    if ! rg -q --fixed-strings "$pattern" "$repository_root/$file"; then
        printf 'missing documentation requirement: %s -> %s\n' "$file" "$pattern" >&2
        exit 1
    fi
}

forbidden_text() {
    local file=$1
    local pattern=$2
    if rg -n --fixed-strings "$pattern" "$repository_root/$file" >/dev/null; then
        printf 'stale documentation text: %s -> %s\n' "$file" "$pattern" >&2
        exit 1
    fi
}

require_text DEPLOYMENT.md 'V15: Quartz JDBC PostgreSQL tables'
require_text DEPLOYMENT.md 'V16: shared login-attempt reservations'
require_text DEPLOYMENT.md 'spring.datasource.hikari.maximum-pool-size >= max-concurrent-runs + 4'
require_text DEPLOYMENT.md 'RAM/JDBC scheduler'
require_text DEPLOYMENT.md 'Process-level worker-loss'
require_text README.md '## Managed server artifact'
require_text replicadb-server/frontend/README.develop.md 'topología Compose local'
require_text ARCHITECTURE_DECISIONS.md 'Phase 3.3 is in progress'
require_text ARCHITECTURE_DECISIONS.md 'Flyway migrations V1 through V16'

forbidden_text README.md 'It is unreleased, unauthenticated, and has no metadata persistence or scheduler yet'
forbidden_text replicadb-server/frontend/README.develop.md 'Quartz JDBC clustering, throttling de login compartido, métricas y pruebas de carga/caos siguen diferidos a Phase 3.3'
forbidden_text replicadb-server/frontend/README.develop.md 'mientras Phase 2b y Phase 2c sigan pendientes'

require_text replicadb-server/src/main/resources/application-api.yml 'job-store-type: jdbc'
require_text replicadb-server/src/main/resources/application-api.yml 'initialize-schema: never'
require_text replicadb-server/src/main/resources/application-worker.yml 'web-application-type: servlet'
require_text replicadb-server/src/main/resources/application-worker.yml 'port: -1'
require_text docker-compose.server.yml 'REPLICADB_SERVER_LOCAL_EXECUTION_ENABLED: "false"'

if rg -n -i '(^|["[:space:]])(password|token)(["[:space:]]*[:=]["[:space:]]*)[[:alnum:]]' \
        "$repository_root/DEPLOYMENT.md" "$repository_root/docker-compose.server.yml" \
        "$repository_root/scripts/phase3-compose-smoke.sh" >/dev/null; then
    printf 'possible secret-bearing deployment content found\n' >&2
    exit 1
fi

printf 'phase3 documentation checks passed\n'
