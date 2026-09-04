#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=${1:-$(CDPATH= cd -- "$script_dir/.." && pwd)}

require_text() {
    local file=$1
    local pattern=$2
    if ! grep -Fq -- "$pattern" "$repository_root/$file"; then
        printf 'missing documentation requirement: %s -> %s\n' "$file" "$pattern" >&2
        exit 1
    fi
}

forbidden_text() {
    local file=$1
    local pattern=$2
    if grep -Fn -- "$pattern" "$repository_root/$file" >/dev/null; then
        printf 'stale documentation text: %s -> %s\n' "$file" "$pattern" >&2
        exit 1
    fi
}

require_text DEPLOYMENT.md 'V15: Quartz JDBC PostgreSQL tables'
require_text DEPLOYMENT.md 'V16: shared login-attempt reservations'
require_text DEPLOYMENT.md 'V17: managed datasource profiles'
require_text DEPLOYMENT.md 'V18: datasource-only job bindings'
require_text DEPLOYMENT.md 'V19: claim-time resolved datasource identifiers'
require_text DEPLOYMENT.md 'Datasource key management'
require_text DEPLOYMENT.md 'Base64-encoded 256-bit AES keys'
require_text DEPLOYMENT.md 'authenticated TLS'
require_text DEPLOYMENT.md 'technical_params'
require_text DEPLOYMENT.md 'resolved datasource UUIDs'
require_text DEPLOYMENT.md 'spring.datasource.hikari.maximum-pool-size >= max-concurrent-runs + 4'
require_text DEPLOYMENT.md 'REPLICADB_WORKER_ADMISSION_JITTER_MAX'
require_text DEPLOYMENT.md 'busy-slot-seconds / max-concurrent-runs'
require_text DEPLOYMENT.md 'one generic fallback'
require_text DEPLOYMENT.md 'RAM/JDBC scheduler'
require_text DEPLOYMENT.md 'Process-level worker-loss'
require_text README.md '## Choose a release'
require_text replicadb-server/frontend/README.develop.md 'topología Compose local'
require_text ARCHITECTURE_DECISIONS.md 'Phase 3.3 is complete'
require_text ARCHITECTURE_DECISIONS.md 'Phase 3.4 is complete'
require_text ARCHITECTURE_DECISIONS.md 'standalone CLI compatibility validation'
require_text ARCHITECTURE_DECISIONS.md 'Flyway migrations V1 through V20'
require_text ARCHITECTURE_DECISIONS.md 'Phase 4 is complete'

forbidden_text README.md 'It is unreleased, unauthenticated, and has no metadata persistence or scheduler yet'
forbidden_text ARCHITECTURE_DECISIONS.md 'Phase 3.4 is not started'
forbidden_text ARCHITECTURE_DECISIONS.md 'Phase 3.4 (hybrid worker load distribution) is approved but not started'
forbidden_text ARCHITECTURE_DECISIONS.md '- [ ] Preserve the CLI artifact'
forbidden_text ARCHITECTURE_DECISIONS.md 'APPROVED FOR PLANNING; NOT IMPLEMENTED'
forbidden_text ARCHITECTURE_DECISIONS.md 'Planned Phase 4'
forbidden_text replicadb-server/frontend/README.develop.md 'Quartz JDBC clustering, throttling de login compartido, métricas y pruebas de carga/caos siguen diferidos a Phase 3.3'
forbidden_text replicadb-server/frontend/README.develop.md 'mientras Phase 2b y Phase 2c sigan pendientes'

require_text replicadb-server/src/main/resources/application-api.yml 'job-store-type: jdbc'
require_text replicadb-server/src/main/resources/application-api.yml 'initialize-schema: never'
require_text replicadb-server/src/main/resources/application-worker.yml 'web-application-type: servlet'
require_text replicadb-server/src/main/resources/application-worker.yml 'port: -1'
require_text docker-compose.server.yml 'REPLICADB_SERVER_LOCAL_EXECUTION_ENABLED: "false"'
require_text docker-compose.server.yml 'replicadb-master-key'
require_text DEPLOYMENT.md 'scripts/phase4-acceptance.sh'

for script in \
    scripts/phase3-compose-smoke.sh \
    scripts/phase3-load-test.sh \
    scripts/phase3-resilience-test.sh \
    scripts/phase3-worker-loss-test.sh \
    scripts/phase3-fairness-test.sh; do
    forbidden_text "$script" 'sourceConnect'
    forbidden_text "$script" 'sinkConnect'
    forbidden_text "$script" 'sourcePassword'
    forbidden_text "$script" 'sinkPassword'
done

if grep -Ein '(^|["[:space:]])(password|token)(["[:space:]]*[:=]["[:space:]]*)[[:alnum:]]' \
        "$repository_root/DEPLOYMENT.md" "$repository_root/docker-compose.server.yml" \
        "$repository_root/scripts/phase3-compose-smoke.sh" >/dev/null; then
    printf 'possible secret-bearing deployment content found\n' >&2
    exit 1
fi

printf 'datasource documentation checks passed\n'
