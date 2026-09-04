#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/local-api-processes.sh"

replicadb_process_list() {
    printf '%s\n' \
        '101 org.replicadb.server.ReplicaDbServerApplication --spring.profiles.active=api --server.port=8080' \
        '102 org.replicadb.server.ReplicaDbServerApplication --spring.profiles.active=worker --server.port=9091' \
        '103 org.replicadb.server.ReplicaDbServerApplication --spring.profiles.active=api --server.port=8081' \
            '104 org.example.OtherApplication --spring.profiles.active=api --server.port=8082' \
            '105 mvn -B -f /workspace/replicadb-server/pom.xml spring-boot:run -Dspring-boot.run.profiles=api --server.port=8080' \
            '106 npm run dev --host 127.0.0.1 --port 5173' \
            '107 node /workspace/replicadb-server/frontend/node_modules/.bin/vite --host 127.0.0.1 --port 5173'
}

replicadb_process_cwd() {
    case "$1" in
        101|102|103) printf '%s\n' '/workspace/replicadb-server' ;;
        104) printf '%s\n' '/workspace/other' ;;
            105) printf '%s\n' '/workspace' ;;
            106|107) printf '%s\n' '/workspace/replicadb-server/frontend' ;;
    esac
}

replicadb_process_start() {
    case "$1" in
        101|105|106|107) printf '%s\n' 'Tue Sep 3 10:00:00 2026' ;;
        103) printf '%s\n' 'Tue Sep 3 11:00:00 2026' ;;
    esac
}

processes="$(local_replica_api_processes '/workspace/replicadb-server')"
expected=$'101\t8080\tTue Sep 3 10:00:00 2026\n103\t8081\tTue Sep 3 11:00:00 2026'
[[ "$processes" == "$expected" ]]

managed_processes="$(local_replica_managed_processes '/workspace/replicadb-server' '/workspace/replicadb-server/frontend')"
managed_expected=$'api\t101\t8080\tTue Sep 3 10:00:00 2026\napi\t103\t8081\tTue Sep 3 11:00:00 2026\napi-launcher\t105\t8080\tTue Sep 3 10:00:00 2026\nfrontend\t106\t5173\tTue Sep 3 10:00:00 2026\nfrontend\t107\t5173\tTue Sep 3 10:00:00 2026'
[[ "$managed_processes" == "$managed_expected" ]]

if replicadb_handle_existing_processes "$processes" >/dev/null 2>&1; then
    printf '%s\n' 'Expected non-interactive startup to abort.' >&2
    exit 1
fi

killed=()
kill() {
    if [[ "$1" == '-0' ]]; then
        return 1
    fi
    killed+=("$1")
}

sleep() {
    :
}

replicadb_stop_processes "$processes"
[[ "${killed[*]}" == '101 103' ]]

replicadb_process_children() {
    case "$1" in
        200) printf '%s\n' 201 202 ;;
        201) printf '%s\n' 203 ;;
    esac
}

killed=()
kill() {
    case "$1" in
        -0) return 1 ;;
        -TERM|-KILL) killed+=("$2") ;;
    esac
}

replicadb_stop_process_tree 200
[[ "${killed[*]}" == '203 201 202 200' ]]

printf '%s\n' 'local-api-processes tests passed.'
