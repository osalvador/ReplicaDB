#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export PHASE3_EXPECT_WORKERS=2
export COMPOSE_PROFILES=multinode
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-replicadb-phase3-multinode-$PPID}

"$script_dir/phase3-compose-smoke.sh"

if [[ "${PHASE3_RUN_FAIRNESS:-false}" = true ]]; then
	"$script_dir/phase3-fairness-test.sh"
fi

printf 'phase3 multinode smoke passed with two API processes and two worker processes\n'
