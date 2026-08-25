#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
base_project=${COMPOSE_PROJECT_NAME:-replicadb-phase3-chaos-$PPID}

COMPOSE_PROJECT_NAME="${base_project}-worker-loss" "$script_dir/phase3-worker-loss-test.sh" all
COMPOSE_PROJECT_NAME="${base_project}-resilience" "$script_dir/phase3-resilience-test.sh" all

printf 'phase3 chaos validation passed\n'
