#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/naming.sh"

simple_name=$(naming_resource_name api demo release-one)
repeat_name=$(naming_resource_name api demo release-one)
other_name=$(naming_resource_name api demo release-two)
long_name=$(naming_resource_name worker-pool 'A very long prefix with spaces' \
    'This deployment identifier is deliberately longer than Cloud Run allows')

[[ "$simple_name" == "$repeat_name" ]] || { printf 'name was not stable\n' >&2; exit 1; }
[[ "$simple_name" != "$other_name" ]] || { printf 'deployment IDs collided\n' >&2; exit 1; }
[[ ${#long_name} -le 49 ]] || { printf 'name exceeds Cloud Run length budget\n' >&2; exit 1; }
[[ "$long_name" =~ ^[a-z0-9-]+$ ]] || { printf 'name contains invalid characters\n' >&2; exit 1; }
[[ "$(naming_deployment_label 'Release One')" == 'replicadb-deployment=release-one' ]] || {
    printf 'deployment label was not normalized\n' >&2
    exit 1
}

printf 'naming tests passed\n'
