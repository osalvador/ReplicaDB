#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-state-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/state.sh"

state_init "$TEMP_ROOT/deployment.state"
state_put deploymentId deployment-one
state_put mode distributed
state_put image osalvador/replicadb-server@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
state_put dbPasswordSecretName replicadb-password
state_put dbPasswordSecretVersion 3
state_save

[[ -f "$TEMP_ROOT/deployment.state" ]] || { printf 'state file missing\n' >&2; exit 1; }
[[ "$(state_get mode)" == distributed ]] || { printf 'state round trip failed\n' >&2; exit 1; }
[[ "$(state_get dbPasswordSecretVersion)" == 3 ]] || { printf 'secret version round trip failed\n' >&2; exit 1; }
[[ "$(stat -f '%Lp' "$TEMP_ROOT/deployment.state")" == 600 ]] || { printf 'state permissions are too broad\n' >&2; exit 1; }

state_put mode simple
state_save
[[ "$(state_load "$TEMP_ROOT/deployment.state"; state_get mode)" == simple ]] || {
    printf 'atomic replacement failed\n' >&2
    exit 1
}

if state_put password hunter2 >/dev/null 2>&1; then
    printf 'secret-shaped key was accepted\n' >&2
    exit 1
fi
if state_put dbUrl 'postgres://user:password@example.invalid/db' >/dev/null 2>&1; then
    printf 'database URL was accepted\n' >&2
    exit 1
fi
printf 'secret-shaped rejection passed\n' >"$TEMP_ROOT/rejection.marker"

printf 'state tests passed\n'