#!/usr/bin/env bash

set -euo pipefail

naming_normalize() {
    local value=$1
    value=$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g; s/--*/-/g; s/^-*//; s/-*$//')
    printf '%s' "${value:-replicadb}"
}

naming_hash() {
    printf '%s' "$1" | shasum -a 256 | cut -c1-8
}

naming_resource_name() {
    local role=$1
    local prefix=${2:-replicadb}
    local deployment_id=${3:-default}
    local normalized_prefix normalized_id normalized_role hash stem max_stem

    normalized_prefix=$(naming_normalize "$prefix")
    normalized_id=$(naming_normalize "$deployment_id")
    normalized_role=$(naming_normalize "$role")
    hash=$(naming_hash "$deployment_id")
    stem="${normalized_prefix}-${normalized_id}-${normalized_role}"
    max_stem=$((49 - ${#hash} - 1))
    if [[ ${#stem} -gt $max_stem ]]; then
        stem=${stem:0:max_stem}
        stem=${stem%-}
    fi
    printf '%s-%s' "$stem" "$hash"
}

naming_deployment_label() {
    printf 'replicadb-deployment=%s' "$(naming_normalize "$1")"
}