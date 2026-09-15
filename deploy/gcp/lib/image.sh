#!/usr/bin/env bash

set -euo pipefail

IMAGE_MIRROR="${REPLICADB_ARTIFACT_REGISTRY_IMAGE:-}"
ALLOW_MUTABLE_IMAGE="${REPLICADB_ALLOW_LATEST:-false}"
FINAL_IMAGE=""
FINAL_DIGEST=""

image_fail() {
    printf 'Image error: %s\n' "$*" >&2
    return 1
}

image_digest_valid() {
    [[ "$1" =~ ^sha256:[a-f0-9]{64}$ ]]
}

image_is_latest() {
    [[ "$1" == *:latest || "$1" == *'@latest' ]]
}

image_manifest_digest() {
    local image=$1
    local manifest digest architectures
    command -v docker >/dev/null 2>&1 || {
        image_fail "docker is required to resolve mutable image tags; provide an explicit digest"
        return 1
    }
    command -v jq >/dev/null 2>&1 || {
        image_fail 'jq is required to parse an image manifest; provide an explicit digest'
        return 1
    }
    manifest=$(docker manifest inspect --verbose "$image" 2>/dev/null) || {
        image_fail "image cannot be inspected: $image"
        return 1
    }
    architectures=$(printf '%s\n' "$manifest" | jq -r '[.. | objects | (.platform.architecture? // .architecture?) | select(. != null)] | unique | .[]' 2>/dev/null) || {
        image_fail "image manifest is not valid JSON: $image"
        return 1
    }
    [[ -n "$architectures" && "$architectures" == *amd64* ]] || {
        image_fail "image has no linux/amd64 manifest: $image"
        return 1
    }
    digest=$(printf '%s\n' "$manifest" | jq -r '[.. | objects | select(.digest? and ((.platform.architecture? // .architecture?) == "amd64")) | .digest] | .[0] // empty' 2>/dev/null)
    image_digest_valid "$digest" || {
        image_fail "image manifest did not provide an amd64 digest: $image"
        return 1
    }
    printf '%s' "$digest"
}

image_base_reference() {
    if [[ -n "$IMAGE" ]]; then
        printf '%s' "$IMAGE"
    else
        printf 'osalvador/replicadb-server:%s' "$IMAGE_TAG"
    fi
}

image_mirror() {
    local source=$1
    [[ -n "$IMAGE_MIRROR" ]] || return 0
    command -v docker >/dev/null 2>&1 || { image_fail 'docker is required for Artifact Registry mirroring'; return 1; }
    docker pull "$source" >/dev/null 2>&1 || { image_fail 'could not pull source image for mirroring'; return 1; }
    docker tag "$source" "$IMAGE_MIRROR" >/dev/null 2>&1 || { image_fail 'could not tag image for mirroring'; return 1; }
    docker push "$IMAGE_MIRROR" >/dev/null 2>&1 || { image_fail 'could not push image to Artifact Registry'; return 1; }
    printf '%s' "$IMAGE_MIRROR"
}

image_resolve() {
    local base digest resolved
    base=$(image_base_reference)
    if image_is_latest "$base" && [[ "$ALLOW_MUTABLE_IMAGE" != true ]]; then
        image_fail 'latest is not allowed without REPLICADB_ALLOW_LATEST=true; use a release tag or digest'
        return 1
    fi
    if [[ "$base" == *@* ]]; then
        digest=${base##*@}
        image_digest_valid "$digest" || { image_fail "invalid image digest: $digest"; return 1; }
        resolved="$base"
    elif [[ -n "$IMAGE_DIGEST" ]]; then
        image_digest_valid "$IMAGE_DIGEST" || { image_fail "invalid image digest: $IMAGE_DIGEST"; return 1; }
        resolved="${base}@${IMAGE_DIGEST}"
    else
        digest=$(image_manifest_digest "$base") || return 1
        resolved="${base}@${digest}"
    fi
    if [[ -n "$IMAGE_MIRROR" ]]; then
        local mirror_digest
        image_mirror "$resolved" >/dev/null || return 1
        mirror_digest=$(image_manifest_digest "$IMAGE_MIRROR") || return 1
        resolved="${IMAGE_MIRROR%@*}@${mirror_digest}"
        digest=$mirror_digest
    else
        digest=${resolved##*@}
    fi
    FINAL_IMAGE=$resolved
    FINAL_DIGEST=$digest
    printf '%s\n' "$FINAL_IMAGE"
}
