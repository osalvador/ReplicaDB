#!/usr/bin/env bash

set -euo pipefail

STATE_SCHEMA_VERSION=1
STATE_FILE_PATH=""
STATE_DATA=""

state_init() {
    STATE_FILE_PATH=$1
    STATE_DATA="schemaVersion=${STATE_SCHEMA_VERSION}"
}

state_key_allowed() {
    case "$1" in
        schemaVersion|deploymentId|projectId|region|mode|image|imageDigest|apiServiceName|workerPoolName|cloudSqlInstance|cloudSqlConnection|databaseName|dbUser|network|subnet|apiServiceAccount|workerServiceAccount|publicAccess|apiMinInstances|apiMaxInstances|workerInstances|cloudSqlOwned|secretsOwned|createdAt|updatedAt|*SecretName|*SecretVersion)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

state_value_safe() {
    local value=$1
    [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || return 1
    [[ "$value" != *'postgres://'* && "$value" != *'postgresql://'* && "$value" != *'jdbc:'* ]] || return 1
    [[ "$value" != *'password='* && "$value" != *'passwd='* && "$value" != *'-----BEGIN '* ]] || return 1
}

state_put() {
    local key=$1
    local value=${2-}
    state_key_allowed "$key" || { printf 'Error: state key is not allowed: %s\n' "$key" >&2; return 1; }
    state_value_safe "$value" || { printf 'Error: state value is secret-shaped or malformed for key: %s\n' "$key" >&2; return 1; }
    [[ "$value" != *'='* ]] || { printf 'Error: state values may not contain equals: %s\n' "$key" >&2; return 1; }
    STATE_DATA=$(printf '%s\n' "$STATE_DATA" | awk -F= -v key="$key" -v value="$value" '
        BEGIN { replaced = 0 }
        $1 == key { if (!replaced) { print key "=" value; replaced = 1 }; next }
        NF > 0 { print }
        END { if (!replaced) print key "=" value }
    ')
}

state_get() {
    local key=$1
    printf '%s\n' "$STATE_DATA" | awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }'
}

state_validate_file() {
    local path=$1
    local line key value
    [[ -f "$path" ]] || { printf 'Error: state file does not exist: %s\n' "$path" >&2; return 1; }
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ -n "$line" ]] || continue
        [[ "$line" == *=* ]] || { printf 'Error: malformed state line\n' >&2; return 1; }
        key=${line%%=*}
        value=${line#*=}
        state_key_allowed "$key" || { printf 'Error: state key is not allowed: %s\n' "$key" >&2; return 1; }
        state_value_safe "$value" || { printf 'Error: unsafe state value for key: %s\n' "$key" >&2; return 1; }
        [[ "$value" != *'='* ]] || { printf 'Error: state values may not contain equals: %s\n' "$key" >&2; return 1; }
    done <"$path"
    [[ "$(awk -F= '$1 == "schemaVersion" { print $2; exit }' "$path")" == "$STATE_SCHEMA_VERSION" ]] || {
        printf 'Error: unsupported state schema\n' >&2
        return 1
    }
}

state_save() {
    local directory temp
    directory=$(dirname "$STATE_FILE_PATH")
    mkdir -p "$directory"
    chmod 700 "$directory"
    state_validate_file <(printf '%s\n' "$STATE_DATA") 2>/dev/null || true
    temp=$(mktemp "${STATE_FILE_PATH}.tmp.XXXXXX")
    printf '%s\n' "$STATE_DATA" >"$temp"
    state_validate_file "$temp"
    chmod 600 "$temp"
    mv -f "$temp" "$STATE_FILE_PATH"
}

state_load() {
    STATE_FILE_PATH=$1
    state_validate_file "$STATE_FILE_PATH"
    STATE_DATA=$(cat "$STATE_FILE_PATH")
}
