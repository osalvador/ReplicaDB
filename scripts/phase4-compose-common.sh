#!/usr/bin/env bash

phase4_require_commands() {
    local required_command
    for required_command in curl jq openssl; do
        command -v "$required_command" >/dev/null 2>&1 || {
            printf 'Required command not found: %s\n' "$required_command" >&2
            exit 2
        }
    done
}

phase4_prepare_keyring() {
    if [[ -z "${REPLICADB_SECURITY_MASTER_KEY_FILE:-}" ]]; then
        REPLICADB_SECURITY_MASTER_KEY_FILE="$state_directory/replicadb-master-key.json"
        local key_material
        key_material=$(openssl rand -base64 32)
        printf '{"currentVersion":"local","keys":{"local":"%s"}}\n' "$key_material" \
            >"$REPLICADB_SECURITY_MASTER_KEY_FILE"
        chmod 644 "$REPLICADB_SECURITY_MASTER_KEY_FILE"
        export REPLICADB_SECURITY_MASTER_KEY_FILE
    fi
}

phase4_create_postgres_datasources() {
    local api_url=$1
    local name_prefix=$2
    local datasource_payload

    datasource_payload=$(jq -n \
        --arg name "$name_prefix source datasource" \
        --arg database "$POSTGRES_DB" \
        --arg username "$POSTGRES_USER" \
        --arg password "$POSTGRES_PASSWORD" \
        '{name: $name, connectorType: "postgres", technicalParams: {},
          security: {connect: ("jdbc:postgresql://postgres:5432/" + $database), user: $username, password: $password},
          clearSecurityKeys: []}')
    phase4_source_datasource_id=$(printf '%s' "$datasource_payload" \
        | curl -fsS -b "$cookie_file" \
            -H 'Content-Type: application/json' \
            -H "X-XSRF-TOKEN: $csrf_token" \
            --data-binary @- "$api_url/api/v1/datasources" \
        | jq -er '.id')

    datasource_payload=$(jq -n \
        --arg name "$name_prefix sink datasource" \
        --arg database "$POSTGRES_DB" \
        --arg username "$POSTGRES_USER" \
        --arg password "$POSTGRES_PASSWORD" \
        '{name: $name, connectorType: "postgres", technicalParams: {},
          security: {connect: ("jdbc:postgresql://postgres:5432/" + $database), user: $username, password: $password},
          clearSecurityKeys: []}')
    phase4_sink_datasource_id=$(printf '%s' "$datasource_payload" \
        | curl -fsS -b "$cookie_file" \
            -H 'Content-Type: application/json' \
            -H "X-XSRF-TOKEN: $csrf_token" \
            --data-binary @- "$api_url/api/v1/datasources" \
        | jq -er '.id')
}
