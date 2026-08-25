#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
server_directory="$repository_root/replicadb-server"
image_name=${REPLICADB_SERVER_IMAGE:-replicadb-server:phase3-smoke}

docker build \
    --file "$server_directory/Dockerfile" \
    --tag "$image_name" \
    "$server_directory"

image_user=$(docker image inspect "$image_name" --format '{{.Config.User}}')
test "$image_user" = "replicadb:replicadb"

docker image inspect "$image_name" --format '{{json .Config.Entrypoint}}' \
    | grep -Fq 'spring.profiles.active'

for profile in api worker; do
    docker run --rm \
        --entrypoint sh \
        --env SPRING_PROFILES_ACTIVE="$profile" \
        "$image_name" \
        -c 'test "$(id -un)" = replicadb; test -f /opt/replicadb/replicadb-server.jar; test "$SPRING_PROFILES_ACTIVE" = "$1"' \
        sh "$profile"
done

printf 'server image smoke passed for api and worker profile selection\n'
