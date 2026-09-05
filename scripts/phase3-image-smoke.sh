#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
server_directory="$repository_root/replicadb-server"
image_name=${REPLICADB_SERVER_IMAGE:-replicadb-server:phase3-smoke}
if [[ -n "${REPLICADB_SERVER_VERSION:-}" ]]; then
    server_version=$REPLICADB_SERVER_VERSION
else
    server_version=$(awk '
        /<artifactId>replicadb-server<\/artifactId>/ {
            found = 1
            next
        }
        found && match($0, /<version>[^<]+<\/version>/) {
            value = substr($0, RSTART, RLENGTH)
            sub(/^<version>/, "", value)
            sub(/<\/version>$/, "", value)
            print value
            exit
        }
    ' "$server_directory/pom.xml")
fi
if [[ ! "$server_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'Error: unable to determine a valid server version: %s\n' "$server_version" >&2
    exit 1
fi
server_jar="target/replicadb-server-${server_version}.jar"

docker build \
    --file "$server_directory/Dockerfile" \
    --build-arg "SERVER_VERSION=$server_version" \
    --build-arg "SERVER_JAR=$server_jar" \
    --tag "$image_name" \
    "$server_directory"

image_user=$(docker image inspect "$image_name" --format '{{.Config.User}}')
test "$image_user" = "replicadb:replicadb"

image_entrypoint=$(docker image inspect "$image_name" --format '{{json .Config.Entrypoint}}')
grep -Fq 'spring.profiles.active' <<< "$image_entrypoint"
grep -Fq 'replicadb.embedded-postgres.enabled=false' <<< "$image_entrypoint"
grep -Fq "replicadb-server.jar" <<< "$image_entrypoint"

for profile in api worker; do
    docker run --rm \
        --entrypoint sh \
        --env SPRING_PROFILES_ACTIVE="$profile" \
        "$image_name" \
        -c 'test "$(id -un)" = replicadb; test -f /opt/replicadb/replicadb-server.jar; test "$SPRING_PROFILES_ACTIVE" = "$1"' \
        sh "$profile"
done

if grep -Fq 'REPLICADB_EMBEDDED_POSTGRES_ENABLED=true' <<< "$image_entrypoint"; then
    echo 'embedded PostgreSQL must remain disabled in the image entrypoint' >&2
    exit 1
fi

printf 'server image smoke passed for api and worker profile selection\n'
