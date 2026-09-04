#!/usr/bin/env bash

set -euo pipefail

usage() {
    printf 'Usage: %s <version> <server-jar> <output-directory>\n' "$(basename "$0")" >&2
}

if [[ $# -ne 3 ]]; then
    usage
    exit 2
fi

VERSION=$1
SERVER_JAR=$2
OUTPUT_DIR=$3
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACKAGE_NAME="ReplicaDB-server-${VERSION}"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'Error: version must use X.Y.Z format\n' >&2
    exit 1
fi
if [[ ! -f "$SERVER_JAR" || ! -r "$SERVER_JAR" ]]; then
    printf 'Error: server JAR is missing or unreadable: %s\n' "$SERVER_JAR" >&2
    exit 1
fi
JAR_TOOL="${JAVA_HOME:-}/bin/jar"
if [[ ! -x "$JAR_TOOL" ]]; then
    JAR_TOOL=$(command -v jar || true)
fi
if [[ -z "$JAR_TOOL" || ! -x "$JAR_TOOL" ]]; then
    printf 'Error: Java 17 jar tool is required\n' >&2
    exit 1
fi
if ! "$JAR_TOOL" tf "$SERVER_JAR" >/dev/null 2>&1; then
    printf 'Error: server JAR is not a readable archive\n' >&2
    exit 1
fi
if "$JAR_TOOL" tf "$SERVER_JAR" | rg -q '(^|/)postgres-[^/]*\.txz$'; then
    printf 'Error: server JAR contains a PostgreSQL native bundle\n' >&2
    exit 1
fi

SOURCE_LAUNCHER="${REPO_ROOT}/replicadb-server/bin/replicadb-server"
SOURCE_WINDOWS_LAUNCHER="${REPO_ROOT}/replicadb-server/bin/replicadb-server.cmd"
SOURCE_ENV="${REPO_ROOT}/replicadb-server/conf/replicadb-server.env.example"
SOURCE_README="${REPO_ROOT}/replicadb-server/README.md"
SOURCE_LICENSE="${REPO_ROOT}/LICENSE"
for source in "$SOURCE_LAUNCHER" "$SOURCE_WINDOWS_LAUNCHER" "$SOURCE_ENV" "$SOURCE_README" "$SOURCE_LICENSE"; do
    if [[ ! -f "$source" ]]; then
        printf 'Error: required package source is missing: %s\n' "$source" >&2
        exit 1
    fi
done

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
ARCHIVE_TAR="${OUTPUT_DIR}/${PACKAGE_NAME}.tar.gz"
ARCHIVE_ZIP="${OUTPUT_DIR}/${PACKAGE_NAME}.zip"
STAGING_DIR=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-server-package.XXXXXX")
trap 'rm -rf "$STAGING_DIR"' EXIT

PACKAGE_ROOT="${STAGING_DIR}/${PACKAGE_NAME}"
mkdir -p "$PACKAGE_ROOT/bin" "$PACKAGE_ROOT/lib" "$PACKAGE_ROOT/conf"
install -m 755 "$SOURCE_LAUNCHER" "$PACKAGE_ROOT/bin/replicadb-server"
install -m 644 "$SOURCE_WINDOWS_LAUNCHER" "$PACKAGE_ROOT/bin/replicadb-server.cmd"
install -m 644 "$SOURCE_ENV" "$PACKAGE_ROOT/conf/replicadb-server.env.example"
install -m 644 "$SOURCE_README" "$PACKAGE_ROOT/README.md"
install -m 644 "$SOURCE_LICENSE" "$PACKAGE_ROOT/LICENSE"
install -m 644 "$SERVER_JAR" "$PACKAGE_ROOT/lib/replicadb-server-${VERSION}.jar"
printf '%s\n' "$VERSION" > "$PACKAGE_ROOT/VERSION"

if rg -n -P '(?i)(password|api[_-]?key|access[_-]?token)\s*[:=]\s*[\x27\x22]?[^<\x27\x22\$\{[:space:]]' \
        "$PACKAGE_ROOT/conf" "$PACKAGE_ROOT/README.md" >/dev/null; then
    printf 'Error: package documentation or example contains a resolved secret or DSN\n' >&2
    exit 1
fi

# Normalize source timestamps so tar and zip metadata are stable across runs.
find "$PACKAGE_ROOT" -exec touch -t 198001010000 {} +
rm -f "$ARCHIVE_TAR" "$ARCHIVE_ZIP"

if tar --help 2>&1 | rg -q -- '--sort'; then
    GZIP=-n tar --sort=name --mtime='1980-01-01 00:00:00Z' --owner=0 --group=0 --numeric-owner \
        -C "$STAGING_DIR" -czf "$ARCHIVE_TAR" "$PACKAGE_NAME"
else
    (cd "$STAGING_DIR" && COPYFILE_DISABLE=1 tar -cf - \
    $(find "$PACKAGE_NAME" -type f -print | LC_ALL=C sort)) | gzip -n > "$ARCHIVE_TAR"
fi
(cd "$STAGING_DIR" && find "$PACKAGE_NAME" -type f -print | LC_ALL=C sort \
    | zip -X -q "$ARCHIVE_ZIP" -@)

printf 'Created %s\nCreated %s\n' "$ARCHIVE_TAR" "$ARCHIVE_ZIP"
