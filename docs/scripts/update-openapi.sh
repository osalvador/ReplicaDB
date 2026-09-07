#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
target="$script_dir/../openapi/replicadb-server.json"
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-openapi.XXXXXX")
trap 'rm -rf "$temporary_root"' EXIT

raw="$temporary_root/raw.json"
canonical="$temporary_root/canonical.json"
second="$temporary_root/second.json"

(cd "$repository_root" && mvn -B -f replicadb-server/pom.xml \
  -Dtest=OpenApiSpecificationIT \
  -Dskip.installnodenpm=true \
  -Dskip.npm=true \
  -Dreplicadb.openapi.output="$raw" test)

canonicalize() {
  node --input-type=module - "$1" "$2" <<'NODE'
import { readFileSync, writeFileSync } from 'node:fs';

const [, , input, output] = process.argv;
const sort = (value) => {
  if (Array.isArray(value)) return value.map(sort);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sort(value[key])]));
  }
  return value;
};

writeFileSync(output, `${JSON.stringify(sort(JSON.parse(readFileSync(input, 'utf8'))), null, 2)}\n`);
NODE
}

canonicalize "$raw" "$canonical"
canonicalize "$canonical" "$second"
cmp "$canonical" "$second"
mkdir -p "$(dirname "$target")"
mv "$canonical" "$target"
