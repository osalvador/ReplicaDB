#!/usr/bin/env bash

set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-image-test.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
STUB_BIN="$TEMP_ROOT/bin"
mkdir -p "$STUB_BIN"
cat >"$STUB_BIN/docker" <<'EOF'
#!/usr/bin/env bash
case "$*" in
    *'push'* ) [[ "${IMAGE_TEST_PUSH_FAIL:-false}" == true ]] && exit 1 || exit 0 ;;
    *'pull'*|*'tag'* ) exit 0 ;;
    *'manifest inspect'*bad-arch*) printf '{"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","architecture":"arm64","os":"linux"}' ;;
    *'manifest inspect'*) printf '{"digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","architecture":"amd64","os":"linux"}' ;;
    *) exit 0 ;;
esac
EOF
cat >"$STUB_BIN/jq" <<'EOF'
#!/usr/bin/env bash
input=$(cat)
case "$input" in
    *architecture*amd64*)
        case "$*" in
            *'unique'*) printf 'amd64\n' ;;
            *) printf 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' ;;
        esac
        ;;
    *) printf '\n' ;;
esac
EOF
chmod 755 "$STUB_BIN/docker" "$STUB_BIN/jq"
export PATH="$STUB_BIN:$PATH"
# shellcheck disable=SC1091
source "$TEST_DIR/../lib/image.sh"

IMAGE=osalvador/replicadb-server:1.0.0
IMAGE_DIGEST=
resolved=$(image_resolve)
[[ "$resolved" == osalvador/replicadb-server:1.0.0@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ]] || {
    printf 'tag was not resolved to digest\n' >&2
    exit 1
}

IMAGE=osalvador/replicadb-server:1.0.0
IMAGE_DIGEST=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
resolved=$(image_resolve)
[[ "$resolved" == *'@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' ]] || exit 1

IMAGE=osalvador/replicadb-server:latest
IMAGE_DIGEST=
if image_resolve >/dev/null 2>&1; then exit 1; fi
ALLOW_MUTABLE_IMAGE=true
image_resolve >/dev/null

IMAGE=osalvador/replicadb-server:bad-arch
ALLOW_MUTABLE_IMAGE=false
if image_resolve >/dev/null 2>&1; then exit 1; fi

IMAGE=osalvador/replicadb-server@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
image_resolve >/dev/null
[[ "$FINAL_DIGEST" == sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc ]] || exit 1

IMAGE=osalvador/replicadb-server:1.0.0
IMAGE_DIGEST=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
IMAGE_MIRROR=europe-west4-docker.pkg.dev/test-project/replicadb/server:1.0.0
resolved=$(image_resolve)
[[ "$resolved" == "$IMAGE_MIRROR@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ]] || exit 1
export IMAGE_TEST_PUSH_FAIL=true
if image_resolve >/dev/null 2>&1; then exit 1; fi

printf 'image tests passed\n'