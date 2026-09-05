#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/replicadb-release-test.XXXXXX")"
FIXTURE_INDEX=0
FIXTURE_PATH=""
FIXTURE_REMOTE=""

cleanup() {
    rm -rf "$TEMP_ROOT"
}

trap cleanup EXIT

fail() {
    printf 'release-script test failed: %s\n' "$*" >&2
    exit 1
}

expect_failure() {
    local output
    output="$(mktemp "${TEMP_ROOT}/output.XXXXXX")"
    if "$@" >"$output" 2>&1; then
        cat "$output" >&2
        rm -f "$output"
        fail "expected command to fail: $*"
    fi
    rm -f "$output"
}

assert_equal() {
    local expected=$1
    local actual=$2
    local message=$3

    [[ "$expected" == "$actual" ]] || \
        fail "$message (expected '$expected', got '$actual')"
}

copy_fixture_files() {
    local destination=$1
    local file
    local files=(
        "pom.xml"
        "release.sh"
        "README.md"
        "DEPLOYMENT.md"
        "RELEASE_GUIDE.md"
        "CONTRIBUTING.md"
        "PRODUCT.md"
        "docs/index.md"
        "docs/server.md"
        "replicadb-server/pom.xml"
        "replicadb-server/README.md"
        "replicadb-server/frontend/README.develop.md"
    )

    for file in "${files[@]}"; do
        mkdir -p "$(dirname "$destination/$file")"
        cp "$PROJECT_ROOT/$file" "$destination/$file"
    done
}

set_fixture_version() {
    local fixture=$1
    local file
    local files=(
        "pom.xml"
        "README.md"
        "DEPLOYMENT.md"
        "RELEASE_GUIDE.md"
        "CONTRIBUTING.md"
        "PRODUCT.md"
        "docs/index.md"
        "docs/server.md"
        "replicadb-server/pom.xml"
        "replicadb-server/README.md"
        "replicadb-server/frontend/README.develop.md"
    )

    for file in "${files[@]}"; do
        perl -0pi -e 's/1\.0\.0/0.19.0/g' "$fixture/$file"
    done
}

make_fixture() {
    local fixture
    local remote

    FIXTURE_INDEX=$((FIXTURE_INDEX + 1))
    fixture="${TEMP_ROOT}/repo-${FIXTURE_INDEX}"
    remote="${TEMP_ROOT}/remote-${FIXTURE_INDEX}.git"
    mkdir -p "$fixture"
    git init -q -b master "$fixture"
    git -C "$fixture" config user.name "ReplicaDB Release Test"
    git -C "$fixture" config user.email "release-test@example.invalid"
    copy_fixture_files "$fixture"
    set_fixture_version "$fixture"
    git -C "$fixture" add .
    git -C "$fixture" commit -qm "fixture: initial release state"
    git init -q --bare "$remote"
    git -C "$fixture" remote add origin "$remote"
    git -C "$fixture" push -q -u origin master
    FIXTURE_PATH="$fixture"
    FIXTURE_REMOTE="$remote"
}

assert_no_tag() {
    local tag=$1

    if git -C "$FIXTURE_PATH" rev-parse --verify --quiet "refs/tags/$tag" >/dev/null; then
        fail "local tag should not exist: $tag"
    fi
    if git --git-dir "$FIXTURE_REMOTE" show-ref --verify --quiet "refs/tags/$tag"; then
        fail "remote tag should not exist: $tag"
    fi
}

test_invalid_arguments() {
    make_fixture
    expect_failure "$FIXTURE_PATH/release.sh"
    expect_failure "$FIXTURE_PATH/release.sh" unknown 0.19.0
    expect_failure "$FIXTURE_PATH/release.sh" validate
    expect_failure "$FIXTURE_PATH/release.sh" validate v0.19.0
    expect_failure "$FIXTURE_PATH/release.sh" validate 0.19
}

test_validate_is_read_only() {
    local head_before
    local status_before

    make_fixture
    head_before="$(git -C "$FIXTURE_PATH" rev-parse HEAD)"
    status_before="$(git -C "$FIXTURE_PATH" status --porcelain)"
    "$FIXTURE_PATH/release.sh" validate 0.19.0 >/dev/null
    assert_equal "$head_before" "$(git -C "$FIXTURE_PATH" rev-parse HEAD)" \
        "validate changed HEAD"
    assert_equal "$status_before" "$(git -C "$FIXTURE_PATH" status --porcelain)" \
        "validate changed the worktree"
}

test_version_mismatch() {
    make_fixture
    perl -0pi -e 's/<version>0\.19\.0<\/version>/<version>9.9.9<\/version>/' \
        "$FIXTURE_PATH/pom.xml"
    expect_failure "$FIXTURE_PATH/release.sh" validate 0.19.0
}

test_prepare_rejects_wrong_branch() {
    make_fixture
    git -C "$FIXTURE_PATH" checkout -q -b release-work
    expect_failure "$FIXTURE_PATH/release.sh" prepare 1.0.0
}

test_prepare_rejects_dirty_scope() {
    make_fixture
    printf 'tracked fixture file\n' >"$FIXTURE_PATH/unrelated.txt"
    git -C "$FIXTURE_PATH" add unrelated.txt
    git -C "$FIXTURE_PATH" commit -qm "fixture: add unrelated file"
    printf 'unexpected tracked change\n' >>"$FIXTURE_PATH/unrelated.txt"
    expect_failure "$FIXTURE_PATH/release.sh" prepare 1.0.0

    make_fixture
    : >"$FIXTURE_PATH/unexpected.txt"
    expect_failure "$FIXTURE_PATH/release.sh" prepare 1.0.0
}

test_prepare_pushes_without_tag() {
    local prepared_head

    make_fixture
    mkdir -p "$FIXTURE_PATH/.ai"
    printf 'planning note\n' >"$FIXTURE_PATH/.ai/notes.md"
    printf 'separate documentation plan\n' >"$FIXTURE_PATH/implementation_plan_doc.md"
    mkdir -p "$FIXTURE_PATH/docs/src"
    printf 'documentation portal source\n' >"$FIXTURE_PATH/docs/src/index.md"
    mkdir -p "$FIXTURE_PATH/docs/.astro/collections"
    printf 'generated documentation metadata\n' >"$FIXTURE_PATH/docs/.astro/collections/docs.schema.json"
    : >"$FIXTURE_PATH/shape-datasources.png"
    "$FIXTURE_PATH/release.sh" prepare 1.0.0 >/dev/null
    prepared_head="$(git -C "$FIXTURE_PATH" rev-parse HEAD)"

    assert_equal "feat(release): prepare 1.0.0" \
        "$(git -C "$FIXTURE_PATH" log -1 --format=%s)" \
        "prepare commit message is incorrect"
    assert_equal "$prepared_head" \
        "$(git --git-dir "$FIXTURE_REMOTE" rev-parse refs/heads/master)" \
        "prepare did not push master"
    assert_no_tag "v1.0.0"
    git -C "$FIXTURE_PATH" diff --quiet
    git -C "$FIXTURE_PATH" diff --cached --quiet
    if git -C "$FIXTURE_PATH" ls-files --error-unmatch .ai/notes.md >/dev/null 2>&1; then
        fail "known untracked planning files must not be staged"
    fi
    if git -C "$FIXTURE_PATH" ls-files --error-unmatch implementation_plan_doc.md >/dev/null 2>&1; then
        fail "the separate documentation plan must not be staged"
    fi
    if git -C "$FIXTURE_PATH" ls-files --error-unmatch docs/src/index.md >/dev/null 2>&1; then
        fail "untracked documentation portal files must not be staged"
    fi
    if git -C "$FIXTURE_PATH" ls-files --error-unmatch docs/.astro/collections/docs.schema.json >/dev/null 2>&1; then
        fail "generated documentation portal files must not be staged"
    fi
}

test_tag_requires_gate_and_rejects_duplicates() {
    make_fixture
    "$FIXTURE_PATH/release.sh" prepare 1.0.0 >/dev/null
    expect_failure "$FIXTURE_PATH/release.sh" tag 1.0.0
    assert_no_tag "v1.0.0"

    "$FIXTURE_PATH/release.sh" tag 1.0.0 --ci-green >/dev/null
    git -C "$FIXTURE_PATH" rev-parse --verify --quiet refs/tags/v1.0.0 >/dev/null
    expect_failure "$FIXTURE_PATH/release.sh" tag 1.0.0 --ci-green

    "$FIXTURE_PATH/release.sh" push-tag 1.0.0 >/dev/null
    git --git-dir "$FIXTURE_REMOTE" show-ref --verify --quiet refs/tags/v1.0.0
    assert_equal "$(git -C "$FIXTURE_PATH" rev-parse HEAD)" \
        "$(git --git-dir "$FIXTURE_REMOTE" rev-parse refs/tags/v1.0.0^{})" \
        "remote tag does not point to the prepared commit"
}

test_tag_rejects_dirty_worktree() {
    make_fixture
    "$FIXTURE_PATH/release.sh" prepare 1.0.0 >/dev/null
    printf 'dirty release documentation\n' >>"$FIXTURE_PATH/README.md"
    expect_failure "$FIXTURE_PATH/release.sh" tag 1.0.0 --ci-green
    assert_no_tag "v1.0.0"
}

test_invalid_arguments
test_validate_is_read_only
test_version_mismatch
test_prepare_rejects_wrong_branch
test_prepare_rejects_dirty_scope
test_prepare_pushes_without_tag
test_tag_requires_gate_and_rejects_duplicates
test_tag_rejects_dirty_worktree

printf 'release-script tests passed\n'