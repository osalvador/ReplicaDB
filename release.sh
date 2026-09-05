#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE="${RELEASE_REMOTE:-origin}"
TARGET_BRANCH="master"
POM_FILE="pom.xml"
SERVER_POM_FILE="replicadb-server/pom.xml"

VERSION_SURFACE_FILES=(
    "README.md"
    "DEPLOYMENT.md"
    "RELEASE_GUIDE.md"
    "CONTRIBUTING.md"
    "PRODUCT.md"
    "docs/index.md"
    "docs/server.md"
    "replicadb-server/README.md"
    "replicadb-server/frontend/README.develop.md"
)

RELEASE_FILES=(
    "$POM_FILE"
    "$SERVER_POM_FILE"
    "release.sh"
    "scripts/release-script.test.sh"
    "scripts/package-server-release.sh"
    "scripts/phase3-image-smoke.sh"
    ".github/workflows/CI_Release.yml"
    ".github/workflows/CT_Push.yml"
    ".github/skills/replicadb-release/SKILL.md"
    "${VERSION_SURFACE_FILES[@]}"
)

print_error() {
    printf 'error: %s\n' "$*" >&2
}

print_info() {
    printf '%s\n' "$*"
}

die() {
    print_error "$*"
    exit 1
}

usage() {
    cat <<'USAGE'
Usage:
  ./release.sh validate VERSION
  ./release.sh prepare VERSION
  ./release.sh tag VERSION --ci-green
  ./release.sh push-tag VERSION

Commands:
  validate   Check version, POMs, release documentation, and artifact names.
  prepare    Update the release contract, commit it, and push master.
  tag        Create an annotated local tag after explicit CI confirmation.
  push-tag   Push an already-created local tag to origin.

The prepare command never creates or pushes a tag. The tag command requires
the explicit --ci-green confirmation unless RELEASE_CI_GREEN=1 is set.
USAGE
}

require_file() {
    [[ -f "$1" ]] || die "Required file not found: $1"
}

validate_version() {
    local version=$1

    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || \
        die "Invalid version '$version'; expected X.Y.Z"
}

get_root_version() {
    sed -n 's/^[[:space:]]*<version>\([^<]*\)<\/version>[[:space:]]*$/\1/p' \
        "$POM_FILE" | head -n 1
}

get_version_after_artifact() {
    local artifact=$1
    local file=$2

    awk -v artifact="$artifact" '
        index($0, "<artifactId>" artifact "</artifactId>") {
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
    ' "$file"
}

path_is_release_file() {
    local path=$1
    local release_file

    case "$path" in
        .github/skills/replicadb-release/*)
            return 0
            ;;
    esac

    for release_file in "${RELEASE_FILES[@]}"; do
        [[ "$path" == "$release_file" ]] && return 0
    done
    return 1
}

path_is_known_untracked() {
    case "$1" in
        implementation_plan.md|implementation_plan_doc.md|shape-datasources.png|.ai|.ai/*|docs/astro.config.mjs|docs/package-lock.json|docs/package.json|docs/public|docs/public/*|docs/src|docs/src/*|docs/tests|docs/tests/*|docs/tsconfig.json|docs/node_modules|docs/node_modules/*|docs/dist|docs/dist/*|docs/.astro|docs/.astro/*|docs/.starlight|docs/.starlight/*|docs/test-results|docs/test-results/*|docs/playwright-report|docs/playwright-report/*|replicadb-server/frontend/.impeccable|replicadb-server/frontend/.impeccable/*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

check_branch() {
    local branch
    branch="$(git branch --show-current)"
    [[ "$branch" == "$TARGET_BRANCH" ]] || \
        die "Release commands must run on '$TARGET_BRANCH' (current: '${branch:-detached}')"
}

check_remote() {
    git remote get-url "$REMOTE" >/dev/null 2>&1 || \
        die "Git remote '$REMOTE' is not configured"
}

check_unexpected_untracked() {
    local path

    while IFS= read -r path; do
        [[ -z "$path" ]] && continue
        path_is_release_file "$path" && continue
        path_is_known_untracked "$path" && continue
        die "Unexpected untracked file: $path"
    done < <(git ls-files --others --exclude-standard)
}

check_release_scope() {
    local path

    while IFS= read -r path; do
        [[ -z "$path" ]] && continue
        path_is_release_file "$path" || \
            die "Tracked change outside the release scope: $path"
    done < <({ git diff --name-only; git diff --cached --name-only; } | sort -u)
}

check_prepare_worktree() {
    check_branch
    check_release_scope
    check_unexpected_untracked
}

check_release_clean() {
    check_branch
    check_unexpected_untracked
    git diff --quiet || die "Tracked release changes are still present"
    git diff --cached --quiet || die "Staged changes are still present"
}

validate_poms() {
    local version=$1
    local root_version
    local server_version
    local dependency_version

    require_file "$POM_FILE"
    require_file "$SERVER_POM_FILE"

    root_version="$(get_root_version)"
    server_version="$(get_version_after_artifact "replicadb-server" "$SERVER_POM_FILE")"
    dependency_version="$(get_version_after_artifact "ReplicaDB" "$SERVER_POM_FILE")"

    [[ "$root_version" == "$version" ]] || \
        die "Root POM version is '$root_version', expected '$version'"
    [[ "$server_version" == "$version" ]] || \
        die "Server POM version is '$server_version', expected '$version'"
    [[ "$dependency_version" == "$version" ]] || \
        die "Server CLI dependency is '$dependency_version', expected '$version'"
}

validate_version_surface() {
    local version=$1
    local file
    local artifact
    local jar
    local url_version
    local artifacts
    local jars
    local url_versions

    for file in "${VERSION_SURFACE_FILES[@]}"; do
        require_file "$file"
    done

    grep -F -q -- "ReplicaDB-${version}" "${VERSION_SURFACE_FILES[@]}" || \
        die "Release documentation does not contain ReplicaDB-${version}"
    grep -F -q -- "ReplicaDB-server-${version}" "${VERSION_SURFACE_FILES[@]}" || \
        die "Release documentation does not contain ReplicaDB-server-${version}"
    grep -F -q -- "replicadb-server-${version}.jar" "${VERSION_SURFACE_FILES[@]}" || \
        die "Release documentation does not contain replicadb-server-${version}.jar"

    artifacts="$(grep -Eho 'ReplicaDB(-server)?-[0-9]+\.[0-9]+\.[0-9]+' \
        "${VERSION_SURFACE_FILES[@]}" | sort -u || true)"
    while IFS= read -r artifact; do
        [[ -z "$artifact" ]] && continue
        [[ "$artifact" == "ReplicaDB-${version}" || \
            "$artifact" == "ReplicaDB-server-${version}" ]] || \
            die "Stale release artifact name: $artifact"
    done <<< "$artifacts"

    jars="$(grep -Eho 'replicadb-server-[0-9]+\.[0-9]+\.[0-9]+\.jar' \
        "${VERSION_SURFACE_FILES[@]}" | sort -u || true)"
    while IFS= read -r jar; do
        [[ -z "$jar" ]] && continue
        [[ "$jar" == "replicadb-server-${version}.jar" ]] || \
            die "Stale server JAR name: $jar"
    done <<< "$jars"

    url_versions="$(grep -Eho 'releases/(download|tag)/v[0-9]+\.[0-9]+\.[0-9]+' \
        "${VERSION_SURFACE_FILES[@]}" | sed 's/.*\/v//' | sort -u || true)"
    while IFS= read -r url_version; do
        [[ -z "$url_version" ]] && continue
        [[ "$url_version" == "$version" ]] || \
            die "Stale release URL version: $url_version"
    done <<< "$url_versions"
}

validate_release_contract() {
    local version=$1

    validate_version "$version"
    validate_poms "$version"
    validate_version_surface "$version"
}

replace_version_in_file() {
    local file=$1
    local old_version=$2
    local new_version=$3

    perl -0pi -e 's/\Q'"$old_version"'\E/'"$new_version"'/g' "$file"
}

update_poms() {
    local old_version=$1
    local new_version=$2

    perl -0pi -e 's{(<artifactId>ReplicaDB</artifactId>\s*<version>)\Q'"$old_version"'\E(</version>)}{${1}'"$new_version"'${2}}' "$POM_FILE"
    perl -0pi -e 's{(<artifactId>replicadb-server</artifactId>\s*<version>)\Q'"$old_version"'\E(</version>)}{${1}'"$new_version"'${2}}' "$SERVER_POM_FILE"
    perl -0pi -e 's{(<artifactId>ReplicaDB</artifactId>\s*<version>)\Q'"$old_version"'\E(</version>)}{${1}'"$new_version"'${2}}' "$SERVER_POM_FILE"
}

update_release_contract() {
    local new_version=$1
    local old_version
    local file

    old_version="$(get_root_version)"
    [[ -n "$old_version" ]] || die "Unable to read the current root POM version"

    update_poms "$old_version" "$new_version"
    for file in "${VERSION_SURFACE_FILES[@]}"; do
        replace_version_in_file "$file" "$old_version" "$new_version"
    done
}

stage_release_files() {
    local file

    for file in "${RELEASE_FILES[@]}"; do
        [[ -e "$file" ]] || continue
        git add -- "$file"
    done
}

check_staged_scope() {
    local path

    while IFS= read -r path; do
        [[ -z "$path" ]] && continue
        path_is_release_file "$path" || \
            die "Staged file outside the release scope: $path"
    done < <(git diff --cached --name-only)
}

prepare_release() {
    local version=$1

    check_prepare_worktree
    check_remote
    require_command perl
    update_release_contract "$version"
    validate_release_contract "$version"
    git diff --check
    stage_release_files
    check_staged_scope
    git diff --cached --quiet && die "No release changes are available to commit"
    git commit -m "feat(release): prepare ${version}"
    git push "$REMOTE" "HEAD:${TARGET_BRANCH}"
    print_info "Prepared ${version} and pushed ${TARGET_BRANCH}; no tag was created."
}

remote_tag_exists() {
    local tag=$1
    git ls-remote --exit-code --refs "$REMOTE" "refs/tags/${tag}" >/dev/null 2>&1
}

assert_tag_absent() {
    local tag=$1

    if git rev-parse --verify --quiet "refs/tags/${tag}" >/dev/null; then
        die "Tag ${tag} already exists locally"
    fi
    if remote_tag_exists "$tag"; then
        die "Tag ${tag} already exists on ${REMOTE}"
    fi
}

require_ci_confirmation() {
    local confirmation=${1:-}
    local configured=${RELEASE_CI_GREEN:-}

    if [[ "$confirmation" == "--ci-green" ]]; then
        return 0
    fi

    case "$configured" in
        1|true|yes)
            return 0
            ;;
        *)
            die "Tagging requires --ci-green or RELEASE_CI_GREEN=1 after remote gates pass"
            ;;
    esac
}

tag_release() {
    local version=$1
    local confirmation=${2:-}
    local tag="v${version}"

    require_ci_confirmation "$confirmation"
    validate_release_contract "$version"
    check_release_clean
    check_remote
    assert_tag_absent "$tag"
    git tag -a "$tag" -m "Release ${tag}"
    print_info "Created local annotated tag ${tag}; use push-tag to publish it."
}

push_tag() {
    local version=$1
    local tag="v${version}"
    local tag_commit

    validate_release_contract "$version"
    check_release_clean
    check_remote
    git rev-parse --verify --quiet "refs/tags/${tag}" >/dev/null || \
        die "Local tag ${tag} does not exist"
    tag_commit="$(git rev-list -n 1 "$tag")"
    [[ "$tag_commit" == "$(git rev-parse HEAD)" ]] || \
        die "Local tag ${tag} does not point to HEAD"
    if remote_tag_exists "$tag"; then
        die "Tag ${tag} already exists on ${REMOTE}"
    fi
    git push "$REMOTE" "$tag"
    print_info "Pushed ${tag} to ${REMOTE}."
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

main() {
    cd "$REPO_ROOT"
    git rev-parse --is-inside-work-tree >/dev/null 2>&1 || \
        die "release.sh must run inside a Git worktree"

    case "${1:-}" in
        validate)
            [[ $# -eq 2 ]] || { usage >&2; exit 2; }
            validate_release_contract "$2"
            print_info "Release contract ${2} is coherent."
            ;;
        prepare)
            [[ $# -eq 2 ]] || { usage >&2; exit 2; }
            prepare_release "$2"
            ;;
        tag)
            [[ $# -eq 2 || $# -eq 3 ]] || { usage >&2; exit 2; }
            [[ $# -eq 2 || "$3" == "--ci-green" ]] || { usage >&2; exit 2; }
            tag_release "$2" "${3:-}"
            ;;
        push-tag)
            [[ $# -eq 2 ]] || { usage >&2; exit 2; }
            push_tag "$2"
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
}

main "$@"
