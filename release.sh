#!/bin/bash

################################################################################
# ReplicaDB Release Script
# 
# Automates the release process:
# 1. Validates version format (semantic versioning)
# 2. Updates pom.xml with new version
# 3. Commits version bump
# 4. Creates git tag
# 5. Pushes to origin (CI/CD builds release automatically)
#
# Usage: ./release.sh <version>
# Example: ./release.sh 0.16.0
################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POM_FILE="${REPO_ROOT}/pom.xml"

################################################################################
# Functions
################################################################################

print_header() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC} $1"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

validate_version() {
    local version=$1
    # Validate semantic versioning (X.Y.Z)
    if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        print_error "Invalid version format: $version"
        print_info "Use semantic versioning format: X.Y.Z"
        exit 1
    fi
    print_success "Version format valid: $version"
}

get_current_version() {
    grep '<version>' "$POM_FILE" | head -1 | sed 's/.*<version>\(.*\)<\/version>.*/\1/'
}

check_git_clean() {
    if ! git diff-index --quiet HEAD --; then
        print_error "Git working directory is not clean"
        print_info "Commit or stash your changes before releasing"
        exit 1
    fi
    print_success "Git working directory is clean"
}

check_git_branch() {
    local branch=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$branch" != "master" && "$branch" != "main" ]]; then
        print_warning "Current branch is '$branch', not master/main"
        read -p "Continue anyway? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_info "Release cancelled"
            exit 1
        fi
    fi
    print_success "On branch: $branch"
}

update_pom_version() {
    local old_version=$1
    local new_version=$2
    
    print_info "Updating pom.xml: $old_version -> $new_version"
    
    # Update version in pom.xml
    sed -i.bak "s/<version>${old_version}<\/version>/<version>${new_version}<\/version>/" "$POM_FILE"
    rm -f "${POM_FILE}.bak"
    
    print_success "pom.xml updated"
}

create_release_commit() {
    local version=$1
    
    print_info "Creating release commit..."
    git add "$POM_FILE"
    git commit -m "Release v${version}" || print_warning "Commit may have failed or nothing to commit"
    
    print_success "Release commit created"
}

create_git_tag() {
    local version=$1
    local tag="v${version}"
    
    print_info "Creating git tag: $tag"
    
    if git rev-parse "$tag" >/dev/null 2>&1; then
        print_error "Tag $tag already exists"
        exit 1
    fi
    
    git tag -a "$tag" -m "Release $tag" || print_warning "Tag creation completed (may already exist)"
    print_success "Git tag created: $tag"
}

push_to_origin() {
    local version=$1
    local tag="v${version}"
    
    print_info "Pushing to origin..."
    print_info "  - Branch: master/main"
    print_info "  - Tag: $tag"
    
    git push origin HEAD:master || git push origin HEAD:main
    git push origin "$tag"
    
    print_success "Pushed to origin"
}

show_release_summary() {
    local old_version=$1
    local new_version=$2
    
    echo
    print_header "RELEASE SUMMARY"
    echo -e "${BLUE}Previous Version:${NC}     ${old_version}"
    echo -e "${BLUE}Release Version:${NC}      v${new_version}"
    echo -e "${BLUE}Release Tag:${NC}          v${new_version}"
    echo -e "${BLUE}Git Branch:${NC}           $(git rev-parse --abbrev-ref HEAD)"
    echo -e "${BLUE}Commit Hash:${NC}          $(git rev-parse --short HEAD)"
    echo
    echo -e "${GREEN}CI/CD Pipeline:${NC}       Automatically triggered on tag push"
    echo -e "${GREEN}Release Assets:${NC}"
    echo "  - ReplicaDB-${new_version}.tar.gz"
    echo "  - ReplicaDB-${new_version}.zip"
    echo "  - ReplicaDB-${new_version}-no-oracle.tar.gz"
    echo "  - ReplicaDB-${new_version}-no-oracle.zip"
    echo
    echo -e "${GREEN}Docker Images:${NC}"
    echo "  - osalvador/replicadb:${new_version}"
    echo "  - osalvador/replicadb:latest"
    echo "  - osalvador/replicadb:ubi9-${new_version}"
    echo "  - osalvador/replicadb:ubi9-latest"
    echo
    echo -e "${BLUE}Release URL:${NC}          https://github.com/osalvador/ReplicaDB/releases/tag/v${new_version}"
    echo
}

################################################################################
# Main Script
################################################################################

main() {
    # Check arguments
    if [[ $# -ne 1 ]]; then
        print_error "Missing version argument"
        echo
        echo "Usage: ./release.sh <version>"
        echo "Example: ./release.sh 0.16.0"
        echo
        echo "Steps performed:"
        echo "  1. Validate version format (semantic versioning)"
        echo "  2. Verify git working directory is clean"
        echo "  3. Update pom.xml with new version"
        echo "  4. Create release commit"
        echo "  5. Create git tag (v\${version})"
        echo "  6. Push commits and tags to origin"
        echo "  7. CI/CD builds and publishes release automatically"
        exit 1
    fi
    
    local new_version=$1
    local old_version=$(get_current_version)
    
    print_header "ReplicaDB Release Process"
    
    echo "Current Version:  ${old_version}"
    echo "Release Version:  ${new_version}"
    echo
    
    # Validations
    validate_version "$new_version"
    check_git_clean
    check_git_branch
    
    # Ask for confirmation
    echo
    read -p "Ready to release v${new_version}? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Release cancelled"
        exit 0
    fi
    
    # Execute release steps
    print_header "Executing Release Steps"
    
    update_pom_version "$old_version" "$new_version"
    create_release_commit "$new_version"
    create_git_tag "$new_version"
    push_to_origin "$new_version"
    
    # Show summary
    show_release_summary "$old_version" "$new_version"
    
    print_header "RELEASE SUCCESSFUL"
    echo -e "${GREEN}Release v${new_version} has been created and pushed!${NC}"
    echo
    echo "CI/CD Pipeline Status:"
    echo "  → Visit: https://github.com/osalvador/ReplicaDB/actions"
    echo
    echo "To view release progress:"
    echo "  → Visit: https://github.com/osalvador/ReplicaDB/releases/tag/v${new_version}"
    echo
}

# Run main function
main "$@"
