# ReplicaDB Release Process

## Overview

This document describes the automated release process for ReplicaDB. The process is designed to be simple and automated, with CI/CD handling the build and publication.

## Release Workflow

```
1. Developer runs release.sh
   ↓
2. Version validation & pre-checks
   ↓
3. Update pom.xml
   ↓
4. Create git commit & tag
   ↓
5. Push to origin/master
   ↓
6. GitHub Actions CI/CD triggered
   ↓
7. Build & publish release
   ↓
8. Docker images pushed
   ↓
9. Release assets available on GitHub
```

## Quick Start

### Creating a Release

```bash
# Move to project root
cd /path/to/ReplicaDB

# Run release script with new version
./release.sh 0.16.0
```

### Script Workflow

The `release.sh` script automatically:

1. **Validates Version Format**
   - Enforces semantic versioning (X.Y.Z)
   - Example: `0.16.0` ✓, `0.16` ✗

2. **Pre-flight Checks**
   - Verifies git working directory is clean
   - Checks current branch (master/main)
   - Confirms no uncommitted changes

3. **Updates Version**
   - Modifies pom.xml with new version
   - Creates release commit

4. **Creates Release Tag**
   - Creates annotated git tag: `v0.16.0`
   - Tags are pushed to origin

5. **Triggers CI/CD Pipeline**
   - GitHub Actions automatically builds on tag push
   - Runs on: `.github/workflows/CI_Release.yml`

## CI/CD Pipeline

### What CI/CD Does

The GitHub Actions workflow (`CI_Release.yml`) automatically:

1. **Builds with Maven**
   - Runs full Maven build
   - Skips tests (already run in main CI)
   - Creates JAR package

2. **Creates Release Artifacts**
   - ReplicaDB-X.Y.Z.tar.gz (with Oracle drivers)
   - ReplicaDB-X.Y.Z.zip (with Oracle drivers)
   - ReplicaDB-X.Y.Z-no-oracle.tar.gz
   - ReplicaDB-X.Y.Z-no-oracle.zip

3. **Creates GitHub Release**
   - Attaches build artifacts
   - Auto-generates release page

4. **Builds & Pushes Docker Images**
   - DockerHub: `osalvador/replicadb:0.16.0`
   - DockerHub: `osalvador/replicadb:latest`
   - DockerHub: `osalvador/replicadb:ubi9-0.16.0`
   - DockerHub: `osalvador/replicadb:ubi9-latest`

## Release Checklist

Before creating a release:

- [ ] All feature branches merged to master
- [ ] All tests passing
- [ ] CHANGELOG.md updated (if applicable)
- [ ] Version bumped in relevant places
- [ ] No uncommitted changes in git

## Example Release Session

```bash
# 1. Navigate to project
$ cd ~/Documents/GitHub/ReplicaDB

# 2. Verify you're on master branch
$ git branch
* master

# 3. Ensure working directory is clean
$ git status
On branch master
nothing to commit, working tree clean

# 4. Run release script
$ ./release.sh 0.16.0

╔════════════════════════════════════════════════════════╗
║ ReplicaDB Release Process
╚════════════════════════════════════════════════════════╝

Current Version:  0.15.1
Release Version:  0.16.0

✓ Version format valid: 0.16.0
✓ Git working directory is clean
✓ On branch: master

Ready to release v0.16.0? (y/n) y

╔════════════════════════════════════════════════════════╗
║ Executing Release Steps
╚════════════════════════════════════════════════════════╝

ℹ Updating pom.xml: 0.15.1 -> 0.16.0
✓ pom.xml updated
ℹ Creating release commit...
✓ Release commit created
ℹ Creating git tag: v0.16.0
✓ Git tag created: v0.16.0
ℹ Pushing to origin...
✓ Pushed to origin

╔════════════════════════════════════════════════════════╗
║ RELEASE SUMMARY
╚════════════════════════════════════════════════════════╝

Previous Version:     0.15.1
Release Version:      v0.16.0
Release Tag:          v0.16.0
Git Branch:           master
Commit Hash:          abc1234

CI/CD Pipeline:       Automatically triggered on tag push
Release Assets:
  - ReplicaDB-0.16.0.tar.gz
  - ReplicaDB-0.16.0.zip
  - ReplicaDB-0.16.0-no-oracle.tar.gz
  - ReplicaDB-0.16.0-no-oracle.zip

Docker Images:
  - osalvador/replicadb:0.16.0
  - osalvador/replicadb:latest
  - osalvador/replicadb:ubi9-0.16.0
  - osalvador/replicadb:ubi9-latest

Release URL:          https://github.com/osalvador/ReplicaDB/releases/tag/v0.16.0

╔════════════════════════════════════════════════════════╗
║ RELEASE SUCCESSFUL
╚════════════════════════════════════════════════════════╝

Release v0.16.0 has been created and pushed!

CI/CD Pipeline Status:
  → Visit: https://github.com/osalvador/ReplicaDB/actions

To view release progress:
  → Visit: https://github.com/osalvador/ReplicaDB/releases/tag/v0.16.0

# 5. Check CI/CD progress
$ # Open GitHub Actions in browser
```

## Monitoring the Release

### Step 1: Check GitHub Actions

1. Go to: https://github.com/osalvador/ReplicaDB/actions
2. Look for: "Java CI/CD ReplicaDB Release" workflow
3. Status should progress from: Queued → In Progress → Success

### Step 2: Verify Release Assets

Once CI/CD completes:

1. Go to: https://github.com/osalvador/ReplicaDB/releases/tag/v0.16.0
2. Check for attached files:
   - ReplicaDB-0.16.0.tar.gz
   - ReplicaDB-0.16.0.zip
   - etc.

### Step 3: Verify Docker Images

Check Docker Hub:

```bash
# Pull and verify image
docker pull osalvador/replicadb:0.16.0
docker run --version osalvador/replicadb:0.16.0

# Should show version info
```

## Troubleshooting

### Script Fails: "Git working directory is not clean"

**Solution:**
```bash
# Commit or stash changes
git status
git add .
git commit -m "commit message"
git stash  # or stash if not ready to commit
./release.sh 0.16.0
```

### Script Fails: "Invalid version format"

**Solution:**
```bash
# Use semantic versioning X.Y.Z
./release.sh 0.16.0    # ✓ Correct
./release.sh 0.16      # ✗ Wrong - missing patch version
./release.sh v0.16.0   # ✗ Wrong - don't include 'v' prefix
```

### CI/CD Pipeline Fails

**Solution:**
1. Check GitHub Actions logs: https://github.com/osalvador/ReplicaDB/actions
2. Common issues:
   - Tests failing (fix and re-run)
   - Docker credentials invalid (contact maintainer)
   - Build dependencies outdated (update pom.xml)

### Rollback a Release

If a release needs to be rolled back:

```bash
# Remove the tag from origin
git push origin --delete v0.16.0

# Delete the local tag
git tag -d v0.16.0

# Revert the commit
git revert HEAD

# Create new release with patch version
./release.sh 0.16.1
```

## Release History

View all releases:

```bash
# List all tags
git tag -l | sort -V

# Show tag details
git show v0.16.0

# View GitHub releases
# https://github.com/osalvador/ReplicaDB/releases
```

## Best Practices

1. **Test Before Release**
   - Run full test suite locally
   - Verify on multiple databases if possible

2. **Document Changes**
   - Update CHANGELOG.md with new features/fixes
   - Document breaking changes clearly

3. **Semantic Versioning**
   - MAJOR.MINOR.PATCH (e.g., 1.2.3)
   - MAJOR: Breaking changes
   - MINOR: New features (backward compatible)
   - PATCH: Bug fixes

4. **Release Notes**
   - Add meaningful release notes on GitHub
   - Highlight major features and fixes

5. **Version Schedule**
   - Plan releases in advance
   - Announce feature freeze before release
   - Allow time for testing and bugfixes

## Environment Requirements

### Local Development

- Git (version 2.0+)
- Bash shell
- Maven 3.6+
- Java 11+

### CI/CD (GitHub Actions)

- All requirements handled automatically
- Builds on Ubuntu Linux
- Deploys to DockerHub

### Docker Requirements

- DockerHub credentials configured in GitHub Secrets:
  - `DOCKERHUB_USERNAME`
  - `DOCKERHUB_TOKEN`

## Questions?

For more information:
- GitHub: https://github.com/osalvador/ReplicaDB
- Issues: https://github.com/osalvador/ReplicaDB/issues
- Discussions: https://github.com/osalvador/ReplicaDB/discussions
