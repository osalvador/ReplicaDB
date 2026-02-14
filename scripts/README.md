# Dependabot Rebase Tools

This directory contains tools to help rebase Dependabot PRs when they need to pick up fixes from the master branch.

## Background

When Dependabot PRs are created before critical fixes are merged to master (e.g., TestContainers version upgrades), the PRs may fail CI tests. Rebasing these PRs allows them to pick up the fixes and pass CI, enabling the auto-merge workflow to complete.

See `.github/workflows/INVESTIGATION-SUMMARY.md` for the full context on why this is needed.

## Tools

### 1. GitHub Actions Workflow (Recommended)

**File**: `.github/workflows/rebase-dependabot-prs.yml`

**Usage**:
1. Go to the Actions tab in GitHub
2. Select "Rebase Dependabot PRs" workflow
3. Click "Run workflow"
4. Wait for completion

**Advantages**:
- Has proper permissions to comment on PRs
- Automatic verification that PRs are still open
- Checks that PRs are from Dependabot
- Provides detailed logs and summary

### 2. Bash Script

**File**: `scripts/rebase-dependabot-prs.sh`

**Usage**:
```bash
# Requires gh CLI to be authenticated with write permissions
./scripts/rebase-dependabot-prs.sh
```

**Advantages**:
- Can be run locally
- Quick execution
- No need to navigate GitHub UI

**Requirements**:
- GitHub CLI (`gh`) installed
- Authenticated with a token that has `repo` permissions

## What These Tools Do

Both tools comment `@dependabot rebase` on all open Dependabot PRs. This triggers Dependabot to:
1. Rebase the PR on the current master branch
2. Pick up any fixes that were merged after the PR was created
3. Trigger new CI runs with the updated code

## Expected Outcome

After running either tool:
1. All Dependabot PRs will be rebased
2. CI tests should pass (with TestContainers 1.21.4 from master)
3. The auto-merge workflow will automatically merge the PRs
4. All dependency updates will be applied

## Troubleshooting

### PRs Not Rebasing
- Check that the PR is still open
- Verify the PR is from `dependabot[bot]`
- Look at the PR comments to confirm the rebase command was posted

### CI Still Failing After Rebase
- Check `.github/workflows/INVESTIGATION-SUMMARY.md` for known issues
- Review CI logs for new errors
- Ensure master branch has the necessary fixes

### Permission Errors
When using the bash script:
- Ensure you're authenticated: `gh auth status`
- Check token scopes: `gh auth login` with appropriate permissions
- Try using the GitHub Actions workflow instead (recommended)

## Manual Alternative

If automation isn't working, you can manually comment on each PR:

1. Navigate to the PR on GitHub
2. Add a comment: `@dependabot rebase`
3. Wait for Dependabot to respond and rebase

## List of Current Dependabot PRs

As of the last update, these Dependabot PRs are open:

- PR #222: org.apache.commons:commons-csv from 1.7 to 1.14.1
- PR #223: com.oracle.database.xml:xmlparserv2
- PR #224: version.testContainers from 1.21.3 to 1.21.4 (⭐ contains the fix!)
- PR #225: com.google.code.gson:gson from 2.8.9 to 2.13.2
- PR #226: org.postgresql:postgresql from 42.7.2 to 42.7.9
- PR #227: actions/upload-artifact from 4 to 6
- PR #228: docker/setup-qemu-action from 1 to 3
- PR #229: docker/login-action from 1 to 3
- PR #230: rake requirement from ~> 10.0 to ~> 13.3 in /docs
- PR #231: docker/setup-buildx-action from 1 to 3
- PR #232: actions/setup-java from 4 to 5
- PR #233: jekyll requirement from ~> 3.3 to >= 3.3, < 5.0 in /docs

**Note**: This list may be outdated. The workflow automatically checks which PRs are still open.
