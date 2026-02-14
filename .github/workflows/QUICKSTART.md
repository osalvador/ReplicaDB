# Quick Start: Rebasing Dependabot PRs

## The Problem
All 12 open Dependabot PRs are failing CI tests due to Docker/TestContainers API incompatibility.

## The Solution in 3 Steps

### Step 1: Merge This PR
Merge the current PR that contains the automation tools.

### Step 2: Run the Workflow

1. Navigate to the **Actions** tab in GitHub
2. Find "**Rebase Dependabot PRs**" in the workflow list (left sidebar)
3. Click on it
4. Click the "**Run workflow**" button (on the right)
5. Confirm by clicking "Run workflow" in the popup

```
Actions Tab → Rebase Dependabot PRs → Run workflow button → Confirm
```

### Step 3: Wait and Monitor

The workflow will:
- ✅ Comment on all 12 Dependabot PRs (~15 seconds)
- ✅ Dependabot will rebase each PR (a few minutes)
- ✅ CI tests will run with TestContainers 1.21.4 (15-30 min per PR)
- ✅ Auto-merge will merge passing PRs automatically

## What You'll See

### In the Workflow
- Success logs for each PR
- Summary with next steps
- Total execution time: ~15 seconds

### On Each PR
- New comment: `@dependabot rebase`
- Dependabot response: "OK, I'll rebase this PR"
- New commits showing rebase
- CI checks running
- Eventually: "This PR has been merged"

## Troubleshooting

### Workflow Not Appearing?
- Make sure you merged the PR first
- Refresh the Actions page
- Check that the workflow file exists: `.github/workflows/rebase-dependabot-prs.yml`

### PRs Not Rebasing?
- Check that PRs are still open (not already merged/closed)
- Look for Dependabot's response in PR comments
- Wait a few minutes - Dependabot may be processing other requests

### CI Still Failing?
- Check that master has TestContainers 1.21.4 in `pom.xml`
- Review CI logs for new errors (not Docker API errors)
- See `TROUBLESHOOTING-dependabot.md` for detailed help

## Alternative Methods

If the workflow doesn't work for any reason:

### Method 1: Bash Script (Local)
```bash
./scripts/rebase-dependabot-prs.sh
```
Requires: GitHub CLI authenticated with write permissions

### Method 2: Manual Comments
Go to each PR and comment: `@dependabot rebase`

PRs to comment on: #222, #223, #224, #225, #226, #227, #228, #229, #230, #231, #232, #233

## Expected Timeline

- **Commenting**: 15 seconds (workflow)
- **Rebasing**: 2-3 minutes per PR (Dependabot)
- **CI Tests**: 15-30 minutes per PR (GitHub Actions)
- **Auto-merge**: Immediate after CI passes

**Total time**: 1-2 hours for all PRs to be merged

## Success Criteria

You'll know it worked when:
- ✅ All 12 Dependabot PRs are closed as merged
- ✅ No Docker API v1.32 errors in CI logs
- ✅ TestContainers 1.21.4 showing in all test runs
- ✅ Master branch has all dependency updates

## Need Help?

See detailed documentation:
- `scripts/README.md` - Complete tool documentation
- `INVESTIGATION-SUMMARY.md` - Technical analysis
- `TROUBLESHOOTING-dependabot.md` - Troubleshooting guide
