# Troubleshooting Dependabot Auto-Merge Issues

## Common Issue: TestContainers Docker API Incompatibility

### Problem
Dependabot PRs fail with Docker/TestContainers errors:
```
BadRequestException (Status 400: {"message":"client version 1.32 is too old. Minimum supported API version is 1.44, please upgrade your client to a newer version"})
IllegalStateException: Previous attempts to find a Docker environment failed
```

### Root Cause
- GitHub Actions runners use Docker 29+ (requires API version 1.44+)
- TestContainers 1.21.3 and earlier use Docker API version 1.32
- **This is a known compatibility issue** in the TestContainers community

### Solution

#### Option 1: Rebase Dependabot PRs (Recommended)
When master branch has the TestContainers fix but old PRs don't:

1. Comment on the Dependabot PR:
   ```
   @dependabot rebase
   ```
2. Wait for CI to complete
3. Auto-merge workflow will merge if tests pass

#### Option 2: Add Workaround to CI Workflow
Add this step to `.github/workflows/CT_Push.yml` before running tests:

```yaml
- name: Force Docker API version for TestContainers
  run: echo api.version=1.44 >> ~/.docker-java.properties
```

This forces the Docker Java client to use API v1.44.

#### Option 3: Upgrade TestContainers
Ensure `pom.xml` has TestContainers 1.21.4+ or 2.0.2+:

```xml
<version.testContainers>1.21.4</version.testContainers>
```

Or upgrade to 2.x for long-term compatibility:

```xml
<version.testContainers>2.0.2</version.testContainers>
```

### Prevention
- Keep TestContainers dependency up-to-date
- Monitor TestContainers release notes for Docker compatibility issues
- Rebase old Dependabot PRs regularly

### Verification
Check that tests pass by looking for:
- ✅ All CI/CT jobs succeed
- ✅ No Docker initialization errors in logs
- ✅ TestContainers version shown in logs matches pom.xml

### Related PRs
- PR #224: Upgrade testcontainers from 1.21.3 to 1.21.4 (contains fix)
- PR #232: Example PR that failed with this issue

### References
- [TestContainers Issue #11250](https://github.com/testcontainers/testcontainers-java/issues/11250)
- [TestContainers Issue #11235](https://github.com/testcontainers/testcontainers-java/issues/11235)
- [Stack Overflow: Docker API version error](https://stackoverflow.com/questions/79817033/sudden-docker-error-about-client-api-version)

## Why Auto-Merge Didn't Work

### Expected Behavior
The auto-merge workflow (`dependabot-auto-merge.yml`) is designed to:

1. Detect Dependabot PRs
2. Wait for CI tests to complete
3. Check if all tests passed
4. Auto-approve and merge if tests pass
5. Do nothing if tests fail

### Actual Behavior
The workflow is **working correctly**! It:

- ✅ Detected Dependabot PRs
- ✅ Waited for CI completion
- ✅ Found test failures (Docker errors)
- ✅ Correctly refused to merge failed PRs

### What Looked Like a Bug But Wasn't
- Workflow runs show "cancelled" or "failure" status ✓ Expected for failed tests
- PRs remain open after CI completes ✓ Expected when tests fail
- No auto-merge happened ✓ Expected behavior (tests failed)

### When to Investigate Auto-Merge
Only investigate the auto-merge workflow if:
- ❌ CI tests pass but PR doesn't merge
- ❌ Non-Dependabot PRs get auto-merged
- ❌ Workflow errors out before checking CI status
- ❌ PR gets merged despite failed tests

## Monitoring Auto-Merge

### Check Workflow Status
```bash
# View recent auto-merge workflow runs
gh run list --workflow=dependabot-auto-merge.yml

# View logs for a specific run
gh run view <run-id> --log
```

### Check PR Status
The workflow adds a summary to each run showing:
- PR number
- Ready to merge status
- Has failed checks status
- Status state
- Number of check runs

### Manual Intervention
If auto-merge should work but doesn't:

1. Check CI logs for test failures
2. Verify Dependabot PR requirements:
   - PR author is `dependabot[bot]`
   - CI workflow ("Only CI/CT") completed
   - All tests passed (no failures, no cancellations)
3. Check workflow permissions (contents: write, pull-requests: write)
4. Manually merge if needed: `gh pr merge <pr-number> --squash`
