# Dependabot Auto-Merge Investigation Summary

**Date**: February 14, 2026  
**Issue**: Why aren't Dependabot PRs being auto-merged?  
**Status**: ✅ RESOLVED - No workflow changes needed

## TL;DR

**The auto-merge workflow is working correctly!** Dependabot PRs aren't being merged because they have genuine test failures due to a Docker/TestContainers compatibility issue. The fix is simple: rebase the Dependabot PRs to pick up the TestContainers 1.21.4 update that's already in master.

## Key Findings

### 1. Auto-Merge Workflow Status: ✅ WORKING
The `dependabot-auto-merge.yml` workflow is functioning exactly as designed:
- Correctly identifies Dependabot PRs
- Waits for CI tests to complete
- Detects test failures
- **Correctly refuses to merge PRs with failed tests**

### 2. Root Cause: Docker API Incompatibility
All 11 open Dependabot PRs are failing with the same error:
```
BadRequestException (Status 400: client version 1.32 is too old. 
Minimum supported API version is 1.44)
```

**Why this happened:**
- GitHub Actions runners recently upgraded to Docker 29+ (requires API v1.44)
- TestContainers 1.21.3 uses Docker API v1.32 (incompatible)
- Old Dependabot PRs were created before the TestContainers 1.21.4 fix was merged
- These PRs still reference the old TestContainers version

**Good news:** Master branch already has TestContainers 1.21.4, which fixes this!

### 3. Affected Dependabot PRs
All 11 open Dependabot PRs are affected:
- PR #233: Jekyll dependency update
- PR #232: actions/setup-java v4 → v5
- PR #231: docker/setup-buildx-action v1 → v3
- PR #230: rake dependency update
- PR #229: docker/login-action v1 → v3
- PR #228: docker/setup-qemu-action v1 → v3
- PR #227: actions/upload-artifact v4 → v6
- PR #226: postgresql JDBC driver update
- PR #225: gson library update
- PR #224: testcontainers 1.21.3 → 1.21.4 ⭐ (contains the fix!)
- PR #223: Oracle XML parser update
- PR #222: Apache Commons CSV update

## Solution: Rebase Dependabot PRs

### Automated Solution (Recommended) ⭐
Use the provided GitHub Actions workflow to automatically comment on all Dependabot PRs:

**Steps**:
1. Go to the **Actions** tab in GitHub
2. Select "**Rebase Dependabot PRs**" workflow
3. Click "**Run workflow**" button
4. Wait for completion (takes ~15 seconds)

The workflow will:
- Comment `@dependabot rebase` on all 12 open Dependabot PRs
- Verify PRs are still open and from Dependabot
- Provide detailed logs and summary

See `scripts/README.md` for complete documentation.

### Manual Alternative
If you prefer to do it manually, comment on each open Dependabot PR:
```
@dependabot rebase
```

Or use the bash script: `./scripts/rebase-dependabot-prs.sh`

### What Happens After Rebase
1. Dependabot rebases each PR on current master (which has TestContainers 1.21.4)
2. CI tests are triggered (which should now pass)
3. Auto-merge workflow automatically merges the PR once tests pass

## Technical Details

### Test Failure Analysis
From PR #232 logs:
```
2026-02-12 16:52:56,154 ERROR DockerClientProviderStrategy:266 
Could not find a valid Docker environment. Please check configuration. 
Attempted configurations were:
  UnixSocketClientProviderStrategy: failed with exception 
  BadRequestException (Status 400: client version 1.32 is too old. 
  Minimum supported API version is 1.44)
```

**7 test failures** in non-integration tests:
- LocalStackContainerTest
- Csv2DB2Test
- Csv2PostgresTest  
- Csv2SqlserverTest
- OracleManagerNullHandlingTest
- PostgresqlBinaryCopyBenchmarkTest (2 tests)

**Multiple failures** in integration tests across all databases (db2, mariadb, mysql, oracle, postgres, sqlite, sqlserver, mongo).

### Why Master Branch Works
Master branch CI runs (e.g., run #21999845738) succeed because:
- Uses TestContainers 1.21.4 (compatible with Docker API 1.44)
- All integration tests pass
- Package job completes successfully

### Workflow Logic Verification
Checked `dependabot-auto-merge.yml` lines 123-126:
```javascript
const hasFailedChecks = checkRuns.check_runs.some(check =>
  check.status === 'completed' &&
  (check.conclusion === 'failure' || check.conclusion === 'cancelled')
);
```

This correctly identifies failed/cancelled checks and prevents merge. ✅

## Documentation Created

Added two new troubleshooting documents:
1. `TROUBLESHOOTING-dependabot.md` - Comprehensive guide for this issue
2. This investigation summary

## Next Steps

### Immediate Action (Choose One)

**Option A - Use Automated Workflow (Recommended)** ⭐
1. Merge this investigation PR  
2. Go to **Actions** tab → "**Rebase Dependabot PRs**" workflow
3. Click "**Run workflow**"
4. Wait for all PRs to be rebased (~15 seconds)

**Option B - Use Bash Script**
1. Merge this investigation PR
2. Run: `./scripts/rebase-dependabot-prs.sh`

**Option C - Manual Comments**
Comment `@dependabot rebase` on each PR individually

See `scripts/README.md` for detailed instructions on all methods.

### After Rebasing

1. **Monitor**: Watch for CI completion on rebased PRs

2. **Verify**: Auto-merge should kick in once tests pass

3. **Future Prevention**: 
   - Regularly rebase old Dependabot PRs
   - Keep TestContainers dependency current
   - Monitor TestContainers release notes

## References

- TestContainers Issue: https://github.com/testcontainers/testcontainers-java/issues/11250
- Docker API Compatibility: https://stackoverflow.com/questions/79817033/sudden-docker-error-about-client-api-version
- TestContainers 1.21.4 Release Notes: Fixed Docker 29+ compatibility

## Conclusion

**No changes needed to the auto-merge workflow.** The workflow is correctly designed and functioning as intended. The issue is purely a TestContainers/Docker version mismatch that can be resolved by rebasing the Dependabot PRs.

The auto-merge workflow will automatically merge PRs once they pass CI tests.
