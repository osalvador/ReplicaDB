# Testing the Dependabot Auto-Merge Workflow

This guide explains how to test and verify the automated Dependabot merge workflow.

## Testing Approach

Since this workflow requires actual GitHub Actions infrastructure and Dependabot PRs, testing needs to happen in the live repository environment.

### Method 1: Wait for Dependabot PR (Recommended)

The workflow will automatically trigger when:
1. Dependabot creates a new PR
2. An existing Dependabot PR is updated
3. CI tests complete on a Dependabot PR

### Method 2: Manual Trigger Simulation

You can test the workflow logic by:

1. **Check current Dependabot PRs:**
   ```bash
   gh pr list --author "dependabot[bot]" --state open
   ```

2. **Monitor workflow runs:**
   ```bash
   gh run list --workflow=dependabot-auto-merge.yml
   ```

3. **View workflow logs:**
   ```bash
   gh run view <run-id> --log
   ```

## Expected Behavior

### Scenario 1: CI Tests Pass
1. Dependabot creates/updates PR
2. "Only CI/CT" workflow (CT_Push.yml) runs integration tests
3. All tests pass (integration + non-integration)
4. Auto-merge workflow triggers
5. Workflow approves the PR
6. Workflow merges the PR with squash

### Scenario 2: CI Tests Fail
1. Dependabot creates/updates PR
2. "Only CI/CT" workflow (CT_Push.yml) runs integration tests
3. Some tests fail
4. Auto-merge workflow triggers
5. Workflow detects failed checks
6. PR is NOT approved or merged
7. Manual review required

### Scenario 3: CI Tests Pending
1. Dependabot creates/updates PR
2. "Only CI/CT" workflow (CT_Push.yml) is still running
3. Auto-merge workflow triggers
4. Workflow detects pending checks
5. Waits for workflow_run trigger
6. Re-evaluates when tests complete

## Verification Checklist

After the workflow runs, verify:

- [ ] Workflow triggered for Dependabot PR only
- [ ] Workflow skipped non-Dependabot PRs
- [ ] CI status correctly detected (success/failure/pending)
- [ ] PR approved only after tests pass
- [ ] PR merged with squash method
- [ ] Commit message includes proper title
- [ ] Non-passing PRs were NOT merged

## Current Dependabot PRs (as of implementation)

The following PRs are currently open and will be processed by this workflow:

- #233 - jekyll requirement update (docs)
- #232 - actions/setup-java from 4 to 5
- #231 - docker/setup-buildx-action from 1 to 3
- #230 - rake requirement update (docs)
- #229 - docker/login-action from 1 to 3
- #228 - docker/setup-qemu-action from 1 to 3
- #227 - actions/upload-artifact from 4 to 6
- #226 - org.postgresql:postgresql from 42.7.2 to 42.7.9
- #225 - com.google.code.gson:gson from 2.8.9 to 2.13.2
- #224 - version.testContainers from 1.21.3 to 1.21.4
- #223 - com.oracle.database.xml:xmlparserv2 from 23.3.0.23.09 to 23.26.1.0.0
- #222 - org.apache.commons:commons-csv from 1.7 to 1.14.1

## Monitoring and Debugging

### Check Workflow Status
```bash
gh run list --workflow=dependabot-auto-merge.yml --limit 10
```

### View Detailed Logs
```bash
gh run view <run-id> --log --verbose
```

### Check PR Status
```bash
gh pr view <pr-number> --json state,statusCheckRollup,reviews
```

## Troubleshooting

### Workflow Not Triggering

**Problem:** Workflow doesn't run on Dependabot PRs

**Solutions:**
- Ensure workflow is on default branch (master)
- Check repository settings for Actions permissions
- Verify `pull_request_target` trigger is allowed

### Workflow Runs But Doesn't Merge

**Problem:** Workflow runs but PR remains open

**Solutions:**
- Check workflow logs for errors
- Verify CI tests actually passed
- Check repository settings for merge restrictions
- Ensure token has required permissions

### Permission Errors

**Problem:** "Resource not accessible by integration" errors

**Solutions:**
- Verify workflow has `contents: write` permission
- Verify workflow has `pull-requests: write` permission
- Check repository settings for Actions permissions

## Success Criteria

The workflow is working correctly when:
1. ✅ Dependabot PRs with passing tests are automatically merged
2. ✅ Dependabot PRs with failing tests remain open
3. ✅ Non-Dependabot PRs are unaffected
4. ✅ Merge commits follow proper format (squash)
5. ✅ Workflow logs show clear decision-making process

## Next Steps

After successful validation:
1. Monitor first few auto-merges
2. Adjust workflow if needed based on feedback
3. Consider adding:
   - Label-based exclusions
   - Dependency type filtering
   - Notification integration (Slack/email)
   - Merge method configuration per dependency type
