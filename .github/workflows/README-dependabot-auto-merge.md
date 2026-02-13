# Dependabot Auto-Merge Workflow

This workflow automatically reviews and merges Dependabot pull requests when their integration tests pass.

## How It Works

The `dependabot-auto-merge.yml` workflow is triggered in two scenarios:

### 1. Pull Request Created/Updated (pull_request_target)
When a Dependabot PR is opened, synchronized, or reopened, the workflow:
- Checks if the PR is from `dependabot[bot]`
- Monitors the CI status
- Waits for all checks to complete

### 2. CI Tests Complete (workflow_run)
When the "Only CI/CT" workflow completes, the workflow:
- Finds the associated PR
- Verifies all integration tests passed
- Auto-approves the PR
- Merges the PR using squash merge

## Integration Test Requirements

Before merging, the workflow ensures:
- ✅ All status checks are successful
- ✅ All check runs have completed with `success` or `skipped` conclusion
- ❌ No checks have `failure` or `cancelled` status

## What Gets Auto-Merged

The workflow automatically merges Dependabot PRs for:
- **Maven dependencies** (Java libraries)
- **Bundler dependencies** (Jekyll docs)
- **GitHub Actions** (workflow dependencies)

## Safety Features

1. **Only Dependabot PRs**: Non-Dependabot PRs are ignored
2. **CI validation**: PRs only merge if all integration tests pass
3. **Failed check detection**: PRs with failed tests are NOT merged
4. **Idempotent**: Safe to run multiple times (checks if already approved/merged)

## Workflow Permissions

The workflow requires:
- `contents: write` - To merge PRs
- `pull-requests: write` - To approve PRs

## Monitoring

Each workflow run produces a summary showing:
- PR number
- Whether ready to merge
- CI status state
- Number of check runs
- Whether any checks failed

## Manual Override

If you need to prevent auto-merge for a specific Dependabot PR:
1. Close the PR
2. Add a label (future enhancement)
3. Or manually merge with a different strategy

## Troubleshooting

### PR not merging automatically
- Check that CI tests passed in the "Only CI/CT" workflow
- Verify the PR is from `dependabot[bot]`
- Check workflow logs for errors

### Workflow not triggering
- Ensure the workflow file is on the default branch
- Check that permissions are correctly configured
- Verify Dependabot is enabled in repository settings
