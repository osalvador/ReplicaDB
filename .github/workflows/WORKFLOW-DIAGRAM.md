# Dependabot Auto-Merge Workflow Diagram

## Process Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Dependabot Creates/Updates PR                    │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                 Trigger: pull_request_target Event                       │
│                 • Event: opened, synchronize, reopened                   │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Step 1: Get PR Details                                │
│                    • Extract PR number and SHA                           │
│                    • Verify PR author is dependabot[bot]                 │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │ Is Dependabot PR?       │
                    └──────┬──────────┬───────┘
                          NO          YES
                           │           │
                           ▼           ▼
                    ┌──────────┐   ┌──────────────────────────────────────┐
                    │   Exit   │   │ Step 2: Check CI Status              │
                    └──────────┘   │ • Get combined status                │
                                   │ • Get check runs                      │
                                   │ • Validate checks exist               │
                                   └──────────────┬───────────────────────┘
                                                  │
                                     ┌────────────┴────────────┐
                                     │ All Checks Passed?      │
                                     └──┬──────────┬──────┬───┘
                                    FAIL  PENDING  PASS
                                     │       │      │
                                     ▼       ▼      ▼
                        ┌─────────────┐  ┌──────┐  ┌────────────────────┐
                        │Report Failed│  │ Wait │  │Step 3: Approve PR  │
                        │   Checks    │  │      │  │• Check existing    │
                        └─────────────┘  └──┬───┘  │  approvals         │
                                            │      │• Create approval    │
                                            │      └──────────┬─────────┘
                                            │                 │
                                            │                 ▼
                                            │      ┌────────────────────┐
                                            │      │Step 4: Merge PR    │
                                            │      │• Squash merge      │
                                            │      │• Descriptive msg   │
                                            │      └──────────┬─────────┘
                                            │                 │
┌───────────────────────────────────────────┼─────────────────┤
│                                           │                 │
│                                           ▼                 ▼
│    ┌──────────────────────────────────────────────────────────┐
│    │              CI Tests Complete                            │
│    │              "Only CI/CT" Workflow Finishes               │
│    └────────────────────────────┬─────────────────────────────┘
│                                 │
│                                 ▼
│    ┌─────────────────────────────────────────────────────────┐
│    │              Trigger: workflow_run Event                 │
│    │              • Event: completed                          │
│    │              • Workflow: "Only CI/CT"                    │
│    └────────────────────────────┬────────────────────────────┘
│                                 │
│                                 ▼
│    ┌─────────────────────────────────────────────────────────┐
│    │           Find Associated PR from Workflow Run          │
│    │           • Match by head branch                        │
│    │           • Filter for Dependabot PRs                   │
│    └────────────────────────────┬────────────────────────────┘
│                                 │
└─────────────────────────────────┘
```

## Decision Points

### 1. Is Dependabot PR?
- **YES**: Continue to check CI status
- **NO**: Exit workflow (do nothing)

### 2. All Checks Passed?
- **PASS**: Approve and merge PR
- **FAIL**: Report failure, do not merge
- **PENDING**: Wait for workflow_run trigger

### 3. Validation Checks
Before merge, the workflow verifies:
- ✅ All status checks successful
- ✅ All check runs completed with success/skipped
- ✅ No failed or cancelled checks
- ✅ At least some form of validation exists

## Event Flow Examples

### Example 1: New Dependabot PR (Happy Path)
```
1. Dependabot creates PR → pull_request_target trigger
2. Workflow identifies Dependabot PR
3. CI tests pending → workflow waits
4. CI tests complete → workflow_run trigger
5. Workflow checks status (all passed)
6. Workflow approves PR
7. Workflow merges PR (squash)
✅ PR closed and merged
```

### Example 2: Dependabot PR with Test Failure
```
1. Dependabot creates PR → pull_request_target trigger
2. Workflow identifies Dependabot PR
3. CI tests pending → workflow waits
4. CI tests complete with failures → workflow_run trigger
5. Workflow checks status (found failures)
6. Workflow reports failed checks
❌ PR remains open (manual review required)
```

### Example 3: Non-Dependabot PR
```
1. User creates PR → pull_request_target trigger
2. Workflow checks PR author
3. Author is not dependabot[bot]
⏭️ Workflow exits (no action taken)
```

## Safety Mechanisms

### Layer 1: Author Verification
```javascript
if (pullRequest.user.login !== 'dependabot[bot]') {
  return; // Exit workflow
}
```

### Layer 2: CI Validation
```javascript
const ready_to_merge = 
  allStatusPassed &&      // Status checks passed
  allChecksPassed &&      // Check runs passed
  !hasFailedChecks &&     // No failed checks
  hasValidation;          // Some validation exists
```

### Layer 3: Duplicate Prevention
```javascript
// Check if already approved
const alreadyApproved = reviews.some(review => 
  review.state === 'APPROVED' && 
  review.user.login === 'github-actions[bot]'
);

if (alreadyApproved) return;
```

### Layer 4: Merge Verification
```javascript
// Check if already merged
if (pullRequest.merged) {
  return; // Skip merge attempt
}
```

## Timing Considerations

### Status Update Delay
After CI completion, GitHub needs time to update check status:
```javascript
const STATUS_UPDATE_DELAY_MS = 5000;
if (context.eventName === 'workflow_run') {
  await new Promise(resolve => setTimeout(resolve, STATUS_UPDATE_DELAY_MS));
}
```

### Typical Timeline
```
T+0:00  → Dependabot creates PR
T+0:01  → pull_request_target triggers auto-merge workflow
T+0:02  → CI tests start (Only CI/CT workflow)
T+15:00 → CI tests complete (typical duration)
T+15:01 → workflow_run triggers auto-merge workflow
T+15:06 → Status checks validated (5s delay)
T+15:07 → PR approved
T+15:08 → PR merged
```

## Integration Points

### GitHub Events
- `pull_request_target`: PR creation/updates
- `workflow_run`: CI completion

### GitHub APIs Used
- `pulls.list`: Find PRs by branch
- `pulls.get`: Get PR details
- `repos.getCombinedStatusForRef`: Get status checks
- `checks.listForRef`: Get check runs
- `pulls.listReviews`: Check existing approvals
- `pulls.createReview`: Approve PR
- `pulls.merge`: Merge PR (squash)

### Workflow Dependencies
- Depends on: "Only CI/CT" workflow (CT_Push.yml)
- Must exist on default branch (master)
- Requires proper permissions configuration

## Monitoring Points

### Success Indicators
- ✅ Workflow run completes successfully
- ✅ PR shows approved review from github-actions[bot]
- ✅ PR merged with squash commit
- ✅ Commit message includes attribution

### Failure Indicators
- ❌ Workflow errors in logs
- ❌ PR remains open after CI passes
- ❌ Multiple approval attempts
- ❌ Merge conflicts or permission errors
