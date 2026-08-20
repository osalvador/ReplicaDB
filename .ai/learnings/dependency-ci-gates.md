---
type: Learning
description: Dependency automation must merge only after successful CI for the exact current pull request head.
sources:
  - id: auto-merge
    resource: .github/workflows/dependabot-auto-merge.yml
  - id: ci-workflow
    resource: .github/workflows/CT_Push.yml
  - id: rebase-workflow
    resource: .github/workflows/rebase-dependabot-prs.yml
generated: { by: itx-init/2.1, at: "2026-08-20T12:55:57Z" }
status: stable
---

The previous auto-merge guard could treat a `pull_request_target` event or an empty check list as validation. That allowed a race between PR creation and CI completion. The safe boundary is a completed `workflow_run` for a pull request with a successful conclusion, an open Dependabot PR, and an exact head-SHA match. Missing checks must wait, never pass.

Manual rebase automation should query open Dependabot PRs dynamically. Fixed historical PR-number lists become stale and can report that work was processed when current dependency updates were never touched. Workflows that ignore paths must also be considered when deciding whether a dependency PR has meaningful validation.