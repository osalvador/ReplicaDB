---
type: Learning
description: Cross-check the actual implementation against proposed replacement blocks before marking a compatibility fix complete.
sources:
  - id: plan
    resource: .ai/archive/fix-sqlite-null-handling-test.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The planned SQLite DATE/TIMESTAMP parsing fallback was initially absent even though the proposal described it. Compare the final source and focused driver tests with the plan's concrete replacement blocks before declaring the task complete.
