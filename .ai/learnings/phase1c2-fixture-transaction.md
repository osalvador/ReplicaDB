---
type: Learning
description: Use one explicit transaction for large integration fixtures so setup time does not consume scheduler timing budgets.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-2-quartz-scheduler.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

A 50,000-row SQLite fixture created with auto-commit consumed almost the entire scheduled-fire timeout. Batch large fixture setup in one transaction and measure setup separately from the behavior under test.
