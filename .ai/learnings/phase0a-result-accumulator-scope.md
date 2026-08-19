---
type: Learning
description: Declare the scope of accumulators before nested lifecycle blocks assemble task results.
sources:
  - id: plan
    resource: .ai/archive/phase-0a-execution-context-rich-task-result.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The task-result expansion exposed an accumulator declared inside an insert block but consumed after that block. When a result is assembled after nested lifecycle code, define the accumulator in the outer scope and assign it inside the nested block. A focused compile check should follow the change.
