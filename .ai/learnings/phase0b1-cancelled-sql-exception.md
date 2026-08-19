---
type: Learning
description: Classify cancellation with both the cooperative run token and the thrown driver exception.
sources:
  - id: plan
    resource: .ai/archive/phase-0b1-cancellation-plumbing.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

JDBC drivers can report a statement cancellation as an ordinary `SQLException`, including during merge or another blocking call. When the execution context is already cancelled, normalize that failure to the cancelled result while preserving the original cause.
