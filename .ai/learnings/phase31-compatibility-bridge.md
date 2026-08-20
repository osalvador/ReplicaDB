---
type: Learning
description: Repository contract migrations need a bounded compatibility bridge while Spring callers and integration fixtures move to the new port.
sources:
  - id: plan
    resource: .ai/archive/phase-3-1-distributed-state-contract.plan.md
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

Phase 3.1 introduced `JobRunStore` and token-fenced operations before every existing test and API fixture had moved from `claimById`, `claimNextPending`, and un-fenced finalizers. Removing the old methods immediately would have blocked the ordered migration and obscured whether the new production path worked.

Keep a clearly deprecated adapter bridge during a sequential contract migration, mark the production Spring constructor explicitly when test-only overloads exist, and remove the bridge once the next runtime boundary owns all callers.
