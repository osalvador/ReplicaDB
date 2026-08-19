---
type: Learning
description: Separate positive persistence assertions from bean-replacement boundary tests when the mocked dependency is itself the assertion surface.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-3c-audit-events-and-cancellation-warning.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Replacing the audit repository or run coordinator in a shared Spring test context removed the real bean needed by positive-path assertions. Use narrow contexts for fail-open and race tests, and retain real-table assertions in the primary integration suite.
