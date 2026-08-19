---
type: Learning
description: Validate endpoint examples against current DTO constraints and response assumptions before adding audit behavior.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-3c-audit-events-and-cancellation-warning.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The permission endpoint intended to represent revoke-all with an empty set, but `@NotEmpty` and grouped response assumptions blocked it. Check request annotations and empty/null response behavior before instrumenting audit details.
