---
type: Learning
description: Use explicit test class lists when Surefire wildcard expansion is not proven for this repository.
sources:
  - id: plan
    resource: .ai/archive/phase-1a-artifact-split.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

A slash-based package wildcard selected unrelated integration tests and vendor containers. For a focused regression, name the intended classes explicitly, then widen to the full suite only after the focused behavior is known.
