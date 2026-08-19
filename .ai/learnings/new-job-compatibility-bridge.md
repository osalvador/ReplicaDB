---
type: Learning
description: Keep a narrow compatibility bridge while a cross-layer immutable record migration is staged.
sources:
  - id: plan
    resource: .ai/archive/new-job-wizard-parity.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Nested source/sink records were introduced before repositories, mappers, execution, and tests could migrate together. Preserve limited legacy accessors or constructors only as a temporary adapter surface, then remove them in a dedicated cleanup pass.
