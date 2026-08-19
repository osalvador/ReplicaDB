---
type: Learning
description: Test serialized API values at the wire boundary instead of assuming domain enum names are public values.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-1-rest-api-core.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The API contract used lower-case mode text while Jackson's default enum binding expected upper-case constants. Keep explicit request parsing and response mapping when public values differ from Java names, and test both directions.
