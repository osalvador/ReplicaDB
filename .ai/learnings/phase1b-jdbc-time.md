---
type: Learning
description: Bind PostgreSQL temporal parameters through explicit JDBC representations at Spring repository boundaries.
sources:
  - id: plan
    resource: .ai/archive/phase-1b-state-layer.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The PostgreSQL driver rejected an untyped `Instant` for a temporal parameter. Convert to the JDBC representation expected by the target column at the repository boundary and map back to the domain type.
