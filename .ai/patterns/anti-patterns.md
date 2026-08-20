---
type: Anti-Pattern
description: The project rejects abstractions and integrations that blur capability, security, lifecycle, or contract boundaries.
sources:
  - id: java-rules
    resource: .github/instructions/project-technical.instructions.md
  - id: functional-rules
    resource: .github/instructions/project-functional.instructions.md
  - id: frontend-rules
    resource: .github/instructions/frontend.instructions.md
  - id: decisions
    resource: ARCHITECTURE_DECISIONS.md
  - id: admin-page
    resource: replicadb-server/frontend/src/pages/JobPermissionsPage.tsx
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

Do not place vendor branches in generic orchestration, share mutable JDBC connections across tasks, treat a frontend-hidden control as authorization, hand-copy OpenAPI DTOs, or infer universal manager support from one test. Do not turn counters into resume semantics, advance watermarks before a successful merge, or reset failed run rows into a new attempt.

Do not introduce a generic resource-table abstraction for incompatible one-off screens, add application code during context generation, commit resolved credentials or machine-specific registries, or use broad test selectors and shared fixture mutation without a proven boundary. Do not let workers write run state without the current lease token, compare application timestamps with PostgreSQL eligibility time, or remove a deprecated repository bridge before all production callers have moved to the port.

These are project-specific prohibitions; the missing organization/AMIGA baseline has not been compared in this session.
