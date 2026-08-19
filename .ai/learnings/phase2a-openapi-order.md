---
type: Learning
description: Keep framework implementation types out of public OpenAPI contracts and make generated schema ordering deterministic.
sources:
  - id: plan
    resource: .ai/archive/phase-2a-frontend-auth-monitoring.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

A framework CSRF parameter leaked into the generated document and property ordering varied across environments. Use explicit public DTOs and deterministic property ordering when CI compares generated artifacts byte-for-byte.
