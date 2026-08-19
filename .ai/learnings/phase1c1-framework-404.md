---
type: Learning
description: Preserve framework-level missing-resource 404 behavior separately from application exception handling.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-1-rest-api-core.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

A catch-all RFC 7807 handler converted Spring MVC's `NoResourceFoundException` into a 500. Add explicit handlers for framework status-bearing exceptions and use generic details for malformed request bodies without echoing parser content.
