---
type: Learning
description: Explicit OpenAPI responses are required for empty destructive endpoints.
sources:
  - id: plan
    resource: .ai/archive/physical-job-deletion.plan.md
generated: { by: itx-code, at: "2026-09-03T21:10:00+02:00" }
status: draft
---

Springdoc may infer a DELETE method's empty response as 200 and omit globally handled error statuses even when the runtime returns 204, 403, 404, and 409. Add explicit response annotations, regenerate the TypeScript schema from a live API, and protect the generated response keys with compile-time schema assertions.
