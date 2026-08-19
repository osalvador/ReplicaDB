---
type: Learning
description: Validate generated API types against serialized JSON fixtures, not only endpoint names or schema paths.
sources:
  - id: plan
    resource: .ai/archive/phase-2a-frontend-auth-monitoring.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Springdoc emitted optional properties while Java responses serialized null watermark and warning fields. Keep the generated schema as the source, but add a narrow adapter or contract assertion when actual JSON nullability differs from generated types.
