---
type: Learning
description: Validate every credential-bearing field, including composite connection strings, against the secret-reference policy.
sources:
  - id: plan
    resource: .ai/archive/phase-1b-state-layer.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Restricting password fields to environment references did not prevent credentials embedded in URI user-info or query parameters. Test both scalar fields and complete connection-string forms before persisting managed definitions.
