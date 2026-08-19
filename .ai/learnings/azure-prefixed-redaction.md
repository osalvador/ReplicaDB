---
type: Learning
description: Test security key matching against complete prefixed names emitted by configuration boundaries.
sources:
  - id: plan
    resource: .ai/archive/azure-sql-authentication.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Exact leaf-key matching missed names such as source- or sink-prefixed authentication properties and diagnostic DSNs. Redaction tests should use the complete keys and output forms produced by options files, telemetry, launchers, and drivers.
