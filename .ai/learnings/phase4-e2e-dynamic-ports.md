---
type: Learning
description: Pattern-match dynamically allocated local resources in end-to-end assertions.
sources:
  - id: plan
    resource: .ai/archive/phase-4-reusable-managed-datasources-with-encrypted-credentials.plan.md
generated: { by: itx-code, at: "2026-09-01T09:49:02Z" }
status: stable
---

The local browser harness selects an available PostgreSQL port to avoid collisions, while a datasource assertion assumed the default port.

When test infrastructure allocates ports or paths dynamically, assert the stable resource shape or derive the value from the harness rather than hard-coding a default.
