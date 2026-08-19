---
type: Learning
description: Check each platform launcher independently when runtime flags affect interactive authentication.
sources:
  - id: plan
    resource: .ai/archive/azure-sql-authentication.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The Windows launcher retained an unconditional headless JVM flag after the Unix path was corrected. Review every launcher and container entry point separately when a platform-sensitive authentication flow depends on GUI availability.
