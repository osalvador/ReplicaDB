---
type: Learning
description: Include framework test modules explicitly when tests use security or MVC-specific helpers.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-3-security.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The existing test classpath did not provide Spring Security CSRF request processors. Dependency inventories must cover test-scoped framework modules, not only production starters, whenever the validation strategy uses their helpers.
