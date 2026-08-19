---
type: Learning
description: Inspect the resolved framework API before selecting version-sensitive security factories.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-3-security.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Spring Security minor versions exposed a versioned Argon2 compatibility factory rather than the assumed name. Check the resolved dependency and compile against the actual API instead of relying on a remembered framework method.
