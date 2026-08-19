---
type: Learning
description: Pair fail-closed bootstrap behavior with an explicit test profile strategy before adding it to context-heavy modules.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-3-security.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Existing integration contexts intentionally started without deployment bootstrap variables and an empty user table. Keep those contexts isolated with test-only configuration, while lifecycle security tests provide generated non-secret values through dynamic test properties.
