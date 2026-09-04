---
type: Learning
description: Validation commands and authenticated E2E helpers must follow repository topology and route ownership.
sources:
  - id: plan
    resource: .ai/archive/physical-job-deletion.plan.md
generated: { by: itx-code, at: "2026-09-03T21:10:00+02:00" }
status: draft
---

Before prescribing Maven selectors, verify whether a module is part of the root reactor; standalone modules require module-local commands. Authenticated Playwright helpers should wait for the protected-route redirect before querying controls, and should navigate to the owning page before using page-specific actions such as New job.
