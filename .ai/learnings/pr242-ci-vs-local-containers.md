---
type: Learning
description: Distinguish clean CI container assumptions from local socket, reuse, architecture, and memory constraints.
sources:
  - id: plan
    resource: .ai/archive/PR-242.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Local Docker Desktop behavior could not reproduce the CI matrix for every vendor. Record the daemon and architecture assumptions, run isolated local slices where possible, and classify remaining image/resource failures as infrastructure rather than assertion failures.
