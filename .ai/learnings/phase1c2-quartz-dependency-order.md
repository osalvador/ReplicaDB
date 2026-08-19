---
type: Learning
description: Include dependencies used by domain validation in the build task that first compiles the domain type.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-2-quartz-scheduler.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`JobSchedule` validation referenced Quartz before the later runtime-wiring task added the starter. Order build dependencies by compile-time use as well as runtime integration, especially when domain records validate external formats.
