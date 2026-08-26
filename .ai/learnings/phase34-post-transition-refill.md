---
type: Learning
description: Capacity refill signals must follow the resource transition they describe.
sources:
  - id: plan
    resource: .ai/archive/phase-3-4-hybrid-worker-load-distribution-and-cli-compatibility-closeout.plan.md
generated: { by: itx-code, at: "2026-08-26" }
status: stable
---

A completion-triggered refill emitted before a worker released its permit could observe no free capacity and lose the next generic opportunity. Emit capacity-driven refill signals after permit release and utilization refresh, and make tests model durable rows becoming ineligible after a successful claim.
