---
type: Learning
description: Raw capacity share and normalized worker utilization are separate fairness measures.
sources:
  - id: plan
    resource: .ai/archive/phase-3-4-hybrid-worker-load-distribution-and-cli-compatibility-closeout.plan.md
generated: { by: itx-code, at: "2026-08-26" }
status: stable
---

A worker with twice the configured capacity should complete proportionally more raw runs while its busy-slot-seconds divided by capacity remains approximately balanced with an equal peer. Fairness assertions must name the unit and compare throughput and normalized occupancy independently.
