---
type: Learning
description: Observability metadata in event APIs requires exact overload assertions.
sources:
  - id: plan
    resource: .ai/archive/phase-3-4-hybrid-worker-load-distribution-and-cli-compatibility-closeout.plan.md
generated: { by: itx-code, at: "2026-08-26" }
status: stable
---

When listener routing adds a receive timestamp for latency measurement, existing tests may still stub the older event overload. Update stubs and verifications in the same contract sweep, and assert the timestamped call without relaxing UUID-only payload validation.
