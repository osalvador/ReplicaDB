---
type: Learning
description: Progress counters used as convergence signals must include blocked unknown work.
sources: [{ id: plan, resource: .ai/archive/keyring-lifecycle-status-re-encryption-and-flat-environment-configuration.plan.md }]
generated: { by: itx-code, at: "2026-09-10" }
status: stable
---

If a maintenance operation cannot process orphaned or unknown versions, its `remaining` result must still count them when zero means converged. Report processable work and unknown work separately in status, but never let an unknown row produce a false success signal.
