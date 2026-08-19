---
type: Learning
description: Close resources locally on failure when a method transfers ownership only through a successful return.
sources:
  - id: plan
    resource: .ai/archive/phase-0b1-cancellation-plumbing.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The replication task executor was returned for caller cleanup, but exceptions before the return left the caller with a null reference. Any method that transfers an executor or similar resource through its return value needs a local failure cleanup path before rethrowing.
