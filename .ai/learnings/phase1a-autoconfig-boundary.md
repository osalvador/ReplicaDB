---
type: Learning
description: Inspect transitive auto-configuration before adding a Spring context around a broad sibling artifact.
sources:
  - id: plan
    resource: .ai/archive/phase-1a-artifact-split.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The server's dependency on the broad CLI artifact caused Mongo auto-configuration to attempt a local database before the server state layer existed. Review transitive starters and explicitly exclude unrelated auto-configuration at the runtime boundary until the corresponding feature is configured.
