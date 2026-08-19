---
type: Learning
description: Place new database tests beside the nearest existing fixture package rather than deriving the path from the database label.
sources:
  - id: plan
    resource: .ai/archive/phase-0b1-cancellation-plumbing.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The SQLite cancellation test belonged under `org.replicadb.manager`, where related SQLite manager tests already lived. Before adding a test path, inspect the neighboring fixture and package conventions and follow the existing ownership boundary.
