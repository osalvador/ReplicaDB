---
type: Learning
description: Documentation paths in implementation plans must be resolved against the repository rather than inferred from module names.
sources:
  - id: plan
    resource: .ai/archive/phase-3-1-distributed-state-contract.plan.md
generated: { by: itx-code/1.0, at: "2026-08-20T10:14:01Z" }
status: stable
---

The plan named `replicadb-server/README.develop.md`, but this checkout maintains the development guide at `replicadb-server/frontend/README.develop.md`. The Phase 3.1 runtime documentation was added to the existing guide and the archived task path was corrected.

Resolve documentation paths with a repository file search during planning, especially in monorepos where build and frontend documentation live in different module directories.
