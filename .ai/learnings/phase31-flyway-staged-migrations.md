---
type: Learning
description: Flyway migration tests must use the resolved FluentConfiguration API and explicit targets when validating staged forward-only changes.
sources:
  - id: plan
    resource: .ai/archive/phase-3-1-distributed-state-contract.plan.md
generated: { by: itx-code/1.0, at: "2026-08-20T10:14:01Z" }
status: stable
---

Phase 3.1 initially assumed `target()` and `load()` could be chained on `Flyway`, and that the second migration assertion would apply one migration automatically. The resolved Flyway version exposes those methods on `FluentConfiguration`, and an unbounded second migration applies every pending version.

Use explicit schema targets and stage V12, V13, and V14 independently when testing backfills, constraints, and indexes. This keeps migration-count assertions meaningful and catches forward-only ordering errors.
