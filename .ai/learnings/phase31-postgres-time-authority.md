---
type: Learning
description: Eligibility and lease timestamps must remain database-owned when JVM-created compatibility rows are compared with PostgreSQL now().
sources:
  - id: plan
    resource: .ai/archive/phase-3-1-distributed-state-contract.plan.md
generated: { by: itx-code/1.0, at: "2026-08-20T10:14:01Z" }
status: stable
---

A legacy `insertPending` wrapper using `Instant.now()` could create a row briefly in the future relative to PostgreSQL `now()` because the insert and claim used different timing boundaries. The new explicit `availableAt` contract remains authoritative; the deprecated compatibility wrapper uses a conservative past value until callers are fully migrated.

Do not compare application timestamps with database-owned eligibility predicates in distributed state paths. Prefer a database default or a timestamp computed by PostgreSQL itself.
