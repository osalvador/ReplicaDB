---
type: Learning
description: Known implementation and documentation limitations that should remain visible without being treated as approved patterns.
sources:
  - id: decisions
    resource: ARCHITECTURE_DECISIONS.md
  - id: plan
    resource: implementation_plan.md
  - id: admin-e2e
    resource: replicadb-server/frontend/e2e/admin-management.spec.ts
  - id: config
    resource: replicadb-server/src/main/resources/application.yml
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

| Source | Description | Impact |
| --- | --- | --- |
| Architecture decisions | Distributed workers and PostgreSQL notification dispatch are Phase 3 work, not current execution. | Do not document multi-worker behavior as implemented. |
| Architecture decisions | Incremental replication has one watermark column, no delete propagation, and late-commit limitations. | Product behavior remains upsert-oriented and source-tuning dependent. |
| Implementation plan | The permission user picker requests a bounded page instead of server-side search. | Larger user populations need a search contract before the picker is complete. |
| Application configuration | Quartz currently uses an in-memory runtime store and relies on schedule reconciliation. | Runtime trigger bookkeeping and missed-fire behavior need explicit operational coverage. |
| Frontend E2E | Admin browser coverage is credential-gated by environment variables. | Missing bootstrap configuration produces a skipped flow rather than product evidence. |
| Architecture documentation | Some lower sections still contain pre-Phase-2c wording while earlier sections say Phase 2c is implemented. | Resolve documentation status drift before treating the document as a single current source. |
