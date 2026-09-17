---
type: Learning
description: Known implementation and documentation limitations that should remain visible without being treated as approved patterns.
sources:
  - id: decisions
    resource: ARCHITECTURE_DECISIONS.md
  - id: admin-page
    resource: replicadb-server/frontend/src/pages/JobPermissionsPage.tsx
  - id: admin-e2e
    resource: replicadb-server/frontend/e2e/admin-management.spec.ts
  - id: config
    resource: replicadb-server/src/main/resources/application.yml
  - id: phase33-plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

| Source | Description | Impact |
| --- | --- | --- |
| Architecture decisions | Phase 3.3 distributed workers, PostgreSQL dispatch, Quartz clustering, shared throttling, operational validation, and Phase 3.4 hybrid worker load distribution are implemented. | Fairness remains probabilistic; do not present it as strict round-robin assignment or perfect equality. |
| Architecture decisions | Incremental replication has one watermark column, no delete propagation, and late-commit limitations. | Product behavior remains upsert-oriented and source-tuning dependent. |
| Frontend admin page | The permission user picker requests a bounded page instead of server-side search. | Larger user populations need a search contract before the picker is complete. |
| Application configuration | Quartz now uses PostgreSQL JDBC clustering; missed fires still follow the configured `DO_NOTHING` policy and mixed RAM/JDBC ownership is prohibited. | Keep migration handoff and missed-fire alerting in deployment operations. |
| Frontend E2E | Admin browser coverage is credential-gated by environment variables. | Missing bootstrap configuration produces a skipped flow rather than product evidence. |
| Architecture documentation | Historical phase narratives retain the implementation-time state of earlier slices, while the current roadmap is maintained in the post-Phase 4 section. | Read phase retrospectives as history; use the current roadmap table for scheduling decisions. |
| Phase 3.1 plan | The deprecated repository bridge was removed after production callers migrated to the state ports and token-aware services. | Keep the CI signature guard so the compatibility surface does not return. |
| Phase 3.1 plan | Expired-run recovery, worker polling, heartbeat, and remote cancellation are now implemented and covered by process-level validation. | Keep recovery as new-attempt-from-start semantics; it is not resume. |
| Phase 3.1 validation | The local ARM64 host could not complete the full cross-database CLI suite because DB2 amd64 emulation failed during startup, but the exact matrix passed in GitHub Actions. | Treat local architecture limits as environment risk and retain the Linux CI gate. |
| Post-Phase 4 roadmap | Decisions 9-12 are planned but not implemented. Decision 13 is partially implemented through bounded logs and core diagnostics; termination reasons and the final detail contract remain. | Keep these capabilities separate from the completed Phase 0-4 foundation and schedule them explicitly. |
| Cloud Run frontend | The public frontend path is opt-in and its final disposable-deployment acceptance remains open. | Keep the API as the only public component; workers and PostgreSQL remain private. |
