---
type: Decision
description: Cancellation is delivered immediately through a per-run token and active-statement cancellation, with persisted sink-risk warnings.
sources:
  - id: decision
    resource: ARCHITECTURE_DECISIONS.md
  - id: context
    resource: src/main/java/org/replicadb/execution/ReplicationExecutionContext.java
  - id: durable-intent
    resource: replicadb-server/src/main/java/org/replicadb/server/job/application/RunCancellationService.java
  - id: core-test
    resource: src/test/java/org/replicadb/ReplicaDBCancellationTest.java
  - id: server-test
    resource: replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunCancellationRaceTest.java
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

Driving force: operators need a direct stop action even when a JDBC call or merge is in progress.

Decision: cancellation sets the run token, calls `Statement.cancel()` for active statements, interrupts task futures, and maps cancellation-related driver failures to the cancelled result. The API first persists `CANCEL_REQUESTED` and then attempts the local signal, so the durable state remains authoritative when no execution is registered in the current instance. Expired cancellation wins over automatic retry, and the API returns and persists a warning that sink consistency depends on mode and cancellation point.

Trade-off: the contract prioritizes responsiveness over a universal safe point. Cleanup remains responsible for generated staging resources, while user-provided staging tables are preserved. PostgreSQL `COPY` and SQL Server bulk cancellation remain best effort. Remote cancellation delivery and a worker heartbeat are deferred to Phase 3.2.
