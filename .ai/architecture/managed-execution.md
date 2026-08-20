---
type: Use Case
description: The managed executor claims durable runs, resolves configuration references, delegates to the core, and records outcomes.
sources:
  - id: service
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java
  - id: coordinator
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java
  - id: resolver
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/JobDefinitionEnvResolver.java
  - id: options-file
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/JobDefinitionOptionsFileWriter.java
  - id: lease
    resource: replicadb-server/src/main/java/org/replicadb/server/job/application/RunLeaseService.java
  - id: finalization
    resource: replicadb-server/src/main/java/org/replicadb/server/job/application/RunFinalizationService.java
  - id: store-port
    resource: replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java
  - id: scheduler
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduleReconciler.java
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

`RunExecutionCoordinator` submits a directed claim for the pending run created by an API or scheduler trigger and tracks only the local `ToolOptions` needed for immediate cancellation. `RunLeaseService` and `JobRunStore` perform the lease claim; `JobExecutionService` loads the definition, finds the prior committed watermark, resolves environment references, writes a temporary options file, constructs `ToolOptions`, and calls `ReplicaDB.processReplica`. `RunFinalizationService` passes the claimed opaque token to fenced progress and terminal updates. A stale worker receives a fenced result and cannot emit a second terminal audit or advance state. The temporary options file is deleted in a finally path.

Quartz integration creates stable schedule identities, reconciles enabled database schedules into the runtime scheduler, and inserts pending runs through the same coordinator. Scheduled execution and manual trigger therefore share the run repository and overlap constraints.

Audit records are emitted only after a fenced terminal update succeeds. Failure details pass through credential redaction before persistence or audit. The current `api` runtime has no heartbeat loop, expiry-recovery scheduler, worker profile, or PostgreSQL notification listener; those Phase 3.2/3.3 capabilities remain deferred. Deprecated repository wrappers remain as a bounded compatibility bridge for older fixtures and callers.

Reference implementations: `JobExecutionService.java`, `RunExecutionCoordinator.java`, `RunLeaseService.java`, `RunFinalizationService.java`, and `ScheduleReconciler.java`.
