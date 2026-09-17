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
  - id: worker
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java
  - id: listener
    resource: replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListener.java
  - id: metrics
    resource: replicadb-server/src/main/java/org/replicadb/server/observability/ManagedRuntimeMetrics.java
  - id: log-capture
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/RunLogCaptureAppender.java
  - id: diagnostics
    resource: src/main/java/org/replicadb/execution/ReplicationDiagnosticCollector.java
generated: { by: itx-init/2.1, at: "2026-09-04T05:47:18Z" }
status: stable
---

`RunExecutionCoordinator` submits a directed claim for the pending run created by an API or scheduler trigger and tracks only the local `ToolOptions` needed for immediate cancellation. `RunLeaseService` and `JobRunStore` perform the lease claim; `JobExecutionService` loads the definition, finds the prior committed watermark, resolves environment references, writes a temporary options file, constructs `ToolOptions`, and calls `ReplicaDB.processReplica`. `RunFinalizationService` passes the claimed opaque token to fenced progress and terminal updates. A stale worker receives a fenced result and cannot emit a second terminal audit or advance state. The temporary options file is deleted in a finally path.

Quartz integration creates stable schedule identities, reconciles enabled database schedules into the clustered JDBC scheduler, and inserts pending runs through the same dispatch boundary. Scheduled execution and manual trigger therefore share the run repository and overlap constraints. Worker notifications carry only durable run identifiers; startup, reconnect, and periodic polling remain correctness paths when notifications are missed.

Audit records are emitted only after a fenced terminal update succeeds. Failure details pass through credential redaction before persistence, audit, or metrics. Run execution captures bounded Log4j2 output and structured core diagnostic events for the run-detail API without exposing resolved credentials. The `api` runtime remains available as a compatibility execution path; the `worker` runtime adds bounded dispatch, dedicated PostgreSQL notification listening, mandatory polling, lease heartbeat, durable cancellation delivery, and internal Actuator health/metrics. Quartz and login-throttle state are shared through PostgreSQL, while the standalone CLI remains independent.

Reference implementations: `JobExecutionService.java`, `RunExecutionCoordinator.java`, `RunLeaseService.java`, `RunFinalizationService.java`, and `ScheduleReconciler.java`.
