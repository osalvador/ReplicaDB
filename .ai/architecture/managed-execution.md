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
  - id: scheduler
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduleReconciler.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`RunExecutionCoordinator` claims a pending run and tracks in-flight execution. `JobExecutionService` loads the definition, finds the prior committed watermark, resolves environment references, writes a temporary options file, constructs `ToolOptions`, and calls `ReplicaDB.processReplica`. It maps the core exit code to a run status and persists counters, duration, errors, and a successful watermark candidate. The temporary options file is deleted in a finally path.

Quartz integration creates stable schedule identities, reconciles enabled database schedules into the runtime scheduler, and inserts pending runs through the same coordinator. Scheduled execution and manual trigger therefore share the run repository and overlap constraints.

Audit records are emitted for terminal outcomes. Failure details pass through credential redaction before persistence or audit. Phase 3 distributed workers and PostgreSQL notification dispatch are documented decisions, not current execution code.

Reference implementations: `JobExecutionService.java`, `RunExecutionCoordinator.java`, `QuartzScheduleService.java`, and `ScheduleReconciler.java`.
