---
type: Decision
description: Managed retries start from the beginning and use replication modes plus committed watermarks for safety.
sources:
  - id: decision
    resource: ARCHITECTURE_DECISIONS.md
  - id: mode
    resource: src/main/java/org/replicadb/cli/ReplicationMode.java
  - id: execution
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java
  - id: tests
    resource: src/test/java/org/replicadb/ReplicaDBWatermarkCommitTest.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Driving force: manager-specific partitioning is not reliably reproducible across executions, so a checkpoint that skips completed partitions could silently lose rows.

Decision: retries re-execute the definition. `complete-atomic` and `incremental` defer sink mutation to staging/swap or merge; incremental watermarks use one typed source column and advance only after a successful merge. Counters and timings remain observational and must not be presented as resumable progress.

Trade-offs: `complete` can leave a truncated or partially loaded sink after interruption, incremental mode does not propagate deletes, and late commits before the watermark can require configured read lag.
