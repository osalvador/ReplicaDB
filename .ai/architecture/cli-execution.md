---
type: Use Case
description: The CLI parses configuration, prepares source and sink managers, executes parallel tasks, aggregates results, and performs cleanup.
sources:
  - id: orchestrator
    resource: src/main/java/org/replicadb/ReplicaDB.java
  - id: options
    resource: src/main/java/org/replicadb/cli/ToolOptions.java
  - id: task
    resource: src/main/java/org/replicadb/ReplicaTask.java
  - id: result
    resource: src/main/java/org/replicadb/ReplicaTaskResult.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`ReplicaDB.processReplica` chooses help/version, one-table, or multi-table execution. A single replication creates source and sink managers, starts pre-sink work, submits one `ReplicaTask` per configured job, waits on individually cancellable futures, reduces rows/duration/watermark candidates, runs post-sink work, and cleans resources. Exit codes are success `0`, error `1`, and cancelled `2`.

A multi-table CLI invocation creates a `ToolOptions` copy per `ReplicationTable` and processes entries sequentially, stopping at the first non-success result. This capability remains CLI-only; the managed server models one source/sink table pair per job.

The execution context carries run identity, cancellation state, active statements, temporary files, counters, and the watermark candidate. Task resources are task-owned. Failures retain the original cause where the surrounding API permits it, and interrupted threads restore their interrupted status.

Reference implementations: `src/main/java/org/replicadb/ReplicaDB.java` and `src/main/java/org/replicadb/ReplicaTask.java`.
