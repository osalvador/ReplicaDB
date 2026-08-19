---
type: Pattern
description: A per-run execution context coordinates counters, temporary resources, active statements, and cooperative cancellation.
sources:
  - id: context
    resource: src/main/java/org/replicadb/execution/ReplicationExecutionContext.java
  - id: task
    resource: src/main/java/org/replicadb/ReplicaTask.java
  - id: orchestrator
    resource: src/main/java/org/replicadb/ReplicaDB.java
  - id: tests
    resource: src/test/java/org/replicadb/execution/ReplicationExecutionContextConcurrencyTest.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Each `ToolOptions` instance owns a `ReplicationExecutionContext`. Parallel tasks share that run context but not mutable connection ownership. The context stores a run ID, concurrent temporary-file paths, counters, watermark candidate, cancellation token, and active JDBC statements.

Cancellation sets the token and invokes `Statement.cancel()` for registered statements. Tasks also observe the token in copy and lifecycle paths. The orchestrator individually waits on futures, cancels siblings when needed, and closes the executor on failures that occur before ownership is returned.

Reference implementations: `ReplicationExecutionContext.java`, `ReplicaDB.java`, and `ReplicaTask.java`.
