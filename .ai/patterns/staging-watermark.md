---
type: Pattern
description: Staging modes and committed watermarks provide retry safety without partition checkpoints.
sources:
  - id: decisions
    resource: ARCHITECTURE_DECISIONS.md
  - id: binder
    resource: src/main/java/org/replicadb/manager/util/WatermarkBinder.java
  - id: context
    resource: src/main/java/org/replicadb/execution/ReplicationExecutionContext.java
  - id: tests
    resource: src/test/java/org/replicadb/ReplicaDBWatermarkCommitTest.java
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

`complete-atomic` and `incremental` use manager-owned staging or merge paths so a full retry can avoid the destructive behavior of direct `complete` loads. Incremental selection injects a typed `>` predicate and binds the stored watermark through source metadata rather than concatenating it into SQL.

Tasks report watermark candidates; the run reduces them and the managed service commits the value only after the sink merge succeeds. Failed, cancelled, or stale token-fenced runs do not advance the committed boundary. The durable run finalizer writes the terminal state and candidate together under the current lease token. Bind order and pagination arguments remain manager-specific.

Reference implementations: `WatermarkBinder.java`, `ReplicaDB.java`, `JobExecutionService.java`, and `ReplicaDBWatermarkCommitTest.java`.
