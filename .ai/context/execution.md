## Core Orchestration
| Component | Responsibility | Contract |
| --- | --- | --- |
| `ReplicaDB` | Run lifecycle, telemetry, task submission, result aggregation, cleanup | `processReplica(ToolOptions)` returns success, failure, or cancellation exit code |
| `ReplicaTask` | One source-to-sink callable | owns one source/sink manager pair and returns `ReplicaTaskResult` |
| `ReplicationExecutionContext` | Per-run id, cancellation, active statements, temp files, counters, watermark candidate | shared by tasks from one `ToolOptions`; isolated across runs/tables |
| `ManagerFactory` | Scheme-to-manager dispatch | specialized manager first, standard JDBC fallback where applicable |

`ToolOptions` is created first. The core creates managers, executes source/sink hooks, submits a fixed pool of `jobs` tasks, waits on individually cancellable futures, runs post-hooks, and releases resources in cleanup. Task threads use `TaskId-N`; connections are never shared between tasks.

## Modes and Cancellation
- `complete` truncates/direct-loads and is destructive when interrupted.
- `incremental` stages, merges by sink keys, and commits the watermark only after merge success.
- `complete-atomic` stages and swaps where the manager supports it.
- Cancellation combines a per-run token, active JDBC statement cancellation, loop checks, and normalized driver failures. Cleanup is still required; sibling futures are not assumed to fail-fast cancel.

## Managed Execution
`RunExecutionCoordinator` claims one pending PostgreSQL row, registers the live `ToolOptions` in an in-flight map, and delegates to `JobExecutionService`. The service resolves environment references, builds core arguments, maps exit codes to `JobRunStatus`, persists counters/watermarks, and redacts failure text. Quartz triggers claim-and-submit work; it does not execute replication on scheduler threads. `ScheduleReconciler` rebuilds Quartz state from the durable schedule table at startup.

## Invariants
- The core moves data; scheduling, persistence, authorization, and audit stay outside it.
- Resource ownership must close locally on failure before a resource-returning method can transfer ownership.
- A managed retry is a new run, never a reset of the failed row; a cancelled run never advances its watermark.

## Reference Implementations
- `src/main/java/org/replicadb/ReplicaDB.java`
- `src/main/java/org/replicadb/ReplicaTask.java`
- `src/main/java/org/replicadb/execution/ReplicationExecutionContext.java`
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java`

## Recent Learnings
- [WARNING] Cancellation classification must use both the cooperative token and the thrown exception because JDBC drivers differ after `Statement.cancel()`. Source: `phase-0b1-cancellation-plumbing`.
- [WARNING] A resource returned to the caller still needs local cleanup on exceptions before the return point. Source: `phase-0b1-cancellation-plumbing`.
