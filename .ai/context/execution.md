## Orchestration
| Component | Responsibility | Contract |
| --- | --- | --- |
| `ReplicaDB` | Parse-independent run orchestration, logging, Sentry transaction, cleanup | `processReplica(ToolOptions)` returns `0` or `1` |
| `ReplicaTask` | One callable source-to-sink transfer | Creates one source and one sink manager per task and returns its task id |
| `ManagerFactory` | Select managers from source/sink connection schemes | Falls back to `StandardJDBCManager` for unknown JDBC schemes |
| `ReplicationMode` | Names `complete`, `incremental`, and `complete-atomic` | Mode text is part of the CLI/property contract |

## Execution Flow
`ToolOptions` is constructed first. `ReplicaDB` then resets file temp state, initializes telemetry, creates source and sink managers, runs source pre-tasks and asynchronous sink pre-tasks, submits `jobs` `ReplicaTask` instances to a fixed pool, waits for completion, runs post-tasks, and closes managers and executors in `finally` cleanup.

Each task sets its thread name to `TaskId-N`, opens its own source and sink connections, reads a `ResultSet`, inserts it, and closes both managers. The orchestrator observes `Future` failures and returns a non-zero result; cleanup is attempted even after an exception. The code does not explicitly cancel sibling futures, so new changes must not assume fail-fast cancellation.

## Modes and State
- **Complete** targets the sink table directly and normally truncates it before loading.
- **Incremental** writes to a staging table and merges into the sink; the sink primary key is required by SQL managers.
- **Complete-atomic** uses staging and a merge/rename path where the manager supports it.
- Staging names can be user supplied or generated. The staging cleanup specification requires generated tables to be droppable while user-defined staging tables are preserved.

## Invariants
- The core moves data; it is not a transformation or scheduler layer.
- Source and sink lifecycle hooks belong to `ConnManager` implementations.
- A task must not share a JDBC connection with another task.
- Any new mode capability or limitation must be implemented and documented in the relevant manager, not inferred globally.

## Reference Implementations
- `src/main/java/org/replicadb/ReplicaDB.java`
- `src/main/java/org/replicadb/ReplicaTask.java`
- `src/main/java/org/replicadb/cli/ReplicationMode.java`
- `src/main/java/org/replicadb/manager/ConnManager.java`
- `openspec/specs/staging-table-cleanup/spec.md`
