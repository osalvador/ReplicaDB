# Architecture Decisions and Evolution Strategy

## Executive Summary

ReplicaDB is currently a Java 17 command-line tool for heterogeneous batch data replication. The existing core owns database-specific connections, partitioning, type handling, staging, and sink lifecycle behavior.

The approved direction is to evolve the product into an operable job platform while preserving the CLI. The first product step is a control plane with API-managed jobs, scheduling, monitoring, durable execution state, and incremental watermarks.

ReplicaDB is not being extended into a CDC, ETL, schema-migration, or universal data-reconciliation platform. The control plane manages and observes the batch engine; it does not move database-specific behavior into generic orchestration code.

The control plane does not resume interrupted work. A run either completes or is executed again from the beginning. Safety comes from the staging-based replication modes, not from progress checkpoints.

**Date**: August 13, 2026
**Last decision review**: August 14, 2026
**Status**: Approved direction; Phase 0-a, Phase 0-b1, Phase 0-b2, and Phase 1a (artifact split) implemented; Phase 1b (state layer) pending
**Owner**: Development Team

---

## Core Decisions

### Decision 1: Single Codebase, Two Artifacts, CLI Compatibility

**Status**: APPROVED

ReplicaDB will remain a single codebase. The current CLI is the compatibility baseline. No separate `replicadb-core` project is extracted.

The build produces **two artifacts from the same sources**:

- `replicadb` — the existing CLI assembly. Spring Boot is never on its classpath.
- `replicadb-server` — the managed runtime, started with the `api` or `worker` profile.

A single fat artifact carrying Spring Boot into every CLI installation is rejected. It would change footprint, startup time, and dependency surface for users who never adopt the control plane, which contradicts the compatibility contract below.

This preserves:

- One implementation of manager dispatch, type mapping, staging, and lifecycle behavior.
- Existing CLI arguments, options files, launcher scripts, and exit codes.
- A gradual migration path from standalone execution to managed jobs.

The current repository does not contain Spring Boot, REST, scheduler, worker, or broker runtime code. Those are planned deployment capabilities, not current features.

```text
Current:
  CLI -> ToolOptions -> ReplicaDB core -> source/sink managers

Target:
  replicadb        (CLI)    ---------------------------> ReplicaDB core
  replicadb-server (api)    -> JobDefinition -> Executor -> ReplicaDB core
  replicadb-server (worker) -> JobRun -------> Executor -> ReplicaDB core
```

`ToolOptions` remains the configuration boundary for the existing core. The server translates a stored job definition into `ToolOptions` rather than duplicating manager behavior.

PostgreSQL is required only by the managed `api` and `worker` profiles for control-plane metadata, scheduling, leases, watermarks, and worker coordination. The standalone CLI does not require it and remains executable with its existing source and sink configuration.

#### CLI compatibility contract

The CLI is a permanent execution mode across all implementation phases and deployment models. "Compatibility" is a verifiable contract, not an aspiration. A change breaks it if it alters any of:

- Accepted arguments, options-file keys, and their semantics.
- Process exit codes.
- The ability to run with no metadata database reachable.
- The `replicadb` artifact's classpath gaining a Spring Boot application context.

#### Multi-table replication remains CLI-only

`ReplicaDB.executeMultipleReplications` iterates a list of `ReplicationTable` entries sequentially and stops at the first failure, leaving earlier tables already applied. This partial-application outcome is acceptable for an interactive CLI invocation, but it is not a state model the control plane will manage: a single run identifier cannot honestly report "three of seven tables replicated", and retrying it would re-truncate tables that already succeeded.

Therefore a managed `JobDefinition` targets **exactly one source/sink table pair**. Scheduling N tables means N job definitions, each with its own run history, watermark, and permissions. Options files containing a multi-table replication list remain fully supported through the CLI and are rejected by the API with an explicit validation error.

### Decision 2: Monolithic Control Plane First

**Status**: APPROVED

The first evolution step is a monolithic control plane around the existing batch engine. It should provide value in a single-instance deployment before introducing distributed workers.

The implementation target for Phase 1 is a Spring Boot application using the `api` profile. This profile starts the REST API and the scheduler in the same JVM. Spring Boot is not required by the existing CLI path.

#### Phase 1 scope

- REST API for job definitions and run management.
- Scheduler for recurring executions.
- Asynchronous execution so API requests do not block on replication.
- Durable job history, run state, and incremental watermarks.
- Monitoring of status, progress counters, timings, throughput, and errors.
- Existing CLI remains usable without starting the API runtime.

The API initially manages configuration and execution state. A future frontend can edit job definitions through the API without introducing a data transformation layer.

```text
                    +------------------+
                    | REST API         |
                    | Scheduler        |
                    +--------+---------+
                             |
                       JobDefinition
                             |
                             v
                    +------------------+          +---------------+
                    | Job Executor     +--------->+ State store   |
                    +--------+---------+          | Monitoring    |
                             |                    +---------------+
                             v
                    +------------------+
                    | ReplicaDB Core   |
                    +--------+---------+
                             |
                       Source and sink
```

The monitoring transport is an implementation detail. REST polling is sufficient for the first version; a push channel can be added later if operational requirements justify it.

#### Run concurrency inside one JVM

Before Phase 0-a, the core kept process-global mutable state. `ConnManager.randomSinkStagingTableName` and `FileManager.tempFilesPath` were `static` and were reset at the start of every replication in `ReplicaDB.executeSingleReplication`. Two replications running concurrently in the same JVM could overwrite each other's staging table name and temporary file map, silently corrupting data or dropping another run's staging table.

```text
  Before Phase 0-a:
  JobRun A --+                     +-- reset global state --+
          +---- same JVM ------+                       +--> collision
  JobRun B --+                     +-- reset global state --+
```

Phase 0-a, delivered in commit `c228ddc`, removes this static state and replaces it with a `ReplicationExecutionContext` owned by each `ToolOptions` instance. All managers and tasks created from one options instance share that context, while `ToolOptions.forReplicationTable(...)` creates a fresh context for each table. Generated staging names and temporary-file paths are therefore isolated across runs and remain shared within the parallel tasks of one run.

The context also provides a run identifier and a concurrent temporary-file map. Generated staging-name creation is synchronized per context, so parallel managers cannot initialize different names for the same run. The static-state reason for forcing managed execution concurrency to one is resolved by Phase 0-a. The managed profile must still wait for the remaining Phase 0 cancellation and watermark work before exposing the complete execution contract.

#### Replication mode guidance for managed jobs

Because runs are never resumed (Decision 3), the safety of a retry depends entirely on the replication mode:

| Mode | Sink during the run | Full re-execution | Commit point |
|---|---|---|---|
| `complete` | mutated from the start (`TRUNCATE` then direct writes) | destructive | none |
| `complete-atomic` | untouched until the final swap | safe | `atomicInsertStagingTable()` |
| `incremental` | untouched until the merge | safe when the merge is an idempotent upsert by primary key | `mergeStagingTable()` |

`complete-atomic` is the **recommended mode for managed jobs** and the API surfaces that recommendation when a definition is created or updated. `complete` remains available, but the API must return an explicit warning stating that an interrupted or retried run leaves the sink truncated or partially loaded, and that warning must be persisted on the job definition so it is visible in the UI.

#### State storage

PostgreSQL is the mandatory state store for the managed `api` and `worker` profiles. It owns job definitions, runs, leases, watermarks, users, permissions, audit events, and scheduler coordination. In direct CLI mode the state store is not used at all. Sentry and application logs are telemetry, not the source of truth for job state. SQLite remains suitable for isolated CLI fixtures or unit tests, but it is not a supported control-plane deployment store.

### Decision 3: Durable State, No Resume, and Incremental Watermarks

**Status**: APPROVED

The control plane distinguishes a reusable job definition from an individual execution. There is no third level.

```text
JobDefinition
  configuration reference, schedule, mode,
  one source/sink table pair, optional watermark column
        |
        v
JobRun
  status, attempt, timestamps, row counters, error,
  executor identity, lease, heartbeat, committed watermark
```

A `Checkpoint` entity was considered and removed from the model. See "No resume" below.

#### Job run states

The initial state model supports:

- `PENDING`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`
- `CANCEL_REQUESTED`
- `CANCELLED`
- `RETRY_SCHEDULED`

Retries and cancellation requests are state transitions. They must not be inferred from log messages.

#### No resume

**Status**: APPROVED as an explicit non-feature.

An interrupted run is never continued. A retry executes the job from the beginning.

This follows from how the engine partitions work, not from a wish to simplify scheduling. Partition assignment is manager-specific and is not reproducible across executions:

```text
PostgreSQL   SELECT * FROM (source-query) as T1 OFFSET ? LIMIT ?
             no ORDER BY, so the row-to-task assignment may differ
             between executions even over identical data

Oracle       ... WHERE ora_hash(rowid, jobs-1) = ?
             reproducible only while rowids remain stable
```

Skipping "already completed" partitions on a second execution would therefore drop rows silently on at least one supported source, with no error and no log entry. Correctness comes from the replication mode instead (see the mode table in Decision 2): `complete-atomic` and `incremental` leave the sink untouched until `postSinkTasks()`, so a full re-execution is safe.

The control plane persists row counters and timings for observability. It must never present them as resumable progress, and no API field may imply that a retry continues where a previous attempt stopped.

#### Incremental watermarks

Watermarks apply only to `incremental` mode and are deliberately minimal.

- The job definition declares **one source column** as the watermark column. Arbitrary expressions and multi-column watermarks are out of scope.
- The committed value is persisted as a `String` holding the highest-precision textual representation the source driver can produce. This keeps one storage format across every supported engine.
- The comparison type is **inferred from the source column metadata at run time** and used to bind the stored value back as a typed parameter. The value is never concatenated into SQL.
- The predicate is injected by the engine and composes with the existing `$CONDITIONS` partition substitution. A `source-query` may be used only if it exposes the declared watermark column.
- The initial value is explicit configuration. Absent it, the first run replicates everything the query returns.
- The boundary is `>` against the last committed value, so a run never re-reads the previous boundary row.
- Each parallel `ReplicaTask` reports its highest observed value; the executor reduces them to one candidate for the run.
- The candidate is committed **only after `mergeStagingTable()` succeeds**.
- A failed or cancelled run leaves the last committed watermark unchanged, so retrying cannot advance it twice.

Accepted limitations, which must be stated in user-facing documentation:

- **Deletes are never propagated.** The merge is an upsert by primary key.
- **Sources without primary keys cannot merge**, and the engine already warns about this.
- **Late-committing source transactions are lost.** A transaction whose timestamp precedes the committed watermark but which commits after the read will never be replicated. A configurable read lag subtracted from the committed value mitigates this; the default lag is `0` and must be tuned per source.
- No watermark is inferred from an arbitrary `source-query`. Without a declared column the job still runs as a batch job, but the control plane does not automate its watermark.

### Decision 4: API, Frontend, Scheduler, and Access Control

**Status**: APPROVED

The control plane exposes these operations:

- Create, update, disable, and delete a job definition.
- Trigger a run manually.
- Schedule recurring runs.
- Query the current run and historical runs.
- Request cancellation.
- Retry a failed run by re-executing its persisted definition.
- View watermarks, counters, timings, and failure details.
- Authenticate local users and manage their roles.
- Enforce permissions for each job through backend-controlled ACLs.
- Serve a small planning and monitoring frontend from the `api` profile.

The API must execute runs asynchronously. The current `ReplicaDB.processReplica(ToolOptions)` method returns a run-level success/error code and does not expose progress events, so the first adapter reports coarse-grained state until Phase 0 widens the task result.

#### API conventions

These are decided so that they are not re-litigated per endpoint:

- Base path is `/api/v1`. Breaking changes require a new major path segment.
- Errors use RFC 7807 `application/problem+json`, and never echo connection strings or credentials.
- Collection endpoints are paginated with `page` and `size`; default `size` is 50 and the maximum is 200.
- Triggering a run requires an `Idempotency-Key` header. A replay of the same key within 24 hours returns the originally created run instead of starting a second one.
- Session cookies are `HttpOnly`, `Secure`, `SameSite=Lax`, and every state-changing request carries a CSRF token.
- Authentication endpoints are rate limited to 5 failed attempts per 15 minutes, counted per account and per source address.

#### Credentials and secret references

Job definitions must contain configuration references, not passwords, tokens, or credential-bearing connection strings. The reference syntax is the existing environment expansion, `${env:VARIABLE}`, resolved by the executor immediately before building `ToolOptions`. Provider-prefixed references such as `${secret:<provider>/<path>#<key>}` are reserved for a later secret-manager integration and are rejected until that integration exists. A resolved secret never enters the state store, the API responses, the audit log, or a dispatch payload.

#### Identity and permissions

The first identity model uses local users stored in PostgreSQL. `ADMIN`, `OPERATOR`, and `VIEWER` are global roles, while job-level ACLs grant `VIEW`, `EDIT`, `EXECUTE`, and `CANCEL` permissions. The backend is the authority for these permissions; hiding a button in the frontend is not a security control.

### Decision 5: Immediate Cancellation

**Status**: APPROVED

`POST /api/v1/runs/{id}/cancel` stops the replication as soon as it is invoked. Cancellation is immediate and unconditional: it does not wait for a partition boundary, a batch commit, or any other safe point.

The core had no cancellation support before Phase 0-b1: there were no interrupt checks in the copy path, no access to the active statements, and `ReplicaDB.executeReplicationTasks` blocked on `invokeAll` until every task finished. Phase 0 added:

- A per-run cancellation token carried by the execution context.
- Access to the active `Statement` of each `ReplicaTask` so a control thread can call `Statement.cancel()`. `Future.cancel(true)` alone is insufficient, because a thread blocked inside a JDBC call does not observe interruption.
- Interrupt checks in the row-copy loop so a task exits promptly and still runs its cleanup.
- Replacement of the blocking `invokeAll` pattern with individually cancellable futures.

Cancellation also interrupts `mergeStagingTable()` and `atomicInsertStagingTable()` when they are already running. The endpoint's contract is obedience, not consistency.

Consequently the API response to a cancellation request must **explicitly warn that the sink may be left in an indeterminate state**. The warning is returned by the API and recorded on the run; it is not merely a frontend message. Its severity depends on the mode and on when cancellation arrives:

| Mode | Cancelled before `postSinkTasks()` | Cancelled during merge or swap |
|---|---|---|
| `incremental` | sink intact, staging discarded | partially merged rows, watermark not advanced |
| `complete-atomic` | sink intact, staging discarded | swap interrupted, sink indeterminate |
| `complete` | sink already truncated or partially loaded | not applicable |

After cancellation the engine runs its normal cleanup path and drops the staging table when it was auto-generated. A user-provided `sink-staging-table` is left untouched, consistent with existing behavior.

A cancelled run never advances the watermark and terminates in `CANCELLED`, never in `FAILED`.

This decision's core-side plumbing is implemented in Phase 0-b1 (commit `4dd4cb5`). What remains for Phase 1 is the `/api/v1/runs/{id}/cancel` endpoint itself and persisting the indeterminate-state warning on the run record; SQL Server `BulkCopy` and PostgreSQL `COPY` cancellation remain best-effort rather than immediate.

### Decision 6: PostgreSQL Worker Dispatch

**Status**: APPROVED FOR PHASE 2

Distributed workers are introduced as a second phase when a single control-plane instance cannot provide the required concurrency or isolation. PostgreSQL is both the durable source of truth and the dispatch coordination point. `LISTEN/NOTIFY` wakes workers quickly, while periodic polling is the mandatory recovery path for missed notifications.

```text
                         +----------------------+
                         | PostgreSQL state     |
                         | definitions          |
                         | runs / leases        |
                         | watermarks           |
                         | users / permissions  |
                         +----------+-----------+
                                    ^
                                    |
                      API + Quartz  |  Worker updates
                                    |
                         +----------+-----------+
                         | JobRun(PENDING)      |
                         +----------+-----------+
                                    |
                       LISTEN/NOTIFY + polling
                                    |
                         +----------v-----------+
                         | Worker execution     |
                         | claim -> ToolOptions |
                         | -> ReplicaDB core    |
                         +----------------------+
```

The distributed contract is:

- The API inserts the `JobRun` and issues `pg_notify` in the same PostgreSQL transaction.
- The notification payload contains only the durable `JobRun` identifier; never credentials or a complete configuration.
- Every worker receives the signal, but workers compete to claim work using PostgreSQL row locking.
- Workers load the job definition and last committed watermark from PostgreSQL.
- A worker claims one run at a time by default.
- Worker loss is recovered through leases, heartbeats, and polling of expired or retryable runs.
- Duplicate notifications and duplicate polling must be safe under the claim, sink idempotency, and watermark commit rules.
- The API reads status only from PostgreSQL.

`LISTEN/NOTIFY` provides no acknowledgements, replay, consumer groups, or durable per-worker delivery: it is a wake-up signal, not a work queue. The durable work item is always the `JobRun` row, so periodic polling is the mandatory recovery path and notification delivery is only an optimization. An external broker is deliberately excluded from this phase.

#### Lease and heartbeat rules

- Every lease and heartbeat timestamp is computed with PostgreSQL `now()`, never with a worker's local clock. Workers do not need synchronized clocks and clock skew cannot expire a healthy lease.
- The default heartbeat interval is 30 seconds and the default lease duration is 5 minutes, so a lease survives several missed heartbeats.
- The heartbeat keeps running during `mergeStagingTable()` and `atomicInsertStagingTable()`. Those are server-side operations that can take minutes, and a lapsed lease during a merge would let a second worker start a duplicate run.
- A run whose `lease_until` has elapsed returns to `RETRY_SCHEDULED` and becomes claimable again.

### Decision 7: Explicitly Out of Scope

The following are deliberately excluded from the current roadmap:

- **Resuming an interrupted run**: partition assignment is not reproducible across executions, so partition-level resume would lose rows silently. See Decision 3.
- **Managed multi-table jobs**: multi-table replication stays in the CLI. See Decision 1.
- **Delete propagation**: incremental merge is an upsert by primary key and never removes rows from the sink.
- **Exactly-once execution**: the model is at-least-once, made safe by claim state, idempotent merges, and watermark commit rules.
- **CDC and real-time replication**: this requires log readers, offsets, ordering, delete capture, and backpressure that are different from batch execution.
- **Universal validation or reconciliation**: source data may change during a run, queries may be non-repeatable, and heterogeneous databases do not provide one equivalent consistency or comparison model. Operational row counters are not proof of source/sink equality.
- **Schema evolution, mappings, and ETL transformations**: ReplicaDB remains a transport tool. Query-specific conversions stay in `source-query` for now. A future frontend may edit job configuration, but it will not add a transformation engine through this architecture decision.

---

## Implementation Phases

### Phase 0: Core and State Foundation

This phase prepares the existing core for managed execution without adding an HTTP server. Most of its cost is **inside the engine**, not in the state layer: the control plane cannot honestly offer watermarks or cancellation until the core supports them. Direct CLI execution remains independent of the managed state store.

#### Phase 0-a: Per-run execution context and rich task results — IMPLEMENTED

Delivered in commit `c228ddc` and covered by focused JUnit tests, concurrency tests, orchestration regressions, and the successful `Only CI/CT` workflow:

- Replaced the static generated staging-table name and temporary-file map with `ReplicationExecutionContext`, owned by `ToolOptions`.
- Preserved sharing of generated staging state across parallel tasks in one run while isolating independent runs and multi-table `ToolOptions` copies.
- Added a run identifier and a `ConcurrentHashMap`-backed temporary-file registry to the context.
- Made generated staging-name initialization safe when multiple managers first access the same run context concurrently.
- Widened `ReplicaTask` from `Callable<Integer>` to `Callable<ReplicaTaskResult>`.
- Added row counts, start/finish timestamps, duration calculation, and a reserved nullable watermark candidate to the task result.
- Added run-level aggregation for total rows, task count, and longest task duration without changing the `processReplica(ToolOptions)` exit-code contract.
- Removed obsolete static reset calls and updated file-manager and multi-table tests.

The Phase 0-a exit criterion is met: two concurrent runs use independent staging names and temporary-file maps, verified across 100 repetitions. Phase 0-a did not add cancellation or watermark extraction/injection; cancellation plumbing is implemented in Phase 0-b1 below, while watermark extraction/injection remains pending.

#### Phase 0-b1: Cancellation plumbing — IMPLEMENTED

Delivered in commit `4dd4cb5`, covered by focused JUnit tests (Mockito-backed context/manager unit tests, `CountDownLatch`-synchronized mid-loop cancellation tests, and a Testcontainers-backed PostgreSQL `COPY` cancellation test), orchestration regressions, and the successful `Only CI/CT` workflow:

- Extended `ReplicationExecutionContext` with an `AtomicBoolean` cancellation flag and a concurrent active-`Statement` registry. `requestCancellation()` sets the flag and calls `.cancel()` on every registered statement, tolerating a failing `cancel()` on one statement without skipping the rest.
- Added `ReplicationCancelledException` (checked, extends `SQLException`) so cancellation propagates through every existing manager method's declared `throws` clause without any signature changes.
- Added `checkCancellation()`/`registerActiveStatement()`/`unregisterActiveStatement()` helpers to `ConnManager` and `FileManager`, and centralized source-side statement tracking in `SqlManager.execute()`/`release()` so `readTable()` cancellation is covered for every SQL manager in one place.
- Wired per-row interrupt checks and active-statement tracking into the batch insert loops of `StandardJDBCManager`, `MySQLManager`, `OracleManager`, `Db2Manager`, `SqliteManager`, PostgreSQL's binary and text `COPY`, MongoDB's bulk write, Kafka's producer loop, and the CSV/ORC file writers; added a best-effort pre-flight guard before SQL Server's `BulkCopy` (no mid-transfer cancel hook exists in that API).
- Guarded `SqlManager.atomicInsertStagingTable()` and every real `mergeStagingTable()` override, including a pre-merge guard for MongoDB's native aggregation merge, which has no `Statement` to register.
- Replaced the blocking `invokeAll(...)` in `ReplicaDB.executeReplicationTasks(...)` with individually submitted, cancellable futures. A cancellation observed on one task cancels its sibling futures and shuts the executor down before rethrowing, so no thread pool is leaked on the cancelled or failed path.
- Added a package-visible `CANCELLED` exit code distinct from `SUCCESS`/`ERROR`. `executeSingleReplication(...)` maps both `ReplicationCancelledException` and any other exception observed while the execution context's cancellation flag is set to `CANCELLED`, so a JDBC driver that reports a cancelled `Statement` as an ordinary `SQLException` is still classified as `CANCELLED`, not `FAILED`.
- Cleanup already ran unconditionally before this change (`finally { cleanupResources(...); }`), so a cancelled run continues to drop an auto-generated staging table and leave a user-provided one untouched without further modification.

Known limitations, accepted for this plan and unchanged by later work: SQL Server `BulkCopy` and PostgreSQL's `COPY` protocol are best-effort — cancellation is observed at the next loop iteration or call boundary, not as an immediate `Statement.cancel()`; MongoDB, Kafka, and ORC have cancellation checks wired in but no dedicated mid-stream cancellation test (only non-cancelled regression coverage); Decision 5's full per-mode/per-timing severity table is exercised only through general cleanup-path tests, not one assertion per table cell.

#### Phase 0-b2: Watermark injection — IMPLEMENTED

Covered by focused JUnit tests (mocked-connection manager unit tests asserting generated SQL and bind order, orchestration unit tests, and a Testcontainers-backed PostgreSQL end-to-end test):

- Added `--incremental-watermark-column`/`--incremental-watermark-value` CLI and options-file settings to `ToolOptions`, validated to require `incremental` mode and to reject combination with `replication.table.*` multi-table entries.
- Added `manager.util.WatermarkBinder`, which resolves the watermark column's JDBC type from the existing `probeSourceMetadata()`/`ColumnDescriptor` machinery (now also triggered by a declared watermark column, not just `sink.auto-create`), converts the configured value into a typed bind parameter, and reduces per-task candidate strings type-aware (not lexicographically) for run-level aggregation.
- Injected a bound `AND <column> > ?` predicate into the 8 JDBC-based managers' existing `readTable()` query-building code (`Db2Manager`, `DenodoManager` — source-only, since Denodo cannot be a sink — `MySQLManager`, `OracleManager`, `PostgresqlManager`, `SqliteManager`, `SQLServerManager`, `StandardJDBCManager`), positioned correctly relative to each manager's existing partition/pagination binds. Non-SQL managers (`MongoDBManager`, `KafkaManager`, `S3Manager`, file managers) are explicitly out of scope, consistent with their existing merge/incremental limitations.
- Centralized the last-executed source SQL/bind-args in `SqlManager.execute()` and added `resolveWatermarkCandidate(int taskId)`, which issues a follow-up `SELECT MAX(...)` probe reusing the same query shape and bind values to report each task's highest observed value.
- Populated `ReplicaTaskResult.watermarkCandidate` from `ReplicaTask`, and widened `ReplicaDB.summarize(...)` to reduce all tasks' candidates to one run-level value.
- Exposed the reduced candidate on `ReplicationExecutionContext.setWatermarkCandidate(...)`/`getWatermarkCandidate()` **only after `executePostTasks()` (staging load + merge) completes without throwing** — a failed run, a cancelled run (either the explicit `ReplicationCancelledException` path or the flag-checked generic-exception path), and every other exception path leave it unset.

Scope boundary: this phase does not persist the watermark anywhere. No `JobDefinition`/`JobRun`/PostgreSQL state store exists yet (Phase 1b). The reduced candidate is exposed only on the in-memory `ReplicationExecutionContext` for a future Phase 1b job-execution service to read and persist; the CLI itself has no mechanism to feed a previous run's committed value back in automatically — the caller (a script, an orchestrator, or Phase 1b) must pass it via `--incremental-watermark-value` on the next invocation.

#### Core changes

1. **Watermark injection.** ~~Populate `ReplicaTaskResult.watermarkCandidate`, inject a typed `> :lastWatermark` predicate composed with the existing `$CONDITIONS` partition substitution, and infer the bind type from the source column metadata.~~ Implemented in Phase 0-b2 above.

#### State layer (deferred to Phase 1b)

- Introduce `JobDefinition` and `JobRun` domain models. There is no `Checkpoint` entity.
- Add a persistence layer for job definitions, run states, and watermarks.
- Define legal state transitions and the claim mechanism, using PostgreSQL row locking rather than application-level optimistic locking.
- Add an execution service that converts a job definition into `ToolOptions`.
- Keep `ReplicaDB.processReplica(ToolOptions)` as the compatibility entry point.

#### Resources and tools

- Java 17 and the existing Maven build, producing the two artifacts of Decision 1.
- Spring JDBC for the PostgreSQL metadata store; the replication managers remain unchanged.
- **Flyway** for versioning the metadata schema. Migrations are forward-only, and a rolling upgrade must tolerate one release of schema skew between `api` and `worker` instances.
- JUnit Jupiter 6, Mockito, and Testcontainers for state, retry, cancellation, and integration tests.

#### Exit criteria

- **Met in Phase 0-a:** Two replications run concurrently in one JVM with independent staging tables and temporary files.
- **Met in Phase 0-a:** Task results expose row counters and timings, and the executor aggregates them into a run-level summary.
- **Met in Phase 0-b1:** A cancellation request stops an in-flight replication, including SQL merge and atomic-insert statements, and the run ends in `CANCELLED`, never in `FAILED`, even when a JDBC driver reports the cancelled statement as a plain `SQLException`. SQL Server `BulkCopy` and PostgreSQL `COPY` remain best-effort, and MongoDB's native merge has a pre-merge guard but no active statement to cancel mid-operation.
- **Met in Phase 0-b2, at the core level:** A failed or cancelled incremental run leaves its previous watermark unchanged (verified: the reduced candidate is never written to `ReplicationExecutionContext` unless `executePostTasks()` succeeds). Persisting that unchanged value across runs is Phase 1b's responsibility once a state store exists.
- **Met in Phase 0-b2, at the core level:** A successful staging load and merge commit exactly one new watermark, reduced from all parallel tasks, exposed on `ReplicationExecutionContext`. Durable commit to a state store is Phase 1b's responsibility.
- A retry can identify the previous run and attempt number, and never claims to resume it.
- CLI behavior and existing `ToolOptions` configuration remain compatible, including multi-table options files.

### Phase 1: Spring Boot API and Scheduler

This is the first user-facing platform phase. API and scheduler run together in one Spring Boot process, while the existing replication core continues to execute the actual database transfer.

#### Phase 1a: Artifact split — IMPLEMENTED

Delivered as the standalone `replicadb-server` sibling Maven project:

- The `replicadb` CLI remains the root Maven artifact, with no Spring Boot dependency or application context on its classpath.
- `replicadb-server` builds against the installed CLI artifact, starts under the `api` profile, and exposes only `/actuator/health` through Actuator.
- The server skeleton excludes inherited MongoDB auto-configuration until the metadata state layer exists, so startup does not require an external database.
- CI builds and tests the server module after installing the CLI artifact; the release workflow uploads its unreleased `0.1.0-SNAPSHOT` jar as a separate build artifact rather than publishing it with the CLI release assets.

The next slice is **Phase 1b: State layer**, covering `JobDefinition`/`JobRun`, Flyway migrations, PostgreSQL persistence, row-locking claims, and the execution service. REST resources, scheduling, security, and the frontend remain pending after Phase 1a.

#### Spring Boot modules

The planned runtime would use the following components:

- `spring-boot-starter-web` for REST controllers and JSON requests.
- `spring-boot-starter-validation` for validating job definitions before persistence.
- `spring-boot-starter-jdbc` for the metadata repository while preserving the project's JDBC-oriented design.
- `spring-boot-starter-security` for authentication and endpoint authorization.
- `spring-session-jdbc` for server-side sessions persisted in PostgreSQL.
- `spring-boot-starter-quartz` for durable schedules, misfire handling, and non-overlapping triggers.
- `spring-boot-starter-actuator` and Micrometer for health, counters, and operational metrics.
- The existing Log4j2 and Sentry integration for logs and error reporting, with credential redaction preserved.
- An OpenAPI generator or documentation library can be added once the endpoint contract is stable.

These dependencies belong exclusively to the `replicadb-server` artifact. The `replicadb` CLI assembly is built from the same sources without them, as decided in Decision 1.

#### API and scheduler execution

The API and scheduler are not two separate processes in this phase:

```text
Spring Boot process (profile=api)
    |
    +-- REST controllers
    +-- Quartz scheduler
    +-- Job repository / state store
    +-- Bounded execution executor
    +-- ReplicaDB execution service
              |
              +-- ReplicaDB.processReplica(ToolOptions)
```

The execution sequence is:

1. A client creates a `JobDefinition` through the API.
2. The API validates and stores the definition without storing raw secrets.
3. A manual request or Quartz trigger creates a `JobRun` in `PENDING` state.
4. Quartz submits the run to a bounded application executor. Quartz must not hold a scheduler thread while a database replication is running.
5. The executor claims the run, changes it to `RUNNING`, resolves configuration references, and creates `ToolOptions`.
6. The executor calls the existing ReplicaDB core.
7. Row counters, timings, errors, and the final state are persisted.
8. For incremental mode, the watermark candidate reduced from all parallel tasks is committed only after the sink staging and merge operations succeed.

By default, a job should not overlap with another run of itself. Quartz's non-concurrent execution controls or an equivalent database claim must enforce this rule. Different jobs may run concurrently subject to configured resource limits, and only after the Phase 0 execution-context refactor has removed the static state described in Decision 2.

#### Operational defaults

These values are decided so that the first deployment is operable without further discussion. They are configurable; the defaults are what ships.

| Concern | Default | Rationale |
|---|---|---|
| Run history retention | 90 days | Bounded growth of `JobRun`; a scheduled purge deletes older rows |
| Audit event retention | 365 days | Longer than run history because it supports access review |
| Maximum run duration | 24 hours | Exceeding it cancels the run and marks it `FAILED` with a timeout reason |
| Schedule timezone | explicit IANA zone per job, `UTC` when unset | Avoids ambiguous and skipped local times across DST transitions |
| Persisted run log | last 256 KB of the run's own log, truncated from the head | Enough for a failure diagnosis in the UI without turning PostgreSQL into a log store |
| Full run logs | process logs and Sentry | The state store is the source of truth for state, not for output |
| Metadata backup | daily managed backup with point-in-time recovery | The metadata store becomes critical once jobs are managed there |

#### API surface

The first API remains small and explicit:

```text
POST   /api/v1/jobs                 Create a job definition
GET    /api/v1/jobs                 List job definitions
GET    /api/v1/jobs/{id}            Read a definition and current state
PUT    /api/v1/jobs/{id}            Update a definition
POST   /api/v1/jobs/{id}/runs       Trigger a manual run (Idempotency-Key)
GET    /api/v1/jobs/{id}/runs       Read historical runs for a job
GET    /api/v1/runs                 List runs, filterable by state
GET    /api/v1/runs/{id}            Read run status and counters
GET    /api/v1/runs/{id}/log        Read the persisted log excerpt
POST   /api/v1/runs/{id}/cancel     Cancel immediately, returns the sink warning
POST   /api/v1/runs/{id}/retry      Re-execute the job from the beginning
```

The API returns a run identifier quickly. Clients poll `GET /api/v1/runs/{id}` initially; server-sent events or WebSocket monitoring can be considered later without changing the execution contract.

#### Frontend, users, and permissions

The frontend is a separate web package from `docs/markdown/`, which remains an unrelated Markdown tool. A small React/TypeScript/Vite application is compiled to static assets and served by the `replicadb-server` process under the `api` profile, keeping the first deployment to one public application.

```text
Browser
  |
  +-- Login session cookie (HttpOnly, Secure, SameSite)
  |
  +-- Frontend static assets served by api
  |
  +-- REST API
        |
        +-- Spring Security authentication
        +-- role checks
        +-- job ACL checks
        +-- JobDefinition / JobRun services
```

The initial screens are:

- Login and session management.
- Dashboard of jobs visible to the current user.
- Job editor for the source/sink table pair, mode, parallelism, watermark column, and schedule.
- Run history and run detail with status, counters, log excerpt, and errors.
- Job permission editor for authorized users.
- User and role administration for `ADMIN` users.

The initial persistence model should include `app_user`, `app_role`, `app_user_role`, `job_permission`, `user_session`, and `audit_event`. Passwords are stored only as Argon2id hashes. The first administrator must be bootstrapped through a deployment-controlled setup flow; no default password may exist.

Every job endpoint must authorize the requested operation in the backend. A typical rule is that `ADMIN` bypasses job ACLs, while `OPERATOR` and `VIEWER` can access only jobs for which their user has the corresponding permission. Login attempts, user changes, permission changes, job changes, and execution actions should be auditable without recording passwords or connection secrets.

#### How the API deployment starts

The two artifacts of Decision 1 are started as follows:

```bash
# Existing CLI artifact: no Spring Boot context on the classpath
./bin/replicadb --options-file ./conf/replicadb.conf

# Server artifact: Spring Boot starts REST API and Quartz together
java -Dspring.profiles.active=api -jar replicadb-server.jar

# Equivalent container path
docker run -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=api \
  -e REPLICADB_METADATA_URL=${REPLICADB_METADATA_URL} \
  replicadb-server:api
```

At startup, Spring Boot loads the `api` profile configuration, connects to the metadata store, registers Quartz jobs, exposes the HTTP port, and creates the bounded executor. The scheduler then creates run records; it does not invoke database managers directly.

#### Phase 1 resources

- One API container or JVM process.
- PostgreSQL for local, managed, and multi-instance control-plane deployments.
- Network access from the API process to the configured source and sink databases.
- CPU and memory reserved for the executor's concurrent ReplicaDB runs.
- Environment variables or a secret manager for credentials and connection references.
- Frontend Node/Vite build tooling, with compiled assets packaged into the `replicadb-server` image.
- Docker or Podman for packaging; Testcontainers for API, metadata, security, and database integration tests.

### Phase 2: Distributed Workers

This phase separates the Spring Boot API/scheduler from replication execution. It is only justified when one API process cannot provide enough isolation or concurrency.

#### Components

- `api` deployment: Spring Boot REST API, Quartz scheduler, job repository, and run dispatcher.
- `worker` deployment: same codebase with the worker profile and no public API requirement.
- Mandatory PostgreSQL metadata database for job definitions, runs, leases, and watermarks.
- PostgreSQL `LISTEN/NOTIFY` connection for low-latency worker wake-up.
- Periodic PostgreSQL polling for startup recovery, reconnects, and missed notifications.
- Optional Kubernetes deployment, autoscaling, and centralized logs/metrics.

#### Worker runtime

`replicadb-server` under the `worker` profile is a runtime mode of the same artifact, not a second replication engine. It starts the common execution services and one PostgreSQL dispatch coordinator:

```text
replicadb-server (worker)
    |
    +-- PostgresNotifyDispatcher
    |       +-- dedicated LISTEN connection
    |       +-- NOTIFY wake-up handler
    |       +-- periodic polling fallback
    |
    +-- RunClaimService
    +-- LeaseRecoveryService
    +-- ReplicationExecutionService
    +-- WatermarkRepository
    +-- ReplicaDB core
```

The worker should execute one `JobRun` at a time by default. ReplicaDB's existing `jobs` option still controls the internal parallel tasks for that run. A future `worker.max-concurrent-runs` setting must be bounded because total database pressure is approximately:

```text
worker instances * concurrent runs per worker * jobs per run
```

#### PostgreSQL notification and claim flow

```text
API + Quartz
  |
  | PostgreSQL transaction
  +-- INSERT JobRun(PENDING)
  +-- SELECT pg_notify('replicadb_runs', run_id)
  +-- COMMIT
        |
        v
   All LISTEN workers wake up
        |
        v
   One worker claims with
   FOR UPDATE SKIP LOCKED
        |
        v
   ReplicaDB execution
        |
        v
   PostgreSQL state + watermark
```

The API inserts the `JobRun` and issues `pg_notify` in the same transaction. PostgreSQL delivers the notification after commit, so a worker never wakes for a run that was rolled back. The payload contains only a `runId` and must remain below PostgreSQL's notification payload limit; all job data is loaded from the state tables. The claim transaction, not the notification, is what assigns a run to exactly one active worker.

#### Claim transaction and polling fallback

```text
BEGIN
  select an eligible PENDING or RETRY_SCHEDULED run
  using FOR UPDATE SKIP LOCKED
  update it to RUNNING with worker_id and lease_until
COMMIT
```

The claim must select only runs whose `available_at` has elapsed and whose lease is empty or expired. PostgreSQL row locking allows several workers to claim different runs concurrently without waiting on already claimed rows. A worker must never select a `PENDING` row and update it later in a separate unprotected operation.

The worker performs a periodic fallback poll even when `LISTEN` is active. It polls at startup, after reconnecting, and on a configurable interval. This covers a worker that was offline when `NOTIFY` was sent and makes notification delivery an optimization rather than a correctness dependency.

#### Common execution and recovery

The notification handler and polling fallback execute the same sequence after discovering eligible work:

```text
1. Receive a notification or find an eligible row by polling
2. Atomically claim JobRun(PENDING or RETRY_SCHEDULED)
3. Set RUNNING, workerId, heartbeatAt, and leaseUntil
4. Load an immutable job-definition snapshot
5. Resolve secret references and build ToolOptions
6. Execute ReplicaDB core
7. Persist row counters, timings, error, and terminal state
8. Commit the watermark only after staging and merge succeed
9. Release the lease through a terminal state
```

The `JobRun` contains a lease and heartbeat governed by the rules in Decision 6. A recovery process returns a run to `RETRY_SCHEDULED` when `lease_until` expires, which handles a worker process that disappears during a database operation. Because runs are never resumed, that retry re-executes the job from the beginning. The worker uses a dedicated PostgreSQL connection for `LISTEN`; replication source and sink connections remain owned by the existing ReplicaDB tasks.

The execution model is at-least-once, not exactly-once. Duplicate notifications and polling must be handled by the claim state, the idempotent sink merge, and the watermark commit rules. The API reads status only from PostgreSQL; it does not infer status from notification delivery or worker logs.

The Phase 0 rich task result is what allows a worker to persist row counters and a watermark candidate. Without it the worker records only coarse-grained lifecycle state.

#### Optional worker shapes

The same worker profile can run as either:

- A long-lived worker pool with a dedicated `LISTEN` connection and polling fallback.
- An ephemeral task started with a `runId`, which executes one run and exits.

The long-lived pool is the natural fit for PostgreSQL `LISTEN/NOTIFY`. Ephemeral tasks are useful on Cloud Run Jobs, Azure Container Apps Jobs, ECS tasks, or Kubernetes Jobs when the platform starts them with a `runId`; those tasks still claim the run from PostgreSQL and do not rely on a notification remaining available.

#### Phase 2 resources

- At least one API instance and one worker instance.
- Managed PostgreSQL with private connectivity from API and workers.
- Dedicated PostgreSQL listener connection per long-lived worker.
- Worker leases, heartbeats, retry limits, and dead-run recovery process.
- Docker Compose for local topology tests and Kubernetes for production scaling when required.
- Micrometer/Actuator or an equivalent metrics pipeline, plus centralized logs and Sentry error reporting.
- Load tests that measure notification wake-up, polling fallback, row-claim contention, database load, worker recovery, duplicate notifications, and duplicate polling.

## Implementation Priorities

### Priority 1: Core and State Contract

- [x] Replace the static staging-name and temp-file state with a per-run execution context. **Completed in Phase 0-a (`c228ddc`).**
- [x] Widen the `ReplicaTask` result to carry counters, timings, and a watermark candidate. **Counters and timings completed in Phase 0-a (`c228ddc`); watermark population remains pending.**
- [x] Add cancellation: statement handles, interrupt checks, cancellable futures. **Implemented in Phase 0-b1 (commit `4dd4cb5`).**
- [x] Implement watermark injection with type inference from source column metadata. **Implemented in Phase 0-b2.**
- [ ] Define `JobDefinition` and `JobRun` persistence models and Flyway migrations.
- [ ] Define legal state transitions, retry behavior, and idempotency rules.
- [ ] Split the build into the `replicadb` and `replicadb-server` artifacts.
- [ ] Preserve CLI behavior, including multi-table options files.
- [ ] Add focused tests for concurrent runs, cancellation, and watermark advancement.

### Priority 2: Monolithic Control Plane

- [ ] Add the `/api/v1` REST API for job and run management.
- [ ] Reject multi-table definitions at the API boundary with an explicit error.
- [ ] Add Quartz scheduling with an explicit timezone per job.
- [ ] Add asynchronous execution and monitoring.
- [ ] Add run history, operational counters, persisted log excerpts, and error details.
- [ ] Return and persist the indeterminate-sink warning on cancellation.
- [ ] Add local-user authentication with Spring Security and PostgreSQL sessions.
- [ ] Add global roles `ADMIN`, `OPERATOR`, and `VIEWER`.
- [ ] Add per-job ACLs for `VIEW`, `EDIT`, `EXECUTE`, and `CANCEL`.
- [ ] Add the planning and monitoring frontend to the `replicadb-server` package.
- [ ] Add audit events, retention purge jobs, and secure bootstrap of the first administrator.
- [ ] Keep credentials outside persisted job payloads.

### Priority 3: Optional Distributed Deployment

- [ ] Define the PostgreSQL worker dispatcher and state schema.
- [ ] Implement atomic run claims, leases, heartbeats, and retry scheduling using PostgreSQL `now()`.
- [ ] Keep heartbeats alive during long merge and swap operations.
- [ ] Implement the dedicated `LISTEN/NOTIFY` connection and reconnect logic.
- [ ] Implement periodic polling as the mandatory notification-recovery path.
- [ ] Dispatch run identifiers without transporting secrets.
- [ ] Test worker loss, retries, missed notifications, duplicate notifications, and duplicate polling.

---

## Success Metrics

### Phase 0

- **Met:** Two concurrent runs in one JVM produce two distinct staging tables and zero cross-run interference across 100 repetitions.
- **Met:** Task results report row counts and timings, and run-level aggregation reports the total rows and longest task duration.
- A cancellation request halts source reads within 5 seconds at the 95th percentile.
- Control-plane overhead adds no more than 5% to the wall-clock duration of an equivalent CLI run.

### Phase 1

- CLI invocation, exit codes, and existing configuration remain compatible, verified by the existing CLI test suite with zero modifications.
- The `replicadb` artifact contains no Spring Boot classes.
- A failed or cancelled run never advances its incremental watermark.
- Restarting the control plane does not lose persisted run state.
- Monitoring exposes status, counters, timestamps, and failure details.
- Unauthorized users cannot view, edit, execute, or cancel jobs.
- Administrators can manage users, roles, job permissions, and audit history.
- The frontend can create, schedule, execute, and monitor jobs through the API.
- Credentials are absent from job payloads, state records, API responses, and logs.
- A replayed `Idempotency-Key` never produces a second run.

### Phase 2

- Worker loss does not lose a persisted run; the run reappears as claimable within one lease period.
- Duplicate notifications and duplicate database polling do not advance a watermark twice.
- Expired worker leases return recoverable runs to the retry path.
- A worker reconnects and rescans PostgreSQL after a listener failure.
- Missed notifications are recovered by polling within one polling interval.
- A merge lasting longer than the lease duration never triggers a duplicate claim.
- Concurrency and scaling limits are established from reproducible benchmarks.

---

## Constraints and Limitations

### Current core

- Phase 0-a removed the static staging-name and temporary-file state. Each `ToolOptions` owns one `ReplicationExecutionContext`; a multi-table copy receives a fresh context.
- `ReplicaTask` now returns `ReplicaTaskResult` with row counts, timings, and a populated watermark candidate; `executeReplicationTasks` reduces those candidates to one run-level value.
- Phase 0-b1 added the per-run cancellation token, active-statement registry, and cancellable futures described above; SQL Server `BulkCopy` and PostgreSQL `COPY` cancellation remain best-effort, and MongoDB/Kafka/ORC lack dedicated mid-stream cancellation tests.
- Phase 0-b2 added watermark predicate injection to the 8 JDBC-based managers (Denodo source-only); MongoDB/Kafka/S3/file managers do not support it. The reduced watermark candidate is exposed on `ReplicationExecutionContext` only after a successful merge; it is not persisted anywhere until Phase 0-c adds the state layer.
- Each parallel task owns its source and sink managers; the state layer must not assume shared JDBC connections.
- Manager capabilities differ by source, sink, and replication mode.
- Multi-table replication stops at the first failure and leaves earlier tables applied; it is a CLI-only capability.

### Execution semantics

- Runs are never resumed. A retry is a full re-execution.
- Partition assignment is not reproducible across executions, so no feature may depend on partition identity.
- Retrying a `complete` run is destructive because the sink is truncated before any write.
- Cancelling during a merge or atomic swap leaves the sink in an indeterminate state, and the API must say so.
- Watermarks exist only for `incremental` mode, use a single declared column, and never propagate deletes.
- An incremental merge requires primary keys on the source.
- Monitoring counters describe execution; they do not prove source/sink equality.

### Deployment

- PostgreSQL is mandatory for the `api` and `worker` profiles; the CLI does not use it.
- SQLite is limited to isolated CLI fixtures or unit tests.
- The CLI remains available in every implementation phase and deployment model.
- PostgreSQL `LISTEN/NOTIFY` is a wake-up signal, not a durable queue; polling recovery is mandatory.
- Workers require a dedicated listener connection and must reconnect and re-subscribe after failure.
- Lease and heartbeat timestamps come from PostgreSQL `now()`, never from worker clocks.
- PostgreSQL row locking and leases must prevent two workers from claiming the same run.
- User passwords are stored only as Argon2id hashes; sessions require secure cookie and CSRF configuration.
- Every job operation must enforce ACLs in the backend, independently of frontend visibility.
- The first administrator requires a controlled bootstrap flow with no default password.
- The frontend is served by the `api` profile; workers expose no public login or UI.
- Secrets are resolved by the executor and never transported as job data.
- More workers increase source and sink contention; scaling limits must be measured.

---

## References

### Internal Documentation

- `README.md` - CLI, deployment, and compatibility baseline.
- `docs/docs/docs.md` - Replication modes and connector behavior.
- `src/main/java/org/replicadb/ReplicaDB.java` - Orchestration, multi-table loop, cancellable task execution, and the `CANCELLED` exit code.
- `src/main/java/org/replicadb/ReplicaTask.java` - Task execution and rich result production.
- `src/main/java/org/replicadb/ReplicaTaskResult.java` - Row counters, timings, and reserved watermark candidate.
- `src/main/java/org/replicadb/execution/ReplicationExecutionContext.java` - Per-run identifier, staging name, temporary-file state, cancellation flag, and active-statement registry.
- `src/main/java/org/replicadb/execution/ReplicationCancelledException.java` - Checked cancellation signal propagated through existing manager method signatures.
- `src/main/java/org/replicadb/cli/ReplicationMode.java` - `complete`, `complete-atomic`, `incremental`.
- `src/main/java/org/replicadb/cli/ToolOptions.java` - Current configuration boundary.
- `src/main/java/org/replicadb/manager/SqlManager.java` - Staging, merge, atomic swap, and cancellation-aware statement lifecycle.
- `src/main/java/org/replicadb/manager/ConnManager.java` - Per-run staging-table name access and cancellation helpers.
- `src/main/java/org/replicadb/manager/file/FileManager.java` - Per-run temporary-file access and cancellation helper.
- `openspec/` - Change proposals and specs for engine-level behavior; this document governs product direction, not individual engine changes.
- `.ai/context/execution.md` - Current execution and lifecycle constraints.
- `.ai/context/operations.md` - Current runtime, telemetry, and deployment constraints.

### External Resources

- ReplicaDB GitHub: https://github.com/osalvador/ReplicaDB
- PostgreSQL `NOTIFY`: https://www.postgresql.org/docs/current/sql-notify.html
- PostgreSQL `LISTEN`: https://www.postgresql.org/docs/current/sql-listen.html
- PostgreSQL explicit locking: https://www.postgresql.org/docs/current/explicit-locking.html

---

**Document Version**: 2.2
**Last Updated**: August 15, 2026
**Next Review**: Before implementation of Phase 0-c (`JobDefinition`/`JobRun` persistence and the state layer)
