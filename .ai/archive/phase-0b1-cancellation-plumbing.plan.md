# Implementation Plan: Phase 0-b1 — Cancellation Plumbing

## Task Source

`ARCHITECTURE_DECISIONS.md`, Decision 5 ("Immediate Cancellation") and the "Phase 0: Core and State Foundation" → "Core changes" item 1 ("Cancellation plumbing"). No JIRA ticket.

Phase 0-b in the architecture document bundles two independent items under one label: (1) cancellation plumbing and (2) watermark injection. Following the same split strategy Phase 0-a used (splitting "Phase 0" into three sub-plans), this plan implements **cancellation only**. Watermark injection (Decision 3, "Incremental watermarks") is deferred to a follow-up **Phase 0-b2** plan, since combining both would exceed ~35-45 tasks and mixes two independently testable concerns.

Acceptance criteria extracted from Decision 5 and the Phase 0 exit criteria, scoped to this plan:

- A per-run cancellation token carried by the execution context (`ReplicationExecutionContext`).
- Access to the active `Statement` of running tasks so a control thread can call `Statement.cancel()` — `Future.cancel(true)` alone is insufficient because a thread blocked inside a JDBC call does not observe interruption.
- Interrupt checks in the row-copy loop so a task exits promptly and still runs its cleanup.
- Replacement of the blocking `invokeAll` pattern with individually cancellable futures.
- Cancellation also interrupts `mergeStagingTable()` and `atomicInsertStagingTable()` when they are already running.
- A cancelled run runs its normal cleanup path (drops an auto-generated staging table; leaves a user-provided one untouched) and terminates in a new `CANCELLED` state, never in `FAILED`.
- CLI behavior, exit codes for existing success/error paths, and `ToolOptions` configuration remain compatible.

**Explicitly out of scope for this plan**:
- Watermark injection, `ReplicaTaskResult.watermarkCandidate` population, and type inference from source column metadata (deferred to Phase 0-b2).
- `JobDefinition`/`JobRun` persistence, Flyway/PostgreSQL, the claim mechanism, and the `replicadb`/`replicadb-server` artifact split (deferred to Phase 0-c per the architecture document).
- An actual HTTP cancellation endpoint — Phase 1 introduces the API; this plan only builds the internal mechanism `ReplicationExecutionContext.requestCancellation()` that a future controller will call.
- Cancellation-path test coverage for `MongoDBManager`, `KafkaManager`, and `OrcFileManager` (Testcontainers-heavy, no fast in-memory substitute exists today); the interrupt-check wiring itself is still added and covered by existing non-cancelled regression tests.
- `S3Manager`/`LocalFileManager`'s `mergeFiles()` step and `DenodoManager`'s insert path (read-only source, `insertDataToTable`/`mergeStagingTable` already throw `UnsupportedOperationException`).
- True sub-call interruption of `SQLServerBulkCopy.writeToServer()` — these are not `java.sql.Statement`-based APIs, so cancellation there is a best-effort check at the call boundary, not mid-transfer interruption.

## Overview

ReplicaDB's core has no cancellation support today: there are no interrupt checks in the copy path, no access to the active `Statement` of a running task, and `ReplicaDB.executeReplicationTasks` blocks on `invokeAll` until every task finishes. This plan adds a per-run cancellation token and active-`Statement` registry to the existing `ReplicationExecutionContext` (introduced in Phase 0-a), wires interrupt checks and statement tracking into the row-copy loops and merge/atomic-insert paths of every JDBC-backed manager, replaces the blocking `invokeAll` call with individually submitted, cancellable futures, and introduces a new `CANCELLED` exit code distinct from `ERROR`. This is the mechanism the Phase 1 Spring Boot API will call; it does not add an HTTP endpoint itself.

## Architecture & Design

**Approach**: Attach cancellation state to the existing `ReplicationExecutionContext` (per-run, shared by all managers/tasks of one `ToolOptions`), and expose it to managers through small protected helper methods on the existing `ConnManager`/`FileManager` base classes — the same "attach to the shared boundary instead of touching every constructor" pattern Phase 0-a used successfully.

Key design decisions:

1. **`ReplicationCancelledException extends SQLException`.** Every `insertDataToTable`/`readTable`/`mergeStagingTable`/`atomicInsertStagingTable` method across the 13 manager implementations already declares `throws SQLException` or the broader `throws Exception`. Making the cancellation signal a checked `SQLException` subtype means it propagates through every existing method signature with **zero signature changes**, and `ReplicaTask`/`ReplicaDB` can catch it specifically to distinguish `CANCELLED` from `FAILED`.

2. **A single `Set<Statement>` registry, not a per-task map.** Decision 5 asks for "access to the active `Statement` of each `ReplicaTask`", but since cancellation is a run-level, all-or-nothing operation (the endpoint stops "the replication", not one task within it), a plain `ConcurrentHashMap.newKeySet()` of currently-active statements for the run is sufficient and avoids threading `taskId` through every registration call.

3. **Centralize source-side statement tracking in `SqlManager.execute()`.** Nearly every manager's `readTable()` ultimately calls the shared, non-abstract `SqlManager.execute(...)` helper (which already tracks `lastStatement` for `release()`). Registering/unregistering the statement there covers the source-read side of cancellation for 8 of the 13 managers in one file change, instead of touching each `readTable()` override.

4. **Sink-side tracking is per-manager because insert mechanisms differ structurally**: plain batched `PreparedStatement` (StandardJDBC, MySQL, Oracle, Db2, Sqlite), PostgreSQL binary `COPY` (`CopyIn`, not a `Statement`), SQL Server `SQLServerBulkCopy` (not a `Statement`), MongoDB bulk writes and Kafka producer sends (no JDBC `Statement` at all), and delegated file writers (CSV/ORC via `FileManager`). Each family gets its own small, mechanical wiring task instead of one generic abstraction that would not fit the non-JDBC cases.

5. **`checkCancellation()` calls are the primary interrupt mechanism**, called once per row/batch iteration in every copy loop; `Statement.cancel()` (via the registry) is the secondary mechanism for unblocking a thread already parked inside a blocking JDBC call (e.g., `rs.next()` waiting on network I/O, or `ps.executeBatch()`), consistent with Decision 5's explicit statement that `Future.cancel(true)` alone is insufficient.

**Rejected alternative**: Thread-interrupt-only cancellation (drop `Statement.cancel()` entirely). Rejected because Decision 5 explicitly requires it — a thread blocked inside a JDBC driver call does not observe `Thread.interrupt()`.

**Rejected alternative**: A per-task `Map<Integer, Statement>` keyed registry. Rejected as unnecessary complexity — cancellation always targets the whole run, not an individual task, so a flat `Set` is simpler and cannot leak stale entries tied to a `taskId` that get overwritten.

## Implementation Tasks

### 1. Cancellation Foundation

- [x] **1.1 Add cancellation flag and active-statement registry to `ReplicationExecutionContext`**
  Files: `src/main/java/org/replicadb/execution/ReplicationExecutionContext.java`
  Changes: Add `private final AtomicBoolean cancellationRequested = new AtomicBoolean(false);` and `private final Set<Statement> activeStatements = ConcurrentHashMap.newKeySet();`. Add `public void requestCancellation()` (sets the flag, then calls a private `cancelActiveStatements()` that iterates `activeStatements` calling `.cancel()` on each, catching and logging `SQLException` per statement so one failing cancel does not stop the others), `public boolean isCancellationRequested()`, `public void registerActiveStatement(Statement statement)`, `public void unregisterActiveStatement(Statement statement)`. Add a `Logger` field (Log4j2, matching project convention) for the cancel-failure log line.
  Tests: `src/test/java/org/replicadb/execution/ReplicationExecutionContextTest.java` (extend existing file) — `isCancellationRequested()` is `false` initially and `true` after `requestCancellation()`; a registered Mockito-mocked `Statement` has `.cancel()` invoked exactly once after `requestCancellation()`; a second registered statement whose `.cancel()` throws `SQLException` does not prevent a third statement's `.cancel()` from being invoked; `unregisterActiveStatement` before `requestCancellation()` means `.cancel()` is never invoked on that statement.
  Dependencies: None

- [x] **1.2 Add `ReplicationCancelledException`**
  Files: `src/main/java/org/replicadb/execution/ReplicationCancelledException.java` (new)
  Changes: `public final class ReplicationCancelledException extends SQLException { public ReplicationCancelledException(String message) { super(message); } }`. Extending `SQLException` lets it propagate through every existing manager method's `throws SQLException`/`throws Exception` declaration without any signature changes.
  Tests: `src/test/java/org/replicadb/execution/ReplicationCancelledExceptionTest.java` (new) — verify it `instanceof SQLException`; verify the constructor's message round-trips via `getMessage()`.
  Dependencies: None

### 2. Base-Class Cancellation Hooks

- [x] **2.1 Add `checkCancellation`/`registerActiveStatement`/`unregisterActiveStatement` helpers to `ConnManager`**
  Files: `src/main/java/org/replicadb/manager/ConnManager.java`
  Changes: Add `protected void checkCancellation() throws ReplicationCancelledException { if (options.getExecutionContext().isCancellationRequested()) { throw new ReplicationCancelledException("Replication run " + options.getExecutionContext().getRunId() + " was cancelled"); } }`, `protected void registerActiveStatement(Statement statement) { options.getExecutionContext().registerActiveStatement(statement); }`, `protected void unregisterActiveStatement(Statement statement) { options.getExecutionContext().unregisterActiveStatement(statement); }`. Add the `java.sql.Statement` and `org.replicadb.execution.ReplicationCancelledException` imports.
  Tests: `src/test/java/org/replicadb/manager/ConnManagerCancellationTest.java` (new) — using a minimal `ConnManager` stub (same shape as `ConnManagerStagingIsolationTest`'s `StagingManager`), verify `checkCancellation()` does not throw before `requestCancellation()` and throws `ReplicationCancelledException` after; verify `registerActiveStatement`/`unregisterActiveStatement` delegate correctly by checking a Mockito-mocked `Statement` is/isn't cancelled after `options.getExecutionContext().requestCancellation()`.
  Dependencies: Tasks 1.1, 1.2

- [x] **2.2 Add a `checkCancellation` helper to `FileManager`**
  Files: `src/main/java/org/replicadb/manager/file/FileManager.java`
  Changes: Add `protected void checkCancellation() throws ReplicationCancelledException { if (options.getExecutionContext().isCancellationRequested()) { throw new ReplicationCancelledException("Replication run " + options.getExecutionContext().getRunId() + " was cancelled"); } }` (mirrors 2.1; `FileManager` does not extend `ConnManager` so it needs its own copy). Add the `ReplicationCancelledException` import.
  Tests: `src/test/java/org/replicadb/manager/file/FileManagerCancellationTest.java` (new) — minimal `FileManager` stub verifying `checkCancellation()` throws only after `options.getExecutionContext().requestCancellation()` is called.
  Dependencies: Tasks 1.1, 1.2

### 3. Source-Side Statement Tracking (Shared Path)

- [x] **3.1 Track the shared `execute()` statement in `SqlManager`**
  Files: `src/main/java/org/replicadb/manager/SqlManager.java`
  Changes: In `execute(String stmt, Integer fetchSize, Object... args)`, immediately after `this.lastStatement = statement;`, add `registerActiveStatement(statement);`. In `release()`, before closing `lastStatement`, add `if (lastStatement != null) { unregisterActiveStatement(lastStatement); }`. This covers the source-read (`readTable()`) side of cancellation for every manager that calls `super.execute(...)` — `StandardJDBCManager`, `MySQLManager`, `OracleManager`, `Db2Manager`, `SqliteManager`, `PostgresqlManager`, `SQLServerManager`, `DenodoManager` — in one file change, without touching each `readTable()` override.
  Tests: `src/test/java/org/replicadb/manager/SqlManagerCancellationTest.java` (new, following the `TestSqlManager extends PostgresqlManager` pattern from `SqlManagerDDLGenerationTest`) — call `execute(...)` against a lightweight backing (or verify via a `SqliteManager` instance backed by a `@TempDir` file DB, matching `SqliteManagerNullHandlingTest`'s fixture) and assert the execution context's active-statement set contains the created statement until `release()`/`close()` is called, after which it no longer does.
  Dependencies: Task 2.1

### 4. Interrupt Checks in JDBC Batch-Insert Managers

- [x] **4.1 Wire cancellation into the batched `PreparedStatement` insert loops**
  Files: `src/main/java/org/replicadb/manager/StandardJDBCManager.java`, `src/main/java/org/replicadb/manager/MySQLManager.java`, `src/main/java/org/replicadb/manager/OracleManager.java`, `src/main/java/org/replicadb/manager/db2/Db2Manager.java`, `src/main/java/org/replicadb/manager/SqliteManager.java`
  Changes: In each class's `insertDataToTable(...)`, immediately after the `PreparedStatement` is created (e.g. `PreparedStatement ps = this.getConnection().prepareStatement(sqlCdm);`), call `registerActiveStatement(ps);`. At the top of the `do { ... } while (resultSet.next());` loop body (before `bt.acquiere();`), call `checkCancellation();`. Wrap the statement's lifetime in a `try/finally` so `unregisterActiveStatement(ps);` runs before `ps.close();` regardless of whether the loop completed or threw.
  Tests: `src/test/java/org/replicadb/sqlite/SqliteManagerCancellationTest.java` (new, `@TempDir`-backed SQLite source/sink following `SqliteManagerNullHandlingTest`'s fixture — fast, no Testcontainers): insert 500 source rows using a batch size of 10 so the loop yields control periodically; before starting, register a `CountDownLatch rowLatch = new CountDownLatch(1)` and have a test-only row counter (checked via the sink's `PreparedStatement` batch callback, or by wrapping the source `ResultSet` in a thin counting decorator) count down `rowLatch` after the 50th row is read. Start `insertDataToTable(...)` on a background thread; the test thread calls `rowLatch.await()` then `options.getExecutionContext().requestCancellation()`, guaranteeing cancellation is requested only after at least 50 of 500 rows were processed and before the loop can finish. Assert (a) the background task's `Future`/thread completes by throwing `ReplicationCancelledException`, and (b) the sink table's committed row count is strictly less than 500 and greater than 0, proving the loop exited early rather than completing then throwing or never starting. Run the existing MySQL/Oracle/Db2/StandardJDBC Testcontainers insert tests unmodified to confirm the non-cancelled path is unaffected.
  Dependencies: Tasks 2.1, 1.2

- [x] **4.2 Wire cancellation into PostgreSQL's binary `COPY` loop**
  Files: `src/main/java/org/replicadb/manager/PostgresqlManager.java`
  Changes: Inside the `copyIn.writeToCopy(...)` `do { ... } while (resultSet.next());` loop in `insertDataToTable(...)`, call `checkCancellation();` once per iteration. In the existing `catch (Exception e)` block that already calls `copyIn.cancelCopy()` (current lines ~227/233), ensure a thrown `ReplicationCancelledException` is caught by that same block so `cancelCopy()` runs before the exception is rethrown (no new catch block needed if `ReplicationCancelledException` is already an `Exception` subtype caught there — verify and adjust catch ordering only if a narrower catch currently intercepts it first).
  Tests: `src/test/java/org/replicadb/postgres/PostgresqlManagerCancellationTest.java` (new, Testcontainers-backed, following the existing `Postgres2CsvFileTest`-style fixture) — seed a source table with enough rows (e.g. 50,000) that the `COPY` measurably takes longer than a `CountDownLatch.await(2, TimeUnit.SECONDS)` window; start `insertDataToTable(...)` on a background thread, wait a fixed short interval on the test thread (accepting some timing tolerance since PostgreSQL's `CopyIn` API exposes no progress hook), then call `options.getExecutionContext().requestCancellation()`; assert `insertDataToTable` throws `ReplicationCancelledException` and the sink/staging table's row count is `0` or strictly less than the seeded count, never the full count. Do not assert on `copyIn.isActive()` or any other private internal state — only the thrown exception and the observable row count are asserted, since `copyIn` is a local variable with no accessor.
  Dependencies: Tasks 2.1, 1.2

- [x] **4.3 Add a best-effort cancellation guard to SQL Server's `BulkCopy` path**
  Files: `src/main/java/org/replicadb/manager/SQLServerManager.java`
  Changes: In `insertDataToTableInternal(...)`, extract the pre-flight check into a package-visible method `boolean shouldAbortBeforeBulkCopy()` that returns `options.getExecutionContext().isCancellationRequested()`; call it immediately before constructing `SQLServerBulkCopy`, and if it returns `true`, throw `checkCancellation()`'s `ReplicationCancelledException` (i.e., call `checkCancellation();` right after the `shouldAbortBeforeBulkCopy()` check, or simply call `checkCancellation();` directly and drop the boolean wrapper — the package-visible method exists solely so the guard is unit-testable without a live connection). Add a one-line code comment stating that `SQLServerBulkCopy` exposes no mid-transfer cancellation hook, so a cancellation requested while a copy is already in flight is observed only at the next retry-loop iteration in `insertDataToTable(...)`, not immediately — an accepted limitation consistent with the "swap interrupted, sink indeterminate" caveat already accepted for `complete-atomic` in Decision 5's table.
  Tests: `src/test/java/org/replicadb/sqlserver/SQLServerManagerCancellationGuardTest.java` (new) — construct a `SQLServerManager` instance with a `ToolOptions` that has not opened any real connection, call `options.getExecutionContext().requestCancellation()`, then call `shouldAbortBeforeBulkCopy()` directly and assert it returns `true`; construct a second instance without requesting cancellation and assert it returns `false`. This tests the guard in isolation without invoking `insertDataToTableInternal(...)` or requiring a live SQL Server connection.
  Dependencies: Tasks 2.1, 1.2

### 5. Interrupt Checks in Non-JDBC and File-Based Managers

- [x] **5.1 Wire `checkCancellation()` into MongoDB and Kafka insert loops**
  Files: `src/main/java/org/replicadb/manager/MongoDBManager.java`, `src/main/java/org/replicadb/manager/KafkaManager.java`
  Changes: In `MongoDBManager.insertDataToTable(...)`, call `checkCancellation();` once per iteration of the `do { ... } while (resultSet.next());` bulk-write loop, before adding each `WriteModel` to the batch. In `KafkaManager.insertDataToTable(...)`, call `checkCancellation();` once per iteration before `producer.send(...)`. Both classes inherit `checkCancellation()` from `ConnManager` (Task 2.1) via `SqlManager`.
  Tests: No new Testcontainers-backed cancellation-path test in this task (MongoDB/Kafka containers make a fast, deterministic mid-stream cancellation test impractical here) — this is an explicitly accepted gap, called out in the Testing Strategy below. Run the existing `Mongo2CsvFileTest`/`Mongo2OrcFileTest` and Kafka integration tests unmodified to confirm the non-cancelled path is unaffected by the added `checkCancellation()` calls.
  Dependencies: Tasks 2.1, 1.2

- [x] **5.2 Wire `checkCancellation()` into the CSV and ORC file-writer loops**
  Files: `src/main/java/org/replicadb/manager/file/CsvFileManager.java`, `src/main/java/org/replicadb/manager/file/OrcFileManager.java`
  Changes: In each class's `writeData(...)`, call the inherited `checkCancellation();` (from Task 2.2) once per iteration of the `do { ... } while (resultSet.next());` loop, before processing each row's columns.
  Tests: `src/test/java/org/replicadb/manager/file/CsvFileManagerCancellationTest.java` (new, `@TempDir`-backed, no Testcontainers) — build a fake/scripted `ResultSet` of 500 rows whose `next()` implementation counts down a `CountDownLatch rowLatch = new CountDownLatch(1)` after its 50th invocation; start `writeData(...)` on a background thread, have the test thread call `rowLatch.await()` then `options.getExecutionContext().requestCancellation()`, guaranteeing cancellation lands after at least 50 rows are written and before the loop finishes; assert `writeData(...)` throws `ReplicationCancelledException` and the partial output file has strictly fewer than 500 written rows and more than 0. If `OrcFileManager`'s writer cannot be exercised this cheaply, document that its cancellation path is covered only by the existing non-cancelled Testcontainers-backed ORC tests, as an accepted gap identical to Task 5.1's.
  Dependencies: Tasks 2.2, 1.2

### 6. Cancellation Guards on Merge and Atomic-Insert

- [x] **6.1 Add a cancellation guard to `SqlManager.atomicInsertStagingTable()`**
  Files: `src/main/java/org/replicadb/manager/SqlManager.java`
  Changes: At the start of `atomicInsertStagingTable()`, call `checkCancellation();` before creating the `Statement`. Immediately after `Statement statement = this.getConnection().createStatement();`, call `registerActiveStatement(statement);`. Wrap the remainder of the method body in a `try/finally` that calls `unregisterActiveStatement(statement);` before `statement.close();`.
  Tests: Extend `src/test/java/org/replicadb/manager/SqlManagerCancellationTest.java` (from Task 3.1) with a test asserting `atomicInsertStagingTable()` throws `ReplicationCancelledException` immediately (no SQL executed) when `options.getExecutionContext().requestCancellation()` was already called beforehand.
  Dependencies: Tasks 2.1, 1.2

- [x] **6.2 Add a cancellation guard to every real `mergeStagingTable()` override**
  Files: `src/main/java/org/replicadb/manager/db2/Db2Manager.java`, `src/main/java/org/replicadb/manager/MySQLManager.java`, `src/main/java/org/replicadb/manager/OracleManager.java`, `src/main/java/org/replicadb/manager/SQLServerManager.java`, `src/main/java/org/replicadb/manager/SqliteManager.java`, `src/main/java/org/replicadb/manager/PostgresqlManager.java`, `src/main/java/org/replicadb/manager/StandardJDBCManager.java`, `src/main/java/org/replicadb/manager/MongoDBManager.java`
  Changes: In each SQL-based override, call `checkCancellation();` as the first statement in the method body, before the merge `Statement` is created. Register/unregister the created `Statement` the same way as Task 6.1 (`registerActiveStatement` right after creation, `unregisterActiveStatement` in a `finally` before `close()`). `MongoDBManager.mergeStagingTable()` runs a real, non-trivial `$merge` aggregation pipeline via the MongoDB driver (no `java.sql.Statement` involved) — add only the `checkCancellation();` guard at the start of that method, with no statement registration since none applies. `DenodoManager`, `KafkaManager`, and `S3Manager`'s `mergeStagingTable()` remain unchanged (unsupported/no-op/delegated — out of scope, see Overview).
  Tests: `src/test/java/org/replicadb/sqlite/SqliteManagerCancellationTest.java` (extend Task 4.1's file) — add a test asserting `SqliteManager.mergeStagingTable()` throws `ReplicationCancelledException` and executes no SQL when cancellation is already requested. Run the remaining 6 SQL managers' existing Testcontainers-backed merge tests (`Db2`, `MySQL`, `Oracle`, `SQLServer`, `Postgresql`, `StandardJDBC` incremental-mode suites) unmodified to confirm the non-cancelled merge path is unaffected. `MongoDBManager`'s merge-path cancellation guard is exercised only by the existing non-cancelled `Mongo2CsvFileTest`/`Mongo2OrcFileTest` incremental-mode assertions (same accepted Testcontainers-coverage gap as Task 5.1 — the guard code is present, but no dedicated mid-merge cancellation test is added for Mongo in this plan).
  Dependencies: Tasks 2.1, 1.2

### 7. Cancellable Task Execution and Exit Code

- [x] **7.1 Replace the blocking `invokeAll` with individually submitted, cancellable futures**
  Files: `src/main/java/org/replicadb/ReplicaDB.java`
  Changes: In `executeReplicationTasks(...)`, replace `final List<Future<ReplicaTaskResult>> futures = replicaTasksService.invokeAll(replicaTasks);` with a loop that calls `replicaTasksService.submit(task)` for each task in `replicaTasks`, collecting the results into `final List<Future<ReplicaTaskResult>> futures = new ArrayList<>();`. When iterating to call `future.get()`, unwrap a `ReplicationCancelledException` cause, cancel the remaining futures with `future.cancel(true)`, and rethrow it so the caller can distinguish cancellation from failure; if the cancellation flag is already set but the driver reports a normal SQL exception, normalize that cause to `ReplicationCancelledException` as well. Add `java.sql.SQLException` to `executeReplicationTasks(...)`'s `throws` clause since cancellation is checked; when collection fails before the executor can be returned, call `replicaTasksService.shutdownNow()` before rethrowing so no worker pool is leaked.
  Tests: Covered by Task 7.2's `ReplicaDBCancellationTest` (this task changes internal control flow only; behavioral proof is at the `processReplica` level).
  Dependencies: Task 1.2

- [x] **7.2 Add a `CANCELLED` exit code and propagate it through `executeSingleReplication`/`processReplica`**
  Files: `src/main/java/org/replicadb/ReplicaDB.java`
  Changes: Add `static final int CANCELLED = 2;` (package-private, not `private`, so `ReplicaDBCancellationTest` in the same `org.replicadb` package can assert against `ReplicaDB.CANCELLED` directly) alongside the existing `private static final int SUCCESS`/`ERROR` constants. In `executeSingleReplication`'s exception handling, map `ReplicationCancelledException` and any operation failure observed while the execution-context cancellation flag is set to `CANCELLED`, logging at `INFO` rather than `ERROR`; this covers JDBC drivers that report `Statement.cancel()` as a normal `SQLException`. Do not change the existing `finally { cleanupResources(...); }` block — it already runs unconditionally, so the auto-generated staging table drop and connection cleanup already happen correctly on the cancelled path.
  Tests: `src/test/java/org/replicadb/ReplicaDBCancellationTest.java` (new, following the `RecordingManager`/`StubManagerFactory` pattern from `ReplicaTaskTest.java` and `ReplicaDBMultipleTablesTest.java`) — a `RecordingManager` sink whose `insertDataToTable(...)` counts down a `CountDownLatch startedLatch = new CountDownLatch(1)` on its first invocation and then loops calling `options.getExecutionContext().isCancellationRequested()` (polling, since the stub is not a real JDBC manager) until `true`, then throws `new ReplicationCancelledException(...)`; the test calls `ReplicaDB.processReplica(options, factory)` on a background thread, the main thread calls `startedLatch.await()` then `options.getExecutionContext().requestCancellation()`, and finally joins the background thread to read its result; assert the returned exit code equals `ReplicaDB.CANCELLED`, not `ERROR`; assert the `RecordingManager`'s `cleanUp()` method was still invoked (add a `boolean cleanUpCalled` flag to the stub).
  Dependencies: Tasks 7.1, 1.1, 1.2

### 8. Cleanup Semantics on Cancellation

- [x] **8.1 Add regression tests for staging-table cleanup rules on a cancelled run**
  Files: `src/test/java/org/replicadb/ReplicaDBCancellationTest.java` (extend Task 7.2's file)
  Changes: Add two test methods: `dropsAutoGeneratedStagingTableWhenCancelled` (options without `--sink-staging-table` set; assert the stub sink's staging-drop-equivalent flag was set after a cancelled run) and `preservesUserProvidedStagingTableWhenCancelled` (options with `--sink-staging-table` explicitly set; assert the stub records that no drop was attempted). Extend `RecordingManager` with a `boolean stagingTableDropped` flag settable from `cleanUp()`, gated on `options.getSinkStagingTable() == null || options.getSinkStagingTable().isEmpty()` to mirror the real `dropStagingTable()` guard in `SqlManager`.
  Tests: This task *is* the test; it directly encodes the Decision 5 cleanup contract ("the engine runs its normal cleanup path and drops the staging table when it was auto-generated; a user-provided `sink-staging-table` is left untouched").
  Dependencies: Task 7.2

> ⚠️ Critic note: Decision 5's cancellation severity table enumerates six distinct outcome cells across `incremental`/`complete-atomic`/`complete` modes × before-cleanup/during-merge timing (e.g., "partially merged rows, watermark not advanced" for `incremental`, "swap interrupted, sink indeterminate" for `complete-atomic`). This plan's tests verify that cancellation is detected and that generic cleanup (staging drop/preserve) runs, but no task asserts the specific per-mode/per-timing outcomes from that table. Treat this as a known coverage gap to close in a short follow-up test-only task before relying on this plan for the full Decision 5 contract.

### 9. Documentation Sync

- [x] **9.1 Update `ARCHITECTURE_DECISIONS.md` status markers for completed cancellation plumbing**
  Files: `ARCHITECTURE_DECISIONS.md`
  Changes: Update the "Cancellation plumbing" bullet (around line 375) to note it is implemented by this plan (reference the commit once available, following the `c228ddc`/Phase 0-a precedent). Update the checklist line `- [ ] Add cancellation: statement handles, interrupt checks, cancellable futures.` (around line 673) to `- [x]`, annotated as completed by this plan; leave the `Implement watermark injection...` checklist line unchecked. Update the exit-criteria bullet "A cancellation request stops an in-flight replication, including during a merge, and the run ends in `CANCELLED`" to note it is met, with the documented accepted limitations (SQL Server `BulkCopy`, MongoDB/Kafka/ORC test-coverage gap) called out inline. Update the final "Next Review" line to reference the Phase 0-b2 watermark plan instead of "Phase 0-b cancellation and watermark changes".
  Tests: N/A (documentation only); reviewed manually for accuracy against the tasks actually completed.
  Dependencies: Tasks 6.2, 7.2, 5.2

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

```java
// org.replicadb.execution.ReplicationExecutionContext (additions)
private final AtomicBoolean cancellationRequested = new AtomicBoolean(false);
private final Set<Statement> activeStatements = ConcurrentHashMap.newKeySet();

public void requestCancellation() {
    cancellationRequested.set(true);
    cancelActiveStatements();
}
public boolean isCancellationRequested() { return cancellationRequested.get(); }
public void registerActiveStatement(Statement statement) { activeStatements.add(statement); }
public void unregisterActiveStatement(Statement statement) { activeStatements.remove(statement); }
private void cancelActiveStatements() {
    for (Statement statement : activeStatements) {
        try { statement.cancel(); }
        catch (SQLException e) { LOG.warn("Failed to cancel statement for run {}: {}", runId, e.getMessage()); }
    }
}

// org.replicadb.execution.ReplicationCancelledException
public final class ReplicationCancelledException extends SQLException {
    public ReplicationCancelledException(String message) { super(message); }
}

// org.replicadb.manager.ConnManager (additions)
protected void checkCancellation() throws ReplicationCancelledException { ... }
protected void registerActiveStatement(Statement statement) { ... }
protected void unregisterActiveStatement(Statement statement) { ... }

// org.replicadb.ReplicaDB (additions)
static final int CANCELLED = 2; // package-private: visible to ReplicaDBCancellationTest in the same package
```

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed: 16/16 (100%)
- Tasks that required plan adjustment: 3/16 (19%)
- Test loop iterations: 11 total (7 first-pass, 4 repair loops, 0 third-pass)

### Gaps Encountered

#### Gap 1: SQLite cancellation test package differed from the planned path (Intent-to-Plan)

- **Task**: 4.1 — Wire cancellation into the batched `PreparedStatement` insert loops
- **Plan assumed**: The new SQLite cancellation test would live under `src/test/java/org/replicadb/sqlite/`.
- **Reality**: The existing SQLite manager tests use package `org.replicadb.manager` and there is no `org/replicadb/sqlite/` test package.
- **Resolution**: Added `SqliteManagerCancellationTest` beside `SqliteManagerNullHandlingTest` under `src/test/java/org/replicadb/manager/`.
- **Learning**: New test paths should follow the package of the nearest existing fixture, not the database family label alone.

#### Gap 2: Executor ownership ended before caller cleanup on task failure (Plan-to-Implementation)

- **Task**: 7.1 — Replace the blocking `invokeAll` with individually submitted, cancellable futures
- **Plan assumed**: The existing `cleanupResources(...)` call would always receive the task executor and shut it down after a cancellation.
- **Reality**: The executor is returned from `executeReplicationTasks(...)`; when that method throws before returning, the caller's assignment remains `null` and cleanup cannot see the pool.
- **Resolution**: `executeReplicationTasks(...)` now calls `shutdownNow()` before rethrowing `InterruptedException`, `ExecutionException`, or `SQLException`.
- **Learning**: When a method transfers ownership by returning a resource, failure paths before return must close that resource locally.

#### Gap 3: JDBC drivers may report `Statement.cancel()` as a normal SQL exception (Plan-to-Implementation)

- **Task**: 7.2 — Add a `CANCELLED` exit code and propagate it through the orchestrator
- **Plan assumed**: A cancelled task would always propagate `ReplicationCancelledException` directly from the copy loop.
- **Reality**: A driver can throw an ordinary `SQLException` after its statement is cancelled, especially during merge or a blocking JDBC call.
- **Resolution**: The executor and outer orchestration path normalize failures observed while the execution-context cancellation flag is set to `ReplicationCancelledException`/`CANCELLED`, preserving the original cause; a dedicated regression test covers this behavior.
- **Learning**: Cancellation classification must use both the cooperative token and the thrown exception type because native driver cancellation errors are not uniform.

### Patterns Discovered

- Active JDBC resources can be registered centrally through `ConnManager` and `SqlManager`, while non-JDBC managers use cooperative loop checks at their native operation boundary.
- Registering a statement after the cancellation flag is set must cancel it immediately, closing the race between task startup and an external cancellation request.
- `CountDownLatch`-controlled result-set fixtures provide deterministic mid-stream cancellation tests without sleeps or CI timing assumptions.

Known coverage limits remain from the plan: no dedicated mid-operation cancellation tests for MongoDB, Kafka, or ORC, and no direct assertions for every mode/timing cell in Decision 5's sink-severity table. Watermark extraction/injection remains deferred to Phase 0-b2.

<details>
<summary>Dependencies</summary>

No new Maven dependencies. This plan uses `java.sql.Statement`, `java.util.concurrent.atomic.AtomicBoolean`, and `java.util.concurrent.ConcurrentHashMap.newKeySet()`, all already available. Mockito (already a test dependency, used elsewhere in the suite) is used for `Statement` mocking in the new context/`ConnManager` unit tests.

</details>

<details>
<summary>Testing Strategy</summary>

- Foundation classes (`ReplicationExecutionContext`, `ReplicationCancelledException`, `ConnManager`/`FileManager` helpers) are covered by fast unit tests using Mockito-mocked `Statement`s — no database required.
- Manager-level cancellation is proven with `@TempDir`-backed SQLite fixtures (following the existing `SqliteManagerNullHandlingTest` pattern) wherever a fast, non-Testcontainers path exists (`StandardJDBCManager`/`SqliteManager` batch-insert and merge paths, CSV file writer).
- PostgreSQL binary `COPY` and SQL Server `BulkCopy` cancellation are verified with Testcontainers (Postgres) and a guard-clause unit test (SQL Server), respectively, since their APIs are not `Statement`-based.
- **Accepted test-coverage gap** (documented, not silently dropped): `MongoDBManager`, `KafkaManager`, and `OrcFileManager`'s cancellation *paths* are wired (Tasks 5.1/5.2) but not covered by a dedicated mid-stream cancellation test in this plan — their Testcontainers setups make a deterministic mid-stream trigger impractical within this plan's scope. Existing non-cancelled integration tests for these three continue to run unmodified, proving the added `checkCancellation()` calls do not regress normal operation.
- Run `mvn -Dtest=org.replicadb.execution.*,ConnManagerCancellationTest,FileManagerCancellationTest,SqlManagerCancellationTest,SqliteManagerCancellationTest,CsvFileManagerCancellationTest,ReplicaDBCancellationTest,SQLServerManagerCancellationGuardTest test` as the fast regression slice for this plan before running the full Testcontainers-backed suite (`PostgresqlManagerCancellationTest` and the unmodified per-manager insert/merge suites).

</details>
