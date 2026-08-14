# Implementation Plan: Phase 0a — Per-Run Execution Context & Rich Task Result Foundation

## Task Source

`ARCHITECTURE_DECISIONS.md`, Decision 2 ("Monolithic Control Plane First") and the "Phase 0: Core and State Foundation" implementation phase. No JIRA ticket; this plan implements the first of three sub-plans the user selected to split Phase 0 into:

1. **Concurrency + Execution Context** (this plan)
2. Cancellation + Watermark Injection (future plan)
3. State Layer + Build Split (future plan)

Acceptance criteria extracted from the architecture document, scoped to this plan:

- Replace the process-global `ConnManager.randomSinkStagingTableName` and `FileManager.tempFilesPath` statics with state owned by a single run, so two runs can execute concurrently in one JVM without colliding.
- `ReplicaTask` must return a result carrying row counters and timings (the watermark field is added now as reserved plumbing — Decision 2 explicitly says "progress counters and watermarks are the same plumbing and are built once" — but it is not populated until the follow-up watermark-injection plan).
- Exit criterion (Phase 0, scoped to this plan): "Two replications run concurrently in one JVM with independent staging tables and temporary files" — reproduced across 100 repetitions per the Phase 0 success metric.
- CLI behavior, exit codes, and existing `ToolOptions` configuration (including multi-table options files) remain compatible.

**Explicitly out of scope for this plan** (deferred to Plan 2): cancellation tokens, `Statement` handle exposure, interrupt checks in the copy loop, replacing `invokeAll`, watermark predicate injection, and type inference from source column metadata. **Deferred to Plan 3**: `JobDefinition`/`JobRun` persistence, Flyway/PostgreSQL, the claim mechanism, and the `replicadb`/`replicadb-server` artifact split.

## Overview

ReplicaDB's core currently uses two process-global static fields — `ConnManager.randomSinkStagingTableName` and `FileManager.tempFilesPath` — to share generated staging-table names and temp-file paths across the parallel tasks of a single replication run. This works today because the CLI only ever runs one replication at a time, resetting the statics before each table. It silently breaks the moment two replications execute concurrently in the same JVM (the future `api`/`worker` profiles), because the second run's reset would overwrite the first run's in-flight state.

This plan introduces a `ReplicationExecutionContext` object owned by each `ToolOptions` instance instead of process-global statics, so isolation is a structural property of "one `ToolOptions` instance per run" rather than an explicit reset call. It also widens `ReplicaTask`'s return type from `Integer` to a `ReplicaTaskResult` record carrying row counts and timings, laying the plumbing the follow-up cancellation/watermark plan will populate further. No new dependencies are introduced; this is a pure-Java refactor of `org.replicadb` and `org.replicadb.manager`.

## Architecture & Design

**Approach**: Attach the execution context to `ToolOptions` rather than introducing a new constructor parameter threaded through every `ConnManager`/`FileManager` subclass.

- `ToolOptions` is already the configuration boundary passed to every manager constructor (`new XManager(options, dsType)`) and to every `ReplicaTask`. Both `ConnManager` and `FileManager` already hold a `protected ToolOptions options` field.
- `ToolOptions.forReplicationTable(...)` already creates a brand-new `ToolOptions` instance (via its private no-arg constructor) for each entry in a multi-table run. A `ReplicationExecutionContext` field initialized inline on `ToolOptions` therefore gets a fresh instance per table automatically, with zero explicit reset call needed — this replaces and removes the existing `ConnManager.resetGeneratedSinkStagingTableName()` / `FileManager.setTempFilesPath(new HashMap<>())` calls in `ReplicaDB.executeSingleReplication`.
- Within one run, every task-owned `ConnManager`/`FileManager` instance receives the **same** `ToolOptions` object reference (verified: `ReplicaDB` passes one `options` instance to all `ReplicaTask`s of a run), so they naturally share one `ReplicationExecutionContext` — preserving the current intra-run sharing semantics (all parallel tasks write to the same generated staging table) while eliminating cross-run collisions.
- This avoids touching the constructor signature of ~20 `ConnManager`/`FileManager` subclasses and the large number of tests that construct them directly (`new PostgresqlManager(options, dsType)`, etc.).

**Rejected alternative**: Pass the context as an explicit new constructor parameter to every manager. Rejected because it touches every manager subclass and every test that constructs one directly, for no behavioral benefit over the `ToolOptions`-attached approach.

**Rejected alternative**: `ThreadLocal`-based context. Rejected because tasks of one run execute on a thread pool (`Executors.newFixedThreadPool`), so a per-thread context would not be shared across the parallel tasks of the same run — it would break the existing "all tasks share one staging table" behavior instead of just fixing cross-run isolation.

## Implementation Tasks

### 1. Execution Context Foundation

- [x] **1.1 Create `ReplicationExecutionContext`**
  Files: `src/main/java/org/replicadb/execution/ReplicationExecutionContext.java` (new)
  Changes: New class holding per-run state: a `runId` (`String`, generated via `UUID.randomUUID().toString()` at construction, exposed via `getRunId()`), a nullable `sinkStagingTableName` with `getSinkStagingTableName()`/`setSinkStagingTableName(String)`, and a temp-file map (`Map<Integer, String>` backed by `ConcurrentHashMap` for thread-safe concurrent task access) with `setTempFilePath(int, String)`, `getTempFilePath(int)`, `getTempFilesPath()`, `getTempFilePathSize()`. No cancellation token or watermark accumulator field yet — those are added in Plan 2 on this same class.
  Tests: `src/test/java/org/replicadb/execution/ReplicationExecutionContextTest.java` (new) — verify `getRunId()` is non-null and non-empty; verify `getSinkStagingTableName()` starts `null`; verify `setSinkStagingTableName`/`getSinkStagingTableName` round-trip; verify `setTempFilePath`/`getTempFilePath`/`getTempFilePathSize` behavior with 0, 1, and multiple entries; verify two separately constructed instances have different `runId`s.
  Dependencies: None

- [x] **1.2 Wire the context into `ToolOptions`**
  Files: `src/main/java/org/replicadb/cli/ToolOptions.java`
  Changes: Add `import org.replicadb.execution.ReplicationExecutionContext;` and a field `private final ReplicationExecutionContext executionContext = new ReplicationExecutionContext();` plus `public ReplicationExecutionContext getExecutionContext() { return executionContext; }`. Do **not** copy this field inside `forReplicationTable(...)` — leave the `copy` object's field initializer create its own fresh instance, which is what gives per-table isolation in the multi-table loop.
  Tests: `src/test/java/org/replicadb/cli/ToolOptionsExecutionContextTest.java` (new) — verify `new ToolOptions(args).getExecutionContext()` is non-null; verify two separately constructed `ToolOptions` instances return different (`assertNotSame`) `ReplicationExecutionContext` objects; verify `forReplicationTable(...)` returns an instance whose `getExecutionContext()` is `assertNotSame` to the base options' context.
  Dependencies: Task 1.1

### 2. Remove Static Staging-Table State (`ConnManager`)

- [x] **2.1 Refactor `ConnManager` staging-name accessors to use the per-run context**
  Files: `src/main/java/org/replicadb/manager/ConnManager.java`
  Changes: Remove `private static String randomSinkStagingTableName;` and `public static void resetGeneratedSinkStagingTableName()`. Rewrite `getSinkStagingTableName()` to read/write the generated name via `options.getExecutionContext().getSinkStagingTableName()` / `setSinkStagingTableName(...)` instead of the static field, keeping the existing precedence (explicit `options.getSinkStagingTable()` first, then the previously generated name for this run, then generate and cache a new one using `sinkStagingTableAlias` when present). `getQualifiedStagingTableName()` and all other methods are unchanged since they already call `getSinkStagingTableName()`.
  Tests: Covered by Task 2.2 (rewritten `ConnManagerStagingIsolationTest`).
  Dependencies: Task 1.2

- [x] **2.2 Rewrite `ConnManagerStagingIsolationTest` for the new per-`ToolOptions` isolation model**
  Files: `src/test/java/org/replicadb/manager/ConnManagerStagingIsolationTest.java`
  Changes: Remove all calls to the now-deleted `ConnManager.resetGeneratedSinkStagingTableName()`. Replace `generatedStagingNameIsSharedWithinRunButResetBetweenTables` with two tests: `generatedStagingNameIsSharedAcrossManagersOfTheSameRun` (two `StagingManager` instances built from the **same** `ToolOptions` instance return the same generated name) and `generatedStagingNameIsIsolatedAcrossDifferentRuns` (two `StagingManager` instances built from **two separate** `ToolOptions` instances — simulating two concurrent/sequential runs — return different generated names). Keep `userDefinedStagingNameIsNotReplacedByReset` (renamed `userDefinedStagingNameIsNotReplacedByGeneration`), dropping its reset call.
  Tests: This task *is* the test update; run `mvn -Dtest=ConnManagerStagingIsolationTest test` to confirm all three scenarios pass.
  Dependencies: Task 2.1

### 3. Remove Static Temp-File State (`FileManager`)

- [x] **3.1 Refactor `FileManager` temp-file accessors to use the per-run context**
  Files: `src/main/java/org/replicadb/manager/file/FileManager.java`
  Changes: Remove `protected static Map<Integer, String> tempFilesPath;`, the constructor's call to `newTempFilesPath()`, and the static `newTempFilesPath()`/`getTempFilesPath()`/`setTempFilesPath()`/`setTempFilePath()`/`getTempFilePath()`/`getTempFilePathSize()` methods. Add instance methods with the same names and signatures (minus `static`), each delegating to `options.getExecutionContext()` (e.g., `public void setTempFilePath(int taskId, String path) { options.getExecutionContext().setTempFilePath(taskId, path); }`). Keep them `public` (not narrowed) since `LocalFileManager`, a different package, calls them on a held `FileManager` instance. Remove the now-unused `java.util.HashMap` import if no longer referenced.
  Tests: Covered by Tasks 3.4 (existing manager/file tests continue to pass since `CsvFileManager`/`OrcFileManager` call these accessors unqualified and unchanged).
  Dependencies: Task 1.2

- [x] **3.2 Update `LocalFileManager` to use the instance-based accessor**
  Files: `src/main/java/org/replicadb/manager/LocalFileManager.java`
  Changes: In `insertDataToTable(...)`, change `FileManager.setTempFilePath(taskId, randomFileUrl);` (static class-qualified call) to `this.fileManager.setTempFilePath(taskId, randomFileUrl);` (instance call on the already-held `fileManager` field).
  Tests: Existing `LocalFileManager`-backed tests under `src/test/java/org/replicadb/file/` (if present) or the relevant `*2CsvFileTest`/`*2OrcFileTest` classes continue to pass unmodified in behavior, only losing their now-unnecessary static reset lines (Task 3.4).
  Dependencies: Task 3.1

- [x] **3.3 Remove obsolete static resets in `ReplicaDB.executeSingleReplication`**
  Files: `src/main/java/org/replicadb/ReplicaDB.java`
  Changes: Remove the lines `ConnManager.resetGeneratedSinkStagingTableName();` and `FileManager.setTempFilesPath(new HashMap<>());` (both now reference deleted static methods) and their preceding comment. Remove the now-unused `import java.util.HashMap;` and `import org.replicadb.manager.file.FileManager;` (confirmed unused elsewhere in this file).
  Tests: Existing `ReplicaDBTest`, `ReplicaDBMultipleTablesTest` (post Task 3.4 update), and `ReplicaTaskAuthenticationFailureTest` continue to pass — isolation is now structural (fresh `ToolOptions` per run/table) rather than reset-driven.
  Dependencies: Tasks 2.1, 3.1

- [x] **3.4 Update existing tests that call the removed static `FileManager` methods**
  Files:
  - `src/test/java/org/replicadb/ReplicaDBMultipleTablesTest.java`
  - `src/test/java/org/replicadb/db2/DB22CsvFileTest.java`
  - `src/test/java/org/replicadb/mariadb/MariaDB2CsvFileTest.java`
  - `src/test/java/org/replicadb/mariadb/MariaDB2OrcFileTest.java`
  - `src/test/java/org/replicadb/mongo/Mongo2CsvFileTest.java`
  - `src/test/java/org/replicadb/mongo/Mongo2OrcFileTest.java`
  - `src/test/java/org/replicadb/mysql/MySQL2CsvFileTest.java`
  - `src/test/java/org/replicadb/mysql/MySQL2OrcFileTest.java`
  - `src/test/java/org/replicadb/oracle/Oracle2CsvFileTest.java`
  - `src/test/java/org/replicadb/oracle/Oracle2OrcFileTest.java`
  - `src/test/java/org/replicadb/postgres/Postgres2CsvFileTest.java`
  - `src/test/java/org/replicadb/postgres/Postgres2OrcFileTest.java`
  - `src/test/java/org/replicadb/sqlserver/Sqlserver2CsvFileTest.java`
  - `src/test/java/org/replicadb/sqlserver/Sqlserver2OrcFileTest.java`

  Changes: In the 13 `*2CsvFileTest`/`*2OrcFileTest` files, delete every `FileManager.setTempFilesPath(new HashMap<>());` line (and the now-unused `HashMap`/`Map` imports where nothing else in the file uses them) — each test already constructs its own `ToolOptions`, so isolation is automatic and the explicit reset is dead code. In `ReplicaDBMultipleTablesTest.java`: (a) in `RecordingManagerFactory.accept(...)`, change `tempFileSizes.add(FileManager.getTempFilePathSize());` to `tempFileSizes.add(options.getExecutionContext().getTempFilePathSize());`; (b) in `resetsStagingAndTemporaryStateAtEachTableBoundary`, delete the `FileManager.setTempFilesPath(new HashMap<>(Map.of(7, "stale-file")));` seeding line entirely — since `forReplicationTable(...)` now gives every table its own fresh context, the assertion that all 8 recorded sizes are `0` holds structurally without needing to simulate a stale leftover; remove the now-unused `HashMap`/`Map` imports if nothing else in the file uses them.
  Tests: Run `mvn -Dtest=ReplicaDBMultipleTablesTest test` to confirm `resetsStagingAndTemporaryStateAtEachTableBoundary` and the other five tests in that class still pass. The 13 file-manager integration tests are Testcontainers-backed; compile and run the smallest relevant one (e.g., `MySQL2CsvFileTest`) to confirm the deleted lines were dead code with no behavior change. **Verification step (mitigates an incomplete file inventory)**: after all edits, run `grep -rn "FileManager\.\(setTempFilesPath\|newTempFilesPath\|getTempFilePathSize\|getTempFilePath\|getTempFilesPath\|setTempFilePath\)" src/` and confirm zero matches remain (the only legitimate remaining calls are unqualified instance calls inside `CsvFileManager`/`OrcFileManager`, which do not match this class-qualified pattern), then run `mvn test-compile` to confirm no other file in the repository references the deleted static methods. If either check finds a match outside the list above, add that file to this task before proceeding.
  Dependencies: Tasks 3.1, 3.2

### 4. Concurrency Proof (Phase 0 Exit Criterion)

- [x] **4.1 Add a concurrent-run isolation test covering staging names and temp-file maps**
  Files: `src/test/java/org/replicadb/execution/ReplicationExecutionContextConcurrencyTest.java` (new)
  Changes: A JUnit Jupiter test that, across 100 iterations, launches two `ToolOptions` instances (representing two concurrent runs) on two separate threads (`ExecutorService` with 2 threads, `CountDownLatch` or `CyclicBarrier` to force overlap), each thread: builds a small `ConnManager` stub (analogous to `StagingManager` in `ConnManagerStagingIsolationTest`) with its own `ToolOptions`, calls `getSinkStagingTableName()`, and writes several entries into `options.getExecutionContext()`'s temp-file map via `setTempFilePath(taskId, path)`. After both threads complete each iteration, assert: the two generated staging names differ; each context's `getTempFilesPath()` contains exactly the entries that thread wrote, with no entries from the other thread (`assertEquals` on the expected map, not just size, to catch cross-run key collisions). Fail the test (not just log) on any iteration where isolation breaks.
  Tests: This task *is* the test; it directly encodes the Phase 0 success metric "Two concurrent runs in one JVM produce two distinct staging tables and zero cross-run interference across 100 repetitions."
  Dependencies: Tasks 2.1, 3.1

### 5. Rich Task Result

- [x] **5.1 Introduce the `ReplicaTaskResult` record**
  Files: `src/main/java/org/replicadb/ReplicaTaskResult.java` (new)
  Changes: `public record ReplicaTaskResult(int taskId, long rowsProcessed, long startedAtMillis, long finishedAtMillis, String watermarkCandidate)` with a default `durationMillis()` method (`finishedAtMillis - startedAtMillis`). `watermarkCandidate` is always `null` in this plan; it is populated by the follow-up watermark-injection plan. Add a compact constructor validating `rowsProcessed >= 0` and `finishedAtMillis >= startedAtMillis`, throwing `IllegalArgumentException` otherwise.
  Tests: `src/test/java/org/replicadb/ReplicaTaskResultTest.java` (new) — verify `durationMillis()` arithmetic; verify the compact constructor rejects a negative `rowsProcessed` and a `finishedAtMillis` earlier than `startedAtMillis`.
  Dependencies: None

- [x] **5.2 Widen `ReplicaTask` to return `ReplicaTaskResult`**
  Files: `src/main/java/org/replicadb/ReplicaTask.java`
  Changes: Change `implements Callable<Integer>` to `implements Callable<ReplicaTaskResult>`. In `call()`, capture `long startedAt = System.currentTimeMillis();` before opening connections, and on the success path replace `return this.taskId;` with `return new ReplicaTaskResult(this.taskId, processedRows, startedAt, System.currentTimeMillis(), null);` (reusing the existing `int processedRows = sinkDs.insertDataToTable(rs, taskId);` local variable, widened to `long`). Exception propagation in the `catch`/`finally` blocks is unchanged.
  Tests: Confirm the five existing `ReplicaTaskAuthenticationFailureTest` cases that call `.call()` inside `assertThrows` still compile and pass unmodified — they only assert on the thrown exception, not the return value. New success-path coverage is added in Task 5.4's new file, not in this file.
  Dependencies: Task 5.1

- [x] **5.3 Update `ReplicaDB.executeReplicationTasks` to aggregate task results**
  Files: `src/main/java/org/replicadb/ReplicaDB.java`
  Changes: Change `final List<Future<Integer>> futures = replicaTasksService.invokeAll(replicaTasks);` to `final List<Future<ReplicaTaskResult>> futures = ...` and the loop to collect each `future.get()` into a `List<ReplicaTaskResult> results`. Extract a package-private, pure static method `static ReplicaTaskResultsSummary summarize(List<ReplicaTaskResult> results)` (in `ReplicaDB.java`, or as a small nested/record type `ReplicaTaskResultsSummary(long totalRowsProcessed, long maxDurationMillis, int taskCount)`) that computes the sum of `rowsProcessed()` and the max of `durationMillis()` across the list. Call it after the `futures` loop and log the result once via `LOG.info("Replication tasks completed: {} rows across {} tasks, longest task {}ms", summary.totalRowsProcessed(), summary.taskCount(), summary.maxDurationMillis());`. Extracting `summarize(...)` as a pure function (no logging, no I/O) makes it directly unit-testable without a log-capturing appender. The method's return type (`ExecutorService`) and the public `processReplica`/exit-code contract are unchanged — this is an internal aggregation and logging improvement only, not a new public surface.
  Tests: `src/test/java/org/replicadb/ReplicaDBMultipleTablesTest.java` and `src/test/java/org/replicadb/ReplicaDBTest.java` continue to pass unmodified (exit codes unchanged). Add `src/test/java/org/replicadb/ReplicaDBTaskSummaryTest.java` (new, same package `org.replicadb` so it can call the package-private `ReplicaDB.summarize(...)`): unit tests calling `ReplicaDB.summarize(...)` directly with (a) an empty list → `totalRowsProcessed == 0`, `maxDurationMillis == 0`, `taskCount == 0`; (b) three `ReplicaTaskResult` values with distinct row counts and durations → asserts the exact expected sum and max; (c) a single-element list → sum and max equal that element's values.
  Dependencies: Task 5.2

- [x] **5.4 Add a `ReplicaTask` success-path test for the rich result**
  Files: `src/test/java/org/replicadb/ReplicaTaskTest.java` (new)
  Changes: New self-contained test file (kept separate from `ReplicaTaskAuthenticationFailureTest.java`, which is scoped to failure scenarios) with its own minimal `RecordingManager extends ConnManager` and `StubManagerFactory extends ManagerFactory` fixtures (mirroring the pattern in `ReplicaTaskAuthenticationFailureTest.java`, not reused from it since those inner classes are `private` to that file). The `RecordingManager` used as sink returns a fixed row count (e.g., `42`) from `insertDataToTable(...)` and `null` from `readTable(...)`.
  Tests: `succeedsAndReturnsRichResultWithRowCountAndTimings` — calls `new ReplicaTask(0, options, new StubManagerFactory(source, sink)).call()` and asserts the returned `ReplicaTaskResult.taskId() == 0`, `rowsProcessed() == 42`, `finishedAtMillis() >= startedAtMillis()`, `durationMillis() >= 0`, and `watermarkCandidate() == null`.
  Dependencies: Task 5.2

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

```java
// org.replicadb.execution.ReplicationExecutionContext
public final class ReplicationExecutionContext {
    private final String runId = UUID.randomUUID().toString();
    private final Map<Integer, String> tempFilesPath = new ConcurrentHashMap<>();
    private volatile String sinkStagingTableName;

    public String getRunId() { return runId; }
    public String getSinkStagingTableName() { return sinkStagingTableName; }
    public void setSinkStagingTableName(String name) { this.sinkStagingTableName = name; }
    public void setTempFilePath(int taskId, String path) { tempFilesPath.put(taskId, path); }
    public String getTempFilePath(int taskId) { return tempFilesPath.get(taskId); }
    public Map<Integer, String> getTempFilesPath() { return tempFilesPath; }
    public int getTempFilePathSize() { return tempFilesPath.size(); }
}

// org.replicadb.ReplicaTaskResult
public record ReplicaTaskResult(int taskId, long rowsProcessed, long startedAtMillis,
                                 long finishedAtMillis, String watermarkCandidate) {
    public long durationMillis() { return finishedAtMillis - startedAtMillis; }
}

// org.replicadb.ReplicaDB (package-private nested type, pure function — no logging/I/O)
record ReplicaTaskResultsSummary(long totalRowsProcessed, long maxDurationMillis, int taskCount) {}

static ReplicaTaskResultsSummary summarize(List<ReplicaTaskResult> results) { ... }
```

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 13/13 (100%)
- Tasks that required plan adjustment: 0/13 (0%)
- Test loop iterations: 3 repair loops (first-pass validations otherwise passed; no third-pass loops)
- Full-suite validation: focused checks passed; `mvn -q test` was stopped after 17 minutes during Oracle cross-version container startup on arm64 emulation and produced no final Maven result.

### Gaps Encountered

#### Gap 1: Result accumulator scope needed an explicit declaration (Plan-to-Implementation)
- **Task**: 5.2 — Widen `ReplicaTask` to return `ReplicaTaskResult`
- **Plan assumed**: The existing row-count local could be widened and reused directly when constructing the result.
- **Reality**: The first implementation kept the declaration inside the insert block while constructing the result after that block, which failed compilation.
- **Resolution**: Declared the widened accumulator before the insert block and assigned it inside the block; the focused compile then passed.
- **Learning**: When a task result is assembled after nested lifecycle blocks, plans should specify the accumulator's scope explicitly, not only its type.

### Patterns Discovered
- Per-run mutable state can be attached to the existing `ToolOptions` boundary to preserve shared state across parallel task managers without threading a new constructor dependency through every manager.
- Context-owned `ConcurrentHashMap` state supports concurrent task writes while keeping multi-table `ToolOptions.forReplicationTable(...)` copies isolated.

<details>
<summary>Dependencies</summary>

No new Maven dependencies. This plan is a pure `java.util.concurrent`/JDK refactor of `org.replicadb`, `org.replicadb.cli`, and `org.replicadb.manager`(`.file`). Spring JDBC, Flyway, and PostgreSQL are introduced only in Plan 3 (State Layer & Build Split).

</details>

<details>
<summary>Testing Strategy</summary>

- Unit tests only in this plan — no Testcontainers-dependent behavior changes, only dead-code removal in Testcontainers-backed test classes (Task 3.4).
- The concurrency exit criterion (Task 4.1) is validated with plain threads and in-memory stubs, not real database connections, to keep it fast and deterministic while still proving the structural isolation property.
- Aggregation logic (Task 5.3) is extracted into a pure, package-private `summarize(...)` function so it is unit-testable directly without a log-capturing appender or mock logger.
- Run `mvn -Dtest=org.replicadb.execution.*,ConnManagerStagingIsolationTest,ReplicaDBMultipleTablesTest,ReplicaDBTest,ReplicaDBTaskSummaryTest,ReplicaTaskAuthenticationFailureTest,ReplicaTaskTest,ReplicaTaskResultTest test` as the fast regression slice for this plan before running the full suite.
- Focused CLI compatibility is verified by `ReplicaDBMultipleTablesTest` passing with unchanged exit-code and lifecycle behavior; the full Testcontainers matrix remains environment-limited by the Oracle startup hang above.

</details>
