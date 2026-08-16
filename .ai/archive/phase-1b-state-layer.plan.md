# Implementation Plan: Phase 1b — Control Plane State Layer

## Task Source

No JIRA ticket. Source is [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md), which explicitly scopes the next slice:

> "The next slice is **Phase 1b: State layer**, covering `JobDefinition`/`JobRun`, Flyway migrations, PostgreSQL persistence, row-locking claims, and the execution service. REST resources, scheduling, security, and the frontend remain pending after Phase 1a."

Acceptance criteria (derived from Decision 1, Decision 2 "State storage", Decision 3, and the "State layer (deferred to Phase 1b)" bullet list):

- Introduce `JobDefinition` and `JobRun` domain models. No `Checkpoint` entity.
- Add a PostgreSQL persistence layer (Spring JDBC, not JPA) for job definitions, run states, and watermarks, versioned with Flyway forward-only migrations.
- Define legal `JobRun` state transitions and a claim mechanism using **PostgreSQL row locking**, not application-level optimistic locking.
- Add an execution service that converts a `JobDefinition` into `ToolOptions` and invokes the existing `ReplicaDB.processReplica(ToolOptions)` compatibility entry point.
- A managed `JobDefinition` targets exactly one source/sink table pair (Decision 1).
- Config references use `${env:VARIABLE}` expansion resolved by the executor immediately before building `ToolOptions`; `${secret:...}` is explicitly rejected as reserved (Decision 4).
- A retry can identify the previous run and attempt number, and never claims to resume it (Decision 3, "No resume").
- A failed or cancelled run leaves the previously committed watermark unchanged; a successful run commits exactly one reduced watermark (Decision 3, exit criteria already met at the core level in Phase 0-b2 — this phase persists it durably).
- Explicitly **out of scope**: REST controllers, Quartz scheduler, authentication/authorization, frontend.

## Overview

ReplicaDB's core CLI already supports cancellation and watermark injection (Phase 0), and `replicadb-server` exists only as a Spring Boot skeleton exposing `/actuator/health`. This phase gives the control plane durable memory: a PostgreSQL-backed `JobDefinition`/`JobRun` model, row-locking claims so a `PENDING` run is picked up at most once, and a service that turns a stored definition into a `ToolOptions` invocation of the unchanged core engine — without yet exposing any of it over HTTP.

## Architecture & Design

**Approach**: Spring JDBC + `NamedParameterJdbcTemplate` repositories (no JPA/Spring Data), Flyway forward-only migrations, PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` for claiming, Testcontainers PostgreSQL for every test that needs the database (via Spring Boot 3.3's `@ServiceConnection`). This is not a choice among alternatives — it is what Decision 2 and the "Resources and tools" section of `ARCHITECTURE_DECISIONS.md` already mandate. The design decisions below are the parts the architecture doc leaves open.

### A required core change

`ReplicaDB.processReplica(ToolOptions)` only returns an exit code (`0`/`1`/`2`). The only run data exposed publicly today is `ReplicationExecutionContext.getWatermarkCandidate()` (set only on success). Decision 3 requires `JobRun` to durably store **row counters and timings** for every outcome, not just success. This requires a small, additive change to the core (`org.replicadb.execution.ReplicationExecutionContext` + `org.replicadb.ReplicaDB`) mirroring exactly how Phase 0-b2 added `watermarkCandidate` — new fields, populated unconditionally right after the replication tasks finish (unlike the watermark, which is conditional on merge success). This does not change the exit-code contract or any CLI-visible behavior.

Confirmed existing types in `ReplicaDB.java` (verified by reading the source, so Task 1.2 is self-contained): a package-private nested `record ReplicaTaskResultsSummary(long totalRowsProcessed, long maxDurationMillis, int taskCount, String watermarkCandidate)` and a private nested `record ReplicationTasksResult(ExecutorService executor, ReplicaTaskResultsSummary summary)`. `executeSingleReplication` already does `final ReplicationTasksResult replicationTasksResult = executeReplicationTasks(options, managerFactory); replicaTasksService = replicationTasksResult.executor();` before `waitForTaskCompletion(...)`/`executePostTasks(...)`. Task 1.2 inserts the two new setter calls right after that existing assignment line, using `replicationTasksResult.summary().totalRowsProcessed()` and `.maxDurationMillis()`.

### Package base

Confirmed: `replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java` declares `package org.replicadb.server;`. All new domain/persistence/execution classes below therefore correctly nest under the existing `org.replicadb.server` base package as `org.replicadb.server.job.*`.

### Credential handling (closes a security gap, implements Decision 4)

Decision 4 requires: "Job definitions must contain configuration references, not passwords... A resolved secret never enters the state store". This means `job_definition.source_password`/`sink_password` must **only ever contain a `${env:VARIABLE}` reference or be null** — never a literal secret — enforced by `JobDefinition`'s validation (Task 2.3), so the database itself never holds a plaintext credential. `JobExecutionService` (Task 5.3) must never log or persist the *resolved* connect strings, passwords, or the built `String[]` args array — only `runId`/`jobDefinitionId` and (via `CredentialRedactor.redactMessage(...)`) a redacted exception message when `ToolOptions` construction itself fails.

### Accepted limitation (documented, not fixed in this phase)

`processReplica` does not expose the underlying exception on failure (it is logged/sent to Sentry internally). `JobRun.errorMessage` for a `FAILED` run will therefore be a generic message ("replication failed; see application logs for run &lt;runId&gt;") in this phase, not the original exception text. Widening the core's error contract is left to a later phase and must not be silently assumed away.

### Domain model

`JobDefinition` (one source/sink table pair, per Decision 1): id, name, source connect/user/password/table/where, sink connect/user/password/table, `mode` (reusing the existing public `org.replicadb.cli.ReplicationMode` enum instead of duplicating it), `jobs`, optional `incrementalWatermarkColumn`, optional `initialWatermarkValue`, `createdAt`/`updatedAt`.

`JobRun`: id, `jobDefinitionId`, `previousRunId` (nullable, so a retry can be traced), `status` (`PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCEL_REQUESTED`, `CANCELLED`, `RETRY_SCHEDULED` — Decision 3's exact list), `attempt`, `executorIdentity`, `leaseUntil`, `heartbeatAt` (columns included now per user decision, unused until Phase 2's distributed-worker lease rules — avoids a forward-only ALTER migration later), `createdAt`/`startedAt`/`finishedAt`, `rowsProcessed`, `durationMillis`, `committedWatermark`, `errorMessage`.

Legal transitions (`JobRunStateMachine`): `PENDING → RUNNING | CANCELLED`, `RUNNING → SUCCEEDED | FAILED | CANCEL_REQUESTED | CANCELLED`, `CANCEL_REQUESTED → CANCELLED`, `FAILED → RETRY_SCHEDULED`. `SUCCEEDED`, `CANCELLED`, and `RETRY_SCHEDULED` are terminal for that row; a retry is always a **new** `JobRun` row referencing `previousRunId`, never a transition back to `RUNNING` on the same row (Decision 3, "No resume").

### Claim mechanism

`JobRunRepository.claimNextPending(executorIdentity, leaseDuration)` runs, in one transaction:
```sql
SELECT id FROM job_run WHERE status = 'PENDING' ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1;
-- then, same transaction:
UPDATE job_run SET status='RUNNING', executor_identity=?, lease_until=?, started_at=now(), heartbeat_at=now() WHERE id=?;
```
This is real PostgreSQL row locking, not an application-level compare-and-swap, and is safe if multiple threads/instances race to claim.

### Execution service flow

`JobExecutionService.executeNextPending(executorIdentity)`:
1. Claim a `PENDING` run (row lock).
2. Load its `JobDefinition`.
3. Resolve `${env:VARIABLE}` references in connect/user/password fields (`JobDefinitionEnvResolver`); reject `${secret:...}` explicitly.
4. Look up the last committed watermark for this definition (last `SUCCEEDED` run's `committedWatermark`, else the definition's `initialWatermarkValue`).
5. Build a `String[]` CLI-style args array (`ToolOptionsArgsBuilder`) — `ToolOptions`'s only public constructor is `ToolOptions(String[] args)`, so this is the actual mechanism behind "converts a job definition into `ToolOptions`".
6. `new ToolOptions(args)`, then `ReplicaDB.processReplica(options)`.
7. Map exit code → terminal state: `0 → SUCCEEDED` (persist `committedWatermark` from `getExecutionContext().getWatermarkCandidate()`), `1 → FAILED`, `2 → CANCELLED`. Persist `rowsProcessed`/`durationMillis` from the widened `ReplicationExecutionContext` in every case.

No REST endpoint or scheduler calls this service in this phase — it is unit/integration-tested directly, ready for Phase 1c to wire behind Quartz and a controller.

### Testing strategy

- Pure logic (state machine, env resolver, args builder, validation) → plain JUnit 5, no containers.
- Persistence and full Spring context → Testcontainers PostgreSQL via `@ServiceConnection`, replacing the two context-loading tests that currently boot with no database.
- The execution-service end-to-end test needs a *replication* source/sink too; it uses SQLite file databases (already used elsewhere in the core's test suite) for source/sink so the test only needs one container (Postgres, for state), not three.
- CI's `server` job in `CT_Push.yml` currently has no Docker/Testcontainers wiring (unlike the `non_integration`/`integration` jobs); it needs the same `TESTCONTAINERS_CONFIG_FILE`/`DOCKER_HOST` env and a `docker info` sanity step.

---

## Implementation Tasks

### 1. Core: expose row counters and timings unconditionally

- [x] **1.1 Widen `ReplicationExecutionContext` with rows/duration accessors**
  Files: [src/main/java/org/replicadb/execution/ReplicationExecutionContext.java](src/main/java/org/replicadb/execution/ReplicationExecutionContext.java)
  Changes: Add `private volatile long rowsProcessed;` and `private volatile long durationMillis;` with `getRowsProcessed()`/`setRowsProcessed(long)` and `getDurationMillis()`/`setDurationMillis(long)`, following the existing `watermarkCandidate` field style (plain getter/setter, no synchronization needed beyond `volatile` since it is written once per run before being read).
  Tests: Extend [src/test/java/org/replicadb/execution/ReplicationExecutionContextTest.java](src/test/java/org/replicadb/execution/ReplicationExecutionContextTest.java) with default-zero-value and set/get round-trip assertions for both fields.
  Dependencies: None

- [x] **1.2 Populate counters unconditionally in `ReplicaDB.executeSingleReplication`**
  Files: [src/main/java/org/replicadb/ReplicaDB.java](src/main/java/org/replicadb/ReplicaDB.java)
  Changes: In `executeSingleReplication`, the existing line `final ReplicationTasksResult replicationTasksResult = executeReplicationTasks(options, managerFactory); replicaTasksService = replicationTasksResult.executor();` already runs inside the `try` block before `waitForTaskCompletion(preSinkTasksFuture); executePostTasks(sourceDs, sinkDs);`. Immediately after that assignment, add `options.getExecutionContext().setRowsProcessed(replicationTasksResult.summary().totalRowsProcessed());` and `options.getExecutionContext().setDurationMillis(replicationTasksResult.summary().maxDurationMillis());` (using the confirmed `ReplicaTaskResultsSummary`/`ReplicationTasksResult` record accessors described in the Architecture section above), so the values are recorded before any later exception (merge failure, cancellation) can be thrown — unlike the watermark candidate, which stays conditional on `executePostTasks()` succeeding.
  Tests: New [src/test/java/org/replicadb/ReplicaDBRunCountersTest.java](src/test/java/org/replicadb/ReplicaDBRunCountersTest.java), mirroring `ReplicaDBWatermarkCommitTest`'s `RecordingManager`/`StubManagerFactory` stub style: assert counters are set (non-default) on a successful run, on a merge failure, and on both cancellation paths (explicit `ReplicationCancelledException` and flag-based); assert counters stay at `0` when `executeReplicationTasks` never produces a summary (e.g., a pre-flight Azure-auth validation failure).
  Dependencies: Task 1.1

### 2. Domain models

- [x] **2.1 `JobRunStatus` enum**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStatus.java`
  Changes: Enum with `PENDING, RUNNING, SUCCEEDED, FAILED, CANCEL_REQUESTED, CANCELLED, RETRY_SCHEDULED`; add `boolean isTerminal()` returning `true` for `SUCCEEDED`, `CANCELLED`, `RETRY_SCHEDULED`. Also add `static JobRunStatus fromReplicaExitCode(int exitCode)` mapping `0 → SUCCEEDED`, `1 → FAILED`, `2 → CANCELLED`, throwing `IllegalArgumentException("Unknown ReplicaDB exit code: " + exitCode)` for any other value — this is the single place `JobExecutionService` (Task 5.3) delegates its exit-code mapping to, so the mapping itself is unit-tested here instead of requiring a live cancelled replication run in an integration test.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobRunStatusTest.java` — assert `isTerminal()` for every value, table-driven; assert `fromReplicaExitCode(0)==SUCCEEDED`, `fromReplicaExitCode(1)==FAILED`, `fromReplicaExitCode(2)==CANCELLED`, and `fromReplicaExitCode(99)` throws `IllegalArgumentException`.
  Dependencies: None

- [x] **2.2 `JobRunStateMachine` with legal transitions**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java`
  Changes: `static void assertLegalTransition(JobRunStatus from, JobRunStatus to)` throwing `IllegalStateException` for any pair not in the map: `PENDING→{RUNNING, CANCELLED}`, `RUNNING→{SUCCEEDED, FAILED, CANCEL_REQUESTED, CANCELLED}`, `CANCEL_REQUESTED→{CANCELLED}`, `FAILED→{RETRY_SCHEDULED}`.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobRunStateMachineTest.java` — parameterized test asserting every legal pair passes and every other combination (including terminal→anything) throws.
  Dependencies: Task 2.1

- [x] **2.3 `JobDefinition` record and validation**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobDefinition.java`
  Changes: Java record with fields `id (UUID)`, `name`, `sourceConnect`, `sourceUser`, `sourcePassword`, `sourceTable`, `sourceWhere`, `sinkConnect`, `sinkUser`, `sinkPassword`, `sinkTable`, `mode (org.replicadb.cli.ReplicationMode)`, `jobs (int)`, `incrementalWatermarkColumn`, `initialWatermarkValue`, `createdAt`, `updatedAt` (all `Instant`/`String`/nullable as appropriate). Compact constructor validates: `name`/`sourceConnect`/`sourceTable`/`sinkConnect`/`sinkTable` non-blank, `jobs >= 1`, `incrementalWatermarkColumn` only set when `mode == ReplicationMode.INCREMENTAL` (throws `IllegalArgumentException` otherwise, matching Decision 1's one-table-pair intent), and — implementing Decision 4's "never enters the state store" rule — `sourcePassword`/`sinkPassword` must each be either `null` or match `^\$\{env:[A-Za-z_][A-Za-z0-9_]*\}$` (throws `IllegalArgumentException("sourcePassword/sinkPassword must be an ${env:VARIABLE} reference")` for a literal value), so a `JobDefinition` can never be constructed with a plaintext credential.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobDefinitionTest.java` — valid construction, and one test per invariant violation (blank name, blank tables, `jobs <= 0`, watermark column set with `COMPLETE`/`COMPLETE_ATOMIC` mode, a literal (non-`${env:...}`) `sourcePassword`, a literal `sinkPassword`); assert a `null` password and a well-formed `${env:DB_PASSWORD}` password both construct successfully.
  Dependencies: None

- [x] **2.4 `JobRun` record**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRun.java`
  Changes: Java record with fields `id (UUID)`, `jobDefinitionId (UUID)`, `previousRunId (UUID, nullable)`, `status (JobRunStatus)`, `attempt (int)`, `executorIdentity (String, nullable)`, `leaseUntil (Instant, nullable)`, `heartbeatAt (Instant, nullable)`, `createdAt`, `startedAt (nullable)`, `finishedAt (nullable)`, `rowsProcessed (long, nullable/Long)`, `durationMillis (Long, nullable)`, `committedWatermark (String, nullable)`, `errorMessage (String, nullable)`. Compact constructor validates `attempt >= 1`.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobRunTest.java` — valid construction and `attempt <= 0` rejection.
  Dependencies: Task 2.1

### 3. Persistence foundation

- [x] **3.1 Add PostgreSQL/Flyway/Testcontainers dependencies**
  Files: `replicadb-server/pom.xml`
  Changes: Add `org.springframework.boot:spring-boot-starter-jdbc`, `org.flywaydb:flyway-core`, `org.flywaydb:flyway-database-postgresql`, `org.postgresql:postgresql` (runtime), and test-scope `org.springframework.boot:spring-boot-testcontainers`, `org.testcontainers:postgresql`, `org.testcontainers:junit-jupiter`. All versions are managed by the existing `spring-boot-starter-parent:3.3.5` BOM plus the Testcontainers BOM already used by the root `pom.xml` (import the same Testcontainers BOM version for consistency).
  Tests: New `replicadb-server/src/test/java/org/replicadb/server/job/persistence/PersistenceDependencyResolutionTest.java`, mirroring the existing `CoreDependencyResolutionTest` pattern — asserts `Class.forName("org.flywaydb.core.Flyway")`, `Class.forName("org.postgresql.Driver")`, and `Class.forName("org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate")` all resolve without throwing.
  Dependencies: None

- [x] **3.2 Flyway migrations for `job_definition` and `job_run`**
  Files: `replicadb-server/src/main/resources/db/migration/V1__create_job_definition.sql`, `replicadb-server/src/main/resources/db/migration/V2__create_job_run.sql`
  Changes: `V1` creates `job_definition` (`id UUID PRIMARY KEY`, `name VARCHAR(255) NOT NULL UNIQUE`, source/sink connect/user/password/table/where columns, `mode VARCHAR(32) NOT NULL CHECK (mode IN ('complete','incremental','complete-atomic'))`, `jobs INTEGER NOT NULL DEFAULT 4 CHECK (jobs > 0)`, `incremental_watermark_column VARCHAR(255)`, `initial_watermark_value VARCHAR(255)`, `created_at/updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`). `V2` creates `job_run` (`id UUID PRIMARY KEY`, `job_definition_id UUID NOT NULL REFERENCES job_definition(id)`, `previous_run_id UUID REFERENCES job_run(id)`, `status VARCHAR(32) NOT NULL CHECK (status IN (...the 7 values...))`, `attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt > 0)`, `executor_identity VARCHAR(255)`, `lease_until/heartbeat_at TIMESTAMPTZ`, `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`, `started_at/finished_at TIMESTAMPTZ`, `rows_processed BIGINT`, `duration_millis BIGINT`, `committed_watermark VARCHAR(255)`, `error_message TEXT`) plus a partial index `CREATE INDEX idx_job_run_pending ON job_run (created_at) WHERE status = 'PENDING'` to keep the claim query's `ORDER BY created_at` scan cheap.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java` — plain JUnit 5 + `@Testcontainers`/`@Container` `PostgreSQLContainer` (no Spring context), runs `Flyway.configure().dataSource(container.getJdbcUrl(), ...).load().migrate()` and asserts 2 migrations applied and `flyway.validate()` passes. This test deliberately uses a raw (non-Spring) container rather than the `@ServiceConnection` config from Task 3.3: it exists specifically to validate the migration scripts in isolation, before Task 3.3 wires a Spring `DataSource` at all, and stays as a fast, dependency-light regression check afterward — it does not replace the Spring-context coverage added by later repository/service tests.
  Dependencies: Task 3.1

- [x] **3.3 Spring datasource/Flyway wiring and Testcontainers `@ServiceConnection` for existing tests**
  Files: `replicadb-server/src/main/resources/application-api.yml`, `replicadb-server/src/test/java/org/replicadb/server/config/PostgresTestcontainersConfig.java` (new `@TestConfiguration`), [replicadb-server/src/test/java/org/replicadb/server/HealthEndpointTest.java](replicadb-server/src/test/java/org/replicadb/server/HealthEndpointTest.java), [replicadb-server/src/test/java/org/replicadb/server/ReplicaDbServerApplicationTest.java](replicadb-server/src/test/java/org/replicadb/server/ReplicaDbServerApplicationTest.java)
  Changes: `application-api.yml` gets `spring.datasource.url/username/password` (via `${DB_URL}`/`${DB_USERNAME}`/`${DB_PASSWORD}` placeholders — never literal credentials) and `spring.flyway.enabled: true`. `PostgresTestcontainersConfig` declares a `@Bean @ServiceConnection PostgreSQLContainer<?>` shared by both tests via `@Import`. Both existing tests get `@Import(PostgresTestcontainersConfig.class)` so the full context (now including the mandatory `DataSource`) loads successfully.
  Tests: The two updated test files themselves are the test — they must pass with a real Testcontainers-backed Postgres and the Flyway migrations applied at context startup.
  Dependencies: Task 3.2

### 4. Repositories

- [x] **4.0 (context) Exact `ToolOptions` CLI flags reused by Task 5.2**
  Confirmed from `ToolOptions`'s Commons CLI option definitions (`.longOpt(...)`): `--source-connect`, `--source-user`, `--source-password`, `--source-table`, `--source-where`, `--sink-connect`, `--sink-user`, `--sink-password`, `--sink-table`, `--mode`, `--jobs` (short `-j`), `--incremental-watermark-column`, `--incremental-watermark-value`. This list is authoritative for Task 5.2; no other flags are needed for a single source/sink table pair.

- [x] **4.1 `JobDefinitionRepository`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java`
  Changes: `@Repository` using `NamedParameterJdbcTemplate`; methods `JobDefinition insert(JobDefinition)` (generates `id`/`createdAt`/`updatedAt` if absent), `Optional<JobDefinition> findById(UUID)`, `Optional<JobDefinition> findByName(String)`, `List<JobDefinition> findAll()`. Row mapping via a `RowMapper<JobDefinition>` that maps `mode` through `ReplicationMode.valueOf(...)`/`getModeText()`.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java` (`@Import(PostgresTestcontainersConfig.class)` + `@JdbcTest` or a minimal `@SpringBootTest`) — insert-then-`findById` round-trip, `findByName` miss returns empty, duplicate `name` insert surfaces a `DataIntegrityViolationException`.
  Dependencies: Tasks 2.3, 3.3

- [x] **4.2 `JobRunRepository` with row-locking claim and state transitions**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`
  Changes: `@Repository` using `NamedParameterJdbcTemplate` and `@Transactional` where multi-statement atomicity matters. Methods: `JobRun insertPending(UUID jobDefinitionId, UUID previousRunId, int attempt)`; `Optional<JobRun> claimNextPending(String executorIdentity, Duration leaseDuration)` (the `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE` pair from the Architecture section, calling `JobRunStateMachine.assertLegalTransition(PENDING, RUNNING)` before the update); `void markSucceeded(UUID runId, long rowsProcessed, long durationMillis, String committedWatermark)`; `void markFailed(UUID runId, long rowsProcessed, long durationMillis, String errorMessage)`; `void markCancelled(UUID runId, long rowsProcessed, long durationMillis)` — each `mark*` method asserts the legal transition and uses a conditional `UPDATE ... WHERE id=? AND status='RUNNING'`, throwing `IllegalStateException` if 0 rows were affected; `Optional<String> findLastCommittedWatermark(UUID jobDefinitionId)` (latest `SUCCEEDED` run's `committed_watermark`); `Optional<JobRun> findById(UUID)`.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java` — `insertPending` then `claimNextPending` transitions to `RUNNING`; a second concurrent `claimNextPending` call (two threads, two `DataSource` connections) against two different `PENDING` rows each claim a distinct row and neither blocks on the other (verifies `SKIP LOCKED` behavior); `claimNextPending` on an empty table returns `Optional.empty()`; `markSucceeded` after `markCancelled` throws `IllegalStateException` (illegal transition guard); `findLastCommittedWatermark` returns empty when no `SUCCEEDED` run exists, and the correct value after one does.
  Dependencies: Tasks 2.2, 2.4, 4.1

- [x] **4.3 `JobRunRepository.scheduleRetry(...)` — implements "a retry can identify the previous run and never resumes"**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java` (same file as Task 4.2, additive method)
  Changes: Add `JobRun scheduleRetry(UUID failedRunId)`: loads the run, asserts its `status == FAILED` (else `IllegalStateException`), transitions it to `RETRY_SCHEDULED` via `JobRunStateMachine.assertLegalTransition(FAILED, RETRY_SCHEDULED)` and the conditional `UPDATE ... WHERE id=? AND status='FAILED'`, then calls the existing `insertPending(jobDefinitionId, previousRunId=failedRunId, attempt=oldRun.attempt()+1)` to create the brand-new `PENDING` `JobRun` row — never mutates the failed row back to `RUNNING`, satisfying Decision 3's "no resume" rule. The new row is claimable by `claimNextPending` like any other `PENDING` run.
  Tests: Extend `JobRunRepositoryIT.java` — `scheduleRetry` on a `FAILED` run creates a new `PENDING` row with `previousRunId` equal to the original run's id and `attempt` incremented by 1, and leaves the original row in `RETRY_SCHEDULED`; `scheduleRetry` on a `SUCCEEDED`/`PENDING`/`RUNNING` run throws `IllegalStateException`.
  Dependencies: Task 4.2

### 5. Execution service

- [x] **5.1 `JobDefinitionEnvResolver`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobDefinitionEnvResolver.java`
  Changes: `String resolve(String template)` replacing every `${env:VARIABLE}` token with `System.getenv("VARIABLE")`, throwing `IllegalArgumentException("Missing environment variable: VARIABLE")` (naming only the variable, never any other part of the template) if unset; throws `UnsupportedOperationException("Secret references are not yet supported")` if the template contains `${secret:`; returns the input unchanged if it contains no `${...}` token.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobDefinitionEnvResolverTest.java` — plain string unchanged, single/multiple `${env:...}` resolved from a variable set via `EnvironmentVariables` test support or a small resolver seam allowing an injectable lookup function, missing variable throws with the variable name only, `${secret:...}` throws `UnsupportedOperationException`.
  Dependencies: None

- [x] **5.2 `ToolOptionsArgsBuilder`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/ToolOptionsArgsBuilder.java`
  Changes: `String[] build(JobDefinition definition, String previousWatermarkValue)` producing `--source-connect`, `--source-user`/`--source-password` (only if non-null), `--source-table`, `--source-where` (if non-null), `--sink-connect`, `--sink-user`/`--sink-password` (only if non-null), `--sink-table`, `--mode <definition.mode().getModeText()>`, `--jobs <definition.jobs()>`, and — only when `incrementalWatermarkColumn` is set — `--incremental-watermark-column` plus `--incremental-watermark-value <previousWatermarkValue or initialWatermarkValue>` if either is non-null.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/execution/ToolOptionsArgsBuilderTest.java` — for each of `complete`/`incremental`/`complete-atomic` modes, assert the built args successfully construct a `new ToolOptions(args)` without throwing; assert watermark args are present only for `incremental` with a declared column and absent otherwise; assert optional user/password/where args are omitted when null.
  Dependencies: Task 2.3

- [x] **5.3 `JobExecutionService`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobRunOutcome.java` (result record: `runId`, `status`, `rowsProcessed`, `durationMillis`)
  Changes: `@Service` orchestrating the flow described in the Architecture section: claim → resolve env refs → look up prior watermark → build args → `new ToolOptions(args)` → `ReplicaDB.processReplica(options)` → `JobRunStatus.fromReplicaExitCode(exitCode)` (Task 2.1, so 0/1/2 mapping is not re-implemented or re-tested here) → `markSucceeded`/`markFailed`/`markCancelled` accordingly, using `org.replicadb.config.CredentialRedactor.redactMessage(...)` on any exception message from `ToolOptions` construction itself (e.g. malformed args) before persisting it as `errorMessage`, consistent with the accepted limitation documented above for a plain `processReplica` failure. Per the Credential handling note above: never log the resolved connect strings, passwords, or the built `String[]` args array — log only `runId`/`jobDefinitionId`.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java` (`@Import(PostgresTestcontainersConfig.class)`) — end-to-end with a real `JobDefinition` pointing source/sink at two SQLite fixture files: (1) a successful `complete` run reaches `SUCCEEDED` with `rowsProcessed > 0`, a populated `finished_at`, and asserts the persisted `committedWatermark` matches the reduced value ReplicaDB reported; (2) an `incremental` run with a bad/unreachable sink table reaches `FAILED` with `errorMessage` set and the job's prior `committedWatermark` unchanged; (3) a `JobDefinition` whose `sourceConnect` resolves to a malformed value causing `new ToolOptions(args)` to throw reaches `FAILED` with a `CredentialRedactor`-redacted `errorMessage` that contains no connection string; (4) `executeNextPending` on an empty queue returns `Optional.empty()` without touching the repository's `mark*` methods. The `CANCELLED` exit-code branch is covered by `JobRunStatusTest` (Task 2.1) rather than duplicated here, since reliably triggering a live mid-flight cancellation in an integration test is flaky.
  Dependencies: Tasks 1.2, 2.1, 4.1, 4.2, 4.3, 5.1, 5.2

### 6. CI

- [x] **6.1 Align the `server` CI job's Testcontainers/Docker wiring**
  Files: `.github/workflows/CT_Push.yml`
  Changes: Add the same `env` block used by the `non_integration`/`integration` jobs (`TESTCONTAINERS_REUSE_ENABLE: 'false'`, `TESTCONTAINERS_CONFIG_FILE: src/test/resources/testcontainers-ci.properties`, `DOCKER_HOST: unix:///var/run/docker.sock`, `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE: /var/run/docker.sock`) and a `Check Docker info` (`docker info`) step to the `server` job, ahead of its `mvn -B test --file replicadb-server/pom.xml` step.
  Tests: No new test code — this task's own verification is that `mvn -B test --file replicadb-server/pom.xml` passes in CI once run with these env vars, exercising every Testcontainers-backed test added in Tasks 3–5 in the actual CI environment (not just locally).
  Dependencies: Tasks 3.3, 4.1, 4.2, 4.3, 5.3

---

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

```java
// org.replicadb.server.job.domain
public enum JobRunStatus {
    PENDING, RUNNING, SUCCEEDED, FAILED, CANCEL_REQUESTED, CANCELLED, RETRY_SCHEDULED;
    public boolean isTerminal() { ... } // SUCCEEDED, CANCELLED, RETRY_SCHEDULED
}

public record JobDefinition(
        UUID id, String name,
        String sourceConnect, String sourceUser, String sourcePassword, String sourceTable, String sourceWhere,
        String sinkConnect, String sinkUser, String sinkPassword, String sinkTable,
        org.replicadb.cli.ReplicationMode mode, int jobs,
        String incrementalWatermarkColumn, String initialWatermarkValue,
        Instant createdAt, Instant updatedAt) { /* compact-constructor validation */ }

public record JobRun(
        UUID id, UUID jobDefinitionId, UUID previousRunId,
        JobRunStatus status, int attempt,
        String executorIdentity, Instant leaseUntil, Instant heartbeatAt,
        Instant createdAt, Instant startedAt, Instant finishedAt,
        Long rowsProcessed, Long durationMillis,
        String committedWatermark, String errorMessage) { /* compact-constructor validation */ }
```

`job_definition` / `job_run` PostgreSQL tables as described in Task 3.2.

</details>

<details>
<summary>Dependencies</summary>

- `org.springframework.boot:spring-boot-starter-jdbc` (compile, `replicadb-server`)
- `org.flywaydb:flyway-core`, `org.flywaydb:flyway-database-postgresql` (compile, `replicadb-server`)
- `org.postgresql:postgresql` (runtime, `replicadb-server`)
- `org.springframework.boot:spring-boot-testcontainers`, `org.testcontainers:postgresql`, `org.testcontainers:junit-jupiter` (test, `replicadb-server`)
- No new dependency on the root `pom.xml` / CLI artifact.

</details>

<details>
<summary>Testing Strategy</summary>

| Layer | Tooling | Example |
| --- | --- | --- |
| Pure domain/logic | JUnit 5, no containers | `JobDefinitionTest`, `JobRunStateMachineTest`, `JobDefinitionEnvResolverTest`, `ToolOptionsArgsBuilderTest` |
| Migrations | JUnit 5 + raw Testcontainers `PostgreSQLContainer`, no Spring context | `FlywayMigrationTest` |
| Repositories / full context | Spring Boot Test + Testcontainers `@ServiceConnection` | `JobDefinitionRepositoryIT`, `JobRunRepositoryIT`, updated `HealthEndpointTest`/`ReplicaDbServerApplicationTest` |
| Execution service | Testcontainers Postgres (state) + SQLite fixture files (source/sink, no extra container) | `JobExecutionServiceIT` |
| Core widening | Existing `RecordingManager`/`StubManagerFactory` stub style | `ReplicaDBRunCountersTest` |

</details>

---

## Known Gaps and Deferred Decisions (carried forward, not silently dropped)

- `JobRun.errorMessage` on `FAILED` is generic in this phase; the core does not expose the underlying exception except when `ToolOptions` construction itself fails. A future phase must widen `processReplica`'s result if per-run root-cause detail from inside a replication run is required.
- No REST endpoint, Quartz scheduler, or security triggers `JobExecutionService`/`scheduleRetry` yet; both are called directly from tests only. Phase 1c wires them behind `/api/v1` and a scheduler.
- `executor_identity`/`lease_until`/`heartbeat_at` are populated with simple single-instance values (`"local"`-style identity, a fixed lease duration) in this phase; the full lease/heartbeat *rules* (Decision 6) are Phase 2 work once distributed workers exist.
- Decision 2's requirement that the API "surface and persist" a warning for `complete`-mode job definitions has no home in this phase (no API exists yet); no `mode_warning` column was added to avoid dead schema — add it alongside the Phase 1c API work instead.
- `${secret:<provider>/<path>#<key>}` references remain rejected (Decision 4); only `${env:VARIABLE}` is supported, and `sourcePassword`/`sinkPassword` are validated to be exactly that pattern or `null` — never a literal secret — closing the credential-at-rest risk without requiring column-level encryption in this phase.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 16/16 implementation tasks (100%).
- Tasks that required plan adjustment: 3/16 (18.75%).
- Test loop iterations: 6 retry loops; the focused checks otherwise passed first time.

### Gaps Encountered

#### Gap 1: Container readiness needed an explicit port wait (Plan-to-Implementation)

- **Task**: 3.2 — Flyway migrations.
- **Plan assumed**: Testcontainers' PostgreSQL startup check would be sufficient before the raw Flyway connection.
- **Reality**: The local Docker runtime reported the container as started while the mapped host port still returned `Connection refused`.
- **Resolution**: Added `Wait.forListeningPort()` to the migration test container.
- **Learning**: Database integration tests should assert the externally reachable port when they connect through Testcontainers' mapped JDBC URL.

#### Gap 2: JDBC timestamp binding required explicit PostgreSQL types (Plan-to-Implementation)

- **Task**: 4.1 — `JobDefinitionRepository`.
- **Plan assumed**: Spring JDBC could infer PostgreSQL `TIMESTAMPTZ` from `java.time.Instant` in named parameters.
- **Reality**: The PostgreSQL driver rejected the untyped `Instant` parameter.
- **Resolution**: Bound timestamps as `java.sql.Timestamp` and converted them back to `Instant` in the row mapper.
- **Learning**: PostgreSQL temporal parameters in Spring JDBC need an explicit JDBC representation at repository boundaries.

#### Gap 3: Managed core execution required Log4j2 dependency alignment (Intent-to-Plan)

- **Task**: 5.3 — `JobExecutionService`.
- **Plan assumed**: The existing server Spring Boot dependencies could invoke the core directly.
- **Reality**: ReplicaDB's Sentry initialization expected a Log4j2 `LoggerContext`, while Spring Boot's default logging bridge supplied `SLF4JLoggerContext`.
- **Resolution**: Excluded `spring-boot-starter-logging` from server starters and added `spring-boot-starter-log4j2`.
- **Learning**: A managed runtime embedding the CLI core must validate logging implementation compatibility before exercising the execution path.

#### Gap 4: Connection strings also needed credential-reference validation (Intent-to-Plan)

- **Task**: 2.3 — `JobDefinition` validation.
- **Plan assumed**: Restricting password fields to `${env:VARIABLE}` references was sufficient to keep secrets out of the state store.
- **Reality**: A literal password could still be embedded in a JDBC URL's user-info or query parameters.
- **Resolution**: Reject credential-bearing connection-string patterns and cover URI user-info and `password=` regressions.
- **Learning**: Secret-reference policies must validate every credential-bearing field, including composite connection strings.

### Patterns Discovered

- Spring server tests can share a `@ServiceConnection` PostgreSQL configuration while raw Flyway tests remain isolated with a direct `PostgreSQLContainer`.
- `JobRunRepository.claimNextPending` must be tested with a deliberately held row; a fast two-thread race does not prove `SKIP LOCKED` behavior.
- The managed server must use Log4j2 when it invokes ReplicaDB core code that configures Sentry through Log4j2.
