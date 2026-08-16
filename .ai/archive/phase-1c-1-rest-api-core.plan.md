# Implementation Plan: Phase 1c-1 — REST API Core (Job Definitions & Runs, no auth)

## Task Source

No JIRA ticket. Source is [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md), which scopes Phase 1c as "REST API, scheduler, security, frontend" and explicitly defers all of it after Phase 1a/1b. This plan covers the **REST API core slice only** (agreed with the user): job-definition and job-run HTTP resources, asynchronous execution, and cancellation — explicitly **without** Quartz scheduling, Spring Security/users/ACLs, or the frontend, which remain separate future slices (1c-2, 1c-3, 1c-4).

Acceptance criteria (derived from Decision 2 "Phase 1 scope", Decision 4 "API conventions" and "API surface", and Decision 5):

- REST API for job definitions and run management, matching exactly the endpoint table in Decision 4:
  `POST /api/v1/jobs`, `GET /api/v1/jobs`, `GET /api/v1/jobs/{id}`, `PUT /api/v1/jobs/{id}`,
  `POST /api/v1/jobs/{id}/runs`, `GET /api/v1/jobs/{id}/runs`, `GET /api/v1/runs`, `GET /api/v1/runs/{id}`,
  `GET /api/v1/runs/{id}/log`, `POST /api/v1/runs/{id}/cancel`, `POST /api/v1/runs/{id}/retry`.
  No DELETE/disable endpoint exists in that table, so none is added here.
- Asynchronous execution: API requests must not block on replication (Decision 2).
- `POST /api/v1/runs/{id}/cancel` stops the replication immediately and unconditionally (Decision 5), and the response must explicitly warn that the sink may be left in an indeterminate state.
- `POST /api/v1/jobs/{id}/runs` requires an `Idempotency-Key` header; a replay of the same key within 24 hours returns the originally created run instead of starting a second one (Decision 4).
- Base path `/api/v1`; errors use RFC 7807 `application/problem+json` and never echo connection strings or credentials (Decision 4).
- Collection endpoints are paginated with `page`/`size`; default `size` is 50, maximum is 200 (Decision 4).
- A job should not overlap with another run of itself by default (Decision 2, "API and scheduler execution" step 4 note).
- `POST /api/v1/runs/{id}/retry` re-executes the job definition from the beginning via the existing `JobRunRepository.scheduleRetry(...)`, never resuming the failed run (Decision 3, "No resume").
- `complete` mode must surface an explicit warning that a retried/interrupted run leaves the sink truncated or partially loaded (Decision 2).
- Explicitly **out of scope**: Quartz scheduler, authentication/authorization/sessions/ACLs, frontend, full 256 KB run-log capture (stubbed — see Architecture & Design), DELETE/disable of a job definition (not in the Decision 4 endpoint table).

## Overview

`replicadb-server` currently has a fully tested domain/persistence/execution layer (`JobDefinition`, `JobRun`, `JobExecutionService`) with nothing exposed over HTTP — `JobExecutionService.executeNextPending(...)` is only called from tests. This plan wires that layer behind a REST API: controllers for job definitions and runs, a new `RunExecutionCoordinator` that executes runs asynchronously on a bounded pool while keeping a live, cancellable reference to each in-flight `ReplicationExecutionContext`, RFC 7807 error handling, pagination, and an idempotency store for manual triggers. This is the last piece needed before Phase 1c-2 (Quartz) can submit runs through the same coordinator instead of duplicating execution logic.

## Architecture & Design

**Approach**: Dedicated `RunExecutionCoordinator` (Approach A, chosen over folding execution-tracking into `JobExecutionService`) — a bounded `ExecutorService` plus an in-memory `ConcurrentHashMap<UUID, ToolOptions>` of in-flight runs. This matches the "Bounded execution executor" component in Decision 2's architecture diagram and is a single-JVM design, consistent with Decision 2's "Monolithic Control Plane First" — a second control-plane instance is explicitly Phase 2 territory, so an in-memory-only cancellation registry is acceptable here.

### Why a specific-run claim is needed (not just `claimNextPending`)

`JobRunRepository.claimNextPending` claims whatever `PENDING` row is oldest, which is fine for a future poller/worker loop but wrong for "trigger *this* run I just created" when other jobs may have older pending rows. A new `JobRunRepository.claimById(runId, executorIdentity, leaseDuration)` claims one specific row with the same `FOR UPDATE SKIP LOCKED` + conditional `UPDATE` pattern as `claimNextPending`. The coordinator uses this so the run it submits is the run it registers for cancellation.

### Reusing `JobExecutionService` without breaking existing tests

`JobExecutionService.executeClaimedRun(JobRun)` is currently `private` and always blocks until `ReplicaDB.processReplica` returns, so there is no seam to register the live `ToolOptions`/`ReplicationExecutionContext` before the blocking call. It becomes `public JobRunOutcome executeClaimedRun(JobRun run, Consumer<ToolOptions> onStarted)`, invoking `onStarted.accept(options)` immediately after `new ToolOptions(arguments)` and before `ReplicaDB.processReplica(options)`. `executeNextPending(executorIdentity)` keeps its existing signature and behavior, now delegating to the new overload with a no-op callback (`options -> {}`) — `JobExecutionServiceIT` and the `emptyQueueDoesNotMarkAnyRun` unit test keep passing unmodified.

### Cancellation flow

1. `POST /api/v1/runs/{id}/cancel`: read the run.
   - If `PENDING`: transition directly to `CANCELLED` in one `UPDATE ... WHERE status='PENDING'` (nothing is executing yet, no coordinator involvement) — legal per `JobRunStateMachine` (`PENDING → CANCELLED`).
   - If `RUNNING`: call `RunExecutionCoordinator.requestCancellation(runId)` **first** (a plain in-memory map lookup + method call, no I/O), then, only if it returned `true` (a live context was found and signaled), persist `markCancelRequested` (new `JobRunRepository.markCancelRequested`, legal `RUNNING → CANCEL_REQUESTED`). This ordering matters: the in-memory signal is what actually stops the running SQL (Decision 5's "immediate" requirement), and delivering it before the DB write means a crash between the two operations still leaves the replication cancelled — only the DB's observability of that fact would be lost, not the cancellation itself. If `requestCancellation` returns `false` (the run finished or was never registered — e.g. the async task completed between the read and this call), re-read the run's current status and respond based on what it actually is (already terminal → 409; still `RUNNING` in the DB but not in the coordinator's registry is a benign race the coordinator's own completion callback will resolve within its next persist step).
   - Any other status: HTTP 409 (already terminal).
2. `JobRunRepository.markCancelled(...)`'s `WHERE` clause is broadened from `status = 'RUNNING'` to `status IN ('RUNNING', 'CANCEL_REQUESTED')`, since the coordinator's async completion callback now runs after the row may already be `CANCEL_REQUESTED`.
3. The cancel response body is `{"runId": "<uuid>", "status": "CANCEL_REQUESTED"|"CANCELLED", "warning": "<text>"}`. `warning` always contains Decision 5's warning text, chosen from the per-mode table (`incremental`/`complete-atomic`/`complete`) using only `JobDefinition.mode()` — the API cannot observe whether cancellation landed before or during a merge, so it returns the worst-case warning for that mode, consistent with "the endpoint's contract is obedience, not consistency."
4. `POST /api/v1/jobs/{id}/runs` and `POST /api/v1/runs/{id}/retry` both return `202 Accepted` with a `Location: /api/v1/runs/{runId}` header and a body containing the full `JobRunResponse` for the newly created run (status `PENDING`), not an empty body — consistent with returning the resource a client would otherwise have to immediately re-fetch.

> ⚠️ Critic note: a JVM crash after `requestCancellation` succeeds but before the coordinator's own completion callback persists a terminal state (or, more generally, any crash while a run is `RUNNING`) leaves that `job_run` row stuck in a non-terminal status forever, since there is no lease-expiry reconciliation in this phase. This is a pre-existing property of the single-instance design, not something introduced by cancellation, and matches Decision 6's placement of lease/heartbeat recovery in Phase 2. Not fixed here; flagging for visibility.

### `mode_warning` — deliberately NOT a persisted column in this slice

Decision 2 says the `complete`-mode warning "must be persisted on the job definition." Implementing that literally means adding a field to the `JobDefinition` record, which is a **positional** record constructor already used across ~10 existing call sites (`JobDefinitionRepository`'s insert/row-mapper/update, `JobExecutionServiceIT` (x3), `ToolOptionsArgsBuilderTest`, `JobDefinitionEnvResolverTest`, `JobDefinitionRepositoryIT`, `JobDefinitionTest`). Adding a field would force touching all of them for a value that is 100% derivable from the already-persisted `mode` column.

> ⚠️ Design decision (not a critic-flagged issue, called out here for visibility): this plan computes the warning **dynamically** in `JobDefinitionResponse`/`JobDefinitionMapper` from `mode()` instead of storing a redundant string. No information is lost — the warning is deterministic from the stored mode — but this is a conscious deviation from the literal "persisted on the job definition" wording. If a future slice needs the warning text itself to be independently editable or audited, add the column then.

### Idempotency store — accepted race window, with scheduled cleanup

`run_trigger_idempotency(idempotency_key PK, job_definition_id, run_id, created_at)`. Lookup: `SELECT run_id FROM run_trigger_idempotency WHERE idempotency_key = :key AND created_at > now() - interval '24 hours'`. If present, return that run without creating a new one. Otherwise create the run, then `INSERT ... ON CONFLICT (idempotency_key) DO UPDATE SET run_id = EXCLUDED.run_id, job_definition_id = EXCLUDED.job_definition_id, created_at = EXCLUDED.created_at` — this single Postgres statement is atomic regardless of the surrounding transaction's isolation level, so no explicit `@Transactional`/isolation tuning is required beyond Spring JDBC's default auto-commit-per-statement behavior. Two genuinely concurrent requests with the same brand-new key could each create a run before either finishes its upsert (last write wins in the idempotency table, leaving one run "orphaned" from the key's perspective). This is an accepted limitation for a monolithic single-instance phase — the real-world use case is a client retrying after a timeout, not two simultaneous first-time submissions — and is documented here rather than solved with an upfront reservation row, consistent with how Phase 0-b1/1b documented other accepted limitations instead of over-building. Unlike that race window, **unbounded row growth is not accepted** — Task 1.3 adds a scheduled cleanup deleting rows older than 48 hours (double the 24-hour dedup window, so a key is never evicted while still valid).

### Non-overlap: enforced atomically at the database level, not check-then-act

A check (`hasActiveRun`) followed separately by an insert is race-prone: two concurrent trigger requests can both pass the check before either row exists. Task 1.1 therefore adds a **partial unique index** on `job_run(job_definition_id)` restricted to `PENDING`, `RUNNING`, and `CANCEL_REQUESTED`, so Postgres itself rejects a second concurrent active run for the same job definition. `RETRY_SCHEDULED` is deliberately excluded because it is bookkeeping for the failed predecessor and must coexist transactionally with the replacement `PENDING` retry row. `JobRunRepository.insertPending` catches the resulting `DuplicateKeyException` and rethrows `IllegalStateException`, which `GlobalExceptionHandler` (Task 3.2) already maps to HTTP 409. `hasActiveRun(jobDefinitionId)` is kept only as a fast, non-authoritative pre-check so the common case returns 409 without needing to reach the database's constraint machinery, but the index — not the check — is what actually prevents overlap under concurrency.

### Error handling

A `@RestControllerAdvice` (`GlobalExceptionHandler`) builds Spring's built-in `org.springframework.http.ProblemDetail` (native RFC 7807 support, no custom class needed) for: bean-validation failures (400), `IllegalArgumentException` (400), not-found (`NoSuchElementException`/empty `Optional` cases) (404), `IllegalStateException` such as illegal transitions or non-overlap conflicts (409), and a catch-all (500). Every message passes through the existing `org.replicadb.config.CredentialRedactor.redactMessage(...)` before being placed in the problem `detail` field.

### Log excerpt (explicit stub)

`GET /api/v1/runs/{id}/log` returns the persisted `error_message` as the excerpt (empty string when null/succeeded), with a comment stating that full 256 KB run-log capture (the "Persisted run log" row of the *Operational defaults* markdown table in `ARCHITECTURE_DECISIONS.md`'s Phase 1 section — a documentation cross-reference, not a database table) is deferred — building a per-run Log4j2 capturing appender is a separate, larger feature not required by this slice's acceptance criteria.

### Concurrent job-definition updates (documented, not fixed)

> ⚠️ Critic note: two simultaneous `PUT /api/v1/jobs/{id}` requests are last-write-wins with no optimistic-locking `version` column. Adding one would face the same positional-record blast-radius problem documented above for `modeWarning`. Not fixed in this slice; acceptable for a single-operator control plane with no concurrent-editor UI yet (the frontend is Phase 1c-4).

### Pagination

Shared `PageRequestParams.of(Integer page, Integer size)` (new small utility) validates/clamps: `page` defaults to `0` (rejects negative), `size` defaults to `50`, clamped to `[1, 200]` per Decision 4. Reused by both controllers.

### Testing strategy

- Pure logic (state machine reuse, mode-warning derivation, pagination clamping, idempotency upsert SQL) → plain JUnit 5 where no DB is needed, Testcontainers PostgreSQL (`@ServiceConnection`, matching the existing `PostgresTestcontainersConfig` pattern) where it is.
- `RunExecutionCoordinator` unit tests use a real `JobExecutionService` against a SQLite source fixture seeded with 5,000 rows (large enough for the test thread to reliably win the race to call `requestCancellation` before completion) so cancellation-of-a-running-task can be exercised deterministically, mirroring the Phase 0-b1 cancellation test style already used in the core.
- Controllers are tested with `@SpringBootTest` + `TestRestTemplate`/`MockMvc` (already available via `spring-boot-starter-test`), against the Testcontainers Postgres context.
- One end-to-end IT triggers a real run through `POST /api/v1/jobs/{id}/runs` and polls `GET /api/v1/runs/{id}` until it reaches `SUCCEEDED`, verifying the full HTTP → coordinator → `JobExecutionService` → `ReplicaDB.processReplica` path.

---

## Implementation Tasks

### 1. Persistence: repository additions

- [x] **1.1 Add `JobRunRepository.claimById`, `markCancelRequested`, broaden `markCancelled`, and enforce non-overlap with a partial unique index**
  Files: [replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java](replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java), `replicadb-server/src/main/resources/db/migration/V3__add_job_run_active_constraint.sql` (new), [replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java](replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java)
  Changes: New migration:
  ```sql
  CREATE UNIQUE INDEX ux_job_run_one_active_per_definition
      ON job_run (job_definition_id)
      WHERE status IN ('PENDING', 'RUNNING', 'CANCEL_REQUESTED');
  ```
  This is the actual non-overlap guarantee (Decision 2) — a check-then-insert without it is race-prone under concurrency. `RETRY_SCHEDULED` is intentionally excluded because it does not execute and must coexist transactionally with the replacement `PENDING` retry row. Add `claimById(UUID runId, String executorIdentity, Duration leaseDuration)` mirroring `claimNextPending`'s `FOR UPDATE SKIP LOCKED` + conditional `UPDATE` pattern but scoped to `WHERE id = :id AND status = 'PENDING'`. Add `hasActiveRun(UUID jobDefinitionId)` (`SELECT EXISTS(... WHERE job_definition_id = :id AND status IN ('PENDING','RUNNING','CANCEL_REQUESTED'))`) — a fast, non-authoritative pre-check only; the index above is the real guarantee. Change `insertPending(...)` to catch `org.springframework.dao.DuplicateKeyException` and rethrow `new IllegalStateException("Job definition " + jobDefinitionId + " already has an active run")`, reusing `GlobalExceptionHandler`'s existing `IllegalStateException` → 409 mapping (Task 3.2) with no new exception type. Add `markCancelRequested(UUID runId)` (`RUNNING → CANCEL_REQUESTED`, same `assertUpdated` pattern as existing `mark*` methods, but caught and logged rather than rethrown when it returns 0 rows because the run already reached a terminal state through a benign race with the coordinator's own completion callback — see Task 2.2). Change `markCancelled`'s `WHERE` clause from `status = 'RUNNING'` to `status IN ('RUNNING', 'CANCEL_REQUESTED')`.
  Tests: Two concurrent `insertPending` calls for the same `jobDefinitionId`, launched from two threads via an `ExecutorService` + `CountDownLatch` so they race — exactly one succeeds and the other throws `IllegalStateException`; after the winning run reaches a terminal status (`SUCCEEDED`/`FAILED`/`CANCELLED`), a new `insertPending` for the same job definition succeeds (the index no longer blocks); `claimById` claims only the targeted row when other `PENDING` rows exist for a different job; `claimById` returns empty for a non-`PENDING` or non-existent id; `hasActiveRun` is true for `PENDING`/`RUNNING`/`CANCEL_REQUESTED` and false for `RETRY_SCHEDULED`/terminal statuses; `markCancelRequested` succeeds from `RUNNING` and is a no-op (logged, not thrown) when the row is already terminal; `markCancelled` succeeds from both `RUNNING` and `CANCEL_REQUESTED`.
  Dependencies: None.

- [x] **1.2 Add pagination and listing queries to `JobRunRepository` and `JobDefinitionRepository`**
  Files: [replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java](replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java), [replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java](replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java), IT files for both
  Changes: `JobRunRepository`: add `findPage(UUID jobDefinitionIdOrNull, JobRunStatus statusFilterOrNull, int page, int size)` (`ORDER BY created_at DESC` with `LIMIT`/`OFFSET`) and matching `count(UUID jobDefinitionIdOrNull, JobRunStatus statusFilterOrNull)`. `JobDefinitionRepository`: add `findPage(int page, int size)` (`ORDER BY name`) and `count()`, plus `update(JobDefinition definition)` (full update of all mutable columns by `id`, setting `updated_at = now()`, throwing `NoSuchElementException` if the id does not exist).
  Tests: pagination returns correct slice/order across 3 pages of 2-row size; filtering by job definition id and by status each isolate the expected rows; `update` persists every mutable field and bumps `updated_at`; `update` on an unknown id throws.
  Dependencies: None.

- [x] **1.3 Add `run_trigger_idempotency` table, `RunTriggerIdempotencyRepository`, and a scheduled cleanup task**
  Files: `replicadb-server/src/main/resources/db/migration/V4__create_run_trigger_idempotency.sql` (new), `replicadb-server/src/main/java/org/replicadb/server/job/persistence/RunTriggerIdempotencyRepository.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/IdempotencyCleanupTask.java` (new), [replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java](replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/RunTriggerIdempotencyRepositoryIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/execution/IdempotencyCleanupTaskTest.java` (new)
  Changes: New migration:
  ```sql
  CREATE TABLE run_trigger_idempotency (
      idempotency_key VARCHAR(255) PRIMARY KEY,
      job_definition_id UUID NOT NULL,
      run_id UUID NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );

  CREATE INDEX idx_run_trigger_idempotency_created_at ON run_trigger_idempotency (created_at);
  ```
  Repository exposes `findValidRunId(String key)` (24h-window `SELECT`, per the Architecture & Design lookup query) and `upsert(String key, UUID jobDefinitionId, UUID runId)` (the `ON CONFLICT` upsert described above; a single atomic statement, no extra transaction configuration needed). Add `@EnableScheduling` to `ReplicaDbServerApplication` if not already present. New `IdempotencyCleanupTask` — a plain `@Component` (Spring's built-in `@Scheduled`, **not** Quartz, so this does not encroach on the 1c-2 scheduler slice) with a `@Scheduled(cron = "0 0 3 * * *")` method plus a package-visible `int purgeExpired()` (the actual logic, called directly by the test) deleting rows where `created_at < now() - interval '48 hours'` — double the 24-hour dedup window, so a key is never evicted while still valid.
  Tests: `findValidRunId` returns the run id when created within 24h, empty when older (insert a row with an explicit past `created_at` via direct JDBC to simulate expiry, and add the boundary case: a key exactly at the 24h edge, both just inside and just outside, is treated correctly) or absent; `upsert` followed by `findValidRunId` round-trips; two threads calling `upsert` with the same brand-new key concurrently both complete without throwing and the table ends with exactly one row for that key (last-write-wins, matching the documented accepted race window); `IdempotencyCleanupTask.purgeExpired()` deletes rows older than 48h and preserves newer ones (insert both via direct JDBC with explicit `created_at` values, call `purgeExpired()` directly rather than waiting for the cron trigger, assert row counts before/after).
  Dependencies: None.

### 2. Execution: async coordinator

- [x] **2.1 Widen `JobExecutionService.executeClaimedRun` with an `onStarted` callback**
  Files: [replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java](replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java), [replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java](replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java)
  Changes: Change `executeClaimedRun(JobRun run)` to `public JobRunOutcome executeClaimedRun(JobRun run, java.util.function.Consumer<ToolOptions> onStarted)`, calling `onStarted.accept(options)` right after `options = new ToolOptions(arguments);` and before `ReplicaDB.processReplica(options)`. `executeNextPending(String executorIdentity)` keeps its exact current signature/behavior, delegating to `executeClaimedRun(run, options -> {})`.
  Tests: Existing `JobExecutionServiceIT` and `emptyQueueDoesNotMarkAnyRun` tests must pass unmodified (run them, no behavior change expected). Add one new unit test asserting `onStarted` is invoked exactly once with the constructed `ToolOptions` before `processReplica` runs (verify via a callback that records the run id from `options.getExecutionContext()` and asserts it happened before the outcome is returned).
  Dependencies: None.

- [x] **2.2 Add `RunExecutionCoordinator`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java` (new), `replicadb-server/src/main/resources/application.yml` (add `replicadb.server.execution.pool-size: 4`)
  Changes: New `@Service` with a constructor-injected `JobRunRepository` and `JobExecutionService`, a `java.util.concurrent.ExecutorService` built from a bounded `ThreadPoolExecutor` sized by `@Value("${replicadb.server.execution.pool-size:4}")`, and a `ConcurrentHashMap<UUID, org.replicadb.cli.ToolOptions>` in-flight registry.
  `submit(UUID runId, String executorIdentity)`: submits a task to the executor that calls `jobRunRepository.claimById(runId, executorIdentity, Duration.ofMinutes(5))`; if present, calls `jobExecutionService.executeClaimedRun(claimedRun, options -> inFlight.put(runId, options))` in a `try/finally` that removes the entry from `inFlight` afterward. `executeClaimedRun`'s own persistence calls (`markSucceeded`/`markFailed`/`markCancelled`) can race with a concurrent `markCancelRequested` from a cancel request (Task 1.1 already turns that into a logged no-op rather than a thrown exception on the `markCancelRequested` side); if instead the *terminal* `mark*` call itself observes 0 rows updated because a cancel request changed the row first, catch and log that specific `IllegalStateException` in the coordinator's `finally` block rather than letting it propagate, since the row already reached a valid terminal state through the other path. Returns immediately (fire-and-forget `Future<?>`, all other exceptions logged, never propagated to the caller thread).
  `requestCancellation(UUID runId)`: looks up `inFlight.get(runId)`; if present, calls `.getExecutionContext().requestCancellation()` (confirmed existing method: `org.replicadb.cli.ToolOptions.getExecutionContext()` returns `org.replicadb.execution.ReplicationExecutionContext`, which already exposes `requestCancellation()`) and returns `true`; else returns `false` (caller decides the PENDING-vs-terminal branching per the Architecture & Design cancellation flow).
  Add a `@PreDestroy` method calling `executor.shutdown()`.
  Tests: New `RunExecutionCoordinatorTest` — `submit` executes a real SQLite-backed job (same fixture technique as `JobExecutionServiceIT`) and the run reaches `SUCCEEDED` in the repository after the async task completes (poll with a bounded retry loop); `requestCancellation` for an unknown/not-yet-registered run id returns `false`; a source SQLite table seeded with 5,000 rows (large enough that the test thread reliably wins the race to call `requestCancellation(runId)` after `submit(...)` returns but before the replication finishes) lets a test assert the run finishes as `CANCELLED`, not `SUCCEEDED`, by polling the repository with a bounded-retry loop; `@PreDestroy` shutdown test asserts `executor.isShutdown()` is `true` after the coordinator bean is destroyed (invoke the `@PreDestroy` method directly, or close a dedicated `AnnotationConfigApplicationContext` in the test) and that a task submitted just before shutdown still completes rather than being abandoned.
  Dependencies: Task 1.1 (`claimById`), Task 2.1 (`executeClaimedRun` overload).

### 3. API layer: DTOs and error handling

- [x] **3.1 Add `spring-boot-starter-validation` dependency and request/response DTOs**
  Files: [replicadb-server/pom.xml](replicadb-server/pom.xml), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionRequest.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionResponse.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunResponse.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/PageResponse.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionMapper.java` (new)
  Changes: Add the `spring-boot-starter-validation` dependency (excluding `spring-boot-starter-logging` like the other starters in this pom). `JobDefinitionRequest` mirrors `JobDefinition`'s mutable fields with `jakarta.validation` annotations (`@NotBlank` name/sourceConnect/sourceTable/sinkConnect/sinkTable, `@Min(1)` jobs). `JobDefinitionResponse` mirrors `JobDefinition` plus a computed `modeWarning` (non-null only when `mode == ReplicationMode.COMPLETE`, using Decision 2's exact warning text) — never includes `sourcePassword`/`sinkPassword` raw values, only whether each is set (`boolean sourcePasswordConfigured`). `JobRunResponse` mirrors `JobRun` 1:1 (no secrets to redact there). `PageResponse<T>(List<T> content, int page, int size, long totalElements)`. `JobDefinitionMapper` converts `JobDefinitionRequest` + generated/looked-up fields → `JobDefinition`, and `JobDefinition` → `JobDefinitionResponse`.
  Tests: `JobDefinitionMapperTest` — round-trip mapping preserves all fields; `modeWarning` is present only for `COMPLETE` mode and null for `INCREMENTAL`/`COMPLETE_ATOMIC`; response never exposes a literal password value even when `sourcePassword` is an `${env:...}` reference (only the boolean flag).
  Dependencies: None.

- [x] **3.2 Add `GlobalExceptionHandler` and `PageRequestParams`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/PageRequestParams.java` (new), corresponding test files (new)
  Changes: `@RestControllerAdvice` handling `MethodArgumentNotValidException`/`ConstraintViolationException` (400), `IllegalArgumentException` (400), `NoSuchElementException` (404), `IllegalStateException` (409), `Exception` (500) — every handler builds a `ProblemDetail` via `ProblemDetail.forStatusAndDetail(status, CredentialRedactor.redactMessage(message))`. `PageRequestParams.of(Integer page, Integer size)` returns a small record `(int page, int size)` defaulting/clamping per Decision 4 (`page` default 0, reject negative with `IllegalArgumentException`; `size` default 50, clamp silently to `[1, 200]`).
  Tests: Each exception type maps to the correct HTTP status and `application/problem+json` content type, and the `detail` field never contains a raw connection string when the triggering message included one (use a message containing `password=secret` and assert it is redacted); `PageRequestParams` defaults (`page=0`, `size=50`) when both are `null`; rejects negative page with `IllegalArgumentException`; `size=0` clamps to `1`; `size=500` clamps to `200`; `size=200` (the exact maximum) passes through unchanged.
  Dependencies: None.

### 4. API layer: controllers

- [x] **4.1 `JobDefinitionController`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java` (new)
  Changes: `@RestController @RequestMapping("/api/v1/jobs")` with `POST` (201 + `Location` header, body validated via `@Valid @RequestBody`), `GET` (paginated list using `PageRequestParams`), `GET /{id}` (404 via `NoSuchElementException` if absent), `PUT /{id}` (`@Valid @RequestBody`, delegates to `JobDefinitionRepository.update`, 404 if absent). `PUT` is a **full replace of every field except `id`, `name`, `createdAt`**: `name` is immutable (it is used as a natural lookup key elsewhere via `findByName`) and `id`/`createdAt` are server-owned; `sourceConnect`/`sourceUser`/`sourcePassword`/`sourceTable`/`sourceWhere`/`sinkConnect`/`sinkUser`/`sinkPassword`/`sinkTable`/`mode`/`jobs`/`incrementalWatermarkColumn`/`initialWatermarkValue` are all writable, and `updatedAt` is set to `now()` by the repository. `JobDefinitionRequest` therefore omits `name` on the update path — the controller reads the existing definition's `name` and passes it through unchanged, and rejects (400) any request body that includes a `name` differing from the stored one, to make the immutability explicit rather than silently ignored.
  Tests: `@SpringBootTest` + `MockMvc`/`TestRestTemplate` against Testcontainers Postgres — create returns 201 with `Location`; create with a blank `name` returns 400 problem+json; list respects `page`/`size` and returns correct `totalElements`; get-by-id 200 and 404; update changes every writable field and 404 for unknown id; update attempting to change `name` returns 400; a `mode=complete` create's response includes a non-null `modeWarning`.
  Dependencies: Task 1.2 (`update`, `findPage`, `count`), Task 3.1 (DTOs/mapper), Task 3.2 (exception handler/page params).

- [x] **4.2 `JobRunController` — read endpoints**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java` (new)
  Changes: `@RestController` with `GET /api/v1/jobs/{id}/runs` (paginated, filtered by `jobDefinitionId`), `GET /api/v1/runs` (paginated, optional `?status=` filter), `GET /api/v1/runs/{id}` (404 if absent), `GET /api/v1/runs/{id}/log` (returns `{"runId":..., "excerpt": <error_message or "">}`, per the Architecture & Design log stub). The `status` query parameter is declared as `@RequestParam(required = false) String status` (plain `String`, not `JobRunStatus`, to control the error response precisely) and parsed manually with `JobRunStatus.valueOf(status.toUpperCase(Locale.ROOT))` inside a `try/catch (IllegalArgumentException)` that rethrows as the same `IllegalArgumentException` type `GlobalExceptionHandler` already maps to 400 — this avoids relying on Spring's default enum-conversion failure (which produces a less controlled `TypeMismatchException`/500 by default).
  Tests: list-by-job returns only that job's runs across pages; `GET /runs?status=FAILED` filters correctly; `GET /runs?status=not_a_real_status` returns a 400 problem detail with a message naming the invalid value; get-by-id 200/404; log endpoint returns the persisted `error_message` for a `FAILED` run and an empty excerpt for a `SUCCEEDED` one.
  Dependencies: Task 1.2 (`findPage`/`count`), Task 3.1 (`JobRunResponse`), Task 3.2.

- [x] **4.3 `JobRunController` — trigger, cancel, retry**
  Files: same `JobRunController.java` and test file as Task 4.2
  Changes: `POST /api/v1/jobs/{id}/runs` — requires `Idempotency-Key` header (400 if missing); checks `RunTriggerIdempotencyRepository.findValidRunId` first (returns 202 + the existing run's body/`Location` if hit, and does **not** call `insertPending` again); else calls `insertPending` directly (relying on the Task 1.1 partial unique index for the atomic non-overlap guarantee — `hasActiveRun` may still be checked first purely to produce a cleaner 409 message before hitting the constraint, but the constraint is authoritative), catching the `IllegalStateException` it throws on conflict (409); on success, `upsert` the idempotency row, `coordinator.submit(runId, "api")`, return 202 with `Location: /api/v1/runs/{runId}` and body `JobRunResponse` for the new `PENDING` run. `POST /api/v1/runs/{id}/cancel` — implements the exact reordered branching described in Architecture & Design (`PENDING`→direct `CANCELLED`; `RUNNING`→`coordinator.requestCancellation` first, `markCancelRequested` only if that returned `true`; else 409), response body is `{"runId", "status", "warning"}` as specified there. `POST /api/v1/runs/{id}/retry` — 409 if the run is not `FAILED`; else `jobRunRepository.scheduleRetry(id)` then `coordinator.submit(newRunId, "api")`, returns 202 + `Location` and `JobRunResponse` body for the new run.
  Tests: trigger without `Idempotency-Key` → 400; trigger twice with the same key → same run id both times, only one row created in `job_run` (assert via a repository count, not just response equality); trigger with an idempotency key that is present but older than 24h (inserted directly via JDBC with a past `created_at`) → creates a genuinely new run, not the stale one; trigger while a run is already active for that job (different idempotency key) → 409; trigger then poll `GET /runs/{id}` until `SUCCEEDED` (SQLite fixture, same technique as `JobExecutionServiceIT`); cancel on a `PENDING` run → immediately `CANCELLED`, response `warning` is still populated; cancel on a terminal run → 409; cancel response body always contains a non-empty `warning` string, and its content differs between a `complete`-mode job and an `incremental`-mode job (assert both); retry on a `FAILED` run creates a new `PENDING`→(eventually)`SUCCEEDED`/`FAILED` run referencing `previousRunId`; retry on a non-`FAILED` run → 409.
  Dependencies: Task 1.1, Task 1.3, Task 2.2 (`RunExecutionCoordinator`), Task 4.2 (same controller class).

### 5. Integration test and configuration cleanup

- [x] **5.1 End-to-end IT: full HTTP trigger → async execution → cancel path**
  Files: `replicadb-server/src/test/java/org/replicadb/server/job/api/JobLifecycleIT.java` (new)
  Changes: One `@SpringBootTest(webEnvironment = RANDOM_PORT)` test class exercising the full stack with a real embedded server and Testcontainers Postgres: create a job definition via `POST /api/v1/jobs`, trigger a run via `POST /api/v1/jobs/{id}/runs` against SQLite source/sink fixtures, poll `GET /api/v1/runs/{id}` until `SUCCEEDED`, then assert `GET /api/v1/jobs/{id}/runs` and `GET /api/v1/runs?status=SUCCEEDED` both include it.
  Tests: This task *is* the test (no separate test-of-a-test needed) — the file above is the deliverable and must pass.
  Dependencies: Task 4.1, Task 4.2, Task 4.3.

- [x] **5.2 Wire pool-size config and verify Actuator/CI unaffected**
  Files: [replicadb-server/src/main/resources/application-api.yml](replicadb-server/src/main/resources/application-api.yml), [replicadb-server/src/test/java/org/replicadb/server/HealthEndpointTest.java](replicadb-server/src/test/java/org/replicadb/server/HealthEndpointTest.java)
  Changes: No functional change expected — this task is a verification pass confirming `HealthEndpointTest` (Phase 1a) still passes with the new controllers/beans on the classpath, and that `replicadb.server.execution.pool-size` is documented in `application-api.yml` with the shipped default of 4.
  Tests: Run `HealthEndpointTest`, `ReplicaDbServerApplicationTest`, `CoreDependencyResolutionTest`, and `CoreVersionAlignmentTest` unmodified and confirm they still pass with the new `job.api` package present.
  Dependencies: Task 4.1, Task 4.2, Task 4.3.

---

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `V3__add_job_run_active_constraint.sql` — partial unique index `ux_job_run_one_active_per_definition` on `job_run(job_definition_id)`, the actual non-overlap guarantee
- `JobRunRepository.claimById(UUID runId, String executorIdentity, Duration leaseDuration): Optional<JobRun>`
- `JobRunRepository.hasActiveRun(UUID jobDefinitionId): boolean` (fast pre-check only, not authoritative)
- `JobRunRepository.markCancelRequested(UUID runId): void`
- `JobRunRepository.findPage(UUID jobDefinitionIdOrNull, JobRunStatus statusFilterOrNull, int page, int size): List<JobRun>`
- `JobRunRepository.count(UUID jobDefinitionIdOrNull, JobRunStatus statusFilterOrNull): long`
- `JobDefinitionRepository.findPage(int page, int size): List<JobDefinition>`
- `JobDefinitionRepository.count(): long`
- `JobDefinitionRepository.update(JobDefinition definition): JobDefinition`
- `RunTriggerIdempotencyRepository.findValidRunId(String key): Optional<UUID>`
- `RunTriggerIdempotencyRepository.upsert(String key, UUID jobDefinitionId, UUID runId): void`
- `IdempotencyCleanupTask.purgeExpired(): int` — `@Scheduled` daily, deletes rows older than 48h
- `JobExecutionService.executeClaimedRun(JobRun run, Consumer<ToolOptions> onStarted): JobRunOutcome` (widened from private, no-op-callback overload preserved as `executeNextPending`)
- `RunExecutionCoordinator.submit(UUID runId, String executorIdentity): void`
- `RunExecutionCoordinator.requestCancellation(UUID runId): boolean`
- `PageRequestParams(int page, int size)` — record, `PageRequestParams.of(Integer, Integer)`
- `PageResponse<T>(List<T> content, int page, int size, long totalElements)` — record
- `JobDefinitionRequest` / `JobDefinitionResponse` / `JobRunResponse` — DTOs in `org.replicadb.server.job.api`
- Cancel response shape: `{"runId": UUID, "status": JobRunStatus, "warning": String}`

</details>

<details>
<summary>Dependencies</summary>

- New Maven dependency: `spring-boot-starter-validation` (Task 3.1), excluding `spring-boot-starter-logging` like the existing starters in `replicadb-server/pom.xml`.
- No new dependency needed for RFC 7807: Spring 6 / Spring Boot 3.3's `org.springframework.http.ProblemDetail` is already transitively available via `spring-boot-starter-web`.
- No new dependency for MockMvc/`TestRestTemplate`: already available via `spring-boot-starter-test`.

</details>

<details>
<summary>Testing Strategy</summary>

- Unit tests (no container): `JobExecutionServiceIT`'s new callback-ordering test, `JobDefinitionMapperTest`, `GlobalExceptionHandler` tests, `PageRequestParams` tests.
- Testcontainers PostgreSQL (`@ServiceConnection`, via the existing `PostgresTestcontainersConfig` import pattern): all repository IT additions, `RunExecutionCoordinatorTest`, both controller test classes, `JobLifecycleIT`.
- SQLite file fixtures (same technique as `JobExecutionServiceIT.createDatabase(...)`) stand in as source/sink for every test that needs a real end-to-end replication, avoiding extra containers.
- Existing Phase 1a/1b tests (`HealthEndpointTest`, `CoreDependencyResolutionTest`, `CoreVersionAlignmentTest`, `JobExecutionServiceIT`, `FlywayMigrationTest`, `JobDefinitionRepositoryIT`, `JobRunRepositoryIT`, `JobDefinitionTest`, `JobRunTest`, `JobRunStatusTest`, `JobRunStateMachineTest`, `ToolOptionsArgsBuilderTest`, `JobDefinitionEnvResolverTest`) must all continue to pass unmodified — none of this plan's changes alter their signatures except the additive `executeClaimedRun` overload (Task 2.1), which is backward compatible.
- CI: no changes expected to `CT_Push.yml`'s `server` job — it already runs Testcontainers-backed tests for this module with Docker configured (Phase 1b).

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 12/12 (100%).
- Tasks that required plan adjustment: 4/12 (33%).
- Test loop iterations: 22 total (12 first-pass, 8 second-pass, 2 third-pass).

### Gaps Encountered

#### Gap 1: Retry bookkeeping conflicts with active-run uniqueness (Plan-to-Implementation)
- **Task**: 1.1 — active-run partial unique index.
- **Plan assumed**: `RETRY_SCHEDULED` could be included in the active-run index.
- **Reality**: `scheduleRetry(...)` changes the failed row to `RETRY_SCHEDULED` before inserting its replacement `PENDING` row in one transaction; the index would reject every retry.
- **Resolution**: Excluded `RETRY_SCHEDULED` from the index and `hasActiveRun`; only executable states are unique per definition.
- **Learning**: Check state constraints against every multi-row state transition before finalizing a partial unique index.

#### Gap 2: REST mode casing was not compatible with Jackson defaults (Plan-to-Implementation)
- **Task**: 3.1/4.1 — request and response DTO mode field.
- **Plan assumed**: JSON values such as `complete` would bind directly to the existing `ReplicationMode` enum.
- **Reality**: Jackson's default enum deserializer accepts `COMPLETE`, not the user-facing lower-case `getModeText()` values used by the API contract.
- **Resolution**: REST DTOs use lower-case mode text, the mapper parses it case-insensitively, and responses return the same lower-case representation.
- **Learning**: Test serialized API representations at the boundary; domain enum names are not automatically API enum values.

#### Gap 3: Existing migration-count test was not included in migration impact analysis (Intent-to-Plan)
- **Task**: 5.2 — final server verification.
- **Plan assumed**: Existing migration tests would remain valid after adding V3/V4.
- **Reality**: `FlywayMigrationTest` asserted exactly two migrations.
- **Resolution**: Updated it to assert and validate all four forward-only migrations.
- **Learning**: When adding migrations, search for exact version/count assertions in all test layers before running the full suite.

#### Gap 4: Generic exception handling changed Spring's unmapped-resource 404 to 500 (Intent-to-Plan)
- **Task**: 5.2 — Actuator/server contract verification.
- **Plan assumed**: A catch-all RFC 7807 handler would preserve existing framework status behavior.
- **Reality**: Spring MVC's `NoResourceFoundException` was caught by the generic handler and returned 500.
- **Resolution**: Added an explicit 404 handler for missing resources and made unreadable request bodies return a generic 400 detail without echoing parser content.
- **Learning**: Exception advice must preserve framework-level 404 exceptions separately from application failures.

### Patterns Discovered
- **Atomic per-job non-overlap**: PostgreSQL partial unique indexes are the authoritative guard; pre-checks only improve common-case error messages.
- **Single-instance cancellation**: Register the live `ToolOptions` after construction and before core execution so API cancellation can reach the per-run context.
- **REST validation boundary**: Use validation groups when create and update have different server-owned fields.
- **Problem details**: Return Spring `ProblemDetail` and redact all dynamic details at the exception boundary.
