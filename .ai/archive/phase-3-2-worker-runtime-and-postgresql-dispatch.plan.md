# Implementation Plan: Phase 3.2 - Worker Runtime and PostgreSQL Dispatch

## Task Source - JIRA: none - approved Phase 3.2 architecture decision

The source of truth is the Phase 3.2 section of `ARCHITECTURE_DECISIONS.md`. The selected approach is **B: explicit shared runtime boundary**.

Acceptance criteria derived from the architecture decision:

- Add an isolated `worker` Spring profile with no public REST API, frontend, Spring Security session/authentication, or Quartz scheduler.
- Keep `api` responsible for REST, authentication, schedules, and product-level schedule reconciliation. It must publish durable run identifiers instead of transporting job configuration.
- Extract or formalize one shared execution path used by both the compatibility `api` executor and worker executors: load the definition, resolve references, build `ToolOptions`, invoke `ReplicaDB`, commit the watermark only after successful merge, and use token-fenced finalization/audit behavior.
- Add a transactional dispatch boundary for manual triggers, Quartz triggers, manual retries, and lease-recovery replacements. The transaction must create/update the durable `JobRun` and issue `pg_notify('replicadb_runs', run_id)` using the same PostgreSQL connection before commit.
- Add a dedicated PostgreSQL listener for `replicadb_runs` and a run-control channel for cancellation identifiers. Listener payloads contain only durable run identifiers, never credentials or complete job definitions.
- Implement listener reconnect and re-subscription. A lost notification must affect latency only, not correctness.
- Implement mandatory polling at worker startup, after listener reconnect, and at a configurable interval. Polling must discover pending/retryable work, active cancellation requests owned by the worker, and expired runs requiring recovery.
- Implement a bounded worker dispatcher that claims through `JobRunStore`/`RunLeaseService`, executes one run at a time by default, and safely handles duplicate notifications and duplicate polling.
- Implement an independent heartbeat loop that renews the PostgreSQL lease during source reads, staging, cleanup, `mergeStagingTable()`, and `atomicInsertStagingTable()`. A failed or fenced renewal must request local cancellation and must never be treated as a successful lease extension.
- Move cancellation delivery to durable `CANCEL_REQUESTED` state plus best-effort local/control-channel signaling. A worker that misses the control notification must discover the request through polling.
- Recover expired runs as new attempts from the beginning, preserving the abandoned row and using Phase 3.1 retry/backoff/fencing semantics. Never resume an abandoned attempt.
- Preserve the standalone CLI artifact, CLI exit codes, options-file contract, no-PostgreSQL CLI path, redaction rules, and all existing mode-specific sink semantics.
- Do not add an external broker, Quartz JDBC clustering, shared login throttling, a new cancellation table, or Phase 3.3 load/chaos deployment topology in this plan.

## Overview

Phase 3.2 moves managed execution from an API-local coordination assumption to a PostgreSQL-backed worker runtime while retaining the existing `api` process as a configurable single-instance compatibility path. The API and Quartz create durable runs and publish only their identifiers; one or more workers compete for those rows, execute the existing ReplicaDB core, and persist fenced outcomes through the Phase 3.1 contract.

The implementation makes the runtime boundary explicit so API-local cancellation, worker cancellation, lease renewal, watermark commit, audit, and temporary-options cleanup share one execution lifecycle. PostgreSQL `LISTEN/NOTIFY` is deliberately only a wake-up mechanism; startup, reconnect, and periodic polling remain the correctness path.

## Architecture & Design - Approach: explicit shared runtime boundary

### Runtime topology

```text
                         PostgreSQL
      job definitions / job runs / leases / watermarks
                    / cancellation intent / audit
                       ^                 ^
             short JDBC transactions  LISTEN/NOTIFY
                       |                 |
       +---------------+-----------------+----------------+
       |                                                |
+------v------+                                   +-----v------+
| API profile |                                   | Worker     |
| REST        |-- insert + pg_notify(run_id) -->  | profile    |
| Security    |                                   | listener   |
| Quartz      |                                   | polling    |
| optional    |                                   | heartbeat  |
| local exec  |                                   | core exec  |
+-------------+                                   +------------+
```

- `JobRun` is the only durable work item. Notification payloads are hints to attempt a claim; they are not commands containing configuration.
- The existing API-local `RunExecutionCoordinator` remains available behind `replicadb.server.local-execution.enabled`, defaulting to `true` for compatibility with the monolithic deployment. A distributed deployment sets it to `false`; API and Quartz then only create and notify runs, while workers execute them.
- The API must publish notifications even when local execution is enabled. PostgreSQL claim uniqueness makes a worker racing with the compatibility coordinator safe; only one claimant receives the lease.
- No database lock is held during ReplicaDB execution. Claims, recovery, notification, lease renewal, cancellation intent, and finalization are short transactions.

### Shared execution lifecycle

`RunExecutionHandle` is an internal, non-serialized handle containing the claimed run, its `ToolOptions`, and its `ReplicationExecutionContext`. `JobExecutionService` creates it immediately before `ReplicaDB.processReplica(options)` and invokes an observer before the core starts. Each JVM/application context has exactly one profile-neutral `ActiveRunRegistry` singleton; an API process and a worker process have separate registries and never share them. Both runtimes register handles only for local cancellation and heartbeat coordination. The registry is never the source of truth for run state.

The common service continues to own:

1. Definition lookup and the last committed watermark lookup.
2. Environment-reference resolution and temporary options-file creation.
3. Construction of `ToolOptions` without logging the resolved arguments.
4. Invocation of the unchanged `ReplicaDB.processReplica(ToolOptions)` contract.
5. Token-fenced counters, terminal state, watermark, and audit writes.
6. Redacted failure handling and options-file deletion in every exit path.

A fenced finalization is a stale-worker result, not a successful terminal operation. It must not produce a second terminal audit event or change the replacement attempt.

### Dispatch and notification contract

- `RunDispatchService` owns transaction boundaries for pending-run creation, idempotency records, manual retry replacement, and recovery replacement notification.
- `RunNotificationPublisher` has two internal operations: publish a run identifier on `replicadb_runs` and publish a cancellation identifier on `replicadb_run_control`.
- The PostgreSQL adapter executes `SELECT pg_notify(...)` through the transaction-bound Spring JDBC connection. A committed run has a notification; a rolled-back run has no notification. Polling still covers a listener that was disconnected at commit time.
- Normal dispatch uses database-owned `available_at`/`created_at` values (`now()`), rather than JVM timestamps. Existing explicit-timestamp repository methods remain a bounded deprecated bridge until all callers and fixtures move.
- A local-seed request creates and cancels a pending row in one transaction without publishing an execution notification, preserving the existing no-execution fixture behavior.

### Worker execution and cancellation

- `WorkerDispatchCoordinator` has a bounded executor with `max-concurrent-runs=1` by default. It claims directed identifiers from notifications and undirected eligible rows from polling through `RunLeaseService`.
- Duplicate notification, duplicate polling, and an API-local claim race are harmless because the PostgreSQL claim is conditional and token-generating.
- A worker registry records active handles only after the run is claimed. The coordinator checks durable state immediately after registration so a cancellation that raced with claim/registration is still delivered locally.
- The listener's cancellation event requests cancellation on the local handle when present. `PollingFallback` queries `CANCEL_REQUESTED` rows owned by the worker, covering missed control notifications and the registration race.
- Worker shutdown stops intake, closes the listener connection, stops polling/heartbeat scheduling, and requests local cancellation for active handles. Abrupt loss is recovered by the Phase 3.1 expiry contract.

### Lease and heartbeat policy

- Worker defaults are a five-minute lease, a 30-second heartbeat interval, a 30-second polling interval, and one concurrent run. All are configurable under `replicadb.worker`.
- Every heartbeat uses `runId + LeaseToken` and delegates time calculation to PostgreSQL `now()` through `RunLeaseService.renewLease(...)`.
- `RENEWED` is the only successful heartbeat result. `FENCED`, `NOT_FOUND`, or a renewal exception requests local cancellation; the worker never fabricates a lease timestamp or continues as though ownership were certain.
- The heartbeat begins after `ToolOptions` construction and remains active until the shared execution method has completed cleanup and terminal finalization, including merge and atomic swap operations.

### Profile boundary

- `api` gates REST controllers, RFC 7807 web advice, Spring Security/session beans, user bootstrap, audit/admin HTTP endpoints, Quartz schedule reconciliation/jobs, API maintenance tasks, and the local execution coordinator.
- `worker` uses `spring.main.web-application-type=none`, excludes Quartz and servlet security/session auto-configuration, and starts only shared PostgreSQL repositories/services, worker dispatch, listener, polling, heartbeat, audit writes, and the ReplicaDB core.
- Both managed profiles use PostgreSQL/Flyway configuration. The worker does not expose a web server or session store; it may run Flyway with PostgreSQL's migration lock before starting its repositories.
- The root CLI Maven artifact and launchers are not modified for this phase.

### Performance, security, and failure considerations

- The listener holds one dedicated PostgreSQL JDBC connection and uses a bounded notification wait so shutdown/reconnect can be observed. It never shares a connection with a long-running state transaction.
- The worker executor and listener/poller activity must be sized so the listener connection cannot starve execution/repository connections; configuration and documentation will state the required pool headroom.
- Notification payloads are parsed as UUIDs and malformed payloads are ignored with a bounded diagnostic containing no dynamic configuration. Run identifiers and worker identities are safe operational metadata; resolved credentials, DSNs, options arrays, and password values never enter notifications, logs, audit details, or exceptions.
- No new Flyway migration is required. Phase 3.2 consumes V13/V14 state and adds no durable control table; PostgreSQL notification loss is handled by polling.

## Implementation Tasks

### 1. Establish the shared execution handle and local registry

- [x] **1.1 Introduce an internal execution handle and migrate both execution entry points to it**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionHandle.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/ActiveRunRegistry.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunExecutionHandleTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunExecutionCoordinatorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java`
  Changes: Replace the `Consumer<ToolOptions>` callback boundary with a non-serialized `RunExecutionHandle`/observer that exposes only the run id, lease token, `ToolOptions`, and cancellation context to internal runtime code. Register `ActiveRunRegistry` as one profile-neutral singleton per application context; an API process and a worker process each get their own local instance. Add register/remove/request-cancellation operations; keep it local and explicitly non-authoritative. Ensure `JobExecutionService` creates/registers the handle before `ReplicaDB.processReplica(...)`, preserves fenced finalization/audit behavior, watermark commit timing, redacted errors, and temporary options-file cleanup, and never logs the handle or resolved options. Update the API coordinator to use the registry and retain its existing directed claim behavior.
  Tests: Unit-test handle cancellation and registry removal/idempotency; assert a callback runs before core invocation; verify an absent local handle does not change durable state; retain successful, failed, cancelled, stale-finalizer, and options-file cleanup coverage in the existing service/coordinator integration tests.
  Dependencies: None; consumes the Phase 3.1 `JobRunStore`, lease, and finalization contracts.

### 2. Make new dispatch paths database-time authoritative

- [x] **2.1 Add immediate pending/retry and polling-scan operations to the run port**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java`, `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`
  Changes: Add production-facing operations that create an immediately eligible pending run and an immediately eligible manual-retry replacement using PostgreSQL `now()`, returning the persisted row rather than JVM-constructed timestamps. Add bounded `findExpiredRunIds(limit)` and `findCancellationRequestedRunIds(executorIdentity, limit)` queries. Keep `available_at <= now()`, lease expiry, ordering, and recovery selection database-owned; retain explicit-`Instant` methods only as deprecated compatibility bridges until Task 14. Do not add a migration or a durable notification table.
  Tests: With real PostgreSQL connections, assert newly dispatched rows are immediately claimable and never briefly appear in the future relative to PostgreSQL `now()`; verify expired scans include `RUNNING`/`CANCEL_REQUESTED` rows and exclude live/terminal rows; verify cancellation scans are limited to the owning executor identity and respect the batch limit; apply all 14 existing Flyway migrations explicitly and assert no schema version was added by this task.
  Dependencies: None beyond the implemented Phase 3.1 schema and port.

### 3. Add the PostgreSQL notification port and adapter

- [x] **3.1 Implement transactional run/control notification publishing**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/port/RunNotificationPublisher.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/persistence/PostgresNotificationPublisher.java` (new), `replicadb-server/pom.xml`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/PostgresNotificationPublisherIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/NotificationChannelContractTest.java` (new)
  Changes: Define internal `publishRun(UUID)` and `publishCancellation(UUID)` operations with fixed channels `replicadb_runs` and `replicadb_run_control`. Implement them with the PostgreSQL JDBC driver and `SELECT pg_notify(...)` through the transaction-bound `JdbcTemplate` connection. Change the server module's PostgreSQL dependency from runtime-only to compile scope because the listener and publisher require the driver's notification API; do not add R2DBC or alter the root CLI dependency graph. Reject/avoid non-UUID payloads and never include job definitions, connection strings, credentials, or arbitrary caller text.
  Tests: Use one connection to listen and a separate transactional connection to publish; prove a committed notification is delivered only after commit, a rolled-back transaction produces no notification, both channels carry exactly one UUID payload, and publisher calls participate in the caller's transaction rather than opening an independent connection. Assert the channel names and payload contract in a focused unit test.
  Dependencies: Task 2.1 for the database-time/transaction port conventions.

### 4. Centralize durable dispatch and notification coupling

- [x] **4.1 Add `RunDispatchService` for manual, scheduled, retry, and recovery dispatches**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/application/RunDispatchService.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/application/RunDispatchResult.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java`, `replicadb-server/src/main/java/org/replicadb/server/job/persistence/RunTriggerIdempotencyRepository.java`, `replicadb-server/src/test/java/org/replicadb/server/job/application/RunDispatchServiceTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/application/RunDispatchServiceIT.java` (new)
  Changes: Add transactional application methods for normal manual dispatch, scheduled dispatch, manual retry, and expired-run recovery. Move idempotency lookup/upsert into the manual-dispatch transaction, preserve the existing same-key replay and cross-job conflict behavior, and let the PostgreSQL active-run constraint remain authoritative under races. Insert the run/replacement with database-owned immediate eligibility, publish only its UUID before the transaction commits, and return whether the request created or replayed a run. Preserve local seeding by creating/cancelling the pending row without an execution notification. For recovery, publish only when `recoverExpiredRun(...)` returns a replacement; cancellation recovery publishes nothing. Treat a notification failure as a transaction failure for run creation/replacement so an uncommitted work item cannot be orphaned, while polling remains the recovery path for notifications lost after commit.
  Tests: Mock the ports for idempotency/replay/conflict and transaction decision coverage; with PostgreSQL, prove run creation plus idempotency plus notification commit atomically, rollback leaves neither row nor notification, concurrent same-key requests return one run, active-run races remain a clean conflict, manual retry publishes the new attempt only, recovery publishes a replacement only once, and expired cancellation creates no replacement/notification.
  Dependencies: Tasks 2.1 and 3.1.

### 5. Define the API/worker Spring profile boundary

- [x] **5.1 Isolate API-only beans and add the worker application configuration**
  Files: `replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java`, `replicadb-server/src/main/java/org/replicadb/server/config/ApiSchedulingConfiguration.java` (new), `replicadb-server/src/main/resources/application.yml`, `replicadb-server/src/main/resources/application-api.yml`, `replicadb-server/src/main/resources/application-worker.yml` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleController.java`, `replicadb-server/src/main/java/org/replicadb/server/security/api/AuthController.java`, `replicadb-server/src/main/java/org/replicadb/server/security/api/UserController.java`, `replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java`, `replicadb-server/src/main/java/org/replicadb/server/security/config/ProblemDetailAuthenticationEntryPoint.java`, `replicadb-server/src/main/java/org/replicadb/server/security/JobAccessService.java`, `replicadb-server/src/main/java/org/replicadb/server/security/auth/LoginAttemptService.java`, `replicadb-server/src/main/java/org/replicadb/server/security/auth/ReplicaDbUserDetailsService.java`, `replicadb-server/src/main/java/org/replicadb/server/security/persistence/AppUserRepository.java`, `replicadb-server/src/main/java/org/replicadb/server/security/persistence/JobPermissionRepository.java`, `replicadb-server/src/main/java/org/replicadb/server/security/execution/AdminBootstrapRunner.java`, `replicadb-server/src/main/java/org/replicadb/server/audit/api/AuditEventController.java`, `replicadb-server/src/main/java/org/replicadb/server/audit/execution/AuditRetentionTask.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/IdempotencyCleanupTask.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/QuartzScheduleService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduleReconciler.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduledRunTriggerJob.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java`, `replicadb-server/src/test/java/org/replicadb/server/ReplicaDbServerApplicationTest.java`, `replicadb-server/src/test/java/org/replicadb/server/WorkerProfileContextTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/HealthEndpointTest.java`
  Changes: Move `@EnableScheduling` into an API-only configuration and apply `@Profile("api")` to REST/advice, security/session, user bootstrap, Quartz, API maintenance, schedule reconciliation, and API-local execution beans. Keep shared repositories, `JobExecutionService`, audit write services, and future worker beans profile-neutral or worker-scoped as appropriate. Move Quartz properties out of the base configuration; add `application-worker.yml` with PostgreSQL/Flyway, `spring.main.web-application-type=none`, no session store, Quartz auto-configuration exclusion, and these explicit defaults: `replicadb.worker.identity=${REPLICADB_WORKER_IDENTITY:}`, `max-concurrent-runs=1`, `lease-duration=5m`, `heartbeat-interval=30s`, `poll-interval=30s`, `listener.initial-reconnect-delay=1s`, `listener.max-reconnect-delay=30s`, `shutdown-timeout=30s`, and `spring.datasource.hikari.maximum-pool-size=8`. Keep `application-api.yml` servlet/Quartz/session settings and add `replicadb.server.local-execution.enabled=true` as the compatibility default. Validate exact auto-configuration exclusions against the resolved Spring Boot version rather than relying only on property names.
  Tests: Keep the API context/health tests green and assert its REST/security/session/Quartz beans remain present. Add a base worker-profile context assertion with Testcontainers that has no servlet web server, `SecurityFilterChain`, `SessionRepository`, REST controller, Quartz scheduler, frontend handler, or API bootstrap bean, while the shared PostgreSQL datasource/repositories can load without worker-specific beans; defer full listener/executor startup and resource-close assertions to Task 11. Verify worker startup does not expose a public HTTP port once the worker runtime is wired.
  Dependencies: Tasks 1.1 and 4.1 for the beans and services being separated; worker-specific runtime beans are wired in Task 11.

### 6. Rewire API and Quartz creation paths through durable dispatch

- [x] **6.1 Preserve monolithic API compatibility while publishing every durable run**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduledRunTriggerJob.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java`, `replicadb-server/src/main/resources/application-api.yml`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunCancellationRaceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/ScheduledJobLifecycleIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/RunDispatchApiIT.java` (new)
  Changes: Replace direct `insertPending`, idempotency upsert, `scheduleRetry`, and local submission in the controller/scheduled job with `RunDispatchService`. After the dispatch transaction commits, submit to the API-local coordinator only when `replicadb.server.local-execution.enabled` is true; when false, leave execution to workers. Preserve local-seed behavior, ACL checks, idempotency responses, active-run conflict handling, audit actors/details, schedule trigger identity, cancellation warning text, and retry history. Ensure no local claim is attempted before the dispatch transaction commits. Keep local cancellation as a best-effort signal after durable cancellation intent and use the same `ActiveRunRegistry` handle as the shared path.
  Tests: Assert normal API mode still completes a SQLite replication without a worker, distributed mode creates a `PENDING` run and publishes it without submitting to the local coordinator, manual/scheduled/retry paths do not create duplicate runs, same-key replay returns the original run, local seed never executes, and controller responses never expose a lease token. Exercise the cancellation race where terminal finalization happens between durable request and local signaling.
  Dependencies: Tasks 1.1, 4.1, and 5.1.

### 7. Add the independent heartbeat service

- [x] **7.1 Renew active worker leases and fence local execution on loss**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/HeartbeatService.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/HeartbeatHandle.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/application/RunLeaseService.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/HeartbeatServiceTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/HeartbeatServiceIT.java` (new)
  Changes: Add a worker-only heartbeat component backed by an independent `ScheduledExecutorService`, with explicit start/stop handles per claimed run. Bind the interval from `replicadb.worker.heartbeat-interval` as a `Duration` (default `30s`) and the lease duration from `replicadb.worker.lease-duration` (default `5m`); allow test profiles to override both with short durations. Use the claimed `JobRun` id/token and configured lease duration on every renewal. Renew throughout the complete `executeClaimedRun` lifetime, including core merge/swap and cleanup. On `FENCED`, `NOT_FOUND`, or a renewal exception, request cancellation on the handle and stop treating the lease as owned; do not use a JVM timestamp or silently continue. Make shutdown cancel scheduled heartbeat tasks and await their termination without logging options or credentials.
  Tests: Unit-test repeated renewal, stop-on-completion, stop-on-fence, stop-on-database-error, and no renewal after shutdown. With real PostgreSQL, assert `heartbeat_at`/`lease_until` advance from database time while a fake long operation is blocked, a stale token cannot be renewed, and a fenced heartbeat requests local cancellation. Include a merge/atomic-operation seam or a controlled execution barrier proving the heartbeat remains active beyond a single source-copy batch.
  Dependencies: Tasks 1.1 and 2.1; consumes the Phase 3.1 lease-renewal contract.

### 8. Implement the bounded worker execution coordinator

- [x] **8.1 Claim, register, execute, heartbeat, finalize, and clean up worker runs**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerRunIdentity.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/ActiveRunRegistry.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerDispatchCoordinatorTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerDispatchCoordinatorIT.java` (new)
  Changes: Add a worker-scoped bounded dispatcher with configurable `replicadb.worker.max-concurrent-runs` (default `1`), `replicadb.worker.lease-duration` (default `5m`), and graceful stop. Bind `replicadb.worker.identity` as an optional non-secret override; when blank/unset, generate `worker-<UUID>` once at bean construction so each process has a unique executor identity. Expose separate wake-up methods for a directed run id, general eligible work, and a cancellation id. Claim directed notifications with `RunLeaseService.claimRequested(...)` and poll/general wake-ups with `claimNextEligible(...)`; never trust a notification as proof of ownership. On claim, register the `RunExecutionHandle`, check the durable row for a raced `CANCEL_REQUESTED` state, start the heartbeat, execute the shared service, then stop heartbeat/remove the handle in a `finally` path. Treat empty claims and fenced terminal outcomes as normal coordination results, not successes or process-fatal errors. Ensure duplicate wake-ups cannot exceed the configured concurrency and worker shutdown does not claim new rows.
  Tests: With mocked services, cover empty/direct/queue claims, capacity limits, cancellation-before-registration, heartbeat lifecycle, executor rejection, cleanup after core failure, and graceful shutdown. With two independently constructed coordinators and real PostgreSQL, prove distinct runs are claimed exactly once, duplicate signals do not execute twice, a worker-local stale finalization cannot alter a replacement attempt, and one worker can continue claiming after another coordinator stops.
  Dependencies: Tasks 1.1, 2.1, 5.1, and 7.1.

### 9. Implement the mandatory polling fallback and expiry recovery

- [x] **9.1 Poll pending work, owned cancellation intent, and expired leases**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PollingFallback.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunDispatchService.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PollingFallbackTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PollingFallbackIT.java` (new)
  Changes: Add a worker lifecycle component that runs an immediate startup scan, a scan after listener reconnect, and a single non-overlapping periodic scan at the configured interval. Each scan must: request the worker coordinator to claim eligible pending rows; query and signal `CANCEL_REQUESTED` rows whose `executor_identity` is this worker; find bounded expired `RUNNING`/`CANCEL_REQUESTED` ids; call the transactional dispatch/recovery service for each; and signal any replacement run returned by recovery. Use PostgreSQL-owned time and bounded batches. A missed notification, duplicate scan, or scan overlap must be harmless. Polling must remain active when the listener is down.
  Tests: Unit-test startup/reconnect/periodic invocation, overlap suppression, bounded batches, and cancellation signaling with no active local handle. With PostgreSQL, publish a run before the worker starts and prove startup polling claims it, stop notification delivery and prove periodic polling still claims it, detect a missed cancellation and signal its active handle, and run two concurrent expiry scans proving one recovery/replacement and no duplicate notification.
  Dependencies: Tasks 2.1, 4.1, and 8.1.

### 10. Add the dedicated PostgreSQL listener with reconnect behavior

- [x] **10.1 Listen for run and cancellation wake-ups without owning durable state**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListener.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/NotificationPayloadParser.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PollingFallback.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListenerTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListenerIT.java` (new)
  Changes: Use one dedicated PostgreSQL JDBC connection per worker, issue `LISTEN replicadb_runs` and `LISTEN replicadb_run_control`, and consume notifications with a bounded wait so the listener can stop cleanly. Parse only UUID payloads; route run ids to directed claims and control ids to local cancellation; invoke polling immediately after a successful reconnect/re-subscription. On SQL/connection failure, close the connection and use exponential reconnect backoff starting at `replicadb.worker.listener.initial-reconnect-delay=1s`, doubling after each failure and capping at `replicadb.worker.listener.max-reconnect-delay=30s`; reset the delay after a successful subscription. Log only channel/run/reconnect metadata, reconnect within the cap, and never assume a missed notification is durable work loss. Keep the listener lifecycle separate from transactional repositories and the worker execution pool.
  Tests: Unit-test valid/malformed payloads, channel routing, reconnect callback, shutdown while blocked, exponential backoff/cap/reset, and no credential-bearing diagnostics. With PostgreSQL, assert notifications wake a listener on both channels, a notification sent while disconnected is recovered by polling, reconnect re-subscribes, duplicate notifications result in at most one claim, a listener reconnect during an active heartbeat does not stop lease renewal or execution, and a listener connection is distinct from repository transaction connections.
  Dependencies: Tasks 3.1, 8.1, and 9.1.

### 11. Wire the worker profile and lifecycle as a runnable runtime

- [x] **11.1 Register worker-only beans and enforce startup/shutdown boundaries**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeConfiguration.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeProperties.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeLifecycle.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PollingFallback.java`, `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListener.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/HeartbeatService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/resources/application-worker.yml`, `replicadb-server/src/test/java/org/replicadb/server/WorkerProfileContextTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/config/WorkerRuntimeConfigurationTest.java` (new)
  Changes: Register worker components only under the `worker` profile with a `WorkerRuntimeLifecycle` coordinator implementing `SmartLifecycle`. Its `start()` order is coordinator intake, polling, then listener; its `stop()` first calls `WorkerDispatchCoordinator.stopAccepting()`, then stops polling and listener delivery, stops per-run heartbeats, shuts down the worker executor, and awaits the configured timeout. Do not rely on bean declaration order or `@PreDestroy` ordering. Configure worker identity, concurrency, lease, heartbeat, polling, listener reconnect, executor shutdown timeout, and datasource pool headroom from environment/property values. Validate at startup that `spring.datasource.hikari.maximum-pool-size >= max-concurrent-runs + 4` (one listener, claim/recovery/polling, heartbeat/finalization headroom); ship a default pool size of `8` for the default concurrency of `1`, and require operators to raise it when concurrency increases. Start polling even if listener startup fails. Ensure a worker process has no public web listener, frontend static route, Spring Security session/authentication, Quartz scheduler, API schedule reconciliation, or API-local run registry.
  Tests: Start the full worker Spring context against PostgreSQL, assert all required shared/worker beans and all forbidden API/web beans, verify a pending run is claimed from a real notification, stop the context and assert no listener thread/connection or executor remains, verify the lifecycle event order is intake-stop before polling/listener close, reject an undersized datasource pool with a clear startup error, and verify a listener startup failure still leaves polling able to execute a pending run. Keep the existing API context test proving the API profile remains runnable.
  Dependencies: Tasks 5.1, 7.1, 8.1, 9.1, and 10.1.

### 12. Make durable cancellation and remote control race-safe

- [x] **12.1 Publish control notifications after durable intent and deliver them to active workers**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/application/RunCancellationService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/ActiveRunRegistry.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListener.java`, `replicadb-server/src/test/java/org/replicadb/server/job/application/RunCancellationServiceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunCancellationRaceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/RemoteCancellationIT.java` (new)
  Changes: Extend `RunCancellationService` with the internal control publisher. Persist `CANCEL_REQUESTED` and the mode-specific warning first, publish the run id on `replicadb_run_control`, then attempt the best-effort local/API signal; a missing local handle must not reject the durable request. A control-notification failure is logged without dynamic details and does not roll back the already durable cancellation intent; polling remains the recovery path. Route worker control notifications to the active registry and keep polling as the fallback. Preserve pending cancellation, terminal races, cancellation warnings, `CANCELLED` classification, and token-fenced completion. Do not add a cancellation table or expose lease tokens.
  Tests: Unit-test ordering (repository update before publisher/local callback), fail-open notification behavior, absent local handles, and duplicate cancellation requests. With PostgreSQL and a controlled long-running execution, cancel through the API-side service while the worker listens and assert the worker's context is signaled and the run finishes `CANCELLED`; repeat with the listener disabled and prove polling delivers it; assert a stale worker cannot finalize or advance a watermark after cancellation/recovery.
  Dependencies: Tasks 3.1, 6.1, 8.1, 9.1, and 10.1.

### 13. Prove the distributed execution path end to end

- [x] **13.1 Run shared API/worker scenarios against one PostgreSQL state store**
  Files: `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/DistributedWorkerLifecycleIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerExecutionIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/config/PostgresTestcontainersConfig.java`, `replicadb-server/src/test/java/org/replicadb/server/ReplicaDbServerApplicationTest.java`
  Changes: Add a test harness that exposes one class-scoped PostgreSQL Testcontainer and injects its JDBC URL into both the API dispatch context and one or more worker runtime contexts through `@DynamicPropertySource`/`SpringApplicationBuilder`; do not start one independent container per context. Use an isolated UUID-derived schema per test class, create it before contexts start, run Flyway explicitly against that schema with `FluentConfiguration.schemas(schema).defaultSchema(schema)`, set both JDBC URLs to `currentSchema=<schema>`, and drop the schema in teardown; do not mutate the shared default schema. Disable API-local execution for the distributed scenarios, create single-table SQLite source/sink fixtures, dispatch through the same application service used by the API/scheduler, and let workers discover via notification or polling. Keep test data isolated per method/schema and never print resolved connection values. Cover terminal audit, row counters, duration, incremental watermark commit, failed/cancelled watermark preservation, retry attempt linkage, recovery/backoff, and no-resume semantics.
  Tests: Verify one API dispatch is executed by exactly one of two workers; duplicate notification and duplicate polling do not duplicate the run or watermark; notification rollback leaves no pending run; missed notification is recovered at startup/reconnect/periodic polling; worker loss/recovery creates a new attempt; a healthy heartbeat prevents expiry during a long operation; remote cancellation reaches the owner; stale finalization is fenced; API status reads come from PostgreSQL; and worker profile exposes no HTTP/auth/UI surface. Use real PostgreSQL for claims/leases/notifications and SQLite only as the source/sink fixture, never as the metadata store.
  Dependencies: Tasks 6.1, 7.1, 8.1, 9.1, 10.1, 11.1, and 12.1.

### 14. Remove the Phase 3.1 repository compatibility bridge after migration

- [x] **14.1 Move remaining tests/callers to ports and token-aware services**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, `.github/workflows/CT_Push.yml`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunExecutionCoordinatorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunCancellationRaceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/ScheduledJobLifecycleIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduledRunTriggerJobTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/PersistenceDependencyResolutionTest.java`
  Changes: Search all production/test callers and migrate them from deprecated `claimById`, `claimNextPending`, JVM-timestamp compatibility inserts, and un-fenced terminal methods to `JobRunStore`, `RunLeaseService`, `RunFinalizationService`, and the database-time dispatch operations. Remove only the deprecated repository wrappers and test-only repository constructor overloads once no caller remains; keep the production Spring constructor explicit. Add a CI guard after the server test step that checks only deprecated signatures with `rg -n 'claimById|claimNextPending|public void markSucceeded\\(UUID runId, long|public void markFailed\\(UUID runId, long|public void markCancelled\\(UUID runId, long|public void markCancelRequested|public void markPendingCancelled' replicadb-server/src/main/java` and fails when any compatibility method remains; token-aware `markSucceeded/markFailed/markCancelled` overloads are intentionally allowed. This guard is a migration check, not a runtime source of truth. Preserve direct repository integration coverage for PostgreSQL behavior while making application tests assert port/service contracts.
  Tests: Run the CI search guard and compile the server after removal; rerun claim, recovery, fencing, cancellation-race, watermark, and dependency-resolution tests. Confirm the migration count remains 14 and the root CLI source/artifact has no dependency on the managed server or Spring Boot.
  Dependencies: Tasks 1.1, 2.1, 4.1, 6.1, 8.1, and 13.1.

### 15. Document operations and complete release verification

- [x] **15.1 Document profile startup and validate the Phase 3.2 boundary**
  Files: `replicadb-server/frontend/README.develop.md`, `ARCHITECTURE_DECISIONS.md`, `.github/workflows/CT_Push.yml`, `replicadb-server/pom.xml`
  Changes: Document API-only versus worker startup, environment-managed PostgreSQL/worker identity settings, `local-execution.enabled`, notification channels, mandatory polling, listener reconnect, heartbeat/lease defaults, worker concurrency, shutdown behavior, and the fact that retries re-execute from the beginning. Update the architecture status and Phase 3.2 checklist only after executable validation passes; keep Phase 3.3 Quartz JDBC clustering, shared throttling, metrics/chaos/load topology, and production deployment hardening explicitly not implemented. Keep CI's Docker/Testcontainers setup and add an explicit worker-profile context/build check to the server job if the existing full Maven suite does not already exercise it. Do not add secrets, DSNs, or credentials to documentation or workflow fixtures.
  Tests: Resolve and validate the existing documentation path; run focused server tests for dispatch, listener, polling, heartbeat, worker profile, cancellation, recovery, and distributed lifecycle; run the complete `mvn -B test --file replicadb-server/pom.xml` with Docker/Testcontainers; package the server jar and smoke-start both `api` and `worker` profiles using environment-managed metadata settings; run the root CLI compatibility tests and verify its artifact/classpath contains no Spring Boot classes and starts without PostgreSQL; finish with `git diff --check` and a targeted search proving no notification payload, API DTO, OpenAPI schema, log, or audit detail contains a lease token or resolved credential.
  Dependencies: Tasks 1.1 through 14.1.

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `RunExecutionHandle`: internal lifecycle object for one claimed run; contains the claimed `JobRun`, `LeaseToken`, `ToolOptions`, and `ReplicationExecutionContext` needed by local cancellation/heartbeat. It has no REST/OpenAPI serialization and no credential-bearing `toString`.
- `ActiveRunRegistry`: process-local signal registry only. It maps run ids to active handles and is never used to decide whether a run exists, is owned, terminal, or eligible.
- `RunNotificationPublisher`: internal port with fixed UUID-only run/control notifications. `PostgresNotificationPublisher` implements it with `pg_notify` on the transaction-bound PostgreSQL connection.
- `RunDispatchService`: transactional application boundary returning a `RunDispatchResult` that distinguishes a newly created run, an idempotency replay, and a recovery replacement.
- `WorkerDispatchCoordinator`: bounded worker claim/execute adapter. It does not accept credentials or complete job definitions from notifications.
- `PollingFallback`: worker lifecycle component for startup, reconnect, periodic eligibility/cancellation scans, and expired-run recovery.
- `PostgreSQLNotificationListener`: dedicated JDBC listener with two fixed channels, UUID parsing, reconnect, and polling wake-up.
- `HeartbeatService`/`HeartbeatHandle`: independent per-run lease-renewal lifecycle. Only `RENEWED` extends ownership; all other outcomes request local cancellation.
- Existing `JobRun`, `RetryPolicy`, `LeaseToken`, `JobRunStore`, `RunLeaseService`, `RunRecoveryService`, `RunCancellationService`, and `RunFinalizationService` remain the durable state contract from Phase 3.1.

</details>

<details>
<summary>Dependencies</summary>

- No new external broker, ORM, migration, or reactive database stack is required.
- The existing PostgreSQL JDBC driver becomes a compile-time server dependency because `LISTEN/NOTIFY` requires its driver-specific notification API. The root CLI artifact is not changed.
- Existing Spring JDBC, Spring transactions, Flyway, Quartz, Spring Security/session, Log4j2, Testcontainers, JUnit Jupiter 6, and SQLite fixture dependencies remain in use.
- Worker runtime properties belong under `replicadb.worker`; API compatibility execution belongs under `replicadb.server.local-execution`.
- Flyway remains forward-only and at version 14 for this phase. No `V15` notification/control table is planned.

</details>

<details>
<summary>Testing Strategy</summary>

| Layer | Tooling | Required evidence |
| --- | --- | --- |
| Shared execution | JUnit Jupiter 6, Mockito, existing SQLite fixtures | Handle registration before core execution, cleanup, cancellation, watermark commit, redaction, and fenced finalization |
| Dispatch port/application | Spring JDBC + Mockito + PostgreSQL Testcontainers | Database-time insertion, idempotency transaction, commit/rollback notification coupling, manual retry and recovery notification |
| Notification transport | PostgreSQL JDBC + two real connections | Commit visibility, rollback suppression, UUID-only payloads, dedicated listener connection, reconnect/resubscription |
| Worker coordination | JUnit Jupiter + real PostgreSQL claims | Bounded concurrency, directed/queue claims, duplicate signals, two-worker non-overlap, graceful shutdown |
| Polling/recovery | PostgreSQL Testcontainers | Startup/reconnect/periodic scans, missed notifications, cancellation fallback, duplicate expiry recovery, backoff eligibility |
| Heartbeat/fencing | Mockito plus PostgreSQL Testcontainers | Renewal during long operation, database-owned timestamps, stop-on-fence/error, local cancellation, stale update rejection |
| Profile boundary | Spring Boot context tests with `@ActiveProfiles("api")`/`worker` | API surface present only in API; worker has no web/security/session/Quartz surface and closes resources |
| Distributed lifecycle | API/worker contexts sharing isolated PostgreSQL state | Exactly-once claim effect, watermark safety, remote cancellation, recovery attempt linkage, audit and status visibility |
| Release regression | Maven, Docker/Testcontainers, CLI smoke test | Server package and both profiles start; root CLI remains Spring-free and PostgreSQL-independent |

Use real PostgreSQL for locking, `now()`, notifications, leases, recovery, and fencing. Use Mockito only for narrow lifecycle/port seams. Keep listener/poller tests bounded and avoid broad Surefire selectors whose expansion is unknown.

</details>

## Risks, Assumptions, and Deferred Work

- The API-local coordinator remains a compatibility path, not the durable source of truth. Distributed deployments must set `replicadb.server.local-execution.enabled=false`; workers then own execution.
- `LISTEN/NOTIFY` has no replay or acknowledgement. Startup, reconnect, and periodic polling are mandatory and are tested as correctness paths, not merely operational conveniences.
- A worker can lose its lease while a database driver or sink operation is unresponsive. The heartbeat requests local cancellation, token fencing protects metadata, and mode-specific sink safety remains the existing contract; `complete` remains destructive and may require operator repair.
- A cancellation control notification can arrive before a worker registers the claimed handle. The worker must check durable `CANCEL_REQUESTED` immediately after registration and polling must repeat the check.
- The PostgreSQL JDBC notification API is driver-specific. The resolved driver version/API must be inspected before coding, and the compile-scope change must be limited to `replicadb-server`.
- The listener's dedicated connection consumes pool capacity. Worker configuration and documentation must leave headroom for the listener, heartbeats, claims, repositories, and concurrent ReplicaDB runs.
- Spring profile isolation must be verified with a real worker application context. Merely setting `spring.main.web-application-type=none` is insufficient if explicit controller/security/Quartz beans still load.
- Phase 3.3 remains deferred: Quartz JDBC clustering, shared PostgreSQL login throttling, health/metrics, multi-node deployment packaging, and reproducible chaos/load checks.
- No JIRA acceptance criteria were supplied; the acceptance criteria in this plan are extracted from the approved architecture decision and the Phase 3.1 contract already implemented in the repository.

## Phase Exit Criteria

Phase 3.2 is complete only when:

- A worker profile starts with PostgreSQL state and no public API/frontend/security session/Quartz scheduler.
- Manual and scheduled API dispatches create durable runs and transaction-coupled UUID notifications; API-local execution remains available only through the explicit compatibility setting.
- Multiple workers can claim distinct eligible runs through PostgreSQL, and duplicate/missed notifications have the same correctness result as polling.
- Listener reconnect triggers a rescan, and startup/reconnect/periodic polling discovers pending work, cancellation intent, and expired leases.
- Heartbeats renew a lease during long source/sink operations; a fenced or failed heartbeat cancels local execution and cannot mutate state afterward.
- Remote cancellation reaches the owning worker through control notification or polling, persists the warning, and finishes as `CANCELLED` without advancing a watermark.
- Lease expiry retains the abandoned attempt and creates a new attempt only when Phase 3.1 policy permits it; no execution resumes from a prior partition.
- A stale worker cannot finalize a run, emit terminal audit success, overwrite counters/errors, or commit a watermark.
- The API reads durable state from PostgreSQL, and the CLI artifact/classpath and no-PostgreSQL execution path remain unchanged.

## Quality Gate Notes

The plan must be reviewed once by a critic sub-agent before presentation. Critical issues must be corrected directly; limited important issues should be made concrete in the relevant task, and ambiguous important issues should be called out without silently inventing a new product decision.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 15/15 (100%)
- Tasks that required plan adjustment: 2/15 (13.3%)
- Test loop iterations: 35 total (27 first-pass, 8 second-pass, 0 third-pass)

### Gaps Encountered

#### Gap 1: SpringApplicationBuilder property precedence (Plan-to-Implementation)

- **Task**: 13.1 — Run shared API/worker scenarios against one PostgreSQL state store
- **Plan assumed**: `SpringApplicationBuilder.properties(...)` would override empty profile datasource defaults for the isolated shared schema.
- **Reality**: The builder properties had lower precedence than the profile YAML, so contexts failed before datasource creation.
- **Resolution**: Passed the isolated datasource and worker settings as Spring Boot `--property=value` application arguments and kept Flyway disabled after explicit schema migration.
- **Learning**: Shared-context integration harnesses must verify configuration-property precedence before relying on profile-specific overrides.

#### Gap 2: Worker test lifecycle readiness (Plan-to-Implementation)

- **Task**: 10.1/12.1 — Validate listener delivery and remote cancellation
- **Plan assumed**: Starting the asynchronous listener and immediately publishing a UUID would reliably exercise the subscribed connection.
- **Reality**: A notification published before `LISTEN` completed was legitimately missed, causing a false timeout in the live cancellation test.
- **Resolution**: Synchronized publication with the listener reconnect/subscription callback; missed-notification behavior remains covered separately through polling tests.
- **Learning**: Notification integration fixtures must synchronize on successful subscription and test lost delivery independently through the durable polling path.

### Patterns Discovered

- Shared-schema Spring contexts: see `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/DistributedWorkerLifecycleIT.java`.
- Explicit worker shutdown ordering: see `replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeLifecycle.java`.
- UUID-only PostgreSQL wake-ups with polling recovery: see `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListener.java` and `PollingFallback.java`.
