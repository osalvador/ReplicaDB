# Implementation Plan: Phase 1c-2 — Quartz Scheduler

## Task Source

No JIRA ticket. Source is [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md), which lists Quartz scheduling as the next unimplemented item in the Priority 2 checklist ("Add Quartz scheduling with an explicit timezone per job") and names `spring-boot-starter-quartz` in the "Spring Boot modules" section for "durable schedules, misfire handling, and non-overlapping triggers." Agreed scope with the user: **Quartz scheduler only** (Approach B: `RAMJobStore` + PostgreSQL-backed reconciliation on startup) — Spring Security/users/ACLs (Phase 1c-3) and the frontend (Phase 1c-4) remain separate future slices.

Acceptance criteria (derived from Decision 2 "API and scheduler execution", Decision 4's "Schedule recurring runs" operation, and the Operational Defaults table):

- A job definition can be given a recurring schedule: a cron expression and an explicit IANA timezone (`UTC` when unset, per the Operational Defaults table).
- Quartz fires a trigger, which creates a `JobRun` in `PENDING` and submits it through the existing `RunExecutionCoordinator` — the same asynchronous execution path already used by manual `POST /api/v1/jobs/{id}/runs` triggers (Phase 1c-1), with no duplicated execution logic.
- A job never overlaps with another run of itself by default (Decision 2, "API and scheduler execution" step 4 note) — reusing the existing `job_run` partial unique index (Phase 1c-1) rather than reimplementing non-overlap in Quartz.
- Quartz must not hold a scheduler thread while a database replication is running (Decision 2, step 4) — the scheduled `Job` only claims/submits and returns; it never blocks on `ReplicaDB.processReplica(...)` itself.
- Schedules survive a process restart: `job_schedule` in PostgreSQL is the durable source of truth, and Quartz's in-memory scheduler is reconciled from it on every startup.
- Explicitly **out of scope**: Spring Security/roles/ACLs, the frontend, persisting the cancellation warning onto the `job_run` row (a separate, unrelated Priority 2 gap), and a JDBC-backed (persistent) Quartz `JobStore` — this slice uses `RAMJobStore`, consistent with Decision 2's "Monolithic Control Plane First."

## Overview

`replicadb-server` can create, trigger, monitor, cancel, and retry job runs over HTTP (Phase 1c-1), but every run must be triggered manually. This plan adds a per-job recurring schedule (`job_schedule` table + `JobSchedule` domain model), a generic Quartz `Job` that fires by `jobDefinitionId` and submits through the existing `RunExecutionCoordinator`, and `PUT`/`GET`/`DELETE /api/v1/jobs/{id}/schedule` endpoints to manage it — closing the last unchecked item under Priority 2's "Monolithic Control Plane" before Phase 1c-3 (security) or Phase 2 (distributed workers).

## Architecture & Design

**Approach**: Quartz with `RAMJobStore` (in-memory) plus a PostgreSQL-backed `job_schedule` table as the actual durable source of truth. An `ApplicationRunner` reads every `enabled` schedule at startup and registers it into Quartz. This was chosen over a JDBC (`JobStoreTX`) Quartz store because a JDBC store would require importing and Flyway-versioning Quartz's own `qrtz_*` schema and configuring cluster-checkin behavior for a control plane that Decision 2 explicitly scopes as single-instance ("Monolithic Control Plane First") — clustering concerns belong to Phase 2, and Phase 2's own dispatch model is PostgreSQL `LISTEN/NOTIFY` + row-locking claims, not Quartz clustering. Functionally the two approaches are equivalent for this phase: neither "catches up" on fires missed while the process was down (see misfire policy below), so `RAMJobStore` loses nothing except Quartz-native persistence of trigger *bookkeeping*, which `job_schedule` already provides at the product level.

### Why a separate `JobSchedule` entity, not new `JobDefinition` fields

`JobDefinition` is a Java record with a positional constructor already used across roughly ten existing call sites (repository insert/update/row-mapper, `JobExecutionServiceIT`, `ToolOptionsArgsBuilderTest`, `JobDefinitionEnvResolverTest`, `JobDefinitionRepositoryIT`, `JobDefinitionTest`, `JobDefinitionMapperTest`, `JobDefinitionControllerTest`, `JobRunControllerTest`, `JobLifecycleIT`). Adding `cronExpression`/`timeZone`/`enabled` fields to it would force touching all of them for data that is conceptually a *trigger* attached to a job definition, not part of the definition itself (mirroring Quartz's own model, where a `JobDetail` and its `Trigger`(s) are separate). This plan therefore adds a new `job_schedule` table and `JobSchedule` record, one-to-one with `job_definition_id`, referenced but not embedded — the same reasoning already documented for why Phase 1c-1 kept `modeWarning` out of `JobDefinition`.

### Domain model

`JobSchedule(UUID jobDefinitionId, String cronExpression, String timeZone, boolean enabled, Instant createdAt, Instant updatedAt)`. Validation in its compact constructor: `cronExpression` non-blank and valid per `org.quartz.CronExpression.isValidExpression(...)`; `timeZone` non-blank and a valid `ZoneId` (`ZoneId.of(timeZone)` must not throw); defaults applied by the mapper, not the record, so the domain type stays a pure validated value (consistent with `JobDefinition`'s own validation style).

### Non-overlap: reused, not reimplemented

The scheduled `Job` calls exactly the same sequence the manual trigger endpoint already uses: `jobRunRepository.hasActiveRun(jobDefinitionId)` as a fast pre-check, then `jobRunRepository.insertPending(...)`, then `runExecutionCoordinator.submit(runId, "scheduler")`. The Phase 1c-1 partial unique index (`ux_job_run_one_active_per_definition`) is what actually guarantees no overlap under concurrency; if a fire races with an already-active run, `insertPending(...)` throws `IllegalStateException`, which the `Job` catches and logs as a benign, expected skip (a scheduled fire finding the previous run still in flight is normal, not an error, and must not surface as a job failure in Quartz's own history).

### Generic `Job`, per-schedule `Trigger`

One Quartz `Job` class (`ScheduledRunTriggerJob`, annotated `@DisallowConcurrentExecution` as defense-in-depth against Quartz re-firing the same trigger while a previous fire's `Job.execute(...)` is still running — the database index remains the authoritative guarantee) is reused for every job definition. `QuartzScheduleService.schedule(JobSchedule)` builds a `JobDetail` keyed by `jobDefinitionId` carrying it in the `JobDataMap`, and a `CronTrigger` built from `CronScheduleBuilder.cronSchedule(jobSchedule.cronExpression()).inTimeZone(TimeZone.getTimeZone(jobSchedule.timeZone())).withMisfireHandlingInstructionDoNothing()` — `TimeZone.getTimeZone(String)` is used directly (not the `ZoneId` overload) since `JobSchedule`'s own compact constructor already validated the string via `ZoneId.of(...)`, so there is nothing left to gain from constructing a second `ZoneId` here. `withMisfireHandlingInstructionDoNothing()` is a deliberate choice: if the process was down when a fire was due, that fire is simply skipped rather than caught up — consistent with this architecture's existing "no resume, no catch-up" posture (Decision 3) and avoiding a burst of catch-up runs hammering a source database after an outage.

> ⚠️ Known limitation, accepted and not fixed in this slice: a misfire (a fire skipped because the process was down, or because Quartz's small firing thread pool was saturated) is silent by design — there is no alert or dashboard signal today. `ScheduledRunTriggerJob` logs every fire, skip (already-active), and misfire at `INFO`/`WARN` so the information exists in application logs for now; a metrics/alerting integration is left to a later phase. A cron expression that lands on a nonexistent or ambiguous local time during a DST transition (e.g. `"0 2 * * *"` in `America/New_York` on the spring-forward date) follows Quartz's own built-in DST handling as-is; this plan does not add special-case handling or a dedicated test for it.

> ⚠️ Design note (not a critic-flagged issue, called out here for visibility): Spring Boot's Quartz auto-configuration is expected to wire an autowiring-capable `JobFactory` so `@Autowired` fields work directly inside `ScheduledRunTriggerJob`. Task 2.2 verifies this empirically and owns adding the one-line `SpringBeanJobFactory` fallback bean in the same task if the assumption does not hold, so no downstream task is blocked by an undocumented conditional path.

### Startup reconciliation

`ScheduleReconciler implements ApplicationRunner` loads every `enabled = true` row from `JobScheduleRepository` and calls `QuartzScheduleService.schedule(...)` for each, right after the Spring context (including the `Scheduler` bean) is fully initialized. This is what makes `RAMJobStore` durable at the product level: PostgreSQL, not Quartz, is what survives a restart, and this runner is what re-populates Quartz from it every time the process starts.

### REST surface

New `JobScheduleController` under `/api/v1/jobs/{jobDefinitionId}/schedule`:
- `PUT` — create-or-replace (upsert) the schedule, validates the job definition exists, validates the cron expression and timezone, persists via `JobScheduleRepository.upsert(...)`, and calls `QuartzScheduleService.schedule(...)` (or `unschedule(...)` if the request sets `enabled: false`) in the same request. Returns the persisted schedule plus a computed `nextFireTime` (from the registered `Trigger`, or `null` when disabled).
- `GET` — read the current schedule; `404` (`NoSuchElementException`, already mapped by `GlobalExceptionHandler`) if none exists.
- `DELETE` — remove the schedule row and unregister the Quartz trigger; `204` on success, idempotent (no error if no schedule existed).

This is an intentional, documented extension of Decision 4's original API surface table, which predates Phase 1c's slicing and never listed a schedule endpoint.

### Testing strategy

- Pure logic (no Spring context): `JobSchedule` validation unit tests (cron/timezone rejection).
- `QuartzScheduleService` tests use a real, short-lived `org.quartz.Scheduler` (Quartz's own `StdSchedulerFactory` with `RAMJobStore`, started/shut down per test) to assert register/reschedule/unregister behavior without needing the full Spring context.
- One integration test fires a real schedule end-to-end: register a schedule with a 1-second cron against SQLite source/sink fixtures (same technique as `JobExecutionServiceIT`/`RunExecutionCoordinatorTest`), then poll `JobRunRepository` until a `JobRun` reaches `SUCCEEDED`.
- `ScheduleReconciler` is tested by inserting `JobSchedule` rows directly via JDBC, invoking the runner, and asserting the scheduler now has the expected `TriggerKey`s registered — proving restart-durability without an actual process restart.
- Controller tests follow the existing `@SpringBootTest` + `MockMvc` + Testcontainers Postgres pattern (`JobDefinitionControllerTest`, `JobRunControllerTest`).

---

## Implementation Tasks

### 1. Persistence: schedule domain and repository

- [x] **1.1 Add `job_schedule` table and `JobSchedule` domain record**
  Files: `replicadb-server/src/main/resources/db/migration/V5__create_job_schedule.sql` (new), `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobSchedule.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobScheduleTest.java` (new), [replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java](replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java)
  Changes: Migration:
  ```sql
  CREATE TABLE job_schedule (
      job_definition_id UUID PRIMARY KEY REFERENCES job_definition(id) ON DELETE CASCADE,
      cron_expression VARCHAR(120) NOT NULL,
      time_zone VARCHAR(64) NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  ```
  `ON DELETE CASCADE` is deliberate: a schedule has no meaning without its job definition, and there is still no `DELETE /api/v1/jobs/{id}` endpoint in this phase, so the clause only matters if a definition is ever removed directly against the database or by a future phase. `JobSchedule` record `(UUID jobDefinitionId, String cronExpression, String timeZone, boolean enabled, Instant createdAt, Instant updatedAt)` with a compact constructor validating: `jobDefinitionId` non-null; `cronExpression` non-blank and `org.quartz.CronExpression.isValidExpression(cronExpression)`; `timeZone` non-blank and `ZoneId.of(timeZone)` does not throw (wrap `DateTimeException` as `IllegalArgumentException`). Update `FlywayMigrationTest`'s migration-count assertions from 4 to 5 in the same task, since this is the migration that changes the count.
  Tests: valid cron/timezone combination constructs successfully; blank/invalid cron expression throws `IllegalArgumentException` with a message naming the field; invalid timezone id (e.g. `"Not/AZone"`) throws `IllegalArgumentException`; `null` `jobDefinitionId` throws; `FlywayMigrationTest` passes with the updated count of 5 applied migrations.
  Dependencies: None.

- [x] **1.2 Add `JobScheduleRepository`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobScheduleRepository.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobScheduleRepositoryIT.java` (new)
  Changes: `NamedParameterJdbcTemplate`-based repository (matching `JobDefinitionRepository`'s style) with `upsert(JobSchedule)` (`INSERT ... ON CONFLICT (job_definition_id) DO UPDATE SET cron_expression = EXCLUDED.cron_expression, time_zone = EXCLUDED.time_zone, enabled = EXCLUDED.enabled, updated_at = now()`, returning the persisted row with fresh timestamps), `findByJobDefinitionId(UUID): Optional<JobSchedule>`, `findAllEnabled(): List<JobSchedule>`, `delete(UUID jobDefinitionId): boolean` (returns whether a row existed).
  Tests: `upsert` inserts a new row and a second `upsert` for the same `jobDefinitionId` replaces `cronExpression`/`timeZone`/`enabled` and bumps `updatedAt`; `findByJobDefinitionId` returns empty for an unknown id; `findAllEnabled` excludes `enabled = false` rows; `delete` returns `true` when a row existed and `false` when it did not, and is safe to call twice.
  Dependencies: Task 1.1.

### 2. Scheduling: Quartz wiring and generic job

- [x] **2.1 Add `spring-boot-starter-quartz` and configure `RAMJobStore`**
  Files: [replicadb-server/pom.xml](replicadb-server/pom.xml), [replicadb-server/src/main/resources/application.yml](replicadb-server/src/main/resources/application.yml)
  Changes: Add the `spring-boot-starter-quartz` dependency (excluding `spring-boot-starter-logging` like the other starters in this pom). Add to `application.yml`: `spring.quartz.job-store-type: memory` (explicit, so the default is not left implicit), `spring.quartz.properties.org.quartz.scheduler.instanceName: ReplicaDbScheduler` (named for clearer logs; `instanceId` is left unset since it only matters for clustered `JobStoreTX`, not `RAMJobStore`), and `spring.quartz.properties.org.quartz.threadPool.threadCount: 2` (Quartz's own firing pool is separate from and much smaller than `replicadb.server.execution.pool-size`, since firing a trigger only claims-and-submits, it never runs a replication itself).
  Tests: none dedicated — covered by Task 2.2's context-loading test and the existing `ReplicaDbServerApplicationTest` continuing to pass with the new starter on the classpath.
  Dependencies: None.

- [x] **2.2 Add `ScheduledRunTriggerJob`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduledRunTriggerJob.java` (new), `replicadb-server/src/main/java/org/replicadb/server/config/QuartzJobFactoryConfig.java` (new, only if the verification below requires it), `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduledRunTriggerJobTest.java` (new)
  Changes: `@DisallowConcurrentExecution public class ScheduledRunTriggerJob implements org.quartz.Job`, with `@Autowired` `JobRunRepository` and `RunExecutionCoordinator`. Verify empirically (by running `ScheduledJobLifecycleIT`'s equivalent smoke path once field injection is wired) that Spring Boot's Quartz auto-configuration allows this field injection; if it does not, add `QuartzJobFactoryConfig` with a `SpringBeanJobFactory` `@Bean` that calls `setApplicationContext(...)` and is wired into the auto-configured `SchedulerFactoryBean` via a `SchedulerFactoryBeanCustomizer` — this task owns that fallback, so no later task is blocked by it. `execute(JobExecutionContext context)` reads `jobDefinitionId` (as a `String`, parsed to `UUID`) from `context.getMergedJobDataMap()`, logs at `INFO` that the trigger fired, calls `jobRunRepository.hasActiveRun(jobDefinitionId)`; if `true`, logs at `INFO` that the fire was skipped because a run is already active, and returns without creating a run. Otherwise calls `jobRunRepository.insertPending(jobDefinitionId, null, 1)` then `runExecutionCoordinator.submit(pending.id(), "scheduler")`, catching `IllegalStateException` from a benign race with the unique index, logging it at `INFO` (not `ERROR` — this is an expected outcome, not a failure) rather than rethrowing, since Quartz would otherwise record it as a failed job execution.
  Tests: `hasActiveRun` returning `true` results in no `insertPending`/`submit` call (Mockito-verified) and logs the skip; `hasActiveRun` returning `false` results in exactly one `insertPending` + `submit` call with `"scheduler"` as the executor identity; an `IllegalStateException` thrown by `insertPending` is caught and does not propagate out of `execute(...)`.
  Dependencies: Task 1.2 (for the real `hasActiveRun`/`insertPending` signatures used in tests), Task 2.1.

- [x] **2.3 Add `QuartzScheduleService`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/QuartzScheduleService.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/execution/QuartzScheduleServiceTest.java` (new)
  Changes: `@Service` constructor-injected with the Spring-managed `org.quartz.Scheduler` bean. `schedule(JobSchedule)`: builds a `JobKey`/`TriggerKey` both equal to `jobSchedule.jobDefinitionId().toString()` in group `"replicadb-jobs"`; if `!jobSchedule.enabled()`, calls `unschedule(jobDefinitionId)` instead. When enabled, builds a `JobDetail` for `ScheduledRunTriggerJob.class` (`storeDurably(true)`, `usingJobData("jobDefinitionId", jobDefinitionId.toString())`) and a `CronTrigger` via `CronScheduleBuilder.cronSchedule(jobSchedule.cronExpression()).inTimeZone(TimeZone.getTimeZone(jobSchedule.timeZone())).withMisfireHandlingInstructionDoNothing()`, then calls `scheduler.scheduleJob(jobDetail, trigger)` if the job is not yet registered, or `scheduler.rescheduleJob(triggerKey, trigger)` if it is (checked via `scheduler.checkExists(jobKey)`). `unschedule(UUID jobDefinitionId)`: `scheduler.deleteJob(jobKey)` if it exists, no-op otherwise. `nextFireTime(UUID jobDefinitionId): Optional<Instant>` reads the registered `Trigger.getNextFireTime()`, empty if not scheduled.
  Tests: Using a real `org.quartz.impl.StdSchedulerFactory`-created `RAMJobStore` scheduler started in `@BeforeEach` and shut down in `@AfterEach` (no Spring context needed) — `schedule(...)` on a new `JobSchedule` registers exactly one `JobKey`/`TriggerKey`; calling `schedule(...)` again with a different `cronExpression` reschedules (verified via `nextFireTime` changing) rather than throwing a duplicate-job error; calling `schedule(...)` twice in a row with the **same** `cronExpression` for the same `jobDefinitionId` (simulating the realistic overlap between `ScheduleReconciler`'s startup pass and a `PUT` arriving shortly after) still results in exactly one registered `JobKey`/`TriggerKey`, not a duplicate-job exception; `schedule(...)` with `enabled = false` results in no registered job; `unschedule(...)` removes a previously registered job and is a no-op when nothing was registered; `nextFireTime(...)` returns empty for an unknown `jobDefinitionId`.
  Dependencies: Task 1.1 (`JobSchedule`), Task 2.1, Task 2.2 (`ScheduledRunTriggerJob.class` reference).

- [x] **2.4 Add `ScheduleReconciler`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduleReconciler.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduleReconcilerTest.java` (new)
  Changes: `@Component public class ScheduleReconciler implements ApplicationRunner`, constructor-injected with `JobScheduleRepository` and `QuartzScheduleService`. `run(ApplicationArguments args)` calls `jobScheduleRepository.findAllEnabled()` and calls `quartzScheduleService.schedule(...)` for each, logging a summary count at `INFO` on completion; any single schedule failing to register is caught, logged at `WARN` with its `jobDefinitionId`, and does not prevent the remaining schedules from being reconciled.
  Tests: Insert 3 `JobSchedule` rows directly via `NamedParameterJdbcTemplate` (2 enabled, 1 disabled), invoke `run(...)`, assert exactly the 2 enabled `TriggerKey`s are now registered in a real test `Scheduler` and the disabled one is not; a schedule with a cron expression that is valid at construction time but fails Quartz registration for an unrelated reason (simulate by mocking `QuartzScheduleService` to throw for one id) does not prevent the other schedules from being reconciled.
  Dependencies: Task 1.2, Task 2.3.

### 3. API layer: schedule endpoints

- [x] **3.1 Add `JobScheduleRequest`/`JobScheduleResponse` and `JobScheduleController`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleRequest.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleResponse.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleController.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/api/JobScheduleControllerTest.java` (new)
  Changes: `JobScheduleRequest(@NotBlank String cronExpression, String timeZone, boolean enabled)` — `timeZone` is deliberately **not** `@NotBlank`: per the Operational Defaults table ("explicit IANA zone per job, `UTC` when unset"), the controller defaults a `null`/blank `timeZone` to `"UTC"` before constructing the `JobSchedule`, so the default lives at the API boundary, not silently inside the domain record. `JobScheduleResponse(UUID jobDefinitionId, String cronExpression, String timeZone, boolean enabled, Instant createdAt, Instant updatedAt, Instant nextFireTime)`. `JobScheduleController` under `/api/v1/jobs/{jobDefinitionId}/schedule`: `PUT` — verifies the job definition exists (`JobDefinitionRepository.findById(...).orElseThrow(NoSuchElementException::new)`, reusing the existing 404 mapping), resolves `timeZone` to `"UTC"` when null/blank, constructs and validates a `JobSchedule` (letting its compact constructor's `IllegalArgumentException` map to 400 via the existing `GlobalExceptionHandler`), persists via `JobScheduleRepository.upsert(...)`, calls `QuartzScheduleService.schedule(...)`, and returns the response with `nextFireTime` from `QuartzScheduleService.nextFireTime(...)` (`null` when disabled). `GET` — `JobScheduleRepository.findByJobDefinitionId(...).orElseThrow(NoSuchElementException::new)`, response includes current `nextFireTime`. `DELETE` — `JobScheduleRepository.delete(...)` then `QuartzScheduleService.unschedule(...)` regardless of whether a row existed (idempotent), returns `204`.
  Tests: `@SpringBootTest` + `MockMvc` against Testcontainers Postgres — `PUT` on an existing job definition with a valid cron/timezone returns `200` with a non-null `nextFireTime`; `PUT` with an invalid cron expression returns `400` problem+json; `PUT` with no `timeZone` field defaults to `UTC` (assert the response's `timeZone` field); `PUT` for an unknown `jobDefinitionId` returns `404`; `PUT` with `enabled: false` returns `200` with `nextFireTime: null` and no trigger registered (assert via `QuartzScheduleService.nextFireTime(...)` directly); a second `PUT` with a different cron expression updates the schedule (assert the new `nextFireTime` differs); `GET` returns `404` when no schedule exists yet, `200` after one is created; `DELETE` returns `204` and a subsequent `GET` returns `404`; `DELETE` on a job definition with no schedule also returns `204`.
  Dependencies: Task 1.2, Task 2.3.

### 4. Integration and reconciliation verification

- [x] **4.1 End-to-end IT: a fired schedule creates and completes a run**
  Files: `replicadb-server/src/test/java/org/replicadb/server/job/api/ScheduledJobLifecycleIT.java` (new)
  Changes: `@SpringBootTest(webEnvironment = RANDOM_PORT)` against Testcontainers Postgres, mirroring `JobLifecycleIT`'s structure: create a job definition via `POST /api/v1/jobs` against SQLite source/sink fixtures, `PUT` a schedule with a cron expression firing roughly every second (e.g. `"*/1 * * * * ?"`) and `timeZone: "UTC"`, then poll `GET /api/v1/jobs/{id}/runs` until at least one `JobRun` reaches `SUCCEEDED` within a bounded timeout. Also asserts that triggering a second manual run (`POST /api/v1/jobs/{id}/runs`) while the fixture is mid-replication is rejected with `409`, proving the schedule and the manual-trigger path share the same non-overlap guarantee.
  Tests: This task *is* the test — the file above is the deliverable and must pass.
  Dependencies: Task 3.1.

- [x] **4.2 Verify existing Phase 1a/1b/1c-1 tests remain unaffected**
  Files: none (verification only)
  Changes: No functional change expected — this task runs the full existing `replicadb-server` test suite (`mvn -f replicadb-server/pom.xml test`) to confirm the new Quartz dependency and beans do not break `HealthEndpointTest`, `ReplicaDbServerApplicationTest`, `CoreDependencyResolutionTest`, `CoreVersionAlignmentTest`, or any Phase 1c-1 controller/repository test.
  Tests: Full module test run must report zero failures.
  Dependencies: Task 4.1.

---

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `JobSchedule(UUID jobDefinitionId, String cronExpression, String timeZone, boolean enabled, Instant createdAt, Instant updatedAt)` — validated record
- `JobScheduleRepository.upsert(JobSchedule): JobSchedule`
- `JobScheduleRepository.findByJobDefinitionId(UUID): Optional<JobSchedule>`
- `JobScheduleRepository.findAllEnabled(): List<JobSchedule>`
- `JobScheduleRepository.delete(UUID): boolean`
- `ScheduledRunTriggerJob implements org.quartz.Job` — `@DisallowConcurrentExecution`, reads `jobDefinitionId` from `JobDataMap`
- `QuartzScheduleService.schedule(JobSchedule): void`
- `QuartzScheduleService.unschedule(UUID jobDefinitionId): void`
- `QuartzScheduleService.nextFireTime(UUID jobDefinitionId): Optional<Instant>`
- `ScheduleReconciler implements ApplicationRunner`
- `JobScheduleRequest(String cronExpression, String timeZone, boolean enabled)` / `JobScheduleResponse(UUID jobDefinitionId, String cronExpression, String timeZone, boolean enabled, Instant createdAt, Instant updatedAt, Instant nextFireTime)` — DTOs in `org.replicadb.server.job.api`

</details>

<details>
<summary>Dependencies</summary>

- New Maven dependency: `spring-boot-starter-quartz` (Task 2.1), excluding `spring-boot-starter-logging` like the existing starters in `replicadb-server/pom.xml`.
- No new dependency for cron/timezone validation: `org.quartz.CronExpression` and `java.time.ZoneId` are already transitively/JDK-available once the Quartz starter is added.

</details>

<details>
<summary>Testing Strategy</summary>

- Unit tests (no Spring context): `JobScheduleTest`, `ScheduledRunTriggerJobTest` (Mockito), `QuartzScheduleServiceTest` and `ScheduleReconcilerTest`'s Quartz-registration assertions (real `RAMJobStore` `Scheduler`, no Spring).
- Testcontainers PostgreSQL (`@ServiceConnection`, existing `PostgresTestcontainersConfig` pattern): `JobScheduleRepositoryIT`, `JobScheduleControllerTest`, `ScheduledJobLifecycleIT`.
- SQLite file fixtures (same technique as `JobExecutionServiceIT`/`JobLifecycleIT`) stand in as source/sink for the one test that needs a real fired-and-completed replication.
- Existing Phase 1a/1b/1c-1 tests must all continue to pass unmodified (Task 4.2) — this plan adds new files and one new Maven dependency but does not change any existing public method signature.
- CI: no changes expected to `CT_Push.yml`'s `server` job — it already runs Testcontainers-backed tests for this module with Docker configured.

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 7/9 (77.8%)
- Tasks that required plan adjustment: 2/9 (22.2%)
- Test loop iterations: 9 task validation loops (5 first-pass, 4 second-pass, 0 third-pass)

### Gaps Encountered

#### Gap 1: Quartz dependency ordering (Plan-to-Implementation)
- **Task**: 1.1 - Add `job_schedule` table and `JobSchedule` domain record
- **Plan assumed**: Task 1.1 had no dependencies, with the Quartz starter added later in task 2.1.
- **Reality**: `JobSchedule` validation directly uses `org.quartz.CronExpression`, so the domain task could not compile before the Quartz dependency was present.
- **Resolution**: Added `spring-boot-starter-quartz` in task 1.1 and completed its explicit scheduler configuration in task 2.1.
- **Learning**: Build dependency ordering must include libraries referenced by domain validation, not only runtime wiring tasks.

#### Gap 2: Large SQLite fixture setup time (Plan-to-Implementation)
- **Task**: 4.1 - End-to-end IT: a fired schedule creates and completes a run
- **Plan assumed**: The existing SQLite fixture technique would keep the scheduled overlap test within its bounded timeout.
- **Reality**: Auto-commit inserts for the 50,000-row fixture consumed almost the entire timeout before the first Quartz fire.
- **Resolution**: Wrapped fixture creation in one SQLite transaction; the same test then passed in 7.5 seconds.
- **Learning**: Large integration fixtures should use an explicit transaction so setup cost does not obscure scheduler timing.

### Patterns Discovered
- **Stable Quartz identities**: `QuartzScheduleService` uses one deterministic job and trigger key per job definition so reconciliation and API upserts safely converge.
- **Product-level schedule durability**: PostgreSQL `job_schedule` remains the source of truth while `ScheduleReconciler` repopulates the in-memory scheduler at startup.
- **Claim-and-submit scheduling**: `ScheduledRunTriggerJob` reuses the existing repository constraint and `RunExecutionCoordinator`, keeping Quartz threads out of replication work.
