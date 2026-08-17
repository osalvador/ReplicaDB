# Implementation Plan: Phase 1c-3c — Audit Events, Retention Purge, and Persisted Cancellation Warning

## Task Source

No JIRA ticket. Source is [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md) (v2.6), section **Phase 1c-3: Security — 1c-3a+b IMPLEMENTED; 1c-3c PENDING**:

> Remaining scope for the next plan targeting Phase 1c-3c:
> - Audit events for job/run/user changes, with the 365-day retention purge from the Operational Defaults table. `IdempotencyCleanupTask` (Phase 1c-1) only purges `run_trigger_idempotency`; it does not cover audit events.

Derived acceptance criteria, each traced back to the architecture document:

| ID | Criterion | Source |
|---|---|---|
| **AC1** | Login attempts, user changes, permission changes, job changes, and execution actions are auditable. | Decision 4, "Frontend, users, and permissions" |
| **AC2** | No password, connection secret, or credential-bearing string ever reaches the audit log. | Decision 4, "Credentials and secret references" |
| **AC3** | An `audit_event` table exists in the PostgreSQL metadata store. | Decision 2, "State storage"; the persistence-model list |
| **AC4** | Audit events are purged after 365 days by default, configurable. | Operational Defaults table |
| **AC5** | Administrators can read audit history. | Success Metrics, Phase 1 — "Administrators can manage users, roles, job permissions, and audit history" |
| **AC6** | The audit read endpoint follows `/api/v1` conventions: RFC 7807 errors, `page`/`size` with default 50 and maximum 200, session + CSRF auth, ADMIN-only. | Decision 4, "API conventions" |
| **AC7** | The cancellation warning is *persisted on the run*, not only returned in the HTTP response. | Decision 5; the open Priority 2 checkbox "Persist the indeterminate-sink warning on cancellation onto the `job_run` row" |

## Overview

Phase 1c-3a+b delivered authentication, global roles, and per-job ACLs, but nothing in `replicadb-server` records **who did what**. This plan adds a durable `audit_event` table, an explicit `AuditService` invoked at every state-changing endpoint and at run terminal outcomes, an ADMIN-only read API, and a daily retention purge. It also closes the one remaining Phase 1c-1 gap by persisting the cancellation warning onto the `job_run` row, which belongs in this slice because a cancelled run's indeterminate-sink warning is itself audit-grade information.

This is a `replicadb-server`-only change. The `replicadb` CLI artifact, `ToolOptions`, and every replication manager are untouched, preserving Decision 1's CLI compatibility contract.

## Architecture & Design

**Approach: A — Explicit `AuditService` calls at each audit point** (chosen by the user over Spring `ApplicationEvent`s and an AOP aspect).

Rationale: the set of audit points is small, finite, and fully enumerated below. The codebase consistently prefers explicit code over framework magic — `NamedParameterJdbcTemplate` instead of JPA, Spring `@Scheduled` instead of Quartz for the idempotency purge, and `JobAccessService.require(...)` called explicitly in each controller instead of an ACL aspect. An aspect would additionally have to reverse-engineer resource identifiers from method arguments and still could not cover the background executor path.

### New package layout

Mirrors the existing `org.replicadb.server.security` layout:

```text
org.replicadb.server.audit
├── AuditService.java              // the single write entry point
├── AuditActorResolver.java        // Authentication/HttpServletRequest -> AuditActor
├── domain
│   ├── AuditEvent.java            // record
│   ├── AuditAction.java           // enum
│   ├── AuditResourceType.java     // enum
│   ├── AuditOutcome.java          // enum
│   └── AuditActor.java            // record
├── persistence
│   ├── AuditEventRepository.java
│   └── AuditEventFilter.java      // record
├── api
│   ├── AuditEventController.java
│   └── AuditEventResponse.java
└── execution
    └── AuditRetentionTask.java
```

> **Naming hazard**: Spring Boot Actuator is on the `replicadb-server` classpath and ships `org.springframework.boot.actuate.audit.AuditEvent`. Every file must import `org.replicadb.server.audit.domain.AuditEvent` explicitly. Do not accept an IDE auto-import of the Actuator class.

### Audit points and their actors

| Integration point | File | Actions |
|---|---|---|
| Login / logout | `AuthController` | `LOGIN_SUCCEEDED`, `LOGIN_FAILED`, `LOGOUT` |
| User management | `UserController` | `USER_CREATED`, `USER_UPDATED`, `USER_PASSWORD_CHANGED` |
| Admin bootstrap | `AdminBootstrapRunner` | `USER_CREATED` (system actor) |
| Job definitions | `JobDefinitionController` | `JOB_CREATED`, `JOB_UPDATED` |
| Job ACLs | `JobPermissionController` | `JOB_PERMISSION_REPLACED`, `JOB_PERMISSION_REVOKED` |
| Schedules | `JobScheduleController` | `JOB_SCHEDULE_UPSERTED`, `JOB_SCHEDULE_DELETED` |
| Run actions | `JobRunController` | `RUN_TRIGGERED`, `RUN_CANCEL_REQUESTED`, `RUN_RETRIED` |
| Scheduled trigger | `ScheduledRunTriggerJob` | `RUN_TRIGGERED` (system actor `system:scheduler`) |
| Run terminal outcome | `JobExecutionService` | `RUN_SUCCEEDED`, `RUN_FAILED`, `RUN_CANCELLED` (system actor) |

**Actor on background threads — explicit design decision.** `JobExecutionService.executeClaimedRun(...)` runs on a `ReplicadbRun-N` pool thread created by `RunExecutionCoordinator`, and `ScheduledRunTriggerJob` runs on a Quartz thread. Neither has a populated `SecurityContextHolder`. Terminal-outcome events therefore record a **system actor** derived from `JobRun.executorIdentity()` (`system:api` or `system:scheduler`), never a human user. The human actor stays discoverable by correlating on `resourceId` = the run id: the `RUN_TRIGGERED` / `RUN_RETRIED` event carries the real user, and the `RUN_SUCCEEDED` / `RUN_FAILED` / `RUN_CANCELLED` event for the same run id carries the system actor. This is documented as an accepted limitation rather than smuggling a `SecurityContext` across a thread boundary.

**Auditing must never break the audited operation — explicit design decision.** `AuditService.record(...)` catches every `RuntimeException` from serialization or the insert, logs it at `ERROR` with the action and resource id, and returns normally. A failed audit insert must not abort a login, a job update, or the persistence of a run's terminal state. This is fail-open auditing; the accepted mitigation is the `ERROR` log line, recorded as a known limitation in the architecture document.

**Redaction (AC2).** `AuditService` passes every detail *value* through the core's existing `org.replicadb.config.CredentialRedactor.redactMessage(...)` and truncates each value to 1000 characters before serializing. Callers are additionally forbidden from putting a password field into the detail map at all — job-definition detail records non-secret fields only, mirroring how `JobDefinitionResponse` already avoids echoing passwords.

**Detail column.** `detail JSONB NOT NULL DEFAULT '{}'::jsonb`, serialized from a `Map<String, String>` with the Jackson `ObjectMapper` already provided by `spring-boot-starter-web`. Writes use `CAST(:detail AS jsonb)` — an explicit cast, not `:detail::jsonb`, to avoid ambiguity in `NamedParameterJdbcTemplate`'s placeholder parser. Reads use `resultSet.getString("detail")` and `ObjectMapper.readValue(...)` back into a `Map<String, String>`.

**Migrations.** The metadata schema is at `V9`. This plan adds `V10__create_audit_event.sql` and `V11__add_job_run_cancellation_warning.sql`, forward-only, bringing the validated migration count to **11**. `FlywayMigrationTest` currently asserts `9` twice and is edited in both task 1.1 and task 6.1.

**Security posture.** `GET /api/v1/audit` carries a class-level `@PreAuthorize("hasRole('ADMIN')")`, exactly like `UserController`. Audit history is deliberately *not* filtered by per-job ACL: it is an administrative access-review surface, and ADMIN already bypasses job ACLs everywhere else.

**Performance.** One extra `INSERT` per state-changing request. Four indexes cover the read API's filters (`occurred_at`, `actor_user_id`, `action`, `(resource_type, resource_id)`), each with `occurred_at DESC` as a trailing key so the default newest-first ordering is index-supported. Every filter parameter the endpoint accepts is therefore index-backed; no filter combination degrades to a full table scan on the leading column.

## Implementation Tasks

### 1. Foundation — schema and domain

- [x] **1.1 Add the `audit_event` Flyway migration**
  Files: `replicadb-server/src/main/resources/db/migration/V10__create_audit_event.sql` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`
  Changes: Create `audit_event` with `id UUID PRIMARY KEY`, `occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()`, `actor_user_id UUID REFERENCES app_user(id) ON DELETE SET NULL`, `actor_username VARCHAR(100) NOT NULL`, `source_address VARCHAR(45)`, `action VARCHAR(60) NOT NULL`, `resource_type VARCHAR(30) NOT NULL`, `resource_id VARCHAR(64)`, `outcome VARCHAR(10) NOT NULL CHECK (outcome IN ('SUCCESS','FAILURE'))`, `detail JSONB NOT NULL DEFAULT '{}'::jsonb`. `actor_user_id` is nullable because a failed login may name an unknown username and a system actor has no user row; `actor_username` is always populated (the attempted username, or `system:api`/`system:scheduler`). `VARCHAR(100)` matches `app_user.username` from `V7__create_app_user.sql`. Add `idx_audit_event_occurred_at (occurred_at DESC)`, `idx_audit_event_actor (actor_user_id, occurred_at DESC)`, `idx_audit_event_action (action, occurred_at DESC)`, and `idx_audit_event_resource (resource_type, resource_id, occurred_at DESC)` — one per filter parameter the task 4.1 endpoint accepts, so no supported filter degrades to a full table scan. In `FlywayMigrationTest`, change both `assertEquals(9, ...)` assertions to `10` (task 6.1 raises them to `11`).
  Tests: `FlywayMigrationTest.appliesAndValidatesMetadataMigrations` — asserts 10 migrations execute and 10 are applied against a raw `PostgreSQLContainer`, proving the new DDL is syntactically valid and Flyway-validatable before any Spring wiring exists.
  Dependencies: None

- [x] **1.2 Add the audit domain enums and records**
  Files: `replicadb-server/src/main/java/org/replicadb/server/audit/domain/AuditAction.java`, `AuditResourceType.java`, `AuditOutcome.java`, `AuditActor.java`, `AuditEvent.java` (all new)
  Changes:
  - `AuditAction` enum with exactly: `LOGIN_SUCCEEDED`, `LOGIN_FAILED`, `LOGOUT`, `USER_CREATED`, `USER_UPDATED`, `USER_PASSWORD_CHANGED`, `JOB_CREATED`, `JOB_UPDATED`, `JOB_PERMISSION_REPLACED`, `JOB_PERMISSION_REVOKED`, `JOB_SCHEDULE_UPSERTED`, `JOB_SCHEDULE_DELETED`, `RUN_TRIGGERED`, `RUN_CANCEL_REQUESTED`, `RUN_RETRIED`, `RUN_SUCCEEDED`, `RUN_FAILED`, `RUN_CANCELLED`.
  - `AuditResourceType` enum: `USER`, `JOB_DEFINITION`, `JOB_RUN`, `SESSION`.
  - `AuditOutcome` enum: `SUCCESS`, `FAILURE`.
  - `AuditActor(UUID userId, String username, String sourceAddress)` record; compact constructor rejects a null or blank `username` with `IllegalArgumentException` and truncates `sourceAddress` to 45 characters. Static factory `system(String identity)` returns `new AuditActor(null, "system:" + identity, null)`.
  - `AuditEvent(UUID id, Instant occurredAt, AuditActor actor, AuditAction action, AuditResourceType resourceType, String resourceId, AuditOutcome outcome, Map<String,String> detail)` record; compact constructor requires non-null `actor`/`action`/`resourceType`/`outcome`, normalizes a null `detail` to `Map.of()`, and defensively copies `detail` with `Map.copyOf(...)`.
  Tests: New `replicadb-server/src/test/java/org/replicadb/server/audit/domain/AuditActorTest.java` — accepts a valid actor; rejects a null username; rejects a blank username; truncates a 60-character source address to 45; `system("scheduler")` yields username `system:scheduler` with a null user id and null source address. New `replicadb-server/src/test/java/org/replicadb/server/audit/domain/AuditEventTest.java` — accepts a fully populated event; rejects a null `actor`, `action`, `resourceType`, and `outcome` individually; normalizes a null detail map to empty; mutating the caller's map after construction does not change the event's detail; asserts every `AuditAction` name is at most 60 characters and every `AuditResourceType` name at most 30, matching the `V10` column widths.
  Dependencies: None

- [x] **1.3 Add `AuditEventRepository` and `AuditEventFilter`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/audit/persistence/AuditEventRepository.java`, `AuditEventFilter.java` (both new)
  Changes:
  - `AuditEventFilter(UUID actorUserId, AuditAction action, AuditResourceType resourceType, String resourceId, Instant from, Instant to)` record; compact constructor throws `IllegalArgumentException` when both bounds are present and `from.isAfter(to)`. Static `empty()` returns an all-null filter.
  - `AuditEventRepository` — `@Repository`, constructor takes `NamedParameterJdbcTemplate` and `ObjectMapper`. Methods: `insert(AuditEvent)` returning the persisted event (generating the id and `occurredAt` when null, writing `detail` via `CAST(:detail AS jsonb)`); `findPage(AuditEventFilter, int page, int size)` ordered `occurred_at DESC, id DESC` with `LIMIT`/`OFFSET`; `count(AuditEventFilter)`; `deleteOlderThan(int retentionDays)` executing `DELETE FROM audit_event WHERE occurred_at < now() - (:days * interval '1 day')` and returning the deleted row count. Filter SQL is assembled with the same `StringBuilder` + `MapSqlParameterSource` pattern as `JobRunRepository.appendFilters(...)`; paging validation mirrors `JobRunRepository.validatePage(...)` (reject negative `page`, non-positive `size`). Jackson `JsonProcessingException` is wrapped in `IllegalStateException` — `AuditService` (task 2.1) is what prevents it from reaching a caller.
  Tests: New Testcontainers IT `replicadb-server/src/test/java/org/replicadb/server/audit/persistence/AuditEventRepositoryIT.java`, following the `JobPermissionRepositoryIT` pattern with `@ServiceConnection` via `PostgresTestcontainersConfig`: round-trips an event including a multi-entry `detail` map; inserts an event with a null `actorUserId` and null `resourceId` (the failed-login shape); filters independently by `actorUserId`, `action`, `resourceType`, `resourceId`, and a `from`/`to` window, asserting each excludes non-matching rows; paginates with `size=1` across two events and asserts newest-first ordering; `count(...)` agrees with the unpaginated result size; `AuditEventFilter` with `from` after `to` throws `IllegalArgumentException`; `findPage(filter, -1, 50)` and `findPage(filter, 0, 0)` each throw `IllegalArgumentException` (repository-level validation is stricter than `PageRequestParams`, which clamps rather than throws — the controller clamps first, so the repository only ever sees valid values, and these assertions pin that contract); `deleteOlderThan(365)` removes a row inserted at `now() - interval '400 days'`, retains one at `now() - interval '10 days'`, and returns `1`; inserting an event referencing a real `app_user` and then deleting that user leaves the audit row present with a null `actor_user_id` and its `actor_username` intact.
  Dependencies: Task 1.1, Task 1.2

### 2. Service layer

- [x] **2.1 Add `AuditService`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/audit/AuditService.java` (new)
  Changes: `@Service` with constructor injection of `AuditEventRepository`. Public `record(AuditActor actor, AuditAction action, AuditResourceType resourceType, String resourceId, AuditOutcome outcome, Map<String,String> detail)` plus a convenience overload without `detail`. It builds the `AuditEvent` with a fresh `UUID` and `Instant.now()`, passes every detail *value* through `CredentialRedactor.redactMessage(...)`, drops entries whose value becomes null or blank, truncates each remaining value to 1000 characters, and inserts. The entire body is wrapped in `try/catch (RuntimeException)` logging `LOG.error("Failed to record audit event {} for {} {}", action, resourceType, resourceId, exception)` and swallowing — an audit failure must never abort the audited operation. Use `org.apache.logging.log4j.LogManager`, matching `RunExecutionCoordinator`.
  Tests: New `replicadb-server/src/test/java/org/replicadb/server/audit/AuditServiceTest.java` (Mockito, no Spring context), capturing the argument passed to `AuditEventRepository.insert(...)`: the captured event carries the given action, resource type, resource id, and outcome plus a non-null id and `occurredAt`; a detail value containing a `password=secret` fragment is redacted before reaching the repository; a 2000-character detail value is truncated to exactly 1000; an entry whose value is null or blank is omitted from the persisted detail; a null detail map produces an empty map; when the repository stub throws `RuntimeException`, `record(...)` returns normally without propagating; the no-detail overload persists an empty detail map.
  Dependencies: Task 1.3

- [x] **2.2 Add `AuditActorResolver`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/audit/AuditActorResolver.java` (new)
  Changes: `@Component` exposing `resolve(Authentication authentication)`, which builds an `AuditActor` from a `ReplicaDbUserDetails` principal (user id + username) using the same principal-shape check as `JobAccessService.currentUserId(...)`. Unlike `JobAccessService` it must **never throw**: a null `Authentication` or an unexpected principal yields `new AuditActor(null, "anonymous", sourceAddress)`, so an audit call can never break a request. The source address is read from `RequestContextHolder.getRequestAttributes()` cast to `ServletRequestAttributes`, returning `null` when no request is bound to the thread (background executor and Quartz threads). Additional factories: `forAttemptedLogin(String username, String sourceAddress)` — falling back to `"unknown"` when the submitted username is null or blank, since `AuditActor` rejects a blank username — and `system(String executorIdentity)` delegating to `AuditActor.system(...)` with a `"api"` fallback for a null identity.
  Tests: New `replicadb-server/src/test/java/org/replicadb/server/audit/AuditActorResolverTest.java` (Mockito): resolves the user id and username from a `ReplicaDbUserDetails` principal; returns the `anonymous` actor for a null `Authentication`; returns the `anonymous` actor for an `Authentication` whose principal is a plain `String`; returns a null source address when no request is bound; populates the source address from a `MockHttpServletRequest` bound through `RequestContextHolder` (cleared in an `@AfterEach`); `forAttemptedLogin(null, "10.0.0.1")` yields username `unknown` without throwing; `system("scheduler")` yields `system:scheduler` and `system(null)` yields `system:api`.
  Dependencies: Task 1.2

### 3. Instrumentation — call `AuditService` at every audit point

> Every task in this phase asserts against the real `audit_event` table through `AuditEventRepository.findPage(...)` rather than a Mockito verification, so the test proves the row is actually persisted and correctly typed.

- [x] **3.1 Audit authentication**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/api/AuthController.java`
  Changes: Inject `AuditService` and `AuditActorResolver`. In `login(...)`: record `LOGIN_SUCCEEDED` / `SUCCESS` / `SESSION` (resource id = the authenticated username) after `loginAttemptService.recordSuccess(...)`; record `LOGIN_FAILED` / `FAILURE` inside the existing `catch (AuthenticationException)` block before rethrowing, using `AuditActorResolver.forAttemptedLogin(request.username(), httpRequest.getRemoteAddr())`; wrap `loginAttemptService.checkAllowed(...)` so a `TooManyAttemptsException` records `LOGIN_FAILED` / `FAILURE` with detail `{"reason": "THROTTLED"}` before rethrowing. In `logout(...)`: record `LOGOUT` / `SUCCESS` / `SESSION`, resolving the actor **before** `session.invalidate()` and `SecurityContextHolder.clearContext()` — `AuditActorResolver` never throws, so resolving after the context is cleared would silently produce an `anonymous` actor and lose the identity of every logout without any error. The submitted password must never appear in any detail map.
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/security/api/AuthControllerTest.java`: a successful login writes exactly one `LOGIN_SUCCEEDED` row with the correct username and a non-null source address; a wrong-password login writes exactly one `LOGIN_FAILED` row and still returns 401; a login for a non-existent username writes a `LOGIN_FAILED` row with a null `actorUserId`; a throttled login (the attempt after the configured failure limit) writes a `LOGIN_FAILED` row whose detail contains `reason=THROTTLED` and still returns 429; logout writes a `LOGOUT` row naming the previously authenticated user and **not** the `anonymous` actor, guarding the resolution-order requirement above; no persisted row's serialized detail contains the submitted password string.
  Dependencies: Task 2.1, Task 2.2

- [x] **3.2 Audit user management and admin bootstrap**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/api/UserController.java`, `replicadb-server/src/main/java/org/replicadb/server/security/execution/AdminBootstrapRunner.java`
  Changes: In `UserController`, inject `AuditService`/`AuditActorResolver` and record after each successful repository write, resource type `USER`: `USER_CREATED` (resource id = new user id, detail `{"username", "role"}`), `USER_UPDATED` (detail `{"role", "enabled"}`), `USER_PASSWORD_CHANGED` (no detail). Each controller method gains an `Authentication` parameter for actor resolution, matching how the job controllers already receive it. In `AdminBootstrapRunner`, record `USER_CREATED` with `AuditActorResolver.system("bootstrap")` after the first administrator is inserted. Neither may place a raw or encoded password in a detail map.
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/security/api/UserControllerTest.java`: creating a user writes one `USER_CREATED` row whose `resourceId` equals the created user's id and whose actor is the acting ADMIN; a role/enabled update writes one `USER_UPDATED` row containing the new role; a password change writes one `USER_PASSWORD_CHANGED` row whose detail contains neither the plaintext password nor the Argon2 hash; a duplicate-username create attempt (409) writes no `USER_CREATED` row. Extend `replicadb-server/src/test/java/org/replicadb/server/security/execution/AdminBootstrapRunnerTest.java`: bootstrapping records one `USER_CREATED` with actor username `system:bootstrap` (verified with a mocked `AuditService`, since this test has no Spring context or database); a run where an ADMIN already exists records nothing.
  Dependencies: Task 2.1, Task 2.2

- [x] **3.3 Audit job definition changes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java`
  Changes: Inject `AuditService`/`AuditActorResolver`. Record `JOB_CREATED` after `repository.insert(...)` and the ACL grant, and `JOB_UPDATED` after `repository.update(...)`, both `SUCCESS` / `JOB_DEFINITION` with the definition id as resource id. Detail carries only non-secret fields: `{"name", "mode", "jobs", "sourceTable", "sinkTable"}`, with `mode` serialized as the lower-case `ReplicationMode.getModeText()` used by the REST contract. Do **not** include `sourceConnect`/`sinkConnect`/`sourcePassword`/`sinkPassword` — a connection string can carry credentials, and `JobDefinitionResponse` already redacts it. Both endpoints already receive `Authentication`.
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java`: a successful create writes one `JOB_CREATED` row whose `resourceId` is the created job id and whose detail's `mode` is the lower-case mode text; a successful update writes one `JOB_UPDATED` row; a create rejected by validation (400) writes no audit row; an update rejected because the name changed (400) writes no audit row; a create carrying an `${env:VAR}` password produces a persisted detail map containing no key matching `(?i).*password.*` and no `${env:` fragment; **fail-open boundary check** — with the `AuditEventRepository` bean replaced by a `@MockBean` whose `insert(...)` throws `RuntimeException` (the real `AuditService` is left in place so its `catch` block is exercised through the full HTTP stack), a create still returns 201 with an unchanged response body and the job definition is still persisted, proving an audit failure cannot corrupt the HTTP contract at the request boundary. Task 2.1 proves the same at the service boundary; mocking `AuditService` itself would instead test a contract violation that the design forbids.
  Dependencies: Task 2.1, Task 2.2

- [x] **3.4 Audit job permission changes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionController.java`
  Changes: Inject `AuditService`/`AuditActorResolver`. Record `JOB_PERMISSION_REPLACED` as the last statement of `replace(...)` (resource type `JOB_DEFINITION`, resource id = job definition id, detail `{"targetUserId", "permissions"}` where `permissions` is the comma-joined requested permission names, or the literal `none` when the set is empty) and `JOB_PERMISSION_REVOKED` at the end of `delete(...)` (detail `{"targetUserId"}`). `replace(...)` is `@Transactional`, so the audit insert joins that transaction and commits with it; a rollback discards both, which is the desired coupling.
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/job/api/JobPermissionControllerTest.java`: replacing permissions writes one `JOB_PERMISSION_REPLACED` row naming the target user id and the granted permission names; replacing with an empty permission set writes one row whose detail's `permissions` is `none`; deleting permissions writes one `JOB_PERMISSION_REVOKED` row; a request rejected with 403 by `JobAccessService.require(...)` writes no audit row; a request naming an unknown target user (404) writes no audit row.
  Dependencies: Task 2.1, Task 2.2

- [x] **3.5 Audit schedule changes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleController.java`
  Changes: Inject `AuditService`/`AuditActorResolver`. Record `JOB_SCHEDULE_UPSERTED` after `quartzScheduleService.schedule(persisted)` returns, with detail `{"cronExpression", "timeZone", "enabled"}`; record `JOB_SCHEDULE_DELETED` after `quartzScheduleService.unschedule(...)`. Resource type `JOB_DEFINITION`, resource id = job definition id (the schedule is one-to-one with the definition and has no separate identifier).
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/job/api/JobScheduleControllerTest.java`: an upsert writes one `JOB_SCHEDULE_UPSERTED` row containing the cron expression and the resolved timezone; an upsert that omitted `timeZone` records `UTC`; a delete writes one `JOB_SCHEDULE_DELETED` row; a delete for a job with no schedule (the idempotent 204 path) still writes exactly one row; an upsert rejected for an invalid cron expression (400) writes no row.
  Dependencies: Task 2.1, Task 2.2

- [x] **3.6 Audit run actions triggered through the API**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`
  Changes: Inject `AuditService`/`AuditActorResolver`. With resource type `JOB_RUN`, record: `RUN_TRIGGERED` after `executionCoordinator.submit(...)` in `trigger(...)` (resource id = the new run id, detail `{"jobDefinitionId", "trigger": "manual"}`); `RUN_CANCEL_REQUESTED` on every path of `cancel(...)` that returns a `CancellationResponse` (detail `{"warning", "resultingStatus"}`); `RUN_RETRIED` after `executionCoordinator.submit(retry.id(), "api")` (resource id = the *new* run id, detail `{"previousRunId"}`). An idempotent replay in `trigger(...)` that returns the pre-existing run must **not** write a second `RUN_TRIGGERED` row — place the audit call after the early `return accepted(existingRun)`, on the fresh-run path only.
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java`: a manual trigger writes one `RUN_TRIGGERED` row whose `resourceId` is the returned run id; replaying the same `Idempotency-Key` returns the same run and leaves the `RUN_TRIGGERED` row count at one; cancelling a `PENDING` run writes one `RUN_CANCEL_REQUESTED` row whose detail's `warning` equals the response body's warning; a retry writes one `RUN_RETRIED` row whose detail's `previousRunId` is the failed run's id; a trigger rejected for a missing `Idempotency-Key` (400), a trigger rejected because a run is already active (409), and a cancel rejected by ACL (403) each write no audit row; **race path** — when a `RUNNING` run reaches a terminal state between the controller's `findRun(id)` read and the `markCancelRequested(...)` update (simulated by stubbing `RunExecutionCoordinator.requestCancellation` to return `true` while the row is transitioned to `CANCELLED` first), the endpoint still returns a `CancellationResponse` and still writes exactly one `RUN_CANCEL_REQUESTED` row, since `markCancelRequested(...)`'s tolerated no-op must not suppress the audit record.
  Dependencies: Task 2.1, Task 2.2

- [x] **3.7 Audit scheduled triggers and run terminal outcomes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduledRunTriggerJob.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`
  Changes:
  - `ScheduledRunTriggerJob`: add `@Autowired` `AuditService` and `AuditActorResolver` fields, matching the existing field-injection style that Quartz instantiation forces. Record `RUN_TRIGGERED` / `SUCCESS` / `JOB_RUN` with `AuditActorResolver.system("scheduler")`, resource id = the pending run id, detail `{"jobDefinitionId", "trigger": "schedule"}`, after `runExecutionCoordinator.submit(...)`. Both skip paths (`hasActiveRun`, the caught `IllegalStateException`) record nothing.
  - `JobExecutionService`: inject `AuditService`/`AuditActorResolver` and record the terminal outcome immediately after each `jobRunRepository.mark*` call in `executeClaimedRun(...)` — `RUN_SUCCEEDED` / `SUCCESS`, `RUN_FAILED` / `FAILURE`, `RUN_CANCELLED` / `SUCCESS` — with `AuditActorResolver.system(run.executorIdentity())`. Detail carries `{"rowsProcessed", "durationMillis"}` plus, on both failure paths, the **already redacted** `errorMessage` that is written to `job_run.error_message`. The `catch (Exception)` path records `RUN_FAILED` with that same redacted message. Keep `executeNextPending(...)`'s signature unchanged.
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduledRunTriggerJobTest.java` (Mockito, mocked `AuditService`): a fired trigger records one `RUN_TRIGGERED` with actor username `system:scheduler` and the pending run id; a trigger skipped because a run is already active records nothing; a trigger skipped by the `IllegalStateException` path records nothing. Extend `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java` (real database): a successful end-to-end SQLite run writes one `RUN_SUCCEEDED` row whose detail's `rowsProcessed` matches the persisted `job_run.rows_processed`; a failing run writes one `RUN_FAILED` row whose detail message equals the persisted `error_message` and contains no credential fragment; both rows' actor usernames start with `system:` and never equal a human username.
  Dependencies: Task 2.1, Task 2.2

### 4. Read API

- [x] **4.1 Add the ADMIN-only audit read endpoint**
  Files: `replicadb-server/src/main/java/org/replicadb/server/audit/api/AuditEventController.java`, `replicadb-server/src/main/java/org/replicadb/server/audit/api/AuditEventResponse.java` (both new)
  Changes:
  - `AuditEventResponse(UUID id, Instant occurredAt, UUID actorUserId, String actorUsername, String sourceAddress, AuditAction action, AuditResourceType resourceType, String resourceId, AuditOutcome outcome, Map<String,String> detail)` with a `from(AuditEvent)` factory that flattens the nested `AuditActor`.
  - `AuditEventController` at `@RequestMapping("/api/v1/audit")` with class-level `@PreAuthorize("hasRole('ADMIN')")`, mirroring `UserController`. One `@GetMapping` accepting optional `actorUserId`, `action`, `resourceType`, `resourceId`, `from`, `to`, `page`, `size`. `action` and `resourceType` are declared as `String` request params and parsed case-insensitively; an unknown value throws `new IllegalArgumentException("Unknown audit action: " + value)` / `new IllegalArgumentException("Unknown audit resource type: " + value)` → 400 via the existing `GlobalExceptionHandler`, matching the message shape `JobRunController.parseStatus(...)` already uses (`"Unknown run status: " + status`). String params are used rather than direct enum binding because Spring's default enum binding accepts only the exact upper-case constant and would surface a less useful error — the same DTO-boundary reasoning `JobDefinitionMapper` already applies to `mode`. `from`/`to` bind as `Instant` with `@DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)`. Paging uses `PageRequestParams.of(page, size)` and the method returns `PageResponse<AuditEventResponse>`, matching every other collection endpoint.
  - Add `/api/v1/audit` to nothing in `SecurityConfig` — it is already covered by the existing `anyRequest().authenticated()` rule, and the role check lives on the controller.
  Tests: New `replicadb-server/src/test/java/org/replicadb/server/audit/api/AuditEventControllerTest.java` (MockMvc + Testcontainers, following `UserControllerTest`): an ADMIN receives a paginated newest-first list; an OPERATOR receives 403; a VIEWER receives 403; an unauthenticated request receives 401 with an RFC 7807 body; each of `actorUserId`, `action`, `resourceType`, `resourceId`, and the `from`/`to` window narrows the result set; an unknown `action` value returns 400 `application/problem+json` whose `detail` is `Unknown audit action: <value>`; an unknown `resourceType` value returns 400 with the corresponding message; `size=500` is clamped to 200; `page=-1` returns 400; a `from` later than `to` returns 400.
  Dependencies: Task 1.3, Task 2.1

### 5. Retention

- [x] **5.1 Add the 365-day audit retention purge**
  Files: `replicadb-server/src/main/java/org/replicadb/server/audit/execution/AuditRetentionTask.java` (new), `replicadb-server/src/main/resources/application.yml`
  Changes: `@Component` modelled on `IdempotencyCleanupTask` — a `@Scheduled(cron = "0 30 3 * * *")` method (offset from the idempotency purge's `0 0 3 * * *` so the two do not contend) delegating to a package-visible `purgeExpired()` that calls `AuditEventRepository.deleteOlderThan(retentionDays)` and logs the deleted count at `INFO`. `retentionDays` is injected with `@Value("${replicadb.server.audit.retention-days:365}")` and the constructor rejects a non-positive value with `IllegalArgumentException`. Add `audit: {retention-days: 365}` under the existing `replicadb.server` block in `application.yml`, alongside `execution.pool-size`.
  Tests: New `replicadb-server/src/test/java/org/replicadb/server/audit/execution/AuditRetentionTaskTest.java` (Mockito, mirroring `IdempotencyCleanupTaskTest`): `purgeExpired()` delegates to the repository with the configured retention days and returns its count; the `@Scheduled` method invokes `purgeExpired()`; constructing with `0` and with `-1` each throws `IllegalArgumentException`. The real-SQL retention boundary is covered by the `deleteOlderThan(365)` assertion already specified in `AuditEventRepositoryIT` (task 1.3).
  Dependencies: Task 1.3

### 6. Persist the cancellation warning (closes the Phase 1c-1 gap, AC7)

- [x] **6.1 Add the `cancellation_warning` column and widen `JobRun`**
  Files: `replicadb-server/src/main/resources/db/migration/V11__add_job_run_cancellation_warning.sql` (new), `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRun.java`, `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunResponse.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobRunTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduledRunTriggerJobTest.java`
  Changes:
  - Migration: `ALTER TABLE job_run ADD COLUMN cancellation_warning TEXT;` — nullable, since existing rows have no warning.
  - `JobRun`: append a 16th component `String cancellationWarning`. This breaks the positional canonical constructor; update all five existing `new JobRun(...)` call sites — `JobRunRepository.insertPending` (passes `null`), `JobRunRepository.ROW_MAPPER`, two constructions in `JobRunTest`, and one in `ScheduledRunTriggerJobTest`.
  - `JobRunRepository`: add `cancellation_warning` to `SELECT_COLUMNS` and to `ROW_MAPPER`; change `markCancelRequested(UUID)` to `markCancelRequested(UUID, String cancellationWarning)` and `markPendingCancelled(UUID)` to `markPendingCancelled(UUID, String cancellationWarning)`, each setting the column in the same `UPDATE` so the status change and the warning are written atomically. `markCancelled(...)` is deliberately **not** changed, so the executor's later terminal transition cannot overwrite the warning.
  - `JobRunResponse`: append `String cancellationWarning` and map it in `from(...)`.
  - `FlywayMigrationTest`: raise both assertions from `10` to `11`.
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`: `markPendingCancelled(id, warning)` persists both the `CANCELLED` status and the warning text; `markCancelRequested(id, warning)` persists both on a `RUNNING` row; a subsequent `markCancelled(...)` leaves the previously stored warning intact; a run that was never cancelled has a null `cancellationWarning`; `markCancelRequested` against a row that already reached a terminal state (the existing tolerated no-op path) leaves the column untouched and does not throw. `FlywayMigrationTest` asserts 11 migrations. `JobRunTest` and `ScheduledRunTriggerJobTest` compile and pass with the 16-argument form.
  Dependencies: Task 1.1 (`V11` must sort after `V10`)

- [x] **6.2 Persist the warning from the cancel endpoint**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`
  Changes: Pass the already-computed `warning` string into `jobRunRepository.markPendingCancelled(id, warning)` and `jobRunRepository.markCancelRequested(id, warning)`. The `CancellationResponse` body is unchanged. Task 3.6's `RUN_CANCEL_REQUESTED` audit call and this persistence change touch the same method, so implement them consistently: the audit detail's `warning` value and the persisted column must be the same string.
  Tests: Extend `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java`: cancelling a `PENDING` run persists the mode-specific warning on the `job_run` row and returns the identical text in the response body; all three replication modes persist their respective warning text; `GET /api/v1/runs/{id}` afterwards exposes the same `cancellationWarning`; a cancel rejected with 409 because the run is in a non-cancellable state leaves the column null.
  Dependencies: Task 6.1, Task 3.6

### 7. Verification and documentation

- [x] **7.1 Full-module verification**
  Files: `replicadb-server/src/test/java/org/replicadb/server/security/SecurityJobLifecycleIT.java`; fix-ups land in files touched by tasks 1–6
  Changes: Run `mvn -q install -DskipTests -pl .` at the repository root (the server module builds against the installed CLI artifact) followed by `mvn -f replicadb-server/pom.xml test`. Confirm the security context tests still boot with `replicadb.security.bootstrap.enabled: false` from `replicadb-server/src/test/resources/application-api.yml`, and that the real-port lifecycle tests `JobLifecycleIT`, `ScheduledJobLifecycleIT`, and `SecurityJobLifecycleIT` still pass with the added audit writes. Requires Docker for Testcontainers, as the `server` CI job already provides.
  Tests: The full `replicadb-server` suite is the test for this task. Additionally add one assertion to `SecurityJobLifecycleIT`: after the existing real login → create job → trigger run flow, `GET /api/v1/audit` as the ADMIN (replaying the session cookie and CSRF header through the test's existing cookie-jar helper) returns rows for `LOGIN_SUCCEEDED`, `JOB_CREATED`, and `RUN_TRIGGERED` in one response — proving the write path and the read path agree end-to-end over real HTTP.
  Dependencies: Tasks 3.1–3.7, 4.1, 5.1, 6.2

- [x] **7.2 Update the architecture document**
  Files: `ARCHITECTURE_DECISIONS.md`
  Changes: Retitle the Phase 1c-3 heading to `1c-3a+b+c IMPLEMENTED`. Replace the "Remaining scope for the next plan targeting Phase 1c-3c" list with a delivered-scope section describing the `audit_event` table, the explicit `AuditService`, the ADMIN-only `GET /api/v1/audit` endpoint, `AuditRetentionTask`, and the known limitations (fail-open auditing; system actor on terminal run outcomes; no ACL filtering of audit history; no auditing of read operations). Tick the two open Priority 2 checkboxes: "Persist the indeterminate-sink warning on cancellation onto the `job_run` row" and "**Phase 1c-3c.** Add audit events and the 365-day audit retention purge". Mark the Success Metrics line "Administrators can manage users, roles, job permissions, and audit history" as met. Add `GET /api/v1/audit` to the "API surface" endpoint block. Correct the three places that currently state the cancellation warning "is not written back onto the `job_run` row" — Decision 5's closing paragraph, the Phase 1c-1 known-limitations paragraph, and the Phase 1c-2 known-limitations paragraph. Update the Deployment constraint that lists migrations "V1 through V6" to "V1 through V11". Add the new audit files and both migrations to the References list. Bump **Document Version** to 2.7, set **Last Updated** to the implementation date, and set **Next Review** to before Phase 1c-4.
  Tests: No automated test. Verification is a manual read-through confirming that no remaining sentence claims audit events or the persisted cancellation warning are pending, and that the migration-count and endpoint-table statements match the code.
  Dependencies: Task 7.1

## Technical Reference

<details>
<summary>Types &amp; Data Structures</summary>

```java
// org.replicadb.server.audit.domain
enum AuditAction {
    LOGIN_SUCCEEDED, LOGIN_FAILED, LOGOUT,
    USER_CREATED, USER_UPDATED, USER_PASSWORD_CHANGED,
    JOB_CREATED, JOB_UPDATED,
    JOB_PERMISSION_REPLACED, JOB_PERMISSION_REVOKED,
    JOB_SCHEDULE_UPSERTED, JOB_SCHEDULE_DELETED,
    RUN_TRIGGERED, RUN_CANCEL_REQUESTED, RUN_RETRIED,
    RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELLED
}
enum AuditResourceType { USER, JOB_DEFINITION, JOB_RUN, SESSION }
enum AuditOutcome { SUCCESS, FAILURE }

record AuditActor(UUID userId, String username, String sourceAddress) {
    static AuditActor system(String identity);
}
record AuditEvent(UUID id, Instant occurredAt, AuditActor actor, AuditAction action,
                  AuditResourceType resourceType, String resourceId,
                  AuditOutcome outcome, Map<String, String> detail) {}

// org.replicadb.server.audit.persistence
record AuditEventFilter(UUID actorUserId, AuditAction action, AuditResourceType resourceType,
                        String resourceId, Instant from, Instant to) {
    static AuditEventFilter empty();
}
class AuditEventRepository {
    AuditEvent insert(AuditEvent event);
    List<AuditEvent> findPage(AuditEventFilter filter, int page, int size);
    long count(AuditEventFilter filter);
    int deleteOlderThan(int retentionDays);
}

// org.replicadb.server.audit
class AuditService {
    void record(AuditActor actor, AuditAction action, AuditResourceType type,
                String resourceId, AuditOutcome outcome, Map<String, String> detail);
    void record(AuditActor actor, AuditAction action, AuditResourceType type,
                String resourceId, AuditOutcome outcome);
}
class AuditActorResolver {
    AuditActor resolve(Authentication authentication);
    AuditActor forAttemptedLogin(String username, String sourceAddress);
    AuditActor system(String executorIdentity);
}

// Modified in task 6.1 — a 16th component is appended
record JobRun(..., String errorMessage, String cancellationWarning) {}
```

</details>

<details>
<summary>Dependencies</summary>

- **No new Maven dependencies.** `ObjectMapper` comes from `spring-boot-starter-web`, `@Scheduled` from `spring-context`, `@PreAuthorize` from `spring-boot-starter-security` (added in Phase 1c-3a+b), and `CredentialRedactor` from the `org.replicadb:ReplicaDB` core artifact already on the server classpath.
- `MockHttpServletRequest` and `RequestContextHolder`, used by `AuditActorResolverTest`, come from `spring-test`, already available through `spring-boot-starter-test`.
- No change to `replicadb-server/pom.xml`, the root `pom.xml`, or `.github/workflows/CT_Push.yml` — the `server` CI job already provisions Docker for Testcontainers.

</details>

<details>
<summary>Testing Strategy</summary>

- **Pure unit (Mockito, no Spring context)**: `AuditActorTest`, `AuditEventTest`, `AuditServiceTest`, `AuditActorResolverTest`, `AuditRetentionTaskTest`, and the `ScheduledRunTriggerJobTest` / `AdminBootstrapRunnerTest` additions.
- **Testcontainers PostgreSQL** via `@ServiceConnection` and the existing `PostgresTestcontainersConfig`: `AuditEventRepositoryIT`, `AuditEventControllerTest`, and every extended controller test. `FlywayMigrationTest` continues to use a raw `PostgreSQLContainer` with no Spring wiring.
- **Assertion style for instrumentation tasks (3.1, 3.3–3.6)**: assert against the real `audit_event` table through `AuditEventRepository.findPage(...)`, not a Mockito verification, so the test proves the row is persisted and correctly typed. Tasks 3.2 (`AdminBootstrapRunnerTest`) and 3.7 (`ScheduledRunTriggerJobTest`) are the exceptions — those are context-free Mockito tests, so they verify the `AuditService` interaction instead; `JobExecutionServiceIT` covers the run-outcome path against the real table.
- **CSRF and authentication**: every mutating `MockMvc` call must use `SecurityMockMvcRequestPostProcessors.csrf()`; `@WithMockReplicaDbUser` supplies the `ReplicaDbUserDetails` principal that `AuditActorResolver` reads. `SecurityJobLifecycleIT` remains the only real-login, real-port flow and gains the task 7.1 assertion.
- **Redaction coverage (AC2)** is asserted in at least five places: `AuditServiceTest` (unit), task 3.1 (no submitted password in any row), task 3.2 (neither plaintext nor Argon2 hash), task 3.3 (no password-like key and no `${env:` fragment in a job-definition detail), and task 3.7 (no credential fragment in a failed run's message).
- **JUnit Jupiter 6 only** — no JUnit 4 imports, per `.github/instructions/test-patterns.instructions.md`.
- **Migration-count regression guard**: `FlywayMigrationTest` asserts the count twice and is edited in both task 1.1 (`9` → `10`) and task 6.1 (`10` → `11`). Running 1.1 and 6.1 out of order leaves that assertion wrong.

</details>

<details>
<summary>Known Limitations Accepted by This Plan</summary>

- **Fail-open auditing**: `AuditService` swallows insert failures and logs at `ERROR`. A metadata-database outage produces missing audit rows rather than failed requests. No metric or alert is added for this; that is a follow-up.
- **System actor on run outcomes**: `RUN_SUCCEEDED` / `RUN_FAILED` / `RUN_CANCELLED` record `system:api` or `system:scheduler`, not the human who triggered the run. The human is discoverable by correlating on the run id with the `RUN_TRIGGERED` / `RUN_RETRIED` event.
- **No ACL filtering on audit history**: the endpoint is ADMIN-only by design, so per-job visibility filtering is not applied.
- **No audit of read operations**: `GET` endpoints are not audited. Access review covers changes, not reads.
- **No audit of run-log or definition reads by non-admins**: same rationale as above.
- **Audit rows survive user deletion**: `actor_user_id` is set to `NULL` while `actor_username` is retained, preserving the historical record without a dangling foreign key.

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 16/18 (88.9%)
- Tasks that required plan adjustment: 2/18 (11.1%)
- Test loop iterations: 31 total (13 first-pass, 15 second-pass, 3 third-pass)

### Gaps Encountered

#### Gap 1: Empty permission replacement was blocked by DTO validation (Plan-to-Implementation)
- **Task**: 3.4 - Audit job permission changes
- **Plan assumed**: An empty permission set could reach `JobPermissionController.replace(...)` and produce an auditable `none` detail.
- **Reality**: `JobPermissionRequest` used `@NotEmpty`, and the controller's grouped response also assumed at least one permission.
- **Resolution**: Changed the request invariant to non-null, allowed an empty set as revoke-all, returned an explicit empty permission response, and added persisted `none` coverage.
- **Learning**: Validate planned endpoint examples against current DTO constraints and response assumptions before instrumenting the controller.

#### Gap 2: Failure and race coverage needed isolated Spring contexts (Plan-to-Implementation)
- **Tasks**: 3.3 and 3.6 - job-definition fail-open and cancellation race coverage
- **Plan assumed**: The existing integration test classes could replace one bean with `@MockBean` while continuing to assert against the real audit repository.
- **Reality**: Replacing `AuditEventRepository` removes the real persistence bean needed by the same class's positive-path assertions; the race also needs a mocked coordinator without changing normal execution tests.
- **Resolution**: Added two narrowly scoped Spring test contexts for the mocked repository and mocked coordinator, while retaining real-table assertions in the primary integration suites.
- **Learning**: Separate positive persistence tests from bean-replacement boundary tests when the mocked dependency is itself the assertion surface.

### Patterns Discovered
- Explicit audit calls after successful state transitions keep actor/resource/detail ownership visible and testable across controllers and background execution.
- The existing `StringBuilder` plus `MapSqlParameterSource` repository pattern extends cleanly to optional audit filters, JSONB detail mapping, and retention deletion.
