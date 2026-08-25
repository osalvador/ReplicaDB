# Implementation Plan: Phase 3.3 - API High Availability and Operational Hardening

## Task Source - JIRA: none - approved Phase 3.3 architecture decision

The source of truth is the approved Phase 3.3 section of [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md). Phase 3.1 and Phase 3.2 are treated as implemented and are not re-planned here. The user selected the **Full operational hardening** approach.

Acceptance criteria derived from the architecture decision:

- Replace the API profile's RAMJobStore with a PostgreSQL-backed Quartz JDBC clustered store. Two API instances sharing PostgreSQL must not create duplicate durable runs for one schedule firing.
- Add a forward-only Quartz schema migration and a rollout procedure that keeps one-release schema skew safe; do not use Quartz's automatic schema initializer.
- Make schedule reconciliation and schedule updates idempotent under concurrent API startup and concurrent changes, while keeping PostgreSQL `job_schedule` as the product-level schedule source of truth.
- Replace the process-local login-attempt map with a PostgreSQL-backed, multi-instance throttle enforcing five failed attempts per 15 minutes per account and per source address.
- Add cleanup for expired login-attempt state and prove the throttle across two independent API contexts.
- Add health/readiness signals for PostgreSQL, Quartz/API scheduling, worker listener/polling, eligible work, and worker executor capacity.
- Add Micrometer metrics for claims, notification latency, polling lag, lease renewals, expired leases, retries, stale updates, cancellations, and terminal outcomes, with bounded tags and no secret-bearing values.
- Keep the worker free of a public product HTTP listener, REST controllers, frontend, Spring Security session/authentication, and Quartz scheduler beans. Permit the minimal servlet context required by Spring Boot to expose Actuator only on a separately configurable internal management port.
- Add a server container image and a Compose topology for PostgreSQL, two API instances, and one or more workers with environment-managed configuration and separated public/management exposure.
- Add real multi-process validation for API clustering, worker loss during source copy and merge, PostgreSQL restart, notification loss/reconnect, duplicate polling, stale-worker fencing, and bounded reproducible load/chaos scenarios.
- Add CI and packaging gates for the managed server and distributed topology without changing the standalone CLI artifact, its exit codes, its options-file contract, or its no-PostgreSQL execution path.

## Overview

Phase 3.3 turns the Phase 3.2 distributed runtime into an operable deployment. PostgreSQL remains the only durable source of truth: Quartz uses it for clustered trigger ownership, login throttling is shared through it, and workers expose operational state without moving credentials or job configuration through a new channel.

The plan deliberately separates schema/configuration foundations, runtime observability, packaging, and real process-level failure tests. The CLI and the existing Phase 3.1/3.2 execution contracts remain compatibility surfaces throughout.

## Architecture & Design - Approach: Full operational hardening

### Runtime topology

```text
                         PostgreSQL
          metadata + Quartz tables + login throttle
                    ^                    ^
                    |                    |
          +---------+---------+  +-------+--------+
          | API 1 / API 2     |  | Worker 1..N    |
          | REST + Quartz     |  | no public API  |
          | sessions + auth  |  | listener       |
          | local execution  |  | polling        |
          +---------+---------+  | heartbeat/core |
                    |            +-------+--------+
                    |                    |
                    +-- UUID notify ----+
```

- Quartz uses a JDBC job store with `isClustered=true`, stable job/trigger keys, PostgreSQL locking, and `MISFIRE_INSTRUCTION_DO_NOTHING`. Quartz tables are created by Flyway, not by startup auto-initialization.
- `job_schedule` remains the product schedule source of truth. `ScheduleReconciler` converges that state into Quartz and tolerates concurrent registration. The active-run uniqueness constraint and dispatch transaction remain the final duplicate-run protection.
- Login throttling uses PostgreSQL time and a short transaction. A reservation row is created atomically for the account and source-address keys before authentication, so concurrent API instances cannot all pass the same last available slot. A successful authentication removes its reservation and clears prior failed rows; a failed authentication finalizes the reservation as a failure.
- The API keeps its existing public HTTP contract and exposes health on its existing actuator path. Metrics and detailed health are restricted to authenticated or network-internal management access. The worker uses a servlet-capable primary context only because Spring Boot 3.3.5 does not create a different-port management context for `web-application-type=none`; `server.port=-1` disables the primary listener, while a separate Actuator management context/port is the only HTTP surface permitted in that profile and is bound to an internal/configurable address.
- Worker readiness requires PostgreSQL access and a running polling/execution lifecycle. A disconnected listener is reported as degraded detail, not as a hard readiness failure, because polling is the correctness path. Liveness is process/runtime health, not queue emptiness.
- Prometheus metrics use bounded tags such as outcome, profile, and reason. They never use job ids, run ids, usernames, connection strings, lease tokens, or resolved credentials as tags or values.
- Process-level tests run outside the default unit/integration test discovery against built server images and an isolated Compose project. They are explicit, bounded, and use environment-managed credentials.

### Migration and rollout policy

V15 adds Quartz tables and V16 adds login-attempt state. Both are additive and ignored by Phase 3.2 workers. The deployment guide must apply migrations before enabling JDBC Quartz, keep `spring.quartz.jdbc.initialize-schema=never`, and define a controlled scheduler handoff: API instances using the old RAMJobStore must not run concurrently with JDBC-clustered schedulers for the same schedules. API read/auth traffic may be rolled independently, but scheduler ownership must be drained and switched as one operational step until every scheduler instance uses the same store.

### Performance and security

- Quartz JDBC check-in and lock operations use short transactions and a bounded scheduler pool; no replication runs inside a Quartz lock.
- Login throttle queries use indexed `(throttle_key, attempted_at)` access and a bounded cleanup job. Database failures fail closed for the pre-auth throttle decision rather than allowing a cluster-wide bypass.
- Worker management endpoints are not exposed on the public API port. The Compose and deployment examples bind them to an internal network/address, and the docs require network policy or an authenticated reverse proxy before remote access.
- Health details and metrics are sanitized; `/actuator/env`, beans, mappings, and configuration properties remain unexposed.
- No new dependency is added to the root CLI POM, no manager behavior changes, and no external broker or CDC capability is introduced.

## Implementation Tasks

### 1. Quartz JDBC persistence and clustered scheduler

- [x] **1.1 Add the forward-only Quartz PostgreSQL schema migration**
  Files: `replicadb-server/src/main/resources/db/migration/V15__create_quartz_jdbc_schema.sql`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/QuartzSchemaMigrationIT.java` (new)
  Changes: First resolve the effective Quartz version with `mvn -q -f replicadb-server/pom.xml dependency:tree -Dincludes=org.quartz:quartz`; Spring Boot 3.3.5 is expected to resolve Quartz 2.3.2, but the dependency tree is authoritative. Extract `org/quartz/impl/jdbcjobstore/tables_postgres.sql` from that exact jar with `jar tf`/`unzip -p`, then add the complete PostgreSQL Quartz JDBC schema, including scheduler, job, trigger, fired-trigger, calendar, lock, and paused-group tables plus the required primary keys/indexes. Keep table prefix `QRTZ_`, run it in the same configured metadata schema, use no destructive statements, and leave Quartz startup schema initialization disabled. Update migration-count and schema assertions from 14 to 15.
  Tests: Run Flyway against PostgreSQL Testcontainers and assert V15 applies after V14, all required Quartz tables/indexes exist in the isolated schema, a second migrate is idempotent, existing ReplicaDB tables remain intact, and an old-schema context can start before the new migration is applied. Verify no `DROP`, automatic initializer, credentials, or hard-coded external connection values are introduced.
  Dependencies: None

- [x] **1.2 Switch the API profile to Quartz JDBC clustering**
  Files: `replicadb-server/src/main/resources/application-api.yml`, `replicadb-server/src/main/resources/application.yml`, `replicadb-server/src/main/java/org/replicadb/server/config/ApiSchedulingConfiguration.java`, `replicadb-server/src/main/java/org/replicadb/server/config/QuartzClusterConfiguration.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/config/QuartzClusterConfigurationTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/HealthEndpointTest.java`
  Changes: Replace `spring.quartz.job-store-type: memory` with `jdbc`, set `spring.quartz.jdbc.initialize-schema: never`, and configure these exact resolved properties: `spring.quartz.properties.org.quartz.scheduler.instanceName=ReplicaDbScheduler`, `spring.quartz.properties.org.quartz.scheduler.instanceId=AUTO`, `spring.quartz.properties.org.quartz.threadPool.threadCount=2`, `spring.quartz.properties.org.quartz.jobStore.class=org.springframework.scheduling.quartz.LocalDataSourceJobStore`, `spring.quartz.properties.org.quartz.jobStore.driverDelegateClass=org.quartz.impl.jdbcjobstore.PostgreSQLDelegate`, `spring.quartz.properties.org.quartz.jobStore.tablePrefix=QRTZ_`, `spring.quartz.properties.org.quartz.jobStore.isClustered=true`, `spring.quartz.properties.org.quartz.jobStore.clusterCheckinInterval=15000`, and `spring.quartz.properties.org.quartz.jobStore.misfireThreshold=60000`. Set `replicadb.server.scheduler.clustered-required=true` by default and make `QuartzClusterConfiguration` fail startup when the API is accidentally configured with `RAMJobStore`, so the current release cannot silently join a mixed scheduler topology. Keep `@EnableScheduling` API-only and ensure the worker profile still excludes Quartz auto-configuration. Add an explicit configuration class only where Spring Boot defaults cannot express the shared DataSource/cluster settings; do not instantiate a second metadata DataSource. Preserve the existing actuator health behavior and update its test harness for any management-port property introduced here.
  Tests: Assert the API context uses a JDBC job store, clustering and PostgreSQL delegate properties, never attempts schema auto-initialization, and starts with two distinct scheduler instance identities against one PostgreSQL database. Assert a deliberately misconfigured `RAMJobStore` API fails the clustered-required guard before schedule reconciliation; the worker context contains no `Scheduler` or Quartz configuration and existing health/security tests remain valid.
  Dependencies: Task 1.1

- [x] **1.3 Make Quartz schedule registration idempotent under concurrent reconciliation**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/QuartzScheduleService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduleReconciler.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/QuartzScheduleServiceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduleReconcilerTest.java`
  Changes: Keep stable per-job `JobKey`/`TriggerKey` values and `DO_NOTHING` misfire behavior, but replace the unguarded `checkExists`/create assumption with a bounded convergence sequence: use the existing-key fast path, call `scheduler.scheduleJob(jobDetail, Set.of(trigger), false)` for creation, catch `ObjectAlreadyExistsException`, and call `scheduler.rescheduleJob(triggerKey, trigger)`; if the trigger disappeared between those operations, retry the sequence once and surface a scheduler failure if it still cannot converge. Preserve schedule updates and disabled-schedule deletion, and avoid logging dynamic configuration or credentials. Keep `job_schedule` persistence and Quartz registration separate so no database lock is held while a replication executes.
  Tests: With an in-memory scheduler, retain timezone/misfire/update/delete coverage and add concurrent registration from two threads. With two JDBC schedulers sharing PostgreSQL, register the same definition concurrently and assert one durable Quartz job and one trigger with the final cron/timezone. Verify a transient registration failure for one schedule does not prevent reconciliation of the others.
  Dependencies: Task 1.2

- [x] **1.4 Prove clustered schedule firing creates one durable run**
  Files: `replicadb-server/src/test/java/org/replicadb/server/job/execution/QuartzClusterIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/api/ScheduledJobLifecycleIT.java`, `replicadb-server/src/test/java/org/replicadb/server/config/PostgresTestcontainersConfig.java`
  Changes: Build a class-scoped PostgreSQL fixture with an isolated schema and start two independent API scheduler contexts against it. Seed one product-level schedule, disable API-local execution for the distributed assertion, let both contexts reconcile and fire the same short-interval cron trigger, then stop the trigger after a bounded observation window. Query PostgreSQL rather than either scheduler's local state and preserve the existing ACL, dispatch, and active-run uniqueness behavior. Include a negative handoff fixture that starts one current-code scheduler with `RAMJobStore` and one with JDBC for the same schedule; it must detect the resulting duplicate-fire risk and fail the validation when the handoff is attempted, documenting that mixed scheduler modes are prohibited rather than silently claiming they are safe.
  Tests: Assert both JDBC scheduler instances check in, exactly one `JobRun` is created for each observed fire, no duplicate pending/running row is produced during concurrent startup, schedule changes converge to one trigger, and a scheduler restart resumes the persisted schedule without replaying a missed fire. Assert the mixed RAM/JDBC fixture is rejected by the validation harness before it can be used as a deployment topology. Use explicit waits for container readiness and never share the default test schema.
  Dependencies: Tasks 1.1, 1.2, and 1.3

### 2. Shared PostgreSQL login throttling

- [x] **2.1 Add durable login-attempt reservations and the V16 migration**
  Files: `replicadb-server/src/main/resources/db/migration/V16__create_login_attempt.sql`, `replicadb-server/src/main/java/org/replicadb/server/security/auth/LoginAttemptReservation.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/persistence/LoginAttemptRepository.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`, `replicadb-server/src/test/java/org/replicadb/server/security/persistence/LoginAttemptRepositoryIT.java` (new)
  Changes: Add a forward-only `login_attempt` table with a UUID reservation id, throttle key, status (`PENDING`/`FAILED`), PostgreSQL `timestamptz` attempt time, constraints, and an index on `(throttle_key, attempted_at)`. Implement repository reservation operations in one short transaction using `SELECT pg_advisory_xact_lock(hashtextextended(:throttleKey, 0))` for both account/address keys in sorted key order, avoiding deadlocks while serializing competing reservations (hash collisions may conservatively serialize unrelated keys). In that transaction, purge rows outside the 15-minute window, count pending plus failed reservations, and insert two rows only when both keys are below five. Add token-aware finalization for failure/success and bounded old-row deletion. Use PostgreSQL time for the decision and keep repository errors free of usernames beyond bounded audit context.
  Tests: Apply all 16 migrations and assert the table/constraints/indexes. Test reservation success, fifth-allowed/sixth-blocked boundaries, account-only and address-only blocking, expired-window cleanup, duplicate finalization, concurrent reservations on the same key, and fail-closed behavior when the database operation is unavailable.
  Dependencies: None (Flyway version ordering applies V16 after V15; no code dependency on Quartz)

- [x] **2.2 Replace the in-memory service and wire authentication cleanup**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/auth/LoginAttemptService.java`, `replicadb-server/src/main/java/org/replicadb/server/security/api/AuthController.java`, `replicadb-server/src/main/java/org/replicadb/server/security/execution/LoginAttemptCleanupTask.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/auth/LoginAttemptServiceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/security/api/AuthControllerTest.java`
  Changes: Remove the `ConcurrentHashMap` and `Clock`-owned production state. Change `checkAllowed` to obtain a reservation, finalize it as `FAILED` after an authentication exception, and finalize it as successful after authentication; preserve the existing 5/15-minute account and source-address policy, audit events, exception mapping, and session flow. Fail closed when the shared store cannot make a pre-auth decision. Add a profile-scoped cleanup task that deletes stale pending/failed rows on a bounded schedule and does not use Quartz. Never log passwords, connection values, reservation tokens, or raw authentication payloads.
  Tests: Unit-test reservation lifecycle, blocked requests, success reset, failed-auth recording, store outage, cleanup scheduling, and audit behavior. Assert the controller never calls `AuthenticationManager` after a blocked decision and that existing login/logout/CSRF tests remain unchanged apart from the new repository seam.
  Dependencies: Task 2.1

- [x] **2.3 Prove throttle consistency across API instances**
  Files: `replicadb-server/src/test/java/org/replicadb/server/security/SharedLoginThrottleIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/RealHttpAuthenticationIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/config/PostgresTestcontainersConfig.java`, `replicadb-server/src/main/resources/application-api.yml`
  Changes: Start two independent API/security contexts sharing one isolated PostgreSQL schema and exercise the real login boundary through separate clients/source addresses. Keep bootstrap credentials environment-managed and use a test-only user fixture. Add only the properties needed to run cleanup and two API instances against the same state store.
  Tests: Distribute five failed attempts across both API instances and assert the sixth attempt is blocked regardless of instance; repeat with the same account from different addresses and the same address against different accounts; assert a successful login clears prior failures for the relevant keys; assert a 15-minute-expired row no longer blocks; and run concurrent failed attempts to prove the reservation transaction cannot exceed the configured boundary. Verify no password or session secret appears in logs or database detail.
  Dependencies: Task 2.2

### 3. Health, readiness, and metrics

- [x] **3.1 Add Micrometer registry and profile-specific management configuration**
  Files: `replicadb-server/pom.xml`, `replicadb-server/src/main/resources/application.yml`, `replicadb-server/src/main/resources/application-api.yml`, `replicadb-server/src/main/resources/application-worker.yml`, `replicadb-server/src/test/java/org/replicadb/server/ManagementConfigurationTest.java` (new)
  Changes: Add the Prometheus Micrometer registry only to `replicadb-server`. Expose `health`, `metrics`, and `prometheus` through the API management contract while keeping environment/configuration endpoints closed. Enable liveness/readiness probes and configure health groups. Preserve unauthenticated API health compatibility; protect detailed metrics/health on the API through the existing authenticated/internal management boundary. Configure a separate worker management port/address with environment overrides, defaulting to an internal port, set `server.port=-1` to disable the primary worker listener, and exclude REST, frontend, Security/session, and Quartz auto-configuration. The first implementation step is a minimal worker management-port probe using Spring Boot's management child context with `management.server.port=0`; do not begin indicator work until this probe proves that the resolved Boot 3.3.5 runtime can start Actuator without a public product listener.
  Tests: Resolve the effective Spring Boot dependency versions and assert the management properties match the APIs actually available in that version. The first test must start a worker with `server.port=-1` and a random management port, assert that only the management listener accepts HTTP, and fail before later observability tasks if that boundary is not supported. Then verify the API exposes only the intended actuator paths, `/actuator/env` remains unavailable, and no registry dependency is added to the root CLI POM.
  Dependencies: Tasks 1.2 and 2.2

- [x] **3.2 Implement control-plane and worker health indicators**
  Files: `replicadb-server/src/main/java/org/replicadb/server/observability/ControlPlaneHealthIndicator.java` (new), `replicadb-server/src/main/java/org/replicadb/server/observability/QuartzHealthIndicator.java` (new), `replicadb-server/src/main/java/org/replicadb/server/observability/WorkerRuntimeHealthIndicator.java` (new), `replicadb-server/src/main/java/org/replicadb/server/observability/RunQueueHealthIndicator.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListener.java`, `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PollingFallback.java`, `replicadb-server/src/test/java/org/replicadb/server/observability/HealthIndicatorTest.java` (new)
  Changes: Add sanitized indicators for PostgreSQL state-store reachability, Quartz scheduler state, eligible queue age/count, worker listener connectivity, polling lifecycle, and executor capacity. This task owns only the read-only status snapshot methods and health indicator classes in the listener, polling, coordinator, and `JobRunStore`; it must not add meter calls. Define readiness as PostgreSQL plus an active polling/execution lifecycle; report listener loss as degraded detail because polling remains the recovery path. Keep liveness independent of queue emptiness and avoid expensive full-table scans on probe requests. Do not expose run ids, job ids, lease tokens, usernames, or connection details.
  Tests: Unit-test each indicator's UP/DOWN/DEGRADED mapping, database errors, listener reconnect state, stopped polling, zero/full executor capacity, and bounded queue reads. Assert health details contain only fixed field names and numeric/boolean state, never dynamic credentials or opaque identifiers.
  Dependencies: Task 3.1

- [x] **3.3 Instrument managed dispatch, leases, polling, and terminal outcomes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/observability/ManagedRuntimeMetrics.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/application/RunLeaseService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunDispatchService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunFinalizationService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunCancellationService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PollingFallback.java`, `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListener.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/test/java/org/replicadb/server/observability/ManagedRuntimeMetricsTest.java` (new)
  Changes: This task owns only meter registration and metric calls in the classes already extended by Task 3.2; it must not change their health-state or lifecycle semantics. Centralize meter names and bounded tag values. Record claim outcomes, notification-to-claim latency, polling lag, lease renewal outcomes, expired-lease recoveries, manual/automatic retries, fenced/stale updates, cancellation requests/completions, and terminal outcomes. Add gauges for active/free worker slots and listener/polling state. Propagate only an internal receive timestamp for notification-latency measurement; never attach a run id or job id as a tag. Keep metrics side effects non-blocking and ensure a metrics failure cannot change state transitions.
  Tests: Use a `SimpleMeterRegistry` to assert each required counter/timer/gauge changes on success, failure, fencing, cancellation, retry, notification, polling, and recovery paths; assert bounded tag sets, no duplicate registration, no high-cardinality identifiers, and no secret values in meter names/tags.
  Dependencies: Tasks 3.1 and 3.2

- [x] **3.4 Verify actuator exposure, readiness semantics, and scrape safety**
  Files: `replicadb-server/src/test/java/org/replicadb/server/HealthEndpointTest.java`, `replicadb-server/src/test/java/org/replicadb/server/observability/MetricsEndpointIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/WorkerManagementEndpointIT.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java`, `replicadb-server/src/main/resources/application.yml`
  Changes: Add the endpoint/security wiring needed to keep public API health compatible while preventing unauthenticated access to detailed API actuator data. Verify worker management access is limited by its configured internal listener/network boundary and does not inherit a public API/session surface. Keep `/actuator/env`, `/actuator/beans`, `/actuator/mappings`, and raw exception details closed.
  Tests: Through a real HTTP client, assert API health/liveness/readiness responses, authenticated/unauthenticated metrics behavior, Prometheus content type and metric names, worker management health/metrics on its management port, refusal of the main worker port, and no leakage of DSNs, passwords, lease tokens, or usernames. Exercise listener-down/polling-up readiness and PostgreSQL-down failure cases.
  Dependencies: Tasks 3.2 and 3.3

### 4. Packaging, topology, and documentation

- [x] **4.1 Add a non-root managed-server container image**
  Files: `replicadb-server/Dockerfile` (new), `replicadb-server/.dockerignore` (new), `scripts/phase3-image-smoke.sh` (new), `replicadb-server/pom.xml`
  Changes: Package the already-built `replicadb-server` Spring Boot jar on Java 17, run as a non-root user, accept `SPRING_PROFILES_ACTIVE`/database/management settings from the environment, expose only documented application and management ports, and avoid baking credentials or configuration files into the image. Keep the existing root `Dockerfile` and `Containerfile` CLI images unchanged. Document the image entrypoint so `api` and `worker` are selected by deployment configuration rather than separate artifacts.
  Tests: Run `scripts/phase3-image-smoke.sh` to build the image with the server module as context, inspect the image user/entrypoint/layers, smoke-start both `api` and `worker` profiles with environment-managed metadata settings, assert the worker has no main HTTP listener, and verify the root CLI image build and artifact dependency graph remain unchanged.
  Dependencies: Tasks 3.1 and 3.4

- [x] **4.2 Add a Compose deployment topology and local smoke harness**
  Files: `docker-compose.server.yml` (new), `replicadb-server/src/test/resources/phase3/fixture.sql` (new), `scripts/phase3-compose-smoke.sh` (new), `.gitignore`
  Changes: Define PostgreSQL, two API services, and at least one worker using the server image. Wire persistent metadata, health-based startup dependencies, isolated API/worker networks, internal management ports, `local-execution.enabled=false` for distributed API instances, unique worker identity, lease/polling settings, and environment-managed database/bootstrap values. Seed only non-secret source/sink fixtures through SQL and make the smoke script create an isolated Compose project/schema, wait for health, trigger one job, and clean up on exit. Add only the generated local-state patterns `.phase3-compose/`, `.phase3-logs/`, and `.phase3-*.env` to `.gitignore`; never ignore or commit a real credential file as a substitute for environment management.
  Tests: Run `docker compose config` with placeholder environment variables, start the topology on dynamically selected host ports, assert both APIs share jobs/schedules and the worker claims a run, assert management endpoints are reachable only on the internal mapping, verify no credentials are printed by the script, and run the script twice without stale-volume or stale-network interference.
  Dependencies: Tasks 4.1 and 3.4

- [x] **4.3 Document deployment, upgrade, sizing, and failure operations**
  Files: `DEPLOYMENT.md` (new), `README.md`, `replicadb-server/frontend/README.develop.md`, `ARCHITECTURE_DECISIONS.md`, `scripts/check-phase3-docs.sh` (new)
  Changes: Document API versus worker startup, V15/V16 migration order, the RAMJobStore-to-JDBC scheduler handoff, `instanceId`/cluster settings, login throttle behavior and retention, management-port/network policy, readiness interpretation when a listener is down, datasource pool headroom, worker identity, listener reconnect, heartbeat/lease defaults, concurrency formula, PostgreSQL backup/restart behavior, rolling shutdown, and worker-loss recovery. Update stale statements that call the server unauthenticated/metadata-free or describe Phase 3.3 as pending only after implementation and validation pass. Keep all credentials, DSNs, tokens, and real endpoints environment-managed. Add `check-phase3-docs.sh` with required-heading/property assertions and forbidden-stale-wording/secret-pattern checks so the documentation gate is executable.
  Tests: Run `scripts/check-phase3-docs.sh` in a clean checkout and with an intentionally stale temporary copy to prove it fails; validate every documented property against the profile YAML and image/Compose files; review the upgrade sequence against V15/V16 forward-only migrations and confirm the CLI deployment section is unchanged. The script must not print credential values.
  Dependencies: Tasks 1.4, 2.3, and 3.4 through 4.2

### 5. Multi-process, failure, load, and chaos validation

- [x] **5.1 Add an explicit process-isolated validation profile**
  Files: `replicadb-server/pom.xml`, `scripts/phase3-multinode-test.sh` (new), `replicadb-server/src/test/resources/phase3/fixture.sql`, `docker-compose.server.yml`
  Changes: Add a named Maven profile with id `phase3-multinode` and bind `org.codehaus.mojo:exec-maven-plugin` to the `verify` phase so it runs `../scripts/phase3-multinode-test.sh` after the server jar is packaged; the script builds the image and launches the Compose topology as separate API/worker processes. Inject only ephemeral environment-managed credentials and collect bounded logs/artifacts on failure. Keep these tests out of default Surefire discovery so ordinary server tests remain fast, but make the named profile fail deterministically on health timeout, SQL assertion, duplicate run, stale state, or cleanup failure. Use dynamic ports/project names and an isolated PostgreSQL schema or volume per invocation. Task 6.1 owns changes to `.github/workflows/CT_Push.yml`; this task owns only the reusable profile and harness.
  Tests: Run the harness locally with Docker and in CI; assert two API processes, two worker processes when requested, one shared PostgreSQL state store, clean startup/shutdown, no leaked containers/networks/volumes, and no secret-bearing output. Verify the standard server Maven suite remains independently runnable.
  Dependencies: Tasks 4.1 and 4.2

- [x] **5.2 Validate worker loss during copy and merge recovery**
  Files: `scripts/phase3-worker-loss-test.sh` (new), `docker-compose.server.yml`, `replicadb-server/src/test/resources/phase3/fixture.sql`, `DEPLOYMENT.md`
  Changes: Extend the process harness with deterministic PostgreSQL fixtures and observable barriers rather than fixed sleeps. For copy loss, use a PostgreSQL source query that calls `pg_sleep` per selected row and poll the run until it is `RUNNING` before killing the owner. For merge loss, hold the sink table lock from a separate `psql` transaction, poll `pg_stat_activity` until the worker's merge/atomic-swap statement is demonstrably waiting on that lock, then kill the owner. Release the lock only after the old process is gone, wait for the configured lease expiry, restart a worker, and query PostgreSQL state for the preserved abandoned attempt and a new attempt from the beginning. Keep complete-mode destructive warnings visible and use incremental/complete-atomic fixtures for safe automatic retry assertions.
  Tests: For copy loss and merge loss, assert the old run is never reopened, `previousRunId`/attempt/backoff are correct, the replacement is claimed once, the old lease token cannot finalize or advance a watermark, a healthy worker heartbeat prevents recovery during a long merge, and an exhausted policy ends in `FAILED` without a replacement. Confirm the sink/watermark result matches the mode-specific safety contract.
  Dependencies: Task 5.1

- [x] **5.3 Validate PostgreSQL restart, notification loss, reconnect, and duplicate polling**
  Files: `scripts/phase3-resilience-test.sh` (new), `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListenerIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PollingFallbackIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/DistributedWorkerLifecycleIT.java`
  Changes: Extend the process and existing Testcontainers suites to stop/restart PostgreSQL, interrupt listener delivery, restart or reconnect workers, and run two workers against the same pending/retryable queue. Keep notification payloads UUID-only and use polling as the mandatory recovery path rather than asserting delivery guarantees from `LISTEN/NOTIFY`.
  Tests: Assert API and workers reconnect after PostgreSQL restart, no healthy run is duplicated, a run dispatched while a worker/listener is unavailable is found by startup or periodic polling, reconnect triggers a scan, duplicate notifications and simultaneous polling yield one claim, remote cancellation still reaches the owner or polling fallback, and stale finalization remains fenced. Assert health/metrics show the outage and recovery without exposing credentials.
  Dependencies: Tasks 3.2, 3.3, and 5.1

- [x] **5.4 Run reproducible multi-node load and chaos checks**
  Files: `scripts/phase3-load-test.sh` (new), `scripts/phase3-chaos-test.sh` (new), `scripts/phase3-worker-loss-test.sh`, `scripts/phase3-resilience-test.sh`, `scripts/phase3-multinode-test.sh`, `DEPLOYMENT.md`
  Changes: Add bounded, parameterized scenarios for N concurrent manual/scheduled dispatches across two APIs and one or more workers, duplicate idempotency keys, worker/API termination, notification loss, and metadata restart. Record only aggregate counts, durations, notification latency, polling lag, lease renewals, retries, cancellations, stale updates, and terminal outcomes. Define pass/fail thresholds and a repeatable seed; do not make wall-clock-sensitive tests depend on unbounded sleeps. Leave workflow wiring to Task 6.1 so this task remains reusable locally and by CI.
  Tests: Assert every accepted run has exactly one durable terminal outcome or an explicitly expected retry chain, no committed watermark advances twice, no duplicate schedule firing survives the database constraints, no stale worker mutates state, bounded executor/pool limits hold, and the required Prometheus meters are nonzero and bounded. Run a short PR smoke variant and a larger workflow-dispatch/nightly variant with diagnostic artifacts.
  Dependencies: Tasks 5.2 and 5.3

### 6. CI, release gates, and compatibility verification

- [x] **6.1 Add release gates for the managed artifact and distributed topology**
  Files: `.github/workflows/CT_Push.yml`, `.github/workflows/CI_Release.yml`, `replicadb-server/pom.xml`, `replicadb-server/Dockerfile`, `docker-compose.server.yml`, `scripts/phase3-compose-smoke.sh`, `scripts/phase3-multinode-test.sh`, `pom.xml`
  Changes: Add a server-image build/smoke step and a bounded `multi_node` job with Docker/Testcontainers configuration, while preserving the existing integration/non-integration/server/frontend jobs. Ensure the server job applies all 16 Flyway migrations, runs focused Quartz/throttle/health/metrics tests, verifies the deprecated Phase 3.1 bridge guard, and packages static assets. Keep release packaging of the CLI archive separate from the server image/jar and do not add Spring Boot to the root artifact.
  Tests: Run the exact CI commands locally where possible: server `mvn -B test`, focused migration/Quartz/security/observability tests, `mvn -B verify -Pphase3-multinode` or the documented script, `docker compose config`, image smoke checks for both profiles, frontend typecheck/build/E2E, root CLI tests, `mvn dependency:tree`/jar inspection for no Spring Boot in the CLI, a CLI no-PostgreSQL smoke invocation, and `git diff --check`. Fail CI on missing checks, stale generated artifacts, secret-bearing output, leaked management endpoints, or any duplicate/missed durable run assertion.
  Dependencies: Tasks 1.1 through 5.4

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `login_attempt`: two indexed rows per reservation, one for the account key and one for the source-address key; `PENDING` reservations count conservatively while authentication is in flight and become `FAILED` only after an authentication failure.
- `LoginAttemptReservation`: internal UUID plus the two throttle keys; never serialized, logged, or returned from an API.
- Quartz tables: the exact schema required by the resolved Quartz version, installed by Flyway V15 in the configured PostgreSQL metadata schema with `QRTZ_` prefix.
- `ControlPlaneHealthIndicator` and `QuartzHealthIndicator`: API state-store/scheduler status without dynamic identifiers.
- `WorkerRuntimeHealthIndicator`: listener, polling, executor-capacity, and queue state; listener loss is degraded while polling remains ready.
- `ManagedRuntimeMetrics`: centralized bounded meter names/tags for claims, notification/polling, leases, retries, fencing, cancellations, and terminal outcomes.
- `docker-compose.server.yml`: PostgreSQL, two API services, and worker services using the same server image and separate management/public network exposure.

</details>

<details>
<summary>Dependencies</summary>

- Existing Spring Boot 3.3.5, Spring JDBC, Flyway, Quartz, Spring Security/Session, Log4j2, PostgreSQL JDBC, Testcontainers, Docker/Compose, and frontend build tooling remain the foundation.
- Add only `micrometer-registry-prometheus` to `replicadb-server`; do not alter the root CLI dependency graph.
- Use the PostgreSQL JDBC DataSource for Quartz and metadata state. Do not add R2DBC, an external broker, a new cancellation table, or a second source of truth for schedules.
- Use the resolved Quartz PostgreSQL DDL and resolved Spring Boot management API. Verify both before coding because dependency behavior has caused prior plan gaps.
- Process-level validation is an explicit build/profile step, not an unconditional default server test run. It requires Docker and dynamically allocated ports.

</details>

<details>
<summary>Testing Strategy</summary>

| Layer | Tooling | Required evidence |
| --- | --- | --- |
| Migrations | Flyway + PostgreSQL Testcontainers | V15 Quartz and V16 throttle apply forward-only after V14; constraints/indexes/schema are correct |
| Quartz | Spring context + two JDBC Quartz schedulers | Cluster check-in, idempotent reconciliation, no duplicate trigger firing, no missed persisted schedule after restart |
| Authentication | JUnit/Mockito plus two real PostgreSQL-backed API contexts and HTTP clients | Shared 5/15-minute account/address throttle, concurrent reservation boundary, success reset, fail-closed store outage |
| Health/management | Spring Boot context + real HTTP clients | API health compatibility, worker internal management port, readiness/liveness semantics, no public worker API/session/Quartz |
| Metrics | Micrometer `SimpleMeterRegistry` plus Prometheus endpoint | Required meters, bounded tags, outcome coverage, no secrets/high-cardinality ids |
| Packaging | Maven, Docker, Docker Compose | Non-root server image, API/worker startup, dynamic ports, no credentials baked into layers |
| Distributed failures | Separate Compose processes + PostgreSQL | Worker loss during copy/merge, lease recovery, PostgreSQL restart, notification loss/reconnect, duplicate polling, stale fencing |
| Load/chaos | Reproducible shell harness with bounded parameters | No duplicate terminal outcomes/watermarks, measured lag/latency, expected recovery chains, cleanup |
| Compatibility | Root Maven/CLI and jar inspection | CLI tests pass, no Spring Boot in CLI artifact, no metadata database required, existing exit/options contracts remain |

</details>

## Risks, Assumptions, and Deferred Work

- Quartz JDBC clustering prevents duplicate firing only when every scheduler for a schedule uses the clustered store. The deployment guide must make the RAM-to-JDBC handoff explicit; mixed old/new scheduler ownership is not treated as a safe rolling state.
- Spring Boot management-server behavior with `spring.main.web-application-type=none` was tested and does not create the required child context in 3.3.5. The worker therefore uses a servlet-capable primary context with `server.port=-1`; explicit profile gates keep product REST, frontend, Security/session, and Quartz absent, while only the internal management port listens.
- Login reservations count in-flight authentication conservatively. A crashed request can temporarily consume a slot until cleanup; the stale reservation TTL and cleanup cadence must be shorter than the 15-minute window and tested.
- Quartz and login tables increase PostgreSQL metadata traffic. Pool sizing and lock/check-in intervals must be measured with the worker listener, heartbeats, API sessions, scheduler, and replication workload together.
- Health indicators are operational signals, not source/sink reconciliation. Queue emptiness and terminal status do not prove data equality.
- Process-level kill/restart tests require Docker and may be architecture-sensitive. The harness must use the repository's Testcontainers/CI configuration and report infrastructure unavailability separately from product failures.
- No JIRA acceptance criteria were supplied. The acceptance criteria above are extracted from the approved Phase 3.3 architecture decision and the implemented Phase 3.1/3.2 contracts.

## Phase Exit Criteria

Phase 3.3 is complete only when:

- Two API instances use PostgreSQL Quartz clustering without duplicate schedule firings and continue serving the same durable state.
- Shared login throttling behaves consistently across API instances and old attempt state is bounded by cleanup.
- API and worker health/readiness signals distinguish PostgreSQL, scheduler, listener, polling, queue, and executor conditions without exposing secrets.
- Required metrics are available with bounded labels and show claims, notification/polling behavior, lease/retry/fencing, cancellation, and terminal outcomes.
- The worker has no public REST/frontend/security-session/Quartz surface; its management port is internal and documented.
- Worker loss during source copy or sink merge preserves the abandoned attempt and creates a new attempt only when policy permits; a healthy heartbeat prevents false recovery.
- PostgreSQL restart, listener reconnect, missed notifications, and duplicate polling recover through durable state and mandatory polling.
- Reproducible multi-node load/chaos checks pass with measured concurrency and no duplicate watermarks or stale-worker writes.
- Server packaging and CI verify both managed profiles while the standalone CLI artifact remains Spring-free and PostgreSQL-independent.

## Quality Gate Notes

The draft was reviewed once by a critic sub-agent. The review found implementation gaps around resolved Quartz APIs/DDL, shared-throttle locking, worker management-port viability, deterministic worker-loss barriers, existing test paths, and task ownership; those issues were corrected before execution. The remaining local CLI integration limitation is environmental: DB2 amd64 emulation failed during startup on the ARM64 Docker host, while the standalone Spring-free classpath check passed.

## Execution Retrospective

### Plan Accuracy

- Tasks completed: 19/19 (100%).
- Tasks requiring implementation adjustment: 8/19 (42.1%). Adjustments were concentrated in the runtime harness and framework boundaries rather than product scope.
- Focused server and distributed validation passed; the full root CLI integration suite was attempted but could not complete successfully on the local ARM64/emulated database environment.

### Gaps Encountered

#### Gap 1: Deferred toolchain and dependency resolution

- **Task**: 1.1, 1.2, and 3.1.
- **Plan assumed**: the shell's default Java and the initially described Spring Boot management behavior were immediately usable.
- **Reality**: the terminal selected an unavailable Java version, and Spring Boot 3.3.5 did not create a management child context with `web-application-type=none`; the first Prometheus configuration also required an explicit exporter-enabled property.
- **Resolution**: all Maven checks used the installed Java 17 path; Quartz 2.3.2 DDL/classes were resolved from the dependency; the worker uses a servlet-capable context with `server.port=-1`, an internal management port, explicit Actuator security exclusion, and `management.prometheus.metrics.export.enabled=true`.
- **Learning**: resolve framework behavior and effective dependency APIs with a minimal executable probe before building dependent runtime tasks.

#### Gap 2: Profile test configuration overrode production Quartz settings

- **Task**: 1.2.
- **Plan assumed**: the profile YAML used by context tests would inherit the production Quartz block.
- **Reality**: `src/test/resources/application-api.yml` overrode the profile without the JDBC cluster properties, causing the guard to reject a valid production configuration.
- **Resolution**: synchronized the test profile with the clustered Quartz properties and retained a dedicated misconfiguration guard test.
- **Learning**: inspect profile-specific test resources whenever a Spring context assertion disagrees with the main application configuration.

#### Gap 3: Compose fixtures ran before Flyway

- **Task**: 4.2.
- **Plan assumed**: mounting the source/sink fixture under PostgreSQL's init directory was harmless.
- **Reality**: the init script created application tables before Flyway, which rejected the non-empty schema without history.
- **Resolution**: removed the init mount and loaded the non-secret fixture after the services became healthy.
- **Learning**: metadata migrations must own first schema initialization; test data loading belongs after Flyway unless the fixture is itself a migration.

#### Gap 4: Image and process readiness were weaker than container liveness

- **Task**: 4.1, 5.1, and 5.3.
- **Plan assumed**: published ports and `curl --retry` alone would provide a stable startup signal.
- **Reality**: Docker exposed ports before the JVM completed startup, and the first image used a stale jar because the jar had not been repackaged after source edits.
- **Resolution**: added `curl` and healthchecks to the image/Compose services, used `docker compose --wait`, built the server jar explicitly before image validation, and used explicit Compose project names with cleanup.
- **Learning**: process harnesses need image-level healthchecks and must validate the packaged artifact, not only compiled classes.

#### Gap 5: Failure harnesses needed database-observable barriers

- **Task**: 5.2 and 5.3.
- **Plan assumed**: stopping/restarting containers and short polling loops would deterministically place a run in copy, merge, or recovery states.
- **Reality**: a PostgreSQL lock-holder process could outlive its local shell, and workers could still be starting while a run was polled.
- **Resolution**: used `pg_sleep` source queries, `pg_stat_activity`/`pg_locks` observation, explicit `pg_terminate_backend` cleanup, health-aware worker restarts, and scenario-specific harness entry points.
- **Learning**: kill/restart tests must wait on database-visible operation state and terminate helper backends explicitly.

#### Gap 6: Full CLI integration was not a valid local release signal

- **Task**: 6.1.
- **Plan assumed**: the complete root suite would be a useful final local check.
- **Reality**: the local Docker host is ARM64 with limited memory; the amd64 DB2 container failed during instance startup, causing cascading DB2/Mongo/SQL Server connection errors. A focused DB2 rerun reproduced the same infrastructure failure after removing the stale reused container.
- **Resolution**: did not modify CLI production code or mask the failures. The server suite, managed multinode gates, image checks, documentation gate, and `NoSpringBootOnClasspathTest` passed; full CLI integration remains a CI/architecture-specific release check.
- **Learning**: classify heterogeneous database integration failures by container readiness and architecture before treating them as product regressions.

### Patterns Discovered

- **Framework probe first**: validate Spring Boot management, Actuator exporter, and Quartz property binding with a minimal context before adding dependent health/metrics code.
- **Database-owned lifecycle evidence**: use PostgreSQL health, `pg_stat_activity`, `pg_locks`, durable run rows, and Compose healthchecks instead of fixed sleeps.
- **Artifact-before-image rule**: package the server jar explicitly, inspect migration/configuration resources, then build the image with a minimal context.
- **Process harness isolation**: every scenario uses an explicit Compose project, generated credentials, dynamic API ports, disposable metadata volume, bounded waits, and a cleanup trap.
- **Compatibility boundary**: the server adds metrics and runtime dependencies only to `replicadb-server`; the root CLI remains Spring-free and metadata-database independent.

### Validation Summary

- `replicadb-server`: full Maven suite passed with 308 tests and zero failures/errors.
- Focused Actuator/worker tests: 8 tests passed; health-indicator and metrics tests passed.
- Named `phase3-multinode` Maven profile: passed with two API processes and two workers.
- Compose smoke, four-run concurrent load, worker-loss copy/merge, PostgreSQL restart, notification loss, listener restart, duplicate polling, and chaos harnesses: passed.
- Documentation, shell syntax, POM XML, Compose YAML, and CI YAML checks: passed. `actionlint` retains pre-existing warnings in the release workflow unrelated to these changes.
- Root CLI `NoSpringBootOnClasspathTest`: 2 tests passed. Full cross-database CLI suite: attempted, but blocked by DB2 amd64 emulation/container readiness on the local ARM64 environment.
