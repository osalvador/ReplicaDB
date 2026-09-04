---
type: Adapter
description: Spring JDBC repositories and Flyway migrations persist managed jobs, runs, schedules, identity, permissions, sessions, and audit events in PostgreSQL.
sources:
  - id: migrations
    resource: replicadb-server/src/main/resources/db/migration
  - id: job-repository
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java
  - id: run-repository
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java
  - id: run-port
    resource: replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java
  - id: schedule-repository
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobScheduleRepository.java
  - id: application
    resource: replicadb-server/src/main/resources/application-api.yml
  - id: quartz-migration
    resource: replicadb-server/src/main/resources/db/migration/V15__create_quartz_jdbc_schema.sql
  - id: login-attempt-repository
    resource: replicadb-server/src/main/java/org/replicadb/server/security/persistence/LoginAttemptRepository.java
  - id: run-log
    resource: replicadb-server/src/main/resources/db/migration/V20__create_run_log.sql
  - id: deletion-cascade
    resource: replicadb-server/src/main/resources/db/migration/V21__cascade_job_dependent_state_on_definition_delete.sql
generated: { by: itx-init/2.1, at: "2026-09-04T05:47:18Z" }
status: stable
---

The managed server uses forward-only Flyway migrations and Spring JDBC repositories. PostgreSQL stores job definitions, job runs, retry idempotency, schedules, local users, job permissions, Spring Session tables, audit events, run logs, Quartz JDBC state, and login-attempt reservations. V13 persists per-job retry policy; V14 adds database-owned `available_at`, lease identity, and the partial eligible-run index; V15 adds Quartz tables/locks; V16 adds shared login-throttle state; V20 adds bounded run-log persistence; V21 adds job-owned deletion cascades after an orphan precondition check. `JobRunStore` separates the application contract from the JDBC adapter, while repositories map immutable records and keep state-machine checks around updates.

The database is the durable source of truth for product schedules; the API uses a PostgreSQL-backed clustered Quartz store and reconciles product schedules into stable Quartz keys. PostgreSQL `now()` owns claim eligibility, lease timestamps, and expiry backoff. `FOR UPDATE SKIP LOCKED` claims one eligible pending row, and token-checked updates return explicit `UPDATED`, `FENCED`, or `NOT_FOUND` outcomes. Recovery retains an expired attempt and may insert a backoff-delayed replacement in one transaction. Partial uniqueness protects active-run overlap, while a bounded idempotency table protects repeated manual triggers. Login-attempt reservations serialize account/address decisions with ordered PostgreSQL advisory locks and expire through an API cleanup task.

Repository boundaries use explicit JDBC representations where the PostgreSQL driver needs them, including UUID and temporal parameters. Successful runs with a null committed watermark are excluded when selecting the last usable watermark, so an earlier non-null value remains available. Production callers use the state ports and token-aware services; the deprecated Phase 3.1 repository bridge is guarded against reintroduction. Integration tests use Testcontainers and staged migration-count, backfill, index, constraint, cascade, claim, recovery, fencing, Quartz, and shared-throttle assertions.

Reference implementations: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `JobDefinitionRepository.java`, and `replicadb-server/src/main/resources/db/migration`.
