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
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

The managed server uses forward-only Flyway migrations and Spring JDBC repositories. PostgreSQL stores job definitions, job runs, retry idempotency, schedules, local users, job permissions, Spring Session tables, and audit events. V13 persists per-job retry policy; V14 adds database-owned `available_at`, lease identity, and the partial eligible-run index. `JobRunStore` separates the application contract from the JDBC adapter, while repositories map immutable records and keep state-machine checks around updates.

The database is the durable source of truth for product schedules; the current Quartz configuration uses an in-memory runtime store and is reconciled from persisted schedules. PostgreSQL `now()` owns claim eligibility, lease timestamps, and expiry backoff. `FOR UPDATE SKIP LOCKED` claims one eligible pending row, and token-checked updates return explicit `UPDATED`, `FENCED`, or `NOT_FOUND` outcomes. Recovery retains an expired attempt and may insert a backoff-delayed replacement in one transaction. Partial uniqueness protects active-run overlap, while a bounded idempotency table protects repeated manual triggers.

Repository boundaries use explicit JDBC representations where the PostgreSQL driver needs them, including UUID and temporal parameters. Deprecated claim/finalization wrappers remain only for compatibility during migration to the ports. Integration tests use Testcontainers and staged migration-count, backfill, index, constraint, claim, recovery, and fencing assertions.

Reference implementations: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `JobDefinitionRepository.java`, and `replicadb-server/src/main/resources/db/migration`.
