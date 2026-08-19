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
  - id: schedule-repository
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobScheduleRepository.java
  - id: application
    resource: replicadb-server/src/main/resources/application-api.yml
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The managed server uses forward-only Flyway migrations and Spring JDBC repositories. PostgreSQL stores job definitions, job runs, retry idempotency, schedules, local users, job permissions, Spring Session tables, and audit events. Repository methods map immutable records and keep state-machine checks around updates.

The database is the durable source of truth for product schedules; the current Quartz configuration uses an in-memory runtime store and is reconciled from persisted schedules. PostgreSQL partial uniqueness protects active-run overlap, while a bounded idempotency table protects repeated manual triggers.

Repository boundaries use explicit JDBC representations where the PostgreSQL driver needs them, including temporal parameters. Integration tests use Testcontainers and migration-count/constraint assertions.

Reference implementations: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `JobDefinitionRepository.java`, and `replicadb-server/src/main/resources/db/migration`.
