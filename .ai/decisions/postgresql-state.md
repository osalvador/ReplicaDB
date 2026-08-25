---
type: Decision
description: PostgreSQL is the durable state store for managed control-plane metadata while the CLI remains database-independent.
sources:
  - id: decision
    resource: ARCHITECTURE_DECISIONS.md
  - id: config
    resource: replicadb-server/src/main/resources/application-api.yml
  - id: migrations
    resource: replicadb-server/src/main/resources/db/migration
  - id: repository
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java
  - id: quartz
    resource: replicadb-server/src/main/resources/db/migration/V15__create_quartz_jdbc_schema.sql
  - id: login-throttle
    resource: replicadb-server/src/main/java/org/replicadb/server/security/persistence/LoginAttemptRepository.java
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

Driving forces: durable job history, overlap constraints, idempotency, schedules, sessions, permissions, audit, and watermark state need transactional ownership rather than logs or process memory.

Decision: managed profiles use PostgreSQL with forward-only Flyway migrations and Spring JDBC repositories. The standalone CLI does not require this metadata store. Product schedules are persisted in PostgreSQL and reconciled into the clustered Quartz JDBC runtime. Phase 3.1 extends the store with retry policy, `available_at`, lease tokens, atomic eligible claims, recovery, and fenced writes behind `JobRunStore`; Phase 3.3 adds Quartz tables and shared login-attempt reservations through V15/V16.

Trade-offs: local and CI integration tests require database/container setup, explicit readiness, and migration discipline. Database `now()` must remain the time authority for distributed eligibility and lease operations, and V13 through V16 must remain forward-only. Quartz lock/check-in traffic and login-throttle reservations add metadata load that deployment sizing must account for. SQLite remains useful for isolated CLI source/sink fixtures but is not the managed production store.
