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
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

Driving forces: durable job history, overlap constraints, idempotency, schedules, sessions, permissions, audit, and watermark state need transactional ownership rather than logs or process memory.

Decision: managed profiles use PostgreSQL with forward-only Flyway migrations and Spring JDBC repositories. The standalone CLI does not require this metadata store. Product schedules are persisted in PostgreSQL and reconciled into the current Quartz runtime. Phase 3.1 extends the store with retry policy, `available_at`, lease tokens, atomic eligible claims, recovery, and fenced writes behind `JobRunStore`.

Trade-offs: local and CI integration tests require database/container setup, explicit readiness, and migration discipline. Database `now()` must remain the time authority for distributed eligibility and lease operations, and V13/V14 must remain forward-only. SQLite remains useful for isolated CLI source/sink fixtures but is not the managed production store.
