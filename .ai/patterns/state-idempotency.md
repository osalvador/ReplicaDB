---
type: Pattern
description: Explicit state transitions, database uniqueness, and idempotency records coordinate managed run lifecycle.
sources:
  - id: machine
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java
  - id: runs
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java
  - id: trigger
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/RunTriggerIdempotencyRepository.java
  - id: migration
    resource: replicadb-server/src/main/resources/db/migration
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The domain state machine rejects illegal transitions before repository updates. PostgreSQL partial uniqueness is the authoritative overlap guard for executable statuses. A retry transitions the old failed row and inserts a new pending attempt, so history remains append-like and the uniqueness predicate must exclude transitional `RETRY_SCHEDULED` rows.

Manual trigger requests use an idempotency key with bounded retention. Pre-checks can improve error messages but do not replace the database constraint or transactional state update.

Reference implementations: `JobRunStateMachine.java`, `JobRunRepository.java`, and the run-trigger migrations.
