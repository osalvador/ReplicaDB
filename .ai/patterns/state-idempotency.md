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
  - id: port
    resource: replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java
  - id: deletion
    resource: replicadb-server/src/main/resources/db/migration/V21__cascade_job_dependent_state_on_definition_delete.sql
generated: { by: itx-init/2.1, at: "2026-09-04T05:47:18Z" }
status: stable
---

The domain state machine rejects illegal transitions before repository updates. PostgreSQL partial uniqueness is the authoritative overlap guard for executable statuses. A retry transitions the old failed row and inserts a new pending attempt, so history remains append-like and the uniqueness predicate must exclude transitional `RETRY_SCHEDULED` rows.

Manual trigger requests use an idempotency key with bounded retention. Eligible claims use `available_at <= now()` and `FOR UPDATE SKIP LOCKED`, then assign a fresh lease token. Expired recovery changes the abandoned row before inserting a backoff-delayed replacement in one transaction; token-fenced updates return explicit outcomes so a stale worker cannot finalize or advance a watermark. Pre-checks can improve error messages but do not replace the database constraint or transactional state update.

Job deletion locks the parent, rejects active runs, unschedules Quartz, and relies on job-owned foreign-key cascades to remove schedule, permissions, runs, logs, and idempotency rows while leaving independent audit events intact.

Reference implementations: `JobRunStateMachine.java`, `JobRunRepository.java`, and the run-trigger migrations.
