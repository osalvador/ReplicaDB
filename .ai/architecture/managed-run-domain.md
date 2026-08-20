---
type: Domain Model
description: Managed runs and schedules make execution state explicit and enforce legal transitions.
sources:
  - id: run
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRun.java
  - id: lease
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/LeaseToken.java
  - id: status
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStatus.java
  - id: machine
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java
  - id: retry-policy
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/RetryPolicy.java
  - id: schedule
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobSchedule.java
  - id: tests
    resource: replicadb-server/src/test/java/org/replicadb/server/job/domain/JobRunStateMachineTest.java
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

`JobRun` records status, attempt, executor/lease data, counters, timestamps, error text, committed watermark, cancellation warning, and the database-owned `availableAt` eligibility time. A claimed run carries an opaque `LeaseToken`; pending and historical rows may have no current token. Observed statuses are `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCEL_REQUESTED`, `CANCELLED`, and `RETRY_SCHEDULED`.

`RetryPolicy` validates `maxAttempts >= 1` and nonnegative backoff. Its defaults are three total attempts and 60 seconds; automatic expiry retry defaults on for `incremental` and `complete-atomic`, and off for destructive `complete` unless explicitly enabled. `JobRunStateMachine` checks transitions before persistence. A manual or recovered retry changes the prior row to `RETRY_SCHEDULED` and inserts a new pending row with a higher attempt rather than resetting history. Expired `CANCEL_REQUESTED` runs become `CANCELLED` without replacement. A partial unique database constraint prevents overlapping executable runs per job. `JobSchedule` stores cron, time zone, and enabled state as the product schedule model.

Lease identity is internal state, not an API field. Token-checked updates fence stale workers after recovery; `JobRunResponse` exposes `availableAt` for retry observability but never the lease token.

Reference implementations: `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java`, `JobRun.java`, and `JobSchedule.java`.
