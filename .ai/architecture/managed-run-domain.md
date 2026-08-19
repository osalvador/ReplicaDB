---
type: Domain Model
description: Managed runs and schedules make execution state explicit and enforce legal transitions.
sources:
  - id: run
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRun.java
  - id: status
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStatus.java
  - id: machine
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java
  - id: schedule
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobSchedule.java
  - id: tests
    resource: replicadb-server/src/test/java/org/replicadb/server/job/domain/JobRunStateMachineTest.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`JobRun` records status, attempt, executor/lease data, counters, timestamps, error text, committed watermark, and cancellation warning. Observed statuses are `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCEL_REQUESTED`, `CANCELLED`, and `RETRY_SCHEDULED`.

`JobRunStateMachine` checks transitions before persistence. A retry changes the failed row to `RETRY_SCHEDULED` and inserts a new pending row with a higher attempt rather than resetting the original history. A partial unique database constraint prevents overlapping executable runs per job. `JobSchedule` stores cron, time zone, and enabled state as the product schedule model.

Reference implementations: `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java`, `JobRun.java`, and `JobSchedule.java`.
