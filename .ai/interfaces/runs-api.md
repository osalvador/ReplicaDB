---
type: REST Endpoint
description: The runs API triggers and observes asynchronous job executions with explicit cancellation, retry, and idempotency behavior.
sources:
  - id: controller
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java
  - id: repository
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java
  - id: idempotency
    resource: replicadb-server/src/main/java/org/replicadb/server/job/persistence/RunTriggerIdempotencyRepository.java
  - id: execution
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java
  - id: tests
    resource: replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The controller exposes paginated job-run history at `GET /jobs/{jobId}/runs` and `GET /runs`, detail at `GET /runs/{id}`, and log text at `GET /runs/{id}/log`. `POST /jobs/{jobId}/runs` triggers a run and requires an `Idempotency-Key`; `POST /runs/{id}/cancel` requests cancellation; `POST /runs/{id}/retry` creates a new pending attempt from a failed run.

Run status is represented by the explicit state model, not inferred from log text. Cancellation returns and persists a mode-specific warning because the sink may be indeterminate. Retry keeps the prior row in history and uses the repository overlap constraint and idempotency record to avoid duplicate executable work.

Authorization is job-scoped. `VIEW` is needed for reads, `EXECUTE` for trigger/retry, and `CANCEL` for cancellation, with the administrator bypass defined by the access service.

Reference implementations: `JobRunController.java`, `JobRunRepository.java`, and `RunExecutionCoordinator.java`.
