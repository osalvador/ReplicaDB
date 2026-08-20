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
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

The controller exposes paginated job-run history at `GET /jobs/{jobId}/runs` and `GET /runs`, detail at `GET /runs/{id}`, and log text at `GET /runs/{id}/log`. Run responses include `availableAt`, attempt, lease timestamps, counters, and cancellation warning, but not the internal lease token. `POST /jobs/{jobId}/runs` triggers a run and requires an `Idempotency-Key`; `POST /runs/{id}/cancel` requests durable cancellation; `POST /runs/{id}/retry` creates a new immediately eligible pending attempt from a failed run.

Run status is represented by the explicit state model, not inferred from log text. Cancellation persists `CANCEL_REQUESTED` before attempting the local signal and returns a mode-specific warning because the sink may be indeterminate. Retry keeps the prior row in history and uses the repository overlap constraint and idempotency record to avoid duplicate executable work. Automatic lease-expiry recovery is a database contract, but the current API runtime has no recovery scheduler or worker loop.

Authorization is job-scoped. `VIEW` is needed for reads, `EXECUTE` for trigger/retry, and `CANCEL` for cancellation, with the administrator bypass defined by the access service.

Reference implementations: `JobRunController.java`, `JobRunRepository.java`, and `RunExecutionCoordinator.java`.
