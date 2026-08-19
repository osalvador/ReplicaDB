---
type: REST Endpoint
description: The jobs API manages one-table job definitions and their durable schedules under /api/v1.
sources:
  - id: jobs-controller
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java
  - id: schedule-controller
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleController.java
  - id: mapper
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionMapper.java
  - id: dto
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionRequest.java
  - id: tests
    resource: replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Base path: `/api/v1`.

Job definition operations are `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, and `PUT /jobs/{id}`. List responses are paginated and filtered by visible job IDs for non-admin users. Create/update request DTOs validate at the HTTP boundary and map through immutable domain records. Mode text is lower-case at the REST boundary even though the Java enum constants are upper-case.

Schedule operations are `PUT /jobs/{id}/schedule`, `GET /jobs/{id}/schedule`, and `DELETE /jobs/{id}/schedule`. The persisted schedule is the product source of truth; Quartz is reconciled from it. Authorization uses method security and `JobAccessService` checks.

Errors use `application/problem+json` through `GlobalExceptionHandler`, with redacted dynamic details. `springdoc-openapi` exposes the contract consumed by the frontend generator.

Reference implementations: `JobDefinitionController.java`, `JobScheduleController.java`, and `JobDefinitionMapper.java`.
