---
type: Pattern
description: API DTOs and mappers isolate wire representations from immutable domain records and normalize failures to RFC 7807.
sources:
  - id: mapper
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionMapper.java
  - id: controller
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java
  - id: handler
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java
  - id: user-api
    resource: replicadb-server/src/main/java/org/replicadb/server/security/api/UserController.java
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

Request records validate at controller boundaries and map into domain records. Optional retry-policy request fields are resolved to mode-aware domain defaults, while response records expose stable policy values, lower-case mode text, `availableAt`, and computed warnings without leaking persistence or framework implementation types. Lease tokens and credential values remain outside the response shape. The pattern is used across job, run, schedule, user, permission, and audit controllers.

`GlobalExceptionHandler` maps validation, authentication, authorization, conflict, missing-resource, rate-limit, and unexpected failures to `ProblemDetail`. Dynamic details are passed through credential redaction; framework-level missing-resource errors retain 404 semantics.

Reference implementations: `JobDefinitionMapper.java`, `GlobalExceptionHandler.java`, and `UserController.java`.
