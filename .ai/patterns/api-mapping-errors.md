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
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Request records validate at controller boundaries and map into domain records. Response records expose stable wire values, including lower-case mode text and computed warnings, without leaking persistence or framework implementation types. The pattern is used across job, run, schedule, user, permission, and audit controllers.

`GlobalExceptionHandler` maps validation, authentication, authorization, conflict, missing-resource, rate-limit, and unexpected failures to `ProblemDetail`. Dynamic details are passed through credential redaction; framework-level missing-resource errors retain 404 semantics.

Reference implementations: `JobDefinitionMapper.java`, `GlobalExceptionHandler.java`, and `UserController.java`.
