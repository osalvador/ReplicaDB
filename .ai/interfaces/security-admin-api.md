---
type: REST Endpoint
description: Authentication, local-user administration, job permissions, and audit reads form the managed security API.
sources:
  - id: auth
    resource: replicadb-server/src/main/java/org/replicadb/server/security/api/AuthController.java
  - id: users
    resource: replicadb-server/src/main/java/org/replicadb/server/security/api/UserController.java
  - id: permissions
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionController.java
  - id: audit
    resource: replicadb-server/src/main/java/org/replicadb/server/audit/api/AuditEventController.java
  - id: security
    resource: replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Authentication uses `POST /auth/login`, `GET /auth/me`, `POST /auth/logout`, and public `GET /auth/csrf`. Session cookies and the CSRF cookie/header contract support browser state changes. Failed login attempts are throttled by account and source address.

ADMIN-only user operations are `POST /users`, `GET /users`, `GET /users/{id}`, `PUT /users/{id}`, and `PUT /users/{id}/password`. Job permission operations are `GET`, `PUT`, and `DELETE` under `/jobs/{id}/permissions`; the backend checks job access and ADMIN bypass. `GET /audit` is ADMIN-only and paginated/filterable.

Responses use explicit DTOs and sanitized details. The frontend's `RequireRole`, navigation, and action visibility improve usability but do not authorize requests.

Reference implementations: `AuthController.java`, `UserController.java`, `JobPermissionController.java`, and `SecurityConfig.java`.
