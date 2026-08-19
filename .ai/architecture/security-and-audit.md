---
type: Domain Model
description: Local identity, global roles, per-job permissions, session security, and audit records protect managed operations.
sources:
  - id: security
    resource: replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java
  - id: access
    resource: replicadb-server/src/main/java/org/replicadb/server/security/JobAccessService.java
  - id: user
    resource: replicadb-server/src/main/java/org/replicadb/server/security/domain/AppUser.java
  - id: permission
    resource: replicadb-server/src/main/java/org/replicadb/server/security/domain/JobPermission.java
  - id: audit
    resource: replicadb-server/src/main/java/org/replicadb/server/audit/AuditService.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Local users have `ADMIN`, `OPERATOR`, or `VIEWER` roles. Job ACLs grant `VIEW`, `EDIT`, `EXECUTE`, or `CANCEL`. `JobAccessService` is the backend authority and gives administrators an explicit bypass; frontend route and navigation guards do not replace it.

Spring Security uses session cookies, JDBC-backed Spring Session, Argon2 password hashing, a CSRF cookie/header contract, and login-attempt throttling. A public CSRF bootstrap endpoint supports browser flows before login. Audit events record actor, resource, action, outcome, and sanitized detail; retention is scheduled and audit persistence failures have an explicit service-level policy.

Credential references are resolved at execution time. Resolved values are redacted before error, log, audit, or telemetry boundaries.

Reference implementations: `SecurityConfig.java`, `JobAccessService.java`, `AuthController.java`, and `AuditService.java`.
