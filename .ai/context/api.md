## REST API
| Surface | Base path | Behavior |
| --- | --- | --- |
| Job definitions | `/api/v1/jobs` | create/list/read/update; list is paginated and ACL-filtered |
| Runs | `/api/v1/jobs/{id}/runs`, `/api/v1/runs` | read history/detail/log, trigger with `Idempotency-Key`, cancel, retry |
| Schedules | `/api/v1/jobs/{id}/schedule` | CRUD-like upsert/read/delete backed by Quartz |
| Permissions/users | `/api/v1/jobs/{id}/permissions`, `/api/v1/users` | ADMIN-managed users and job ACLs |
| Auth/audit | `/api/v1/auth`, `/api/v1/audit` | session login/logout/me/CSRF; ADMIN-only audit reads |

Controllers map immutable domain records to DTOs. Lower-case mode text is the public representation, pagination is bounded by `PageRequestParams`, validation occurs at request boundaries, and `GlobalExceptionHandler` emits RFC 7807 problem details without echoing sensitive request content. `springdoc-openapi` exposes `/v3/api-docs`; the committed frontend schema is generated from it.

## Security
`SecurityConfig` uses session cookies, `CookieCsrfTokenRepository` with the browser `XSRF-TOKEN`/`X-XSRF-TOKEN` contract, and a public CSRF bootstrap endpoint. `JobAccessService` is the backend authority for ADMIN bypass and `VIEW`/`EDIT`/`EXECUTE`/`CANCEL` checks. `AdminBootstrapRunner` provisions an initial admin only through environment-managed values; authentication attempts are throttled.

## Event and External Protocols
There are no REST event consumers, gRPC services, or AsyncAPI contracts. Kafka is a core sink adapter, not a server event bus. API protocol-specific rules are delegated to `/itx-init-api` because OpenAPI is present.

## Reference Implementations
- `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java`
- `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`
- `replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java`
- `replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java`
- `replicadb-server/src/main/java/org/replicadb/server/security/api/AuthController.java`

## Recent Learnings
- [WARNING] Real-port session/CSRF tests need a cookie jar and cannot be replaced by MockMvc security annotations. Source: `phase-1c-3-security`.
- [WARNING] Keep framework types out of public OpenAPI schemas and compare generated output deterministically. Source: `phase-2a-frontend-auth-monitoring`.