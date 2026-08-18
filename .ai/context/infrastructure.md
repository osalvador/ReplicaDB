## Core Adapters
Concrete managers under `src/main/java/org/replicadb/manager` are the core infrastructure boundary. SQL adapters use JDBC transactions and vendor dialects; MongoDB, Kafka, S3, CSV, and ORC own their native clients or file behavior while preserving the manager/row-set contract. Dispatch is through `SupportedManagers`, `ManagerFactory`, and `FileManagerFactory`.

## Managed Persistence
| Adapter | Store | Key Details |
| --- | --- | --- |
| `JobDefinitionRepository` | PostgreSQL via `NamedParameterJdbcTemplate` | maps one table-pair definition; ACL filtering is applied before pagination |
| `JobRunRepository` | PostgreSQL | claims with `FOR UPDATE SKIP LOCKED`; persists states, counters, leases, watermarks, and warnings |
| `JobScheduleRepository` | PostgreSQL | durable schedule source of truth for Quartz reconciliation |
| Security repositories | PostgreSQL | users and per-job permissions; UUID-array restrictions support visible-job queries |
| `AuditEventRepository` | PostgreSQL JSONB | filtered admin history; actor username survives user deletion |

## Schema and Configuration
Flyway migrations `V1` through `V11` create job definitions/runs, active-run constraints, idempotency, schedules, users, permissions, JDBC sessions, audit events, and cancellation warnings. PostgreSQL is required for the managed server state store; SQLite remains a CLI/test fixture, not a control-plane deployment store.

`application.yml` owns server port, Quartz memory store, execution pool size, health exposure, and audit retention. `application-api.yml` supplies environment-managed PostgreSQL settings, Flyway, JDBC sessions, and secure session-cookie attributes. Mongo auto-configuration is explicitly excluded because the core artifact brings Mongo classes transitively.

## Cross-Cutting
- Log4j2 is required in the server because core Sentry setup expects the Log4j2 context; Spring Boot starter logging is excluded.
- Audit writes sanitize and truncate details and are best-effort; they must not become the source of truth for job state.
- Credentials stay as environment references in state and are redacted at error, audit, and telemetry boundaries.

## Reference Implementations
- `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`
- `replicadb-server/src/main/java/org/replicadb/server/security/persistence/JobPermissionRepository.java`
- `replicadb-server/src/main/java/org/replicadb/server/audit/persistence/AuditEventRepository.java`
- `replicadb-server/src/main/resources/db/migration/V1__create_job_definition.sql`
- `replicadb-server/src/main/resources/application-api.yml`

## Recent Learnings
- [WARNING] Bind PostgreSQL temporal values explicitly as JDBC timestamps at repository boundaries; driver inference for `Instant` is not reliable. Source: `phase-1b-state-layer`.
- [WARNING] Test container port readiness separately when raw JDBC connects through a mapped port. Source: `phase-1b-state-layer`.