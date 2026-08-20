okf_version: "0.2"

# Service Description
ReplicaDB is a Java bulk-replication engine for moving data between heterogeneous databases, files, object storage, and Kafka. A sibling Spring Boot control plane manages durable jobs, schedules, runs, authentication, permissions, and audit history, while a React SPA consumes its REST contract. The standalone CLI remains usable without the control plane.

# Tech Stack
Java 17/Maven, JDBC and vendor SDKs, Spring Boot 3.3.5, PostgreSQL/Flyway, Quartz, React 18/TypeScript/Vite, MUI, TanStack Query, Log4j2/Sentry, JUnit Jupiter 6, Vitest, Playwright, and Testcontainers. Deploy: CLI archive or managed server/container.

# Architecture
* [System overview](/architecture/system-overview.md) - Monorepo boundaries and dependency direction.
* [CLI execution](/architecture/cli-execution.md) - Options, managers, tasks, aggregation, and exit codes.
* [Manager adapters](/architecture/manager-adapters.md) - Database, file, object-storage, and Kafka dispatch.
* [Row-set transfer](/architecture/rowset-transfer.md) - JDBC-shaped source adaptation.
* [Managed job domain](/architecture/managed-job-domain.md) - Job definitions, modes, endpoints, and invariants.
* [Managed run domain](/architecture/managed-run-domain.md) - Run states, retries, and schedules.
* [Managed execution](/architecture/managed-execution.md) - Run claiming, scheduling, core delegation, and state updates.
* [Persistence state](/architecture/persistence-state.md) - PostgreSQL, Flyway, repositories, and runtime scheduler state.
* [Security and audit](/architecture/security-and-audit.md) - Identity, ACLs, sessions, and audit records.
* [Frontend application](/architecture/frontend-application.md) - SPA routes, state, and admin slice.
* [Deployment and operations](/architecture/deployment-and-operations.md) - Maven, containers, configuration, and test infrastructure.

# Interfaces
* [CLI options contract](/interfaces/cli-options.md) - Command-line and options-file boundary.
* [Jobs API](/interfaces/jobs-api.md) - Job definitions and schedules.
* [Runs API](/interfaces/runs-api.md) - Trigger, monitor, cancel, retry, and idempotency.
* [Security and administration API](/interfaces/security-admin-api.md) - Sessions, users, ACLs, and audit reads.
* [OpenAPI frontend contract](/interfaces/openapi-frontend.md) - Generated TypeScript contract and drift checks.
* [Protocol inventory](/interfaces/protocol-inventory.md) - Supported sinks and absent server event protocols.

# Patterns
* [Manager factory](/patterns/manager-factory.md) - Ordered scheme dispatch and extension points.
* [JDBC-shaped transfer](/patterns/jdbc-shaped-transfer.md) - Row sets preserve sink reuse.
* [Execution context and cancellation](/patterns/execution-context-cancellation.md) - Per-run mutable state and cooperative stop.
* [Staging and watermarks](/patterns/staging-watermark.md) - Retry safety and commit boundaries.
* [State and idempotency](/patterns/state-idempotency.md) - State machine, uniqueness, and retry rows.
* [API mapping and errors](/patterns/api-mapping-errors.md) - DTO boundaries and RFC 7807.
* [Frontend query state](/patterns/frontend-query-state.md) - Queries, mutations, routes, and forms.
* [Security redaction](/patterns/security-redaction.md) - Configuration references and output hygiene.
* [Testing and fixtures](/patterns/testing-fixtures.md) - Unit, integration, container, and browser tests.
* [Anti-patterns](/patterns/anti-patterns.md) - Practices the project explicitly rejects.

# Decisions
* [Two artifacts](/decisions/two-artifacts.md) - Preserve CLI compatibility while adding the server.
* [Monolithic control plane](/decisions/monolithic-control-plane.md) - Start with one API and scheduler runtime.
* [No resume and watermarks](/decisions/no-resume-watermarks.md) - Retry safety through modes and commit points.
* [Immediate cancellation](/decisions/immediate-cancellation.md) - Stop quickly and persist sink-risk warnings.
* [PostgreSQL state](/decisions/postgresql-state.md) - Durable control-plane source of truth.
* [Generated API contract](/decisions/generated-api-contract.md) - OpenAPI is the frontend boundary.
* [Frontend administration boundary](/decisions/frontend-admin-boundary.md) - Admin UX over backend ACL contracts.

# Recent Learnings
* [Gap recurrence](/learnings/gap-recurrence.md) - Repeated planning risks suitable for shared rules.
* [Distributed state contract](/learnings/phase31-compatibility-bridge.md) - Lease fencing, database time, staged migrations, and compatibility bridges.
* [Frontend administration gaps](/learnings/phase2c-custom-form-validation.md) - Validation and route-context lessons from the current admin slice.
* [API contract gaps](/learnings/phase2a-openapi-nullability.md) - Serialize and validate the actual wire format.
* [Technical debt](/learnings/tech-debt.md) - Known limitations and deferred capabilities.
* [Dependency upgrade compatibility](/learnings/dependency-upgrade-compatibility.md) - Validate APIs, bytecode, and coordinated dependency families before major bumps.
* [Dependency CI gates](/learnings/dependency-ci-gates.md) - Merge automation requires successful CI for the exact current PR head.
* [Frontend E2E fixture isolation](/learnings/frontend-e2e-fixture-isolation.md) - Keep browser fixtures environment-aware and safe under parallel workers.
