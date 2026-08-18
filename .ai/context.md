## Service Description
ReplicaDB is a Java bulk-replication engine that transfers data between heterogeneous databases, files, object storage, and Kafka. The repository preserves a standalone CLI while adding a sibling Spring Boot control plane for durable job definitions, scheduling, monitoring, authentication, and audit history, plus a React monitoring SPA.

## Tech Stack
Java 17/Maven, JDBC and vendor SDKs, Spring Boot 3.3.5, PostgreSQL/Flyway, Quartz, React 18/TypeScript/Vite, Log4j2/Sentry, JUnit Jupiter 6, Vitest, Playwright, and Testcontainers. Deploy: CLI archive or managed server/container.

## Project Structure
| Layer | Key Packages | Key Patterns |
| --- | --- | --- |
| Core execution | `org.replicadb`, `org.replicadb.execution` | per-run context, fixed task pool, cancellable futures |
| CLI and adapters | `org.replicadb.cli`, `manager`, `rowset` | options boundary, manager factories, JDBC-shaped transfer |
| Managed server | `replicadb-server/.../job`, `security`, `audit` | immutable records, JDBC repositories, REST, ACLs, Quartz |
| Frontend | `replicadb-server/frontend/src` | React Router, TanStack Query, generated OpenAPI types |
| Delivery and tests | `src/test`, server tests, workflows, Docker, `docs` | fixture containers, Maven/npm build, Jekyll/Vite tooling |

## Key Decisions
- **Two artifacts**: keep the CLI free of Spring Boot while the server translates stored jobs into core `ToolOptions`.
- **JDBC-shaped transfer**: adapt files and documents to row-set contracts so sink behavior stays reusable.
- **Explicit modes and no resume**: staging/merge semantics provide retry safety; a managed job is one table pair.
- **PostgreSQL state**: the managed server stores jobs, runs, schedules, users, permissions, and audit events durably.

## Anti-Patterns
- Do not move vendor SQL, type mapping, or native bulk behavior into generic orchestration or server code.
- Do not infer universal mode/capability support from one manager or test.
- Do not persist, log, audit, or generate context containing passwords, DSNs, tokens, or resolved secret values.

## Key Conventions
- Preserve the CLI/options-file contract; `ToolOptions` remains the core configuration boundary.
- Use manager and file factories, with `DataSourceType.SOURCE`/`SINK` and manager-specific partitioning.
- Keep cancellation, watermarks, staging cleanup, and state transitions explicit at their owning boundary.
- Use lower-case mode text at the REST boundary and RFC 7807 problem responses.
- Use JUnit Jupiter 6/Testcontainers for Java, Vitest/Playwright for the SPA, and environment-managed credentials.

## Recent Changes
- `5abc156`: added Phase 2a React authentication and read-only monitoring.
- `a7b9225` / `56f9243`: added persisted audit events, cancellation warnings, authentication, and ACLs.
- `8d12cdc`: added durable Quartz scheduling and startup reconciliation.
- `24bec4a` / `c897181`: added REST API and PostgreSQL-backed job/run state.
- `2668d99` / `f6535af` / `472adb3`: stabilized OpenAPI schema and frontend CI packaging.

## Recent Learnings
- [WARNING] API: keep framework implementation types out of generated OpenAPI contracts and validate nullability against JSON.
- [WARNING] Frontend: never commit environment-specific registry URLs; validate `npm ci` in a clean runner-like environment.
- [WARNING] Runtime: exercise packaged ORC and image paths, and distinguish CI Docker health from local emulation/reuse limits.

-> Pointers: `.ai/context/domain.md`, `.ai/context/execution.md`, `.ai/context/cli.md`, `.ai/context/managers.md`, `.ai/context/rowsets.md`, `.ai/context/infrastructure.md`, `.ai/context/api.md`, `.ai/context/frontend.md`, `.ai/context/testing.md`, `.ai/context/operations.md`, `.ai/context/recent-changes.md`
