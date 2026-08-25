---
type: Pattern
description: Tests combine focused unit seams, real database containers, server lifecycle clients, generated-schema checks, and browser smoke flows.
sources:
  - id: core-tests
    resource: src/test/java/org/replicadb
  - id: server-tests
    resource: replicadb-server/src/test/java/org/replicadb/server
  - id: frontend-tests
    resource: replicadb-server/frontend/src
  - id: e2e
    resource: replicadb-server/frontend/e2e
  - id: config
    resource: replicadb-server/src/test/java/org/replicadb/server/config/PostgresTestcontainersConfig.java
  - id: distributed-state
    resource: replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java
  - id: migration-staging
    resource: replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java
  - id: process-harness
    resource: scripts/phase3-multinode-test.sh
  - id: resilience-harness
    resource: scripts/phase3-resilience-test.sh
  - id: documentation-gate
    resource: scripts/check-phase3-docs.sh
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

Core tests use inline options and manager seams, Mockito for narrow JDBC behavior, and Testcontainers for driver/dialect integration. Server tests mix domain/unit tests, MockMvc controller tests, Spring JDBC repository integration, Flyway assertions, and real-port session/CSRF lifecycle tests. Frontend tests use Vitest, Testing Library, Axios mocks, fresh query clients, and route context; Playwright covers built-asset browser flows.

Shared container scope, architecture emulation, port readiness, fixture mutation, and environment-managed browser credentials are treated as separate validation concerns. Distributed-state tests use real PostgreSQL connections for `SKIP LOCKED`, PostgreSQL time, recovery backoff, lease fencing, Quartz, and migration indexes; Flyway staging targets explicit versions. Process harnesses validate the packaged server image with Compose healthchecks, explicit project names, dynamic API ports, database-observable barriers, and cleanup traps. CI-invoked shell checks use baseline POSIX tools and avoid quiet pipeline consumers under `pipefail`. Focused test class lists are preferred when broad selectors expand into unrelated integration suites.

Reference implementations: `src/test/java/org/replicadb`, `replicadb-server/src/test/java/org/replicadb/server`, and `replicadb-server/frontend/e2e`.
