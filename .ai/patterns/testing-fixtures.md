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
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Core tests use inline options and manager seams, Mockito for narrow JDBC behavior, and Testcontainers for driver/dialect integration. Server tests mix domain/unit tests, MockMvc controller tests, Spring JDBC repository integration, Flyway assertions, and real-port session/CSRF lifecycle tests. Frontend tests use Vitest, Testing Library, Axios mocks, fresh query clients, and route context; Playwright covers built-asset browser flows.

Shared container scope, architecture emulation, port readiness, fixture mutation, and environment-managed browser credentials are treated as separate validation concerns. Focused test class lists are preferred when broad selectors expand into unrelated integration suites.

Reference implementations: `src/test/java/org/replicadb`, `replicadb-server/src/test/java/org/replicadb/server`, and `replicadb-server/frontend/e2e`.
