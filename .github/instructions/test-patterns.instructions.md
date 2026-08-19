---
applyTo: '**/*Test.java,**/*IT.java,replicadb-server/frontend/**/*.{test,spec}.{ts,tsx}'
---

# ReplicaDB Testing Rules

## Conventions
- Use JUnit Jupiter 6 for new Java tests and keep legacy JUnit 4 code isolated. Use Mockito for narrow JDBC or lifecycle seams and real databases for dialect, driver, cursor, type, and transaction behavior.
- Keep fixtures under the existing test-resource and `Replicadb*Container` conventions. Follow the nearest package and fixture family rather than inventing a database-labelled test path.
- Use MockMvc plus security test support for isolated server behavior; use a real HTTP client with session and CSRF cookies for lifecycle authentication.
- Use Vitest and Testing Library for SPA behavior, a fresh TanStack Query client per test, matching React Router routes for parameterized pages, and Playwright for real cookie/session flows.

## Modification Strategy
- When options or signatures change, update inline `ToolOptions` setups, options-file properties, migrations, DTOs, generated OpenAPI assertions, and serialized-nullability tests together.
- For manager changes, cover supported mode semantics, nulls, type boundaries, and partitioning without claiming compatibility from mocks alone.
- For migrations and state transitions, update exact migration-count and constraint assertions, including multi-row retry transitions.
- Prefer read-only assertions against JVM-wide singleton fixtures. Use explicit transactions for large fixture setup and wait for externally reachable container ports.
- Keep authenticated E2E credentials environment-managed; report missing configuration separately from product failures.

## Anti-Patterns
- Do not use mocked databases to claim dialect compatibility.
- Do not use broad Surefire wildcards before their expansion is known; select explicit classes for focused validation.
- Do not mutate shared Testcontainers state without an isolated schema or cleanup boundary.
- Do not put credentials, real endpoints, or resolved secrets in fixtures or test output.

## Contradiction Check
⚠️ Baseline unavailable: `inditex.instructions.md` and `amiga-*.instructions.md` were not present, and the AMIGA documentation search was unavailable. No project override was recorded; copy the baseline files before the next context regeneration.
