---
applyTo: '**/*Test.java,**/*IT.java'
---

# ReplicaDB Testing Rules

## Conventions
- Use JUnit Jupiter 6 annotations and assertions with the repository's Surefire 3.5.3 configuration. Do not add new JUnit 4 tests.
- Build manager tests with inline `ToolOptions` argument arrays unless a shared test helper already exists for the exact concern.
- Use Mockito for isolated JDBC metadata, statement, and transaction behavior; use real databases for driver, dialect, cursor, type, and transaction integration.
- Keep database fixtures under `src/test/resources` and load them through the existing container or script-runner setup.

## Testcontainers and Fixtures
- Follow the existing one-container-per-database-family singleton pattern when adding integration coverage. Keep initialization and fixture loading in the corresponding `Replicadb*Container` class.
- Reset reused collections/tables or disable reuse for CI so tests do not depend on order or retained state.
- Select the smallest relevant database package or test class before running a full integration matrix. Docker architecture, socket, reuse, and memory failures must be distinguished from assertion failures.

## Modification Strategy
- When a production option or signature changes, update every affected inline `ToolOptions` setup and the corresponding properties/SQL/JSON fixture. Do not duplicate a second builder or fixture convention.
- For manager changes, cover complete, incremental, and complete-atomic behavior only where supported, plus null values, empty and single-row inputs, type boundaries, and parallel partitioning as applicable.
- For Java 17 or dependency changes, run test compilation, the focused unit slice, packaged/runtime checks, and the relevant Testcontainers slice.

## Anti-Patterns
- Do not use a mocked database to claim JDBC dialect compatibility.
- Do not copy `ReplicaDBTest.java`'s legacy JUnit 4 imports into new tests.
- Do not leave shared container state, credentials, or real endpoints in test output or committed fixtures.

## Contradiction Check
WARNING: `inditex.instructions.md` and `amiga-java.instructions.md` were not present in `.github/instructions/`, so no comparison against the organization or AMIGA baseline was possible. Copy those baseline files before using this project-specific file as a complete policy set.
