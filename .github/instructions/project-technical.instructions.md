---
applyTo: '**/*.java'
---

# ReplicaDB Java Rules

## Architecture and Package Structure
- Keep orchestration in `org.replicadb.ReplicaDB` and `ReplicaTask`; keep CLI parsing in `org.replicadb.cli`.
- Put database-specific SQL, JDBC type mapping, partitioning, and native bulk behavior in the nearest manager subclass. Keep generic connection and SQL behavior in `ConnManager`/`SqlManager`.
- Register new connection schemes through `SupportedManagers` and `ManagerFactory`. Use `FileManagerFactory` for file-format dispatch.
- Keep the transfer boundary JDBC-shaped. Adapt non-JDBC inputs through row-set implementations instead of teaching the orchestrator about every source format.

## Manager and Transfer Patterns
- Extend `SqlManager` for SQL/JDBC sources and sinks; extend `ConnManager` directly only when the source does not fit the generic SQL lifecycle.
- Preserve the manager hooks for pre-source, pre-sink, post-source, post-sink, cleanup, and release operations.
- Prefer an existing native path when one exists: PostgreSQL `COPY`, MySQL/MariaDB `LOAD DATA`, SQL Server `SQLServerBulkCopy`, MongoDB unordered bulk writes, or the generic JDBC batch fallback.
- Keep partition expressions manager-specific. The `jobs` value controls task count, but no single hash, offset, or cursor strategy is universal.

## Transactions and Error Handling
- SQL managers use explicit transactions with auto-commit disabled. Commit only after a successful batch or native bulk operation; roll back on failure.
- Source and sink connections are task-owned. Close statements, result sets, connections, SDK clients, and temporary resources through the existing lifecycle hooks.
- Preserve the original cause and task context when propagating failures. Restore the interrupted flag after catching `InterruptedException`.
- Do not assume the executor cancels sibling tasks when one `Future` fails; preserve cleanup behavior in the orchestrator.

## Domain Invariants and Type Mapping
- Check `ResultSet.wasNull()` immediately after primitive getters and check object values for null before binding or serializing them.
- Preserve numeric precision, temporal semantics, LOB handling, and source metadata. Add manager-specific mappings rather than weakening the generic type contract.
- Protect user-defined staging tables from automatic cleanup. Only generated staging resources may be dropped by cleanup logic.

## Configuration, Logging, and Observability
- Read runtime behavior from `ToolOptions`; do not duplicate parsing or environment expansion in managers.
- Use `TaskId-*` thread names and the existing Log4j2 levels for parallel operation correlation.
- Never add passwords, DSNs, connection parameter maps, or credential-bearing URLs to logs, Sentry contexts, tags, exception messages, or test output.
- Keep the Java 17 baseline and the ORC `java.nio` module-opening requirement aligned across Maven and packaged launchers.

## Anti-Patterns
- Do not put vendor branches in `ReplicaDB` or `SqlManager` when a manager override is the correct extension point.
- Do not share mutable JDBC connections across tasks or introduce connection pooling as an incidental refactor.
- Do not copy the legacy JUnit 4 style from `ReplicaDBTest.java` into new Java tests.
