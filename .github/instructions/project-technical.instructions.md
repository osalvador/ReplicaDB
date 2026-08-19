---
applyTo: '**/*.java'
---

# ReplicaDB Java Rules

## Architecture and Package Structure
- Keep CLI parsing in `org.replicadb.cli`, core orchestration in `org.replicadb`, and managed-server responsibilities in their existing `job`, `security`, and `audit` packages.
- Put vendor SQL, type mapping, partitioning, and native bulk behavior in the nearest manager subclass. Keep shared lifecycle behavior in `ConnManager` or `SqlManager`.
- Register new schemes through `SupportedManagers` and `ManagerFactory`; use `FileManagerFactory` for file formats.
- Keep controllers and server services translating into `ToolOptions`; they must not reimplement manager behavior.

## Base Patterns and Abstractions
- Preserve manager lifecycle hooks for pre-source, pre-sink, post-source, post-sink, cleanup, and release operations.
- Adapt non-JDBC sources through row-set implementations so sink code can keep its JDBC-shaped contract.
- Prefer an existing native path for a supported manager before adding a generic fallback or a new abstraction.

## Error Handling and Invariants
- Keep source and sink resources task-owned and close them through existing lifecycle paths, including executor failure paths.
- Preserve causes and task context when propagating failures; restore the interrupted flag after `InterruptedException`.
- Validate null handling, precision, temporal values, LOBs, and staging ownership at the manager boundary. Do not drop user-defined staging resources.
- Keep server errors in RFC 7807 form and redact dynamic details before API, audit, log, or telemetry output.

## Configuration and Observability
- Read runtime behavior from `ToolOptions`; do not duplicate parsing or environment expansion inside managers.
- Keep Log4j2 alignment, Java 17, ORC module-opening flags, and packaged launchers in sync when changing runtime configuration.
- Treat PostgreSQL and Flyway as the managed state-store boundary; do not substitute in-memory production state.
- Never put passwords, DSNs, connection parameter maps, or resolved credential values in logs, Sentry context, exceptions, or test output.

## Concurrency and Resilience
- Give each run a `ReplicationExecutionContext`; do not share mutable JDBC connections or manager state across tasks.
- Preserve cooperative cancellation and active-statement cancellation; classify driver SQL exceptions as cancellation when the run token is set.
- Keep manager-specific partition and retry behavior explicit. Do not generalize a hash, offset, cursor, or bulk strategy from one manager.

## Anti-Patterns
- Do not add vendor branches to `ReplicaDB` or `SqlManager` when a manager override is the extension point.
- Do not copy legacy JUnit 4 patterns into new Java tests.

## Contradiction Check
⚠️ Baseline unavailable: `inditex.instructions.md` and `amiga-*.instructions.md` were not present, and the AMIGA documentation search was unavailable. No project override was recorded; copy the baseline files before the next context regeneration.
