---
applyTo: '**/*.java'
---

# ReplicaDB Java Rules

## Architecture and Package Structure
- Keep CLI parsing in `org.replicadb.cli`, core orchestration in `org.replicadb`, and managed-server responsibilities in their existing `job`, `security`, and `audit` packages.
- Keep managed run policy and state transitions in `job.domain`, use-case contracts and services in `job.port`/`job.application`, and SQL/time/locking behavior in `job.persistence`. Controllers and execution adapters must depend on the ports where a port exists.
- Put vendor SQL, type mapping, partitioning, and native bulk behavior in the nearest manager subclass. Keep shared lifecycle behavior in `ConnManager` or `SqlManager`.
- Register new schemes through `SupportedManagers` and `ManagerFactory`; use `FileManagerFactory` for file formats.
- Keep controllers and server services translating into `ToolOptions`; they must not reimplement manager behavior.

## Base Patterns and Abstractions
- Preserve manager lifecycle hooks for pre-source, pre-sink, post-source, post-sink, cleanup, and release operations.
- Adapt non-JDBC sources through row-set implementations so sink code can keep its JDBC-shaped contract.
- Prefer an existing native path for a supported manager before adding a generic fallback or a new abstraction.
- Use `JobRunStore`, `JobDefinitionStore`, `RunLeaseService`, `RunRecoveryService`, `RunCancellationService`, and `RunFinalizationService` for managed state operations instead of adding SQL-shaped behavior to controllers.
- Keep retry policy validation in `RetryPolicy`; keep lease identity in `LeaseToken` and never add it to REST or frontend models.

## Error Handling and Invariants
- Keep source and sink resources task-owned and close them through existing lifecycle paths, including executor failure paths.
- Preserve causes and task context when propagating failures; restore the interrupted flag after `InterruptedException`.
- Validate null handling, precision, temporal values, LOBs, and staging ownership at the manager boundary. Do not drop user-defined staging resources.
- Keep server errors in RFC 7807 form and redact dynamic details before API, audit, log, or telemetry output.
- Treat a zero-row token-checked update as an explicit `FENCED` or `NOT_FOUND` result. Do not emit terminal audit success or advance a watermark unless the durable update returns `UPDATED`.
- Persist cancellation intent before attempting the best-effort local execution signal.

## Configuration and Observability
- Read runtime behavior from `ToolOptions`; do not duplicate parsing or environment expansion inside managers.
- Keep Log4j2 alignment, Java 17, ORC module-opening flags, and packaged launchers in sync when changing runtime configuration.
- Treat PostgreSQL and Flyway as the managed state-store boundary; do not substitute in-memory production state.
- Keep `available_at`, lease timestamps, and expiry backoff comparisons database-owned with PostgreSQL `now()`; keep V13/V14 forward-only and update staged migration assertions when changing them.
- Never put passwords, DSNs, connection parameter maps, or resolved credential values in logs, Sentry context, exceptions, or test output.

## Concurrency and Resilience
- Give each run a `ReplicationExecutionContext`; do not share mutable JDBC connections or manager state across tasks.
- Preserve cooperative cancellation and active-statement cancellation; classify driver SQL exceptions as cancellation when the run token is set.
- Keep manager-specific partition and retry behavior explicit. Do not generalize a hash, offset, cursor, or bulk strategy from one manager.
- Claim eligible runs with the shared lease contract and `FOR UPDATE SKIP LOCKED`; do not hold a database lock while running ReplicaDB. Expiry recovery creates a new attempt from the beginning and does not resume an abandoned run.

## Anti-Patterns
- Do not add vendor branches to `ReplicaDB` or `SqlManager` when a manager override is the extension point.
- Do not copy legacy JUnit 4 patterns into new Java tests.
- Do not compare JVM-created compatibility timestamps with database-owned eligibility predicates, expose lease tokens, or remove the deprecated repository bridge before all production callers have moved to the port.

## Contradiction Check
⚠️ Baseline unavailable: `inditex.instructions.md` and `amiga-*.instructions.md` were not present, and the AMIGA documentation search was unavailable. No project override was recorded; copy the baseline files before the next context regeneration.
