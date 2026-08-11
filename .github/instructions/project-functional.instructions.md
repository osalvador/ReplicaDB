---
applyTo: '**/*.{java,properties,conf}'
---

# ReplicaDB Functional Rules

## Replication Contract
- Preserve the CLI and options-file contract when changing behavior. Add flags and properties compatibly; do not silently rename or remove existing options.
- Keep ReplicaDB focused on point-to-point bulk transfer. Scheduling, complex transformations, CDC, and data quality belong to external tools unless the request explicitly changes scope.
- Preserve source-to-sink data meaning, precision, nullability, and supported type behavior. Fail explicitly when a conversion or capability is not supported.

## Mode Semantics
- Treat `complete`, `incremental`, and `complete-atomic` as distinct user-visible contracts.
- Complete loads the sink table; incremental and complete-atomic may use staging and manager-specific merge/cleanup behavior.
- Require primary-key and staging assumptions only where the concrete sink manager needs them, and document unsupported combinations in the capability matrix.

## Capability Boundaries
- Do not generalize one manager's behavior to every source or sink. Check `SupportedManagers`, `ManagerFactory`, the concrete manager, and the README matrix before changing a capability.
- Keep file, MongoDB, S3, and Kafka semantics explicit because they do not share SQL table or staging behavior.
- Treat `ARCHITECTURE_DECISIONS.md` as future evolution guidance. Do not describe Spring Boot, REST, WebSocket, Quartz, Redis, or Kubernetes as implemented unless source code and tests support it.

## Configuration and Security
- Keep credentials in environment-expanded configuration and avoid exposing them in command history, logs, telemetry, tests, or generated documentation.
- Preserve `ToolOptions` defaults and the options-file-to-command-line precedence when adding configuration.
- Treat user-supplied table names, column expressions, filters, queries, and connection parameters as untrusted input at the manager boundary.

## Anti-Patterns
- Do not claim universal support from a single database test or manager implementation.
- Do not make a Java-version change in only one of Maven, CI, launchers, container images, or written requirements.
- Do not add application code while regenerating AI context or project instructions.

## Contradiction Check
WARNING: `inditex.instructions.md` and `amiga-java.instructions.md` were not present in `.github/instructions/`, so no comparison against the organization or AMIGA baseline was possible. Copy those baseline files before using this project-specific file as a complete policy set.
