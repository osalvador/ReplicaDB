---
applyTo: '**/*.{java,properties,conf,yml,yaml}'
---

# ReplicaDB Functional Rules

## Replication Contract
- Preserve existing CLI arguments, options-file keys, defaults, precedence, exit codes, and multi-table behavior when changing the core.
- Keep the product focused on point-to-point batch transfer. CDC, complex transformations, and data quality remain outside the current managed scope unless explicitly added.
- Preserve source-to-sink meaning, precision, nullability, and unsupported-conversion failures.
- Keep the root CLI artifact free of Spring Boot dependencies. The managed server translates a stored single-table job into `ToolOptions`.

## Mode and Capability Semantics
- Treat `complete`, `incremental`, and `complete-atomic` as separate user-visible contracts with manager-specific staging and merge behavior.
- Managed retries re-execute from the beginning. Commit an incremental watermark only after a successful sink merge, and do not advance it on failure or cancellation.
- A managed retry policy counts the initial attempt: default to `maxAttempts=3` and `retryBackoffSeconds=60`; automatic lease-expiry retry defaults on for `complete-atomic` and `incremental`, and off for `complete` unless explicitly enabled.
- Lease-expiry recovery changes the abandoned run to history and creates a new pending attempt with `previousRunId`; it is never resume semantics. Eligibility and backoff use PostgreSQL time, and stale workers cannot write through a different lease token.
- Check `SupportedManagers`, `ManagerFactory`, the concrete manager, and maintained capability documentation before generalizing support.
- Keep file, MongoDB, S3, Kafka, and SQL semantics explicit; they do not share the same table or staging guarantees.

## Configuration and Security
- Preserve `ToolOptions` defaults and options-file precedence when adding configuration.
- Treat table names, expressions, filters, queries, and connection parameters as untrusted manager inputs.
- Store configuration references such as `${env:VARIABLE}` in managed definitions and reject embedded credentials in connection strings.
- Keep backend ACLs authoritative. Frontend visibility is a usability aid, not authorization.
- Keep cancellation warnings mode-specific and durable; local in-memory cancellation delivery is only an optimization.

## Anti-Patterns
- Do not infer universal support from one manager or one database test.
- Do not change the Java baseline in only one build, launcher, image, CI, or documentation surface.
- Do not add application code while regenerating context or project instructions.
- Do not describe the Phase 3.1 contract as a deployed worker runtime: `worker`, `LISTEN/NOTIFY`, polling dispatch, heartbeat renewal, and Quartz clustering remain deferred.

## Contradiction Check
No organization baseline was available in this checkout, so no contradiction or project override was recorded.
