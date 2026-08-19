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
- Check `SupportedManagers`, `ManagerFactory`, the concrete manager, and maintained capability documentation before generalizing support.
- Keep file, MongoDB, S3, Kafka, and SQL semantics explicit; they do not share the same table or staging guarantees.

## Configuration and Security
- Preserve `ToolOptions` defaults and options-file precedence when adding configuration.
- Treat table names, expressions, filters, queries, and connection parameters as untrusted manager inputs.
- Store configuration references such as `${env:VARIABLE}` in managed definitions and reject embedded credentials in connection strings.
- Keep backend ACLs authoritative. Frontend visibility is a usability aid, not authorization.

## Anti-Patterns
- Do not infer universal support from one manager or one database test.
- Do not change the Java baseline in only one build, launcher, image, CI, or documentation surface.
- Do not add application code while regenerating context or project instructions.

## Contradiction Check
⚠️ Baseline unavailable: `inditex.instructions.md` and `amiga-*.instructions.md` were not present, and the AMIGA documentation search was unavailable. No project override was recorded; copy the baseline files before the next context regeneration.
