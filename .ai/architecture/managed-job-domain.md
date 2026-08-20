---
type: Domain Model
description: Managed job definitions capture one source/sink table pair, mode, execution settings, and configuration references.
sources:
  - id: definition
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/JobDefinition.java
  - id: source
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/SourceEndpoint.java
  - id: sink
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/SinkEndpoint.java
  - id: credentials
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/ConnectionCredentials.java
  - id: retry-policy
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/RetryPolicy.java
  - id: decisions
    resource: ARCHITECTURE_DECISIONS.md
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

`JobDefinition` is an immutable record with nested `SourceEndpoint` and `SinkEndpoint` values. It validates a nonblank name, non-null endpoints and mode, positive job/fetch settings, nonnegative throttling, and the rule that an incremental watermark column requires incremental mode. The source endpoint can represent a table or query; the API and persistence layers enforce the current table-or-query contract.

The public modes are `complete`, `incremental`, and `complete-atomic`. Their sink safety and merge behavior differ by manager. `RetryPolicy` belongs to the definition and validates total attempts and direct backoff seconds. Defaults are three attempts and 60 seconds, with automatic lease-expiry retry enabled for `incremental` and `complete-atomic` and disabled for `complete`; a complete job may opt in but keeps its destructive warning. Managed retries rerun the definition from the beginning. Connection passwords and credential-bearing connection strings are represented by environment references and resolved only immediately before core execution.

The managed model deliberately represents one table pair. Multi-table replication remains a CLI feature because a single managed run cannot accurately model partial application across several tables.

Reference implementations: `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobDefinition.java`, `SourceEndpoint.java`, and `SinkEndpoint.java`.
