---
type: CLI Interface
description: The standalone command-line and options-file contract configures one or more core replications through ToolOptions.
sources:
  - id: options
    resource: src/main/java/org/replicadb/cli/ToolOptions.java
  - id: options-file
    resource: src/main/java/org/replicadb/cli/OptionsFile.java
  - id: mode
    resource: src/main/java/org/replicadb/cli/ReplicationMode.java
  - id: launcher
    resource: bin/replicadb
  - id: docs
    resource: README.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`ToolOptions` is the core configuration boundary. It accepts command-line arguments and an options file, preserves defaults and precedence, and carries source/sink connection, table/query, mode, staging, authentication, throttling, and execution settings. `ReplicationMode` exposes the user-facing lower-case mode values `complete`, `incremental`, and `complete-atomic`.

The CLI supports a list of `ReplicationTable` entries for sequential multi-table execution. The managed API intentionally does not reuse that multi-table state model. Launchers and the options-file property loader are part of the compatibility surface; new options must be wired through both paths when both are advertised.

Credentials are runtime configuration. Documentation and generated context must refer to environment-managed values without copying resolved secrets.

Reference implementations: `src/main/java/org/replicadb/cli/ToolOptions.java`, `OptionsFile.java`, and `bin/replicadb`.
