---
type: Pattern
description: Ordered manager factories isolate scheme detection and keep database-specific behavior in adapters.
sources:
  - id: factory
    resource: src/main/java/org/replicadb/manager/ManagerFactory.java
  - id: supported
    resource: src/main/java/org/replicadb/manager/SupportedManagers.java
  - id: files
    resource: src/main/java/org/replicadb/manager/file/FileManagerFactory.java
  - id: managers
    resource: src/main/java/org/replicadb/manager/PostgresqlManager.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The core uses ordered dispatch: factories inspect the configured source or sink and return the first suitable `ConnManager` or file manager. Specialized managers override dialect, type, partition, native bulk, and lifecycle behavior; `StandardJDBCManager` is the fallback for an unrecognized JDBC scheme.

When adding a manager, update the supported-scheme registry, dispatch path, and the closest capability/type tests. Keep vendor conditions out of generic orchestration unless they describe a shared contract.

Reference implementations: `ManagerFactory.java`, `SupportedManagers.java`, and `FileManagerFactory.java`.
