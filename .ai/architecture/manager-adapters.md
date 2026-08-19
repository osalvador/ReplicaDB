---
type: Adapter
description: Manager factories dispatch configured source and sink schemes to database, file, object-storage, and Kafka adapters.
sources:
  - id: factory
    resource: src/main/java/org/replicadb/manager/ManagerFactory.java
  - id: supported
    resource: src/main/java/org/replicadb/manager/SupportedManagers.java
  - id: connection
    resource: src/main/java/org/replicadb/manager/ConnManager.java
  - id: sql
    resource: src/main/java/org/replicadb/manager/SqlManager.java
  - id: files
    resource: src/main/java/org/replicadb/manager/file/FileManagerFactory.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`ManagerFactory` extracts a connection scheme and selects specialized managers in an ordered chain. Observed adapters include PostgreSQL, Oracle, SQL Server, MySQL/MariaDB, SQLite, MongoDB, DB2, Denodo, S3, Kafka, and local files, with `StandardJDBCManager` as a fallback. `FileManagerFactory` dispatches file formats separately.

`ConnManager` owns the shared connection and lifecycle contract. `SqlManager` supplies common SQL behavior, while concrete managers own dialect-specific SQL, partitioning, type mappings, native bulk paths, and staging details. Examples include PostgreSQL binary `COPY`, SQL Server bulk-copy adapters, MongoDB bulk writes, and file row-set production.

The number of configured jobs controls parallel task count, but partition expressions and bind shapes remain manager-specific. New support should extend the nearest adapter and register the scheme rather than add vendor branches to the orchestrator.

Reference implementations: `src/main/java/org/replicadb/manager/ManagerFactory.java`, `src/main/java/org/replicadb/manager/PostgresqlManager.java`, and `src/main/java/org/replicadb/manager/file/FileManagerFactory.java`.
