---
type: Pattern
description: Row-set adapters present non-JDBC sources through a JDBC-shaped cursor and metadata contract.
sources:
  - id: base
    resource: src/main/java/org/replicadb/rowset/ReplicaRowSetBase.java
  - id: csv
    resource: src/main/java/org/replicadb/rowset/CsvCachedRowSetImpl.java
  - id: mongo
    resource: src/main/java/org/replicadb/rowset/MongoDBRowSetImpl.java
  - id: sink
    resource: src/main/java/org/replicadb/manager/SqlManager.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

CSV, MongoDB, ORC, and streaming sources implement or extend the row-set surface so SQL-oriented sink code can reuse column descriptors, null handling, and batching. The adapter owns format-specific extraction while the transfer pipeline retains a common sink-facing contract.

Preserve metadata and value semantics at this boundary. Add format-specific behavior to the row-set or manager adapter rather than adding source-format branches to `ReplicaDB`.

Reference implementations: `ReplicaRowSetBase.java`, `CsvCachedRowSetImpl.java`, `MongoDBRowSetImpl.java`, and `SqlManager.java`.
