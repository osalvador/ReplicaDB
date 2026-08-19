---
type: Port
description: JDBC-shaped row-set contracts let non-JDBC sources reuse sink transfer behavior.
sources:
  - id: base
    resource: src/main/java/org/replicadb/rowset/ReplicaRowSetBase.java
  - id: provider
    resource: src/main/java/org/replicadb/rowset/ReplicaRowSetProvider.java
  - id: csv
    resource: src/main/java/org/replicadb/rowset/CsvCachedRowSetImpl.java
  - id: mongo
    resource: src/main/java/org/replicadb/rowset/MongoDBRowSetImpl.java
  - id: orc
    resource: src/main/java/org/replicadb/rowset/OrcCachedRowSetImpl.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`ReplicaRowSetBase` and related metadata/provider types define the transfer surface expected by sink managers. CSV, MongoDB, ORC, and streaming implementations adapt their source representation into JDBC-shaped cursor and metadata behavior. This keeps sink lifecycle, column mapping, null handling, and batching reusable without teaching `ReplicaDB` about every source format.

The port carries source metadata needed for type-aware writes. Implementations must preserve value meaning and nullability where the source format can represent them; unsupported conversions should remain explicit failures. Row-set resource bundles and provider factories belong beside the row-set implementation.

Reference implementations: `src/main/java/org/replicadb/rowset/ReplicaRowSetBase.java`, `src/main/java/org/replicadb/rowset/CsvCachedRowSetImpl.java`, and `src/main/java/org/replicadb/rowset/MongoDBRowSetImpl.java`.
