---
title: Oracle connector
description: Oracle source and sink behavior, partitioning, staging, and security.
---

# Oracle

Use the `jdbc:oracle:` scheme. Oracle is supported as both source and sink for
`complete`, `complete-atomic`, and `incremental` replication.

## Connection and execution

Use an environment-substituted connection string and select a table or query.
Oracle-specific partitioning and Flashback behavior depend on database
permissions and the chosen read path. LOB and temporal types should be tested
with representative values before a large transfer.

A service-name connection commonly follows
`jdbc:oracle:thin:@//database-host:1521/service-name`; keep the actual host,
service, wallet, and account outside committed examples. Table-backed reads
can partition work with Oracle row identifiers. A free-form `source.query`
owns its own selection and may not provide the same partitioning behavior.

ReplicaDB captures an Oracle system change number when Flashback is available
so parallel reads can share a consistent source point. The source account
needs the corresponding Flashback privileges, and undo retention must cover
the read. If the SCN is outside the retention window, correct retention or
reduce the run window rather than treating the result as a complete snapshot.

## Staging and security

Complete-atomic and incremental sink runs use staging before replacement or
merge. Set `sink.staging.schema` when generated staging tables should live in a
dedicated schema. Keep wallet, TLS, and user values in the runtime environment;
do not put resolved connection security in an options file committed to source
control.

BLOB and CLOB values are streamed to avoid materializing an entire value in
memory, but cross-version LOB, temporal, XML, and vendor-specific values still
need a representative compatibility run. Grant create/drop rights for generated
staging objects or manage a fixed staging table explicitly.
