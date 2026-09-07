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

## Staging and security

Complete-atomic and incremental sink runs use staging before replacement or
merge. Set `sink.staging.schema` when generated staging tables should live in a
dedicated schema. Keep wallet, TLS, and user values in the runtime environment;
do not put resolved connection security in an options file committed to source
control.