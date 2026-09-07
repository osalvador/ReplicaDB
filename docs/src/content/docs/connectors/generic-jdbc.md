---
title: Generic JDBC and legacy managers
description: Fallback JDBC schemes and their intentionally narrow capability contract.
---

# Generic JDBC and legacy managers

ReplicaDB includes explicit legacy manager schemes for HSQLDB, CUBRID, jTDS SQL
Server, and Netezza, plus a Standard JDBC fallback for other JDBC-compatible
schemes. The explicit legacy managers support complete source and sink
replication, are single-job only, and do not claim complete-atomic or
incremental behavior.

Use the connector's driver class through the relevant
`source.connect.parameter.*` or `sink.connect.parameter.*` option. Generic JDBC
compatibility does not imply vendor-specific bulk loading, partitioning,
staging, type conversion, or security support. Validate the full table shape
and permissions with a disposable sink before production use.

The fallback uses ordinary JDBC result-set reads and prepared-statement writes.
It can execute a source query, but the maintained legacy capability remains
complete mode with one job. Supply the driver JAR in the CLI `lib` directory,
confirm that the driver class is loadable, and verify BLOB, CLOB, SQLXML,
temporal, and binary mappings for the exact driver version.
