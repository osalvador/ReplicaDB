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