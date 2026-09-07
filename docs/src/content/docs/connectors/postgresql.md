---
title: PostgreSQL connector
description: PostgreSQL source and sink behavior including COPY and staging.
---

# PostgreSQL

Use `jdbc:postgresql:` for source or sink connections. PostgreSQL supports all
three replication modes and bandwidth throttling.

The sink can use the binary COPY path for compatible row shapes. Complex values
such as JSON or arrays may use a text-compatible path. Complete-atomic and
incremental runs create staging data and then perform the mode-specific merge.

Grant the runtime user the table, schema, sequence, and staging permissions
required by the chosen mode. Put SSL settings in
`source.connect.parameter.*` or `sink.connect.parameter.*` and resolve any
security values through the environment.