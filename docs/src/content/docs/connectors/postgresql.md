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

Binary COPY is selected for compatible simple types when a binary value is
present. ARRAY, JSON/JSONB, XML, and INTERVAL shapes use TEXT COPY, as do
MongoDB source rows whose BSON values do not map directly to PostgreSQL binary
encoding. This fallback preserves correctness; it is not an error or a promise
of identical throughput for every table shape.

Generated staging tables use the sink table's shape. Incremental merge depends
on a usable sink key, while complete-atomic performs its final replacement in
the sink transaction supported by the manager.

Grant the runtime user the table, schema, sequence, and staging permissions
required by the chosen mode. Put SSL settings in
`source.connect.parameter.*` or `sink.connect.parameter.*` and resolve any
security values through the environment.
