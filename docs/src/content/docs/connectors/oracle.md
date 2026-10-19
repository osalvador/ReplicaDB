---
title: Oracle connector
description: Oracle source and sink behavior, partitioning, staging, and security.
---


Oracle is supported as source and sink for `complete`, `complete-atomic`,
and `incremental` replication. Use the `jdbc:oracle:` scheme with the Oracle
thin driver (`oracle.jdbc.OracleDriver`).

## Connection string formats

Keep host, port, SID, service, and credentials in the runtime environment.

| Addressing | Format |
| ---------- | ------ |
| SID | `jdbc:oracle:thin:@${ORAHOST}:${ORAPORT}:${ORASID}` |
| Service name | `jdbc:oracle:thin:@//${ORAHOST}:${ORAPORT}/${ORASERVICE}` |
| TNS alias | `jdbc:oracle:thin:@${ORATNSALIAS}` |

A TNS alias requires `tnsnames.ora` to be reachable through the Oracle client
configuration. Advice: service names are the usual choice for pluggable databases.

## Example properties

```properties
source.connect=jdbc:oracle:thin:@//${ORAHOST}:${ORAPORT}/${ORASERVICE}
source.user=${ORAUSER}
source.password=${ORAPASSWORD}
source.table=SOURCE_SCHEMA.ORDERS
source.connect.parameter.oracle.net.tns_admin=${TNS_ADMIN}
source.connect.parameter.oracle.net.networkCompression=on

sink.connect=jdbc:oracle:thin:@//${ORAHOST}:${ORAPORT}/${ORASERVICE}
sink.user=${ORAUSER}
sink.password=${ORAPASSWORD}
sink.table=SINK_SCHEMA.ORDERS

mode=complete-atomic
jobs=4
```

`oracle.net.tns_admin` and `oracle.net.networkCompression` are Oracle JDBC
driver properties. ReplicaDB does not interpret them; it passes them to the
driver as `source.connect.parameter.*` or `sink.connect.parameter.*` entries.
Check the Oracle JDBC documentation for the values your driver version supports.

## Parallel reads and Flashback

Table-backed reads with `jobs` greater than 1 partition rows with
`ora_hash(rowid, jobs - 1) = ?`. A free-form `source.query` owns its own
selection and does not receive this partitioning.

Flashback: ReplicaDB reads the current system change number with
`SELECT CURRENT_SCN FROM V$DATABASE` and applies `AS OF SCN` to the source
read. If the SCN cannot be read, ReplicaDB logs a warning and continues without
Flashback.

If a read fails with ORA-01555 (snapshot too old) or another SCN-related error,
ReplicaDB logs the error and rethrows the exception. Advice: increase undo
retention or shorten the run window, then rerun.

## Limits and notes

- LOB handling: BLOB and CLOB columns are streamed to the sink. Advice: test LOB
  and vendor-specific types with representative values before a large transfer.
- The sink applies `bandwidth.throttling` on the insert path.
- Complete-atomic and incremental sink runs write to a staging table before
  replacement or merge. Set `sink.staging.schema` to place generated staging
  tables in a dedicated schema. A fixed staging table can be named with
  `sink.staging.table`, but it cannot be used with `replication.table.*` entries.
- General guidance: run ReplicaDB close to the database server to reduce
  network latency. This is not a ReplicaDB requirement.

See also: [PostgreSQL](/ReplicaDB/connectors/postgresql/),
[SQL Server](/ReplicaDB/connectors/sql-server/), and
[DB2](/ReplicaDB/connectors/db2/).
