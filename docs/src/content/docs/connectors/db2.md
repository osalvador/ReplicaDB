---
title: IBM Db2 connector
description: Db2 LUW and IBM i source/sink modes and JDBC caveats.
---


Use `jdbc:db2:` for Db2 LUW and `jdbc:as400:` for IBM i. Both variants support
source and sink roles, all three replication modes, staging, partitioned reads,
and bandwidth throttling.

## Connection string

| Variant | Format | Driver class |
| ------- | ------ | ------------ |
| Db2 LUW | `jdbc:db2://${DB2_HOST}:${DB2_PORT}/${DB2_DATABASE}` | `com.ibm.db2.jcc.DB2Driver` |
| IBM i | `jdbc:as400://${AS400_HOST}:446/${DB2_DATABASE}` | `com.ibm.as400.access.AS400JDBCDriver` |

Credentials are set with `source.user` / `source.password` (or the `sink.*`
equivalents), not in the URL.

## Example properties

```properties
mode=complete-atomic
jobs=4

source.connect=jdbc:db2://${DB2_HOST}:${DB2_PORT}/${DB2_DATABASE}
source.user=${DB2_USER}
source.password=${DB2_PASSWORD}
source.table=${SOURCE_TABLE}

sink.connect=jdbc:as400://${AS400_HOST}:446/${DB2_DATABASE}
sink.user=${AS400_USER}
sink.password=${AS400_PASSWORD}
sink.table=${SINK_TABLE}
```

## Partitioned reads

Partitioned source reads use a `ROW_NUMBER() OVER (...)` window with `MOD` on
the job count to split rows across parallel jobs. Verify the ordering and query
plan on the target Db2 edition before raising `jobs`; a syntactically valid
query can still put excessive work on the source.

## LOB values

BLOB and CLOB transfer follows JDBC type handling. Test null, empty, and large
LOB values on both Db2 LUW and IBM i rather than assuming they behave the same.

## Limits and notes

- Db2 LUW and IBM i use different JDBC schemes, drivers, catalog conventions,
  and identifier rules. Driver versions, naming conventions, permissions, and
  type mappings differ between environments.
- Complete-atomic and incremental sink modes use a staging table, so the
  runtime user needs create and drop rights, or write rights on the configured
  staging table.
- Validate ROW_NUMBER-based partitioning, LOB values, and staging-table
  permissions with a representative schema.
- See [Oracle](/ReplicaDB/connectors/oracle/) and [SQL Server](/ReplicaDB/connectors/sql-server/) for other connectors.
