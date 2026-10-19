---
title: MySQL and MariaDB connectors
description: MySQL and MariaDB source and sink behavior and driver guidance.
---


Use `jdbc:mysql:` for MySQL or `jdbc:mariadb:` for MariaDB. Both managers can
be source or sink and support complete, complete-atomic, and incremental modes.
Both schemes use the MariaDB JDBC driver (`org.mariadb.jdbc.Driver`).

## Connection string

| Format | Example |
| ------ | ------- |
| MySQL | `jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}` |
| MariaDB | `jdbc:mariadb://${MARIADB_HOST}:${MARIADB_PORT}/${MARIADB_DATABASE}` |

Credentials are set with `source.user` / `source.password` (or the `sink.*`
equivalents), not in the URL.

## Example properties

```properties
mode=complete-atomic
jobs=4

source.connect=jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}
source.user=${MYSQL_USER}
source.password=${MYSQL_PASSWORD}
source.table=${SOURCE_TABLE}

sink.connect=jdbc:mariadb://${MARIADB_HOST}:${MARIADB_PORT}/${MARIADB_DATABASE}
sink.user=${MARIADB_USER}
sink.password=${MARIADB_PASSWORD}
sink.table=${SINK_TABLE}

sink.connect.parameter.sslMode=verify-full
```

`sslMode` is a standard MariaDB JDBC driver property. ReplicaDB passes it
through `sink.connect.parameter.*` and does not implement it.

## Bulk load path

The optimized sink path uses `LOAD DATA LOCAL INFILE`. For the sink only, the connection is created with
`characterEncoding=UTF-8`, `allowLoadLocalInfile=true`, and
`rewriteBatchedStatements=true` set automatically. The server-side `local_infile`
policy still applies and is outside ReplicaDB. Enabling local loading changes
the driver's data-loading surface, so approve it explicitly rather than
weakening a global database policy.

Staging tables are used for complete-atomic and incremental sink flows; keep
their schema and permissions explicit. Advice: run a representative
compatibility test for BLOB, date, and JSON values before production use.

## Limits and notes

- Validate the server-side `local_infile` policy with the database administrator
  before using the bulk path.
- Use a scheme and driver properties that match the actual server, and keep
  TLS and authentication parameters under the corresponding
  `source.connect.parameter.*` or `sink.connect.parameter.*` namespace.
- See [PostgreSQL](/ReplicaDB/connectors/postgresql/) and [SQL Server](/ReplicaDB/connectors/sql-server/) for other connectors.
