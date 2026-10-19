---
title: PostgreSQL connector
description: PostgreSQL source and sink behavior including COPY and staging.
---


Use `jdbc:postgresql:` for a source or sink connection. PostgreSQL supports all
three replication modes and bandwidth throttling.

## Connection string

| Format | Example |
| ------ | ------- |
| Host, port, database | `jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}` |
| With schema in the URL | `jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}?currentSchema=${PGSCHEMA}` |

Credentials are set with `source.user` / `source.password` (or the `sink.*`
equivalents), not in the URL. The driver class is `org.postgresql.Driver`.

## Example properties

```properties
mode=complete-atomic
jobs=4

source.connect=jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}
source.user=${PGUSER}
source.password=${PGPASSWORD}
source.table=${SOURCE_TABLE}

sink.connect=jdbc:postgresql://${PGHOST}:${PGPORT}/${PGDATABASE}
sink.user=${PGUSER}
sink.password=${PGPASSWORD}
sink.table=${SINK_TABLE}

source.connect.parameter.ApplicationName=ReplicaDB
source.connect.parameter.ssl=true
source.connect.parameter.sslmode=verify-full
source.connect.parameter.sslrootcert=${PG_SSL_ROOT_CERT}
```

`ApplicationName`, `ssl`, `sslmode`, and `sslrootcert` are standard pgjdbc
driver properties. ReplicaDB passes them through `source.connect.parameter.*`
or `sink.connect.parameter.*`; it does not implement them. Resolve secrets and
certificate paths through the environment.

## Write paths

The sink writes with binary COPY for simple column types. Complex types (ARRAY,
JSON, JSONB, XML, and INTERVAL) use TEXT COPY. TEXT COPY is also forced for
MongoDB and DB2 sources, and for MySQL sources with FLOAT or DOUBLE columns.
The choice is made when the job runs and is logged.

Complete-atomic and incremental runs write to an unlogged staging table
created with `LIKE` on the sink table, then merge it into the sink table.

## Limits and notes

- Grant the runtime user the table, schema, sequence, and staging permissions
  required by the chosen mode.
- Advice: run ReplicaDB close to the database server, in the same network
  zone where possible, to reduce latency on the COPY path. This is general
  guidance, not a ReplicaDB requirement.
- Put SSL settings in `source.connect.parameter.*` or `sink.connect.parameter.*`
  and resolve any security values through the environment.
- See [Oracle](/ReplicaDB/connectors/oracle/), [MySQL and MariaDB](/ReplicaDB/connectors/mysql-mariadb/), and [SQL Server](/ReplicaDB/connectors/sql-server/) for other connectors.
