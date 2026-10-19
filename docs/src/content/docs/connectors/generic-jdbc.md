---
title: Generic JDBC and legacy managers
description: Fallback JDBC schemes and their intentionally narrow capability contract.
---


ReplicaDB has explicit legacy managers for HSQLDB, CUBRID, jTDS SQL Server, and
Netezza. Any other JDBC URL falls back to the Standard JDBC manager, which reads
the driver class from the `driver` connection parameter.

## Capabilities

| Manager | Schemes | Driver class | Source and sink modes | Jobs |
| ------- | ------- | ------------ | --------------------- | ---- |
| HSQLDB | `jdbc:hsqldb:` | `org.hsqldb.jdbcDriver` | complete | single-job |
| CUBRID | `jdbc:cubrid:` | `cubrid.jdbc.driver.CUBRIDDriver` | complete | single-job |
| jTDS SQL Server | `jdbc:jtds:sqlserver:` | `net.sourceforge.jtds.jdbc.Driver` | complete | single-job |
| Netezza | `jdbc:netezza:` | `org.netezza.Driver` | complete | single-job |
| Standard JDBC (fallback) | other JDBC URLs | set with `driver` | complete | single-job |

- Explicit legacy managers support complete mode only, as source and as sink.
- Incremental mode and complete-atomic mode are not supported by these managers.
- The fallback does not add vendor-specific bulk loading, partitioning, staging,
  or type conversion. Validate column types and permissions with a disposable
  sink before production use.

## Connection parameters

Set the driver class through the `driver` connection parameter when the URL
scheme has no explicit manager. For the explicit legacy schemes, the driver
class is resolved from the scheme and does not need to be set.

| Property | Description |
| -------- | ----------- |
| `source.connect` / `sink.connect` | JDBC URL, for example `jdbc:hsqldb:file:${HSQLDB_PATH}`. |
| `source.connect.parameter.driver` / `sink.connect.parameter.driver` | Driver class for the fallback manager, for example `com.example.jdbc.Driver`. |
| `source.connect.parameter.user` / `sink.connect.parameter.user` | Database user, for example `${JDBC_USER}`. |
| `source.connect.parameter.password` / `sink.connect.parameter.password` | Database password, for example `${JDBC_PASSWORD}`. |

## Example: legacy HSQLDB to a PostgreSQL sink

```properties
mode=complete
jobs=1

source.connect=jdbc:hsqldb:file:${HSQLDB_PATH}
source.connect.parameter.user=${HSQLDB_USER}
source.connect.parameter.password=${HSQLDB_PASSWORD}
source.table=PUBLIC.CATALOG

sink.connect=jdbc:postgresql://${PGHOST}:5432/${PGDATABASE}
sink.connect.parameter.user=${PGUSER}
sink.connect.parameter.password=${PGPASSWORD}
sink.table=public.catalog
```

## Example: fallback JDBC source with a query

```properties
mode=complete
jobs=1

source.connect=jdbc:example://${EXAMPLE_HOST}:${EXAMPLE_PORT}/${EXAMPLE_DATABASE}
source.connect.parameter.driver=com.example.jdbc.Driver
source.connect.parameter.user=${EXAMPLE_USER}
source.connect.parameter.password=${EXAMPLE_PASSWORD}
source.query=SELECT id, name FROM catalog WHERE active = 1

sink.connect=jdbc:postgresql://${PGHOST}:5432/${PGDATABASE}
sink.connect.parameter.user=${PGUSER}
sink.connect.parameter.password=${PGPASSWORD}
sink.table=public.catalog
```

## Limits

- The fallback is intended for complete replication with one job. Use a
  dedicated connector page when a vendor manager exists.
- Each driver class must be on the classpath and loadable before the run.
