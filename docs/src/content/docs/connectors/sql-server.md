---
title: SQL Server and Azure SQL connector
description: SQL Server bulk copy, modes, and Microsoft Entra guidance.
---


Use `jdbc:sqlserver:` for SQL Server and Azure SQL. SQL Server supports source
and sink roles and all three replication modes. The `bandwidth-throttling`
option is not applied on the sink path.

## Connection string

| Format | Example |
| ------ | ------- |
| Host and port | `jdbc:sqlserver://${MSSQL_HOST}:${MSSQL_PORT};databaseName=${MSSQL_DATABASE}` |
| Azure SQL | `jdbc:sqlserver://${AZURE_SQL_SERVER}.database.windows.net:1433;databaseName=${MSSQL_DATABASE}` |

Credentials are set with `source.user` / `source.password` (or the `sink.*`
equivalents), or with `source.auth.*` / `sink.auth.*` for Microsoft Entra
modes. The driver class is `com.microsoft.sqlserver.jdbc.SQLServerDriver`.

## Example properties

```properties
mode=complete-atomic
jobs=4
fetch.size=10000

source.connect=jdbc:sqlserver://${MSSQL_HOST}:${MSSQL_PORT};databaseName=${MSSQL_DATABASE}
source.user=${MSSQL_USER}
source.password=${MSSQL_PASSWORD}
source.table=${SOURCE_TABLE}

sink.connect=jdbc:sqlserver://${MSSQL_HOST}:${MSSQL_PORT};databaseName=${MSSQL_DATABASE}
sink.user=${MSSQL_USER}
sink.password=${MSSQL_PASSWORD}
sink.table=${SINK_TABLE}
```

## Bulk copy and data types

The sink uses the Microsoft `SQLServerBulkCopy` API with table locking. The
configured `fetch.size` is the bulk-copy batch size, so it controls how many
rows are sent per batch. SQL Server reports XML with a vendor-specific JDBC
type, which ReplicaDB maps to `SQLXML` and writes as XML. Test XML and other
vendor types across the exact source and sink pair.

Bulk copy has no mid-transfer cancellation hook. A cancelled run may leave the
sink partially written, so use `complete-atomic` or `incremental` with staging
where a partial write matters.

## Microsoft Entra authentication

Use the `source.auth.*` or `sink.auth.*` options rather than embedding secrets
in a JDBC URL. The supported `source.auth.mode` and `sink.auth.mode` values are
`ActiveDirectoryInteractive`, `Default`, `ManagedIdentity`, `ServicePrincipal`,
`ServicePrincipalCertificate`, and `Integrated`. `ActiveDirectoryPassword` is
deprecated.

`ActiveDirectoryInteractive` forces `jobs=1`; use it only where an interactive
sign-in is possible. `Integrated` does not accept a user
name. Default credentials, managed identity, service principal, and certificate
flows must be supplied by the runtime environment.

## Staging and limits

Complete-atomic and incremental sinks use a staging table. The identity that
runs ReplicaDB needs permission to create and drop it, or to write to the
configured staging table.

- Test Azure firewall, TLS, and identity access with the same host identity
  that will run ReplicaDB.
- Advice: validate throughput with a representative `fetch.size` before
  changing `jobs`.
- See [Oracle](/ReplicaDB/connectors/oracle/), [PostgreSQL](/ReplicaDB/connectors/postgresql/), and [MySQL and MariaDB](/ReplicaDB/connectors/mysql-mariadb/) for other connectors.
