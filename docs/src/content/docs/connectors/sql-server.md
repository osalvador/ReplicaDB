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

Set `source.auth.mode` for a SQL Server source or `sink.auth.mode` for a SQL
Server sink. The fields in this table use the same endpoint prefix.

| Mode value | Settings to provide | Requirements and limits |
| --- | --- | --- |
| `ActiveDirectoryInteractive` | Optional `auth.login.hint` for the account to sign in with. | Requires interactive sign-in and `jobs=1`. Leave the password empty. |
| `ActiveDirectoryDefault` | Optional `auth.principal.id` for a managed identity client ID. | Uses the driver's default credential chain. Configure the host identity or credentials and leave the password empty. |
| `ActiveDirectoryManagedIdentity` | Optional `auth.principal.id` for a user-assigned identity. | Requires a managed identity available to the runtime and access granted in Azure SQL. Leave the password empty. |
| `ActiveDirectoryServicePrincipal` | `auth.principal.id` and the endpoint's `password` property for its client secret. | Supply the secret through an environment reference. The principal ID supplies the driver username. |
| `ActiveDirectoryServicePrincipalCertificate` | `auth.principal.id` and `auth.client.certificate`. Add `auth.client.key` when the private key is separate. | Make the certificate and key available to the launching user on the host running the CLI. |
| `ActiveDirectoryIntegrated` | The runtime identity and Kerberos setup. | Leave the username and password empty. |

`ActiveDirectoryMSI` is accepted as an alias for
`ActiveDirectoryManagedIdentity`. The CLI rejects `ActiveDirectoryPassword`.
Set the mode in the options file and keep any authentication properties in the
JDBC URL or extra parameters consistent with it. Conflicting values fail.

The following settings give a SQL Server destination a service principal
and a secret.

```properties
sink.auth.mode=ActiveDirectoryServicePrincipal
sink.auth.principal.id=${AZURE_CLIENT_ID}
sink.password=${AZURE_CLIENT_SECRET}
```

Export both variables and verify that the principal can write to the destination.

## Staging and limits

Complete-atomic and incremental sinks use a staging table. The identity that
runs ReplicaDB needs permission to create and drop it, or to write to the
configured staging table.

- Test Azure firewall, TLS, and identity access with the same host identity
  that will run ReplicaDB.
- Advice: validate throughput with a representative `fetch.size` before
  changing `jobs`.
- See [Oracle](/ReplicaDB/connectors/oracle/), [PostgreSQL](/ReplicaDB/connectors/postgresql/), and [MySQL and MariaDB](/ReplicaDB/connectors/mysql-mariadb/) for other connectors.
