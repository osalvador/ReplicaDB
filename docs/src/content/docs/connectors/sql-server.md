---
title: SQL Server and Azure SQL connector
description: SQL Server bulk copy, modes, and Microsoft Entra guidance.
---

# SQL Server and Azure SQL

Use `jdbc:sqlserver:`. SQL Server supports source and sink roles and all three
replication modes. Its bulk-copy sink path does not expose the generic
bandwidth-throttling control.

## Microsoft Entra authentication

Use the `source.auth.*` or `sink.auth.*` options rather than embedding secrets
in a JDBC URL. `ActiveDirectoryInteractive` requires `jobs=1` and a local
browser/MFA flow; it is not a headless container mode. Default credentials,
managed identity, service principal, and certificate flows must be supplied by
the runtime environment.

Complete-atomic and incremental sinks require staging and the permissions for
the generated or configured staging table. Test Azure firewall, TLS, and
identity access with the same host identity that will run ReplicaDB.