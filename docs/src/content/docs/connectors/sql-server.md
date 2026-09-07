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

The sink uses the Microsoft JDBC bulk-copy API with table locking and the
configured fetch size as its batch size. SQL Server reports XML with a
vendor-specific JDBC type, which ReplicaDB maps to `SQLXML`; test XML and other
vendor types across the exact source/sink pair. Bulk copy has no mid-transfer
cancellation hook, so an interrupted run can retain the mode-specific sink
warning even after cancellation is requested.
