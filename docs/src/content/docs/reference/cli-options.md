---
title: CLI option reference
description: Exact long options supported by the standalone ReplicaDB CLI.
---

# CLI option reference

These names are the maintained long-option contract. Run `./bin/replicadb
--help` for the generated descriptions and argument names.

| Option | Purpose |
| --- | --- |
| `--source-connect` | Source connection string. |
| `--source-user` | Source user. |
| `--source-password` | Source password supplied through a protected options file. |
| `--source-auth-mode` | Source connector authentication mode. |
| `--source-auth-principal-id` | Source authentication principal identifier. |
| `--source-auth-login-hint` | Source authentication login hint. |
| `--source-auth-client-certificate` | Source client certificate path or value. |
| `--source-auth-client-key` | Source client key path or value. |
| `--source-table` | Source table. |
| `--source-columns` | Source column selection. |
| `--source-where` | Source filtering expression. |
| `--incremental-watermark-column` | Incremental source watermark column. |
| `--incremental-watermark-value` | Previously successful watermark value. |
| `--source-query` | Free-form source query. |
| `--source-file-format` | Source file format. |
| `--sink-connect` | Sink connection string. |
| `--sink-user` | Sink user. |
| `--sink-password` | Sink password supplied through a protected options file. |
| `--sink-auth-mode` | Sink connector authentication mode. |
| `--sink-auth-principal-id` | Sink authentication principal identifier. |
| `--sink-auth-login-hint` | Sink authentication login hint. |
| `--sink-auth-client-certificate` | Sink client certificate path or value. |
| `--sink-auth-client-key` | Sink client key path or value. |
| `--sink-table` | Sink table. |
| `--sink-columns` | Sink column selection. |
| `--sink-disable-escape` | Disable sink escaping. |
| `--sink-disable-index` | Disable sink index handling. |
| `--sink-disable-truncate` | Disable sink truncation. |
| `--sink-auto-create` | Create the sink table when supported. |
| `--sink-analyze` | Analyze the sink table after populate. |
| `--sink-staging-table` | Existing sink staging table. |
| `--sink-staging-table-alias` | Alias for the sink staging table. |
| `--sink-staging-schema` | Schema for generated sink staging tables. |
| `--sink-file-format` | Sink file format. |
| `--options-file` | Java-properties options file path. |
| `--mode` | `complete`, `complete-atomic`, or `incremental`. |
| `--fetch-size` | Rows requested by a source read. |
| `--version` | Print the implementation version and exit. |
| `--bandwidth-throttling` | Transfer cap in KB/s. |
| `--help` | Print the help screen and exit. |
| `--jobs` | Number of parallel jobs. |
| `--verbose` | Print more information while working. |
| `--quoted-identifiers` | Quote database identifiers. |