---
title: CLI configuration
description: Configure options files, precedence, connection settings, and safe substitution.
---


ReplicaDB accepts long command-line options and Java-properties options files.
Use an options file for repeatable settings and keep values that change per
invocation on the command line.

## Review a file from the wizard

1. Choose the source and destination in the
   [configuration wizard](/ReplicaDB/wizard/index.html). Check their roles and
   common modes in the [connector matrix](/ReplicaDB/connectors/).
2. Replace the example connection URLs, tables, paths, or topics with values
   for your environment. The wizard checks fields and connector combinations
   locally. Connection access, schemas, permissions, and types are checked
   when you run ReplicaDB.
3. Check what the mode will do to the destination. For an initial test, use a
   disposable destination and compare the actual rows with the expected result.
   The [mode walkthrough](/ReplicaDB/cli/replication-modes/) shows what to
   inspect during a complete load, an atomic replacement, and an incremental run.
4. Download the file and run `./bin/replicadb --options-file replicadb.conf`.
   Use the [CLI option reference](/ReplicaDB/reference/cli-options/) to check
   the resulting keys or the [examples](/ReplicaDB/reference/example-options-files/)
   to compare complete configurations.

## Options-file rules

Pass a file with `--options-file <file-path>`. It may appear anywhere in the
command line. The file supports comments on their own lines, backslash
continuations, and `${VARIABLE}` environment substitution. Command-line
arguments override values loaded from the options file.

The loader reads the file once before applying explicit flags. An unset
environment variable leaves its `${VARIABLE}` token in the value. For
example, an unset token in a JDBC URL reaches the connector as literal text.
Export required variables before launching ReplicaDB. Keep resolved files
owner-readable and omit them from diagnostic output.

Use one `key=value` per line. Java properties interpret backslashes, so
`\t` represents a tab and `\\` represents a literal backslash. A continuation
at the end of a line joins the next line to the same value. The complete
flag-to-key mapping is in the [CLI option reference](/ReplicaDB/reference/cli-options/).

## Connection settings

This example copies one table with complete mode and one parallel task.

```properties
mode=complete
jobs=1
fetch.size=100
verbose=INFO
source.connect=${SOURCE_CONNECT}
source.user=${SOURCE_USER}
source.password=${SOURCE_PASSWORD}
source.table=${SOURCE_TABLE}
sink.connect=${SINK_CONNECT}
sink.user=${SINK_USER}
sink.password=${SINK_PASSWORD}
sink.table=${SINK_TABLE}
```

Keep the file outside the repository when it contains resolved values. JDBC
driver-specific parameters use `source.connect.parameter.*` and
`sink.connect.parameter.*`; authentication settings for supported SQL Server
flows use the corresponding `source.auth.*` and `sink.auth.*` keys.
In the wizard's **Additional connection parameters** field, enter the bare
name and value, for example `sslmode=verify-full` on PostgreSQL. The source
field writes `source.connect.parameter.sslmode=verify-full`. See the
[PostgreSQL connector](/ReplicaDB/connectors/postgresql/) for the TLS settings
that go with it.

Common properties control the transfer as a whole: `mode`, `jobs`,
`fetch.size`, `bandwidth.throttling`, `quoted.identifiers`, and `verbose`.
Source and sink properties describe their respective endpoints. Numbered
`replication.table.*` entries define a sequential catalog. `sentry.dsn` is an
optional telemetry setting and is disabled when no DSN is configured.

## Precedence and validation

The CLI loads the options file first, then applies explicit command-line
values. It validates required source and sink connections, mode-specific
settings, contiguous multi-table indexes, and watermark restrictions before
starting replication. A malformed invocation or validation failure exits with
code 1.

Defaults apply only when neither source supplies a value: CLI `jobs` defaults
to 4, `fetch.size` to 100, `mode` to `complete`, bandwidth throttling to 0
(unlimited), and quoted identifiers to false. The maintained sample chooses
`jobs=1` deliberately as a conservative first-run setting; it does not change
the command-line default.
