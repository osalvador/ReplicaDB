---
title: CLI configuration
description: Configure options files, precedence, connection settings, and safe substitution.
---

# CLI configuration

ReplicaDB accepts long command-line options and Java-properties options files.
Use an options file for repeatable settings and keep values that change per
invocation on the command line.

## Options-file rules

Pass a file with `--options-file <file-path>`. It may appear anywhere in the
command line. The file supports comments on their own lines, backslash
continuations, and `${VARIABLE}` environment substitution. Command-line
arguments override values loaded from the options file.

The complete property inventory is in the
[options-file reference](/ReplicaDB/reference/example-options-files/). The
exact flag inventory is in the [CLI option reference](/ReplicaDB/reference/cli-options/).

## Connection settings

The usual source and sink properties are:

```properties
mode=complete
jobs=1
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

## Precedence and validation

The CLI loads the options file first, then applies explicit command-line
values. It validates required source and sink connections, mode-specific
settings, contiguous multi-table indexes, and watermark restrictions before
starting replication. A malformed invocation or validation failure exits with
code 1.