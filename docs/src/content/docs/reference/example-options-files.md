---
title: Options-file examples
description: Safe Java-properties patterns for repeatable standalone CLI runs.
---


The options file is loaded before command-line values. Use environment
substitution for connection and security values, and pass a one-off override
explicitly when needed.

## Single-table complete run

```properties
mode=complete
jobs=1
fetch.size=100
source.connect=${SOURCE_CONNECT}
source.user=${SOURCE_USER}
source.password=${SOURCE_PASSWORD}
source.table=${SOURCE_TABLE}
sink.connect=${SINK_CONNECT}
sink.user=${SINK_USER}
sink.password=${SINK_PASSWORD}
sink.table=${SINK_TABLE}
```

Invoke it with:

```bash
./bin/replicadb --options-file ./replicadb.conf --jobs 2
```

The explicit `--jobs 2` wins over `jobs=1` in the file.

## File endpoints and diagnostic level

File connectors use an explicit format from `csv`, `json`, `avro`, `parquet`,
or `orc` where that connector supports the requested role:

```properties
mode=complete
jobs=1
verbose=INFO
source.connect=${SOURCE_FILE_PATH}
source.file.format=csv
sink.connect=${SINK_FILE_PATH}
sink.file.format=parquet
```

Use `verbose=DEBUG` temporarily for diagnosis. The command-line `--verbose`
flag also selects diagnostic output, but an explicit command-line value is not
accepted because it is a boolean flag.

## Multi-table catalog

```properties
mode=complete
source.connect=${SOURCE_CONNECT}
sink.connect=${SINK_CONNECT}
replication.table.1.source=${SOURCE_TABLE_ONE}
replication.table.1.sink=${SINK_TABLE_ONE}
replication.table.2.source=${SOURCE_TABLE_TWO}
replication.table.2.sink=${SINK_TABLE_TWO}
```

Indexes must start at 1 and be contiguous. Do not add `source.table`,
`sink.table`, or `source.query` to this form.

## Driver parameters and authentication

Connector-specific properties use the prefixes below. The concrete names and
allowed values belong to the connector guide:

```properties
source.connect.parameter.driver=${SOURCE_DRIVER}
sink.connect.parameter.driver=${SINK_DRIVER}
source.auth.mode=${SOURCE_AUTH_MODE}
source.auth.client.certificate=${SOURCE_CLIENT_CERTIFICATE}
source.auth.client.key=${SOURCE_CLIENT_KEY}
sink.auth.mode=${SINK_AUTH_MODE}
```

Optional telemetry is configured only in the options file:

```properties
sentry.dsn=${SENTRY_DSN}
```

Leave the variable unset to keep telemetry disabled. Do not commit a resolved
DSN, certificate, private key, password, token, or connection value.

Keep the resolved file in an owner-controlled location and remove it according
to the retention policy for the host running the CLI.
