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

Run the following command to use two jobs for this invocation.

```bash
./bin/replicadb --options-file ./replicadb.conf --jobs 2
```

The explicit `--jobs 2` wins over `jobs=1` in the file.

## File endpoints and diagnostic level

Use this complete CSV-to-CSV example with a local file that contains a header
and two columns. Set the source types in the same order as the input. The
destination also gets a header and uses LF line endings.

```bash
printf 'id,status\n1,paid\n2,pending\n' > /tmp/replicadb-orders-input.csv
```

```properties
mode=complete
jobs=1
verbose=INFO
source.connect=file:///tmp/replicadb-orders-input.csv
source.file.format=csv
source.columns=id,status
source.connect.parameter.columns.types=INTEGER,VARCHAR
source.connect.parameter.format.firstRecordAsHeader=true
sink.connect=file:///tmp/replicadb-orders-output.csv
sink.file.format=csv
sink.connect.parameter.format.firstRecordAsHeader=true
sink.connect.parameter.format.recordSeparator=\n
```

Save the options as `orders.conf`. Run the transfer and print the generated file.

```bash
./bin/replicadb --options-file orders.conf
cat /tmp/replicadb-orders-output.csv
```

```csv
id,status
1,paid
2,pending
```

Use `verbose=DEBUG` temporarily for diagnosis. The command-line `--verbose`
flag selects DEBUG and takes no argument. Local files require one job and
implement CSV and ORC. In `complete` mode this example replaces the output
file. See the [file connector](/ReplicaDB/connectors/csv/) for CSV presets,
escaping, types, and ORC compression.

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

## PostgreSQL staging and watermarks

With the [sample PostgreSQL data](/ReplicaDB/getting-started/sample-data/),
start from the [CLI quickstart options file](/ReplicaDB/getting-started/cli-quickstart/#configure-and-run).
Replace its `mode` value and add the staging schema. A complete replacement
uses one transaction to publish the staged rows.

```properties
mode=complete-atomic
sink.staging.schema=reporting
```

Use the following settings to merge a selected batch.

```properties
mode=incremental
incremental.watermark.column=change_seq
incremental.watermark.value=10
sink.staging.schema=public
```

The incremental file selects rows where `change_seq > 10` and merges them
by the destination key. With the
[mode comparison data](/ReplicaDB/cli/replication-modes/#prepare-the-comparison),
this updates order `1`, adds order `2`, and keeps the older order `9`.
In v1.0.3 the merge resolves the staging table through the connection's
search path. The sample uses `public` for that reason. Review the
[watermark limits](/ReplicaDB/cli/incremental-watermarks/#limits) before
adapting the example.

## Driver parameters and authentication

These are source-side additions to a working options file. For PostgreSQL,
pass JDBC TLS settings using the source prefix as shown below. Use the sink
prefix to configure TLS on the destination.

```properties
source.connect.parameter.sslmode=verify-full
source.connect.parameter.sslrootcert=${PG_SSL_ROOT_CERT}
```

For SQL Server, this setting uses the driver's default credential chain.

```properties
source.auth.mode=ActiveDirectoryDefault
```

See the [PostgreSQL connector](/ReplicaDB/connectors/postgresql/) for TLS
properties, the [SQL Server connector](/ReplicaDB/connectors/sql-server/#microsoft-entra-authentication)
for the required Entra fields, and the
[generic JDBC guide](/ReplicaDB/connectors/generic-jdbc/#connection-parameters)
for custom drivers and credentials.

To configure optional telemetry in the options file, set the Sentry DSN.

```properties
sentry.dsn=${SENTRY_DSN}
```

Omit the property to keep telemetry disabled. Do not commit a resolved
DSN, certificate, private key, password, token, or connection value.

Keep the resolved file in an owner-controlled location and remove it according
to the retention policy for the host running the CLI.
