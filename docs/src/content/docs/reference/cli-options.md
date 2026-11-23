---
title: CLI option reference
description: Match command-line flags to options-file keys and check defaults, examples, and connector support.
---

Use this page to check what an option does before running the file generated
by the [configuration wizard](/ReplicaDB/wizard/index.html). Options here
belong to the standalone CLI. For a managed job, see
[Server jobs](/ReplicaDB/server/jobs/).

## Invocation and defaults

Run `./bin/replicadb --options-file replicadb.conf`. The CLI loads that file
first and then applies explicit flags. For example, `--jobs 2` overrides
`jobs=1`. Connector parameters, numbered table pairs, and `sentry.dsn` are
set in the options file. See [CLI configuration](/ReplicaDB/cli/configuration/)
for syntax and environment substitution.

| Flag | Purpose |
| --- | --- |
| `--options-file <file-path>` | Load a Java-properties file. |
| `--help` | Print CLI help and exit. |
| `--version` | Report the CLI version and exit. |

The CLI defaults to `mode=complete`, `jobs=4`, `fetch.size=100`,
`bandwidth.throttling=0`, `verbose=INFO`, and `quoted.identifiers=false`.
The examples often use `jobs=1` to make the first run easier to inspect.
Set boolean properties explicitly to `true` or `false`. Boolean flags
such as `--quoted-identifiers` and `--verbose` take no argument and are
enabled by their presence.

## Connections and authentication

| Options-file keys | CLI flags | Values and support |
| --- | --- | --- |
| `source.connect`, `sink.connect` | `--source-connect`, `--sink-connect` | Both are required. The scheme selects the connector. For example, a PostgreSQL source uses `jdbc:postgresql://localhost:5432/shop` and a CSV sink uses `file:///tmp/orders.csv`. Check the [connector matrix](/ReplicaDB/connectors/) for roles and modes. |
| `source.user`, `sink.user` | `--source-user`, `--sink-user` | Account name where supported, for example a reference to a DBA-provisioned read account. MongoDB uses its connection URI. The [generic JDBC path](/ReplicaDB/connectors/generic-jdbc/#connection-parameters) documents credentials under the connection-parameter prefix. |
| `source.password`, `sink.password` | `--source-password`, `--sink-password` | Password or service principal secret where supported. Use an environment reference in a protected options file. |
| `source.connect.parameter.*`, `sink.connect.parameter.*` | Options file | Pass settings to the selected connector or driver. Examples include `source.connect.parameter.sslmode=verify-full` for PostgreSQL and `sink.connect.parameter.topic=orders` for Kafka. The receiving connector defines valid names and values. |

### Microsoft Entra properties

These properties apply to the Microsoft SQL Server connector on either side.
Choose the authentication mode first. The
[Entra mode table](/ReplicaDB/connectors/sql-server/#microsoft-entra-authentication)
lists the exact values and the accepted fields for each flow.

| Options-file keys | CLI flags | Purpose |
| --- | --- | --- |
| `source.auth.mode`, `sink.auth.mode` | `--source-auth-mode`, `--sink-auth-mode` | For example `ActiveDirectoryDefault`. Leave unset to use the connection's ordinary authentication settings. |
| `source.auth.principal.id`, `sink.auth.principal.id` | `--source-auth-principal-id`, `--sink-auth-principal-id` | Service principal application ID or an optional managed identity client ID where supported. |
| `source.auth.login.hint`, `sink.auth.login.hint` | `--source-auth-login-hint`, `--sink-auth-login-hint` | Account hint used by the Interactive flow. |
| `source.auth.client.certificate`, `sink.auth.client.certificate` | `--source-auth-client-certificate`, `--sink-auth-client-certificate` | Client certificate path for the service principal certificate flow. |
| `source.auth.client.key`, `sink.auth.client.key` | `--source-auth-client-key`, `--sink-auth-client-key` | Private key path when the certificate flow uses separate key material. |

## Tables, columns, and queries

| Options-file keys | CLI flags | Values and support |
| --- | --- | --- |
| `source.table`, `sink.table` | `--source-table`, `--sink-table` | Table, view where supported, or MongoDB collection. Use names such as `shop.orders` and `reporting.orders` for databases with schemas. File, S3, and Kafka endpoints identify their data through connection settings. |
| `source.columns`, `sink.columns` | `--source-columns`, `--sink-columns` | For SQL, comma-separated columns in transfer order, such as `id,status`. Match the order and types on both sides. For CSV, supply input column names with `source.columns` and override the output header with `sink.columns`. MongoDB uses a [projection or field mapping](/ReplicaDB/connectors/mongodb/#field-mapping). |
| `source.where` | `--source-where` | A predicate such as `status = 'paid'`, without the SQL `WHERE` keyword. MongoDB uses a BSON filter. Use the table selection path. |
| `source.query` | `--source-query` | A complete query such as `SELECT id, status FROM shop.orders` on a source that supports it. MongoDB aggregation uses a collection and a JSON pipeline. Query selection excludes the CLI table catalog and automatic watermarks. |
| `replication.table.<n>.source`, `replication.table.<n>.sink` | Options file | Explicit pairs, with contiguous indexes starting at 1. Pairs run sequentially and share connection settings and mode. See [multi-table replication](/ReplicaDB/cli/multi-table/) for failure and staging behavior. |

After defining a query or projection, compare the selected columns with the
destination. See [filtering and queries](/ReplicaDB/cli/filtering-and-queries/)
for examples and [connector support](/ReplicaDB/connectors/) for query
availability.

## Modes and incremental watermarks

| Options-file key | CLI flag | Values and support |
| --- | --- | --- |
| `mode` | `--mode` | `complete`, `complete-atomic`, or `incremental`. The available modes are the intersection of the source's read modes and the sink's write modes. |
| `incremental.watermark.column` | `--incremental-watermark-column` | Column exposed by a concrete table on a supported JDBC source, for example `change_seq`. Requires `incremental` and a single table selection. |
| `incremental.watermark.value` | `--incremental-watermark-value` | Last successful lower bound, for example `10`. Requires a watermark column. The next read selects values strictly greater than the bound. |

`incremental` writes the selected rows according to the sink's behavior. A
database merge and a file append have different results. Read the
[mode walkthrough](/ReplicaDB/cli/replication-modes/) before choosing how to
write, and the [watermark guide](/ReplicaDB/cli/incremental-watermarks/) before
advancing a saved boundary.

## Destination preparation and staging

| Options-file key | CLI flag | Effect and support |
| --- | --- | --- |
| `sink.disable.truncate` | `--sink-disable-truncate` | In `complete` mode, `true` skips clearing an existing database table or collection on the shared sink preparation path. The load inserts into existing data and can hit duplicate keys. File replacement follows its own connector rules. |
| `sink.auto.create` | `--sink-auto-create` | `true` creates a missing SQL destination from source metadata on supporting JDBC paths. It requires readable source metadata and destination DDL permission. Existing tables are kept. Create the schema and any keys or constraints required by your workload explicitly when needed. |
| `sink.disable.escape` | `--sink-disable-escape` | `true` skips escaping backslashes and line breaks in PostgreSQL text COPY and MySQL/MariaDB text bulk loads. Keep the default `false` unless you have verified the input. This flag has no effect on PostgreSQL binary COPY. |
| `sink.disable.index` | `--sink-disable-index` | Accepted by the CLI. Current connectors do not act on it. Manage indexes with the destination database when needed. |
| `sink.analyze` | `--sink-analyze` | Accepted by the CLI. Current connectors do not run a statistics update for it. Use the destination's own maintenance process. |
| `sink.staging.schema` | `--sink-staging-schema` | Schema for an automatically generated SQL staging table, for example `staging`. Requires a destination path that uses staging and permission to create and drop a table in that schema. |
| `sink.staging.table` | `--sink-staging-table` | Reuse a staging table or collection that already exists, where supported. Match its structure to the load and give the runtime identity permission to prepare and write it. Use only with a single table pair. |
| `sink.staging.table.alias` | `--sink-staging-table-alias` | Prefix for an automatically generated staging name, for example `orders_stage`. Applies to staging paths that generate a name. The CLI excludes this option from a staged table catalog. |

These settings change what gets written or prepared on the destination.
For supported staged loads, choose a generated staging schema or an existing
staging object. Keep the default `sink.disable.truncate=false` with reused
incremental staging so pre-existing rows do not carry into a later merge.
Review connector-specific staging and transaction behavior
before sharing a destination with readers. See
[complete-atomic](/ReplicaDB/cli/replication-modes/#complete-atomic).

## Parallelism and diagnostics

| Options-file key | CLI flag | Values and support |
| --- | --- | --- |
| `jobs` | `--jobs` | Positive integer, default `4`. Sets parallel tasks inside the current table pair. Use `1` for local files, generic JDBC, and interactive Entra authentication. |
| `fetch.size` | `--fetch-size` | Positive integer, default `100`. Sets reader fetch size and is also used for batching by some sinks, such as SQL Server bulk copy. |
| `bandwidth.throttling` | `--bandwidth-throttling` | Non-negative integer in KB/s per worker. `0` is unlimited. Check the [connector matrix](/ReplicaDB/connectors/) for advertised sink support. SQL Server bulk copy has no generic throttle. |
| `quoted.identifiers` | `--quoted-identifiers` | `true` enables quoting on supporting generated SQL paths. A custom query retains the quoting supplied by its author. |
| `verbose` | `--verbose` | Set `DEBUG`, `INFO`, `WARN`, or `ERROR` in a file. The flag alone selects DEBUG. |
| `sentry.dsn` | Options file | An optional Sentry DSN. Omit the property to leave telemetry disabled. If enabled, supply the DSN through the environment. |

See [CLI parallelism](/ReplicaDB/cli/parallelism/) for connection and resource
trade-offs and [CLI troubleshooting](/ReplicaDB/cli/troubleshooting/) for exit
codes and diagnostic output.

## File formats and output services

| Options-file key | CLI flag | Values and support |
| --- | --- | --- |
| `source.file.format` | `--source-file-format` | `csv` or `orc` for a local file source. CSV input requires `source.connect.parameter.columns.types`. |
| `sink.file.format` | `--sink-file-format` | `csv` or `orc` for a local file sink and S3 file output. S3 object-per-row mode takes its content directly from the selected column. |

The generated CLI help also lists `json`, `avro`, and `parquet`. The
current file manager only implements CSV and ORC and falls back to CSV for
unknown formats. Always set the intended implemented format explicitly.

- [CSV parameters and ORC compression](/ReplicaDB/connectors/csv/#csv-options)
- [S3 object keys, HTTPS, and file output](/ReplicaDB/connectors/amazon-s3/#properties)
- [Kafka topic, partition, producer key, and acknowledgements](/ReplicaDB/connectors/kafka/#sink-connection-parameters)
