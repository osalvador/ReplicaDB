## CLI Contract
`ToolOptions` is the single boundary for command-line and properties-file configuration. It defines source and sink connection strings, credentials, tables, columns, filters, query text, file formats, staging settings, `jobs`, `fetch-size`, bandwidth throttling, quoted identifiers, verbose logging, mode, and sink flags such as auto-create and index/truncate controls.

The normal path requires `source-connect`, `sink-connect`, and a non-null mode. `--help` and `--version` short-circuit normal validation. Defaults include complete mode, four jobs, fetch size 100, no bandwidth cap, INFO logging, and disabled boolean sink flags.

## Configuration Files
`OptionsFile` loads Java properties and maps keys such as `source.connect`, `sink.table`, `jobs`, and `sink.auto.create` into `ToolOptions`. Keys beginning with `source.connect.parameter.` or `sink.connect.parameter.` become driver-specific connection properties. `EnvironmentVariableEvaluator` expands `${NAME}` in property values before managers use them.

## Option Precedence
An options file is loaded before command-line values are assigned, so command-line values can override file values when the corresponding setter accepts a non-empty argument. Preserve this behavior when adding options. Add both the CLI spelling and the properties-file spelling, and keep defaults backward compatible.

## Validation and Safety Rules
- Treat table names, column lists, `source-where`, `source-query`, and connection parameters as user-controlled input passed to manager-specific SQL or SDK code.
- Do not log or serialize `source-password`, `sink-password`, Sentry DSNs, or connection parameter maps.
- Prefer `${ENV_NAME}` references in configuration examples; generated context and instructions must never contain real values.
- Keep mode and manager capability checks explicit. A flag accepted by `ToolOptions` may still be unsupported by a concrete source or sink.

## Reference Implementations
- `src/main/java/org/replicadb/cli/ToolOptions.java`
- `src/main/java/org/replicadb/cli/OptionsFile.java`
- `src/main/java/org/replicadb/cli/EnvironmentVariableEvaluator.java`
- `src/main/java/org/replicadb/cli/ReplicationMode.java`
- `conf/_replicadb.conf`
