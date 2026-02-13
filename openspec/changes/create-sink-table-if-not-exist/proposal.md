## Why

ReplicaDB assumes the sink table already exists before replication begins. If it doesn't, the user gets a raw `SQLException` with no helpful guidance -- whether from `TRUNCATE TABLE` in complete mode, `DELETE FROM` in complete-atomic mode, or `CREATE STAGING ... LIKE <sink>` in incremental mode. This forces users to manually create destination tables before running replication, which is friction for common use cases like bootstrapping a new environment, migrating across database vendors, or one-off data copies. Every comparable tool in the ecosystem (Airbyte, Debezium JDBC Sink, Sling) supports auto-creating destination tables. This is the most-requested missing capability per [GitHub issue #44](https://github.com/osalvador/ReplicaDB/issues/44).

## What Changes

- Add a new `--sink-auto-create` CLI option (boolean flag, default `false`) that instructs ReplicaDB to create the sink table automatically if it does not exist.
- When enabled, ReplicaDB probes the source metadata via a lightweight query (`SELECT * FROM source WHERE 1=0`) to obtain `ResultSetMetaData`, then generates a `CREATE TABLE` DDL on the sink using per-manager JDBC-to-native type mappings.
- Each SQL-based sink manager gains a `mapJdbcTypeToNativeDDL(int jdbcType, int precision, int scale, boolean nullable)` method that converts JDBC type codes to database-native column definitions.
- Table creation executes during `preSinkTasks()`, before any data movement or staging table creation.
- The auto-created table includes columns with correct types and nullability. Primary keys are derived from source `DatabaseMetaData.getPrimaryKeys()` and included in the DDL, since incremental replication mode requires PKs on the sink table for merge/upsert operations. No indexes or other constraints are created (users can add these afterwards).
- Non-SQL sinks (MongoDB, Kafka, S3, local files) ignore the flag since they don't have table DDL.

## Capabilities

### New Capabilities

- `sink-auto-create`: Automatic creation of sink tables from source metadata when the sink table does not exist, including JDBC type-to-native-DDL mapping for each supported SQL database, table existence detection, and primary key derivation from source metadata for incremental mode compatibility.

### Modified Capabilities

_(none -- this is additive functionality that does not change existing replication behavior when the flag is not set)_

## Impact

- **CLI / Configuration**: One new option (`--sink-auto-create`) added to `ToolOptions`. Properties file equivalent: `sink.auto.create`.
- **Manager hierarchy**: New abstract method in `SqlManager` (`mapJdbcTypeToNativeDDL`), with concrete implementations in `PostgresqlManager`, `OracleManager`, `MySQLManager`, `SQLServerManager`, `Db2Manager`, `SqliteManager`. `StandardJDBCManager` gets a best-effort generic JDBC mapping.
- **Replication lifecycle**: New step in `preSinkTasks()` -- check table existence, optionally probe source metadata, generate and execute DDL. Runs before truncation or staging table creation.
- **Source connection**: One additional lightweight query per replication run (only when flag is enabled) to fetch `ResultSetMetaData`.
- **Non-SQL managers**: `ConnManager` base class provides a no-op default so MongoDB, Kafka, S3, and file managers are unaffected.
- **Backwards compatibility**: Fully backwards compatible. Default behavior (`--sink-auto-create` is `false`) is unchanged. No breaking changes.
