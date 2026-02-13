## 1. CLI Option and Data Structures

- [x] 1.1 Add `sinkAutoCreate` boolean field to `ToolOptions` (default `false`), with getter `isSinkAutoCreate()`, setter, and `NotNull` setter following the `--sink-disable-truncate` pattern
- [x] 1.2 Register `--sink-auto-create` option in `ToolOptions.checkOptions()` (no `.hasArg()`, near the other sink options)
- [x] 1.3 Parse `--sink-auto-create` in the command-line parsing block: `if (line.hasOption("sink-auto-create")) setSinkAutoCreateNotNull(true);`
- [x] 1.4 Load from properties file in `loadOptionsFile()`: `setSinkAutoCreate(Boolean.parseBoolean(prop.getProperty("sink.auto.create")));`
- [x] 1.5 Create `ColumnDescriptor` data class (or record) in `org.replicadb.manager.util` with fields: `columnName`, `jdbcType`, `precision`, `scale`, `nullable` -- used to store probe results without holding `ResultSetMetaData` references
- [x] 1.6 Add `List<ColumnDescriptor> sourceColumnDescriptors` and `String[] sourcePrimaryKeys` fields to `ToolOptions` with getters/setters

## 2. Source Metadata Probe

- [x] 2.1 Add `probeSourceMetadata()` method to `SqlManager` that executes `SELECT * FROM <source-table> WHERE 1=0`, extracts `ResultSetMetaData` into `List<ColumnDescriptor>`, and stores on `options.setSourceColumnDescriptors()`
- [x] 2.2 Handle `--source-query` variant in probe: wrap as `SELECT * FROM (<source-query>) tmp WHERE 1=0`
- [x] 2.3 Add source primary key retrieval in `probeSourceMetadata()`: call `DatabaseMetaData.getPrimaryKeys()` on the source connection (only when `--source-table` is set), sort by `KEY_SEQ`, store on `options.setSourcePrimaryKeys()`
- [x] 2.4 Call `probeSourceMetadata()` from `SqlManager.preSourceTasks()` when `options.isSinkAutoCreate()` is true

## 3. Table Existence Check

- [x] 3.1 Add `sinkTableExists()` method to `SqlManager` using `DatabaseMetaData.getTables(null, schema, table, {"TABLE"})` with three-case-variant retry (as-is, UPPERCASE, lowercase) following the pattern in `getSinkPrimaryKeys()`
- [x] 3.2 Use existing `getSchemaFromQualifiedTableName()` and `getTableNameFromQualifiedTableName()` for schema/table parsing

## 4. DDL Generation in SqlManager

- [x] 4.1 Add abstract method `mapJdbcTypeToNativeDDL(int jdbcType, int precision, int scale)` to `SqlManager` returning a `String` DDL type fragment
- [x] 4.2 Add `generateCreateTableDDL(String tableName, List<ColumnDescriptor> columns, String[] primaryKeys)` method to `SqlManager` that iterates columns, calls `mapJdbcTypeToNativeDDL()` per column, appends `NOT NULL` where `nullable == columnNoNulls`, and appends `PRIMARY KEY (...)` constraint if PKs are present
- [x] 4.3 Add `autoCreateSinkTable()` method to `SqlManager`: check `sinkTableExists()`, if not exists read `sourceColumnDescriptors` and `sourcePrimaryKeys` from options, call `generateCreateTableDDL()`, execute the DDL, set `sinkTableJustCreated` flag on the manager instance
- [x] 4.4 Log a warning when incremental mode is active but no source primary keys are available (custom query or source table has no PKs)

## 5. Per-Manager Type Mappings

- [x] 5.1 Implement `mapJdbcTypeToNativeDDL()` in `PostgresqlManager` per spec (including `TEXT` fallback when precision is 0 for VARCHAR types, `BYTEA` for binary types)
- [x] 5.2 Implement `mapJdbcTypeToNativeDDL()` in `OracleManager` per spec (including `NUMBER` without parameters when precision=0/scale=-127, `VARCHAR2` → `CLOB` threshold at 4000, `RAW` → `BLOB` threshold at 2000)
- [x] 5.3 Implement `mapJdbcTypeToNativeDDL()` in `MySQLManager` per spec (including `VARCHAR` → `LONGTEXT` threshold at 16383, `VARBINARY` → `LONGBLOB` threshold at 65535, `DATETIME` for timestamps)
- [x] 5.4 Implement `mapJdbcTypeToNativeDDL()` in `SQLServerManager` per spec (including `NVARCHAR(MAX)` threshold at 4000, `VARBINARY(MAX)` threshold at 8000, `DATETIME2` for timestamps)
- [x] 5.5 Implement `mapJdbcTypeToNativeDDL()` in `Db2Manager` per spec (including `VARCHAR FOR BIT DATA` → `BLOB` threshold at 32672, `SMALLINT` for boolean)
- [x] 5.6 Implement `mapJdbcTypeToNativeDDL()` in `SqliteManager` per spec (5 affinity types: `INTEGER`, `REAL`, `TEXT`, `BLOB`, `NUMERIC`)
- [x] 5.7 Implement `mapJdbcTypeToNativeDDL()` in `StandardJDBCManager` with a best-effort generic JDBC mapping using standard SQL types as fallback
- [x] 5.8 Add default fallback to broadest text type in each manager for unmapped JDBC type codes, with a LOG.warn identifying the column name and type code
- [x] 5.9 Add stub implementations for non-SQL managers (Denodo, MongoDB, Kafka, S3, LocalFile) that throw `UnsupportedOperationException` since they don't use SQL DDL

## 6. Lifecycle Integration

- [x] 6.1 Insert `autoCreateSinkTable()` call at the top of `SqlManager.preSinkTasks()`, guarded by `if (options.isSinkAutoCreate())`
- [x] 6.2 Add `sinkTableJustCreated` boolean instance field to `SqlManager` (transient, not persisted), set to `true` when `autoCreateSinkTable()` creates a table
- [x] 6.3 Modify truncation logic in `preSinkTasks()` to skip truncation when `sinkTableJustCreated` is true (the table is already empty)

## 7. Staging Schema Fix

- [x] 7.1 In `SqlManager.preSinkTasks()`, replace the hardcoded `"public"` default for `sinkStagingSchema` with the schema extracted from `getSinkTableName()` via `getSchemaFromQualifiedTableName()`, falling back to `null` (let JDBC driver resolve default) when no schema is specified
- [x] 7.2 Remove or update the existing `TODO` comment at line 516-518

## 8. Testing

- [x] 8.1 Unit test: `ToolOptions` correctly parses `--sink-auto-create` from CLI and properties file
- [x] 8.2 Unit test: `ColumnDescriptor` correctly captures metadata fields
- [x] 8.3 Unit test: `generateCreateTableDDL()` produces correct DDL with columns, nullability, and PK constraint
- [x] 8.4 Unit test: `generateCreateTableDDL()` omits PK constraint when `primaryKeys` is null/empty
- [x] 8.5 Unit test: each manager's `mapJdbcTypeToNativeDDL()` returns correct native types for all specified JDBC type codes, including precision/scale thresholds and fallback behavior
- [x] 8.6 Integration test: auto-create with PostgreSQL sink (complete mode, table does not exist) -- verify table is created with correct columns and data is replicated
- [x] 8.7 Integration test: auto-create with incremental mode -- verify table is created with PK and merge/upsert succeeds
- [x] 8.8 Integration test: auto-create skipped when sink table already exists -- verify no DDL is executed and replication proceeds normally
- [x] 8.9 Integration test: auto-create with `--source-query` -- verify table is created without PK, warning is logged for incremental mode
