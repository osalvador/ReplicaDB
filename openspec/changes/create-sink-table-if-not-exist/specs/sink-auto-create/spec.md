## ADDED Requirements

### Requirement: Sink table auto-creation when table does not exist

When `--sink-auto-create` is enabled and the sink table does not exist, ReplicaDB SHALL create the sink table automatically by probing the source metadata and generating database-native DDL on the sink connection. If the sink table already exists, auto-creation SHALL be skipped silently. When `--sink-auto-create` is not enabled (default), behavior SHALL be unchanged from current behavior.

#### Scenario: Sink table does not exist and auto-create is enabled

- **WHEN** `--sink-auto-create` is enabled
- **AND** the sink table specified by `--sink-table` does not exist in the sink database
- **THEN** ReplicaDB SHALL create the sink table with columns derived from source metadata
- **AND** column types SHALL be mapped from JDBC type codes to sink-database-native types
- **AND** column nullability SHALL be preserved from source metadata
- **AND** table creation SHALL complete before any data movement, staging table creation, or truncation

#### Scenario: Sink table already exists and auto-create is enabled

- **WHEN** `--sink-auto-create` is enabled
- **AND** the sink table specified by `--sink-table` already exists in the sink database
- **THEN** ReplicaDB SHALL skip table creation
- **AND** replication SHALL proceed with the existing table as it does today

#### Scenario: Auto-create is not enabled (default behavior)

- **WHEN** `--sink-auto-create` is not set or is `false`
- **THEN** ReplicaDB SHALL NOT check for table existence or attempt to create any table
- **AND** behavior SHALL be identical to the current release

### Requirement: Table existence detection via JDBC metadata

ReplicaDB SHALL detect whether the sink table exists using `DatabaseMetaData.getTables()` on the sink connection. The check MUST handle schema-qualified table names and case-sensitivity variations across database vendors.

#### Scenario: Schema-qualified sink table name

- **WHEN** `--sink-table` is specified as `schema.tablename`
- **THEN** ReplicaDB SHALL parse the schema and table name using existing `getSchemaFromQualifiedTableName()` and `getTableNameFromQualifiedTableName()` methods
- **AND** pass both to `DatabaseMetaData.getTables(catalog, schema, table, {"TABLE"})`

#### Scenario: Unqualified sink table name

- **WHEN** `--sink-table` is specified without a schema prefix
- **THEN** ReplicaDB SHALL pass `null` as the schema to `DatabaseMetaData.getTables()`
- **AND** let the JDBC driver resolve the default schema

#### Scenario: Case-insensitive table name matching

- **WHEN** the sink database uses case-insensitive identifiers (e.g., Oracle stores as UPPERCASE, PostgreSQL as lowercase)
- **THEN** ReplicaDB SHALL attempt table lookup using the provided name, then UPPERCASE, then lowercase variants
- **AND** follow the same three-attempt pattern used in `SqlManager.getPrimaryKeys()`

### Requirement: Source metadata probing via lightweight query

When auto-creation is triggered, ReplicaDB SHALL obtain source column metadata by executing a lightweight probe query against the source connection. This query MUST NOT return any data rows.

#### Scenario: Probe query execution for source metadata

- **WHEN** auto-creation determines the sink table does not exist
- **THEN** ReplicaDB SHALL execute `SELECT * FROM <source-table> WHERE 1=0` on the source connection
- **AND** capture `ResultSetMetaData` including column name, JDBC type code, precision, scale, and nullability for every column
- **AND** the probe query SHALL be executed only once per replication run

#### Scenario: Source uses a custom query instead of table name

- **WHEN** `--source-query` is specified instead of `--source-table`
- **THEN** ReplicaDB SHALL wrap the custom query as `SELECT * FROM (<source-query>) tmp WHERE 1=0`
- **AND** capture the same `ResultSetMetaData` as the table-based probe

### Requirement: Primary key derivation from source for incremental mode

When auto-creating the sink table, ReplicaDB SHALL retrieve primary key information from the source database via `DatabaseMetaData.getPrimaryKeys()` and include a `PRIMARY KEY` constraint in the generated DDL. This is necessary because incremental mode merge/upsert operations require PKs on the sink table.

#### Scenario: Source table has primary keys defined

- **WHEN** auto-creation is triggered
- **AND** the source table has primary keys in its database metadata
- **THEN** the generated `CREATE TABLE` DDL SHALL include a `PRIMARY KEY (col1, col2, ...)` constraint with the source PK columns

#### Scenario: Source table has no primary keys

- **WHEN** auto-creation is triggered
- **AND** the source table has no primary keys in its database metadata
- **AND** replication mode is `complete` or `complete-atomic`
- **THEN** the generated `CREATE TABLE` DDL SHALL omit the `PRIMARY KEY` constraint
- **AND** replication SHALL proceed normally

#### Scenario: Source table has no primary keys and mode is incremental

- **WHEN** auto-creation is triggered
- **AND** the source table has no primary keys in its database metadata
- **AND** replication mode is `incremental`
- **THEN** ReplicaDB SHALL log a warning indicating that incremental mode requires primary keys
- **AND** the table SHALL still be created without a PK constraint
- **AND** the replication SHALL fail later during merge with the existing `IllegalArgumentException` from `getSinkPrimaryKeys()`

#### Scenario: Source uses a custom query (no PKs available)

- **WHEN** `--source-query` is specified instead of `--source-table`
- **THEN** `DatabaseMetaData.getPrimaryKeys()` cannot be called (no physical table)
- **AND** the generated DDL SHALL omit the `PRIMARY KEY` constraint
- **AND** if mode is `incremental`, a warning SHALL be logged

### Requirement: JDBC type to sink-native DDL mapping

Each SQL-based sink manager SHALL implement a method that maps JDBC type codes to database-native column DDL fragments. The mapping MUST cover all JDBC types currently handled in the manager's `insertDataToTable()` type switch.

#### Scenario: Common type mappings for PostgreSQL sink

- **WHEN** the sink is PostgreSQL
- **THEN** the following JDBC types SHALL map to these native types:
  - `Types.INTEGER`, `Types.TINYINT`, `Types.SMALLINT` → `INTEGER`
  - `Types.BIGINT` → `BIGINT`
  - `Types.FLOAT`, `Types.DOUBLE` → `DOUBLE PRECISION`
  - `Types.REAL` → `REAL`
  - `Types.NUMERIC`, `Types.DECIMAL` → `NUMERIC(precision, scale)`
  - `Types.VARCHAR`, `Types.NVARCHAR`, `Types.LONGVARCHAR` → `VARCHAR(precision)` or `TEXT` if precision is 0 or undefined
  - `Types.CHAR`, `Types.NCHAR` → `CHAR(precision)`
  - `Types.BOOLEAN`, `Types.BIT` → `BOOLEAN`
  - `Types.DATE` → `DATE`
  - `Types.TIME`, `Types.TIME_WITH_TIMEZONE` → `TIME`
  - `Types.TIMESTAMP`, `Types.TIMESTAMP_WITH_TIMEZONE` → `TIMESTAMP`
  - `Types.BINARY`, `Types.VARBINARY`, `Types.LONGVARBINARY`, `Types.BLOB` → `BYTEA`
  - `Types.CLOB` → `TEXT`

#### Scenario: Common type mappings for Oracle sink

- **WHEN** the sink is Oracle
- **THEN** the following JDBC types SHALL map to these native types:
  - `Types.INTEGER`, `Types.TINYINT`, `Types.SMALLINT` → `NUMBER(10)`
  - `Types.BIGINT` → `NUMBER(19)`
  - `Types.FLOAT`, `Types.DOUBLE` → `BINARY_DOUBLE`
  - `Types.REAL` → `BINARY_FLOAT`
  - `Types.NUMERIC`, `Types.DECIMAL` → `NUMBER(precision, scale)`
  - `Types.VARCHAR`, `Types.NVARCHAR`, `Types.LONGVARCHAR` → `VARCHAR2(precision)` or `CLOB` if precision exceeds 4000
  - `Types.CHAR`, `Types.NCHAR` → `CHAR(precision)`
  - `Types.BOOLEAN`, `Types.BIT` → `NUMBER(1)`
  - `Types.DATE` → `DATE`
  - `Types.TIME`, `Types.TIME_WITH_TIMEZONE` → `TIMESTAMP`
  - `Types.TIMESTAMP`, `Types.TIMESTAMP_WITH_TIMEZONE` → `TIMESTAMP`
  - `Types.BINARY`, `Types.VARBINARY`, `Types.LONGVARBINARY` → `RAW(precision)` or `BLOB` if precision exceeds 2000
  - `Types.BLOB` → `BLOB`
  - `Types.CLOB` → `CLOB`

#### Scenario: Common type mappings for MySQL sink

- **WHEN** the sink is MySQL
- **THEN** the following JDBC types SHALL map to these native types:
  - `Types.INTEGER` → `INT`
  - `Types.TINYINT` → `TINYINT`
  - `Types.SMALLINT` → `SMALLINT`
  - `Types.BIGINT` → `BIGINT`
  - `Types.FLOAT`, `Types.REAL` → `FLOAT`
  - `Types.DOUBLE` → `DOUBLE`
  - `Types.NUMERIC`, `Types.DECIMAL` → `DECIMAL(precision, scale)`
  - `Types.VARCHAR`, `Types.NVARCHAR`, `Types.LONGVARCHAR` → `VARCHAR(precision)` or `LONGTEXT` if precision exceeds 16383
  - `Types.CHAR`, `Types.NCHAR` → `CHAR(precision)`
  - `Types.BOOLEAN`, `Types.BIT` → `BOOLEAN`
  - `Types.DATE` → `DATE`
  - `Types.TIME`, `Types.TIME_WITH_TIMEZONE` → `TIME`
  - `Types.TIMESTAMP`, `Types.TIMESTAMP_WITH_TIMEZONE` → `DATETIME`
  - `Types.BINARY`, `Types.VARBINARY`, `Types.LONGVARBINARY` → `VARBINARY(precision)` or `LONGBLOB` if precision exceeds 65535
  - `Types.BLOB` → `LONGBLOB`
  - `Types.CLOB` → `LONGTEXT`

#### Scenario: Common type mappings for SQL Server sink

- **WHEN** the sink is SQL Server
- **THEN** the following JDBC types SHALL map to these native types:
  - `Types.INTEGER` → `INT`
  - `Types.TINYINT` → `TINYINT`
  - `Types.SMALLINT` → `SMALLINT`
  - `Types.BIGINT` → `BIGINT`
  - `Types.FLOAT`, `Types.DOUBLE` → `FLOAT`
  - `Types.REAL` → `REAL`
  - `Types.NUMERIC`, `Types.DECIMAL` → `NUMERIC(precision, scale)`
  - `Types.VARCHAR`, `Types.NVARCHAR`, `Types.LONGVARCHAR` → `NVARCHAR(precision)` or `NVARCHAR(MAX)` if precision exceeds 4000
  - `Types.CHAR`, `Types.NCHAR` → `NCHAR(precision)`
  - `Types.BOOLEAN`, `Types.BIT` → `BIT`
  - `Types.DATE` → `DATE`
  - `Types.TIME`, `Types.TIME_WITH_TIMEZONE` → `TIME`
  - `Types.TIMESTAMP`, `Types.TIMESTAMP_WITH_TIMEZONE` → `DATETIME2`
  - `Types.BINARY`, `Types.VARBINARY`, `Types.LONGVARBINARY` → `VARBINARY(precision)` or `VARBINARY(MAX)` if precision exceeds 8000
  - `Types.BLOB` → `VARBINARY(MAX)`
  - `Types.CLOB` → `NVARCHAR(MAX)`

#### Scenario: Common type mappings for DB2 sink

- **WHEN** the sink is DB2
- **THEN** the following JDBC types SHALL map to these native types:
  - `Types.INTEGER` → `INTEGER`
  - `Types.TINYINT`, `Types.SMALLINT` → `SMALLINT`
  - `Types.BIGINT` → `BIGINT`
  - `Types.FLOAT`, `Types.DOUBLE` → `DOUBLE`
  - `Types.REAL` → `REAL`
  - `Types.NUMERIC`, `Types.DECIMAL` → `DECIMAL(precision, scale)`
  - `Types.VARCHAR`, `Types.NVARCHAR`, `Types.LONGVARCHAR` → `VARCHAR(precision)` or `CLOB` if precision exceeds 32672
  - `Types.CHAR`, `Types.NCHAR` → `CHAR(precision)`
  - `Types.BOOLEAN`, `Types.BIT` → `SMALLINT`
  - `Types.DATE` → `DATE`
  - `Types.TIME`, `Types.TIME_WITH_TIMEZONE` → `TIME`
  - `Types.TIMESTAMP`, `Types.TIMESTAMP_WITH_TIMEZONE` → `TIMESTAMP`
  - `Types.BINARY`, `Types.VARBINARY`, `Types.LONGVARBINARY` → `VARCHAR(precision) FOR BIT DATA` or `BLOB` if precision exceeds 32672
  - `Types.BLOB` → `BLOB`
  - `Types.CLOB` → `CLOB`

#### Scenario: Common type mappings for SQLite sink

- **WHEN** the sink is SQLite
- **THEN** the following JDBC types SHALL map to these native types:
  - `Types.INTEGER`, `Types.TINYINT`, `Types.SMALLINT`, `Types.BIGINT` → `INTEGER`
  - `Types.FLOAT`, `Types.DOUBLE`, `Types.REAL` → `REAL`
  - `Types.NUMERIC`, `Types.DECIMAL` → `NUMERIC`
  - `Types.VARCHAR`, `Types.NVARCHAR`, `Types.LONGVARCHAR`, `Types.CHAR`, `Types.NCHAR` → `TEXT`
  - `Types.BOOLEAN`, `Types.BIT` → `INTEGER`
  - `Types.DATE`, `Types.TIME`, `Types.TIMESTAMP`, `Types.TIME_WITH_TIMEZONE`, `Types.TIMESTAMP_WITH_TIMEZONE` → `TEXT`
  - `Types.BINARY`, `Types.VARBINARY`, `Types.LONGVARBINARY`, `Types.BLOB` → `BLOB`
  - `Types.CLOB` → `TEXT`

#### Scenario: Fallback for unmapped JDBC types

- **WHEN** a JDBC type code is encountered that has no explicit mapping in the sink manager
- **THEN** the manager SHALL map it to the broadest text type for the database (e.g., `TEXT` for PostgreSQL, `CLOB` for Oracle, `LONGTEXT` for MySQL)
- **AND** log a warning identifying the column name, JDBC type code, and the fallback type used

### Requirement: Non-SQL sinks ignore auto-create

Non-SQL sink managers (MongoDB, Kafka, S3, local files) SHALL ignore the `--sink-auto-create` flag since they do not have relational table DDL.

#### Scenario: Auto-create enabled with non-SQL sink

- **WHEN** `--sink-auto-create` is enabled
- **AND** the sink is a non-SQL manager (MongoDB, CSV, Kafka, etc.)
- **THEN** the flag SHALL be silently ignored
- **AND** replication SHALL proceed as normal

### Requirement: CLI option registration

A new boolean CLI option `--sink-auto-create` SHALL be added to `ToolOptions` following the existing boolean flag pattern (e.g., `--sink-disable-truncate`). The equivalent properties file key SHALL be `sink.auto.create`.

#### Scenario: Option specified on command line

- **WHEN** `--sink-auto-create` is passed on the command line
- **THEN** `ToolOptions.isSinkAutoCreate()` SHALL return `true`

#### Scenario: Option specified in properties file

- **WHEN** `sink.auto.create=true` is set in the properties file
- **THEN** `ToolOptions.isSinkAutoCreate()` SHALL return `true`

#### Scenario: Option not specified

- **WHEN** neither `--sink-auto-create` nor `sink.auto.create` is provided
- **THEN** `ToolOptions.isSinkAutoCreate()` SHALL return `false`
