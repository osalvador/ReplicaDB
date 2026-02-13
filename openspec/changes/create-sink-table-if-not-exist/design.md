## Context

ReplicaDB currently assumes the sink table exists before replication begins. The `preSinkTasks()` method in `SqlManager` (line 505) immediately proceeds to create staging tables, truncate, or delete -- all of which fail with a raw `SQLException` if the sink table is missing.

The codebase already has patterns for working with database metadata: `DatabaseMetaData.getPrimaryKeys()` is used in `SqlManager.getPrimaryKeys()` (line 329), `DatabaseMetaData.getColumns()` is used in `SQLServerManager` and `Db2Manager` for sink column types, and `ResultSetMetaData` is captured during every `insertDataToTable()` call. However, no code currently checks table existence or generates `CREATE TABLE` DDL.

Source and sink are independent `ConnManager` instances that share a single `ToolOptions` object. They communicate through this shared options during the `preSourceTasks() → preSinkTasks() → readTable()/insertDataToTable() → postSinkTasks()` lifecycle. The source connection is not directly accessible from the sink manager.

## Goals / Non-Goals

**Goals:**
- Auto-create the sink table from source metadata when `--sink-auto-create` is enabled and the table does not exist
- Map JDBC type codes to database-native DDL types for each supported SQL sink
- Include primary keys from source metadata to support incremental mode
- Integrate cleanly into the existing replication lifecycle with no behavior change when the flag is off

**Non-Goals:**
- Replicating indexes, foreign keys, CHECK constraints, or DEFAULT values
- Schema evolution (altering an existing table to match source changes)
- Custom per-column type overrides (may be added later)
- Auto-creation for non-SQL sinks (MongoDB, CSV, Kafka, S3)

## Decisions

### Decision 1: Source metadata probe runs during `preSourceTasks()`

**Choice:** Execute the probe query (`SELECT * FROM <source> WHERE 1=0`) on the source connection during `preSourceTasks()`, then store the captured metadata on `ToolOptions` so the sink manager can access it in `preSinkTasks()`.

**Alternatives considered:**
- *Probe from the sink manager by opening a second source connection:* Rejected because it would require the sink manager to know source connection details and manage an additional connection, violating the current separation of concerns.
- *Rearrange the lifecycle to pass the source manager to the sink:* Rejected because it would change the `preSinkTasks()` signature and break all existing manager implementations.
- *Probe during `preSinkTasks()` via a new source connection:* Rejected for the same reasons as the first alternative.

**Rationale:** `ToolOptions` already serves as shared state between source and sink managers (both hold a reference to the same instance). Adding `sourceResultSetMetaData` and `sourcePrimaryKeys` fields to `ToolOptions` follows this existing pattern. The probe runs in `preSourceTasks()` which executes before `preSinkTasks()` (see `ReplicaDB.java` line 218-219), so the metadata is guaranteed to be available when the sink needs it.

**Implementation:**
- Add to `ToolOptions`: `ResultSetMetaData sourceMetaData` and `String[] sourcePrimaryKeys` fields with getters/setters
- In `SqlManager.preSourceTasks()` (or a new overridable method called from it): if `sinkAutoCreate` is enabled, execute the probe query and store results on `options`
- The probe query for `--source-table`: `SELECT * FROM <source-table> WHERE 1=0`
- The probe query for `--source-query`: `SELECT * FROM (<source-query>) tmp WHERE 1=0`
- Primary keys via `DatabaseMetaData.getPrimaryKeys()` on the source connection (only when `--source-table` is set, not for custom queries)

### Decision 2: Table existence check via `DatabaseMetaData.getTables()`

**Choice:** Use `DatabaseMetaData.getTables(null, schema, table, new String[]{"TABLE"})` with the same three-case-variant retry pattern used in `SqlManager.getPrimaryKeys()` (as-is, UPPERCASE, lowercase).

**Alternatives considered:**
- *Try-catch on `SELECT 1 FROM <table> WHERE 1=0`:* Works but is database-specific in its error behavior (some throw, some return empty). The metadata API is cleaner.
- *`information_schema.tables` query:* Not portable across all supported databases (Oracle doesn't have it in the same form).

**Rationale:** `DatabaseMetaData.getTables()` is standard JDBC, available across all supported databases, and the case-variant retry pattern is already proven in the codebase.

### Decision 3: Table creation at the top of `preSinkTasks()`

**Choice:** Add auto-creation logic as the first step in `SqlManager.preSinkTasks()`, before any staging table creation, truncation, or deletion.

```
preSinkTasks() {
    // NEW: Auto-create sink table if needed
    if (options.isSinkAutoCreate()) {
        autoCreateSinkTable();
    }

    // EXISTING: Create staging table (if mode != COMPLETE)
    if (!options.getMode().equals(COMPLETE)) { ... }

    // EXISTING: Truncate or atomic delete
    ...
}
```

**Rationale:** All downstream operations in `preSinkTasks()` assume the sink table exists -- `createStagingTable()` clones the sink table's structure, `truncateTable()` operates on it, `atomicDeleteSinkTable()` deletes from it. Auto-creation must come first. Since this runs in `SqlManager` (the base class for all SQL managers), no per-manager override of `preSinkTasks()` is needed.

### Decision 4: Per-manager type mapping via abstract method

**Choice:** Add an abstract method `mapJdbcTypeToNativeDDL(int jdbcType, int precision, int scale)` to `SqlManager`, with concrete implementations in each SQL manager subclass. A shared method `generateCreateTableDDL(ResultSetMetaData rsmd, String[] primaryKeys)` in `SqlManager` iterates columns and calls the per-manager mapping.

**Alternatives considered:**
- *Central mapping table (Map<DatabaseType, Map<Integer, String>>):* Rejected because the mapping isn't a simple lookup -- it depends on precision/scale thresholds that vary per database (e.g., Oracle `VARCHAR2` max 4000 → `CLOB`, MySQL `VARCHAR` max 16383 → `LONGTEXT`).
- *External configuration file:* Too complex for initial implementation. Would add a configuration file format, parsing, and validation. Can be added later as an enhancement.

**Rationale:** Each manager already has deep knowledge of its database's type system (evident in the per-manager `insertDataToTable()` switch statements). The abstract method approach is consistent with how the codebase already works -- `createStagingTable()` and `mergeStagingTable()` are per-manager methods that generate database-specific SQL.

### Decision 5: DDL generation structure

**Choice:** Generate a standard `CREATE TABLE` statement of the form:

```sql
CREATE TABLE <sink-table> (
    col1 TYPE1 [NOT NULL],
    col2 TYPE2,
    ...,
    PRIMARY KEY (pk1, pk2)
)
```

The `PRIMARY KEY` constraint is included inline at the end of the column list (table-level constraint syntax) rather than as column-level `PRIMARY KEY` annotations. This works consistently across all supported databases.

**Rationale:** Table-level PK syntax is universally supported and handles composite keys naturally. Column-level PK only works for single-column keys.

### Decision 6: `ConnManager` provides no-op default for non-SQL sinks

**Choice:** The `autoCreateSinkTable()` method is implemented in `SqlManager`. Non-SQL managers that extend `ConnManager` directly (MongoDB, CSV, Kafka, etc.) inherit a no-op from `ConnManager.preSinkTasks()` which has no auto-create logic. The flag is silently ignored.

**Alternatives considered:**
- *Throw an error when `--sink-auto-create` is used with a non-SQL sink:* Rejected because it adds friction with no benefit. Silently ignoring is fine for a boolean flag.

### Decision 7: Nullability handling in generated DDL

**Choice:** Use `ResultSetMetaData.isNullable(column)` to determine nullability. Columns with `columnNoNulls` get `NOT NULL` in the DDL. Columns with `columnNullable` or `columnNullableUnknown` are left without a constraint (nullable by default in SQL).

**Rationale:** This is the conservative approach. If we're unsure, we allow nulls -- the user can always add constraints later.

## Risks / Trade-offs

**[Risk] Oracle `NUMBER` without precision** → The JDBC driver reports `NUMBER` columns as `Types.NUMERIC` with precision 0 and scale -127. The Oracle manager must handle this special case by mapping to `NUMBER` (unparameterized) rather than `NUMBER(0, -127)`.

**[Risk] Cross-database string length semantics** → A `VARCHAR(4000)` in Oracle means 4000 bytes (not characters in some configurations). When mapped to PostgreSQL's `VARCHAR(4000)`, it means 4000 characters. This could cause truncation on multi-byte data. Mitigation: document this as a known limitation. Users with multi-byte data should verify generated column sizes.

**[Risk] `ResultSetMetaData` accuracy varies by driver** → Some JDBC drivers return imprecise metadata (e.g., precision=0 for all VARCHAR columns). Mitigation: when precision is 0 or unreported for string types, use the broadest unbounded type (`TEXT`, `CLOB`, `LONGTEXT`) instead of `VARCHAR(0)`.

**[Risk] Source probe query on large views or complex queries** → `SELECT * FROM <source> WHERE 1=0` should be optimized by the database to return only metadata, but some databases may still parse expensive views. Mitigation: this is a known trade-off, same as used by Sqoop and every staging table creation in ReplicaDB itself.

**[Risk] Primary key column order** → `DatabaseMetaData.getPrimaryKeys()` returns columns with `KEY_SEQ` ordering. The implementation must sort by `KEY_SEQ` to preserve composite PK column order, matching the existing pattern in `SqlManager.getPrimaryKeys()`.

**[Risk] Storing `ResultSetMetaData` after Statement close** → Some JDBC drivers invalidate `ResultSetMetaData` when the underlying `Statement` or `ResultSet` is closed. Mitigation: extract all needed metadata (column names, types, precision, scale, nullability) into a plain data structure (list of column descriptors) before closing the probe statement, rather than storing the `ResultSetMetaData` object itself.

### Decision 8: Skip truncate only when the table was just created in this run

**Choice:** When `autoCreateSinkTable()` actually creates the table (i.e., the table did not exist and was just created), it sets a transient flag (e.g., `sinkTableJustCreated`) on the manager instance. The truncation step in `preSinkTasks()` checks this flag and skips truncation if the table was just created. In consecutive replications where the table already exists, truncation proceeds as configured by `--sink-disable-truncate`.

**Rationale:** Truncating a table that was just created is a no-op but generates unnecessary SQL traffic and log noise. However, this optimization must be scoped to the current run only -- on subsequent runs with the same configuration file, the table already exists and the user's truncation preference must be respected.

### Decision 9: Staging table uses the same schema as the sink table

**Choice:** When `--sink-auto-create` is enabled and no explicit `--sink-staging-schema` is provided, the staging table schema defaults to the schema extracted from the `--sink-table` name (via `getSchemaFromQualifiedTableName()`), rather than hardcoding `"public"`. This fixes the existing PostgreSQL-specific `TODO` at `SqlManager.preSinkTasks()` line 516-518.

**Alternatives considered:**
- *Keep the current "public" default and document it:* Rejected because it creates a confusing mismatch -- the sink table lives in the user's schema but the staging table appears in "public", which may not even exist in non-PostgreSQL databases.
- *Always require `--sink-staging-schema` when using `--sink-auto-create`:* Rejected because it adds friction to a feature meant to reduce setup.

**Rationale:** The sink table and its staging table should live in the same schema by default. If `--sink-table` is `myschema.mytable`, the staging table should be `myschema.repdb<random>`, not `public.repdb<random>`. This is the correct behavior regardless of `--sink-auto-create`, but this change provides the right moment to fix it since the staging schema logic is being touched anyway.

**Scope note:** The staging schema default fix benefits all users, not just those using `--sink-auto-create`. However, the change is small (replace `"public"` with the schema parsed from sink table name, falling back to `null` to let the JDBC driver resolve the default) and low-risk.

## Open Questions

_(All resolved -- see Decisions 8 and 9 above.)_
