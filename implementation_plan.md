# Implementation Plan: Oracle BLOB to SQL Server BulkCopy Support

## Task Source
**Source**: Manual: GitHub issue https://github.com/osalvador/ReplicaDB/issues/218
**Original Request**: Enable Oracle BLOB/CLOB replication into SQL Server while maintaining SQL Server bulk copy performance; keep bulk copy always on; re-enable disabled Oracle→SQLServer tests.

## Overview
Enable SQL Server bulk copy to accept JDBC `ResultSet` rows containing Oracle `BLOB`/`CLOB` values by introducing an `ISQLServerBulkRecord` adapter that maps JDBC types to SQL Server-compatible types and streams LOB values. This preserves bulk copy performance while resolving the “Data type blob is not supported in bulk copy” error. The change is scoped to SQL Server sink behavior and will re-enable Oracle→SQLServer integration tests currently disabled due to BLOB limitations.

## Architecture & Design
**Decision**: Clean architecture approach — introduce a dedicated `ISQLServerBulkRecord` adapter for JDBC `ResultSet` with explicit type mapping and LOB streaming to keep bulk copy performance and avoid fallback inserts.
- System architecture changes
  - Add a SQL Server–specific adapter for JDBC `ResultSet` similar to the existing RowSet adapter, but capable of streaming `BLOB`/`CLOB`.
  - Keep `SQLServerManager` on bulk copy path for all data sources (no fallback).
- Component interactions and data flow
  - `SQLServerManager.insertDataToTable()` selects the adapter based on `ResultSet` implementation and uses `SQLServerBulkCopy.writeToServer()` with the adapter.
  - Adapter converts JDBC metadata to SQL Server types and supplies row data with streaming for LOBs.
- Integration patterns
  - Reuse existing `SQLServerBulkRecordAdapter` behavior and extend it (or add a parallel adapter) for JDBC `ResultSet` without requiring changes in source managers.
- Performance/scalability considerations
  - Bulk copy remains the only path. LOBs are streamed per row to avoid full materialization and maintain throughput.
- Security implications
  - No new credentials or network paths. Ensure streams are closed promptly to avoid resource exhaustion.

## Implementation Tasks

- [x] **Task 1**: Confirm SQL Server JDBC BulkCopy expectations for LOBs and define type mappings
  - **Type**: Refactor
  - **Risk**: Medium - incorrect mappings can corrupt data or fail bulk copy
  - **Files**: [src/main/java/org/replicadb/manager/SQLServerBulkRecordAdapter.java](src/main/java/org/replicadb/manager/SQLServerBulkRecordAdapter.java)
  - **Changes**:
    - Review existing column-type mapping logic and identify LOB handling gaps for JDBC `ResultSet` sources.
    - Define mapping for `Types.BLOB`/`Types.LONGVARBINARY` → `Types.VARBINARY` and `Types.CLOB`/`Types.LONGNVARCHAR` → `Types.NVARCHAR` aligned with SQL Server `VARBINARY(MAX)` / `NVARCHAR(MAX)`.
    - Document expected object types for row data (e.g., `InputStream`, `Reader`).
  - **Impact**: Wrong mapping causes bulk copy errors or data truncation.
  - **Tests**: None (design step).
  - **Success**:
    - Mapping rules are explicit and aligned with SQL Server column types.
    - Streaming strategy decided for `BLOB`/`CLOB`.
    - No changes to runtime behavior yet.
  - **Dependencies**: None

- [x] **Task 2**: Implement JDBC `ResultSet` bulk record adapter with LOB streaming
  - **Type**: New Feature
  - **Risk**: High - affects core data path and driver compatibility
  - **Files**: [src/main/java/org/replicadb/manager/SQLServerResultSetBulkRecordAdapter.java](src/main/java/org/replicadb/manager/SQLServerResultSetBulkRecordAdapter.java)
  - **Changes**:
    - Create adapter implementing `ISQLServerBulkRecord` backed by a JDBC `ResultSet`.
    - Map column types using metadata with SQL Server–compatible substitutions (BLOB/LONGVARBINARY to VARBINARY, CLOB/LONGNVARCHAR to NVARCHAR).
    - Stream LOBs via `getBinaryStream()` / `getCharacterStream()` and provide row data as `InputStream`/`Reader`.
    - Ensure `next()` advances the `ResultSet` and handles exceptions clearly.
  - **Impact**: If adapter fails, bulk copy breaks for SQL Server sinks.
  - **Tests**: Re-enabled Oracle→SQLServer tests; optional manual validation with a BLOB-heavy table.
  - **Success**:
    - Bulk copy accepts Oracle `BLOB`/`CLOB` rows without type errors.
    - LOBs are streamed (no full in-memory load).
    - Adapter preserves existing non-LOB mappings.
    - No regression for non-LOB SQL Server bulk copy.
    - Clear error messages when metadata is incompatible.
  - **Dependencies**: Task 1

- [x] **Task 3**: Wire JDBC adapter into SQLServer bulk copy flow
  - **Type**: Refactor
  - **Risk**: Medium - switch in insert flow affects all SQL Server sinks
  - **Files**: [src/main/java/org/replicadb/manager/SQLServerManager.java](src/main/java/org/replicadb/manager/SQLServerManager.java)
  - **Changes**:
    - When `ResultSet` is not a `RowSet`, use the new adapter instead of passing `ResultSet` directly to `writeToServer()`.
    - Maintain existing column mappings and bulk copy options.
  - **Impact**: Incorrect wiring reintroduces blob error or breaks bulk copy.
  - **Tests**: Oracle→SQLServer test suite and at least one non-Oracle SQL Server sink test.
  - **Success**:
    - Bulk copy path always used (no fallback).
    - Oracle BLOB replication works end-to-end.
    - Existing SQL Server tests pass.
  - **Dependencies**: Task 2

- [x] **Task 4**: Re-enable Oracle→SQLServer tests and validate BLOB coverage
  - **Type**: Test
  - **Risk**: Medium - integration tests may expose driver edge cases
  - **Files**: [src/test/java/org/replicadb/oracle/Oracle2SqlserverTest.java](src/test/java/org/replicadb/oracle/Oracle2SqlserverTest.java)
  - **Changes**:
    - Remove `@Disabled` annotations tied to BLOB bulk copy limitation (including parallel variants).
    - Ensure test resources include BLOB columns and SQL Server sink uses `varbinary(max)` (already present).
  - **Impact**: Failing tests indicate bulk copy still incompatible with LOBs.
  - **Tests**: Oracle→SQLServer tests for complete, complete-atomic, incremental (and parallel variants if stable).
  - **Success**:
    - All previously disabled tests pass.
    - No change in expected row counts.
  - **Dependencies**: Task 3

- [x] **Task 5**: Update documentation in gh-pages branch
  - **Type**: Documentation
  - **Risk**: Low - documentation-only change
  - **Files**: gh-pages branch (paths TBD based on existing site structure)
  - **Changes**:
    - Document SQL Server bulk copy support for Oracle `BLOB`/`CLOB` replication.
    - Add any required notes about LOB streaming behavior and SQL Server column types (`VARBINARY(MAX)`, `NVARCHAR(MAX)`).
  - **Impact**: Users may miss the fix or configuration expectations if docs are not updated.
  - **Tests**: None
  - **Success**:
    - gh-pages documentation reflects LOB support for Oracle→SQL Server.
  - **Dependencies**: Task 4

- [x] **Task 6**: Stabilize LOB streaming for ResultSet bulk copy
  - **Type**: Bug Fix
  - **Risk**: Medium - affects SQL Server bulk copy for LOB columns
  - **Files**: [src/main/java/org/replicadb/manager/SQLServerResultSetBulkRecordAdapter.java](src/main/java/org/replicadb/manager/SQLServerResultSetBulkRecordAdapter.java)
  - **Changes**:
    - Defer LOB stream access until after non-LOB column reads to avoid closing streams mid-row.
    - Add Javadoc for public methods in the adapter.
  - **Impact**: Stream errors can abort bulk copy and fail replication tasks.
  - **Tests**: SQL Server-related replication tests that previously failed with stream errors.
  - **Success**:
    - No `stream is closed` errors during SQL Server bulk copy.
  - **Dependencies**: Task 3

- [x] **Task 7**: Stream LOBs only for LOB source types
  - **Type**: Bug Fix
  - **Risk**: Medium - affects SQL Server bulk copy for mixed column types
  - **Files**: [src/main/java/org/replicadb/manager/SQLServerResultSetBulkRecordAdapter.java](src/main/java/org/replicadb/manager/SQLServerResultSetBulkRecordAdapter.java)
  - **Changes**:
    - Use LOB streaming only when the source type is `BLOB`/`CLOB`/`LONG*`.
    - For non-LOB `VARBINARY`/`NVARCHAR`, read values via `getBytes()` / `getString()`.
  - **Impact**: Prevents invalid column length and closed stream errors for non-LOB columns.
  - **Tests**: SQL Server-related integration tests (MariaDB/MySQL/Oracle/SQL Server to SQL Server).
  - **Success**:
    - No `invalid column length` errors for non-LOB columns.
  - **Dependencies**: Task 6

- [x] **Task 8**: Respect sinkColumns mapping for ResultSet bulk copy
  - **Type**: Bug Fix
  - **Risk**: Medium - affects SQL Server column mapping logic
  - **Files**: [src/main/java/org/replicadb/manager/SQLServerManager.java](src/main/java/org/replicadb/manager/SQLServerManager.java)
  - **Changes**:
    - Use name-based mapping (`source column name` → `sink column name`) for JDBC `ResultSet` sources.
    - Keep index-based mapping for `RowSet` sources to avoid case sensitivity issues.
  - **Impact**: Prevents misaligned sink column mapping when sinkColumns are specified.
  - **Tests**: SQL Server-related integration tests using sinkColumns.
  - **Success**:
    - Bulk copy maps to intended sink columns.
  - **Dependencies**: Task 3

## Technical Reference
<details><summary><strong>Types & Data Structures</strong></summary>

- `SQLServerResultSetBulkRecordAdapter` implements `ISQLServerBulkRecord`
  - Fields: `ResultSet resultSet`, `ResultSetMetaData metaData`, `int columnCount`
  - Type mapping rules: `BLOB`/`LONGVARBINARY` → `VARBINARY`, `CLOB`/`LONGNVARCHAR` → `NVARCHAR`, `BOOLEAN` → `BIT`
</details>

<details><summary><strong>Files & Components</strong></summary>

- src/main/java/org/replicadb/manager/SQLServerResultSetBulkRecordAdapter.java — new adapter for JDBC `ResultSet` bulk copy.
- src/main/java/org/replicadb/manager/SQLServerManager.java — use adapter for non-RowSet results.
- src/main/java/org/replicadb/manager/SQLServerBulkRecordAdapter.java — reference for RowSet behavior and type mapping.
- src/test/java/org/replicadb/oracle/Oracle2SqlserverTest.java — re-enable BLOB tests.
</details>

<details><summary><strong>Functions & Methods</strong></summary>

- `SQLServerResultSetBulkRecordAdapter.getColumnType(int column)` — applies SQL Server compatible mappings.
- `SQLServerResultSetBulkRecordAdapter.getRowData()` — returns row values with LOB streaming.
- `SQLServerResultSetBulkRecordAdapter.next()` — advances result set.
</details>

<details><summary><strong>Testing Strategy</strong></summary>

- Run Oracle→SQLServer integration tests (complete, complete-atomic, incremental).
- Validate non-LOB SQL Server sinks remain stable (one existing SQL Server sink test).
</details>

<details><summary><strong>Code Examples</strong></summary>

- Adapter usage: `bulkCopy.writeToServer(new SQLServerResultSetBulkRecordAdapter(resultSet));`
</details>

--- END OF DOCUMENT ---
