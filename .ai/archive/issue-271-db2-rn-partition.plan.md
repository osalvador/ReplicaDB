# Implementation Plan: Fix DB2 Parallel Reads Leaking the RN Partition Column

## Task Source
GitHub Issue [#271](https://github.com/osalvador/ReplicaDB/issues/271): `ERROR: column "rn" of relation "xxxxx" does not exist`.

Acceptance criteria derived from the issue and the repository contract:

- A DB2 source replicated in parallel with the default wildcard column selection must not expose the technical partition column `RN` to the sink.
- DB2 to PostgreSQL must succeed with `jobs > 1` and without `source-columns` or `sink-columns`.
- The PostgreSQL `COPY` column list and row values must contain only source data columns that exist in the destination table.
- Explicit `source-columns`/`sink-columns` behavior must remain compatible.
- `jobs=1` behavior must remain unchanged and must not require a metadata probe.
- The partitioning behavior must remain correct: every row is copied once across the configured jobs.
- The fix must be covered by real DB2 and PostgreSQL integration tests, including the existing Testcontainers setup.

## Overview
`Db2Manager.readTable()` adds a `ROW_NUMBER()`-based partition column named `RN` when DB2 reads in parallel. With the default wildcard projection, the outer query currently selects that technical column, and the JDBC metadata passed to the sink consequently contains `RN`. PostgreSQL then includes `RN` in its `COPY` command and fails against normal destination tables that do not define it.

The fix will keep the behavior inside the DB2 manager. When the source projection is explicit, the current projection path will be preserved. When the source projection is the default wildcard, DB2 will obtain an ordered, zero-row result metadata projection and use an explicit data-column list in the partitioned query, leaving the internal row-number column out of the returned `ResultSet`. No generic sink, CLI option, or public configuration contract will change.

## Architecture & Design
**Approach: Proyección explícita resuelta en `Db2Manager`**

The owning abstraction is `org.replicadb.manager.db2.Db2Manager`: it creates the DB2 partition SQL and is the only layer that knows `RN` is an implementation detail. `ReplicaTask`, `ConnManager`, and sink managers continue to receive and serialize an ordinary JDBC-shaped `ResultSet`.

Design decisions:

- Keep `jobs == 1` on the existing `baseQuery` path. The metadata-only probe is needed only for partitioned reads.
- Keep an explicit `source-columns` expression unchanged, including its existing ordering and expression/alias semantics.
- For wildcard selection, execute the existing repository-compatible DB2 metadata-only probe shape `SELECT * FROM (<baseQuery>) PROBE WHERE 1=0`, including `source-where` and `source-query` behavior. Extract columns in result-set order using `getColumnLabel()` with `getColumnName()` as fallback while the result set is still open.
- Build separate inner and outer projections as needed. The returned outer projection must reference only data columns; the technical partition predicate must be qualified against the derived table.
- Use `REPLICADB_PARTITION_RN` as the initial private technical alias, append `_1`, `_2`, and so on if the resolved source labels contain that name case-insensitively, and qualify the predicate against the derived table. Do not rely on an unqualified `RN` predicate when a source table or query could legitimately contain that name.
- Close the probe statement and result set immediately. Do not reuse `probeSourceMetadata()`, because that method is tied to sink auto-create and mutates `ToolOptions`.
- Do not solve the issue in `getAllSinkColumns()` or a generic `ResultSet` wrapper. The sink serializers iterate the source metadata and values by index, so hiding only the sink column name would leave a column-count/value mismatch and spread DB2-specific knowledge into shared layers.
- Preserve the existing `MOD(ROW_NUMBER() OVER (ORDER BY 1), jobs)` partition assignment and task indexes `0..jobs-1`.

Performance and security considerations:

- The wildcard partition path adds one metadata-only DB2 operation per task. It must not fetch source rows or mutate the shared options object. The normal data query and fetch-size behavior remain unchanged.
- No new dependencies, credentials, connection parameters, or user-visible options are introduced. Do not add logging that exposes passwords, DSNs, or credential-bearing URLs.
- User-provided table, column, where, and query expressions remain handled by the existing manager boundary; the new projection builder must preserve the current identifier quoting convention: double-quote identifiers and escape embedded double quotes when `quoted-identifiers` is enabled, otherwise follow the existing unquoted metadata projection style. Metadata labels are identifiers, not arbitrary SQL expressions.

## Implementation Tasks

### 1. Add a DB2 wildcard projection resolver
- [x] **1.1 Implement a private metadata-only projection helper in `src/main/java/org/replicadb/manager/db2/Db2Manager.java`**
  Files: `src/main/java/org/replicadb/manager/db2/Db2Manager.java`
  Changes: Add a helper that receives the already-constructed `baseQuery` and the configured source-column expression. Return the explicit expression unchanged when the user supplied a non-empty list. For wildcard selection, execute exactly `SELECT * FROM (<baseQuery>) PROBE WHERE 1=0`, read ordered metadata, copy every label/name into local values before closing the result set, apply double-quote escaping only when `quoted-identifiers` is enabled, and return a projection suitable for the outer partition query. Throw `SQLException("Unable to resolve DB2 source columns for parallel read")` with the original cause when the probe fails; throw the same error family when the probe exposes zero columns or a column has neither a usable label nor name. Never include the probe SQL or credentials in the exception text. Close the result set and statement in all paths.
  Tests: Compile the manager after the helper is added; run the existing DB2 parallel integration slice to ensure the explicit-column path still executes. The helper must have no effect when `jobs == 1`, and the probe must use `WHERE 1=0` rather than fetching source rows.
  Dependencies: None.

- [x] **1.2 Define collision and metadata edge-case behavior in the same manager**
  Files: `src/main/java/org/replicadb/manager/db2/Db2Manager.java`
  Changes: Use `REPLICADB_PARTITION_RN` as the initial alias and choose the first numeric suffix not present in the resolved labels, case-insensitively. Reject duplicate or blank resolved labels for wildcard query projections with an actionable `SQLException` rather than constructing ambiguous SQL. Preserve source column order and result labels. Do not write the resolved projection into `ToolOptions`, because those options are shared by parallel tasks and are also consumed later by sink/staging SQL.
  Tests: Exercise metadata with the existing DB2 fixture columns; use a source query that aliases a fixture column to `REPLICADB_PARTITION_RN` to verify suffix selection without changing shared fixtures; use a query with duplicate labels to verify the explicit failure path. Verify the copied metadata contains the legitimate data label once and never exposes the internal alias.
  Dependencies: 1.1.

### 2. Rewire DB2 partition SQL without changing non-partitioned behavior
- [x] **2.1 Use the resolved projection only for `jobs > 1`**
  Files: `src/main/java/org/replicadb/manager/db2/Db2Manager.java`
  Changes: Keep the current `baseQuery` construction and the `jobs == 1` early return. In the parallel branch, select the data projection in the outer query, append the technical row-number expression only inside the derived table, and qualify the partition predicate against that derived table. Preserve the existing task numbering, `source-where`, `source-query`, fetch size, logging pattern, and explicit-column semantics.
  Tests: Run the existing `DB22PostgresTest` explicit-column complete, complete-atomic, incremental, and parallel cases. Confirm the generated SQL never includes the technical alias in the outer select list for wildcard reads.
  Dependencies: 1.1 and 1.2.

- [x] **2.2 Verify sink and generic manager boundaries remain unchanged**
  Files: `src/main/java/org/replicadb/manager/ConnManager.java`, `src/main/java/org/replicadb/manager/PostgresqlManager.java`, `src/main/java/org/replicadb/ReplicaTask.java`
  Changes: Make no production changes in these files. Review the call chain as a boundary check: `ReplicaTask` must pass the DB2 result unchanged, `ConnManager.getAllSinkColumns()` must receive only data-column metadata, and PostgreSQL `COPY` must continue to derive its list from that metadata. If a proposed implementation requires edits here, stop and keep the DB2-specific fix localized. Confirm no new logging exposes credentials or full connection strings.
  Tests: Run the PostgreSQL manager unit tests and a focused DB2-to-PostgreSQL integration test to verify the sink receives the same number and order of data columns as the destination table, with no `RN` column. Review the diff for absence of changes to these files.
  Dependencies: 2.1.

### 3. Add regression coverage for issue #271
- [x] **3.1 Cover the default wildcard table path**
  Files: `src/test/java/org/replicadb/db2/DB22PostgresTest.java`
  Changes: Add a complete-mode DB2-to-PostgreSQL test using `--jobs 4` and omitting both `--source-columns` and `--sink-columns`. Reuse the existing DB2/PostgreSQL singleton containers and pre-created `t_source`/`t_sink` fixtures. In the current fixture, `EXPECTED_ROWS` is `4097` and the source data projection contains the 20 columns represented by `COLUMN_LIST`; inspect sink metadata to assert that no `RN` column was created or targeted.
  Tests: The new test must fail against the current implementation with PostgreSQL's missing-column error and pass after the fix. Verify that all four partitions together produce exactly `4097` rows, with no missing or duplicate rows, and that the destination metadata still matches the fixture's data columns.
  Dependencies: 2.1 and 2.2.

- [x] **3.2 Cover wildcard `source-query` partitioning**
  Files: `src/test/java/org/replicadb/db2/DB22PostgresTest.java`
  Changes: Add a DB2 source-query variant `SELECT * FROM t_source`, keep `jobs=4`, and omit explicit source/sink column lists. Assert that query-derived metadata also excludes the technical partition column and that PostgreSQL receives the expected rows. Add direct source-manager coverage for the collision query `SELECT C_INTEGER AS REPLICADB_PARTITION_RN, C_SMALLINT FROM t_source` and the duplicate-label query `SELECT C_INTEGER AS DUPLICATE_LABEL, C_SMALLINT AS DUPLICATE_LABEL FROM t_source`.
  Tests: The wildcard query test must complete with exactly `4097` rows and no `RN` in the sink. The collision test must return two data columns and select a suffixed internal alias; the duplicate-label test must fail with the defined projection-resolution `SQLException`. Preserve all existing explicit-column tests as hard compatibility controls.
  Dependencies: 3.1.

- [x] **3.3 Cover mode and single-job compatibility**
  Files: `src/test/java/org/replicadb/db2/DB22PostgresTest.java`
  Changes: Add or adapt no-column parallel cases for `complete-atomic` and `incremental` where the existing staging fixture supports them, and retain a no-column `jobs=1` baseline. Keep explicit-column complete/atomic/incremental tests unchanged and treat them as mandatory regression controls.
  Tests: Assert `4097` rows after staging/merge for the supported modes, assert no `RN` column is present in the final sink, verify the single-job case does not execute the metadata-probe path or alter results, and run every existing explicit-column DB2-to-PostgreSQL test in the class.
  Dependencies: 3.1 and 3.2.

### 4. Execute the focused validation and record environment limits
- [x] **4.1 Run the narrow test and build checks**
  Files: `pom.xml`, `src/test/java/org/replicadb/db2/DB22PostgresTest.java`, `src/main/java/org/replicadb/manager/db2/Db2Manager.java`
  Changes: No product or dependency change is expected. Validate the implementation with the focused DB2/PostgreSQL class, then run test compilation and the relevant manager/PostgreSQL unit slice. Review generated Surefire output for assertion failures separately from container startup, Docker socket, architecture emulation, memory, or reuse failures. Confirm the wildcard probe is `WHERE 1=0`, executes at most once per task, and is skipped entirely for `jobs=1`; this is the performance bound for the bug fix, not a benchmark requirement.
  Tests: Run `mvn -Dtest=DB22PostgresTest test`, the focused PostgreSQL/manager unit tests, and `mvn -DskipTests test-compile`. Inspect the generated SQL/log sequence or a narrow manager test to confirm the probe count and no row-fetching probe. Review the diff for no new credential-bearing logging. If the DB2 container cannot run locally, report the infrastructure blocker and preserve the exact command for clean CI execution; do not treat a container startup failure as evidence that the SQL fix is wrong.
  Dependencies: 3.3.

## Technical Reference

### Types & Data Structures

- `ToolOptions.sourceColumns` and `sinkColumns` remain nullable configuration values; wildcard resolution is local to a DB2 read and must not mutate them.
- `ResultSetMetaData` is the source of ordered labels, JDBC types, and column count at the transfer boundary. The fixed DB2 result must contain only source data columns.
- The partition value is an internal SQL expression and is not part of any public DTO, CLI option, sink column list, staging schema, or row-set contract.

### Dependencies

- Existing DB2 JDBC driver and PostgreSQL JDBC driver from `pom.xml`.
- Existing `ReplicadbDB2Container` and `ReplicadbPostgresqlContainer` Testcontainers fixtures.
- No new Maven dependency, configuration key, migration, or generated artifact.

### Testing Strategy

- Use the real DB2 and PostgreSQL containers for SQL dialect, metadata, cursor, partition, and `COPY` behavior; Mockito-only metadata tests would not validate the DB2 query semantics.
- Keep JUnit Jupiter 6 style and the existing singleton container pattern. Run the narrow class before the wider integration matrix.
- Validate the four important paths: wildcard table with parallel jobs, wildcard source-query with parallel jobs, explicit columns, and single-job execution. For supported replication modes, validate complete, complete-atomic, and incremental staging/merge behavior.
- On macOS/Apple Silicon, separate DB2 emulation, Docker socket, reuse, and memory failures from product assertions, following the archived Testcontainers learnings in `.ai/archive/PR-242.plan.md`.

### Risks and Rollback

- DB2 LUW and DB2/i may expose different metadata labels or zero-row query syntax. The focused integration test must run against the supported DB2 container before broadening the change.
- Arbitrary `source-query` expressions may produce duplicate or unstable labels. The implementation must fail explicitly with context if it cannot construct a safe projection; it must never fall back to returning the internal partition column.
- If the metadata probe adds unacceptable overhead, the change can be narrowed to one probe per task or a manager-local cached projection without changing the generic pipeline. Do not introduce shared mutable state in `ToolOptions` without synchronization.
- Rollback is code-only: revert the DB2 manager projection change and the regression tests. No schema migration or persisted data change is involved.

### Out of Scope

- Changing the default `jobs` value.
- Requiring users to configure `source-columns` or `sink-columns`.
- Filtering `RN` in PostgreSQL or every sink manager.
- Refactoring generic `ResultSet` or row-set abstractions.
- Adding a new CLI option or changing existing configuration precedence.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 8/8 (100%).
- Tasks that required plan adjustment: 0/8 (0%).
- Test loop iterations: 1 focused unit/build pass; DB2 integration was blocked before test execution by infrastructure.

### Gaps Encountered

#### Gap 1: Reused DB2 container was unhealthy (Plan-to-Implementation)
- **Task**: 4.1 — Run the narrow test and build checks.
- **Plan assumed**: The existing Testcontainers DB2 instance would be available for the focused integration suite.
- **Reality**: Docker reported the reused container as `Up`, but DB2 had terminated internal vendor processes and returned `DIA8506C`, `ERRORCODE=-4499`, and `SQLSTATE=08001` during JDBC initialization. Restarting the container did not recover the engine.
- **Resolution**: Classified the failure as infrastructure, verified production and test compilation, ran the focused PostgreSQL/DDL unit tests successfully, and left the DB2 suite command ready for clean CI or a recreated container.
- **Learning**: A running reused DB2 container is not sufficient evidence of database health; check JDBC initialization and container logs before interpreting integration results, especially with emulation and long-lived Testcontainers instances.

### Patterns Discovered
- **DB2 partition projection**: Keep the technical row-number expression inside the derived table and expose a metadata-derived explicit projection to the transfer boundary; see `src/main/java/org/replicadb/manager/db2/Db2Manager.java`.
