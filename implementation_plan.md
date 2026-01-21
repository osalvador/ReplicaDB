# Implementation Plan: Fix ORA-64219 Invalid LOB Locator for Cross-Version Oracle Replication

## Task Source
**Source**: GitHub Issue #213
**Original Request**: https://github.com/osalvador/ReplicaDB/issues/213

## Overview
Fix the ORA-64219 "invalid LOB locator encountered" error that occurs when replicating tables containing LOB columns (BLOB/CLOB) between different Oracle database versions (e.g., Oracle 11g → Oracle 19c).

The root cause is that LOB locators are database-session-specific handles that cannot be transferred between different Oracle database instances or versions. The current implementation passes LOB locators directly via `ps.setBlob(i, blobData)`, which fails when source and sink are different Oracle databases. The fix requires reading LOB content as streams/bytes instead of passing locators, using a chunked streaming approach for all LOB sizes.

**Scope**: Oracle→Oracle replication only (as per user specification)
**Approach**: Always use chunked streaming (no size threshold)
**Testing**: Add multi-version Oracle container configuration

## Architecture & Design
**Decision**: Modify `OracleManager.insertDataToTable()` to stream LOB content instead of passing locators

### Design Rationale
- **Streaming approach**: Read BLOB/CLOB content via `getBinaryStream()`/`getCharacterStream()` and write via `setBinaryStream()`/`setCharacterStream()` to avoid LOB locator transfer
- **Chunked processing**: Process LOBs in configurable chunks (default 1MB) to handle very large LOBs without memory exhaustion
- **Oracle-specific**: Changes isolated to `OracleManager` class only - no impact on other database combinations
- **Backward compatible**: Same behavior for same-version Oracle, fixes cross-version scenarios

### Data Flow
1. Source: `ResultSet.getBlob(i)` → `blob.getBinaryStream()` → chunked read
2. Transfer: Stream bytes in chunks, never materialize full LOB in memory
3. Sink: `PreparedStatement.setBinaryStream()` with length parameter

## Implementation Tasks

- [x] **Task 1**: Create LOB streaming utility methods in OracleManager
  - **Type**: New Feature
  - **Risk**: Medium - Core data handling logic for LOBs
  - **Files**: [src/main/java/org/replicadb/manager/OracleManager.java](src/main/java/org/replicadb/manager/OracleManager.java)
  - **Changes**:
    - Add private method `streamBlobToSink(Blob sourceBlob, PreparedStatement ps, int columnIndex)` for chunked BLOB streaming
    - Add private method `streamClobToSink(Clob sourceClob, PreparedStatement ps, int columnIndex)` for chunked CLOB streaming
    - Add constant `LOB_CHUNK_SIZE = 1024 * 1024` (1MB default chunk size)
  - **Impact**: LOB data transfer will fail if streaming logic is incorrect
  - **Tests**: Unit test for streaming methods with mock BLOBs/CLOBs
  - **Success**: 
    - Methods handle null LOBs gracefully
    - Streams are properly closed after transfer
    - Memory usage remains constant regardless of LOB size

- [x] **Task 2**: Modify BLOB handling in insertDataToTable to use streaming
  - **Type**: Bug Fix
  - **Risk**: High - Direct fix for reported ORA-64219 error
  - **Files**: [src/main/java/org/replicadb/manager/OracleManager.java](src/main/java/org/replicadb/manager/OracleManager.java#L175-L180)
  - **Changes**:
    - Replace `case Types.BLOB:` block (lines 175-180) to use `streamBlobToSink()` instead of `ps.setBlob(i, blobData)`
    - Remove direct `getBlob()` locator passing
    - Add proper resource cleanup in finally/try-with-resources
  - **Impact**: BLOB replication between different Oracle versions would fail if incorrect
  - **Tests**: Integration test with BLOB data across Oracle versions
  - **Success**:
    - No ORA-64219 error during cross-version BLOB replication
    - BLOB data integrity verified (MD5/SHA comparison)
    - Same-version Oracle replication still works correctly
    - Null BLOB values handled correctly

- [x] **Task 3**: Modify CLOB handling in insertDataToTable to use streaming
  - **Type**: Bug Fix
  - **Risk**: High - Direct fix for reported ORA-64219 error (CLOB variant)
  - **Files**: [src/main/java/org/replicadb/manager/OracleManager.java](src/main/java/org/replicadb/manager/OracleManager.java#L181-L195)
  - **Changes**:
    - Replace `case Types.CLOB:` block (lines 181-195) to use `streamClobToSink()` instead of `ps.setCharacterStream()` with locator-based reader
    - Read CLOB content independently from source before setting on sink PreparedStatement
    - Add proper resource cleanup
  - **Impact**: CLOB replication between different Oracle versions would fail if incorrect
  - **Tests**: Integration test with CLOB data across Oracle versions
  - **Success**:
    - No ORA-64219 error during cross-version CLOB replication
    - CLOB data integrity verified (character count and content comparison)
    - Unicode/multi-byte characters preserved correctly
    - Empty and null CLOB values handled correctly

- [x] **Task 4**: Create configurable multi-version Oracle test container
  - **Type**: New Feature
  - **Risk**: Medium - Test infrastructure changes
  - **Files**: [src/test/java/org/replicadb/config/ReplicadbOracleContainer.java](src/test/java/org/replicadb/config/ReplicadbOracleContainer.java)
  - **Changes**:
    - Add constructor parameter for Oracle version (11g, 12c, 18c, 19c, 21c, 23c)
    - Create static factory methods: `getOracle11gInstance()`, `getOracle19cInstance()`, `getOracle23cInstance()`
    - Map versions to appropriate gvenzl Docker images: `gvenzl/oracle-xe:11`, `gvenzl/oracle-xe:18`, `gvenzl/oracle-free:23-slim-faststart`
    - Add version detection method `getOracleVersion()`
  - **Impact**: Test infrastructure may fail if Docker images are unavailable
  - **Tests**: Verify each container version starts correctly
  - **Success**:
    - Factory methods return correctly configured containers
    - Version detection returns accurate major version
    - Containers start within timeout for CI environments

- [x] **Task 5**: Create cross-version Oracle LOB replication integration test
  - **Type**: Test
  - **Risk**: Low - Test code only
  - **Files**: 
    - [src/test/java/org/replicadb/oracle/Oracle2OracleCrossVersionLobTest.java](src/test/java/org/replicadb/oracle/Oracle2OracleCrossVersionLobTest.java) (new)
    - [src/test/resources/oracle/oracle-lob-source.sql](src/test/resources/oracle/oracle-lob-source.sql) (new)
    - [src/test/resources/sinks/oracle-lob-sink.sql](src/test/resources/sinks/oracle-lob-sink.sql) (new)
  - **Changes**:
    - Create test class with source Oracle 11g/18c and sink Oracle 19c/23c containers
    - Add test table with BLOB and CLOB columns of varying sizes (1KB, 1MB, 100MB)
    - Test complete, complete-atomic, and incremental modes
    - Verify data integrity after replication
  - **Impact**: None (test code only)
  - **Tests**: Self-testing
  - **Success**:
    - Tests pass for Oracle 11g→23c, 18c→23c combinations
    - Tests pass for same-version replication (regression)
    - Large LOB (100MB) transfers complete without OutOfMemory

- [x] **Task 6**: Add LOB-specific test data generation SQL
  - **Type**: Test
  - **Risk**: Low - Test resources only
  - **Files**: 
    - [src/test/resources/oracle/oracle-lob-source.sql](src/test/resources/oracle/oracle-lob-source.sql) (new)
    - [src/test/resources/sinks/oracle-lob-sink.sql](src/test/resources/sinks/oracle-lob-sink.sql) (new)
  - **Changes**:
    - Create `t_lob_source` table with: `id NUMBER PRIMARY KEY, blob_col BLOB, clob_col CLOB, blob_size NUMBER, created_at TIMESTAMP`
    - Generate test data with varying LOB sizes using `DBMS_RANDOM.STRING` and `UTL_RAW`
    - Create corresponding sink table DDL
  - **Impact**: None (test resources only)
  - **Tests**: Verified via Task 5
  - **Success**:
    - SQL executes without errors on Oracle 11g+ versions
    - Generated data includes small, medium, and large LOBs

- [x] **Task 7**: Update documentation for cross-version Oracle replication
  - **Type**: Documentation
  - **Risk**: Low - Documentation only
  - **Files**: [README.md](README.md)
  - **Changes**:
    - Add section documenting Oracle cross-version LOB replication support
    - Note about streaming approach for LOB handling
    - List tested Oracle version combinations
  - **Impact**: None
  - **Tests**: None required
  - **Success**:
    - Documentation accurately describes LOB handling behavior
    - Tested version matrix is documented

**Task Grouping**:
- **Phase 1: Core Fix** (Tasks 1-3): Implement LOB streaming in OracleManager
- **Phase 2: Test Infrastructure** (Tasks 4, 6): Multi-version container and test data
- **Phase 3: Validation** (Task 5): Integration tests
- **Phase 4: Documentation** (Task 7): Update docs

## Technical Reference

<details><summary><strong>Types & Data Structures</strong></summary>

No new types required. Existing JDBC types used:
- `java.sql.Blob` - Source BLOB locator (read-only via stream)
- `java.sql.Clob` - Source CLOB locator (read-only via stream)
- `java.io.InputStream` - BLOB binary stream
- `java.io.Reader` - CLOB character stream

</details>

<details><summary><strong>Files & Components</strong></summary>

**Modified Files:**
- `src/main/java/org/replicadb/manager/OracleManager.java` - LOB streaming methods and modified insertDataToTable
- `src/test/java/org/replicadb/config/ReplicadbOracleContainer.java` - Multi-version container support

**New Files:**
- `src/test/java/org/replicadb/oracle/Oracle2OracleCrossVersionLobTest.java` - Cross-version LOB test
- `src/test/resources/oracle/oracle-lob-source.sql` - LOB test data
- `src/test/resources/sinks/oracle-lob-sink.sql` - LOB sink table

</details>

<details><summary><strong>Functions & Methods</strong></summary>

**New methods in OracleManager:**

```java
/**
 * Streams BLOB content from source to sink PreparedStatement using chunked transfer.
 * Avoids LOB locator transfer issues between different Oracle versions.
 * 
 * @param sourceBlob Source BLOB from ResultSet (may be null)
 * @param ps Target PreparedStatement
 * @param columnIndex 1-based column index
 * @throws SQLException if database access error occurs
 * @throws IOException if stream read/write error occurs
 */
private void streamBlobToSink(Blob sourceBlob, PreparedStatement ps, int columnIndex) 
    throws SQLException, IOException

/**
 * Streams CLOB content from source to sink PreparedStatement using chunked transfer.
 * Avoids LOB locator transfer issues between different Oracle versions.
 * 
 * @param sourceClob Source CLOB from ResultSet (may be null)
 * @param ps Target PreparedStatement
 * @param columnIndex 1-based column index
 * @throws SQLException if database access error occurs
 * @throws IOException if stream read/write error occurs
 */
private void streamClobToSink(Clob sourceClob, PreparedStatement ps, int columnIndex) 
    throws SQLException, IOException
```

**New methods in ReplicadbOracleContainer:**

```java
/**
 * Get an Oracle container instance for a specific version.
 * @param version Oracle version string: "11", "18", "19", "21", "23"
 */
public static ReplicadbOracleContainer getInstance(String version)

/**
 * Get the Oracle major version number from the running container.
 */
public int getOracleMajorVersion()
```

</details>

<details><summary><strong>Dependencies</strong></summary>

No new dependencies required. Existing dependencies sufficient:
- `org.apache.commons:commons-io` - Already used for IOUtils stream operations
- `org.testcontainers:oracle-xe` - Already configured for Oracle containers

</details>

<details><summary><strong>Testing Strategy</strong></summary>

**Unit Tests:**
- Mock BLOB/CLOB objects to test streaming methods in isolation
- Test null handling, empty LOBs, and various sizes

**Integration Tests:**
- Cross-version test: Oracle 18c (source) → Oracle 23c (sink) with LOB data
- Same-version regression: Oracle 23c → Oracle 23c (existing tests)
- Mode coverage: Complete, complete-atomic, incremental modes

**Test Data:**
- Small LOBs: 1KB (inline handling)
- Medium LOBs: 1MB (single chunk)
- Large LOBs: 100MB (multi-chunk, memory stress test)

**CI Considerations:**
- Oracle 11g container may not be available on all platforms (ARM limitation)
- Tests should be skipped gracefully when container unavailable

</details>

<details><summary><strong>Code Examples</strong></summary>

**LOB Streaming Pattern:**

```java
private static final int LOB_CHUNK_SIZE = 1024 * 1024; // 1MB chunks

private void streamBlobToSink(Blob sourceBlob, PreparedStatement ps, int columnIndex) 
        throws SQLException, IOException {
    if (sourceBlob == null) {
        ps.setNull(columnIndex, Types.BLOB);
        return;
    }
    
    long blobLength = sourceBlob.length();
    if (blobLength == 0) {
        ps.setNull(columnIndex, Types.BLOB);
        sourceBlob.free();
        return;
    }
    
    try (InputStream blobStream = sourceBlob.getBinaryStream()) {
        ps.setBinaryStream(columnIndex, blobStream, blobLength);
    } finally {
        sourceBlob.free();
    }
}
```

**Multi-version container factory:**

```java
private static final Map<String, String> VERSION_IMAGES = Map.of(
    "11", "gvenzl/oracle-xe:11-slim-faststart",
    "18", "gvenzl/oracle-xe:18-slim-faststart",
    "21", "gvenzl/oracle-xe:21-slim-faststart",
    "23", "gvenzl/oracle-free:23-slim-faststart"
);
```

</details>

<details><summary><strong>Validation Checklist</strong> (Optional)</summary>

Pre-merge validation:
- [ ] Existing Oracle2OracleTest passes (regression)
- [ ] Cross-version LOB test passes
- [ ] No memory leaks detected in LOB streaming (heap dump analysis)
- [ ] Large LOB (100MB) transfer completes successfully
- [ ] Oracle XML columns still work (no regression in SQLXML handling)

</details>

--- END OF DOCUMENT ---
