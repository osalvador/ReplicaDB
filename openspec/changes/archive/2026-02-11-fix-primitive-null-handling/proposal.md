# Fix Primitive NULL Handling in JDBC Managers

## Why

Issue #51 reported that NULL INTEGER values from Denodo sources convert to 0 when inserted into Oracle sinks, causing silent data corruption. Investigation reveals this is a systematic bug affecting multiple database managers (OracleManager, StandardJDBCManager, SqliteManager) and multiple primitive data types (INTEGER, BIGINT, NUMERIC, DOUBLE, FLOAT, DATE, TIMESTAMP).

**Root Cause**: These managers call `resultSet.getInt(i)` directly without checking `resultSet.wasNull()`. Per JDBC specification, primitive getter methods (getInt, getDouble, etc.) return default values (0, 0.0) for NULL database values and set an internal flag that must be checked via `wasNull()`.

**Business Impact**: NULL values carry semantic meaning distinct from zero (e.g., "unknown profit" vs "zero profit"). Converting NULL→0 silently corrupts business data without error messages, making the issue invisible until discovered through data analysis.

**Test Gap**: Oracle test data (`oracle-source.sql`) contains no NULL values, so this bug went undetected. DB2 tests include a NULL test row (row 4096), which forced DB2Manager to implement correct NULL handling—this manager serves as the reference implementation.

## What Changes

Implement consistent NULL-safe handling for all primitive data types across affected JDBC managers:

1. **OracleManager**: Add `resultSet.wasNull()` checks for INTEGER, BIGINT, NUMERIC, DOUBLE, FLOAT, DATE, TIMESTAMP types
2. **StandardJDBCManager**: Same fixes (affects multiple databases using fallback manager)
3. **SqliteManager**: Same fixes
4. **Test Data**: Add NULL test rows to oracle-source.sql and other test fixtures lacking NULL coverage

**Pattern to apply** (following DB2Manager reference):
```java
// ❌ Current broken code
case Types.INTEGER:
    ps.setInt(i, resultSet.getInt(i));  
    break;

// ✅ Correct NULL-safe pattern (primitives)
case Types.INTEGER:
case Types.TINYINT:
case Types.SMALLINT:
    int intVal = resultSet.getInt(i);
    if (resultSet.wasNull()) {
        ps.setNull(i, targetType);
    } else {
        ps.setInt(i, intVal);
    }
    break;

// ✅ Correct NULL-safe pattern (objects - belt-and-suspenders)
case Types.BIGINT:
case Types.NUMERIC:
case Types.DECIMAL:
    java.math.BigDecimal bdVal = resultSet.getBigDecimal(i);
    if (resultSet.wasNull() || bdVal == null) {
        ps.setNull(i, targetType);
    } else {
        ps.setBigDecimal(i, bdVal);
    }
    break;

// ✅ Correct DATE with string fallback (DB2Manager pattern)
case Types.DATE:
    try {
        java.sql.Date dateVal = resultSet.getDate(i);
        if (resultSet.wasNull() || dateVal == null) {
            ps.setNull(i, Types.DATE);
        } else {
            ps.setDate(i, dateVal);
        }
    } catch (SQLException e) {
        // Fallback for string-based dates from SQLite/MongoDB
        String dateStr = resultSet.getString(i);
        if (dateStr == null || resultSet.wasNull()) {
            ps.setNull(i, Types.DATE);
        } else {
            // Parse and convert string to java.sql.Date
        }
    }
    break;
```

## Capabilities

### Modified Capabilities
- `jdbc-type-mapping`: JDBC ResultSet→PreparedStatement type conversion now preserves NULL values for primitive types

## Impact

**Modified Files**:
- [src/main/java/org/replicadb/manager/OracleManager.java](src/main/java/org/replicadb/manager/OracleManager.java): Fix NULL handling in `insertDataToTable()` for ~10 primitive type cases
- [src/main/java/org/replicadb/manager/StandardJDBCManager.java](src/main/java/org/replicadb/manager/StandardJDBCManager.java): Same fixes
- [src/main/java/org/replicadb/manager/SqliteManager.java](src/main/java/org/replicadb/manager/SqliteManager.java): Same fixes
- [src/test/resources/oracle/oracle-source.sql](src/test/resources/oracle/oracle-source.sql): Add NULL test row (following DB2 pattern)
- [src/test/resources/sqlite/sqlite-source.sql](src/test/resources/sqlite/sqlite-source.sql): Add NULL test row (if exists)

**Affected Data Types** (all types get NULL checks):
- Primitive numeric: INTEGER, TINYINT, SMALLINT, BIGINT, DOUBLE, FLOAT, REAL
- Object numeric: NUMERIC, DECIMAL (getBigDecimal returns object but check null anyway)
- Temporal: DATE, TIMESTAMP (with/without timezone) - follow DB2Manager string fallback pattern
- String: VARCHAR, CHAR, LONGVARCHAR (getString returns object, add explicit null checks)
- Binary: BINARY, VARBINARY, LONGVARBINARY (getBytes returns object, add explicit null checks)
- Boolean: BOOLEAN, BIT (getBoolean is primitive, needs wasNull())

**Verification Strategy**:
- Create specific unit test for each affected manager (OracleManager, StandardJDBCManager, SqliteManager)
- Each test verifies NULL handling for all data types (numeric, temporal, string, binary, boolean)
- Tests use real databases via TestContainers to validate end-to-end NULL preservation
- Run existing integration tests to ensure no regressions

**Database Compatibility**: No breaking changes—only fixes silent corruption. NULL values now correctly flow through instead of converting to defaults.
