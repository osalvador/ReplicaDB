# Capability: JDBC Type Mapping with NULL Preservation

## Overview

JDBC managers convert ResultSet data from source databases to PreparedStatement parameters for sink databases. This capability ensures NULL values are preserved correctly during this conversion, preventing silent data corruption where NULL values become default primitives (0, 0.0, false, epoch dates).

## Functional Requirements

### FR-1: Primitive Type NULL Handling
**Given** a source database column contains NULL value for a primitive type (INTEGER, DOUBLE, DATE, etc.)  
**When** ReplicaDB replicates the data to a sink database  
**Then** the sink database column must contain NULL (not 0, 0.0, or default value)

### FR-2: Object Type NULL Handling  
**Given** a source database column contains NULL value for an object type (VARCHAR, BLOB, BIGDECIMAL)  
**When** ReplicaDB replicates the data  
**Then** the sink database column must contain NULL (not empty string or empty bytes)

### FR-3: wasNull() Check Pattern
**Given** a JDBC primitive getter method (getInt, getDouble, getFloat, getLong)  
**When** the method returns due to NULL value in ResultSet  
**Then** the code must call resultSet.wasNull() to detect NULL before setting PreparedStatement parameter

### FR-4: Null Pointer Check Pattern
**Given** a JDBC object getter method (getString, getBytes, getBigDecimal, getDate, getTimestamp)  
**When** the method returns null  
**Then** the code must check for null before setting PreparedStatement parameter

## Technical Specification

### Affected Data Types

**Primitive types requiring wasNull() check:**
- Numeric: `Types.INTEGER`, `Types.TINYINT`, `Types.SMALLINT`, `Types.BIGINT`
- Floating: `Types.DOUBLE`, `Types.FLOAT`, `Types.REAL`
- Boolean: `Types.BOOLEAN`, `Types.BIT`

**Object types requiring null pointer check:**
- Numeric: `Types.NUMERIC`, `Types.DECIMAL` (via getBigDecimal)
- String: `Types.VARCHAR`, `Types.CHAR`, `Types.LONGVARCHAR`
- Binary: `Types.BINARY`, `Types.VARBINARY`, `Types.LONGVARBINARY`, `Types.BLOB`
- Temporal: `Types.DATE`, `Types.TIMESTAMP`, `Types.TIME` (return objects but some drivers may behave differently)
- Complex: `Types.CLOB`, `Types.SQLXML`, `Types.ARRAY`, `Types.STRUCT`

### Implementation Patterns

**Pattern 1: Primitive Type (wasNull check)**
```java
case Types.INTEGER:
case Types.TINYINT:
case Types.SMALLINT:
    int value = resultSet.getInt(columnIndex);
    if (resultSet.wasNull()) {
        preparedStatement.setNull(parameterIndex, Types.INTEGER);
    } else {
        preparedStatement.setInt(parameterIndex, value);
    }
    break;
```

**Pattern 2: Object Type (belt-and-suspenders - both checks)**
```java
case Types.VARCHAR:
case Types.CHAR:
case Types.LONGVARCHAR:
    String value = resultSet.getString(columnIndex);
    if (resultSet.wasNull() || value == null) {
        preparedStatement.setNull(parameterIndex, Types.VARCHAR);
    } else {
        preparedStatement.setString(parameterIndex, value);
    }
    break;

case Types.BIGINT:
case Types.NUMERIC:
case Types.DECIMAL:
    java.math.BigDecimal value = resultSet.getBigDecimal(columnIndex);
    if (resultSet.wasNull() || value == null) {
        preparedStatement.setNull(parameterIndex, Types.NUMERIC);
    } else {
        preparedStatement.setBigDecimal(parameterIndex, value);
    }
    break;
```

**Pattern 3: Temporal with String Fallback (DB2Manager pattern)**
```java
case Types.DATE:
    try {
        java.sql.Date value = resultSet.getDate(columnIndex);
        if (resultSet.wasNull() || value == null) {
            preparedStatement.setNull(parameterIndex, Types.DATE);
        } else {
            preparedStatement.setDate(parameterIndex, value);
        }
    } catch (SQLException e) {
        // Fallback for SQLite/MongoDB string-based dates
        String dateStr = resultSet.getString(columnIndex);
        if (dateStr == null || resultSet.wasNull()) {
            preparedStatement.setNull(parameterIndex, Types.DATE);
        } else {
            // Parse ISO 8601 or SQL date format
            try {
                java.sql.Date parsedDate;
                if (dateStr.contains("T")) {
                    LocalDateTime ldt = LocalDateTime.parse(dateStr, DateTimeFormatter.ISO_DATE_TIME);
                    parsedDate = java.sql.Date.valueOf(ldt.toLocalDate());
                } else {
                    parsedDate = java.sql.Date.valueOf(dateStr);
                }
                preparedStatement.setDate(parameterIndex, parsedDate);
            } catch (IllegalArgumentException | DateTimeParseException ex) {
                LOG.warn("Unable to parse date '{}', setting NULL", dateStr);
                preparedStatement.setNull(parameterIndex, Types.DATE);
            }
        }
    }
    break;
```

### Manager Implementation Status

| Manager | Status | Notes |
|---------|--------|-------|
| DB2Manager | ✅ Correct | Reference implementation with proper NULL handling |
| PostgresqlManager | ✅ Safe | COPY protocol requires explicit NULL encoding |
| OracleManager | ❌ Broken | Missing wasNull() checks for primitives (fixing in this change) |
| StandardJDBCManager | ❌ Broken | Missing wasNull() checks for primitives (fixing in this change) |
| SqliteManager | ❌ Broken | Missing wasNull() checks for primitives (fixing in this change) |
| MySQLManager | ✅ Safe | LOAD DATA INFILE with explicit wasNull() check at line 120 |
| SQLServerManager | ✅ Safe | BulkCopy adapter uses getObject() which returns null correctly |
| MongoDBManager | ✅ N/A | NoSQL, different type system |
| DenodoManager | ❌ Inherited | Extends StandardJDBCManager, inherits broken behavior (fixed via parent) |

## Test Requirements

### TR-1: NULL Integer Test
**Setup**: Source table with INTEGER column containing NULL  
**Execute**: Replicate to sink database  
**Verify**: `SELECT col FROM sink WHERE col IS NULL` returns the row  
**Anti-pattern**: `SELECT col FROM sink WHERE col = 0` should NOT return the row

### TR-2: NULL Numeric Test
**Setup**: Source with NUMERIC/DECIMAL column containing NULL  
**Execute**: Replicate  
**Verify**: Sink preserves NULL

### TR-3: NULL Temporal Test
**Setup**: Source with DATE and TIMESTAMP columns containing NULL  
**Execute**: Replicate  
**Verify**: Sink preserves NULL (not epoch 1970-01-01)

### TR-4: Manager-Specific Unit Tests
**OracleManagerNullHandlingTest**: Verify Oracle-specific NULL handling for all types
**StandardJDBCManagerNullHandlingTest**: Verify fallback manager with H2/Derby
**SqliteManagerNullHandlingTest**: Verify SQLite-specific NULL handling including TIME string conversion

Each test includes:
- NULL INTEGER preservation (vs 0)
- NULL DOUBLE preservation (vs 0.0)
- NULL BIGDECIMAL preservation
- NULL DATE preservation (vs epoch)
- NULL TIMESTAMP preservation
- NULL VARCHAR preservation
- NULL BINARY preservation
- NULL BOOLEAN preservation

## Known Issues

### Issue #51: Denodo → Oracle NULL INTEGER Corruption
**Symptom**: NULL INTEGER values from Denodo convert to 0 in Oracle  
**Root Cause**: OracleManager missing wasNull() check for Types.INTEGER case  
**Impact**: Silent data corruption - NULL semantic meaning lost  
**Status**: ❌ Unfixed (this change addresses it)

## Edge Cases

### EC-1: Object Getters and wasNull()
**Decision**: Always check both `wasNull()` and object null pointer for all data types
**Rationale**: Belt-and-suspenders approach handles all JDBC driver variations (some drivers may behave differently)
**Pattern**: `if (resultSet.wasNull() || value == null)`

### EC-2: DATE/TIMESTAMP String Fallback
**Decision**: Follow DB2Manager pattern with try-catch and string parsing fallback
**Rationale**: SQLite and MongoDB return dates as strings, not java.sql.Date objects
**Pattern**: Try getDate(), catch SQLException, fall back to getString() + parse

### EC-3: SQLite TIME Type
**Special Case**: SQLite converts TIME to string at line ~135 with explicit null check
**Pattern**: Check if `timeData != null` then `setString(timeData.toString())` else `setNull()`
**Verification**: SqliteManagerNullHandlingTest includes TIME NULL test

## Compliance & Standards

### JDBC 4.3 Specification Compliance
- `ResultSet.getXxx()` methods on primitive types return default values for SQL NULL
- `ResultSet.wasNull()` must be called immediately after getter to detect NULL
- Object getters return `null` for SQL NULL values

### ReplicaDB Conventions
- Follow DB2Manager as reference implementation for NULL handling
- **Always check NULL for all data types** (both primitives and objects)
- Use belt-and-suspenders approach: `wasNull() || value == null`
- Follow DB2Manager DATE/TIMESTAMP pattern with string fallback for SQLite/MongoDB
- Prefer explicit NULL checks over implicit behavior
- Document NULL handling in manager class JavaDoc
- Create manager-specific unit tests for NULL handling verification

## Performance Considerations

**Impact**: One additional method call per column per row (`wasNull()`)  
**Cost**: ~1-2 nanoseconds (CPU register check)  
**Context**: Negligible compared to network I/O (milliseconds) and database writes (microseconds)  
**Conclusion**: Correctness outweighs micro-optimization

## Migration & Backwards Compatibility

**Breaking Change**: No - fixing silent corruption  
**User Impact**: Users previously receiving 0 for NULL will now receive NULL (correct behavior)  
**Risk**: If downstream systems expect 0-for-NULL, they have a data pipeline bug that needs fixing  
**Mitigation**: Document behavior change in release notes

## Future Enhancements

### FE-1: Null-Safe Helper Methods
Extract common pattern to base SqlManager class:
```java
protected void setIntOrNull(PreparedStatement ps, ResultSet rs, int index, int sqlType)
protected void setLongOrNull(PreparedStatement ps, ResultSet rs, int index, int sqlType)
// etc.
```

### FE-2: Automated NULL Testing
Create test generator that:
1. Reads database schema
2. Generates INSERT with NULL for each nullable column
3. Replicates to all supported sinks
4. Verifies NULL preservation

### FE-3: Static Analysis Rule
Create linter rule to detect direct primitive getter calls in PreparedStatement arguments:
```java
// ❌ Should be flagged
ps.setInt(i, rs.getInt(i));

// ✅ Approved pattern
int val = rs.getInt(i);
if (rs.wasNull()) { ... } else { ... }
```
