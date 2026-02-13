# Design: Fix Primitive NULL Handling in JDBC Managers

## Context

Three JDBC managers (OracleManager, StandardJDBCManager, SqliteManager) exhibit inconsistent NULL handling:
- **Complex types** (BLOB, CLOB, BOOLEAN, SQLXML): Correct NULL checks implemented
- **Primitive types** (INTEGER, DOUBLE, DATE, etc.): Missing NULL checks → silent data corruption

This inconsistency suggests different developers/timeframes, where complex types failed visibly (NullPointerException), forcing fixes, while primitive types failed silently (0 instead of NULL), hiding the bug.

## JDBC NULL Handling Specification

**Primitive getters return default values for NULL:**
```java
ResultSet rs = ...;
int value = rs.getInt("col");      // Returns 0 if NULL
boolean isNull = rs.wasNull();     // Returns true if last read was NULL
```

**Object getters return null:**
```java
String value = rs.getString("col"); // Returns null if NULL  
Blob blob = rs.getBlob("col");      // Returns null if NULL
// No wasNull() check needed - null pointer check sufficient
```

## Reference Implementation: DB2Manager

DB2Manager demonstrates correct primitive NULL handling:

```java
case Types.INTEGER:
case Types.TINYINT:
case Types.SMALLINT:
    int intVal = resultSet.getInt(i);      // Store primitive value
    if (resultSet.wasNull()) {              // Check NULL flag
        ps.setNull(i, targetType);          // Preserve NULL
    } else {
        ps.setInt(i, intVal);               // Set actual value
    }
    break;
```

**Why this works:**
1. `getInt()` returns 0 and sets internal NULL flag
2. `wasNull()` checks the flag immediately (before next read)
3. Conditional logic preserves NULL semantic vs setting 0

**Why direct call fails:**
```java
ps.setInt(i, resultSet.getInt(i));  // ❌ Cannot check wasNull() after
```
Once `getInt()` is passed to `setInt()`, there's no way to check `wasNull()` afterward.

## Implementation Strategy

### Phase 1: Core Fixes (Immediate)

**Priority 1: OracleManager**
- Most reported issues involve Oracle as sink (Issue #51)
- Fix all primitive types in `insertDataToTable()` method
- Affected types: INTEGER, BIGINT, NUMERIC, DOUBLE, FLOAT, DATE, TIMESTAMP

**Pattern Application:**
```java
// Template for numeric types
case Types.INTEGER:
case Types.TINYINT:
case Types.SMALLINT:
    int intVal = resultSet.getInt(i);
    if (resultSet.wasNull()) {
        ps.setNull(i, Types.INTEGER);  // Use appropriate SQL type
    } else {
        ps.setInt(i, intVal);
    }
    break;

// Template for temporal types  
case Types.DATE:
    java.sql.Date dateVal = resultSet.getDate(i);
    if (resultSet.wasNull()) {
        ps.setNull(i, Types.DATE);
    } else {
        ps.setDate(i, dateVal);
    }
    break;
```

**Special Cases:**

1. **BIGINT/NUMERIC/DECIMAL**: Use `getBigDecimal()` which returns object
   - Current: `ps.setBigDecimal(i, resultSet.getBigDecimal(i))`
   - **Action**: Add belt-and-suspenders null checks (both `wasNull()` and `bdVal == null`)
   - Rationale: Defensive programming across JDBC driver variations

2. **String types** (VARCHAR, CHAR): Use `getString()` which returns object
   - Current: `ps.setString(i, resultSet.getString(i))`
   - **Action**: Add explicit null check (`strVal == null`)
   - Rationale: Consistent NULL handling pattern across all types

3. **Binary types** (BINARY, VARBINARY): Use `getBytes()` which returns object
   - Current: `ps.setBytes(i, resultSet.getBytes(i))`
   - **Action**: Add explicit null check (`bytesVal == null`)
   - Rationale: Same as string types - consistency

3. **Binary types**: Use `getBytes()` which returns object (byte[])
   - Current: `ps.setBytes(i, resultSet.getBytes(i))`  
   - Should be safe
   - **Action**: Verify in tests

### Phase 2: StandardJDBCManager & SqliteManager

Apply identical fixes to these managers since they share the same broken pattern.

**StandardJDBCManager impact:**
- Serves as fallback for databases without specific manager
- Fixing this helps: Cubrid, SQLite (if not using SqliteManager), and future databases

### Phase 3: Test Data Enhancement

**Add NULL test rows to test fixtures:**

```sql
-- Pattern from db2-source.sql (row 4096)
INSERT INTO t_source (
    C_INTEGER, C_SMALLINT, C_BIGINT, C_NUMERIC, C_DECIMAL,
    C_REAL, C_DOUBLE_PRECISION, C_FLOAT,
    C_BINARY, C_BINARY_VAR, C_BINARY_LOB,
    C_BOOLEAN, C_CHARACTER, C_CHARACTER_VAR, C_CHARACTER_LOB,
    C_NATIONAL_CHARACTER, C_NATIONAL_CHARACTER_VAR,
    C_DATE, C_TIMESTAMP_WITHOUT_TIMEZONE, C_TIMESTAMP_WITH_TIMEZONE
)
VALUES (
    4096,  -- Primary key (NOT NULL)
    null, null, null, null,     -- All numeric types
    null, null, null,            -- All floating types  
    null, null, null,            -- All binary types
    null, null, null, null,      -- All string types
    null, null,                  -- National char types
    null, null, null             -- All temporal types
);
```

**Files to update:**
- `src/test/resources/oracle/oracle-source.sql`
- `src/test/resources/sqlite/sqlite-source.sql` (if exists)
- `src/test/resources/mysql/mysql-source.sql` (verify if has NULL row)

## Type Mapping Reference

**Primitive types requiring fix:**

| JDBC Type | Getter | Returns on NULL | Fix Needed |
|-----------|--------|-----------------|------------|
| INTEGER | getInt() | 0 | ✅ Yes |
| TINYINT | getInt() | 0 | ✅ Yes |
| SMALLINT | getInt() | 0 | ✅ Yes |
| BIGINT | getLong() | 0L | ✅ Yes |
| DOUBLE | getDouble() | 0.0 | ✅ Yes |
| FLOAT/REAL | getFloat() | 0.0f | ✅ Yes |
| DATE | getDate() | null* | 🟡 Maybe** |
| TIMESTAMP | getTimestamp() | null* | 🟡 Maybe** |

\* Date/Timestamp return objects (should be null), but some JDBC drivers may return default Epoch  
** Test with NULL values to confirm behavior

**Object types (should be safe):**

| JDBC Type | Getter | Returns on NULL | Fix Needed |
|-----------|--------|-----------------|------------|
| NUMERIC/DECIMAL | getBigDecimal() | null | ❌ No |
| VARCHAR/CHAR | getString() | null | ❌ No |
| BLOB | getBlob() | null | ❌ No |
| CLOB | getClob() | null | ❌ No |
| BINARY/VARBINARY | getBytes() | null | ❌ No |

## Testing Strategy

**Unit Test Approach** (if adding):
```java
@Test
void testNullIntegerHandling() {
    // Setup source with NULL INTEGER
    // Run replication
    // Verify sink has NULL, not 0
    assertNull(sinkRs.getObject("col"));  // getObject returns null for NULL
    sinkRs.getInt("col");
    assertTrue(sinkRs.wasNull());         // Confirms NULL, not 0
}
```

**Integration Test Verification:**
- Existing tests with new NULL row should pass
- If tests fail with "0 != NULL" assertions, that confirms the bug exists
- After fix, tests should show NULL preserved correctly

## Rollout Plan

1. **Fix OracleManager** + add Oracle NULL test row → Run Oracle integration tests
2. **Fix StandardJDBCManager** → Run generic JDBC tests  
3. **Fix SqliteManager** + add SQLite NULL test row → Run SQLite tests
4. **Verify cross-database**: Run sample transfers (MySQL→Oracle, Postgres→Oracle, etc.)

## Edge Cases & Considerations

**Should we add wasNull() to string/binary types?**
- Pro: Defensive programming, consistent pattern
- Con: Extra overhead, likely unnecessary (null pointer check sufficient)
- **Decision**: Verify in tests first, add only if NULL corruption observed

**Performance impact?**
- One extra method call per column per row (`wasNull()`)
- Negligible compared to network I/O and disk writes
- **Conclusion**: Correctness > micro-optimization

**Backward compatibility?**
- **No breaking changes**: Fixing silent corruption, not changing APIs
- Users currently getting 0 will now get NULL (correct behavior)
- If users depend on 0-for-NULL, they have a bug in their pipeline

## Rejected Alternatives

### Alternative 1: Helper Methods in SqlManager Base Class
```java
protected void setIntOrNull(PreparedStatement ps, int paramIndex, 
                           ResultSet rs, int columnIndex)
```
**Rejected because:**
- Requires refactoring all managers immediately (large scope)
- Can be added later as optimization
- Direct inline pattern is clearer for now

### Alternative 2: Fix via Wrapper ResultSet
Create a NullSafeResultSet wrapper that throws on primitive getters for NULL values.

**Rejected because:**
- Breaks at runtime instead of fixing the issue
- Requires changes at connection level, not manager level
- 
### Alternative 3: Fix Only Reported Issue (Integer in Oracle)

**Rejected because:**
- Leaves time bomb for other types/managers
- Same bug pattern exists across codebase
- Systematic fix is cleaner with similar effort

## Success Criteria

✅ Issue #51 fixed: Denodo→Oracle NULL INTEGERs remain NULL  
✅ All existing integration tests pass with NULL test rows added  
✅ No performance regression (measured via existing benchmarks)  
✅ Pattern consistent with DB2Manager reference implementation  
✅ Future managers inherit correct pattern via documentation/example
