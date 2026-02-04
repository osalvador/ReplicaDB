# Binary COPY Schema Flexibility Implementation Plan

## Problem Statement

Binary COPY format is currently **rigid** and requires exact schema matching between source ResultSet and PostgreSQL sink table. This causes failures when:

1. **Column count mismatch**: Source has 21 columns, sink has 33 columns
2. **Column order mismatch**: Source columns in different order than sink
3. **Column name mismatch**: Source uses `c_data`, sink expects `data_column`
4. **Missing columns in source**: Sink has extra columns (C_ARRAY, C_JSON, C_XML) not in source
5. **Extra columns in source**: Source has columns that don't exist in sink

## Current Behavior (Causes Failures)

```
Source (MySQL):    c_integer, c_smallint, c_bigint, ..., c_timestamp (21 cols)
Sink (PostgreSQL): c_integer, c_smallint, c_bigint, ..., c_array, c_json, c_xml (33 cols)

Binary COPY writes: 21 fields
PostgreSQL expects: 33 fields → ERROR: incorrect binary data format
```

## Solution Architecture

### Strategy 1: Query Sink Table Metadata Before COPY (RECOMMENDED)

**Approach**: Query PostgreSQL system catalogs to get actual sink table schema, then adapt binary data stream to match.

**Implementation Steps**:

1. **Query sink table columns** from `information_schema.columns` or `pg_catalog.pg_attribute`
   ```sql
   SELECT column_name, ordinal_position, data_type, udt_name
   FROM information_schema.columns 
   WHERE table_name = 't_sink' 
   ORDER BY ordinal_position;
   ```

2. **Build column mapping**:
   - Source column index → Sink column position
   - Handle missing columns with NULL values
   - Skip source columns not in sink
   - Validate type compatibility

3. **Write binary data in sink column order**:
   ```
   For each row:
     For each sink column (ordered):
       if source has matching column:
         write binary data for that column
       else:
         write NULL marker (-1)
   ```

4. **Handle DEFAULT values**:
   - Exclude columns with DEFAULT from COPY column list
   - Let PostgreSQL apply defaults automatically

**Pseudocode**:
```java
// 1. Query sink table schema
Map<String, SinkColumnInfo> sinkColumns = querySinkTableSchema(tableName);

// 2. Build mapping: sink column → source column index
Map<Integer, Integer> sinkToSourceMapping = buildColumnMapping(
    sinkColumns, 
    resultSetMetadata
);

// 3. Detect columns with DEFAULT constraints
Set<String> columnsWithDefaults = detectDefaultColumns(tableName);

// 4. Build COPY column list (exclude DEFAULT columns)
String copyColumns = buildCopyColumnList(sinkColumns, columnsWithDefaults);
String copyCmd = "COPY " + tableName + " (" + copyColumns + ") FROM STDIN (FORMAT BINARY)";

// 5. Write binary data in sink column order
for each row:
    dos.writeShort(sinkColumns.size() - columnsWithDefaults.size());
    
    for (String sinkColumn : sinkColumns.keySet() - columnsWithDefaults) {
        Integer sourceIdx = sinkToSourceMapping.get(sinkColumn);
        
        if (sourceIdx != null) {
            // Write data from source column
            writeColumnBinaryData(resultSet, sourceIdx, sinkColumns.get(sinkColumn).type);
        } else {
            // Column not in source → write NULL
            dos.writeInt(-1);
        }
    }
```

### Strategy 2: Dynamic Column List in COPY Command (SIMPLER)

**Approach**: Only COPY columns that exist in both source and sink, in source order.

**Implementation**:
```java
// 1. Get columns from source ResultSet
Set<String> sourceColumns = getSourceColumns(rsmd);

// 2. Query sink table columns
Set<String> sinkColumns = querySinkTableColumns(tableName);

// 3. Find intersection (columns in both)
Set<String> commonColumns = sourceColumns.intersect(sinkColumns);

// 4. Build COPY command with only common columns
String copyCmd = "COPY " + tableName + " (" + String.join(",", commonColumns) + ") FROM STDIN (FORMAT BINARY)";

// 5. Write binary data ONLY for common columns
for each row:
    dos.writeShort(commonColumns.size());
    
    for (String column : commonColumns) {
        int sourceIdx = getSourceColumnIndex(column);
        writeColumnBinaryData(resultSet, sourceIdx);
    }
```

**Pros**:
- ✅ Simple to implement
- ✅ Preserves source column order
- ✅ Handles missing sink columns gracefully

**Cons**:
- ❌ Doesn't populate sink-only columns (will use DEFAULT or NULL)
- ❌ Requires sink table columns to be nullable or have defaults

### Strategy 3: Hybrid Approach (BEST SOLUTION)

Combine both strategies for maximum flexibility:

1. **Query sink table schema** to understand what PostgreSQL expects
2. **Match by column name** (case-insensitive) between source and sink
3. **Write data in sink column order** for columns that exist in source
4. **Exclude columns with DEFAULT** from COPY, let PostgreSQL handle them
5. **Write NULL for sink columns** not in source (if column is nullable)
6. **Skip source columns** not in sink

**Benefits**:
- ✅ Handles column order differences
- ✅ Handles column count differences  
- ✅ Respects sink table constraints (NOT NULL, DEFAULT)
- ✅ Works across all database types → PostgreSQL
- ✅ Future-proof for PostgreSQL-only workflow

## Implementation Plan

### Phase 1: Query Sink Table Metadata (Week 1)

**Task 1.1**: Create method to query sink table schema
```java
private List<SinkColumnInfo> querySinkTableSchema(String tableName) throws SQLException {
    // Query information_schema.columns
    // Return ordered list of columns with:
    //   - column_name
    //   - ordinal_position
    //   - data_type (PostgreSQL type name)
    //   - is_nullable
    //   - column_default (to detect DEFAULT constraints)
}
```

**Task 1.2**: Create method to detect DEFAULT columns
```java
private Set<String> getColumnsWithDefaults(List<SinkColumnInfo> sinkColumns) {
    return sinkColumns.stream()
        .filter(col -> col.hasDefault())
        .map(SinkColumnInfo::getName)
        .collect(Collectors.toSet());
}
```

**Task 1.3**: Create method to build column mapping
```java
private Map<String, Integer> buildSourceToSinkMapping(
    ResultSetMetaData sourceMetadata,
    List<SinkColumnInfo> sinkColumns
) throws SQLException {
    Map<String, Integer> mapping = new HashMap<>();
    
    for (int i = 1; i <= sourceMetadata.getColumnCount(); i++) {
        String sourceName = sourceMetadata.getColumnName(i).toLowerCase();
        
        // Find matching sink column (case-insensitive)
        for (SinkColumnInfo sinkCol : sinkColumns) {
            if (sinkCol.getName().equalsIgnoreCase(sourceName)) {
                mapping.put(sinkCol.getName(), i);
                break;
            }
        }
    }
    
    return mapping;
}
```

### Phase 2: Adapt Binary COPY Logic (Week 2)

**Task 2.1**: Modify `insertDataViaBinaryCopy()` to use sink schema
```java
private int insertDataViaBinaryCopy(...) throws SQLException {
    // 1. Query sink table schema
    List<SinkColumnInfo> sinkColumns = querySinkTableSchema(tableName);
    
    // 2. Build source→sink mapping
    Map<String, Integer> sourceMapping = buildSourceToSinkMapping(rsmd, sinkColumns);
    
    // 3. Exclude DEFAULT columns
    Set<String> defaultColumns = getColumnsWithDefaults(sinkColumns);
    List<SinkColumnInfo> copyColumns = sinkColumns.stream()
        .filter(col -> !defaultColumns.contains(col.getName()))
        .collect(Collectors.toList());
    
    // 4. Build COPY command with explicit column list
    String columnList = copyColumns.stream()
        .map(SinkColumnInfo::getName)
        .collect(Collectors.joining(","));
    String copyCmd = "COPY " + tableName + " (" + columnList + ") FROM STDIN (FORMAT BINARY)";
    
    // 5. Write binary data in sink column order
    while (resultSet.next()) {
        dos.writeShort(copyColumns.size());
        
        for (SinkColumnInfo sinkCol : copyColumns) {
            Integer sourceIdx = sourceMapping.get(sinkCol.getName());
            
            if (sourceIdx != null) {
                // Write data from source
                writeBinaryColumn(resultSet, sourceIdx, sinkCol.getType(), dos);
            } else if (sinkCol.isNullable()) {
                // Sink column not in source → write NULL
                dos.writeInt(-1);
            } else {
                throw new SQLException("Cannot map non-nullable column: " + sinkCol.getName());
            }
        }
    }
}
```

**Task 2.2**: Extract column writing logic into separate method
```java
private void writeBinaryColumn(
    ResultSet rs, 
    int columnIndex, 
    int jdbcType,
    DataOutputStream dos
) throws SQLException, IOException {
    // Use existing type-specific encoding logic
    switch (jdbcType) {
        case Types.SMALLINT: ...
        case Types.INTEGER: ...
        case Types.BIGINT: ...
        // etc.
    }
}
```

### Phase 3: Type Mapping Validation (Week 3)

**Task 3.1**: Add type compatibility checking
```java
private void validateTypeCompatibility(
    int sourceType, 
    String sinkTypeName
) throws SQLException {
    // Check if source JDBC type can be safely encoded for sink PostgreSQL type
    // Example: MySQL TINYINT → PostgreSQL SMALLINT (OK)
    //          MySQL BIGINT → PostgreSQL INTEGER (WARNING: potential overflow)
}
```

**Task 3.2**: Add logging for column mapping
```java
LOG.info("Column mapping for table {}:", tableName);
for (SinkColumnInfo sinkCol : copyColumns) {
    Integer sourceIdx = sourceMapping.get(sinkCol.getName());
    if (sourceIdx != null) {
        LOG.debug("  {} → {} (source col {})", 
                 sinkCol.getName(), 
                 rsmd.getColumnName(sourceIdx), 
                 sourceIdx);
    } else {
        LOG.debug("  {} → NULL (not in source)", sinkCol.getName());
    }
}
```

### Phase 4: Testing and Validation (Week 4)

**Task 4.1**: Add unit tests for column mapping logic
```java
@Test
void testColumnMapping_ExtraSourceColumns() {
    // Source: A, B, C, D
    // Sink:   A, B, C
    // Expected: Map A→1, B→2, C→3, skip D
}

@Test
void testColumnMapping_ExtraSinkColumns() {
    // Source: A, B, C
    // Sink:   A, B, C, D (nullable), E (with DEFAULT)
    // Expected: Map A→1, B→2, C→3, D→NULL, skip E
}

@Test
void testColumnMapping_DifferentOrder() {
    // Source: A, B, C
    // Sink:   C, A, B
    // Expected: Write in sink order (C, A, B)
}
```

**Task 4.2**: Run integration tests with schema mismatches
- MySQL (21 cols) → PostgreSQL (33 cols)
- MariaDB (21 cols) → PostgreSQL (33 cols)
- Oracle (20 cols) → PostgreSQL (33 cols)

**Task 4.3**: Benchmark performance impact
- Measure overhead of schema query (should be < 10ms)
- Compare throughput: old vs new implementation
- Target: < 5% performance regression

## Detailed Technical Specifications

### SinkColumnInfo Data Structure
```java
class SinkColumnInfo {
    private String name;
    private int ordinalPosition;
    private String dataType;          // PostgreSQL type: int4, varchar, bytea
    private int jdbcType;             // Types.INTEGER, Types.VARCHAR, etc.
    private boolean nullable;
    private String defaultValue;      // "nextval(...)", "CURRENT_TIMESTAMP", etc.
    
    public boolean hasDefault() {
        return defaultValue != null && !defaultValue.isEmpty();
    }
}
```

### Query for Sink Schema
```sql
SELECT 
    column_name,
    ordinal_position,
    data_type,
    udt_name,                     -- PostgreSQL internal type
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = ? 
  AND table_name = ?
ORDER BY ordinal_position;
```

### Alternative Query (Faster, uses pg_catalog)
```sql
SELECT 
    a.attname AS column_name,
    a.attnum AS ordinal_position,
    t.typname AS data_type,
    a.attnotnull AS not_null,
    pg_get_expr(d.adbin, d.adrelid) AS column_default
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
WHERE a.attrelid = ?::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
```

## Error Handling Strategy

### Error Scenarios and Responses

| Scenario | Response |
|----------|----------|
| Sink column NOT NULL, not in source | **FAIL**: Throw SQLException with clear message |
| Sink column nullable, not in source | **OK**: Write NULL marker |
| Sink column has DEFAULT, not in source | **OK**: Exclude from COPY, PostgreSQL applies DEFAULT |
| Source column not in sink | **OK**: Skip column (log warning) |
| Type incompatibility | **WARN**: Log warning, attempt conversion with try-catch fallback |

### Example Error Messages
```
ERROR: Cannot replicate to non-nullable column 'c_mandatory' which is not present in source
HINT: Add column to source query, or make sink column nullable/add DEFAULT

WARNING: Skipping source column 'old_column' - not found in sink table 't_sink'

WARNING: Type mismatch for column 'c_amount': source=DECIMAL(10,2), sink=INTEGER
         May cause precision loss or overflow
```

## Performance Considerations

### Optimization 1: Cache Sink Schema Per Table
```java
private Map<String, List<SinkColumnInfo>> sinkSchemaCache = new HashMap<>();

private List<SinkColumnInfo> querySinkTableSchema(String tableName) {
    return sinkSchemaCache.computeIfAbsent(tableName, 
        key -> querySinkSchemaFromDatabase(key));
}
```

### Optimization 2: Lazy Schema Query
Only query sink schema when:
- Binary COPY is triggered (hasBinaryColumns = true)
- First batch of data is ready to write
- Cache miss for this table

### Optimization 3: Reuse Column Mapping Across Batches
```java
// Query once per table
List<SinkColumnInfo> sinkSchema = querySinkTableSchema(tableName);
Map<String, Integer> mapping = buildSourceToSinkMapping(rsmd, sinkSchema);

// Reuse for all rows
while (resultSet.next()) {
    writeBinaryRowWithMapping(resultSet, mapping, sinkSchema, dos);
}
```

## Backward Compatibility

### Ensure Existing Behavior Preserved
- ✅ PostgreSQL → PostgreSQL with identical schemas: **No change**
- ✅ Text COPY fallback for failures: **Still available**
- ✅ Error messages for genuine issues: **Improved clarity**

### Configuration Option (Future)
```properties
# Optional: Strict mode (fail on schema mismatch)
sink.binary-copy-strict-mode=false

# Optional: Disable schema flexibility (use old behavior)
sink.binary-copy-schema-flexible=true  # default
```

## Success Criteria

### Must Have
- ✅ MySQL → PostgreSQL integration tests pass (all 6 tests)
- ✅ MariaDB → PostgreSQL integration tests pass (all 7 tests)
- ✅ Oracle → PostgreSQL integration tests pass (all 6 tests)
- ✅ Mongo → PostgreSQL integration tests pass (all 9 tests)
- ✅ PostgreSQL → PostgreSQL tests still pass (all 7 tests)
- ✅ Performance regression < 5%
- ✅ Clear error messages for unsupported scenarios

### Nice to Have
- ✅ Automatic type coercion (e.g., MySQL TINYINT → PostgreSQL SMALLINT)
- ✅ Column name normalization (case-insensitive matching with underscore/camelCase support)
- ✅ Detailed logging of column mappings for troubleshooting
- ✅ Benchmark showing performance improvement over text COPY

## Migration Path

### For Users
No action required. Binary COPY will automatically adapt to schema differences.

### For Developers
1. Review new error messages for clarity
2. Add custom type mappings if needed (via extensible mapping registry)
3. Monitor logs for schema mismatch warnings

## Timeline

- **Week 1**: Query sink schema + column mapping logic
- **Week 2**: Integrate into binary COPY writer
- **Week 3**: Type validation + comprehensive logging
- **Week 4**: Testing + performance validation
- **Week 5**: Documentation + release

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Performance regression | Medium | High | Benchmark early, cache aggressively |
| Schema query failures | Low | High | Fallback to text COPY on error |
| Type conversion errors | Medium | Medium | Try-catch with text fallback per column |
| Breaking existing workflows | Low | High | Extensive integration testing |

## Next Steps

1. **Review this plan** with team/stakeholders
2. **Create JIRA tickets** for each task
3. **Start with Phase 1** (sink schema querying)
4. **Implement incrementally** with tests at each phase
5. **Get feedback early** from integration test results
