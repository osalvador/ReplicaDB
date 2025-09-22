---
applyTo: '**'
---

# ReplicaDB: Technical Implementation Patterns

## Architectural Decisions and Rationale

### Manager Factory Pattern Implementation
**Why chosen**: Supports 15+ heterogeneous data sources with unified interface
**Key benefits**: Runtime database selection, clean separation of concerns, extensible design

```java
// Pattern: Database managers selected by JDBC URL scheme analysis
public ConnManager accept(ToolOptions options, DataSourceType dsType) {
    String scheme = extractScheme(options, dsType);
    if (POSTGRES.isTheManagerTypeOf(options, dsType)) {
        return new PostgresqlManager(options, dsType);
    } else if (ORACLE.isTheManagerTypeOf(options, dsType)) {
        return new OracleManager(options, dsType);
    }
    // Fallback to StandardJDBCManager for unknown databases
    return new StandardJDBCManager(options, dsType);
}
```

### Abstract SQL Manager Hierarchy
**Why chosen**: JDBC databases share common patterns but need database-specific optimizations
**Implementation**: Three-layer hierarchy (ConnManager → SqlManager → Database-specific)

```java
// Base abstraction for all data sources
public abstract class ConnManager {
    public abstract ResultSet readTable(String tableName, String[] columns, int nThread);
    public abstract int insertDataToTable(ResultSet resultSet, int taskId);
    public abstract Connection getConnection();
}

// SQL-specific base class with common JDBC operations
public abstract class SqlManager extends ConnManager {
    protected Connection makeSourceConnection() throws SQLException { /* common logic */ }
    protected ResultSet execute(String stmt, Integer fetchSize, Object... args) { /* shared implementation */ }
}

// Database-specific implementations
public class PostgresqlManager extends SqlManager {
    // PostgreSQL-specific optimizations and SQL dialect handling
}
```

## Module/Package Structure

### Core Package Organization
```
org.replicadb/
├── cli/                    # Command-line interface and option parsing
├── config/                 # Configuration management and validation
├── manager/                # Database connection managers
│   ├── file/              # File system managers (CSV, ORC)
│   └── [database]/        # Database-specific managers
├── rowset/                # Custom ResultSet implementations
└── time/                  # Timestamp and scheduling utilities
```

**Rationale**: Package boundaries follow functional responsibilities, making database-specific code easy to locate and maintain.

### Manager Registration Pattern
New database support requires:
1. **Extend SqlManager** with database-specific class
2. **Add scheme detection** in SupportedManagers enum
3. **Register in ManagerFactory.accept()** method
4. **Implement required abstract methods** (getDriverClass, readTable, insertDataToTable)

## Essential Java Patterns and Conventions

### Connection Management Pattern
```java
// Singleton connection pattern with lazy initialization
@Override
public Connection getConnection() throws SQLException {
    if (this.connection == null) {
        if (dsType.equals(DataSourceType.SOURCE)) {
            this.connection = makeSourceConnection();
        } else {
            this.connection = makeSinkConnection();
        }
    }
    return this.connection;
}
```

### Resource Management Pattern
```java
// Always use try-with-resources or explicit release
public void release() {
    if (null != this.lastStatement) {
        try {
            this.lastStatement.close();
        } catch (SQLException e) {
            LOG.error("Exception closing executed Statement: " + e, e);
        }
        this.lastStatement = null;
    }
}
```

### Error Handling Strategy
```java
// Database-specific error handling with context
try {
    // Database operation
} catch (SQLException e) {
    LOG.error("Database operation failed for table: " + tableName, e);
    throw new RuntimeException("Replication failed: " + e.getMessage(), e);
}
```

## Framework Integration Patterns

### Maven Dependency Management
```xml
<!-- Pattern: Version properties for consistency -->
<properties>
    <version.debezium>1.5.2.Final</version.debezium>
    <version.testContainers>1.18.3</version.testContainers>
</properties>

<!-- Database drivers bundled for convenience -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.2</version>
</dependency>
```

### Log4j2 Integration Pattern
```java
// Consistent logging across all classes
private static final Logger LOG = LogManager.getLogger(ClassName.class.getName());

// Thread-aware logging for parallel processing
LOG.info("{}: Executing SQL statement: {}", Thread.currentThread().getName(), stmt);
```

## Testing Philosophy and Strategies

### TestContainers Integration
**Why**: Enables real database testing without external dependencies
**Pattern**: Container-based integration tests for cross-database scenarios

```java
@Testcontainers
class Postgres2MySQLTest {
    @Rule
    public static MySQLContainer<ReplicadbMysqlContainer> mysql = ReplicadbMysqlContainer.getInstance();
    
    @Rule
    public static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres = ReplicadbPostgresqlContainer.getInstance();
    
    @Test
    void testCompleteReplication() throws SQLException, ParseException, IOException {
        // Test actual replication between live database containers
        ToolOptions options = new ToolOptions(args);
        int processedRows = ReplicaDB.processReplica(options);
        assertEquals(TOTAL_SINK_ROWS, processedRows);
    }
}
```

### Test Naming Conventions
- **Class pattern**: `{Source}2{Sink}Test` (e.g., `Postgres2MySQLTest`)
- **Method pattern**: `test{Mode}Replication` (e.g., `testIncrementalReplication`)
- **Integration tests**: Focus on cross-database compatibility
- **Unit tests**: Minimal - most value in integration testing

### Test Data Strategy
```
src/test/resources/
├── {database}/
│   └── {database}-source.sql    # Source table setup
├── sinks/
│   └── {database}-sink.sql      # Target table DDL
└── csv/
    └── source.csv               # File format test data
```

## Configuration Management Patterns

### Property File Structure
```properties
# Hierarchical configuration with clear separators
######################## ReplicadB General Options ########################
mode=complete
jobs=4
fetch.size=5000

############################# Source Options ##############################
source.connect=jdbc:postgresql://localhost:5432/source
source.user=postgres
source.password=${POSTGRES_PASSWORD}
source.table=employees

############################# Sink Options ################################
sink.connect=jdbc:mysql://localhost:3306/target
sink.user=mysql
sink.password=${MYSQL_PASSWORD}
sink.table=employees
```

### Environment Variable Integration
```java
// Pattern: Support environment variable substitution
private String resolveProperty(String value) {
    if (value != null && value.startsWith("${") && value.endsWith("}")) {
        String envVar = value.substring(2, value.length() - 1);
        return System.getenv(envVar);
    }
    return value;
}
```

## Performance Requirements and Implementation

### Memory Management
- **Streaming ResultSets**: Use `ResultSet.TYPE_FORWARD_ONLY` and configurable fetch sizes
- **Connection pooling**: Single connection per thread to avoid overhead
- **Large object handling**: Stream BLOBs/CLOBs without loading into memory

### Throughput Optimization
```java
// Configurable fetch size for optimal performance
statement.setFetchSize(options.getFetchSize()); // Default: 5000

// Bandwidth throttling implementation
if (bandwidthRateLimiter != null) {
    bandwidthRateLimiter.acquire(rowSize);
}
```

### Parallel Processing Pattern
```java
// ExecutorService for parallel table processing
ExecutorService replicaTasksService = Executors.newFixedThreadPool(options.getJobs());
for (int i = 0; i < options.getJobs(); i++) {
    Future<Integer> replicaTaskFuture = replicaTasksService.submit(new ReplicaTask(i));
    replicaTasksFutures.add(replicaTaskFuture);
}
```

## Database-Specific Implementation Patterns

### Type Mapping Strategy
```java
// Handle database-specific type conversions
switch (resultSet.getMetaData().getColumnType(i)) {
    case Types.CLOB:
        // Stream CLOB data for Oracle-to-Oracle replication
        Reader reader = resultSet.getClob(i).getCharacterStream();
        preparedStatement.setClob(i, reader);
        break;
    case Types.BLOB:
        // Handle binary data appropriately
        preparedStatement.setBytes(i, resultSet.getBytes(i));
        break;
}
```

### SQL Dialect Handling
```java
// Database-specific SQL generation
@Override
public String getInsertSQLCommand(String tableName, String allColumns, int columnsNumber) {
    // PostgreSQL-specific INSERT with ON CONFLICT handling
    return "INSERT INTO " + tableName + " (" + allColumns + ") VALUES (" + placeholders + ") ON CONFLICT DO NOTHING";
}
```

## Integration Patterns

### File System Integration
```java
// Abstraction for file-based sources/sinks
public class LocalFileManager extends ConnManager {
    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) {
        // CSV/ORC file reading implementation
    }
}
```

## Code Generation and Utilities

### When to Use Code Generation
- **Avoid**: Don't use code generation for database managers
- **Prefer**: Hand-written implementations for better error handling and debugging
- **Exception**: Build-time generation acceptable for version information

### Utility Classes Pattern
```java
// Static utility methods for common operations
public class DatabaseUtils {
    public static String[] getPrimaryKeys(Connection conn, String tableName) {
        // Common primary key detection logic
    }
    
    public static void validateConnection(Connection conn) throws SQLException {
        // Standard connection validation
    }
}
```

## Development Workflow Guidelines

### Adding New Database Support
1. **Create manager class** extending SqlManager
2. **Implement abstract methods** with database-specific logic
3. **Add scheme detection** in SupportedManagers
4. **Register in ManagerFactory**
5. **Create integration tests** with TestContainers
6. **Update documentation** with connection examples

### Performance Optimization Process
1. **Profile with realistic data volumes** (not toy datasets)
2. **Measure memory usage** under different fetch sizes
3. **Test parallel processing** with actual database load
4. **Validate bandwidth throttling** in network-constrained environments

### Error Handling Best Practices
- **Preserve original exceptions** in stack traces
- **Log context information** (table names, row counts, connection details)
- **Fail fast** on configuration errors
- **Retry transient errors** with exponential backoff

## Anti-Patterns to Avoid

### Database Connection Anti-Patterns
- **DON'T**: Create new connections per query
- **DO**: Use singleton connection pattern with proper cleanup

### Memory Anti-Patterns
- **DON'T**: Load entire ResultSet into memory
- **DO**: Use streaming with appropriate fetch sizes

### Error Handling Anti-Patterns
- **DON'T**: Catch and ignore SQLExceptions
- **DO**: Log errors with context and propagate appropriately

### Testing Anti-Patterns
- **DON'T**: Use H2 or embedded databases for integration tests
- **DO**: Test against actual database engines using TestContainers
