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
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
    <version.testContainers>1.21.3</version.testContainers>
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
// Consistent logging pattern used across ALL manager classes
private static final Logger LOG = LogManager.getLogger(ClassName.class.getName());

// Thread-aware logging for parallel processing
LOG.info("{}: Executing SQL statement: {}", Thread.currentThread().getName(), stmt);

// Use structured logging with placeholders (NOT string concatenation)
LOG.info("Total process time: {}ms", elapsed);
LOG.trace("Trying with scheme: {}", scheme);
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

### Build and Test Commands

**Standard Development Build**
```bash
# Build with integration tests (excludes problematic Oracle LOB tests)
mvn -B package --file pom.xml -Dtest='!Oracle2OracleCrossVersionLobTest'

# Build without tests (faster development cycle)
mvn -DskipTests -B package --file pom.xml

# Clean build with dependency resolution
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -B -V
```

**Release Build**
```bash
# Full release with all dependencies
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -B -V -P release

# Release without Oracle JDBC driver (licensing compliance)
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -B -V -P release-no-oracle
```

**Running Tests**
```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=MySQL2PostgresTest

# Run tests with TestContainers (requires Docker)
mvn test -Dtest='*2PostgresTest'
```

### Local Development Setup

1. **Prerequisites**
   - Java 11 JDK (LTS recommended)
   - Maven 3.6+
   - Docker (for TestContainers integration tests)
   - Git

2. **Initial Setup**
   ```bash
   git clone https://github.com/osalvador/ReplicaDB.git
   cd ReplicaDB
   mvn clean package -DskipTests
   ```

3. **Running ReplicaDB Locally**
   ```bash
   # After building, use the CLI wrapper scripts
   ./bin/replicadb --options-file conf/replicadb.conf
   
   # Or run directly with Java
   java -jar target/ReplicaDB-0.16.0.jar --source-connect jdbc:postgresql://...
   ```

### Release Process Workflow

ReplicaDB uses an **automated release script** that handles version management:

```bash
# Create and publish a new release
./release.sh 0.16.0
```

**What happens automatically:**
1. Validates semantic versioning format (X.Y.Z)
2. Updates `pom.xml` with new version
3. Updates `README.md` installation instructions
4. Creates git commit: `Release v0.16.0`
5. Creates git tag: `v0.16.0`
6. Pushes to origin/master
7. GitHub Actions CI/CD triggered on tag push
8. Builds release artifacts (JAR, tar.gz, zip)
9. Publishes Docker images
10. Creates GitHub release with assets

**Manual steps** (if needed):
```bash
# Check current version
mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec

# View release history
git tag -l "v*"

# Build specific release profile
mvn clean install -P release
```

### Continuous Integration Workflows

**CT_Push.yml** (Continuous Testing on Push)
- Triggers on: Push to master, Pull requests
- Java: 11 with Adopt distribution
- Tests: All except Oracle LOB cross-version tests
- Builds: Standard release artifacts

**CI_Release.yml** (Release Pipeline)
- Triggers on: Git tags matching `v*`, Manual workflow_dispatch
- Java: 11 with Adopt distribution
- Profiles: `release` (full) and `release-no-oracle`
- Outputs: JAR, tar.gz, zip archives, Docker images
- Deployment: Automatic GitHub release creation

### Debug and Development Tips

**TestContainers Debugging**
```java
// Enable TestContainers logging
@Testcontainers
class MyTest {
    static {
        // Shows container startup logs
        System.setProperty("testcontainers.reuse.enable", "true");
    }
}
```

**Logging Configuration**
- Development: Edit `src/main/resources/log4j2.xml`
- Testing: Edit `src/test/resources/log4j2-test.xml`
- Runtime: Use `--verbose` CLI flag (TRACE, DEBUG, INFO, WARN, ERROR)

```bash
# Run with debug logging
java -jar ReplicaDB.jar --verbose TRACE --options-file config.conf
```

**Maven Profiles**
```xml
<!-- pom.xml profiles available -->
<profiles>
    <profile>
        <id>release</id>
        <!-- Full release with all JDBC drivers including Oracle -->
    </profile>
    <profile>
        <id>release-no-oracle</id>
        <!-- Release excluding Oracle JDBC (licensing) -->
    </profile>
</profiles>
```

### IDE Configuration

**IntelliJ IDEA**
1. Import as Maven project
2. Set SDK to Java 11
3. Enable annotation processing (if using Lombok in future)
4. Configure TestContainers plugin for better integration test experience

**VS Code**
1. Install Java Extension Pack
2. Configure `java.configuration.runtimes` for Java 11
3. Use integrated terminal for Maven commands
4. TestContainers work out-of-the-box with Docker Desktop

### Development Workflow Guidelines

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
