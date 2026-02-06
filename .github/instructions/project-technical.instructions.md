---
applyTo: '**'
---

# Technical Implementation Patterns

## Architectural Decisions and Rationale

**Manager Pattern (Database Abstraction Layer)**
- **Why**: Supporting 15+ databases with different JDBC dialects, type systems, and performance optimizations requires isolated, database-specific logic
- **Structure**: `ConnManager` (base) → `SqlManager` (SQL databases) → `OracleManager`, `PostgresqlManager`, etc.
- **Registration**: `ManagerFactory` uses JDBC URL pattern matching to instantiate correct manager

**Stateless Execution Model**
- **Why**: CLI tool invoked by external schedulers should not maintain state between runs
- **State Tracking**: Incremental mode uses database columns (timestamp/sequence), not local files
- **Connection Lifecycle**: Acquire on job start, release on completion (no connection pooling in Phase 1)

**Thread-Based Parallelism**
- **Why**: Avoids external framework dependencies (Hadoop, Spark), deployable anywhere Java 11+ runs
- **Model**: Main thread spawns N worker threads (ExecutorService), each with dedicated JDBC connection
- **Partition Strategy**: Database-native hash functions (`ORA_HASH`, `HASH`) ensure even distribution

## Package Structure and Boundaries

```
org.replicadb/
├── cli/                    # Command-line argument parsing (ToolOptions, ReplicationMode)
├── manager/                # Database-specific adapters (Manager pattern)
│   ├── ConnManager.java    # Abstract base for all data sources
│   ├── SqlManager.java     # Base for SQL databases (JDBC-based)
│   ├── OracleManager.java  # Oracle-specific: hints, LOB handling, SDO_GEOMETRY
│   ├── PostgresqlManager.java  # PostgreSQL-specific: COPY protocol, array types
│   ├── file/               # File-based sources (CSV, ORC, Parquet)
│   └── db2/                # DB2-specific (AS/400 support, EBCDIC handling)
├── rowset/                 # ResultSet manipulation utilities
├── config/                 # Sentry error tracking, logging configuration
└── ReplicaDB.java          # Main entry point, job orchestration
```

**What Belongs Where**:
- **Database-specific logic**: Always in `XYZManager` subclass, never in `SqlManager` or `ReplicaDB`
- **Generic SQL logic**: `SqlManager` (e.g., metadata queries, column escaping)
- **Cross-database patterns**: Helper classes in `rowset` package
- **CLI parsing**: `cli/ToolOptions` with Apache Commons CLI

## Base Patterns and Abstractions

### Manager Interface Contract

```java
public abstract class ConnManager {
    // Read partitioned data (returns ResultSet for streaming)
    public abstract ResultSet readTable(String tableName, String[] columns, int nThread) throws Exception;
    
    // Insert data from ResultSet (batch insert logic)
    public abstract int insertDataToTable(ResultSet resultSet, int taskId) throws Exception;
    
    // JDBC driver class name
    public abstract String getDriverClass();
    
    // Column name escaping for SQL generation
    public String escapeColName(String colName) {
        return colName;  // Override for backticks, quotes, etc.
    }
}
```

**Key Extension Points**:
- **readTable()**: Override to add database-specific hints, partition logic, or query optimizations
- **insertDataToTable()**: Override for bulk APIs (PostgreSQL COPY, SQL Server Bulk Insert) over standard JDBC
- **escapeColName()**: Override for identifier quoting rules (MySQL backticks, PostgreSQL double quotes, SQL Server brackets)

### Example: Oracle-Specific Optimizations

```java
public class OracleManager extends SqlManager {
    @Override
    public ResultSet readTable(String tableName, String[] columns, int nThread) {
        // Oracle hint to avoid index scans (faster full table scan for bulk transfer)
        String sql = "SELECT /*+ NO_INDEX(" + tableName + ")*/ * FROM " + tableName 
                   + " WHERE ORA_HASH(rowid, " + (options.getJobs() - 1) + ") = ?";
        // Bind partition index: 0, 1, 2, ... (jobs-1)
        return executeQuery(sql, nThread);
    }
    
    @Override
    public String escapeColName(String colName) {
        // Oracle supports case-sensitive identifiers with double quotes
        return options.getQuotedIdentifiers() ? "\"" + colName + "\"" : colName;
    }
}
```

### Example: PostgreSQL COPY Protocol

```java
public class PostgresqlManager extends SqlManager {
    @Override
    public int insertDataToTable(ResultSet resultSet, int taskId) {
        // Use PostgreSQL COPY for 10x faster inserts than standard JDBC
        CopyManager copyManager = ((PGConnection) connection).getCopyAPI();
        String copySQL = "COPY " + tableName + " FROM STDIN WITH (FORMAT binary)";
        return copyManager.copyIn(copySQL, new ResultSetInputStream(resultSet));
    }
}
```

## Error Handling Strategy

**Exception Propagation**:
- **Database errors**: Caught in worker threads, logged with task ID, propagated to main thread
- **Type conversion errors**: Explicit exceptions (not silent failures) with source/target type details
- **Connection errors**: Retry logic in `ConnManager.getConnection()` with exponential backoff

**Transaction Management**:
- **Source**: Read-only, auto-commit disabled (streaming large ResultSets)
- **Sink**: Auto-commit disabled, manual commit after batch completion
- **Rollback**: On any thread exception, all sink transactions rolled back

**Logging Conventions**:
```java
LOG.info("TaskId-{}: Starting replication from {} to {}", taskId, sourceTable, sinkTable);
LOG.error("TaskId-{}: Failed to insert batch: {}", taskId, e.getMessage(), e);
LOG.debug("TaskId-{}: Fetched {} rows in {}ms", taskId, rowCount, elapsedMs);
```

## Testing Philosophy and Strategies

**TestContainers Integration Tests**:
- **Why**: Database logic requires real databases, not mocks (JDBC driver quirks, SQL dialect differences)
- **Pattern**: One container per database type (PostgreSQL, MySQL, Oracle Free, SQL Server, MongoDB)
- **Lifecycle**: `@BeforeAll` starts container (reused across tests), `@AfterEach` truncates tables

```java
@Testcontainers
class Postgres2PostgresTest {
    private static PostgreSQLContainer<ReplicadbPostgresqlContainer> postgres;
    
    @BeforeAll
    static void setUp() {
        postgres = ReplicadbPostgresqlContainer.getInstance();  // Singleton pattern
    }
    
    @Test
    void testCompleteMode() throws Exception {
        String[] args = {
            "--source-connect", postgres.getJdbcUrl(),
            "--source-table", "t_source",
            "--sink-table", "t_sink",
            "--mode", "complete",
            "--jobs", "4"
        };
        assertEquals(0, ReplicaDB.processReplica(new ToolOptions(args)));
        assertEquals(4097, countSinkRows());  // Verify row count
    }
}
```

**Test Coverage Requirements**:
- **New databases**: Test complete, incremental, parallel modes
- **Data types**: Test all supported types (numeric, string, date/time, LOB, binary)
- **Edge cases**: Null values, empty tables, single-row tables, Unicode, timezones

**Test Data Organization**:
- SQL fixtures: `src/test/resources/{database}/{database}-source.sql`
- Sink schemas: `src/test/resources/sinks/{database}-sink.sql`
- CSV test data: `src/test/resources/csv/`

## Configuration and Build Strategy

**Maven Multi-Profile Build**:
- **Default**: Skips long-running tests (Oracle spatial, cross-version tests)
- **CI Profile**: Runs all tests with Docker containers
- **Dependencies**: `provided` scope for optional JDBC drivers (DB2, Denodo)

**Property-Based Configuration**:
```properties
# replicadb.conf - supports environment variable substitution
source.connect=jdbc:oracle:thin:@${ORACLE_HOST}:${ORACLE_PORT}:${ORACLE_SID}
source.user=${ORACLE_USER}
source.password=${ORACLE_PASSWORD}
mode=incremental
jobs=4
```

**JDBC Driver Management**:
- **Bundled**: PostgreSQL, MySQL/MariaDB, Oracle, SQL Server, SQLite, Kafka
- **User-Provided**: DB2, Denodo (licensing restrictions)
- **Location**: `$REPLICADB_HOME/lib/` for custom drivers

## Performance Patterns and SLAs

**Target Metrics**:
- **Small tables (<100K rows)**: Single thread (no parallelism overhead)
- **Medium tables (100K-10M rows)**: 2-4 threads, sub-10 minute transfers
- **Large tables (>10M rows)**: 4-8 threads, network bandwidth typically bottleneck

**Optimization Techniques**:
- **Fetch size tuning**: `--fetch-size` controls JDBC cursor batch size (default 5000)
- **Bandwidth throttling**: `--bandwidth-throttling` limits bytes/sec for network-constrained links
- **Index management**: `--sink-disable-index` drops indexes before insert, rebuilds after

**Database-Specific Optimizations** (in Manager subclasses):
- **Oracle**: `NO_INDEX` hint forces full table scan
- **PostgreSQL**: Binary COPY protocol instead of INSERT statements
- **SQL Server**: Bulk Insert API with batch retry logic for deadlocks
- **MongoDB**: Aggregation pipeline optimization for complex queries

## Anti-Patterns to Avoid

**DO NOT**:
- Add database-specific logic to `SqlManager` or `ReplicaDB` (use `XYZManager` subclass)
- Skip TestContainers tests (unit tests insufficient for database logic)
- Break CLI argument compatibility (add new args, never remove/rename)
- Use connection pooling in Phase 1 (stateless model, one job = one connection lifecycle)
- Assume JDBC compliance (test actual driver behavior with TestContainers)

**DO Instead**:
- Isolate database logic: Create new Manager subclass
- Test with real databases: Add TestContainers integration test
- Extend CLI args: Add `--new-feature` flag with backward-compatible default
- Optimize connections: Tune fetch size, batch size, connection properties
- Handle driver quirks: Document exceptions in Manager class comments

## Development Workflow Guidelines

**Adding New Database Support**:
1. Create `XYZManager extends SqlManager` in `org.replicadb.manager`
2. Override `getDriverClass()`, `escapeColName()`, partition logic
3. Register in `ManagerFactory` JDBC URL pattern switch statement
4. Add TestContainers configuration in `src/test/java/org/replicadb/config`
5. Create test fixtures in `src/test/resources/{database}/`
6. Write integration tests covering complete/incremental/parallel modes

**Fixing Type Mapping Issues**:
1. Identify failing conversion in TestContainers test output
2. Override `getSqlType()` or `setColumnValue()` in database Manager
3. Add test case with specific data type to `{Database}2{Database}Test`
4. Document limitation if no clean mapping exists (e.g., Oracle SDO_GEOMETRY → PostgreSQL without PostGIS)
