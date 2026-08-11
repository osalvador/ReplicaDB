## Hierarchy and Dispatch
| Layer | Examples | Rule |
| --- | --- | --- |
| Connection contract | `ConnManager` | owns read/insert, connection, lifecycle hooks, naming, cleanup, and primary-key queries |
| Generic SQL | `SqlManager` | owns JDBC connections, streaming statements, metadata, transactions, staging helpers, and shared type utilities |
| SQL adapters | PostgreSQL, Oracle, MySQL, SQL Server, DB2, SQLite, Denodo, standard JDBC | own dialect SQL, partitioning, type mapping, and sink operations |
| Non-SQL adapters | MongoDB, Kafka, S3, local files | override connection and row/SDK behavior while retaining the manager contract |
| Dispatch | `SupportedManagers`, `ManagerFactory`, `FileManagerFactory` | select by connection scheme and file format |

## Connection and Transaction Pattern
SQL managers load the configured driver class, create a source or sink connection, and disable auto-commit. Source close rolls back an open transaction; sink operations commit after successful batches or native bulk operations and roll back on errors. Non-JDBC managers may return a null JDBC connection and manage their SDK client separately.

## Transfer Strategies
- PostgreSQL uses text or binary `COPY`, selecting binary only for supported column types.
- MySQL and MariaDB use `LOAD DATA LOCAL INFILE` with driver-specific statement unwrapping.
- SQL Server uses `SQLServerBulkCopy`, column metadata mapping, table locking, and bounded retry with jitter for deadlock error 1205.
- MongoDB uses cursor/aggregation reads and unordered bulk writes; its sink rejects complete-atomic mode.
- Kafka serializes rows to JSON with a producer; S3 delegates file or object upload behavior to AWS SDK code; CSV/ORC use `FileManager` implementations.
- `StandardJDBCManager` is the generic batch-insert fallback for JDBC sources/sinks without a specialized manager.

## Parallel Reads and Mapping
Partitioning is manager-specific. Oracle uses `ORA_HASH(rowid, jobs - 1)`; PostgreSQL, MySQL, and MongoDB use offset/limit or equivalent chunking; other managers provide their own SQL or cursor strategy. Do not move these expressions into `SqlManager` unless they are genuinely generic.

Manager implementations also own JDBC-to-native DDL mapping and vendor handling for binary, LOB, temporal, XML, array, and JSON values. Preserve precision and null semantics; use `ResultSet.wasNull()` for primitive getters before binding sink values.

## Known Capability Boundaries
The README capability matrix is the user-facing reference for supported source/sink/mode combinations. S3 and Kafka have no SQL staging DDL path, SQLite and MongoDB reject complete-atomic in their current managers, and the generic JDBC fallback has fewer mode guarantees than specialized SQL adapters.

## Reference Implementations
- `src/main/java/org/replicadb/manager/ConnManager.java`
- `src/main/java/org/replicadb/manager/SqlManager.java`
- `src/main/java/org/replicadb/manager/ManagerFactory.java`
- `src/main/java/org/replicadb/manager/PostgresqlManager.java`
- `src/main/java/org/replicadb/manager/SQLServerManager.java`
- `src/main/java/org/replicadb/manager/OracleManager.java`
- `src/main/java/org/replicadb/manager/MongoDBManager.java`
