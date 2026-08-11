## Replication Vocabulary
| Term | Definition | Evidence |
| --- | --- | --- |
| Source | The configured database, file, document store, object store, or other input | `DataSourceType.SOURCE`, `ToolOptions` |
| Sink | The configured destination receiving rows, documents, files, objects, or messages | `DataSourceType.SINK`, manager insert methods |
| Manager | A source/sink adapter that owns connection, dialect, type, and lifecycle behavior | `ConnManager`, `SqlManager`, concrete managers |
| Task | One parallel callable with its own source and sink managers | `ReplicaTask`, `--jobs` |
| Partition | The subset read by one task, selected by a manager-specific hash, offset, or cursor strategy | manager `readTable` implementations |
| Staging table | Temporary sink storage used by incremental or complete-atomic flows before merge/cleanup | `sink-staging-*` options, `SqlManager` |
| Row set | A JDBC-shaped cursor and metadata adapter used to bridge non-JDBC sources | `org.replicadb.rowset` |

## Business Rules
- Complete, incremental, and complete-atomic are distinct execution contracts; capability depends on the sink manager.
- Data movement should preserve source values, metadata, precision, and null semantics. Unsupported conversions must remain visible as errors.
- User-defined staging tables are not disposable temporary state; cleanup protects them, while generated staging resources may be removed.
- Parallelism is bounded by the configured `jobs` count and database-specific partition behavior, not by a universal cross-database algorithm.

## Reference Implementations
- `src/main/java/org/replicadb/cli/ReplicationMode.java`
- `src/main/java/org/replicadb/manager/ConnManager.java`
- `src/main/java/org/replicadb/ReplicaTask.java`
- `openspec/specs/jdbc-type-mapping/spec.md`
- `openspec/specs/staging-table-cleanup/spec.md`
