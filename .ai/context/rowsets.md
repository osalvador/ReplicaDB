## ResultSet Adaptation
ReplicaDB keeps a JDBC-shaped transfer contract even for non-JDBC sources. Managers return a `ResultSet`; `ReplicaTask` passes it to the sink manager; row-set adapters provide metadata, cursor movement, and typed getters for file and document data.

| Adapter | Input | Behavior |
| --- | --- | --- |
| `StreamingRowSetImpl` | JDBC query | forward-only streaming row set with fetch-size and connection properties |
| `CsvCachedRowSetImpl` | CSV file | parses configured columns/types and caches rows in memory |
| `OrcCachedRowSetImpl` | ORC file | reads ORC records and exposes JDBC metadata/values |
| `MongoDBRowSetImpl` | Mongo cursor or aggregation | presents BSON documents and projections as row-set columns |
| `ReplicaRowSetBase` | shared base | minimal read-only `CachedRowSet` implementation and metadata storage |

## Contracts
Row-set implementations must expose stable column labels and JDBC types because sink managers use `ResultSetMetaData` to choose DDL, column lists, typed bindings, or native serialization. File adapters are cached and therefore have a higher memory cost than `StreamingRowSetImpl`. `ReplicaRowSetProvider` supplies the standard row-set provider integration.

## Type and Null Semantics
The base row set supplies convenience getters, but primitive getters return Java defaults when the underlying value is absent. Sink managers that bind primitive values must check `wasNull()` immediately after the getter; object getters also require null checks. The OpenSpec JDBC type-mapping specification and manager-specific null tests are the reference for this behavior.

## Throughput Controls
`BandwidthThrottling` estimates the first row size and gates fetch groups through a timed semaphore. Managers invoke it while iterating rows, alongside the configured fetch size. Keep throttling in the row iteration path so JDBC, file, MongoDB, and Kafka/S3 transfers share the same operational control.

## Reference Implementations
- `src/main/java/org/replicadb/rowset/ReplicaRowSetBase.java`
- `src/main/java/org/replicadb/rowset/StreamingRowSetImpl.java`
- `src/main/java/org/replicadb/rowset/CsvCachedRowSetImpl.java`
- `src/main/java/org/replicadb/rowset/OrcCachedRowSetImpl.java`
- `src/main/java/org/replicadb/rowset/MongoDBRowSetImpl.java`
- `src/main/java/org/replicadb/manager/util/BandwidthThrottling.java`
