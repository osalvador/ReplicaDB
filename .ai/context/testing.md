## Test Layers
| Layer | Evidence | Use |
| --- | --- | --- |
| CLI and unit tests | `src/test/java/org/replicadb/cli`, manager unit tests, Mockito | validate option parsing, metadata, staging safeguards, type conversion, and native encoding without a live database |
| Integration matrix | `src/test/java/org/replicadb/{db2,file,mariadb,mongo,mysql,oracle,postgres,sqlite,sqlserver}` | exercise source-to-sink combinations against real services |
| Container fixtures | `src/test/java/org/replicadb/config` | singleton Testcontainers with SQL/JSON fixture loading and database setup |
| File fixtures | `src/test/resources` | database schemas, source/sink SQL, CSV, ORC, and Mongo data |

## Construction Patterns
Most tests construct `ToolOptions` from inline `String[]` arguments and then instantiate a concrete manager. Mockito is used for isolated JDBC behavior, such as staging-table cleanup and statements. Database tests obtain a shared container through `Replicadb*Container.getInstance()`, load fixtures in container `start()`, and use `@BeforeAll`/`@BeforeEach` for setup and reset. There are no shared production builders or DTO factories to update; changing an option or manager constructor requires updating these inline argument arrays and affected fixture setup.

## Testcontainers Rules
Use real containers for driver, SQL dialect, cursor, type, and transaction behavior. Keep one fixture container per database family when the existing class follows the singleton pattern, but ensure test data is reset when reuse is enabled. Current CI configuration disables reuse; local Docker architecture, socket, and memory can still change outcomes. Do not treat local infrastructure failures as product failures without reproducing the smallest relevant test under a clean environment.

## Framework Baseline
New tests use JUnit Jupiter 6 annotations and assertions, with Surefire 3.5.3. `ReplicaDBTest.java` still contains legacy JUnit 4 imports and is an exception to the dominant pattern; do not copy that style into new tests. ORC-related execution under Java 17 retains the `--add-opens=java.base/java.nio=ALL-UNNAMED` Surefire/launcher setting.

## Modification Strategy
When production models or signatures change, update the inline `ToolOptions` setup, the concrete manager test, the relevant container fixture, and the matching SQL/JSON resource. Add null, empty, single-row, type-boundary, mode-capability, and parallel partition coverage where the changed manager path warrants it.

## Reference Implementations
- `src/test/java/org/replicadb/manager/SqlManagerStagingTableTest.java`
- `src/test/java/org/replicadb/manager/PostgresqlManagerTest.java`
- `src/test/java/org/replicadb/config/ReplicadbPostgresqlContainer.java`
- `src/test/java/org/replicadb/config/ReplicadbMysqlContainer.java`
- `src/test/resources`
