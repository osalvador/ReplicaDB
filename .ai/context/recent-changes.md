## Recent Master History
The latest history contains direct commits rather than a recent run of JIRA-keyed merge commits.

| Commit / PR | Description | Layers Affected | Date | Key Decision |
| --- | --- | --- | --- | --- |
| `d4f8816` | Align Oracle XML and Snappy versions | build | 2026-08-11 | Keep related runtime artifacts on compatible versions |
| `bbb5017` / `PR-242` | Migrate tests to JUnit 6 and Java 17 | build, CI, tests, packaging | 2026-08-11 | Java 17 is the runtime/build baseline |
| `70e58c0` / `#280` | Remove the extra XML declaration from SQL Server bulk records | SQL Server manager | 2026-08-11 | Preserve source XML representation for SQL Server |
| `78d6bb7` / `#267` | Bump Kafka clients | Kafka manager, build | 2026-08-11 | Keep the producer dependency current |
| `2ea79b5` / `#273` | Bump docs concurrent-ruby | docs | 2026-08-11 | Maintain the Jekyll dependency set |
| `b1d6e7f` / `#274` | Bump Actions checkout | CI | 2026-08-11 | Keep workflow actions current |
| `bf112a1` / `#278` | Bump Jackson databind | Kafka/Mongo-related serialization, build | 2026-08-11 | Apply dependency security/update maintenance |
| `8c85227` / `#277` | Bump PostgreSQL JDBC | PostgreSQL manager, build | 2026-08-11 | Keep the JDBC driver current |
| `44c760d` / `#279` | Bump docs json | docs | 2026-08-11 | Maintain frontend/documentation dependencies |
| `0f7087d` | Update Markup Forge docs tooling | docs/markdown | 2026-06-02 | Continue the browser-tool migration |

## Structural Changes
- Java 17, JUnit Jupiter 6, Surefire 3.5.3, and the ORC module-opening flag are now part of the build/runtime contract.
- SQL Server bulk serialization changed without changing the manager boundary.
- The repository contains a separate Vite-based Markdown tool alongside the Java product and Jekyll docs.

## Patterns Introduced
- Package/runtime migration checks must include compiled tests, packaged launchers, both container image families, and architecture-specific Docker behavior.
- Native manager optimizations remain localized: SQL Server bulk records, PostgreSQL COPY, Kafka producer, and vendor type mapping stay outside generic orchestration.

## Recent Learnings
- WARNING **Packaging**: check image manifests and user/group commands on every supported architecture before choosing a base tag. Source: `PR-242`.
- WARNING **Java runtime**: exercise packaged ORC flows; compile-only validation misses reflective access requirements. Source: `PR-242`.
- WARNING **Integration**: separate CI Docker assumptions from local reuse, emulation, socket, and memory constraints. Source: `PR-242`.

## Known Tech Debt
| Source | Description | Impact |
| --- | --- | --- |
| `README.md` | Prerequisites still mention Java 11 while the system requirements and build use Java 17 | deployment guidance can conflict |
| `config/Sentry.java` | Connection parameter maps and full connect strings are attached to telemetry | possible secret disclosure |
| `ReplicaDBTest.java` | A legacy JUnit 4 test remains beside the Jupiter suite | inconsistent test discovery/style |
| `S3Manager.java`, `KafkaManager.java` | SQL staging/DDL lifecycle hooks are intentionally incomplete for non-SQL sinks | mode behavior differs by adapter |

## Gap Recurrence Candidates
None. Only one archived plan contains an execution retrospective, so recurrence cannot yet be established.
