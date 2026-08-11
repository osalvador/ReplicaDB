## Service Description
ReplicaDB is a Java command-line tool for bulk transfer between heterogeneous databases, files, object storage, and Kafka. DBAs and automation invoke one process with source, sink, mode, and parallelism options; the current repository is a CLI and library-style core, not a REST microservice.

## Tech Stack
Java 17, Maven, Apache Commons CLI, JDBC plus vendor SDKs, Log4j2/Sentry, JUnit Jupiter 6 and Testcontainers. Deploy: standalone archive, shell/Windows launchers, Docker, or Podman.

## Project Structure
| Layer | Key Packages | Key Patterns |
| --- | --- | --- |
| Orchestration | `org.replicadb`, `cli.ReplicationMode` | fixed executor, per-task managers, pre/post hooks |
| CLI and configuration | `org.replicadb.cli` | Commons CLI, properties file, `${ENV}` expansion |
| Source/sink adapters | `org.replicadb.manager` and subpackages | `ConnManager` -> `SqlManager` -> manager subclasses, factory dispatch |
| Data adaptation | `org.replicadb.rowset`, `manager.util`, `time` | JDBC-shaped row sets, metadata, type conversion, throttling |
| Verification and delivery | `src/test`, `conf`, `bin`, Docker files, `docs`, workflows | Testcontainers fixtures, release profiles, Jekyll and Vite tooling |

## Key Decisions
- **Manager hierarchy**: isolate dialects, type mappings, and native bulk APIs so new data sources do not expand the orchestrator.
- **JDBC-shaped pipeline**: adapt non-JDBC inputs to `ResultSet`/row-set contracts to reuse sink logic.
- **Job-level parallelism**: `--jobs` creates a fixed pool and each task owns source and sink manager instances, avoiding a heavyweight runtime.
- **Explicit replication modes**: complete, incremental, and complete-atomic control truncation, staging, and merge behavior.

## Anti-Patterns
- Do not put database-specific SQL, type mapping, or bulk behavior in `ReplicaDB` or the generic base manager.
- Do not assume every manager supports every mode; capability differences are explicit and some sinks are unsupported for staging or incremental behavior.
- Do not add unredacted passwords, DSNs, connection parameters, or full credential-bearing URLs to logs, telemetry, or generated context; current Sentry scope setup accepts connection parameters and connect tags.

## Key Conventions
- Preserve the CLI/property option contract and use `ToolOptions` as the configuration boundary.
- Use manager factory dispatch and `DataSourceType.SOURCE`/`SINK`; extend the nearest manager abstraction.
- Keep source reads streaming where possible, honor `fetch-size`, and apply `BandwidthThrottling` inside row iteration.
- Correlate parallel work with `TaskId-*` thread names and propagate failures to the orchestrator.
- Use JUnit Jupiter 6 for new tests, Testcontainers for real database behavior, and repository fixtures under `src/test/resources`.

## Recent Changes
- `d4f8816`: aligned Oracle XML and Snappy dependency versions (build).
- `bbb5017` / `PR-242`: migrated tests and runtime tooling to Java 17 and JUnit 6 (build, CI, tests, packaging).
- `70e58c0` / `#280`: stopped adding an XML declaration in SQL Server bulk records (SQL Server adapter).
- `78d6bb7` / `#267`: updated Kafka clients (Kafka adapter, build).
- `b1d6e7f` / `#274`: updated GitHub Actions checkout usage (CI).

## Recent Learnings
- WARNING [packaging]: validate runtime image manifests and user-management commands on every target architecture. Source: `PR-242`.
- WARNING [Java 17]: exercise packaged ORC paths because reflective dependencies may require the existing module-opening flag. Source: `PR-242`.
- WARNING [integration tests]: distinguish clean CI Docker assumptions from local reuse, architecture, socket, and memory constraints. Source: `PR-242`.

-> Pointers: `.ai/context/domain.md`, `.ai/context/execution.md`, `.ai/context/cli.md`, `.ai/context/managers.md`, `.ai/context/rowsets.md`, `.ai/context/testing.md`, `.ai/context/operations.md`, `.ai/context/recent-changes.md`
