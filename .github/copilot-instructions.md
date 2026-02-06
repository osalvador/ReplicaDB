---
applyTo: '**'
---

# ReplicaDB Project Overview

## Business Purpose

ReplicaDB solves the **enterprise data mobility problem**: organizations need to move large volumes of data between heterogeneous databases and storage systems for ETL/ELT workflows, data migrations, and synchronization tasks. Traditional solutions either require database agents (intrusive), are locked to specific ecosystems (Hadoop/Sqoop), or demand custom development for each job (Pentaho/Talend). ReplicaDB provides a **zero-agent, zero-custom-code, heterogeneous bulk data transfer** solution.

## Strategic Architectural Decisions

**1. Manager Pattern for Database Abstraction**
- **Why**: Supporting 15+ heterogeneous data sources (Oracle, PostgreSQL, MySQL, SQL Server, MongoDB, S3, Kafka, etc.) requires clean separation of database-specific logic
- **Impact**: Each database gets its own `Manager` class extending `SqlManager` or `ConnManager`, isolating JDBC dialects, data type mappings, and performance optimizations
- **Trade-off**: More classes but eliminates conditional logic and makes adding new databases straightforward

**2. Native Parallelism with JDBC**
- **Why**: Enterprises move millions/billions of rows, requiring parallel execution without external frameworks
- **Approach**: Built-in thread pool with configurable `--jobs` parameter, data partitioning via database-native functions (`ORA_HASH`, `HASH`, `ROW_NUMBER`)
- **Constraint**: Avoids Hadoop dependency (unlike Sqoop), making ReplicaDB deployable anywhere Java 11+ runs

**3. CLI-First, Evolving to API**
- **Why**: Started as CLI tool for batch/cron jobs (primary use case), now evolving to REST API for scheduling and monitoring
- **Current State**: Pure CLI with configuration files; Phase 1 evolution adds Spring Boot API without breaking CLI compatibility
- **Design Principle**: CLI remains first-class citizen with optimized startup (no Spring Boot overhead when running as CLI)

## Technology Context

**Core Stack**:
- **Java 11+**: Cross-platform compatibility, mature ecosystem, enterprise adoption
- **Pure JDBC**: Direct database connectivity without ORM overhead or framework lock-in
- **Maven**: Standard Java build tool with clear dependency management
- **TestContainers**: Real database integration tests, not mocks

**Why Java/JDBC**: Enterprise data pipelines require stability, performance, and compatibility with corporate environments where Java is standard. JDBC provides universal database connectivity without vendor lock-in.

## Decision Framework for AI

**When adding new database support**:
1. Create `XYZManager extends SqlManager` in `org.replicadb.manager`
2. Override type-specific methods (`escapeColName`, `getDriverClass`, data type mappings)
3. Register in `ManagerFactory` via JDBC URL pattern matching
4. Add TestContainers integration tests for complete/incremental/parallel modes

**When fixing bugs**:
- For data type issues → investigate `SqlManager` type mapping and database-specific `Manager` overrides
- For parallel execution issues → check partitioning logic in database-specific `Manager.readTable()`
- For connection issues → review `ConnManager` connection lifecycle and `discardConnection()` implementations

**When optimizing performance**:
- Database-specific optimizations belong in `Manager` classes (e.g., Oracle hints, PostgreSQL COPY, SQL Server Bulk)
- General parallelism tuning → adjust `--jobs` parameter and partition boundary calculations
- Network optimization → `--bandwidth-throttling` for rate limiting

## Integration Philosophy

ReplicaDB is a **point-to-point replication tool**, not an ETL platform:
- **Operates between**: Any two supported data sources (source → sink)
- **Does NOT**: Complex transformations, orchestration, scheduling (these belong in external tools)
- **Integration Pattern**: Invoked by schedulers (cron, Jenkins, Airflow) or soon via REST API
- **State Management**: Stateless execution model; state tracking via incremental mode using timestamps/sequences

## Quality Principles

**Good contributions**:
- Preserve backward compatibility (CLI arguments, configuration files)
- Add comprehensive TestContainers integration tests for new features
- Follow Manager pattern for database-specific logic
- Document performance characteristics and limitations
- Use existing partitioning strategies before inventing new ones

**Avoid**:
- Breaking CLI argument compatibility
- Adding dependencies that increase Docker image size unnecessarily
- Database-specific logic in core `ReplicaDB` or `SqlManager` (belongs in `XYZManager`)
- Skipping integration tests (unit tests alone are insufficient for database logic)
- Hardcoding database-specific behavior without configuration options

## Evolution Path

**Current**: Pure CLI tool with extensive database support
**Phase 1** (planned): Spring Boot API for job scheduling/monitoring while preserving CLI mode
**Phase 2** (future): Kubernetes-native with Redis queue and horizontal worker scaling
**Principle**: Each phase builds on previous without rewrites; CLI remains first-class forever
