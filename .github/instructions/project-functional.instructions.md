---
applyTo: '**'
---

# Business Context and Functional Scope

## Business Problem Domain

**Enterprise Data Mobility Challenge**: Organizations struggle to move data between heterogeneous databases for analytics, migrations, and synchronization. Traditional approaches have critical limitations:
- **ETL platforms** (Talend, Pentaho) require custom development per job, increasing maintenance burden
- **Database-specific tools** (Oracle GoldenGate, SQL Server Replication) lock you into vendor ecosystems
- **Hadoop-dependent tools** (Sqoop) require massive infrastructure even for simple transfers
- **CDC solutions** (SymmetricDS) install triggers in source databases, creating intrusive overhead

**What Businesses Need**: Simple, fast, non-intrusive bulk data transfer between ANY two databases without custom code, database agents, or heavyweight infrastructure.

## Functional Scope and Boundaries

**What ReplicaDB Does**:
- **Bulk data replication** between 15+ heterogeneous database types (RDBMS, NoSQL, object storage, streaming)
- **Schema-aware transfers** preserving data types, nullability, and precision across different database dialects
- **Parallel data transfer** splitting large tables into partitions for concurrent processing
- **Incremental synchronization** using timestamp or sequence columns to transfer only new/changed data
- **Network-constrained transfers** with bandwidth throttling for production environments
- **Table-to-table transfers** with optional column mapping and WHERE clause filtering

**What ReplicaDB Does NOT Do** (by design):
- Complex data transformations (use dbt, Apache Spark, or ETL platforms)
- Job scheduling and orchestration (use cron, Jenkins, Airflow, or Kubernetes CronJobs)
- Real-time change data capture (use Debezium, SymmetricDS, or database-native CDC)
- Data quality validation (use Great Expectations, dbt tests, or custom validation scripts)
- Data lineage tracking (use Apache Atlas, OpenLineage, or data catalog tools)

**Boundary Rationale**: ReplicaDB excels at ONE thing—moving data efficiently. External tools handle scheduling, transformation, and validation **because** separation of concerns enables best-of-breed integration.

## Primary Business Workflows

### 1. Initial Database Migration (Complete Mode)
**Business Context**: Moving entire database to new platform (Oracle → PostgreSQL, SQL Server → MySQL)
**Flow**: 
1. DBA identifies source tables and target database
2. ReplicaDB truncates sink tables (or creates if missing)
3. Parallel threads extract partitioned source data via hash functions
4. Each thread inserts directly into sink database
5. Optional: Disable sink indexes during transfer, rebuild after completion

**Why This Matters**: Organizations avoid months of custom migration scripts. Example: 10TB Oracle database migrated to PostgreSQL in 6 hours with 8 parallel jobs.

### 2. Incremental Data Synchronization (Incremental Mode)
**Business Context**: Daily/hourly sync of new records (e.g., transaction logs, event streams)
**Flow**:
1. Previous sync recorded `MAX(updated_at)` timestamp
2. ReplicaDB queries `WHERE updated_at > last_sync_timestamp`
3. New records inserted, existing records updated/merged based on primary keys
4. New sync timestamp persisted for next run

**Why This Matters**: Businesses keep analytics databases current without complex CDC infrastructure. Example: Hourly sync of customer orders from production MySQL to data warehouse.

### 3. Cross-Database Analytics Export (Complete-Atomic Mode)
**Business Context**: Export production data to analytics platform without downtime risk
**Flow**:
1. ReplicaDB creates staging table with temporary name
2. Data transferred to staging table (production untouched)
3. Transaction commits: staging table atomically renamed to target table
4. Rollback on failure: staging table dropped, target unchanged

**Why This Matters**: Analytics teams get fresh data snapshots without impacting production queries or risking partial data states.

## Business Rules and Constraints

**Parallelism Rules**:
- **Jobs parameter** (`--jobs=4`) controls concurrency **because** database connection limits and CPU cores constrain scalability
- **Partition strategy** uses database-native hash functions **because** cross-database portability requires standard SQL compatibility
- **Row-based partitioning** (not byte-based) **because** database cursors operate on rows, not bytes

**Data Type Mapping Rules**:
- **Preserve precision** when possible (Oracle NUMBER(10,2) → PostgreSQL NUMERIC(10,2))
- **Fail explicitly** on incompatible types (Oracle SDO_GEOMETRY → PostgreSQL without PostGIS)
- **Lossy conversions forbidden** without explicit user configuration **because** silent data corruption violates trust

**Connection Management Rules**:
- **Source connections**: Read-only, auto-commit disabled, configurable fetch size **because** large result sets require cursor streaming
- **Sink connections**: Auto-commit disabled, batch inserts enabled **because** transaction overhead dominates small insert performance

## Domain Concepts and Entities

**Replication Job**: Configuration defining source, sink, mode, and parallelism for a single table transfer
**Manager**: Database-specific adapter handling JDBC dialects, type mappings, and performance optimizations
**Mode**: Execution strategy (complete, incremental, complete-atomic) determining how data is written to sink
**Partition**: Subset of source table rows assigned to a single thread based on hash function output
**Staging Table**: Temporary sink table used in atomic mode to avoid partial data visibility

## Information and Event Flows

**Configuration Flow**:
- CLI arguments → `ToolOptions` parser → Property validation → Manager instantiation
- Configuration files support environment variable substitution (`${DB_PASSWORD}`) for credential management

**Data Flow (Parallel Execution)**:
1. **Main thread**: Analyzes source table, calculates partition boundaries
2. **Partition creation**: Generates N WHERE clauses based on hash function ranges (e.g., `WHERE ORA_HASH(rowid, 3) = 0`)
3. **Worker threads**: Each opens source connection, executes partitioned SELECT, streams ResultSet to sink
4. **Sink insertion**: Batched INSERTs or database-native bulk APIs (PostgreSQL COPY, SQL Server Bulk Insert)
5. **Completion**: All threads join, final row counts validated, connections closed

**Error Propagation**: First thread exception cancels remaining threads, rolls back sink transaction, exits with non-zero status **because** partial data states are unacceptable.

## Business Quality Requirements

**Performance Targets** (driving technical choices):
- **Million+ row tables**: Sub-10 minute transfers with 4-8 parallel jobs
- **Billion+ row tables**: Hours, not days (network bandwidth typically bottleneck, not CPU)
- **Cross-datacenter**: Bandwidth throttling prevents saturation of shared links

**Data Consistency Requirements**:
- **Complete mode**: Snapshot consistency (source data at job start time)
- **Incremental mode**: No lost records between syncs (idempotent based on timestamps/sequences)
- **Atomic mode**: All-or-nothing visibility in sink database

**Operational Requirements** (why CLI-first design):
- **Zero-dependency deployment**: Single JAR file, no application server
- **Embeddable**: Docker containers, Kubernetes Jobs, Lambda functions
- **Exit codes**: Standard Unix conventions (0=success, non-zero=failure) for scripting integration
- **Logging**: Structured output for log aggregation (ELK, Splunk)

## Compliance and Integration Context

**Security Constraints**:
- Credentials via environment variables (not command-line arguments visible in `ps`)
- No credential logging or persistence outside configuration files
- Database permissions: Source requires SELECT only, sink requires INSERT/UPDATE/CREATE

**Corporate Integration** (upcoming evolution):
- REST API for centralized job scheduling
- Metrics export (Prometheus) for observability
- Event emission for data lineage tracking
