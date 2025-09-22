---
applyTo: '**'
---

# ReplicaDB: Enterprise Data Replication Tool

## Business Purpose

ReplicaDB solves the critical business problem of **efficient bulk data transfer between heterogeneous databases and data stores**. Unlike complex ETL tools, ReplicaDB provides a lightweight, cross-platform solution for organizations that need:

- **High-performance data migration** without streaming replication complexity
- **Multi-database compatibility** across SQL, NoSQL, and file systems
- **Simple deployment** without remote agents or database modifications
- **Cost-effective ETL/ELT processing** for analytics and data warehousing

## Strategic Architectural Decisions

### 1. Manager Factory Pattern
**Why**: ReplicaDB chose the Manager Factory pattern to handle the complexity of supporting 15+ different database types and file formats. This decision enables:
- **Uniform interface** across diverse data sources (Oracle, PostgreSQL, MongoDB, S3, Kafka)
- **Runtime selection** of appropriate connection managers based on JDBC URL schemes
- **Extensibility** for new data sources without core changes

### 2. Abstract SQL Manager Hierarchy
**Why**: SQL databases share common JDBC patterns, so ReplicaDB uses abstract base classes (SqlManager) with database-specific implementations. This architectural choice:
- **Reduces code duplication** across similar database implementations
- **Enables consistent behavior** for transactions, connection pooling, and error handling
- **Simplifies maintenance** when adding JDBC-compliant databases

### 3. Non-Intrusive Design Philosophy
**Why**: Unlike SymmetricDS or similar tools, ReplicaDB deliberately avoids installing triggers or agents in source databases because:
- **Enterprise security requirements** often prohibit database modifications
- **Operational simplicity** reduces maintenance overhead
- **Cross-platform compatibility** works with any server that can run Java

## Technology Context

ReplicaDB is built on **Java 8+ with Maven** for several strategic reasons:
- **Enterprise compatibility**: Java ensures broad platform support in corporate environments
- **JDBC ecosystem**: Leverages mature database driver ecosystem
- **Container deployment**: Docker/Podman support for modern infrastructure
- **Dependency management**: Maven handles complex driver dependencies automatically

## Decision Framework for AI

### When adding new data sources:
- **Extend existing patterns**: Use ManagerFactory registration and SqlManager inheritance
- **Follow URL scheme detection**: Implement `isTheManagerTypeOf()` for automatic selection
- **Maintain connection abstraction**: Override abstract methods in ConnManager hierarchy

### When modifying replication logic:
- **Preserve mode compatibility**: Support complete, complete-atomic, and incremental modes
- **Consider parallel processing**: Ensure thread safety in multi-job scenarios
- **Handle bandwidth throttling**: Respect existing performance controls

### When updating dependencies:
- **Security first**: Dependabot automatically manages vulnerabilities
- **Compatibility preservation**: Test against all supported database versions
- **Container compatibility**: Verify Docker and Podman builds

## Integration Philosophy

ReplicaDB operates as a **data movement utility** in enterprise data architectures:
- **Source of truth preservation**: Never modifies source data
- **Transaction consistency**: Ensures atomic operations where configured
- **Monitoring integration**: Sentry support for production observability
- **Configuration-driven**: Property files enable infrastructure-as-code patterns

## Quality Principles

### Good Contributions
- **Database-specific optimizations** in dedicated manager classes
- **Configuration options** that enhance replication flexibility
- **Performance improvements** that scale with data volume
- **Test coverage** for cross-database scenarios

### Avoid
- **Breaking changes** to configuration file format
- **Dependencies** that conflict with enterprise security policies
- **Platform-specific code** that reduces cross-platform compatibility
- **Complex abstractions** that obscure database-specific behavior

## Success Metrics

AI contributions should optimize for:
- **Replication throughput** (rows per second across different database combinations)
- **Memory efficiency** (handling large datasets without OOM errors)
- **Configuration simplicity** (minimal setup for common use cases)
- **Error diagnostics** (clear messages for connection and data type issues)
