# ReplicaDB Product Roadmap

> A living document for product strategy and evolution.
>
> Last reviewed: 2026-09-17.

## Purpose

This roadmap defines how ReplicaDB should evolve relative to general-purpose
data integration platforms such as Airbyte. It does not aim to reproduce
Airbyte's entire connector catalog. Its goal is to turn ReplicaDB's current
strengths into a clear, demonstrable, and sustainable product proposition.

The priorities in this document are based on the product available in version
1.0.2, the capabilities documented in this repository, and a comparison with
Airbyte's public documentation as of September 2026.

## Strategic thesis

ReplicaDB should be positioned as:

> The simplest, most open, and most efficient way to move large data volumes
> between heterogeneous databases, especially operational and legacy systems,
> without installing agents or requiring CDC on the source.

ReplicaDB should not attempt to become a smaller Airbyte. It should outperform
general-purpose platforms in a deliberately narrow set of needs:

1. Simple installation and operation.
2. Direct bulk replication between databases.
3. First-class compatibility with Oracle, Db2/IBM i, SQL Server, PostgreSQL,
   MySQL, MariaDB, MongoDB, and JDBC.
4. Minimal intrusion into source systems.
5. Full deployment control and a permissive Apache 2.0 license.
6. Observable and verifiable behavior for large migrations and batch
   synchronization.

## Priority users and use cases

### Primary users

- Database administrators responsible for migrations, copies, and controlled
  synchronization.
- Platform teams that need to run data movement inside private networks and
  regulated environments.
- Integration engineers connecting heterogeneous databases or operating
  legacy systems.
- Data teams that need bulk or micro-batch loads without deploying a complete
  data integration platform.

### Primary use cases

- Database migrations and planned cutovers.
- Large initial loads and backfills.
- Development, testing, and recovery environment refreshes.
- Batch synchronization between operational databases.
- Controlled exports to files, S3, or Kafka.
- Embedded or OEM execution by integrators and other products.

### Non-priority use cases

- Ingestion from hundreds of SaaS applications.
- Complex analytical transformations or a built-in dbt engine.
- Reverse ETL and data activation as a product category.
- Sub-second synchronization or continuous streaming as a baseline behavior.
- Building a platform for AI agents.
- Kubernetes as a requirement or the primary installation surface.

## Position relative to Airbyte

| Area | Current ReplicaDB advantage | Main gap |
| --- | --- | --- |
| Installation | Java CLI and local server without mandatory Docker | Further simplify upgrades, backups, and diagnostics |
| License | Apache 2.0, suitable for commercial use, redistribution, and OEM | Define a sustainable support and community strategy |
| Replication | Database-to-database focus and `complete-atomic` mode | Demonstrate performance and fidelity through benchmarks |
| Legacy sources | Oracle, Db2 LUW, IBM i, Denodo, and generic JDBC | Make per-connector capabilities visible and verifiable |
| Intrusiveness | No agents, triggers, or transaction logs required | Limited incremental mode and no delete propagation |
| Control plane | Jobs, schedules, permissions, auditing, and distributed workers | Missing self-service discovery and data configuration |
| Catalog | Small and coherent connector set | Airbyte offers hundreds of connectors and a mature CDK |
| Schema | Explicit sink control and CLI auto-creation | Missing schema discovery, selection, and change management |
| Automation | CLI and documented HTTP API | Missing tokens, service accounts, and declarative configuration |
| Operations | Durable state, retries, redacted logs, and metrics | Missing data quality, alerts, and a richer execution timeline |

The comparison does not yet justify claiming that ReplicaDB is faster than
Airbyte. ReplicaDB's design is potentially lighter, but the product does not
yet have public, reproducible benchmarks. Turning that hypothesis into evidence
is a product priority.

## Product principles

- **Bulk-first:** optimize full loads, migrations, and micro-batch before
  expanding the product toward real-time replication.
- **Non-intrusive by default:** do not require source-side changes for the
  primary workflow.
- **Correctness before speed:** a fast run without completeness verification
  is not an acceptable result.
- **Operational simplicity:** preserve a useful path that does not require
  Kubernetes, Docker, or an external orchestration platform.
- **Push transformations down:** let users define database-native SQL
  expressions and lifecycle actions that are executed by the source or sink.
  ReplicaDB should generate, validate, coordinate, and observe these actions
  without becoming a transformation engine.
- **Explicit compatibility:** document and test every connector, mode, and
  capability combination; avoid unverified general claims.
- **Automatable:** every important UI operation should have a stable
  automation contract.
- **Extensible without losing focus:** add connectors based on actual demand,
  not to increase a catalog number.
- **Permissive open source:** preserve Apache 2.0 as part of the value
  proposition.

## Current state

The platform already has a substantial foundation:

- Standalone CLI compatible with Windows, Linux, and macOS.
- `complete`, `complete-atomic`, and `incremental` modes where supported by the
  connector.
- In-run parallelism and database-specific optimizations.
- Explicit multi-table catalog support in the CLI.
- Authenticated server with a frontend, API, and durable PostgreSQL state.
- Jobs, Quartz schedules, retries, cancellation, and attempt history.
- Local execution or distributed workers with leases and fencing.
- Encrypted datasources, resource-level permissions, and auditing.
- Redacted run logs, Prometheus metrics, and health checks.
- Local installation with embedded PostgreSQL and optional distributed
  deployment.

The immediate capabilities already identified in the
[README](README.md#roadmap) remain necessary and are incorporated into Phase 1
of this document.

## Roadmap

### Phase 0 — Positioning and evidence

**Indicative horizon:** 0–6 weeks  
**Priority:** P0

#### Objective

Define precisely who ReplicaDB is built for and demonstrate advantages that
are currently only product claims.

#### Deliverables

- [ ] Select one primary user and three reference use cases.
- [ ] Publish a canonical capability matrix per connector covering:
  - source and sink roles;
  - supported modes;
  - parallelism;
  - auto-creation;
  - staging;
  - special data types;
  - authentication;
  - known limitations.
- [ ] Create a reproducible comparison against Airbyte and Sling for at least:
  - PostgreSQL to MySQL;
  - SQL Server to PostgreSQL;
  - Oracle or Db2 to PostgreSQL.
- [ ] Compare installation and time to first successful replication, including:
  - required services and external dependencies;
  - download and installation size;
  - number of installation and configuration steps;
  - startup time and baseline resource consumption;
  - availability of a lightweight local execution path.
- [ ] Compare ease of use for representative workflows, including:
  - connection configuration and testing;
  - schema and table discovery;
  - single-table and multi-table configuration;
  - incremental replication setup;
  - error diagnosis and recovery.
- [ ] Measure throughput, duration, CPU, memory, connections, source pressure,
  startup time, and behavior during failures under equivalent conditions.
- [ ] Add correctness verification to the benchmark through row counts,
  checksums, and representative data type samples.
- [ ] Publish results, configuration, datasets, and limitations without
  selecting only favorable scenarios.
- [ ] Review project messaging so that “high performance” is not presented as
  a demonstrated advantage until reproducible evidence exists.

#### Exit criteria

- There is a verifiable, scenario-specific answer to “when should I choose
  ReplicaDB instead of Airbyte or Sling?”
- A third party can reproduce the results.
- Every performance claim links to a test rather than only to an architecture
  description.

### Phase 1 — Safe self-service

**Indicative horizon:** 2–4 months  
**Priority:** P0

#### Objective

Allow users to configure a correct transfer from the UI without understanding
JDBC internals or discovering configuration errors during a real load.

#### Deliverables

- [ ] Datasource connection test with bounded, redacted results.
- [ ] Standalone CLI commands to test a connection, inspect its capabilities,
  and discover its schemas, tables, views, columns, keys, and supported
  replication modes.
- [ ] Pre-execution validation of source, sink, permissions, tables, keys, and
  capabilities.
- [ ] Schema, table, view, and column explorer.
- [ ] Visual and declarative source-to-sink column mapping with:
  - column selection and destination renaming;
  - optional database-native SQL expressions for source-side conversion or
    transformation;
  - explicit and unique result aliases;
  - source and destination type preview;
  - bounded result preview and JDBC metadata validation;
  - generation of a compatible `source.query`;
  - warnings when custom queries affect parallelism, incremental watermarks,
    partition columns, keys, or auto-creation.
  SQL expressions remain in the source database dialect and are executed by
  the source database; ReplicaDB does not introduce an internal transformation
  language or runtime.
- [ ] Dry run that validates and estimates execution without writing data.
- [ ] Sink auto-creation from managed jobs.
- [ ] Enhanced run detail with attempts, termination reasons, rows, bytes,
  duration, and warnings.
- [ ] Multi-table selection from the schema explorer.
- [ ] Declarative multi-table replication definitions with shared defaults,
  wildcard include and exclude rules, deterministic expansion, and per-table
  overrides for mode, source query, columns, destination, parallelism, and
  incremental settings.
- [ ] Job groups or templates that create and operate multiple jobs through a
  shared view, schedule, and action.
- [ ] An assistant for converting existing CLI configurations into managed
  datasources and jobs without copying secrets into responses or logs.

#### Exit criteria

- A user can test connections, select multiple tables, validate, and start a
  replication without manually editing a properties file.
- A user can preview and validate source-side SQL mappings before any
  destination data is modified.
- Predictable configuration failures are detected before a run is created.
- A migration with dozens of tables does not require operating every job in
  isolation.

### Phase 2 — Reliable incremental replication and data quality

**Indicative horizon:** 4–8 months  
**Priority:** P0/P1

#### Objective

Reduce the risk of silent data loss and make ReplicaDB a reliable option for
repeated micro-batch synchronization.

#### Deliverables

- [ ] Composite watermarks, for example `(updated_at, id)`.
- [ ] Configurable overlap or lookback window for late-arriving data.
- [ ] Deterministic deduplication of rows reread by the lookback window.
- [ ] Consistent snapshot option where supported by the connector.
- [ ] A documented strategy for transactions with late commits.
- [ ] Explicit support for soft deletes and tombstone columns.
- [ ] User-defined database actions, including SQL statements and stored
  procedures, bound to explicit lifecycle phases and executed only when their
  configured phase is reached. The contract must define:
  - whether the action runs against the source or sink;
  - whether it runs before loading, after a successful load, after
    reconciliation, or after a failure;
  - transaction and commit boundaries;
  - timeout and cancellation behavior;
  - retry and idempotency expectations;
  - redaction and audit requirements;
  - distinct outcomes for replication failure and action failure.
- [ ] Connector-aware rejected-row reporting and optional quarantine that
  preserves the run, table, partition, column, database error, and row payload
  under configurable retention and redaction policies. Connectors that cannot
  identify an individual rejected row must report that limitation and fail the
  affected batch explicitly.
- [ ] Configurable post-run reconciliation:
  - source and destination counts;
  - minimum and maximum values;
  - checksums per partition;
  - typed sampling.
- [ ] Schema drift policies: notify, pause, approve, or accept non-destructive
  changes.
- [ ] Pre-execution schema diff.
- [ ] Alerts and webhooks for failures, delays, drift, or data differences.
- [ ] Freshness, lag, rows-read, rows-written, and rows-reconciled metrics.
- [ ] Evaluate per-partition checkpointing and safe resume for long-running
  loads without promising exactly-once where the sink cannot guarantee it.
- [ ] Specify and enforce distinct lifecycle semantics across the CLI, API,
  and UI:
  - `retry` creates another attempt for the same run and follows the
    mode-specific checkpoint policy;
  - `resume` continues from a durable checkpoint without repeating completed
    partitions where the connector can guarantee that behavior;
  - `restart` creates a new run from its configured initial state and applies
    an explicit destination reset policy.
  Each operation must expose its effect on checkpoints, destination data,
  attempt lineage, hooks, and reconciliation.

#### Exit criteria

- Simple watermarks are no longer the recommended path for new managed jobs.
- A successful run can provide evidence of completeness, not merely an
  absence of exceptions.
- Successful replication and successful post-replication actions are reported
  as separate, unambiguous lifecycle outcomes.
- Operators can determine whether a failed run should be retried, resumed, or
  restarted without risking an implicit destination reset.
- Schema changes are detected before they first appear as production write
  failures.

### Phase 3 — Automation and enterprise adoption

**Indicative horizon:** 8–12 months  
**Priority:** P1

#### Objective

Integrate ReplicaDB securely into GitOps workflows, internal platforms, and
enterprise environments without relying on interactive browser sessions.

#### Deliverables

- [ ] Service accounts and revocable scoped tokens.
- [ ] API authentication separated from the frontend session and CSRF
  contract.
- [ ] Declarative export and import of datasources, jobs, schedules, and
  permissions, excluding resolved secrets.
- [ ] Server administration commands in the CLI.
- [ ] Public API versioning and compatibility policy.
- [ ] SDK generated and tested from OpenAPI.
- [ ] Signed, idempotent webhooks for the run lifecycle.
- [ ] OIDC/OAuth2 login and group mapping.
- [ ] External secret references for:
  - HashiCorp Vault;
  - AWS Secrets Manager;
  - GCP Secret Manager;
  - Azure Key Vault.
- [ ] High availability, upgrade, backup, and disaster recovery guides backed
  by automated acceptance tests.
- [ ] Evaluate a Terraform provider after the declarative model and API have
  stabilized.

#### Exit criteria

- A complete environment can be declared and promoted across development,
  testing, and production without exporting credentials.
- Automation does not depend on user cookies.
- Upgrades document compatibility, migrations, and rollback.

### Phase 4 — Selective expansion

**Indicative horizon:** 12 months or later  
**Priority:** P2, conditional on demand

#### Objective

Expand the addressable market without losing the database-to-database focus
or turning the connector catalog into an unsustainable maintenance burden.

#### Candidates

- [ ] BigQuery, Snowflake, or Databricks as sinks, prioritized by user evidence
  rather than catalog parity.
- [ ] Amazon S3 as a source.
- [ ] A complete, tested contract for Parquet, Avro, JSON, and ORC.
- [ ] Public SPI for Java/JDBC connectors.
- [ ] Connector compatibility kit with tests for data types, modes,
  cancellation, retries, redaction, and performance.
- [ ] Independent connector lifecycle and versioning.
- [ ] Optional CDC for PostgreSQL, MySQL, and SQL Server.

CDC should be evaluated as an optional capability. Its introduction must not
make agents, triggers, or transaction log configuration a requirement for
existing bulk and micro-batch workflows.

#### Entry criteria

- There is repeated, quantified demand for the connector or capability.
- Phases 0–3 have stable metrics and no critical correctness or operational
  debt.
- The connector has an owner and a maintenance plan for its full lifecycle.

## Items that will not be prioritized

- Reaching hundreds of SaaS connectors to compete on catalog size.
- Creating a clone of Airbyte Connector Builder.
- Building a proprietary transformation or dbt orchestration engine.
- Adding AI functionality unrelated to replication.
- Making Kubernetes a requirement for local deployment.
- Promising end-to-end exactly-once semantics without actual transactional
  support on both sides.
- Presenting a connector as supported based only on JDBC compatibility without
  integration tests.

## Product metrics

Metrics should compare outcomes, not merely development activity.

### Adoption and experience

- Time from installation to the first correct replication.
- Percentage of connections and jobs that pass preflight on the first attempt.
- Number of manual steps required to configure a multi-table migration.
- Run success rate and the causes of preventable failures.

### Performance and efficiency

- Rows and bytes transferred per second.
- CPU and memory per unit of data transferred.
- Connections and load added to the source and sink.
- Recovery time after worker or connection loss.

### Correctness and operations

- Percentage of reconciled runs with no differences.
- Rejected rows by connector, table, error category, and quarantine outcome.
- Percentage of interrupted runs that resume without repeating completed
  partitions.
- Schema differences detected before writes begin.
- Freshness and delay of incremental jobs.
- Mean time to detect and resolve a failed run.
- Percentage of diagnostics that keep secrets correctly redacted.

### Ecosystem

- Connectors with a complete compatibility matrix and integration CI.
- External contributions accepted and maintained.
- Mean time to validate and publish a new connector version.

## Open decisions

The following decisions require user validation and should not be resolved
solely through feature comparison:

1. Is the primary user a DBA, platform engineer, or data engineer?
2. Is the dominant use case migration, environment refresh, or recurring
   synchronization?
3. What volume, frequency, and recovery point objective do real users expect?
4. Which three source-to-sink routes should form the canonical benchmark?
5. Should a job group be only a collection of jobs or a transactional unit
   with a shared failure policy?
6. Which schema drift policies are safe by default for operational databases?
7. What actual demand exists for CDC compared with non-intrusive micro-batch?
8. Which support or service model can sustain project maintenance without
   restricting Apache 2.0?
9. Which lifecycle phases should support user-defined SQL or stored
   procedures, and which failure policy is safe by default?
10. Which source-query transformations can preserve parallelism, incremental
    watermarks, and deterministic resume for each connector?
11. Which connectors can identify and quarantine individual rejected rows
    without disabling their native bulk-loading path?

## Reference sources

### ReplicaDB

- [Description and compatibility matrix](README.md)
- [Product purpose and principles](PRODUCT.md)
- [Deployment architecture](DEPLOYMENT.md)
- [Replication concepts](docs/src/content/docs/getting-started/concepts.md)
- [Watermark limitations](docs/src/content/docs/cli/incremental-watermarks.md)
- [OpenAPI specification](docs/openapi/replicadb-server.json)

### Airbyte

- [Airbyte documentation](https://docs.airbyte.com/)
- [Connector catalog](https://airbyte.com/connectors)
- [Sync modes](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes)
- [Schema change management](https://docs.airbyte.com/platform/using-airbyte/schema-change-management)
- [Connector Builder](https://docs.airbyte.com/platform/connector-development/connector-builder-ui/overview)
- [Self-managed deployment](https://docs.airbyte.com/platform/deploying-airbyte)
- [Plans and capabilities](https://airbyte.com/pricing)
- [Main repository license](https://github.com/airbytehq/airbyte/blob/master/LICENSE)

### Sling

- [Sling CLI](https://github.com/slingdata-io/sling-cli)
- [Replication configuration](https://docs.slingdata.io/concepts/replication)
