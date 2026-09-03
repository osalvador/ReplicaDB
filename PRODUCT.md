# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

ReplicaDB serves engineering users across data and platform engineering,
database administration and operations, and development or integration work.
They need to move or synchronize data between heterogeneous systems, either by
automating standalone command-line workflows or by configuring and operating
managed replication jobs.

The priority among these engineering audiences is intentionally undecided.

## Product Purpose

ReplicaDB enables engineers to perform high-performance bulk data replication
between heterogeneous databases and other supported data stores. The
standalone CLI supports repeatable transfer workflows, while the managed
server adds authenticated control-plane operation for datasource profiles,
durable jobs, scheduling, execution, and run diagnostics.

Success means engineers can move or synchronize large datasets efficiently,
reliably, and with operational visibility without installing agents or
database triggers in source systems.

## Positioning

ReplicaDB is an open-source, non-intrusive replication tool that combines
parallel bulk transfer with broad heterogeneous source and sink support. Its
core mechanism does not require database agents or source triggers. The
managed server extends that mechanism with authenticated, durable scheduling
and operational control rather than replacing the standalone CLI workflow.

## Operating Context

Standalone workflows run from Windows, Linux, or macOS environments using Java
17 or newer, command-line options, or configuration files. Managed workflows
use an authenticated web control plane backed by PostgreSQL metadata and can
be executed by local or distributed worker processes. Engineers configure
datasource profiles, define jobs, run them manually or on a schedule, inspect
run history and diagnostics, and use administrator or resource-level
permissions to control access.

## Capabilities and Constraints

- Supports relational databases, MongoDB, CSV, Amazon S3, Kafka, and other
  JDBC-compliant sources or sinks, subject to connector limitations.
- Supports complete and incremental replication modes, parallel task
  execution, retries, scheduling, and durable run state in the managed
  server.
- The standalone CLI remains cross-platform, Spring-free, and compatible with
  its existing options-file contract.
- Bulk transfer must remain non-intrusive: source agents and database
  triggers are not required.
- Managed datasource credentials and other sensitive connector values are
  encrypted before persistence and redacted from frontend-facing responses
  and operational diagnostics.
- Managed access includes administrator controls plus datasource and job
  permissions.
- The managed control plane is a web product; the standalone CLI is a
  cross-platform companion surface rather than a native mobile product.

## Evidence on Hand

- `README.md` documents the product purpose, supported connectors, CLI
  workflows, installation, examples, and comparison with alternatives.
- `DEPLOYMENT.md` documents the managed API and worker topology, security
  requirements, durable scheduling, and operational constraints.
- `replicadb-server/frontend/README.develop.md` documents the local web
  control plane and its datasource, job, and run workflows.
- `docs/docs/media/replicadb-logo.png` is the existing logo asset.

No customer testimonials, performance benchmarks, pricing, or deployment
claims beyond the repository documentation are established here; future work
must not fabricate them.

## Product Principles

- Keep replication non-intrusive and easy to introduce into existing systems.
- Make heterogeneous data movement practical through broad connector support.
- Use parallel execution to make bulk transfer efficient at scale.
- Protect credentials and operational data throughout configuration and
  execution.
- Give engineers durable, observable, permissioned control over managed runs.

## Accessibility & Inclusion

The existing web frontend requires semantic accessible roles and visible focus
states in its test suite. No additional product-specific accessibility
standard has been confirmed; this remains an open decision for future product
work.
