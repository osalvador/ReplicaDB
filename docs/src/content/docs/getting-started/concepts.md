---
title: Core concepts
description: Understand ReplicaDB's replication model, durable work, and control boundaries.
---

ReplicaDB is a batch replication tool. It moves a selected set of rows from a
source to a sink without requiring agents, triggers, or change-data-capture
installation on the source. The standalone CLI runs one direct transfer. The
managed server adds durable definitions, access control, schedules, attempts,
and diagnostics around the same replication core.

This page explains the model before you choose a command or create a managed
job. Each concept links to the guide that defines its exact options, connector
limits, or operational procedure.

## Replication model

## Source

The source is the system, table, query, or file that provides rows to a
replication. A source can be a relational database, a file-oriented connector,
object storage, a stream-oriented destination supported as an input, or a
generic JDBC endpoint with its documented constraints.

Most transfers select a source table. A source query is useful when the read
must include a stable filter, projection, or join that belongs to the transfer
definition. ReplicaDB reads from the source; it does not infer business changes
from a transaction log. The source account needs the permissions required by
the connector and selected query, typically read access for a relational table.

See [filtering and queries](/ReplicaDB/cli/filtering-and-queries/) for table
selection, query boundaries, and incremental-filter limitations.

## Sink

The sink is the system, table, or storage location that receives replicated
rows. Its schema, write permissions, locks, storage, and connector capability
determine which transfer modes are safe and how much parallel work it can
accept. ReplicaDB does not make unlike schemas equivalent: validate data types,
keys, constraints, and the destination table before enabling production runs.

An interrupted sink is not always reversible. `complete` can leave a target
empty or partially loaded; `complete-atomic` and `incremental` have different
safety properties. Choose the mode from the sink's required visibility and
recovery behavior, not only from transfer speed.

See [connector support](/ReplicaDB/connectors/) for connector-specific limits
and [replication modes](/ReplicaDB/cli/replication-modes/) for sink behavior.

## Replication modes

Every run has one mode. The mode defines how ReplicaDB reads the selected rows,
writes them to the sink, and behaves when a run is interrupted or retried.

| Mode | Intended result | Interruption and retry boundary |
| --- | --- | --- |
| `complete` | Replace the sink contents with a full source load. | Destructive: the sink can be empty, truncated, or partially populated after interruption. |
| `complete-atomic` | Stage a new full load, then replace the sink in a final operation. | The ordinary sink remains visible until the final replacement when the connector supports the staging contract. |
| `incremental` | Read a bounded source selection and merge it into the sink. | A successful run commits its next boundary; failed or cancelled work keeps the prior committed boundary. |

`complete-atomic` is usually the better managed choice when users must not see
the ordinary sink during a replacement. `incremental` is appropriate when the
source can identify a bounded set of changed candidate rows and the sink can
apply its merge semantics. A mode is supported only where the selected
connector advertises it.

See [replication modes](/ReplicaDB/cli/replication-modes/) for the detailed
flows and [failure recovery](/ReplicaDB/operations/failure-recovery/) before
accepting a retry or cancellation risk.

## Units of work

## Job

A managed-server definition that combines source and sink datasources with
replication settings, filters, retries, and scheduling choices. A managed job
models one source table or query and one sink table. It stores references to
datasource profiles rather than copying connection security into the job.

Editing a job changes future claims. A worker that already owns an attempt
keeps the inputs recorded when it claimed that attempt. This separates a
repeatable historical execution from a later change to the definition.

## Task

A task is a unit of work inside a run that can be processed with configured
parallelism. Depending on the connector and job shape, it may represent a
source partition or a source-to-sink pair. Tasks are an implementation unit;
they are not independent managed jobs and do not carry their own user-facing
schedule, permission, or retry history.

Increasing task parallelism can improve throughput, but it also increases
source sessions, sink writers, locks, memory use, and network pressure. It is
not a guarantee of a faster transfer when the source or sink is the limiting
resource.

## Run

One execution of a managed job or one CLI invocation. A run has an outcome,
diagnostics, and, in the server, durable state. The CLI reports its process
outcome to the caller. The server persists a run so operators can inspect its
status, timestamps, row counts, warning, diagnostics, and retry eligibility.

For a managed job, a manual trigger and a schedule both create durable work.
Neither bypasses datasource bindings, permissions, or the ownership rules that
prevent overlapping executable work.

## Attempt

An attempt is one try to execute a managed run. A failed or expired attempt is
not reset in place. Retry creates a new pending attempt linked to its previous
attempt, which preserves diagnostics and the outcome chain. The new attempt
starts from the beginning and resolves current eligible datasource profiles;
it is not a checkpoint or resume mechanism.

The server records states such as `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`,
`CANCEL_REQUESTED`, `CANCELLED`, and `RETRY_SCHEDULED`. A terminal result does
not prove that every external sink write can be undone. Inspect the mode warning
and the old attempt before retrying.

See [run lifecycle](/ReplicaDB/architecture/run-lifecycle/) for legal state
transitions and [runs and diagnostics](/ReplicaDB/server/runs-and-diagnostics/)
for the managed interface.

## Configuration and bounded reads

The CLI accepts long options and Java-properties options files. It loads an
options file first, then applies explicit command-line values as overrides.
Use an options file for repeatable non-secret settings, keep changing values in
the calling environment, and validate the resolved configuration before a
large transfer.

Managed jobs present the corresponding replication choices through the API and
browser control plane. They preserve datasource references and settings for
future runs, while connection security remains in the datasource profile.

## Parallelism and batching

Parallelism and batching solve different problems. `jobs` controls how much
work can execute concurrently inside a run. Fetch size controls how many rows
the source driver requests in a read batch. Bandwidth throttling limits transfer
rate rather than the number of tasks. Raising all three at once makes it hard
to identify which source, sink, database pool, or network boundary is limiting
the run.

Start with the connector defaults, measure source and sink pressure, then
adjust one class of setting at a time. See [CLI parallelism](/ReplicaDB/cli/parallelism/)
and [capacity planning](/ReplicaDB/operations/capacity-planning/) for the
different CLI and worker-capacity controls.

## Datasource

A datasource is a managed-server connection profile. It stores reusable
connector settings and protects security values before persistence. API and
browser responses expose safe connection displays and capabilities, never the
resolved values used during execution.

Datasource permissions are separate from global roles. `VIEW` exposes safe
metadata, `USE` permits binding a profile into a job, and `EDIT` permits
profile changes. A visible button is not authorization; backend checks remain
the authority. Blank security fields on edit preserve an existing encrypted
value until an explicit key-clear action is requested.

See [datasources](/ReplicaDB/server/datasources/) and
[permissions](/ReplicaDB/server/permissions/) for profile lifecycle and
access rules.

## Watermark

A watermark is a source-column value used to bound a later incremental read.
It is an explicit incremental-mode contract, not an inferred CDC cursor. The
source selection can also be bounded by an explicit filter; a watermark is used
when the caller or managed job needs ReplicaDB to track the last successfully
observed value.

ReplicaDB uses the prior successful value as the lower boundary for the next
eligible read. The boundary advances only after successful finalization. A
failed, cancelled, or expired attempt preserves the previous committed
watermark even if the sink may already contain some writes. This protects the
next retry from silently skipping unconfirmed work, but it does not propagate
deletes or provide multi-column checkpointing.

The standalone CLI reports a successful candidate for its caller to persist.
The managed server stores the committed watermark with the durable run state.
See [incremental watermarks](/ReplicaDB/cli/incremental-watermarks/) for
column, query, and orchestration limits.

## Managed control plane

## API

The authenticated HTTP interface used by the managed frontend and automation.
It creates and reads managed resources, returns paginated resource data and
structured errors, and enforces roles and resource permissions. The browser
frontend is served with the API control plane; it is a usability layer, not the
authorization boundary.

The API has session and CSRF protections and deliberately excludes lease
tokens, resolved datasource security, and unsafe operational detail. It is not
part of the standalone CLI contract, which can run under the process owner's
existing scheduler and secret-management workflow.

## Worker

A managed-server process that executes claimed work. Workers renew leases and
are fenced from finalizing work after ownership expires. In a distributed
deployment, an API instance persists a pending run and worker instances claim
eligible work from shared PostgreSQL. A notification can reduce dispatch
latency, but polling preserves correctness when notifications are missed.

The worker's lease token is an internal ownership capability. If a worker loses
or outlives its lease, token-checked updates reject its late progress or
terminal result. Recovery can create a new attempt; it never asks a worker to
resume unknown in-memory transfer progress. Workers do not serve the browser
frontend or public API.

See [distributed topology](/ReplicaDB/architecture/distributed-topology/) and
[distributed deployment](/ReplicaDB/operations/distributed-deployment/) for
the architecture and operational roles.

## Schedules and retries

A schedule is durable managed intent: a CRON expression, time zone, and enabled
state. API instances reconcile enabled schedules into clustered JDBC Quartz;
a scheduled fire still creates the same pending run and follows the same claim
and permission boundary as a manual trigger.

A retry policy limits total attempts and sets its backoff. Safe modes can
automatically recover an expired lease according to that policy, while
destructive `complete` requires deliberate retry acceptance. Scheduling and
retry do not turn a batch transfer into continuous CDC and do not promise that
partial external writes can be rolled back.

See [schedules](/ReplicaDB/server/schedules/),
[scheduling and high availability](/ReplicaDB/architecture/scheduling-and-ha/),
and [failure recovery](/ReplicaDB/operations/failure-recovery/).

## CLI and managed server

| Concern | Standalone CLI | Managed server |
| --- | --- | --- |
| Invocation | Process owner starts one direct transfer. | API or schedule creates a durable job run. |
| State | Caller owns logs, exit status, and any persisted watermark. | PostgreSQL stores definitions, attempts, schedules, permissions, and history. |
| Scheduling | External scheduler owned by the caller. | Clustered Quartz reconciled from durable schedule intent. |
| Retry | Caller starts a new invocation. | Manual or policy-driven attempt lineage with backoff. |
| Access | The process owner's environment and database permissions. | Authenticated roles, resource permissions, sessions, and audit history. |
| Scale | Process and connector parallelism. | API control plane plus independently scaled workers. |

Choose the model based on who must own repeatability, access, scheduling, and
recovery state. The products share the same replication core but do not migrate
state into each other. Continue with [Choose CLI or Server](/ReplicaDB/getting-started/choose-cli-or-server/)
for the decision path.
