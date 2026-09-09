---
title: Core and server boundaries
description: Separate standalone CLI behavior from managed server responsibilities.
---


The CLI is Spring-free and owns command parsing, options-file precedence,
connector selection, task execution, exit codes, and local logs for one
invocation. It does not require a ReplicaDB metadata database.

## The replication core stays reusable

The CLI and managed server both delegate data movement to the same replication
core. The core receives `ToolOptions`, chooses a connector manager, and owns
the manager-specific transfer behavior. This keeps supported connectors,
replication modes, task execution, and conversion rules independent of whether
a run began from a terminal, an API request, or a schedule.

The standalone CLI has no durable control-plane contract. Each invocation owns
its own configuration and output, then exits. It cannot participate in server
scheduling, server sessions, managed permissions, or worker lease recovery.

## The server owns durable coordination

The managed server owns authentication, datasource snapshots, job definitions,
permissions, schedules, durable runs, attempts, leases, cancellation intent,
audit records, and frontend-safe diagnostics. The API and worker translate a
stored job into `ToolOptions`; they do not reimplement manager behavior.

The API owns the authenticated control plane, scheduling, and durable job
creation. A worker owns a claimed attempt only while its lease remains valid.
PostgreSQL persists the coordination state between processes and is therefore
the authority for eligibility, ownership, retries, and history. The frontend
only consumes safe API DTOs; it does not receive resolved datasource values or
lease state.

## Consequences of the boundary

This boundary explains why a server installation does not migrate CLI state and
why CLI options such as source and sink connection values are not returned by
managed frontend DTOs. It also explains why a managed retry creates a new
attempt rather than resuming a process with unknown in-memory state.

Use the [system overview](/ReplicaDB/architecture/overview/) to see the
dependency direction, [run lifecycle](/ReplicaDB/architecture/run-lifecycle/)
for durable attempts, and [security boundaries](/ReplicaDB/architecture/security-boundaries/)
for the values that cannot cross this boundary.
