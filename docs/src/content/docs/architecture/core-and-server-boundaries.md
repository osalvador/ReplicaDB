---
title: Core and server boundaries
description: Separate standalone CLI behavior from managed server responsibilities.
---


The CLI is Spring-free and owns command parsing, options-file precedence,
connector selection, task execution, exit codes, and local logs for one
invocation. It does not require a ReplicaDB metadata database.

The managed server owns authentication, datasource snapshots, job definitions,
permissions, schedules, durable runs, attempts, leases, cancellation intent,
audit records, and frontend-safe diagnostics. The API and worker translate a
stored job into `ToolOptions`; they do not reimplement manager behavior.

This boundary explains why a server installation does not migrate CLI state and
why CLI options such as source and sink connection values are not returned by
managed frontend DTOs.