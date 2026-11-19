---
title: Core concepts
description: Understand the terms used in the ReplicaDB quickstarts and replication guide.
---

ReplicaDB reads a selected set of rows from a source and writes them to a sink
in one batch replication run. In both quickstarts the source is
`shop.orders` and the sink is `reporting.orders`.

## Source

The source provides the rows to copy. It can be a database table, a query, a
file, or another readable endpoint. Use a source table for a straightforward
copy. For supported connectors, a filter limits the rows, a projection
selects columns, and a query can join or transform data before it is read.
See [filtering and queries](/ReplicaDB/cli/filtering-and-queries/).

## Sink

The sink receives the rows. For a database, check the destination schema,
column order and types, permissions, and any key the selected mode uses.
The [connector matrix](/ReplicaDB/connectors/) lists which systems can be a
source, a sink, or both and what each role supports.

## Mode

The mode determines whether a run replaces the destination directly, stages
a replacement, or applies a set of new and changed rows. The
[replication mode guide](/ReplicaDB/cli/replication-modes/) follows the same
orders through all three flows.

## Datasource

A datasource is a named, reusable Server connection profile. It identifies the
connector and connection settings. A job can reference that profile, while
the profile retains its protected credentials. The CLI takes its connection
settings directly from the invocation. See
[datasources](/ReplicaDB/server/datasources/).

## Job and run

A job is a saved Server definition. It specifies the source, sink, mode,
and execution settings. A run is one execution of a job, started manually
or by a schedule. Server records its outcome and diagnostics. In the CLI,
one process invocation runs the configured transfer and returns an exit code.

Parallel tasks are units of work within one transfer. For example, tasks can
read partitions of one source table concurrently. A Server worker is the
process that executes a managed run. See
[concurrency within a CLI run](/ReplicaDB/cli/parallelism/) and the
[Server run lifecycle](/ReplicaDB/architecture/run-lifecycle/) for tasks,
attempts, retries, workers, and lease ownership.

## Watermark

A watermark is a value from a source column used as the lower bound of the
next incremental read. For example, passing `10` as the last value reads
rows where `change_seq > 10`. The caller supplies the CLI's lower bound.
Server can persist the value and advance it after a successful run.
See [incremental watermarks](/ReplicaDB/cli/incremental-watermarks/).
