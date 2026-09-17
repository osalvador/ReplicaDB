---
title: CLI guide
description: Task-oriented reference for the standalone ReplicaDB command-line tool.
---


The standalone CLI performs direct bulk replication from one source to one
sink, or through a sequential multi-table catalog. It is Spring-free, runs on
Java 17 or newer, and does not require a ReplicaDB metadata database.

## Follow a workflow

- [Install the CLI](/ReplicaDB/cli/installation/).
- [Configure an options file](/ReplicaDB/cli/configuration/).
- [Choose a replication mode](/ReplicaDB/cli/replication-modes/).
- [Tune parallelism](/ReplicaDB/cli/parallelism/).
- [Filter rows and queries](/ReplicaDB/cli/filtering-and-queries/).
- [Run multiple tables](/ReplicaDB/cli/multi-table/).
- [Use incremental watermarks](/ReplicaDB/cli/incremental-watermarks/).
- [Troubleshoot a run](/ReplicaDB/cli/troubleshooting/).

The [CLI option reference](/ReplicaDB/reference/cli-options/) is generated
from the maintained option evidence and preserves exact flag names. The
[options-file examples](/ReplicaDB/reference/example-options-files/) show
safe environment substitution without resolving secrets into source control.

## What one invocation does

1. Parse the options file, then apply command-line overrides.
2. Validate the source, sink, mode, table selection, and staging constraints.
3. Open connector managers and run their pre-transfer preparation.
4. Partition the current source table across `jobs` workers and write rows to
	 the sink or its staging area.
5. Run the mode-specific sink operation, cleanup, and connector shutdown.

A multi-table catalog repeats that complete lifecycle for each table pair in
numeric order. It does not turn one invocation into a durable workflow: keep
the options, exit code, logs, destination checks, and last committed watermark
with the external scheduler or run record that launched it.

## Choose the next guide

- Start with `complete` when replacing the destination is acceptable.
- Choose `complete-atomic` when readers must not see the ordinary replacement
	window and the sink supports transactional staging.
- Choose `incremental` when a source predicate or committed watermark can
	identify changed rows and the sink has the keys required to merge them.
- Use `source.query` for a result set that cannot be expressed as one table,
	but accept that automatic watermark and multi-table features are unavailable.

## Scope and limits

The CLI is a batch replication tool. It does not provide change-data-capture,
durable server-side scheduling, remote run cancellation, or automatic resume
after a process is interrupted. Use the managed server when those operations
need durable shared state.
