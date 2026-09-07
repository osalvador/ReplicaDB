---
title: CLI guide
description: Task-oriented reference for the standalone ReplicaDB command-line tool.
---

# CLI guide

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

## Scope and limits

The CLI is a batch replication tool. It does not provide change-data-capture,
durable server-side scheduling, remote run cancellation, or automatic resume
after a process is interrupted. Use the managed server when those operations
need durable shared state.