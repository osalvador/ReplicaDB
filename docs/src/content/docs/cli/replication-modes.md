---
title: Replication modes
description: Understand complete, complete-atomic, and incremental CLI behavior.
---

# Replication modes

Set `mode` in an options file or use `--mode`. The supported values are
`complete`, `complete-atomic`, and `incremental`.

## Complete

`complete` prepares the sink and inserts all rows read from the source. The
sink can be empty while the transfer is running. This is the default mode.
Use it for a disposable or replaceable sink where the straightforward bulk
path is the right tradeoff.

## Complete-atomic

`complete-atomic` stages the new data and replaces the sink contents within a
transactional flow so readers do not observe the ordinary complete-mode
truncation window. It requires staging support and enough sink capacity for
the staging table. With a fixed staging table, the sink must provide the
required permissions; a staging schema can be used for generated tables.

## Incremental

`incremental` reads a bounded source selection and merges rows into the sink.
Use a source filter or the explicit watermark options described in the
[watermark guide](/ReplicaDB/cli/incremental-watermarks/). Incremental mode
does not propagate deletes and requires a sink primary key for merge behavior.

## Cancellation and recovery

The CLI does not resume a partially completed process. A cancellation exits
with code 2; a validation or replication error exits with code 1. Inspect the
sink after interruption and choose a new complete or incremental run based on
the connector's transaction and staging behavior.