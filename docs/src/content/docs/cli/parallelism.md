---
title: CLI parallelism
description: Tune jobs, fetch size, and bandwidth for standalone transfers.
---


Use `--jobs` or the `jobs` property to control the number of source-to-sink
workers. The command-line help reports the default as 4; set a value that the
source, sink, network, and host can sustain rather than maximizing the number
blindly.

`--fetch-size` or `fetch.size` controls how many rows a reader requests at
once. Larger values can improve throughput while increasing memory pressure.
The default options file uses 100 as a starting point.

`--bandwidth-throttling` or `bandwidth.throttling` applies a transfer cap in
KB/s. It is useful when replication shares a constrained network, but it does
not change database lock, query, or sink commit behavior.

The limit is applied per worker. For example, four workers each capped at
10,240 KB/s can collectively approach 40,960 KB/s when the source, sink, and
network can sustain it. Use a per-worker value derived from the total budget,
and remember that connectors without throttling support ignore this tuning
path as documented on their connector page.

## A tuning sequence

1. Start with one job and a representative table.
2. Measure source query time, sink write time, memory, and network use.
3. Increase `jobs` gradually and check database connection limits.
4. Adjust `fetch.size` only after confirming the reader is the bottleneck.
5. Add bandwidth throttling when the transfer must yield to other traffic.

The standalone CLI runs all entries in a multi-table catalog sequentially;
`jobs` controls work within the current table rather than table-level
parallelism.

Parallel reads also increase active source and sink connections. Stop raising
`jobs` when throughput flattens, database waits rise, or the host approaches
its memory or file-descriptor limits. A larger worker count is not a substitute
for a selective source predicate or a suitable sink key/index strategy.
