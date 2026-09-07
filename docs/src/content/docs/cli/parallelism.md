---
title: CLI parallelism
description: Tune jobs, fetch size, and bandwidth for standalone transfers.
---

# CLI parallelism

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

## A tuning sequence

1. Start with one job and a representative table.
2. Measure source query time, sink write time, memory, and network use.
3. Increase `jobs` gradually and check database connection limits.
4. Adjust `fetch.size` only after confirming the reader is the bottleneck.
5. Add bandwidth throttling when the transfer must yield to other traffic.

The standalone CLI runs all entries in a multi-table catalog sequentially;
`jobs` controls work within the current table rather than table-level
parallelism.