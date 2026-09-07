---
title: CLI performance
description: Diagnose and tune throughput without changing the CLI contract.
---

# CLI performance

Throughput is bounded by the slowest source read, sink write, network path,
connector staging path, and host resources. Start with a representative table
and keep the options-file and driver versions fixed while comparing runs.

## Measure before changing settings

Record row counts, elapsed time, source query duration, sink commit duration,
host memory, active database sessions, and network throughput. Use
`--verbose` for additional process detail and keep the output with the run
record owned by your scheduler.

## Change one control at a time

Tune `jobs`, `fetch.size`, and `bandwidth.throttling` in that order. More jobs
can increase source locks, sink contention, and connection usage. More fetch
rows can increase memory without improving a database-bound query. A bandwidth
cap is appropriate for fairness but cannot fix a slow source plan.

For complete-atomic mode, include staging writes and the final replacement in
the measurement. For incremental mode, include the cost of the source filter
and sink merge rather than comparing only raw row throughput.