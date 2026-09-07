---
title: Scaling and fairness
description: Calculate capacity and understand the worker admission lanes.
---


The arithmetic capacity ceiling is:

`worker instances * concurrent runs per worker * jobs per run`

Bind those terms to the runtime model:

- **worker instances** is the number of API/worker execution processes;
- **concurrent runs per worker** is `replicadb.worker.max-concurrent-runs`
  (default `1`); and
- **jobs per run** is the job's core `jobs` option, which controls work inside
  one replication run.

The result is a ceiling, not a throughput promise. Datasource connection
pools, source and sink limits, database locks, network bandwidth, staging
capacity, and host memory can reduce effective throughput below the arithmetic
maximum.

`WorkerAdmissionQueue` uses `DIRECTED`, `FALLBACK`, and `GENERIC` lanes.
Directed signals preserve FIFO order; generic refills use cooldown, jitter,
and contention backoff. This is approximate fairness, not round-robin
fairness. Duplicate signals can coalesce and a full directed queue can drop a
wake-up because polling remains the correctness path.

A `DIRECTED` item attempts the signalled run. If it misses, one `FALLBACK`
attempt looks for other eligible work before normal `GENERIC` polling refills
free slots. The defaults add up to 100 ms of jitter, a 250 ms generic cooldown,
and adaptive contention backoff from 25 ms to 2 s with a 30 s decay half-life.
These delays schedule claim attempts; they do not hold a worker permit while
waiting.
