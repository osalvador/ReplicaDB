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

## Separate capacity from ownership

Adding an API improves control-plane availability; adding a worker adds claim
and execution capacity. Neither changes PostgreSQL's role as the ownership
arbiter. A worker's run permits limit its local execution, while each run's
`jobs` setting controls parallel work inside that one replication attempt.
These limits compose, but they are not interchangeable.

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

## Approximate fairness by design

The queue seeks to avoid a noisy worker claiming every run while preserving
fast directed wake-ups. It does not promise strict round-robin allocation or
an equal count of runs when worker capacities differ. Compare normalized
busy-slot time, queue age, claim outcomes, and recovery rates when evaluating
a fleet. A missed notification or full directed queue remains recoverable
because polling refills generic claim opportunities.

This architecture describes admission behavior; [capacity planning](/ReplicaDB/operations/capacity-planning/)
describes how to measure and change limits safely.
