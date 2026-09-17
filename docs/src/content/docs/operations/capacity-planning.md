---
title: Capacity planning
description: Plan worker concurrency, database pools, and effective throughput.
---


The arithmetic ceiling is `worker instances * concurrent runs per worker * jobs
per run`. Bind it to `replicadb.worker.max-concurrent-runs`, the count of
worker processes, and each job's `jobs` setting.

Reserve datasource pool headroom for claim, heartbeat, recovery, and API work:
`datasourcePoolSize >= max-concurrent-runs + 4`. Then check source sessions,
sink locks, staging space, network bandwidth, host memory, and PostgreSQL
connections. Any of these can reduce effective throughput below the arithmetic
maximum.

## Establish a baseline

Start with one concurrent run per worker and measure source sessions, sink
locks, staging space, network bandwidth, host memory, and PostgreSQL
connections for a representative job. Increase one capacity dimension at a
time, run a manual workload, and compare throughput, queue age, polling lag,
and failed claims before the next change.

For example, one worker admitting four concurrent runs needs at least eight
metadata-pool connections under that rule. The extra four cover coordination
outside the run permits, including claim scans, listener activity, lease
renewal/finalization, and recovery. Size each process pool independently and
watch active/free worker slots plus connection wait time before adding runs.

The admission queue uses directed, fallback, and generic lanes with jitter,
cooldown, and adaptive backoff. It provides approximate fairness, not strict
round-robin fairness. Observe queue age, claim outcomes, busy slots, and failed
recoveries before increasing concurrency.

## Scale and verify

Add workers with unique identities when execution is saturated; add APIs for
control-plane availability. Compare normalized busy-slot time rather than raw
run counts when workers have different capacities. Roll back the last capacity
increase when connection waits, lease failures, sink contention, or recovery
rates rise. Use [health and metrics](/ReplicaDB/operations/health-and-metrics/)
to validate each adjustment.
