---
title: Capacity planning
description: Plan worker concurrency, database pools, and effective throughput.
---

# Capacity planning

The arithmetic ceiling is `worker instances * concurrent runs per worker * jobs
per run`. Bind it to `replicadb.worker.max-concurrent-runs`, the count of
worker processes, and each job's `jobs` setting.

Reserve datasource pool headroom for claim, heartbeat, recovery, and API work:
`datasourcePoolSize >= max-concurrent-runs + 4`. Then check source sessions,
sink locks, staging space, network bandwidth, host memory, and PostgreSQL
connections. Any of these can reduce effective throughput below the arithmetic
maximum.

The admission queue uses directed, fallback, and generic lanes with jitter,
cooldown, and adaptive backoff. It provides approximate fairness, not strict
round-robin fairness. Observe queue age, claim outcomes, busy slots, and failed
recoveries before increasing concurrency.