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

For example, one worker admitting four concurrent runs needs at least eight
metadata-pool connections under that rule. The extra four cover coordination
outside the run permits, including claim scans, listener activity, lease
renewal/finalization, and recovery. Size each process pool independently and
watch active/free worker slots plus connection wait time before adding runs.

The admission queue uses directed, fallback, and generic lanes with jitter,
cooldown, and adaptive backoff. It provides approximate fairness, not strict
round-robin fairness. Observe queue age, claim outcomes, busy slots, and failed
recoveries before increasing concurrency.
