---
title: Health and metrics
description: Probe liveness, readiness, actuator metrics, and bounded runtime families.
---

# Health and metrics

The API exposes unauthenticated `/actuator/health`,
`/actuator/health/liveness`, and `/actuator/health/readiness` on port 8080.
`/actuator/metrics` and `/actuator/prometheus` require authentication. Worker
versions expose the same paths only on private management port 9091.

Liveness answers whether the process is alive. Readiness includes PostgreSQL,
Quartz, queue, and worker-runtime conditions. A worker can be `DEGRADED` when
its notification listener is disconnected but polling and admission remain
healthy; a listener delay affects latency, not durable correctness.

## Bounded metric families

Interpret `replicadb.managed.claims`, `replicadb.managed.dispatches`,
`replicadb.managed.notifications`, `replicadb.managed.polling.scans`,
`replicadb.managed.lease.renewals`, `replicadb.managed.lease.recoveries`,
`replicadb.managed.retries`, `replicadb.managed.fenced.updates`,
`replicadb.managed.cancellations`, and `replicadb.managed.terminal.outcomes`
for control-plane behavior. Use `replicadb.worker.admission.events` and
`replicadb.worker.completed.runs` for worker outcomes. Timers include
`replicadb.managed.notification.claim.latency`,
`replicadb.managed.polling.lag`, and worker busy-slot seconds. Gauges include
active/free slots, listener connectivity, and polling state.

Metric tags are bounded and never contain job IDs, run IDs, usernames,
credentials, or lease tokens.