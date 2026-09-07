---
title: Health and metrics
description: Probe liveness, readiness, actuator metrics, and bounded runtime families.
---


The API exposes unauthenticated `/actuator/health`,
`/actuator/health/liveness`, and `/actuator/health/readiness` on port 8080.
`/actuator/metrics` and `/actuator/prometheus` require authentication. Worker
versions expose the same paths only on private management port 9091.

Liveness answers whether the process is alive. Readiness includes PostgreSQL,
Quartz, queue, and worker-runtime conditions. A worker can be `DEGRADED` when
its notification listener is disconnected but polling and admission remain
healthy; a listener delay affects latency, not durable correctness.

Polling is the correctness path and notifications are a latency optimization.
When `replicadb.worker.listener.connected` is 0, correlate
`replicadb.managed.polling.lag` with claim latency and inspect listener
reconnects before restarting healthy workers.

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

## Bounded run logs

Run output is credential-redacted before it enters the in-memory capture and
again before persistence. Captured content is limited to 256 KiB. When it
exceeds that bound, ReplicaDB keeps the first 75% and last 25% of the available
bytes with `[TRUNCATED: middle omitted]` between them. Treat the result as
sensitive operational data even after redaction, and correlate the
`truncated` and captured-size fields before concluding that a diagnostic line
was never emitted.
