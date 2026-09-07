---
title: Operations troubleshooting
description: Distinguish liveness, readiness, notification delay, and durable failures.
---

# Operations troubleshooting

Start with `/actuator/health/liveness` and `/actuator/health/readiness`, then
inspect logs and bounded metrics. A live process is not necessarily ready for
database work.

- **Liveness up, readiness down:** inspect PostgreSQL, Quartz, queue, or worker
  admission state before restarting.
- **Listener disconnected, polling healthy:** notification latency may rise;
  polling preserves correctness. Check reconnect backoff and
  `replicadb.worker.listener.connected`. The listener retries from 1 second up
  to 30 seconds; inspect PostgreSQL `LISTEN/NOTIFY`, network, and firewall
  reachability while `replicadb.managed.polling.lag` shows fallback latency.
- **No claims:** inspect queue age, `replicadb.managed.claims`, worker identity,
  permissions, available time, and lease expiry.
- **Repeated recovery:** inspect lease renewal outcomes, database time, pool
  headroom, and retry backoff before adding workers.
- **Missing datasource values:** verify the keyring version and do not replace
  a blank edit input with an empty security value.

Keep worker management endpoints private while collecting diagnostics.
