---
title: Runtime configuration
description: Configure profiles, pools, sessions, worker admission, and logging.
---

# Runtime configuration

Set `SPRING_PROFILES_ACTIVE` to `local`, `api`, or `worker`. External profiles
require `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`, and the master-key file. The
full environment inventory is in the [environment reference](/ReplicaDB/reference/environment-variables/).

Worker defaults are deliberately small: `max-concurrent-runs: 1`,
`lease-duration: 5m`, `heartbeat-interval: 30s`, `poll-interval: 30s`, and
`poll-batch-size: 100`. Pool headroom must satisfy
`datasourcePoolSize >= max-concurrent-runs + 4`.

## Admission tuning

The worker's directed queue defaults to 1,024 entries. Admission adds up to
100 ms of jitter and a 250 ms generic cooldown; adaptive contention backoff
starts at 25 ms, caps at 2 s, and decays over a 30 s half-life. Tune
`replicadb.worker.admission.jitter-max`, `generic-cooldown`,
`directed-queue-capacity`, and the `adaptive-backoff` properties together.
Shorter delays reduce idle dispatch latency but increase claim contention.

## Datasource resolution

Pending runs reference job bindings, not frozen plaintext connections. At
claim time, the worker locks the binding and datasource rows in UUID order,
requires both bindings to remain enabled, and records encrypted datasource
snapshots for that attempt. A profile update affects only a later claim. A
retry is a new attempt and resolves the current profiles again.

Use the packaged profile YAML as the source of truth. Change one class of
settings at a time, record the effective profile, and restart all instances
that share a scheduler or keyring before judging the result.
