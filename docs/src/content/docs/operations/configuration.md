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

Use the packaged profile YAML as the source of truth. Change one class of
settings at a time, record the effective profile, and restart all instances
that share a scheduler or keyring before judging the result.