---
title: Environment variables
description: Managed server environment names and their ownership boundaries.
---

# Environment variables

Use `replicadb-server.env.example` as the maintained name inventory. Never
commit resolved values.

| Variable | Purpose | Default or scope |
| --- | --- | --- |
| `SPRING_PROFILES_ACTIVE` | Selects `local`, `api`, or `worker`. | Required profile choice. |
| `REPLICADB_SERVER_HOME` | Durable server home. | `$HOME/.replicadb`. |
| `REPLICADB_SECURITY_MASTER_KEY_FILE` | Keyring path. | Server-home keyring or `/run/secrets/replicadb-master-key`. |
| `REPLICADB_WORKER_IDENTITY` | Unique worker identity. | Required for workers. |
| `REPLICADB_WORKER_MANAGEMENT_PORT` | Private worker management port. | `9091`. |
| `REPLICADB_WORKER_MANAGEMENT_ADDRESS` | Worker management bind address. | `127.0.0.1`. |
| `DB_URL` | External PostgreSQL JDBC URL. | Required for `api` and `worker`. |
| `DB_USERNAME` | External PostgreSQL user. | Required for `api` and `worker`. |
| `DB_PASSWORD` | External PostgreSQL password. | Environment or secret injection only. |
| `REPLICADB_BOOTSTRAP_ADMIN_USERNAME` | First local administrator name. | First local bootstrap only. |
| `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` | First local administrator bootstrap value. | Environment or hidden prompt only. |

Worker runtime defaults such as `replicadb.worker.max-concurrent-runs`, lease
duration, heartbeat interval, polling interval, admission jitter, cooldown,
queue capacity, and adaptive backoff live in the profile YAML and are covered
by the [capacity](/ReplicaDB/operations/capacity-planning/) and
[configuration](/ReplicaDB/operations/configuration/) runbooks.