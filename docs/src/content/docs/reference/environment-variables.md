---
title: Environment variables
description: Managed server environment names and their ownership boundaries.
---


Use `replicadb-server.env.example` as the maintained name inventory. Never
commit resolved values.

| Variable | Purpose | Default or scope |
| --- | --- | --- |
| `SPRING_PROFILES_ACTIVE` | Selects the external `api` or `worker` Spring profile. | `local` is the `start local` launcher mode. |
| `REPLICADB_SERVER_HOME` | Durable server home. | `$HOME/.replicadb`. |
| `REPLICADB_SECURITY_KEYRING_FILE` | Canonical path to the JSON keyring file. | Server-home keyring or `/run/secrets/replicadb-master-key`; mutually exclusive with inline keyring variables. |
| `REPLICADB_SECURITY_KEYRING_CURRENT_VERSION` | Current keyring version for inline configuration. | Required with `REPLICADB_SECURITY_KEYRING_CURRENT_KEY` when using inline configuration. |
| `REPLICADB_SECURITY_KEYRING_CURRENT_KEY` | Base64-encoded 256-bit current key for inline configuration. | Required with `REPLICADB_SECURITY_KEYRING_CURRENT_VERSION`; never log or commit it. |
| `REPLICADB_SECURITY_KEYRING_SECONDARY_VERSION` | Optional second readable key version for inline rotation. | Must be paired with `REPLICADB_SECURITY_KEYRING_SECONDARY_KEY`. |
| `REPLICADB_SECURITY_KEYRING_SECONDARY_KEY` | Base64-encoded 256-bit secondary key for inline rotation. | Must be paired with `REPLICADB_SECURITY_KEYRING_SECONDARY_VERSION`. |
| `REPLICADB_SECURITY_MASTER_KEY_FILE` | Deprecated alias for `REPLICADB_SECURITY_KEYRING_FILE`. | Still accepted for compatibility; migrate deployments to the canonical name. |
| `REPLICADB_WORKER_IDENTITY` | Optional unique worker identity. | Generates `worker-<uuid>` at startup when unset; Compose sets explicit names only for operational clarity. |
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
