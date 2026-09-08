---
title: Runtime configuration
description: Configure profiles, pools, sessions, worker admission, and logging.
---


`local` is a launcher mode, invoked with `./bin/replicadb-server start local`.
The launcher starts the `api` profile with embedded PostgreSQL and local
execution. For an external deployment, use the `api` or `worker` Spring
profile; both require `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`, and the
master-key file. The full environment inventory is in the [environment reference](/ReplicaDB/reference/environment-variables/).

## Establish the configuration boundary

Keep database connection values, bootstrap values, port bindings, and keyring
paths in the deployment environment or secret manager. Do not commit resolved
values. All API and worker instances in one distributed cluster use the same
database and keyring, while each worker has a distinct
`REPLICADB_WORKER_IDENTITY`.

Start configuration from the packaged profile YAML and change one setting
class at a time. Record the effective values, deploy them consistently to the
affected role, then restart that role and verify readiness. Do not use
`start api local`: `api`, `worker`, and `local` describe mutually exclusive
launcher modes. See [distributed deployment](/ReplicaDB/operations/distributed-deployment/)
for the role topology.

## Configure API instances

An API instance serves the control plane, authenticated API, sessions, and
clustered JDBC Quartz scheduler. When a separate worker fleet executes runs,
set `REPLICADB_SERVER_LOCAL_EXECUTION_ENABLED=false`. Every API in that fleet
must use the same scheduler and database configuration; never mix a RAM Quartz
store with the JDBC store. Verify API readiness after any scheduler or database
change before returning it to traffic.

## Configure worker instances

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

## Validate and roll back

Use liveness to confirm a process exists and readiness to confirm it can use
its database and role runtime. Validate one manual run after a worker,
keyring, pool, or admission change before increasing concurrency. If a change
causes readiness failures, restore the last known-good environment values,
restart only the affected role, and inspect its logs and metrics before making
another adjustment. The [health and metrics](/ReplicaDB/operations/health-and-metrics/)
and [troubleshooting](/ReplicaDB/operations/troubleshooting/) runbooks define
the observable checks.
