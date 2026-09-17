---
title: Local server
description: Operate the durable embedded-PostgreSQL local profile.
---


Local mode is a single-node durable deployment. The launcher manages embedded
PostgreSQL on loopback, migrations, Quartz state, the keyring, jobs, and local
execution. It does not start a separate worker.

## When to use it

Use local mode for a durable single-machine control plane, evaluation, and
development. It is not a distributed API/worker topology. For external
PostgreSQL and separate execution workers, follow [distributed deployment](/ReplicaDB/operations/distributed-deployment/).

## Prepare the server home

The default home is `${REPLICADB_SERVER_HOME:-$HOME/.replicadb}`. Keep
`data/postgresql/`, `cache/postgresql/`, `security/master-key.json`, `locks/`,
`run/`, and `logs/` together. The first administrator uses the bootstrap
environment names or a hidden interactive prompt.

Choose a server home on local durable storage with enough space for PostgreSQL,
the native bundle cache, and run logs. Do not share one local server home
between simultaneous launcher processes. Keep its keyring with the data backup:
the cache can be recreated, but a missing keyring cannot decrypt stored
datasource credentials.

## Start and verify

Run the launcher from the extracted server archive:

```bash
./bin/replicadb-server start local
./bin/replicadb-server status
```

On its first start, the launcher creates the state directories, downloads a
verified PostgreSQL bundle when it is not cached, initializes PostgreSQL, runs
migrations, and creates the administrator. It prints the control-plane URL
after a successful health check. A stopped status or a failed health check is
not a successful startup; inspect `${REPLICADB_SERVER_HOME}/logs/server.log`
before retrying.

The default API port is `8080`. Change the server port through the deployment
environment when that port is occupied, then use the URL reported by the
launcher. Keep the API loopback-only unless a trusted TLS reverse proxy is
configured.

## Stop, back up, and recover

Stop the local server cleanly before copying its data or keyring:

```bash
./bin/replicadb-server stop
./bin/replicadb-server status
```

Confirm the status is stopped before taking a backup. Restore the PostgreSQL
data and matching keyring as one set, then start the server and verify health
and login before scheduling work. Do not expose the local HTTP session endpoint
beyond the host without TLS or an authenticated proxy. See [backups and restore](/ReplicaDB/operations/backups-and-restore/),
[failure recovery](/ReplicaDB/operations/failure-recovery/), and
[troubleshooting](/ReplicaDB/operations/troubleshooting/) when a start or
restore does not complete.
