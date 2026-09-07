---
title: Server quickstart
description: Start a durable local ReplicaDB server with embedded PostgreSQL.
---


The local server is the smallest managed deployment. It provides an
authenticated web control plane, durable metadata, jobs, schedules, and run
diagnostics while managing its PostgreSQL process locally.

## Install and start local mode

The server needs Java 17 or newer. Extract the server archive and keep its home
separate from any CLI installation:

```bash
curl -fL -o ReplicaDB-server-1.0.0.tar.gz "https://github.com/osalvador/ReplicaDB/releases/download/v1.0.0/ReplicaDB-server-1.0.0.tar.gz"
tar -xzf ReplicaDB-server-1.0.0.tar.gz
cd ReplicaDB-server-1.0.0
export REPLICADB_SERVER_HOME="${HOME}/.replicadb"
./bin/replicadb-server start local
./bin/replicadb-server status
```

On first start, the launcher creates the local state and asks for the initial
administrator through a hidden prompt when bootstrap values are not supplied.
Keep the server home and its keyring together when backing up the installation.

## Sign in and create the first job

Open the API URL shown by the launcher, sign in with the administrator created
during startup, and create a datasource before creating a job. A job refers to
datasources and replication settings; it does not copy connection secrets into
the browser response.

Start with a manual run, inspect its diagnostics, and only then add a
schedule. Stop the server cleanly after a local test:

```bash
./bin/replicadb-server status
./bin/replicadb-server stop
```

## Keep the boundary clear

Local mode is a durable single-node deployment. Use the `api` and `worker`
profiles with external PostgreSQL when execution must scale beyond one host.
The worker management endpoint remains private and is not a public UI.

Continue with [server installation](/ReplicaDB/server/installation/),
[server configuration](/ReplicaDB/operations/configuration/),
[server troubleshooting](/ReplicaDB/operations/troubleshooting/), and
[security and TLS](/ReplicaDB/operations/security-and-tls/).