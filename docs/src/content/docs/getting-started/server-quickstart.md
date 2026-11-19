---
title: Server quickstart
description: Run a managed job in local Server mode and verify the copied PostgreSQL order.
---

Use the ReplicaDB web interface to copy one order from `shop.orders` to
`reporting.orders`. Local mode saves your datasource profiles, job, and run
history in a separate embedded PostgreSQL database. You need Java 17 or
newer, Docker, and a POSIX shell.

## Prepare sample data

Follow [Sample data](/ReplicaDB/getting-started/sample-data/) to start
PostgreSQL and create both tables. If you already ran the CLI quickstart,
[reset the lab](/ReplicaDB/getting-started/sample-data/#inspect-or-reset-the-data)
so the destination is empty.

## Start local Server

Download and extract the published Server archive. Choose a fresh Server
home for this quickstart:

```bash
curl -fLO https://github.com/osalvador/ReplicaDB/releases/download/v1.0.3/ReplicaDB-server-1.0.3.tar.gz
tar -xzf ReplicaDB-server-1.0.3.tar.gz
cd ReplicaDB-server-1.0.3
export REPLICADB_SERVER_HOME="$PWD/demo-server-home"
./bin/replicadb-server start local
```

At the first start, enter an initial administrator username and password when
prompted. Open the web interface at `http://127.0.0.1:8080/` (the default
Server port) and sign in. If startup fails, check
`./bin/replicadb-server status` and
[Server installation](/ReplicaDB/server/installation/).

## Create two datasources

In **Datasources**, select **New datasource**. Create **Orders source**, then
repeat for **Reporting sink**. Set both to **PostgreSQL** and use the same
values for each connection. The job will select the tables and decide which
profile reads and which writes.

| Connection setting | Value |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `55432` |
| Database / SID or Service Name | `replicadb_demo` |
| Datasource user | `demo` |
| Datasource password | The value of `DEMO_DB_PASSWORD` from the lab setup |

Save each with **Create datasource**. See
[datasources](/ReplicaDB/server/datasources/) for details on connection
settings and access.

## Create and run a job

Open **Jobs** and select **New job**. Set these values and select **Create job**.

| Job setting | Value |
| --- | --- |
| Name | `Copy orders to reporting` |
| Mode | **Complete mode** |
| Source datasource | **Orders source** |
| Table (under Source) | `shop.orders` |
| Sink datasource | **Reporting sink** |
| Sink table | `reporting.orders` |
| Parallel tasks | `1` |

Leave **Truncate sink table** enabled for this disposable destination. On
the job page, select **Trigger run**. The run details open automatically.
Wait for the run to succeed. If the run fails, check its diagnostics and see
[runs and diagnostics](/ReplicaDB/server/runs-and-diagnostics/) and
[operations troubleshooting](/ReplicaDB/operations/troubleshooting/).

## Verify the destination

```bash
docker exec replicadb-docs-demo psql -U demo -d replicadb_demo \
  -c 'SELECT id, status, change_seq FROM reporting.orders ORDER BY id;'
```

| id | status | change_seq |
| --- | --- | --- |
| 1 | pending | 10 |

## Keep exploring or clean up

Keep Server and the sample database running to try
[replication modes](/ReplicaDB/cli/replication-modes/) or add a
[schedule](/ReplicaDB/server/schedules/). When finished, stop them:

```bash
./bin/replicadb-server stop
docker stop replicadb-docs-demo
```

Your job and run history remain in the Server home. To deploy beyond a single
host, run `api` and `worker` as separate processes. See
[distributed deployment](/ReplicaDB/operations/distributed-deployment/),
[runtime configuration](/ReplicaDB/operations/configuration/), and
[security and TLS](/ReplicaDB/operations/security-and-tls/).
