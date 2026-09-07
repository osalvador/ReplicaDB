---
title: Distributed deployment
description: Run API and worker profiles against shared external PostgreSQL.
---


Use external PostgreSQL with one or more API instances and one or more worker
instances. API nodes serve authenticated HTTP and clustered Quartz schedules;
workers claim and execute durable runs. All instances use the same database
and keyring contract.

The API listens on product port `8080`. A worker uses `server.port=-1` and
keeps management health on `127.0.0.1:9091` by default. Set a unique
`REPLICADB_WORKER_IDENTITY` for every worker and never publish its management
port.

Use `LISTEN/NOTIFY` as a wake-up path, not a correctness dependency. Workers
retain polling for claims, cancellation, and recovery. Keep API scheduler
ownership on JDBC Quartz; mixed RAM/JDBC ownership is prohibited.