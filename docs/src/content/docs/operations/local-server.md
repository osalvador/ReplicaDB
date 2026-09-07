---
title: Local server
description: Operate the durable embedded-PostgreSQL local profile.
---

# Local server

Local mode is a single-node durable deployment. The launcher manages embedded
PostgreSQL on loopback, migrations, Quartz state, the keyring, jobs, and local
execution. It does not start a separate worker.

The default home is `${REPLICADB_SERVER_HOME:-$HOME/.replicadb}`. Keep
`data/postgresql/`, `cache/postgresql/`, `security/master-key.json`, `locks/`,
`run/`, and `logs/` together. The first administrator uses the bootstrap
environment names or a hidden interactive prompt.

Use `start local`, `status`, and `stop` through the packaged launcher. Stop the
server before copying the data directory or keyring. Do not expose the local
HTTP session endpoint beyond the host without TLS or an authenticated proxy.