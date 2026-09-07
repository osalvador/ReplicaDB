---
title: Server installation
description: Install local, API, and worker server profiles with durable state.
---


The managed server uses Java 17 or newer. Its distribution is separate from
the CLI and uses `REPLICADB_SERVER_HOME`; it does not migrate or read
`REPLICADB_HOME`.

## Local profile

Extract the server archive and start `local` for a durable single-node control
plane. The launcher manages embedded PostgreSQL, Flyway state, Quartz jobs, the
keyring, and local execution. The first start obtains the bootstrap
administrator through environment-managed values or a hidden interactive
prompt.

## API and worker profiles

Use `api` for an authenticated control plane with external PostgreSQL. Use
`worker` for distributed execution. Keep worker management health private and
put TLS or an authenticated reverse proxy in front of an API exposed beyond
the host.

## State boundaries

Back up the PostgreSQL data and the security keyring together. The cached
native PostgreSQL bundle can be recreated; the keyring is required to decrypt
stored datasource security values. See the operations runbooks for upgrades,
TLS, and recovery boundaries.