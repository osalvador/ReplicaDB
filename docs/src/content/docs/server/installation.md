---
title: Server installation
description: Install local, API, and worker server profiles with durable state.
---


The managed server uses Java 17 or newer. Its distribution is separate from
the CLI and uses `REPLICADB_SERVER_HOME`; it does not migrate or read
`REPLICADB_HOME`.

## Choose the installation model

Extract the server archive and start `local` for a durable single-node control
plane. The launcher manages embedded PostgreSQL, Flyway state, Quartz jobs, the
keyring, and local execution. The first start obtains the bootstrap
administrator through environment-managed values or a hidden interactive
prompt.

Use local mode when one host owns the control plane and execution. Back up its
server home as a unit and use the URL printed by the launcher after health
passes. Do not treat a process start as successful until `status` reports it
running and the control-plane URL responds.

## Use separate API and worker roles

Use `api` for an authenticated control plane with external PostgreSQL. Use
`worker` for distributed execution. Keep worker management health private and
put TLS or an authenticated reverse proxy in front of an API exposed beyond
the host.

`local` is a launcher mode, not a suffix for `api` or `worker`. A distributed
installation starts API and worker processes separately against the same
PostgreSQL database and keyring. The browser frontend is served by API
instances; workers never serve a product UI. Follow [distributed deployment](/ReplicaDB/operations/distributed-deployment/)
for its prerequisites, startup order, health checks, and scaling boundaries.

## State boundaries

Back up the PostgreSQL data and the security keyring together. The cached
native PostgreSQL bundle can be recreated; the keyring is required to decrypt
stored datasource security values. See the operations runbooks for upgrades,
TLS, and recovery boundaries.

Before returning an installation to users, sign in with the intended
administrator, create a non-sensitive datasource profile, and complete a
manual job. A successful launch without database readiness, keyring access, or
worker execution is not a complete server validation.
