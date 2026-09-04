---
layout: page
title: Server
permalink: /server.html
---

# ReplicaDB Server 0.19.0

The server is the managed ReplicaDB product. The standalone CLI remains a
separate download and keeps its `REPLICADB_HOME` contract.

## Choose a distribution

| Use | Artifact | PostgreSQL |
| --- | --- | --- |
| Direct transfer | `ReplicaDB-0.19.0.tar.gz` or `.zip` | None required |
| Durable local server | `ReplicaDB-server-0.19.0.tar.gz` or `.zip` | Downloaded and verified on first local start |
| Advanced server or Docker | `replicadb-server-0.19.0.jar` | External or embedded according to the launch contract |

All server package launchers require Java 17. The package does not require
Maven, npm, Docker, or a system PostgreSQL installation.

## Local installation

Extract the server package and use the launcher. The mode is always explicit:

```bash
tar -xzf ReplicaDB-server-0.19.0.tar.gz
cd ReplicaDB-server-0.19.0
export REPLICADB_SERVER_HOME="${REPLICADB_SERVER_HOME:-$HOME/.replicadb}"
export REPLICADB_BOOTSTRAP_ADMIN_USERNAME='<local-admin>'
export REPLICADB_BOOTSTRAP_ADMIN_PASSWORD='<managed-bootstrap-password>'
./bin/replicadb-server start local
./bin/replicadb-server status
./bin/replicadb-server stop
```

On the first local start, the server downloads the platform PostgreSQL bundle
from Maven Central and verifies its SHA-256 value before caching it. A warm
cache allows restart without network access. The supported release matrix is
macOS ARM64 and x64, Linux x64, and Windows x64. Unsupported platforms fail
before Spring starts.

The default persistent home is `~/.replicadb`. Set
`REPLICADB_SERVER_HOME` to use another location. The server does not read
`REPLICADB_HOME` as a fallback because that variable belongs to the CLI. A
legacy experimental server home under `REPLICADB_HOME` is not migrated
automatically.

```text
REPLICADB_SERVER_HOME/
  data/postgresql/       PostgreSQL metadata and job history
  cache/postgresql/      verified native bundle containers
  security/master-key.json
  locks/
  run/                   PID, mode, and startup lock
  logs/                  server.log and server.log.1
```

The first administrator is created from the managed environment variables or
from the hidden interactive prompt. A non-interactive local start without
those values fails with an actionable message. Credentials are never passed
as command-line arguments or written to logs.

## External PostgreSQL

Use `api` for the authenticated control plane and local execution, or `worker`
for distributed execution without a product API. Both require external
PostgreSQL metadata and a mounted keyring:

```bash
export DB_URL='<metadata-jdbc-url>'
export DB_USERNAME='<metadata-user>'
export DB_PASSWORD='<managed-database-password>'
export REPLICADB_SECURITY_MASTER_KEY_FILE='<managed-keyring-path>'
./bin/replicadb-server start api
```

A worker uses the same metadata variables and a unique identity:

```bash
export REPLICADB_WORKER_IDENTITY='<worker-id>'
./bin/replicadb-server start worker
```

The API listens on port 8080. Worker management health is on loopback port
9091 by default and must remain private. Put TLS or an authenticated reverse
proxy in front of an API exposed beyond the local machine.

## Datasource profiles

The server stores reusable source and sink profiles in its encrypted datasource
catalog. Jobs reference datasource UUIDs and keep only replication settings
such as tables, modes, watermarks, retry policy, and tuning.

Connection credentials and connector security values are submitted through the
authenticated API, encrypted before PostgreSQL persistence, and never returned
to the frontend. Non-secret connector settings belong to `technicalParams`;
sensitive values belong to the datasource security bundle. Blank security fields
preserve existing values during updates; `clearSecurityKeys` explicitly removes
one.

Both `api` and `worker` require the configured keyring. See
[`DEPLOYMENT.md`](../DEPLOYMENT.md) for TLS, key rotation, backups, and
API/worker operations.

## Backup and upgrade

Stop the server before backing up or restoring `data/postgresql`. Back up the
PostgreSQL data and `security/master-key.json` together. The native bundle
cache can be recreated, but the keyring is required to decrypt datasource
credentials. Major PostgreSQL upgrades are not automatic.

Keep an experimental previous home as a backup and configure a new
`REPLICADB_SERVER_HOME`; verify any manual migration before using it. CLI
installations from `0.18.x` continue to use `REPLICADB_HOME` and are not moved
by a server installation.

## Troubleshooting

- A permissions error means the server home, including `run` and `logs`, must
  be writable by the launching user.
- A port error means another process owns port 8080 or the configured worker
  management port; inspect `status` before stopping anything.
- A network error on first local start can be resolved by retrying with network
  access or placing a verified bundle in the server cache. Do not disable
  checksum verification.
- A stale PID is cleared by `status`; `stop` validates the process identity
  before sending a signal.
- The development script
  `replicadb-server/frontend/scripts/start-local.sh` is a disposable Docker
  and Vite harness. It is not a durable server installer.
