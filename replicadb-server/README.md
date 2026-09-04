# ReplicaDB Server 0.19.0

This package runs the managed ReplicaDB server without Maven, npm, Docker, or
a system PostgreSQL installation. Java 17 is required.

## Install

Extract `ReplicaDB-server-0.19.0.tar.gz` or the matching ZIP, then run the
launcher from the extracted directory:

```bash
./bin/replicadb-server start local
./bin/replicadb-server status
./bin/replicadb-server stop
```

`start` always requires an explicit mode. `local` starts the API and manages
an embedded PostgreSQL process on loopback. On the first start, PostgreSQL is
downloaded and verified under the server home. Set
`REPLICADB_BOOTSTRAP_ADMIN_USERNAME` and
`REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` for automation, or run from a terminal
and answer the hidden password prompt. Passwords are never command-line
arguments or log output.

The server home defaults to `~/.replicadb` and can be changed with
`REPLICADB_SERVER_HOME`. It contains the PostgreSQL data directory, verified
bundle cache, keyring, PID and lock state, and logs. The CLI keeps its separate
`REPLICADB_HOME`; it is not reused or migrated automatically by the server.

## External PostgreSQL

Use `api` for the authenticated API and local execution, or `worker` for the
distributed execution runtime without a product API. Both require an external
PostgreSQL metadata database:

```bash
export DB_URL='jdbc:postgresql://db.example.invalid:5432/replicadb'
export DB_USERNAME='<managed-database-user>'
export DB_PASSWORD='<managed-database-password>'
./bin/replicadb-server start api
./bin/replicadb-server start worker
```

The worker management endpoint defaults to `127.0.0.1:9091` and should remain
private. Use TLS or an authenticated reverse proxy before exposing the API.

## Operations

Back up the server home while the server is stopped, including
`data/postgresql` and `security/master-key.json`. The native bundle cache can
be rebuilt. Keep the keyring with the database backup because encrypted
datasource credentials cannot be recovered without it. A major PostgreSQL
upgrade is not performed automatically.

The initial server JAR is approximately 213 MB. Native PostgreSQL bundles are
downloaded on demand and are not included in this package or the JAR.
