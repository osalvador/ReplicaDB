---
title: CLI installation
description: Install and verify the standalone ReplicaDB archive.
---

# CLI installation

The CLI archive contains its launcher, runtime libraries, and connector
drivers. It requires Java 17 or newer and does not require Maven, npm, Docker,
or a metadata PostgreSQL database.

## Release archive

```bash
curl -fL -o ReplicaDB-1.0.0.tar.gz "https://github.com/osalvador/ReplicaDB/releases/download/v1.0.0/ReplicaDB-1.0.0.tar.gz"
tar -xzf ReplicaDB-1.0.0.tar.gz
cd ReplicaDB-1.0.0
./bin/replicadb --version
./bin/replicadb --help
```

The equivalent ZIP archive is suitable for Windows. Keep the extracted
installation and its `REPLICADB_HOME` separate from
`REPLICADB_SERVER_HOME`; the two products do not migrate each other's state.

## Add an external JDBC driver

Drivers included in the release cover the maintained connector set. For an
additional JDBC-compliant system, place its driver JAR under the CLI
installation's `lib` directory and set the driver class through a
`source.connect.parameter.*` or `sink.connect.parameter.*` property in an
options file.

## Verify before a real transfer

Run `--help`, inspect the exact mode and connector settings in the options
file, and perform a small complete replication against a disposable sink.
Keep logs and options files under the owner-controlled directory, and resolve
connection security from environment-managed values.