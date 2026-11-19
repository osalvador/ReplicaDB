---
title: CLI quickstart
description: Use the standalone CLI to copy a PostgreSQL order and verify the destination.
---

Copy one order from `shop.orders` to `reporting.orders` with the standalone
ReplicaDB CLI. You need Java 17 or newer, Docker, and a POSIX shell.

## Prepare sample data

Follow [Sample data](/ReplicaDB/getting-started/sample-data/) to start the
database and create the tables. The source starts with order `1` and the
destination starts empty. Keep using the terminal where you set
`DEMO_DB_PASSWORD`.

## Install the CLI

Download and extract the published CLI archive:

```bash
curl -fLO https://github.com/osalvador/ReplicaDB/releases/download/v1.0.3/ReplicaDB-1.0.3.tar.gz
mkdir -p ReplicaDB-1.0.3
tar -xzf ReplicaDB-1.0.3.tar.gz -C ReplicaDB-1.0.3
cd ReplicaDB-1.0.3
./bin/replicadb --version
```

See [CLI installation](/ReplicaDB/cli/installation/) for platform and
driver setup.

## Configure and run

Save this options file in the CLI directory. The JDBC URL points to the same
lab database for both endpoints. The table names select separate schemas.

```bash
cat > orders.conf <<'EOF'
mode=complete
jobs=1
source.connect=jdbc:postgresql://127.0.0.1:55432/replicadb_demo
source.user=demo
source.password=${DEMO_DB_PASSWORD}
source.table=shop.orders
sink.connect=jdbc:postgresql://127.0.0.1:55432/replicadb_demo
sink.user=demo
sink.password=${DEMO_DB_PASSWORD}
sink.table=reporting.orders
EOF

./bin/replicadb --options-file ./orders.conf
```

This runs one task. In `complete` mode ReplicaDB clears the lab destination
before copying the selected source rows. The command should exit with code
`0`. For options-file precedence and substitutions, see
[CLI configuration](/ReplicaDB/cli/configuration/).

## Verify the destination

```bash
docker exec replicadb-docs-demo psql -U demo -d replicadb_demo \
  -c 'SELECT id, status, change_seq FROM reporting.orders ORDER BY id;'
```

| id | status | change_seq |
| --- | --- | --- |
| 1 | pending | 10 |

If the command fails, use the exit code and process logs with
[CLI troubleshooting](/ReplicaDB/cli/troubleshooting/). For a real deployment,
review [security and TLS](/ReplicaDB/operations/security-and-tls/).

## Keep exploring or clean up

Keep the database running to try [replication modes](/ReplicaDB/cli/replication-modes/).
To try the [Server quickstart](/ReplicaDB/getting-started/server-quickstart/) on
the same database, first [reset the sample data](/ReplicaDB/getting-started/sample-data/#inspect-or-reset-the-data).
When finished, stop the lab with `docker stop replicadb-docs-demo`.
