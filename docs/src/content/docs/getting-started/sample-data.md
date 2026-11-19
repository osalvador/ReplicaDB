---
title: Sample data
description: Start a disposable PostgreSQL database and prepare the orders used by both quickstarts.
---

Use this database for the CLI quickstart, Server quickstart, and replication
modes walkthrough. One PostgreSQL container holds two schemas so you can
replicate between them without starting two database instances. The Server
quickstart uses a separate embedded PostgreSQL database for Server metadata.

## Start the database

Run these commands in a POSIX shell with Docker running and host port
`55432` available. Keep the same terminal for the CLI quickstart so the
password variable remains available. The credentials are for this local
container.

```bash
export DEMO_DB_PASSWORD=demo-local-only

docker run --rm -d --name replicadb-docs-demo \
  -p 127.0.0.1:55432:5432 \
  -e POSTGRES_DB=replicadb_demo \
  -e POSTGRES_USER=demo \
  -e POSTGRES_PASSWORD="$DEMO_DB_PASSWORD" \
  postgres:16-alpine

docker exec replicadb-docs-demo pg_isready -h 127.0.0.1 -U demo -d replicadb_demo
```

Repeat the last command until it reports `accepting connections`.

| Setting | Value |
| --- | --- |
| JDBC URL | `jdbc:postgresql://127.0.0.1:55432/replicadb_demo` |
| Username | `demo` |
| Password | Value of `DEMO_DB_PASSWORD` set above |
| Source table | `shop.orders` |
| Sink table | `reporting.orders` |

## Create the tables

Run once on the new database. The destination starts empty.

```bash
docker exec -i replicadb-docs-demo psql -v ON_ERROR_STOP=1 -U demo -d replicadb_demo <<'SQL'
CREATE SCHEMA shop;
CREATE SCHEMA reporting;

CREATE TABLE shop.orders (
  id integer PRIMARY KEY,
  status text NOT NULL,
  change_seq bigint NOT NULL
);

CREATE TABLE reporting.orders (LIKE shop.orders INCLUDING ALL);

INSERT INTO shop.orders VALUES (1, 'pending', 10);
SQL
```

## Inspect or reset the data

This query displays the destination before and after a run.

```bash
docker exec replicadb-docs-demo psql -U demo -d replicadb_demo \
  -c 'SELECT id, status, change_seq FROM reporting.orders ORDER BY id;'
```

To restore the quickstart baseline in this disposable database, run:

```bash
docker exec -i replicadb-docs-demo psql -v ON_ERROR_STOP=1 -U demo -d replicadb_demo <<'SQL'
TRUNCATE shop.orders, reporting.orders;
INSERT INTO shop.orders VALUES (1, 'pending', 10);
SQL
```

When you finish the walkthroughs, stop the container. Docker removes it and
its sample data.

```bash
docker stop replicadb-docs-demo
```
