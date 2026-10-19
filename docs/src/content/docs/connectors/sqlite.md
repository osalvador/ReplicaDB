---
title: SQLite connector
description: SQLite file-based JDBC behavior and mode limitations.
---


SQLite uses the `jdbc:sqlite:` scheme with the `org.sqlite.JDBC` driver. It can be a
source or sink. As a source, all modes are available. As a sink, it supports
complete and incremental modes. The sink does not support complete-atomic mode.

## Connection

| Item | Value |
| ---- | ----- |
| Source or sink URL | `jdbc:sqlite:${SQLITE_PATH}` |
| Driver class | `org.sqlite.JDBC` |
| Connection parameters | `busy_timeout` is set to 30000 ms by ReplicaDB |

## Source options

| Property | Description |
| -------- | ----------- |
| `source.table` | Table to read. |
| `source.columns` | Comma-separated column list to read. |
| `source.where` | SQL filter applied to the source read. |
| `source.query` | Free-form SQL query used as the source. |

## Sink options

| Property | Description |
| -------- | ----------- |
| `sink.table` | Target table. A schema prefix such as `main.t_sink` is allowed. |
| `sink.columns` | Comma-separated column list to write. |
| `sink.staging.schema` | Schema used for staging tables during incremental merges, for example `main`. |
| `mode` | `complete` or `incremental`. `complete-atomic` is not supported for this sink. |

## Locking and concurrency

SQLite is a single-file database, so writes use file locking. ReplicaDB sets
`busy_timeout` to 30000 ms so that a connection waits for a lock instead of
failing immediately. When the connection is closed, ReplicaDB runs a WAL
checkpoint to release locks. 

## Incremental mode

Incremental merges match rows on the sink primary key, so the sink table must
define one.

## Example

Replicate a table from a source database into a SQLite file:

```properties
source.connect=jdbc:postgresql://${PGHOST}:5432/catalog
source.user=${PGUSER}
source.password=${PGPASSWORD}
source.table=t_source

sink.connect=jdbc:sqlite:${SQLITE_PATH}
sink.table=main.t_sink
sink.staging.schema=main

mode=complete
```
