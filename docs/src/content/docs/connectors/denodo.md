---
title: Denodo connector
description: Denodo source-only behavior and JDBC query guidance.
---


Denodo is a source-only connector. It can read rows for the source modes
(complete, complete-atomic, and incremental), but it cannot receive sink writes,
so it cannot be used as `sink.connect` and has no staging path. The selected
sink connector determines staging and merge guarantees.

## Connection

The JDBC URL uses the `jdbc:vdb:` scheme and loads `com.denodo.vdp.jdbc.Driver`.

| Setting | Example |
| ------- | ------- |
| URL | `jdbc:vdb://${DENODO_HOST}:${DENODO_PORT}/${DENODO_DATABASE}` |
| Driver class | `com.denodo.vdp.jdbc.Driver` (selected from the scheme) |
| Username | `${DENODO_USER}` |
| Password | `${DENODO_PASSWORD}` |

Provide credentials through the environment or a secret store, not in
committed properties files.

## Source options

| Option | Use |
| ------ | --- |
| `source.table` | A Denodo virtual view to read in full. |
| `source.query` | A SQL query that Denodo executes, for joins, projections, or filters. |
| `source.columns` | A column list applied to `source.table`. |
| `source.where` | A filter applied to `source.table`. |

Use `source.table` for a virtual view, or `source.query` when Denodo must own a
join, projection, or filter. Denodo runs as a single-job source, so the query
is executed once per run.

## Query pushdown

Denodo decides which parts of a query run in the underlying data sources. Check
the execution plan in the Denodo tools to confirm that filters, joins, and
projections are pushed down as intended. A query that is not pushed down can
move large volumes of data through the Denodo server.

## Example properties file

```properties
source.connect.url=jdbc:vdb://${DENODO_HOST}:${DENODO_PORT}/${DENODO_DATABASE}
source.connect.user=${DENODO_USER}
source.connect.password=${DENODO_PASSWORD}
source.query=SELECT id, name, updated_at FROM customers WHERE active = 1
mode=complete

sink.connect=jdbc:postgresql://${PGHOST}:5432/${PGDATABASE}
sink.connect.user=${PGUSER}
sink.connect.password=${PGPASSWORD}
sink.table=customers
```

## Limits and notes

- The service account must have read access to the virtual views and to every
  underlying source they use.
- Type conversion is performed by Denodo. Validate the result types against the
  sink table.
- Denodo cannot receive sink writes, so the connector does not support Denodo as
  a sink or as a staging target.
