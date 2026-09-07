---
title: Filtering and queries
description: Select source rows with tables, predicates, columns, and queries.
---

# Filtering and queries

Use `source.table` with `source.where` for a table-backed replication. Use
`source.columns` to limit the selected columns when the sink mapping supports
that shape.

The table name may identify a connector-supported table-like object such as a
view. `source.where` is appended to the connector's generated selection, so
provide only the predicate rather than the `WHERE` keyword. `source.columns`
and `sink.columns` are ordered, comma-separated mappings; keep their counts and
types compatible when names or order differ.

```properties
mode=complete
source.connect=${SOURCE_CONNECT}
source.table=${SOURCE_TABLE}
source.columns=id,payload
source.where=updated_at > ${SOURCE_CUTOFF}
sink.connect=${SINK_CONNECT}
sink.table=${SINK_TABLE}
```

For free-form reads, use `source.query` instead of `source.table`. Do not
combine `source.query` with the explicit `replication.table.{N}.*` catalog or
with automated incremental watermark tracking, because ReplicaDB cannot infer
the required table and merge semantics from an arbitrary query.

```properties
mode=complete
source.connect=${SOURCE_CONNECT}
source.query=SELECT customer_id, total FROM reporting_orders
sink.connect=${SINK_CONNECT}
sink.table=${SINK_TABLE}
sink.columns=customer_id,total
```

A free-form query owns its projection, joins, expressions, and ordering.
ReplicaDB streams its result; it does not validate that an expression is
portable to the source engine or that the projected values fit the sink types.

Treat filters as source SQL expressions supported by the connector. Use
environment substitution for values and validate the resulting query against
a read-only account before a production run.
