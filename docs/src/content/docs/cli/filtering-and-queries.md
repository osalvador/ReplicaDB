---
title: Filtering and queries
description: Select source rows with tables, predicates, columns, and queries.
---

# Filtering and queries

Use `source.table` with `source.where` for a table-backed replication. Use
`source.columns` to limit the selected columns when the sink mapping supports
that shape.

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

Treat filters as source SQL expressions supported by the connector. Use
environment substitution for values and validate the resulting query against
a read-only account before a production run.