---
title: Incremental watermarks
description: Use explicit source watermarks for repeatable incremental CLI runs.
---


Automated watermarking is an explicit incremental-mode contract. Set
`--incremental-watermark-column` for a column exposed by `--source-table` and
optionally set `--incremental-watermark-value` for the last successful value.
The options-file keys are `incremental.watermark.column` and
`incremental.watermark.value`.

Use automatic watermarks with a table-backed source on Oracle, PostgreSQL,
MySQL, MariaDB, SQL Server, Db2 (including IBM i), SQLite, or Denodo. The
destination must independently support `incremental`. For example, a file
destination appends the selected rows, while PostgreSQL merges by key. See
the [connector matrix](/ReplicaDB/connectors/) for the allowed combinations.

```bash
./bin/replicadb --mode incremental \
  --source-table "$SOURCE_TABLE" \
  --incremental-watermark-column "$WATERMARK_COLUMN" \
  --incremental-watermark-value "$LAST_WATERMARK" \
  --options-file ./replicadb.conf
```

When the value is omitted, the first run reads all rows returned by the source
selection. The engine captures the highest observed value and makes it
available after a successful merge. Server persists it for subsequent runs of
the job. With the standalone CLI, supply the lower bound from your own
orchestration. The CLI reports an exit code and does not print the computed
candidate as output.

For subsequent runs, ReplicaDB adds a strict greater-than condition for the
committed value. Choose a stable, monotonically increasing column exposed by
the source table. Advance an external checkpoint only after verifying a
successful run and the boundary it processed. Preserve the previous value
after code 1 or code 2. See the
[orders example](/ReplicaDB/cli/replication-modes/#incremental) for a known
lower bound and expected result.

## Limits

- Watermarks apply only to `incremental` mode and a concrete `source.table`.
- They cannot be combined with `source.query` or `replication.table.*`.
- The watermark selects candidate rows. Relational sinks that merge use a
  destination primary key. File sinks append the batch. Neither path
  propagates source deletes.
- A failed or cancelled run does not advance the value.
- A transaction that commits after the read with an older value can be missed;
  there is no read-lag setting yet.
- Rows sharing the maximum value are safe only when the source's ordering and
  commit model ensure no later row can appear with that same committed value;
  otherwise use a source predicate managed by your orchestration strategy.
