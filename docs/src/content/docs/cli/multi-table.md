---
title: Multi-table replication
description: Configure a sequential table catalog for the standalone CLI.
---


Define an explicit catalog in the options file with contiguous one-based
indexes. Each entry needs both a source and sink table:

```properties
mode=complete
jobs=1
source.connect=${SOURCE_CONNECT}
sink.connect=${SINK_CONNECT}
replication.table.1.source=${SOURCE_TABLE_ONE}
replication.table.1.sink=${SINK_TABLE_ONE}
replication.table.2.source=${SOURCE_TABLE_TWO}
replication.table.2.sink=${SINK_TABLE_TWO}
```

The CLI validates that indexes start at 1 and have no gaps. It rejects a
catalog combined with `source.table`, `sink.table`, or `source.query`.
Tables execute sequentially and the process stops on the first failure.

For each pair, ReplicaDB creates fresh source and sink managers, runs normal
pre-source and pre-sink hooks, executes that table's workers, completes
post-processing, and cleans up before opening the next pair. This isolates
generated staging and temporary state between entries. `jobs` still controls
partitions inside one pair; table pairs are never concurrent.

For `incremental` and `complete-atomic`, use `sink.staging.schema` when
staging is needed. Fixed `sink.staging.table` and
`sink.staging.table.alias` values are not supported with a multi-table
catalog. Automated incremental watermarks cannot be combined with the
catalog.

If pair 2 fails, pair 3 is not started and the process returns code 1. Pair 1
is not rolled back as part of that later failure. Inspect every completed and
failed destination, correct the cause, and generate a new catalog containing
only the table pairs that should run again.
