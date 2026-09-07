---
title: Incremental watermarks
description: Use explicit source watermarks for repeatable incremental CLI runs.
---

# Incremental watermarks

Automated watermarking is an explicit incremental-mode contract. Set
`--incremental-watermark-column` for a column exposed by `--source-table` and
optionally set `--incremental-watermark-value` for the last successful value.
The options-file keys are `incremental.watermark.column` and
`incremental.watermark.value`.

```bash
./bin/replicadb --mode incremental \
  --source-table "$SOURCE_TABLE" \
  --incremental-watermark-column "$WATERMARK_COLUMN" \
  --incremental-watermark-value "$LAST_WATERMARK" \
  --options-file ./replicadb.conf
```

When the value is omitted, the first run reads all rows returned by the source
selection and reports the highest observed value. Store that value in your
orchestration system and pass it to the next invocation only after a
successful run.

## Limits

- Watermarks apply only to `incremental` mode and a concrete `source.table`.
- They cannot be combined with `source.query` or `replication.table.*`.
- Deletes are not propagated; merging requires a sink primary key.
- A failed or cancelled run does not advance the value.
- A transaction that commits after the read with an older value can be missed;
  there is no read-lag setting yet.