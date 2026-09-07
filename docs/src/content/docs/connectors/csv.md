---
title: CSV and local file connector
description: CSV source/sink formats, local file roles, and mode limitations.
---

# CSV and local files

Use the `file:` scheme. The file manager supports source and sink roles for
complete and incremental sink modes and all three source-side flows, with CSV
and ORC file-format settings. It is single-job only and does not provide
complete-atomic sink replacement or a transactional table merge.

## Formats and paths

Select the source or sink format with `--source-file-format` and
`--sink-file-format`, or the corresponding `source.file.format` and
`sink.file.format` properties. The CLI help names `csv`, `json`, `avro`,
`parquet`, and `orc`; the current file manager has concrete implementations
for CSV and ORC. Any other value logs a warning and selects CSV, so do not use
the other names as a promise of JSON, Avro, or Parquet encoding.

The connection value identifies a local path accessible to the process. Use a
directory owned by that account, keep input immutable during a run, and verify
the completed output before publishing or moving it. Multiple workers are not
supported for file endpoints.

## CSV controls

CSV starts from the Apache Commons CSV default and accepts connector parameters
for `format`, `format.quoteMode`, `format.delimiter`, `format.escape`,
`format.quote`, `format.recordSeparator`, `format.nullString`,
`format.firstRecordAsHeader`, `format.ignoreEmptyLines`,
`format.ignoreSurroundingSpaces`, and `format.trim`. Delimiter, escape, and
quote values must each be one character. Match null, header, and whitespace
rules on both sides before transferring production data.

```properties
source.connect=${SOURCE_FILE_PATH}
source.file.format=csv
source.connect.parameter.format=RFC4180
source.connect.parameter.format.firstRecordAsHeader=true
sink.connect=${SINK_FILE_PATH}
sink.file.format=csv
sink.connect.parameter.format.quoteMode=MINIMAL
```

## ORC limits

ORC uses a vectorized schema inferred from the result-set metadata and can
read or write local files. Validate decimal precision, nested or unsupported
types, null values, and compression with representative data. ORC output may
use temporary files during worker processing and merge them at completion;
failed or cancelled work is not an atomic replacement of an existing file.
