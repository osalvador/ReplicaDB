---
title: CSV and local file connector
description: CSV source/sink formats, local file roles, and mode limitations.
---


Use the `file:` scheme to read or write local files with the CSV and ORC file
formats. Both source and sink roles are supported. File endpoints are
single-job: one task writes the file, and the header is written only by task 0.
Replication modes and their limits are listed below. The sink does not provide
complete-atomic replacement, and a failed run is not an atomic replacement of
an existing file.

## Selecting the format

Set the format with `--source-file-format` or `--sink-file-format` on the CLI,
or with the `source.file.format` and `sink.file.format` properties. The
implemented values are `csv` and `orc`. Any other value logs a warning and
falls back to CSV.

## Predefined CSV formats

Set the `format` parameter to one of these names. Names are case-insensitive.
An unknown name falls back to `DEFAULT`. The settings follow the Apache Commons
CSV predefined formats; a dash means the format does not set that value.

| Name | Delimiter | Quote | Escape | Record separator | Null string | Other settings |
| ---- | --------- | ----- | ------ | ---------------- | ----------- | -------------- |
| `DEFAULT` | `,` | `"` | - | `\r\n` | - | Ignores empty lines |
| `EXCEL` | `,` | `"` | - | `\r\n` | - | Does not ignore empty lines |
| `INFORMIX_UNLOAD` | `,` | `"` | `\` | `\n` | - | - |
| `INFORMIX_UNLOAD_CSV` | `,` | `"` | - | `\n` | - | - |
| `MONGODB_CSV` | `,` | `"` | `"` | - | - | `quoteMode` `ALL_NON_NULL` |
| `MONGODB_TSV` | Tab | `"` | `"` | - | - | `quoteMode` `ALL_NON_NULL` |
| `MYSQL` | Tab | - | `\` | `\n` | `\N` | Does not ignore empty lines; `quoteMode` `ALL_NON_NULL` |
| `ORACLE` | `,` | `"` | `\` | - | `\N` | Trims values; `quoteMode` `MINIMAL` |
| `POSTGRESQL_CSV` | `,` | `"` | `"` | `\n` | `""` | `quoteMode` `ALL_NON_NULL` |
| `POSTGRESQL_TEXT` | Tab | `"` | `\` | `\n` | `\\N` | `quoteMode` `ALL_NON_NULL` |
| `RFC4180` | `,` | `"` | - | `\r\n` | - | Does not ignore empty lines |
| `TDF` | Tab | `"` | - | `\r\n` | - | Ignores surrounding spaces |

The obsolete names `MONGO_CSV` and `MONGO_TSV` are not accepted; use
`MONGODB_CSV` and `MONGODB_TSV`.

## CSV options

Set these as `source.connect.parameter.*` or `sink.connect.parameter.*`. Each
option overrides the value from the predefined format.

| Parameter | Description | Default |
| --------- | ----------- | ------- |
| `format` | Predefined format name (see the table above) | `DEFAULT` |
| `format.delimiter` | Field delimiter. Must be exactly one character. | Format value |
| `format.quote` | Quote character. Must be exactly one character. | Format value |
| `format.escape` | Escape character. Must be exactly one character. | Format value |
| `format.recordSeparator` | Record separator. Applied only when non-empty. | Format value |
| `format.nullString` | Text written for null values and read as null. Applied only when non-empty. | Format value |
| `format.quoteMode` | Quote mode (see the table below). Values are case-insensitive. Unknown values are ignored. | Format value |
| `format.firstRecordAsHeader` | `true` or `false`. Reads the first record as the header on a source, and writes a header on a sink. | Format value |
| `format.ignoreEmptyLines` | `true` or `false`. Skips empty lines. | Format value |
| `format.ignoreSurroundingSpaces` | `true` or `false`. Trims spaces around values. | Format value |
| `format.trim` | `true` or `false`. Trims values. | Format value |

Boolean options are parsed with `Boolean.parseBoolean`, so any value other than
`true` (ignoring case) is read as `false`. Use `format.recordSeparator` to
control output line endings. In the options file, enter `\n` for LF or
`\r\n` for CRLF. The following options write a tab-separated file with a header.

```properties
sink.file.format=csv
sink.connect.parameter.format=TDF
sink.connect.parameter.format.firstRecordAsHeader=true
sink.connect.parameter.format.recordSeparator=\n
```

CSV settings apply to CSV paths, including S3 CSV file output. Use the ORC
compression option for ORC output. Check the expected delimiter, header,
null marker, and quoting with the system that consumes the file.

## Quote modes

Set `format.quoteMode` to one of these values. The values are case-insensitive.

| Value | Behavior |
| ----- | -------- |
| `ALL` | Quotes every field. |
| `ALL_NON_NULL` | Quotes every field that is not null. |
| `MINIMAL` | Quotes only fields that contain the delimiter, the quote character, or a line separator. |
| `NON_NUMERIC` | Quotes every field that is not numeric. |
| `NONE` | Never quotes fields. |

## Source column types

A CSV source requires `source.connect.parameter.columns.types`. If it is not
set, the run fails with `Parameter 'source.connect.parameter.columns.types'
cannot be null`. Provide one type entry for each source column.

For input such as `1,paid`, use `INTEGER,VARCHAR` for the two types.
If the file starts with `id,status`, set
`source.connect.parameter.format.firstRecordAsHeader=true` to skip that
record as data. Set `source.columns=id,status` whenever the transfer needs
column names, even if the input has a header. ReplicaDB does not use that
header to populate the source's column metadata. For custom output header
names, set `sink.columns` in file order and enable
`sink.connect.parameter.format.firstRecordAsHeader`. The
[CSV-to-CSV example](/ReplicaDB/reference/example-options-files/#file-endpoints-and-diagnostic-level)
shows the complete file and its expected output.

## Replication modes

| Mode | Behavior |
| ---- | -------- |
| `complete` | The first temporary file replaces the sink file. The remaining temporary files are appended to it. |
| `incremental` | All temporary files are appended to the sink file. The sink file is created if it does not exist. |
| `complete-atomic` | Rejected for file sinks with an `UnsupportedOperationException`. |

When appending to an existing CSV that already has a header, set
`sink.connect.parameter.format.firstRecordAsHeader=false` to avoid writing
another header inside the file. Repeating the same incremental input
appends those rows again.

## Example properties

This example replaces a CSV file with data read from another CSV file.

```properties
mode=complete
jobs=1
source.connect=${SOURCE_FILE_PATH}
source.file.format=csv
source.columns=id,status
source.connect.parameter.columns.types=${SOURCE_COLUMN_TYPES}
source.connect.parameter.format=RFC4180
source.connect.parameter.format.firstRecordAsHeader=true
sink.connect=${SINK_FILE_PATH}
sink.file.format=csv
sink.connect.parameter.format=RFC4180
sink.connect.parameter.format.quoteMode=MINIMAL
```

The following example appends a batch to an existing CSV file.

```properties
mode=incremental
jobs=1
source.connect=${SOURCE_CONNECTION}
sink.connect=${SINK_FILE_PATH}
sink.file.format=csv
sink.connect.parameter.format=DEFAULT
```

## ORC limits

ORC is supported for local files and is read and written through the same
`file:` scheme.

- `sink.connect.parameter.orc.compression` sets the compression codec for
  ORC output. Values are case-insensitive
  names of the ORC `CompressionKind` enum, such as `NONE`, `ZLIB`, or
  `SNAPPY`. An invalid value logs a warning and falls back to `ZLIB`. The
  default is `ZLIB`.
- Each task writes a local temporary file. On completion, the temporary files
  are merged. The merge uses local paths, so ORC is local-only.
- In `complete` mode, the first temporary file is moved to the sink and the
  remaining files are merged into it. In `incremental` mode, all temporary
  files are merged into the sink, which is created if it does not exist.
- A failure or cancellation during the merge is not an atomic replacement of
  the sink file.
- Validate column types and null handling with representative data before a
  production run.

This example writes ORC output with ZLIB compression.

```properties
mode=complete
jobs=1
source.connect=${SOURCE_CONNECTION}
sink.connect=${SINK_FILE_PATH}
sink.file.format=orc
sink.connect.parameter.orc.compression=ZLIB
```
