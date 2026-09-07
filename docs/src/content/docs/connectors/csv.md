---
title: CSV and local file connector
description: CSV source/sink formats, local file roles, and mode limitations.
---

# CSV and local files

Use the `file:` scheme. The file manager supports source and sink roles for
complete and incremental modes, with CSV and related file-format settings. It
is single-job only and does not provide complete-atomic sink replacement.

Select the source or sink format with `source-file-format` or
`sink-file-format` and configure delimiter, quoting, escaping, and type
conversion through the connector's supported parameters. Use a staging
directory owned by the process and verify the resulting files before moving
them into a downstream workflow.