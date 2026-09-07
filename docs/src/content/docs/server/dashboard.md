---
title: Dashboard
description: Read run outcomes, throughput context, and time-window metrics.
---

# Dashboard

The protected `/` route is the `Dashboard` page. It summarizes jobs, active
runs, success rate, failed runs, rows processed, average duration, and queue
latency for a selected time window.

Use the time-window controls or `Custom time range`, then choose `Apply range`.
`Refresh` refetches the summary. A run chart separates `Succeeded`, `Failed`,
and `Active`; duration and queue latency are shown as separate series.

The dashboard is an overview, not an authorization boundary. Use `Open jobs`
to inspect the job definition, schedule, bindings, and run history behind a
metric. Empty windows show `No runs in this window.` and suggest choosing a
wider range; load failures show an error with `Try again`.