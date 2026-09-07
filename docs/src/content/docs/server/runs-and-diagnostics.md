---
title: Runs and diagnostics
description: Trigger, inspect, cancel, retry, and diagnose managed runs.
---

# Runs and diagnostics

Job details contain run history, and `/runs/:id` opens a run detail page. The
page shows status, attempt information, timestamps, row counts, cancellation
state, and a bounded diagnostic log. Statuses include pending, running,
cancel-requested, cancelled, retry-scheduled, succeeded, and failed.

`Cancel run` persists cancellation intent before the best-effort local signal.
The UI displays a persisted cancellation warning when the sink outcome may be
indeterminate. `Retry run` starts a new attempt from the beginning; it is not
resume behavior. Retry availability depends on the run status and permission.

Run logs are operational data. They are bounded, redacted, and not a place for
credentials, keyrings, lease tokens, or resolved datasource security. A log
load failure offers a retry action and does not turn an absent log into a
successful run.