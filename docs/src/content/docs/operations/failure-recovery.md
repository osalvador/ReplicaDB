---
title: Failure recovery
description: Recover worker loss, cancellation, retries, and indeterminate sinks.
---

# Failure recovery

Worker loss never resumes an abandoned run. Wait for PostgreSQL lease expiry;
recovery preserves the abandoned attempt and creates a new attempt when the
retry policy allows it. Inspect the previous run before accepting the
replacement outcome.

Cancellation persists intent before attempting a local signal. A run can be
`CANCEL_REQUESTED` while the worker drains, then becomes `CANCELLED`; the sink
may be indeterminate. Manual retry starts from the beginning and is not a
resume operation.

For destructive `complete` mode, expect a truncated or partially populated
sink after interruption. Prefer `complete-atomic` or `incremental` where the
connector supports it, and verify source/sink row counts before declaring
recovery complete.

Graceful shutdown stops new admissions, lets active runs finish or requests
cancellation, stops polling/listener delivery, and exits within the configured
shutdown timeout.