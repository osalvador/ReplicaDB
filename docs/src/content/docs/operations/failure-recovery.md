---
title: Failure recovery
description: Recover worker loss, cancellation, retries, and indeterminate sinks.
---

# Failure recovery

Worker loss never resumes an abandoned run. Wait for PostgreSQL lease expiry;
recovery preserves the abandoned attempt and creates a new attempt when the
retry policy allows it. Inspect the previous run before accepting the
replacement outcome.

The abandoned attempt becomes `RETRY_SCHEDULED`, and the replacement is a new
`PENDING` row linked through `previous_run_id`. It receives a new lease,
re-resolves current datasource profiles, and starts the replication from the
beginning. Manual retry follows the same restart model for an eligible failed
run.

Cancellation persists intent before attempting a local signal. A run can be
`CANCEL_REQUESTED` while the worker drains, then becomes `CANCELLED`; the sink
may be indeterminate. Manual retry starts from the beginning and is not a
resume operation.

An incremental watermark advances only when finalization records `SUCCEEDED`.
Failed, cancelled, expired, and retry-scheduled attempts preserve the previous
committed watermark, even though the sink may already contain some writes.

For destructive `complete` mode, expect a truncated or partially populated
sink after interruption. Prefer `complete-atomic` or `incremental` where the
connector supports it, and verify source/sink row counts before declaring
recovery complete.

Graceful shutdown stops new admissions, lets active runs finish or requests
cancellation, stops polling/listener delivery, and exits within the configured
`replicadb.worker.shutdown-timeout`, which defaults to 30 seconds. Set the
orchestrator's termination grace period above that value and remove a worker
from readiness before terminating it.
