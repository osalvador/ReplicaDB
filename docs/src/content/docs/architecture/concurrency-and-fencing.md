---
title: Concurrency and fencing
description: Understand atomic claims, leases, token fencing, and overlap prevention.
---

# Concurrency and fencing

Claims use an atomic database update with `FOR UPDATE SKIP LOCKED` so workers
do not wait behind one another while selecting eligible `PENDING` runs. The
claim writes executor identity, an opaque lease token, `lease_until`,
`started_at`, and `heartbeat_at` in one ownership transition.

`RunLeaseService` renews a lease only while the token matches and the lease is
still valid. `HeartbeatService` stops on `FENCED` or renewal error and requests
local cancellation. `RunFinalizationService` applies the same token check to
progress, success, failure, and cancellation updates. A stale worker cannot
finalize, advance a watermark, or overwrite a newer attempt.

`JobRunStateMachine` rejects illegal overlaps and terminal-state rewrites. The
database state, not an in-memory active-run map, prevents a second durable run
from being treated as the same execution.