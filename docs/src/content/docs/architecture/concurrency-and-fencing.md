---
title: Concurrency and fencing
description: Understand atomic claims, leases, token fencing, and overlap prevention.
---


Claims use an atomic database update with `FOR UPDATE SKIP LOCKED` so workers
do not wait behind one another while selecting eligible `PENDING` runs. The
claim writes executor identity, an opaque lease token, `lease_until`,
`started_at`, and `heartbeat_at` in one ownership transition.

## One durable owner

The claim predicate and partial active-run constraint are the overlap boundary.
They make PostgreSQL, rather than any API or worker memory, decide whether a
run is eligible and who owns it. `SKIP LOCKED` allows independent workers to
search for other eligible work rather than serializing behind a busy row.

`RunLeaseService` renews a lease only while the token matches and the lease is
still valid. `HeartbeatService` stops on `FENCED` or renewal error and requests
local cancellation. `RunFinalizationService` applies the same token check to
progress, success, failure, and cancellation updates. A stale worker cannot
finalize, advance a watermark, or overwrite a newer attempt.

## Fence stale processes

A lease token is an opaque capability for one claim, not a user-facing run
identifier. Every mutable update checks it. If recovery or a newer claimant
changes ownership, the old process receives `FENCED`, stops its local work, and
cannot turn a newer result into an older terminal outcome. This is necessary
even after a network partition or slow process resumes unexpectedly.

`JobRunStateMachine` rejects illegal overlaps and terminal-state rewrites. The
database state, not an in-memory active-run map, prevents a second durable run
from being treated as the same execution.

## What fencing does not guarantee

Fencing protects ReplicaDB's durable metadata; it cannot undo writes that a
stale process already sent to an external sink. Connector mode and idempotency
therefore remain part of the recovery design. Follow the [run lifecycle](/ReplicaDB/architecture/run-lifecycle/)
for allowed transitions and [failure recovery](/ReplicaDB/operations/failure-recovery/)
for sink-risk decisions.