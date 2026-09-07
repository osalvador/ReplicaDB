---
title: Dispatch and recovery
description: Explain directed claims, fallback polling, cancellation, and lease expiry.
---

# Dispatch and recovery

`WorkerDispatchCoordinator` has directed, fallback, and generic admission
paths. A signal for a specific run tries a directed claim first. If that claim
misses, the worker falls back to generic eligible work. Polling refills the
generic path when notifications are absent.

`ScheduleReconciler` registers enabled schedules on API startup. A schedule
creates durable pending work and a wake-up; it does not bypass the same claim,
permission, or lease path used by manual execution.

Claiming a run and preparing its inputs is one database operation. The worker
locks the run, job binding, and both datasource rows in stable UUID order,
checks that the bindings are still enabled, and stores encrypted source and
sink snapshots plus `datasourcesResolvedAt`. That claim-time snapshot is
immutable for the active attempt. Editing a datasource affects the next claim;
disabling a binding blocks a future claim but does not rewrite an attempt that
is already running.

When `lease_until <= now()`, recovery does one of three things:

- a cancellation-requested run becomes `CANCELLED`;
- an eligible automatic retry becomes `RETRY_SCHEDULED` plus a new `PENDING`
  attempt at its backoff time; or
- a run without another attempt becomes `FAILED` with an expiry reason.

The recovery path uses PostgreSQL time. It does not compare JVM wall clocks to
database eligibility and it does not resume abandoned work.
