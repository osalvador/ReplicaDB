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

When `lease_until <= now()`, recovery does one of three things:

- a cancellation-requested run becomes `CANCELLED`;
- an eligible automatic retry becomes `RETRY_SCHEDULED` plus a new `PENDING`
  attempt at its backoff time; or
- a run without another attempt becomes `FAILED` with an expiry reason.

The recovery path uses PostgreSQL time. It does not compare JVM wall clocks to
database eligibility and it does not resume abandoned work.