---
title: Dispatch and recovery
description: Explain directed claims, fallback polling, cancellation, and lease expiry.
---


`WorkerDispatchCoordinator` has directed, fallback, and generic admission
paths. A signal for a specific run tries a directed claim first. If that claim
misses, the worker falls back to generic eligible work. Polling refills the
generic path when notifications are absent.

## From intent to execution

Manual and scheduled triggers share the same path: create durable pending work,
offer a wake-up, then let an available worker claim through PostgreSQL. A
notification carries only the run identity; it does not contain a job
definition, credentials, or ownership decision. This makes a lost, duplicate,
or delayed notification a latency concern rather than a reason to lose work.

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

## Claim-time consistency

The stable UUID lock order prevents competing claims from taking datasource
locks in opposite order. Claiming validates that the job and bindings remain
enabled before the core sees configuration. The resulting snapshot records the
inputs chosen for that attempt, while later edits remain eligible for future
attempts. This deliberately favors a reproducible active attempt over an
in-flight configuration rewrite.

When `lease_until <= now()`, recovery does one of three things:

- a cancellation-requested run becomes `CANCELLED`;
- an eligible automatic retry becomes `RETRY_SCHEDULED` plus a new `PENDING`
  attempt at its backoff time; or
- a run without another attempt becomes `FAILED` with an expiry reason.

The recovery path uses PostgreSQL time. It does not compare JVM wall clocks to
database eligibility and it does not resume abandoned work.

## Failure model

Database time prevents separate worker clocks from disagreeing about lease
expiry or retry backoff. Recovery preserves the expired attempt, records its
reason, and creates at most one new eligible attempt when policy permits. The
next worker starts from the beginning because a row count cannot prove which
external writes completed. See [concurrency and fencing](/ReplicaDB/architecture/concurrency-and-fencing/)
for stale-worker protection and [failure recovery](/ReplicaDB/operations/failure-recovery/)
for operator actions.
