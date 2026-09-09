---
title: Scheduling and high availability
description: Explain Quartz JDBC clustering and the limits of notification delivery.
---


Quartz uses JDBC-backed scheduler state in the API profile. Multiple API nodes
can participate in the same scheduler ownership model; the durable schedule
record and run state remain in PostgreSQL.

## Schedule intent and scheduler mechanics

`job_schedule` is the product-level intent: cron expression, timezone, and
enabled state. Quartz is the clustered mechanism that turns this intent into a
fire. Stable job and trigger keys let every API reconcile the same persisted
schedule without inventing a second schedule catalog.

`ScheduleReconciler` tolerates partial registration failures and reconciles
enabled schedules on startup. A notification is an acceleration signal. The
worker polling path is mandatory for correctness when a listener is delayed,
disconnected, duplicated, or unavailable.

## Failure and rollout boundary

An API restart can leave reconciliation incomplete temporarily, so startup
reconciles enabled schedules again. A scheduled fire creates the same durable
pending run as a manual trigger and cannot bypass permissions, claims, leases,
or active-run constraints. Mixed RAM and JDBC Quartz ownership is prohibited:
those stores cannot coordinate one schedule fleet.

High availability does not mean duplicate execution is acceptable. Claims,
leases, token fencing, and overlap state transitions keep one durable run
owner at a time. Recovery creates a new attempt rather than resuming a worker
whose lease expired.

The design tolerates an unavailable notification listener because polling
remains durable. It does not promise that every external sink write can be
reversed after a process failure. See [distributed topology](/ReplicaDB/architecture/distributed-topology/)
and [upgrades](/ReplicaDB/operations/upgrades/) for the corresponding topology
and rollout constraints.
