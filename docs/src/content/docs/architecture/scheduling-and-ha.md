---
title: Scheduling and high availability
description: Explain Quartz JDBC clustering and the limits of notification delivery.
---

# Scheduling and high availability

Quartz uses JDBC-backed scheduler state in the API profile. Multiple API nodes
can participate in the same scheduler ownership model; the durable schedule
record and run state remain in PostgreSQL.

`ScheduleReconciler` tolerates partial registration failures and reconciles
enabled schedules on startup. A notification is an acceleration signal. The
worker polling path is mandatory for correctness when a listener is delayed,
disconnected, duplicated, or unavailable.

High availability does not mean duplicate execution is acceptable. Claims,
leases, token fencing, and overlap state transitions keep one durable run
owner at a time. Recovery creates a new attempt rather than resuming a worker
whose lease expired.