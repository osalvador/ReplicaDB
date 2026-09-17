---
title: Managed server guide
description: Operate authenticated ReplicaDB jobs, schedules, permissions, and runs.
---


The managed server adds authenticated durable operation around the ReplicaDB
replication core. It provides datasource profiles, jobs, schedules, run
history, diagnostics, permissions, users, and audit events.

## What the server owns

The server stores the operational intent and history that a standalone CLI
invocation does not keep: datasource profiles, job definitions, permissions,
schedules, attempts, audit events, and safe diagnostics. The API serves the
browser frontend and control plane; workers execute durable runs in a
distributed deployment. PostgreSQL and the encryption keyring are part of this
managed state boundary.

## Follow an operator workflow

1. [Install and choose a server profile](/ReplicaDB/server/installation/).
2. [Sign in and manage your profile](/ReplicaDB/server/sign-in-and-profile/).
3. [Create and verify datasources](/ReplicaDB/server/datasources/).
4. [Create a job and run it manually](/ReplicaDB/server/jobs/).
5. [Inspect its run and diagnostics](/ReplicaDB/server/runs-and-diagnostics/).
6. [Schedule recurring work](/ReplicaDB/server/schedules/) only after the
	manual run is understood.
7. [Apply resource permissions](/ReplicaDB/server/permissions/), manage
	[users](/ReplicaDB/server/users/), and review [audit events](/ReplicaDB/server/audit/)
	as administrative operations.

Use the [dashboard](/ReplicaDB/server/dashboard/) to locate operational
changes, but use the job and run views to investigate their causes. For
deployment, monitoring, backup, and recovery procedures, continue to the
[Operations guide](/ReplicaDB/operations/).

The frontend is a usability layer. Backend authorization remains authoritative
for every resource and action. A visible control can still fail after a role,
permission, binding, or resource changes; the page reports the safe error and
preserves context where possible.
