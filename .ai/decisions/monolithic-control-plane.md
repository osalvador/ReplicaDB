---
type: Decision
description: The first managed deployment combines REST API, scheduler, and execution coordination in one Spring Boot runtime.
sources:
  - id: decision
    resource: ARCHITECTURE_DECISIONS.md
  - id: app
    resource: replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java
  - id: scheduler
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduleReconciler.java
  - id: config
    resource: replicadb-server/src/main/resources/application.yml
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

Driving forces: deliver durable jobs, scheduling, monitoring, and access control before the complexity of distributed workers is justified.

Decision: the `api` profile starts the REST surface and Quartz integration in one JVM, with asynchronous run coordination around the core engine. PostgreSQL stores product state and persisted schedules; the current Quartz runtime is reconciled from that state. Phase 3.1 places claims, recovery, cancellation intent, and fenced finalization behind application ports/services without introducing a worker runtime.

Trade-off: a single instance limits scale and worker isolation. The current API coordinator still has only best-effort local cancellation delivery and no heartbeat or expiry-recovery loop. Distributed worker dispatch through PostgreSQL notifications and polling is an approved Phase 3.2 direction, not current implementation.
