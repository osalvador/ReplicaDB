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
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Driving forces: deliver durable jobs, scheduling, monitoring, and access control before the complexity of distributed workers is justified.

Decision: the `api` profile starts the REST surface and Quartz integration in one JVM, with asynchronous run coordination around the core engine. PostgreSQL stores product state and persisted schedules; the current Quartz runtime is reconciled from that state.

Trade-off: a single instance limits scale and worker isolation. Distributed worker dispatch through PostgreSQL notifications and polling is an approved Phase 3 direction, not current implementation.
