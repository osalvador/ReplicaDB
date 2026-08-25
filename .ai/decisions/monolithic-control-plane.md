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
  - id: deployment
    resource: DEPLOYMENT.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

Driving forces: deliver durable jobs, scheduling, monitoring, and access control before the complexity of distributed workers is justified.

Decision: the initial managed deployment starts the REST surface and Quartz integration in one JVM, with asynchronous run coordination around the core engine. PostgreSQL stores product state and persisted schedules; the API now uses the clustered JDBC Quartz store and reconciles product state into it. Phase 3.1 and Phase 3.2 extended the original monolith with durable claims, recovery, cancellation intent, fenced finalization, and an isolated worker runtime; Phase 3.3 operationalized the multi-API/multi-worker topology.

Trade-off: the original single-instance design limited scale and worker isolation. The compatibility API coordinator remains available, while distributed worker dispatch through PostgreSQL notifications/polling, heartbeats, shared throttling, and clustered Quartz are now implemented. Phase 3.4 still addresses approximate fairness across worker capacity and is not part of this decision's completed scope.
