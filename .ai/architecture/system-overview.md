---
type: Architecture
description: ReplicaDB is a two-artifact batch replication product with a reusable Java core, managed Spring Boot control plane, and React frontend.
sources:
  - id: decisions
    resource: ARCHITECTURE_DECISIONS.md
  - id: root-build
    resource: pom.xml
  - id: server-build
    resource: replicadb-server/pom.xml
  - id: server-app
    resource: replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

ReplicaDB has three observed implementation surfaces:

- The root Maven artifact is the standalone CLI and core engine. It owns `ToolOptions`, orchestration, managers, row sets, launchers, and broad database/file compatibility.
- `replicadb-server` is a sibling Spring Boot module. Its `api` profile exposes REST, runs Quartz scheduling, persists control-plane state, and delegates execution to the root core. Phase 3.1 adds the durable retry/lease/fencing contract, but a distributed worker profile, notification dispatch, and heartbeat runtime are still future work.
- `replicadb-server/frontend` is a React/Vite SPA served as static assets by the server build. It uses the REST/OpenAPI contract rather than duplicating server domain behavior.

The dependency direction is CLI/options -> core orchestration -> manager adapters. Managed server domain -> application ports/services -> JDBC persistence, API, security, scheduling, and execution packages surround a translation boundary that produces `ToolOptions`; controllers do not call vendor managers directly. The frontend calls endpoint modules through a credentialed API client and generated OpenAPI types.

Reference implementations: `src/main/java/org/replicadb/ReplicaDB.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, and `replicadb-server/frontend/src/router/routes.tsx`.
