---
type: REST Endpoint
description: The dashboard API returns access-controlled run and job performance aggregates for a requested time window.
sources:
  - id: controller
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/DashboardController.java
  - id: response
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/DashboardSummaryResponse.java
  - id: store
    resource: replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java
  - id: frontend
    resource: replicadb-server/frontend/src/api/dashboardApi.ts
  - id: page
    resource: replicadb-server/frontend/src/pages/DashboardPage.tsx
generated: { by: itx-init/2.1, at: "2026-09-04T05:47:18Z" }
status: stable
---

Base path: `/api/v1/dashboard`.

`GET /summary` accepts optional ISO date-time `from` and `to` parameters. Missing values default to the preceding 24 hours; an invalid non-positive window is rejected. The controller applies `JobAccessService.visibleJobIds` before asking the stores for aggregates, so non-admin users receive only metrics for visible jobs.

The response reports the effective window, job and run totals, active/succeeded/failed counts, processed rows, average duration and queue latency, outcome buckets, and per-job performance. The frontend passes the window through the configured API client, includes it in the TanStack Query key, refetches periodically, and offers fixed lookbacks plus a validated custom duration. Dashboard navigation links to the jobs catalog for investigation; the dashboard does not replace job-level run history.

Reference implementations: `DashboardController.java`, `DashboardSummaryResponse.java`, `dashboardApi.ts`, and `DashboardPage.tsx`.
