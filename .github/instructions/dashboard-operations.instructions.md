---
applyTo: 'replicadb-server/src/main/java/org/replicadb/server/job/api/Dashboard*.java,replicadb-server/frontend/src/api/dashboardApi.ts,replicadb-server/frontend/src/pages/DashboardPage.tsx'
---
# ReplicaDB Dashboard Operations Rules

## Metrics Contract
- Keep dashboard aggregates bounded by an explicit effective time window and apply backend-visible-job restrictions before calculating results.
- Preserve the distinction between active runs, terminal outcomes, processed rows, duration, and queue latency; do not infer replication correctness from one aggregate.

## Frontend Behavior
- Include the selected window in the server-state query key and retain loading, error, empty, refresh, and periodic-refetch states.
- Keep dashboard actions oriented toward investigation; link to the owning jobs or run surfaces instead of duplicating job lifecycle mutations in the summary view.

## Contradiction Check
No organization baseline was available in this checkout, so no contradiction or project override was recorded.
