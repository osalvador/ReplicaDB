---
applyTo: 'replicadb-server/frontend/scripts/**/*.sh,**/Dockerfile,Containerfile,docker-compose*.yml,DEPLOYMENT.md'
---
# ReplicaDB Deployment Rules

## Runtime Profiles
- Keep the `api` profile as the REST, authentication, session, Quartz, and local-execution surface; keep the `worker` profile HTTP-free except for its internal management endpoint.
- Treat PostgreSQL as the durable state boundary and keep product schedules reconciled from persisted state.

## Local Lifecycle
- Run local startup with Java 17, Docker or Podman, Maven, Node/npm, `curl`, and `lsof` available before creating resources.
- Identify existing ReplicaDB resources by checkout ownership and role before stopping anything; require interactive confirmation and leave resources untouched when confirmation is unavailable or declined.
- Stop child processes before launchers, verify termination, and remove only the dedicated local PostgreSQL container and temporary key material owned by the current run.

## Configuration And Security
- Keep datasource, bootstrap, port, container-engine, and master-key settings environment-managed; never commit resolved credentials or key material.
- Keep container labels and workspace ownership markers consistent when adding managed local resources.

## Observability
- Preserve health checks for API readiness and PostgreSQL readiness before seeding or accepting traffic. Keep logs and diagnostics redacted at API, audit, and local process boundaries.

## Contradiction Check
No organization baseline was available in this checkout, so no contradiction or project override was recorded.
