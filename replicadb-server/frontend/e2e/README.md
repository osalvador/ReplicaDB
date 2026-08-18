# Frontend E2E prerequisites

Run the `replicadb-server` API with the `api` profile, a reachable PostgreSQL metadata database, and these environment variables:

- `REPLICADB_BOOTSTRAP_ADMIN_USERNAME`
- `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD`

The Playwright spec reads the same variables from its process environment. Do not commit their values or place them in source files.

From `replicadb-server/frontend/`, run `npm run test:e2e` with `PLAYWRIGHT_BASE_URL` set when the API is not using `http://localhost:8080`.
