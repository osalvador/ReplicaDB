# Frontend E2E prerequisites

Run the `replicadb-server` API with the `api` profile, a reachable PostgreSQL metadata database, and these environment variables:

- `REPLICADB_BOOTSTRAP_ADMIN_USERNAME`
- `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD`

The Playwright spec reads the same variables from its process environment. Do not commit their values or place them in source files.

From `replicadb-server/frontend/`, run `npm run test:e2e` with `PLAYWRIGHT_BASE_URL` set when the API is not using `http://localhost:8080`.

For the administration smoke test with an isolated local stack, run this from the frontend directory:

```bash
npm run test:e2e:admin:local
```

This command generates an ephemeral ADMIN password when the bootstrap variables are not set, starts a clean PostgreSQL/API/Vite stack on available local ports, runs `admin-management.spec.ts`, and removes the temporary stack when it finishes. It never writes the generated password to the repository or to the test output. Set `REPLICADB_BOOTSTRAP_ADMIN_USERNAME` and `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` beforehand when you need to reuse explicit local credentials.
