---
applyTo: 'replicadb-server/frontend/**/*.{ts,tsx,mts,js,mjs}'
---
# ReplicaDB Frontend Rules

## Architecture and State
- Keep endpoint calls in `src/api`, authentication state in `src/auth`, route protection in `src/router`, reusable views in `src/components`, and page composition in `src/pages`.
- Use TanStack Query for server state and a fresh query client in tests; do not copy server state into ad hoc React state without a clear UI-only reason.
- Keep the Phase 2a surface read-only. Add mutating controls only with the corresponding backend contract, permission check, CSRF coverage, and planned product slice.

## API Contract
- Treat `src/api/schema.ts` as generated output from the server OpenAPI document; do not hand-maintain duplicate Java response interfaces.
- Keep `apiClient` credentials-enabled at `/api/v1`, preserve RFC 7807 error mapping, and use endpoint modules rather than direct Axios calls in pages.
- Validate generated types against real serialized JSON, including nullable fields and stable schema ordering, before changing adapters.

## Authentication and Authorization
- Bootstrap CSRF before login and preserve the browser `XSRF-TOKEN`/`X-XSRF-TOKEN` contract for logout and future mutations.
- Treat `/auth/me` 401/403 as an anonymous session and redirect through `ProtectedRoute`; never expose protected data while auth status is unresolved.
- Backend ACLs are authoritative. A hidden or disabled frontend control is not authorization, and secrets must never enter client state, fixtures, or logs.

## Polling and Interaction
- Keep run polling aligned with the backend terminal definition: only `SUCCEEDED`, `CANCELLED`, and `RETRY_SCHEDULED` stop polling; `FAILED` remains eligible for retry state changes.
- Preserve stable loading, empty, error, and pagination states without layout-dependent text assumptions.

## Build and Testing
- Keep `npm ci` reproducible with registry-neutral lockfiles and project configuration; never commit machine-specific registry URLs.
- Use Vitest/Testing Library for components and hooks, and Playwright for real cookie/session flows against built static assets. Local browser or certificate workarounds must not weaken CI reproducibility.

## Anti-Patterns
- Do not hand-copy OpenAPI DTOs, bypass the API client, or infer authorization from route visibility.
- Do not stop polling `FAILED` runs merely because they display an error; this differs from the backend's terminal contract.
- Do not put backend credentials, connection strings, or resolved environment values into frontend code or generated artifacts.

## Baseline Check
No shared baseline instruction files were found in this repository. These rules remain project-specific and were derived from the current codebase.