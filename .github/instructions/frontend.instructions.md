---
applyTo: 'replicadb-server/frontend/**/*.{ts,tsx,mts,js,mjs}'
---
# ReplicaDB Frontend Rules

## Architecture and State
- Keep endpoint calls in `src/api`, authentication state in `src/auth`, route protection in `src/router`, reusable views in `src/components`, and page composition in `src/pages`.
- Use TanStack Query for server state and local React state only for UI or form state. Invalidate the owning query after successful mutations.
- Keep admin navigation and route guards inside `ProtectedRoute`, while backend ACLs remain the authorization boundary for job and user operations.

## API Contract
- Treat `src/api/schema.ts` as generated output from the server OpenAPI document. Use endpoint modules and the configured `apiClient`; do not hand-copy Java DTOs or call Axios directly from pages.
- Preserve the `/api/v1` credentialed client and RFC 7807 error mapping. Validate generated types against serialized JSON, including nullable fields and deterministic schema output.
- Keep retry-policy request fields optional at the wire boundary and resolved policy values non-null in responses. Expose run `availableAt` for retry visibility, but never add `leaseToken` or resolved credentials to API helpers, types, or UI state.

## Authentication and Authorization
- Bootstrap CSRF before login and preserve the browser `XSRF-TOKEN`/`X-XSRF-TOKEN` contract for state-changing requests.
- Treat `/auth/me` 401/403 as anonymous and keep protected content behind `ProtectedRoute` while auth is unresolved.
- Frontend visibility is UX only. Never place backend credentials, connection strings, resolved secrets, or password values in client state, fixtures, logs, or generated artifacts.

## Polling and Interaction
- Stop run polling for `SUCCEEDED`, `CANCELLED`, and `RETRY_SCHEDULED`; keep `FAILED` available for retry-oriented state changes.
- Preserve loading, empty, error, and pagination states. Form tabs switch visible panels without clearing values from hidden panels.
- Keep custom inline validation explicit when native browser validation would prevent the intended testable submit path.
- In the job form, preserve an explicitly edited complete-mode retry choice when the mode changes; use the mode defaults only until the user has touched that policy.

## Build and Testing
- Keep `npm ci` reproducible with registry-neutral lockfiles and project configuration.
- Use Vitest and Testing Library with fresh query clients and matching route context; use Playwright for real cookie/session flows against built static assets.
- Regenerate the OpenAPI-derived schema from the live server contract when DTOs change, then run the schema drift assertions before frontend typecheck/build.

## Anti-Patterns
- Do not hand-copy OpenAPI DTOs, bypass the API client, or infer authorization from route visibility.
- Do not stop polling `FAILED` runs merely because the UI displays an error.
- Do not introduce a generic shared table abstraction when the current resource shapes and call sites do not justify it.
- Do not display or persist lease tokens in the frontend, and do not present `availableAt` as resume progress.

## Contradiction Check
No organization baseline was available in this checkout, so no contradiction or project override was recorded.
