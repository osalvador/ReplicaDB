---
applyTo: 'replicadb-server/src/main/resources/db/migration/**/*.sql,replicadb-server/src/test/**/*Migration*.java'
---
# ReplicaDB Database Migration Rules

## Forward-Only Changes
- Add a new versioned Flyway migration for schema changes; do not rewrite an applied migration or silently renumber versions.
- Keep foreign-key, index, and constraint names explicit and update migration-count and constraint assertions when versions change.

## State Safety
- Validate orphan and legacy data before adding constraints or delete cascades, and fail the migration with an actionable condition when the precondition is not met.
- Keep PostgreSQL-owned time, partial active-run uniqueness, lease fencing, and job-scoped idempotency semantics aligned with repository SQL.
- Use cascading deletion only for job-owned state; preserve independent audit records.

## Verification
- Exercise staged migration behavior with the real migration API and Testcontainers PostgreSQL. Test both the new schema and the relevant precondition or backfill path.

## Contradiction Check
No organization baseline was available in this checkout, so no contradiction or project override was recorded.
