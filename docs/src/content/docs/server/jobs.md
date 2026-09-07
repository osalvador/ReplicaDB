---
title: Jobs
description: Define, edit, run, bind, and remove managed replication jobs.
---

# Jobs

The protected `/jobs` route lists jobs visible to the current user. `New job`
opens a definition with source and sink datasource bindings, table/query
selection, mode, parallel tasks, fetch size, bandwidth, logging, staging, and
retry settings.

## Definition and actions

The job detail route is `/jobs/:id` and is read-only for the definition. It
offers `Trigger run`, `Edit`, and, for ADMIN users, `Manage permissions` and
`Delete`. Deleting a job also removes its run history, schedules, permissions,
and logs after confirmation.

The edit route is `/jobs/:id/edit`. Validation includes `Name is required`,
`Source datasource is required`, `Source table or query is required`,
`Sink datasource is required`, `Sink table is required`, `Parallelism must be
at least 1`, watermark requirements for incremental mode, and retry bounds.

Complete mode shows a warning that it clears the sink before loading; an
interrupted or retried run may leave it empty, truncated, or partially populated. Use
complete-atomic when the connector supports an all-or-nothing load. Binding a
source or sink can be disabled; disabling either blocks future manual and
scheduled runs but does not cancel active work.

Jobs refer to datasource IDs only. Connection security is managed by the
datasource catalog and is never copied into a job response.