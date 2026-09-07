---
title: Errors and empty states
description: Interpret loading, empty, validation, conflict, and unauthorized states.
---

# Errors and empty states

The frontend names recovery paths instead of hiding operational states.

## Common states

- Loading states say what is being fetched, such as `Loading jobs`, `Loading
  datasource`, `Loading job`, `Loading run`, or `Loading audit events`.
- Empty states distinguish no data from no permission: `No jobs available.`,
  `No datasources configured.`, `No permissions granted.`, and `No runs in this
  window.`.
- Validation errors stay beside the field that needs correction.
- API failures expose RFC 7807 `detail` when available, otherwise a safe
  operation-specific message such as `Unable to save this datasource.`.
- A 401 returns the session to login; a 403 renders `You do not have permission
  to view this page.` through the unauthorized route guard.
- A 409 conflict is recoverable: preserve the user's context, refresh the
  resource, and resolve the changed binding or definition before retrying.

## Binding and secret edge cases

If a job references a datasource the user can no longer `USE`, the binding is
shown as unavailable or disabled and cannot be re-enabled until access is
restored. If a datasource edit omits a security input, the stored encrypted
value is preserved; clearing requires the explicit `clearSecurityKeys` path.

Complete mode can truncate the sink before a failure. Read the run warning and
choose a mode-specific recovery rather than treating a failed run as a safe
rollback.