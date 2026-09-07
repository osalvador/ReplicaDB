---
title: Permissions
description: Grant and revoke datasource and job resource permissions.
---

# Permissions

Permission pages are ADMIN-only and are reached from a datasource or job
detail page through `Manage permissions`.

## Datasource permissions

Datasource grants use `VIEW`, `USE`, and `EDIT`:

- `VIEW` exposes safe metadata;
- `USE` permits the datasource to be bound into a job; and
- `EDIT` permits profile changes.

The page can grant, update, and revoke rows. The API remains authoritative if
the resource disappears or access changes while the page is open.

## Job permissions

Job grants use `VIEW`, `EDIT`, `EXECUTE`, and `CANCEL`:

- `VIEW` reads the definition and history;
- `EDIT` changes the definition;
- `EXECUTE` triggers a run; and
- `CANCEL` requests cancellation.

Frontend visibility is a convenience. A hidden button is not proof that an API
request would be rejected, and a visible button is not proof that a mutation
will succeed. Render RFC 7807 problem details and permission errors as
recoverable operational notices.