---
title: Users
description: Administer users, roles, enabled state, and password resets.
---

# Users

`/users` is ADMIN-only. It lists users and supports `Create user`, `Edit`, and
`Reset password for ...`. User edits change role or enabled state; roles are
`ADMIN`, `OPERATOR`, and `VIEWER`.

Password reset is an administrator action and is distinct from the disabled
self-service fields on `My profile`. Keep the reset flow inside the
authenticated control plane and do not document or log resolved values.

The route guard is not the authorization system. The backend must enforce the
same ADMIN boundary even if a user reaches a URL directly.