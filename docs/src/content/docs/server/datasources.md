---
title: Datasources
description: Create, edit, inspect, and permission datasource profiles safely.
---

# Datasources

The `/datasources` catalog lists profiles visible to the current user and can
filter by role. A datasource response exposes a redacted connection display,
capabilities, and permission flags such as `canUse` and `canEdit`; it never
rehydrates stored security values.

## Actions

- ADMIN users see `New datasource` and can open the create form.
- A user with `canEdit` sees `Edit datasource` on its detail page.
- ADMIN users see `Manage permissions` and `Delete datasource`.
- A profile can be source-capable, sink-capable, or single-job only according
  to its connector capability response.

When no profiles exist, ADMIN users see `No datasources configured.` with
`Create a profile before defining a job.` Non-admin users see the same title
with `Ask an administrator for access.` Load failures say `Unable to load
datasources.` and preserve a retry path.

## Secret-preserving edits

Edit forms parse only `safeConnectDisplay` and keep security fields in memory.
Blank security inputs preserve the encrypted stored values. Entered values
replace the corresponding values. `clearSecurityKeys` is the explicit removal
path for a selected key; it is not implied by leaving an input blank.

Do not treat a visible edit control as authorization. The API enforces
datasource `VIEW`, `USE`, and `EDIT` permissions.